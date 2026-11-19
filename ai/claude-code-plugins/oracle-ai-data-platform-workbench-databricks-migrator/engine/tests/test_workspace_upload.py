#!/usr/bin/env python3
"""
Test workspace upload and OCI Object Storage fallback.

Tests the upload logic extracted from job_migrate.py:
1. upload_file_to_workspace — write a file to AIDP workspace via session.execute()
2. upload_to_object_storage — write a file to OCI Object Storage (fallback)
3. fallback_upload_to_ocs — upload all artifacts to OCS when workspace fails

Usage:
    pytest tests/test_workspace_upload.py -v
    python3 tests/test_workspace_upload.py  # run directly
"""

import asyncio
import base64
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


# ─── Extracted upload function (mirrors job_migrate.py upload logic) ──

async def upload_file_to_workspace(session, local_file: str, remote_path: str,
                                   log_fn=None, verify_fn=None) -> bool:
    """Upload a local file to the AIDP workspace FUSE filesystem via session.execute().

    Handles chunked upload for large files (base64 > 45000 chars).
    Extracted from job_migrate.py process_notebook() upload block.

    Args:
        session: ClusterSession or compatible object with execute() method
        local_file: path to local file to upload
        remote_path: destination path on workspace (e.g. /Workspace/...)
        log_fn: optional logging function
        verify_fn: optional async verify function(session, remote_path, log_fn) -> bool

    Returns True on success. Raises on failure.
    """
    with open(local_file, 'r') as f:
        content_str = f.read()
    b64 = base64.b64encode(content_str.encode('utf-8')).decode('ascii')
    CHUNK = 45000

    if len(b64) <= CHUNK:
        await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
""", timeout=60)
    else:
        chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]
        await session.execute(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
""", timeout=60)
        for chunk in chunks[1:]:
            await session.execute(f"""
import base64, builtins
with builtins.open('{remote_path}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
""", timeout=60)

    if log_fn:
        log_fn(f"  Uploaded: {os.path.basename(remote_path)} ({len(content_str):,} chars)")

    # Verify large files
    if len(content_str) > 40000 and verify_fn:
        if not await verify_fn(session, remote_path, log_fn=log_fn):
            raise RuntimeError(f"FUSE verification failed: {remote_path}")

    return True


# ─── Tests ───────────────────────────────────────────────────────────


class TestUploadFileToWorkspace(unittest.TestCase):
    """Test the extracted workspace upload function."""

    def test_small_file_single_chunk(self):
        """Small file should be uploaded in a single execute call."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("hello world")
            tmp_path = f.name

        try:
            result = asyncio.run(
                upload_file_to_workspace(session, tmp_path, "/Workspace/test/file.txt"))
            self.assertTrue(result)
            # Single execute call (no chunking)
            self.assertEqual(session.execute.call_count, 1)
            # Verify the execute call contains base64 decode
            call_code = session.execute.call_args[0][0]
            self.assertIn("base64.b64decode", call_code)
            self.assertIn("/Workspace/test/file.txt", call_code)
        finally:
            os.unlink(tmp_path)

    def test_large_file_chunked(self):
        """File > 45000 base64 chars should be chunked into multiple execute calls."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}

        # Create a file that produces > 45000 base64 chars (~34KB raw)
        large_content = "x" * 35000

        with tempfile.NamedTemporaryFile(mode='w', suffix='.ipynb', delete=False) as f:
            f.write(large_content)
            tmp_path = f.name

        try:
            result = asyncio.run(
                upload_file_to_workspace(session, tmp_path, "/Workspace/test/large.ipynb"))
            self.assertTrue(result)
            # Should have multiple execute calls (first chunk + append chunks)
            self.assertGreater(session.execute.call_count, 1)
            # First call should use 'wb' (write), subsequent should use 'ab' (append)
            first_call = session.execute.call_args_list[0][0][0]
            self.assertIn("'wb'", first_call)
            if session.execute.call_count > 1:
                second_call = session.execute.call_args_list[1][0][0]
                self.assertIn("'ab'", second_call)
        finally:
            os.unlink(tmp_path)

    def test_verify_called_for_large_files(self):
        """Files > 40000 chars should trigger verification."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}
        verify_fn = AsyncMock(return_value=True)

        large_content = "y" * 41000

        with tempfile.NamedTemporaryFile(mode='w', suffix='.ipynb', delete=False) as f:
            f.write(large_content)
            tmp_path = f.name

        try:
            result = asyncio.run(
                upload_file_to_workspace(session, tmp_path, "/Workspace/test/big.ipynb",
                                         verify_fn=verify_fn))
            self.assertTrue(result)
            verify_fn.assert_called_once()
        finally:
            os.unlink(tmp_path)

    def test_verify_not_called_for_small_files(self):
        """Files <= 40000 chars should NOT trigger verification."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}
        verify_fn = AsyncMock(return_value=True)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("small file")
            tmp_path = f.name

        try:
            asyncio.run(
                upload_file_to_workspace(session, tmp_path, "/Workspace/test/small.txt",
                                         verify_fn=verify_fn))
            verify_fn.assert_not_called()
        finally:
            os.unlink(tmp_path)

    def test_verify_failure_raises(self):
        """Failed verification should raise RuntimeError."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}
        verify_fn = AsyncMock(return_value=False)

        large_content = "z" * 41000

        with tempfile.NamedTemporaryFile(mode='w', suffix='.ipynb', delete=False) as f:
            f.write(large_content)
            tmp_path = f.name

        try:
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(
                    upload_file_to_workspace(session, tmp_path, "/Workspace/test/fail.ipynb",
                                             verify_fn=verify_fn))
            self.assertIn("FUSE verification failed", str(ctx.exception))
        finally:
            os.unlink(tmp_path)

    def test_session_execute_failure_propagates(self):
        """If session.execute raises, the error should propagate."""
        session = AsyncMock()
        session.execute.side_effect = Exception("WS disconnected")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("test")
            tmp_path = f.name

        try:
            with self.assertRaises(Exception) as ctx:
                asyncio.run(
                    upload_file_to_workspace(session, tmp_path, "/Workspace/test/err.txt"))
            self.assertIn("WS disconnected", str(ctx.exception))
        finally:
            os.unlink(tmp_path)

    def test_log_fn_called(self):
        """log_fn should be called with upload details."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}
        logs = []

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("logged upload")
            tmp_path = f.name

        try:
            asyncio.run(
                upload_file_to_workspace(session, tmp_path, "/Workspace/test/log.txt",
                                         log_fn=logs.append))
            self.assertEqual(len(logs), 1)
            self.assertIn("Uploaded", logs[0])
            self.assertIn("log.txt", logs[0])
        finally:
            os.unlink(tmp_path)

    def test_makedirs_in_execute_code(self):
        """Execute code should include os.makedirs for the parent directory."""
        session = AsyncMock()
        session.execute.return_value = {"status": "ok"}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("dir test")
            tmp_path = f.name

        try:
            asyncio.run(
                upload_file_to_workspace(session, tmp_path,
                                         "/Workspace/deep/nested/path/file.txt"))
            call_code = session.execute.call_args[0][0]
            self.assertIn("os.makedirs", call_code)
        finally:
            os.unlink(tmp_path)


# TestOCIObjectStorageFallback class removed 2026-08-22: OCI Object Storage
# fallback feature (upload_to_object_storage, fallback_upload_to_ocs, _get_os_client,
# OCI_BACKUP_BUCKET, OCI_BACKUP_NAMESPACE) was removed from job_migrate.py.
# See the Session 1 config-externalization refactor.


if __name__ == "__main__":
    unittest.main()
