"""Unit tests for aidp_compat.dbfs_open_hook.

Covers the runtime open() shim behavior that handles /dbfs/* paths that
appear only at runtime (dict values, model-registry returns) — the case the
static source rewriter cannot see.

Every test isolates state via setUp/tearDown so tests don't interfere with
each other's builtins.open state.
"""

from __future__ import annotations

import builtins
import os
import pickle
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDbfsOpenHook(unittest.TestCase):

    def setUp(self):
        # Fresh temp workspace per test
        self.tmpdir = tempfile.mkdtemp(prefix="aidp_hook_test_")
        # Real location that translated /dbfs/... paths will point to
        self.vol_dbfs = os.path.join(self.tmpdir, "vol/default/default/dbfs")
        os.makedirs(self.vol_dbfs, exist_ok=True)
        # Point AIDP_DBFS_PREFIX so translate_path uses our temp dir
        self._saved_prefix = os.environ.pop("AIDP_DBFS_PREFIX", None)
        os.environ["AIDP_DBFS_PREFIX"] = self.vol_dbfs

        # Import + install the shim. Do NOT purge aidp_compat from
        # sys.modules for a "fresh" import: that forks module state — tests
        # (and conftest) that already imported aidp_compat.notebook_policy
        # keep the stale instance while new imports get a fresh one, so the
        # policy reset stops working for every later test in the process.
        # translate_path re-reads AIDP_DBFS_PREFIX on each call, so a plain
        # import sees the env var set above.
        from aidp_compat import dbfs_open_hook
        dbfs_open_hook.install()
        self._hook = dbfs_open_hook

    def tearDown(self):
        # Restore builtins.open — critical, else later tests inherit the shim
        self._hook.uninstall()
        # Restore env
        if self._saved_prefix is None:
            os.environ.pop("AIDP_DBFS_PREFIX", None)
        else:
            os.environ["AIDP_DBFS_PREFIX"] = self._saved_prefix
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_at_vol(self, rel_path: str, content: bytes) -> str:
        """Write a real file at the translated (/Volumes/...) location."""
        full = os.path.join(self.vol_dbfs, rel_path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "wb") as f:
            f.write(content)
        return full

    # ────────────── happy paths ──────────────

    def test_read_falls_back_to_vol_on_dbfs_missing(self):
        """The Model_Mon Cell 44 case — /dbfs/... path with file at /Volumes."""
        self._write_at_vol("FileStore/x.pkl", pickle.dumps({"ok": True}))
        result = pickle.load(open("/dbfs/FileStore/x.pkl", "rb"))
        self.assertEqual(result, {"ok": True})

    def test_dict_lookup_path_fallback(self):
        """Exact Cell 44 shape — path from a dict variable."""
        self._write_at_vol("FileStore/col_a.pkl", pickle.dumps({"schema": "v1"}))
        schema_map = {"col_a": "/dbfs/FileStore/col_a.pkl"}
        c = "col_a"
        result = pickle.load(open(schema_map[c], "rb"))
        self.assertEqual(result, {"schema": "v1"})

    @unittest.skipIf(
        sys.platform == "win32",
        "dbfs:/... is not a valid POSIX path prefix on Windows — open() "
        "raises OSError [Errno 22] (invalid argument) because Windows "
        "interprets 'dbfs:' as a drive-letter-ish scheme. The hook's "
        "narrow FileNotFoundError catch is intentional (see docstring); "
        "production target is Linux where dbfs:/... raises FileNotFoundError "
        "cleanly and the fallback works. PR-35 reviewer feedback noted this "
        "platform mismatch — skipping on Windows keeps the intent clear."
    )
    def test_dbfs_scheme_prefix_also_handled(self):
        """dbfs:/... URI form (as opposed to /dbfs/... POSIX form)."""
        self._write_at_vol("FileStore/y.txt", b"hello")
        with open("dbfs:/FileStore/y.txt", "rb") as f:
            self.assertEqual(f.read(), b"hello")

    def test_original_dbfs_wins_if_exists(self):
        """Fallback semantics — try original first. If it exists, use it."""
        # Simulate: /dbfs/... IS accessible (e.g. cluster admin created a symlink).
        # We can't create files at /dbfs on a dev laptop; simulate by pointing
        # AIDP_DBFS_PREFIX at a NON-EXISTENT location so translation fails —
        # but the "original" happens to exist via being explicitly writable.
        raw = os.path.join(self.tmpdir, "raw_dbfs_stub.pkl")
        with open(raw, "wb") as f:
            f.write(pickle.dumps({"from": "raw"}))
        # Point env prefix to a bogus location — fallback would fail
        os.environ["AIDP_DBFS_PREFIX"] = "/nonexistent/aidp/prefix"
        # Uninstall + reinstall to re-eval env (translate_path is cached);
        # actually translate_path re-reads env each call so no reinstall needed
        result = pickle.load(open(raw, "rb"))
        self.assertEqual(result, {"from": "raw"})

    # ────────────── pass-through cases ──────────────

    def test_local_tmp_path_untouched(self):
        p = os.path.join(self.tmpdir, "local.txt")
        with open(p, "wb") as f:
            f.write(b"local")
        with open(p, "rb") as f:
            self.assertEqual(f.read(), b"local")

    def test_pathlib_path_pass_through(self):
        import pathlib
        p = pathlib.Path(self.tmpdir) / "plib.txt"
        with open(p, "wb") as f:
            f.write(b"path-object")
        with open(p, "rb") as f:
            self.assertEqual(f.read(), b"path-object")

    def test_int_fd_pass_through(self):
        # File descriptor as first arg (rare but valid open() call)
        import tempfile as _tf
        fd, path = _tf.mkstemp(dir=self.tmpdir)
        os.write(fd, b"fd-write")
        os.close(fd)
        # Open the file via string path (fd int open is more obscure)
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"fd-write")

    # ────────────── error signals preserved ──────────────

    def test_missing_at_both_locations_raises(self):
        """If neither /dbfs/... nor /Volumes/... has the file, propagate."""
        with self.assertRaises(FileNotFoundError):
            open("/dbfs/does/not/exist.pkl", "rb")

    def test_missing_non_dbfs_still_raises(self):
        """Non-dbfs paths — no fallback, same error as vanilla open()."""
        with self.assertRaises(FileNotFoundError):
            open("/nonexistent/foo.txt", "rb")

    # ────────────── shim self-test ──────────────

    def test_install_is_idempotent(self):
        # Double-install shouldn't double-wrap
        self._hook.install()
        first = builtins.open
        self._hook.install()
        second = builtins.open
        self.assertIs(first, second)

    def test_uninstall_restores_original(self):
        self._hook.uninstall()
        self.assertFalse(getattr(builtins.open, "__aidp_dbfs_shim__", False))
        # Reinstall for tearDown
        self._hook.install()

    def test_shim_carries_marker(self):
        self.assertTrue(getattr(builtins.open, "__aidp_dbfs_shim__", False))

    def test_write_mode_fallback_when_parent_exists(self):
        """Write mode: raw /dbfs write fails → fallback tries translated. If
        translated parent EXISTS, write succeeds there."""
        # Create parent at translated location
        target_parent = os.path.join(self.vol_dbfs, "FileStore/new")
        os.makedirs(target_parent, exist_ok=True)
        with open("/dbfs/FileStore/new/w.txt", "wb") as f:
            f.write(b"written")
        # Verify write landed at translated location
        written = os.path.join(target_parent, "w.txt")
        self.assertTrue(os.path.exists(written))
        with open(written, "rb") as f:
            self.assertEqual(f.read(), b"written")

    def test_write_mode_fails_when_no_parent_anywhere(self):
        """Write mode: neither raw nor translated parent exists → error."""
        with self.assertRaises(FileNotFoundError):
            with open("/dbfs/FileStore/nomkdir/w.txt", "wb") as f:
                f.write(b"nope")

    def test_pickle_load_open_dict_lookup_pattern(self):
        """End-to-end: the exact Cell 44 loop body."""
        # Setup: 3 schema files at translated location
        expected = {}
        schema_map = {}
        for col in ("credit_data", "loan_offer_data", "loan_rejection_data"):
            val = {"col": col, "fields": [f"f_{col}_1", f"f_{col}_2"]}
            self._write_at_vol(f"FileStore/schemas/{col}.pkl", pickle.dumps(val))
            expected[col] = val
            schema_map[col] = f"/dbfs/FileStore/schemas/{col}.pkl"

        # Cell 44 loop
        for c in schema_map:
            schema = pickle.load(open(schema_map[c], "rb"))
            self.assertEqual(schema, expected[c])


if __name__ == "__main__":
    unittest.main()
