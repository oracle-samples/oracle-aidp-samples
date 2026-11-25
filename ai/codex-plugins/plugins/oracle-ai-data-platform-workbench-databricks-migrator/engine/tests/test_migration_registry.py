"""
Unit tests for the migration registry feature:
- Per-job registry read/write
- Cross-job lookup with timestamp-aware merge
- Skip logic when SKIP_MIGRATED is on/off
- Registry update only when SKIP_MIGRATED is enabled
- Parallel-safe: each job writes its own file
"""

import os
import sys
import json
import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate

pytestmark = pytest.mark.real_registry


# ─── Helpers ──────────────────────────────────────────────────────────────────

def make_mock_session(execute_returns=None):
    """Create a mock AIDPSession that returns predefined outputs."""
    session = AsyncMock()
    if execute_returns is None:
        execute_returns = [{"outputs": []}]
    session.execute = AsyncMock(side_effect=execute_returns)
    return session


def make_execute_result(text):
    """Create a mock execute result with the given text output."""
    return {"outputs": [{"type": "stream", "text": text}]}


# What a confirmed save_registry step prints.
SAVED = job_migrate._REGISTRY_SAVED_MARK + "\n"
# _load_one_registry's kernel output: the marker, then the registry JSON.
READ = job_migrate._REGISTRY_READ_MARK


# ─── Reset module state before each test ──────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_registry_state():
    """Reset all registry module-level state before each test."""
    job_migrate._registry_cache.clear()
    job_migrate._all_registries_loaded = False
    job_migrate._all_registries_merged.clear()
    job_migrate._migration_cache.clear()
    original_skip = job_migrate.SKIP_MIGRATED
    original_output = job_migrate.MIGRATE_OUTPUT
    yield
    job_migrate.SKIP_MIGRATED = original_skip
    job_migrate.MIGRATE_OUTPUT = original_output
    job_migrate._registry_cache.clear()
    job_migrate._all_registries_loaded = False
    job_migrate._all_registries_merged.clear()
    job_migrate._migration_cache.clear()


# ─── _registry_path_for_job ──────────────────────────────────────────────────

class TestRegistryPathForJob:
    def test_returns_correct_path(self):
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        assert job_migrate._registry_path_for_job("my_job") == "/Workspace/output/my_job/migration_registry.json"

    def test_different_jobs_different_paths(self):
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        p1 = job_migrate._registry_path_for_job("job_a")
        p2 = job_migrate._registry_path_for_job("job_b")
        assert p1 != p2
        assert "job_a" in p1
        assert "job_b" in p2


# ─── registry_update ─────────────────────────────────────────────────────────

class TestRegistryUpdate:
    def test_writes_even_when_skip_migrated_false(self):
        """registry_update should always write — progress recorded regardless of --skip-migrated."""
        job_migrate.SKIP_MIGRATED = False
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result(READ + "{}"),   # load_registry
            make_execute_result(SAVED),  # save_registry (confirmed)
        ])
        asyncio.run(job_migrate.registry_update(
            session, "Users/foo/nb.ipynb", "/Workspace/out/nb.ipynb", "test_job", "PASS"))
        assert session.execute.call_count >= 1  # should have written

    def test_writes_when_skip_migrated_true(self):
        """registry_update should write to cluster when SKIP_MIGRATED is True."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        # First call: load_registry reads existing (empty)
        # Second call: save_registry writes
        session = make_mock_session([
            make_execute_result(READ + "{}"),   # load_registry
            make_execute_result(SAVED),  # save_registry (confirmed)
        ])
        asyncio.run(job_migrate.registry_update(
            session, "Users/foo/nb.ipynb", "/Workspace/out/nb.ipynb", "test_job", "PASS"))
        assert session.execute.call_count == 2  # load + save

    def test_updates_merged_view(self):
        """registry_update should also update _all_registries_merged for same-run lookups."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result(READ + "{}"),   # load_registry
            make_execute_result(SAVED),  # save_registry (confirmed)
        ])
        asyncio.run(job_migrate.registry_update(
            session, "Users/foo/nb.ipynb", "/Workspace/out/nb.ipynb", "test_job", "PASS"))
        # registry_key() canonicalizes to the FULL /Workspace path (one
        # unambiguous key per notebook across jobs with different roots).
        key = "/Workspace/Users/foo/nb.ipynb"
        assert key in job_migrate._all_registries_merged
        entry = job_migrate._all_registries_merged[key]
        assert entry["migrated_path"] == "/Workspace/out/nb.ipynb"
        assert entry["job_name"] == "test_job"
        assert entry["status"] == "PASS"

    def test_caches_per_job(self):
        """Each job should have its own entry in _registry_cache."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result(READ + "{}"),   # load job_a
            make_execute_result(SAVED),  # save job_a (confirmed)
            make_execute_result(READ + "{}"),   # load job_b
            make_execute_result(SAVED),  # save job_b (confirmed)
        ])
        asyncio.run(job_migrate.registry_update(
            session, "Users/foo/nb1.ipynb", "/Workspace/out/a/nb1.ipynb", "job_a", "PASS"))
        asyncio.run(job_migrate.registry_update(
            session, "Users/foo/nb2.ipynb", "/Workspace/out/b/nb2.ipynb", "job_b", "PASS"))
        assert "job_a" in job_migrate._registry_cache
        assert "job_b" in job_migrate._registry_cache
        assert "/Workspace/Users/foo/nb1.ipynb" in job_migrate._registry_cache["job_a"]
        assert "/Workspace/Users/foo/nb2.ipynb" in job_migrate._registry_cache["job_b"]


# ─── registry_lookup ─────────────────────────────────────────────────────────

class TestRegistryLookup:
    def test_returns_none_when_skip_migrated_false(self):
        """Should return None immediately when SKIP_MIGRATED is off."""
        job_migrate.SKIP_MIGRATED = False
        session = make_mock_session()
        result = asyncio.run(job_migrate.registry_lookup(session, "Users/foo/nb.ipynb"))
        assert result is None
        session.execute.assert_not_called()

    def test_returns_none_when_not_found(self):
        """Should return None when path is not in any registry."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result("[]"),  # _load_all_registries: list dirs
        ])
        result = asyncio.run(job_migrate.registry_lookup(session, "Users/foo/nb.ipynb"))
        assert result is None

    def test_returns_path_when_valid(self):
        """Should return migrated_path when found and VALID on cluster."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        # Pre-populate merged registries to skip the load
        job_migrate._all_registries_loaded = True
        job_migrate._all_registries_merged = {
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_a",
                "status": "PASS",
                "timestamp": "2026-04-08T12:00:00",
            }
        }
        session = make_mock_session([
            make_execute_result("VALID"),  # validation check
        ])
        result = asyncio.run(job_migrate.registry_lookup(session, "Users/foo/nb.ipynb"))
        assert result == "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb"

    def test_returns_none_when_corrupt(self):
        """Should return None when file exists but is corrupt."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate._all_registries_loaded = True
        job_migrate._all_registries_merged = {
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_a",
                "status": "PASS",
                "timestamp": "2026-04-08T12:00:00",
            }
        }
        session = make_mock_session([
            make_execute_result("CORRUPT"),
        ])
        result = asyncio.run(job_migrate.registry_lookup(session, "Users/foo/nb.ipynb"))
        assert result is None

    def test_returns_none_when_missing(self):
        """Should return None when migrated file no longer exists."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate._all_registries_loaded = True
        job_migrate._all_registries_merged = {
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_a",
                "status": "PASS",
                "timestamp": "2026-04-08T12:00:00",
            }
        }
        session = make_mock_session([
            make_execute_result("MISSING"),
        ])
        result = asyncio.run(job_migrate.registry_lookup(session, "Users/foo/nb.ipynb"))
        assert result is None


# ─── _load_all_registries (timestamp-aware merge) ────────────────────────────

class TestLoadAllRegistries:
    def test_most_recent_timestamp_wins(self):
        """When same notebook in multiple registries, most recent timestamp should win."""
        job_migrate.SKIP_MIGRATED = True
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"

        reg_a = json.dumps({
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_a",
                "status": "PASS",
                "timestamp": "2026-04-07T10:00:00",
            }
        })
        reg_b = json.dumps({
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_b/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_b",
                "status": "PASS",
                "timestamp": "2026-04-08T10:00:00",
            }
        })

        session = make_mock_session([
            # _load_all_registries: list dirs
            make_execute_result(json.dumps([
                "/Workspace/output/job_a/migration_registry.json",
                "/Workspace/output/job_b/migration_registry.json",
            ])),
            # _load_one_registry for job_a
            make_execute_result(READ + reg_a),
            # _load_one_registry for job_b
            make_execute_result(READ + reg_b),
        ])

        result = asyncio.run(job_migrate._load_all_registries(session))
        entry = result["Users/foo/nb.ipynb"]
        assert entry["job_name"] == "job_b"  # job_b has later timestamp
        assert entry["migrated_path"] == "/Workspace/output/job_b/notebooks/Users/foo/nb.ipynb"

    def test_older_timestamp_does_not_overwrite(self):
        """Earlier timestamp should NOT overwrite a later one."""
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"

        reg_a = json.dumps({
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_a/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_a",
                "status": "PASS",
                "timestamp": "2026-04-08T10:00:00",  # later
            }
        })
        reg_b = json.dumps({
            "Users/foo/nb.ipynb": {
                "migrated_path": "/Workspace/output/job_b/notebooks/Users/foo/nb.ipynb",
                "job_name": "job_b",
                "status": "PASS",
                "timestamp": "2026-04-07T10:00:00",  # earlier
            }
        })

        session = make_mock_session([
            make_execute_result(json.dumps([
                "/Workspace/output/job_a/migration_registry.json",
                "/Workspace/output/job_b/migration_registry.json",
            ])),
            make_execute_result(READ + reg_a),
            make_execute_result(READ + reg_b),
        ])

        result = asyncio.run(job_migrate._load_all_registries(session))
        entry = result["Users/foo/nb.ipynb"]
        assert entry["job_name"] == "job_a"  # job_a has later timestamp

    def test_loads_once_and_caches(self):
        """Second call should return cached result without cluster calls."""
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result("[]"),  # first call
        ])
        asyncio.run(job_migrate._load_all_registries(session))
        assert job_migrate._all_registries_loaded is True

        # Second call — no more execute calls
        call_count_before = session.execute.call_count
        asyncio.run(job_migrate._load_all_registries(session))
        assert session.execute.call_count == call_count_before

    def test_empty_when_no_registries(self):
        """Should return empty dict when no registry files exist."""
        job_migrate.MIGRATE_OUTPUT = "/Workspace/output"
        session = make_mock_session([
            make_execute_result("[]"),
        ])
        result = asyncio.run(job_migrate._load_all_registries(session))
        assert result == {}


# ─── verify_upload ────────────────────────────────────────────────────────────

class TestVerifyUpload:
    def test_returns_true_when_file_exists(self):
        """Should return True when file exists with size > 0."""
        session = make_mock_session([
            make_execute_result("12345"),  # file size
        ])
        result = asyncio.run(job_migrate.verify_upload(session, "/Workspace/out/nb.ipynb"))
        assert result is True

    def test_returns_false_when_missing(self):
        """Should return False after retries when file is missing."""
        session = make_mock_session([
            make_execute_result("MISSING"),  # attempt 1
            make_execute_result("MISSING"),  # attempt 2 (after sleep)
            make_execute_result("MISSING"),  # attempt 3 (after sleep)
        ])
        log_messages = []
        result = asyncio.run(job_migrate.verify_upload(
            session, "/Workspace/out/nb.ipynb", retries=2, log_fn=log_messages.append))
        assert result is False
        assert session.execute.call_count == 3
        assert any("not persisted" in m for m in log_messages)
        assert any("WARNING" in m for m in log_messages)

    def test_retries_then_succeeds(self):
        """Should return True if file appears after retry."""
        session = make_mock_session([
            make_execute_result("MISSING"),  # attempt 1
            make_execute_result("45000"),    # attempt 2 — file appeared
        ])
        log_messages = []
        result = asyncio.run(job_migrate.verify_upload(
            session, "/Workspace/out/nb.ipynb", retries=2, log_fn=log_messages.append))
        assert result is True
        assert session.execute.call_count == 2

    def test_returns_false_on_zero_size(self):
        """Should treat zero-size file as missing."""
        session = make_mock_session([
            make_execute_result("0"),       # attempt 1 — zero size
            make_execute_result("0"),       # attempt 2
            make_execute_result("0"),       # attempt 3
        ])
        result = asyncio.run(job_migrate.verify_upload(
            session, "/Workspace/out/nb.ipynb", retries=2))
        assert result is False

    def test_handles_execute_exception(self):
        """Should handle session.execute raising an exception."""
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[
            Exception("WS dead"),
            Exception("WS dead"),
            make_execute_result("12345"),
        ])
        result = asyncio.run(job_migrate.verify_upload(
            session, "/Workspace/out/nb.ipynb", retries=2))
        assert result is True

    def test_no_log_fn(self):
        """Should work without log_fn (no crash)."""
        session = make_mock_session([
            make_execute_result("MISSING"),
            make_execute_result("MISSING"),
            make_execute_result("MISSING"),
        ])
        result = asyncio.run(job_migrate.verify_upload(
            session, "/Workspace/out/nb.ipynb", retries=2, log_fn=None))
        assert result is False


# ─── normalize_nb_path consistency ────────────────────────────────────────────

class TestNormalizeNbPath:
    def test_strips_workspace_prefix(self):
        assert job_migrate.normalize_nb_path("/Workspace/Users/foo/nb.ipynb") == "Users/foo/nb.ipynb"

    def test_strips_leading_slash(self):
        assert job_migrate.normalize_nb_path("/Users/foo/nb.ipynb") == "Users/foo/nb.ipynb"

    def test_no_prefix(self):
        assert job_migrate.normalize_nb_path("Users/foo/nb.ipynb") == "Users/foo/nb.ipynb"


# ─── _load_one_registry: AIDP duplicated stdout ──────────────────────────────
# AIDP can deliver a cell's stdout twice ("{...}\n{...}"); a plain json.loads
# raised "Extra data" and the loader returned {} — a resume then found no
# recorded verdict (live run 2026-10-15, review-fixes r2) and the job's write
# cache started empty.

class TestLoadOneRegistryDuplicatedStdout:
    def test_duplicated_registry_output_still_loads(self):
        registry = {"/Workspace/Users/x/nb.ipynb": {"migrated_path": "/m/nb.ipynb", "status": "PARTIAL"}}
        line = json.dumps(registry)
        session = make_mock_session([make_execute_result(f"{READ}{line}\n{READ}{line}\n")])
        loaded = asyncio.run(job_migrate._load_one_registry(session, "/m/migration_registry.json"))
        assert loaded == registry

    def test_duplicated_corrupt_marker_is_still_reported(self, capsys):
        marker = json.dumps({"__registry_corrupt__": "/m/r.json.corrupt.1", "error": "bad"})
        session = make_mock_session([make_execute_result(f"{READ}{marker}\n{READ}{marker}\n")])
        assert asyncio.run(job_migrate._load_one_registry(session, "/m/r.json")) == {}
        assert "corrupt registry quarantined -> /m/r.json.corrupt.1" in capsys.readouterr().out
