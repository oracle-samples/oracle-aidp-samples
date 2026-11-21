"""Customization pack (Layer 1 golden-notebook overrides) — engine behavior.

Covers: basename matching (any path), optional path_glob disambiguation,
drift detection (APPLIED vs DRIFT), fail-loud on missing replacement / bad
schema / configured-but-unloadable pack, the inert (no-pack) path, the
basename-collision ambiguity guard, posix/case-sensitive glob semantics,
job-level CUSTOMIZATION_FAILED propagation (summary, DAG blocking, circuit
breaker), and the pip install-skip pin/persistence rules in
cluster_lifecycle.install_missing_package.
"""
import asyncio
import contextlib
import io
import json
import os
import re
import sys
import types
from functools import reduce

import pytest
import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import customization_pack as cp  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def _reset_pack_engine():
    """Never leak a loaded pack (or ambiguity tracker) into other tests."""
    yield
    cp.load_pack(None)


def _write_pack(tmp_path, overrides, golden_cells):
    (tmp_path / "notebooks").mkdir(exist_ok=True)
    (tmp_path / "notebooks" / "read_ft_tables.ipynb").write_text(
        json.dumps({"cells": golden_cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
    )
    (tmp_path / "overrides.yaml").write_text(
        yaml.safe_dump({"schema_version": 1, "overrides": overrides})
    )
    return str(tmp_path)


def _orig(*code):
    return {"cells": [{"cell_type": "code", "source": [c]} for c in code]}


def _hash(nb):
    return cp._norm_code_hash(nb)


GOLDEN = [{"cell_type": "code", "source": ["# golden\nx = 1"]}]


def test_absent_pack_is_inert():
    cp.load_pack(None)
    assert cp.match_override("/Workspace/x/read_ft_tables.ipynb") is None


def test_match_by_basename_any_path(tmp_path):
    orig = _orig("a=1")
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "original_sha256": _hash(orig),
    }], GOLDEN)
    # One notebook per run: a globless entry matches the basename at ANY path
    # (reload between paths — within a single run, two DISTINCT paths for one
    # globless entry are ambiguous, see the collision tests below).
    for p in ("/Workspace/Users/a@x.com/foo/read_ft_tables.ipynb",
              "/Workspace/Repos/team/bar/read_ft_tables.ipynb",
              "read_ft_tables"):  # no .ipynb suffix
        cp.load_pack(pack)
        assert cp.match_override(p) is not None
    assert cp.match_override("/Workspace/x/unrelated.ipynb") is None


def test_exact_hash_reports_applied(tmp_path):
    orig = _orig("a=1", "b=2")
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "original_sha256": _hash(orig),
    }], GOLDEN)
    cp.load_pack(pack)
    res = cp.apply_override(cp.match_override("/w/read_ft_tables.ipynb"), orig)
    assert res["status"] == "OVERRIDE_APPLIED"
    assert res["cells"] == GOLDEN


def test_hash_mismatch_reports_drift_but_still_applies(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "original_sha256": "deadbeef",
    }], GOLDEN)
    cp.load_pack(pack)
    res = cp.apply_override(cp.match_override("/w/read_ft_tables.ipynb"), _orig("changed=1"))
    assert res["status"] == "OVERRIDE_DRIFT"
    assert res["cells"] == GOLDEN  # applied regardless — verify+fix reconciles


def test_no_original_hash_is_always_applied(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
    }], GOLDEN)
    cp.load_pack(pack)
    res = cp.apply_override(cp.match_override("/w/read_ft_tables.ipynb"), _orig("whatever"))
    assert res["status"] == "OVERRIDE_APPLIED"


def test_path_glob_disambiguates_same_basename(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "path_glob": "*/auto_ft_table_generation/*",
    }], GOLDEN)
    cp.load_pack(pack)
    assert cp.match_override("/W/Users/a/auto_ft_table_generation/read_ft_tables.ipynb") is not None
    assert cp.match_override("/W/Users/a/other_dir/read_ft_tables.ipynb") is None


def test_missing_replacement_raises(tmp_path):
    (tmp_path / "overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1,
        "overrides": [{"notebook": "read_ft_tables.ipynb",
                       "replacement": "notebooks/does_not_exist.ipynb"}],
    }))
    cp.load_pack(str(tmp_path))
    with pytest.raises(FileNotFoundError):
        cp.apply_override(cp.match_override("/w/read_ft_tables.ipynb"), _orig("a=1"))


def test_bad_schema_version_raises(tmp_path):
    (tmp_path / "overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 99, "overrides": []}))
    with pytest.raises(ValueError):
        cp.load_pack(str(tmp_path))


def test_override_missing_required_keys_raises(tmp_path):
    (tmp_path / "overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1, "overrides": [{"notebook": "x.ipynb"}]}))  # no replacement
    with pytest.raises(ValueError):
        cp.load_pack(str(tmp_path))


# ── Fix 2: a CONFIGURED pack that cannot be loaded fails loud at load time ──
# (only an absent `customizations` section is inert — see
# test_absent_pack_is_inert above, which pins the inert path)

def test_configured_missing_pack_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="pack directory not found"):
        cp.load_pack(str(tmp_path / "no_such_dir_typo"))
    # the failed load must leave the engine inert, not half-loaded
    assert cp.match_override("/w/read_ft_tables.ipynb") is None


def test_configured_pack_without_registry_raises(tmp_path):
    (tmp_path / "notebooks").mkdir()  # dir exists but no overrides.yaml
    with pytest.raises(FileNotFoundError, match="no overrides.yaml"):
        cp.load_pack(str(tmp_path))


def test_configured_pack_malformed_yaml_raises(tmp_path):
    (tmp_path / "overrides.yaml").write_text("schema_version: 1\noverrides: [unclosed")
    with pytest.raises(yaml.YAMLError):
        cp.load_pack(str(tmp_path))


# ── Fix 3: basename collision without path_glob is ambiguous — fail loud ──

def test_globless_override_two_distinct_notebooks_raises(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
    }], GOLDEN)
    cp.load_pack(pack)
    assert cp.match_override("/w/team_a/read_ft_tables.ipynb") is not None
    # same notebook again (retry / suffixless form) is fine — not ambiguous
    assert cp.match_override("/w/team_a/read_ft_tables.ipynb") is not None
    assert cp.match_override("/w/team_a/read_ft_tables") is not None
    # a SECOND distinct notebook path sharing the basename must fail loud
    with pytest.raises(ValueError, match="ambiguous override — add path_glob"):
        cp.match_override("/w/team_b/elsewhere/read_ft_tables.ipynb")


def test_path_glob_resolves_basename_collision(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "path_glob": "*/auto_ft_table_generation/*",
    }], GOLDEN)
    cp.load_pack(pack)
    # only the production path is overridden; the same-name notebook elsewhere
    # is left alone (returns None) with no ambiguity error
    assert cp.match_override("/w/x/auto_ft_table_generation/read_ft_tables.ipynb") is not None
    assert cp.match_override("/w/team_b/elsewhere/read_ft_tables.ipynb") is None




def test_path_glob_backslash_path_matches_posix_glob(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "path_glob": "*/auto_ft_table_generation/*",
    }], GOLDEN)
    cp.load_pack(pack)
    assert cp.match_override(
        "C:\\ws\\auto_ft_table_generation\\read_ft_tables.ipynb") is not None


def test_path_glob_case_mismatch_never_matches(tmp_path):
    pack = _write_pack(tmp_path, [{
        "notebook": "read_ft_tables.ipynb",
        "replacement": "notebooks/read_ft_tables.ipynb",
        "path_glob": "*/Auto_FT_Table_Generation/*",
    }], GOLDEN)
    cp.load_pack(pack)
    # case-sensitive everywhere — identical result on Windows and Linux
    assert cp.match_override("/w/auto_ft_table_generation/read_ft_tables.ipynb") is None
    assert cp.match_override("/w/Auto_FT_Table_Generation/read_ft_tables.ipynb") is not None


# ── Fix 1: CUSTOMIZATION_FAILED must fail the JOB, block dependents, and ──
# ── count toward the circuit breaker — identical routing to a failure.   ──

class _NoopSession:
    cluster_id = "fake-cluster"

    async def execute(self, code, timeout=None):
        return {"status": "ok", "outputs": [{"type": "stream", "text": ""}]}

    async def run_stateless(self, code, timeout=None):
        return {"status": "ok", "outputs": [{"type": "stream", "text": ""}]}


def _job_env(monkeypatch, tmp_path):
    """Wire job_migrate for an offline process_job run (real process_notebook
    up to the customization gate; no cluster, no LLM)."""
    import job_migrate as jm

    async def _no_backfill(session, job_name, max_iterations=4):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    async def _noop_stage(session):
        return None

    orig_nb = json.dumps({
        "cells": [{"cell_type": "code", "source": ["print('original')\n"],
                   "outputs": [], "metadata": {}, "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }).encode("utf-8")

    monkeypatch.setattr(jm, "MIGRATE_OUTPUT",
                        str(tmp_path / "out").replace("\\", "/"))
    monkeypatch.setattr(jm, "CODE_ONLY", True)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "backfill_missing_deps", _no_backfill)
    monkeypatch.setattr(jm, "stage_export_resources", _noop_stage, raising=False)
    monkeypatch.setattr(jm, "download_notebook",
                        lambda path, max_retries=3: orig_nb)
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None, raising=False)
    return jm


def _task(key, nb, depends_on=()):
    return {"task_key": key, "notebook_path": nb, "resolved_path": nb,
            "depends_on": list(depends_on), "run_deps": []}


def test_customization_failed_fails_job_and_blocks_dependents(monkeypatch, tmp_path):
    jm = _job_env(monkeypatch, tmp_path)
    # configured override whose replacement file is missing -> apply_override
    # raises -> the REAL process_notebook returns CUSTOMIZATION_FAILED
    (tmp_path / "pack").mkdir()
    (tmp_path / "pack" / "overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1,
        "overrides": [{"notebook": "nb_a.ipynb",
                       "replacement": "notebooks/missing_golden.ipynb"}],
    }))
    cp.load_pack(str(tmp_path / "pack"))

    job = {"job_name": "CUSTFAILJOB", "root": "", "parameters": {},
           "default_cluster": "fake-cluster",
           "execution_layers": [["taskA"], ["taskB"]],
           "tasks": [_task("taskA", "Users/p/nb_a.ipynb"),
                     _task("taskB", "Users/p/nb_b.ipynb", depends_on=["taskA"])]}
    res = asyncio.run(jm.process_job(job, _NoopSession()))

    tr = res["task_results"]
    assert tr["taskA"]["status"] == "CUSTOMIZATION_FAILED"
    # dependent is BLOCKED like any failed parent — never runs on the
    # unmigrated dependency
    assert tr["taskB"]["status"] == "BLOCKED"
    assert tr["taskB"]["blocked_by"] == "taskA"
    # summary counts it (CUSTOMIZATION_FAILED + the cascaded BLOCKED)
    assert res["total_errored"] == 2
    # exit path: the workflow launcher's verdict (job_migrate_from_workflow
    # main(): migration_failed = bool(fail or errored) -> RuntimeError ->
    # non-zero exit) must see this run as failed
    assert bool(res["total_failed"] or res["total_errored"]) is True


def test_customization_failed_trips_circuit_breaker(monkeypatch, tmp_path):
    jm = _job_env(monkeypatch, tmp_path)
    monkeypatch.setattr(jm, "CONSECUTIVE_NB_FAIL_THRESHOLD", 2)
    (tmp_path / "pack").mkdir()
    (tmp_path / "pack" / "overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1,
        "overrides": [
            {"notebook": "nb_a1.ipynb", "replacement": "notebooks/gone1.ipynb"},
            {"notebook": "nb_a2.ipynb", "replacement": "notebooks/gone2.ipynb"},
        ],
    }))
    cp.load_pack(str(tmp_path / "pack"))

    job = {"job_name": "CUSTCBJOB", "root": "", "parameters": {},
           "default_cluster": "fake-cluster",
           "execution_layers": [["taskA1"], ["taskA2"], ["taskC"]],
           "tasks": [_task("taskA1", "Users/p/nb_a1.ipynb"),
                     _task("taskA2", "Users/p/nb_a2.ipynb"),
                     _task("taskC", "Users/p/nb_c.ipynb")]}
    res = asyncio.run(jm.process_job(job, _NoopSession()))

    tr = res["task_results"]
    assert tr["taskA1"]["status"] == "CUSTOMIZATION_FAILED"
    assert tr["taskA2"]["status"] == "CUSTOMIZATION_FAILED"
    # two consecutive customization failures trip the breaker; taskC never runs
    assert tr["taskC"]["status"] == "BLOCKED"
    assert tr["taskC"]["blocked_by"] == "circuit_breaker"


# ── Fix 4: pip install-skip — pins beat importability; skip still persists ──

class _ExecSession:
    """Executes generated kernel code in-process (real files under tmp)."""
    cluster_id = "fake-cluster"

    async def execute(self, code, timeout=None):
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf):
                exec(code, {"__name__": "__probe__"})
            return {"status": "ok",
                    "outputs": [{"type": "stream", "text": buf.getvalue()}]}
        except Exception as e:
            return {"status": "error",
                    "outputs": [{"type": "error",
                                 "traceback": [f"{type(e).__name__}: {e}"]}]}


def _pip_env(monkeypatch):
    import cluster_lifecycle as cl
    installs = []

    async def _fake_poll(cluster_id, library_path, timeout=600):
        return True

    async def _fake_verify(session, import_name, attempts=12, interval=10):
        # Post-install verification is covered by dedicated tests; these tests
        # exercise the skip/pin/persistence logic only.
        return True

    async def _fake_verify_distribution(
            session, distribution_name, specifier=None,
            attempts=12, interval=10):
        return True

    monkeypatch.setattr(cl, "get_installed_libraries", lambda cluster_id: [])
    monkeypatch.setattr(cl, "_resolve_pip_name", lambda name: name)
    monkeypatch.setattr(cl, "install_library",
                        lambda cluster_id, path: installs.append(path))
    monkeypatch.setattr(cl, "_poll_library_install", _fake_poll)
    monkeypatch.setattr(cl, "_verify_import_on_kernel", _fake_verify)
    monkeypatch.setattr(
        cl, "_verify_distribution_on_kernel", _fake_verify_distribution
    )
    return cl, installs


def _req_file(tmp_path):
    return tmp_path / "resolved_requirements_runtime.txt"


def _run_install(cl, tmp_path, spec):
    job_output = str(tmp_path).replace("\\", "/")
    return asyncio.run(cl.install_missing_package(
        "fake-cluster", _ExecSession(), spec, job_output))


def test_pip_pinned_mismatch_installs(monkeypatch, tmp_path):
    cl, installs = _pip_env(monkeypatch)
    ok = _run_install(cl, tmp_path, "pytest==0.0.1")  # installed pytest != 0.0.1
    assert ok is True
    assert len(installs) == 1  # importable is NOT sufficient under a pin
    assert "pytest==0.0.1" in _req_file(tmp_path).read_text()


def test_pip_pinned_satisfied_skips_but_persists(monkeypatch, tmp_path):
    import importlib.metadata as md
    cl, installs = _pip_env(monkeypatch)
    spec = f"pytest=={md.version('pytest')}"
    ok = _run_install(cl, tmp_path, spec)
    assert ok is True
    assert installs == []  # satisfied pin -> no re-install
    assert spec in _req_file(tmp_path).read_text()  # ... but still recorded


def test_pip_pinned_metadata_failure_installs(monkeypatch, tmp_path):
    cl, installs = _pip_env(monkeypatch)
    ok = _run_install(cl, tmp_path, "no_such_dist_xyz9==1.0")
    assert ok is True
    assert len(installs) == 1  # metadata failure falls through to install
    assert "no_such_dist_xyz9==1.0" in _req_file(tmp_path).read_text()


def test_pip_unpinned_importable_skips_and_persists(monkeypatch, tmp_path):
    cl, installs = _pip_env(monkeypatch)
    ok = _run_install(cl, tmp_path, "yaml")  # importable in this env
    assert ok is True
    assert installs == []  # importable + no pin -> skip install
    # the skip must still land the requirement in job_requirements persistence
    assert "yaml" in _req_file(tmp_path).read_text()


def test_pip_unpinned_missing_installs(monkeypatch, tmp_path):
    cl, installs = _pip_env(monkeypatch)
    ok = _run_install(cl, tmp_path, "no_such_module_xyz9")
    assert ok is True
    assert len(installs) == 1
    assert "no_such_module_xyz9" in _req_file(tmp_path).read_text()
