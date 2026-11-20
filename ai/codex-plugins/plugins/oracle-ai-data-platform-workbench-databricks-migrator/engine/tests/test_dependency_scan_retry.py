"""A failed scan must never create an authoritative empty dependency list."""
import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_dag
import build_dag_from_workflow as workflow_dag
from test_build_dag_trailing_slash import _ExecutingSession


BASE = "/Workspace/export/notebooks"
MAIN = BASE + "/Users/fixture/main.ipynb"
HELPER = BASE + "/Users/fixture/helper.ipynb"
LEAF = BASE + "/Users/fixture/leaf.ipynb"


def notebook(source):
    return {"cells": [{"cell_type": "code", "source": source}]}


class ScanSession(_ExecutingSession):
    def __init__(self, failed_path=MAIN, failures=()):
        super().__init__(MAIN, notebook("%run /Users/fixture/helper"), {
            HELPER: notebook("%run /Users/fixture/leaf"), LEAF: notebook("VALUE = 42")})
        self.failed_path = failed_path
        self.failures = list(failures)
        self.scan_calls = []

    async def _execute_locked(self, code, timeout=60):
        if "runs = extract_run_targets(" in code:
            self.scan_calls.append(code)
            if f"path = {json.dumps(self.failed_path)}\n" in code and self.failures:
                failure = self.failures.pop(0)
                if isinstance(failure, Exception):
                    raise failure
                return failure
        return await super()._execute_locked(code, timeout)


@pytest.mark.parametrize("failure", [
    {"outputs": []},
    {"outputs": [{"type": "stream", "text": "not JSON"}]},
    {"outputs": [{"type": "stream", "text": "[]"}]},
    {"outputs": [{"type": "stream", "text": '{"exists":true}'}]},
    {"status": "error", "outputs": [{"type": "stream", "text": '{"exists":true,"runs":[]}'}]},
    TimeoutError("scan timed out"),
])
def test_retry_repeats_real_extraction_and_retains_dependencies(failure):
    session = ScanSession(failures=[failure])
    exists, runs = asyncio.run(build_dag.scan_external_notebook(session, MAIN))
    assert exists is True
    assert runs == ["/Users/fixture/helper.ipynb"]
    assert len(session.scan_calls) == 2
    assert session.scan_calls[0] == session.scan_calls[1]


@pytest.mark.parametrize("failed_path", [MAIN, HELPER])
def test_failed_root_or_nested_scan_blocks_manifest_construction(failed_path):
    session = ScanSession(failed_path, failures=[{"outputs": []}, {"outputs": []}])
    with pytest.raises(RuntimeError, match="dependency extraction failed after 2 attempts"):
        asyncio.run(workflow_dag.discover_run_deps_for_tasks(
            session, [{"task_key": "main", "notebook_path": MAIN}], BASE, BASE))
    assert len([s for s in session.scan_calls
                if f"path = {json.dumps(failed_path)}\n" in s]) == 2


def test_recovered_scan_builds_complete_transitive_manifest():
    session = ScanSession(failures=[{"outputs": []}])
    deps = asyncio.run(workflow_dag.discover_run_deps_for_tasks(
        session, [{"task_key": "main", "notebook_path": MAIN}], BASE, BASE))
    assert deps["main"][0]["path"] == HELPER
    assert deps["main"][0]["nested_deps"][0]["path"] == LEAF
    assert deps["main"][0]["nested_deps"][0]["exists"] is True


@pytest.mark.parametrize("exists", [True, False])
def test_successful_empty_or_absent_scan_remains_distinct(monkeypatch, exists):
    async def run(*args, **kwargs):
        return {"outputs": [{"type": "stream", "text": json.dumps({"exists": exists, "runs": []})}]}
    monkeypatch.setattr(build_dag, "run", run)
    assert asyncio.run(build_dag.scan_external_notebook(object(), MAIN)) == (exists, [])
