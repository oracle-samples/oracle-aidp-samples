"""Regression coverage for fixup_cell save-back semantics.

Incident: fixup_cell repaired an upstream cell in the live kernel, but the
uploaded notebook was built from stale source. These tests pin persistence,
long-cell replay, task boundaries, dependency notebooks, and protected Ray
cells without retaining full notebook/output objects in job-wide history.

Child incident (2026-09-30, supplied UW_PAYLOAD_RUN_JOB1 log): model_selection
needed Idisplay, but child replay was rejected and the caller exhausted retries.
Scope: active invocation -> pending source -> existing save/cache pipeline.
Matrix: active/completed/cross-task owners; supplied/discovered repairs;
successful/failed upstream cells; replay-only/caller edits; normal/early/aborted
completion; successful/failed save; repeated/nested invocations. Completed
child direct replays and authoritative Ray changes remain unsupported and fail explicitly.
Completed shared-context dependencies use a separate, source-verified conversion
invocation, never a reopened replay owner.
"""
from __future__ import annotations

import asyncio
import ast
import contextlib
import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am  # noqa: E402
import job_migrate as jm  # noqa: E402,F401 - import registers replay callback


@pytest.fixture
def child_replay_flow(monkeypatch, tmp_path):
    """Real child loop, replay tool, and emitted save scripts; local kernel/LLM."""
    import cluster_lifecycle
    from types import ModuleType

    # IPython is supplied by AIDP, not a migration-host dependency.
    ipython = ModuleType("IPython")
    display_module = ModuleType("IPython.display")
    display_module.display = lambda _: "rendered"
    monkeypatch.setitem(sys.modules, "IPython", ipython)
    monkeypatch.setitem(sys.modules, "IPython.display", display_module)

    original = "# CHILD_REPLAY_DEF\ndef model_selection():\n    return display('frame')\n"
    repaired = original.replace(
        "    return display('frame')",
        "    # return display('frame')\n"
        "    # Oracle tool modification: IPython renderer for pandas\n"
        "    return Idisplay('frame')",
    )
    caller = "# CHILD_REPLAY_CALL\nresult = model_selection()\n"
    messages, repairs = [], []

    class Log:
        def log(self, message):
            messages.append(message)

    class Kernel:
        cluster_id = "fixture-cluster"

        def __init__(self):
            self.executed = []
            self.saves = 0
            self.fail_save = False
            def display(_):
                raise AttributeError("DataFrame has no attribute isStreaming")
            self.namespace = {"display": display, "Idisplay": lambda _: "rendered"}

        async def execute(self, code, **kwargs):
            self.executed.append(code)
            output = io.StringIO()
            try:
                with contextlib.redirect_stdout(output):
                    if ("__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code
                            or "__AIDP_DEPENDENCY_REPAIR_MATCH__" in code):
                        if "all_cells = payload.get" in code:
                            self.saves += 1
                            if self.fail_save:
                                raise OSError("fixture save failed")
                        exec(code, {})
                    elif "# CHILD_REPLAY" in code:
                        exec(code, self.namespace)
                    elif "_aidp_percent_run_iso_stack" in code:
                        print(jm._ISO_OK)
                    elif "aidputils.notebook.exit('done')" in code:
                        print("__AIDP_NOTEBOOK_EXIT__ value=done")
            except Exception as exc:
                return {"status": "error", "outputs": [
                    {"type": "error", "ename": type(exc).__name__, "evalue": str(exc),
                     "traceback": [str(exc)]}]}
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": output.getvalue()}]}

    async def summarize(*args, **kwargs):
        return "fixture cell"

    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    async def fix(code, *args, session=None, **kwargs):
        target = next(e for e in reversed(am._cell_history)
                      if "# CHILD_REPLAY_DEF" in e["replay_code"])
        result = await am._handle_tool_call("fixup_cell", {
            "start_index": target["index"], "fixed_code": repaired,
            "why": "display on pandas fails inside the upstream function",
        }, session=session)
        repairs.append(result)
        return code  # caller needs no edit: only the upstream source changed

    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", am._cell_history)
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)

    def notebook(name="child", sources=None):
        path = tmp_path / f"{name}.ipynb"
        nb = {"cells": [{"cell_type": "code", "source": [src], "metadata": {},
                         "outputs": [], "execution_count": None}
                        for src in (sources if sources is not None else [original, caller])],
              "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
        path.write_text(json.dumps(nb))
        jm._notebook_content_cache[str(path)] = json.dumps(nb)
        return path

    async def run(path, kernel):
        return await jm._inline_child_notebook(
            str(path), kernel, Log(), "fixture", "", "", "", {})

    return notebook, run, Kernel, original, repaired, caller, messages, repairs


@pytest.mark.parametrize("early_exit", [False, True])
@pytest.mark.parametrize("fail_save", [False, True])
def test_active_child_replay_reaches_saved_artifact_and_cache(child_replay_flow, early_exit, fail_save):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    sources = [original, caller]
    if early_exit:
        sources += ["aidputils.notebook.exit('done')", "TAIL_PRESERVED = True"]
    path = notebook(sources=sources)
    before_cache = jm._notebook_content_cache[str(path)]
    kernel = Kernel()
    kernel.fail_save = fail_save

    result = asyncio.run(run(path, kernel))

    assert len(repairs) == 1
    assert repairs[0].startswith("OK:"), repairs
    assert kernel.namespace["result"] == "rendered"
    assert kernel.saves == 1  # even though the caller's source did not change
    owners = [e["_child_replay_owner"] for e in am._cell_history
              if e.get("_child_replay_owner") is not None]
    assert owners and all(not o.active and o.sources is None for o in owners)
    saved = json.loads(path.read_text())
    saved_sources = ["".join(c["source"]) for c in saved["cells"]]
    if fail_save:
        assert result["status"] == "error"
        assert result["kind"] == "CHILD_PERSISTENCE"
        assert am._fixup_persistence_failures
        assert saved_sources == sources
        assert jm._notebook_content_cache[str(path)] == before_cache
    else:
        assert result["status"] == "ok", result
        assert result["did_exit"] is early_exit
        assert not am._fixup_persistence_failures
        assert saved_sources[0].endswith(repaired.rstrip())
        assert "from IPython.display import display as Idisplay" in saved_sources[0]
        assert saved_sources[1:] == [s.rstrip() for s in sources[1:]]
        cached = json.loads(jm._notebook_content_cache[str(path)])
        assert ["".join(c["source"]) for c in cached["cells"]] == saved_sources
        # Fresh kernel can run the delivered definition/caller without fixup.
        fresh = Kernel()
        exec(saved_sources[0], fresh.namespace)
        exec(saved_sources[1], fresh.namespace)
        assert "result" in fresh.namespace


def test_repeated_child_invocations_and_completed_owner_rejection(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook()
    assert asyncio.run(run(path, Kernel()))["status"] == "ok"
    first = am._cell_history[0]
    second_kernel = Kernel()
    assert asyncio.run(run(path, second_kernel))["status"] == "ok"
    assert len(repairs) == 1  # second invocation consumes the repaired cache
    second = am._cell_history[2]
    assert first["_child_replay_owner"] is not second["_child_replay_owner"]
    before_sources = path.read_text()
    before_history = len(am._cell_history)
    kernel = Kernel()
    result = _fix(0, repaired + "\nAFTER_COMPLETION = True", kernel)
    assert result.startswith("PARTIAL:")
    assert "no active child source owner" in result
    assert kernel.executed == []
    assert path.read_text() == before_sources
    assert len(am._cell_history) == before_history  # unexecuted tail retained


def test_completed_dependency_repair_persists_and_fresh_caller_passes(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook(sources=[original])  # definition completes; caller fails later
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    assert asyncio.run(kernel.execute(caller))["status"] == "error"
    target = am._cell_history[0]
    fixed = target["replay_code"].replace("return display(", "return Idisplay(")
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": fixed, "why": "completed helper uses Spark display",
    }, session=kernel))
    assert result.startswith("OK:"), result
    assert kernel.saves == 1
    assert not target["_child_replay_owner"].active  # old guard remains intact
    assert asyncio.run(kernel.execute(caller))["status"] == "ok"
    saved = json.loads(path.read_text())
    fresh = Kernel()
    exec("".join(saved["cells"][0]["source"]), fresh.namespace)
    exec(caller, fresh.namespace)
    assert fresh.namespace["result"] == "rendered"
    assert "return Idisplay(" in jm._notebook_content_cache[str(path)]


@pytest.mark.parametrize("helper", ["get_dp_token", "get_gocd_headers"])
@pytest.mark.parametrize("redaction", ["free_text", "constructor"])
def test_completed_redacted_dependency_repairs_secret_signature(
    child_replay_flow, monkeypatch, helper, redaction,
):
    """c969c1c1: definitions pass; later callers expose scope= TypeError."""
    from types import SimpleNamespace
    notebook, run, Kernel, *_ = child_replay_flow
    token = "xox" + "b-12345678901234567890-fixture"
    comment = (f"# old credential: {token}\n" if redaction == "free_text" else
               f"from slack_sdk import WebClient\nclient = WebClient(token={token!r})\n")
    monkeypatch.setitem(sys.modules, "slack_sdk", SimpleNamespace(
        WebClient=lambda **kwargs: SimpleNamespace(token=kwargs["token"])))
    source = ("# CHILD_REPLAY_DEF\n" + f"def {helper}():\n"
              '    return aidputils.secrets.get(scope="fixture", key="token")\n')
    caller = f"# CHILD_REPLAY_CALL\nresult = {helper}()\n"

    def kernel():
        instance = Kernel()
        instance.namespace["aidputils"] = SimpleNamespace(
            secrets=SimpleNamespace(get=lambda name, key: "fixture-result"))
        return instance

    path = notebook(sources=["# CHILD_REPLAY_AUTH\n" + comment, source])
    session = kernel()
    assert asyncio.run(run(path, session))["status"] == "ok"
    failed = asyncio.run(session.execute(caller))
    assert failed["status"] == "error"
    assert failed["outputs"][0]["ename"] == "TypeError"
    target = am._cell_history[1]
    assert target["_child_replay_owner"].completed_repair is not None
    saved_cells = ["".join(c["source"]) for c in json.loads(path.read_text())["cells"]]
    saved = saved_cells[1]
    assert token not in path.read_text()
    assert token in jm._notebook_content_cache[str(path)]
    assert target["replay_code"] == saved
    assert target["_child_replay_owner"].source_hashes == jm._child_source_hashes(saved_cells)
    fixed = target["replay_code"].replace(
        '    return aidputils.secrets.get(scope="fixture", key="token")',
        '    # Original: return aidputils.secrets.get(scope="fixture", key="token")\n'
        '    # Oracle tool modification: native secret lookup takes name\n'
        '    return aidputils.secrets.get("fixture", key="token")')
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": target["index"], "fixed_code": fixed,
        "why": "scope is unsupported by the native secret API",
    }, session=session))
    assert result.startswith("OK:"), result
    assert asyncio.run(session.execute(caller))["status"] == "ok"
    durable = "".join(json.loads(path.read_text())["cells"][1]["source"])
    assert token not in path.read_text()
    assert token in jm._notebook_content_cache[str(path)]
    if redaction == "constructor":
        assert session.namespace["client"].token == token
    fresh = kernel()
    exec(durable, fresh.namespace)
    assert asyncio.run(fresh.execute(caller))["status"] == "ok"
    assert fresh.namespace["result"] == "fixture-result"
    assert not am._fixup_persistence_failures


def test_completed_dependency_rejects_repair_of_redacted_authentication_cell(child_replay_flow):
    notebook, run, Kernel, original, repaired, *_ = child_replay_flow
    path = notebook(sources=["# WebClient(token='fixture-secret')\n" + original])
    session = Kernel()
    assert asyncio.run(run(path, session))["status"] == "ok"
    before = (list(session.executed), path.read_text())
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": repaired, "why": "repair a credential-bearing cell",
    }, session=session))
    assert result.startswith("ERROR:") and "Manual source repair is required" in result
    assert (session.executed, path.read_text()) == before


@pytest.mark.parametrize("rejected_first", [False, True])
def test_completed_dependency_rejects_table_swap_before_execution(child_replay_flow, rejected_first):
    notebook, run, Kernel, *_ = child_replay_flow
    source = "# CHILD_REPLAY_DEF\ndef read_orders():\n    return spark.read.table('sales.orders')\n"
    path = notebook(sources=[source])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    disk, cache = path.read_text(), jm._notebook_content_cache[str(path)]
    candidate = am._cell_history[0]["replay_code"].replace(
        "table('sales.orders')", "parquet('oci://fixture@ns/orders/')")
    before_exec = list(kernel.executed)
    if rejected_first:
        assert _fix(0, candidate, kernel).startswith("PARTIAL:")
    with pytest.raises(am.FixupPersistenceError, match="table-to-path conversion"):
        asyncio.run(am._handle_tool_call("repair_dependency", {
            "index": 0, "fixed_code": candidate, "why": "negative source substitution",
        }, session=kernel))
    assert kernel.executed == before_exec
    assert kernel.saves == 0
    assert path.read_text() == disk
    assert jm._notebook_content_cache[str(path)] == cache


def test_completed_dependency_hash_anchor_rejects_history_drift(child_replay_flow):
    notebook, run, Kernel, original, repaired, *_ = child_replay_flow
    path = notebook(sources=[original])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    disk, cache = path.read_text(), jm._notebook_content_cache[str(path)]
    # Corrupted history must not become a replacement for the saved-source anchor.
    am._cell_history[0]["replay_code"] += "\n# unsaved history change"
    before_exec = list(kernel.executed)
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": repaired, "why": "negative history drift",
    }, session=kernel))
    assert "history source no longer matches" in result
    assert kernel.executed == before_exec
    assert kernel.saves == 0
    assert path.read_text() == disk
    assert jm._notebook_content_cache[str(path)] == cache


def test_rejected_completed_fixup_does_not_disable_legitimate_repair(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, *_ = child_replay_flow
    path = notebook(sources=[original])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    accepted = am._cell_history[0]["replay_code"]
    before_exec = list(kernel.executed)
    for _ in range(2):
        assert _fix(0, accepted + "\n# benign candidate", kernel).startswith("PARTIAL:")
        assert kernel.executed == before_exec
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": repaired, "why": "use supported completed-child repair",
    }, session=kernel))
    assert result.startswith("OK:"), result
    assert kernel.saves == 1
    fresh = Kernel()
    exec("".join(json.loads(path.read_text())["cells"][0]["source"]), fresh.namespace)
    exec(caller, fresh.namespace)
    assert fresh.namespace["result"] == "rendered"


@pytest.mark.parametrize("initial_failure", [False, True])
@pytest.mark.parametrize("recover_again", [False, True])
def test_child_replay_finalization_uses_latest_slot_outcome(
        child_replay_flow, monkeypatch, initial_failure, recover_again):
    notebook, run, Kernel, *_ = child_replay_flow
    source = "# CHILD_REPLAY_FAILED\nvalue = 1 / 0" if initial_failure else "# CHILD_REPLAY_FAILED\nvalue = 0"
    fixed = "# CHILD_REPLAY_FAILED\n# Original value assignment repaired\nvalue = 42"
    caller = "# CHILD_REPLAY_CALL\nresult = ready"
    path = notebook(sources=[source, caller])
    disk, cache = path.read_text(), jm._notebook_content_cache[str(path)]

    class ReplayKernel(Kernel):
        fail_slot = False
        async def execute(self, code, **kwargs):
            if self.fail_slot and "# CHILD_REPLAY_FAILED" in code:
                self.executed.append(code)
                return {"status": "error", "outputs": [
                    {"type": "error", "ename": "ValueError", "evalue": "later replay failed"}]}
            return await super().execute(code, **kwargs)

    async def fix(code, *args, session=None, **kwargs):
        if "# CHILD_REPLAY_CALL" not in code:
            return code
        assert (await am._handle_tool_call("fixup_cell", {
            "start_index": 0, "fixed_code": fixed, "why": "repair prerequisite",
        }, session=session)).startswith("OK:")
        session.fail_slot = True
        assert (await am._handle_tool_call("fixup_cell", {
            "start_index": 0, "why": "rebuild same prerequisite",
        }, session=session)).startswith("PARTIAL:")
        session.fail_slot = False
        if recover_again:
            assert (await am._handle_tool_call("fixup_cell", {
                "start_index": 0, "why": "prerequisite now executes successfully",
            }, session=session)).startswith("OK:")
        session.namespace["ready"] = True
        return code

    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    kernel = ReplayKernel()
    result = asyncio.run(run(path, kernel))
    owner = am._cell_history[0]["_child_replay_owner"]
    if recover_again:
        assert result["status"] == "ok", result
        assert kernel.saves == 1
        assert owner.completed_repair is not None
        assert "value = 42" in path.read_text()
    else:
        assert result["status"] == "error", result
        assert result["kind"] == "CHILD_PERSISTENCE"
        assert kernel.saves == 0
        assert path.read_text() == disk
        assert jm._notebook_content_cache[str(path)] == cache
        assert owner.completed_repair is None


def test_completed_repair_history_tracks_finalizer_imports(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, *_ = child_replay_flow
    path = notebook(sources=[original])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    assert asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": repaired, "why": "first display repair",
    }, session=kernel)).startswith("OK:")
    saved_source = "".join(json.loads(path.read_text())["cells"][0]["source"])
    assert "from IPython.display import display as Idisplay" in saved_source
    latest = am._cell_history[-1]
    assert latest["replay_code"] == saved_source
    second = saved_source.replace("Idisplay('frame')", "Idisplay('second frame')")
    assert asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": latest["index"], "fixed_code": second, "why": "second legitimate helper repair",
    }, session=kernel)).startswith("OK:")
    assert kernel.saves == 2
    delivered = "".join(json.loads(path.read_text())["cells"][0]["source"])
    assert delivered == am._cell_history[-1]["replay_code"]
    fresh = Kernel()
    exec(delivered, fresh.namespace)
    exec(caller, fresh.namespace)
    assert fresh.namespace["result"] == "rendered"
    # Finalization alignment must not turn later history corruption into trust.
    am._cell_history[-1]["replay_code"] += "\n# unsaved drift"
    before = list(kernel.executed)
    rejected = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": am._cell_history[-1]["index"], "fixed_code": second, "why": "negative drift",
    }, session=kernel))
    assert "history source no longer matches" in rejected
    assert kernel.executed == before
    assert kernel.saves == 2


@pytest.mark.parametrize("failed_replay", [False, True])
def test_replay_recovery_cannot_hide_another_failed_child_cell(child_replay_flow, monkeypatch, failed_replay):
    notebook, run, Kernel, original, repaired, caller, *_ = child_replay_flow
    failing = "# CHILD_REPLAY_FAILED\nmissing_value = 1 / 0"
    path = notebook(sources=[original, failing, caller])
    disk, cache = path.read_text(), jm._notebook_content_cache[str(path)]
    calls = []
    class ReplayKernel(Kernel):
        fail_definition = False
        async def execute(self, code, **kwargs):
            if self.fail_definition and "# CHILD_REPLAY_DEF" in code and "return display(" in code:
                self.executed.append(code)
                return {"status": "error", "outputs": [
                    {"type": "error", "ename": "ValueError", "evalue": "replay failed"}]}
            return await super().execute(code, **kwargs)
    async def fix(code, *args, session=None, **kwargs):
        if "# CHILD_REPLAY_CALL" not in code:
            return code
        if failed_replay:
            session.fail_definition = True
            calls.append(await am._handle_tool_call("fixup_cell", {
                "start_index": 0, "why": "unchanged replay fails in this kernel",
            }, session=session))
            session.fail_definition = False
        # Repair the definition, but leave the unrelated middle cell failing.
        # The caller now works; that must not certify the entire saved child.
        calls.append(await am._handle_tool_call("fixup_cell", {
            "start_index": 0, "fixed_code": repaired, "why": "repair only the definition",
        }, session=session))
        return code
    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    kernel = ReplayKernel()
    result = asyncio.run(run(path, kernel))
    assert calls and all(c.startswith("PARTIAL:") for c in calls)
    assert result["status"] == "error", result
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert kernel.saves == 0
    assert path.read_text() == disk
    assert jm._notebook_content_cache[str(path)] == cache
    assert all(e["_child_replay_owner"].completed_repair is None for e in am._cell_history)


@pytest.mark.parametrize("failure", ["save", "disk_drift", "cache_drift", "cross_task", "wrong_session"])
@pytest.mark.parametrize("redacted", [False, True])
def test_completed_dependency_repair_fails_closed(child_replay_flow, monkeypatch, failure, redacted):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    if redacted:
        sources = ["# CHILD_REPLAY_AUTH\n# WebClient(token='fixture-secret')\n", original]
    else:
        sources = [original]
    path = notebook(sources=sources)
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    target = am._cell_history[-1]
    before_disk = path.read_text()
    before_cache = jm._notebook_content_cache[str(path)]
    fixed = target["replay_code"].replace("return display(", "return Idisplay(")
    if failure == "save":
        kernel.fail_save = True
    elif failure == "disk_drift":
        path.write_text(before_disk.replace("return display(", "return changed("))
        before_disk = path.read_text()
    elif failure == "cache_drift":
        jm._notebook_content_cache[str(path)] = before_cache.replace("return display(", "return changed(")
        before_cache = jm._notebook_content_cache[str(path)]
    elif failure == "cross_task":
        monkeypatch.setattr(am, "_current_task_key", "other-task")
    else:
        kernel = Kernel()
    async def repair():
        return await am._handle_tool_call("repair_dependency", {
            "index": target["index"], "fixed_code": fixed, "why": "fixture failure path",
        }, session=kernel)
    if failure == "cross_task":
        assert asyncio.run(repair()).startswith("ERROR:")
    else:
        with pytest.raises(am.FixupPersistenceError):
            asyncio.run(repair())
        assert am._fixup_persistence_failures
    assert path.read_text() == before_disk
    assert jm._notebook_content_cache[str(path)] == before_cache


@pytest.mark.parametrize("initial_noop", [False, True])
@pytest.mark.parametrize("parallel_submit", [False, True])
def test_model_repairs_completed_nested_definition_not_the_caller(
        child_replay_flow, monkeypatch, initial_noop, parallel_submit):
    from types import SimpleNamespace as NS
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    helper = notebook("helper", [original])
    parent = notebook("parent", ["%run ./helper", caller])
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        repair_turn = 1 + int(initial_noop)
        if initial_noop and len(calls) == 1:
            return NS(content=[NS(type="tool_use", name="submit_code", id="noop", input={"code": caller})],
                      stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
        if len(calls) == repair_turn:
            if initial_noop:
                assert "no completed dependency repair was saved" in str(kwargs["messages"])
            target = next(e for e in reversed(am._cell_history)
                          if e["notebook_path"].endswith("helper.ipynb"))
            assert not target["_child_replay_owner"].active
            source = target["replay_code"].replace("return display(", "return Idisplay(")
            tool, payload = "repair_dependency", {"index": target["index"],
                "fixed_code": source, "why": "completed helper, read-only definition"}
        else:
            tool, payload = "submit_code", {"code": caller}
        blocks = [NS(type="tool_use", name=tool, id=f"tool-{len(calls)}", input=payload)]
        if parallel_submit and len(calls) == repair_turn:
            # Submission deliberately comes FIRST: order must not skip save-back.
            blocks.insert(0, NS(type="tool_use", name="submit_code", id="parallel", input={"code": caller}))
        return NS(content=blocks,
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(jm, "call_fix", am.call_fix)
    result = asyncio.run(run(parent, Kernel()))
    assert result["status"] == "ok", result
    expected_calls = 2 + int(initial_noop) - int(parallel_submit)
    assert len(calls) == expected_calls
    assert "return Idisplay(" in helper.read_text()
    assert "result = model_selection()" in parent.read_text()
    # Clear caches/history: consume the saved dependency in a new kernel.
    jm._notebook_content_cache.clear()
    for p in (parent, helper):
        jm._notebook_content_cache[str(p)] = p.read_text()
    fresh = Kernel()
    assert asyncio.run(run(parent, fresh))["status"] == "ok"
    assert fresh.namespace["result"] == "rendered"
    assert len(calls) == expected_calls  # no additional AI repair needed


@pytest.mark.parametrize("capability", ["current", "foreign", "consumed", "none"])
def test_unchanged_caller_feedback_is_bounded_and_task_owned(monkeypatch, capability):
    from types import SimpleNamespace as NS
    caller = "result = helper()"
    history = [] if capability == "none" else [{
        "index": 0, "_task_key": "foreign" if capability == "foreign" else "task",
        "_child_replay_owner": NS(completed_repair=None if capability == "consumed" else object()),
    }]
    monkeypatch.setattr(am, "_cell_history", history)
    monkeypatch.setattr(am, "_current_task_key", "task")
    calls = []
    async def unchanged(*args, **kwargs):
        calls.append((args, kwargs))
        return caller, {}
    monkeypatch.setattr(am, "call_opus_with_tools", unchanged)
    assert asyncio.run(am.call_fix(caller, "helper failed", [], 1, session=object())) == caller
    assert len(calls) == 1  # one conversation, never a second context-free call
    assert calls[0][1]["repair_caller"] == (caller if capability == "current" else None)


@pytest.mark.parametrize("validator", [None, lambda code: bool(code)])
def test_parallel_dependency_save_failure_prevents_caller_submission(monkeypatch, validator):
    from types import SimpleNamespace as NS
    reply = NS(content=[
        NS(type="tool_use", name="submit_code", id="submit", input={"code": "helper()"}),
        NS(type="tool_use", name="repair_dependency", id="repair", input={"index": 0}),
    ], stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=lambda **kw: reply)))
    calls = []
    async def failed(name, payload, **kw):
        calls.append(name)
        am._record_fixup_persistence_failure("save unavailable")
        return "ERROR: save unavailable"
    monkeypatch.setattr(am, "_handle_tool_call", failed)
    with pytest.raises(am.FixupPersistenceError, match="save unavailable"):
        asyncio.run(am.call_opus_with_tools("system", "fix", session=object(), code_validator=validator))
    assert calls == ["repair_dependency"]


@pytest.mark.parametrize('first_stop_reason', ['tool_use', 'end_turn'])
def test_recoverable_paired_repair_error_does_not_bypass_code_validator(
        monkeypatch, first_stop_reason):
    from types import SimpleNamespace as NS
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        blocks = ([
            NS(type='tool_use', name='submit_code', id='early', input={'code': 'helper()'}),
            NS(type='tool_use', name='fixup_cell', id='bad-index',
               input={'start_index': 999, 'fixed_code': 'fixed()'}),
        ] if len(calls) == 1 else [
            NS(type='tool_use', name='submit_code', id='corrected',
               input={'code': 'fixed()'}),
        ])
        return NS(content=blocks, stop_reason=(first_stop_reason if len(calls) == 1
                                               else 'tool_use'),
                  usage=NS(input_tokens=1, output_tokens=1))

    async def invalid_index(name, payload, **kwargs):
        assert name == 'fixup_cell'
        return 'ERROR: start_index 999 out of range. Use get_cell_history first.'

    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(am, '_handle_tool_call', invalid_index)
    submitted, _ = asyncio.run(am.call_opus_with_tools(
        'system', 'fix', session=object(), code_validator=bool,
        repair_caller='helper()', max_tool_rounds=2))
    assert submitted == 'fixed()'
    assert len(calls) == 2
    assert not am._fixup_persistence_failures


def test_completed_repair_rechecks_disk_at_save_boundary(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook(sources=[original])
    class DriftKernel(Kernel):
        drift = False
        async def execute(self, code, **kwargs):
            if self.drift and "all_cells = payload.get" in code:
                self.drift = False
                path.write_text(path.read_text().replace("return display(", "return newer("))
            return await super().execute(code, **kwargs)
    kernel = DriftKernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    original_cache = jm._notebook_content_cache[str(path)]
    kernel.drift = True
    with pytest.raises(am.FixupPersistenceError, match="changed during repair"):
        asyncio.run(am._handle_tool_call("repair_dependency", {
            "index": 0, "fixed_code": am._cell_history[0]["replay_code"].replace("return display(", "return Idisplay("),
            "why": "fixture drift during execution",
        }, session=kernel))
    assert "return newer(" in path.read_text()
    assert jm._notebook_content_cache[str(path)] == original_cache


@pytest.mark.parametrize("exclude", [("repair_dependency",), ("repair_dependency", "submit_code")])
def test_parallel_repair_does_not_bypass_excluded_tools(monkeypatch, exclude):
    from types import SimpleNamespace as NS
    replies = iter([
        NS(content=[
            NS(type="tool_use", name="submit_code", id="submit", input={"code": "helper()"}),
            NS(type="tool_use", name="repair_dependency", id="repair", input={"index": 0}),
        ], stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1)),
        NS(content=[NS(type="text", text="unavailable")], stop_reason="end_turn",
           usage=NS(input_tokens=1, output_tokens=1)),
    ])
    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=lambda **kw: next(replies))))
    async def unexpected(*args, **kwargs):
        pytest.fail("An excluded dependency repair must not execute")
    monkeypatch.setattr(am, "_handle_tool_call", unexpected)
    result, _ = asyncio.run(am.call_opus_with_tools(
        "system", "check", session=object(), exclude_tools=exclude))
    assert result == ("unavailable" if "submit_code" in exclude else "helper()")


@pytest.mark.parametrize("provider,load", [
    ("hudi", 'f"{base_path}/*"'), ("org.apache.hudi", 'base_path + "/*"'),
])
def test_completed_hudi_helper_repair_keeps_partition_branch(child_replay_flow, monkeypatch, provider, load):
    from types import SimpleNamespace as NS
    notebook, run, Kernel, _, _, _, messages, repairs = child_replay_flow
    source = ('# CHILD_REPLAY_DEF\ndef read_hudi(spark, base_path, partition=None):\n'
              f'    reader = spark.read.format({provider!r}).option("hoodie.datasource.read.basepath", base_path)\n'
              '    if partition is not None:\n        return reader.load(partition)\n'
              f'    return reader.load({load})\n')
    fixed = source.replace(f'    return reader.load({load})',
                           f'    # Original: return reader.load({load})\n'
                           '    # Oracle tool modification: use declared Hudi root\n'
                           '    return reader.load(base_path)')
    caller = '# CHILD_REPLAY_CALL\nresult = read_hudi(spark, "/fixture/hudi")'
    class Reader:
        def format(self, value):
            assert value == provider
            return self
        def option(self, key, value):
            assert (key, value) == ('hoodie.datasource.read.basepath', '/fixture/hudi')
            return self
        def load(self, path):
            if path.endswith('/*'):
                raise RuntimeError('HoodieException: Error checking path: date=20200413_$folder$')
            return path
    helper = notebook('helper', [source])
    parent = notebook('parent', ['%run ./helper', caller])
    fix_calls = []
    async def fix(code, error, *args, session=None, **kwargs):
        fix_calls.append(error)
        assert 'folder$' in error
        target = next(e for e in reversed(am._cell_history) if e['notebook_path'].endswith('helper.ipynb'))
        result = await am._handle_tool_call('repair_dependency', {
            'index': target['index'], 'fixed_code': fixed, 'why': 'Hudi declared-root marker failure',
        }, session=session)
        messages.append(result)
        assert result.startswith('OK:'), result
        return code
    monkeypatch.setattr(jm, 'call_fix', fix)
    kernel = Kernel()
    kernel.namespace['spark'] = NS(read=Reader())
    assert asyncio.run(run(parent, kernel))['status'] == 'ok'
    assert fix_calls, messages
    saved = ''.join(json.loads(helper.read_text())['cells'][0]['source'])
    assert 'return reader.load(partition)' in saved
    assert 'return reader.load(base_path)' in saved, '\n'.join(messages)
    namespace = {'spark': NS(read=Reader())}
    exec(saved, namespace)
    exec(caller, namespace)
    assert namespace['result'] == '/fixture/hudi'
    assert namespace['read_hudi'](namespace['spark'], '/fixture/hudi', '/fixture/hudi/date=1') == '/fixture/hudi/date=1'


def test_completed_repair_rejects_obsolete_invocation(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook(sources=[original])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    before = list(kernel.executed)
    result = asyncio.run(am._handle_tool_call("repair_dependency", {
        "index": 0, "fixed_code": repaired, "why": "older invocation may have different parameters",
    }, session=kernel))
    assert "latest invocation" in result
    assert kernel.executed == before


def test_completed_repair_cannot_save_partial_child(child_replay_flow, monkeypatch):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook(sources=[original])
    kernel = Kernel()
    assert asyncio.run(run(path, kernel))["status"] == "ok"
    before = path.read_text()
    async def unchanged(code, *args, **kwargs):
        return code
    monkeypatch.setattr(jm, 'call_fix', unchanged)
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 1)
    # Valid Python that fails while re-executing the repaired dependency.
    broken = am._cell_history[0]['replay_code'] + '\nraise RuntimeError("repair execution failed")'
    with pytest.raises(am.FixupPersistenceError, match='failed cells'):
        asyncio.run(am._handle_tool_call('repair_dependency', {
            'index': 0, 'fixed_code': broken, 'why': 'negative partial conversion',
        }, session=kernel))
    assert path.read_text() == before
    assert kernel.saves == 0


def test_nested_child_owners_keep_repairs_in_their_own_artifacts(child_replay_flow, monkeypatch):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    inner = notebook("inner")
    outer_original = original.replace("model_selection", "outer_selection")
    outer_caller = caller.replace("model_selection", "outer_selection")
    outer = notebook("outer", [outer_original, "%run ./inner", outer_caller])
    owners = []

    async def fix(code, error, context, attempt, session=None, notebook_path="", **kwargs):
        if notebook_path == str(inner):
            assert outer_original.rstrip() in context
            assert original.rstrip() in context
        else:
            # The later outer failure must see the inner definition as repaired
            # by replay, not a snapshot taken on entry to the outer invocation.
            # The durable cell now also includes the finalizer-injected import.
            assert any(repaired.rstrip() in source for source in context)
            assert all(original.rstrip() not in source for source in context)
        entry = next(e for e in am._cell_history
                     if e["notebook_path"] == notebook_path and "# CHILD_REPLAY_DEF" in e["replay_code"])
        replacement = repaired if notebook_path == str(inner) else repaired.replace("model_selection", "outer_selection")
        owners.append(entry["_child_replay_owner"])
        result = await am._handle_tool_call("fixup_cell", {
            "start_index": entry["index"], "fixed_code": replacement, "why": "nested display",
        }, session=session)
        assert result.startswith("OK:"), result
        assert "durable save-back pending" in result
        return code

    monkeypatch.setattr(jm, "call_fix", fix)
    result = asyncio.run(run(outer, Kernel()))
    assert result["status"] == "ok", result
    assert len(owners) == 2 and owners[0] is not owners[1], "\n".join(messages)
    for path, name in [(inner, "model_selection"), (outer, "outer_selection")]:
        saved = json.loads(path.read_text())
        definition = "".join(saved["cells"][0]["source"])
        assert f"def {name}():" in definition
        assert "return Idisplay('frame')" in definition
        assert not owners[0].active and not owners[1].active
    assert not am._fixup_persistence_failures


@pytest.mark.parametrize("supplied", [False, True])
def test_failed_child_cell_can_be_repaired_by_later_consumer(child_replay_flow, monkeypatch, supplied):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    failing = "# CHILD_REPLAY_FAILED\nmissing_value = 1 / 0"
    fixed = "# CHILD_REPLAY_FAILED\n# missing_value = 1 / 0\nmissing_value = 42"
    consumer = "# CHILD_REPLAY_CALL\nresult = missing_value"
    path = notebook(sources=[failing, consumer])
    replaying = False

    async def fix(code, *args, session=None, **kwargs):
        nonlocal replaying
        if "# CHILD_REPLAY_CALL" not in code:
            return fixed if replaying else code
        replaying = True
        result = await am._handle_tool_call("fixup_cell", {
            "start_index": 0, "fixed_code": fixed if supplied else None, "why": "failed prerequisite",
        }, session=session)
        assert result.startswith("OK:"), result
        return code

    monkeypatch.setattr(jm, "call_fix", fix)
    kernel = Kernel()
    result = asyncio.run(run(path, kernel))
    assert result["status"] == "ok", result
    assert result["cells_ok"] == 2
    assert kernel.namespace["result"] == 42
    assert all(e["status"] == "ok" for e in am._cell_history)
    assert "missing_value = 42" in "".join(json.loads(path.read_text())["cells"][0]["source"])
    assert not am._fixup_persistence_failures


@pytest.mark.parametrize("abort", ["exception", "cancel"])
def test_child_owner_closes_when_repair_is_interrupted(child_replay_flow, monkeypatch, abort):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook()
    original_cache = jm._notebook_content_cache[str(path)]
    class InterruptedKernel(Kernel):
        async def execute(self, code, **kwargs):
            result = await super().execute(code, **kwargs)
            if abort == "cancel" and "# CHILD_REPLAY_CALL" in code and result["status"] == "ok":
                raise asyncio.CancelledError()
            return result

    async def summarize(code, *args, **kwargs):
        if abort == "exception" and "# CHILD_REPLAY_CALL" in code:
            raise RuntimeError("interrupted after caller recovery")
        return "fixture cell"
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    with pytest.raises(asyncio.CancelledError if abort == "cancel" else RuntimeError):
        asyncio.run(run(path, InterruptedKernel()))
    owner = am._cell_history[0]["_child_replay_owner"]
    assert not owner.active and owner.sources is None
    assert jm._child_replay_owner.get() is None
    assert jm._notebook_content_cache[str(path)] == original_cache
    assert am._fixup_persistence_failures


@pytest.mark.parametrize("refusal", ["completed", "ray", "cross_task"])
def test_child_stops_after_unpersistable_repair(child_replay_flow, monkeypatch, refusal):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook()
    calls = []
    async def fix(code, *args, session=None, **kwargs):
        calls.append(code)
        if refusal == "completed":
            am._cell_history[0]["_child_replay_owner"].close()
        elif refusal == "ray":
            am._cell_history[0]["_ray_authoritative"] = True
        else:
            am._cell_history[0]["_task_key"] = "completed_task"
        result = await am._handle_tool_call("fixup_cell", {
            "start_index": 0, "fixed_code": repaired, "why": "unpersistable upstream",
        }, session=session)
        assert result.startswith("ERROR:" if refusal == "cross_task" else "PARTIAL:")
        return code
    monkeypatch.setattr(jm, "call_fix", fix)
    kernel = Kernel()
    result = asyncio.run(run(path, kernel))
    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert len(calls) == 1
    assert sum("# CHILD_REPLAY_CALL" in c for c in kernel.executed) == 1
    assert kernel.saves == 0


def test_repaired_child_cannot_bypass_save_alignment_guard(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook()
    before_cache = jm._notebook_content_cache[str(path)]
    nb = json.loads(path.read_text())
    nb["cells"].append({"cell_type": "code", "source": ["UNRELATED = True"],
                        "outputs": [], "metadata": {}, "execution_count": None})
    path.write_text(json.dumps(nb))
    before_disk = path.read_text()
    kernel = Kernel()
    result = asyncio.run(run(path, kernel))
    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert "drift" in result["error"]
    assert kernel.saves == 0
    assert jm._notebook_content_cache[str(path)] == before_cache
    assert path.read_text() == before_disk
    assert am._fixup_persistence_failures


@pytest.mark.parametrize("padding", [100, 8000, 24000])
def test_explicit_saved_output_reaches_model_without_retruncation(monkeypatch, tmp_path, padding):
    """A full-source retrieval must deliver the tail, not another file pointer."""
    import re
    from types import SimpleNamespace as NS
    from context_compactor import ContextCompactor

    monkeypatch.setenv("AIDP_TMP_DIR", str(tmp_path))
    monkeypatch.setattr(am, "_compactor_history", [])
    source = "# helper context\n" + "# x\n" * padding + "def incident_helper():\n    return 'tail'\n"
    am._cell_history.append(_entry(source, is_child=True))
    expected = asyncio.run(am._handle_tool_call(
        "get_history_entry", {"index": 0}, session=_SuccessfulSession()))
    requests = []

    def create(**kwargs):
        requests.append(kwargs)
        turn = len(requests)
        if turn == 1:
            name, payload = "get_history_entry", {"index": 0}
        else:
            delivered = kwargs["messages"][-1]["content"][0]["content"]
            if turn == 2 and len(expected) > ContextCompactor.TRUNCATE_AT:
                assert "def incident_helper" not in delivered
                saved = re.search(r"Full output saved to (\S+)", delivered)
                assert saved, delivered[-300:]
                name, payload = "get_tool_output", {"filename": saved.group(1)}
            else:
                assert delivered == expected
                assert "def incident_helper" in delivered
                name, payload = "submit_code", {"code": "answer = 1"}
        return NS(content=[NS(type="tool_use", name=name, id=f"tool-{turn}", input=payload)],
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    code, _ = asyncio.run(am.call_opus_with_tools(
        "Fix", "Read the original helper before repair", session=_SuccessfulSession()))
    assert code == "answer = 1"
    assert len(requests) == (3 if len(expected) > ContextCompactor.TRUNCATE_AT else 2)


def test_model_tool_loop_stops_immediately_on_fixup_persistence_failure(monkeypatch):
    from types import SimpleNamespace as NS
    am._cell_history.append(_entry("answer = 1", is_child=True))
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return NS(content=[NS(type="tool_use", name="fixup_cell", id="fixup-1",
                             input={"start_index": 0, "fixed_code": "answer = 2", "why": "fixture"})],
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    with pytest.raises(am.FixupPersistenceError, match="child replay"):
        asyncio.run(am.call_opus_with_tools("Fix", "fixture", session=_SuccessfulSession()))
    assert len(calls) == 1


def test_old_persistence_failure_does_not_poison_new_successful_fixup(monkeypatch):
    """The final task stays red, but this fixup gets its own outcome."""
    from types import SimpleNamespace as NS

    am._record_fixup_persistence_failure("earlier cell could not be saved")
    original = "answer = 1\n"
    source_ref = [original]
    am._cell_history.append(_entry(original, source_ref=source_ref))
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            block = NS(type="tool_use", name="fixup_cell", id="fixup-1",
                       input={"start_index": 0, "fixed_code": "answer = 2\n",
                              "why": "fixture"})
        else:
            block = NS(type="tool_use", name="submit_code", id="submit-1",
                       input={"code": "answer = 2\n"})
        return NS(content=[block], stop_reason="tool_use",
                  usage=NS(input_tokens=1, output_tokens=1))

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    code, _ = asyncio.run(am.call_opus_with_tools(
        "Fix", "fixture", session=_SuccessfulSession()))

    assert code == "answer = 2\n"
    assert len(calls) == 2
    assert source_ref == ["answer = 2"]
    assert am._fixup_persistence_failures == ["earlier cell could not be saved"]


def test_repeated_same_persistence_failure_still_stops_tool_loop(monkeypatch):
    """Task-report deduplication must not hide this call's failure event."""
    from types import SimpleNamespace as NS

    entry = _entry("answer = 1\n", is_child=True)
    am._cell_history.append(entry)
    assert _fix(0, "answer = 2\n", _SuccessfulSession()).startswith("PARTIAL:")
    prior_messages = list(am._fixup_persistence_failures)
    am._cell_history[:] = [entry]  # repeat the same failed call in this task
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        return NS(content=[NS(type="tool_use", name="fixup_cell", id="fixup-1",
                              input={"start_index": 0, "fixed_code": "answer = 2\n",
                                     "why": "fixture"})],
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    with pytest.raises(am.FixupPersistenceError, match="child replay"):
        asyncio.run(am.call_opus_with_tools(
            "Fix", "fixture", session=_SuccessfulSession()))
    assert len(calls) == 1
    assert am._fixup_persistence_failures == prior_messages


def test_fixup_failure_event_is_isolated_between_dependency_tasks():
    """A sibling dependency's failure must not poison this tool call."""
    async def exercise():
        ready, recorded = asyncio.Event(), asyncio.Event()

        async def unaffected():
            before = am._fixup_persistence_event()
            ready.set()
            await recorded.wait()
            return before == am._fixup_persistence_event()

        async def failing_sibling():
            await ready.wait()
            am._record_fixup_persistence_failure("sibling dependency failed")
            recorded.set()

        unaffected_result, _ = await asyncio.gather(
            unaffected(), failing_sibling())
        return unaffected_result

    assert asyncio.run(exercise())


def test_old_persistence_failure_does_not_stop_later_child_fix(
    child_replay_flow,
):
    notebook, run, Kernel, _, repaired, _, _, repairs = child_replay_flow
    am._record_fixup_persistence_failure("earlier cell could not be saved")

    path = notebook()
    result = asyncio.run(run(path, Kernel()))

    assert result["status"] == "ok", result
    assert repairs and repairs[0].startswith("OK:")
    assert repaired.rstrip() in "".join(json.loads(path.read_text())["cells"][0]["source"])
    assert am._fixup_persistence_failures == ["earlier cell could not be saved"]


def test_repeated_same_persistence_failure_stops_child_fix(
    child_replay_flow, monkeypatch,
):
    notebook, run, Kernel, _, _, _, _, _ = child_replay_flow
    message = "earlier cell could not be saved"
    am._record_fixup_persistence_failure(message)
    calls = []

    async def fix(code, *args, **kwargs):
        calls.append(code)
        am._record_fixup_persistence_failure(message)
        return code

    monkeypatch.setattr(jm, "call_fix", fix)
    result = asyncio.run(run(notebook(), Kernel()))

    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert message in result["error"]
    assert len(calls) == 1
    assert am._fixup_persistence_failures == [message]


@pytest.mark.parametrize("status,kind,expected", [
    ("error", "DATA_UNAVAILABLE", "DATA_UNAVAILABLE"),
    ("error", "NESTED_CHILD", "NESTED_CHILD"),
    ("ok", None, "CHILD_PERSISTENCE"),
])
def test_unsaved_child_replay_preserves_existing_failure_kind(
    status, kind, expected,
):
    @jm._owns_child_replay
    async def child():
        owner = jm._child_replay_owner.get()
        owner.sources = ["value = 1"]
        owner.dirty = True
        return {"status": status, "kind": kind, "error": "upstream failed"}

    result = asyncio.run(child())

    assert result["status"] == "error"
    assert result["kind"] == expected
    assert am._fixup_persistence_failures == ["upstream failed"]


def test_child_replay_and_caller_edit_are_both_saved(child_replay_flow, monkeypatch):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    consumer = caller + "display(result)\n"
    path = notebook(sources=[original, consumer])
    upstream_fix = jm.call_fix
    async def fix(code, *args, **kwargs):
        await upstream_fix(code, *args, **kwargs)
        return code.replace("display(result)", "# display(result)\nIdisplay(result)")
    monkeypatch.setattr(jm, "call_fix", fix)
    result = asyncio.run(run(path, Kernel()))
    assert result["status"] == "ok", result
    assert len(repairs) == 1
    saved = json.loads(path.read_text())
    assert "return Idisplay('frame')" in "".join(saved["cells"][0]["source"])
    assert "Idisplay(result)" in "".join(saved["cells"][1]["source"])


def test_child_replay_slot_survives_markdown_empty_and_legacy_config_cells(child_replay_flow):
    notebook, run, Kernel, original, repaired, caller, messages, repairs = child_replay_flow
    path = notebook()
    nb = json.loads(path.read_text())
    nb["cells"][:0] = [
        {"cell_type": "markdown", "source": ["Keep this documentation"], "metadata": {}},
        {"cell_type": "code", "source": ["# AIDP performance configuration\nLEGACY = True"],
         "metadata": {}, "outputs": [], "execution_count": None},
        {"cell_type": "code", "source": [], "metadata": {}, "outputs": [], "execution_count": None},
    ]
    path.write_text(json.dumps(nb))
    jm._notebook_content_cache[str(path)] = json.dumps(nb)
    result = asyncio.run(run(path, Kernel()))
    assert result["status"] == "ok", result
    saved = json.loads(path.read_text())
    assert saved["cells"][:3] == nb["cells"][:3]
    assert "return Idisplay('frame')" in "".join(saved["cells"][3]["source"])
    assert "".join(saved["cells"][4]["source"]) == caller.rstrip()


class _SuccessfulSession:
    def __init__(self):
        self.executed = []

    async def execute(self, code, **kwargs):
        self.executed.append(code)
        return {"status": "ok", "outputs": []}


@pytest.fixture(autouse=True)
def _restore_fixup_state(monkeypatch):
    # Other collection-time tests load alternate job_migrate module instances.
    # Bind the real callback belonging to the module whose LLM boundary this
    # test patches; restore the prior registration through monkeypatch teardown.
    monkeypatch.setattr(am, "_replay_cell_fn", jm._replay_cell_entry)
    monkeypatch.setattr(am, "_repair_dependency_fn", jm._repair_completed_dependency)
    # Allow the same behavioral reproducer to run against the pre-review head.
    if not hasattr(am, "_fixup_persistence_failures"):
        monkeypatch.setattr(am, "_fixup_persistence_failures", [], raising=False)
    original_history = list(am._cell_history)
    original_failures = list(am._fixup_persistence_failures)
    original_task = am._current_task_key
    original_base = jm.MIGRATED_BASE
    am._cell_history.clear()
    am._fixup_persistence_failures.clear()
    am.set_current_task_key("task_current")
    try:
        yield
    finally:
        am._cell_history.clear()
        am._cell_history.extend(original_history)
        am._fixup_persistence_failures.clear()
        am._fixup_persistence_failures.extend(original_failures)
        am.set_current_task_key(original_task)
        jm.MIGRATED_BASE = original_base


def _entry(
    code: str,
    *,
    cell_idx: int = 0,
    is_child: bool = False,
    source_ref: list[str] | None = None,
    task_key: str = "task_current",
    ray: bool = False,
    notebook_path: str = "/Workspace/migrated/model.ipynb",
) -> dict:
    entry = {
        "index": len(am._cell_history),
        "notebook_path": notebook_path,
        "cell_idx": cell_idx,
        "summary": f"cell {cell_idx}",
        "final_code": code[:3000],
        "replay_code": code,
        "output_preview": "",
        "status": "ok",
        "is_child": is_child,
        "last_note": "",
        "_task_key": task_key,
        "_ray_authoritative": ray,
    }
    if source_ref is not None:
        entry["_persist_source"] = source_ref
        entry["_persist_active"] = True
    return entry


def _fix(start_index: int, fixed_code: str | None, session) -> str:
    payload = {"start_index": start_index, "why": "persistence regression"}
    if fixed_code is not None:
        payload["fixed_code"] = fixed_code
    return asyncio.run(am._handle_tool_call(
        "fixup_cell", payload, session=session,
    ))


@pytest.mark.parametrize("nested", [False, True])
def test_child_repair_receives_task_source_and_analysis_context(
    child_replay_flow, monkeypatch, nested,
):
    """Oct 1 output-push incident: child call_fix dropped available context.

    Scope: same base repair context as parent, plus successful full source from
    this task. Matrix: direct/nested child; parent/child source; successful,
    failed and foreign-task history; first/repeated repair. Preserve source,
    save-back and bounded failures. No evaluator or parameter-policy changes.
    """
    from types import SimpleNamespace

    notebook, _, Kernel, _, _, _, messages, _ = child_replay_flow
    prelude = "# CHILD_REPLAY_SETUP\noutput_dict = {}\n" + "# context padding\n" * 200
    prelude += "OUTPUT_PRODUCER_TAIL = True\n"
    output_cell = (
        '# CHILD_REPLAY_OUTPUT\n'
        'output_file_name = aidputils.widgets.get("file_name")\n'
        'requests.request("PUT", output_file_name, json=output_dict)\n'
    )
    incomplete = output_cell.replace('requests.request', '# requests.request')
    repaired = "\n".join("# " + line for line in output_cell.splitlines())
    repaired += '\n# Oracle tool modification: external output push disabled\nprint("Output push skipped")'
    child = notebook("context_child", [prelude, output_cell])
    outer_source = "# CHILD_REPLAY_OUTER\nouter_value = 1"
    path = notebook("context_outer", [outer_source, "%run ./context_child"]) if nested else child

    parent_source = "parent_value = 1\n" + "# parent padding\n" * 200 + "PARENT_SOURCE_TAIL = True"
    am._cell_history.extend([
        _entry(parent_source),
        _entry("FOREIGN_TASK_SOURCE = True", task_key="other_task"),
        {**_entry("FAILED_SOURCE = True"), "status": "error"},
    ])
    prompts = []

    async def model(system, prompt, **kwargs):
        assert system == am.FIX_PROMPT
        prompts.append(prompt)
        return (incomplete if len(prompts) == 1 else repaired), {}

    def missing_widget(name):
        raise ValueError(f"Widget not found: {name}")

    class Log:
        log = staticmethod(messages.append)

    class ErrorKernel(Kernel):
        async def execute(self, code, **kwargs):
            result = await super().execute(code, **kwargs)
            for output in result.get("outputs", []):
                if output["type"] == "error":
                    output["traceback"] = [f"{output['ename']}: {output['evalue']}"]
            return result

    monkeypatch.setattr(jm, "call_fix", am.call_fix)
    monkeypatch.setattr(am, "call_opus_with_tools", model)
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "BUCKET_CONTEXT")
    kernel = ErrorKernel()
    kernel.namespace["aidputils"] = SimpleNamespace(widgets=SimpleNamespace(get=missing_widget))
    result = asyncio.run(jm._inline_child_notebook(
        str(path), kernel, Log(), "fixture", "TASK_ANALYSIS", "CATALOG_CONTEXT",
        "DEPENDENCY_CONTEXT", {},
    ))

    assert result["status"] == "ok", result
    assert len(prompts) == 2
    for prompt in prompts:
        assert "TASK_ANALYSIS" in prompt
        assert "DEPENDENCY_CONTEXT" in prompt
        assert "BUCKET_CONTEXT" in prompt and "CATALOG_CONTEXT" in prompt
        assert parent_source in prompt  # full source, not the 3k history preview
        assert prelude.rstrip() in prompt
        assert (outer_source in prompt) is nested
        assert "FOREIGN_TASK_SOURCE" not in prompt
        assert "FAILED_SOURCE" not in prompt
        assert "Widget not found: file_name" in prompt
        assert f"Source notebook path: {child}" in prompt
    assert "PREVIOUS FIXES APPLIED" in prompts[1]
    assert "output_file_name = aidputils.widgets.get" in prompts[1]
    saved = json.loads(child.read_text())
    assert repaired in "".join(saved["cells"][1]["source"])


@pytest.mark.parametrize("size", [0, 20, 6000])
def test_shared_repair_context_preserves_parent_input_and_limits(size):
    fields = [
        f"BUCKET_START{'b' * size}BUCKET_END",
        f"CATALOG_START{'c' * size}CATALOG_END",
        f"DEPENDENCIES_START{'d' * size}DEPENDENCIES_END",
        f"ANALYSIS_START{'a' * size}ANALYSIS_END",
    ]
    context = jm._build_repair_context(*fields)

    assert context.startswith(fields[0])  # bucket mapping remains complete
    positions = [context.index(f"{name}_START") for name in
                 ("BUCKET", "CATALOG", "DEPENDENCIES", "ANALYSIS")]
    assert positions == sorted(positions)  # no field was dropped or reordered
    if size < 6000:
        assert all(f"{name}_END" in context for name in
                   ("BUCKET", "CATALOG", "DEPENDENCIES", "ANALYSIS"))
    else:
        assert "BUCKET_END" in context
        assert all(f"{name}_END" not in context for name in
                   ("CATALOG", "DEPENDENCIES", "ANALYSIS"))
        assert len(context) < len("".join(fields))  # bounded non-bucket context


def test_fixup_persists_parent_source_via_lightweight_reference():
    original = "def model_selection(df):\n    display(df)\n"
    repaired = (
        "from IPython.display import display as Idisplay\n"
        "def model_selection(df):\n    Idisplay(df)\n"
    )
    source_ref = [original]
    entry = _entry(original, source_ref=source_ref)
    am._cell_history.append(entry)

    result = _fix(0, repaired, _SuccessfulSession())

    assert result.startswith("OK:")
    assert source_ref == [repaired.rstrip()]
    assert "_persist_cell" not in entry


def test_long_target_and_downstream_cells_replay_without_truncation():
    target = "value = 1\n" + ("# target padding\n" * 400) + "TARGET_END = True\n"
    repaired = target.replace("value = 1", "value = 2")
    downstream = (
        "other = value\n" + ("# downstream padding\n" * 400)
        + "DOWNSTREAM_END = True\n"
    )
    target_ref = [target]
    downstream_ref = [downstream]
    am._cell_history.extend([
        _entry(target, cell_idx=0, source_ref=target_ref),
        _entry(downstream, cell_idx=1, source_ref=downstream_ref),
    ])
    session = _SuccessfulSession()

    result = _fix(0, repaired, session)

    assert result.startswith("OK:")
    assert target_ref[0].endswith("TARGET_END = True")
    assert any("DOWNSTREAM_END = True" in code for code in session.executed)
    assert am._cell_history[-1]["replay_code"].endswith("DOWNSTREAM_END = True")
    assert len(am._cell_history[-1]["final_code"]) == 3000


def test_unchanged_replay_does_not_require_save_target():
    am._cell_history.append(_entry("answer = 42\n"))

    result = _fix(0, None, _SuccessfulSession())

    assert result.startswith("OK:")
    assert not am._fixup_persistence_failures


def test_unchanged_child_replay_still_succeeds():
    am._cell_history.append(_entry("answer = 42\n", is_child=True))
    assert _fix(0, None, _SuccessfulSession()).startswith("OK:")
    assert not am._fixup_persistence_failures


@pytest.mark.parametrize("is_child", [False, True])
def test_downstream_repair_discovered_during_replay(monkeypatch, is_child):
    code = "# broken downstream\nvalue = 1\n" + "# padding\n" * 400 + "TAIL = True\n"
    repaired = code.replace("# broken downstream", "# repaired downstream")
    source_ref = [code]
    am._cell_history.extend([
        _entry("start = True\n"),
        _entry(code, cell_idx=1, source_ref=source_ref, is_child=is_child),
    ])

    class Session(_SuccessfulSession):
        async def execute(self, code, **kwargs):
            self.executed.append(code)
            if "# broken downstream" in code:
                return {"status": "error", "outputs": [
                    {"type": "error", "ename": "ValueError", "evalue": "replay failure"}
                ]}
            return {"status": "ok", "outputs": []}

    async def fix(*args, **kwargs):
        return repaired

    monkeypatch.setattr(jm, "call_fix", fix)
    result = _fix(0, None, Session())
    if is_child:
        assert result.startswith("PARTIAL:")
        assert am._fixup_persistence_failures
        assert source_ref == [code]
    else:
        assert result.startswith("OK:")
        assert source_ref == [repaired]
        assert "TAIL = True" in source_ref[0]
        # Repeating an already-persisted replay leaves the artifact stable.
        assert _fix(0, None, Session()).startswith("OK:")
        assert source_ref == [repaired]


def test_changed_replay_without_save_target_fails_task_contract():
    am._cell_history.append(_entry("display(df)\n"))

    result = _fix(0, "Idisplay(df)\n", _SuccessfulSession())

    assert result.startswith("PARTIAL:")
    assert "could not be persisted" in result
    assert am._fixup_persistence_failures


def test_child_fixup_fails_loudly_without_claiming_durable_persistence():
    repaired = "from IPython.display import display as Idisplay\nIdisplay(df)\n"
    am._cell_history.append(_entry(
        "display(df)\n",
        is_child=True,
        notebook_path="/Workspace/migrated/child.ipynb",
    ))
    session = _SuccessfulSession()
    result = _fix(0, repaired, session)
    assert result.startswith("PARTIAL:")
    assert "Cannot persist changed child replay" in result
    assert am._fixup_persistence_failures
    assert not any("json.dump" in code for code in session.executed)


def test_cross_task_rewind_is_rejected_before_execution():
    am._cell_history.append(_entry(
        "answer = 1\n", task_key="completed_task", source_ref=["answer = 1\n"],
    ))
    session = _SuccessfulSession()

    result = _fix(0, "answer = 2\n", session)

    assert result.startswith("ERROR:")
    assert "cannot rewind across completed task boundaries" in result
    assert session.executed == []
    assert am._fixup_persistence_failures


def test_changed_authoritative_ray_cell_fails_instead_of_being_overwritten():
    source_ref = ["run_distributed_search()\n"]
    am._cell_history.append(_entry(
        source_ref[0], source_ref=source_ref, ray=True,
    ))

    result = _fix(0, "run_local_search()\n", _SuccessfulSession())

    assert result.startswith("PARTIAL:")
    assert "authoritative Ray conversion" in result
    assert source_ref == ["run_distributed_search()\n"]
    assert am._fixup_persistence_failures


def test_every_history_producer_keeps_full_replay_code_and_task_scope():
    source_path = Path(jm.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    producers = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or len(node.args) != 1:
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "append"
            and isinstance(func.value, ast.Name)
            and func.value.id == "_cell_history"
            and isinstance(node.args[0], ast.Dict)
        ):
            continue
        keys = {
            key.value for key in node.args[0].keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }
        producers.append(keys)

    # Data-gap continuation now shares the pre-dispatch convert-only history
    # producer with conditional exits instead of maintaining a Python-only one.
    assert len(producers) == 20
    assert all("replay_code" in keys for keys in producers)
    assert all("_task_key" in keys for keys in producers)
    parent_producers = [keys for keys in producers if "_persist_source" in keys]
    assert len(parent_producers) == 15
    assert all("_persist_active" in keys for keys in parent_producers)
    assert all("_ray_authoritative" in keys for keys in parent_producers)


@pytest.mark.parametrize("failure_kind", [None, "missing_target", "child", "ray", "cross_task"])
@pytest.mark.parametrize("strict", [False, True])
def test_real_notebook_flow_replay_to_upload(monkeypatch, tmp_path, failure_kind, strict):
    """Real process_notebook + real fixup tool; fake external kernel and LLM."""
    import base64
    import re
    import cell_analyzer

    original = (
        "# FIXUP_SOURCE\ndef model_selection():\n    return display('frame')\n"
        + "# preserve long source\n" * 200 + "SOURCE_TAIL = True\n"
    )
    repaired = original.replace(
        "    return display('frame')",
        "    # Oracle tool modification: use the pandas renderer\n"
        "    # return display('frame')\n    return Idisplay('frame')",
    )
    downstream = (
        "# FIXUP_DOWNSTREAM\n" + "# preserve downstream\n" * 200
        + "DOWNSTREAM_TAIL = True\n"
    )
    consumer = "# FIXUP_CONSUMER\nresult = model_selection()\nassert DOWNSTREAM_TAIL\n"
    notebook = {
        "cells": [{"cell_type": "code", "source": [code], "metadata": {},
                   "outputs": [], "execution_count": None}
                  for code in (original, downstream, consumer)],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }

    class Kernel(_SuccessfulSession):
        cluster_id = "test-cluster"

        def __init__(self):
            super().__init__()
            self.files = {}
            def display(_):
                raise AttributeError("DataFrame has no attribute isStreaming")
            self.namespace = {"display": display, "Idisplay": lambda _: "rendered"}

        async def execute(self, code, **kwargs):
            self.executed.append(code)
            # This transport fake does not execute setup; acknowledge successful
            # task isolation so this test reaches the fixup persistence boundary.
            if "_task_expected =" in code:
                return {"status": "ok", "outputs": [
                    {"type": "stream", "name": "stdout", "text": "__AIDP_ISO_OK__\n"}]}

            match = re.search(r"with builtins.open\('([^']+)', 'w[ba]'\) as f:", code)
            encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
            if match and encoded:
                self.files[match.group(1)] = base64.b64decode(encoded.group(1)).decode()
            if "# FIXUP_" in code and not encoded:
                try:
                    exec(code, self.namespace)
                except Exception as exc:
                    return {"status": "error", "outputs": [
                        {"type": "error", "ename": type(exc).__name__, "evalue": str(exc)}
                    ]}
            return {"status": "ok", "outputs": []}

    async def analyze(cells, *args, **kwargs):
        return [{"cell_index": i, "action": "execute", "description": "plain Python",
                 "risks": [], "changes_needed": [], "storage_paths": [],
                 "table_refs": [], "fuse_risks": []} for i in range(len(cells))]

    async def fix(code, *args, **kwargs):
        if failure_kind == "missing_target":
            am._cell_history[0]["_persist_source"] = None
            am._cell_history[0].pop("_persist_cell", None)
        elif failure_kind == "child":
            am._cell_history[0]["is_child"] = True
        elif failure_kind == "ray":
            am._cell_history[0]["_ray_authoritative"] = True
        elif failure_kind == "cross_task":
            am._cell_history[0]["_task_key"] = "previous_task"
        result = await am._handle_tool_call(
            "fixup_cell", {"start_index": 0, "fixed_code": repaired, "why": "pandas display"},
            session=kwargs["session"],
        )
        expected = "ERROR:" if failure_kind == "cross_task" else ("PARTIAL:" if failure_kind else "OK:")
        assert result.startswith(expected), result
        return code

    async def noop(*args, **kwargs):
        return None

    async def verified(*args, **kwargs):
        return True

    monkeypatch.setattr(jm, "download_notebook", lambda *args, **kwargs: json.dumps(notebook).encode())
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "load_catalog_snapshot", lambda: "")
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "")
    monkeypatch.setattr(jm, "_inject_write_guard", lambda code: code)
    monkeypatch.setattr(jm, "registry_update", noop)
    monkeypatch.setattr(jm, "verify_upload", verified)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_fixup")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_fixup/notebooks")
    monkeypatch.setattr(jm.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr(jm.shutil, "rmtree", lambda *args, **kwargs: None)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_fixup/source.ipynb", kernel, "test_fixup", "01_task_current", {},
    ))

    if failure_kind:
        assert result["status"] == "FAIL", result
        assert result["failed"] >= 1
        assert any("fixup persistence failed" in cr.get("note", "") for cr in result["cell_results"])
        assert not any(path.endswith(".ipynb") for path in kernel.files)
    else:
        assert result["status"] == "PASS", result
        finals = [json.loads(text) for path, text in kernel.files.items() if path.endswith(".ipynb")]
        assert len(finals) == 2
        for saved in finals:
            sources = "\n".join("".join(c["source"]) for c in saved["cells"])
            assert "return Idisplay('frame')" in sources
            assert "SOURCE_TAIL = True" in sources
            assert "DOWNSTREAM_TAIL = True" in sources
    assert all(not e.get("_persist_active") for e in am._cell_history)
    assert all(e.get("_persist_source") is None for e in am._cell_history)

@pytest.mark.parametrize("is_child", [False, True])
@pytest.mark.parametrize("injected", [False, True])
def test_replay_saves_slack_neutralization_on_first_success(
    monkeypatch, is_child, injected,
):
    """Review repro: the guarded execution copy must match durable safety."""
    original = (
        "# NEEDS_REPAIR\n"
        "# requests.post('https://hooks.slack.com/services/T/B/fixture')\n"
        "value = 1\n"
    )
    repaired = (
        "import requests\n"
        "requests.post('https://hooks.slack.com/services/T/B/fixture', json={'text': 'test'})\n"
        "value = 1\n"
    )
    source_ref = [original]
    entry = _entry(original, is_child=is_child,
                   source_ref=None if is_child else source_ref)
    if is_child:
        owner = jm._ChildReplayOwner()
        owner.sources = source_ref
        entry.update(_child_replay_owner=owner, _child_replay_slot=0)
    am._cell_history.append(entry)

    class Session(_SuccessfulSession):
        async def execute(self, code, **kwargs):
            self.executed.append(code)
            if "# NEEDS_REPAIR" in code:
                return {"status": "error", "outputs": [
                    {"type": "error", "ename": "ValueError", "evalue": "needs repair"}
                ]}
            return {"status": "ok", "outputs": []}

    async def fix(*args, **kwargs):
        return repaired

    monkeypatch.setattr(jm, "call_fix", fix)
    session = Session()
    result = _fix(0, repaired if injected else None, session)

    assert result.startswith("OK:"), result
    assert len(session.executed) == (1 if injected else 2)
    assert "requests.post" in source_ref[0]  # original line is preserved
    assert "requests.post" not in jm._active_lines(source_ref[0])
    assert am._cell_history[-1]["replay_code"] == source_ref[0]
    assert not am._fixup_persistence_failures


@pytest.mark.parametrize("is_child", [False, True])
@pytest.mark.parametrize("original,repaired,reason", [
    ("df = spark.read.table('sales.orders')\n",
     "df = spark.read.parquet('oci://backup@ns/orders/')\n",
     "table-to-path conversion"),
    ("# requests.post('https://hooks.slack.com/services/T/B/fixture')\n"
     "df = spark.read.table('sales.orders')\n",
     "import requests\n"
     "requests.post('https://hooks.slack.com/services/T/B/fixture')\n"
     "df = spark.read.parquet('oci://backup@ns/orders/')\n",
     "table-to-path conversion"),
    ("orders_path = get_table_location('orders')\n",
     "orders_path = 'sales.orders'\n",
     "path-to-identifier conversion"),
    ("df.write.saveAsTable('sales.orders')\n",
     "print('done')\n",
     "significant operation"),
    ("def credential():\n    return aidputils.secrets.get(scope='fixture', key='token')\n",
     "def credential():\n    return 'fabricated'\n",
     "significant operation"),
    ("def credential():\n    return aidputils.secrets.get(scope='fixture', key='token')\n",
     "def credential():\n    try:\n        return aidputils.secrets.get('fixture', key='token')\n"
     "    except Exception:\n        return 'fabricated'\n",
     "significant operation"),
])
def test_first_attempt_fixup_rejects_unsafe_saved_source(
    is_child, original, repaired, reason,
):
    """A passing execution cannot certify a different/empty saved operation."""
    source_ref = [original]
    entry = _entry(original, is_child=is_child,
                   source_ref=None if is_child else source_ref)
    if is_child:
        owner = jm._ChildReplayOwner()
        owner.sources = source_ref
        entry.update(_child_replay_owner=owner, _child_replay_slot=0)
    am._cell_history.append(entry)
    session = _SuccessfulSession()

    for _ in range(2):
        result = _fix(0, repaired, session)
        assert result.startswith("PARTIAL:"), result
        assert reason in result
        assert session.executed == []
        assert source_ref == [original]
        assert am._fixup_persistence_failures


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("long_source", [False, True])
@pytest.mark.parametrize("initial_noop", [False, True])
@pytest.mark.parametrize("fail_save", [False, True])
@pytest.mark.parametrize("parallel_submit", [False, True])
@pytest.mark.parametrize("parallel_retrieval", [False, True])
def test_repair_handoff_retrieval_save_rebuild_and_fresh_execution(
        child_replay_flow, monkeypatch, wrapped, long_source, initial_noop, fail_save, parallel_submit, parallel_retrieval):
    """Real fix/tool/child/fixup/save loops; only model and remote kernel are fixtures."""
    import copy
    import re
    from types import SimpleNamespace as NS
    notebook, run, Kernel, _, _, _, messages, _ = child_replay_flow
    original = ("# CHILD_REPLAY_DEF\ndef read_table():\n"
                "    return {'value': 'parquet bytes'}\n")
    if long_source:
        original += "# complete helper source retrieval fixture\n" * 1300
    fixed = original.replace("    return {'value': 'parquet bytes'}",
        "    # return {'value': 'parquet bytes'}\n"
        "    # Oracle tool modification: read Provider PARQUET as parquet\n"
        "    return {'type': 'event', 'userLatitude': '12.5'}")
    read = "# CHILD_REPLAY_READ\nframe = read_table()\n"
    caller = "# CHILD_REPLAY_CALL\nresult = (frame['type'], float(frame['userLatitude']))\n"
    helper = notebook('helper', [original])
    parent = notebook('parent', ['%run ./helper', read, caller])
    calls = []
    kernel = Kernel()
    kernel.fail_save = fail_save
    before_helper = helper.read_text()
    def create(**kwargs):
        calls.append(copy.deepcopy(kwargs))
        turn = len(calls)
        # Every follow-up retains the original failing caller and tool conversation.
        assert caller in kwargs['messages'][0]['content']
        names = {t['name'] for t in kwargs['tools']}
        target = next(e for e in am._cell_history if e['notebook_path'] == str(helper))
        parent_read = next(e for e in am._cell_history if e['replay_code'].strip() == read.strip())
        if turn == 1:
            name, payload = (('submit_code', {'code': '```python\n' + caller + '```' if wrapped else caller})
                             if initial_noop else ('get_cell_history', {}))
        elif turn == 2:
            name, payload = 'get_cell_history', {}
        elif turn == 3:
            name, payload = 'get_history_entry', {'index': target['index']}
        elif turn == 4:
            if long_source:
                # Full source must be delivered, not another truncated preview.
                text = str(next(m['content'] for m in reversed(kwargs['messages']) if isinstance(m['content'], list)))
                filename = re.search(r'tool_[a-zA-Z0-9_]+\.txt', text)
                assert filename, text[-1000:]
                name, payload = 'get_tool_output', {'filename': filename.group()}
            else:
                name, payload = 'get_history_entry', {'index': parent_read['index']}
        elif turn == 5:
            assert names == {'repair_dependency', 'fixup_cell', 'submit_code', 'make_note'}
            assert original.rstrip() in '\n'.join(
                block['content'] for msg in kwargs['messages'] if isinstance(msg['content'], list)
                for block in msg['content'] if isinstance(block, dict) and block.get('type') == 'tool_result')
            name, payload = 'repair_dependency', {'index': target['index'],
                'fixed_code': fixed, 'why': 'Provider PARQUET, missing InputFormat, value-only schema'}
        elif turn == 6:
            assert 'dependency re-executed and saved' in str(kwargs['messages'])
            assert kernel.namespace['frame'] == {'value': 'parquet bytes'}  # still stale!
            name, payload = 'fixup_cell', {'start_index': parent_read['index'],
                                         'why': 'rebuild materialized parent read after helper repair'}
        else:
            assert turn == 7 and not parallel_submit
            assert kernel.namespace['frame'] == {'type': 'event', 'userLatitude': '12.5'}
            assert 'OK:' in str(kwargs['messages'][-1]['content'])
            name, payload = 'submit_code', {'code': caller}
        blocks = [NS(type='tool_use', name=name, id=f't-{turn}', input=payload)]
        if parallel_retrieval and turn in (2, 3, 4):
            blocks.insert(0, NS(type='tool_use', name='submit_code', id=f'early-{turn}', input={'code': caller}))
        if turn == 6 and parallel_submit:
            # Caller first must not skip the pending parent replay.
            blocks.insert(0, NS(type='tool_use', name='submit_code', id='caller', input={'code': caller}))
        return NS(content=blocks, stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    monkeypatch.setattr(am, 'MAX_TOOL_ROUNDS_FIX', 0)
    result = asyncio.run(run(parent, kernel))
    if fail_save:
        assert result['status'] == 'error', result
        assert result['kind'] == 'CHILD_PERSISTENCE'
        assert helper.read_text() == before_helper
        assert kernel.namespace['frame'] == {'value': 'parquet bytes'}
        assert 'result' not in kernel.namespace
        assert len(calls) == 5
        return
    assert result['status'] == 'ok' and not result.get('failed_cells'), (result, messages)
    assert kernel.namespace.get('result') == ('event', 12.5), (result, calls, messages, kernel.executed)
    assert len(calls) == (6 if parallel_submit else 7)
    assert kernel.saves >= 2
    assert fixed.rstrip() in ''.join(json.loads(helper.read_text())['cells'][0]['source'])
    assert [c['source'] for c in json.loads(jm._notebook_content_cache[str(helper)])['cells']] == [
        c['source'] for c in json.loads(helper.read_text())['cells']]
    # Use the actually saved notebook/cache in a fresh namespace. No extra model call.
    fresh = Kernel()
    assert asyncio.run(run(parent, fresh))['status'] == 'ok'
    assert fresh.namespace['result'] == ('event', 12.5)
    assert len(calls) == (6 if parallel_submit else 7)


@pytest.mark.parametrize('wrapped', [False, True])
def test_unresolved_handoff_stops_child_without_reexecuting_caller(
        child_replay_flow, monkeypatch, wrapped):
    from types import SimpleNamespace as NS
    notebook, run, Kernel, original, _, caller, messages, _ = child_replay_flow
    helper = notebook('helper', [original])
    parent = notebook('parent', ['%run ./helper', caller])
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return NS(content=[NS(type='tool_use', name='submit_code', id=f'noop-{len(calls)}',
                  input={'code': '```python\n' + caller + '```' if wrapped else caller})],
                  stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    kernel = Kernel()
    result = asyncio.run(run(parent, kernel))
    assert result['status'] == 'error'
    assert result['kind'] == 'UNRESOLVED_REPAIR'
    assert 'UNRESOLVED' in result['error']
    assert len(calls) == 7
    assert sum(code.strip() == caller.strip() for code in kernel.executed) == 1
    assert 'return display(' in helper.read_text()


@pytest.mark.parametrize('language,caller', [
    ('python', 'result = helper()'), ('scala', '%scala\nval result = helper()'),
    ('sql', '%sql\nSELECT * FROM helper'),
])
@pytest.mark.parametrize('response_kind', ['fenced', 'prose', 'investigation_only'])
def test_handoff_normalized_no_progress_is_bounded(monkeypatch, language, caller, response_kind):
    import copy
    from types import SimpleNamespace as NS
    calls, dispatched = [], []
    def create(**kwargs):
        calls.append(copy.deepcopy(kwargs))
        names = {t['name'] for t in kwargs['tools']}
        if response_kind == 'investigation_only':
            name = 'get_cell_history' if 'get_cell_history' in names else 'make_note'
            blocks = [NS(type='tool_use', name=name, id=f't-{len(calls)}',
                         input={} if name == 'get_cell_history' else {'note': 'CANNOT_FIX: source unavailable'})]
            reason = 'tool_use'
        else:
            blocks = [NS(type='text', text=(f'```{language}\n{caller}\n```'
                      if response_kind == 'fenced' else 'Unfortunately no safe fix is possible.'))]
            reason = 'end_turn'
        return NS(content=blocks, stop_reason=reason, usage=NS(input_tokens=1, output_tokens=1))
    async def handle(name, payload, **kwargs):
        dispatched.append(name)
        return 'source investigation result' if name == 'get_cell_history' else 'Note recorded'
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(am, '_handle_tool_call', handle)
    with pytest.raises(am.UnresolvedRepairError, match='UNRESOLVED'):
        asyncio.run(am.call_opus_with_tools('fix', caller, session=object(),
                    max_tool_rounds=1, repair_caller=caller))
    assert len(calls) == (8 if response_kind == 'investigation_only' else 7)
    assert all('tools' in c for c in calls)  # no text-only final call
    if response_kind == 'investigation_only':
        assert dispatched == ['get_cell_history'] * 5 + ['make_note'] * 3
    assert calls[0]['messages'][0] == calls[-1]['messages'][0]


@pytest.mark.parametrize('repair_tool', ['repair_dependency', 'fixup_cell'])
def test_handoff_does_not_accept_tool_ok_without_saved_receipt(monkeypatch, repair_tool):
    from types import SimpleNamespace as NS
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        return NS(content=[
            NS(type='tool_use', name=repair_tool, id=f'r-{len(calls)}', input={}),
            NS(type='tool_use', name='submit_code', id=f's-{len(calls)}', input={'code': 'helper()'}),
        ], stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    async def not_saved(*args, **kwargs):
        return 'OK: claimed progress without actual receipt'
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(am, '_handle_tool_call', not_saved)
    with pytest.raises(am.UnresolvedRepairError):
        asyncio.run(am.call_opus_with_tools('fix', 'helper()', session=object(), repair_caller='helper()'))
    assert len(calls) == 7


@pytest.mark.parametrize('caller', [
    '# HANDOFF_MAIN\nresult = helper()',
    '%sql\nMSCK REPAIR TABLE default.test.HANDOFF_MAIN;',
    '%scala\nval result = HANDOFF_MAIN.filter("type = 1")',
])
def test_main_loop_reports_unresolved_without_reexecuting(monkeypatch, tmp_path, caller):
    import test_pip_magic_gate as harness
    from types import SimpleNamespace as NS
    notebook = {'cells': [{'cell_type': 'code', 'source': [caller], 'metadata': {},
                          'outputs': [], 'execution_count': None}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    harness._patch_common(monkeypatch, tmp_path, notebook, [])
    history = [_entry('def helper(): return None', task_key=am._current_task_key)]
    history[0]['_child_replay_owner'] = NS(completed_repair=object())
    monkeypatch.setattr(am, '_cell_history', history)
    monkeypatch.setattr(jm, '_cell_history', history)
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    monkeypatch.setattr(jm, '_notebook_content_cache', {})
    monkeypatch.setattr(jm, 'fetch_recent_spark_errors', lambda *a, **kw: '')
    model_calls = []
    def create(**kwargs):
        model_calls.append(kwargs)
        return NS(content=[NS(type='tool_use', name='submit_code', id=f's-{len(model_calls)}',
                             input={'code': '```\n' + caller + '\n```'})],
                  stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    class Kernel(harness._Kernel):
        def __init__(self):
            super().__init__()
            self.caller_executions = 0
        async def execute(self, code, **kwargs):
            if 'HANDOFF_MAIN' in code and 'base64.b64decode' not in code:
                self.caller_executions += 1
                return {'status': 'error', 'outputs': [{'type': 'error',
                    'ename': 'AnalysisException', 'evalue': 'cannot resolve type in value schema',
                    'traceback': ['AnalysisException: cannot resolve type in value schema']}]}
            return await super().execute(code, **kwargs)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_pip/source.ipynb', kernel, 'handoff', '01_task', {}, run_all=True))
    assert result['status'] == 'FAIL', result
    assert kernel.caller_executions == 1
    assert len(model_calls) == 7
    assert any('UNRESOLVED' in cr.get('note', '') for cr in result['cell_results']), result


@pytest.mark.parametrize('variable', ['it', 'here', 'we', 'you', 'this'])
@pytest.mark.parametrize('delivery', ['submit_code', 'text'])
def test_handoff_normalization_preserves_valid_fenced_caller_edit(monkeypatch, variable, delivery):
    from types import SimpleNamespace as NS
    original = 'result = 1\n'
    fixed = f'{variable} = 2\nresult = {variable}\n'
    history = [_entry('def helper(): return None', task_key=am._current_task_key)]
    history[0]['_child_replay_owner'] = NS(completed_repair=object())
    monkeypatch.setattr(am, '_cell_history', history)
    fenced = '```python\n' + fixed + '```'
    reply = NS(content=[NS(type='tool_use', name='submit_code', id='caller', input={'code': fenced})]
               if delivery == 'submit_code' else [NS(type='text', text=fenced)],
               stop_reason='tool_use' if delivery == 'submit_code' else 'end_turn',
               usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=lambda **kw: reply)))
    delivered = asyncio.run(am.call_fix(original, 'fixture failure', [], 1, session=object()))
    namespace = {}
    exec(delivered, namespace)
    assert namespace['result'] == 2
    assert delivered.strip() == fixed.strip()


def test_parallel_unchanged_submission_keeps_cluster_cap_across_reserve(monkeypatch):
    import copy
    from types import SimpleNamespace as NS
    calls, dispatched = [], []
    def create(**kwargs):
        calls.append(copy.deepcopy(kwargs))
        turn = len(calls)
        names = {t['name'] for t in kwargs['tools']}
        name = 'run_on_cluster' if 'run_on_cluster' in names else 'make_note'
        blocks = [NS(type='tool_use', name=name, id=f'tool-{turn}',
                     input={'code': 'probe'} if name == 'run_on_cluster' else {'note': 'CANNOT_FIX: fixture blocker'})]
        if turn > 2:
            blocks.insert(0, NS(type='tool_use', name='submit_code', id=f'caller-{turn}',
                                input={'code': 'helper()'}))
        return NS(content=blocks, stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    async def handle(name, payload, **kwargs):
        dispatched.append(name)
        return 'investigation result' if name == 'run_on_cluster' else 'Note recorded'
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(am, '_handle_tool_call', handle)
    monkeypatch.setattr(am, 'MAX_RUN_ON_CLUSTER_PER_CALL', 3)
    with pytest.raises(am.UnresolvedRepairError):
        asyncio.run(am.call_opus_with_tools('fix', 'helper()', session=object(),
                    max_tool_rounds=1, repair_caller='helper()'))
    assert len(calls) == 8  # two normal + three retrieval + three delivery
    assert dispatched == ['run_on_cluster'] * 3 + ['make_note'] * 3
    results = [b for msg in calls[-1]['messages'] if isinstance(msg['content'], list)
               for b in msg['content'] if isinstance(b, dict) and b.get('type') == 'tool_result']
    assert sum('REFUSED: run_on_cluster budget exhausted' in b['content'] for b in results) == 2
    assert len({b['tool_use_id'] for b in results}) == len(results)


@pytest.mark.parametrize('parallel_submit', [False, True])
@pytest.mark.parametrize('fail_save', [False, True])
def test_handoff_preserves_active_source_owner_fixup(
        child_replay_flow, monkeypatch, parallel_submit, fail_save):
    from types import SimpleNamespace as NS
    notebook, run, Kernel, original, repaired, caller, messages, _ = child_replay_flow
    # A completed, unrelated dependency enables the handoff; the actual repair
    # belongs to an active parent source owner and must still use fixup_cell.
    notebook('bootstrap', ['# CHILD_REPLAY_BOOTSTRAP\nBOOTSTRAP_READY = True'])
    parent = notebook('parent', ['%run ./bootstrap', original, caller])
    before = parent.read_text()
    calls = []
    def create(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            target = next(e for e in am._cell_history if 'def model_selection' in e['replay_code'])
            assert target['_child_replay_owner'].active
            blocks = [NS(type='tool_use', name='fixup_cell', id='fix-parent', input={
                'start_index': target['index'], 'fixed_code': repaired,
                'why': 'repair active parent source through its existing owner'})]
            if parallel_submit:
                blocks.insert(0, NS(type='tool_use', name='submit_code', id='parallel', input={'code': caller}))
        else:
            blocks = [NS(type='tool_use', name='submit_code', id=f'caller-{len(calls)}', input={'code': caller})]
        return NS(content=blocks, stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    kernel = Kernel()
    kernel.fail_save = fail_save
    result = asyncio.run(run(parent, kernel))
    assert kernel.namespace.get('result') == 'rendered', (result, messages)
    assert len(calls) == (1 if parallel_submit else 2)
    if fail_save:
        assert result['status'] == 'error' and result['kind'] == 'CHILD_PERSISTENCE'
        assert parent.read_text() == before
    else:
        assert result['status'] == 'ok' and not result.get('failed_cells'), result
        assert 'return Idisplay(' in parent.read_text()
        fresh = Kernel()
        assert asyncio.run(run(parent, fresh))['status'] == 'ok'
        assert fresh.namespace['result'] == 'rendered'
        assert len(calls) == (1 if parallel_submit else 2)


@pytest.mark.parametrize('invalid_tool', ['fixup_cell', 'repair_dependency'])
@pytest.mark.parametrize('paired_submission', [False, True])
@pytest.mark.parametrize('submission_first', [False, True])
def test_recoverable_fixup_index_error_reaches_model(
        child_replay_flow, monkeypatch, paired_submission, submission_first, invalid_tool):
    from types import SimpleNamespace as NS
    notebook, run, Kernel, original, repaired, caller, _, _ = child_replay_flow
    notebook('bootstrap', ['# CHILD_REPLAY_BOOTSTRAP\nBOOTSTRAP_READY = True'])
    parent = notebook('parent', ['%run ./bootstrap', original, caller])
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        turn = len(calls)
        if turn == 1:
            payload = ({'start_index': 999, 'why': 'repair the helper', 'fixed_code': repaired}
                       if invalid_tool == 'fixup_cell' else
                       {'index': 999, 'why': 'repair the helper', 'fixed_code': repaired})
            blocks = [NS(type='tool_use', name=invalid_tool,
                         id='mistaken-index', input=payload)]
            if paired_submission:
                submit = NS(type='tool_use', name='submit_code', id='submit', input={'code': caller})
                blocks.insert(0 if submission_first else 1, submit)
        elif turn == 2:
            target = next(e for e in am._cell_history if 'def model_selection' in e['replay_code'])
            blocks = [NS(type='tool_use', name='fixup_cell', id='correct-index', input={
                'start_index': target['index'], 'why': 'use the actual history index',
                'fixed_code': repaired})]
        else:
            blocks = [NS(type='tool_use', name='submit_code', id=f'submit-{turn}',
                         input={'code': caller})]
        return NS(content=blocks, stop_reason='tool_use',
                  usage=NS(input_tokens=1, output_tokens=1))

    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    kernel = Kernel()
    result = asyncio.run(run(parent, kernel))
    assert result['status'] == 'ok', result
    assert kernel.namespace['result'] == 'rendered'
    assert 'return Idisplay(' in parent.read_text()
    assert len(calls) == 3
    if paired_submission:
        feedback = calls[1]['messages']
        assert any(
            item.get('tool_use_id') == 'mistaken-index'
            and 'ERROR:' in item.get('content', '')
            and item.get('is_error') is True
            for message in feedback if isinstance(message.get('content'), list)
            for item in message['content'] if isinstance(item, dict)
        )
        assert kernel.saves > 0
        assert not am._fixup_persistence_failures


@pytest.mark.parametrize('changed', [False, True])
def test_fixup_receipt_requires_applied_source_not_successful_replay(monkeypatch, changed):
    source = 'value = 1'
    source_ref = [source]
    am._cell_history.append(_entry(source, source_ref=source_ref))
    monkeypatch.setattr(am, '_replay_cell_fn', jm._replay_cell_entry)
    async def scenario():
        before = am._fixup_source_receipt.get()
        result = await am._handle_tool_call('fixup_cell', {
            'start_index': 0, 'fixed_code': 'value = 2' if changed else source,
            'why': 'source acknowledgement fixture'}, session=_SuccessfulSession())
        assert result.startswith('OK:'), result
        assert am._fixup_source_receipt.get() == before + int(changed)
        assert source_ref == ['value = 2' if changed else source]
    asyncio.run(scenario())


def test_fixup_source_ack_is_isolated_between_concurrent_contexts(monkeypatch):
    from contextvars import ContextVar
    monkeypatch.setattr(am, '_fixup_source_receipt', ContextVar('test_source_receipt', default=0))
    source_ref = ['value = 1']
    am._cell_history.append(_entry(source_ref[0], source_ref=source_ref))
    monkeypatch.setattr(am, '_replay_cell_fn', jm._replay_cell_entry)
    async def scenario():
        applied = asyncio.Event()
        async def repair():
            result = await am._handle_tool_call('fixup_cell', {
                'start_index': 0, 'fixed_code': 'value = 2', 'why': 'context isolation'},
                session=_SuccessfulSession())
            assert result.startswith('OK:'), result
            applied.set()
            return am._fixup_source_receipt.get()
        async def unrelated():
            await applied.wait()
            return am._fixup_source_receipt.get()
        assert await asyncio.gather(repair(), unrelated()) == [1, 0]
        assert am._fixup_source_receipt.get() == 0
    asyncio.run(scenario())


@pytest.mark.parametrize("redacted", [False, True])
@pytest.mark.parametrize("variant", ["constant", "except"])
@pytest.mark.parametrize("magic", ["", "%python\n", "%pyspark\n"])
def test_completed_dependency_cannot_deliver_fabricated_credential(
        child_replay_flow, monkeypatch, redacted, variant, magic):
    """PR #305 review: a model repair must not turn a lookup failure into PASS."""
    from types import SimpleNamespace
    notebook, run, Kernel, *_ = child_replay_flow
    source = (magic + "# CHILD_REPLAY_DEF\ndef helper():\n"
              "    return aidputils.secrets.get(scope='fixture', key='token')\n")
    sources = (["# CHILD_REPLAY_AUTH\n# WebClient(token='fixture-secret')\n"]
               if redacted else []) + [source]
    helper = notebook("helper", sources)
    parent = notebook("parent", ["%run ./helper", "# CHILD_REPLAY_CALL\nresult = helper()\n"])
    replacement = ("def helper():\n    return 'invented-workaround'\n" if variant == "constant" else
                   "def helper():\n    try:\n"
                   "        return aidputils.secrets.get(scope='fixture', key='token')\n"
                   "    except Exception:\n        return 'invented-workaround'\n")
    candidate = ("# CHILD_REPLAY_DEF\n"
                 + "".join("# " + line + "\n" for line in source.splitlines())
                 + "# Oracle tool modification: synthetic masking candidate\n" + replacement)
    failures = []
    async def fix(code, error, *args, session=None, **kwargs):
        target = next(e for e in reversed(am._cell_history)
                      if e["notebook_path"].endswith("helper.ipynb"))
        try:
            failures.append(await am._handle_tool_call("repair_dependency", {
                "index": target["index"], "fixed_code": candidate,
                "why": "synthetic model masking reply",
            }, session=session))
        except am.FixupPersistenceError as exc:
            failures.append(str(exc))
            raise
        return code
    monkeypatch.setattr(jm, "call_fix", fix)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    class NotebookKernel(Kernel):
        async def execute(self, code, **kwargs):
            # AIDP accepts these native headers; the local fixture uses Python.
            return await super().execute(jm._python_cell_ast_source(code), **kwargs)
    kernel = NotebookKernel()
    kernel.namespace["aidputils"] = SimpleNamespace(
        secrets=SimpleNamespace(get=lambda name, key: "real-fixture-result"))
    outcome = asyncio.run(run(parent, kernel))
    assert kernel.namespace.get("result") != "invented-workaround"
    assert failures and not any(item.startswith("OK:") for item in failures)
    assert outcome.get("failed_cells") or outcome["status"] == "error"
    assert all(candidate not in code for code in kernel.executed)
    saved = json.loads(helper.read_text())
    assert any("scope=" in "".join(cell["source"]) for cell in saved["cells"])
    assert "invented-workaround" not in helper.read_text()
    assert "invented-workaround" not in jm._notebook_content_cache[str(helper)]
    assert am._fixup_persistence_failures


@pytest.mark.parametrize("body", [
    "return 'fabricated'",
    "value = 'fabricated'\n    return value",
    "return None",
    "return str('fabricated')",
    "return aidputils.secrets.get('fixture', key='token') or 'fabricated'",
    "if False:\n        return aidputils.secrets.get('fixture', key='token')\n    return 'fabricated'",
    "try:\n        return aidputils.secrets.get('fixture', key='token')\n    except Exception:\n        return 'fabricated'",
    "try:\n        return aidputils.secrets.get('fixture', key='token')\n    except:\n        pass",
    "return fake.secrets.get('fixture', key='token')",
])
@pytest.mark.parametrize("magic", ["", "%python\n", "%pyspark\n"])
def test_credential_repair_preserves_result_and_failure_flow(body, magic):
    original = magic + "def helper():\n    return aidputils.secrets.get(scope='fixture', key='token')\n"
    candidate = magic + "def helper():\n    " + body + "\n"
    messages = []
    assert jm._reject_neutered_fix(original, original, candidate, messages.append) == original
    assert messages and "manual source repair" in messages[-1]
    with pytest.raises(am.FixupPersistenceError, match="removed or masked"):
        jm._check_replay_source(original, candidate)


@pytest.mark.parametrize("root", ["dbutils", "aidputils"])
@pytest.mark.parametrize("original_body", [
    "return LOOKUP",
    "token = LOOKUP\n    return token",
    "return {'Authorization': 'Bearer ' + LOOKUP}",
    "try:\n        return LOOKUP\n    except LookupError:\n        raise",
])
@pytest.mark.parametrize("arguments", ["'fixture', key='token'", "name='fixture', key='token'"])
@pytest.mark.parametrize("magic", ["", "%python\n", "%pyspark\n"])
def test_credential_repair_allows_signature_fixes(root, original_body, arguments, magic):
    body = original_body.replace("LOOKUP", root + ".secrets.get(scope='fixture', key='token')")
    original = magic + "def helper():\n    " + body + "\n"
    fixed = original.replace(root + ".secrets.get(scope='fixture', key='token')",
                             "aidputils.secrets.get(" + arguments + ")")
    # Comments, quotes and layout do not alter the structural refusal boundary.
    fixed = magic + "# Oracle tool modification: fix native lookup signature\n" + fixed.removeprefix(magic)
    assert jm._check_replay_source(original, fixed) == fixed


def test_credential_guard_ignores_text_and_requires_manual_wider_changes():
    text = "def helper():\n    return 'aidputils.secrets.get(scope=...)'\n"
    replacement = "def helper():\n    return 'different text'\n"
    assert jm._reject_neutered_fix(text, text, replacement) == replacement
    original = "def helper():\n    return aidputils.secrets.get(scope='fixture', key='token')\n"
    wider = original.replace("scope=", "name=").replace("    return ", "    print('diagnostic')\n    return ")
    assert jm._reject_neutered_fix(original, original, wider) == original
