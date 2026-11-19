"""REF-293: run ce6883ce (2026-10-29 IST) removed a source-defined skip.

Pin prompt delivery and the existing source/history evidence channel. Scripted
model replies do not prove model compliance; that requires a live model replay.
"""
import asyncio
import hashlib

import pytest

from test_caught_output_verification import _run
from test_conditional_exit_below_execute import am, jm, _patch_common
from test_eval_verdict_gate import _notebook, _ScriptedKernel


# Exact original section from the main branch at c094cd6daa65b8f9a88ba8e9fea4aced810a1cbd.
ORIGINAL_GUIDANCE_SHA256 = "73b3e8388ac02846db7de835c50f7febd5fbaa47ae1a91744c9439ab4e91dc14"


def _assert_provenance_guidance(prompt):
    assert jm._CAUGHT_ERROR_EVAL_GUIDANCE in prompt
    assert "only when assessing an observed caught failure" in prompt


def test_original_fallback_guidance_is_preserved_verbatim():
    heading = "CAUGHT ERRORS AND SOURCE-DEFINED FALLBACKS:"
    scope, original_body = jm._CAUGHT_ERROR_EVAL_GUIDANCE.split("This execution", 1)
    assert scope == heading + "\n" + (
        "Apply the following fallback checks only when assessing an observed caught failure\n"
        "in this cell or its executed helpers. Evidence may come from printed output or\n"
        "read-only investigation. A successful cell without an observed caught failure\n"
        "does not need to demonstrate a fallback or independently establish upstream data\n"
        "completeness to satisfy this section. Continue to assess its output under the\n"
        "other verification rules above and below.\n")
    original = heading + "\nThis execution" + original_body
    assert hashlib.sha256(original.encode()).hexdigest() == ORIGINAL_GUIDANCE_SHA256


def test_cell_evaluator_receives_provenance_check(monkeypatch, tmp_path):
    _, _, evaluations, _ = _run(monkeypatch, tmp_path)
    _assert_provenance_guidance(evaluations[0][0])


def test_successful_output_receives_conditional_fallback_scope(monkeypatch, tmp_path):
    # Delivery check for the ordinary-success path; the scripted PASS does not
    # prove real-model compliance with this scope instruction.
    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"))
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    evaluations = []

    async def evaluate(system, user, **kwargs):
        evaluations.append((system, user))
        return "PASS: expected successful output", {"input": 0, "output": 0}

    async def unexpected_fix(*args, **kwargs):
        pytest.fail("ordinary success should not enter repair")

    monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)
    monkeypatch.setattr(jm, "call_fix", unexpected_fix)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", _ScriptedKernel(),
        "fallback_scope", "task", {}, run_all=True))
    assert result["status"] == "PASS"
    assert len(evaluations) == 1
    system, user = evaluations[0]
    assert "CAUGHT ERRORS AND SOURCE-DEFINED FALLBACKS:" not in system
    assert "Output findings to assess: []" in user
    assert "cell A produced plenty of output" in user
    assert not any(cell.get("output_warning") for cell in result["cell_results"])


@pytest.mark.parametrize("message,include_guidance", [
    ("java.io.IOException: optional read failed", True),
    ("Traceback (most recent call last):\nValueError: optional source empty", True),
    ("NotAuthorizedOrNotFound: optional source denied", True),
    ("WARNING Exception: optional diagnostic only", False),
    ("optional source unavailable", False),
])
def test_existing_printed_findings_control_fallback_guidance(
        monkeypatch, tmp_path, message, include_guidance):
    _, _, evaluations, _ = _run(monkeypatch, tmp_path, message=message)
    assert evaluations
    for system, _ in evaluations:
        assert (jm._CAUGHT_ERROR_EVAL_GUIDANCE in system) == include_guidance


@pytest.mark.parametrize("raw_dbfs,spark_log", [(False, True), (True, False)])
def test_other_evaluation_triggers_do_not_add_fallback_guidance(
        monkeypatch, tmp_path, raw_dbfs, spark_log):
    from test_raw_dbfs_runtime_evidence import _DbfsKernel, RAW_QUIET

    source = RAW_QUIET if raw_dbfs else "SENTINEL_A = 1\n"
    _patch_common(monkeypatch, tmp_path, _notebook(source))
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "fetch_recent_spark_errors",
                        lambda *a, **k: "BucketNotFound: synthetic Spark log" if spark_log else "")
    evaluations = []

    async def evaluate(system, user, **kwargs):
        if not system.startswith("You are verifying a migrated Spark cell"):
            assert system == jm.CELL_MIGRATE_PROMPT
            return source, {"input": 0, "output": 0}
        evaluations.append((system, user))
        return "PASS: fixture output verified", {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)
    kernel = _DbfsKernel() if raw_dbfs else _ScriptedKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "other_eval_trigger", "task", {}, run_all=True))
    assert result["status"] == "PASS"
    assert evaluations
    for system, user in evaluations:
        assert jm._CAUGHT_ERROR_EVAL_GUIDANCE not in system
        assert ("Raw DBFS paths used at runtime" in user) == raw_dbfs
        assert ("Spark log error:" in user) == spark_log


def test_repair_retains_existing_source_inspection_context(monkeypatch):
    prompts = []
    caller = "combine_images([], 'Activation')"

    async def capture(system, user, **kwargs):
        prompts.append((system, user))
        return caller, {"input": 0, "output": 0}

    monkeypatch.setattr(am, "call_opus_with_tools", capture)
    monkeypatch.setattr(am, "_cell_history", [])
    fixed = asyncio.run(am.call_fix(
        caller, "ISSUE: skip allegedly migration-added", [], 1,
        session=object(), notebook_path="/Workspace/exported/source.ipynb"))
    assert fixed == caller
    _, user = prompts[0]
    assert "Source notebook path: /Workspace/exported/source.ipynb" in user
    assert ("Use this path with read_notebook_source if you need to check the original cell content."
            in user)


@pytest.mark.parametrize("origin", ["source", "migration", "unavailable"])
@pytest.mark.parametrize("tagged", [False, True])
def test_existing_tools_distinguish_original_from_executed_helper(
        monkeypatch, origin, tagged):
    # An annotation is independent of origin; neither its presence nor absence
    # replaces the exported source. No live reads or model calls in this test.
    plain = "def combine_images(images):\n    return max(images)\n"
    guarded = ("def combine_images(images):\n"
               "    if not images:\n        print('[skip] empty image list')\n        return\n"
               "    return max(images)\n")
    code = guarded + ("# Oracle tool modification: annotation\n" if tagged else "")
    path = "/Workspace/exported/helper.ipynb"
    entry = {"index": 0, "notebook_path": path, "cell_idx": 12, "status": "ok",
             "summary": "helper", "output_preview": "", "final_code": code[:30],
             "replay_code": code}
    monkeypatch.setattr(am, "_cell_history", [entry])
    reads = []

    def download(requested):
        reads.append(requested)
        return None if origin == "unavailable" else _notebook(
            guarded if origin == "source" else plain)

    monkeypatch.setattr(am, "_download_notebook_source", download)
    history = asyncio.run(am._handle_tool_call("get_history_entry", {"index": 0}))
    original = asyncio.run(am._handle_tool_call("read_notebook_source", {"path": path}))
    assert code in history and path in history
    assert reads == [path]
    if origin == "unavailable":
        assert "Notebook not found" in original
    else:
        assert original == (guarded if origin == "source" else plain)
        assert ("[skip]" in original) == (origin == "source")
