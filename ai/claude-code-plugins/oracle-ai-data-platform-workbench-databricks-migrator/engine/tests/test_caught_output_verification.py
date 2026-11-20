"""Printed failures need a verdict before repairing a completed Python cell.

Incident: run c07df0fa, 2026-10-23 IST. The source's optional read caught an
IOException and the caller used its existing null fallback. Replaying the
completed join cell solely because stdout contained ``Exception:`` duplicated
columns. Exercise execution, evaluation, saved reports and job aggregation.
"""
import asyncio
import contextlib
import io
import json

import pytest

from test_eval_verdict_gate import _notebook, jm
from test_conditional_exit_below_execute import _Kernel, _patch_common


SOURCE = '''# OPTIONAL_READ_CASE
def get_offer_roi():
    try:
        return read_offers()
    except Exception as e:
        print(f"Error reading optional offers: {e}")
    return None

offer_roi_table = get_offer_roi()
base_data = base_data + ["joined_features"]
offer_roi = offer_roi_table if offer_roi_table is not None else None
'''
REASON = "source-defined null ROI fallback completed; offers data remains unvalidated"


class _OptionalReadKernel(_Kernel):
    def __init__(self, message, *, status="ok", structured_error=False):
        super().__init__()
        self.calls = 0
        self.status = status
        self.structured_error = structured_error

        def read_offers():
            raise OSError(message)

        self.namespace = {"read_offers": read_offers, "base_data": []}

    async def execute(self, code, **kwargs):
        if "offer_roi_table = get_offer_roi()" in code and "base64.b64decode" not in code:
            self.executed.append(code)
            self.calls += 1
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exec(code, self.namespace)
            outputs = [{"type": "stream", "name": "stdout", "text": stdout.getvalue()}]
            if self.structured_error:
                outputs.append({"type": "error", "ename": "RuntimeError",
                                "evalue": "required output missing",
                                "traceback": ["RuntimeError: required output missing"]})
            return {"status": self.status, "outputs": outputs}
        return await super().execute(code, **kwargs)


def _run(monkeypatch, tmp_path, *, message="java.io.IOException: lookup failed",
         replies=None, strict=False, direct=False, status="ok", structured_error=False):
    _patch_common(monkeypatch, tmp_path, _notebook(SOURCE))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", direct)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    monkeypatch.setattr(jm, "_cell_history", [])
    replies = iter(replies if replies is not None else [f"PASS: {REASON}"])
    evaluations, fixes = [], []

    async def evaluate(system, user, **kwargs):
        evaluations.append((system, user))
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply, {"input": 0, "output": 0}

    async def fix(code, *args, **kwargs):
        fixes.append(code)
        return code

    monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)
    monkeypatch.setattr(jm, "call_fix", fix)
    kernel = _OptionalReadKernel(message, status=status, structured_error=structured_error)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "optional_read", "task", {}, run_all=True))
    return result, kernel, evaluations, fixes


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("message", [
    "java.io.IOException: lookup failed",
    "Traceback (most recent call last):\nValueError: optional source empty",
    "NotAuthorizedOrNotFound: optional source denied",
])
def test_source_fallback_passes_once_with_durable_warning(monkeypatch, tmp_path, strict, message):
    result, kernel, evaluations, fixes = _run(
        monkeypatch, tmp_path, strict=strict, message=message)
    assert result["status"] == "PASS", result
    assert not result["job_should_stop"]
    assert result["data_unavailable"] == result["failed"] == result["fixed"] == 0
    assert kernel.calls == 1 and not fixes
    assert kernel.namespace["base_data"] == ["joined_features"]
    assert kernel.namespace["offer_roi"] is None
    system, user = evaluations[0]
    assert "source-defined" in system and "Do not re-execute" in system
    assert "Original source cell" in user and SOURCE in user
    assert message in user
    note = result["cell_results"][0]["note"]
    assert "WARNING" in note and REASON in note
    reports = {p.rsplit("/", 1)[-1]: text for p, text in kernel.files.items()}
    for name in ("migration_report.md", "test_report.md", "execution_log.txt"):
        assert note in reports[name]
    saved = next(json.loads(t) for p, t in kernel.files.items() if p.endswith(".ipynb"))
    assert SOURCE.strip() in "".join(saved["cells"][0]["source"])


@pytest.mark.parametrize("replies", [
    ["ISSUE: required outputs are missing | FIX: preserve the computation"] * 2,
    ["", ""] * 2,
    ["PASS: okay\nISSUE: required outputs missing"] * 4,
    [RuntimeError("evaluator unavailable")] * 2,
])
def test_output_is_not_accepted_without_a_completed_pass(monkeypatch, tmp_path, replies):
    result, _, evaluations, fixes = _run(monkeypatch, tmp_path, replies=replies)
    assert result["status"] == "FAIL" and result["job_should_stop"]
    assert evaluations and fixes
    assert not any(cr.get("output_warning") for cr in result["cell_results"])


@pytest.mark.parametrize("status,structured_error", [("error", False), ("ok", True)])
def test_execution_errors_cannot_be_overruled_by_a_model_pass(
        monkeypatch, tmp_path, status, structured_error):
    result, _, evaluations, _ = _run(
        monkeypatch, tmp_path, status=status, structured_error=structured_error)
    assert result["status"] == "FAIL" and result["job_should_stop"]
    assert not evaluations


def test_direct_execute_does_not_accept_printed_failures_without_evaluation(monkeypatch, tmp_path):
    result, _, evaluations, _ = _run(monkeypatch, tmp_path, direct=True)
    assert result["status"] == "FAIL" and result["failed"] == 1
    assert not evaluations


@pytest.mark.parametrize("strict", [False, True])
def test_warning_reaches_real_job_report_without_failing_job(monkeypatch, tmp_path, strict):
    import test_pip_magic_gate as job_harness

    original_patch = job_harness._patch_common
    evaluations = []

    async def evaluate(system, user, **kwargs):
        evaluations.append(user)
        return f"PASS: {REASON}", {"input": 0, "output": 0}

    def patch(*args, **kwargs):
        original_patch(*args, **kwargs)
        monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)
        monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")

    class JobKernel(_OptionalReadKernel):
        def __init__(self, **kwargs):
            super().__init__("java.io.IOException: lookup failed")

        async def run_stateless(self, code, **kwargs):
            return await self.execute(code, **kwargs)

    monkeypatch.setattr(job_harness, "_patch_common", patch)
    monkeypatch.setattr(job_harness, "_ReportingKernel", JobKernel)
    result, reports, _, kernel = job_harness._drive_reporting_job(
        monkeypatch, tmp_path, [SOURCE], strict=strict)
    task = result["task_results"]["task"]
    assert task["status"] == "PASS" and task["data_unavailable"] == 0
    assert len(evaluations) == kernel.calls == 1
    assert kernel.namespace["base_data"] == ["joined_features"]
    for name in ("JOB_REPORT.md", "migration_report.md", "test_report.md"):
        assert "WARNING" in reports[name] and REASON in reports[name]
    assert "errors, warnings, or unresolved dependencies" in reports["JOB_REPORT.md"]


@pytest.mark.parametrize("message", [
    'AnalysisException: [DATATYPE_MISMATCH.INVALID_JSON_SCHEMA] to_json input is STRING',
    'AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] dotted field cannot be resolved',
])
def test_evaluator_receives_schema_first_guidance_for_rendered_errors(
        monkeypatch, tmp_path, message):
    # Run 20261027T052128Z-c62c3887 spent most of cell 9 in evaluator data
    # scans before a schema-based repair. Run aaaaaaaa-0000-4000-8000-000000000206
    # still scanned upstream JSON with the initial schema-first guidance.
    # Pin delivery by the cell loop; this mock does not establish model compliance.
    result, _, evaluations, fixes = _run(
        monkeypatch, tmp_path, message=message,
        replies=["ISSUE: required output absent | FIX: inspect runtime schema"] * 2)
    assert result["status"] == "FAIL"
    assert len(evaluations) == 2 and len(fixes) == 1
    for system, user in evaluations:
        assert ("For type or column-resolution errors, inspect the relevant runtime schema\n"
                "and failing expression first.") in system
        assert ("Avoid row counts or payload scans unless needed\n"
                "to resolve the failure.") in system
        assert ("Report separate data-quality concerns without delaying\n"
                "a repair recommendation supported by schema evidence.") in system
        assert ("When the exception already establishes a type mismatch, diagnose it using\n"
                "the failing expression and runtime schema. Do not investigate upstream data\n"
                "quality before returning the repair recommendation unless it is necessary\n"
                "to determine that repair.") in system
        assert "Do not re-execute the completed cell" in system
        assert "A missing TABLE (TABLE_OR_VIEW_NOT_FOUND) is DATA_UNAVAIL" in system
        assert message in user and SOURCE in user
