"""Confirmed AIDP target paths must not enter LLM repair.

Spark's structured ``PATH_NOT_FOUND`` error is definitive only after the
reported path is already in an AIDP target namespace.  Source paths still need
the existing translation flow, and a printed marker is not an execution error.
"""

import asyncio

import pytest

from scripts.job_migrate import _confirmed_missing_target_path
from test_pip_magic_gate import _Kernel, _patch_common, jm


_OCI_PATH = "oci://example-bucket@example-namespace/data/dt=20260909"


@pytest.mark.parametrize(
    ("output", "status", "raw_outputs", "expected"),
    [
        (
            f"AnalysisException: [PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}.",
            "error",
            [],
            _OCI_PATH,
        ),
        (
            "Traceback (most recent call last):\n"
            "AnalysisException: [PATH_NOT_FOUND] Path does not exist: "
            "/Volumes/default/default/data/missing/.",
            "error",
            [],
            "/Volumes/default/default/data/missing/",
        ),
        (
            "Command ID 00000000-0000-0000-0000-000000000000 failed with error: "
            f"[PATH_NOT_FOUND] Path does not exist: `{_OCI_PATH}`",
            "ok",
            [],
            _OCI_PATH,
        ),
    ],
    ids=["oci-error", "volumes-traceback", "aidp-stream-error"],
)
def test_structured_missing_aidp_target_path_is_confirmed(
    output, status, raw_outputs, expected
):
    assert _confirmed_missing_target_path(output, status, raw_outputs) == expected


@pytest.mark.parametrize(
    "path",
    [
        "s3://source-bucket/data/dt=20260909",
        "s3a://source-bucket/data/dt=20260909",
        "dbfs:/FileStore/data/dt=20260909",
        "/dbfs/FileStore/data/dt=20260909",
    ],
)
def test_source_paths_remain_eligible_for_translation(path):
    output = f"AnalysisException: [PATH_NOT_FOUND] Path does not exist: {path}."
    assert _confirmed_missing_target_path(output, "error", []) is None


def test_printed_marker_is_not_an_execution_failure():
    output = f"[PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}."
    assert _confirmed_missing_target_path(output, "ok", []) is None


def test_printed_analysis_exception_text_is_not_an_execution_failure():
    output = f"AnalysisException: [PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}."
    assert _confirmed_missing_target_path(output, "ok", []) is None


def test_unrelated_error_with_printed_marker_is_not_misclassified():
    output = (
        f'print("[PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}.")\n'
        "TypeError: unsupported operand type"
    )
    raw_outputs = [
        {"type": "error", "ename": "TypeError", "evalue": "unsupported operand"}
    ]
    assert _confirmed_missing_target_path(output, "error", raw_outputs) is None


def test_permission_failure_is_not_misclassified_as_missing_data():
    output = (
        "NotAuthorizedOrNotFound: permission denied\n"
        f"[PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}."
    )
    assert _confirmed_missing_target_path(output, "error", []) is None


def test_caught_traceback_followed_by_success_is_not_missing_data():
    output = (
        "Traceback (most recent call last):\n"
        f"AnalysisException: [PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}.\n"
        "Fallback source succeeded; rows=1532"
    )
    assert _confirmed_missing_target_path(output, "ok", []) is None


@pytest.mark.parametrize("output", ["", None])
@pytest.mark.parametrize("status", ["error", "ok"])
def test_raw_error_without_rendered_traceback_is_classified(output, status):
    raw = [{"type": "error", "ename": "AnalysisException",
            "evalue": f"[PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}."}]
    assert _confirmed_missing_target_path(output, status, raw) == _OCI_PATH


def test_raw_permission_error_without_rendered_output_is_not_missing_data():
    raw = [{"type": "error", "ename": "AnalysisException", "evalue": (
        f"NotAuthorizedOrNotFound: [PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}."
    )}]
    assert _confirmed_missing_target_path("", "error", raw) is None


@pytest.mark.parametrize("separator", ["\n", " x" * 750], ids=["different-line", "distant-text"])
def test_marker_cannot_join_an_unrelated_printed_path(separator):
    output = (
        "AnalysisException: [PATH_NOT_FOUND]" + separator
        + f"Path does not exist: {_OCI_PATH}."
    )
    assert _confirmed_missing_target_path(output, "error", []) is None


def test_missing_path_phrase_cannot_join_a_path_on_another_line():
    output = f"AnalysisException: [PATH_NOT_FOUND] Path does not exist:\n{_OCI_PATH}"
    assert _confirmed_missing_target_path(output, "error", []) is None


class _MissingTargetPathKernel(_Kernel):
    def __init__(self, with_traceback=True):
        super().__init__()
        self.customer_attempts = 0
        self.with_traceback = with_traceback

    async def execute(self, code, **kwargs):
        if "spark.read.parquet" in code and _OCI_PATH in code:
            self.customer_attempts += 1
            message = (
                f"[PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}. "
                "SQLSTATE: 42K03"
            )
            return {
                "status": "error",
                "outputs": [
                    {
                        "type": "error",
                        "ename": "AnalysisException",
                        "evalue": message,
                        **({"traceback": [f"AnalysisException: {message}"]}
                           if self.with_traceback else {}),
                    }
                ],
            }
        return await super().execute(code, **kwargs)


def _notebook(source):
    return {
        "cells": [
            {
                "cell_type": "code",
                "source": [source],
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            }
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }


@pytest.mark.parametrize("strict", [False, True], ids=["default", "strict"])
@pytest.mark.parametrize("with_traceback", [True, False], ids=["traceback", "raw-only"])
def test_process_notebook_stops_before_llm_retry(monkeypatch, tmp_path, strict, with_traceback):
    source = (
        f'paths = ["{_OCI_PATH}", '
        '"oci://example-bucket@example-namespace/data/dt=20260910"]\n'
        "df = spark.read.parquet(*paths)\n"
        "print(df.count())"
    )
    _patch_common(monkeypatch, tmp_path, _notebook(source), [])
    kernel = _MissingTargetPathKernel(with_traceback)
    calls = {"evaluate": 0, "fix": 0, "recover": 0}

    async def unexpected_evaluator(*args, **kwargs):
        calls["evaluate"] += 1
        raise AssertionError("confirmed target-path absence must bypass evaluation")

    async def unexpected_fix(*args, **kwargs):
        calls["fix"] += 1
        raise AssertionError("confirmed target-path absence must bypass repair")

    async def unexpected_recovery(*args, **kwargs):
        calls["recover"] += 1
        raise AssertionError("confirmed target-path absence must bypass date recovery")

    async def summarize(*args, **kwargs):
        return "missing target path cell"

    monkeypatch.setattr(jm, "call_opus_with_tools", unexpected_evaluator)
    monkeypatch.setattr(jm, "call_fix", unexpected_fix)
    monkeypatch.setattr(jm, "attempt_data_recovery", unexpected_recovery)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    monkeypatch.setattr(jm, "_apply_namespace_from_mapping", lambda code: code)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])

    result = asyncio.run(
        jm.process_notebook(
            "/Workspace/example/source.ipynb",
            kernel,
            "example_job",
            "01_task",
            {},
        )
    )

    assert kernel.customer_attempts == 1
    assert calls == {"evaluate": 0, "fix": 0, "recover": 0}
    assert result["status"] == "PARTIAL_DATA_UNAVAILABLE"
    assert result["failed"] == 0
    assert result["data_unavailable"] == 1
    assert result["cell_results"][0]["attempts"] == 1
    assert result["cell_results"][0]["missing_path"] == _OCI_PATH
    assert "spark.read.parquet(*paths)" in result["cell_results"][0]["code"]
    assert "_aidp_read_dbfs" not in result["cell_results"][0]["code"]


def test_process_notebook_continues_after_logged_successful_fallback(monkeypatch, tmp_path):
    # Warning logging is already accepted by the normal output verifier.
    # The new missing-path gate must not override that successful result.
    class FallbackKernel(_Kernel):
        async def execute(self, code, **kwargs):
            if 'print("FALLBACK_RESULT")' in code:
                return {"status": "ok", "outputs": [{"type": "stream", "text": (
                    "WARNING Traceback (most recent call last):\n"
                    f"WARNING AnalysisException: [PATH_NOT_FOUND] Path does not exist: {_OCI_PATH}.\n"
                    "Fallback source succeeded; rows=1532"
                )}]}
            return await super().execute(code, **kwargs)

    notebook = _notebook('print("FALLBACK_RESULT")')
    notebook["cells"].extend(_notebook('print("NEXT_CELL")')["cells"])
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "fetch_recent_spark_errors", lambda *a, **k: "")
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    kernel = FallbackKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/example/source.ipynb", kernel, "example_job", "01_task", {}
    ))
    assert result["status"] == "PASS"
    assert result["data_unavailable"] == 0
    assert result["failed"] == 0
    assert any('print("NEXT_CELL")' in code for code in kernel.executed)
