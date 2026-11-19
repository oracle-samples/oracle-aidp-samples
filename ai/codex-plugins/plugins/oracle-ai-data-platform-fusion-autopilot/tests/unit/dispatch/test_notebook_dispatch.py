"""Unit tests for ``dispatch.notebook_dispatch.dispatch_notebook_and_fetch_marker``.

Phase 4.1 / D3 — the neutral end-to-end helper that wraps the
AidpRestClient sequence. The bootstrap-specific orchestration in
``commands/cluster_bootstrap_probe.py`` consumes this helper, so the
tests here exercise it with a mocked client + canned cluster outputs.

The import-boundary regression (the helper MUST NOT pull
``orchestrator/*`` into ``sys.modules``) ships in
``tests/unit/dispatch/test_imports.py``.
"""

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerDecodeError,
    DispatchMarkerEnvelopeMissing,
    DispatchPollTimeoutError,
    DispatchRunFailedError,
    DispatchUploadError,
)
from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_dispatch import (
    dispatch_notebook_and_fetch_marker,
)
from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (
    AidpRestError,
    RunResult,
)


MARKER_BEGIN = "AIDP_BOOTSTRAP_PROBE_MARKER_BEGIN"
MARKER_END = "AIDP_BOOTSTRAP_PROBE_MARKER_END"


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _b64_marker_cell(payload: dict) -> dict:
    """Build an executed-notebook cell whose stdout text carries the
    base64-wrapped marker envelope between MARKER_BEGIN / MARKER_END."""
    token = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    text = f"some chatter\n{MARKER_BEGIN} {token} {MARKER_END}\nmore chatter\n"
    return {"outputs": [{"text": text}]}


def _executed_notebook(*cells: dict) -> dict:
    return {"cells": list(cells)}


def _happy_marker_payload() -> dict:
    return {"ok": True, "marker": {"tenant": "demo-pod"}}


def _client(
    *,
    poll_result: RunResult | None = None,
    poll_exc: Exception | None = None,
    upload_exc: Exception | None = None,
    create_job_exc: Exception | None = None,
    submit_run_exc: Exception | None = None,
    fetch_output_exc: Exception | None = None,
    executed_notebook: dict | None = None,
) -> MagicMock:
    """Mocked AidpRestClient mirroring the methods the helper calls.

    Defaults to the happy path: SUCCESS poll, base64-wrapped marker in
    a single stdout cell.
    """
    client = MagicMock(name="AidpRestClient")

    # upload_notebook
    if upload_exc is not None:
        client.upload_notebook.side_effect = upload_exc
    else:
        client.upload_notebook.return_value = "/Workspace/path/probe.ipynb"

    # create_notebook_job
    if create_job_exc is not None:
        client.create_notebook_job.side_effect = create_job_exc
    else:
        client.create_notebook_job.return_value = "job-key"

    # submit_run
    if submit_run_exc is not None:
        client.submit_run.side_effect = submit_run_exc
    else:
        client.submit_run.return_value = "run-key"

    # poll_run
    if poll_exc is not None:
        client.poll_run.side_effect = poll_exc
    else:
        raw = {"taskToTaskRunMap": {"notebook_task": "trk-1"}}
        client.poll_run.return_value = poll_result or RunResult(
            status="SUCCESS", raw=raw
        )

    # fetch_output
    if fetch_output_exc is not None:
        client.fetch_output.side_effect = fetch_output_exc
    else:
        nb = executed_notebook or _executed_notebook(
            _b64_marker_cell(_happy_marker_payload())
        )
        client.fetch_output.return_value = json.dumps(nb)

    return client


def _call(client: MagicMock, **overrides):
    kwargs = dict(
        notebook={"cells": [], "nbformat": 4, "nbformat_minor": 5},
        workspace_path="/Workspace/Shared/fusion-autopilot-bootstrap/probe.ipynb",
        cluster_key="cluster-uuid",
        cluster_name="cluster_dev",
        job_name="aidp_fusion_autopilot_bootstrap_probe",
        marker_begin=MARKER_BEGIN,
        marker_end=MARKER_END,
        marker_b64=True,
    )
    kwargs.update(overrides)
    return dispatch_notebook_and_fetch_marker(client, **kwargs)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_parsed_marker_payload(self) -> None:
        client = _client()
        result = _call(client)
        assert result == _happy_marker_payload()

    def test_primitives_called_in_order(self) -> None:
        client = _client()
        _call(client)
        # Each primitive called once, in order: upload → create_job →
        # submit_run → poll_run → fetch_output.
        client.upload_notebook.assert_called_once()
        client.create_notebook_job.assert_called_once()
        client.submit_run.assert_called_once()
        client.poll_run.assert_called_once()
        client.fetch_output.assert_called_once()

    def test_create_job_args_thread_through(self) -> None:
        client = _client()
        _call(client)
        kw = client.create_notebook_job.call_args.kwargs
        assert kw["cluster_key"] == "cluster-uuid"
        assert kw["cluster_name"] == "cluster_dev"
        assert kw["name"] == "aidp_fusion_autopilot_bootstrap_probe"
        assert kw["task_key"] == "notebook_task"


# ---------------------------------------------------------------------------
# Per-primitive failure mapping (existing dispatch error classes)
# ---------------------------------------------------------------------------


class TestPerPrimitiveFailureMapping:
    def test_upload_failure_raises_upload_error(self) -> None:
        client = _client(upload_exc=AidpRestError("HTTP 500 upload"))
        with pytest.raises(DispatchUploadError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_UPLOAD_HTTP"
        assert "upload" in str(exc.value)

    def test_create_job_failure_raises_submit_error(self) -> None:
        client = _client(create_job_exc=AidpRestError("HTTP 400 job create"))
        with pytest.raises(DispatchJobSubmitError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_JOB_SUBMIT"

    def test_submit_run_failure_raises_submit_error(self) -> None:
        client = _client(submit_run_exc=AidpRestError("HTTP 503 submit"))
        with pytest.raises(DispatchJobSubmitError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_JOB_SUBMIT"

    def test_poll_timeout_raises_timeout_error(self) -> None:
        client = _client(poll_exc=AidpRestError("poll deadline exceeded after 1800s"))
        with pytest.raises(DispatchPollTimeoutError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_TIMEOUT"

    def test_poll_other_transport_failure_raises_fetch_error(self) -> None:
        # AidpRestError without "deadline exceeded" message → treat as
        # fetch-level (the cluster work might have completed but the
        # poll itself broke).
        client = _client(poll_exc=AidpRestError("connection reset"))
        with pytest.raises(DispatchFetchOutputError):
            _call(client)

    def test_fetch_output_failure_raises_fetch_error(self) -> None:
        client = _client(fetch_output_exc=AidpRestError("HTTP 404 fetchOutput"))
        with pytest.raises(DispatchFetchOutputError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_FETCH_OUTPUT"


# ---------------------------------------------------------------------------
# enrich_on_timeout — optional callback for partial-progress diagnostics
# ---------------------------------------------------------------------------


class TestEnrichOnTimeout:
    def test_callback_invoked_only_on_timeout(self) -> None:
        callback = MagicMock(return_value="partial progress: step 3/5")
        client = _client(poll_exc=AidpRestError("poll deadline exceeded after 1800s"))
        with pytest.raises(DispatchPollTimeoutError) as exc:
            _call(client, enrich_on_timeout=callback)
        callback.assert_called_once()
        # Enriched text appears in the exception message.
        assert "step 3/5" in str(exc.value)

    def test_callback_not_invoked_on_happy_path(self) -> None:
        callback = MagicMock()
        _call(_client(), enrich_on_timeout=callback)
        callback.assert_not_called()

    def test_callback_exception_swallowed(self) -> None:
        # If the enrichment callback itself raises, don't mask the
        # original timeout error — the operator still wants to know
        # the dispatch timed out.
        callback = MagicMock(side_effect=RuntimeError("oci raw-request blew up"))
        client = _client(poll_exc=AidpRestError("poll deadline exceeded after 1800s"))
        with pytest.raises(DispatchPollTimeoutError):
            _call(client, enrich_on_timeout=callback)


# ---------------------------------------------------------------------------
# Marker parse failures (new exception classes carry payload)
# ---------------------------------------------------------------------------


class TestMarkerParseFailures:
    def test_envelope_missing_raises_with_executed_notebook(self) -> None:
        # Notebook ran successfully but never emitted MARKER_BEGIN/END.
        nb = _executed_notebook({"outputs": [{"text": "no marker here\n"}]})
        client = _client(executed_notebook=nb)
        with pytest.raises(DispatchMarkerEnvelopeMissing) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_MARKER_ENVELOPE_MISSING"
        # The exception carries the executed notebook + stdout excerpt
        # so the caller can write the AIDPF-2049 companion log.
        assert exc.value.executed_notebook == nb
        assert "no marker here" in exc.value.stdout_excerpt

    def test_decode_error_when_base64_invalid(self) -> None:
        # MARKER_BEGIN/END present but the payload between them isn't
        # valid base64 (or isn't valid JSON after decoding).
        text = f"{MARKER_BEGIN} not-valid-base64!!! {MARKER_END}\n"
        nb = _executed_notebook({"outputs": [{"text": text}]})
        client = _client(executed_notebook=nb)
        with pytest.raises(DispatchMarkerDecodeError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_MARKER_DECODE"
        assert exc.value.executed_notebook == nb

    def test_truncated_envelope_raises_decode_error(self) -> None:
        # BEGIN present but END never — `value.index(end, b)` raises
        # ValueError → wrapped as DispatchMarkerDecodeError.
        text = f"chatter\n{MARKER_BEGIN} partial-token-no-end\n"
        nb = _executed_notebook({"outputs": [{"text": text}]})
        client = _client(executed_notebook=nb)
        with pytest.raises(DispatchMarkerDecodeError):
            _call(client)

    def test_marker_b64_false_uses_raw_json(self) -> None:
        # Legacy run-dispatch path: raw JSON between delimiters,
        # marker_b64=False.
        text = f"{MARKER_BEGIN} {json.dumps({'ok': True})} {MARKER_END}\n"
        nb = _executed_notebook({"outputs": [{"text": text}]})
        client = _client(executed_notebook=nb)
        result = _call(client, marker_b64=False)
        assert result == {"ok": True}

    def test_run_failed_with_no_envelope_raises_run_failed(self) -> None:
        # The cluster reported terminal FAILED status AND there's no
        # marker envelope to provide a structured error payload. This
        # is the only path where DispatchRunFailedError fires.
        nb = _executed_notebook({"outputs": [{"text": "crash trace...\n"}]})
        client = _client(
            poll_result=RunResult(
                status="FAILED",
                raw={"taskToTaskRunMap": {"notebook_task": "trk-1"}},
            ),
            executed_notebook=nb,
        )
        with pytest.raises(DispatchRunFailedError) as exc:
            _call(client)
        assert exc.value.code == "DISPATCH_RUN_FAILED"

    def test_executed_notebook_malformed_json_raises_envelope_missing(self) -> None:
        # fetch_output returned bytes that don't parse as JSON. The
        # helper can't extract stdout (no notebook structure) — raises
        # envelope-missing with no payload.
        client = MagicMock()
        client.upload_notebook.return_value = "/path"
        client.create_notebook_job.return_value = "job"
        client.submit_run.return_value = "run"
        client.poll_run.return_value = RunResult(
            status="SUCCESS",
            raw={"taskToTaskRunMap": {"notebook_task": "trk"}},
        )
        client.fetch_output.return_value = "not-a-json{"
        with pytest.raises(DispatchMarkerEnvelopeMissing) as exc:
            _call(client)
        # Payload absent because we couldn't parse the notebook.
        assert exc.value.executed_notebook is None


# ---------------------------------------------------------------------------
# stdout excerpt budget — truncation behaviour
# ---------------------------------------------------------------------------


class TestStdoutExcerptBudget:
    def test_long_stdout_is_tail_truncated(self) -> None:
        # Generate >4 KiB of output without any marker. The excerpt
        # should keep the tail.
        long_text = "X" * 10_000 + "TAIL_MARKER"
        nb = _executed_notebook({"outputs": [{"text": long_text}]})
        client = _client(executed_notebook=nb)
        with pytest.raises(DispatchMarkerEnvelopeMissing) as exc:
            _call(client)
        assert "TAIL_MARKER" in exc.value.stdout_excerpt
        assert exc.value.stdout_excerpt.startswith("...[truncated]...")
