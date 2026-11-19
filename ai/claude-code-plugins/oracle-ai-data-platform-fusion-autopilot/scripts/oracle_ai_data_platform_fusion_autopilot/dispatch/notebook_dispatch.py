"""Neutral end-to-end notebook dispatch helper.

Wraps :class:`AidpRestClient`'s primitives into one orchestrator-free
end-to-end call: ``upload_notebook`` → ``create_notebook_job`` →
``submit_run`` → ``poll_run`` → ``resolve_task_run_key`` →
``fetch_output`` → ``parse_marker``. The bootstrap cluster-side dispatcher
(``commands/cluster_bootstrap_probe.py``) consumes this helper.

**Architectural boundary**: MUST NOT import from
``oracle_ai_data_platform_fusion_autopilot.orchestrator`` or any submodule
under ``orchestrator/``. The dispatched notebook itself may import
orchestrator code (it runs on the cluster); this laptop-side helper
just shuttles bytes. The boundary is locked by
``tests/unit/dispatch/test_imports.py``.

Schema-specific validation (e.g. ``ClusterProbeMarker`` for bootstrap,
``RunSummary.from_marker_dict`` for run) is the caller's job — this
helper returns the parsed marker as a plain ``dict`` (or raises a
typed exception).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from .errors import (
    DispatchError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerDecodeError,
    DispatchMarkerEnvelopeMissing,
    DispatchPollTimeoutError,
    DispatchRunFailedError,
    DispatchUploadError,
)
from .rest_client import AidpRestClient, AidpRestError


# Last N bytes of cluster stdout preserved on marker-parse failures.
# Sized so the AIDPF-2049 `cluster_stdout.log` companion file stays
# under a few KiB of the artifact JSON. Operators can pull the full
# stdout from the executed notebook itself if they need more.
_STDOUT_EXCERPT_BUDGET = 4096


def dispatch_notebook_and_fetch_marker(
    client: AidpRestClient,
    *,
    notebook: dict,
    workspace_path: str,
    cluster_key: str,
    cluster_name: str,
    job_name: str,
    task_key: str = "notebook_task",
    marker_begin: str,
    marker_end: str,
    marker_b64: bool = True,
    description: str | None = None,
    poll_timeout_s: int = 1800,
    poll_interval_s: int = 5,
    enrich_on_timeout: Callable[[AidpRestClient, str, str], str] | None = None,
    log: Callable[[str], None] = lambda _msg: None,
) -> dict[str, Any]:
    """Upload a notebook, run it on the named cluster, return the parsed marker.

    All AIDP REST contact happens inside this function. The caller
    supplies the notebook dict (which may embed orchestrator imports
    for cluster-side execution) and discriminates the parsed marker
    via its own schema model.

    Args:
        client: configured :class:`AidpRestClient`.
        notebook: nbformat-v4 notebook dict (caller-built).
        workspace_path: AIDP server-side path to upload the notebook
            to (e.g. ``/Workspace/Shared/fusion-autopilot-bootstrap/probe.ipynb``).
        cluster_key: AIDP compute UUID. Caller resolved this from the
            override chain (CLI flag → env var → ``EnvSpec``).
        cluster_name: AIDP compute display name (paired with
            ``cluster_key`` in the job-creation API).
        job_name: AIDP-side job-display name. Caller is responsible
            for sanitisation (letters / underscores / slashes only —
            no hyphens, no dots; see ``dispatch/__init__.py:253-258``).
        task_key: notebook-task identifier within the job. Defaults
            to ``"notebook_task"``; the run dispatcher uses
            ``"orchestrator_run"`` historically. Either value works —
            the helper doesn't inspect it beyond
            ``resolve_task_run_key`` lookup.
        marker_begin / marker_end: stdout delimiters the cluster cell
            uses to bracket the marker payload.
        marker_b64: ``True`` ⇒ the delimited payload is base64-encoded
            JSON (survives AIDP's Jupyter output corruption). ``False`` ⇒
            raw JSON between delimiters.
        description: optional job-description override; defaults to
            ``"aidp-fusion-autopilot dispatch ({job_name})"``.
        poll_timeout_s: laptop-side patience budget for ``poll_run``.
        poll_interval_s: forwarded into ``AidpRestClient.poll_run``;
            currently advisory (the client picks its own intervals).
        enrich_on_timeout: optional callable invoked on
            :class:`DispatchPollTimeoutError`. Signature
            ``(client, job_run_key, task_key) → str``. Returned text
            is appended to the exception message. Allows the run
            dispatcher to thread its ``_diagnose_partial_progress``
            closure through; bootstrap callers typically pass ``None``.
        log: optional log callback for status transitions.

    Returns:
        Parsed marker payload (``dict``). Schema-specific validation
        is the caller's responsibility.

    Raises:
        DispatchUploadError: notebook upload (``upload_notebook``) failed.
        DispatchJobSubmitError: job creation or run submission failed.
        DispatchPollTimeoutError: ``poll_run`` exceeded the deadline.
            Enriched with ``enrich_on_timeout`` output when supplied.
        DispatchRunFailedError: cluster reported a terminal FAILED /
            CANCELED / TIMED_OUT status.
        DispatchFetchOutputError: ``fetch_output`` HTTP error, or
            unexpected ``poll_run`` transport failure.
        DispatchMarkerEnvelopeMissing: notebook ran but emitted no
            ``MARKER_BEGIN/END`` envelope. Exception carries
            ``executed_notebook`` + ``stdout_excerpt`` for the
            caller's diagnostic file writer.
        DispatchMarkerDecodeError: envelope found but base64/JSON
            decoding failed. Same attributes as
            :class:`DispatchMarkerEnvelopeMissing`.
    """
    # ---- Upload notebook -------------------------------------------------
    try:
        client.upload_notebook(workspace_path, notebook)
    except AidpRestError as exc:
        raise DispatchUploadError(
            f"notebook upload failed (path={workspace_path}): "
            f"{str(exc).splitlines()[0][:200]}"
        ) from exc
    log(f"notebook uploaded to {workspace_path}")

    # ---- Create job + submit run ----------------------------------------
    try:
        job_key = client.create_notebook_job(
            name=job_name,
            description=description or f"aidp-fusion-autopilot dispatch ({job_name})",
            notebook_path=workspace_path,
            cluster_key=cluster_key,
            cluster_name=cluster_name,
            task_key=task_key,
        )
        log(f"jobKey={job_key}")
        job_run_key = client.submit_run(job_key)
        log(f"jobRunKey={job_run_key}")
    except AidpRestError as exc:
        raise DispatchJobSubmitError(
            f"job submission failed (job_name={job_name}): "
            f"{str(exc).splitlines()[0][:200]}"
        ) from exc

    # ---- Poll to terminal status ----------------------------------------
    try:
        result = client.poll_run(
            job_run_key,
            timeout_s=poll_timeout_s,
            on_status_change=lambda status: log(f"status={status}"),
        )
    except AidpRestError as exc:
        msg = str(exc)
        if "deadline exceeded" in msg:
            enriched = ""
            if enrich_on_timeout is not None:
                try:
                    enriched = enrich_on_timeout(client, job_run_key, task_key) or ""
                except Exception as enrich_exc:  # noqa: BLE001
                    log(f"enrich_on_timeout raised {type(enrich_exc).__name__}: {enrich_exc}")
            full_msg = (
                f"{msg}\nPartial progress at timeout:\n{enriched}"
                if enriched
                else msg
            )
            raise DispatchPollTimeoutError(full_msg) from exc
        raise DispatchFetchOutputError(
            f"poll_run transport failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # Terminal FAILED status — caller decides how to surface it. Marker
    # parsing still attempted below in case the cell emitted an error
    # envelope before raising; the bootstrap-mode caller prefers the
    # envelope's structured payload over the bare RUN_FAILED.
    run_failed = (result.status or "").upper() in (
        "FAILED",
        "CANCELED",
        "TIMED_OUT",
    )

    # ---- Fetch executed notebook ----------------------------------------
    try:
        task_run_key = AidpRestClient.resolve_task_run_key(result.raw, task_key)
        executed_notebook_json = client.fetch_output(task_run_key)
    except AidpRestError as exc:
        raise DispatchFetchOutputError(
            f"fetchOutput failed (jobRunKey={job_run_key}): "
            f"{str(exc).splitlines()[0][:200]}"
        ) from exc

    try:
        executed_notebook = (
            json.loads(executed_notebook_json) if executed_notebook_json else {}
        )
    except json.JSONDecodeError as exc:
        # Notebook JSON itself is malformed — there's nothing to extract
        # stdout from. Surface as envelope-missing so callers route to
        # the AIDPF-2049 path (the more useful diagnostic), but with
        # no executed_notebook payload.
        raise DispatchMarkerEnvelopeMissing(
            f"executed notebook JSON decode failed (jobRunKey={job_run_key}): "
            f"{type(exc).__name__}: {str(exc)[:200]}",
            executed_notebook=None,
            stdout_excerpt="",
        ) from exc

    # ---- Parse marker ----------------------------------------------------
    stdout_excerpt = _collect_stdout_excerpt(executed_notebook)
    try:
        marker = AidpRestClient.parse_marker(
            executed_notebook,
            begin=marker_begin,
            end=marker_end,
            decode_base64=marker_b64,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        # ValueError covers `value.index(end, b)` failure (truncated stdout
        # — BEGIN found but no END), base64 decode errors, and similar.
        # JSONDecodeError covers a malformed payload between valid
        # delimiters.
        raise DispatchMarkerDecodeError(
            f"marker decode failed (jobRunKey={job_run_key}): "
            f"{type(exc).__name__}: {str(exc)[:200]}",
            executed_notebook=executed_notebook,
            stdout_excerpt=stdout_excerpt,
        ) from exc

    if marker is None:
        # No envelope present anywhere in the executed notebook. If the
        # run also failed, raise RUN_FAILED — the cluster cell crashed
        # before emitting its envelope (this is the only path where
        # DispatchRunFailedError fires; a successful run that simply
        # lost its envelope is the EnvelopeMissing branch).
        if run_failed:
            raise DispatchRunFailedError(
                f"cluster run reached terminal {result.status!r} without "
                f"emitting the {marker_begin!r}/{marker_end!r} envelope "
                f"(jobRunKey={job_run_key})."
            )
        raise DispatchMarkerEnvelopeMissing(
            f"no {marker_begin!r}/{marker_end!r} envelope found in executed "
            f"notebook (jobRunKey={job_run_key}); notebook cells: "
            f"{len(executed_notebook.get('cells', []))}.",
            executed_notebook=executed_notebook,
            stdout_excerpt=stdout_excerpt,
        )

    return marker


def _collect_stdout_excerpt(executed_notebook: dict[str, Any]) -> str:
    """Walk the executed notebook's cell outputs and return the last
    ~4 KiB of stdout text. Used to populate the AIDPF-2049
    ``cluster_stdout.log`` companion file when marker parsing fails.

    Concatenates ``output.text`` / ``output.data['text/plain']``
    across all cells in order. Truncates from the END (operators
    care about the most-recent output, which is where the failure
    blew up).
    """
    chunks: list[str] = []
    for cell in executed_notebook.get("cells", []):
        for output in cell.get("outputs", []):
            for src in ("text", "data"):
                value = output.get(src)
                if value is None:
                    continue
                if src == "data":
                    value = value.get("text/plain", "")
                if isinstance(value, list):
                    value = "".join(value)
                if value:
                    chunks.append(value)
    full = "".join(chunks)
    if len(full) <= _STDOUT_EXCERPT_BUDGET:
        return full
    # Keep the tail — the failure-relevant bytes.
    return "...[truncated]...\n" + full[-_STDOUT_EXCERPT_BUDGET:]


__all__ = ["dispatch_notebook_and_fetch_marker"]
