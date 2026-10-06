"""Dispatch package for the laptop-CLI to AIDP REST round-trip.

Owns the REST path used by ``aidp-fusion-autopilot run`` when the command is not
executing inline inside an AIDP notebook. It validates local configuration,
builds the wheel, generates the cluster notebook, submits it through AIDP REST,
polls the run, and parses the notebook marker back into a ``RunSummary``.

The package is a strict client of the ``schema/`` layer
(:mod:`oracle_ai_data_platform_fusion_autopilot.schema.bundle`,
:mod:`oracle_ai_data_platform_fusion_autopilot.schema.errors`,
:mod:`oracle_ai_data_platform_fusion_autopilot.schema.run_summary`). It MUST
NOT import from :mod:`oracle_ai_data_platform_fusion_autopilot.orchestrator`
or any submodule under ``orchestrator/`` — that pulls extractors,
dimensions, transforms, and the registry into ``sys.modules`` and breaks
the schema/dispatch/orchestrator separation. The boundary is locked by
``tests/unit/dispatch/test_imports.py``.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import oci

# Diagnose-on-timeout enrichment budget. Total wall time the dispatcher spends
# fetching the partial executed notebook and walking cell outputs before
# re-raising DispatchPollTimeoutError. The operator already waited
# --poll-timeout seconds; another 10s for diagnostic enrichment is acceptable.
# Each diagnostic HTTP call passes ``timeout=_remaining()`` so requests bounds
# the per-call latency; ``time.monotonic()`` alone cannot cancel a blocking
# HTTP call.
_DIAG_BUDGET_S = 10
_RUNTIME_ENV_PASSTHROUGH_KEYS = (
    "FUSION_BICC_BASE_URL",
    "FUSION_BICC_USER",
    "FUSION_BICC_EXTERNAL_STORAGE",
    "OAC_URL",
)

from ..schema.bundle import AidpConfig, EnvSpec
from ..schema.diagnostic_artifact import (
    BronzeSourceColumnMissingV1,
    BronzeTypeMismatchV1,
    DiagnosticArtifactAlreadyExistsError,
    SchemaDriftDiagnosticV1,
    write_bronze_source_column_missing_diagnostic,
    write_bronze_type_mismatch_diagnostic,
    write_schema_drift_diagnostic,
)
from ..schema.errors import SchemaDriftDetectedError
from ..schema.path_segment import UnsafePathSegmentError, validate_path_segment
from ..schema.run_summary import RunSummary
from .errors import (
    DispatchAuthError,
    DispatchError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerDegradedError,
    DispatchMarkerMissingError,
    DispatchPollTimeoutError,
    DispatchPreflightError,
    DispatchRunFailedError,
    DispatchUploadError,
)
from .notebook_builder import MARKER_BEGIN, MARKER_END, build_notebook
from .preflight import (
    PreflightResult,
    any_failed,
    render as render_preflight,
    run_local_preflight,
    run_remote_preflight,
)
from .rest_client import AidpRestClient, AidpRestError
from .wheel_builder import DispatchWheelBuildError, build_wheel


def _format_preflight_failure(results: list[PreflightResult]) -> str:
    """One-line summary per failed check + remediation, suitable for the
    ``DispatchPreflightError`` message body."""
    lines: list[str] = []
    for r in results:
        if r.status == "FAIL":
            lines.append(f"{r.name}: {r.detail}")
            if r.remediation:
                lines.append(f"  → {r.remediation}")
    return "; ".join(lines) if not any(
        "\n" in line for line in lines
    ) else "\n".join(lines)


def _collect_runtime_env_passthrough() -> dict[str, str]:
    """Return non-secret env vars the cluster needs to load bundle.yaml."""
    return {
        key: value
        for key in _RUNTIME_ENV_PASSTHROUGH_KEYS
        if (value := os.environ.get(key))
    }


def dispatch_via_rest(
    *,
    bundle_path: Path,
    config: AidpConfig,
    env: EnvSpec,
    env_name: str,
    mode: Literal["seed", "incremental"],
    datasets: list[str] | None,
    layers: list[str] | None,
    dry_run: bool = False,
    plugin_checkout: Path | None = None,
    auto_start_cluster: bool = True,
    poll_timeout_s: int = 3600,
    log: Callable[[str], None] = lambda msg: None,
    # Content-pack primitives only; dispatch never imports orchestrator code.
    execution_backend: str = "legacy-python",
    profile_yaml: str | None = None,
    pack_files: "Mapping[str, str] | None" = None,
    pack_manifest: "dict[str, Any] | None" = None,
    # Passthrough only; dispatch never inspects it. Threaded into the generated
    # notebook's orchestrator.run(...) call so the cluster-side gate honours the
    # operator's break-glass intent.
    force_fingerprint_skip: bool = False,
    # Passthrough only; dispatch never inspects it. Threaded into the generated
    # notebook's orchestrator.run(...) call so the cluster-side AIDPF-4040 gate
    # honours --repin-plan-hash.
    repin_plan_hash: bool = False,
    # Passthrough only. When provided, the cluster-side bootstrap cell
    # materializes the snapshot to the resolved
    # profiles/<tenant>.schema-snapshot.yaml path so preflight can populate
    # `datasetDeltas` on drift.
    schema_snapshot_yaml: str | None = None,
    # REST-dispatch resume. When provided, the generated notebook cell passes
    # ``resume_run_id=<id>`` to the cluster-side ``orchestrator.run(...)`` call
    # so the resumed run adopts the supplied id and joins state rows with the
    # prior failed run.
    resume_run_id: str | None = None,
    # Caller loads and passes the resolved pack for dispatch dry-run so
    # schema.plan_resolver can walk it without crossing the dispatch import
    # boundary into orchestrator/*. Required when ``dry_run=True``; ignored
    # otherwise.
    resolved_pack: "Any | None" = None,
    # ``--strict-scope`` opt-out of implicit transitive include. Threaded into
    # both the dispatch dry-run resolver and the cluster-side orchestrator.run
    # call. Default False matches the CLI default.
    strict_scope: bool | None = None,
) -> RunSummary:
    """Dispatch the orchestrator notebook to AIDP and return the parsed RunSummary.

    Composes the dispatch package's primitives:

    1. Local preflight (bundle.yaml, dispatch coords, OCI profile + session).
    2. Build the :class:`AidpRestClient` once local preflight is all-PASS.
    3. Remote preflight (control plane, credential store, cluster state).
    4. **If** ``dry_run`` — return :meth:`RunSummary.empty` and stop.
    5. Build the wheel (content-hash cached).
    6. Generate the 4-cell notebook in-memory.
    7. Upload notebook → create job → submit run → poll → fetch output.
    8. Parse the ``AIDP_LIVE_TEST_RESULT_BEGIN/END`` marker into a RunSummary.

    Raises (all :class:`DispatchError` subclasses — :class:`AidpRestError`
    is wrapped at every call site so the CLI's ``except (DispatchError,
    OrchestratorConfigError)`` catch is exhaustive):
        :class:`DispatchPreflightError`: any local- or remote-phase check fails.
        :class:`DispatchAuthError`: OCI signer construction failed at client init.
        :class:`DispatchWheelBuildError`: ``python -m build`` failed.
        :class:`DispatchUploadError`: contents-API PUT non-2xx.
        :class:`DispatchJobSubmitError`: ``POST /jobs`` or ``POST /jobRuns`` non-2xx.
        :class:`DispatchPollTimeoutError`: ``poll_run`` deadline exceeded.
        :class:`DispatchRunFailedError`: terminal status FAILED/CANCELED/TIMED_OUT.
        :class:`DispatchFetchOutputError`: ``fetchOutput`` non-200.
        :class:`DispatchMarkerMissingError`: SUCCESS but no marker.
        :class:`SchemaDriftDetectedError`: the cluster-side run cell caught a
            drift, emitted a discriminated marker
            (``_kind == "schema_drift"``), and re-raised; this function
            translates the marker back into a SchemaDriftDetectedError
            after writing the artifact locally so the CLI can return
            exit 14 (NOT exit 2 via DispatchRunFailedError).
        :class:`DispatchMarkerDegradedError`: marker delimiters found
            but body unparseable; run_id recovered via regex fallback
            so the operator can resume.

    ``resume_run_id`` is supported on the REST dispatch path. The notebook
    cell threads it into ``orchestrator.run(..., resume_run_id=<id>)`` so the
    cluster-side run adopts the supplied id and writes state rows under the
    same identifier as the prior failed run. Bad run_ids surface as cell-3
    errors enriched into ``DispatchRunFailedError``'s message.
    """
    # ---- Local preflight --------------------------------------------------
    local_results = run_local_preflight(
        bundle_path=bundle_path,
        config=config,
        env_name=env_name,
        env=env,
    )
    log(render_preflight(local_results))
    if any_failed(local_results):
        raise DispatchPreflightError(_format_preflight_failure(local_results))

    # ---- Construct the REST client (cannot fail "out of band" of preflight
    # because local preflight validated the OCI profile already; defense in depth
    # catches malformed key files that slipped past from_file()).
    try:
        client = AidpRestClient(
            region=env.region or config.defaults.region,
            aidp_id=env.ai_data_platform_id or "",
            workspace_key=env.workspace_key,
            oci_profile=env.oci_profile or "DEFAULT",
            log=lambda stage, **kw: log(
                f"[rest] {stage} " + " ".join(f"{k}={v}" for k, v in kw.items())
            ),
        )
    except (
        oci.exceptions.ConfigFileNotFound,
        oci.exceptions.InvalidConfig,
        oci.exceptions.MissingPrivateKeyPassphrase,
    ) as exc:
        raise DispatchAuthError(f"OCI signer construction failed: {exc}") from exc
    except AidpRestError as exc:
        # _build_signer raises AidpRestError on missing/empty session-token
        # file — wrap into the AUTH code so the operator sees the correct
        # remediation hint.
        raise DispatchAuthError(str(exc)) from exc

    # ---- Remote preflight -------------------------------------------------
    remote_results = run_remote_preflight(
        client=client,
        env=env,
        auto_start_cluster=auto_start_cluster,
        log=log,
    )
    log(render_preflight(remote_results))
    if any_failed(remote_results):
        raise DispatchPreflightError(_format_preflight_failure(remote_results))

    # ---- Dry-run short-circuit -------------------------------------------
    # Resolve the plan laptop-side from neutral schema metadata so the renderer
    # can show "Would dispatch" + "Extra-plan prerequisites" Rich tables. The
    # bundle YAML was already validated by local preflight; load_bundle here is
    # a sub-millisecond re-parse with no Spark involvement.
    if dry_run:
        from ..schema.bundle import load_bundle
        from ..schema.plan_resolver import resolve_dry_run_plan

        if resolved_pack is None:
            raise ValueError(
                "dispatch_via_rest(dry_run=True) requires resolved_pack; "
                "the caller (commands/run.py) must load the pack via "
                "load_full_chain(...) and pass it in. This preserves "
                "the dispatch import boundary."
            )
        bundle, paths = load_bundle(bundle_path)
        plan_nodes, prereq_nodes = resolve_dry_run_plan(
            resolved_pack, bundle, paths,
            datasets=datasets, layers=layers,
            strict_scope=strict_scope,
        )
        log("dry-run requested — skipping wheel build + upload + dispatch")
        # Dry-run mode parity: a FRESH run with ``--mode`` omitted (mode=None
        # after the tri-state CLI change) resolves to "seed" in
        # ``orchestrator.run`` cluster-side. Mirror that here so the REST
        # dry-run header shows the same mode the dispatched notebook would
        # actually execute — not a bare ``mode=None``. Inlined (not
        # ``orchestrator.run_manifest.resolve_run_mode``) to keep dispatch
        # free of orchestrator imports. Dry-run never resumes (dispatch
        # short-circuits above), so the fresh-run default is the only case.
        dry_run_mode = mode or "seed"
        return RunSummary.empty(
            bundle_project=config.project, mode=dry_run_mode,
            plan=plan_nodes, prereqs=prereq_nodes,
        )

    # ---- Build wheel -----------------------------------------------------
    checkout = plugin_checkout or _detect_plugin_checkout()
    wheel_path = build_wheel(plugin_checkout=checkout, log=log)

    # ---- Generate notebook + upload --------------------------------------
    bundle_yaml = bundle_path.read_text(encoding="utf-8")
    notebook = build_notebook(
        wheel_path=wheel_path,
        bundle_yaml=bundle_yaml,
        mode=mode,
        datasets=datasets,
        layers=layers,
        resume_run_id=resume_run_id,
        bicc_secret_name=env.bicc_secret_name,
        bicc_secret_key=env.bicc_secret_key,
        # Content-pack primitives; passthrough only, dispatch never inspects them.
        execution_backend=execution_backend,
        profile_yaml=profile_yaml,
        pack_files=pack_files,
        pack_manifest=pack_manifest,
        force_fingerprint_skip=force_fingerprint_skip,
        repin_plan_hash=repin_plan_hash,
        schema_snapshot_yaml=schema_snapshot_yaml,
        env_vars=_collect_runtime_env_passthrough(),
        # Emit ``strict_scope=...`` in the generated orchestrator.run() call so
        # the cluster honors the operator's opt-out of implicit transitive
        # include.
        strict_scope=strict_scope,
    )

    workspace_root = config.defaults.workspace_root.strip("/")
    notebook_path = f"/Workspace/{workspace_root}/aidp-fusion-autopilot-{config.project}/run.ipynb"
    try:
        client.upload_notebook(notebook_path, notebook)
    except AidpRestError as exc:
        raise DispatchUploadError(
            f"notebook upload failed: {str(exc).splitlines()[0][:200]}"
        ) from exc
    log(f"notebook uploaded to {notebook_path}")

    # ---- Create job + submit run ----------------------------------------
    # AIDP job-name rule (empirical): letters, underscores, slashes only.
    # No hyphens, no dots. Sanitize the project + env tokens; suffix with
    # epoch seconds so resubmits don't collide.
    _safe_proj = "".join(c if c.isalnum() or c == "_" else "_" for c in config.project)
    _safe_env = "".join(c if c.isalnum() or c == "_" else "_" for c in env_name)
    job_name = f"aidp_fusion_autopilot_{_safe_proj}_{_safe_env}_{int(time.time())}"
    task_key = "orchestrator_run"
    try:
        job_key = client.create_notebook_job(
            name=job_name,
            description=f"aidp-fusion-autopilot run (env={env_name}, mode={mode})",
            notebook_path=notebook_path,
            cluster_key=env.cluster_key or "",
            cluster_name=env.cluster_name or "",
            task_key=task_key,
        )
        log(f"jobKey={job_key}")
        job_run_key = client.submit_run(job_key)
        log(f"jobRunKey={job_run_key}")
    except AidpRestError as exc:
        raise DispatchJobSubmitError(
            f"job submission failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # ---- Poll to terminal status -----------------------------------------
    try:
        result = client.poll_run(
            job_run_key,
            timeout_s=poll_timeout_s,
            on_status_change=lambda status: log(f"status={status}"),
        )
    except AidpRestError as exc:
        msg = str(exc)
        if "deadline exceeded" in msg:
            # Opportunistically enrich the timeout with the cluster-side
            # partial-progress snapshot so operators don't have to drop into
            # `oci raw-request` to see where the job is stuck.
            enriched = _diagnose_partial_progress(
                client, job_run_key, task_key, log
            )
            full_msg = (
                f"{msg}\nPartial progress at timeout:\n{enriched}"
                if enriched
                else msg
            )
            raise DispatchPollTimeoutError(full_msg) from exc
        # Some other transport failure during polling — treat as fetch-level
        # since we can't tell whether the cluster work completed.
        raise DispatchFetchOutputError(
            f"poll_run transport failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # ---- Fetch executed notebook + parse marker --------------------------
    try:
        task_run_key = AidpRestClient.resolve_task_run_key(result.raw, task_key)
        executed_notebook_json = client.fetch_output(task_run_key)
    except AidpRestError as exc:
        raise DispatchFetchOutputError(
            f"fetchOutput failed: {str(exc).splitlines()[0][:200]}"
        ) from exc

    # Decode + marker-parse defense (reviewer-driven): a truncated AIDP
    # output or a partial marker (BEGIN without END) would otherwise raise
    # raw json.JSONDecodeError / ValueError out of parse_marker — the CLI's
    # `except (DispatchError, OrchestratorConfigError)` clause wouldn't
    # catch those, so the operator would see a Python traceback instead of
    # exit 2 with DISPATCH_MARKER_MISSING. Wrap both the JSON decode AND
    # the marker walk so every malformed-output failure mode lands in the
    # typed taxonomy with jobRunKey context.
    try:
        executed_notebook = (
            json.loads(executed_notebook_json) if executed_notebook_json else {}
        )
    except json.JSONDecodeError as exc:
        raise DispatchMarkerMissingError(
            f"executed notebook JSON decode failed (jobRunKey={job_run_key}); "
            f"evidence-capture failure — underlying: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        ) from exc

    # Parse marker FIRST so a drift marker emitted by the run cell before
    # re-raising SchemaDriftDetectedError takes precedence over
    # DispatchRunFailedError. Status check moves below.
    try:
        marker = AidpRestClient.parse_marker(
            executed_notebook, begin=MARKER_BEGIN, end=MARKER_END,
            decode_base64=True,
        )
    except (ValueError, json.JSONDecodeError) as exc:
        # ValueError covers `value.index(end, b)` failure (BEGIN found but
        # no END — truncated stdout); JSONDecodeError covers the inner
        # `json.loads(value[b:e])` blowing up on a malformed payload that
        # happens to sit between valid BEGIN/END delimiters.
        raise DispatchMarkerMissingError(
            f"marker parse failed (jobRunKey={job_run_key}); "
            f"evidence-capture failure — underlying: "
            f"{type(exc).__name__}: {str(exc)[:200]}"
        ) from exc

    # Drift marker takes precedence over status. The notebook caught
    # SchemaDriftDetectedError, emitted this marker carrying the artifact JSON,
    # then re-raised, so the cluster-side cell errored and result.status is
    # FAILED. We translate the marker back into a SchemaDriftDetectedError on
    # the laptop, reconstructing the diagnostic file locally so the operator can
    # run `bootstrap --refresh` against it.
    #
    # Validate the marker before honouring it: a truncated / malformed
    # drift marker (missing run_id, summary, fingerprints, or
    # artifact_json that isn't a parseable AIDPF-2012 payload) must NOT
    # silently write an unusable artifact + exit 14 — that would put
    # the operator into the drift recovery flow with no actionable
    # diagnostic. Reject as DispatchMarkerMissingError so the standard
    # evidence-capture failure path runs.
    if isinstance(marker, dict) and marker.get("_kind") == "schema_drift":
        required_fields = (
            "run_id",
            "summary",
            "prior_fingerprint",
            "current_fingerprint",
            "artifact_json",
        )
        missing = [f for f in required_fields if not marker.get(f)]
        if missing:
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"missing required field(s): {', '.join(missing)}. "
                f"Cannot reconstruct local AIDPF-2012 artifact — "
                f"evidence-capture failure."
            )
        artifact_text = marker["artifact_json"]
        if not isinstance(artifact_text, str):
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"artifact_json is not a string "
                f"(got {type(artifact_text).__name__})."
            )
        try:
            artifact_obj = json.loads(artifact_text)
        except json.JSONDecodeError as exc:
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"artifact_json is not valid JSON: "
                f"{type(exc).__name__}: {str(exc)[:200]}"
            ) from exc
        if (
            not isinstance(artifact_obj, dict)
            or artifact_obj.get("errorCode") != "AIDPF-2012"
        ):
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"artifact_json payload is not an AIDPF-2012 diagnostic "
                f"(errorCode={artifact_obj.get('errorCode') if isinstance(artifact_obj, dict) else type(artifact_obj).__name__})."
            )
        # Treat marker payloads as untrusted: validate run_id is a safe
        # path segment + cross-check the inner artifact's runId matches
        # before letting it drive a filesystem write. A malicious or
        # corrupted marker with ``run_id="../outside"`` would otherwise
        # write outside the workdir's .aidp/diagnostics tree.
        drift_run_id = str(marker["run_id"])
        try:
            validate_path_segment(drift_run_id, field="marker.run_id")
        except UnsafePathSegmentError as exc:
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"{exc}"
            ) from exc
        if str(artifact_obj.get("runId", "")) != drift_run_id:
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"artifact_json.runId={artifact_obj.get('runId')!r} does "
                f"not match marker.run_id={drift_run_id!r}."
            )
        # Reconstruct via the Pydantic model + canonical writer so the
        # laptop-side artifact uses the same path-segment validation,
        # within-root assertion, and atomic-no-overwrite semantics as
        # the cluster-side write.
        try:
            artifact_model = SchemaDriftDiagnosticV1.model_validate(artifact_obj)
        except Exception as exc:  # ValidationError, ValueError, etc.
            raise DispatchMarkerMissingError(
                f"drift marker malformed (jobRunKey={job_run_key}); "
                f"artifact_json failed Pydantic validation: "
                f"{type(exc).__name__}: {str(exc)[:200]}"
            ) from exc
        try:
            diagnostic_path = write_schema_drift_diagnostic(
                bundle_path.resolve().parent,
                drift_run_id,
                artifact_model,
            )
        except DiagnosticArtifactAlreadyExistsError as exc:
            raise DispatchMarkerMissingError(
                f"drift marker reconstruction refused: a local "
                f"AIDPF-2012 artifact already exists for "
                f"run_id={drift_run_id!r} "
                f"(jobRunKey={job_run_key}). Delete the prior file or "
                f"re-run with a fresh run_id to overwrite."
            ) from exc
        except UnsafePathSegmentError as exc:
            raise DispatchMarkerMissingError(
                f"drift marker reconstruction refused (jobRunKey="
                f"{job_run_key}); {exc}"
            ) from exc
        raise SchemaDriftDetectedError(
            run_id=drift_run_id,
            diagnostic_path=diagnostic_path,
            summary=str(marker["summary"]),
            prior_fingerprint=str(marker["prior_fingerprint"]),
            current_fingerprint=str(marker["current_fingerprint"]),
        )

    if result.status != "SUCCESS":
        # Best-effort cell-error enrichment. The generated run cell does not
        # wrap orchestrator.run(...) in try/except, so a bad --resume <id>
        # (ResumeRunNotFoundError /
        # ResumeRunNotResumableError / ResumeBundleMismatchError) fails
        # cell 3 before marker emit. Walk the executed notebook for
        # cell-3 errors and append the typed ename/evalue so the operator
        # sees the orchestrator exception class without opening the
        # notebook. Diagnostic must not mask the original failure, so any
        # exception in the enricher is swallowed.
        detail = ""
        try:
            cell_errors = AidpRestClient.extract_cell_errors(executed_notebook)
            # The legacy layout put the run cell at index 3; content-pack runs
            # insert a bootstrap cell before the run cell, so fall back to the
            # last captured cell error when index 3 is not the failing cell.
            run_cell_err = next(
                (e for e in cell_errors if e.get("cell_index") == 3),
                None,
            ) or (cell_errors[-1] if cell_errors else None)
            if run_cell_err is not None:
                cell_index = run_cell_err.get("cell_index", "?")
                ename = run_cell_err.get("ename") or "UnknownError"
                evalue = (run_cell_err.get("evalue") or "")[:200]
                detail = f"; cell {cell_index} error: {ename}: {evalue}"
        except Exception:  # noqa: BLE001 - diagnostic is best-effort
            detail = ""
        raise DispatchRunFailedError(
            f"job_run_key={job_run_key} reached terminal status "
            f"{result.status!r}; see AIDP console / executed notebook "
            f"for details{detail}"
        )

    if marker is None:
        raise DispatchMarkerMissingError(
            f"job reported SUCCESS but no marker found in executed notebook "
            f"(jobRunKey={job_run_key}); evidence-capture failure"
        )

    # parse_marker's regex fallback fired: JSON body was unparseable but a
    # run_id was recovered. Surface as a typed exception carrying the resume
    # handle in the message so the operator can pass --resume <id> back to the
    # same CLI without grepping the executed notebook.
    if marker.get("_marker_parse_failed"):
        recovered_run_id = marker["run_id"]
        raise DispatchMarkerDegradedError(
            f"marker JSON parse failed (jobRunKey={job_run_key}); "
            f"cluster job reached terminal status SUCCESS but the "
            f"summary marker is unparseable. Recovered "
            f"run_id={recovered_run_id} from regex fallback — re-run "
            f"with --resume {recovered_run_id} to continue.",
            recovered_run_id=recovered_run_id,
        )

    try:
        summary = RunSummary.from_marker_dict(marker)
    except ValueError as exc:
        raise DispatchMarkerMissingError(
            f"marker payload malformed (jobRunKey={job_run_key}): {exc}"
        ) from exc

    # Persist any structured per-node diagnostics the run carried (e.g.
    # AIDPF-4071 bronze source-column-missing, AIDPF-4070 bronze type-mismatch)
    # under the laptop's .aidp/diagnostics/<run_id>/ so `/medallion-author` can
    # resolve them. Best-effort: a malformed/partial diagnostic must never mask
    # a successful run's summary.
    _workdir = bundle_path.resolve().parent
    for _diag in summary.diagnostics:
        try:
            _code = _diag.get("errorCode")
            if _code == "AIDPF-4071":
                _artifact = BronzeSourceColumnMissingV1.model_validate(_diag)
                _path = write_bronze_source_column_missing_diagnostic(
                    _workdir, summary.run_id, _artifact
                )
                log(f"wrote diagnostic {_path}")
            elif _code == "AIDPF-4070":
                _artifact = BronzeTypeMismatchV1.model_validate(_diag)
                _path = write_bronze_type_mismatch_diagnostic(
                    _workdir, summary.run_id, _artifact
                )
                log(f"wrote diagnostic {_path}")
        except Exception:  # noqa: BLE001 — diagnostic write is best-effort
            continue

    log(f"orchestrator run_id={summary.run_id}")
    return summary


def _detect_plugin_checkout() -> Path:
    """Walk up from this module's location to find the plugin checkout root
    (the directory containing ``pyproject.toml``).
    """
    current = Path(__file__).resolve().parent
    for _ in range(8):
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    raise DispatchError(
        "could not auto-detect plugin checkout root; pass plugin_checkout="
        " explicitly to dispatch_via_rest"
    )


def _diagnose_partial_progress(
    client: AidpRestClient,
    job_run_key: str,
    task_key: str,
    log: Callable[[str], None],
) -> str:
    """Fetch the partial executed notebook and return per-cell progress.

    Bounded by ``_DIAG_BUDGET_S`` total wall time.

    Best-effort: any exception in the diagnostic path is swallowed and
    we return ``""`` so the caller emits the original
    ``DispatchPollTimeoutError`` clean. The operator already waited
    ``--poll-timeout`` seconds; the diagnostic must NOT mask that signal.

    Each underlying HTTP call passes ``timeout=_remaining()`` so
    ``requests`` itself bounds the per-call latency — ``time.monotonic()``
    alone can't cancel a blocking HTTP call.
    """
    t0 = time.monotonic()

    def _remaining() -> int:
        # Never less than 1 — `requests` rejects `timeout=0` and
        # negative numbers raise immediately, which would crash the
        # diagnostic path instead of best-effort failing.
        return max(1, int(_DIAG_BUDGET_S - (time.monotonic() - t0)))

    try:
        run = client.get_run(job_run_key, timeout=_remaining())
        task_run_key = AidpRestClient.resolve_task_run_key(run, task_key)
        executed_notebook_json = client.fetch_output(
            task_run_key, timeout=_remaining()
        )
        if not executed_notebook_json:
            return ""
        executed_notebook = json.loads(executed_notebook_json)
        return _format_cell_progress(executed_notebook)
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment
        log(
            f"diagnostic enrichment failed (swallowed): "
            f"{type(exc).__name__}: {str(exc)[:120]}"
        )
        return ""


def _format_cell_progress(executed_notebook: dict[str, Any]) -> str:
    """Walk ``executed_notebook.cells[*]`` and return a per-cell summary
    line so the operator can see where the cluster job is stuck.

    Output shape (one line per code cell):
        cell 1: pip rc=0 plugin installed to /tmp/...
        cell 2: FUSION_BICC_PASSWORD loaded
        cell 3: <in flight or no output>
    """
    lines: list[str] = []
    for i, cell in enumerate(executed_notebook.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        outputs = cell.get("outputs", [])
        err_output = next(
            (o for o in outputs if o.get("output_type") == "error"),
            None,
        )
        if err_output is not None:
            lines.append(
                f"cell {i}: ERR {err_output.get('ename')}: "
                f"{str(err_output.get('evalue', ''))[:100]}"
            )
            continue
        text = ""
        for o in outputs:
            t = o.get("text", "")
            if t:
                text += t if isinstance(t, str) else "".join(t)
            else:
                d = o.get("data", {})
                tp = d.get("text/plain", "")
                text += tp if isinstance(tp, str) else "".join(tp)
        # Last non-empty line — that's the most recent print before things
        # stopped flowing. Truncate to keep the message body readable.
        last_line = next(
            (ln for ln in reversed(text.splitlines()) if ln.strip()),
            "<in flight or no output>",
        )
        lines.append(f"cell {i}: {last_line[:200]}")
    return "\n".join(lines)


__all__ = [
    "DispatchAuthError",
    "DispatchError",
    "DispatchFetchOutputError",
    "DispatchJobSubmitError",
    "DispatchMarkerDegradedError",
    "DispatchMarkerMissingError",
    "DispatchPollTimeoutError",
    "DispatchPreflightError",
    "DispatchRunFailedError",
    "DispatchUploadError",
    "DispatchWheelBuildError",
    "dispatch_via_rest",
]
