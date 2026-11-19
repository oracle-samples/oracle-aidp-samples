"""Bootstrap variation-phase probe dispatcher.

`aidp-fusion-autopilot bootstrap --dispatch-mode=cluster` (the default in
cluster mode) routes the bronze-schema probe + walker through this module
instead of the laptop's in-process Spark session. The cluster path:

1. Build the plugin wheel locally (cached).
2. Stage the merged content pack into base64+JSON primitives.
3. Compose a 3-cell notebook (install / stage / probe-with-in-cell-try-except).
4. Dispatch via the neutral
   :func:`dispatch.notebook_dispatch.dispatch_notebook_and_fetch_marker`
   helper.
5. Validate the returned envelope and convert to a
   :class:`ClusterProbeMarker` the laptop-side variation phase consumes.

**Architectural boundary**: this module is the ONLY place that imports
both ``dispatch/`` and ``orchestrator/`` (via the pack-staging helper).
The ``dispatch/`` package itself stays orchestrator-free — see
``dispatch/__init__.py:8-16`` + ``tests/unit/dispatch/test_imports.py``.

Step 8 wires the failure-context dataclasses below to the AIDPF-2048 /
AIDPF-2049 diagnostic-artifact writers. Until then the
:class:`ClusterDispatchError` / :class:`ClusterMarkerError` exceptions
carry the same payload — Step 8's writers translate them to disk.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from ..dispatch import _collect_runtime_env_passthrough
from ..dispatch.errors import (
    DispatchError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerDecodeError,
    DispatchMarkerEnvelopeMissing,
    DispatchPollTimeoutError,
    DispatchRunFailedError,
    DispatchUploadError,
    DispatchWheelBuildError,
)
from ..dispatch.notebook_builder import (
    _build_creds_cell,
    _build_install_cell,
    _code_cell,
    _encode_payload_b64,
    _encode_text_b64,
    _markdown_cell,
)
from ..dispatch.notebook_dispatch import dispatch_notebook_and_fetch_marker
from ..dispatch.preflight import _check_bicc_credential
from ..dispatch.rest_client import AidpRestClient
from ..dispatch.wheel_builder import build_wheel
from ..orchestrator.content_pack_staging import stage_pack_files
from ..schema.cluster_probe_marker import (
    ClusterProbeEnvelope,
    ClusterProbeMarker,
)

if TYPE_CHECKING:  # pragma: no cover
    from rich.console import Console

    from ..orchestrator.content_pack import ResolvedPack
    from ..schema.bundle import Bundle, EnvSpec
    from .bootstrap import ResolvedClusterDispatchConfig


# ---------------------------------------------------------------------------
# Constants — marker envelope + cluster-side stage names
# ---------------------------------------------------------------------------


_BOOTSTRAP_MARKER_BEGIN = "AIDP_BOOTSTRAP_PROBE_MARKER_BEGIN"
"""stdout delimiter the cluster cell uses to mark the start of the
base64-wrapped envelope. Distinct from the run dispatcher's
``AIDP_LIVE_TEST_RESULT_BEGIN`` so an operator who accidentally points
the bootstrap helper at a run-dispatch notebook (or vice versa) gets
a clear ``DispatchMarkerEnvelopeMissing`` rather than silent data
crossover."""

_BOOTSTRAP_MARKER_END = "AIDP_BOOTSTRAP_PROBE_MARKER_END"


# AIDP job-name rule (empirical, mirrors dispatch/__init__.py:253-258):
# letters, underscores, slashes only. No hyphens, no dots. The helper
# enforces this for the bootstrap job-name we generate.
def _safe_job_token(s: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in s)


# ---------------------------------------------------------------------------
# Failure context — payloads Step 8's diagnostic writers consume
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ClusterDispatchFailureContext:
    """Payload for ``AIDPF-2048`` (cluster dispatch failed).

    Step 8 maps this 1:1 to the ``ClusterDispatchFailure`` Pydantic
    artifact model. Kept as a dataclass here so the exception path
    doesn't depend on the artifact-writer module — the writer pulls
    fields off the dataclass when it materialises the JSON file.
    """

    failed_step: str
    """One of ``build_wheel`` / ``stage_pack`` / ``upload_notebook`` /
    ``create_notebook_job`` / ``submit_run`` / ``poll_run`` /
    ``fetch_output`` (the dispatch helper's exception-class taxonomy
    mapped to a stable artifact-side enum)."""

    cause_type: str
    """``type(exc).__name__`` of the original exception."""

    cause_message: str
    """``str(exc)[:2000]`` — bounded to keep the artifact file small."""

    workspace_path: str | None = None
    cluster_key: str | None = None
    run_state: str | None = None
    poll_elapsed_seconds: float | None = None


@dataclass(frozen=True)
class ClusterMarkerFailureContext:
    """Payload for ``AIDPF-2049`` (cluster marker validation failed).

    Step 8 maps this 1:1 to the ``ClusterMarkerFailure`` Pydantic
    artifact model and writes the companion ``cluster_stdout.log``
    file from :attr:`stdout_full`.
    """

    kind: str
    """One of ``envelope_missing`` / ``cluster_reported_error`` /
    ``marker_version_unsupported`` / ``validation_failed``."""

    cluster_error_type: str | None = None
    cluster_error_message: str | None = None
    cluster_traceback: str | None = None
    validation_errors: list[str] = field(default_factory=list)
    stdout_excerpt: str = ""
    """Last ~4 KiB of stdout the cluster emitted — what goes in the
    artifact JSON body."""
    stdout_full: str = ""
    """Full stdout (untruncated) for the ``cluster_stdout.log``
    companion file Step 8 writes alongside the artifact."""


# ---------------------------------------------------------------------------
# Public exceptions
# ---------------------------------------------------------------------------


class ClusterDispatchError(Exception):
    """Cluster-mode probe dispatch failed before producing a valid marker.

    Step 8 wires the bootstrap CLI's exception handler to inspect
    :attr:`failure_context` and write the AIDPF-2048 diagnostic
    artifact."""

    def __init__(self, msg: str, *, failure_context: ClusterDispatchFailureContext) -> None:
        super().__init__(msg)
        self.failure_context = failure_context


class ClusterMarkerError(Exception):
    """Cluster reported a marker the laptop can't use.

    Step 8 wires the bootstrap CLI's exception handler to inspect
    :attr:`failure_context` + :attr:`executed_notebook` and write the
    AIDPF-2049 diagnostic artifact + ``cluster_stdout.log`` companion."""

    def __init__(
        self,
        msg: str,
        *,
        failure_context: ClusterMarkerFailureContext,
        executed_notebook: dict | None = None,
    ) -> None:
        super().__init__(msg)
        self.failure_context = failure_context
        self.executed_notebook = executed_notebook


# ---------------------------------------------------------------------------
# Notebook builder (Step 5)
# ---------------------------------------------------------------------------


def _build_bootstrap_staging_cell(
    *,
    bundle_yaml: str,
    pack_files: Mapping[str, str],
    pack_manifest: dict[str, Any],
) -> str:
    """Cluster-side cell #2: materialise bundle.yaml + stage pack tree.

    Stashes the staged pack root + the closure-bound base resolver as
    module-global names so the probe cell (#3) consumes them without
    re-running the staging logic. Embed everything as base64+JSON so
    the cluster doesn't need filesystem access to the laptop's pack
    directory.
    """
    bundle_b64 = _encode_text_b64(bundle_yaml)
    pack_files_b64 = _encode_payload_b64(dict(pack_files))
    pack_manifest_b64 = _encode_payload_b64(pack_manifest)
    return (
        f"import base64, json\n"
        f"from pathlib import Path\n"
        f"from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging "
        f"import materialize_staged_pack\n"
        f"# Decode laptop-side payloads.\n"
        f'_BUNDLE_YAML = base64.b64decode("""{bundle_b64}""").decode("utf-8")\n'
        f'_PACK_FILES = json.loads(base64.b64decode("""{pack_files_b64}""").decode("utf-8"))\n'
        f'_PACK_MANIFEST = json.loads(base64.b64decode("""{pack_manifest_b64}""").decode("utf-8"))\n'
        f"# Write bundle.yaml to the cluster-side working dir.\n"
        f'BUNDLE_PATH = Path("bundle.yaml")\n'
        f"BUNDLE_PATH.write_text(_BUNDLE_YAML)\n"
        f"# materialize_staged_pack creates its own tempdir internally;\n"
        f"# returns (top_overlay_root, base_resolver).\n"
        f"_TOP_OVERLAY_ROOT, _BASE_RESOLVER = materialize_staged_pack(\n"
        f"    _PACK_FILES, _PACK_MANIFEST\n"
        f")\n"
        f'print(f"bundle staged at {{BUNDLE_PATH.resolve()}}")\n'
        f'print(f"pack staged at {{_TOP_OVERLAY_ROOT}}")\n'
    )


def _build_coa_metadata_block() -> str:
    """The generated COA-metadata section of the probe cell (v2 only).

    Kept out of the v1 cell entirely so existing snapshot/substring
    assertions on the v1 cell stay byte-stable. Fail-soft by
    construction: a KFF fetch failure becomes coverage='skipped' +
    skipReason (AIDPF-2021 on the laptop); gl_coa unavailable (FR-14a
    S5) becomes probeNote with zero probes — never a probe abort.
    """
    return (
        f"    _coa_meta = None\n"
        f"    if True:\n"
        f"        from oracle_ai_data_platform_fusion_autopilot.commands.coa_metadata_resolution "
        f"import BICC_ROW_CANDIDATE, arms_probe_map, cluster_fetch_kff_rows, derive_arms\n"
        f"        from oracle_ai_data_platform_fusion_autopilot.orchestrator import coa_probe\n"
        f"        from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker "
        f"import CoaMetadataMarker, CoaCandidateArmMarker, CoaArmRejectMarker, CoaChartProbeMarker\n"
        f"        try:\n"
        f"            _kff_rows = cluster_fetch_kff_rows(\n"
        f"                spark, service_url=bundle.fusion.service_url,\n"
        f"                username=bundle.fusion.username,\n"
        f"                password=_resolved_password,\n"
        f"                external_storage=bundle.fusion.external_storage,\n"
        f"            )\n"
        f"            _arms, _rejects = derive_arms(_kff_rows, candidate=BICC_ROW_CANDIDATE)\n"
        f"            _probes, _active, _probe_note = {{}}, {{}}, None\n"
        f"            try:\n"
        f"                _gl_table = f'{{catalog}}.{{bronze_schema}}.gl_coa'\n"
        f"                _active = coa_probe.coa_chart_active(spark, _gl_table)\n"
        f"                _probes = coa_probe.probe_charts(\n"
        f"                    spark, _gl_table, arms_probe_map(_arms), _active,\n"
        f"                )\n"
        f"            except Exception as _pexc:  # gl_coa not landed (S5) / probe issue\n"
        f"                _probe_note = f'gl_coa probe unavailable: {{type(_pexc).__name__}}'\n"
        f"            _coa_meta = CoaMetadataMarker(\n"
        f"                source='bicc-pvo', coverage='complete',\n"
        f"                probeNote=_probe_note,\n"
        f"                candidates=[CoaCandidateArmMarker(\n"
        f"                    chartId=a.chart_id,\n"
        f"                    balancingSegment=a.balancing_segment,\n"
        f"                    costCenterSegment=a.cost_center_segment,\n"
        f"                    naturalAccountSegment=a.natural_account_segment,\n"
        f"                ) for a in _arms.values()],\n"
        f"                rejects=[CoaArmRejectMarker(\n"
        f"                    chart=r.chart_id, reason=r.reason, detail=r.detail,\n"
        f"                ) for r in _rejects],\n"
        f"                probes=[CoaChartProbeMarker(\n"
        f"                    chartId=pr.chart_id, activeRows=pr.active_row_count,\n"
        f"                    naDistinct=pr.natural_account_distinct,\n"
        f"                    naAmbiguous=pr.natural_account_ambiguous,\n"
        f"                ) for pr in _probes.values()],\n"
        f"                activeCharts=dict(_active),\n"
        f"            )\n"
        f"        except Exception as _cexc:  # noqa: BLE001 — fail-soft to a labelled skip\n"
        f"            _coa_meta = CoaMetadataMarker(\n"
        f"                source='bicc-pvo', coverage='skipped',\n"
        f"                skipReason=f'{{type(_cexc).__name__}}: {{str(_cexc)[:300]}}',\n"
        f"            )\n"
    )


def _build_probe_cell(*, tenant: str, resolve_coa_metadata: bool = False) -> str:
    """Cluster-side cell #3: probe → walk → emit base64-wrapped marker.

    The try/except is INSIDE this cell — Jupyter cell N exceptions are
    not catchable by cell N+1 (the failure-cell-wraps-step-2 design was
    rejected for that reason).
    """
    return (
        f"import base64, json, traceback\n"
        f"from datetime import datetime, timezone\n"
        f"from oracle_ai_data_platform_fusion_autopilot.schema.bundle import load_bundle\n"
        f"from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack "
        f"import load_full_chain\n"
        f"from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime "
        f"import CredentialResolutionError, _resolve_password\n"
        f"from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe "
        f"import resolve_observed\n"
        f"from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint "
        f"import compute_bronze_fingerprint\n"
        f"from oracle_ai_data_platform_fusion_autopilot.commands.variation_resolver "
        f"import walk_column_alias, walk_semantic_variant, AutoResolved, MultiMatch, NoMatch\n"
        f"from oracle_ai_data_platform_fusion_autopilot.schema.cluster_probe_marker "
        f"import ClusterProbeMarker, ClusterProbeEnvelope, WalkerOutcomeMarker, "
        f"CandidateAttemptMarker, ColumnInfoMarker\n"
        f"# Marker constants — must match the laptop-side helper's "
        f"marker_begin / marker_end kwargs.\n"
        f"MARKER_BEGIN = {_BOOTSTRAP_MARKER_BEGIN!r}\n"
        f"MARKER_END = {_BOOTSTRAP_MARKER_END!r}\n"
        f"try:\n"
        f"    # Render the bundle in-cell (load_bundle expands bare ${{VAR}}\n"
        f"    # fusion fields from the env the creds cell populated; it leaves\n"
        f"    # ${{env:}}/${{vault:}} refs for _resolve_password).\n"
        f"    bundle, _paths = load_bundle(BUNDLE_PATH)\n"
        f"    pack = load_full_chain(_TOP_OVERLAY_ROOT, base_resolver=_BASE_RESOLVER)\n"
        f"    catalog = bundle.aidp.catalog\n"
        f"    bronze_schema = bundle.aidp.bronze_schema\n"
        f"    # Resolve the BICC password from the env the creds cell loaded\n"
        f"    # (${{env:FUSION_BICC_PASSWORD}}) or a ${{vault:OCID}} ref. The\n"
        f"    # ${{aidp:secret:...}} form is not runtime-resolvable.\n"
        f"    _pw_ref = bundle.fusion.password\n"
        f'    if isinstance(_pw_ref, str) and _pw_ref.startswith("${{aidp:secret:"):\n'
        f"        raise CredentialResolutionError(\n"
        f'            "bundle.fusion.password is an ${{aidp:secret:...}} reference, "\n'
        f'            "which the runtime resolver does not support. Use "\n'
        f'            "${{env:FUSION_BICC_PASSWORD}} (populated by the creds cell "\n'
        f'            "from biccSecretName/biccSecretKey) or ${{vault:OCID}}."\n'
        f"        )\n"
        f"    _resolved_password = _resolve_password(_pw_ref).get_secret_value()\n"
        f"    # Per-node landed-vs-source selection; scope from pack.bronze,\n"
        f"    # physical node.target probed, results keyed by node id.\n"
        f"    observed = resolve_observed(\n"
        f"        spark, catalog=catalog, bronze_schema=bronze_schema,\n"
        f"        pack=pack, bundle=bundle, resolved_password=_resolved_password,\n"
        f"    )\n"
        f"    fingerprint = compute_bronze_fingerprint(observed=observed)\n"
        f"    walker_results = []\n"
        f"    for name, spec in pack.pack.column_aliases.items():\n"
        f"        if spec.resolution == 'semanticRole':\n"
        f"            continue  # COA semantic roles resolve via config ladder, not existence\n"
        f'        ds_id = spec.appliesTo.split(".", 1)[1] if "." in spec.appliesTo else spec.appliesTo\n'
        f"        cols = {{c.name for c in observed.get(ds_id, [])}}\n"
        f"        outcome = walk_column_alias(spec, cols)\n"
        f"        if isinstance(outcome, AutoResolved):\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='columnAliases', outcome='auto_resolved',\n"
        f"                chosen=outcome.chosen,\n"
        f"            ))\n"
        f"        elif isinstance(outcome, MultiMatch):\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='columnAliases', outcome='multi_match',\n"
        f"                matched=outcome.matched,\n"
        f"            ))\n"
        f"        else:  # NoMatch\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='columnAliases', outcome='no_match',\n"
        f"                candidates_tried=[\n"
        f"                    CandidateAttemptMarker(candidate=a.candidate, outcome=a.outcome, detail=a.detail)\n"
        f"                    for a in outcome.candidates_tried\n"
        f"                ],\n"
        f"            ))\n"
        f"    for name, spec in pack.pack.semantic_variants.items():\n"
        f'        ds_id = spec.appliesTo.split(".", 1)[1] if "." in spec.appliesTo else spec.appliesTo\n'
        f"        cols = {{c.name for c in observed.get(ds_id, [])}}\n"
        f"        outcome = walk_semantic_variant(spec, cols)\n"
        f"        if isinstance(outcome, AutoResolved):\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='semanticVariants', outcome='auto_resolved',\n"
        f"                chosen=outcome.chosen,\n"
        f"            ))\n"
        f"        elif isinstance(outcome, MultiMatch):\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='semanticVariants', outcome='multi_match',\n"
        f"                matched=outcome.matched,\n"
        f"            ))\n"
        f"        else:\n"
        f"            walker_results.append(WalkerOutcomeMarker(\n"
        f"                name=name, kind='semanticVariants', outcome='no_match',\n"
        f"                candidates_tried=[\n"
        f"                    CandidateAttemptMarker(candidate=a.candidate, outcome=a.outcome, detail=a.detail)\n"
        f"                    for a in outcome.candidates_tried\n"
        f"                ],\n"
        f"            ))\n"
        f"    observed_schema = {{\n"
        f"        ds: [ColumnInfoMarker.from_column_info(c) for c in cols]\n"
        f"        for ds, cols in observed.items()\n"
        f"    }}\n"
        # COA metadata resolution block — emitted ONLY when requested, so a
        # v1 probe cell stays byte-stable (feature coa-mapping-auto-
        # remediation; transport pivot: BICC PVOs read cluster-side,
        # fail-soft to coverage='skipped' → AIDPF-2021 on the laptop).
        + (_build_coa_metadata_block() if resolve_coa_metadata
           else f"    _coa_meta = None\n")
        +        f"    marker = ClusterProbeMarker(\n"
        f"        markerVersion=2 if _coa_meta is not None else 1,\n"
        f"        tenant={tenant!r},\n"
        f"        bronzeFingerprint=fingerprint,\n"
        f"        observedSchema=observed_schema,\n"
        f"        walkerResults=walker_results,\n"
        f"        dispatchedAt=datetime.now(timezone.utc),\n"
        f"        coaMetadata=_coa_meta,\n"
        f"    )\n"
        f"    envelope = ClusterProbeEnvelope(ok=True, marker=marker)\n"
        f"    payload = envelope.model_dump_json(by_alias=True)\n"
        f"except Exception as exc:  # noqa: BLE001\n"
        f"    envelope = ClusterProbeEnvelope(\n"
        f"        ok=False,\n"
        f"        errorType=type(exc).__name__,\n"
        f"        errorMessage=str(exc)[:4000],\n"
        f"        traceback=traceback.format_exc()[:8000],\n"
        f"    )\n"
        f"    payload = envelope.model_dump_json(by_alias=True)\n"
        f'_token = base64.b64encode(payload.encode("utf-8")).decode("ascii")\n'
        f"print(MARKER_BEGIN, _token, MARKER_END)\n"
    )


def _build_notebook(
    *,
    wheel_path: Path,
    bundle_yaml: str,
    pack_files: Mapping[str, str],
    pack_manifest: dict[str, Any],
    tenant: str,
    bicc_secret_name: str,
    bicc_secret_key: str,
    runtime_env_vars: Mapping[str, str] | None = None,
    resolve_coa_metadata: bool = False,
) -> dict:
    """Compose the 4-cell ipynb dict the cluster runs.

    Cells in order:

    1. **install** — base64-decoded wheel, ``pip install --target``,
       ``sys.path.insert``. Reuses :func:`dispatch.notebook_builder._build_install_cell`
       verbatim — same primitive the run dispatcher ships.
    2. **creds** — load ``FUSION_BICC_PASSWORD`` from the AIDP credential
       store (``aidputils.secrets.get(name=bicc_secret_name,
       key=bicc_secret_key)``) + the non-secret Fusion env passthrough.
       Reuses :func:`dispatch.notebook_builder._build_creds_cell` verbatim
       (same primitive the run dispatcher ships) so the source-schema probe
       has BICC credentials. The secret is fetched cluster-side and never
       serialized into the notebook payload.
    3. **stage** — write ``bundle.yaml`` to the cluster's working dir +
       materialise the pack tree. Stashes module globals the probe cell
       consumes.
    4. **probe** — single cell with in-cell ``try``/``except`` that
       probes, walks, and emits the ``ClusterProbeEnvelope`` payload
       wrapped in base64 between ``MARKER_BEGIN`` / ``MARKER_END``.
    """
    return {
        "cells": [
            _markdown_cell(
                "# Bootstrap variation probe\n"
                "Cluster-side bronze schema probe + walker dispatched "
                "from `aidp-fusion-autopilot bootstrap`."
            ),
            _code_cell(_build_install_cell(wheel_path)),
            _code_cell(
                _build_creds_cell(
                    bundle_yaml=bundle_yaml,
                    bicc_secret_name=bicc_secret_name,
                    bicc_secret_key=bicc_secret_key,
                    env_vars=runtime_env_vars,
                )
            ),
            _code_cell(
                _build_bootstrap_staging_cell(
                    bundle_yaml=bundle_yaml,
                    pack_files=pack_files,
                    pack_manifest=pack_manifest,
                )
            ),
            _code_cell(_build_probe_cell(
                tenant=tenant, resolve_coa_metadata=resolve_coa_metadata,
            )),
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------
# Public API — dispatch_cluster_probe (Step 4)
# ---------------------------------------------------------------------------


def dispatch_cluster_probe(
    *,
    env: "EnvSpec",
    bundle: "Bundle",
    bundle_path: Path,
    pack: "ResolvedPack",
    dispatch_config: "ResolvedClusterDispatchConfig",
    tenant: str,
    run_id: str | None = None,
    client_factory=None,
    console: "Console | None" = None,
    poll_timeout_s: int = 1800,
    resolve_coa_metadata: bool = False,
) -> ClusterProbeMarker:
    """Dispatch the variation-phase bronze probe to the AIDP cluster.

    Returns the validated :class:`ClusterProbeMarker` (the cluster
    cell's success payload). Raises :class:`ClusterDispatchError` on
    pre-marker dispatch failures (wheel build, upload, submit, poll,
    fetch) and :class:`ClusterMarkerError` on marker-validation
    failures (envelope absent, cluster reported error,
    ``markerVersion`` mismatch, etc.).

    Args:
        env: resolved EnvSpec carrying ``workspace_key`` /
            ``ai_data_platform_id`` / OCI profile / region.
        bundle: parsed Bundle — its YAML content gets re-serialised + embedded
            in the staging cell.
        bundle_path: filesystem path to the original ``bundle.yaml``.
            Used to locate the wheel-cache dir
            (``<bundle.yaml.parent>/.aidp/wheel-cache``).
        pack: merged ResolvedPack from
            :func:`orchestrator.content_pack.load_full_chain`.
        dispatch_config: post-override cluster-dispatch coords from
            :func:`commands.bootstrap._resolve_cluster_dispatch_config`.
            ``cluster_key`` / ``cluster_name`` / ``workspace_dir`` must
            be set (Step 9 fails-closed before this function is reached
            when they're missing).
        tenant: profile name — used as the marker's ``tenant`` field
            and in the generated workspace path / job name.
        run_id: optional bootstrap run id. Defaults to a fresh uuid
            timestamp so retries don't collide.
        client_factory: callable returning an :class:`AidpRestClient`
            (test seam). Production code passes ``None`` and the
            function constructs one from ``dispatch_config``.
        console: optional Rich console for status logging.
        poll_timeout_s: laptop-side patience budget for ``poll_run``.
    """
    if run_id is None:
        run_id = f"bootstrap-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    log = console.log if console is not None else (lambda _msg: None)

    # ---- Build wheel ----
    try:
        wheel_path = build_wheel(
            plugin_checkout=_find_plugin_checkout(bundle_path),
            cache_dir=bundle_path.parent / ".aidp" / "wheel-cache",
            log=lambda msg: log(f"[wheel] {msg}"),
        )
    except DispatchWheelBuildError as exc:
        raise ClusterDispatchError(
            f"wheel build failed: {exc}",
            failure_context=ClusterDispatchFailureContext(
                failed_step="build_wheel",
                cause_type=type(exc).__name__,
                cause_message=str(exc)[:2000],
                cluster_key=dispatch_config.cluster_key,
            ),
        ) from exc

    # ---- Stage pack ----
    try:
        pack_files, pack_manifest = stage_pack_files(pack)
    except Exception as exc:  # noqa: BLE001 — staging can raise from inside the orchestrator
        raise ClusterDispatchError(
            f"pack staging failed: {exc}",
            failure_context=ClusterDispatchFailureContext(
                failed_step="stage_pack",
                cause_type=type(exc).__name__,
                cause_message=str(exc)[:2000],
                cluster_key=dispatch_config.cluster_key,
            ),
        ) from exc

    # ---- Compose notebook ----
    bundle_yaml_text = bundle_path.read_text(encoding="utf-8")
    notebook = _build_notebook(
        wheel_path=wheel_path,
        bundle_yaml=bundle_yaml_text,
        pack_files=pack_files,
        pack_manifest=pack_manifest,
        tenant=tenant,
        bicc_secret_name=env.bicc_secret_name,
        bicc_secret_key=env.bicc_secret_key,
        runtime_env_vars=_collect_runtime_env_passthrough(),
        resolve_coa_metadata=resolve_coa_metadata,
    )

    # ---- Dispatch via the neutral helper ----
    workspace_dir = dispatch_config.workspace_dir.rstrip("/")
    workspace_path = f"{workspace_dir}/probe-{_safe_job_token(tenant)}-{int(time.time())}.ipynb"
    job_name = f"aidp_fusion_autopilot_bootstrap_{_safe_job_token(tenant)}_{int(time.time())}"

    client = (
        client_factory()
        if client_factory is not None
        else AidpRestClient(
            region=dispatch_config.region,
            aidp_id=dispatch_config.aidp_id or "",
            workspace_key=dispatch_config.workspace_key,
            oci_profile=dispatch_config.oci_profile,
            log=lambda stage, **kw: log(f"[rest] {stage} {kw}"),
        )
    )

    # ---- Credential preflight (Step 5a) ----
    # The creds cell fetches FUSION_BICC_PASSWORD from the AIDP credential
    # store; if that entry is missing the failure would otherwise surface
    # only mid-notebook (after wheel build, upload, submit, cluster ramp) as
    # a generic run failure with no executed-stdout detail. Fast-fail here
    # with the credential-specific message — mirrors the run dispatcher's
    # remote preflight (`_check_bicc_credential`).
    cred_check = _check_bicc_credential(
        client, env.bicc_secret_name, env.bicc_secret_key
    )
    if cred_check.status != "PASS":
        raise ClusterDispatchError(
            f"BICC credential preflight failed: {cred_check.detail}",
            failure_context=ClusterDispatchFailureContext(
                failed_step="credential_preflight",
                cause_type="CredentialPreflightFailed",
                cause_message=cred_check.detail[:2000],
                cluster_key=dispatch_config.cluster_key,
            ),
        )

    try:
        payload = dispatch_notebook_and_fetch_marker(
            client,
            notebook=notebook,
            workspace_path=workspace_path,
            cluster_key=dispatch_config.cluster_key or "",
            cluster_name=dispatch_config.cluster_name or "",
            job_name=job_name,
            task_key="notebook_task",
            marker_begin=_BOOTSTRAP_MARKER_BEGIN,
            marker_end=_BOOTSTRAP_MARKER_END,
            marker_b64=True,
            poll_timeout_s=poll_timeout_s,
            log=lambda msg: log(f"[dispatch] {msg}"),
        )
    except DispatchMarkerEnvelopeMissing as exc:
        full_stdout = _serialize_stdout(exc.executed_notebook)
        raise ClusterMarkerError(
            f"cluster ran but emitted no envelope: {exc}",
            failure_context=ClusterMarkerFailureContext(
                kind="envelope_missing",
                stdout_excerpt=exc.stdout_excerpt,
                stdout_full=full_stdout,
            ),
            executed_notebook=exc.executed_notebook,
        ) from exc
    except DispatchMarkerDecodeError as exc:
        full_stdout = _serialize_stdout(exc.executed_notebook)
        raise ClusterMarkerError(
            f"cluster marker decode failed: {exc}",
            failure_context=ClusterMarkerFailureContext(
                kind="validation_failed",
                stdout_excerpt=exc.stdout_excerpt,
                stdout_full=full_stdout,
            ),
            executed_notebook=exc.executed_notebook,
        ) from exc
    except DispatchError as exc:
        raise ClusterDispatchError(
            f"cluster dispatch failed at {exc.code}: {exc}",
            failure_context=ClusterDispatchFailureContext(
                failed_step=_dispatch_step_from_code(exc.code),
                cause_type=type(exc).__name__,
                cause_message=str(exc)[:2000],
                workspace_path=workspace_path,
                cluster_key=dispatch_config.cluster_key,
            ),
        ) from exc

    # ---- Validate envelope + unwrap marker ----
    try:
        envelope = ClusterProbeEnvelope.model_validate(payload)
    except Exception as exc:  # noqa: BLE001 — Pydantic raises many shapes
        raise ClusterMarkerError(
            f"envelope validation failed: {exc}",
            failure_context=ClusterMarkerFailureContext(
                kind="validation_failed",
                validation_errors=[str(exc)[:1000]],
            ),
        ) from exc

    if not envelope.ok:
        # The cluster cell's try/except caught its own exception and
        # emitted a structured error envelope. Surface it through
        # AIDPF-2049 with the cluster-reported traceback intact.
        raise ClusterMarkerError(
            f"cluster reported {envelope.error_type}: {envelope.error_message}",
            failure_context=ClusterMarkerFailureContext(
                kind="cluster_reported_error",
                cluster_error_type=envelope.error_type,
                cluster_error_message=envelope.error_message,
                cluster_traceback=envelope.traceback,
            ),
        )

    assert envelope.marker is not None, (
        "ClusterProbeEnvelope._consistency validator should reject "
        "ok=True with no marker"
    )
    return envelope.marker


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


_DISPATCH_CODE_TO_STEP: dict[str, str] = {
    "DISPATCH_UPLOAD_HTTP": "upload_notebook",
    "DISPATCH_JOB_SUBMIT": "create_notebook_job",
    "DISPATCH_TIMEOUT": "poll_run",
    "DISPATCH_RUN_FAILED": "poll_run",
    "DISPATCH_FETCH_OUTPUT": "fetch_output",
}


def _dispatch_step_from_code(code: str) -> str:
    return _DISPATCH_CODE_TO_STEP.get(code, "unknown")


def _serialize_stdout(executed_notebook: dict | None) -> str:
    """Concatenate every cell's stdout text into one string for the
    AIDPF-2049 ``cluster_stdout.log`` companion file.

    Distinct from
    :func:`dispatch.notebook_dispatch._collect_stdout_excerpt` which
    returns only the LAST ~4 KiB. Here we keep the full output so an
    operator inspecting the log file has everything the cluster
    emitted, including the in-cell traceback.
    """
    if executed_notebook is None:
        return ""
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
    return "".join(chunks)


def _find_plugin_checkout(bundle_path: Path) -> Path:
    """Find the plugin checkout root used to build the dispatch wheel.

    The wheel builder needs the directory containing ``pyproject.toml`` +
    ``scripts/oracle_ai_data_platform_fusion_autopilot``. Customer bundle
    directories are commonly siblings of the plugin checkout, so walking up
    from ``bundle.yaml`` alone is not enough. Resolution order:

    1. ``AIDP_FUSION_PLUGIN_CHECKOUT`` override.
    2. Ancestors of ``bundle.yaml`` for legacy in-repo bundles.
    3. Ancestors of this installed module for editable installs.
    """
    env_override = os.environ.get("AIDP_FUSION_PLUGIN_CHECKOUT")
    if env_override:
        candidate = Path(env_override).expanduser().resolve()
        if _is_plugin_checkout(candidate):
            return candidate
        raise FileNotFoundError(
            "AIDP_FUSION_PLUGIN_CHECKOUT does not point to a plugin checkout "
            f"(expected pyproject.toml + scripts/oracle_ai_data_platform_fusion_autopilot): "
            f"{candidate}"
        )

    search_roots = [bundle_path.parent.resolve(), Path(__file__).resolve().parent]
    for root in search_roots:
        for candidate in [root, *root.parents]:
            if _is_plugin_checkout(candidate):
                return candidate

    raise FileNotFoundError(
        f"could not locate plugin checkout (looked for pyproject.toml + "
        f"scripts/oracle_ai_data_platform_fusion_autopilot) walking up from "
        f"{bundle_path!r} and from installed module {Path(__file__).resolve()!r}; "
        "set AIDP_FUSION_PLUGIN_CHECKOUT=/path/to/oracle-ai-data-platform-fusion-autopilot "
        "to override."
    )


def _is_plugin_checkout(candidate: Path) -> bool:
    return (candidate / "pyproject.toml").exists() and (
        candidate / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    ).exists()


__all__ = [
    "ClusterDispatchError",
    "ClusterDispatchFailureContext",
    "ClusterMarkerError",
    "ClusterMarkerFailureContext",
    "dispatch_cluster_probe",
]
