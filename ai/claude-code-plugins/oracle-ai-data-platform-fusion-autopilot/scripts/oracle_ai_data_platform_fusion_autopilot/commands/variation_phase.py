"""Bootstrap's variation-resolution phase.

Runs after the existing pre-onboarding probes (or in place of them when
``--skip-preonboarding-probes`` is set) iff ``bundle.content_pack`` is
non-None. v1 bundles (no ``contentPack:`` block) skip this phase
entirely; their existing ``bootstrap`` behaviour is unchanged.

Responsibilities:

1. Resolve operator identity (Step 5) or raise ``AIDPF-1020``.
2. Load the resolved pack (overlay chain included).
3. Acquire a Spark session, probe bronze once into an ``observed`` dict.
4. Compute ``bronzeSchemaFingerprint`` from the observation (Step 2).
5. For each ``columnAliases.<name>`` and ``semanticVariants.<name>``:
   walk → collect outcome. Never exit early.
6. After both loops complete:

   * If any required no-match outcome was collected → write one
     diagnostic artifact per failure (Step 3), exit non-zero.
   * Otherwise → assemble profile + evidence snapshot, write them
     (Step 4), exit 0.

``--refresh`` semantics (Step 9): re-walk-all every variation point;
no-op only when fingerprints match byte-for-byte. Never emits
``AIDPF-2012``.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Literal

import yaml
from rich.console import Console

from ..orchestrator.content_pack import ResolvedPack, load_full_chain, load_pack
from .coa_resolution import (
    CoaResolutionError,
    CoaResolutionInput,
    resolve_coa_roles,
)
from ..schema.bronze_fingerprint import ColumnInfo, compute_bronze_fingerprint
from ..schema.bronze_schema_snapshot import (
    BronzeSchemaSnapshotSchemaError,
    BronzeSchemaSnapshotV1,
    from_observed as snapshot_from_observed,
    load_bronze_schema_snapshot,
    resolve_snapshot_path,
    snapshot_to_observed,
    write_bronze_schema_snapshot,
)
from ..schema.bundle import Bundle, resolve_content_pack_root
from ..schema.diagnostic_artifact import (
    AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
    AIDPF_2010_COLUMN_ALIAS_UNRESOLVED,
    AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED,
    CandidateProbeOutcome,
    IdentityDiagnosticV1,
    IdentityProbeFailure,
    ObservedColumn,
    VariationPointDiagnosticV1,
    VariationPointFailure,
    write_identity_diagnostic,
    write_variation_diagnostic,
)
from ..schema.evidence_snapshot import (
    ApprovedBy,
    CandidateConsidered,
    EvidenceContainer,
    EvidenceSnapshotV1,
    ResolvedVariationPoint,
    SnapshotEntry,
    SnapshotProvenance,
    write_evidence_snapshot,
)
from ..schema.incremental_impact import IncrementalImpact
from ..schema.path_segment import UnsafePathSegmentError, validate_path_segment
from ..schema.resolutions_input import (
    ResolutionsFileError,
    ResolutionsInputV1,
    validate_against_pack,
)
from ..schema.tenant_profile import (
    TenantProfile,
    load_tenant_profile,
    resolve_profile_path,
)
from .bronze_probe import resolve_observed
from .operator_identity import OperatorIdentityUnresolved, resolve_operator
from .resolution_prompt import PromptResult, prompt_multi_match
from .variation_resolver import (
    AutoResolved,
    CandidateAttempt,
    CandidateWalkResult,
    MultiMatch,
    NoMatch,
    walk_column_alias,
    walk_semantic_variant,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Inputs / outputs
# ---------------------------------------------------------------------------


@dataclass
class VariationPhaseOptions:
    """All knobs the CLI passes into the variation phase."""

    refresh: bool = False
    operator: str | None = None
    non_interactive: bool = False
    resolutions_path: Path | None = None
    spark_session: Any | None = None
    """Caller-provided Spark session. ``None`` → acquire local-mode session.
    Only consulted when ``dispatch_mode == "local"``."""

    spark_factory: Callable[[], Any] | None = None
    """Test injection — replace local-Spark acquisition. The CLI never sets
    this directly; tests use it to inject a mock without monkeypatching."""

    input_fn: Callable[[str], str] | None = None
    """Test injection for the interactive y/N confirmation prompt during
    ``--refresh`` when a pinned value would change. ``None`` falls back
    to stdlib ``input()``. Tests pass a lambda to drive accept/decline."""

    accept_coa_convention: bool = False
    """``--accept-coa-convention``: operator accepts the pack's conventional
    COA role->segment default (or upgrades a legacy back-derived pin) as a
    deliberate convention. Without it, a non-interactive run with no explicit
    ``profile.chartOfAccounts`` config fails closed (AIDPF-2013), and a legacy
    back-derived pin is recorded as ``legacy_unverified`` with a warning."""

    accept_singleton_coa: bool = False
    """``--accept-singleton-coa``: operator asserts all active charts of accounts
    share the COA role->segment layout. Persists
    ``profile.chartOfAccounts.singletonAccepted: true`` so the multi-COA preflight
    gate (AIDPF-2018) passes for a singleton mapping."""

    # --- Cluster-side bootstrap dispatcher knobs ---
    dispatch_mode: Literal["cluster", "local"] = "local"
    """``"local"`` (default in this dataclass to keep existing test
    behaviour) runs today's in-process Spark + walker path.
    ``"cluster"`` dispatches a notebook to the AIDP cluster via
    :func:`commands.cluster_bootstrap_probe.dispatch_cluster_probe` and
    converts the returned marker into the same internal shape — the
    resolution + write code below is identical for both modes."""

    dispatch_config: Any | None = None
    """When ``dispatch_mode == "cluster"``, the resolved
    :class:`commands.bootstrap.ResolvedClusterDispatchConfig` carrying
    cluster coords (post-CLI-override). ``None`` is a programmer error
    in cluster mode and surfaces as a ``ValueError`` from
    :func:`_acquire_probe_result`."""

    env: Any | None = None
    """When ``dispatch_mode == "cluster"``, the resolved
    :class:`schema.bundle.EnvSpec` carrying OCI profile + AIDP id /
    region. Same null contract as ``dispatch_config``."""


@dataclass(frozen=True)
class _ProbeResult:
    """Internal payload threaded between :func:`_acquire_probe_result`
    and the rest of :func:`run_variation_phase`.

    Identical data shape across local and cluster modes — the resolve
    + write code below treats both sources uniformly. Cluster mode
    rehydrates this from a :class:`ClusterProbeMarker` (the
    laptop-only conversion lives in :func:`_probe_result_from_marker`).
    """

    observed: dict[str, list[ColumnInfo]]
    fingerprint: str
    walker_results: dict[tuple[str, str], CandidateWalkResult]


@dataclass
class VariationPhaseOutcome:
    """Result the CLI uses to decide its exit code."""

    exit_code: int
    """0 on success / drift-no-op; non-zero on any failure."""

    profile_path: Path | None = None
    """Set when a profile was (re-)written."""

    evidence_path: Path | None = None
    """Set when a new evidence snapshot was written."""

    diagnostic_paths: list[Path] = field(default_factory=list)
    """Diagnostic artifacts written (any combination of 1020 / 2010 / 2011)."""

    summary: str = ""
    """One-line summary for the CLI to print."""


class RefreshRequiresConfirmation(Exception):
    """Raised in ``--refresh --non-interactive`` mode when re-walk would
    change a pinned variation-point value. Pinned values may not change
    silently; non-interactive runs must refuse and direct the operator to
    re-run interactively (or supply ``--resolutions``)."""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_variation_phase(
    bundle: Bundle,
    bundle_path: Path,
    *,
    options: VariationPhaseOptions,
    console: Console | None = None,
) -> VariationPhaseOutcome:
    """Run the variation-resolution phase end-to-end.

    Args:
        bundle: parsed bundle. Caller guarantees
            ``bundle.content_pack is not None``.
        bundle_path: path to the bundle YAML — drives ``workdir``
            (``bundle_path.resolve().parent``).
        options: CLI flag values.
        console: Rich console for logging (test injection).

    Returns:
        :class:`VariationPhaseOutcome` describing what was written + the
        exit code to return.
    """
    console = console or Console()
    if bundle.content_pack is None:
        # Defensive — caller should have guarded.
        raise ValueError("run_variation_phase requires bundle.content_pack to be set")

    workdir = bundle_path.resolve().parent
    run_id = _generate_run_id()

    # --- Path-traversal hard-fail (defence-in-depth) ---
    # The bundle's contentPack.profile is a free-form string. A malformed
    # or malicious bundle could pass `../../outside`, which after .resolve()
    # would land profiles/evidence/diagnostics OUTSIDE the bundle's
    # persistence root. Validate up-front so the failure surfaces with a
    # clear AIDPF-style message rather than as a vague write failure (or
    # silent arbitrary-write success). The writers also re-validate as
    # defence-in-depth.
    tenant_name = bundle.content_pack.profile or bundle.content_pack.name
    try:
        validate_path_segment(tenant_name, field="contentPack.profile")
    except UnsafePathSegmentError as exc:
        console.print(f"[red]{exc}[/red]")
        return VariationPhaseOutcome(
            exit_code=1, summary=f"unsafe contentPack.profile: {tenant_name!r}"
        )

    # --- Step 5: operator identity gate ---
    try:
        operator = resolve_operator(options.operator)
    except OperatorIdentityUnresolved as exc:
        artifact = IdentityDiagnosticV1(
            runId=run_id,
            tenant=None,
            errorCode=AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED,
            errorMessage=str(exc),
            generatedAt=_now(),
            identityProbe=IdentityProbeFailure(probedSources=exc.probed_sources),
        )
        diag_path = write_identity_diagnostic(workdir, run_id, artifact)
        console.print(
            f"[red]{AIDPF_1020_OPERATOR_IDENTITY_UNRESOLVED}: operator identity "
            f"unresolved. Diagnostic at {diag_path}.[/red]"
        )
        return VariationPhaseOutcome(
            exit_code=1,
            diagnostic_paths=[diag_path],
            summary="AIDPF-1020 operator identity unresolved",
        )

    # --- Pack load + Spark probe ---
    pack_root = resolve_content_pack_root(bundle_path, bundle.content_pack)
    pack: ResolvedPack = load_full_chain(pack_root)
    # Also load the unmerged entry overlay, if any, to access
    # its untouched ``provenance`` block. ``merge_overlay`` discards
    # overlay-level provenance (see content_pack.py:486), so we re-read
    # the entry root to detect skill-authored overlays.
    entry_overlay_pack = _load_entry_overlay_provenance(pack_root)

    try:
        probe_result = _acquire_probe_result(
            bundle=bundle,
            bundle_path=bundle_path,
            pack=pack,
            tenant=tenant_name,
            options=options,
            console=console,
        )
    except Exception as exc:  # noqa: BLE001 — typed by class below
        # Translate cluster-dispatch failures into
        # diagnostic artifacts + a non-zero VariationPhaseOutcome.
        # Local imports keep the cluster-side module out of the
        # local-mode import graph.
        from .cluster_bootstrap_probe import (
            ClusterDispatchError,
            ClusterMarkerError,
        )

        if isinstance(exc, ClusterDispatchError):
            diag_path = _write_cluster_dispatch_diagnostic(
                workdir, run_id, tenant_name, exc.failure_context
            )
            console.print(
                f"[red]AIDPF-2048: cluster dispatch failed at "
                f"{exc.failure_context.failed_step}. "
                f"Diagnostic at {diag_path}.[/red]"
            )
            return VariationPhaseOutcome(
                exit_code=1,
                diagnostic_paths=[diag_path],
                summary=(
                    f"AIDPF-2048 cluster dispatch failed "
                    f"({exc.failure_context.failed_step})"
                ),
            )
        if isinstance(exc, ClusterMarkerError):
            diag_path = _write_cluster_marker_diagnostic(
                workdir, run_id, tenant_name, exc
            )
            console.print(
                f"[red]AIDPF-2049: cluster marker invalid "
                f"({exc.failure_context.kind}). "
                f"Diagnostic at {diag_path}.[/red]"
            )
            return VariationPhaseOutcome(
                exit_code=1,
                diagnostic_paths=[diag_path],
                summary=(
                    f"AIDPF-2049 cluster marker invalid "
                    f"({exc.failure_context.kind})"
                ),
            )
        raise

    observed = probe_result.observed
    fingerprint = probe_result.fingerprint

    # --- --refresh drift detection (Step 9) ---
    profile_path = resolve_profile_path(bundle_path, tenant_name)
    prior_profile: TenantProfile | None = None
    if options.refresh and profile_path.exists():
        prior_profile = load_tenant_profile(profile_path)
        if prior_profile.bronze_schema_fingerprint == fingerprint:
            # An unchanged bronze fingerprint says nothing about NEW charts
            # of accounts (a newly active chart doesn't alter existing
            # bronze schemas) — so the COA advisory runs on this early
            # return too, before either exit below. Advisory-only: it can
            # not change the exit code (metadata-driven-coa-resolution
            # Finding 1).
            _render_coa_advisory(
                console,
                bundle=bundle,
                chart_of_accounts=prior_profile.profile.get("chartOfAccounts"),
            )
            # Back-fill the snapshot if missing / desynced /
            # hand-edited. This is the only path that exits the no-drift
            # branch with a write — profile + evidence stay untouched so
            # the back-fill is observably scoped (no audit-trail noise on
            # the profile timeline for a snapshot-only repair). Without
            # this, pre-3d profiles whose fingerprint never drifts could
            # never recover via the documented `--refresh` remediation;
            # the refresh would no-op forever and preflight would
            # forever degrade to empty `datasetDeltas`.
            now = _now()
            fresh_snapshot = snapshot_from_observed(
                tenant=tenant_name,
                pinned_at=now,
                fingerprint=fingerprint,
                observed=observed,
            )
            repair_reason = _snapshot_needs_repair(
                bundle_path=bundle_path,
                tenant_name=tenant_name,
                live_fingerprint=fingerprint,
            )
            if repair_reason is None:
                console.print(
                    f"[green]No drift detected — fingerprint matches "
                    f"{fingerprint[:24]}... — profile unchanged.[/green]"
                )
                return VariationPhaseOutcome(
                    exit_code=0,
                    summary="bootstrap --refresh: no drift detected",
                )
            write_bronze_schema_snapshot(workdir, tenant_name, fresh_snapshot)
            console.print(
                f"[green]No drift detected — snapshot back-filled from "
                f"observed probe ({repair_reason}) — profile unchanged.[/green]"
            )
            return VariationPhaseOutcome(
                exit_code=0,
                summary=(
                    f"bootstrap --refresh: no drift; snapshot back-filled "
                    f"({repair_reason})"
                ),
            )

    # --- Steps 6/7: walker results sourced from the probe (local
    # mode runs the walkers in-process; cluster mode receives them
    # in the marker payload).
    walker_results = probe_result.walker_results

    # --- Step 8: aggregate failures, write one artifact per failing VP ---
    failure_paths: list[Path] = []
    for (name, kind), outcome in walker_results.items():
        if isinstance(outcome, NoMatch):
            required = _is_required(name, kind, pack)
            if required:
                artifact = _build_variation_artifact(
                    run_id=run_id,
                    tenant=tenant_name,
                    name=name,
                    kind=kind,
                    outcome=outcome,
                    applies_to=_applies_to_for(name, kind, pack),
                    observed=observed,
                    prior_pinned=_prior_pinned(prior_profile, name, kind),
                )
                failure_paths.append(
                    write_variation_diagnostic(workdir, run_id, artifact)
                )

    if failure_paths:
        for path in failure_paths:
            console.print(f"[red]Diagnostic written: {path}[/red]")
        return VariationPhaseOutcome(
            exit_code=1,
            diagnostic_paths=failure_paths,
            summary=f"{len(failure_paths)} variation point(s) unresolved",
        )

    # --- Build the walker-outcome maps the resolutions validator uses ---
    multi_match_outcomes: dict[tuple[str, str], list[str]] = {
        key: outcome.matched
        for key, outcome in walker_results.items()
        if isinstance(outcome, MultiMatch)
    }
    # Under --refresh, AutoResolved outcomes whose chosen value differs
    # from the prior profile's pinned value are eligible for scripted
    # acceptance via --resolutions (mechanism: cli_flag). The validator
    # accepts such entries; the chosen_candidate MUST equal the
    # walker's value (the candidate that actually exists on bronze).
    accepted_autoresolved: dict[tuple[str, str], str] = {}
    if options.refresh and prior_profile is not None:
        for (name, kind), outcome in walker_results.items():
            if not isinstance(outcome, AutoResolved):
                continue
            prior = _prior_pinned(prior_profile, name, kind)
            if prior is not None and prior != outcome.chosen:
                accepted_autoresolved[(name, kind)] = outcome.chosen

    # Single validated load — strict on unknown names / kind / duplicates,
    # permissive on AutoResolved changes under --refresh.
    scripted = _load_resolutions(
        options,
        expected_tenant=tenant_name,
        walker_outcomes=multi_match_outcomes,
        accepted_autoresolved=accepted_autoresolved,
        pack=pack,
    )

    # --- Multi-match resolution (interactive / scripted / non-interactive) ---
    picks: dict[tuple[str, str], PromptResult] = {}
    for key in sorted(multi_match_outcomes):
        if scripted is not None and key in scripted:
            picks[key] = PromptResult(
                chosen=scripted[key], mechanism="cli_flag"
            )
            continue
        result = prompt_multi_match(
            variation_point_name=key[0],
            kind=key[1],
            matched=multi_match_outcomes[key],
            non_interactive=options.non_interactive,
            console=console,
        )
        picks[key] = result

    # If --refresh changes a pinned value, prompt confirm. No silent change to
    # a previously-pinned value. Three acceptance paths:
    #   1. ``--resolutions`` (scripted) — operator supplied an entry
    #      that names this VP; the validator above confirmed it matches
    #      either a MultiMatch outcome or an AutoResolved-change
    #      outcome. Record ``mechanism: cli_flag`` and skip the prompt.
    #   2. ``--non-interactive`` (no resolutions file) — refuses to
    #      make a silent decision; raises ``RefreshRequiresConfirmation``.
    #   3. Interactive y/N prompt — must read a real answer and abort
    #      on no/default. The prior print-only branch fell through and
    #      wrote the profile silently.
    if options.refresh and prior_profile is not None:
        for (name, kind), outcome in walker_results.items():
            chosen = _chosen_value(outcome, picks.get((name, kind)))
            if chosen is None:
                continue  # NoMatch (optional VP) — skip silently.
            prior = _prior_pinned(prior_profile, name, kind)
            if prior is not None and prior != chosen:
                # Path 1: scripted via ``--resolutions``. The validator
                # already confirmed the entry is valid for this VP.
                if scripted is not None and (name, kind) in scripted:
                    picks[(name, kind)] = PromptResult(
                        chosen=chosen, mechanism="cli_flag"
                    )
                    continue
                # Path 2: --non-interactive without scripted approval → abort.
                if options.non_interactive:
                    raise RefreshRequiresConfirmation(
                        f"refresh would change pinned {kind}.{name} from "
                        f"{prior!r} to {chosen!r}; re-run without "
                        f"--non-interactive to confirm, or supply "
                        f"--resolutions with an entry for ({name!r}, {kind!r})."
                    )
                # Path 3: interactive y/N prompt — actually read input.
                if not _prompt_confirm_change(
                    name=name,
                    kind=kind,
                    prior=prior,
                    chosen=chosen,
                    console=console,
                    input_fn=options.input_fn or input,
                ):
                    console.print(
                        f"[yellow]Operator declined to change pinned "
                        f"{kind}.{name}; refresh aborted. Profile + evidence "
                        f"unchanged.[/yellow]"
                    )
                    return VariationPhaseOutcome(
                        exit_code=1,
                        summary=(
                            f"refresh aborted — operator declined to change "
                            f"pinned {kind}.{name}"
                        ),
                    )
                # Operator confirmed — record mechanism as terminal_prompt
                # so the evidence trail reflects the y/N decision.
                picks[(name, kind)] = PromptResult(
                    chosen=chosen, mechanism="terminal_prompt"
                )

    # --- Profile + evidence ---
    resolutions, snapshot_entry_resolutions, mechanism_record = _assemble_resolutions(
        walker_results=walker_results,
        picks=picks,
        operator=operator,
        entry_overlay_pack=entry_overlay_pack,
    )

    now = _now()
    profile = _build_profile(
        tenant=tenant_name,
        pinned_at=now,
        bronze_schema_fingerprint=fingerprint,
        resolutions=resolutions,
        operator=operator,
        mechanism=mechanism_record,
        existing_profile=prior_profile,
        run_id=run_id,
    )
    # Resolve COA semantic roles from explicit config (NOT column existence) and
    # fold the derived columns + per-role provenance into the profile. Fails
    # closed (AIDPF-2013) in a non-interactive run with no accepted convention.
    try:
        profile = _apply_coa_resolution(
            profile=profile,
            pack=pack,
            options=options,
            prior_profile=prior_profile,
            tenant_name=tenant_name,
            console=console,
        )
    except CoaResolutionError as exc:
        console.print(f"[red]{exc}[/red]")
        return VariationPhaseOutcome(exit_code=1, summary=str(exc))
    _write_profile_yaml(profile_path, profile)

    # COA advisory against the freshly resolved profile (the full-path
    # twin of the no-drift call above). Runs AFTER the profile is durable
    # so a mid-advisory interrupt can't lose the resolution. Advisory-only.
    _render_coa_advisory(
        console,
        bundle=bundle,
        chart_of_accounts=profile.profile.get("chartOfAccounts"),
    )

    # Thread skill_version from the entry overlay, when
    # skill-authored) into the snapshot's top-level provenance so audit
    # tooling can correlate evidence files with the skill version that
    # produced them.
    skill_version_for_snapshot: str | None = None
    if _is_skill_authored_overlay(entry_overlay_pack):
        prov = entry_overlay_pack.pack.provenance  # type: ignore[union-attr]
        if prov is not None:
            skill_version_for_snapshot = prov.skill_version

    snapshot = EvidenceSnapshotV1(
        tenant=tenant_name,
        generatedAt=now,
        runId=run_id,
        bronzeSchemaFingerprint=fingerprint,
        provenance=SnapshotProvenance(
            approvedBy=ApprovedBy(
                operator=operator,
                timestamp=now,
                mechanism=mechanism_record,  # type: ignore[arg-type]
            ),
            skillVersion=skill_version_for_snapshot,
            evidence=EvidenceContainer(
                snapshots=[
                    SnapshotEntry(
                        snapshotId=run_id,
                        capturedAt=now,
                        resolutions=snapshot_entry_resolutions,
                    )
                ],
            ),
        ),
    )
    evidence_path = write_evidence_snapshot(workdir, snapshot)

    # Snapshot writes AFTER profile + evidence so the snapshot
    # is never present without a matching profile (rules out a confusing
    # "snapshot exists but profile is gone" state for preflight). Pin-time
    # writer for both initial-pin and `--refresh`-with-drift paths.
    schema_snapshot = snapshot_from_observed(
        tenant=tenant_name,
        pinned_at=now,
        fingerprint=fingerprint,
        observed=observed,
    )
    write_bronze_schema_snapshot(workdir, tenant_name, schema_snapshot)

    console.print(
        f"[green]bootstrap variation phase complete — profile "
        f"{profile_path}, evidence {evidence_path}.[/green]"
    )
    return VariationPhaseOutcome(
        exit_code=0,
        profile_path=profile_path,
        evidence_path=evidence_path,
        summary="variation phase resolved",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _generate_run_id() -> str:
    return _now().strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]


def _columns_for_applies_to(
    observed: dict[str, list[ColumnInfo]],
    applies_to: str,
) -> set[str]:
    """``applies_to`` is ``bronze.<dataset_id>`` → return that dataset's
    column-name set (uppercase variant preserved; walker normalises case)."""
    if "." not in applies_to:
        return set()
    _, dataset = applies_to.split(".", 1)
    return {c.name for c in observed.get(dataset, [])}


def _is_required(name: str, kind: str, pack: ResolvedPack) -> bool:
    if kind == "columnAliases":
        spec = pack.pack.column_aliases.get(name)
    else:
        spec = pack.pack.semantic_variants.get(name)
    return bool(spec and spec.required)


def _applies_to_for(name: str, kind: str, pack: ResolvedPack) -> str:
    if kind == "columnAliases":
        return pack.pack.column_aliases[name].appliesTo
    return pack.pack.semantic_variants[name].appliesTo


def _prior_pinned(
    profile: TenantProfile | None,
    name: str,
    kind: str,
) -> str | None:
    if profile is None:
        return None
    if kind == "columnAliases":
        return profile.resolved.column.get(name)
    return profile.resolved.semantic.get(name)


def _chosen_value(
    outcome: CandidateWalkResult, pick: PromptResult | None
) -> str | None:
    if isinstance(outcome, AutoResolved):
        return outcome.chosen
    if isinstance(outcome, MultiMatch):
        return pick.chosen if pick else None
    return None


def _build_variation_artifact(
    *,
    run_id: str,
    tenant: str,
    name: str,
    kind: str,
    outcome: NoMatch,
    applies_to: str,
    observed: dict[str, list[ColumnInfo]],
    prior_pinned: str | None,
) -> VariationPointDiagnosticV1:
    _, dataset = applies_to.split(".", 1)
    observed_cols = observed.get(dataset, [])
    return VariationPointDiagnosticV1(
        runId=run_id,
        tenant=tenant,
        errorCode=(
            AIDPF_2010_COLUMN_ALIAS_UNRESOLVED
            if kind == "columnAliases"
            else AIDPF_2011_SEMANTIC_VARIANT_UNRESOLVED
        ),
        errorMessage=(
            f"{kind}.{name} has no matching candidate on the tenant's bronze. "
            f"appliesTo={applies_to}"
        ),
        generatedAt=_now(),
        variationPoint=VariationPointFailure(
            name=name,
            kind=kind,  # type: ignore[arg-type]
            appliesTo=applies_to,
            candidatesTried=[
                CandidateProbeOutcome(
                    candidate=a.candidate,
                    outcome=a.outcome,  # type: ignore[arg-type]
                    detail=a.detail,
                )
                for a in outcome.candidates_tried
            ],
            observedBronzeSchema=[
                ObservedColumn(name=col.name, type=col.type, nullable=col.nullable)
                for col in observed_cols
            ],
            priorPinned=prior_pinned,
        ),
    )


def _assemble_resolutions(
    *,
    walker_results: dict[tuple[str, str], CandidateWalkResult],
    picks: dict[tuple[str, str], PromptResult],
    operator: str,
    entry_overlay_pack=None,
) -> tuple[dict[tuple[str, str], str], list[ResolvedVariationPoint], str]:
    """Turn walker outcomes + operator picks into:

    * ``resolutions``: ``{(name, kind): chosen}`` map for the profile writer.
    * ``snapshot_entries``: the per-resolution audit list for the evidence snapshot.
    * ``mechanism_record``: the strongest mechanism applied across all picks
      (used in the profile's approvedBy block).

    When ``entry_overlay_pack`` is a skill-authored overlay,
    stamp ``mechanism: skill_proposed`` on resolutions whose chosen
    candidate matches the overlay's ``provenance.proposals[vp].candidate_added``,
    and copy ``provenance.incremental_impact[vp]`` into the resolved
    variation point. Without this, the initial-onboarding flow
    silently records ``auto_resolve`` even though the skill drafted
    the candidate that resolved — audit trail can't tell skill-driven
    from pack-author-driven resolutions.
    """
    skill_authored = _is_skill_authored_overlay(entry_overlay_pack)
    skill_proposals: dict[str, str] = {}
    skill_incremental_impacts: dict[str, IncrementalImpact] = {}
    if skill_authored:
        prov = entry_overlay_pack.pack.provenance  # type: ignore[union-attr]
        if prov is not None and prov.proposals:
            skill_proposals = {
                vp_name: rec.candidate_added
                for vp_name, rec in prov.proposals.items()
            }
        if prov is not None and prov.incremental_impact:
            skill_incremental_impacts = dict(prov.incremental_impact)

    resolutions: dict[tuple[str, str], str] = {}
    snapshot_resolutions: list[ResolvedVariationPoint] = []
    mechanisms: list[str] = []

    for key, outcome in sorted(walker_results.items()):
        name, kind = key
        if isinstance(outcome, AutoResolved):
            # For AutoResolved outcomes the picks map normally has no
            # entry — but the --refresh confirmation path stamps one
            # when the operator explicitly approves a pinned-value
            # change. Prefer that mechanism over the bare auto_resolve.
            chosen = outcome.chosen
            override = picks.get(key)
            if override is not None and override.chosen == chosen:
                mechanism = override.mechanism
            elif skill_authored and skill_proposals.get(name) == chosen:
                # AutoResolved on a skill-proposed candidate:
                # record skill_proposed instead of bare auto_resolve so
                # the audit trail attributes the resolution to the skill.
                mechanism = "skill_proposed"
            else:
                mechanism = "auto_resolve"
            considered = [
                CandidateConsidered(candidate=chosen, outcome="matched")
            ]
        elif isinstance(outcome, MultiMatch):
            pick = picks[key]
            chosen = pick.chosen
            mechanism = pick.mechanism
            # cli_flag picks driven by a skill-authored overlay
            # become skill_proposed — BUT only when the operator actually
            # picked the skill-proposed candidate. A scripted
            # resolutions.json may legitimately edit the choice (e.g.
            # operator overrides the skill's proposal with a different
            # MultiMatch candidate); in that case the audit trail
            # records cli_flag (not skill_proposed) because the chosen
            # value didn't come from the skill.
            if (
                skill_authored
                and mechanism == "cli_flag"
                and skill_proposals.get(name) == chosen
            ):
                mechanism = "skill_proposed"
            considered = [
                CandidateConsidered(candidate=c, outcome="matched")
                for c in outcome.matched
            ]
        else:  # NoMatch — only reached for required=False
            continue

        resolutions[key] = chosen
        mechanisms.append(mechanism)
        # Copy incrementalImpact onto the resolution ONLY when
        # the chosen candidate matches the skill-proposed candidate.
        # If the operator edited the resolutions.json to pick a
        # DIFFERENT MultiMatch candidate, the skill's impact analysis
        # was computed for a value that didn't get pinned — copying it
        # would mislabel the audit trail (e.g. claim a "promotion"
        # happened when the operator overrode that exact promotion).
        impact_for_resolution = _impact_if_candidate_matches(
            skill_incremental_impacts.get(name),
            skill_proposals.get(name),
            chosen,
        )
        snapshot_resolutions.append(
            ResolvedVariationPoint(
                name=name,
                kind=kind,  # type: ignore[arg-type]
                chosenCandidate=chosen,
                candidatesConsidered=considered,
                incrementalImpact=impact_for_resolution,
            )
        )

    # Record the profile-level mechanism for audit:
    #   1. ``auto_resolve`` is the baseline — any operator-touched
    #      mechanism takes precedence (an operator-touched profile
    #      should NOT look identical to an all-auto profile in the
    #      audit trail).
    #   2. Among operator-touched mechanisms, the WEAKEST wins — a
    #      single ``non_interactive`` choice taints the whole profile.
    # Order (weakest → strongest among operator-touched):
    #   non_interactive < cli_flag < skill_proposed < terminal_prompt.
    operator_touched = [m for m in mechanisms if m != "auto_resolve"]
    if not operator_touched:
        mechanism_record = "auto_resolve"
        # If all resolutions are auto_resolve but ANY came
        # via a skill-proposed candidate, the run is skill-driven.
        if skill_authored and "skill_proposed" in mechanisms:
            mechanism_record = "skill_proposed"
    else:
        precedence_among_operator = [
            "non_interactive",
            "cli_flag",
            "skill_proposed",
            "terminal_prompt",
        ]
        mechanism_record = operator_touched[0]
        for m in precedence_among_operator:
            if m in operator_touched:
                mechanism_record = m
                break

    return resolutions, snapshot_resolutions, mechanism_record


def _load_entry_overlay_provenance(pack_root: Path):
    """Re-load the entry overlay pack (unmerged) to access its untouched
    ``provenance`` block.

    ``merge_overlay`` discards overlay-level provenance (see
    ``orchestrator/content_pack.py:486``), so the merged pack returned
    by ``load_full_chain`` always reflects the BASE's provenance. To
    detect skill-authored overlays we need to re-read the entry root
    via ``load_pack`` (which performs no overlay merging).

    Returns ``None`` when the entry root does not declare ``extends:``
    (i.e. the bundle points directly at a base pack — no overlay layer,
    nothing to thread).
    """
    try:
        entry = load_pack(pack_root)
    except Exception:  # noqa: BLE001 — defensive: any load failure → skip
        return None
    if entry.pack.extends is None:
        # The entry root IS the base pack (no overlay layer).
        return None
    return entry


def _impact_if_candidate_matches(
    impact: IncrementalImpact | None,
    skill_proposal: str | None,
    chosen: str,
) -> IncrementalImpact | None:
    """Return ``impact`` iff the chosen candidate matches what the skill
    proposed — otherwise ``None``.

    The skill's per-VP ``incrementalImpact`` is computed for its
    proposed candidate. If the operator edits
    ``resolutions.json`` to pick a different MultiMatch candidate, the
    skill's impact analysis no longer applies. Copying it onto the
    resolution would mislabel the audit trail (e.g. record a "promotion
    from X to Y" when in fact the operator stayed on X).

    Match-or-drop rule:

    * ``skill_proposal`` set AND equal to ``chosen`` → copy impact.
    * ``impact.new_candidate`` set AND equal to ``chosen`` → copy impact
      (defensive — covers the case where the overlay provides impact
      but no explicit proposal entry).
    * Otherwise → return None, drop the impact silently.
    """
    if impact is None:
        return None
    if skill_proposal is not None and skill_proposal == chosen:
        return impact
    if impact.new_candidate == chosen:
        return impact
    return None


def _is_skill_authored_overlay(entry_overlay_pack) -> bool:
    """Return True iff the entry overlay's provenance carries the
    medallion-author skill_id."""
    if entry_overlay_pack is None:
        return False
    prov = getattr(entry_overlay_pack.pack, "provenance", None)
    if prov is None:
        return False
    return getattr(prov, "skill_id", None) == "aidp-fusion-medallion-author"


def _build_profile(
    *,
    tenant: str,
    pinned_at: datetime,
    bronze_schema_fingerprint: str,
    resolutions: dict[tuple[str, str], str],
    operator: str,
    mechanism: str,
    existing_profile: TenantProfile | None,
    run_id: str,
) -> TenantProfile:
    column_map: dict[str, str] = {}
    semantic_map: dict[str, str] = {}
    for (name, kind), value in resolutions.items():
        if kind == "columnAliases":
            column_map[name] = value
        else:
            semantic_map[name] = value

    # Preserve the existing profile's free-form `profile:` block on refresh.
    free_form: dict[str, Any] = {}
    if existing_profile is not None:
        free_form = dict(existing_profile.profile)

    return TenantProfile(
        schemaVersion=1,
        tenant=tenant,
        pinnedAt=pinned_at,
        bronzeSchemaFingerprint=bronze_schema_fingerprint,
        resolved={  # type: ignore[arg-type]
            "column": column_map,
            "semantic": semantic_map,
        },
        profile=free_form,
        provenance={
            "approvedBy": {
                "operator": operator,
                "timestamp": pinned_at.isoformat(),
                "mechanism": mechanism,
            },
            "runId": run_id,
        },
    )


def _pack_coa_default(pack: ResolvedPack, tenant_name: str) -> dict | None:
    """The pack's conventional COA default mapping, as a dict, or None.

    Indexes ``pack.profiles`` by the active profile name; falls back to the sole
    profile when there is exactly one.
    """
    profiles = pack.pack.profiles or {}
    chosen = None
    if tenant_name in profiles:
        chosen = profiles[tenant_name]
    elif len(profiles) == 1:
        chosen = next(iter(profiles.values()))
    if chosen is None or chosen.chart_of_accounts is None:
        return None
    return chosen.chart_of_accounts.model_dump(by_alias=True, exclude_none=True)


def _render_coa_advisory(
    console: "Console",
    *,
    bundle: "Bundle",
    chart_of_accounts: "dict | None",
) -> None:
    """Non-blocking bootstrap COA advisory (feature:
    metadata-driven-coa-resolution, Phase 1).

    Compares the charts of accounts VISIBLE to the configured Fusion user
    (transactional REST LOVs, laptop-side, `_probe_bicc` env-credential
    contract) against the given ``chartOfAccounts`` mapping and prints
    yellow, advisory-only lines. Two call paths share this helper: the
    no-drift ``--refresh`` early-return branch (prior profile) and the full
    path after COA resolution (fresh profile).

    NEVER affects the phase outcome: ``run_coa_advisory`` is fail-soft by
    contract (skips on missing creds / 401 / 403 / timeout / budget expiry),
    and this wrapper is belt-and-braces guarded on top.
    """
    try:
        from .coa_advisory import run_coa_advisory

        result = run_coa_advisory(
            service_url=bundle.fusion.service_url,
            chart_of_accounts=chart_of_accounts,
        )
        if result.kind == "skipped":
            console.print(
                f"[yellow]COA advisory skipped: {result.skip_reason}[/yellow]"
            )
            return
        for line in result.lines:
            console.print(f"[yellow]{line}[/yellow]")
    except Exception as exc:  # noqa: BLE001 — advisory must never block bootstrap
        console.print(
            f"[yellow]COA advisory skipped: unexpected "
            f"{type(exc).__name__}: {str(exc)[:120]}[/yellow]"
        )


def _apply_coa_resolution(
    *,
    profile: TenantProfile,
    pack: ResolvedPack,
    options: "VariationPhaseOptions",
    prior_profile: TenantProfile | None,
    tenant_name: str,
    console: Console,
) -> TenantProfile:
    """Resolve COA semantic-role aliases and fold the result into ``profile``.

    Returns the profile with ``resolved.column.coa_*`` derived,
    ``profile.chartOfAccounts`` canonical, and
    ``provenance.chartOfAccounts.roles`` recorded per role. Raises
    :class:`CoaResolutionError` (fail-closed) when no safe value exists.
    """
    semantic_role_aliases = {
        name: spec.role
        for name, spec in pack.pack.column_aliases.items()
        if spec.resolution == "semanticRole" and spec.role
    }
    if not semantic_role_aliases:
        return profile

    prior_coa = None
    prior_role_prov = None
    prior_resolved_col = None
    if prior_profile is not None:
        prior_coa = prior_profile.profile.get("chartOfAccounts")
        prior_role_prov = (prior_profile.provenance.get("chartOfAccounts") or {}).get(
            "roles"
        )
        prior_resolved_col = dict(prior_profile.resolved.column)

    # A pre-authored chartOfAccounts on a fresh (non-refresh) bootstrap is
    # explicit operator config; on --refresh it's the carried-forward pin.
    explicit_config = None
    existing_coa = None
    if prior_coa is not None:
        if options.refresh:
            existing_coa = prior_coa
        else:
            explicit_config = prior_coa

    inp = CoaResolutionInput(
        semantic_role_aliases=semantic_role_aliases,
        existing_chart_of_accounts=existing_coa,
        existing_role_provenance=prior_role_prov,
        existing_resolved_column=prior_resolved_col,
        explicit_config=explicit_config,
        pack_default=_pack_coa_default(pack, tenant_name),
        interactive=not options.non_interactive,
        accept_convention=options.accept_coa_convention,
        accept_singleton=options.accept_singleton_coa,
        is_refresh=options.refresh,
    )
    result = resolve_coa_roles(inp)

    for warning in result.warnings:
        console.print(f"[yellow]COA: {warning}[/yellow]")

    # Fold into the profile: resolved.column (derived), profile.chartOfAccounts
    # (canonical), provenance.chartOfAccounts.roles (per-role).
    profile.resolved.column.update(result.column_map)
    profile.profile["chartOfAccounts"] = result.chart_of_accounts
    coa_prov = profile.provenance.setdefault("chartOfAccounts", {})
    coa_prov["roles"] = result.role_provenance
    return profile


def _write_profile_yaml(path: Path, profile: TenantProfile) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = profile.model_dump(mode="json", by_alias=True, exclude_none=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, default_flow_style=False)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Spark session management
# ---------------------------------------------------------------------------


def _snapshot_needs_repair(
    *,
    bundle_path: Path,
    tenant_name: str,
    live_fingerprint: str,
) -> str | None:
    """Decide whether no-drift refresh should rewrite the snapshot
    inside the no-drift ``--refresh`` branch.

    Returns ``None`` when the snapshot is healthy (file exists, parses,
    metadata fingerprint matches ``live_fingerprint``, and a fresh
    recompute over the snapshot's ``datasets`` produces the same value).
    Returns a short reason string when repair is needed; the caller
    rewrites the snapshot atomically using the live probe and surfaces
    the reason in the operator-visible message.

    Repair triggers:

    * **Absent**: snapshot file does not exist. A pre-3d profile, or a
      partial-failure state where the snapshot write failed after the
      profile/evidence writes succeeded.
    * **Unparseable**: file exists but YAML / Pydantic validation fails
      (schemaVersion unknown, malformed YAML, missing keys).
    * **Metadata desync**: snapshot's own ``bronze_schema_fingerprint``
      field differs from the freshly-computed live fingerprint.
      Indicates a snapshot whose dataset list was never updated after
      the live bronze changed — preflight would later recompute, find
      the cross-check failed, degrade to empty deltas, and tell the
      operator to ``--refresh``. Without back-fill, that refresh would
      no-op forever.
    * **Content cross-check failure**: recomputing
      :func:`compute_bronze_fingerprint` over the snapshot's
      ``datasets`` produces a different hash than the snapshot's own
      metadata fingerprint OR the live fingerprint. Catches
      hand-edited content where the fingerprint field stayed stale.

    Pure function (modulo a file read) — no Spark, no mutation.
    """
    snapshot_path = resolve_snapshot_path(bundle_path, tenant_name)
    if not snapshot_path.exists():
        return "snapshot absent"
    try:
        snapshot = load_bronze_schema_snapshot(snapshot_path)
    except BronzeSchemaSnapshotSchemaError:
        return "snapshot unparseable"
    if snapshot.bronze_schema_fingerprint != live_fingerprint:
        return "snapshot metadata fingerprint desynced"
    recomputed = compute_bronze_fingerprint(
        observed=snapshot_to_observed(snapshot)
    )
    if recomputed != snapshot.bronze_schema_fingerprint:
        return "snapshot content fingerprint desynced"
    if recomputed != live_fingerprint:
        # Defence-in-depth: should be unreachable given the two checks
        # above, but a future divergence would silently leave a stale
        # snapshot. Surface it as a repair so the next read works.
        return "snapshot content does not match live fingerprint"
    return None


def _write_cluster_dispatch_diagnostic(
    workdir: Path, run_id: str, tenant: str, failure_context
) -> Path:
    """Translate a :class:`ClusterDispatchFailureContext` into an
    ``AIDPF-2048.json`` artifact via the canonical writer.

    Lives here (not inside `cluster_bootstrap_probe.py`) so the
    `dispatch/`-touching module stays decoupled from the artifact
    contract; the artifact writer + Pydantic models live in `schema/`
    where every diagnostic surface is centralised."""
    from ..schema.diagnostic_artifact import (
        ClusterDispatchDiagnosticV1,
        ClusterDispatchFailure,
        write_cluster_dispatch_diagnostic,
    )

    artifact = ClusterDispatchDiagnosticV1(
        runId=run_id,
        tenant=tenant,
        errorCode="AIDPF-2048",
        errorMessage=(
            f"cluster dispatch failed at {failure_context.failed_step}: "
            f"{failure_context.cause_message}"
        ),
        generatedAt=_now(),
        clusterDispatch=ClusterDispatchFailure(
            failedStep=failure_context.failed_step,
            causeType=failure_context.cause_type,
            causeMessage=failure_context.cause_message,
            workspacePath=failure_context.workspace_path,
            clusterKey=failure_context.cluster_key,
            runState=failure_context.run_state,
            pollElapsedSeconds=failure_context.poll_elapsed_seconds,
        ),
    )
    return write_cluster_dispatch_diagnostic(workdir, run_id, artifact)


def _write_cluster_marker_diagnostic(
    workdir: Path, run_id: str, tenant: str, exc
) -> Path:
    """Translate a :class:`ClusterMarkerError` into an
    ``AIDPF-2049.json`` artifact + the companion ``cluster_stdout.log``
    via the canonical writer."""
    from ..schema.diagnostic_artifact import (
        ClusterMarkerDiagnosticV1,
        ClusterMarkerFailure,
        write_cluster_marker_diagnostic,
    )

    ctx = exc.failure_context
    artifact = ClusterMarkerDiagnosticV1(
        runId=run_id,
        tenant=tenant,
        errorCode="AIDPF-2049",
        errorMessage=f"cluster marker invalid ({ctx.kind})",
        generatedAt=_now(),
        clusterMarker=ClusterMarkerFailure(
            kind=ctx.kind,
            clusterErrorType=ctx.cluster_error_type,
            clusterErrorMessage=ctx.cluster_error_message,
            clusterTraceback=ctx.cluster_traceback,
            validationErrors=list(ctx.validation_errors),
            stdoutExcerpt=ctx.stdout_excerpt,
            stdoutLogPath="cluster_stdout.log",
        ),
    )
    return write_cluster_marker_diagnostic(
        workdir,
        run_id,
        artifact,
        stdout_full=ctx.stdout_full,
    )


def _acquire_probe_result(
    *,
    bundle: Bundle,
    bundle_path: Path,
    pack: ResolvedPack,
    tenant: str,
    options: VariationPhaseOptions,
    console: Console,
) -> _ProbeResult:
    """Run the bronze probe + variation walkers; return the shared
    :class:`_ProbeResult` payload.

    Local mode (the default + every test that doesn't opt in):
    today's path — acquire local Spark, ``describe_bronze``,
    ``compute_bronze_fingerprint``, walk each variation point
    in-process.

    Cluster mode: dispatch a notebook to the AIDP
    cluster via
    :func:`commands.cluster_bootstrap_probe.dispatch_cluster_probe`,
    convert the returned :class:`ClusterProbeMarker` into the same
    ``_ProbeResult`` shape. The probe never touches a local Spark
    session in cluster mode.
    """
    if options.dispatch_mode == "cluster":
        if options.dispatch_config is None or options.env is None:
            raise ValueError(
                "VariationPhaseOptions.dispatch_mode='cluster' requires "
                "both `dispatch_config` and `env` to be set. The CLI "
                "(commands/bootstrap.py) populates both; tests calling "
                "run_variation_phase directly must do the same or use "
                "dispatch_mode='local'."
            )
        # Local import to keep the boundary direction explicit — this
        # module pulls in the cluster-dispatch path only when actually
        # entering cluster mode. Local-mode test runs stay free of the
        # dispatch import (and its transitive REST / oci deps).
        from .cluster_bootstrap_probe import dispatch_cluster_probe

        marker = dispatch_cluster_probe(
            env=options.env,
            bundle=bundle,
            bundle_path=bundle_path,
            pack=pack,
            dispatch_config=options.dispatch_config,
            tenant=tenant,
            console=console,
        )
        return _probe_result_from_marker(marker)

    # ---- Local mode (today's path) ----
    # Lazy import — keep the credential resolver (and its transitive
    # orchestrator surface) off the module-load path for callers that
    # never enter local mode.
    from ..orchestrator.runtime import (
        CredentialResolutionError,
        _resolve_password,
    )

    catalog = bundle.aidp.catalog
    bronze_schema = bundle.aidp.bronze_schema

    # Resolve the BICC password up front (fail fast, before Spark). The
    # source-schema producer needs it when a bronze table is not yet
    # landed; resolving here keeps one credential contract and surfaces a
    # credential error before any probe work — as a credential-specific
    # error, NOT a generic AIDPF-2049 (that code is reserved for genuine
    # probe / cluster-marker failures).
    password_ref = bundle.fusion.password
    if isinstance(password_ref, str) and password_ref.startswith("${aidp:secret:"):
        raise CredentialResolutionError(
            "bundle.fusion.password is an ${aidp:secret:...} reference, which "
            "the runtime resolver does not support on the local dispatch "
            "path. Use ${env:VAR} or ${vault:OCID} (same as the local run "
            "path), or run bootstrap in cluster dispatch mode where the creds "
            "cell loads the secret from the AIDP credential store."
        )
    resolved_password = _resolve_password(password_ref).get_secret_value()

    spark = _resolve_spark(options)
    try:
        observed = resolve_observed(
            spark,
            catalog=catalog,
            bronze_schema=bronze_schema,
            pack=pack,
            bundle=bundle,
            resolved_password=resolved_password,
        )
    finally:
        _close_spark_if_owned(spark, options)

    fingerprint = compute_bronze_fingerprint(observed=observed)

    walker_results: dict[tuple[str, str], CandidateWalkResult] = {}
    for name, spec in pack.pack.column_aliases.items():
        # Semantic-role aliases (e.g. COA balancing) are resolved from explicit
        # config via the COA ladder, NOT by column-existence -- skip the walk.
        if spec.resolution == "semanticRole":
            continue
        cols = _columns_for_applies_to(observed, spec.appliesTo)
        walker_results[(name, "columnAliases")] = walk_column_alias(spec, cols)
    for name, spec in pack.pack.semantic_variants.items():
        cols = _columns_for_applies_to(observed, spec.appliesTo)
        walker_results[(name, "semanticVariants")] = walk_semantic_variant(spec, cols)

    return _ProbeResult(
        observed=observed,
        fingerprint=fingerprint,
        walker_results=walker_results,
    )


def _probe_result_from_marker(marker) -> _ProbeResult:
    """Convert a :class:`schema.cluster_probe_marker.ClusterProbeMarker`
    back to the internal :class:`_ProbeResult` shape.

    Per-walker-outcome conversion: the cluster's
    :class:`WalkerOutcomeMarker` Pydantic models translate to the
    laptop's :class:`AutoResolved` / :class:`MultiMatch` /
    :class:`NoMatch` dataclasses (same fields, just JSON-friendly
    wrappers stripped).
    """
    observed: dict[str, list[ColumnInfo]] = {
        ds: [c.to_column_info() for c in cols]
        for ds, cols in marker.observed_schema.items()
    }
    walker_results: dict[tuple[str, str], CandidateWalkResult] = {}
    for entry in marker.walker_results:
        key = (entry.name, entry.kind)
        if entry.outcome == "auto_resolved":
            assert entry.chosen is not None  # envelope validator guarantees this
            walker_results[key] = AutoResolved(chosen=entry.chosen)
        elif entry.outcome == "multi_match":
            walker_results[key] = MultiMatch(matched=list(entry.matched))
        else:  # no_match
            walker_results[key] = NoMatch(
                candidates_tried=[
                    CandidateAttempt(
                        candidate=a.candidate,
                        outcome=a.outcome,
                        detail=a.detail,
                    )
                    for a in entry.candidates_tried
                ]
            )
    return _ProbeResult(
        observed=observed,
        fingerprint=marker.bronze_fingerprint,
        walker_results=walker_results,
    )


def _resolve_spark(options: VariationPhaseOptions):
    if options.spark_session is not None:
        return options.spark_session
    if options.spark_factory is not None:
        return options.spark_factory()
    return _acquire_local_spark()


def _acquire_local_spark():  # pragma: no cover — exercised in integration tests
    try:
        from pyspark.sql import SparkSession  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "bootstrap variation phase requires PySpark. Install with "
            "`pip install pyspark` or pass --skip-bootstrap if v1-only."
        ) from exc
    return (
        SparkSession.builder.master("local[1]")
        .appName("aidp-fusion-autopilot-bootstrap")
        .getOrCreate()
    )


def _close_spark_if_owned(
    spark, options: VariationPhaseOptions
) -> None:
    if options.spark_session is None and options.spark_factory is None:
        # We acquired it ourselves — best-effort close.
        with contextlib.suppress(Exception):
            spark.stop()


# ---------------------------------------------------------------------------
# Resolutions-file loading
# ---------------------------------------------------------------------------


def _load_resolutions(
    options: VariationPhaseOptions,
    *,
    expected_tenant: str,
    walker_outcomes: dict[tuple[str, str], list[str]],
    accepted_autoresolved: dict[tuple[str, str], str],
    pack: ResolvedPack,
) -> dict[tuple[str, str], str] | None:
    """Parse + validate the ``--resolutions`` file if provided.

    Returns ``{(name, kind): chosen}`` covering BOTH the multi-match
    picks loop AND the refresh-change acceptance path. The validator
    runs once with the full set of permitted entries — entries for
    multi-matches, plus (when ``--refresh`` is in play) entries for
    AutoResolved outcomes whose value differs from the prior profile.

    ``None`` when the flag was not supplied.
    """
    if options.resolutions_path is None:
        return None

    with options.resolutions_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    input_data = ResolutionsInputV1.model_validate(raw)

    validate_against_pack(
        input_data=input_data,
        expected_tenant=expected_tenant,
        column_alias_names=set(pack.pack.column_aliases.keys()),
        semantic_variant_names=set(pack.pack.semantic_variants.keys()),
        walker_outcomes=walker_outcomes,
        accepted_autoresolved=accepted_autoresolved,
    )

    return {
        (entry.name, entry.kind): entry.chosen_candidate
        for entry in input_data.resolutions
    }


def _prompt_confirm_change(
    *,
    name: str,
    kind: str,
    prior: str,
    chosen: str,
    console: Console,
    input_fn: Callable[[str], str],
) -> bool:
    """Prompt the operator to confirm a pinned-value change during
    ``--refresh``. Default is **no** — operator must explicitly type
    ``y`` / ``yes`` to accept.

    Returns ``True`` on accept, ``False`` on decline / default.
    """
    console.print(
        f"[yellow]Variation {name!r} would change from "
        f"{prior!r} → {chosen!r}.[/yellow]"
    )
    raw = input_fn(f"Confirm change to {kind}.{name}? (y/N): ").strip().lower()
    return raw in ("y", "yes")


__all__ = [
    "RefreshRequiresConfirmation",
    "VariationPhaseOptions",
    "VariationPhaseOutcome",
    "run_variation_phase",
]
