"""Runtime preflight for bronze schema fingerprint drift detection.

Runs at the start of every ``aidp-fusion-autopilot run --mode incremental``,
AFTER ``_run_content_pack_backend`` mints Spark + ``run_id`` but BEFORE
``state.ensure_state_table`` and any state-row write.

Compares the live bronze schema fingerprint against
``profile.bronze_schema_fingerprint`` (pinned at bootstrap). On mismatch:

* Writes ``<workdir>/.aidp/diagnostics/<run_id>/AIDPF-2012.json``
  with the structured drift context (``SchemaDriftFailure`` + per-
  pinned-VP ``affectedVariationPoints``).
* RETURNS ``PreflightOutcome(kind="drift", ...)``. The caller
  (``_run_content_pack_backend``) is the ONLY place that raises
  :class:`schema.errors.SchemaDriftDetectedError` so the CLI's
  catch arm can map to exit 14.

Skip cases:

* ``--mode seed`` → seed is the new baseline; skip.
* ``--force-fingerprint-skip`` → probe + record both fingerprints
  in the outcome (caller writes audit row via
  :func:`state.write_fingerprint_skip_row`); skip comparison.
* Legacy / placeholder fingerprint → WARN log once; skip.

The probe is the same ``commands.bronze_probe.describe_bronze``
feature #2 ships; the fingerprint is the same
``schema.bronze_fingerprint.compute_bronze_fingerprint`` — single
source of truth across bootstrap and this preflight.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ..commands.bronze_probe import BronzeProbeFailure, describe_bronze_present
from ..schema.bronze_fingerprint import ColumnInfo, compute_bronze_fingerprint
from ..schema.bronze_schema_snapshot import (
    BronzeSchemaSnapshotSchemaError,
    BronzeSchemaSnapshotV1,
    load_bronze_schema_snapshot,
    resolve_snapshot_path,
    snapshot_to_observed,
)
from ..schema.diagnostic_artifact import (
    AIDPF_2012_SCHEMA_DRIFT_DETECTED,
    AffectedVariationPoint,
    ColumnTypeChange,
    DatasetSchemaDelta,
    ObservedColumn,
    SchemaDriftDiagnosticV1,
    SchemaDriftFailure,
    write_schema_drift_diagnostic,
)

if TYPE_CHECKING:  # pragma: no cover — type-only
    from pyspark.sql import SparkSession

    from ..orchestrator.content_pack import ResolvedPack
    from ..schema.bundle import Bundle
    from ..schema.tenant_profile import TenantProfile


logger = logging.getLogger(__name__)


# A well-formed pinned fingerprint per
# ``schema.bronze_fingerprint.compute_bronze_fingerprint``:
# "sha256:<64-hex>". Anything else (legacy/placeholder sentinels
# like ``sha256:placeholder-finance-default-2026-06-05``) → legacy
# graceful-degrade path.
_VALID_FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

_LEGACY_WARN_EMITTED = False
"""Module-level flag so the legacy-profile WARN log fires once per
process. Reset by tests via :func:`_reset_legacy_warn` if needed."""


PreflightKind = Literal[
    "match", "drift", "skip_seed", "skip_legacy_profile", "skip_force_flag"
]


@dataclass(frozen=True)
class PreflightOutcome:
    """Result of :func:`check_bronze_fingerprint_drift`.

    Caller (``_run_content_pack_backend``) inspects ``kind`` to
    decide what to do. The helper itself NEVER raises
    :class:`schema.errors.SchemaDriftDetectedError` — that's the
    CLI-mapping boundary, raised by the caller from the outcome's
    fields.
    """

    kind: PreflightKind
    prior_fingerprint: str | None = None
    current_fingerprint: str | None = None
    diagnostic_path: Path | None = None
    """Set when ``kind == "drift"`` — path to the written
    ``AIDPF-2012.json``."""

    summary: str = ""
    """Human-readable hand-off message; used for stderr printing
    on drift. Empty for non-drift outcomes, EXCEPT when absent bronze
    tables were baseline-filled — then it names them so the effective
    fingerprint is never mistaken for an all-live probe."""

    absent_datasets: tuple[str, ...] = ()
    """Bronze dataset ids whose tables were ABSENT (never materialised)
    and therefore baseline-filled from the pinned schema snapshot — or
    tolerated un-filled when the pack declared them after the pin
    (feature: bronze-fingerprint-gate-scope). Empty on the healthy
    all-present path."""


def check_bronze_fingerprint_drift(
    *,
    spark: "SparkSession",
    bundle: "Bundle",
    bundle_path: Path,
    pack: "ResolvedPack",
    profile: "TenantProfile",
    run_id: str,
    mode: str,
    workdir: Path,
    force_skip: bool = False,
) -> PreflightOutcome:
    """Probe live bronze, compute fingerprint, compare to pinned.

    Args:
        spark: active Spark session (caller owns; preflight does NOT
            create one).
        bundle: loaded ``Bundle`` (carries ``aidp.catalog`` +
            ``aidp.bronze_schema`` for the probe).
        bundle_path: path to ``bundle.yaml`` (unused today but
            threaded for forward-compat with future per-tenant
            checks).
        pack: resolved content pack — the bronze dataset list comes
            from ``pack.bronze_yaml["datasets"]`` (the same source
            bootstrap uses).
        profile: loaded ``TenantProfile`` — read
            ``bronze_schema_fingerprint`` + ``resolved.*`` for the
            affected-VP diff.
        run_id: the SAME run_id ``_run_content_pack_backend``
            minted. Threads through to the diagnostic artifact path
            so the run, the drift artifact, and any force-skip
            audit row all correlate.
        mode: ``"seed"`` | ``"incremental"``.
        workdir: persistence root (``bundle_path.resolve().parent``).
        force_skip: ``--force-fingerprint-skip`` operator flag.

    Returns:
        :class:`PreflightOutcome`. Never raises drift-typed
        exceptions; caller is responsible for that.
    """
    # ─── Skip: --mode seed ─────────────────────────────────────────
    if mode == "seed":
        logger.debug("Bronze fingerprint drift gate skipped in seed mode")
        return PreflightOutcome(kind="skip_seed")

    # ─── Skip: legacy / placeholder fingerprint ────────────────────
    prior_fingerprint = profile.bronze_schema_fingerprint
    if _is_legacy_fingerprint(prior_fingerprint):
        _emit_legacy_warn_once()
        return PreflightOutcome(kind="skip_legacy_profile")

    # ─── Probe + compute current fingerprint (UNCONDITIONAL) ───────
    # Even under --force-fingerprint-skip we run the probe so the
    # outcome carries a real `current_fingerprint` for the audit row.
    dataset_ids = _bronze_dataset_ids(pack)
    # Map id -> physical target table (pack permits id != target, e.g.
    # gl_journal_lines -> gl_journal_headers); DESCRIBE the target, key by id.
    table_names = {
        nid: pack.bronze[nid].target
        for nid in dataset_ids
        if nid in getattr(pack, "bronze", {}) and getattr(pack.bronze[nid], "target", None)
    }
    # Snapshot path is keyed by `bundle.contentPack.profile` (same key
    # bootstrap writes under), NOT `profile.tenant` — see the comment at the
    # drift branch below.
    snapshot_key = bundle.content_pack.profile or bundle.content_pack.name
    snapshot: BronzeSchemaSnapshotV1 | None = None
    try:
        # Absence-tolerant probe (feature: bronze-fingerprint-gate-scope):
        # a bronze table that was never materialised (e.g. its extract is
        # blocked — LIMITS.md L2) must not abort every incremental. Present
        # tables are all still probed and all still drift-checked.
        observed, absent_ids = describe_bronze_present(
            spark,
            catalog=bundle.aidp.catalog,
            bronze_schema=bundle.aidp.bronze_schema,
            dataset_ids=dataset_ids,
            table_names=table_names,
        )
        # Baseline-filled EFFECTIVE observation: live columns for present
        # tables; the pinned snapshot's columns for absent ones. "Assume an
        # absent table still looks like its pin; verify everything else
        # live." The effective fingerprint is then comparable to the pinned
        # monolithic hash — and a dataset REMOVED from the pack falls out of
        # the effective set (it is not a current pack id), so pack-removal
        # still drifts exactly as today.
        filled: dict[str, list[ColumnInfo]] = {}
        unpinned_absent: list[str] = []
        if absent_ids:
            snapshot = _load_snapshot_if_present(
                bundle_path=bundle_path,
                profile_name=snapshot_key,
                profile=profile,
            )
            if snapshot is None:
                # Nothing to baseline-fill from → fail closed (absence-
                # tolerance must never silently drop drift protection).
                # Raised as BronzeProbeFailure so --force-fingerprint-skip
                # keeps its existing break-glass reach.
                first = absent_ids[0]
                raise BronzeProbeFailure(
                    dataset_id=first,
                    fully_qualified=(
                        f"{bundle.aidp.catalog}.{bundle.aidp.bronze_schema}."
                        f"{table_names.get(first, first)}"
                    ),
                    cause=RuntimeError(
                        f"bronze table(s) absent ({', '.join(absent_ids)}) and "
                        f"no healthy schema snapshot exists to baseline-fill "
                        f"from. Run `aidp-fusion-autopilot bootstrap --refresh` to "
                        f"(re)write profiles/{snapshot_key}.schema-snapshot.yaml "
                        f"(no-drift refresh back-fills it without repinning)."
                    ),
                )
            snapshot_observed = snapshot_to_observed(snapshot)
            for did in absent_ids:
                if did in snapshot_observed:
                    filled[did] = snapshot_observed[did]
                else:
                    # Declared after the pin and never landed — tolerated,
                    # unprotected until seeded or re-pinned.
                    unpinned_absent.append(did)
            logger.warning(
                "Bronze fingerprint gate: %d absent table(s) tolerated — "
                "baseline-filled from snapshot: %s%s. Drift protection for "
                "them resumes once materialised.",
                len(absent_ids),
                sorted(filled) or "(none)",
                (
                    f"; unpinned (declared after the pin, unprotected until "
                    f"seeded or re-pinned): {sorted(unpinned_absent)}"
                    if unpinned_absent
                    else ""
                ),
            )
        effective_observed = {**observed, **filled}
        current_fingerprint = compute_bronze_fingerprint(observed=effective_observed)
    except BronzeProbeFailure as exc:
        # A declared bronze table is unreachable (e.g. a dataset whose extract
        # failed, so its table was never materialised). Under the break-glass
        # flag the operator has accepted skipping the drift gate — don't let a
        # broken probe abort the run; proceed with no current fingerprint.
        if force_skip:
            logger.warning(
                "Bronze fingerprint probe failed (%s); --force-fingerprint-skip "
                "set, proceeding without a current fingerprint.", exc,
            )
            return PreflightOutcome(
                kind="skip_force_flag",
                prior_fingerprint=prior_fingerprint,
                current_fingerprint=None,
                summary=f"--force-fingerprint-skip bypassed comparison (probe failed: {exc})",
            )
        raise

    # Names the baseline-filled datasets on every outcome so the effective
    # fingerprint is never mistaken for an all-live probe.
    absent_note = (
        f"absent bronze tolerated (baseline-filled from snapshot): "
        f"{sorted(filled)}"
        + (f"; unpinned absent: {sorted(unpinned_absent)}" if unpinned_absent else "")
        if absent_ids
        else ""
    )

    # ─── Skip: --force-fingerprint-skip ────────────────────────────
    if force_skip:
        return PreflightOutcome(
            kind="skip_force_flag",
            prior_fingerprint=prior_fingerprint,
            current_fingerprint=current_fingerprint,
            summary="--force-fingerprint-skip bypassed comparison"
            + (f" ({absent_note})" if absent_note else ""),
            absent_datasets=tuple(absent_ids),
        )

    # ─── Compare (effective fingerprint vs pinned) ─────────────────
    if prior_fingerprint == current_fingerprint:
        return PreflightOutcome(
            kind="match",
            prior_fingerprint=prior_fingerprint,
            current_fingerprint=current_fingerprint,
            summary=absent_note,
            absent_datasets=tuple(absent_ids),
        )

    # ─── Drift: write artifact + return outcome ────────────────────
    # Affected-VP diff runs on the EFFECTIVE observation: absent datasets'
    # pinned columns stay visible via their baseline fill, so a variation
    # point living only in an absent table is never falsely reported as
    # "NO LONGER EXISTS".
    affected_vps = _compute_affected_variation_points(
        pack=pack, profile=profile, observed=effective_observed
    )
    # Read the pinned snapshot, when present and healthy, and diff it
    # against the effective observation to populate per-dataset column-level
    # deltas. Absent / unparseable / desynced snapshot degrades to empty
    # `datasetDeltas` + a one-time WARN log; drift signal must always
    # reach the artifact even when the snapshot path is unhealthy.
    # (Already loaded above when absent tables were baseline-filled —
    # guaranteed healthy in that case, else the probe raised.)
    # Snapshot path is keyed by `bundle.contentPack.profile` (same key
    # bootstrap writes under), NOT `profile.tenant` — a hand-authored
    # pre-3d profile YAML may carry a different `tenant:` value than
    # the active profile name, and using the YAML field as the path
    # key would silently look in the wrong place after a healthy
    # back-fill.
    if snapshot is None:
        snapshot = _load_snapshot_if_present(
            bundle_path=bundle_path,
            profile_name=snapshot_key,
            profile=profile,
        )
    # Baseline-filled entries diff as identical by construction → no false
    # "all columns removed" delta for an absent table. A dataset REMOVED
    # from the pack is genuinely gone from the effective set, so its
    # "all columns removed" delta is a TRUE signal.
    dataset_deltas: list[DatasetSchemaDelta] = (
        _compute_dataset_deltas(snapshot=snapshot, observed=effective_observed)
        if snapshot is not None
        else []
    )
    artifact = SchemaDriftDiagnosticV1(
        runId=run_id,
        tenant=profile.tenant,
        errorCode=AIDPF_2012_SCHEMA_DRIFT_DETECTED,
        errorMessage=(
            "Live bronze fingerprint differs from the value pinned in the "
            "tenant profile. Re-run `aidp-fusion-autopilot bootstrap --refresh` "
            "to repin."
        ),
        generatedAt=datetime.now(tz=timezone.utc),
        schemaDrift=SchemaDriftFailure(
            priorFingerprint=prior_fingerprint,  # type: ignore[arg-type]
            currentFingerprint=current_fingerprint,
            pinnedAt=profile.pinned_at,
            datasetDeltas=dataset_deltas,
            affectedVariationPoints=affected_vps,
        ),
    )
    diagnostic_path = write_schema_drift_diagnostic(workdir, run_id, artifact)
    summary = _build_handoff_message(
        run_id=run_id,
        prior=prior_fingerprint,  # type: ignore[arg-type]
        current=current_fingerprint,
        affected_vps=affected_vps,
        diagnostic_path=diagnostic_path,
    )
    return PreflightOutcome(
        kind="drift",
        prior_fingerprint=prior_fingerprint,
        current_fingerprint=current_fingerprint,
        diagnostic_path=diagnostic_path,
        summary=summary + (f"\n{absent_note}" if absent_note else ""),
        absent_datasets=tuple(absent_ids),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_legacy_fingerprint(value: str | None) -> bool:
    """Detect a missing / placeholder / malformed pinned fingerprint.

    Three flavors all map to "no real pin" (legacy graceful degrade):

    1. ``None`` — the ``TenantProfile`` field is Optional, so
       schema change; legacy profiles parse without the field.
    2. ``sha256:placeholder-...`` — the existing
       ``examples/profiles/finance-default.yaml`` placeholder shape.
    3. Anything that doesn't match ``sha256:[0-9a-f]{64}$`` — defensive
       catch-all for malformed values.
    """
    if value is None:
        return True
    return not bool(_VALID_FINGERPRINT_RE.match(value))


def _emit_legacy_warn_once() -> None:
    global _LEGACY_WARN_EMITTED
    if _LEGACY_WARN_EMITTED:
        return
    logger.warning(
        "Bronze fingerprint drift gate skipped — tenant profile has no real "
        "bronze fingerprint pinned (legacy profile). "
        "Run `aidp-fusion-autopilot bootstrap --refresh` to pin a real "
        "fingerprint and enable drift detection from the next run on."
    )
    _LEGACY_WARN_EMITTED = True


def _reset_legacy_warn() -> None:
    """Test helper — reset the module-level flag between cases."""
    global _LEGACY_WARN_EMITTED
    _LEGACY_WARN_EMITTED = False


def _bronze_dataset_ids(pack: "ResolvedPack") -> list[str]:
    """Extract bronze dataset ids from the resolved pack.

    ``pack.bronze`` (per-file bronze nodes) is the source of
    truth; legacy ``pack.bronze_yaml`` retained transitionally for
    packs that haven't migrated to ``bronze/<id>.yaml`` per-file form.
    NEVER reads ``bundle.datasets`` (bundle.datasets is a legitimate
    subset of pack bronze sources; using it here would produce a
    different fingerprint than bootstrap pinned).
    """
    ids: list[str] = list(pack.bronze.keys())
    bronze = pack.bronze_yaml or {}
    for entry in bronze.get("datasets", []) or []:
        if isinstance(entry, dict) and "id" in entry:
            entry_id = str(entry["id"])
            if entry_id not in ids:
                ids.append(entry_id)
    return ids


def _compute_affected_variation_points(
    *,
    pack: "ResolvedPack",
    profile: "TenantProfile",
    observed: dict[str, list[Any]],
) -> list[AffectedVariationPoint]:
    """For each pinned VP in ``profile.resolved.*``, check whether
    the pinned candidate is still present on the live bronze
    observation.

    * ``columnAliases``: the pinned value IS a physical column name;
      check existence directly.
    * ``semanticVariants``: the pinned value is a candidate ``id``
      (e.g., ``cancelled_date``). Look up that candidate in
      ``pack.pack.semantic_variants[vp].candidates`` and check whether
      its ``detect.columnExists`` column is present on bronze. If the
      pinned id no longer exists in the pack (overlay removed it) or
      its detect column has vanished from bronze, surface
      ``stillExistsOnBronze=False`` so the operator sees the real
      drift signal.

    Returns a flat list — the medallion-author skill reads it for recovery
    context.
    """
    # Flatten observed columns across all datasets — the walker
    # checks per-dataset existence, but for the drift gate we only
    # need "does this column exist somewhere on bronze?"
    all_observed_columns: set[str] = set()
    for cols in observed.values():
        for col in cols:
            all_observed_columns.add(col.name.lower())

    semantic_variants = pack.pack.semantic_variants or {}

    result: list[AffectedVariationPoint] = []
    for vp_name, pinned in profile.resolved.column.items():
        result.append(
            AffectedVariationPoint(
                name=vp_name,
                kind="columnAliases",
                pinnedCandidate=pinned,
                stillExistsOnBronze=pinned.lower() in all_observed_columns,
            )
        )
    for vp_name, pinned in profile.resolved.semantic.items():
        # Resolve the pinned id → its detect column → existence on bronze.
        # If the pack no longer declares this VP or this candidate id (overlay
        # was removed / pack version changed), treat as drift — the
        # rendering path will fail later anyway, so flag it here for the
        # operator's recovery context.
        variant = semantic_variants.get(vp_name)
        detect_column: str | None = None
        if variant is not None:
            for cand in variant.candidates:
                if cand.id == pinned:
                    detect_column = cand.detect.column_exists
                    break
        still_exists = (
            detect_column is not None
            and detect_column.lower() in all_observed_columns
        )
        result.append(
            AffectedVariationPoint(
                name=vp_name,
                kind="semanticVariants",
                pinnedCandidate=pinned,
                stillExistsOnBronze=still_exists,
            )
        )
    return result


def _build_handoff_message(
    *,
    run_id: str,
    prior: str,
    current: str,
    affected_vps: list[AffectedVariationPoint],
    diagnostic_path: Path,
) -> str:
    """Build the multi-line schema-drift hand-off message for stderr."""
    affected_lines = [
        f"      - {vp.name} (pinned '{vp.pinned_candidate}' "
        f"{'still exists on bronze' if vp.still_exists_on_bronze else 'NO LONGER EXISTS on bronze'})"
        for vp in affected_vps
    ]
    affected_block = (
        "\n".join(affected_lines)
        if affected_lines
        else "      (no pinned variation points to inspect)"
    )
    return (
        f"✗ AIDPF-2012  bronze schema fingerprint drift detected\n"
        f"    Prior fingerprint (pinned at bootstrap):   {prior}\n"
        f"    Current fingerprint:                       {current}\n"
        f"    Affected variation points:\n{affected_block}\n"
        f"\n"
        f"    To recover, run:\n"
        f"      aidp-fusion-autopilot bootstrap --refresh\n"
        f"\n"
        f"    If --refresh cannot resolve mechanically (Tier 1), open\n"
        f"    Claude Code in this project and ask the\n"
        f"    aidp-fusion-medallion-author skill to draft an overlay.\n"
        f"\n"
        f"    Diagnostic artifact: {diagnostic_path}\n"
        f"    Documentation:       workflow.md and docs/aidpf-error-codes.md\n"
        f"    run_id:              {run_id}"
    )


def _load_snapshot_if_present(
    *,
    bundle_path: Path,
    profile_name: str,
    profile: "TenantProfile",
) -> BronzeSchemaSnapshotV1 | None:
    """Read the pinned bronze schema snapshot for the active profile.

    Returns ``None`` (with a WARN log) when ANY of:

    * The snapshot file is absent. A pre-3d profile, or a partial-failure
      state where the snapshot write failed. Operator's remediation:
      ``bootstrap --refresh`` repins both the profile and the snapshot.
    * The file fails to parse. Hand-edit corruption — same remediation.
    * The snapshot's metadata ``bronze_schema_fingerprint`` differs from
      a recomputed fingerprint over its own ``datasets`` list. Indicates
      the dataset/column lists were hand-edited but the fingerprint field
      stayed stale. Operator should ``--refresh`` so the metadata
      catches up.
    * The snapshot's metadata fingerprint differs from
      ``profile.bronze_schema_fingerprint``. Profile/snapshot desync —
      e.g. snapshot remained from an older pin while the profile was
      rewritten by hand.

    In all four cases the drift signal still reaches the artifact —
    only ``datasetDeltas`` stays empty.
    The function NEVER raises; it's a defensive helper.

    Args:
        bundle_path: path to ``bundle.yaml`` — drives the snapshot
            file location via
            :func:`schema.bronze_schema_snapshot.resolve_snapshot_path`.
        profile_name: the **active profile name** — same key bootstrap
            writes the snapshot under (``bundle.contentPack.profile or
            .name``). NOT ``profile.tenant``: a hand-authored pre-3d
            profile YAML's ``tenant:`` value may differ from the
            active profile name, and using the YAML field as the
            path key would silently look in the wrong place after a
            healthy back-fill.
        profile: loaded ``TenantProfile``;
            ``bronze_schema_fingerprint`` is the cross-check anchor
            against the snapshot's own metadata fingerprint.

    Returns:
        ``BronzeSchemaSnapshotV1`` when healthy; ``None`` otherwise.
    """
    snapshot_path = resolve_snapshot_path(bundle_path, profile_name)
    if not snapshot_path.exists():
        logger.warning(
            "Pinned schema snapshot absent — datasetDeltas will be empty in "
            "the AIDPF-2012 artifact. Run `aidp-fusion-autopilot bootstrap "
            "--refresh` to pin the snapshot. Expected at: %s",
            snapshot_path,
        )
        return None

    try:
        snapshot = load_bronze_schema_snapshot(snapshot_path)
    except BronzeSchemaSnapshotSchemaError as exc:
        logger.warning(
            "Pinned schema snapshot unparseable — datasetDeltas will be empty "
            "in the AIDPF-2012 artifact. Run `aidp-fusion-autopilot "
            "bootstrap --refresh` to repin. Reason: %s",
            exc,
        )
        return None

    # Content cross-check: recompute over the snapshot's datasets and
    # confirm it matches the snapshot's own metadata fingerprint. A
    # mismatch means the dataset list was hand-edited but the metadata
    # field wasn't updated to match — we can't trust the diff key set.
    recomputed = compute_bronze_fingerprint(
        observed=snapshot_to_observed(snapshot)
    )
    if recomputed != snapshot.bronze_schema_fingerprint:
        logger.warning(
            "Pinned schema snapshot content/metadata fingerprint desync at "
            "%s — datasetDeltas will be empty in the AIDPF-2012 artifact. "
            "Run `aidp-fusion-autopilot bootstrap --refresh` to repin "
            "atomically.",
            snapshot_path,
        )
        return None

    # Profile cross-check: snapshot's pinned fingerprint must match the
    # profile's pinned fingerprint. If they differ the snapshot is from
    # a different pin than the profile — the diff would compare against
    # a schema the profile never saw.
    if snapshot.bronze_schema_fingerprint != profile.bronze_schema_fingerprint:
        logger.warning(
            "Pinned schema snapshot fingerprint %s differs from profile "
            "fingerprint %s at %s — datasetDeltas will be empty in the "
            "AIDPF-2012 artifact. Run `aidp-fusion-autopilot bootstrap "
            "--refresh` to repin atomically.",
            snapshot.bronze_schema_fingerprint[:24] + "...",
            (profile.bronze_schema_fingerprint or "<unset>")[:24] + "...",
            snapshot_path,
        )
        return None

    return snapshot


def _compute_dataset_deltas(
    *,
    snapshot: BronzeSchemaSnapshotV1,
    observed: dict[str, list[ColumnInfo]],
) -> list[DatasetSchemaDelta]:
    """Diff the pinned snapshot against the live observation.

    One :class:`DatasetSchemaDelta` per dataset in the union of
    snapshot.datasets and observed.keys() — datasets only present in
    the snapshot surface every column as ``removedColumns``; datasets
    only present in observed surface every column as ``addedColumns``.

    Canonical diff key MIRRORS
    :func:`schema.bronze_fingerprint.compute_bronze_fingerprint`
    exactly: ``name.strip().lower()`` for column matching,
    ``type.strip().lower()`` for type comparison. Original casing is
    preserved on the surfaced entries for operator display.

    Pure function — no Spark, no I/O. Returns an empty list when the
    snapshot and observed are byte-equivalent under the canonical key.
    """
    snapshot_by_dataset: dict[str, list[ColumnInfo]] = snapshot_to_observed(
        snapshot
    )
    all_dataset_ids = sorted(
        set(snapshot_by_dataset.keys()) | set(observed.keys())
    )

    deltas: list[DatasetSchemaDelta] = []
    for dataset_id in all_dataset_ids:
        snapshot_cols = snapshot_by_dataset.get(dataset_id, [])
        observed_cols = observed.get(dataset_id, [])

        snapshot_index = _canonical_index(snapshot_cols)
        observed_index = _canonical_index(observed_cols)

        added: list[ObservedColumn] = []
        removed: list[ObservedColumn] = []
        type_changed: list[ColumnTypeChange] = []

        for key, obs_col in observed_index.items():
            if key not in snapshot_index:
                added.append(
                    ObservedColumn(name=obs_col.name, type=obs_col.type)
                )
                continue
            prior_col = snapshot_index[key]
            if prior_col.type.strip().lower() != obs_col.type.strip().lower():
                type_changed.append(
                    ColumnTypeChange(
                        name=obs_col.name,
                        priorType=prior_col.type,
                        currentType=obs_col.type,
                    )
                )

        for key, prior_col in snapshot_index.items():
            if key not in observed_index:
                removed.append(
                    ObservedColumn(name=prior_col.name, type=prior_col.type)
                )

        if added or removed or type_changed:
            deltas.append(
                DatasetSchemaDelta(
                    datasetId=dataset_id,
                    addedColumns=added,
                    removedColumns=removed,
                    typeChangedColumns=type_changed,
                )
            )

    return deltas


def _canonical_index(columns: list[ColumnInfo]) -> dict[str, ColumnInfo]:
    """Build a ``{canonical_name: ColumnInfo}`` map preserving the first
    occurrence of each canonical key.

    Mirrors the dedupe behaviour of
    :func:`schema.bronze_fingerprint._dedupe_by_name` so the diff walker
    sees the same effective column set the fingerprint hashed. Original
    casing on the returned :class:`ColumnInfo` is preserved for display.
    """
    out: dict[str, ColumnInfo] = {}
    for col in columns:
        key = col.name.strip().lower()
        if key in out:
            continue
        out[key] = col
    return out


__all__ = [
    "PreflightOutcome",
    "PreflightKind",
    "check_bronze_fingerprint_drift",
]
