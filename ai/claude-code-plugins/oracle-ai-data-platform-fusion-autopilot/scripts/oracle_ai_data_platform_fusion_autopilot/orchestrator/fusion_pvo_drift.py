"""Live Fusion PVO schema drift gate.

Before bronze extraction starts, probe the live Fusion PVO schemas via
metadata-only ``extract_pvo(...).schema`` (BICC ``inferSchema`` round-trip;
no row transfer) and compare against:

* The downstream silver/gold ``requiredColumns`` /
  ``watermark.column`` union — proves the live PVO still surfaces the
  columns the pack expects.
* The pinned per-dataset snapshot at
  ``profiles/<tenant>.schema-snapshot.yaml`` — catches incompatible
  **type** narrowing AND surfaces paired missing+extra columns as renames.

When the snapshot is absent (legacy tenant), the gate degrades to
missing-column / renamed-column detection only and logs a one-time
WARN recommending ``bootstrap --refresh``.

Extra source columns are ALWAYS permitted — bronze seed adopts via
``overwriteSchema=true``; incremental MERGE handles column-add semantics
upstream.

The gate raises :class:`FusionPvoDriftError` (AIDPF-2072) with a
consolidated, operator-actionable message. The dispatcher catches it
and translates to a synthetic gate-failure :class:`RunStep`.

Snapshot models live in ``schema/bronze_schema_snapshot.py``.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .bronze_readiness import _REMEDIATION
from .errors import OrchestratorConfigError

if TYPE_CHECKING:  # pragma: no cover
    from ..schema.bronze_schema_snapshot import BronzeSchemaSnapshotV1
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AIDPF error code
# ---------------------------------------------------------------------------

AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED = "AIDPF-2072"
"""Live Fusion PVO schema drifted from what the pack + pinned snapshot
expect.

Single consolidated code; the diagnostic JSON written under
``.aidp/diagnostics/<run_id>/AIDPF-2072.json`` discriminates per-PVO
sub-errors (missing-column / type-changed / renamed-column).
"""


class FusionPvoDriftError(OrchestratorConfigError):
    """Live Fusion PVO drifted from pack + snapshot expectations (AIDPF-2072)."""

    def __init__(self, *, message: str, gaps: dict[str, dict[str, Any]]) -> None:
        super().__init__(message)
        self.gaps = gaps


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _required_columns_union(
    in_scope_silver_gold_nodes: list[Any],
    resolved_pack: "ResolvedPack",
    tenant_profile: "TenantProfile | None",
) -> dict[str, set[str]]:
    """Same algorithm as ``bronze_readiness._compute_required_columns``
    but re-implemented locally to keep the two gates independent and
    drift-resistant.

    Walks ``dependsOn.silver`` transitively so a gold node depending on
    a silver dim picks up the dim's bronze deps too.

    ``$column.<key>`` references in ``requiredColumns`` are resolved
    against the pack's ``columnAliases`` + the tenant profile's
    ``resolved.column`` map BEFORE union. Without this, the live-PVO
    diff at :func:`assert_fusion_pvo_compatibility` compares literal
    alias-reference strings against the physical Fusion column names
    and false-fails any pack that uses the alias syntax (the shipped
    starter pack does).
    """
    from .required_column_resolver import resolve_required_column_entries

    required_columns: dict[str, set[str]] = defaultdict(set)
    visited_silver: set[str] = set()

    def _add_node(node: Any) -> None:
        node_required = getattr(node, "required_columns", None) or {}
        for src_id, cols in node_required.items():
            required_columns[src_id].update(
                resolve_required_column_entries(
                    cols,
                    resolved_pack=resolved_pack,
                    tenant_profile=tenant_profile,
                )
            )
        inc = node.refresh.incremental if node.refresh else None
        if inc is not None and inc.watermark is not None:
            wm_source = inc.watermark.source
            wm_col = inc.watermark.column
            if wm_source and wm_col:
                required_columns[wm_source].add(wm_col)
        for silver_src in node.depends_on.silver:
            if silver_src.id in visited_silver:
                continue
            visited_silver.add(silver_src.id)
            silver_node = resolved_pack.silver.get(silver_src.id)
            if silver_node is not None:
                _add_node(silver_node)

    for node in in_scope_silver_gold_nodes:
        _add_node(node)
    return dict(required_columns)


def _snapshot_columns_by_dataset(
    snapshot: "BronzeSchemaSnapshotV1 | None",
) -> dict[str, dict[str, str]]:
    """``snapshot.datasets[*]`` → ``{dataset_id: {col_name_lower: type_lower}}``.

    Empty dict when ``snapshot is None`` (degraded mode).

    Defensive: drops :data:`BRONZE_AUDIT_COLUMNS` even when present in
    the snapshot. The fix at :mod:`schema.bronze_fingerprint` /
    :func:`schema.bronze_schema_snapshot.from_observed` prevents future
    writes from including audit columns, but pre-fix snapshots on disk
    (pinned before this code shipped) still have them. This filter
    lets the gate compare apples-to-apples against live BICC
    `inferSchema` (which never has audit columns) without forcing every
    tenant to re-bootstrap before the next run. No-op for clean
    snapshots.
    """
    from ..schema.bronze_fingerprint import BRONZE_AUDIT_COLUMNS

    if snapshot is None:
        return {}
    out: dict[str, dict[str, str]] = {}
    for ds in snapshot.datasets:
        out[ds.dataset_id] = {
            col.name.lower(): col.type.lower()
            for col in ds.columns
            if col.name.strip().lower() not in BRONZE_AUDIT_COLUMNS
        }
    return out


def _types_compatible(snapshot_type: str, live_type: str) -> bool:
    """Live type must be a non-narrowing equivalent of the snapshot type.

    Conservative rule: exact match after lower-case + whitespace trim.
    Wider equivalences (``int`` → ``bigint``, ``double`` → ``decimal``)
    are intentionally treated as drift in v0.3 — operators should opt
    in via ``bootstrap --refresh`` rather than have the gate silently
    accept widening that might still confuse downstream SQL casts.
    Future versions can relax this.
    """
    return snapshot_type.strip().lower() == live_type.strip().lower()


def assert_fusion_pvo_compatibility(
    *,
    live_pvo_columns: dict[str, dict[str, str]],
    resolved_pack: "ResolvedPack | None",
    cp_filter: tuple[list[str] | None, list[str] | None] | None,
    bronze_filter: tuple[list[str] | None, list[str] | None],
    schema_snapshot: "BronzeSchemaSnapshotV1 | None",
    run_id: str,
    diagnostics_root: Path | None = None,
    tenant_profile: "TenantProfile | None" = None,
) -> None:
    """Compare live Fusion PVO schemas against pack + snapshot expectations.

    Args:
        live_pvo_columns: per-PVO live schema as ``{dataset_id:
            {column_name_lower: type_string}}``. Produced upstream by
            the caller from ``extract_pvo(...).schema`` (a Spark
            ``StructType``). Lower-cased keys to match snapshot
            comparison semantics.
        resolved_pack: loaded content pack — used to look up
            ``requiredColumns`` / ``watermark.column`` per in-scope
            silver/gold node. ``None`` when there's no pack scope
            (bronze-only run); in that case only the snapshot-based
            checks fire.
        cp_filter: ``RunScope.cp_filter`` — narrows the silver/gold
            scope. ``None`` means "no silver/gold work" → required-
            columns check skipped.
        bronze_filter: ``RunScope.bronze_filter`` — narrows which
            bronze ids the caller actually intends to extract. The
            gate only complains about PVOs in this filter (an out-of-
            scope PVO's drift is for a future run to discover).
        schema_snapshot: pinned snapshot. ``None`` for tenants without a
            snapshot — gate degrades to required-column-only mode
            (type-change detection requires the snapshot's types).
        run_id: shared run identifier for the diagnostic path.
        diagnostics_root: override for tests; defaults to
            ``./.aidp/diagnostics``.

    Raises:
        FusionPvoDriftError: AIDPF-2072. ``gaps`` carries per-PVO
            ``missing_columns`` / ``type_changes`` / ``renames``.
    """
    in_scope_bronze: set[str] = _bronze_in_scope(bronze_filter, live_pvo_columns)
    if not in_scope_bronze:
        return  # No bronze in scope → gate is trivially OK.

    # Required columns union (from in-scope silver/gold nodes).
    required: dict[str, set[str]] = {}
    if resolved_pack is not None and cp_filter is not None:
        in_scope_nodes = _resolve_in_scope_silver_gold(resolved_pack, cp_filter)
        required = _required_columns_union(
            in_scope_nodes, resolved_pack, tenant_profile,
        )

    # Snapshot columns by dataset.
    snapshot_by_ds = _snapshot_columns_by_dataset(schema_snapshot)

    if schema_snapshot is None:
        logger.warning(
            "Fusion PVO drift gate: pinned schema snapshot is absent; "
            "degrading to missing-column / renamed-column "
            "detection. Run `aidp-fusion-autopilot bootstrap --refresh` "
            "to pin the snapshot and enable type-change detection."
        )

    gaps: dict[str, dict[str, Any]] = {}
    for bronze_id in sorted(in_scope_bronze):
        live_cols_map = live_pvo_columns.get(bronze_id, {})
        live_cols_lower = {name.lower() for name in live_cols_map.keys()}

        # Required column union from downstream nodes.
        required_cols = required.get(bronze_id, set())
        missing_required = sorted(
            col for col in required_cols if col.lower() not in live_cols_lower
        )

        # Snapshot-based checks (only when snapshot present).
        snapshot_cols_map = snapshot_by_ds.get(bronze_id, {})

        # Missing columns vs snapshot (pinned col absent from live).
        snapshot_only = sorted(
            col_lower for col_lower in snapshot_cols_map
            if col_lower not in live_cols_lower
        )
        # Extra columns in live vs snapshot.
        live_only = sorted(
            col_lower for col_lower in live_cols_lower
            if col_lower not in snapshot_cols_map and snapshot_cols_map
        )

        # Rename detection — pairs of (snapshot_only, live_only) at
        # the same dataset surface as candidate renames. Surface them
        # informationally; operators decide via `/medallion-author`.
        renames: list[dict[str, str]] = []
        if snapshot_only and live_only and snapshot_cols_map:
            for s, l in zip(snapshot_only, live_only):
                renames.append({"snapshot": s, "live": l})

        # Type changes — for columns present in both snapshot AND live,
        # compare types. Live types come as Spark type strings (e.g.
        # ``StringType``, ``DecimalType(18,2)``); snapshot types are
        # the same strings pinned at bootstrap. Mismatch is drift.
        type_changes: list[dict[str, str]] = []
        for col_lower, snap_type in snapshot_cols_map.items():
            if col_lower not in live_cols_lower:
                continue
            # Recover the original-case live name to look up the type.
            live_type = None
            for live_name, live_t in live_pvo_columns.get(bronze_id, {}).items():
                if live_name.lower() == col_lower:
                    live_type = live_t
                    break
            if live_type is None:
                continue
            if not _types_compatible(snap_type, live_type):
                type_changes.append({
                    "column": col_lower,
                    "snapshot_type": snap_type,
                    "live_type": live_type,
                })

        node_gap: dict[str, Any] = {}
        if missing_required:
            node_gap["missing_required_columns"] = missing_required
        # Only surface snapshot-missing when snapshot is present (degraded
        # mode skips this branch).
        if snapshot_only and not renames:
            node_gap["snapshot_columns_missing_from_live"] = snapshot_only
        if renames:
            node_gap["candidate_renames"] = renames
        if type_changes:
            node_gap["type_changes"] = type_changes

        if node_gap:
            gaps[bronze_id] = node_gap

    if not gaps:
        return

    # Compose the consolidated message.
    lines = [
        f"{AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED}: live Fusion PVO "
        f"schemas drifted for: {sorted(gaps.keys())!r}.",
    ]
    for bronze_id, gap in sorted(gaps.items()):
        if "missing_required_columns" in gap:
            lines.append(
                f"  - {bronze_id}: live PVO is missing required "
                f"columns {gap['missing_required_columns']!r}."
            )
        if "snapshot_columns_missing_from_live" in gap:
            lines.append(
                f"  - {bronze_id}: snapshot-pinned columns absent from "
                f"live: {gap['snapshot_columns_missing_from_live']!r}."
            )
        if "candidate_renames" in gap:
            lines.append(
                f"  - {bronze_id}: candidate renames "
                f"{gap['candidate_renames']!r}."
            )
        if "type_changes" in gap:
            lines.append(
                f"  - {bronze_id}: type changes "
                f"{gap['type_changes']!r}."
            )
    lines.append("")
    lines.append(_REMEDIATION)
    message = "\n".join(lines)

    _write_diagnostic(
        run_id=run_id,
        gaps=gaps,
        message=message,
        diagnostics_root=diagnostics_root,
    )

    raise FusionPvoDriftError(message=message, gaps=gaps)


def _bronze_in_scope(
    bronze_filter: tuple[list[str] | None, list[str] | None],
    live_pvo_columns: dict[str, dict[str, str]],
) -> set[str]:
    """Narrow ``live_pvo_columns`` keys to the bronze_filter scope.

    A filter with explicit datasets restricts to those ids. A filter
    with ``--layers bronze`` (and no datasets) accepts every probed
    bronze id (matches "extract bronze" semantics).
    """
    datasets, _layers = bronze_filter
    if datasets:
        ds_set = set(datasets)
        return {ds for ds in live_pvo_columns.keys() if ds in ds_set}
    return set(live_pvo_columns.keys())


def _resolve_in_scope_silver_gold(
    resolved_pack: "ResolvedPack",
    cp_filter: tuple[list[str] | None, list[str] | None],
) -> list[Any]:
    """Same logic as ``bronze_readiness._resolve_in_scope_nodes``."""
    datasets, layers = cp_filter
    layer_set = set(layers) if layers else {"silver", "gold"}
    candidate_nodes: list[Any] = []
    if "silver" in layer_set:
        candidate_nodes.extend(resolved_pack.silver.values())
    if "gold" in layer_set:
        candidate_nodes.extend(resolved_pack.gold.values())
    if datasets:
        ds_set = set(datasets)
        candidate_nodes = [n for n in candidate_nodes if n.id in ds_set]
    return candidate_nodes


def _write_diagnostic(
    *,
    run_id: str,
    gaps: dict[str, dict[str, Any]],
    message: str,
    diagnostics_root: Path | None,
) -> None:
    """Write the structured AIDPF-2072 diagnostic JSON (best-effort)."""
    if diagnostics_root is None:
        diagnostics_root = Path(".aidp") / "diagnostics"
    target_dir = diagnostics_root / run_id
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "AIDPF-2072.json"
        target_path.write_text(
            json.dumps(
                {
                    "code": AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED,
                    "run_id": run_id,
                    "gaps": gaps,
                    "remediation": _REMEDIATION,
                    "message": message,
                },
                indent=2, sort_keys=True,
            ),
            encoding="utf-8",
        )
    except OSError:
        pass


__all__ = [
    "AIDPF_2072_FUSION_PVO_DRIFT_GATE_FAILED",
    "FusionPvoDriftError",
    "assert_fusion_pvo_compatibility",
]
