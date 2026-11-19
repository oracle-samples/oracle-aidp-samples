"""Run-level bronze readiness preflight.

Before any silver/gold node runs, verify the in-scope bronze tables exist
and carry every column the downstream pack nodes declare in
``requiredColumns`` / ``watermark.column``.

Without this, partial or schema-drifted bronze (typo'd source column,
upstream Fusion column rename, BICC partial-success) would surface as
a raw ``AnalysisException`` mid-silver-execution — too late to write a
clean state row, too noisy for operators to diagnose.

The gate emits one consolidated ``BronzeReadinessGateError`` listing
ALL gaps (table-missing + column-missing across every in-scope node)
so a single ``aidp-fusion-autopilot run`` invocation surfaces every
fix-needed in one error rather than failing iteratively.

Schema source-of-truth:
``NodeYaml.required_columns`` and
``NodeYaml.refresh.incremental.watermark.{source, column}``.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .errors import OrchestratorConfigError

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

    from ..config.paths import TablePaths
    from ..schema.medallion_pack import NodeYaml
    from ..schema.tenant_profile import TenantProfile
    from .content_pack import ResolvedPack


# ---------------------------------------------------------------------------
# AIDPF error code
# ---------------------------------------------------------------------------

AIDPF_2071_BRONZE_READINESS_GATE_FAILED = "AIDPF-2071"
"""Bronze table or column missing for an in-scope silver/gold node.

The single consolidated code; the diagnostic JSON written under
``.aidp/diagnostics/<run_id>/AIDPF-2071.json`` discriminates per-
bronze-id sub-errors (missing-table vs missing-columns).
"""


class BronzeReadinessGateError(OrchestratorConfigError):
    """Bronze surface insufficient for in-scope silver/gold nodes (AIDPF-2071).

    Carries a structured ``gaps`` dict so the dispatcher can serialise
    it to the diagnostic JSON without re-deriving the layout.
    """

    def __init__(self, *, message: str, gaps: dict[str, dict[str, Any]]) -> None:
        super().__init__(message)
        self.gaps = gaps


# ---------------------------------------------------------------------------
# Remediation runbook (shared with AIDPF-2072 — same operator workflow)
# ---------------------------------------------------------------------------


_REMEDIATION = (
    "Step 1: re-run `aidp-fusion-autopilot bootstrap --refresh` to re-resolve "
    "variation points + refresh the pinned bronze schema snapshot. "
    "Step 2 (if Step 1 does not clear the gate): invoke the "
    "`/medallion-author` Claude Code skill to draft a content-pack overlay "
    "extending the candidate list with the live Fusion column names. "
    "See docs/workflow.md and docs/aidpf-error-codes.md for the current "
    "source-drift workflow."
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _resolve_in_scope_nodes(
    resolved_pack: "ResolvedPack",
    cp_filter: tuple[list[str] | None, list[str] | None],
) -> list["NodeYaml"]:
    """Apply ``cp_filter`` to the pack's silver+gold node sets."""
    datasets, layers = cp_filter
    layer_set = set(layers) if layers else {"silver", "gold"}
    candidate_nodes: list["NodeYaml"] = []
    if "silver" in layer_set:
        candidate_nodes.extend(resolved_pack.silver.values())
    if "gold" in layer_set:
        candidate_nodes.extend(resolved_pack.gold.values())
    if datasets:
        ds_set = set(datasets)
        candidate_nodes = [n for n in candidate_nodes if n.id in ds_set]
    return candidate_nodes


def _walk_transitive_bronze_deps(
    in_scope_nodes: list["NodeYaml"],
    resolved_pack: "ResolvedPack",
) -> set[str]:
    """Resolve the union of bronze ids every in-scope node touches.

    Walks ``depends_on.bronze`` directly AND walks
    ``depends_on.silver`` recursively — a gold node that depends on a
    silver dim still needs that dim's bronze sources (otherwise the
    silver dim's rebuild would crash, then gold).
    """
    required_bronze: set[str] = set()
    visited_silver: set[str] = set()

    def _visit(node: "NodeYaml") -> None:
        for src in node.depends_on.bronze:
            required_bronze.add(src.id)
        for silver_src in node.depends_on.silver:
            if silver_src.id in visited_silver:
                continue
            visited_silver.add(silver_src.id)
            silver_node = resolved_pack.silver.get(silver_src.id)
            if silver_node is not None:
                _visit(silver_node)

    for node in in_scope_nodes:
        _visit(node)

    return required_bronze


def _compute_required_columns(
    in_scope_nodes: list["NodeYaml"],
    resolved_pack: "ResolvedPack",
    tenant_profile: "TenantProfile | None",
) -> dict[str, set[str]]:
    """Union the per-bronze required-column declarations across every
    in-scope silver/gold node AND every silver dim they transitively
    depend on. Also folds in each node's ``watermark.column`` (when
    the watermark source matches the bronze id) — operators sometimes
    list joined cols in ``requiredColumns`` but forget the lineage col.

    ``$column.<key>`` references in ``requiredColumns`` are resolved
    against the pack's ``columnAliases`` + the tenant profile's
    ``resolved.column`` map BEFORE adding to the required set. Without
    this, the live-column comparison at :func:`assert_bronze_readiness`
    would compare literal alias-reference strings against physical
    Fusion column names and false-fail on any pack that uses the alias
    syntax (the shipped starter pack does).

    The watermark column is treated as a literal (not alias-resolved)
    because ``NodeYaml.refresh.incremental.watermark.column`` is
    schema-typed as a plain identifier in v0.3 — the alias substitution
    surface is ``requiredColumns`` only.
    """
    from .required_column_resolver import resolve_required_column_entries

    required_columns: dict[str, set[str]] = defaultdict(set)
    visited_silver: set[str] = set()

    def _add_node(node: "NodeYaml") -> None:
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
                # Only fold the watermark column into the required set
                # when the source IS a bronze id (silver→silver watermarks
                # exist but aren't in scope for this bronze gate).
                required_columns[wm_source].add(wm_col)

        for silver_src in node.depends_on.silver:
            if silver_src.id in visited_silver:
                continue
            visited_silver.add(silver_src.id)
            silver_node = resolved_pack.silver.get(silver_src.id)
            if silver_node is not None:
                _add_node(silver_node)

    for node in in_scope_nodes:
        _add_node(node)

    return dict(required_columns)


def _describe_bronze_columns(
    spark: "SparkSession",
    bronze_id: str,
    paths: "TablePaths",
) -> tuple[bool, set[str]]:
    """Return ``(exists, columns_lower)`` for one bronze table.

    Catches ``AnalysisException`` (and any subclass thereof) — more
    robust against catalog-binding edge cases than calling
    ``tableExists`` separately. When the DESCRIBE raises for ANY
    reason, the gate treats the table as missing.
    """
    fqn = paths.bronze(bronze_id)
    try:
        df = spark.sql(f"DESCRIBE TABLE {fqn}")
        rows = df.collect()
    except Exception:  # noqa: BLE001 — gate must catch every catalog-binding flavour
        return False, set()

    columns: set[str] = set()
    for r in rows:
        try:
            name = r["col_name"] if isinstance(r, dict) else r[0]
        except (KeyError, IndexError, TypeError):
            continue
        if not name or name.startswith("#"):
            break
        columns.add(str(name).lower())
    return True, columns


def assert_bronze_readiness(
    spark: "SparkSession",
    *,
    resolved_pack: "ResolvedPack",
    cp_filter: tuple[list[str] | None, list[str] | None],
    paths: "TablePaths",
    run_id: str,
    diagnostics_root: Path | None = None,
    tenant_profile: "TenantProfile | None" = None,
) -> None:
    """Verify every in-scope silver/gold node's bronze deps are landable.

    Raises :class:`BronzeReadinessGateError` (AIDPF-2071) iff any
    transitive bronze table is missing OR is missing a required
    column. Writes a structured diagnostic JSON to
    ``.aidp/diagnostics/<run_id>/AIDPF-2071.json`` listing every gap
    so operators can fix in bulk.

    Args:
        spark: live SparkSession.
        resolved_pack: the loaded content pack.
        cp_filter: ``(datasets, layers)`` filter for the content-pack
            scope (matches ``RunScope.cp_filter``).
        paths: TablePaths (for ``paths.bronze(<id>)``).
        run_id: shared run identifier used for the diagnostic path.
        diagnostics_root: override for diagnostics directory (mostly
            useful in tests). Defaults to ``./.aidp/diagnostics``.
        tenant_profile: loaded tenant profile, used to resolve
            ``$column.<key>`` references in ``requiredColumns``
            against the profile's ``resolved.column`` map. When
            ``None`` (legacy callers), ``$column.*`` entries fall
            through unresolved — per-node preflight will surface
            AIDPF-2046 as it always has. New callers MUST pass the
            tenant profile for AIDPF-2071 to compare resolved
            physical columns against the live bronze schema.

    Raises:
        BronzeReadinessGateError: AIDPF-2071. The exception's ``gaps``
            attribute carries the structured per-bronze-id diagnostic.
    """
    in_scope_nodes = _resolve_in_scope_nodes(resolved_pack, cp_filter)
    if not in_scope_nodes:
        return  # Nothing in scope → trivially ready.

    required_bronze_ids = _walk_transitive_bronze_deps(
        in_scope_nodes, resolved_pack
    )
    required_columns = _compute_required_columns(
        in_scope_nodes, resolved_pack, tenant_profile
    )

    gaps: dict[str, dict[str, Any]] = {}
    for bronze_id in sorted(required_bronze_ids):
        exists, actual_cols_lower = _describe_bronze_columns(
            spark, bronze_id, paths,
        )
        if not exists:
            gaps[bronze_id] = {"table_missing": True}
            continue
        required = required_columns.get(bronze_id, set())
        missing = sorted(
            col for col in required if col.lower() not in actual_cols_lower
        )
        if missing:
            gaps[bronze_id] = {"missing_columns": missing}

    if not gaps:
        return

    # Compose the human-readable message.
    lines = [
        f"{AIDPF_2071_BRONZE_READINESS_GATE_FAILED}: bronze readiness "
        f"gate failed for tables: {sorted(gaps.keys())!r}.",
    ]
    for bronze_id, gap in sorted(gaps.items()):
        if gap.get("table_missing"):
            lines.append(
                f"  - {bronze_id}: table missing (run "
                f"`aidp-fusion-autopilot run --layers bronze --datasets "
                f"{bronze_id}` first)."
            )
        elif "missing_columns" in gap:
            lines.append(
                f"  - {bronze_id}: missing columns "
                f"{gap['missing_columns']!r}."
            )
    lines.append("")
    lines.append(_REMEDIATION)
    message = "\n".join(lines)

    # Write the diagnostic JSON.
    _write_diagnostic(
        run_id=run_id,
        gaps=gaps,
        message=message,
        diagnostics_root=diagnostics_root,
    )

    raise BronzeReadinessGateError(message=message, gaps=gaps)


def _write_diagnostic(
    *,
    run_id: str,
    gaps: dict[str, dict[str, Any]],
    message: str,
    diagnostics_root: Path | None,
) -> None:
    """Write the structured AIDPF-2071 diagnostic JSON.

    Best-effort: if the directory isn't writeable (read-only filesystem,
    sandboxed environment) the gate still raises the exception — the
    JSON is for operator convenience, not the contract.
    """
    if diagnostics_root is None:
        diagnostics_root = Path(".aidp") / "diagnostics"
    target_dir = diagnostics_root / run_id
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "AIDPF-2071.json"
        target_path.write_text(
            json.dumps(
                {
                    "code": AIDPF_2071_BRONZE_READINESS_GATE_FAILED,
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
        # Diagnostic-write failure must NEVER swallow the gate.
        pass


__all__ = [
    "AIDPF_2071_BRONZE_READINESS_GATE_FAILED",
    "BronzeReadinessGateError",
    "assert_bronze_readiness",
]
