"""The ONE Tier-B COA probe definition, shared by the gate and the verifier.

The COA safety argument is *"a derived arm is verified by the same probe that
caught the bad singleton mapping"* — which is only true if there is literally
one implementation of the probe SQL. This module is that implementation,
extracted behaviour-preservingly from ``node_preflight._evaluate_coa``:

* step 4's active-count query (:func:`chart_active_sql` /
  :func:`coa_chart_active`) — the ``active_row_count`` source, and
* step 5's per-chart Tier-B aggregate (:func:`natural_account_probe_sql` /
  :func:`probe_chart`) building :class:`coa_gate.ChartProbe`.

``node_preflight`` delegates here for the runtime gate; the bootstrap-time
metadata-arm verifier calls the same functions.
``tests/unit/test_coa_probe_sql_identity.py`` pins that both callers produce
byte-identical SQL for the same inputs.

Pure vs I/O split (testability contract, mirroring ``coa_gate``):
the ``*_sql`` builders and :func:`chart_probe_from_row` are pure string/value
functions; only :func:`coa_chart_active` / :func:`probe_chart` /
:func:`probe_charts` touch Spark, and each runs exactly one query. Verdicts
are NEVER decided here — that is :mod:`coa_gate`'s job.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping

from . import coa_gate

if TYPE_CHECKING:  # pragma: no cover — typing only
    from pyspark.sql import SparkSession

# Physical gl_coa column names (canonical home; ``node_preflight`` re-imports
# these under its historical private aliases). ``sql_renderer`` duplicates the
# discriminant deliberately — see its `_COA_DISCRIMINANT_COLUMN` comment.
COA_DISCRIMINANT = "CodeCombinationChartOfAccountsId"
COA_ACCOUNT_TYPE = "CodeCombinationAccountType"
COA_ENABLED_FLAG = "CodeCombinationEnabledFlag"


# ── pure SQL builders ────────────────────────────────────────────────────────


def chart_active_sql(
    table: str,
    *,
    discriminant: str = COA_DISCRIMINANT,
    enabled_flag_col: str = COA_ENABLED_FLAG,
) -> str:
    """Active (enabled) row count per chart — byte-identical to the gate's
    historical step-4 query."""
    return (
        f"SELECT CAST({discriminant} AS STRING) AS chart_id, COUNT(*) AS n "
        f"FROM {table} "
        f"WHERE {discriminant} IS NOT NULL "
        f"AND COALESCE({enabled_flag_col}, 'Y') <> 'N' "
        f"GROUP BY CAST({discriminant} AS STRING)"
    )


def natural_account_probe_sql(
    table: str,
    chart_id: str,
    na_col: str,
    *,
    discriminant: str = COA_DISCRIMINANT,
    account_type_col: str = COA_ACCOUNT_TYPE,
    enabled_flag_col: str = COA_ENABLED_FLAG,
) -> str:
    """Tier-B natural-account determinism aggregate for ONE chart —
    byte-identical to the gate's historical step-5 query.

    Callers own identifier safety: the gate runs this only after the
    AIDPF-5001 allowlist passed (``na_col`` is a plain column name) and
    ``chart_id`` matched ``_COA_CHART_ID_RE`` (numeric) at the structural
    gate; the verifier feeds the same validated shapes.
    """
    return (
        f"SELECT "
        f"COUNT(*) AS total, "
        f"SUM(CASE WHEN t > 1 THEN 1 ELSE 0 END) AS ambiguous "
        f"FROM (SELECT {na_col} AS na, "
        f"COUNT(DISTINCT {account_type_col}) AS t "
        f"FROM {table} "
        f"WHERE CAST({discriminant} AS STRING) = '{chart_id}' "
        f"AND {na_col} IS NOT NULL "
        f"AND COALESCE({enabled_flag_col}, 'Y') <> 'N' "
        f"GROUP BY {na_col})"
    )


def chart_probe_from_row(
    chart_id: str, active_rows: int, row: tuple | list
) -> coa_gate.ChartProbe:
    """Build the gate's :class:`coa_gate.ChartProbe` from the aggregate row
    ``(total, ambiguous)`` — the same null-coalescing the gate historically
    applied inline."""
    total = int(row[0] or 0)
    ambiguous = int(row[1] or 0)
    return coa_gate.ChartProbe(
        chart_id=chart_id,
        active_row_count=active_rows,
        natural_account_distinct=total,
        natural_account_ambiguous=ambiguous,
    )


# ── I/O (one query per call; verdicts stay in coa_gate) ─────────────────────


def coa_chart_active(spark: "SparkSession", table: str) -> dict[str, int]:
    """Active (enabled) gl_coa row count per chart_of_accounts_id."""
    rows = spark.sql(chart_active_sql(table)).collect()
    return {str(r[0]): int(r[1]) for r in rows if r[0] is not None}


def probe_chart(
    spark: "SparkSession",
    table: str,
    *,
    chart_id: str,
    na_col: str,
    active_rows: int,
) -> coa_gate.ChartProbe | None:
    """Run the Tier-B aggregate for one chart and build its ``ChartProbe``.

    Returns ``None`` when the aggregate yields no rows (nothing to judge).
    Raises whatever Spark raises — the CALLER owns fail-soft disposition
    (the gate records a ``probe_failure``; the verifier marks the arm
    ``UNVERIFIED``), so failure semantics stay at the call site.
    """
    agg = spark.sql(natural_account_probe_sql(table, chart_id, na_col)).collect()
    if not agg:
        return None
    return chart_probe_from_row(chart_id, active_rows, agg[0])


def probe_charts(
    spark: "SparkSession",
    table: str,
    arms: Mapping[str, Mapping[str, str]],
    chart_active: Mapping[str, int],
    *,
    max_charts: int | None = None,
) -> dict[str, coa_gate.ChartProbe]:
    """Probe many charts (largest active-row counts first), fail-soft per
    chart: a chart whose query raises or yields nothing is simply absent from
    the result — the caller treats absence as UNVERIFIED, never as a pass."""
    probes: dict[str, coa_gate.ChartProbe] = {}
    ordered = sorted(chart_active.items(), key=lambda kv: (-kv[1], kv[0]))
    if max_charts is not None:
        ordered = ordered[:max_charts]
    for chart_id, active_rows in ordered:
        mapping = arms.get(chart_id) or arms.get("default") or {}
        na_col = mapping.get("coa.natural_account")
        if not na_col:
            continue
        try:
            probe = probe_chart(
                spark, table, chart_id=chart_id, na_col=na_col,
                active_rows=active_rows,
            )
        except Exception:  # noqa: BLE001 — per-chart fail-soft by contract
            continue
        if probe is not None:
            probes[chart_id] = probe
    return probes


__all__ = [
    "COA_ACCOUNT_TYPE",
    "COA_DISCRIMINANT",
    "COA_ENABLED_FLAG",
    "chart_active_sql",
    "chart_probe_from_row",
    "coa_chart_active",
    "natural_account_probe_sql",
    "probe_chart",
    "probe_charts",
]
