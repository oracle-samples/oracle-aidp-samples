"""Pure COA plausibility + multi-COA gate logic (no Spark, no I/O).

This is the algorithm for the M2 preflight gates of
coa-role-segment-resolution. ``node_preflight`` runs the live ``gl_coa`` data
probes and feeds the measured values here; this module decides pass / warn /
fail-closed. Keeping it pure makes every tier a unit-testable input/output pair.

Gate structure (feature diagnostic §6f), all evaluated **per
chart_of_accounts_id** using that chart's own role mapping:

* **Tier A (structural, HARD)** -- existence of the union of referenced columns
  in landed ``gl_coa``; per-arm distinctness (three roles -> three distinct
  columns within each arm; cross-arm reuse is valid).
* **Tier B (strong contradiction, HARD with guards)** -- natural-account
  segment must functionally determine account_type. Measured per chart after
  filtering enabled + non-null, above a sample floor; below the floor -> warn.
* **Tier C (soft, warn)** -- cardinality/coverage shape.
* **Multi-COA gate** -- more than one chart with meaningful active data and no
  ``singletonAccepted`` / ``byChart`` -> fail closed.
"""

from __future__ import annotations

from dataclasses import dataclass, field

AIDPF_2042_REQUIRED_COLUMN_MISSING = "AIDPF-2042"
AIDPF_2016_COA_DUP_ROLE_COLUMN = "AIDPF-2016"
"""Two COA roles map to the same physical column within one chart's mapping."""
AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION = "AIDPF-2017"
"""The column bound as naturalAccountSegment does not classify into account
types (strong, sample-backed contradiction) -- likely the wrong segment."""
AIDPF_2018_MULTI_COA_UNCONFIGURED = "AIDPF-2018"
"""Multiple active charts of accounts but only a singleton mapping and no
operator acceptance / byChart -- fail closed."""

# Tunable thresholds (tier guards).
SAMPLE_FLOOR_DISTINCT = 20
"""Min distinct natural-account values for Tier B to hard-fail; below -> warn."""
SAMPLE_FLOOR_ROWS = 100
"""Min enabled rows for Tier B to hard-fail; below -> warn."""
CONTRADICTION_FAIL_FRACTION = 0.5
"""Fraction of natural-account values mapping to >1 account_type above which a
chart is a strong contradiction (hard fail)."""
CONTRADICTION_WARN_FRACTION = 0.2
"""Ambiguous fraction above which Tier B warns (below the fail threshold)."""
MULTI_COA_MIN_ACTIVE_ROWS = 1
"""A chart counts toward the multi-COA gate only with at least this many active
(enabled) rows -- legacy/inactive structures don't inflate the count."""


@dataclass(frozen=True)
class ChartProbe:
    """Measured ``gl_coa`` stats for one chart_of_accounts_id, computed with
    that chart's own role mapping."""

    chart_id: str
    active_row_count: int
    natural_account_distinct: int
    # Of the distinct natural-account values (enabled, non-null), how many map
    # to more than one account_type.
    natural_account_ambiguous: int
    balancing_distinct: int = 0
    cost_center_distinct: int = 0


@dataclass
class CoaGateResult:
    errors: list[tuple[str, str]] = field(default_factory=list)  # (code, message)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def check_existence_union(
    referenced_columns: set[str], present_columns: set[str]
) -> list[tuple[str, str]]:
    """Tier A existence: every referenced COA column (union of default + all
    byChart arms) must be present in landed ``gl_coa`` (case-insensitive)."""
    present_ci = {c.lower() for c in present_columns}
    errors: list[tuple[str, str]] = []
    for col in sorted(referenced_columns):
        if col.lower() not in present_ci:
            errors.append(
                (
                    AIDPF_2042_REQUIRED_COLUMN_MISSING,
                    f"COA-bound column {col!r} is not present in landed gl_coa. "
                    f"Extend the gl_coa bronze contract / re-seed bronze before "
                    f"binding this role.",
                )
            )
    return errors


def check_distinctness(arms: dict[str, dict[str, str]]) -> list[tuple[str, str]]:
    """Tier A distinctness: within EACH arm (default + each byChart chart), the
    three roles must map to three distinct columns. Cross-arm reuse is valid.

    ``arms`` maps arm-id -> {role_token: column}.
    """
    errors: list[tuple[str, str]] = []
    for arm_id, mapping in arms.items():
        cols = list(mapping.values())
        if len(set(c.lower() for c in cols)) != len(cols):
            errors.append(
                (
                    AIDPF_2016_COA_DUP_ROLE_COLUMN,
                    f"COA arm {arm_id!r} maps two roles to the same column "
                    f"({mapping!r}); each role must bind a distinct segment.",
                )
            )
    return errors


def check_natural_account(probe: ChartProbe) -> CoaGateResult:
    """Tier B + a Tier C cardinality nudge for one chart.

    Hard-fails (AIDPF-2017) only on a sample-backed strong contradiction;
    below the sample floor it downgrades to a warning.
    """
    res = CoaGateResult()
    n = probe.natural_account_distinct
    if n == 0:
        return res
    ambiguous_fraction = probe.natural_account_ambiguous / n
    below_floor = (
        n < SAMPLE_FLOOR_DISTINCT or probe.active_row_count < SAMPLE_FLOOR_ROWS
    )
    if ambiguous_fraction >= CONTRADICTION_FAIL_FRACTION and not below_floor:
        res.errors.append(
            (
                AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION,
                f"chart {probe.chart_id!r}: the column bound as naturalAccountSegment "
                f"does not classify into account types "
                f"({probe.natural_account_ambiguous}/{n} values span multiple "
                f"account_types). You likely bound the wrong segment.",
            )
        )
    elif ambiguous_fraction >= CONTRADICTION_WARN_FRACTION:
        reason = "below sample floor" if below_floor else "ambiguous"
        res.warnings.append(
            f"chart {probe.chart_id!r}: naturalAccountSegment determinism is weak "
            f"({probe.natural_account_ambiguous}/{n} values multi-typed; {reason}). "
            f"Verify the COA role mapping."
        )
    return res


def check_multi_coa(
    chart_active_rows: dict[str, int],
    *,
    singleton_accepted: bool,
    has_by_chart: bool,
) -> list[tuple[str, str]]:
    """Multi-COA gate: more than one chart with active data, and neither a
    persisted ``singletonAccepted`` opt-in nor a ``byChart`` mapping -> fail
    closed (AIDPF-2018). Charts below the active-row floor don't count."""
    active_charts = [
        cid
        for cid, rows in chart_active_rows.items()
        if rows >= MULTI_COA_MIN_ACTIVE_ROWS
    ]
    if len(active_charts) <= 1:
        return []
    if singleton_accepted or has_by_chart:
        return []
    return [
        (
            AIDPF_2018_MULTI_COA_UNCONFIGURED,
            f"gl_coa has {len(active_charts)} active charts of accounts "
            f"({sorted(active_charts)!r}) but the profile has only a singleton COA "
            f"mapping. A single role->segment layout cannot be correct for all "
            f"charts. Declare `profile.chartOfAccounts.byChart`, or accept a shared "
            f"layout via `bootstrap --accept-singleton-coa`.",
        )
    ]


__all__ = [
    "AIDPF_2016_COA_DUP_ROLE_COLUMN",
    "AIDPF_2017_COA_NATURAL_ACCOUNT_CONTRADICTION",
    "AIDPF_2018_MULTI_COA_UNCONFIGURED",
    "ChartProbe",
    "CoaGateResult",
    "check_distinctness",
    "check_existence_union",
    "check_multi_coa",
    "check_natural_account",
]
