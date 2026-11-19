"""Hazard-replay verification harness.

A candidate pipeline repair is proven idempotent and replay-safe BEFORE a human sees it:
run it over an ordered baseline schedule, run the final batch twice, then run it under
late / duplicate / out_of_order / retried delivery schedules and diff every result against a
batch recompute (latest row per key). ArrivalBench found 7-79 percent of agent-written
pipelines that pass a single run are silently wrong under exactly these schedules, with
idempotency hazards dominating; this module is the local, deterministic version of that gate.

Two engines share the same vocabulary:

* local  - pandas DataFrames for the schedules, any Python callable or a duckdb SQL repair
           (`sql_apply`) as the thing under test; what the tests exercise.
* live   - `spark_sql_plan` emits the Spark SQL statement sequence for Oracle AI Data Platform
           (Delta) using only these features: SHALLOW CLONE, DESCRIBE HISTORY as a
           standalone statement, CTAS, temp views, window functions, EXCEPT ALL. The notebook
           cell in notebooks/replay_spark_template.py executes that plan with `spark.sql`.

Verdict contract:
    {'passes_snapshot': bool, 'passes_run_twice': bool, 'hazards': {name: bool},
     'diff_rows': int, 'verdict': 'replay_safe'|'silent_wrong'|'crash'}
Extensions: a candidate whose BASELINE result already differs from the recompute gets the
verdict 'snapshot_wrong'. Only the three values above are standard verdicts, but a loudly wrong
candidate must not be counted as 'silent_wrong' (that would corrupt the silent-wrong rate,
defined as candidates that pass snapshot but fail replay) and is not a crash. A candidate that
passes every check while a check exercised no row at all (a table too small to move, repeat or
revise anything) gets 'insufficient_coverage', never 'replay_safe': a hazard that selected
nothing proved nothing. The verdict dict carries `coverage` (rows each check exercised) and
`uncovered` (the checks that exercised none).

The revision schedule has two trailing deliveries: the corrections, then the superseded originals
of the same keys again. The second one is the case a blind last-write-wins upsert gets wrong (an
old row arriving after its correction); without it such a statement passes all five hazards.

No network or live calls happen here.
"""
from __future__ import annotations

import math
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ApplyFn = Callable[[pd.DataFrame | None, pd.DataFrame], pd.DataFrame]
RecomputeFn = Callable[[pd.DataFrame, list[str], str], pd.DataFrame]

HAZARDS: tuple[str, ...] = ("late", "duplicate", "out_of_order", "retried", "revision")
SCHEDULES: tuple[str, ...] = ("baseline",) + HAZARDS
DELIVERY_HAZARDS: tuple[str, ...] = ("late", "duplicate", "out_of_order", "retried")  # same rows, different arrival

VERDICT_REPLAY_SAFE = "replay_safe"
VERDICT_SILENT_WRONG = "silent_wrong"
VERDICT_SNAPSHOT_WRONG = "snapshot_wrong"  # extension, see module docstring
VERDICT_INSUFFICIENT = "insufficient_coverage"  # extension: every check passed, at least one exercised no row
VERDICT_CRASH = "crash"

# On a table this many times smaller than a 1-in-k sampling modulus the pmod flags can select nothing, so the first
# row is flagged as well (live plan only; the local schedules already pick at least one row per hazard).
SMALL_TABLE_FACTOR = 4

# Per-schedule stream offsets so each hazard draws from its own deterministic RNG stream.
_STREAM = {"late": 1, "duplicate": 2, "out_of_order": 3, "retried": 4, "revision": 5}

# Plan tag prefix: every emitted Spark statement starts with "-- ns:<stage>:<name>" so the
# notebook can route results (pin version, diff counts) and split cleanup from work.
PLAN_TAG_PREFIX = "-- ns:"


# --------------------------------------------------------------------------------------
# Configuration and schedules
# --------------------------------------------------------------------------------------
@dataclass
class HazardConfig:
    seed: int = 20261102
    late_fraction: float = 0.1
    dup_fraction: float = 0.1
    shuffle: bool = True
    retry_batches: int = 1
    n_batches: int = 4
    rev_fraction: float = 0.1          # share of rows re-delivered as revisions (revision hazard)
    revise_col: str | None = None      # the one payload column to change; None = every non-key, non-ordering column

    def __post_init__(self) -> None:
        if self.n_batches < 1:
            raise ValueError("n_batches must be >= 1")
        for name in ("late_fraction", "dup_fraction", "rev_fraction"):
            v = getattr(self, name)
            if not 0.0 <= v <= 1.0:
                raise ValueError(f"{name} must be within [0, 1], got {v}")
        if self.retry_batches < 0:
            raise ValueError("retry_batches must be >= 0")


def effective_batches(n_rows: int | None, cfg: HazardConfig) -> int:
    """Deliveries the baseline is split into: cfg.n_batches, lowered to the row count so that no batch is empty.
    An empty batch makes the retried, out_of_order and run-twice checks vacuous (they would repeat or reorder
    nothing). None means the row count is not known and cfg.n_batches is used as is."""
    if n_rows is None:
        return cfg.n_batches
    return max(1, min(cfg.n_batches, int(n_rows)))


def _rng(cfg: HazardConfig, stream: str) -> np.random.Generator:
    return np.random.default_rng([cfg.seed, _STREAM[stream]])


def _batch_permutation(n: int, cfg: HazardConfig) -> list[int]:
    """Deterministic non-identity batch order for the out_of_order schedule (0-based).

    shuffle=True draws a random permutation (rolled by one if it happens to be the identity);
    shuffle=False reverses the batch order, so the hazard is never vacuous when n > 1.
    """
    if n <= 1:
        return list(range(n))
    if not cfg.shuffle:
        return list(range(n))[::-1]
    perm = [int(i) for i in _rng(cfg, "out_of_order").permutation(n)]
    if perm == list(range(n)):
        perm = perm[1:] + perm[:1]
    return perm


def _retry_indices(n: int, cfg: HazardConfig) -> list[int]:
    """Deterministic set of batch indices (0-based, sorted) delivered twice consecutively."""
    k = min(cfg.retry_batches, n)
    if k == 0 or n == 0:
        return []
    return sorted(int(i) for i in _rng(cfg, "retried").choice(n, size=k, replace=False))


def _select_fraction(rng: np.random.Generator, eligible: np.ndarray, fraction: float) -> np.ndarray:
    if fraction <= 0 or len(eligible) == 0:
        return np.array([], dtype=int)
    k = max(1, int(round(fraction * len(eligible))))
    return np.sort(rng.choice(eligible, size=k, replace=False))


def _ordered(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    missing = [c for c in [ts_col, *key_cols] if c not in df.columns]
    if missing:
        raise KeyError(f"columns missing from frame: {missing}")
    return df.sort_values([ts_col, *key_cols], kind="mergesort").reset_index(drop=True)


def pick_revise_col(columns: Sequence[str], key_cols: Sequence[str], ts_col: str,
                    preferred: str | None = None) -> str | None:
    """The payload column a revision changes: `preferred` when given (must exist and not be a key or
    the ordering column), else the first column that is neither. None when the table is keys + ts only;
    a revision then changes only the ordering column."""
    if preferred is not None:
        if preferred not in columns:
            raise KeyError(f"revise_col {preferred!r} is not a column")
        if preferred in key_cols or preferred == ts_col:
            raise ValueError(f"revise_col {preferred!r} must not be a key or the ordering column")
        return preferred
    for c in columns:
        if c not in key_cols and c != ts_col:
            return c
    return None


def _shift_series(s: pd.Series) -> pd.Series:
    """Move a series 'later': +1 hour for datetimes, +1 for numbers (used for the ordering column)."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s + pd.Timedelta(hours=1)
    if pd.api.types.is_numeric_dtype(s):
        return s + 1
    raise TypeError(f"ordering column {s.name!r} must be datetime or numeric for the revision hazard")


def _mutate_series(s: pd.Series) -> pd.Series:
    """A visibly different value of the same type: numbers +1, booleans flipped, datetimes +1 day,
    everything else gets a '_rev' suffix as text."""
    if pd.api.types.is_bool_dtype(s):
        return ~s.astype(bool)
    if pd.api.types.is_datetime64_any_dtype(s):
        return s + pd.Timedelta(days=1)
    if pd.api.types.is_numeric_dtype(s):
        return s + 1
    return s.astype(str) + "_rev"


def _revision_rows(df_ordered: pd.DataFrame, ts_col: str, cfg: HazardConfig) -> np.ndarray:
    """Positions of the rows the revision hazard corrects (deterministic for a seed). Only a row whose ordering value
    can be moved strictly later is eligible: a NULL ordering value, or a float so large that +1 does not change it,
    cannot carry a correction the recompute would prefer, and counting it would make the check look exercised."""
    if len(df_ordered) == 0:
        return np.array([], dtype=int)
    ts = df_ordered[ts_col]
    eligible = np.flatnonzero((_shift_series(ts) > ts).to_numpy(dtype=bool, na_value=False))
    return _select_fraction(_rng(cfg, "revision"), eligible, cfg.rev_fraction)


def make_stale_batch(df_ordered: pd.DataFrame, ts_col: str, cfg: HazardConfig) -> pd.DataFrame:
    """The superseded originals of the revised keys, unchanged, for delivery AFTER their corrections: an old row
    that arrives late, or a source that replays history. The truth still holds the correction (latest ordering
    value), so a statement that lets whatever arrives last win puts the stale row back and fails."""
    return df_ordered.iloc[_revision_rows(df_ordered, ts_col, cfg)].reset_index(drop=True).copy()


def make_revision_batch(df_ordered: pd.DataFrame, key_cols: list[str], ts_col: str, cfg: HazardConfig) -> pd.DataFrame:
    """cfg.rev_fraction of the rows re-delivered as revisions: same key, ordering column moved later, and every other
    column changed (only cfg.revise_col when that is set). The batch truth must therefore contain these rows. Every
    payload column changes so that an UPDATE which forgets a column is caught."""
    rows = _revision_rows(df_ordered, ts_col, cfg)
    rev = df_ordered.iloc[rows].reset_index(drop=True).copy()
    if len(rev) == 0:
        return rev
    rev[ts_col] = _shift_series(rev[ts_col])
    if cfg.revise_col is not None:
        cols = [pick_revise_col(list(df_ordered.columns), key_cols, ts_col, cfg.revise_col)]
    else:
        cols = [c for c in df_ordered.columns if c not in key_cols and c != ts_col]
    for col in cols:
        rev[col] = _mutate_series(rev[col])
    return rev


def make_hazard_batches(
    df: pd.DataFrame, key_cols: list[str], ts_col: str, cfg: HazardConfig
) -> dict[str, list[pd.DataFrame]]:
    """Deterministic delivery schedules, each a list of batches (DataFrames).

    baseline      rows ordered by (ts_col, key_cols) and split into cfg.n_batches chunks.
    late          cfg.late_fraction of the rows from non-final batches are each moved to a
                  random later batch (same multiset of rows as baseline).
    duplicate     cfg.dup_fraction of the rows are re-delivered in a later batch (an extra
                  trailing batch is added when the source batch is the last one).
    out_of_order  batch order permuted (see _batch_permutation) and rows shuffled within
                  each batch (same multiset as baseline).
    retried       cfg.retry_batches whole batches are delivered twice consecutively.
    revision      baseline followed by two trailing batches: cfg.rev_fraction rows re-delivered
                  with a later ordering value and every payload column changed (a correction), then
                  the superseded originals of those keys again (a stale row arriving after its
                  correction). Unlike the four delivery hazards, this changes the truth: the batch
                  recompute must keep the revised row. A repair with no WHEN MATCHED path keeps the
                  stale one at the first trailing batch; a blind last-write-wins upsert restores it
                  at the second.

    The baseline is split into effective_batches(len(df), cfg) deliveries, so a frame with fewer rows
    than cfg.n_batches has no empty batch.
    """
    base = _ordered(df, key_cols, ts_col)
    n = effective_batches(len(base), cfg)
    bounds = np.linspace(0, len(base), n + 1).round().astype(int)
    baseline = [base.iloc[bounds[i]:bounds[i + 1]].reset_index(drop=True) for i in range(n)]
    membership = np.repeat(np.arange(n), [len(b) for b in baseline])  # baseline batch of each row

    # late
    rng = _rng(cfg, "late")
    eligible = np.flatnonzero(membership < n - 1)
    late_rows = _select_fraction(rng, eligible, cfg.late_fraction)
    late_member = membership.copy()
    for r in late_rows:
        late_member[r] = int(rng.integers(membership[r] + 1, n))
    late = [base.iloc[np.flatnonzero(late_member == i)].reset_index(drop=True) for i in range(n)]

    # duplicate (re-deliver in a later batch; n means a new trailing batch)
    rng = _rng(cfg, "duplicate")
    dup_rows = _select_fraction(rng, np.arange(len(base)), cfg.dup_fraction)
    extra: list[list[int]] = [[] for _ in range(n + 1)]
    for r in dup_rows:
        extra[int(rng.integers(membership[r] + 1, n + 1))].append(int(r))
    duplicate = []
    for i in range(n):
        idx = np.concatenate([np.flatnonzero(membership == i), np.array(extra[i], dtype=int)])
        duplicate.append(base.iloc[idx].reset_index(drop=True))
    if extra[n]:
        duplicate.append(base.iloc[extra[n]].reset_index(drop=True))

    # out_of_order
    rng = _rng(cfg, "out_of_order")
    out_of_order = []
    for i in _batch_permutation(n, cfg):
        b = baseline[i]
        if cfg.shuffle and len(b) > 1:
            b = b.iloc[rng.permutation(len(b))].reset_index(drop=True)
        out_of_order.append(b)

    # retried
    retry = set(_retry_indices(n, cfg))
    retried: list[pd.DataFrame] = []
    for i, b in enumerate(baseline):
        retried.append(b)
        if i in retry:
            retried.append(b.copy())

    # revision (baseline, then corrections for a sample of keys, then those keys' superseded rows again)
    revision = list(baseline)
    rev_batch = make_revision_batch(base, key_cols, ts_col, cfg)
    if len(rev_batch):
        revision.append(rev_batch)
        revision.append(make_stale_batch(base, ts_col, cfg))

    return {
        "baseline": baseline,
        "late": late,
        "duplicate": duplicate,
        "out_of_order": out_of_order,
        "retried": retried,
        "revision": revision,
    }


# --------------------------------------------------------------------------------------
# Things under test: reference repairs and the SQL adapter
# --------------------------------------------------------------------------------------
def naive_append(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
    """Buggy reference: append every delivered row (passes snapshot on unique-key data,
    silently wrong under duplicate / retried delivery)."""
    if state is None or len(state) == 0:
        return batch.reset_index(drop=True).copy()
    return pd.concat([state, batch], ignore_index=True)


def merge_on_keys(key_cols: list[str], ts_col: str) -> ApplyFn:
    """Correct reference: upsert on key_cols keeping the latest ts_col (MERGE semantics)."""

    def _apply(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
        combined = batch if state is None or len(state) == 0 else pd.concat([state, batch], ignore_index=True)
        return recompute(combined, key_cols, ts_col)

    _apply.__name__ = "merge_on_keys"
    return _apply


def dedup_within_batch_append(key_cols: list[str], ts_col: str) -> ApplyFn:
    """Planted known-bad repair: dedups keys inside each batch, then appends. Handles
    intra-batch repeats only, so a batch delivered twice (retried) or a row re-delivered in a
    later batch (duplicate) lands twice. Used to validate that the harness catches it."""

    def _apply(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
        clean = recompute(batch, key_cols, ts_col)
        return naive_append(state, clean)

    _apply.__name__ = "dedup_within_batch_append"
    return _apply


def insert_if_absent(key_cols: list[str]) -> ApplyFn:
    """Reference for the common production MERGE with no WHEN MATCHED clause: append only the rows
    whose key is not already present. Safe under every delivery hazard on key-unique data; keeps
    the stale row under the revision hazard."""

    def _apply(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
        if state is None or len(state) == 0:
            return recompute_first(batch, key_cols)
        new = recompute_first(batch, key_cols)
        merged = new.merge(state[key_cols].drop_duplicates(), on=key_cols, how="left", indicator=True)
        fresh = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        return pd.concat([state, fresh], ignore_index=True)

    _apply.__name__ = "insert_if_absent"
    return _apply


def last_write_wins(key_cols: list[str]) -> ApplyFn:
    """Planted known-bad repair: MERGE ... WHEN MATCHED THEN UPDATE SET * with no ordering guard, so whatever is
    delivered last for a key wins. On key-unique data it passes late, duplicate, out_of_order and retried delivery
    and the corrections delivery; it is wrong only when a superseded row arrives after its correction."""

    def _apply(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
        new = batch.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
        if state is None or len(state) == 0:
            return new
        merged = state.merge(new[key_cols], on=key_cols, how="left", indicator=True)
        kept = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        return pd.concat([kept, new], ignore_index=True)

    _apply.__name__ = "last_write_wins"
    return _apply


def recompute_first(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    """First delivered row per key within a frame (what an insert-only MERGE keeps from one batch)."""
    return df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)


def sql_apply(duckdb_sql: str, key_cols: list[str], *, declare_primary_key: bool = False) -> ApplyFn:
    """Adapter: run a user SQL repair against duckdb tables `target` (current state) and
    `batch` (the delivered batch) and return the new target.

    Statements are split on ';'. If the last statement is a query (SELECT / WITH / FROM) its
    result IS the new target; otherwise `SELECT * FROM target` is returned after the DML ran.
    `declare_primary_key=True` adds a unique index on key_cols so `INSERT ... ON CONFLICT`
    works; the default mirrors Delta, which has no key constraints, so a buggy plain INSERT is
    silently wrong instead of raising. Local-only (duckdb dialect); the live equivalent is the
    Spark SQL repair template passed to `spark_sql_plan`.
    """
    statements = [s.strip() for s in duckdb_sql.split(";") if s.strip()]
    if not statements:
        raise ValueError("duckdb_sql contains no statements")
    key_list = ", ".join(f'"{c}"' for c in key_cols)

    def _apply(state: pd.DataFrame | None, batch: pd.DataFrame) -> pd.DataFrame:
        import duckdb  # lazy: the local SQL adapter only; the Spark cluster does not ship duckdb

        con = duckdb.connect()
        try:
            con.register("ns_batch_df", batch)
            con.execute("CREATE TABLE batch AS SELECT * FROM ns_batch_df")
            if state is None or len(state) == 0:
                con.execute("CREATE TABLE target AS SELECT * FROM ns_batch_df WHERE 1 = 0")
            else:
                con.register("ns_state_df", state)
                con.execute("CREATE TABLE target AS SELECT * FROM ns_state_df")
            if declare_primary_key and key_cols:
                con.execute(f"CREATE UNIQUE INDEX target_pk ON target ({key_list})")
            result: pd.DataFrame | None = None
            for stmt in statements:
                rel = con.execute(stmt)
                result = rel.df() if stmt.upper().startswith(("SELECT", "WITH", "FROM")) else None
            if result is None:
                result = con.execute("SELECT * FROM target").df()
            return result
        finally:
            con.close()

    _apply.__name__ = "sql_apply"
    return _apply


# --------------------------------------------------------------------------------------
# Running, truth and diff
# --------------------------------------------------------------------------------------
def run_schedule(apply_fn: ApplyFn, schedule: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Fold apply_fn over the delivery schedule starting from an empty target (state=None)."""
    state: pd.DataFrame | None = None
    for batch in schedule:
        state = apply_fn(state, batch)
        if not isinstance(state, pd.DataFrame):
            raise TypeError(f"apply_fn must return a pandas DataFrame, got {type(state).__name__}")
    if state is None:
        return pd.DataFrame(columns=schedule[0].columns if len(schedule) else [])
    return state


def recompute(df_all_rows: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    """Batch truth: the latest row (by ts_col) per key, canonically sorted."""
    if df_all_rows is None or len(df_all_rows) == 0:
        return pd.DataFrame(columns=[] if df_all_rows is None else df_all_rows.columns)
    ordered = _ordered(df_all_rows, key_cols, ts_col)
    return ordered.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def _canon(v: Any) -> Any:
    if v is None or v is pd.NaT:
        return None
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (int, np.integer)):
        return int(v)
    if isinstance(v, (float, np.floating, Decimal)):
        f = float(v)
        if math.isnan(f):
            return None
        return int(f) if f.is_integer() else f
    if isinstance(v, (pd.Timestamp, datetime, np.datetime64, date)):
        return pd.Timestamp(v).isoformat()
    if isinstance(v, (bytes, np.bytes_)):
        return bytes(v)
    if isinstance(v, str):
        return v
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return str(v)


def _canonical_rows(df: pd.DataFrame | None, cols: list[str]) -> Counter:
    if df is None or len(df) == 0:
        return Counter()
    frame = df.reindex(columns=cols)
    return Counter(tuple(_canon(v) for v in row) for row in frame.itertuples(index=False, name=None))


def diff(a: pd.DataFrame | None, b: pd.DataFrame | None, key_cols: list[str] | None = None) -> int:
    """Rows differing between a and b, symmetric: |a - b| + |b - a| as multisets after
    canonicalising values (dtype drift such as int64 vs Int64, datetime64[ns] vs [us],
    NaN vs None does not count as a difference). Row order and column order are ignored.
    Columns present in only one frame make every row differ, which is the intended schema
    check. key_cols is accepted for API symmetry; the comparison is over all columns."""
    cols_a = list(a.columns) if a is not None else []
    cols_b = list(b.columns) if b is not None else []
    cols = sorted(set(cols_a) | set(cols_b))
    ca, cb = _canonical_rows(a, cols), _canonical_rows(b, cols)
    return sum(abs(ca[k] - cb[k]) for k in set(ca) | set(cb))


# --------------------------------------------------------------------------------------
# Verdict and grading
# --------------------------------------------------------------------------------------
def hazard_coverage(schedules: dict[str, list[pd.DataFrame]]) -> dict[str, int]:
    """Rows each check actually exercised, from the schedules themselves: `run_twice` rows in the delivery applied
    again, `late` rows that arrived in a later batch, `duplicate` and `retried` rows delivered more than once,
    `out_of_order` batches that changed position, `revision` rows corrected. Zero means the check was vacuous."""
    base = schedules["baseline"]
    n_base = sum(len(b) for b in base)
    cols = sorted(base[0].columns) if base else []

    def bag(b: pd.DataFrame) -> Counter:
        return _canonical_rows(b, cols)

    cov: dict[str, int] = {"run_twice": len(base[-1]) if base else 0}
    if "late" in schedules:
        cov["late"] = sum(sum((bag(late) - bag(b)).values()) for late, b in zip(schedules["late"], base))
    if "duplicate" in schedules:
        cov["duplicate"] = sum(len(b) for b in schedules["duplicate"]) - n_base
    if "out_of_order" in schedules:
        cov["out_of_order"] = sum(1 for o, b in zip(schedules["out_of_order"], base) if len(b) and bag(o) != bag(b))
    if "retried" in schedules:
        cov["retried"] = sum(len(b) for b in schedules["retried"]) - n_base
    if "revision" in schedules:
        extra = schedules["revision"][len(base):]
        cov["revision"] = len(extra[0]) if extra else 0
    return cov


def coverage_from_row(row: Any) -> dict[str, int]:
    """The live plan's `coverage` statement returns one row of named counts (n_rows, run_twice and one column per
    hazard). Accepts a Spark Row, a mapping or None; a missing or NULL count is 0."""
    if row is None:
        return {}
    data = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {str(k): int(v or 0) for k, v in data.items()}


def settle_verdict(diffs: dict[str, int], hazard_names: Sequence[str], coverage: dict[str, int] | None) -> dict[str, Any]:
    """The verdict fields from the per-check diff counts and the coverage counts; one rule for the local harness and
    the live plan. replay_safe needs the baseline, run-twice and every named hazard to diff to zero AND each of those
    checks to have exercised at least one row; passing with a vacuous check is insufficient_coverage. No coverage
    at all (None) is read as nothing exercised."""
    hazards = {h: diffs[h] == 0 for h in hazard_names if h in diffs}
    out: dict[str, Any] = {
        "passes_snapshot": diffs.get("baseline", 1) == 0,
        "passes_run_twice": diffs.get("run_twice", 1) == 0,
        "hazards": hazards,
        "diff_rows": int(sum(diffs.values())),
        "coverage": dict(coverage) if coverage is not None else None,
        # reported with every verdict: a failing statement on a table that leaves a check vacuous can be fixed, but the
        # fix can never be proven there
        "uncovered": [c for c in ("run_twice", *hazard_names) if int((coverage or {}).get(c) or 0) < 1],
    }
    if not out["passes_snapshot"]:
        out["verdict"] = VERDICT_SNAPSHOT_WRONG
    elif not (out["passes_run_twice"] and all(hazards.values()) and len(hazards) == len(list(hazard_names))):
        out["verdict"] = VERDICT_SILENT_WRONG
    else:
        out["verdict"] = VERDICT_INSUFFICIENT if out["uncovered"] else VERDICT_REPLAY_SAFE
    return out


def replay_verdict(
    apply_fn: ApplyFn,
    df: pd.DataFrame,
    key_cols: list[str],
    ts_col: str,
    cfg: HazardConfig = HazardConfig(),
    recompute_fn: RecomputeFn = recompute,
) -> dict[str, Any]:
    """Grade one candidate. Returns the verdict dict plus `diffs` (per check), `coverage` (rows each check
    exercised), `uncovered`, `error` (exception type name or None) and `error_message`/`failed_stage` on crash."""
    truth = recompute_fn(df, key_cols, ts_col)
    schedules = make_hazard_batches(df, key_cols, ts_col, cfg)
    coverage = hazard_coverage(schedules)
    diffs: dict[str, int] = {}
    hazards = {h: False for h in HAZARDS}
    out: dict[str, Any] = {
        "passes_snapshot": False,
        "passes_run_twice": False,
        "hazards": hazards,
        "diff_rows": 0,
        "verdict": VERDICT_CRASH,
        "diffs": diffs,
        "error": None,
        "coverage": coverage,
        "uncovered": [],
    }
    stage = "baseline"
    try:
        baseline_state = run_schedule(apply_fn, schedules["baseline"])
        diffs["baseline"] = diff(baseline_state, truth, key_cols)
        out["passes_snapshot"] = diffs["baseline"] == 0

        stage = "run_twice"
        last = schedules["baseline"][-1]
        again = apply_fn(baseline_state.copy(), last)
        diffs["run_twice"] = diff(again, baseline_state, key_cols)
        out["passes_run_twice"] = diffs["run_twice"] == 0

        for stage in HAZARDS:
            # delivery hazards re-arrange the same rows, so the truth is unchanged; a revision adds
            # corrected rows, so its truth is the recompute over everything that was delivered
            stage_truth = truth
            if stage == "revision":
                stage_truth = recompute_fn(pd.concat(schedules[stage], ignore_index=True), key_cols, ts_col)
            diffs[stage] = diff(run_schedule(apply_fn, schedules[stage]), stage_truth, key_cols)
            hazards[stage] = diffs[stage] == 0
    except Exception as exc:  # the candidate crashed; record and stop
        out["error"] = type(exc).__name__
        out["error_message"] = str(exc)[:500]
        out["failed_stage"] = stage
        out["diff_rows"] = int(sum(diffs.values()))
        return out

    out.update(settle_verdict(diffs, HAZARDS, coverage))
    return out


GRADE_COLUMNS = [
    "candidate", "verdict", "passes_snapshot", "passes_run_twice",
    *HAZARDS, "diff_rows", "error",
]


def silent_wrong_rate(table: pd.DataFrame) -> float:
    """Share of candidates that reached the card (passed snapshot, did not crash) yet are
    silently wrong under replay. NaN when no candidate reached the card."""
    if len(table) == 0:
        return float("nan")
    reached = table[(table["passes_snapshot"] == True) & (table["verdict"] != VERDICT_CRASH)]  # noqa: E712
    if len(reached) == 0:
        return float("nan")
    return float((reached["verdict"] == VERDICT_SILENT_WRONG).sum() / len(reached))


class GradeTable(pd.DataFrame):
    """One row per candidate (see GRADE_COLUMNS) with a `silent_wrong_rate` property."""

    _metadata: list[str] = []

    @property
    def _constructor(self):  # keeps slices as GradeTable
        return GradeTable

    @property
    def silent_wrong_rate(self) -> float:
        return silent_wrong_rate(self)


def grade_many(
    candidates: dict[str, ApplyFn],
    df: pd.DataFrame,
    key_cols: list[str],
    ts_col: str,
    cfg: HazardConfig = HazardConfig(),
    recompute_fn: RecomputeFn = recompute,
) -> GradeTable:
    rows = []
    for name, fn in candidates.items():
        v = replay_verdict(fn, df, key_cols, ts_col, cfg, recompute_fn)
        rows.append({
            "candidate": name,
            "verdict": v["verdict"],
            "passes_snapshot": v["passes_snapshot"],
            "passes_run_twice": v["passes_run_twice"],
            **{h: v["hazards"][h] for h in HAZARDS},
            "diff_rows": v["diff_rows"],
            "error": v["error"],
        })
    return GradeTable(rows, columns=GRADE_COLUMNS)


# --------------------------------------------------------------------------------------
# Live plan: Spark SQL on Oracle AI Data Platform (string generation only)
# --------------------------------------------------------------------------------------
def _tag(stage: str, name: str, sql: str) -> str:
    return f"{PLAN_TAG_PREFIX}{stage}:{name}\n{sql}"


def parse_plan_tag(statement: str) -> tuple[str, str, str]:
    """Split a plan statement into (stage, name, sql). Statements without a tag get
    ('work', 'untagged', statement)."""
    first, _, rest = statement.partition("\n")
    if first.startswith(PLAN_TAG_PREFIX):
        stage, _, name = first[len(PLAN_TAG_PREFIX):].partition(":")
        return stage.strip(), name.strip(), rest
    return "work", "untagged", statement


def _delivery_predicates(hazard: str, n: int, cfg: HazardConfig) -> list[str]:
    """Row predicates over the staging table, one per delivery, for a schedule.
    Columns: ns_batch (1-based ntile), ns_late / ns_dup (deterministic row flags)."""
    if hazard == "baseline":
        return [f"ns_batch = {b}" for b in range(1, n + 1)]
    if hazard == "late":
        return [f"(ns_batch = {b} AND NOT ns_late) OR (ns_batch = {b - 1} AND ns_late)" for b in range(1, n + 1)]
    if hazard == "duplicate":
        preds = [f"ns_batch = {b} OR (ns_batch = {b - 1} AND ns_dup)" for b in range(1, n + 1)]
        preds.append(f"ns_batch = {n} AND ns_dup")  # trailing re-delivery of last-batch rows
        return preds
    if hazard == "out_of_order":
        return [f"ns_batch = {i + 1}" for i in _batch_permutation(n, cfg)]
    if hazard == "retried":
        retry = set(_retry_indices(n, cfg))
        preds = []
        for i in range(n):
            preds.append(f"ns_batch = {i + 1}")
            if i in retry:
                preds.append(f"ns_batch = {i + 1}")
        return preds
    if hazard == "revision":
        # the baseline deliveries; the trailing corrections delivery is a separate table (see spark_sql_plan)
        return [f"ns_batch = {b}" for b in range(1, n + 1)]
    raise ValueError(f"unknown hazard {hazard!r}; expected one of {HAZARDS}")


def _every_k(fraction: float) -> int:
    """pmod modulus so that roughly `fraction` of rows carry a flag (0 disables)."""
    return 0 if fraction <= 0 else max(1, int(round(1.0 / fraction)))


def _spark_type_family(spark_type: str) -> str:
    t = spark_type.strip().lower()
    if t.startswith("timestamp"):
        return "timestamp"
    if t == "date":
        return "date"
    if t.startswith(("int", "bigint", "smallint", "tinyint", "double", "float", "decimal", "numeric", "long", "short", "byte")):
        return "number"
    if t in ("boolean", "bool"):
        return "boolean"
    if t.startswith(("string", "varchar", "char")):
        return "string"
    return "other"


def spark_shift_expr(col: str, spark_type: str) -> str:
    """SQL moving the ordering column later (revision rows must win the latest-by-ts recompute)."""
    fam = _spark_type_family(spark_type)
    if fam == "timestamp":
        return f"{col} + INTERVAL 1 HOUR"
    if fam == "date":
        return f"date_add({col}, 1)"
    if fam == "number":
        return f"{col} + 1"
    raise TypeError(f"ordering column {col!r} of type {spark_type!r} cannot be shifted for the revision hazard")


def spark_mutate_expr(col: str, spark_type: str) -> str | None:
    """SQL for a visibly changed value of the same type, or None when the type is not mutable here."""
    fam = _spark_type_family(spark_type)
    if fam == "number":
        return f"{col} + 1"
    if fam == "boolean":
        return f"NOT {col}"
    if fam == "string":
        return f"concat({col}, '_rev')"
    if fam == "timestamp":
        return f"{col} + INTERVAL 1 DAY"
    if fam == "date":
        return f"date_add({col}, 1)"
    return None


def pick_spark_revise_col(columns: Sequence[tuple[str, str]], key_cols: Sequence[str], ts_col: str,
                          preferred: str | None = None) -> tuple[str, str] | None:
    """(name, type) of the payload column a live revision changes: the preferred one when given, else the
    first non-key, non-ordering column whose type spark_mutate_expr can change; None when there is none."""
    types = dict(columns)
    if preferred is not None:
        pick_revise_col([c for c, _ in columns], key_cols, ts_col, preferred)  # same validation as the local path
        if spark_mutate_expr(preferred, types[preferred]) is None:
            raise TypeError(f"revise_col {preferred!r} has type {types[preferred]!r}, which the harness cannot mutate")
        return preferred, types[preferred]
    for c, t in columns:
        if c in key_cols or c == ts_col:
            continue
        if spark_mutate_expr(c, t) is not None:
            return c, t
    return None


def spark_sql_plan(
    prod_table: str,
    sandbox_schema: str,
    repair_sql_template: str,
    key_cols: list[str],
    ts_col: str,
    hazards: list[str],
    cfg: HazardConfig = HazardConfig(),
    pin_version: int | None = None,
    columns: Sequence[tuple[str, str]] | None = None,
    n_rows: int | None = None,
) -> list[str]:
    """Spark SQL statement sequence for the live harness on Oracle AI Data Platform.

    `n_rows` is the table's row count when the caller has it (the heartbeat counts before it plans): the baseline
    is split into effective_batches(n_rows, cfg) deliveries so that a table with fewer rows than cfg.n_batches
    has no empty delivery to retry, reorder or run twice. The sampling flags also flag the first row on a small
    table (SMALL_TABLE_FACTOR), and a `coverage` statement counts the rows each check will exercise; the
    caller turns a passing result with an unexercised check into insufficient_coverage (settle_verdict).

    `columns` ((name, spark type) in table order) is required only for the `revision` hazard: the
    corrections table is built with an explicit select list that moves the ordering column later and
    changes every other non-key column whose type spark_mutate_expr can change (only cfg.revise_col
    when that is set), because Spark SQL cannot override a column out of `struct(*)` without naming
    it. A row with a NULL ordering value is never revised (it could not carry a later value). The revision truth is the recompute over staging plus the
    corrections, so a repair that never updates a matched key keeps the stale row and fails this hazard.
    After the corrections the superseded originals of the same keys are delivered once more; a repair
    that lets the last delivery win puts the stale row back and fails there.

    Every statement is prefixed with "-- ns:<stage>:<name>" (see parse_plan_tag). Stages, in
    order: pin (DESCRIBE HISTORY standalone, SHALLOW CLONE), stage (struct(*) + NTILE /
    ROW_NUMBER staging so no column list is needed), coverage (one row of counts, see
    coverage_from_row), target (empty CTAS per schedule), repair
    (temp view per delivery + the template with {target} and {batch} filled), twice (SHALLOW
    CLONE of the baseline target, final delivery applied again), diff (recompute CTE and
    EXCEPT ALL both ways), cleanup (DROP ... IF EXISTS, always last). The notebook runs
    cleanup in a finally block.

    Delta features relied on: SHALLOW CLONE (deep CLONE is NOT supported), VERSION AS OF,
    DESCRIBE HISTORY as a standalone statement (never a subquery). Column-agnostic staging
    relies on `struct(*)` / `ns_row.*` star expansion, which is standard Spark SQL but should
    be confirmed on the tested runtime on first live run.
    """
    unknown = [h for h in hazards if h not in HAZARDS]
    if unknown:
        raise ValueError(f"unknown hazards {unknown}; expected a subset of {HAZARDS}")
    if "{target}" not in repair_sql_template or "{batch}" not in repair_sql_template:
        raise ValueError("repair_sql_template must contain both {target} and {batch} placeholders")
    if not key_cols:
        raise ValueError("key_cols must not be empty")

    n = effective_batches(n_rows, cfg)
    t = prod_table.split(".")[-1]
    sbx = f"{sandbox_schema}.{t}_sbx"
    stage = f"{sandbox_schema}.{t}_stage"
    rev = f"{sandbox_schema}.{t}_rev"
    batch_view = f"ns_batch_{t}"
    schedules = ["baseline", *hazards]
    targets = {s: f"{sandbox_schema}.{t}_tgt_{s}" for s in schedules}
    twice = f"{sandbox_schema}.{t}_tgt_run_twice"
    order_by = ", ".join([ts_col, *key_cols])
    partition_by = ", ".join(f"ns_row.{k}" for k in key_cols)
    k_late, k_dup, k_rev = _every_k(cfg.late_fraction), _every_k(cfg.dup_fraction), _every_k(cfg.rev_fraction)

    def sampled(offset: str, k: int) -> str:
        # 1 row in k; on a table under SMALL_TABLE_FACTOR * k rows the modulus can miss every row (or hit only the
        # final batch), so the first row is flagged too and the hazard always has something to act on
        return f"(pmod(ns_slot{offset}, {k}) = 0 OR (ns_total < {SMALL_TABLE_FACTOR * k} AND ns_slot = 1))"

    late_flag = f"({sampled('', k_late)} AND ns_batch < {n})" if k_late else "false"
    dup_flag = sampled(" + 1", k_dup) if k_dup else "false"
    # a row with no ordering value cannot be moved later, so it is never the one a correction is built from
    rev_flag = f"({sampled(' + 2', k_rev)} AND ns_row.{ts_col} IS NOT NULL)" if k_rev else "false"

    with_revision = "revision" in hazards
    rev_select: str | None = None
    if with_revision:
        if not columns:
            raise ValueError("the revision hazard needs `columns` ((name, spark type) in table order)")
        names = [c for c, _ in columns]
        missing = [c for c in [ts_col, *key_cols] if c not in names]
        if missing:
            raise KeyError(f"columns is missing {missing}")
        types = dict(columns)
        only = pick_spark_revise_col(columns, key_cols, ts_col, cfg.revise_col) if cfg.revise_col is not None else None
        parts = []
        for c in names:
            changed = None
            if c == ts_col:
                changed = spark_shift_expr(c, types[c])
            elif c in key_cols:
                pass
            elif only is not None:
                changed = spark_mutate_expr(c, only[1]) if c == only[0] else None
            else:
                changed = spark_mutate_expr(c, types[c])   # None for a type the harness cannot change: passed through
            parts.append(f"{changed} AS {c}" if changed else c)
        rev_select = ",\n       ".join(parts)

    created_tables = [sbx, stage, *targets.values(), twice] + ([rev] if with_revision else [])
    drops = [_tag("cleanup", f"drop_{x.split('.')[-1]}", f"DROP TABLE IF EXISTS {x}") for x in created_tables]
    drops.append(_tag("cleanup", "drop_batch_view", f"DROP VIEW IF EXISTS {batch_view}"))

    plan: list[str] = [_tag("setup", "schema", f"CREATE SCHEMA IF NOT EXISTS {sandbox_schema}")]
    # stale objects from a kernel that died mid-run; same statements as the cleanup tail
    plan += [s.replace(f"{PLAN_TAG_PREFIX}cleanup:", f"{PLAN_TAG_PREFIX}setup:stale_") for s in drops]

    # pin
    plan.append(_tag("pin", "history", f"DESCRIBE HISTORY {prod_table}"))
    clone_src = prod_table if pin_version is None else f"{prod_table} VERSION AS OF {pin_version}"
    plan.append(_tag("pin", "shallow_clone", f"CREATE TABLE {sbx} SHALLOW CLONE {clone_src}"))

    # stage
    plan.append(_tag("stage", "ntile", (
        f"CREATE TABLE {stage} USING DELTA AS\n"
        f"SELECT ns_row, ns_batch, ns_slot,\n"
        f"       {late_flag} AS ns_late,\n"
        f"       {dup_flag} AS ns_dup,\n"
        f"       {rev_flag} AS ns_rev\n"
        f"FROM (\n"
        f"  SELECT struct(*) AS ns_row,\n"
        f"         NTILE({n}) OVER (ORDER BY {order_by}) AS ns_batch,\n"
        f"         ROW_NUMBER() OVER (ORDER BY {order_by}) AS ns_slot,\n"
        f"         COUNT(*) OVER () AS ns_total\n"
        f"  FROM {sbx}\n"
        f")"
    )))

    # corrections table for the revision hazard (same keys, ordering column later, one payload column changed)
    if with_revision:
        plan.append(_tag("stage", "revisions", (
            f"CREATE TABLE {rev} USING DELTA AS\n"
            f"SELECT {rev_select}\n"
            f"FROM (SELECT ns_row.* FROM {stage} WHERE ns_rev)"
        )))

    # coverage: the rows each check will exercise (zero = that check proves nothing on this table)
    def counted(pred: str) -> str:
        return f"SUM(CASE WHEN {pred} THEN 1 ELSE 0 END)"

    retry_batches = ", ".join(str(i + 1) for i in _retry_indices(n, cfg))
    cover = {"run_twice": counted(f"ns_batch = {n}")}
    if "late" in hazards:
        cover["late"] = counted("ns_late")
    if "duplicate" in hazards:
        cover["duplicate"] = counted("ns_dup")
    if "out_of_order" in hazards:
        cover["out_of_order"] = "CASE WHEN COUNT(DISTINCT ns_batch) > 1 THEN COUNT(DISTINCT ns_batch) ELSE 0 END"
    if "retried" in hazards:
        cover["retried"] = counted(f"ns_batch IN ({retry_batches})") if retry_batches else "0"
    if "revision" in hazards:
        cover["revision"] = counted("ns_rev")
    plan.append(_tag("coverage", "counts", (
        "SELECT COUNT(*) AS n_rows,\n       "
        + ",\n       ".join(f"{expr} AS {name}" for name, expr in cover.items())
        + f"\nFROM {stage}"
    )))

    # targets
    for s in schedules:
        plan.append(_tag("target", s, f"CREATE TABLE {targets[s]} USING DELTA AS SELECT * FROM {sbx} WHERE 1 = 0"))

    # repairs
    last_baseline_pred = _delivery_predicates("baseline", n, cfg)[-1]
    for s in schedules:
        preds = _delivery_predicates(s, n, cfg)
        for j, pred in enumerate(preds, start=1):
            plan.append(_tag("repair", f"{s}_batch{j}_view",
                             f"CREATE OR REPLACE TEMP VIEW {batch_view} AS\nSELECT ns_row.* FROM {stage} WHERE {pred}"))
            plan.append(_tag("repair", f"{s}_batch{j}_apply",
                             repair_sql_template.format(target=targets[s], batch=batch_view)))
        if s == "revision":
            j = len(preds) + 1
            plan.append(_tag("repair", f"{s}_batch{j}_view",
                             f"CREATE OR REPLACE TEMP VIEW {batch_view} AS\nSELECT * FROM {rev}"))
            plan.append(_tag("repair", f"{s}_batch{j}_apply",
                             repair_sql_template.format(target=targets[s], batch=batch_view)))
            # the superseded originals of the corrected keys, delivered after their corrections
            j += 1
            plan.append(_tag("repair", f"{s}_batch{j}_view",
                             f"CREATE OR REPLACE TEMP VIEW {batch_view} AS\nSELECT ns_row.* FROM {stage} WHERE ns_rev"))
            plan.append(_tag("repair", f"{s}_batch{j}_apply",
                             repair_sql_template.format(target=targets[s], batch=batch_view)))

    # run twice: clone the finished baseline target and apply the final delivery again
    plan.append(_tag("twice", "clone", f"CREATE TABLE {twice} SHALLOW CLONE {targets['baseline']}"))
    plan.append(_tag("twice", "view",
                     f"CREATE OR REPLACE TEMP VIEW {batch_view} AS\nSELECT ns_row.* FROM {stage} WHERE {last_baseline_pred}"))
    plan.append(_tag("twice", "apply", repair_sql_template.format(target=twice, batch=batch_view)))

    # diffs
    truth_cte = (
        "WITH truth AS (\n"
        "  SELECT ns_row.* FROM (\n"
        f"    SELECT ns_row, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY ns_row.{ts_col} DESC) AS rn\n"
        f"    FROM {stage}\n"
        "  ) WHERE rn = 1\n"
        ")"
    )
    # the revision truth is the recompute over everything delivered: staging rows plus the corrections
    rev_truth_cte = (
        "WITH delivered AS (\n"
        f"  SELECT ns_row.* FROM {stage}\n"
        "  UNION ALL\n"
        f"  SELECT * FROM {rev}\n"
        "), truth AS (\n"
        "  SELECT ns_row.* FROM (\n"
        f"    SELECT ns_row, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY ns_row.{ts_col} DESC) AS rn\n"
        "    FROM (SELECT struct(*) AS ns_row FROM delivered)\n"
        "  ) WHERE rn = 1\n"
        ")"
    )
    for s in schedules:
        plan.append(_tag("diff", s, (
            f"{rev_truth_cte if s == 'revision' else truth_cte}\n"
            f"SELECT COUNT(*) AS diff_rows FROM (\n"
            f"  (SELECT * FROM {targets[s]} EXCEPT ALL SELECT * FROM truth)\n"
            f"  UNION ALL\n"
            f"  (SELECT * FROM truth EXCEPT ALL SELECT * FROM {targets[s]})\n"
            f")"
        )))
    plan.append(_tag("diff", "run_twice", (
        f"SELECT COUNT(*) AS diff_rows FROM (\n"
        f"  (SELECT * FROM {twice} EXCEPT ALL SELECT * FROM {targets['baseline']})\n"
        f"  UNION ALL\n"
        f"  (SELECT * FROM {targets['baseline']} EXCEPT ALL SELECT * FROM {twice})\n"
        f")"
    )))

    plan += drops
    return plan


NOTEBOOK_TEMPLATE_PATH = Path(__file__).resolve().parents[1] / "notebooks" / "replay_spark_template.py"
NOTEBOOK_PLAN_TOKEN = "__NS_PLAN__"


def render_spark_cell(plan: list[str], template_path: Path = NOTEBOOK_TEMPLATE_PATH) -> str:
    """Fill the notebook template's plan token with a literal list so the cell is
    self-contained (no nightshift import in the kernel)."""
    text = template_path.read_text(encoding="utf-8")
    if NOTEBOOK_PLAN_TOKEN not in text:
        raise ValueError(f"{template_path} has no {NOTEBOOK_PLAN_TOKEN} token")
    return text.replace(NOTEBOOK_PLAN_TOKEN, repr(list(plan)), 1)


__all__ = [
    "HAZARDS", "SCHEDULES", "DELIVERY_HAZARDS", "HazardConfig", "GradeTable", "ApplyFn",
    "make_hazard_batches", "make_revision_batch", "make_stale_batch", "pick_revise_col", "pick_spark_revise_col",
    "spark_shift_expr", "spark_mutate_expr", "effective_batches",
    "naive_append", "merge_on_keys", "dedup_within_batch_append", "insert_if_absent", "last_write_wins", "sql_apply",
    "run_schedule", "recompute", "diff", "replay_verdict", "grade_many", "silent_wrong_rate",
    "hazard_coverage", "coverage_from_row", "settle_verdict",
    "VERDICT_REPLAY_SAFE", "VERDICT_SILENT_WRONG", "VERDICT_SNAPSHOT_WRONG", "VERDICT_INSUFFICIENT", "VERDICT_CRASH",
    "spark_sql_plan", "parse_plan_tag", "render_spark_cell",
]
