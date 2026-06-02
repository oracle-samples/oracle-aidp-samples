---
plan_version: v1
base_branch: main
base_commit: 52fe466
---

# Plan: Backfill Follow-ups and Polish

## Scope

Six follow-ups deferred from `backfill-api-and-wap-target-mode`
(PR #13). All items are independent — each can ship as a separate
commit. Bundled under one branch + one PR for review economy, not
because they share code.

| # | Item | Bucket | Effort |
|---|------|--------|--------|
| 1 | `Qualifire.backfill(parallelism=N)` + `--parallelism N` | A | M |
| 2 | `WAPConfig.partition_column` auto-detect from bare `partition_ts` | A | S |
| 3 | `read_validation_history_bulk(limit>1)` across 4 backends | A | M |
| 4 | `metrics-backfill-and-soft-delete/shipped.md` doc drift fix | B | XS |
| 5 | CLI: route `errored` summary lines to stderr | B | XS |
| 6 | `Qualifire.backfill(max_partitions=N)` + `--max-partitions N` | B | XS |

## Hard Stops (out of scope, do NOT touch)

- **HS1.** No change to default behavior anywhere, with TWO
  explicit, intentional carve-outs (codex R5 BLOCKER #1, #2):
  - **Carve-out A (engine-call dedup, Item 1):** Today's serial
    `_backfill_one` per metric makes the engine run N times per
    anchor, producing N redundant collection/validation writes
    and notification fires. The refactor cuts this to 1
    engine.run() per anchor regardless of `parallelism`. The
    *count of write/notification side-effects per anchor*
    therefore drops from N to 1 in `parallelism=1` mode too.
    This is intentional and operator-positive (today's redundant
    writes were never useful — they re-collected the same metric
    rows). Documented in `docs/CHANGELOG.md` as a compatibility
    note. T1.9 pins the new contract; T1.11 (added) pins that
    notifications fire EXACTLY once per anchor in serial mode.
  - **Carve-out B (single-row bulk semantics, Item 3):** Today's
    `read_validation_history_bulk(limit=1)` is partition-blind:
    a tombstone on ANY partition for a key hides ALL rows for
    that key. v7+ aligns it with the singular
    `read_validation_history` partition-aware semantics: a
    tombstone for partition P hides only P. Affects the
    notification suppression caller — operators get
    *more accurate* per-partition suppression instead of
    blanket-key suppression. Documented in CHANGELOG as a
    behavior change. Suppression-caller regression test pins
    the alignment.

  Every other new kwarg defaults to today's value
  (`max_partitions=10000`, multi-row bulk read still defaults
  to `limit=1` at every caller, parallelism=1 default).
- **HS2.** Parallelism stays at the **outer anchor loop only**. The
  inner per-metric loop (`for metric_name in scope.metric_filter`)
  shares storage-side ordering and partition-source state — leave
  serial.
- **HS3.** Multi-row bulk read returns active-only rows. **No
  `include_inactive` kwarg** in this iteration. Audit consumers
  needing tombstones surface as a separate backlog item.
- **HS4.** `read_validation_history` (singular) signature unchanged.
  Only `read_validation_history_bulk` lifts the `limit=1` ceiling.
- **HS5.** No new CLI flag rename. `--skip-if-cached` stays as-is
  (the broader rename to `--skip-recollection` is a separate feature
  — `skip-recollection` — and is explicitly NOT bundled here).
- **HS6.** `expand_partition_ts` gains exactly one keyword-only
  argument `max_partitions: int = 10000`. No other internal API
  picks up the kwarg — `run_backfill` and `Qualifire.backfill`
  forward their own kwarg through to this single call site at
  `qualifire/core/backfill.py:163`. (Verified via `grep -rn
  expand_partition_ts qualifire/`: this is the only call site.)
- **HS7.** Auto-detected `partition_column` does NOT mutate the
  user's `WAPConfig` instance. The resolution is local to the
  backfill source-select preflight — read-only inspection.
- **HS8.** `_read_severity_before` (qualifire/core/backfill.py:459)
  calls `read_validation_history(validation_name="")`, which with
  today's exact-match filter returns no rows — meaning
  `severity_before` is always `None`. This is a pre-existing
  bug surfaced by codex R4 HIGH and does NOT affect the
  parallelism refactor's correctness (the refactor preserves
  exact pre-existing behavior). Captured as a separate
  backlog item: `docs/features/backfill-severity-before-broken-readback/idea.md`
  (to be filed during the follow-ups capture phase). Plan
  v6 explicitly declines to fix it here — fixing it would
  expand scope into the diff-classification semantics
  (`refreshed` vs `unchanged` when only severity differs) and
  belongs in its own iteration.

## Item Details

### 1. Backfill `--parallelism N`

**API surface.**
- `Qualifire.backfill(..., parallelism: int = 1)` — new kwarg.
- `qualifire backfill --parallelism N` — new CLI flag.
- `run_backfill(..., parallelism: int = 1)` — plumbed through.

**Mechanism.** `concurrent.futures.ThreadPoolExecutor`.

Rationale:
- Most backfill cells are I/O-bound on storage reads + Spark calls
  (which release the GIL) — threads scale fine.
- Process pool would force pickling Spark sessions, storage handles,
  and validator config — large blast radius.
- Forwarding to a Spark scheduler couples non-Spark backends
  (SQLite/JDBC) to Spark configuration.

**Where to fan out.** `qualifire/core/backfill.py:139-210` —
the outer `for scope in scopes:` × `for anchor in anchors:` loops.

**Work-unit granularity.** ONE future per
`(scope_idx, scope, anchor_idx, anchor)`. Inside each future,
the engine runs **exactly once** for the anchor — NOT once per
metric. Per codex R2 BLOCKER #1: today's `_backfill_one` calls
`_run_partition` which runs the full engine, and we loop over
metrics calling it N times per anchor → today does N redundant
engine.run() calls per anchor (writes / notifications duplicate
N times). Under parallelism this duplication races.

**Refactor (in scope, item 1):** Introduce a new helper
`_run_anchor_once(scope, anchor, *, config, backend, storage,
notifiers, ctx) -> EngineResult`. It calls the engine exactly
once with `scope.validations` and returns the full result. Then
`_backfill_one` becomes a thin wrapper that takes that cached
`EngineResult` and extracts the metric+severity tuple.
Per-metric loop iterates over the cached result.

```python
# Inside the (scope, anchor) worker:
engine_result = _run_anchor_once(unit.scope, unit.anchor, ...)
diffs = []
for metric_name in unit.scope.metric_filter or _all_metric_names(unit.scope):
    diff = _build_partition_diff(engine_result, metric_name, ...)
    diffs.append(diff)
# Sample-validator skipped emissions from the existing
# qualifire/core/backfill.py:196-210 block live here, once per
# anchor (preserves contract).
```

This is a contained refactor of `_backfill_one`. Cuts today's
engine calls from O(N) to O(1) per anchor — measurable wall-time
win on its own, even at `parallelism=1`. Add T1.9 below to assert
exactly one engine.run() call per anchor under serial mode (was
N).

**Pre-engine ordering preserved (codex R3 BLOCKER #2).** Today
`_backfill_one` does FIVE things in this order:
1. Read `original_value` from storage (pre-engine state).
2. Read `severity_before` (last validation pre-backfill).
3. If `soft_delete_prior`, write prior tombstone (BEFORE the
   new collection lands).
4. Call `_run_partition` (engine.run()).
5. Read `backfilled_value` / `severity_after` from result.

Naively running engine first would tombstone the just-collected
value and read post-engine state for `original_value`. The
worker must split the per-metric loop into TWO passes around
the single engine call:

```python
def _process_anchor(unit, *, config, run_config, backend, storage,
                    notifiers_or_empty, soft_delete_prior, ...):
    diffs: list[PartitionDiff] = []
    ctr = Counter()
    pre: dict[str, _PreEngine] = {}

    metric_names = unit.scope.metric_filter or _all_metric_names(unit.scope)

    # Pass 1 (pre-engine, per metric): read originals + write
    # prior tombstones. **Any failure here aborts the anchor**
    # — codex R4 BLOCKER. If a tombstone for metric A fails,
    # running the engine would still write fresh collection
    # rows for every metric in scope.validations, producing a
    # mixed-state row family with metric A tombstoned and B/C
    # collected — operator can't recover cleanly.
    for m in metric_names:
        try:
            pre[m] = _read_pre_engine_state(
                storage=storage, scope=unit.scope, metric_name=m,
                anchor=unit.anchor)
            if soft_delete_prior:
                _write_prior_tombstone(
                    storage=storage, dataset_name=unit.scope.dataset.name or "",
                    table_name=pre[m].table, metric_name=m,
                    anchor=unit.anchor, owner=owner, bu=bu)
        except Exception as e:
            # Abort the anchor: every metric for this anchor is
            # 'errored'. Engine.run() is skipped — no partial
            # writes. Sibling anchors (other workers) unaffected.
            for mm in metric_names:
                diffs.append(_errored_diff(unit, mm, f"pre-engine[{m}]: {e}"))
                ctr["errored"] += 1
            return unit.seq_idx, diffs, ctr

    # Single engine call per anchor.
    try:
        engine_result = _run_anchor_once(
            scope=unit.scope, anchor=unit.anchor, config=config,
            run_config=run_config, backend=backend, storage=storage,
            notifiers=notifiers_or_empty)
    except Exception as e:
        # Anchor-wide engine failure → error every metric.
        for m in metric_names:
            diffs.append(_errored_diff(unit, m, f"engine: {e}"))
            ctr["errored"] += 1
        return unit.seq_idx, diffs, ctr

    # Pass 2 (post-engine, per metric): build per-metric diffs.
    for m in metric_names:
        if pre[m] is None:
            continue  # already errored in Pass 1
        diff = _build_partition_diff(
            engine_result=engine_result, pre=pre[m],
            scope=unit.scope, metric_name=m, anchor=unit.anchor)
        diffs.append(diff)
        ctr[diff.status] += 1

    # Sample-validator "skipped" emissions (qualifire/core/backfill.py:196-210).
    if unit.scope.metric_filter is None:
        for sample_name in _sample_validator_names(unit.scope):
            diffs.append(PartitionDiff(..., status="skipped",
                                       skip_reason="no_historical_samples"))
            ctr["skipped"] += 1
    return unit.seq_idx, diffs, ctr
```

**Public API surface.** `_backfill_one` is INTERNAL (single
underscore prefix, no callers outside `qualifire/core/backfill.py`
— verified via `grep -rn _backfill_one qualifire/`). Its
signature is free to change. v5 plan: keep the name, change
the signature to take a cached `EngineResult` and a `_PreEngine`
record. Equivalent contract; no behavior change to the public
`Qualifire.backfill` API.

**Work-unit driver:**

```python
@dataclass
class _WorkUnit:
    seq_idx: int           # original linearization order
    scope_idx: int
    scope: ScopedDataset
    anchor_idx: int
    anchor: datetime

# Build work list serially; preserve sequencing.
work: list[_WorkUnit] = []
seq = 0
for s_idx, scope in enumerate(scopes):
    anchors = expand_partition_ts(partition_ts, step=step_td,
                                  max_partitions=max_partitions)
    for a_idx, anchor in enumerate(anchors):
        work.append(_WorkUnit(seq, s_idx, scope, a_idx, anchor))
        seq += 1

if parallelism == 1:
    results = [_process_anchor(u, ...) for u in work]
else:
    notifiers_or_empty = {}  # forced-empty under parallelism>1
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = [ex.submit(_process_anchor, u, ..., notifiers_or_empty=notifiers_or_empty) for u in work]
        try:
            results = [f.result() for f in as_completed(futures)]
        except KeyboardInterrupt:
            ex.shutdown(wait=True, cancel_futures=True)
            raise  # see "Cancellation contract" below
```

**Determinism.** Sort `results` by `seq_idx` (the original
serial-traversal index), then flatten diffs in order. This
preserves the *exact* ordering of today's serial output:
`(scope_order, anchor_order, metric_order)` — NOT a
metric-first lexicographic sort. Add a regression test that
serial and `parallelism=8` produce identical
`BackfillReport.partitions`.

**Validation.** `parallelism` must be `>= 1`. Reject `0` / negative
at API boundary with `QualifireConfigError`. Cap at `64` (sanity —
operators going beyond should chunk by date range).

**Storage thread safety.**

Audit summary of `qualifire/storage/sqlite_storage.py`:
- Today: `__init__` builds a single persistent connection
  (`self._conn`), no `check_same_thread=False`, no lock.
- Default `sqlite3.connect()` raises `ProgrammingError` if used
  from a thread other than its creator.

Decision per backend:

| Backend | Approach |
|---------|----------|
| SQLite | Add `check_same_thread=False` to the connection in `_get_connection` (qualifire/storage/sqlite_storage.py:38) AND a `threading.Lock` (`self._lock`) wrapping **every** public method (`write_results`, `read_metric_history`, `read_validation_history`, `read_validation_history_bulk`, `read_health_data`, `read_metric_history_by_partition`, `read_collection_metric_at_partition`, `read_latest_run`). Per codex R1 BLOCKER #2 — locking writes only is wrong because reads through the shared connection from worker threads also raise `ProgrammingError`. **Serializes I/O** — acceptable since SQLite is the dev/test backend. |
| Delta | Add per-call UUID salt to the temp-view name in `read_validation_history_bulk` (delta_storage.py:430) — currently `_qf_bulk_keys_{abs(hash(tuple(keys))) % (1 << 31)}` collides under concurrent workers with same `keys`. New: `_qf_bulk_keys_{uuid.uuid4().hex}`. Also wrap the create-view + query + drop in a try/finally so a worker crash doesn't leak temp views. **Otherwise** Spark sessions are thread-safe — no storage-wide lock. |
| ExternalCatalog | Same temp-view collision (external_catalog.py:780). Same fix: UUID salt + try/finally. |
| JDBC | py4j JavaGateway is thread-safe; each Spark JDBC write opens its own connection — no shared mutable storage state. No change required, but add a concurrency smoke test (T1.5j below). |

**Fan-out granularity.** Fan out at `(scope, anchor)` — NOT at
`(scope, anchor, metric)`. Rationale: within an anchor, the
per-metric serial loop preserves the storage natural-key write
ordering per partition (avoids two parallel metric writes
contending on the same `(table, anchor, dim)` row family). Cross
-anchor parallelism captures most of the wall-time win without
this risk.

**Notification side-effects under parallelism.** The engine fires
notifications during `_backfill_one`. Two parallel workers
processing different anchors of the same validation could race on
the suppression read in `read_validation_history_bulk` (today
limit=1; same in v2 default). Decision:
- Backfill is a *retroactive* operation; notifications during
  backfill are noisy and rarely useful operationally.
- New behavior: when `parallelism > 1`, the backfill driver
  forces `notifiers={}` for the inner engine call. Single-thread
  default (`parallelism=1`) keeps existing notification behavior
  unchanged.
- **Visibility (adversarial R2 find).** Silent suppression is
  hostile. Surface this in two places:
  1. `BackfillReport` gains `notifications_suppressed: bool`
     (set when `parallelism > 1`).
  2. CLI `_cmd_backfill` emits a stderr line on entry:
     `note: parallelism=N — notifications suppressed for this
     run` when `args.parallelism > 1`.
- Test T1.7 below pins both signals.

**Cancellation contract (codex R3 MEDIUM #6 — pick one).**
Ctrl-C / `KeyboardInterrupt` during backfill:
- Queued futures are cancelled via
  `ex.shutdown(wait=True, cancel_futures=True)`.
- In-flight workers are allowed to complete (cannot interrupt
  threads cleanly in CPython; partial work is dangerous under
  storage writes).
- The `KeyboardInterrupt` propagates out of `Qualifire.backfill`.
  **No partial `BackfillReport` is constructed.** Operators see
  the exception and re-run with a narrowed range.

Rationale: a partial report would invite operators to act on
incomplete data; the cleaner contract is "if you cancelled, the
run did not produce a final accounting." Document in
`docs/backfill_and_soft_delete.md`.

**Test plan (Item 1).**
- T1.1: Concurrency proven via a counter+barrier on the
  ANCHOR worker (codex R5 MEDIUM): each `_run_anchor_once` (or
  the wrapping `_process_anchor`) increments an atomic counter
  on entry, waits on `threading.Barrier(parallelism)`, then
  decrements. Test issues `parallelism=4` over 4+ distinct
  anchors. Barrier waits forever if anchor-level fan-out
  collapsed to serial — assertion is "no timeout". Instruments
  the anchor worker, NOT the now-thin `_backfill_one`
  metric-level wrapper.
- T1.2: `BackfillReport.partitions` ordering is BYTE-FOR-BYTE
  identical between `parallelism=1` and `parallelism=8` runs
  (no lex sort — sort by `seq_idx` only). Test:
  `assert serial.partitions == parallel.partitions`. Per codex
  R2 HIGH #3 — drop any lexicographic sort claim from earlier
  drafts.
- T1.3: Errored anchor in one worker does not cancel siblings.
  Setup: 3 anchors × 2 metrics. Inject Pass 1 failure on anchor
  #2's metric A (e.g., monkey-patch `_write_prior_tombstone`
  to raise for that anchor only). Assertions:
  - Anchor #2: BOTH metrics report `errored` (abort-anchor
    contract); engine.run() was NOT called for anchor #2.
  - Anchors #1 + #3: complete normally (each yields 2 diffs).
  - `BackfillReport.errored == 2` (not 1 — codex R5 HIGH #3
    correction).
  - Sample-validator skipped emissions absent for anchor #2.
- T1.4: `parallelism=0` raises `QualifireConfigError`. Same for
  negative; same for `parallelism > 64`.
- T1.5: SQLite concurrent writes don't corrupt rowcounts (issue
  100 backfills with `parallelism=8`, count rows persisted).
  Lock works.
- T1.6: CLI `--parallelism 4` plumbs through; default unchanged.
- T1.7: `parallelism > 1` forces `notifiers={}` — observe a
  registered test notifier is NOT called. `parallelism=1` calls
  the notifier (regression).
- T1.8: KeyboardInterrupt during backfill: in-flight workers
  complete, no new ones start, then the exception propagates.
  Operator does NOT receive a partial `BackfillReport` —
  exception path is the contract (simpler than partial-result
  reconstruction; documented in `docs/backfill_and_soft_delete.md`).
- T1.9: Engine call count per anchor — registers a counter on
  `engine.run` and asserts exactly **one** call per anchor under
  `parallelism=1`. Was N today. Regression guard for the
  metric-loop dedup refactor.
- T1.10: `BackfillReport.notifications_suppressed` is `True`
  when `parallelism > 1` and `False` otherwise. CLI prints the
  stderr advisory only when `parallelism > 1`.
- T1.11: Engine-call dedup (HS1 carve-out A pin). Setup:
  scope with N=3 metrics, registered notifier, `parallelism=1`.
  Assertions:
  - `engine.run()` invoked **once** per anchor (was N today).
  - Notifier invoked **once** per
    `(anchor, validation_name)` (was N today).
  - All N metric diffs still produced from the cached result.

**Exit criteria (Item 1).** All T1.* green; serial path shows
zero behavior change (apart from HS1 carve-outs A/B) in existing `tests/test_backfill_*` (untouched
defaults); `git diff` shows no change to `default=1` semantics on
any caller.

---

### 2. `WAPConfig.partition_column` auto-detect

**Where it kicks in.** `_validate_backfill_source` at
`qualifire/core/backfill.py:287-313`. Today line 290 raises if
`partition_column` is missing on a WAP backfill in metrics mode.

**New behavior.** Before raising:
1. If `ds.wap.partition_column` is set → use it (today).
2. Else, compute `pt = effective_partition_ts(ds, run_config)`
   (NOT just `ds.partition_ts`) — codex R1 MEDIUM picked up that
   datasets can inherit `partition_ts` from `QualifireConfig`.
3. If `pt` matches `_BARE_IDENT_RE` → derive
   `partition_column = pt.strip()` and continue.
4. Else → existing error.

`_validate_backfill_source` gains a `run_config: QualifireConfig`
parameter (forwarded from the call site at
`qualifire/core/backfill.py:160`).

**Threading the effective value to all consumers (codex R2
BLOCKER #2 + missed HIGH #7).** Auto-detect at preflight is not
enough: `_build_partition_dataset` at backfill.py:591-660 still
reads `ds.wap.partition_column` directly (line 631), and the
non-WAP filter construction reads `ds.partition_ts` directly.
Both must consult the *effective* values.

Plan:
1. New helper `_resolve_partition_anchors(ds, run_config) ->
   tuple[str | None, str | None]` returning
   `(effective_partition_column, effective_partition_ts_expr)`.
   - WAP path: explicit `wap.partition_column` wins; else if
     `effective_partition_ts(ds, run_config)` matches
     `_BARE_IDENT_RE`, derive from it; else `None`.
   - Non-WAP path: returns `(None, effective_partition_ts(ds, run_config))`.
2. `_build_partition_dataset` accepts the resolved values as
   parameters (no more reading `ds.wap.partition_column` or
   `ds.partition_ts` directly). The call site passes them in.
3. `_validate_backfill_source` consults the same resolver.

This closes both BLOCKER #2 (preflight only) and HIGH #7 (non-
WAP filter construction also ignored run-level inheritance).
T2.10 below pins the non-WAP path explicitly.

**Stricter regex than `_SAFE_NAME_RE`.** Per codex R1 HIGH #6:
`_SAFE_NAME_RE` at `qualifire/core/config.py:17` is
`^[a-zA-Z0-9_.\-]+$` — accepts leading digits, dots, and
hyphens. The auto-detect path interpolates the result *unquoted*
into SQL at `qualifire/core/backfill.py:631`
(`f"{col} = {anchor_lit}"`), so values like `123abc`,
`my-col`, or `t.col` would build invalid or mis-targeted SQL.

Define a NEW stricter constant in `qualifire/core/backfill.py`:
`_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")` and
use it for auto-detect ONLY. The existing `_SAFE_NAME_RE`
validator on `WAPConfig.partition_column` stays — operators who
explicitly set a permissive value continue to work.

**Accepted forms (explicit list).** Auto-detection accepts ONLY
bare unqualified identifiers matching `_BARE_IDENT_RE`
(`^[A-Za-z_][A-Za-z0-9_]*$`). It does NOT accept:
- Dot-qualified refs (`my_table.event_date`) — operator could
  mean either side.
- Backtick-quoted (`` `event_date` ``) — would need stripping
  and we don't want backend-specific quoting in resolver logic.
- Any string with whitespace, parens, or operators.

Operators using non-bare partition_ts (Jinja, dotted, quoted)
must continue to set `wap.partition_column` explicitly. The error
message at backfill.py:290 names this exact opt-in.

**Confirmed consumer audit (`grep -rn "wap.partition_column"`).**
- `qualifire/core/backfill.py:290` (preflight raise — patch site)
- `qualifire/core/backfill.py:631` (filter construction — patch site)
- `qualifire/api.py:677` (docstring only — no code change)

Two code sites. Both route through the **single** new helper
`_resolve_partition_anchors(ds: DatasetConfig, run_config:
QualifireConfig) -> tuple[str | None, str | None]` defined
above. There is no separate `_effective_partition_column` or
`_derive_partition_column(ds)` helper — codex R3 BLOCKER #3
flagged earlier prose suggesting them. They drop `run_config`
and so cannot honour run-level `partition_ts` inheritance.
**Single resolver, single call shape, run_config always
threaded through.**

**Critical: do NOT mutate `WAPConfig`.** The derived value is
returned by `_resolve_partition_anchors` and passed by
parameter to consumers. Mutating a Pydantic model the
operator owns would surprise them in
post-backfill inspection.

**Search-and-replace audit (codex R4 BLOCKER — stale prose
removed).** Every consumer of `wap.partition_column` and every
non-WAP read of `ds.partition_ts` receives the resolved values
`(effective_partition_column, effective_partition_ts_expr)`
from `_resolve_partition_anchors(ds, run_config)` *as
parameters*. Specifically:
- `qualifire/core/backfill.py:631` — the filter construction —
  receives the resolved column as a function parameter; no
  direct read of `ds.wap.partition_column`.
- `qualifire/core/backfill.py:659` — non-WAP filter — receives
  the resolved partition_ts expression as a parameter; no
  direct read of `ds.partition_ts`.
- `qualifire/core/backfill.py:290`, `:300` — preflight — calls
  the resolver itself.

`grep -n "wap\.partition_column\|ds\.partition_ts" qualifire/core/backfill.py`
**post-implementation** must show only the resolver itself
reading these fields. CI grep guard added to T2.* (one of the
sub-cases checks the post-impl source for stray reads).

**Test plan (Item 2).**
- T2.1: WAP dataset with `partition_ts='event_date'` and
  `partition_column` unset — backfill succeeds, derived
  partition_column is `'event_date'`.
- T2.2: WAP dataset with `partition_ts='{{ ds }}'` and
  `partition_column` unset — raises existing error.
- T2.3: WAP dataset with both set — explicit value wins
  (regression that auto-detect doesn't override).
- T2.4: WAP dataset with `partition_ts="event_date AND active=1"`
  — does NOT match `_BARE_IDENT_RE`, falls through to error.
- T2.5: WAP dataset with `partition_ts="123abc"` (leading digit)
  — auto-detect rejects, falls through to error.
- T2.6: WAP dataset with `partition_ts="my-col"` (hyphen)
  — auto-detect rejects.
- T2.7: WAP dataset with `partition_ts="t.col"` (dotted)
  — auto-detect rejects.
- T2.8: WAP dataset with no dataset-level `partition_ts` but
  run-level `QualifireConfig.partition_ts="event_date"` — auto-
  detect derives via `effective_partition_ts`, succeeds.
- T2.9: Confirm `WAPConfig` instance is unmodified after backfill
  (`assert ds.wap.partition_column is None` post-run).
- T2.10: **Non-WAP** dataset with `partition_ts` unset on the
  dataset but set at run level (`QualifireConfig.partition_ts="event_date"`)
  — backfill source-select preflight passes (uses effective
  value) and the partition filter constructed by
  `_build_partition_dataset` references `event_date`.

**Exit criteria (Item 2).** T2.* green; derived value never written
back to config; existing explicit-partition_column tests untouched.

---

### 3. Multi-row `read_validation_history_bulk(limit > 1)`

**Contract.** For each `ValidationKey` in `keys`, return up to
`limit` most-recent **active** validation rows ordered
`run_timestamp DESC`. Tombstones (`is_active='false'` on the
latest version per natural key) hide older active rows for that
key — matches singular `read_validation_history` semantics.

**Walk algorithm (per backend).** TWO-stage CTE — codex R1
BLOCKER #3 nailed why a single PARTITION BY `(metric, dim)` is
wrong: it would let a tombstone for partition A hide active
rows for partition B (same metric/dim, older partition). The
single-row variant at sqlite_storage.py:374-397 already gets
this right by partitioning per `(metric_name, dimension_value,
partition_ts)` first. Multi-row generalizes the same shape:

```sql
WITH per_partition AS (
  -- Stage 1: latest version per natural key (input_idx, partition_ts)
  SELECT *, input_idx,
    ROW_NUMBER() OVER (
      PARTITION BY input_idx, partition_ts
      ORDER BY run_timestamp DESC, validated_at DESC, run_id DESC
    ) AS pn
  FROM {table} JOIN keys_view USING (...)
  WHERE record_type='validation'
),
active_per_partition AS (
  -- Stage 2: drop tombstones, then re-rank per input_idx
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY input_idx
      ORDER BY run_timestamp DESC, validated_at DESC, run_id DESC
    ) AS kn
  FROM per_partition
  WHERE pn = 1 AND COALESCE(is_active, 'true') = 'true'
)
SELECT * FROM active_per_partition
WHERE kn <= ?
ORDER BY input_idx, kn   -- codex R3 HIGH #5: kn already encodes
                         -- (run_timestamp DESC, validated_at DESC,
                         -- run_id DESC) deterministically; ordering
                         -- by run_timestamp here would re-introduce
                         -- backend-defined ties.
```

**Why two stages.** Stage 1 collapses each partition to its
latest version (so a tombstone supersedes older actives at the
SAME partition). Stage 2 re-ranks survivors per key and applies
the row cap. A tombstoned partition contributes zero rows to
Stage 2; older partitions for that key remain visible. This
matches the singular `read_validation_history` semantics
verified at sqlite_storage.py:374-397.

**Tiebreak ordering rationale.** `validated_at` may be NULL on
some validation rows (legacy). `run_id` is always populated
(generated at engine entry) and provides a deterministic third
tiebreak. Without it, two rows with same `(run_timestamp,
validated_at=NULL)` produce backend-defined order, which masks
test flakiness across SQLite vs Spark.

**Input-key indexing (codex R2 HIGH #5).**
`monotonically_increasing_id()` is NOT safe — Spark only
guarantees ordering, not dense `0..n-1` IDs, and the Python
caller indexes into `keys[idx]` by position. Use Python-side
`enumerate(keys)` and pass the index column as part of the
keys-view rows:
- **Delta / ExternalCatalog**: build the keys DataFrame from
  `[(idx, k.dataset, k.validation, k.metric, k.dim) for idx, k in enumerate(keys)]`.
  The `idx` column is dense and stable.
- **SQLite**: build a CTE with `VALUES (idx0, k0…), (idx1, k1…), …`
  rows and join the system table against it on the four key
  fields. SQLite's `VALUES` is more portable than `UNNEST`
  (which SQLite doesn't support).
- **JDBC**: see Item 3's JDBC dialect note — predicate-OR path
  with Python-side keys DataFrame join (NOT a remote `VALUES`
  table; that's not portable across JDBC drivers).

**Backend SQL dialect notes.**
- SQLite: requires SQLite ≥ 3.25 for window functions (CPython
  3.10+ ships ≥ 3.37 — fine).
- Delta / ExternalCatalog (Spark SQL): `<=>` for null-safe
  equality on `dimension_value`; `IS NULL` on JDBC.
- JDBC: codex R2 HIGH #6 + R3 HIGH #4 — the JDBC backend is
  Spark-coupled and passes a SQL string to `spark.read.jdbc`;
  no parameter-binding path. Existing escape pattern at
  `qualifire/storage/jdbc_storage.py:420` and similar sites is
  inline `value.replace("'", "''")` — there is NO
  `_quote_sql_str` helper today. Plan: factor out a small
  internal helper `_quote_sql_str(s: str) -> str` in
  `jdbc_storage.py` that wraps the existing escape pattern, and
  use it in `read_validation_history_bulk`. **Do NOT use a
  `VALUES` keys table** — that's the Delta/SQLite path, not the
  JDBC remote-DB path (drivers vary). Algorithm:
  1. Build `WHERE record_type='validation' AND ((dataset='X1' AND validation='Y1' AND metric='Z1' AND ...) OR ...)`
     predicate string from `enumerate(keys)`.
  2. Pull rows with `spark.read.jdbc(..., predicates=[that_string])`.
  3. Build a Python-side `keys_df` (Spark DataFrame from
     `enumerate(keys)`) carrying the dense `input_idx`.
  4. Join the pulled rows against `keys_df` on the four key
     fields to attach `input_idx`.
  5. Apply Spark `Window.partitionBy("input_idx", "partition_ts")
     .orderBy(F.desc("run_timestamp"), F.desc("validated_at"),
     F.desc("run_id"))` for Stage 1, filter `is_active`, then
     re-rank by `input_idx` for Stage 2.
  6. `.filter(F.col("kn") <= limit)`, `.orderBy("input_idx", "kn")`,
     `.collect()`.

  Trade-off documented: JDBC pulls candidate rows broader than
  needed (no SQL-side limit pushdown) and ranks Spark-side. Perf
  acceptable — `limit=1` is the only hot-path consumer (notification
  suppression).

  **Preserve existing fallback (codex R4 HIGH).** Today's path
  at `qualifire/storage/jdbc_storage.py:1093-1113` wraps the
  one-shot JDBC pull in `try/except` and falls back to per-key
  `read_validation_history(...)` calls on any failure. The new
  predicate-OR + Spark `Window` path keeps the same try/except
  envelope: on exception, log type-only ("JDBC bulk read failed
  (%s); falling back to per-key reads") and iterate per-key
  using the (already lifted) `read_validation_history` with the
  new `limit > 1` support. Add a regression test (T3.7) where
  `_read_jdbc` is monkey-patched to raise — verify the
  per-key fallback returns correct results at `limit=3`.

**Tombstone invariant (clarified per codex R2 HIGH #4).**
Tombstones are scoped to a *partition*, not the whole key:
- Stage 1: per `(input_idx, partition_ts)` ROW_NUMBER picks the
  latest version of each partition (tombstone or active).
- Filter `is_active` between Stage 1 and Stage 2: a tombstoned
  partition contributes zero rows; sibling partitions with
  active latests survive.
- Stage 2: re-rank survivors per `input_idx` and apply `LIMIT`.

This matches the singular `read_validation_history` invariant
at sqlite_storage.py:374-397. If the operator wants "key K is
fully deleted", they must tombstone every partition for K — no
implicit cascade.

**Single-row caller behavior change (call out).** The notification
suppression caller uses `limit=1`. Today's bulk implementation
used `PARTITION BY (metric, dim)` (partition-blind), which made
a tombstone on ANY partition hide the whole key. v3+ aligns the
bulk path with the singular partition-aware path. Net result:
suppression now correctly considers each partition independently,
matching `read_validation_history(...)` semantics. No public
contract document promises the old behavior; this is alignment,
not a regression. Audit `qualifire/core/engine.py:1729` (the sole
caller) and add a regression test in
`tests/test_storage/test_bulk_history_contract.py` that pins
the new alignment.

**Backends to modify.**
- `qualifire/storage/sqlite_storage.py:401-430` — drop
  `if limit != 1: raise ValueError`. Generalize the existing
  `limit=1` CTE.
- `qualifire/storage/delta_storage.py:397-415` — same.
- `qualifire/storage/external_catalog.py:750-766` — same.
- `qualifire/storage/jdbc_storage.py:1028-1062` — same.

**Caller audit.** Search for every `read_validation_history_bulk(`
call. Confirm `limit=1` (today's only caller is the notification
suppression path). No caller changes in this PR — they remain at
`limit=1`.

**Test plan (Item 3) — shared cross-backend contract.**

Per codex R1 MEDIUM #9: a backend can silently mis-implement
limit/order/edge-cases under a thin per-backend test scope.
Author a single parametrized test class
`TestReadValidationHistoryBulkContract` in
`tests/test_storage/test_bulk_history_contract.py` that takes a
`storage` fixture per backend and runs T3.1-T3.6 against each.
Backends that cannot run in CI (Delta, JDBC, ExternalCatalog
when their drivers aren't available) are conditionally
skipped — but the contract remains the source of truth for
behavior.

- T3.1: `limit=1` regression — output identical to today's
  hard-enforced path (15 active rows in DB → 15 keys × 1 row).
- T3.2: `limit=3` returns up to 3 most-recent active rows per key,
  per-key ordered `run_timestamp DESC`.
- T3.3: Tombstone-hides-partition: key K has 5 partitions, the
  most-recent partition for K is tombstoned, the older 4 are
  active — `limit=3` returns the 3 newest **of the 4 surviving
  partitions** (NOT zero — partition-scoped tombstones don't
  hide siblings, per the two-stage CTE contract).
- T3.4: Tombstone-hides-key: key K has 5 versions of the SAME
  partition, the latest is a tombstone — `limit=3` returns
  ZERO rows for K (entire partition deleted).
- T3.5: Mixed keys — some all-tombstoned, some not — only the
  live ones return rows; result-set length matches.
- T3.6: `limit=0` raises `ValueError` (preserved); `limit=-1`
  raises (preserved); `limit=1` is the default, no change.
- T3.7: **JDBC bulk-pull fallback** (codex R4 HIGH preserved
  contract). Monkey-patch `JDBCStorage._read_jdbc` to raise.
  Call `read_validation_history_bulk(keys, limit=3)`.
  Assertions: result matches the per-key fallback path's
  output for `limit=3` exactly; warning log emitted with
  type-only message ("JDBC bulk read failed (%s); falling
  back to per-key reads"). JDBC backend only.

**Per-backend coverage.** All four backends (SQLite, Delta,
JDBC, ExternalCatalog) run T3.1-T3.6. T3.7 is JDBC-only. Spark-backed backends use
the existing in-memory test fixtures (`pyspark.sql.SparkSession.builder.master("local[2]")`)
the soft-delete tests already employ.

**Exit criteria (Item 3).** All four backends pass T3.* on the
relevant scope. No production caller flips to `limit > 1` in this
PR.

---

### 4. `metrics-backfill-and-soft-delete/shipped.md` drift fix

**Targeted edits.** Lines 96-107 of the file claim certain
symbols as shipped in PR #10 that actually shipped in PR #13.

Verified via `git log --diff-filter=A --follow -- <path>` and
`git log -p -S '<symbol>'`:

| Claim location | Actually shipped in |
|----------------|---------------------|
| `expected_metrics()` on validators (line 98) | PR #13 commit `9f05dc2` |
| `_add_backfill_tag` helper (line 103) | PR #13 (renamed `_tag_with_backfill`) |
| `backfill_eligibility.py` (line 104) | PR #13 commit `9f05dc2` (verified) |
| `read_collection_metric_at_partition` Protocol (line 107) | **PR #10** commit `41f9bda` (verified — claim correct) |
| `Qualifire.validate(skip_if_cached=...)` (impl behind kwarg, line 41) | API kwarg PR #10, engine wiring PR #13 |

**Replacement text (verbatim).** Section "Files Changed" gets a
prefix line:

> *Note: lines below describe the foundation slice. Items marked*
> *⓭ landed in the follow-up PR #13 ([backfill-api-and-wap-target-mode](../backfill-api-and-wap-target-mode/idea.md))*
> *— the foundation PR shipped only the scaffolding contract.*

Then ⓭ markers prefix:
- "`expected_metrics()` on threshold / historical / forecast
  validators (used by the future S5 cache pre-pass)" → ⓭
- "`_add_backfill_tag` helper for future S5" → ⓭ (also note rename)
- "`qualifire/core/backfill_eligibility.py` (new)" → ⓭

The `read_collection_metric_at_partition` claim stays — it's
correct.

**Deliverable.** A single docs commit that rewords the four
inaccurate claims as "scaffold added in PR #10, fully consumed
in PR #13" with cross-references. Verification step: run
`git log --diff-filter=A --follow -- qualifire/core/backfill_eligibility.py`
to confirm the introducing commit and reference its SHA in the doc.

**Test plan (Item 4).** None — pure docs. Validate by re-running
the verification grep commands listed above before/after.

**Exit criteria (Item 4).** Reviewer can grep `git log` for each
claimed symbol and find the SHA matches what shipped.md now says.

---

### 5. CLI error routing

**Where.** `qualifire/cli.py:419-437` — the summary block in
`_cmd_backfill`. Today every diff line goes to stdout via
`print(line)`. Errored diffs (`diff.status == 'errored'`) carry
operator-actionable failure context that should be greppable in
stderr.

**Edit.** Inside the `for diff in report.partitions:` loop:
```python
if diff.status == "errored":
    print(line, file=sys.stderr)
else:
    print(line)
```

**JSON mode untouched.** `--json` emits one line of JSON to
stdout; that path doesn't loop diffs. Operators piping `--json`
to `jq` get the full report on stdout, errored entries embedded
inside — no behavior change.

**Test plan (Item 5).**
- T5.1: Backfill with one errored anchor → capture stdout / stderr
  separately (subprocess test); errored line appears in stderr,
  succeeded lines in stdout, summary header in stdout.
- T5.2: `--json` mode unchanged — full report on stdout.

**Exit criteria (Item 5).** T5.* green; non-`--json` path is the
only branch touched.

---

### 6. Configurable `max_partitions` cap

**API surface.**
- `Qualifire.backfill(..., max_partitions: int = 10000)`.
- `qualifire backfill --max-partitions N` (default 10000).
- `run_backfill(..., max_partitions: int = 10000)`.
- `expand_partition_ts(...)` gains a `max_partitions: int` kwarg
  (default 10000) — replaces the hard-coded literal at
  `qualifire/core/backfill.py:82`.

**Validation.** `max_partitions >= 1`. Reject `0` / negative
with `QualifireConfigError`.

**Why a kwarg, not a constant.** Operators backfilling 5 years
of hourly partitions (~43800) need to lift the cap once, not
chunk by hand. Constant-with-monkey-patch is hostile to scripted
backfills.

**Test plan (Item 6).**
- T6.1: `expand_partition_ts(..., max_partitions=5)` raises on
  the 6th anchor; default 10000 unchanged.
- T6.2: `Qualifire.backfill(..., max_partitions=50000)` succeeds
  on a 30000-anchor range.
- T6.3: `--max-partitions 0` rejected.
- T6.4: Default kwarg value still 10000 (regression).

**Exit criteria (Item 6).** T6.* green; the literal `10000`
appears only as the default-arg value (`grep` audit).

---

## Cross-Cutting Tests

- `tests/test_backfill_parallelism.py` (new) — Items 1.
- `tests/test_backfill_partition_column_autodetect.py` (new) — Item 2.
- Extend `tests/test_storage/test_soft_delete_sqlite.py` and the
  three sibling backend test files — Item 3.
- Extend `tests/test_cli.py` (or new `tests/test_cli_backfill.py`)
  — Items 5, 6.
- Extend `tests/test_backfill_*` — Item 6 boundary cases.

## Docs Sync

- `docs/backfill_and_soft_delete.md` — add `--parallelism`,
  `--max-partitions` flags + auto-detect example.
- `docs/programmatic_api.md` — `Qualifire.backfill` signature
  table refresh.
- `examples/backfill_quickstart.py` — show `--parallelism 4`
  in a comment, not as a default.
- `docs/CHANGELOG.md` — Enhancement entry per item PLUS two
  explicit behavior-change notes:
  - **Compat note A (HS1 carve-out A):** "Backfill engine is
    now invoked once per anchor instead of once per metric.
    Per-anchor write/notification counts drop accordingly. No
    operator action required."
  - **Compat note B (HS1 carve-out B):** "`read_validation_history_bulk(limit=1)`
    now uses partition-aware semantics matching
    `read_validation_history`. Tombstones for partition P hide
    only P, not sibling partitions for the same key. Affects
    notification-suppression caller — surfaces become
    per-partition rather than per-key."

## Migration / Compat

- All new kwargs default to today's behavior. No deprecation
  warnings emitted. No version bump beyond patch.
- Storage Protocol contract for `read_validation_history_bulk`
  is **broadened, not narrowed** — `limit > 1` becomes legal but
  no caller is forced to use it. Backend compat preserved.

## Review Plan

After draft, run **2 adversarial self-passes + 2 codex plan reviews**.
Iterate. Do not commit between rounds — rewrite plan in place.

## Exit Criteria (overall)

1. All 6 items shipped.
2. ~25 new test cases (5-6 per item except #4) green.
3. Existing test suite (1432 tests on main) green; no regressions.
4. Docs / examples / CHANGELOG synced.
5. PR diff has clear per-item commit boundaries (1 commit per item)
   so reviewers can land them piecewise if needed.
6. Codex plan + impl reviews each return `PASS` with no BLOCKER /
   HIGH outstanding.

## Commit Granularity

Items 1 and 6 share `expand_partition_ts` and `run_backfill`
signatures — bundle them in one commit (Items 1+6 together).
Items 2, 3, 4, 5 each get their own commit. Six items total → 5
commits. Reviewer can revert any individual commit cleanly.

## Iteration Log

### v6 → v7 (after codex plan-review R5)
- BLOCKER #1: HS1 was internally inconsistent. Engine-call
  dedup IS a default-behavior change in serial mode (today's
  N redundant writes/notifications drop to 1 per anchor). v7:
  HS1 carve-out A explicitly names the change as intentional
  + operator-positive; T1.11 added to pin "engine + notifier
  fire exactly once per anchor in serial mode".
- BLOCKER #2: HS1 also conflicted with Item 3's
  partition-aware `limit=1` semantics. v7: HS1 carve-out B
  explicitly names this as an intentional alignment with the
  singular path. CHANGELOG migration note required.
- HIGH #3: T1.3 said `BackfillReport.errored == 1` after a
  Pass 1 failure — but v6's abort-anchor contract makes ALL
  metrics for that anchor errored. T1.3 rewritten: 3-anchor
  setup, anchor #2 fails Pass 1 → both its metrics errored
  (errored=2 for a 2-metric scope), no engine.run() for
  anchor #2, siblings complete normally.
- HIGH #4: T3.7 was referenced in JDBC fallback prose but
  the test plan only enumerated T3.1-T3.6. v7 adds explicit
  T3.7 bullet (monkey-patch `_read_jdbc` raise → per-key
  fallback at limit=3) + per-backend coverage note.
- MEDIUM #5: T1.1 instrumented the now-thin `_backfill_one`.
  v7 moves the barrier to the anchor worker (`_run_anchor_once`
  / `_process_anchor`) — which IS the parallelism boundary.

### v5 → v6 (after codex plan-review R4)
- BLOCKER: Stale `_effective_partition_column` /
  `_derive_partition_column(ds)` references at v5's "Search-and-
  replace audit" section directly contradicted the new
  `_resolve_partition_anchors(ds, run_config)` single-helper
  contract. v6 deletes that prose and replaces with a CI-grep
  guard requiring NO direct `ds.wap.partition_column` /
  `ds.partition_ts` reads outside the resolver.
- BLOCKER: Pass 1 metric failure was per-metric "errored"; engine
  ran for the whole anchor anyway → mixed-state writes. v6:
  ANY Pass 1 failure aborts the anchor; engine.run() skipped;
  every metric for the anchor reports `errored` with
  `pre-engine[X]: <reason>`. Sibling anchors unaffected.
- HIGH: JDBC fallback at jdbc_storage.py:1093-1113 (per-key
  fallback on bulk pull failure) was dropped in v5's new path.
  v6 preserves it: bulk path wrapped in try/except with the
  same log-type-only message; on failure, per-key reads via
  `read_validation_history(..., limit=N)` (using the new
  `limit > 1` support). T3.7 added.
- HIGH: `_read_severity_before` calls
  `read_validation_history(validation_name="")` and returns
  None today — pre-existing bug. v6 EXPLICITLY scopes it OUT
  via new HS8; captures it as a separate backlog idea
  (`backfill-severity-before-broken-readback`) to be filed in
  the follow-ups capture phase. Refactor preserves existing
  behavior; classification semantics not touched.

### v4 → v5 (after codex plan-review R3)
- BLOCKER #1: Stale worker-skeleton prose still showed
  `_backfill_one(...)` per-metric calls (which would re-run
  the engine N times per anchor). v5 replaces with explicit
  TWO-pass worker: Pass 1 reads pre-engine state + writes prior
  tombstones; SINGLE engine.run(); Pass 2 builds per-metric
  diffs from the cached `EngineResult`. `_backfill_one` is
  internal-only — its signature is changed to take a cached
  result.
- BLOCKER #2: v4 ran the engine first → broke `soft_delete_prior`
  (would tombstone freshly-collected rows). v5 splits worker
  into pre-engine + post-engine passes; tombstone writes happen
  in Pass 1, before engine.run().
- BLOCKER #3: Stale Item 2 prose mentioned
  `_effective_partition_column` / `_derive_partition_column(ds)`
  helpers (no `run_config` parameter). Deleted; `_resolve_partition_anchors(ds, run_config)`
  is the SOLE helper, run_config always threaded.
- HIGH #4: JDBC plan internally inconsistent — referenced a
  non-existent `_quote_sql_str` helper and conflicted with the
  `VALUES` claim. v5: factor inline `replace("'", "''")` into
  `_quote_sql_str(s)`; predicate-OR + Spark-side keys-DataFrame
  join + Spark `Window` ranking. No remote `VALUES` table.
- HIGH #5: Multi-row CTE final ORDER BY changed from
  `(input_idx, run_timestamp DESC)` to `(input_idx, kn)` —
  `kn` already encodes the deterministic tiebreak; ordering by
  run_timestamp re-introduces backend-defined ties.
- MEDIUM #6: KeyboardInterrupt contradiction — v4 had two
  contradicting passages. v5 commits to "exception propagates,
  no partial report". T1.8 already says this; the
  doc-note now matches.

### v3 → v4 (after codex plan-review R2 + adversarial R2)
- BLOCKER #1: Fan-out granularity was correct, but per-anchor
  worker still called `_backfill_one` per metric — which calls
  `_run_partition` running the full engine. v4 introduces
  `_run_anchor_once(...)`: engine runs ONCE per (scope, anchor),
  metric-loop extracts from cached result. Cuts engine work from
  N to 1 per anchor (perf win, parallelism correctness preserved).
  Sample-validator skipped block kept inside the worker.
- BLOCKER #2: `effective_partition_ts(ds, run_config)` only
  reached preflight, not `_build_partition_dataset`. v4 adds
  `_resolve_partition_anchors(ds, run_config)` returning
  `(effective_partition_column, effective_partition_ts_expr)`;
  ALL backfill consumers route through this resolver. Closes
  the WAP filter construction gap AND the missed non-WAP gap.
- HIGH #3: Ordering test T1.2 had a stale lex-sort claim. v4
  drops the lex sort entirely — `seq_idx` only, byte-for-byte
  serial parity.
- HIGH #4: Multi-row CTE prose had a leftover "tombstoned keys
  return zero" invariant from v2. v4 rewrites to clarify
  partition-scoped tombstones.
- HIGH #5 (new): `monotonically_increasing_id()` is not dense
  in Spark. Use Python `enumerate(keys)` to materialize idx into
  the keys DataFrame. SQLite uses `VALUES (...)` (no UNNEST).
- HIGH #6 (new): JDBC backend has no parameter-binding path —
  uses Spark's `spark.read.jdbc` with SQL string. Plan now uses
  quote-escaped SQL + Spark-side `Window` ranking instead of
  `?` markers.
- HIGH #7 (missed): Non-WAP backfill filter also ignored run-
  level `partition_ts`. Folded into `_resolve_partition_anchors`.
- T1.7 (notification visibility) — `BackfillReport.notifications_suppressed`
  + stderr advisory.
- T1.9 (engine-call count regression).
- T1.10 (suppression visibility regression).
- T2.10 (non-WAP run-level partition_ts).
- Single-row bulk semantic alignment with singular path called
  out explicitly; suppression caller audit added.

### v2 → v3 (after codex plan-review R1)
- BLOCKER #1: Work-unit was `(scope, anchor, metric)`. Codex
  flagged this against HS2 + the post-loop sample-skipped
  emission. Now `(scope_idx, scope, anchor_idx, anchor)`; per-
  metric loop stays inside the worker. Sample-skipped block
  also runs inside the worker once per anchor.
- BLOCKER #2: SQLite shared connection raises `ProgrammingError`
  for reads from non-creator threads, not just writes. v2 said
  "wrap reads + writes" but didn't list every method. v3
  explicit: lock every public method on `SQLiteStorage`.
- BLOCKER #3: Multi-row CTE was wrong — single PARTITION BY
  `(metric, dim)` would leak tombstones across partitions for
  the same key. v3 adopts the singular path's
  `(metric, dim, partition_ts)` Stage-1 dedup, then re-rank per
  key in Stage 2. Documented invariant: tombstone hides the
  partition it tombstones, not sibling partitions.
- HIGH #4: Delta + ExternalCatalog `read_validation_history_bulk`
  uses `_qf_bulk_keys_{hash(tuple(keys)) % (1<<31)}` — collision
  race under concurrent same-key prefetches. v3: `uuid.uuid4().hex`
  per call + try/finally drop.
- HIGH #5: BackfillReport.partitions order under parallelism —
  carrying `seq_idx` from the original linearization preserves
  exact serial-mode order; not a metric-first lexicographic sort.
- HIGH #6: `_SAFE_NAME_RE` accepts leading digits, dots, hyphens
  — unsafe for unquoted SQL interpolation. New stricter
  `_BARE_IDENT_RE = ^[A-Za-z_][A-Za-z0-9_]*$` for auto-detect
  ONLY. Existing explicit-set validator unchanged.
- MEDIUM #7: Auto-detect now uses `effective_partition_ts(ds,
  run_config)` — picks up run-level inheritance. T2.8 added.
- MEDIUM #8: HS6 contradicted Item 6 (kwarg pushed into
  `expand_partition_ts`). HS6 reworded to allow exactly that
  one keyword-only kwarg.
- MEDIUM #9: Cross-backend test scope — was T3.1+T3.3 for
  Delta/JDBC/EC. Now: shared parametrized contract
  T3.1-T3.6 across all four backends.

### v1 → v2 (after adversarial pass R1)
- BLOCKER: SQLite single persistent connection is not
  thread-safe; original "lock writes only" approach was wrong.
  v2: `check_same_thread=False` + serialize ALL SQLite I/O via
  `_lock`. Spark backends untouched.
- HIGH: Fan-out granularity moved from `(scope, anchor, metric)`
  to `(scope, anchor)` — preserves per-anchor write ordering.
- HIGH: Notification side-effects under parallelism — force
  `notifiers={}` when `parallelism > 1`.
- HIGH: `_SAFE_NAME_RE` accepts only bare unqualified
  identifiers; explicitly rejects dot-qualified and backtick-
  quoted forms.
- HIGH: T1.1 wall-clock assertion replaced with a barrier-based
  concurrency proof (CI-stable).
- HIGH: Item 4 doc-drift claims now verified via git log;
  `read_collection_metric_at_partition` is in fact PR #10
  (claim was correct, dropped from drift list).
- MEDIUM: Per-backend test coverage upgraded — Delta/JDBC/EC
  each get T3.1+T3.2+T3.3 (was T3.1+T3.3 only).
- MEDIUM: SQL ordering tiebreak adds `run_id DESC` for
  deterministic order under NULL `validated_at`.
- MEDIUM: KeyboardInterrupt semantics documented +
  `cancel_futures=True` on shutdown.
- MEDIUM: Commit granularity — Items 1+6 bundled (shared
  signatures); 5 commits total.

