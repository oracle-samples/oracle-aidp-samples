# infa_compat: supported operations

This is the **closed contract** the generator (both the deterministic
template generator and the LLM-first prompt) is constrained to call.
Everything below is real, tested code; nothing here is prose describing
intended behavior. If a mapping needs a Class C semantic not listed
here, that is a gap to fix in this library, not a reason to let the
generator improvise Spark it wasn't given.

Class A constructs (`IIF`, `DECODE`, `NVL`/`NVL2`, `TO_DATE`/`TO_CHAR`,
`SUBSTR`/`INSTR`, `LTRIM`/`RTRIM`/`TRIM`, `LPAD`/`RPAD`,
`REPLACECHR`/`REPLACESTR`, `IS_NUMBER`/`IS_DATE`/`IS_SPACES`,
`DATE_DIFF`, `ADD_TO_DATE`, `ADD_MONTHS`, `TRUNC`, `ROUND`, `REG_MATCH`,
casts, aggregates) are **not** in this contract -- they stay inlined by
`infa2aidp.converters.expression_converter`. Do not call this
library for those; there is nothing to call.

Mirrors the scope the sibling Databricks-migrator plugin's `aidp_compat`
occupies: shim the stateful/environmental surface, never arithmetic.

Version: `infa_compat.__version__` -- generated notebooks must assert
this matches what they were generated against ("Accepted
costs: version skew") -- a notebook generated against 0.3 running on a
cluster with 0.2 fails confusingly otherwise.

---

## `sequence.py` -- Sequence Generator (`NEXTVAL`)

```python
infa_compat.sequence(
    spark, name, target_catalog_type="delta",
    start=1, increment=1, cache=1000, cycle=False, max_value=None,
    **backend_kwargs,
) -> Sequence
```

- `target_catalog_type`: `"delta"` (default) or `"adw"` -- **always
  explicit, never inferred** from a table name or connection string.
  `"adw"` requires `connection=<oracledb connection>` in
  `backend_kwargs`; it wraps a real Oracle `SEQUENCE` object that must
  already exist (provisioning it is a deployment-time DDL concern, out
  of scope here).
- Returned `Sequence` has `.next_value() -> int`, `.next_values(n) ->
  list[int]`, and `.assign(df, column) -> DataFrame` (forces a `count()`
  action -- unavoidable, since the reservation size must be known
  first).
- **Restart-safe, not gap-free.** Concurrent/restarted callers for the
  same `name` never collide; a crash mid-cache-block leaves a gap
  (documented cost, mirrors Informatica's own cache-size trade-off).
- `cycle=True` remaps the delivered value into `[start, max_value]` by
  modulo arithmetic; the backend's own persisted counter is never reset.
  This does NOT give two independently-restarted jobs the identical
  wraparound sequence in lockstep -- documented gap, see the module
  docstring.
- **UNVERIFIED against a live Spark/Delta or ADW runtime.**

## `lookup.py` -- Lookup transformation

```python
infa_compat.cached_lookup(
    df, lookup_df, on, policy="first", cache="static", order_by=None,
) -> DataFrame
```

- `policy`: `"first"` / `"last"` / `"error"` / `"all"` -- **always
  explicit**, no default silently narrower than what was asked.
  `"error"` raises `LookupMultipleMatchError` (forces an eager count
  check). `"all"` fans out (no de-dup) rather than picking a row.
  Informatica's "Use Any Value" has no distinct mapping here -- treat it
  as `"first"` at conversion time (documented assumption).
- `cache`: only `"static"` is implemented. `"dynamic"` raises
  `NotImplementedError` -- it is a different, stateful algorithm
  (Informatica's `NewLookupRow` insert/update semantics), never silently
  run as a static broadcast join.
- Join is always `how="left"` -- NULL keys and genuinely unmatched rows
  are preserved with NULL lookup columns, never dropped.
- **UNVERIFIED against a live Spark runtime.**
- **Open question, NOT settled: dotted join-key names.** Three corpus
  fixtures name a real Lookup's join key with a literal dot (e.g.
  `"SQ_EMPLOYEES.EMP_ID"` -- Informatica's own Source-Qualifier-instance
  naming, carried through unchanged by the generator's
  `withColumnRenamed` step). `cached_lookup` joins via Spark's
  `on=[...]` named-column list form; the previous inline implementation
  joined via bracket indexing (`df["SQ_EMPLOYEES.EMP_ID"]`). Whether
  Spark resolves that literal dotted string identically under both
  forms is genuinely unverified -- a dot in a column-name string is
  also valid Spark syntax for a qualified/nested reference, and PySpark
  is not installed in this environment to settle it. **Needs
  verification on a live Spark session.** No defensive normalization
  (e.g. stripping an `<instance>.` prefix) has been applied -- see
  `lookup.py`'s docstrings for why that was considered and rejected as
  not obviously safe (this function never aliases `lookup_df`, so there
  is no reliable alias to match a stripped prefix against).

## `params.py` -- `$$PARAM` / `$$$SessStartTime`

```python
infa_compat.load_parameter_file(path) -> dict[str, dict[str, str]]
infa_compat.ParameterScope.from_file(path) -> ParameterScope
infa_compat.activate(scope)
infa_compat.param(name, default=None) -> Any
infa_compat.sess_start_time() -> datetime.datetime
```

- Scope precedence, most-specific first:
  `workflow > worklet > session > mapping` (`SCOPE_PRECEDENCE`).
- `param()` raises `ParameterNotFoundError` if undefined at every scope
  and no default given.
- `sess_start_time()` is fixed once per session via
  `ParameterScope.mark_session_start()` -- calling it twice raises
  rather than re-stamping.
- Pure Python/stdlib -- no PySpark dependency, no "unverified" caveat.

## `scd2.py` -- SCD Type 2 effective-dating

```python
infa_compat.scd2_merge(
    spark, target, source, keys, effective_from, effective_to,
    current_flag, high_date="9999-12-31", *,
    target_catalog_type="delta", current_flag_values=("Y", "N"),
    jdbc_options=None, adw_connection=None, staging_table=None,
) -> Scd2MergeResult
```

- Two-step expire-then-insert, for BOTH target types:
  - `"delta"` (default): a real `DeltaTable.forName(target).merge()` to
    expire matched current rows, then `source.write.mode("append")`.
  - `"adw"`: stage `source` via a JDBC **overwrite** into a staging
    table, then run the expire `MERGE INTO` + a plain `INSERT INTO ...
    SELECT` as real SQL from the driver via `python-oracledb`. Requires
    `jdbc_options=` and `adw_connection=`.
- **Assumes `source` already contains only rows needing a new current
  version** (new keys + changed keys) -- the same assumption
  `write_strategies.py`'s pre-existing `_scd2_write` documents. Violating
  it wastes work, it does not lose data.
- `keys` is required and must be a natural/business key, never a
  surrogate.
- Never assumes Delta for the ADW path; never downgrades to a plain
  overwrite for either path.
- **UNVERIFIED against a live Spark/Delta or ADW runtime.**

## `update_strategy.py` -- `DD_INSERT`/`DD_UPDATE`/`DD_DELETE`/`DD_REJECT`

```python
infa_compat.apply_update_strategy(df, strategy_col="DD_STRATEGY") -> UpdateStrategyResult
infa_compat.write_update_strategy(
    result, target, keys, reject_sink, *,
    target_catalog_type="delta", spark=None,
    jdbc_options=None, adw_connection=None, staging_table=None,
)
```

- `apply_update_strategy` only splits (`.inserts`/`.updates`/`.deletes`/
  `.rejects`) -- no action forced, no write performed.
- **`strategy_col` MUST be an INTEGER-typed column holding the
  `UpdateStrategyCode` values (`DD_INSERT=0`, `DD_UPDATE=1`,
  `DD_DELETE=2`, `DD_REJECT=3`) -- NEVER the Informatica expression-
  language STRING names.** These filters compare against integer
  literals, so a string-typed column matches none of them:

  ```python
  # RIGHT -- integer codes, via the enum:
  df = df.withColumn(
      "DD_STRATEGY",
      F.when(some_cond, F.lit(int(infa_compat.UpdateStrategyCode.DD_INSERT)))
       .when(other_cond, F.lit(int(infa_compat.UpdateStrategyCode.DD_UPDATE)))
       .otherwise(F.lit(int(infa_compat.UpdateStrategyCode.DD_REJECT))),
  )
  result = infa_compat.apply_update_strategy(df, strategy_col="DD_STRATEGY")
  ```

  ```python
  # WRONG -- DO NOT DO THIS. Informatica's own DD_INSERT/DD_UPDATE/
  # DD_DELETE/DD_REJECT names LOOK like the right thing to write here,
  # but they are STRING labels. Filtering a string column against the
  # integer codes matches nothing on EVERY partition: apply_update_strategy
  # would (before the guard below existed) silently hand back four EMPTY
  # DataFrames -- no error, no rows, no signal.
  df = df.withColumn(
      "DD_STRATEGY",
      F.when(some_cond, F.lit("DD_INSERT")).otherwise(F.lit("DD_REJECT")),
  )
  ```

  `apply_update_strategy` now checks `strategy_col`'s Spark dtype via
  `df.dtypes` before filtering and raises
  `infa_compat.NonIntegerStrategyColumnError` (a `TypeError`) if it is
  not one of `tinyint`/`smallint`/`int`/`bigint` -- a loud failure
  instead of the four-empty-partitions silent failure above. (If
  `strategy_col` is missing from `df` entirely, this check is a no-op
  and Spark's own `AnalysisException` from the subsequent `.filter()`
  is the signal instead.)
- `write_update_strategy` requires `reject_sink` -- **no default** --
  either a table name (appended to) or a callable. Rejects are never
  dropped.
- `"delta"`: UPDATE partition uses `whenMatchedUpdateAll()` with no
  insert clause; DELETE uses `whenMatchedDelete()`; INSERT
  merge-inserts. An UPDATE/DELETE for a non-existent key is correctly a
  no-op, never silently turned into an insert.
- `"adw"`: INSERT is a plain JDBC append; UPDATE/DELETE stage-then-merge
  (update-only / delete-only Oracle `MERGE`). Requires `jdbc_options=`
  and `adw_connection=`, and requires non-empty `keys` -- an ADW
  UPDATE/DELETE with no keys raises rather than falling back to a
  full-table overwrite.
- **UNVERIFIED against a live Spark/Delta or ADW runtime.**

## `datemask.py` -- date-mask conversion (shared seam)

```python
infa_compat.to_java_format(infa_mask: str) -> str
```

Class A itself (pure, no state) -- listed here because
`expression_converter.py` imports this rather than keeping its own copy
of the token table. Fully tested, no PySpark involved.

## `decode.py` -- DECODE fallthrough (shared seam)

```python
infa_compat.decode_fallthrough(value, *pairs, default=None) -> Any
infa_compat.split_pairs_and_default(values) -> tuple[list[tuple], Any]
infa_compat.build_when_chain(value_expr, pairs, default_expr) -> str
```

`decode_fallthrough` is the one Class C entry point (a driver-side
Python evaluator for a DECODE-shaped rule, e.g. config resolution) --
`build_when_chain` is what `expression_converter.py` imports for the
inline Class A emission. Fully tested, no PySpark involved.

---

## What is explicitly NOT here

- **Pre/post-session SQL** (`session_sql` in the table) is not
  yet implemented in this package -- out of scope for, tracked
  separately.
- **Dynamic lookup cache** (`cache="dynamic"`) -- raises
  `NotImplementedError`, not silently downgraded.
- **Inline-expansion mode for Class C** -- every helper here is a thin,
  single-purpose wrapper that could in principle be pasted inline
  mechanically, but that mode itself does not exist yet (
  "Standing rules": build it only if a customer actually refuses a
  cluster dependency, and generate it from this source, never
  hand-author a second copy).
