# Spark 3.5 → Spark 4: the measured differences

Companion to [`spark-4-upgrade.md`](spark-4-upgrade.md), which explains why
the upgrade matters and what the generator does about it. This file is the
evidence: every behavioural difference that actually shows up in code this
tool emits, measured rather than recalled.

**Measured 2026-09-29 on pyspark 4.2.0.** Method: take every expression the
converter emits, evaluate it twice on the same rows — once with the settings
Spark 3.5 ships (which generated notebooks pin), once with the settings
Spark 4 ships — and record only where the two disagree. Edge cases were put
in the data deliberately: a zero divisor, a non-numeric string, a 20-digit
value that overflows an `INT`, an invalid date, and NULLs.

**58 expressions compared · 41 identical · 17 differed.** Three of the 17
are nondeterministic by nature (`SYSDATE`, `SYSTIMESTAMP`, `UUID_STRING` —
they differ between any two calls), leaving **14 real differences**.

**AIDP was still on Spark 3.5.0 on every cluster in the tenancy when this
was measured.** Re-check before acting; the upgrade was reported as
imminent from 2026-09-23 onward.

## What Spark 4 actually ships

Read off pyspark 4.2.0 rather than from release notes:

| Setting | Spark 3.5 | Spark 4 |
| --- | --- | --- |
| `spark.sql.ansi.enabled` | `false` | **`true`** |
| `spark.sql.storeAssignmentPolicy` | `LEGACY` | **`ANSI`** |
| `spark.sql.execution.arrow.pyspark.enabled` | `false` | `true` |
| `spark.sql.legacy.timeParserPolicy` | `CORRECTED` | `CORRECTED` (unchanged) |
| `spark.sql.legacy.charVarcharAsString` | `false` | `false` (unchanged) |
| `spark.sql.legacy.allowNegativeScaleOfDecimal` | `false` | `false` (unchanged) |
| `spark.sql.ansi.enforceReservedKeywords` | `false` | `false` (unchanged) |
| `spark.sql.ansi.doubleQuotedIdentifiers` | `false` | `false` (unchanged) |

Only the first two reach generated code. The Arrow default cannot, for the
reason in "Surfaces that are clean" below.

Cluster-side, not ours: Spark 4 requires **Java 17+** and **Python 3.9+**.

## The 14 differences

Every one of these **returns a value on 3.5 and raises on Spark 4**, and
every one traces to `spark.sql.ansi.enabled`. The 3.5 column is the
behaviour Informatica also has — a bad value becomes NULL and the run
continues — so on Spark 4 these are **aborted jobs, not wrong numbers**.

### Division and modulo by zero

| Informatica | Emitted PySpark | 3.5 | Spark 4 |
| --- | --- | --- | --- |
| `A / B` | `F.col('A') / F.col('B')` | `5.0`, `NULL` | `DIVIDE_BY_ZERO` |
| `ROUND(A / B, 2)` | `F.round(F.col('A') / F.col('B'), 2)` | `5.0`, `2.33` | `DIVIDE_BY_ZERO` |
| `ABS(A / B)` | `F.abs(F.col('A') / F.col('B'))` | `5.0` | `DIVIDE_BY_ZERO` |
| `MOD(A, B)` | `(F.col('A') % F.col('B'))` | `0`, `NULL` | `REMAINDER_BY_ZERO` |

Any arithmetic wrapping a division inherits this — the three above are the
shapes the corpus happens to contain, not the full set.

### Malformed value cast to a number

| Informatica | Emitted PySpark | 3.5 | Spark 4 |
| --- | --- | --- | --- |
| `TO_FLOAT(S)` | `F.col('S').cast('double')` | `NULL` | `CAST_INVALID_INPUT` |
| `TO_DECIMAL(S)` | `F.col('S').cast('decimal(38,10)')` | `NULL` | `CAST_INVALID_INPUT` |
| `TO_DECIMAL(S, 2)` | `F.col('S').cast('decimal(38,2)')` | `NULL` | `CAST_INVALID_INPUT` |
| `IS_NUMBER(S)` | `…cast('double').isNotNull()` | `False` | `CAST_INVALID_INPUT` |

`IS_NUMBER` is the sharpest case. Its entire job is to answer "is this a
number", and on Spark 4 asking the question about a non-number aborts the
job instead of answering `False`.

### Numeric overflow on cast

| Informatica | Emitted PySpark | 3.5 | Spark 4 |
| --- | --- | --- | --- |
| `TO_INTEGER(S)` | `F.round(…cast('double')).cast('int')` | `2147483647` | `CAST_OVERFLOW` |
| `TO_INTEGER(S, TRUE)` | `…cast('double').cast('int')` | `2147483647` | `CAST_OVERFLOW` |
| `TO_BIGINT(S)` | `F.round(…cast('double')).cast('long')` | `9223372036854775807` | `CAST_OVERFLOW` |

Note that 3.5 is not benign here either: it saturates at the type maximum,
which is a wrong number rather than a NULL. Spark 4 raising is arguably
better; it is still a behaviour change on the upgrade day.

### Date parsing

| Informatica | Emitted PySpark | 3.5 | Spark 4 |
| --- | --- | --- | --- |
| `TO_DATE(S, 'MM/DD/YYYY')` | `F.to_timestamp(F.col('S'), 'MM/dd/yyyy')` | `NULL` | `CANNOT_PARSE_TIMESTAMP` |
| `IS_DATE(S, 'MM/DD/YYYY')` | `F.to_timestamp(…).isNotNull()` | `False` | `CANNOT_PARSE_TIMESTAMP` |

Measured on both an unparseable string (`'abc'`) and a structurally valid
but impossible date (`'13/45/2024'` — month 13). Spark 4 raises on both;
3.5 returns NULL, which is what Informatica does.

`IS_DATE` has the same problem as `IS_NUMBER`: the predicate cannot survive
the value it exists to reject.

## All 14 are already covered

The generated setup cell pins both settings rather than inheriting them:

```python
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")
```

Two tests in `tests/test_expression_execution.py` turn ANSI back **on** and
require the failure, so the pins cannot decay into decoration.

## The write path, and a pin that is probably wrong

`storeAssignmentPolicy` affects the **write**, which no expression test
touches. Inserting `3000000000` into an `INT` column:

```
LEGACY (3.5 default, what we pin) -> stored -1294967296
ANSI   (Spark 4 default)          -> CAST_OVERFLOW_IN_TABLE_INSERT
```

`-1294967296` is a silent two's-complement wraparound: not a NULL, not a
saturated maximum, a **negative number where the source had a positive
one**, written to the target with no error anywhere.

So the `storeAssignmentPolicy=LEGACY` pin preserves silent data corruption.
It was added alongside the ANSI pin on the assumption that both pins were
protective; for the write path that is backwards, and Spark 4's default is
the better behaviour.

It is probably also the more *faithful* behaviour. Informatica rejects an
overflowing row to the error/bad file rather than wrapping it — **general
Informatica knowledge, not verified against a running instance**, and worth
confirming before acting, because it argues for dropping this pin while
keeping the ANSI one.

This also corrects a row in `spark-4-upgrade.md`, which described the 3.5
write behaviour as "truncates / nulls". Measured, it wraps.

**Decided (2026-10-02): the pin stays, and the write became loud instead.**

Dropping the pin was the wrong lever. Migrated *expression* logic depends on
permissive evaluation -- that is what the 14 differences above are about --
so unpinning ANSI-adjacent behaviour to fix a write problem would break the
read path to fix the write path.

Instead the generated notebook now counts, before every write, the rows
whose value does not fit the target's **declared** precision, and raises
with the column, the count and the declared `NUMBER(p,s)`. The export
carries precision and scale for every target column, so the limit is
derivable rather than guessed.

It raises rather than wrapping or dropping. Informatica would have sent the
row to the session's reject file and carried on, which is neither of Spark's
behaviours; reject routing is modelled in `infa_compat.update_strategy` for
the update-strategy path but is not available on every write, so stopping
with the column named is the honest default. Writing a negative number where
the source had a positive one is the one outcome with no defence.

Verified by execution, not emission: the check is run against an overflowing
DataFrame on a real Spark session and must raise, and against boundary
values (999 in `NUMBER(3,0)`) and must not -- a check that fires on good
data gets switched off. See `tests/test_write_strategies.py`.

## Surfaces that are clean

Checked, and no action needed:

- **Nothing the converter emits was removed in Spark 4.** All 58 probe
  expressions converted and executed on 4.2.0. The existing API guard in
  `tests/test_spark_api_compatibility.py` covers the opposite direction —
  emitting something too *new* for 3.5.
- **No Arrow or pandas exposure.** All 12 generated corpus notebooks scanned
  for `toPandas`, `pandas_udf`, `applyInPandas`, `mapInPandas`, `.rdd`,
  `SQLContext`, `registerTempTable`, `selectExpr`, `udf(` and `char` casts:
  **zero hits for every one**. Spark 4 enabling Arrow by default cannot
  change behaviour in code that never crosses into pandas.
- **No legacy-config dependence.** No generated notebook sets any
  `spark.sql.legacy.*` flag other than the two pins above.
- The other flipped or unchanged defaults in the table above touch nothing
  the generator produces.

## Reproducing this

**The probe is a suite test: `tests/test_spark4_ansi_differences.py`.** It
runs on every suite run, so this inventory cannot decay into a measurement
that was true once.

For each expression the converter emits, it evaluates twice on the same rows
-- once with `spark.sql.ansi.enabled=false` (3.5's default, which generated
notebooks pin) and once with it `true` (Spark 4's default) -- on rows
containing a zero divisor, a non-numeric string, an overflowing value, an
invalid date and NULLs. Each of the 14 above must still return under the
permissive setting and raise under the strict one.

It also pins the other direction: 15 expressions believed ANSI-*neutral*
must agree value-for-value under both settings. If one starts differing, the
pin has become load-bearing for a family this document does not cover.

The count stated at the top of this file is asserted against the probe, and
the three nondeterministic expressions are asserted *absent* -- they differ
between any two calls, so counting them as ANSI differences would inflate
the inventory.

A new difference appearing is a finding: add it to the probe and to this
file, then decide whether the generated notebook still covers it. One
disappearing means the converter changed and this file is stale.
