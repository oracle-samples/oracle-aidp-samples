# Whole-stage codegen — when generated code is *slower*

Routes here when: a stage is **CPU-bound, not shuffle/IO-bound**; a wide `groupBy(...).agg(...)`
(many grouping columns + many aggregates) dominates task time; `HashAggregate` build time is
>90% of task duration; task `p50 ≈ p100` (uniform, not skew); result cardinality is tiny vs input
rows. **Core idea:** whole-stage codegen fuses an operator chain into one Java method and almost
always helps — but a *very wide aggregation* produces a method so large the JVM JIT (C2) refuses to
optimize it, so the generated path runs **slower** than Spark's non-codegen fallback. Fix is to push
just the wide stage to fallback by **lowering `spark.sql.codegen.maxFields`** — not by disabling
codegen globally. Target: open-source Apache Spark 3.5.0.

---

## Background: what whole-stage codegen does (and why it normally wins)

A Spark SQL / DataFrame query is compiled: logical plan → physical plan → **Java source → JVM
bytecode**. Spark generates the Java code; the JVM's own tiered compiler (interpreter → C1 → C2)
decides how to compile it at runtime. There are two code-generation strategies:

- **Volcano / iterator model** (the old default): every operator (Scan, Filter, Project, Aggregate)
  is its own object implementing `next() -> Row`. The aggregate calls `next()` on the project,
  which calls `next()` on the filter, which calls `next()` on the scan — a chain of **virtual
  dispatches** (runtime method-resolution), one per operator per row. Small methods, easy for the
  JVM to JIT, but virtual dispatch is per-row overhead and is not cache-friendly.

```java
// Volcano: each operator is a separate object; per-row virtual dispatch
class Aggregate extends Operator {
  Row next() { Row r = child.next(); /* virtual dispatch */ updateHashTable(r); ... }
}
class Filter extends Operator {
  Row next() { Row r = child.next(); /* virtual dispatch */ if (predicate(r)) return r; }
}
```

- **Whole-stage codegen** (`spark.sql.codegen.wholeStage=true`, the default since Spark 2.x):
  fuses the whole operator chain in a stage into **one Java method** with one tight loop. Scan →
  filter → project → aggregate all inline, no virtual dispatch.

```java
// Whole-stage: scan + filter + aggregate fused into one loop, no virtual dispatch
void processNext() {
  while (scan.hasNext()) {
    InternalRow r = scan.next();
    if (!r.isNullAt(2) && r.getLong(2) > 100) {          // Filter INLINED
      int hash = hashFunc(r.getLong(0));                  // Aggregate INLINED
      UnsafeRow buf = hashMap.lookup(hash);
      buf.setLong(0, buf.getLong(0) + val);
    }
  }
}
```

**Why it normally wins:** one tight loop with no virtual dispatch lets the JVM's **C2 compiler**
optimize it toward near-native speed — direct jumps instead of virtual calls, data stays in CPU
registers, branch prediction exceeds 99%, instructions fit in the instruction cache. For most
queries codegen is a clear win; **prefer DataFrame/SQL over RDDs** precisely because RDD code bypasses
Catalyst and codegen.

> Confirm codegen is active for a stage: look for `WholeStageCodegen (n)` wrapper nodes in
> `explain(mode="formatted")` or in the Spark UI SQL plan. (Note: a `formatted` plan does not always
> print the wrapper for in-memory-relation paths — verify from the SQL tab / generated-code dump if
> in doubt, see "Detect in Spark UI" below.)

---

## When generated code loses: very wide aggregation

**What:** A `groupBy` with many columns plus many aggregate expressions makes the fused method huge.
The JVM has a hard limit — **methods larger than ~8,000 bytecodes are never compiled by C2** — so the
generated aggregation runs interpreted or only C1-compiled, and you *lose* the optimization codegen
was supposed to deliver.

**Why it matters (the mechanism):** as the fused method grows past the JIT size limit:
- **C2 refuses to compile it** — the hottest loop in the query never gets the top-tier optimizer.
- **Register spilling** — too many live variables fight for too few CPU registers; values get pushed
  to the stack.
- **Branch-prediction rate drops** — long if/else null-check chains (one per field) defeat the
  predictor.
- **Instruction cache misses** — the method is too big to stay resident in the i-cache.

Net effect: a **CPU-bound** stage. Scan volume and shuffle are unchanged; the extra time is pure
per-row execution overhead in the aggregate, multiplied by every row.

**The counterintuitive insight — "just-under-`maxFields` is *slower* than just-over":**
Spark only generates whole-stage code for a stage when its field count is **below
`spark.sql.codegen.maxFields` (default 100)**; at or above that, it **auto-falls back** to the
non-codegen (Volcano) path. So two near-identical pipelines can diverge sharply:

- A pipeline with **>100 fields** → codegen **auto-disabled** → runs the fallback → **fast**.
- A near-identical pipeline with fields **just under 100** → codegen **kicks in**, builds a huge
  method past the JIT limit, **performs poorly** → **slow**.

The slower pipeline is slower *because* codegen turned on. The fix is to make the slow one behave
like the fast one: lower `maxFields` so the wide stage also falls back.

**Patterns (use when):**
- Wide `groupBy(...).agg(...)`: `group_cols + agg_outputs ≳ 100` (the heuristic below).
- Stage is **CPU-bound**, not shuffle/IO-bound — `HashAggregate` build time dominates (>90% of task).
- Task durations are **uniformly slow** (`p50 ≈ p100`), i.e. not a skew problem.
- **Result cardinality is tiny relative to input rows** — each task probes/updates the same few
  aggregate buffers millions of times, so any per-row overhead compounds hugely.
- Changing partitions / cache settings does little, but changing the **execution path** does a lot.
- Especially suspect if **someone has raised `spark.sql.codegen.maxFields`** (e.g. to 200) — that
  forces wide stages back into the slow generated path.

**Anti-patterns (avoid when):**
- The stage is **shuffle-bound or IO-bound** — codegen is not your bottleneck; see `02-joins.md` /
  `03-file-layout-io.md`.
- The aggregation is **narrow** (few fields) — codegen is helping; leave it on. Lowering `maxFields`
  here only disables a beneficial optimization.
- Task time is **skewed** (`p100/p50 > 2x`) — that's a data-distribution problem, not a codegen one;
  see `02-joins.md` and `07-aqe.md`.
- **Do NOT set `spark.sql.codegen.wholeStage=false`.** That disables whole-stage codegen for *all*
  jobs and stages of *every* query on the session — it punishes the 95% of stages where codegen
  helps to fix one wide aggregate. Use `maxFields` instead (it targets only over-wide stages).
- **Never guess.** Both directions are plausible a priori; always A/B with the data before committing
  a config change.

**Apply:**

```python
# before  (the smell): a very wide aggregation, codegen on by default,
#                       possibly with maxFields raised above the default 100
agg_df = (
    src.groupBy(*group_cols)            # many grouping columns
       .agg(*agg_exprs)                  # many aggregate expressions
)                                        # group_cols + agg_outputs >> 100 -> huge fused method
result = agg_df.collect()                # CPU-bound: HashAggregate build dominates each task

# after   (the fix): push ONLY the wide aggregate stage to the non-codegen fallback
#                     by lowering maxFields below the field count of this stage.
prev = spark.conf.get("spark.sql.codegen.maxFields")   # save to revert (shared session!)
spark.conf.set("spark.sql.codegen.maxFields", "50")    # <- the change: < this stage's field count
agg_df = src.groupBy(*group_cols).agg(*agg_exprs)      # now runs the fallback path -> less CPU
result = agg_df.collect()
spark.conf.set("spark.sql.codegen.maxFields", prev)    # revert: leaks to other notebooks otherwise
```

| Config | Default | Suggested | Set where |
|---|---|---|---|
| `spark.sql.codegen.maxFields` | `100` | lower (e.g. `50`) **below the wide stage's field count** to force its fallback; or do NOT raise it | **[notebook]** (runtime SQL, `.internal`/advanced) |
| `spark.sql.codegen.wholeStage` | `true` | leave `true` — do **not** set `false` globally | **[notebook]** (runtime SQL, `.internal`/advanced) |

> **Shared-cluster warning:** there is **one SparkSession per AIDP cluster**. A notebook
> `spark.conf.set("spark.sql.codegen.maxFields", ...)` **leaks to every other notebook** on that
> cluster. Save the prior value and restore it (`spark.conf.set(..., prev)` or `spark.conf.unset`)
> when the job is done. A cluster restart resets everything to the cluster config. Both
> `codegen.*` keys are runtime SQL confs (`[notebook]`-settable), but are marked `.internal`/advanced
> in this Spark 3.5.0 build — they are not surfaced as ordinary tunables, so set them deliberately.
> See `config-matrix.md` and `aidp-notes.md`.

**Other fixes (least-invasive first):**
1. Lower `spark.sql.codegen.maxFields` for the problematic job and re-benchmark (above).
2. **Reduce aggregate width** — drop unnecessary grouping columns / aggregates from the query.
3. **Break a very wide aggregation into stages** when the business logic allows it.
4. **Re-check after schema growth** — a query fine at 85 fields can regress badly at 118 fields once
   it crosses the threshold. Keep a small reproducible benchmark notebook to catch this as a guardrail.

**Heuristic — is this aggregation wide enough to be suspect?**

```text
wide_agg_field_count = number_of_group_columns + number_of_aggregate_outputs
# >= ~100  AND  CPU-bound  AND  tiny result cardinality  -> suspect codegen
```

**Evidence:**
- **Field engagement (Spark 3.5.0):** two batch pipelines, ~same 2 TB read, very similar group-by
  logic; differed only in transformations and group-by columns. One ran **1.3 H**, the other **7.9 H**
  (~6x). The fast one had **>100 fields → codegen auto-disabled**; the slow one was **just under the
  threshold → codegen kicked in and performed poorly**. The divergence was localized to
  `HashAggregate`: cumulative hash-aggregation time was **9,691 H** (slow) vs **1,386 H** (fast) for
  a similar volume of data read. After **lowering `maxFields`** to push the wide stage to fallback,
  hash-aggregation time dropped to **420 H** and runtime from **7.9 H to ~23 min** (well under SLA).
- **AIDP reproduction (4-core cluster, Spark 3.5.0):** synthetic hostile shape — 10.95M rows, 119
  columns, **85 grouping columns + 33 aggregates = 118 aggregate fields**, collapsing to only **12
  output groups**. Two notebooks, same cached source, same aggregation; the only change was
  `spark.sql.codegen.maxFields` **200 (codegen path) vs 50 (fallback path)**.
  - Avg runtime **26.56 s → 18.76 s = 1.42x** faster with codegen constrained.
  - Plan shape **identical** (`HashAggregate → Exchange → HashAggregate`); only the scan flag
    changed (`Batched: true` vs `Batched: false`).
  - **Same scan, same shuffle, lower CPU:** input bytes (4,910,088,048), shuffle write (144,416 B),
    shuffle records (192), task count (17) all unchanged; executor CPU time **94.68 s → 67.02 s**.
  - **p50 task duration 6,055 ms → 4,252 ms** with input records/task unchanged (171) → per-record
    median cost **35.4 → 24.9 ms/record**. `p50 ≈ p100` (uniform, not skew). This is the textbook
    signature: data movement flat, CPU down → operator/expression execution overhead, not IO/skew.
  - Memory folklore caution: peak execution memory moved *opposite* to the simple story here
    (`4,456,448` vs `1,077,935,872`). Do not assume codegen always uses more/less memory — **measure**.

**A/B diagnostic (how to confirm it's codegen, not something else):**
1. **Prove the runs are logically identical** — same dataset, row count, grouping columns, aggregate
   expressions, and result count. Otherwise it isn't an A/B test.
2. **Capture the formatted plan:** `agg_df.explain(mode="formatted")`. Confirm the operator shape
   (`HashAggregate → Exchange → HashAggregate`) is **constant** across both runs and only the
   execution path differs. If the whole plan shape changes, investigate that first.
3. **Isolate the two runs in the Spark UI** by job group / timestamp so you compare the timed
   aggregation, not cache-materialization or source-generation jobs.
4. **Compare stage metrics before task metrics:** did input bytes / shuffle bytes / task count
   change? If data movement is flat but **executor CPU time fell sharply**, it's execution overhead.
5. **Compare p50 task cost,** not just wall time — `p50 ≈ p100` confirms a uniform CPU issue (codegen),
   not a skew issue.
6. **Run the threshold A/B:** one run with high `maxFields` (codegen), one with a deliberately lower
   `maxFields` (fallback), everything else fixed. Scan/shuffle unchanged + CPU drops = strong evidence.
7. **Only if doubt remains:** dump generated code with `spark.sql.codegen.dumpGenCode=true`, inspect
   SQL query details in the UI, or check JVM compilation behavior in executor logs to prove the
   method-size / JIT-fallback effect. Do this *after* the cheap A/B, not before.

**Detect in Spark UI:**
- **SQL tab:** the dominant cost is a `HashAggregate` node (build time >90% of task duration); the
  partial/final pair sits around an `Exchange`. A `WholeStageCodegen (n)` wrapper around the
  HashAggregate indicates the generated path is active.
- **Stage tab:** high **executor CPU time** while input/shuffle bytes are modest → CPU-bound.
- **Task summary:** uniform durations (`p50 ≈ p100`, no straggler) rules out skew and points at a
  per-row execution issue.
- Tiny shuffle-write record count vs huge input record count → tiny result cardinality, the
  buffer-thrashing pattern that makes per-row overhead compound.
- Cross-ref `diagnosis.md` for the symptom→cause table, skew-ratio math, and query→stage timing
  method; `07-aqe.md` and `02-joins.md` if the stage turns out to be skew/shuffle-bound instead.

---

## Python UDFs, `pandas_udf` & Arrow (a codegen / pushdown blackbox)

**What:** a plain Python UDF runs **row-at-a-time**, serializing every row across the JVM↔Python boundary; it is opaque to Catalyst, so it **breaks whole-stage codegen and blocks predicate/column pushdown** through it.
**Why it matters:** the most common avoidable PySpark slowdown. Preference order: **native Spark SQL functions (`F.*`) > vectorized `pandas_udf` (Arrow batches) > plain Python UDF (worst).**
**Patterns (use when):** replace a Python UDF with built-in column expressions where one exists; if you must run Python, use `pandas_udf` so the boundary crossing is amortized over Arrow batches.
**Anti-patterns (avoid when):** a Python UDF inside a filter (kills pushdown — filter on native columns *before* the UDF); a UDF over a huge columnar scan where a native expression exists.
**Apply:**
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")   # [notebook]; Arrow for pandas_udf & toPandas
from pyspark.sql.functions import pandas_udf
@pandas_udf("double")
def scale(s):                 # receives a pandas Series (a batch), not one row
    return s * 1.5
df.withColumn("y", scale("x"))      # vectorized; better still: F.col("x")*1.5 (native, no Python at all)
```
Config: `spark.sql.execution.arrow.pyspark.enabled` (default `false` → `true`) [notebook]; `spark.sql.execution.arrow.maxRecordsPerBatch` (default 10000) tunes the Arrow batch size.
**Detect in Spark UI:** the SQL plan shows a `BatchEvalPython` (plain UDF) or `ArrowEvalPython` (`pandas_udf`) node that **breaks the WholeStageCodegen chain**; high executor time with Python-worker activity. Cross-ref `diagnosis.md`.
