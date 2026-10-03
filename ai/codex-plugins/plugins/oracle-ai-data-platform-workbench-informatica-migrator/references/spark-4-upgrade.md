# Spark 4 on AIDP — what changes, and what to do

AIDP runs **Spark 3.5 today** and moves to **Spark 4**. Reported as
imminent as of 2026-09-23; confirm the actual date against your tenancy
before acting on the scheduled work below.

This matters more to a migrator than to most tools. Spark 4 changes
defaults that decide whether a migrated notebook returns a NULL or aborts
the job — and Informatica's behaviour is the permissive one, so the new
defaults are the wrong ones for migration fidelity.

## The problem the upgrade creates

| Situation | Spark 3.5 | Spark 4 (ANSI on) | Informatica |
| --- | --- | --- | --- |
| Failed cast | NULL | **raises** | NULL / port default, row error |
| Divide by zero | NULL | **raises** | NULL |
| Numeric overflow | NULL or wrap | **raises** | NULL or row error |
| Value too wide for target column on write | **wraps** (measured: 3000000000 into INT stored as -1294967296) | **raises** | truncates, row error |

`spark.sql.ansi.enabled` defaults to `false` in 3.5 and `true` in 4.
`spark.sql.storeAssignmentPolicy` defaults to `LEGACY` in 3.5 and `ANSI` in 4.

Both verified by reading them off pyspark 4.2.0, not from release notes.
The per-expression evidence -- which 14 of the 58 expressions this tool
emits actually change behaviour, with the exact error each one raises -- is
in [`spark-4-behaviour-differences.md`](spark-4-behaviour-differences.md).
That file also documents a measured argument that the
`storeAssignmentPolicy` pin is the wrong call: on the write path `LEGACY`
preserves a silent wraparound, so pinning it keeps data corruption that
Spark 4 would have turned into an error.

A generated notebook that inherits the cluster default therefore **changes
behaviour the day the runtime is upgraded, with nothing in the notebook to
explain it**. Rows that were NULL become an aborted job; or a reconciliation
total moves and the pipeline looks fine. This applies to every notebook
already generated and delivered, not just future ones.

These are gotchas #8, #9 and #10 in the conversion reference — decimal
overflow, ANSI versus implicit coercion, and divide by zero — all three
classified as *wrong results or failures, no error at generation time*.

## What the generator does about it

Every generated notebook pins both settings in its setup cell, with the
reason inline:

```python
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")
```

Pinned, not inherited. The value matters — `false` matches Informatica —
but the pinning matters more: it makes behaviour reproducible across the
upgrade rather than dependent on which week the notebook ran.

`tests/test_spark_runtime_semantics.py` asserts both are present, that the
reason is stated alongside them (a bare `conf.set` reads as arbitrary and
gets tidied away by the next person), and that both precede any DataFrame
work.

**Do not remove these when AIDP reaches Spark 4.** The upgrade is exactly
what they exist for. They should be removed only if a customer explicitly
wants ANSI semantics, which means accepting that the pipeline will fail
where Informatica returned NULL.

## Scheduled work — after AIDP reaches Spark 4

- [ ] **Confirm the runtime actually flipped** before changing anything
      else. Everything below depends on it.
- [ ] **Convert XML Parser and XML Generator.** They are currently refused
      because Spark has no built-in `from_xml` / `to_xml` before Spark 4 —
      those live in the external `spark-xml` package, which may not be on
      the cluster. On Spark 4 both are built in and these become
      straightforward converters. Deferring this until after the upgrade
      *saves* work: a dual-path emitter with a one-week shelf life is not
      worth building. Worth roughly +2 PowerCenter types.
- [ ] **Add an explicit target-runtime input** (`--spark-version`, default
      4) so version-dependent emission is one decision in one place rather
      than assumptions scattered across the generators.
- [ ] **Re-run the corpus and re-measure.** The conversion rates in the
      README were measured on 3.5. Confirm they hold.
- [ ] **Re-check the rest of the 3.5 → 4 delta** for anything else
      affecting emitted code — datetime parsing policy, decimal arithmetic,
      and interval handling are the usual suspects.

## Why this is not simply "use ANSI, it is more correct"

ANSI is stricter and, in isolation, better practice. It is the wrong
default *here* because the goal is equivalence with a source system, not
best-practice SQL. A migration that raises where Informatica returned NULL
has not preserved the pipeline — it has changed it, and the customer finds
out in production.

If a customer wants ANSI semantics, that is a deliberate post-migration
decision to make once the migration is proven equivalent. It is not
something to inherit accidentally from a runtime upgrade.
