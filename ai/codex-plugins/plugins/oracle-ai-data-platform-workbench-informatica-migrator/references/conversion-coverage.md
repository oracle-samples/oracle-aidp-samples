# What this tool converts

The scope of the converter, read off the code rather than described from
memory: the transformation dispatch table in
`engine/infa2aidp/converters/transformation_converter.py`, the function
registry in `engine/infa2aidp/converters/expression_converter.py`, and the
task handling in `engine/infa2aidp/generators/workflow_generator.py`.

Every row below is one of three outcomes, and the distinction matters more
than the counts:

- **Converted** — the generator emits PySpark for it.
- **Reported** — there is no faithful Spark equivalent, so the generator
  emits a named reason and what to do instead, rather than code that looks
  right and is not.
- **Refused** — the input is outside the supported scope and the migration
  stops rather than guessing.

A reported transformation is a successful run of this tool. It is not a
conversion, and the generated report says so.

## Sources accepted

| Source | Format | Notes |
| --- | --- | --- |
| IDMC / IICS | CDI mapping export (JSON) | Current Informatica cloud. |
| PowerCenter 10.x | Repository XML (`pmrep` / Repository Manager) | Repository version gate; see below. |

PowerCenter 8.x and 9.x exports are **refused by the version gate**, not
converted on a best-effort basis. Repository versions 186 and 187 are read
as 10.1 and 10.2 on reviewer recollection rather than a verified export —
the generated report names that dispute so it is not inherited silently.

## Transformations

**36 types converted · 14 reported with a reason.**

### Converted

| Transformation | Becomes |
| --- | --- |
| Source Qualifier | `spark.table()` plus the qualifier properties that change *which rows* are read — SQL override WHERE, Source Filter, Select Distinct. An override that JOINs, UNIONs or sub-selects runs as `spark.sql()` over catalog-qualified tables; a User Defined Join is synthesised into the same shape. Both refuse, naming the reason, on Oracle-only SQL or a table that is not a source here |
| Application / MQ / XML Source Qualifier | the same handler: the read differs, the transformation semantics do not |
| Expression | per-port column expressions, in port-dependency order |
| Filter | `.filter()` |
| Router | one DataFrame per output group, from what each group's condition tests |
| Joiner | `.join()`, with the Informatica join type. Master/detail comes from the MASTER port flag or Master Source property; when neither resolves it, an inner join is still applied (it is symmetric) and an outer join is refused |
| Lookup | a join against the lookup source, connected or unconnected |
| Aggregator | `.groupBy().agg()` |
| Sorter | `.orderBy()` |
| Rank | a window function |
| Union | `.unionByName()` |
| Normalizer | `explode` over the occurring port group |
| Deduplicate | `.dropDuplicates()` |
| Sequence Generator | a monotonic surrogate key, cast to `BIGINT`; frozen on a matched update so a re-run does not re-key existing rows |
| Update Strategy | insert / update / delete / reject routing |
| Transaction Control | commit-boundary handling |
| Stored Procedure | a JDBC call-out |
| SQL | the configured query against the catalog |
| Mapplet, Input, Output | inlined into the parent mapping |
| Hierarchy Parser / Builder / Processor | nested-column read, build and reshape |
| Structure Parser | parse of the configured structure |
| Data Masking | partial — the masking rules with a direct Spark expression |
| Chunking | AIDP-native text chunking |
| Vector Embedding | AIDP-native embedding |
| Machine Learning | a model served from the AIDP registry |
| Java, Python, Velocity, Custom, External Procedure | the embedded source is **carried across verbatim, not translated**, as a review item next to the cell that needs it |

The five code-carrying types deserve emphasis: the tool preserves the
original code so nothing is lost, and does not pretend to have ported it.

### Reported with a reason

These 14 have no faithful Spark equivalent, almost always because the logic
lives in an asset the mapping export does not contain:

Cleanse, Labeler, Parse, Verifier, Rule Specification (Cloud Data Quality
assets) · XML Parser, XML Generator (no built-in `from_xml` before Spark
4) · Unstructured Data, B2B · Web Services, Web Services Consumer, HTTP,
Data Services (runtime service calls) · Access Policy.

Each emits the referenced asset name, why it cannot convert, and the
rebuild path.

## Expression language

**72 Informatica functions** convert to PySpark, covering string, numeric,
date, conditional, test, encoding, encryption and mapping-variable
families, plus `IIF`/`DECODE` conditionals, `$$PARAM` references and port
variables.

An expression the converter cannot parse becomes an explicit refusal with
`REVIEW REQUIRED`, not a filter on `F.lit(None)`. The semantic differences
that change *results* rather than fail — NULL handling, division by zero,
bad casts, date parsing — are in
[`conversion-hazards.md`](conversion-hazards.md) and
[`spark-4-behaviour-differences.md`](spark-4-behaviour-differences.md).

## Orchestration

A PowerCenter workflow becomes an **AIDP job** whose DAG preserves the
session dependency graph.

| Workflow element | Outcome |
| --- | --- |
| Session | a job task running the session's notebook |
| Link dependencies | task `dependsOn`, resolved to nearest-session ancestors so a dependency that passes *through* a non-session task is kept |
| Unconditional links | `ALL_DONE`, matching PowerCenter |
| `$task.Status = SUCCEEDED` links | `ALL_SUCCESS` |
| Scheduler | a Quartz cron expression |
| Start task | correctly represented by its absence |
| Worklet, Command, Email, Decision, Timer, Event-Wait / Event-Raise, Control, Assignment | **not translated** — no notebook to run; each is reported, not dropped silently |
| Target load order | preserved |

The schedule's **timezone is assumed `UTC`**. PowerCenter records
`STARTTIME` in the Integration Service's local time with no zone, so this
is a supplied value the operator has to confirm — it is listed under the
job's assumptions, not its translations. Read the schedule callout at the
top of [`migration-steps.md`](migration-steps.md) before enabling any
migrated job.

## Also generated

- **Target DDL** — `ddl/<table>.sql` per target, from the export's
  `DATATYPE` / `PRECISION` / `SCALE`. Columns fed by a Sequence Generator
  are widened to `BIGINT`, traced through the connector graph rather than
  by column name.
- **Reconciliation** — row and aggregate comparisons between source and
  migrated target.
- **Reports** — per-mapping conversion confidence, review items, and
  `reports/broken_notebooks.md` for any notebook that reads back as
  unresolved or abandoned.

## What the counts are not

Every number above counts *recognised types with a defined outcome*. It is
not a fidelity claim: "emits Spark" is not "emits Spark that matches what
Informatica computed". See
[What this does NOT do](../README.md#what-this-does-not-do) for the limits
that still apply.
