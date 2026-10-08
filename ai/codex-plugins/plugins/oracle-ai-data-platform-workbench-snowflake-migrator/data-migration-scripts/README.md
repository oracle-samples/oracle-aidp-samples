# Data-plane notebooks — run INSIDE AIDP, schema by schema

These **notebooks** are the **data plane** of the migrator. The Codex
plugin (assessment, plan, DDL, catalog registration) is the control plane;
its `provision` stage uploads these notebooks to the workspace folder
`backup-snowflake-migration/scripts/` and wires each stage into an AIDP job.
They can also be run by hand: open one in the console, edit the `PARAMS` cell
at the top, and run it.

| # | Notebook | Reads | Writes | Purpose |
|---|---|---|---|---|
| — | `diagnose_environment.ipynb` | everything | nothing | **run this first**: workspace mount, network reach to Snowflake, credentials through the connector, EXTERNAL catalog contents |
| 0 | `00_discover_snowflake.ipynb` | Snowflake | reports dir | inventory every schema, table and column into `discovery_manifest.json`, with a dated backup |
| 1 | `01_create_structure.ipynb` | `ddl_plan.json` (or source) | target catalog | create the target schemas and empty Delta tables, read each back against the plan, then create the plan's views |
| 2 | `02_copy_schema.ipynb` | Snowflake | target catalog + reports | copy ONE schema's tables and verify them: row counts, and exact decimal sums on request |
| 3 | `03_reconcile.ipynb` | reports + target catalog | reports dir | plan-versus-reality report: what landed, what did not, and why |

## Notebooks, not scripts

AIDP types a workspace object by its **extension**: an `.ipynb` becomes a
`NOTEBOOK`, which a job's `NOTEBOOK_TASK` runs, while a `.py` is stored as a
`FILE`. So everything that runs on AIDP ships as `.ipynb`.

Each notebook is **self-contained** — parameters, the shared source helper
and the stage logic in one object. There is no driver wrapper and nothing is
imported from the `/Workspace` mount, so the code you open is exactly the code
that runs.

## They are generated — edit the source, not the notebook

The canonical Python lives in `engine/dataplane/`. The notebooks are
assembled from it:

```bash
bin/snowmig build-notebooks
```

They are generated so the shared helper cannot drift between copies, and
committed so what ships is reviewable. **A hand edit here is overwritten on
the next build.**

## Preconditions and safety

**Preconditions:** a Snowflake connection config on the workspace mount (the
plugin's `provision --source-config` puts it there) and an INTERNAL target
catalog. Registering the account as an EXTERNAL catalog is optional for these
notebooks, and required only for `--source-mode external-catalog`.

**Nothing here can write to Snowflake.** The AIDP Snowflake connector is
read-only (AIDP 4.0), an EXTERNAL catalog refuses DDL, and every statement
these notebooks issue against the source is a `SELECT`. The service user's
read-only grant is a second guarantee.

- **Nothing is dropped.** `--mode overwrite` rewrites a table's **rows**
  (`INSERT OVERWRITE`); it never drops the table. The default mode
  (`skip-existing`) touches nothing that already has rows — it compares their
  count with the source's and records `count_mismatch` when they differ.
- **A per-table failure is recorded and the run continues**; the report, not
  the exit code alone, is the deliverable.
- **Verification is explicit.** Row counts by default; `counts+sums` adds an
  exact `SUM` over every decimal column **of the source**, at the source's
  scale on both sides. In connector mode the source total is computed in
  Snowflake (`sum("C")::VARCHAR`, one qualified pushdown per table) and the
  target's by Apache Spark over the Delta column, and the two are compared as exact
  decimals. Floats are never summed for equality, because float tolerance is
  wrong for money. A total past 38 digits cannot be held by either engine's
  SUM: that column is listed under `sums_not_comparable`, the other decimal
  columns are still compared, and the table is `sum_not_comparable`. A target
  column that cannot hold a source decimal without loss stops the copy as
  `type_drift` before any row moves, in both verify modes.

## Source modes — use `connector`

| Mode | How it reads Snowflake | When |
|---|---|---|
| `connector` *(default)* | the AIDP Snowflake connector, from the cluster: `spark.read.format("aidataplatform").option("type","SNOWFLAKE")` | **always, unless asked otherwise** — needs no extra cluster library and no catalog crawl |
| `external-catalog` | three-part names against a registered EXTERNAL catalog | only on explicit request — needs the catalog's metadata to be populated, and costs a `DESCRIBE` per object |

Why `connector`:

- **Cost.** Connector discovery is **two `INFORMATION_SCHEMA` queries for the
  whole database**. The three-part-name route is `SHOW` plus a `DESCRIBE`
  per object, so its cost grows with the object count.
- **Fewer preconditions.** The connector needs only the credentials `smoke`
  already checked; the external-catalog route also needs the catalog's
  metadata to be in place before discovery can start.
- **Evidence.** A job leaves a run, its task output and a manifest on the
  platform — the record of the migration.

## Target schema

01, 02 and 03 all address the **target schema the approved plan names**
(`target_fqn` in `ddl_plan.json`, e.g. `db_core` under a bronze prefix), not
`--schema` itself; the source schema's own name stands in only where the plan
names none. `--target-schema` may restate the plan's schema in any case,
never contradict it. Schema names compare case-insensitively, as Spark
resolves them. The copy refuses to run when the structure report on disk
targets a different schema, rather than widening its scope to the whole
manifest.

## How the copy reads a table (connector mode)

`02_copy_schema` reads each table with **one qualified pushdown**, never the
connector's table read, which costs minutes a table and is lossy: it drops
`NUMBER` digits, `TIME` / `TIMESTAMP` fractions and the `TIMESTAMP_TZ`
offset, and cannot open a table holding a `VECTOR`, `MAP` or structured
`OBJECT`.

- The approved `ddl_plan.json` carries a per-column read spec on every
  TABLE statement (`columns`: `name`, `target_type`, `read_expr`,
  `convert_expr`). The copy sends
  `SELECT <read_expr> AS "<name>", ... FROM "DB"."SCHEMA"."TABLE"` as one
  pushdown (e.g. `"N"::VARCHAR`, `TO_VARCHAR("T", 'HH24:MI:SS.FF9')`,
  `"V"::ARRAY::VARCHAR`), then inserts
  `SELECT <convert_expr> AS <target column>, ...` in the target's column
  order (e.g. `CAST(N AS DECIMAL(38,37))`, `from_json(V, 'array<float>')`).
- Every table a pushdown names is fully qualified, with the database taken
  from the source config: the pushdown session has no current schema.
- **A plan without `columns`**, written by an older `ddl`, is still copied:
  every column is read bare (`"NAME"`), with the connector's own typing and
  its losses. Each table's record says so under `read.bare`. Re-run `ddl`
  for the exact reads.
- Before any row is read, the live source's columns come from
  `INFORMATION_SCHEMA.COLUMNS` (one query per chunk of tables) and are
  compared with the target's by name, and each source `NUMBER(p,s)` with the
  target's DECIMAL: a renamed, dropped or added column, or a narrower target
  DECIMAL, is `type_drift` and nothing is read or written. A table
  `INFORMATION_SCHEMA` does not list, or a chunk whose query fails, is
  `failed`, NOT copied.
- `external-catalog` mode reads the three-part name; the plan's read
  expressions are Snowflake SQL and are not applied there (the run log says
  so).

## Structure modes

`01_create_structure` `--mode`:

| Mode | Types come from | Notes |
|---|---|---|
| `ddl-plan` *(default)* | `plan/ddl_plan.json` — the migrator's own mapper, which refuses what it cannot map exactly, reviewed and signed off before the run | no source read. A table absent from the plan is reported `not_in_plan` and NOT created; a run in which **every** table is `not_in_plan` exits 1, because the plan and the requested schema do not overlap. Creates the plan's views after every table |
| `ctas` | Spark, derived through the connector | one Snowflake round trip per table, and the types are the connector's mapping rather than the reviewed plan's. Tables only |
| `manifest` | `discovery_manifest.json` verbatim | valid only for an external-catalog manifest (Spark types); a connector manifest carries Snowflake types and is refused rather than mistranslated. Tables only |

In every mode each table is `DESCRIBE`d after the CREATE and compared with
the plan, column by column and in order. `CREATE TABLE IF NOT EXISTS` does
nothing on a table that is already there, so without the read-back a stale
layout could be certified as created from the plan — and the copy fills the
target's columns in the target's order.

`--parallel` (default 8) sets how many tables are created at a time within a
schema, in every mode, each still read back on its own; dry runs create one
at a time, and `parallel=1` creates them one by one. The report lists every
table in the manifest's order, whatever the setting. Views are not parallel:
they are created after **every** table exists, one at a time, in the plan's
dependency order.

## Statuses and verdicts

What each report records per table. Anything marked **problem** exits 1 in
the stage that records it and is a problem verdict in `MIGRATION_REPORT.md`.

**`structure_report_<schema>.json`** (`objects`, one per table)

| Status | Meaning | Problem? |
|---|---|---|
| `created` | not there before; reads back as planned | no |
| `already_existed` | there before, and it matches the plan (in `ctas` mode there is no plan: the layout is NOT compared, and the record's reason says so) | no |
| `type_drift` | there before with a layout the plan did not produce; left as found, differing columns listed; excluded from the copy's default scope | **yes** |
| `not_in_plan` | the approved plan carries no columns for it; NOT created | no (but a run of nothing else exits 1) |
| `failed` | the CREATE raised; the error is the reason | **yes** |
| `dry_run` | `--dry-run`; nothing was issued | no |

Views sit under a separate `views` key, never in `objects` (the table map the
copy takes its default scope from). In `ddl-plan` mode each planned view is
created from the plan's own CREATE VIEW SQL after every table exists, and
recorded `created`, `failed` (**a problem**: exit 1, the error is the reason)
or `dry_run`; a manifest view the plan does not carry is `not_in_plan`
(`in_plan: false`) and NOT created. `ctas` and `manifest` mode create tables
only and record every view `not_created_by_this_path`; create those with
`snowmig deploy --execute` and verify them against the source. Every manifest
view is listed, so none is ever absent from every report with exit 0. The
copy never writes into a view: its default scope is the `objects` the
manifest lists as tables.

**`copy_report_<schema>.json`** (`tables`, one per table)

| Status | Meaning | Problem? |
|---|---|---|
| `verified` | counts equal after the copy (and decimal sums, with `counts+sums`) | no |
| `skipped_nonempty` | `skip-existing` found rows already there, **equal** to the source count; not re-verified | no |
| `count_mismatch` | counts differ — after a copy, or on a `skip-existing` target that already held a different number of rows (nothing copied) | **yes** |
| `sum_mismatch` | counts equal, a decimal column does not sum equal | **yes** |
| `sum_not_comparable` | counts equal (and every other decimal column sums equal), but a decimal column's total is past 38 digits, which neither engine's SUM can hold; `sums_not_comparable` names each such column and why. Re-copying does not change it: re-copy with `--mode overwrite --verify counts` to accept the count check for that table | **yes** |
| `type_drift` | the live source's column names are not the target's (renamed, dropped or added since the plan; `layout_drift` lists them), or a source DECIMAL column is not DECIMAL, or narrower, on the target; NOT copied — the rows would land in the wrong columns, or be rounded or truncated, with the count intact. A source whose columns are only **reordered** is copied: every column is selected by name, in the target's order | **yes** |
| `failed` | the copy raised — including a `DESCRIBE` of the target that failed for any reason but not-found (a metastore timeout, a permission denied: "could not look" is never recorded as absent); `insert_completed: true` means the rows landed before verification failed (or, with `awaiting_source_recount: true`, before the run that wrote them stopped), so re-copy with `--mode overwrite`; an `append` run refuses such a table and records why | **yes** |
| `target_missing` | Spark says there is no table to copy into (usually `not_in_plan` upstream); the table is skipped and the run continues | no — **yes** when the structure report records the table `created` or `already_existed`, or the approved plan places the table at this target (also with `--tables`, or before `01_create_structure` has run) |

The copy's default scope is **what the structure step created for this
target**, not the whole manifest. A re-run never softens a recorded failure:
`count_mismatch`, `sum_mismatch`, `sum_not_comparable`, `type_drift` and
`failed` stand until a real re-copy verifies the table. `--force` re-copies
verified tables and needs `--mode overwrite` or `append` — `skip-existing`
cannot re-copy a table that holds rows.

**`MIGRATION_REPORT.md` verdicts** (per table, from the two reports plus the
live catalog)

| Verdict | Meaning | Problem? |
|---|---|---|
| `MIGRATED_VERIFIED` | copy verified, table present (and, with `--counts`, still at the verified row count) | no |
| `PRESENT_NOT_REVERIFIED` | rows were already there at the source's count; sums not re-checked | no |
| `STRUCTURE_ONLY` | table present, no copy yet | no |
| `NOT_MIGRATED` | not attempted yet (expected while the migration runs schema by schema) | no |
| `NOT_IN_PLAN` | the approved plan leaves it out (an S9 scope reduction, or a table the engine blocked); the structure report records it `not_in_plan`. Not pending | no |
| `VIEW_CREATED` | a view the structure job created from the approved plan | no |
| `VIEW_NOT_IN_PLAN` | a manifest view the approved plan does not carry; NOT created | no |
| `VIEW_NOT_CREATED_YET` | a planned view not created yet (`--dry-run`) | no |
| `VIEW_NOT_CREATED_BY_THIS_PATH` | a manifest view no `ddl-plan` structure run recorded (`ctas` / `manifest` mode create tables only) | no |
| `VIEW_FAILED` | the structure job's CREATE VIEW raised; the error is the reason | **yes** |
| `VIEW_MISSING_DESPITE_REPORT` | the structure report records the view created, but the target does not have it (looked for with SHOW TABLES, SHOW VIEWS, then DESCRIBE) | **yes** |
| `MISSING_DESPITE_REPORT` | a report says created or verified; the catalog lacks it | **yes** |
| `STRUCTURE_FAILED` | the CREATE raised | **yes** |
| `STRUCTURE_TYPE_DRIFT` | the table's layout is not the plan's — outranks a verified copy, since counts match when rows land in the wrong columns | **yes** |
| `STRUCTURE_ONLY_COPY_FAILED` | the copy ended in a mismatch, drift or failure (`sum_not_comparable` included), or recorded `target_missing` for a table the catalog lists | **yes** |
| `COUNT_DRIFT` | `--counts` only (the live counts are read in one batched query per 50 tables; a chunk with an unreadable table is re-counted one by one, and that table keeps its own `count_error`): verified at N rows, the target now holds a different number — changed since the copy, not by it | **yes** |
| `TARGET_UNREADABLE` | `SHOW TABLES` failed; not the same as empty | **yes** |

A report written for a **different target** — another catalog, or another
schema than the one the plan names — is ignored by reconcile (and named in
the report), never applied to this one.

## Parameters and the intended run

`provision` uploads these notebooks and wires one AIDP job per stage, each
pointing straight at its notebook, plus one copy job per schema of the
approved plan, all running the same `02_copy_schema` notebook. This run's
coordinates are written into each notebook's `PARAMS` cell at upload time, as
defaults. A job task's `parameters` override them by the same names at run
time — the notebook reads them with `oidlUtils.parameters.getParameter` —
which is how each per-schema copy job passes its `schema`.

To run one by hand, open it in the console and edit the `PARAMS` cell:

```python
# ── PARAMETERS ──
PARAMS = {
    'source-mode': 'connector',
    'source-config': '/Workspace/backup-snowflake-migration/plan/snowmig-config.json',
    'target-catalog': 'snowdemo',   # REQUIRED
    'schema': 'SALES',              # REQUIRED for 02_copy_schema
    'verify': 'counts+sums',
    'reports-dir': '/Workspace/backup-snowflake-migration/reports',
}
```

`None` omits a flag entirely; `True` passes a bare switch. The notebook turns
`PARAMS` into the stage's argument list.

Run order: `00_discover_snowflake` once, then `01_create_structure` and
`02_copy_schema` per schema in the order the plan's waves give, then
`03_reconcile` at the end (or at any time — it only reads). For a first run on
a new estate, run `diagnose_environment.ipynb`, then one small schema end to
end, then read `MIGRATION_REPORT.md` before running the rest.

**Scope and mode are inputs.** To migrate less, change a `PARAMS` value or a
job's task parameter — never edit the stage logic to make it cover less.

**A schema is the operating unit, never a table.** Two costs drive that:

- each job run has a start-up cost before it does any work, so per-table runs
  are the wrong shape;
- in connector mode every source read opens its own Snowflake session, so the
  copy batches a schema's source counts into **one** round trip per 50 tables
  instead of two per table. `--verify counts+sums` still costs a read per
  table for the sums, which is why it is opt-in.

Within a schema, the copy runs `--parallel` tables at once (default 8; `1`
copies them one after another), in chunks of 50: one batched source count
before the chunk, one `INFORMATION_SCHEMA` read, the copies, then one batched
count after it, so a source that grew while a table was copied is still
`count_mismatch`, with the source named as the side that moved. Each table's
record is written as soon as its copy finishes — provisionally `failed` with
`insert_completed: true` and `awaiting_source_recount: true` until the
chunk's recount replaces it with the verdict — so a job that stops mid-chunk
leaves no table holding rows without a record.

Every notebook is **resumable**: re-running skips work its report already
records as done (`--force` overrides; for the copy it needs `--mode overwrite`
or `append`), because a large estate may take several sittings and a run
should never restart from zero. A re-run never softens a recorded failure.

## Consistency — read before a production cutover

Each table is copied at a different moment. If the source keeps changing
during the copy, the target is consistent per table but NOT across tables.
For a real cutover: freeze writers, or copy from a Snowflake zero-copy
`CLONE` taken at a single point in time, or plan an incremental re-sync. The
reconcile report (`--counts`) shows what changed since the copy.

## Cluster libraries (`requirements-aidp.txt`)

**Nothing needs installing.** Both source modes use surfaces already on the
cluster — the `aidataplatform` format is built in. `requirements-aidp.txt`
is for optional additions and ships with no active entries; the plugin's
provisioning step installs whatever it contains through the cluster-libraries
API (`PATCH .../clusters/{key}/libraries`, types `PYPI` / `WORKSPACE_FILE` /
`MAVEN`), followed by a cluster restart.
