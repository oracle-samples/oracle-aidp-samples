---
name: snowflake-migrator-overview
description: Router, runbook and shared rules for migrating a Snowflake estate onto Oracle AI Data Platform (AIDP). Read this first whenever the user mentions migrating, moving, assessing, inventorying or cloning Snowflake databases, schemas, tables, views or warehouses onto AIDP, asks to "run the migration" or "start from zero", or asks what a Snowflake migration would involve. Carries the FIXED twelve-step order of operations and the rules that apply at every step; adds no API surface of its own.
---

# Snowflake → AIDP migrator — the runbook

A migration is **twelve steps, in this order, every time**. This is not a menu
of stages to pick from. If the user asks to migrate, you run S1 through S12 in
sequence. A step is skipped only when the user explicitly says to skip it, and
you say out loud which step you skipped and what that costs.

Before S1, two things must exist: the one connection config (copied from
`snowmig-config.example.yaml`; `0600` on POSIX; gitignored only inside the
plugin folder, so the user adds it to their own repo's `.gitignore`) and the
user's answer to *which database*. `README.md` → "How to run a migration, from
zero" carries the prerequisites and the flags; this file is the sequence and
the rules.

**The whole data plane runs inside AIDP, on Spark, as workflows.** The
operator's machine registers coordinates, reads reports and drives the
conversation. It does not read the estate. A discovery run on the laptop
leaves no workflow, log or evidence inside AIDP, and does not count.

## The rule that overrides convenience: CREATE, never reuse

**Never reuse a workspace, cluster, catalog, folder or job that already
exists.** Do not list existing ones and offer them. Do not "ensure" one. The
migration creates its own, named after this migration, so that everything it
touches can be identified, audited and torn down as a unit. The only question
you ask is *may I create it*, never *which of these should I use*.

If a name is already taken, that is a collision to report and resolve with the
user — a new name — not an invitation to adopt the existing object.

## The twelve steps

| # | Step | Creates | Gate |
|---|---|---|---|
| S1 | Create the **workspace**, named from the Snowflake project | 1 workspace | may I create |
| S2 | Create the **migration compute cluster** | 1 cluster | may I create |
| S3 | Register the source database as an **EXTERNAL catalog** | 1 catalog | user picks the database |
| S4 | Create the **INTERNAL target catalog** | 1 catalog | may I create |
| S5 | Create the migration folder and **upload the scripts** | folder + scripts | — |
| S6 | **Discovery, as a workflow inside AIDP**; back up the manifest | manifest + backup | — |
| S7 | **Generate the translation plan** by script; flag what needs review | plan JSON | — |
| S8 | **Resolve the flagged conflicts**, grouped | revised plan | grouped questions |
| S9 | **Present the summary** and take the go/no-go + scope reduction | reduced plan + backup | explicit OK |
| S10 | **Create the assets** by workflow, from the approved plan | schemas + tables | — |
| S11 | Generate the **per-schema data-migration scripts** + one workflow each | scripts + workflows | never run |
| S12 | Propose the **warehouse-equivalent clusters**, list, create on OK | clusters | explicit OK |

At S12 the migration is **done**: the assets exist, the scripts exist, the
plans and backups exist. **The data migration is not run.** Moving rows is a
later decision the customer makes, with the scripts already in place.

---

### S1 — Create the workspace, named from the Snowflake project

**The environment comes first, because everything after it needs its
coordinates.** An AIDP write is addressed by four coordinates — DataLake OCID,
workspace, cluster and catalog — and `resolve_target()` requires all four for
*any* write, including a catalog registration. A catalog registration
attempted before the workspace and cluster exist stops on `AIDP target
coordinates not supplied`.

The name comes from the source, so the workspace is identifiable as this
migration's. It passes through the simplest-charset translation
(`[a-z0-9_]`, accents folded, separators to `_`); a rename is reported, never
silent.

Create it. Do not look for an existing one to use.

### S2 — Create the migration compute cluster

One cluster, default config, dedicated to this migration: discovery, asset
creation and later the data copy. Sizing is not guessed here — S12 handles
warehouse-equivalent sizing as a separate, explicit decision.

S1, S2 and S5 are all produced by one `provision` call; they are numbered
separately because each is a distinct object with its own *may I create* gate,
not because each needs its own command.

**Hand-off.** `provision --execute` prints the workspace key and the cluster
key (and records them in `provision_result.json`). Every later command needs
them: pass `--workspace <key> --cluster-id <key>`, as the commands in this
runbook do, or put them under `aidp.workspace` / `aidp.cluster_id` in the
config — one or the other, never a mix. They are never read from the record
implicitly.

**Re-pushing.** A `--reuse-existing` re-push into this workspace (the plan
push at S9/S10) keeps the cluster name the first push recorded when no
`--cluster-name` is given, so it re-adopts this migration's cluster instead
of creating a second one under the default name.

**Resuming a partial run.** A new workspace becomes `ACTIVE` a few seconds
after its create returns. If the cluster create answers `409 Conflict — not
in an active state`, the workspace exists and the cluster does not: the run
is *partial*, not failed. Resume it with `--reuse-existing` against the
workspace this migration just created. Re-adopting your own half-built
environment is not the reuse the CREATE rule forbids — say which object you
are resuming and why it is yours.

### S3 — Register the source database as an EXTERNAL catalog

**One Snowflake database becomes one AIDP catalog. Always.** There is no
many-to-one and no partial registration: an EXTERNAL catalog registers the
whole database, and a plan-level restriction does not narrow it.

So if the account holds more than one database, **the user chooses which single
database this migration covers, now, before anything is created.** Run
`snowmig.py databases` and ask. Another database is another migration, run
again from S1.

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" catalog \
  --catalog <source_db_lowercased> --config ./snowmig-config.yaml \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cluster>
```

Dry-run first, show `CATALOG.md`, then `--execute` with confirmation in that
turn. Then run `--test-connection`, which starts the catalog's connection
test and polls it. Report the result as it is: **`PENDING` is pending, never
a pass**, and a catalog showing zero schemas against a source that has many
has not connected.

If the connection test returns `FAILED` without a reason, keep the
registration and continue; discovery (S6) validates the connection through
the connector. Never delete and re-register to make the test pass.

### S4 — Create the INTERNAL target catalog

This is the catalog the migrated schemas and tables land in. It is a
**container** — one control-plane object — and creating it is not the same as
creating its tables. Tables are created on AIDP compute by the structure
workflow (S10), where each create is read back; the control-plane catalog API
is used for the catalog container only.

**The API type is `INTERNAL`.** The runbook and the CLI say "standard", an
accepted alias that `normalize_catalog_type()` translates to `INTERNAL` before
any call. AIDP's two catalog types are `INTERNAL` and `EXTERNAL`.

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" catalog \
  --catalog <target> --catalog-type standard \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cluster>
```

The result carries `container_only: true`. Pass that on: the container
existing must never be reported as the structure existing.

S3 and S4 are two runs of the one `catalog` stage, and each keeps its own
record — `catalog_result_<name>.json` and `CATALOG_<name>.md` — so the S4 dry
run is allowed after S3 has executed and never overwrites it.
`catalog_result.json` is the latest executed run and lists every catalog
registered so far (`catalogs_recorded`), which is what the stage board
shows, connection test included.

To see what is on the DataLake, and with which types, ask the server rather
than assuming:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" catalogs \
  --datalake-ocid <ocid>
```

### S5 — Create the migration folder and upload the notebooks

`backup-snowflake-migration/` with `scripts/`, `plan/`, `reports/` and
`backup/`. The four data-plane **notebooks** from
`${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/` go into `scripts/`. Every
upload is read back before it is called done.

**Everything the data plane runs is an `.ipynb` notebook.** AIDP types a
workspace object by its extension — `.py` is stored as `FILE`, `.ipynb` as
`NOTEBOOK` — and a job task runs a NOTEBOOK.

Each stage notebook is **self-contained**: its parameters, the shared source
helpers and the stage logic are all in the one object, so the code a user
opens in the console is the code the job runs.

### S6 — Discovery, as a workflow inside AIDP

**This step does not run on the operator's machine.** Discovery is the
`00_discover_snowflake` notebook, run as an AIDP **workflow** on the migration
cluster, reading Snowflake through the AIDP connector.

#### Discover through the workflow, not through the external catalog

**Use the workflow in `connector` mode. Do not enumerate the estate through
three-part names against the EXTERNAL catalog.** This is settled; do not
re-evaluate it per migration.

| | workflow, `connector` mode | three-part names on the EXTERNAL catalog |
|---|---|---|
| Cost for a whole database | **two `INFORMATION_SCHEMA` queries** | `SHOW` + a `DESCRIBE` **per object** |
| Depends on | the connector, which the same credentials prove at smoke | the catalog's metadata crawl having completed |
| Leaves behind | a job run, its task output, and a manifest | nothing on the platform |

The connector route costs the same for any estate size; the three-part-name
route grows with object count. `--source-mode connector` is the default. Use
`external-catalog` only if the user explicitly asks for it, and say what it
costs before agreeing.

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" run \
  --datalake-ocid <ocid> --workspace <ws> --job snowmig_00_discover
```

`run` starts the job, polls it to a terminal state, and writes `RUN_*.md` with
the task output as evidence. A poll budget that runs out is reported as
**STILL RUNNING** — never rounded to success, never to failure. A status that
could not be read (a 503, an expired session) is **STATUS COULD NOT BE READ**,
exit 1, with the run key in `RUN_*.md`: the run was submitted and may still
be going, so check it in the console before starting another.

**Refresh a STILL RUNNING record; never re-run.** When the budget runs out the
job keeps going on AIDP, but `run_<job>.json` keeps saying STILL RUNNING and
the stage board holds everything behind it. Bring the record up to date from
AIDP — nothing is submitted, cancelled or resubmitted:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" run --workspace <ws> --job snowmig_01_structure --refresh
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" run --workspace <ws> --job snowmig_02_copy_sales --run-key <key>  # started from the console
```

`--run-key` records a run started from the console, which has no local
record; a run AIDP says belongs to another job is refused.

#### Cold start — handled by `run`

A new cluster may not pick up the first run on a new workspace: the job run
reports `RUNNING` while its task run's `startTime` is still `null`. That one
field tells an unstarted run from a working one.

`run` detects an unstarted task after `--cold-start-seconds` (default
**120**), cancels the run, waits for the cancel to reach a terminal state and
resubmits — up to `--cold-start-restarts` times (default **5**; `0`
disables) — and records each restart in `RUN_*.md`. The wait measures
**pick-up, not work**: a task that has started is never cancelled, however
long it runs.

When every attempt is spent and the last run is still unstarted, `run`
cancels it too (so it does not hold the job's slot) and exits 1 with **COLD
START — attempts exhausted**: nothing ran; check the cluster and re-run. If
that cancel did not reach a terminal state, the report says **NOT confirmed
cancelled**: cancel the run in the console before re-running.

When a restart happens, tell the user:

- **The output belongs to the LAST run key, not the first.** `RUN_*.md` lists
  every restart so the report can be matched against the console.
- **A resubmit needs the slot free.** A job allows one run at a time
  (`maxConcurrentRuns: 1`); a run submitted while the previous one still
  holds the slot does not execute. `run` waits for the cancel to finish
  before resubmitting — never cancel and resubmit by hand in quick succession.

The discovery notebook writes the manifest to `reports/` and **backs it up
itself into `backup/`** as `discovery_manifest_<UTC>.json` — every run leaves
its own dated copy, the reference input every later stage reads. Never
re-derive what the manifest already holds, and never upload a backup by hand:
if one is missing, report it rather than filling the gap.

### S7 — Generate the translation plan, by script

A script — not you — translates Snowflake types, names and structures into the
AIDP plan. It reads the manifest and emits one JSON with the **nested**
structure: schemas → tables → columns → types, plus the target name for each.

The script resolves everything with a direct correspondence, using the mapping
vocabularies it ships with. What it cannot map exactly it **flags for review**
rather than guessing. That flag is the deliverable of this step.

**You do not translate types by hand.** Your turn comes at S8, and only for
what the script flagged.

`run` downloads the manifest itself when discovery ends in SUCCESS (to
`migration-artifacts/discovery_manifest.json`). If that download failed, or
you need another file from the workspace, use `fetch` — the console's own
download action, read-only, bytes checked against the size the server
reports:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" fetch        # default: reports/discovery_manifest.json
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" fetch --path backup-snowflake-migration/reports/DISCOVERY.md
```

Then bridge it into the shape the planning stages read:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" ingest \
  --manifest ./discovery_manifest.json --database-name <SOURCE_DB> \
  [--semi-structured string] [--timestamp-ntz timestamp]

"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" plan \
  --bronze-catalog-prefix <the INTERNAL catalog created at S4> \
  [--restrictions <file>]
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" ddl
```

The prefix is required here. S10 creates each approved target name as it
stands and refuses a plan whose catalog is not its `--target-catalog`;
without the prefix the plan's catalog is the source database name — the
EXTERNAL catalog registered at S3 — and S10 refuses it with exit 1.

`ingest` calls the **same type mapper** a live `assess` calls, so a column
planned from the manifest reaches the same verdict as one planned from a live
read. `--database-name` is required and never guessed: a manifest does not
record which database it describes, and a wrong name aims the plan at the
wrong catalog.

Two things `ingest` reports that you must pass on:

- **Views arrive without their SQL.** A manifest carries columns, not
  definitions, so a view cannot be dialect-translated from it and the planner
  refuses it. If views must migrate, plan them from a live `assess`.
- **Lineage was not extracted.** `ingest` writes `dependencies.json` empty
  with provenance `not_extracted`. Tables carry no inter-table dependency and
  manifest views are refused anyway, but it means "not looked at", never
  "looked at and found nothing". Run `deps` against a live session if view
  ordering matters.

### S8 — Resolve the flagged conflicts, grouped

Read every flagged item first. Then group them: on a real estate the same
decision repeats across hundreds of columns, and the flags fall into a handful
of families — semi-structured types that should become strings or maps,
timestamp variants, precision that does not fit, view SQL with no portable
rewrite.

**Ask about the families, not the instances.** One question that settles 400
`VARIANT` columns is right; 400 questions is a failure of this step. Say how
many objects each answer covers, so the user knows the weight of what they are
deciding.

Write the resolved decisions back into the plan JSON. The plan, not the chat,
is what S10 executes.

### S9 — Present the summary, take the go/no-go and the scope

Show tables **in the chat** — not a pointer to a file — covering: what was
discovered, what is planned, what problems were found, what has been created so
far. Then two questions, together:

1. **Go or no-go** on creating the assets.
2. **Full scope or reduced?** The engineer may not want the whole estate. If
   they reduce it, **back up the full plan JSON into AIDP first**, then write
   the reduced plan and run S10 from that. The full plan stays recoverable;
   the reduced one is what executes.

Reducing scope is an **input** change. Never edit a script to make it cover
less.

### S10 — Create the assets, by workflow

`01_create_structure.ipynb`, run as a workflow, reads the approved plan and
creates the schemas, then the empty Delta tables, in one job run — a job run
per table would pay the job start-up cost for every table.

The plan it reads is `ddl_plan.json` **on the workspace**. It gets there
through `provision`, re-run against this migration's own workspace. That push
uploads `plan.json`, `ddl_plan.json` and their reports to `plan/`, backs the
two plans up **dated** into `backup/`, and registers the per-schema copy
workflows (S11):

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" provision --execute --reuse-existing \
  --workspace-name <the S1 name> --plan-label FULL      # before an S9 reduction
# re-plan with --restrictions, then:
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" provision --execute --reuse-existing \
  --workspace-name <the S1 name> --plan-label REDUCED
```

`--reuse-existing` here re-adopts **this migration's own** environment, the
one `provision_result.json` records — not the reuse the CREATE rule forbids.
Never upload a plan with a raw `aidp workspace-object create`: it skips the
backup and the copy workflows, and leaves no record in `PROVISION.md`.

The stage runs in `ddl-plan` mode: its types are engine-translated.
`manifest` mode refuses a connector-built manifest before creating anything:
it records Snowflake types, which Delta rejects or, like `FLOAT` (64-bit in
Snowflake, 32-bit in Spark), accepts with a different meaning. A table another
mode recorded as created is re-checked by a `ddl-plan` run, not skipped.

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" run \
  --datalake-ocid <ocid> --workspace <ws> --job snowmig_01_structure
```

**Stage parameters are not passed on the `run` command line;** `run --param`
is refused. A job task's `parameters` reach the notebook through
`oidlUtils.parameters.getParameter`, which every stage notebook reads, and win
over the notebook's `PARAMS` cell — that is how the per-schema copy workflows
are scoped (S11). Each stage notebook carries its own `PARAMS` cell;
`provision --execute --reuse-existing --refresh-notebooks` rewrites it and
re-uploads, writing the values given as `--stage-param NAME=VALUE`
(repeatable). Without `--refresh-notebooks`, `--reuse-existing` keeps a
notebook already on the workspace, because its PARAMS cell may have been
edited in the console, and `--stage-param` with `--reuse-existing` but without
`--refresh-notebooks` is refused rather than dropped.

NAME is the stage flag without `--` (`schema`, `tables`, `mode`, `dry-run`,
`counts`, …). A name no stage declares is refused; a switch takes
`true`/`false`; a list flag takes a comma-separated value; an unqualified value
some declaring stage would reject is refused (`mode` is
`ddl-plan`/`ctas`/`manifest` in 01 but `skip-existing`/`append`/`overwrite` in
02; write `copy_schema.mode=overwrite` to reach 02 only). To narrow what S10
creates, narrow the **plan** it reads — that is the input — and never edit the
stage logic to make it cover less.

**Creation speed.** A table create is a few metastore round trips. The
structure job lists each schema once (`SHOW TABLES`), so a table known to be
absent skips the `DESCRIBE` before its create; it creates `parallel` tables at
a time within a schema (default 8; each still read back on its own; dry runs
one at a time), and writes its report every few seconds rather than after
every table. AIDP's CREATE TABLE sets the pace: on a cluster with a 2-OCPU
driver, about 2 s a table at the default, so roughly a day per 50,000 tables
per cluster of that size. If the metastore objects to concurrent creates, set
the job's task parameter `parallel=1` — no notebook edit, no re-provision.

Monitor the runs and report progress. Report `verified`, never `executed` — a
batch can report success while statements inside it failed.

### S11 — Generate the data-migration scripts and register their workflows

**One script per SCHEMA, never per table. One workflow per script.** Register
them; **do not run them.**

`provision` does this from the approved `ddl_plan.json`: for every source
schema it moves tables for, one job `snowmig_02_copy_<schema>` with ONE task
running **the same** `02_copy_schema.ipynb` and passing
`parameters: [{"name": "schema", "value": "<SCHEMA>"}]`, which the notebook
reads over its PARAMS literals at run time. One script; each schema its own
job, run history and evidence.

A schema whose job name would collide with a stage job's (a schema named
`SCHEMA` would get the generic `snowmig_02_copy_schema`) gets
`snowmig_02_copy_schema_<schema>` instead, and a job of the right name whose
task parameters name another schema is refused, not adopted. `PROVISION.md`
lists them (schema → job → this push's outcome for it). Re-push after
re-planning to add a schema.

**Stale copy jobs.** A schema reduced out of the plan gets no new job, but a
copy job an earlier push registered for it is still on the workspace and
runnable, so the push reports it as `stale` (and exits 1) until it is deleted —
in the console, or by re-pushing with `--delete-stale-copy-jobs`, which deletes
it (and the schemaless generic job) and records a job as deleted only once it
is gone from the listing. It deletes only a job this migration's records show
it created (`created_jobs` in `provision_result.json`); a same-prefix job on a
reused workspace that no record names is reported and left alone.

**Task-parameter check.** Before a copy job runs, `run` reads the job's task
parameters and refuses a name that matches no spelling of a stage parameter
(`dryRn=true` would leave `dry-run` False: a real write) or a value the stage
refuses (`mode=apend`) — before any job start-up is spent. A job definition
that cannot be read is reported and does not block.

**What Snowflake refreshed or scheduled** gets generated jobs, never
scheduled ones:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" jobs              # offline: GENERATED_JOBS.md + notebooks
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" jobs --register   # create them in AIDP, UNSCHEDULED
```

A dynamic table or materialized view the plan migrated as a table snapshot
with `refresh generated` gets an `INSERT OVERWRITE` refresh notebook; each
task graph gets one job with its tasks in dependency order, SQL DML bodies
over migrated tables translated, everything else (CALL, Scripting, MERGE,
UPDATE, streams, anything not migrating) a stub that FAILS when run. Every
job is MANUAL: the source cadence (TARGET_LAG, SCHEDULE) is recorded and,
where exact, shown as a PAUSED Quartz proposal — never applied. Present
GENERATED_JOBS.md, stubs and "not carried" list first, and register only on
explicit confirmation; say that the schedule is recorded, not applied.

### S12 — Propose the warehouse-equivalent clusters

List the Snowflake warehouses with the cluster proposed for each, say plainly
that they will be created with **default configuration**, and create them only
on explicit confirmation.

### After S12 — release the migration's compute

The migration cluster exists to run discovery and structure creation. When
S10 is verified and the run's record is written, release it:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" teardown            # dry run: lists the clusters
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" teardown --execute  # stop them, read back
```

Only clusters that `provision_result.json` proves this migration **created**
(`created: true`, carried forward by re-pushes into the same workspace;
clusters an earlier push created stay listed under `earlier_allocations`) are
touched — never one found by name. A cluster the record names but did not
create — adopted with `--reuse-existing`, or the one
`compute.warehouse_clusters: existing` maps the warehouses to — is listed in
`TEARDOWN.md` as not this migration's and left alone. A cluster whose
provenance the record cannot prove is `provenance_unknown`: not touched,
reported as a failed step (exit 1) asking you to confirm in the console, and
listed apart, unbilled, in the billing report. An executed record that names no
workspace makes teardown exit 1 ("cannot tell what was allocated"), never
"nothing to terminate". A cluster whose create was accepted but whose key was
never returned is a failed step (exit 1): look it up by name in the console;
teardown never picks one by name.

`stop` (default, `teardown.action` in the config) is reversible and leaves the
registered copy jobs working; `--action delete` is final and must be asked
for. The workspace, the catalogs and the jobs are kept: they are the
migration's output and its record.

**Teardown has three scopes**, each a dry run unless `--execute`: `compute`
(the default, above), and two opt-in scopes:

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" teardown --scope credential   # only the Snowflake credential on the workspace
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" teardown --scope all          # UNDO the migration
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" teardown --scope all --include-data  # ...and the INTERNAL catalog with its rows
```

- `--scope credential` deletes `backup-snowflake-migration/plan/<stem>.json`
  (the `snowflake:` block `provision --source-config` placed there) and reads
  it back gone. Afterwards the copy jobs can no longer read Snowflake — run
  it once the copies are done.
- `--scope all` is for a lab, a rehearsal or an abandoned migration: the
  credential, the jobs, the clusters, the catalogs the migration created and
  the workspace, in that order, each only where the record proves this
  migration created it (`provision_result.json` provenance, the catalog
  ledger) and each read back gone. A workspace, cluster or catalog adopted
  with `--reuse-existing` is never deleted; a catalog the catalog stage
  created stays this migration's even after a re-run records it `reused`. The INTERNAL catalog holds the migrated tables, so it goes only
  with `--include-data`. Deleting is final: show the dry run's list and get
  an explicit yes in that turn.

With `reporting.publish_each_stage: true`, every stage — this one included —
also writes the accumulated report (tokens per stage and phase, the phase
report, the run log) and a per-step snapshot into `report/output` in the
migration workspace, and each in-AIDP notebook saves its own `SXX_*.json`
there. Say where to find it.

---

## Where output goes: one directory, named for what it is

Every stage writes to **`./migration-artifacts/`**, in the working directory
the command runs from — the user's project, not the plugin folder. One
directory; nothing is created anywhere else. Run every stage of a migration
from the same directory.

It persists between commands because the stages chain: `plan` reads the
`inventory.json` that `assess` wrote. It lives outside the plugin because an
installed plugin sits in a per-version directory that an update replaces.
Keep it until the migration is torn down: `provision_result.json` and
`resources.jsonl` are the record `teardown` works from. It carries a
`README.md` describing each file and a `.gitignore` of its own, so it is never
committed from the user's repository. These files name a real estate's
databases, schemas, tables and columns — customer data that must never reach a
public repository.

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" assess          # writes there by default
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" clean           # removes it
```

`clean` deletes only that directory. It refuses to touch an `--out-dir` the
operator named.

**Nothing else is ever created.** No virtualenv in the user's home, no cache
beside the plugin, no scratch left behind. `bin/snowmig` runs on the current
interpreter when it already imports the dependencies, and otherwise builds a
venv in a temp directory that its `EXIT` trap removes — on success, on
failure and on interrupt. `bin/snowmig-test` follows the same rule.

Use `--out-dir` only when the user wants artifacts kept somewhere they chose.
Any directory it creates, or finds empty, also gets its own `.gitignore`.

## Rules that apply at every step

0. **THE ENGINE IS THE METHOD. Never do by hand what a stage does.** Every
   decision — what can move, how a type maps, what a target is named, whether
   a create landed — belongs to `snowmig.py` and the scripts in
   `data-migration-scripts/`. They are deterministic, they refuse rather than
   guess, and each run leaves an auditable artifact.

   - **Do not re-implement a stage.** If a stage exists, run it. If its output
     is not what the user needs, change the inputs, not the method.
   - **Do not translate SQL or types yourself.** The mappers refuse what they
     cannot do exactly; that refusal is the deliverable of S7, and S8 is where
     you resolve it with the user.
   - **Do not create AIDP objects by hand**, and do not verify by eyeballing
     the console — the stages read objects back and compare.
   - **If the engine or the scripts cannot be found, STOP and say so.** The
     engine lives at `${CLAUDE_PLUGIN_ROOT}/engine/snowmig.py` and the in-AIDP
     scripts at `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`. A missing
     engine is a broken install to report, never a reason to improvise a
     migration.

   What IS yours: reading artifacts, explaining them, driving the
   conversation, and resolving at S8 what the script flagged.

1. **Change inputs, not scripts.** The scripts are the audited, resumable part.
   Scope, mode and target are arguments and plan files. If you find yourself
   editing a script to change behaviour, you are on the wrong path — change its
   input, or report that the script lacks the option.

2. **Everything runs as a WORKFLOW.** Not an interactive notebook. A workflow
   is a job with task runs: it is logged, it can be re-run, and
   `aidp workflow export-task-run-output` produces the HTML/ipynb evidence.
   Interactive execution leaves nothing behind and is not acceptable as the
   record of a migration.

   **That includes finding out what is in the estate.** Discover schemas and
   tables with the discovery workflow in `connector` mode — two
   `INFORMATION_SCHEMA` queries for the whole database — not by walking
   three-part names against the EXTERNAL catalog, which costs a `DESCRIBE` per
   object. This is decided; do not re-evaluate it. See S6.

3. **Evidence is a deliverable, not a side effect.** Every step leaves a file
   or a workflow run inside AIDP. Manifests are backed up before they are used,
   plans are backed up before they are reduced, and reports are written where
   the next person can find them.

4. **Read-only against Snowflake — enforced, not promised.** The transport
   rejects any statement whose verb is not `SELECT`, `SHOW`, `DESCRIBE`,
   `DESC`, `WITH` (only when what follows the CTE list is a `SELECT`) or
   `EXPLAIN`, before it reaches Snowflake.
   **Nothing is ever written to or dropped from the source**, whatever the
   credential permits and whatever any prompt asks for.

5. **Structure first; data only by an explicit job.** S1–S12 create schemas
   and empty tables and move no rows. Rows are copied only when the operator
   runs `snowmig_02_copy_schema`, one schema per run, after S12 and on their
   own decision — never as part of the runbook and never on their behalf.
   Say which of the two the user is asking for whenever their language
   suggests they expect data.

6. **Dry-run is the default; approval does not carry.** Nothing is created on
   AIDP without `--execute`, a resolved destination, and confirmation **in that
   turn**. A config file holding a destination is not an approval. Two
   opt-ins write without `--execute` — `jobs --register` and
   `reporting.publish_each_stage: true` — and need the same confirmation.

   AIDP coordinates come from the config's `aidp:` block or a flag, which
   wins. When a value comes from the file the CLI prints `destination from the
   config file: ...` — repeat that to the user before any `--execute`.
   **If no destination is supplied, assume none.** Reports say *not supplied*
   rather than inferring a region, catalog or cluster, and the plugin never
   picks one of several catalogs on the user's behalf.

7. **Never present an approximation as a conversion.** An unmapped type is
   reported as flagged, with the reason, and resolved at S8 with the user. Do
   not substitute a "close enough" type silently.

8. **A halt is a halt.** Exit code 3 means a condition to resolve with the
   user, never an error to retry and never one to pick a winner on. From
   `assess` or `plan` it is an identifier-case or target-name collision:
   show the collisions and stop. From `ddl` it is a column type the target
   refuses at CREATE TABLE — `TIMESTAMP_NTZ`, when the estate was assessed
   with `--timestamp-ntz preserve` or `--mapping-defaults off`:
   show the columns stderr and `DDL_PLAN.md` name, and put the remedy
   (`ddl --timestamp-ntz timestamp`, offline, which changes timezone
   semantics) to the user as a decision.

9. **Never report success ahead of verification.** AIDP creates are
   asynchronous: an object exists when it has been read back, not when the
   request was accepted. "Pending", "still settling" and "exit code nonzero"
   are not success — name which one it is. Never say a migration is complete
   before every object shows `verified`.

10. **After every step, say where the run stands — unprompted.** What this step
    actually produced, which step is next, and the command or confirmation that
    would run it. The user should never have to ask "where are we".

11. **Always present the data-movement architecture options.** S1–S12 create
    structure; they move no rows. *How* rows eventually move is a separate
    decision, and it belongs to the customer because it drives cost,
    wall-clock and whether a later migration can run unattended. Raise it at
    S11, when the per-schema copy scripts are handed over, and do not let the
    section pass unread.

    There are six. `A1` unload to object storage, `A2` federate through the
    EXTERNAL catalog, `A3` redirect ingestion, `A4` Iceberg interop, `A5`
    hybrid waves, and **`A6_CUSTOMER_DEFINED` — the open slot**. The customer
    may already run a pattern better than anything here, or may simply not
    have decided; both are valid answers and neither is forced into one of
    the others.

    If nothing has been chosen, say plainly that the architecture is
    **undecided**. Record a choice with `snowmig.py data-options --choose
    <id> --rationale "..."`; a rationale is mandatory. **None of the six is
    implemented** — recording a choice executes nothing, and
    `execute_transfer()` refuses by design.

    When the user describes their own design, record it verbatim with
    `--custom-name` and `--custom-description-file`. It is **never mapped**
    onto one of ours: a customer design filed under `A1` reads as an assessed
    variant of `A1`, and it is not. Never paraphrase their design into ours.

## Scale is the design constraint

An estate is thousands to hundreds of thousands of objects. Everything above is
shaped by that: two queries for discovery instead of one per object, a schema
as the unit of work instead of a table, resumable scripts that skip what a
report already records as done, and grouped decisions instead of per-column
questions. If you find yourself doing something once per table — asking,
translating, verifying, creating — stop and work per schema or per family
instead.

## Engine

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" <stage> [...]
```

AIDP calls go through `oci raw-request` whenever `oci` is installed;
workspace files, job cancel and delete, and catalog delete always use the
`aidp` CLI, so both are needed. The engine prints which backend it chose and
fails loudly if neither is present rather than guessing a transport.

The in-AIDP data plane is `${CLAUDE_PLUGIN_ROOT}/data-migration-scripts/`:
`00_discover_snowflake.ipynb`, `01_create_structure.ipynb`,
`02_copy_schema.ipynb`, `03_reconcile.ipynb`.

**Everything that runs on AIDP is `.ipynb`; the `.py` files are local only.**
The `.py` under `${CLAUDE_PLUGIN_ROOT}/engine/dataplane/` — the four stages
over the shared `snowmig_source.py` — are the sources the notebooks are
generated from (`"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" build-notebooks`). Never
hand-edit a generated notebook; a rebuild overwrites it.

## One thing to raise even when nobody asks

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`** — it maintains layout and
reclaims storage in the background, un-asked. AIDP has both, plus `ZORDER BY`
and liquid clustering, and **runs none of them for you**. Nothing is lost in
the migration; the *responsibility* moves, on day one, to whoever owns the
target. Raise it before S10, not after.
