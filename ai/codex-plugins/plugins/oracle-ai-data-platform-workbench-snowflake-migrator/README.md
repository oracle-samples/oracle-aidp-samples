# Oracle AI Data Platform — Migrator for Snowflake (Codex plugin)

Migrate a Snowflake database onto Oracle AI Data Platform (AIDP): structure
first, then data, schema by schema. The plugin:

- inventories the Snowflake estate and states, per object, what can migrate
  and what cannot, with the reason;
- registers the source as an EXTERNAL catalog (a read-only pointer at the
  live Snowflake database) and creates the INTERNAL target catalog;
- provisions the AIDP migration environment: a workspace, the
  `migration_assets` cluster, the migration notebooks and their jobs;
- runs discovery and structure creation **inside AIDP**, as jobs, from a plan
  you approve;
- leaves one copy job per schema that copies the rows and verifies them (row
  counts, and exact decimal sums on request), for you to run when you decide.

**The control plane copies no data itself.** Rows move only when the operator
runs one of the in-AIDP copy jobs: `snowmig_02_copy_<schema>`, one per schema
of the approved plan, each running the `02_copy_schema` notebook (before a
plan is pushed, the single `snowmig_02_copy_schema` job). The jobs carry **no
schedule**, and row data flows from Snowflake to your AIDP cluster and catalog
storage without passing through your machine. Stored procedures, streams and
pipes are inventoried with effort bands, not translated. Tasks, dynamic tables
and materialized views get generated MANUAL jobs (`snowmig jobs`, see
[Generated jobs](#generated-jobs)): SQL translated where the translation is
exact, a stub that fails when run where it is not.

Two modes: **dev** (`bin/snowmig demo --out-dir ./snowmig_demo` — the whole
pipeline against a built-in emulation, no credentials, every artifact
narrated in `DEMO.md`) and **prod** (the run below, with `--execute` gates).

## Install

Register the Oracle AIDP Codex marketplace and install the plugin:

~~~bash
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main --sparse .agents --sparse ai/codex-plugins
codex plugin add oracle-ai-data-platform-workbench-snowflake-migrator@oracle-aidp-codex
~~~

From a clone of this repository, register the local folder instead:
`codex plugin marketplace add ./oracle-aidp-samples/ai/codex-plugins`. Start a
new Codex thread after installing, so the skills are loaded.

**Start here: [How to run a migration, from zero](#how-to-run-a-migration-from-zero).**
The design record — the full Snowflake-to-AIDP mapping and what is
deterministic versus AI-assisted — is
[MIGRATION-ARCHITECTURE.md](MIGRATION-ARCHITECTURE.md).

---

## How to run a migration, from zero

A migration is twelve steps, **S1–S12, in a fixed order**.
`skills/snowflake-migrator-overview/SKILL.md` is the authority on the
sequence; the sections below are its runnable form and name the steps each
one covers. Every command reads one config file, and nothing writes to AIDP
without `--execute` — except `run`, which has no dry run: it starts a job
that `provision` already created; `jobs --register`, where the flag is the
confirmation; and, when you set `reporting.publish_each_stage: true`, the
report upload after each stage.

> **Paths.** Commands are written `bin/snowmig <stage>`, as typed from a
> checkout of this repository. With the plugin installed, use
> `<plugin-root>/bin/snowmig` (or `<plugin-root>/engine/snowmig.py`) from any
> directory, where `<plugin-root>` is the plugin folder `codex plugin list`
> shows; the in-AIDP notebooks are in `<plugin-root>/data-migration-scripts/`. The
> config file and the artifacts (`./migration-artifacts/` by default) live in
> **your** working directory, so run every stage of a migration from the same
> one — an installed plugin's directory may be read-only, and an update
> replaces it.

### 0. Prerequisites

| | |
|---|---|
| Snowflake | a **read-only** role and a service user, authenticated by password or key pair. Both go in the one config file (a PEM can be pasted inline under `private_key:`). No write grant is needed: the transport refuses any statement not led by a read verb, and the read-only role is what prevents writes |
| AIDP | the `oci` CLI configured, the `aidp` CLI (`pip install aidp-python-client aidp-cli`) for workspace files, and the **aiDataPlatform OCID** |
| Local | Python 3.10+ and `pip install -r engine/requirements.txt` (`bin/snowmig` does this for you when needed) |

### 1. One config file

```bash
bin/snowmig init-config
```

This writes `./snowmig-config.yaml` from `snowmig-config.example.yaml`,
readable by you alone (`0600` on POSIX; on Windows the file inherits your
profile's ACL), and refuses to overwrite an existing one. Copying the
template by hand works too — `chmod 600` it. **The Snowflake connection and
the AIDP destination both live in this file**, so nothing is repeated on the
command line:

```yaml
snowflake:
  account: MYORG-MYACCOUNT
  host: MYORG-MYACCOUNT.snowflakecomputing.com   # the Account/Server URL
  user: MIGRATION_READER
  warehouse: MIGRATE_WH
  database: SALES_DB
  role: MIGRATION_READER_ROLE
  schema: PUBLIC
  auth: password
  password: the-password            # or: auth: keypair + `private_key: |` inline

aidp:
  datalake_ocid: ocid1.aidataplatform.oc1.<region>.<unique-id>
  # catalog: my_target_catalog        # the INTERNAL target catalog (S4)
  # external_catalog: my_snowflake_source
```

The CLI looks for it in this order, and every stage prints which file it
used:

1. `--config <path>`, when given;
2. `./snowmig-config.yaml` in the working directory;
3. the same name beside the plugin.

Any field can be overridden per run with a flag (`--role`, `--warehouse`,
`--catalog`, `--datalake-ocid`).

**AIDP authentication is not in this file.** The plugin drives the `oci` and
`aidp` CLIs with your OCI setup (`~/.oci/config`, from `oci setup config`):

- **Profile.** When the config sets `aidp.oci_profile`, it is announced and
  passed as `--profile <name>` to every `oci` and `aidp` call. Otherwise
  each CLI uses `OCI_CLI_PROFILE` from your shell, else `DEFAULT`.
- **Auth mode.** Every `oci` and `aidp` call is given an explicit `--auth`:
  `aidp.oci_auth` when set (`api_key` or `security_token`), else
  `OCI_CLI_AUTH` from your shell, else the mode the profile implies — a
  profile with a `security_token_file` is `security_token`, any other is
  `api_key`.
- Every `aidp` call is also given the `--region` of the DataLake OCID.

The config names which AIDP resources to use, never a credential for them.

**Rules for the inline secret.** The file holds live credentials in plain
text:

- It is **gitignored only inside this plugin's own folder**, and the file
  belongs in your working directory — so add `snowmig-config.yaml` (and
  `.yml`, `.json`) to your own `.gitignore` before filling it in. Keep it out
  of commits, tickets and chat.
- **Secrets are never echoed**: `preflight` and every report render the
  config through a redactor.
- **An agent asks before reading it**, and never asks you to paste a secret
  into the conversation. If one ends up there, rotate it.
- A destination read from the file is **announced** before anything acts on
  it, and writing still needs `--execute`.

### 2. Confirm the config and test the source

```bash
bin/snowmig preflight --test-source
```

`PREFLIGHT_CONFIG.md` lists every field with what it is for, secrets masked
(an inline secret reads as *inline*, a `*_path` shows the path), then the
checks. Review it with the account owner before going further: a wrong host
or role is cheapest to fix here.

- `--test-source` connects to Snowflake; it is opt-in because it resumes the
  warehouse.
- The AIDP end is checked when the config carries both `datalake_ocid` and
  `catalog`: the report says whether that catalog exists and whether it is
  `INTERNAL` or `EXTERNAL`.
- **A skipped check is not a pass.** The report says which end was not
  configured.

### 3. Assess the estate (optional preview, read-only, from your machine)

This pass answers *"what is in this account?"* before anyone commits to a
migration. It is **optional**: it reads Snowflake from your machine and
leaves no job run or evidence inside AIDP. The migration's own discovery is
the `snowmig_00_discover` job (step 6, S6).

```bash
bin/snowmig assess
bin/snowmig deps
bin/snowmig maintenance
bin/snowmig security
bin/snowmig compute --credit-price <USD>
```

Coordinates come from the config; override them per run with `--role`,
`--warehouse` or `--database`. Read `INVENTORY.md`, `CENSUS.md` (everything
that is not a table or view), `SECURITY.md` (protections that do not carry
over) and `MAINTENANCE.md`. `compute` writes `COMPUTE_PROPOSAL.md` and the
`warehouses.json` that `provision --warehouse-clusters` reads. A live
`assess` is also how views are planned (step 7).

A table with a `GEOGRAPHY`/`GEOMETRY` column is blocked until someone
decides; `--geospatial string` carries it as text, as a deliberate decision
(see [the type modes](#semi-structured-geospatial-and-timestamp-types)).

### 4. Provision the migration environment inside AIDP (S1, S2, S5)

**The environment comes first**, because every AIDP write — including a
catalog registration — is addressed by four coordinates (DataLake OCID,
workspace, cluster, catalog), and a write without all four stops on
`AIDP target coordinates not supplied`.

```bash
bin/snowmig provision \
  --workspace-name "<the Snowflake account or project name>" \
  --source-mode connector --source-config ./snowmig-config.yaml \
  --external-catalog <name> --target-catalog <internal catalog> \
  --datalake-ocid <ocid> [--warehouse-clusters]                      # dry run
bin/snowmig provision ... --execute
```

This creates:

- the workspace, its name translated to the charset `[a-z0-9_]` (the rename
  is reported);
- the `migration_assets` cluster, plus cluster libraries from
  `data-migration-scripts/requirements-aidp.txt` (it ships with no active
  entries);
- with `--warehouse-clusters` (S12, once the warehouse list is approved),
  **one cluster per Snowflake warehouse** on the AIDP default config, named
  from its base name (`COMPUTE_WH` → `compute`; names that would collide keep
  the full warehouse name). It reads `warehouses.json`, so run `compute`
  first; sizing stays a decision in `COMPUTE_PROPOSAL.md`;
- the workspace folder `backup-snowflake-migration/` (`scripts/`, `plan/`,
  `reports/`, `backup/`) holding the notebooks;
- 3 + N **unscheduled** jobs: discover, structure and reconcile, plus one copy
  job per schema of the approved plan (a single generic copy job until a
  plan is pushed).

`--external-catalog` and `--target-catalog` pre-declare names for the job
parameters; the catalogs do not have to exist yet. Read `PROVISION.md`: a
pending step is reported as pending.

**Hand-off.** After `--execute`, `provision_result.json` records the
**workspace key** under `workspace.key` and the **cluster key** under
`cluster.key`, and the CLI and `PROVISION.md` print both in a hand-off block.
Every later AIDP command needs them. There are two equivalent ways to supply
them — pick one and keep to it:

- pass `--workspace <key> --cluster-id <key>` on each command (as the
  commands below do; the config file stays untouched), or
- paste them into `aidp.workspace` and `aidp.cluster_id` in
  `snowmig-config.yaml` (the commented lines in the template).

`provision` does not write them back into the config, and later commands
never read them from `provision_result.json` implicitly, so a record from
another migration cannot redirect a write. The one exception is the opt-in
per-stage report publish (`reporting.publish_each_stage: true`), which
uploads reports to the workspace that record names. Every command prints the
destination it resolved and whether each value came from a flag or from the
config file.

**The Snowflake credential on the workspace.** `--source-config` places the
config's `snowflake:` block — only that block, as JSON — at
`backup-snowflake-migration/plan/<config stem>.json`, so the in-AIDP
notebooks can reach Snowflake. It carries the credential, so it is uploaded
only when you pass the flag. The `aidp:` block is not copied, and a config
whose secret is a `*_path` is refused before anything is uploaded, because
that path does not exist on the cluster. The copy is recorded as holding the
credential once it is read back on the workspace. A re-push with a different
`--source-config` keeps the earlier object on the record, flagged as holding
the previous credential, until you remove it. Remove the credential with
`teardown --scope credential` once the copies are done (step 10).

### 5. Register the source as an EXTERNAL catalog, then create the INTERNAL target (S3, S4)

One Snowflake database becomes one AIDP catalog. If the account holds
several, choose the one this migration covers (`bin/snowmig databases` lists
them); another database is another migration.

```bash
bin/snowmig catalog --catalog <name> \
  --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>          # dry run
# then, after reading CATALOG.md:
bin/snowmig catalog ... --execute --test-connection
```

`--datalake-ocid`, `--workspace` and `--cluster-id` are **required for
`--execute`** unless the config's `aidp:` block carries them (the step 4
hand-off); the dry run does not need them. The EXTERNAL catalog is a
read-only pointer at the live source and copies nothing; the Snowflake
credential it registers is read from the config file. `--test-connection`
runs only with `--execute`, because the connection is tested on an existing
catalog, and a `PENDING` result is reported as pending. **If the test returns
`FAILED` without a reason, keep the registration and continue** — discovery
(S6) validates the connection through the connector. Do not delete and
re-register to make the test pass.

Then the **INTERNAL** target catalog, which this plugin creates as a
container:

```bash
bin/snowmig catalog --catalog <internal catalog> --catalog-type standard --execute
```

`standard` is the CLI's name for AIDP's `INTERNAL` catalog type. Each catalog
keeps its own record (`catalog_result_<name>.json`, `CATALOG_<name>.md`), so
dry-running S4 after S3 has executed leaves the S3 record intact.

A catalog that already carries the name (in any case) is reused only when
the resource ledger (`resources.jsonl`) records this migration creating it
on this DataLake — re-running `catalog --execute` is safe — or when you pass
`--reuse-existing`. Otherwise the stage refuses it, exit 1, and nothing is
written into it. A catalog of the other type (an INTERNAL one where the
EXTERNAL registration was asked for, or the reverse) is refused either way.
`bin/snowmig catalogs` lists the catalogs the DataLake holds, with their
types.

The container is not the structure. A control-plane table create is
asynchronous (`202 Accepted`), so schemas and tables are created on AIDP
compute by the structure job at S10, where each create is read back.

### 6. Check both ends, then discover inside AIDP (S6)

```bash
bin/snowmig smoke \
  [--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>]
```

Anything already in the config's `aidp:` block can be left off; the CLI
prints the destination it resolved. **Read that printed line before you
approve a later `--execute`** — it is the moment to notice a config written
for a different environment. Without all four coordinates the run exits 1
with `verdict: PARTIAL`: only Snowflake was checked, which is neither a pass
nor a connectivity failure. Exit 0 needs both ends.

Then open `backup-snowflake-migration/scripts/diagnose_environment.ipynb` in
the workspace and run it once. It gives a verdict per check — the workspace
mount, network reach from the cluster to Snowflake, the credentials through
the AIDP Snowflake connector, and whether the EXTERNAL catalog lists the
source's schemas — and writes nothing.

Now discovery:

```bash
bin/snowmig run --workspace <ws> --job snowmig_00_discover
```

The job reads the whole database through the AIDP Snowflake connector in two
`INFORMATION_SCHEMA` queries, writes `discovery_manifest.json` and
`DISCOVERY.md` to `reports/`, and backs the manifest up, dated, into
`backup/`. When it ends in SUCCESS, `run` downloads the manifest into the
artifacts directory; `bin/snowmig fetch` downloads it on demand (`--path`
for any other workspace file).

How `run` behaves, for every job:

- The DataLake OCID and workspace come from the config's `aidp:` block, or
  from `--datalake-ocid` / `--workspace`.
- It polls the run to a terminal state and writes `RUN_*.md` with the task
  output. When the poll budget runs out it reports **STILL RUNNING**; bring
  the record up to date later with `run --job <name> --refresh` (nothing is
  resubmitted). A status that cannot be read is reported as **STATUS COULD
  NOT BE READ** (exit 1): check the console before starting another run. A
  run started from the console is recorded with `--run-key <key>`.
- If a run's task has not started after `--cold-start-seconds` (default
  120), `run` cancels it and resubmits, up to `--cold-start-restarts` times
  (default 5). A task that has started is never cancelled. The output
  belongs to the last run key; `RUN_*.md` lists every attempt.

Discovery uses `connector` source mode, the default. The `external-catalog`
mode reads through three-part names and costs a `DESCRIBE` per object; use
it only on explicit request.

### 7. Plan, review and approve (S7–S9)

Turn the manifest into the inventory the planner reads, then plan and
generate the DDL — all offline:

```bash
bin/snowmig ingest --manifest <artifacts dir>/discovery_manifest.json \
  --database-name <SOURCE_DB>
bin/snowmig plan --bronze-catalog-prefix <internal catalog> [--restrictions ./restrictions.json]
bin/snowmig ddl
bin/snowmig summary
```

- `--database-name` is required: a manifest does not record which database
  it describes.
- `ingest` uses the same type mapper as a live `assess`. **A view's SQL is not
  part of the in-AIDP discovery manifest, so views are planned from a live
  `assess`** (step 3), which writes the same `inventory.json`; add `deps`
  when view ordering matters.
- `--bronze-catalog-prefix` names the INTERNAL catalog from S4. The
  structure job creates each approved target name as it stands and refuses a
  `--target-catalog` that is not the plan's catalog. Without the prefix the
  plan's catalog is the source database name — under this runbook, the
  EXTERNAL pointer — and S10 refuses it.
- `--restrictions` narrows the scope (see [Restrictions](#restrictions)); a
  canary of a few tables is a good first wave.

The planning stages resolve everything with a direct correspondence and
**flag** what they cannot map exactly (S7). Resolve the flags with the
owner, grouped by kind — one decision covers every column it applies to
(S8) — and record each decision in the plan, which is what S10 executes.

**`PLANNED_OBJECTS.md` is the approval artifact — stop here for sign-off
(S9).** From this point `ddl_plan.json` is the authority: the in-AIDP
notebooks create only what it contains and report anything else as
`not_in_plan`.

### 8. Create the structure (S10–S12)

Push the approved plan to the workspace with `provision` itself, never by
hand, then run the structure job:

```bash
bin/snowmig provision --execute --reuse-existing --workspace-name <the S1 name> \
  --plan-label FULL        # REDUCED after an S9 scope reduction
bin/snowmig run --workspace <ws> --job snowmig_01_structure
```

The push uploads `plan.json`, `ddl_plan.json` and their reports to `plan/`,
backs the two plans up, dated, into `backup/`, and registers one copy job per
schema of the plan (S11). To reduce the scope, push the full plan first
(`FULL`), re-plan with `--restrictions`, and push again with `--plan-label
REDUCED`: the full plan stays recoverable in `backup/`, and the reduced one
is what runs. `--reuse-existing` here re-adopts this migration's own
environment, as `provision_result.json` records it.

- Catalogs, `--source-mode` and the credential path not given as flags are
  carried over from the earlier push's record (same aiDataPlatform and
  workspace), so a re-push does not switch the source mode.
- A push keeps what earlier pushes created: the clusters from step 4 stay in
  `provision_result.json` (under `earlier_allocations`), so `teardown` still
  reaches them. A push that halts before recording a workspace — a name
  collision without `--reuse-existing` — writes
  `provision_result.halted.json` / `PROVISION_HALTED.md` and keeps the
  earlier record.
- A schema reduced out of the plan gets no new copy job. A copy job an
  earlier push registered for it is reported `stale` (exit 1) until it is
  deleted — in the console, or by re-pushing with
  `--delete-stale-copy-jobs`. That flag deletes only a job this migration's
  records show it created (`created_jobs` in `provision_result.json`,
  carried from push to push); any other `snowmig_02_copy_*` job on a reused
  workspace is reported stale and left alone.

`snowmig_01_structure` works schema by schema: it creates the schemas, then
the empty Delta tables, then the approved plan's views, and reads each table
back (`DESCRIBE`) to compare it with the plan, column by column. Tables are
created `parallel` at a time within a schema (default 8); set the job's task
parameter `parallel=1` to create them one at a time. Each schema's outcome
is in `structure_report_<schema>.json`.

At S12 the migration is **done**: the structure, the notebooks, the plans
and their backups exist, and so do the warehouse-equivalent clusters if you
approved them. **The data migration is not run.** Moving rows is a later
decision the customer makes, with the copy jobs already in place.

#### Stage parameters

Each stage notebook's `PARAMS` cell holds its defaults (`tables`, `mode`,
`dry-run`, reconcile's `counts`, ...). A job task's `parameters` override
them by the same name at run time — every stage notebook reads them with
`oidlUtils.parameters.getParameter` — and that is how each per-schema copy
job passes the schema it covers. A run-level `run --param` is refused; set
values on the jobs with `provision --stage-param NAME=VALUE` (repeatable;
NAME is the stage flag without `--`):

- a name no stage declares is refused; a switch takes `true`/`false`; a list
  flag takes `A,B`;
- an unqualified NAME reaches every stage that declares it, so its value
  must suit them all — `mode` is `ddl-plan`/`ctas`/`manifest` in 01 but
  `skip-existing`/`append`/`overwrite` in 02 — and one that does not is
  refused. Qualify it to reach one stage: `copy_schema.mode=overwrite`
  (stages: `discover`, `structure`, `copy_schema`, `reconcile`);
- with `--reuse-existing`, add `--refresh-notebooks`; otherwise the notebooks
  already on the workspace are kept and the value is refused rather than
  dropped;
- next to per-schema copy jobs, `schema` and `copy_schema.schema` are refused
  (each job's task parameter wins), while `structure.schema=` narrows 01
  only; `tables` is refused when there are two or more copy schemas, because
  the one shared `02_copy_schema` notebook would narrow every copy job.

The `PARAMS` literals can also be edited in the console. There is no driver
wrapper: each job runs the stage notebook itself. Every stage is resumable —
a re-run skips what its report already records as done.

### 9. Copy the data and reconcile, when the customer decides

| Job | What it does | Report |
|---|---|---|
| `snowmig_02_copy_<schema>` (`snowmig_02_copy_schema` before a plan is pushed) | one job per schema of the approved plan, each running the same `02_copy_schema` notebook with `schema` as a task parameter; copies that schema and **verifies** it (row counts; `--verify counts+sums` adds exact decimal sums) | `copy_report_<schema>.json` |
| `snowmig_03_reconcile` | compares the plan with what the target catalog holds | **`MIGRATION_REPORT.md`** |

```bash
bin/snowmig run --workspace <ws> --job snowmig_02_copy_<schema>
bin/snowmig run --workspace <ws> --job snowmig_03_reconcile
```

Before a copy job starts, `run` reads the job's task parameters and refuses
a name no stage parameter matches, or a value the stage would reject, before
any job start-up time is spent. Start with one small schema end to end and
read `MIGRATION_REPORT.md` before running the rest.

**`MIGRATION_REPORT.md` is the deliverable.** A table reads `NOT_MIGRATED`
when it has not been attempted yet — expected while the migration runs
schema by schema — and `NOT_IN_PLAN` when the approved plan leaves it out
(not pending). Only `MISSING_DESPITE_REPORT`, `STRUCTURE_FAILED`,
`STRUCTURE_TYPE_DRIFT`, `STRUCTURE_ONLY_COPY_FAILED`, `COUNT_DRIFT` (with
`--counts`), `TARGET_UNREADABLE`, `VIEW_FAILED` or
`VIEW_MISSING_DESPITE_REPORT` mean something is wrong; the full vocabulary
is in [data-migration-scripts/README.md](data-migration-scripts/README.md).
Views are listed too: the structure job creates the plan's views after every
table, and the copy never writes into a view.

### 10. Release the compute, or remove what the migration created

```bash
bin/snowmig teardown                                  # dry run: stop the migration's clusters
bin/snowmig teardown --execute
bin/snowmig teardown --scope credential [--execute]   # only the Snowflake credential on the workspace
bin/snowmig teardown --scope all [--include-data] [--execute]   # undo the migration
```

- **Default scope**: stops (or, with `--action delete`, deletes) only the
  clusters this migration created, and keeps its output — the workspace, the
  catalogs and the jobs. Stopping is reversible and leaves the copy jobs
  runnable.
- **`--scope credential`** removes the credential `--source-config` placed on
  the workspace. Run it after the copies: the copy jobs need it.
- **`--scope all`** is for a lab, a rehearsal or an abandoned migration: the
  credential, the jobs, the clusters, the catalogs it created and the
  workspace, each only where `provision_result.json` and the catalog ledger
  prove this migration created it, and each read back gone. The INTERNAL
  catalog holds the migrated rows; `teardown --scope all` deletes it only
  with `--include-data`.

Anything adopted with `--reuse-existing` is never deleted; a catalog the
catalog stage created stays this migration's even after a re-run records it
`reused`, and a cluster whose provenance the record cannot establish is
left alone and reported (exit 1) for you to confirm in the console. Every
scope is a dry run unless `--execute`, and `TEARDOWN.md` lists what was, or
would be, removed.

### Before a production cutover

Each table is copied at its own moment, so a live source yields a target that
is consistent per table but **not across tables**. Freeze writers, copy from a
point-in-time Snowflake `CLONE`, or plan an incremental re-sync. And remember
what does **not** travel: tasks, streams, pipes, procedures, UDFs and every
masking or row-access policy. `CENSUS.md` and `SECURITY.md` list them.

---

## Object mapping

| Snowflake | AIDP |
|---|---|
| Database | an **INTERNAL catalog** that receives the migrated tables (S4), plus an **EXTERNAL catalog of source type SNOWFLAKE** registered over the live database (S3) — a read-only pointer that copies nothing |
| Schema | Schema in the INTERNAL catalog |
| Table | Managed Delta table, created empty at S10; rows arrive when its schema's copy job runs |
| View | View, created after the tables at S10 — when every Snowflake-only construct in its SQL has an exact rewrite (see [Why a view might not migrate](#why-a-view-might-not-migrate)) |
| Warehouse | Apache Spark compute cluster (see the compute proposal) |

The stand-alone `/snowflake-catalog` and `/snowflake-soft-clone` commands
register the EXTERNAL catalog by default, and create a Standard (INTERNAL)
catalog only when you ask for one.

Bronze mirrors the source 1:1: table and column names are kept. With
`--bronze-catalog-prefix <internal catalog>`, each source schema lands in
that catalog as `<database>_<schema>` (the default `--bronze-schema-style
db_schema`; `db` names it after the database alone). `PLANNED_OBJECTS.md`
shows every target name. Silver and Gold are requirement-driven: the plan
emits **disabled job stubs** that the migrator never triggers, because their
content is a requirement to define with the customer.

## Safety posture

| | |
|---|---|
| Against Snowflake | **Read-only, always.** Only statements led by `SELECT`, `SHOW`, `DESCRIBE` or `EXPLAIN` (a CTE ending in `SELECT`; `GET_DDL` is called through `SELECT`), with a read-only role |
| Against AIDP | **Dry-run by default.** Writing needs `--execute` plus all four target coordinates. The exceptions are `run`, `jobs --register` and the opt-in per-stage report publish |
| Target coordinates | **Never used without being shown.** They come from a flag or the one config file; a value read from the file is printed before anything acts on it, and a write still needs `--execute` and a confirmation in that turn. No environment default, no cache |
| Unmapped types and features | **Blocked with a reason.** Never approximated, never silently defaulted. The type decisions have explicit modes — see [Semi-structured, geospatial and timestamp types](#semi-structured-geospatial-and-timestamp-types) |
| "Verified" | **Means the planned columns are there**, checked by `DESCRIBE`. A name that already belongs to a different structure is reported as a mismatch and left untouched |
| Assessment cost | **Free by default.** Row counts come from Snowflake's maintained metadata; a `COUNT(*)` per object, which executes every view, is opt-in |
| Collisions | **Halt (exit 3).** Identifier-case and target-name collisions stop the run rather than picking a winner |

## Skills and commands

| Command | Skill | What it does |
|---|---|---|
| — | `snowflake-migrator-overview` | the S1–S12 runbook and shared rules; routes to the others |
| — | `snowflake-migrator-bootstrap` | first-run setup: config, authentication, connection check |
| `/snowflake-smoke` | `snowflake-smoke-test` | connectivity and permissions on both ends |
| `/snowflake-assess` | `snowflake-assess-estate` | read-only preview of the estate |
| `/snowflake-plan` | `snowflake-migration-plan` | the migration plan, for approval |
| `/snowflake-provision` | `snowflake-provision-environment` | workspace, cluster, notebooks and jobs |
| `/snowflake-catalog`, `/snowflake-soft-clone` | `snowflake-medallion-clone` | catalog registration and medallion structure |
| `/snowflake-notebook` | `snowflake-clone-notebook` | an offline table-creation notebook for a Standard catalog |
| `/snowflake-compute` | `snowflake-compute-proposal` | warehouse-to-cluster sizing and cost |
| `/snowflake-demo` | `snowflake-migrator-demo` | dev mode against an emulated estate |
| — | `snowflake-stage-board` | where the run stands, stage by stage |

Each planning stage reads the previous stage's artifact (`inventory.json` →
`plan.json` → `ddl_plan.json`) and can be re-run on its own; `summary` writes
the per-object roll-up, `SUMMARY.md`.

## Assessment flags and exit codes

| Flag | Default | Why you might change it |
|---|---|---|
| `--row-counts metadata\|exact\|none` | `metadata` | free, and exact for a settled table. `exact` runs `COUNT(*)` per object and **executes every view** |
| `--semi-structured string\|block` | `string` | `block` refuses a table with `VARIANT`/`OBJECT`/`ARRAY` until a typed design exists |
| `--geospatial block\|string\|wkt` | `block` (or the config's `mapping.geospatial`) | `string` carries `GEOGRAPHY`/`GEOMETRY` as GeoJSON text, `wkt` as WKT text |
| `--timestamp-ntz timestamp\|preserve` | `timestamp` | `preserve` keeps `TIMESTAMP_NTZ`, and `ddl` then halts on it |
| `--mapping-defaults on\|off` | the config's `mapping.enabled` (`true`) | `off` restores the strict modes for one run |

**Exit codes:** `0` ok · `1` error · `3` halt — a condition to resolve with
you, never resolved for you: an identifier-case or target-name collision
(`assess`, `plan`), or a column type the target refuses at CREATE TABLE
(`ddl`; `TIMESTAMP_NTZ` under `--timestamp-ntz preserve`, remedied offline
with `ddl --timestamp-ntz timestamp`).

## Restrictions

Narrow the estate before planning with `--restrictions restrictions.json`.
Every exclusion appears in `PLANNED_OBJECTS.md` with the restriction that
fired.

```json
{
  "exclude_databases": ["SNOWFLAKE_LEARNING_DB"],
  "exclude_schemas": ["STAGE"],
  "exclude_object_types": ["VIEW"],
  "exclude_name_patterns": ["^TMP_", "_BAK$"],
  "max_rows": 100000000
}
```

`include_*` variants act as allowlists. An unrecognised key is an **error**,
not an ignored line, so a typo cannot appear to work while applying nothing.

## Why a view might not migrate

Object references inside a view are left as they are in the default Bronze
mirror (`R40`) and rewritten to the planned names under
`--bronze-catalog-prefix` or a schema-style option (`R41`): whole three-part
names only, never inside a string literal or a comment.

Dialect is the other half. The translator carries 21 rules
(`translate.RULES`):

- 9 have a provably exact rewrite and are translated, with the rule id
  recorded in the DDL plan: `IFF`, `x::TYPE` on a bare column or literal,
  `ARRAY_CONSTRUCT`, `OBJECT_CONSTRUCT`, `DATEADD(unit, n, col)` (exact for
  `DATE` operands only, and the plan says so), `LISTAGG(x, sep)`,
  `"quoted identifiers"` → backticks, `''` → `\'` and `//` line comments →
  `--`. Only those exact forms are rewritten; a form the rule cannot prove (an
  expression left of `::`, `LISTAGG … WITHIN GROUP` or `… OVER`, a
  non-literal `DATEADD` amount) is refused with the construct named.
- 12 others — `QUALIFY`, `LATERAL FLATTEN`, `DATEDIFF`, `PIVOT`, `DECODE`,
  `$$…$$`, … — **block** the view with the construct named, rather than being
  rewritten on a guess.

A mixed view is blocked, never partially translated. Secure views are
blocked unless you opt in with `--secure-views as-view` (see
[Security posture](#security-posture)); a materialized view migrates as a
table snapshot (see [What is not a table or a view](#what-is-not-a-table-or-a-view)).
The authoritative rule table is
[references/dialect-translation.md](references/dialect-translation.md);
[references/type-mapping.md](references/type-mapping.md) summarises it.

## Row counts, and what they cost

| Mode | Source | Cost |
|---|---|---|
| `metadata` (default) | Snowflake's maintained count, from `SHOW`. Views get none | Free |
| `exact` | `COUNT(*)` per object | **Executes every view.** Warehouse time per object |
| `none` | — | Free |

The metadata count agrees with `COUNT(*)` for a settled standard table, but
it can lag very recent DML and is not maintained for external tables, so the
reports label it as metadata and never call it verified. Every blank in a
Rows column carries its reason: not counted, not requested, or a named error.
Rows are verified by the copy job, after the copy.

## Semi-structured, geospatial and timestamp types

| Snowflake | Default | Alternative |
|---|---|---|
| `VARIANT`, `OBJECT`, `ARRAY` | carried as JSON text (`STRING`), with a warning on every affected column (`mapping.semi_structured: string`) | `--semi-structured block`: the table is blocked until a typed struct/map/array design exists |
| `GEOGRAPHY`, `GEOMETRY` | the table is blocked | `--geospatial string` (GeoJSON) or `--geospatial wkt` (WKT, which does not carry a `GEOMETRY`'s SRID): carried as text, with no spatial type, index or predicate support |
| `TIMESTAMP_NTZ` | carried as `TIMESTAMP`, with the timezone caveat recorded on every affected column (`mapping.timestamp_ntz: timestamp`); values are read through the session timezone, so keep sessions on UTC | `--timestamp-ntz preserve`: kept as `TIMESTAMP_NTZ`, which the target refuses at CREATE TABLE, so `ddl` halts (exit 3) |
| a column whose type changed after the plan was approved | the copy refuses the table, `type_drift`, before any row is read (`mapping.source_type_drift: refuse`); re-run `assess` and `plan` to pick up the new type | `mapping.source_type_drift: convert`, then re-run `ddl`: the column is copied under the mapping rules for its new type into the existing target column, with a warning on the column, and the table is recorded `verified_with_conversion`, never `verified` |

**Structured** types are typed, so neither switch applies to them:
`VECTOR(FLOAT, n)` becomes `ARRAY<FLOAT>`, `MAP(K, V)` `MAP<STRING, v>`, a
structured `OBJECT(f T, ...)` a `STRUCT` and `ARRAY(T)` a typed `ARRAY`, each
with a warning for what the typed column does not carry (a VECTOR's dimension,
a numeric MAP key's type). Their full type is read with `DESCRIBE TABLE` (or
`GET_DDL` in the in-AIDP discovery), only for the tables that hold one; where
that read fails, a VECTOR or MAP is blocked with the reason. See
[references/type-mapping.md](references/type-mapping.md).

Carrying JSON as text defers the design rather than completing it: nothing is
lost, but nothing on the target can address a field inside the value until a
struct/map design is agreed (`from_json` / `get_json_object` read it), and a
view using Snowflake path syntax (`col:field`) is blocked. The semi-structured
and geospatial flags are separate because they are separate decisions.
`--mapping-defaults off` (or `mapping.enabled: false`) restores the strict
modes for a run; an explicit flag always wins.

## What is not a table or a view

`assess` also censuses procedures, UDFs and UDTFs, external functions, tasks,
streams, alerts, materialized and dynamic tables, stages (internal and
external, told apart), pipes, sequences, file formats, secrets, network
rules, Streamlit apps, notebooks and container services per database, and
the account's shares, roles, network policies, applications and compute
pools once per run → `CENSUS.md`. **None of them migrate as objects**, and
no procedure or UDF equivalent is generated: rebuilding them on AIDP is
outside this plugin's scope, and each is listed with an effort band so the
work can be planned. Tasks, dynamic tables and materialized views are the
exception, where a translation can be exact: see
[Generated jobs](#generated-jobs). The scope statement travels into
`PLANNED_OBJECTS.md` and `SUMMARY.md`, so the migratable count is never
mistaken for the size of the estate.

For tasks, dynamic tables, materialized views and streams, the census also
records what a generated job needs (`source_facts` in `inventory.json`): a
task's schedule, predecessors and condition, a dynamic table's target lag,
and a stream's base table, from the `SHOW` rows already read. A task's body
and a dynamic table's defining query are kept only with
`assess --capture-definitions`; without it, the generated job or refresh
names the flag. A materialized view's query is always captured, with the
view text the inventory keeps.

A table that `SHOW TABLES` flags as event or hybrid is not a plain table
either: `plan` blocks it with the reason named (`unsupported_object`), under
"Object kinds with no AIDP equivalent" in `PLANNED_OBJECTS.md`.

An **external or Apache Iceberg** table is registered in place (`register_in_place`):
its files already sit in object storage, so nothing is copied.
`bin/snowmig external-registration` (read-only against Snowflake) writes
`EXTERNAL_REGISTRATION.md`: per table, the S3/Azure/GCS path its files come
from and the statement to run on AIDP **once the files are in OCI Object
Storage** — `CREATE TABLE ... USING <format> LOCATION 'oci://...'` for an
external table; for an Iceberg table, `CALL
<iceberg_catalog>.system.register_table(...)` over its root metadata file
after the absolute paths in it are rewritten, so it is not counted as
registered. The plugin does not move the files or rewrite the metadata, and
executes nothing; the report lists what to check on AIDP.

A **dynamic table** or **materialized view** migrates as a **table
snapshot**: planned as a `TABLE` (`snapshot_of` names what it was), created
with `CREATE TABLE` and copied like any other table. Snowflake's refresh does
not travel: `plan` translates the defining query with the view translator and
marks each snapshot `refresh generated` or `refresh NOT generated: <why>`.
`INVENTORY.md` shows `table snapshot (<kind>)`, and `PLANNED_OBJECTS.md` lists
each under "Planned as table snapshots". A secure materialized view is
refused, as secure; a dynamic Iceberg table is registered in place.

A view whose base table or view is blocked or excluded is blocked too
(`dependency_not_migrated`), naming what it depends on.

Procedures and UDFs are read from `INFORMATION_SCHEMA`, which lists the
account's own routines (`SHOW PROCEDURES` also lists system built-ins) and
carries the handler language that drives the effort band. JavaScript
handlers are rated HIGH: they are rewritten rather than ported.

Pay particular attention to **tasks**: a task that populates a table you are
migrating does not move with it, so after cutover that table is no longer
refreshed until an equivalent AIDP job exists.

An **outbound share** is a live contract with a consumer account.
`bin/snowmig share-plan` (read-only; run it after `security`, without which
every shared table is held) writes `SHARE_PLAN.md`: each share mapped to an
AIDP Delta Sharing share, one recipient per consumer account, and the
`aidp delta-share` steps to run, none of them executed. A shared table
carrying a masking or row-access policy is held, because Delta Sharing
publishes the table as stored.

## Generated jobs

```bash
bin/snowmig jobs              # offline: generated_jobs.json, GENERATED_JOBS.md, generated_jobs/*.ipynb
bin/snowmig jobs --register   # also create the jobs in AIDP, unscheduled
```

Task bodies and dynamic-table queries come from `assess --capture-definitions`;
without the flag, those jobs are stubs that name it.

`jobs` reads `plan.json` and `inventory.json` and writes a notebook and a job
spec per object. Every job is **MANUAL**: the cadence Snowflake used is
recorded as the intended one and, where it maps exactly onto a Quartz cron,
written down as a paused proposal that is never sent.

- **Refresh jobs**, one per table snapshot marked `refresh generated`: an
  `INSERT OVERWRITE TABLE <target> <defining query>`, the query translated and
  its references rewritten to the migrated tables. It is a full refresh, not
  Snowflake's incremental one. A snapshot marked `refresh NOT generated` gets
  no notebook.
- **Task-graph jobs**, one per task graph (a root task and everything after
  it), its tasks in dependency order. A single `INSERT`, `DELETE` or
  `TRUNCATE` over migrated tables is translated. Any other body — `CALL`,
  Snowflake Scripting, `EXECUTE IMMEDIATE`, `MERGE`, `UPDATE`, a read of a
  stage, a table function or a session variable, anything that touches an
  object not migrating — is a **stub notebook that raises when run**, with the
  reason. A body over a stream names its nearest equivalent (the Delta change
  data feed of the migrated base table), which is not generated. A `WHEN`
  condition, overlapping execution and a finalizer task are recorded and not
  carried.
- `GENERATED_JOBS.md` maps each load that stops at cutover to the job that
  would take it over, and says which of those are stubs.

`--register` uploads the notebooks to
`backup-snowflake-migration/generated_jobs/`, reads them back, creates the
jobs and confirms them with a job listing. A job is never pointed at a
notebook that is not visible, and a job name that already exists is not
adopted or overwritten (exit 1). `--register` writes to AIDP without
`--execute`: the flag is the confirmation. It needs the DataLake OCID, the
workspace key and the cluster key — from flags or the config's `aidp:` block
only, never from `provision_result.json` (see Hand-off) — and is held to
`decisions.allow_new_objects` like any `--execute`. A multi-task job has not
yet been verified live.

## Security posture

`snowmig security` reports masking, row-access, aggregation and projection
policy *attachments*, secure views, and who holds grants today →
`SECURITY.md`.

A masked column arrives **unmasked**. A row filter is absent. A secure view
is refused by `plan` unless you pass `--secure-views as-view`, which plans it
as a plain view without `SECURE` and says so in a SECURITY WARNING section of
`PLANNED_OBJECTS.md`, on its DDL statement (`R60_SECURE_VIEW_AS_PLAIN`) and at
HIGH in `SUMMARY.md`. The copy **succeeds without the protection**.
Recreating it on AIDP — for example, a restricted view plus ontology
sensitivity granted per role — is a design decision rather than a
translation, so this plugin reports and changes nothing.

If `ACCOUNT_USAGE` cannot be read, the exposure count is `null` and the report
says the question is **unanswered** — never "none found".

## Table maintenance — read before you migrate

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`**: it maintains layout and
reclaims storage in the background. AIDP has `OPTIMIZE`, `VACUUM`, `ZORDER BY`
and liquid clustering as explicit statements, which the table owner schedules.
The capability carries over; the *responsibility* moves to whoever owns the
target.

`snowmig maintenance` (after `assess`) measures what the source actually does
— clustering keys, `automatic_clustering`, Search Optimization,
`change_tracking`, the retention cascade, and reclustering credits plus DML
churn from `ACCOUNT_USAGE` — and writes `MAINTENANCE.md`. An unreadable
`ACCOUNT_USAGE` reports **not measured**, never zero: those two lead to
opposite decisions. The retention *level* is inferred from effective values
rather than probed per table, so the stage costs a handful of queries;
`--probe-table-parameters` opts into the exact path.

This plugin schedules no maintenance — no `OPTIMIZE` or `VACUUM` job is
generated. Source settings with a Delta equivalent are **carried into the
CREATE TABLE** by `ddl` and listed per object under *"Carried into the CREATE
TABLE"*:

- a plain-column clustering key as liquid `CLUSTER BY` (at most four keys,
  each on a column Delta keeps statistics for);
- `retention_time` as `delta.deletedFileRetentionDuration` /
  `delta.logRetentionDuration`, only where it is above Delta's 7 / 30 day
  defaults (nothing is lowered);
- `change_tracking`, or a stream on the table, as
  `delta.enableChangeDataFeed = true`.

The structure job applies them and reads the properties back. `snowmig deploy
--execute` cannot carry them (the catalog API's table body has no field for
them); the plan says so per table (`R13`) and the deploy result lists each
clause under `properties_not_applied`. What cannot be carried — an expression
key, a key on a type Delta cannot cluster on — stays in the DDL plan under
*"Maintenance and layout — decisions, NOT applied"* with the reason, and
raises the object's risk to MEDIUM. Every data-movement option also states
who takes on `OPTIMIZE`/`VACUUM`.

Two points to know up front: on Delta, **`VACUUM` is what bounds time
travel** (on Snowflake the two are independent and automatic), and
**`OPTIMIZE` increases storage until `VACUUM` runs**. The full mapping is in
[references/maintenance-and-layout.md](references/maintenance-and-layout.md).

## Execution backend

AIDP calls go through **`oci raw-request`** against the documented AIDP REST
API whenever `oci` is installed, and provisioning requires it. Workspace
files, job cancel and delete, and catalog delete always use the **`aidp`
CLI**, so both CLIs are needed. The engine prints which backend it chose and
stops with an error if neither CLI is present.

## Tests

Offline, with no credentials and no AIDP:

```bash
bin/snowmig-test            # or: cd engine && python3 -m pytest tests -q
```

An optional end-to-end check against your own Snowflake account (read-only,
deploys nothing):

```bash
cd engine && SNOWMIG_LIVE=1 SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... \
  SNOWFLAKE_PRIVATE_KEY_PATH=... python3 -m pytest tests/test_live_smoke.py -q
```


## Scope

The plugin migrates one Snowflake database per run into AIDP: its schemas,
its tables as managed Delta, and the views whose SQL translates exactly, with
per-schema jobs that copy and verify the rows. The rest of the estate —
procedures, UDFs, tasks, streams, pipes, policies, shares and everything else
listed in `CENSUS.md` and `SECURITY.md` — is inventoried and reported;
rebuilding it on AIDP is outside the plugin's scope, as are scheduling table
maintenance and choosing a long-term data-movement architecture
([references/data-movement-options.md](references/data-movement-options.md)).
On a new estate, run `diagnose_environment.ipynb`, then one small schema end
to end, and read `MIGRATION_REPORT.md` before scaling out.

## Docs

- [README.md](README.md) — this file: what the plugin does and how to run it
- [MIGRATION-ARCHITECTURE.md](MIGRATION-ARCHITECTURE.md) — the migration design: the Snowflake-to-AIDP mapping, the run order, what is automated
- [ARCHITECTURE.md](ARCHITECTURE.md) — the CLI engine: stages, invariants and gates
- [data-migration-scripts/README.md](data-migration-scripts/README.md) — the in-AIDP notebooks, their parameters, report statuses and verdicts
- [PRIVACY.md](PRIVACY.md) — what leaves your machine, and where the credentials go
- [CHANGELOG.md](CHANGELOG.md) — release notes
- [references/type-mapping.md](references/type-mapping.md) — the type table and view SQL portability
- [references/dialect-translation.md](references/dialect-translation.md) — the dialect rule table
- [references/maintenance-and-layout.md](references/maintenance-and-layout.md) — table maintenance and layout, Snowflake versus AIDP
- [references/data-movement-options.md](references/data-movement-options.md) — the ways data could move; the in-AIDP INSERT-SELECT (`snowmig_02_copy_schema`) is the implemented one
