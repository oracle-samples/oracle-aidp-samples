---
name: snowflake-medallion-clone
description: Create the medallion architecture on Oracle AI Data Platform - registering an EXTERNAL catalog of source type SNOWFLAKE by default, and generating Spark SQL for schemas, tables and views from an approved Snowflake plan only when the user has explicitly asked for a Standard catalog. This stage copies no data - rows are copied only by the snowmig_02_copy_schema job, when the operator runs it. Use when the user asks to create the medallion structure, soft clone, shallow clone, create the target catalog, or deploy the target schema.
---

> **Paths.** `<plugin-root>` is this plugin's directory, two levels above
> this `SKILL.md`. Write its absolute path wherever `<plugin-root>` appears.

# Catalogs — runbook S3 and S4

A migration creates **two** catalogs, and they are different steps.

**S3 — the EXTERNAL catalog** registers the live Snowflake source: a read-only
pointer that copies no bytes, creates no tables and has nothing to keep in
sync. **One Snowflake database becomes one AIDP catalog, always.** An EXTERNAL
catalog registers the *whole database* — a plan-level restriction such as
`include_objects` does **not** narrow it, so never tell a user their subset
applies here. If the account holds several databases, the user picks one
before anything is created; another database is another migration.

**S4 — the INTERNAL target catalog** is the managed target the migrated
schemas and tables land in. Creating it is part of the sequence, not an
exception to argue for. It is a **container**: one control-plane object.
Tables are created on AIDP compute by the structure workflow (S10), where each
create is read back; the control-plane catalog API is used for the catalog
container only.

**`INTERNAL` is the API type.** "Standard" is the runbook's and the CLI's
word, an accepted alias that `normalize_catalog_type()` translates to
`INTERNAL` before any call. AIDP's two catalog types are `INTERNAL` and
`EXTERNAL`.

**Both catalogs come after the workspace and the cluster.** Any AIDP write
resolves four coordinates — DataLake, workspace, cluster, catalog — so neither
catalog can be registered before S1 and S2 have made the first three.

## Phase A — register the EXTERNAL catalog (the default path)

**Take these from the `aidp:` block of `snowmig-config.yaml` when present
(see `snowmig-config.example.yaml`); a flag on the command line wins, and the
CLI prints whatever it took from the file before acting. Ask the user only
for what is missing, and ask in this turn:**

- DataLake OCID
- workspace
- cluster id
- **target catalog name** — one catalog per Snowflake database; see the scoping rule
- path to the **connection config** (YAML or JSON)

The Snowflake account, warehouse, database, user and credential come from a
config **file**, never from inline arguments — see
`snowmig-config.example.yaml`. The password or private key lives **inline in
that file** (`password:` / `private_key: |`; a `*_path` variant is an opt-in
alternative), so the file holds a live credential in plain text. The rules
that go with that are the bootstrap skill's, and they apply here too:

- **Ask the user before reading the config**, and say why you need it.
- **Never print, echo, quote or summarise a secret value** — not in chat, not
  in a report, not in a commit. Render the config only through
  `"<plugin-root>/bin/snowmig" preflight`, which uses
  `migration_config.redact()`.
- **Never ask the user to paste a secret into the conversation.** If one
  lands in the chat anyway, say so and tell them to rotate it.

```bash
"<plugin-root>/bin/snowmig" catalog \
  --catalog <cat> --config ./snowmig-config.yaml \
  --execute --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl>
```

Without `--execute` this is a dry run: it validates the config, reports which
connection fields were built, and creates nothing. Show `CATALOG.md`.

Then validate the connection with `--test-connection` on the same command
(`"<plugin-root>/bin/snowmig" catalog ... --execute --test-connection`;
it only runs with `--execute`, because the API resolves RBAC on an existing
catalog) before claiming the catalog is usable. Report the result as it is:
`PENDING` is pending, never a pass.

If the connection test returns `FAILED` without a reason, keep the
registration and continue; discovery (S6) validates the connection through
the connector. Never delete and re-register to make the test pass.

## Phase B — generate the DDL (offline, safe; needed only for Phase C)

```bash
"<plugin-root>/bin/snowmig" ddl
```

Show `DDL_PLAN.md`: the SQL, the rule behind each transformation, dropped
properties, and everything blocked. Statements come out in wave order, so a view
always follows the tables it reads. Nothing has touched AIDP.
`R04_EXACT_READ` lists the columns the copy reads as text and converts on AIDP
(NUMBER, FLOAT, TIME, timestamps, structured and geospatial types): the
connector's own read loses digits, fractions or offsets on those, or cannot open
the table at all. Each table statement in `ddl_plan.json` carries that per-column
spec as `columns`.

**Exit code 3 is a halt, not a failure:** a column uses a type the target
refuses at CREATE TABLE, usually `TIMESTAMP_NTZ` on an estate assessed with
`--timestamp-ntz preserve` or `--mapping-defaults off`.
stderr and `DDL_PLAN.md` name the columns. The remedy is a decision for the
user — `ddl --timestamp-ntz timestamp` (offline) maps them to `TIMESTAMP`,
which changes timezone semantics — and until it is made, do not hand this
plan to Phase C.

## Phase C — Standard catalog, only when requested

Within the migration runbook this is not a separate request: S4 creates the
INTERNAL target catalog and S10's structure workflow creates its tables. This
phase covers a Standard catalog asked for outside the runbook.

An EXTERNAL catalog needs no tables. A Standard catalog does, and **those tables
are created on AIDP compute, not through the control-plane API**: a Spark run on
the cluster prints per-object progress and the Spark error for any failure, and
reads each object back.

So for a Standard catalog's TABLES, hand over the script and let it run on
compute. In the runbook that is S10: `snowmig.py run --job
snowmig_01_structure`, one workflow per schema, logged and re-runnable.

The catalog container itself comes from
`snowmig.py catalog --catalog-type standard` at S4, which creates it as
`INTERNAL` and reports `container_only: true` — pass that on, so nobody reads
a created container as created structure.

`snowmig.py deploy` creates the structure through the control-plane catalog
API instead. Prefer Phase C; use `deploy` only when the user asks for it
specifically.

## Rules

1. **One catalog per run.** Bronze mirrors the source, so a multi-database estate
   spans catalogs. A run covers only `--catalog` and reports the rest as out of
   scope. Another catalog is another explicit confirmation.
2. **Both catalogs, in order: EXTERNAL at S3, INTERNAL at S4 — and both
   after the workspace and cluster.** Say plainly what an EXTERNAL catalog is
   — a live read-only view of Snowflake, not a copy — so nobody expects
   migrated tables from it, and say that its scope is the whole database. The
   tables the user wants on AIDP live in the INTERNAL target catalog and are
   created at S10, on compute.
3. **Never `--execute` on an earlier approval.** Ask in the turn you run it.
4. **ADB/ADW/ALH EXTERNAL catalogs cannot hold managed Delta.** If a Standard
   clone is aimed at one, explain rather than trying.

Rules 5–7 govern the Phase C clone; there is nothing to verify per object when
a catalog is merely registered.

5. **Report `verified`, never `executed`.** A batch can report success while
   statements inside it failed, so every object is probed individually. The honest
   number is `verified/total`. `verified` means the object exists **with the
   planned column list**, checked by `DESCRIBE` — not merely that something of
   that name is there.
5b. **Two outcomes are not successes, and must be read out.** *Structure
   differs* means the name already belonged to an object with different
   columns; the DDL is `CREATE IF NOT EXISTS`, so it was left exactly as found
   and has **not** been cloned. Resolve the collision before re-running — do
   not describe it as migrated. *Structure not verified* means it exists but
   its columns could not be compared, so no clone claim has been earned.
5c. **Creation is asynchronous — a `202 Accepted` is not a create.** An
   object exists once it has been read back. If `verified/total` is less than
   the statement count when the command returns, that batch is still settling,
   not failed and not done — say "pending, N of M verified so far" and check
   again rather than reporting the run as complete either way. Never say
   "migration complete" or "clone succeeded" before every object in this
   `--catalog` shows `verified`.
6. **Say the objects are empty.** This is a structural clone: schemas, tables and
   views with no rows. Data movement is a later phase.
7. **No `CREATE OR REPLACE`, no `DROP`.** Existing objects are left alone; a 409
   means "already exists", not a failure.
8. **Silver/Gold jobs are not created here.** The plan defines them, disabled and
   never triggered. Creating them on AIDP is a separate step the user asks for.
9. **When the stage returns, report the outcome and the next stage in the
   same turn** — the registered catalog, or the verified count for a Phase C
   clone — then run `summary` (stage 4) and name it, per the router's
   rule on reporting stage progress unprompted. Do not stop on the raw command
   output and wait for the user to ask what happened or what's next.

## Backend

The engine uses `oci raw-request` whenever `oci` is installed; workspace
files, job cancel and delete, and catalog delete always use the `aidp` CLI, so
both are needed. It prints which backend it chose. If neither CLI is present
it fails loudly rather than guessing a transport.
