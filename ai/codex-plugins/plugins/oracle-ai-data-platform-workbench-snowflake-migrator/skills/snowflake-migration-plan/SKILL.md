---
name: snowflake-migration-plan
description: Build a high-level Snowflake to AIDP migration plan and present it for approval. States which objects can migrate and which cannot with a brief reason for each, applies user-supplied restrictions such as excluded databases or size caps, derives dependency order so views follow their base tables, and lists the Silver and Gold job stubs. Use after an estate assessment, or when the user asks what can be migrated, in what order, why something is excluded, or wants to see the migration plan.
---

> **Paths.** `<plugin-root>` is this plugin's directory, two levels above
> this `SKILL.md`. Write its absolute path wherever `<plugin-root>` appears.

# Stage 2 — dependencies and plan

Two commands. The first needs Snowflake; the second is offline.

```bash
"<plugin-root>/bin/snowmig" deps

"<plugin-root>/bin/snowmig" plan \
  --bronze-catalog-prefix <S4 INTERNAL catalog> [--restrictions restrictions.json]
```

Every Snowflake coordinate comes from the migration config (`snowmig-config.yaml`, discovered automatically and printed as `config: <path>`). Pass `--account/--user/--auth/...` only to override a field for one run.

`PLANNED_OBJECTS.md` is the report to walk the user through. It is the answer to
"what are the objects planned to move".

## The bronze mapping is structural, not a choice

```
Snowflake database  ->  AIDP catalog
Snowflake schema    ->  AIDP schema
Snowflake table     ->  AIDP table
Snowflake view      ->  AIDP view
```

Bronze mirrors the source 1:1, so target names equal source names and nothing is
flattened. `--bronze-catalog-prefix` is the only variation: it puts everything in
one catalog and folds the database into the schema name. Under the runbook that
catalog is the S4 INTERNAL catalog, and the prefix is required: S10 refuses a
plan whose catalog is not its `--target-catalog`, and without the prefix the
plan's catalog is the source database name — the EXTERNAL catalog registered
at S3.

## Can and cannot, with reasons

Every inventoried object lands in exactly one of `can_migrate` or
`cannot_migrate`. Read the reasons out — they are the point of the report.
Categories:

| Category | Means |
|---|---|
| `restriction` | The user's own restriction excluded it. Name which one |
| `unmapped_type` | A column type has no Delta equivalent, e.g. `VARIANT`, `GEOGRAPHY` |
| `snowflake_only_sql` | A view uses a construct the translator recognises but has no exact rewrite for — `QUALIFY`, `LATERAL FLATTEN`, `DATEDIFF`/`TIMESTAMPDIFF`, `TIMESTAMPADD`/`TIMEADD`, `$$…$$`, `DECODE`, `NVL2`, a `::` cast over an expression or to `VARIANT` or `TIME`, a `DATEADD` whose amount is an expression or whose unit is quoted or a nested call. The reason names the construct and why. `IFF`, `x::TYPE` on a bare column or literal, `LISTAGG(x, sep)`, `DATEADD(unit, n, col)` and `"quoted identifiers"` are translated, not blocked. Full rule table: [references/dialect-translation.md](../../references/dialect-translation.md) |
| `unsupported_object` | Secure view (unless `plan --secure-views as-view`, below; a secure materialized view too); event or hybrid table (`SHOW TABLES` flags; decided by its kind before its column types) — see also `CENSUS.md`. A plain dynamic table or materialized view is **not** here: it is planned as a table snapshot, listed under "Planned as table snapshots" with `refresh generated` or `refresh NOT generated: <why>` |
| `register_in_place` | External or Iceberg table (a dynamic Iceberg table too): its files already sit in object storage, so they are **not copied**. Once the files are moved to OCI Object Storage they are registered as an AIDP table there — `snowmig external-registration` writes the statements (below). Never pointed at the S3/Azure/GCS source |
| `dependency_not_migrated` | Depends on an object that is not migrating (a blocked or excluded base table or view, or an object outside the assessed scope, e.g. another database); the reason names it |
| `no_definition` / `unparseable_sql` | The view SQL could not be read or parsed |
| `columns_unread` | The schema's `INFORMATION_SCHEMA.COLUMNS` read failed (the reason quotes the error, e.g. a timeout), so no column was assessed. Not a privilege verdict and not an empty table: fix the read and re-run `assess` |

## Secure views — refused unless the operator opts in, loudly

```bash
"<plugin-root>/bin/snowmig" plan --secure-views as-view [--bronze-catalog-prefix ...]
```

Default `refuse`: a SECURE view is `unsupported_object`. `as-view` plans it as
a **plain** view — only on the operator's explicit request, never as your
suggestion without saying what it costs: the definition becomes visible, the
secure-view optimizer barrier is gone, and any row filter keyed to Snowflake
roles (`CURRENT_ROLE()`, `IS_ROLE_IN_SESSION()` — named in the warning when
the body calls them) does not filter the same rows on AIDP.
`PLANNED_OBJECTS.md` opens with a **SECURITY WARNING** section, the DDL
statement carries `R60_SECURE_VIEW_AS_PLAIN` and the warning, and
`SUMMARY.md` scores the view HIGH. The translator still applies (Snowflake-only
SQL stays refused), and a secure *materialized* view stays refused. Common
reason to ask for it: the view is secure only because a Snowflake share
required it.

## External and Iceberg tables — register in place, after the files move

```bash
"<plugin-root>/bin/snowmig" external-registration
```

Reads Snowflake read-only (`SHOW EXTERNAL TABLES` / `SHOW ICEBERG TABLES` per
database, `DESCRIBE EXTERNAL VOLUME` / `CATALOG INTEGRATION`,
`SYSTEM$GET_ICEBERG_TABLE_INFORMATION`) and writes `EXTERNAL_REGISTRATION.md`:
per table, where the files come FROM (the S3/Azure/GCS path), where they are
registered AT (`oci://<bucket>@<namespace>/<source key prefix>`), and, for an
external table, one `CREATE TABLE IF NOT EXISTS … USING PARQUET|CSV|… LOCATION
'oci://…'` with placeholders. An Iceberg table gets a `CALL
<iceberg_catalog>.system.register_table(table => …, metadata_file => '…/metadata/<rewritten root metadata file>')`
instead, listed as **not registrable as generated** until the rewrite. Say
four things out loud:

- **The move is a prerequisite, not a step this plugin performs.** It moves no
  bytes and executes none of the statements; until the files are in the OCI
  bucket each statement registers a table over nothing.
- **Iceberg metadata holds absolute paths**, so a byte-copy is not yet a
  readable table: the paths are rewritten (or the table re-written) first.
- **Never `CREATE TABLE … USING ICEBERG LOCATION`** over the moved directory:
  it does not adopt the existing metadata -- it makes a new, empty table (or a
  path catalog rejects it) that reads 0 rows. `register_table` adopts it. An
  Iceberg table catalogued in Glue (or another external catalog) forks when a
  copy is registered on AIDP.
- **The statements are generated, never executed** — the report's last
  section lists the checks to make before running them.

## Outbound shares — a Delta Sharing plan, never executed

```bash
"<plugin-root>/bin/snowmig" share-plan      # after `security`, so exposures are known
```

For each OUTBOUND share (`SHOW SHARES`, `DESCRIBE SHARE`, read-only) it
writes `SHARE_PLAN.md`: the AIDP share, one Delta Sharing recipient per
consumer account, each shared object with its AIDP target and status, and the
`aidp delta-share` steps (create → manage-data-asset → create-recipient →
manage-access). Carry to the user: a recipient is **not a Snowflake account**
(consumers switch to a Delta Sharing client); a shared table with a masking or
row-access policy is **HELD**, because Delta Sharing ships raw values; the step bodies follow the `aidp delta-share` CLI reference;
nothing is run by `snowmig`, and every step publishes data outside the
tenancy.

## Restrictions — ask for them, do not invent them

`--restrictions` takes a JSON file. Ask the user what they want to exclude before
running; do not guess a scope.

```json
{
  "exclude_databases": ["SNOWFLAKE_LEARNING_DB"],
  "exclude_schemas": ["STAGE"],
  "exclude_object_types": ["VIEW"],
  "exclude_name_patterns": ["^TMP_", "_BAK$"],
  "exclude_objects": ["DB.SCHEMA.SCRATCH"],
  "max_rows": 100000000,
  "max_bytes": 1099511627776
}
```

`include_*` variants act as allowlists. An unrecognised key is an error, not an
ignored line — a typo would otherwise apply nothing while appearing to work.
A well-formed entry can still match nothing: `PLANNED_OBJECTS.md` prints each
entry's match count and flags a zero as *matched nothing — check the spelling*;
read those out. Names follow Snowflake's case rule in every list: unquoted
folds to upper, `"sales_eu"` matches only that exact spelling.

## Lineage provenance matters — state it

`deps` prints its source:

- `account_usage` — authoritative lineage across all object types
- `parsed_ddl` — the `ACCOUNT_USAGE` grant was unavailable, so edges come from
  parsing view DDL. **View→object edges only.** Say the graph is partial; do not
  present it as complete lineage.

## Present, do not just run

1. The can/cannot split and every reason.
2. **The catalogs stage 3 will register.** One per Snowflake database, and
   **EXTERNAL/SNOWFLAKE by default** — a read-only pointer at the live source
   that copies nothing. Name a Standard catalog only if the user has explicitly
   asked for one.
3. The waves — views follow their base tables.
4. Cycles, if any: they need a human decision, not a broken edge. Objects
   listed as *blocked behind a cycle* are not members; they wait on the cycle
   named and move once it is resolved.
5. The Silver/Gold job stubs: created, disabled, never triggered.
6. **The data-movement architecture options — always.**

Exit 3 means a target-name collision — show it and stop.

## The architecture options are not optional reading

`PLANNED_OBJECTS.md` ends with every option. Do not skip past them because the
runbook itself moves no data: the user needs to know which architecture they are
heading toward *before* structure lands, because it decides whether the destination is an
INTERNAL catalog they will fill, an EXTERNAL catalog they will read through, or
both.

| | Moves bytes | Handles |
|---|---|---|
| `A1` unload → object storage → managed Delta | yes | historic bulk, cutover |
| `A2` federate through an EXTERNAL catalog | no | read without copy |
| `A3` redirect ingestion (Fivetran / Kafka) | yes | ongoing incremental, cutover |
| `A4` Iceberg interop — share storage | no | read without copy, ongoing |
| `A5` hybrid waves | yes | all four |
| `A6` **customer-defined — or not decided yet** | *unknown* | *unknown until described* |

State whether one has been chosen. If not, say the architecture is **undecided**
and put all six in front of the user.

**`A6` is a real answer, not a fallback.** The customer may already have a
pattern their platform team runs, and it may be better than anything here. They
may also simply not have decided. Either way, record `A6` with their reasoning
rather than pressing them toward `A1`–`A5`. Nothing in the assessment, the plan
or the shallow clone depends on the answer.

When they do describe a design — including one not listed here — record it
verbatim:

```bash
"<plugin-root>/bin/snowmig" data-options \
  --choose A6_CUSTOMER_DEFINED --chosen-by <name> --rationale "<why>" \
  --custom-name "<their name for it>" --custom-description-file <file>
```

It is recorded as-is and **never mapped** to one of ours. Say plainly that this
plugin has not assessed it, so none of the trade-offs or unknowns listed against
`A1`–`A5` transfer to it.

**If the user has no preference**, the honest recommendation is `A2` first — it
moves nothing, needs the least building, and lets results be validated against
the source before anything is copied — with `A1` for whatever usage data later
shows is worth making resident. Say clearly that this is a recommendation, not a
decision: it belongs to the customer because it drives cost, wall-clock and
whether a later migration can run unattended.

Record a choice with:

```bash
"<plugin-root>/bin/snowmig" data-options \
  --choose A2_FEDERATE_EXTERNAL_CATALOG --chosen-by <name> --rationale "<why>"
```

A rationale is mandatory. Recording executes nothing — **none of the six is
implemented**, and `execute_transfer()` refuses by design.

## Raise the maintenance question — it does not raise itself

Snowflake exposes **no `OPTIMIZE` and no `VACUUM`**. It maintains layout via
Automatic Clustering and reclaims storage in the background, un-asked. AIDP has
`OPTIMIZE`, `VACUUM`, `ZORDER BY` and liquid clustering, and **runs none of
them**. So a customer who asks for "the same maintenance on AIDP" is not asking
for a missing feature — they are inheriting a responsibility.

If the DDL plan has a *"Carried into the CREATE TABLE"* section, say that
those source settings (a plain clustering key, retention, change tracking or a
stream) are emitted in the CREATE TABLE and applied by the structure notebook
-- and NOT by `snowmig deploy --execute`, which has no field for them (R13).
After such a deploy, its summary lists each dropped clause per table under
*"Properties this transport cannot carry"*; read those out, they are not
applied even though the table verified.

If it has a *"Maintenance and layout — decisions, NOT applied"* section, read
it out. Say three things:

1. **Every listed setting has an AIDP equivalent this table could not take as
   it stands**, and each row says why (an expression key, a key Delta cannot
   cluster on). A clustering key that does not arrive is a performance
   regression on the largest tables in the estate, and it is silent.
2. **On Delta, `VACUUM` is what bounds time travel.** On Snowflake, retention
   and storage reclamation are independent and automatic. A customer used to
   reclaiming storage freely will delete their own recovery window. Snowflake's
   7-day Fail-safe has **no** equivalent at all.
3. **`OPTIMIZE` without `VACUUM` increases storage.** It leaves the old files
   behind until retention expires, so half the job is a cost regression.

Do not propose a cadence or a retention. Both need the customer's recovery
requirements and query patterns. `references/maintenance-and-layout.md` has the
full mapping.

## Finish with `summary` — it is not optional

```bash
"<plugin-root>/bin/snowmig" summary
```

`SUMMARY.md` is the per-object roll-up the user asked for: one row per table,
view and job with its row count, migration risk, migration status and a note.
Run it after `plan`, and again after `deploy` so the statuses reflect what
actually happened. It also carries the source→destination header, the
row-count provenance, and the data-movement architecture options.
