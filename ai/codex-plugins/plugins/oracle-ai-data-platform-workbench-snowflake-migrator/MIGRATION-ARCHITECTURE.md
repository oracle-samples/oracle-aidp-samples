# Architecture: Snowflake → Oracle AI Data Platform migration

A deterministic, auditable migration of a Snowflake estate — structure first,
then data, schema by schema — into AIDP managed Delta, with **the whole data
plane running inside AIDP on Spark**: the operator's machine plans and
verifies, and no row of customer data ever passes through it.

This document is the migration design record: what maps to what, how a run
proceeds, what is automated, and what deliberately is not.

**To actually run one, follow `README.md` → "How to run a migration, from
zero".** That is the order of operations, with the commands and the one config
file they all read. This document explains *why* that order is what it is.
`ARCHITECTURE.md` is the companion engineering record of the CLI engine
itself, including its scope and future scope.

---

## 1. The problem

A Snowflake estate is thousands to hundreds of thousands of objects. Migrating
it by conversation — an AI reading one table at a time — is slow, expensive
and unauditable. The straightforward alternatives all cost something:

| Approach | Cost |
|---|---|
| Hand-written one-off scripts | no inventory, no verification, no report — nobody can say what moved |
| ETL tooling per table | per-table configuration grows with the estate |
| "AI does everything" | minutes per object, non-deterministic, and the audit trail is a chat log |

The answer here: **deterministic scripts discover, plan, create and copy;
verification is code; the AI plans, explains, and handles only what cannot be
translated mechanically** — and says so instead of guessing.

## 2. Solution in one diagram

```
 OPERATOR (Codex plugin — control plane)                ORACLE AIDP
 ┌───────────────────────────────┐        ┌──────────────────────────────────┐
 │ snowmig.py                    │        │  workspace  <ws-name, translated> │
 │  assess/deps/security/…  ─────┼─READ──►│  ┌────────────────────────────┐  │
 │  plan + ddl   (offline)       │  only  │  │ cluster: migration_assets  │  │
 │  provision  ──────────────────┼──REST─►│  │  Spark 3.5 · Delta 3.2     │  │
 │  catalog (register EXTERNAL) ─┼──REST─►│  └─────────────┬──────────────┘  │
 │  deploy / notebook (structure)│        │                │ runs            │
 │  reads reports, reconciles ◄──┼────────│  backup-snowflake-migration/    │
 └──────────────┬────────────────┘        │   ├─ scripts/ 00…03 (jobs)      │
                │                         │   ├─ plan/    plan.json, DDL    │
        Snowflake account                 │   └─ reports/ manifest, copies, │
 ┌──────────────┴───────────────┐         │               MIGRATION_REPORT  │
 │ databases · schemas · tables │◄──READ──│  EXTERNAL catalog (read-only    │
 │ views · warehouses · tasks…  │  only   │  3-part names: cat.schema.tbl)  │
 └──────────────────────────────┘         │  INTERNAL catalog(s) = target   │
                                          └──────────────────────────────────┘
```

Two read paths, one write surface. The operator's engine reads Snowflake
directly (fast, batched, read-only **enforced at the transport**) for the
inventory and plan; the data itself flows inside AIDP, through the **AIDP
Snowflake connector** on the cluster into managed Delta, where Spark reports
each statement's result and nothing transits the operator's machine.

Registering the account as an EXTERNAL catalog is part of the flow — it is
how the estate becomes browsable in AIDP — and the data plane does not depend
on it: the in-AIDP scripts read through the connector by default
(`--source-mode connector`), with `external-catalog` as the option.

## 3. What becomes what

| Snowflake | AIDP | How | Fidelity notes |
|---|---|---|---|
| Account (the estate) | **Workspace** | `provision` — name translated to `[a-z0-9_]` so it cannot be rejected | rename reported, never silent |
| Database | **Catalog (INTERNAL)** | plan mirrors 1:1; created via structure clone | AIDP lower-cases identifiers; case collisions HALT the plan |
| Schema | Schema | structure clone | |
| Table | **Managed Delta table** | from the approved `ddl_plan` (engine-translated types, default), or CTAS `WHERE 1=0` through the source | `NUMBER(p,s)`→`DECIMAL(p,s)` exact; `VARIANT/OBJECT/ARRAY` carried as `STRING` by default, with a warning; `GEOGRAPHY/GEOMETRY` **blocked** unless the operator opts into text; `TIMESTAMP_NTZ` mapped to `TIMESTAMP` by default, with the timezone caveat recorded |
| View | View | 9 dialect rewrites (`IFF`, `::` via the type mapper, `DATEADD` — exact for DATE operands only, caveat recorded on the plan — `LISTAGG`, quoted identifiers, `''` escapes, `//` comments…); 12 constructs (`QUALIFY`, `LATERAL FLATTEN`, …) **refused and named** for a human | target re-derives column types — drift is reported, narrowing flagged |
| Warehouse | **Compute cluster** | `provision` creates `migration_assets`; per-warehouse clusters proposed by `compute` with sizing left as a decision | same-name clusters, default config, per request |
| Table data | Delta rows | `02_copy_schema.ipynb` per schema: one qualified pushdown SELECT per table through the connector, built from the plan's per-column read spec (exact `NUMBER`, `TIME`/`TIMESTAMP` fractions, `VECTOR`/`MAP`/structured `OBJECT` as typed columns), then INSERT-SELECT with the plan's conversions (or the external catalog's three-part name), verified by counts (+ exact decimal sums) | per-table snapshots — see §6, cutover consistency |
| Task / Stream / Pipe | **AIDP Job** — a MANUAL job per task graph generated by `snowmig jobs` (the Snowflake schedule recorded, not applied); a stream on a migrated table becomes Change Data Feed on the target table | census inventories them with effort bands. A dynamic table or materialized view migrates as a table snapshot with a generated refresh job; an external or Iceberg table is registered in place; an event or hybrid table is blocked by `plan` with the reason named (`PLANNED_OBJECTS.md`, "Object kinds with no AIDP equivalent") | a task that fed a migrated table stops feeding it after cutover, so its replacement is planned before cutover |
| Procedure / UDF | Job or Spark UDF (rewrite) | census + language verdict (SQL/JS/Python/Java/Scala) | code is rewritten by humans/AI with review, never mechanically |
| Masking / row-access policy | Redesigned on AIDP | security stage reports every exposure | policies do not carry over with the data; restricted views and classification are a design task |
| Secure view | — | blocked, named | its guarantees are part of the security redesign |
| Time Travel + Fail-safe | Delta retention | maintenance stage reports the source retention settings | Fail-safe has no Delta equivalent |
| Stage / File format | Object-storage location / reader options | census pointer | |

## 4. Dev mode and prod mode

**Dev mode** (`snowmig.py demo`, `/snowflake-demo`) runs the entire pipeline
against a built-in emulated estate and an emulated AIDP — production code,
fake transports. It exists so anyone can see, in one minute and with zero
credentials, every artifact a real run produces and every refusal the design
makes (a blocked `VARIANT`, a `QUALIFY` view, a create that is read back and
diagnosed, a deploy refused against the read-only external catalog).
Everything it writes is marked emulated.

**Prod mode** is the same pipeline with credentials and `--execute` gates.
See `README.md` for the runnable form of this table, and
`skills/snowflake-migrator-overview/SKILL.md` for the twelve-step order
(S1–S12) it follows; the `S` labels below are that runbook's.

| # | Step | Command | Writes |
|---|---|---|---|
| 0 | **Confirm the connection config with the user, field by field**, and test both ends | `preflight` | no |
| 1 | Preview the estate from the laptop (objects, census, lineage, security, maintenance, warehouses) — optional; the migration's own discovery is step 6 | `assess` `deps` `security` `maintenance` `compute` | no |
| 2 | Plan + generate DDL, get sign-off (S7–S9) | `plan` `ddl` | no |
| 3 | Prove both ends | `smoke` | only with `--write-probe --execute`: one probe schema, removed again |
| 4 | Provision the AIDP environment: workspace (named after the source account), `migration_assets` cluster, `backup-snowflake-migration/` (scripts + plan), and the unscheduled migration jobs — discover, structure, reconcile, and one copy job per schema of the approved plan (S1, S2, S5). With `--warehouse-clusters` (S12, after `compute`, once the warehouse list is approved) it also creates **one cluster per Snowflake warehouse**. The CLI and `PROVISION.md` print the workspace and cluster keys every later command needs (recorded as `workspace.key` / `cluster.key` in `provision_result.json`): pass them as `--workspace`/`--cluster-id`, or put them in the config's `aidp:` block | `provision --execute` | yes |
| 5 | Register Snowflake as an EXTERNAL catalog (S3), then create the INTERNAL target catalog as a container (S4) | `catalog --execute`, then `catalog --catalog-type standard --execute` | yes |
| 5b | Confirm the environment from inside AIDP | `diagnose_environment.ipynb` | no |
| 6 | Discover the estate as a workflow inside AIDP; back up the manifest (S6) | `run --job snowmig_00_discover` | yes (workspace files) |
| 7 | Create the structure in the INTERNAL catalog on AIDP compute, from the approved plan, one workflow per schema (S10) | `run --job snowmig_01_structure` | yes |
| 8 | Later, on the customer's decision: **copy, schema by schema** (one job per schema of the pushed plan), then reconcile | `run --job snowmig_02_copy_<schema>`, `run --job snowmig_03_reconcile` | yes (target only) |
| 9 | Read `MIGRATION_REPORT.md`: per table — structure, copy, live existence, verdict, why | `03_reconcile` | no |
| 10 | Release the compute (default), remove the Snowflake credential from the workspace (`--scope credential`), or undo the migration in a lab or rehearsal (`--scope all`, the INTERNAL catalog only with `--include-data`) — each only what the record proves this migration created | `teardown [--scope …] --execute` | yes |

`deploy` — control-plane CRUD straight into a Standard catalog — is not in
this table: the structure workflow is the recommended path, and `deploy` is
reached for only when the user asks for it by name (see `ARCHITECTURE.md`).

Every control-plane write stage is a dry run until `--execute` (`run` has no
dry run: running is the write); every create is read back before it is called
done; a 2xx is never the claim.

## 5. What is deterministic and what is AI

| Work | Who |
|---|---|
| Discovery, inventory, census, lineage, sizing inputs | scripts (batched SQL, paginated) |
| Type mapping, DDL, the 9 SQL rewrites (8 exact; `DATEADD` exact for DATE operands only, and the plan says so) | scripts — refuse rather than guess |
| Environment provisioning, uploads, job wiring | scripts, with per-step read-back |
| Data copy + verification (counts, exact decimal sums) | scripts, resumable per schema |
| Plan-vs-reality reconciliation | script, consulting the live catalog |
| Explaining reports, driving the conversation, sign-offs | AI (the plugin skills) |
| Rewriting the refused SQL (`QUALIFY`…), procedures, tasks, security design | AI/human, with the refusal artifact as the worklist |

## 6. Considerations

What to plan for before a production cutover.

- **Cutover consistency.** Each table is copied at its own instant, so while
  the source takes writes, tables can reflect different moments. Freeze
  writers, copy from a point-in-time Snowflake `CLONE`, or plan an
  incremental re-sync. The copy report records timestamps, so any drift is
  attributable.
- **Pipelines and procedures are rewritten, not translated.** Streams, pipes,
  procedures and UDFs are inventoried with effort bands and a language
  verdict. Tasks, dynamic tables and materialized views get generated MANUAL
  jobs (`snowmig jobs`): single-statement DML and refresh queries that
  translate exactly are translated, and everything else is a stub that fails
  when run. Rebuilding the rest as AIDP jobs or Spark UDFs is a separate
  workstream — on a real estate usually the largest — and a table fed by a
  Snowflake task needs its replacement job in place before cutover.
- **Security policies are redesigned on AIDP.** Masking and row-access
  policies do not travel with the data. The security stage lists every
  exposure; design the restricted views and classification before the
  migrated tables are opened to users.
- **The schema is the unit of work.** Every job run carries a fixed start-up
  cost, so structure and copy run one job per schema, never per table. Start
  with one small canary schema end to end (after `diagnose_environment.ipynb`)
  and read `MIGRATION_REPORT.md` against the console before anything larger.
- **Plan the copy as a separate step.** Structure comes first and copies no
  rows; the copy runs later, schema by schema, on the customer's decision.
  Give it its own window and cluster time, and decide the verification level
  up front: row counts by default, exact decimal sums with
  `--verify counts+sums`, which adds a source read per table.
