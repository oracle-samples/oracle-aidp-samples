---
name: fabric-aidp-migrator
description: "Migrate a Microsoft Fabric workspace (notebooks, Warehouse T-SQL, Dataflow Gen2 / Power Query M, Data Pipelines, Lakehouse shortcuts, OneLake paths) to Oracle AI Data Platform. Use when the user wants to inventory a Fabric estate, plan a migration to AIDP, translate Fabric notebooks, Warehouse T-SQL or Power Query to Spark, verify a migration's output, or publish it into an AIDP workspace. Wraps the `fabric-aidp` CLI, with the verbs inventory, plan, migrate, verify and publish."
---

# Oracle AIDP migrator for Microsoft Fabric

Drives the `fabric-aidp` CLI, which migrates a Fabric workspace to Oracle AIDP in five
verbs. The translators are **deterministic**: every rewrite is a named rule with a
reason, and anything that cannot be safely rewritten is **flagged and left in place**,
never silently changed. Preserve that principle — never hand-edit a flagged construct
into a silent rewrite.

## Prerequisites

The CLI is a Python package with no runtime dependencies. Install it from the
plugin root — the directory holding `pyproject.toml`, which is the repository
root when cloned and `${CLAUDE_PLUGIN_ROOT}` when installed as a plugin:

```bash
pip install -e "${CLAUDE_PLUGIN_ROOT:-.}"
fabric-aidp --version                      # check it resolved
```

**Dataflows additionally need Node 18+.** Power Query has no Python parser —
Microsoft's own is TypeScript — so the M translator shells out to one:

```bash
cd "${CLAUDE_PLUGIN_ROOT:-.}/fabric_aidp/mparse" && npm install
```

Skip that and everything else still works, but **every Dataflow is reported as
counted and not translated**: `inventory` says `parser=unavailable` with
`translatable_count=None`, and `migrate` emits no PySpark for any of them. If a
user's Dataflows produced nothing, check this before looking at their M.

## Input

The tool reads a **Fabric Git export** — the folder a workspace is synced to via
Fabric's native Git integration. The four offline verbs need no Azure credentials, no
network and no Fabric tenant.

With no export to hand, `fabric-aidp inventory --fixture demo` scans a bundled Acme
Insurance estate. That writes a manifest and stops — it is one verb, not the pipeline;
run `plan`, `migrate` and `verify` after it as below.

## Workflow

```bash
fabric-aidp inventory <export-dir> -o inv.json
fabric-aidp plan      inv.json     -o plan.json --namespace <oci-namespace> \
                                                --catalog <aidp-catalog>
fabric-aidp migrate   plan.json    -o ./migrated
fabric-aidp verify    ./migrated
```

`migrate --demo` was removed: the CLI now exits 2 on it. There is no mode flag to pass, and `migrate` has always written locally. Do not suggest it.

A fifth verb, `publish`, uploads a finished migration into an AIDP workspace. It is
the only verb that writes anywhere, it is a dry run until `--apply`, and it never
overwrites anything it did not create. See the `/publish` command for the rules.

## What it translates

| Fabric | → | AIDP |
|---|---|---|
| Notebook OneLake paths | → | `oci://bucket@namespace/...` |
| Notebook table references | → | a three-part AIDP name; see **One name for one table** in [README.md](../../README.md#one-name-for-one-table) for the rule |
| `display(df)` | → | `df.show()` |
| Warehouse `CREATE TABLE` / `CREATE VIEW` / queries | → | Spark SQL |
| Dataflow Gen2 queries (Power Query M) | → | PySpark scripts, one per query — needs Node |
| Data Pipelines | → | AIDP workflow jobs (tasks + `dependsOn`) |
| External shortcut targets | → | `oci://` locations |
| Semantic models | → | inventoried only; DAX has no AIDP target |

## What it flags rather than guessing

- `notebookutils` / `mssparkutils` — no AIDP equivalent, especially `credentials.getSecret`
- A table reference that is a **shortcut** — its data lives outside the lakehouse
- A table no catalog tier knows
- Stored procedures, and the T-SQL control flow named in
  [README.md](../../README.md#what-it-flags-rather-than-guessing) — a fixed list
  (`DECLARE @`, `SET @`, line-initial `IF`, `BEGIN…END`, `WHILE`, `GOTO`, `RETURN`,
  `THROW`, `RAISERROR`, `WAITFOR`, transaction control, cursors, `EXEC`),
  not "anything procedural". Say so: control flow outside the list reads as clean
- `#temp` and `##global` tables, named one by one
- `MONEY`, `UNIQUEIDENTIFIER`, `DATETIMEOFFSET`, `STRING_AGG`, `MERGE`, query hints
  (`WITH (NOLOCK)`, `OPTION (…)`, join hints), `CROSS`/`OUTER APPLY`
- A Dataflow on Excel, Snowflake, Databricks or SQL Server — named and refused
- A pipeline holding a `Copy`, `Lookup` or `ExecutePipeline` activity — refused whole,
  because a job with only its notebook tasks would run and be wrong

## Reporting results

State what PASS means: translated, no known issue detected — **not execution-verified**.
Nothing parses or runs the artifacts. A construct no rule covers is reported clean, so a
low REVIEW count is not by itself evidence of a clean migration.

Report `blocked` alongside ok / needs-review / planned. A blocked object is one the
tool refused rather than translated partly, so it is work the user still owes.

Three things worth surfacing unprompted:

- A lakehouse whose shortcut tracking reads `not_tracked` — the estate may hold
  shortcuts the scan could not see. Report `unknown`, never zero.
- A table resolved only by **inference** from a notebook write. It is an `info` finding,
  not a flag, but it means the tool guessed the table exists because some notebook
  creates it.
- `parser=unavailable` on the dataflow line — see **Prerequisites**. The Dataflows were
  counted, not dropped, but none of them was translated.

## What the bundled demo does and does not prove

The demo estate exercises every source kind, and the rules it fires are regression-
tested. It is not a coverage gate: the 110 assets it migrates fire 57 of the 251 rule
ids the translators define. Do not describe a clean demo, or a clean migration, as
evidence that a rule nothing exercised is correct.
