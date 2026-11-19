---
name: aidp-migrator-overview
description: Router skill. Read this first whenever the user mentions migrating a Databricks workload (notebooks, jobs, catalogs, schedules) onto Oracle AI Data Platform (AIDP). Lays out the toolkit, the two-pass migration architecture, and which of the other aidp-* skills handles each phase. Compose those skills; this one adds no API surface.
---

# `aidp-migrator-overview` — router

Migrating a Databricks workload onto AIDP is a multi-phase operation. This skill picks the right next skill based on what the user asks.

## When to use

- The user mentions "migrate Databricks", "port from Databricks to AIDP", "Unity Catalog migration", "DBX → AIDP", "lift-and-shift Databricks job", or similar.
- The user asks "where do I start" with the migrator toolkit.
- The user asks for an overview of the migrator architecture.

## The two-pass architecture (mental model)

```
┌────────────────────┐   ┌──────────────────────┐   ┌─────────────────────┐
│ Pass-0: Plan       │ → │ Pass-1: Dep code     │ → │ Pass-2: Execute     │
│  build_dag.py      │   │  ensure_migrated()   │   │  job_migrate.py     │
│  check_data_       │   │  walks %run tree,    │   │  cell-by-cell on a  │
│  availability.py   │   │  rewrites Databricks │   │  live AIDP cluster, │
│                    │   │  APIs in each dep    │   │  4-way verify, up   │
│  (read-only)       │   │  notebook (no run)   │   │  to 10 fix attempts │
└────────────────────┘   └──────────────────────┘   └─────────────────────┘

The catalog migration is a SEPARATE flow, run BEFORE Pass-2 (so the
schemas + table locations exist when migrated notebooks try to read them):

┌──────────────────────────────────┐   ┌──────────────────────────────────┐
│ extract_catalog_databricks.py    │ → │ migrate_catalog.py               │
│  REST against Unity Catalog API  │   │  18 DDL rewrite rules → batched  │
│  → reports/catalog_pack.json     │   │  CREATE SCHEMA / CREATE TABLE    │
│                                  │   │  on AIDP in a single WS execute  │
└──────────────────────────────────┘   └──────────────────────────────────┘
```

## Pick the right skill for the user's ask

| User says | Skill to invoke |
|---|---|
| "Migrate this Databricks job", "port this workflow", "convert to AIDP" | [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) |
| "Build a migration manifest", "what would migrate", "show the DAG" | [`aidp-build-dag`](../aidp-build-dag/SKILL.md) |
| "Are my source tables available?", "pre-migration check", "is the data ready" | [`aidp-check-data`](../aidp-check-data/SKILL.md) |
| "Resume the migration", "skip already-migrated", "pick up where I left off" | [`aidp-resume-migration`](../aidp-resume-migration/SKILL.md) |
| "Cell N is failing", "fix this notebook", "retry from cell K" | [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md) |
| "Migrate the Unity Catalog", "port the HMS schemas", "DDL migration" | [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) |
| "Map s3 buckets to OCI", "configure bucket mapping" | [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) |
| "Streaming convergence", "acceptance contract", "wait for pipeline to settle" | [`aidp-acceptance-contract`](../aidp-acceptance-contract/SKILL.md) |
| First time using this toolkit, "what do I need to install" | [`aidp-migrator-bootstrap`](../aidp-migrator-bootstrap/SKILL.md) |

## What the user must have set up before any of this

This plugin is **self-contained** — the full migrator engine ships bundled under `${CLAUDE_PLUGIN_ROOT}/engine/`. Before any skill in this plugin can do real work, the user needs:

1. **Engine Python deps installed.** One-time `pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt`. Skill [`aidp-migrator-bootstrap`](../aidp-migrator-bootstrap/SKILL.md) walks through this and the rest of these checks.
2. **`~/.oci/config`** with either an `api_key` profile (unattended) or session-token profile (interactive).
3. **An ACTIVE AIDP cluster.** The engine's Pass-2 requires a live cluster — the WebSocket execute path. If the cluster is stopped, ask the user to start it via AIDP console before invoking [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md).
4. **`ANTHROPIC_API_KEY`** in the environment. The engine uses Claude with tool use for every cell rewrite. Without this key the Pass-2 loop won't run.
5. **An `env-coords.md` file** — see [references/env-coords.template.md](../../references/env-coords.template.md). The customer fills in their DataLake OCID, workspace UUID, cluster ID, AIDP base URL, OCI profile name once; every other skill threads these through.

## What this plugin does NOT do

- It does not migrate Databricks `dbutils.fs` to OCI Object Storage *files* — only table/notebook/job constructs. File-level DBFS replication is a separate exercise.
- It does not handle Databricks Workflows-on-Pipelines (DLT) — only Jobs + tasks. DLT pipelines need manual recreation as Spark structured streaming + scheduling.
- It does not migrate Databricks ML feature-store registrations or MLflow model versions automatically. Those need separate handling.
- It does not provision the AIDP cluster, workspace, or DataLake. Assume those exist.

## Key references

- [`references/cli-map.md`](../../references/cli-map.md) — every migrator CLI entrypoint mapped to its purpose.
- [`references/gotchas.md`](../../references/gotchas.md) — 15 Databricks → AIDP gotchas with fix recipes.
- [`references/ddl-rewrite-rules.md`](../../references/ddl-rewrite-rules.md) — the 18 DDL rewrite rules.
- [`references/env-coords.template.md`](../../references/env-coords.template.md) — the scaffold every skill threads from.

## Order of operations for a fresh migration

1. [`aidp-migrator-bootstrap`](../aidp-migrator-bootstrap/SKILL.md) — once per workstation.
2. [`aidp-migrate-catalog`](../aidp-migrate-catalog/SKILL.md) — schemas and tables FIRST, so notebook reads have targets.
3. [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) — only if migrating tables with `s3://` external locations.
4. [`aidp-build-dag`](../aidp-build-dag/SKILL.md) — produces `reports/<job>_manifest.json`.
5. [`aidp-check-data`](../aidp-check-data/SKILL.md) — verify before committing cluster time.
6. [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) — the big run.
7. [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md) (only if needed) for cells the auto-fix loop couldn't recover.
8. [`aidp-acceptance-contract`](../aidp-acceptance-contract/SKILL.md) (for streaming / batch convergence pipelines).
