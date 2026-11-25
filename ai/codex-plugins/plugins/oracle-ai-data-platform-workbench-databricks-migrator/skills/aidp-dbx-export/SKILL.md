---
name: aidp-dbx-export
description: Export a Databricks job into AIDP — notebooks, JARs, init scripts, workflow + cluster specs — via python3 -m dbx_export. Three phases (discover+download → upload → clusters+workflow) with AIDP-first dependency resolution. Artifacts are copied, NOT code-migrated; the migration itself comes later. Use FIRST, before aidp-build-dag / aidp-migrate-job, whenever the workload still lives only in Databricks.
---

# `aidp-dbx-export` — export a Databricks job into AIDP

`dbx_export` is the canonical Databricks ingestion path: it replicates a Databricks Job — notebooks, libraries/JARs, init scripts, workflow + cluster specs — into an AIDP workspace. It does **not** rewrite code (`dbutils.*`, `s3://` paths, DBFS calls stay exactly as on DBX); code-level migration is the later [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) stage. Think `dbx → aidp` *replication*.

## When to use

- The workload still lives only in Databricks — run this FIRST, before [`aidp-build-dag`](../aidp-build-dag/SKILL.md) (which reads the AIDP workflow this export creates).
- The user asks "export this Databricks job", "bring the job over", "stage the notebooks on AIDP".
- Re-run after the source job changed (re-runs are idempotent: already-uploaded notebooks are skipped unless `--overwrite`).

## Invocation

`dbx_export` is a package — run it with `-m` from the engine directory:

```bash
cd ~/.aidp-migrator/engine
python3 -m dbx_export --config <customer>.yaml --job-id <dbx-job-id>
```

- `--job-id` — the Databricks job_id to export (required unless `--probe`, a connectivity-only check).
- `--config` — customer YAML (falls back to `$AIDP_CONFIG_PATH`, `./aidp_migrator.yaml`, `~/aidp_migrator.yaml`). Supplies the AIDP coordinates and `paths.export_output`.
- Databricks access: `DATABRICKS_HOST` + `DATABRICKS_TOKEN` env (direct, from a laptop) OR `--dbx-proxy host:port` (cluster-side runs; optional `--proxy-basic-user/-pass`). Prefer the env vars over `--dbx-token` so the PAT never appears in argv.

Useful flags:

| Flag | Purpose |
|---|---|
| `--export-output <path>` | AIDP base path for ALL exported artifacts (default `cfg.paths.export_output`; `--target-base` is a back-compat alias) |
| `--prefix <name>` | Prefix for the created AIDP workflow + cluster names (default `ai_tool`) |
| `--dry-run` | Phase 1 + pre-flight only — no AIDP writes; use it to validate before exporting |
| `--notebook-only` | Upload the notebook tree only; skip files/jars AND Phase 3 (no cluster/workflow creation) |
| `--overwrite` | Force re-upload / re-create (the DEFAULT is no-overwrite — a run never clobbers prior artifacts) |
| `--strict` / `--strict-deps` / `--strict-jars` / `--strict-unresolved-jars` | Fail-loud variants (see below) |
| `--param KEY=VALUE` | Repeatable; resolves widget-based `notebook.run` paths during discovery |
| `--cluster-key <key>` | Reuse an existing AIDP cluster for all tasks instead of creating new ones |
| `--max-depth <n>` | BFS depth cap for child-job recursion (default 10) |
| `--lake-ocid` / `--workspace-id` / `--oci-profile` | Per-run overrides of the YAML |

## The three phases

| Phase | What happens | AIDP writes? |
|---|---|---|
| **1 — Discover + download** | Fetch the DBX job, recurse `%run` / `dbutils.notebook.run` / child jobs, download every notebook + library + init script, cross-validate formats. A PRE-FLIGHT gate then prints a summary (notebook counts, files, child jobs, cluster/workflow names). | no |
| **2 — Upload** | Upload notebooks + jars/init scripts/wheels to AIDP with read-back validation. Writes only under `<export-output>`. | yes |
| **3 — Clusters + workflow** | Create/reuse the `<prefix>_<name>` cluster(s), install file-backed libraries, create the `<prefix>_<name>` workflow pointing at the uploaded notebooks. | yes |

If Phase 2 fails partway, partial uploads stay in place (the exporter never deletes anything); a re-run picks up where it left off.

## AIDP-first dependency resolution

Library/JAR dependencies are resolved **AIDP-first**: the shared dependency repository `paths.lib_repository_dir` (default `/Workspace/migration-dependencies/libs/`) is checked **before** any Databricks or proxy fetch. A dependency already staged there (matched by name/checksum) is reused; only misses go out to DBX/proxy. Stage recurring customer JARs/wheels in that repository once and every export benefits.

Failure semantics (fail-loud where it matters, non-blocking where decided):
- **JAR fetch failures are non-blocking by default** — recorded in the report, export continues (exit code 12). `--strict-jars` aborts instead.
- **Confirmed notebook dependency failures ARE blocking** under `--strict-deps`; a 404 for a path inferred only from loose literal scanning is a reported skip.
- Failed task notebooks become **placeholders** so the workflow stays runnable; they are auto-replaced when a later export succeeds.

## Safety contract

- **Databricks is READ-ONLY.** The exporter only lists/GETs on the DBX side — never any write or job/cluster mutation there.
- **No-overwrite is the default** on the AIDP side; `--overwrite` is an explicit operator choice.
- Phase 3 CREATES real AIDP clusters and a real workflow — confirm with the user before running without `--dry-run` / `--notebook-only`.

## After this

1. [`aidp-build-dag`](../aidp-build-dag/SKILL.md) — build the manifest from the created AIDP workflow (`--job-key <workflow-key>`).
2. [`aidp-check-data`](../aidp-check-data/SKILL.md) — verify source data availability.
3. [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) — the actual code migration.
