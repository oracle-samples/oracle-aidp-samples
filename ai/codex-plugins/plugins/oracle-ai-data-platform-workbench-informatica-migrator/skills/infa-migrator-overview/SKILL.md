---
name: infa-migrator-overview
description: Router skill. Read this first whenever the user mentions migrating Informatica ETL (IDMC/IICS cloud mappings, PowerCenter 10.x repository XML) onto Oracle AI Data Platform (AIDP). Lays out the toolkit, the CLI command sequence, which infa-* skill handles each phase, and the tool's honest limitations — no taskflow/Mapping Task/parameter-set support, no cell-by-cell execute-and-fix loop on a live cluster, nothing validated against an export from a production Informatica repository. Compose those skills; this one adds no API surface.
---

# `infa-migrator-overview` — router

> **Engine path (Codex).** The SessionStart hook stages the bundled engine to
> `~/.aidp-infa-migrator/engine`. Every command in these skills reads it as
> `${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}`, so it works in a fresh
> shell; export `INFA_ENGINE` only to point at a different checkout:
> ```bash
> export INFA_ENGINE=~/.aidp-infa-migrator/engine
> ```
> If the hook did not run, stage it by hand with `python hooks/session_start.py`
> from the plugin root. The staging directory is deliberately not
> `~/.aidp-migrator` -- that belongs to the sibling Databricks migrator, and
> two plugins sharing one tree would overwrite each other's engine.

`infa2aidp` converts Informatica mappings (PowerCenter 10.x repository XML, or
IDMC/IICS cloud JSON) into PySpark notebooks that run on Oracle AI Data
Platform. This skill picks the right next skill based on what the user asks,
and carries the gap list every other skill assumes you already know.

## When to use

- The user mentions "migrate Informatica", "PowerCenter to AIDP", "IDMC/IICS
  migration", "port this mapping", or similar.
- The user asks "where do I start" with this toolkit.
- The user asks what the tool can and cannot do.

## What this tool actually is

The full Python engine ships under `engine/infa2aidp/` in this plugin. There
is **no `engine/scripts/` directory** — unlike some other AIDP migrator
plugins, the only entry point is a CLI:

```bash
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli <command> [flags]
```

`pip install -e .` from the repo root also registers an `infa2aidp` console
script (`infa2aidp <command>`). **Verify which one resolves before trusting
it** — `pip show infa2aidp` / `which infa2aidp` — if another `infa2aidp`
package is installed system-wide (editable installs from an unrelated repo
are easy to pick up by accident), the bare command can silently run the wrong
code. `PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli` from this repo's root is
the invocation every skill in this plugin uses, because it is unambiguous.

There are **exactly 10 commands**: `discover`, `analyze`, `migrate`, `deploy`,
`reconcile`, `optimize`, `review`, `rag`, `lineage`, `version`. Every skill in
this plugin wraps one of them. Nothing else exists to invoke.

## The pipeline (mental model)

```
┌─────────────┐  ┌───────────┐  ┌──────────────┐  ┌───────────┐  ┌──────────┐  ┌────────────┐
│ discover    │→ │ analyze   │→ │ migrate       │→ │ review    │→ │ deploy   │→ │ reconcile  │
│ (optional — │  │ inventory,│  │ mappings →    │  │ human gate│  │ upload   │  │ source-DB  │
│ live repo   │  │ complexity│  │ PySpark       │  │ for LOW/  │  │ notebooks│  │ vs AIDP    │
│ crawl)      │  │ compat.   │  │ notebooks +   │  │ MEDIUM/   │  │ + create │  │ target row │
│             │  │ report    │  │ job JSON      │  │ MANUAL    │  │ jobs     │  │ diff (after│
└─────────────┘  └───────────┘  └──────────────┘  │  items    │  └──────────┘  │ a job run) │
                                                    └───────────┘                └────────────┘
             optimize (Spark perf suggestions) · lineage (field-level) · rag (learned-pattern store)
                        — side tools, run against migrate's output whenever useful
```

`discover` is the only command that talks to a live Informatica repository.
Everything else takes files: a directory of exported PowerCenter XML / IDMC
JSON, or the notebooks `migrate` already produced.

## Pick the right skill for the user's ask

| User says | Skill to invoke |
|---|---|
| First time using this toolkit, "what do I need to install" | [`infa-migrator-bootstrap`](../infa-migrator-bootstrap/SKILL.md) |
| "Pull mappings from our PowerCenter repository", "crawl the live repo" | [`infa-discover`](../infa-discover/SKILL.md) |
| "What would this migration involve", "inventory this export", "is this compatible" | [`infa-analyze`](../infa-analyze/SKILL.md) |
| "Migrate this mapping", "convert to PySpark", "run the port" | [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) |
| "Review the low-confidence items", "what needs a human look" | [`infa-review`](../infa-review/SKILL.md) |
| "Does the migrated data match the source", "prove it's correct" | [`infa-reconcile`](../infa-reconcile/SKILL.md) |
| "Push these notebooks to AIDP", "create the jobs" | [`infa-deploy`](../infa-deploy/SKILL.md) |
| "Any Spark performance issues in the generated notebooks" | [`infa-optimize`](../infa-optimize/SKILL.md) |
| "Where does this field come from", "field-level lineage" | [`infa-lineage`](../infa-lineage/SKILL.md) |
| "What has the tool learned", "manage the pattern store" | [`infa-rag`](../infa-rag/SKILL.md) |

## What this plugin does NOT do — read this before promising anything

- **IDMC orchestration has no support; PowerCenter orchestration is partial.**
  For IDMC a mapping's *transformation logic* migrates, but the Mapping Task
  that wraps it, the taskflow that sequences it, and the parameter set it
  draws `$$` values from are not parsed at all. For PowerCenter, a
  `<WORKFLOW>` *does* become an AIDP job definition (`workflows/<name>.json`:
  session task instances and links become tasks with `dependsOn`, an exactly
  convertible `<SCHEDULER>` becomes a cron, worklets are expanded), but
  non-session tasks (Command, Email, Decision, Timer, Event-Wait, Control,
  Assignment) are dropped, and link conditions other than
  `$X.Status = SUCCEEDED` are not applied -- each one is named in a
  companion `workflows/<name>.review.md`. Read that file
  before enabling any migrated job.
- **Verified on one live AIDP workspace, not many.** On 2026-09-24/25
  `infa-deploy` uploaded notebooks and created, updated and ran jobs, and
  all 12 corpus notebooks ran as an AIDP job on Spark 3.5.0 against
  synthetic tables -- 11 end to end, 1 stopped where its REVIEW marker
  says a human is needed. A real customer export has still never been run.
  There is no cell-by-cell execute/verify/fix loop the way some sibling
  migrator plugins have -- that loop, and the executor it depends on, are
  not implemented and not vendored in this repo today.
- **Generated notebooks need the `infa_compat` wheel installed on the
  cluster** (built with `pip wheel "${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" -w dist/`); every notebook
  asserts its version in its second cell.
- **Nothing has been validated against a real Informatica export.** Every
  fixture behind `demo.sh` and the test suite is authored for this project —
  11 files in `tests/fixtures/corpus/`. Of those, `orders_transform.xml` and
  the two IDMC JSON files carry full source/target metadata; the remaining
  XML files are deliberately flattened and so produce a placeholder source
  read and no target write, even though their transformation body converts
  for real. Building and running this tool
  against a real Informatica export has already turned up 15 real defects
  — expect more from a real customer estate.
- **`AdwWriteStrategy`** — the write path used by `migrate --target-catalog-type
  adw` — **is unverified against a live ADW.** It is derived from the AIDP
  connector reference (`aidp-alh`), never run. Every code path it emits
  carries an inline comment saying so; treat generated ADW-target notebooks
  as needing a first live run before trusting the write semantics.

## Six skills that are planned but do not exist yet

These were left out because none of them has a backing CLI
command — writing them would have described capability the tool doesn't
have. Do not invoke them; they are not present in `skills/`.

| Planned skill | What it would do |
|---|---|
| `infa-migrate-catalog` | Extract source/target DDL and register the AIDP catalogs/schemas a migration needs *before* the mapping migration runs, so generated `spark.table()` reads and writes have somewhere to land. |
| `infa-register-connections` | Resolve each Informatica connection to its AIDP source-access tier (catalog / external catalog / `aidataplatform` connector / native JDBC / no path — see the ladder below) and record it in the manifest. |
| `infa-build-dag` | Turn PowerCenter workflows/worklets and IDMC taskflows into a `reports/<job>_manifest.json` job DAG with cron, once workflow/taskflow parsing exists. |
| `infa-check-data` | Pre-migration scan confirming every source table/path a manifest needs is reachable before spending migration time on it. |
| `infa-fixup-cell` | Retry or rewind a single notebook cell that failed live execution. | M3 — needs the vendored cluster executor |
| `infa-resume-migration` | Resume a partially completed live-cluster migration run, skipping cells already verified. | M3 — needs the vendored cluster executor |

## Standing rules every skill in this plugin carries

- **Catalog-addressed access only.** Generated notebooks read and write via
  `spark.table("catalog.schema.table")` / `saveAsTable(...)`. No JDBC URLs,
  no credential literals (`password=`, `pwd=`, `secret=`) ever appear in a
  generated cell.
- **Unsupported constructs become explicit review items, never silent
  approximations.** `# REVIEW REQUIRED: ...` is emitted from
  `converters/transformation_converter.py`,
  `generators/notebook_generator.py`, and `generators/write_strategies.py`
  (`grep -c "REVIEW REQUIRED"` for the current count).
  [`infa-review`](../infa-review/SKILL.md) is the gate that surfaces them —
  never assume a clean `migrate` run means every mapping converted fully.
- **`DeltaTable.forName()` is Delta-only.** It is emitted by default because
  the default target is managed Delta. An ADW (or any external-catalog)
  target needs `migrate --target-catalog-type adw`, which switches every
  write cell to the JDBC-based `AdwWriteStrategy` instead — see the
  unverified-against-a-live-ADW note above before treating that path as
  proven.
- **Where AIDP has no dedicated connector for a source, there is a
  documented four-tier source-access ladder** — AIDP standard catalog →
  AIDP external catalog → dedicated `aidataplatform` connector (25 of them,
  covering Db2, Azure SQL, NetSuite, Snowflake, Salesforce, and more) →
  native Spark JDBC as a last resort → explicit "no path" for mainframe
  VSAM/IMS and SAP IDoc/RFC. The full ladder, with the Informatica
  connection-type mapping, is summarized below. **The generator does not yet
  implement tier-aware emission** — it
  emits `spark.table()` for every source today regardless of tier, so a
  mapping reading from a T3/T4 source is something
  [`infa-review`](../infa-review/SKILL.md) and a human need to catch, not
  something the CLI resolves automatically yet. `infa-register-connections`
  (see the table above) is the planned fix and does not exist.

## What the user needs before any of this does real work

See [`infa-migrator-bootstrap`](../infa-migrator-bootstrap/SKILL.md) for the
full check. Short version:

1. Python 3.9+ and the engine's dependencies (`pip install -e .` from the
   repo root, or `pip install -r "${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}/requirements.txt"`).
2. An LLM provider key — **only** for `migrate --use-llm` / `--agentic`.
   This is the Codex build, so `OPENAI_API_KEY` is the default; set
   `LLM_PROVIDER=anthropic` with `ANTHROPIC_API_KEY` to use Claude instead.
   The deterministic path (what `demo.sh` exercises, and where the 36
   transformation types and 72 expression functions are converted) needs
   neither a key nor a network connection.
3. For `infa-discover` only: reachability to the PowerCenter repository host
   plus `--user`/`--password`/`--repo`/`--domain` or the matching
   `INFA_*` environment variables.
4. For `infa-deploy` only: an OCI credential in `~/.oci/config` plus
   `AIDP_REGION` / `AIDP_INSTANCE_ID` / `AIDP_WORKSPACE_KEY` /
   `AIDP_CLUSTER_KEY` (or the matching flags), and the `infa_compat` wheel
   installed on the cluster. `pip install oci` for request signing.
5. For `infa-reconcile` only: a `--config` YAML (see
   `config/reconcile_config.example.yaml`) and, for a live Oracle source,
   `pip install -e ".[reconcile]"` (`jaydebeapi`, `oracledb`).

## The fastest way to see it work: `demo.sh`

`./demo.sh` runs `analyze` then `migrate` (rule-based, no LLM) over the
12-fixture corpus in `tests/fixtures/corpus/` and verifies the generated
notebooks are real, valid, catalog-addressed PySpark — **no cloud account,
no LLM provider key, no OCI profile required.** It ends with
`notebooks=11 error=0` on a clean checkout. Point a first-time user here
before anything that needs live credentials.

## Order of operations for a fresh migration

1. [`infa-migrator-bootstrap`](../infa-migrator-bootstrap/SKILL.md) — once per workstation.
2. [`infa-discover`](../infa-discover/SKILL.md) — only if pulling from a live PowerCenter repository; otherwise start from an export the user already has.
3. [`infa-analyze`](../infa-analyze/SKILL.md) — inventory, complexity, and compatibility, before spending LLM budget or cluster time.
4. [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) — the main run.
5. [`infa-review`](../infa-review/SKILL.md) — resolve every LOW/MEDIUM/MANUAL-confidence and `REVIEW REQUIRED` item.
6. [`infa-deploy`](../infa-deploy/SKILL.md) — upload notebooks and create AIDP jobs, then run them in AIDP.
7. [`infa-reconcile`](../infa-reconcile/SKILL.md) — once the AIDP target is populated by that run, prove the migration is correct, not just that it ran.
8. [`infa-optimize`](../infa-optimize/SKILL.md) / [`infa-lineage`](../infa-lineage/SKILL.md) / [`infa-rag`](../infa-rag/SKILL.md) as needed.

## Key references

- `references/aidp-runtime-constraints.md` — real, observed AIDP cluster behavior generated code must respect (no matplotlib/seaborn, pin pandas==2.2.3, `DeltaTable.forName()` is Delta-only, name-resolution checks).
- `env.template` (repo root) — the environment/config keys the engine reads, with defaults.
- `references/conversion-coverage.md` — every transformation type, and whether it converts, is reported, or is refused.
- `CHANGELOG.md` (repo root) — what this release includes and what it does not.
