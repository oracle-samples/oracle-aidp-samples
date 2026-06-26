# Changelog

All notable changes to this plugin are documented here. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.2.0] - 2026-06-24

- Bundled the OpenAI-based Python migration engine under `engine/`.
- Added a Codex SessionStart hook that stages the engine to `~/.aidp-migrator/engine`.
- Updated skills and references to invoke `~/.aidp-migrator/engine/scripts/...` instead of requiring a separate migrator clone.
- Replaced legacy provider-specific setup language with `OPENAI_API_KEY` / `OPENAI_MODEL` guidance.
- Kept customer-specific coordinates and secrets out of the plugin; users provide them through local env vars and gitignored env-coords files.

## [0.1.0] — 2026-06-23 (initial release)

First public release of the **Codex CLI** plugin for the Oracle AIDP Databricks Migration Toolkit. Codex has no separate "commands" or "agents" abstraction at the plugin layer - both are folded into the `skills/` directory.

### Skills (16)

**Core toolkit (10) — identical to the source toolkit guidance:**

- `aidp-migrator-overview` — router / lay of the toolkit
- `aidp-migrator-bootstrap` — environment readiness check (Python deps, OCI auth, cluster state, env-coords)
- `aidp-build-dag` — build migration manifest from a Databricks workspace path
- `aidp-check-data` — pre-migration data-availability scan
- `aidp-migrate-job` — Pass-1 deps + Pass-2 cell-by-cell execute/verify/fix on a live AIDP cluster
- `aidp-fixup-cell` — targeted rewind: re-execute cells from a history index
- `aidp-resume-migration` — resume an interrupted run
- `aidp-migrate-catalog` — Unity Catalog / HMS DDL → 18-rule rewriter → batched replay
- `aidp-bucket-mapping` — `s3://` → `oci://` bucket/namespace mapping config
- `aidp-acceptance-contract` — consecutive-zero-window convergence for batch / streaming

**Guided flows (4) — translated from the guided command flows:**

- `migrate-job-flow` — guided full-job migration with phase checkpoints
- `migrate-catalog-flow` — guided catalog migration with dry-run preview
- `check-data-flow` — pre-migration data-availability scan wrapper
- `migration-status` — parse + summarize a `JOB_REPORT.md`

**Specialist reviewers (2) — translated from the specialist reviewers:**

- `databricks-notebook-analyzer` — pre-migration single-notebook readiness report
- `migration-reviewer` — post-Pass-2 migrated-notebook correctness review (catches cell-execute drift that PASS verdicts miss)

### References (5)

- DDL rewrite rules (18 rules with examples)
- 15 Databricks → AIDP gotchas + fix recipes
- Env-coords scaffold
- `JOB_REPORT.md` parsing format
- CLI map (every migrator entrypoint → purpose + canonical invocation)
