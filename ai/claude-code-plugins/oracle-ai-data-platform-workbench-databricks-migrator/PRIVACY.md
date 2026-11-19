# Privacy Policy

**Plugin:** `oracle-ai-data-platform-workbench-databricks-migrator`
**Effective:** 2026-06-24

## Summary

This plugin **does not collect, store, transmit, or share any user data**. It is a **self-contained** plugin that ships the full migrator engine bundled under `engine/`. Everything runs locally against **your own** Oracle AI Data Platform (AIDP) tenancy and **your own** Databricks workspace.

## What the plugin ships

- **10 SKILL.md** files (Markdown with frontmatter) under `skills/`.
- **4 slash commands** (Markdown) under `commands/`.
- **2 specialist agents** (Markdown) under `agents/`.
- **reference docs** (Markdown) under `references/` — DDL rewrite rules, gotchas, env-coords scaffold, `JOB_REPORT.md` format, CLI map.
- **The full Python migration engine** bundled under `engine/`:
  - `engine/scripts/` (Python engine modules) — `job_migrate.py`, `agent_migrate.py`, `cluster_session.py`, `aidp_executor.py`, `build_dag.py`, `check_data_availability.py`, `migrate_catalog.py`, `extract_catalog_databricks.py`, `acceptance_contract.py`, etc.
  - `engine/aidp_compat/` (21 Python files) — drop-in `dbutils` compatibility shim for AIDP clusters.
  - `engine/schemas/` — JSON schemas (acceptance contract).
  - `engine/setup.py`, `engine/requirements.txt` — Python package metadata + deps.
  - `engine/run_migration.sh` — generic convenience script.

No bundled credentials, no telemetry, no MCP server, no third-party network calls beyond the user's own infrastructure + their chosen model provider.

## What the plugin does at runtime

When you invoke a skill, Claude follows the skill's Markdown instructions to call the **bundled migrator engine** at `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...` with the right arguments. Examples of what the engine itself does:

- Reads notebooks from your Databricks workspace via the Databricks REST API (under **your** Databricks PAT).
- Calls the AIDP REST API (under **your** OCI profile) to upload migrated `.ipynb` files, register jobs, and start cluster sessions.
- Opens a Spark WebSocket to **your** AIDP cluster to execute Databricks-rewritten cells live + verify outputs.
- Invokes Claude with tool use (under **your** `ANTHROPIC_API_KEY`) to rewrite Databricks-specific APIs cell by cell and self-correct on failures.

All of this is under **your own** credentials, against **your own** infrastructure, with no involvement from the plugin author.

## What the plugin does NOT do

- **No telemetry.** The plugin sends nothing to the author or to any third party. No analytics, no error reporting, no usage metrics.
- **No credential collection.** OCI authentication, Databricks PATs, and `ANTHROPIC_API_KEY` are read from **your** local environment by the bundled engine. The plugin cannot collect or transmit them.
- **No phone-home.** The skills make no outbound calls to the author. Every network call goes to **your** Databricks workspace, **your** AIDP REST endpoint, and Anthropic's API under **your** key.

## Data flow

```
You (Claude Code) → plugin skill (Markdown only)
                  → bundled engine (${CLAUDE_PLUGIN_ROOT}/engine/scripts/...)
                  → YOUR Databricks workspace + YOUR AIDP tenancy + Anthropic API (your key)
```

There is no party between you and your infrastructure. The plugin author has no visibility into any of it.

## Marketplace install / update

When you `/plugin marketplace add` and `/plugin install` from the public GitHub repo, Claude Code clones the repo from GitHub. That clone is governed by [GitHub's privacy policy](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement). The plugin author has no visibility into that clone activity.

## Contact

For questions about this privacy policy, open an issue at <https://github.com/oracle-samples/oracle-aidp-samples/issues>.

## Changes

If this policy ever changes, the change will be announced in `CHANGELOG.md` with a major version bump.
