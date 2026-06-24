# Changelog

All notable changes to this plugin are documented here. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [0.1.0+codex.20260623113518] — 2026-06-23 (initial release)

First public release of the **Codex CLI** plugin for the Oracle AI Data Platform (AIDP) Workbench Engineer Agent. Mirrors the Claude Code plugin (`ai/claude-code-plugins/oracle-ai-data-platform-workbench-engineer-agent`). Codex has no separate "commands" or "agents" abstraction at the plugin layer — everything is folded into the `skills/` directory.

### Skills (37)

The full AIDP data-engineering surface: catalog discovery and grounding, plain-English Spark SQL, AI-in-SQL (`ai_generate`), the Delta lifecycle (CREATE/INSERT/UPDATE/DELETE/MERGE/OPTIMIZE/VACUUM/DESCRIBE HISTORY/time-travel), file ingestion, data profiling and quality, pipelines/jobs, cluster provisioning, governance (roles, credentials, Delta Sharing, audit), agent flows, MLOps, migration, and workspace administration. SKILL.md content is kept identical to the Claude Code plugin so guidance stays in sync.

### Codex-specific packaging

- **Helper layout.** The Spark-SQL helper and readiness check live in `aidp/` (`aidp_sql.py`, `check_env.py`) instead of the Claude `scripts/` dir. Every skill references `$HOME/.aidp/aidp_sql.py`.
- **SessionStart hook.** `hooks/session_start.py` stages `aidp/` → `~/.aidp/` and runs the staged `check_env.py` (installs Python deps if missing, reports OCI readiness) on each session.
- **MCP is optional.** `.mcp.json.template` documents how to wire the optional AIDP MCP accelerator; the plugin is fully self-contained without it (`oci raw-request` + the bundled helper). The filled `.mcp.json` is git-ignored.
