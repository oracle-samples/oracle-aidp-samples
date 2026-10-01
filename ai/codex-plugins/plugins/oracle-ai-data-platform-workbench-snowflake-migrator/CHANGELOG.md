# Changelog

Release notes for the Oracle AI Data Platform (AIDP) Workbench Snowflake
migrator Codex plugin, newest first. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

## [0.28.0] — 2026-10-01

### Added
- First Codex release. Same engine, data-plane notebooks, skills and
  twelve-step runbook as the Claude Code plugin 0.28.0
  (`ai/claude-code-plugins/oracle-ai-data-platform-workbench-snowflake-migrator`).
  Codex has no plugin-level commands, so that plugin's `commands/` are not
  shipped; each was a thin wrapper over a skill listed in the README.
  Skills address the plugin's files as `<plugin-root>`, resolved from each
  `SKILL.md`.
- The changes and fixes in that plugin's 0.28.0 notes apply to this copy too:
  among them the every-column source type drift check
  (`mapping.source_type_drift`), teardown deleting only what this migration
  created on its own DataLake and workspace, and `clean` refusing a directory
  that records created resources until `teardown` has run.
