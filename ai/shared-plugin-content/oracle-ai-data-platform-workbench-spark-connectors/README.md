# Oracle AI Data Platform Workbench Spark Connectors shared skills

This directory is the canonical source for the Spark connector `SKILL.md` files shared by the Claude Code and Codex plugin variants.

Edit skills here first, then run:

```bash
python3 ai/shared-plugin-content/oracle-ai-data-platform-workbench-spark-connectors/sync_skills.py
```

The script refreshes:

- `ai/claude-code-plugins/oracle-ai-data-platform-workbench-spark-connectors/skills`
- `ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-spark-connectors/skills`

The plugin variants keep materialized copies instead of symlinks so plugin installers that package directories directly do not need symlink support.
