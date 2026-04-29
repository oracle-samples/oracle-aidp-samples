# Claude Code plugins

This directory hosts Claude Code plugins published by the Oracle AI Data Platform team.

Each subdirectory is a self-contained plugin (with its own `.claude-plugin/plugin.json`, skills, helpers, examples, and tests). Plugins are referenced from Anthropic's community Claude Code plugin marketplace ([`anthropics/claude-plugins-community`](https://github.com/anthropics/claude-plugins-community)) via a `git-subdir` source pointing at the plugin's directory in this repo.

## Plugins

| Plugin | What it does |
|---|---|
| [`oracle-ai-data-platform-workbench-spark-connectors`](oracle-ai-data-platform-workbench-spark-connectors/) | 18 model-invokable skills connecting Oracle AI Data Platform Workbench Spark notebooks to Oracle (ALH/ADW/ATP, ExaCS, Fusion ERP, BICC, EPM Cloud, Essbase) and external (PostgreSQL, MySQL/HeatWave, SQL Server, Snowflake, Azure ADLS Gen2, AWS S3, OCI Streaming, Object Storage, Iceberg, generic REST/JDBC, Excel) data sources. |

## Installing

```
/plugin marketplace add anthropics/claude-plugins-community
/plugin install <plugin-name>
```

## Authoring a new plugin

Follow [Anthropic's plugin reference](https://code.claude.com/docs/en/plugins-reference). Each plugin must have:
- `.claude-plugin/plugin.json` at the plugin root (sibling-to-this-README level + 1)
- A `README.md`, `LICENSE` (MIT preferred for samples), and `CHANGELOG.md`
- Action-oriented `description:` frontmatter on every `SKILL.md` so Claude Code's skill discovery fires correctly
- Optional but encouraged: `examples/`, `tests/` (unit tests for any helpers), and a live-test results matrix

Once the plugin is merged here, request listing in the community marketplace via the [Claude Code plugin directory submission form](https://clau.de/plugin-directory-submission). Reference this directory using `git-subdir` source pattern.
