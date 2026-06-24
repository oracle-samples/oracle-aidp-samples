# Privacy Policy

**Plugin:** `oracle-ai-data-platform-workbench-databricks-migrator`
**Surface:** OpenAI Codex CLI plugin
**Effective:** 2026-06-23

## Summary

This plugin **does not collect, store, transmit, or share any user data**. It is a **knowledge-only** plugin — Markdown SKILL files and reference docs — that teach Codex how to drive a separate Oracle migration toolkit you check out locally. Everything runs against **your own** Oracle AI Data Platform (AIDP) tenancy and **your own** Databricks workspace.

## What the plugin ships

- **16 SKILL.md** files (Markdown with frontmatter).
- **5 reference docs** (Markdown — DDL rewrite rules, gotchas, env-coords scaffold, JOB_REPORT.md format, CLI map).

That's it. No bundled Python code, no third-party telemetry, no MCP server in the plugin manifest, no `.app.json` registration.

## What the plugin does at runtime

When you invoke a skill, Codex follows the skill's Markdown instructions to call the **migrator's CLI** (which lives in a separate Oracle toolkit you cloned locally) with the right arguments. Examples of what the migrator itself does:

- Reads notebooks from your Databricks workspace via the Databricks REST API (under your token).
- Calls the AIDP REST API (under your OCI profile) to upload migrated `.ipynb` files, register jobs, and start cluster sessions.
- Opens a Spark WebSocket to your AIDP cluster to execute Databricks-rewritten cells live + verify outputs.
- Invokes a model with tool use (under your model-provider key) to rewrite Databricks-specific APIs cell by cell and self-correct on failures.

All of this is under your own credentials, against your own infrastructure, **with no involvement from the plugin author**.

## What the plugin does NOT do

- **No telemetry.** The plugin sends nothing to the author or to any third party. No analytics, no error reporting, no usage metrics.
- **No credential collection.** OCI authentication, Databricks PATs, and model-provider API keys are read from your local environment by the migrator scripts. The plugin's SKILL Markdown files cannot collect or transmit them.
- **No phone-home.** The skills make no outbound calls to the author. Every network call goes to **your** Databricks workspace, **your** AIDP REST endpoint, and **your** model provider under **your** key.

## Data flow

```
You (Codex CLI) → plugin skill (Markdown only)
                → migrator CLI (Python, in your local clone)
                → YOUR Databricks workspace + YOUR AIDP tenancy + YOUR model provider (your key)
```

There is no party between you and your infrastructure. The plugin author has no visibility into any of it.

## Marketplace install / update

When you run `codex plugin marketplace add oracle-samples/oracle-aidp-samples --sparse ai/codex-plugins` and `codex plugin add oracle-ai-data-platform-workbench-databricks-migrator@oracle-aidp-codex`, Codex clones the repo from GitHub. That clone is governed by [GitHub's privacy policy](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement). The plugin author has no visibility into that clone activity.

## Contact

For questions about this privacy policy, open an issue at <https://github.com/oracle-samples/oracle-aidp-samples/issues>.

## Changes

If this policy ever changes, the change will be announced in `CHANGELOG.md` with a major version bump.
