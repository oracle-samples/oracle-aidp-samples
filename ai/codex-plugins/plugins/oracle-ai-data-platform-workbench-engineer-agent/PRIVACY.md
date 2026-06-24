# Privacy Policy

**Plugin:** `oracle-ai-data-platform-workbench-engineer-agent`
**Effective:** 2026-06-12

## Summary

This plugin **does not collect, store, transmit, or share any user data**. Everything runs locally against
**your own** Oracle AI Data Platform (AIDP) tenancy.

## What the plugin does

The plugin ships **37 Codex skills** (Markdown files with frontmatter) and **two bundled Python helpers**
(`aidp/aidp_sql.py` for interactive Spark-SQL over the Jupyter WebSocket, `aidp/check_env.py` for a
one-time dependency/readiness check; both staged to `~/.aidp/` on SessionStart). When invoked, the skills run the AIDP control plane via the official
Oracle `aidp` CLI (or an `oci raw-request` fallback) and interactive Spark-SQL via the helper — all against
**your own** AIDP DataLake, workspace, and cluster. A local grounding cache (`.aidp/catalog.md`,
`.aidp/semantic.md`, `.aidp/verified-queries.md`) is written into your project directory only.

## What the plugin does NOT do

- **No telemetry.** The plugin sends nothing to the author or to any third party. No analytics, no error
  reporting, no usage metrics.
- **No credential collection.** OCI authentication (api_key or `oci session authenticate` session-token
  profiles) is read from your local `~/.oci/config` at runtime by the `oci` CLI and the bundled helper. The
  plugin code never logs, transmits, or persists tokens, keys, or fingerprints. The short-lived UPST minted by
  `aidp_sql.py` (for api_key profiles) lives only in memory for the WebSocket session.
- **No phone-home.** The skills make no outbound calls to the author. Every network call — `aidp` CLI /
  `oci raw-request` control-plane requests and the Spark-SQL WebSocket — goes to **your** AIDP REST endpoint
  (`https://aidp.<your-region>.oci.oraclecloud.com`) under your own OCI credentials. `ai_generate(...)` in SQL
  runs on your AIDP cluster against your tenancy's GenAI models.
- **The `.aidp/` cache stays local.** It is git-ignored, written only into your working directory, and never
  transmitted. It contains catalog/schema/table names and validated query text *you* chose to cache.
- **No bundled third-party telemetry.** Runtime deps (`oci`, `requests`, `websocket-client`, `cryptography`)
  follow their vendors' own privacy policies; the plugin wraps them without modification.

## Data flow

```
You (Codex) → plugin skill (Markdown + local Python helpers)
                  → official aidp CLI / oci raw-request / Jupyter WebSocket
                  → YOUR Oracle AI Data Platform tenancy (REST + Spark cluster)
```

There is no party between you and your AIDP tenancy. The plugin author has no visibility into any of it.

## Marketplace install / update

When you `codex plugin marketplace add` and `codex plugin add` from the public GitHub repo, Codex clones the
repo from GitHub. That clone is governed by
[GitHub's privacy policy](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement).
The plugin author has no visibility into that clone activity.

## Contact

For questions about this privacy policy, open an issue at
<https://github.com/oracle-samples/oracle-aidp-samples/issues>.

## Changes

If this policy ever changes (e.g. if the plugin ever starts collecting data), the change will be announced in
`CHANGELOG.md` with a major version bump.
