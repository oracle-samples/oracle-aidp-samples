# Privacy Policy

**Plugin:** `oracle-ai-data-platform-workbench-spark-connectors`
**Effective:** 2026-04-27

## Summary

This plugin **does not collect, store, transmit, or share any user data**.

## What the plugin does

The plugin ships **20 Claude Code skills** (Markdown files with frontmatter) and **a Python helper package** that runs in the user's own Oracle AI Data Platform Workbench notebook. When invoked, the skills produce Spark JDBC, REST, or structured-streaming code snippets that the user pastes into their notebook. All execution happens on the user's own infrastructure (their AIDP cluster, their database, their object storage).

## What the plugin does NOT do

- **No telemetry.** The plugin sends nothing to the author or to any third party. No analytics, no error reporting, no usage metrics.
- **No credential collection.** Auth values (database passwords, OCI tokens, AWS keys, Azure secrets, etc.) are read from the user's local environment variables / OCI Vault / `.env` file at runtime by the user's own notebook code. The plugin code never logs, transmits, or persists them.
- **No phone-home.** The skills don't make outbound network calls. Any network calls — Spark JDBC, REST APIs, OCI Object Storage, S3, ADLS — are made by the user's notebook code to the user's own configured endpoints.
- **No third-party SDKs that telemetry.** The plugin's runtime dependencies (`requests`, `oci`, `pyspark`, `psycopg2-binary` if installed by the user) follow their respective vendors' privacy policies; the plugin itself wraps them without modification.

## Data flow

```
User notebook
   ↓
Plugin skill (Markdown text + Python helpers, runs locally)
   ↓
User-configured endpoint (Oracle ALH, Fusion REST, S3, etc.)
```

There is no fourth box. The plugin does not introduce any party between the user and their data.

## Marketplace install / update

When you `/plugin marketplace add` and `/plugin install` from the public GitHub repo, Claude Code clones the repo from GitHub. That clone operation is governed by [GitHub's privacy policy](https://docs.github.com/en/site-policy/privacy-policies/github-general-privacy-statement). The plugin author has no visibility into that clone activity.

## Contact

For questions about this privacy policy, open an issue at
<https://github.com/ahmedawan-oracle/oracle-ai-data-platform-workbench-spark-connectors/issues>.

## Changes

If this policy ever changes (e.g. if the plugin starts collecting data), the change will be announced in `CHANGELOG.md` with a major version bump.
