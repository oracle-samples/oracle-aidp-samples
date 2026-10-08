---
description: Dev mode — run the whole migration pipeline against an emulated Snowflake estate and an emulated AIDP. No credentials, no network, nothing real is touched; writes every real artifact plus a narrated DEMO.md.
---

> **Paths.** `<plugin-root>` is this plugin's directory, the parent of
> this `commands/` folder. Write its absolute path wherever `<plugin-root>` appears.

# `/snowflake-demo`

Thin wrapper over
[`snowflake-migrator-demo`](../skills/snowflake-migrator-demo/SKILL.md).

1. Run `"<plugin-root>/bin/snowmig" demo --out-dir
   ./snowmig_demo`. Nothing to ask the user for. For a larger enterprise
   estate (external/Iceberg/hybrid/event tables, shares, policies,
   container services, replication), add `--estate enterprise` and a separate
   `--out-dir ./snowmig_demo_enterprise`; that run stops before any AIDP step.
2. Lead with the banner: **everything in the output is emulated.**
3. Present `DEMO.md` — the stage-by-stage narrative and the lessons list —
   then point at `SUMMARY.md` and `STAGES.md` as previews of what a real run
   delivers.
