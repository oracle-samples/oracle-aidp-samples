---
description: Build and present a high-level Snowflake to AIDP migration plan - dependency waves plus a proposed medallion layout - for approval.
---

> **Paths.** `<plugin-root>` is this plugin's directory, the parent of
> this `commands/` folder. Write its absolute path wherever `<plugin-root>` appears.

# `/snowflake-plan`

Thin wrapper over [`snowflake-migration-plan`](../skills/snowflake-migration-plan/SKILL.md).

1. Require `inventory.json`; run `/snowflake-assess` first if absent.
2. Run `deps`, then `plan`. State the lineage source and whether it is partial.
3. Walk through the waves and the medallion assignment, calling out every fallback assignment.
   If any object is `register_in_place` (external or Iceberg table), run
   `"<plugin-root>/bin/snowmig" external-registration` and present
   `EXTERNAL_REGISTRATION.md`: the files move to OCI Object Storage first, and
   nothing in it is executed.
4. Get explicit agreement before any clone.
