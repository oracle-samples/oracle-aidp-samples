---
description: Investigate a Snowflake environment and list every table and view with row counts (Snowflake's metadata count by default), sizes, and column types. Read-only.
---

# `/snowflake-assess`

Thin wrapper over [`snowflake-assess-estate`](../skills/snowflake-assess-estate/SKILL.md).

1. If auth has never been verified, run [`snowflake-migrator-bootstrap`](../skills/snowflake-migrator-bootstrap/SKILL.md) first.
2. Ask which databases to scope, warning that exact row counts cost warehouse time.
3. Run the assess stage; present `INVENTORY.md`.
4. On exit 3, show the identifier-case collisions and stop.
