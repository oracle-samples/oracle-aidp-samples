---
description: Check Snowflake and AIDP connectivity and permissions before migrating. Read on the source, read/write on the destination.
---

# `/snowflake-smoke`

Thin wrapper over [`snowflake-smoke-test`](../skills/snowflake-smoke-test/SKILL.md).

1. Run against Snowflake first. Pass `--database` if the account has many.
2. Check the destination too, using the config's `aidp:` block for whatever it
   carries and asking for the rest. Repeat the resolved destination back.
3. Only pass `--write-probe --execute` after telling them it creates one
   uniquely-named probe schema and removes it again. `--write-probe` alone is a
   dry run: it prints what it would create and reports write access as not attempted.
4. Present `SMOKE_TEST.md`. If the destination was skipped, say only the source
   was verified. A `verdict: PARTIAL` (exit 1) means the destination was
   skipped for want of the four coordinates; it is not a pass and not a
   connectivity failure — ask for the coordinates.
