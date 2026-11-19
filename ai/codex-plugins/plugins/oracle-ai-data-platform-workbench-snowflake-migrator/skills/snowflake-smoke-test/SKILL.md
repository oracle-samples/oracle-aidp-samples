---
name: snowflake-smoke-test
description: Check connectivity and permissions on both ends before any migration work. Verifies Snowflake is reachable and readable (list databases, read INFORMATION_SCHEMA) and, when AIDP coordinates are supplied, that the target catalog is readable and optionally writable. Use before the first assessment, when credentials change, or whenever a stage fails with an authorization error and you need to know which end is at fault.
---

> **Paths.** `<plugin-root>` is this plugin's directory, two levels above
> this `SKILL.md`. Write its absolute path wherever `<plugin-root>` appears.

# Smoke test — both ends

```bash
"<plugin-root>/bin/snowmig" smoke \
  [--database <db>] \
  [--datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>] \
  [--write-probe --execute]
```

Every Snowflake coordinate comes from the migration config (`snowmig-config.yaml`, discovered automatically and printed as `config: <path>`). Pass `--account/--user/--auth/...` only to override a field for one run.

The verdict is three-valued and the `verdict:` line on stdout names it — read
that line, not just the exit code. Exit 0 = both ends were checked and every
check passed (verdict PASS). Exit 1 = either a check that ran failed (verdict
FAIL) or only the Snowflake source was checked because the four AIDP
coordinates were not all supplied (verdict PARTIAL — not a pass, not a
connectivity failure; supply the coordinates). `SMOKE_TEST.md` and the
`STAGES.md` smoke row carry the same verdict, so a failure points at one end
rather than "it doesn't work".

## What it checks

| End | Check | Needs |
|---|---|---|
| Snowflake | identity: user, role, account, region | connection |
| Snowflake | list databases | `USAGE` on at least one database |
| Snowflake | read `INFORMATION_SCHEMA` | read on a real database — the probe is **qualified**, because a fresh session has no current database, so an unqualified reference fails with `090105` for any role |
| AIDP | read the target catalog (`SHOW SCHEMAS`) | the four coordinates |
| AIDP | **write** — creates a probe schema | `--write-probe --execute` |

## The write probe writes — say so before you pass the flag

Proving write means actually writing. `--write-probe --execute` creates ONE
schema named `snowmig_permission_probe_<8 hex chars>` (a fresh suffix per run,
so no earlier probe's name is ever reused) in the target catalog,
confirms it is visible, then **drops that one schema**. Never `CASCADE`; it
only ever removes the schema it just created.

The no-`DROP` rule is a **source** guarantee: nothing is ever written to or
dropped from Snowflake. It does not extend to AIDP, which is where this plugin
legitimately creates objects.

It is still off by default, because it writes, and like every write it needs
`--execute`: `--write-probe` alone is a dry run that prints what it would create
and reports write access as *not attempted*. Tell the user what it will create
before you pass both flags. If cleanup fails, the report names what was left under
"left behind" — pass that on.

**If the target catalog is EXTERNAL, the probe is skipped with a note** — an
EXTERNAL catalog is a registered, read-only pointer at the live Snowflake
source, so it accepts no writes by design. That skip is correct behaviour, not
a failure: say so rather than treating "write not verified" as a problem. The
report carries `catalog_type` either way ("unknown" when it could not be
resolved).

## If the destination is skipped

Without all four AIDP coordinates the destination section reads *Skipped*. That
is not a pass. Say plainly that only the source was verified, and ask for the
coordinates if the user wants the destination checked too. The CLI prints
`verdict: PARTIAL` and exits 1 in this case (see the exit contract above); do
not report it as a failed check.
