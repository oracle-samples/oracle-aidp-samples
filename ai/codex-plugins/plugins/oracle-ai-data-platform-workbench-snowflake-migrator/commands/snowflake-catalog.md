---
description: Register the target catalog on AIDP — EXTERNAL/SNOWFLAKE by default, a read-only pointer at the live source that copies nothing. Dry-run first; --execute only after showing the user exactly what will be registered.
---

# `/snowflake-catalog`

Thin wrapper over the registration phase of
[`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Let the CLI find the migration config and **repeat the `config:` line it
   prints**, plus the destination it resolved. Ask for any coordinate the file
   does not carry (DataLake OCID, workspace, cluster id, catalog name) **in
   this turn**. Ask permission before reading the config yourself: it holds
   live credentials. Never ask for a secret in the conversation.
2. Dry-run first: `snowmig.py catalog --out-dir ... --catalog <name>`. Show
   `CATALOG.md` — it lists the connection *field names*, never the values.
3. On the user's go-ahead, re-run with `--execute`. The destination must be
   confirmed in that same turn; a value sitting in the config is not an
   approval.
4. Report `action` and `verified` from `catalog_result.json` — never
   `executed`. If a catalog of that name exists and this migration did not
   create it, the stage stops (exit 1) without touching it: report it as a
   name collision and ask for another name. Pass `--reuse-existing` only when
   the user explicitly says to migrate into that existing catalog; a catalog
   of the other type stops the stage either way. `create_requested` means the create was accepted but the
   catalog never became visible: say it is pending, not done.
5. Validate the EXTERNAL catalog with `--test-connection` (with `--execute`)
   and report the result as it is; `PENDING` is not a pass. If it returns
   `FAILED` without a reason, keep the registration and continue: discovery
   (S6) validates the connection through the connector. Never delete and
   re-register to make the test pass.
6. A **Standard** (INTERNAL) catalog is created here too, as the CONTAINER
   only: at runbook S4 in every migration, or when the user asks for one
   outside the runbook:
   `snowmig.py catalog --catalog <name> --catalog-type standard --execute`.
   `catalog_result.json` then carries `container_only: true` — pass that on,
   so nobody reads a created container as created structure. Its schemas and
   tables come later, at S10, from
   `snowmig.py run --job snowmig_01_structure` (one workflow per schema on
   AIDP compute, each create read back), never through this command: the
   control-plane catalog API is used for the catalog container only.
