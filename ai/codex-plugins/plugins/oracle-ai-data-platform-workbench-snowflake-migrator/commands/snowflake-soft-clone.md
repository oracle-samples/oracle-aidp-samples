---
description: Create the target catalog and medallion structure on AIDP from an approved plan. Registers an EXTERNAL/SNOWFLAKE catalog by default; a Standard catalog at runbook S4 or when you ask for one, its structure created on AIDP compute.
---

# `/snowflake-soft-clone`

Thin wrapper over [`snowflake-medallion-clone`](../skills/snowflake-medallion-clone/SKILL.md).

1. Require an approved `plan.json`.
2. Ask for the DataLake OCID, workspace, cluster id, catalog name and the path
   to the migration config **in this turn** — or confirm the one the CLI
   discovered and printed.
3. Confirm, then register the **EXTERNAL/SNOWFLAKE** catalog with
   `snowmig.py catalog --execute`. It copies nothing and creates no tables.
4. **Standard catalog (runbook S4, or when the user asks for one):** create the
   container with `snowmig.py catalog --catalog <name> --catalog-type standard
   --execute` (S4; `catalog_result.json` says `container_only: true`), then
   generate DDL and show `DDL_PLAN.md`, then create the structure on AIDP
   compute with `snowmig.py run --job snowmig_01_structure` (S10), one
   workflow per schema, reading the approved `ddl_plan.json` from the
   workspace, each create read back. The control-plane catalog API is used
   for the catalog container only.
5. Report the registered catalog, or `verified/total` from the structure
   run's report for a Standard clone — never `executed`.
