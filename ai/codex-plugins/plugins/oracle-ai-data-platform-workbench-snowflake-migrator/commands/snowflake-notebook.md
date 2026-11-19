---
description: Generate the shallow-clone notebook locally (offline) for a Standard catalog the user explicitly asked for. --upload is a dry run and is refused with --execute; the structure itself is created by `snowmig run --job snowmig_01_structure` at S10.
---

# `/snowflake-notebook`

Thin wrapper over [`snowflake-clone-notebook`](../skills/snowflake-clone-notebook/SKILL.md).

1. Require `ddl_plan.json`; run `/snowflake-plan` then the ddl stage first.
2. Generate locally and summarise what it will create.
3. Do not upload. Say the notebook is at
   `<out-dir>/snowmig_shallow_clone_<catalog>.ipynb` and `NOTEBOOK.md`;
   `notebook --upload` is a dry run and is refused with `--execute`.
4. To create the structure, route to the runbook path: the container from
   `snowmig.py catalog --catalog-type standard --execute` (S4,
   `container_only: true`), the environment and stage notebooks from
   `/snowflake-provision` (`provision --execute`), then
   `snowmig.py run --job snowmig_01_structure` (S10), one workflow per schema
   from the approved `ddl_plan.json`. Report `verified/total` from the
   structure run's report, never `executed` or `uploaded`.
