---
name: snowflake-provision-environment
description: Provision the migration environment inside AIDP for the prod flow - the workspace (name auto-translated to the simplest safe charset), the migration-assets compute cluster, optional cluster libraries from requirements-aidp.txt, the backup-snowflake-migration/ workspace folder holding the data-migration scripts and the plan artifacts, and the migration jobs (discover, structure, reconcile, plus one copy job per schema of the approved plan, each passing its schema as a task parameter). Use when the user wants to set up AIDP for the migration, upload the migration scripts, create the migration workspace or cluster, or wire the migration jobs. Dry-run by default.
---

# Provision the migration environment

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" provision \
  --workspace-name "<name>" \
  [--cluster-name migration-assets] \
  [--external-catalog <registered EXTERNAL catalog>] \
  [--target-catalog <INTERNAL catalog>] \
  --datalake-ocid <aiDataPlatform OCID> \
  [--subnet-id <ocid>] [--maven <coords>] [--skip-libraries] \
  [--execute]
```

Dry run first, always. Show `PROVISION.md` — it lists every step that would
run, the **translated** workspace/cluster names with the reasons, and the
API-contract note — and only then, on the user's go-ahead, re-run with
`--execute`.

## What it does, in order

1. **Workspace** — CREATED, and poll until visible. The name passes through
   the simplest-charset translation (`[a-z0-9_]`, starts with a letter,
   accents folded) so a name the API might reject never reaches it; a rename
   is reported, never silent.

   **A name already in use is a COLLISION and the run STOPS.** Nothing is
   adopted. A migration creates its own environment so that everything it
   touches can be identified, audited and torn down as a unit; inheriting a
   stranger's workspace makes the blast radius unknowable. Report the
   collision and ask the user for another name. `--reuse-existing` opts back
   in, and it is never what you offer first (its legitimate use is re-pushing
   into this migration's own workspace — see Rules).
2. **Cluster `migration_assets`** — CREATED, default small config; sizing is
   a later, explicit decision (`/snowflake-compute` proposes it). A taken
   name stops the run, exactly as for the workspace.
3. **Libraries** — only what `requirements-aidp.txt` enables (the default
   file enables NOTHING: the external-catalog path needs no extra library).
   Installing forces a cluster restart, per the AIDP doc.
4. **`backup-snowflake-migration/`** — the four data-migration scripts into
   `scripts/`, and whatever plan artifacts exist in `--out-dir`
   (`plan.json`, `ddl_plan.json`, `SUMMARY.md`, …) into `plan/`, each upload
   read back before it is called done.
5. **3 + N jobs** — `snowmig_00_discover`, `snowmig_01_structure`,
   `snowmig_03_reconcile`, and one `snowmig_02_copy_<schema>` per schema of
   the approved `ddl_plan.json` (the schemaless `snowmig_02_copy_schema`
   only before a plan is pushed). Each runs one **self-contained stage
   notebook** whose own `PARAMS` cell carries the default arguments. A job
   TASK's `parameters` win over those defaults by the same name: every
   stage notebook reads them with `oidlUtils.parameters.getParameter`, and
   that is how each per-schema copy job passes its `schema` to the ONE
   shared `02_copy_schema` notebook. `run --param` is refused; set stage
   values as task parameters or with `--stage-param`. No schedule: running
   one is always the user's call, and the PARAMS cell is editable in the
   console.

   `--stage-param NAME=VALUE` (repeatable) writes a value into the PARAMS
   cell of every stage that declares NAME — the stage flag without `--`,
   e.g. `schema=SALES`, `tables=ORDERS,LINES`, `dry-run=true`. Prefix it
   with a stage (`discover`, `structure`, `copy_schema`, `reconcile`) to
   write that stage only: `copy_schema.mode=overwrite`. An unqualified
   value a declaring stage would reject is refused — `mode` means
   different things to 01 and 02 — as is a name no stage declares; a
   switch takes `true`/`false`, and with
   `--reuse-existing` it needs `--refresh-notebooks`, because a notebook
   already on the workspace is otherwise kept as it is. Next to per-schema
   copy jobs, `schema` and `copy_schema.schema` are refused (each job's
   task parameter wins, so the value would change nothing the copy does;
   `structure.schema=` narrows 01 only), and so is a `tables` with two or
   more copy schemas: the one shared notebook would narrow every
   per-schema copy job to those names.

   A job is a **workflow**: logged, re-runnable, and its task output is
   exportable as evidence. Run one with `snowmig.py run --job <name>`, never
   by executing a notebook interactively — interactive execution leaves
   nothing behind and is not an acceptable record of a migration.

## Source mode — say which one is in play

`--source-mode connector` (the default) has the in-AIDP scripts read
Snowflake through the AIDP connector on the cluster. It needs no extra cluster
library and does not depend on the external catalog's metadata crawl. It
needs `--source-config` so the credential reaches the workspace mount: its `snowflake:` block is uploaded as JSON to
`plan/<config stem>.json` (the `aidp:` block is not copied), and because that
block carries a secret it is uploaded only when passed explicitly. A `*_path`
secret is refused before anything is uploaded — the path is not on the
cluster.

`--source-mode external-catalog` uses three-part names instead, and needs a
catalog whose metadata crawl has completed. Check that first — an empty
`SHOW SCHEMAS IN <catalog>` means the catalog has not been populated yet, not
that the database is empty. Use it only when the user explicitly asks for it.

## Rules

- The EXTERNAL catalog is registered by `/snowflake-catalog`, not here —
  one writer per concern. Pass its name via `--external-catalog` so the jobs
  are born pointing at it.
- Report `verified` per step, never `executed`. `create_requested` means the
  API accepted and the object never became visible in the poll budget — say
  it is pending and point at the console. `name_taken` is neither: it means
  something of that name was already there and this run did **not** adopt it.
- **Hand-off.** After `--execute`, the CLI (`hand-off: --workspace …
  --cluster-id …`) and a hand-off block in `PROVISION.md` print the keys,
  recorded as `workspace.key` and `cluster.key` in `provision_result.json`.
  The display names are not the keys. Pass them as `--workspace` /
  `--cluster-id` on every later command, or have the user put them under
  `aidp:` in `snowmig-config.yaml` (`aidp.workspace`, `aidp.cluster_id`) —
  one or the other. `provision` does not write them back, and no command
  reads them from the record implicitly.
- **Re-push (the plan push, S9/S10).** `provision --execute --reuse-existing
  --workspace-name <the S1 name> --plan-label FULL|REDUCED` re-adopts this
  migration's own workspace, uploads the approved plans to `plan/`, backs them
  up dated into `backup/` and registers the per-schema copy jobs. It keeps the
  cluster name the first push recorded when no `--cluster-name` is given. A
  copy job for a schema no longer in the plan is reported `stale` (exit 1)
  until it is deleted in the console or by re-pushing with
  `--delete-stale-copy-jobs`, which deletes only a job this migration's
  records show it created.
- **Never reuse, never "ensure".** Do not list existing workspaces or
  clusters and offer the user a choice among them. The only question is *may
  I create this*.
- The workspace, cluster, folder, upload, job and delete calls follow the
  documented 20260430 API contract. The cluster-library item format is
  inferred from that contract: validate one against a library created in the
  console before relying on it, and say so to the user.
- After provisioning, the run order is: the catalogs (S3, S4), then
  `snowmig_00_discover`, then `01_structure`, then each
  `snowmig_02_copy_<schema>` job (on the customer's decision), then
  `03_reconcile`. `MIGRATION_REPORT.md` in the reports folder is the
  plan-vs-reality deliverable.
