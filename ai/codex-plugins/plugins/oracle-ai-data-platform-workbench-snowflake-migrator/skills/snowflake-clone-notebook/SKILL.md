---
name: snowflake-clone-notebook
description: Generate the table-creation script for a Standard AIDP catalog as an executable notebook, locally and offline. --upload is a dry run and is refused with --execute; the structure itself is created by the snowmig_01_structure workflow at S10. The script creates schemas, then tables, then views in dependency order, prints per-object progress with elapsed time so a long run stays visible, and verifies each object individually at the end. This script creates structure only and copies no data - every table arrives with zero rows; rows are copied only by the snowmig_02_copy_schema job, when the operator runs it. Use when the user has explicitly asked for a Standard catalog, or wants the migration delivered as a runnable script rather than executed straight from the CLI.
---

> **Paths.** `<plugin-root>` is this plugin's directory, two levels above
> this `SKILL.md`. Write its absolute path wherever `<plugin-root>` appears.

# Standard-catalog table-creation script

**This is the Standard-catalog path, and only for a Standard catalog the user
explicitly asked for.** The default target is an EXTERNAL/SNOWFLAKE catalog,
which needs no tables at all — see `snowflake-medallion-clone` Phase A. Do not
reach for this skill just because a migration is in progress.

Why a script on compute: it runs **inside AIDP compute**, so every
statement's success, failure and elapsed time appears in the cluster's own
output, and each object is read back. The control-plane catalog API is used
for the catalog container only.

Generate locally. `notebook --upload` is a dry run and is refused with
`--execute`; the structure is created by `run --job snowmig_01_structure`
(S10).

**The script creates empty structure and moves no data.** Every table it
creates has its columns and zero rows. A user who hears "clone" may expect rows —
say this before they run it.

## 1. Generate (offline, safe)

```bash
"<plugin-root>/bin/snowmig" notebook \
  [--catalog <catalog>]
```

Writes `snowmig_shallow_clone_<catalog>.ipynb` locally plus `NOTEBOOK.md`.
One notebook per catalog — bronze mirrors the source, so a multi-database estate
has several.

## 2. `--upload` — a dry run, refused with `--execute`

```bash
"<plugin-root>/bin/snowmig" notebook \
  --upload --datalake-ocid <ocid> --workspace <ws> --cluster-id <cl> --catalog <cat>
```

`--upload` is a **dry run** without `--execute`: it prints where the notebook
would land and sends nothing. With `--execute` the upload is **refused**, and
the command says so. The structure is created by `snowmig provision --execute`
(which places the stage notebooks in the workspace) followed by `snowmig run
--job snowmig_01_structure` (S10). Ask the user for the four coordinates in
this turn; nothing is stored.

Nothing lands there from this command. `NOTEBOOK.md` records
`/Workspace/Shared/snowmig_shallow_clone_<catalog>.ipynb` as the intended
path, and if the user places it there themselves from the workspace UI, the
**shared** directory is the right home — re-runnable, readable and debuggable
independently of this plugin and of the conversation that generated it.
**Notebooks live in the workspace filesystem, not in a data catalog** — catalogs
hold tables and views. Say that if the user expects to find it under a catalog.

## 3. Execution is the user's call

**Do not run it for them.** Ask, then let them run it in the AIDP workspace
after they agree.

The notebook is built to be watched: each object prints
`[3/7] D.S.ORDERS ... ok (0.4s)`. If a run is slow, that output is where the
progress is — report the last line rather than guessing. The final cell prints
`verified N/M` plus any missing object.

## What the notebook does and does not do

- Creates schemas → tables → views, in that order, views after the tables they read
- `IF NOT EXISTS` throughout, so re-running is safe
- **No `INSERT`, `COPY INTO`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE` or `DROP`.**
  Its metadata records `moves_data: false`
- Tables arrive **empty**. Say this out loud — a user seeing "clone" may expect rows
- Blocked objects appear in markdown only, never in a code cell, so it cannot
  attempt them
