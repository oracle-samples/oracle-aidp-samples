# Privacy Policy

**Plugin:** `oracle-ai-data-platform-workbench-snowflake-migrator`
**Effective:** 2026-10-01

## Summary

This plugin **does not collect, store, transmit or share any data with its
authors**. There is no telemetry, no analytics and no usage reporting. It is
self-contained: the migration engine ships under `engine/` and runs locally
against **your** Snowflake account and **your** Oracle AI Data Platform (AIDP)
tenancy, and — from `provision` on — as notebooks on **your** AIDP compute.
Table rows never pass through the operator's machine.

## What the plugin ships

- **Skills and slash commands** (Markdown) under `skills/` and `commands/`.
- **The Python engine** under `engine/` (runtime dependencies:
  `snowflake-connector-python`, `cryptography`, `pyyaml`).
- **Data-plane notebooks** under `data-migration-scripts/`, which `provision`
  uploads to your AIDP workspace.
- **Reference docs** under `references/`.

No bundled credentials, no MCP server, no third-party network calls.

## Where it connects

1. **Your Snowflake account** — read-only, from your machine through
   `snowflake-connector-python`, and from your AIDP cluster through the AIDP
   Snowflake connector when the data-plane jobs run. The transport refuses
   any statement that is not a read.
2. **Your AIDP tenancy** — through the `aidp` CLI or `oci raw-request`, using
   the OCI configuration already on your machine, over TLS. The plugin does
   not read or transmit your OCI credentials.

## Credentials

- The Snowflake connection lives in one local file, `snowmig-config.yaml`
  (inline, or as `key_path:` / `password_path:`). Secrets are never taken as
  command-line flags, and every report renders the config through a
  redactor.
- The Snowflake credential reaches your AIDP tenancy in two ways, each only
  with `--execute`:
  1. `provision --execute --source-config <file>` uploads the `snowflake:`
     block, as JSON, to `backup-snowflake-migration/plan/<config stem>.json`
     (`plan/snowmig-config.json` by default) so the data-plane jobs can reach
     Snowflake. The `aidp:` block is not copied. Workspace access controls
     who can read it.
  2. `catalog --execute` registers the EXTERNAL catalog with the credential
     in its `connectionDetails` (`SNOWFLAKE_PASSWORD` or
     `SNOWFLAKE_PRIVATE_KEY_CONTENT`), sent from a temporary file, never as a
     command-line argument.
- Use a dedicated, read-only Snowflake user for the migration, and rotate
  its credential when the migration is done. `teardown --scope credential
  --execute` removes the workspace copy.

## What it writes

- **Locally**, to `--out-dir` (default `./migration-artifacts/` in your
  working directory, with its own `.gitignore`): metadata about your estate —
  object and column names, types, row counts, view SQL, generated DDL and
  notebooks, and the AIDP coordinates a run used. No table rows. Treat these
  files as sensitive as your schema.
- **A stage log**, in the same directory: each stage appends its start and
  end to `run_log.jsonl`. No agent transcript is read under Codex, so token
  use is reported as not measured. With `reporting.publish_each_stage: true`,
  or `publish --execute`, the log is uploaded to the migration workspace with
  the reports.
- **In your AIDP tenancy**, only with `--execute` (or, for `run`, when you
  start a job): the migration workspace, cluster, folder, notebooks and jobs
  (`provision --execute`); the catalogs (`catalog --execute`); the target
  schemas and tables and, when you run a copy job, the rows — the copy job
  reads every row of each in-scope table from Snowflake into your AIDP
  catalog (`run`); a probe schema `snowmig_permission_probe_<suffix>`,
  created and dropped (`smoke --write-probe --execute`). `notebook --upload`
  is not a writer: it is a dry run, and refused with `--execute`.

## What the plugin does not do

- **No telemetry, no phone-home.** Every network call goes to your Snowflake
  account or your AIDP tenancy.
- **No writes to Snowflake.** Nothing is created in, changed in or dropped
  from the source.
- **No credential collection.** Credentials stay in your config file and
  your OCI configuration, and reach AIDP only as described above.

## Data flow

```
You (Codex) → plugin skills (Markdown)
                  → engine on your machine ──► your Snowflake (read-only)
                                           └─► your AIDP tenancy (dry run unless --execute)
                  → notebooks on your AIDP cluster ──► your Snowflake (read-only) → your AIDP catalog
```

The skills instruct Codex, and the reports it generates are read back into
your conversation, which is handled under the terms of the OpenAI product you
use. The plugin never asks you to paste a secret into the conversation.

## Removal

Delete the plugin directory, `migration-artifacts/` (or your `--out-dir`) and
`snowmig-config.yaml`. On AIDP, `teardown --scope all --execute` removes what
the migration created (the INTERNAL catalog, which holds the migrated rows,
only with `--include-data`).

## Contact

For questions about this policy, open an issue at
<https://github.com/oracle-samples/oracle-aidp-samples/issues>.
