# AIDP Environment Coordinates

Copy this file to your project as `env-coords.md` (gitignored) and fill in your environment details before running the migrator.

## Primary method: the customer YAML (`--config`)

The engine's primary configuration mechanism is a **customer YAML config file**, not environment variables. Template: `~/.aidp-migrator/engine/default_aidp_migrator.yaml` — copy it, fill in your values, and pass it to every entrypoint:

```bash
python3 ~/.aidp-migrator/engine/scripts/job_migrate_from_workflow.py --config <customer>.yaml --job-key <workflow-key>
```

Discovery order when `--config` is omitted:

1. `--config <path>` (explicit flag — always wins)
2. `$AIDP_CONFIG_PATH`
3. `./aidp_migrator.yaml`
4. `~/aidp_migrator.yaml`

The loader validates the YAML against `~/.aidp-migrator/engine/aidp_config/schema.json` and fails loudly. Key fields to fill:

```yaml
aidp:
  lake_ocid: <datalake-ocid>          # ocid1.aidataplatform.oc1.<region>.<id>
  workspace_id: <workspace-uuid>      # 8-4-4-4-12 UUID
  region: <your-region>               # e.g. us-ashburn-1
  oci_profile: <profile>              # section name in ~/.oci/config
paths:
  migrate_output: <output-workspace-path>   # where migrated notebooks land
  export_output: <export-workspace-path>    # where dbx_export stages artifacts
bucket_mapping:
  path: <path-to-bucket_map.csv>      # s3_bucket,oci_bucket,oci_namespace CSV
llm:
  provider: openai                    # this bundle's default; "anthropic" also dispatches
  model: gpt-5.5
```

Per-script CLI flags (`--lake-ocid`, `--workspace-id`, `--oci-profile`, `--migrate-output`, `--bucket-mapping`, ...) are **per-run overrides** of the YAML.

## Secondary method: environment-variable overrides

The loader also honors a fixed set of environment overrides (`_ENV_OVERRIDES` in `~/.aidp-migrator/engine/aidp_config/loader.py`). Each one overrides the corresponding YAML field — use them for one-off shells, not as the primary config. (An env var is ignored when the YAML declares that field as a credential-store ref — a plaintext export must not silently downgrade a secret ref.)

```bash
# Config file discovery
export AIDP_CONFIG_PATH=<path-to-customer-yaml>

# aidp.* — AIDP coordinates
export AIDP_LAKE_OCID=<datalake-ocid>
export AIDP_WORKSPACE_ID=<workspace-uuid>
export AIDP_REGION=<your-region>
export AIDP_OCI_PROFILE=<profile>

# paths.*
export AIDP_MIGRATE_OUTPUT=<output-workspace-path>
export AIDP_EXPORT_OUTPUT=<export-workspace-path>
export AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE=<path>
export AIDP_COMPATIBILITY_POLICY_FILE=<path>

# runtime.* / secrets.* / dbx_source.*
export AIDP_READINESS_REPORT=<path>        # runtime.readiness_report_path
export AIDP_MLFLOW_URI=<uri>               # secrets.mlflow_tracking_uri
export AIDP_ENTITY_CRYPTO_ARN=<arn>               # secrets.entity_crypto_arn
export AIDP_DBX_HOST=<databricks-host>     # dbx_source.host (dbx_export only)
export AIDP_DBX_PROXY=<host:port>          # dbx_source.proxy (dbx_export only)
```

> Names like `AIDP_BASE`, `AIDP_DATALAKE_OCID`, `AIDP_CLUSTER_ID`, `OCI_PROFILE` are NOT read by the config loader — the list above is exact.

Optional, read directly by the migrator (not a loader override):

```bash
# FQCN of the entity-crypto UDF registrar class (the JAR's registerUdf entry
# point). Overrides secrets.entity_crypto_registrar; REQUIRED when
# entity_crypto_arn is set.
export AIDP_ENTITY_CRYPTO_REGISTRAR=<fqcn>
```

## LLM provider API key

```bash
# OpenAI API key for model-driven cell rewriting (Pass-2) — the default
# provider in this bundle (llm.provider: openai).
export OPENAI_API_KEY=sk-...
```

If you switch the YAML to the alternative provider (`llm.provider: anthropic` with a Claude model), set `ANTHROPIC_API_KEY` instead.

## Other coordinates the skills ask for (not config-loader fields)

- **Cluster ID** (`<8-4-4-4-12>` UUID) — passed per-run as `--cluster` to `build_dag_from_workflow.py` / `check_data_availability*` (NOT to `job_migrate_from_workflow.py`, where `--cluster` is a deprecated no-op; per-task clusters come from the workflow/manifest).
- **Databricks PAT** — only for `dbx_export` / `extract_catalog_databricks.py`, via `DATABRICKS_HOST` + `DATABRICKS_TOKEN` (dbx_export: `--dbx-token`; extract_catalog_databricks.py also accepts `--token-file`).

## Notes

- This template is shipped with the plugin.
- Your filled-in `env-coords.md` and customer YAML should be **gitignored** — they contain tenancy-specific identifiers. Secrets in the YAML must be AIDP Credential Store refs, never plaintext.
- Precedence, highest first: CLI flags > environment overrides > YAML values.
