# Environment Coordinates Scaffold

Every skill in this plugin threads the same set of customer-specific identifiers through to the migrator CLI. **Copy this template** to your project at `env-coords.md` (or any name the user prefers — must be gitignored), fill it in once, and reference whenever a skill asks for a coordinate.

> **Do NOT commit a filled-in copy of this file.** It contains tenancy-specific identifiers (OCIDs, workspace UUIDs, cluster IDs). Add it to `.gitignore`.

---

## Required coordinates

```yaml
# ──────────────────────────────────────────────────────────────────────
# AIDP target — the destination
# ──────────────────────────────────────────────────────────────────────
aidp_region: "<your-region>"                            # e.g. us-ashburn-1
aidp_base_url: "https://aidp.<your-region>.oci.oraclecloud.com/20240831"

datalake_ocid: "<your-datalake-ocid>"                   # ocid1.aidataplatform.oc1.<region>.<id>
workspace_uuid: "<your-workspace-uuid>"                 # 8-4-4-4-12 UUID
cluster_id: "<your-cluster-uuid>"                       # 8-4-4-4-12 UUID

# Where migrated .ipynb files land (workspace path, NOT local path).
# Convention: Shared/aidp-migration-tool-output/  (or your team's prefix)
output_base_path: "<your-output-workspace-path>"

# OCI authentication profile name (from ~/.oci/config)
oci_profile: "<your-profile-name>"

# ──────────────────────────────────────────────────────────────────────
# Databricks source — what we're migrating from
# ──────────────────────────────────────────────────────────────────────
databricks_host: "https://<your-workspace>.cloud.databricks.com"
databricks_token_env: "DATABRICKS_TOKEN"                # set via env, NOT in this file

# Optional — only if migrating from a Databricks Job (workflow shape)
databricks_job_id: <numeric_job_id_or_omit>

# ──────────────────────────────────────────────────────────────────────
# Bucket mapping — needed if any source tables/paths are s3://
# ──────────────────────────────────────────────────────────────────────
bucket_mapping_path: "<path-to-bucket-mapping.json>"    # absolute path to your local file

# ──────────────────────────────────────────────────────────────────────
# Catalog manifest — needed if source code has hardcoded <source-catalog>.<schema>
# literals (e.g. SCHEMA = "<source-cat>.<schema>") that should be
# deterministically rewritten to the default catalog
# ──────────────────────────────────────────────────────────────────────
catalog_manifest_path: "<path-to-catalog-manifest.json>"   # optional

# ──────────────────────────────────────────────────────────────────────
# Anthropic — for Pass-2 cell-by-cell migration
# ──────────────────────────────────────────────────────────────────────
anthropic_api_key_env: "ANTHROPIC_API_KEY"              # set via env, NOT in this file
```

---

## Filling it in — quick reference

| Coordinate | How to find it |
|---|---|
| `aidp_region` | OCI region your DataLake is in. `oci iam region list` shows valid values. |
| `aidp_base_url` | Always `https://aidp.<region>.oci.oraclecloud.com/20240831`. |
| `datalake_ocid` | AIDP console → DataLakes → your DataLake → "Show OCID". |
| `workspace_uuid` | AIDP console → DataLakes → your DataLake → Workspaces → your workspace → URL contains the UUID. |
| `cluster_id` | AIDP console → Workspace → Clusters → your cluster → URL contains the UUID. Verify state is `Active`. |
| `output_base_path` | A folder in the workspace where migrated `.ipynb`s land. Convention: `Shared/aidp-migration-tool-output/`. Folder must already exist (create via AIDP console). |
| `oci_profile` | The section name in `~/.oci/config` for the principal you want to run as. |
| `databricks_host` | The Databricks workspace URL (without `/api/...` path). |
| `databricks_token_env` | A Databricks PAT — generate via Databricks User Settings → Access Tokens. Set as env var, do NOT paste here. |
| `databricks_job_id` | (Optional) Job ID from Databricks Jobs UI → URL contains the integer. |
| `bucket_mapping_path` | See [`aidp-bucket-mapping`](../skills/aidp-bucket-mapping/SKILL.md). |
| `catalog_manifest_path` | A JSON listing `<source-catalog>.<schema>` literals to remap; see [`aidp-migrate-job`](../skills/aidp-migrate-job/SKILL.md) §`--catalog-manifest`. |

---

## Threading into CLI invocations

The skills generate commands like this — every coordinate above maps to a `--flag`:

```bash
python3 scripts/job_migrate.py \
  --manifest reports/<job>_manifest.json \
  --cluster $cluster_id \
  --aidp-base $aidp_base_url \
  --datalake-ocid $datalake_ocid \
  --workspace-id $workspace_uuid \
  --output-base $output_base_path \
  --oci-profile $oci_profile \
  --bucket-mapping $bucket_mapping_path \
  --catalog-manifest $catalog_manifest_path
```

When a skill in this plugin asks for a coordinate by name (e.g. "the cluster ID"), reach for this file rather than asking the user to type the value each time.

---

## Sanity-check the file

After filling in:

```bash
# Verify the cluster is Active
python3 -c "
import oci, requests, yaml
with open('env-coords.md') as f: pass  # this is markdown but you can parse YAML out of the code block manually
"
```

Or simpler — invoke [`aidp-migrator-bootstrap`](../skills/aidp-migrator-bootstrap/SKILL.md) which runs the full readiness check against the filled-in values.

---

## Security notes

- **Never commit this file with real values filled in.** Always gitignore.
- **Tokens (`DATABRICKS_TOKEN`, `ANTHROPIC_API_KEY`)** belong in the shell env, not in this file.
- **`bucket_mapping_path`** points at a file that itself contains tenancy-specific identifiers — also gitignore.
- **OCIDs are not secrets** but they ARE tenancy-identifying. Treat similarly to internal hostnames.
