# AIDP Environment Coordinates Template

Copy this file to `env-coords.md` (gitignored) and fill in your environment details before running the migrator.

## OCI Tenancy
```bash
# OCI region code (e.g. us-ashburn-1, ap-mumbai-1)
export AIDP_REGION=<your-region>

# Your OCI tenancy OCID
export OCI_TENANCY_OCID=<tenancy-ocid>

# OCI auth profile name in ~/.oci/config (default: DEFAULT)
export OCI_PROFILE=DEFAULT
```

## AIDP Resources
```bash
# AIDP datalake OCID
export AIDP_DATALAKE_OCID=<datalake-ocid>

# AIDP workspace UUID
export AIDP_WORKSPACE_ID=<workspace-uuid>

# Active AIDP cluster UUID
export AIDP_CLUSTER_ID=<cluster-uuid>

# AIDP REST API base URL
export AIDP_BASE="https://aidp.${AIDP_REGION}.oci.oraclecloud.com/20240831"
```

## Anthropic API
```bash
# Anthropic API key for model-driven cell rewriting
export ANTHROPIC_API_KEY=sk-ant-...
```

## Notes
- This template is shipped with the plugin.
- Your filled-in `env-coords.md` should be **gitignored** — it contains credentials.
- The migrator scripts read these env vars as defaults; CLI args (`--lake-ocid`, `--workspace-id`, `--cluster`) always override.
