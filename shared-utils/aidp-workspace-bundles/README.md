# AIDP Workbench export and import

This folder provides a customer-parameterized migration process for Oracle AI Data Platform Workbench. It uses the OCI CLI and AIDP REST APIs; no AIDP SDK dependency is required.

## Start here

1. Copy [migration.env.template](migration.env.template) to `migration.env` and fill in the source and target values.
2. Load it with `set -a; . ./migration.env; set +a`.
3. Follow [CUSTOMER_MIGRATION_GUIDE.md](CUSTOMER_MIGRATION_GUIDE.md).
4. Review [FUNCTIONALITY_REVIEW.md](FUNCTIONALITY_REVIEW.md) before scheduling a migration.

The populated `migration.env` file must remain local: it can identify customer tenancies, Workbenches, private subnets, and storage destinations.

## Utility commands

Every command needs an AIDP Workbench OCID and region, passed explicitly or through `AIDP_WORKBENCH_OCID` and `OCI_REGION`:

```bash
python3 aidp_workspace_bundle.py --aidp-id "$SOURCE_WORKBENCH_OCID" \
  --region "$SOURCE_REGION" --profile "$OCI_CLI_PROFILE" list
```

| Command | Purpose | Writes to AIDP? |
| --- | --- | --- |
| `list` | Inventory workspaces and catalogs | No |
| `archive` | Save metadata and optional workspace files locally | No |
| `export` | Create a Bundle from jobs/agent flows | Yes, source bundle folder |
| `sync` | Refresh a source Bundle | Yes, source bundle folder |
| `import --confirm` | Deploy a Bundle to a target workspace | Yes, target jobs/agent flows |

Examples:

```bash
# Audit/recovery archive
python3 aidp_workspace_bundle.py "${SOURCE[@]}" archive --output-dir "$AIDP_EXPORT_DIR"

# Bundle all jobs in an approved source workspace
python3 aidp_workspace_bundle.py "${SOURCE[@]}" export \
  --workspace-key SOURCE_WORKSPACE_KEY --all-jobs \
  --name migration_YYYYMMDD --path /Workspace/aidp_bundles

# Deploy only after copying the Bundle folder to the target workspace volume
python3 aidp_workspace_bundle.py "${TARGET[@]}" import \
  --workspace-key TARGET_WORKSPACE_KEY \
  --path /Workspace/aidp_bundles/migration_YYYYMMDD --confirm
```

## Artifact coverage

Bundles deploy selected jobs/workflows and agent flows plus their referenced source code. The read-only archive also records workspace metadata, permissions, clusters, libraries, jobs, catalogs, and workspace files.

Neither mechanism exports credential secrets, active sessions, job-run history, running compute state, or underlying catalog data. Recreate those per target environment; see the migration guide for the required sequence and private-network prerequisites. Bundle is a preview API; validate deployments in a non-production target first.

`archive` requests short-lived download PARs for individual workspace files. Treat the archive run and its process output as sensitive, do not log PAR URLs, and verify the service's PAR expiry policy for your environment. Use `--endpoint` (or `AIDP_ENDPOINT`) for a non-commercial OCI realm; otherwise the utility defaults to the commercial `oraclecloud.com` endpoint.

Examples use Bash. Windows users should run them under WSL or Git Bash.
