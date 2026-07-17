# Customer AIDP Workbench migration process

Use this process to migrate source code and selected AIDP resources between Oracle AI Data Platform Workbenches. It is designed for a customer to parameterize per environment without placing tenancy identifiers, passwords, or tokens in source control.

## 1. Configure the environments

Copy [migration.env.template](migration.env.template) to `migration.env`, complete the values, and keep the populated file out of Git:

```bash
cp migration.env.template migration.env
set -a; . ./migration.env; set +a
```

For each command, set the API context explicitly. This avoids accidentally exporting from or importing to the wrong Workbench:

```bash
SOURCE=(--tenancy "$SOURCE_TENANCY_OCID" --aidp-id "$SOURCE_WORKBENCH_OCID" --region "$SOURCE_REGION" --profile "$OCI_CLI_PROFILE" --auth "$OCI_CLI_AUTH")
TARGET=(--tenancy "$TARGET_TENANCY_OCID" --aidp-id "$TARGET_WORKBENCH_OCID" --region "$TARGET_REGION" --profile "$OCI_CLI_PROFILE" --auth "$OCI_CLI_AUTH")
```

## 2. Preflight

Validate the security-token session and list both environments before writing anything:

```bash
oci session validate --profile "$OCI_CLI_PROFILE" --auth "$OCI_CLI_AUTH"
python3 aidp_workspace_bundle.py "${SOURCE[@]}" list
python3 aidp_workspace_bundle.py "${TARGET[@]}" list
```

Record the source-to-target workspace mapping in the change record. Workspace *keys* are unique to an individual Workbench; map by approved display name, then use the target key returned by `list`.

## 3. Export in two layers

Create a read-only archive for audit, recovery, and code review:

```bash
python3 aidp_workspace_bundle.py "${SOURCE[@]}" archive --output-dir "$AIDP_EXPORT_DIR"
```

Then create one Bundle per source workspace for every workflow/job that must be deployed:

```bash
python3 aidp_workspace_bundle.py "${SOURCE[@]}" export \
  --workspace-key SOURCE_WORKSPACE_KEY --all-jobs \
  --name migration_YYYYMMDD --path /Workspace/aidp_bundles
```

For agent flows, pass each item explicitly as `--resource AGENTFLOW:<key>`. The Bundle API supports only `JOB` and `AGENTFLOW` roots; it includes their referenced source code.

## 4. Prepare the target

Create target workspaces first. A workspace can be created publicly with `displayName` and `defaultCatalogKey`; private workspaces additionally require a target subnet and, where used, NSG/SCAN details. Never reuse source-region subnet, NSG, database, catalog, or credential OCIDs.

Before private workspaces are created, verify:

- the target subnet is in the intended target VCN and is available;
- VCN security rules permit VCN-internal ingress to the required destination ports;
- the AIDP service can manage VNICs and use subnets/NSGs in the target subnet compartment;
- required DNS/SCAN, routing, peering, and service-gateway paths are configured.

The target must recreate external catalog connection details and credentials using its own vault secrets. Do not export passwords, API keys, wallets, security tokens, or PAR URLs.

## 5. Transfer and deploy the bundle

Copy the completed bundle folder from the source workspace volume to the same or another approved folder in the target workspace volume. Review `aidp_workbench.yaml` and environment-specific variables before deployment.

Then deploy it only after confirming the target workspace and bundle path:

```bash
python3 aidp_workspace_bundle.py "${TARGET[@]}" import \
  --workspace-key TARGET_WORKSPACE_KEY \
  --path /Workspace/aidp_bundles/migration_YYYYMMDD \
  --confirm
```

Use a non-production target first. The Bundle deploy creates or updates jobs and agent flows.

## Coverage and validation

| Artifact | Archive | Bundle deploy | Migration action |
| --- | --- | --- | --- |
| Workspace metadata and permissions | Yes | No | Recreate target workspace and review permissions. |
| Notebooks, scripts, workspace files | Yes | Referenced code only | Copy files; validate paths and secrets. |
| Jobs/workflows and agent flows | Yes | Yes | Deploy Bundle after target prerequisites exist. |
| Compute settings and libraries | Yes | No | Recreate and test against target quotas/network. |
| External catalog definitions | Metadata only | No | Recreate with target credentials and network. |
| Catalog credentials, vault secrets, PARs | No | No | Recreate from target secret management. |
| Underlying data, session state, job-run history | No | No | Migrate independently using the relevant data service. |

After deployment, validate lifecycle state, catalog connectivity, workspace files, compute startup, and a representative job run. Preserve the archive, bundle manifest, workspace mapping, API request IDs, and validation evidence with the change record.
