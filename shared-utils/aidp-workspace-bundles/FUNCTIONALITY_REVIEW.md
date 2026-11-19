# AIDP Workbench API migration capability review

## Purpose and conclusion

The AIDP Workbench APIs support a **controlled, layered migration**, not a single all-inclusive backup-and-restore operation. Teams can use the APIs to inventory an environment, archive selected metadata and workspace files, create target workspaces, and deploy selected workflows/jobs and agent flows through Bundles.

The migration remains partially manual by design: target credentials, private-network configuration, compute capacity, underlying data, and runtime state are environment-specific and must be created or validated in the destination.

## Supported API capabilities

| Capability | Workbench API support | Migration use |
| --- | --- | --- |
| Inventory workspaces | `GET /workspaces` | Build and validate source-to-target workspace mapping. |
| Create workspaces | `POST /workspaces` | Create public or private target workspaces. Private targets require destination network values. |
| Inventory catalogs | `GET /catalogs` | Record catalog metadata and identify target dependencies. |
| Inventory compute | `GET /workspaces/{workspaceKey}/clusters` | Archive configuration and installed library metadata. |
| Inventory workflows | `GET /workspaces/{workspaceKey}/jobs` | Archive job definitions and identify Bundle roots. |
| Archive files/notebooks | Workspace-object list and download-PAR APIs | Retain source code and notebooks for review or transfer. |
| Bundle workflows/agent flows | `POST /workspaces/{workspaceKey}/bundles` | Produce a portable deployment descriptor and referenced code structure. |
| Deploy a Bundle | `POST /workspaces/{workspaceKey}/bundles/actions/deploy` | Create or update Bundle-defined jobs and agent flows in the target. |

The Bundle APIs are preview APIs and currently accept `JOB` and `AGENTFLOW` resources as Bundle roots. See the [Bundle API](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/op-aidataplatforms-aidataplatformid-workspaces-workspacekey-bundles-post.html) and [Bundle endpoint list](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aiwap/api-bundle.html).

## Artifact limitations

| Artifact | Can be archived? | Can be deployed automatically? | Limitation / required action |
| --- | --- | --- | --- |
| Workspace | Metadata and permissions | No | Create a new target workspace; workspace keys are not portable. |
| Public workspace networking | Metadata only | No | Recreate with the target default catalog and policies. |
| Private workspace networking | Metadata only | No | Map to target subnet, NSGs, DNS/SCAN, routing, security rules, and IAM service policies. Source OCIDs cannot be reused cross-region or cross-tenancy. |
| Notebooks, scripts, folders, and workspace files | Yes | Only code referenced by a Bundle | Transfer remaining files to the target workspace volume; review for secrets and environment paths. |
| Jobs/workflows | Yes | Yes, through Bundle | Validate target compute, catalog, secret, and path references before deployment. |
| Agent flows | Selected definitions | Yes, through Bundle | Bundle must include each required `AGENTFLOW` root; validate target AI and knowledge-base dependencies. |
| Compute clusters and libraries | Configuration metadata | No | Recreate capacity, shape, Spark settings, and libraries; runtime state is not portable. |
| External catalog metadata | Metadata only | No | Recreate the target catalog and validate target network access. |
| Catalog credentials, passwords, wallets, PATs, API keys, PARs | No | No | Recreate from destination Vault or other approved secret management. Never archive secrets. |
| Tables, object-storage data, and volume data | No | No | Migrate with the owning data service and validate data separately. |
| Notebook sessions, job runs, task runs, logs, and active compute state | No | No | Treat as operational history, not a portable environment artifact. |
| Workbench instance provisioning | No public Workbench REST create operation | No | Provision the destination Workbench through OCI Console/approved provisioning process first. |

## Required process

```text
Preflight → Archive → Bundle → Prepare target → Transfer bundle/files → Deploy → Validate
```

1. **Preflight**: authenticate, inventory both Workbenches, confirm regions/tenancies, record workspace mapping, and verify target quotas and IAM.
2. **Archive**: run the read-only archive for audit evidence and non-Bundle workspace files.
3. **Bundle**: create one Bundle per approved source workspace, including jobs and required agent flows.
4. **Prepare target**: create workspaces, recreate catalogs and compute, configure private networking, and provision target secrets.
5. **Transfer**: copy Bundle folders and non-Bundle workspace files to the target workspace volume. Do not transfer source credentials.
6. **Deploy**: deploy the Bundle to a non-production target workspace first; then promote using the same reviewed artifact.
7. **Validate**: confirm resource lifecycle states, catalogs, network connectivity, compute startup, file paths, and representative workflow runs.

## Private-network gate

Before attempting a private workspace, the target must have:

- an available subnet in the target VCN;
- VCN security rules that permit required VCN-internal traffic;
- IAM policies allowing the AIDP service principal to manage VNICs and use subnets and NSGs in the relevant compartment;
- required DNS, SCAN, routing, peering, and service-gateway connectivity.

NSG and SCAN fields are optional in the API request, but their absence does not remove the network and IAM prerequisites. Oracle documents the private-workspace prerequisites in [Workspaces](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/workspaces.html) and the service permissions in [AIDP IAM policies](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html).

## Migration completion criteria

The migration is ready for production promotion only when the following are recorded:

- approved source-to-target workspace and catalog mapping;
- reviewed Bundle manifest and target variable overrides;
- target secret and private-network validation evidence;
- successful deployment status and representative job/agent-flow test results;
- archive location, checksum/retention policy, API request IDs, and rollback plan.

The detailed operational commands are in [CUSTOMER_MIGRATION_GUIDE.md](CUSTOMER_MIGRATION_GUIDE.md).
