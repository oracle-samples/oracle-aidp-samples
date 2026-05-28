# AIDP Customer Workbench Usage UI

This folder contains the customer-facing AIDP Workbench usage UI separated from the service-operator compute-cluster admin prototype.

The UI shows AIDP Workbench workspaces, compute clusters, active capacity, notebooks, workflows, and cluster-scoped special libraries. It can run from a local fixture or through a read-only local proxy that calls AIDP Workbench REST endpoints.

## What Is Included

| Path | Purpose |
|---|---|
| `app/index.html` | Customer Workbench UI shell. |
| `app/customer-workbench.css` | Customer UI styling. |
| `app/data/customer-workbench-fixture.js` | Sanitized local fixture. |
| `app/scripts/customer-workbench-api.js` | Browser adapter and normalizer for fixture/raw Workbench data. |
| `app/scripts/customer-workbench-app.js` | UI state, rendering, filters, and live refresh wiring. |
| `tools/customer-workbench-proxy.js` | Customer-only read-only proxy and static file server. |
| `tools/test-customer-workbench-api.js` | Browser adapter tests. |
| `tools/test-customer-workbench-proxy.js` | Proxy route tests. |
| `Containerfile` | Podman/Docker image for the customer UI. |
| `compose.customer.yml` | Podman Compose service definition. |

## What Is Excluded

The service-operator admin UI and its privileged incident workflows are not part of this repo. Excluded files and behavior include:

- `app/index.html` from the operator UI
- operator compute-cluster search and OCID-first incident workflow
- WorkRequests history and operation evidence views
- DevOps MCP workflow-log lookup
- dev-dot compute-cluster details enrichment
- Lumberjack navigation links
- incident handoff summaries
- operator-only tests and fixtures

## Local Fixture Mode

```bash
node tools/customer-workbench-proxy.js --port 8083
```

Open:

```text
http://localhost:8083/
```

The default mode is fixture mode. It serves the UI and uses the in-browser sanitized fixture.

## Live Workbench REST Mode

Use `service-api` mode when you have a Workbench REST endpoint and an authorization value:

```bash
AIDP_PROXY_MODE=service-api \
AIDP_WORKBENCH_API_BASE_URL="https://aidp.<region>.oci.oraclecloud.com" \
AIDP_SERVICE_API_BEARER_TOKEN="<token>" \
node tools/customer-workbench-proxy.js --port 8083
```

Then open:

```text
http://localhost:8083/?liveApi=1
```

The browser calls only the local proxy route:

```text
GET /api/aidp-customer/workbench?region={region}&aiDataPlatformId={aiDataPlatformId}
```

The proxy fans out with read-only `GET` requests to the AIDP Workbench REST API:

```text
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/notebook/api/sessions
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/jobs
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/jobRuns
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters/{clusterKey}
GET /20260430/aiDataPlatforms/{aiDataPlatformId}/workspaces/{workspaceKey}/clusters/{clusterKey}/libraries
```

## OCI CLI Raw Request Mode

Use `oci-raw-request` mode when you want the proxy to call Workbench REST through the local OCI CLI security-token session:

```bash
AIDP_PROXY_MODE=oci-raw-request \
AIDP_OCI_PROFILE=AIDP_CUSTOMER \
node tools/customer-workbench-proxy.js --port 8083
```

Open:

```text
http://localhost:8083/?liveApi=1
```

The UI will show the session status and the sign-in command when authentication is required.

## Podman

```bash
podman build -t aidp-customer-workbench:local -f Containerfile .
podman run --rm -p 8083:8083 aidp-customer-workbench:local
```

For live REST mode:

```bash
podman run --rm -p 8083:8083 \
  -e AIDP_PROXY_MODE=service-api \
  -e AIDP_WORKBENCH_API_BASE_URL="https://aidp.<region>.oci.oraclecloud.com" \
  -e AIDP_SERVICE_API_BEARER_TOKEN="<token>" \
  aidp-customer-workbench:local
```

## Tests

```bash
node tools/test-customer-workbench-api.js
node tools/test-customer-workbench-proxy.js
```
