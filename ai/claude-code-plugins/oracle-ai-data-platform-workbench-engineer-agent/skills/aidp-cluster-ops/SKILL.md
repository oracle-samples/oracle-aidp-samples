---
name: aidp-cluster-ops
description: Manage AIDP Spark compute clusters — list, status, start/stop/restart, installed libraries (JARs/Python), provision/scale a new cluster (driver/worker shapes, autoscale, GPU/RAPIDS, AI Compute), and connect external BI tools (JDBC/ODBC). Use when the user asks about clusters, needs to start/stop compute, create or scale a cluster, install libraries, set up a GPU cluster, use AI Compute for agent flows, connect Tableau/Power BI/DBeaver, or pick a cluster before running data work.
---

# `aidp-cluster-ops` — cluster lifecycle & libraries

Inspect and control AIDP Spark clusters. Most data skills depend on a RUNNING cluster, so this is the
common pre-step. This is a **control-plane** skill. No MCP and no `ai-data-engineer-agent` repo are required.

## When to use
- "What clusters are there / is X running?", "start/stop/restart the cluster", "what libraries are
  installed", "check if compute is up before running data work".

## Engine — official `aidp` CLI (control-plane)
Preferred engine is the official Oracle `aidp` CLI; `oci raw-request` is the fallback when the CLI isn't
installed. Both hit the same data-plane REST API with the same auth — see
[references/aidp-cli-map.md](../../references/aidp-cli-map.md) for the full command map,
[references/oci-raw-request.md](../../references/oci-raw-request.md) for base URL + auth ladder + async/error
conventions, and [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) for REST endpoint shapes.

| Op | CLI (preferred) | REST fallback |
|---|---|---|
| List clusters | `aidp cluster list` | `GET /clusters` — or `GET /workspaces/<ws>/clusters` |
| Status / config | `aidp cluster get --cluster-key <key>` | `GET /workspaces/<ws>/clusters/<key>` |
| Default cluster | `aidp cluster get-default` | (in list output) |
| Libraries | `aidp cluster list-libraries --cluster-key <key>` (`patch-library` to add/remove) | inside cluster GET |
| Start / Stop / Restart | `aidp cluster start\|stop\|restart --cluster-key <key>` | `POST /workspaces/<ws>/clusters/<key>/actions/start\|stop\|restart` |
| Logs / metrics | `aidp cluster search-logs\|download-logs\|summarize-metrics-data --cluster-key <key>` | `…/clusters/<key>/…` |

All CLI calls take `--instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`.

```bash
# CLI (preferred): list clusters
aidp cluster list --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1

# CLI (preferred): cluster detail — state, config, connections
aidp cluster get --cluster-key <KEY> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1

# CLI (preferred): start (the CLI handles the required body for you)
aidp cluster start --cluster-key <KEY> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region us-ashburn-1
```

Mutating ops (start/stop/restart, patch-library) — for shared clusters, persist any non-trivial body to
`.aidp/payloads/` and confirm first ([references/payloads.md](../../references/payloads.md)).

**Fallback (no CLI installed) — `oci raw-request`** against
`https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`:

```bash
# List clusters (verified GET)
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/clusters" \
  --profile DEFAULT

# Cluster detail — state, config, connections, AND installed libraries
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/clusters/<KEY>" \
  --profile DEFAULT

# Start (POST action) — a JSON body is REQUIRED (use {}); empty body 400s
oci raw-request --http-method POST \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/clusters/<KEY>/actions/start" \
  --request-body '{}' --request-headers '{"content-type":"application/json"}' \
  --profile DEFAULT
```

## Patterns
- **Find compute:** `aidp cluster list` (REST `GET /clusters`) lists DataLake clusters;
  `GET /workspaces/<ws>/clusters` scopes the REST fallback to one workspace. Cross-workspace questions must
  pass the right `<ws>` — don't rely on a single default workspace.
- **Status + readiness:** `aidp cluster get --cluster-key <key>` (REST `GET /workspaces/<ws>/clusters/<key>`)
  returns `state` (e.g. `STARTING` → `ACTIVE`) + `stateDetails`, config, connections, and attached
  notebooks/sessions. Data/SQL skills should call this first and `start` if stopped, then re-check, polling
  `state` until `ACTIVE` (start takes minutes).
- **Lifecycle:** `aidp cluster start|stop|restart` (REST `…/actions/start|stop|restart`). The async 202 is
  poll-to-terminal. Confirm before stopping a shared cluster.
- **Libraries:** `aidp cluster list-libraries --cluster-key <key>`; with the REST fallback the installed
  JARs + Python libs come back **inside the cluster GET**. Check before relying on a connector/lib.

## Caveats
- **REST fallback only — `actions/start|stop|restart` need a body `{}`** (LIVE-VERIFIED 2026-06-09):
  calling with no body returns `400 InvalidParameter: The request body must not be null`; passing
  `--request-body '{}'` returns `202`. This (not workspace mismatch) was the original "start 400". The
  `aidp` CLI sets this body for you. A second `start` while already `STARTING` returns `409 Conflict`
  (expected) on either engine.
- **Use the cluster's home workspace** in the REST action URL — find it via `GET /workspaces/<ws>/clusters`
  (a cluster may not live in your default workspace). The CLI resolves the workspace from the cluster key.
- Per the no-fabrication gate in `oci-raw-request.md`: don't present an endpoint/version/prefix as confirmed
  until a live `2xx` (or documented 4xx) is recorded in `rest-endpoint-map.md`.

## Provision / scale a cluster (`aidp cluster create|update|delete`; REST `POST/PUT/DELETE …/workspaces/<ws>/clusters`)
**Live-verified create body** (this provisioned `agent_e2e_cluster` → ACTIVE, 2026-06-10):
```json
{ "type": "USER", "displayName": "etl_cluster",
  "driverConfig": { "driverShape": "amd.generic", "driverShapeConfig": { "ocpus": 2, "memoryInGBs": 16, "gpus": 0 } },
  "workerConfig": { "minWorkerCount": 1, "maxWorkerCount": 1, "workerShape": "amd.generic",
                    "workerShapeConfig": { "ocpus": 2, "memoryInGBs": 16, "gpus": 0 } },
  "clusterRuntimeConfig": { "sparkVersion": "3.5.0", "type": "SPARK", "initScripts": [] },
  "autoTerminationMinutes": 120 }
```
- **`displayName` charset:** must **start with a letter**; the only special chars allowed are **underscore
  and slash**. A hyphen (e.g. `etl-cluster`) → `400 InvalidParameter` ("no special characters … except for
  underscore, slash") — use `etl_cluster`.
- **Shapes:** AMD / ARM / Intel / NVIDIA GPU (platform-ref §12). **Quickstart** = 1 driver + ≤10 workers, AMD
  2 OCPU/32 GB, autoscale (fast start); **Custom** = full control. Installing custom libs to a Quickstart
  cluster converts it to Custom.
- **Scale:** static (`minWorkerCount == maxWorkerCount`) or **autoscale** (min < max).
- **Run duration:** always-on (omit) or **idle timeout** via `autoTerminationMinutes`.
- **Runtime:** Spark 3.5.0 / Delta 3.2.0 / Python 3.11 / Java 17 (Python + SQL user code only).

## Libraries (install) — `aidp cluster patch-library`
Formats `.jar` / `.whl` / `requirements.txt`; source = workspace / volume / uploaded file. **Must restart the
cluster after installing.** Notebook-scoped installs (`!pip install …`, `.ipynb` only) don't need a restart
but apply only to that notebook (see `aidp-notebooks`).

## GPU / RAPIDS clusters (platform-ref §14)
GPU shapes: 1 GPU = 15 OCPU/24 GB GPU mem; 2 GPU = 30 OCPU/48 GB. **Rule: both driver AND worker must be
NVIDIA GPU — no CPU/GPU mixing.** Required RAPIDS Spark configs: `spark.plugins=com.nvidia.spark.SQLPlugin`,
`spark.shuffle.manager=com.nvidia.spark.rapids.spark350.RapidsShuffleManager`,
`spark.rapids.shuffle.mode=MULTITHREADED`, `spark.executor.resource.gpu.amount=1`,
`spark.task.resource.gpu.amount=1/executor.cores`. Libraries: Spark RAPIDS, Spark RAPIDS ML (cuML).

## AI Compute (Preview) — powers agent flows
Specialized compute for **agent flows** (`aidp-agent-flows` / `aidp-agent-highcode`): 1–64 OCPU, restricted to
**PvtDefaultWorkspace**; private workspaces must connect to a private Autonomous AI Lakehouse (immutable once
linked); public workspaces can't use private ALH (AI features unavailable). Create via Workspace > Create >
AI Compute; Start/Stop frees/meters compute; attached flows show on the cluster's Agent flows tab.

## Connect external BI tools (JDBC / ODBC) — additive to OAC/FDI, not a replacement
The cluster **Connection Details** tab provides the **simbaSpark JDBC** and **ODBC** drivers for DBeaver /
Tableau / Power BI. JDBC driver class `com.simba.spark.jdbc.Driver`; use the JDBC URL from that tab. Auth:
**token-based** (no `ociProfile` in the URL → browser SSO) or **API key** (append `ociProfile=<profile_name>`).
OAC connection setup itself is OAC-side (the OAC/fusion-bundle plugin); here we only expose the AIDP driver/URL.

## Notes
- For Spark job/stage/task diagnostics on a running cluster, use `aidp-spark-debugging`.
- **Optional accelerator:** if an `aidp` MCP happens to be configured, its `list_clusters` /
  `get_cluster_status` / `get_default_cluster` / `start_cluster` / `stop_cluster` / `restart_cluster` /
  `list_cluster_libraries` tools wrap these same REST calls. The MCP is **not required** — the
  `oci raw-request` calls above are the source of truth.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) — skill → official `aidp` CLI command map (primary engine)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) — base URL, auth ladder, async/errors
- [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) — cluster endpoint map + start-400 note
- [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) — verification ledger
