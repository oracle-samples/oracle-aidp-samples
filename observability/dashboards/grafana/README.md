# AIDP — Grafana Dashboards (OCI Metrics)

Three ready-to-import **Grafana** dashboards for monitoring the **Oracle AI Data Platform (AIDP)**,
built on the **OCI Metrics** datasource plugin.

| File | Dashboard | Variables |
|---|---|---|
| `aidp-spark.json` | **Spark (Executor-level)** — Data Flow compute, per executor/driver | Data Source, Tenancy, Compartment, Region, AIDP Instance, Cluster, **Cluster Key** |
| `aidp-workflow-jobs.json` | **Workflow Jobs** — job run durations + status | Data Source, Tenancy, Compartment, Region, AIDP Instance, Job, **Job ID** |
| `aidp-ai-platform.json` | **AI Platform** — GenAI / agent tooling & sessions | Data Source, Tenancy, Compartment, Region, AI Compute, **Compute Key**, Agent Flow |

All three are **portable**: the datasource is referenced through a `Data Source` template variable
(`${ds}`), and there are no hardcoded tenancy/region/OCID values. See
[`../AIDP-Metrics-Reference.md`](../AIDP-Metrics-Reference.md) for what every metric means.

## Prerequisites — Grafana on OCI + plugin + policies

Set up Grafana with the **OCI Metrics datasource** plugin and the required IAM policies (instance
principal or user principal) by following:

> **https://github.com/oracle-quickstart/oci-o11y-solutions/blob/main/knowledge-content/grafana-on-oci/README.md**

That guide covers installing Grafana on an OCI compute instance, installing the
`oci-metrics-datasource` plugin, and configuring the dynamic group + policies so Grafana can read
OCI Monitoring metrics.

## Import
1. In Grafana: **Dashboards → New → Import**, upload each `*.json` (or paste its contents).
2. On import, select your configured **OCI Metrics** datasource for the `Data Source` variable.
3. Set **Tenancy / Compartment / Region**, then pick an **AIDP Instance** and a **Cluster / Job / AI Compute**.

Or via the API:

```bash
GRAFANA="http://<host>:3000"; AUTH="<user>:<pass>"
for f in aidp-spark.json aidp-workflow-jobs.json aidp-ai-platform.json; do
  curl -s -u "$AUTH" -H 'Content-Type: application/json' \
    -d "{\"dashboard\": $(cat "$f"), \"overwrite\": true}" "$GRAFANA/api/dashboards/db"
done
```

## Behavior & design notes
- **AIDP-instance dimension differs by family:** Spark → `datalakeId`, Jobs → `resourceId` (both equal
  the AiDataPlatform OCID). **GenAI metrics carry no AIDP-instance dimension**, so the AI Platform
  dashboard has **no AIDP variable** (filter by AI Compute / Agent Flow).
- **Combination isolation:** each query ANDs the selected values via `dimensionValues`
  (`datalakeId/resourceId =~ "${aidp}"` **AND** `resourceName/jobName =~ "${...}"`), so a specific
  AIDP + Cluster/Job isolates that resource within that AIDP.
- **One value per legend line (plugin limitation):** the OCI Metrics plugin (v6.5.4) **ignores
  `legendFormat`**, breaks on multi-dimension `groupBy`, and attaches **only the grouped dimension**
  as a series label (grouping by a key carries the key, not the name). Legends are therefore set with
  Grafana **`displayName` field overrides** (`${__field.name}` = the grouped value), one dimension per
  line:
  - **Spark** → `groupBy(executorId)` → `driver`, `1`, `2`, … (per executor/driver).
  - **Jobs** → `groupBy(jobName)` / `groupBy(status)`.
  - **AI** → `groupBy(computeClusterName)`.
  Combined panels (Disk Read/Write, Network Rx/Tx, Tasks, GenAI Success/Failure, tokens In/Out, …)
  prefix the metric per query via the same override — e.g. `Read · driver`, `Success · ‹compute›`.
  Because a key and a name can't both appear on one line, every dashboard ships **both a name and a key
  dropdown** — **Cluster / Cluster Key**, **Job / Job ID**, **AI Compute / Compute Key** — so you can
  filter by the key to disambiguate same-named entities. Pick an AIDP + Cluster to focus the dense
  per-executor Spark view.
- **No workspace variable:** the telemetry exposes no workspace dimension on any metric family.
- **Dropdown lists don't cascade:** the OCI Metrics plugin's `dimensions()` macro returns each
  dimension's full, un-joined value list, so picking an AIDP narrows the **data**, not the contents of
  the other variables.
