# AIDP — Native OCI Management Dashboards

Three ready-to-import **OCI Management Dashboard** definitions for monitoring the
**Oracle AI Data Platform (AIDP)** directly from OCI Monitoring metrics — no Grafana required.

| File | Dashboard | Dropdowns |
|---|---|---|
| `aidp-spark.json` | **Spark (executor-level)** — Data Flow compute, per executor/driver | Compartment, Region, AIDP, Cluster |
| `aidp-workflow-jobs.json` | **Workflow Jobs** — job run durations + status | Compartment, Region, AIDP, Job |
| `aidp-ai-platform.json` | **AI Platform** — GenAI / agent tooling & sessions | Compartment, Region, AI Compute |

All three are **portable**: no tenancy/region/OCID is hardcoded — the compartment is a
`__COMPARTMENT_OCID__` placeholder overridden at import time. See
[`../AIDP-Metrics-Reference.md`](../AIDP-Metrics-Reference.md) for what every metric means.

## Prerequisites
- OCI CLI configured; IAM permission to manage management dashboards and read monitoring metrics.
- The target compartment must contain AIDP telemetry (namespaces `oracle_aidataplatform`, and
  `oracle_datalake` for older instances).

## Required IAM permissions

Grant your group these policies (Console → **Identity & Security → Policies**):

```
Allow group <your-group> to manage management-dashboard-family in compartment <your-compartment>
Allow group <your-group> to read metrics in compartment <your-compartment>
```

- `management-dashboard-family` is the aggregate covering `management-dashboard` (the dashboards) **and**
  `management-saved-search` (the widgets/filters) — both are imported, so you need both. To only **view**
  existing dashboards (not import/edit), use `read` instead of `manage`.
- `read metrics` (OCI Monitoring) is required to render the metric data the widgets query and to populate
  the dropdowns.

## Import — Option A: OCI Console (no CLI needed)

The Console imports the JSON directly:

1. Console → **Observability & Management → Dashboards**.
2. Click **Import dashboards** and choose a file (e.g. `aidp-spark.json`).
3. In the dialog, **select your target compartment** from the drop-down — this overrides the
   `__COMPARTMENT_OCID__` placeholder in the file. (Do **not** pick "use the compartment from the file" —
   the file ships a placeholder, not a real OCID.)
4. Import — the widgets and filters are imported with the dashboard. Repeat for the other two files.

## Import — Option B: OCI CLI

```bash
COMP="<your-compartment-ocid>"
for f in aidp-spark.json aidp-workflow-jobs.json aidp-ai-platform.json; do
  oci management-dashboard dashboard import \
    --dashboards "file://$f" \
    --override-dashboard-compartment-ocid "$COMP" \
    --override-saved-search-compartment-ocid "$COMP"
done
```

After import, open **Observability & Management → Dashboards** (filter to your compartment), set
**Compartment** + **Time**, then pick an **AIDP Instance** and a **Cluster / Job / AI Compute**.

## Behavior & design notes
- **AIDP-instance dimension differs by family:** Spark → `datalakeId`, Jobs → `resourceId` (both equal
  the AiDataPlatform OCID). **GenAI metrics carry no AIDP-instance dimension**, so the AI Platform
  dashboard has **no AIDP dropdown** and is filtered by AI Compute.
- **Combination isolation:** a specific **AIDP + Cluster** (or **AIDP + Job**) isolates that resource
  *within that AIDP* — separating clusters/jobs that share a name across AIDPs.
- **Spark = per executor/driver:** grouped by `(datalakeId, resourceName, executorId)` (`executorId =
  driver, 1, 2…`). With both filters on *All*, the view is dense — pick an AIDP + Cluster to focus.
- **AI Compute uniqueness:** AI panels group by `computeClusterId` so AI Computes that share a name stay
  distinct lines (the engine has no AIDP dimension to label them by).
- **Distinct dropdown values:** dimension dropdowns set `preventDefaultTransform: true` so each value
  appears **once** (a cluster name shared across many AIDPs isn't repeated per metric stream).
- **No workspace dropdown:** the telemetry exposes no workspace dimension on any metric family.
- **Dropdown lists don't cascade:** the dimension dropdown only sends `{namespace, metricName}` to
  ListMetrics (any `dimensionFilters` is stripped), so picking an AIDP narrows the **data**, not the
  contents of the other dropdowns.
- OCI MD chart legends key on a **single** dimension (e.g. `executorId`); AIDP/cluster context comes
  from the filter selection. (The Grafana version is the same — its plugin also keys the legend on a
  single dimension's value.)

## Implementation notes
The per-panel filter is a **top-level `{{ }}`** 4-case nested-ternary (each branch a literal MQL
clause); namespace union (Spark, `oracle_aidataplatform` + `oracle_datalake`) uses `transformUnion`.
Deleting a dashboard does **not** delete its saved searches — clean up orphans separately if you
re-import repeatedly.
