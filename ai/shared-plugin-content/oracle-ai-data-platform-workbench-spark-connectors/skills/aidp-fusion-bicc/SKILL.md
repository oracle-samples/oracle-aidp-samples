---
description: Pull a Fusion BICC bulk extract into a Spark DataFrame from an AIDP notebook. Use when the user mentions BICC, Fusion bulk extract, BI Cloud Connector, PVO, or needs >50k rows from Fusion. The recommended path uses AIDP's built-in `spark.read.format("aidataplatform")` connector (matches the official Oracle AIDP sample). HTTP Basic auth.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-fusion-bicc` — Fusion BICC bulk extract → Spark

Mirrors the official Oracle AIDP sample at [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb), which routes BICC through AIDP's built-in `aidataplatform` format handler.

## When to use
- User wants a **bulk** extract from Fusion (millions of rows, daily snapshots, full-table loads).
- User mentions: "BICC", "Fusion bulk extract", "BI Cloud Connector", "PVO extract".
- Live REST paging would be too slow (>50k rows or daily refresh).

## When NOT to use
- For small/live REST queries → use [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md).

## Prerequisites in the AIDP notebook
1. **AIDP-side prep (one-time, by an administrator):** an `EXTERNAL STORAGE` profile registered in the AIDP catalog pointing at the OCI Object Storage bucket BICC writes to. The user references it by name via `fusion.external.storage` — they don't supply OCI credentials in the notebook.
2. **Fusion-side requirement:** the Fusion user must have a BICC-administrator role (e.g. `BIA_ADMINISTRATOR_DUTY`). A regular Fusion REST user (Finance Manager persona) gets a 302 to IDCS instead of BICC JSON — confirmed live.
3. The PVO (Public View Object) name and its source schema (e.g. `ERP`).
4. Helpers on `sys.path`.

## Auth: HTTP Basic (Recommended path = AIDP `aidataplatform` format)

### Option A — AIDP built-in connector (recommended; matches official sample)

```python
import os
from oracle_ai_data_platform_connectors.rest.fusion import read_bicc_via_aidp_format

df = read_bicc_via_aidp_format(
    spark=spark,
    fusion_service_url=os.environ["FUSION_BICC_BASE_URL"],
    username=os.environ["FUSION_BICC_USER"],            # MUST have BICC privileges
    password=os.environ["FUSION_BICC_PASSWORD"],
    schema=os.environ["FUSION_BICC_SCHEMA"],            # e.g. "ERP"
    datastore=os.environ["FUSION_BICC_PVO"],            # the PVO name
    fusion_external_storage=os.environ["FUSION_BICC_EXTERNAL_STORAGE"],
)
df.show(5)
print("rows:", df.count())
```

The AIDP format handler does the BICC trigger, polling, manifest read, and OCI Object Storage CSV materialization internally — the user just gets a Spark DataFrame.

Equivalent verbatim invocation (same as the official Oracle sample):

```python
df = (
    spark.read.format("aidataplatform")
        .option("type", "FUSION_BICC")
        .option("fusion.service.url", os.environ["FUSION_BICC_BASE_URL"])
        .option("user.name", os.environ["FUSION_BICC_USER"])
        .option("password", os.environ["FUSION_BICC_PASSWORD"])
        .option("schema", os.environ["FUSION_BICC_SCHEMA"])
        .option("fusion.external.storage", os.environ["FUSION_BICC_EXTERNAL_STORAGE"])
        .option("datastore", os.environ["FUSION_BICC_PVO"])
        .load()
)
```

### Option B — Custom REST trigger + manual Object Storage read (fallback)

Only use this when the AIDP `aidataplatform` connector isn't available on the cluster, when you need a custom polling cadence, or when you want to inspect the `MANIFEST.MF` directly. NOT validated against the official sample — endpoint paths and response schema are best-effort and may need adjustment per Fusion version.

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.fusion import (
    trigger_bicc_extract, read_bicc_csv_from_object_storage,
)

session = http_basic_session(
    username=os.environ["FUSION_BICC_USER"],
    password=os.environ["FUSION_BICC_PASSWORD"],
    base_url=os.environ["FUSION_BICC_BASE_URL"],
)
prefix = trigger_bicc_extract(
    session=session,
    base_url=os.environ["FUSION_BICC_BASE_URL"],
    offering=os.environ["FUSION_BICC_OFFERING"],
    poll_interval_seconds=30,
    timeout_seconds=3600,
)
df = read_bicc_csv_from_object_storage(
    spark=spark,
    namespace=os.environ["OCI_NAMESPACE"],
    bucket=os.environ["OCI_BUCKET_BICC"],
    prefix=prefix,
)
print("rows:", df.count())
```

## Gotchas
- **BICC privileges** — Fusion user must hold a BICC-admin role. Without it, `/biacm/api/v[12]/*` endpoints 302-redirect to IDCS OAuth (HTTP Basic isn't honored). This is the #1 reason live tests fail. (Live-confirmed against the demo pod with `Casey.Brown` finance-mgr persona — every BICC endpoint redirected.)
- **`fusion.external.storage` is a catalog-managed name**, not a URL. Set it up once via AIDP Catalog UI (or `oci aidataplatform` CLI), then reference by name. The user never types OCI namespace/bucket in the notebook for Option A.
- **`schema` and `datastore`** — these are BICC concepts: `schema` = offering schema (`ERP`, `HCM`, etc.); `datastore` = PVO name (e.g. `FscmTopModelAM.AnalyticsServiceAM`). Get them from the BICC console under "Configure Cloud Extract".
- **First extract is slow** — BICC builds a full snapshot. Subsequent runs are incremental. Plan for >5 min on the first call.
- **Schema inference is slow on big CSVs** (Option B path). Option A's connector knows the schema in advance from the BICC metadata.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/fusion.py](../../scripts/oracle_ai_data_platform_connectors/rest/fusion.py)
- Official Oracle AIDP sample: [Read_Only_Ingestion_Connectors.ipynb](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- BICC docs: https://docs.oracle.com/en/cloud/saas/applications-common/24a/oafsm/
