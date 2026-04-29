---
description: Pull data from any REST API into a Spark DataFrame using the AIDP `aidataplatform` Generic REST connector. Use when the user has a non-Fusion / non-EPM / non-Essbase REST endpoint with a `manifest.url` describing the schema. Auth is HTTP Basic with derived properties driving query parameters.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-rest-generic` — Generic REST via AIDP `aidataplatform` (`type=GENERIC_REST`)

Read from arbitrary REST APIs as a Spark DataFrame. The connector requires a server-published **manifest** (a small JSON describing each API endpoint, parameters, and response schema) so it knows how to parse responses without a custom integration.

## When to use
- Any REST endpoint that exposes a manifest URL (custom enterprise APIs commonly do).
- Mentioned: "Generic REST", "manifest URL", "REST connector".

## When NOT to use
- For **Fusion ERP/HCM/SCM** REST → [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md). Different shape (no manifest; ≤499/page paging).
- For **Fusion BICC** bulk extracts → [`aidp-fusion-bicc`](../aidp-fusion-bicc/SKILL.md).
- For **EPM Cloud Planning** → [`aidp-epm-cloud`](../aidp-epm-cloud/SKILL.md).
- For **Essbase** → [`aidp-essbase`](../aidp-essbase/SKILL.md).

## Read

```python
import os
from oracle_ai_data_platform_connectors.aidataplatform import (
    AIDP_FORMAT, aidataplatform_options,
)

opts = aidataplatform_options(
    type="GENERIC_REST",
    user=os.environ["REST_USER"],
    password=os.environ["REST_PASSWORD"],
    schema=os.environ.get("REST_SCHEMA", "default"),
    extra={
        "base.url":     os.environ["REST_BASE_URL"],   # e.g. http://api.internal/v1
        "manifest.url": os.environ["REST_MANIFEST_URL"], # e.g. http://api.internal/v1/manifest
        "auth.type":    "basic",
        "api":          os.environ["REST_API"],         # e.g. "getOrdersByOrderID"
        # Any number of derived.property.<name> values feed into the API call:
        "derived.property.orderNo": os.environ.get("REST_ORDER_NO", "12345"),
    },
)
df = spark.read.format(AIDP_FORMAT).options(**opts).load()
df.show(5)
```

## Manifest contract

The manifest describes:
- `apis` — the named API operations (e.g. `getOrdersByOrderID`)
- `parameters` — what the connector should send (path/query/body)
- `responseSchema` — the Spark schema the connector should infer

If you don't have a manifest URL, this connector won't work — fall back to the requests-based pattern in [`aidp-fusion-rest`](../aidp-fusion-rest/SKILL.md) and adapt for your API.

## Multiple derived properties

Pass each as a separate `extra={}` key:

```python
extra={
    "base.url":     "...",
    "manifest.url": "...",
    "auth.type":    "basic",
    "api":          "searchOrders",
    "derived.property.fromDate":  "2025-01-01",
    "derived.property.toDate":    "2025-12-31",
    "derived.property.status":    "OPEN",
}
```

## Gotchas
- **`auth.type=basic` only.** If the API uses OAuth / API key headers / mTLS, this connector won't help — use the Python `requests` path.
- **Manifest must be reachable from the AIDP cluster's VCN.** Egress restrictions apply.
- **Schema `schema` option is the AIDP/Spark logical schema** for the resulting DataFrame, not a server-side one. Use `default` if unsure.
- **Paging** is handled by the connector based on the manifest. If the manifest declares `maxPageSize`, the connector batches automatically.

## References
- Helper: [scripts/oracle_ai_data_platform_connectors/aidataplatform.py](../../scripts/oracle_ai_data_platform_connectors/aidataplatform.py)
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
