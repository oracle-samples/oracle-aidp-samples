---
description: Pull data from Oracle Fusion ERP / HCM / SCM REST APIs into a Spark DataFrame from an AIDP notebook. Use when the user mentions Fusion ERP, Fusion REST API, FA REST, Cloud ERP, or wants live data from a Fusion pod. HTTP Basic auth only. For volumes >499 rows/page or bulk extracts, route to aidp-fusion-bicc.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-fusion-rest` — Fusion ERP / HCM / SCM REST → Spark

## When to use
- User wants to pull a small-to-medium volume of records from Fusion REST APIs (`/fscmRestApi/`, `/hcmRestApi/`, etc.) into a Spark DataFrame.
- User mentions: "Fusion ERP", "Fusion REST", "FA REST", "Cloud ERP API".
- Total expected rows fit comfortably in memory (≤ ~50k); for >499 rows the helper auto-pages, but for bulk → BICC is faster.

## When NOT to use
- For **bulk extracts** (>50k rows, daily snapshots) → use [`aidp-fusion-bicc`](../aidp-fusion-bicc/SKILL.md). Fusion's REST surface is hard-capped at 499 rows/page (MOS Doc ID 2429019.1) — pulling millions paginated is slow.
- For EPM Cloud Planning → use [`aidp-epm-cloud`](../aidp-epm-cloud/SKILL.md).
- For Essbase MDX → use [`aidp-essbase`](../aidp-essbase/SKILL.md).

## Prerequisites in the AIDP notebook
1. `pip install requests pandas` (usually already on the cluster).
2. Helpers on `sys.path`.
3. Fusion pod URL + HTTP Basic credentials.

## Auth: HTTP Basic

```python
import os
from oracle_ai_data_platform_connectors.auth import http_basic_session
from oracle_ai_data_platform_connectors.rest.fusion import (
    fetch_paged, rows_to_spark_dataframe,
)

session = http_basic_session(
    username=os.environ["FUSION_USER"],
    password=os.environ["FUSION_PASSWORD"],
    base_url=os.environ["FUSION_BASE_URL"],
)

rows = fetch_paged(
    session=session,
    base_url=os.environ["FUSION_BASE_URL"],
    path="/fscmRestApi/resources/11.13.18.05/invoices",
    fields="InvoiceId,InvoiceNumber,InvoiceAmount,InvoiceDate",
    extra_params={"q": "InvoiceDate >= '2026-01-01'"},
)

df = rows_to_spark_dataframe(spark, rows)
df.show(5)
print("rows:", df.count())
```

## Gotchas
- **499 row/page hard cap** — Fusion silently truncates `limit=500+` to 499. Helper enforces this automatically.
- **`onlyData=true`** — helper sets this so only the actual fields come back, not Fusion's HATEOAS link envelope. Saves bandwidth.
- **`q=` filter syntax** is Fusion-specific (`q=InvoiceDate >= '2026-01-01' AND Status = 'PAID'`). Quote string values in single quotes.
- **Nested struct columns** — Fusion responses contain nested objects (links, addresses). `rows_to_spark_dataframe()` defaults to `mode="json_string"` which packs each row into a single `row_json` column. Use `from_json` downstream to project specific fields.
- **Network** — Fusion pods are public (`*.fa.<region>.oraclecloud.com`); no AIDP VCN routing needed.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/rest/fusion.py](../../scripts/oracle_ai_data_platform_connectors/rest/fusion.py)
- Auth helpers: [scripts/oracle_ai_data_platform_connectors/auth/user_principal.py](../../scripts/oracle_ai_data_platform_connectors/auth/user_principal.py)
- Fusion REST API catalog: https://docs.oracle.com/en/cloud/saas/applications-common/24a/farws/index.html
