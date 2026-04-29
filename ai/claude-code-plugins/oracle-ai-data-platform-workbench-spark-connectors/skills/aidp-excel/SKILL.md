---
description: Read Excel (.xlsx, .xls) files into a Spark DataFrame from an AIDP notebook. Use when the user mentions Excel, .xlsx, .xls, or has spreadsheet files in a Volume / Object Storage bucket. Two paths — the `com.crealytics.spark.excel` Spark format (cluster jar required) and a `pandas → CSV → spark.read.csv` fallback that needs no jars.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-excel` — Excel (.xlsx) ingestion

Two ways to land Excel data in Spark: the native Spark Excel format (faster, parallel) or a pandas-mediated CSV path (no cluster setup).

## When to use
- User has `.xlsx` / `.xls` files in a Volume or Object Storage bucket.
- Mentioned: "Excel", ".xlsx", "spreadsheet ingestion".

## When NOT to use
- For CSV files → just use [`aidp-object-storage`](../aidp-object-storage/SKILL.md). Spark reads CSV natively.

## Option C — Pure-stdlib parser (no openpyxl, no JARs) ★ live-validated

The plugin ships a stdlib-only `.xlsx` reader. **No** `openpyxl`. **No** Crealytics JAR. Works on AIDP clusters that have neither PyPI access nor Maven access for the Crealytics dependency closure. Live-validated on the AIDP `tpcds` cluster — 5,460-byte file parsed in ~120 ms.

```python
import os
from oracle_ai_data_platform_connectors.excel import read_xlsx_stdlib

xlsx_path = os.environ["EXCEL_PATH"]
header, *body = read_xlsx_stdlib(xlsx_path)
df = spark.createDataFrame(body, schema=header)
df.show()
```

Limitations: read-only (no stdlib path to write .xlsx), first sheet only by default (pass `sheet_path="xl/worksheets/sheet2.xml"` for others), best-effort cell type coercion. Good for ingestion of small-to-medium workbooks; for big files (>50 MB) prefer Option A's `com.crealytics.spark.excel` JAR for parallel reads.

The implementation is at [scripts/oracle_ai_data_platform_connectors/excel.py](../../scripts/oracle_ai_data_platform_connectors/excel.py).

## Option A — `com.crealytics.spark.excel` format

### Cluster prerequisite
Upload the Crealytics Spark Excel jar (and its Apache POI dependencies) to a Volume and attach via the cluster Library tab:

| JAR | Maven coordinates |
|---|---|
| spark-excel | `com.crealytics:spark-excel_2.12:3.5.0_0.20.4` (matches Spark 3.5; pick the `_<spark-ver>_<release>` matching your cluster) |
| poi | bundled with spark-excel; if missing, add `org.apache.poi:poi-ooxml:5.2.5` and transitive deps |

```python
import os

excel_path = os.environ["EXCEL_PATH"]   # e.g. /Volumes/default/default/uploads/data.xlsx

df = (spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dataAddress", "'Sheet1'!A1")    # optional — default is first sheet, A1
      .load(excel_path))
df.show()
```

Strengths: parallel reads on large workbooks; predicate pushdown.

## Option B — pandas → CSV → Spark (no jars)

```python
import os, pandas as pd

excel_path = os.environ["EXCEL_PATH"]
csv_path   = excel_path.replace(".xlsx", ".csv")

# Read with pandas (single-threaded, in-driver)
pdf = pd.read_excel(excel_path)

# Convert to CSV in the same Volume / Object Storage path
pdf.to_csv(csv_path, index=False)
print(pdf.head())

# Re-read as Spark for distributed downstream work
df = spark.read.csv(csv_path, header=True, inferSchema=True)
df.show()
```

Strengths: no cluster JAR install. Tradeoff: driver-side single-threaded read; OOM risk for files >500 MB.

## Gotchas
- **`com.crealytics.spark.excel` jar version must match the cluster's Spark version.** A 3.4 jar on a 3.5 cluster errors out at format registration time.
- **`dataAddress` for multi-sheet files** — `"'Sheet 2'!A1"` (note quotes around sheet name with spaces).
- **`inferSchema=true` is slow** for big files — pre-declare schema with `.schema(...)` for production jobs.
- **Encoding / merged cells** — pandas handles most quirks; the Spark Excel jar can choke on merged-cell headers. If you see misaligned columns, prefer Option B.
- **Excel files in `oci://`** — both options work; pass `oci://bucket@ns/path/file.xlsx` directly, or pre-stage to `/Volumes/...` for repeated reads.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Read_excel_data/read_excel.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Read_excel_data/read_excel.ipynb)
- Crealytics Spark Excel: <https://github.com/crealytics/spark-excel>
