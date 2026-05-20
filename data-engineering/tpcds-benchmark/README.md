# AIDP TPC-DS Benchmark — Sample Notebook

> **TL;DR:** Upload two wheel files to an OCI bucket → set `BUCKET` in Cell 2 → set `SF` in Cell 1 → Run All → results land in your AIDP Unified Catalog as Iceberg tables.

## What this notebook does

Runs the official TPC-DS v4.0.0 benchmark against your AIDP cluster:

1. Auto-detects your OCI namespace from the AIDP Unified Catalog (no hardcoding)
2. Generates TPC-DS data via DuckDB `dsdgen` (single-node, SF ≤ 100)
3. Uploads 24 Parquet files to `oci://tpcds-benchmark-sf{SF}/` (idempotent — skips on re-runs)
4. Runs all 103 TPC-DS queries (99 + a/b splits) sequentially as a power test
5. Computes GeoMean + P50/P90/P95/P99 latency per query
6. Saves results to two Iceberg tables in the AIDP Unified Catalog:
   - `aidp_benchmark.run_summary` — one row per run
   - `aidp_benchmark.query_results` — one row per query per run

## Prerequisites

### 1. AIDP Workbench cluster
- Python 3.11, Spark 3.5.x
- Recommended shape for SF=1/10: AMD, 2 OCPUs, 32 GB memory, 200 GB block volume on the driver
- No internet egress required — all dependencies are served from OCI Object Storage

### 2. Two wheel files uploaded to an OCI bucket

Download both files and upload them to an OCI bucket you control (e.g. `aidp-tpcds`):

| File | Where to get it |
|---|---|
| `aidp_benchmark-0.1.2-py3-none-any.whl` | [GitHub Release — tpcds-benchmark-v1.2](https://github.com/oracle-samples/oracle-aidp-samples/releases/tag/tpcds-benchmark-v1.2) |
| `duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` | [PyPI — duckdb 0.10.3](https://pypi.org/pypi/duckdb/0.10.3/json) or the same GitHub Release |

Upload both to OCI Console → Object Storage → your bucket.

### 3. OCI bucket for benchmark data

Create a bucket named `tpcds-benchmark-sf{SF}` (e.g. `tpcds-benchmark-sf10`) in the same region and compartment as your cluster.

### 4. IAM policy

Your cluster's dynamic group needs `manage objects` on both buckets:

```
allow dynamic-group <your-aidp-cluster-dg> to manage objects
  in compartment <your-compartment>
  where target.bucket.name = /tpcds-benchmark-sf*/

allow dynamic-group <your-aidp-cluster-dg> to manage objects
  in compartment <your-compartment>
  where target.bucket.name = '<your-wheel-bucket>'
```

### 5. AIDP Unified Catalog
The cluster's default Spark catalog must be the AIDP Unified Catalog (this is the default on all AIDP clusters — no action needed unless you customized it).

---

## How to run

**Step 1 — Upload the notebook**

Upload `aidp_benchmark_v1_2.ipynb` to your AIDP Workbench and attach your cluster.

**Step 2 — Configure Cell 2**

Open Cell 2 and set `BUCKET` to the bucket where you uploaded the wheel files:

```python
BUCKET = "aidp-tpcds"   # ← change this to your bucket name
```

**Step 3 — Configure Cell 1**

Set the scale factor:

```python
SF = 1   # start with 1 to validate end-to-end, then 10 or 100
```

**Step 4 — Run All**

Click **Run All** and wait. Do not cancel mid-run.

**Step 5 — View results**

```sql
SELECT * FROM aidp_benchmark.run_summary ORDER BY end_time DESC LIMIT 5
```

---

## What to expect

| SF | First-run (data gen + upload) | Power test (103 queries) | GeoMean baseline |
|---|---|---|---|
| 1 | ~30 s | ~5–7 min | ~3.10s |
| 10 | ~3 min | ~17 min | ~9.61s |
| 100 | ~20 min | ~60+ min | TBD |

Re-runs at the same SF skip data generation and go straight to queries.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `RuntimeError: Cannot access bucket '...'` | Bucket doesn't exist or IAM policy missing | Create the bucket; check IAM policy covers both your wheel bucket and `tpcds-benchmark-sf*` |
| `RuntimeError: duckdb .so not found` | Wrong duckdb wheel filename (Python version mismatch) | Ensure you uploaded the `cp311` Linux x86_64 wheel; your cluster must be Python 3.11 |
| `ModuleNotFoundError: aidp_benchmark` | Cell 2 didn't complete or `BUCKET` is wrong | Re-run Cell 2; verify both wheel files are in the specified bucket |
| `RuntimeError: Cannot auto-detect OCI namespace` | Catalog has no databases with OCI locations | Run `SHOW DATABASES` — at least one DB must have an OCI-backed location |
| `OutOfMemoryError` in DuckDB at SF=100 | Driver memory too small | Use ≥ 32 GB driver memory |
| `Disk full` during generation | Driver block volume too small | Use 200 GB+ block volume |
| Cell 3 runs for >20 min at SF=1 | One query exceeded the 300s per-query timeout | Check Spark UI for stuck jobs; restart cluster if needed |

---

## Compliance

Results produced by this toolkit are **NOT** official TPC Benchmark Results. The toolkit implements TPC-DS workloads for internal performance testing — it cannot be used to publish or market official TPC results without TPC sponsorship.

## License

[UPL (Universal Permissive License)](https://oss.oracle.com/licenses/upl/). TPC-DS query templates and `dsdgen` bundled per the TPC EULA — see `NOTICE`.
