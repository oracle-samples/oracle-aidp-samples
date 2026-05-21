# AIDP TPC-DS Benchmark — Sample Notebook

> **TL;DR:** Copy the `tpcds-benchmark/` folder into your AIDP workspace → set `SF` in Cell 1 → Run All → results land in your AIDP Unified Catalog as Iceberg tables.

## What this notebook does

Runs the official TPC-DS v4.0.0 benchmark against your AIDP cluster:

1. Imports `aidp_benchmark` directly from the local folder — no wheel download needed
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

### 2. Local package — no wheel needed

The `aidp_benchmark/` Python package ships alongside this notebook in the same folder. Cell 2 adds the folder to `sys.path` so `import aidp_benchmark` works directly. No wheel file, no OCI bucket download.

### 3. OCI bucket for benchmark data

Create a bucket named `tpcds-benchmark-sf{SF}` (e.g. `tpcds-benchmark-sf10`) in the same region and compartment as your cluster.

**Option A — OCI Console:**
Object Storage → Buckets → Create Bucket → name: `tpcds-benchmark-sf10`

**Option B — OCI CLI (from your local machine with `~/.oci/config` configured):**
```bash
oci os bucket create \
  --name tpcds-benchmark-sf10 \
  --compartment-id <your-compartment-ocid> \
  --namespace <your-namespace>
```

### 4. IAM policy

Your cluster's dynamic group needs `manage objects` on the data bucket:

```
allow dynamic-group <your-aidp-cluster-dg> to manage objects
  in compartment <your-compartment>
  where target.bucket.name = /tpcds-benchmark-sf*/
```

### 5. AIDP Unified Catalog
The cluster's default Spark catalog must be the AIDP Unified Catalog (default on all AIDP clusters).

---

## How to run

**Step 1 — Get the folder into your AIDP workspace**

Clone the repo and upload the entire `tpcds-benchmark/` folder to your AIDP Workbench, or use the AIDP Git integration to clone directly. The notebook and the `aidp_benchmark/` package must be in the same directory.

**Step 2 — Open the notebook and attach your cluster**

Open `aidp_benchmark_v1_2.ipynb` and attach your AIDP cluster.

**Step 3 — Set the scale factor in Cell 1**

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
| `ModuleNotFoundError: aidp_benchmark` | Notebook not in same folder as `aidp_benchmark/` | Ensure the full `tpcds-benchmark/` folder is in your workspace, not just the notebook |
| `RuntimeError: Cannot access bucket '...'` | Bucket doesn't exist or IAM policy missing | Create the bucket; verify IAM policy covers `tpcds-benchmark-sf*` |
| `RuntimeError: Cannot auto-detect OCI namespace` | Catalog has no databases with OCI locations | Run `SHOW DATABASES` — at least one DB must have an OCI-backed location |
| `OutOfMemoryError` in DuckDB at SF=100 | Driver memory too small | Use ≥ 32 GB driver memory |
| `Disk full` during generation | Driver block volume too small | Use 200 GB+ block volume |
| Cell 3 runs for >20 min at SF=1 | One query hit the 300s per-query timeout | Check Spark UI; restart cluster if needed |

---

## Compliance

Results produced by this toolkit are **NOT** official TPC Benchmark Results. The toolkit implements TPC-DS workloads for internal performance testing — it cannot be used to publish or market official TPC results without TPC sponsorship.

## License

[UPL (Universal Permissive License)](https://oss.oracle.com/licenses/upl/). TPC-DS query templates and `dsdgen` bundled per the TPC EULA — see `NOTICE`.
