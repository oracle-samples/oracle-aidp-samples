# AIDP TPC-DS Benchmark — Sample Notebook

Runs the official TPC-DS v4.0.0 benchmark against your AIDP cluster. Results land in your AIDP Unified Catalog as Iceberg tables.

---

## Prerequisites

- Python 3.9+ on your local machine
- OCI CLI installed and configured (`oci setup config`)
- AIDP Workbench access

---

## Quick start

**1. Automated (recommended):**
```bash
# Skip the next two lines if you already have the repo cloned and open in your terminal
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
cd oracle-aidp-samples

cd data-engineering/tpcds-benchmark
python3 setup_tpcds.py   # Windows: use `python setup_tpcds.py`
```
The script builds the wheel, downloads duckdb, creates the OCI bucket, and uploads everything. Then follow the printed instructions to upload the notebook and run.

**2. Manual step-by-step:** see [`docs/SETUP.md`](docs/SETUP.md)

---

## What the notebook does

1. Downloads and installs dependencies from your OCI bucket — no internet access needed
2. Generates TPC-DS data via DuckDB `dsdgen` (SF ≤ 100)
3. Uploads 24 Parquet files to `oci://tpcds-benchmark-sf{SF}/` (idempotent — skips on re-runs)
4. Runs all 103 TPC-DS queries sequentially as a power test
5. Saves GeoMean + P50/P90/P95/P99 results to two Iceberg tables in the AIDP Unified Catalog

---

## Cluster shape by scale factor

| SF | Recommended shape | Expected run time | Status |
|---|---|---|---|
| 1 | 2 OCPUs, 32 GB, 200 GB block | ~7 min | Validated ✅ |
| 10 | 2 OCPUs, 32 GB, 200 GB block | ~17 min | Validated ✅ |
| 100 | 4+ OCPUs, 64 GB, 500 GB block | ~60+ min | Coming |

---

## Expected results

| SF | GeoMean baseline |
|---|---|
| 1 | ~3.10s |
| 10 | ~9.61s |

---

## Troubleshooting

See [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md) for the full guide covering installation errors, namespace issues, catalog schema mismatches, memory/disk errors, and query timeouts.

---

## Compliance

Results are **NOT** official TPC Benchmark Results. For internal performance testing only.

## License

[UPL](https://oss.oracle.com/licenses/upl/). TPC-DS query templates bundled per the TPC EULA — see `NOTICE`.
