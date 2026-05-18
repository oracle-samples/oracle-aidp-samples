# AIDP TPC-DS Benchmark — Sample Notebook

> Customer-facing README to ship alongside `aidp_benchmark_v1_2.ipynb` in [`oracle-aidp-samples`](https://github.com/oracle-samples/oracle-aidp-samples). When this gets PR'd into the samples repo, it becomes that folder's `README.md`.

## TL;DR

Set the scale factor → Run All → results land in your AIDP Unified Catalog as Iceberg tables. Five lines of customer code total.

## What this notebook does

Runs the official TPC-DS v4.0.0 benchmark against your AIDP cluster:

1. Auto-detects your OCI namespace, region, and compartment via Resource Principal
2. Generates TPC-DS data via DuckDB `dsdgen` (single-node, fast for SF ≤ 100)
3. Uploads 24 Parquet files to `oci://tpcds-benchmark-sf{SF}/` in your tenancy (idempotent — skips on re-runs)
4. Runs all 103 TPC-DS queries (99 + a/b splits) sequentially as a power test
5. Computes geometric mean + P50/P90/P95/P99 latency
6. Captures `EXPLAIN EXTENDED` for each query
7. Saves results to two Iceberg tables in the AIDP Unified Catalog:
   - `aidp_benchmark.run_summary` (one row per run)
   - `aidp_benchmark.query_results` (one row per query per run)

## Prerequisites

Before running:

1. **AIDP Workbench cluster** — Python 3.10+, Spark 3.5.x, with internet egress
2. **Cluster shape** — for SF=1 / SF=10: any default shape. For SF=100: ≥ 2 OCPUs, 32 GB memory, 200 GB block volume on the driver
3. **OCI bucket pre-created** — name: `tpcds-benchmark-sf{SF}` (e.g., `tpcds-benchmark-sf10`), in the same region as your cluster
4. **IAM policy** — cluster's dynamic group needs `manage objects` on the bucket:
   ```
   allow dynamic-group <your-aidp-cluster-dg> to manage objects
     in compartment <your-compartment>
     where target.bucket.name = /tpcds-benchmark-sf*/
   ```
5. **AIDP Unified Catalog** is the default catalog for the Spark session

## How to run

1. Upload `aidp_benchmark_v1_2.ipynb` to your AIDP Workbench
2. Attach your cluster
3. Open Cell 1 — set `SF = 1`, `10`, or `100`
4. **Run All**
5. View results: `SELECT * FROM aidp_benchmark.run_summary ORDER BY end_time DESC LIMIT 5`

That's it.

## What to expect

| SF | First-run time (data gen + upload) | Power test (103 queries) | GeoMean (baseline) |
|---|---|---|---|
| 1 | ~30 sec | ~5 min | ~3.10s |
| 10 | ~3 min | ~17 min | ~9.67s |
| 100 | ~20 min | ~60+ min | TBD |

Re-runs at the same SF skip data generation (idempotent) — straight to queries.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: aidp_benchmark` | Cell 2 didn't complete | Re-run Cell 2; check cluster has internet egress |
| `Cannot access bucket 'tpcds-benchmark-sfX'` | Bucket doesn't exist OR IAM denied | Create the bucket in OCI Console; grant the dynamic group `manage objects` |
| `OutOfMemoryException` in DuckDB at SF=100 | Driver memory too small | Use a 2-OCPU / 32 GB driver shape (Quickstart default works) |
| `Disk full` during generation | Driver block volume too small | Use 200 GB+ driver block volume |
| Cell 3 hangs forever | SF too large for the cluster | Try a smaller SF first to validate the pipeline |
| Wrong namespace in error | Cluster's RP lives in a different tenancy than expected | Confirm with your OCI admin; pass `namespace=` explicitly if needed |

## Compliance

Results produced by this toolkit are **NOT** official TPC Benchmark Results. The toolkit implements TPC-DS workloads for internal performance testing — it cannot be used to publish or market official TPC results without TPC sponsorship.

## Support

File issues at the project's GitHub repo. PRs welcome.

## License

[UPL (Universal Permissive License)](https://oss.oracle.com/licenses/upl/). TPC-DS query templates and `dsdgen` bundled per the TPC EULA — see `NOTICE`.
