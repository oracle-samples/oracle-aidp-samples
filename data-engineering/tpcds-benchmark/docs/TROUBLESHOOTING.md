# TPC-DS Benchmark — Troubleshooting Guide

---

## Installation issues

### `RuntimeError: Cannot access bucket '...'`
**Cause:** The OCI bucket doesn't exist or the cluster's dynamic group doesn't have permission to access it.

**Fix:**
1. Verify the bucket exists: OCI Console → Object Storage → Buckets
2. Check the bucket name matches `tpcds-benchmark-sf{SF}` exactly
3. Add this IAM policy in OCI Console → Identity → Policies:
```
allow dynamic-group <your-cluster-dynamic-group> to manage objects
  in compartment <your-compartment>
  where target.bucket.name = /tpcds-benchmark-sf*/
```

---

### `FileNotFoundException: ... .whl`
**Cause:** One or both wheel files were not uploaded to the bucket.

**Fix:** Re-run `python3 setup_tpcds.py` from your local machine, or upload manually:
```bash
oci os object put --bucket-name tpcds-benchmark-sf1 \
  --file dist/aidp_benchmark-0.1.2-py3-none-any.whl --force

oci os object put --bucket-name tpcds-benchmark-sf1 \
  --file "duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" --force
```

---

### `ModuleNotFoundError: aidp_benchmark`
**Cause:** Cell 2 didn't complete successfully.

**Fix:** Re-run Cell 2. Check the output for any error — the most common causes are bucket access (see above) or a stale `/tmp` directory. Cell 2 cleans `/tmp/aidp_pkg` on every run.

---

## Namespace issues

### `RuntimeError: Cannot auto-detect OCI namespace`
**Cause:** The AIDP Unified Catalog has no databases with OCI-backed storage locations, so the namespace cannot be detected.

**Fix:** Run this in a notebook cell to check:
```python
spark.sql("SHOW DATABASES").show()
```
At least one database must have an OCI URI location (e.g. `oci://bucket@namespace/...`). If none do, confirm that the AIDP Unified Catalog is the default catalog for your Spark session.

---

### `BucketNotFound` with namespace `axawcvicgt9r`
**Cause:** The namespace auto-detection returned the RP auth namespace instead of the storage namespace. This can happen on some AIDP deployments.

**Fix:** This is a known issue. The toolkit detects the storage namespace from catalog DB properties — ensure your catalog has at least one database with an OCI location (see above).

---

## Catalog / save issues

### `AnalysisException: Cannot find data for column`
**Cause:** The `aidp_benchmark.run_summary` or `aidp_benchmark.query_results` table was created by a previous version of the toolkit with a different schema. The current version can't append to it.

**Fix:** Run Cell 6 once to drop and recreate the catalog database:
1. Open the notebook
2. Uncomment the two lines in Cell 6
3. Run Cell 6
4. Comment the lines back out
5. Re-run Cell 4

> ⚠️ This permanently deletes all previous benchmark results from the catalog.

---

## Memory / disk issues

### `OutOfMemoryError` in DuckDB (SF=100)
**Cause:** The driver doesn't have enough memory to run `dsdgen` at SF=100.

**Fix:** Use a driver shape with at least 64 GB memory and 500 GB block volume.

### `Disk full` during data generation
**Cause:** The driver block volume is too small.

**Fix:** Use 200 GB+ block volume for SF=1/10, 500 GB+ for SF=100.

---

## Query timeout issues

### Cell 3 runs for more than 20 minutes at SF=1
**Cause:** One or more queries exceeded the 300s per-query timeout. This can happen if the cluster is under heavy load or a query triggered a Spark job that stalled.

**Fix:**
1. Check the Spark UI (AIDP Workbench → cluster → Active Jobs)
2. If a job is stuck, restart the cluster
3. Re-run from Cell 2

---

## setup_tpcds.py issues

### `oci: command not found`
**Cause:** OCI CLI is not installed or not on PATH.

**Fix:** Install from https://docs.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm then run `oci setup config`.

### `~/.oci/config not found`
**Cause:** OCI CLI is installed but not configured.

**Fix:** Run `oci setup config` and follow the prompts.

### Wheel build fails
**Cause:** Missing Python build tools.

**Fix:**
```bash
pip3 install setuptools wheel
python3 setup_tpcds.py
```
