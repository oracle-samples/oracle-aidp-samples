# TPC-DS Benchmark — Manual Setup Guide

> Use this guide if you prefer to run each step yourself instead of using `setup.py`.
> For the automated path, run `python3 setup_tpcds.py` from the `tpcds-benchmark/` folder.

---

## Prerequisites

- Python 3.9+ on your local machine
- OCI CLI installed: https://docs.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm
- OCI CLI configured: run `oci setup config` or follow https://docs.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm
- Access to AIDP Workbench

---

## Step 1 — Clone the repo

```bash
# Skip the next two lines if you already have the repo cloned and open in your terminal
git clone https://github.com/oracle-samples/oracle-aidp-samples.git
cd oracle-aidp-samples

cd data-engineering/tpcds-benchmark
```

---

## Step 2 — Build the aidp_benchmark wheel

```bash
pip3 wheel . --no-deps -w dist/        # Windows: pip wheel . --no-deps -w dist/
# Output: dist/aidp_benchmark-0.1.2-py3-none-any.whl
```

---

## Step 3 — Download the duckdb wheel

This wheel is for your AIDP cluster (Linux, Python 3.11) — not for your local machine.

```bash
pip3 download duckdb==0.10.3 \   # Windows: pip download duckdb==0.10.3 \
  --platform manylinux_2_17_x86_64 \
  --python-version 311 \
  --only-binary=:all: --no-deps
# Output: duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

---

## Step 4 — Create an OCI bucket

Replace `<your-compartment-ocid>` with your compartment OCID (OCI Console → Identity → Compartments).

```bash
oci os bucket create \
  --name tpcds-benchmark-sf1 \
  --compartment-id <your-compartment-ocid>
```

Or in OCI Console: Object Storage → Buckets → Create Bucket → name: `tpcds-benchmark-sf1`

> For SF=10, create `tpcds-benchmark-sf10`. For SF=100, create `tpcds-benchmark-sf100`.

---

## Step 5 — Upload both wheels to the bucket

**Option A — OCI Console (no terminal needed):**
OCI Console → Object Storage → `tpcds-benchmark-sf1` → Upload
Upload these two files from your `tpcds-benchmark/` folder:
- `dist/aidp_benchmark-0.1.2-py3-none-any.whl`
- `duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`

**Option B — OCI CLI (run from tpcds-benchmark/ folder):**
```bash
oci os object put --bucket-name tpcds-benchmark-sf1 \
  --file dist/aidp_benchmark-0.1.2-py3-none-any.whl \
  --force

oci os object put --bucket-name tpcds-benchmark-sf1 \
  --file "duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" \
  --force
```

---

## Step 6 — Create an AIDP cluster

In AIDP Workbench: Compute → Create

| SF | Recommended shape | Expected run time |
|---|---|---|
| 1 | 2 OCPUs, 32 GB, 200 GB block | ~7 min |
| 10 | 2 OCPUs, 32 GB, 200 GB block | ~17 min |
| 100 | 4+ OCPUs, 64 GB, 500 GB block | Coming |

---

## Step 7 — Set IAM policy (if you hit a permission error)

If Cell 3 fails with a bucket access error, add this policy in OCI Console → Identity → Policies:

```
allow dynamic-group <your-cluster-dynamic-group> to manage objects
  in compartment <your-compartment>
  where target.bucket.name = /tpcds-benchmark-sf*/
```

---

## Step 8 — Upload the notebook and run

1. In AIDP Workbench: Workspaces → Upload
   Select: `oracle-aidp-samples/data-engineering/tpcds-benchmark/aidp_benchmark_v1_2.ipynb`
2. Open the notebook → attach the cluster you created in Step 6
3. Cell 1 is pre-set to `SF = 1` — confirm this matches what you want
4. Click **Run All** — do not cancel mid-run

Expected result: `103/103 PASS, GeoMean ~3.10s` at SF=1

> See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) if anything goes wrong.

