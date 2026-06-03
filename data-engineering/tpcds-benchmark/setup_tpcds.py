"""
AIDP TPC-DS Benchmark — Setup Script
Runs on Mac, Linux, and Windows (Python 3.9+).

What this script does:
  1. Checks prerequisites (Python version, OCI CLI, OCI config)
  2. Builds the aidp_benchmark wheel from source
  3. Downloads the duckdb wheel for AIDP clusters (Linux, Python 3.11)
  4. Creates the OCI bucket tpcds-benchmark-sf{SF}
  5. Uploads both wheels to the bucket

What you still do manually (script prints instructions):
  - Create an AIDP cluster
  - Upload the notebook to AIDP Workbench
  - Run the notebook
"""

import os
import sys
import subprocess
import platform
import shutil

# ── Colours (disabled on Windows if not supported) ────────────────────────
def green(s):  return f"\033[92m{s}\033[0m" if _colour else s
def red(s):    return f"\033[91m{s}\033[0m" if _colour else s
def yellow(s): return f"\033[93m{s}\033[0m" if _colour else s
def bold(s):   return f"\033[1m{s}\033[0m"  if _colour else s
_colour = platform.system() != "Windows" or os.environ.get("TERM") == "xterm"

HERE = os.path.dirname(os.path.abspath(__file__))
NOTEBOOK = os.path.join(HERE, "aidp_benchmark_v1_2.ipynb")
DIST_DIR = os.path.join(HERE, "dist")
DUCKDB_WHL = "duckdb-0.10.3-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
WHL_NAME   = "aidp_benchmark-0.1.2-py3-none-any.whl"


def step(n, title):
    print(f"\n{bold(f'[{n}]')} {title}")


def ok(msg):   print(f"  {green('✓')} {msg}")
def warn(msg): print(f"  {yellow('!')} {msg}")
def fail(msg): print(f"  {red('✗')} {msg}"); sys.exit(1)


def run(cmd, cwd=None, capture=True):
    r = subprocess.run(cmd, shell=True, capture_output=capture, text=True, cwd=cwd)
    return r


# ── Step 1: Prerequisites ─────────────────────────────────────────────────
step(1, "Checking prerequisites")

if sys.version_info < (3, 9):
    fail(f"Python 3.9+ required (you have {sys.version})")
ok(f"Python {sys.version_info.major}.{sys.version_info.minor}")

if shutil.which("oci") is None:
    fail(
        "OCI CLI not found.\n"
        "  Install it: https://docs.oracle.com/iaas/Content/API/SDKDocs/cliinstall.htm\n"
        "  Then re-run this script."
    )
ok("OCI CLI found")

oci_config = os.path.expanduser("~/.oci/config")
if not os.path.exists(oci_config):
    fail(
        "~/.oci/config not found.\n"
        "  Set up OCI CLI config: oci setup config\n"
        "  Or follow: https://docs.oracle.com/iaas/Content/API/Concepts/apisigningkey.htm"
    )
ok("OCI config found")


# ── Step 2: Build the aidp_benchmark wheel ────────────────────────────────
step(2, "Building aidp_benchmark wheel")

if os.path.isdir(DIST_DIR):
    shutil.rmtree(DIST_DIR)
os.makedirs(DIST_DIR, exist_ok=True)

pip = "pip3" if shutil.which("pip3") else "pip"
r = run(f"{pip} wheel . --no-deps -w dist/ --quiet", cwd=HERE)
if r.returncode != 0:
    fail(f"Wheel build failed:\n{r.stderr}")

whl_path = os.path.join(DIST_DIR, WHL_NAME)
if not os.path.exists(whl_path):
    fail(f"Expected wheel not found: {whl_path}")
ok(f"Built {WHL_NAME}")


# ── Step 3: Download duckdb wheel for AIDP cluster ────────────────────────
step(3, "Downloading duckdb wheel (Linux / Python 3.11 — for AIDP cluster)")
warn("This wheel is for the cluster, not your machine — that's intentional.")

duckdb_path = os.path.join(HERE, DUCKDB_WHL)
if os.path.exists(duckdb_path):
    ok(f"Already downloaded: {DUCKDB_WHL}")
else:
    r = run(
        f"{sys.executable} -m pip download duckdb==0.10.3 "
        "--platform manylinux_2_17_x86_64 "
        "--python-version 311 "
        "--only-binary=:all: --no-deps "
        f"-d \"{HERE}\"",
    )
    if r.returncode != 0:
        fail(f"duckdb download failed:\n{r.stderr}")
    if not os.path.exists(duckdb_path):
        fail(f"Expected duckdb wheel not found after download.")
    ok(f"Downloaded {DUCKDB_WHL}")


# ── Step 4: Get SF and compartment OCID ──────────────────────────────────
step(4, "Bucket configuration")

print("  Which scale factor do you plan to run?")
print("  1 = ~7 min  |  10 = ~17 min  |  100 = coming soon")
sf_input = input("  SF [default: 1]: ").strip() or "1"
if sf_input not in ("1", "10", "100"):
    fail("SF must be 1, 10, or 100.")
bucket = f"tpcds-benchmark-sf{sf_input}"
ok(f"Bucket name: {bucket}")

# Read tenancy OCID from ~/.oci/config to use as default compartment
import configparser as _cp
_cfg = _cp.ConfigParser()
_cfg.read(oci_config)
tenancy_from_config = _cfg.get("DEFAULT", "tenancy", fallback=None)

print("\n  How would you like to set up the bucket and upload the wheels?")
print("  1. Let the script do it automatically")
print("  2. Manual — terminal commands (OCI CLI)")
print("  3. Manual — OCI Console only (no terminal needed)")
upload_choice = input("  Choice [1/2/3]: ").strip()

whl_path      = os.path.join(DIST_DIR, WHL_NAME)
duck_path     = os.path.join(HERE, DUCKDB_WHL)
whl_rel       = f"dist/{WHL_NAME}"
duck_rel      = DUCKDB_WHL

if upload_choice == "2":
    print(f"""
  Manual setup — terminal commands:

  1. Create the bucket:
     (Find compartment OCID: OCI Console → Identity & Security → Compartments → copy OCID)
     oci os bucket create --name tpcds-benchmark-sf{sf_input} --compartment-id <compartment-ocid>

  2. Upload both wheels (run from tpcds-benchmark/ folder):
     oci os object put --bucket-name tpcds-benchmark-sf{sf_input} --file "{whl_rel}" --force
     oci os object put --bucket-name tpcds-benchmark-sf{sf_input} --file "{duck_rel}" --force

  3. Create an AIDP cluster:
     AIDP Workbench → Compute → Create
     Recommended shape for SF={sf_input}: 2 OCPUs, 32 GB, 200 GB block

  4. Upload the notebook:
     AIDP Workbench → Workspaces → Upload
     Select: oracle-aidp-samples/data-engineering/tpcds-benchmark/aidp_benchmark_v1_2.ipynb

  5. Open notebook → attach cluster → confirm SF={sf_input} in Cell 1 → Run All
""")
    sys.exit(0)

if upload_choice == "3":
    print(f"""
  Manual setup — OCI Console only:

  1. Create the bucket:
     OCI Console → Object Storage → Buckets → Create Bucket
     Name: tpcds-benchmark-sf{sf_input}

  2. Upload both wheels to the bucket:
     OCI Console → Object Storage → tpcds-benchmark-sf{sf_input} → Upload
     Upload these two files (from your tpcds-benchmark/ folder):
       • {whl_rel}
       • {duck_rel}

  3. Create an AIDP cluster:
     AIDP Workbench → Compute → Create
     Recommended shape for SF={sf_input}: 2 OCPUs, 32 GB, 200 GB block

  4. Upload the notebook:
     AIDP Workbench → Workspaces → Upload
     Select: oracle-aidp-samples/data-engineering/tpcds-benchmark/aidp_benchmark_v1_2.ipynb

  5. Open notebook → attach cluster → confirm SF={sf_input} in Cell 1 → Run All
""")
    sys.exit(0)

# ── Auto upload path ───────────────────────────────────────────────────────
def prompt_compartment():
    print("  Find your compartment OCID:")
    print("  OCI Console → Identity & Security → Compartments → click compartment → copy OCID")
    for attempt in range(1, 4):
        val = input(f"  Compartment OCID (attempt {attempt}/3): ").strip()
        if val.startswith("ocid1."):
            return val
        warn(f"Doesn't look like a valid OCID (should start with 'ocid1.'). {3 - attempt} attempt(s) left.")

    # 3 failed attempts — switch to manual
    whl_path  = os.path.join(DIST_DIR, WHL_NAME)
    duck_path = os.path.join(HERE, DUCKDB_WHL)
    print(f"""
  No worries — run these commands yourself once you have your compartment OCID:

  # Find OCID: OCI Console → Identity & Security → Compartments → click compartment → copy OCID

  oci os bucket create --name tpcds-benchmark-sf{sf_input} --compartment-id <your-compartment-ocid>
  oci os object put --bucket-name tpcds-benchmark-sf{sf_input} --file "{whl_path}" --force
  oci os object put --bucket-name tpcds-benchmark-sf{sf_input} --file "{duck_path}" --force

  Then:
    1. Create an AIDP cluster: AIDP Workbench → Compute → Create
       Shape: 2 OCPUs, 32 GB, 200 GB block
    2. Upload notebook: Workspaces → Upload
       oracle-aidp-samples/data-engineering/tpcds-benchmark/aidp_benchmark_v1_2.ipynb
    3. Open notebook → attach cluster → confirm SF={sf_input} in Cell 1 → Run All
""")
    sys.exit(0)

if tenancy_from_config:
    ok("Found tenancy in ~/.oci/config")
    confirm = input("  Use root compartment (tenancy) as target? [Y/n]: ").strip().lower() or "y"
    if confirm in ("y", "yes"):
        compartment_id = tenancy_from_config
    else:
        compartment_id = prompt_compartment()
else:
    warn("Tenancy not found in ~/.oci/config — enter compartment OCID manually.")
    compartment_id = prompt_compartment()


# ── Step 5: Create bucket ────────────────────────────────────────────────
step(5, f"Creating bucket: {bucket}")

r = run(f"oci os bucket create --name {bucket} --compartment-id {compartment_id}")
if r.returncode != 0:
    if "BucketAlreadyExists" in r.stderr or "409" in r.stderr:
        ok(f"Bucket already exists — skipping creation")
    else:
        fail(f"Bucket creation failed:\n{r.stderr[-500:]}")
else:
    ok(f"Bucket created: {bucket}")


# ── Step 6: Upload wheels ────────────────────────────────────────────────
step(6, "Uploading wheels to bucket")

for name, path in [(WHL_NAME, whl_path), (DUCKDB_WHL, duckdb_path)]:
    r = run(f"oci os object put --bucket-name {bucket} --file \"{path}\" --name {name} --force")
    if r.returncode != 0:
        fail(f"Upload failed for {name}:\n{r.stderr[-300:]}")
    ok(f"Uploaded {name}")


SF_SHAPES = {"1": "2 OCPUs, 32 GB, 200 GB block", "10": "2 OCPUs, 32 GB, 200 GB block", "100": "4+ OCPUs, 64 GB, 500 GB block"}
SF_TIMES  = {"1": "~7 min", "10": "~17 min", "100": "~60+ min"}
NOTEBOOK_REL = "oracle-aidp-samples/data-engineering/tpcds-benchmark/aidp_benchmark_v1_2.ipynb"

# ── Done: print next steps ───────────────────────────────────────────────
print(f"""
{green('━' * 60)}
{bold('  Setup complete!')}
{green('━' * 60)}

  What was done:
    ✓ Built aidp_benchmark wheel
    ✓ Downloaded duckdb wheel (Linux / Python 3.11)
    ✓ Created bucket: {bucket}
    ✓ Uploaded both wheels to {bucket}

  Next steps (manual — script can't do these):

  {bold('Step 1 — Create an AIDP cluster')}
    Go to: AIDP Workbench → Compute → Create
    Recommended shape for SF={sf_input}: {SF_SHAPES.get(sf_input, "2 OCPUs, 32 GB, 200 GB block")}
    Expected run time: {SF_TIMES.get(sf_input, "~7 min")}

  {bold('Step 2 — Upload the notebook')}
    In AIDP Workbench: Workspaces → Upload
    Select this file from your cloned repo:
      {NOTEBOOK_REL}

  {bold('Step 3 — Run the benchmark')}
    1. Open the uploaded notebook
    2. Attach the cluster you just created
    3. Cell 1 is already set to SF={sf_input} — confirm it is correct
    4. Click Run All and wait

  Expected result: 103/103 PASS, GeoMean ~3.10s at SF={sf_input}

  Need help? See docs/TROUBLESHOOTING.md
{green('━' * 60)}
""")
