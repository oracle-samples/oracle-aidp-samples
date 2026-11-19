# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Fabric Schema Drift Detective — in-workspace runner
#
# Runs one full drift-detection cycle **inside Fabric** using the
# notebook's executing identity (`notebookutils.credentials.getToken`) —
# no client secrets, no `.env`.
#
# **Setup (once):** set `REPO_URL` below (and optionally `REPO_REF`)
# to the Git repository holding this project, and fill the workspace /
# item IDs. Schedule this notebook, or call it from the
# `DriftCheckPipeline` Data Factory pipeline in this folder.

# PARAMETERS CELL ********************

# -- parameters (overridable from a pipeline Notebook activity) --------
REPO_URL = ""            # e.g. "https://github.com/your-org/microsoft-fabric-schema-drift-detection.git"
REPO_REF = "main"        # branch or tag to run
WORKSPACE_ID = ""        # this workspace's GUID
LAKEHOUSE_ID = ""        # Bronze+Silver lakehouse item id
WAREHOUSE_ID = ""        # Gold warehouse item id
SEMANTIC_MODEL_ID = ""   # semantic model item id
SQL_ENDPOINT = ""        # SQL analytics endpoint host (optional, richer schema)
SQL_DATABASE = ""        # SQL endpoint database name (optional)
DRY_RUN = True           # True: render notifications/PR only, send nothing
CAPTURE_BASELINE = False # True: (re)capture baselines instead of diffing
# persist baselines in the attached lakehouse so they survive between runs
# (the /tmp clone is ephemeral); "" keeps the repo-local .baselines dir
BASELINE_DIR = "/lakehouse/default/Files/drift_baselines"

# CELL ********************

# -- fetch the project ---------------------------------------------------
import os
import subprocess
import sys

if not REPO_URL:
    raise ValueError("Set REPO_URL in the parameters cell (or pipeline parameters).")

WORK_DIR = "/tmp/drift-detective"
if not os.path.exists(WORK_DIR):
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", REPO_REF, REPO_URL, WORK_DIR],
        check=True,
    )
os.chdir(WORK_DIR)
subprocess.run(
    [sys.executable, "-m", "pip", "install", "-q", "-r", "requirements.txt"],
    check=True,
)
sys.path.insert(0, WORK_DIR)
print(f"project ready at {WORK_DIR} ({REPO_REF})")

# CELL ********************

# -- configure: notebook identity + IDs from parameters -------------------
os.environ["FABRIC_AUTH_METHOD"] = "notebookutils"  # notebook identity, no secrets

from fabric_drift_detective.config import load_config

cfg = load_config("config.yaml")
cfg["fabric"].update(
    {
        "auth_method": "notebookutils",
        "workspace_id": WORKSPACE_ID or cfg["fabric"].get("workspace_id", ""),
        "lakehouse_id": LAKEHOUSE_ID or cfg["fabric"].get("lakehouse_id", ""),
        "warehouse_id": WAREHOUSE_ID or cfg["fabric"].get("warehouse_id", ""),
        "semantic_model_id": SEMANTIC_MODEL_ID
        or cfg["fabric"].get("semantic_model_id", ""),
        "sql_endpoint": SQL_ENDPOINT or cfg["fabric"].get("sql_endpoint", ""),
        "sql_database": SQL_DATABASE or cfg["fabric"].get("sql_database", ""),
    }
)
if BASELINE_DIR:
    cfg.setdefault("baseline", {})["dir"] = BASELINE_DIR
print("effective fabric config:",
      {k: v for k, v in cfg["fabric"].items() if k != "auth_method"})

# CELL ********************

# -- run one detection cycle ----------------------------------------------
from fabric_drift_detective.cli import capture_baseline, make_backend, run_once
from fabric_drift_detective.schema_store import BaselineError, SchemaStore

if CAPTURE_BASELINE:
    capture_baseline(
        make_backend("live", cfg),
        SchemaStore(cfg.get("baseline", {}).get("dir", ".baselines")),
    )
    critical_count = 0
else:
    try:
        critical_count = run_once("live", cfg, dry_run=DRY_RUN, open_pr=False)
    except BaselineError as exc:
        # Baselines are never recreated implicitly. On the very first run
        # (or if the lakehouse baseline files were deleted) set
        # CAPTURE_BASELINE = True above, run once, then flip it back.
        raise RuntimeError(
            f"Baseline error: {exc}\n"
            "First run? Set CAPTURE_BASELINE = True, run this notebook "
            "once to snapshot baselines into the lakehouse, then set it "
            "back to False for drift detection."
        ) from exc

print(f"critical drifts: {critical_count}")

# CELL ********************

# -- surface the result to a calling pipeline ------------------------------
try:
    import notebookutils  # type: ignore[import-not-found]

    notebookutils.notebook.exit(str(critical_count))
except ImportError:
    pass  # running outside Fabric (local test) - exit value not needed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
