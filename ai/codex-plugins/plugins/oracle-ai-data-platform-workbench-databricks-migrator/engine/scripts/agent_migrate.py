#!/usr/bin/env python3
"""
Agent Migration Pipeline
=========================
Single-workflow, no-local-storage notebook migration.

For each notebook:
  1. DOWNLOAD from AIDP workspace (temporary)
  2. ANALYZE with OpenAI GPT-5.5 - deep compatibility analysis
  3. MIGRATE with OpenAI GPT-5.5 - produce migrated notebook + cell classifications
  4. TEST on AIDP cluster - execute READ_ONLY cells, test API connectivity,
     verify dependencies, compare outputs against originals
  5. FIX with OpenAI GPT-5.5 - fix any failures found in testing
  6. UPLOAD all artifacts to agent-migrated/ folder on AIDP workspace
  7. DELETE all local temporary files

Produces per notebook:
  - analysis_report.md     (Step 2)
  - migrated.ipynb         (Step 3)
  - migration_report.md    (Step 3)
  - test_report.md         (Step 4+5)
  - final.ipynb            (Step 5 - after fixes)

Usage:
    python3 agent_migrate.py --parallel 20
    python3 agent_migrate.py --start 0 --end 100 --parallel 20
"""

from openai_model import OpenAIModelCompatClient, OPENAI_MODEL
import asyncio
import json
import os
import sys
import re
import copy
import time
import tempfile
import shutil
import argparse
import urllib.parse
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import oci
import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs, get_oci_signer

# â”€â”€â”€ Configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Generic â€” no hardcoded customer/AIDP-instance config. Set at runtime
# (job_migrate_from_workflow overrides these and recomputes the URLs). Profile
# defaults to "DEFAULT"; lake/workspace/cluster are required (no default).
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
DOWNLOAD_META_URL = None  # recomputed once DATALAKE_OCID/WORKSPACE_ID are set
UPLOAD_FOLDER_URL = None
OCI_PROFILE = "DEFAULT"
DEFAULT_CLUSTER = None
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OUTPUT_FOLDER = "agent-migrated"

# â”€â”€â”€ OCI Auth â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_signer():
    _, s = get_oci_signer(OCI_PROFILE)
    return s

SIGNER = None
def signer():
    global SIGNER
    if not SIGNER:
        SIGNER = get_signer()
    return SIGNER

# â”€â”€â”€ AIDP Workspace Operations â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def download_notebook(notebook_path: str, local_path: str) -> bool:
    """Download a notebook from AIDP workspace to local temp file."""
    headers = {"Content-Type": "application/json", "path": notebook_path, "type": "NOTEBOOK"}
    resp = http_requests.post(DOWNLOAD_META_URL, auth=signer(), headers=headers, data="")
    resp.raise_for_status()
    par_url = resp.json().get("parUrl")
    if not par_url:
        return False
    resp = http_requests.get(par_url)
    resp.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(resp.content)
    return True


def create_folder(folder_path: str):
    """Create a folder on the AIDP workspace."""
    http_requests.post(UPLOAD_FOLDER_URL, auth=signer(),
        headers={"path": folder_path, "type": "FOLDER", "description": "Agent migration output"},
        data="{}")


def upload_to_workspace(session: AIDPSession, content: str, remote_path: str) -> bool:
    """Upload content to workspace via the executor (since direct upload API has issues)."""
    # We'll write files via the executor since /Workspace is writable
    # For large content, we base64 encode and decode on the cluster
    import base64
    b64 = base64.b64encode(content.encode('utf-8')).decode('ascii')

    # Split into chunks if too large for a single cell
    MAX_CHUNK = 50000  # Safe size for a single cell execution
    if len(b64) > MAX_CHUNK:
        # Write in chunks
        chunks = [b64[i:i+MAX_CHUNK] for i in range(0, len(b64), MAX_CHUNK)]
        # First chunk creates the file
        code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
print('chunk 0 written')
"""
        asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        # Append remaining chunks
        for i, chunk in enumerate(chunks[1:], 1):
            code = f"""
import base64, builtins
with builtins.open('{remote_path}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
print('chunk {i} written')
"""
            asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        return True
    else:
        code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
print('written')
"""
        result = asyncio.get_event_loop().run_until_complete(session.execute(code, timeout=30))
        return result.get("status") == "ok"


# â”€â”€â”€ OpenAI GPT-5.5 Calls â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

CLIENT = None
def model_client():
    global CLIENT
    if not CLIENT:
        CLIENT = OpenAIModelCompatClient(api_key=OPENAI_API_KEY or None)
    return CLIENT


# â”€â”€â”€ fixup_cell Feature State â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_cell_history: list = []          # job-wide flat list, cleared per job
_current_cell_notes: list = []    # notes for the current cell being processed
_replay_cell_fn = None            # registered from job_migrate.py (avoids circular import)
_current_task_key: str = ""       # set by process_job() before each task
_job_manifest: dict = {}          # set by job_migrate.py
_current_task_params: dict = {}   # merged params (task + job override), set per task
_current_compactor = None         # ContextCompactor for the active call_opus_with_tools call
_compactor_history: list = []     # all compactors created this session (for cross-call file retrieval)

# â”€â”€â”€ Pip Install Tracking â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_installed_packages: set = set()  # package names installed via pip during migration, cleared per job
_table_lookup_cache: dict = {}   # table name -> result, avoids repeated lookups within a run
_path_explore_cache: dict = {}   # explored path -> result, avoids repeated explore_path calls

# Regex patterns for detecting pip install commands
_PIP_SHELL_RE = re.compile(
    r'(?:^|\n)\s*(?:!|%)?pip\s+install\s+(.+)', re.IGNORECASE)
_PIP_SUBPROCESS_RE = re.compile(
    r'subprocess\.(?:check_call|run|call)\s*\(\s*\[.*?["\']install["\']\s*,\s*(.+?)\]',
    re.IGNORECASE | re.DOTALL)

_PIP_FLAGS = {'-q', '--quiet', '-U', '--upgrade', '--no-cache-dir', '--user',
              '--force-reinstall', '--no-deps', '--pre', '--no-warn-script-location'}
_PIP_FLAGS_WITH_ARG = {'--index-url', '-i', '--extra-index-url', '--trusted-host',
                       '-t', '--target', '--prefix', '-c', '--constraint'}


def extract_pip_packages(code: str) -> list:
    """Extract package names from pip install commands in code.
    Returns list of package specifiers (e.g. ['pandas', 'scikit-learn==1.3.0'])."""
    packages = []

    # Shell-style: pip install pkg1 pkg2, %pip install pkg, !pip install pkg
    for m in _PIP_SHELL_RE.finditer(code):
        args = m.group(1).strip()
        # Stop at comment or line continuation
        args = args.split('#')[0].strip()
        packages.extend(_filter_pip_args(args.split()))

    # subprocess.check_call([sys.executable, "-m", "pip", "install", "pkg"])
    for m in _PIP_SUBPROCESS_RE.finditer(code):
        raw = m.group(1).strip()
        # Extract quoted strings from the list literal
        tokens = re.findall(r'["\']([^"\']+)["\']', raw)
        packages.extend(_filter_pip_args(tokens))

    return packages


def _filter_pip_args(tokens: list) -> list:
    """Filter out pip flags, keep only package specifiers."""
    result = []
    skip_next = False
    for tok in tokens:
        if skip_next:
            skip_next = False
            continue
        if tok in _PIP_FLAGS:
            continue
        if tok in _PIP_FLAGS_WITH_ARG:
            skip_next = True
            continue
        if tok.startswith('-'):
            continue
        # Skip sys.executable, python references
        if tok in ('sys.executable', 'python', 'python3'):
            continue
        if tok.strip():
            result.append(tok.strip())
    return result


def clear_installed_packages():
    """Clear the set of tracked pip-installed packages. Called per job."""
    _installed_packages.clear()
    _table_lookup_cache.clear()
    _path_explore_cache.clear()


def get_installed_packages() -> list:
    """Return sorted list of pip-installed packages tracked during this job."""
    return sorted(_installed_packages)


def register_replay_fn(fn):
    global _replay_cell_fn
    _replay_cell_fn = fn


def set_current_task_key(task_key: str):
    """Set the current task key for get_job_parameters tool."""
    global _current_task_key
    _current_task_key = task_key


def set_job_manifest(manifest: dict):
    """Set the job manifest for get_job_parameters tool."""
    global _job_manifest
    _job_manifest = manifest


def set_current_task_params(params: dict):
    """Set the merged parameters for the current task (called from process_job)."""
    global _current_task_params
    _current_task_params = dict(params) if params else {}


async def _summarize_cell_code(code: str) -> str:
    """Quick OpenAI model call â€” 1-sentence description of what the code does."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(None, lambda: model_client().messages.create(
            model=OPENAI_MODEL,
            max_tokens=80,
            timeout=30,
            messages=[{"role": "user", "content":
                f"In one sentence, what does this Python/PySpark code do?\n\n```python\n{code[:1000]}\n```\n\nReply with just the sentence, no preamble."}]
        ))
        return resp.content[0].text.strip()
    except Exception:
        return code.strip()[:120].replace("\n", " ")


def _format_history_context() -> str:
    """Format last 100 history entries as context for OpenAI model."""
    if not _cell_history:
        return ""
    n = len(_cell_history)
    entries = _cell_history[-100:]
    lines = [
        f"=== JOB EXECUTION HISTORY (last {len(entries)} of {n} cells) ===",
        "Context: Cells execute sequentially across notebooks. Child notebooks inlined via %run "
        "execute in the parent's kernel namespace. Use get_cell_history/get_history_entry to inspect, "
        "fixup_cell(start_index, why) to rewind and replay, make_note to annotate this cell.",
        "",
    ]
    for e in entries:
        child_tag = "[child] " if e.get("is_child") else ""
        nb = e["notebook_path"]
        note_tag = f" | note: {e['last_note'][:60]}" if e.get("last_note") else ""
        lines.append(
            f"[{e['index']}] {child_tag}{nb} cell {e['cell_idx']} | {e['status'].upper()} | "
            f"{e['summary'][:100]}{note_tag}"
        )
    return "\n".join(lines)


ANALYSIS_PROMPT = """You are a Databricks-to-Oracle-AIDP migration analyst. Oracle AIDP runs open-source Apache Spark (Scala 2.12.18, Java 17 GraalVM, Python 3.11). It does NOT include any Databricks proprietary runtime, libraries, or services.

## AIDP Environment (confirmed by testing):
- Pre-installed JARs: Delta Lake 3.2, Avro, OCI HDFS connector (BmcFilesystem)
- Installed JARs: Hudi 0.15.0, FeatureLib, FeatureLib2, MessageParser, DecryptUDF, Scala Logging
- Pre-installed Python: pandas 2.3.3, numpy 2.4.2, requests, oci, nbformat, ray 2.54, slack_sdk, boto3, delta, IPython
- Installed Python (via requirements.txt): matplotlib, scikit-learn, xgboost, seaborn, plotly, tqdm, etc.
- MLflow may NOT be pre-installed â€” install with pip install mlflow (no version pin) if needed
- Auth: OCI API key via CLI config file at /Workspace/<oci-config-workspace-path> (DEFAULT profile).
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() â€” resource principal has known
  failure modes on AIDP and MUST NOT be used. Use API key + Signer pattern always.
- Filesystem: /Workspace/ is local, oci:// paths work via BmcFilesystem (configured with API key)
- SparkSession pre-initialized as `spark`, SparkContext as `sc`
- Libraries should be installed via cluster libraries section (not pip install at runtime)
- spark.jars is immutable at runtime
- aidp_compat installed as a cluster library (just import it, no sys.path needed)

## AIDP Compatibility Features (DO NOT flag these):
- display() - native on AIDP, Spark DataFrames ONLY. For other types, use the
  library's NATIVE display function:
    Spark DataFrame:    `df.display()`   -> `display(df)`           (AIDP native)
    Pandas DataFrame:   `df.display()`   -> `df` as last expression (auto-render)
    matplotlib figure:  `fig.display()`  -> `plt.show()`            (matplotlib native)
    plotly figure:      `fig.display()`  -> `fig.show()`            (plotly native)
    PIL Image:          `img.display()`  -> `img.show()`            (PIL native)
    numpy / dict / etc: `obj.display()`  -> `print(obj)`            (Python native)
  Rule: NEVER pass non-Spark objects to AIDP's display(). Use the library's own
  display function for the type at hand.
  NEVER add a `DataFrame.display = _display_patch` monkey-patch (toPandas-based shims are slow).
- dbutils.fs - available via aidp_compat
- aidp_dbutils - the customer's existing shim
- %run magic - available on AIDP
- %sql magic - available on AIDP
- %scala magic - AIDP supports Scala
- oidlUtils - AIDP native
- Open Source Delta Lake and Delta Sharing
- Reading from Volume paths
- pandas, numpy - pre-installed

## INCOMPATIBLE FEATURES - Flag with severity:

### HIGH severity:
- Any Databricks internal API reference
- Any AWS Cloud references (S3 paths, boto3 for AWS-specific ops, Glue, Redshift)
- dbutils.notebook.getContext().notebookPath
- Reading and writing same path in single operation (works in Databricks, fails in OSS Spark)
- UDFs defined in one language used in another (AIDP has separate Spark sessions for Python & Scala)
- Databricks Connect, Databricks SDK
- Unity Catalog three-level namespace (catalog.schema.table)
- OPTIMIZE / ZORDER (Databricks-only)
- Databricks Feature Store, AutoML

### MEDIUM severity:
- MLflow Databricks-specific integrations
- spark.databricks.* configurations
- Photon engine references
- Structured Streaming Databricks-specific sources/sinks

### LOW severity:
- dbutils.widgets -> oidlUtils.parameters.getParameter (comment out original, add replacement)
- Custom utility notebook references (available in workspace)
- get_glue_table_s3_location
- DBFS paths (work after Volume migration)

## FEATURE MAPPINGS:
- dbutils.exit -> oidlUtils.notebook.exit
- dbutils.notebook.run -> oidlUtils.notebook.run
- dbutils.jobs.taskValues.set -> oidlUtils.parameters.setTaskValue
- dbutils.jobs.taskValues.get -> oidlUtils.parameters.getParameter
- dbutils.widgets.get("name") -> oidlUtils.parameters.getParameter("name", "")
- dbutils.widgets.text/dropdown/combobox/multiselect -> comment out (not needed in AIDP)

## REPORT FORMAT:
1. **Compatibility Status**: GREEN / YELLOW / RED
2. **Notebook Description**: What does this notebook do (1-2 sentences)
3. **Notebooks Called**: All notebooks called via %run, dbutils.notebook.run, or APIs
4. **Jobs Called**: All jobs triggered via Databricks Job API
5. **Data Dependencies**: ALL tables, storage paths, volumes - mark READ or WRITE
6. **Incompatible Code**: Each issue with cell number, code snippet, severity, and migration action. Do NOT highlight markdown cells. Mask any sensitive tokens/passwords.
7. **External Dependencies**: imports, libraries, JARs, APIs that may not be available
8. **Migration Challenges**: Anything needing additional work beyond code changes
9. **Migration Summary Table**: | Notebook | Status | Issues | Effort (Low/Medium/High/Very High) |

COMMENT-AWARENESS: Only analyze ACTIVE (uncommented) code. Ignore patterns inside:
- Python: # line comments, triple-quoted strings (\"\"\"...\"\"\", '''...''')
- Scala: // line comments, /* ... */ block comments
- SQL: -- line comments
Commented-out code (e.g. "# import boto3", "# s3://old-bucket/data") is dead code from previous
migrations or developer cleanup â€” do NOT flag it as incompatible or include it in your report.

Be EXHAUSTIVE about ACTIVE code. Check every uncommented cell, import, path, and API call."""


MIGRATION_PROMPT = """You are migrating a Databricks notebook to Oracle AIDP (Python 3.11).

You will receive the notebook AND its analysis report. Produce a JSON object with:

1. "cells": Array of migrated cells, each with:
   - "cell_type": "code" | "markdown" | "raw"
   - "source": the migrated source code
   - "classification": "READ_ONLY" | "WRITE" | "NOTIFICATION" | "SKIP"

   Classification rules (be PRECISE):
   READ_ONLY: imports, reads, queries, transforms, display, function/class definitions, variable assignments, print, F.to_json(), spark.read.*, createOrReplaceTempView, .show(), .count()
   WRITE: df.write.*, saveAsTable, insertInto, CREATE TABLE (non-temp), DROP TABLE, DELETE, INSERT, os.remove, shutil.rmtree, put_object, delete_object
   NOTIFICATION: Slack SDK, email, webhook posts to notification services
   SKIP: empty, markdown, raw cells

2. "migration_notes": Array of strings describing each change

## Migration rules:
- First code cell must be:
  ```
  from aidp_compat import dbutils, display, displayHTML, sql, translate_path
  ```
- %pip install: Comment out original with "# AIDP: install via cluster libraries", then add subprocess pip install so library is available during migration testing
- Convert %sh to subprocess.run
- Replace `from aidp_dbutils import _DBUtils` / `dbutils = _DBUtils(oidlUtils)` with the aidp_compat import
- dbutils.widgets.get("name") -> oidlUtils.parameters.getParameter("name", "")
  Comment out the original line and add the oidlUtils replacement below it.
- dbutils.widgets.text/dropdown/combobox/multiselect: Comment out (not needed in AIDP).
- Comment out spark.databricks.* configs
- spark and sc are pre-initialized on AIDP
- /Workspace/ paths work as-is
- Mask sensitive tokens/passwords with placeholder comments
- When you modify a line, comment out the original above it with "# Original", and tag the new line with "# Oracle tool modification:"
- When you add a new line, tag it with "# Oracle tool modification:"
- COMMENTED-OUT CODE: Do NOT migrate, modify, or translate code inside comments. Commented-out
  s3:// paths, boto3 imports, dbutils calls, etc. are dead code â€” leave them as-is.
  Only migrate ACTIVE (uncommented) code.

CRITICAL â€” TABLE READS STAY AS TABLE READS (UNCONDITIONAL):
spark.read.table(X), spark.table(X), and spark.sql("...SELECT...FROM X") must NEVER be
rewritten as spark.read.parquet(...), spark.read.format(...).load(...), spark.read.load(...),
spark.read.csv(...), spark.read.json(...), or any other path-based read. AIDP supports
Unity Catalog identifiers identically to Databricks â€” the table read IS the migration.
Forbidden regardless of try/except wrapping, regardless of whether you obtained a path
from DESCRIBE FORMATTED, and regardless of whether the table looks "missing" at analysis
time. If a table is broken, leave the table read as-is and let the runtime error surface.

CRITICAL â€” DESCRIBE DETAIL is Delta-only and FAILS on AIDP for non-Delta tables:
On AIDP, `spark.sql("DESCRIBE DETAIL <tbl>")` raises "Operation not allowed: DESCRIBE
DETAIL is only supported for Delta tables" whenever the underlying table is parquet,
ORC, CSV, JSON, Iceberg, or any non-Delta format. Customer codebases commonly call
DESCRIBE DETAIL on parquet tables (e.g. SaveTableUtils.createTable looks up the existing
location before appending). Rewrite to DESCRIBE EXTENDED, which works universally on
AIDP â€” BUT note the result schema differs and the downstream access must change too:

  DESCRIBE DETAIL    â†’ returns 1 Row with NAMED fields:  row['location'], row['format'], â€¦
  DESCRIBE EXTENDED  â†’ returns N Rows with col_name/data_type/comment columns;
                       Location appears as the row where col_name == 'Location'.

Rewrite pattern (apply BOTH the SQL and the result-access change together):

  # Before
  info = spark.sql(f"DESCRIBE DETAIL {tbl}").collect()[0]
  path = info['location']

  # After  -- Oracle tool modification: DESCRIBE DETAIL â†’ DESCRIBE EXTENDED (AIDP-safe)
  rows = spark.sql(f"DESCRIBE EXTENDED {tbl}").collect()
  path = next(r['data_type'] for r in rows if r['col_name'] == 'Location')

Apply this rewrite IN-PLACE wherever you find `DESCRIBE DETAIL` â€” inside helper function
bodies (createTable, etc.) and inline at call sites. NEVER leave `DESCRIBE DETAIL` in
migrated code. Do NOT rewrite `DESCRIBE EXTENDED`, `DESCRIBE FORMATTED`, or plain
`DESCRIBE` â€” only `DESCRIBE DETAIL`.

CRITICAL â€” AIDP LOCATION EXTRACTION (for code that returns a LOCATION STRING, not data):

SCOPE â€” exactly TWO kinds of locations get touched. Anything else stays byte-identical.

A) FUNCTION DEFINITION SITES â€” where a helper is `def`-ed:
       def get_glue_table_s3_location(database, table):
           <body that calls boto3 Glue / does inline DESCRIBE FORMATTED / etc.>
   ACTION: replace ONLY the function body with the canonical AIDP snippet below.
   KEEP the function name, signature, and parameter names IDENTICAL. The function
   still RETURNS A PATH STRING â€” just derived from AIDP DESCRIBE FORMATTED instead
   of AWS Glue.

B) INLINE EXTRACTION BLOCKS â€” where DESCRIBE FORMATTED is written out in the cell
   directly, no helper function:
       df = spark.sql("DESCRIBE FORMATTED ...")
       loc = df.filter(...).collect()[0][0]
   ACTION: replace those extraction lines with the canonical snippet.

ABSOLUTELY FORBIDDEN â€” these are the over-migration mistakes we are correcting:

  - REWRITING A CALL SITE. A line like
        events_table_path = get_glue_table_s3_location('analytics', 'events_data')
    MUST STAY IDENTICAL. NEVER comment it out and replace with
        events_table_path = "analytics.events_data"
    or any other hardcoded catalog identifier. The call expression is correct
    as-is â€” the FUNCTION's body is what gets fixed (case A above), not the call.

  - HARDCODING A 3-PART CATALOG NAME ("db.schema.table") AS THE VALUE OF A
    *_path / *_location / *_uri / *_s3_location variable. These variables are
    documented to hold PATH STRINGS; downstream code like
        spark.read.parquet(f'{events_table_path}/load_date=...')
    depends on them being paths. Hardcoding a catalog identifier corrupts every
    downstream consumer silently.

  - "BECAUSE AIDP READS FROM CATALOG, REPLACE THE CALL WITH A CATALOG REFERENCE."
    This reasoning is WRONG. AIDP returns the catalog table's LOCATION via
    DESCRIBE FORMATTED, which IS A PATH STRING. The function's return-contract
    is unchanged. Only its implementation moves to DESCRIBE FORMATTED.

WHEN THE FUNCTION DEFINITION IS NOT IN THIS CELL (only call sites are visible):
   LEAVE THE CALL SITES UNCHANGED. Do not touch them. The function is defined in
   a %run-ed dep or imported module; that definition gets migrated when the dep
   is processed. You are NOT allowed to rewrite a call site to compensate for
   what the dep migration may or may not have done. If the dep is broken, that
   is the dep's bug to fix, not the caller's.

WHEN A HELPER FUNCTION IS CALLED BUT NEITHER DEFINED LOCALLY NOR PROVIDED BY A
KNOWN DEP: insert a canonical inline DEFINITION at the top of THIS cell (using
the snippet below), with the SAME name and signature as the call expects. The
call site itself still stays unchanged.

Canonical AIDP location-extraction snippet (the body of the function definition,
or the replacement for an inline extraction block; argument names adjusted to
match the surrounding signature):

      # Oracle tool modification: AIDP location extraction (DESCRIBE FORMATTED)
      rows = spark.sql(f"DESCRIBE FORMATTED `{database}`.`{table}`").collect()
      location = next(
          (r[1].strip() for r in rows if r[0].strip().lower() == "location"),
          None,
      )
      if not location:
          raise ValueError(f"Location not found for {database}.{table}")
      return location

Snippet rules:
  - BACKTICKS around `{db}`.`{tbl}` for reserved-word safety.
  - POSITIONAL row[0] / row[1] access â€” AIDP's DESCRIBE FORMATTED column names
    differ slightly from Databricks; positional access is robust.
  - CASE-INSENSITIVE match (r[0].strip().lower() == "location") â€” AIDP row
    labels vary in case.

This pattern is ONLY for code that returns a LOCATION STRING. FORBIDDEN to
apply to spark.read.table() / spark.table() / spark.sql("...FROM...") data
reads â€” those stay as table reads (see the unconditional rule above).

Return ONLY valid JSON: {"cells": [...], "migration_notes": [...]}"""


FIX_PROMPT = """You are fixing a PySpark , Scala or SQL cell that failed on OCI AIDP.
Keep the fix minimal. Return ONLY the fixed code, no markdown.

SCYLLADB â†’ AIDP METASTORE (applies when the failing cell uses Cassandra/Scylla patterns):
The migration policy is a TEMPORARY mapping â€” every ScyllaDB keyspace is mirrored to the
AIDP metastore as a schema named `scylla_<keyspace>` (original keyspace prefixed with
`scylla_` for identification); every Scylla table is registered as a Spark table with the
SAME table name. Schema = `scylla_<keyspace>`; table name preserved; reference shape
spark.sql("â€¦  scylla_$keyspace.$table  â€¦").
Example: Scylla `sample_keyspace.foo` â†’ AIDP metastore `scylla_sample_keyspace.foo`.

If the failure traces back to a Cassandra pattern that wasn't fully migrated, apply the
substitutions below (preserve %scala magic if present â€” do NOT port to Python):
  - REMOVE: CassandraConnector(...).withSessionDo { session => BODY } â†’ inline BODY at top level
  - REMOVE: any com.datastax.spark.connector.* imports; remove Cluster/Session construction
  - session.execute("DROP TABLE $ks.$t")          â†’ spark.sql(s"DROP TABLE IF EXISTS scylla_$ks.$t")
  - session.execute("TRUNCATE $ks.$t")            â†’ spark.sql(s"TRUNCATE TABLE scylla_$ks.$t")
  - session.execute("DELETE FROM $ks.$t WHERE â€¦") â†’ spark.sql(s"DELETE FROM scylla_$ks.$t WHERE â€¦")  // Delta only
  - session.execute("UPDATE $ks.$t SET â€¦ WHERE â€¦")â†’ spark.sql(s"UPDATE scylla_$ks.$t SET â€¦ WHERE â€¦") // Delta only
  - session.execute("INSERT INTO $ks.$t (â€¦) VALUES (â€¦)") â†’ spark.sql(s"INSERT OVERWRITE scylla_$ks.$t VALUES (â€¦)")
  - session.execute("SELECT â€¦ FROM $ks.$t â€¦")     â†’ spark.sql(s"SELECT â€¦ FROM scylla_$ks.$t â€¦").collect()
  - SELECT â€¦ FROM system_schema.tables WHERE keyspace_name='$ks'
                                                  â†’ spark.sql(s"SHOW TABLES IN scylla_$ks").collect().map(_.getString(1))
  - spark.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->ks,"table"->t)).load()
                                                  â†’ spark.read.table(s"scylla_$ks.$t")
  - df.write.format("org.apache.spark.sql.cassandra").options(...).save()
                                                  â†’ df.write.mode("overwrite").saveAsTable(s"scylla_$ks.$t")
  - PreparedStatement / BoundStatement â†’ strip wrapper, build SQL string for spark.sql (prefix schema with scylla_) or use DataFrame write
  - BEGIN BATCH â€¦ APPLY BATCH â†’ unroll to sequential spark.sql(â€¦) calls
  - ALLOW FILTERING â†’ drop the clause
  - USING TTL / USING TIMESTAMP / lightweight txn IF EXISTS on UPDATE/INSERT â†’ call make_note (no equivalent)

WRITE-MODE rule: ALL writes to a Scylla-mirrored table use .mode("overwrite") (DataFrame) or
INSERT OVERWRITE (SQL). This is temporary â€” comment every write with:
  // Oracle tool modification: write mode forced to "overwrite" (was Cassandra upsert).
  // Temporary â€” restore original semantics when Scylla becomes directly reachable on AIDP.

EXISTENCE GUARDS: if the failure is "Schema not found" or "Table not found" on a Scylla-
mirrored keyspace/table, wrap the offending op with the guard. The schema name is the
keyspace prefixed with `scylla_`:
  val _schemas = spark.sql("SHOW SCHEMAS").collect().map(_.getString(0)).toSet
  if (!_schemas.contains(s"scylla_$keyspace")) { println(s"AIDP: schema 'scylla_$keyspace' not in metastore â€” skipping"); }
  else { /* original body */ }
For per-table failures, additionally check SHOW TABLES IN scylla_$ks before the per-table op.

After applying, the cell must have ZERO references to: cassandra, Cluster, Session,
SimpleStatement, withSessionDo, system_schema, com.datastax. If a Scylla-related failure
cannot be resolved with these substitutions, call make_note() with the specific blocker and
return the cell as-is.


TRINO / ATHENA (pyathena) â†’ AIDP SPARK CATALOG:
AWS Athena and Trino/Presto are external query engines NOT available on AIDP. Do NOT
install or connect to them (pyathena = AWS-only; trino needs an unreachable endpoint).
The same tables they query are registered in the AIDP Spark catalog â€” read via Spark.
Apply these substitutions (preserve %scala if present; default is Python):
  - REMOVE/comment the client imports: `from pyathena import connect`,
    `from pyathena.pandas.util import as_pandas`, `import trino` (+ any
    try/except `!pip install pyathena|trino` bootstrap â€” pip in cell code is forbidden).
  - REMOVE the connection object:
      pyathena: conn = connect(s3_staging_dir=..., region_name=...)
      trino:    conn = trino.dbapi.connect(host=..., port=..., catalog=..., schema=...)
  - REWRITE the query execution onto Spark, KEEPING the SQL text:
      pd.read_sql(QUERY, conn)            â†’ spark.sql(QUERY).toPandas()   # keep pandas shape
      cur=conn.cursor(); cur.execute(QUERY); cur.fetchall()
                                          â†’ spark.sql(QUERY).collect()
      as_pandas(cursor)                   â†’ spark.sql(QUERY).toPandas()
  - TABLE NAMES in QUERY: keep `schema.table` (it resolves in the AIDP catalog). For
    trino `catalog.schema.table`, DROP the leading trino catalog â†’ `schema.table`.
    (This is a TABLE READ â€” the "table reads stay as table reads" rule applies; never
    convert to a path read.)
  - SQL DIALECT: the QUERY is Presto/Trino/Athena SQL. Most ANSI SQL runs unchanged in
    Spark SQL. For engine-specific functions that error, translate to the Spark equivalent
    (e.g. approx_distinctâ†’approx_count_distinct, cardinalityâ†’size, cast/date_format diffs).
    `great_circle_distance(...)` has NO Spark builtin â€” reimplement via Haversine. If a
    function cannot be translated, call make_note() with the specific function.
  - VALIDATE first: confirm the referenced table exists (describe_table / search_catalog)
    before rewriting; if NOT found, make_note() and leave the cell as-is (do not invent a path).
  - DORMANT IMPORTS: if pyathena/trino are imported but never used to query (dead import),
    just comment the import line + make_note(); do NOT install, do NOT add a connection.
After applying, the cell must have ZERO references to: pyathena, trino, connect(,
s3_staging_dir, as_pandas. If it can't be resolved, make_note() and return the cell as-is.


CRITICAL â€” NO EXECUTION-TIME SCAFFOLDING IN THE MIGRATED NOTEBOOK:
The fix you write here is PERSISTED to the saved notebook forever. Apply ONLY the
documented migration substitutions (dbutils.* â†’ oidlUtils.*/aidp_compat.*, s3://â†’OCI,
%run / notebook.run path rewrite, comment-out unsupported APIs). FORBIDDEN even if it
would make the cell pass:
  - os.environ.get("AIDP_PARAMS") fallbacks. oidlUtils.parameters.getParameter() already
    reads AIDP_PARAMS internally. No parallel json.loads(os.environ['AIDP_PARAMS']).
  - "if not X: X = json.loads(...) / os.environ.get(...)" defensive defaults for values
    sourced from oidlUtils.parameters.* or dbutils.widgets.*.
  - **TABLE READS STAY AS TABLE READS â€” UNCONDITIONAL.** spark.read.table(X),
    spark.table(X), and spark.sql("...SELECT...FROM X") must NEVER be rewritten as
    spark.read.parquet(...), spark.read.format(...).load(...), spark.read.load(...),
    spark.read.csv(...), spark.read.json(...), or any other path-based read. AIDP
    supports Unity Catalog identifiers identically to Databricks â€” the table read
    IS the migration. Forbidden regardless of try/except wrapping, regardless of
    whether you obtained the path from DESCRIBE FORMATTED / describe_table tool,
    and regardless of whether the table looks "missing" or "empty" at probe time.
    If a table is broken, call make_note() and let the original Spark error fire.
  - "if len(df.columns) == 0" empty-schema rescue logic. Trust spark.read.table().
  - Inlining the body of a dbutils.notebook.run() / oidlUtils.notebook.run() target into
    the calling cell. Fix the call (path/args); never paste the child's cells inline.
  - sys.path.insert/append, !pip install, %pip install in cell code.
  - try/except blocks that silently swallow errors or substitute defaults for code that
    worked in Databricks.
  - **NEVER inline-define customer writer-wrapper functions** to "fix" a NameError.
    The wrapper-call redirect at exec-time rewrites literal db/bucket args (e.g.,
    `createTable(df, 't', 'analytics_db', ...)` â†’ `createTable(df, 't', 'oracle_ai_automation_overwrite', ...)`)
    BEFORE the call is sent to the kernel. If the wrapper function is missing
    (NameError), defining a fresh copy of the customer's `def createTable(...)`
    locally in the cell is FORBIDDEN â€” that copy is NOT what the call site sees
    (the cell-text redirect has already changed the arg), AND if the rewrite missed
    something, the inline copy writes to whatever database_name was passed. The
    forbidden names: createTable, createTableOld, createTableIntermediate,
    createTableIntermediateOld, createTableIntermediate_append, createTableIntermediate_1,
    createTableInFeatureLib, createTable_oracle, saveTable, writeTable, write_to_delta,
    process_source, drop_database, drop_table, delete_table. If any of these are
    missing at runtime, call make_note() describing the failure and leave the cell
    code unchanged. The dep needs to be re-loaded â€” that's a systemic recovery, not
    a per-cell fix.

If the documented substitutions cannot make the cell pass, call make_note() describing
the failure and return the migrated code as-is. A failing cell with a clear note is
better than a "passing" cell with hidden runtime scaffolding.

OCI PATH / FILESYSTEM OPERATIONS â€” ONE simple rule for BOTH Python and Scala:
- TABULAR data: use Spark (spark.read / df.write on "oci://...") â€” Spark wires BmcFilesystem
  internally and is always safe (interactive AND scheduled).
- ANY non-Spark filesystem op (existence check, listing, copy / delete / put objects, read a
  JSON config, etc.): ALWAYS use the OCI Python SDK with API-key auth. Do NOT use Hadoop
  FileSystem â€” in Scala it is unreachable (AIDP runs SPARK CONNECT: spark.sparkContext,
  spark.sessionState, and a fresh Configuration all fail), and in Python it can initialize
  differently at scheduled-job time. The OCI SDK works everywhere:
      import oci
      _oci_config = oci.config.from_file("/Workspace/<oci-config-workspace-path>", "DEFAULT")
      _oci_signer = oci.signer.Signer(
          tenancy=_oci_config["tenancy"], user=_oci_config["user"],
          fingerprint=_oci_config["fingerprint"],
          private_key_file_location=_oci_config["key_file"],
          pass_phrase=oci.config.get_config_value_or_default(_oci_config, "pass_phrase"),
      )
      _oci_client = oci.object_storage.ObjectStorageClient(config=_oci_config, signer=_oci_signer)
      _oci_namespace = _oci_client.get_namespace().data
      # then use _oci_client.get_object(...) / .list_objects(...) / .put_object(...)
- SCALA cell needing a non-Spark FS op: the OCI SDK is Python, so do the op in a %python block
  and bridge the result back to Scala (spark.conf.set/get for a scalar; a temp view for a
  list). NEVER use spark.sparkContext / spark.sessionState / jvm Hadoop FS in Scala.
- NEVER substitute boto3/S3 with raw Hadoop FS. If customer code uses jvm Hadoop FS for a
  non-Spark read/write, REWRITE it to the OCI Python SDK above.

CRITICAL â€” NEVER drop or delete a cell:
The migrated notebook MUST have the same cell count as the source. At most, comment out
the cell's contents with "# Oracle tool modification: <reason>" â€” but the cell itself
must remain in the notebook. Utility cells often define helpers (functions, constants)
that downstream cells or other notebooks (via %run) depend on; silently dropping such a
cell causes NameError in callers, often hours later in unrelated tasks. If you cannot
migrate a cell's logic, comment all of it out â€” never delete.

CRITICAL â€” Path rewriting safety for %run / dbutils.notebook.run / oidlUtils.notebook.run:
1. NEVER prepend the MIGRATED_BASE prefix to a path that already starts with it. If the
   target path already begins with the migrated-base prefix, use it as-is. Doubled
   prefixes like ".../notebooks/.../notebooks/..." are always wrong.
2. When matching a `%run` token (e.g., `M90`) against the MIGRATED DEPENDENCY PATHS list,
   match by EXACT basename equality only â€” never by prefix. A `%run M90` lookup must
   NOT resolve to the entry for `M9` and append the remainder, producing
   ".../M9.ipynb0.ipynb".
3. Every migrated `%run` path must end in exactly one ".ipynb" suffix. Patterns like
   ".ipynb<digit>.ipynb" or ".ipynb.ipynb" indicate a path construction bug â€” never emit.

CONDITIONAL â€” oidlUtils Java type coercion (only when this specific error appears):
If the failure output contains "Py4JException" with a message like
"Method <name>([class java.lang.String, class java.lang.Integer/Boolean/Double]) does not exist",
the JVM method requires a Java String. Cast the Python value to str:
  oidlUtils.parameters.setTaskValue("alert_promise_tables", str(value))
Do NOT preemptively cast values â€” only apply this fix when the Py4J type-mismatch error fires.

Key AIDP facts:
- aidp_compat is installed as a cluster library (just import it directly)
- spark (SparkSession) is pre-initialized. AIDP runs SPARK CONNECT: in %scala,
  `spark.sparkContext` and `sc` DO NOT EXIST (SparkSession has no `sparkContext`
  member â€” `value sparkContext is not a member of org.apache.spark.sql.SparkSession`).
  NEVER use `spark.sparkContext`, `sc`, or anything derived from them (e.g.
  `spark.sparkContext.hadoopConfiguration`, `new org.apache.hadoop.fs.Path(p).getFileSystem(
  spark.sparkContext.hadoopConfiguration)`). Use `spark` SQL/DataFrame APIs directly. There
  is NO Scala fallback for filesystem listing via sparkContext â€” do not attempt one.
- dbutils.fs.* in a %scala cell: `dbutils` is a Python object (aidp_compat) and is NOT in
  scope in the Scala kernel ("not found: value dbutils"). Do NOT reimplement it with jvm
  Hadoop FS / spark.sparkContext / spark.sessionState (ALL unavailable on AIDP Spark Connect
  Scala â€” forbidden above). If the FS operation can be expressed with Spark DataFrame/SQL
  APIs (spark.read/write for oci:// paths), do that. Otherwise do NOT comment out or delete
  the code and do NOT stub it â€” the symbol may be used by ANOTHER job and this is the shared
  migrated copy, so silently no-op'ing it there would break that job. Call make_note()
  describing the blocker and return the cell UNCHANGED; the runtime continues past a failed
  helper cell on its own.
- Scala (%scala) UDF with an `Any` parameter â€” `def f(x: Any)...` + `val u = udf(f(_:Any))`:
  AIDP Spark Connect's udf() CANNOT build an encoder for `Any` (fails with
  "[ENCODER_NOT_FOUND] Not found an encoder of the type Any to Spark SQL internal
  representation"). FIX: change the `Any` to the CONCRETE type the function actually
  operates on â€” infer it from the function body AND the call sites:
    â€¢ body does numeric work (e.g. `x.toString.toDouble`, compares to Double.MinValue) and
      the udf is applied to a numeric column/expression  â†’  `Double`
    â€¢ body does pure string ops and the column is a string  â†’  `String`
  Update BOTH the `def` signature and the `udf(f(_:<type>))` ascription, and PRESERVE the
  function's logic exactly. This is a TYPE-ANNOTATION fix, NOT a rewrite to native Spark
  functions (do not change the business logic). Example:
    def getTruncatedValue(num: Any): Double = {...}   ;  val u = udf(getTruncatedValue(_:Any))
      â†’ def getTruncatedValue(num: Double): Double = {...} ;  val u = udf(getTruncatedValue(_:Double))
  If the concrete type is genuinely ambiguous, make_note() for customer review rather than
  guess. (PYTHON UDFs are NOT affected â€” they declare only a return type and take
  dynamically-typed input, so there is no `Any` input-encoder; never apply this to Python.)
- /Workspace/ paths work for local file access
- AIDP does not support spaces in paths â€” replace spaces with underscores in ALL /Workspace/ paths
- dbutils.notebook.exit() -> oidlUtils.notebook.exit() â€” NEVER comment out or replace with pass
- notebook.run TIMEOUT: if the failure is "timeoutSeconds is null or empty" (AIDP
  CreateUtilsRun / InvalidParameter), the 2nd positional arg of notebook.run is the
  timeout in seconds and it is 0 or missing. AIDP REJECTS 0/null â€” set it to a positive
  value (3600, or the max supported). Applies to Scala and Python:
    oidlUtils.notebook.run(path, 3600, args)   // never 0
- notebook.run RESULT-AS-JSON: if the failure is from parsing the notebook.run result
  (e.g. `new JSONObject(oidlUtils.notebook.run(...))` throwing JSONException / "A JSONObject
  text must begin with", or getString on a missing key), the child may return empty/non-JSON
  on AIDP. Capture the result, then parse DEFENSIVELY: guard `raw.trim.startsWith("{")`, use
  optString/opt* (not getString), treat empty/non-JSON as success, and throw only on an
  explicit non-OK status. Do NOT silently swallow real errors. If the child gets the wrong
  params, check the notebook.run Map keys match the child's getParameter("<key>") names
  (read the child with read_notebook_source) â€” prefer real names over positional arg1/arg2.
- dbutils.widgets.get("name") -> oidlUtils.parameters.getParameter("name", "")
  Comment out the original line and add the oidlUtils replacement below it.
- oci:// paths work via BmcFilesystem (configured at cluster level with API key auth)
- OCI Python SDK auth: API key via CLI config file at /Workspace/<oci-config-workspace-path>
  (DEFAULT profile). Canonical init pattern when migrating code that creates OCI clients:
    import oci
    _oci_config = oci.config.from_file("/Workspace/<oci-config-workspace-path>", "DEFAULT")
    _oci_signer = oci.signer.Signer(
        tenancy=_oci_config["tenancy"], user=_oci_config["user"],
        fingerprint=_oci_config["fingerprint"],
        private_key_file_location=_oci_config["key_file"],
        pass_phrase=oci.config.get_config_value_or_default(_oci_config, "pass_phrase"),
    )
    _oci_client = oci.object_storage.ObjectStorageClient(config=_oci_config, signer=_oci_signer)
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() â€” resource principal has known
  failure modes on AIDP and MUST NEVER be used as a replacement for boto3/S3 client or for
  any new OCI client. If customer code already uses API key auth (oci.config.from_file
  pointing under /Workspace/), PRESERVE that init code unchanged.
- Libraries should be installed via cluster libraries section (not pip install at runtime)
- oidlUtils is a NATIVE AIDP module â€” it is pre-loaded in every kernel. Do NOT import it.
  No "from aidp_compat import oidlUtils" or "import oidlUtils" â€” just use oidlUtils.xxx directly.
- COMMENTED-OUT CODE: Leave ALL commented-out code as-is. Do NOT uncomment, translate, or fix
  code inside comments (Python #, Scala //, /* */, SQL --, or triple-quoted strings).
  Do NOT call suggest_oci_path or explore_path on paths inside comments.
  Commented code is dead code from previous migrations â€” do NOT spend time on it.

CRITICAL â€” Path-prefix replacements MUST be idempotent:
NEVER write an unconditional `path = path.replace('/dbfs/', '/Volumes/.../dbfs/')` or
similar prefix-substitution .replace() call. .replace() is unguarded â€” if the path is
already translated, or fed through twice, the prefix appears mid-string and gets replaced
again, producing mangled paths like '/Volumes/.../Volumes/.../dbfs/...'.

API REMINDER â€” `.startswith()` and `.replace()` are NOT interchangeable:
  str.startswith(prefix) -> bool      (ONLY tests; does NOT modify the string)
  str.replace(old, new)  -> str       (returns a NEW string)
NEVER write `s = s.startswith("/dbfs/", "/Volumes/...")` â€” that passes the second
argument as the `start` index (must be int) and returns a bool. Always use
.startswith INSIDE an `if`, then build the new string explicitly.

FORBIDDEN:
  path = path.replace('/dbfs/', '/Volumes/default/default/dbfs/')        # non-idempotent
  path = path.startswith('/dbfs/', '/Volumes/default/default/dbfs/')     # TypeError â€” wrong API

CORRECT (use ONE of these):
  # Option A â€” startswith guard, then build the new path explicitly:
  if path.startswith('/dbfs/'):
      path = '/Volumes/default/default/dbfs/' + path[len('/dbfs/'):]

  # Option B (PREFERRED) â€” translate_path() is idempotent and handles all path types:
  from aidp_compat import translate_path
  path = translate_path(path)

Same rule for /mnt/, s3://, dbfs://, /Workspace/ prefix translations.

CRITICAL - OCI Path Resolution (only for s3://, s3a://, oci:// paths):
- OCI bucket/namespace resolution ONLY applies when the code contains s3://, s3a://, or oci:// paths.
- All other paths (/FileStore/, /dbfs/, dbfs:/, /mnt/) are local DBFS paths â€” use translate_path()
  to convert to /Volumes/... paths. Do NOT explore them in OCI object storage.
- S3 paths (s3://bucket/...) must be translated to OCI paths (oci://oci-bucket@namespace/...)
- The S3 bucket name maps to an OCI bucket name, and each OCI bucket has a specific namespace
- Use the suggest_oci_path tool to find the correct OCI path for any S3 path
- Use the explore_path tool to verify a path exists BEFORE writing code that reads from it
- Use run_on_cluster to test data availability, check schemas, debug, or validate that a variable
  is defined (run_on_cluster runs in the MAIN kernel and shares all variables from prior cells)
- IMPORTANT: Many OCI buckets are in DIFFERENT namespaces (cross-tenancy). The namespace in the OCI path
  must match where the data actually lives, which may NOT be the AIDP workspace namespace (<WORKSPACE_NAMESPACE>).
- If a path doesn't exist, use suggest_oci_path first to find the correct bucket+namespace.
- PATH NOT FOUND RULE: If the bucket exists but the sub-path does not, do NOT keep guessing other
  buckets or sub-paths. Stop after at most 2 explore_path attempts for the same data. If the data is
  not found, use make_note to record the missing path and continue with the code as-is.
  The data may not be migrated to OCI yet â€” that is OK, just note it and move on.
- CROSS-TENANCY ACCESS: In AIDP, cross-tenancy OCI buckets require a registered table (real or dummy)
  for access. Direct spark.read from oci://bucket@namespace/... will fail with BucketNotFound even if
  the bucket exists. Therefore:
  1. AIDP LOCATION EXTRACTION (for code that returns a LOCATION STRING â€” NOT for data reads).

     SCOPE â€” exactly TWO kinds of sites get touched. Anything else stays byte-identical.

     A) FUNCTION DEFINITION SITES â€” where a helper is `def`-ed:
            def get_glue_table_s3_location(database, table):
                <body that calls boto3 Glue / does inline DESCRIBE FORMATTED / etc.>
        ACTION: replace ONLY the function body with the canonical AIDP snippet
        below. KEEP the function name, signature, and parameter names IDENTICAL.
        The function STILL RETURNS A PATH STRING â€” just derived from AIDP
        DESCRIBE FORMATTED instead of AWS Glue.

     B) INLINE EXTRACTION BLOCKS â€” where DESCRIBE FORMATTED is written out in
        the cell, no helper function:
            df = spark.sql("DESCRIBE FORMATTED ...")
            loc = df.filter(...).collect()[0][0]
        ACTION: replace those extraction lines with the canonical snippet.

     ABSOLUTELY FORBIDDEN â€” these are the over-migration mistakes we are
     correcting:

       - REWRITING A CALL SITE. A line like
             events_table_path = get_glue_table_s3_location('analytics', 'events_data')
         MUST STAY IDENTICAL. NEVER comment it out and replace with
             events_table_path = "analytics.events_data"
         or any other hardcoded catalog identifier. The call expression is
         correct as-is â€” the FUNCTION's body is what gets fixed (case A above),
         not the call.

       - HARDCODING A 3-PART CATALOG NAME ("db.schema.table") as the VALUE of a
         *_path / *_location / *_uri / *_s3_location variable. These variables
         hold PATH STRINGS; downstream code like
             spark.read.parquet(f'{events_table_path}/load_date=...')
         depends on them being paths. Hardcoding a catalog identifier corrupts
         every downstream consumer silently.

       - "BECAUSE AIDP READS FROM CATALOG, REPLACE THE CALL WITH A CATALOG
         REFERENCE." This reasoning is WRONG. AIDP returns the catalog table's
         LOCATION via DESCRIBE FORMATTED, which IS A PATH STRING. The function's
         return-contract is unchanged; only its implementation moves to
         DESCRIBE FORMATTED.

     WHEN THE FUNCTION DEFINITION IS NOT IN THIS CELL (only call sites are
     visible): LEAVE THE CALL SITES UNCHANGED. The function is defined in a
     %run-ed dep or imported module; that definition gets migrated when the
     dep is processed. You are NOT allowed to rewrite a call site to compensate
     for what the dep migration may or may not have done.

     WHEN A HELPER FUNCTION IS CALLED BUT NEITHER DEFINED LOCALLY NOR PROVIDED
     BY A KNOWN DEP: insert a canonical inline DEFINITION at the top of THIS
     cell (using the snippet below), with the SAME name and signature the call
     expects. The call site itself still stays unchanged.

     Canonical AIDP location-extraction snippet (the body of the function
     definition, or the replacement for an inline extraction block; argument
     names adjusted to match the surrounding signature):

       # Oracle tool modification: AIDP location extraction (DESCRIBE FORMATTED on AIDP metastore)
       rows = spark.sql(f"DESCRIBE FORMATTED `{database}`.`{table}`").collect()
       location = next(
           (r[1].strip() for r in rows if r[0].strip().lower() == "location"),
           None,
       )
       if not location:
           raise ValueError(f"Location not found for {database}.{table}")
       return location

     Snippet rules:
       - BACKTICKS around `{db}`.`{tbl}` for reserved-word safety.
       - POSITIONAL row[0] / row[1] access â€” AIDP's DESCRIBE FORMATTED column
         names differ slightly from Databricks; positional access is robust.
       - CASE-INSENSITIVE match (r[0].strip().lower() == "location") â€” AIDP
         row labels vary in case.

     Comment out (do not delete) the original boto3 imports and Glue client
     setup with "# Oracle tool modification: AWS SDK not available on AIDP".

     THIS PATTERN IS ONLY FOR PATH-STRING-RETURNING CODE. FORBIDDEN to apply to
     spark.read.table() / spark.table() / spark.sql("...FROM...") data reads â€”
     those stay as table reads (see the unconditional rule above).
  2. If search_catalog says a table does not exist, do NOT explore OCI paths for that data â€” direct
     bucket access will fail anyway. Use make_note to record the missing table and keep original code.
  3. Never hardcode oci:// paths as a replacement for table-based data access.
- SEARCH_CATALOG IS DEFINITIVE: If search_catalog returns no results, the table does NOT exist.
  Do NOT retry with name variations, different schemas, partial names, or explore_path.
  Tables match exactly or they don't exist. Record the missing table with make_note and move on.

CRITICAL - ROOT CAUSE IN PREVIOUS CELL:
If the error is caused by a PREVIOUS cell being incorrectly migrated (e.g. notebook.exit() was
commented out or replaced with pass, causing downstream cells to execute in unreachable code paths),
use fixup_cell to rewind to that cell and fix the root cause. Do NOT work around it in the current
cell â€” that creates fragile patches and wastes fix attempts on cascading failures.

CRITICAL - Do NOT:
- Generate code that calls dbutils.notebook.run() on non-migrated original Databricks paths
- Generate Slack webhook calls or external notification code (replace with a pass comment)
- REVERT previous fixes listed in the PREVIOUS FIXES section - those changes were validated and must be preserved

CRITICAL - DO NOT CHANGE CUSTOMER CODE LOGIC:
- Do NOT convert pandas to PySpark (e.g. pd.read_csv â†’ spark.read.format('csv') is WRONG)
- Do NOT convert PySpark to pandas
- Do NOT change data processing logic, algorithms, or library choices
- Do NOT remove or stub code because you think a variable is "unused" or "dead code".
  You do NOT have full visibility into how variables are used across all cells and notebooks.
- Do NOT replace file reads (pickle.load, open, pd.read_csv, etc.) with hardcoded values.
  If the file path needs translation, translate the path and keep the read.
- Do NOT invent problems that don't exist. Do NOT add workarounds for hypothetical issues
  (e.g. "unmocking" libraries, clearing sys.modules, invalidating caches, writing to /tmp
  then copying to /Volumes for "FUSE consistency") unless the ACTUAL error requires it.
  FUSE issues are resolved on AIDP â€” write directly to /Volumes.
- Only fix the specific error â€” do not rewrite surrounding code
- Python/pandas version upgrades are OK (e.g. df.append â†’ pd.concat for pandas 2.x)

CRITICAL - CODE QUALITY:
- No TODO comments, no placeholder code, no "implement later" stubs
  (EXCEPTION: unfixable Databricks infrastructure â€” see UNFIXABLE DATABRICKS INFRASTRUCTURE section above)
- No fallback/hack workarounds - fix the root cause
- No try/except that silently swallows errors without logging
- Every fix must be complete and production-ready
- When you modify a line, comment out the original above it with "# Original", and tag the new line with "# Oracle tool modification:"
- When you add a new line, tag it with "# Oracle tool modification:"

# --- FUSE WORKAROUNDS (DISABLED 2026-04-11: AIDP FUSE issues resolved) ---
# Uncomment if FUSE consistency issues resurface.
# CRITICAL - AIDP SAFE I/O (aidp_compat v0.4.3):
# - For pickle write+read: use safe_pickle_dump() and safe_pickle_load() from aidp_compat
# - For parquet overwrite-same-path: use safe_write_parquet() or safe_read_modify_write_parquet()
# - For saveAsTable overwrite: use safe_save_as_table() (caches before overwrite)
# - For pandas CSV write: use safe_pandas_to_csv()
# - These handle AIDP /Volumes FUSE consistency (write-then-read delay) automatically
# --- END FUSE WORKAROUNDS ---

EXECUTION CONTEXT: Fixing a cell in a sequential kernel. The job execution history (last 100 cells)
is shown above. If the root cause is in an earlier cell or child notebook, use get_cell_history
to identify it, then fixup_cell(start_index, why) to rewind and replay. Verify the replay sequence
is idempotent before calling fixup_cell (no double file writes, no duplicate external calls).
Use make_note to annotate concerns about this cell.
WHEN TO REWIND: If this is attempt 7+ and the root cause appears to be upstream, prefer fixup_cell
over continuing to accumulate workarounds for this cell.

CRITICAL - HOW TO RETURN YOUR ANSWER:
You MUST call the submit_code tool with your final Python code. Do NOT return code as plain text.
The code field must contain ONLY valid executable Python. No markdown, no prose, no explanations.
Do NOT add migration debug comments, fix-log annotations, or notes about cluster issues into the
submitted code. Use make_note for that. The delivered code must look like production code.

UNFIXABLE DATABRICKS INFRASTRUCTURE â€” stub immediately, do NOT keep retrying:
If the error is caused by Databricks-specific infrastructure with NO AIDP equivalent,
do NOT attempt another fix on the call itself. Instead:
1. Comment out the original code (preserve as reference)
2. Stub ALL return values so downstream cells get the variables they expect
3. print("STUBBED: <what> â€” original code preserved in comments above")
4. make_note("STUBBED: <what> â€” <why unfixable on AIDP>")

Detect by ANY of these signals:
- Direct Databricks REST API calls: requests.post/get to /api/2.0/jobs/*, /api/2.0/clusters/*
- Hardcoded Databricks job_id (large integer like 938951762375103) in any function call
- Wrapper functions that trigger Databricks jobs: run_job(), trigger_job(), submit_job(), etc.
- AWS Secrets Manager / boto3.client('secretsmanager')
- Databricks cluster policies / dbutils.secrets
- Databricks-specific webhooks or internal URLs

Example:
  # --- STUBBED: Databricks job trigger not available on AIDP ---
  # Original:
  # job_result = run_job({"run_name": model_name, "job_id": 938951762375103, ...})
  job_result = {"status": "STUBBED", "note": "Databricks job trigger not available on AIDP"}
  print("STUBBED: Databricks job trigger â€” original code in comments above")
If unsure what downstream cells need, use get_cell_history to check before stubbing.

KNOWN AIDP ERROR PATTERNS â€” fix these directly, don't investigate:
- "spark_catalog" doesn't exist / AnalysisException: catalog spark_catalog:
  Replace spark_catalog.x.y with x.y throughout the cell
- Table not found / AnalysisException on table lookup:
  AIDP uses 3-part names (catalog.schema.table) with "default" as the catalog.
  If a table name is 2-part (schema.table), try default.schema.table.
  If a table name is 3-part with catalog `main` (or any source catalog that does not exist on
  AIDP), replace ONLY the catalog with `default`, keeping schema and table unchanged
  (e.g. main.sample_schema.t -> default.sample_schema.t). These are the ONLY fixes.
  Do NOT explore, search, or guess alternative table names. Tables match exactly or they don't exist.
  If the 3-part name also fails, use make_note to record the missing table and keep original code.
- ModuleNotFoundError / No module named:
  Do NOT run pip install via run_on_cluster â€” the cluster has no direct internet access (DNS fails).
  Missing packages are auto-installed via the AIDP cluster libraries API by the migration tool.
  If a module is still missing after auto-install, use make_note to record it and keep original code.
- NEVER run recursive filesystem scans via run_on_cluster:
  No glob.glob(..., recursive=True) on /Workspace or /aidp â€” FUSE mounts, hangs forever.
  No os.walk('/Workspace/...') or os.walk('/aidp/...') â€” same reason.
  Use read_notebook_source or explore_path for targeted lookups instead.
# (DISABLED 2026-04-11: FUSE issues resolved) FileNotFoundError on /Volumes â€” was FUSE cache bug, no longer applicable
- SparkOutOfMemoryError / OOM / driver memory exceeded:
  Try: df.persist(); df.count(); then proceed with the write. For very large DataFrames, use checkpoint.
- toPandas() OOM / driver memory exceeded on toPandas() / collect():
  Do NOT modify the original code. A runtime safety patch auto-limits large DataFrames during
  migration execution. If OOM still occurs, use make_note to record it and move on.
- ConnectTimeout / ConnectionError on internal-host.example or internal-gateway.example:
  Comment out original code (keep as reference), then:
  If side-effect only: add print("AIDP: Skipped â€” internal AWS endpoint not reachable")
  If it fetches data: use run_on_cluster + explore_path to find that data in OCI first
- "Table does not support deletes" / DELETE FROM / UPDATE / MERGE:
  Keep code as-is â€” do NOT rewrite. These are customer logic. Skip execution during migration
  (destructive operations can cause data loss). Validate syntax only.
- NoCredentialsError from boto3 / botocore:
  No AWS credentials on AIDP. Use describe_table/explore_path to find the data in the AIDP catalog
  or OCI Object Storage, then read from there. Or use aidp_compat.read_s3_object / get_glue_table_s3_location.
- AttributeError: 'MlflowClient' object has no attribute 'get_latest_versions':
  MLflow 3.x removed get_latest_versions() with stages. Replace with alias-based API:
  client.get_latest_versions(name, stages=['Production']) â†’ client.get_model_version_by_alias(name, "production")
  client.get_latest_versions(name, stages=['Staging']) â†’ client.get_model_version_by_alias(name, "staging")
- ModuleNotFoundError: No module named 'mlflow':
  MLflow may not be pre-installed. Install with: import subprocess, sys; subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow"])
  Do NOT pin a specific version.
- XGBoost 'gpu_hist' / Invalid Input 'gpu_hist':
  XGBoost 3.x removed 'gpu_hist'. Fix immediately â€” no investigation needed:
  Replace tree_method='gpu_hist' with tree_method='hist', device='cuda' everywhere in the cell.
  If GPU is not available on the cluster, use tree_method='hist' without device='cuda' (CPU fallback).
  If the cell defines a function, fix inside the function. If gpu_hist is in a dict/space, fix the dict.
  Use fixup_cell if gpu_hist appears in an earlier cell's function definition.
- SparkTrials / NameError: name 'SparkTrials' is not defined:
  SparkTrials is Databricks-only (distributed hyperopt). Fix immediately â€” no investigation needed:
  Replace SparkTrials(parallelism=N) with Trials(). Add "from hyperopt import Trials" if not imported.
  This runs hyperopt serially on the driver instead of distributed across Spark workers.
- aidp_dbutils / ModuleNotFoundError: No module named 'aidp_dbutils':
  Replace "from aidp_dbutils import _DBUtils" with "from aidp_compat import dbutils, displayHTML, sql, translate_path".
  Remove the companion "dbutils = _DBUtils(...)" line â€” aidp_compat provides dbutils directly.

INVESTIGATION LIMIT: Do NOT spend more than 3 run_on_cluster calls investigating the same error.
If the error is in the KNOWN AIDP ERROR PATTERNS above, fix it directly on the first attempt â€” zero investigation needed.
After 3 investigation rounds, you MUST submit a fix or use make_note. Do not keep inspecting.

NEVER run recursive filesystem scans on the cluster:
- No glob.glob(..., recursive=True) on /Workspace or /aidp â€” these are FUSE mounts and will hang forever.
- No os.walk('/Workspace/...') or os.walk('/aidp/...') â€” same reason.
- If you need a notebook path, use read_notebook_source with the known path. If it fails, check the cell history
  or use explore_path on the specific directory â€” do NOT scan the entire workspace."""


def call_opus(system: str, user_content: str, max_tokens: int = 32000) -> Tuple[str, dict]:
    """Call OpenAI GPT-5.5 with streaming. Returns (response_text, usage)."""
    text_parts = []

    with model_client().messages.stream(
        model=OPENAI_MODEL,
        max_tokens=max_tokens,
        timeout=600,  # 10 min timeout for the API call itself
        system=system,
        messages=[{"role": "user", "content": user_content}]
    ) as stream:
        for text in stream.text_stream:
            text_parts.append(text)

    response = stream.get_final_message()
    full_text = "".join(text_parts)
    return full_text, {"input": response.usage.input_tokens, "output": response.usage.output_tokens}


# â”€â”€â”€ Tool Definitions for OCI Path Exploration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

OCI_PATH_TOOLS = [
    {
        "name": "explore_path",
        "description": "Explore an OCI, S3, or HDFS path on the AIDP cluster. Checks if the path exists, lists directory contents, or expands glob/wildcard patterns. Runs on the cluster using Spark's Hadoop filesystem (BmcFilesystem is configured at cluster level with API key auth). Use this to verify whether data is available before writing code that reads from it. IMPORTANT: Do NOT use this for /Workspace/, /Volumes/, or /tmp/ paths â€” those are local FUSE mounts, not OCI object storage. Only use for oci://, s3://, s3a://, or hdfs:// paths.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "The storage path to explore. Examples: 'oci://bucket@namespace/prefix/', 'oci://bucket@namespace/path/*.parquet', 's3://bucket/prefix/'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "suggest_oci_path",
        "description": "Given an S3, DBFS, or OCI path, look up the correct OCI path from the S3-to-OCI bucket mapping. Use this when you see an S3 path (s3://...) or a DBFS path (dbfs:/...) or an OCI path with the wrong namespace and need to find the correct OCI equivalent. Returns suggested oci:// paths with the right bucket name and namespace.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "The path to look up. Can be s3://, dbfs:/, /mnt/, or oci:// with potentially wrong namespace."
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "search_catalog",
        "description": "Check if a table exists in the AIDP catalog by exact name. Pass the exact table name (e.g. 'schema.table'). Returns EXISTS or NOT FOUND. Result is DEFINITIVE â€” if NOT FOUND, the table does NOT exist. The ONLY alternative to try is 3-part name (default.schema.table). Do NOT retry with name variations, partial names, or explore_path. Record missing table with make_note.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The table name or partial name to search for. Example: 'interactions', 'sample_events', 'allocation'"
                }
            },
            "required": ["query"],
            "additionalProperties": False
        }
    },
    {
        "name": "run_on_cluster",
        "description": "Execute arbitrary Python/PySpark code on the AIDP cluster and return the output. Use this to: check table schemas (spark.sql('DESCRIBE table').show()), test data availability (spark.read.parquet('path').count()), inspect DataFrames, list files, or debug issues. The cluster has spark, sc, and aidp_compat available. Code must use print() to produce output. Keep code short and focused - this is for exploration, not for producing the final cell code.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python/PySpark code to execute. Must use print() for output. Example: \"print(spark.sql('SHOW TABLES IN collection').collect()[:10])\""
                }
            },
            "required": ["code"],
            "additionalProperties": False
        }
    },
    {
        "name": "describe_table",
        "description": "Get the schema (columns, types) of a Spark table or view. Only call on actual Spark table names (schema.table or default.schema.table), NOT Python module names. If the table was not found by search_catalog, do NOT call describe_table on it.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Fully qualified table name. Example: 'collection.sample_events_all', 'default.my_table'"
                }
            },
            "required": ["table_name"],
            "additionalProperties": False
        }
    },
    {
        "name": "list_schemas_and_tables",
        "description": "List available Spark schemas (databases) and optionally tables within a specific schema. Use this to discover what data is registered in the AIDP Spark catalog.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "schema": {
                    "type": ["string", "null"],
                    "description": "Specific schema to list tables from. Pass null to list all schemas."
                }
            },
            "required": ["schema"],
            "additionalProperties": False
        }
    },
    {
        "name": "submit_data_recovery_plan",
        "description": (
            "Submit a plan to substitute hardcoded dates in a cell with actual "
            "available dates from the upstream table. ONLY call this when a "
            "cell has failed with an empty-data signature (IndexError after "
            "indexed-access on a query, AnalysisException about missing path, "
            "etc.) and the failure is caused by hardcoded date filter values "
            "that the upstream table simply doesn't have data for. "
            "Your job is to IDENTIFY: (1) the upstream table being queried, "
            "(2) the column used to filter by date, and (3) the cell-scope "
            "variable(s) that hold the date filter values. Do NOT modify the "
            "cell's code. The framework will query the table for actually "
            "available dates and build the substitution override at execution "
            "time only â€” the saved cell stays byte-identical to the original. "
            "Use describe_table and run_on_cluster tools to confirm the table "
            "name and date column before submitting."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Fully-qualified table the cell queries, e.g. 'default.db.tbl' or 'analytics.tbl'. Must be a real table verified via describe_table."
                },
                "date_column": {
                    "type": "string",
                    "description": "The column name in the table that holds the date the cell is filtering by, e.g. 'load_date', 'partition_date', 'decision_date'."
                },
                "overrides": {
                    "type": "array",
                    "description": "List of cell-scope variables whose values should be replaced with actual dates from the table.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "variable": {
                                "type": "string",
                                "description": "Name of the variable in the cell (e.g. 'partition_date_list', 'start_date')."
                            },
                            "is_list": {
                                "type": "boolean",
                                "description": "True if the variable is a list of dates (e.g. partition_date_list = ['2026-04-01', ...]); False if it's a single date string."
                            },
                            "max_dates": {
                                "type": "integer",
                                "description": "For is_list=True: how many recent dates to include in the substituted list (capped at 14)."
                            }
                        },
                        "required": ["variable", "is_list", "max_dates"],
                        "additionalProperties": False
                    }
                }
            },
            "required": ["table", "date_column", "overrides"],
            "additionalProperties": False
        }
    },
    {
        "name": "submit_code",
        "description": "Submit your final migrated or fixed Python code. You MUST call this tool to return your answer. Put the complete executable Python code in the 'code' field. Do NOT include markdown fences, explanations, or prose - ONLY valid Python code that can be exec()'d.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "The complete Python code. Must be valid, executable Python. No markdown, no prose, no explanations."
                }
            },
            "required": ["code"],
            "additionalProperties": False
        }
    },
    {
        "name": "read_notebook_source",
        "description": "Read the source code of a notebook from the AIDP workspace. Use this to understand dependent notebooks referenced via %run or dbutils.notebook.run - check function definitions, imports, variable assignments etc.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the notebook in the AIDP workspace. Example: 'Users/user@example.com/Utils/Data_Utils.ipynb'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "inspect_package_source",
        "description": "Look up the source code of a function, class, or method from an installed Python package on the AIDP cluster. Use this to understand how a dependency works - e.g., how aidp_dbutils._DBUtils.run() is implemented, or what pyspark.sql.functions.sum does vs builtins.sum.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "module": {
                    "type": "string",
                    "description": "Python module path. Example: 'aidp_compat.safe_io', 'pyspark.sql.functions', 'aidp_dbutils'"
                },
                "name": {
                    "type": "string",
                    "description": "Function or class name. Example: 'safe_pickle_dump', 'sum', '_DBUtils'"
                },
                "method": {
                    "type": ["string", "null"],
                    "description": "Method name if inspecting a class method. Pass null if not applicable. Example: 'run'"
                }
            },
            "required": ["module", "name", "method"],
            "additionalProperties": False
        }
    },
    {
        "name": "summarize_notebook",
        "description": "Get a concise summary of a notebook's purpose, key functions, imports, and variables it defines. Uses a fast model (OpenAI model) for summarization. Results are cached - calling this twice with the same path returns the cached summary instantly.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the notebook. Example: 'Users/user@example.com/ExampleProject/ExampleJob/ExampleUtils/0_Config.ipynb'"
                }
            },
            "required": ["path"],
            "additionalProperties": False
        }
    },
    {
        "name": "make_note",
        "description": (
            "Append a note about the current cell to the fix log and to cell history memory. "
            "Use this to record migration concerns, root cause observations, or important context "
            "you want to remember for this cell (e.g. 'This cell divides by X which will be 0 if "
            "upstream data is empty â€” fix by adding a null-check guard'). "
            "Notes are truncated to 500 characters. Multiple notes can be made; the last one is "
            "stored in the cell history entry. All notes are appended to the cell source's fix log comment."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "note": {
                    "type": "string",
                    "description": "Your note about this cell â€” migration concerns, root causes, or important observations. Max 500 chars."
                }
            },
            "required": ["note"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_cell_history",
        "description": (
            "Get a slice of the job-wide cell execution history. Each entry shows the index, "
            "notebook, cell number, a summary of what the cell does, its status (ok/error), "
            "and a short output preview. Use this to scan backwards and find which index "
            "caused the root-cause issue before calling fixup_cell."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "from_index": {
                    "type": ["integer", "null"],
                    "description": "Start index (inclusive). Use negative for tail: -20 = last 20. Pass null to start from beginning."
                },
                "to_index": {
                    "type": ["integer", "null"],
                    "description": "End index (exclusive). Pass null for end of history."
                }
            },
            "required": ["from_index", "to_index"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_history_entry",
        "description": "Get full details of a single history entry including its complete code. Use after get_cell_history identifies a suspicious entry.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "index": {"type": "integer", "description": "The history entry index."}
            },
            "required": ["index"],
            "additionalProperties": False
        }
    },
    {
        "name": "fixup_cell",
        "description": (
            "Rewind to a specific history index and replay all cells from that point to the current position. "
            "Each replayed cell goes through the full execute+verify+OpenAI model-fix loop with your 'why' diagnosis "
            "injected as context at every fix attempt. Use after get_cell_history identifies the root cause index.\n\n"
            "IMPORTANT â€” IDEMPOTENCY REQUIREMENT: Replaying re-executes every cell from start_index forward. "
            "This is only safe if those cells are idempotent (re-running them does not cause double writes, "
            "duplicate Spark jobs, or corrupted state). Before calling fixup_cell, verify that the replay "
            "sequence does not include cells that write to files, tables, or external systems in a non-idempotent "
            "way. If the sequence has non-idempotent side effects, use get_history_entry to identify the cells "
            "and consider whether fixup_cell is the right approach, or whether you should fix the current cell "
            "directly instead.\n\n"
            "Optionally provide fixed_code for the target cell to replace its code before replay begins. "
            "For child notebook cells (is_child=True), fixed_code also patches the notebook file on-cluster."
        ),
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "start_index": {
                    "type": "integer",
                    "description": "The history index to start replay from (inclusive). All entries from here to end of history are replayed."
                },
                "why": {
                    "type": "string",
                    "description": "Your diagnosis of the root cause. Injected into the fix context for every replayed cell."
                },
                "fixed_code": {
                    "type": ["string", "null"],
                    "description": "Your fixed version of the target cell's code. Pass null if not providing a fix upfront. If provided, replaces the target cell's code before replay. Also patches the child notebook file if is_child=True."
                }
            },
            "required": ["start_index", "why", "fixed_code"],
            "additionalProperties": False
        }
    },
    {
        "name": "scan_sensitive_info",
        "description": "Scan a notebook file for hardcoded sensitive info: Databricks API tokens (dapi...), Slack tokens/webhooks, internal Customer endpoints (internal-host.example, internal-gateway.example), and Databricks REST API calls. Returns list of matches with cell index, pattern type, and matched line. Use this when migrating cells that may have hardcoded credentials or internal AWS endpoint calls.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "notebook_path": {
                    "type": "string",
                    "description": "Absolute path to the notebook file on the cluster (e.g. /Workspace/Users/.../notebook.ipynb)"
                }
            },
            "required": ["notebook_path"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_job_parameters",
        "description": "Get job-level parameters for the current task from the job manifest. Use when dbutils.widgets.get(name) raises ValueError â€” the parameter value may be defined at the job level, not in the notebook itself.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "task_key": {
                    "type": ["string", "null"],
                    "description": "Task key to look up parameters for. Pass null to use the current task."
                }
            },
            "required": ["task_key"],
            "additionalProperties": False
        }
    },
    {
        "name": "get_tool_output",
        "description": "Retrieve the full untruncated output of a previous tool call that was truncated to save context space. The filename is shown in the truncation notice (e.g. 'tool_003_run_on_cluster.txt'). Use this when you need details that were cut off.",
        "strict": True,
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {
                    "type": "string",
                    "description": "Filename from the truncation notice, e.g. 'tool_003_run_on_cluster.txt'"
                }
            },
            "required": ["filename"],
            "additionalProperties": False
        }
    },
]


async def _handle_scan_sensitive_info(notebook_path: str, session, log_fn=None) -> str:
    """Scan a notebook for hardcoded sensitive info."""
    def _log(msg):
        if log_fn: log_fn(msg)

    import re as _re
    import json as _json

    _log(f"scan_sensitive_info: {notebook_path}")

    SENSITIVE_PATTERNS = [
        ("databricks_token", r'dapi[a-zA-Z0-9]{32,}'),
        ("slack_token", r'xox[bprs]-[0-9A-Za-z\-]+'),
        ("slack_webhook", r'hooks\.slack\.com/services/[A-Za-z0-9/]+'),
        ("internal_endpoint", r'internal-host\.example|internal-gateway.example'),
        ("databricks_api", r'azuredatabricks\.net|api/2\.0/jobs'),
        # MLflow tracking URI with embedded credentials (user:password@host)
        # e.g. mlflow.set_tracking_uri("https://admin:secret@<MLFLOW_HOST>")
        ("mlflow_embedded_creds", r'mlflow\.set_tracking_uri\s*\([^)]*://[^:]+:[^@]+@'),
    ]

    # Read notebook via cluster â€” cap at 150 cells to stay within AIDP output limits
    code = f"""
import json, builtins
with builtins.open("{notebook_path}", "r") as f:
    nb = json.load(f)
cells = nb.get("cells", [])
sources = []
for i, cell in enumerate(cells):
    if cell.get("cell_type") == "code":
        src = "".join(cell.get("source", []))
        sources.append((i, src))
    if len(sources) >= 150:
        break
print(json.dumps(sources))
"""
    run = getattr(session, 'run_stateless', session.execute)
    result = await run(code, timeout=30)
    raw = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))

    try:
        sources = _json.loads(raw)
    except Exception:
        return (
            f"Could not parse notebook sources (output may have been truncated by AIDP). "
            f"Raw output ({len(raw)} chars): {raw[:300]}"
        )

    matches = []
    for cell_idx, source in sources:
        for pattern_name, pattern in SENSITIVE_PATTERNS:
            for m in _re.finditer(pattern, source):
                line = source[max(0, m.start()-30):m.end()+30].replace('\n', ' ')
                matches.append({"cell_idx": cell_idx, "pattern": pattern_name, "match": line})

    if not matches:
        return "No sensitive info found."
    return _json.dumps(matches, indent=2)


async def _handle_get_job_parameters(log_fn=None) -> str:
    """Return the merged parameters for the current task.

    Parameters are pre-merged by process_job(): task-level base_parameters
    first, then job-level parameters override (job wins on conflict).
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    import json as _json

    _log(f"get_job_parameters: task={_current_task_key!r}, params={bool(_current_task_params)}")

    if _current_task_params:
        return _json.dumps(_current_task_params, indent=2)
    return "No job parameters found for the current task."


def _handle_get_tool_output(filename: str, log_fn=None) -> str:
    """Read full tool output from disk.

    Searches the current compactor first, then previous compactors in reverse
    order (newest first). This allows the fix phase to retrieve files that were
    saved during the eval phase of the same cell.
    """
    def _log(msg):
        if log_fn: log_fn(msg)

    if not _compactor_history:
        return "No active compactor â€” tool output files not available."

    _log(f"get_tool_output: {filename}")
    # Search current first, then walk history in reverse order
    for compactor in reversed(_compactor_history):
        result = compactor.get_saved_output(filename)
        if not result.startswith("[context_compactor] File not found"):
            return result

    # All compactors tried â€” return the last "not found" message (lists available files)
    return result


def _download_notebook_source(path: str):
    """Download a notebook from the AIDP workspace and return the parsed JSON dict.

    Returns the notebook dict on success, or None if not found.
    Raises on network/auth errors.

    Uses the module-level DOWNLOAD_META_URL + signer() â€” both configured at
    runtime for the active deployment (job_migrate_from_workflow overrides them),
    so this targets whatever lake/workspace/profile the run is using.
    """
    import requests as _requests

    for try_path in [path, path + ".ipynb" if not path.endswith(".ipynb") else path]:
        headers = {"Content-Type": "application/json", "path": try_path, "type": "NOTEBOOK"}
        resp = _requests.post(DOWNLOAD_META_URL, auth=signer(), headers=headers, data="")
        if resp.status_code == 200:
            par_url = resp.json().get("parUrl")
            if par_url:
                data = _requests.get(par_url)
                if data.status_code == 200:
                    return json.loads(data.content)
    return None


async def _handle_tool_call(tool_name: str, tool_input: dict, session=None, log_fn=None) -> str:
    """Execute a tool call and return the result string.
    If session is provided, can run cluster-based tools (async)."""
    from context_tools import explore_oci_path, suggest_oci_path, search_catalog

    _SKIP_MODULES = {
        "os", "sys", "json", "re", "io", "math", "time", "datetime",
        "collections", "functools", "itertools", "typing", "pathlib",
        "hashlib", "base64", "copy", "csv", "logging", "traceback",
        "subprocess", "tempfile", "shutil", "glob", "urllib", "http",
        "concurrent", "threading", "multiprocessing", "abc",
        "dataclasses", "enum", "struct", "pickle", "gzip", "zipfile",
        "contextlib", "pyspark", "numpy", "pandas", "scipy", "sklearn",
        "matplotlib", "seaborn", "plotly", "optuna", "xgboost",
        "lightgbm", "tensorflow", "torch", "keras", "pil", "cv2",
        "requests", "boto3", "botocore", "oci", "delta", "pytz",
        "dateutil", "aidp_compat", "oidlutils", "pprint", "operator",
        "decimal", "fractions", "warnings", "inspect", "ast", "textwrap",
        "string", "random", "secrets", "uuid", "socket", "ssl",
        "email", "html", "xml", "ctypes", "np", "pd", "spark",
    }

    if tool_name == "submit_code":
        # This is handled specially in call_opus_with_tools - should not reach here
        return "Code submitted"

    if tool_name == "submit_data_recovery_plan":
        # Also handled specially in call_opus_with_tools (early-return) â€” this
        # path is only reached if OpenAI model calls the tool twice or in an unexpected
        # context. Return a harmless ack so the tool-use loop continues without
        # exception.
        return "Data recovery plan recorded."

    if tool_name == "explore_path":
        path = tool_input.get("path", "")
        if path in _path_explore_cache:
            return _path_explore_cache[path] + "\n(cached â€” already explored this path)"
        if session:
            result = await explore_oci_path(session, path)
            _path_explore_cache[path] = result
            return result
        else:
            return f"No cluster session available. Path: {path}"

    elif tool_name == "suggest_oci_path":
        path = tool_input.get("path", "")
        suggestions = suggest_oci_path(path)
        if suggestions:
            return "Suggested OCI paths:\n" + "\n".join(f"  {s}" for s in suggestions)
        else:
            return f"No mapping found for: {path}"

    elif tool_name == "search_catalog":
        query = tool_input.get("query", "")
        _q_first = query.split(".")[0].lower()
        if _q_first in _SKIP_MODULES:
            return f"'{query}' is a Python module, not a table. Skip."
        # Normalize cache key: lowercase + strip default. prefix â€” so
        # "dream_pipeline.FOO", "default.dream_pipeline.foo", "dream_pipeline.foo"
        # all hit the same cache entry.
        _cache_key = query.lower().strip()
        if _cache_key.startswith("default."):
            _cache_key = _cache_key[len("default."):]
        # Check run-level cache first â€” avoid repeated lookups for the same table
        if _cache_key in _table_lookup_cache:
            return _table_lookup_cache[_cache_key] + " (cached â€” already searched, do NOT retry)"
        result = search_catalog(query)
        # If offline catalog unavailable, try live cluster check
        if "No catalog available" in result and session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                _check_code = (
                    f"try:\n"
                    f"    cols = spark.sql('DESCRIBE TABLE {query}').collect()\n"
                    f"    print(f'Table EXISTS: {query} ({{len(cols)}} cols)')\n"
                    f"except Exception as e:\n"
                    f"    print(f'Table NOT FOUND: {query}')\n"
                )
                live_result = await run(_check_code, timeout=60)
                result = _extract_output(live_result)
            except Exception:
                result = f"Table NOT FOUND: '{query}' â€” catalog unavailable, cluster check failed"
        _table_lookup_cache[_cache_key] = result
        return result

    elif tool_name == "run_on_cluster":
        code = tool_input.get("code", "")
        # Block recursive filesystem scans â€” these hang indefinitely on FUSE-mounted
        # /Workspace and /aidp. OpenAI model falls back to these when read_notebook_source fails.
        import re as _re_scan
        if _re_scan.search(r'glob\.glob\s*\([^)]*recursive\s*=\s*True', code) and \
           _re_scan.search(r'["\']/(Workspace|aidp)', code):
            return ("BLOCKED: Recursive glob on /Workspace or /aidp hangs indefinitely (FUSE mount). "
                    "Use read_notebook_source with the correct path, or explore_path for targeted lookups.")
        if _re_scan.search(r'os\.walk\s*\(\s*[\'"](/(Workspace|aidp))', code):
            return ("BLOCKED: os.walk on /Workspace or /aidp hangs indefinitely (FUSE mount). "
                    "Use read_notebook_source with the correct path, or explore_path for targeted lookups.")
        # Track any pip install commands
        for pkg in extract_pip_packages(code):
            _installed_packages.add(pkg)
        if session:
            try:
                result = await session.execute(code, timeout=1200)
                output = _extract_output(result)
                # Detect AIDP compute cluster down â€” surface clearly so OpenAI model doesn't
                # try to "fix" the cell code. The main execution loop handles reconnect.
                import re as _re
                if _re.search(r"Compute cluster [a-z0-9.\-:]+ is not running", output or "", _re.IGNORECASE):
                    return (
                        "[INFRA] AIDP compute cluster is paused â€” this is NOT a code error. "
                        "The main cell execution loop will reconnect automatically. "
                        "Do not modify the cell code. Skip this tool call and proceed with submit_code."
                    )
                return output
            except Exception as e:
                return f"Execution error: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "describe_table":
        table_name = tool_input.get("table_name", "")
        _first_part = table_name.split(".")[0].lower()
        if _first_part in _SKIP_MODULES:
            return f"'{table_name}' is a Python module, not a Spark table. Skip."
        # Don't DESCRIBE a table we already know is missing
        _desc_key = table_name.lower().strip()
        if _desc_key.startswith("default."):
            _desc_key = _desc_key[len("default."):]
        _cached = _table_lookup_cache.get(_desc_key)
        if _cached and "NOT FOUND" in _cached:
            return f"'{table_name}' was already confirmed NOT FOUND. Do not retry."
        if session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                result = await run(
                    f"spark.sql('DESCRIBE TABLE {table_name}').show(100, truncate=False)",
                    timeout=60)
                return _extract_output(result)
            except Exception as e:
                return f"Error describing {table_name}: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "list_schemas_and_tables":
        schema = tool_input.get("schema") or ""
        _schema_cache_key = f"_schema_:{schema.lower()}"
        if _schema_cache_key in _table_lookup_cache:
            return _table_lookup_cache[_schema_cache_key] + " (cached â€” already listed, do NOT retry)"
        if session:
            try:
                run = getattr(session, 'run_stateless', session.execute)
                if schema:
                    result = await run(
                        f"for t in spark.sql('SHOW TABLES IN {schema}').collect()[:50]: print(f'{{t.tableName}}')",
                        timeout=60)
                else:
                    result = await run(
                        "for s in spark.sql('SHOW SCHEMAS').collect()[:50]: print(f'{s[0]}')",
                        timeout=60)
                _output = _extract_output(result)
                _table_lookup_cache[_schema_cache_key] = _output
                return _output
            except Exception as e:
                return f"Error listing: {str(e)[:300]}"
        else:
            return "No cluster session available"

    elif tool_name == "read_notebook_source":
        path = tool_input.get("path", "")
        try:
            nb = _download_notebook_source(path)
            if nb is None:
                return f"Notebook not found: {path}"
            code_parts = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        code_parts.append(src)
            full_src = "\n\n# ---\n\n".join(code_parts)
            return full_src[:50000] if len(full_src) > 50000 else full_src
        except Exception as e:
            return f"Error reading notebook: {str(e)[:300]}"

    elif tool_name == "inspect_package_source":
        module = tool_input.get("module", "")
        name = tool_input.get("name", "")
        method = tool_input.get("method", "")
        if session:
            try:
                if method:
                    code = f"import inspect; from {module} import {name}; print(inspect.getsource(getattr({name}, '{method}')))"
                else:
                    code = f"import inspect; from {module} import {name}; print(inspect.getsource({name}))"
                run = getattr(session, 'run_stateless', session.execute)
                result = await run(code, timeout=30)
                return _extract_output(result)
            except Exception as e:
                return f"Error inspecting {module}.{name}: {str(e)[:200]}"
        else:
            return "No cluster session available"

    elif tool_name == "summarize_notebook":
        path = tool_input.get("path", "")
        # Check cache first (stored in cell_context if available)
        try:
            nb = _download_notebook_source(path)
            if nb is None:
                return f"Notebook not found: {path}"
            code_parts = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        code_parts.append(src[:200])
            full_src = "\n---\n".join(code_parts)

            # Call OpenAI model for summary
            summary_resp = model_client().messages.create(
                model=OPENAI_MODEL,
                max_tokens=1000,
                timeout=60,
                messages=[{"role": "user", "content": f"Summarize this notebook in 3-5 bullet points. What does it do? What functions/variables does it define? What does it import?\n\n{full_src[:5000]}"}]
            )
            summary = summary_resp.content[0].text if summary_resp.content else "No summary"
            return summary[:1000]
        except Exception as e:
            return f"Error summarizing notebook: {str(e)[:200]}"

    elif tool_name == "make_note":
        raw_note = tool_input.get("note", "").strip()[:500]
        _current_cell_notes.append(raw_note)
        if log_fn:
            log_fn(f"[note] {raw_note[:120]}")
        return f"Note recorded ({len(raw_note)} chars). Total notes this cell: {len(_current_cell_notes)}."

    elif tool_name == "get_cell_history":
        from_idx = tool_input.get("from_index") or 0
        to_idx   = tool_input.get("to_index") or None
        n = len(_cell_history)
        if from_idx < 0:
            from_idx = max(0, n + from_idx)
        if to_idx is not None and to_idx < 0:
            to_idx = max(0, n + to_idx)
        slice_ = _cell_history[from_idx:to_idx]
        lines = [f"Cell history (indices {from_idx}-{from_idx+len(slice_)-1} of {n} total):"]
        for e in slice_:
            child_tag = "[child] " if e.get("is_child") else ""
            nb = e["notebook_path"]
            lines.append(
                f"[{e['index']}] {child_tag}{nb} cell {e['cell_idx']} | {e['status'].upper()} | "
                f"{e['summary'][:120]} | out: {e['output_preview'][:80]}"
            )
        return "\n".join(lines)

    elif tool_name == "get_history_entry":
        idx = int(tool_input.get("index", 0))
        if idx < 0 or idx >= len(_cell_history):
            return f"ERROR: index {idx} out of range (history has {len(_cell_history)} entries)"
        e = _cell_history[idx]
        return (
            f"History entry [{idx}]\n"
            f"Notebook: {e['notebook_path']}\n"
            f"Cell: {e['cell_idx']} | is_child: {e.get('is_child', False)} | status: {e['status']}\n"
            f"Summary: {e['summary']}\n"
            f"Output preview: {e['output_preview']}\n"
            f"Code:\n```python\n{e['final_code']}\n```"
        )

    elif tool_name == "fixup_cell":
        start_idx = int(tool_input.get("start_index", 0))
        why       = tool_input.get("why", "")
        fixed     = tool_input.get("fixed_code", None)

        n = len(_cell_history)
        if start_idx < 0 or start_idx >= n:
            return f"ERROR: start_index {start_idx} out of range (history has {n} entries). Use get_cell_history first."
        if not _replay_cell_fn:
            return "ERROR: replay function not registered."

        target = _cell_history[start_idx]
        old_entries = [dict(e) for e in _cell_history[start_idx:]]
        replay_entries = list(old_entries)

        if log_fn:
            child_tag = "[child] " if target.get("is_child") else ""
            log_fn(f"[fixup_cell] Rewinding to index {start_idx}: "
                   f"{child_tag}{os.path.basename(target['notebook_path'])} cell {target['cell_idx']}: "
                   f"{target['summary'][:100]}")
            log_fn(f"[fixup_cell] Why: {why[:200]}")
            log_fn(f"[fixup_cell] Pruning {len(old_entries)} history entries, replaying through execute+verify+fix loop")

        # Prune history from start_index onward â€” replay will rebuild it
        del _cell_history[start_idx:]

        # Inject fixed_code into the target entry before replay
        if fixed:
            replay_entries[0] = {**replay_entries[0], "final_code": fixed}
            if target.get("is_child"):
                nb_path  = target["notebook_path"]
                cell_idx = target["cell_idx"]
                # `cell_idx` is recorded against the real-cell-only index space
                # (the AIDP config cell prepended by process_notebook is skipped
                # at child-cell collection time in _inline_child_notebook).
                # Mirror that filter here so `_code_cells[cell_idx]` lands on
                # the correct on-disk position; otherwise the patch would write
                # to the config cell at index 0 instead of the intended real cell.
                patch_code = f"""import json
with open({repr(nb_path)}) as _f:
    _nb = json.load(_f)
_CFG_MARKER = '# AIDP performance configuration'
_code_cells = [i for i, c in enumerate(_nb['cells'])
               if c.get('cell_type') == 'code'
               and ''.join(c.get('source', [])).strip()
               and _CFG_MARKER not in ''.join(c.get('source', []))]
_real_idx = _code_cells[{cell_idx}]
_nb['cells'][_real_idx]['source'] = {repr(fixed.splitlines(keepends=True))}
with open({repr(nb_path)}, 'w') as _f:
    json.dump(_nb, _f, indent=1)
print('patched')
"""
                if session:
                    patch_res = await session.execute(patch_code, timeout=30)
                    if "patched" not in _extract_output(patch_res):
                        return f"ERROR: Failed to patch child notebook {nb_path} cell {cell_idx}"
                    if log_fn:
                        log_fn(f"[fixup_cell] Patched child notebook: {nb_path} cell {cell_idx}")

        # Replay each entry through execute+verify+fix loop
        errors, change_lines = [], []
        for i, entry in enumerate(replay_entries):
            result = await _replay_cell_fn(entry, why, session, log_fn)
            old_e = old_entries[i]
            new_status = result["status"]
            changed = (old_e.get("final_code", "") != result["final_code"] or
                       old_e.get("status", "") != new_status)
            change_tag = " [CHANGED]" if changed else ""
            nb = os.path.basename(entry["notebook_path"])
            change_lines.append(
                f"[{start_idx+i}] {nb} cell {entry['cell_idx']}: "
                f"{old_e.get('status','?').upper()} -> {new_status.upper()}{change_tag}"
            )
            if result["status"] != "ok":
                errors.append(f"idx {start_idx+i} cell {entry['cell_idx']}: {result.get('output','')[:100]}")

        new_n = len(_cell_history)
        changes_summary = "\n".join(change_lines[-15:])
        if errors:
            return (f"PARTIAL: Rewound to index {start_idx}, replayed {len(replay_entries)} cells. "
                    f"History now has {new_n} entries.\n"
                    f"Issues: {'; '.join(errors[:3])}\n"
                    f"Changed cells:\n{changes_summary}")
        return (f"OK: Rewound to index {start_idx}, replayed {len(replay_entries)} cells â€” all passed. "
                f"History now has {new_n} entries.\n"
                f"Changed cells:\n{changes_summary}\n"
                f"Now submit your fix for the current cell.")

    elif tool_name == "scan_sensitive_info":
        notebook_path = tool_input.get("notebook_path", "")
        return await _handle_scan_sensitive_info(notebook_path, session, log_fn)

    elif tool_name == "get_job_parameters":
        return await _handle_get_job_parameters(log_fn)

    elif tool_name == "get_tool_output":
        filename = tool_input.get("filename", "")
        return _handle_get_tool_output(filename, log_fn)

    return f"Unknown tool: {tool_name}"


def _unwrap_aidp_text(raw_text: str) -> str:
    """AIDP wraps output in JSON: [{"type":"TEXT_PLAIN","value":"actual text"}].
    Handles concatenated arrays: [{"value":"a"}][{"value":"b"}].
    Extract the actual text value. Returns raw text if not wrapped."""
    if not raw_text:
        return ""

    # Try single JSON array first
    try:
        items = json.loads(raw_text, strict=False)
        if isinstance(items, list):
            parts = []
            for item in items:
                if isinstance(item, dict) and "value" in item:
                    parts.append(item["value"])
            if parts:
                return "".join(parts)
    except (json.JSONDecodeError, TypeError):
        pass

    # Handle concatenated JSON arrays: ][
    if "][" in raw_text:
        extracted = []
        for chunk in re.split(r'\]\s*\[', raw_text):
            chunk = chunk.strip()
            if not chunk.startswith("["):
                chunk = "[" + chunk
            if not chunk.endswith("]"):
                chunk = chunk + "]"
            try:
                items = json.loads(chunk, strict=False)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and "value" in item:
                            extracted.append(item["value"])
            except (json.JSONDecodeError, TypeError):
                pass
        if extracted:
            # Deduplicate consecutive identical entries (AIDP sends twice)
            deduped = []
            for e in extracted:
                if not deduped or e != deduped[-1]:
                    deduped.append(e)
            return "".join(deduped)

    return raw_text  # Return as-is if not wrapped


def _extract_output(result: dict) -> str:
    """Extract text output from AIDP session execution result.
    Handles the AIDP JSON-wrapped format."""
    outputs = result.get("outputs", [])
    text = ""
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            text += _unwrap_aidp_text(raw)
        elif o.get("type") == "execute_result":
            data = o.get("data", {})
            raw = data.get("text/plain", "")
            text += _unwrap_aidp_text(raw)
        elif o.get("type") == "error":
            text += f"ERROR: {o.get('ename', '')}: {o.get('evalue', '')}\n"
            for tb in o.get("traceback", [])[:3]:
                import re as _re
                clean = _re.sub(r'\x1b\[[0-9;]*m', '', str(tb))
                text += f"  {clean}\n"
    # Deduplicate (AIDP sometimes sends output twice)
    if text and len(text) > 20:
        half = len(text) // 2
        if text[:half] == text[half:]:
            text = text[:half]
    return text[:50000] if text else "(no output)"


async def _create_message_with_529_retry(loop, create_fn, log_fn=None,
                                          max_retries: int = 5,
                                          grammar_max_retries: int = 3):
    """Run an OpenAI Responses API call with retries on transient errors.

    create_fn: a no-arg callable that synchronously invokes model_client().messages.create(...)
               and returns the response object.
    log_fn:    optional callback(msg: str) for retry-progress logging.
    max_retries: 529-Overloaded retry ATTEMPTS after the initial call (default 5,
                so up to 6 total tries with backoff ~1s, 2s, 4s, 8s, 16s).
    grammar_max_retries: retry ATTEMPTS for "Grammar compilation timed out" (default 3).

    Behavior:
    - On success, returns the response.
    - On OpenAI 529 ("Overloaded"), waits with exponential backoff + jitter and retries.
    - On "Grammar compilation timed out" (a 400 invalid_request_error): the API timed out
      SERVER-SIDE compiling the tool-schema grammar â€” transient (the same schemas compile
      fine on a retry; verified: sibling tasks using the identical tool set succeed). Without
      a retry, ONE such timeout aborts an entire notebook task (0 cells migrated). Retry with
      backoff up to grammar_max_retries before giving up.
    - On any other error (e.g. "prompt is too long", other invalid_request_error, etc.),
      re-raises immediately so the caller's existing error-handling logic still runs.
    - If a retried error persists past its budget, raises RuntimeError chained from the last one.
    """
    import asyncio
    import random
    attempt_529 = 0
    attempt_grammar = 0
    while True:
        try:
            return await loop.run_in_executor(None, create_fn)
        except Exception as exc:
            exc_str = str(exc)
            # Match provider-specific transient error markers (avoid matching incidental
            # substrings in user error messages). Anything not matched propagates so
            # the caller (e.g. context-overflow handler) can react appropriately.
            is_529 = ("Error code: 529" in exc_str) or ("overloaded_error" in exc_str)
            is_grammar = "Grammar compilation timed out" in exc_str
            if is_529:
                if attempt_529 >= max_retries:
                    raise RuntimeError(
                        f"OpenAI 529 Overloaded persisted after {max_retries} retries. "
                        f"Last error: {exc}"
                    ) from exc
                wait = (2 ** attempt_529) + random.random()
                label = f"OpenAI 529 Overloaded â€” retry {attempt_529 + 1}/{max_retries} in {wait:.1f}s"
                attempt_529 += 1
            elif is_grammar:
                if attempt_grammar >= grammar_max_retries:
                    raise RuntimeError(
                        f"OpenAI 'Grammar compilation timed out' persisted after "
                        f"{grammar_max_retries} retries. Last error: {exc}"
                    ) from exc
                wait = (2 ** attempt_grammar) + random.random()
                label = (f"OpenAI Grammar compilation timed out (transient) â€” "
                         f"retry {attempt_grammar + 1}/{grammar_max_retries} in {wait:.1f}s")
                attempt_grammar += 1
            else:
                raise
            if log_fn:
                log_fn(label)
            else:
                print(f"  [tools] {label}")
            await asyncio.sleep(wait)


async def call_opus_with_tools(system: str, user_content: str, session=None,
                                max_tokens: int = 128000, max_tool_rounds: int = 20,
                                log_fn=None) -> Tuple[str, dict]:
    """Call OpenAI GPT-5.5 with tool use for path exploration.
    Handles the tool-use loop: OpenAI model can call tools, we execute them on cluster, send results back.
    Returns (final_text, usage_totals).

    log_fn: optional callback(msg: str) for logging tool calls. If None, prints to stdout.
    Async because tool handlers need to execute code on AIDP cluster."""
    import asyncio
    import uuid
    from context_compactor import ContextCompactor

    def _log(msg):
        if log_fn:
            log_fn(msg)
        else:
            print(f"  [tools] {msg}")

    # Set up context compactor for this call. Store it as a module global so
    # _handle_get_tool_output can reference it. Each call_opus_with_tools
    # invocation gets its own compactor; sequential calls (eval then fix)
    # each set this, so only the most-recent call's files are directly
    # accessible via get_tool_output.
    call_id = str(uuid.uuid4())[:8]
    compactor = ContextCompactor(call_id=call_id, log_fn=_log)
    global _current_compactor, _compactor_history
    _current_compactor = compactor
    _compactor_history.append(compactor)

    # Prepend job execution history so OpenAI model has context before reasoning
    history_ctx = _format_history_context()
    if history_ctx:
        user_content = history_ctx + "\n\n" + user_content

    messages = [{"role": "user", "content": user_content}]
    total_usage = {"input": 0, "output": 0}

    loop = asyncio.get_event_loop()

    for round_num in range(max_tool_rounds + 1):
        # Apply context compaction before each API call
        messages = compactor.maybe_compact(messages, system)

        # Run the sync API call in a thread to not block the event loop
        try:
            response = await _create_message_with_529_retry(loop, lambda: model_client().messages.create(
                model=OPENAI_MODEL,
                max_tokens=max_tokens,
                timeout=600,
                system=system,
                messages=messages,
                tools=OCI_PATH_TOOLS,
                thinking={"type": "adaptive"},
            ), log_fn=_log)
        except Exception as _api_exc:
            _exc_str = str(_api_exc)
            if "prompt is too long" in _exc_str or "prompt_too_long" in _exc_str:
                _log(f"WARNING: Context overflow even after compaction. Falling back to emergency trim.")
                if len(messages) > 3:
                    messages = messages[:1] + messages[-2:]
                    try:
                        response = await _create_message_with_529_retry(loop, lambda: model_client().messages.create(
                            model=OPENAI_MODEL,
                            max_tokens=max_tokens,
                            timeout=600,
                            system=system,
                            messages=messages,
                            tools=OCI_PATH_TOOLS,
                            thinking={"type": "adaptive"},
                        ), log_fn=_log)
                    except Exception as _retry_exc:
                        raise RuntimeError(
                            f"Context overflow: prompt too long even after emergency trim "
                            f"({len(messages)} msgs). Aborting this cell attempt so the fix "
                            f"loop can retry with a fresh context. Original error: {_api_exc}"
                        ) from _retry_exc
                else:
                    raise RuntimeError(
                        f"Context overflow: prompt too long with only {len(messages)} messages. "
                        f"Cannot trim further. Aborting this cell attempt. Error: {_api_exc}"
                    ) from _api_exc
            else:
                raise

        total_usage["input"] += response.usage.input_tokens
        total_usage["output"] += response.usage.output_tokens

        # Log thinking blocks and usage stats
        thinking_tokens = getattr(response.usage, 'thinking_tokens', None) or 0
        for block in response.content:
            if block.type == "thinking":
                thinking_text = block.thinking or ""
                # Log up to 2000 chars of thinking, line by line
                for line in thinking_text[:2000].split("\n"):
                    if line.strip():
                        _log(f"[thinking] {line.strip()}")
                if len(thinking_text) > 2000:
                    _log(f"[thinking] ... ({len(thinking_text)} chars total)")
        if thinking_tokens:
            _log(f"[usage] in={response.usage.input_tokens} out={response.usage.output_tokens} thinking={thinking_tokens}")

        # Check if response has tool use
        has_tool_use = any(block.type == "tool_use" for block in response.content)

        if not has_tool_use or response.stop_reason == "end_turn":
            # Extract text from response (skip thinking blocks)
            text_parts = [block.text for block in response.content if block.type == "text"]
            if round_num > 0:
                _log(f"Tool loop done after {round_num} round(s), {total_usage['input']+total_usage['output']} tokens")
            return "\n".join(text_parts), total_usage

        # Check if submit_code was called - extract code directly
        for block in response.content:
            if block.type == "tool_use" and block.name == "submit_code":
                code = block.input.get("code", "")
                if code.strip():
                    _log(f"submit_code: {len(code)} chars")
                    if round_num > 0:
                        _log(f"Tool loop done after {round_num+1} round(s), {total_usage['input']+total_usage['output']} tokens")
                    return code, total_usage

        # Check if submit_data_recovery_plan was called â€” return the plan as
        # a JSON-encoded string (caller will json.loads it). Parallel handling
        # to submit_code: a "submit" tool signals "OpenAI model is done, here's the
        # answer."
        for block in response.content:
            if block.type == "tool_use" and block.name == "submit_data_recovery_plan":
                plan = block.input
                _log(f"submit_data_recovery_plan: table={plan.get('table')!r} date_column={plan.get('date_column')!r} overrides={len(plan.get('overrides', []))}")
                if round_num > 0:
                    _log(f"Tool loop done after {round_num+1} round(s), {total_usage['input']+total_usage['output']} tokens")
                return json.dumps(plan), total_usage

        # Handle other tool calls
        messages.append({"role": "assistant", "content": response.content})

        # Process each tool use block (async - can execute on cluster)
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                if block.name == "submit_code":
                    # Already handled above, but provide a result to continue the conversation
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": "Code submitted successfully"
                    })
                    continue

                tool_input_str = json.dumps(block.input)
                if len(tool_input_str) > 120:
                    tool_input_str = tool_input_str[:120] + "..."
                _log(f"Round {round_num+1}: {block.name}({tool_input_str})")

                import time as _time
                t0 = _time.time()
                result_str = await _handle_tool_call(block.name, block.input, session=session, log_fn=log_fn)
                elapsed = _time.time() - t0

                # Tier 1 compaction: save full result to disk, truncate what goes into messages
                result_str = compactor.save_and_truncate(block.name, result_str)

                result_preview = result_str[:150].replace("\n", " ") if result_str else "(empty)"
                _log(f"  -> {result_preview} ({elapsed:.1f}s)")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str
                })

        messages.append({"role": "user", "content": tool_results})

    # Exhausted tool rounds - force a final text response without tools
    _log(f"WARNING: Exhausted {max_tool_rounds} tool rounds, forcing final answer")

    # First check if the last response had any text
    text_parts = [block.text for block in response.content if block.type == "text"]
    if text_parts and any(t.strip() for t in text_parts):
        return "\n".join(text_parts), total_usage

    # No text in last response - do one more call WITHOUT tools to force text output
    # Include the tool results so far so OpenAI model has context
    messages.append({"role": "assistant", "content": response.content})
    # Send tool results for any pending tool calls
    tool_results = []
    for block in response.content:
        if block.type == "tool_use":
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": "(tool round limit reached - please produce your final answer now)"
            })
    if tool_results:
        messages.append({"role": "user", "content": tool_results})

    # OpenAI Responses handles reasoning via OPENAI_REASONING_EFFORT in openai_model.py.


    _final_max = max(max_tokens, 16000)
    final_response = await _create_message_with_529_retry(loop, lambda: model_client().messages.create(
        model=OPENAI_MODEL,
        max_tokens=_final_max,
        timeout=600,
        system=system,
        messages=messages,
        thinking={"type": "adaptive"},
        # No tools - forces text response
    ), log_fn=_log)
    total_usage["input"] += final_response.usage.input_tokens
    total_usage["output"] += final_response.usage.output_tokens
    text_parts = [block.text for block in final_response.content if block.type == "text"]
    return "\n".join(text_parts) if text_parts else "", total_usage


async def call_fix(code: str, error: str, context: List[str], attempt: int,
                   extra_context: str = "", session=None, log_fn=None,
                   notebook_path: str = "") -> str:
    """Call OpenAI GPT-5.5 to fix a cell. Uses full 1M context window.
    If session is provided, enables tool use for path exploration on cluster."""
    import asyncio
    # Send ALL previously executed code - we have 1M tokens
    ctx = ""
    if notebook_path:
        ctx += f"\n\nSource notebook path: {notebook_path}\n"
        ctx += f"(Use this path with read_notebook_source if you need to check the original cell content.)\n"
    if extra_context:
        ctx += f"\n\n{extra_context}\n"
    if context:
        ctx += "\n\nALL previously executed cells in this session (in order):\n"
        for i, c in enumerate(context):
            ctx += f"### Cell {i} (executed OK):\n```python\n{c}\n```\n\n"

    fix_input = f"""Fix this cell (attempt {attempt}/10).

The code:
```python
{code}
```

The error:
```
{error[:5000]}
```
{ctx}

IMPORTANT: Use the tools available to you (explore_path, suggest_oci_path, search_catalog, run_on_cluster)
to verify data availability BEFORE rewriting the code. Do not guess paths - check them first.

You MUST call the submit_code tool to return your fixed Python code. Do NOT return code as text.
The code field must contain ONLY valid executable Python."""

    if session:
        # Tool-enabled path: call_opus_with_tools extracts code via submit_code,
        # so the returned string is already clean Python â€” no further processing needed.
        resp, _ = await call_opus_with_tools(FIX_PROMPT, fix_input, session=session, max_tokens=128000, max_tool_rounds=25, log_fn=log_fn)
        # Safety check: if tool rounds exhausted, OpenAI model may return prose instead of code.
        # Detect PROSE explicitly (language-agnostic) rather than whitelisting Python
        # prefixes. A Python-only whitelist drops valid non-Python fixes â€” code
        # starting with "%scala", "val ", "object ", "SELECT ", etc. is not Python
        # and must not be discarded just because it isn't.
        # Rule: trust the response as code UNLESS it clearly opens like English prose
        # or is wrapped in a markdown fence (then extract the fenced code).
        stripped_resp = resp.strip() if resp else ""
        _PROSE_OPENERS = (
            "i ", "i'", "iâ€™", "here", "to fix", "to resolve", "to make", "sure",
            "let me", "let's", "the error", "the issue", "the problem", "the cell",
            "the code", "this ", "it ", "we ", "you ", "based on", "looking at",
            "note:", "note that", "unfortunately", "apologies", "sorry", "first,",
            "the fix", "the root", "the failure", "okay", "ok,", "certainly",
        )
        # Positive code signal: an unambiguous code/magic opener can never be
        # English prose, so don't let a prose-opener match (e.g. a fix that
        # legitimately starts with "select"/"with") send valid code through the
        # extract-from-prose path. (Markdown-fenced responses still go through
        # extraction below.)
        _CODE_OPENERS = (
            "%scala", "%sql", "%python", "%pip", "%sh", "val ", "var ", "def ",
            "object ", "class ", "import ", "from ", "package ", "@", "select ",
            "with ", "create ", "insert ", "spark.", "print(",
        )
        _has_code_signal = stripped_resp.lower().startswith(_CODE_OPENERS)
        _looks_like_prose = (
            not stripped_resp
            or stripped_resp.startswith("```")
            or (stripped_resp.lower().startswith(_PROSE_OPENERS) and not _has_code_signal)
        )
        if _looks_like_prose:
            # Looks like prose (or markdown-fenced) â€” try extracting code from it
            extracted = _extract_code_from_response(resp)
            if extracted and extracted != resp:
                if log_fn:
                    log_fn("  [fix] Answer was prose/fenced â€” extracted code from response")
                return extracted
            # Still prose â€” return original code unchanged so it doesn't execute garbage
            if log_fn:
                log_fn(f"  [fix] WARNING: Answer is prose, not code â€” keeping original code")
            return code  # return the ORIGINAL code, not the prose
        return resp
    else:
        loop = asyncio.get_event_loop()
        def _call_streaming():
            text_parts = []
            with model_client().messages.stream(
                model=OPENAI_MODEL,
                max_tokens=128000,
                system=FIX_PROMPT,
                messages=[{"role": "user", "content": fix_input}]
            ) as stream:
                for text in stream.text_stream:
                    text_parts.append(text)
            return "".join(text_parts)
        resp = await loop.run_in_executor(None, _call_streaming)
        return _extract_code_from_response(resp)


# â”€â”€ Data recovery (test-only date substitution) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
#
# When a cell fails with an empty-data signature, the framework calls
# attempt_data_recovery() to get a one-shot override block that substitutes
# the cell's date-filter variables with dates that ACTUALLY have data in the
# upstream table. The override is prepended to the cell's exec code ONLY;
# the saved cell stays byte-identical to the customer's original.
#
# Flow:
#   1. OpenAI model inspects the cell + error, uses describe_table / run_on_cluster
#      to identify the table, date column, and cell-scope date variables.
#   2. OpenAI model submits a plan via submit_data_recovery_plan (table, date_column,
#      list of variable overrides).
#   3. Framework queries the table for distinct recent dates.
#   4. Framework builds the override block (with begin/end markers) and
#      returns it. Caller prepends it to exec code, re-runs the cell.
#   5. If the query returns no rows: recovery fails (data truly unavailable);
#      caller falls through to the regular fix loop / DATA_UNAVAILABLE path.

DATA_RECOVERY_PROMPT = """You are diagnosing an EMPTY-DATA failure in a cell migrated to OCI AIDP (Python 3.11).

The cell's CODE is correct. The problem is that the cell hardcodes specific dates (in a list, a string, or a range) and the upstream table simply has no rows for those dates. Your job is NOT to modify the cell. Your job is to identify, exactly:

  1. The fully-qualified TABLE the cell is reading from (e.g. "default.db.tbl").
  2. The COLUMN in that table that the cell is filtering by date (e.g. "load_date", "partition_date").
  3. The cell-scope VARIABLES that hold the date filter values (e.g. "partition_date_list", "start_date").

You have these tools:
  - describe_table: get schema of a Spark table (use to confirm the date column name and type).
  - run_on_cluster: execute PySpark on cluster (use to verify the table exists, the column exists, what the column's type/format is).
  - read_notebook_source: read the cell's notebook if needed for context.

When you have identified all three pieces, call the submit_data_recovery_plan tool with:
  - "table": the FQ table name (string).
  - "date_column": the column name to filter on (string).
  - "overrides": a list of {variable, is_list, max_dates} entries â€” one per cell-scope variable that holds date values.

Rules:
  - Do NOT modify the cell's code. The framework will inject a test-only override at execution time; the saved cell stays as-is.
  - Confirm the table actually exists via describe_table or run_on_cluster BEFORE submitting. If the table genuinely doesn't exist, don't submit â€” let the cell fail through the normal path.
  - is_list=true: variable is a list of date strings (e.g. `partition_date_list = ['2026-04-01', '2026-04-02', ...]`). max_dates: how many recent available dates to include (3-14 typical).
  - is_list=false: variable is a single date string (e.g. `start_date = "2026-04-01"`). max_dates: always 1 in this case.
  - Date format: use whatever format the table's column uses (typically YYYY-MM-DD).
  - If multiple cell-scope variables need overrides (e.g. start_date AND end_date), include all of them in overrides.
  - If you cannot identify the table or date column with high confidence, do not submit. The cell will fall through to DATA_UNAVAILABLE handling, which is the correct outcome.

Return ONLY the tool call. No prose."""


async def attempt_data_recovery(
    cell_source: str,
    error_output: str,
    session,
    notebook_path: str = "",
    log_fn=None,
    max_dates_cap: int = 14,
) -> Optional[str]:
    """Identify date-substitution overrides for an empty-data cell failure and
    return an EXEC-only override block (BEGIN/END marker-bracketed) that the
    caller prepends to the cell's exec code. Returns None if recovery is not
    possible (OpenAI model couldn't identify the table, table query returned no rows,
    plan was malformed, etc.).

    The returned block is a Python snippet of the form:

        # === AIDP_DATA_RECOVERY_OVERRIDE_BEGIN (test-only â€” not saved) ===
        # Source table: <fq>, date column: <col>, dates: <values from cluster>
        partition_date_list = ['2024-12-08', '2024-12-07', ...]
        start_date = '2024-12-08'
        # === AIDP_DATA_RECOVERY_OVERRIDE_END ===

    The caller injects this BEFORE the cell's code, executes, and on success
    saves the cell's code WITHOUT this block (via _strip_data_recovery_block).
    """
    import json as _json

    def _log(msg):
        if log_fn:
            log_fn(msg)
        else:
            print(msg)

    if not session:
        _log("[data-recovery] no cluster session â€” skipping")
        return None

    # Build the OpenAI model user message
    ctx = f"\n\nSource notebook path: {notebook_path}\n" if notebook_path else ""
    recovery_input = f"""A cell failed on the AIDP cluster with what looks like an empty-data signature (the code is correct, the data isn't there for the hardcoded dates).

The cell source:
```python
{cell_source[:6000]}
```

The error output:
```
{(error_output or "")[:3000]}
```
{ctx}

Identify the table, date column, and date-holding variables in this cell. Confirm the table exists. Then call submit_data_recovery_plan with your findings. If you cannot identify these with confidence, do not call the tool â€” the cell will be marked DATA_UNAVAILABLE which is the correct outcome."""

    # Call OpenAI model with the recovery prompt. call_opus_with_tools returns the
    # plan as a JSON-encoded string when submit_data_recovery_plan is called.
    try:
        resp, _ = await call_opus_with_tools(
            DATA_RECOVERY_PROMPT, recovery_input,
            session=session, max_tokens=64000, max_tool_rounds=10, log_fn=log_fn,
        )
    except Exception as e:
        _log(f"[data-recovery] OpenAI model call failed: {str(e)[:200]}")
        return None

    # Parse the plan
    try:
        plan = _json.loads(resp)
    except (ValueError, TypeError):
        _log(f"[data-recovery] OpenAI model returned non-JSON (likely chose not to submit a plan)")
        return None

    table = plan.get("table", "").strip()
    date_column = plan.get("date_column", "").strip()
    overrides = plan.get("overrides", [])
    if not table or not date_column or not overrides:
        _log(f"[data-recovery] plan missing required fields: {plan}")
        return None

    # Safety: identifier hygiene (prevent SQL injection via OpenAI model). Allow only
    # alphanumerics, dots, underscores in table/column names.
    import re as _re
    _SAFE_IDENT = _re.compile(r"^[A-Za-z_][A-Za-z_0-9.]*$")
    if not _SAFE_IDENT.match(table):
        _log(f"[data-recovery] unsafe table identifier: {table!r} â€” aborting")
        return None
    if not _SAFE_IDENT.match(date_column):
        _log(f"[data-recovery] unsafe date_column identifier: {date_column!r} â€” aborting")
        return None

    # Determine how many distinct dates we need to fetch (cap across all overrides).
    requested_max = 1
    for ov in overrides:
        if not isinstance(ov, dict):
            _log(f"[data-recovery] malformed override: {ov!r}")
            return None
        var = ov.get("variable", "").strip()
        if not _SAFE_IDENT.match(var):
            _log(f"[data-recovery] unsafe variable identifier: {var!r}")
            return None
        n = int(ov.get("max_dates", 1) or 1)
        if n > requested_max:
            requested_max = min(n, max_dates_cap)

    # Query the cluster for the most recent distinct dates that exist in the
    # column. This is the GROUND TRUTH that drives the substitution.
    query_code = f"""
import json as _j
try:
    rows = spark.sql("SELECT DISTINCT `{date_column}` AS d FROM `{table.replace('.', '`.`')}` WHERE `{date_column}` IS NOT NULL ORDER BY `{date_column}` DESC LIMIT {requested_max}").collect()
    vals = [str(r[0]) for r in rows if r[0] is not None]
    print(_j.dumps({{"ok": True, "dates": vals}}))
except Exception as _e:
    print(_j.dumps({{"ok": False, "error": str(_e)[:300]}}))
""".strip()
    try:
        from aidp_executor import format_outputs
        result = await session.execute(query_code, timeout=120)
        raw = format_outputs(result.get("outputs", [])).strip()
        # Last line is the JSON print result
        json_line = raw.split("\n")[-1] if raw else ""
        probe = _json.loads(json_line)
    except Exception as e:
        _log(f"[data-recovery] cluster probe failed: {str(e)[:200]}")
        return None

    if not probe.get("ok"):
        _log(f"[data-recovery] cluster probe returned error: {probe.get('error', '?')}")
        return None

    available_dates = probe.get("dates", [])
    if not available_dates:
        _log(f"[data-recovery] table {table} has NO available dates in column {date_column} â€” recovery not possible")
        return None

    # Build the override block. Each variable gets its slice of available_dates.
    lines = [
        "# === AIDP_DATA_RECOVERY_OVERRIDE_BEGIN (test-only â€” not saved) ===",
        f"# Source table: {table}, date column: {date_column}",
        f"# Available dates (most recent first): {available_dates[:3]}{'...' if len(available_dates) > 3 else ''}",
    ]
    for ov in overrides:
        var = ov["variable"].strip()
        is_list = bool(ov.get("is_list", False))
        n = min(int(ov.get("max_dates", 1) or 1), len(available_dates))
        if n < 1:
            n = 1
        if is_list:
            slice_ = available_dates[:n]
            lines.append(f"{var} = {repr(slice_)}")
        else:
            lines.append(f"{var} = {repr(available_dates[0])}")
    lines.append("# === AIDP_DATA_RECOVERY_OVERRIDE_END ===")
    block = "\n".join(lines) + "\n"

    # Defensive: ensure the built block compiles as valid Python. Guards
    # against pathological edge cases (OpenAI model returning a variable name that
    # is a Python reserved keyword like "class" or "for"; a date value with
    # a control character; etc.) that would otherwise turn the cell failure
    # from "empty data" into "SyntaxError in injected code" and confuse the
    # downstream fix loop.
    try:
        compile(block, "<aidp-data-recovery-override>", "exec")
    except SyntaxError as _se:
        _log(f"[data-recovery] override block failed compile check: {_se} â€” aborting recovery")
        return None

    _log(f"[data-recovery] built override: table={table} col={date_column} dates={available_dates[:3]} vars={[ov['variable'] for ov in overrides]}")
    return block


def _extract_code_from_response(resp: str) -> str:
    """Extract Python code from OpenAI model response, stripping explanation text.
    Handles: markdown fences, preamble text before code, trailing explanations."""
    resp = resp.strip()

    # Strip markdown code fences
    if resp.startswith("```"):
        lines = resp.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        resp = "\n".join(lines).strip()

    # If the response contains code fences anywhere (OpenAI model wrapped code in them)
    if "```python" in resp or "```\n" in resp:
        # Extract content between first ``` and last ```
        import re as _re
        m = _re.search(r'```(?:python)?\s*\n(.*?)```', resp, _re.DOTALL)
        if m:
            resp = m.group(1).strip()

    # If there's preamble text before actual Python code, try to strip it
    # Look for the first line that looks like Python code
    lines = resp.split("\n")
    code_start = 0

    # Patterns that indicate prose/explanation (NOT Python code)
    prose_starters = (
        "The ", "This ", "I ", "Note", "Here", "We ", "Since ", "Good", "Now ",
        "Let ", "First", "OK", "Alright", "Sure", "Based ", "After ", "Looking ",
        "To ", "In ", "However", "But ", "Also", "So ", "My ", "Actually",
    )

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue

        # Check if this line looks like Python code
        if any(stripped.startswith(kw) for kw in [
            "import ", "from ", "def ", "class ", "if ", "for ", "while ", "try:",
            "with ", "return ", "raise ", "yield ", "async ", "await ",
            "#", "spark.", "dbutils.", "display(", "sql(", "print(",
        ]) or ("=" in stripped and not stripped.endswith(".")
               and ":" not in stripped.split("=")[0]
               and not stripped[0].isdigit()
               and not any(stripped.startswith(p) for p in prose_starters)):
            code_start = i
            break

        # Prose indicators - skip these lines
        if stripped.startswith(prose_starters):
            continue
        if stripped.startswith("`"):
            continue
        # Numbered list items (1. 2. etc)
        if stripped[0].isdigit() and ("." in stripped[:3] or ")" in stripped[:3]):
            continue
        # Bullet points
        if stripped.startswith(("- ", "* ", "â€¢ ")):
            continue

        # Otherwise assume it's code
        code_start = i
        break

    if code_start > 0:
        resp = "\n".join(lines[code_start:]).strip()

    # Also strip trailing prose after the last line of code
    lines = resp.split("\n")
    code_end = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if not stripped:
            continue
        if stripped.startswith(prose_starters) or (stripped[0].isdigit() and "." in stripped[:3]):
            code_end = i
            continue
        break
    if code_end < len(lines):
        resp = "\n".join(lines[:code_end]).strip()

    return resp


# â”€â”€â”€ Notebook Processing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def read_notebook(path: str) -> Tuple[dict, List[Optional[str]], str]:
    """Read notebook, extract outputs, build readable text."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        nb = json.load(f)

    outputs = []
    readable_parts = []
    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "unknown")
        source = "".join(cell.get("source", []))
        readable_parts.append(f"--- Cell {i} [{cell_type}] ---")
        readable_parts.append(source)
        readable_parts.append("")

        # Extract original output
        cell_outputs = cell.get("outputs", [])
        text_parts = []
        for out in cell_outputs:
            if out.get("output_type") == "stream":
                text_parts.append("".join(out.get("text", [])))
            elif out.get("output_type") in ("execute_result", "display_data"):
                data = out.get("data", {})
                if "text/plain" in data:
                    v = data["text/plain"]
                    text_parts.append("".join(v) if isinstance(v, list) else v)
        outputs.append("".join(text_parts) if text_parts else None)

    readable = "\n".join(readable_parts)
    if len(readable) > 80000:
        readable = readable[:80000] + "\n[... TRUNCATED ...]"
    return nb, outputs, readable


async def process_single_notebook(notebook_path: str, session: AIDPSession) -> dict:
    """Full pipeline for one notebook: download, analyze, migrate, test, fix, upload, cleanup."""
    nb_name = os.path.basename(notebook_path).replace(".ipynb", "")
    output_base = f"/Workspace/{OUTPUT_FOLDER}/{os.path.dirname(notebook_path)}/{nb_name}"

    print(f"\n{'='*60}")
    print(f"PROCESSING: {notebook_path}")
    print(f"{'='*60}")

    # Create temp dir for this notebook
    tmpdir = tempfile.mkdtemp(prefix=f"aidp_migrate_{nb_name}_")
    local_nb = os.path.join(tmpdir, "original.ipynb")
    total_tokens = 0

    try:
        # â”€â”€ STEP 1: Download â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        print(f"  [1/6] Downloading...")
        try:
            download_notebook(notebook_path, local_nb)
        except Exception as e:
            print(f"  DOWNLOAD FAILED: {e}")
            return {"path": notebook_path, "status": "download_failed", "error": str(e)[:200]}

        nb, original_outputs, readable = read_notebook(local_nb)
        total_cells = len(nb.get("cells", []))
        outputs_with_data = sum(1 for o in original_outputs if o)
        print(f"  Downloaded: {total_cells} cells, {outputs_with_data} with saved outputs")

        # â”€â”€ STEP 2: Analyze with OpenAI model â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        print(f"  [2/6] Analyzing with OpenAI GPT-5.5...")
        analysis_text, usage = call_opus(
            ANALYSIS_PROMPT,
            f"Analyze this notebook for Databricks-to-AIDP migration:\n\nPath: {notebook_path}\n\n```\n{readable}\n```"
        )
        total_tokens += usage["input"] + usage["output"]
        print(f"  Analysis: {usage['input']+usage['output']:,} tokens")

        # Save analysis report
        analysis_path = os.path.join(tmpdir, "analysis_report.md")
        with open(analysis_path, 'w') as f:
            f.write(f"# Analysis Report: {notebook_path}\n\n{analysis_text}")

        # â”€â”€ STEP 3: Migrate with OpenAI model â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        print(f"  [3/6] Migrating with OpenAI GPT-5.5...")
        migrate_text, usage = call_opus(
            MIGRATION_PROMPT,
            f"Migrate this notebook. Path: {notebook_path}\n\nAnalysis:\n{analysis_text[:5000]}\n\nNotebook:\n```\n{readable}\n```"
        )
        total_tokens += usage["input"] + usage["output"]
        print(f"  Migration: {usage['input']+usage['output']:,} tokens")

        # Parse migrated cells
        cleaned = migrate_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines)
        start = cleaned.find("{")
        end = cleaned.rfind("}") + 1
        if start >= 0 and end > start:
            cleaned = cleaned[start:end]

        try:
            migrate_result = json.loads(cleaned)
            migrated_cells = migrate_result.get("cells", [])
            migration_notes = migrate_result.get("migration_notes", [])
        except json.JSONDecodeError:
            migrated_cells = [{"cell_type": c.get("cell_type", "code"),
                               "source": "".join(c.get("source", [])),
                               "classification": "READ_ONLY"} for c in nb.get("cells", [])]
            migration_notes = ["JSON parse failed - using original cells"]

        # Build migrated notebook
        migrated_nb = copy.deepcopy(nb)
        new_cells = []
        for mc in migrated_cells:
            cell = {"cell_type": mc.get("cell_type", "code"), "metadata": {},
                    "source": [mc.get("source", "")], "outputs": [], "execution_count": None}
            if mc.get("cell_type") == "markdown":
                cell.pop("outputs", None)
                cell.pop("execution_count", None)
            new_cells.append(cell)
        migrated_nb["cells"] = new_cells

        # Save migration report
        migration_report = f"# Migration Report: {notebook_path}\n\n## Notes\n"
        for note in migration_notes:
            migration_report += f"- {note}\n"

        migrated_path = os.path.join(tmpdir, "migrated.ipynb")
        with open(migrated_path, 'w') as f:
            json.dump(migrated_nb, f, indent=1)

        migration_report_path = os.path.join(tmpdir, "migration_report.md")
        with open(migration_report_path, 'w') as f:
            f.write(migration_report)

        # â”€â”€ STEP 4: Test on cluster â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        print(f"  [4/6] Testing on cluster ({len(migrated_cells)} cells)...")

        # Bootstrap aidp_compat
        await session.execute("import sys; sys.path.insert(0, '/Workspace/migration-dependencies/python_libs/')", timeout=30)

        cell_results = []
        executed_code = []
        cells_ok = 0
        cells_failed = 0
        cells_skipped = 0
        cells_fixed = 0
        output_matches = 0
        output_diffs = 0

        for i, mc in enumerate(migrated_cells):
            classification = mc.get("classification", "SKIP")
            source = mc.get("source", "").strip()
            cell_type = mc.get("cell_type", "code")

            if cell_type != "code" or not source or classification in ("SKIP", "WRITE", "NOTIFICATION"):
                cell_results.append({"cell": i, "classification": classification, "status": "skipped"})
                cells_skipped += 1
                continue

            # Execute READ_ONLY cell
            current_code = source
            cell_passed = False

            for attempt in range(6):  # 0=first try, 1-2=OpenAI model, 3-5=OpenAI model
                result = await session.execute(current_code, timeout=120)
                status = result.get("status", "error")
                output = format_outputs(result.get("outputs", []))

                if status == "ok":
                    cell_passed = True
                    executed_code.append(current_code)

                    # Compare output
                    orig = original_outputs[i] if i < len(original_outputs) else None
                    match_detail = None
                    if orig:
                        orig_clean = " ".join(orig.strip().split())
                        actual_clean = " ".join((output or "").strip().split())
                        if orig_clean == actual_clean:
                            match_detail = "exact match"
                            output_matches += 1
                        else:
                            match_detail = "differs"
                            output_diffs += 1

                    if attempt > 0:
                        cells_fixed += 1
                        new_cells[i]["source"] = [current_code]

                    cell_results.append({
                        "cell": i, "classification": "READ_ONLY", "status": "ok",
                        "attempts": attempt + 1, "fixed": attempt > 0,
                        "code": current_code[:500],
                        "output": (output or "")[:1000],
                        "original_output": (orig[:500] if orig else None),
                        "output_match": match_detail,
                    })
                    cells_ok += 1
                    break
                else:
                    if attempt < 5:
                        model_name = "OpenAI model" if attempt < 2 else "OpenAI model"
                        print(f"    Cell {i} FAIL (attempt {attempt+1}), asking {model_name}...")
                        try:
                            current_code = call_fix(current_code, output, executed_code, attempt + 1)
                        except:
                            break
                    else:
                        cell_results.append({
                            "cell": i, "classification": "READ_ONLY", "status": "error",
                            "attempts": 6, "code": current_code[:500],
                            "output": (output or "")[:1000],
                        })
                        cells_failed += 1

        # â”€â”€ STEP 5: Build test report â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        overall = "PASS" if cells_failed == 0 else "PARTIAL" if cells_ok > 0 else "FAIL"
        print(f"  [5/6] Result: {overall} | OK:{cells_ok} Failed:{cells_failed} Skipped:{cells_skipped} Fixed:{cells_fixed}")

        test_report = f"""# Test Report: {notebook_path}

## Summary
- **Date**: {datetime.now().isoformat()}
- **Result**: **{overall}**
- **Cells**: {cells_ok} OK, {cells_failed} failed, {cells_skipped} skipped, {cells_fixed} auto-fixed
- **Output comparison**: {output_matches} match, {output_diffs} differ
- **Total OpenAI tokens**: {total_tokens:,}

## Cell Results
"""
        for cr in cell_results:
            ci = cr["cell"]
            st = cr.get("status", "?")
            cls = cr.get("classification", "?")

            if st == "skipped":
                test_report += f"\n### Cell {ci} - SKIPPED ({cls})\n"
                continue

            test_report += f"\n### Cell {ci} - {st.upper()}"
            if cr.get("fixed"):
                test_report += f" (fixed, attempt {cr.get('attempts', '?')})"
            test_report += f"\n"

            if cr.get("code"):
                code_display = cr["code"] if len(cr["code"]) < 1000 else cr["code"][:1000] + "\n# ... truncated"
                test_report += f"\n```python\n{code_display}\n```\n"
            if cr.get("output"):
                out_display = cr["output"] if len(cr["output"]) < 1500 else cr["output"][:1500] + "\n... truncated"
                test_report += f"\n**Output:**\n```\n{out_display}\n```\n"
            if cr.get("original_output"):
                test_report += f"\n**Original output:** `{cr['original_output'][:200]}`\n"
                test_report += f"**Match:** {cr.get('output_match', 'N/A')}\n"

        test_report_path = os.path.join(tmpdir, "test_report.md")
        with open(test_report_path, 'w') as f:
            f.write(test_report)

        # Save final notebook (with fixes applied)
        final_path = os.path.join(tmpdir, "final.ipynb")
        with open(final_path, 'w') as f:
            json.dump(migrated_nb, f, indent=1)

        # â”€â”€ STEP 6: Upload artifacts to agent-migrated/ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        print(f"  [6/6] Uploading artifacts to {OUTPUT_FOLDER}/...")

        # Create output folder structure via executor
        await session.execute(f"import os; os.makedirs('{output_base}', exist_ok=True)", timeout=30)

        artifacts = [
            (analysis_path, f"{output_base}/analysis_report.md"),
            (migrated_path, f"{output_base}/migrated.ipynb"),
            (migration_report_path, f"{output_base}/migration_report.md"),
            (test_report_path, f"{output_base}/test_report.md"),
            (final_path, f"{output_base}/final.ipynb"),
        ]

        for local_file, remote_path in artifacts:
            with open(local_file, 'r') as f:
                content = f.read()
            # Write via executor
            import base64
            b64 = base64.b64encode(content.encode('utf-8')).decode('ascii')
            if len(b64) < 50000:
                code = f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
"""
                await session.execute(code, timeout=60)
            else:
                # Skip very large files for now
                print(f"    Skipping upload of {os.path.basename(local_file)} (too large)")

        return {
            "path": notebook_path, "status": overall,
            "ok": cells_ok, "failed": cells_failed, "skipped": cells_skipped,
            "fixed": cells_fixed, "output_matches": output_matches,
            "output_diffs": output_diffs, "tokens": total_tokens,
        }

    finally:
        # â”€â”€ STEP 7: Cleanup local temp files â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        shutil.rmtree(tmpdir, ignore_errors=True)


# â”€â”€â”€ Batch Processing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def run_batch(notebooks: List[str], cluster_id: str, parallel: int):
    """Process notebooks in parallel with separate sessions."""
    print(f"{'='*60}")
    print(f"Agent Migration Pipeline")
    print(f"{'='*60}")
    print(f"Notebooks: {len(notebooks)}")
    print(f"Parallel: {parallel}")
    print(f"Cluster: {cluster_id}")
    print(f"Output: /Workspace/{OUTPUT_FOLDER}/")
    print(f"Started: {datetime.now().isoformat()}")

    # Create output folder
    try:
        create_folder(OUTPUT_FOLDER)
    except:
        pass

    # Split into chunks
    chunks = [[] for _ in range(parallel)]
    for i, nb in enumerate(notebooks):
        chunks[i % parallel].append(nb)

    all_results = []

    async def worker(worker_id: int, chunk: List[str]):
        session = None
        results = []
        for i, nb_path in enumerate(chunk):
            # Reconnect every 10 notebooks
            if session is None or i % 10 == 0:
                if session:
                    try: await session.close()
                    except: pass
                session = AIDPSession(cluster_id=cluster_id)
                await session.connect()

            try:
                result = await process_single_notebook(nb_path, session)
                results.append(result)
            except Exception as e:
                print(f"  [Worker {worker_id}] Error: {nb_path}: {e}")
                results.append({"path": nb_path, "status": "error", "error": str(e)[:200]})
                try: await session.close()
                except: pass
                session = None

        if session:
            try: await session.close()
            except: pass
        return results

    tasks = [worker(i, chunk) for i, chunk in enumerate(chunks) if chunk]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    for cr in chunk_results:
        if isinstance(cr, list):
            all_results.extend(cr)

    # Summary
    total_ok = sum(r.get("ok", 0) for r in all_results if isinstance(r, dict))
    total_fail = sum(r.get("failed", 0) for r in all_results if isinstance(r, dict))
    total_fix = sum(r.get("fixed", 0) for r in all_results if isinstance(r, dict))
    total_tokens = sum(r.get("tokens", 0) for r in all_results if isinstance(r, dict))
    pass_count = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "PASS")

    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"  Notebooks: {len(all_results)} | PASS: {pass_count}")
    print(f"  Cells OK: {total_ok} | Failed: {total_fail} | Fixed: {total_fix}")
    print(f"  Tokens: {total_tokens:,}")
    print(f"  Output: /Workspace/{OUTPUT_FOLDER}/")
    print(f"{'='*60}")

    return all_results


async def main():
    global AIDP_BASE, DATALAKE_OCID, WORKSPACE_ID, OCI_PROFILE, DOWNLOAD_META_URL, UPLOAD_FOLDER_URL, SIGNER
    parser = argparse.ArgumentParser(description="Agent Migration Pipeline")
    parser.add_argument("--parallel", type=int, default=20)
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER,
                        help="AIDP cluster ID (default: %(default)s)")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=None)
    parser.add_argument("--notebook", help="Single notebook path")
    # AIDP environment â€” override these for non-Customer deployments
    parser.add_argument("--aidp-base", default=AIDP_BASE,
                        help="AIDP REST endpoint base URL (default: %(default)s)")
    parser.add_argument("--datalake-ocid", default=DATALAKE_OCID,
                        help="AIDP data lake OCID (default: %(default)s)")
    parser.add_argument("--workspace-id", default=WORKSPACE_ID,
                        help="AIDP workspace UUID (default: %(default)s)")
    parser.add_argument("--oci-profile", default=OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    args = parser.parse_args()

    # Apply environment config â€” must happen before any code uses these globals
    AIDP_BASE       = args.aidp_base
    DATALAKE_OCID   = args.datalake_ocid
    WORKSPACE_ID    = args.workspace_id
    OCI_PROFILE     = args.oci_profile
    DOWNLOAD_META_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                         f"/actions/downloadFileMeta")
    UPLOAD_FOLDER_URL = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                         f"/objects")
    SIGNER = None  # reset so signer() picks up new OCI_PROFILE

    if args.notebook:
        session = AIDPSession(cluster_id=args.cluster)
        await session.connect()
        try:
            result = await process_single_notebook(args.notebook, session)
            print(json.dumps(result, indent=2))
        finally:
            await session.close()
        return

    # Load notebook list
    report_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports", "notebook_list.json")
    with open(report_path) as f:
        notebooks = json.load(f)

    paths = [nb["path"] for nb in notebooks]
    paths = paths[args.start:args.end]

    await run_batch(paths, args.cluster, args.parallel)


if __name__ == "__main__":
    asyncio.run(main())
