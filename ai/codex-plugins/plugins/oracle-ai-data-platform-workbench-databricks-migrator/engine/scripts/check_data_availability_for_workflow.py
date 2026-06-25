#!/usr/bin/env python3
"""
Check Data Availability for a Workflow
=========================================
Scans notebooks in an AIDP workflow for READ data dependencies (storage paths
and table references), maps S3/DBFS paths to OCI equivalents, and verifies
availability on the AIDP cluster.

Produces a per-notebook data readiness report to help decide whether to
proceed with migration.

Usage:
    # Check data for a workflow job
    python3 check_data_availability_for_workflow.py \\
        --job-key <CLUSTER_ID_ALT> \\
        --cluster <your-cluster-id>

    # From a pre-built manifest
    python3 check_data_availability_for_workflow.py \\
        --manifest reports/sample_workflow_manifest.json \\
        --cluster <your-cluster-id>

    # Check only specific tasks
    python3 check_data_availability_for_workflow.py \\
        --job-key <uuid> --cluster <uuid> \\
        --only-tasks "02_BaseData_ExampleApp,05_ExampleApp_Feature"
"""

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from aidp_executor import AIDPSession
from build_dag_from_workflow import (
    fetch_job_definition,
    extract_tasks_from_job,
)

# ─── Defaults ───────────────────────────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LAKE_OCID = os.environ.get("AIDP_DATALAKE", "")
DEFAULT_WORKSPACE_ID = os.environ.get("AIDP_WORKSPACE", "")
DEFAULT_OCI_PROFILE = os.environ.get("OCI_PROFILE", "DEFAULT")
DEFAULT_CLUSTER = os.environ.get("AIDP_CLUSTER", "")

# ─── Bucket → Namespace Mapping ────────────────────────────────────

_BUCKET_NS: dict = {}


def load_bucket_ns_mapping(json_path: str = None):
    """Load {bucket_name: namespace} from oci_bucket_tenancy_mapping.json."""
    global _BUCKET_NS
    if json_path is None:
        json_path = os.path.join(PROJECT_DIR, "config", "oci_bucket_tenancy_mapping.json")
    if not os.path.exists(json_path):
        print(f"WARNING: {json_path} not found — S3→OCI mapping unavailable")
        return
    with open(json_path) as f:
        _BUCKET_NS = json.load(f)
    print(f"[data-check] Loaded {len(_BUCKET_NS)} bucket→namespace mappings")


def map_to_oci_path(source_path: str) -> dict:
    """Map an S3/DBFS/mnt path to OCI path using bucket→namespace index.

    Returns: {
        "source": original path,
        "oci_path": mapped oci:// path or None,
        "bucket": extracted bucket name,
        "namespace": resolved namespace or None,
    }
    """
    bucket = None
    sub_path = ""

    if source_path.startswith("s3://") or source_path.startswith("s3a://"):
        m = re.match(r's3a?://([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"
    elif source_path.startswith("dbfs:/"):
        m = re.match(r'dbfs:/(?:mnt/)?([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"
    elif source_path.startswith("/mnt/"):
        m = re.match(r'/mnt/([^/]+)(/.*)?', source_path)
        if m:
            bucket = m.group(1)
            sub_path = m.group(2) or "/"

    if not bucket:
        return {"source": source_path, "oci_path": None, "bucket": None, "namespace": None}

    # Direct lookup in bucket→namespace mapping
    ns = _BUCKET_NS.get(bucket)
    if not ns:
        # Try with oci- prefix
        ns = _BUCKET_NS.get(f"oci-{bucket}")
        if ns:
            bucket = f"oci-{bucket}"
    if not ns:
        # Try stripping oci- prefix
        if bucket.startswith("oci-"):
            ns = _BUCKET_NS.get(bucket[4:])
            if ns:
                bucket = bucket[4:]

    oci_path = f"oci://{bucket}@{ns}{sub_path}" if ns else None
    return {"source": source_path, "oci_path": oci_path, "bucket": bucket, "namespace": ns}


# ─── Read vs Write Classification ──────────────────────────────────

# Patterns that indicate READ operations
_READ_PATTERNS = [
    r'spark\.read\.',
    r'spark\.table\s*\(',
    r'spark\.sql\s*\(\s*["\'](?:SELECT|SHOW|DESCRIBE|WITH)',
    r'pd\.read_',
    r'pandas\.read_',
    r'open\s*\([^)]*["\']r["\']',
    r'open\s*\([^)]*\)\s*(?!.*["\']w)',
    r'dbutils\.fs\.ls\s*\(',
    r'dbutils\.fs\.head\s*\(',
    r'\.load\s*\(',
    r'FROM\s+',
    r'JOIN\s+',
    r'\.option\(.*\.load\(',
    r'sc\.textFile\s*\(',
    r'sc\.wholeTextFiles\s*\(',
]

# Patterns that indicate WRITE operations
_WRITE_PATTERNS = [
    r'\.write\.',
    r'\.save\s*\(',
    r'\.saveAsTable\s*\(',
    r'INSERT\s+(?:INTO|OVERWRITE)',
    r'CREATE\s+TABLE',
    r'dbutils\.fs\.put\s*\(',
    r'dbutils\.fs\.mkdirs\s*\(',
    r'dbutils\.fs\.cp\s*\(',
    r'dbutils\.fs\.mv\s*\(',
    r'open\s*\([^)]*["\']w["\']',
    r'\.to_csv\s*\(',
    r'\.to_parquet\s*\(',
    r'\.to_json\s*\(',
]


def classify_path_usage(source_code: str, path: str) -> str:
    """Classify whether a path is used for READ or WRITE in the given code.

    Returns 'read', 'write', or 'unknown'.
    Looks at the lines containing the path to determine context.
    """
    lines_with_path = []
    for line in source_code.splitlines():
        if path in line or (len(path) > 10 and path[:10] in line):
            lines_with_path.append(line)

    if not lines_with_path:
        return "unknown"

    context = "\n".join(lines_with_path)

    is_read = any(re.search(p, context, re.IGNORECASE) for p in _READ_PATTERNS)
    is_write = any(re.search(p, context, re.IGNORECASE) for p in _WRITE_PATTERNS)

    if is_write and not is_read:
        return "write"
    if is_read and not is_write:
        return "read"
    if is_read and is_write:
        return "read"  # if both, treat as read (conservative)
    return "unknown"


def classify_table_usage(source_code: str, table: str) -> str:
    """Classify whether a table is READ or WRITE."""
    lines_with_table = [l for l in source_code.splitlines() if table in l]
    if not lines_with_table:
        return "unknown"

    context = "\n".join(lines_with_table)

    write_patterns = [
        r'INSERT\s+(?:INTO|OVERWRITE)\s+' + re.escape(table),
        r'CREATE\s+TABLE.*' + re.escape(table),
        r'\.saveAsTable\s*\(\s*["\']' + re.escape(table),
        r'\.write\.',
    ]
    read_patterns = [
        r'FROM\s+' + re.escape(table),
        r'JOIN\s+' + re.escape(table),
        r'spark\.table\s*\(\s*["\']' + re.escape(table),
        r'spark\.read\.table\s*\(\s*["\']' + re.escape(table),
        r'SELECT.*FROM.*' + re.escape(table),
    ]

    is_write = any(re.search(p, context, re.IGNORECASE) for p in write_patterns)
    is_read = any(re.search(p, context, re.IGNORECASE) for p in read_patterns)

    if is_write and not is_read:
        return "write"
    if is_read:
        return "read"
    return "unknown"


# ─── AIDP Output Helpers ────────────────────────────────────────────

def _unwrap(raw: str) -> str:
    """Unwrap AIDP JSON output wrapper.

    AIDP wraps display output as [{"type": ..., "value": ...}]; an empty
    wrapper [] carries no value and must be dropped, otherwise it gets
    concatenated ahead of real stdout (e.g. "[]{...json...}") and breaks the
    json.loads() at the call sites.
    """
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            if not parsed:
                return ""  # empty wrapper — no value
            if "value" in parsed[0]:
                return parsed[0]["value"]
    except Exception:
        pass
    return raw


def unwrap_outputs(outputs: list) -> str:
    """Extract text from AIDP session outputs."""
    text = ""
    seen = set()
    for o in outputs:
        raw = ""
        if o.get("type") == "stream":
            raw = o.get("text", "")
        elif o.get("type") == "execute_result":
            raw = o.get("data", {}).get("text/plain", "")
        val = _unwrap(raw)
        if val and val not in seen:
            text += val
            seen.add(val)
    return text


def parse_cluster_json(output: str) -> dict:
    """Parse the result object from concatenated cluster stdout.

    Every cluster block here ends with exactly one `print(json.dumps(<dict>))`,
    but Spark job-progress events (JSON arrays like [{"stageStatus":"running"}])
    and empty [] display wrappers get interleaved into the stream ahead of it.
    Since every real result is a JSON OBJECT ({...}) and the noise is arrays,
    scan for the LAST top-level {...} span (string-aware brace matching; objects
    nested inside arrays are ignored) and parse that. Raises ValueError if none.
    """
    try:
        v = json.loads(output)
        if isinstance(v, dict):
            return v
    except Exception:
        pass

    spans = []
    depth = 0
    obj_start = None
    in_str = False
    esc = False
    quote = ""
    for i, ch in enumerate(output):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            continue
        if ch in ('"', "'"):
            in_str = True
            quote = ch
        elif ch in "{[":
            if ch == "{" and depth == 0:
                obj_start = i
            depth += 1
        elif ch in "}]":
            if depth > 0:
                depth -= 1
                if depth == 0 and ch == "}" and obj_start is not None:
                    spans.append((obj_start, i + 1))
                    obj_start = None
    for s, e in reversed(spans):
        try:
            return json.loads(output[s:e])
        except Exception:
            continue
    raise ValueError("no top-level JSON object found in cluster output")


# ─── Data Reference Extraction ──────────────────────────────────────

async def extract_data_refs(session, notebook_paths: list) -> dict:
    """Extract storage paths and table references from notebooks on cluster.

    Returns per-notebook dict:
    {
        notebook_path: {
            "paths": [{"path": str, "usage": "read"|"write"|"unknown"}],
            "tables": [{"table": str, "usage": "read"|"write"|"unknown"}],
            "unresolved": [str],  # dynamic/parameterized paths
        }
    }
    """
    paths_json = json.dumps(notebook_paths)
    result = await session.execute(f"""
import json, os, re

notebook_paths = {paths_json}
all_data = {{}}

for nb_path in notebook_paths:
    # Try path variants
    candidates = [nb_path]
    if not nb_path.startswith("/Workspace"):
        candidates.append("/Workspace" + nb_path)
    if not nb_path.endswith(".ipynb"):
        candidates = [c + ".ipynb" for c in candidates] + candidates

    nb_data = {{"cells_source": "", "paths": [], "tables": [], "unresolved": [],
                "python_imports": [], "scala_imports": []}}

    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path) as fh:
                nb = json.load(fh)
            def _strip_comments(code):
                # Remove multi-line: Python triple-quotes and Scala /* */
                code = re.sub(r'(\\\"\\\"\\\"[\\s\\S]*?\\\"\\\"\\\")', '', code)
                code = re.sub(r"(\\\'\\\'\\\'[\\s\\S]*?\\\'\\\'\\\')", '', code)
                code = re.sub(r'/\\*[\\s\\S]*?\\*/', '', code)
                # Remove single-line: Python # and Scala //
                code = re.sub(r'#[^\\n]*', '', code)
                code = re.sub(r'//[^\\n]*', '', code)
                return code

            # Notebook default language (Jupyter/Databricks metadata).
            default_lang = (nb.get("metadata", {{}}).get("language_info", {{}}).get("name", "") or "python").lower()
            all_source = ""
            for cell in nb.get("cells", []):
                raw_src = "".join(cell.get("source", []))
                # Databricks marks a non-default-language cell with a leading
                # %scala/%python/%sql/%r magic. Some exports wrap the whole cell
                # body as "# MAGIC <code>" — recover the real code first, else
                # _strip_comments would delete every line.
                if "# MAGIC" in raw_src or "#MAGIC" in raw_src:
                    raw_src = "\\n".join(
                        (re.match(r'^#\\s*MAGIC ?(.*)$', l).group(1)
                         if re.match(r'^#\\s*MAGIC', l) else l)
                        for l in raw_src.splitlines()
                    )
                # Per-cell language overrides the notebook default. This is the
                # authoritative signal — Databricks encodes a cell's language as
                # this magic, there is no separate per-cell language field.
                cell_lang = default_lang
                _first = next((l.strip() for l in raw_src.splitlines() if l.strip()), "")
                _lm = re.match(r'^%(\\w+)', _first)
                if _lm:
                    _lang = _lm.group(1).lower()
                    cell_lang = {{"py": "python", "python": "python", "scala": "scala",
                                 "sql": "sql", "r": "r"}}.get(_lang, _lang)
                src = _strip_comments(raw_src)
                all_source += src + "\\n"

                # S3 paths
                for m in re.findall(r's3[a]?://[\\w\\-\\.]+/[\\w\\-\\./]*', src):
                    nb_data["paths"].append(m)
                # OCI paths
                for m in re.findall(r'oci://[\\w\\-\\.]+@[\\w]+/[\\w\\-\\./]*', src):
                    nb_data["paths"].append(m)
                # DBFS paths
                for m in re.findall(r'dbfs:/[\\w\\-\\./]+', src):
                    nb_data["paths"].append(m)
                # /mnt paths
                for m in re.findall(r'/mnt/[\\w\\-\\./]+', src):
                    nb_data["paths"].append(m)
                # Volume paths
                for m in re.findall(r'/Volumes/[\\w\\-\\./]+', src):
                    nb_data["paths"].append(m)

                # Tables: look for table references in SQL keywords and Spark API calls only.
                # Scanning all dotted identifiers picks up too much noise (method calls,
                # email addresses, Java API calls, column references, etc.)
                _qt = chr(39) + chr(34)
                _SKIP_LINE = ("self", "spark", "dbutils", "sc", "np", "pd", "plt")

                for line in src.splitlines():
                    stripped = line.strip()
                    # Skip Python imports: "from X.Y import ..." or "import X.Y"
                    if re.match(r'^(?:from|import)\\s+', stripped, re.IGNORECASE) and 'import' in stripped:
                        continue
                    # SQL keywords: FROM, JOIN, INTO, TABLE, OVERWRITE
                    for m in re.findall(r'(?:FROM|JOIN|INTO|TABLE|OVERWRITE)\\s+(\\w+\\.\\w+)', line, re.IGNORECASE):
                        if m.split(".")[0] not in _SKIP_LINE:
                            nb_data["tables"].append(m)
                # Spark API: spark.table("schema.table"), spark.read.table("schema.table")
                for m in re.findall(r"spark\\.(?:sql|table|read\\.table)\\s*\\(\\s*[" + _qt + r"]([\\w]+\\.[\\w]+)", src):
                    nb_data["tables"].append(m)
                # saveAsTable("schema.table")
                for m in re.findall(r"\\.saveAsTable\\s*\\(\\s*[" + _qt + r"]([\\w]+\\.[\\w]+)", src):
                    nb_data["tables"].append(m)
                # insertInto("schema.table")
                for m in re.findall(r"\\.insertInto\\s*\\(\\s*[" + _qt + r"]([\\w]+\\.[\\w]+)", src):
                    nb_data["tables"].append(m)

                # Detect unresolved/parameterized paths (f-strings with variables)
                _fpat = r"f[" + _qt + r"]((?:s3|oci|dbfs|/mnt|/Volumes)[^" + _qt + r"]*)[" + _qt + r"]"
                for m in re.findall(_fpat, src):
                    if "{{" in m:
                        nb_data["unresolved"].append(m)

                # Python imports: "from X import ..." or "import X"
                # Standard library + common pre-installed packages on Spark clusters
                _SKIP_PY = {{"os", "sys", "json", "re", "math", "time", "datetime",
                           "collections", "functools", "itertools", "typing", "abc",
                           "io", "copy", "pickle", "csv", "pathlib", "shutil", "hashlib",
                           "base64", "uuid", "socket", "http", "urllib", "logging",
                           "warnings", "subprocess", "multiprocessing", "threading",
                           "importlib", "operator", "string", "struct", "enum",
                           "contextlib", "textwrap", "traceback", "inspect", "gc",
                           "signal", "tempfile", "glob", "fnmatch", "stat",
                           "builtins", "__future__", "array", "bisect", "heapq",
                           "decimal", "fractions", "random", "statistics",
                           "pprint", "reprlib", "types", "weakref",
                           "codecs", "unicodedata", "locale", "gettext",
                           "argparse", "configparser", "secrets", "hmac",
                           "xml", "html", "email", "mimetypes",
                           "concurrent", "asyncio", "queue",
                           "ctypes", "platform", "sysconfig",
                           "unittest", "doctest", "pdb",
                           "zipfile", "gzip", "bz2", "lzma", "tarfile",
                           "sqlite3", "dbm", "shelve",
                           "IPython", "ipykernel", "ipywidgets",
                           # Common pre-installed on Spark/AIDP clusters
                           "pyspark", "pandas", "numpy", "scipy", "sklearn",
                           "matplotlib", "seaborn", "plotly",
                           "pyarrow", "koalas", "delta",
                           "mlflow", "hyperopt",
                           "requests", "boto3", "botocore",
                           "oci", "azure", "google",
                           "py4j", "six", "dateutil", "pytz",
                           "setuptools", "pip", "pkg_resources", "wheel"}}
                # JVM namespace roots — secondary guard for python cells in case
                # the notebook language metadata is wrong/missing.
                _JVM_ROOTS = {{"java", "javax", "scala", "com", "org", "net", "io", "akka"}}
                for line in src.splitlines():
                    stripped = line.strip()
                    if cell_lang == "python":
                        # Python: "from X.Y import ..." or "import X"
                        py_from = re.match(r'^from\\s+([\\w\\.]+)\\s+import', stripped)
                        py_imp = re.match(r'^import\\s+([\\w\\.]+)', stripped)
                        if py_from:
                            top = py_from.group(1).split(".")[0]
                            if top not in _SKIP_PY and top not in _JVM_ROOTS:
                                nb_data["python_imports"].append(py_from.group(1))
                        elif py_imp:
                            top = py_imp.group(1).split(".")[0]
                            if top not in _SKIP_PY and top not in _JVM_ROOTS:
                                nb_data["python_imports"].append(py_imp.group(1))
                    elif cell_lang == "scala":
                        # In a Scala cell, any `import X` is a Scala/JVM import.
                        sc_imp = re.match(r'^import\\s+([\\w\\.]+)', stripped)
                        if sc_imp:
                            nb_data["scala_imports"].append(sc_imp.group(1))
                    # JVM access via spark._jvm works from any language (e.g. PySpark)
                    for jvm_m in re.findall(r'spark\\._jvm\\.([\\w\\.]+)', stripped):
                        nb_data["scala_imports"].append(jvm_m)

            nb_data["cells_source"] = all_source
            nb_data["paths"] = list(set(nb_data["paths"]))
            nb_data["tables"] = list(set(nb_data["tables"]))
            nb_data["unresolved"] = list(set(nb_data["unresolved"]))
            nb_data["python_imports"] = list(set(nb_data["python_imports"]))
            nb_data["scala_imports"] = list(set(nb_data["scala_imports"]))
        except:
            pass
        break

    all_data[nb_path] = nb_data

print(json.dumps({{k: {{"paths": v["paths"], "tables": v["tables"],
                        "unresolved": v["unresolved"],
                        "python_imports": v.get("python_imports", []),
                        "scala_imports": v.get("scala_imports", []),
                        "source_len": len(v.get("cells_source", ""))}}
                   for k, v in all_data.items()}}))
""", timeout=120)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        raw = parse_cluster_json(output)
    except Exception:
        print(f"ERROR parsing extraction output: {output[:500]}")
        return {}

    # Now get full source for read/write classification
    # (source was too large to return in JSON, so we classify on cluster)
    classified = {}
    for nb_path, data in raw.items():
        classified[nb_path] = {
            "paths": data["paths"],
            "tables": data["tables"],
            "unresolved": data.get("unresolved", []),
            "python_imports": data.get("python_imports", []),
            "scala_imports": data.get("scala_imports", []),
        }
    return classified


async def classify_refs_on_cluster(session, notebook_paths: list, refs: dict) -> dict:
    """Classify paths and tables as read/write by running regex on cluster.

    Returns same structure with usage field added.
    """
    # Build classification request — send paths/tables per notebook
    # and let the cluster do the regex classification
    classify_input = {}
    for nb_path in notebook_paths:
        nb_refs = refs.get(nb_path, {})
        if nb_refs.get("paths") or nb_refs.get("tables"):
            classify_input[nb_path] = {
                "paths": nb_refs.get("paths", []),
                "tables": nb_refs.get("tables", []),
            }

    if not classify_input:
        return refs

    input_json = json.dumps(classify_input)
    result = await session.execute(f"""
import json, os, re

classify_input = {input_json}
results = {{}}

# Read patterns
READ_P = [r'spark\\.read\\.', r'spark\\.table\\s*\\(', r'pd\\.read_',
          r'\\.load\\s*\\(', r'FROM\\s+', r'JOIN\\s+',
          r'sc\\.textFile', r'sc\\.wholeTextFiles', r'dbutils\\.fs\\.ls']
# Write patterns
WRITE_P = [r'\\.write\\.', r'\\.save\\s*\\(', r'\\.saveAsTable',
           r'INSERT\\s+(?:INTO|OVERWRITE)', r'CREATE\\s+TABLE',
           r'dbutils\\.fs\\.put', r'\\.to_csv\\s*\\(', r'\\.to_parquet']

for nb_path, data in classify_input.items():
    candidates = [nb_path]
    if not nb_path.startswith("/Workspace"):
        candidates.append("/Workspace" + nb_path)
    if not nb_path.endswith(".ipynb"):
        candidates = [c + ".ipynb" for c in candidates] + candidates

    def _strip_comments(code):
        code = re.sub(r'(\\\"\\\"\\\"[\\s\\S]*?\\\"\\\"\\\")', '', code)
        code = re.sub(r"(\\\'\\\'\\\'[\\s\\S]*?\\\'\\\'\\\')", '', code)
        code = re.sub(r'/\\*[\\s\\S]*?\\*/', '', code)
        code = re.sub(r'#[^\\n]*', '', code)
        code = re.sub(r'//[^\\n]*', '', code)
        return code

    source = ""
    for path in candidates:
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    nb = json.load(fh)
                for cell in nb.get("cells", []):
                    source += _strip_comments("".join(cell.get("source", []))) + "\\n"
            except:
                pass
            break

    nb_result = {{"paths": [], "tables": []}}

    for p in data["paths"]:
        lines = [l for l in source.splitlines() if p in l]
        ctx = "\\n".join(lines)
        is_r = any(re.search(rp, ctx, re.IGNORECASE) for rp in READ_P)
        is_w = any(re.search(wp, ctx, re.IGNORECASE) for wp in WRITE_P)
        if is_w and not is_r:
            usage = "write"
        else:
            usage = "read"
        nb_result["paths"].append({{"path": p, "usage": usage}})

    _qt = chr(39) + chr(34)  # quotes: ' and "
    for t in data["tables"]:
        lines = [l for l in source.splitlines() if t in l]
        ctx = "\\n".join(lines)
        # Insert-style writes: table must exist
        insert_pats = [r'INSERT\\s+(?:INTO|OVERWRITE)\\s+' + re.escape(t)]
        # Create-style writes: table will be created
        create_pats = [r'CREATE\\s+TABLE.*' + re.escape(t),
                       r'\\.saveAsTable\\s*\\(\\s*[' + _qt + r']' + re.escape(t)]
        r_pats = [r'FROM\\s+' + re.escape(t),
                   r'JOIN\\s+' + re.escape(t),
                   r'spark\\.table\\s*\\(\\s*[' + _qt + r']' + re.escape(t)]
        is_r = any(re.search(rp, ctx, re.IGNORECASE) for rp in r_pats)
        is_insert = any(re.search(wp, ctx, re.IGNORECASE) for wp in insert_pats)
        is_create = any(re.search(wp, ctx, re.IGNORECASE) for wp in create_pats)
        if is_r:
            usage = "read"
        elif is_insert and not is_create:
            usage = "write_insert"
        elif is_create:
            usage = "write_create"
        else:
            usage = "read"
        nb_result["tables"].append({{"table": t, "usage": usage}})

    results[nb_path] = nb_result

print(json.dumps(results))
""", timeout=120)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        classified = parse_cluster_json(output)
    except Exception as e:
        # Do NOT default to "read". A write misclassified as read slips past the
        # write-guard that redirects customer-data writes to the tmp bucket —
        # too dangerous to swallow. Fail loud so the operator can rerun.
        raise RuntimeError(
            "Read/write classification parse failed; refusing to default to 'read' "
            "(would risk treating a customer-data write as a read). "
            f"Raw cluster output (first 500): {output[:500]}"
        ) from e

    # Merge classification into refs
    for nb_path, data in classified.items():
        if nb_path in refs:
            refs[nb_path]["paths"] = data.get("paths", [])
            refs[nb_path]["tables"] = data.get("tables", [])
            refs[nb_path]["unresolved"] = refs[nb_path].get("unresolved", [])
    return refs


# ─── Availability Checks ────────────────────────────────────────────

async def check_tables_describe(session, tables: list) -> dict:
    """Check table availability using DESCRIBE TABLE. Returns {table: result}."""
    if not tables:
        return {}

    tables_json = json.dumps(tables)
    result = await session.execute(f"""
import json
tables = {tables_json}
results = {{}}
for table in tables:
    # Try 2-part and 3-part names
    candidates = [table]
    parts = table.split(".")
    if len(parts) == 2:
        candidates.append(f"default.{{table}}")

    found = False
    for t in candidates:
        try:
            desc = spark.sql(f"DESCRIBE TABLE {{t}}")
            cols = desc.collect()
            schema_info = [f"{{r['col_name']}} {{r['data_type']}}" for r in cols[:10]]
            results[table] = {{"exists": True, "resolved": t,
                              "columns": len(cols),
                              "schema_preview": schema_info}}
            found = True
            break
        except Exception as e:
            err = str(e)[:150]
            if "not found" in err.lower() or "does not exist" in err.lower():
                continue
            results[table] = {{"exists": False, "error": err}}
            found = True
            break

    if not found:
        results[table] = {{"exists": False}}

print(json.dumps(results))
""", timeout=300)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        return parse_cluster_json(output)
    except Exception:
        print(f"WARNING: Table check failed: {output[:300]}")
        return {}


async def check_oci_paths(session, paths: list) -> dict:
    """Check OCI path availability via fs.listStatus() with timeout.
    Returns {path: result}."""
    if not paths:
        return {}

    paths_json = json.dumps(paths)
    result = await session.execute(f"""
import json
paths = {paths_json}
results = {{}}
for path in paths:
    try:
        jvm = spark._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        exists = fs.exists(p)
        if exists:
            try:
                status = fs.listStatus(p)
                count = 0
                for s in status:
                    count += 1
                    if count > 100:
                        break
                results[path] = {{"exists": True, "items": count}}
            except:
                results[path] = {{"exists": True, "items": "?"}}
        else:
            results[path] = {{"exists": False}}
    except Exception as e:
        err = str(e)[:150]
        if "AccessDenied" in err or "403" in err:
            results[path] = {{"exists": "access_denied", "error": err}}
        else:
            results[path] = {{"exists": False, "error": err}}
print(json.dumps(results))
""", timeout=60)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        return parse_cluster_json(output)
    except Exception:
        print(f"WARNING: OCI path check failed: {output[:300]}")
        return {}


async def check_local_paths(session, paths: list) -> dict:
    """Check Volume/Workspace paths via ls on cluster.
    Returns {path: result}."""
    if not paths:
        return {}

    paths_json = json.dumps(paths)
    result = await session.execute(f"""
import json, subprocess
paths = {paths_json}
results = {{}}
for path in paths:
    try:
        r = subprocess.run(["ls", path], capture_output=True, timeout=10, text=True)
        if r.returncode == 0:
            items = [x for x in r.stdout.strip().splitlines() if x]
            results[path] = {{"exists": True, "items": len(items)}}
        else:
            results[path] = {{"exists": False, "error": r.stderr.strip()[:100]}}
    except subprocess.TimeoutExpired:
        results[path] = {{"exists": "timeout"}}
    except Exception as e:
        results[path] = {{"exists": False, "error": str(e)[:100]}}
print(json.dumps(results))
""", timeout=60)
    output = unwrap_outputs(result.get("outputs", []))
    try:
        return parse_cluster_json(output)
    except Exception:
        print(f"WARNING: Local path check failed: {output[:300]}")
        return {}


async def check_libraries(session, python_imports: list, scala_imports: list) -> dict:
    """Check library availability on cluster. Auto-installs missing Python packages via pip.

    Returns {"python": {module: {"available": bool, "installed": bool, "error": str}},
             "scala":  {class:  {"available": bool, "error": str}}}
    Only missing/failed entries are returned — available packages are omitted from report.
    """
    result = {"python": {}, "scala": {}}
    if not python_imports and not scala_imports:
        return result

    py_json = json.dumps(python_imports)
    sc_json = json.dumps(scala_imports)
    check_result = await session.execute(f"""
import json, importlib, subprocess

py_imports = {py_json}
sc_imports = {sc_json}
results = {{"python": {{}}, "scala": {{}}}}

# Dedupe Python imports to their top-level package — submodules
# (slack_sdk.errors, slack_sdk.webhook) all resolve to the same install.
py_imports = sorted({{m.split(".")[0] for m in py_imports}})

# Step 1: Check Python imports, collect missing
missing_py = []
for mod in py_imports:
    top = mod.split(".")[0]
    try:
        importlib.import_module(top)
    except ImportError:
        missing_py.append(mod)
    except Exception:
        missing_py.append(mod)

# Step 2: Try pip install for missing packages
installed = []
for mod in missing_py:
    top = mod.split(".")[0]
    # Map common import names to pip package names
    pip_name = {{"sklearn": "scikit-learn", "cv2": "opencv-python",
                "pil": "Pillow", "yaml": "pyyaml", "attr": "attrs",
                "dateutil": "python-dateutil", "bs4": "beautifulsoup4",
                "crypto": "pycryptodome"}}.get(top.lower(), top)
    try:
        r = subprocess.run(["pip", "install", "--quiet", pip_name],
                           capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            installed.append(mod)
    except Exception:
        pass

# Step 3: Re-check everything that was missing
for mod in missing_py:
    top = mod.split(".")[0]
    try:
        importlib.import_module(top)
        if mod in installed:
            results["python"][mod] = {{"available": True, "installed": True}}
        else:
            results["python"][mod] = {{"available": True}}
    except ImportError as e:
        results["python"][mod] = {{"available": False, "error": str(e)[:200]}}
    except Exception as e:
        results["python"][mod] = {{"available": False, "error": str(e)[:200]}}

# Scala/JVM classes — no auto-install possible. Class.forName cannot resolve
# wildcard/group imports or Scala type aliases (e.g. DataFrame = Dataset[Row]),
# and core platform namespaces are always present on the cluster — verify only
# genuine third-party classes to avoid false "missing" reports.
_CORE_JVM = ("java.", "javax.", "scala.", "org.apache.spark.", "spark.")
for cls in sc_imports:
    # Wildcard (pkg._), group (pkg.{{A,B}} -> captured as "pkg.") and trailing-dot
    # imports name a package, not a class — skip.
    if cls.endswith(".") or cls.endswith("._") or "{{" in cls or "}}" in cls:
        continue
    # Core platform classes are guaranteed present.
    if cls.startswith(_CORE_JVM):
        continue
    loaded = False
    for cand in (cls, cls + "$"):  # try class, then Scala object form
        try:
            spark._jvm.java.lang.Class.forName(cand)
            loaded = True
            break
        except Exception:
            pass
    if not loaded:
        results["scala"][cls] = {{"available": False, "error": f"Class not found: {{cls}}"}}

print(json.dumps(results))
""", timeout=300)
    output = unwrap_outputs(check_result.get("outputs", []))
    try:
        return parse_cluster_json(output)
    except Exception:
        print(f"WARNING: Library check failed: {output[:300]}")
        return result


# ─── Report Generation ──────────────────────────────────────────────

def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def generate_report(refs: dict, table_results: dict, oci_results: dict,
                    local_results: dict, lib_results: dict, job_name: str) -> dict:
    """Generate per-notebook data readiness report.

    Returns report dict and prints human-readable summary.
    """
    report = {
        "job_name": job_name,
        "generated_at": datetime.now().isoformat(),
        "summary": {"tables_found": 0, "tables_missing": 0,
                     "paths_found": 0, "paths_missing": 0,
                     "paths_unmapped": 0, "unresolved": 0,
                     "libs_installed": 0, "libs_missing": 0},
        "notebooks": {},
    }

    py_lib_results = lib_results.get("python", {})
    sc_lib_results = lib_results.get("scala", {})

    for nb_path, data in refs.items():
        nb_name = os.path.basename(nb_path)
        nb_report = {"read_tables": [], "read_paths": [], "unresolved": data.get("unresolved", []),
                     "write_tables": [], "write_paths": [], "libraries": []}

        # Tables
        for t_entry in data.get("tables", []):
            table = t_entry["table"] if isinstance(t_entry, dict) else t_entry
            usage = t_entry.get("usage", "unknown") if isinstance(t_entry, dict) else "unknown"
            if usage == "write_create":
                nb_report["write_tables"].append(table)
                continue

            info = table_results.get(table, {})
            entry = {"table": table, "exists": info.get("exists", False)}
            # resolved_name only when it actually differs (e.g. resolved via default.)
            if info.get("resolved") and info["resolved"] != table:
                entry["resolved_name"] = info["resolved"]
            if info.get("columns"):
                entry["columns"] = info["columns"]
            if info.get("error"):
                entry["error"] = info["error"]
            nb_report["read_tables"].append(entry)

            if info.get("exists") is True:
                report["summary"]["tables_found"] += 1
            else:
                report["summary"]["tables_missing"] += 1

        # Paths
        for p_entry in data.get("paths", []):
            path = p_entry["path"] if isinstance(p_entry, dict) else p_entry
            usage = p_entry.get("usage", "unknown") if isinstance(p_entry, dict) else "unknown"
            if usage == "write":
                nb_report["write_paths"].append(path)
                continue

            entry = {"source": path}

            if path.startswith("oci://"):
                info = oci_results.get(path, {})
                entry["type"] = "oci"
                entry["oci_path"] = path
                entry["exists"] = info.get("exists", False)
                if info.get("items"):
                    entry["items"] = info["items"]
                if info.get("error"):
                    entry["error"] = info["error"]
            elif path.startswith("/Volumes/") or path.startswith("/Workspace/"):
                info = local_results.get(path, {})
                entry["type"] = "volume" if path.startswith("/Volumes/") else "workspace"
                entry["exists"] = info.get("exists", False)
                if info.get("items"):
                    entry["items"] = info["items"]
                if info.get("error"):
                    entry["error"] = info["error"]
            else:
                # S3/DBFS/mnt — map to OCI
                mapped = map_to_oci_path(path)
                entry["type"] = "s3" if "s3" in path else "dbfs" if "dbfs" in path else "mnt"
                entry["oci_path"] = mapped["oci_path"]
                if mapped["oci_path"]:
                    info = oci_results.get(mapped["oci_path"], {})
                    entry["exists"] = info.get("exists", False)
                    if info.get("items"):
                        entry["items"] = info["items"]
                    if info.get("error"):
                        entry["error"] = info["error"]
                else:
                    entry["exists"] = "unmapped"
                    report["summary"]["paths_unmapped"] += 1

            if entry.get("exists") is True:
                report["summary"]["paths_found"] += 1
            elif entry.get("exists") != "unmapped":
                report["summary"]["paths_missing"] += 1

            nb_report["read_paths"].append(entry)

        # Libraries — only report items that needed attention
        for mod in data.get("python_imports", []):
            info = py_lib_results.get(mod)
            if info is None:
                # Was already available — skip from report
                continue
            entry = {"module": mod, "type": "python", "available": info.get("available", False)}
            if info.get("installed"):
                entry["installed"] = True
                report["summary"]["libs_installed"] += 1
            if info.get("error"):
                entry["error"] = info["error"]
            nb_report["libraries"].append(entry)
            if not info.get("available"):
                report["summary"]["libs_missing"] += 1
        for cls in data.get("scala_imports", []):
            info = sc_lib_results.get(cls)
            if info is None:
                # Was already on classpath — skip from report
                continue
            entry = {"module": cls, "type": "scala", "available": info.get("available", False)}
            if info.get("error"):
                entry["error"] = info["error"]
            nb_report["libraries"].append(entry)
            if not info.get("available"):
                report["summary"]["libs_missing"] += 1

        report["summary"]["unresolved"] += len(nb_report["unresolved"])
        # Only include notebooks that actually have something to report — skip
        # the ones whose buckets are all empty (most notebooks) to cut noise.
        if any(nb_report[k] for k in ("read_tables", "read_paths", "write_tables",
                                      "write_paths", "unresolved", "libraries")):
            report["notebooks"][nb_path] = nb_report

    return report


def print_report(report: dict):
    """Print human-readable report to console."""
    summary = report["summary"]

    for nb_path, nb_data in report["notebooks"].items():
        nb_name = os.path.basename(nb_path)
        tables = nb_data.get("read_tables", [])
        paths = nb_data.get("read_paths", [])
        unresolved = nb_data.get("unresolved", [])
        libraries = nb_data.get("libraries", [])

        if not tables and not paths and not unresolved and not libraries:
            continue

        print(f"\n{'─'*60}")
        print(f"  {nb_name}")
        print(f"{'─'*60}")

        if tables:
            print(f"\n  TABLES ({len(tables)}):")
            for t in tables:
                table = t["table"]
                resolved = t.get("resolved_name", "")
                resolved_str = f" → {resolved}" if resolved and resolved != table else ""
                if t.get("exists") is True:
                    cols = t.get("columns", "?")
                    print(f"    EXISTS ({cols} cols)  {table}{resolved_str}")
                elif t.get("error"):
                    print(f"    ERROR            {table}")
                    print(f"                     {t['error']}")
                else:
                    print(f"    MISSING          {table}")

        if paths:
            print(f"\n  STORAGE PATHS ({len(paths)} reads):")
            for p in paths:
                source = p["source"]
                oci = p.get("oci_path", "")
                exists = p.get("exists", False)

                if p.get("type") in ("s3", "dbfs", "mnt"):
                    print(f"    Source: {source}")
                    if oci:
                        status = "EXISTS" if exists is True else "ACCESS DENIED" if exists == "access_denied" else "MISSING"
                        items = f" ({p['items']} items)" if p.get("items") else ""
                        print(f"    OCI:    {oci}")
                        print(f"    Status: {status}{items}")
                    else:
                        print(f"    OCI:    UNMAPPED (bucket not in mapping)")
                    print()
                elif p.get("type") == "oci":
                    status = "EXISTS" if exists is True else "ACCESS DENIED" if exists == "access_denied" else "MISSING"
                    items = f" ({p['items']} items)" if p.get("items") else ""
                    print(f"    {status}{items}  {source}")
                else:
                    status = "EXISTS" if exists is True else "TIMEOUT" if exists == "timeout" else "MISSING"
                    items = f" ({p['items']} items)" if p.get("items") else ""
                    print(f"    {status}{items}  {source}")

        # Write targets (create-style, not checked — table will be created)
        write_tables = nb_data.get("write_tables", [])
        write_paths = nb_data.get("write_paths", [])
        if write_tables or write_paths:
            print(f"\n  WRITE TARGETS (create-style, not checked):")
            for t in write_tables:
                print(f"    TABLE  {t}")
            for p in write_paths:
                print(f"    PATH   {p}")

        if unresolved:
            print(f"\n  UNRESOLVED ({len(unresolved)}) — dynamic paths, needs execution:")
            for u in unresolved:
                print(f"    {u}")

        # Libraries — only show items that needed attention
        missing_libs = [l for l in libraries if not l.get("available")]
        installed_libs = [l for l in libraries if l.get("available") and l.get("installed")]
        if missing_libs:
            print(f"\n  LIBRARIES — MISSING ({len(missing_libs)}):")
            for l in missing_libs:
                ltype = l["type"].upper()
                print(f"    MISSING  [{ltype}]  {l['module']}")
                if l.get("error"):
                    print(f"             {l['error']}")
        if installed_libs:
            print(f"\n  LIBRARIES — auto-installed ({len(installed_libs)}):")
            for l in installed_libs:
                print(f"    INSTALLED  [PYTHON]  {l['module']}")

    # Overall summary
    print(f"\n{'='*60}")
    print(f"  DATA AVAILABILITY SUMMARY: {report['job_name']}")
    print(f"{'='*60}")
    print(f"  Tables:     {summary['tables_found']} found, {summary['tables_missing']} missing")
    print(f"  Paths:      {summary['paths_found']} found, {summary['paths_missing']} missing, {summary['paths_unmapped']} unmapped")
    print(f"  Libraries:  {summary['libs_installed']} auto-installed, {summary['libs_missing']} missing")
    print(f"  Unresolved: {summary['unresolved']} (dynamic/parameterized)")
    total_missing = summary["tables_missing"] + summary["paths_missing"]
    libs_missing = summary["libs_missing"]
    if total_missing == 0 and summary["paths_unmapped"] == 0 and libs_missing == 0:
        print(f"\n  RESULT: ALL DEPENDENCIES AVAILABLE — ready to migrate")
    elif libs_missing > 0:
        print(f"\n  RESULT: {libs_missing} library(s) MISSING — install before migrating")
        if total_missing > 0:
            print(f"          {total_missing} data source(s) also MISSING")
    elif total_missing == 0:
        print(f"\n  RESULT: DATA AVAILABLE (but {summary['paths_unmapped']} paths unmapped — check manually)")
    else:
        print(f"\n  RESULT: {total_missing} data source(s) MISSING — review before migrating")
    print(f"{'='*60}")


# ─── Main ────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Check data availability for notebooks in an AIDP workflow")

    parser.add_argument("--job-key",
                        help="AIDP job key (UUID) from the workflow")
    parser.add_argument("--manifest",
                        help="Pre-built manifest JSON (skips API fetch)")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER,
                        help="AIDP cluster ID (default: %(default)s)")
    parser.add_argument("--only-tasks", default="",
                        help="Comma-separated task_key substrings to check")
    parser.add_argument("--output",
                        help="Save JSON report to file (optional)")
    parser.add_argument("--bucket-mapping",
                        default=os.path.join(PROJECT_DIR, "config", "oci_bucket_tenancy_mapping.json"),
                        help="Path to bucket→namespace JSON (default: %(default)s)")

    # AIDP environment
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID)
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID)
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE)

    args = parser.parse_args()

    if not args.manifest and not args.job_key:
        parser.error("Either --job-key or --manifest is required")

    # Load bucket mapping
    load_bucket_ns_mapping(args.bucket_mapping)

    # ── Extract notebook paths ──
    if args.manifest:
        tprint(f"Loading manifest from {args.manifest}")
        with open(args.manifest) as f:
            manifest = json.load(f)
        job = manifest["jobs"][0]
        tasks = job["tasks"]
        job_name = job.get("job_name", "unknown")
    else:
        tprint(f"Fetching workflow: {args.job_key}")
        job_data = fetch_job_definition(
            args.workspace_id, args.job_key,
            lake_ocid=args.lake_ocid, oci_profile=args.oci_profile)
        job_name = job_data.get("name", "").replace(".job", "") or args.job_key
        tasks = extract_tasks_from_job(job_data)

    if not tasks:
        print("ERROR: No notebook tasks found")
        sys.exit(1)

    # Filter tasks
    only_tasks = [t.strip() for t in args.only_tasks.split(",") if t.strip()] if args.only_tasks else []
    if only_tasks:
        tasks = [t for t in tasks if any(ot in t["task_key"] for ot in only_tasks)]
        tprint(f"Filtered to {len(tasks)} task(s)")

    # Collect notebook paths (tasks + deps, recursively flattening nested_deps)
    def _flatten_deps(dep_list, out):
        for dep in dep_list:
            dp = dep if isinstance(dep, str) else dep.get("path", "")
            if dp and dp not in out:
                out.append(dp)
            if isinstance(dep, dict) and dep.get("nested_deps"):
                _flatten_deps(dep["nested_deps"], out)

    notebook_paths = []
    for t in tasks:
        nb = t.get("notebook_path", "")
        if nb and nb not in notebook_paths:
            notebook_paths.append(nb)
        _flatten_deps(t.get("run_deps", []), notebook_paths)

    tprint(f"Workflow: {job_name} ({len(tasks)} tasks, {len(notebook_paths)} notebooks)")

    # ── Connect to cluster ──
    tprint(f"Connecting to cluster {args.cluster[:12]}...")
    session = AIDPSession(lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
                          cluster_id=args.cluster, oci_profile=args.oci_profile,
                          session_name=f"data_check_{job_name}")
    await session.connect()

    # ── Step 1: Extract data references ──
    tprint("Extracting data references from notebooks...")
    refs = await extract_data_refs(session, notebook_paths)
    total_paths = sum(len(r.get("paths", [])) for r in refs.values())
    total_tables = sum(len(r.get("tables", [])) for r in refs.values())
    tprint(f"Found {total_paths} storage paths, {total_tables} table references")

    # ── Step 2: Classify read vs write ──
    tprint("Classifying read vs write operations...")
    refs = await classify_refs_on_cluster(session, notebook_paths, refs)

    # Count read-only refs
    read_tables = set()
    read_oci_paths = []
    read_local_paths = []
    read_s3_mapped_oci = []

    for nb_path, data in refs.items():
        for t_entry in data.get("tables", []):
            if isinstance(t_entry, dict) and t_entry.get("usage") not in ("write_create",):
                # Check read tables AND write_insert tables (INSERT INTO needs table to exist)
                read_tables.add(t_entry["table"])
            elif isinstance(t_entry, str):
                read_tables.add(t_entry)

        for p_entry in data.get("paths", []):
            path = p_entry["path"] if isinstance(p_entry, dict) else p_entry
            usage = p_entry.get("usage", "unknown") if isinstance(p_entry, dict) else "unknown"
            if usage == "write":
                continue

            if path.startswith("oci://"):
                read_oci_paths.append(path)
            elif path.startswith("/Volumes/") or path.startswith("/Workspace/"):
                read_local_paths.append(path)
            else:
                mapped = map_to_oci_path(path)
                if mapped["oci_path"]:
                    read_s3_mapped_oci.append(mapped["oci_path"])

    # Deduplicate
    read_tables = sorted(read_tables)
    read_oci_paths = sorted(set(read_oci_paths + read_s3_mapped_oci))
    read_local_paths = sorted(set(read_local_paths))

    # Gather library imports across all notebooks (deduplicated)
    all_python_imports = set()
    all_scala_imports = set()
    for nb_path, data in refs.items():
        all_python_imports.update(data.get("python_imports", []))
        all_scala_imports.update(data.get("scala_imports", []))
    all_python_imports = sorted(all_python_imports)
    all_scala_imports = sorted(all_scala_imports)

    tprint(f"Read dependencies: {len(read_tables)} tables, "
           f"{len(read_oci_paths)} OCI paths, {len(read_local_paths)} local paths")
    tprint(f"Library imports: {len(all_python_imports)} Python, {len(all_scala_imports)} Scala/JVM")

    # ── Step 3: Check availability ──
    tprint("Checking table availability (DESCRIBE TABLE)...")
    table_results = await check_tables_describe(session, read_tables)

    tprint("Checking OCI path availability (fs.listStatus)...")
    oci_results = await check_oci_paths(session, read_oci_paths)

    tprint("Checking Volume/Workspace paths (ls)...")
    local_results = await check_local_paths(session, read_local_paths)

    tprint("Checking library availability (import/Class.forName)...")
    lib_results = await check_libraries(session, all_python_imports, all_scala_imports)

    await session.close()

    # ── Step 4: Generate report ──
    report = generate_report(refs, table_results, oci_results, local_results, lib_results, job_name)
    print_report(report)

    # Save JSON report if requested
    if args.output:
        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        tprint(f"Report saved to {args.output}")


if __name__ == "__main__":
    asyncio.run(main())
