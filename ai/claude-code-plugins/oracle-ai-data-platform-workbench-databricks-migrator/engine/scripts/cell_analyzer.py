"""
Cell-by-Cell Analysis Engine
==============================
Produces actionable per-cell migration plans by analyzing each cell
with Opus (adaptive thinking + tools). The plans are then followed
during the actual migration pass, making migration deterministic.

Each cell gets:
- Storage path verification (OCI paths checked on cluster)
- Table reference verification (catalog lookup)
- FUSE consistency risk detection (pickle write+read, parquet overwrite)
- Databricks API translation requirements
- Dependency tracking (what variables this cell needs from prior cells)
"""

import json
import re
import os
import sys
from collections import defaultdict
from typing import List, Dict, Optional, Any, Tuple

# Import FUSE risky package database from fuse_scanner (same directory)
try:
    _scripts_dir = os.path.dirname(os.path.abspath(__file__))
    if _scripts_dir not in sys.path:
        sys.path.insert(0, _scripts_dir)
    from fuse_scanner import FUSE_RISKY_PACKAGES, extract_imports, _build_import_index
    _FUSE_IMPORT_INDEX = _build_import_index()
except Exception:
    FUSE_RISKY_PACKAGES = {}
    _FUSE_IMPORT_INDEX = {}

    def extract_imports(src: str) -> list:
        return []

# FUSE risk patterns to detect
FUSE_PATTERNS = [
    {
        "name": "pickle_write_read",
        "pattern": r"pickle\.dump\s*\(",
        "followup": r"pickle\.load\s*\(",
        "fix": "Note: pickle write+read on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        "name": "parquet_overwrite_same_path",
        "pattern": r"\.write\.(?:mode\(['\"]overwrite['\"]\)|parquet\()",
        "context_check": "read_path_equals_write_path",
        "fix": "Note: parquet overwrite-same-path on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        "name": "save_as_table_overwrite",
        "pattern": r"\.write\..*saveAsTable\(",
        "fix": "Note: saveAsTable overwrite on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        "name": "pandas_to_csv",
        "pattern": r"\.to_csv\s*\(",
        "fix": "Note: pandas to_csv on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        "name": "file_write_then_read",
        "pattern": r"open\s*\([^)]+['\"]w[b]?['\"]\)",
        "followup": r"open\s*\([^)]+['\"]r[b]?['\"]\)",
        "fix": "Note: file write-then-read on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        # TensorFlow SavedModel save — the model.save() call itself is fine but any
        # subsequent load_model() in the same or a later cell will hit FUSE shard lag.
        "name": "tensorflow_savedmodel_save",
        "pattern": r"(?:model\.save|tf\.saved_model\.save)\s*\(\s*['\"]?\/Volumes",
        "followup": r"(?:tf\.keras\.models\.load_model|tf\.saved_model\.load|keras\.models\.load_model)\s*\(",
        "fix": "Note: TF SavedModel save+load on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        # Any load_model() / tf.saved_model.load() reading from /Volumes — even when
        # the save happened in a prior cell or a prior job run the FUSE cache may be
        # cold, causing intermittent RestoreV2 I/O errors.
        "name": "tensorflow_savedmodel_load_volumes",
        "pattern": r"(?:tf\.keras\.models\.load_model|tf\.saved_model\.load|keras\.models\.load_model)\s*\(\s*['\"]?\/Volumes",
        "fix": "Note: TF SavedModel load from /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        # Two open(..., "r") or open(..., "rb") calls in the same cell.
        # FUSE can evict the inode cache between the first and second read,
        # causing FileNotFoundError on the second open even though the file exists.
        # Confirmed: example_input.txt in ExampleJob job.
        "name": "volumes_double_read",
        "pattern": r"open\s*\([^)]+['\"]r[b]?['\"]\)",
        "followup": r"open\s*\([^)]+['\"]r[b]?['\"]\)",
        "fix": "Note: double open() read on /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
]

# Databricks patterns to translate
DATABRICKS_PATTERNS = [
    {"pattern": r"%sql\b", "action": "Preserve as-is — AIDP supports %sql natively; do NOT convert to spark.sql()"},
    {"pattern": r"%run\b", "action": "Will be inlined by migration engine"},
    {"pattern": r"%pip\b", "action": "Convert to subprocess.check_call"},
    {"pattern": r"%sh\b", "action": "Convert to subprocess.run"},
    {"pattern": r"aidp_dbutils|_DBUtils", "action": "Replace with from aidp_compat import dbutils"},
    {"pattern": r"spark\.databricks\.", "action": "Comment out (Databricks-specific config)"},
    # dbfs:/ URI format (Databricks colon notation)
    {"pattern": r"dbfs:/", "action": "Translate dbfs:/path → /Volumes/default/default/dbfs/path using translate_path() from aidp_compat"},
    # /dbfs/ filesystem format (no colon) — same DBFS root, different notation
    # e.g. base_path = "/dbfs/FileStore/example_user" → "/Volumes/default/default/dbfs/FileStore/example_user"
    {"pattern": r"""['"]/dbfs/|= ?['"]/dbfs""", "action": "Translate /dbfs/path → /Volumes/default/default/dbfs/path using translate_path() from aidp_compat"},
    {"pattern": r"s3[a]?://", "action": "Translate to oci:// using bucket mapping"},
    {"pattern": r"requests\.post.*slack|send_slack|slack_webhook", "action": "Skip (Slack notification)"},
    {"pattern": r"display\s*\(", "action": "Use display() from aidp_compat"},
    # dbutils widget/job/notebook replacements — flagged here so Opus sees them in changes_needed
    {"pattern": r"dbutils\.widgets\.get\s*\(",
     "action": "Replace dbutils.widgets.get('x') → oidlUtils.parameters.getParameter('x', '') — comment original line, add oidlUtils line below tagged # Oracle tool modification:"},
    {"pattern": r"dbutils\.jobs\.taskValues\.(get|set)\s*\(",
     "action": "Replace dbutils.jobs.taskValues.get/set → oidlUtils.parameters.getParameter/setTaskValue — tag # Oracle tool modification:"},
    # dbutils.fs.* — file system operations
    {"pattern": r"dbutils\.fs\.(ls|cp|mv|rm|mkdirs|put|head|mount|unmount|mounts|refreshMounts)\s*\(",
     "action": "dbutils.fs is available via aidp_compat — ensure 'from aidp_compat import dbutils' is present. dbutils.fs.ls/cp/mv/rm/mkdirs/put/head work as-is on AIDP. dbutils.fs.mount/unmount/mounts: comment out — AIDP does not use mount points, use oci:// paths directly"},
    # dbutils.widgets registration (text/dropdown/combobox/multiselect) — not supported on AIDP
    {"pattern": r"dbutils\.widgets\.(text|dropdown|combobox|multiselect)\s*\(",
     "action": "Comment out dbutils.widgets.text/dropdown/combobox/multiselect — widget registration not supported on AIDP. Parameters come from oidlUtils.parameters. Tag: # Oracle tool modification: widget registration not supported on AIDP"},
    # sc.addJar() — cannot add JARs at runtime on AIDP
    {"pattern": r"sc\.addJar\s*\(|spark\._jsc\.addJar\s*\(",
     "action": "Comment out sc.addJar() / spark._jsc.addJar() — AIDP does not support runtime JAR loading. JARs must be installed via cluster libraries API before cluster start. Tag: # Oracle tool modification: sc.addJar() not supported — JAR must be pre-installed via cluster libraries API"},
    # get_glue_table_s3_location — keep call sites UNCHANGED. The function is
    # a path-returning helper; on AIDP its DEFINITION body uses DESCRIBE FORMATTED
    # (see the AIDP LOCATION EXTRACTION rule in CELL_MIGRATE_PROMPT). Do NOT
    # rewrite call sites or replace the variable value with a catalog identifier.
    {"pattern": r"get_glue_table_s3_location\s*\(",
     "action": "KEEP call sites unchanged. <var> = get_glue_table_s3_location('db','tbl') stays as-is. The function DEFINITION (if found in this cell or a %run dep) is body-swapped to use DESCRIBE FORMATTED on the AIDP metastore via the prompt's AIDP LOCATION EXTRACTION rule. Downstream `spark.read.parquet(<var>)` etc. continue to work because <var> still holds a path string."},
    # Hudi reads — supported on AIDP (the Hudi 0.15.0 Spark-3.5 bundle JAR is
    # pre-installed). Leave path reads as-is; do NOT convert to spark.read.table.
    {"pattern": r"spark\.read\.format\s*\(\s*[\"'](?:org\.apache\.)?hudi[\"']\s*\)",
     "action": "KEEP spark.read.format('hudi').load(<path>) AS-IS. Hudi is supported on AIDP via the pre-installed hudi-spark3.5-bundle_2.12-0.15.0 JAR. Do NOT convert to spark.read.table; do NOT comment out. The OCI path translation already handles the underlying storage URI."},
]

# Additional risk patterns — flagged in cell_plan["risks"] for Opus to handle
RISK_PATTERNS = [
    {
        "name": "delete_dml",
        "pattern": r"DELETE\s+FROM",
        "severity": "HIGH",
        "fix": "Non-Delta tables don't support DELETE — rewrite as: df=spark.table(tbl).filter(~cond); df.write.mode('overwrite').saveAsTable(tbl)",
    },
    {
        "name": "pip_install",
        "pattern": r"^\s*%pip\s+install",
        "severity": "MEDIUM",
        "fix": "Comment out and verify library availability: run_on_cluster('import <lib>; print(\"available\")'). Replace with print('AIDP: library installed at cluster level')",
    },
    {
        "name": "boto3_aws",
        "pattern": r"import\s+boto3|boto3\.client|boto3\.resource",
        "severity": "HIGH",
        "fix": "No AWS SDK on AIDP — replace boto3.client('s3') with aidp_compat.read_s3_object/write_s3_object, boto3.client('glue') with aidp_compat.get_glue_table_s3_location",
    },
    {
        "name": "glue_lookup",
        "pattern": r"get_glue_table_s3_location|glue.*get_table|GetTable.*glue",
        "severity": "MEDIUM",
        "fix": "Use: from aidp_compat import get_glue_table_s3_location (drop-in shim using DESCRIBE FORMATTED)",
    },
    {
        "name": "spark_catalog",
        "pattern": r"spark_catalog\.",
        "severity": "HIGH",
        "fix": "Replace spark_catalog.schema.table with schema.table (spark_catalog catalog does not exist on AIDP)",
    },
    {
        "name": "databricks_api",
        "pattern": r"JobRunAPI|api/2\.0/jobs|azuredatabricks\.net",
        "severity": "MEDIUM",
        "fix": "Databricks-specific API — replace with: print('AIDP: Skipped Databricks job trigger — not applicable on OCI')",
    },
    {
        "name": "internal_endpoint",
        "pattern": r"internal-host\.example|internal-gateway.example",
        "severity": "HIGH",
        "fix": "AWS-internal endpoint, not reachable from OCI. If side-effect only: print('AIDP: Skipped'). If fetches data: use explore_path/describe_table to find equivalent data in OCI",
    },
    {
        # /dbfs/ or dbfs:/ paths loaded from DB/config at runtime — cannot be
        # translated statically. Detect spark.sql/pd.read_sql/spark.table calls
        # that retrieve path-like column names (path, location, uri, dir, etc.).
        "name": "dbfs_path_from_config",
        "pattern": r"(?:spark\.sql|pd\.read_sql|spark\.table)\s*\(.*(?:path|location|uri|dir|folder|base)",
        "severity": "MEDIUM",
        "fix": (
            "DBFS path may be loaded from a DB/config table at runtime — static "
            "translation is not possible. Use run_on_cluster to inspect the actual "
            "value, then wrap with translate_path() for runtime translation: "
            "from aidp_compat import translate_path; path = translate_path(db_path). "
            "Call make_note if path source is unclear."
        ),
    },
    {
        # Any open() read from a /Volumes path — even a single read can be the
        # second access of a cross-cell pair, causing FUSE cache eviction FileNotFoundError.
        # Catches hard-coded /Volumes paths; the cross-cell detector catches variable paths.
        "name": "volumes_file_read",
        "pattern": r"open\s*\(\s*['\"]\/Volumes[^'\"]*['\"],?\s*['\"]r[b]?['\"]",
        "severity": "MEDIUM",
        "fix": "Note: open() read from /Volumes via FUSE. No workaround needed — FUSE issues resolved.",
    },
    {
        # Catches TF SavedModel I/O where the path is a variable (not a /Volumes literal).
        # e.g. model.save(model_path) where model_path was set to a /Volumes path earlier.
        # The FUSE_PATTERNS above cover hard-coded /Volumes strings; this catches the
        # broader pattern so Opus is prompted to check any TF model I/O.
        "name": "tensorflow_model_io",
        "pattern": r"(?:model\.save|tf\.saved_model\.save|tf\.keras\.models\.load_model|tf\.saved_model\.load|keras\.models\.load_model)\s*\(",
        "severity": "HIGH",
        "fix": "Note: TF SavedModel I/O detected. No FUSE workaround needed — FUSE issues resolved.",
    },
    {
        # Scala cells using org.json classes — Databricks pre-imports these, AIDP kernel does NOT.
        # Affects JSONObject, JSONArray, JSONException used in %scala cells.
        "name": "scala_missing_json_import",
        "pattern": r"(?:new\s+JSONObject|new\s+JSONArray|JSONObject\s*\(|JSONArray\s*\(|JSONException)",
        "severity": "LOW",
        "fix": "Add missing import at top of Scala cell: import org.json.JSONObject (and/or import org.json.JSONArray, import org.json.JSONException) — Databricks pre-imports these, AIDP Scala kernel does NOT",
    },
    {
        # %scala magic cells share the same SparkSession on Databricks but AIDP is
        # Python-only. Opus should port the Scala logic to Python (UDFs, DataFrame ops)
        # rather than skipping — only skip if JVM-specific library has no Python equiv.
        "name": "scala_magic",
        "pattern": r"^%scala",
        "severity": "HIGH",
        "fix": (
            "%scala cell — AIDP kernel is Python-only. Port to Python: "
            "(1) Scala UDF registrations → spark.udf.register('name', python_fn, ReturnType()); "
            "(2) DataFrame ops → PySpark equivalents. "
            "Use run_on_cluster to verify. Only comment out if JVM-only library (e.g. legacy_decrypt) — "
            "add: # AIDP: <feature> disabled — Scala-only library, no Python equivalent"
        ),
    },
    {
        # Cross-language UDF registration — e.g. Scala UDF registered and called from Python.
        # AIDP Python kernel cannot access Scala-registered UDFs.
        "name": "cross_lang_udf",
        "pattern": r"\.udf\.register\s*\(.*(?:StringType|IntegerType|ArrayType|DoubleType|LongType|BooleanType)",
        "severity": "HIGH",
        "fix": (
            "Cross-language UDF registration — verify it uses Python lambda or function "
            "(safe on AIDP). If originally a Scala UDF, re-register as Python UDF: "
            "spark.udf.register('name', python_fn, StringType()). "
            "Use run_on_cluster to test the Python UDF returns expected results."
        ),
    },
    {
        "name": "legacy_udf",
        "pattern": r"legacy_(decrypt|encrypt)|Legacy(Decrypt|Encrypt)",
        "severity": "HIGH",
        "fix": (
            "legacy_decrypt/legacy_encrypt UDF requires AWS Secrets Manager keys — not available on OCI. "
            "Comment out the call and add: # Oracle tool modification: legacy_decrypt disabled — "
            "AWS Secrets Manager not available on OCI"
        ),
    },
    {
        "name": "aws_credentials",
        "pattern": r"AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|AWS_DEFAULT_REGION|aws_access_key_id|aws_secret_access_key",
        "severity": "HIGH",
        "fix": (
            "AWS credentials detected — not available on OCI AIDP. "
            "AIDP uses OCI API key auth via config file at /Workspace/<oci-config-workspace-path> "
            "(DEFAULT profile). NEVER use oci.auth.signers.get_resource_principals_signer() — "
            "resource principal has known failure modes on AIDP. "
            "Comment out AWS credential setup. For S3 access use aidp_compat.s3_compat or translate_path() + spark.read. "
            "Tag: # Oracle tool modification: AWS credentials not available on OCI — using API key auth"
        ),
    },
    {
        "name": "databricks_cluster_id",
        "pattern": r"""['"]\d{4}-\d{6}-[a-z0-9]{8}['"]|clusterName\s*=\s*['"][^'"]+['"]|cluster_id\s*=\s*['"][0-9a-f\-]{36}['"]""",
        "severity": "MEDIUM",
        "fix": (
            "Hardcoded Databricks cluster ID detected. "
            "Comment out and replace with AIDP cluster ID or remove if not needed. "
            "Tag: # Oracle tool modification: Databricks cluster ID removed — not applicable on AIDP"
        ),
    },
]


# ─── Stale Lazy Eval Detection ────────────────────────────────────────────────
#
# Spark lazy evaluation problem:
#   Cell A: df = spark.read.parquet("oci://bucket/path/")   <- captures file list lazily
#   Cell B: other.write.mode("overwrite").parquet("oci://bucket/path/")  <- overwrites source
#   Cell C: df.write.mode("overwrite")...  <- BOOM: df still has stale file list from Cell A
#
# Spark's execution plan is lazily evaluated — it records the file list at read() time
# but only reads actual bytes at write() time. Any overwrite between those two points
# produces a stale plan, causing AnalysisException or silent wrong-data writes.
#
# Fix: inject safe_materialize(df) before the write in Cell C.

# Detect: var = spark.read.FORMAT("path") or spark.read.option(...).FORMAT("path")
_SPARK_READ_RE = re.compile(
    r'(\w+)\s*=\s*(?:[\w.]+\.)?spark\s*\.read'
    r'(?:\s*\.\s*\w+\s*\([^)]*\))*'        # chained options/format calls
    r'\s*\.\s*(?:parquet|csv|orc|json|text|load)\s*\(\s*["\']([^"\']+)["\']',
    re.DOTALL,
)

# Detect: var = spark.table("schema.table")
_SPARK_TABLE_RE = re.compile(
    r'(\w+)\s*=\s*(?:[\w.]+\.)?spark\s*\.table\s*\(\s*["\']([^"\']+)["\']'
)

# Detect overwrite paths: .write[.opt...].mode("overwrite")[.opt...].FORMAT("path")
# Handles both orderings: .write.mode("overwrite").parquet("p") and
#                          .write.format("parquet").mode("overwrite").save("p")
_WRITE_OVERWRITE_PATH_RE = re.compile(
    r'\.write'
    r'(?:\s*\.\s*\w+\s*\([^)]*\))*'        # chained before mode
    r'\s*\.\s*mode\s*\(\s*["\']overwrite["\']\s*\)'
    r'(?:\s*\.\s*\w+\s*\([^)]*\))*'        # chained after mode
    r'\s*\.\s*(?:parquet|csv|orc|json|save)\s*\(\s*["\']([^"\']+)["\']',
    re.DOTALL,
)

# Detect overwrite tables: .write[.opt...].mode("overwrite")[.opt...].saveAsTable("t")
_WRITE_OVERWRITE_TABLE_RE = re.compile(
    r'\.write'
    r'(?:\s*\.\s*\w+\s*\([^)]*\))*'
    r'\s*\.\s*mode\s*\(\s*["\']overwrite["\']\s*\)'
    r'(?:\s*\.\s*\w+\s*\([^)]*\))*'
    r'\s*\.\s*saveAsTable\s*\(\s*["\']([^"\']+)["\']',
    re.DOTALL,
)


def _normalize_path(path: str) -> str:
    """Normalize a storage path for comparison (strip trailing slash)."""
    return path.rstrip("/").strip()


def _extract_df_reads(cell_source: str) -> List[Tuple[str, str]]:
    """Extract (df_variable_name, source_path) from spark.read.* calls in a cell.

    Returns list of (var_name, normalized_path) pairs.
    """
    results = []
    for m in _SPARK_READ_RE.finditer(cell_source):
        var, path = m.group(1), _normalize_path(m.group(2))
        if var and path:
            results.append((var, path))
    for m in _SPARK_TABLE_RE.finditer(cell_source):
        var, table = m.group(1), m.group(2).strip()
        if var and table:
            results.append((var, table))
    return results


def _extract_overwrite_targets(cell_source: str) -> List[str]:
    """Extract paths/tables that are written with mode='overwrite' in a cell."""
    targets = []
    for m in _WRITE_OVERWRITE_PATH_RE.finditer(cell_source):
        targets.append(_normalize_path(m.group(1)))
    for m in _WRITE_OVERWRITE_TABLE_RE.finditer(cell_source):
        targets.append(m.group(1).strip())
    return targets


def detect_stale_lazy_eval_risks(all_sources: List[str]) -> Dict[int, List[dict]]:
    """Detect cross-cell stale Spark lazy evaluation risks across all notebook cells.

    Scans all cells in order to find the three-cell pattern:
      Cell A (read_cell):       df = spark.read.parquet("oci://bucket/path/")
      Cell B (overwrite_cell):  other.write.mode("overwrite").parquet("oci://bucket/path/")
      Cell C (consume_cell):    df.write...  <- stale plan, will fail or write wrong data

    Returns:
        dict mapping consume_cell_index -> list of stale_lazy_eval risk dicts.
        Each risk dict describes the DataFrame, source path, and which cells are involved.
    """
    # Phase 1: per-cell reads and writes
    cell_reads: Dict[int, List[Tuple[str, str]]] = {}   # cell_idx -> [(var, path)]
    cell_writes: Dict[int, List[str]] = {}               # cell_idx -> [path]

    for i, source in enumerate(all_sources):
        if source.strip():
            reads = _extract_df_reads(source)
            if reads:
                cell_reads[i] = reads
            writes = _extract_overwrite_targets(source)
            if writes:
                cell_writes[i] = writes

    # Phase 2: build path -> [(cell_idx, df_var)] index for all reads
    path_to_readers: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
    for cell_idx, reads in cell_reads.items():
        for df_var, path in reads:
            path_to_readers[path].append((cell_idx, df_var))

    # Phase 3: for each overwrite, find earlier reads from the same path,
    # then find later cells that use that DataFrame variable in a write.
    risks: Dict[int, List[dict]] = defaultdict(list)

    for write_cell_idx, write_paths in cell_writes.items():
        for write_path in write_paths:
            for (read_cell_idx, df_var) in path_to_readers.get(write_path, []):
                if read_cell_idx >= write_cell_idx:
                    continue  # read is after the overwrite — not a stale plan issue

                # Find cells after the overwrite that use df_var in a write operation.
                # Stop scanning if df_var is reassigned from a new source (breaks stale plan).
                df_var_re = re.compile(rf'\b{re.escape(df_var)}\b')
                reassign_re = re.compile(
                    rf'^[ \t]*{re.escape(df_var)}\s*='
                    rf'(?!\s*{re.escape(df_var)}\b)',  # exclude df = df.xxx transforms
                    re.MULTILINE,
                )
                for consume_cell_idx, source in enumerate(all_sources):
                    if consume_cell_idx <= write_cell_idx:
                        continue
                    # If df_var is reassigned from a different source in this cell,
                    # the stale plan is broken — stop looking further.
                    if reassign_re.search(source):
                        break
                    if not df_var_re.search(source):
                        continue
                    # Only flag if this cell also does a write (otherwise it's a read-only use)
                    if not re.search(r'\.write\b', source):
                        continue
                    # Avoid duplicate risks for the same (df_var, path) pair
                    already = any(
                        r["dataframe"] == df_var and r["read_path"] == write_path
                        for r in risks[consume_cell_idx]
                    )
                    if already:
                        continue

                    risks[consume_cell_idx].append({
                        "type": "stale_lazy_eval",
                        "severity": "HIGH",
                        "dataframe": df_var,
                        "read_path": write_path,
                        "read_cell": read_cell_idx,
                        "overwritten_by_cell": write_cell_idx,
                        "fix": (
                            f"Materialize '{df_var}' before write — source path was "
                            f"overwritten in cell {write_cell_idx}. "
                            f"Inject: from aidp_compat import safe_materialize, safe_unpersist; "
                            f"{df_var} = safe_materialize({df_var}); "
                            f"<your write here>; "
                            f"safe_unpersist({df_var})"
                        ),
                    })

    return dict(risks)


# ─── FUSE Double-Read Detection ───────────────────────────────────────────────
#
# FUSE cache eviction between two reads of the same file path:
#   Cell A: open("/Volumes/.../file.txt", "r")  → succeeds, FUSE caches inode
#   <kernel evicts FUSE inode cache>
#   Cell B: open("/Volumes/.../file.txt", "r")  → FileNotFoundError (path exists!)
#
# Also occurs within a class when the same file is read in two methods.
# Fix: use safe_read_file(path) from aidp_compat — retries with delay on eviction.
# Confirmed: example_input.txt in ExampleJob notebook.

# Match: open("path", "r") or open("path", "rb") — capture the path literal
_FILE_READ_LITERAL_RE = re.compile(
    r'open\s*\(\s*["\']([^"\']+)["\'],?\s*["\']r[b]?["\']',
)

# Match: open(var, "r") — capture the variable name
_FILE_READ_VAR_RE = re.compile(
    r'open\s*\(\s*(\w+)\s*,\s*["\']r[b]?["\']',
)


def _extract_file_reads(cell_source: str) -> List[Tuple[str, str]]:
    """Extract (identifier, kind) pairs for file reads in a cell.

    kind is 'literal' for hard-coded paths, 'var' for variable names.
    Returns list of (value, kind) pairs — value is the path string or variable name.
    """
    results = []
    for m in _FILE_READ_LITERAL_RE.finditer(cell_source):
        path = _normalize_path(m.group(1))
        if path:
            results.append((path, "literal"))
    for m in _FILE_READ_VAR_RE.finditer(cell_source):
        var = m.group(1)
        # Skip variables that are obviously not paths (single chars, common loop vars)
        if var and len(var) > 1 and var not in ("f", "fp", "fh", "fd"):
            results.append((var, "var"))
    return results


def detect_double_read_risks(all_sources: List[str]) -> Dict[int, List[dict]]:
    """Detect cross-cell FUSE double-read FileNotFoundError risks.

    Pattern:
        Cell A: open("/Volumes/.../file.txt", "r")  → FUSE caches inode
        Cell B: open("/Volumes/.../file.txt", "r")  → FUSE cache evicted → FileNotFoundError

    File exists on disk — the error is caused by the FUSE kernel cache being evicted
    between the two reads. Adding a delay before the second open fixes it. The robust
    fix is safe_read_file() from aidp_compat which retries automatically.

    Only flags the SECOND (and later) reads of the same path — the first read is safe.
    Both literal paths and variable names are tracked.

    Returns:
        dict mapping cell_index -> list of double_read risk dicts.
    """
    # Track first-seen cell index for each path/variable
    first_seen: Dict[str, int] = {}   # identifier -> cell_idx of first read
    risks: Dict[int, List[dict]] = defaultdict(list)

    for cell_idx, source in enumerate(all_sources):
        if not source.strip():
            continue
        reads = _extract_file_reads(source)
        for identifier, kind in reads:
            if identifier in first_seen:
                first_cell = first_seen[identifier]
                if first_cell == cell_idx:
                    # Same cell — already flagged by volumes_double_read FUSE_PATTERN
                    continue
                # Cross-cell second read — flag this cell
                already = any(
                    r["identifier"] == identifier
                    for r in risks[cell_idx]
                )
                if not already:
                    risks[cell_idx].append({
                        "type": "fuse_double_read",
                        "severity": "LOW",
                        "identifier": identifier,
                        "kind": kind,
                        "first_read_cell": first_cell,
                        "fix": (
                            f"Note: cross-cell double-read of "
                            f"{'path' if kind == 'literal' else 'variable'} "
                            f"'{identifier}' (first in cell {first_cell}). "
                            f"No workaround needed — FUSE issues resolved."
                        ),
                    })
            else:
                first_seen[identifier] = cell_idx

    return dict(risks)


def _strip_comments(source: str) -> str:
    """Strip comments from source for active-code-only pattern matching.

    Handles Python (#), Scala (// and /* */), SQL (--), and Python
    triple-quoted strings used as block comments. Used so that pattern
    detection only flags active code — commented-out s3:// paths, boto3
    imports, etc. are dead code and should not trigger tool calls or
    migration actions.
    """
    # Phase 1: Remove triple-quoted string contents (Python docstrings/block comments)
    result = re.sub(r'"""[\s\S]*?"""', '""""""', source)
    result = re.sub(r"'''[\s\S]*?'''", "''''''", result)

    # Phase 2: Remove /* ... */ block comments (Scala/Java)
    result = re.sub(r'/\*[\s\S]*?\*/', '', result)

    # Phase 3: Remove full-line comments (Python #, Scala //, SQL --)
    lines = []
    for line in result.splitlines():
        stripped = line.strip()
        if stripped.startswith('#') or stripped.startswith('//') or stripped.startswith('--'):
            continue
        lines.append(line)

    return '\n'.join(lines)


def _is_unconditional_notebook_exit(source: str) -> bool:
    """Return True if source contains an unconditional notebook.exit() call.

    Unconditional = the exit is at the top level: zero indentation AND not
    inside an open brace block. Indentation alone is NOT sufficient — Scala/Java
    code inside `if (...) { ... }` may be written at column 0, so a conditional
    Scala exit can look unindented. We also track brace depth: an exit while a
    `{` block is still open (depth > 0) is conditional, so we do NOT treat it as
    unconditional. (Python conditional bodies are indented, so depth stays 0 and
    the indent check still handles them.)

    Bias to safety: if brace counting is thrown off (e.g. braces in strings),
    it can only over-count depth → we under-report "unconditional" → cells are
    executed rather than wrongly skipped. The dangerous direction (skipping
    reachable cells) is what we guard against.
    """
    depth = 0
    for line in source.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#") or stripped.startswith("//"):
            continue
        if (stripped.startswith("oidlUtils.notebook.exit(") or
                stripped.startswith("dbutils.notebook.exit(")):
            indent = len(line) - len(stripped)
            if indent == 0 and depth == 0:
                return True
        # Update brace depth AFTER the exit check (the if-open is on a prior line).
        depth += stripped.count("{") - stripped.count("}")
    return False


def analyze_cell_static(cell_index: int, cell_source: str,
                        all_cells: List[str], prior_plans: List[dict]) -> dict:
    """Static analysis of a single cell - no API calls needed.

    Extracts paths, tables, FUSE risks, and Databricks patterns
    using regex. Fast and deterministic.

    Returns a partial plan that can be enriched by Opus.
    """
    plan = {
        "cell_index": cell_index,
        "action": "migrate",
        "changes_needed": [],
        "storage_paths": [],
        "table_refs": [],
        "fuse_risks": [],
        "databricks_patterns": [],
        "dependencies": [],
        "is_run_cell": False,
        "is_empty": False,
        "is_markdown": False,
    }

    source = cell_source.strip()
    if not source:
        plan["action"] = "skip_empty"
        plan["is_empty"] = True
        return plan

    # Detect %scala/%%scala and %sql cells — AIDP supports these natively.
    # Skip dependency extraction: Scala imports (org.*, java.*, scala.*) are
    # not Python packages and must NOT be fed to ensure_requirements_installed.
    _is_scala = source.startswith("%scala") or source.startswith("%%scala")
    _is_sql = source.startswith("%sql") or source.startswith("%%sql")

    # Detect %run / notebook.run
    if source.startswith("%run"):
        plan["action"] = "inline_run"
        plan["is_run_cell"] = True
        run_path = source[4:].strip().split()[0]
        plan["changes_needed"].append(f"Inline %run target: {run_path}")
        return plan

    # Strip comments for pattern matching — only analyze active code.
    # Avoids wasting tool calls/tokens on commented-out s3:// paths, boto3, etc.
    active_source = _strip_comments(source)

    if "dbutils.notebook.run(" in active_source or "notebook.run(" in active_source:
        plan["action"] = "inline_run"
        plan["is_run_cell"] = True
        m = re.search(r'notebook\.run\s*\(\s*["\']([^"\']+)["\']', active_source)
        if m:
            plan["changes_needed"].append(f"Inline notebook.run target: {m.group(1)}")
        if "dbutils.notebook.run(" in active_source:
            plan["changes_needed"].append(
                "Convert dbutils.notebook.run(path, timeout, params) → oidlUtils.notebook.run(path, timeout, params) "
                "if inlining is not available — tag # Oracle tool modification:. "
                "AIDP REJECTS timeoutSeconds=0/null ('timeoutSeconds is null or empty'); if the timeout "
                "(2nd arg) is 0, set a positive value (3600 or max supported)."
            )
        return plan

    # Extract storage paths (active code only)
    for pattern in [
        r'oci://[\w\-\.]+@[\w]+/[\w\-\./]*',
        r's3[a]?://[\w\-\.]+/[\w\-\./]*',
        r'dbfs:/[\w\-\./]+',
        r'/mnt/[\w\-\./]+',
    ]:
        for match in re.findall(pattern, active_source):
            plan["storage_paths"].append(match)

    # Extract table references (active code only)
    # Strip import lines first — "from X.Y import Z" triggers the SQL FROM regex
    _non_import_source = "\n".join(
        ln for ln in active_source.splitlines()
        if not ln.strip().startswith(("import ", "from "))
    )
    _PYTHON_MODULES = {
        "os", "sys", "json", "re", "io", "math", "time", "datetime", "collections",
        "functools", "itertools", "typing", "pathlib", "hashlib", "base64", "copy",
        "csv", "logging", "traceback", "subprocess", "tempfile", "shutil", "glob",
        "urllib", "http", "concurrent", "threading", "multiprocessing", "abc",
        "dataclasses", "enum", "struct", "pickle", "gzip", "zipfile", "contextlib",
        "spark", "pyspark", "dbutils", "np", "pd", "numpy", "pandas", "scipy",
        "sklearn", "matplotlib", "seaborn", "plotly", "optuna", "xgboost",
        "lightgbm", "tensorflow", "torch", "keras", "PIL", "cv2", "requests",
        "boto3", "botocore", "oci", "self", "delta", "pytz", "dateutil",
        "aidp_compat", "oidlUtils", "pprint", "operator", "decimal", "fractions",
        "warnings", "inspect", "ast", "textwrap", "string", "random", "secrets",
        "uuid", "socket", "ssl", "email", "html", "xml", "ctypes",
    }
    # Patterns return 2-part or 3-part dotted names; we normalise to schema.table
    for pattern in [
        r'(?:FROM|JOIN|INTO|TABLE|OVERWRITE)\s+(\w+\.\w+(?:\.\w+)?)',
        r'spark\.(?:sql|table|read\.table)\s*\(\s*[\'"](\w+\.\w+(?:\.\w+)?)',
    ]:
        # Use import-stripped source for SQL keyword regex, original for spark.table
        src = _non_import_source if "FROM|JOIN" in pattern else active_source
        for match in re.findall(pattern, src, re.IGNORECASE):
            parts = match.split(".")
            if parts[0].lower() in _PYTHON_MODULES:
                continue
            # 3-part name (catalog.schema.table) → keep schema.table only
            if len(parts) == 3:
                match = f"{parts[1]}.{parts[2]}"
            plan["table_refs"].append(match)

    # Populate dependencies (from active code only).
    # Skip for %scala/%%scala/%sql cells — their imports (org.*, java.*, scala.*)
    # are JVM packages, not Python pip packages.
    if not (_is_scala or _is_sql):
        plan["dependencies"] = extract_imports(active_source)

    # Detect FUSE risks from explicit write/read patterns (active code only)
    for fuse in FUSE_PATTERNS:
        if re.search(fuse["pattern"], active_source):
            risk = {"name": fuse["name"], "fix": fuse["fix"]}
            # Check followup pattern (e.g., write then read in same cell)
            if "followup" in fuse and re.search(fuse["followup"], active_source):
                risk["severity"] = "LOW"
            else:
                risk["severity"] = "LOW"
            plan["fuse_risks"].append(risk)

    # Detect FUSE risks from risky package imports (active code only)
    seen_risky_pkgs: set[str] = set()
    for imp in plan["dependencies"]:
        pkg_key = _FUSE_IMPORT_INDEX.get(imp)
        if not pkg_key or pkg_key in seen_risky_pkgs:
            continue
        seen_risky_pkgs.add(pkg_key)
        pkg_info = FUSE_RISKY_PACKAGES[pkg_key]
        # Only flag if at least one risky call pattern is present in this cell
        matched = any(re.search(pat, active_source) for pat in pkg_info.get("patterns", []))
        if matched:
            plan["fuse_risks"].append({
                "type": "risky_package",
                "name": f"risky_package_{pkg_key}",
                "package": pkg_key,
                "severity": "LOW",
                "fix": f"Note: {pkg_key} I/O on /Volumes via FUSE detected. No workaround needed — FUSE issues resolved.",
            })
            # Informational only — do not add to changes_needed (FUSE issues resolved)

    # Detect Databricks patterns (active code only)
    for db_pat in DATABRICKS_PATTERNS:
        if re.search(db_pat["pattern"], active_source, re.IGNORECASE):
            plan["databricks_patterns"].append(db_pat["action"])
            if "Skip" in db_pat["action"]:
                plan["action"] = "skip_notification"
            elif db_pat["action"] not in plan["changes_needed"]:
                plan["changes_needed"].append(db_pat["action"])

    # Detect additional risk patterns (active code only)
    if "risks" not in plan:
        plan["risks"] = []
    for risk_pat in RISK_PATTERNS:
        if re.search(risk_pat["pattern"], active_source, re.IGNORECASE | re.MULTILINE):
            plan["risks"].append({
                "type": risk_pat["name"],
                "severity": risk_pat["severity"],
                "fix": risk_pat["fix"],
            })
            if risk_pat["fix"] not in plan["changes_needed"]:
                plan["changes_needed"].append(f"[{risk_pat['name']}] {risk_pat['fix']}")

    return plan


def render_cell_plan(plan: dict) -> str:
    """Render a cell plan as text for the Opus migration prompt."""
    parts = [f"Cell {plan['cell_index']} plan:"]
    parts.append(f"  Action: {plan['action']}")

    if plan["changes_needed"]:
        parts.append("  Changes needed:")
        for change in plan["changes_needed"]:
            parts.append(f"    - {change}")

    if plan["storage_paths"]:
        parts.append(f"  Storage paths to verify: {', '.join(plan['storage_paths'][:5])}")

    if plan["table_refs"]:
        parts.append(f"  Table references: {', '.join(plan['table_refs'][:5])}")

    if plan["fuse_risks"]:
        parts.append("  FUSE/IO patterns detected (informational — FUSE issues resolved, no workaround needed):")
        for risk in plan["fuse_risks"]:
            parts.append(f"    - [{risk['severity']}] {risk.get('name', risk.get('type', 'unknown'))}: {risk['fix']}")

    return "\n".join(parts)


async def analyze_notebook_cells(
    cells: list,
    session,
    cell_context,
    catalog_context: str,
    bucket_mapping_context: str,
    log_fn=None,
) -> List[dict]:
    """Analyze all cells in a notebook, producing per-cell migration plans.

    Phase 1: Static analysis (regex, fast)
    Phase 2: Dynamic verification on cluster (path/table checks via tools)

    Returns list of cell plans.
    """
    from context_tools import extract_storage_paths, suggest_oci_path

    def _log(msg):
        if log_fn:
            log_fn(msg)
        else:
            print(f"  [analysis] {msg}")

    all_sources = []
    for cell in cells:
        src = "".join(cell.get("source", []))
        all_sources.append(src)

    _log(f"Analyzing {len(all_sources)} cells...")

    # Phase 1: Static analysis
    plans = []
    for i, source in enumerate(all_sources):
        cell_type = cells[i].get("cell_type", "code")
        if cell_type != "code" or not source.strip():
            plans.append({
                "cell_index": i,
                "action": "skip_empty" if not source.strip() else "skip_markdown",
                "changes_needed": [],
                "storage_paths": [],
                "table_refs": [],
                "fuse_risks": [],
                "databricks_patterns": [],
                "is_run_cell": False,
            })
            continue

        plan = analyze_cell_static(i, source, all_sources, plans)
        plans.append(plan)

        # Stop analyzing cells after an unconditional notebook.exit()
        # Cells after an unconditional exit will never execute — no point migrating them.
        if _is_unconditional_notebook_exit(source):
            _log(f"Cell {i}: unconditional notebook.exit() — skipping {len(all_sources) - i - 1} subsequent cell(s)")
            for j in range(i + 1, len(all_sources)):
                plans.append({
                    "cell_index": j,
                    "action": "skip_after_exit",
                    "changes_needed": [],
                    "storage_paths": [],
                    "table_refs": [],
                    "fuse_risks": [],
                    "databricks_patterns": [],
                    "is_run_cell": False,
                    "skip_reason": "unreachable — after unconditional notebook.exit()",
                })
            break

    # Phase 1b: Cross-cell stale lazy eval detection
    # Must run after all plans are built so we have all_sources in scope.
    stale_risks = detect_stale_lazy_eval_risks(all_sources)
    if stale_risks:
        _log(f"Stale lazy eval scan: {sum(len(v) for v in stale_risks.values())} risk(s) across {len(stale_risks)} cell(s)")
    for cell_idx, risks in stale_risks.items():
        if cell_idx < len(plans):
            for risk in risks:
                plans[cell_idx]["fuse_risks"].append(risk)
                fix_msg = (
                    f"[stale_lazy_eval] '{risk['dataframe']}' read in cell {risk['read_cell']}, "
                    f"source overwritten in cell {risk['overwritten_by_cell']} — "
                    f"inject safe_materialize({risk['dataframe']}) before write"
                )
                if fix_msg not in plans[cell_idx]["changes_needed"]:
                    plans[cell_idx]["changes_needed"].append(fix_msg)

    # Phase 1c: Cross-cell FUSE double-read detection
    # Tracks every open(..., "r") across all cells — flags second reads of the same
    # path/variable as fuse_double_read risks (FUSE inode cache eviction between reads).
    double_read_risks = detect_double_read_risks(all_sources)
    if double_read_risks:
        _log(f"Double-read scan: {sum(len(v) for v in double_read_risks.values())} risk(s) across {len(double_read_risks)} cell(s)")
    for cell_idx, risks in double_read_risks.items():
        if cell_idx < len(plans):
            for risk in risks:
                plans[cell_idx]["fuse_risks"].append(risk)
                # Informational only — do not add to changes_needed (FUSE issues resolved)

    # Phase 2: Verify storage paths on cluster (batch)
    all_paths = set()
    all_tables = set()
    for plan in plans:
        all_paths.update(plan.get("storage_paths", []))
        all_tables.update(plan.get("table_refs", []))

    if all_paths:
        _log(f"Verifying {len(all_paths)} storage paths on cluster...")
        for path in list(all_paths)[:20]:  # limit to 20
            try:
                from context_tools import check_oci_path
                exists = await check_oci_path(session, path)
                status = "EXISTS" if exists else "NOT_FOUND"
                cell_context.paths_checked[path] = status

                # If not found, get suggestions
                if not exists:
                    suggestions = suggest_oci_path(path)
                    if suggestions:
                        cell_context.paths_checked[f"{path} -> {suggestions[0]}"] = "SUGGESTED"
                        # Update plans that use this path
                        for plan in plans:
                            if path in plan.get("storage_paths", []):
                                plan["changes_needed"].append(
                                    f"Path {path} NOT FOUND. Use {suggestions[0]} instead")
            except Exception:
                pass

    if all_tables:
        _log(f"Verifying {len(all_tables)} table references on cluster...")
        for table in list(all_tables)[:15]:
            try:
                from context_tools import verify_table_schema
                result = await verify_table_schema(session, table)
                if result.startswith("EXISTS:"):
                    cell_context.tables_checked[table] = result
                elif result == "EMPTY_SCHEMA":
                    cell_context.tables_checked[table] = "EMPTY_SCHEMA"
                    # Flag every plan that references this table
                    for plan in plans:
                        if table in plan.get("table_refs", []):
                            plan["changes_needed"].append(
                                f"Table {table} exists but has EMPTY SCHEMA (0 columns) — "
                                f"data not synced to AIDP. Add to "
                                f"/Workspace/<deploy_dir>/datafiles/tables_to_migrate.csv and wait "
                                f"for hourly sync, or contact infra team."
                            )
                elif result == "MISSING":
                    cell_context.tables_checked[table] = "MISSING"
                    for plan in plans:
                        if table in plan.get("table_refs", []):
                            plan["changes_needed"].append(
                                f"Table {table} NOT FOUND in AIDP catalog. "
                                f"Add to /Workspace/<deploy_dir>/datafiles/tables_to_migrate.csv "
                                f"with its S3 source path and wait for hourly sync."
                            )
                else:
                    cell_context.tables_checked[table] = result
            except Exception:
                # Fallback to offline catalog search if cluster check fails
                try:
                    from context_tools import search_catalog
                    result = search_catalog(table)
                    if "No tables matching" in result:
                        cell_context.tables_checked[table] = "MISSING"
                    else:
                        cell_context.tables_checked[table] = "EXISTS"
                except Exception:
                    pass

    # Table readiness summary
    if all_tables:
        missing = [t for t, s in cell_context.tables_checked.items() if s == "MISSING"]
        empty = [t for t, s in cell_context.tables_checked.items() if s == "EMPTY_SCHEMA"]
        healthy = [t for t, s in cell_context.tables_checked.items() if str(s).startswith("EXISTS:")]
        if missing or empty:
            _log(f"Table readiness: {len(healthy)} OK, {len(missing)} MISSING, {len(empty)} EMPTY_SCHEMA")
            for t in missing:
                _log(f"  ❌ {t} — NOT FOUND")
            for t in empty:
                _log(f"  ⚠️  {t} — EMPTY SCHEMA (0 columns, data not synced)")

    # Summary
    code_cells = sum(1 for p in plans if p["action"] not in ("skip_empty", "skip_markdown"))
    run_cells = sum(1 for p in plans if p.get("is_run_cell"))
    fuse_risks = sum(len(p.get("fuse_risks", [])) for p in plans)
    changes = sum(len(p.get("changes_needed", [])) for p in plans)
    _log(f"Analysis complete: {code_cells} code cells, {run_cells} %run, {fuse_risks} FUSE risks, {changes} changes needed")

    return plans
