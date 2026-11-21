"""DBX cluster spec → AIDP cluster payload.

Slim port of the reference notebook's databricks_jobcluster_to_aidp (cell 69)
plus extraction of libraries and init-script references. The actual library
fetch and AIDP upload happen elsewhere — this module produces:

  - the AIDP cluster create payload (displayName, driver/worker, spark cfg)
  - a list of artifact references (init scripts, libraries) that need to be
    uploaded to <export_output> and then translated to AIDP-side paths
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field, replace as dataclass_replace
from typing import Any, Dict, List, Optional, Tuple

from . import sanitizer
from .auth import _get_aidputils_from_kernel
from .node_type_catalog import (
    AIDP_SHAPE_AMD,
    AIDP_SHAPE_GPU,
    AIDP_SHAPE_INTEL,
    ShapeResolution,
    format_mapping_log_line,
    resolve_shape,
)

from .cluster_shape_mapping import (
    AUTO_TERMINATION_MAX,
    AUTO_TERMINATION_MIN,
    MAX_WORKER_COUNT,
    ShapeCapacityError,
    ShapeMappingError,
    _snap_up,
    apply_learned_limits,
    load_shape_limits,
    normalize_shape_config,
    parse_limits_from_error,
    update_shape_limits,
)

# ─── Recommended-spark-config helpers (customer YAML is source of truth) ─────
# Data moved out of this file 2026-08-22. The baseline `spark.*` values that get
# baked into every AIDP cluster this tool creates now live in the customer's
# YAML under `spark_config` and `autoscale_config`. See
# default_aidp_migrator.yaml for the canonical template + full rationale
# (Kryo trade-off, ignoreCorruptFiles trade-off, autoscale purpose).
#
# Consumers:
#   - convert_dbx_cluster() below — reads cfg.spark_config + cfg.autoscale_config
#   - scripts/job_migrate_from_workflow.py pre-flight — warns if the target
#     cluster is missing any of these keys (WARN only, no PATCH)


def recommended_spark_config(cfg) -> Dict[str, str]:
    """Return the customer's baseline spark.* config as a flat dict of strings.

    Merges cfg.spark_config + cfg.autoscale_config. YAML values may parse as
    non-string (e.g. int, bool); coerce to string here so downstream type
    inference / auto-heal handles them consistently with the customer's DBX
    source-cluster values (which are always strings from Databricks).
    """
    out: Dict[str, str] = {}
    for section_name in ("spark_config", "autoscale_config"):
        section = getattr(cfg, section_name, None) if cfg is not None else None
        if not isinstance(section, dict):
            continue
        for k, v in section.items():
            out[k] = v if isinstance(v, str) else str(v)
    return out


def _autoscale_keys(cfg) -> set:
    """Set of AIDP autoscale keys from cfg.autoscale_config (for pre-flight
    categorization). Empty set when cfg or the block is absent."""
    section = getattr(cfg, "autoscale_config", None) if cfg is not None else None
    return set(section.keys()) if isinstance(section, dict) else set()


# Cluster-level environment variables forced onto every NEWLY created AIDP
# cluster's sparkEnvVariables.
#   MLFLOW_TRACKING_URI — set at the cluster level so MLflow resolves the OCI
#   tracking server uniformly for EVERY task, vs. the reactive in-code
#   mlflow.set_tracking_uri() fix which only fires on tasks whose MLflow cell
#   errors (that reactive fix remains as an independent backup).
# Resolution order per var (see recommended_env_vars):
#   1. operator environment ($MLFLOW_TRACKING_URI) — wins if set
#   2. plain-string YAML value (cfg.secrets.mlflow_tracking_uri)
#   3. credential-store ref — resolved on-cluster via aidputils at cluster-CREATE
#      and MATERIALIZED into sparkEnvVariables. NOTE: the resolved URI may embed a
#      password, so it is then stored in the cluster resource (readable via the
#      cluster GET) — a deliberate, accepted trade-off for uniform coverage over
#      the more complex bootstrap-resolve approach. Rotate the credential per
#      issue #50. On laptop aidputils is None → the ref is skipped (unchanged).
# A var with none of the above is simply not injected (no regression).
_RECOMMENDED_ENV_VAR_SOURCES = ("MLFLOW_TRACKING_URI",)


def recommended_env_vars(cfg=None) -> Dict[str, str]:
    """Env vars to inject onto every new cluster's sparkEnvVariables.

    Injected vars:
      - MLFLOW_TRACKING_URI: precedence is
          1. operator environment ($MLFLOW_TRACKING_URI) — wins if set
          2. cfg.secrets.mlflow_tracking_uri — customer YAML value
          3. absent — not injected (MLflow feature disabled for this customer)
      - AIDP_WORKSPACE_ID: from cfg.aidp.workspace_id. Consumed by
          aidp_compat/notebook.py's _NotebookContext.toJson() mock so migrated
          notebooks that call dbutils.notebook.getContext().toJson() see the
          customer's real workspace UUID instead of a stale literal.
          It must be the workspace the cluster is created in — the migrated
          job trigger posts to $AIDP_WORKSPACE_ID when it is set — so
          dbx_export main() passes a cfg whose aidp.workspace_id already
          carries the effective --workspace-id value, not the raw YAML
          (REF-208).
    """
    out: Dict[str, str] = {}
    for _name in _RECOMMENDED_ENV_VAR_SOURCES:
        _val = os.environ.get(_name)
        if _val:
            out[_name] = _val
    if "MLFLOW_TRACKING_URI" not in out and cfg is not None:
        _mlflow = getattr(getattr(cfg, "secrets", None), "mlflow_tracking_uri", None)
        if isinstance(_mlflow, str) and _mlflow:
            # Plain-string URI in the YAML (no embedded secret) — inject directly.
            out["MLFLOW_TRACKING_URI"] = _mlflow
        elif _mlflow is not None:
            # Credential-store ref (SimpleNamespace with .name/.key). The MLflow
            # tracking URI embeds a password, so it is kept OUT of the git-tracked
            # YAML and lives in the credential store. dbx_export runs ON an AIDP
            # cluster, so `aidputils` is reachable here — resolve the ref to the
            # real URI at cluster-create and bake it into the new cluster's
            # sparkEnvVariables (so EVERY task gets it uniformly, not just the ones
            # whose MLflow cell happens to error). On laptop `aidputils` is None,
            # so this is skipped and laptop behavior is unchanged. The reactive
            # in-code mlflow.set_tracking_uri() fix (agent_migrate.py) is retained
            # as a runtime backup for any cluster that still lacks this env var.
            _aidputils = _get_aidputils_from_kernel()
            _name = getattr(_mlflow, "name", None)
            _key = getattr(_mlflow, "key", None)
            if _aidputils is not None and _name and _key:
                try:
                    _uri = _aidputils.secrets.get(_name, key=_key)
                    if _uri and str(_uri).strip():
                        out["MLFLOW_TRACKING_URI"] = str(_uri).strip()
                except Exception as _e:  # noqa: BLE001 - never block cluster create
                    print(
                        f"[cluster-env] MLflow tracking URI resolve from credential "
                        f"store failed ({type(_e).__name__}: {_e}); skipping env var "
                        f"(reactive set_tracking_uri fix remains as backup)",
                        flush=True,
                    )
    if cfg is not None:
        _ws_id = getattr(getattr(cfg, "aidp", None), "workspace_id", None)
        if _ws_id:
            out["AIDP_WORKSPACE_ID"] = _ws_id
    return out


# DBX runtime paths that don't exist on AIDP. Init script lines that reference
# these get commented out — the JAR/file install becomes the AIDP libraries
# API's job; /dbfs/* operations have no AIDP equivalent and must be reviewed
# manually by the user.
_DBX_RUNTIME_PREFIXES = ("/databricks/", "/dbfs/")


# Matches absolute /Workspace/... paths pointing at artifact files referenced
# inside an init script (or any other shell-style content). Used to discover
# JARs/wheels that the init script depends on but that aren't listed in the
# DBX job's libraries[] array.
#
# Three alternations handle: double-quoted paths (may contain spaces),
# single-quoted paths, and bare paths (no whitespace).
_ARTIFACT_EXTS = r"(?:jar|whl|egg|zip|tar\.gz|so)"
_ARTIFACT_PATH_RE = re.compile(
    r'"(/Workspace/[^"]+?\.' + _ARTIFACT_EXTS + r')"'
    r"|'(/Workspace/[^']+?\." + _ARTIFACT_EXTS + r")'"
    r"|(/Workspace/[^\s'\"<>;|&]+?\." + _ARTIFACT_EXTS + r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def find_artifact_refs_in_script(content: str) -> List[str]:
    """Return distinct /Workspace/... artifact paths referenced in a script."""
    if not content:
        return []
    seen, out = set(), []
    for m in _ARTIFACT_PATH_RE.finditer(content):
        # One of three groups matched (double-quoted, single-quoted, bare).
        p = m.group(1) or m.group(2) or m.group(3)
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    return out


def rewrite_init_script(
    content: str,
    workspace_uri_to_aidp_path: Dict[str, str],
) -> Tuple[str, List[Tuple[str, str]]]:
    """Rewrite an init script's content so it works on AIDP.

    Two transformations:
      1. Every /Workspace/... artifact path that we've exported to AIDP is
         replaced with its AIDP destination (so `cp <source>` finds the file).
      2. Any line that still references a DBX runtime path (e.g. /databricks/)
         is commented out — those paths don't exist on AIDP and would error.
         The JAR/artifact those lines were trying to install is registered
         via the AIDP cluster libraries API by the orchestrator, so the copy
         is redundant on AIDP anyway.

    Returns (new_content, replacements) where replacements is a list of
    (old, new) tuples for logging.
    """
    # (1) source artifact paths
    out = content
    replacements: List[Tuple[str, str]] = []
    for old_path, new_path in workspace_uri_to_aidp_path.items():
        if old_path in out:
            out = out.replace(old_path, new_path)
            replacements.append((old_path, new_path))

    # (2) comment out lines that still reference DBX-only runtime paths.
    new_lines: List[str] = []
    commented = 0
    surviving_logical = 0       # non-shebang, non-blank, non-comment, not-commented-out
    for line in out.split("\n"):
        stripped = line.lstrip()
        # Skip blanks, plain comments, shebangs.
        if not stripped or stripped.startswith("#"):
            new_lines.append(line)
            continue
        if any(p in line for p in _DBX_RUNTIME_PREFIXES):
            new_lines.append(
                f"# [dbx_export] disabled — DBX runtime path; install via AIDP libraries API: {line}"
            )
            commented += 1
        else:
            new_lines.append(line)
            surviving_logical += 1
    if commented:
        replacements.append((f"<commented out {commented} DBX runtime line(s)>", "# [dbx_export] ..."))

    became_no_op = commented > 0 and surviving_logical == 0
    partial_export = commented > 0 and surviving_logical > 0
    return "\n".join(new_lines), replacements, became_no_op, partial_export

# Every DBX node_type_id flows through node_type_catalog.resolve_shape() which
# picks arm.generic / amd.generic / intel.generic / gpu.a10 based on the DBX
# node's architecture family, with a documented AMD fallback for unknowns.
# See dbx_export/node_type_catalog.py for the catalog + resolution rules.
AIDP_SPARK_VERSION = "3.5.0"

# ─── Spark conf / env-var filter ─────────────────────────────────────
#
# DBX clusters carry conf and env-var entries that don't make sense (and
# typically break) on AIDP because they reference resources only DBX has:
# DBX-bundled Python, S3 buckets, DBFS paths, DBX JARs, Azure/GCP/S3-specific
# hadoop configs, custom listener classes whose JARs we haven't exported, etc.
# We drop anything matching the patterns below and report what was dropped.

# Env-var keys to drop outright. Every entry here is a DBX-runtime artifact
# (not customer workload tuning) — the customer didn't "tune" these for their
# job, DBX just injects them into the cluster spec as part of its runtime
# bootstrap. Porting them silently to AIDP creates confusing dead-freight at
# best and JVM/Python-path mismatches at worst.
_DROP_ENV_KEYS = {
    "PYSPARK_PYTHON",          # /databricks/python3/bin/python3 — wrong on AIDP
    "PYSPARK_DRIVER_PYTHON",
    # Databricks Java runtime selector — DBX bootstrap reads this to pick
    # /databricks/jdk/<jname>/ on the runtime image (e.g. zulu11-ca-amd64 =
    # Azul Zulu OpenJDK 11 AMD64). AIDP has no such mechanism; setting JNAME
    # on the AIDP cluster is at best dead-freight, and at worst a downstream
    # shell script may grep for JNAME and pick a wrong JVM path.
    # Observed on a pilot-customer AutoRefit export 2026-07-25: JNAME=zulu11-ca-amd64 landed
    # in sparkEnvVariables and did nothing useful. Filed as an unconditional
    # drop because it's a DBX artifact, not customer tuning.
    "JNAME",
}

# Spark properties AIDP reserves for internal use. Overriding any of them fails
# the cluster create with SPARK_CONFIGURATION_PROPERTY_RESERVED — the platform
# (not the user) sets these, but a DBX cluster spec often carries them. Matched
# by exact key (case-insensitive) and dropped. AIDP reports reserved props one
# at a time, so extend this set if the create rejects another one.
# NOTE: keys are stored LOWERCASED so membership tests can use `k.lower()`.
# (Canonical Spark casing is e.g. `spark.submit.deployMode`; lowercase here is
# intentional for matching, not the real property name.)
_RESERVED_CONF_KEYS = {
    "spark.master",
    "spark.submit.deploymode",
    "spark.app.id",
    "spark.driver.host",
    "spark.driver.port",
    "spark.driver.bindaddress",
    "spark.executor.id",
    # DBX Maven/Ivy install controls — AIDP reserves these because Maven
    # package install goes through the cluster-libraries API instead. Empirical:
    # spark.jars.packages proven reserved (SPARK_CONFIGURATION_PROPERTY_RESERVED
    # observed 2026-07-24). The rest of the family are added together because
    # they are pointless without spark.jars.packages and typically paired.
    "spark.jars.packages",
    "spark.jars.repositories",
    "spark.jars.ivysettings",
    "spark.jars.excludes",
}

# ABSOLUTE MEMORY-SIZING KEYS — DROP UNCONDITIONALLY.
# These specify raw byte/gigabyte values (spark.executor.memory=30g etc.) that
# were tuned for the DBX cluster's specific shape (e.g. r-fleet.4xlarge with
# 16 vCPU / 128 GB). Copying them to AIDP is unsafe because:
#   1. AIDP shape is typically different (e.g. amd.generic 8 OCPU / 128 GB
#      driver, 4 OCPU / 64 GB workers). Absolute values sized for DBX
#      oversubscribe the smaller AIDP worker (30g executor + 11g overhead
#      = 41g on a 64GB worker leaves too little for OS/YARN reserve).
#   2. AIDP already sizes executor/driver memory from driverShapeConfig /
#      workerShapeConfig, with overhead as a percentage via
#      spark.executor.memoryOverheadFactor / spark.driver.memoryOverheadFactor
#      (both set to 0.2 in the customer YAML's spark_config). Those percentages scale
#      correctly to any shape; the absolute values don't.
#   3. spark.driver.maxResultSize=0 (unlimited) is a DBX default that removes
#      the runaway-collect() protection. AIDP has a sane default cap.
# These are DBX-shape ARTIFACTS, not workload tuning — the customer picked a
# DBX INSTANCE TYPE; the memory numbers followed automatically. Dropping them
# lets AIDP's shape-based sizing apply cleanly.
# Observed on a pilot-customer AutoRefit export 2026-07-25 where the DBX absolute values
# would have oversubscribed the AIDP 64GB worker.
# NOTE: keys are stored LOWERCASED so membership tests can use `k.lower()`.
_DROP_ABSOLUTE_MEMORY_KEYS = {
    "spark.executor.memory",
    "spark.executor.memoryoverhead",
    "spark.driver.memory",
    "spark.driver.memoryoverhead",
    "spark.driver.maxresultsize",
}

# JVM extra-options — DROP UNCONDITIONALLY.
# spark.executor.extraJavaOptions / spark.driver.extraJavaOptions carry
# JVM tuning flags (e.g. -XX:G1HeapRegionSize=32m, -XX:+UseG1GC,
# -Djava.io.tmpdir=/local_disk0/tmp). Every value here is DBX-runtime
# provenance: DBX ships specific flags for their JDK build and their
# node hardware. Porting them to AIDP's different JVM/hardware has
# undefined impact and is very hard to reason about after the fact:
#   - A flag that's benign on DBX Zulu 11 might interact badly with
#     AIDP's JDK version or GC tuning.
#   - DBX-specific paths (/databricks/, /local_disk0/) fail on AIDP.
# Better to drop entirely. If the operator has a genuine AIDP-side
# JVM tuning need, they can add it explicitly post-migration.
# Observed on a pilot-customer AutoRefit: -XX:G1HeapRegionSize=32m appeared in
# spark.executor.extraJavaOptions; benign in isolation but no way to
# verify it's safe with AIDP's G1GC defaults.
_DROP_JVM_OPT_KEYS = {
    "spark.executor.extrajavaoptions",
    "spark.driver.extrajavaoptions",
}

# Substrings matched (case-insensitive) against Spark conf keys.
_DROP_CONF_KEY_SUBSTRINGS = (
    "databricks",
    "fs.s3",                   # spark.hadoop.fs.s3a.*, fs.s3n.*, etc.
    "fs.aws",
    "kinesis",
    "redshift",
    "azure", "abfs", "wasb",
    "fs.gs", "fs.gcs",
    "queryexecutionlistener",  # spark.sql.queryExecutionListeners
    "extralisteners",          # spark.extraListeners
)

# Substrings matched against conf/env VALUES (paths that don't exist on AIDP).
_DROP_VALUE_SUBSTRINGS = (
    "/databricks/",
    "dbfs:/",
    "s3://", "s3a://", "s3n://",
    "com.databricks.",
)

# Spark conf keys that AIDP rejects unless the value is a positive integer.
# DBX specs sometimes carry non-integer values here (e.g. "auto", ""), which
# fail cluster creation with HTTP 400.
_POSITIVE_INT_CONF_KEYS = {
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
}

# Non-integer values on the keys above that express "let Spark size partitions
# dynamically". Rather than DROP them (which discards the adaptive-partitioning
# intent and can cause 3–5× regressions / OOM on skewed data), we TRANSLATE the
# intent to AIDP's platform-native Adaptive Query Execution. This preserves the
# customer's tuning on the target platform instead of silently losing it.
_ADAPTIVE_INTENT_VALUES = {"auto", "adaptive", "dynamic"}
# HISTORICAL NOTE: this dict used to inject
#     "spark.sql.adaptive.enabled": "true",
#     "spark.sql.adaptive.coalescePartitions.enabled": "true"
# but BOTH of those keys default to "true" on Spark 3.2+, and AIDP runs Spark
# 3.5. Injecting them explicitly is a no-op that just clutters the effective
# cluster config and makes it harder to see what was actually customer-tuned
# vs platform default. The size-tuning AQE keys
# (spark.sql.adaptive.advisoryPartitionSizeInBytes,
#  spark.sql.adaptive.coalescePartitions.minPartitionSize) DO differ from
# platform defaults and live in the customer's `spark_config` YAML block —
# they still apply unconditionally.
#
# We keep _ADAPTIVE_QUERY_EXEC_CONF as an (intentionally empty) dict + the
# translation code path so the operator log still shows the "auto → AQE"
# intent was recognized. The behavior is unchanged since AIDP has AQE on by
# default.
_ADAPTIVE_QUERY_EXEC_CONF: Dict[str, str] = {}


# ─── Value-type inference + validation helpers (auto-heal support) ──────────
#
# These helpers back the two-tier baseline-vs-customer merge in
# convert_dbx_cluster(): given our baseline value for a key, infer what
# TYPE the customer's DBX value should also have, and check whether it does.
# If not, we can attempt cleanup or fall back to the baseline value.
#
# Only used for auto-heal — no other call sites in the module. If auto-heal
# is ever reverted, these three helpers can be removed together.

# Spark "size" values are digits + a required magnitude letter (k/m/g/t),
# optionally followed by "b" (bytes suffix, e.g. "512mb"). We REQUIRE the
# magnitude letter — a plain "50000" is not a size, it's a raw int (e.g.
# spark.sql.execution.arrow.maxRecordsPerBatch = 50000 is a record count).
# Case-insensitive to accept "512M" / "1G" the same as "512m" / "1g".
_SIZE_VALUE_REGEX = re.compile(r"^\d+[kmgtKMGT](?:b|B)?$")


def _infer_recommended_value_type(rec_v: Any) -> str:
    """Classify a baseline spark-config default value into one of five
    types so we can validate the customer's DBX value against the same shape.
    Returns: 'bool' | 'size' | 'int' | 'float' | 'string'.

    Check order is deliberate:
      - 'bool' first (literal 'true'/'false' would fail every downstream regex
        anyway, but explicit ordering makes the code easier to follow).
      - 'size' before 'int' because our size values have a required unit letter
        that a pure-int check would reject anyway; ordering here just makes the
        classification unambiguous when reading the function.
      - 'int' before 'float' because int(x) accepts a strict subset of what
        float(x) accepts; we want the tighter type when it applies.
      - 'string' as the catch-all for anything we can't parse (class names,
        arbitrary tokens). No validation applies to string type.
    """
    s = str(rec_v).strip()
    if s.lower() in ("true", "false"):
        return "bool"
    if _SIZE_VALUE_REGEX.match(s):
        return "size"
    try:
        int(s)
        return "int"
    except (ValueError, TypeError):
        pass
    try:
        float(s)
        return "float"
    except (ValueError, TypeError):
        pass
    return "string"


def _matches_expected_type(v: Any, expected_type: str) -> bool:
    """True iff the value `v` is a well-formed instance of `expected_type`.

    Deliberately does NOT normalize: 'TRUE' and 'true' both return True but
    the caller keeps the original casing. Reason: some downstream Spark/JVM
    parsers historically checked case-sensitively; auto-normalizing risks
    breaking a customer who deliberately typed 'TRUE'. The auto-heal path is
    for MALFORMED values, not for cosmetic casing.
    """
    s = str(v).strip()
    if expected_type == "bool":
        return s.lower() in ("true", "false")
    if expected_type == "size":
        return bool(_SIZE_VALUE_REGEX.match(s))
    if expected_type == "int":
        try:
            int(s)
            return True
        except (ValueError, TypeError):
            return False
    if expected_type == "float":
        try:
            float(s)
            return True
        except (ValueError, TypeError):
            return False
    # 'string' — no validation possible; any non-empty is accepted.
    return bool(s)


def _cleanup_light_value(v: Any) -> str:
    """Strip whitespace + leading '=' from a value string.

    DELIBERATELY NARROW. Targeting only the observed corruption pattern:
        spark.sql.execution.arrow.maxRecordsPerBatch = "= 128"
    Some source (DBX admin typo or upstream data corruption) prepended
    '= ' to a value that would otherwise be a valid int. We strip that
    prefix + any surrounding whitespace and re-validate.

    We deliberately do NOT do:
      - Quote stripping ('"512m"' → '512m'): risks unwrapping an intentional
        quoted string.
      - Comma / bracket stripping: risks discarding structural chars in
        list-shaped values (rare but possible for some Spark keys).
      - Case normalization: handled implicitly by _matches_expected_type
        without mutating the value (see rationale there).
    """
    s = str(v).strip()
    # Drop any prefix of the form "= " or "=" (repeated in case of "== 128").
    # `.lstrip("=").lstrip()` handles both compact "=128" and spaced "= 128".
    while s.startswith("="):
        s = s[1:].lstrip()
    return s


def _filter_spark_dict(
    d: Optional[Dict[str, Any]],
    is_env: bool,
) -> Tuple[Dict[str, Any], List[Tuple[str, Any, str]], List[Tuple[str, Any, str]]]:
    """Drop DBX/cloud-specific entries. Returns (kept, dropped, translated):
    dropped = list of (key, value, reason) for genuinely-removed entries;
    translated = list of (key, value, note) for entries whose intent was
    re-expressed as an AIDP-native equivalent (the replacement confs are
    already merged into `kept`)."""
    kept: Dict[str, Any] = {}
    dropped: List[Tuple[str, Any, str]] = []
    translated: List[Tuple[str, Any, str]] = []
    if not d:
        return kept, dropped, translated
    for k, v in d.items():
        kl = (k or "").lower()
        vl = str(v) if v is not None else ""
        if is_env and k in _DROP_ENV_KEYS:
            dropped.append((k, v, "DBX-specific env var"))
            continue
        if not is_env and kl in _RESERVED_CONF_KEYS:
            dropped.append((k, v, "AIDP-reserved Spark property (internal use)"))
            continue
        if not is_env and kl in _DROP_ABSOLUTE_MEMORY_KEYS:
            # Absolute memory values are DBX-shape artifacts, not workload tuning.
            # AIDP sizes executor/driver memory from driverShapeConfig/workerShapeConfig,
            # with overhead as a percentage (memoryOverheadFactor=0.2 in RECOMMENDED).
            # See _DROP_ABSOLUTE_MEMORY_KEYS definition for full rationale.
            dropped.append((k, v, "absolute memory value (AIDP sizes from shape config + memoryOverheadFactor)"))
            continue
        if not is_env and kl in _DROP_JVM_OPT_KEYS:
            # JVM extra flags carry DBX-runtime provenance (their JDK, their hardware).
            # Porting to AIDP has undefined impact. Operator can re-add explicitly if needed.
            # See _DROP_JVM_OPT_KEYS definition for full rationale.
            dropped.append((k, v, "DBX-runtime JVM extra flags (undefined impact on AIDP)"))
            continue
        if not is_env and any(s in kl for s in _DROP_CONF_KEY_SUBSTRINGS):
            dropped.append((k, v, "DBX/cloud-specific conf key"))
            continue
        if not is_env and kl in _POSITIVE_INT_CONF_KEYS:
            try:
                valid = int(vl.strip()) > 0
            except (ValueError, AttributeError):
                valid = False
            if not valid:
                # Preserve "let Spark decide" intent as AQE instead of dropping.
                if vl.strip().lower() in _ADAPTIVE_INTENT_VALUES:
                    for _ak, _av in _ADAPTIVE_QUERY_EXEC_CONF.items():
                        kept.setdefault(_ak, _av)
                    # If _ADAPTIVE_QUERY_EXEC_CONF is empty (Spark 3.2+ defaults
                    # both AQE toggles to true — see comment on the constant),
                    # the translation is effectively a no-op recognition: AIDP's
                    # platform default already delivers the customer's intent.
                    _injected = (
                        ', '.join(_ADAPTIVE_QUERY_EXEC_CONF)
                        if _ADAPTIVE_QUERY_EXEC_CONF
                        else "AIDP Spark 3.5 defaults enable AQE — no explicit override needed"
                    )
                    translated.append((
                        k, v,
                        f"'{vl}' → Adaptive Query Execution ({_injected})"))
                else:
                    dropped.append((k, v, "non-positive-integer value with no AIDP equivalent"))
                continue
        if any(s in vl for s in _DROP_VALUE_SUBSTRINGS):
            dropped.append((k, v, "value references DBX/cloud path"))
            continue
        kept[k] = v
    return kept, dropped, translated

# Legacy DBX node_type_id table removed 2026-07-24. All types now flow through
# dbx_export/node_type_catalog.py::resolve_shape() which:
#   - preserves architecture family (ARM → arm.generic, Intel → intel.generic,
#     AMD → amd.generic, GPU → gpu.a10) instead of collapsing everything to AMD
#   - preserves exact vCPU / memory from the node-type catalog (derived from
#     the pilot deployment's real compute-usage report + common-family fill-ins; the SINGLE
#     node-type source of truth — see node_type_catalog.py)
#   - applies the correct OCPU formula per family (ARM: 1:1, x86/GPU: vCPU/2)
#   - falls back with a family-suffix parser for unknown DBX types
#   - flags ambiguous x86-fleet pools (Intel vs AMD undetectable) as WARN


@dataclass
class ClusterConversion:
    """Output of converting a single DBX cluster."""

    aidp_payload: Dict[str, Any]
    dbx_job_cluster_key: Optional[str] = None  # if from job_clusters[]
    init_script_refs: List[str] = field(default_factory=list)   # DBX URIs
    library_refs: List[Dict[str, Any]] = field(default_factory=list)  # raw lib entries
    dropped_conf: List[Tuple[str, Any, str]] = field(default_factory=list)
    dropped_env: List[Tuple[str, Any, str]] = field(default_factory=list)
    # Per-role shape mapping decisions (populated by convert_dbx_cluster). Each
    # entry is a rendered log line describing "DBX <type> (<arch>, N vCPU / M GB)
    # → AIDP <shape> (K OCPU / M GB) [reason]". Consumed by the phase3 create
    # path to print operator-facing log lines and to include in the JSON report.
    shape_mapping_log_lines: List[str] = field(default_factory=list)
    driver_shape_res: Optional[ShapeResolution] = None
    worker_shape_res: Optional[ShapeResolution] = None
    # PR #31: Spark confs whose intent was re-expressed as an AIDP-native
    # equivalent (e.g., 'auto' → AQE), rather than dropped outright.
    translated_conf: List[Tuple[str, Any, str]] = field(default_factory=list)



# GPU minimum-config policy (2026-07-31)
# --------------------------------------
# AIDP's only GA GPU shape is gpu.a10 (NVIDIA A10). DBX runs on AWS/Azure/GCP
# with a wide variety of GPU node types (T4, V100, A100, K80, etc.) — the
# per-GPU compute/memory profile differs significantly between families.
# Silently mapping DBX-side "8 GPUs / 128 GB" to gpu.a10 with the same numbers
# risks bringing up an oversized (and very expensive) cluster the customer
# didn't intend.
#
# Policy: on GPU shapes, force the SMALLEST valid config (1 GPU) and log what
# DBX asked. Operators can raise the spec in AIDP if the workload needs more.
#
# IMPORTANT: gpu.a10 is a FIXED bundle — the create API fills OCPU/memory from
# the gpu count and REJECTS a driverShapeConfig that also pins ocpus/memoryInGBs
# ("gpus should be 1 or 2", misleading; verified 2026-08-03). So _shape_config
# emits gpus ONLY for GPU shapes. The OCPU/MEMORY values below are INFORMATIONAL
# — the bundle AIDP materializes (1 GPU → 15 OCPU / 240 GB; 2 → 30 / 480) — and
# are intentionally NOT sent in the payload.
_AIDP_GPU_MIN_GPUS = 1
_AIDP_GPU_MIN_OCPUS = 15        # informational: AIDP fills this for 1 GPU
_AIDP_GPU_MIN_MEMORY_GB = 240   # informational: AIDP fills this for 1 GPU


def _shape_config(res: ShapeResolution, log_lines: List[str], role: str,
                  limits: Optional[dict]) -> Dict[str, int]:
    """AIDP driver/worker ShapeConfig from a resolved catalog entry.
    GPU shapes need a non-zero `gpus` count; CPU shapes get gpus=0.

    The catalog preserves the DBX vCPU/memory exactly, but AIDP only accepts
    a discrete set of OCPU counts and, per OCPU count, a discrete list of
    memory values — the create API rejects anything else with
    "Invalid ...ShapeConfig.memoryInGBs. Valid values are: [...]" (e.g.
    amd.generic 4 OCPU allows exactly [32, 64, 128] GB; live-verified
    2026-07-29/30, table in config/aidp_shape_limits.json). Without snapping,
    catalog entries like mg-fleet.xlarge (ARM 4 OCPU / 16 GB) are guaranteed
    create-time 400s. We snap UP onto the nearest valid combo so the workload
    never gets less than the DBX spec had; a request above the shape maximum
    raises ShapeCapacityError instead of being clamped down (fail at export
    time, not as a runtime OOM).

    GPU shapes (gpu.a10) get a special MINIMUM-CONFIG policy — see the module
    constants and log line below for rationale.
    """
    # GPU: always start at the minimum config; operators scale up in AIDP if
    # needed. See _AIDP_GPU_MIN_* constants above.
    if res.aidp_shape == AIDP_SHAPE_GPU:
        # GPU shapes (gpu.a10) are FIXED bundles: the create API fills
        # ocpus/memory from the gpu count and REJECTS a driverShapeConfig that
        # also pins ocpus/memoryInGBs — it returns the misleading
        # "driverShapeConfig is invalid! gpus should be 1 or 2" (empirically
        # verified 2026-08-03: gpus-only creates succeed and AIDP materializes
        # 1 GPU -> 15 OCPU/240 GB, 2 -> 30/480; sending ocpus/memory 400s).
        # So emit gpus ONLY. Policy unchanged: default to the minimum 1-GPU
        # bundle (GPU is costly); operators scale up in AIDP if needed.
        log_lines.append(
            f"  [shape-normalize] {role}: GPU shape — mapped to {AIDP_SHAPE_GPU} "
            f"with {_AIDP_GPU_MIN_GPUS} GPU (AIDP fills OCPU/memory from the fixed "
            f"bundle). DBX asked for {res.gpus} GPU / {res.ocpus} OCPU / "
            f"{res.memory_gb} GB — defaulting to minimum; adjust in AIDP if "
            f"higher capacity is needed."
        )
        return {"gpus": _AIDP_GPU_MIN_GPUS}

    try:
        norm = normalize_shape_config(
            res.ocpus, res.memory_gb, shape=res.aidp_shape, gpus=res.gpus,
            limits=limits)
    except ShapeCapacityError:
        # The table KNOWS this shape and says the request is impossible —
        # never pass it through; the caller fails this cluster loudly.
        raise
    except ShapeMappingError:
        # No limits table for this shape — pass through (self-heal on 400
        # will populate it if AIDP rejects).
        log_lines.append(
            f"  [shape-normalize] {role}: no validity table for "
            f"{res.aidp_shape} — {res.ocpus} OCPU / {res.memory_gb} GB "
            f"passed through unvalidated")
        return {"ocpus": res.ocpus, "gpus": res.gpus, "memoryInGBs": res.memory_gb}
    if norm.was_adjusted:
        log_lines.append(f"  [shape-normalize] {role}: {norm.describe()}")
    return {
        "ocpus": norm.ocpus,
        "gpus": res.gpus,
        "memoryInGBs": norm.memory_gb,
    }


# x86 shape families. AIDP hosts amd.generic and intel.generic on the same
# instruction set; amd.generic is the strict superset (64 OCPU max vs
# intel.generic's 32) and the only x86 shape live-verified against a real
# workspace — so it is the harmonization target whenever the two roles land
# on different x86 shapes.
_X86_SHAPES = frozenset({AIDP_SHAPE_AMD, AIDP_SHAPE_INTEL})


def harmonize_shape_family(
    driver_res: ShapeResolution,
    worker_res: ShapeResolution,
) -> Tuple[ShapeResolution, ShapeResolution, List[str]]:
    """Force driver and worker onto ONE common AIDP shape name.

    AIDP rejects a cluster whose driver and worker use different shapes:
        "Invalid driver shape amd.generic and executor Shape intel.generic.
         Both need to have a same shape name"
    (live-hit 2026-08-03). This happens whenever the two roles resolve to
    different shapes — most commonly when one x86 role is big enough to be
    overridden to amd.generic (e.g. c6i.24xlarge driver, 48 OCPU > intel's
    32-OCPU ceiling) while its smaller x86 counterpart stays intel.generic.

    resolve_shape() maps each role independently, so this harmonization is a
    SECOND pass over the pair. Both roles are x86 in every realistic mismatch
    (the override only ever targets amd.generic), so we snap both to
    amd.generic — the x86 superset — and let _shape_config re-normalize each
    role's OCPU/memory against the new shape (never downsizing capacity).

    Returns ``(driver_res, worker_res, log_lines)`` with the shape name(s)
    updated on whichever role(s) changed and a WARN line per change. A
    cross-family mismatch (x86↔ARM, or GPU↔CPU) cannot be reconciled without
    silently changing the instruction set or dropping GPUs, so it raises
    ShapeCapacityError for the operator to resolve deliberately.
    """
    if driver_res.aidp_shape == worker_res.aidp_shape:
        return driver_res, worker_res, []

    dshape, wshape = driver_res.aidp_shape, worker_res.aidp_shape
    if not (dshape in _X86_SHAPES and wshape in _X86_SHAPES):
        raise ShapeCapacityError(
            f"driver shape {dshape!r} and worker shape {wshape!r} are in "
            f"different families — AIDP requires both roles to share one shape "
            f"name, and reconciling these would change the instruction set or "
            f"drop GPUs. Align the DBX driver/worker node types (or pick one "
            f"AIDP shape) and re-export."
        )

    target = AIDP_SHAPE_AMD
    lines: List[str] = []

    def _retarget(res: ShapeResolution, role: str) -> ShapeResolution:
        if res.aidp_shape == target:
            return res
        lines.append(
            f"  [shape-harmonize][WARN] {role}: {res.aidp_shape} -> {target} "
            f"to match the other role (AIDP requires driver and worker to "
            f"share one shape name; amd.generic is the x86 superset)")
        return dataclass_replace(
            res,
            aidp_shape=target,
            note=f"{res.note} | harmonized to {target} so driver and worker "
                 f"share one shape name",
        )

    driver_res = _retarget(driver_res, "driver")
    worker_res = _retarget(worker_res, "worker")
    return driver_res, worker_res, lines


def learn_limits_and_renormalize(payload: Dict[str, Any],
                                 error_text: str) -> List[str]:
    """Self-heal a create payload after a shape-validation 400.

    The limits file can lag reality (AIDP platform update, or a shape with
    no table yet like gpu.a10). When the create API rejects a payload, its
    400 message states the truth ("Invalid ...ShapeConfig.memoryInGBs.
    Valid values are: [32, 64, 128]."). This function:

      1. parses the learned facts out of ``error_text`` for the role the
         error names (driverShapeConfig vs executorShapeConfig — AIDP's name
         for the worker side),
      2. persists list-type facts into the limits store (precedence:
         explicit path > $AIDP_SHAPE_LIMITS_PATH > the committed global
         table) so every later cluster benefits,
      3. applies the learned facts DIRECTLY to both roles' shape configs in
         ``payload`` — direct application (not full re-normalization) so that
         partial knowledge is still used: an OCPU-only error snaps OCPUs even
         when no memory table exists yet for that shape; the memory list for
         the new OCPU count then arrives on the next 400, converging in at
         most two heal cycles.

    Retry contract: returns non-empty log lines ONLY when the payload
    actually changed. Learning/persisting alone never justifies a retry —
    an unchanged payload would just produce the identical 400 again. An
    empty return means the caller must re-raise the original error.
    """
    is_driver_err = "driverShapeConfig" in error_text
    role_key, shape_key, cfg_key = (
        ("driverConfig", "driverShape", "driverShapeConfig") if is_driver_err
        else ("workerConfig", "workerShape", "workerShapeConfig"))
    shape = payload[role_key][shape_key]
    err_ocpus = payload[role_key][cfg_key].get("ocpus")

    learned = parse_limits_from_error(error_text, ocpus=err_ocpus)
    if not learned:
        return []

    lines: List[str] = []

    # Persist list-type knowledge (valid OCPU counts / memory lists) via the
    # interprocess-safe transaction: parallel exporters self-heal into the
    # same file, and a plain load->apply->save here would let them lose each
    # other's learned values. Bounds (memory_gb_min/max) are payload-level
    # only: the table model has no consumer for them and the global floor is
    # already encoded in cluster_shape_mapping.
    state = {"changed": False}

    def _apply(current_limits: dict) -> None:
        state["changed"] = apply_learned_limits(
            current_limits, shape, err_ocpus, learned)

    def _mark_partial(current_meta: dict, _limits_after: dict) -> dict:
        # Self-heal learns single facts from create errors — it never proves
        # a full shape sweep. When a learned fact CHANGED the stored table,
        # the shape is recorded as PARTIAL — even if a probe sweep had marked
        # it verified: the API just contradicted the stored data, which is
        # proof the verification is STALE (platform change). Keeps the
        # table's provenance honest for anyone auditing where values came from.
        if not state["changed"]:
            return {}
        verification = dict(current_meta.get("verification") or {})
        was_verified = (verification.get(shape) or {}).get("status") == "verified"
        verification[shape] = {"status": "partial",
                               "date": time.strftime("%Y-%m-%d"),
                               "source": ("self-heal (stale verified table)"
                                          if was_verified else "self-heal")}
        return {"verification": verification}

    limits = update_shape_limits(_apply, meta_updates=_mark_partial)
    if state["changed"]:
        lines.append(f"  [shape-learn] {shape}: learned {learned} from "
                     f"create-API 400; limits file updated")

    mutated = False
    for r_key, s_key, c_key, role in (
            ("driverConfig", "driverShape", "driverShapeConfig", "driver"),
            ("workerConfig", "workerShape", "workerShapeConfig", "worker")):
        if payload[r_key][s_key] != shape:
            continue  # learned facts are shape-specific
        c = payload[r_key][c_key]
        before = (c["ocpus"], c["memoryInGBs"])

        # 1) Direct application of the learned facts (partial-table safe).
        if "ocpus_valid" in learned and c["ocpus"] not in learned["ocpus_valid"]:
            c["ocpus"] = _snap_up(c["ocpus"], learned["ocpus_valid"])
        if ("memory_valid_values" in learned
                and before[0] == err_ocpus and c["ocpus"] == err_ocpus
                and c["memoryInGBs"] not in learned["memory_valid_values"]):
            # The memory list is specific to the OCPU count the error was
            # about — only apply it to a role still at that count.
            c["memoryInGBs"] = _snap_up(c["memoryInGBs"],
                                        learned["memory_valid_values"])
        if "memory_gb_min" in learned and c["memoryInGBs"] < learned["memory_gb_min"]:
            c["memoryInGBs"] = learned["memory_gb_min"]
        if "memory_gb_max" in learned and c["memoryInGBs"] > learned["memory_gb_max"]:
            # Same invariant as normalize_shape_config: never hand the
            # workload less than it asked for — fail instead of clamping.
            raise ShapeCapacityError(
                f"{role}: {c['memoryInGBs']:g} GB exceeds the API-stated "
                f"maximum of {learned['memory_gb_max']} GB for '{shape}'")

        # 2) Best-effort full normalization on top, now that the persisted
        # table may cover this shape — harmonizes with any previously
        # learned facts. Shapes still lacking a usable table keep the
        # directly-applied values from step 1.
        try:
            norm = normalize_shape_config(
                c["ocpus"], c["memoryInGBs"], shape=shape,
                gpus=c.get("gpus", 0), limits=limits)
            c["ocpus"], c["memoryInGBs"] = norm.ocpus, norm.memory_gb
        except ShapeCapacityError:
            raise  # impossible request — never downgrade silently
        except ShapeMappingError:
            pass

        if (c["ocpus"], c["memoryInGBs"]) != before:
            mutated = True
            lines.append(
                f"  [shape-learn] {role}: {before[0]} OCPU / {before[1]} GB "
                f"-> {c['ocpus']} OCPU / {c['memoryInGBs']} GB")

    if not mutated:
        return []  # identical payload -> identical 400; caller must re-raise
    return lines


def convert_dbx_cluster(
    new_cluster: Dict[str, Any],
    cluster_display_name: str,
    prefix: str = "ai_tool",
    extra_libraries: Optional[List[Dict[str, Any]]] = None,
    cfg=None,
) -> ClusterConversion:
    """Build the AIDP cluster create payload from a DBX new_cluster spec.

    `cfg`: optional aidp_config namespace. When provided, its `spark_config`
    and `autoscale_config` blocks provide the baseline that gets baked into the
    new cluster's spark_conf (customer's DBX spark_conf still overlays with
    auto-heal — see the merge block below), and `secrets.mlflow_tracking_uri`
    is used as the MLFLOW_TRACKING_URI cluster env var when the operator env is
    unset. Callers can omit `cfg` for legacy/test paths — behavior degrades to
    "no tool-side spark defaults, no MLflow env var injected."
    """
    autoscale = new_cluster.get("autoscale") or {}
    num_workers = new_cluster.get("num_workers")
    min_workers = autoscale.get("min_workers")
    max_workers = autoscale.get("max_workers") or num_workers or 1
    if not min_workers:
        min_workers = max(1, (max_workers or 1) // 10)

    driver_type = new_cluster.get("driver_node_type_id") or new_cluster.get("node_type_id")
    worker_type = new_cluster.get("node_type_id")

    # Resolve driver + worker shapes via the DBX-node catalog. Each resolution
    # preserves arch family (ARM/Intel/AMD/GPU), exact vCPU/memory when the DBX
    # type is catalogued, and produces a log line describing the decision for
    # the operator to review before running the migrated workflow.
    driver_res = resolve_shape(driver_type)
    worker_res = resolve_shape(worker_type)
    # AIDP requires driver and worker to share ONE shape name. resolve_shape
    # maps each role independently, so a capacity override on one role (e.g.
    # a big Intel driver forced to amd.generic while a small Intel worker
    # stays intel.generic) yields a mismatched pair AIDP rejects at create.
    # Snap both onto a common shape before building the payload.
    driver_res, worker_res, harmonize_lines = harmonize_shape_family(
        driver_res, worker_res)
    shape_log_lines = [
        format_mapping_log_line("driver", driver_res),
        format_mapping_log_line("worker", worker_res),
        *harmonize_lines,
        f"[worker-count] DBX num_workers={new_cluster.get('num_workers')}, "
        f"autoscale={new_cluster.get('autoscale')} -> AIDP autoscale "
        f"[min={min_workers}, max={max_workers}]",
    ]

    name = sanitizer.sanitize_cluster_name(cluster_display_name, prefix)

    spark_conf, dropped_conf, translated_conf = _filter_spark_dict(
        new_cluster.get("spark_conf"), is_env=False)
    spark_env, dropped_env, _ = _filter_spark_dict(
        new_cluster.get("spark_env_vars"), is_env=True)

    # ─── Recommended-spark-config merge + auto-heal ────────────────────────
    # Two-tier rule per baseline key (baseline = cfg.spark_config + cfg.autoscale_config):
    #   (1) Missing on customer's DBX cluster       → use baseline value
    #   (2) Present & VALID for the expected type   → keep customer value verbatim
    #                                                 (customer knows their workload)
    #   (3) Present but MALFORMED                   → try light cleanup (strip
    #       whitespace + leading '='); if cleaned value passes validation, use
    #       cleaned; if still invalid, use baseline value and record the
    #       override in `translated_conf` (surfaces in operator log).
    #
    # Motivation — a pilot-customer AutoRefit export 2026-07-25 shipped
    #     spark.sql.execution.arrow.maxRecordsPerBatch = "= 128"
    # (literal string with leading "= "). Spark can't parse that as int at
    # runtime; the cluster spec passed dbx_export verbatim because our filter
    # doesn't inspect values against expected types. Rather than mask the
    # corruption or crash the migration, we clean the value if we can and fall
    # back to a baseline value if we can't.
    #
    # SCOPE: auto-heal only fires for keys in the baseline. For non-baseline
    # keys we don't know the expected value type, so we can't validate — they
    # pass through verbatim (existing behavior).
    for _k, _rec_v in recommended_spark_config(cfg).items():
        # Case (1): customer didn't set → baseline default fills in.
        if _k not in spark_conf:
            spark_conf[_k] = _rec_v
            continue

        _expected_type = _infer_recommended_value_type(_rec_v)
        _customer_v = spark_conf[_k]

        # String type has no format we can validate against — customer
        # value wins verbatim regardless of what it looks like.
        if _expected_type == "string":
            continue

        # Try the customer value AS-IS first — preserves any legit tuning
        # including case variants (e.g. "TRUE" for a bool key stays "TRUE"
        # because some downstream parsers may care; we don't normalize).
        if _matches_expected_type(_customer_v, _expected_type):
            continue

        # Value failed validation. Attempt narrow cleanup: strip whitespace
        # and leading "=" (the specific corruption pattern observed).
        _cleaned = _cleanup_light_value(_customer_v)
        if _cleaned != str(_customer_v) and _matches_expected_type(_cleaned, _expected_type):
            translated_conf.append((
                _k, _customer_v,
                f"malformed {_expected_type} value {_customer_v!r} auto-cleaned → {_cleaned!r}",
            ))
            spark_conf[_k] = _cleaned
            continue

        # Cleanup didn't recover a valid value. Drop the customer's bad
        # value and fall back to baseline. Recorded prominently so the
        # operator sees the correction wasn't invisible.
        translated_conf.append((
            _k, _customer_v,
            f"unfixable {_expected_type} value {_customer_v!r} replaced with "
            f"baseline {_rec_v!r}",
        ))
        spark_conf[_k] = _rec_v
    # ─── END: recommended-spark-config merge + auto-heal ──────────────────

    # ─── Recommended cluster env vars (e.g. MLFLOW_TRACKING_URI) ───────────
    # Force operator-provided env vars onto the new cluster's sparkEnvVariables,
    # OVERRIDING any inherited DBX value (e.g. point MLflow at the OCI tracking
    # server). No-op when none are set in the environment (no regression). This
    # is a second, independent guard alongside the in-code mlflow.set_tracking_uri().
    # The VALUE is intentionally not logged (it may carry a credential).
    for _ek, _ev in recommended_env_vars(cfg).items():
        _prev = spark_env.get(_ek)
        if _prev != _ev:
            shape_log_lines.append(
                f"  [cluster-env] {_ek} set from operator environment"
                + (" (overriding DBX value)" if _prev is not None else ""))
        spark_env[_ek] = _ev
    # ─── END: recommended cluster env vars ─────────────────────────────────

    # Worker counts vs AIDP's live-verified bound (maxWorkerCount <= 2000,
    # per the API's own 400 message). Same test as per-node OCPU/memory —
    # "can delivering less silently break the workload?" — but the answer
    # differs by field:
    #   - minWorkerCount is a GUARANTEE (a provision): a floor above 2000
    #     cannot be satisfied -> fail, never quietly run smaller.
    #   - a FIXED size (DBX num_workers, no autoscale) is a steady-state
    #     request: >2000 cannot be satisfied -> fail.
    #   - an AUTOSCALE CEILING is an allowance, not a provision: capping it
    #     at 2000 changes nothing until the job tries to scale past it, and
    #     then it only degrades throughput (per-task memory is untouched)
    #     -> clamp and log.
    #   - min > max as given is invalid input (DBX itself rejects it at
    #     creation; only corrupt/hand-edited specs) -> fail.
    min_workers = int(min_workers)
    max_workers = int(max_workers)
    if min_workers > MAX_WORKER_COUNT:
        raise ShapeCapacityError(
            f"minWorkerCount {min_workers} exceeds AIDP's maximum of "
            f"{MAX_WORKER_COUNT} workers — the spec's guaranteed floor "
            f"cannot be satisfied; lower min_workers or split the workload")
    if max_workers > MAX_WORKER_COUNT:
        if not autoscale:
            raise ShapeCapacityError(
                f"num_workers {max_workers} exceeds AIDP's maximum of "
                f"{MAX_WORKER_COUNT} workers — a fixed-size cluster this "
                f"large cannot be satisfied; reduce it or use autoscale")
        shape_log_lines.append(
            f"  [shape-normalize] autoscale maxWorkerCount {max_workers} -> "
            f"{MAX_WORKER_COUNT} (AIDP maximum; ceiling is an allowance, "
            f"not a provision)")
        max_workers = MAX_WORKER_COUNT
    if min_workers > max_workers:
        raise ShapeCapacityError(
            f"min_workers {min_workers} > max_workers {max_workers} is an "
            f"invalid autoscale range (DBX itself rejects it) — fix the "
            f"cluster spec")

    # autoTerminationMinutes policy on AIDP (2026-07-31):
    #   DBX 0 or missing → AIDP minimum (15 min). NEVER pass "always on".
    #   DBX in [1, 14]   → clamp UP to AIDP minimum (15 min).
    #   DBX in [15, 44640] → passed through.
    #   DBX > 44640      → clamp DOWN to AIDP maximum.
    # Rationale: DBX-side "always on" (0) usually reflects an interactive DBX
    # cluster left running for convenience — that intent does NOT translate to
    # exported AIDP clusters (cost hygiene). Operators can raise the value
    # deliberately in AIDP if a specific export truly needs longer sessions.
    # AIDP effective range is [15, 44640] (live-verified business rule —
    # the schema claims [5, 59940] but a stricter layer rejects outside
    # 15..44640 with "Please set an idle timeout between 15 minutes and
    # 44640 minutes only").
    _dbx_att = new_cluster.get("autotermination_minutes")
    if not _dbx_att:  # 0 or None (missing) -> force AIDP minimum
        auto_termination = AUTO_TERMINATION_MIN
        _dbx_att_display = "0 (never)" if _dbx_att == 0 else "not set"
        shape_log_lines.append(
            f"  [shape-normalize] DBX autotermination_minutes={_dbx_att_display} "
            f"-> forcing AIDP minimum {AUTO_TERMINATION_MIN} min "
            f"(policy: never export always-on clusters)")
    else:
        auto_termination = min(AUTO_TERMINATION_MAX, max(AUTO_TERMINATION_MIN, int(_dbx_att)))
        if auto_termination != int(_dbx_att):
            shape_log_lines.append(
                f"  [shape-normalize] autoTerminationMinutes {_dbx_att} -> "
                f"{auto_termination} (AIDP allows [{AUTO_TERMINATION_MIN}, "
                f"{AUTO_TERMINATION_MAX}])")

    # One table read per conversion: driver and worker snap against the same
    # snapshot ({} -> None keeps normalize's missing-table pass-through).
    _limits_snapshot = load_shape_limits(required=False) or None

    aidp_payload: Dict[str, Any] = {
        "displayName": name,
        "description": f"Exported from DBX cluster: {cluster_display_name}",
        "nodeType": driver_res.aidp_shape,
        "driverConfig": {
            "driverNodeType": driver_res.aidp_shape,
            "driverShape": driver_res.aidp_shape,
            "driverShapeConfig": _shape_config(driver_res, shape_log_lines,
                                               "driver", _limits_snapshot),
        },
        "workerConfig": {
            "workerShape": worker_res.aidp_shape,
            "workerShapeConfig": _shape_config(worker_res, shape_log_lines,
                                               "worker", _limits_snapshot),
            "minWorkerCount": min_workers,
            "maxWorkerCount": max_workers,
        },
        "clusterRuntimeConfig": {
            "type": "SPARK",
            "sparkVersion": AIDP_SPARK_VERSION,
            "sparkAdvancedConfigurations": spark_conf,
            "sparkEnvVariables": spark_env,
            "initScripts": [],  # filled in later, after init scripts are uploaded
        },
    }

    # After the policy above, auto_termination is always a value in
    # [AUTO_TERMINATION_MIN, AUTO_TERMINATION_MAX] — always send it.
    aidp_payload["autoTerminationMinutes"] = auto_termination

    # Collect init script refs for later upload/translation.
    init_refs: List[str] = []
    for item in new_cluster.get("init_scripts", []) or []:
        for k in ("workspace", "dbfs", "s3", "abfss", "gcs", "file"):
            ref = item.get(k)
            if ref and isinstance(ref, dict) and ref.get("destination"):
                init_refs.append(ref["destination"])

    library_refs = list(new_cluster.get("libraries", []) or [])
    if extra_libraries:
        library_refs.extend(extra_libraries)

    return ClusterConversion(
        aidp_payload=aidp_payload,
        init_script_refs=init_refs,
        library_refs=library_refs,
        dropped_conf=dropped_conf,
        dropped_env=dropped_env,
        shape_mapping_log_lines=shape_log_lines,
        driver_shape_res=driver_res,
        worker_shape_res=worker_res,
        translated_conf=translated_conf,
    )


def attach_init_scripts(
    cluster_conv: ClusterConversion,
    dbx_uri_to_aidp_path: Dict[str, str],
) -> None:
    """After init scripts are uploaded under <target-base>/init_scripts/,
    rewrite the cluster payload's initScripts[] to point at the AIDP paths.
    Entries we couldn't fetch (external S3, missing) are skipped silently —
    they're recorded in the report.
    """
    items = []
    for uri in cluster_conv.init_script_refs:
        aidp = dbx_uri_to_aidp_path.get(uri)
        if aidp:
            items.append({"destination": aidp})
    cluster_conv.aidp_payload["clusterRuntimeConfig"]["initScripts"] = items


def build_library_install_items(
    library_refs: List[Dict[str, Any]],
    dbx_uri_to_aidp_path: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Translate DBX library entries to AIDP /clusters/{key}/libraries items.

    File-backed libs (jar/whl/egg/requirements) point at the AIDP workspace
    path where we uploaded them. PyPI / Maven entries pass through as remote
    install instructions (AIDP cluster has internet per feedback memory).
    """
    items: List[Dict[str, Any]] = []
    for lib in library_refs:
        if "jar" in lib:
            aidp = dbx_uri_to_aidp_path.get(lib["jar"])
            if aidp:
                items.append({"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": aidp})
        elif "whl" in lib:
            aidp = dbx_uri_to_aidp_path.get(lib["whl"])
            if aidp:
                items.append({"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": aidp})
        elif "egg" in lib:
            aidp = dbx_uri_to_aidp_path.get(lib["egg"])
            if aidp:
                items.append({"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": aidp})
        elif "requirements" in lib:
            aidp = dbx_uri_to_aidp_path.get(lib["requirements"])
            if aidp:
                items.append({"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": aidp})
        elif "pypi" in lib:
            pkg = lib["pypi"].get("package")
            if pkg:
                items.append({"operation": "INSTALL", "type": "PYPI", "package": pkg})
        elif "maven" in lib:
            coord = lib["maven"].get("coordinates")
            if coord:
                items.append({"operation": "INSTALL", "type": "MAVEN", "coordinates": coord})
    return items
