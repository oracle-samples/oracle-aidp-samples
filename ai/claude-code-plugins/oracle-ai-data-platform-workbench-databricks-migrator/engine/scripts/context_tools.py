#!/usr/bin/env python3
"""
Context Tools for Opus
=======================
Gathers context that helps Claude Opus make better migration decisions:
1. Catalog snapshot (schemas, tables, columns)
2. Dependent notebook content
3. Kernel state after execution
4. OCI path validation
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

import oci
import requests as http_requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _unwrap_aidp_text(raw_text: str) -> str:
    """AIDP wraps output in JSON: [{"type":"TEXT_PLAIN","value":"actual text"}].
    Handles concatenated arrays: [{"value":"a"}][{"value":"b"}].
    Extract the actual text value. Returns raw text if not wrapped."""
    if not raw_text:
        return ""

    # Try single JSON array first (strict=False allows control chars like \n in strings)
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

    # AIDP sometimes emits an empty display-output wrapper "[]" concatenated
    # ahead of plain stdout (e.g. "[]40121"). Strip leading empty arrays so
    # callers that parse the value (int(size), etc.) aren't tripped up.
    stripped = raw_text.lstrip()
    while stripped.startswith("[]"):
        stripped = stripped[2:].lstrip()
    if stripped != raw_text.lstrip():
        return stripped

    return raw_text

# AIDP config for log fetching — generic, no hardcoded customer/instance values.
# Set at runtime (job_migrate_from_workflow overrides these). Profile defaults to "DEFAULT".
AIDP_BASE = None
DATALAKE_OCID = None
WORKSPACE_ID = None
OCI_PROFILE = "DEFAULT"


# ─── Catalog Context ─────────────────────────────────────────────────

def load_catalog_snapshot() -> str:
    """Load catalog snapshot as a compact string for Opus context.
    With Opus 4.8 1M context, we can include the full catalog."""
    cache = os.path.join(PROJECT_DIR, "reports", "catalog_snapshot.json")
    if os.path.exists(cache):
        with open(cache) as f:
            catalog = json.load(f)
    else:
        return "(no catalog snapshot available)"

    # Full representation - Opus has 1M context, include everything
    parts = ["## Available Catalog on AIDP (full snapshot)"]
    parts.append("These are the schemas and tables that exist on AIDP right now.")
    parts.append("If a notebook references a table not in this list, it may not exist yet.\n")

    schemas = catalog.get("schemas", [])
    tables = catalog.get("tables", {})

    for schema in sorted(schemas):
        schema_tables = tables.get(schema, [])
        if isinstance(schema_tables, list) and schema_tables:
            parts.append(f"### {schema}")
            parts.append(", ".join(schema_tables))
            parts.append("")

    return "\n".join(parts)


def search_catalog(query: str) -> str:
    """Search the catalog for an exact table match. No fuzzy/substring matching."""
    cache = os.path.join(PROJECT_DIR, "reports", "catalog_snapshot.json")
    if not os.path.exists(cache):
        return "No catalog available"

    with open(cache) as f:
        catalog = json.load(f)

    query_lower = query.lower().strip()
    # Strip "default." prefix if provided (default catalog is implicit)
    if query_lower.startswith("default."):
        query_lower = query_lower[len("default."):]

    # Try exact match: "schema.table" or just "table"
    parts = query_lower.split(".")
    for schema, tables in catalog.get("tables", {}).items():
        if not isinstance(tables, list):
            continue
        for table in tables:
            full = f"{schema}.{table}".lower()
            # Exact match on schema.table
            if query_lower == full:
                return f"Table EXISTS: {schema}.{table}"
            # Exact match on table name alone
            if len(parts) == 1 and query_lower == table.lower():
                return f"Table EXISTS: {schema}.{table}"

    return f"Table NOT FOUND: '{query}' — this is definitive, do NOT retry with variations"


async def get_table_schema(session, table_name: str) -> str:
    """Get column names and types for a specific table."""
    from aidp_executor import format_outputs
    result = await session.execute(
        f"spark.sql('DESCRIBE TABLE {table_name}').show(100, truncate=False)",
        timeout=120
    )
    return format_outputs(result.get("outputs", []))


async def verify_table_schema(session, table_name: str) -> str:
    """Verify a table has actual schema (columns) — not just a hollow shell.

    On AIDP, tables can be registered in the catalog but have empty schema
    (0 columns) when the underlying data hasn't been synced. DESCRIBE returns
    an empty DataFrame. Every query against such tables fails silently.

    Confirmed: <schema>.<table> in a real migration (example).

    Returns:
        'EXISTS:<N>cols' if table has N columns (healthy),
        'EMPTY_SCHEMA' if table exists but DESCRIBE returns 0 columns (broken),
        'MISSING' if table does not exist,
        'ERROR:<msg>' on unexpected failure.
    """
    from aidp_executor import format_outputs
    code = f"""
try:
    cols = spark.sql("DESCRIBE TABLE {table_name}").collect()
    if len(cols) == 0:
        print("EMPTY_SCHEMA")
    else:
        print(f"EXISTS:{{len(cols)}}cols")
except Exception as e:
    err = str(e)
    if "Table or view not found" in err or "TABLE_OR_VIEW_NOT_FOUND" in err:
        print("MISSING")
    else:
        print(f"ERROR:{{err[:200]}}")
"""
    try:
        run = getattr(session, 'run_stateless', session.execute)
        result = await run(code, timeout=30)
        output = format_outputs(result.get("outputs", [])).strip()
        for line in output.splitlines():
            line = line.strip()
            if line.startswith("EXISTS:") or line in ("EMPTY_SCHEMA", "MISSING") or line.startswith("ERROR:"):
                return line
        return f"ERROR:unexpected output: {output[:100]}"
    except Exception as e:
        return f"ERROR:{str(e)[:200]}"


async def check_table_exists(session, table_name: str) -> bool:
    """Check if a table exists in the catalog."""
    from aidp_executor import format_outputs
    result = await session.execute(
        f"print(spark.catalog.tableExists('{table_name}'))",
        timeout=15
    )
    output = format_outputs(result.get("outputs", []))
    return "True" in output


async def build_catalog_cache(session) -> dict:
    """Build full catalog snapshot via executor."""
    from aidp_executor import format_outputs
    result = await session.execute("""
import json
catalog = {}
schemas = [row[0] for row in spark.sql('SHOW SCHEMAS').collect()]
catalog['schemas'] = schemas
catalog['tables'] = {}
for schema in schemas:
    try:
        tables = spark.sql(f'SHOW TABLES IN {schema}').collect()
        catalog['tables'][schema] = [t.tableName for t in tables[:50]]
    except:
        catalog['tables'][schema] = []
print(json.dumps(catalog))
""", timeout=120)
    output = format_outputs(result.get("outputs", []))
    try:
        catalog = json.loads(output)
        # Save locally
        cache_path = os.path.join(PROJECT_DIR, "reports", "catalog_snapshot.json")
        with open(cache_path, 'w') as f:
            json.dump(catalog, f, indent=2)
        return catalog
    except:
        return {}


# ─── Dependent Notebook Context ──────────────────────────────────────

# ─── Language-aware comment handling ─────────────────────────────────
# A cell's comment marker is language-specific: Python/R use '#', Scala/Java
# use '//', SQL uses '--'. Stripping the wrong marker (or all of them blindly)
# is incorrect. These helpers detect a cell's language and strip only that
# language's line comments. Shared so every scanner behaves identically.

_LINE_COMMENT_BY_LANG = {
    "python": "#", "r": "#",
    "scala": "//", "java": "//",
    "sql": "--",
}


def comment_prefixes_for_lang(lang: str):
    """Line-comment prefix(es) for a cell language. Unknown language → all known
    markers (safe fallback: a live %run/notebook.run never starts a line with a
    comment marker, so over-stripping can't drop a real dependency)."""
    marker = _LINE_COMMENT_BY_LANG.get((lang or "").lower())
    return (marker,) if marker else ("#", "//", "--")


def detect_cell_language(cell_source: str, default_lang: str = "") -> str:
    """Per-cell language: a leading %scala/%python/%sql/%r magic (or Databricks
    '# MAGIC %scala') overrides the notebook default."""
    first = next((l.strip() for l in cell_source.splitlines() if l.strip()), "")
    m = re.match(r'^(?:#\s*MAGIC\s+)?%(\w+)', first)
    if m:
        lang = m.group(1).lower()
        return {"py": "python", "python": "python", "scala": "scala",
                "sql": "sql", "r": "r"}.get(lang, lang)
    return (default_lang or "").lower()


def notebook_default_language(nb: dict) -> str:
    meta = nb.get("metadata", {}) or {}
    return (meta.get("language_info", {}).get("name")
            or meta.get("kernelspec", {}).get("language") or "").lower()


def strip_comment_lines(cell_source: str, lang: str) -> str:
    """Drop blank lines and whole-line comments for the cell's language."""
    prefixes = comment_prefixes_for_lang(lang)
    return "\n".join(l for l in cell_source.splitlines()
                     if l.strip() and not l.lstrip().startswith(prefixes))


def _scan_run_targets(active_source: str) -> List[str]:
    """Find %run / dbutils.notebook.run / oidlUtils.notebook.run targets in
    already-comment-stripped source."""
    deps = []
    for match in re.findall(r'%run\s+([^\s$]+)', active_source):
        clean = re.sub(r'\$\w+', '', match).strip()
        if clean and clean[0] in ('"', "'") and clean[-1] == clean[0]:
            clean = clean[1:-1]
        if clean:
            deps.append(clean)
    for match in re.findall(r'(?:dbutils|oidlUtils)\.notebook\.run\s*\(\s*["\']([^"\']+)["\']', active_source):
        deps.append(match)
    return deps


def extract_notebook_dependencies(nb_or_source) -> List[str]:
    """Extract %run / notebook.run dependency paths, excluding commented-out
    refs using LANGUAGE-SPECIFIC comment syntax.

    Accepts either a parsed notebook dict (preferred — per-cell language-aware,
    handles mixed %python/%scala notebooks) or a raw source string (legacy —
    falls back to a safe blanket strip of all comment markers).
    """
    if isinstance(nb_or_source, dict):
        default_lang = notebook_default_language(nb_or_source)
        deps = []
        for cell in nb_or_source.get("cells", []):
            if cell.get("cell_type") != "code":
                continue
            src = cell.get("source", "")
            src = "".join(src) if isinstance(src, list) else (src or "")
            lang = detect_cell_language(src, default_lang)
            deps.extend(_scan_run_targets(strip_comment_lines(src, lang)))
        return deps
    # Legacy: raw blob with no per-cell boundaries → blanket strip (lang="").
    return _scan_run_targets(strip_comment_lines(nb_or_source, ""))


def get_dependent_notebook_context(nb_content: str, all_downloaded: Dict[str, bytes]) -> str:
    """Get function signatures and key definitions from dependent notebooks."""
    try:
        nb = json.loads(nb_content)
    except:
        return ""

    # Pass the parsed notebook (not a concatenated blob) so dependency scanning
    # is per-cell language-aware (correct comment syntax per cell language).
    deps = extract_notebook_dependencies(nb)
    if not deps:
        return ""

    parts = ["## Dependent Notebook Definitions"]
    for dep_path in deps:
        # Normalize path
        normalized = dep_path.lstrip("/")
        if normalized.startswith("Workspace/"):
            normalized = normalized[len("Workspace/"):]
        if not normalized.endswith(".ipynb"):
            normalized += ".ipynb"

        # Check if we have the content
        content = all_downloaded.get(normalized)
        if not content:
            # Try alternate paths
            alt = normalized.replace(" ", "_").replace("(", "").replace(")", "")
            content = all_downloaded.get(alt)

        if content:
            try:
                dep_nb = json.loads(content.decode('utf-8', errors='replace'))
                # With 1M context, include full source code of dependent notebooks
                code_parts = []
                for cell in dep_nb.get("cells", []):
                    if cell.get("cell_type") != "code":
                        continue
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        code_parts.append(src)

                full_dep_source = "\n\n".join(code_parts)
                # Cap at 20K chars per dependent notebook (still generous with 1M context)
                if len(full_dep_source) > 20000:
                    full_dep_source = full_dep_source[:20000] + "\n# ... (truncated)"

                parts.append(f"\n### {dep_path}")
                parts.append("```python")
                parts.append(full_dep_source)
                parts.append("```")
            except:
                pass
        else:
            parts.append(f"\n### {dep_path} (not available)")

    return "\n".join(parts) if len(parts) > 1 else ""


# ─── Kernel State ────────────────────────────────────────────────────

async def get_kernel_state(session) -> str:
    """Get current kernel state - defined variables, their types, registered tables."""
    from aidp_executor import format_outputs

    result = await session.execute("""
import json
state = {}

# User-defined variables (not builtins)
skip = {'__builtins__', '__name__', '__doc__', '__package__', '__loader__', '__spec__',
        'In', 'Out', 'get_ipython', 'exit', 'quit', 'open', 'spark', 'sc', 'sqlContext',
        'sql', 'table', 'sys', 'os', 'json', 'display', 'dbutils', 'displayHTML',
        'translate_path', 'base64', 'builtins'}
user_vars = {}
for name, val in list(globals().items()):  # list() to avoid dict mutation during iteration
    if name.startswith('_') or name in skip:
        continue
    try:
        user_vars[name] = type(val).__name__
    except:
        pass
state['variables'] = dict(list(user_vars.items())[:30])

# Registered temp views
try:
    tables = [t.name for t in spark.catalog.listTables() if t.isTemporary]
    state['temp_views'] = tables[:20]
except:
    state['temp_views'] = []

print(json.dumps(state))
""", timeout=15)

    output = format_outputs(result.get("outputs", []))
    try:
        state = json.loads(output)
        parts = ["## Current Kernel State"]
        if state.get("variables"):
            parts.append("Variables: " + ", ".join(f"{k}:{v}" for k, v in state["variables"].items()))
        if state.get("temp_views"):
            parts.append("Temp views: " + ", ".join(state["temp_views"]))
        return "\n".join(parts)
    except:
        return ""


# ─── S3-to-OCI Bucket Mapping ────────────────────────────────────────

_BUCKET_MAPPING = None
_BUCKET_NS_INDEX: Dict[str, str] = {}  # {oci_bucket: namespace} from JSON


def _normalize_bucket_name(name: str) -> str:
    """Strip common prefixes/suffixes for fuzzy matching.
    oci-customer-data-lake -> customer-data-lake, bucket-oci -> bucket"""
    n = name.lower().strip()
    if n.startswith("oci-"):
        n = n[4:]
    if n.endswith("-oci"):
        n = n[:-4]
    return n


def _load_bucket_ns_json() -> Dict[str, List[Dict[str, str]]]:
    """Load oci_bucket_tenancy_mapping.json and build mapping in the same format
    as the CSV loader: {bucket_name: [{oci_bucket, oci_namespace}]}.
    Each OCI bucket maps to itself so suggest_oci_path can find it."""
    json_path = os.path.join(PROJECT_DIR, "config", "oci_bucket_tenancy_mapping.json")
    if not os.path.exists(json_path):
        return {}
    with open(json_path) as f:
        index = json.load(f)
    global _BUCKET_NS_INDEX
    _BUCKET_NS_INDEX = index
    # Build mapping keyed by bucket name (same key as oci_bucket for direct match)
    mapping = {}
    for bucket, ns in index.items():
        mapping[bucket] = [{"oci_bucket": bucket, "oci_namespace": ns}]
    return mapping


def find_oci_bucket(s3_name: str) -> List[Dict[str, str]]:
    """Find OCI bucket(s) matching an S3 bucket name using the JSON index.
    Tries: exact match, oci- prefix, -oci suffix, normalized fuzzy match.
    Returns: [{oci_bucket, oci_namespace}, ...] sorted by match quality."""
    # Ensure index is loaded
    if not _BUCKET_NS_INDEX:
        load_bucket_mapping()
    index = _BUCKET_NS_INDEX
    if not index:
        return []

    # 1. Exact match
    if s3_name in index:
        return [{"oci_bucket": s3_name, "oci_namespace": index[s3_name]}]

    # 2. Try common OCI naming variants
    for candidate in [f"oci-{s3_name}", f"oci-customer-{s3_name}", f"{s3_name}-oci"]:
        if candidate in index:
            return [{"oci_bucket": candidate, "oci_namespace": index[candidate]}]

    # 3. Normalized fuzzy match
    s3_norm = _normalize_bucket_name(s3_name)
    if not s3_norm or len(s3_norm) <= 5:
        return []

    results = []
    for oci_bucket, ns in index.items():
        oci_norm = _normalize_bucket_name(oci_bucket)
        if not oci_norm:
            continue
        if s3_norm == oci_norm:
            results.append((0, oci_bucket, ns))
        elif s3_norm in oci_norm or oci_norm in s3_norm:
            diff = abs(len(s3_norm) - len(oci_norm))
            results.append((diff, oci_bucket, ns))

    results.sort(key=lambda r: r[0])
    return [{"oci_bucket": b, "oci_namespace": ns} for _, b, ns in results[:5]]


def load_bucket_mapping(csv_path: str = None) -> Dict[str, List[Dict[str, str]]]:
    """Load the OCI bucket→tenancy mapping (the single source of truth for
    namespaces) from config/oci_bucket_tenancy_mapping.json.
    Returns: {bucket: [{oci_bucket, oci_namespace}, ...]} and populates
    _BUCKET_NS_INDEX = {oci_bucket: namespace}.

    The legacy reports/s3_to_oci_bucket_mapping.csv no longer exists in the
    project and is NOT used. The `csv_path` arg is accepted for backward
    compatibility but ignored.
    """
    global _BUCKET_MAPPING
    if _BUCKET_MAPPING is not None:
        return _BUCKET_MAPPING
    _BUCKET_MAPPING = _load_bucket_ns_json()
    return _BUCKET_MAPPING


def get_bucket_mapping_context() -> str:
    """Build a compact string of S3-to-OCI bucket mappings for Opus context.
    Groups by namespace for readability."""
    mapping = load_bucket_mapping()
    if not mapping:
        return ""

    # Group by namespace
    by_ns: Dict[str, List[str]] = {}
    for s3, entries in mapping.items():
        for e in entries:
            ns = e["oci_namespace"]
            oci = e["oci_bucket"]
            by_ns.setdefault(ns, []).append(f"{s3} -> {oci}")

    parts = ["## S3-to-OCI Bucket Mapping (from mapping CSV)"]
    parts.append("Use these to translate S3/Databricks paths to correct OCI paths.\n")

    # Show all namespaces — Opus 4.8 has 1M context, no need to truncate.
    # Priority namespaces first, then the rest.
    priority_ns = ["<DATALAKE_NAMESPACE>", "<NFS_NAMESPACE>", "<COLLECTIONS_NAMESPACE>", "<WORKSPACE_NAMESPACE>"]
    shown = set()
    for ns in priority_ns:
        if ns in by_ns:
            parts.append(f"### Namespace: {ns}")
            for e in sorted(by_ns[ns]):
                parts.append(f"  {e}")
            parts.append("")
            shown.add(ns)

    for ns in sorted(by_ns):
        if ns not in shown:
            parts.append(f"### Namespace: {ns}")
            for e in sorted(by_ns[ns]):
                parts.append(f"  {e}")
            parts.append("")

    return "\n".join(parts)


def suggest_oci_path(s3_or_oci_path: str) -> List[str]:
    """Given an S3 or OCI path, suggest correct OCI path(s) using the mapping.
    Returns list of suggested oci:// paths."""
    mapping = load_bucket_mapping()
    if not mapping:
        return []

    # Extract bucket name from various path formats
    bucket_name = None
    sub_path = ""

    if "oci://" in s3_or_oci_path:
        # oci://bucket@namespace/sub/path
        # The mapping is the authoritative source for the namespace. If the
        # bucket's namespace already matches the mapping, the path is correct —
        # return as-is. Otherwise fall through and suggest the MAPPING's
        # namespace (exported notebooks often carry a wrong namespace, e.g.
        # oci-customer-feature-bucket tagged @<DATALAKE_NAMESPACE> when the
        # mapping + manual gold say @<WORKSPACE_NAMESPACE>).
        m = re.match(r'oci://([^@]+)@([^/]+)(/.*)?', s3_or_oci_path)
        if m:
            existing_bucket = m.group(1)
            existing_ns = m.group(2)
            sub_path = m.group(3) or "/"
            if _BUCKET_NS_INDEX and existing_bucket in _BUCKET_NS_INDEX:
                if _BUCKET_NS_INDEX[existing_bucket] == existing_ns:
                    return [s3_or_oci_path]
            bucket_name = existing_bucket
    elif "s3://" in s3_or_oci_path or "s3a://" in s3_or_oci_path:
        # s3://bucket/sub/path
        m = re.match(r's3a?://([^/]+)(/.*)?', s3_or_oci_path)
        if m:
            bucket_name = m.group(1)
            sub_path = m.group(2) or "/"
    elif s3_or_oci_path.startswith("dbfs:/"):
        # dbfs:/mnt/bucket/sub/path -> extract bucket name
        m = re.match(r'dbfs:/(?:mnt/)?([^/]+)(/.*)?', s3_or_oci_path)
        if m:
            bucket_name = m.group(1)
            sub_path = m.group(2) or "/"
    elif s3_or_oci_path.startswith("/mnt/"):
        m = re.match(r'/mnt/([^/]+)(/.*)?', s3_or_oci_path)
        if m:
            bucket_name = m.group(1)
            sub_path = m.group(2) or "/"

    if not bucket_name:
        return []

    suggestions = []
    # Strip common prefixes for matching
    clean_bucket = bucket_name.replace("oci-", "").replace("customer-", "")

    for s3, entries in mapping.items():
        # Direct match
        if s3 == bucket_name or s3 == bucket_name.replace("oci-", ""):
            for e in entries:
                suggestions.append(f"oci://{e['oci_bucket']}@{e['oci_namespace']}{sub_path}")
        # Fuzzy match on bucket name
        elif clean_bucket in s3 or s3.replace("customer-", "") == clean_bucket:
            for e in entries:
                suggestions.append(f"oci://{e['oci_bucket']}@{e['oci_namespace']}{sub_path}")

    # Fallback: try fuzzy matching via JSON index if no suggestions found
    if not suggestions and _BUCKET_NS_INDEX:
        matches = find_oci_bucket(bucket_name)
        for m in matches:
            suggestions.append(f"oci://{m['oci_bucket']}@{m['oci_namespace']}{sub_path}")

    return list(dict.fromkeys(suggestions))  # deduplicate preserving order


# ─── OCI Path Validation ─────────────────────────────────────────────

async def check_oci_path(session, path: str) -> bool:
    """Check if an OCI path exists. Uses stateless pool if available."""
    from aidp_executor import format_outputs
    run = getattr(session, 'run_stateless', session.execute)
    result = await run(f"""
try:
    jvm = spark._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI("{path}")
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    p = jvm.org.apache.hadoop.fs.Path("{path}")
    print(fs.exists(p))
except Exception as e:
    print(f"ERROR: {{str(e)[:200]}}")
""", timeout=15)
    output = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))
    return "true" in output.lower()


async def verify_and_suggest_paths(session, source: str) -> str:
    """Extract storage paths from cell source, verify on cluster, suggest alternatives.
    Returns a context string with path status and suggestions for Opus."""
    paths = extract_storage_paths(source)
    if not paths:
        return ""

    parts = ["## Storage Path Verification (checked on AIDP cluster)"]

    for p in paths[:8]:  # limit to 8 paths per cell
        # Check if path exists on cluster
        try:
            exists = await check_oci_path(session, p)
        except Exception:
            exists = None

        # Get suggestions from mapping
        suggestions = suggest_oci_path(p)

        if exists:
            parts.append(f"  {p}: EXISTS")
        elif exists is False:
            parts.append(f"  {p}: NOT FOUND")
            if suggestions:
                parts.append(f"    -> Suggested alternatives from mapping:")
                for s in suggestions[:3]:
                    # Check if suggestion exists
                    try:
                        s_exists = await check_oci_path(session, s)
                        status = "EXISTS" if s_exists else "NOT VERIFIED"
                    except Exception:
                        status = "NOT VERIFIED"
                    parts.append(f"       {s} [{status}]")
        else:
            parts.append(f"  {p}: CHECK FAILED")
            if suggestions:
                parts.append(f"    -> Mapping suggests: {suggestions[0]}")

    return "\n".join(parts) if len(parts) > 1 else ""


async def explore_oci_path(session, path: str, max_depth: int = 2) -> str:
    """Explore an OCI path on the AIDP cluster - check existence, list contents,
    handle wildcards/globs. Returns a human-readable report.

    This runs ON the cluster using Spark's Hadoop filesystem (BmcFilesystem is
    configured at cluster level with API key auth, supporting cross-tenancy access).
    """
    from aidp_executor import format_outputs

    # Escape the path for embedding in Python string
    safe_path = path.replace("'", "\\'")

    code = f'''
import json, os, glob as globmod

def explore_path(path, max_depth={max_depth}):
    """Explore an OCI/HDFS path on the cluster."""
    result = {{"path": path, "exists": False}}

    # /Volumes/ and /Workspace/ are local FUSE mounts — use os directly.
    # Catch misformed paths like oci://bucket@ns/Volumes/... where Opus
    # incorrectly wrapped a local FUSE path in an OCI URI.
    for _fp in ("/Volumes/", "/Workspace/", "/tmp/"):
        _ix = path.find(_fp)
        if _ix > 0:
            path = path[_ix:]
            result["path"] = path
            break
    if path.startswith("/Volumes/") or path.startswith("/Workspace/") or path.startswith("/tmp/"):
        try:
            if "*" in path or "?" in path:
                matches = sorted(globmod.glob(path))
                result["exists"] = len(matches) > 0
                result["type"] = "glob"
                result["match_count"] = len(matches)
                result["sample_matches"] = matches[:10]
            elif os.path.exists(path):
                result["exists"] = True
                if os.path.isdir(path):
                    result["type"] = "directory"
                    children = []
                    for i, name in enumerate(sorted(os.listdir(path))):
                        if i >= 20:
                            result["truncated"] = True
                            break
                        fp = os.path.join(path, name)
                        info = {{"name": name, "is_dir": os.path.isdir(fp)}}
                        if not os.path.isdir(fp):
                            info["size"] = os.path.getsize(fp)
                        children.append(info)
                    result["children"] = children
                else:
                    result["type"] = "file"
                    result["size"] = os.path.getsize(path)
        except Exception as e:
            result["error"] = str(e)[:300]
        return result

    try:
        jvm = spark._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

        # Handle glob patterns - use globStatus
        if "*" in path or "?" in path or "{{" in path:
            uri_base = path.split("*")[0].rsplit("/", 1)[0] + "/"
            uri = jvm.java.net.URI(uri_base)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            p = jvm.org.apache.hadoop.fs.Path(path)
            statuses = fs.globStatus(p)
            if statuses and len(statuses) > 0:
                result["exists"] = True
                result["type"] = "glob"
                result["match_count"] = len(statuses)
                result["sample_matches"] = []
                for i, s in enumerate(statuses):
                    if i >= 10:
                        break
                    result["sample_matches"].append(str(s.getPath()))
            else:
                result["exists"] = False
                result["type"] = "glob"
                result["match_count"] = 0
        else:
            uri = jvm.java.net.URI(path)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            p = jvm.org.apache.hadoop.fs.Path(path)
            exists = fs.exists(p)
            result["exists"] = exists

            if exists:
                status = fs.getFileStatus(p)
                if status.isDirectory():
                    result["type"] = "directory"
                    children = fs.listStatus(p)
                    result["children"] = []
                    for i, child in enumerate(children):
                        if i >= 20:
                            result["truncated"] = True
                            break
                        child_name = str(child.getPath()).split("/")[-1]
                        if not child_name:
                            child_name = str(child.getPath()).split("/")[-2]
                        child_info = {{"name": child_name, "is_dir": child.isDirectory()}}
                        if not child.isDirectory():
                            child_info["size"] = child.getLen()
                        result["children"].append(child_info)
                else:
                    result["type"] = "file"
                    result["size"] = status.getLen()
    except Exception as e:
        result["error"] = str(e)[:300]

    return result

result = explore_path('{safe_path}')
print(json.dumps(result))
'''

    run = getattr(session, 'run_stateless', session.execute)
    exec_result = await run(code, timeout=90)
    output = format_outputs(exec_result.get("outputs", []))
    # AIDP wraps output in JSON: [{"type":"TEXT_PLAIN","value":"text"}]
    output = _unwrap_aidp_text(output)

    try:
        data = json.loads(output)
    except Exception:
        return f"Path exploration failed for {path}: {output[:200]}"

    # Format as readable report
    lines = [f"Path: {path}"]
    if data.get("error"):
        lines.append(f"  Error: {data['error'][:200]}")
    elif not data.get("exists"):
        lines.append("  Status: NOT FOUND")
    else:
        ptype = data.get("type", "unknown")
        if ptype == "glob":
            lines.append(f"  Status: EXISTS (glob match, {data.get('match_count', 0)} matches)")
            for m in data.get("sample_matches", [])[:5]:
                lines.append(f"    {m}")
        elif ptype == "directory":
            children = data.get("children", [])
            lines.append(f"  Status: EXISTS (directory, {len(children)}{'+ ' if data.get('truncated') else ' '}items)")
            for c in children[:10]:
                prefix = "d" if c.get("is_dir") else "f"
                size = f" ({c['size']} bytes)" if "size" in c else ""
                lines.append(f"    [{prefix}] {c['name']}{size}")
        elif ptype == "file":
            lines.append(f"  Status: EXISTS (file, {data.get('size', '?')} bytes)")

    return "\n".join(lines)


def extract_storage_paths(source: str) -> List[str]:
    """Extract oci://, s3://, dbfs:/ paths from cell source."""
    paths = []
    patterns = [
        r'oci://[\w\-\.]+@[\w]+/[\w\-\./]+',
        r's3[a]?://[\w\-\.]+/[\w\-\./]+',
        r'dbfs:/[\w\-\./]+',
    ]
    for pattern in patterns:
        paths.extend(re.findall(pattern, source))
    return list(set(paths))


# ─── Spark Log Fetcher ────────────────────────────────────────────────

_LOG_SIGNER = None
def _log_signer():
    global _LOG_SIGNER
    if not _LOG_SIGNER:
        from aidp_executor import get_oci_signer
        _, _LOG_SIGNER = get_oci_signer(OCI_PROFILE)
    return _LOG_SIGNER


def fetch_recent_spark_errors(cluster_id: str, minutes_back: int = 5) -> str:
    """Fetch recent Spark driver AND worker error logs from AIDP.
    Returns a compact string of unique errors for Opus context."""
    all_errors = set()

    for log_source, subject in [("driver", "spark-driver"), ("executor", "spark-executor-1")]:
        try:
            url = (f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}"
                   f"/clusters/{cluster_id}/actions/searchLogs")

            now = datetime.now(timezone.utc)
            body = {
                "timeBegin": (now - timedelta(minutes=minutes_back)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "timeEnd": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "logContentTypeContains": log_source,
                "subjectContains": subject,
                "logLevel": "error",
            }

            resp = http_requests.post(url, json=body, auth=_log_signer(),
                                      headers={"Content-Type": "application/json"}, timeout=10)
            if resp.status_code != 200:
                continue

            logs = resp.json()
            items = logs.get("items", []) if isinstance(logs, dict) else []

            for item in items:
                msg = item.get("message", "") if isinstance(item, dict) else str(item)
                for line in msg.split("\n"):
                    clean = line.strip()
                    if any(err in clean for err in ["Error:", "Exception:", "NameError", "TypeError",
                        "ModuleNotFoundError", "FileNotFoundError", "RuntimeError", "ImportError",
                        "BucketNotFound", "NotAuthorized", "ClassNotFoundException"]):
                        clean = re.sub(r'\x1b\[[0-9;]*m', '', clean)
                        if len(clean) > 10:
                            all_errors.add(f"[{log_source}] {clean[:300]}")
        except Exception:
            continue

    if all_errors:
        return "## Recent Spark Errors (driver + worker)\n" + "\n".join(f"- {e}" for e in sorted(all_errors)[:15])
    return ""


# ─── Build Full Context for a Cell ───────────────────────────────────

async def build_cell_context(
    session,
    cell_source: str,
    cell_index: int,
    total_cells: int,
    notebook_path: str,
    analysis: str,
    dependent_context: str,
    catalog_context: str,
) -> str:
    """Build full context string for Opus when migrating a cell."""
    parts = []

    # Catalog (truncate if huge)
    if catalog_context:
        parts.append(catalog_context[:3000])

    # Dependent notebooks
    if dependent_context:
        parts.append(dependent_context[:2000])

    # S3-to-OCI bucket mapping
    bucket_mapping_ctx = get_bucket_mapping_context()
    if bucket_mapping_ctx:
        parts.append(bucket_mapping_ctx)

    # Kernel state (what's been defined so far)
    try:
        kernel_state = await get_kernel_state(session)
        if kernel_state:
            parts.append(kernel_state)
    except:
        pass

    # Verify storage paths in this cell + suggest alternatives
    try:
        path_verification = await verify_and_suggest_paths(session, cell_source)
        if path_verification:
            parts.append(path_verification)
    except:
        # Fallback to simple check
        paths = extract_storage_paths(cell_source)
        if paths:
            path_checks = []
            for p in paths[:5]:
                try:
                    exists = await check_oci_path(session, p)
                    path_checks.append(f"  {p}: {'EXISTS' if exists else 'NOT FOUND'}")
                except:
                    path_checks.append(f"  {p}: (check failed)")
            if path_checks:
                parts.append("## Storage Paths\n" + "\n".join(path_checks))

    # Analysis summary (truncated)
    if analysis:
        parts.append(f"## Analysis Summary\n{analysis[:1500]}")

    return "\n\n".join(parts)
