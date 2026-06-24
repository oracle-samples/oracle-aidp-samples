#!/usr/bin/env python3
"""
Check Data Availability for a Job
====================================
Scans all notebooks in a workspace directory for storage paths and table
references, then verifies they exist on the AIDP cluster.

Usage:
    python3 check_data_availability.py \
        --root "Users/user@example.com/ExampleProject/ExampleJob" \
        --cluster <cluster-id>
"""

import argparse
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession
from context_tools import load_bucket_mapping, suggest_oci_path

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CLUSTER = "<CLUSTER_ID>"


def unwrap(outputs):
    """Extract text from AIDP session outputs, handling JSON wrapping."""
    from context_tools import _unwrap_aidp_text
    text = ""
    seen = set()
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            val = _unwrap_aidp_text(raw)
            if val not in seen:
                text += val
                seen.add(val)
        elif o.get("type") == "execute_result":
            data = o.get("data", {})
            raw = data.get("text/plain", "")
            val = _unwrap_aidp_text(raw)
            if val not in seen:
                text += val
                seen.add(val)
    return text


async def extract_data_refs(session, workspace_root):
    """Extract all storage paths and table references from notebooks."""
    result = await session.execute(f"""
import json, os, re

base = "/Workspace/{workspace_root}"
all_paths = set()
all_tables = set()
path_sources = {{}}  # path -> list of notebooks that use it
table_sources = {{}}

for root, dirs, files in os.walk(base):
    for f in files:
        if not f.endswith(".ipynb"):
            continue
        path = os.path.join(root, f)
        rel = path[len(base)+1:]
        try:
            with open(path) as fh:
                nb = json.load(fh)
            for cell in nb.get("cells", []):
                src = "".join(cell.get("source", []))

                # S3 paths
                for m in re.findall(r's3[a]?://[\\w\\-\\.]+/[\\w\\-\\./]*', src):
                    all_paths.add(m)
                    path_sources.setdefault(m, []).append(rel)

                # OCI paths
                for m in re.findall(r'oci://[\\w\\-\\.]+@[\\w]+/[\\w\\-\\./]*', src):
                    all_paths.add(m)
                    path_sources.setdefault(m, []).append(rel)

                # DBFS paths
                for m in re.findall(r'dbfs:/[\\w\\-\\./]+', src):
                    all_paths.add(m)
                    path_sources.setdefault(m, []).append(rel)

                # /mnt paths
                for m in re.findall(r'/mnt/[\\w\\-\\./]+', src):
                    all_paths.add(m)
                    path_sources.setdefault(m, []).append(rel)

                # Spark SQL table references (schema.table format only)
                for m in re.findall(r'(?:FROM|JOIN|INTO|TABLE|OVERWRITE)\\s+(\\w+\\.\\w+)', src, re.IGNORECASE):
                    # Filter out Python module refs
                    if m.split(".")[0] not in ("os", "sys", "json", "re", "spark", "dbutils", "optuna", "np", "pd", "plt", "self", "pyspark"):
                        all_tables.add(m)
                        table_sources.setdefault(m, []).append(rel)
                _q = "['" + chr(34) + "]"  # char class matching ' or " without quote-collision
                for m in re.findall(r'spark\\.(?:sql|table|read\\.table)\\s*\\(\\s*' + _q + r'([\\w]+\\.[\\w]+)', src):
                    if m.split(".")[0] not in ("os", "sys", "json", "re", "optuna", "np", "pd", "pyspark"):
                        all_tables.add(m)
                        table_sources.setdefault(m, []).append(rel)
        except:
            pass

# Deduplicate sources
for k in path_sources:
    path_sources[k] = list(set(path_sources[k]))
for k in table_sources:
    table_sources[k] = list(set(table_sources[k]))

print(json.dumps({{"paths": sorted(all_paths), "tables": sorted(all_tables),
                   "path_sources": path_sources, "table_sources": table_sources}}))
""", timeout=120)
    output = unwrap(result.get("outputs", []))
    try:
        return json.loads(output)
    except:
        print(f"ERROR: {output[:500]}")
        return {"paths": [], "tables": [], "path_sources": {}, "table_sources": {}}


async def check_paths(session, paths):
    """Check which storage paths exist on the cluster."""
    if not paths:
        return {}
    paths_json = json.dumps(paths[:50])
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
        results[path] = {{"error": str(e)[:100]}}
print(json.dumps(results))
""", timeout=180)
    output = unwrap(result.get("outputs", []))
    try:
        return json.loads(output)
    except:
        return {}


async def check_tables(session, tables):
    """Check which tables exist in the catalog."""
    if not tables:
        return {}
    tables_json = json.dumps(tables[:50])
    result = await session.execute(f"""
import json
tables = {tables_json}
results = {{}}
for table in tables:
    try:
        exists = spark.catalog.tableExists(table)
        results[table] = {{"exists": exists}}
        if exists:
            count = spark.sql(f"SELECT COUNT(*) FROM {{table}}").collect()[0][0]
            results[table]["rows"] = count
    except Exception as e:
        results[table] = {{"error": str(e)[:100]}}
print(json.dumps(results))
""", timeout=180)
    output = unwrap(result.get("outputs", []))
    try:
        return json.loads(output)
    except:
        return {}


async def main():
    from aidp_executor import DEFAULT_LAKE_OCID, DEFAULT_WORKSPACE_ID, DEFAULT_OCI_PROFILE
    parser = argparse.ArgumentParser(description="Check data availability for notebooks")
    parser.add_argument("--root", required=True, help="Workspace root path")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER,
                        help="AIDP cluster ID (default: %(default)s)")
    parser.add_argument("--bucket-mapping", default=None)  # (Deprecated/ignored) supplied via load_bucket_mapping(); kept for backward compatibility
    # AIDP environment — all required (no compiled-in defaults)
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID,
                        help="AIDP data lake OCID (default: %(default)s)")
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID,
                        help="AIDP workspace UUID (default: %(default)s)")
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE,
                        help="OCI config profile name in ~/.oci/config (default: %(default)s)")
    args = parser.parse_args()

    # Load bucket mapping
    load_bucket_mapping(args.bucket_mapping)

    session = AIDPSession(lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
                          cluster_id=args.cluster, oci_profile=args.oci_profile)
    await session.connect()

    print(f"Scanning /Workspace/{args.root}/ for data references...")
    refs = await extract_data_refs(session, args.root)

    paths = refs["paths"]
    tables = refs["tables"]
    path_sources = refs.get("path_sources", {})
    table_sources = refs.get("table_sources", {})

    print(f"Found {len(paths)} storage paths, {len(tables)} table references\n")

    total_missing = 0  # accumulated across paths + tables (avoids UnboundLocalError when empty)

    # Check paths
    if paths:
        print(f"{'='*60}")
        print(f"Storage Paths ({len(paths)})")
        print(f"{'='*60}")
        path_results = await check_paths(session, paths)
        ok = 0
        missing = 0
        for path in sorted(paths):
            info = path_results.get(path, {})
            sources = path_sources.get(path, [])
            src_str = f" (used by: {', '.join(sources[:3])})" if sources else ""
            if info.get("exists"):
                items = info.get("items", "?")
                print(f"  EXISTS ({items} items)  {path}{src_str}")
                ok += 1
            elif info.get("error"):
                print(f"  ERROR            {path}{src_str}")
                print(f"                   {info['error']}")
                # Check suggestions
                suggestions = suggest_oci_path(path)
                if suggestions:
                    print(f"                   Suggestions: {suggestions[0]}")
            else:
                print(f"  NOT FOUND        {path}{src_str}")
                suggestions = suggest_oci_path(path)
                if suggestions:
                    print(f"                   Suggestions: {suggestions[0]}")
                missing += 1
        print(f"\n  Summary: {ok} exist, {missing} missing, {len(paths)-ok-missing} errors")
        total_missing += missing

    # Check tables
    if tables:
        print(f"\n{'='*60}")
        print(f"Spark Tables ({len(tables)})")
        print(f"{'='*60}")
        table_results = await check_tables(session, tables)
        ok = 0
        missing = 0
        for table in sorted(tables):
            info = table_results.get(table, {})
            sources = table_sources.get(table, [])
            src_str = f" (used by: {', '.join(sources[:3])})" if sources else ""
            if info.get("exists"):
                rows = info.get("rows", "?")
                print(f"  EXISTS ({rows} rows)  {table}{src_str}")
                ok += 1
            elif info.get("error"):
                print(f"  ERROR   {table}: {info['error']}{src_str}")
            else:
                print(f"  MISSING {table}{src_str}")
                missing += 1
        print(f"\n  Summary: {ok} exist, {missing} missing")
        total_missing += missing

    # Overall assessment
    total_issues = total_missing
    print(f"\n{'='*60}")
    if total_issues == 0:
        print("RESULT: ALL DATA AVAILABLE - ready to migrate")
    else:
        print(f"RESULT: {total_issues} data sources missing - check suggestions above")
    print(f"{'='*60}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
