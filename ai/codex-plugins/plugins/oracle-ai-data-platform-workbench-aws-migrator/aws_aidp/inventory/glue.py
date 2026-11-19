"""Glue inventory — Data Catalog (databases + tables) and Glue ETL jobs."""
from __future__ import annotations

import re
from typing import Any

from aws_aidp.aws_client import AwsClient


def _databases(glue) -> list[dict]:
    out = []
    for page in glue.get_paginator("get_databases").paginate():
        for db in page.get("DatabaseList", []):
            out.append({"name": db["Name"], "location_uri": db.get("LocationUri", "")})
    return out


def _tables(glue, db_name: str) -> list[dict]:
    out = []
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=db_name):
        for t in page.get("TableList", []):
            sd = t.get("StorageDescriptor", {}) or {}
            out.append({
                "database": db_name,
                "name": t["Name"],
                "location": sd.get("Location", ""),
                "input_format": sd.get("InputFormat", ""),
                "table_type": t.get("TableType", ""),
                "partition_keys": [p["Name"] for p in t.get("PartitionKeys", [])],
                "column_count": len((sd.get("Columns") or [])),
            })
    return out


def _fetch_script(
    client, s3_uri: str, warnings: list[dict[str, str]], job_name: str
) -> str:
    """Best-effort read of a Glue job script from S3. Returns '' on any failure
    so inventory never hard-fails on a single unreadable script."""
    m = re.match(r"s3://([^/]+)/(.+)$", s3_uri or "")
    if not m:
        if s3_uri:
            warnings.append({
                "scope": "job_script",
                "resource": job_name,
                "error": f"unsupported script location: {s3_uri}",
            })
        return ""
    try:
        s3 = client.client("s3")
        obj = s3.get_object(Bucket=m.group(1), Key=m.group(2))
        script = obj["Body"].read()
        try:
            return script.decode("utf-8")
        except UnicodeDecodeError as e:
            warnings.append({
                "scope": "job_script",
                "resource": job_name,
                "error": f"script is not valid UTF-8; undecodable bytes were replaced: {e}",
            })
            return script.decode("utf-8", errors="replace")
    except Exception as e:
        warnings.append({"scope": "job_script", "resource": job_name, "error": str(e)})
        return ""


def _jobs(glue, client, warnings: list[dict[str, str]]) -> list[dict]:
    out = []
    for page in glue.get_paginator("get_jobs").paginate():
        for j in page.get("Jobs", []):
            cmd = j.get("Command", {}) or {}
            script_loc = cmd.get("ScriptLocation", "")
            out.append({
                "name": j["Name"],
                "type": cmd.get("Name", ""),                # "glueetl" | "pythonshell" | "gluestreaming"
                "script_location": script_loc,
                "script": _fetch_script(client, script_loc, warnings, j["Name"]),
                "python_version": cmd.get("PythonVersion", ""),
                "glue_version": j.get("GlueVersion", ""),
                "worker_type": j.get("WorkerType", ""),
                "num_workers": j.get("NumberOfWorkers"),
            })
    return out


def scan(client: AwsClient) -> dict[str, Any]:
    glue = client.client("glue")
    try:
        dbs = _databases(glue)
    except Exception as e:
        return {"summary": {"error": str(e)}, "items": {"databases": [], "tables": [], "jobs": []}}
    warnings: list[dict[str, str]] = []
    tables: list[dict] = []
    for db in dbs:
        try:
            tables.extend(_tables(glue, db["name"]))
        except Exception as e:
            warnings.append({"scope": "database_tables", "resource": db["name"], "error": str(e)})
    try:
        jobs = _jobs(glue, client, warnings)
    except Exception as e:
        jobs = []
        warnings.append({"scope": "jobs", "resource": "*", "error": str(e)})
    tables_per_db: dict[str, int] = {}
    for t in tables:
        tables_per_db[t["database"]] = tables_per_db.get(t["database"], 0) + 1
    return {
        "summary": {
            "database_count": len(dbs),
            "table_count": len(tables),
            "tables_per_db": tables_per_db,
            "job_count": len(jobs),
        },
        "items": {"databases": dbs, "tables": tables, "jobs": jobs},
        "warnings": warnings,
    }
