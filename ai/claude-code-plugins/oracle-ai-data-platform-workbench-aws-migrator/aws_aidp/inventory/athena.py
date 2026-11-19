"""Athena inventory — workgroups + named (saved) queries with their SQL."""
from __future__ import annotations

from typing import Any

from aws_aidp.aws_client import AwsClient


def _workgroups(athena) -> list[str]:
    # list_work_groups is NOT a paginatable operation in boto3 — page manually.
    names = []
    token = None
    while True:
        resp = athena.list_work_groups(**({"NextToken": token} if token else {}))
        for wg in resp.get("WorkGroups", []):
            names.append(wg["Name"])
        token = resp.get("NextToken")
        if not token:
            break
    return names


def _named_queries(
    athena, workgroup: str, warnings: list[dict[str, str]]
) -> list[dict]:
    ids: list[str] = []
    for page in athena.get_paginator("list_named_queries").paginate(WorkGroup=workgroup):
        ids.extend(page.get("NamedQueryIds", []))
    out = []
    # batch_get_named_query handles up to 50 ids per call
    for i in range(0, len(ids), 50):
        batch = ids[i : i + 50]
        try:
            r = athena.batch_get_named_query(NamedQueryIds=batch)
        except Exception as e:
            warnings.append({
                "scope": "named_query_batch",
                "resource": workgroup,
                "error": f"failed to fetch {len(batch)} query definition(s): {e}",
            })
            continue
        for q in r.get("NamedQueries", []):
            out.append({
                "id": q["NamedQueryId"],
                "workgroup": workgroup,
                "database": q.get("Database", ""),
                "name": q.get("Name", ""),
                "description": q.get("Description", ""),
                "query": q.get("QueryString", ""),
            })
        unprocessed = r.get("UnprocessedNamedQueryIds", [])
        if unprocessed:
            warnings.append({
                "scope": "named_query_batch",
                "resource": workgroup,
                "error": f"{len(unprocessed)} query definition(s) were unprocessed",
            })
    return out


def scan(client: AwsClient) -> dict[str, Any]:
    athena = client.client("athena")
    try:
        wgs = _workgroups(athena)
    except Exception as e:
        return {"summary": {"error": str(e)}, "items": {"workgroups": [], "named_queries": []}}
    queries: list[dict] = []
    warnings: list[dict[str, str]] = []
    for wg in wgs:
        try:
            queries.extend(_named_queries(athena, wg, warnings))
        except Exception as e:
            warnings.append({"scope": "workgroup", "resource": wg, "error": str(e)})
    per_wg: dict[str, int] = {}
    for q in queries:
        per_wg[q["workgroup"]] = per_wg.get(q["workgroup"], 0) + 1
    return {
        "summary": {
            "workgroup_count": len(wgs),
            "named_query_count": len(queries),
            "queries_per_workgroup": per_wg,
        },
        "items": {"workgroups": wgs, "named_queries": queries},
        "warnings": warnings,
    }
