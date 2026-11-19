"""EMR inventory — clusters (active + recently terminated) and notebooks (Studio)."""
from __future__ import annotations

from typing import Any

from aws_aidp.aws_client import AwsClient

ACTIVE_STATES = ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]
ALL_STATES = ACTIVE_STATES + ["TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"]


def _clusters(
    emr, states: list[str], warnings: list[dict[str, str]]
) -> list[dict]:
    out = []
    for page in emr.get_paginator("list_clusters").paginate(ClusterStates=states):
        for c in page.get("Clusters", []):
            try:
                desc = emr.describe_cluster(ClusterId=c["Id"])["Cluster"]
            except Exception as e:
                desc = {}
                warnings.append({
                    "scope": "cluster_description",
                    "resource": c.get("Id", "<unknown>"),
                    "error": str(e),
                })
            apps = [a.get("Name") for a in desc.get("Applications", []) or []]
            out.append({
                "id": c["Id"],
                "name": c["Name"],
                "state": (c.get("Status") or {}).get("State"),
                "release_label": desc.get("ReleaseLabel", ""),
                "instance_collection_type": desc.get("InstanceCollectionType", ""),
                "applications": apps,
                "service_role": desc.get("ServiceRole", ""),
                "auto_terminate": desc.get("AutoTerminate"),
                "log_uri": desc.get("LogUri", ""),
            })
    return out


def _studio_notebooks(emr, warnings: list[dict[str, str]]) -> list[dict]:
    """EMR Studio notebooks via list_notebook_executions. May be empty if not used."""
    out = []
    try:
        for page in emr.get_paginator("list_notebook_executions").paginate():
            for n in page.get("NotebookExecutions", []):
                out.append({
                    "id": n.get("NotebookExecutionId"),
                    "name": n.get("NotebookExecutionName"),
                    "status": n.get("Status"),
                    "editor_id": n.get("EditorId"),
                    "cluster_id": n.get("ExecutionEngine", {}).get("Id"),
                })
    except Exception as e:
        warnings.append({"scope": "notebook_executions", "resource": "*", "error": str(e)})
    return out


def scan(client: AwsClient, *, active_only: bool = True) -> dict[str, Any]:
    emr = client.client("emr")
    states = ACTIVE_STATES if active_only else ALL_STATES
    warnings: list[dict[str, str]] = []
    try:
        clusters = _clusters(emr, states, warnings)
    except Exception as e:
        return {"summary": {"error": str(e)}, "items": {"clusters": [], "notebooks": []}}
    notebooks = _studio_notebooks(emr, warnings)
    by_state: dict[str, int] = {}
    for c in clusters:
        by_state[c["state"] or "unknown"] = by_state.get(c["state"] or "unknown", 0) + 1
    return {
        "summary": {
            "cluster_count": len(clusters),
            "by_state": by_state,
            "notebook_execution_count": len(notebooks),
        },
        "items": {"clusters": clusters, "notebooks": notebooks},
        "warnings": warnings,
    }
