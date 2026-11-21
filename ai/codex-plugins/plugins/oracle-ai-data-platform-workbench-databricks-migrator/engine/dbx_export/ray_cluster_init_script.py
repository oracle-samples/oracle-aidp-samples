"""Attach an operator-staged Ray head script to the owning job cluster."""

from __future__ import annotations

import posixpath
from pathlib import Path

import yaml

from dbx_export import foreach_bridge
from dbx_export.customer_cluster_init_scripts import (
    attach_cluster_init_script,
    verify_existing_cluster_init_script,
)
from dbx_export.dependency_resolver import task_cluster_context
from scripts.ray_aidp_rewriter import notebook_requires_ray_head_script


def _registry(cfg):
    pack = getattr(getattr(cfg, "customizations", None), "path", None)
    if not pack:
        return {}
    registry = Path(pack).expanduser() / "overrides.yaml"
    try:
        with registry.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid customer-pack YAML: {registry}") from exc
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError(f"Invalid customer-pack registry: {registry}")
    return document


def _valid_destination(destination):
    return (isinstance(destination, str)
            and destination.startswith("/Workspace/")
            and destination.endswith(".sh")
            and "\\" not in destination
            and posixpath.normpath(destination) == destination)


def _task_owners(graph, notebook_path, visited=None):
    visited = set() if visited is None else visited
    if notebook_path in visited:
        return set()
    visited.add(notebook_path)
    owners = set()
    for ref in graph.notebooks[notebook_path].referenced_by:
        if ref.startswith("job:") and "/task:" in ref:
            job_id, _, task_ref = ref[4:].partition("/task:")
            task_key, _, suffix = task_ref.partition("/")
            owners.add((job_id, task_key, suffix == "for_each"))
        elif ref in graph.notebooks:
            owners.update(_task_owners(graph, ref, visited))
    return owners


def _contracts(graph, rule):
    contracts = {}
    for path, notebook in graph.notebooks.items():
        if not notebook.nb_dict or not notebook_requires_ray_head_script(
            notebook.nb_dict.get("cells") or []
        ):
            continue
        for job_id, task_key, nested_for_each in _task_owners(graph, path):
            job = graph.jobs.get(job_id)
            if job is None or job.job_def is None:
                continue
            settings = job.job_def.get("settings") or job.job_def
            task = next((item for item in settings.get("tasks", [])
                         if item.get("task_key") == task_key), None)
            execution_task = (
                foreach_bridge.nested_notebook_task(task)
                if task is not None and nested_for_each
                else task
            )
            context = (
                task_cluster_context(str(job_id), execution_task)
                if execution_task is not None
                else None
            )
            if context is None:
                raise ValueError(
                    f"Ray notebook {path} is used by {job_id}/{task_key} without "
                    "a declared execution cluster; cannot attach required init script"
                )
            if context.startswith(f"job:{job_id}/job_cluster:"):
                cluster_key = context.rsplit(":", 1)[1]
                if not any(
                    item.get("job_cluster_key") == cluster_key
                    for item in settings.get("job_clusters", [])
                ):
                    raise ValueError(
                        f"Ray notebook {path} references missing job cluster "
                        f"{cluster_key!r} in {job_id}/{task_key}"
                    )
            contracts[(str(job_id), context)] = dict(rule)
    return contracts


def prepare_ray_cluster_init_script_rules(aidp, graph, cfg, log, report):
    """Validate the Ray contract and staged file before creating clusters."""
    rule = _registry(cfg).get("ray_init_script")
    if rule is None:
        return {}
    if (not isinstance(rule, dict)
            or not _valid_destination(rule.get("destination"))
            or not isinstance(rule.get("reason"), str)
            or not rule["reason"].strip()):
        raise ValueError(
            "customer-pack ray_init_script requires an absolute Workspace "
            ".sh destination and reason"
        )
    contracts = _contracts(graph, rule)
    if not contracts:
        return {}
    destination = rule["destination"]
    folder = posixpath.dirname(destination)
    if not any(
        item.get("path") == destination and item.get("type") == "FILE"
        for item in aidp.list_workspace_objects(folder)
    ):
        raise ValueError(f"Required Ray init script not found: {destination}")
    for identity in sorted(contracts):
        message = (f"WARN ray-cluster-init-script: job={identity[0]}, "
                   f"execution_context={identity[1]}; destination={destination}; "
                   f"reason={rule['reason']}")
        log(message)
        report.errors.append(message)
    return contracts


def attach_ray_init_script(payload, rule):
    """Use the shared attach contract after Ray-specific ownership discovery."""
    attach_cluster_init_script(payload, rule)


def verify_existing_ray_init_script(aidp, cluster_key, rule):
    """Use the shared exact-attachment check for a reused Ray cluster."""
    verify_existing_cluster_init_script(aidp, cluster_key, rule)
