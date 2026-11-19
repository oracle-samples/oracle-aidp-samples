"""Opt-in, customer-pack init scripts for exact exported job clusters.

Databricks init scripts remain upload-only unless the customer pack names an
already staged AIDP Workspace script for a specific job and job-cluster key.
"""

from __future__ import annotations

import posixpath
from pathlib import Path

import yaml


def load_cluster_init_script_rules(cfg):
    pack = getattr(getattr(cfg, "customizations", None), "path", None)
    if not pack:
        return []
    registry = Path(pack).expanduser() / "overrides.yaml"
    try:
        with registry.open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid customer-pack YAML: {registry}") from exc
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError(f"Invalid customer-pack registry: {registry}")
    rules = document.get("cluster_init_scripts", [])
    if not isinstance(rules, list):
        raise ValueError("customer-pack cluster_init_scripts must be a list")
    seen = set()
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError("customer-pack cluster init-script rule must be an object")
        for key in ("dbx_job_id", "job_cluster_key", "destination", "reason"):
            if not isinstance(rule.get(key), str) or not rule[key].strip():
                raise ValueError(f"customer-pack cluster init-script rule requires {key}")
        destination = rule["destination"]
        if (not rule["dbx_job_id"].isdigit()
                or any(token in rule["job_cluster_key"] for token in ("/", "\\", ".."))
                or not destination.startswith("/Workspace/")
                or not destination.endswith(".sh")
                or "\\" in destination
                or posixpath.normpath(destination) != destination):
            raise ValueError("customer-pack init script requires a numeric DBX job ID, "
                             "plain job-cluster key and absolute Workspace .sh path")
        identity = (rule["dbx_job_id"], rule["job_cluster_key"])
        if identity in seen:
            raise ValueError(f"Duplicate customer-pack cluster init-script rule: {identity}")
        seen.add(identity)
    return rules


def prepare_cluster_init_script_rules(aidp, graph, cfg, log, report):
    """Validate target and staged script before any Phase-3 cluster write."""
    contracts = {}
    folders = {}
    for rule in load_cluster_init_script_rules(cfg):
        job_id = rule["dbx_job_id"]
        node = graph.jobs.get(job_id)
        if node is None or node.job_def is None:
            continue
        settings = node.job_def.get("settings") or node.job_def
        matches = [cluster for cluster in settings.get("job_clusters", [])
                   if cluster.get("job_cluster_key") == rule["job_cluster_key"]]
        if len(matches) != 1:
            raise ValueError(
                f"customer-pack init-script target not unique: job {job_id}, "
                f"job_cluster_key={rule['job_cluster_key']}"
            )
        destination = rule["destination"]
        folder = posixpath.dirname(destination)
        if folder not in folders:
            folders[folder] = aidp.list_workspace_objects(folder)
        if not any(item.get("path") == destination and item.get("type") == "FILE"
                   for item in folders[folder]):
            raise ValueError(f"Required customer-pack init script not found: {destination}")
        contracts[(job_id, rule["job_cluster_key"])] = rule
        message = (f"WARN customer-pack-cluster-init-script: job={job_id}, "
                   f"job_cluster_key={rule['job_cluster_key']}; "
                   f"destination={destination}; reason={rule['reason']}")
        log(message)
        report.errors.append(message)
    return contracts


def attach_cluster_init_script(payload, rule):
    """Attach only the approved AIDP script, never the exported DBX script."""
    runtime = payload["clusterRuntimeConfig"]
    scripts = runtime.get("initScripts") or []
    destination = rule["destination"]
    if scripts and scripts != [{"destination": destination}]:
        raise ValueError("customer-pack init script conflicts with cluster initScripts")
    runtime["initScripts"] = [{"destination": destination}]


def verify_existing_cluster_init_script(aidp, cluster_key, rule):
    cluster = aidp.get_cluster(cluster_key)
    scripts = ((cluster.get("clusterRuntimeConfig") or {}).get("initScripts") or [])
    if scripts != [{"destination": rule["destination"]}]:
        raise ValueError(
            f"Reused cluster {cluster_key} does not have the required customer-pack "
            f"init script {rule['destination']}; attach it or recreate the cluster"
        )
