"""Optional, exact cluster-library contracts from removable customer packs.

Customer identities and artifact names belong in overrides.yaml, not the
exporter. A configured replacement is mandatory, unlike best-effort generic
JAR discovery. Existing conflicting registrations require operator removal:
attaching another assembly beside them is not a safe classloader replacement.
"""
from __future__ import annotations

import copy
import fnmatch
import posixpath
from pathlib import Path

import yaml

from .dependency_resolver import FileNode


def load_cluster_library_rules(cfg):
    pack = getattr(getattr(cfg, "customizations", None), "path", None)
    if not pack:
        return []
    registry = Path(pack).expanduser() / "overrides.yaml"
    with registry.open(encoding="utf-8") as stream:
        try:
            document = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid customer-pack YAML: {registry}") from exc
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError(f"Invalid customer-pack registry: {registry}")
    rules = document.get("cluster_libraries", [])
    if not isinstance(rules, list):
        raise ValueError("customer-pack cluster_libraries must be a list")
    seen = set()
    for rule in rules:
        if not isinstance(rule, dict):
            raise ValueError("customer-pack cluster-library rule must be an object")
        for key in ("dbx_job_id", "job_cluster_key", "jar", "reason"):
            if not isinstance(rule.get(key), str) or not rule[key].strip():
                raise ValueError(f"customer-pack cluster-library rule requires {key}")
        jar = rule["jar"]
        if not rule["dbx_job_id"].isdigit() or any(
            token in rule["job_cluster_key"] for token in ("/", "\\", "..")
        ):
            raise ValueError("customer-pack requires a numeric DBX job ID and a plain job-cluster key")
        patterns = rule.get("replace_basenames")
        if (posixpath.basename(jar) != jar or "\\" in jar
                or not jar.endswith(".jar") or jar in {".", ".."}):
            raise ValueError("customer-pack jar must be an exact JAR basename")
        if (not isinstance(patterns, list) or not patterns
                or any(not isinstance(p, str) or not p or "/" in p or "\\" in p
                       or not p.endswith(".jar") for p in patterns)):
            raise ValueError("customer-pack replace_basenames must contain JAR basename patterns")
        # One cluster may require several independent approved replacements.
        identity = (rule["dbx_job_id"], rule["job_cluster_key"], rule["jar"])
        if identity in seen:
            raise ValueError(f"Duplicate customer-pack cluster-library rule: {identity}")
        seen.add(identity)
    return rules


def _replaced(path, rule):
    basename = posixpath.basename(path)
    return basename != rule["jar"] and any(
        fnmatch.fnmatchcase(basename, pattern)
        for pattern in rule["replace_basenames"]
    )


def filter_cluster_library_refs(refs, graph, context, uri_to_aidp=None):
    rules = [rule for key, rule in getattr(graph, "customer_cluster_library_contracts", {}).items()
             if key == context or key.startswith(context + "#")]
    if not rules:
        return refs
    mapping = uri_to_aidp or getattr(graph, "customer_cluster_library_paths", {})
    return [lib for lib in refs if not lib.get("jar") or not (
        any(_replaced(lib["jar"], rule) or _replaced(mapping.get(lib["jar"], ""), rule)
            for rule in rules)
    )]


def prepare_cluster_library_rules(aidp, graph, cfg, repository, uri_to_aidp, log, report):
    """Return a private graph with overridden library plans; preserve source graph.

    Called before Phase 3 cluster writes. Exact repository existence is checked
    first; aliases, highest-version heuristics and generic class-index matches
    must never supersede an operator-approved customer-pack artifact.
    """
    rules = load_cluster_library_rules(cfg)
    applicable = [rule for rule in rules if rule["dbx_job_id"] in graph.jobs]
    if not applicable:
        return graph, {}
    planned = copy.deepcopy(graph)
    contracts = {}
    objects = None
    for rule in applicable:
        node = planned.jobs[rule["dbx_job_id"]]
        if node.job_def is None:
            continue
        settings = node.job_def.get("settings") or node.job_def
        clusters = [cluster for cluster in settings.get("job_clusters", [])
                    if cluster.get("job_cluster_key") == rule["job_cluster_key"]]
        if len(clusters) != 1:
            raise ValueError(
                f"customer-pack cluster-library target not unique: job "
                f"{rule['dbx_job_id']}, job_cluster_key={rule['job_cluster_key']}"
            )
        if objects is None:
            objects = aidp.list_workspace_objects(repository.rstrip("/"))
        target = repository.rstrip("/") + "/" + rule["jar"]
        if not any(item.get("path") == target and item.get("type") == "FILE"
                   for item in objects):
            raise ValueError(f"Required customer-pack JAR not found: {target}")
        context = f"job:{rule['dbx_job_id']}/job_cluster:{rule['job_cluster_key']}"
        task_refs = {f"job:{rule['dbx_job_id']}/task:{task['task_key']}"
                     for task in settings.get("tasks", [])
                     if task.get("job_cluster_key") == rule["job_cluster_key"]}
        replaced = set()
        for uri, file_node in planned.files.items():
            path = uri_to_aidp.get(uri) or file_node.aidp_repo_path or file_node.aidp_fallback_path or uri
            if file_node.kind == "jar" and (_replaced(uri, rule) or _replaced(path, rule)):
                if context in file_node.referenced_by:
                    replaced.add(path)
                file_node.referenced_by = [ref for ref in file_node.referenced_by
                                           if ref not in task_refs and ref != context]
        def filter_libraries(spec):
            libraries = spec.get("libraries")
            if libraries is not None:
                spec["libraries"] = [lib for lib in libraries if not lib.get("jar") or not (
                    _replaced(lib["jar"], rule)
                    or _replaced(uri_to_aidp.get(lib["jar"], ""), rule)
                )]
        filter_libraries(clusters[0].get("new_cluster") or {})
        for task in settings.get("tasks", []):
            if task.get("job_cluster_key") == rule["job_cluster_key"]:
                filter_libraries(task)
                filter_libraries(task.get("new_cluster") or {})
        # Context-specific identity prevents this override from redirecting a
        # shared source URI used by another cluster/job.
        uri = f"/dbx_export_generated/customer_pack/{rule['dbx_job_id']}/{rule['job_cluster_key']}/{rule['jar']}"
        planned.files[uri] = FileNode(
            dbx_source=uri, kind="jar", referenced_by=[context],
            aidp_repo_path=target, aidp_repo_provenance=rule["reason"],
        )
        # The dependency graph is private (`planned` is a deep copy), while this
        # caller-owned lookup is the phase-wide path registry.  Add only the new,
        # context-scoped synthetic URI so later install planning can resolve the
        # approved artifact without rewriting any source URI mapping.
        uri_to_aidp[uri] = target
        contract_key = context if context not in contracts else context + "#" + rule["jar"]
        contracts[contract_key] = dict(rule, path=target)
        message = (f"INFO customer-pack-cluster-library: {context}: {target}; "
                   f"replaced={sorted(replaced)}; reason={rule['reason']}")
        log(message)
        report.errors.append(message)
    planned.customer_cluster_library_contracts = contracts
    planned.customer_cluster_library_paths = dict(uri_to_aidp)
    return planned, contracts


def check_cluster_library_conflicts(aidp, cluster, rule):
    """Reject competing registrations before any install on a reused cluster."""
    rows = aidp.list_cluster_libraries(cluster)
    conflicts = sorted({row.get("path", "") for row in rows
                        if row.get("status", "").upper() != "DELETED"
                        and (_replaced(row.get("path", ""), rule)
                             or (posixpath.basename(row.get("path", "")) == rule["jar"]
                                 and row.get("path") != rule["path"]))})
    if conflicts:
        raise ValueError(
            f"customer-pack JAR conflict on {cluster}: {conflicts}. "
            "Detach competing assemblies and restart this job cluster before rerunning; "
            "the tool will not load both assemblies or restart an active cluster here."
        )


def verify_cluster_library_contract(aidp, cluster, rule, log, report):
    """Enforce exact attachment; never mistake staged for runtime-loaded."""
    check_cluster_library_conflicts(aidp, cluster, rule)
    aidp.install_cluster_libraries(cluster, [{
        "operation": "INSTALL", "type": "WORKSPACE_FILE", "path": rule["path"],
    }])
    rows = aidp.list_cluster_libraries(cluster)
    statuses = {row.get("status", "").upper() for row in rows if row.get("path") == rule["path"]}
    if not statuses.intersection({"INSTALLED", "RESOLVED", "INSTALL_ON_RESTART"}):
        raise ValueError(f"Required customer-pack JAR not ready on {cluster}: {rule['path']}; {sorted(statuses)}")
    pending = not statuses.intersection({"INSTALLED", "RESOLVED"})
    message = (f"INFO customer-pack-cluster-library: cluster={cluster}; jar={rule['path']}; "
               + ("STAGED; restart required before validation (existing readiness policy)"
                  if pending else "attachment verified"))
    log(message)
    report.errors.append(message)
