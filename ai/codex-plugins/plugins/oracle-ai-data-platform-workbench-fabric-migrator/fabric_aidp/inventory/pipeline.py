"""Scan Data Pipeline items. Inventory-only in v0.1 — these become SKIP assets.

Pipelines still matter to ordering: an activity that invokes a notebook is a
real dependency edge, so `notebook_refs` is collected even though no pipeline
translator exists yet.
"""
from __future__ import annotations

import json

from fabric_aidp.inventory.git_workspace import items_of_type

CONTENT_FILE = "pipeline-content.json"
SCHEDULES_FILE = ".schedules"
_NOTEBOOK_ACTIVITY_TYPES = {"tridentnotebook", "notebook", "synapsenotebook"}


# A ForEach or IfCondition holds its children in typeProperties, so a
# top-level-only walk cannot see a notebook inside a loop. Measured on 30 real
# pipelines: 2 notebook references were invisible, and their ordering edges
# with them.
_NESTED_KEYS = ("activities", "ifTrueActivities", "ifFalseActivities",
                "defaultActivities")


def _walk(activities):
    """Every activity, including those nested in ForEach / If / Switch."""
    for activity in activities:
        if not isinstance(activity, dict):
            continue
        yield activity
        properties = activity.get("typeProperties")
        if not isinstance(properties, dict):
            continue
        for key in _NESTED_KEYS:
            inner = properties.get(key)
            if isinstance(inner, list):
                for nested in _walk(inner):
                    yield nested
        for case in properties.get("cases") or []:
            if isinstance(case, dict) and isinstance(case.get("activities"), list):
                for nested in _walk(case["activities"]):
                    yield nested


def _depends_on(activity):
    """Fabric records order as [{"activity": "X", "dependencyConditions": [...]}].

    18 of 30 real pipelines carry these. Dropping them turns an ordered DAG
    into a bag of tasks that all start at once.
    """
    raw = activity.get("dependsOn")
    if not isinstance(raw, list):
        return []
    names = []
    for entry in raw:
        if isinstance(entry, dict):
            value = entry.get("activity")
            if isinstance(value, str) and value.strip():
                names.append(value)
        elif isinstance(entry, str) and entry.strip():
            names.append(entry)
    # One name per upstream. An activity wired to A twice (on success and on
    # failure, as two entries) listed A twice, and the job came out with
    # `dependsOn: [{"taskKey": "A"}, {"taskKey": "A"}]` (PL_DupEdge).
    return list(dict.fromkeys(names))


def _depends_on_conditions(activity):
    """{upstream: [conditions]}. Fabric runs an activity when *every* edge's
    condition holds, and an edge holds when *any* of its conditions does.
    Keeping only the names turned an on-failure alert into an on-success
    task.

    Two entries for the same upstream are that upstream's edge twice -- the
    canvas's "on success" and "on failure" arrows from A into B -- so their
    conditions are one edge's, united: Succeeded + Failed is "A finished".
    Keyed by name, the second entry overwrote the first: PL_DupEdge's B,
    wired to A on success and on failure, became `{"A": ["Failed"]}` and an
    ALL_FAILED task that no longer ran after a successful A. An entry with
    no conditions is Fabric's default, Succeeded, once it has to be united.
    """
    raw = activity.get("dependsOn")
    conditions = {}
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict) or not isinstance(entry.get("activity"), str):
            continue
        listed = entry.get("dependencyConditions")
        listed = sorted(str(c) for c in listed
                        if isinstance(c, str)) if isinstance(listed, list) else []
        upstream = entry["activity"]
        if upstream in conditions:
            listed = sorted(set(conditions[upstream] or ["Succeeded"])
                            | set(listed or ["Succeeded"]))
        conditions[upstream] = listed
    return conditions


def _notebook_workspace(activity):
    properties = activity.get("typeProperties")
    value = properties.get("workspaceId") if isinstance(properties, dict) else None
    return value if isinstance(value, str) else ""


def _policy(activity):
    """timeout / retry / retryIntervalInSeconds, as Fabric wrote them."""
    raw = activity.get("policy")
    if not isinstance(raw, dict):
        return {}
    return {key: raw[key] for key in ("timeout", "retry", "retryIntervalInSeconds")
            if key in raw}


def _notebook_parameters(activity):
    """typeProperties.parameters of a notebook activity, as Fabric wrote them:
    {name: {"value": literal | {"value": "@...", "type": "Expression"},
    "type": "string"}}. 25 of the 30 real pipelines pass notebook parameters;
    the inventory did not keep them, so every emitted task ran its notebook
    with the parameter-cell defaults and nothing said so."""
    if str(activity.get("type", "")).casefold() not in _NOTEBOOK_ACTIVITY_TYPES:
        return {}
    properties = activity.get("typeProperties")
    raw = properties.get("parameters") if isinstance(properties, dict) else None
    return raw if isinstance(raw, dict) else {}


def _pipeline_parameters(content):
    """properties.parameters: {name: {"type": ..., "defaultValue": ...}}, what
    `@pipeline().parameters.X` in an activity resolves against."""
    if not isinstance(content, dict):
        return {}
    for container in (content.get("properties"), content):
        if isinstance(container, dict) and isinstance(container.get("parameters"), dict):
            return container["parameters"]
    return {}


def _pipeline_name(activity):
    """The pipeline an ExecutePipeline activity runs, for the ordering edge.

    17 of 89 activities across 30 real pipelines are ExecutePipeline. Without
    this the plan cannot know that one pipeline must run before another.
    """
    if str(activity.get("type", "")).casefold() != "executepipeline":
        return None
    properties = activity.get("typeProperties")
    if not isinstance(properties, dict):
        return None
    reference = properties.get("pipeline")
    if isinstance(reference, dict):
        value = reference.get("referenceName") or reference.get("name")
        if isinstance(value, str) and value.strip():
            return value
    if isinstance(reference, str) and reference.strip():
        return reference
    return None


def _activities(content):
    if not isinstance(content, dict):
        return []
    for container in (content.get("properties"), content):
        if isinstance(container, dict) and isinstance(container.get("activities"), list):
            return [a for a in container["activities"] if isinstance(a, dict)]
    return []


def _notebook_name(activity, notebook_names=None):
    """The notebook a notebook activity runs, as the display name when known.

    A Fabric Git export never writes `notebookName`: a TridentNotebook activity
    carries `notebookId` + `workspaceId`, and for a notebook in the same
    workspace Fabric rewrites the id to the notebook's logicalId (with an
    all-zero workspaceId). All 42 notebook activities in the 30 real pipelines
    have this shape. Map it back through the export's own `.platform` files;
    an id that is not in this export stays as the raw id, which no migrated
    notebook will match, so the pipeline is blocked rather than guessed.
    """
    if str(activity.get("type", "")).casefold() not in _NOTEBOOK_ACTIVITY_TYPES:
        return None
    properties = activity.get("typeProperties")
    if not isinstance(properties, dict):
        return None
    for key in ("notebookName", "notebook", "notebookId"):
        value = properties.get(key)
        if isinstance(value, str) and value.strip():
            if key == "notebookId":
                return (notebook_names or {}).get(value.strip().casefold(), value)
            return value
        if isinstance(value, dict):
            nested = value.get("referenceName") or value.get("name")
            if isinstance(nested, str) and nested.strip():
                return nested
    return None


def _schedules(item):
    """(schedules, error) from the item's `.schedules`, as Fabric wrote them.

    A Fabric Git export keeps a pipeline's triggers in this file, beside
    pipeline-content.json, and nothing read it: PL_Params runs every 60
    minutes in Fabric, and its AIDP job was emitted with no word that it
    would never run by itself. No file is no schedule. A file that is there
    but unusable is recorded, not fatal -- the pipeline's activities are
    still worth migrating, and the translator says the schedule is unknown.
    """
    path = item.file(SCHEDULES_FILE)
    try:
        content = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return [], None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return [], f"cannot read {SCHEDULES_FILE}: {exc}"
    raw = content.get("schedules") if isinstance(content, dict) else None
    if not isinstance(raw, list):
        return [], f"{SCHEDULES_FILE} has no `schedules` list"
    schedules = [{
        "enabled": entry.get("enabled"),
        "job_type": entry.get("jobType") if isinstance(entry.get("jobType"), str) else "",
        "configuration": (entry["configuration"]
                          if isinstance(entry.get("configuration"), dict) else {}),
    } for entry in raw if isinstance(entry, dict)]
    skipped = len(raw) - len(schedules)
    return schedules, (f"{SCHEDULES_FILE}: {skipped} schedule entr"
                       f"{'y is' if skipped == 1 else 'ies are'} not an object"
                       if skipped else None)


def scan(items, *, log=None) -> dict:
    pipelines = []
    activity_total = 0
    notebook_names = {item.logical_id.strip().casefold(): item.name
                      for item in items_of_type(items, "Notebook")
                      if item.logical_id.strip()}

    for item in items_of_type(items, "DataPipeline"):
        record = {
            "name": item.name,
            "logical_id": item.logical_id,
            "folder": item.folder,
            "activities": [],
            "notebook_refs": [],
            "pipeline_refs": [],
            "parameters": {},
        }
        record["schedules"], schedules_error = _schedules(item)
        if schedules_error:
            record["schedules_error"] = schedules_error
        path = item.file(CONTENT_FILE)
        try:
            content = json.loads(path.read_text(encoding="utf-8-sig"))
        except FileNotFoundError:
            record["content_error"] = f"no {CONTENT_FILE} in the item directory"
            pipelines.append(record)
            continue
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            record["content_error"] = f"cannot read {CONTENT_FILE}: {exc}"
            pipelines.append(record)
            continue

        record["parameters"] = _pipeline_parameters(content)
        refs, pipeline_refs = [], []
        for activity in _walk(_activities(content)):
            notebook = _notebook_name(activity, notebook_names)
            downstream = _pipeline_name(activity)
            record["activities"].append({
                "name": activity.get("name") if isinstance(activity.get("name"), str) else "",
                "type": activity.get("type") if isinstance(activity.get("type"), str) else "",
                "notebook": notebook or "",
                "notebook_workspace_id": _notebook_workspace(activity),
                "pipeline": downstream or "",
                "depends_on": _depends_on(activity),
                "depends_on_conditions": _depends_on_conditions(activity),
                "policy": _policy(activity),
                "parameters": _notebook_parameters(activity),
                "state": activity.get("state") if isinstance(activity.get("state"), str) else "",
                "on_inactive": (activity.get("onInactiveMarkAs")
                                if isinstance(activity.get("onInactiveMarkAs"), str) else ""),
            })
            if notebook:
                refs.append(notebook)
            if downstream:
                pipeline_refs.append(downstream)
        record["notebook_refs"] = sorted(set(refs))
        record["pipeline_refs"] = sorted(set(pipeline_refs))
        activity_total += len(record["activities"])
        pipelines.append(record)
        if log:
            log(f"  pipeline {item.name} ({len(record['activities'])} activities)")

    return {
        "summary": {"pipeline_count": len(pipelines), "activity_count": activity_total},
        "items": {"pipelines": pipelines},
    }
