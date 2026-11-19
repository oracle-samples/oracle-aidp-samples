"""Fabric Data Pipeline -> AIDP workflow job definition.

A Fabric pipeline and an AIDP job are the same idea: named steps with an
ordering between them. Activities become tasks, Fabric's
`dependsOn: [{activity: "X"}]` becomes AIDP's `dependsOn: [{taskKey: "x"}]`,
and a TridentNotebook activity becomes a NOTEBOOK_TASK pointing at the
notebook this tool already migrated. Its notebook parameters travel as task
`parameters` when they are literals; a value Fabric computes at run time is
flagged, never guessed.

Two refusals, both taken from the Databricks validator's clone_workflow, which
learned them the hard way:

  * A pipeline containing an activity with no AIDP equivalent -- Copy, Lookup,
    ForEach -- is **blocked whole**. Emitting only its notebook tasks would
    produce a job that runs, skips the Copy that fed it, and is wrong. Half of
    the 30 real pipelines measured are in this bucket, and saying so is more
    use than a job that quietly does less than the original.
  * A notebook task whose notebook was not migrated is **blocked**, never
    pointed at a guessed path. A workflow referencing a notebook that is not
    there fails at run time, far from the cause.

Output is the JSON payload the AIDP Job API accepts, minus the cluster: a
job with no `--cluster-key` carries `PL12_NO_CLUSTER` and every task omits
`cluster`, so it cannot run until one is supplied. `publish` adds that and
namespaces the job name with `--prefix`; it does not otherwise reshape the
payload.
"""
from __future__ import annotations

import json
import re

from fabric_aidp.translate.types import Finding, TranslationResult

NOTEBOOK_ACTIVITY_TYPES = {"tridentnotebook", "synapsenotebook", "notebook"}
# Fabric's own default activity timeout (policy.timeout "0.12:00:00"), used
# only when an activity carries no policy. A flat 3600 cut 13 of the 15 real
# jobs from 12h to 1h, and repeating it as the job-level timeout capped a
# whole chain of tasks at one hour.
DEFAULT_TIMEOUT_SECONDS = 12 * 3600
# Fabric's retry interval when `retry` is set without one.
DEFAULT_RETRY_INTERVAL_SECONDS = 30
_TIMESPAN = re.compile(r"^(?:(\d+)\.)?(\d{1,2}):(\d{2}):(\d{2})$")
# Fabric edge conditions -> AIDP runIf, when every edge of a task carries the
# same one. An edge of [Succeeded, Failed] is Completed.
_RUN_IF = {
    frozenset({"succeeded"}): "ALL_SUCCESS",
    frozenset({"failed"}): "ALL_FAILED",
    frozenset({"completed"}): "ALL_DONE",
    frozenset({"succeeded", "failed"}): "ALL_DONE",
}
_NOT_TASK_KEY = re.compile(r"[^A-Za-z0-9_-]+")
_GUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


class Blocked(Exception):
    """This pipeline has no faithful AIDP job. Named, never approximated."""


# AIDP refuses anything else: "Invalid resource name. Must start with letter
# and no special characters are allowed except for underscore, slash" (live,
# create-job, 2026-09). Slash is left out: it reads as a path.
JOB_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_NOT_JOB_NAME = re.compile(r"[^A-Za-z0-9_]+")


def job_name(name) -> str:
    """Pipeline display name -> a name AIDP accepts. `Daily Refresh` -> `Daily_Refresh`."""
    cleaned = _NOT_JOB_NAME.sub("_", str(name or "")).strip("_") or "job"
    return cleaned if cleaned[0].isascii() and cleaned[0].isalpha() else "job_" + cleaned


def task_key(name) -> str:
    """Activity name -> AIDP taskKey. `Load Meters` -> `Load_Meters`."""
    cleaned = _NOT_TASK_KEY.sub("_", str(name or "")).strip("_")
    return cleaned or "task"


def _unique_keys(activities):
    """(keys by position, name -> key for dependency lookup).

    Keys are positional, not keyed by name: two activities may share a name,
    and a name-keyed map collapsed them onto one taskKey -- AIDP would have
    seen a duplicate. The lookup map deliberately omits duplicated names,
    because `dependsOn: "Run"` when two activities are called `Run` is
    ambiguous in the source and must not be resolved by guessing.

    The first activity with a given base key keeps it; a later one takes the
    lowest `_N` that no activity's base key and no earlier suffix holds. A
    per-base counter alone gave `Load A`, `Load_A`, `Load_A_2` the keys
    `Load_A`, `Load_A_2`, `Load_A_2`: the third task depended on itself, and a
    task after it depended on whichever one AIDP picked.
    """
    # Every base key is reserved before any suffix is handed out. Appending
    # `_2` to the second `A` collides with an activity genuinely called
    # `A_2`: measured, names ['A', 'A', 'A_2'] produced keys
    # ['A', 'A_2', 'A_2'] -- two tasks with one taskKey, which is the thing
    # this function exists to prevent. Suffixes now skip any key already
    # spoken for, by a base name or by an earlier suffix.
    bases = [task_key(activity.get("name") or "") for activity in activities]
    taken, seen, keys = set(bases), {}, []
    for base in bases:
        count = seen.get(base, 0)
        seen[base] = count + 1
        if count == 0:
            keys.append(base)
            continue
        suffix = count + 1
        while f"{base}_{suffix}" in taken:
            suffix += 1
        key = f"{base}_{suffix}"
        taken.add(key)
        keys.append(key)

    names = [activity.get("name") or "" for activity in activities]
    ambiguous = {name for name in names if names.count(name) > 1}
    lookup = {name: key for name, key in zip(names, keys)
              if name not in ambiguous}
    return keys, lookup


def _timeout(activity, default):
    """policy.timeout (d.hh:mm:ss) -> seconds, or (default, why not)."""
    raw = (activity.get("policy") or {}).get("timeout")
    if raw is None or raw == "":
        return default, None
    match = _TIMESPAN.match(str(raw).strip()) if isinstance(raw, str) else None
    if match is None:
        return default, (f"timeout {raw!r} is not a d.hh:mm:ss timespan; "
                         f"the task uses {default}s")
    days, hours, minutes, seconds = (int(g or 0) for g in match.groups())
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds, None


def _retries(activity, findings):
    policy = activity.get("policy") or {}
    retry = policy.get("retry")
    if "retry" in policy and not (isinstance(retry, int) and not isinstance(retry, bool)
                                  and retry >= 0):
        findings.append(Finding(
            "PL13_RETRY", f"activity {activity.get('name')!r}: retry {retry!r} is not "
            f"a whole number (an expression?); the task does not retry", "flag"))
    if not isinstance(retry, int) or isinstance(retry, bool) or retry <= 0:
        return {"maxRetries": 0}
    interval = policy.get("retryIntervalInSeconds")
    if not isinstance(interval, int) or isinstance(interval, bool) or interval < 0:
        interval = DEFAULT_RETRY_INTERVAL_SECONDS
    return {"maxRetries": retry, "minRetryIntervalMillis": interval * 1000}


# `@pipeline().parameters.X`, bare or string-interpolated, as the whole value.
# Anything else -- @utcNow(), @concat(...), @activity(...).output,
# @variables(...), @item(), @pipeline().RunId -- is evaluated by Fabric at run
# time and has no value to carry.
_PIPELINE_PARAMETER = re.compile(
    r"^\s*(?:@pipeline\(\)\.parameters\.(\w+)|@\{\s*pipeline\(\)\.parameters\.(\w+)\s*\})\s*$")
_LITERAL = (str, int, float, bool)


def _as_text(value):
    # AIDP task parameter values are strings. Python spelling for a bool: the
    # Fabric parameter cell it replaced was Python, and `flag = True` there.
    return str(value)


def _declared_type(value, declared) -> str:
    """What Fabric says this parameter is: its own `type`, else the value's."""
    if declared and str(declared).casefold() != "string":
        return str(declared)
    return type(value).__name__


def _retyped(value, declared) -> bool:
    """Whether carrying this parameter loses the type Fabric gave it.

    Review note 8, decided. `PL21` stays `rewrite`: the value did reach the
    job, faithfully, and that is a change worth counting. What the reviewer
    was right about is the *other* fact, which PL21 was carrying as a
    parenthesis inside a `rewrite` and so keeping out of REVIEW entirely --
    that an `int` in Fabric arrives at the notebook as a `str`.

    It is split out rather than made a second severity of PL21, so one id
    still means one thing in a report. It is `flag` rather than `rewrite`
    because this translator *cannot* know the outcome: the type comes back
    only if the notebook's parameters cell re-reads that name, and the cell
    is in a different file, read by `to_ipynb` at publish time. Review notes
    3 and 4 are the evidence -- measured over the vendored corpus, 42 names
    in real parameters cells cannot be re-read at all. "A human has to look
    at another file" is the definition of `flag` here.

    Two ways the type is lost, and both count. Fabric declaring a non-string
    type is the obvious one. A parameter with no declared type whose *value*
    is not a string is the same loss with nothing written down: Fabric wrote
    `100`, AIDP carries `"100"`.
    """
    if not isinstance(value, str):
        return True
    return bool(declared) and str(declared).casefold() != "string"


def _parameters(activity, pipeline_parameters, findings):
    """Fabric notebook parameters -> AIDP `[{"name", "value"}]`, or flags.

    They used to be dropped with no finding: PL_Params' `run_date` and
    `limit` never reached AIDP, and the notebook ran with its parameter-cell
    defaults. A list, not a mapping: AIDP rejects a mapping with "Unable to
    process JSON input" (FabricToaidpTool STATUS.md, live POST), and the
    Databricks validator emits the same list shape for every task.

    A value Fabric computes at run time is never invented. A pipeline
    parameter is carried at its default, and flagged, because AIDP has no
    `@pipeline()` for a run to override it through.
    """
    raw = activity.get("parameters") or {}
    activity_name = activity.get("name") or ""
    if not isinstance(raw, dict):
        findings.append(Finding(
            "PL23_PARAMETER_EXPRESSION",
            f"activity {activity_name!r}: parameters {raw!r} is not a name -> value "
            f"mapping; no parameter is passed", "flag"))
        return []
    carried = []
    for name, spec in raw.items():
        value = spec.get("value") if isinstance(spec, dict) else spec
        declared = spec.get("type") if isinstance(spec, dict) else None
        expression = (value.get("value") if isinstance(value, dict)
                      and str(value.get("type") or "").casefold() == "expression" else None)
        if expression is None and isinstance(value, _LITERAL):
            carried.append({"name": str(name), "value": _as_text(value)})
            findings.append(Finding(
                "PL21_NOTEBOOK_PARAMETER",
                f"activity {activity_name!r}: parameter {name!r} = "
                f"{_as_text(value)!r}", "rewrite"))
            if _retyped(value, declared):
                findings.append(Finding(
                    "PL24_PARAMETER_RETYPED",
                    f"activity {activity_name!r}: parameter {name!r} is "
                    f"{_declared_type(value, declared)} in Fabric and an AIDP task "
                    f"parameter is a string, so it arrives as "
                    f"{_as_text(value)!r}; whether the notebook turns it back into "
                    f"a {_declared_type(value, declared)} depends on its parameters "
                    f"cell, which this translator does not read -- check that cell, "
                    f"or that the notebook copes with a string", "flag"))
            continue
        match = _PIPELINE_PARAMETER.match(expression) if isinstance(expression, str) else None
        reference = (match.group(1) or match.group(2)) if match else None
        declaration = (pipeline_parameters or {}).get(reference) if reference else None
        default = (declaration.get("defaultValue")
                   if isinstance(declaration, dict) else None)
        if reference and isinstance(default, _LITERAL):
            carried.append({"name": str(name), "value": _as_text(default)})
            findings.append(Finding(
                "PL22_PARAMETER_DEFAULT",
                f"activity {activity_name!r}: parameter {name!r} is {expression!r}; "
                f"it is fixed at the pipeline default {_as_text(default)!r}, and a "
                f"run that set {reference} differently must now edit the task "
                f"parameter by hand", "flag"))
            continue
        if reference:
            why = (f"pipeline parameter {reference!r} has no literal default"
                   if declaration is not None else
                   f"the pipeline declares no parameter {reference!r}")
        elif expression is not None:
            why = "Fabric evaluates it at run time and AIDP has no expression engine"
        else:
            why = f"{value!r} is not a literal"
        findings.append(Finding(
            "PL23_PARAMETER_EXPRESSION",
            f"activity {activity_name!r}: parameter {name!r} is "
            f"{expression if expression is not None else value!r}; {why}, so it is "
            f"not passed and the notebook uses its parameter-cell default", "flag"))
    return carried


def _edge_run_if(conditions):
    kind = frozenset(str(c).casefold() for c in conditions)
    if "completed" in kind and kind <= {"completed", "succeeded", "failed"}:
        return "ALL_DONE"  # Completed already covers Succeeded and Failed
    return _RUN_IF.get(kind)


def _run_if(activity, upstream_names):
    """The single AIDP runIf equal to this activity's Fabric edge conditions,
    over the edges the job actually keeps."""
    conditions = activity.get("depends_on_conditions") or {}
    kinds = {_edge_run_if(conditions.get(name) or ["Succeeded"]) for name in upstream_names}
    if not kinds:
        return "ALL_SUCCESS"
    if len(kinds) == 1 and None not in kinds:
        return next(iter(kinds))
    described = "; ".join("%s: %s" % (name, "/".join(conditions.get(name) or ["Succeeded"]))
                          for name in upstream_names)
    raise Blocked(
        f"activity {activity.get('name')!r} runs on {described}; AIDP runIf is "
        f"one condition over all upstream tasks, so this ordering has no "
        f"faithful equivalent")


# The fields of a Fabric schedule's `configuration`, in the order a person
# reads them; anything else Fabric writes follows, as written. A field may be
# spelled more than one way -- `weekdays` and `weekDays` are both accepted --
# and is one field: listed as two, a configuration carrying both read
# "on Monday, on Monday".
_SCHEDULE_FIELDS = ((("interval",), "every {} min"), (("times",), "at {}"),
                    (("weekdays", "weekDays"), "on {}"),
                    (("localTimeZoneId",), "time zone {}"),
                    (("startDateTime",), "from {}"), (("endDateTime",), "until {}"))


def _empty(value) -> bool:
    return value in (None, "", [])


def _describe_schedule(schedule):
    config = schedule.get("configuration") or {}
    parts = [f"{config.get('type') or 'untyped'} schedule"]
    known = {"type"}
    for spellings, form in _SCHEDULE_FIELDS:
        present = [key for key in spellings if not _empty(config.get(key))]
        if present:
            value = config[present[0]]
            parts.append(form.format(", ".join(map(str, value))
                                     if isinstance(value, list) else value))
            # The first spelling present is the field. A later one that
            # repeats it is dropped; one that disagrees is left to the
            # as-written tail, so taking the first never hides a value.
            known.update(key for key in spellings
                         if key not in present or config[key] == value)
        else:
            known.update(spellings)
    parts += [f"{key}={config[key]!r}" for key in config if key not in known]
    return " ".join(parts[:1]) + (": " + ", ".join(parts[1:]) if parts[1:] else "")


def _schedule_findings(pipeline):
    """One finding per Fabric schedule. The job itself is emitted unscheduled.

    Fabric keeps a pipeline's triggers in `.schedules`, and nothing carried
    them: PL_Params runs every 60 minutes in Fabric, and its job graded with
    no word that on AIDP it would never run by itself. The AIDP job payload
    has a `schedule` (timezoneId, quartzCronExpression, pauseStatus in the
    Databricks validator's job_converter), but no scheduled job has been
    created live from this tool, and a Fabric schedule does not map onto one
    Quartz expression without loss: a Cron interval is anchored at its start
    time, start/end bounds have no field, and Fabric time-zone ids are not
    checked against what AIDP accepts. So the schedule is described for a
    person to recreate, never emitted.

    A disabled schedule is `info`: Fabric does not run it either, so the
    unscheduled job behaves the same, but whoever re-enables it needs to know
    it existed.
    """
    findings = []
    if pipeline.get("schedules_error"):
        findings.append(Finding(
            "PL20_SCHEDULE",
            f"{pipeline['schedules_error']}; whether Fabric runs this pipeline on a "
            f"schedule is unknown -- check it in Fabric, since a job from it runs "
            f"only when started", "flag"))
    for schedule in pipeline.get("schedules") or []:
        if not isinstance(schedule, dict):
            continue
        described = _describe_schedule(schedule)
        if schedule.get("enabled") is False:
            findings.append(Finding(
                "PL20_SCHEDULE",
                f"{described} (disabled in Fabric); not recreated on AIDP", "info"))
            continue
        state = ("" if schedule.get("enabled") is True
                 else f" (enabled is {schedule.get('enabled')!r}, read as on)")
        findings.append(Finding(
            "PL20_SCHEDULE",
            f"Fabric runs this pipeline on a {described}{state}; the AIDP job "
            f"carries no schedule and runs only when started -- recreate the "
            f"schedule on the job", "flag"))
    return findings


def _stale_inventory():
    return Finding(
        "PL15_STALE_INVENTORY",
        "this inventory predates dependency conditions, activity policies or "
        "notebook parameters; every edge was read as Succeeded, every timeout "
        "as 12h and every task as passing no parameter -- re-run `inventory`",
        "flag")


def translate(pipeline, *, notebook_paths=None, cluster_key=None,
              timeout_seconds=DEFAULT_TIMEOUT_SECONDS) -> TranslationResult:
    """One pipeline record -> an AIDP job payload, or a blocked result.

    `notebook_paths` maps a Fabric notebook name to the path it was migrated
    to -- the equivalent of the Databricks validator's migration registry.
    """
    name = pipeline.get("name") or "pipeline"
    activities = [a for a in (pipeline.get("activities") or []) if isinstance(a, dict)]
    source = json.dumps(pipeline, indent=2, sort_keys=True)
    # First, and on every path: a blocked pipeline's hand-built job needs the
    # schedule as much as an emitted one.
    findings: list = _schedule_findings(pipeline)

    if not activities:
        # Two different facts arrive here as the same empty list, and they
        # need different actions. A pipeline with no activities is empty --
        # nothing to migrate, nothing wrong. A pipeline whose
        # pipeline-content.json would not open or would not parse has an
        # unknown number of activities, and telling the operator it is
        # empty sends them to look at the wrong thing: measured on a
        # truncated pipeline-content.json, the report read
        # `PL92_EMPTY_PIPELINE  pipeline 'Broken' has no activities`.
        if pipeline.get("content_error"):
            findings.append(Finding(
                "PL93_DEFINITION_UNREADABLE",
                f"pipeline {name!r} could not be read: "
                f"{pipeline['content_error']}. This is not an empty pipeline "
                f"-- how many activities it has is unknown, so nothing here "
                f"says what it does. Fix the export and run the inventory "
                f"again", "flag"))
        else:
            findings.append(Finding("PL92_EMPTY_PIPELINE",
                                    f"pipeline {name!r} has no activities",
                                    "flag"))
        return TranslationResult(source, "", findings)

    keys, lookup = _unique_keys(activities)
    paths = notebook_paths or {}
    tasks = []
    stale = False

    try:
        for index, activity in enumerate(activities):
            kind = str(activity.get("type") or "").casefold()
            activity_name = activity.get("name") or ""
            if kind not in NOTEBOOK_ACTIVITY_TYPES:
                raise Blocked(
                    f"activity {activity_name!r} is a {activity.get('type')}, which "
                    f"has no AIDP job equivalent; the whole pipeline is left for a "
                    f"human so that no job runs a subset of it")
            if str(activity.get("state") or "").casefold() == "inactive":
                raise Blocked(
                    f"activity {activity_name!r} is deactivated in Fabric (marked "
                    f"{activity.get('on_inactive') or 'Succeeded'}); an AIDP job has "
                    f"no inactive task, and emitting it would run it every time")
            notebook = activity.get("notebook") or ""
            path = paths.get(notebook)
            if not path and _GUID.match(notebook):
                # An all-zero workspaceId is Fabric's "this workspace" marker.
                same = set(str(activity.get("notebook_workspace_id") or "")) <= set("0-")
                where = ("is in this workspace but not in the exported folder; export "
                         "the whole workspace" if same and activity.get("notebook_workspace_id")
                         else "lives in another workspace; export and migrate that "
                         "workspace too")
                raise Blocked(
                    f"activity {activity_name!r} runs notebook id {notebook!r}, which "
                    f"is not a notebook in this export -- it {where}")
            if not path:
                raise Blocked(
                    f"activity {activity_name!r} runs notebook {notebook or '<unnamed>'!r}, "
                    f"which was not migrated; a workflow pointing at a notebook that "
                    f"is not there fails at run time, far from the cause")
            seconds, timeout_problem = _timeout(activity, timeout_seconds)
            if timeout_problem:
                findings.append(Finding("PL13_TIMEOUT", timeout_problem, "flag"))
            named = activity.get("depends_on") or []
            # Before the repeated-upstream block below, which ends the
            # translation: PL15 is emitted after the loop, and a block used to
            # return before the staleness of this activity was ever recorded.
            if named and ("depends_on_conditions" not in activity or "policy" not in activity):
                stale = True
            if "parameters" not in activity:
                stale = True
            if len(set(named)) != len(named):
                # The inventory now lists each upstream once, with its
                # entries' conditions united. One that repeats a name is
                # older and kept only the last entry's conditions: PL_DupEdge's
                # B (A on success, A on failure) read as A-on-failure, and ran
                # ALL_FAILED after two copies of the same edge. Older still,
                # it kept no conditions at all, and saying it kept the last
                # entry's would name evidence that is not there.
                repeated = sorted({u for u in named if named.count(u) > 1})
                if "depends_on_conditions" not in activity:
                    kept_what = ("this inventory records no dependency conditions "
                                 "at all, so which of those edges run on success "
                                 "and which on failure is unknown")
                else:
                    kept_what = ("this inventory kept only the last edge's "
                                 "conditions")
                raise Blocked(
                    f"activity {activity_name!r} depends on {repeated} more than "
                    f"once, and {kept_what} -- re-run `inventory`")
            kept = [u for u in named if u in lookup]
            if len(kept) != len(named):
                findings.append(Finding(
                    "PL16_DANGLING_EDGE",
                    f"activity {activity_name!r} depends on "
                    f"{sorted(set(named) - set(kept))}, which is not a single activity "
                    f"of this pipeline; that ordering is dropped", "flag"))
            parameters = _parameters(activity, pipeline.get("parameters"), findings)
            task = {
                "type": "NOTEBOOK_TASK",
                "taskKey": keys[index],
                "runIf": _run_if(activity, kept),
                **_retries(activity, findings),
                "notebookPath": path,
                "source": "WORKSPACE",
                "timeoutSeconds": seconds,
            }
            if parameters:
                task["parameters"] = parameters
            upstream = [lookup[u] for u in kept]
            if upstream:
                task["dependsOn"] = [{"taskKey": key} for key in upstream]
            if cluster_key:
                task["cluster"] = {"clusterKey": cluster_key}
            tasks.append(task)
            findings.append(Finding(
                "PL10_NOTEBOOK_TASK",
                f"activity {activity_name!r} -> NOTEBOOK_TASK {keys[index]!r}",
                "rewrite"))
            if upstream:
                findings.append(Finding(
                    "PL11_DEPENDS_ON",
                    f"{keys[index]!r} runs after {', '.join(upstream)}",
                    "rewrite"))
    except Blocked as exc:
        findings.append(Finding("PL90_UNSUPPORTED_ACTIVITY", str(exc), "flag"))
        # Carried on a blocked pipeline too: it is a fact about the
        # inventory, and whoever rebuilds this job by hand is working from
        # the same stale record.
        if stale:
            findings.append(_stale_inventory())
        return TranslationResult(source, "", findings)

    if stale:
        findings.append(_stale_inventory())
    if not cluster_key:
        findings.append(Finding(
            "PL12_NO_CLUSTER",
            "no cluster key supplied, so every task omits `cluster`; set one "
            "before this job can run", "flag"))

    if job_name(name) != name:
        findings.append(Finding(
            "PL14_JOB_NAME",
            f"job name {name!r} -> {job_name(name)!r}; AIDP job names are a letter "
            f"then letters, digits and underscores", "rewrite"))
    payload = {
        "name": job_name(name),
        "description": f"Migrated from the Fabric Data Pipeline {name!r}. "
                       f"Not execution-verified.",
        "maxConcurrentRuns": 1,
        # No job-level timeout: each task carries its own, and a job-level
        # one bounds the whole chain.
        "tasks": tasks,
    }
    return TranslationResult(source, json.dumps(payload, indent=2) + "\n", findings)
