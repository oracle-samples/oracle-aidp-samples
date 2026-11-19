"""Generated AIDP jobs for what Snowflake refreshes or schedules. Pure.

Snowflake keeps three kinds of object up to date by itself: a dynamic table
(refreshed to a TARGET_LAG from its defining query), a materialized view
(maintained on every change to its base table) and a task (a SQL body on a
SCHEDULE or after its PREDECESSORS). None of them has an AIDP object that
does the same. What does exist is a Delta table and a notebook job, so:

  * a dynamic table or a materialized view migrates as a TABLE SNAPSHOT of
    its current contents (the planner, plan/build.py), and its refresh is a
    generated notebook -- `INSERT OVERWRITE <target> <defining query>`, the
    query translated by the same view translator `ddl` uses and its
    references rewritten to the migrated names -- behind a job that is
    created MANUAL (no schedule), with the source cadence recorded as the
    intended one;
  * a task graph (a root task and everything after it through
    PREDECESSORS) becomes ONE job whose tasks run in dependency order. A
    SQL DML body over migrated tables is translated the same way; a CALL,
    Snowflake Scripting, MERGE/UPDATE, or a body over a stream or a table
    that is not migrating becomes a STUB notebook that raises when run.
    The SCHEDULE is parsed and recorded, a WHEN condition is recorded and
    not carried.

A query the translator cannot carry exactly, or one that reads an object
that is not migrating, gets NO notebook and a verdict naming why. A refresh
that runs wrong SQL over a real table is worse than one that does not exist.

This module decides the translation for both callers -- the planner, which
says `refresh generated` / `refresh NOT generated: <why>`, and the generator
-- so the two cannot disagree.
"""
from __future__ import annotations

import re

from snowflake_source.dialect import lexer

__all__ = ["SNAPSHOT_KINDS", "JOB_PREFIX", "NOTEBOOK_DIR",
           "build_generated_jobs", "interval_cron", "job_body",
           "lag_schedule", "parse_schedule", "refresh_query",
           "schedule_proposal", "snapshot_definition", "translate_task_body",
           "SCHEDULE_NOT_APPLIED", "generated_folder",
           "register_generated_jobs"]

# Planner label -> census kind, for the kinds that migrate as a snapshot.
SNAPSHOT_KINDS = {"dynamic table": "DYNAMIC_TABLE",
                  "materialized view": "MATERIALIZED_VIEW"}

# The header of the statement that defines one. Matched over CODE only, so a
# leading comment or a literal cannot stand in for it. Everything between
# the name and the first top-level AS (a dynamic table's TARGET_LAG,
# WAREHOUSE, REFRESH_MODE, CLUSTER BY ...) is options, not query.
_SNAPSHOT_HEADER = re.compile(
    r"(?is)^\s*create\s+(?:or\s+replace\s+)?"
    r"(?:(?:secure|transient)\s+)*"
    r"(?:materialized\s+view|dynamic\s+(?:iceberg\s+)?table)\b")


def _norm_parts(parts: list[tuple[str, str]]) -> str:
    return ".".join(name.lower() for _, name in parts)


def refresh_query(ddl: str | None, *, source_identifier: str,
                  source_database: str, source_schema: str, target_fqn: str,
                  name_map: dict[str, str]) -> dict:
    """The SELECT a refresh would run, or why there is none.

    `name_map` is source identifier -> target name for the objects that are
    MIGRATING, and nothing else: a reference to anything outside it has no
    table on the target to read, so the refresh is not generated.

    Returns {"generated", "select", "reason", "rules", "warnings", "reads"}.
    `select` is the translated query with every reference rewritten to a
    target name; `reads` is those target names.
    """
    if not ddl or not str(ddl).strip():
        return _refused("the defining query was not captured (re-run "
                        "`assess --capture-definitions` to keep it)")
    try:
        mask = lexer.code_only(str(ddl))
    except lexer.UnterminatedLiteral as exc:
        return _refused(f"the defining statement could not be scanned: {exc}")
    header = _SNAPSHOT_HEADER.match(mask)
    if not header:
        return _refused("the defining statement does not begin with CREATE "
                        "DYNAMIC TABLE or CREATE MATERIALIZED VIEW, so its "
                        "query cannot be located")
    # The view translator reads `create view <name> [(cols)] ... as <query>`.
    # Everything after the header is kept byte for byte.
    return _translate_as_view(
        "create view" + str(ddl)[header.end():],
        source_identifier=source_identifier, source_database=source_database,
        source_schema=source_schema, target_fqn=target_fqn,
        name_map=name_map, purpose="to refresh from")


# Snowflake-only sources the relation check cannot see: it finds tables and
# views after FROM/JOIN, not a stage, a table function or a session value.
# Matched over lexer.code_only text, so '@' or '$1' inside a literal or a
# quoted identifier is data and does not match.
_STAGE_REF = re.compile(r"@")
_TABLE_FUNCTION = re.compile(r"(?i)\btable\s*\(")
_DOLLAR_REF = re.compile(r"(?<![\w$])\$(\d+|[A-Za-z_]\w*)")


def snowflake_only_source(sql: str) -> str | None:
    """Why `sql` reads something only a Snowflake session has, or None.

    A staged file (`@stage/path`, `@~`, `@%table`), a table function
    (`TABLE(RESULT_SCAN(...))`, `TABLE(INFORMATION_SCHEMA.TASK_HISTORY())`),
    a positional column (`$1`, which exists only over a staged file or a
    VALUES list) and a session variable (`$name`) have no AIDP counterpart
    this generator could rewrite them to.
    """
    try:
        mask = lexer.code_only(str(sql))
    except lexer.UnterminatedLiteral:
        return None       # the caller's own scan reports the literal
    m = _STAGE_REF.search(mask)
    if m:
        token = re.match(r"@[^\s,;()]*", str(sql)[m.start():]).group(0)
        return (f"it reads the Snowflake stage {token}: staged files do not "
                f"migrate with the tables, so AIDP has nothing to read -- "
                f"rewrite it to read the file from an AIDP volume or Object "
                f"Storage")
    if _TABLE_FUNCTION.search(mask):
        return ("it reads a table function (TABLE(...), e.g. RESULT_SCAN or "
                "an INFORMATION_SCHEMA function), which exists only in a "
                "Snowflake session and has no AIDP table to rewrite it to")
    for m in _DOLLAR_REF.finditer(mask):
        ref = m.group(0)
        if m.group(1).isdigit():
            return (f"it reads the positional column {ref}, which exists "
                    f"only over a staged file or a VALUES list in Snowflake")
        return (f"it reads the session variable {ref}, which is set in a "
                f"Snowflake session; Spark has no such variable")
    return None


def _refused(reason: str, **extra) -> dict:
    return {"generated": False, "select": None, "reason": reason,
            "rules": extra.get("rules", []),
            "warnings": extra.get("warnings", []), "reads": [],
            "missing": extra.get("missing", [])}


def _translate_as_view(as_view: str, *, source_identifier: str,
                       source_database: str, source_schema: str,
                       target_fqn: str, name_map: dict[str, str],
                       purpose: str) -> dict:
    """Translate `create view ... as <query>` and return the query alone,
    every reference rewritten to a MIGRATING target, or why not."""
    # Imported here: target.ddl imports the planner's neighbours, and this
    # module is imported by the planner.
    from target.ddl import (_is_cte, _relation_spans, _view_text,
                            build_create_view)

    refused = _refused
    why = snowflake_only_source(as_view)
    if why:
        return refused(why)
    record = {"source_identifier": source_identifier,
              "source_database": source_database,
              "source_schema": source_schema,
              "view_ddl_get_ddl": as_view, "source_metadata": {},
              "columns": []}
    res = build_create_view(record, target_fqn, name_map)
    rules = [r.rule_id for r in res.rules_applied]
    if res.blocked:
        return refused(res.blocked_reason or "the query could not be "
                       "translated", rules=rules)
    select = _view_text(res.sql)
    if not select:
        return refused("the translated query could not be read back",
                       rules=rules, warnings=list(res.warnings))

    # Every relation left in the query must be a table the migration
    # creates. The translator names an unqualified leftover; a fully
    # qualified name outside the migration (another database, a table that
    # is blocked or excluded) is carried as written, and would be read on
    # the target as a table that does not exist.
    targets = {t.lower(): t for t in name_map.values()}
    ctes = lexer.cte_scopes(select)
    reads, missing = [], []
    for start, end, parts in _relation_spans(select):
        if len(parts) == 1 and _is_cte(select, start, end, ctes):
            continue
        key = _norm_parts(parts)
        if key in targets:
            if targets[key] not in reads:
                reads.append(targets[key])
        elif select[start:end] not in missing:
            missing.append(select[start:end])
    if missing:
        return refused(
            "it reads " + ", ".join(missing) + ", which "
            + ("is" if len(missing) == 1 else "are")
            + f" not migrating with it, so the target has no table {purpose}",
            rules=rules, warnings=list(res.warnings), missing=missing)
    return {"generated": True, "select": select, "reason": None,
            "rules": rules, "warnings": list(res.warnings), "reads": reads,
            "missing": []}


# ---------------------------------------------------------------------------
# Cadence. Recorded, never applied: every generated job is created MANUAL.
# Where the source cadence maps EXACTLY onto a Quartz cron -- the schedule
# shape the AIDP job API documents (`schedule: {quartzCronExpression,
# timezoneId, pauseStatus}`, aidp CLI create-job reference) -- the cron is
# written down as a PAUSED proposal for whoever decides to turn it on.
# ---------------------------------------------------------------------------

_INTERVAL = re.compile(r"(?i)^\s*(\d+)\s*(second|minute|hour|day)s?\s*$")


def interval_cron(count: int, unit: str) -> tuple[str | None, str | None]:
    """(Quartz cron, None) that fires every `count` `unit`s on the clock,
    or (None, why not). Only exact forms: a step that does not divide its
    field (every 7 minutes, every 5 hours) restarts at each hour or day
    boundary in cron, so it is NOT every N, and is refused."""
    unit = unit.lower().rstrip("s")
    if count <= 0:
        return None, f"an interval of {count} {unit}(s) is not a cadence"
    if unit == "second":
        if count % 60 == 0:
            return interval_cron(count // 60, "minute")
        if 60 % count == 0:
            return f"0/{count} * * * * ?", None
        return None, (f"{count} seconds does not divide the minute, so no "
                      f"cron fires exactly every {count} seconds")
    if unit == "minute":
        if count % 60 == 0:
            return interval_cron(count // 60, "hour")
        if 60 % count == 0:
            return f"0 0/{count} * * * ?", None
        return None, (f"{count} minutes does not divide the hour, so no "
                      f"cron fires exactly every {count} minutes")
    if unit == "hour":
        if count % 24 == 0:
            return interval_cron(count // 24, "day")
        if count == 1:
            return "0 0 * * * ?", None
        if 24 % count == 0:
            return f"0 0 0/{count} * * ?", None
        return None, (f"{count} hours does not divide the day, so no cron "
                      f"fires exactly every {count} hours")
    if unit == "day":
        if count == 1:
            return "0 0 0 * * ?", None
        return None, (f"every {count} days has no exact cron: a day-of-month "
                      f"step restarts at every month boundary")
    return None, f"unit {unit!r} is not one this reads"


def _proposal(cron: str | None, reason: str | None, *, timezone: str,
              basis: str) -> dict:
    return {"quartzCronExpression": cron, "timezoneId": timezone if cron
            else None, "pauseStatus": "PAUSED" if cron else None,
            "basis": basis, "reason": reason}


def lag_schedule(lag: str | None) -> dict:
    """A dynamic table's TARGET_LAG as a PAUSED Quartz proposal.

    A lag is a staleness bound, not a clock: Snowflake refreshed whenever it
    needed to stay within it. Refreshing once per lag interval keeps the
    target within about twice the lag, which is the honest reading of the
    proposal. UTC, because a lag has no time zone.
    """
    text = str(lag or "").strip()
    if text.upper() == "DOWNSTREAM":
        return _proposal(None, "TARGET_LAG DOWNSTREAM has no cadence of its "
                         "own: it refreshed when a dynamic table reading it "
                         "needed it", timezone="UTC", basis="TARGET_LAG")
    m = _INTERVAL.match(text)
    if not m:
        return _proposal(None, f"TARGET_LAG {text!r} is not an interval this "
                         f"reads" if text else "no TARGET_LAG was recorded",
                         timezone="UTC", basis="TARGET_LAG")
    cron, reason = interval_cron(int(m.group(1)), m.group(2))
    return _proposal(cron, reason, timezone="UTC",
                     basis=f"once per TARGET_LAG ({text}); a lag is a "
                           f"staleness bound, so this keeps the target "
                           f"within about twice it")


# ---------------------------------------------------------------------------
# Notebooks and job specs.
# ---------------------------------------------------------------------------

JOB_PREFIX = "snowmig_"
NOTEBOOK_DIR = "generated_jobs"

_MANUAL_NOTE = (
    "Created MANUAL: no schedule is sent, and the job starts nothing on its "
    "own. The intended cadence is recorded here, not applied. Run it by hand "
    "(`snowmig run --job {name}`), compare its output with the source, then "
    "add a schedule in the AIDP console (the PAUSED proposal below is the "
    "source cadence as a Quartz cron, where one exists).")


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(text).lower()).strip("_")


def _md(*lines: str) -> dict:
    return {"cell_type": "markdown", "metadata": {},
            "source": [line + "\n" for line in lines]}


def _code(*lines: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None,
            "outputs": [], "source": [line + "\n" for line in lines]}


def _notebook(cells: list[dict], meta: dict) -> dict:
    return {"cells": cells, "metadata": {"snowmig": {"generated": True,
                                                     **meta}},
            "nbformat": 4, "nbformat_minor": 5}


def _sql_cell(sql: str, label: str) -> dict:
    # The statement is data (repr), so nothing in it is read as Python.
    return _code(f"# Generated by `snowmig jobs`. {label}",
                 f"SQL = {sql!r}",
                 "print(SQL, flush=True)",
                 "spark.sql(SQL)",
                 "print('ok', flush=True)")


def snapshot_definition(rec: dict, facts: dict) -> str | None:
    """The statement that defines a snapshot's refresh, or None.

    A dynamic table's is only on SHOW DYNAMIC TABLES (`text`), which the
    census keeps; a materialized view's is on the same census row, or on
    the SHOW VIEWS / GET_DDL text the inventory keeps for every view.
    """
    return (facts.get("text") or rec.get("view_ddl_get_ddl")
            or rec.get("view_text_show"))


def _db_schema(rec: dict) -> tuple[str, str]:
    db, schema = rec.get("source_database"), rec.get("source_schema")
    if db is None or schema is None:
        db, schema, _ = str(rec["source_identifier"]).split(".", 2)
    return str(db), str(schema)


def _unique(name: str, taken: set[str]) -> str:
    out, n = name, 2
    while out in taken:
        out, n = f"{name}_{n}", n + 1
    taken.add(out)
    return out


def _refresh_jobs(plan: dict, inventory: dict, taken: set[str]
                  ) -> tuple[list[dict], list[dict], dict[str, dict]]:
    """(jobs, not_generated, notebooks) for the plan's table snapshots."""
    from target.ddl import _qualify

    can = plan.get("can_migrate") or []
    name_map = {c["source_identifier"]: c["target"] for c in can}
    by_id = {r["source_identifier"]: r
             for r in inventory.get("inventory") or []}
    census = plan.get("census") or inventory.get("census") or {}
    facts_by_id = {o.get("source_identifier"): o.get("source_facts") or {}
                   for o in census.get("objects") or []
                   if o.get("kind") in SNAPSHOT_KINDS.values()}
    jobs, skipped, notebooks = [], [], {}
    job_of: dict[str, str] = {}
    snapshots = [c for c in sorted(can, key=lambda c: c["source_identifier"])
                 if c.get("snapshot_of")]
    for c in snapshots:
        job_of[c["source_identifier"]] = _unique(
            f'{JOB_PREFIX}refresh_{_slug(c["target"])}', taken)
    for c in snapshots:
        ident, refresh = c["source_identifier"], c.get("refresh") or {}
        base = {"source_identifier": ident, "kind": "refresh",
                "snapshot_of": c["snapshot_of"], "target": c["target"]}
        if not refresh.get("generated"):
            skipped.append({**base, "verdict": refresh.get("verdict")
                            or "refresh NOT generated: the plan carries no "
                               "refresh verdict for it; re-run `plan`"})
            continue
        rec = by_id.get(ident)
        if rec is None:
            skipped.append({**base, "verdict": (
                "refresh NOT generated: the plan says `refresh generated`, "
                "but the inventory has no record of it; re-run `plan` over "
                "this inventory")})
            continue
        db, schema = _db_schema(rec)
        result = refresh_query(
            snapshot_definition(rec, facts_by_id.get(ident, {})),
            source_identifier=ident, source_database=db,
            source_schema=schema, target_fqn=c["target"], name_map=name_map)
        if not result["generated"]:
            skipped.append({**base, "verdict": (
                f"refresh NOT generated: the plan said `refresh generated`, "
                f"but over this inventory {result['reason']} -- re-run "
                f"`plan` so the approval and the generated job agree")})
            continue
        name = job_of[ident]
        sql = f"INSERT OVERWRITE TABLE {_qualify(c['target'])}\n{result['select']}"
        path = f"{NOTEBOOK_DIR}/{name}.ipynb"
        cadence = refresh.get("cadence")
        proposal = (lag_schedule(cadence.get("value")) if cadence
                    else None)
        mode = refresh.get("refresh_mode")
        notebooks[path] = _notebook([
            _md(f"# Refresh `{c['target']}`",
                "",
                f"Generated by `snowmig jobs` from the {c['snapshot_of']} "
                f"`{ident}`, which migrated as a table snapshot.",
                "",
                "**What it does:** a FULL, atomic `INSERT OVERWRITE` of the "
                "target from the translated defining query, reading the "
                "migrated tables" + (f" ({', '.join(result['reads'])})"
                                     if result["reads"] else "") + ".",
                "",
                "**What it does not do:** it is not incremental "
                + (f"(Snowflake's refresh mode was {mode}) " if mode else "")
                + "and it is not scheduled: " + refresh.get(
                    "cadence_note", "no cadence was recorded") + ".",
                "",
                "Verify its result against the source before relying on it: "
                + "; ".join(result["warnings"]) if result["warnings"] else
                "Verify its result against the source before relying on it."),
            _sql_cell(sql, f"Refresh of {c['target']} from {ident}."),
        ], {"kind": "refresh", "source": ident, "target": c["target"]})
        jobs.append({
            **base, "name": name, "trigger": "MANUAL",
            "schedule_applied": False,
            "note": _MANUAL_NOTE.format(name=name),
            "intended_cadence": cadence,
            "cadence_note": refresh.get("cadence_note"),
            "proposed_schedule": (proposal if proposal and
                                  proposal["quartzCronExpression"] else None),
            "proposed_schedule_note": (proposal or {}).get("reason")
            or (proposal or {}).get("basis"),
            "refresh_sql": sql,
            "reads": result["reads"],
            "run_after": sorted(job_of[s] for s in
                                refresh.get("reads_snapshots") or []
                                if s in job_of),
            "translation_warnings": result["warnings"],
            "max_concurrent_runs": 1,
            "tasks": [{"task_key": name, "notebook": path,
                       "depends_on": [], "generated": True}]})
    return jobs, skipped, notebooks


def job_body(job: dict, *, notebook_folder: str, cluster_key: str) -> dict:
    """The documented create-job body for one generated job. UNSCHEDULED by
    construction: no `schedule` key is ever emitted, whatever the spec
    proposes. One NOTEBOOK_TASK per generated task, in the live-verified
    task shape (target/provision_api.build_job_body); a task graph's order
    is the documented `dependsOn: [{taskKey}]` -- documented in the aidp CLI
    create-job schema, not yet live-verified on a multi-task job."""
    tasks = []
    for task in job["tasks"]:
        body = {"type": "NOTEBOOK_TASK", "taskKey": task["task_key"],
                "notebookPath": (f"{notebook_folder.rstrip('/')}/"
                                 f"{task['notebook'].rsplit('/', 1)[-1]}"),
                "source": "WORKSPACE", "runIf": "ALL_SUCCESS",
                "cluster": {"clusterKey": cluster_key}}
        if task.get("depends_on"):
            body["dependsOn"] = [{"taskKey": k} for k in task["depends_on"]]
        tasks.append(body)
    return {"name": job["name"],
            "maxConcurrentRuns": int(job.get("max_concurrent_runs") or 1),
            "tasks": tasks}


def build_generated_jobs(plan: dict, inventory: dict) -> dict:
    """plan.json + inventory.json -> the generated jobs. Pure; registers
    nothing. Returns {"jobs", "not_generated", "notebooks", "summary"};
    `notebooks` maps a path relative to the output directory to ipynb JSON.
    """
    taken: set[str] = set()
    jobs, skipped, notebooks = _refresh_jobs(plan, inventory, taken)
    task_jobs, task_notebooks, tasks_note = _task_jobs(plan, inventory, taken)
    notebooks.update(task_notebooks)
    tasks = [t for j in task_jobs for t in j["tasks"]]
    return {"jobs": jobs + task_jobs, "not_generated": skipped,
            "notebooks": notebooks,
            "summary": {"refresh_jobs": len(jobs),
                        "refresh_not_generated": len(skipped),
                        "task_jobs": len(task_jobs),
                        "tasks_translated": sum(1 for t in tasks
                                                if t["generated"]),
                        "task_stubs": sum(1 for t in tasks
                                          if not t["generated"]),
                        "tasks_note": tasks_note}}


# ---------------------------------------------------------------------------
# Tasks. One job per task graph; one notebook per task, translated or stub.
# ---------------------------------------------------------------------------

_CRON = re.compile(r"(?is)^\s*using\s+cron\s+(.*?)\s*$")
_CRON_FIELD = re.compile(r"^[0-9A-Za-z*,/\-]+$")
_DOW_NAMES = ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT")


def parse_schedule(text: str | None) -> dict | None:
    """SHOW TASKS `schedule` as recorded facts: an interval, a CRON with
    its time zone, or the raw text when neither reads. None when the task
    has no schedule (a child task runs after its predecessors)."""
    if text is None or not str(text).strip():
        return None
    raw = str(text).strip()
    out: dict = {"source": "SCHEDULE", "value": raw}
    m = _INTERVAL.match(raw)
    if m:
        out["interval"] = {"count": int(m.group(1)),
                           "unit": m.group(2).upper()}
        return out
    c = _CRON.match(raw)
    if c:
        fields = c.group(1).split()
        if len(fields) == 6:
            out["cron"] = {"expression": " ".join(fields[:5]),
                           "timezone": fields[5]}
            return out
    out["unparsed"] = True
    return out


def _dow_atom(atom: str) -> str | None:
    if atom.upper() in _DOW_NAMES:
        return atom.upper()
    if atom.isdigit() and 0 <= int(atom) <= 7:
        # cron: 0 (or 7) = Sunday; Quartz: 1 = Sunday.
        return str(int(atom) % 7 + 1)
    return None


def _quartz_dow(field: str) -> str | None:
    out = []
    for token in field.split(","):
        if "/" in token:
            return None
        atoms = token.split("-")
        if len(atoms) > 2:
            return None
        converted = [_dow_atom(a) for a in atoms]
        if None in converted:
            return None
        out.append("-".join(converted))
    return ",".join(out)


def _step(field: str, start: str) -> str:
    # `*/n` is "every n from the first value": Quartz spells it `first/n`.
    return f"{start}/{field[2:]}" if field.startswith("*/") else field


def cron_to_quartz(expression: str) -> tuple[str | None, str | None]:
    """A Snowflake CRON (`min hour dom month dow`) as a Quartz cron
    (`sec min hour dom month dow`), or (None, why not). Exact forms only."""
    fields = expression.split()
    if len(fields) != 5:
        return None, f"a CRON needs 5 fields, this has {len(fields)}"
    if not all(_CRON_FIELD.match(f) for f in fields):
        return None, "a field uses a character this conversion does not read"
    minute, hour, dom, month, dow = fields
    if any(ch in dom.upper() for ch in "LW?") or \
            any(ch in dow.upper() for ch in "L#?"):
        return None, ("L / W / # forms are not converted: their Quartz "
                      "meanings differ in detail")
    if dow == "*":
        dom_q, dow_q = _step(dom, "1"), "?"
    elif dom == "*":
        dom_q, dow_q = "?", _quartz_dow(dow)
        if dow_q is None:
            return None, f"the day-of-week field {dow!r} could not be converted"
    else:
        return None, ("both day-of-month and day-of-week are set: cron runs "
                      "when EITHER matches, and Quartz cannot say that")
    return (f"0 {_step(minute, '0')} {_step(hour, '0')} {dom_q} "
            f"{_step(month, '1')} {dow_q}"), None


def schedule_proposal(text: str | None) -> dict:
    """A task SCHEDULE as a PAUSED Quartz proposal. Recorded, never sent."""
    parsed = parse_schedule(text)
    if parsed is None:
        return _proposal(None, "the task has no schedule of its own",
                         timezone="UTC", basis="SCHEDULE")
    if "interval" in parsed:
        cron, reason = interval_cron(parsed["interval"]["count"],
                                     parsed["interval"]["unit"])
        return _proposal(cron, reason, timezone="UTC", basis=(
            f"every {parsed['value']}. A Snowflake interval counts from when "
            f"the task was resumed; this cron fires on the clock, in UTC"))
    if "cron" in parsed:
        cron, reason = cron_to_quartz(parsed["cron"]["expression"])
        return _proposal(cron, reason, timezone=parsed["cron"]["timezone"],
                         basis=f"the task's own CRON ({parsed['value']})")
    return _proposal(None, f"the schedule {parsed['value']!r} is neither an "
                     f"interval nor a USING CRON with a time zone",
                     timezone="UTC", basis="SCHEDULE")


# The verbs a task body is recognised by. Matched over CODE only.
_INSERT = re.compile(r"(?is)^\s*insert\s+(overwrite\s+)?into\b")
_DELETE = re.compile(r"(?is)^\s*delete\s+from\b")
# No trailing whitespace consumed: the name after it is read by
# census.name_at, which expects the gap before the name.
_TRUNCATE = re.compile(r"(?is)^\s*truncate(?:\s+table(?=\s))?"
                       r"(?:\s+if\s+exists(?=\s))?")
_CALL = re.compile(r"(?is)^\s*call\b")
_WHERE = re.compile(r"(?is)^\s*where\b")


def _stub(reason: str) -> dict:
    return {"generated": False, "sql": None, "verdict": f"stub: {reason}",
            "reads": [], "warnings": []}


_VERBATIM = ("SQL carried over unchanged: no dialect rule matched, and "
             "functions outside the rule table were NOT checked (they may "
             "not exist in Spark 3.5)")


def _view_gist(res: dict) -> str:
    """What the view translator did to a task's query, for its verdict."""
    if "R42_VIEW_PORTABLE_SQL" in res.get("rules", []):
        return _VERBATIM
    if any("not all exact" in w for w in res.get("warnings", [])):
        return "dialect-translated, NOT every rule exact (see the notebook)"
    return "dialect-translated by exact rules; verify against the source"


def _fragment_warnings(tr) -> tuple[list[str], str]:
    """Warnings and verdict gist for a fragment translated by translate_sql
    (a VALUES list, a DELETE condition)."""
    warnings = list(tr.warnings or [])
    if not tr.applied:
        return warnings + [_VERBATIM + "."], _VERBATIM
    caveats = [a["caveat"] for a in tr.applied if a.get("caveat")]
    if caveats:
        return (warnings + ["dialect-translated, not every rule exact: "
                            + "; ".join(caveats)],
                "dialect-translated, NOT every rule exact (see the notebook)")
    return warnings, ("dialect-translated by exact rules; verify against "
                      "the source")


def _stream_hint(missing: list[str], db: str, schema: str,
                 streams: dict[str, str]) -> str:
    from snowflake_source.extract.census import name_at

    hints = []
    for written in missing:
        got = name_at(" " + written.replace("`", '"'), 0)
        if not got:
            continue
        parts = got[0]
        full = ".".join([db, schema][:3 - len(parts)] + parts)
        if full in streams:
            hints.append(
                f"{written} is a STREAM on {streams[full]}: stream offsets "
                f"do not transfer. The nearest AIDP equivalent is the Delta "
                f"change data feed of the migrated {streams[full]} "
                f"(`table_changes`, once delta.enableChangeDataFeed is set "
                f"on it), which this generator does not write")
    return ("; " + "; ".join(hints)) if hints else ""


def _write_target(stmt: str, pos: int, db: str, schema: str,
                  name_map: dict[str, str]) -> tuple[str | None, str, int]:
    """(target name or None, source name, end offset) of the table a DML
    statement writes, resolved in the task's own schema."""
    from snowflake_source.extract.census import name_at

    got = name_at(stmt, pos)
    if not got or (len(got[0]) == 1 and got[0][0] in ("IDENTIFIER", "SET")):
        return None, "", pos
    parts, end = got
    full = ".".join([db, schema][:3 - len(parts)] + parts)
    return name_map.get(full), full, end


def _matching_paren(mask: str, start: int) -> int | None:
    depth = 0
    for i in range(start, len(mask)):
        if mask[i] == "(":
            depth += 1
        elif mask[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return None


def translate_task_body(body: str | None, *, task: str, db: str, schema: str,
                        name_map: dict[str, str],
                        streams: dict[str, str] | None = None) -> dict:
    """{generated, sql, verdict, reads} for one task body.

    Translated: INSERT [OVERWRITE] INTO <migrated table> [(cols)] SELECT /
    WITH / VALUES, DELETE FROM <migrated table> [WHERE <condition without a
    subquery>], TRUNCATE [TABLE] <migrated table> (carried as DELETE FROM).
    Everything else is a stub, and the verdict says which rule refused it.
    """
    from snowflake_source.dialect.translate import translate_sql
    from target.ddl import _qualify, _relation_spans

    streams = streams or {}
    if not body or not str(body).strip():
        return _stub("the task body was not captured -- re-run `assess "
                     "--capture-definitions` to keep task bodies, then "
                     "`plan` and `jobs` again")
    try:
        statements = lexer.split_statements(str(body))
    except lexer.UnterminatedLiteral as exc:
        return _stub(f"the body could not be scanned: {exc}")
    if not statements:
        return _stub("the body is empty")
    verb = lexer.leading_verb(statements[0])
    if verb in ("BEGIN", "DECLARE"):
        return _stub("Snowflake Scripting (BEGIN ... END / DECLARE) is "
                     "procedural and is not translated; rewrite it as "
                     "Spark SQL or Python in this notebook")
    if len(statements) != 1:
        return _stub(f"a body of {len(statements)} statements is Snowflake "
                     f"Scripting and is not translated")
    stmt = statements[0]
    mask = lexer.code_only(stmt)

    if _CALL.match(mask):
        got = _write_target(stmt, _CALL.match(mask).end(), db, schema, {})
        name = got[1] or "a procedure"
        return _stub(f"calls {name}, a stored procedure, which does not "
                     f"migrate (see CENSUS.md): its body has to be rewritten "
                     f"on AIDP before this task can run")
    if verb == "EXECUTE":
        return _stub("EXECUTE IMMEDIATE runs SQL built at run time, which "
                     "cannot be translated ahead of it")
    if verb in ("MERGE", "UPDATE"):
        return _stub(f"{verb} is not translated by this version: Delta runs "
                     f"{verb}, but its target, source and SET/ON clauses are "
                     f"not rewritten here, so it would run against the "
                     f"Snowflake names")

    def target_or_stub(pos):
        tgt, full, end = _write_target(stmt, pos, db, schema, name_map)
        if tgt is None:
            return None, (_stub(f"writes {full}, which is not migrating, so "
                                f"there is no target table to write")
                          if full else
                          _stub("the table it writes could not be read"))
        return (tgt, end), None

    m = _INSERT.match(mask)
    if m:
        got, stub = target_or_stub(m.end())
        if stub:
            return stub
        tgt, end = got
        rest_start = end + (len(stmt[end:]) - len(stmt[end:].lstrip()))
        cols = ""
        query_start = rest_start
        if mask[rest_start:rest_start + 1] == "(":
            close = _matching_paren(mask, rest_start)
            if close is None:
                return _stub("the INSERT's column list never closes")
            inner = stmt[rest_start + 1:close]
            if lexer.leading_verb(inner) not in ("SELECT", "WITH"):
                cols = stmt[rest_start:close + 1]
                query_start = close + 1
        query = stmt[query_start:].strip()
        qverb = lexer.leading_verb(query)
        if qverb == "VALUES":
            why = snowflake_only_source(query)
            if why:
                return _stub(why)
            tr = translate_sql(query)
            if tr.unsupported:
                return _stub("Snowflake-only SQL: " + "; ".join(
                    u["construct"] for u in tr.unsupported))
            if _relation_spans(tr.sql):
                return _stub("its VALUES reads a table, which is not "
                             "translated")
            select, reads = tr.sql, []
            warnings, gist = _fragment_warnings(tr)
        elif qverb in ("SELECT", "WITH"):
            res = _translate_as_view(
                "create view snowmig_task_query as " + query,
                source_identifier=task, source_database=db,
                source_schema=schema, target_fqn=tgt, name_map=name_map,
                purpose="to read")
            if not res["generated"]:
                return _stub(res["reason"] + _stream_hint(
                    res.get("missing") or [], db, schema, streams))
            select, reads = res["select"], res["reads"]
            warnings, gist = res["warnings"], _view_gist(res)
        else:
            return _stub("the INSERT's source is not a SELECT, WITH or "
                         "VALUES")
        if cols:
            tr = translate_sql(cols)
            if tr.unsupported:
                return _stub("its column list could not be translated")
            cols = " " + tr.sql
        head = ("INSERT OVERWRITE TABLE" if m.group(1) else "INSERT INTO")
        return {"generated": True, "reads": reads, "warnings": warnings,
                "sql": f"{head} {_qualify(tgt)}{cols}\n{select}",
                "verdict": "translated: " + ("INSERT OVERWRITE" if m.group(1)
                                             else "INSERT")
                           + f" into {tgt} -- {gist}"}

    m = _DELETE.match(mask)
    if m:
        got, stub = target_or_stub(m.end())
        if stub:
            return stub
        tgt, end = got
        rest = stmt[end:].strip()
        if not rest:
            return {"generated": True, "reads": [], "warnings": [],
                    "sql": f"DELETE FROM {_qualify(tgt)}",
                    "verdict": f"translated: DELETE from {tgt}"}
        where = _WHERE.match(lexer.code_only(rest))
        if not where:
            return _stub("a DELETE with USING or an alias is not translated")
        cond = rest[where.end():].strip()
        if re.search(r"(?i)\bselect\b", lexer.code_only(cond)):
            return _stub("a subquery in the DELETE condition is not "
                         "translated: its references would not be rewritten")
        why = snowflake_only_source(cond)
        if why:
            return _stub(why)
        tr = translate_sql(cond)
        if tr.unsupported:
            return _stub("Snowflake-only SQL: " + "; ".join(
                u["construct"] for u in tr.unsupported))
        warnings, gist = _fragment_warnings(tr)
        return {"generated": True, "reads": [], "warnings": warnings,
                "sql": f"DELETE FROM {_qualify(tgt)} WHERE {tr.sql}",
                "verdict": f"translated: DELETE from {tgt} -- {gist}"}

    m = _TRUNCATE.match(mask)
    if m and verb == "TRUNCATE":
        got, stub = target_or_stub(m.end())
        if stub:
            return stub
        tgt, _ = got
        return {"generated": True, "reads": [], "warnings": [],
                "sql": f"DELETE FROM {_qualify(tgt)}",
                "verdict": f"translated: TRUNCATE carried as DELETE FROM "
                           f"{tgt} -- the same rows go, in the form a Delta "
                           f"table takes"}

    return _stub(f"a {verb or 'non-SQL'} body is not translated")


def _task_notebook(job: str, task: dict, facts: dict) -> dict:
    ident = task["source_identifier"]
    body = str(facts.get("definition") or "")
    header = [f"# Task `{ident}`", "",
              f"Generated by `snowmig jobs` as a task of job `{job}`.", ""]
    if task["generated"]:
        warned = task.get("warnings") or []
        cells = [_md(*header, f"**{task['verdict']}.** The body was "
                     "translated with every table name rewritten to its "
                     "migrated target. Verify its effect against the source "
                     "before relying on it.", "",
                     *(["Translator warnings:", ""]
                       + [f"- {w}" for w in warned] + [""] if warned else []),
                     "Original Snowflake body:",
                     "", "```sql", body, "```"),
                 _sql_cell(task["sql"], f"Task {ident}.")]
    else:
        cells = [_md(*header, f"**STUB -- {task['verdict']}.**", "",
                     "Running this notebook FAILS on purpose: a stub that "
                     "succeeded would read as the task having run. Replace "
                     "the cell below with the rewritten body.", "",
                     "Original Snowflake body:", "", "```sql", body, "```"),
                 _code(f"# Generated by `snowmig jobs` as a STUB for {ident}.",
                       f"raise RuntimeError({task['verdict']!r})")]
    return _notebook(cells, {"kind": "task", "source": ident, "job": job})


def _task_jobs(plan: dict, inventory: dict, taken: set[str]
               ) -> tuple[list[dict], dict[str, dict], str | None]:
    census = plan.get("census") or inventory.get("census")
    if not census:
        return [], {}, ("no census in the plan or the inventory: tasks were "
                        "not read (`assess --no-census`?), so no task job is "
                        "generated")
    note = None
    task_kind = (census.get("kinds") or {}).get("TASK") or {}
    if task_kind.get("readable") is False:
        note = ("SHOW TASKS was not readable in every database "
                f"({task_kind.get('note', '')}); task jobs cover only the "
                f"tasks that were")
    name_map = {c["source_identifier"]: c["target"]
                for c in plan.get("can_migrate") or []}
    objects = census.get("objects") or []
    tasks = {o["source_identifier"]: o.get("source_facts") or {}
             for o in objects if o.get("kind") == "TASK"}
    streams = {o["source_identifier"]:
               str((o.get("source_facts") or {}).get("table_name") or "?")
               for o in objects if o.get("kind") == "STREAM"}

    notes: dict[str, list[str]] = {t: [] for t in tasks}
    parents: dict[str, list[str]] = {}
    for ident, facts in sorted(tasks.items()):
        known = []
        if "predecessors_unread" in facts:
            notes[ident].append(
                f"{ident}: its predecessors could not be read "
                f"({facts['predecessors_unread']!r}), so it is generated as "
                f"a root -- its place in the task graph is unknown")
        for parent in facts.get("predecessors") or []:
            if parent in tasks:
                known.append(parent)
            else:
                notes[ident].append(
                    f"{ident} runs after {parent}, which is not in the "
                    f"census (another database, or not visible to the "
                    f"role), so this job's order stops at {ident}")
        parents[ident] = known

    # Components: a task graph is everything reachable through
    # predecessor edges, either way.
    group = {t: t for t in tasks}

    def find(t):
        while group[t] != t:
            group[t] = group[group[t]]
            t = group[t]
        return t

    for child, ps in parents.items():
        for p in ps:
            group[find(child)] = find(p)
    components: dict[str, list[str]] = {}
    for t in sorted(tasks):
        components.setdefault(find(t), []).append(t)

    jobs, notebooks = [], {}
    for members in sorted(components.values()):
        # Dependency order, ties by name. A cycle cannot exist in Snowflake;
        # if one is read, its members are appended and said.
        order, placed = [], set()
        pending = sorted(members)
        while pending:
            ready = [t for t in pending
                     if all(p in placed for p in parents[t])]
            if not ready:
                order += pending
                notes[pending[0]].append(
                    "a predecessor cycle was read among " + ", ".join(pending)
                    + "; they are listed in name order")
                break
            for t in ready:
                order.append(t)
                placed.add(t)
            pending = [t for t in pending if t not in placed]
        roots = [t for t in order if not parents[t]]
        root = roots[0] if roots else order[0]
        name = _unique(f"{JOB_PREFIX}task_{_slug(root)}", taken)
        keys: dict[str, str] = {}
        key_taken: set[str] = set()
        for t in order:
            keys[t] = _unique(_slug(t.rsplit(".", 1)[-1]) or "task",
                              key_taken)
        job_tasks = []
        for t in order:
            facts = tasks[t]
            db, schema = t.split(".", 2)[:2]
            result = translate_task_body(
                facts.get("definition"), task=t, db=db, schema=schema,
                name_map=name_map, streams=streams)
            path = f"{NOTEBOOK_DIR}/{name}__{keys[t]}.ipynb"
            entry = {"task_key": keys[t], "source_identifier": t,
                     "notebook": path,
                     "depends_on": [keys[p] for p in parents[t]],
                     "generated": result["generated"],
                     "verdict": result["verdict"], "sql": result["sql"],
                     "reads": result["reads"],
                     "warnings": result.get("warnings") or [],
                     "source_state": facts.get("state")}
            notebooks[path] = _task_notebook(name, entry, facts)
            job_tasks.append(entry)
        rfacts = tasks[root]
        job_notes = [n for t in order for n in notes[t]]
        if len(roots) > 1:
            job_notes.append("more than one root task was read in this graph "
                             "(" + ", ".join(roots) + "); the schedule "
                             "recorded is " + root + "'s")
        for t in order:
            cond = tasks[t].get("condition")
            if cond:
                job_notes.append(
                    f"{t} runs only WHEN {cond} in Snowflake. The condition "
                    f"is recorded and NOT carried: the generated job runs "
                    f"unconditionally when triggered")
            overlap = str(tasks[t].get("allow_overlapping_execution")
                          or "").lower()
            if overlap == "true":
                job_notes.append(
                    f"{t} allowed overlapping execution in Snowflake; the "
                    f"generated job allows one run at a time "
                    f"(maxConcurrentRuns 1)")
            rel = tasks[t].get("task_relations")
            if rel:
                try:
                    import json as _json
                    fin = (_json.loads(rel) if isinstance(rel, str)
                           else rel).get("FinalizerTask")
                except (ValueError, AttributeError):
                    fin = None
                if fin:
                    job_notes.append(
                        f"{t} has the finalizer task {fin}, which runs after "
                        f"the graph whatever its outcome; it is recorded and "
                        f"NOT carried")
        proposal = schedule_proposal(rfacts.get("schedule"))
        jobs.append({
            "name": name, "kind": "task_graph", "root": root,
            "source_identifiers": order, "trigger": "MANUAL",
            "schedule_applied": False,
            "note": _MANUAL_NOTE.format(name=name),
            "intended_schedule": parse_schedule(rfacts.get("schedule")),
            "proposed_schedule": (proposal if proposal["quartzCronExpression"]
                                  else None),
            "proposed_schedule_note": proposal.get("reason")
            or proposal.get("basis"),
            "condition": rfacts.get("condition"),
            "warehouse": rfacts.get("warehouse"),
            "source_state": rfacts.get("state"),
            "max_concurrent_runs": 1,
            "notes": job_notes,
            "tasks": job_tasks})
    return jobs, notebooks, note


# ---------------------------------------------------------------------------
# Registration: only on `snowmig jobs --register`. Through the provisioning
# calls `provision` uses, and never with a schedule.
# ---------------------------------------------------------------------------

SCHEDULE_NOT_APPLIED = (
    "recorded in generated_jobs.json, not applied: every job was created "
    "without a schedule, so none of them runs until someone runs it or "
    "adds a schedule in the AIDP console")


def generated_folder() -> str:
    """Where the notebooks go on the workspace: beside the stage scripts."""
    from target.provisioning import SCRIPTS_FOLDER
    return f"{SCRIPTS_FOLDER.rsplit('/', 1)[0]}/{NOTEBOOK_DIR}"


def register_generated_jobs(call, *, workspace: str, cluster_key: str,
                            jobs: list[dict], notebook_dir,
                            folder: str | None = None) -> dict:
    """Create `jobs` in AIDP, unscheduled. Returns what happened, per job.

    A job whose name is already taken is NOT adopted and NOT overwritten.
    Every notebook is uploaded, then the folder is read back once; a job
    whose notebook failed to upload, or is not visible, is not created (it
    would point at an earlier run's notebook, or at nothing). Creates are confirmed by one `list_jobs` read-back: a 2xx is
    not the claim.
    """
    import pathlib

    folder = folder or generated_folder()
    res = {"folder": folder, "created": [], "unconfirmed": [],
           "name_taken": [], "failed": [], "steps": [],
           "schedule": SCHEDULE_NOT_APPLIED}

    def step(what, outcome, detail):
        res["steps"].append({"step": what, "outcome": outcome,
                             "detail": detail})

    try:
        existing = call("list_jobs", workspace=workspace).get("items") or []
    except Exception as exc:
        step("list_jobs", "failed", f"{str(exc)[:200]}; a name clash could "
             f"not be ruled out, so nothing was created")
        res["failed"] = [j["name"] for j in jobs]
        return res
    taken = {str(i.get("displayName") or i.get("name") or "").lower()
             for i in existing}
    todo = []
    for job in jobs:
        if job["name"].lower() in taken:
            res["name_taken"].append(job["name"])
            step("job", "name_taken", f'{job["name"]} already exists and was '
                 f"NOT adopted or overwritten; rename or delete it in the "
                 f"console, then re-run")
        else:
            todo.append(job)
    if not todo:
        return res

    try:
        call("create_ws_folder", workspace=workspace, path=folder)
    except Exception as exc:
        # A folder that exists may refuse the create; the listing decides.
        step("folder", "create_failed_or_exists", f"{folder}: {str(exc)[:160]}")
    base = pathlib.Path(notebook_dir)
    # An upload error wins over the listing (as in provisioning.py): a
    # same-named notebook left by an earlier run is visible in the folder,
    # but it is not what this run generated.
    upload_errors: dict[str, str] = {}
    for job in todo:
        for task in job["tasks"]:
            leaf = task["notebook"].rsplit("/", 1)[-1]
            remote = f"{folder}/{leaf}"
            try:
                call("upload_ws_file", workspace=workspace, path=remote,
                     local_path=str(base / task["notebook"]),
                     object_type="NOTEBOOK")
            except Exception as exc:
                upload_errors[leaf] = str(exc)[:160]
                step("notebook", "upload_failed", f"{remote}: {str(exc)[:160]}")
    try:
        listed = call("list_ws_objects", workspace=workspace,
                      path=folder).get("items") or []
    except Exception as exc:
        listed = []
        step("notebook", "read_back_failed", f"{folder}: {str(exc)[:160]}")
    seen = {str(i.get("path") or "").rsplit("/", 1)[-1] for i in listed} | {
        str(i.get("displayName") or "") for i in listed}

    requested = []
    for job in todo:
        leaves = [t["notebook"].rsplit("/", 1)[-1] for t in job["tasks"]]
        not_uploaded = [n for n in leaves if n in upload_errors]
        if not_uploaded:
            res["failed"].append(job["name"])
            step("job", "not_created", f'{job["name"]}: notebook upload '
                 f'failed ({", ".join(not_uploaded)}); a same-named notebook '
                 f"already in the folder would be an earlier run's content, "
                 f"so the job was not created over it")
            continue
        missing = [n for n in leaves if n not in seen]
        if missing:
            res["failed"].append(job["name"])
            step("job", "not_created", f'{job["name"]}: notebook(s) not '
                 f'visible on the workspace ({", ".join(missing)}), so the '
                 f"job was not pointed at nothing")
            continue
        body = job_body(job, notebook_folder=folder, cluster_key=cluster_key)
        try:
            call("create_job", workspace=workspace, body=body)
            requested.append(job["name"])
        except Exception as exc:
            res["failed"].append(job["name"])
            step("job", "failed", f'{job["name"]}: {str(exc)[:200]}')
    if requested:
        try:
            now = {str(i.get("displayName") or i.get("name") or "").lower()
                   for i in call("list_jobs",
                                 workspace=workspace).get("items") or []}
        except Exception as exc:
            now = set()
            step("job", "read_back_failed", str(exc)[:200])
        for name in requested:
            if name.lower() in now:
                res["created"].append(name)
                step("job", "created", f"{name} (unscheduled)")
            else:
                res["unconfirmed"].append(name)
                step("job", "create_requested", f"{name}: not visible in "
                     f"the job listing yet -- NOT confirmed")
    return res
