"""Tests for dbx_export/param_analysis.py + its job_converter wiring.

Q1: shared widget params (read-but-undeclared) hoist to job level.
Q2: dbutils.jobs.taskValues set/get wire to {{tasks.X.values.K}} references.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # dbx_export

from dbx_export.param_analysis import analyze_task_params  # noqa: E402
from dbx_export.job_converter import convert_dbx_job, _merge_params  # noqa: E402


def _nb(*cells):
    return {"cells": [{"cell_type": "code", "source": c} for c in cells]}


def _settings(tasks, parameters=None):
    return {"name": "job", "parameters": parameters or [], "tasks": tasks}


# ── Q1: widget hoist ────────────────────────────────────────────────────────

def test_read_but_undeclared_widget_not_hoisted():
    # Q1 hoist REMOVED (2026-08-05): a read-but-undeclared widget is NOT hoisted
    # to job level. Hoisting it created a job-level param value that OVERRIDES the
    # widget default (job params beat widget defaults), silently changing behaviour
    # (e.g. forcing SCORING_MODE='dl_only'). Undeclared reads now raise on AIDP
    # like Databricks and the notebook handles them; job_level_params stays empty.
    s = _settings([
        {"task_key": "a", "notebook_task": {"notebook_path": "/a"}},
        {"task_key": "b", "notebook_task": {"notebook_path": "/b"},
         "depends_on": [{"task_key": "a"}]},
    ])
    nbs = {"a": _nb('x = dbutils.widgets.get("model_name")'),
           "b": _nb('y = dbutils.widgets.get("model_name")')}
    plan = analyze_task_params(s, nbs)
    assert "model_name" not in plan.job_level_params


def test_declared_widget_not_hoisted():
    # base_parameters declaration → self-sufficient → not hoisted
    s = _settings([{"task_key": "a",
                    "notebook_task": {"notebook_path": "/a",
                                      "base_parameters": {"decision_path": ""}}}])
    nbs = {"a": _nb('d = dbutils.widgets.get("decision_path")')}
    plan = analyze_task_params(s, nbs)
    assert "decision_path" not in plan.job_level_params


def test_widgets_text_declaration_not_hoisted():
    # Q1 hoist REMOVED (2026-08-05): task A declares env via widgets.text("env","prod")
    # (self-sufficient in A); task B reads env undeclared. Previously B's read hoisted
    # env to job level with A's default — now nothing is hoisted (B's undeclared read
    # raises on AIDP like Databricks). job_level_params stays empty for env.
    s = _settings([
        {"task_key": "a", "notebook_task": {"notebook_path": "/a"}},
        {"task_key": "b", "notebook_task": {"notebook_path": "/b"}},
    ])
    nbs = {"a": _nb('dbutils.widgets.text("env", "prod")\ne = dbutils.widgets.get("env")'),
           "b": _nb('e = dbutils.widgets.get("env")')}
    plan = analyze_task_params(s, nbs)
    assert "env" not in plan.job_level_params


def test_existing_job_param_not_rehoisted():
    s = _settings([{"task_key": "a", "notebook_task": {"notebook_path": "/a"}}],
                  parameters=[{"name": "model_name", "value": "v"}])
    nbs = {"a": _nb('m = dbutils.widgets.get("model_name")')}
    plan = analyze_task_params(s, nbs)
    assert "model_name" not in plan.job_level_params


# ── Q2: taskValues wiring ───────────────────────────────────────────────────

def test_taskvalues_set_get_wires_reference():
    s = _settings([
        {"task_key": "setter", "notebook_task": {"notebook_path": "/s"}},
        {"task_key": "reader", "notebook_task": {"notebook_path": "/r"},
         "depends_on": [{"task_key": "setter"}]},
    ])
    nbs = {
        "setter": _nb('dbutils.jobs.taskValues.set("fl_score_table", "db.fl")'),
        "reader": _nb('v = dbutils.jobs.taskValues.get(taskKey="setter", key="fl_score_table")'),
    }
    plan = analyze_task_params(s, nbs)
    assert plan.task_extra_params["reader"] == [
        {"name": "fl_score_table", "value": "{{tasks.setter.values.fl_score_table}}"}]
    assert plan.task_extra_params["setter"] == [{"name": "fl_score_table", "value": ""}]


def test_taskvalues_get_without_taskkey_uses_setter_map():
    s = _settings([
        {"task_key": "setter", "notebook_task": {"notebook_path": "/s"}},
        {"task_key": "reader", "notebook_task": {"notebook_path": "/r"},
         "depends_on": [{"task_key": "setter"}]},
    ])
    nbs = {
        "setter": _nb('dbutils.jobs.taskValues.set("k", "v")'),
        "reader": _nb('v = dbutils.jobs.taskValues.get(key="k")'),  # no taskKey
    }
    plan = analyze_task_params(s, nbs)
    assert plan.task_extra_params["reader"][0]["value"] == "{{tasks.setter.values.k}}"


def test_taskvalues_dynamic_key_warns_no_wiring():
    s = _settings([{"task_key": "reader", "notebook_task": {"notebook_path": "/r"}}])
    nbs = {"reader": _nb('kv = "fl"\nv = dbutils.jobs.taskValues.get(taskKey="x", key=kv)')}
    plan = analyze_task_params(s, nbs)
    assert "reader" not in plan.task_extra_params      # nothing wired
    assert any("non-literal" in w for w in plan.warnings)


def test_taskvalues_missing_dependency_warns():
    s = _settings([
        {"task_key": "setter", "notebook_task": {"notebook_path": "/s"}},
        {"task_key": "reader", "notebook_task": {"notebook_path": "/r"}},  # no depends_on
    ])
    nbs = {
        "setter": _nb('dbutils.jobs.taskValues.set("k", "v")'),
        "reader": _nb('v = dbutils.jobs.taskValues.get(taskKey="setter", key="k")'),
    }
    plan = analyze_task_params(s, nbs)
    assert any("not a (transitive) upstream task" in w for w in plan.warnings)


def test_taskvalues_transitive_dependency_no_warn():
    # A → B → C chain; C reads a value A produced. A is a TRANSITIVE ancestor of
    # C, so the reference resolves — no warning (regression on direct-dep check).
    s = _settings([
        {"task_key": "A", "notebook_task": {"notebook_path": "/a"}},
        {"task_key": "B", "notebook_task": {"notebook_path": "/b"},
         "depends_on": [{"task_key": "A"}]},
        {"task_key": "C", "notebook_task": {"notebook_path": "/c"},
         "depends_on": [{"task_key": "B"}]},
    ])
    nbs = {
        "A": _nb('dbutils.jobs.taskValues.set("k", "v")'),
        "B": _nb('pass'),
        "C": _nb('x = dbutils.jobs.taskValues.get(taskKey="A", key="k")'),
    }
    plan = analyze_task_params(s, nbs)
    assert not any("upstream task" in w for w in plan.warnings)
    assert plan.task_extra_params["C"][0]["value"] == "{{tasks.A.values.k}}"


# ── _merge_params ───────────────────────────────────────────────────────────

def test_merge_params_reference_overrides_empty_base():
    base = [{"name": "k", "value": ""}]
    out = _merge_params(base, [{"name": "k", "value": "{{tasks.s.values.k}}"}])
    assert out == [{"name": "k", "value": "{{tasks.s.values.k}}"}]


def test_merge_params_adds_new_and_preserves_base_value():
    base = [{"name": "keep", "value": "real"}]
    out = _merge_params(base, [{"name": "new", "value": ""}])
    assert {"name": "keep", "value": "real"} in out
    assert {"name": "new", "value": ""} in out


# ── integration: full convert_dbx_job payload ───────────────────────────────

def test_convert_dbx_job_applies_plan():
    s = _settings([
        {"task_key": "serve_cache",
         "notebook_task": {"notebook_path": "/x/sc", "base_parameters": {"decision_path": ""}}},
        {"task_key": "FL_table_scoring", "notebook_task": {"notebook_path": "/x/fl"},
         "depends_on": [{"task_key": "serve_cache"}]},
        {"task_key": "agg_scores", "notebook_task": {"notebook_path": "/x/agg"},
         "depends_on": [{"task_key": "FL_table_scoring"}]},
    ])
    job_data = {"job_id": "1", "settings": s}
    nbs = {
        "serve_cache": _nb('m = dbutils.widgets.get("model_name")'),
        "FL_table_scoring": _nb('u = dbutils.widgets.get("unique_identifier")\n'
                                'dbutils.jobs.taskValues.set("fl_score_table", "db.fl")'),
        "agg_scores": _nb('v = dbutils.jobs.taskValues.get(taskKey="FL_table_scoring", '
                          'key="fl_score_table", debugValue="test1")'),
    }
    plan = analyze_task_params(s, nbs)
    payload = convert_dbx_job(job_data, "/Workspace/ai", "ai_tool", {},
                              aidp_default_cluster_key="ck", param_plan=plan)
    jp = {p["name"]: p["value"] for p in payload["parameters"]}
    assert jp == {}   # Q1 removed 2026-08-05: undeclared reads (model_name, unique_identifier) no longer hoisted
    tasks = {t["taskKey"]: t for t in payload["tasks"]}
    assert {"name": "fl_score_table", "value": ""} in tasks["FL_table_scoring"]["parameters"]
    assert {"name": "fl_score_table",
            "value": "{{tasks.FL_table_scoring.values.fl_score_table}}"} \
        in tasks["agg_scores"]["parameters"]
    assert {"name": "decision_path", "value": ""} in tasks["serve_cache"]["parameters"]


def test_undeclared_read_does_not_hoist_or_strip_task_param():
    # Q1 hoist REMOVED (2026-08-05): serve_cache declares model_name (empty) in
    # base_parameters; a downstream task reads it undeclared. Previously the read
    # hoisted model_name to job level and serve_cache's empty duplicate was
    # stripped. Now nothing is hoisted, so model_name stays OUT of job-level and
    # serve_cache KEEPS its own base_parameters entry (nothing shadows it).
    s = _settings([
        {"task_key": "serve_cache",
         "notebook_task": {"notebook_path": "/sc", "base_parameters": {"model_name": ""}}},
        {"task_key": "fl", "notebook_task": {"notebook_path": "/fl"},
         "depends_on": [{"task_key": "serve_cache"}]},
    ])
    nbs = {"serve_cache": _nb('m = dbutils.widgets.get("model_name")'),
           "fl": _nb('m = dbutils.widgets.get("model_name")')}
    plan = analyze_task_params(s, nbs)
    payload = convert_dbx_job({"job_id": "1", "settings": s}, "/Workspace/ai", "ai_tool", {},
                              aidp_default_cluster_key="ck", param_plan=plan)
    assert all(p["name"] != "model_name" for p in payload["parameters"])    # NOT hoisted
    sc = [t for t in payload["tasks"] if t["taskKey"] == "serve_cache"][0]
    assert {"name": "model_name", "value": ""} in sc["parameters"]          # kept (not stripped)


def test_nonempty_task_override_kept_even_if_hoisted():
    # A real per-task value (not an empty placeholder) is a genuine override
    # and must survive the shadow-strip.
    s = _settings([
        {"task_key": "a",
         "notebook_task": {"notebook_path": "/a", "base_parameters": {"model_name": "special"}}},
        {"task_key": "b", "notebook_task": {"notebook_path": "/b"},
         "depends_on": [{"task_key": "a"}]},
    ])
    nbs = {"a": _nb('m = dbutils.widgets.get("model_name")'),
           "b": _nb('m = dbutils.widgets.get("model_name")')}
    plan = analyze_task_params(s, nbs)
    payload = convert_dbx_job({"job_id": "1", "settings": s}, "/Workspace/ai", "ai_tool", {},
                              aidp_default_cluster_key="ck", param_plan=plan)
    a = [t for t in payload["tasks"] if t["taskKey"] == "a"][0]
    assert {"name": "model_name", "value": "special"} in a["parameters"]    # override kept


def test_convert_dbx_job_no_plan_is_backwards_compatible():
    s = _settings([{"task_key": "a",
                    "notebook_task": {"notebook_path": "/a", "base_parameters": {"p": "1"}}}])
    payload = convert_dbx_job({"job_id": "1", "settings": s}, "/Workspace/ai", "ai_tool", {},
                              aidp_default_cluster_key="ck")   # no param_plan
    assert payload["parameters"] == []
    assert payload["tasks"][0]["parameters"] == [{"name": "p", "value": "1"}]


def test_convert_dbx_job_rewrites_parameter_root_for_exported_workspace_file():
    source_root = (
        "/Workspace/Users/example.user@example.com/EXRISK/DL V2/artifacts"
    )
    source_file = (
        "/Users/example.user@example.com/EXRISK/DL V2/artifacts/"
        "scaling_files/Feature_List.csv"
    )
    target_root = (
        "/Workspace/ai_tool_exported/notebooks/Users/"
        "example.user@example.com/EXRISK/DL_V2/artifacts"
    )
    target_file = target_root + "/scaling_files/Feature_List.csv"
    settings = _settings([{
        "task_key": "score",
        "notebook_task": {
            "notebook_path": "/Jobs/score",
            "base_parameters": {
                "model_scaling_files_path": source_root,
                "unrelated": "/Workspace/Users/u/keep",
            },
        },
    }])

    payload = convert_dbx_job(
        {"job_id": "1", "settings": settings},
        "/Workspace/ai_tool_exported",
        "ai_tool",
        {},
        aidp_default_cluster_key="cluster",
        workspace_resource_path_map={source_file: target_file},
    )
    parameters = {
        item["name"]: item["value"]
        for item in payload["tasks"][0]["parameters"]
    }

    assert parameters["model_scaling_files_path"] == target_root
    assert parameters["unrelated"] == "/Workspace/Users/u/keep"


# ── Review #44: setter-declaration strip (MAJOR) regression ─────────────────
# A key that is BOTH a taskValues setter output AND a hoisted widget name must
# not lose its setter declaration. Two independent guards:
#   Fix A (job_converter): shadow-strip runs BEFORE Q2 extras are merged.
#   Fix B (param_analysis): setter keys are excluded from the Q1 hoist.

def test_fixA_q2_extra_survives_even_if_name_in_job_level():
    # Direct convert_dbx_task: a Q2 setter declaration ({k: ""}) must survive
    # even when its name is in job_level_names (the strip must not reach it).
    from dbx_export.job_converter import convert_dbx_task
    t = {"task_key": "s", "notebook_task": {"notebook_path": "/s", "base_parameters": {}}}
    conv = convert_dbx_task(t, "/W", {}, extra_params=[{"name": "k", "value": ""}],
                            job_level_names={"k"})
    assert {"name": "k", "value": ""} in conv["parameters"]   # Fix A: not stripped


def test_fixB_widget_fallback_setter_key_not_hoisted_and_survives():
    # Trigger 1: A sets fl_score_table; B reads it via the fallback pattern
    # (taskValues.get(...) or widgets.get("fl_score_table")). The widget read
    # must NOT hoist the setter key, and the setter declaration must survive.
    s = _settings([
        {"task_key": "A", "notebook_task": {"notebook_path": "/a"}},
        {"task_key": "B", "notebook_task": {"notebook_path": "/b"},
         "depends_on": [{"task_key": "A"}]},
    ])
    nbs = {
        "A": _nb('dbutils.jobs.taskValues.set("fl_score_table", "x")'),
        "B": _nb('v = dbutils.jobs.taskValues.get(taskKey="A", key="fl_score_table") '
                 'or dbutils.widgets.get("fl_score_table")'),
    }
    plan = analyze_task_params(s, nbs)
    assert "fl_score_table" not in plan.job_level_params          # Fix B: not hoisted
    payload = convert_dbx_job({"job_id": "1", "settings": s}, "/W", "ai_tool", {},
                              aidp_default_cluster_key="ck", param_plan=plan)
    tasks = {t["taskKey"]: t["parameters"] for t in payload["tasks"]}
    assert {"name": "fl_score_table", "value": ""} in tasks["A"]  # setter decl survives
    assert {"name": "fl_score_table",
            "value": "{{tasks.A.values.fl_score_table}}"} in tasks["B"]  # reader ref


def test_fixB_reexport_migrated_reader_getparameter_of_setter_key():
    # Trigger 2: re-export of ALREADY-MIGRATED notebooks. A's setter is the
    # migrated form (oidlUtils...setTaskValue) and its output declaration lives
    # in base_parameters; B reads via getParameter (matches the widget-read
    # regex). fl_score_table must be recognized as a setter key → excluded from
    # the Q1 hoist → A's base declaration is NOT stripped.
    s = _settings([
        {"task_key": "A", "notebook_task": {"notebook_path": "/a",
                                            "base_parameters": {"fl_score_table": ""}}},
        {"task_key": "B", "notebook_task": {"notebook_path": "/b"},
         "depends_on": [{"task_key": "A"}]},
    ])
    nbs = {
        "A": _nb('oidlUtils.parameters.setTaskValue("fl_score_table", x)'),   # migrated setter
        "B": _nb('v = oidlUtils.parameters.getParameter("fl_score_table")'),  # migrated reader
    }
    plan = analyze_task_params(s, nbs)
    assert "fl_score_table" not in plan.job_level_params          # Fix B: not hoisted
    payload = convert_dbx_job({"job_id": "1", "settings": s}, "/W", "ai_tool", {},
                              aidp_default_cluster_key="ck", param_plan=plan)
    a = [t for t in payload["tasks"] if t["taskKey"] == "A"][0]
    assert {"name": "fl_score_table", "value": ""} in a["parameters"]  # base decl survives
