"""Run-context metadata and validation handoff (no customer-code rewriting)."""

import asyncio
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from dbx_export.__main__ import _analyze_job_parameter_plan
from dbx_export.dependency_resolver import Graph, NotebookNode
from dbx_export.job_converter import convert_dbx_job
from dbx_export.param_analysis import analyze_task_params
from build_dag_from_workflow import build_manifest, extract_tasks_from_job
from clone_workflow import clone_workflow
from job_migrate_from_workflow import _run_workflow_job


CONTEXT = "dbutils.notebook.entry_point.getDbutils().notebook().getContext()"
READ = f"ctx = json.loads({CONTEXT}.toJson())\njob_run_id = ctx.get('tags', {{}}).get('jobRunId', None)"


def nb(*cells):
    return {"cells": [{"cell_type": "code", "source": source} for source in cells]}


def settings():
    return {"name": "run_context_test", "tasks": [
        {"task_key": "prepare", "notebook_task": {"notebook_path": "/Shared/prepare"}},
        {"task_key": "consume", "notebook_task": {"notebook_path": "/Shared/consume"},
         "depends_on": [{"task_key": "prepare"}]},
    ]}


@pytest.mark.parametrize("cells", [
    [READ],
    [f"ctx = json.loads(\n {CONTEXT}.toJson()\n)", "alias = ctx", "run = alias['tags']['jobRunId']"],
    ["import json as js", f"context = js.loads({CONTEXT}.toJson())", "run = context.get(\"tags\", {}).get(\"jobRunId\")"],
    ["from json import loads as parse", f"run = parse({CONTEXT}.toJson())['tags']['jobRunId']"],
    [f"ctx = {CONTEXT}", "run = ctx.tags().get('jobRunId').get()"],
    ["%pip install example", READ],
    ["!pip install example", READ],
    ["%python\n" + READ],
])
def test_detects_literal_context_reads_without_modifying_notebooks(cells):
    notebooks = {"prepare": nb(*cells), "consume": nb(READ)}
    original = copy.deepcopy(notebooks)
    plan = analyze_task_params(settings(), notebooks)
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}
    assert notebooks == original


READ_ONLY_CONTEXT_FORMS = [
    (f"ctx = json.loads({CONTEXT}.toJson())",
     "other = ctx.get('tags', {}).get(KEY, None)",
     "rid = ctx.get('tags', {}).get('jobRunId')"),
    (f"ctx = json.loads({CONTEXT}.toJson())",
     "other = ctx.get(KEY, None)", "rid = ctx['tags']['jobRunId']"),
    (f"ctx = json.loads({CONTEXT}.toJson())\ntags = ctx['tags']",
     "other = tags.get(KEY)", "rid = tags.get('jobRunId')"),
    (f"ctx = {CONTEXT}", "other = ctx.tags().get(KEY).get()",
     "rid = ctx.tags().get('jobRunId').get()"),
    (f"ctx = {CONTEXT}\ntags = ctx.tags()", "other = tags.get(KEY).get()",
     "rid = tags.get('jobRunId').get()"),
]


@pytest.mark.parametrize("setup,other,run_read", READ_ONLY_CONTEXT_FORMS)
@pytest.mark.parametrize("key", ["'user'", "'jobId'", "dynamic_key"])
@pytest.mark.parametrize("run_first", [False, True])
def test_read_only_context_lookups_preserve_run_binding_in_either_order(setup, other, run_read, key, run_first):
    other = other.replace("KEY", key)
    reads = [run_read, other] if run_first else [other, run_read]
    notebook = nb(setup, *reads)
    original = copy.deepcopy(notebook)
    plan = analyze_task_params(settings(), {"prepare": notebook})
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}
    assert not any("mutation or unmodeled" in warning for warning in plan.warnings)
    assert notebook == original


@pytest.mark.parametrize("setup,other,_run_read", READ_ONLY_CONTEXT_FORMS)
def test_read_only_does_not_give_unrelated_result_run_context_provenance(setup, other, _run_read):
    job = settings()
    job["parameters"] = [{"name": "jobRunId", "default": "business"}]
    plan = analyze_task_params(job, {"prepare": nb(
        setup, other.replace("KEY", "'user'"), "rid = other.get('jobRunId')",
    )})
    assert plan.job_level_params == {}
    assert not any("mutation or unmodeled" in warning for warning in plan.warnings)


@pytest.mark.parametrize("read_with_mutation", [
    "other = ctx.get('tags', ctx.update({'tags': {'jobRunId': 'business'}}))",
    "other = ctx.get('tags', {}).get(ctx.clear())",
])
def test_read_only_outer_call_does_not_hide_argument_mutations(read_with_mutation):
    job = settings()
    job["parameters"] = [{"name": "jobRunId", "default": "business"}]
    plan = analyze_task_params(job, {"prepare": nb(
        f"ctx = json.loads({CONTEXT}.toJson())", read_with_mutation,
        "rid = ctx.get('tags', {}).get('jobRunId')",
    )})
    assert plan.job_level_params == {}
    assert any("mutation or unmodeled" in warning for warning in plan.warnings)


@pytest.mark.parametrize("source", [
    "# " + READ.replace("\n", "\n# "),
    repr(READ),
    "ctx = {'tags': {'jobRunId': 'business_value'}}\nrun = ctx.get('tags', {}).get('jobRunId')",
    f"ctx = json.loads({CONTEXT}.toJson())\nctx = {{}}\nrun = ctx.get('tags', {{}}).get('jobRunId')",
    "run = dbutils.widgets.get('jobRunId')",
    "run = oidlUtils.parameters.getParameter('unrelated', '')",
    "%scala\n" + READ,
    f"ctx = json.loads({CONTEXT}.toJson())\ndef f(ctx):\n    return ctx.get('tags', {{}}).get('jobRunId')",
    f"ctx = json.loads({CONTEXT}.toJson())\nf = lambda ctx: ctx.get('tags', {{}}).get('jobRunId')",
    f"ctx = json.loads({CONTEXT}.toJson())\nvalues = [ctx.get('tags', {{}}).get('jobRunId') for ctx in rows]",
])
def test_unrelated_or_unproven_reads_do_not_add_parameters(source):
    assert analyze_task_params(settings(), {"prepare": nb(source)}).job_level_params == {}


def test_unsupported_syntax_is_reported_not_silently_wired():
    plan = analyze_task_params(settings(), {"prepare": nb("ctx = getContext(\njobRunId")})
    assert not plan.job_level_params
    assert any("UNRESOLVED" in warning for warning in plan.warnings)


@pytest.mark.parametrize("scope", ["job", "task"])
def test_conflicting_customer_parameter_is_not_overwritten(scope):
    job = settings()
    if scope == "job":
        job["parameters"] = [{"name": "jobRunId", "default": "customer-value"}]
    else:
        job["tasks"][1]["notebook_task"]["base_parameters"] = {"jobRunId": "customer-value"}
    original = copy.deepcopy(job)
    with pytest.raises(ValueError, match="conflicts"):
        analyze_task_params(job, {"prepare": nb(READ)})
    assert job == original


@pytest.mark.parametrize("method", ["text", "dropdown", "combobox", "multiselect"])
@pytest.mark.parametrize("task", ["prepare", "consume"])
def test_context_binding_rejects_widget_declaration_in_any_task(method, task):
    declaration = f'dbutils.widgets.{method}("jobRunId", "business-default")'
    notebooks = {"prepare": nb(READ), "consume": nb("pass")}
    notebooks[task]["cells"].append({"cell_type": "code", "source": declaration})
    original = copy.deepcopy(notebooks)
    with pytest.raises(ValueError, match=f"{task}.*jobRunId.*widget"):
        analyze_task_params(settings(), notebooks)
    assert notebooks == original


def test_widget_without_context_reader_is_unchanged():
    plan = analyze_task_params(settings(), {"consume": nb(
        'dbutils.widgets.text("jobRunId", "business-default")',
        'value = dbutils.widgets.get("jobRunId")',
    )})
    assert plan.job_level_params == {} and plan.warnings == []


@pytest.mark.parametrize("method", ["text", "dropdown", "combobox", "multiselect"])
@pytest.mark.parametrize("task", ["prepare", "consume"])
@pytest.mark.parametrize("transitive", [False, True])
def test_dependency_widget_conflict_reaches_exporter(method, task, transitive):
    parent = f"/Shared/{task}"
    graph = Graph(notebooks={
        "/Shared/prepare": NotebookNode("/Shared/prepare", "task", nb_dict=nb(READ)),
        "/Shared/consume": NotebookNode("/Shared/consume", "task", nb_dict=nb("pass")),
        "/Shared/helper": NotebookNode("/Shared/helper", "run_dep", referenced_by=[parent], nb_dict=nb("%run ./child")),
        "/Shared/child": NotebookNode("/Shared/child", "run_dep",
            referenced_by=["/Shared/helper" if transitive else parent],
            nb_dict=nb(f'dbutils.widgets.{method}("jobRunId", "business-default")')),
    })
    original = copy.deepcopy(graph)
    with pytest.raises(ValueError, match=f"{task}.*jobRunId.*widget"):
        _analyze_job_parameter_plan(graph, SimpleNamespace(job_def={"settings": settings()}), log=None)
    assert graph == original


@pytest.mark.parametrize("widget,reader,reachable", [
    ("unrelated", True, True), ("jobRunId", False, True), ("jobRunId", True, False),
])
def test_dependency_widgets_do_not_hoist_or_conflict_without_context(widget, reader, reachable):
    graph = Graph(notebooks={
        "/Shared/prepare": NotebookNode("/Shared/prepare", "task", nb_dict=nb(READ if reader else "pass")),
        "/Shared/child": NotebookNode("/Shared/child", "run_dep",
            referenced_by=["/Shared/consume" if reachable else "/another_job"],
            nb_dict=nb(f'dbutils.widgets.text("{widget}", "business-default")')),
    })
    plan = _analyze_job_parameter_plan(graph, SimpleNamespace(job_def={"settings": settings()}), log=None)
    assert plan.job_level_params == ({"jobRunId": "{{job.run_id}}"} if reader else {})
    assert plan.task_extra_params == {}


@pytest.mark.parametrize("mutation", [
    'ctx.update({"tags": {"jobRunId": "business"}})',
    'ctx.update(tags={"jobRunId": "business"})',
    'alias.update({"tags": {"jobRunId": "business"}})',
    'tags.update({"jobRunId": "business"})',
    'ctx["tags"].update({"jobRunId": "business"})',
    'ctx.get("tags", {}).update({"jobRunId": "business"})',
    'ctx.clear()',
    'result = ctx.pop("tags")',
    'ctx.unmodeled_method()',
    'if condition:\n    tags.update({"jobRunId": "business"})',
])
@pytest.mark.parametrize("explicit_parameter", [False, True])
def test_unmodeled_context_methods_neither_bind_nor_abort_valid_input(mutation, explicit_parameter):
    job = settings()
    if explicit_parameter:
        job["parameters"] = [{"name": "jobRunId", "default": "business"}]
    notebook = nb(f"ctx = json.loads({CONTEXT}.toJson())", "alias = ctx",
                  "tags = ctx['tags']", mutation, "rid = alias['tags']['jobRunId']")
    original = copy.deepcopy((job, notebook))
    plan = analyze_task_params(job, {"prepare": notebook})
    assert plan.job_level_params == {}
    assert any("UNRESOLVED" in warning for warning in plan.warnings)
    assert (job, notebook) == original


@pytest.mark.parametrize("middle", [
    "other = {}\nother.update({'jobRunId': 'business'})",
    f"ctx.update({{}})\nctx = json.loads({CONTEXT}.toJson())",
    "tags = ctx.get('tags', {})",
])
def test_unrelated_method_or_fresh_context_preserves_supported_reads(middle):
    plan = analyze_task_params(settings(), {"prepare": nb(
        f"ctx = json.loads({CONTEXT}.toJson())", middle, "rid = ctx['tags']['jobRunId']",
    )})
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}


@pytest.mark.parametrize("setup,read", [
    (f"ctx = {CONTEXT}", "rid = alias.tags().get('jobRunId').get()"),
    (f"ctx = {CONTEXT}.tags()", "rid = alias.get('jobRunId').get()"),
    (f"ctx = {CONTEXT}.tags().get('jobRunId')", "rid = alias.get()"),
    (f"ctx = {CONTEXT}.toJson()", "rid = json.loads(alias)['tags']['jobRunId']"),
])
def test_unmodeled_methods_invalidate_other_context_aliases(setup, read):
    job = settings()
    job["parameters"] = [{"name": "jobRunId", "default": "business"}]
    plan = analyze_task_params(job, {"prepare": nb(setup, "alias = ctx", "ctx.unmodeled_method()", read)})
    assert plan.job_level_params == {}
    assert any("UNRESOLVED" in warning for warning in plan.warnings)


@pytest.mark.parametrize("mutation,read", [
    ("ctx['tags']['jobRunId'] = 'business'", ""),
    ("ctx['tags']['jobRunId'] = 'business'", "rid = ctx['tags']['jobRunId']"),
    ("ctx['tags'] = {'jobRunId': 'business'}", "rid = ctx.get('tags', {}).get('jobRunId')"),
    ("alias['tags']['jobRunId'] = 'business'", "rid = ctx['tags']['jobRunId']"),
    ("tags['jobRunId'] = 'business'", "rid = alias['tags']['jobRunId']"),
    ("ctx['tags']['jobRunId']: str = 'business'", "rid = tags['jobRunId']"),
    ("ctx['tags']['jobRunId'] += 'suffix'", "rid = ctx['tags']['jobRunId']"),
    ("del ctx['tags']['jobRunId']", "rid = ctx.get('tags', {}).get('jobRunId')"),
    ("if condition:\n    ctx['tags']['jobRunId'] = 'business'", "rid = ctx['tags']['jobRunId']"),
])
def test_context_dictionary_mutations_do_not_register_binding(mutation, read):
    job = settings()
    # False discovery must not raise a spurious conflict with this real input.
    job["parameters"] = [{"name": "jobRunId", "default": "business"}]
    notebook = nb(f"ctx = json.loads({CONTEXT}.toJson())", "alias = ctx",
                  "tags = ctx['tags']", mutation, read)
    original = copy.deepcopy(notebook)
    plan = analyze_task_params(job, {"prepare": notebook})
    assert plan.job_level_params == {}
    assert any("mutat" in warning for warning in plan.warnings)
    assert notebook == original


@pytest.mark.parametrize("middle", [
    "other = {}\nother['jobRunId'] = 'business'",
    f"ctx['tags']['jobRunId'] = 'business'\nctx = json.loads({CONTEXT}.toJson())",
])
def test_unrelated_mutation_or_fresh_context_keeps_proven_read(middle):
    plan = analyze_task_params(settings(), {"prepare": nb(
        f"ctx = json.loads({CONTEXT}.toJson())", middle, "rid = ctx['tags']['jobRunId']",
    )})
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}


@pytest.mark.parametrize("source", [
    f"ctx = json.loads({CONTEXT}.toJson())\nrid = ctx['tags'][key]",
    f"ctx = json.loads({CONTEXT}.toJson())\nrid = ctx.get('tags', {{}}).get(key)",
    "ctx = {'tags': {'jobRunId': 'business'}}\nrid = ctx['tags']['jobRunId']",
])
def test_unproven_run_context_lookup_warns_without_binding(source):
    plan = analyze_task_params(settings(), {"prepare": nb(source)})
    assert plan.job_level_params == {}
    assert any("not proven" in warning for warning in plan.warnings)


def test_export_wires_once_preserves_customer_parameters_and_clone_binding():
    job = settings()
    job["parameters"] = [{"name": "business", "default": "original"},
                         {"name": "jobRunId", "default": "{{job.run_id}}"}]
    plan = analyze_task_params(job, {"prepare": nb(READ), "consume": nb(READ)})
    exported = convert_dbx_job(
        {"job_id": "123", "settings": job}, export_output="/Workspace/export",
        prefix="TEST", dbx_cluster_key_to_aidp_key={}, param_plan=plan,
    )
    assert exported["parameters"] == [
        {"name": "business", "value": "original"},
        {"name": "jobRunId", "value": "{{job.run_id}}"},
    ]
    # Real manifest builder must carry the template into the canonical runner.
    tasks = extract_tasks_from_job(exported)
    manifest = build_manifest("test", tasks, [["prepare"], ["consume"]], {}, exported)
    manifest_job = manifest["jobs"][0]
    assert manifest_job["parameters"]["jobRunId"] == "{{job.run_id}}"
    source = copy.deepcopy(manifest_job)
    seen = []

    async def process_job(runtime_job, session, **kwargs):
        # The existing engine merges this same job-parameter dict into every task.
        from notebook_path_parameters import effective_task_parameters
        for task in runtime_job["tasks"]:
            seen.append(effective_task_parameters(task.get("base_parameters", {}), runtime_job["parameters"])["jobRunId"])
        return {"job_name": runtime_job["job_name"]}

    asyncio.run(_run_workflow_job(manifest_job, None, SimpleNamespace(process_job=process_job), False))
    assert len(seen) == 2 and seen[0] == seen[1]
    assert seen[0].startswith("migration_validation_")
    assert manifest_job == source
    cloned, _, _ = clone_workflow(exported, {})
    assert cloned["parameters"] == exported["parameters"]
    assert seen[0] not in str(cloned)


def test_dependency_scan_uses_only_already_discovered_reachable_notebooks():
    graph = Graph(notebooks={
        "/Shared/prepare": NotebookNode("/Shared/prepare", "task", nb_dict=nb("%run ./helper")),
        "/Shared/helper": NotebookNode("/Shared/helper", "run_dep", referenced_by=["/Shared/prepare"], nb_dict=nb(READ)),
        "/unrelated": NotebookNode("/unrelated", "run_dep", referenced_by=["/another_job"], nb_dict=nb(READ)),
    })
    job_node = SimpleNamespace(job_def={"settings": settings()})
    assert _analyze_job_parameter_plan(graph, job_node, log=None).job_level_params == {"jobRunId": "{{job.run_id}}"}
    graph.notebooks["/Shared/helper"].nb_dict = nb("x = 1")
    assert _analyze_job_parameter_plan(graph, job_node, log=None).job_level_params == {}


def test_shared_task_notebook_dependency_and_cycles_are_scanned_once():
    graph = Graph(notebooks={
        "/Shared/prepare": NotebookNode("/Shared/prepare", "task", referenced_by=["/Shared/helper"], nb_dict=nb("%run ./helper")),
        # First discovered as another job's task, then used as this job's helper.
        "/Shared/helper": NotebookNode("/Shared/helper", "task", referenced_by=["/Shared/prepare", "/Shared/consume"], nb_dict=nb(READ)),
    })
    plan = _analyze_job_parameter_plan(graph, SimpleNamespace(job_def={"settings": settings()}), log=None)
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}


def test_null_base_parameters_and_unrelated_widgets_are_preserved():
    job = settings()
    job["tasks"][0]["notebook_task"]["base_parameters"] = None
    plan = analyze_task_params(job, {"prepare": nb(READ)})
    assert plan.job_level_params == {"jobRunId": "{{job.run_id}}"}
    unrelated = analyze_task_params(settings(), {"prepare": nb("x = dbutils.widgets.get('jobRunId')")})
    assert unrelated.job_level_params == {} and unrelated.warnings == []


@pytest.mark.parametrize("parameters", [{}, {"jobRunId": "explicit-test-id"}, {"other": "{{job.run_id}}"}])
def test_non_template_parameters_and_existing_jobs_unchanged(parameters):
    job = {"parameters": parameters}

    async def process_job(runtime_job, session, **kwargs):
        assert runtime_job is job
        return "ok"

    assert asyncio.run(_run_workflow_job(job, None, SimpleNamespace(process_job=process_job), False)) == "ok"


def test_each_validation_invocation_gets_a_fresh_namespace():
    ids = []
    async def process_job(job, session, **kwargs):
        ids.append(job["parameters"]["jobRunId"])
    job = {"parameters": {"jobRunId": "{{job.run_id}}"}}
    for _ in range(2):
        asyncio.run(_run_workflow_job(job, None, SimpleNamespace(process_job=process_job), False))
    assert ids[0] != ids[1]
    assert job["parameters"]["jobRunId"] == "{{job.run_id}}"
