"""Orchestration a production workflow carries: worklets, renamed session
instances, one mapping run by two sessions, parameter files, and the
SUCCEEDED link condition.

Each of these was lost before without an error:

- worklets were never expanded, so their sessions were absent from the job
  (reported, but a nightly load built from worklets deployed as an empty or
  partial DAG);
- sessions were keyed by TASKNAME while links name INSTANCES, so a renamed
  instance of a reusable session lost every dependency, and the same
  session run twice became one task;
- a mapping run by two sessions gave only the last one a notebook path, so
  the other task pointed at a placeholder;
- session ``$$`` overrides and ``.par`` values never reached the job:
  ``workflow.sessions`` held names, so the parameter branch never ran.

The fixture (tests/fixtures/orchestration/wf_worklets_params.xml + .par) is
hand-authored from the PowerCenter 10.x export schema; every expected value
below is derived from PowerCenter's documented behaviour, not read back from
the generator.
"""
from __future__ import annotations

import json
import os
import shutil

from infa2aidp.generators.workflow_generator import WorkflowGenerator
from infa2aidp.models import Workflow
from infa2aidp.parsers.parameter_parser import ParameterFileParser
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

HERE = os.path.dirname(__file__)
XML = os.path.join(HERE, "fixtures", "orchestration", "wf_worklets_params.xml")
PAR = os.path.join(HERE, "fixtures", "orchestration", "wf_worklets_params.par")


def _translate(with_par: bool = True):
    parsed = InformaticaXMLParser().parse(XML)
    nb = {s.name: f"/Workspace/Migrated/SALES_DM/nb_{s.mapping_name}.ipynb" for s in parsed.sessions}
    pf = ParameterFileParser().parse(PAR) if with_par else None
    tr = WorkflowGenerator().generate(
        parsed.workflows[0], nb, sessions={s.name: s for s in parsed.sessions}, parameter_file=pf,
    )
    return tr, {t["taskKey"]: t for t in tr.job["tasks"]}


def _params(task) -> dict:
    return {p["name"]: p["value"] for p in task.get("parameters", [])}


# ── Worklets and instances ─────────────────────────────────────────────

def test_worklet_sessions_become_job_tasks_keyed_by_instance():
    _, tasks = _translate()
    assert set(tasks) == {
        "wl_dims__s_dim_customer", "wl_dims__s_dim_product",
        "s_m_load_fact", "s_m_load_fact_hist", "s_m_cleanup",
        "wl_audit__wl_marts__s_m_mart_sales",
    }


def test_a_reusable_session_run_twice_is_two_tasks_on_one_notebook():
    _, tasks = _translate()
    a, b = tasks["wl_dims__s_dim_customer"], tasks["wl_dims__s_dim_product"]
    assert a["notebookPath"] == b["notebookPath"] == "/Workspace/Migrated/SALES_DM/nb_m_load_dim.ipynb"
    assert b["dependsOn"] == [{"taskKey": "wl_dims__s_dim_customer"}]


def test_links_into_and_out_of_a_worklet_attach_to_its_entry_and_exit_sessions():
    _, tasks = _translate()
    assert tasks["wl_dims__s_dim_customer"]["dependsOn"] == []
    # wl_dims -> s_m_load_fact: the worklet's last session gates the fact.
    assert tasks["s_m_load_fact"]["dependsOn"] == [{"taskKey": "wl_dims__s_dim_product"}]
    # s_m_cleanup -> non-reusable wl_audit -> reusable wl_marts (two levels).
    assert tasks["wl_audit__wl_marts__s_m_mart_sales"]["dependsOn"] == [{"taskKey": "s_m_cleanup"}]


def test_dependencies_pass_through_decision_and_command_tasks():
    _, tasks = _translate()
    assert tasks["s_m_load_fact_hist"]["dependsOn"] == [{"taskKey": "s_m_load_fact"}]
    assert tasks["s_m_cleanup"]["dependsOn"] == [{"taskKey": "s_m_load_fact"}]


def test_expanded_worklets_are_not_reported_but_dropped_tasks_are():
    tr, _ = _translate()
    review = "\n".join(tr.not_translated)
    assert "Worklet" not in review
    for ttype, name in (("DECISION", "dec_month_end"), ("EMAIL", "em_fact_failed"),
                        ("COMMAND", "cmd_archive")):
        assert any(ttype in r and name in r for r in tr.not_translated), (ttype, review)


def test_a_worklet_missing_from_the_export_is_still_reported():
    """Expansion needs the definition; without it the DAG is incomplete."""
    xml = open(XML, encoding="utf-8").read()
    start = xml.index('  <WORKLET NAME="wl_marts"')
    end = xml.index("</WORKLET>", start) + len("</WORKLET>")
    path = os.path.join(os.path.dirname(PAR), "_tmp_missing_worklet.xml")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml[:start] + xml[end:])
        wf = InformaticaXMLParser().parse(path).workflows[0]
    finally:
        os.remove(path)
    review = WorkflowGenerator().generate(wf, {}).not_translated
    assert any("Worklet" in r and "wl_marts" in r for r in review), review


def test_a_worklet_that_contains_itself_does_not_recurse():
    wf_xml = """<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F">
      <WORKLET NAME="wl_loop" REUSABLE="YES">
        <TASKINSTANCE NAME="wl_loop" TASKNAME="wl_loop" TASKTYPE="Worklet"/>
      </WORKLET>
      <WORKFLOW NAME="wf">
        <TASKINSTANCE NAME="wl_loop" TASKNAME="wl_loop" TASKTYPE="Worklet"/>
      </WORKFLOW></FOLDER></REPOSITORY></POWERMART>"""
    path = os.path.join(os.path.dirname(PAR), "_tmp_loop.xml")
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(wf_xml)
        wf = InformaticaXMLParser().parse(path).workflows[0]
    finally:
        os.remove(path)
    assert [t["instance_name"] for t in wf.tasks] == ["wl_loop__wl_loop"]


# ── Link conditions ────────────────────────────────────────────────────

def test_succeeded_conditions_are_applied_not_reported():
    """``$X.Status = SUCCEEDED`` on X's own outgoing link is exactly
    dependsOn + runIf ALL_SUCCESS -- including a worklet's status."""
    tr, _ = _translate()
    conditions = [r for r in tr.not_translated if "Link condition" in r]
    assert not [r for r in conditions if "SUCCEEDED" in r], conditions


def test_other_conditions_are_still_reported():
    tr, _ = _translate()
    conditions = "\n".join(r for r in tr.not_translated if "Link condition" in r)
    assert "$dec_month_end.Condition = TRUE" in conditions
    assert "$s_m_load_fact.Status = FAILED" in conditions


def test_succeeded_on_a_different_task_than_the_upstream_is_reported():
    wf = Workflow(name="wf", sessions=["s_a", "s_b", "s_c"])
    wf.tasks = [{"name": n, "type": "Session"} for n in ("s_a", "s_b", "s_c")]
    wf.dependencies = [
        {"from_task": "s_a", "to_task": "s_b", "condition": "$s_a.Status = SUCCEEDED"},
        {"from_task": "s_b", "to_task": "s_c", "condition": "$s_a.Status = SUCCEEDED"},
    ]
    review = WorkflowGenerator().generate(wf, {n: "/nb" for n in ("s_a", "s_b", "s_c")}).not_translated
    assert [r for r in review if "Link condition" in r] == [
        r for r in review if "s_b -> s_c" in r
    ] and len([r for r in review if "Link condition" in r]) == 1, review


def test_an_unconditional_link_from_a_session_is_a_stated_assumption():
    """PowerCenter runs the next task even when the previous one FAILED."""
    tr, _ = _translate()
    hits = [a for a in tr.assumptions if "unconditional" in a]
    assert hits and "s_m_cleanup -> wl_audit" in hits[0] and "ALL_DONE" in hits[0], tr.assumptions
    assert not tr.job.get("assumptions")


# ── Parameters ─────────────────────────────────────────────────────────

def test_parameter_file_sections_apply_narrowest_scope_last():
    _, tasks = _translate()
    cust = _params(tasks["wl_dims__s_dim_customer"])
    assert cust == {
        "migration.env": "PROD",               # [Global]
        "migration.batch_size": "10000",       # workflow section overrides Global's 5000
        "migration.run_region": "ALL",         # workflow section
        "migration.dim_scope": "FULL",         # worklet section
        "migration.dim_name": "CUSTOMER",      # the instance's own section
        "migration.src_table": "CRM.CUSTOMERS",
    }
    prod = _params(tasks["wl_dims__s_dim_product"])
    assert prod["migration.dim_name"] == "PRODUCT" and prod["migration.src_table"] == "ERP.PRODUCTS"


def test_a_session_section_overrides_the_workflow_and_other_workflows_do_not_leak():
    _, tasks = _translate()
    fact = _params(tasks["s_m_load_fact"])
    assert fact["migration.run_region"] == "EMEA"
    assert fact["migration.load_type"] == "INCREMENTAL"   # not wf_other's value
    assert "migration.dim_name" not in fact               # another instance's section


def test_session_level_overrides_reach_the_task_without_a_parameter_file():
    _, tasks = _translate(with_par=False)
    assert _params(tasks["s_m_load_fact"]) == {"migration.load_type": "DELTA"}
    assert _params(tasks["s_m_load_fact_hist"]) == {"migration.load_type": "FULL"}
    assert "parameters" not in tasks["s_m_cleanup"]


def test_non_dollar_dollar_values_are_reported_not_applied():
    tr, tasks = _translate()
    assert not any("dbconnection" in p for t in tasks.values() for p in _params(t))
    review = "\n".join(tr.not_translated)
    assert "$DBConnection_SRC" in review and "$InputFile_Products" in review


def test_old_style_folder_session_scope_applies_to_every_instance_of_the_session():
    parsed = InformaticaXMLParser().parse(XML)
    par = os.path.join(os.path.dirname(PAR), "_tmp_old_style.par")
    try:
        with open(par, "w", encoding="utf-8") as f:
            f.write("[SALES_DM.s_m_load_dim]\n$$DIM_SCOPE=OLD_STYLE\n[OTHER_FOLDER.s_m_load_dim]\n$$X=1\n")
        pf = ParameterFileParser().parse(par)
    finally:
        os.remove(par)
    tr = WorkflowGenerator().generate(parsed.workflows[0], {}, parameter_file=pf)
    tasks = {t["taskKey"]: t for t in tr.job["tasks"]}
    for key in ("wl_dims__s_dim_customer", "wl_dims__s_dim_product"):
        assert _params(tasks[key]) == {"migration.dim_scope": "OLD_STYLE"}, key


def test_parameter_names_are_case_insensitive_across_sections():
    """$$Env and $$ENV are one parameter; the narrower section wins and the
    task carries it once."""
    parsed = InformaticaXMLParser().parse(XML)
    par = os.path.join(os.path.dirname(PAR), "_tmp_case.par")
    try:
        with open(par, "w", encoding="utf-8") as f:
            f.write("[Global]\n$$Env=DEV\n[SALES_DM.WF:wf_sales_nightly.ST:s_m_cleanup]\n$$ENV=PROD\n")
        pf = ParameterFileParser().parse(par)
    finally:
        os.remove(par)
    tr = WorkflowGenerator().generate(parsed.workflows[0], {}, parameter_file=pf)
    cleanup = next(t for t in tr.job["tasks"] if t["taskKey"] == "s_m_cleanup")
    assert cleanup["parameters"] == [{"name": "migration.env", "value": "PROD"}]


# ── End to end through run_migration ───────────────────────────────────

def test_every_session_of_a_mapping_gets_the_notebook_and_its_own_parameters(tmp_path):
    """One mapping, two sessions: both tasks point at the one notebook and
    carry their own $$ value; the .par file flows through run_migration."""
    from infa2aidp.migrator import run_migration

    src = open(os.path.join(HERE, "fixtures", "constructs", "session_pre_post_sql.xml"),
               encoding="utf-8").read()
    extra = """
  <SESSION NAME="s_m_stage_orders_full" MAPPINGNAME="m_stage_orders" ISVALID="YES">
    <ATTRIBUTE NAME="$$LOAD_TYPE" VALUE="FULL"/>
  </SESSION>
  <WORKFLOW NAME="wf_stage" ISENABLED="YES">
    <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
    <TASKINSTANCE NAME="s_m_stage_orders" TASKNAME="s_m_stage_orders" TASKTYPE="Session"/>
    <TASKINSTANCE NAME="s_full" TASKNAME="s_m_stage_orders_full" TASKTYPE="Session"/>
    <WORKFLOWLINK FROMTASK="Start" TOTASK="s_m_stage_orders" CONDITION=""/>
    <WORKFLOWLINK FROMTASK="s_m_stage_orders" TOTASK="s_full" CONDITION="$s_m_stage_orders.Status = SUCCEEDED"/>
  </WORKFLOW>
</FOLDER>"""
    xml = tmp_path / "two_sessions.xml"
    xml.write_text(src.replace("</FOLDER>", extra, 1), encoding="utf-8")
    par = tmp_path / "p.par"
    par.write_text("[Global]\n$$ENV=QA\n", encoding="utf-8")

    run_migration([str(xml)], str(tmp_path / "out"), use_llm=False, params_path=str(par),
                  skip_lineage=True, skip_optimize=True, score_confidence=False)
    job = json.loads((tmp_path / "out" / "workflows" / "wf_stage.json").read_text(encoding="utf-8"))
    tasks = {t["taskKey"]: t for t in job["tasks"]}
    assert set(tasks) == {"s_m_stage_orders", "s_full"}
    assert tasks["s_m_stage_orders"]["notebookPath"] == tasks["s_full"]["notebookPath"]
    assert tasks["s_full"]["notebookPath"].endswith("/nb_m_stage_orders.ipynb")
    assert tasks["s_full"]["dependsOn"] == [{"taskKey": "s_m_stage_orders"}]
    assert _params(tasks["s_full"]) == {"migration.env": "QA", "migration.load_type": "FULL"}
    assert _params(tasks["s_m_stage_orders"]) == {"migration.env": "QA"}
    review = tmp_path / "out" / "workflows" / "wf_stage.review.md"
    assert not review.exists() or "No migrated notebook" not in review.read_text(encoding="utf-8")


def test_the_parallel_batch_path_emits_workflows_too(monkeypatch, tmp_path):
    """--workers > 1 used to write notebooks and no job definition at all."""
    from infa2aidp.migrator import run_migration

    class _LLM:
        claude_model = "fake"

        def is_available(self):
            return True

    xml_a = tmp_path / "in" / "a.xml"
    xml_b = tmp_path / "in" / "b.xml"
    xml_a.parent.mkdir()
    shutil.copy(XML, xml_a)
    shutil.copy(os.path.join(HERE, "fixtures", "constructs", "session_pre_post_sql.xml"), xml_b)

    class _R:
        def __init__(self, xml, mapping):
            self.xml_path, self.mapping_name = str(xml), mapping
            self.notebook_path = str(tmp_path / "out" / "SALES_DM" / f"nb_{mapping}.ipynb")
            self.score, self.status, self.error = 90, "success", None

    class _Summary:
        success, fallback, failed = 2, 0, 0
        results = [_R(xml_a, "m_load_dim"), _R(xml_a, "m_load_fact"), _R(xml_b, "m_stage_orders")]

    class _Batch:
        def __init__(self, **kw):
            pass

        def migrate_folder(self, input_dir, output_dir):
            return _Summary()

    monkeypatch.setattr("infa2aidp.handlers.codellama_handler.LLMHandler", _LLM)
    monkeypatch.setattr("infa2aidp.batch.BatchMigrator", _Batch)
    result = run_migration([str(xml_a), str(xml_b)], str(tmp_path / "out"), use_llm=True,
                           max_workers=4, score_confidence=False, skip_lineage=True,
                           skip_optimize=True, params_path=PAR)
    assert result.workflows == 1
    job = json.loads((tmp_path / "out" / "workflows" / "wf_sales_nightly.json").read_text(encoding="utf-8"))
    tasks = {t["taskKey"]: t for t in job["tasks"]}
    assert tasks["wl_dims__s_dim_customer"]["notebookPath"].endswith("/nb_m_load_dim.ipynb")
    assert _params(tasks["s_m_load_fact"])["migration.run_region"] == "EMEA"
    # m_cleanup produced no notebook in this batch: its task says so.
    assert "s_m_cleanup" in "\n".join(result.workflow_reviews["wf_sales_nightly"])
