"""Regressions for five orchestration / deploy defects found in review.

1. ``--schedule-timezone`` refused every zone on a host with no tz database
   (Python on Windows has none without ``tzdata``), matched zone names
   case-insensitively there was one, and was checked only after the first
   export's notebooks were already written.
2. A link condition in front of a pass-through node (Command, Decision, a
   worklet's Start) was dropped: ``s_a -[SUCCEEDED]-> cmd -> s_b`` gave s_b
   runIf ALL_DONE, so s_b ran after s_a FAILED.
3. One SUCCEEDED link made a task ALL_SUCCESS while the assumption text
   still said its unconditional link ran "on completion (runIf ALL_DONE).
   This matches PowerCenter." -- false for that task.
4. After a notebook NAME CLASH the second job pointed at the FIRST notebook.
5. A cluster reporting Spark "3.5" was refused as older than "3.5.0".

Fake clients only -- nothing here touches a network or a Spark.
"""
from __future__ import annotations

import json
import logging
import os
import re

import pytest

from infa2aidp.generators import workflow_generator as wg
from infa2aidp.generators.workflow_generator import (
    WorkflowGenerator,
    validate_schedule_timezone,
)
from infa2aidp.models import Workflow
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
C08 = os.path.join(ROOT, "tests", "golden", "cases",
                   "c08_unconnected_lookup_in_expression", "m_txn_to_usd.xml")

_DB = frozenset({"America/New_York", "Europe/London", "UTC", "Asia/Kolkata"})


# ---------------------------------------------------------------------------
# 1. schedule timezone
# ---------------------------------------------------------------------------

@pytest.fixture
def with_tz_db(monkeypatch):
    """A host WITH a tz database, whatever this one has."""
    monkeypatch.setattr(wg, "_known_zones", lambda: _DB)


@pytest.fixture
def without_tz_db(monkeypatch):
    """A host with NO tz database -- Windows without tzdata."""
    monkeypatch.setattr(wg, "_known_zones", lambda: frozenset())
    monkeypatch.setattr(wg, "_UNCHECKED_ZONES_WARNED", set())


def test_zone_is_compared_case_sensitively_against_the_database(with_tz_db):
    """AIDP's Java ZoneId is case-sensitive. ZoneInfo() opens a file, so on
    a case-insensitive filesystem it accepted 'america/new_york' and every
    job then failed at deploy."""
    assert validate_schedule_timezone("America/New_York") == "America/New_York"
    for bad in ("america/new_york", "AMERICA/NEW_YORK"):
        with pytest.raises(ValueError, match="not an IANA timezone") as e:
            validate_schedule_timezone(bad)
        assert "case-sensitive" in str(e.value) and "'America/New_York'" in str(e.value)


def test_a_misspelt_zone_is_refused_when_a_database_exists(with_tz_db):
    with pytest.raises(ValueError, match="not an IANA timezone"):
        WorkflowGenerator(schedule_timezone="America/New_Yrok")


def test_no_tz_database_does_not_call_a_valid_zone_invalid(without_tz_db, caplog):
    """The old check refused America/New_York and UTC as 'not an IANA
    timezone' on Windows. Without a database the name can only be checked
    for shape; it is accepted and the gap is logged, naming tzdata."""
    with caplog.at_level(logging.WARNING, logger=wg.__name__):
        gen = WorkflowGenerator(schedule_timezone="America/New_York")
        WorkflowGenerator(schedule_timezone="America/New_York")
    assert gen.schedule_timezone == "America/New_York"
    warned = [r.getMessage() for r in caplog.records if "tzdata" in r.getMessage()]
    assert len(warned) == 1, f"warn once per zone, not per generator: {warned}"
    assert WorkflowGenerator(schedule_timezone="UTC").schedule_timezone == "UTC"


def test_no_tz_database_refusal_names_tzdata_not_an_invalid_zone(without_tz_db):
    """'US/Eastern' is a real (backward-link) zone; with no database it
    cannot be checked, and the message must say so rather than call it
    invalid. A wrong-cased name never has an IANA shape, so it is refused."""
    for zone in ("US/Eastern", "america/new_york", "Europe/london", "utc"):
        with pytest.raises(ValueError) as e:
            validate_schedule_timezone(zone)
        msg = str(e.value)
        assert "tzdata" in msg and "not an IANA timezone" not in msg, msg


def test_a_bad_zone_fails_migrate_before_anything_is_written(tmp_path):
    """It used to fail when the first workflow was emitted -- after that
    export's notebooks and DDL were on disk."""
    from infa2aidp.migrator import run_migration
    out = tmp_path / "out"
    with pytest.raises(ValueError):
        run_migration([C08], str(out), use_llm=False, skip_lineage=True,
                      skip_optimize=True, score_confidence=False,
                      schedule_timezone="america/new_york")
    assert not out.exists() or not any(out.rglob("*")), list(out.rglob("*"))


def test_tzdata_is_declared_everywhere_dependencies_are():
    with open(os.path.join(ROOT, "engine", "requirements.txt"), encoding="utf-8") as f:
        assert re.search(r"^tzdata\b", f.read(), re.M)
    tomllib = pytest.importorskip("tomllib")
    with open(os.path.join(ROOT, "pyproject.toml"), "rb") as f:
        deps = tomllib.load(f)["project"]["dependencies"]
    assert any(re.match(r"tzdata\b", d) for d in deps), deps
    with open(os.path.join(ROOT, "setup.py"), encoding="utf-8") as f:
        setup_src = f.read()
    install = setup_src[setup_src.index("install_requires"):setup_src.index("extras_require")]
    assert '"tzdata' in install, install


# ---------------------------------------------------------------------------
# 2./3. link conditions through pass-through nodes, mixed links
# ---------------------------------------------------------------------------

def _wf(deps, sessions=("s_a", "s_b", "s_c"), others=()):
    wf = Workflow(name="wf_t", sessions=list(sessions))
    wf.tasks = ([{"name": n, "type": "Session"} for n in sessions]
                + [{"name": n, "type": t} for n, t in others])
    wf.dependencies = [
        {"from_task": f, "to_task": t, "to_instance": t, "condition": c}
        for f, t, c in deps
    ]
    wf.scheduler = {}
    tr = WorkflowGenerator().generate(wf, {s: f"/Workspace/Migrated/F/{s}.ipynb" for s in sessions})
    return {t["taskKey"]: t for t in tr.job["tasks"]}, tr


def _all_done_claims(tr) -> list[str]:
    return [a for a in tr.assumptions if "runIf ALL_DONE" in a]


def test_a_succeeded_condition_before_a_command_task_gates_the_next_session():
    tasks, tr = _wf([("s_a", "cmd_archive", "$s_a.Status = SUCCEEDED"),
                     ("cmd_archive", "s_b", "")],
                    sessions=("s_a", "s_b"), others=[("cmd_archive", "Command")])
    assert tasks["s_b"]["dependsOn"] == [{"taskKey": "s_a"}]
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS", "s_b would run after s_a FAILED"
    assert not [r for r in tr.not_translated if "Link condition" in r]
    assert not _all_done_claims(tr), tr.assumptions


def test_a_succeeded_condition_after_a_command_task_gates_it_too():
    tasks, tr = _wf([("s_a", "cmd_archive", ""),
                     ("cmd_archive", "s_b", "$cmd_archive.Status = SUCCEEDED")],
                    sessions=("s_a", "s_b"), others=[("cmd_archive", "Command")])
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS"
    # The unconditional s_a -> cmd_archive hop is not "on completion": the
    # next hop needs success, in PowerCenter as in the job.
    assert not _all_done_claims(tr), tr.assumptions


def test_any_success_gated_path_from_an_ancestor_gates_the_edge():
    """Two routes from s_a to s_b; the second demands success. Walking with
    one shared 'seen' set would stop at s_a on the second route and lose it."""
    tasks, _ = _wf([("s_a", "cmd1", ""), ("cmd1", "s_b", ""),
                    ("s_a", "cmd2", "$s_a.Status = SUCCEEDED"), ("cmd2", "s_b", "")],
                   sessions=("s_a", "s_b"), others=[("cmd1", "Command"), ("cmd2", "Command")])
    assert tasks["s_b"]["dependsOn"] == [{"taskKey": "s_a"}]
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS"


def test_an_unconditional_chain_through_a_command_still_runs_on_completion():
    tasks, tr = _wf([("s_a", "cmd_archive", ""), ("cmd_archive", "s_b", "")],
                    sessions=("s_a", "s_b"), others=[("cmd_archive", "Command")])
    assert tasks["s_b"]["runIf"] == "ALL_DONE"
    assert [a for a in _all_done_claims(tr) if "s_a -> cmd_archive" in a]


def test_another_condition_on_a_hop_is_reported_like_a_direct_one():
    tasks, tr = _wf([("s_a", "cmd_archive", "$s_a.Status = FAILED"),
                     ("cmd_archive", "s_b", "")],
                    sessions=("s_a", "s_b"), others=[("cmd_archive", "Command")])
    assert tasks["s_b"]["runIf"] == "ALL_DONE"
    line = next(r for r in tr.not_translated if "Link condition" in r)
    assert "s_a -> cmd_archive" in line and "$s_a.Status = FAILED" in line
    assert "gates s_b" in line
    assert "COMPLETES" in line and "also run on success" in line


_WORKLET_XML = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F1">
<SESSION NAME="s_a" MAPPINGNAME="m_a" REUSABLE="YES"/>
<SESSION NAME="s_x" MAPPINGNAME="m_x" REUSABLE="YES"/>
<WORKLET NAME="wl_dims" REUSABLE="YES">
  <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
  <TASKINSTANCE NAME="s_x" TASKNAME="s_x" TASKTYPE="Session"/>
  <WORKFLOWLINK CONDITION="" FROMTASK="Start" TOTASK="s_x"/>
</WORKLET>
<WORKFLOW NAME="wf_worklet">
  <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
  <TASKINSTANCE NAME="s_a" TASKNAME="s_a" TASKTYPE="Session"/>
  <TASKINSTANCE NAME="wl_dims" TASKNAME="wl_dims" TASKTYPE="Worklet"/>
  <WORKFLOWLINK CONDITION="" FROMTASK="Start" TOTASK="s_a"/>
  <WORKFLOWLINK CONDITION="$s_a.Status = SUCCEEDED" FROMTASK="s_a" TOTASK="wl_dims"/>
</WORKFLOW>
</FOLDER></REPOSITORY></POWERMART>
"""


def test_a_succeeded_link_into_a_worklet_gates_the_worklet_sessions(tmp_path):
    """The link lands on the worklet's Start inside it, which is not a job
    task -- the same pass-through case as a Command."""
    path = tmp_path / "wf_worklet.xml"
    path.write_text(_WORKLET_XML, encoding="utf-8")
    wf = InformaticaXMLParser().parse(str(path)).workflows[0]
    tasks = {t["taskKey"]: t for t in WorkflowGenerator().generate(wf, {}).job["tasks"]}
    assert tasks["wl_dims__s_x"]["dependsOn"] == [{"taskKey": "s_a"}]
    assert tasks["wl_dims__s_x"]["runIf"] == "ALL_SUCCESS"


def test_mixed_links_report_the_divergence_not_a_matching_all_done():
    tasks, tr = _wf([("s_a", "s_b", "$s_a.Status = SUCCEEDED"), ("s_c", "s_b", "")])
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS"
    assert not _all_done_claims(tr), (
        f"says the s_c link runs on completion, but s_b is ALL_SUCCESS: {tr.assumptions}"
    )
    line = next((r for r in tr.not_translated if "Mixed incoming links on s_b" in r), None)
    assert line, tr.not_translated
    assert "s_c" in line and "skipped" in line and "PowerCenter" in line


def test_every_all_done_claim_is_about_a_task_that_got_all_done():
    tasks, tr = _wf([("s_a", "s_b", "$s_a.Status = SUCCEEDED"), ("s_c", "s_b", ""),
                     ("s_a", "s_c", "")])
    for claim in _all_done_claims(tr):
        for _frm, to in re.findall(r"(\w+) -> (\w+)", claim):
            assert tasks[to]["runIf"] == "ALL_DONE", (claim, tasks[to])


def test_an_unapplied_condition_into_an_all_success_task_says_so():
    """s_b is ALL_SUCCESS because of s_a's link, so the FAILED-only link
    from s_c never fires -- not 'runs on completion'."""
    tasks, tr = _wf([("s_a", "s_b", "$s_a.Status = SUCCEEDED"),
                     ("s_c", "s_b", "$s_c.Status = FAILED")])
    assert tasks["s_b"]["runIf"] == "ALL_SUCCESS"
    line = next(r for r in tr.not_translated if "Link condition" in r and "s_c -> s_b" in r)
    assert "runIf ALL_DONE" not in line and "ALL_SUCCESS" in line and "NEVER" in line


# ---------------------------------------------------------------------------
# 4./5. name clash -> job notebook path; Spark version gate
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def clash_out(tmp_path_factory):
    """Two exports of one mapping that disagree (ROUND to 2 vs 4 places),
    in the same folder, each with its own workflow."""
    from infa2aidp.migrator import format_run_summary, run_migration
    d = tmp_path_factory.mktemp("clash")
    raw = open(C08, encoding="cp1252").read()
    v2 = (raw.replace("CURRENCY_CODE)), 2)", "CURRENCY_CODE)), 4)")
             .replace('NAME="wf_m_txn_to_usd"', 'NAME="wf_m_txn_to_usd_v2"'))
    assert v2 != raw
    (d / "a").mkdir()
    (d / "b").mkdir()
    (d / "a" / "export_v1.xml").write_text(raw, encoding="cp1252")
    (d / "b" / "export_v2.xml").write_text(v2, encoding="cp1252")
    out = d / "out"
    result = run_migration([str(d / "a" / "export_v1.xml"), str(d / "b" / "export_v2.xml")],
                           str(out), use_llm=False, skip_lineage=True,
                           skip_optimize=True, score_confidence=False)
    return str(out), result, format_run_summary(result)


def _job_paths(out, name):
    with open(os.path.join(out, "workflows", f"{name}.json"), encoding="utf-8") as f:
        return [t["notebookPath"] for t in json.load(f)["tasks"]]


def test_after_a_name_clash_each_job_runs_its_own_notebook(clash_out):
    out, result, _ = clash_out
    assert result.notebook_collisions
    assert _job_paths(out, "wf_m_txn_to_usd") == [
        "/Workspace/Migrated/SALES_DM/nb_m_txn_to_usd.ipynb"]
    assert _job_paths(out, "wf_m_txn_to_usd_v2") == [
        "/Workspace/Migrated/SALES_DM/nb_m_txn_to_usd__2.ipynb"]
    # And the notebook that path names is the v2 export's (4 places).
    with open(os.path.join(out, "SALES_DM", "nb_m_txn_to_usd__2.ipynb"), encoding="utf-8") as f:
        assert ", 4)" in f.read()


def test_the_name_clash_summary_says_which_job_runs_which_notebook(clash_out):
    _, _, summary = clash_out
    assert "NAME CLASH" in summary
    assert "wf_m_txn_to_usd -> SALES_DM/nb_m_txn_to_usd.ipynb" in summary, summary
    assert "wf_m_txn_to_usd_v2 -> SALES_DM/nb_m_txn_to_usd__2.ipynb" in summary, summary


class _FakeClient:
    """Records calls; reports one USER cluster on the given Spark version."""

    def __init__(self, spark):
        self.workspace_key = "ws-1"
        self.calls: list = []
        self.spark = spark

    def list_clusters(self):
        return [{"key": "cluster-9", "displayName": "c", "type": "USER", "state": "ACTIVE",
                 "clusterRuntimeConfig": {"sparkVersion": self.spark}}]

    def mkdir(self, p, workspace_key=None):
        self.calls.append(("mkdir", p))

    def object_exists(self, p, workspace_key=None):
        return False

    def upload_file(self, ws, lp, rp, overwrite=True):
        self.calls.append(("upload", rp))

    def find_job_by_name(self, n, workspace_key=None):
        return None

    def create_job(self, ws, cfg):
        self.calls.append(("create_job", cfg))
        return {"key": "j"}

    def update_job(self, ws, k, cfg):
        return {"key": k}


def _deploy(out, spark, fake=None):
    from infa2aidp.deployer.deployer import AIDPDeployer
    from infa2aidp.deployer.models import DeployConfig
    fake = fake or _FakeClient(spark)
    dep = AIDPDeployer(DeployConfig(region="us-ashburn-1", instance_id="ocid1.x",
                                    workspace_key="ws-1", cluster_key="cluster-9",
                                    overwrite=True), client=fake)
    return dep.deploy(out, os.path.join(out, "workflows")), fake


def test_the_deployed_v2_job_runs_the_v2_notebook(clash_out):
    out = clash_out[0]
    result, fake = _deploy(out, "3.5.0")
    assert result.total_failed == 0
    jobs = {c[1]["name"]: [t["notebookPath"] for t in c[1]["tasks"]]
            for c in fake.calls if c[0] == "create_job"}
    assert jobs["wf_m_txn_to_usd_v2"] == ["/Workspace/Migrated/SALES_DM/nb_m_txn_to_usd__2.ipynb"]
    assert jobs["wf_m_txn_to_usd"] == ["/Workspace/Migrated/SALES_DM/nb_m_txn_to_usd.ipynb"]


@pytest.mark.parametrize("raw,expected", [
    ("3.5", (3, 5, 0)), ("3.5.x", (3, 5, 0)), ("3", (3, 0, 0)), ("3.5.0", (3, 5, 0)),
    ("3.5.1", (3, 5, 1)), ("4.0.0-preview", (4, 0, 0)), ("custom-build", None),
    ("x.5", None), ("", None), (None, None),
])
def test_spark_versions_are_padded_to_three_parts(raw, expected):
    from infa2aidp.spark_target import parse_spark_version
    assert parse_spark_version(raw) == expected


@pytest.mark.parametrize("spark", ["3.5", "3.5.x", "3.5.0", "4.0"])
def test_a_3_5_or_newer_cluster_is_not_refused(clash_out, spark):
    result, fake = _deploy(clash_out[0], spark)
    assert result.total_failed == 0
    assert [c for c in fake.calls if c[0] == "create_job"]


@pytest.mark.parametrize("spark", ["3.4", "3.4.x", "3.4.1"])
def test_an_older_cluster_is_still_refused_before_upload(clash_out, spark):
    fake = _FakeClient(spark)
    with pytest.raises(ValueError, match="at least 3.5.0"):
        _deploy(clash_out[0], spark, fake)
    assert not [c for c in fake.calls if c[0] == "upload"]
