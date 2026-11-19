"""The phase structure: every step of a run is a discrete, reported phase.

STAGES in report/stages.py is the one ordered list of the pipeline. The board,
the phase report, the diagram, the token roll-up and the "what can run now"
check all read it, so none of them can disagree about what the phases are.
"""
import json
import re
import pathlib

from report.stages import STAGES, UTILITY_COMMANDS, build_stage_board

ENGINE = pathlib.Path(__file__).resolve().parents[1]


def _cli_commands():
    return set(re.findall(r'sub\.add_parser\(\s*"([a-z-]+)"',
                          (ENGINE / "snowmig.py").read_text()))


def _write(out, name, payload):
    (out / name).write_text(json.dumps(payload))


# ------------------------------------------------------ A1: coverage

def test_every_cli_command_is_a_phase_or_a_named_utility():
    covered = {s["command"] for s in STAGES} | set(UTILITY_COMMANDS)
    missing = sorted(_cli_commands() - covered)
    assert not missing, f"CLI commands with no phase: {missing}"


def test_the_in_aidp_workflows_are_phases_of_their_own():
    """S6 and S10 run as AIDP jobs through `run`. A flat `run` hides which
    job ran, so each workflow is its own phase with its own artifact."""
    by = {s["stage"]: s for s in STAGES}
    assert by["discover-workflow"]["artifact"] == "run_snowmig_00_discover.json"
    assert by["structure-workflow"]["artifact"] == "run_snowmig_01_structure.json"
    assert by["copy-workflow"]["optional"] is True
    assert by["discover-workflow"]["command"] == "run"


def test_every_phase_names_its_runbook_step_and_phase():
    for s in STAGES:
        assert s.get("phase") in ("setup", "discovery", "planning", "target",
                                  "reporting", "teardown"), s["stage"]
        assert "runbook" in s, s["stage"]


def test_stage_names_are_unique():
    names = [s["stage"] for s in STAGES]
    assert len(names) == len(set(names))


def test_a_workflow_run_reads_as_done_with_its_verdict(tmp_path):
    _write(tmp_path, "run_snowmig_00_discover.json",
           {"status": "SUCCESS", "ok": True, "job": "snowmig_00_discover"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "discover-workflow")
    assert row["status"] == "DONE" and "SUCCESS" in row["found"]
    assert row["attention"] is False


def test_a_workflow_that_did_not_succeed_is_flagged(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"status": "RUNNING", "ok": False, "terminal": False})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "structure-workflow")
    assert row["attention"] is True and "RUNNING" in row["found"]


def test_an_alternative_path_satisfies_its_twin(tmp_path):
    """`deploy` (catalog API) and `structure-workflow` both create the
    structure. Having done one, the board must not stall on the other."""
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"status": "SUCCESS", "ok": True})
    board = build_stage_board(tmp_path)
    deploy = next(r for r in board["stages"] if r["stage"] == "deploy")
    assert deploy["status"] == "SATISFIED"
    assert "structure-workflow" in deploy["found"]
    assert board["next_stage"] != "deploy"


def test_ingest_leaves_an_artifact_of_its_own(tmp_path):
    import snowmig
    manifest = tmp_path / "m.json"
    manifest.write_text(json.dumps({"schemas": [{"name": "S", "tables": [
        {"name": "T", "columns": [{"name": "C", "data_type": "NUMBER",
                                   "numeric_precision": 38,
                                   "numeric_scale": 0,
                                   "facts_recorded": True}]}]}]}))
    out = tmp_path / "out"
    assert snowmig.main(["ingest", "--out-dir", str(out), "--manifest",
                         str(manifest), "--database-name", "D"]) == 0
    rec = json.loads((out / "ingest_result.json").read_text())
    assert rec["objects"] == 1 and rec["database"] == "D"
    row = next(r for r in build_stage_board(out)["stages"]
               if r["stage"] == "ingest")
    assert row["status"] == "DONE"


# ------------------------------------------------------ A2: phase report

from report.stages import phase_report, stage_for  # noqa: E402
from report.tokens import record_stage_run  # noqa: E402


def test_a_workflow_run_is_named_by_its_job():
    assert stage_for("run", "snowmig_00_discover") == "discover-workflow"
    assert stage_for("run", "snowmig_01_structure") == "structure-workflow"
    assert stage_for("assess", None) == "assess"
    assert stage_for("run", "someone_elses_job") == "run"


def test_the_report_has_start_end_duration_and_verdict(tmp_path):
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", 0, None)
    record_stage_run(tmp_path, "plan", "2026-09-24T10:01:00+00:00",
                     "2026-09-24T10:01:02+00:00", 3, None)
    rep = {r["stage"]: r for r in phase_report(tmp_path)["phases"]}
    a = rep["assess"]
    assert a["started_at"].startswith("2026-09-24T10:00:00")
    assert a["duration_seconds"] == 30.0 and a["result"] == "PASS"
    assert rep["plan"]["result"] == "HALT"


def test_every_phase_appears_even_when_it_never_ran(tmp_path):
    rep = phase_report(tmp_path)
    assert [p["stage"] for p in rep["phases"]] == [s["stage"] for s in STAGES]
    by = {p["stage"]: p for p in rep["phases"]}
    assert by["assess"]["result"] == "NOT_RUN"
    assert by["copy-workflow"]["result"] == "SKIPPED (optional)"


def test_a_failed_run_is_fail_and_the_last_run_decides(tmp_path):
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:01+00:00", 1, None)
    record_stage_run(tmp_path, "ddl", "2026-09-24T10:05:00+00:00",
                     "2026-09-24T10:05:01+00:00", 0, None)
    ddl = next(p for p in phase_report(tmp_path)["phases"]
               if p["stage"] == "ddl")
    assert ddl["result"] == "PASS" and ddl["runs"] == 2 and ddl["failed_runs"] == 1


def test_an_artifact_with_no_logged_run_is_not_passed_off_as_measured(tmp_path):
    _write(tmp_path, "inventory.json", {"object_count": 1})
    a = next(p for p in phase_report(tmp_path)["phases"]
             if p["stage"] == "assess")
    assert a["result"] == "DONE (not logged)" and a["started_at"] is None


def test_a_workflow_run_logs_under_its_phase(tmp_path):
    record_stage_run(tmp_path, "discover-workflow", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:02:00+00:00", 0, None, job="snowmig_00_discover")
    d = next(p for p in phase_report(tmp_path)["phases"]
             if p["stage"] == "discover-workflow")
    assert d["result"] == "PASS" and d["duration_seconds"] == 120.0


def test_a_legacy_bare_run_is_matched_to_its_workflow_by_time(tmp_path):
    import os
    record_stage_run(tmp_path, "run", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:02:00+00:00", 0, None)
    art = tmp_path / "run_snowmig_00_discover.json"
    art.write_text(json.dumps({"status": "SUCCESS", "ok": True}))
    import datetime
    ts = datetime.datetime(2026, 9, 24, 10, 1, 59,
                           tzinfo=datetime.timezone.utc).timestamp()
    os.utime(art, (ts, ts))
    d = next(p for p in phase_report(tmp_path)["phases"]
             if p["stage"] == "discover-workflow")
    assert d["result"] == "PASS" and d["runs"] == 1


def test_main_logs_a_run_under_its_workflow_name(tmp_path, monkeypatch):
    import snowmig
    monkeypatch.setattr(snowmig, "cmd_run", lambda args: 0)
    parser = snowmig.build_parser()
    args = parser.parse_args(["run", "--out-dir", str(tmp_path), "--job",
                              "snowmig_01_structure"])
    assert snowmig._log_name(args) == "structure-workflow"


def test_the_phase_report_renders_every_phase(tmp_path):
    from report.render import render_phase_report
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", 0, None)
    md = render_phase_report(phase_report(tmp_path))
    assert "| Phase |" in md and "30.0s" in md and "PASS" in md
    for s in STAGES:
        assert f"`{s['stage']}`" in md


# ------------------------------------------------------ A3: diagram

from report.diagram import phase_diagram  # noqa: E402


def test_the_diagram_has_every_phase_in_pipeline_order():
    mmd = phase_diagram()
    assert mmd.startswith("%%") and "flowchart TB" in mmd
    for s in STAGES:
        assert f'"<b>{s["stage"]}</b>' in mmd
    # The pipeline order is the chain of edges; node declarations are
    # grouped by phase for readability.
    edges = [l.split() for l in mmd.splitlines()
             if " --> " in l or " ==> " in l]
    chain = [(e[0], e[2]) for e in edges]
    node = lambda n: n.upper().replace("-", "_")
    assert chain == [(node(a["stage"]), node(b["stage"]))
                     for a, b in zip(STAGES, STAGES[1:])]


def test_the_diagram_groups_phases_and_marks_writers_and_options():
    mmd = phase_diagram()
    for group in ("setup", "discovery", "planning", "target", "reporting"):
        assert f'subgraph PHASE_{group.upper()}' in mmd
    assert "class " in mmd and "writer" in mmd and "optional" in mmd


def test_the_diagram_shows_the_alternative_path():
    mmd = phase_diagram()
    assert "-. or .-" in mmd


def test_a_board_colours_the_diagram_by_status(tmp_path):
    (tmp_path / "plan.json").write_text("{}")
    mmd = phase_diagram(build_stage_board(tmp_path))
    done = [l for l in mmd.splitlines()
            if l.strip().startswith("class ") and l.strip().endswith(" done")]
    assert done and "PLAN" in done[0].split()[1].split(","), done


def test_the_committed_diagram_matches_the_code():
    """ARCHITECTURE.md embeds the generated diagram. If STAGES changes and
    the block is not refreshed (`snowmig stages --write-diagram`), this
    fails."""
    from report.diagram import embed_in_architecture
    text = (ENGINE.parent / "ARCHITECTURE.md").read_text(encoding="utf-8")
    assert embed_in_architecture(text) == text, (
        "the phase diagram in ARCHITECTURE.md is stale; refresh it with "
        "`bin/snowmig stages --write-diagram`")


# ------------------------------------------------------ A4: compute per phase

from report.stages import RUNS_ON  # noqa: E402


def test_every_phase_says_where_it_runs():
    for s in STAGES:
        assert s.get("runs_on") in RUNS_ON.values(), s["stage"]


def test_the_workflows_run_on_the_provisioned_migration_cluster():
    by = {s["stage"]: s for s in STAGES}
    for stage in ("discover-workflow", "structure-workflow", "copy-workflow",
                  "reconcile-workflow"):
        assert by[stage]["runs_on"] == RUNS_ON["migration_cluster"]
    for stage in ("provision", "catalog", "smoke"):
        assert by[stage]["runs_on"] == RUNS_ON["control_plane"]
    assert by["plan"]["runs_on"] == RUNS_ON["local"]
    assert by["assess"]["runs_on"] == RUNS_ON["local_snowflake"]


def test_deploy_names_both_of_its_transports():
    deploy = next(s for s in STAGES if s["stage"] == "deploy")
    assert deploy["runs_on"] == RUNS_ON["control_plane_or_configured"]
    assert "cluster_id" in RUNS_ON["control_plane_or_configured"]


def test_the_board_the_report_and_the_diagram_carry_it(tmp_path):
    from report.render import render_phase_report, render_stages
    md = render_stages(build_stage_board(tmp_path))
    assert "| Runs on |" in md
    assert "| Runs on |" in render_phase_report(phase_report(tmp_path))
    assert phase_report(tmp_path)["phases"][0]["runs_on"]
    assert RUNS_ON["migration_cluster"] in phase_diagram()


# ------------------------------------------------------ A5: tokens per phase

def test_the_token_phase_of_every_stage_comes_from_stages():
    from report.tokens import phase_of
    for s in STAGES:
        assert phase_of(s["stage"]) == s["phase"], s["stage"]


def test_tokens_for_a_workflow_land_in_its_phase(tmp_path):
    from report.tokens import build_token_report
    record_stage_run(tmp_path, "discover-workflow", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:02:00+00:00", 0, "s", job="snowmig_00_discover")
    projects = tmp_path / "p" / "-x"
    projects.mkdir(parents=True)
    (projects / "s.jsonl").write_text(json.dumps({
        "type": "assistant", "timestamp": "2026-09-24T10:01:00Z",
        "message": {"id": "m", "usage": {"output_tokens": 7}}}) + "\n")
    rep = build_token_report(tmp_path, projects_dir=tmp_path / "p")
    assert rep["by_phase"]["discovery"]["output"] == 7
    assert "other" not in rep["by_phase"]


def test_a_legacy_bare_run_is_attributed_to_its_workflow_phase(tmp_path):
    import os
    import datetime
    from report.tokens import build_token_report
    record_stage_run(tmp_path, "run", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:02:00+00:00", 0, "s")
    art = tmp_path / "run_snowmig_01_structure.json"
    art.write_text("{}")
    ts = datetime.datetime(2026, 9, 24, 10, 1, 30,
                           tzinfo=datetime.timezone.utc).timestamp()
    os.utime(art, (ts, ts))
    projects = tmp_path / "p" / "-x"
    projects.mkdir(parents=True)
    (projects / "s.jsonl").write_text(json.dumps({
        "type": "assistant", "timestamp": "2026-09-24T10:01:00Z",
        "message": {"id": "m", "usage": {"output_tokens": 3}}}) + "\n")
    rep = build_token_report(tmp_path, projects_dir=tmp_path / "p")
    assert "structure-workflow" in rep["by_stage"]
    assert rep["by_phase"]["target"]["output"] == 3


def test_every_phase_command_is_logged_by_main():
    """Only `clean` (deletes the log) and `tokens` (reads it) are exempt."""
    src = (ENGINE / "snowmig.py").read_text()
    assert "log_it = args.func not in (cmd_clean, cmd_tokens)" in src


# ------------------------------------------------------ A6: summary tokens

def _summary_inputs(out):
    _write(out, "plan.json", {"can_migrate": [], "cannot_migrate": [],
                              "bronze_mapping": "x"})
    _write(out, "inventory.json", {"inventory": [], "session": {}})


def _session(tmp_path, out):
    record_stage_run(out, "plan", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:10+00:00", 0, "s")
    proj = tmp_path / "p" / "-x"
    proj.mkdir(parents=True)
    lines = [{"type": "assistant", "timestamp": ts,
              "message": {"id": i, "usage": {"output_tokens": n}}}
             for i, ts, n in (("a", "2026-09-24T10:00:05Z", 5),
                              ("b", "2026-09-24T10:30:00Z", 900))]
    (proj / "s.jsonl").write_text("\n".join(json.dumps(l) for l in lines))
    return tmp_path / "p"


def _link_projects(home, projects):
    """Copy the fake transcripts to <home>/.claude/projects.

    A copy, not a symlink: a symlink needs admin rights or Developer Mode on
    Windows (WinError 1314), and the code under test only reads the
    directory."""
    import shutil
    shutil.copytree(projects, home / ".claude" / "projects")


def test_the_summary_ends_with_tokens_by_phase_and_a_grand_total(
        tmp_path, monkeypatch):
    import snowmig
    out = tmp_path / "out"
    out.mkdir()
    _summary_inputs(out)
    projects = _session(tmp_path, out)
    monkeypatch.setattr("report.tokens.pathlib.Path.home",
                        lambda: projects.parent / "home")
    _link_projects(projects.parent / "home", projects)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "s")
    assert snowmig.main(["summary", "--out-dir", str(out)]) == 0
    md = (out / "SUMMARY.md").read_text()
    section = md[md.index("## LLM token usage"):]
    assert "| planning |" in section and "| **Total** |" in section
    assert md.rstrip().endswith(section.rstrip()), "tokens close the summary"


def test_the_summary_honours_exclude_windows(tmp_path, monkeypatch):
    import snowmig
    out = tmp_path / "out"
    out.mkdir()
    _summary_inputs(out)
    projects = _session(tmp_path, out)
    monkeypatch.setattr("report.tokens.pathlib.Path.home",
                        lambda: projects.parent / "home")
    _link_projects(projects.parent / "home", projects)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "s")
    assert snowmig.main(["summary", "--out-dir", str(out), "--exclude-window",
                         "2026-09-24T10:20:00Z/2026-09-24T10:40:00Z"]) == 0
    tokens = json.loads((out / "tokens.json").read_text())
    assert tokens["excluded"]["excluded_windows"]["output"] == 900
    assert tokens["totals"]["output"] == 5


# ------------------------------------------------------ A9: what can run now

from plan.status import pipeline_status  # noqa: E402


def _status(tmp_path):
    return pipeline_status(build_stage_board(tmp_path))


def test_every_phase_declares_its_prerequisites():
    names = {s["stage"] for s in STAGES}
    for s in STAGES:
        for group in s.get("requires", []):
            assert group and set(group) <= names, (s["stage"], group)


def test_an_empty_run_counts_every_phase_and_unblocks_only_the_roots(tmp_path):
    st = _status(tmp_path)
    assert st["total"] == len(STAGES) and st["complete"] == []
    assert "assess" in st["unblocked"] and "provision" in st["unblocked"]
    assert "plan" not in st["unblocked"] and "ddl" not in st["unblocked"]
    assert "plan" in st["blocked"]


def test_source_already_extracted_unblocks_planning_without_a_laptop_assess(
        tmp_path):
    for name, payload in (("provision_result.json", {"dry_run": False}),
                          ("run_snowmig_00_discover.json",
                           {"status": "SUCCESS", "ok": True}),
                          ("ingest_result.json", {"objects": 3}),
                          ("dependencies.json", {})):
        _write(tmp_path, name, payload)
    st = _status(tmp_path)
    assert "plan" in st["unblocked"]
    assert "assess" in st["complete"], "ingest satisfies the inventory"
    assert "plan" not in st["blocked"]


def test_the_structure_path_needs_the_ddl_and_the_environment(tmp_path):
    _write(tmp_path, "ddl_plan.json", {"statements": [], "blocked": []})
    assert "structure-workflow" not in _status(tmp_path)["unblocked"]
    _write(tmp_path, "provision_result.json", {"dry_run": False})
    assert "structure-workflow" in _status(tmp_path)["unblocked"]


def test_a_failed_phase_is_not_complete_and_blocks_what_follows(tmp_path):
    _write(tmp_path, "run_snowmig_00_discover.json",
           {"status": "FAILED", "ok": False})
    st = _status(tmp_path)
    assert "discover-workflow" not in st["complete"]
    assert "discover-workflow" in st["needs_attention"]
    assert "ingest" not in st["unblocked"]


def test_the_board_renders_what_can_run_now(tmp_path):
    from report.render import render_stages
    md = render_stages(build_stage_board(tmp_path))
    assert "## What can run now" in md
    assert f"0 of {len(STAGES)} phase(s) complete" in md


# ------------------------------- PHASES.md: phases first, stages within

def test_the_report_rolls_stages_up_into_phases(tmp_path):
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", 0, None)
    record_stage_run(tmp_path, "deps", "2026-09-24T10:01:00+00:00",
                     "2026-09-24T10:01:10+00:00", 1, None, retries=2)
    rep = phase_report(tmp_path)
    by = {p["phase"]: p for p in rep["phase_summary"]}
    d = by["discovery"]
    assert d["stages"] == len([s for s in STAGES if s["phase"] == "discovery"])
    assert d["passed"] == 1 and d["failed"] == 1
    assert d["duration_seconds"] == 40.0 and d["retries"] == 2
    assert d["verdict"] == "FAIL", "one failed stage fails its phase"
    assert [p["phase"] for p in rep["phase_summary"]] == \
        list(dict.fromkeys(s["phase"] for s in STAGES))


def test_a_phase_with_nothing_run_says_so(tmp_path):
    rep = phase_report(tmp_path)
    by = {p["phase"]: p for p in rep["phase_summary"]}
    assert by["planning"]["verdict"] == "NOT_RUN"
    assert by["teardown"]["verdict"] == "SKIPPED (optional)"


def test_phases_md_lists_every_phase_with_its_stages_nested(tmp_path):
    from report.render import render_phase_report
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", 0, None)
    md = render_phase_report(phase_report(tmp_path))
    assert "## Phases at a glance" in md and "| Phase | Verdict |" in md
    order = list(dict.fromkeys(s["phase"] for s in STAGES))
    heads = [md.index(f"## Phase: {p}") for p in order]
    assert heads == sorted(heads)
    disc = md[md.index("## Phase: discovery"):md.index("## Phase: planning")]
    assert "| Stage |" in disc and "`assess`" in disc and "`plan`" not in disc
    for s in STAGES:
        assert f"`{s['stage']}`" in md


def test_no_subgraph_id_is_also_a_node_id():
    """GitHub refused to render the diagram: the `teardown` phase and the
    `teardown` stage both became TEARDOWN, which Mermaid reads as a cycle."""
    import re as _re
    mmd = phase_diagram()
    subgraphs = set(_re.findall(r"^\s*subgraph (\w+)\[", mmd, _re.M))
    nodes = set(_re.findall(r"^\s{4}(\w+)\[", mmd, _re.M))
    assert subgraphs and nodes and not subgraphs & nodes, subgraphs & nodes
