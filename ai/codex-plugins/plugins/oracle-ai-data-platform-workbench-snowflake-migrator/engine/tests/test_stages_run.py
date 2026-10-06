"""`run` writes to AIDP, and the stage board has to say so.

STAGES.md and the stage-board skill said three stages write -- provision,
catalog and deploy, each a dry run without --execute -- and "Every other
stage is read-only". `run` was never on the board: it has no --execute
(`run --execute` is an unrecognised argument), and a single `run --job
snowmig_02_copy_schema` starts a job that copies rows on the cluster, while
`run --job snowmig_01_structure` creates schemas and tables. PRIVACY.md
already listed `run` as a writer. After jobs had run, the board showed no
row for them and said "Every stage has run". The skill tells the agent to
answer a nervous user with that read-only sentence.
"""
import json
import pathlib

from report.render import render_stages
from report.stages import STAGES, build_stage_board

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _write(d, name, data):
    (d / name).write_text(json.dumps(data), encoding="utf-8")


def test_run_is_a_writing_stage_on_the_board():
    # The board now names the WORKFLOWS the `run` command starts rather than
    # the command itself, which is a finer statement of the same fact. What
    # must hold is unchanged: a stage that starts a job is on the board, it
    # is marked as writing, and it is optional.
    specs = {s["stage"]: s for s in STAGES if s.get("command") == "run"}
    assert specs, "no stage is driven by `run`"
    # Finer than "run writes": the board says WHICH job writes. Structure
    # creates schemas and tables and copy moves rows; discover and
    # reconcile only read. The defect was a writing stage being invisible,
    # and that is what must not come back.
    for name in ("structure-workflow", "copy-workflow"):
        assert specs[name]["writes"] is True, name
        assert specs[name].get("optional"), \
            "a structure-only clone never runs a job"
    for name in ("discover-workflow", "reconcile-workflow"):
        assert specs[name]["writes"] is False, f"{name} only reads"


def test_the_preamble_names_run_as_a_writer_with_no_dry_run(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    preamble = md.split("| Stage |", 1)[0]
    assert "no dry run" in preamble
    assert "Three stages write" not in preamble
    assert "`copy-workflow`" in preamble and "`structure-workflow`" in preamble
    row = next(l for l in md.splitlines()
               if l.startswith("| `copy-workflow`"))
    assert "(writes)" in row


def test_a_job_run_shows_on_the_board(tmp_path):
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": True, "ok": True,
            "status": "SUCCEEDED"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "copy-workflow")
    assert row["status"] == "DONE"
    # The job is named by the STAGE now, so the cell carries the state.
    assert "SUCCEEDED" in row["found"]
    assert row["attention"] is False


def test_a_failed_or_unfinished_job_run_needs_attention(tmp_path):
    _write(tmp_path, "run_snowmig_01_structure.json",
           {"job": "snowmig_01_structure", "terminal": True, "ok": False,
            "status": "FAILED"})
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": False, "ok": False,
            "status": "RUNNING"})
    rows = {r["stage"]: r for r in build_stage_board(tmp_path)["stages"]}
    assert rows["structure-workflow"]["attention"] is True
    assert "FAILED" in rows["structure-workflow"]["found"]
    assert rows["copy-workflow"]["attention"] is True
    # A run whose poll budget ran out is STILL RUNNING -- cmd_run exits 0
    # and says "not failed, not done" -- never a bare status that reads
    # like a verdict.
    assert "STILL RUNNING" in rows["copy-workflow"]["found"]


def test_the_stage_board_skill_names_run_as_a_writer():
    text = (ROOT / "skills/snowflake-stage-board/SKILL.md").read_text(
        encoding="utf-8")
    assert "Three stages write" not in text
    point = text.split("Then say three things out loud:", 1)[1].split("\n2. ", 1)[0]
    assert "`run`" in point and "no dry run" in point


def test_an_unrecognised_run_state_is_not_rounded_up_to_still_running(tmp_path):
    """A status this plugin does not classify is neither done nor running.

    watch_job keeps a separate `unrecognised` flag for a non-terminal run
    whose status is not one of the ACTIVE_STATES, and cmd_run prints
    UNRECOGNISED STATE for it and exits 1, saying that reporting STILL
    RUNNING "would round it up". The board read every non-terminal run as
    STILL RUNNING, so run_snowmig_02_copy_schema.json with status
    WEIRD_STATE was on the board as "snowmig_02_copy_schema: STILL RUNNING"
    -- an agent reading it would tell the user to wait for a run that may
    never finish.
    """
    _write(tmp_path, "run_snowmig_02_copy_schema.json",
           {"job": "snowmig_02_copy_schema", "terminal": False, "ok": False,
            "unrecognised": True, "status": "X"})
    row = next(r for r in build_stage_board(tmp_path)["stages"]
               if r["stage"] == "copy-workflow")
    assert "STILL RUNNING" not in row["found"]
    assert "UNRECOGNISED STATE X" in row["found"]
    assert row["attention"] is True


# ------------------------------------------------ one verdict for a job run
#
# After the fold no stage was named `run`, so the per-job branch above that
# said STILL RUNNING and UNRECOGNISED STATE was unreachable. Every workflow
# row took a generic branch -- "job run <status>" plus "after N cold-start
# restart(s)" -- that ignored terminal, unrecognised, cancel_unconfirmed,
# cold_start_exhausted and status_unreadable, and counted restarts whose
# new_run was None. So RUN.md said "Nothing ran" or "nothing was
# resubmitted" while the board said "job run RUNNING after 5 cold-start
# restart(s)", and the phase report turned a healthy run whose poll budget
# ran out (cmd_run exits 0) into "FAIL (job RUNNING)" with the target phase
# FAIL.

from report.stages import phase_report  # noqa: E402
from report.tokens import record_stage_run  # noqa: E402

_UNSTARTED = {"abandoned_run": None, "cancel_state": "CANCELING",
              "cancel_error": None, "new_run": None, "kept_run": "r-1",
              "after_seconds": 120.0}


def _structure(tmp_path, **record):
    base = {"job": "snowmig_01_structure", "run_key": "r-1",
            "terminal": False, "ok": False, "status": "RUNNING",
            "restarts": [], "unrecognised": False,
            "status_unreadable": False, "cold_start_exhausted": None,
            "cancel_unconfirmed": False}
    _write(tmp_path, "run_snowmig_01_structure.json", {**base, **record})
    return next(r for r in build_stage_board(tmp_path)["stages"]
                if r["stage"] == "structure-workflow")


def _resubmitted(n):
    return [{"abandoned_run": f"r-{i}", "cancel_state": "CANCELED",
             "cancel_error": None, "new_run": f"r-{i + 1}",
             "after_seconds": 120.0} for i in range(n)]


def test_cold_start_exhausted_says_nothing_ran_not_running(tmp_path):
    row = _structure(tmp_path, restarts=_resubmitted(5),
                     cold_start_exhausted={"run": "r-5",
                                           "after_seconds": 120.0,
                                           "cancel_state": "CANCELED",
                                           "cancel_error": None})
    assert "COLD START" in row["found"] and "nothing ran" in row["found"]
    assert "RUNNING" not in row["found"]
    assert "cold-start restart(s)" not in row["found"]
    assert row["attention"] is True and row["failed"] is True


def test_cancel_unconfirmed_is_not_a_restart_and_not_running(tmp_path):
    row = _structure(tmp_path, restarts=[_UNSTARTED] * 5,
                     cancel_unconfirmed=True)
    assert "cancel unconfirmed" in row["found"]
    assert "nothing was resubmitted" in row["found"]
    assert "RUNNING" not in row["found"]
    assert "cold-start restart(s)" not in row["found"], \
        "an attempt whose cancel failed resubmitted nothing"
    assert row["attention"] is True


def test_only_resubmitted_restarts_are_counted(tmp_path):
    row = _structure(tmp_path, terminal=True, ok=True, status="SUCCESS",
                     restarts=_resubmitted(2) + [_UNSTARTED])
    assert "after 2 cold-start restart(s)" in row["found"], row["found"]
    assert row["attention"] is False


def test_an_unreadable_status_is_not_a_verdict(tmp_path):
    row = _structure(tmp_path, status="UNREADABLE", status_unreadable=True)
    assert "STATUS UNREADABLE" in row["found"]
    assert "STILL RUNNING" not in row["found"]
    assert row["attention"] is True


def test_the_single_job_row_says_still_running_and_unrecognised(tmp_path):
    assert "STILL RUNNING" in _structure(tmp_path)["found"]
    row = _structure(tmp_path, status="WEIRD", unrecognised=True)
    assert "UNRECOGNISED STATE WEIRD" in row["found"]
    assert "STILL RUNNING" not in row["found"]


def test_a_run_still_going_is_not_a_phase_failure(tmp_path):
    record_stage_run(tmp_path, "structure-workflow",
                     "2026-09-24T10:00:00+00:00", "2026-09-24T10:10:00+00:00",
                     0, None, job="snowmig_01_structure")
    _structure(tmp_path)
    rep = phase_report(tmp_path)
    row = next(p for p in rep["phases"] if p["stage"] == "structure-workflow")
    assert not row["result"].startswith("FAIL"), row["result"]
    assert row["result"].startswith("STILL RUNNING"), row["result"]
    target = next(p for p in rep["phase_summary"] if p["phase"] == "target")
    assert target["verdict"] != "FAIL"
    assert target["passed"] == 0, "a run still going has not passed"
    assert target["verdict"] == "STILL RUNNING"


def test_an_unrecognised_state_in_the_phase_report_is_unknown(tmp_path):
    # cmd_run exits 1 here, and that exit is "neither done nor running",
    # not a failure the job reported.
    record_stage_run(tmp_path, "structure-workflow",
                     "2026-09-24T10:00:00+00:00", "2026-09-24T10:10:00+00:00",
                     1, None, job="snowmig_01_structure")
    _structure(tmp_path, status="WEIRD", unrecognised=True)
    row = next(p for p in phase_report(tmp_path)["phases"]
               if p["stage"] == "structure-workflow")
    assert row["result"].startswith("UNKNOWN"), row["result"]
    assert "UNRECOGNISED STATE WEIRD" in row["result"]


def test_a_terminal_failure_is_still_a_phase_failure(tmp_path):
    record_stage_run(tmp_path, "structure-workflow",
                     "2026-09-24T10:00:00+00:00", "2026-09-24T10:10:00+00:00",
                     1, None, job="snowmig_01_structure")
    _structure(tmp_path, terminal=True, status="FAILED")
    row = next(p for p in phase_report(tmp_path)["phases"]
               if p["stage"] == "structure-workflow")
    assert row["result"] == "FAIL (job FAILED)"


# ------------------------------------------- every writer, in every place
#
# The fold added `publish` and `teardown` as writing stages and the board's
# preamble said "Seven stages write", while the stage-board skill (point 1,
# the sentence the agent is told to say to a nervous user), ARCHITECTURE.md
# and this module's docstring still said "Four stages write ... Everything
# else is read-only" -- calling the stage that stops or deletes clusters,
# and the one that writes into the workspace, read-only.

_COUNT = {3: "Three", 4: "Four", 5: "Five", 6: "Six", 7: "Seven",
          8: "Eight", 9: "Nine"}


def _writers():
    return [s["stage"] for s in STAGES if s.get("writes")]


def _point_one():
    text = (ROOT / "skills/snowflake-stage-board/SKILL.md").read_text(
        encoding="utf-8")
    return text.split("Then say three things out loud:", 1)[1].split(
        "\n2. ", 1)[0]


def _architecture_writers():
    text = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
    start = text.index("stages write")
    return text[start - 40:text.index("\n\n", start)]


def test_the_writer_count_agrees_everywhere(tmp_path):
    word = _COUNT[len(_writers())]
    preamble = render_stages(build_stage_board(tmp_path)).split(
        "| Stage |", 1)[0]
    assert f"**{word} stages write" in preamble
    import report.stages as stages_module
    for where, text in (("SKILL.md point 1", _point_one()),
                        ("ARCHITECTURE.md", _architecture_writers()),
                        ("report/stages.py", stages_module.__doc__)):
        assert f"{word} stages write" in text, where
        for stale in ("Four stages write", "Three stages write"):
            assert stale not in text, (where, stale)


def test_every_writing_stage_is_named_as_a_writer():
    for where, text in (("SKILL.md point 1", _point_one()),
                        ("ARCHITECTURE.md", _architecture_writers())):
        for stage in _writers():
            assert f"`{stage}`" in text, (where, stage)
        assert "destructive" in text, \
            f"{where}: teardown stops or deletes clusters"
        assert "snowmig_02_copy_<schema>" in text, where
