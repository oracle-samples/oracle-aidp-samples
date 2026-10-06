"""Two claims this branch makes, held to what they say.

* "A dry run never silently becomes a write": a task parameter spelled the
  way an operator types it in the console (`dryRun`, `dryrun`) was read by
  no lookup, so `dry-run` kept its default False and the stage wrote.
* "Nothing ran" after an exhausted cold start: the last cancel can read back
  a terminal state other than CANCELED -- the task started after the
  pick-up check and ended before the cancel landed. That run RAN, and
  "re-run" would repeat it (twice the rows for an append copy).
"""
import snowmig
from report.stages import cold_start_outcome, run_verdict
from target.stage_notebooks import STAGES, build_stage_notebook


def _params(stage_key, workflow, monkeypatch):
    for name in ("dry-run", "dry_run", "DRY_RUN", "schema", "SCHEMA"):
        monkeypatch.delenv(name, raising=False)
    stage = next(s for s in STAGES if s.key == stage_key)
    nb = build_stage_notebook(stage, overrides={"target-catalog": "lake",
                                                "schema": "S"})
    scope = {}

    class _P:
        @staticmethod
        def getParameter(name, default):
            return workflow.get(name, default)
    scope["oidlUtils"] = type("U", (), {"parameters": _P})
    exec(compile("".join(nb["cells"][1]["source"]), "<params>", "exec"), scope)
    return scope


def test_a_camel_case_dry_run_is_a_dry_run(monkeypatch):
    g = _params("copy_schema", {"dryRun": "true"}, monkeypatch)
    assert g["PARAMS"]["dry-run"] is True
    assert "--dry-run" in g["ARGV"]


def test_a_flat_dry_run_is_a_dry_run(monkeypatch):
    g = _params("copy_schema", {"dryrun": "true"}, monkeypatch)
    assert g["PARAMS"]["dry-run"] is True


def test_the_documented_spelling_still_wins(monkeypatch):
    g = _params("copy_schema", {"dry-run": "true", "dryRun": "false"},
                monkeypatch)
    assert g["PARAMS"]["dry-run"] is True


def test_only_a_confirmed_cancel_means_nothing_ran():
    assert cold_start_outcome({"cancel_state": "CANCELED"}) == "cancelled"
    assert cold_start_outcome({"cancel_state": "SUCCESS"}) == "ended"
    assert cold_start_outcome({"cancel_state": "FAILED"}) == "ended"
    assert cold_start_outcome({"cancel_state": "RUNNING"}) == "unconfirmed"
    assert cold_start_outcome({}) == "unconfirmed"


def _exhausted(state):
    return {"job": "snowmig_02_copy_sales", "status": "RUNNING",
            "terminal": False, "ok": False, "workspace": "ws",
            "restarts": [{"abandoned_run": "r1", "new_run": "r2",
                          "cancel_state": "CANCELED",
                          "after_seconds": 120.0}],
            "cold_start_exhausted": {"run": "r2", "after_seconds": 120.0,
                                     "cancel_state": state,
                                     "cancel_error": None}}


def test_run_md_says_a_run_that_ended_before_the_cancel_ran():
    md = snowmig._render_run(_exhausted("SUCCESS"))
    assert "Nothing ran" not in md
    assert "RAN" in md and "SUCCESS" in md


def test_run_md_still_says_nothing_ran_after_a_confirmed_cancel():
    assert "Nothing ran" in snowmig._render_run(_exhausted("CANCELED"))


def test_the_board_does_not_call_an_ended_run_nothing():
    verdict, kind = run_verdict(_exhausted("SUCCESS"))
    assert "nothing ran" not in verdict.lower()
    assert kind == "unknown"
    verdict, kind = run_verdict(_exhausted("CANCELED"))
    assert "nothing ran" in verdict.lower() and kind == "failed"
