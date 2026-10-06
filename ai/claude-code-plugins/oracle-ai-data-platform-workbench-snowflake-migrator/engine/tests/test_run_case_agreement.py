"""One decision for every reading of a job run.

cmd_run's exit branches, RUN.md and the stage board each used to test the
same flags in their own hand-kept order; round 3's verdicts became dead
code on the board that way. run_case() now decides, and all three word the
case. These pin that each case reads as itself everywhere.
"""
import pytest

import snowmig
from report.stages import RUN_CASES, run_case, run_verdict

BASE = {"job": "snowmig_01_structure", "job_key": "k", "run_key": "r",
        "workspace": "ws", "status": "RUNNING", "terminal": False,
        "ok": False, "restarts": [], "polls": 3}

RECORDS = {
    "unreadable": {**BASE, "status_unreadable": True,
                   "status_error": "503"},
    "cold_start_exhausted": {**BASE, "cold_start_exhausted": {
        "run": "r", "after_seconds": 120.0, "cancel_state": "CANCELED",
        "cancel_error": None}},
    "unrecognised": {**BASE, "status": "WEIRD", "unrecognised": True},
    "cancel_unconfirmed": {**BASE, "cancel_unconfirmed": True},
    "still_running": dict(BASE),
    "success": {**BASE, "status": "SUCCESS", "terminal": True, "ok": True},
    "failed": {**BASE, "status": "FAILED", "terminal": True, "ok": False},
}

# What RUN.md's verdict line must say, and the board's kind, per case.
RUN_MD = {"unreadable": "STATUS COULD NOT BE READ",
          "cold_start_exhausted": "COLD START",
          "unrecognised": "UNRECOGNISED STATE",
          "cancel_unconfirmed": "cancel unconfirmed",
          "still_running": "STILL RUNNING",
          "success": "**SUCCESS**",
          "failed": "**FAILED**"}
KIND = {"unreadable": "unknown", "cold_start_exhausted": "failed",
        "unrecognised": "unknown", "cancel_unconfirmed": "unknown",
        "still_running": "running", "success": "success",
        "failed": "failed"}


def test_every_case_has_a_record_here():
    assert set(RECORDS) == set(RUN_CASES)


@pytest.mark.parametrize("case", RUN_CASES)
def test_each_record_reads_as_its_case_everywhere(case):
    record = RECORDS[case]
    assert run_case(record) == case
    verdict_line = snowmig._render_run(record).splitlines()[2]
    assert RUN_MD[case] in verdict_line, verdict_line
    assert run_verdict(record)[1] == KIND[case]


def test_unreadable_outranks_every_other_flag():
    record = {**RECORDS["cold_start_exhausted"], "status_unreadable": True,
              "unrecognised": True}
    assert run_case(record) == "unreadable"


def test_a_record_without_terminal_is_read_as_terminal():
    assert run_case({"status": "SUCCESS", "ok": True}) == "success"
