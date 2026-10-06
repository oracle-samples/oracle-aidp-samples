"""LLM token accounting per stage and phase.

The engine makes no LLM call. The tokens are spent by the agent driving it,
so they are read from the agent's own transcript and attributed to the stage
each one was spent on.
"""
import json

import pytest

from report.tokens import (
    PHASES, attribute, build_token_report, find_transcripts, load_usage,
    phase_of, record_stage_run, render_tokens, tokens_section,
)


def _msg(mid, ts, out=10, inp=1, cread=100, ccreate=0, model="m",
         side=False):
    return {"type": "assistant", "timestamp": ts, "isSidechain": side,
            "message": {"id": mid, "model": model, "usage": {
                "input_tokens": inp, "output_tokens": out,
                "cache_read_input_tokens": cread,
                "cache_creation_input_tokens": ccreate}}}


def _write(path, events):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


# ---------------------------------------------------------------- the log

def test_each_stage_run_is_appended_to_the_run_log(tmp_path):
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:05+00:00", 0, "sess-1")
    record_stage_run(tmp_path, "plan", "2026-09-24T10:01:00+00:00",
                     "2026-09-24T10:01:01+00:00", 3, None)
    lines = [json.loads(l) for l in
             (tmp_path / "run_log.jsonl").read_text().splitlines()]
    assert [l["stage"] for l in lines] == ["assess", "plan"]
    assert lines[0]["claude_session_id"] == "sess-1"
    assert lines[1]["exit_code"] == 3
    assert lines[0]["phase"] == "discovery"


def test_the_log_records_no_argument_values(tmp_path):
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:05+00:00", 0, "s")
    rec = json.loads((tmp_path / "run_log.jsonl").read_text())
    assert set(rec) == {"stage", "phase", "started_at", "ended_at",
                        "exit_code", "claude_session_id"}


def test_every_stage_has_a_phase():
    for stage in ("preflight", "assess", "deps", "plan", "ddl", "smoke",
                  "catalog", "deploy", "run", "summary"):
        assert phase_of(stage) in PHASES
    assert phase_of("something-new") == "other"


# ------------------------------------------------------------- transcripts

def test_a_message_repeated_on_several_lines_is_counted_once(tmp_path):
    f = tmp_path / "s.jsonl"
    _write(f, [_msg("a", "2026-09-24T10:00:00Z", out=5),
               _msg("a", "2026-09-24T10:00:00Z", out=5),
               _msg("a", "2026-09-24T10:00:01Z", out=7)])
    usage = load_usage([f])
    assert len(usage) == 1
    assert usage[0]["output"] == 7, "the fullest usage of a message wins"


def test_non_assistant_and_malformed_lines_are_ignored(tmp_path):
    f = tmp_path / "s.jsonl"
    f.write_text('{"type":"user"}\nnot json\n'
                 + json.dumps(_msg("a", "2026-09-24T10:00:00Z")) + "\n")
    assert len(load_usage([f])) == 1


def test_the_session_and_its_subagents_are_found(tmp_path):
    proj = tmp_path / "projects" / "-some-project"
    _write(proj / "sess-1.jsonl", [])
    _write(proj / "sess-1" / "subagents" / "agent-x.jsonl", [])
    _write(proj / "other.jsonl", [])
    found = sorted(p.name for p in find_transcripts("sess-1",
                                                    tmp_path / "projects"))
    assert found == ["agent-x.jsonl", "sess-1.jsonl"]


def test_an_unknown_session_finds_nothing(tmp_path):
    assert find_transcripts("nope", tmp_path) == []
    assert find_transcripts(None, tmp_path) == []


# ------------------------------------------------------------- attribution

RUNS = [
    {"stage": "assess", "phase": "discovery",
     "started_at": "2026-09-24T10:00:00+00:00",
     "ended_at": "2026-09-24T10:00:10+00:00", "claude_session_id": "s"},
    {"stage": "plan", "phase": "planning",
     "started_at": "2026-09-24T10:05:00+00:00",
     "ended_at": "2026-09-24T10:05:01+00:00", "claude_session_id": "s"},
]


def _usage(*pairs):
    """(timestamp, total output tokens) -> usage rows."""
    from report.tokens import _parse_ts
    return [{"id": f"m{i}", "ts": _parse_ts(ts), "model": "m",
             "input": 0, "output": out, "cache_read": 0, "cache_creation": 0,
             "sidechain": False} for i, (ts, out) in enumerate(pairs)]


def test_tokens_before_a_stage_ends_belong_to_that_stage():
    rep = attribute(RUNS, _usage(("2026-09-24T10:00:05Z", 10),   # in assess
                                 ("2026-09-24T10:03:00Z", 20),   # preparing plan
                                 ("2026-09-24T10:05:00Z", 30)))  # in plan
    by = {r["stage"]: r["tokens"]["output"] for r in rep["runs"]}
    assert by == {"assess": 10, "plan": 50}


def test_tokens_outside_the_run_are_excluded_and_counted():
    rep = attribute(RUNS, _usage(("2026-09-24T09:00:00Z", 7),    # before
                                 ("2026-09-24T10:00:05Z", 10),
                                 ("2026-09-24T11:00:00Z", 9)))   # after
    assert rep["totals"]["output"] == 10
    assert rep["excluded"]["before_first_stage"]["output"] == 7
    assert rep["excluded"]["after_last_stage"]["output"] == 9


def test_since_pulls_the_setup_into_the_first_stage():
    rep = attribute(RUNS, _usage(("2026-09-24T09:59:00Z", 7),
                                 ("2026-09-24T10:00:05Z", 10)),
                    since="2026-09-24T09:58:00+00:00")
    assert rep["runs"][0]["tokens"]["output"] == 17


def test_totals_roll_up_by_stage_and_by_phase():
    runs = RUNS + [{"stage": "assess", "phase": "discovery",
                    "started_at": "2026-09-24T10:06:00+00:00",
                    "ended_at": "2026-09-24T10:06:10+00:00",
                    "claude_session_id": "s"}]
    rep = attribute(runs, _usage(("2026-09-24T10:00:05Z", 10),
                                 ("2026-09-24T10:05:00Z", 30),
                                 ("2026-09-24T10:06:05Z", 5)))
    assert rep["by_stage"]["assess"]["output"] == 15
    assert rep["by_stage"]["assess"]["runs"] == 2
    assert rep["by_phase"]["discovery"]["output"] == 15
    assert rep["by_phase"]["planning"]["output"] == 30
    assert rep["totals"]["output"] == 45


def test_total_is_every_token_kind_summed():
    from report.tokens import _parse_ts
    u = [{"id": "x", "ts": _parse_ts("2026-09-24T10:00:05Z"), "model": "m",
          "input": 1, "output": 2, "cache_read": 3, "cache_creation": 4,
          "sidechain": True}]
    rep = attribute(RUNS, u)
    t = rep["runs"][0]["tokens"]
    assert t["total"] == 10 and t["messages"] == 1 and t["subagent_messages"] == 1


def test_an_in_flight_stage_is_attributed_up_to_now():
    rep = attribute(RUNS, _usage(("2026-09-24T10:07:00Z", 4)),
                    in_flight={"stage": "summary",
                               "started_at": "2026-09-24T10:08:00+00:00",
                               "ended_at": "2026-09-24T10:08:00+00:00"})
    assert rep["by_stage"]["summary"]["output"] == 4


# ------------------------------------------------------------- end to end

def test_the_report_reads_the_log_and_the_transcript(tmp_path):
    out = tmp_path / "out"
    for r in RUNS:
        record_stage_run(out, r["stage"], r["started_at"], r["ended_at"], 0, "s")
    projects = tmp_path / "projects"
    _write(projects / "-p" / "s.jsonl",
           [_msg("a", "2026-09-24T10:00:05Z", out=10, cread=0, inp=0)])
    _write(projects / "-p" / "s" / "subagents" / "agent-1.jsonl",
           [_msg("b", "2026-09-24T10:03:00Z", out=20, cread=0, inp=0,
                 side=True)])
    rep = build_token_report(out, projects_dir=projects)
    assert rep["measured"] is True
    assert rep["by_stage"]["plan"]["output"] == 20
    assert rep["totals"]["output"] == 30


def test_no_log_means_not_measured_rather_than_zero(tmp_path):
    rep = build_token_report(tmp_path, projects_dir=tmp_path)
    assert rep["measured"] is False
    assert "run_log.jsonl" in rep["reason"]


def test_a_run_by_hand_has_no_session_and_is_not_measured(tmp_path):
    record_stage_run(tmp_path, "assess", RUNS[0]["started_at"],
                     RUNS[0]["ended_at"], 0, None)
    rep = build_token_report(tmp_path, projects_dir=tmp_path)
    assert rep["measured"] is False
    assert "session" in rep["reason"].lower()


def test_the_report_renders_per_stage_and_per_phase():
    rep = attribute(RUNS, _usage(("2026-09-24T10:00:05Z", 10)))
    rep["measured"] = True
    md = render_tokens(rep)
    assert "## By stage" in md and "## By phase" in md
    assert "`assess`" in md and "discovery" in md
    section = "\n".join(tokens_section(rep))
    assert "## LLM token usage" in section and "Total" in section


def test_the_summary_section_says_when_nothing_was_measured():
    section = "\n".join(tokens_section({"measured": False,
                                        "reason": "no run log"}))
    assert "not measured" in section.lower() and "no run log" in section


def test_main_logs_every_stage_with_its_session(tmp_path, monkeypatch):
    import snowmig
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "sess-xyz")
    assert snowmig.main(["stages", "--out-dir", str(tmp_path)]) == 0
    rec = json.loads((tmp_path / "run_log.jsonl").read_text().splitlines()[-1])
    assert rec["stage"] == "stages" and rec["exit_code"] == 0
    assert rec["claude_session_id"] == "sess-xyz"


def test_a_failing_stage_is_logged_with_its_exit_code(tmp_path, monkeypatch):
    import snowmig
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    assert snowmig.main(["plan", "--out-dir", str(tmp_path)]) == 1
    rec = json.loads((tmp_path / "run_log.jsonl").read_text().splitlines()[-1])
    assert rec["stage"] == "plan" and rec["exit_code"] == 1
    assert rec["claude_session_id"] is None


def test_the_tokens_stage_does_not_log_itself(tmp_path):
    import snowmig
    snowmig.main(["tokens", "--out-dir", str(tmp_path)])
    assert not (tmp_path / "run_log.jsonl").exists()


def test_an_excluded_window_is_carved_out_and_counted():
    """The same session may do unrelated work between two stages; that is
    not the migration's cost."""
    rep = attribute(RUNS, _usage(("2026-09-24T10:00:05Z", 10),
                                 ("2026-09-24T10:03:00Z", 20),
                                 ("2026-09-24T10:05:00Z", 30)),
                    exclude=[("2026-09-24T10:02:00+00:00",
                              "2026-09-24T10:04:00+00:00")])
    assert rep["by_stage"]["plan"]["output"] == 30
    assert rep["excluded"]["excluded_windows"]["output"] == 20
    assert rep["totals"]["output"] == 40


def test_the_cli_takes_exclude_windows(tmp_path):
    import snowmig
    args = snowmig.build_parser().parse_args(
        ["tokens", "--exclude-window",
         "2026-09-24T10:02:00Z/2026-09-24T10:04:00Z"])
    assert args.exclude_window == ["2026-09-24T10:02:00Z/2026-09-24T10:04:00Z"]
