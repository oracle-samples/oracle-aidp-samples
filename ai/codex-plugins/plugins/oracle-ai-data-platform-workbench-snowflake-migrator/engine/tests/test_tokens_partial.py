"""A stage whose transcript was not read is not measured -- never zero.

build_token_report said `measured: false` only when no stage had a session
id at all, or no transcript was found for ANY session. Once one transcript
was found the whole report was `measured: true`, and every run under
another session (resumed the next day, transcripts pruned) or run by hand
got an all-zero bucket. TOKENS.md, tokens.json and the SUMMARY token
section showed **0** for those stages, and the grand total silently left
them out -- understating the cost and contradicting the promise, in this
module and in the stage-board skill, that such a stage is "reported as not
measured, never as zero".
"""
import json

from report.tokens import build_token_report, record_stage_run, render_tokens, \
    tokens_section


def _msg(mid, ts, out):
    return {"type": "assistant", "timestamp": ts,
            "message": {"id": mid, "model": "m", "usage": {
                "input_tokens": 0, "output_tokens": out,
                "cache_read_input_tokens": 0,
                "cache_creation_input_tokens": 0}}}


def _three_sessions(tmp_path, projects=None):
    out = tmp_path / "out"
    record_stage_run(out, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:10+00:00", 0, "sess-A")
    # Resumed the next day under a session whose transcript is gone.
    record_stage_run(out, "plan", "2026-09-25T09:00:00+00:00",
                     "2026-09-25T09:00:05+00:00", 0, "sess-B")
    # Run by hand, outside any session.
    record_stage_run(out, "ddl", "2026-09-25T09:10:00+00:00",
                     "2026-09-25T09:10:02+00:00", 0, None)
    projects = projects or tmp_path / "projects"
    (projects / "-p").mkdir(parents=True)
    (projects / "-p" / "sess-A.jsonl").write_text(
        json.dumps(_msg("a", "2026-09-24T10:00:05Z", 1010)) + "\n")
    return build_token_report(out, projects_dir=projects)


def test_runs_whose_transcript_was_not_read_are_not_measured(tmp_path):
    rep = _three_sessions(tmp_path)
    runs = {r["stage"]: r for r in rep["runs"]}
    assert runs["assess"]["measured"] is True
    assert runs["assess"]["tokens"]["output"] == 1010
    for stage in ("plan", "ddl"):
        assert runs[stage]["measured"] is False, stage
        assert runs[stage]["tokens"] is None, "not measured is not zero"
        assert runs[stage]["unmeasured_reason"]
    assert "sess-B" in runs["plan"]["unmeasured_reason"]
    assert "outside" in runs["ddl"]["unmeasured_reason"]
    assert rep["by_stage"]["plan"]["measured"] is False
    assert rep["by_stage"]["plan"]["total"] is None
    assert rep["by_stage"]["assess"]["measured"] is True


def test_the_report_is_flagged_partial(tmp_path):
    rep = _three_sessions(tmp_path)
    assert rep["measured"] is True
    assert rep["partial"] is True
    assert rep["unmeasured_runs"] == 2
    assert rep["totals"]["output"] == 1010


def test_tokens_md_says_not_measured_and_never_zero(tmp_path):
    rep = _three_sessions(tmp_path)
    md = render_tokens(rep)
    assert "partial: 2 stage run(s) not measured" in md.lower()
    section = "\n".join(tokens_section(rep))
    assert "partial: 2 stage run(s) not measured" in section.lower()
    for text in (md, section):
        rows = [l for l in text.splitlines()
                if l.startswith("|") and ("`plan`" in l or "`ddl`" in l)]
        assert rows, text
        for line in rows:
            assert "not measured" in line, line
            assert "**0**" not in line, line


def test_a_fully_measured_report_is_not_partial(tmp_path):
    out = tmp_path / "out"
    record_stage_run(out, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:10+00:00", 0, "sess-A")
    projects = tmp_path / "projects"
    (projects / "-p").mkdir(parents=True)
    (projects / "-p" / "sess-A.jsonl").write_text(
        json.dumps(_msg("a", "2026-09-24T10:00:05Z", 7)) + "\n")
    rep = build_token_report(out, projects_dir=projects)
    assert rep["partial"] is False and rep["unmeasured_runs"] == 0
    assert "partial" not in render_tokens(rep).lower()


def test_the_tokens_cli_prints_not_measured_for_those_stages(
        tmp_path, capsys, monkeypatch):
    import snowmig
    home = tmp_path / "home"
    _three_sessions(tmp_path, projects=home / ".claude" / "projects")
    monkeypatch.setattr("report.tokens.pathlib.Path.home", lambda: home)
    assert snowmig.main(["tokens", "--out-dir", str(tmp_path / "out")]) == 0
    printed = capsys.readouterr().out
    plan = next(l for l in printed.splitlines() if l.strip().startswith("plan"))
    assert "not measured" in plan, plan
    assert "partial" in printed.lower()


def test_the_per_stage_snapshot_says_this_stage_was_not_measured(
        tmp_path, monkeypatch):
    from report.stage_output import publish_stage_output
    from test_stage_output import FakeCall
    home = tmp_path / "home"
    _three_sessions(tmp_path, projects=home / ".claude" / "projects")
    monkeypatch.setattr("report.tokens.pathlib.Path.home", lambda: home)
    call = FakeCall()
    publish_stage_output(call, "ws-fake", tmp_path / "out", "report/output")
    snap = json.loads(next(body for path, body in call.uploaded
                           if path.endswith("_ddl.json")))
    assert snap["tokens"] is None
    assert snap["tokens_measured"] is False
    assert "outside" in snap["tokens_note"]
    assert snap["tokens_partial"] is True
