"""Claude usage ledger: meters, attribution windows, cost estimate, effort knobs.

Every Claude call in the suite is mocked, so these pin the deterministic
accounting that JOB_REPORT and the per-task results are built from.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am  # noqa: E402


@pytest.fixture
def ledger(monkeypatch):
    fresh = []
    monkeypatch.setattr(am, "USAGE_LEDGER", fresh)
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    monkeypatch.delenv("AIDP_USAGE_RUN_ID", raising=False)
    return fresh


def _usage(inp=0, read=0, write=0, write_1h=0, out=0, thinking=0):
    return NS(input_tokens=inp, cache_read_input_tokens=read,
              cache_creation_input_tokens=write,
              cache_creation=NS(ephemeral_1h_input_tokens=write_1h),
              output_tokens=out, output_tokens_details=NS(thinking_tokens=thinking))


# ── meters ──

def test_record_usage_reads_every_meter(ledger):
    rec = am._record_usage("fix", _usage(inp=11, read=22, write=33, write_1h=4, out=55, thinking=6))
    assert {k: rec[k] for k in ("route", "input", "cache_read", "cache_write",
                                "cache_write_1h", "output", "thinking")} == {
        "route": "fix", "input": 11, "cache_read": 22, "cache_write": 33,
        "cache_write_1h": 4, "output": 55, "thinking": 6}
    assert ledger == [rec]


def test_record_usage_tolerates_partial_usage_objects(ledger):
    rec = am._record_usage("eval", NS(input_tokens=10, output_tokens=5))
    assert (rec["input"], rec["output"], rec["cache_read"], rec["thinking"]) == (10, 5, 0, 0)


def test_usage_delta_splits_by_route_and_bills_cache_traffic(ledger):
    mark = am.open_usage_window()
    am._record_usage("migrate", _usage(inp=100, read=1000, out=10))
    am._record_usage("eval", _usage(inp=50, write=200, out=5))
    am._record_usage("migrate", _usage(inp=100, out=10))
    u = am.usage_delta(mark)
    am.close_usage_window(mark)
    assert u["requests"] == 3
    assert u["billed_tokens"] == 100 + 1000 + 10 + 50 + 200 + 5 + 100 + 10
    assert {r: b["requests"] for r, b in u["by_route"].items()} == {"migrate": 2, "eval": 1}


# ── cost estimate ──

@pytest.mark.parametrize("model,usd", [("claude-opus-5-5", 30.4), ("claude-opus-5", 38.25)])
def test_cost_uses_the_active_models_rates(ledger, monkeypatch, model, usd):
    monkeypatch.setattr(am.llm_config, "MODEL", model)
    mark = am.open_usage_window()
    am._record_usage("fix", _usage(inp=1_000_000, read=1_000_000, write=1_000_000,
                                   write_1h=400_000, out=1_000_000))
    assert am.usage_delta(mark)["est_cost_usd"] == pytest.approx(usd)
    am.close_usage_window(mark)


def test_unknown_model_falls_back_and_the_report_note_says_so(monkeypatch):
    monkeypatch.setattr(am.llm_config, "MODEL", "claude-future-9")
    note = am.usage_rates_note()
    assert "claude-opus-5-5" in note
    assert "fallback; claude-future-9 not in the rate table" in note


# ── attribution windows ──

def test_usage_window_excludes_concurrent_sibling_tasks(ledger):
    """Sibling dependency notebooks migrate concurrently (asyncio.gather) against
    one ledger; each notebook's window must count only its own requests, while
    the enclosing job window counts all of them."""
    async def notebook(n):
        mark = am.open_usage_window()
        try:
            for _ in range(n):
                am._record_usage("migrate", _usage(inp=100, out=10))
                await asyncio.sleep(0)   # interleave with the sibling
            return am.usage_delta(mark)["requests"]
        finally:
            am.close_usage_window(mark)

    async def job():
        mark = am.open_usage_window()
        counts = await asyncio.gather(notebook(3), notebook(5))
        total = am.usage_delta(mark)["requests"]
        am.close_usage_window(mark)
        return counts, total

    assert asyncio.run(job()) == ([3, 5], 8)


def test_closed_window_stops_counting(ledger):
    first = am.open_usage_window()
    am._record_usage("migrate", _usage(inp=1))
    am.close_usage_window(first)
    second = am.open_usage_window()
    am._record_usage("migrate", _usage(inp=1))
    assert am.usage_delta(first)["requests"] == 1
    assert am.usage_delta(second)["requests"] == 1
    am.close_usage_window(second)


def test_job_report_counts_usage_outside_the_task_rows(ledger, monkeypatch, tmp_path):
    """Dependency notebooks and backfill run their own process_notebook calls
    whose result dicts never reach task_results; JOB_REPORT must still count
    their Claude requests."""
    from test_pip_magic_gate import _drive_reporting_job

    async def backfill(*a, **k):
        am._record_usage("migrate", _usage(inp=7000))
        return {"scanned": 1, "missing": 1, "migrated": 1, "failed": []}

    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                                            backfill=backfill)
    report = reports["JOB_REPORT.md"]
    assert "**Claude usage (whole job" in report
    assert "| migrate | 1 | 7,000 |" in report
    assert "Billed tokens not in the task rows above" in report
    assert "failed tasks): 7,000" in report


# ── effort knobs ──

def test_effort_knob_normalises_and_defaults_to_unset(monkeypatch):
    monkeypatch.setenv("AIDP_EFFORT_MIGRATE", "  Medium ")
    assert am._effort_env("AIDP_EFFORT_MIGRATE") == "medium"
    monkeypatch.delenv("AIDP_EFFORT_MIGRATE")
    assert am._effort_env("AIDP_EFFORT_MIGRATE") is None


def test_invalid_effort_knob_fails_fast(monkeypatch):
    """A typo must not silently run at the model default (the knob is a cost
    lever; a silent no-op invalidates the A/B it is set for)."""
    monkeypatch.setenv("AIDP_EFFORT_MIGRATE", "med")
    with pytest.raises(ValueError, match="AIDP_EFFORT_MIGRATE"):
        am._effort_env("AIDP_EFFORT_MIGRATE")


# ── #191 deep review advisory: which effort a run used is on record ──

def _scripted_client(monkeypatch, replies):
    """claude() answering with `replies` in order; returns the requests sent."""
    async def fake_tool(name, tool_input, **k):
        return "ok"

    monkeypatch.setattr(am, "_handle_tool_call", fake_tool)
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    replies, calls = iter(replies), []

    def create(**kwargs):
        calls.append(kwargs)
        return next(replies)

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    return calls


def _tool_round(n):
    return NS(content=[NS(type="tool_use", id=f"tu-{n}", name="explore_path", input={})],
              stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))


def _sent_effort(request):
    return (request.get("output_config") or {}).get("effort")


@pytest.mark.parametrize("model,sent,runs_at", [
    ("claude-opus-5-5", None, "medium"),     # nothing sent: the model's own default
    ("claude-opus-5", None, "high"),
    ("claude-opus-5-5", "low", "low"),       # a knob: sent, and that is the level
])
def test_each_tool_loop_request_records_the_effort_it_ran_at(ledger, monkeypatch, model, sent,
                                                             runs_at):
    """Nothing recorded which effort a run used, so "default effort" in the PR
    body rested on nothing. Each request records the exact level it ran at: the
    level it sent, else the model's documented default (the tool rounds and the
    forced final here)."""
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", model)
    final = NS(content=[NS(type="text", text="DONE")], stop_reason="end_turn",
               usage=NS(input_tokens=1, output_tokens=1))
    calls = _scripted_client(monkeypatch, [_tool_round(0), _tool_round(1), final])
    asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=1, effort=sent))
    assert [_sent_effort(c) for c in calls] == [sent] * 3       # requests unchanged
    assert [(r["route"], r["effort"]) for r in ledger] == [
        ("other", runs_at), ("other", runs_at), ("other_final", runs_at)]


def test_a_model_without_a_known_default_effort_is_unresolved_not_guessed(ledger, monkeypatch):
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-unknown-9")
    lines = []
    am._record_usage("eval", _usage(inp=1), log_fn=lines.append)
    am._record_usage("eval", _usage(inp=1), effort="low", log_fn=lines.append)
    assert [r["effort"] for r in ledger] == ["UNRESOLVED", "low"]
    assert "effort=UNRESOLVED" in lines[0].split()


def test_the_code_delivery_retry_records_its_effort(ledger, monkeypatch):
    delivered = NS(content=[NS(type="tool_use", id="tu-sub", name="submit_code",
                               input={"code": "x = 1"})],
                   stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    calls = _scripted_client(monkeypatch, [_tool_round(0), _tool_round(1), delivered])
    code, _ = asyncio.run(am.call_opus_with_tools("Migrate", "input", max_tool_rounds=1,
                                                  effort="low",
                                                  code_validator=lambda c: c == "x = 1"))
    assert code == "x = 1"
    assert _sent_effort(calls[-1]) == "low"
    assert (ledger[-1]["route"], ledger[-1]["effort"]) == ("other_delivery", "low")


def test_both_summary_requests_record_their_fixed_low_effort(ledger, monkeypatch):
    reply = NS(content=[NS(type="text", text="Reads a table.")], stop_reason="end_turn",
               usage=NS(input_tokens=1, output_tokens=1))
    handle_tool_call = am._handle_tool_call     # the real one: summarize_notebook is a tool
    calls = _scripted_client(monkeypatch, [reply, reply])
    monkeypatch.setattr(am, "_download_notebook_source",
                        lambda path: {"cells": [{"cell_type": "code", "source": ["x = 1"]}]})
    asyncio.run(am._summarize_cell_code("df = spark.table('t')"))
    asyncio.run(handle_tool_call("summarize_notebook", {"path": "/Workspace/nb"}))
    assert [_sent_effort(c) for c in calls] == ["low", "low"]
    assert [(r["route"], r["effort"]) for r in ledger] == [("summary", "low"), ("summary", "low")]


def test_the_usage_log_line_names_the_exact_effort(ledger, monkeypatch):
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    lines = []
    am._record_usage("eval", _usage(inp=1), effort="low", log_fn=lines.append)
    am._record_usage("eval", _usage(inp=1), log_fn=lines.append)
    assert "effort=low" in lines[0].split()
    assert "effort=medium" in lines[1].split()      # claude-opus-5-5's default, not "default"


def test_a_job_logs_and_snapshots_its_effort_settings(ledger, monkeypatch, tmp_path, capsys):
    """The exact level each route runs at, and what set it (an AIDP_EFFORT_*
    knob, the code, or the model's default), go in the runner log once and in
    each usage snapshot, read from the names the request sites use."""
    import job_migrate as jm
    import llm_config
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    async def task_spends(*a, **k):
        am._record_usage("migrate", _usage(inp=3000))
        return {"status": "PASS", "ok": 1, "failed": 0, "tokens": 3000}

    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    monkeypatch.setattr(jm, "process_notebook", task_spends)
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    monkeypatch.setattr(jm, "EFFORT_MIGRATE", None)
    monkeypatch.setattr(jm, "EFFORT_EVAL", "low")
    monkeypatch.setattr(am, "EFFORT_FIX_FIRST_ATTEMPT", "high")
    _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    [snap] = _snapshots(root, "run-1")
    assert snap["effort"] == {
        "migrate": {"level": "medium", "set_by": "model default"},
        "eval": {"level": "low", "set_by": "AIDP_EFFORT_EVAL"},
        "fix_first_attempt": {"level": "high", "set_by": "AIDP_EFFORT_FIX_FIRST_ATTEMPT"},
        "summary": {"level": "low", "set_by": "code"},
        "other": {"level": "medium", "set_by": "model default"}}
    [line] = [ln for ln in capsys.readouterr().out.splitlines() if "[effort]" in ln]
    for part in ("claude-opus-5-5", "migrate=medium (model default)",
                 "eval=low (AIDP_EFFORT_EVAL)", "fix_first_attempt=high (AIDP_EFFORT_FIX_FIRST_ATTEMPT)",
                 "summary=low (code)", "other=medium (model default)"):
        assert part in line


def test_an_unknown_models_default_effort_is_warned_at_job_start(ledger, monkeypatch, tmp_path,
                                                                 capsys):
    import job_migrate as jm
    import llm_config
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setattr(llm_config, "MODEL", "claude-unknown-9")
    monkeypatch.setattr(jm, "EFFORT_MIGRATE", None)
    _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    log = capsys.readouterr().out
    assert "migrate=UNRESOLVED (model default)" in log
    assert "WARNING" in log and "claude-unknown-9" in log and "MODEL_DEFAULT_EFFORT" in log


def _drive_with_notebook(monkeypatch, tmp_path, fake_process_notebook):
    """Real process_job + JOB_REPORT, with the task's notebook run replaced."""
    from test_pip_magic_gate import _drive_reporting_job
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    import job_migrate as jm
    monkeypatch.setattr(jm, "process_notebook", fake_process_notebook)
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    return reports["JOB_REPORT.md"]


def test_job_report_counts_a_task_that_raised(ledger, monkeypatch, tmp_path):
    """A task whose notebook run raises becomes an ERROR result with no usage;
    the requests it made before failing are still billed to the job."""
    async def fails_after_calls(*a, **k):
        am._record_usage("fix", _usage(inp=3000))
        raise RuntimeError("synthetic task failure")

    report = _drive_with_notebook(monkeypatch, tmp_path, fails_after_calls)
    assert "| fix | 1 | 3,000 |" in report
    assert "failed tasks): 3,000" in report


def test_job_report_counts_the_discarded_kernel_lost_attempt(ledger, monkeypatch, tmp_path):
    """On CONNECTION_DIED_KERNEL_LOST the whole task is re-run and the first
    attempt's result is discarded; its requests still count for the job."""
    attempts = []

    async def kernel_lost_then_pass(*a, **k):
        attempts.append(1)
        am._record_usage("migrate", _usage(inp=4000 if len(attempts) == 1 else 500))
        if len(attempts) == 1:
            return {"status": "CONNECTION_DIED_KERNEL_LOST"}
        return {"status": "PASS", "ok": 1, "failed": 0, "tokens": 500}

    report = _drive_with_notebook(monkeypatch, tmp_path, kernel_lost_then_pass)
    assert len(attempts) == 2
    assert "| migrate | 2 | 4,500 |" in report
    assert "failed tasks): 4,000" in report


def test_job_window_is_closed_when_the_job_raises(ledger, monkeypatch):
    """#185 review A4: a job that raises before JOB_REPORT must not leave its
    window open in the caller's context — main() catches the error and runs the
    next job, whose requests would otherwise also be billed to this one."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)

    async def two_failing_jobs():
        seen = []
        for _ in range(2):
            with pytest.raises(KeyError):
                await jm.process_job({"job_name": "j"}, object())   # no "tasks" -> raises
            seen.append(am._USAGE_WINDOWS.get())
        return seen

    assert asyncio.run(two_failing_jobs()) == [(), ()]


@pytest.mark.parametrize("failure", ["upload", "slack_redaction"])
def test_post_loop_fail_results_carry_billed_usage(ledger, monkeypatch, tmp_path, failure):
    """#185 review A3: the FAIL returns after the cell loop (upload failed,
    Slack redaction failed) still report the task's billed tokens and usage."""
    import job_migrate as jm
    from test_eval_verdict_gate import _ScriptedKernel, _notebook, _patch_common

    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"))

    async def fake_opus(system, user_content, *a, **k):
        am._record_usage("eval", _usage(inp=1234))
        return "PASS: output looks right", {"input": 1234, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", fake_opus)

    class _FailingUpload(_ScriptedKernel):
        async def execute(self, code, **kwargs):
            if failure == "upload" and "os.makedirs(os.path.dirname(" in code:
                raise RuntimeError("synthetic upload failure")
            return await super().execute(code, **kwargs)

    if failure == "slack_redaction":
        class _RefusingRedactor:
            """NULL_REDACTOR, except the final notebook cannot be proven clean."""
            def __getattr__(self, name):
                return getattr(jm.NULL_REDACTOR, name)

            def notebook(self, nb):
                raise jm.SlackRedactionError("synthetic redaction failure")

        monkeypatch.setattr(jm, "build_redactor", lambda *a, **k: _RefusingRedactor())

    result = asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb",
                                             _FailingUpload(), "test_usage", "01_task",
                                             {}, run_all=True))
    assert result["status"] == "FAIL", result
    assert result["tokens"] == 1234
    assert result["usage"]["by_route"]["eval"]["requests"] == 1


# ── token_usage/<run>/<process>.json: job-end usage across per-task migration processes ──
# (#160 condition 3 / #185: the daily token report reads billed figures from
# the job report instead of parsing "Tool loop done" lines.)

def test_combine_usage_sums_meters_routes_and_recorded_cost(ledger):
    marks = []
    for route, inp in (("eval", 100), ("eval_retry", 40)):
        mark = am.open_usage_window()
        am._record_usage(route, _usage(inp=inp, read=10, write=5, write_1h=5, out=2))
        marks.append(am.usage_delta(mark))
        am.close_usage_window(mark)
    total = am.combine_usage(marks)
    assert total["requests"] == 2
    assert total["billed_tokens"] == sum(m["billed_tokens"] for m in marks)
    assert total["est_cost_usd"] == pytest.approx(sum(m["est_cost_usd"] for m in marks))
    assert set(total["by_route"]) == {"eval", "eval_retry"}
    assert total["by_route"]["eval_retry"]["input"] == 40


class _LocalKernel:
    """Runs the generated stateless code for real (thread-safe print capture),
    optionally with /Workspace mapped under `root`."""
    def __init__(self, root=None):
        self.root = root

    def _run(self, code):
        if self.root is not None:
            # Forward slashes: a Windows path with backslashes inside a Python
            # string literal is a SyntaxError (#185 round 4).
            root = str(self.root).replace("\\", "/")
            code = code.replace("'/Workspace/", f"'{root}/Workspace/")
        lines = []
        exec(code, {"print": lambda *a, **k: lines.append(" ".join(map(str, a)))})
        return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                                             "text": "\n".join(lines) + "\n"}]}

    async def run_stateless(self, code, timeout=30):
        return await asyncio.to_thread(self._run, code)


def _ledger_kernel_class(root):
    """_ReportingKernel whose token-usage code runs for real, /Workspace mapped
    under `root` (shared by every kernel of a test, like the real workspace)."""
    import test_pip_magic_gate as tpmg

    class _LedgerKernel(tpmg._ReportingKernel):
        async def execute(self, code, **kwargs):
            if "__AIDP_TOKEN_LEDGER__" in code:
                return _LocalKernel(root)._run(code)
            return await super().execute(code, **kwargs)

    return _LedgerKernel


def _run_dir(root, run):
    return Path(root) / "Workspace" / "test_pip" / "test_pip" / "token_usage" / run


def _snapshots(root, run):
    import json as _json
    return [_json.loads(p.read_text()) for p in sorted(_run_dir(root, run).glob("*.json"))]


def _machine_block(report):
    import json as _json
    import job_migrate as jm
    tail = report.split(jm.TOKEN_USAGE_BLOCK_MARKER, 1)[1]
    return _json.loads(tail.split("```json\n", 1)[1].split("\n```", 1)[0])


def _spending_backfill(inp, failed=()):
    async def backfill(*a, **k):
        am._record_usage("migrate", _usage(inp=inp))
        return {"scanned": 1, "missing": len(failed), "migrated": 0, "failed": list(failed)}
    return backfill


def test_job_report_totals_every_process_of_the_run_including_eval_retry(ledger, monkeypatch,
                                                                         tmp_path):
    import json as _json
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    root = tmp_path / "fs"
    run_dir = _run_dir(root, "run-1")
    run_dir.mkdir(parents=True)
    (run_dir / "p1.json").write_text(_json.dumps(_snap(inp=1000, process="p1")))
    (run_dir / "p0.json").write_text("not json")
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")

    async def backfill(*a, **k):
        am._record_usage("eval_retry", _usage(inp=500))
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                                            backfill=backfill)
    report = reports["JOB_REPORT.md"]
    block = _machine_block(report)
    this, total = block["this_process"], block["job_total"]
    assert this["usage"]["by_route"]["eval_retry"]["input"] == 500
    assert (this["run_id"], this["final"]) == ("run-1", True)
    assert total["records_counted"] == 2 and total["unreadable_files"] == 1
    assert total["usage"]["billed_tokens"] == 1000 + this["usage"]["billed_tokens"]
    assert set(total["usage"]["by_route"]) >= {"migrate", "eval_retry"}
    # the unreadable file makes the run figure an approximation, with the reason shown
    assert not total["exact"]
    assert "approximation due to:" in report
    assert "1 unreadable snapshot file(s) were not counted" in report
    assert _json.loads((run_dir / f"{this['process']}.json").read_text()) == this


def test_job_report_says_so_when_the_snapshots_cannot_be_read(ledger, monkeypatch, tmp_path):
    """No read-back (e.g. the kernel failed): this process's figure, labelled as an
    approximation with the reason."""
    from test_pip_magic_gate import _drive_reporting_job
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    report = reports["JOB_REPORT.md"]
    assert "approximation due to:" in report
    assert "could not be read" in report
    block = _machine_block(report)
    assert block["ledger_error"]
    assert block["job_total"]["exact"] is False
    assert block["job_total"]["records_counted"] == 1


def test_clean_run_report_shows_an_unlabelled_cost(ledger, monkeypatch, tmp_path):
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")   # set from the YAML in a run
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(tmp_path / "fs"))
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    report = reports["JOB_REPORT.md"]
    assert "**Cost estimate:** $" in report and "approximation due to" not in report
    block = _machine_block(report)
    assert block["job_total"]["exact"] is True
    # N-1: the run-scoped job_total carries its own schema version
    assert "<!-- AIDP_TOKEN_USAGE v2 -->" in report
    assert block["schema"] == "aidp_token_usage_summary/2"


# ── Cost figure: exact, or "approximation due to: <reasons>" ──

def _snap(job="test_pip", inp=1000, model="claude-opus-5-5", run_id="run-1", process="p1",
          final=True, at="2026-10-02T20:00:00Z", **over):
    usage = {"requests": 1, "input": inp, "cache_read": 0, "cache_write": 0,
             "cache_write_1h": 0, "output": 0, "thinking": 0, "billed_tokens": inp,
             "est_cost_usd": inp * 4 / 1_000_000,
             "by_route": {"migrate": {"requests": 1, "input": inp, "cache_read": 0,
                                      "cache_write": 0, "cache_write_1h": 0, "output": 0,
                                      "thinking": 0, "billed_tokens": inp,
                                      "est_cost_usd": inp * 4 / 1_000_000}}}
    usage.update(over)
    return {"schema": "aidp_token_usage/2", "job": job, "run_id": run_id, "process": process,
            "started_at": at, "recorded_at": at, "final": final, "model": model,
            "tasks": {"t": {"status": "PASS", "tokens": inp}}, "usage": usage}


def _cost(record, records, unreadable=0, other_runs=0, error=None):
    import job_migrate as jm
    return jm._ledger_cost_summary("test_pip", record, records, unreadable, other_runs, error)


def test_clean_run_gives_an_exact_figure():
    sibling, mine = _snap(inp=1000, process="p1"), _snap(inp=500, process="p2")
    cost = _cost(mine, [sibling, mine], other_runs=3)
    assert cost["exact"] and cost["approximation_due_to"] == []
    assert (cost["records_counted"], cost["other_runs"]) == (2, 3)    # other runs: not summed
    assert cost["usage"]["billed_tokens"] == 1500


@pytest.mark.parametrize("case,expect,billed", [
    ("read_error", "could not be read", 500),                 # this process only
    ("unreadable", "unreadable snapshot file", 1500),
    ("other_job", "different job", 1500),                     # other job excluded
    ("own_snapshot_missing", "not found in the read-back", 1500),   # added from memory
    ("unfinished", "stopped without finishing", 1500),        # included up to its checkpoint
    ("no_run_id", "AIDP_USAGE_RUN_ID is not set", 1500),
    ("inconsistent", "do not add up", 1500),
    ("unknown_model", "not in the rate table", 1500),
])
def test_each_problem_makes_the_figure_a_labelled_approximation(case, expect, billed):
    sibling, mine = _snap(inp=1000, process="p1"), _snap(inp=500, process="p2")
    records, unreadable, error = [sibling, mine], 0, None
    if case == "read_error":
        records, error = None, "TimeoutError: kernel"
    elif case == "unreadable":
        unreadable = 2
    elif case == "other_job":
        records = [_snap(job="someone_else", inp=7000, process="p9"), sibling, mine]
    elif case == "own_snapshot_missing":
        records = [sibling]
    elif case == "unfinished":
        records = [_snap(inp=1000, process="p1", final=False), mine]
    elif case == "no_run_id":
        mine = _snap(inp=500, process="p2", run_id=None)
        records = [_snap(inp=1000, process="p1", run_id=None), mine]
    elif case == "inconsistent":
        records = [_snap(inp=1000, process="p1", billed_tokens=999), mine]
    elif case == "unknown_model":
        records = [_snap(inp=1000, process="p1", model="claude-unknown-9"), mine]
    cost = _cost(mine, records, unreadable, 0, error)
    assert not cost["exact"]
    assert len(cost["approximation_due_to"]) == 1, cost["approximation_due_to"]
    assert expect in cost["approximation_due_to"][0]
    assert cost["usage"]["billed_tokens"] == billed


def test_an_unfinished_process_gets_a_short_reason_naming_the_in_flight_loss():
    """A killed process's tokens after its last checkpoint, including a request
    in flight when it died, cannot be counted; the reason says so in one line."""
    killed = _snap(inp=1000, process="p1", final=False, at="2026-10-02T20:15:00Z")
    mine = _snap(inp=500, process="p2")
    [reason] = _cost(mine, [killed, mine])["approximation_due_to"]
    assert reason == ("process p1 stopped without finishing (killed or crashed) or is still "
                      "running; its tokens are counted only up to its last checkpoint at "
                      "2026-10-02T20:15:00Z")


def test_an_interrupted_process_is_not_reported_as_killed():
    """#191 deep review note: a deliberate cancel or Ctrl-C read as a killed
    process. The exit snapshot says the process was interrupted, and the reason
    claims no request in flight it cannot know of."""
    stopped = dict(_snap(inp=1000, process="p1", final=False, at="2026-10-02T20:15:00Z"),
                   interrupted=True)
    mine = _snap(inp=500, process="p2")
    [reason] = _cost(mine, [stopped, mine])["approximation_due_to"]
    assert reason == ("process p1 was stopped by a cancel or Ctrl-C at 2026-10-02T20:15:00Z; "
                      "its tokens up to that moment are counted, but a Claude request still "
                      "running then would be billed without being counted")


def test_a_lone_process_without_a_run_id_is_exact():
    mine = _snap(run_id=None, process="p2")
    assert _cost(mine, [mine])["exact"]


def test_a_per_task_child_reports_an_interim_figure():
    """N-4: a per-task child (--only-tasks) cannot count the processes of the run
    that start after it, so its figure is never exact. The reason must not promise a
    finalise report: the runbook's finalise runs --validate-only, which exits
    before process_job (job_migrate_from_workflow.py, #185 round 4)."""
    import job_migrate as jm
    sibling, mine = _snap(inp=1000, process="p1"), _snap(inp=500, process="p2")
    cost = jm._ledger_cost_summary("test_pip", mine, [sibling, mine], 0, 0, None,
                                   only_tasks=["task_b"])
    assert not cost["exact"] and cost["only_tasks"] == ["task_b"]
    [reason] = cost["approximation_due_to"]
    assert "--only-tasks task_b" in reason and "start after it are not counted" in reason
    assert "finalise" not in reason
    assert cost["usage"]["billed_tokens"] == 1500
    assert _cost(mine, [sibling, mine])["only_tasks"] == []


def test_snapshot_carries_run_id_process_and_final(monkeypatch):
    import job_migrate as jm
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", " run-7 ")
    ledger = jm._new_usage_ledger_state()
    monkeypatch.delenv("AIDP_USAGE_RUN_ID")         # read once, when the process starts
    snap = jm._token_usage_snapshot("j", ledger, {"requests": 0}, final=False)
    assert (snap["run_id"], snap["process"], snap["final"]) == ("run-7", ledger["process"], False)
    assert snap["interrupted"] is False     # a checkpoint, not an exit snapshot
    other = jm._new_usage_ledger_state()
    assert other["run_id"] is None and other["process"] != ledger["process"]


def test_every_run_gets_its_own_directory(monkeypatch):
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/out")
    base, run = jm._token_usage_dirs("job", "2026-10-02/../x y")
    assert base == "/Workspace/out/job/token_usage"
    assert run.startswith(base + "/2026-10-02____x_y-") and "/.." not in run
    assert jm._token_usage_dirs("job", None)[1] == base + "/_no_run_id"
    # the runbook's ids are used as they are
    assert jm._token_usage_dirs("job", "20261002T030000Z-c393ba3d")[1] == \
        base + "/20261002T030000Z-c393ba3d"
    # N-2: ids that clean to the same name still get different directories
    assert len({jm._token_usage_dirs("job", r)[1] for r in ("a.b", "a_b", "a b")}) == 3
    # a run id named like the unset-id directory does not share it
    assert jm._token_usage_dirs("job", "_no_run_id")[1].startswith(base + "/_no_run_id-")


# ── #185 review: run-scoped total, parallel writers, raised and killed processes ──

def test_second_run_into_the_same_folder_reports_only_its_own_cost(ledger, monkeypatch,
                                                                   tmp_path):
    """B-1 through process_job: nightly runs reuse the output folder; the second
    JOB_REPORT's exact figure is the second run's tokens only."""
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    blocks = []
    for run_id, inp in (("run-1", 7000), ("run-2", 500)):
        monkeypatch.setenv("AIDP_USAGE_RUN_ID", run_id)
        _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                                                backfill=_spending_backfill(inp))
        blocks.append(_machine_block(reports["JOB_REPORT.md"]))
    total = blocks[1]["job_total"]
    assert total["exact"] is True, total["approximation_due_to"]
    assert (total["run_id"], total["records_counted"], total["other_runs"]) == ("run-2", 1, 1)
    assert total["usage"]["billed_tokens"] == blocks[1]["this_process"]["usage"]["billed_tokens"]
    assert [len(_snapshots(root, r)) for r in ("run-1", "run-2")] == [1, 1]


def test_parallel_processes_of_a_run_each_write_their_own_file(monkeypatch, tmp_path):
    """Processes of one run (or of parallel runs) never write the same file, so
    concurrent writers cannot interleave or overwrite each other's usage."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    kernel = _LocalKernel()
    ledgers = [dict(jm._new_usage_ledger_state(), run_id="run-1") for _ in range(5)]
    snaps = [jm._token_usage_snapshot("job", lg, {"requests": i}, final=True)
             for i, lg in enumerate(ledgers)]

    async def write_all_then_read():
        errors = await asyncio.gather(*[jm._write_token_usage_snapshot(kernel, "job", s)
                                        for s in snaps])         # real threads, one file each
        return errors, await jm._read_token_usage_run(kernel, "job", "run-1")

    errors, (read, unreadable, other_runs, error) = asyncio.run(write_all_then_read())
    assert errors == [None] * 5 and (unreadable, other_runs, error) == (0, 0, None)
    assert sorted(r["process"] for r in read) == sorted(lg["process"] for lg in ledgers)


def test_a_job_that_raises_still_writes_its_final_snapshot(ledger, monkeypatch, tmp_path):
    """P-1: under --strict-cell-gating a failed backfill raises after the job's
    Claude calls and before JOB_REPORT; its tokens must still reach its snapshot."""
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    with pytest.raises(RuntimeError, match="Dependency backfill found unresolved notebook references"):
        _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"], strict=True,
                             backfill=_spending_backfill(4321, failed=["dep.ipynb"]))
    [snap] = _snapshots(root, "run-1")
    assert snap["final"] is True
    assert snap["usage"]["by_route"]["migrate"]["input"] == 4321


def test_a_killed_process_leaves_a_checkpoint_the_next_report_counts(ledger, monkeypatch,
                                                                    tmp_path, capsys):
    """P-1: a process killed after its task (no finally runs: SIGKILL, cluster
    restart) leaves the snapshot checkpointed before backfill. The next process of
    the run counts that usage and labels the total."""
    import job_migrate as jm
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    class Killed(BaseException):
        pass

    async def task_spends(*a, **k):
        am._record_usage("migrate", _usage(inp=3000))
        return {"status": "PASS", "ok": 1, "failed": 0, "tokens": 3000}

    async def killed_in_backfill(*a, **k):
        raise Killed()

    async def no_finally(*a, **k):
        return None

    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    monkeypatch.setattr(jm, "process_notebook", task_spends)
    real_finally = jm._record_usage_of_unreported_job
    monkeypatch.setattr(jm, "_record_usage_of_unreported_job", no_finally)
    with pytest.raises(Killed):
        _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                             backfill=killed_in_backfill)
    [killed] = _snapshots(root, "run-1")
    assert killed["final"] is False and killed["usage"]["billed_tokens"] == 3000

    monkeypatch.setattr(jm, "_record_usage_of_unreported_job", real_finally)
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    total = _machine_block(reports["JOB_REPORT.md"])["job_total"]
    assert not total["exact"]
    assert any("stopped without finishing" in r for r in total["approximation_due_to"])
    assert total["usage"]["billed_tokens"] == 3000 + 3000
    # the approximation and its reason are also in the process log
    log = capsys.readouterr().out
    assert "this run's token cost is an approximation" in log
    assert "counted only up to its last checkpoint" in log


def test_ledger_paths_with_a_quote_in_the_job_name(monkeypatch, tmp_path):
    """P-2: the paths are Python literals in the code run on the cluster; a quote
    in the job name must not break them."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    kernel = _LocalKernel()
    snap = jm._token_usage_snapshot("O'Neil Daily", dict(jm._new_usage_ledger_state(),
                                                         run_id="run-1"),
                                    {"requests": 0}, final=True)

    async def write_then_read():
        return (await jm._write_token_usage_snapshot(kernel, "O'Neil Daily", snap),
                await jm._read_token_usage_run(kernel, "O'Neil Daily", "run-1"))

    error, read = asyncio.run(write_then_read())
    assert error is None and read == ([snap], 0, 0, None)


def test_a_kernel_error_in_the_ledger_code_is_named_in_the_reason(monkeypatch, tmp_path):
    """#185 round 3: code that raises on the cluster returns status=error with
    no stream text; the reason (log, ledger_error, approximation line) must
    carry the kernel's ename/evalue, not just "no content"."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))

    class _RaisingKernel:
        async def run_stateless(self, code, timeout=30):
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "PermissionError",
                "evalue": "[Errno 13] Permission denied: 'token_usage'",
                "traceback": ["PermissionError: [Errno 13] Permission denied"]}]}

    snap = jm._token_usage_snapshot("job", dict(jm._new_usage_ledger_state(), run_id="run-1"),
                                    {"requests": 0}, final=True)
    write_error = asyncio.run(jm._write_token_usage_snapshot(_RaisingKernel(), "job", snap))
    read_error = asyncio.run(jm._read_token_usage_run(_RaisingKernel(), "job", "run-1"))[3]
    for error in (write_error, read_error):
        assert "PermissionError: [Errno 13] Permission denied: 'token_usage'" in error


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt, asyncio.CancelledError])
def test_restore_and_lease_release_run_before_the_usage_write(monkeypatch, interrupt):
    """P-1 follow-up: process_job's finally writes the failed job's usage after the
    idle-timeout restore and the lease release, so a dead kernel cannot delay them
    and a cancel or second Ctrl-C during the write cannot skip them."""
    import job_migrate as jm
    order = []

    class Coord:
        def acquire(self, label):
            return "lease-1"

        def release(self, lease_id):
            order.append("release")

    async def inner(job, session, ledger=None, *, skip_task_dependency_scan=False):
        raise RuntimeError("synthetic job failure")

    async def interrupted_usage_write(*a, **k):
        order.append("usage")
        raise interrupt()

    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: Coord())
    monkeypatch.setattr(jm, "_process_job_inner", inner)
    monkeypatch.setattr(jm, "_revert_all_idle_timeouts", lambda: order.append("restore") or [])
    monkeypatch.setattr(jm, "_record_usage_of_unreported_job", interrupted_usage_write)
    with pytest.raises(interrupt):
        asyncio.run(jm.process_job({"job_name": "j"}, object()))
    assert order == ["restore", "release", "usage"]


def test_a_process_killed_mid_task_keeps_its_timed_checkpoint(ledger, monkeypatch, tmp_path):
    """N-3: the timer rewrites the snapshot while a task runs, so a process killed
    in the middle of its task (no finally runs) still leaves the task's tokens."""
    import job_migrate as jm
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    class Killed(BaseException):
        pass

    async def spends_then_killed(*a, **k):
        import time
        am._record_usage("fix", _usage(inp=2500))
        # a long cell; the timer fires meanwhile (asyncio.sleep is patched out)
        await asyncio.to_thread(time.sleep, 0.3)
        raise Killed()

    async def no_finally(*a, **k):
        return None

    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    monkeypatch.setattr(jm, "TOKEN_USAGE_CHECKPOINT_SECS", 0.05)
    monkeypatch.setattr(jm, "process_notebook", spends_then_killed)
    monkeypatch.setattr(jm, "_record_usage_of_unreported_job", no_finally)
    with pytest.raises(Killed):
        _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    [snap] = _snapshots(root, "run-1")
    assert snap["final"] is False and snap["usage"]["billed_tokens"] == 2500


def test_a_checkpoint_in_flight_never_lands_after_the_final_snapshot(ledger, monkeypatch,
                                                                     tmp_path):
    """N-3: a slow checkpoint write and the final write never overlap; the file
    left on disk is the final snapshot with all of the process's tokens."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))

    class SlowFirstWrite(_LocalKernel):
        cluster_id = "c1"
        calls = 0

        async def run_stateless(self, code, timeout=30):
            self.calls += 1
            if self.calls == 1:
                await asyncio.sleep(0.2)    # the checkpoint's write
            return await super().run_stateless(code, timeout)

    kernel = SlowFirstWrite()
    lg = dict(jm._new_usage_ledger_state(), run_id="run-1")

    async def race():
        lg["mark"] = am.open_usage_window()
        am._record_usage("migrate", _usage(inp=100))
        checkpoint = asyncio.create_task(jm._checkpoint_token_usage(kernel, "job", lg))
        await asyncio.sleep(0.05)           # the checkpoint's write is in flight
        am._record_usage("migrate", _usage(inp=900))
        await jm._record_usage_of_unreported_job(kernel, "job", lg)
        await checkpoint
        am.close_usage_window(lg["mark"])
        return await jm._read_token_usage_run(kernel, "job", "run-1")

    snaps, unreadable, _, error = asyncio.run(race())
    assert (unreadable, error) == (0, None)
    [snap] = snaps
    assert snap["final"] is True and snap["usage"]["billed_tokens"] == 1000


# ── #185 round 4: all-runs total next to the run total; confirmed JOB_REPORT write ──

def _others(runs=1, records=1, inp=700, unreadable=0, not_final=0, interrupted=0, other_job=0,
            inconsistent=0, models=None):
    usage = {"requests": records, "input": inp, "cache_read": 0, "cache_write": 0,
             "cache_write_1h": 0, "output": 0, "thinking": 0, "billed_tokens": inp,
             "est_cost_usd": inp * 4 / 1_000_000}
    return {"run_folders": runs, "records": records, "unreadable": unreadable,
            "not_final": not_final, "interrupted": interrupted, "other_job": other_job,
            "inconsistent": inconsistent,
            "models": models if models is not None else {"claude-opus-5-5": records},
            "usage": usage}


def test_all_runs_total_adds_the_other_runs_to_this_run():
    """Owner condition / maintainer 3: the block reports this run's total and,
    separately, the total of every run in the output folder."""
    import job_migrate as jm
    mine = _snap(inp=500, process="p2")
    cost = _cost(mine, [mine], other_runs=2)
    total = jm._all_runs_summary(cost, _others(runs=2, records=3, inp=700))
    assert total["exact"] and total["approximation_due_to"] == []
    assert (total["run_folders"], total["records_counted"]) == (3, 4)
    assert total["usage"]["billed_tokens"] == 1200
    assert total["usage"]["requests"] == 4
    assert "by_route" not in total["usage"]       # other runs are summed without routes
    assert cost["usage"]["billed_tokens"] == 500   # the run total is unchanged


@pytest.mark.parametrize("over,expect", [
    ({"unreadable": 2}, "2 unreadable snapshot file(s) of other runs"),
    ({"not_final": 1}, "1 process(es) of other runs stopped without finishing"),
    ({"other_job": 1}, "1 snapshot(s) for a different job"),
    ({"inconsistent": 1}, "do not add up"),
    ({"models": {"claude-unknown-9": 1}}, "not in the rate table"),
])
def test_each_problem_in_another_run_makes_the_all_runs_total_an_approximation(over, expect):
    import job_migrate as jm
    mine = _snap(inp=500, process="p2")
    total = jm._all_runs_summary(_cost(mine, [mine]), _others(**over))
    assert not total["exact"]
    [reason] = total["approximation_due_to"]
    assert expect in reason


def test_an_interrupted_other_run_is_not_reported_as_killed():
    """#191 deep review note, for the all-runs total: an interrupted snapshot
    (counted in not_final) gets its own reason, not the killed-process one."""
    import job_migrate as jm
    mine = _snap(inp=500, process="p2")
    total = jm._all_runs_summary(_cost(mine, [mine]), _others(not_final=1, interrupted=1))
    [reason] = total["approximation_due_to"]
    assert reason == ("1 process(es) of other runs were stopped by a cancel or Ctrl-C; a "
                      "Claude request still running at that moment would be billed without "
                      "being counted")
    assert "killed" not in reason


def test_the_cluster_sum_counts_interrupted_runs_apart(monkeypatch, tmp_path):
    import json as _json
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    run = tmp_path / "job" / "token_usage" / "run-1"
    run.mkdir(parents=True)
    for name, snap in (("a", _snap(job="job", run_id="run-1")),
                       ("b", _snap(job="job", run_id="run-1", final=False)),
                       ("c", dict(_snap(job="job", run_id="run-1", final=False),
                                  interrupted=True))):
        (run / f"{name}.json").write_text(_json.dumps(snap))
    others, error = asyncio.run(jm._read_token_usage_other_runs(_LocalKernel(), "job", "run-2"))
    assert error is None
    assert (others["records"], others["not_final"], others["interrupted"]) == (3, 2, 1)


def test_an_approximate_run_total_makes_the_all_runs_total_one_too():
    import job_migrate as jm
    mine = _snap(inp=500, process="p2")
    cost = _cost(mine, [mine], unreadable=1)
    total = jm._all_runs_summary(cost, _others())
    assert not total["exact"]
    assert total["approximation_due_to"] == [
        "this run's figure is an approximation (see job_total)"]


def test_no_all_runs_total_when_the_folder_could_not_be_read():
    import job_migrate as jm
    mine = _snap(inp=500, process="p2")
    assert jm._all_runs_summary(_cost(mine, None, error="TimeoutError: kernel"), None) is None


def test_the_other_runs_are_summed_on_the_cluster(monkeypatch, tmp_path):
    """The real read code: other runs come back as one aggregate (constant size
    however many runs the folder holds), with every problem counted; a file
    that cannot be summed is unreadable, not a failed read."""
    import json as _json
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    base = tmp_path / "job" / "token_usage"
    corrupt = _snap(job="job", inp=5, run_id="run-2")
    corrupt["usage"] = "corrupt"
    for run, name, snap in (("run-1", "a", _snap(job="job", inp=1000, run_id="run-1")),
                            ("run-1", "b", _snap(job="job", inp=300, run_id="run-1",
                                                 final=False)),
                            ("run-2", "c", _snap(job="job", inp=50, run_id="run-2",
                                                 model="claude-unknown-9")),
                            ("run-2", "d", _snap(job="other", inp=9999, run_id="run-2")),
                            ("run-2", "g", corrupt),
                            ("run-2", "h", _snap(job="job", inp=10, run_id="run-2",
                                                 billed_tokens=999)),
                            ("run-3", "e", _snap(job="job", inp=7, run_id="run-3"))):
        (base / run).mkdir(parents=True, exist_ok=True)
        (base / run / f"{name}.json").write_text(_json.dumps(snap))
    (base / "run-2" / "f.json").write_text("not json")
    others, error = asyncio.run(jm._read_token_usage_other_runs(_LocalKernel(), "job", "run-3"))
    assert error is None
    assert {k: others[k] for k in ("run_folders", "records", "unreadable", "not_final",
                                   "other_job", "inconsistent")} == {
        "run_folders": 2, "records": 4, "unreadable": 2, "not_final": 1, "other_job": 1,
        "inconsistent": 1}
    assert others["models"] == {"claude-opus-5-5": 3, "claude-unknown-9": 1}
    assert others["usage"]["input"] == 1360 and "by_route" not in others["usage"]
    # this run's own read is a separate call and is unaffected
    snaps, unreadable, other_runs, read_error = asyncio.run(
        jm._read_token_usage_run(_LocalKernel(), "job", "run-3"))
    assert (len(snaps), unreadable, other_runs, read_error) == (1, 0, 2, None)


def test_a_failed_other_runs_read_leaves_this_runs_figure_intact(ledger, monkeypatch, tmp_path):
    """Round-5 review: one kernel call read both, so a problem in another run
    lost this run's figure too. Now the all-runs total says why it is missing."""
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    base_kernel = _ledger_kernel_class(tmp_path / "fs")

    class OtherRunsFail(base_kernel):
        async def execute(self, code, **kwargs):
            if '"run_folders"' in code:
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "TimeoutError", "evalue": "60 s", "traceback": []}]}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(tpmg, "_ReportingKernel", OtherRunsFail)
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    report = reports["JOB_REPORT.md"]
    block = _machine_block(report)
    assert block["job_total"]["exact"] is True and block["ledger_error"] is None
    assert block["all_runs"] is None and "TimeoutError: 60 s" in block["all_runs_error"]
    assert "All runs in this output folder: total not available" in report


def test_an_approximate_all_runs_total_is_labelled_in_the_report(ledger, monkeypatch, tmp_path):
    """The human line carries the label and the reasons, and the block the
    count of unreadable files."""
    import json as _json
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    root = tmp_path / "fs"
    old = _run_dir(root, "run-0")
    old.mkdir(parents=True)
    (old / "p0.json").write_text("not json")
    (old / "p1.json").write_text(_json.dumps(_snap(inp=100, run_id="run-0", final=False)))
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    report = reports["JOB_REPORT.md"]
    all_runs = _machine_block(report)["all_runs"]
    assert not all_runs["exact"] and all_runs["unreadable_files"] == 1
    line = report.split("All runs in this output folder", 1)[1].split("\n\n", 1)[0]
    assert "approximation due to:" in line and " — exact." not in line
    assert "1 unreadable snapshot file(s) of other runs" in line
    assert "1 process(es) of other runs stopped without finishing" in line


def test_second_run_reports_the_all_runs_total_beside_its_own(ledger, monkeypatch, tmp_path):
    """Through process_job: run 2's block keeps its own exact total and adds the
    all-runs total of both runs."""
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    import llm_config
    monkeypatch.setattr(llm_config, "MODEL", "claude-opus-5-5")
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(tmp_path / "fs"))
    blocks, reports = [], []
    for run_id, inp in (("run-1", 7000), ("run-2", 500)):
        monkeypatch.setenv("AIDP_USAGE_RUN_ID", run_id)
        _, rep, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                                            backfill=_spending_backfill(inp))
        reports.append(rep["JOB_REPORT.md"])
        blocks.append(_machine_block(rep["JOB_REPORT.md"]))
    run_total, all_runs = blocks[1]["job_total"], blocks[1]["all_runs"]
    assert run_total["exact"] and all_runs["exact"], all_runs["approximation_due_to"]
    assert (all_runs["run_folders"], all_runs["records_counted"]) == (2, 2)
    assert all_runs["usage"]["billed_tokens"] == sum(
        b["this_process"]["usage"]["billed_tokens"] for b in blocks)
    assert "All runs in this output folder" in reports[1]


def test_a_job_report_write_the_kernel_rejects_is_reported(ledger, monkeypatch, tmp_path, capsys):
    """#185 round 4 (A4): a write that returns a kernel error instead of raising
    was silent, and the previous run's JOB_REPORT.md read as current."""
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    class RejectsReport(tpmg._ReportingKernel):
        async def execute(self, code, **kwargs):
            if "JOB_REPORT.md" in code and "os.replace(_tmp, _path)" in code:
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "OSError", "evalue": "[Errno 122] Disk quota exceeded",
                    "traceback": []}]}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(tpmg, "_ReportingKernel", RejectsReport)
    result, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    assert "JOB_REPORT.md" not in reports
    assert "OSError: [Errno 122] Disk quota exceeded" in result["job_report_error"]
    out = capsys.readouterr().out
    assert "JOB_REPORT.md NOT written" in out and "is from an earlier run" in out


def test_a_written_job_report_is_confirmed(ledger, monkeypatch, tmp_path):
    from test_pip_magic_gate import _drive_reporting_job
    result, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    assert "JOB_REPORT.md" in reports and result["job_report_error"] is None


def test_the_report_write_replaces_the_file_whole_and_leaves_no_temporary(monkeypatch, tmp_path):
    """The real write code: a temporary sibling, then one os.replace, so a failed
    write never leaves a truncated report; the temporary never survives."""
    import job_migrate as jm
    path = str(tmp_path / "out" / "JOB_REPORT.md")
    kernel = _LocalKernel()
    assert asyncio.run(jm._write_report_file(kernel, path, "first")) is None
    assert asyncio.run(jm._write_report_file(kernel, path, "second")) is None
    assert Path(path).read_text() == "second"
    blocked = tmp_path / "out" / "BLOCKED.md"
    blocked.mkdir()                      # os.replace onto a directory fails
    error = asyncio.run(jm._write_report_file(kernel, str(blocked), "x"))
    assert error          # the exception, named (the platform decides which OSError)
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == ["BLOCKED.md", "JOB_REPORT.md"]


def test_a_windows_report_path_reaches_the_kernel_instead_of_raising():
    """Round-5 review: the temporary path was split on '/' before the write, so
    a Windows path (the owner's suite box) raised instead of being written."""
    import job_migrate as jm
    sent = []

    class Recording:
        async def run_stateless(self, code, timeout=30):
            sent.append(code)
            return {"status": "ok", "outputs": [{"type": "stream", "text": "__AIDP_REPORT_WRITTEN__\n"}]}

    path = r"C:\Users\me\out\JOB_REPORT.md"
    assert asyncio.run(jm._write_report_file(Recording(), path, "x")) is None
    assert f"_path = {path!r}" in sent[0]


@pytest.mark.parametrize("rejected", [False, True])
def test_required_packages_file_is_written_and_confirmed(ledger, monkeypatch, tmp_path, capsys,
                                                         rejected):
    """The packages file goes through the same confirmed write as JOB_REPORT."""
    import job_migrate as jm
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    class Kernel(tpmg._ReportingKernel):
        async def execute(self, code, **kwargs):
            if rejected and "REQUIRED_PACKAGES.txt" in code and "os.replace" in code:
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "PermissionError", "evalue": "denied",
                    "traceback": []}]}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(tpmg, "_ReportingKernel", Kernel)
    monkeypatch.setattr(jm, "get_installed_packages", lambda: ["sentinel_pkg==1.0"])
    _, _, _, kernel = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    written = {Path(p).name: c for p, c in kernel.files.items()}
    out = capsys.readouterr().out
    if rejected:
        assert "REQUIRED_PACKAGES.txt" not in written
        assert "REQUIRED_PACKAGES.txt NOT written (write not confirmed: PermissionError: denied)" in out
    else:
        assert "sentinel_pkg==1.0" in written["REQUIRED_PACKAGES.txt"]
        assert "Wrote 1 packages" in out


# ── #185 consolidated review round 2: atomic snapshots, final only once written, run id ──

def test_a_failed_snapshot_write_keeps_the_previous_checkpoint(monkeypatch, tmp_path):
    """Finding 2: the snapshot was rewritten in place ('wb'), so a write cut off
    mid-way destroyed the checkpoint it was replacing. It now goes to a
    temporary sibling and replaces the file in one step."""
    import json as _json
    import os
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    kernel = _LocalKernel()
    lg = dict(jm._new_usage_ledger_state(), run_id="run-1")
    first = jm._token_usage_snapshot("job", lg, {"requests": 1}, final=False)
    assert asyncio.run(jm._write_token_usage_snapshot(kernel, "job", first)) is None

    def interrupted(src, dst):
        raise OSError("interrupted")

    monkeypatch.setattr(os, "replace", interrupted)
    second = jm._token_usage_snapshot("job", lg, {"requests": 2}, final=True)
    error = asyncio.run(jm._write_token_usage_snapshot(kernel, "job", second))
    assert error and "interrupted" in error
    run_dir = tmp_path / "job" / "token_usage" / "run-1"
    assert [p.name for p in run_dir.iterdir()] == [f"{lg['process']}.json"]   # no temporary left
    kept = _json.loads((run_dir / f"{lg['process']}.json").read_text())
    assert (kept["usage"]["requests"], kept["final"]) == (1, False)


def test_a_failed_final_snapshot_write_is_retried_at_job_exit(ledger, monkeypatch, tmp_path):
    """Finding 3: the process was marked final before its final snapshot was
    written, so a failed write was never retried. It is marked final only once
    the write is confirmed; process_job's finally then writes it again."""
    import base64 as _b64
    import json as _json
    import re as _re
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job
    root = tmp_path / "fs"
    base_kernel = _ledger_kernel_class(root)
    failed = []

    class FailFirstFinalWrite(base_kernel):
        async def execute(self, code, **kwargs):
            if "__AIDP_TOKEN_LEDGER__written" in code and not failed:
                m = _re.search(r"base64.b64decode\('([^']+)'\)", code)
                if m and _json.loads(_b64.b64decode(m.group(1)))["final"]:
                    failed.append(True)
                    return {"status": "error", "outputs": [{
                        "type": "error", "ename": "OSError", "evalue": "transient", "traceback": []}]}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(tpmg, "_ReportingKernel", FailFirstFinalWrite)
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"],
                         backfill=_spending_backfill(500))
    [snap] = _snapshots(root, "run-1")
    assert failed and snap["final"] is True
    assert snap["usage"]["billed_tokens"] >= 500


def test_an_entrypoint_gives_its_invocation_a_run_id(monkeypatch):
    """Finding 4: only the runbook set AIDP_USAGE_RUN_ID, so any other launch
    put every run under _no_run_id. The entrypoints now keep the launcher's id
    and otherwise make one, exported so child processes inherit it."""
    import os
    import re as _re
    import job_migrate as jm
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "placeholder")
    monkeypatch.delenv("AIDP_USAGE_RUN_ID")
    run_id = jm.ensure_token_usage_run_id()
    assert _re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", run_id)
    assert os.environ["AIDP_USAGE_RUN_ID"] == run_id
    assert jm._new_usage_ledger_state()["run_id"] == run_id
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "launcher-run-7")
    assert jm.ensure_token_usage_run_id() == "launcher-run-7"


def test_both_entrypoints_set_the_run_id_before_the_job_runs():
    import inspect
    import job_migrate as jm
    import job_migrate_from_workflow as jmfw
    # The workflow runner reaches process_job through _run_workflow_job (#182).
    assert "migrator.process_job(" in inspect.getsource(jmfw._run_workflow_job)
    for source, call in ((inspect.getsource(jmfw.main), "_run_workflow_job("),
                         (inspect.getsource(jm.main), "await process_job(job, session)")):
        assert "ensure_token_usage_run_id()" in source
        assert source.index("ensure_token_usage_run_id()") < source.index(call)


def test_the_exit_tail_marks_final_only_after_its_write(ledger, monkeypatch, tmp_path):
    """#185 round 4: the job-exit half of "final only after a confirmed write"
    was not pinned. A failed write leaves the process not final, so a later
    write can still make it final."""
    import job_migrate as jm
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path))
    lg = dict(jm._new_usage_ledger_state(), run_id="run-1",
              started_at="2026-10-05T00:00:00Z", mark=am.open_usage_window())
    am._record_usage("fix", _usage(inp=100))

    class Rejects:
        async def run_stateless(self, code, timeout=30):
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "OSError", "evalue": "transient", "traceback": []}]}

    asyncio.run(jm._record_usage_of_unreported_job(Rejects(), "job", lg))
    assert lg["final"] is False
    asyncio.run(jm._record_usage_of_unreported_job(_LocalKernel(), "job", lg))
    assert lg["final"] is True
    [snap] = [__import__("json").loads(p.read_text())
              for p in (tmp_path / "job" / "token_usage" / "run-1").glob("*.json")]
    assert snap["final"] is True and snap["usage"]["billed_tokens"] == 100


@pytest.mark.parametrize("interrupt", [asyncio.CancelledError, KeyboardInterrupt])
def test_an_interrupted_job_leaves_its_snapshot_unfinished(ledger, monkeypatch, tmp_path,
                                                          capsys, interrupt):
    """#191 round 5: a cancel or Ctrl-C reaches process_job's finally while a
    Claude request still runs in its executor thread. That request is billed and
    never recorded, so the exit snapshot must not be final: the next process of
    the run labels the total an approximation instead of calling it exact."""
    import time
    import job_migrate as jm
    import test_pip_magic_gate as tpmg
    from test_pip_magic_gate import _drive_reporting_job

    async def interrupted_mid_request(*a, **k):
        am._record_usage("migrate", _usage(inp=1000))
        loop = asyncio.get_running_loop()
        request = loop.run_in_executor(None, time.sleep, 0.2)   # billed, never recorded
        if interrupt is asyncio.CancelledError:
            loop.call_soon(asyncio.current_task().cancel)
            await request       # the cancel lands here, mid-request
        raise interrupt()

    async def task_spends(*a, **k):
        am._record_usage("migrate", _usage(inp=3000))
        return {"status": "PASS", "ok": 1, "failed": 0, "tokens": 3000}

    root = tmp_path / "fs"
    monkeypatch.setattr(tpmg, "_ReportingKernel", _ledger_kernel_class(root))
    monkeypatch.setenv("AIDP_USAGE_RUN_ID", "run-1")
    monkeypatch.setattr(jm, "process_notebook", interrupted_mid_request)
    with pytest.raises(interrupt):
        _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    [snap] = _snapshots(root, "run-1")
    assert snap["final"] is False and snap["usage"]["billed_tokens"] == 1000
    assert snap["interrupted"] is True
    log = capsys.readouterr().out
    assert "billed tokens are in it" not in log
    assert "job stopped by a cancel or Ctrl-C" in log and "not final" in log

    monkeypatch.setattr(jm, "process_notebook", task_spends)
    _, reports, _, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X_SENTINEL = 1"])
    total = _machine_block(reports["JOB_REPORT.md"])["job_total"]
    assert not total["exact"]
    assert any("was stopped by a cancel or Ctrl-C" in r for r in total["approximation_due_to"])
    assert not any("killed" in r for r in total["approximation_due_to"])
