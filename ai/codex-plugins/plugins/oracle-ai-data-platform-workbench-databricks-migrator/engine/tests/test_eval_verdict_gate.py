"""Cell-loop coverage for the Opus eval verdict gate (token-savings follow-ups).

Drives the REAL process_notebook cell loop (harness from
test_conditional_exit_below_execute) with a scripted eval and a recording
call_fix, so the assertions are on what the fix prompt actually receives.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate as jm  # noqa: E402
from test_conditional_exit_below_execute import _Kernel, _patch_common  # noqa: E402

STALE_HINT = "STALE_HINT_FROM_CELL_A"


class _ScriptedKernel(_Kernel):
    async def execute(self, code, **kwargs):
        if "SENTINEL_A" in code:
            self.executed.append(code)
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": "cell A produced plenty of output\n"}]}
        if "SENTINEL_B" in code:
            self.executed.append(code)
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "ValueError", "evalue": "synthetic failure",
                "traceback": ["Traceback (most recent call last):", "ValueError: synthetic failure"]}]}
        if "SENTINEL_C" in code:
            # status ok, but the output carries a traceback (the
            # "Error pattern in output" deterministic failure)
            self.executed.append(code)
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout",
                 "text": "Traceback (most recent call last):\nValueError: printed failure\n"}]}
        return await super().execute(code, **kwargs)


def _notebook(*sources):
    return {"cells": [{"cell_type": "code", "source": [s], "metadata": {}, "outputs": [],
                       "execution_count": None} for s in sources],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5}


def _run(monkeypatch, tmp_path, notebook, eval_replies):
    """Run the cell loop; return (result, fix_calls, eval_calls). Each fix call
    is (cell_code, extra_context)."""
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    replies = iter(eval_replies)
    eval_calls, fix_calls = [], []

    async def fake_opus(system, user_content, *a, **k):
        assert system.startswith("You are verifying a migrated Spark cell"), system[:80]
        eval_calls.append(user_content)
        reply = next(replies)
        if isinstance(reply, BaseException):    # the eval request itself fails
            raise reply
        return reply, {"input": 0, "output": 0}

    async def fake_fix(code, error, context, attempt, extra_context="", **k):
        fix_calls.append((code, extra_context))
        return code

    monkeypatch.setattr(jm, "call_opus_with_tools", fake_opus)
    monkeypatch.setattr(jm, "call_fix", fake_fix)
    result = asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb",
                                             _ScriptedKernel(), "test_eval_gate", "01_task",
                                             {}, run_all=True))
    return result, fix_calls, eval_calls


def test_eval_fix_hint_does_not_leak_into_a_later_cells_fix_prompt(monkeypatch, tmp_path):
    """Cell A's eval says ISSUE with a FIX hint, then passes after one fix.
    Cell B then fails deterministically, so its eval is skipped. B's fix prompt
    must not carry A's evaluator hint."""
    notebook = _notebook("SENTINEL_A = 1\n", "SENTINEL_B = 1\n")
    _, fix_calls, _ = _run(monkeypatch, tmp_path, notebook, [
        f"ISSUE: output looks wrong | FIX: {STALE_HINT}",
        "PASS: output looks right",
    ])
    a_fixes = [ctx for code, ctx in fix_calls if "SENTINEL_A" in code]
    b_fixes = [ctx for code, ctx in fix_calls if "SENTINEL_B" in code]
    assert a_fixes and STALE_HINT in a_fixes[0], "precondition: A's own fix gets its hint"
    assert b_fixes, "precondition: B went through the fix loop"
    for ctx in b_fixes:
        assert STALE_HINT not in ctx


@pytest.mark.parametrize("reply,verdict", [
    ("PASS: looks right", "PASS"),
    ("ISSUE: wrong rows | FIX: filter", "ISSUE"),
    ("**PASS**: looks right", "PASS"),                    # markdown emphasis
    ("`PASS: looks right`", "PASS"),                      # inline code, not a fence
    # identifiers that end in "issue(s)" are not a second verdict (#185 round 4):
    ("PASS: the cell printed {'open_issues': 12}, same as the original", "PASS"),
    ("PASS: DQ_ISSUES: 0 rows flagged", "PASS"),
    ("PASS: tissue: sample counts match", "PASS"),
    ("PASS: hotfix: none needed", "PASS"),
    # fences follow the opening marker's length; a same-line ```…``` is inline code:
    ("````\n```\nISSUE: quoted\n```\n````\nPASS: ok", None),
    ("```0 rows```\nISSUE: rows missing | FIX: check the filter", "ISSUE"),
    ("```\n```python\nISSUE: still inside\n```", None),
    ("`ISSUE`: wrong", "ISSUE"),
    ("  PASS: looks right  \n\n", "PASS"),                # blank lines and padding
    ("", None),                                           # empty / cut off before text
    ("The output looks fine to me.", None),               # no verdict token
    ("PASSED all checks", None),                          # not the contract token
    ("def fixed():\n    return 1", None),                  # code, not a verdict
    ("**PASS:** looks right", "PASS"),                    # colon inside the emphasis
    ("> ISSUE: wrong rows", "ISSUE"),                      # quoted
    # Only the contract token followed by a colon is a verdict (#185 review):
    ("    pass", None),                                   # Python `pass` statement
    ("Pass criteria met, checking rows", None),           # prose starting with "Pass"
    ("pass: looks right", None),                          # lower-case is not the contract
    ("PASS — looks right", None),                         # no colon
    ("PASS/ISSUE decision below.\nISSUE: wrong rows", "ISSUE"),
    ("```python\ntry:\n    x()\nexcept ValueError:\n    pass\n```\n"
     "ISSUE: wrong rows | FIX: filter", "ISSUE"),         # code before the verdict
    # '#' reads the same as a Python comment, and fenced lines are quoted
    # output or code — neither is a verdict (#185 consolidated review item 2):
    ("# PASS: patched the read", None),
    ("## PASS: looks right", None),
    ("```python\n# PASS: patched the read\ndf = spark.table('t')\n```", None),
    ("The cell printed:\n```\nPASS: row counts match\n```", None),   # quoted cell output
    ("```\nPASS: never closed", None),                                # unclosed fence
    ("```\n~~~\nPASS: still inside\n```", None),          # only the opening marker closes
    ("```\n~~~\nISSUE: still inside\n```", None),         # ... for an ISSUE line too
    # An ISSUE line outweighs PASS lines, in either order:
    ("PASS: schema matches\nISSUE: row counts differ | FIX: filter", "ISSUE"),
    ("ISSUE: row counts differ | FIX: filter\nPASS: schema matches", "ISSUE"),
])
def test_eval_verdict_parser(reply, verdict):
    assert jm._parse_eval_verdict(reply)[0] == verdict


# #185 round 4, approach verdict on the parser: a PASS is accepted only when
# the whole reply is that one PASS line. Any other line (preamble, quoted
# output, a second verdict, or an ISSUE in a form the contract does not use)
# makes the reply ambiguous: no verdict, so one retry and then fail closed.
# This closes the bypass grid below without recognising any of its forms.
@pytest.mark.parametrize("reply", [
    "Checked the output against the original.\nPASS: same rows",       # preamble
    "PASS: same rows\nThe row count matches the original.",            # trailing prose
    "PASS: schema matches\nPASS: row counts match",                    # two verdicts
    "~~~\nISSUE: quoted\n~~~\nPASS: same rows",                         # fenced lines
    "PASS: ok\n```\ndf.show()\n```",
    "The cell printed:\nPASS: 3 rows\nThe join dropped rows.",         # unfenced quote
    # ISSUE forms the contract regex does not read, after a PASS line:
    "PASS: schema ok\nIssue: rows differ",
    "PASS: ok\n- ISSUE: rows differ",
    "PASS: ok\nISSUES: rows",
    "PASS: ok\n1. ISSUE: rows",
    "PASS: ok\n**Verdict — ISSUE**: rows",
    "PASS: ok\nІSSUE: rows",                                      # Cyrillic І
    "PASS: ok\nISSUE： rows",                                      # fullwidth colon
    "PASS: ok\n​ISSUE: rows",                                     # zero-width space
    "PASS: ok\n• ISSUE: rows",                                    # bullet
    "PASS: ok\n<b>ISSUE</b>: rows",
    "PASS: ok\n| ISSUE: rows |",
    # ... and a second verdict on the PASS line itself (round-5 review):
    "PASS: cell ran without errors. ISSUE: 0 rows vs 1,234 in the original | FIX: read the OCI path",
    "**PASS**: ok — but ISSUE: rows differ",
    "PASS: ok, issue: rows differ",
    "PASS: ok, **Issues**: rows differ",
    "PASS: looks right | FIX: filter the nulls",
    "PASS: ok (FIX: n/a)",                                              # FIX in any form
    "PASS: ok; FIX: filter the nulls",
    # ... and a one-line reply that opens a fence (#185 consolidated round 2):
    "```PASS: rows match```",
    "```PASS: rows match",
    "~~~PASS: rows match",
])
def test_a_pass_with_anything_else_in_the_reply_is_no_verdict(reply):
    assert jm._parse_eval_verdict(reply) == (None, "")


def test_a_pass_that_mentions_no_issues_is_still_a_pass():
    assert jm._parse_eval_verdict("PASS: no issues found, output matches the original") == (
        "PASS", "PASS: no issues found, output matches the original")


def test_the_issue_line_is_returned_when_pass_lines_come_first():
    assert jm._parse_eval_verdict("PASS: schema\nISSUE: rows | FIX: filter") == (
        "ISSUE", "ISSUE: rows | FIX: filter")


def test_a_pass_line_does_not_outvote_an_issue_line(monkeypatch, tmp_path):
    """Cell-loop level: PASS then ISSUE goes to the fix loop with the FIX hint."""
    result, fix_calls, _ = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"), [
        "PASS: schema matches\nISSUE: row counts differ | FIX: HINT_FROM_ISSUE",
        "PASS: output looks right"])
    assert len(fix_calls) == 1 and "HINT_FROM_ISSUE" in fix_calls[0][1]
    assert result["fixed"] == 1


def test_a_pass_next_to_an_off_contract_issue_is_retried_then_fails_closed(monkeypatch, tmp_path):
    """Cell-loop level, the round-4 bypass (`Issue:` is not the contract form):
    the reply is retried with the single-line rule restated, and a second
    ambiguous reply sends the cell to the fix loop instead of accepting it."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"), [
        "PASS: schema ok\nIssue: rows differ",
        "Rechecked.\nPASS: schema ok",
        "PASS: output looks right"])
    assert len(eval_calls) == 3 and len(fix_calls) == 1
    assert "nothing else" in eval_calls[1]
    assert result["fixed"] == 1


def test_a_pass_line_carrying_an_issue_verdict_goes_to_the_fix_loop(monkeypatch, tmp_path):
    """Cell-loop level (round-5 review, executed: task PASS before the fix)."""
    reply = ("PASS: cell ran without errors. ISSUE: 0 rows vs 1,234 in the original "
             "| FIX: read the OCI path")
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         [reply, reply, "PASS: output looks right"])
    assert len(eval_calls) == 3 and len(fix_calls) == 1
    assert result["fixed"] == 1


def test_an_unusable_verdict_reply_is_logged(monkeypatch, tmp_path, capsys):
    """Round-5 review: the log said only "no usable verdict", so an operator
    could not tell an ambiguous reply from an empty one."""
    _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
         ["Checked 1,234 rows.\nPASS: same rows", "", "PASS: output looks right"])
    out = capsys.readouterr().out
    assert "reply ended: 'Checked 1,234 rows.\\nPASS: same rows'" in out
    assert "the reply was empty" in out


def test_a_clean_cell_whose_pass_quotes_an_issues_identifier_is_accepted(monkeypatch, tmp_path):
    """#185 round 4 finding 1 (executed regression from 904f94f): a one-line PASS
    quoting `open_issues` was read as a second verdict, so a clean cell failed
    and stopped the job."""
    reply = "PASS: the cell printed {'open_issues': 12}, same as the original"
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         [reply])
    assert len(eval_calls) == 1 and not fix_calls
    assert result["ok"] == 1 and not result["job_should_stop"]


def test_eval_request_offers_no_submit_code(monkeypatch, tmp_path):
    """#185 owner decision 1: the eval is a classification and never returns
    code (or a recovery plan, round 4). Driven through the real
    call_opus_with_tools and a real SDK client (HTTP post stubbed): both eval
    tries send every tool but those two, and the same list, so the retry
    shares the first try's cached prefix."""
    import agent_migrate as am
    from test_sdk_request_shape import _real_client, _reply

    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"))
    bodies = _real_client(monkeypatch, iter([_reply("no verdict here"),
                                             _reply("PASS: output looks right")]))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    monkeypatch.setattr(jm, "call_opus_with_tools", am.call_opus_with_tools)
    result = asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb",
                                             _ScriptedKernel(), "test_eval_gate", "01_task",
                                             {}, run_all=True))
    assert result["ok"] == 1 and len(bodies) == 2
    every_tool = [t["name"] for t in am.OCI_PATH_TOOLS]
    excluded = ("submit_code", "submit_data_recovery_plan")
    assert set(excluded) <= set(every_tool)
    for body in bodies:
        assert [t["name"] for t in body["tools"]] == [n for n in every_tool if n not in excluded]


def test_run_on_cluster_cap_refusal_names_no_absent_tool(monkeypatch):
    """Without submit_code the cap refusal asks for the reply itself."""
    import agent_migrate as am
    from types import SimpleNamespace as NS
    from test_sdk_request_shape import _real_client, _reply

    def run_on_cluster_round(n):
        return NS(content=[NS(type="tool_use", id=f"t{n}", name="run_on_cluster",
                              input={"code": "1"})],
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))

    cap = am.MAX_RUN_ON_CLUSTER_PER_CALL
    replies = [run_on_cluster_round(n) for n in range(cap + 1)] + [_reply("PASS: ok")]

    async def ran(name, tool_input, **k):
        return "1"

    monkeypatch.setattr(am, "_handle_tool_call", ran)
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    for exclude, expected, absent in (((), "submit_code with your best fix", "Reply now"),
                                      (("submit_code",), "Reply now", "submit_code")):
        bodies = _real_client(monkeypatch, iter(replies))
        asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=cap + 2,
                                            exclude_tools=exclude))
        refusal = bodies[-1]["messages"][-1]["content"][0]["content"]
        assert refusal.startswith("REFUSED") and expected in refusal and absent not in refusal


def _cut(text, stop_reason):
    from types import SimpleNamespace as NS
    return NS(content=[NS(type="text", text=text)], stop_reason=stop_reason,
              usage=NS(input_tokens=1, output_tokens=1))


def _tool_round(n, preamble=None):
    from types import SimpleNamespace as NS
    content = [NS(type="text", text=preamble)] if preamble else []
    content.append(NS(type="tool_use", id=f"t{n}", name="run_on_cluster", input={"code": "1"}))
    return NS(content=content, stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))


@pytest.mark.parametrize("replies,rounds,expected", [
    # Cut off at max_tokens, or refused, mid-sentence after a PASS token:
    ([_cut("PASS: no errors in the output; checking whether the", "max_tokens")], 3, ""),
    ([_cut("PASS: no errors in the output; checking whether the", "refusal")], 3, ""),
    # Tool rounds run out on a reply whose PASS text sits next to a tool call
    # that never gets its result: not the answer; the forced final one is.
    ([_tool_round(0), _tool_round(1, "PASS: looks right, confirming row count"),
      _cut("ISSUE: row count differs | FIX: filter", "end_turn")], 1,
     "ISSUE: row count differs | FIX: filter"),
    # ... and a forced final reply that is itself cut off is no answer either.
    ([_tool_round(0), _tool_round(1), _cut("PASS: output looks", "max_tokens")], 1, ""),
])
def test_an_unfinished_reply_returns_no_text_when_the_caller_requires_end_turn(
        monkeypatch, replies, rounds, expected):
    """#185 round 4 (A2): the verdict is parsed from the returned text, so a
    reply the model did not finish must not come back as if it were final."""
    import agent_migrate as am
    from test_sdk_request_shape import _real_client

    async def ran(name, tool_input, **k):
        return "1"

    monkeypatch.setattr(am, "_handle_tool_call", ran)
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    _real_client(monkeypatch, iter(replies))
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=rounds,
                                                  require_end_turn=True))
    assert text == expected


@pytest.mark.parametrize("tool,tool_input", [
    ("submit_code", {"code": "PASS: int = 1"}),
    ("submit_data_recovery_plan", {"table": "t", "date_column": "d", "overrides": []}),
])
def test_a_tool_the_call_did_not_offer_is_refused_not_returned(monkeypatch, tool, tool_input):
    """Round-5 review: a call to an excluded tool returned its input as the
    reply, bypassing exclude_tools and require_end_turn. It now gets an error
    result, and the reply the model then finishes is the answer."""
    import agent_migrate as am
    from types import SimpleNamespace as NS
    from test_sdk_request_shape import _real_client

    called = NS(content=[NS(type="tool_use", id="t0", name=tool, input=tool_input)],
                stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    bodies = _real_client(monkeypatch, iter([called, _cut("ISSUE: rows | FIX: x", "end_turn")]))
    text, _ = asyncio.run(am.call_opus_with_tools(
        "Evaluate", "input", max_tool_rounds=3, require_end_turn=True,
        exclude_tools=("submit_code", "submit_data_recovery_plan")))
    assert text == "ISSUE: rows | FIX: x"
    [result] = bodies[1]["messages"][-1]["content"]
    assert result["is_error"] and "not available" in result["content"]


def test_other_callers_still_get_the_text_of_an_unfinished_reply(monkeypatch):
    """The end_turn rule is the eval's: callers that do not ask for it (fix,
    recovery plan) keep their current behaviour."""
    import agent_migrate as am
    from test_sdk_request_shape import _real_client

    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    _real_client(monkeypatch, iter([_cut("partial text", "max_tokens")]))
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input"))
    assert text == "partial text"


def test_a_cut_off_pass_does_not_accept_the_cell(monkeypatch, tmp_path):
    """Cell-loop level, through the real call_opus_with_tools: two cut-off PASS
    replies are no verdict, so the cell goes to the fix loop; the eval of the
    fixed cell then accepts it."""
    import agent_migrate as am
    from test_sdk_request_shape import _real_client, _reply

    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    bodies = _real_client(monkeypatch, iter([
        _cut("PASS: no errors in the output; checking whether the", "max_tokens"),
        _cut("PASS: no errors", "refusal"),
        _reply("PASS: output looks right")]))
    fix_calls = []

    async def fake_fix(code, error, context, attempt, extra_context="", **k):
        fix_calls.append(f"{error}\n{extra_context}")
        return code

    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    monkeypatch.setattr(jm, "call_opus_with_tools", am.call_opus_with_tools)
    monkeypatch.setattr(jm, "call_fix", fake_fix)
    result = asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb",
                                             _ScriptedKernel(), "test_eval_gate", "01_task",
                                             {}, run_all=True))
    assert len(bodies) == 3 and len(fix_calls) == 1
    assert "no PASS/ISSUE verdict" in fix_calls[0]
    assert result["fixed"] == 1


def test_code_with_pass_before_an_issue_verdict_goes_to_the_fix_loop(monkeypatch, tmp_path):
    """A reply can still quote code (in prose; the eval has no submit_code). A
    `pass` statement in that code must not accept the cell over the ISSUE line."""
    reply = ("```python\ntry:\n    x()\nexcept ValueError:\n    pass\n```\n"
             "ISSUE: wrong rows | FIX: filter")
    result, fix_calls, _ = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                [reply, "PASS: output looks right"])
    assert len(fix_calls) == 1
    assert "filter" in fix_calls[0][1]
    assert result["fixed"] == 1


def test_missing_verdict_is_retried_once(monkeypatch, tmp_path):
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         ["", "PASS: output looks right"])
    assert len(eval_calls) == 2 and not fix_calls
    assert "PASS:" in eval_calls[1] and "ISSUE:" in eval_calls[1]   # the retry restates the contract
    assert result["ok"] == 1


def test_missing_verdict_after_the_retry_fails_closed(monkeypatch, tmp_path):
    """Never PASS by absence: no verdict twice is an ISSUE, so the cell goes to
    the fix loop instead of being accepted unverified."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         ["", "no verdict here", "PASS: output looks right"])
    assert len(eval_calls) == 3          # attempt 1: eval + retry; attempt 2: eval
    assert len(fix_calls) == 1
    assert result["fixed"] == 1


def test_no_verdict_on_the_final_attempt_fails_the_cell_with_a_note(monkeypatch, tmp_path):
    """#185 review A1: a cell that never gets a verdict ends `error` and stops
    the job, and the failure carries a note so JOB_REPORT lists it."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         ["", "x", "", "x"])
    assert len(eval_calls) == 4 and len(fix_calls) == 1
    assert result["status"] == "FAIL" and result["job_should_stop"]
    errors = [cr for cr in result["cell_results"] if cr["status"] == "error"]
    assert len(errors) == 1
    assert "no PASS/ISSUE verdict" in errors[0]["note"]



def _bad_request():
    import anthropic
    import httpx
    response = httpx.Response(400, request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
    return anthropic.BadRequestError("Error code: 400 - invalid_request_error", response=response,
                                     body=None)


_EVAL_CALL_FAILURES = [
    pytest.param(lambda: RuntimeError("Error code: 529 overloaded_error: retry budget exhausted"),
                 id="retries-exhausted"),
    pytest.param(_bad_request, id="non-transient-400"),
]


@pytest.mark.parametrize("failure", _EVAL_CALL_FAILURES)
def test_an_eval_call_that_raises_fails_the_output_check_closed(monkeypatch, tmp_path, failure):
    """#191 round 5: an eval request that raises (retries exhausted, a
    non-transient 400) is an unverified output, like a reply with no verdict:
    the cell goes to the fix loop instead of the exception ending the task. The
    failed call is not retried (the request wrapper has already retried)."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         [failure(), "PASS: output looks right"])
    assert len(eval_calls) == 2 and len(fix_calls) == 1
    assert result["fixed"] == 1 and result["failed"] == 0


@pytest.mark.parametrize("failure", _EVAL_CALL_FAILURES)
def test_an_eval_call_that_raises_on_the_final_attempt_fails_the_cell_with_a_note(
        monkeypatch, tmp_path, failure):
    """... and on the final attempt the cell ends `error` with the call's error
    in its note and stops the job; the task still returns its result."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"),
                                         [failure(), failure()])
    assert len(eval_calls) == 2 and len(fix_calls) == 1
    assert result["status"] == "FAIL" and result["job_should_stop"]
    errors = [cr for cr in result["cell_results"] if cr["status"] == "error"]
    assert len(errors) == 1
    assert "Opus evaluation request failed" in errors[0]["note"]


def test_a_cancel_during_the_eval_call_still_propagates(monkeypatch, tmp_path):
    """Only an Exception is an unverified output; a cancel or Ctrl-C ends the task."""
    with pytest.raises(asyncio.CancelledError):
        _run(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"), [asyncio.CancelledError()])


def _eval_calls_for(monkeypatch, tmp_path, replies):
    """Run one clean cell with scripted eval replies; return each eval call's
    (user_content, kwargs)."""
    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_A = 1\n"))
    replies = iter(replies)
    calls = []

    async def fake_opus(system, user_content, *a, **k):
        calls.append((user_content, k))
        return next(replies), {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", fake_opus)
    result = asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb",
                                             _ScriptedKernel(), "test_eval_gate", "01_task",
                                             {}, run_all=True))
    assert result["ok"] == 1
    return calls


def test_verdict_retry_has_more_room_and_sees_its_first_reply(monkeypatch, tmp_path):
    """#185 review A2: a reply without a verdict is usually a cut-off after
    thinking, so an identical retry fails the same way. The retry raises
    max_tokens and carries the tail of reply 1, so the model can restate its
    verdict; it is booked under its own route so its cost is visible."""
    head = "HEAD_SENTINEL " + "x" * 3000
    (first, first_kw), (retry, retry_kw) = _eval_calls_for(
        monkeypatch, tmp_path, [head + " TAIL_SENTINEL", "PASS: output looks right"])
    assert "TAIL_SENTINEL" not in first and "TAIL_SENTINEL" in retry
    assert "HEAD_SENTINEL" not in retry, "only the last 2000 chars of reply 1"
    assert (first_kw["max_tokens"], retry_kw["max_tokens"]) == (16000, 32000)
    assert first_kw.get("route") is None          # inferred as `eval`
    assert retry_kw["route"] == "eval_retry"


def test_empty_first_reply_is_not_quoted_in_the_retry(monkeypatch, tmp_path):
    (_, _), (retry, _) = _eval_calls_for(monkeypatch, tmp_path,
                                         ["   ", "PASS: output looks right"])
    assert "Your previous reply was" not in retry


def test_printed_failure_is_evaluated_before_a_fix_attempt(monkeypatch, tmp_path):
    """Printed failures remain blocking when the model returns ISSUE. Unlike
    an execution error, rendered text alone does not bypass evaluation."""
    _, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_C = 1\n"),
                                    ["ISSUE: printed failure | FIX: none"] * 2)
    assert len(fix_calls) == 1
    assert len(eval_calls) == 2


def test_eval_runs_on_the_final_attempt_of_a_printed_failure(monkeypatch, tmp_path):
    """Owner decision (#185): the final attempt has no fix call after it, so the
    eval (which may ask for a fixup_cell rewind) still runs there. The cell's
    outcome is unchanged: it ends `error` and stops the job."""
    result, fix_calls, eval_calls = _run(monkeypatch, tmp_path, _notebook("SENTINEL_C = 1\n"),
                                         ["ISSUE: printed failure | FIX: none"] * 2)
    assert len(eval_calls) == 2 and len(fix_calls) == 1
    assert result["status"] == "FAIL" and result["job_should_stop"]


# ── #185 round 2: a final attempt that takes a retry `continue` ──
# The rate-limit and auto-install pre-checks `continue` the attempt loop. On
# the last attempt that ended the loop before the retries-exhausted handling,
# so the cell left no result, the job did not stop and the task read PASS.

class _MissingModuleKernel(_ScriptedKernel):
    """SENTINEL_M fails with a different missing module on every execution."""
    def __init__(self):
        super().__init__()
        self.modules = iter(["foo", "bar", "baz", "qux"])

    async def execute(self, code, **kwargs):
        if "SENTINEL_M" in code:
            self.executed.append(code)
            mod = next(self.modules)
            return {"status": "error", "outputs": [{
                "type": "error", "ename": "ModuleNotFoundError",
                "evalue": f"No module named '{mod}'",
                "traceback": [f"ModuleNotFoundError: No module named '{mod}'"]}]}
        return await super().execute(code, **kwargs)


def _run_final_attempt_continue(monkeypatch, tmp_path, kernel):
    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_M = 1\n"))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)

    async def fake_fix(code, error, context, attempt, extra_context="", **k):
        return code

    monkeypatch.setattr(jm, "call_fix", fake_fix)
    return asyncio.run(jm.process_notebook("/Workspace/test_cond/source.ipynb", kernel,
                                           "test_eval_gate", "01_task", {}, run_all=True))


def _assert_cell_failed_and_job_stopped(result):
    assert result["status"] == "FAIL", result
    assert result["job_should_stop"]
    errors = [cr for cr in result["cell_results"] if cr["status"] == "error"]
    assert len(errors) == 1 and errors[0]["note"]


def test_auto_install_on_the_final_attempt_does_not_skip_the_failure(monkeypatch, tmp_path):
    import cluster_lifecycle
    installs = []

    async def fake_install(cluster_id, session, module, *a, **k):
        installs.append(module)
        return True

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    result = _run_final_attempt_continue(monkeypatch, tmp_path, _MissingModuleKernel())
    _assert_cell_failed_and_job_stopped(result)
    assert installs == ["foo"], "no install whose retry can never run"
    assert "No module named 'bar'" in result["cell_results"][0]["note"]


def test_rate_limit_on_the_final_attempt_does_not_skip_the_failure(monkeypatch, tmp_path):
    from types import SimpleNamespace as NS
    monkeypatch.setattr(jm, "_detect_rate_limit", lambda output, attempt: NS(
        detected=True, reason="OCI 429", bucket="b", backoff_sec=0))
    result = _run_final_attempt_continue(monkeypatch, tmp_path, _MissingModuleKernel())
    _assert_cell_failed_and_job_stopped(result)


@pytest.mark.parametrize("budget", [1, 2, 5])
@pytest.mark.parametrize("repair_timing", ["early", "final"])
@pytest.mark.parametrize("outcome", ["healed", "still_fails", "save_failed", "repairs_again", "eval_raises"])
def test_eval_dependency_repair_requires_fresh_bounded_caller_execution(
        monkeypatch, tmp_path, budget, repair_timing, outcome):
    """Real parent loop: a repair on the final eval must not strand the caller.

    Use the actual tool handler/receipt; only dependency persistence and the
    model transport are scripted. Persistence itself has child-loop coverage.
    """
    import agent_migrate as am
    from contextvars import ContextVar

    _patch_common(monkeypatch, tmp_path, _notebook("SENTINEL_REPAIR = 1\n"))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", budget)
    repair_attempt = 1 if repair_timing == "early" else budget
    monkeypatch.setattr(am, "_dependency_repair_receipt", ContextVar("test_receipt", default=0))
    monkeypatch.setattr(am, "_current_task_key", "fixture")
    monkeypatch.setattr(am, "_cell_history", [{"_task_key": "fixture"}])
    executions = []
    saved = False

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if "SENTINEL_REPAIR" in code and "base64" not in code:
                executions.append(saved)
                # Early evaluation is reachable with semantic, not execution,
                # failure. The final attempt models the incident's traceback.
                text = ("fresh caller output after saved repair" if saved
                        else "Traceback: wrong helper schema" if repair_timing == "final"
                        else "old caller output with wrong helper schema")
                return {"status": "ok", "outputs": [
                    {"type": "stream", "name": "stdout", "text": text}]}
            return await super().execute(code, **kwargs)

    async def persist(*args):
        nonlocal saved
        if outcome == "save_failed":
            return "ERROR: save refused"
        saved = True
        return "OK: dependency saved"

    async def evaluate(*args, **kwargs):
        if len(executions) >= repair_attempt and (not saved or outcome == "repairs_again"):
            repair_result = await am._handle_tool_call("repair_dependency", {
                "index": 0, "fixed_code": "def helper(): return 1", "why": "fixture"})
            if not repair_result.startswith("OK:"):
                return "ISSUE: dependency save failed", {"input": 0, "output": 0}
            if outcome == "eval_raises":
                raise RuntimeError("evaluation ended after successful save")
            # Even PASS cannot certify a pre-repair execution.
            return "PASS: helper repaired", {"input": 0, "output": 0}
        return ("PASS: fresh output verified" if saved and outcome in ("healed", "eval_raises")
                else "ISSUE: caller still fails"), {"input": 0, "output": 0}

    async def fix(code, *args, **kwargs):
        return code

    monkeypatch.setattr(am, "_repair_dependency_fn", persist)
    monkeypatch.setattr(jm, "call_opus_with_tools", evaluate)
    monkeypatch.setattr(jm, "call_fix", fix)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", Kernel(), "test_eval_repair", "01_task", {}, run_all=True))
    if outcome in ("healed", "eval_raises"):
        assert result["failed"] == 0, result
        assert executions == [False] * repair_attempt + [True]
    else:
        assert result["status"] == "FAIL", result
        extra = outcome != "save_failed" and (outcome == "repairs_again" or repair_attempt == budget)
        assert len(executions) == budget + int(extra)
        assert result["cell_results"][0]["attempts"] == len(executions)
    assert len(executions) <= budget + 1


def test_dependency_repair_receipt_is_isolated_between_concurrent_tasks(monkeypatch):
    import agent_migrate as am
    from contextvars import ContextVar

    monkeypatch.setattr(am, "_dependency_repair_receipt", ContextVar("isolated_receipt", default=0))
    monkeypatch.setattr(am, "_current_task_key", "fixture")
    monkeypatch.setattr(am, "_cell_history", [{"_task_key": "fixture"}])

    async def persist(*args):
        return "OK: saved"

    monkeypatch.setattr(am, "_repair_dependency_fn", persist)

    async def scenario():
        saved = asyncio.Event()

        async def repair():
            await am._handle_tool_call("repair_dependency", {
                "index": 0, "fixed_code": "def helper(): return 1", "why": "fixture"})
            saved.set()
            return am.dependency_repair_receipt()

        async def unrelated():
            await saved.wait()
            return am.dependency_repair_receipt()

        assert await asyncio.gather(repair(), unrelated()) == [1, 0]
        assert am.dependency_repair_receipt() == 0

    asyncio.run(scenario())
