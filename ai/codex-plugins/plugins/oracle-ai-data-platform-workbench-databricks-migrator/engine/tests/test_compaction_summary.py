"""Tier-3 context compaction: the summary request on an always-thinking model.

On the Opus-5 family thinking is on by default and counts against max_tokens,
so the response can open with a thinking block and can be cut off before the
summary ends. Both must be handled explicitly, and the request must be billed
in the usage ledger like every other Claude call.
"""
from __future__ import annotations

import asyncio
import sys
import unittest.mock as mock
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import llm_config  # noqa: E402
from context_compactor import ContextCompactor  # noqa: E402


def _messages(n=20):
    msgs = [{"role": "user", "content": "original prompt"}]
    msgs += [{"role": "assistant" if i % 2 == 0 else "user", "content": f"msg {i}"}
             for i in range(n)]
    return msgs


def _compact(response, **compactor_kwargs):
    """Run Tier 3 against a fake Anthropic client; return (result, create kwargs)."""
    requests = []

    def create(**kwargs):
        requests.append(kwargs)
        return response

    fake = mock.MagicMock()
    fake.Anthropic.return_value = NS(messages=NS(create=create))
    compactor = ContextCompactor(call_id="test-compaction", log_fn=lambda m: None,
                                 **compactor_kwargs)
    with mock.patch.dict("sys.modules", {"anthropic": fake}):
        result = compactor._compact_tier3(_messages())
    return result, requests


def _reply(*blocks, stop_reason="end_turn"):
    return NS(content=list(blocks), stop_reason=stop_reason,
              usage=NS(input_tokens=900, output_tokens=40))


THINKING = NS(type="thinking", thinking="")


def test_summary_is_read_from_the_text_block_after_thinking():
    result, _ = _compact(_reply(THINKING, NS(type="text", text="SUMMARY_SENTINEL")))
    assert len(result) == 3
    assert "SUMMARY_SENTINEL" in result[1]["content"]


def test_summary_request_is_recorded_as_compaction_usage():
    recorded = []
    reply = _reply(THINKING, NS(type="text", text="summary"))
    _compact(reply, record_usage=recorded.append)
    assert recorded == [reply.usage]


def test_truncated_summary_falls_back_to_tier2():
    """A summary cut off at max_tokens would silently drop the tail of the
    conversation state; Tier 2 keeps the recent messages verbatim instead."""
    result, _ = _compact(_reply(THINKING, NS(type="text", text="partial"),
                                stop_reason="max_tokens"))
    assert not any("[CONTEXT COMPACTED]" in str(m.get("content")) for m in result)


def test_response_without_text_falls_back_to_tier2():
    result, _ = _compact(_reply(THINKING))
    assert not any("[CONTEXT COMPACTED]" in str(m.get("content")) for m in result)


def test_summary_request_leaves_room_for_thinking():
    _, requests = _compact(_reply(NS(type="text", text="summary")))
    assert requests[0]["max_tokens"] == llm_config.MAX_TOKENS


def test_tool_loop_bills_compaction_in_the_usage_ledger(monkeypatch):
    """Orchestration: the compactor call_opus_with_tools builds must feed the
    same ledger as every other request, under its own route."""
    import asyncio
    import agent_migrate as am

    monkeypatch.setattr(am, "USAGE_LEDGER", [])
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    monkeypatch.setattr(ContextCompactor, "estimate_tokens", staticmethod(lambda m: 900_000))
    main_reply = NS(content=[NS(type="text", text="VERIFIED")], stop_reason="end_turn",
                    usage=NS(input_tokens=10, output_tokens=5))
    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=lambda **k: main_reply)))
    fake = mock.MagicMock()
    fake.Anthropic.return_value = NS(messages=NS(create=lambda **k: _reply(
        THINKING, NS(type="text", text="summary"))))
    with mock.patch.dict("sys.modules", {"anthropic": fake}):
        text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input"))
    assert text == "VERIFIED"
    assert [r["route"] for r in am.USAGE_LEDGER].count("compaction") == 1


# ── Signed thinking blocks are bound to the prefix they were produced under ──
# (#185 review item 2). Tier 3 sends the loop's turns under a different `system`
# and no `tools`; Tier 2 / the emergency trim drop the middle of the history.
# Either way the API rejects replayed thinking blocks on enforced accounts, so
# they are stripped from every assistant turn (text and tool_use stay).

def _tool_loop_messages(rounds=6):
    msgs = [{"role": "user", "content": "original prompt"}]
    for r in range(rounds):
        msgs.append({"role": "assistant", "content": [
            NS(type="thinking", thinking="", signature=f"sig{r}"),
            {"type": "redacted_thinking", "data": f"enc{r}"},
            NS(type="tool_use", id=f"tu{r}", name="explore_path", input={"path": f"p{r}"}),
        ]})
        msgs.append({"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": f"tu{r}", "content": f"result {r}"}]})
    return msgs


def _block_types(messages):
    return [getattr(b, "type", None) or b.get("type")
            for m in messages if isinstance(m.get("content"), list) for b in m["content"]]


def _fake_anthropic(create):
    fake = mock.MagicMock()
    fake.Anthropic.return_value = NS(messages=NS(create=create))
    return fake


def test_tier3_summary_request_replays_no_thinking_blocks():
    requests = []

    def create(**kwargs):
        requests.append(kwargs)
        return _reply(THINKING, NS(type="text", text="summary"))

    msgs = _tool_loop_messages()
    compactor = ContextCompactor(call_id="test-compaction", log_fn=lambda m: None)
    with mock.patch.dict("sys.modules", {"anthropic": _fake_anthropic(create)}):
        compactor._compact_tier3(msgs)
    sent = _block_types(requests[0]["messages"])
    assert "thinking" not in sent and "redacted_thinking" not in sent
    assert sent.count("tool_use") == 6 and sent.count("tool_result") == 6
    assert _block_types(msgs).count("thinking") == 6, "the loop's own history is not mutated"


def test_tier2_retained_turns_carry_no_thinking_blocks():
    compactor = ContextCompactor(call_id="test-compaction", log_fn=lambda m: None)
    result = compactor._compact_tier2(_tool_loop_messages())
    kept = _block_types(result)
    assert "thinking" not in kept and "redacted_thinking" not in kept
    assert "tool_use" in kept and "tool_result" in kept
    assert all(m["content"] for m in result)


def test_tier2_below_the_drop_size_leaves_history_unchanged():
    """No turn is dropped, so the prefix is intact and thinking stays valid."""
    msgs = _tool_loop_messages(rounds=2)
    compactor = ContextCompactor(call_id="test-compaction", log_fn=lambda m: None)
    assert compactor._compact_tier2(msgs) is msgs


def test_rejected_summary_request_falls_back_to_tier2_without_usage():
    """E.g. a 400 on the summary request: Tier 2 (thinking stripped), nothing
    billed for a request that produced no response, and the fallback is logged."""
    def create(**kwargs):
        raise RuntimeError("400 invalid_request_error: Invalid `signature` in `thinking` block")

    recorded, logged = [], []
    compactor = ContextCompactor(call_id="test-compaction", log_fn=logged.append,
                                 record_usage=recorded.append)
    with mock.patch.dict("sys.modules", {"anthropic": _fake_anthropic(create)}):
        result = compactor._compact_tier3(_tool_loop_messages())
    assert not any("[CONTEXT COMPACTED]" in str(m.get("content")) for m in result)
    assert "thinking" not in _block_types(result)
    assert recorded == []
    assert any("falling back to Tier 2" in line for line in logged)


@pytest.mark.parametrize("stop_reason", ["refusal", "pause_turn", "max_tokens"])
def test_summary_that_did_not_end_its_turn_falls_back_to_tier2(stop_reason):
    """A refusal is HTTP 200 with partial text; only end_turn is a complete summary."""
    result, _ = _compact(_reply(NS(type="text", text="partial"), stop_reason=stop_reason))
    assert not any("[CONTEXT COMPACTED]" in str(m.get("content")) for m in result)


def test_emergency_trim_retry_replays_no_thinking_blocks(monkeypatch):
    """call_opus_with_tools' prompt-too-long fallback keeps first + last two
    turns; the retained assistant turn's thinking predates the trim."""
    import asyncio
    import agent_migrate as am

    monkeypatch.setattr(am, "USAGE_LEDGER", [])
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    monkeypatch.setattr(am, "_format_history_context", lambda: "")

    async def fake_tool(name, tool_input, **k):
        return "ok"

    monkeypatch.setattr(am, "_handle_tool_call", fake_tool)
    calls = []
    tool_reply = NS(content=[NS(type="thinking", thinking="", signature="s"),
                             NS(type="tool_use", id="tu", name="explore_path", input={})],
                    stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    final_reply = NS(content=[NS(type="text", text="DONE")], stop_reason="end_turn",
                     usage=NS(input_tokens=1, output_tokens=1))

    def create(**kwargs):
        calls.append(kwargs["messages"])
        if len(calls) <= 2:
            return tool_reply
        if len(calls) == 3:
            raise RuntimeError("prompt is too long: 1000001 tokens > 1000000 maximum")
        return final_reply

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=5))
    assert text == "DONE"
    assert "thinking" not in _block_types(calls[3])
    assert "tool_use" in _block_types(calls[3])


# ── #185 round 2: the two requests that change `tools` mid-conversation ──
# Removing tools also invalidates replayed thinking on enforced accounts.

def _tool_loop_driver(monkeypatch, replies):
    """call_opus_with_tools against a scripted client; returns the requests."""
    import agent_migrate as am

    monkeypatch.setattr(am, "USAGE_LEDGER", [])
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    monkeypatch.setattr(am, "_format_history_context", lambda: "")

    async def fake_tool(name, tool_input, **k):
        return "ok"

    monkeypatch.setattr(am, "_handle_tool_call", fake_tool)
    replies = iter(replies)
    calls = []

    def create(**kwargs):
        calls.append(kwargs)
        return next(replies)

    monkeypatch.setattr(am, "claude", lambda: NS(messages=NS(create=create)))
    return am, calls


def _thinking_tool_reply(name="explore_path"):
    return NS(content=[NS(type="thinking", thinking="", signature="s"),
                       NS(type="tool_use", id=f"tu-{name}", name=name, input={})],
              stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))


def test_forced_final_answer_replays_no_thinking_blocks(monkeypatch):
    """Tool rounds exhausted, no text: the final request drops `tools`."""
    final = NS(content=[NS(type="text", text="DONE")], stop_reason="end_turn",
               usage=NS(input_tokens=1, output_tokens=1))
    am, calls = _tool_loop_driver(monkeypatch, [_thinking_tool_reply(), _thinking_tool_reply(),
                                                final])
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=1))
    assert text == "DONE"
    assert "tools" not in calls[-1]
    assert "thinking" not in _block_types(calls[-1]["messages"])
    assert "tool_use" in _block_types(calls[-1]["messages"])


def test_forced_final_answer_sends_each_tool_use_once(monkeypatch):
    """#185 round 3 note, made reachable on every exhausted eval by
    require_end_turn (round 5): the loop has already appended the last tool
    turn and its results, so the forced final request must not append them
    again (duplicate tool_use ids are a 400)."""
    def tool_round(n):
        return NS(content=[NS(type="tool_use", id=f"tu-{n}", name="explore_path", input={})],
                  stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))

    final = NS(content=[NS(type="text", text="PASS: ok")], stop_reason="end_turn",
               usage=NS(input_tokens=1, output_tokens=1))
    am, calls = _tool_loop_driver(monkeypatch, [tool_round(0), tool_round(1), final])
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input", max_tool_rounds=1,
                                                  require_end_turn=True))
    assert text == "PASS: ok"
    messages = calls[-1]["messages"]
    uses = [b.id for m in messages if m["role"] == "assistant"
            for b in m["content"] if getattr(b, "type", "") == "tool_use"]
    results = [b["tool_use_id"] for m in messages if m["role"] == "user"
               and isinstance(m["content"], list) for b in m["content"]
               if isinstance(b, dict) and b.get("type") == "tool_result"]
    assert uses == ["tu-0", "tu-1"] and results == ["tu-0", "tu-1"]
    assert "final answer" in messages[-1]["content"]


def test_code_delivery_retry_replays_no_thinking_blocks(monkeypatch):
    """Tool rounds exhausted with a code validator: the delivery retry narrows
    `tools` to submit_code."""
    delivered = NS(content=[NS(type="tool_use", id="tu-sub", name="submit_code",
                               input={"code": "x = 1"})],
                   stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    am, calls = _tool_loop_driver(monkeypatch, [_thinking_tool_reply(), _thinking_tool_reply(),
                                                delivered])
    code, _ = asyncio.run(am.call_opus_with_tools("Migrate", "input", max_tool_rounds=1,
                                                  code_validator=lambda c: c == "x = 1"))
    assert code == "x = 1"
    assert [t["name"] for t in calls[-1]["tools"]] == ["submit_code"]
    assert "thinking" not in _block_types(calls[-1]["messages"])
    assert "tool_use" in _block_types(calls[-1]["messages"])


@pytest.mark.parametrize("rejected_on", ["tool_loop", "delivery_retry_1"])
def test_thinking_only_delivery_reply_leaves_no_empty_assistant_turn(monkeypatch, rejected_on):
    """#185 round 3 / consolidated review item 5: a rejected delivery reply
    that holds only a thinking block was appended raw, and the next request's
    thinking strip turned it into `content: []` — a non-final empty assistant
    turn the API rejects. Strip before appending; leave the turn out if empty."""
    thinking_only = NS(content=[NS(type="thinking", thinking="", signature="s")],
                       stop_reason="end_turn", usage=NS(input_tokens=1, output_tokens=1))
    delivered = NS(content=[NS(type="tool_use", id="tu-sub", name="submit_code",
                               input={"code": "x = 1"})],
                   stop_reason="tool_use", usage=NS(input_tokens=1, output_tokens=1))
    replies = ([thinking_only, delivered] if rejected_on == "tool_loop"
               else [_thinking_tool_reply(), _thinking_tool_reply(), thinking_only, delivered])
    am, calls = _tool_loop_driver(monkeypatch, replies)
    code, _ = asyncio.run(am.call_opus_with_tools("Migrate", "input", max_tool_rounds=1,
                                                  code_validator=lambda c: c == "x = 1"))
    assert code == "x = 1"
    for message in calls[-1]["messages"]:
        assert message["content"], f"empty {message['role']} turn sent"
    assert "thinking" not in _block_types(calls[-1]["messages"])
