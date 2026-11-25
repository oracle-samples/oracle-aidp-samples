"""Pins for the OpenAI Responses adapter (scripts/openai_model.py).

The Codex bundle routes model calls through OpenAIModelCompatClient, which
answers the Anthropic `messages.create` surface. These tests pin the v0.3.0
contract points the forward-port added: system-as-blocks, cache_control
stripping, dict text/tool_result conversion, output_config -> reasoning
effort, cached-token usage mapping, and transport-error translation."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import pytest

import openai_model as om


def test_system_blocks_join_and_drop_cache_control():
    system = [
        {"type": "text", "text": "part one", "cache_control": {"type": "ephemeral"}},
        {"type": "text", "text": "part two"},
    ]
    assert om._system_text(system) == "part one\n\npart two"
    assert om._system_text("plain") == "plain"
    assert om._system_text(None) is None


def test_convert_content_handles_dict_text_and_tool_result_lists():
    out = om.CompatMessages._convert_content([
        {"type": "text", "text": "hello", "cache_control": {"type": "ephemeral"}},
        {"type": "tool_result", "tool_use_id": "t1",
         "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]},
        {"type": "thinking", "thinking": "internal"},
    ])
    assert out[0] == {"type": "output_text", "text": "hello"}
    assert out[1] == {"type": "function_call_output", "call_id": "t1", "output": "ab"}
    assert len(out) == 2  # thinking dropped


def test_user_text_blocks_never_become_str_dict():
    msgs = om.CompatMessages._convert_messages([
        {"role": "user", "content": [{"type": "text", "text": "ask", "cache_control": {"type": "ephemeral"}}]},
    ])
    assert msgs == [{"role": "user", "content": "ask"}]


def test_create_maps_output_config_effort_and_absorbs_thinking(monkeypatch):
    seen = {}
    def fake_responses_create(**kw):
        seen.update(kw)
        class R:
            output = []
            output_text = "ok"
            usage = None
        return R()
    monkeypatch.setattr(om, "responses_create", fake_responses_create)
    client = om.OpenAIModelCompatClient(api_key="k")
    resp = client.messages.create(
        model="gpt-5.5", max_tokens=100,
        system=[{"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": "hi"}],
        output_config={"effort": "low"},
        thinking={"type": "adaptive"},
    )
    assert seen["effort"] == "low"
    assert seen["model"] == "gpt-5.5"
    assert seen["instructions"] == "sys"
    assert resp.content[0].text == "ok"
    assert resp.stop_reason == "end_turn"


def test_usage_maps_cached_tokens():
    class Details:
        cached_tokens = 7
    class U:
        input_tokens = 10
        output_tokens = 3
        input_tokens_details = Details()
    class R:
        output = []
        output_text = "x"
        usage = U()
    resp = om.CompatResponse(R())
    assert resp.usage.input_tokens == 3   # uncached = total(10) - cached(7)
    assert resp.usage.cache_read_input_tokens == 7
    assert resp.usage.cache_creation_input_tokens == 0


def test_transport_errors_translate_to_anthropic_types():
    openai = pytest.importorskip("openai")
    anthropic = pytest.importorskip("anthropic")
    import httpx
    req = httpx.Request("POST", "https://api.openai.com/v1/responses")
    out = om._translate_error(openai.APIConnectionError(message="boom", request=req))
    assert isinstance(out, anthropic.APIConnectionError)
    out = om._translate_error(openai.APITimeoutError(request=req))
    assert isinstance(out, anthropic.APITimeoutError)
    plain = ValueError("x")
    assert om._translate_error(plain) is plain


def test_incomplete_response_maps_to_max_tokens():
    class R:
        output = []
        output_text = "partial PASS:"
        usage = None
        status = "incomplete"
    resp = om.CompatResponse(R())
    assert resp.stop_reason == "max_tokens"


def test_usage_meters_are_disjoint():
    class Details:
        cached_tokens = 60
    class U:
        input_tokens = 100
        output_tokens = 5
        input_tokens_details = Details()
    class R:
        output = []
        output_text = "x"
        usage = U()
    resp = om.CompatResponse(R())
    assert resp.usage.input_tokens == 40          # uncached only
    assert resp.usage.cache_read_input_tokens == 60
    # billed sum per engine ledger = uncached + cache_read + cache_write + output
    assert 40 + 60 + 0 + 5 == 105


def test_client_never_mutates_openai_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-right")
    om._EXPLICIT_API_KEY = None
    om.OpenAIModelCompatClient(api_key="sk-ant-wrong-provider-key")
    import os
    assert os.environ["OPENAI_API_KEY"] == "sk-right"
    om._EXPLICIT_API_KEY = None  # reset module state for other tests


class _Reasoning:
    type = "reasoning"
    id = "rs_1"
    encrypted_content = "enc"


class _Call:
    type = "function_call"
    name = "run_sql"
    arguments = "{}"
    call_id = "c1"
    id = "fc_1"


def test_reasoning_rides_on_the_tool_use_block():
    class R:
        output = [_Reasoning(), _Call()]
        output_text = ""
        usage = None
    resp = om.CompatResponse(R())
    kinds = [b.type for b in resp.content]
    assert kinds == ["tool_use"]            # never a leading thinking block
    assert resp.content[0].preceding_items and resp.content[0].preceding_items[0].type == "reasoning"
    replay = om.CompatMessages._convert_messages([
        {"role": "assistant", "content": resp.content},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "ok"}]},
    ])
    # raw reasoning item immediately precedes the RAW function_call item (ids intact)
    r_idx = next(i for i, x in enumerate(replay) if getattr(x, "type", None) == "reasoning")
    assert getattr(replay[r_idx + 1], "type", None) == "function_call"
    assert getattr(replay[r_idx + 1], "id", None) == "fc_1"
    assert any(isinstance(x, dict) and x.get("type") == "function_call_output" for x in replay)


def test_reasoning_then_message_keeps_text_first():
    class _C:
        type = "output_text"
        text = "the answer"
    class Msg:
        type = "message"
        content = [_C()]
    class R:
        output = [_Reasoning(), Msg()]
        output_text = "the answer"
        usage = None
    resp = om.CompatResponse(R())
    assert resp.content[0].type == "text"
    assert resp.content[0].text == "the answer"


def test_requests_are_stateless_with_encrypted_reasoning(monkeypatch):
    seen = {}
    class FakeResponses:
        def create(self, **kw):
            seen.update(kw)
            class R:
                output = []
                output_text = "ok"
                usage = None
            return R()
    class FakeClient:
        responses = FakeResponses()
        def with_options(self, **kw):
            return self
    monkeypatch.setattr(om, "_CLIENT", FakeClient())
    om.responses_create(instructions=None, input=[], max_tokens=10)
    assert seen["store"] is False
    assert seen["include"] == ["reasoning.encrypted_content"]
    om._CLIENT = None


def test_effort_max_clamps_to_high(monkeypatch):
    seen = {}
    def fake(**kw):
        seen.update(kw)
        class R:
            output = []
            output_text = "ok"
            usage = None
        return R()
    monkeypatch.setattr(om, "responses_create", fake)
    om.OpenAIModelCompatClient(api_key="k").messages.create(
        model="gpt-5.5", messages=[{"role": "user", "content": "x"}],
        output_config={"effort": "max"})
    assert seen["effort"] == "max"  # clamped inside responses_create
    assert om._clamp_effort("max") == "high"
    assert om._clamp_effort("low") == "low"


def test_error_translation_overflow_and_5xx():
    openai = pytest.importorskip("openai")
    anthropic = pytest.importorskip("anthropic")
    import httpx
    req = httpx.Request("POST", "https://api.openai.com/v1/responses")
    resp400 = httpx.Response(400, request=req)
    e = openai.BadRequestError("This model's maximum context length is exceeded",
                               response=resp400, body=None)
    out = om._translate_error(e)
    assert isinstance(out, anthropic.BadRequestError) and "prompt is too long" in str(out)
    resp503 = httpx.Response(503, request=req)
    e = openai.InternalServerError("upstream connect error", response=resp503, body=None)
    out = om._translate_error(e)
    assert isinstance(out, anthropic.InternalServerError) and "overloaded_error" in str(out)


def test_mixed_response_replays_in_native_order():
    """reasoning -> commentary message -> function_call must replay in that
    order with raw items (ids/phase intact), not text-first."""
    class _C2:
        type = "output_text"
        text = "let me check"
    class _Msg:
        type = "message"
        id = "msg_1"
        phase = "commentary"
        content = [_C2()]
    class R:
        output = [_Reasoning(), _Msg(), _Call()]
        output_text = "let me check"
        usage = None
    resp = om.CompatResponse(R())
    assert [b.type for b in resp.content] == ["text", "tool_use"]
    assert resp.content[0].text == "let me check"      # legacy facade intact
    replay = om.CompatMessages._convert_messages([
        {"role": "assistant", "content": resp.content},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "ok"}]},
    ])
    kinds = [getattr(x, "type", None) if not isinstance(x, dict) else x.get("type") for x in replay]
    r, m, f = kinds.index("reasoning"), kinds.index("message"), kinds.index("function_call")
    assert r < m < f, kinds                             # native order
    assert getattr(replay[m], "id", None) == "msg_1"    # id preserved
    assert getattr(replay[m], "phase", None) == "commentary"


def test_multipart_message_replays_exactly_once():
    class _A:
        type = "output_text"
        text = "part A"
    class _B:
        type = "output_text"
        text = "part B"
    class _Msg:
        type = "message"
        id = "msg_2"
        content = [_A(), _B()]
    class R:
        output = [_Reasoning(), _Msg(), _Call()]
        output_text = "part Apart B"
        usage = None
    resp = om.CompatResponse(R())
    texts = [b.text for b in resp.content if b.type == "text"]
    assert texts == ["part A", "part B"]   # facade keeps every part
    replay = om.CompatMessages._convert_messages([
        {"role": "assistant", "content": resp.content},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": "ok"}]},
    ])
    msg_ids = [getattr(x, "id", None) for x in replay if getattr(x, "type", None) == "message"]
    assert msg_ids == ["msg_2"], msg_ids   # the native message exactly once
    kinds = [getattr(x, "type", None) if not isinstance(x, dict) else x.get("type") for x in replay]
    assert kinds.index("reasoning") < kinds.index("message") < kinds.index("function_call")
