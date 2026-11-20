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
    assert resp.usage.input_tokens == 10
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
