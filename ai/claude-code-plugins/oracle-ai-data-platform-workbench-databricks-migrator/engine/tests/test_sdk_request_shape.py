"""The Claude request shape against the INSTALLED anthropic SDK.

Every other test replaces claude() with a stub that accepts any kwarg, so an SDK
that rejects a request field would go unnoticed until a live run. Here the real
client builds each request; only its HTTP post is stubbed. Top-level
`cache_control` (automatic tail caching) exists on Messages.create from
anthropic 0.83.0 and `output_config` (effort) from 0.77.0; older installs raise
TypeError on every tool-loop request.
"""
from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import anthropic
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import agent_migrate as am  # noqa: E402

MIN_SDK = (0, 83, 0)


def _real_client(monkeypatch, replies):
    """A real SDK client whose HTTP post records the built request body."""
    client = anthropic.Anthropic(api_key="test-key", max_retries=0)
    bodies = []

    def post(path, *, body=None, **kwargs):
        bodies.append(body)
        return next(replies)

    # Before `client.messages` is first touched: the resource binds client.post.
    monkeypatch.setattr(client, "post", post)
    monkeypatch.setattr(am, "claude", lambda: client)
    monkeypatch.setattr(am, "USAGE_LEDGER", [])
    monkeypatch.setattr(am, "_USAGE_LOG_PATH", "")
    return bodies


def _reply(text):
    return NS(content=[NS(type="text", text=text)], stop_reason="end_turn",
              usage=NS(input_tokens=1, output_tokens=1))


def test_tool_loop_request_is_accepted_by_the_installed_sdk(monkeypatch):
    bodies = _real_client(monkeypatch, iter([_reply("VERIFIED")]))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    text, _ = asyncio.run(am.call_opus_with_tools("Evaluate", "input"))
    assert text == "VERIFIED"
    body = bodies[0]
    assert body["cache_control"] == {"type": "ephemeral"}               # messages tail
    assert body["system"][-1]["cache_control"] == {"type": "ephemeral", "ttl": "1h"}
    assert not any("cache_control" in tool for tool in body["tools"])   # 1h entry leads


def test_summary_request_is_accepted_by_the_installed_sdk(monkeypatch):
    bodies = _real_client(monkeypatch, iter([_reply("Reads a table.")]))
    assert asyncio.run(am._summarize_cell_code("df = spark.table('t')")) == "Reads a table."
    assert bodies[0]["output_config"] == {"effort": "low"}


@pytest.mark.parametrize("requirements_file", [
    "requirements.txt",
    # What setup_migration_cluster.py installs on the control cluster (#185 review A5).
    "config/default_requirements.txt",
])
def test_declared_sdk_floor_supports_the_request_shape(requirements_file):
    requirements = (ROOT / requirements_file).read_text()
    floor = re.search(r"^anthropic>=([\d.]+)", requirements, re.M)
    assert floor, f"{requirements_file} must declare an anthropic floor"
    assert tuple(int(p) for p in floor.group(1).split(".")) >= MIN_SDK


def test_sdk_floor_is_one_version_everywhere():
    """The local, control-cluster and CI installs must not drift apart: one
    range in both requirement files (a cluster file without the ceiling would
    install an unvalidated 1.x; #185 owner decision 2), the CI pin = the floor."""
    def version(path, pattern):
        return re.search(pattern, (ROOT / path).read_text(), re.M).group(1)

    spec = version("requirements.txt", r"^anthropic([<>=!~][^\s#]*)")
    assert spec == ">=0.125.0,<1.0"
    assert version("config/default_requirements.txt", r"^anthropic([<>=!~][^\s#]*)") == spec
    floor = version("requirements.txt", r"^anthropic>=([\d.]+)")
    assert version("constraints-ci.txt", r"^anthropic==([\d.]+)") == floor


# ── #185 review A9: pins for mutations the suite did not catch ──

def test_effort_reaches_the_tool_loop_request(monkeypatch):
    """M15 (`_effort_kw = {}`): a route's effort must be sent as output_config."""
    bodies = _real_client(monkeypatch, iter([_reply("VERIFIED")]))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    asyncio.run(am.call_opus_with_tools("Evaluate", "input", effort="low"))
    assert bodies[0]["output_config"] == {"effort": "low"}


def test_summary_reads_the_text_block_after_a_thinking_block(monkeypatch):
    """M13 (`content[0].text`): thinking comes first on the Opus-5 family."""
    reply = NS(content=[NS(type="thinking", thinking="", signature="s"),
                        NS(type="text", text="Reads a table.")],
               stop_reason="end_turn", usage=NS(input_tokens=1, output_tokens=1))
    _real_client(monkeypatch, iter([reply]))
    assert asyncio.run(am._summarize_cell_code("df = spark.table('t')")) == "Reads a table."


def test_tool_loop_usage_is_routed_by_prompt_and_billed_to_the_open_window(monkeypatch):
    """M16 (`route or "other"`) and a window check at a real request site: the
    eval prompt is booked as `eval`, inside the caller's window."""
    _real_client(monkeypatch, iter([_reply("PASS: ok")]))
    monkeypatch.setattr(am, "_format_history_context", lambda: "")
    mark = am.open_usage_window()
    try:
        asyncio.run(am.call_opus_with_tools("You are verifying a migrated Spark cell's output",
                                            "input"))
        usage = am.usage_delta(mark)
    finally:
        am.close_usage_window(mark)
    assert usage["requests"] == 1
    assert list(usage["by_route"]) == ["eval"]
