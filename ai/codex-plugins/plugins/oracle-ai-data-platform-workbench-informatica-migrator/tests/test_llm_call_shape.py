"""Opus 5 runs adaptive thinking by default, so content[0] is a thinking block."""
from unittest.mock import MagicMock
import pytest


def _blocks(*types):
    out = []
    for t in types:
        b = MagicMock()
        b.type = t
        b.text = f"<{t}>" if t == "text" else None
        out.append(b)
    return out


def test_text_is_extracted_even_when_a_thinking_block_comes_first():
    from infa2aidp.generators.llm_notebook_generator import _extract_text
    msg = MagicMock()
    msg.content = _blocks("thinking", "text")
    assert _extract_text(msg) == "<text>"


def test_text_only_response_still_works():
    from infa2aidp.generators.llm_notebook_generator import _extract_text
    msg = MagicMock()
    msg.content = _blocks("text")
    assert _extract_text(msg) == "<text>"


def test_no_text_block_raises_rather_than_returning_none():
    """Silent empty output is the failure mode this project forbids."""
    from infa2aidp.generators.llm_notebook_generator import _extract_text
    msg = MagicMock()
    msg.content = _blocks("thinking")
    with pytest.raises(Exception):
        _extract_text(msg)


# ---------------------------------------------------------------------------
# The other two direct Claude call sites (found live during development, not
# named in the original brief's file list): llm_validator.py and
# codellama_handler.py both did the same bare ``message.content[0].text``
# read. Confirmed against the real API with CLAUDE_MODEL=claude-opus-5: the
# validator call broke with "'ThinkingBlock' object has no attribute
# 'text'", silently collapsing every score to 0 (caught by validate()'s
# broad except). Both now route through the same _extract_text(). These
# mocks are deliberately "thinking"-block-first, which none of the existing
# fakes for these two call sites exercised before (they always returned a
# text-only block), so nothing previously would have caught this.
# ---------------------------------------------------------------------------


def test_validator_call_llm_survives_a_thinking_block_first_response():
    from infa2aidp.generators.llm_validator import LLMMigrationValidator

    class _FakeMessages:
        def create(self, **_kwargs):
            msg = MagicMock()
            msg.content = _blocks("thinking", "text")
            return msg

    class _FakeClient:
        messages = _FakeMessages()

    llm = MagicMock()
    llm._claude_client = _FakeClient()
    llm.claude_model = "claude-opus-5"

    validator = LLMMigrationValidator(llm)
    assert validator._call_llm("prompt") == "<text>"


def test_codellama_handler_call_claude_survives_a_thinking_block_first_response():
    from infa2aidp.handlers.codellama_handler import LLMHandler

    class _FakeMessages:
        def create(self, **_kwargs):
            msg = MagicMock()
            msg.content = _blocks("thinking", "text")
            return msg

    class _FakeClient:
        messages = _FakeMessages()

    # Bypass __init__ (which would try to read ANTHROPIC_API_KEY and build
    # a real anthropic.Anthropic client) -- _call_claude only touches
    # _claude_client and claude_model.
    handler = LLMHandler.__new__(LLMHandler)
    handler._claude_client = _FakeClient()
    handler.claude_model = "claude-opus-5"

    assert handler._call_claude("prompt") == "<text>"
