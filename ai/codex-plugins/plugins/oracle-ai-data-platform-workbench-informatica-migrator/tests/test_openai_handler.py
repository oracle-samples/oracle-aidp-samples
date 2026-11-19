"""The OpenAI provider path, which is what the Codex build adds.

The engine was Anthropic-only. This build keeps that path and adds OpenAI as
the default, because Codex users have an OpenAI key and the sibling Codex
plugins are OpenAI-based. Both are OPTIONAL: the deterministic compiler needs
no model, so "no key configured" has to stay a working configuration rather
than a failure.

No network here. The client is faked, because what needs testing is the
unwrapping, the caching and the failure modes -- not OpenAI's API.
"""
from __future__ import annotations

import types

import pytest

from infa2aidp import config as cfg
from infa2aidp.handlers import OpenAIHandler, make_llm_handler
from infa2aidp.handlers.codellama_handler import LLMHandler
from infa2aidp.handlers.openai_handler import _strip_code_fence


def _response(content, finish_reason="stop"):
    msg = types.SimpleNamespace(content=content)
    return types.SimpleNamespace(
        choices=[types.SimpleNamespace(message=msg, finish_reason=finish_reason)]
    )


class _FakeClient:
    """Records calls; returns scripted responses."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls: list[dict] = []
        outer = self

        class _Completions:
            def create(self, **kwargs):
                outer.calls.append(kwargs)
                if not outer.responses:
                    raise AssertionError("called more times than scripted")
                return outer.responses.pop(0)

        self.chat = types.SimpleNamespace(completions=_Completions())

    def with_options(self, **_kwargs):
        """The real client returns a configured copy; the double returns
        itself, because what is under test is the call that follows."""
        return self


def _handler(*responses) -> OpenAIHandler:
    h = OpenAIHandler.__new__(OpenAIHandler)
    h.timeout, h.max_retries, h.model = 30, 3, "gpt-4o"
    h._cache = {}
    h._client = _FakeClient(*responses)
    return h


# ── Provider selection ─────────────────────────────────────────────────

def test_the_codex_build_defaults_to_openai(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.setattr(cfg, "OPENAI_API_KEY", "k", raising=False)
    assert cfg.llm_provider() == "openai"


def test_an_explicit_provider_wins_over_whichever_key_is_set(monkeypatch):
    """A machine with both keys exported must not get a provider by accident
    of which one happened to be read first."""
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setattr(cfg, "OPENAI_API_KEY", "k", raising=False)
    assert cfg.llm_provider() == "anthropic"


def test_provider_falls_back_to_whichever_key_exists(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.setattr(cfg, "OPENAI_API_KEY", None, raising=False)
    monkeypatch.setattr(cfg, "ANTHROPIC_API_KEY", "k", raising=False)
    assert cfg.llm_provider() == "anthropic"


def test_the_factory_returns_the_right_handler_per_provider():
    assert isinstance(make_llm_handler("openai"), OpenAIHandler)
    assert isinstance(make_llm_handler("anthropic"), LLMHandler)


def test_an_unknown_provider_is_refused_rather_than_defaulted():
    """Silently falling back would hide a typo in LLM_PROVIDER."""
    with pytest.raises(ValueError, match="Unknown LLM_PROVIDER"):
        make_llm_handler("gemini")


def test_the_factory_still_returns_a_handler_with_no_key_set(monkeypatch):
    """"No key" must mean "rule-based conversion", not "failed migration"."""
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    h = make_llm_handler("openai")
    assert h is not None
    assert h.is_available() is False


# ── Calling ────────────────────────────────────────────────────────────

def test_a_normal_completion_returns_the_code():
    h = _handler(_response("df = df.filter(F.col('A') > 1)"))
    assert h._call("convert this") == "df = df.filter(F.col('A') > 1)"


def test_the_model_and_prompt_reach_the_api():
    h = _handler(_response("x = 1"))
    h._call("PROMPT-TEXT")
    call = h._client.calls[0]
    assert call["model"] == "gpt-4o"
    roles = [m["role"] for m in call["messages"]]
    assert roles == ["system", "user"]
    assert call["messages"][1]["content"] == "PROMPT-TEXT"
    assert "Informatica" in call["messages"][0]["content"]


def test_an_identical_prompt_is_not_billed_twice():
    """A migration converts many near-identical expressions, and the fixer
    re-sends prompts it has already sent."""
    h = _handler(_response("x = 1"))        # one response only
    assert h._call("same") == "x = 1"
    assert h._call("same") == "x = 1"       # served from cache, no second call
    assert len(h._client.calls) == 1


def test_a_fenced_answer_is_unwrapped():
    """A fenced block pasted into a notebook cell is a syntax error, and
    re-prompting costs more than stripping it."""
    h = _handler(_response("```python\ndf = df.distinct()\n```"))
    assert h._call("p") == "df = df.distinct()"


@pytest.mark.parametrize("raw,expected", [
    ("x = 1", "x = 1"),
    ("```\nx = 1\n```", "x = 1"),
    ("```python\nx = 1\n```", "x = 1"),
    ("```py\nx = 1\ny = 2\n```", "x = 1\ny = 2"),
])
def test_fence_stripping_cases(raw, expected):
    assert _strip_code_fence(raw) == expected


# ── Failure modes that must not produce "code" ──────────────────────────

def test_a_refusal_raises_instead_of_returning_empty_code():
    """content=None on a refusal would otherwise propagate as a TypeError
    several frames from the cause -- or worse, become an empty cell."""
    h = _handler(_response(None, finish_reason="content_filter"))
    with pytest.raises(RuntimeError, match="no content"):
        h._call("p")


def test_a_truncated_completion_names_the_reason():
    h = _handler(_response("", finish_reason="length"))
    with pytest.raises(RuntimeError, match="length"):
        h._call("p")


def test_no_choices_is_reported_clearly():
    h = _handler(types.SimpleNamespace(choices=[]))
    with pytest.raises(RuntimeError, match="no choices"):
        h._call("p")


def test_calling_an_unconfigured_handler_says_what_to_do():
    h = OpenAIHandler.__new__(OpenAIHandler)
    h._client, h._cache, h.model = None, {}, "gpt-4o"
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        h._call("p")


def test_the_two_handlers_share_the_interface_the_pipeline_uses():
    """The pipeline only ever calls these two. If they diverge, the factory
    stops being a drop-in swap."""
    for cls in (OpenAIHandler, LLMHandler):
        assert callable(getattr(cls, "is_available", None)), cls
        assert callable(getattr(cls, "_call", None)), cls


# ── The gap this build actually had ────────────────────────────────────
#
# Wiring the factory into migrator.py was not enough. Three sites reached
# past the handler into an Anthropic client, and every one of them broke the
# OpenAI path:
#
#   llm_notebook_generator._call_llm  -- `hasattr(self.llm, '_claude_client')`
#       is False for OpenAIHandler, so it fell through to
#       `self.llm.generate(prompt)`, which did not exist -> AttributeError.
#   llm_validator._call_llm           -- same shape, same break.
#   migrator                          -- logged `llm.claude_model`, which
#       OpenAIHandler does not have -> AttributeError before any conversion.
#
# The fallback was also dead code for the ANTHROPIC handler: it had no
# `generate` either, so had `_claude_client` ever been None the same
# AttributeError would have fired. Both handlers now implement it.

def test_the_notebook_generator_can_drive_the_openai_handler():
    """The break: _call_llm's non-Anthropic branch called a method that did
    not exist."""
    from infa2aidp.generators.llm_notebook_generator import LLMNotebookGenerator

    h = _handler(_response('[{"cell_type": "code", "source": "df = df"}]'))
    gen = LLMNotebookGenerator(h)
    out = gen._call_llm("PROMPT")
    assert "cell_type" in out
    assert h._client.calls, "the generator never reached the OpenAI client"


def test_the_notebook_generator_passes_its_own_system_prompt():
    """Calling generate(prompt) bare would silently use the handler's generic
    expression prompt instead of the notebook-generation one."""
    from infa2aidp.generators.llm_notebook_generator import (
        _LLM_SYSTEM_PROMPT,
        LLMNotebookGenerator,
    )

    h = _handler(_response("[]"))
    LLMNotebookGenerator(h)._call_llm("PROMPT")
    system = h._client.calls[0]["messages"][0]["content"]
    assert system == _LLM_SYSTEM_PROMPT


def test_the_validator_can_drive_the_openai_handler():
    from infa2aidp.generators.llm_validator import (
        LLMMigrationValidator,
        _VALIDATOR_SYSTEM_PROMPT,
    )

    h = _handler(_response('{"verdict": "ok"}'))
    v = LLMMigrationValidator(h)
    out = v._call_llm("PROMPT")
    assert "verdict" in out
    assert h._client.calls[0]["messages"][0]["content"] == _VALIDATOR_SYSTEM_PROMPT


def test_generate_is_not_cached_so_a_retry_re_asks():
    """The retry loop above generate() exists because an answer never
    arrived. Serving a cached one would defeat it."""
    h = _handler(_response("first"), _response("second"))
    assert h.generate("same prompt") == "first"
    assert h.generate("same prompt") == "second"
    assert len(h._client.calls) == 2


def test_generate_forwards_max_tokens():
    h = _handler(_response("x"))
    h.generate("p", max_tokens=32000)
    assert h._client.calls[0]["max_completion_tokens"] == 32000


def test_an_unconfigured_handler_refuses_generate_too():
    h = OpenAIHandler.__new__(OpenAIHandler)
    h._client, h._cache, h.model = None, {}, "gpt-4o"
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        h.generate("p")


def test_both_handlers_expose_generate():
    """The notebook generator and validator both fall back to it, so a
    handler without it is a handler that breaks the LLM path."""
    for cls in (OpenAIHandler, LLMHandler):
        assert callable(getattr(cls, "generate", None)), cls
