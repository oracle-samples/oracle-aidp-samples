"""The LLM system prompt must teach generic Informatica practice and must
not paraphrase the compiler.
"""
from __future__ import annotations

from infa2aidp.generators.llm_notebook_generator import _LLM_SYSTEM_PROMPT


def test_prompt_still_covers_generic_scd2():
    # SCD2 is standard Informatica practice and the prompt is expected to
    # teach it.
    low = _LLM_SYSTEM_PROMPT.lower()
    assert "scd" in low or "slowly changing" in low


def test_high_date_survives_only_as_a_generic_parameter_example():
    # HIGH_DATE is a conventional SCD2 effective-to sentinel. If the prompt
    # mentions it at all, it must be as a mapping parameter, not as a
    # hardcoded value the generator is taught to emit.
    if "HIGH_DATE" in _LLM_SYSTEM_PROMPT:
        assert "$$HIGH_DATE" in _LLM_SYSTEM_PROMPT, \
            "HIGH_DATE must appear only as a $$-parameter example"
