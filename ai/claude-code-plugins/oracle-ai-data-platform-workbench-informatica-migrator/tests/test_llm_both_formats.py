"""Prove the LLM generation path works on IICS/IDMC JSON,
not just PowerCenter XML.

``LLMNotebookGenerator`` consumes the IR (a ``Mapping``) via
``_mapping_to_spec()``, never the raw export, and the 927-line system
prompt names zero PowerCenter-specific terms (TRANSFORMFIELD, PORTTYPE,
POWERMART, TABLEATTRIBUTE, pmrep -- see test_llm_prompt.py's neighbor
checks) -- so the generator should already be format-agnostic. This test
proves it by driving the real ``.generate()`` code path -- prompt
construction, LLM call, response parsing, notebook assembly, and
validation -- off an IICS JSON fixture, with the Anthropic client mocked
(never a real API call).

If any of this turned out NOT to work on JSON -- e.g. a crash while
building the prompt, or a spec shape the JSON parser can't produce -- that
would be the finding to report, not something to route around. It didn't:
the assertions below pass on the real generator code, unmodified for this
JSON-support fix (none was needed, which confirmed the premise).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from infa2aidp.generators.llm_notebook_generator import LLMNotebookGenerator
from infa2aidp.models import Session
from infa2aidp.parsers.iics_parser import IICSParser

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "idmc"


class _FakeStream:
    """Stands in for the context manager returned by ``messages.stream()``
    -- the generator's ``_call_llm`` does
    ``with ... .messages.stream(...) as stream: stream.get_final_message()``
    (Opus 5 /: max_tokens defaults above the non-streaming ceiling,
    so the real code streams)."""

    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def get_final_message(self):
        return self._message


class _FakeMessages:
    """Stands in for ``anthropic.Anthropic().messages`` -- records every
    call so the test can inspect exactly what prompt was sent, and returns
    canned text instead of making a real API call.

    Both ``create`` (used by the validator) and ``stream`` (used by the
    generator, as of this release) draw from the same call log / response queue,
    since generate() drives both in a single run."""

    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def _next_message(self, kwargs):
        self.calls.append(kwargs)
        text = self._responses.pop(0) if self._responses else self._responses[-1]
        # type="text" matters: _extract_text() skips any block whose type
        # isn't "text" (that's the whole point of -- Opus 5 leads
        # with a "thinking" block by default).
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])

    def create(self, **kwargs):
        return self._next_message(kwargs)

    def stream(self, **kwargs):
        return _FakeStream(self._next_message(kwargs))


class _FakeClaudeClient:
    """``with_options(...)`` (used by the generator) and a bare
    ``.messages`` (used by the validator) must both work and share the
    same call log / response queue, since generate() drives both in a
    single run."""

    def __init__(self, responses: list[str]):
        self.messages = _FakeMessages(responses)

    def with_options(self, **_kwargs):
        return self


def _fake_llm_handler(responses: list[str]):
    return SimpleNamespace(
        _claude_client=_FakeClaudeClient(responses),
        claude_model="fake-claude-for-tests",
    )


def _notebook_response(mapping_name: str, columns: list[str]) -> str:
    """A minimal but well-formed JSON-array-of-cells response, standing in
    for what a real Claude call would return."""
    cells = [
        {"cell_type": "markdown", "source": f"# {mapping_name}\nMigrated mapping."},
        {"cell_type": "code", "source": "from pyspark.sql import functions as F"},
    ]
    for col in columns:
        cells.append({
            "cell_type": "code",
            "source": f'df = df.withColumn("{col}", F.lit(None))',
        })
    cells.append({"cell_type": "code", "source": "df.write.format('delta').mode('append').saveAsTable('t')"})
    return json.dumps(cells)


_VALIDATION_RESPONSE_HIGH_SCORE = json.dumps({
    "score": 95,
    "critical_issues": [],
    "warnings": [],
    "info": [],
    "correct_steps": ["all steps converted"],
})


def _load_mapping(fixture_name: str):
    parsed = IICSParser().parse_file(str(FIXTURES / fixture_name))
    assert parsed.mappings, f"fixture {fixture_name} produced no mappings"
    return parsed.mappings[0]


def test_llm_path_generates_from_json_fixture_star_schema_fact():
    """Full round trip on tests/fixtures/idmc/star_schema_fact.json: the
    prompt sent to the LLM must contain this JSON mapping's real
    transformations, and the response must come back parsed and
    assembled into a notebook."""
    mapping = _load_mapping("star_schema_fact.json")
    session = Session(name=f"s_{mapping.name}")

    # This mapping (see the fixture) has an Aggregator (AGG_REVENUE) whose
    # TOTAL_REVENUE/SALE_COUNT output ports carry real expressions, and a
    # Lookup (LKP_PRODUCT) -- exactly the "transformations" content Step 1
    # asks the test to find in the outgoing prompt.
    llm = _fake_llm_handler([
        _notebook_response(mapping.name, ["TOTAL_REVENUE", "SALE_COUNT", "CATEGORY"]),
        _VALIDATION_RESPONSE_HIGH_SCORE,
    ])
    generator = LLMNotebookGenerator(llm)

    notebook = generator.generate(mapping, session, output_format="ipynb")

    # --- the prompt sent to the LLM contains the JSON mapping's transformations ---
    assert llm._claude_client.messages.calls, "generator never called the (mocked) LLM"
    gen_call = llm._claude_client.messages.calls[0]
    sent_prompt = gen_call["messages"][0]["content"]

    for construct in ("AGG_REVENUE", "LKP_PRODUCT", "SRC_SALES", "TGT_REVENUE_FACT"):
        assert construct in sent_prompt, (
            f"prompt sent to the LLM is missing transformation {construct!r} "
            "from the JSON mapping"
        )
    # The Aggregator's actual expression text must have made it into the
    # spec embedded in the prompt -- not just the transformation names.
    assert "SUM(QUANTITY * UNIT_PRICE)" in sent_prompt
    assert "COUNT(*)" in sent_prompt

    # --- the response is parsed and assembled into a real notebook ---
    assert notebook is not None, "LLM generation path returned nothing for a JSON fixture"
    nb = json.loads(notebook)
    assert nb["nbformat"] == 4
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    assert code_cells, "assembled notebook has no code cells"
    joined = "\n".join("".join(c["source"]) for c in code_cells)
    assert "TOTAL_REVENUE" in joined
    assert "SALE_COUNT" in joined

    # The validator call (second LLM call) also happened, off the same spec.
    assert len(llm._claude_client.messages.calls) == 2
    assert generator._last_score == 95
    assert generator._last_review_required is False


def test_llm_path_generates_from_json_fixture_incremental_cdc():
    """A second, independent JSON fixture -- breadth, not just one lucky
    shape. incremental_cdc.json has an Expression transformation
    (EXP_ENRICH) with two computed output ports."""
    mapping = _load_mapping("incremental_cdc.json")
    session = Session(name=f"s_{mapping.name}")

    llm = _fake_llm_handler([
        _notebook_response(mapping.name, ["AMOUNT_WITH_TAX", "TIER"]),
        _VALIDATION_RESPONSE_HIGH_SCORE,
    ])
    generator = LLMNotebookGenerator(llm)

    notebook = generator.generate(mapping, session, output_format="ipynb")

    sent_prompt = llm._claude_client.messages.calls[0]["messages"][0]["content"]
    assert "EXP_ENRICH" in sent_prompt
    assert "TOTAL_AMOUNT * 1.1" in sent_prompt
    assert "IIF(TOTAL_AMOUNT > 1000, 'PREMIUM', 'STANDARD')" in sent_prompt

    assert notebook is not None
    nb = json.loads(notebook)
    assert nb["cells"], "assembled notebook has no cells"


def test_prompt_names_the_detected_source_format_for_a_json_mapping():
    """Step 3: the prompt must say which format this mapping came from.
    For an IICS-parsed mapping, that must read IICS/IDMC, not PowerCenter."""
    mapping = _load_mapping("star_schema_fact.json")
    session = Session(name=f"s_{mapping.name}")
    llm = _fake_llm_handler([
        _notebook_response(mapping.name, ["TOTAL_REVENUE"]),
        _VALIDATION_RESPONSE_HIGH_SCORE,
    ])
    generator = LLMNotebookGenerator(llm)
    generator.generate(mapping, session, output_format="ipynb")

    sent_prompt = llm._claude_client.messages.calls[0]["messages"][0]["content"]
    assert "## Source Format" in sent_prompt
    assert "IICS/IDMC" in sent_prompt
