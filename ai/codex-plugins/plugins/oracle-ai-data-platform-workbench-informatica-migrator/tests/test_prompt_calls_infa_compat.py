"""The LLM system prompt must CALL infa_compat's
tested Class C functions -- sequence generator, lookup, $$PARAM/
$$$SessStartTime resolution, SCD Type-2, update strategy -- instead of
teaching the model to hand-write that behaviour as prose PySpark.

Four things are asserted here, each corresponding to a step in the task
brief:

1. The prompt's "CLASS C SEMANTICS" cheat sheet names the REAL parameter
   names of every infa_compat function it tells the model to call --
   imported live via ``inspect.signature``, never retyped from memory, so
   a future rename of a real parameter breaks this test instead of
   silently drifting out of sync with a stale prompt.
2. The prose this task replaces (hand-rolled MERGE/counter/spark.conf.get
   code for these constructs) is actually gone from the prompt, not just
   supplemented.
3. Every LLM-generated notebook deterministically imports infa_compat and
   asserts its version in the header, per the design version-skew
   mitigation -- verified both via the unit-level assembly helper and via
   a full ``generate()`` round trip with the Anthropic client mocked (no
   real API call).
4. ``hallucination_detector`` flags a call to a nonexistent infa_compat
   function the same way it already flags a fabricated Spark function,
   and leaves a real call alone.
"""
from __future__ import annotations

import inspect
import json
import re
from types import SimpleNamespace

import pytest

import infa_compat
from infa2aidp.agents.hallucination_detector import (
    INFA_COMPAT_VALID_NAMES,
    HallucinationDetector,
)
from infa2aidp.agents.models import ConversionAttempt
from infa2aidp.generators.llm_notebook_generator import (
    _LLM_SYSTEM_PROMPT,
    LLMNotebookGenerator,
)
from infa2aidp.models import Mapping, Session, SourceDefinition, TargetDefinition

# ---------------------------------------------------------------------------
# 1. The prompt's cheat sheet must name every real parameter -- imported,
#    never retyped from memory.
# ---------------------------------------------------------------------------

_CHEAT_SHEET_ANCHOR = "CLASS C SEMANTICS — CALL infa_compat"
_CHEAT_SHEET_END_ANCHOR = "DATA FLOW:"

# Every infa_compat function the prompt's cheat sheet tells the model to
# call. Kept as a plain list here (not derived from infa_compat.__all__)
# because not every public name (e.g. the exception classes, the backend
# classes) is something the prompt should be teaching a direct call site
# for -- these nine are the ones table names.
FUNCTIONS_IN_CHEAT_SHEET = [
    "sequence",
    "cached_lookup",
    "load_parameter_file",
    "activate",
    "param",
    "sess_start_time",
    "scd2_merge",
    "apply_update_strategy",
    "write_update_strategy",
]


def _cheat_sheet_block() -> str:
    start = _LLM_SYSTEM_PROMPT.index(_CHEAT_SHEET_ANCHOR)
    end = _LLM_SYSTEM_PROMPT.index(_CHEAT_SHEET_END_ANCHOR, start)
    return _LLM_SYSTEM_PROMPT[start:end]


def test_cheat_sheet_exists_exactly_once():
    assert _LLM_SYSTEM_PROMPT.count(_CHEAT_SHEET_ANCHOR) == 1


@pytest.mark.parametrize("fn_name", FUNCTIONS_IN_CHEAT_SHEET)
def test_cheat_sheet_names_every_real_parameter(fn_name):
    """Import the real function and assert every one of its actual
    parameter names (per ``inspect.signature`` against the INSTALLED
    infa_compat, not a hand-typed copy) appears in the prompt's cheat
    sheet. A parameter rename in infa_compat that isn't mirrored in the
    prompt fails here, rather than the prompt silently teaching a stale
    call."""
    block = _cheat_sheet_block()
    fn = getattr(infa_compat, fn_name)
    sig = inspect.signature(fn)

    assert re.search(rf"infa_compat\.{fn_name}\(", block), (
        f"cheat sheet doesn't call infa_compat.{fn_name}(...) at all"
    )

    for pname in sig.parameters:
        assert re.search(rf"\b{re.escape(pname)}\b", block), (
            f"infa_compat.{fn_name}'s real parameter {pname!r} (from the "
            f"installed library, via inspect.signature) is missing from "
            f"the prompt's Class C cheat sheet -- signature drift"
        )


def test_cheat_sheet_functions_are_all_in_the_closed_api():
    """Sanity check on the test itself: every function named in the cheat
    sheet must actually be part of infa_compat's public, closed API."""
    for fn_name in FUNCTIONS_IN_CHEAT_SHEET:
        assert fn_name in INFA_COMPAT_VALID_NAMES
        assert callable(getattr(infa_compat, fn_name))


# ---------------------------------------------------------------------------
# 2. The old hand-rolled prose this task replaces must actually be gone.
# ---------------------------------------------------------------------------

_REMOVED_PROSE_SNIPPETS = [
    # Old surrogate-key generation via monotonically_increasing_id()/
    # row_number() instead of infa_compat.sequence().
    'F.lit(_max_sk) + F.monotonically_increasing_id() + 1',
    "_w = Window.orderBy(F.lit(1))",
    '(F.lit(_max_sk) + F.row_number().over(_w))',
    # Old hand-rolled SCD2 close-out MERGE (now infa_compat.scd2_merge).
    'tgt = DeltaTable.forName(spark, TGT_TBL)',
    # Old hand-rolled DD_DELETE MERGE (now write_update_strategy()).
    'delta_table = DeltaTable.forName(spark, "THE_TARGET_TABLE_FROM_CONNECTORS")',
    'delta_table.alias("t").merge(',
    # Old $$PARAM / $$HIGH_DATE / $$LOW_DATE_MODE resolution via spark.conf.get().
    'spark.conf.get("migration.param_name", "default")',
    'spark.conf.get("migration.HIGH_DATE", "9999-12-31")',
    'spark.conf.get("migration.LOW_DATE_MODE", "N")',
    'spark.conf.get("migration.LAST_EXTRACT_DATE")',
    # Old per-row SESSSTARTTIME capture via datetime.now() instead of
    # infa_compat.sess_start_time().
    '_SESS_START_TS = datetime.now(timezone.utc)',
]


@pytest.mark.parametrize("snippet", _REMOVED_PROSE_SNIPPETS)
def test_removed_prose_snippet_is_gone(snippet):
    assert snippet not in _LLM_SYSTEM_PROMPT, (
        f"prompt still contains the hand-rolled snippet {snippet!r} that "
        f"the prompt should teach an infa_compat call instead"
    )


def test_no_spark_conf_get_call_survives_for_a_dollar_param():
    """Broader sweep: no ``spark.conf.get(`` call should remain anywhere in
    the prompt -- every mention left is inside a "do NOT do this" warning,
    never inside example code the model is told to emit."""
    for line in _LLM_SYSTEM_PROMPT.splitlines():
        if "spark.conf.get(" in line:
            lowered = line.lower()
            assert "never" in lowered or "not" in lowered or "no " in lowered, (
                f"line still teaches spark.conf.get() as the way to resolve "
                f"a $$PARAM, instead of infa_compat.param(): {line!r}"
            )


def test_class_a_guidance_survives():
    """Spec S7: Class A (pure scalar expressions) legitimately stays prose
    -- this task must not have deleted it along with Class C."""
    assert "IIF" in _LLM_SYSTEM_PROMPT
    assert "DECODE" in _LLM_SYSTEM_PROMPT
    assert "F.add_months()" in _LLM_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# 3. Generated notebooks import infa_compat and pin its version.
# ---------------------------------------------------------------------------


def test_ensure_infa_compat_header_inserts_import_and_version_pin():
    cells = [
        {"cell_type": "markdown", "source": "# Some Mapping"},
        {"cell_type": "code", "source": "from pyspark.sql import functions as F"},
    ]
    new_cells = LLMNotebookGenerator._ensure_infa_compat_header(cells)

    assert len(new_cells) == len(cells) + 1
    pin_cell = new_cells[1]
    assert pin_cell["cell_type"] == "code"
    assert "import infa_compat" in pin_cell["source"]
    assert repr(infa_compat.__version__) in pin_cell["source"]
    assert "infa_compat.__version__" in pin_cell["source"]
    assert "raise RuntimeError" in pin_cell["source"]

    # The pin cell must itself compile as valid Python.
    compile(pin_cell["source"], "<pin_cell>", "exec")


def test_ensure_infa_compat_header_survives_no_markdown_first_cell():
    cells = [{"cell_type": "code", "source": "x = 1"}]
    new_cells = LLMNotebookGenerator._ensure_infa_compat_header(cells)
    assert new_cells[0]["cell_type"] == "code"
    assert "import infa_compat" in new_cells[0]["source"]
    assert new_cells[1]["source"] == "x = 1"


# --- Full round trip through generate(), Anthropic client mocked --------


class _FakeStream:
    def __init__(self, message):
        self._message = message

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def get_final_message(self):
        return self._message


class _FakeMessages:
    def __init__(self, responses: list[str]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def _next_message(self, kwargs):
        self.calls.append(kwargs)
        text = self._responses.pop(0) if self._responses else self._responses[-1]
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])

    def create(self, **kwargs):
        return self._next_message(kwargs)

    def stream(self, **kwargs):
        return _FakeStream(self._next_message(kwargs))


class _FakeClaudeClient:
    def __init__(self, responses: list[str]):
        self.messages = _FakeMessages(responses)

    def with_options(self, **_kwargs):
        return self


def _fake_llm_handler(responses: list[str]):
    return SimpleNamespace(
        _claude_client=_FakeClaudeClient(responses),
        claude_model="fake-claude-for-tests",
    )


def _simple_mapping() -> Mapping:
    return Mapping(
        name="m_simple",
        description="minimal fixture mapping",
        sources=[SourceDefinition(name="SRC_CUST", table_name="CUST",
                                   fields=[{"name": "CUST_ID", "datatype": "integer"}])],
        targets=[TargetDefinition(name="TGT_CUST", table_name="CUST",
                                   fields=[{"target_field": "CUST_ID", "datatype": "integer"}])],
        transformations=[],
        connectors=[],
    )


_VALIDATION_RESPONSE_HIGH_SCORE = json.dumps({
    "score": 95, "critical_issues": [], "warnings": [], "info": [],
    "correct_steps": ["all steps converted"],
})


def _notebook_response() -> str:
    cells = [
        {"cell_type": "markdown", "source": "# m_simple\nMigrated mapping."},
        {"cell_type": "code", "source": "from pyspark.sql import functions as F"},
        {"cell_type": "code", "source": "df = spark.table('catalog.schema.CUST')"},
    ]
    return json.dumps(cells)


def test_generate_round_trip_pins_infa_compat_version_in_the_notebook():
    mapping = _simple_mapping()
    session = Session(name="s_m_simple")
    llm = _fake_llm_handler([_notebook_response(), _VALIDATION_RESPONSE_HIGH_SCORE])
    generator = LLMNotebookGenerator(llm)

    notebook = generator.generate(mapping, session, output_format="ipynb")
    assert notebook is not None

    nb = json.loads(notebook)
    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
    joined_first_code_cell = "".join(code_cells[0]["source"])
    assert "import infa_compat" in joined_first_code_cell
    assert repr(infa_compat.__version__) in joined_first_code_cell


# ---------------------------------------------------------------------------
# 4. hallucination_detector polices the closed infa_compat API.
# ---------------------------------------------------------------------------


def test_detector_flags_a_fabricated_infa_compat_call():
    detector = HallucinationDetector()
    attempt = ConversionAttempt(
        generated_code="infa_compat.scd2_reconcile(spark, 'a', 'b')\n"
    )
    report = detector.check(attempt)
    assert not report.is_clean
    descriptions = [i.description for i in report.issues]
    assert any("infa_compat.scd2_reconcile" in d for d in descriptions)
    assert any(i.severity == "HIGH" for i in report.issues)


@pytest.mark.parametrize("fn_name", FUNCTIONS_IN_CHEAT_SHEET)
def test_detector_does_not_flag_a_real_infa_compat_call(fn_name):
    detector = HallucinationDetector()
    attempt = ConversionAttempt(generated_code=f"infa_compat.{fn_name}(spark)\n")
    report = detector.check(attempt)
    infa_compat_issues = [
        i for i in report.issues if f"infa_compat.{fn_name}" in i.description
    ]
    assert not infa_compat_issues, (
        f"real call infa_compat.{fn_name}(...) was flagged as a fabrication: "
        f"{infa_compat_issues}"
    )


def test_valid_names_set_matches_the_live_package():
    """The detector's whitelist must be read live from infa_compat, never a
    hand-typed copy that can drift when the library adds a function."""
    assert INFA_COMPAT_VALID_NAMES == frozenset(infa_compat.__all__)


# ---------------------------------------------------------------------------
# Bonus: the DD_DELETE post-processing safety net must not clobber a
# correct infa_compat.write_update_strategy() call (it would otherwise --
# see the guard added in _fix_dd_delete_cells for why).
# ---------------------------------------------------------------------------


def test_fix_dd_delete_cells_does_not_clobber_a_correct_infa_compat_call():
    mapping = _simple_mapping()
    from infa2aidp.models import Transformation, TransformationType, Connector

    mapping.transformations = [
        Transformation(name="UPD_DEL", type=TransformationType.UPDATE_STRATEGY,
                        update_strategy_expression="DD_DELETE"),
    ]
    mapping.connectors = [Connector(from_instance="UPD_DEL", to_instance="TGT_CUST",
                                     from_field="", to_field="")]

    correct_cell = {
        "cell_type": "code",
        "source": (
            "infa_compat.write_update_strategy(result, target='catalog.schema.CUST', "
            "keys=['CUST_ID'], reject_sink='catalog.schema.CUST_REJECTS')"
        ),
    }
    generator = LLMNotebookGenerator(llm_handler=None)
    fixed = generator._fix_dd_delete_cells([correct_cell], mapping)

    assert fixed == [correct_cell], (
        "a correct infa_compat.write_update_strategy() call for a DD_DELETE "
        "target was rewritten by the legacy hand-rolled-MERGE post-processor"
    )
