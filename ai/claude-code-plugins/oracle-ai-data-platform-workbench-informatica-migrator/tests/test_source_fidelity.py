"""The non-circular source_fidelity check.

The unit tests below exercise ``source_fidelity()`` directly against small,
controlled notebook strings -- proving it counts source constructs and
names the ones missing from the notebook, for both PowerCenter XML and
IICS/IDMC JSON.

The strongest test in this module recreates the actual
historical defect (``PORTTYPE="VARIABLE"`` resolving to ``INPUT``, which
silently dropped every Expression variable port's expression -- see
``tests/test_variable_ports.py``) by monkeypatching the IICS parser's
port-direction resolution back to the pre-fix behavior, generates a
notebook off the resulting (buggy) IR, and proves ``source_fidelity``
flags the dropped expression BY NAME even though it compares against the
untouched raw export -- something a spec-derived validator score cannot
do, because the spec would have been built from the same buggy IR.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.llm_notebook_generator import LLMNotebookGenerator
from infa2aidp.generators.source_fidelity import FidelityReport, source_fidelity
from infa2aidp.models import DataFlowDirection, Session, TransformationField
from infa2aidp.parsers import iics_parser as iics_parser_module
from infa2aidp.parsers.format_detector import detect_and_parse_file
from infa2aidp.parsers.iics_parser import IICSParser

POWERCENTER_DIR = Path(__file__).resolve().parent / "fixtures" / "powercenter"
IDMC_DIR = Path(__file__).resolve().parent / "fixtures" / "idmc"

ALL_FIXTURES = sorted(POWERCENTER_DIR.glob("*.xml")) + sorted(IDMC_DIR.glob("*.json"))


# ---------------------------------------------------------------------------
# Basic unit tests -- controlled notebook text, both formats.
# ---------------------------------------------------------------------------

def test_xml_no_gaps_when_everything_is_represented():
    fixture = POWERCENTER_DIR / "scd_type2.xml"
    # scd_type2.xml has transformations SQ_EMPLOYEES, LKP_CURRENT,
    # EXP_DETECT_CHANGE, with expressions IS_CHANGED and EFF_START.
    complete_code = """
    df = spark.table("HR_DB.STAGING.EMPLOYEES_STAGE")  # SQ_EMPLOYEES
    df = df.select("EMP_ID", "NAME", "DEPT", "SALARY")
    df_lkp = spark.table("...")  # LKP_CURRENT
    df = df.withColumn("LKP_DEPT", F.lit(None))
    df = df.withColumn("LKP_SALARY", F.lit(None))
    # EXP_DETECT_CHANGE
    df = df.withColumn("IS_CHANGED", F.when(...))
    df = df.withColumn("EFF_START", F.current_timestamp())
    df = df.withColumnRenamed("IS_CHANGED", "IS_CURRENT")
    df.write.format("delta").saveAsTable("TGT_EMPLOYEES_HIST")
    """
    report = source_fidelity(str(fixture), complete_code)
    assert isinstance(report, FidelityReport)
    assert report.source_format == "xml"
    assert "SQ_EMPLOYEES" in report.transformations_in_source
    assert "EXP_DETECT_CHANGE.IS_CHANGED" in report.expressions_in_source
    assert report.transformations_missing == []
    assert report.expressions_missing == []
    assert report.has_gaps is False


def test_xml_flags_missing_transformation_and_expression_by_name():
    fixture = POWERCENTER_DIR / "scd_type2.xml"
    # Notebook that never mentions EXP_DETECT_CHANGE at all -- as if the
    # whole transformation, and the IS_CHANGED/EFF_START expressions it
    # carries, were dropped during generation.
    incomplete_code = """
    df = spark.table("HR_DB.STAGING.EMPLOYEES_STAGE")  # SQ_EMPLOYEES
    df_lkp = spark.table("...")  # LKP_CURRENT
    df = df.withColumn("LKP_DEPT", F.lit(None))
    df = df.withColumn("LKP_SALARY", F.lit(None))
    df.write.format("delta").saveAsTable("TGT_EMPLOYEES_HIST")
    """
    report = source_fidelity(str(fixture), incomplete_code)
    assert report.has_gaps is True
    assert "EXP_DETECT_CHANGE" in report.transformations_missing
    assert "EXP_DETECT_CHANGE.IS_CHANGED" in report.expressions_missing
    assert "EXP_DETECT_CHANGE.EFF_START" in report.expressions_missing
    # Named, not just counted.
    assert report.summary().count("EXP_DETECT_CHANGE") >= 1
    assert "gaps are reported, not failed" in " ".join(report.notes)


def test_json_no_gaps_when_everything_is_represented():
    fixture = IDMC_DIR / "incremental_cdc.json"
    complete_code = """
    df = spark.table("ORDERS_STAGE")  # SRC_ORDERS
    df = df.select("ORDER_ID", "TOTAL_AMOUNT", "STATUS")
    df = df.filter(F.col("STATUS") == "ACTIVE")  # FIL_ACTIVE
    # EXP_ENRICH
    df = df.withColumn("AMOUNT_WITH_TAX", F.col("TOTAL_AMOUNT") * 1.1)
    df = df.withColumn("TIER", F.when(F.col("TOTAL_AMOUNT") > 1000, "PREMIUM").otherwise("STANDARD"))
    df.write.format("delta").saveAsTable("TGT_ORDERS_DW")
    """
    report = source_fidelity(str(fixture), complete_code)
    assert report.source_format == "json"
    assert "EXP_ENRICH" in report.transformations_in_source
    assert "EXP_ENRICH.AMOUNT_WITH_TAX" in report.expressions_in_source
    assert "EXP_ENRICH.TIER" in report.expressions_in_source
    assert report.expressions_missing == []
    assert report.has_gaps is False


def test_json_flags_missing_expression_by_name():
    fixture = IDMC_DIR / "incremental_cdc.json"
    # TIER is silently dropped; AMOUNT_WITH_TAX survives.
    incomplete_code = """
    df = spark.table("ORDERS_STAGE")  # SRC_ORDERS
    df = df.filter(F.col("STATUS") == "ACTIVE")  # FIL_ACTIVE
    df = df.withColumn("AMOUNT_WITH_TAX", F.col("TOTAL_AMOUNT") * 1.1)
    df.write.format("delta").saveAsTable("TGT_ORDERS_DW")
    """
    report = source_fidelity(str(fixture), incomplete_code)
    assert report.has_gaps is True
    assert report.expressions_missing == ["EXP_ENRICH.TIER"]
    assert "EXP_ENRICH.AMOUNT_WITH_TAX" not in report.expressions_missing


def test_accepts_full_ipynb_json_string_not_just_bare_code():
    """source_fidelity(source_path, notebook_code) must work with the exact
    string LLMNotebookGenerator._assemble_ipynb() hands back -- a full
    .ipynb JSON document, not just bare code text."""
    fixture = IDMC_DIR / "incremental_cdc.json"
    notebook_json = json.dumps({
        "nbformat": 4,
        "cells": [
            {"cell_type": "markdown", "source": ["# m_incremental_cdc_iics\n"]},
            {"cell_type": "code", "source": ["df = df.withColumn(\"AMOUNT_WITH_TAX\", F.col(\"TOTAL_AMOUNT\") * 1.1)\n"]},
            {"cell_type": "code", "source": ["df = df.withColumn(\"TIER\", F.lit(None))\n"]},
        ],
    })
    report = source_fidelity(str(fixture), notebook_json)
    assert report.expressions_missing == []


def test_unreadable_source_file_raises():
    with pytest.raises(OSError):
        source_fidelity("/no/such/file/here.xml", "df = 1")


def test_malformed_xml_reports_a_note_instead_of_raising(tmp_path):
    bad = tmp_path / "broken.xml"
    bad.write_text("<MAPPING NAME='x'><TRANSFORMATION>", encoding="utf-8")  # unclosed
    report = source_fidelity(str(bad), "df = 1")
    assert report.source_format == "xml"
    assert report.notes
    assert report.has_gaps is False  # never fails a build -- just can't compare


def test_malformed_json_reports_a_note_instead_of_raising(tmp_path):
    bad = tmp_path / "broken.json"
    bad.write_text("{not valid json", encoding="utf-8")
    report = source_fidelity(str(bad), "df = 1")
    assert report.source_format == "json"
    assert report.notes


# ---------------------------------------------------------------------------
# Corpus-wide smoke test: run source_fidelity for all 12 committed fixtures
# against a REAL rule-based notebook (no LLM/network involved) -- proves
# the module handles every committed shape (mapplet-free XML and JSON,
# multiple transformation types, multi-connector fan-out) without raising.
# ---------------------------------------------------------------------------

def _rule_based_notebook_code(fixture_path: Path) -> str:
    """Build real generated code for a fixture via the existing rule-based
    converter -- the same path demo.sh exercises -- so the corpus-wide
    smoke test below compares against genuinely generated code, not a
    hand-written stand-in."""
    parsed = detect_and_parse_file(str(fixture_path))
    assert parsed.mappings, f"{fixture_path} produced no mappings"
    mapping = parsed.mappings[0]
    converter = TransformationConverter()
    parts = []
    for tx in mapping.transformations:
        parts.append(f"# {tx.name}")
        parts.extend(converter.convert(tx, input_df="df"))
    return "\n".join(parts)


@pytest.mark.parametrize("fixture_path", ALL_FIXTURES, ids=lambda p: p.name)
def test_source_fidelity_runs_on_every_committed_fixture(fixture_path):
    code = _rule_based_notebook_code(fixture_path)
    report = source_fidelity(str(fixture_path), code)

    assert isinstance(report, FidelityReport)
    assert report.source_format == ("json" if fixture_path.suffix == ".json" else "xml")
    assert not report.notes or "gaps are reported" in " ".join(report.notes)
    # Every fixture in this corpus has at least one TRANSFORMATION/entry.
    assert report.transformations_in_source, f"{fixture_path} yielded zero transformations"


# ---------------------------------------------------------------------------
# Recreate the historical PORTTYPE=VARIABLE-resolves-to-INPUT bug
# and prove source_fidelity catches it BY NAME.
# ---------------------------------------------------------------------------

_RUNNING_TOTAL_FIXTURE = {
    "name": "m_running_total_iics",
    "description": "Running total via a self-referencing variable port -- "
                    "regression fixture for the historical PORTTYPE=VARIABLE bug.",
    "transformations": [
        {
            "name": "SRC_ORDERS",
            "type": "SOURCE",
            "tableName": "ORDERS_STAGE",
            "fields": [
                {"name": "ORDER_ID", "dataType": "integer", "portType": "OUTPUT"},
                {"name": "AMOUNT", "dataType": "decimal", "portType": "OUTPUT"},
            ],
        },
        {
            "name": "EXP_RUNNING_TOTAL",
            "type": "EXPRESSION",
            "fields": [
                {"name": "ORDER_ID", "dataType": "integer", "portType": "INPUT"},
                {"name": "AMOUNT", "dataType": "decimal", "portType": "INPUT"},
                # The self-referencing running-total accumulator -- exactly
                # the shape the real historical bug silently dropped.
                {
                    "name": "v_run_accum",
                    "dataType": "decimal",
                    "portType": "VARIABLE",
                    "expression": "v_run_accum + AMOUNT",
                },
            ],
        },
        {
            "name": "TGT_ORDERS_RUNNING",
            "type": "TARGET",
            "tableName": "ORDERS_RUNNING",
            "fields": [
                {"name": "ORDER_ID", "dataType": "integer", "portType": "INPUT"},
            ],
        },
    ],
    "connections": [
        {"from": {"transformation": "SRC_ORDERS", "field": "ORDER_ID"},
         "to": {"transformation": "EXP_RUNNING_TOTAL", "field": "ORDER_ID"}},
        {"from": {"transformation": "SRC_ORDERS", "field": "AMOUNT"},
         "to": {"transformation": "EXP_RUNNING_TOTAL", "field": "AMOUNT"}},
        {"from": {"transformation": "EXP_RUNNING_TOTAL", "field": "ORDER_ID"},
         "to": {"transformation": "TGT_ORDERS_RUNNING", "field": "ORDER_ID"}},
    ],
}


def _write_running_total_fixture(tmp_path: Path) -> Path:
    path = tmp_path / "m_running_total_iics.json"
    path.write_text(json.dumps(_RUNNING_TOTAL_FIXTURE, indent=2), encoding="utf-8")
    return path


def _pre_task21_parse_field(self, data: dict) -> TransformationField:
    """Stand-in for IICSParser._parse_field as it existed BEFORE:
    "variable"/"var" isn't in the direction map at all, so it falls through
    to the INPUT default, and there is no belt-and-braces re-promotion of
    an INPUT field that carries an expression back to INPUT_OUTPUT. This
    is a deliberate, monkeypatched regression of the real historical bug
    (see xml_parser._resolve_direction and iics_parser._parse_field for the
    current, fixed behavior and their comments explaining exactly this
    defect) -- used here ONLY to prove source_fidelity would have caught
    it; the shipped parser has carried the fix since then.
    """
    raw_dir = (data.get("portType") or data.get("direction") or "input_output").lower().strip().replace("-", "_")
    pre_fix_dir_map = {
        "input": DataFlowDirection.INPUT,
        "in": DataFlowDirection.INPUT,
        "output": DataFlowDirection.OUTPUT,
        "out": DataFlowDirection.OUTPUT,
        "input_output": DataFlowDirection.INPUT_OUTPUT,
        "input/output": DataFlowDirection.INPUT_OUTPUT,
        "inout": DataFlowDirection.INPUT_OUTPUT,
        # "variable"/"var" deliberately absent -- falls through to INPUT.
    }
    direction = pre_fix_dir_map.get(raw_dir, DataFlowDirection.INPUT)
    return TransformationField(
        name=data.get("name", ""),
        datatype=data.get("dataType") or data.get("datatype") or data.get("type") or "STRING",
        precision=data.get("precision", 0) or 0,
        scale=data.get("scale", 0) or 0,
        expression=data.get("expression", ""),
        direction=direction,
        default_value=data.get("defaultValue", ""),
        description=data.get("description", ""),
        is_master=False,
    )


def test_buggy_direction_resolution_drops_the_expression_entirely(tmp_path, monkeypatch):
    """BEFORE: with the historical bug recreated, the variable port's
    expression must be completely absent from the generated code -- not
    merely wrong."""
    fixture_path = _write_running_total_fixture(tmp_path)

    monkeypatch.setattr(iics_parser_module.IICSParser, "_parse_field", _pre_task21_parse_field)
    buggy_mapping = IICSParser().parse_file(str(fixture_path)).mappings[0]

    exp_tx = next(t for t in buggy_mapping.transformations if t.name == "EXP_RUNNING_TOTAL")
    v_field = next(f for f in exp_tx.fields if f.name == "v_run_accum")
    assert v_field.direction == DataFlowDirection.INPUT, (
        "the monkeypatch didn't recreate the bug -- v_run_accum should have "
        "resolved to INPUT, exactly like the real historical defect"
    )

    buggy_code = "\n".join(TransformationConverter().convert(exp_tx, input_df="df"))
    assert "v_run_accum" not in buggy_code, (
        "the buggy conversion should drop the variable port's expression "
        "entirely, matching the real historical defect"
    )

    report = source_fidelity(str(fixture_path), buggy_code)
    assert "EXP_RUNNING_TOTAL.v_run_accum" in report.expressions_missing, (
        "source_fidelity failed to catch, BY NAME, the exact construct the "
        "historical PORTTYPE=VARIABLE bug silently dropped"
    )


def test_fixed_direction_resolution_is_not_flagged(tmp_path):
    """AFTER: with the real (fixed) parser, the variable port's name still
    appears in the generated code -- as a REVIEW REQUIRED placeholder,
    since the self-referencing running-total translation itself is
    separate M2 work -- so source_fidelity does NOT report it missing.
    This is the contrast: the fixed parser never silently drops the name,
    even though the numeric translation still needs a human."""
    fixture_path = _write_running_total_fixture(tmp_path)

    fixed_mapping = IICSParser().parse_file(str(fixture_path)).mappings[0]
    exp_tx = next(t for t in fixed_mapping.transformations if t.name == "EXP_RUNNING_TOTAL")
    v_field = next(f for f in exp_tx.fields if f.name == "v_run_accum")
    assert v_field.direction == DataFlowDirection.VARIABLE

    fixed_code = "\n".join(TransformationConverter().convert(exp_tx, input_df="df"))
    assert "v_run_accum" in fixed_code
    # the running total is translated now (a window), no longer a placeholder
    assert "Stateful variable ports (v_run_accum)" in fixed_code

    report = source_fidelity(str(fixture_path), fixed_code)
    assert "EXP_RUNNING_TOTAL.v_run_accum" not in report.expressions_missing


def test_fidelity_wired_into_generate_catches_what_a_100_score_could_not(tmp_path, monkeypatch):
    """The centerpiece: drive the actual LLMNotebookGenerator.generate()
    wiring (mocked LLM + mocked validator -- no real API calls) with the
    buggy IR, force the (circular) validator to award a perfect 100/100,
    and prove self._last_fidelity still names the dropped construct."""
    fixture_path = _write_running_total_fixture(tmp_path)

    monkeypatch.setattr(iics_parser_module.IICSParser, "_parse_field", _pre_task21_parse_field)
    buggy_mapping = IICSParser().parse_file(str(fixture_path)).mappings[0]
    session = Session(name=f"s_{buggy_mapping.name}")

    exp_tx = next(t for t in buggy_mapping.transformations if t.name == "EXP_RUNNING_TOTAL")
    buggy_expr_code = "\n".join(TransformationConverter().convert(exp_tx, input_df="df"))
    assert "v_run_accum" not in buggy_expr_code  # sanity: bug is really in the code fed to generate()

    notebook_response = json.dumps([
        {"cell_type": "markdown", "source": f"# {buggy_mapping.name}"},
        {"cell_type": "code", "source": "df = spark.table('ORDERS_STAGE')"},
        {"cell_type": "code", "source": buggy_expr_code},
        {"cell_type": "code", "source": "df.write.format('delta').mode('append').saveAsTable('ORDERS_RUNNING')"},
    ])
    # The circular validator, scoring against a spec built from the SAME
    # buggy IR, says this is perfect -- exactly the failure mode
    # exists to stop being the only signal.
    validation_response = json.dumps({
        "score": 100,
        "critical_issues": [],
        "warnings": [],
        "info": [],
        "correct_steps": ["EXP_RUNNING_TOTAL converted"],
    })

    class _FakeStream:
        """The generator's _call_llm streams as of this release (Opus 5's
        default max_tokens exceeds the non-streaming ceiling)."""

        def __init__(self, message):
            self._message = message

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def get_final_message(self):
            return self._message

    class _FakeMessages:
        def __init__(self, responses):
            self._responses = list(responses)
            self.calls = []

        def _next_message(self, kwargs):
            self.calls.append(kwargs)
            # type="text": _extract_text() only accepts a "text" block --
            # Opus 5 leads with "thinking" by default, which is the exact
            # regression guards against.
            return SimpleNamespace(content=[SimpleNamespace(type="text", text=self._responses.pop(0))])

        def create(self, **kwargs):
            return self._next_message(kwargs)

        def stream(self, **kwargs):
            return _FakeStream(self._next_message(kwargs))

    class _FakeClaudeClient:
        def __init__(self, responses):
            self.messages = _FakeMessages(responses)

        def with_options(self, **_kwargs):
            return self

    llm = SimpleNamespace(
        _claude_client=_FakeClaudeClient([notebook_response, validation_response]),
        claude_model="fake-claude-for-tests",
    )

    generator = LLMNotebookGenerator(llm)
    notebook = generator.generate(
        buggy_mapping, session, output_format="ipynb", source_path=str(fixture_path),
    )

    assert notebook is not None
    # The circular signal: a perfect score, accepted, no review flagged.
    assert generator._last_score == 100
    assert generator._last_review_required is False

    # The independent signal: source_fidelity, run against the untouched
    # raw export, still names the dropped construct.
    fidelity = generator._last_fidelity
    assert fidelity is not None, "generate() did not wire source_fidelity in when source_path was given"
    assert fidelity.has_gaps is True
    assert "EXP_RUNNING_TOTAL.v_run_accum" in fidelity.expressions_missing


# ---------------------------------------------------------------------------
# A construct named only in a comment is not implemented
# ---------------------------------------------------------------------------

def test_an_expression_named_only_in_a_comment_is_still_missing():
    """Found by running the LLM path over the same corpus as the rule-based
    path: it reported 0/12 fidelity gaps against the rule-based 10/12.

    Not better fidelity -- more prose. The check took whole code-cell text,
    so a chatty notebook satisfied it by mentioning names. Fidelity is the
    number this project would quote to a customer, and it rewarded
    verbosity.

    Expression output fields appear in executable code as
    ``withColumn("NET_AMOUNT", ...)``, so they are matched against
    comment-free code.
    """
    fixture = POWERCENTER_DIR / "scd_type2.xml"
    comment_only = """
    df = spark.table("HR_DB.STAGING.EMPLOYEES_STAGE")  # SQ_EMPLOYEES
    df_lkp = spark.table("...")  # LKP_CURRENT
    # EXP_DETECT_CHANGE computes IS_CHANGED and EFF_START
    df.write.format("delta").saveAsTable("TGT_EMPLOYEES_HIST")
    """
    report = source_fidelity(str(fixture), comment_only)
    assert "EXP_DETECT_CHANGE.IS_CHANGED" in report.expressions_missing
    assert "EXP_DETECT_CHANGE.EFF_START" in report.expressions_missing
    assert report.has_gaps is True
    # the transformation names are legitimately comment-only: a
    # transformation has no runtime artifact to look for
    assert report.transformations_missing == []


def test_a_hash_inside_a_string_literal_is_not_a_comment():
    """The strip is tokenize-based, not a regex over ``#``."""
    fixture = POWERCENTER_DIR / "scd_type2.xml"
    code = '''
    df = df.withColumn("IS_CHANGED", F.lit("#not-a-comment"))
    df = df.withColumn("EFF_START", F.current_timestamp())
    '''
    report = source_fidelity(str(fixture), code)
    assert "EXP_DETECT_CHANGE.IS_CHANGED" not in report.expressions_missing
    assert "EXP_DETECT_CHANGE.EFF_START" not in report.expressions_missing


def test_unparseable_code_does_not_invent_gaps():
    """A cell can be a fragment. Over-reporting on untokenizable text would
    be worse than the verbosity this guards against, so the strip is a
    no-op when tokenize fails."""
    fixture = POWERCENTER_DIR / "scd_type2.xml"
    broken = 'df = df.withColumn("IS_CHANGED", F.when(  # unbalanced\n'
    report = source_fidelity(str(fixture), broken)
    assert "EXP_DETECT_CHANGE.IS_CHANGED" not in report.expressions_missing
