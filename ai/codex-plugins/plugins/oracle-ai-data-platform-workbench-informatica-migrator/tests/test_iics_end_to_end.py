"""IICS/IDMC JSON must be migratable end-to-end, not only via the batch path.

Spec. The IICS parser has always worked; every CLI entry point globbed
*.xml only and constructed InformaticaXMLParser directly, so JSON was
unreachable - and failed silently rather than loudly.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

IICS_MAPPING = {
    "name": "m_iics_customers",
    "description": "IICS CDI mapping fixture",
    "transformations": [
        {"name": "SRC_CUST", "type": "SOURCE",
         "fields": [{"name": "CUST_ID", "datatype": "integer"},
                    {"name": "CUST_NAME", "datatype": "string"}]},
        {"name": "TGT_CUST", "type": "TARGET",
         "fields": [{"name": "CUST_ID", "datatype": "integer"},
                    {"name": "CUST_NAME", "datatype": "string"}]},
    ],
    "connections": [
        {"from": {"transformation": "SRC_CUST", "field": "CUST_ID"},
         "to": {"transformation": "TGT_CUST", "field": "CUST_ID"}},
    ],
}


@pytest.fixture
def iics_file(tmp_path) -> Path:
    p = tmp_path / "m_iics_customers.json"
    p.write_text(json.dumps(IICS_MAPPING), encoding="utf-8")
    return p


# ── Fix round 1: an IICS Expression field with no portType key must not
# be silently dropped by the converter (spec section 11 -- a hollow
# notebook that "exits 0" is a silent approximation, not success). ──

IICS_MAPPING_WITH_EXPRESSION = {
    "name": "m_iics_expr",
    "description": "IICS mapping with an Expression transform whose output "
                    "field omits portType, as real IICS exports commonly do",
    "transformations": [
        {"name": "SRC_CUST", "type": "SOURCE",
         "fields": [{"name": "CUST_NAME", "datatype": "string"}]},
        {"name": "EXP_UPPER", "type": "EXPRESSION",
         "fields": [
             {"name": "IN_NAME", "portType": "INPUT", "datatype": "string"},
             # No portType key on this one -- this is exactly the shape
             # that used to default to INPUT and get silently dropped.
             {"name": "OUT_NAME", "datatype": "string", "expression": "UPPER(IN_NAME)"},
         ]},
        {"name": "TGT_CUST", "type": "TARGET",
         "fields": [{"name": "OUT_NAME", "datatype": "string"}]},
    ],
    "connections": [
        {"from": {"transformation": "SRC_CUST", "field": "CUST_NAME"},
         "to": {"transformation": "EXP_UPPER", "field": "IN_NAME"}},
        {"from": {"transformation": "EXP_UPPER", "field": "OUT_NAME"},
         "to": {"transformation": "TGT_CUST", "field": "OUT_NAME"}},
    ],
}


@pytest.fixture
def iics_expression_file(tmp_path) -> Path:
    p = tmp_path / "m_iics_expr.json"
    p.write_text(json.dumps(IICS_MAPPING_WITH_EXPRESSION), encoding="utf-8")
    return p


def test_collect_input_files_finds_json(tmp_path, iics_file):
    from infa2aidp.cli import _collect_input_files
    found = _collect_input_files(str(tmp_path))
    assert str(iics_file) in found, "a .json IICS export must be collected"


def test_serial_run_migration_produces_a_notebook_from_json(tmp_path, iics_file):
    from infa2aidp.migrator import run_migration
    out = tmp_path / "out"
    result = run_migration([str(iics_file)], str(out), use_llm=False)
    notebooks = list(out.rglob("*.ipynb"))
    assert notebooks, f"no notebook generated from IICS JSON; result={result}"


def test_run_migration_does_not_silently_succeed_on_an_unparseable_file(tmp_path):
    """Zero notebooks must not look like success - spec section 11."""
    from infa2aidp.migrator import run_migration
    bad = tmp_path / "broken.json"
    bad.write_text("{not valid json", encoding="utf-8")
    out = tmp_path / "out"
    with pytest.raises(Exception):
        run_migration([str(bad)], str(out), use_llm=False)


def test_run_migration_partial_failure_does_not_raise(tmp_path, iics_file):
    """A mix of one good and one unparseable input must NOT raise - it
    stays a warning plus an accurate (lower) notebook count in the result.
    """
    from infa2aidp.migrator import run_migration
    bad = tmp_path / "broken.json"
    bad.write_text("{not valid json", encoding="utf-8")
    out = tmp_path / "out"
    result = run_migration([str(iics_file), str(bad)], str(out), use_llm=False)
    assert result.notebooks == 1
    notebooks = list(out.rglob("*.ipynb"))
    assert len(notebooks) == 1


def test_end_to_end_expression_is_actually_converted_not_just_a_comment(
    tmp_path, iics_expression_file
):
    """The notebook existing is not enough -- it must contain the converted
    expression, not just the '# Expression: <name>' header comment. Before
    an earlier parser default, this notebook generated successfully
    (exit 0, file on disk) with the UPPER(IN_NAME) logic silently gone."""
    from infa2aidp.migrator import run_migration

    out = tmp_path / "out"
    result = run_migration([str(iics_expression_file)], str(out), use_llm=False)

    notebooks = list(out.rglob("nb_m_iics_expr.ipynb"))
    assert notebooks, f"no notebook generated from IICS JSON; result={result}"

    code = notebooks[0].read_text(encoding="utf-8")
    assert "F.upper" in code or "withColumn" in code, (
        "expression was silently dropped -- notebook only has the "
        "'# Expression: EXP_UPPER' header comment:\n" + code
    )


def test_analyze_finds_json_inputs(tmp_path, iics_file):
    """cli._cmd_analyze must not report 'No XML files found' and exit 0 on
    a directory that only contains IICS/IDMC JSON exports."""
    import argparse
    from infa2aidp.cli import _cmd_analyze

    out = tmp_path / "analysis"
    args = argparse.Namespace(
        input=str(tmp_path), output=str(out), format="json",
    )
    exit_code = _cmd_analyze(args)
    assert exit_code == 0
    assert (out / "analysis_report.json").is_file()
    report = json.loads((out / "analysis_report.json").read_text(encoding="utf-8"))
    assert report["inventory"]["total_mappings"] == 1
