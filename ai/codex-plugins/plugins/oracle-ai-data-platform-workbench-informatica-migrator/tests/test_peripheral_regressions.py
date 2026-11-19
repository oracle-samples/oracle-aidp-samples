"""Peripheral correctness: the tools around the converter.

The first two passes fixed the parser and the generator. This one covers
the periphery that a migration team touches once the notebooks exist --
the generated-code validator, format detection, the reconcile report and
its dashboard, parameter files, the RAG store, package ingestion and the
XXE guard. Each test reproduces a defect confirmed by running the code
before the fix.
"""
from __future__ import annotations

import json
import os
import sys
import types
import zipfile

import pytest

from infa2aidp.agents.models import ConversionSpec
from infa2aidp.agents.rag_store import RAGStore
from infa2aidp.generators.code_validation import unresolved_names
from infa2aidp.generators.dashboard_generator import CELL_SEP, DashboardGenerator
from infa2aidp.parsers import package_extractor
from infa2aidp.parsers.format_detector import detect_and_parse_file
from infa2aidp.parsers.parameter_parser import ParameterFileParser
from infa2aidp.parsers.security import SecurityError, validate_no_xxe
from infa2aidp.reconciler.models import ReconcileResult, SchemaColumnDiff
from infa2aidp.reconciler.report import ReconcileReport

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


# ---------------------------------------------------------------------------
# 1. Generated-code validator: lambda parameters and walrus targets
# ---------------------------------------------------------------------------

class TestUnresolvedNamesScopes:
    """The sequential checker flagged names that Python binds inside the
    expression itself. A notebook that sorted columns with a lambda was
    reported as reading an unassigned variable, and the pipeline's
    'validate before deploy' step rejected working code."""

    def test_lambda_parameter_is_not_a_read_before_assign(self):
        code = "cols = ['B', 'a']\nordered = sorted(cols, key=lambda c: c.upper())\n"
        assert unresolved_names(code) == []

    def test_lambda_star_args_and_defaults(self):
        code = (
            "base = 1\n"
            "f = lambda x, *rest, k=base, **kw: (x, rest, k, kw)\n"
        )
        assert unresolved_names(code) == []

    def test_lambda_body_still_sees_genuinely_missing_names(self):
        code = "f = lambda c: c + missing_var\n"
        assert unresolved_names(code) == ["missing_var"]

    def test_walrus_target_is_bound_for_the_rest_of_the_cell(self):
        code = (
            "rows = [1, 2, 3]\n"
            "if (n := len(rows)) > 2:\n"
            "    print(n)\n"
            "total = n\n"
        )
        assert unresolved_names(code) == []


# ---------------------------------------------------------------------------
# 2. Format detection: UTF-8 BOM on an unknown extension
# ---------------------------------------------------------------------------

class TestFormatDetectionBom:
    def test_bom_prefixed_export_with_unknown_extension_is_detected(self, tmp_path):
        src = os.path.join(FIXTURES, "corpus", "scd_type1.xml")
        with open(src, "rb") as f:
            body = f.read()
        # Exports saved from Windows tools routinely carry a BOM; the
        # extension is often .txt or .exp when the file arrives by mail.
        target = tmp_path / "export_from_customer.txt"
        target.write_bytes(b"\xef\xbb\xbf" + body)

        result = detect_and_parse_file(str(target))

        assert result.mappings, "BOM-prefixed PowerCenter export was not recognised"


# ---------------------------------------------------------------------------
# 3. Reconcile report: no Schema verdict for a check that never ran
# ---------------------------------------------------------------------------

class TestReconcileReportSchemaLine:
    def test_row_count_only_result_has_no_schema_line(self):
        r = ReconcileResult(
            config_name="orders", reconcile_type="row_count", status="PASSED",
            source_row_count=10, target_row_count=10, row_count_match=True,
        )
        md = ReconcileReport([r]).to_markdown()
        assert "Row Count:** PASSED" in md
        assert "Schema:" not in md

    def test_schema_result_still_reports_schema_verdict(self):
        r = ReconcileResult(
            config_name="orders", reconcile_type="schema", status="FAILED",
            schema_diffs=[SchemaColumnDiff("AMT", "NUMBER", "STRING", "TYPE_MISMATCH")],
            schema_match=False,
        )
        md = ReconcileReport([r]).to_markdown()
        assert "Schema:** FAILED" in md
        assert "AMT: TYPE_MISMATCH" in md

    def test_full_run_with_matching_schema_reports_passed(self):
        r = ReconcileResult(config_name="o", reconcile_type="all", status="PASSED",
                            schema_match=True)
        assert "Schema:** PASSED" in ReconcileReport([r]).to_markdown()


# ---------------------------------------------------------------------------
# 4. Parameter files: $$ references and ST:/WF: scope prefixes
# ---------------------------------------------------------------------------

class TestParameterFileResolution:
    def _parse(self, tmp_path, text):
        p = tmp_path / "wf.par"
        p.write_text(text, encoding="utf-8")
        return ParameterFileParser().parse(str(p))

    def test_double_dollar_reference_resolves(self, tmp_path):
        res = self._parse(tmp_path, "[F.s_m]\n$$DESC=/data/in\n$$REF=$$DESC/x\n")
        assert res.all_params["$$REF"] == "/data/in/x"
        assert res.scopes[0].parameters["$$REF"] == "/data/in/x"

    def test_single_dollar_builtin_still_resolves(self, tmp_path):
        res = self._parse(tmp_path, "$PMRootDir=/infa\n$PMSessionLogDir=$PMRootDir/SessLogs\n")
        assert res.all_params["$PMSessionLogDir"] == "/infa/SessLogs"

    def test_single_dollar_name_does_not_rewrite_a_double_dollar_one(self, tmp_path):
        # $DESC (a different parameter) must not be substituted into $$DESC.
        res = self._parse(tmp_path, "[F.s]\n$DESC=WRONG\n$$DESC=/right\n$$OUT=$$DESC/x\n")
        assert res.all_params["$$OUT"] == "/right/x"

    def test_workflow_and_session_prefixes_are_stripped_from_scope(self, tmp_path):
        res = self._parse(tmp_path, "[F.WF:wf_load.ST:s_m_orders]\n$$A=1\n")
        scope = res.scopes[0]
        assert scope.folder == "F"
        assert scope.session == "s_m_orders"
        assert scope.scope == "F.WF:wf_load.ST:s_m_orders"  # raw header kept

    def test_worklet_scope(self, tmp_path):
        res = self._parse(tmp_path, "[F.WF:wf.WT:wl.ST:s_x]\n$$A=1\n")
        assert res.scopes[0].session == "s_x"


# ---------------------------------------------------------------------------
# 5. Reconcile dashboard reads the JSON the reconciler actually writes
# ---------------------------------------------------------------------------

def _run_notebook_cells(notebook: str, wanted: tuple[str, ...]) -> dict:
    """Execute the setup cell plus the cells whose text contains any of
    ``wanted`` with matplotlib stubbed out, returning the namespace."""
    stub = types.ModuleType("matplotlib")
    stub.use = lambda *_a, **_k: None
    stub_plt = types.ModuleType("matplotlib.pyplot")
    stub_plt.show = lambda: None
    stub_plt.close = lambda *_a: None
    stub.pyplot = stub_plt
    saved = {k: sys.modules.get(k) for k in ("matplotlib", "matplotlib.pyplot")}
    sys.modules["matplotlib"] = stub
    sys.modules["matplotlib.pyplot"] = stub_plt
    ns: dict = {"__builtins__": __builtins__}
    try:
        cells = [c for c in notebook.split(CELL_SEP) if not c.strip().startswith("# MAGIC")]
        exec(cells[1], ns)  # setup cell (cells[0] is the notebook header)
        for cell in cells[2:]:
            if any(w in cell for w in wanted):
                exec(cell, ns)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return ns


class TestReconcileDashboardContract:
    def _dashboard(self, tmp_path, results):
        report_path = tmp_path / "reconcile_report.json"
        ReconcileReport(results).save_json(str(report_path))
        out = tmp_path / "dash.py"
        nb = DashboardGenerator().generate_reconcile_dashboard(str(report_path), str(out))
        return _run_notebook_cells(nb, ("verdict =", "schema_mismatches"))

    def test_passed_results_from_the_reconciler_are_seen_as_passed(self, tmp_path):
        ns = self._dashboard(tmp_path, [
            ReconcileResult(config_name="orders", reconcile_type="row_count",
                            status="PASSED", source_row_count=5, target_row_count=5,
                            row_count_match=True),
        ])
        assert ns["total"] == 1
        assert ns["passed"] == 1
        assert ns["verdict"] == "ALL CHECKS PASSED"
        # Legacy keys the cells render are derived from the report's keys.
        assert ns["tables"][0]["table_name"] == "orders"

    def test_failed_schema_result_is_not_a_pass(self, tmp_path):
        ns = self._dashboard(tmp_path, [
            ReconcileResult(config_name="orders", reconcile_type="schema", status="FAILED",
                            schema_diffs=[SchemaColumnDiff("AMT", "NUMBER", "STRING",
                                                           "TYPE_MISMATCH")]),
        ])
        assert ns["passed"] == 0
        assert ns["verdict"] == "ACTION REQUIRED"
        assert ns["tables"][0]["schema_mismatches"][0]["column"] == "AMT"

    def test_empty_report_is_not_all_checks_passed(self, tmp_path):
        ns = self._dashboard(tmp_path, [])
        assert ns["verdict"] == "NO RECONCILIATION RESULTS"

    def test_hand_written_legacy_shape_still_works(self, tmp_path):
        report_path = tmp_path / "legacy.json"
        report_path.write_text(json.dumps({"tables": [
            {"table_name": "T", "status": "PASS"}, {"table_name": "U", "status": "FAIL"},
        ]}), encoding="utf-8")
        nb = DashboardGenerator().generate_reconcile_dashboard(str(report_path),
                                                               str(tmp_path / "d.py"))
        ns = _run_notebook_cells(nb, ("verdict =",))
        assert (ns["total"], ns["passed"]) == (2, 1)


# ---------------------------------------------------------------------------
# 6. RAG store: fingerprint carries the expression's shape
# ---------------------------------------------------------------------------

def _expr_spec(name, expression):
    return ConversionSpec(
        transformation_name=name, transformation_type="Expression",
        inputs=[{"name": "A"}, {"name": "B"}],
        outputs=[{"name": "OUT", "expression": expression}],
        logic={"expressions": [{"field": "OUT", "expression": expression}]},
    )


class TestRagFingerprintShape:
    def test_same_functions_different_arithmetic_do_not_match(self, tmp_path):
        store = RAGStore(store_path=str(tmp_path / "rag.json"))
        s1 = _expr_spec("EXP_1", "ROUND(AMT * RATE, 2)")
        s2 = _expr_spec("EXP_2", "ROUND(QTY / DOZ, 0)")
        store.store(s1, "df.withColumn('OUT', F.round(F.col('AMT') * F.col('RATE'), 2))",
                    confidence=0.95)
        assert store.find_exact(s2) is None
        assert store.find_similar(s2) is None

    def test_renamed_ports_share_a_shape_but_are_not_an_exact_match(self, tmp_path):
        """Renamed ports are one *shape* and two *conversions*.

        This test used to require an exact hit, and that is the defect it
        should have caught: the stored PySpark for ``ROUND(AMT * RATE, 2)``
        contains ``F.col('AMT')``, and nothing re-templates it, so handing it
        back for a mapping whose ports are ``price`` and ``fx_rate`` emits
        columns that do not exist. ``find_exact`` keys on the content hash,
        which includes port names, so it declines -- while the blanked
        skeleton still puts both under one shape.
        """
        store = RAGStore(store_path=str(tmp_path / "rag.json"))
        s1 = _expr_spec("EXP_1", "ROUND(AMT * RATE, 2)")
        s2 = _expr_spec("EXP_2", "ROUND(price * fx_rate, 2)")
        store.store(s1, "code")
        assert store.find_exact(s2) is None
        assert store._expression_shape("ROUND(AMT * RATE, 2)") == \
               store._expression_shape("ROUND(price * fx_rate, 2)")

    def test_shape_keeps_operators_literals_and_functions(self):
        shape = RAGStore._expression_shape
        assert shape("ROUND(AMT * RATE, 2)") == "ROUND(_*_,2)"
        assert shape("IIF(STATUS = 'A', 1, 0)") == "IIF(_='~',1,0)"
        assert shape("ROUND(AMT * RATE, 2)") != shape("ROUND(AMT / RATE, 2)")


# ---------------------------------------------------------------------------
# 7. Package extraction: bounded uncompressed size and entry count
# ---------------------------------------------------------------------------

class TestPackageLimits:
    def test_too_many_entries_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(package_extractor, "MAX_PACKAGE_ENTRIES", 3)
        archive = tmp_path / "p.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            for i in range(4):
                zf.writestr(f"m{i}.json", "{}")
        with pytest.raises(ValueError, match="entries"):
            package_extractor.extract_package(archive, tmp_path / "out")
        assert not (tmp_path / "out" / "m0.json").exists()

    def test_oversized_uncompressed_payload_is_refused(self, tmp_path, monkeypatch):
        monkeypatch.setattr(package_extractor, "MAX_PACKAGE_BYTES", 1024)
        archive = tmp_path / "p.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("big.json", "0" * 4096)  # compresses to a few bytes
        with pytest.raises(ValueError, match="uncompressed size"):
            package_extractor.extract_package(archive, tmp_path / "out")

    def test_normal_package_extracts(self, tmp_path):
        archive = tmp_path / "p.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("mapping.json", '{"name": "m"}')
        dest = package_extractor.extract_package(archive, tmp_path / "out")
        assert (dest / "mapping.json").exists()


# ---------------------------------------------------------------------------
# 8. XXE guard: the DOCTYPE itself must be PowerCenter's
# ---------------------------------------------------------------------------

class TestDoctypeGuard:
    GOOD = '<?xml version="1.0"?>\n<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">\n<POWERMART/>'

    def test_standard_export_doctype_is_allowed(self):
        validate_no_xxe(self.GOOD)
        validate_no_xxe(self.GOOD.replace('"powrmart.dtd"', "'powrmart.dtd'"))

    def test_foreign_doctype_is_rejected_even_when_powrmart_appears_elsewhere(self):
        xml = ('<?xml version="1.0"?>\n'
               '<!DOCTYPE POWERMART SYSTEM "http://attacker.example/evil.dtd">\n'
               '<POWERMART><!-- powrmart.dtd --></POWERMART>')
        with pytest.raises(SecurityError):
            validate_no_xxe(xml)

    def test_public_doctype_pointing_at_powrmart_name_is_rejected(self):
        xml = ('<!DOCTYPE POWERMART PUBLIC "-//x//EN" "http://h/powrmart.dtd">'
               "<POWERMART/>")
        with pytest.raises(SecurityError):
            validate_no_xxe(xml)

    def test_entity_declarations_still_rejected(self):
        with pytest.raises(SecurityError):
            validate_no_xxe('<!DOCTYPE POWERMART SYSTEM "powrmart.dtd" [<!ENTITY x SYSTEM "file:///etc/passwd">]><POWERMART/>')
