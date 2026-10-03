"""The analysis report (analyzer.report_generator) and the compatibility
checker/report (handlers.compatibility_checker).

These are what a migration lead reads before committing to a plan, so the
pinned behaviour is the content: which components are flagged at which
severity, and that the Markdown/JSON/CSV renderings carry the inventory,
complexity ranking, dependency graph and recommendations faithfully.
"""
from __future__ import annotations

import csv
import json

import pytest

from infa2aidp.analyzer.models import (
    AnalysisReport,
    ComponentInventory,
    DependencyInfo,
    MappingComplexity,
)
from infa2aidp.analyzer.report_generator import ReportGenerator, _mermaid_id
from infa2aidp.handlers.compatibility_checker import CompatibilityChecker
from infa2aidp.models import (
    CompatibilityIssue,
    ConnectionInfo,
    FieldMapping,
    Mapping,
    MigrationResult,
    Session,
    SourceDefinition,
    Transformation,
    TransformationType,
    Workflow,
)


# ---------------------------------------------------------------------------
# Analysis report
# ---------------------------------------------------------------------------

def _report(**kw) -> AnalysisReport:
    inv = ComponentInventory(
        total_mappings=4, total_sessions=3, total_workflows=1, total_sources=5,
        total_targets=4, total_transformations=20,
        transformation_type_counts={"Expression": 10, "Lookup": 5, "Joiner": 1},
        folder_counts={"SALES": 3, "HR": 1},
        connection_types={"Oracle": 2, "Relational": 1},
        informatica_version="10.5.4",
    )
    assessments = [
        MappingComplexity("m_simple", complexity_score=5, complexity_level="SIMPLE",
                          estimated_effort_hours=2, migration_risk="LOW"),
        MappingComplexity("m_simple2", complexity_score=8, complexity_level="SIMPLE",
                          estimated_effort_hours=2.5, migration_risk="LOW"),
        MappingComplexity("m_med", complexity_score=30, complexity_level="MEDIUM",
                          estimated_effort_hours=8, migration_risk="MEDIUM",
                          issues=["3 lookups", "SQL override", "third issue"]),
        MappingComplexity("m_crit", complexity_score=90, complexity_level="VERY_COMPLEX",
                          estimated_effort_hours=40, migration_risk="CRITICAL",
                          has_stored_procedures=True, issues=["Java transformation"]),
    ]
    deps = DependencyInfo(
        mapping_dependencies={"m_med": ["m_simple"], "m_crit": ["m_med", "m-simple 2"]},
        shared_lookups={"LKP_CUST": ["m_med", "m_crit"]},
        subject_area_groups={"Sales": [f"m{i}" for i in range(7)], "HR": ["m_hr"]},
    )
    issues = [
        CompatibilityIssue("m_med / sq", "PARTIAL", "DECODE used", "Use CASE", "WARNING"),
        CompatibilityIssue("m_crit / java", "UNSUPPORTED", "Java tx", "Rewrite", "ERROR"),
        CompatibilityIssue("wf", "PARTIAL", "Scheduler", "Configure", "INFO"),
    ]
    defaults = dict(inventory=inv, complexity_assessments=assessments, dependencies=deps,
                    compatibility_issues=issues, summary={"ok": True},
                    generated_at="2026-09-29T00:00:00")
    defaults.update(kw)
    return AnalysisReport(**defaults)


class TestAnalysisMarkdown:
    @pytest.fixture
    def md(self, tmp_path):
        out = tmp_path / "analysis.md"
        ReportGenerator().generate_markdown(_report(), str(out))
        return out.read_text(encoding="utf-8")

    def test_executive_summary(self, md):
        assert "Generated: 2026-09-29T00:00:00" in md
        assert "- **Informatica Version**: 10.5.4" in md
        assert "- **Total Mappings**: 4 (HR: 1, SALES: 3)" in md
        assert "- **Migration Complexity**: 50% Simple, 25% Medium, 0% Complex, 25% Very Complex" in md
        assert "- **Estimated Total Effort**: 52 hours" in md

    def test_inventory_folders_and_connections(self, md):
        for row in ("| Mappings | 4 |", "| Sessions | 3 |", "| Transformations | 20 |",
                    "| SALES | 3 |", "| Oracle | 2 |"):
            assert row in md

    def test_transformation_bars_are_proportional_and_sorted(self, md):
        block = md.split("## Transformation Analysis", 1)[1].split("```")[1]
        lines = [ln for ln in block.splitlines() if ln.strip()]
        assert lines[0].split()[0] == "Expression"
        assert "#" * 40 + " 10" in lines[0]
        assert "#" * 20 + " 5" in lines[1]
        assert lines[2].rstrip().endswith("#### 1")    # round(40/10)=4, never 0

    def test_complexity_table_is_ranked_and_trims_issues(self, md):
        table = md.split("## Complexity Assessment", 1)[1].split("##", 1)[0]
        order = [ln.split("|")[1].strip() for ln in table.splitlines() if ln.startswith("| m")]
        assert order == ["m_crit", "m_med", "m_simple2", "m_simple"]
        assert "3 lookups; SQL override |" in table and "third issue" not in table
        assert "| m_simple | SIMPLE | 5 | 2 | LOW | - |" in table

    def test_dependency_graph_uses_safe_mermaid_ids(self, md):
        assert "```mermaid" in md and "graph LR" in md
        assert "    m_simple[m_simple] --> m_med[m_med]" in md
        assert "    m_simple_2[m-simple 2] --> m_crit[m_crit]" in md

    def test_subject_areas_truncate_after_five(self, md):
        assert "| Sales | m0, m1, m2, m3, m4 (+2 more) | 7 |" in md
        assert "| HR | m_hr | 1 |" in md

    def test_issues_sorted_by_severity(self, md):
        sec = md.split("## Compatibility Issues", 1)[1].split("##", 1)[0]
        sev = [ln.split("|")[1].strip() for ln in sec.splitlines() if ln.startswith("| ") and "---" not in ln]
        assert sev == ["Severity", "ERROR", "WARNING", "INFO"]

    def test_recommendations_cover_each_trigger(self, md):
        recs = md.split("## Migration Recommendations", 1)[1]
        assert "**Start with SIMPLE mappings** (2 mappings)" in recs
        assert "**Investigate CRITICAL risk mappings** (1 total: m_crit)" in recs
        assert "**Centralize shared lookup tables** (1 shared" in recs
        assert "**Rewrite stored procedures as PySpark** (1 mappings affected)" in recs

    def test_empty_report_is_still_well_formed(self, tmp_path):
        out = tmp_path / "empty.md"
        ReportGenerator().generate_markdown(AnalysisReport(), str(out))
        md = out.read_text(encoding="utf-8")
        assert "- **Informatica Version**: Unknown" in md
        assert "- **Total Mappings**: 0\n" in md
        assert "## Complexity Assessment" not in md and "```mermaid" not in md
        assert "No specific blockers detected" in md


class TestAnalysisJsonCsv:
    def test_json_carries_every_section(self, tmp_path):
        out = tmp_path / "a.json"
        ReportGenerator().generate_json(_report(), str(out))
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["generated_at"] == "2026-09-29T00:00:00"
        assert data["inventory"]["total_mappings"] == 4
        assert data["inventory"]["folder_counts"] == {"SALES": 3, "HR": 1}
        assert [a["mapping_name"] for a in data["complexity_assessments"]][-1] == "m_crit"
        assert data["dependencies"]["shared_lookups"] == {"LKP_CUST": ["m_med", "m_crit"]}
        assert data["compatibility_issues"][1]["severity"] == "ERROR"
        assert data["summary"] == {"ok": True}

    def test_csv_one_row_per_mapping_with_joined_issues(self, tmp_path):
        out = tmp_path / "a.csv"
        ReportGenerator().generate_csv(_report(), str(out))
        rows = list(csv.DictReader(out.open(encoding="utf-8")))
        assert [r["mapping_name"] for r in rows] == ["m_simple", "m_simple2", "m_med", "m_crit"]
        assert rows[2]["issues"] == "3 lookups; SQL override; third issue"
        assert rows[3]["has_stored_procedures"] == "True"
        assert rows[3]["estimated_effort_hours"] == "40"

    def test_mermaid_id(self):
        assert _mermaid_id("a.b-c d/e") == "a_b_c_d_e"


# ---------------------------------------------------------------------------
# Compatibility checker
# ---------------------------------------------------------------------------

def _tx(name, ttype, **kw):
    return Transformation(name=name, type=ttype, **kw)


class TestCompatibilityChecker:
    def _check(self, **kw):
        return CompatibilityChecker().check(MigrationResult(**kw))

    @pytest.mark.parametrize("ttype", [
        TransformationType.JAVA, TransformationType.HTTP, TransformationType.XML_PARSER,
        TransformationType.XML_GENERATOR, TransformationType.CUSTOM,
    ])
    def test_unsupported_transformations_are_errors(self, ttype):
        (issue,) = self._check(mappings=[Mapping("m1", transformations=[_tx("t1", ttype)])])
        assert (issue.severity, issue.issue_type, issue.component) == ("ERROR", "UNSUPPORTED", "m1 / t1")
        assert ttype.value in issue.description

    @pytest.mark.parametrize("ttype", [
        TransformationType.STORED_PROCEDURE, TransformationType.SQL,
        TransformationType.TRANSACTION_CONTROL,
    ])
    def test_manual_review_transformations_are_warnings(self, ttype):
        (issue,) = self._check(mappings=[Mapping("m1", transformations=[_tx("t1", ttype)])])
        assert (issue.severity, issue.issue_type) == ("WARNING", "PARTIAL")

    def test_supported_transformations_raise_nothing(self):
        txs = [_tx("e", TransformationType.EXPRESSION), _tx("f", TransformationType.FILTER)]
        assert self._check(mappings=[Mapping("m1", transformations=txs)]) == []

    @pytest.mark.parametrize("sql,label", [
        ("SELECT * FROM emp START WITH mgr IS NULL CONNECT BY PRIOR id = mgr", "CONNECT BY"),
        ("select rownum, a from t", "ROWNUM"),
        ("select decode(a, 1, 'x') from t", "DECODE"),
        ("select nvl2(a, 1, 0) from t", "NVL2"),
        ("select * from a, b where a.id = b.id(+)", "outer join"),
        ("begin dbms_output.put_line('x'); end;", "DBMS_"),
        ("select utl_raw.cast_to_raw(a) from t", "UTL_"),
        ("select sys_context('USERENV','DB_NAME') from dual", "SYS_CONTEXT"),
        ("select xmlagg(xmlelement(e, a)) from t", "XMLAGG"),
    ])
    def test_oracle_specific_sql_in_overrides_is_flagged(self, sql, label):
        tx = _tx("sq", TransformationType.SOURCE_QUALIFIER, sql_override=sql)
        issues = self._check(mappings=[Mapping("m1", transformations=[tx])])
        assert any(label in i.description for i in issues), [i.description for i in issues]
        assert all(i.severity == "WARNING" for i in issues)

    def test_lookup_sql_is_checked_too(self):
        tx = _tx("lkp", TransformationType.LOOKUP, lookup_sql="select decode(x,1,2) from d")
        issues = self._check(mappings=[Mapping("m1", transformations=[tx])])
        assert [i.component for i in issues] == ["m1 / lkp"]

    def test_portable_sql_is_not_flagged(self):
        tx = _tx("sq", TransformationType.SOURCE_QUALIFIER,
                 sql_override="SELECT a, CASE WHEN b = 1 THEN 'x' END FROM t WHERE c > 0")
        assert self._check(mappings=[Mapping("m1", transformations=[tx])]) == []

    SOURCE = SourceDefinition("SRC", fields=[
        FieldMapping("DOC", "DOC", datatype="clob"),
        FieldMapping("GEOM", "GEOM", datatype="SDO_GEOMETRY"),
        FieldMapping("ID", "ID", datatype="NUMBER"),
    ])

    def test_risky_source_datatypes_warn_once_per_field(self):
        issues = self._check(mappings=[Mapping("m1", sources=[self.SOURCE])])
        assert len(issues) == 2
        assert all(i.severity == "WARNING" for i in issues)
        assert sorted(i.description.split()[0] for i in issues) == ["CLOB", "SDO_GEOMETRY"]

    def test_risky_datatype_warning_names_the_column(self):
        issues = self._check(mappings=[Mapping("m1", sources=[self.SOURCE])])
        assert sorted(i.component for i in issues) == ["m1 / SRC.DOC", "m1 / SRC.GEOM"]

    @pytest.mark.parametrize("db,severity", [
        ("FLAT_FILE", "WARNING"), ("sap", "ERROR"), ("MAINFRAME", "ERROR"),
        ("VSAM", "ERROR"), ("IMS", "ERROR"),
    ])
    def test_connection_types(self, db, severity):
        src = SourceDefinition("SRC", connection=ConnectionInfo("C1", db_type=db))
        (issue,) = self._check(mappings=[Mapping("m1", sources=[src])])
        assert issue.severity == severity and issue.component == "m1 / C1"

    def test_ordinary_connection_is_fine(self):
        src = SourceDefinition("SRC", connection=ConnectionInfo("C1", db_type="Oracle"))
        assert self._check(mappings=[Mapping("m1", sources=[src])]) == []

    def test_workflow_with_a_schedule_is_info(self):
        wf = Workflow("wf_daily", scheduler={"SCHEDULETYPE": "DAILY"})
        (issue,) = self._check(workflows=[wf, Workflow("wf_manual")])
        assert (issue.severity, issue.component) == ("INFO", "Workflow: wf_daily")

    def test_enabled_session_features_are_info_and_disabled_ones_are_not(self):
        s = Session("s1", parameters={"Pushdown Optimization": "To Source",
                                      "Incremental Aggregation": "NO",
                                      "Session Partitioning": "0"})
        issues = self._check(sessions=[s])
        assert [i.description for i in issues] == [
            "Session feature 'pushdown optimization' is not applicable in Spark."]
        assert issues[0].severity == "INFO"

    def test_session_features_are_read_from_session_properties(self):
        s = Session("s1", properties={"Pushdown Optimization": "To Source"})
        issues = self._check(sessions=[s])
        assert any("pushdown optimization" in i.description for i in issues)


class TestCompatibilityReport:
    def test_report_counts_percentages_and_sorts(self, tmp_path):
        issues = [
            CompatibilityIssue("c", "PARTIAL", "i1", "s", "INFO"),
            CompatibilityIssue("b", "PARTIAL", "w1", "s", "WARNING"),
            CompatibilityIssue("a", "UNSUPPORTED", "e1", "s", "ERROR"),
            CompatibilityIssue("d", "PARTIAL", "w2", "s", "WARNING"),
        ]
        path = CompatibilityChecker().generate_report(issues, str(tmp_path / "out"))
        assert path.endswith("compatibility_report.md")
        md = open(path, encoding="utf-8").read()
        assert "| Total issues | 4 | |" in md
        assert "| Unsupported (ERROR) | 1 | 25.0% |" in md
        assert "| Partial support (WARNING) | 2 | 50.0% |" in md
        assert "| Info | 1 | 25.0% |" in md
        body = md.split("## Issues", 1)[1].split("##", 1)[0]
        assert body.index("| ERROR |") < body.index("| WARNING |") < body.index("| INFO |")
        assert "**Unsupported items**" in md and "**Partial support items**" in md

    def test_empty_report_has_zero_percentages_and_no_item_recommendations(self, tmp_path):
        md = open(CompatibilityChecker().generate_report([], str(tmp_path)), encoding="utf-8").read()
        assert "| Total issues | 0 | |" in md
        assert "| Unsupported (ERROR) | 0 | 0.0% |" in md
        assert "**Unsupported items**" not in md
        assert "Run `infa2aidp analyze` first" in md
