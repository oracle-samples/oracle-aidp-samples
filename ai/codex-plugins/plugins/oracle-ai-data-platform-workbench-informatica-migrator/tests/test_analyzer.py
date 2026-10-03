"""Tests for InformaticaAnalyzer against a three-stage warehouse-load export."""

import os
import tempfile
import unittest

from infa2aidp.analyzer.analyzer import InformaticaAnalyzer
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

FIXTURES_PC = os.path.join(os.path.dirname(__file__), "fixtures", "powercenter")
FIXTURES_CORPUS = os.path.join(os.path.dirname(__file__), "fixtures", "corpus")
MULTI_STAGE_XML = os.path.join(FIXTURES_PC, "multi_stage_invoice_dw.xml")


class TestAnalyzerInventory(unittest.TestCase):
    """Verify inventory counts from the analyzer."""

    @classmethod
    def setUpClass(cls):
        parser = InformaticaXMLParser()
        cls.result = parser.parse(MULTI_STAGE_XML)
        cls.analyzer = InformaticaAnalyzer()
        cls.inventory = cls.analyzer.inventory_scanner.scan(cls.result)

    def test_total_mappings(self):
        self.assertEqual(self.inventory.total_mappings, 3)

    def test_total_sessions(self):
        self.assertEqual(self.inventory.total_sessions, 3)

    def test_total_workflows(self):
        self.assertEqual(self.inventory.total_workflows, 1)

    def test_total_sources_positive(self):
        self.assertGreater(self.inventory.total_sources, 0)

    def test_total_targets_positive(self):
        self.assertGreater(self.inventory.total_targets, 0)

    def test_total_transformations_positive(self):
        self.assertGreater(self.inventory.total_transformations, 0)

    def test_transformation_type_counts_include_expected(self):
        counts = self.inventory.transformation_type_counts
        self.assertIn("Source Qualifier", counts)
        self.assertIn("Expression", counts)
        self.assertIn("Filter", counts)
        self.assertIn("Lookup", counts)

    def test_version_string(self):
        self.assertIn("10.x", self.inventory.informatica_version)


class TestAnalyzerComplexity(unittest.TestCase):
    """Verify complexity assessments are reasonable."""

    @classmethod
    def setUpClass(cls):
        parser = InformaticaXMLParser()
        cls.result = parser.parse(MULTI_STAGE_XML)
        cls.analyzer = InformaticaAnalyzer()
        cls.assessments = cls.analyzer.complexity_assessor.assess_all(cls.result)

    def test_assessment_count_matches_mappings(self):
        self.assertEqual(len(self.assessments), 3)

    def test_all_scores_positive(self):
        for a in self.assessments:
            self.assertGreater(a.complexity_score, 0, f"{a.mapping_name} score should be > 0")

    def test_all_effort_positive(self):
        for a in self.assessments:
            self.assertGreater(
                a.estimated_effort_hours, 0,
                f"{a.mapping_name} effort should be > 0",
            )

    def test_complexity_levels_valid(self):
        valid_levels = {"SIMPLE", "MEDIUM", "COMPLEX", "VERY_COMPLEX"}
        for a in self.assessments:
            self.assertIn(a.complexity_level, valid_levels, f"{a.mapping_name} has invalid level")

    def test_sil_at_least_moderate(self):
        sil = [a for a in self.assessments if a.mapping_name == "m_load_invoice_fact"]
        self.assertTrue(len(sil) == 1)
        # SIL has lookups, update strategy, expression -- should be at least moderate
        self.assertIn(sil[0].complexity_level, {"MEDIUM", "COMPLEX", "VERY_COMPLEX"})


class TestAnalyzerDependencies(unittest.TestCase):
    """Verify dependency analysis."""

    @classmethod
    def setUpClass(cls):
        parser = InformaticaXMLParser()
        cls.result = parser.parse(MULTI_STAGE_XML)
        cls.analyzer = InformaticaAnalyzer()
        cls.deps = cls.analyzer.dependency_analyzer.build_dependency_graph(cls.result)

    def test_mapping_dependencies_exist(self):
        # SIL depends on SDE (via staging table)
        sil_deps = self.deps.mapping_dependencies.get("m_load_invoice_fact", [])
        self.assertIn("m_stage_invoice_txn", sil_deps)

    def test_workflow_task_graph(self):
        self.assertIn("wf_finance_analytics", self.deps.workflow_task_graph)


class TestAnalyzerFullPipeline(unittest.TestCase):
    """Test the full analyze() pipeline end-to-end."""

    def test_analyze_produces_report(self):
        analyzer = InformaticaAnalyzer()
        with tempfile.TemporaryDirectory() as tmpdir:
            report = analyzer.analyze(MULTI_STAGE_XML, tmpdir, formats=["json"])
            self.assertIsNotNone(report)
            self.assertEqual(report.inventory.total_mappings, 3)
            self.assertTrue(len(report.complexity_assessments) == 3)
            self.assertTrue(report.generated_at != "")
            # JSON output should exist
            self.assertTrue(
                os.path.isfile(os.path.join(tmpdir, "analysis_report.json"))
            )


if __name__ == "__main__":
    unittest.main()
