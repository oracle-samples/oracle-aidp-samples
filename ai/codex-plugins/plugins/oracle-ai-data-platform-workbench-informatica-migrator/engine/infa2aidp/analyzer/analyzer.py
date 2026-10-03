"""Main orchestrator for Informatica analysis."""

import logging
import os
from datetime import datetime, timezone

from infa2aidp.models import CompatibilityIssue, MigrationResult, TransformationType
from infa2aidp.parsers.xml_parser import InformaticaXMLParser
from infa2aidp.analyzer.models import AnalysisReport
from infa2aidp.analyzer.inventory import InventoryScanner
from infa2aidp.analyzer.complexity import ComplexityAssessor
from infa2aidp.analyzer.dependency import DependencyAnalyzer
from infa2aidp.analyzer.report_generator import ReportGenerator

logger = logging.getLogger(__name__)

# Transformations that have no direct PySpark equivalent
_UNSUPPORTED_TX_TYPES = {
    TransformationType.JAVA,
    TransformationType.HTTP,
    TransformationType.XML_PARSER,
    TransformationType.XML_GENERATOR,
    TransformationType.STORED_PROCEDURE,
    TransformationType.CUSTOM,
}


class InformaticaAnalyzer:
    """Full analysis pipeline: parse, inventory, complexity, dependencies, report."""

    def __init__(self):
        self.parser = InformaticaXMLParser()
        self.inventory_scanner = InventoryScanner()
        self.complexity_assessor = ComplexityAssessor()
        self.dependency_analyzer = DependencyAnalyzer()
        self.report_generator = ReportGenerator()

    def analyze(
        self,
        input_path: str,
        output_dir: str,
        formats: list[str] | None = None,
    ) -> AnalysisReport:
        """Run the full analysis pipeline.

        Args:
            input_path: Path to Informatica XML file or directory of XML files.
            output_dir: Directory to write output reports.
            formats: Output formats to generate. Defaults to ["markdown", "json", "csv"].

        Returns:
            Complete AnalysisReport.
        """
        if formats is None:
            formats = ["markdown", "json", "csv"]

        os.makedirs(output_dir, exist_ok=True)

        # 1. Parse XML
        logger.info("Parsing Informatica exports from %s", input_path)
        result = self.parser.parse(input_path)
        logger.info(
            "Parsed %d mappings, %d sessions, %d workflows",
            len(result.mappings),
            len(result.sessions),
            len(result.workflows),
        )

        # 2. Scan inventory
        logger.info("Scanning component inventory")
        inventory = self.inventory_scanner.scan(result)

        # 3. Assess complexity per mapping
        logger.info("Assessing mapping complexity")
        assessments = self.complexity_assessor.assess_all(result)

        # 4. Analyze dependencies
        logger.info("Analyzing dependencies")
        dependencies = self.dependency_analyzer.build_dependency_graph(result)

        # 5. Check compatibility
        logger.info("Checking compatibility")
        compatibility_issues = self._check_compatibility(result)

        # 6. Build report
        report = AnalysisReport(
            inventory=inventory,
            complexity_assessments=assessments,
            dependencies=dependencies,
            compatibility_issues=compatibility_issues,
            summary=self._build_summary(inventory, assessments, dependencies),
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        )

        # 7. Generate output
        self._generate_reports(report, output_dir, formats, dependencies)

        return report

    def analyze_result(self, result: MigrationResult, output_dir: str, formats: list[str] | None = None) -> AnalysisReport:
        """Run analysis on an already-parsed MigrationResult."""
        if formats is None:
            formats = ["markdown", "json", "csv"]

        os.makedirs(output_dir, exist_ok=True)

        inventory = self.inventory_scanner.scan(result)
        assessments = self.complexity_assessor.assess_all(result)
        dependencies = self.dependency_analyzer.build_dependency_graph(result)
        compatibility_issues = self._check_compatibility(result)

        report = AnalysisReport(
            inventory=inventory,
            complexity_assessments=assessments,
            dependencies=dependencies,
            compatibility_issues=compatibility_issues,
            summary=self._build_summary(inventory, assessments, dependencies),
            generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        )

        self._generate_reports(report, output_dir, formats, dependencies)
        return report

    def _check_compatibility(self, result: MigrationResult) -> list[CompatibilityIssue]:
        """Check for compatibility issues with AIDP/PySpark migration."""
        issues = []

        for mapping in result.mappings:
            for tx in mapping.transformations:
                if tx.type in _UNSUPPORTED_TX_TYPES:
                    severity = "ERROR" if tx.type in (
                        TransformationType.JAVA,
                        TransformationType.HTTP,
                    ) else "WARNING"
                    issues.append(CompatibilityIssue(
                        component=f"{mapping.name}/{tx.name}",
                        issue_type="UNSUPPORTED",
                        description=(
                            f"{tx.type.value} has no direct PySpark equivalent"
                        ),
                        suggestion=_migration_suggestion(tx.type),
                        severity=severity,
                    ))

                # SQL overrides need review
                if tx.sql_override:
                    issues.append(CompatibilityIssue(
                        component=f"{mapping.name}/{tx.name}",
                        issue_type="PARTIAL",
                        description="SQL override requires manual review for Spark SQL compatibility",
                        suggestion="Convert to Spark SQL or DataFrame operations",
                        severity="WARNING",
                    ))

                # Lookup SQL overrides
                if tx.lookup_sql:
                    issues.append(CompatibilityIssue(
                        component=f"{mapping.name}/{tx.name}",
                        issue_type="PARTIAL",
                        description="Lookup SQL override may use vendor-specific syntax",
                        suggestion="Convert to broadcast join or Delta table lookup",
                        severity="WARNING",
                    ))

        return issues

    def _build_summary(self, inventory, assessments, dependencies) -> dict:
        dist = {}
        for a in assessments:
            dist[a.complexity_level] = dist.get(a.complexity_level, 0) + 1

        total_effort = sum(a.estimated_effort_hours for a in assessments)
        risk_dist = {}
        for a in assessments:
            risk_dist[a.migration_risk] = risk_dist.get(a.migration_risk, 0) + 1

        return {
            "total_components": (
                inventory.total_mappings
                + inventory.total_sessions
                + inventory.total_workflows
            ),
            "total_mappings": inventory.total_mappings,
            "complexity_distribution": dist,
            "risk_distribution": risk_dist,
            "estimated_total_effort_hours": round(total_effort, 1),
            "shared_lookups_count": len(dependencies.shared_lookups),
            "reusable_transformations_count": len(dependencies.reusable_transformations),
        }

    def _generate_reports(
        self,
        report: AnalysisReport,
        output_dir: str,
        formats: list[str],
        dependencies,
    ) -> None:
        if "markdown" in formats:
            md_path = os.path.join(output_dir, "analysis_report.md")
            self.report_generator.generate_markdown(report, md_path)

        if "json" in formats:
            json_path = os.path.join(output_dir, "analysis_report.json")
            self.report_generator.generate_json(report, json_path)

        if "csv" in formats:
            csv_path = os.path.join(output_dir, "mapping_complexity.csv")
            self.report_generator.generate_csv(report, csv_path)

        # Always generate dependency diagram if there are dependencies
        if dependencies.mapping_dependencies:
            dep_path = os.path.join(output_dir, "dependency_diagram.md")
            self.dependency_analyzer.export_dependency_diagram(dependencies, dep_path)

        logger.info("Reports generated in %s", output_dir)


def _migration_suggestion(tx_type: TransformationType) -> str:
    suggestions = {
        TransformationType.JAVA: "Rewrite Java logic as PySpark UDF or native DataFrame operations",
        TransformationType.HTTP: "Replace with PySpark requests or AIDP REST connector",
        TransformationType.XML_PARSER: "Use spark-xml library for XML parsing in PySpark",
        TransformationType.XML_GENERATOR: "Use spark-xml library for XML generation in PySpark",
        TransformationType.STORED_PROCEDURE: "Rewrite stored procedure logic as PySpark/Spark SQL",
        TransformationType.CUSTOM: "Rewrite custom transformation as PySpark UDF",
    }
    return suggestions.get(tx_type, "Manual migration required")
