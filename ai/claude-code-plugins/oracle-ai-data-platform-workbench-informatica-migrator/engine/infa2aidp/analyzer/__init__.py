"""Informatica export analysis: inventory, complexity, dependencies, and reporting."""

from infa2aidp.analyzer.analyzer import InformaticaAnalyzer
from infa2aidp.analyzer.models import AnalysisReport
from infa2aidp.analyzer.complexity import ComplexityAssessor
from infa2aidp.analyzer.dependency import DependencyAnalyzer
from infa2aidp.analyzer.inventory import InventoryScanner
from infa2aidp.analyzer.report_generator import ReportGenerator

# Public re-exports use the names from the spec
InventoryReport = AnalysisReport

__all__ = [
    "InformaticaAnalyzer",
    "InventoryReport",
    "AnalysisReport",
    "ComplexityAssessor",
    "DependencyAnalyzer",
    "InventoryScanner",
    "ReportGenerator",
]
