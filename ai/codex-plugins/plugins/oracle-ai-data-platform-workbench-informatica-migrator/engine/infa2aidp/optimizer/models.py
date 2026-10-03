"""Data models for the Spark optimization engine."""

from dataclasses import dataclass, field
from enum import Enum


class OptimizationType(Enum):
    BROADCAST_JOIN = "broadcast_join"
    PARTITION_PRUNING = "partition_pruning"
    PREDICATE_PUSHDOWN = "predicate_pushdown"
    CACHE_REUSE = "cache_reuse"
    REPARTITION = "repartition"
    COALESCE = "coalesce"
    AQE = "adaptive_query_execution"
    DELTA_OPTIMIZE = "delta_optimize"
    COLUMN_PRUNING = "column_pruning"
    UDF_ELIMINATION = "udf_elimination"


@dataclass
class OptimizationSuggestion:
    rule_name: str
    optimization_type: OptimizationType
    description: str
    original_code: str = ""
    optimized_code: str = ""
    estimated_improvement: str = ""  # "2x faster", "50% less memory", etc.
    priority: str = "MEDIUM"  # LOW, MEDIUM, HIGH, CRITICAL
    auto_applicable: bool = False  # Can be auto-applied vs needs manual review
    line_number: int = 0
    applies_to: str = ""  # transformation/notebook name


@dataclass
class OptimizationReport:
    notebook_name: str = ""
    total_suggestions: int = 0
    auto_applicable: int = 0
    manual_review: int = 0
    suggestions: list[OptimizationSuggestion] = field(default_factory=list)
    estimated_overall_improvement: str = ""

    def compute_counts(self) -> None:
        """Recompute summary counts from the suggestions list."""
        self.total_suggestions = len(self.suggestions)
        self.auto_applicable = sum(1 for s in self.suggestions if s.auto_applicable)
        self.manual_review = self.total_suggestions - self.auto_applicable
