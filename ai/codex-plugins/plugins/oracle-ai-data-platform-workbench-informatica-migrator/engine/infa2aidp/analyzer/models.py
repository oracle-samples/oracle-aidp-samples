"""Data models for Informatica analysis output."""

from dataclasses import dataclass, field


@dataclass
class ComponentInventory:
    """Inventory of all Informatica components found."""
    total_mappings: int = 0
    total_sessions: int = 0
    total_workflows: int = 0
    total_sources: int = 0
    total_targets: int = 0
    total_transformations: int = 0
    transformation_type_counts: dict = field(default_factory=dict)
    folder_counts: dict = field(default_factory=dict)
    connection_types: dict = field(default_factory=dict)
    informatica_version: str = ""


@dataclass
class MappingComplexity:
    """Complexity assessment for a single mapping."""
    mapping_name: str
    complexity_score: int = 0
    complexity_level: str = "SIMPLE"
    num_transformations: int = 0
    num_sources: int = 0
    num_targets: int = 0
    num_lookups: int = 0
    num_joins: int = 0
    has_complex_expressions: bool = False
    has_stored_procedures: bool = False
    has_custom_transformations: bool = False
    has_sql_overrides: bool = False
    has_scd_logic: bool = False
    estimated_effort_hours: float = 0.0
    migration_risk: str = "LOW"
    issues: list = field(default_factory=list)
    notes: list = field(default_factory=list)


@dataclass
class DependencyInfo:
    """Cross-dependencies between components."""
    mapping_dependencies: dict = field(default_factory=dict)
    table_dependencies: dict = field(default_factory=dict)
    shared_lookups: dict = field(default_factory=dict)
    reusable_transformations: dict = field(default_factory=dict)
    workflow_task_graph: dict = field(default_factory=dict)
    subject_area_groups: dict = field(default_factory=dict)


@dataclass
class AnalysisReport:
    """Complete analysis report."""
    inventory: ComponentInventory = field(default_factory=ComponentInventory)
    complexity_assessments: list = field(default_factory=list)
    dependencies: DependencyInfo = field(default_factory=DependencyInfo)
    compatibility_issues: list = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    generated_at: str = ""
