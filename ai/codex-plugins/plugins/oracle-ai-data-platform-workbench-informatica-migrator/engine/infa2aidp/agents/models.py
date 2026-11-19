"""Data models for the agentic conversion pipeline."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class AgentRole(Enum):
    SPEC = "spec"
    CODEGEN = "codegen"
    VALIDATOR = "validator"
    FIXER = "fixer"
    REVIEWER = "reviewer"


class ValidationResult(Enum):
    PASSED = "passed"
    SYNTAX_ERROR = "syntax_error"
    RUNTIME_ERROR = "runtime_error"
    LOGIC_ERROR = "logic_error"
    NEEDS_REVIEW = "needs_review"


@dataclass
class ConversionSpec:
    """Canonical intermediate representation of a transformation.

    Produced by the SpecAgent -- a deterministic, structured JSON
    representation that reduces LLM hallucination risk compared to
    feeding raw XML.
    """

    mapping_name: str = ""
    transformation_name: str = ""
    transformation_type: str = ""
    inputs: list = field(default_factory=list)     # [{name, datatype, source}]
    outputs: list = field(default_factory=list)    # [{name, datatype, expression}]
    logic: dict = field(default_factory=dict)      # Type-specific logic
    # Expression:        {expressions: [{field, expr}]}
    # Filter:            {condition: str}
    # Joiner:            {condition, join_type, master, detail}
    # Lookup:            {table, condition, sql_override, connected}
    # Aggregator:        {group_by: [str], aggregates: [{field, func}]}
    # Update Strategy:   {strategy_expr: str}
    # Source Qualifier:   {sql_override, source_tables, incremental_filter}
    context: dict = field(default_factory=dict)    # Mapping-level context
    parameters: dict = field(default_factory=dict)  # $$variables


@dataclass
class ConversionAttempt:
    """One attempt at converting a transformation."""

    attempt_number: int = 1
    spec: Optional[ConversionSpec] = None
    generated_code: str = ""
    validation_result: ValidationResult = ValidationResult.NEEDS_REVIEW
    validation_errors: list = field(default_factory=list)
    fix_applied: str = ""
    confidence: float = 0.0
    agent_used: str = ""  # "rule-based", "llm", "fixer"


@dataclass
class ConversionRecord:
    """Full record of converting one transformation, including all attempts."""

    transformation_name: str = ""
    transformation_type: str = ""
    attempts: list = field(default_factory=list)  # List[ConversionAttempt]
    final_code: str = ""
    final_status: str = "pending"  # pending, success, partial, failed
    total_attempts: int = 0
    max_attempts: int = 3
