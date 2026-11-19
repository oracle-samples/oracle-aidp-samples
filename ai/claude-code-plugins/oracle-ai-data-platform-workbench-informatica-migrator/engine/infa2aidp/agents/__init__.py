"""Agentic iterative conversion pipeline for Informatica-to-AIDP migration.

Flow: Generate -> Validate -> Fix -> Re-generate (up to N attempts).
"""

from .models import (
    AgentRole,
    ConversionAttempt,
    ConversionRecord,
    ConversionSpec,
    ValidationResult,
)
from .spec_agent import SpecAgent
from .codegen_agent import CodeGenAgent
from .validator_agent import ValidatorAgent
from .fixer_agent import FixerAgent
from .pipeline import ConversionPipeline
from .rag_store import RAGEntry, RAGStore
from .reviewer import HumanReviewer, ReviewItem
from .hallucination_detector import HallucinationDetector, HallucinationIssue, HallucinationReport

# Convenience alias
AgenticConverter = ConversionPipeline

__all__ = [
    "AgenticConverter",
    "AgentRole",
    "ConversionAttempt",
    "ConversionRecord",
    "ConversionSpec",
    "ValidationResult",
    "SpecAgent",
    "CodeGenAgent",
    "ValidatorAgent",
    "FixerAgent",
    "ConversionPipeline",
    "RAGEntry",
    "RAGStore",
    "HumanReviewer",
    "ReviewItem",
]
