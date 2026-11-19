"""Assess migration complexity for each Informatica mapping."""

import logging
import re

from infa2aidp.models import (
    LoadStrategy,
    Mapping,
    MigrationResult,
    TransformationType,
)
from infa2aidp.analyzer.models import MappingComplexity

logger = logging.getLogger(__name__)

# Transformation complexity tiers
_SIMPLE_TYPES = {
    TransformationType.FILTER,
    TransformationType.SORTER,
    TransformationType.RANK,
    TransformationType.SEQUENCE_GENERATOR,
    TransformationType.UNION,
    TransformationType.SOURCE_QUALIFIER,
}

_MEDIUM_TYPES = {
    TransformationType.EXPRESSION,
    TransformationType.AGGREGATOR,
    TransformationType.JOINER,
    TransformationType.UPDATE_STRATEGY,
    TransformationType.ROUTER,
    TransformationType.NORMALIZER,
    TransformationType.TRANSACTION_CONTROL,
}

_COMPLEX_TYPES = {
    TransformationType.STORED_PROCEDURE,
    TransformationType.CUSTOM,
    TransformationType.SQL,
}

_CRITICAL_TYPES = {
    TransformationType.JAVA,
    TransformationType.HTTP,
    TransformationType.XML_PARSER,
    TransformationType.XML_GENERATOR,
}

# Pattern to detect complex expressions (nested IIF or large DECODE)
_NESTED_IIF = re.compile(r"IIF\s*\(.*IIF\s*\(", re.IGNORECASE | re.DOTALL)
_LARGE_DECODE = re.compile(r"DECODE\s*\((?:[^,]*,){10,}", re.IGNORECASE | re.DOTALL)


class ComplexityAssessor:
    """Score each mapping's migration complexity on a 1-100 scale."""

    def assess_all(self, result: MigrationResult) -> list[MappingComplexity]:
        return [self.assess(mapping) for mapping in result.mappings]

    def assess(self, mapping: Mapping) -> MappingComplexity:
        mc = MappingComplexity(mapping_name=mapping.name)
        mc.num_sources = len(mapping.sources)
        mc.num_targets = len(mapping.targets)
        mc.num_transformations = len(mapping.transformations)

        score = 10  # base score
        has_critical = False
        has_high_risk = False
        has_medium_risk = False

        # Score transformations
        for tx in mapping.transformations:
            if tx.type == TransformationType.LOOKUP:
                mc.num_lookups += 1
                if tx.lookup_sql:
                    score += 10
                    mc.has_sql_overrides = True
                    mc.issues.append(f"Lookup '{tx.name}' has SQL override")
                    has_medium_risk = True
                else:
                    score += 5
            elif tx.type == TransformationType.JOINER:
                mc.num_joins += 1
                score += 5
            elif tx.type in _SIMPLE_TYPES:
                score += 2
            elif tx.type in _MEDIUM_TYPES:
                score += 5
            elif tx.type in _COMPLEX_TYPES:
                score += 10
                has_high_risk = True
                if tx.type == TransformationType.STORED_PROCEDURE:
                    mc.has_stored_procedures = True
                    score += 15
                    mc.issues.append(f"Stored procedure '{tx.name}' requires manual migration")
                elif tx.type == TransformationType.CUSTOM:
                    mc.has_custom_transformations = True
                    score += 20
                    mc.issues.append(f"Custom transformation '{tx.name}' has no PySpark equivalent")
            elif tx.type in _CRITICAL_TYPES:
                score += 10
                has_critical = True
                mc.issues.append(
                    f"{tx.type.value} '{tx.name}' is unsupported in AIDP"
                )

            # Check for SQL overrides on source qualifiers
            if tx.sql_override and tx.type == TransformationType.SOURCE_QUALIFIER:
                score += 10
                mc.has_sql_overrides = True
                mc.issues.append(f"Source qualifier '{tx.name}' has SQL override")
                has_medium_risk = True

            # Check for complex expressions in expression fields
            if tx.type == TransformationType.EXPRESSION:
                for f in tx.fields:
                    if f.expression:
                        if _NESTED_IIF.search(f.expression):
                            mc.has_complex_expressions = True
                            has_medium_risk = True
                        if _LARGE_DECODE.search(f.expression):
                            mc.has_complex_expressions = True
                            has_medium_risk = True

            # Check reusable
            if tx.properties.get("reusable"):
                score += 5
                mc.notes.append(f"Reusable transformation '{tx.name}'")

        # Complex expressions add-on (once)
        if mc.has_complex_expressions:
            score += 10
            mc.issues.append("Contains complex nested expressions (IIF/DECODE)")

        # Multiple sources / targets
        if mc.num_sources > 1:
            score += 5 * (mc.num_sources - 1)
        if mc.num_targets > 1:
            score += 5 * (mc.num_targets - 1)

        # Transformation count thresholds
        if mc.num_transformations > 20:
            score += 20
        elif mc.num_transformations > 10:
            score += 10

        # SCD detection
        for target in mapping.targets:
            if target.load_strategy in (LoadStrategy.SCD_TYPE2,):
                mc.has_scd_logic = True
                score += 15
                mc.notes.append(f"Target '{target.name}' uses SCD Type 2")
                has_medium_risk = True

        # Cap at 100
        mc.complexity_score = min(score, 100)

        # Assign level
        if mc.complexity_score <= 25:
            mc.complexity_level = "SIMPLE"
            mc.estimated_effort_hours = round(1.0 + (mc.complexity_score / 25), 1)
        elif mc.complexity_score <= 50:
            mc.complexity_level = "MEDIUM"
            mc.estimated_effort_hours = round(2.0 + 2.0 * (mc.complexity_score - 25) / 25, 1)
        elif mc.complexity_score <= 75:
            mc.complexity_level = "COMPLEX"
            mc.estimated_effort_hours = round(4.0 + 4.0 * (mc.complexity_score - 50) / 25, 1)
        else:
            mc.complexity_level = "VERY_COMPLEX"
            mc.estimated_effort_hours = round(8.0 + 8.0 * (mc.complexity_score - 75) / 25, 1)

        # Risk assessment
        if has_critical:
            mc.migration_risk = "CRITICAL"
        elif has_high_risk:
            mc.migration_risk = "HIGH"
        elif has_medium_risk:
            mc.migration_risk = "MEDIUM"
        else:
            mc.migration_risk = "LOW"

        return mc
