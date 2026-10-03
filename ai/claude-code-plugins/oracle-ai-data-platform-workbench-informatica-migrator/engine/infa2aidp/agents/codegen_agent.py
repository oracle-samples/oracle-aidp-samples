"""CodeGenAgent -- generate PySpark code from canonical ConversionSpec.

Strategy:
1. Try rule-based conversion (existing TransformationConverter)
2. If rule-based produces TODO markers or is LOW confidence -> use LLM
3. LLM gets the clean JSON spec (not raw XML) -> less hallucination
"""

from __future__ import annotations

import json
import re
from typing import Optional

from ..models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)
from ..converters.transformation_converter import TransformationConverter
from .models import ConversionAttempt, ConversionSpec


class CodeGenAgent:
    """Generate PySpark code from canonical ConversionSpec."""

    def __init__(self, llm_handler=None, custom_rules: Optional[dict] = None):
        self.converter = TransformationConverter()
        self.llm = llm_handler
        self.custom_rules = custom_rules or {}

    def generate(self, spec: ConversionSpec) -> ConversionAttempt:
        """Generate PySpark code from a spec."""
        attempt = ConversionAttempt(attempt_number=1, spec=spec)

        # Step 1: Apply custom expression rules if available
        if self.custom_rules:
            self._apply_custom_rules(spec)

        # Step 2: Try rule-based conversion
        code = self._rule_based_convert(spec)

        # Step 3: If code has TODO markers or is complex -> LLM
        if self.llm and self._needs_llm(code, spec):
            llm_code = self._llm_convert(spec, code)
            if llm_code and not llm_code.startswith("# TODO"):
                code = llm_code
                attempt.agent_used = "llm"
            else:
                attempt.agent_used = "rule-based"
        else:
            attempt.agent_used = "rule-based"

        attempt.generated_code = code
        attempt.confidence = self._estimate_confidence(code, spec)
        return attempt

    # ------------------------------------------------------------------
    # Rule-based conversion
    # ------------------------------------------------------------------

    def _rule_based_convert(self, spec: ConversionSpec) -> str:
        """Convert using existing TransformationConverter.

        Reconstructs a Transformation object from the spec and delegates
        to the battle-tested converter.
        """
        tx = self._spec_to_transformation(spec)
        lines = self.converter.convert(tx, input_df="df", output_df="df_out")
        return "\n".join(lines)

    def _spec_to_transformation(self, spec: ConversionSpec) -> Transformation:
        """Reconstruct a Transformation from a ConversionSpec."""
        type_map = {v.value: v for v in TransformationType}
        tx_type = type_map.get(spec.transformation_type, TransformationType.UNKNOWN)

        tx = Transformation(name=spec.transformation_name, type=tx_type)

        # Rebuild fields
        seen = set()
        for inp in spec.inputs:
            tx.fields.append(TransformationField(
                name=inp["name"],
                datatype=inp.get("datatype", "STRING"),
                precision=inp.get("precision", 0),
                scale=inp.get("scale", 0),
                direction=DataFlowDirection.INPUT,
            ))
            seen.add(inp["name"])

        for out in spec.outputs:
            if out["name"] in seen:
                # Upgrade to INPUT_OUTPUT
                for f in tx.fields:
                    if f.name == out["name"]:
                        f.direction = DataFlowDirection.INPUT_OUTPUT
                        f.expression = out.get("expression", "")
                        break
            else:
                tx.fields.append(TransformationField(
                    name=out["name"],
                    datatype=out.get("datatype", "STRING"),
                    precision=out.get("precision", 0),
                    scale=out.get("scale", 0),
                    expression=out.get("expression", ""),
                    direction=DataFlowDirection.OUTPUT,
                ))

        # Populate type-specific attributes from logic
        logic = spec.logic
        if tx_type == TransformationType.SOURCE_QUALIFIER:
            tx.sql_override = logic.get("sql_override", "")
            tx.properties["source_table"] = (logic.get("source_tables") or [""])[0]

        elif tx_type == TransformationType.FILTER:
            tx.filter_condition = logic.get("condition", "")

        elif tx_type == TransformationType.JOINER:
            tx.join_condition = logic.get("condition", "")
            tx.join_type = logic.get("join_type", "INNER")

        elif tx_type == TransformationType.LOOKUP:
            tx.lookup_table = logic.get("table", "")
            tx.lookup_condition = logic.get("condition", "")
            tx.lookup_sql = logic.get("sql_override", "")
            conn_type = "connected" if logic.get("connected", True) else "unconnected"
            tx.properties["connection_type"] = conn_type

        elif tx_type == TransformationType.AGGREGATOR:
            tx.group_by_fields = logic.get("group_by_fields", [])

        elif tx_type == TransformationType.ROUTER:
            tx.router_groups = logic.get("groups", [])

        elif tx_type == TransformationType.UPDATE_STRATEGY:
            tx.update_strategy_expression = logic.get("strategy_expression", "")

        elif tx_type == TransformationType.SEQUENCE_GENERATOR:
            tx.start_value = logic.get("start_value", 1)
            tx.increment_by = logic.get("increment_by", 1)

        elif tx_type == TransformationType.SORTER:
            tx.sort_keys = logic.get("sort_keys", [])

        elif tx_type == TransformationType.RANK:
            tx.group_by_fields = logic.get("group_by", [])
            tx.sort_keys = logic.get("sort_keys", [])
            tx.properties["top_bottom"] = logic.get("top_bottom", "")

        elif tx_type == TransformationType.UNION:
            tx.properties["input_groups"] = logic.get("input_groups", [])

        elif tx_type == TransformationType.STORED_PROCEDURE:
            tx.properties["procedure_name"] = logic.get("procedure_name", "")
            tx.properties["parameters"] = logic.get("parameters", [])

        elif tx_type == TransformationType.SQL:
            tx.sql_override = logic.get("sql_query", "")

        return tx

    # ------------------------------------------------------------------
    # LLM decision & conversion
    # ------------------------------------------------------------------

    def _needs_llm(self, code: str, spec: ConversionSpec) -> bool:
        """Determine if LLM is needed for this conversion."""
        if "TODO" in code:
            return True
        if spec.transformation_type in ("Stored Procedure", "SQL Transformation"):
            return True
        # Complex expression count
        expressions = spec.logic.get("expressions", [])
        complex_count = sum(
            1 for e in expressions if len(e.get("expression", "")) > 80
        )
        if complex_count > 3:
            return True
        return False

    def _llm_convert(self, spec: ConversionSpec, rule_based_code: str) -> str:
        """Use LLM to convert from spec.

        The prompt includes the canonical JSON spec (structured, not raw
        XML) plus the rule-based attempt as a starting point.
        """
        prompt = f"""Convert this Informatica transformation spec to PySpark code.

Transformation: {spec.transformation_type}
Name: {spec.transformation_name}

Spec (JSON):
{json.dumps(spec.logic, indent=2, default=str)}

Input fields: {json.dumps(spec.inputs, indent=2, default=str)}
Output fields: {json.dumps(spec.outputs, indent=2, default=str)}

Rule-based attempt (may be incomplete):
```python
{rule_based_code}
```

Requirements:
- Use pyspark.sql.functions as F
- Use proper DataFrame operations
- Handle NULL values with F.coalesce or F.when
- Return ONLY executable PySpark code, no explanations
- Variable names: input is `df`, output is `df_out`
"""
        return self.llm._call(prompt)

    # ------------------------------------------------------------------
    # Custom rules & confidence
    # ------------------------------------------------------------------

    def _apply_custom_rules(self, spec: ConversionSpec) -> None:
        """Apply user-supplied expression rewrite rules to the spec."""
        expressions = spec.logic.get("expressions", [])
        for expr_info in expressions:
            original = expr_info.get("expression", "")
            for pattern, replacement in self.custom_rules.items():
                original = re.sub(pattern, replacement, original)
            expr_info["expression"] = original

    def _estimate_confidence(self, code: str, spec: ConversionSpec) -> float:
        """Estimate how confident we are in the generated code."""
        score = 1.0

        todo_count = code.count("TODO")
        score -= todo_count * 0.15

        if "Unsupported" in code:
            score -= 0.3

        if spec.transformation_type in ("Stored Procedure", "SQL Transformation"):
            score -= 0.2

        # Reward: has proper PySpark patterns
        if "F.col(" in code or "F.lit(" in code:
            score += 0.05

        return max(0.0, min(1.0, score))
