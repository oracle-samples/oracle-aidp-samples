"""SpecAgent -- deterministically parse Informatica transformations into canonical specs.

No LLM involved. Normalises the XML-parsed Transformation objects into
a clean JSON structure that the CodeGen agent can work with reliably.
"""

from __future__ import annotations

import json
from typing import Optional

from ..models import (
    DataFlowDirection,
    Transformation,
    TransformationField,
    TransformationType,
)
from .models import ConversionSpec


class SpecAgent:
    """Parse Informatica transformations into canonical ConversionSpec."""

    def generate_spec(
        self,
        transformation: Transformation,
        mapping_context: Optional[dict] = None,
    ) -> ConversionSpec:
        """Convert a parsed Transformation into a canonical spec."""
        spec = ConversionSpec(
            transformation_name=transformation.name,
            transformation_type=(
                transformation.type.value
                if hasattr(transformation.type, "value")
                else str(transformation.type)
            ),
        )

        # Extract inputs and outputs from fields
        for f in transformation.fields:
            if not isinstance(f, TransformationField):
                continue

            field_info = {
                "name": f.name,
                "datatype": f.datatype,
                "precision": f.precision,
                "scale": f.scale,
            }

            if f.direction in (DataFlowDirection.INPUT, DataFlowDirection.INPUT_OUTPUT):
                spec.inputs.append(dict(field_info))

            if f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT):
                field_info["expression"] = f.expression or ""
                field_info["default_value"] = f.default_value or ""
                spec.outputs.append(field_info)

        # Type-specific logic
        spec.logic = self._extract_logic(transformation)

        # Mapping-level context
        if mapping_context:
            spec.context = mapping_context
            spec.mapping_name = mapping_context.get("mapping_name", "")
            spec.parameters = mapping_context.get("parameters", {})

        return spec

    # ------------------------------------------------------------------
    # Type-specific logic extraction
    # ------------------------------------------------------------------

    def _extract_logic(self, tx: Transformation) -> dict:
        """Extract transformation-type-specific logic into a clean dict."""
        handlers = {
            TransformationType.SOURCE_QUALIFIER: self._logic_source_qualifier,
            TransformationType.EXPRESSION: self._logic_expression,
            TransformationType.FILTER: self._logic_filter,
            TransformationType.JOINER: self._logic_joiner,
            TransformationType.LOOKUP: self._logic_lookup,
            TransformationType.AGGREGATOR: self._logic_aggregator,
            TransformationType.ROUTER: self._logic_router,
            TransformationType.UPDATE_STRATEGY: self._logic_update_strategy,
            TransformationType.SEQUENCE_GENERATOR: self._logic_sequence_generator,
            TransformationType.SORTER: self._logic_sorter,
            TransformationType.RANK: self._logic_rank,
            TransformationType.UNION: self._logic_union,
            TransformationType.STORED_PROCEDURE: self._logic_stored_procedure,
            TransformationType.SQL: self._logic_sql_transformation,
        }
        handler = handlers.get(tx.type, self._logic_default)
        return handler(tx)

    def _logic_source_qualifier(self, tx: Transformation) -> dict:
        source_tables = []
        if tx.properties.get("source_table"):
            source_tables.append(tx.properties["source_table"])

        incremental_filter = ""
        if any(
            "$$LAST_EXTRACT_DATE" in (f.expression or "")
            for f in tx.fields
            if isinstance(f, TransformationField)
        ) or "$$LAST_EXTRACT_DATE" in (tx.sql_override or ""):
            incremental_filter = "$$LAST_EXTRACT_DATE"

        return {
            "sql_override": tx.sql_override or "",
            "source_tables": source_tables,
            "incremental_filter": incremental_filter,
        }

    def _logic_expression(self, tx: Transformation) -> dict:
        expressions = []
        for f in tx.fields:
            if not isinstance(f, TransformationField):
                continue
            if f.direction in (
                DataFlowDirection.OUTPUT,
                DataFlowDirection.INPUT_OUTPUT,
            ) and f.expression:
                expressions.append({
                    "field": f.name,
                    "expression": f.expression,
                    "datatype": f.datatype,
                })
        return {"expressions": expressions}

    def _logic_filter(self, tx: Transformation) -> dict:
        return {"condition": tx.filter_condition or ""}

    def _logic_joiner(self, tx: Transformation) -> dict:
        return {
            "condition": tx.join_condition or "",
            "join_type": tx.join_type or "INNER",
            "master_source": tx.properties.get("master_source", ""),
            "detail_source": tx.properties.get("detail_source", ""),
        }

    def _logic_lookup(self, tx: Transformation) -> dict:
        return_fields = [
            f.name
            for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)
        ]
        return {
            "table": tx.lookup_table or tx.properties.get("lookup_table", ""),
            "condition": tx.lookup_condition or "",
            "sql_override": tx.lookup_sql or "",
            "return_fields": return_fields,
            "connected": tx.properties.get("connection_type", "connected").lower() == "connected",
        }

    def _logic_aggregator(self, tx: Transformation) -> dict:
        aggregates = []
        for f in tx.fields:
            if not isinstance(f, TransformationField):
                continue
            if f.direction == DataFlowDirection.INPUT:
                continue
            if f.name in (tx.group_by_fields or []):
                continue
            if f.expression:
                aggregates.append({
                    "field": f.name,
                    "function": f.expression,
                })
        return {
            "group_by_fields": list(tx.group_by_fields or []),
            "aggregates": aggregates,
        }

    def _logic_router(self, tx: Transformation) -> dict:
        groups = []
        for g in (tx.router_groups or []):
            groups.append({
                "name": g.get("name", ""),
                "condition": g.get("condition", ""),
            })
        return {"groups": groups}

    def _logic_update_strategy(self, tx: Transformation) -> dict:
        expr = tx.update_strategy_expression or ""
        return {
            "strategy_expression": expr,
            "dd_insert": "DD_INSERT" in expr.upper(),
            "dd_update": "DD_UPDATE" in expr.upper(),
            "dd_delete": "DD_DELETE" in expr.upper(),
        }

    def _logic_sequence_generator(self, tx: Transformation) -> dict:
        current_port = ""
        next_port = ""
        for f in tx.fields:
            if not isinstance(f, TransformationField):
                continue
            upper = f.name.upper()
            if "CURRVAL" in upper:
                current_port = f.name
            elif "NEXTVAL" in upper:
                next_port = f.name
        return {
            "start_value": tx.start_value or 1,
            "increment_by": tx.increment_by or 1,
            "current_value_port": current_port,
            "next_value_port": next_port,
        }

    def _logic_sorter(self, tx: Transformation) -> dict:
        sort_keys = []
        for key in (tx.sort_keys or []):
            if isinstance(key, dict):
                sort_keys.append({
                    "field": key.get("field", key.get("name", "")),
                    "direction": key.get("direction", "ASC"),
                })
            else:
                sort_keys.append({
                    "field": str(key),
                    "direction": tx.sort_direction or "ASC",
                })
        distinct = tx.properties.get("distinct", False)
        return {"sort_keys": sort_keys, "distinct": distinct}

    def _logic_rank(self, tx: Transformation) -> dict:
        rank_field = ""
        for f in tx.fields:
            if isinstance(f, TransformationField) and "RANK" in f.name.upper():
                rank_field = f.name
                break

        return {
            "rank_field": rank_field or "RANK_NUM",
            "group_by": list(tx.group_by_fields or []),
            "top_bottom": tx.properties.get("top_bottom", ""),
            "rank_count": tx.properties.get("rank_count", ""),
            "sort_keys": [
                {
                    "field": k.get("field", k.get("name", "")) if isinstance(k, dict) else str(k),
                    "direction": k.get("direction", "ASC") if isinstance(k, dict) else "ASC",
                }
                for k in (tx.sort_keys or [])
            ],
        }

    def _logic_union(self, tx: Transformation) -> dict:
        output_fields = [
            f.name
            for f in tx.fields
            if isinstance(f, TransformationField)
            and f.direction in (DataFlowDirection.OUTPUT, DataFlowDirection.INPUT_OUTPUT)
        ]
        input_groups = tx.properties.get("input_groups", [])
        return {
            "input_groups": input_groups,
            "output_fields": output_fields,
        }

    def _logic_stored_procedure(self, tx: Transformation) -> dict:
        return {
            "procedure_name": tx.properties.get("procedure_name", tx.name),
            "parameters": tx.properties.get("parameters", []),
            "database": tx.properties.get("database", ""),
        }

    def _logic_sql_transformation(self, tx: Transformation) -> dict:
        return {
            "sql_query": tx.sql_override or "",
            "connection": tx.properties.get("connection", ""),
        }

    def _logic_default(self, tx: Transformation) -> dict:
        return {"properties": dict(tx.properties)}

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export_specs(self, specs: list[ConversionSpec], output_path: str) -> None:
        """Write all specs to a JSON file for debugging/review."""
        data = []
        for s in specs:
            data.append({
                "mapping_name": s.mapping_name,
                "transformation_name": s.transformation_name,
                "transformation_type": s.transformation_type,
                "inputs": s.inputs,
                "outputs": s.outputs,
                "logic": s.logic,
                "context": s.context,
                "parameters": s.parameters,
            })
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, default=str)
