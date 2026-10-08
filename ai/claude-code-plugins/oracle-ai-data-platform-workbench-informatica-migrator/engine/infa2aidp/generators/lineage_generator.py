"""Generate data lineage documentation from parsed Informatica mappings.

Builds a graph of source -> transformation -> target field-level lineage
and exports it as Markdown and JSON reports.
"""

import json
import os
from collections import defaultdict
from typing import Optional

from ..models import (
    Connector,
    DataLineage,
    LineageEdge,
    LineageNode,
    Mapping,
    SourceDefinition,
    TargetDefinition,
    Transformation,
    TransformationField,
    TransformationType,
)


class LineageGenerator:
    """Builds field-level data lineage from an Informatica mapping."""

    def generate(self, mapping: Mapping) -> DataLineage:
        """Trace connectors to build a complete lineage graph.

        Returns a DataLineage with nodes (sources, transformations, targets)
        and edges (field-level connections between them).
        """
        nodes: dict[str, LineageNode] = {}
        edges: list[LineageEdge] = []

        # Index instances by name for lookup
        instance_types = self._build_instance_index(mapping)

        # Create nodes
        for src in mapping.sources:
            fields = self._extract_field_names(src.fields)
            nodes[src.name] = LineageNode(
                name=src.name, node_type="SOURCE", fields=fields
            )

        for tx in mapping.transformations:
            fields = self._extract_field_names(tx.fields)
            nodes[tx.name] = LineageNode(
                name=tx.name, node_type="TRANSFORMATION", fields=fields
            )

        for tgt in mapping.targets:
            fields = self._extract_field_names(tgt.fields)
            nodes[tgt.name] = LineageNode(
                name=tgt.name, node_type="TARGET", fields=fields
            )

        # Build edges from connectors
        for conn in mapping.connectors:
            tx_name = self._find_transformation_name(
                conn.from_instance, conn.to_instance, mapping
            )
            edges.append(
                LineageEdge(
                    source_node=conn.from_instance,
                    source_field=conn.from_field,
                    target_node=conn.to_instance,
                    target_field=conn.to_field,
                    transformation=tx_name,
                )
            )

        # Fill in any nodes referenced by connectors but not yet in the map
        for edge in edges:
            for node_name in (edge.source_node, edge.target_node):
                if node_name not in nodes:
                    nodes[node_name] = LineageNode(name=node_name, node_type="UNKNOWN")

        return DataLineage(nodes=list(nodes.values()), edges=edges)

    def export_lineage_report(
        self, lineage: DataLineage, output_path: str
    ) -> None:
        """Write Markdown and JSON lineage reports to *output_path*.

        Creates:
            {output_path}.md  — human-readable Markdown
            {output_path}.json — machine-readable JSON
        """
        self._export_markdown(lineage, f"{output_path}.md")
        self._export_json(lineage, f"{output_path}.json")

    # ------------------------------------------------------------------
    # Markdown export
    # ------------------------------------------------------------------

    @staticmethod
    def _export_markdown(lineage: DataLineage, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        sources = [n for n in lineage.nodes if n.node_type == "SOURCE"]
        targets = [n for n in lineage.nodes if n.node_type == "TARGET"]
        transforms = [n for n in lineage.nodes if n.node_type == "TRANSFORMATION"]

        # Build target-field -> [source paths] map
        field_lineage: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: defaultdict(list)
        )
        # Trace each edge chain: group by target_node.target_field
        edge_by_target: dict[tuple[str, str], list[LineageEdge]] = defaultdict(list)
        edge_by_source: dict[tuple[str, str], list[LineageEdge]] = defaultdict(list)
        for e in lineage.edges:
            edge_by_target[(e.target_node, e.target_field)].append(e)
            edge_by_source[(e.source_node, e.source_field)].append(e)

        # For each target field, walk backward to find source fields
        for tgt in targets:
            for tgt_field in tgt.fields:
                chain = _trace_backward(
                    tgt.name, tgt_field, edge_by_target, set()
                )
                if chain:
                    field_lineage[tgt.name][tgt_field] = chain

        lines = ["# Data Lineage Report", ""]

        # Sources
        lines.append("## Sources")
        for s in sources:
            lines.append(f"- **{s.name}**: {', '.join(s.fields[:10])}"
                         + (" ..." if len(s.fields) > 10 else ""))
        lines.append("")

        # Transformations
        if transforms:
            lines.append("## Transformations")
            for t in transforms:
                lines.append(f"- **{t.name}** ({t.node_type})")
            lines.append("")

        # Field-level lineage
        lines.append("## Field-Level Lineage")
        for tgt in targets:
            lines.append(f"\n### Target: {tgt.name}\n")
            lines.append("| Target Field | Source Path |")
            lines.append("|---|---|")
            fl = field_lineage.get(tgt.name, {})
            for tgt_field in tgt.fields:
                paths = fl.get(tgt_field, ["(no lineage traced)"])
                lines.append(f"| {tgt_field} | {'; '.join(paths)} |")

        lines.append("")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ------------------------------------------------------------------
    # JSON export
    # ------------------------------------------------------------------

    @staticmethod
    def _export_json(lineage: DataLineage, path: str) -> None:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        data = {
            "nodes": [
                {"name": n.name, "type": n.node_type, "fields": n.fields}
                for n in lineage.nodes
            ],
            "edges": [
                {
                    "source_node": e.source_node,
                    "source_field": e.source_field,
                    "target_node": e.target_node,
                    "target_field": e.target_field,
                    "transformation": e.transformation,
                }
                for e in lineage.edges
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_instance_index(mapping: Mapping) -> dict[str, str]:
        """Map instance name -> type string (SOURCE, TARGET, or transformation type)."""
        idx: dict[str, str] = {}
        for s in mapping.sources:
            idx[s.name] = "SOURCE"
        for t in mapping.targets:
            idx[t.name] = "TARGET"
        for tx in mapping.transformations:
            idx[tx.name] = tx.type.value
        return idx

    @staticmethod
    def _extract_field_names(fields) -> list[str]:
        """Extract field name strings from heterogeneous field lists."""
        names: list[str] = []
        for f in fields:
            if isinstance(f, str):
                names.append(f)
            elif isinstance(f, TransformationField):
                names.append(f.name)
            elif isinstance(f, dict):
                names.append(f.get("name", f.get("target_field", str(f))))
            elif hasattr(f, "name"):
                names.append(f.name)
            elif hasattr(f, "source_field"):
                names.append(f.source_field)
        return names

    @staticmethod
    def _find_transformation_name(
        from_inst: str, to_inst: str, mapping: Mapping
    ) -> str:
        """Identify which transformation an edge passes through."""
        tx_names = {tx.name for tx in mapping.transformations}
        if from_inst in tx_names:
            return from_inst
        if to_inst in tx_names:
            return to_inst
        return ""


def _trace_backward(
    node: str,
    field: str,
    edge_by_target: dict[tuple[str, str], list[LineageEdge]],
    visited: set,
) -> list[str]:
    """Recursively trace a field back to its source(s).

    Returns a list of human-readable path strings like
    "SourceTable.COLUMN -> Expression.EXPR_COL -> TargetTable.TGT_COL".
    """
    key = (node, field)
    if key in visited:
        return [f"{node}.{field} (circular)"]
    visited.add(key)

    incoming = edge_by_target.get(key, [])
    if not incoming:
        return [f"{node}.{field}"]

    paths: list[str] = []
    for edge in incoming:
        upstream = _trace_backward(
            edge.source_node, edge.source_field, edge_by_target, visited
        )
        for u in upstream:
            paths.append(f"{u} -> {node}.{field}")
    return paths
