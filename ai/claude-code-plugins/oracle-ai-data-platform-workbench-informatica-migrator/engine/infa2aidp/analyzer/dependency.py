"""Analyze cross-dependencies between Informatica components."""

import logging
import re
from collections import defaultdict

from infa2aidp.models import (
    MigrationResult,
    TransformationType,
)
from infa2aidp.analyzer.models import DependencyInfo

logger = logging.getLogger(__name__)


class DependencyAnalyzer:
    """Analyze cross-dependencies between Informatica components."""

    def build_dependency_graph(self, result: MigrationResult) -> DependencyInfo:
        deps = DependencyInfo()

        # Track which mappings read/write which tables
        table_writers: dict[str, list[str]] = defaultdict(list)  # table -> [mapping_names]
        table_readers: dict[str, list[str]] = defaultdict(list)  # table -> [mapping_names]
        lookup_users: dict[str, list[str]] = defaultdict(list)   # lookup_table -> [mapping_names]
        reusable_users: dict[str, list[str]] = defaultdict(list) # reusable_tx -> [mapping_names]

        for mapping in result.mappings:
            mname = mapping.name

            # Sources = tables this mapping reads
            for src in mapping.sources:
                tbl = src.table_name or src.name
                table_readers[tbl].append(mname)

            # Targets = tables this mapping writes
            for tgt in mapping.targets:
                tbl = tgt.table_name or tgt.name
                table_writers[tbl].append(mname)

            # Lookups and reusable transformations
            for tx in mapping.transformations:
                if tx.type == TransformationType.LOOKUP and tx.lookup_table:
                    lookup_users[tx.lookup_table].append(mname)
                if tx.properties.get("reusable"):
                    reusable_users[tx.name].append(mname)

        # Table dependencies: {table: [read_by_mappings]}
        deps.table_dependencies = dict(table_readers)

        # Mapping dependencies: if A writes table X and B reads table X, B depends on A
        mapping_deps: dict[str, list[str]] = defaultdict(list)
        for table, writers in table_writers.items():
            readers = table_readers.get(table, [])
            for reader in readers:
                for writer in writers:
                    if reader != writer and writer not in mapping_deps[reader]:
                        mapping_deps[reader].append(writer)
        deps.mapping_dependencies = dict(mapping_deps)

        # Shared lookups (used by more than one mapping)
        deps.shared_lookups = {
            tbl: mappings for tbl, mappings in lookup_users.items()
            if len(mappings) > 1
        }

        # Reusable transformations (used by more than one mapping)
        deps.reusable_transformations = {
            tx: mappings for tx, mappings in reusable_users.items()
            if len(mappings) > 1
        }

        # Workflow task graph
        for wf in result.workflows:
            task_graph: dict[str, list[str]] = {}
            for link in wf.dependencies:
                from_task = link.get("from_task", "")
                to_task = link.get("to_task", "")
                if to_task:
                    task_graph.setdefault(to_task, [])
                    if from_task:
                        task_graph[to_task].append(from_task)
            if task_graph:
                deps.workflow_task_graph[wf.name] = task_graph

        # Subject area grouping
        self._build_subject_area_groups(result, deps)

        return deps

    def _build_subject_area_groups(
        self,
        result: MigrationResult,
        deps: DependencyInfo,
    ) -> None:
        """Group mappings by their declared subject area, if any.

        The subject area is whatever the source assigns (e.g. an explicit
        ``subject_area`` on the mapping) -- there is no naming-convention
        inference, since that would assume a fixed taxonomy.
        """
        groups: dict[str, list[str]] = defaultdict(list)

        for mapping in result.mappings:
            area = mapping.subject_area or ""
            if area:
                groups[area].append(mapping.name)

        deps.subject_area_groups = dict(groups)

    def export_dependency_diagram(self, deps: DependencyInfo, output_path: str) -> None:
        """Generate a Mermaid diagram of mapping dependencies."""
        lines = ["```mermaid", "graph LR"]

        # Mapping dependencies
        for mapping, depends_on in deps.mapping_dependencies.items():
            safe_m = _mermaid_id(mapping)
            for dep in depends_on:
                safe_d = _mermaid_id(dep)
                lines.append(f"    {safe_d}[{dep}] --> {safe_m}[{mapping}]")

        # Workflow task graphs
        for wf_name, tasks in deps.workflow_task_graph.items():
            lines.append("")
            lines.append(f"    %% Workflow: {wf_name}")
            for task, predecessors in tasks.items():
                safe_task = _mermaid_id(f"{wf_name}_{task}")
                for pred in predecessors:
                    safe_pred = _mermaid_id(f"{wf_name}_{pred}")
                    lines.append(f"    {safe_pred}[{pred}] --> {safe_task}[{task}]")

        lines.append("```")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        logger.info("Dependency diagram written to %s", output_path)


def _mermaid_id(name: str) -> str:
    """Sanitize a name for use as a Mermaid node ID."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)
