"""Scan parsed Informatica exports and produce a component inventory."""

import logging
from collections import defaultdict

from infa2aidp.models import (
    MigrationResult,
    TransformationType,
)
from infa2aidp.analyzer.models import ComponentInventory

logger = logging.getLogger(__name__)


class InventoryScanner:
    """Scans a MigrationResult and produces a ComponentInventory."""

    def scan(self, result: MigrationResult) -> ComponentInventory:
        inv = ComponentInventory()

        inv.informatica_version = result.version_detail or result.version.value

        inv.total_mappings = len(result.mappings)
        inv.total_sessions = len(result.sessions)
        inv.total_workflows = len(result.workflows)

        tx_type_counts: dict[str, int] = defaultdict(int)
        folder_counts: dict[str, int] = defaultdict(int)
        connection_types: dict[str, int] = defaultdict(int)
        total_sources = 0
        total_targets = 0
        total_transformations = 0

        for mapping in result.mappings:
            # Folder counts (source-derived organizational unit)
            folder_counts[mapping.folder or "Migrated"] += 1

            # Source counts
            total_sources += len(mapping.sources)
            total_targets += len(mapping.targets)

            # Transformation counts
            total_transformations += len(mapping.transformations)
            for tx in mapping.transformations:
                tx_type_counts[tx.type.value] += 1

        # Connection types from sessions
        for session in result.sessions:
            for conn in session.source_connections.values():
                if conn.db_type:
                    connection_types[conn.db_type] += 1
            for conn in session.target_connections.values():
                if conn.db_type:
                    connection_types[conn.db_type] += 1

        inv.total_sources = total_sources
        inv.total_targets = total_targets
        inv.total_transformations = total_transformations
        inv.transformation_type_counts = dict(tx_type_counts)
        inv.folder_counts = dict(folder_counts)
        inv.connection_types = dict(connection_types)

        return inv
