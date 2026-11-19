"""Read-only scans of a Fabric Git export.

Each source module exposes `scan(items, **kw) -> {"summary": {...}, "items": {...}}`.
`items` is what `plan` consumes.
"""
from fabric_aidp.inventory.git_workspace import (
    FabricItem, NotAFabricExport, UnreadableItem, discover_items, items_of_type,
)
from fabric_aidp.inventory.manifest import (
    ALL_SOURCES, build_manifest, summarize, write_manifest,
)

__all__ = [
    "FabricItem", "NotAFabricExport", "UnreadableItem", "discover_items",
    "items_of_type",
    "ALL_SOURCES", "build_manifest", "summarize", "write_manifest",
]
