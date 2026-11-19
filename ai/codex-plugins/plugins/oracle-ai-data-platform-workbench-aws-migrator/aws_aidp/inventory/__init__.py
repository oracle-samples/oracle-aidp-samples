"""Inventory: read-only scans of AWS sources. Each module returns
`{"summary": {...}, "items": [...]}`. `items` is what `plan` consumes."""
from aws_aidp.inventory.manifest import build_manifest, write_manifest

__all__ = ["build_manifest", "write_manifest"]
