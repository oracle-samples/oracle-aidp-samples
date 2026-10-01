"""Scan Semantic Model items. Name and description only.

DAX and TMDL have no AIDP target (spec §3 non-goals), so these are recorded so
the estate is complete and then reported as SKIP.
"""
from __future__ import annotations

from fabric_aidp.inventory.git_workspace import items_of_type


def scan(items, *, log=None) -> dict:
    models = []
    for item in items_of_type(items, "SemanticModel"):
        models.append({
            "name": item.name,
            "logical_id": item.logical_id,
            "folder": item.folder,
            "description": item.description,
        })
        if log:
            log(f"  semantic model {item.name}")
    return {
        "summary": {"semantic_model_count": len(models)},
        "items": {"semantic_models": models},
    }
