#!/usr/bin/env python3
"""Content-pack capability ("what's BUILDABLE") for the oac-dataset-advisor.

⚠️ This is NOT live evidence. It reads the active content pack's gold/silver
node YAMLs to answer a different question than `catalog_inventory.py`:

  * `catalog_inventory.py`  -> what gold tables ACTUALLY EXIST live (evidence).
  * `pack_capability.py`    -> what gold marts the pack COULD build if seeded
                               (design-time menu).

Used ONLY when the live gold layer is empty or doesn't cover the request, to
route the operator between:

  * **seed** — the pack already declares a mart/columns that cover the want, so
    `aidp-fusion-autopilot run --mode seed` would materialize it; vs.
  * **gap**  — even the pack can't serve it -> a new mart is needed
    (mart-authoring skill).

NEVER use this output to claim a table exists or to bind an OAC dataset — only
the live catalog can do that ([[feedback_live_catalog_is_evidence]]).

Output (stdout JSON):

    {
      "pack": "fusion-finance-starter", "goldSchema": "gold", "silverSchema": "silver",
      "buildableMarts": [
        {"id": "supplier_spend", "layer": "gold", "table": "gold.supplier_spend",
         "grain": ["vendor_id","currency_code"], "implementation": "sql",
         "dependsOn": {"bronze": ["ap_invoices"], "silver": ["dim_supplier"]},
         "columns": [{"name","type","pii"}...], "highPiiColumns": [...]}
      ],
      "joinKeyCandidates": [{"column": "currency_code", "marts": [...]}]
    }

Usage:
    python3 pack_capability.py [--bundle bundle.yaml] [--pack <name|path>] [--include-silver]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

_AUDIT_COLUMNS = frozenset({
    "bronze_extract_ts", "bronze_source_pvo", "silver_built_at", "silver_run_id",
    "gold_built_at", "gold_run_id", "_extract_ts", "_source_pvo", "_run_id",
    "_watermark_used",
})

_INSTALLED_PACKS = (
    Path(__file__).resolve().parents[2]
    / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "content_packs"
)


def _resolve_pack_dir(bundle_path: Path, pack_override: str | None) -> Path:
    if pack_override:
        p = Path(pack_override)
        if (p / "pack.yaml").exists():
            return p
        if (_INSTALLED_PACKS / pack_override / "pack.yaml").exists():
            return _INSTALLED_PACKS / pack_override
        raise SystemExit(f"pack {pack_override!r} not found as a path or installed pack")
    name = None
    if bundle_path.exists():
        raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8")) or {}
        name = (raw.get("contentPack") or {}).get("name")
    if name and (_INSTALLED_PACKS / name / "pack.yaml").exists():
        return _INSTALLED_PACKS / name
    installed = [d for d in _INSTALLED_PACKS.iterdir() if (d / "pack.yaml").exists()] \
        if _INSTALLED_PACKS.exists() else []
    if len(installed) == 1:
        return installed[0]
    raise SystemExit(
        "could not resolve content pack — set bundle.contentPack.name or pass --pack "
        f"(installed: {[d.name for d in installed]})"
    )


def _grain(node: dict) -> list[str]:
    refresh = node.get("refresh") or {}
    for mode in ("incremental", "seed"):
        nk = (refresh.get(mode) or {}).get("naturalKey")
        if nk:
            return list(nk)
    return list(node.get("naturalKey") or [])


def _columns(node: dict) -> list[dict]:
    cols = ((node.get("outputSchema") or {}).get("columns")) or []
    return [{"name": c.get("name"), "type": c.get("type"), "pii": c.get("pii", "none")} for c in cols]


def _depends(node: dict) -> dict:
    dep = node.get("dependsOn") or {}
    return {
        layer: [d.get("id") for d in (dep.get(layer) or []) if isinstance(d, dict)]
        for layer in ("bronze", "silver", "gold") if dep.get(layer)
    }


def _load_layer(pack_dir: Path, layer: str, schema: str) -> list[dict]:
    marts: list[dict] = []
    layer_dir = pack_dir / layer
    if not layer_dir.is_dir():
        return marts
    for yml in sorted(layer_dir.glob("*.yaml")):
        node = yaml.safe_load(yml.read_text(encoding="utf-8")) or {}
        if not node.get("id"):
            continue
        cols = _columns(node)
        target = node.get("target") or node.get("id")
        marts.append({
            "id": node["id"],
            "layer": node.get("layer", layer),
            "table": f"{schema}.{target}",
            "grain": _grain(node),
            "implementation": (node.get("implementation") or {}).get("type", "unknown"),
            "dependsOn": _depends(node),
            "columns": cols,
            "highPiiColumns": [c["name"] for c in cols if c.get("pii") == "high"],
        })
    return marts


def build_capability(bundle_path: Path, pack_override: str | None, include_silver: bool) -> dict:
    pack_dir = _resolve_pack_dir(bundle_path, pack_override)
    gold_schema, silver_schema = "gold", "silver"
    if bundle_path.exists():
        aidp = (yaml.safe_load(bundle_path.read_text(encoding="utf-8")) or {}).get("aidp") or {}
        gold_schema = aidp.get("goldSchema", gold_schema)
        silver_schema = aidp.get("silverSchema", silver_schema)

    marts = _load_layer(pack_dir, "gold", gold_schema)
    if include_silver:
        marts += _load_layer(pack_dir, "silver", silver_schema)

    col_to_marts: dict[str, list[str]] = {}
    for m in marts:
        for c in m["columns"]:
            n = c["name"]
            if n and n not in _AUDIT_COLUMNS:
                col_to_marts.setdefault(n, []).append(m["id"])
    join_keys = [
        {"column": col, "marts": sorted(ms)}
        for col, ms in sorted(col_to_marts.items()) if len(ms) >= 2
    ]

    return {
        "pack": pack_dir.name,
        "evidence": "content_pack_declaration_NOT_live",
        "goldSchema": gold_schema,
        "silverSchema": silver_schema,
        "buildableMarts": marts,
        "joinKeyCandidates": join_keys,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Emit the content pack's BUILDABLE marts (design-time, NOT live evidence)."
    )
    ap.add_argument("--bundle", type=Path, default=Path("bundle.yaml"))
    ap.add_argument("--pack", default=None, help="Pack name or path (overrides bundle.contentPack.name).")
    ap.add_argument("--include-silver", action="store_true")
    ns = ap.parse_args(argv)
    print(json.dumps(build_capability(ns.bundle, ns.pack, ns.include_silver), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
