#!/usr/bin/env python3
"""Change-strategy planner for the ``mart-author`` skill.

Makes the **safety-critical** authoring decision executable and testable rather
than leaving it to prose: given where each field the business logic needs is
sourced (existing materialized layer / Fusion PVO source only / nowhere), pick
the **lowest-cost, additive, non-destructive** change on the ladder and emit a
node spec with the medallion invariants pre-stamped.

Hard rules this enforces (the "don't touch living delta" contract):
  * NEVER alter an existing node's grain/natural-key or rewrite an existing
    bronze/silver table (they may be terabytes). New bronze is ADDITIVE only.
  * Reading existing tables is fine; reprocessing them is not — so a new gold
    node over existing silver/bronze is preferred over a new extract.
  * A field that exists nowhere (not even at the PVO source) is a HARD GAP —
    do not fabricate a way to serve it.

The change-strategy ladder (cheapest first):
  rung_3_add_column           — derive from columns already in ONE existing
                                table's sources; additive column (no grain change)
  rung_1_new_gold             — new aggregate/business mart over EXISTING bronze/silver
  rung_2_new_silver           — new conformed/typed node over EXISTING bronze
  rung_4_new_bronze_plus_node — a needed raw field isn't extracted yet → new
                                bronze_extract (additive) + downstream node

Input JSON (stdin or --input):

    {
      "request": {
        "id": "supplier_payment_efficiency",
        "targetLayer": "gold",                 # gold | silver
        "grain": ["supplier_number","currency_code"],
        "addToExisting": null,                 # or "gold.supplier_spend" to request rung 3
        "isAggregate": true,                   # aggregate marts need currency-in-grain
        "columns": [                           # business output columns (excl. audit)
          {"name":"supplier_name","pii":"medium"},
          {"name":"currency_code","pii":"none"},
          {"name":"on_time_pct","pii":"none"}
        ]
      },
      "fields": [                              # where each REQUIRED source field lives
        {"name":"supplier_number","source":"existing_silver","table":"silver.dim_supplier"},
        {"name":"total_paid","source":"existing_gold","table":"gold.supplier_spend"},
        {
          "name":"promised_date",
          "source":"pvo_only",
          "pvo":"InvoiceHeaderExtractPVO",
          "sourceColumn":"ApInvoicesPromisedDate",
          "pvoClassification":"transaction_change_feed",
          "metadataLastUpdateColumns":["ApInvoicesLastUpdateDate"],
          "watermarkColumn":"ApInvoicesLastUpdateDate",
          "businessSemanticsConfirmed": true
        },
        {"name":"currency_code","source":"existing_bronze","table":"bronze.ap_invoices"}
      ]
    }

Output JSON: {decision, reason, blastRadius, requiresNewBronze, missingFields,
              warnings, touchesLivingDelta, pvoClassifications, nodeSpecs:[...]}.
"""
from __future__ import annotations

import argparse
import json
import sys

# Audit columns every authored node must carry (SOX trail). Stamped onto the
# node spec automatically so the SQL scaffolder can't forget them.
_AUDIT_BY_LAYER = {
    "bronze": ["_extract_ts", "_source_pvo", "_run_id", "_watermark_used"],
    "silver": ["bronze_extract_ts", "bronze_source_pvo", "silver_built_at", "silver_run_id"],
    "gold": ["gold_built_at", "gold_run_id"],
}

# A currency column must appear in the grain of any amount aggregate (no
# single-currency-summed marts on a multi-currency tenant).
_CURRENCY_HINTS = ("currency_code", "currency")

_VALID_SOURCES = {"existing_gold", "existing_silver", "existing_bronze", "pvo_only", "missing"}

_PVO_CLASS_ALIASES = {
    "transaction": "transaction_change_feed",
    "transaction_change_feed": "transaction_change_feed",
    "transaction-change-feed": "transaction_change_feed",
    "change_feed": "transaction_change_feed",
    "change-feed": "transaction_change_feed",
    "snapshot": "snapshot_config",
    "snapshot_config": "snapshot_config",
    "snapshot-config": "snapshot_config",
    "config": "snapshot_config",
    "period_windowable": "period_windowable_snapshot",
    "period-windowable": "period_windowable_snapshot",
    "period_windowable_snapshot": "period_windowable_snapshot",
    "period-windowable-snapshot": "period_windowable_snapshot",
}

_PVO_CLASS_LABELS = {
    "transaction_change_feed": "transaction/change-feed",
    "snapshot_config": "snapshot/config",
    "period_windowable_snapshot": "period-windowable snapshot",
}

_PVO_INCREMENTAL_RECOMMENDATION = {
    "transaction_change_feed": True,
    "snapshot_config": False,
    "period_windowable_snapshot": False,
}


def _normalize_pvo_classification(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower().replace(" ", "_").replace("/", "_")
    return _PVO_CLASS_ALIASES.get(normalized)


def _boolish(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _string_listish(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        items = value
    elif isinstance(value, tuple):
        items = list(value)
    else:
        return []

    names: list[str] = []
    for item in items:
        if isinstance(item, str) and item:
            names.append(item)
        elif isinstance(item, dict):
            name = item.get("name") or item.get("column") or item.get("columnName")
            if name:
                names.append(str(name))
    return names


def _metadata_last_update_columns(fields: list[dict]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for f in fields:
        for key in (
            "metadataLastUpdateColumns",
            "biccLastUpdateColumns",
            "isLastUpdateDateColumns",
        ):
            for name in _string_listish(f.get(key)):
                if name not in seen:
                    seen.add(name)
                    names.append(name)
    return names


def _classify_pvo_only_fields(pvo_only: list[dict]) -> tuple[dict[str, dict], list[str]]:
    """Validate author-supplied rung-4 PVO classification evidence."""
    grouped: dict[str, list[dict]] = {}
    for f in pvo_only:
        pvo_id = f.get("pvo") or f.get("name") or "<unknown-pvo>"
        grouped.setdefault(str(pvo_id), []).append(f)

    classifications: dict[str, dict] = {}
    warnings: list[str] = []
    for pvo_id, fields in grouped.items():
        raw_class = next(
            (
                f.get("pvoClassification")
                or f.get("pvo_classification")
                or f.get("pvoClass")
                for f in fields
                if (
                    f.get("pvoClassification")
                    or f.get("pvo_classification")
                    or f.get("pvoClass")
                )
            ),
            None,
        )
        pvo_class = _normalize_pvo_classification(raw_class)
        info = {
            "classification": pvo_class,
            "classificationLabel": _PVO_CLASS_LABELS.get(pvo_class),
            "incrementalCapableRecommendation": (
                _PVO_INCREMENTAL_RECOMMENDATION.get(pvo_class)
                if pvo_class is not None
                else None
            ),
            "requiresExplicitApproval": False,
            "requiresExtractWindowPolicy": False,
        }

        if raw_class is None:
            warnings.append(
                f"PVO {pvo_id}: missing pvoClassification for new bronze extract — "
                "classify as transaction_change_feed, snapshot_config, or "
                "period_windowable_snapshot before writing bronze YAML."
            )
            info["requiresExplicitApproval"] = True
        elif pvo_class is None:
            warnings.append(
                f"PVO {pvo_id}: unsupported pvoClassification {raw_class!r}; use "
                "transaction_change_feed, snapshot_config, or "
                "period_windowable_snapshot."
            )
            info["requiresExplicitApproval"] = True
        elif pvo_class == "transaction_change_feed":
            metadata_cols = _metadata_last_update_columns(fields)
            metadata_evidence = bool(metadata_cols) or any(
                _boolish(
                    f.get("hasReliableLastUpdateDate")
                    or f.get("reliableLastUpdateDate")
                    or f.get("isLastUpdateDateReliable")
                )
                for f in fields
            )
            watermark_col = next(
                (
                    f.get("lastUpdateDateColumn")
                    or f.get("watermarkColumn")
                    or f.get("last_update_date_column")
                    for f in fields
                    if (
                        f.get("lastUpdateDateColumn")
                        or f.get("watermarkColumn")
                        or f.get("last_update_date_column")
                    )
                ),
                None,
            )
            if watermark_col is None and len(metadata_cols) == 1:
                watermark_col = metadata_cols[0]
            semantics_confirmed = any(
                _boolish(
                    f.get("businessSemanticsConfirmed")
                    or f.get("changesAdvanceWatermark")
                    or f.get("cdcSemanticsConfirmed")
                )
                for f in fields
            )
            info["metadataLastUpdateColumns"] = metadata_cols
            info["lastUpdateDateColumn"] = watermark_col
            info["businessSemanticsConfirmed"] = semantics_confirmed
            if not metadata_evidence or not watermark_col:
                warnings.append(
                    f"PVO {pvo_id}: transaction_change_feed requires live BICC "
                    "metadata evidence of an isLastUpdateDate column and the "
                    "chosen watermarkColumn before setting incrementalCapable: true. "
                    "Do not infer CDC safety from column naming alone."
                )
            elif len(metadata_cols) > 1 and not any(
                f.get("lastUpdateDateColumn")
                or f.get("watermarkColumn")
                or f.get("last_update_date_column")
                for f in fields
            ):
                warnings.append(
                    f"PVO {pvo_id}: multiple BICC isLastUpdateDate columns were "
                    "reported; choose an explicit watermarkColumn."
                )
            if not semantics_confirmed:
                warnings.append(
                    f"PVO {pvo_id}: transaction_change_feed also requires "
                    "business semantics confirmation that meaningful source "
                    "changes advance the chosen watermark column."
                )
        elif pvo_class == "snapshot_config":
            warnings.append(
                f"PVO {pvo_id}: snapshot_config should use incrementalCapable: false; "
                "incremental runs will full-pull this bronze source, then MERGE/"
                "payload-diff. Stop for approval if the source is high volume."
            )
            if any(_boolish(f.get("highVolume") or f.get("isHighVolume")) for f in fields):
                info["requiresExplicitApproval"] = True
        elif pvo_class == "period_windowable_snapshot":
            has_policy = any(
                _boolish(f.get("hasExtractWindowPolicy") or f.get("extractWindowPolicy"))
                for f in fields
            )
            approved = any(
                _boolish(f.get("explicitUserApproval") or f.get("approvedFullPull"))
                for f in fields
            )
            has_safety_path = has_policy or approved
            info["requiresExtractWindowPolicy"] = not has_safety_path
            info["requiresExplicitApproval"] = not has_safety_path
            if not has_safety_path:
                warnings.append(
                    f"PVO {pvo_id}: period_windowable_snapshot needs an "
                    "extract_window policy or explicit user approval before "
                    "creating a potential high-volume full-pull "
                    "bronze extract."
                )

        classifications[pvo_id] = info

    return classifications, warnings


def _refresh_for(layer: str, grain: list, is_aggregate: bool) -> dict:
    """Default refresh strategy per the medallion taxonomy, with a reason."""
    if layer == "gold" and is_aggregate:
        return {
            "seed": {"strategy": "replace"},
            "incremental": {"strategy": "replace"},
            "reason": "Aggregate grain — partial MERGE leaves stale rows on status/key flips; "
                      "replace each cycle.",
        }
    # Row-grain node (silver dim / row-grain gold): merge on the natural key.
    return {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "naturalKey": list(grain),
            "reason": "Row-grain node — NULL-safe MERGE on the natural key.",
        },
    }


def _node_spec(request: dict, layer: str, depends: dict) -> dict:
    cols = [{"name": c.get("name"), "pii": c.get("pii", "REQUIRED-set-explicitly")}
            for c in (request.get("columns") or [])]
    # Stamp mandatory audit columns (PII none) so they're never omitted.
    for ac in _AUDIT_BY_LAYER.get(layer, []):
        if ac not in [c["name"] for c in cols]:
            cols.append({"name": ac, "pii": "none"})
    return {
        "id": request.get("id"),
        "layer": layer,
        "target": request.get("id"),
        "implementation": "bronze_extract" if layer == "bronze" else "sql",
        "dependsOn": depends,
        "grain": list(request.get("grain") or []),
        "refresh": _refresh_for(layer, request.get("grain") or [], bool(request.get("isAggregate"))),
        "columns": cols,
    }


def plan(payload: dict) -> dict:
    request = payload.get("request") or {}
    fields = payload.get("fields") or []
    warnings: list[str] = []

    for f in fields:
        if f.get("source") not in _VALID_SOURCES:
            raise SystemExit(f"field {f.get('name')!r}: source must be one of {sorted(_VALID_SOURCES)}")

    missing = [f["name"] for f in fields if f.get("source") == "missing"]
    pvo_only = [f for f in fields if f.get("source") == "pvo_only"]
    target_layer = request.get("targetLayer", "gold")
    add_to = request.get("addToExisting")

    # Currency-in-grain invariant for aggregates.
    if request.get("isAggregate"):
        grain = [g.lower() for g in (request.get("grain") or [])]
        if not any(any(h in g for h in _CURRENCY_HINTS) for g in grain):
            warnings.append(
                "aggregate mart grain has no currency column — add currency_code to the "
                "grain (currency-in-grain invariant) before authoring."
            )

    # 1. Hard gap: a needed field exists nowhere (not even at source).
    if missing:
        return {
            "decision": "hard_gap",
            "reason": "field(s) not available in any existing table NOR at the Fusion PVO source",
            "blastRadius": "none — cannot serve this request as specified",
            "requiresNewBronze": False,
            "missingFields": sorted(missing),
            "warnings": warnings,
            "pvoClassifications": {},
            "touchesLivingDelta": False,
            "nodeSpecs": [],
        }

    # Build dependsOn from where existing fields come from.
    depends: dict[str, list] = {}
    for f in fields:
        src = f.get("source")
        layer = {"existing_gold": "gold", "existing_silver": "silver",
                 "existing_bronze": "bronze"}.get(src)
        if layer:
            dep_id = (f.get("table") or "").split(".")[-1] or f.get("name")
            depends.setdefault(layer, [])
            if dep_id not in [d.get("id") for d in depends[layer]]:
                depends[layer].append({"id": dep_id, "role": "lookup"})

    # 2. Rung 4: a raw field is only at the PVO -> new ADDITIVE bronze extract + node.
    if pvo_only:
        bronze_ids = sorted({f.get("pvo") or f["name"] for f in pvo_only})
        pvo_classifications, pvo_warnings = _classify_pvo_only_fields(pvo_only)
        warnings.extend(pvo_warnings)
        bronze_specs = []
        for f in pvo_only:
            pvo_id = str(f.get("pvo") or f["name"])
            pvo_info = pvo_classifications.get(pvo_id, {})
            bronze_specs.append({
                "id": f["name"],
                "layer": "bronze",
                "target": f["name"],
                "implementation": "bronze_extract",
                "pvo": f.get("pvo"),
                "sourceColumn": f.get("sourceColumn"),
                "pvoClassification": pvo_info.get("classification"),
                "incrementalCapableRecommendation": (
                    pvo_info.get("incrementalCapableRecommendation")
                ),
                "note": "NEW additive bronze extract — never alters an existing bronze table.",
            })
        depends.setdefault("bronze", [])
        for f in pvo_only:
            depends["bronze"].append({"id": f["name"], "role": "primary"})
        downstream = _node_spec(request, target_layer, depends)
        return {
            "decision": "rung_4_new_bronze_plus_node",
            "reason": f"raw field(s) not yet extracted; add additive bronze extract(s) for {bronze_ids} "
                      f"then a {target_layer} node over them + existing tables",
            "blastRadius": "one new bronze extract per missing field + one new downstream node; "
                           "existing tables untouched",
            "requiresNewBronze": True,
            "missingFields": [],
            "warnings": warnings,
            "pvoClassifications": pvo_classifications,
            "touchesLivingDelta": False,
            "nodeSpecs": [*bronze_specs, downstream],
        }

    # 3. Rung 3: change an existing single silver/gold mart in place (add / remove
    #    / rewrite a column) via a guarded same-id full replacement (replaceNode).
    if add_to:
        return {
            "decision": "rung_3_add_column",
            "reason": f"all source fields already feed {add_to}; ship a guarded same-id "
                      "full replacement (replaceNode) — id unchanged, identity preserved",
            "blastRadius": f"same-id replacement of {add_to} — id unchanged so no "
                           "dependsOn repointing; identity (layer/target/dependsOn/refresh) "
                           "must equal base",
            "requiresNewBronze": False,
            "missingFields": [],
            "warnings": [
                *warnings,
                f"keep {add_to}'s identity (layer, target, dependsOn edge set, refresh) "
                "equal to base — a replaceNode that changes any of those fails AIDPF-2065; "
                "author a new node id for an identity change instead.",
            ],
            "pvoClassifications": {},
            "touchesLivingDelta": False,
            "nodeSpecs": [{
                "replaceNode": add_to,
                "columns": list(request.get("columns") or []),
                "mechanism": (
                    "guarded same-id full replacement: ship a complete new "
                    f"<layer>/{add_to}.yaml + {add_to}.sql plus an acknowledged "
                    "`overrides: { <layer>/" + add_to + ": { replaceNode: { reason, "
                    "forkedFrom: { sqlSha256, contractSha256, packVersion } } } }` "
                    "block. Compute forkedFrom from the base via "
                    "`content-pack refresh-fork`. Add/remove/rewrite all use this one "
                    "shape; identity must equal base (else AIDPF-2065)."
                ),
            }],
        }

    # 1/2. New node over existing materialized data (cheapest standalone build).
    decision = "rung_2_new_silver" if target_layer == "silver" else "rung_1_new_gold"
    return {
        "decision": decision,
        "reason": f"all source fields already materialized; build a new {target_layer} node "
                  "over existing bronze/silver (read-only on existing tables)",
        "blastRadius": "one new table; existing bronze/silver read but never rewritten",
        "requiresNewBronze": False,
        "missingFields": [],
        "warnings": warnings,
        "pvoClassifications": {},
        "touchesLivingDelta": False,
        "nodeSpecs": [_node_spec(request, target_layer, depends)],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Pick the lowest-cost additive mart change + emit node spec.")
    ap.add_argument("--input", default="-", help="Path to request JSON, or '-' for stdin.")
    ns = ap.parse_args(argv)
    text = sys.stdin.read() if ns.input == "-" else open(ns.input, encoding="utf-8").read()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid request JSON: {exc}") from exc
    print(json.dumps(plan(payload), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
