#!/usr/bin/env python3
"""Structure a LIVE AIDP catalog listing for the ``oac-dataset-advisor`` skill.

**Evidence discipline (load-bearing): this helper works ONLY on the live AIDP
catalog** — the actual materialized Delta tables in `<catalog>.<goldSchema>` —
NOT on content-pack `gold/*.yaml` declarations. Pack YAMLs are design-time
intent; they do not prove a table is materialized on this tenant or that its
real columns match. So this helper refuses to read the pack: it consumes a
live-catalog listing (tables → columns) captured from an AIDP catalog query
and emits a structured inventory + cross-table join-key candidates.

Produce the live listing with whatever AIDP catalog access the environment has
(the advisor SKILL.md documents the options), e.g.:

    SHOW TABLES IN fusion_catalog.gold;
    DESCRIBE TABLE fusion_catalog.gold.<table>;   -- for each table

…then shape the result into the input JSON and pipe it here.

Input JSON (stdin or --input):

    {
      "catalog": "fusion_catalog",
      "schema": "gold",
      "tables": {
        "gl_balance":     [{"name": "currency_code", "type": "string"}, ...],
        "supplier_spend": [{"name": "currency_code", "type": "string"}, ...]
      }
    }

Output JSON (stdout):

    {
      "catalog": "fusion_catalog", "schema": "gold",
      "evidence": "live_aidp_catalog",
      "tableCount": 2,
      "tables": [
        {"table": "gold.gl_balance", "name": "gl_balance",
         "columns": [{"name": "...", "type": "..."}], "columnNames": [...]}
      ],
      "joinKeyCandidates": [
        {"column": "currency_code", "tables": ["gl_balance", "supplier_spend"]}
      ]
    }

If the live listing is empty (no tables), the inventory reports zero tables —
the advisor then tells the operator to run `aidp-fusion-autopilot run --mode seed`
first (nothing is materialized yet), rather than inventing tables from YAML.
"""
from __future__ import annotations

import argparse
import json
import sys

# Audit / lineage columns — never useful as a dashboard join key, excluded
# from cross-table overlap detection.
_AUDIT_COLUMNS = frozenset({
    "bronze_extract_ts", "bronze_source_pvo", "silver_built_at", "silver_run_id",
    "gold_built_at", "gold_run_id", "_extract_ts", "_source_pvo", "_run_id",
    "_watermark_used",
})


def build_inventory(live: dict) -> dict:
    """Structure a live-catalog listing into the advisor inventory shape."""
    catalog = live.get("catalog")
    schema = live.get("schema")
    raw_tables = live.get("tables") or {}
    if not isinstance(raw_tables, dict):
        raise SystemExit("input.tables must be an object mapping tableName -> [columns]")

    tables = []
    col_to_tables: dict[str, list[str]] = {}
    for tname in sorted(raw_tables):
        cols_in = raw_tables[tname] or []
        columns = []
        for c in cols_in:
            if isinstance(c, dict):
                columns.append({"name": c.get("name"), "type": c.get("type")})
            else:  # tolerate a bare column-name string
                columns.append({"name": str(c), "type": None})
        names = [c["name"] for c in columns if c.get("name")]
        qualified = f"{schema}.{tname}" if schema else tname
        tables.append({
            "table": qualified,
            "name": tname,
            "columns": columns,
            "columnNames": names,
        })
        for n in names:
            if n and n not in _AUDIT_COLUMNS:
                col_to_tables.setdefault(n, []).append(tname)

    join_keys = [
        {"column": col, "tables": sorted(ts)}
        for col, ts in sorted(col_to_tables.items())
        if len(set(ts)) >= 2
    ]

    return {
        "catalog": catalog,
        "schema": schema,
        "evidence": "live_aidp_catalog",
        "tableCount": len(tables),
        "tables": tables,
        "joinKeyCandidates": join_keys,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Structure a LIVE AIDP catalog listing into the advisor inventory."
    )
    ap.add_argument(
        "--input", default="-",
        help="Path to the live-catalog JSON, or '-' for stdin (default).",
    )
    ns = ap.parse_args(argv)
    text = sys.stdin.read() if ns.input == "-" else open(ns.input, encoding="utf-8").read()
    try:
        live = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid live-catalog JSON: {exc}") from exc
    print(json.dumps(build_inventory(live), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
