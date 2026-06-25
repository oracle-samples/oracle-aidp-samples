"""Extract catalog metadata from a Databricks workspace via the UC REST API.

Runs on the migrator host. Needs only DATABRICKS_HOST + DATABRICKS_TOKEN —
no SQL warehouse, no cluster, no notebook job.

Output: a catalog_pack.json with the per-table descriptors needed by
migrate_catalog.py to reconstruct AIDP DDL.

Usage:
    python $HOME/.aidp-migrator/engine/scripts/extract_catalog_databricks.py \
        --host https://workspace.cloud.databricks.com \
        --token dapi... \
        --catalogs samples,main \
        --schemas-only samples:tpch,samples:nyctaxi \
        --out reports/catalog_pack_$(date +%Y%m%d).json

If --token is omitted, looks up DATABRICKS_TOKEN in the env.
If --host is omitted, looks up DATABRICKS_HOST.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import requests


def _get(url: str, token: str, *, max_attempts: int = 6, **kwargs) -> dict:
    """REST GET with bearer auth + exponential backoff honoring Retry-After.

    Per code review 2026-06-05 (200k-table sweep concern): the original
    single-retry policy exhausts immediately under sustained 429s. We now do
    up to 6 attempts with backoff = min(60s, 2 ** (attempt-1)) seconds,
    honoring `Retry-After` when the server returns it.

    Raises requests.HTTPError on final non-retryable failure.
    """
    headers = {"Authorization": f"Bearer {token}"}
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=headers, timeout=60, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            r = None
        if r is not None and r.status_code < 400:
            return r.json()
        # Retry on 429 + 5xx + transient connection errors
        retryable = r is None or r.status_code == 429 or 500 <= r.status_code < 600
        if not retryable or attempt == max_attempts:
            if r is not None:
                r.raise_for_status()
            assert last_exc is not None
            raise last_exc
        # Backoff: prefer server's Retry-After if present, else exponential
        wait = 2 ** (attempt - 1)
        if r is not None and r.headers.get("Retry-After"):
            try:
                wait = max(wait, int(r.headers["Retry-After"]))
            except ValueError:
                pass
        wait = min(wait, 60)
        time.sleep(wait)
    return {}  # unreachable, satisfies type


def list_catalogs(host: str, token: str) -> list[dict]:
    return _get(f"{host}/api/2.1/unity-catalog/catalogs", token).get("catalogs", [])


def list_schemas(host: str, token: str, catalog_name: str) -> list[dict]:
    url = f"{host}/api/2.1/unity-catalog/schemas?catalog_name={catalog_name}"
    return _get(url, token).get("schemas", [])


def list_tables_in_schema(host: str, token: str, catalog: str, schema: str,
                          max_per_page: int = 50) -> list[dict]:
    """Paginated table listing."""
    out: list[dict] = []
    page_token: str = ""
    while True:
        url = (f"{host}/api/2.1/unity-catalog/tables"
               f"?catalog_name={catalog}&schema_name={schema}"
               f"&max_results={max_per_page}")
        if page_token:
            url += f"&page_token={page_token}"
        resp = _get(url, token)
        out.extend(resp.get("tables", []))
        page_token = resp.get("next_page_token", "")
        if not page_token:
            break
    return out


def get_table_detail(host: str, token: str, full_name: str) -> dict:
    """Get full table descriptor including columns and properties."""
    url = (f"{host}/api/2.1/unity-catalog/tables/{full_name}"
           f"?include_delta_metadata=true&include_browse=true")
    return _get(url, token)


def list_volumes_in_schema(host: str, token: str, catalog: str, schema: str) -> list[dict]:
    """Best-effort volume listing — endpoint may not be available on all workspaces."""
    try:
        url = f"{host}/api/2.1/unity-catalog/volumes?catalog_name={catalog}&schema_name={schema}"
        return _get(url, token).get("volumes", [])
    except requests.HTTPError as e:
        if e.response.status_code in (403, 404):
            return []
        raise


# ---------- main extract ----------

def extract(
    host: str,
    token: str,
    catalog_filter: list[str] | None,
    schema_filter: dict[str, list[str]] | None,
    skip_systems: bool = True,
) -> dict:
    """Walk catalogs -> schemas -> tables and produce a catalog pack.

    catalog_filter: if set, only these catalog names are walked.
    schema_filter: {catalog_name: [schema_names...]} — if a catalog is in here,
        only the listed schemas are walked. Catalogs not in this dict get full sweep.
    skip_systems: drop 'system' and 'information_schema' (they're auto-generated).
    """
    started_at = datetime.utcnow().isoformat() + "Z"
    pack: dict = {
        "format_version": 1,
        "extracted_at": started_at,
        "source_workspace": host,
        "catalogs": [],
        "schemas": [],
        "tables": [],
        "volumes": [],
        "errors": [],
        "stats": {"catalogs": 0, "schemas": 0, "tables_listed": 0,
                  "tables_detailed": 0, "tables_failed": 0, "volumes": 0},
    }

    print(f"[extract] listing catalogs in {host}", flush=True)
    catalogs = list_catalogs(host, token)
    if catalog_filter:
        catalogs = [c for c in catalogs if c.get("name") in catalog_filter]
    if skip_systems:
        # Drop only the literal `system` catalog (UC metadata). `samples` is also
        # tagged SYSTEM_CATALOG but it's user-facing demo data we want to include.
        catalogs = [c for c in catalogs if c.get("name") != "system"]
    print(f"[extract] {len(catalogs)} catalogs to walk: {[c['name'] for c in catalogs]}", flush=True)

    for cat in catalogs:
        cat_name = cat["name"]
        pack["catalogs"].append(cat)
        pack["stats"]["catalogs"] += 1

        try:
            schemas = list_schemas(host, token, cat_name)
        except Exception as e:
            pack["errors"].append({"stage": "list_schemas", "catalog": cat_name, "error": str(e)})
            continue

        if schema_filter and cat_name in schema_filter:
            wanted = set(schema_filter[cat_name])
            schemas = [s for s in schemas if s.get("name") in wanted]
        if skip_systems:
            schemas = [s for s in schemas if s.get("name") != "information_schema"]

        print(f"[extract] catalog {cat_name}: {len(schemas)} schemas", flush=True)

        for sch in schemas:
            sch_name = sch["name"]
            pack["schemas"].append(sch)
            pack["stats"]["schemas"] += 1

            try:
                tables = list_tables_in_schema(host, token, cat_name, sch_name)
            except Exception as e:
                pack["errors"].append({"stage": "list_tables", "schema": f"{cat_name}.{sch_name}",
                                       "error": str(e)})
                continue
            pack["stats"]["tables_listed"] += len(tables)
            print(f"[extract]   {cat_name}.{sch_name}: {len(tables)} tables", flush=True)

            # The list-tables response already includes full columns + properties
            # for most tables — only Delta-shared tables come back with empty
            # `columns`. Per code review 2026-06-05: skip the per-table detail
            # fetch when the list result is already complete, falling back to
            # get_table_detail() only when columns are missing. At 200k tables
            # this collapses ~200k sequential GETs into ~200k/page list calls.
            for t in tables:
                full_name = t.get("full_name") or f"{cat_name}.{sch_name}.{t['name']}"
                has_columns = bool(t.get("columns"))
                try:
                    if has_columns:
                        # List response is already sufficient for the rewriter
                        pack["tables"].append(t)
                    else:
                        # Fall back to per-table fetch — may still return empty
                        # columns for share-based tables, which the rewriter
                        # handles via NO_COLUMNS_VISIBLE skip.
                        detail = get_table_detail(host, token, full_name)
                        pack["tables"].append(detail)
                    pack["stats"]["tables_detailed"] += 1
                except Exception as e:
                    pack["errors"].append({"stage": "get_table", "table": full_name, "error": str(e)})
                    pack["stats"]["tables_failed"] += 1

            # Volumes (UC only)
            try:
                vols = list_volumes_in_schema(host, token, cat_name, sch_name)
                pack["volumes"].extend(vols)
                pack["stats"]["volumes"] += len(vols)
            except Exception as e:
                pack["errors"].append({"stage": "list_volumes", "schema": f"{cat_name}.{sch_name}",
                                       "error": str(e)})

    pack["finished_at"] = datetime.utcnow().isoformat() + "Z"
    return pack


def _parse_schema_filter(s: str | None) -> dict[str, list[str]]:
    """Parse 'cat1:sch1,cat1:sch2,cat2:sch3' -> {cat1:[sch1,sch2], cat2:[sch3]}."""
    if not s:
        return {}
    out: dict[str, list[str]] = {}
    for item in s.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"--schemas-only entries must be 'catalog:schema', got: {item!r}")
        c, sch = item.split(":", 1)
        out.setdefault(c, []).append(sch)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("DATABRICKS_HOST"))
    ap.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN"))
    ap.add_argument("--catalogs", default="", help="Comma-separated catalog names; default = all non-system")
    ap.add_argument("--schemas-only", default="",
                    help="Comma-separated catalog:schema filter (e.g., 'samples:tpch,samples:nyctaxi')")
    ap.add_argument("--skip-systems", action="store_true", default=True,
                    help="Skip 'system' catalog and 'information_schema' (default: on)")
    ap.add_argument("--out", required=True, help="Path to write catalog pack JSON")
    args = ap.parse_args()

    if not args.host or not args.token:
        sys.exit("ERROR: --host and --token (or DATABRICKS_HOST/DATABRICKS_TOKEN env) are required")

    cat_filter = [c.strip() for c in args.catalogs.split(",") if c.strip()] or None
    sch_filter = _parse_schema_filter(args.schemas_only)

    pack = extract(args.host, args.token, cat_filter, sch_filter, args.skip_systems)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(pack, indent=2), encoding="utf-8")

    s = pack["stats"]
    print(f"\n[extract] PACK WRITTEN: {args.out}")
    print(f"[extract] stats: {s['catalogs']} catalogs, {s['schemas']} schemas, "
          f"{s['tables_detailed']}/{s['tables_listed']} tables detailed, "
          f"{s['volumes']} volumes, {s['tables_failed']} failed")
    if pack["errors"]:
        print(f"[extract] {len(pack['errors'])} errors (first 3):")
        for e in pack["errors"][:3]:
            print(f"  {e}")


if __name__ == "__main__":
    main()
