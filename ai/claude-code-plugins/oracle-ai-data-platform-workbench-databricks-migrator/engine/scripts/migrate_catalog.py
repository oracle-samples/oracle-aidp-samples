"""Replay a Databricks catalog pack onto AIDP.

Reads catalog_pack.json produced by extract_catalog_databricks.py, transforms
each table dict via catalog_ddl_rewriter, and replays the CREATE SCHEMA +
CREATE TABLE statements on AIDP via AIDPSession.

Output:
  - catalog_manifest.json: { run_id, table_map, schema_map, applied_rules,
                              skipped[], errors[] } — consumed by job_migrate.py
                              at process_job() entry to rewrite cell-level
                              table references.
  - catalog_migration_report.md: human-readable summary.

Usage:
    python scripts/migrate_catalog.py \
        --pack reports/catalog_pack_test.json \
        --aidp-cluster <cluster_id> \
        --aidp-lake <lake_ocid> \
        --aidp-workspace <workspace_id> \
        --aidp-profile AIDP_API \
        --out-manifest reports/catalog_manifest.json \
        --out-report   reports/catalog_migration_report.md \
        [--dry-run]
        [--catalog-map samples=samples,main=main]
        [--bucket-map config/bucket_mapping.csv]
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from catalog_ddl_rewriter import (
    rewrite_table_ddl, schema_create_sql, UnsupportedDDL, RewriteResult,
)


def _load_bucket_map(path: str | None) -> dict[str, tuple[str, str]]:
    """Load S3-bucket -> (oci-bucket, oci-namespace) mapping from CSV."""
    if not path:
        return {}
    out: dict[str, tuple[str, str]] = {}
    p = Path(path)
    if not p.exists():
        return out
    with p.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            s3 = (row.get("s3_bucket") or row.get("source_bucket") or "").strip()
            oci_b = (row.get("oci_bucket") or row.get("target_bucket") or "").strip()
            oci_ns = (row.get("oci_namespace") or row.get("target_namespace") or "").strip()
            if s3 and oci_b and oci_ns:
                out[s3] = (oci_b, oci_ns)
    return out


def _parse_catalog_map(s: str | None) -> dict[str, str]:
    """Parse 'samples=samples,main=tpcds' -> dict."""
    if not s:
        return {}
    out: dict[str, str] = {}
    for item in s.split(","):
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _ensure_aidp_schemas(pack: dict, catalog_map: dict[str, str],
                        flatten_strategy: str, default_catalog: str = "default") -> list[str]:
    """Compute the unique AIDP schema names we need to create.

    For catalog-remap these are catalog-qualified ("catalog.schema"); for the
    flatten strategies they are bare schema names.
    """
    out: set[str] = set()
    for sch in pack.get("schemas", []):
        cat = sch["catalog_name"]
        s = sch["name"]
        if flatten_strategy == "catalog-remap":
            cat_mapped = catalog_map.get(cat, default_catalog)
            out.add(f"{cat_mapped}.{s}")
        elif flatten_strategy == "schema-prefix":
            cat_mapped = catalog_map.get(cat, cat)
            out.add(f"{cat_mapped}_{s}")
        else:
            out.add(s)
    return sorted(out)


def _extract_results(outs) -> list | None:
    """Pull our [["OK"|"ERR", ...], ...] results list out of AIDP cell outputs.

    Unwraps the AIDP output wrapper ([{"type":"TEXT_PLAIN","value":"..."}]) and
    scans for the printed JSON results line, validating its shape so the wrapper
    itself (a list of dicts) is never mistaken for results — that mismatch was
    the source of the `KeyError: 0` during table creation.
    """
    from context_tools import _unwrap_aidp_text
    for o in outs:
        if not (isinstance(o, dict) and o.get("text")):
            continue
        text = _unwrap_aidp_text(o["text"]).strip()
        for line in reversed(text.split("\n")):
            line = line.strip()
            if not line.startswith("["):
                continue
            try:
                cand = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(cand, list) and (
                not cand
                or (isinstance(cand[0], list) and cand[0] and cand[0][0] in ("OK", "ERR"))
            ):
                return cand
    return None


async def replay(pack_path: str, args) -> dict:
    """Read pack, rewrite, replay on AIDP."""
    pack = json.loads(Path(pack_path).read_text(encoding="utf-8"))
    print(f"[migrate] loaded pack: {len(pack.get('tables',[]))} tables across "
          f"{len(pack.get('schemas',[]))} schemas in {len(pack.get('catalogs',[]))} catalogs")

    bucket_map = _load_bucket_map(args.bucket_map)
    catalog_map = _parse_catalog_map(args.catalog_map)

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    manifest: dict = {
        "format_version": 1,
        "run_id": run_id,
        "started_at": datetime.utcnow().isoformat() + "Z",
        "source_workspace": pack.get("source_workspace"),
        "aidp_cluster": args.aidp_cluster,
        "aidp_lake": args.aidp_lake,
        "aidp_workspace": args.aidp_workspace,
        "flatten_strategy": args.flatten_strategy,
        "location_strategy": args.location_strategy,
        "target_using": args.target_using,
        "dry_run": args.dry_run,
        "catalog_map": catalog_map,
        "schemas_created": [],
        "tables_created": [],
        "table_map": {},          # dbx_full_name -> aidp_full_name
        "schema_map": {},         # dbx (cat.sch) -> aidp schema name
        "skipped": [],
        "errors": [],
        "applied_rules": [],
        "stats": {"schemas_created": 0, "tables_replayed": 0, "tables_failed": 0,
                  "tables_skipped": 0, "dropped_property_count": 0},
    }

    # AIDP-side session: lazy-imported so dry-run doesn't need it
    session = None
    if not args.dry_run:
        from aidp_executor import AIDPSession
        session = AIDPSession(
            lake_ocid=args.aidp_lake,
            workspace_id=args.aidp_workspace,
            cluster_id=args.aidp_cluster,
            oci_profile=args.aidp_profile,
            session_name="aidp_catalog_migration",
        )
        print(f"[migrate] connecting to AIDP cluster {args.aidp_cluster}...")
        await session.connect()
        print(f"[migrate] AIDP session ready")

    # ── Step 1: schemas ──
    # IMPORTANT (AIDP-specific): batch all schemas into ONE WebSocket message.
    # Per-statement WS calls + session.close() causes the Hive metastore commit
    # to be discarded on session teardown. Single-batch keeps the entire DDL
    # series in one execution context that commits atomically. Verified
    # 2026-06-04: per-statement loop fails to persist, single-batch persists.
    schemas_to_create = _ensure_aidp_schemas(pack, catalog_map, args.flatten_strategy, args.default_catalog)
    print(f"[migrate] creating {len(schemas_to_create)} schemas (batched): {schemas_to_create}")
    if not args.dry_run and schemas_to_create:
        batch_py = "import json\n_results = []\n"
        for sname in schemas_to_create:
            sql = schema_create_sql(sname)
            sql_py = sql.replace('"', '\\"')
            batch_py += (f'try:\n'
                         f'    spark.sql("{sql_py}")\n'
                         f'    _results.append(("OK", "{sname}"))\n'
                         f'except Exception as _e:\n'
                         f'    _results.append(("ERR", "{sname}", str(_e)[:200]))\n')
        batch_py += 'print(json.dumps(_results))\n'
        try:
            exec_result = await session.execute(batch_py, timeout=300)
            outs = exec_result.get("outputs", [])
            parsed = _extract_results(outs)
            for r in (parsed or []):
                if r[0] == "OK":
                    manifest["schemas_created"].append(r[1])
                    manifest["stats"]["schemas_created"] += 1
                    print(f"[migrate] schema OK: {r[1]}")
                else:
                    manifest["errors"].append({"stage": "schema", "schema": r[1],
                                               "error": r[2] if len(r) > 2 else ""})
        except Exception as e:
            manifest["errors"].append({"stage": "schema_batch", "error": f"{type(e).__name__}: {e}"})
    elif args.dry_run:
        for sname in schemas_to_create:
            sql = schema_create_sql(sname)
            print(f"[DRY-RUN] {sql}")
            manifest["schemas_created"].append(sname)
            manifest["stats"]["schemas_created"] += 1

    # Track dbx_schema -> aidp_schema in the manifest map
    for sch in pack.get("schemas", []):
        full = f"{sch['catalog_name']}.{sch['name']}"
        cat_mapped = catalog_map.get(sch['catalog_name'], sch['catalog_name'])
        aidp_sch = (f"{cat_mapped}_{sch['name']}"
                    if args.flatten_strategy == "schema-prefix"
                    else sch["name"])
        manifest["schema_map"][full] = aidp_sch

    # ── Step 2: tables ──
    # Rewrite all tables locally first (pure functions, no I/O).
    tables = pack.get("tables", [])
    print(f"[migrate] rewriting {len(tables)} tables (local pass)...")
    pending_creates: list[tuple[str, str, str]] = []  # (dbx_full, aidp_full, sql)
    for i, t in enumerate(tables, start=1):
        full = t.get("full_name") or "?"
        try:
            res = rewrite_table_ddl(
                dbx_table=t,
                catalog_map=catalog_map,
                bucket_map=bucket_map,
                target_using=args.target_using,
                flatten_strategy=args.flatten_strategy,
                location_strategy=args.location_strategy,
                default_catalog=args.default_catalog,
            )
        except UnsupportedDDL as e:
            manifest["skipped"].append({"table": full, "reason": str(e), "rule_id": e.rule_id})
            manifest["stats"]["tables_skipped"] += 1
            print(f"[{i}/{len(tables)}] SKIP {full}: {e}")
            continue
        except Exception as e:
            manifest["errors"].append({"stage": "rewrite", "table": full,
                                       "error": f"{type(e).__name__}: {e}"})
            manifest["stats"]["tables_failed"] += 1
            print(f"[{i}/{len(tables)}] REWRITE_ERR {full}: {e}")
            continue
        manifest["stats"]["dropped_property_count"] += len(res.dropped_properties)
        manifest["applied_rules"].extend([asdict(r) for r in res.applied_rules])
        pending_creates.append((full, res.aidp_full_name, res.create_table_sql))

    print(f"[migrate] {len(pending_creates)} CREATE TABLE statements ready")

    if args.dry_run:
        for full, aidp_full, sql in pending_creates:
            print(f"[DRY] {full} -> {aidp_full}")
            print(sql)
            print()
            manifest["tables_created"].append(aidp_full)
            manifest["table_map"][full] = aidp_full
            manifest["stats"]["tables_replayed"] += 1
    elif pending_creates:
        # AIDP-specific: batch all CREATE TABLE into ONE WS message (see schema batch above).
        # Chunk by ~50 to avoid extreme single-call sizes.
        CHUNK_SIZE = 25
        for chunk_idx in range(0, len(pending_creates), CHUNK_SIZE):
            chunk = pending_creates[chunk_idx:chunk_idx + CHUNK_SIZE]
            batch_py = "import json\n_results = []\n"
            for full, aidp_full, sql in chunk:
                # Use triple-quoted-string-friendly encoding
                sql_repr = repr(sql)
                aidp_repr = repr(aidp_full)
                full_repr = repr(full)
                batch_py += (f'try:\n'
                             f'    spark.sql({sql_repr})\n'
                             f'    _results.append(("OK", {full_repr}, {aidp_repr}))\n'
                             f'except Exception as _e:\n'
                             f'    _results.append(("ERR", {full_repr}, {aidp_repr}, str(_e)[:300]))\n')
            batch_py += 'print(json.dumps(_results))\n'
            try:
                exec_result = await session.execute(batch_py, timeout=600)
                outs = exec_result.get("outputs", [])
                parsed = _extract_results(outs)
                if parsed is None:
                    print(f"[migrate] chunk {chunk_idx//CHUNK_SIZE + 1}: WARN no JSON output")
                else:
                    for r in parsed:
                        if r[0] == "OK":
                            manifest["tables_created"].append(r[2])
                            manifest["table_map"][r[1]] = r[2]
                            manifest["stats"]["tables_replayed"] += 1
                            print(f"  OK {r[1]} -> {r[2]}")
                        else:
                            manifest["errors"].append({"stage": "create_table", "table": r[1],
                                                       "aidp_target": r[2], "error": r[3]})
                            manifest["stats"]["tables_failed"] += 1
                            print(f"  FAIL {r[1]}: {r[3][:120]}")
            except Exception as e:
                manifest["errors"].append({"stage": "table_batch",
                                           "chunk_idx": chunk_idx // CHUNK_SIZE + 1,
                                           "error": f"{type(e).__name__}: {e}"})
                print(f"[migrate] chunk {chunk_idx//CHUNK_SIZE + 1}: EXC {type(e).__name__}: {e}")

    manifest["finished_at"] = datetime.utcnow().isoformat() + "Z"

    if session is not None:
        try:
            await session.close()
        except Exception:
            pass

    return manifest


def write_report(manifest: dict, path: str) -> None:
    """Generate a human-readable migration report."""
    s = manifest["stats"]
    lines = [
        f"# Catalog Migration Report — {manifest['run_id']}",
        f"",
        f"- **Source:** {manifest['source_workspace']}",
        f"- **Target AIDP cluster:** {manifest['aidp_cluster']}",
        f"- **Started:** {manifest['started_at']}",
        f"- **Finished:** {manifest.get('finished_at','?')}",
        f"- **Dry-run:** {manifest['dry_run']}",
        f"",
        f"## Summary",
        f"",
        f"| Metric | Count |",
        f"|---|---:|",
        f"| Schemas created | {s['schemas_created']} |",
        f"| Tables replayed | {s['tables_replayed']} |",
        f"| Tables skipped (unsupported) | {s['tables_skipped']} |",
        f"| Tables failed | {s['tables_failed']} |",
        f"| Total properties dropped | {s['dropped_property_count']} |",
        f"",
    ]
    if manifest["skipped"]:
        lines += [f"## Skipped Tables ({len(manifest['skipped'])})", ""]
        for sk in manifest["skipped"][:30]:
            lines.append(f"- `{sk['table']}` — {sk['reason']}")
        if len(manifest["skipped"]) > 30:
            lines.append(f"- ... +{len(manifest['skipped']) - 30} more")
        lines.append("")
    if manifest["errors"]:
        lines += [f"## Errors ({len(manifest['errors'])})", ""]
        for e in manifest["errors"][:20]:
            lines.append(f"- **{e.get('stage','?')}** `{e.get('table') or e.get('schema') or '?'}` — {e.get('error','?')[:200]}")
        lines.append("")
    lines += [f"## Schema Map", "", "| Databricks | AIDP |", "|---|---|"]
    for k, v in sorted(manifest["schema_map"].items()):
        lines.append(f"| `{k}` | `{v}` |")
    lines += ["", f"## Table Map ({len(manifest['table_map'])} entries)", "",
              "| Databricks | AIDP |", "|---|---|"]
    for k, v in sorted(manifest["table_map"].items()):
        lines.append(f"| `{k}` | `{v}` |")
    Path(path).write_text("\n".join(lines), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pack", required=True, help="Path to catalog pack JSON")
    ap.add_argument("--aidp-cluster", required=True)
    ap.add_argument("--aidp-lake", required=True)
    ap.add_argument("--aidp-workspace", required=True)
    ap.add_argument("--aidp-profile", default="AIDP_API")
    ap.add_argument("--out-manifest", required=True)
    ap.add_argument("--out-report", required=True)
    ap.add_argument("--catalog-map", default="",
                    help="Per-catalog override: src=tgt,src2=tgt2 (e.g. main=default). "
                         "Unmapped source catalogs fall back to --default-catalog.")
    ap.add_argument("--default-catalog", default="default",
                    help="AIDP catalog for source catalogs not in --catalog-map "
                         "(and for 2-part HMS names). Default: default")
    ap.add_argument("--bucket-map", default="", help="Path to S3->OCI CSV")
    ap.add_argument("--flatten-strategy", default="catalog-remap",
                    choices=["catalog-remap", "schema-prefix", "schema-only"],
                    help="catalog-remap (default): keep schema+table, remap catalog "
                         "(3-level, AIDP supports it). schema-prefix/-only: 2-level flatten.")
    ap.add_argument("--location-strategy", default="auto",
                    choices=["auto", "drop", "preserve"],
                    help="auto (default): EXTERNAL keeps rewritten LOCATION, MANAGED omits it. "
                         "drop/preserve force one behavior for all tables.")
    ap.add_argument("--target-using", default=None,
                    help="Format OVERRIDE (e.g. parquet). Default: preserve source format "
                         "(Delta stays Delta).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    manifest = asyncio.run(replay(args.pack, args))
    Path(args.out_manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_manifest).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    write_report(manifest, args.out_report)

    s = manifest["stats"]
    print(f"\n[migrate] COMPLETE: {s['schemas_created']} schemas, "
          f"{s['tables_replayed']} tables OK, {s['tables_skipped']} skipped, "
          f"{s['tables_failed']} failed")
    print(f"[migrate] manifest: {args.out_manifest}")
    print(f"[migrate] report:   {args.out_report}")


if __name__ == "__main__":
    main()
