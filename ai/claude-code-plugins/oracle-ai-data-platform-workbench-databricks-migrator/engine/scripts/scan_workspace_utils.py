#!/usr/bin/env python3
"""Scan AIDP workspace for writer-wrapper utility functions.

Pipeline (streaming, no full local clone of the workspace):
  1. Recursively enumerate /Workspace/Users/* via the AIDP REST API.
  2. For each .ipynb, fetch source via PAR URL, AST-parse each code cell.
  3. Detect function defs whose body invokes a write operation:
       df.write.saveAsTable / .save / .json / .parquet / .csv / .insertInto
       spark.sql("CREATE|INSERT|UPDATE|DELETE|DROP|MERGE|ALTER ...")
  4. For each detected wrapper, infer which args are table / db / path / bucket
     based on which arg names participate in the write call (string substitution
     into a SQL literal, .option("path", arg), or .saveAsTable(arg)).
  5. Emit a JSON catalog at reports/customer_writer_wrappers.json.

Usage:
  python3 scripts/scan_workspace_utils.py [--root /Workspace/Users] [--enumerate-only]
                                          [--workers 4] [--limit N]
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import oci
import requests

# ----- AIDP config -----
AIDP_BASE = "https://aidp.<OCI_REGION>.oci.oraclecloud.com/20240831"
DATALAKE_OCID = "<DATALAKE_OCID>"
WORKSPACE_ID = "<WORKSPACE_ID>"
OCI_PROFILE = "CUSTOMER"
OBJECTS_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/objects"
DOWNLOAD_META_URL = f"{AIDP_BASE}/dataLakes/{DATALAKE_OCID}/workspaces/{WORKSPACE_ID}/actions/downloadFileMeta"

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(PROJECT_DIR, "reports")
OUT_CATALOG = os.path.join(REPORTS_DIR, "customer_writer_wrappers.json")
OUT_INVENTORY = os.path.join(REPORTS_DIR, "workspace_inventory.json")


def get_signer() -> Any:
    config = oci.config.from_file(profile_name=OCI_PROFILE)
    return oci.signer.Signer(
        tenancy=config["tenancy"],
        user=config["user"],
        fingerprint=config["fingerprint"],
        private_key_file_location=config["key_file"],
        private_key_content=config.get("key_content"),
    )


# ----- Workspace enumeration -----
def list_objects(signer, path: str = "") -> list[dict]:
    url = f"{OBJECTS_URL}?path={urllib.parse.quote(path, safe='')}"
    for attempt in range(5):
        resp = requests.get(url, auth=signer, timeout=60)
        if resp.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp.json().get("items", [])
    resp.raise_for_status()
    return []


def enumerate_recursive(signer, root: str, log_every: int = 200) -> list[dict]:
    """BFS enumeration so we get progress updates."""
    queue = [root]
    out: list[dict] = []
    seen_dirs = 0
    while queue:
        path = queue.pop(0)
        try:
            items = list_objects(signer, path)
        except Exception as e:
            print(f"  [list] {path}: ERROR {e}", file=sys.stderr)
            continue
        seen_dirs += 1
        for it in items:
            out.append(it)
            if it.get("type") == "FOLDER":
                queue.append(it["path"])
        if seen_dirs % log_every == 0:
            nb_count = sum(1 for x in out if x.get("type") == "NOTEBOOK" or x.get("path", "").endswith(".ipynb"))
            print(f"  [enum] dirs={seen_dirs} items={len(out)} notebooks={nb_count} queue={len(queue)}", flush=True)
    return out


# ----- Notebook download -----
def get_par_url(signer, nb_path: str, file_type: str = "NOTEBOOK") -> str | None:
    headers = {"Content-Type": "application/json", "path": nb_path, "type": file_type}
    for attempt in range(5):
        resp = requests.post(DOWNLOAD_META_URL, auth=signer, headers=headers, data="", timeout=60)
        if resp.status_code == 429:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp.json().get("parUrl")
    resp.raise_for_status()
    return None


def fetch_notebook(signer, nb_path: str, file_type: str) -> dict | None:
    par = get_par_url(signer, nb_path, file_type)
    if not par:
        return None
    resp = requests.get(par, timeout=120)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return json.loads(resp.content.decode("utf-8", errors="replace"))


# ----- Writer detection -----
_SQL_WRITE_KW = ("CREATE", "INSERT", "UPDATE", "DELETE", "DROP", "MERGE", "ALTER", "TRUNCATE", "REPLACE")
_WRITE_METHOD_NAMES = {
    "saveAsTable", "insertInto", "save", "parquet", "csv", "json", "orc", "text",
    "saveAsNewAPIHadoopDataset", "saveAsHadoopFile",
}


def _is_spark_sql_call(node: ast.Call) -> bool:
    """Match `spark.sql(...)` or `<spark-ish>.sql(...)`."""
    fn = node.func
    if isinstance(fn, ast.Attribute) and fn.attr == "sql":
        return True
    return False


def _chain_contains_write(node: ast.AST) -> bool:
    """Walk an attribute chain looking for `.write` (writer) — distinguishes
    df.write.json(path) from spark.read.option(...).json(path)."""
    cur = node
    depth = 0
    while depth < 20 and cur is not None:
        if isinstance(cur, ast.Attribute):
            if cur.attr == "write":
                return True
            if cur.attr == "read":
                return False
            cur = cur.value
        elif isinstance(cur, ast.Call):
            cur = cur.func
        else:
            break
        depth += 1
    return False


def _sql_text_arg(node: ast.Call) -> str | None:
    if not node.args:
        return None
    a = node.args[0]
    if isinstance(a, ast.Constant) and isinstance(a.value, str):
        return a.value
    if isinstance(a, ast.JoinedStr):
        # f-string — concat the static parts
        parts: list[str] = []
        for v in a.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
            elif isinstance(v, ast.FormattedValue):
                parts.append("{}")  # placeholder
        return "".join(parts)
    if isinstance(a, ast.Call) and isinstance(a.func, ast.Attribute) and a.func.attr == "format":
        # "<sql>".format(...)
        if isinstance(a.func.value, ast.Constant) and isinstance(a.func.value.value, str):
            return a.func.value.value
    if isinstance(a, ast.BinOp) and isinstance(a.op, ast.Mod):
        if isinstance(a.left, ast.Constant) and isinstance(a.left.value, str):
            return a.left.value
    return None


def _walk_write_signals(fn: ast.FunctionDef) -> dict:
    """Inspect fn body for write signals; return what was found."""
    sql_keywords: set[str] = set()
    write_methods: set[str] = set()
    # arg names that appear *inside* SQL string formatting / write paths
    arg_names = {a.arg for a in fn.args.args}
    arg_names |= {a.arg for a in fn.args.kwonlyargs}
    if fn.args.vararg:
        arg_names.add(fn.args.vararg.arg)
    if fn.args.kwarg:
        arg_names.add(fn.args.kwarg.arg)
    used_in_sql: set[str] = set()
    used_in_path: set[str] = set()
    used_in_save_as_table: set[str] = set()
    used_in_bucket: set[str] = set()
    has_partition_by = False

    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            # spark.sql("...") with write keyword
            if _is_spark_sql_call(node):
                txt = _sql_text_arg(node)
                if txt:
                    upper = txt.upper().lstrip()
                    for kw in _SQL_WRITE_KW:
                        if upper.startswith(kw + " ") or f"\n{kw} " in upper or upper.startswith(kw + "\t"):
                            sql_keywords.add(kw)
                            break
                    # See which arg names appear via f-string placeholders or .format args
                    a = node.args[0] if node.args else None
                    if isinstance(a, ast.JoinedStr):
                        for v in a.values:
                            if isinstance(v, ast.FormattedValue):
                                for name in _names_in_expr(v.value):
                                    if name in arg_names:
                                        used_in_sql.add(name)
                    if isinstance(a, ast.Call) and isinstance(a.func, ast.Attribute) and a.func.attr == "format":
                        for kw in a.keywords:
                            for name in _names_in_expr(kw.value):
                                if name in arg_names:
                                    used_in_sql.add(name)
                        for arg in a.args:
                            for name in _names_in_expr(arg):
                                if name in arg_names:
                                    used_in_sql.add(name)
            # .write.* / writer chain — only count when the chain is `.write.*`
            if isinstance(node.func, ast.Attribute):
                m = node.func.attr
                # saveAsTable / insertInto are unambiguous writers regardless of chain
                if m in ("saveAsTable", "insertInto"):
                    write_methods.add(m)
                    if m == "saveAsTable" and node.args:
                        for name in _names_in_expr(node.args[0]):
                            if name in arg_names:
                                used_in_save_as_table.add(name)
                    if m == "insertInto" and node.args:
                        for name in _names_in_expr(node.args[0]):
                            if name in arg_names:
                                used_in_save_as_table.add(name)
                # path-style methods (.json/.parquet/.csv/.orc/.text/.save) — only count when chain has .write
                elif m in _WRITE_METHOD_NAMES and m not in ("saveAsTable", "insertInto"):
                    if _chain_contains_write(node.func.value):
                        write_methods.add(m)
                        if node.args:
                            for name in _names_in_expr(node.args[0]):
                                if name in arg_names:
                                    used_in_path.add(name)
                # .option("path", x) — only count when chain has .write
                if m == "option" and len(node.args) >= 2 and _chain_contains_write(node.func.value):
                    k = node.args[0]
                    if isinstance(k, ast.Constant) and k.value == "path":
                        for name in _names_in_expr(node.args[1]):
                            if name in arg_names:
                                used_in_path.add(name)
                if m == "partitionBy":
                    has_partition_by = True
        # bucket-name substitution into a path-looking string?
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            base = node.func.value
            if isinstance(base, ast.Constant) and isinstance(base.value, str):
                if any(s in base.value for s in ("s3a://", "s3://", "oci://", "/Volumes/", "{bucket")):
                    for kw in node.keywords:
                        for name in _names_in_expr(kw.value):
                            if name in arg_names:
                                if "bucket" in (kw.arg or "").lower():
                                    used_in_bucket.add(name)
                                else:
                                    used_in_path.add(name)
        if isinstance(node, ast.JoinedStr):
            literal_parts = []
            for v in node.values:
                if isinstance(v, ast.Constant):
                    literal_parts.append(str(v.value))
            literal = "".join(literal_parts)
            if any(s in literal for s in ("s3a://", "s3://", "oci://", "/Volumes/")):
                for v in node.values:
                    if isinstance(v, ast.FormattedValue):
                        for name in _names_in_expr(v.value):
                            if name in arg_names:
                                used_in_path.add(name)

    return {
        "sql_keywords": sorted(sql_keywords),
        "write_methods": sorted(write_methods),
        "used_in_sql": sorted(used_in_sql),
        "used_in_path": sorted(used_in_path),
        "used_in_save_as_table": sorted(used_in_save_as_table),
        "used_in_bucket": sorted(used_in_bucket),
        "has_partition_by": has_partition_by,
    }


def _names_in_expr(node: ast.AST) -> set[str]:
    out: set[str] = set()
    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            out.add(n.id)
    return out


def _fn_signature(fn: ast.FunctionDef) -> dict:
    """Return arg names + defaults + kwonly + vararg/kwarg info."""
    args = fn.args
    pos = [a.arg for a in args.args]
    defaults = list(args.defaults)
    n_no_default = len(pos) - len(defaults)
    pos_with_default = {}
    for i, a in enumerate(pos):
        if i >= n_no_default:
            d = defaults[i - n_no_default]
            pos_with_default[a] = _const_repr(d)
    kwonly = [a.arg for a in args.kwonlyargs]
    kw_defaults = {}
    for a, d in zip(args.kwonlyargs, args.kw_defaults):
        if d is not None:
            kw_defaults[a.arg] = _const_repr(d)
    return {
        "positional": pos,
        "positional_defaults": pos_with_default,
        "kwonly": kwonly,
        "kwonly_defaults": kw_defaults,
        "vararg": args.vararg.arg if args.vararg else None,
        "kwarg": args.kwarg.arg if args.kwarg else None,
    }


def _const_repr(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    try:
        return ast.unparse(node)
    except Exception:
        return "<expr>"


def analyze_notebook(nb: dict, nb_path: str) -> list[dict]:
    """Return list of detected writer wrappers in this notebook."""
    out = []
    for cell_idx, cell in enumerate(nb.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source", "")
        src = src if isinstance(src, str) else "".join(src)
        if not src.strip():
            continue
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            sig = _fn_signature(node)
            sigs = _walk_write_signals(node)
            is_writer = bool(sigs["sql_keywords"]) or bool(sigs["write_methods"])
            if not is_writer:
                continue
            # Heuristic arg-role assignment — NAME pattern wins, usage is fallback only
            roles: dict[str, str] = {}
            all_args = sig["positional"] + sig["kwonly"]

            # PASS 1 — name-based (strongest signal)
            for name in all_args:
                low = name.lower()
                if low in ("table_name", "tablename", "tbl") or low.endswith("_tbl"):
                    roles[name] = "table"
                elif low in ("database_name", "databasename", "db_name", "schema_name", "db", "schema"):
                    roles[name] = "db"
                elif low in ("bucket_name", "bucketname", "bucket"):
                    roles[name] = "bucket"
                elif low in ("write_mode", "mode", "save_mode"):
                    roles[name] = "mode"
                elif low == "path" or low.endswith("_path") or low.endswith("_uri") or low.endswith("_location"):
                    roles[name] = "path"

            # PASS 2 — usage-based (only for args not already roled)
            for name in sigs["used_in_save_as_table"]:
                roles.setdefault(name, "table_or_full_id")
            for name in sigs["used_in_path"]:
                roles.setdefault(name, "path")
            for name in sigs["used_in_bucket"]:
                roles.setdefault(name, "bucket")
            for name in sigs["used_in_sql"]:
                if name in roles:
                    continue
                low = name.lower()
                if "database" in low or "schema" in low:
                    roles[name] = "db"
                elif "table" in low:
                    roles[name] = "table"
                else:
                    roles[name] = "sql_param"

            out.append({
                "function_name": node.name,
                "notebook_path": nb_path,
                "cell_idx": cell_idx,
                "line": node.lineno,
                "signature": sig,
                "signals": sigs,
                "arg_roles": roles,
                "is_writer": True,
            })
    return out


# ----- Per-notebook worker -----
def scan_one(signer, item: dict) -> tuple[str, list[dict] | None, str | None]:
    nb_path = item["path"]
    file_type = item.get("type", "NOTEBOOK")
    try:
        nb = fetch_notebook(signer, nb_path, file_type)
    except Exception as e:
        return nb_path, None, f"fetch_error: {e}"
    if not nb:
        return nb_path, None, "empty_par_url"
    try:
        results = analyze_notebook(nb, nb_path)
    except Exception as e:
        return nb_path, None, f"analyze_error: {e}"
    return nb_path, results, None


# ----- Main -----
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/Workspace/Users", help="Workspace root to scan")
    ap.add_argument("--enumerate-only", action="store_true", help="Stop after enumeration")
    ap.add_argument("--workers", type=int, default=4, help="Parallel notebook scans")
    ap.add_argument("--limit", type=int, default=0, help="Scan only first N notebooks (0=all)")
    ap.add_argument("--inventory", default=OUT_INVENTORY, help="Where to cache the inventory JSON")
    ap.add_argument("--out", default=OUT_CATALOG, help="Where to write the wrapper catalog JSON")
    ap.add_argument("--use-cached-inventory", action="store_true",
                    help="Skip enumeration if inventory file already exists")
    args = ap.parse_args()

    os.makedirs(REPORTS_DIR, exist_ok=True)

    signer = get_signer()
    print(f"=== AIDP Workspace Writer-Wrapper Scan ===")
    print(f"root      : {args.root}")
    print(f"workers   : {args.workers}")
    print(f"limit     : {args.limit or 'all'}")
    print(f"output    : {args.out}")
    print(f"inventory : {args.inventory}")
    print(f"started   : {datetime.now().isoformat()}")
    print()

    # Phase 1 — enumerate
    if args.use_cached_inventory and os.path.exists(args.inventory):
        print(f"[enum] using cached inventory: {args.inventory}")
        with open(args.inventory) as f:
            items = json.load(f)
    else:
        print(f"[enum] starting recursive enumeration from {args.root}")
        items = enumerate_recursive(signer, args.root)
        with open(args.inventory, "w") as f:
            json.dump(items, f, indent=2, default=str)
        print(f"[enum] saved inventory: {args.inventory}")

    notebooks = [i for i in items if i.get("type") == "NOTEBOOK" or i.get("path", "").endswith(".ipynb")]
    print(f"[enum] total items={len(items)} notebooks={len(notebooks)}")

    if args.enumerate_only:
        return 0

    if args.limit:
        notebooks = notebooks[: args.limit]

    # Phase 2 — parallel scan
    print(f"\n[scan] scanning {len(notebooks)} notebooks with {args.workers} workers...")
    started = time.time()
    all_wrappers: list[dict] = []
    errors: list[dict] = []
    done = 0
    lock = threading.Lock()

    def _worker(item):
        return scan_one(signer, item)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(_worker, nb): nb for nb in notebooks}
        for fut in as_completed(futures):
            nb_path, wrappers, err = fut.result()
            with lock:
                done += 1
                if err:
                    errors.append({"path": nb_path, "error": err})
                elif wrappers:
                    all_wrappers.extend(wrappers)
                if done % 100 == 0 or done == len(notebooks):
                    elapsed = time.time() - started
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(notebooks) - done) / rate if rate > 0 else 0
                    print(f"  [scan] {done}/{len(notebooks)} "
                          f"wrappers={len(all_wrappers)} errors={len(errors)} "
                          f"rate={rate:.1f}/s eta={eta:.0f}s", flush=True)

    # Deduplicate (same function name + signature across notebooks) into a roll-up
    rollup: dict[str, dict] = {}
    for w in all_wrappers:
        key = w["function_name"]
        bucket = rollup.setdefault(key, {
            "function_name": key,
            "occurrences": 0,
            "notebooks": [],
            "signatures": [],
            "roles_observed": {},
        })
        bucket["occurrences"] += 1
        bucket["notebooks"].append({"path": w["notebook_path"], "cell_idx": w["cell_idx"], "line": w["line"]})
        sig_key = json.dumps(w["signature"], sort_keys=True)
        if sig_key not in [json.dumps(s, sort_keys=True) for s in bucket["signatures"]]:
            bucket["signatures"].append(w["signature"])
        for arg, role in w["arg_roles"].items():
            bucket["roles_observed"].setdefault(arg, {})
            bucket["roles_observed"][arg][role] = bucket["roles_observed"][arg].get(role, 0) + 1

    # Sort by occurrences desc
    rollup_sorted = sorted(rollup.values(), key=lambda x: -x["occurrences"])

    out_doc = {
        "generated_at": datetime.now().isoformat(),
        "root": args.root,
        "notebooks_scanned": len(notebooks),
        "errors": errors,
        "total_writer_callables": len(all_wrappers),
        "unique_writer_names": len(rollup_sorted),
        "wrappers": rollup_sorted,
        "raw_detections": all_wrappers,
    }
    with open(args.out, "w") as f:
        json.dump(out_doc, f, indent=2, default=str)

    elapsed = time.time() - started
    print(f"\n[done] scanned {len(notebooks)} notebooks in {elapsed:.0f}s")
    print(f"[done] {len(all_wrappers)} writer detections; {len(rollup_sorted)} unique names")
    print(f"[done] errors: {len(errors)}")
    print(f"[done] catalog: {args.out}")
    if rollup_sorted:
        print("\nTop 25 wrappers by occurrence:")
        for r in rollup_sorted[:25]:
            roles_summary = ", ".join(f"{a}=" + "/".join(sorted(rs.keys()))
                                       for a, rs in list(r["roles_observed"].items())[:6])
            print(f"  {r['function_name']:<35} x{r['occurrences']:<5}  [{roles_summary}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
