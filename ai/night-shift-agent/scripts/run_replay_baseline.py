"""Live hazard-replay baseline on Oracle AI Data Platform.

Grades candidate repair templates for a production Delta table inside a sandbox schema
(SHALLOW CLONE, run twice, late / duplicate / out-of-order / retried batches, diff vs recompute).

Usage: python scripts/run_replay_baseline.py [--table default.nightshift_demo.orders_silver]
       [--key order_id] [--ts updated_at] [--sandbox default.nightshift_sbx] [--only merge|naive|dedup]
Prints one JSON verdict per candidate and the silent-wrong rate. Sandbox objects are dropped in finally.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift import replay  # noqa: E402
from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402

CANDIDATES = {
    # correct: upsert on key, keep the latest row by ts
    "merge": (
        "MERGE INTO {target} t USING {batch} s ON t.{key} = s.{key} "
        "WHEN MATCHED AND s.{ts} >= t.{ts} THEN UPDATE SET * "
        "WHEN NOT MATCHED THEN INSERT *"
    ),
    # the ArrivalBench class: passes a single ordered run (keys unique per batch) but not replay
    "naive": "INSERT INTO {target} SELECT * FROM {batch}",
    # planted known-bad repair: dedups within the batch only
    "dedup": (
        "INSERT INTO {target} SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY {ts} DESC) AS ns_rn "
        "FROM {batch}) WHERE ns_rn = 1"
    ),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="default.nightshift_demo.orders_silver")
    ap.add_argument("--key", default=None, help="key column(s), comma-separated (default order_id)")
    ap.add_argument("--ts", default="updated_at")
    ap.add_argument("--sandbox", default="default.nightshift_sbx")
    ap.add_argument("--only", default=None)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--print-only", action="store_true")
    ap.add_argument("--merge-file", default=None,
                    help="path to a real MERGE statement; it is adapted (target/source -> placeholders) and graded "
                         "as the single candidate 'real'. --key defaults to the keys inferred from its ON clause.")
    ap.add_argument("--pin-version", type=int, default=None, help="Delta version of the production table to clone")
    ap.add_argument("--hazards", default=",".join(replay.HAZARDS),
                    help="comma-separated subset of " + ",".join(replay.HAZARDS))
    ap.add_argument("--revise-col", default=None,
                    help="payload column the revision hazard changes (default: first mutable non-key, non-ts column)")
    args = ap.parse_args()
    hazards = [h.strip() for h in args.hazards.split(",") if h.strip()]
    names = [args.only] if args.only else list(CANDIDATES)
    adapted = None
    if args.merge_file:
        from nightshift.merge_adapt import adapt_merge
        adapted = adapt_merge(pathlib.Path(args.merge_file).read_text(encoding="utf-8"))
        CANDIDATES.clear()
        CANDIDATES["real"] = adapted.template
        names = ["real"]
        if not args.key and adapted.key_cols:
            args.key = ",".join(adapted.key_cols)
        print(json.dumps({"adapted": {"target": adapted.target, "source": adapted.source, "keys": list(adapted.key_cols),
                                      "ordering_guard": adapted.ts_guard, "template": adapted.template}}, indent=1))
    verdicts = {}
    if not args.key:
        args.key = "order_id"
    key_cols = [k.strip() for k in args.key.split(",") if k.strip()]
    io = None if args.print_only else AidpIO(Env(), dry_run=True)
    columns = None
    cfg = replay.HazardConfig(revise_col=args.revise_col)
    try:
        if "revision" in hazards:
            if io is None:
                print("print-only: the revision hazard needs live column types; skipped")
                hazards.remove("revision")
            else:
                columns = []
                for r in io.run_sql(f"DESCRIBE {args.table}"):
                    col = str(r[0]).strip()
                    if not col or col.startswith("#"):
                        break  # partition-information block follows the column list
                    columns.append((col, str(r[1])))
                picked = replay.pick_spark_revise_col(columns, key_cols, args.ts, args.revise_col)
                print(json.dumps({"revision": {"columns": len(columns), "revise_col": picked,
                                               "ts_shift": replay.spark_shift_expr(args.ts, dict(columns)[args.ts])}}))
        for name in names:
            if adapted is not None:
                tmpl = CANDIDATES[name]  # a real statement: used verbatim, only {target}/{batch} are substituted
            else:
                tmpl = CANDIDATES[name].format(target="{target}", batch="{batch}", key=key_cols[0], ts=args.ts)
            # the dedup template needs its helper column dropped; INSERT with SELECT * of extra column would fail,
            # so project the batch columns explicitly via a nested select of {batch} columns
            if name == "dedup":
                # struct(*) star expansion is used instead of SELECT * EXCEPT so the statement runs on Spark 3.5 without that extension
                tmpl = (
                    "INSERT INTO {target} SELECT ns_row.* FROM (SELECT struct(*) AS ns_row, ROW_NUMBER() OVER (PARTITION BY "
                    f"{key_cols[0]} ORDER BY {args.ts} DESC) AS ns_rn FROM {{batch}}) WHERE ns_rn = 1"
                )
            plan = replay.spark_sql_plan(args.table, args.sandbox, tmpl, key_cols, args.ts, hazards, cfg,
                                         pin_version=args.pin_version, columns=columns)
            cell = replay.render_spark_cell(plan)
            if args.print_only:
                print(f"--- {name}: {len(plan)} statements; first 6:")
                print("\n".join(plan[:6]))
                continue
            t0 = time.time()
            res = io.run_code(cell, timeout=args.timeout)
            text = "".join(o.get("text", "") for o in res.get("outputs", []) if o.get("output_type") == "stream")
            err = res.get("error")
            verdict = None
            dec = json.JSONDecoder()
            starts = [i for i, ch in enumerate(text) if ch == '{' and (i == 0 or text[i - 1] in "\r\n")]
            for pos in reversed(starts):
                try:
                    obj, _ = dec.raw_decode(text[pos:])
                except Exception:  # noqa: BLE001
                    continue
                if isinstance(obj, dict) and 'verdict' in obj:
                    verdict = obj
                    break
            verdicts[name] = {"verdict": verdict, "elapsed_s": round(time.time() - t0, 1), "error": err,
                              "tail": text[-800:] if verdict is None else None}
            print(json.dumps({name: verdicts[name]}, indent=1, default=str))
        if verdicts:
            graded = [v["verdict"] for v in verdicts.values() if v.get("verdict")]
            snap_ok = [g for g in graded if g.get("passes_snapshot")]
            silent = [g for g in snap_ok if g.get("verdict") == "silent_wrong"]
            print(json.dumps({"candidates": len(verdicts), "graded": len(graded), "passed_snapshot": len(snap_ok),
                              "silent_wrong": len(silent),
                              "silent_wrong_rate": (len(silent) / len(snap_ok)) if snap_ok else None}, indent=1))
    finally:
        if io is not None:
            io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
