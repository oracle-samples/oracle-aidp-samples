#!/usr/bin/env python3
"""Pipeline health classifier for the ``aidp-fusion-status`` skill.

Cross-references the two truth sources into one honest per-node health view:

  * **state rows** — ``fusion_autopilot_state`` (latest per dataset): what the
    orchestrator *recorded* (last_run_at, status, row_count, skip_reason);
  * **live tables** — what is *actually materialized* in the catalog (exists,
    row_count), from a cluster probe.

Why both: ``fusion_autopilot_state`` is run metadata, NOT the physical tables — a
``success`` row can sit next to a dropped/empty mart, and a table can exist
with no state row. Health keys off **the live table as evidence**, with the
state row as the run-history overlay — same discipline the seed guard and the
advisor use ([[feedback_live_catalog_is_evidence]]).

Health classes:
  HEALTHY    — state=success AND live table exists with rows
  EMPTY_OK   — state=success AND live exists with 0 rows (legit empty source)
  STALE      — state=success BUT live table absent (recorded built, not there)
  FAILED     — last state status = failed
  DEFERRED   — last state status = deferred (planned, intentionally not built)
  SKIPPED    — last state status = skipped (carries skip_reason)
  UNTRACKED  — live table exists but NO state row (built outside the orchestrator)
  NEVER_RUN  — declared pack node with neither a state row nor a live table

Input JSON (stdin or --input):
    {
      "state": [{"dataset_id","layer","mode","last_run_at","status",
                 "row_count","skip_reason"}, ...],   # latest per dataset
      "live":  {"<table-name>": {"exists": true, "row_count": 49}, ...},
      "known_nodes": ["gl_balance","supplier_spend", ...]   # optional (pack nodes)
    }

Output JSON: {"summary": {<class>: n, ...}, "nodes": [{...health...}], "attention": [...]}.
"""
from __future__ import annotations

import argparse
import json
import sys

# Audit/probe tables that are never real marts — excluded from UNTRACKED noise.
_NON_MART = frozenset({
    "fusion_autopilot_state", "fusion_autopilot_state_latest", "fusion_autopilot_state_test",
})
# Classes that need an operator's attention.
_ATTENTION = frozenset({"STALE", "FAILED", "UNTRACKED", "NEVER_RUN"})


def _live_for(live: dict, name: str) -> dict | None:
    if name in live:
        return live[name]
    # tolerate schema-qualified keys ("gold.gl_balance")
    for k, v in live.items():
        if k.rsplit(".", 1)[-1] == name:
            return v
    return None


def build_status(payload: dict) -> dict:
    state = payload.get("state") or []
    live = payload.get("live") or {}
    known = list(payload.get("known_nodes") or [])

    nodes: list[dict] = []
    seen_datasets: set[str] = set()

    for row in state:
        ds = row.get("dataset_id")
        if not ds:
            continue
        seen_datasets.add(ds)
        st = str(row.get("status", "")).lower()
        lv = _live_for(live, ds)
        live_exists = bool(lv and lv.get("exists"))
        live_rows = (lv or {}).get("row_count")

        if st == "failed":
            health = "FAILED"
        elif st == "deferred":
            # Planned but intentionally not built (not in active pack / cascade) —
            # not an error and not "was built, now gone". Informational.
            health = "DEFERRED"
        elif st in ("skipped", "resumed_skipped"):
            health = "SKIPPED"
        elif st in ("success", "resumed_skip") and not live_exists:
            health = "STALE"
        elif live_exists and (live_rows or 0) > 0:
            health = "HEALTHY"
        elif live_exists:
            health = "EMPTY_OK"
        else:
            health = "STALE"

        nodes.append({
            "dataset": ds,
            "layer": row.get("layer"),
            "lastRun": row.get("last_run_at"),
            "lastStatus": row.get("status"),
            "stateRowCount": row.get("row_count"),
            "liveExists": live_exists,
            "liveRowCount": live_rows,
            "skipReason": row.get("skip_reason"),
            "health": health,
        })

    # Live tables with no state row -> UNTRACKED (exclude audit/probe tables).
    for name, lv in live.items():
        short = name.rsplit(".", 1)[-1]
        if short in seen_datasets or short in _NON_MART or short.startswith("_"):
            continue
        if lv.get("exists"):
            nodes.append({
                "dataset": short, "layer": None, "lastRun": None, "lastStatus": None,
                "stateRowCount": None, "liveExists": True,
                "liveRowCount": lv.get("row_count"), "skipReason": None,
                "health": "UNTRACKED",
            })

    # Declared pack nodes with neither state nor live -> NEVER_RUN.
    live_shorts = {k.rsplit(".", 1)[-1] for k in live}
    for n in known:
        if n not in seen_datasets and n not in live_shorts:
            nodes.append({
                "dataset": n, "layer": None, "lastRun": None, "lastStatus": None,
                "stateRowCount": None, "liveExists": False, "liveRowCount": None,
                "skipReason": None, "health": "NEVER_RUN",
            })

    summary: dict[str, int] = {}
    for n in nodes:
        summary[n["health"]] = summary.get(n["health"], 0) + 1
    attention = sorted(
        n["dataset"] for n in nodes if n["health"] in _ATTENTION
    )

    return {"summary": summary, "nodes": nodes, "attention": attention}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Classify pipeline health from state + live tables.")
    ap.add_argument("--input", default="-", help="Path to status JSON, or '-' for stdin.")
    ns = ap.parse_args(argv)
    text = sys.stdin.read() if ns.input == "-" else open(ns.input, encoding="utf-8").read()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid status JSON: {exc}") from exc
    print(json.dumps(build_status(payload), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
