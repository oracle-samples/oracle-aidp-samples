#!/usr/bin/env python3
"""Fail-closed destructive-guard decision for the ``aidp-fusion-seed`` skill.

``seed`` uses replace strategy on silver/gold (`CREATE OR REPLACE TABLE`), so a
re-seed silently overwrites populated marts. This module makes the
confirm-vs-proceed decision **executable and unit-testable** rather than leaving
it as SKILL.md prose — the safety logic is load-bearing, so it must be pinned by
a regression test (see ``tests/unit/test_seed_skill_guard.py``).

The decision keys off **physical target-table facts**, never
``fusion_autopilot_state`` rows. Two fail-open traps this closes:

  1. **State ≠ tables.** ``status`` reads run metadata, not the marts. Empty /
     stale state must never read as "empty" while a populated table exists.
  2. **Unreadable ≠ empty.** ``status`` returns exit 0 with only a "pyspark
     not available" message on the laptop path. "Could not inspect" is
     **unknown → confirm**, never "empty".

Input — for EVERY resolved in-scope target table from the ``--dry-run`` plan,
a per-target fact dict (the shape a future ``status --json`` would emit):

    {"target_table": "silver.dim_supplier",
     "target_exists": true, "target_row_count": 4213, "readable": true}

``status_json_supported`` is ``False`` on today's CLI (no ``--json`` mode), in
which case emptiness cannot be machine-proven and the guard ALWAYS confirms.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

CONFIRM = "confirm"
PROCEED = "proceed"


def classify_guard(
    targets: list[dict[str, Any]] | None,
    *,
    status_json_supported: bool,
) -> dict[str, Any]:
    """Return the fail-closed guard decision.

    Returns ``{decision, reason, populated_tables, unprovable_tables}`` where
    ``decision`` is ``"confirm"`` or ``"proceed"``. ``"proceed"`` is the ONLY
    no-prompt path and is reached **only** when every resolved in-scope target
    is readable AND (absent or zero rows) AND the CLI can actually inspect
    targets.
    """
    # Today's CLI cannot emit per-target physical facts -> cannot prove empty.
    if not status_json_supported:
        return {
            "decision": CONFIRM,
            "reason": (
                "status --json per-target inspection unavailable on this CLI; "
                "cannot prove target tables are empty (fail closed)"
            ),
            "populated_tables": [],
            "unprovable_tables": [],
        }

    # No resolved targets -> nothing proven; do not assume empty.
    if not targets:
        return {
            "decision": CONFIRM,
            "reason": "no resolved in-scope targets to inspect (fail closed)",
            "populated_tables": [],
            "unprovable_tables": [],
        }

    populated: list[str] = []
    unprovable: list[str] = []
    for t in targets:
        name = t.get("target_table", "<unknown>")
        # Decision keys off target facts ONLY — never `last_status` / state rows.
        if not t.get("readable", False):
            unprovable.append(name)  # could not inspect -> unknown
            continue
        exists = bool(t.get("target_exists", False))
        rows = t.get("target_row_count")
        if exists and (rows is None or rows > 0):
            # exists + (rows>0 OR unknown count) -> treat as populated.
            populated.append(name)

    if populated:
        return {
            "decision": CONFIRM,
            "reason": "resolved in-scope target table(s) physically populated — would overwrite",
            "populated_tables": sorted(populated),
            "unprovable_tables": sorted(unprovable),
        }
    if unprovable:
        return {
            "decision": CONFIRM,
            "reason": "could not inspect one or more resolved in-scope targets (fail closed)",
            "populated_tables": [],
            "unprovable_tables": sorted(unprovable),
        }
    # Every target readable AND absent-or-zero-rows -> the only proceed case.
    return {
        "decision": PROCEED,
        "reason": "every resolved in-scope target inspectable and empty/absent",
        "populated_tables": [],
        "unprovable_tables": [],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Fail-closed seed destructive-guard decision.")
    ap.add_argument(
        "--targets-json", default="[]",
        help="JSON array of per-target fact dicts (from status --json).",
    )
    ap.add_argument(
        "--status-json-supported", action="store_true",
        help="Set only when the CLI emits per-target physical facts. Absent -> fail closed.",
    )
    ns = ap.parse_args(argv)
    try:
        targets = json.loads(ns.targets_json)
    except json.JSONDecodeError as exc:
        print(json.dumps({"decision": CONFIRM, "reason": f"bad --targets-json: {exc}"}))
        return 0
    decision = classify_guard(targets, status_json_supported=ns.status_json_supported)
    print(json.dumps(decision, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
