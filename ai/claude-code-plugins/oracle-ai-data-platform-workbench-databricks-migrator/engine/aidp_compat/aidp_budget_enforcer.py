#!/usr/bin/env python3
"""
AIDP Per-Owner Budget Enforcer
==============================
Owner cost observability + hard budget enforcement for an AIDP workspace.

Concept (prepaid/allowance model): every owner has a budget cap (e.g. $20,000),
either resetting each calendar month ("monthly") or a fixed depleting allowance
("total"). Spend accrues per owner; when an owner crosses their cap the enforcer
HARD-ENFORCES — it pauses that owner's job schedules and stops the clusters they
created so spend cannot overrun.

Cost model (official AIDP Units mapping, shape-aware): each cluster node is
billed by its OWN shape family — AMD 67 / ARM 27 / Intel 87 units per OCPU/hr,
memory 3 units/GB, NVIDIA GPU 4110 units/GPU/hr. Run cost = units_hr *
runtime_hrs * price_per_unit_hr, attributed to the launcher (createdByName).
All of this logic lives in aidp_compat/budget.py and is shared with the
observability + enforcer notebooks; this CLI is a thin wrapper over it.

History: appends a dated snapshot per run to a /Workspace JSON file so month-on-
month and "total" period tracking survive across runs.

SAFETY: LIVE destructive actions (pause schedules, stop clusters) for
over-budget owners are OFF by default. A bare invocation is report-only; pass
--enforce to actually pause/stop (and only owners with enforce=true in the
budget config are touched).

Usage (run as a module so the relative import resolves — no sys.path hacks):
    python3 -m aidp_compat.aidp_budget_enforcer                 # report only (safe)
    python3 -m aidp_compat.aidp_budget_enforcer --enforce       # LIVE pause/stop
    python3 -m aidp_compat.aidp_budget_enforcer --config reports/aidp_budgets.json
"""

import argparse
import json
import os
import sys

from . import budget as B

DEFAULT_CONFIG = "reports/aidp_budgets.json"


def main():
    ap = argparse.ArgumentParser(description="AIDP per-owner budget enforcer")
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    # SAFETY: default is report-only. LIVE destructive actions (pause schedules,
    # stop clusters) require an explicit --enforce flag so a bare invocation can
    # never accidentally halt jobs/clusters.
    ap.add_argument("--enforce", action="store_true",
                    help="perform LIVE pause/stop for over-budget owners "
                         "(without this, runs report-only / dry-run).")
    ap.add_argument("--history-path", default=os.environ.get("AIDP_BUDGET_HISTORY"),
                    help="snapshot history JSON path (or $AIDP_BUDGET_HISTORY). "
                         "Required — no default.")
    args = ap.parse_args()

    if not args.history_path:
        print("ERROR: --history-path (or $AIDP_BUDGET_HISTORY) is required.",
              file=sys.stderr)
        return 2
    history_path = args.history_path
    dry_run = not args.enforce

    with open(args.config) as f:
        cfg = json.load(f)

    signer = B.make_signer()
    base = B.base_url()
    model = B.CostModel(signer, base,
                        units_per_ocpu=cfg.get("units_per_ocpu", 67),
                        units_per_gb=cfg.get("units_per_gb", 3),
                        worker_count=cfg.get("worker_count", "max"),
                        price_per_unit_hr=cfg.get("price_per_unit_hr", 0.001))

    jobs, rows, newest, estimated = B.collect_spend(signer, base, model)
    report = B.evaluate(rows, cfg, newest)

    # LIVE (or dry-run) enforcement for over-budget owners.
    for r in report:
        r["actions"] = []
        if r["status"] == "OVER" and r["enforce"]:
            r["actions"] = B.enforce_owner(signer, base, r["owner"], jobs, dry_run)
        r["enforced"] = bool(r["actions"])

    # Persist a snapshot for MoM history (without per-run actions).
    B.save_snapshot(history_path,
                    [{k: v for k, v in r.items() if k != "actions"} for r in report],
                    newest)

    # Print summary.
    mode = "DRY-RUN" if dry_run else "LIVE-ENFORCE"
    cur_month = report[0]["month"] if report else None
    print(f"=== AIDP Budget Enforcer [{mode}]  month={cur_month}  history={history_path} ===")
    print(f"{'owner':<38}{'period':<9}{'spent':>12}{'cap':>12}{'pct':>7}  status")
    for r in report:
        flag = "  <-- ENFORCED" if r["enforced"] else ""
        print(f"{r['owner'][:36]:<38}{r['period']:<9}{r['spent_usd']:>12.2f}"
              f"{r['cap_usd']:>12.2f}{r['pct']:>6.1f}%  {r['status']}{flag}")
        for kind, name, result in r["actions"]:
            print(f"    {kind} {name}: {result}")

    # Non-zero exit when any run's cost had to be estimated (unresolvable
    # cluster) so CI/cron can flag it for review — the spend itself is still
    # counted conservatively, so enforcement remains safe.
    return 13 if estimated else 0


if __name__ == "__main__":
    sys.exit(main())
