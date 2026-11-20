"""Human side of the approval loop: list, approve or reject quarantined actions.

  python scripts/approve.py list                              # pending approvals (id, kind, target, params, ...)
  python scripts/approve.py approve <approval_id> --by <name> [--note ...]
  python scripts/approve.py reject  <approval_id> --by <name> [--note ...]
  ... --print-only                                            # render the SQL, touch nothing

An approved row is executed by the NEXT actuator tick (scripts/run_tick.py) through the same policy gate,
recorded in ops.actions with reversible=true, and the approval is marked executed. Rows expire 24 h after
they were opened; expired rows are never executed.

Identity. The ledger is written through the notebook kernel with the ONE API key the steward uses
(OCI profile Env().config_profile), so the platform cannot tell approvers apart. The approving identity
recorded in approvals.approved_by is therefore whatever the operator passes in --by, and the OCI profile
name is appended to the note (``profile=<name>``) so the review can see which key carried the write.
Treat --by as an attestation, not an authenticated principal.

Ledger writes are plain SQL, which AidpIO allows in dry-run mode (dry_run only governs control-plane
mutations), so this script never needs --apply.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nightshift import ledger  # noqa: E402
from nightshift.config import Env  # noqa: E402


def _cell(v: Any, width: int) -> str:
    s = "" if v is None else str(v)
    return (s[: width - 1] + "~") if len(s) > width else s.ljust(width)


def format_pending(rows: list) -> str:
    """Rows in ledger.PENDING_APPROVAL_COLUMNS order -> a fixed-width table (kind/target/params from action_json)."""
    header = [("id", 16), ("kind", 20), ("target", 36), ("params", 48), ("requested_at", 19), ("expires_at", 19), ("note", 40)]
    lines = ["  ".join(_cell(h, w) for h, w in header)]
    for r in rows:
        aid, _sig, aj, requested_at, expires_at, note = (list(r) + [None] * 6)[:6]
        try:
            body = json.loads(aj) if isinstance(aj, (str, bytes)) else (aj or {})
        except ValueError:
            body = {}
        params = json.dumps(body.get("params", {}), sort_keys=True, default=str)
        vals = [aid, body.get("kind"), body.get("target"), params, str(requested_at or "")[:19], str(expires_at or "")[:19], note]
        lines.append("  ".join(_cell(v, w) for v, (_, w) in zip(vals, header)))
    if not rows:
        lines.append("(no pending approvals)")
    return "\n".join(lines)


def build_sql(args: argparse.Namespace, env: Env, now: dt.datetime) -> str:
    if args.cmd == "list":
        return ledger.pending_approvals_sql(env.ops)
    note = f"profile={env.config_profile}" + (f"; {args.note}" if args.note else "")
    fn = ledger.approve_sql if args.cmd == "approve" else ledger.reject_sql
    return fn(env.ops, args.approval_id, args.by, note=note, now=now)


def main(argv: list[str] | None = None, io: Any | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--print-only", action="store_true", help="print the SQL and exit without connecting")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="show pending approvals")
    for name in ("approve", "reject"):
        sp = sub.add_parser(name, help=f"{name} one pending approval")
        sp.add_argument("approval_id")
        sp.add_argument("--by", required=True, help="who is deciding (recorded in approved_by; see docstring)")
        sp.add_argument("--note", default=None)
    args = p.parse_args(argv)
    env = Env()
    now = dt.datetime.now(dt.timezone.utc)
    sql = build_sql(args, env, now)
    if args.print_only:
        print(sql)
        return 0
    if io is None:
        from nightshift.aidp_io import AidpIO

        io = AidpIO(env, dry_run=True)  # dry_run governs control-plane mutations; ledger writes are SQL
    try:
        if args.cmd == "list":
            print(format_pending(io.run_sql(sql)))
            return 0
        io.run_sql(sql)
        rows = io.run_sql(f"SELECT approval_id, status, approved_by, approved_at, note FROM {env.ops}.approvals "
                          f"WHERE approval_id = {ledger.lit(args.approval_id)}")
        if not rows:
            print(f"no approval with id {args.approval_id}")
            return 1
        row = rows[0]
        status = row[1]
        print(json.dumps({"approval_id": row[0], "status": status, "approved_by": row[2],
                          "approved_at": str(row[3]), "note": row[4]}, indent=1, default=str))
        if status != {"approve": "approved", "reject": "rejected"}[args.cmd]:
            print(f"row was not pending (status={status}); nothing changed")
            return 1
        if args.cmd == "approve":
            print("approved: the next actuator tick executes it through the gate (python scripts/run_tick.py)")
        return 0
    finally:
        close = getattr(io, "close", None)
        if close:
            close()


if __name__ == "__main__":
    raise SystemExit(main())
