#!/usr/bin/env python3
"""Write a non-secret resume checkpoint for the Fusion autopilot workflow.

The checkpoint survives Claude Code restarts required by MCP setup/reconnect.
It is intentionally Markdown so a fresh agent can read it without a special
parser, while still being structured enough to resume the next phase.
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path


def _items(values: list[str], empty: str) -> str:
    if not values:
        return f"- {empty}\n"
    return "".join(f"- {value}\n" for value in values)


def _fence(value: str) -> str:
    return f"```text\n{value.strip()}\n```\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", default=".", help="Customer bundle directory")
    parser.add_argument("--goal", required=True, help="User dashboard or workflow goal")
    parser.add_argument("--phase", required=True, help="Current workflow phase")
    parser.add_argument("--next-step", required=True, help="Next action after resume")
    parser.add_argument(
        "--resume-prompt",
        default="Resume the Fusion dashboard workflow from .aidp/autopilot/resume.md.",
        help="Prompt the user should paste after reconnect/restart",
    )
    parser.add_argument("--completed", action="append", default=[], help="Completed step")
    parser.add_argument("--pending", action="append", default=[], help="Pending step")
    parser.add_argument("--evidence", action="append", default=[], help="Evidence or artifact")
    parser.add_argument("--note", action="append", default=[], help="Non-secret note")

    args = parser.parse_args()
    root = Path(args.workdir).resolve()
    out = root / ".aidp" / "autopilot" / "resume.md"
    out.parent.mkdir(parents=True, exist_ok=True)

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    content = (
        "# AIDP Fusion Autopilot Resume\n\n"
        f"Updated: {now}\n\n"
        f"Goal: {args.goal.strip()}\n\n"
        f"Current phase: {args.phase.strip()}\n\n"
        f"Next step: {args.next_step.strip()}\n\n"
        "Resume prompt:\n\n"
        f"{_fence(args.resume_prompt)}\n"
        "Completed:\n\n"
        f"{_items(args.completed, 'None recorded')}\n"
        "Pending:\n\n"
        f"{_items(args.pending, 'None recorded')}\n"
        "Evidence:\n\n"
        f"{_items(args.evidence, 'None recorded')}\n"
        "Notes:\n\n"
        f"{_items(args.note, 'None')}\n"
        "Resume rules:\n\n"
        "- Re-probe live state after reconnect; do not rely only on this file.\n"
        "- Do not paste passwords, private keys, OAuth tokens, or full OCIDs into chat.\n"
        "- Treat disconnected OAC MCP as a connectivity failure, not an empty catalog.\n"
    )
    out.write_text(content, encoding="utf-8")
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
