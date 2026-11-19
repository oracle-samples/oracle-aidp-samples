#!/usr/bin/env python3
"""Classify Fusion-PVO column drift and route it to a fix — the testable core
of the ``fusion-drift-doctor`` skill.

The scenario: the content pack / bronze table is unchanged, but a **Fusion PVO
source column got renamed/removed** (Oracle revs PVOs across releases). The
runtime gates DETECT this (AIDPF-2072 PVO drift gate; AIDPF-4070/4071 per-node
source-schema gates) and fail the run — but they don't decide *how to fix it*.
This module turns the drift facts into a per-variation-point classification and
a route:

  present            — the resolved physical column is still in the live PVO. OK.
  renamed_resolvable — resolved column gone, but ANOTHER declared candidate IS
                       present → `bootstrap --refresh` re-pins to it (mechanical).
  needs_overlay      — NO declared candidate is present in the live PVO → a NEW
                       column name the pack never anticipated → `/medallion-author`
                       drafts an overlay extending columnAliases.<name>.candidates.
  missing_literal    — a declared literal (non-alias) required column vanished
                       from the PVO → investigate (pack/source mismatch).

(This classifier is column-NAME drift only. A column-TYPE drift — AIDPF-4070 —
is routed separately to `/medallion-author` to draft a bronze type-overlay
retyping the column to the live type; see the SKILL.md routing table.)

Input JSON (stdin or --input):
    {
      "live": {"ap_invoices": ["ApInvoicesInvoiceId", "ApInvoicesCurrencyCode", ...], ...},
      "aliases": [
        {"name":"invoice_currency_code","source":"ap_invoices",
         "resolved":"ApInvoicesInvoiceCurrencyCode",
         "candidates":["ApInvoicesInvoiceCurrencyCode","ApInvoicesCurrencyCode"]}
      ],
      "required_literals": [{"source":"ap_invoices","column":"ApInvoicesVendorId"}]   # optional
    }

Output JSON: {ok, findings:[...], routes:{bootstrap_refresh:[],medallion_author:[],investigate:[]}, summary}.
The live columns / candidate lists / resolved picks are the operator's evidence
— acquire them from a live PVO probe or a failed run's AIDPF-2072 diagnostic.
"""
from __future__ import annotations

import argparse
import json
import sys


def _present(col: str | None, live_cols: list[str]) -> bool:
    if not col:
        return False
    lower = {c.lower() for c in live_cols}
    return col.lower() in lower


def classify(payload: dict) -> dict:
    live = payload.get("live") or {}
    aliases = payload.get("aliases") or []
    literals = payload.get("required_literals") or []

    findings: list[dict] = []

    for a in aliases:
        name = a.get("name")
        source = a.get("source")
        resolved = a.get("resolved")
        candidates = a.get("candidates") or []
        live_cols = live.get(source) or []

        if _present(resolved, live_cols):
            findings.append({
                "source": source, "alias": name, "resolved": resolved,
                "status": "present", "route": "none",
                "detail": f"{resolved!r} still present in live {source} PVO.",
            })
            continue

        other = next((c for c in candidates if c != resolved and _present(c, live_cols)), None)
        if other is not None:
            findings.append({
                "source": source, "alias": name, "resolved": resolved,
                "observedCandidate": other, "status": "renamed_resolvable",
                "route": "bootstrap_refresh",
                "detail": (f"{resolved!r} gone from live {source}; candidate {other!r} IS present "
                           f"— `bootstrap --refresh` re-pins {name!r} to it."),
            })
            continue

        # No declared candidate present — a new name the pack never anticipated.
        findings.append({
            "source": source, "alias": name, "resolved": resolved,
            "status": "needs_overlay", "route": "medallion_author",
            "liveColumns": live_cols,
            "detail": (f"none of {name!r} candidates {candidates} present in live {source}; "
                       f"draft an overlay extending columnAliases.{name}.candidates with the "
                       f"tenant's column via /medallion-author."),
        })

    for lit in literals:
        source = lit.get("source")
        column = lit.get("column")
        if not _present(column, live.get(source) or []):
            findings.append({
                "source": source, "column": column, "status": "missing_literal",
                # Primary route stays `investigate`: the classifier cannot decide
                # whether the absence is a real pack/source bug or a legitimate
                # tenant variation — a human must. `relax_route` is the
                # machine-readable hint for the legitimate-absence remediation so
                # automation can surface (never auto-apply) it.
                "route": "investigate",
                "relax_route": "medallion_author",
                "detail": (f"declared literal column {column!r} missing from live {source} PVO. "
                           "Two cases — decide with the operator: (a) it SHOULD exist (real "
                           "pack/source mismatch) → investigate/fix; (b) it is LEGITIMATELY "
                           "absent for this tenant → relax the requiredColumns assertion via a "
                           "`relaxRequiredColumns` block overlay (with a reason) through "
                           "/medallion-author (AIDPF-2062/2063 guard the mechanism)."),
            })

    routes: dict[str, list[str]] = {"bootstrap_refresh": [], "medallion_author": [], "investigate": []}
    for f in findings:
        r = f["route"]
        if r in routes:
            routes[r].append(f.get("alias") or f.get("column"))
    # missing_literal findings carry an optional relax remediation (operator
    # decides). Surface it in a dedicated bucket without removing them from
    # `investigate` (their primary, judgment-required route).
    routes["relax_overlay"] = [
        f.get("column") for f in findings if f.get("relax_route") == "medallion_author"
    ]
    summary: dict[str, int] = {}
    for f in findings:
        summary[f["status"]] = summary.get(f["status"], 0) + 1

    return {
        "ok": all(f["status"] == "present" for f in findings),
        "findings": findings,
        "routes": {k: sorted(v) for k, v in routes.items()},
        "summary": summary,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Classify Fusion-PVO drift and route it to a fix.")
    ap.add_argument("--input", default="-", help="Path to drift JSON, or '-' for stdin.")
    ns = ap.parse_args(argv)
    text = sys.stdin.read() if ns.input == "-" else open(ns.input, encoding="utf-8").read()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid drift JSON: {exc}") from exc
    print(json.dumps(classify(payload), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
