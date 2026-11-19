#!/usr/bin/env python3
"""Deterministic intent parser for the ``aidp-fusion-seed`` skill.

Turns a loose natural-language seed request ("seed", "seed supplier_spend",
"seed just bronze", "resume the seed") into a structured, testable parse
result that the skill turns into an ``aidp-fusion-autopilot run --mode seed``
invocation.

**Why a real helper, not SKILL.md prose.** A misparse can seed too broad a
scope (e.g. "seed supplier" silently pulling six bronze deps via the D-1
implicit transitive include), so the intent→flag mapping is load-bearing and
MUST be executable + unit-testable. SKILL.md still *describes* the mapping for
the operator, but the skill calls THIS module to produce the real argv.

The parser is intentionally conservative: anything it cannot resolve to a
**known pack node id** is surfaced as ``unknown_tokens`` / ``ambiguous`` so the
skill can list the declared node ids and ask — it NEVER guesses a dataset.

Contract (stdout JSON):

    {
      "mode": "seed",
      "datasets": ["supplier_spend"],     # resolved known node ids, in order
      "layers": ["bronze"],               # subset of [bronze, silver, gold]
      "strict_scope": false,
      "resume_run_id": null,              # explicit run id if the user gave one
      "needs_run_id": false,              # resume intent but no id -> ask
      "unknown_tokens": [],               # target-looking words matching no node
      "ambiguous": [                      # target word -> candidate node ids
        {"token": "supplier", "candidates": ["dim_supplier", "erp_suppliers", "supplier_spend"]}
      ],
      "argv": ["run", "--mode", "seed", "--datasets", "supplier_spend"]
    }

Usage:
    python3 intent.py "seed supplier_spend" \
        --known-nodes dim_supplier,dim_account,supplier_spend,erp_suppliers,...

The skill assembles ``--known-nodes`` from the union of the bundle's bronze
dataset ids (``bundle.yaml datasets[]``) and the pack's silver+gold node ids
(``aidp-fusion-autopilot content-pack info <pack> --json``).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field

# Layer names the orchestrator's content-pack plan resolver understands.
LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")

# Words that carry control/grammar meaning, not a dataset target. Removed
# before we look for "leftover" tokens that might be (mis-typed) targets.
# Kept deliberately small — over-stuffing it would let a real typo slip
# through as "no unknown tokens" and seed the wrong scope.
_STOPWORDS: frozenset[str] = frozenset(
    {
        "seed", "the", "a", "an", "and", "or", "of", "for", "to", "please",
        "everything", "all", "full", "whole", "entire", "just", "only",
        "layer", "layers", "mart", "marts", "dimension", "dimensions", "dim",
        "data", "table", "tables", "run", "rerun", "re-run", "build",
        "materialize", "materialise", "load", "refresh", "pull",
        # resume vocabulary
        "resume", "continue", "finish", "died", "failed", "fail", "crashed",
        "interrupted", "restart", "id", "run_id", "runid",
        # strict-scope vocabulary
        "deps", "dependencies", "dependency", "already", "staged", "strict",
        "scope", "exact", "no-deps",
        # layer words handled separately
        "bronze", "silver", "gold",
    }
)

# Tokens that signal the user wants to RESUME a prior run rather than start
# a fresh seed.
_RESUME_TRIGGERS: frozenset[str] = frozenset(
    {"resume", "continue", "finish", "died", "failed", "crashed",
     "interrupted", "restart"}
)

# Tokens that signal --strict-scope (disable D-1 implicit transitive include).
_STRICT_PHRASES: tuple[str, ...] = (
    "deps already staged", "dependencies already staged",
    "strict scope", "strict-scope", "no deps", "no-deps", "exact scope",
)

# A plausible run_id shape. The orchestrator's run ids are opaque, but every
# observed form is a longish token of alnum + [-_]. We REQUIRE either an
# explicit "run <id>" / "run_id <id>" cue or a token that is clearly id-shaped
# (>=8 chars, contains a digit) so we never mistake an English word for an id.
_RUN_ID_CUE = re.compile(
    r"\b(?:run|run[_-]?id|id)\s+([A-Za-z0-9][A-Za-z0-9_\-\.]{3,})",
    re.IGNORECASE,
)
_RUN_ID_SHAPED = re.compile(r"^(?=.*\d)[A-Za-z0-9][A-Za-z0-9_\-\.]{7,}$")


@dataclass
class IntentResult:
    mode: str = "seed"
    datasets: list[str] = field(default_factory=list)
    layers: list[str] = field(default_factory=list)
    strict_scope: bool = False
    resume_run_id: str | None = None
    needs_run_id: bool = False
    unknown_tokens: list[str] = field(default_factory=list)
    ambiguous: list[dict] = field(default_factory=list)
    argv: list[str] = field(default_factory=list)


def _normalize(text: str) -> str:
    """Lowercase, collapse whitespace; keep word chars + spaces + - _ . ,"""
    return re.sub(r"\s+", " ", text.strip().lower())


def _node_spellings(node_id: str) -> list[str]:
    """Surface forms a node id can appear as in free text.

    ``supplier_spend`` -> {"supplier_spend", "supplier spend"}.
    Longest first so multi-word forms win over single tokens.
    """
    spaced = node_id.replace("_", " ")
    forms = {node_id.lower(), spaced.lower()}
    return sorted(forms, key=len, reverse=True)


def parse(phrase: str, known_nodes: list[str], mode: str = "seed") -> IntentResult:
    """Parse ``phrase`` against the set of ``known_nodes`` (pack node ids).

    Pure function — no I/O, no dispatch. Deterministic for a given
    (phrase, known_nodes, mode) tuple. ``mode`` (``seed`` default, or
    ``incremental``) sets the run mode + the emitted ``--mode`` flag, so the
    same parser serves both the seed and incremental skills.
    """
    result = IntentResult()
    result.mode = mode
    known = [n for n in dict.fromkeys(n.strip() for n in known_nodes) if n]
    known_lower = {n.lower(): n for n in known}

    norm = _normalize(phrase)

    # --- 1. resume intent -------------------------------------------------
    tokens = re.findall(r"[A-Za-z0-9_\-\.]+", norm)
    token_set = set(tokens)
    resume_intent = bool(_RESUME_TRIGGERS & token_set)

    run_id: str | None = None
    if resume_intent:
        m = _RUN_ID_CUE.search(phrase)  # search ORIGINAL (preserve id casing)
        if m:
            run_id = m.group(1)
        else:
            # No explicit cue — look for a free-standing id-shaped token.
            for t in re.findall(r"[A-Za-z0-9_\-\.]+", phrase):
                if _RUN_ID_SHAPED.match(t) and t.lower() not in known_lower:
                    run_id = t
                    break
        result.resume_run_id = run_id
        result.needs_run_id = run_id is None

    # --- 2. strict scope --------------------------------------------------
    for phr in _STRICT_PHRASES:
        if phr in norm:
            result.strict_scope = True
            break

    # --- 3. layers --------------------------------------------------------
    layers: list[str] = []
    for layer in LAYERS:
        if re.search(rf"\b{layer}\b", norm):
            layers.append(layer)
    # "the marts" with no explicit layer word -> silver + gold.
    if not layers and re.search(r"\bmarts?\b", norm):
        layers = ["silver", "gold"]
    result.layers = layers

    # --- 4. datasets: exact known-node matches (multi-word aware) ---------
    consumed = norm  # we blank out matched spans so leftovers are real leftovers
    matched: list[str] = []
    # Longest spellings across all nodes first, so "supplier spend" wins
    # before the bare "supplier" token is even considered.
    spellings: list[tuple[str, str]] = []
    for node in known:
        for form in _node_spellings(node):
            spellings.append((form, node))
    spellings.sort(key=lambda fv: len(fv[0]), reverse=True)
    for form, node in spellings:
        pattern = re.compile(rf"(?<!\w){re.escape(form)}(?!\w)")
        if pattern.search(consumed):
            if node not in matched:
                matched.append(node)
            consumed = pattern.sub(" ", consumed)
    result.datasets = matched

    # --- 5. leftover significant tokens -> unknown / ambiguous ------------
    leftover = [
        t
        for t in re.findall(r"[A-Za-z0-9_][A-Za-z0-9_\-\.]*", consumed)
        if len(t) > 2 and t not in _STOPWORDS and not t.isdigit()
    ]
    # Drop a leftover that is the resume run_id we already captured.
    if run_id:
        leftover = [t for t in leftover if t != run_id.lower()]

    for tok in dict.fromkeys(leftover):  # preserve order, dedup
        candidates = sorted(
            node for node in known
            if tok in node.lower() or node.lower() in tok
        )
        if not candidates:
            if tok not in result.unknown_tokens:
                result.unknown_tokens.append(tok)
        else:
            # Token partially matches one or more nodes but wasn't an exact
            # hit -> NEVER guess; surface candidates for the skill to ask.
            result.ambiguous.append({"token": tok, "candidates": candidates})

    result.argv = build_argv(result)
    return result


def build_argv(r: IntentResult) -> list[str]:
    """Build the ``run`` argv from a parse result.

    Always emits ``run --mode seed``. Datasets/layers/strict/resume are added
    when present. The skill MUST still gate on ``ambiguous`` / ``unknown_tokens``
    / ``needs_run_id`` before dispatching — argv reflects only what parsed
    cleanly.
    """
    argv = ["run", "--mode", r.mode]
    if r.datasets:
        argv += ["--datasets", ",".join(r.datasets)]
    if r.layers:
        argv += ["--layers", ",".join(r.layers)]
    if r.strict_scope:
        argv += ["--strict-scope"]
    if r.resume_run_id:
        argv += ["--resume", r.resume_run_id]
    return argv


# Run statuses that indicate a run did NOT finish cleanly and is therefore a
# resume candidate. Anything else (success, resumed_skipped) is not resumable.
_RESUMABLE_STATUSES: frozenset[str] = frozenset(
    {"failed", "running", "deferred", "aborted", "timeout", "interrupted"}
)


def resolve_resume_run_id(recent_runs: list[dict]) -> tuple[str | None, bool]:
    """Resolve a resume target from ``status --recent-runs --json`` output.

    ``recent_runs`` is a list of ``{run_id, status, ...}`` grouped by run_id.
    Returns ``(run_id, needs_ask)``:

      - exactly ONE resumable run        -> (that run_id, False)
      - zero resumable runs              -> (None, True)  — ask the user
      - more than one resumable run      -> (None, True)  — ambiguous, ask

    Never guesses: zero and multiple both force an explicit ask. This is the
    machine-readable replacement for scraping ``status`` text / treating
    ``.aidp/diagnostics/`` as a run index (which it is not).
    """
    resumable = [
        r for r in (recent_runs or [])
        if str(r.get("status", "")).lower() in _RESUMABLE_STATUSES and r.get("run_id")
    ]
    if len(resumable) == 1:
        return str(resumable[0]["run_id"]), False
    return None, True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Parse a natural-language seed request into run argv."
    )
    ap.add_argument("phrase", help="The raw user phrase, e.g. 'seed supplier_spend'.")
    ap.add_argument(
        "--known-nodes",
        default="",
        help="Comma-separated known pack node ids (bronze datasets + silver + gold).",
    )
    ap.add_argument(
        "--mode", default="seed", choices=["seed", "incremental"],
        help="Run mode for the emitted argv (default: seed).",
    )
    ns = ap.parse_args(argv)
    known = [s for s in ns.known_nodes.split(",") if s.strip()]
    result = parse(ns.phrase, known, mode=ns.mode)
    print(json.dumps(asdict(result), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
