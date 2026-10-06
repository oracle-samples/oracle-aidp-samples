"""The phase diagram, generated from STAGES so it cannot drift from the code.

Mermaid, in the style of the component diagrams at the plugin root: one
subgraph per phase group, nodes in pipeline order, thick arrows into a stage
that writes to AIDP, a dotted link between two stages that are alternatives.
Given a stage board, nodes are coloured by what has actually run.

ARCHITECTURE.md carries this output (without its %% comment lines) in a
```mermaid block between the phase-diagram markers; `stages
--write-diagram` refreshes it and a test fails if the two differ.
"""
from __future__ import annotations

from .stages import STAGES

__all__ = ["phase_diagram", "GROUP_TITLES", "DIAGRAM_BEGIN", "DIAGRAM_END",
           "architecture_block", "embed_in_architecture"]

GROUP_TITLES = {
    "setup": "Setup &nbsp;&#40;environment and catalogs&#41;",
    "discovery": "Discovery &nbsp;&#40;read-only against Snowflake&#41;",
    "planning": "Planning &nbsp;&#40;offline, no network&#41;",
    "target": "Target &nbsp;&#40;AIDP structure and data plane&#41;",
    "reporting": "Reporting",
    "teardown": "Teardown &nbsp;&#40;release or remove what the migration created&#41;",
}


def _node(stage: str) -> str:
    return stage.upper().replace("-", "_")


def _label(spec: dict) -> str:
    parts = [f'<b>{spec["stage"]}</b>']
    if spec.get("runbook") and spec["runbook"] != "-":
        parts.append(f'runbook {spec["runbook"]}')
    if spec.get("runs_on"):
        parts.append(f'<i>{spec["runs_on"]}</i>')
    return "<br/>".join(parts)


def phase_diagram(board: dict | None = None) -> str:
    out = ["%% Snowflake -> Oracle AIDP migrator: phase diagram",
           "%% GENERATED from engine/report/stages.py STAGES -- do not edit by "
           "hand;",
           "%% regenerate with `bin/snowmig stages --write-diagram`.",
           "%% Solid = next phase, thick = into a phase that writes to AIDP, "
           "dotted = alternative path.",
           "", "flowchart TB", ""]

    groups: list[str] = []
    for spec in STAGES:
        if spec["phase"] not in groups:
            groups.append(spec["phase"])
    for group in groups:
        # PHASE_-prefixed: a phase and a stage may share a name (`teardown`),
        # and Mermaid reads a subgraph id equal to a node id as a cycle.
        out.append(f'  subgraph PHASE_{group.upper()}'
                   f'["{GROUP_TITLES.get(group, group)}"]')
        out.append("    direction TB")
        for spec in STAGES:
            if spec["phase"] == group:
                out.append(f'    {_node(spec["stage"])}["{_label(spec)}"]')
        out += ["  end", ""]

    for prev, spec in zip(STAGES, STAGES[1:]):
        arrow = "==>" if spec.get("writes") else "-->"
        out.append(f'  {_node(prev["stage"])} {arrow} {_node(spec["stage"])}')
    seen = set()
    for spec in STAGES:
        twin = spec.get("alternative_to")
        pair = tuple(sorted((spec["stage"], twin or "")))
        if twin and pair not in seen:
            seen.add(pair)
            out.append(f'  {_node(spec["stage"])} -. or .- {_node(twin)}')
    out.append("")

    out += ["  classDef local fill:#eef6ff,stroke:#5b8dd9,color:#12314f",
            "  classDef writer fill:#fff1e6,stroke:#d98b3a,color:#5a3410",
            "  classDef optional fill:#f2f2f2,stroke:#888,color:#222,"
            "stroke-dasharray: 4 3",
            "  classDef done fill:#e8f6ea,stroke:#3c9a4c,color:#173d1e",
            "  classDef attention fill:#fdeaea,stroke:#c94343,color:#4d1414",
            ""]
    kinds: dict[str, list[str]] = {"local": [], "writer": [], "optional": []}
    for spec in STAGES:
        kind = ("writer" if spec.get("writes")
                else "optional" if spec.get("optional") else "local")
        kinds[kind].append(_node(spec["stage"]))
    for kind, nodes in kinds.items():
        if nodes:
            out.append(f'  class {",".join(nodes)} {kind}')

    if board:
        # A dry run wrote its artifact and created nothing: not green.
        done = [_node(r["stage"]) for r in board.get("stages") or []
                if r["status"] in ("DONE", "SATISFIED")
                and not r.get("attention") and not r.get("dry_run")]
        flagged = [_node(r["stage"]) for r in board.get("stages") or []
                   if r.get("attention")]
        if done:
            out.append(f'  class {",".join(done)} done')
        if flagged:
            out.append(f'  class {",".join(flagged)} attention')
    return "\n".join(out) + "\n"


DIAGRAM_BEGIN = "<!-- phase-diagram:begin -->"
DIAGRAM_END = "<!-- phase-diagram:end -->"


def architecture_block() -> str:
    """The diagram as ARCHITECTURE.md embeds it: a ```mermaid block, the
    %% comment lines left out (the surrounding prose explains the arrows)."""
    body = "\n".join(line for line in phase_diagram().splitlines()
                     if not line.startswith("%%")).strip("\n")
    return f"{DIAGRAM_BEGIN}\n```mermaid\n{body}\n```\n{DIAGRAM_END}"


def embed_in_architecture(text: str) -> str:
    """ARCHITECTURE.md's text with the block between the markers replaced.
    Raises ValueError when the markers are missing."""
    start, end = text.find(DIAGRAM_BEGIN), text.find(DIAGRAM_END)
    if start < 0 or end < start:
        raise ValueError("ARCHITECTURE.md has no phase-diagram markers")
    return text[:start] + architecture_block() + text[end + len(DIAGRAM_END):]
