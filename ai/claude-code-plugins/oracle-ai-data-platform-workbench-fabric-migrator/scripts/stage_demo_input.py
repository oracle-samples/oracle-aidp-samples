#!/usr/bin/env python3
"""Assemble the one workspace `make demo` migrates.

It holds everything this tool has to read:

  * the bundled Acme estate -- synthetic, written alongside the rules, and the
    only source of Lakehouse shortcuts, a semantic model and a Dataflow with a
    declared write target;
  * 78 real artifacts from 17 public GitHub repositories, every one under a
    permissive licence (see tests/fixtures/real/corpora/NOTICE).

Both, because they fail differently. The synthetic estate covers every feature
and can never surprise the rules that were written against it. The real files
surprise them constantly -- the warehouse asset-id collision, the Synapse
notebook header and the activities buried in a ForEach were all found here --
but no single public repository exercises the whole tool.

    python3 scripts/stage_demo_input.py         # -> demo-input/
    fabric-aidp inventory demo-input -o out/inventory.json

The output is derived, not source: gitignored, rebuilt on demand. Item names
carry their source repository so any row in a report traces back to its file.
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CORPORA = ROOT / "tests" / "fixtures" / "real" / "corpora"
DEFAULT_OUT = ROOT / "demo-input"

_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe(text: str) -> str:
    return _UNSAFE.sub("_", text).strip("_") or "item"


def _platform(kind: str, name: str, logical_id: str) -> str:
    return json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                   "gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": kind, "displayName": name,
                     "description": "vendored third-party sample"},
        "config": {"version": "2.0", "logicalId": logical_id},
    }, indent=2) + "\n"


def _entries(kind: str):
    provenance = CORPORA / kind / "PROVENANCE.json"
    if not provenance.is_file():
        return []
    return json.load(open(provenance, encoding="utf-8"))["files"]


BUNDLED = ROOT / "fabric_aidp" / "fixtures" / "demo-workspace"


def build(out_dir: Path) -> dict:
    if not CORPORA.is_dir():
        raise SystemExit(f"no vendored corpora at {CORPORA}")
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    counts = {}
    # The bundled estate first: it is the only source of Lakehouse shortcuts,
    # a semantic model, and a Dataflow that declares where it writes.
    for item in sorted(BUNDLED.iterdir()):
        if item.is_dir():
            shutil.copytree(item, out_dir / item.name)
            counts["Acme estate items"] = counts.get("Acme estate items", 0) + 1
    plan = [
        ("notebooks", "Notebook", "notebook-content.py"),
        ("pipelines", "DataPipeline", "pipeline-content.json"),
        ("corpus", "Dataflow", "mashup.pq"),
    ]
    for kind, item_type, filename in plan:
        made = 0
        for entry in _entries(kind):
            source = CORPORA / kind / entry["file"]
            if not source.is_file():
                continue
            stem = Path(entry["file"]).stem
            name = _safe(f"{entry['repo'].replace('/', '_')}_{stem}")
            item = out_dir / f"{name}.{item_type}"
            item.mkdir()
            (item / ".platform").write_text(
                _platform(item_type, name, f"{kind}-{stem}"), encoding="utf-8")
            shutil.copy2(source, item / filename)
            if item_type == "Dataflow":
                (item / "queryMetadata.json").write_text(
                    json.dumps({"queriesMetadata": {}}, indent=2) + "\n",
                    encoding="utf-8")
            made += 1
        counts[item_type] = made

    # Warehouse objects are .sql files inside a Warehouse item, not one item
    # per file. Group them by source repository: two repositories both define
    # `dbo.CurrentDate`, and piling every file into one warehouse produced a
    # workspace Fabric could never export -- `plan` rightly refused it with
    # "duplicate asset id(s)".
    grouped = {}
    for entry in _entries("warehouse"):
        if not (CORPORA / "warehouse" / entry["file"]).is_file():
            continue
        # Group by the warehouse each file actually came out of, read from its
        # recorded path. Grouping by repository is not enough: one repo holds
        # two synced copies of the same warehouse, so `dbo.CurrentDate` appears
        # twice and the two must not land in one item.
        # Key on the FULL path to the warehouse directory, not its name: one
        # repo holds two synced copies of `WareSecondary.Warehouse`, so the
        # name alone still collides and `dbo.CurrentDate` lands twice.
        path = entry.get("path") or ""
        match = re.search(r"^(.*?([^/]+)\.Warehouse)/", path)
        key = (entry["repo"], match.group(1) if match else "Ungrouped")
        origin = match.group(2) if match else "Ungrouped"
        grouped.setdefault(key, (origin, []))[1].append(entry)
    objects = 0
    used = {}
    for (repo, _path), (origin, entries) in grouped.items():
        base = _safe(f"{repo.split('/')[-1]}_{origin}")
        seen = used.get(base, 0)
        used[base] = seen + 1
        name = base if seen == 0 else f"{base}_{seen + 1}"
        item = out_dir / f"{name}.Warehouse"
        item.mkdir()
        (item / ".platform").write_text(
            _platform("Warehouse", name, f"warehouse-{name}"), encoding="utf-8")
        for entry in entries:
            shutil.copy2(CORPORA / "warehouse" / entry["file"],
                         item / entry["file"])
            objects += 1
    counts["Warehouse"] = len(grouped)
    counts["Warehouse objects"] = objects
    return counts


def main() -> int:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    counts = build(out_dir)
    print(f"staged {out_dir}")
    for kind, n in counts.items():
        print(f"  {n:4}  {kind}")
    print(f"\n  fabric-aidp inventory {out_dir} -o out/inventory.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
