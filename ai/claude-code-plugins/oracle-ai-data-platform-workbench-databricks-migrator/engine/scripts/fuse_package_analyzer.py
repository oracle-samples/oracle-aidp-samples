"""
AIDP FUSE Package Analyzer
============================
Uses Claude Sonnet to analyze installed Python packages for FUSE risk.
For packages NOT in the built-in fuse_scanner.py FUSE_RISKY_PACKAGES dict.

For each package:
1. pip show <pkg> to find the installed source location
2. Read key I/O modules (looking for file open, mmap, sqlite, WAL patterns)
3. Feed source + prompt to Claude Sonnet for FUSE risk analysis
4. Return a risk card in FUSE_RISKY_PACKAGES format
5. Append to scripts/fuse_risk_db.json for future scanner use

Usage:
    python3 scripts/fuse_package_analyzer.py --packages econml hyperopt skopt knockknock
    python3 scripts/fuse_package_analyzer.py --packages econml --output /tmp/econml_risk.json
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Claude API client
# ---------------------------------------------------------------------------

def _get_anthropic_client():
    try:
        import anthropic
    except ImportError:
        print("anthropic SDK not installed. Run: pip install anthropic", file=sys.stderr)
        sys.exit(1)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    return anthropic.Anthropic(api_key=api_key)


# ---------------------------------------------------------------------------
# Package source discovery
# ---------------------------------------------------------------------------

def _pip_show(package: str) -> dict:
    """Run pip show and parse the output."""
    try:
        out = subprocess.check_output(
            [sys.executable, "-m", "pip", "show", package],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except subprocess.CalledProcessError:
        return {}

    info = {}
    for line in out.splitlines():
        if ": " in line:
            key, _, value = line.partition(": ")
            info[key.strip()] = value.strip()
    return info


def _find_package_source(package: str) -> str | None:
    """Return the directory containing the installed package source."""
    info = _pip_show(package)
    location = info.get("Location")
    if not location:
        return None

    # Try package name as directory
    for candidate in [package, package.replace("-", "_"), package.lower()]:
        path = os.path.join(location, candidate)
        if os.path.isdir(path):
            return path

    return None


def _collect_io_source(pkg_dir: str, max_chars: int = 40_000) -> str:
    """
    Collect source from files most likely to contain I/O operations.
    Priority: files matching *io*, *storage*, *persist*, *cache*, *backend*, *file*, *disk*, *db*
    Fall back to any .py files.
    """
    if not pkg_dir or not os.path.isdir(pkg_dir):
        return ""

    priority_keywords = ["io", "storage", "persist", "cache", "backend", "file", "disk", "db",
                         "store", "write", "read", "dump", "load", "save", "serial"]

    all_py = list(Path(pkg_dir).rglob("*.py"))
    priority = [f for f in all_py if any(kw in f.stem.lower() for kw in priority_keywords)]
    rest = [f for f in all_py if f not in set(priority)]

    collected = []
    total = 0
    for f in priority + rest:
        if total >= max_chars:
            break
        try:
            src = f.read_text(errors="replace")
        except Exception:
            continue
        # Only include files with potential I/O patterns
        if not any(kw in src for kw in [
            "open(", "os.path", "sqlite", "mmap", "tempfile", "shutil",
            "pathlib", ".write(", ".read(", "pickle", "json.dump",
        ]):
            continue
        header = f"\n\n# === {f.relative_to(Path(pkg_dir).parent)} ===\n"
        chunk = header + src[:max_chars - total - len(header)]
        collected.append(chunk)
        total += len(chunk)

    return "".join(collected)


# ---------------------------------------------------------------------------
# Claude analysis
# ---------------------------------------------------------------------------

_ANALYSIS_PROMPT = """You are analyzing a Python package for FUSE filesystem compatibility on OCI AIDP.

FUSE context:
- /Volumes/ paths are OCI Object Storage mounted via FUSE
- FUSE breaks: SQLite WAL mode, byte-range file locking, POSIX rename atomicity, memory-mapped files (mmap),
  multi-step directory writes (TF SavedModel), direct inode caching
- Safe: regular sequential read/write via open(), cloud SDK calls, in-memory operations

Package: {package}
pip show info:
{pip_info}

Key source files (I/O-related):
```python
{source}
```

Analyze this package for FUSE risks. Return a JSON object ONLY (no prose) with exactly these fields:
{{
  "risk": "HIGH" | "MEDIUM" | "LOW" | "NONE",
  "reason": "one sentence explaining the FUSE risk",
  "patterns": ["list of regex patterns that detect risky usage in notebook source"],
  "mitigation": "one sentence fix",
  "import_names": ["list of import aliases users might use, e.g. import lightgbm as lgb → ['lightgbm', 'lgb']"]
}}

If no FUSE risk, set risk to "NONE" and patterns to [].
Respond with ONLY the JSON object, no markdown fences, no explanation.
"""


def analyze_package(client, package: str) -> dict:
    """Analyze one package for FUSE risk using Claude Sonnet."""
    print(f"  Analyzing {package}...", file=sys.stderr)

    pip_info = _pip_show(package)
    pip_info_str = "\n".join(f"{k}: {v}" for k, v in pip_info.items()) if pip_info else "(not installed)"

    pkg_dir = _find_package_source(package)
    source = _collect_io_source(pkg_dir) if pkg_dir else ""
    if not source:
        source = "(source not found — analyzing based on package name and pip metadata only)"

    prompt = _ANALYSIS_PROMPT.format(
        package=package,
        pip_info=pip_info_str,
        source=source[:35_000],  # cap to stay within context
    )

    message = client.messages.create(
        model="claude-opus-4-8",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()

    # Strip markdown fences if model wrapped it anyway
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        print(f"  Warning: could not parse JSON for {package}: {raw[:200]}", file=sys.stderr)
        result = {
            "risk": "UNKNOWN",
            "reason": f"Analysis failed: model returned non-JSON: {raw[:100]}",
            "patterns": [],
            "mitigation": "Manual review required.",
            "import_names": [package],
        }

    result["import_names"] = result.get("import_names") or [package]
    return result


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

def _load_db(db_path: str) -> dict:
    if os.path.exists(db_path):
        try:
            with open(db_path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_db(db_path: str, db: dict) -> None:
    with open(db_path, "w") as f:
        json.dump(db, f, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Analyze Python packages for FUSE risk using Claude Sonnet")
    parser.add_argument("--packages", nargs="+", required=True,
                        help="Package names to analyze (pip package names)")
    parser.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "fuse_risk_db.json"),
                        help="Path to fuse_risk_db.json (default: scripts/fuse_risk_db.json)")
    parser.add_argument("--output", help="Write results JSON to this path (default: stdout)")
    parser.add_argument("--force", action="store_true",
                        help="Re-analyze packages already in the DB")
    args = parser.parse_args()

    client = _get_anthropic_client()
    db = _load_db(args.db)
    results = {}

    for pkg in args.packages:
        if pkg in db and not args.force:
            print(f"  {pkg}: already in DB (risk={db[pkg]['risk']}) — skipping. Use --force to re-analyze.", file=sys.stderr)
            results[pkg] = db[pkg]
            continue

        risk_card = analyze_package(client, pkg)
        db[pkg] = risk_card
        results[pkg] = risk_card
        print(f"  {pkg}: risk={risk_card['risk']} — {risk_card['reason'][:80]}", file=sys.stderr)

    _save_db(args.db, db)
    print(f"  DB updated: {args.db} ({len(db)} entries)", file=sys.stderr)

    output = json.dumps(results, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
    else:
        print(output)


if __name__ == "__main__":
    main()
