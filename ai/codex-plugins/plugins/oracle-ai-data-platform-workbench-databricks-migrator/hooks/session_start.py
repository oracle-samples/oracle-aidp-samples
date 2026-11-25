#!/usr/bin/env python3
"""SessionStart hook for the AIDP Databricks Migrator Codex plugin.

Stages the bundled OpenAI-based migration engine to ~/.aidp-migrator/engine so
skills can invoke a stable path without requiring a separate repository clone
or knowledge of Codex's plugin cache path.
"""
import os
import shutil
import sys


PLUGIN_ROOT = os.environ.get("PLUGIN_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(PLUGIN_ROOT, "engine")
DST_ROOT = os.path.join(os.path.expanduser("~"), ".aidp-migrator")
DST = os.path.join(DST_ROOT, "engine")
SKIP_DIRS = {"__pycache__", ".pytest_cache", ".git", "reports", "env"}


def _manifest_path() -> str:
    return os.path.join(DST_ROOT, ".staged_manifest.json")


def _load_manifest() -> set:
    try:
        import json
        with open(_manifest_path()) as fh:
            return set(json.load(fh))
    except (OSError, ValueError):
        return set()


def _prune(src: str, dst: str, previously_staged: set) -> int:
    """Remove ONLY files this hook itself staged earlier that the bundled
    engine no longer ships (e.g. 0.2.x scripts removed in 0.3.0). Everything
    else in the staging dir is deployment-owned — customer YAML, an activated
    config/writer_wrappers.json, generated reports — and is never touched.
    With no manifest from a previous staging, nothing is pruned."""
    removed = 0
    for rel in sorted(previously_staged, reverse=True):
        if not os.path.exists(os.path.join(src, rel)) and not rel.endswith(".pyc"):
            target = os.path.join(dst, rel)
            if os.path.isfile(target):
                try:
                    os.remove(target)
                    removed += 1
                except OSError:
                    pass
    # drop directories the prune emptied (bundle-owned leftovers only)
    for root, dirs, files in os.walk(dst, topdown=False):
        if root != dst and not dirs and not files:
            rel = os.path.relpath(root, dst)
            if any(p.startswith(rel + os.sep) or p == rel for p in previously_staged):
                try:
                    os.rmdir(root)
                except OSError:
                    pass
    return removed


def stage():
    if not os.path.isdir(SRC):
        print(f"[aidp-migrator] bundled engine not found at {SRC}", file=sys.stderr)
        return False
    os.makedirs(DST, exist_ok=True)
    pruned = _prune(SRC, DST, _load_manifest())
    if pruned:
        print(f"[aidp-migrator] pruned {pruned} stale bundle file(s) from {DST}")
    staged_now = []
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        rel = os.path.relpath(root, SRC)
        out = DST if rel == "." else os.path.join(DST, rel)
        os.makedirs(out, exist_ok=True)
        for name in files:
            staged_now.append(name if rel == "." else os.path.join(rel, name))
            if name.endswith((".pyc", ".pyo")):
                continue
            shutil.copy2(os.path.join(root, name), os.path.join(out, name))
    try:
        import json
        with open(_manifest_path(), "w") as fh:
            json.dump(sorted(staged_now), fh)
    except OSError:
        pass

    return True


def main():
    try:
        if stage():
            print(
                "[aidp-migrator] engine staged to ~/.aidp-migrator/engine. "
                "Run: python -m pip install -r ~/.aidp-migrator/engine/requirements.txt; "
                "set OPENAI_API_KEY plus AIDP/OCI/Databricks coordinates before migration."
            )
    except Exception as exc:
        print(
            f"[aidp-migrator] stage warning: {exc}. "
            "Run hooks/session_start.py manually from the plugin root.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
