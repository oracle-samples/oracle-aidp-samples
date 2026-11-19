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


def stage():
    if not os.path.isdir(SRC):
        print(f"[aidp-migrator] bundled engine not found at {SRC}", file=sys.stderr)
        return False
    os.makedirs(DST, exist_ok=True)
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        rel = os.path.relpath(root, SRC)
        out = DST if rel == "." else os.path.join(DST, rel)
        os.makedirs(out, exist_ok=True)
        for name in files:
            if name.endswith((".pyc", ".pyo")):
                continue
            shutil.copy2(os.path.join(root, name), os.path.join(out, name))
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
