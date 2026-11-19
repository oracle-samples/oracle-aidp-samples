#!/usr/bin/env python3
"""SessionStart hook for the AIDP Engineer Agent Codex plugin.

Stages the bundled Spark-SQL helper (the plugin's `aidp/` dir) to `~/.aidp/` so every skill's
`$HOME/.aidp/aidp_sql.py` reference resolves, then runs the staged readiness check
(`~/.aidp/check_env.py`) which installs the helper's Python deps (only if missing) and reports
OCI readiness. Idempotent and safe to run every session. Cross-platform (uses the interpreter
running the hook).

If your environment can't run this hook (e.g. `python3` isn't on PATH), run it manually once:
    python "<plugin-root>/hooks/session_start.py"
"""
import os
import sys
import shutil
import subprocess

# PLUGIN_ROOT is set by Codex for plugin hooks; fall back to this file's grandparent.
plugin_root = os.environ.get("PLUGIN_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = os.path.join(plugin_root, "aidp")
dst = os.path.join(os.path.expanduser("~"), ".aidp")


def stage():
    if not os.path.isdir(src):
        print(f"[aidp] helper source not found at {src}", file=sys.stderr)
        return False
    os.makedirs(dst, exist_ok=True)
    for root, _dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        out = dst if rel == "." else os.path.join(dst, rel)
        os.makedirs(out, exist_ok=True)
        for f in files:
            shutil.copy2(os.path.join(root, f), os.path.join(out, f))
    return True


def check_env():
    """Run the staged readiness check (installs deps if missing + reports OCI readiness)."""
    checker = os.path.join(dst, "check_env.py")
    if not os.path.exists(checker):
        return
    try:
        subprocess.run([sys.executable, checker], timeout=300)  # its banner flows to stdout
    except Exception:
        pass  # best-effort; never block the session on the readiness check


def main():
    try:
        if stage():
            check_env()
            print("[aidp] helper staged to ~/.aidp. "
                  "Set AIDP_REGION / AIDP_DATALAKE / AIDP_WORKSPACE / AIDP_CLUSTER (or fill ~/.codex/AGENTS.md).")
    except Exception as e:  # never block the session on a hook failure
        print(f"[aidp] stage warning: {e}. Run hooks/session_start.py manually, or the bundle install.sh.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
