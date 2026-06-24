#!/usr/bin/env python3
"""SessionStart readiness check for the oracle-ai-data-platform-workbench-engineer-agent Codex plugin.

Invoked by the SessionStart hook (hooks/session_start.py) after it stages the helper to ~/.aidp/. It:
  1. Ensures the bundled Python deps (requirements.txt, a sibling of this file) are installed -auto `pip install`
     ONLY if an import check fails, then writes a one-time sentinel so later sessions are instant.
  2. Reports local OCI readiness (the `oci` CLI + a ~/.oci/config profile) -the one thing the plugin
     CANNOT bundle (per-user secrets). It does NOT do a network/auth call here (kept fast); the
     `aidp-engineer-bootstrap` skill does the live AIDP reachability check.

NEVER blocks the session: always exits 0. Prints a concise one-line banner to stdout so the result
shows in the session context. Set AIDP_PLUGIN_NO_AUTOINSTALL=1 to make it check-only (no pip).
"""
import os, sys, subprocess, shutil

# requirements.txt sits next to this file -true both in the plugin's aidp/ dir and in the staged ~/.aidp/ copy.
HERE = os.path.dirname(os.path.abspath(__file__))
REQ = os.path.join(HERE, "requirements.txt")
SENTINEL = os.path.join(HERE, ".aidp_deps_ok")
MODS = ("oci", "requests", "websocket", "cryptography")  # websocket-client imports as `websocket`
NO_AUTO = os.environ.get("AIDP_PLUGIN_NO_AUTOINSTALL") == "1"

out = []

def missing():
    m = []
    for mod in MODS:
        try:
            __import__(mod)
        except Exception:
            m.append(mod)
    return m

# 1. Python deps
need = missing()
if not need:
    out.append("deps OK")
elif NO_AUTO:
    out.append("deps MISSING (%s) -run: python -m pip install -r requirements.txt" % ",".join(need))
elif os.path.exists(SENTINEL):
    out.append("deps MISSING after prior install (%s) -run: python -m pip install -r requirements.txt" % ",".join(need))
else:
    err = ""
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", REQ], timeout=280, check=False)
    except Exception as e:
        err = " [%s]" % str(e)[:60]   # e.g. a pip timeout — only surfaced if deps are STILL missing
    need = missing()
    if not need:
        try:
            open(SENTINEL, "w").write("ok")
        except Exception:
            pass
        out.append("deps auto-installed")
    else:
        out.append("deps still MISSING (%s)%s -- run: python -m pip install -r requirements.txt" % (",".join(need), err))

# 2. OCI CLI (used by the oci raw-request control-plane path)
out.append("oci CLI " + ("OK" if shutil.which("oci") else "NOT FOUND -install it (TESTING.md)"))

# 3. OCI config profile (the irreducible per-user step -fast local check only)
cfg = os.path.expanduser("~/.oci/config")
out.append("~/.oci/config " + ("present" if os.path.exists(cfg) else "MISSING -run `oci session authenticate` / `oci setup config`"))

ready = all(s for s in (not need, shutil.which("oci"), os.path.exists(cfg)))
banner = "[aidp-engineer-agent] " + " | ".join(out)
print(banner)
if not ready:
    print("[aidp-engineer-agent] Setup incomplete -run the `aidp-engineer-bootstrap` skill for guided setup + a live AIDP check.")
else:
    print("[aidp-engineer-agent] Ready. (First data call still needs your DataLake OCID + workspace + an ACTIVE cluster -`aidp-engineer-bootstrap` detects them.)")
sys.exit(0)
