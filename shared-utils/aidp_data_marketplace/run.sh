#!/usr/bin/env bash
# Launch the AIDP Marketplace UI.
#   ./run.sh                 # uses ../../venv if present, else system python
#   PORT=9000 ./run.sh       # custom port (default 8090)
set -euo pipefail
cd "$(dirname "$0")"

PY="../../venv/bin/python"
[ -x "$PY" ] || PY="python3"

# Ensure deps (Flask) are available; install into the active interpreter if not.
"$PY" -c "import flask, oci, requests, yaml" 2>/dev/null || "$PY" -m pip install -r requirements.txt

PORT="${PORT:-8090}"
echo "AIDP Marketplace UI -> http://127.0.0.1:${PORT}/"
PORT="$PORT" "$PY" app.py
