#!/usr/bin/env bash
# Launch the SizeWise UI.
#   ./run.sh                 # uses ../../venv if present, else system python3
#   PORT=9000 ./run.sh       # custom port (default 8095)
set -euo pipefail
cd "$(dirname "$0")"

PY="../../venv/bin/python"
[ -x "$PY" ] || PY="python3"

# Ensure deps (Flask, PyYAML) are available; install if not.
"$PY" -c "import flask, yaml" 2>/dev/null || "$PY" -m pip install -r requirements.txt

PORT="${PORT:-8095}"
echo "SizeWise UI -> http://127.0.0.1:${PORT}/"
PORT="$PORT" "$PY" app.py
