"""
SizeWise — Flask backend.

Endpoints:
  GET  /                -> the input UI (static/index.html)
  GET  /api/fields      -> field-group metadata (drives the form)
  POST /api/size        -> run the sizing engine; returns result + both HTML reports
  POST /api/report      -> return one report as a downloadable HTML file

Pure calculator — no OCI/AIDP connectivity, no secrets. Runs fully offline.

Run:  ./run.sh            (http://127.0.0.1:8095/)
      PORT=9000 ./run.sh
"""

import io
import os
import re

from flask import Flask, jsonify, request, send_from_directory, send_file

from fields import FIELD_GROUPS
from sizing_engine import size, SizingInput, SizingConfig
import report_generator as rg

HERE = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=os.path.join(HERE, "static"))

_CFG = SizingConfig.load()
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(name):
    return _SAFE.sub("_", (name or "customer").strip()).strip("_") or "customer"


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/fields")
def api_fields():
    return jsonify({"groups": FIELD_GROUPS})


@app.route("/api/size", methods=["POST"])
def api_size():
    payload = request.get_json(silent=True) or {}
    try:
        inp = SizingInput.from_dict(payload)
        result = size(inp, _CFG)
        return jsonify({
            "ok": True,
            "result": result,
            "comprehensive_html": rg.generate_comprehensive(result),
            "summary_html": rg.generate_summary(result),
        })
    except Exception as exc:  # defensive: never 500 the UI on bad input
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 400


@app.route("/api/report", methods=["POST"])
def api_report():
    payload = request.get_json(silent=True) or {}
    kind = (payload.get("kind") or "comprehensive").lower()
    try:
        inp = SizingInput.from_dict(payload)
        result = size(inp, _CFG)
        html = (rg.generate_summary(result) if kind == "summary"
                else rg.generate_comprehensive(result))
        fname = f"AIDP-SizeWise-{_slug(inp.customer_name)}-{kind}.html"
        buf = io.BytesIO(html.encode("utf-8"))
        return send_file(buf, mimetype="text/html", as_attachment=True, download_name=fname)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8095"))
    print(f"SizeWise UI -> http://127.0.0.1:{port}/")
    app.run(host="127.0.0.1", port=port, debug=False)
