#!/usr/bin/env python3
"""
Local proxy that puts a browser UI in front of a deployed AIDP agent endpoint.

OCI request signing cannot run safely in the browser, so this Flask app loads
your local OCI credentials, signs each request, and streams the agent's SSE
response back to the browser unchanged.

Reuses AIDPChatClient from the parent library to construct the signer.

Usage:
    python3 -m pip install -e ..               # install the parent aidp_chat_client library
    python3 -m pip install -r requirements.txt
    # edit AGENT_URL below, then:
    python3 server.py
    # then open http://localhost:5001
"""

import json
import logging
import os
import oci
import requests
from flask import Flask, request, Response, send_from_directory
from flask_cors import CORS

from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
# Edit these for local dev. When deployed via deploy.py, AIDP_AGENT_URL and
# AIDP_AUTH are injected as container env vars and override these constants.
AGENT_URL = os.environ.get("AIDP_AGENT_URL") or \
    "https://gateway.aidp.<region>.oci.oraclecloud.com/agentendpoint/<AGENT_ID>/chat"
OCI_CONFIG_PATH = None       # None → ~/.oci/config (unused when AUTH == "resource_principal")
OCI_PROFILE = "DEFAULT"
AUTH = os.environ.get("AIDP_AUTH", "api_key")  # "api_key", "security_token", or "resource_principal"

if AUTH == "resource_principal":
    # Resource principal signers auto-refresh in theory but can serve stale
    # tokens once the underlying federation token expires (typically every
    # few hours). To stay robust on long-running deployments we build a
    # fresh signer per request via get_signer() below — the metadata service
    # call is cheap (a few ms) and avoids the "container running for hours
    # then 401s on every request" failure mode.
    _cached_signer = None
else:
    config = load_oci_config(OCI_CONFIG_PATH, OCI_PROFILE)
    _cached_signer = AIDPChatClient(AGENT_URL, config, auth=AUTH).signer


def get_signer():
    if AUTH == "resource_principal":
        return oci.auth.signers.get_resource_principals_signer()
    return _cached_signer

app = Flask(__name__, static_folder=".")
CORS(app)


@app.route("/")
def index():
    response = send_from_directory(".", "index.html")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json() or {}
    logger.info("session_id from browser: %r", data.get("session_id"))
    payload = {
        "isStreamEnabled": True,
        "input": [
            {
                "role": "user",
                "content": [{"type": "INPUT_TEXT", "text": data.get("message", "")}],
            }
        ],
        "session_id": data.get("session_id"),
    }

    # The gateway threads conversation memory by the x-session-id header,
    # not the session_id field in the body. Send both, matching what the
    # AIDP console does (verified via HAR capture).
    session_id = data.get("session_id") or ""

    def generate():
        try:
            logger.info("entering generator; calling AIDP")
            with requests.post(
                AGENT_URL,
                json=payload,
                auth=get_signer(),
                stream=True,
                headers={
                    "Content-Type": "application/json",
                    "x-session-id": session_id,
                },
                timeout=120,
            ) as response:
                logger.info(
                    "AIDP status=%s content-type=%s opc-request-id=%s",
                    response.status_code,
                    response.headers.get("content-type"),
                    response.headers.get("opc-request-id"),
                )
                response.raise_for_status()
                line_count = 0
                for line in response.iter_lines():
                    if line:
                        line_count += 1
                        if line_count <= 3:
                            logger.info(
                                "line %d: %s",
                                line_count,
                                line.decode("utf-8")[:200],
                            )
                        yield f"{line.decode('utf-8')}\n\n"
                logger.info("stream finished — %d non-empty lines yielded", line_count)
        except Exception as e:
            logger.exception("chat stream failed")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


if __name__ == "__main__":
    logger.info("AIDP Agent Chat proxy -> %s", AGENT_URL)
    logger.info("Open http://localhost:5001 in your browser")
    app.run(host="0.0.0.0", port=5001, debug=False)
