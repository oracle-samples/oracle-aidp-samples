"""Regression tests for the aidp-rest skill's REST primitives (PR #4 review fix).

The skill sits outside the plugin's installed package (under
``skills/aidp-rest/``), so import via sys.path manipulation. Tests
here pin the public contract that downstream skills (fusion-tc26-run,
future fusion-medallion-doctor, etc.) depend on.

Reviewer catches captured here:
  - ``fetch_output`` must RAISE on non-200, not silently return ``""``.
    Otherwise an AIDP job that ran SUCCESS but whose output we can't fetch
    becomes a silent evidence-capture gap (dispatch.py exits 0 with empty
    marker). Tests pin both the 404 case and the legitimate empty-data case.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# The skill lives outside the standard package layout; import via sys.path.
_SKILL_DIR = (Path(__file__).resolve().parents[2]
              / "skills" / "aidp-rest")
sys.path.insert(0, str(_SKILL_DIR))
from client import AidpRestClient, AidpRestError  # noqa: E402


# Canonical patch target — the skill's ``client`` module is a re-export shim
# that imports from ``oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client``
# (P1.5ε §Step 2). The ``oci`` module is imported by the canonical module,
# not by the shim — patch it where it's looked up.
_REST_CLIENT_MOD = "oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client"


@pytest.fixture
def client():
    """Build a client without touching ~/.oci/config — patch the signer."""
    with patch(f"{_REST_CLIENT_MOD}.oci.config.from_file", return_value={
        "tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/tmp/k",
    }), patch(f"{_REST_CLIENT_MOD}.oci.signer.Signer", return_value=MagicMock()):
        return AidpRestClient(
            region="us-ashburn-1",
            aidp_id="ocid1.datalake.oc1.iad.aaaa",
            workspace_key="00000000-0000-0000-0000-000000000000",
            log=lambda s, **kw: None,
        )


# ---------------------------------------------------------------------------
# fetch_output — silent-failure regression (PR #4 review catch #2)
# ---------------------------------------------------------------------------


def test_fetch_output_raises_on_404(client):
    """A 404 from fetchOutput must raise AidpRestError, NOT silently return ""."""
    mock_resp = MagicMock(status_code=404, text="Unknown resource X")
    with patch.object(client, "_request", return_value=mock_resp):
        with pytest.raises(AidpRestError, match="HTTP 404"):
            client.fetch_output("task-run-key")


def test_fetch_output_raises_on_500(client):
    """A 500 from fetchOutput must raise too — masks evidence-capture gaps."""
    mock_resp = MagicMock(status_code=500, text="Internal error")
    with patch.object(client, "_request", return_value=mock_resp):
        with pytest.raises(AidpRestError, match="HTTP 500"):
            client.fetch_output("task-run-key")


def test_fetch_output_returns_empty_string_on_200_with_empty_data(client):
    """Empty ``data`` array at 200 is a legitimate "notebook printed nothing"
    case — return ``""`` so the caller can assert presence-of-marker downstream.
    This is the ONLY shape that may return ``""`` cleanly.
    """
    mock_resp = MagicMock(status_code=200)
    mock_resp.json.return_value = {"data": []}
    with patch.object(client, "_request", return_value=mock_resp):
        assert client.fetch_output("task-run-key") == ""


def test_fetch_output_returns_notebook_json_on_success(client):
    """Happy-path: 200 with data[0].value carries the executed notebook JSON."""
    mock_resp = MagicMock(status_code=200)
    mock_resp.json.return_value = {
        "data": [{"type": "NOTEBOOK", "value": '{"cells":[]}'}],
    }
    with patch.object(client, "_request", return_value=mock_resp):
        assert client.fetch_output("task-run-key") == '{"cells":[]}'


# ---------------------------------------------------------------------------
# parse_marker — sanity (used by dispatch.py to detect success-without-marker)
# ---------------------------------------------------------------------------


def test_parse_marker_returns_none_when_marker_absent():
    """The dispatch.py "success but no marker" exit-2 logic depends on this."""
    nb = {"cells": [{"outputs": [{"text": "no marker here"}]}]}
    assert AidpRestClient.parse_marker(nb, begin="X_BEGIN", end="X_END") is None


def test_parse_marker_extracts_json_payload():
    nb = {
        "cells": [{
            "outputs": [{
                "text": 'before X_BEGIN {"ok": true} X_END after',
            }],
        }],
    }
    assert AidpRestClient.parse_marker(nb, begin="X_BEGIN", end="X_END") == {"ok": True}


# parse_marker decode_base64 — the run/drift markers are base64-wrapped so
# they survive AIDP's display_data text/plain capture (which strips JSON-escape
# backslashes and corrupts raw payloads carrying quotes/reprs).
# ---------------------------------------------------------------------------


def _b64_marker(payload, begin="X_BEGIN", end="X_END"):
    import base64
    import json
    token = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    return f"before {begin} {token} {end} after"


def test_parse_marker_decodes_base64_payload():
    """decode_base64=True round-trips a base64-wrapped marker — including a
    payload with quotes/brackets that raw text/plain would corrupt."""
    payload = {
        "run_id": "abc-123",
        "failed": 1,
        "steps": [
            {"dataset_id": "ap_invoices", "status": "source_schema_missing",
             "error_message": "AIDPF-4071: missing ['X'] — type \"decimal(38,30)\""},
        ],
    }
    nb = {"cells": [{"outputs": [{"text": _b64_marker(payload)}]}]}
    out = AidpRestClient.parse_marker(
        nb, begin="X_BEGIN", end="X_END", decode_base64=True
    )
    assert out == payload


def test_parse_marker_base64_mode_tolerates_raw_json():
    """decode_base64=True must still parse a raw-JSON marker (pre-fix
    notebooks / fixtures) — b64decode(validate=True) raises on raw JSON's
    non-alphabet chars, so the consumer falls back to raw."""
    nb = {
        "cells": [{
            "outputs": [{"text": 'X_BEGIN {"ok": true} X_END'}],
        }],
    }
    out = AidpRestClient.parse_marker(
        nb, begin="X_BEGIN", end="X_END", decode_base64=True
    )
    assert out == {"ok": True}
