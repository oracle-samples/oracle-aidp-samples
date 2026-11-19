#!/usr/bin/env python3
"""aidp_sql.py — self-contained AIDP Spark-SQL / notebook-cell executor.

Runs a code cell (e.g. `spark.sql(...)`) on an AIDP Spark cluster over the notebook
WebSocket and prints the result as JSON. This is the plugin's ONLY bundled code: it
exists because interactive Spark-SQL uses a WebSocket (Jupyter v5.3) protocol that
plain `oci raw-request` (HTTP) can't speak. Everything else in the plugin is pure
`oci raw-request`.

Self-contained: depends only on public packages — `oci`, `requests`,
`websocket-client`, `cryptography`. It does NOT import `aidp_agent` or require the
`ai-data-engineer-agent` repo.

Auth (api_key OR session-token — full parity):
  - The base --profile may be an api_key profile OR an `oci session authenticate`
    session-token profile (auto-detected by a `security_token_file` key). Both run EVERY op.
  - REST (create/delete session): signed with the profile's signer — an api_key Signer, or
    a SecurityTokenSigner built from the session token + the session's key.
  - WebSocket: needs a security token. If the base profile is a SESSION profile, its token is
    reused directly (no mint). If the base profile is api_key, a short-lived UPST is minted
    (generate_user_security_token). --session-profile still overrides the WS token explicitly.
  - Session tokens expire (~1h): on expiry the helper prints an `oci session refresh` hint.

Usage:
  python aidp_sql.py \
    --region us-ashburn-1 --datalake <ocid> --workspace <ws-id> \
    --cluster <cluster-key> --notebook "Shared/_aidp_sql_scratch.ipynb" \
    --code "spark.sql('SELECT 1').show()" [--profile DEFAULT] [--session-profile AIDP_SESSION] \
    [--timeout 180]

Exit code 0 on success (status==ok), 1 on cell error or failure. Output is JSON:
  {"status": "ok|error", "execution_count": N, "outputs": [...], "spark_job_ids": [...], "error": {...}}
"""
from __future__ import annotations

import argparse
import email.utils
import json
import struct
import sys
import time
import uuid
from urllib.parse import quote

API_VERSION = "20240831"
WS_GENERIC_HEADERS = ["(request-target)", "host", "accept-language", "opc-request-id", "x-date"]
WS_SUBPROTOCOL = "v1.kernel.websocket.jupyter.org"


def _new_request_id() -> str:
    return f"csid{uuid.uuid4().hex[:32]}/{uuid.uuid4().hex}"


def _sha256_b64(data: bytes) -> str:
    import base64, hashlib
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


def _load_st_signer(cfg, generic_headers=None):
    """Build a SecurityTokenSigner from a session-token profile cfg (token + session key).

    Used for both REST (no generic_headers) and the WebSocket (custom WS_GENERIC_HEADERS).
    Mirrors the reference SDK's `_security_token_signer` / `_create_st_signer`.
    """
    import os
    import oci
    tok = open(os.path.expanduser(cfg["security_token_file"])).read().strip()
    pk = oci.signer.load_private_key_from_file(
        os.path.expanduser(cfg["key_file"]), cfg.get("pass_phrase"))
    if generic_headers:
        return oci.auth.signers.SecurityTokenSigner(tok, pk, generic_headers=generic_headers)
    return oci.auth.signers.SecurityTokenSigner(tok, pk)


def _warn_if_token_expired(cfg):
    """Print an `oci session refresh` hint to stderr if the session token is past its JWT exp."""
    import os, base64, json as _json
    try:
        tok = open(os.path.expanduser(cfg["security_token_file"])).read().strip()
        payload = tok.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = _json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        if exp and exp < time.time():
            sys.stderr.write("[aidp_sql] session token expired — run: "
                             "oci session refresh --profile <profile> (or oci session authenticate)\n")
    except Exception:
        pass


class AidpSql:
    def __init__(self, region, datalake, workspace, profile="DEFAULT",
                 session_profile="", config_file="~/.oci/config"):
        import oci
        self.region = region
        self.datalake = datalake
        self.workspace = workspace
        self.session_profile = session_profile
        self.config_file = config_file
        self.base_url = f"https://aidp.{region}.oci.oraclecloud.com"
        self._cfg = oci.config.from_file(config_file, profile)
        # Detect auth mode: a session profile (from `oci session authenticate`) carries a
        # `security_token_file`; an api_key profile carries tenancy/user/fingerprint.
        self._is_session = bool(self._cfg.get("security_token_file"))
        if self._is_session:
            _warn_if_token_expired(self._cfg)
            self._signer = _load_st_signer(self._cfg)
        else:
            self._signer = oci.signer.Signer(
                tenancy=self._cfg["tenancy"], user=self._cfg["user"],
                fingerprint=self._cfg["fingerprint"],
                private_key_file_location=self._cfg.get("key_file"),
                pass_phrase=self._cfg.get("pass_phrase"),
            )
        self._upst = None  # (token, private_key)

    @property
    def _lake_path(self):
        return f"/{API_VERSION}/dataLakes/{self.datalake}"

    @property
    def _ws_path(self):
        return f"{self._lake_path}/workspaces/{self.workspace}"

    # ---- REST (signed) -------------------------------------------------
    def _rest(self, method, path, body=None):
        import requests
        headers = {"opc-request-id": _new_request_id(), "x-date": email.utils.formatdate(usegmt=True)}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["content-type"] = "application/json"
            headers["x-content-sha256"] = _sha256_b64(data)
            headers["content-length"] = str(len(data))
        elif method.upper() in ("POST", "PUT", "DELETE"):
            headers["content-type"] = "application/json"
            headers["x-content-sha256"] = _sha256_b64(b"")
            headers["content-length"] = "0"
        req = requests.Request(method.upper(), f"{self.base_url}{path}", headers=headers, data=data)
        prepared = req.prepare()
        self._signer(prepared)
        resp = requests.Session().send(prepared, timeout=(10, 60))
        return resp

    def ensure_notebook(self, notebook_path):
        """Create an empty scratch .ipynb if it doesn't already exist (idempotent)."""
        enc = quote(notebook_path, safe="")
        g = self._rest("GET", f"{self._ws_path}/notebook/api/contents/{enc}?content=0")
        if g.status_code == 200:
            return
        nb = {"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
        self._rest("PUT", f"{self._ws_path}/notebook/api/contents/{enc}",
                   body={"type": "notebook", "format": "json", "content": nb, "path": notebook_path})

    def create_session(self, notebook_path, cluster_id):
        name = notebook_path.rsplit("/", 1)[-1]
        r = self._rest("POST", f"{self._ws_path}/notebook/api/sessions", body={
            "path": notebook_path, "type": "notebook", "name": name,
            "kernel": {"name": "notebook"}, "cluster_id": cluster_id,
        })
        if r.status_code >= 400:
            raise RuntimeError(f"create_session {r.status_code}: {r.text[:300]}")
        s = r.json()
        return s["id"], s["kernel"]["id"]

    def delete_session(self, session_id):
        try:
            self._rest("DELETE", f"{self._ws_path}/notebook/api/sessions/{session_id}")
        except Exception:
            pass

    # ---- WebSocket auth ------------------------------------------------
    def _ws_signer(self):
        import oci
        # Explicit session-token profile override (Tier 0)
        if self.session_profile:
            cfg = oci.config.from_file(self.config_file, self.session_profile)
            _warn_if_token_expired(cfg)
            return _load_st_signer(cfg, WS_GENERIC_HEADERS)
        # Base profile is itself a session-token profile → reuse its token directly (no UPST mint).
        if self._is_session:
            return _load_st_signer(self._cfg, WS_GENERIC_HEADERS)
        # api_key → mint a UPST
        if self._upst is None:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            pk = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            pub = pk.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo).decode()
            client = oci.identity_data_plane.DataplaneClient(self._cfg, signer=self._signer, timeout=(10, 60))
            resp = client.generate_user_security_token(
                oci.identity_data_plane.models.GenerateUserSecurityTokenDetails(
                    public_key=pub, session_expiration_in_minutes=60))
            self._upst = (resp.data.token, pk)
        tok, pk = self._upst
        return oci.auth.signers.SecurityTokenSigner(tok, pk, generic_headers=WS_GENERIC_HEADERS)

    def _ws_url(self, kernel_id, session_id):
        import requests
        host = f"c1.ws.aidp.{self.region}.oci.oraclecloud.com"
        path = f"{self._lake_path}/notebook/workspaces/{self.workspace}/api/kernels/{kernel_id}/channels"
        client_sid = f"{uuid.uuid4()}---{session_id}"
        date_val = email.utils.formatdate(usegmt=True)
        rid = _new_request_id()
        base_q = f"session_id={quote(client_sid, safe='')}"
        headers = {"host": host, "accept-language": "en", "opc-request-id": rid, "x-date": date_val}
        prepared = requests.Request("GET", f"https://{host}{path}?{base_q}", headers=headers).prepare()
        self._ws_signer()(prepared)
        authz = prepared.headers.get("Authorization", "")
        q = (f"{base_q}&token={quote(authz, safe='')}&accept-language=en"
             f"&opc-request-id={quote(rid, safe='')}&x-date={quote(date_val, safe='')}")
        return f"wss://{host}{path}?{q}"

    # ---- Jupyter v5.3 binary framing ----------------------------------
    @staticmethod
    def _build_msg(msg_type, content, channel="shell", metadata=None):
        header = {"msg_id": uuid.uuid4().hex, "msg_type": msg_type, "username": "user",
                  "session": uuid.uuid4().hex, "date": email.utils.formatdate(usegmt=True), "version": "5.3"}
        parts = [channel.encode(), json.dumps(header).encode(), json.dumps({}).encode(),
                 json.dumps(metadata or {}).encode(), json.dumps(content).encode(), b"buffer"]
        n = len(parts)
        table = 8 + (n + 1) * 8
        offs, run = [], table
        for p in parts:
            offs.append(run); run += len(p)
        offs.append(run)
        buf = struct.pack("<q", n) + b"".join(struct.pack("<q", o) for o in offs) + b"".join(parts)
        return buf

    @staticmethod
    def _parse_msg(data):
        if len(data) < 8:
            return {}
        n = struct.unpack("<q", data[:8])[0]
        if n < 1 or n > 50:
            return {}
        offs = [struct.unpack("<q", data[8 + i * 8:16 + i * 8])[0] for i in range(n + 1)]
        parts = [data[offs[i]:offs[i + 1]] for i in range(len(offs) - 1)]
        def _j(b):
            try:
                return json.loads(b)
            except Exception:
                return {}
        return {"channel": parts[0].decode("utf-8", "replace") if parts else "",
                "header": _j(parts[1]) if len(parts) > 1 else {},
                "metadata": _j(parts[3]) if len(parts) > 3 else {},
                "content": _j(parts[4]) if len(parts) > 4 else {}}

    # ---- Execute -------------------------------------------------------
    def execute(self, kernel_id, session_id, code, timeout=180.0):
        import websocket
        ws = websocket.create_connection(self._ws_url(kernel_id, session_id),
                                         header=[f"Sec-WebSocket-Protocol: {WS_SUBPROTOCOL}"],
                                         timeout=timeout)
        try:
            ws.send_binary(self._build_msg("execute_request", {
                "code": code, "silent": False, "store_history": True,
                "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
                metadata={"trusted": True, "type": "python", "recordTiming": True, "output_format_v2": "True"}))
            outputs, job_ids = [], []
            result = {"status": "pending", "execution_count": None, "outputs": outputs,
                      "spark_job_ids": job_ids, "error": None}
            seen = set()
            deadline = time.time() + timeout
            while time.time() < deadline:
                ws.settimeout(max(0.1, deadline - time.time()))
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    break
                msg = self._parse_msg(raw) if isinstance(raw, bytes) else (json.loads(raw) if raw else {})
                mt = msg.get("header", {}).get("msg_type", "")
                c = msg.get("content", {})
                if mt == "stream":
                    text = c.get("text", "")
                    h = hash(text)
                    if h in seen:
                        continue
                    seen.add(h)
                    if msg.get("metadata", {}).get("type") == "event":
                        try:
                            for it in json.loads(text):
                                jid = it.get("id")
                                if jid is not None and jid not in job_ids:
                                    job_ids.append(jid)
                        except Exception:
                            pass
                    else:
                        outputs.append({"output_type": "stream", "name": c.get("name", "stdout"), "text": text})
                elif mt == "execute_result":
                    outputs.append({"output_type": "execute_result", "data": c.get("data", {})})
                elif mt == "display_data":
                    outputs.append({"output_type": "display_data", "data": c.get("data", {})})
                elif mt == "error":
                    result["error"] = {"ename": c.get("ename", ""), "evalue": c.get("evalue", ""),
                                       "traceback": c.get("traceback", [])}
                    outputs.append({"output_type": "error", "ename": c.get("ename", ""), "evalue": c.get("evalue", "")})
                elif mt == "execute_reply":
                    result["status"] = c.get("status", "ok")
                    result["execution_count"] = c.get("execution_count")
                    break
            return result
        finally:
            ws.close()


def main(argv=None):
    p = argparse.ArgumentParser(description="Run a Spark-SQL / Python cell on an AIDP cluster (self-contained).")
    p.add_argument("--region", required=True)
    p.add_argument("--datalake", required=True, help="DataLake OCID")
    p.add_argument("--workspace", required=True, help="Workspace id (key)")
    p.add_argument("--cluster", required=True, help="ACTIVE cluster key")
    p.add_argument("--notebook", default="Shared/_aidp_sql_scratch.ipynb",
                   help="Scratch notebook path to attach the kernel to (must exist; create via REST first)")
    p.add_argument("--code", required=True, help="Python/Spark code to run (e.g. spark.sql('...').show())")
    p.add_argument("--profile", default="DEFAULT",
                   help="OCI profile for REST + WS auth — api_key OR session-token (auto-detected)")
    p.add_argument("--session-profile", default="",
                   help="Optional explicit oci-session profile for the WebSocket token (overrides --profile for WS)")
    p.add_argument("--config-file", default="~/.oci/config")
    p.add_argument("--timeout", type=float, default=180.0)
    p.add_argument("--keep-session", action="store_true", help="Do not delete the kernel session afterward")
    a = p.parse_args(argv)

    cli = AidpSql(a.region, a.datalake, a.workspace, profile=a.profile,
                  session_profile=a.session_profile, config_file=a.config_file)
    session_id = kernel_id = None
    try:
        cli.ensure_notebook(a.notebook)
        session_id, kernel_id = cli.create_session(a.notebook, a.cluster)
        time.sleep(2)  # let the kernel come up
        result = cli.execute(kernel_id, session_id, a.code, timeout=a.timeout)
    finally:
        if session_id and not a.keep_session:
            cli.delete_session(session_id)
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("status") == "ok" and not result.get("error") else 1


if __name__ == "__main__":
    sys.exit(main())
