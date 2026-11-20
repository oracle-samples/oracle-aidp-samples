#!/usr/bin/env python3
"""
AIDP Notebook Executor
======================
Executes notebook cells on an AIDP Spark cluster via WebSocket.
Connects to the Jupyter kernel behind AIDP using OCI-signed requests.

Based on the SparkExecutionService.ts from oci-aidp-ai-tools.

Usage:
    # Execute a single Python expression
    python3 aidp_executor.py --code "print('hello from AIDP')"

    # Execute all cells in a notebook
    python3 aidp_executor.py --notebook path/to/notebook.ipynb

    # Interactive REPL
    python3 aidp_executor.py --repl
"""

import asyncio
import json
import os
import struct
import sys
import time
import uuid
import argparse
from datetime import datetime
from typing import Optional, Dict, Any, List

import oci
import requests
import websocket  # websocket-client (sync)

# ─── Configuration ───────────────────────────────────────────────────

# No hardcoded customer config — this is generic code. The lake/workspace/
# cluster identifiers have NO default and must be provided by the caller (passed
# explicitly to AIDPSession, or set on these module globals at runtime, e.g. by
# job_migrate_from_workflow). Only the OCI profile defaults — to "DEFAULT", the
# standard ~/.oci/config profile name.
DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_CLUSTER_ID = None
DEFAULT_OCI_PROFILE = "DEFAULT"

# MUST stay in sync with dbx_export/auth.py::REGION_MAP — both resolve lake
# OCIDs to the AIDP API endpoint; a drift sends the exporter and the
# migrator to different control planes for the same lake. A test asserts
# they are identical.
REGION_MAP = {
    "iad": "us-ashburn-1", "phx": "us-phoenix-1", "fra": "eu-frankfurt-1",
    "lhr": "uk-london-1", "bom": "ap-mumbai-1", "hyd": "ap-hyderabad-1",
    "sin": "ap-singapore-1", "nrt": "ap-tokyo-1", "syd": "ap-sydney-1",
    "gru": "sa-saopaulo-1", "yyz": "ca-toronto-1", "icn": "ap-seoul-1",
}


# ─── AIDP Output Unwrapper ───────────────────────────────────────────

def _unwrap_aidp_text(text: str) -> str:
    """Unwrap AIDP's TEXT_PLAIN JSON envelope from stream output.

    AIDP wraps cell stdout inside the Jupyter stream message text field as:
        [{"type":"TEXT_PLAIN","value":"actual output\n",...}, ...]
    Extract and concatenate the value fields; return original text if not wrapped.
    """
    stripped = text.strip()
    if not (stripped.startswith('[') and '"TEXT_PLAIN"' in stripped):
        return text
    try:
        items = json.loads(stripped)
        if isinstance(items, list):
            parts = [item["value"] for item in items
                     if isinstance(item, dict) and item.get("type") == "TEXT_PLAIN" and "value" in item]
            if parts:
                return "".join(parts)
    except (json.JSONDecodeError, KeyError):
        pass
    return text


# ─── OCI Signing ─────────────────────────────────────────────────────
# Single source: scripts/oci_auth.py (moved verbatim 2026-10-12 so the
# standalone utilities can share it without importing this module's
# websocket dependency). Re-exported here so every existing caller and
# test binding (`ae.get_oci_signer`, `ae._apply_cred_env_from_config`)
# keeps working unchanged.
from oci_auth import get_oci_signer, _apply_cred_env_from_config  # noqa: F401


def get_region_from_ocid(lake_ocid: str) -> str:
    """Extract region from AIDP Lake OCID.

    OCID format: ocid1.<resource>.<realm>.<region>.<unique-id>
    parts[3] is already the full region name (e.g. 'ap-mumbai-1'), not a 3-letter code.

    Raises ValueError for malformed OCIDs — mirrors
    dbx_export.auth.region_from_ocid. The region selects the API endpoint,
    so a silent default region sends requests to the wrong region's
    control plane. Fail loud at the source.
    """
    parts = lake_ocid.split(".")
    if len(parts) > 3 and parts[3]:
        region = parts[3]
        # If it looks like a 3-letter code (legacy), map it; otherwise use as-is
        return REGION_MAP.get(region, region)
    raise ValueError(
        f"malformed lake OCID {lake_ocid!r}: expected "
        f"'ocid1.<type>.<realm>.<region>.<id>' (cannot derive a region)")


# ─── Cluster liveness detection (for stuck-cell recovery) ────────────
#
# When a cell produces no output for a long time, the WS-level _ws_alive
# flag is not enough — the WebSocket can stay TCP-alive while the cluster's
# kernel is gone. Instead, periodically query AIDP's cluster status REST
# API: it's the authoritative source on whether the cluster is alive.
#
# Only states in TERMINAL_DEAD_STATES are treated as "kill the cell".
# Transient states (library installs, restarts) and unknown states are
# treated as "keep waiting" — strongly biased towards NOT killing a
# legitimate long-running cell.

# Cluster states where we conclude the cell will never produce output.
# Conservative — only includes hard-terminal states.
TERMINAL_DEAD_STATES = frozenset({"STOPPED", "FAILED", "TERMINATED"})

# Transient states where the cluster is briefly unavailable but will recover.
# Cells running through these states should NOT be killed.
TRANSIENT_STATES = frozenset({
    "STARTING", "RESTARTING", "INSTALLING_LIBRARIES",
    "RESTARTING_LIBRARIES", "STOPPING", "PROVISIONING",
})


class ClusterDeadError(RuntimeError):
    """Raised when AIDP cluster status check shows the cluster is in a
    terminal-dead state during a long no-output wait. Caller should mark
    the cell failed and proceed to the next task — do NOT retry on the
    same dead cluster."""
    pass


def _check_cluster_status(cluster_id: str, lake_ocid: str, workspace_id: str,
                           profile: str = DEFAULT_OCI_PROFILE,
                           timeout: float = 30.0) -> Optional[str]:
    """Query AIDP cluster status REST API. Returns the lifecycleState
    string (e.g. "ACTIVE", "STOPPED") or None on transport error.

    Returning None on error signals "couldn't tell" — caller should NOT
    treat None as dead; it should keep waiting.

    Pure REST GET on the AIDP control plane. Independent of WS / kernel —
    works even when the kernel is hung. Cheap (~50-200ms).
    """
    try:
        _config, signer = get_oci_signer(profile)
        region = get_region_from_ocid(lake_ocid)
        url = (
            f"https://aidp.{region}.oci.oraclecloud.com/20240831/dataLakes/"
            f"{lake_ocid}/workspaces/{workspace_id}/clusters/{cluster_id}"
        )
        resp = requests.get(url, auth=signer, timeout=timeout,
                            headers={"Accept": "application/json"})
        if resp.status_code != 200:
            # Transient API error — be conservative, don't conclude dead
            return None
        data = resp.json()
        # AIDP returns the cluster object with `lifecycleState` field
        return data.get("lifecycleState")
    except Exception:
        # Network blip, JSON parse error, etc. — be conservative
        return None


def _is_terminal_dead(status: Optional[str]) -> bool:
    """True only for hard-terminal states. Unknown / transient / None → False
    (keep waiting). Strongly biased to NOT kill a long-running cell."""
    return status is not None and status in TERMINAL_DEAD_STATES


# ─── Jupyter Binary Protocol ─────────────────────────────────────────

def encode_jupyter_message(channel: str, header: dict, parent_header: dict,
                           metadata: dict, content: dict) -> bytes:
    """Encode a Jupyter message into AIDP binary wire format.

    Format: 8-byte offset count (LE), then N 8-byte offsets (LE), then N parts.
    AIDP expects 6 parts: channel, header, parent_header, metadata, content, extra.
    """
    parts = [
        channel.encode("utf-8"),
        json.dumps(header).encode("utf-8"),
        json.dumps(parent_header).encode("utf-8"),
        json.dumps(metadata).encode("utf-8"),
        json.dumps(content).encode("utf-8"),
        b"",  # extra empty part expected by AIDP
    ]

    offset_count = len(parts)
    offset_table_size = offset_count * 8
    header_size = 8 + offset_table_size  # 8 for offset_count

    # Calculate offsets
    offsets = []
    current = header_size
    for part in parts:
        offsets.append(current)
        current += len(part)

    # Build buffer
    buf = bytearray()
    buf += struct.pack("<Q", offset_count)
    for off in offsets:
        buf += struct.pack("<Q", off)
    for part in parts:
        buf += part

    return bytes(buf)


def decode_jupyter_message(data: bytes) -> Optional[dict]:
    """Decode a binary Jupyter message from the AIDP WebSocket.

    Handles three formats:
      1. Standard Jupyter binary wire format (offset-table + parts)
      2. Plain JSON object  {"header": ..., "content": ...}
      3. AIDP TEXT_PLAIN wrapper  [{"type":"TEXT_PLAIN","value":"..."}]
         — treated as a synthetic stream message so output is not lost
    """
    if not data or len(data) < 2:
        return None

    # Format 2: plain JSON object
    if data[0:1] == b"{":
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Format 3: AIDP TEXT_PLAIN wrapper array
    if data[0:1] == b"[":
        try:
            items = json.loads(data.decode("utf-8"))
            if isinstance(items, list):
                text_parts = []
                for item in items:
                    if isinstance(item, dict) and item.get("type") == "TEXT_PLAIN":
                        text_parts.append(item.get("value", ""))
                if text_parts:
                    # Synthesise a stream message — parent_header is empty here so
                    # it won't match a pending msg_id and won't trigger execute_reply,
                    # but the text is preserved in debug output.
                    print(f"[WS] AIDP TEXT_PLAIN wrapper: {''.join(text_parts)[:200]!r}")
                    return None  # not routable; caller will log and discard
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # Format 1: Jupyter binary offset-table wire format
    if len(data) < 8:
        print(f"[WS] undecoded message ({len(data)} bytes): {data[:64]!r}")
        return None
    try:
        offset_count = struct.unpack("<Q", data[:8])[0]

        if offset_count == 0 or offset_count > 100:
            print(f"[WS] undecoded message — bad offset_count={offset_count} ({len(data)} bytes): {data[:64]!r}")
            return None

        if len(data) < 8 + offset_count * 8:
            return None

        offsets = []
        for i in range(offset_count):
            off = struct.unpack("<Q", data[8 + i * 8: 16 + i * 8])[0]
            offsets.append(off)
        offsets.append(len(data))

        parts = []
        for i in range(min(5, len(offsets) - 1)):
            part = data[offsets[i]:offsets[i + 1]].decode("utf-8")
            parts.append(part)

        if len(parts) < 5:
            return None

        return {
            "channel": parts[0],
            "header": json.loads(parts[1]),
            "parent_header": json.loads(parts[2]),
            "metadata": json.loads(parts[3]),
            "content": json.loads(parts[4]),
        }
    except Exception as e:
        print(f"[WS] undecoded message ({len(data)} bytes, err={e}): {data[:64]!r}")
        return None


# ─── Jupyter Message Builders ─────────────────────────────────────────

def make_execute_request(msg_id: str, session_id: str, code: str) -> bytes:
    header = {
        "msg_id": msg_id,
        "username": "aidp_executor",
        "session": session_id,
        "date": datetime.utcnow().isoformat() + "Z",
        "msg_type": "execute_request",
        "version": "5.3",
    }
    content = {
        "code": code,
        "silent": False,
        "store_history": True,
        "user_expressions": {},
        "allow_stdin": False,
        "stop_on_error": True,
    }
    return encode_jupyter_message("shell", header, {}, {}, content)


def make_kernel_info_request(msg_id: str, session_id: str) -> bytes:
    header = {
        "msg_id": msg_id,
        "username": "aidp_executor",
        "session": session_id,
        "date": datetime.utcnow().isoformat() + "Z",
        "msg_type": "kernel_info_request",
        "version": "5.3",
    }
    return encode_jupyter_message("shell", header, {}, {}, {})


# ─── AIDP Session ─────────────────────────────────────────────────────

class AIDPSession:
    """Manages a WebSocket connection to an AIDP PySpark kernel."""

    def __init__(self, lake_ocid: str = None,
                 workspace_id: str = None,
                 cluster_id: str = None,
                 oci_profile: str = None,
                 session_name: str = "aidp_executor_session",
                 session_dir: str = "Workspace/aidp_tool_session"):
        # Resolve from the live module globals at CALL time (not import time) so
        # a runtime override of DEFAULT_* takes effect. lake/workspace/cluster
        # have no hardcoded default and must be provided; only profile defaults.
        self.lake_ocid = lake_ocid if lake_ocid is not None else DEFAULT_LAKE_OCID
        self.workspace_id = workspace_id if workspace_id is not None else DEFAULT_WORKSPACE_ID
        self.cluster_id = cluster_id if cluster_id is not None else DEFAULT_CLUSTER_ID
        self.oci_profile = oci_profile if oci_profile is not None else DEFAULT_OCI_PROFILE
        self._session_name = session_name
        # Session scratch notebooks live under this folder (kept out of the Workspace
        # root). Nested path is auto-created by the sessions API. Leading/trailing
        # slashes stripped — a leading slash yields a malformed //Workspace path.
        self._session_dir = (session_dir or "Workspace").strip("/")

        _missing = [n for n, v in (("lake_ocid", self.lake_ocid),
                                   ("workspace_id", self.workspace_id),
                                   ("cluster_id", self.cluster_id)) if not v]
        if _missing:
            raise ValueError(
                f"AIDPSession requires {', '.join(_missing)} — pass explicitly or set "
                f"aidp_executor.DEFAULT_* before constructing. This is generic code with "
                f"no hardcoded environment config.")

        self.region = get_region_from_ocid(self.lake_ocid)
        self.aidp_endpoint = f"https://aidp.{self.region}.oci.oraclecloud.com"
        self.ws_host = f"c1.ws.aidp.{self.region}.oci.oraclecloud.com"

        self.session_id: Optional[str] = None
        self.kernel_id: Optional[str] = None
        self.ws: Optional[websocket.WebSocket] = None
        self.signer = None
        self._oci_config = None
        self._pending: Dict[str, dict] = {}
        self._outputs: Dict[str, List[dict]] = {}
        self._listener_thread = None
        self._running = False
        self._ws_alive = False
        self._last_recv_time = 0
        self._listener_generation = 0
        # opc-request-id of the current WS handshake (OCI stamps it on the 101
        # upgrade response). Captured on every connect/reconnect and logged with
        # the [WS-OPC] tag so engineering can correlate a WS drop to a server-side
        # request when debugging. Empty until the first connect.
        self.ws_opc_request_id = ""
        # Fix 5c v2 (2026-07-25): remember when AIDP's REST kernel-interrupt
        # endpoint has already returned 404 for this session — subsequent
        # timeouts will skip the interrupt HTTP call entirely, reducing log
        # noise. AIDP doesn't currently expose the Jupyter-standard
        # /kernels/{id}/interrupt endpoint (verified 2026-07-25 on Mumbai lake).
        # Once we implement WS control-channel interrupt (Fix 7b follow-up),
        # this flag becomes obsolete.
        self._interrupt_unsupported = False

    async def connect(self):
        """Create a notebook session and connect the WebSocket."""
        print(f"[AIDP] Connecting to {self.aidp_endpoint} ...")
        print(f"[AIDP] Lake: {self.lake_ocid}")
        print(f"[AIDP] Workspace: {self.workspace_id}")
        print(f"[AIDP] Cluster: {self.cluster_id}")

        # Init OCI signer
        self._oci_config, self.signer = get_oci_signer(self.oci_profile)

        # Step 1: Find or create a notebook session via REST
        notebook_name = f"{self._session_name}.ipynb"
        session_path = f"{self._session_dir}/{notebook_name}"
        session_url = (
            f"{self.aidp_endpoint}/20240831/dataLakes/{self.lake_ocid}"
            f"/workspaces/{self.workspace_id}/notebook/api/sessions"
        )

        # List existing sessions and reuse one matching our notebook path
        list_url = f"{session_url}?path={session_path}"
        print(f"[AIDP] GET {list_url}")
        try:
            list_resp = requests.get(list_url, auth=self.signer,
                                     headers={"Accept": "application/json"}, timeout=60)
            if list_resp.status_code >= 500:
                raise RuntimeError(
                    f"[AIDP] Cluster appears to be down (HTTP {list_resp.status_code}: "
                    f"{list_resp.text[:500]}). Please start the cluster and retry."
                )
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(
                f"[AIDP] Could not reach cluster while listing sessions: {exc}. "
                "Please verify the cluster is running and retry."
            ) from exc

        session_data = None
        if list_resp.ok:
            sessions = list_resp.json()
            if isinstance(sessions, list):
                for s in sessions:
                    if s.get("path") == session_path or s.get("name") == notebook_name:
                        session_data = s
                        print(f"[AIDP] Reusing existing session {s['id']} for {notebook_name}")
                        self._log_ws_opc("session_reuse", self._opc_from_headers(list_resp.headers),
                                         session=s.get("id"))
                        break
        else:
            print(f"[AIDP] List sessions failed {list_resp.status_code}: {list_resp.text[:500]} — will create new session")

        if session_data is None:
            body = json.dumps({
                "type": "notebook",
                "name": notebook_name,
                "kernel": {"name": "notebook"},
                "path": session_path,
                "cluster_id": self.cluster_id,
            })
            print(f"[AIDP] POST {session_url}", flush=True)
            print(f"[AIDP] Request body: {body}", flush=True)

            # Retry session creation — after a cluster start, the execution context
            # (dataflowdp) may need 30-90s to initialize even though lifecycleState
            # is ACTIVE.  We retry up to 6 times with 15s backoff (~90s total).
            max_create_retries = 6
            for _create_attempt in range(max_create_retries):
                resp = requests.post(session_url, data=body, auth=self.signer,
                                     headers={"Content-Type": "application/json",
                                              "Accept": "application/json"}, timeout=60)
                if resp.ok:
                    break
                err_text = resp.text[:1000]
                print(f"[AIDP] Session create failed {resp.status_code}: {err_text}", flush=True)
                # Retry on 400/404 when execution context isn't ready yet
                if (resp.status_code in (400, 404)
                        and _create_attempt < max_create_retries - 1
                        and ("execution_context" in err_text.lower()
                             or "NotAuthorizedOrNotFound" in err_text
                             or "create_execution_context" in err_text)):
                    wait = 15
                    print(f"[AIDP] Cluster execution context not ready, "
                          f"retrying in {wait}s ({_create_attempt + 1}/{max_create_retries})...",
                          flush=True)
                    await asyncio.sleep(wait)
                else:
                    resp.raise_for_status()
            else:
                # All retries exhausted
                resp.raise_for_status()
            session_data = resp.json()
            self._log_ws_opc("session_create", self._opc_from_headers(resp.headers),
                             session=session_data.get("id"), http=resp.status_code)

        self.session_id = session_data["id"]

        # Kernel may be null if the cluster is still warming up after a restart.
        # Poll the session until the kernel is ready (up to 3 minutes).
        if session_data.get("kernel") is None:
            get_url = session_url + f"/{self.session_id}"
            print(f"[AIDP] Kernel not ready yet, polling session {self.session_id}...")
            for _attempt in range(36):  # 36 × 5s = 3 min
                await asyncio.sleep(5)
                _r = requests.get(get_url, auth=self.signer,
                                  headers={"Accept": "application/json"}, timeout=60)
                if _r.ok:
                    session_data = _r.json()
                    if session_data.get("kernel") is not None:
                        print(f"[AIDP] Kernel ready after {(_attempt+1)*5}s.")
                        break
                print(f"[AIDP] Still waiting for kernel... ({(_attempt+1)*5}s)")
            else:
                raise RuntimeError("[AIDP] Kernel did not become ready within 3 minutes.")

        self.kernel_id = session_data["kernel"]["id"]
        print(f"[AIDP] Session: {self.session_id}")
        print(f"[AIDP] Kernel:  {self.kernel_id}")

        # Step 2: Connect WebSocket
        request_id = str(uuid.uuid4())
        session_id_param = f"{request_id}---{self.session_id}"

        ws_url = (
            f"wss://{self.ws_host}/20240831/dataLakes/{self.lake_ocid}"
            f"/notebook/workspaces/{self.workspace_id}"
            f"/api/kernels/{self.kernel_id}/channels"
            f"?session_id={session_id_param}"
        )

        # Sign the WebSocket upgrade request
        sign_url = ws_url.replace("wss://", "https://")
        ws_headers = self._sign_ws_headers(sign_url, self.ws_host)

        print(f"[AIDP] Connecting WebSocket to {self.ws_host} ...")

        # Use websocket-client (sync) - proven to work with AIDP
        self.ws = websocket.WebSocket()
        header_list = [f"{k}: {v}" for k, v in ws_headers.items()]
        self.ws.connect(
            ws_url,
            header=header_list,
            subprotocols=["v1.kernel.websocket.jupyter.org"],
        )

        print("[AIDP] WebSocket connected!")

        # Capture the opc-request-id from the 101 upgrade response for WS-drop debugging.
        try:
            self.ws_opc_request_id = self._opc_from_headers(self.ws.getheaders())
        except Exception:
            self.ws_opc_request_id = ""
        self._log_ws_opc("connect", self.ws_opc_request_id,
                         ws_host=self.ws_host, session=self.session_id, kernel=self.kernel_id)

        # Start message listener thread
        import threading
        self._running = True
        self._ws_alive = True
        self._last_recv_time = time.time()
        self._listener_generation += 1
        self._listener_thread = threading.Thread(
            target=self._listen_sync, args=(self._listener_generation,), daemon=True)
        self._listener_thread.start()

        # Wait for kernel to be ready
        await asyncio.sleep(2)

        # Send kernel_info_request
        ki_msg = make_kernel_info_request(str(uuid.uuid4()), self.session_id)
        self.ws.send_binary(ki_msg)
        await asyncio.sleep(1)

        print("[AIDP] Kernel ready.")

    def _sign_ws_headers(self, url: str, ws_host: str) -> dict:
        """Sign a GET request for WebSocket upgrade using OCI signer.

        Returns only date + authorization headers. The host header is NOT
        included because the websocket library adds its own - having both
        causes 'Duplicate Host Header' 400 errors.
        """
        import requests as req
        r = req.Request("GET", url)
        prepared = r.prepare()
        prepared.headers = {"host": ws_host}
        self.signer(prepared)

        signed = dict(prepared.headers)
        # Remove host (websocket lib adds its own) and content-type (no body)
        for h in list(signed.keys()):
            if h.lower() in ("host", "content-type", "content-length"):
                del signed[h]
        return signed

    @staticmethod
    def _opc_from_headers(headers) -> str:
        """Case-insensitive lookup of opc-request-id from a header mapping.

        Accepts a dict (requests.Response.headers, websocket getheaders()) or a
        list of (key, value) tuples. Returns "" if absent.
        """
        if not headers:
            return ""
        try:
            items = list(headers.items())
        except AttributeError:
            items = list(headers)
        for k, v in items:
            if str(k).lower() == "opc-request-id":
                return str(v)
        return ""

    def _log_ws_opc(self, event: str, opc_id, **kv):
        """Emit one greppable line per WS/session lifecycle event.

        Tagged [WS-OPC] so engineering can `grep WS-OPC <run>.log` to get the full
        connection history (incl. the opc-request-id in effect at a drop) to share
        for debugging AIDP WebSocket connection drops.
        """
        parts = [f"event={event}", f"opc-request-id={opc_id or 'MISSING'}"]
        parts += [f"{k}={v}" for k, v in kv.items()]
        print("[WS-OPC] " + " ".join(parts), flush=True)

    def _listen_sync(self, generation: int):
        """Background thread to listen for WebSocket messages.

        generation: snapshot of _listener_generation at thread-start time.
        Only this thread's generation may set _ws_alive=False — prevents a
        stale thread from corrupting a newer connection's state after reconnect.

        unanswered_pings: local counter incremented each time a 30s timeout fires
        and a ping is sent. Reset to 0 on any received pong. If 3 consecutive pings
        go unanswered, the connection is silently dead (TCP black hole) and we mark it.
        """

        def mark_dead():
            if self._listener_generation == generation:
                if self._ws_alive:  # log the live→dead transition once
                    self._log_ws_opc("ws_dead", self.ws_opc_request_id,
                                     ws_host=self.ws_host, session=self.session_id,
                                     unanswered_pings=unanswered_pings)
                self._ws_alive = False

        unanswered_pings = 0
        ws = self.ws  # this listener must never read a later generation's socket

        while self._running and ws and ws.connected:
            try:
                ws.settimeout(30)  # Don't block forever
                opcode, data = ws.recv_data(control_frame=True)
                self._last_recv_time = time.time()

                if opcode == websocket.ABNF.OPCODE_CLOSE:
                    print("[AIDP] WebSocket received CLOSE frame")
                    mark_dead()
                    break
                if opcode == websocket.ABNF.OPCODE_PING:
                    ws.pong(data)
                    continue
                if opcode == websocket.ABNF.OPCODE_PONG:
                    unanswered_pings = 0  # pong received — connection confirmed alive
                    continue

                if isinstance(data, str):
                    data = data.encode("utf-8")

                msg = decode_jupyter_message(data)
                if msg:
                    self._handle_message(msg)

            except websocket.WebSocketTimeoutException:
                # No data received within 30s — send a ping to detect silent drops.
                # Track consecutive unanswered pings: if the remote end has silently
                # dropped the connection (TCP black hole), pongs stop arriving.
                unanswered_pings += 1
                if unanswered_pings >= 3:
                    print(f"[AIDP] {unanswered_pings} consecutive pings unanswered — "
                          f"connection silently dead")
                    mark_dead()
                    break
                try:
                    if ws.connected:
                        ws.ping()
                except:
                    mark_dead()
                    break
                continue
            except websocket.WebSocketConnectionClosedException:
                print("[AIDP] WebSocket connection closed")
                mark_dead()
                break
            except Exception as e:
                if self._running:
                    print(f"[AIDP] Listener error: {e}")
                mark_dead()
                break

    def _handle_message(self, msg: dict):
        """Handle an incoming Jupyter message."""
        msg_type = msg.get("header", {}).get("msg_type", "")
        parent_id = msg.get("parent_header", {}).get("msg_id", "")
        # The waiter can remove completed tracking while this thread handles a
        # late message. Retain references instead of checking then indexing maps.
        outputs = self._outputs.get(parent_id)
        pending = self._pending.get(parent_id)

        if msg_type == "stream":
            if outputs is not None:
                text = msg["content"].get("text", "")
                # AIDP wraps stream output as [{"type":"TEXT_PLAIN","value":"..."}] JSON
                # inside the standard Jupyter stream text field. Unwrap to plain text.
                text = _unwrap_aidp_text(text)
                outputs.append({
                    "type": "stream",
                    "name": msg["content"].get("name", "stdout"),
                    "text": text,
                })

        elif msg_type == "execute_result":
            if outputs is not None:
                outputs.append({
                    "type": "execute_result",
                    "data": msg["content"].get("data", {}),
                })

        elif msg_type == "display_data":
            if outputs is not None:
                outputs.append({
                    "type": "display_data",
                    "data": msg["content"].get("data", {}),
                })

        elif msg_type == "error":
            if outputs is not None:
                outputs.append({
                    "type": "error",
                    "ename": msg["content"].get("ename", ""),
                    "evalue": msg["content"].get("evalue", ""),
                    "traceback": msg["content"].get("traceback", []),
                })

        elif msg_type == "status":
            execution_state = msg["content"].get("execution_state", "")
            if execution_state == "busy" and pending is not None:
                pending["got_busy"] = True
                print(f"[WS] kernel busy — msg_id={parent_id[:8]}")
            elif execution_state == "idle" and pending is not None:
                print(f"[WS] kernel idle — msg_id={parent_id[:8]}")

        elif msg_type == "execute_reply":
            if pending is not None and not pending.get("reply_received", False):
                # REF-295 (plumbing): receipt acknowledges the request even while
                # output validation is running. The waiter owns cleanup; removing
                # tracking here made an acknowledged request look like KernelHung.
                pending["reply_received"] = True
                status = msg["content"].get("status", "error")
                # Freeze the reply's outputs as before, retaining the tracked
                # buffer until consumption so timeout checks cannot lose it.
                outputs = list(outputs) if outputs is not None else []
                wire_status = status
                # Category 1: AIDP may report status=ok even when its own
                # pre-command SparkContext prologue failed before user code.
                # Preserve that evidenced failure for all consumers/fix loops.
                prelude_error = _kernel_prelude_error(outputs)
                if prelude_error:
                    outputs.append(prelude_error)
                if any(out.get("type") == "error" for out in outputs):
                    status = "error"
                wire_detail = f" wire_status={wire_status}" if wire_status != status else ""
                print(f"[WS] execute_reply msg_id={parent_id[:8]} status={status}{wire_detail}")
                pending["result"]["result"] = {
                    "status": status,
                    "outputs": outputs,
                }
                pending["event"].set()

    async def _execute_locked(self, code: str, timeout: float,
                              first_byte_timeout: Optional[float] = None,
                              status_busy_timeout: Optional[float] = 60.0,
                              preserve_on_disconnect: bool = False) -> dict:
        """Execute code on the kernel. Caller (ClusterSession) holds the asyncio lock.

        send → poll event.is_set() → return. No lock held here.

        first_byte_timeout: if set, return FirstByteTimeout error if no output arrives within
        this many seconds. Only use for health pings and stateless pool (where the kernel must
        respond quickly). Leave None for main cell execution — a long Spark query may produce
        no output for hours, and _ws_alive from the listener thread detects genuine WS death.

        status_busy_timeout: if set, return KernelHung error if the kernel does not send a
        'status: busy' message within this many seconds of the execute_request. The kernel
        always sends 'status: busy' immediately before any computation, so a missing busy
        signal means the kernel's execution queue is frozen (even if the WS is alive due to
        pings). Default 60s. Pass None to disable (e.g. for health pings that use
        first_byte_timeout instead).
        """
        import threading

        # Check connection health
        if not self._ws_alive or not self.ws or not self.ws.connected:
            return {"status": "error", "outputs": [{"type": "error",
                     "ename": "ConnectionError",
                     "evalue": "WebSocket not connected. Recycle thread should restore it."}]}

        msg_id = str(uuid.uuid4())
        self._outputs[msg_id] = []

        event = threading.Event()
        result_holder = {}
        pending = {"event": event, "result": result_holder}
        self._pending[msg_id] = pending

        # Send execute request
        # TODO: reduce back to code[:80] after debugging
        code_preview = code.replace("\n", "\\n")
        try:
            msg = make_execute_request(msg_id, self.session_id, code)
            print(f"[{time.strftime('%H:%M:%S')}] [WS] send execute_request msg_id={msg_id[:8]} code={code_preview!r}", flush=True)
            self.ws.send_binary(msg)
            print(f"[{time.strftime('%H:%M:%S')}] [WS] send OK msg_id={msg_id[:8]}", flush=True)
        except (websocket.WebSocketConnectionClosedException, BrokenPipeError, OSError) as e:
            print(f"[{time.strftime('%H:%M:%S')}] [WS] send FAILED msg_id={msg_id[:8]} error={e}", flush=True)
            self._pending.pop(msg_id, None)
            self._outputs.pop(msg_id, None)
            self._ws_alive = False
            return {"status": "error", "outputs": [{"type": "error",
                     "ename": "SendError",
                     "evalue": f"Failed to send: {str(e)[:200]}"}]}

        # Wait for reply
        start = time.time()
        got_first_byte = False
        _last_heartbeat = start

        while not event.is_set() and (time.time() - start) < timeout:
            try:
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                # A cancelled waiter cannot consume its result. Abandon tracking
                # even if the listener is already publishing the reply. The
                # disconnect handoff below still retains ownership for recovery.
                self._pending.pop(msg_id, None)
                self._outputs.pop(msg_id, None)
                raise

            # A completed, validated reply takes precedence over timeout checks.
            # Its request record stays tracked until this waiter consumes it.
            if event.is_set():
                break

            # WS died mid-execution — return immediately so lock is released and recycle can fire
            if not self._ws_alive:
                elapsed = time.time() - start
                print(f"[{time.strftime('%H:%M:%S')}] [WS] connection died mid-execution msg_id={msg_id[:8]} elapsed={elapsed:.1f}s", flush=True)
                result = {"status": "error", "outputs": [{"type": "error",
                         "ename": "ConnectionDied",
                         "evalue": "WebSocket connection died during execution"}]}
                if preserve_on_disconnect:
                    result.update(_interrupted_msg_id=msg_id,
                                  _interrupted_pending=pending,
                                  _execution_deadline=start + timeout)
                else:
                    self._pending.pop(msg_id, None)
                    self._outputs.pop(msg_id, None)
                return result

            # Status-busy timeout: kernel must acknowledge the request with 'status: busy'
            # within status_busy_timeout seconds. Unlike output, this signal arrives
            # immediately — its absence means the kernel's execution queue is frozen
            # (even if the WS is alive via server pings).
            if status_busy_timeout is not None:
                if not pending.get("got_busy", False) and not pending.get("reply_received", False):
                    if (time.time() - start) > status_busy_timeout:
                        elapsed = time.time() - start
                        print(f"[WS] KernelHung: no 'status: busy' within {status_busy_timeout}s "
                              f"msg_id={msg_id[:8]} elapsed={elapsed:.1f}s — kernel execution queue frozen")
                        self._pending.pop(msg_id, None)
                        self._outputs.pop(msg_id, None)
                        self._ws_alive = False
                        return {"status": "error", "outputs": [{"type": "error",
                                 "ename": "KernelHung",
                                 "evalue": f"Kernel did not acknowledge execute_request within "
                                           f"{status_busy_timeout}s — execution queue frozen"}]}

            # Periodic heartbeat — log every 30s so we know we're still waiting
            now = time.time()
            if now - _last_heartbeat >= 30:
                elapsed = now - start
                n_outputs = len(self._outputs.get(msg_id, []))
                print(f"[{time.strftime('%H:%M:%S')}] [WS] still waiting msg_id={msg_id[:8]} "
                      f"elapsed={elapsed:.0f}s got_first_byte={got_first_byte} "
                      f"outputs_so_far={n_outputs} ws_alive={self._ws_alive}", flush=True)
                _last_heartbeat = now

                # Cluster-liveness check: only when we've been waiting > 600s
                # AND received NO output yet. This is the narrow "stuck" pattern
                # — a healthy cluster running a heavy compute cell almost always
                # emits something (Spark stage logs, prints) within 10 minutes.
                # We poll AIDP cluster status REST API every ~5 min (handled by
                # the 30s heartbeat firing 10x) and only abort on TWO consecutive
                # terminal-dead readings (~10 min apart) to avoid one-off API
                # misreports. Transient states (RESTARTING_LIBRARIES, etc.) are
                # ignored — cell continues to wait.
                if elapsed > 600 and not got_first_byte and n_outputs == 0:
                    if not hasattr(self, "_cluster_dead_strikes"):
                        self._cluster_dead_strikes = 0
                        self._cluster_dead_last_check = 0.0
                    # Throttle the REST call to ~once per 5 min
                    if (now - self._cluster_dead_last_check) >= 300:
                        self._cluster_dead_last_check = now
                        try:
                            _status = _check_cluster_status(
                                self.cluster_id, self.lake_ocid, self.workspace_id,
                                profile=self.oci_profile)
                        except Exception:
                            _status = None
                        if _is_terminal_dead(_status):
                            self._cluster_dead_strikes += 1
                            print(f"[WS] cluster-status check: {_status} "
                                  f"(strike {self._cluster_dead_strikes}/2) on cluster "
                                  f"{self.cluster_id[:12]}", flush=True)
                            if self._cluster_dead_strikes >= 2:
                                # Two consecutive terminal-dead readings — abort.
                                self._pending.pop(msg_id, None)
                                self._outputs.pop(msg_id, None)
                                self._cluster_dead_strikes = 0
                                self._cluster_dead_last_check = 0.0
                                raise ClusterDeadError(
                                    f"Cluster {self.cluster_id[:12]} in terminal-dead "
                                    f"state '{_status}' on 2 consecutive checks; "
                                    f"aborting cell (msg_id={msg_id[:8]} elapsed={elapsed:.0f}s)"
                                )
                        else:
                            # Reset strikes on any non-terminal reading
                            # (ACTIVE / TRANSIENT / unknown / network error).
                            if self._cluster_dead_strikes > 0:
                                print(f"[WS] cluster-status check: {_status or 'unknown'} "
                                      f"— resetting strikes", flush=True)
                            self._cluster_dead_strikes = 0

            # First-byte timeout (health pings and stateless pool only — NOT main cell execution).
            # A large Spark query may produce no output for hours; _ws_alive handles genuine death.
            if (first_byte_timeout is not None and not got_first_byte
                    and not pending.get("reply_received", False)):
                if (time.time() - start) > first_byte_timeout:
                    if msg_id in self._outputs and len(self._outputs[msg_id]) > 0:
                        got_first_byte = True
                    else:
                        elapsed = time.time() - start
                        print(f"[WS] first_byte_timeout msg_id={msg_id[:8]} elapsed={elapsed:.1f}s")
                        self._pending.pop(msg_id, None)
                        self._outputs.pop(msg_id, None)
                        # ─── 2026-07-24: DO NOT set self._ws_alive = False here ───
                        # Previously this set _ws_alive = False. That's a FALSE POSITIVE:
                        # first_byte_timeout only proves the kernel didn't respond to THIS
                        # message in time (kernel busy or stuck), not that the WS transport
                        # is dead. The listener thread (mark_dead in _listen_sync) is the
                        # only source of truth for _ws_alive — it sets False on CLOSE frames
                        # and socket errors, which are the real transport-death signals.
                        #
                        # Concrete failure the old flip caused (ExampleVendor v3/v4 migration):
                        #   1. Long-running cell producing output (iopub messages arriving)
                        #   2. Health ping fires in parallel, first_byte_timeouts at 25s
                        #      because kernel is busy — old code flipped _ws_alive = False
                        #   3. cluster_session.py Fix #7's iopub-fresh smart-skip short-
                        #      circuits on the False _ws_alive → triggers recycle
                        #   4. Verify cell also can't get through → fresh session → state loss
                        #
                        # TO REVERT if this change causes NEW issues: re-add
                        #     self._ws_alive = False
                        # here. Symptoms that would justify revert: pool/health-ping
                        # timeouts followed by subsequent execute() calls that hang forever
                        # instead of failing fast (the old flip caused fast-fail via
                        # aidp_executor.py:733 pre-check).
                        return {"status": "error", "outputs": [{"type": "error",
                                 "ename": "FirstByteTimeout",
                                 "evalue": f"No output received within {first_byte_timeout}s - connection likely dead"}]}

            # Track first byte
            if not got_first_byte and msg_id in self._outputs and len(self._outputs[msg_id]) > 0:
                got_first_byte = True
                elapsed = time.time() - start
                print(f"[WS] first output received msg_id={msg_id[:8]} elapsed={elapsed:.1f}s")

        if not event.is_set():
            elapsed = time.time() - start
            print(f"[WS] timeout msg_id={msg_id[:8]} elapsed={elapsed:.0f}s timeout={timeout}s")
            self._pending.pop(msg_id, None)
            self._outputs.pop(msg_id, None)
            # ─── Fix 5c (2026-07-25): interrupt kernel on timeout ─────────────
            # Historical bug: when execute() times out, we abandon the Python-side
            # wait and return an error, BUT the kernel keeps executing the cell.
            # Any subsequent execute_request queues behind that still-running
            # operation and appears to hang with got_first_byte=False for hours.
            # Real incident: a pilot-customer AutoRefit 2026-07-25 lost 1.5 hours to this after
            # an Opus run_on_cluster invocation submitted `grep -rl /Workspace/*`
            # (a FUSE-recursive walk) that ran for 20+ min. The tool call timed
            # out at 1200s; the grep kept running; the next guard cell queued
            # behind it and never got its first byte. Cluster eventually
            # auto-terminated instead of making progress.
            #
            # Fix: on timeout, actively send a Jupyter interrupt to the kernel
            # BEFORE returning. This forces the kernel to abort the current cell
            # (raising KeyboardInterrupt in Python, kill signal to any subprocess
            # it spawned) and become responsive to the next execute request.
            #
            # Endpoint: POST /notebook/api/kernels/{kernel_id}/interrupt is the
            # Jupyter-standard interrupt path. AIDP exposes the standard Jupyter
            # notebook API under /workspaces/{ws}/notebook/api/... so this URL
            # matches the pattern used elsewhere in this file (e.g. session
            # creation at /notebook/api/sessions).
            #
            # Failure modes (all handled by try/except):
            #   - Endpoint 404 (unsupported on this AIDP build): log and return
            #     the timeout error as before — no worse than pre-fix.
            #   - Network error / signer failure: log and return timeout error.
            #   - Kernel already dead: interrupt POST fails, doesn't matter.
            #
            # Regression risk: none in the happy path (timeout is already an
            # error condition, we only add work on that path). Existing callers
            # see the same error return value; they only see the kernel actually
            # become available for the next call afterwards.
            # Fix 5c v2 (2026-07-25): skip the interrupt HTTP call if a prior
            # timeout already discovered the endpoint returns 404 on this
            # AIDP build. Reduces log noise (every 30s pool-session timeout
            # was logging a 404) without changing behavior (the interrupt
            # didn't do anything useful anyway when unsupported).
            # Once WS control-channel interrupt lands (Fix 7b), this gets
            # replaced with a real interrupt path that doesn't 404.
            if self.kernel_id and self.signer and not self._interrupt_unsupported:
                try:
                    interrupt_url = (
                        f"{self.aidp_endpoint}/20240831/dataLakes/{self.lake_ocid}"
                        f"/workspaces/{self.workspace_id}/notebook/api/kernels/"
                        f"{self.kernel_id}/interrupt"
                    )
                    r = requests.post(interrupt_url, auth=self.signer, timeout=15)
                    if r.status_code in (200, 202, 204):
                        print(f"[WS] kernel interrupt sent msg_id={msg_id[:8]} "
                              f"kernel={self.kernel_id[:8]} HTTP {r.status_code}")
                    elif r.status_code == 404:
                        # Endpoint not available on this AIDP build. Log ONCE
                        # (with context) and set flag so future timeouts don't
                        # re-log the same 404. Original error return unchanged.
                        print(f"[WS] kernel interrupt endpoint returned 404 — "
                              f"AIDP build does not expose /kernels/{{id}}/interrupt. "
                              f"Disabling further interrupt attempts for this session "
                              f"(msg_id={msg_id[:8]}). Runaway cells rely on Fix 5a "
                              f"(Opus prompt guardrail) and cluster-lifecycle recycle.")
                        self._interrupt_unsupported = True
                    else:
                        # Other non-2xx (5xx, auth, etc.) — log body for debug,
                        # do NOT flip the unsupported flag (transient error).
                        print(f"[WS] kernel interrupt HTTP {r.status_code} "
                              f"msg_id={msg_id[:8]} — subsequent execute may hang. "
                              f"body: {r.text[:200]}")
                    # Give the kernel a couple seconds to abort the running op
                    # before the caller submits the next request. Empirically
                    # Python interrupts propagate in <1s; subprocess kills take
                    # a bit longer as SIGINT/SIGTERM/SIGKILL cascade. Skip the
                    # sleep when we already know interrupt is unsupported —
                    # no point waiting for something that didn't happen.
                    if not self._interrupt_unsupported:
                        time.sleep(2)
                except Exception as _int_err:
                    # Interrupt is best-effort. Log but return the timeout error.
                    print(f"[WS] kernel interrupt attempt failed msg_id={msg_id[:8]}: "
                          f"{type(_int_err).__name__}: {str(_int_err)[:200]}")
            return {"status": "error", "outputs": [{"type": "error",
                     "ename": "TimeoutError",
                     "evalue": f"Execution timed out after {timeout}s"}]}

        result = result_holder.get("result", {"status": "error", "outputs": []})
        self._pending.pop(msg_id, None)
        self._outputs.pop(msg_id, None)
        return result

    async def execute(self, code: str, timeout: float = 7200,
                      first_byte_timeout: Optional[float] = None) -> dict:
        """Public execute method — delegates to _execute_locked."""
        return await self._execute_locked(code, timeout=timeout,
                                          first_byte_timeout=first_byte_timeout)

    async def reconnect_ws(self, preserve_msg_id: Optional[str] = None):
        """Reconnect WebSocket to the existing session/kernel. Preserves REPL state.

        Closes the old WebSocket and reconnects to the same kernel_id/session_id.
        Does NOT touch the REST session — kernel variables and imports are preserved.
        Raises on failure so caller can decide whether to create a fresh session.
        """
        import threading

        # Stop old listener
        self._running = False
        self._ws_alive = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

        # An interrupted cell may still be executing on this kernel. Keep its
        # parent msg_id so a reply delivered after reconnect can be attributed to
        # the original request. Other entries belong to the dead connection.
        preserved = self._pending.get(preserve_msg_id) if preserve_msg_id else None
        outputs = self._outputs.get(preserve_msg_id) if preserved else None
        self._pending.clear()
        self._outputs.clear()
        if preserved:
            self._pending[preserve_msg_id] = preserved
            self._outputs[preserve_msg_id] = outputs if outputs is not None else []

        await asyncio.sleep(1)

        # Reconnect to same kernel (same session_id, same kernel_id)
        request_id = str(uuid.uuid4())
        session_id_param = f"{request_id}---{self.session_id}"

        ws_url = (
            f"wss://{self.ws_host}/20240831/dataLakes/{self.lake_ocid}"
            f"/notebook/workspaces/{self.workspace_id}"
            f"/api/kernels/{self.kernel_id}/channels"
            f"?session_id={session_id_param}"
        )

        sign_url = ws_url.replace("wss://", "https://")
        ws_headers = self._sign_ws_headers(sign_url, self.ws_host)

        new_ws = websocket.WebSocket()
        header_list = [f"{k}: {v}" for k, v in ws_headers.items()]
        new_ws.connect(
            ws_url,
            header=header_list,
            subprotocols=["v1.kernel.websocket.jupyter.org"],
            timeout=30,
        )
        self.ws = new_ws
        print(f"[AIDP] WebSocket reconnected (session {self.session_id})")

        # New handshake → new opc-request-id; capture it (recycle reconnects are a
        # common point for drops, so each reconnect's id must be on record).
        try:
            self.ws_opc_request_id = self._opc_from_headers(self.ws.getheaders())
        except Exception:
            self.ws_opc_request_id = ""
        self._log_ws_opc("reconnect", self.ws_opc_request_id,
                         ws_host=self.ws_host, session=self.session_id, kernel=self.kernel_id)

        # Restart listener thread
        self._running = True
        self._ws_alive = True
        self._last_recv_time = time.time()
        self._listener_generation += 1
        self._listener_thread = threading.Thread(
            target=self._listen_sync, args=(self._listener_generation,), daemon=True)
        self._listener_thread.start()

        await asyncio.sleep(2)

        # Verify kernel still alive
        ki_msg = make_kernel_info_request(str(uuid.uuid4()), self.session_id)
        self.ws.send_binary(ki_msg)
        await asyncio.sleep(1)
        print("[AIDP] Kernel info requested after WS reconnect.")

    async def close(self, delete_session: bool = True):
        """Close the WebSocket; optionally retain an unresolved REST session."""
        self._running = False

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

        # Delete session via REST — prevents orphaned kernels from accumulating
        # on the cluster and consuming resources.
        if delete_session and self.session_id and self.signer:
            try:
                delete_url = (
                    f"{self.aidp_endpoint}/20240831/dataLakes/{self.lake_ocid}"
                    f"/workspaces/{self.workspace_id}/notebook/api/sessions/{self.session_id}"
                )
                requests.delete(delete_url, auth=self.signer, timeout=60)
                print(f"[AIDP] Session {self.session_id} deleted", flush=True)
            except Exception as e:
                print(f"[AIDP] Error deleting session: {e}", flush=True)


# ─── High-level functions ─────────────────────────────────────────────

def _kernel_prelude_error(outputs: list):
    """REF-225: recognize the observed platform traceback, not arbitrary prose.

    This is a failure-only safety backstop; absence never certifies execution.
    No reset/replay is attempted on an already poisoned kernel.
    """
    import re
    text = re.sub(r'\x1b\[[0-9;]*m', '', format_outputs(outputs))
    frame = re.search(r"(?m)^----> 1 sc\.setJobGroup\([^\n]*", text)
    error = re.search(r"(?m)^AttributeError: [^\n]*has no attribute ['\"]setJobGroup['\"]", text)
    if not frame or not error:
        return None
    return {"type": "error", "ename": "KernelPreexecutionError",
            "evalue": error.group(0),
            "traceback": [frame.group(0), error.group(0),
                          "AIDP command setup failed before submitted code ran. "
                          "The kernel global sc was replaced. Repair the source "
                          "collision and rerun from a fresh kernel; no automatic "
                          "restart or state replay was attempted."]}


def format_outputs(outputs: list) -> str:
    """Format cell outputs for display."""
    parts = []
    for out in outputs:
        if out["type"] == "stream":
            parts.append(out["text"])
        elif out["type"] == "execute_result":
            data = out.get("data", {})
            if "text/plain" in data:
                parts.append(data["text/plain"])
        elif out["type"] == "display_data":
            data = out.get("data", {})
            if "text/plain" in data:
                parts.append(data["text/plain"])
            if "image/png" in data:
                parts.append("[image/png output]")
        elif out["type"] == "error":
            tb = out.get("traceback", [])
            # Strip ANSI codes
            import re
            clean = [re.sub(r'\x1b\[[0-9;]*m', '', line) for line in tb]
            parts.append("\n".join(clean) or
                         f"{out.get('ename', 'Error')}: {out.get('evalue', '')}")
    return "".join(parts)


async def execute_notebook(notebook_path: str, session: AIDPSession,
                           stop_on_error: bool = True) -> list:
    """Execute all code cells in a notebook on AIDP."""
    with open(notebook_path, "r") as f:
        nb = json.load(f)

    cells = nb.get("cells", [])
    results = []

    print(f"\nExecuting {notebook_path} ({len(cells)} cells)")
    print("=" * 60)

    cell_num = 0
    for i, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue

        source = "".join(cell.get("source", []))
        if not source.strip():
            continue

        # Skip magic commands that can't be sent raw
        if source.strip().startswith("!"):
            # Convert shell commands
            cmd = source.strip()[1:]
            source = f"import subprocess; subprocess.run({repr(cmd)}, shell=True)"

        cell_num += 1
        print(f"\n--- Cell {cell_num} (notebook cell {i}) ---")
        # Show first 3 lines of code
        lines = source.strip().split("\n")
        preview = "\n".join(lines[:3])
        if len(lines) > 3:
            preview += f"\n  ... ({len(lines)} lines total)"
        print(f"Code: {preview}")

        result = await session.execute(source, timeout=7200)
        status = result.get("status", "error")
        outputs = result.get("outputs", [])
        output_text = format_outputs(outputs)

        results.append({
            "cell_index": i,
            "cell_num": cell_num,
            "status": status,
            "output": output_text,
            "code_preview": lines[0][:80] if lines else "",
        })

        if output_text:
            # Truncate very long outputs
            display = output_text[:2000]
            if len(output_text) > 2000:
                display += f"\n... ({len(output_text)} chars total)"
            print(f"Output: {display}")

        if status == "ok":
            print(f"Status: OK")
        else:
            print(f"Status: ERROR")
            if stop_on_error:
                print("Stopping execution due to error.")
                break

    print(f"\n{'=' * 60}")
    ok_count = sum(1 for r in results if r["status"] == "ok")
    err_count = sum(1 for r in results if r["status"] != "ok")
    print(f"Results: {ok_count} OK, {err_count} errors out of {len(results)} cells")

    return results


async def interactive_repl(session: AIDPSession):
    """Interactive REPL for the AIDP kernel."""
    print("\nAIDP PySpark REPL")
    print("Type Python/PySpark code. Enter blank line to execute.")
    print("Type 'exit' or 'quit' to end.\n")

    while True:
        try:
            lines = []
            prompt = ">>> "
            while True:
                line = input(prompt)
                if line.strip() in ("exit", "quit") and not lines:
                    return
                if line == "" and lines:
                    break
                lines.append(line)
                prompt = "... "

            code = "\n".join(lines)
            if not code.strip():
                continue

            result = await session.execute(code)
            output = format_outputs(result.get("outputs", []))
            if output:
                print(output)
            if result.get("status") != "ok":
                print(f"[{result.get('status', 'error')}]")

        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            return


# ─── CLI ──────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="AIDP Notebook Executor")
    parser.add_argument("--code", help="Execute a single code string")
    parser.add_argument("--notebook", help="Execute all cells in a notebook")
    parser.add_argument("--repl", action="store_true", help="Start interactive REPL")
    parser.add_argument("--lake", default=DEFAULT_LAKE_OCID, help="AIDP Lake OCID")
    parser.add_argument("--workspace", default=DEFAULT_WORKSPACE_ID, help="Workspace ID")
    parser.add_argument("--cluster", required=True, help="Cluster ID (required — no default)")
    parser.add_argument("--profile", default=DEFAULT_OCI_PROFILE, help="OCI config profile")
    parser.add_argument("--stop-on-error", action="store_true", default=True,
                        help="Stop notebook execution on first error")
    parser.add_argument("--output", help="Save execution results to JSON file")
    args = parser.parse_args()

    session = AIDPSession(
        lake_ocid=args.lake,
        workspace_id=args.workspace,
        cluster_id=args.cluster,
        oci_profile=args.profile,
    )

    try:
        await session.connect()

        if args.code:
            result = await session.execute(args.code)
            output = format_outputs(result.get("outputs", []))
            if output:
                print(output)
            if result.get("status") != "ok":
                print(f"[{result.get('status')}]")
                sys.exit(1)

        elif args.notebook:
            results = await execute_notebook(args.notebook, session,
                                             stop_on_error=args.stop_on_error)
            if args.output:
                with open(args.output, "w") as f:
                    json.dump(results, f, indent=2)
                print(f"\nResults saved to {args.output}")

            # Exit code based on results
            errors = sum(1 for r in results if r["status"] != "ok")
            if errors > 0:
                sys.exit(1)

        elif args.repl:
            await interactive_repl(session)

        else:
            parser.print_help()

    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
