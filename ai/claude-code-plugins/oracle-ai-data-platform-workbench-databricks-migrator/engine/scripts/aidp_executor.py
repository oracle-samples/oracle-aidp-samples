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

REGION_MAP = {
    "iad": "us-ashburn-1", "phx": "us-phoenix-1", "fra": "eu-frankfurt-1",
    "lhr": "uk-london-1", "bom": "<OCI_REGION>", "hyd": "ap-hyderabad-1",
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

def get_oci_signer(profile: str = DEFAULT_OCI_PROFILE):
    """Create OCI signer — supports API key and security token auth.

    Detects auth type from the OCI config profile:
      security_token_file present → SecurityTokenSigner (oci session authenticate)
      otherwise                   → API key Signer (fingerprint + key_file)
    """
    import os
    config = oci.config.from_file(profile_name=profile)

    if config.get("security_token_file"):
        from oci.auth.signers import SecurityTokenSigner
        token_path = os.path.expanduser(config["security_token_file"])
        with open(token_path) as f:
            token = f.read().strip()
        private_key = oci.signer.load_private_key_from_file(
            os.path.expanduser(config["key_file"]),
            config.get("pass_phrase"),
        )
        signer = SecurityTokenSigner(token=token, private_key=private_key)
    else:
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            private_key_content=config.get("key_content"),
        )

    return config, signer


def get_region_from_ocid(lake_ocid: str) -> str:
    """Extract region from AIDP Lake OCID.

    OCID format: ocid1.<resource>.<realm>.<region>.<unique-id>
    parts[3] is already the full region name (e.g. '<OCI_REGION>'), not a 3-letter code.
    """
    parts = lake_ocid.split(".")
    if len(parts) > 3:
        region = parts[3]
        # If it looks like a 3-letter code (legacy), map it; otherwise use as-is
        return REGION_MAP.get(region, region)
    return "<OCI_REGION>"


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
                 session_name: str = "aidp_executor_session"):
        # Resolve from the live module globals at CALL time (not import time) so
        # a runtime override of DEFAULT_* takes effect. lake/workspace/cluster
        # have no hardcoded default and must be provided; only profile defaults.
        self.lake_ocid = lake_ocid if lake_ocid is not None else DEFAULT_LAKE_OCID
        self.workspace_id = workspace_id if workspace_id is not None else DEFAULT_WORKSPACE_ID
        self.cluster_id = cluster_id if cluster_id is not None else DEFAULT_CLUSTER_ID
        self.oci_profile = oci_profile if oci_profile is not None else DEFAULT_OCI_PROFILE
        self._session_name = session_name

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
        session_url = (
            f"{self.aidp_endpoint}/20240831/dataLakes/{self.lake_ocid}"
            f"/workspaces/{self.workspace_id}/notebook/api/sessions"
        )

        # List existing sessions and reuse one matching our notebook name
        list_url = f"{session_url}?path=Workspace/{notebook_name}"
        print(f"[AIDP] GET {list_url}")
        try:
            list_resp = requests.get(list_url, auth=self.signer,
                                     headers={"Accept": "application/json"})
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
                    if s.get("name") == notebook_name or s.get("path") == f"Workspace/{notebook_name}":
                        session_data = s
                        print(f"[AIDP] Reusing existing session {s['id']} for {notebook_name}")
                        break
        else:
            print(f"[AIDP] List sessions failed {list_resp.status_code}: {list_resp.text[:500]} — will create new session")

        if session_data is None:
            body = json.dumps({
                "type": "notebook",
                "name": notebook_name,
                "kernel": {"name": "notebook"},
                "path": f"Workspace/{notebook_name}",
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
                                              "Accept": "application/json"})
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

        self.session_id = session_data["id"]

        # Kernel may be null if the cluster is still warming up after a restart.
        # Poll the session until the kernel is ready (up to 3 minutes).
        if session_data.get("kernel") is None:
            get_url = session_url + f"/{self.session_id}"
            print(f"[AIDP] Kernel not ready yet, polling session {self.session_id}...")
            for _attempt in range(36):  # 36 × 5s = 3 min
                await asyncio.sleep(5)
                _r = requests.get(get_url, auth=self.signer,
                                  headers={"Accept": "application/json"})
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
                self._ws_alive = False

        unanswered_pings = 0

        while self._running and self.ws and self.ws.connected:
            try:
                self.ws.settimeout(30)  # Don't block forever
                opcode, data = self.ws.recv_data(control_frame=True)
                self._last_recv_time = time.time()

                if opcode == websocket.ABNF.OPCODE_CLOSE:
                    print("[AIDP] WebSocket received CLOSE frame")
                    mark_dead()
                    break
                if opcode == websocket.ABNF.OPCODE_PING:
                    self.ws.pong(data)
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
                    if self.ws and self.ws.connected:
                        self.ws.ping()
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

        if msg_type == "stream":
            if parent_id in self._outputs:
                text = msg["content"].get("text", "")
                # AIDP wraps stream output as [{"type":"TEXT_PLAIN","value":"..."}] JSON
                # inside the standard Jupyter stream text field. Unwrap to plain text.
                text = _unwrap_aidp_text(text)
                self._outputs[parent_id].append({
                    "type": "stream",
                    "name": msg["content"].get("name", "stdout"),
                    "text": text,
                })

        elif msg_type == "execute_result":
            if parent_id in self._outputs:
                self._outputs[parent_id].append({
                    "type": "execute_result",
                    "data": msg["content"].get("data", {}),
                })

        elif msg_type == "display_data":
            if parent_id in self._outputs:
                self._outputs[parent_id].append({
                    "type": "display_data",
                    "data": msg["content"].get("data", {}),
                })

        elif msg_type == "error":
            if parent_id in self._outputs:
                self._outputs[parent_id].append({
                    "type": "error",
                    "ename": msg["content"].get("ename", ""),
                    "evalue": msg["content"].get("evalue", ""),
                    "traceback": msg["content"].get("traceback", []),
                })

        elif msg_type == "status":
            execution_state = msg["content"].get("execution_state", "")
            if execution_state == "busy" and parent_id in self._pending:
                self._pending[parent_id]["got_busy"] = True
                print(f"[WS] kernel busy — msg_id={parent_id[:8]}")
            elif execution_state == "idle" and parent_id in self._pending:
                print(f"[WS] kernel idle — msg_id={parent_id[:8]}")

        elif msg_type == "execute_reply":
            if parent_id in self._pending:
                pending = self._pending.pop(parent_id)
                status = msg["content"].get("status", "error")
                outputs = self._outputs.pop(parent_id, [])
                print(f"[WS] execute_reply msg_id={parent_id[:8]} status={status}")
                pending["result"]["result"] = {
                    "status": status,
                    "outputs": outputs,
                }
                pending["event"].set()

    async def _execute_locked(self, code: str, timeout: float,
                              first_byte_timeout: Optional[float] = None,
                              status_busy_timeout: Optional[float] = 60.0) -> dict:
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
        self._pending[msg_id] = {"event": event, "result": result_holder}

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
            await asyncio.sleep(0.1)

            # Re-check immediately after sleep — execute_reply may have arrived during the
            # sleep and set the event + popped msg_id from _pending. Without this guard,
            # the error checks below would run on stale state (e.g. KernelHung fires because
            # got_busy is missing from the already-popped _pending entry).
            if event.is_set():
                break

            # WS died mid-execution — return immediately so lock is released and recycle can fire
            if not self._ws_alive:
                elapsed = time.time() - start
                print(f"[{time.strftime('%H:%M:%S')}] [WS] connection died mid-execution msg_id={msg_id[:8]} elapsed={elapsed:.1f}s", flush=True)
                self._pending.pop(msg_id, None)
                self._outputs.pop(msg_id, None)
                return {"status": "error", "outputs": [{"type": "error",
                         "ename": "ConnectionDied",
                         "evalue": "WebSocket connection died during execution"}]}

            # Status-busy timeout: kernel must acknowledge the request with 'status: busy'
            # within status_busy_timeout seconds. Unlike output, this signal arrives
            # immediately — its absence means the kernel's execution queue is frozen
            # (even if the WS is alive via server pings).
            if status_busy_timeout is not None:
                if not self._pending.get(msg_id, {}).get("got_busy", False):
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
            if first_byte_timeout is not None and not got_first_byte:
                if (time.time() - start) > first_byte_timeout:
                    if msg_id in self._outputs and len(self._outputs[msg_id]) > 0:
                        got_first_byte = True
                    else:
                        elapsed = time.time() - start
                        print(f"[WS] first_byte_timeout msg_id={msg_id[:8]} elapsed={elapsed:.1f}s")
                        self._pending.pop(msg_id, None)
                        self._outputs.pop(msg_id, None)
                        self._ws_alive = False
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
            return {"status": "error", "outputs": [{"type": "error",
                     "ename": "TimeoutError",
                     "evalue": f"Execution timed out after {timeout}s"}]}

        return result_holder.get("result", {"status": "error", "outputs": []})

    async def execute(self, code: str, timeout: float = 7200,
                      first_byte_timeout: Optional[float] = None) -> dict:
        """Public execute method — delegates to _execute_locked."""
        return await self._execute_locked(code, timeout=timeout,
                                          first_byte_timeout=first_byte_timeout)

    async def reconnect_ws(self):
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

        # Clear stale pending requests — they will never receive a reply on the new WS.
        # Normally _pending is empty here because _execute_locked detects _ws_alive=False
        # and pops its own entry before releasing the lock. Clear defensively to prevent
        # ghost entries from accumulating if the cleanup path was skipped.
        self._pending.clear()
        self._outputs.clear()

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
        )
        self.ws = new_ws
        print(f"[AIDP] WebSocket reconnected (session {self.session_id})")

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
        print("[AIDP] Kernel verified after WS reconnect.")

    async def close(self):
        """Close the session and WebSocket."""
        self._running = False

        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

        # Delete session via REST — prevents orphaned kernels from accumulating
        # on the cluster and consuming resources.
        if self.session_id and self.signer:
            try:
                delete_url = (
                    f"{self.aidp_endpoint}/20240831/dataLakes/{self.lake_ocid}"
                    f"/workspaces/{self.workspace_id}/notebook/api/sessions/{self.session_id}"
                )
                requests.delete(delete_url, auth=self.signer)
                print(f"[AIDP] Session {self.session_id} deleted", flush=True)
            except Exception as e:
                print(f"[AIDP] Error deleting session: {e}", flush=True)


# ─── High-level functions ─────────────────────────────────────────────

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
            parts.append("\n".join(clean))
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
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER_ID, help="Cluster ID")
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
