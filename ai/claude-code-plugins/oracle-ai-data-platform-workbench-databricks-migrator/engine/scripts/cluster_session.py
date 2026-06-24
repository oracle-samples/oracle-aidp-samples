"""
Cluster Session Singleton + Stateless Pool
==========================================
Two-tier WebSocket access to the AIDP cluster:

1. ClusterSession (singleton) — main executor:
   - Single WebSocket, asyncio.Lock
   - Serializes code execution (kernel state is shared)
   - Lock window: acquire → ws.send → await response → release
   - asyncio.Lock: waiting coroutines yield to each other (event loop stays alive)
   - Async recycle task: cooperative, no threading.Thread, no asyncio.new_event_loop()

2. StatelessPool — concurrent I/O:
   - 3 independent WebSocket sessions
   - No lock, no shared state
   - File I/O (exists, upload, download), path checks, catalog queries route here
   - asyncio.Queue for fair round-robin across pool sessions

Usage:
    from cluster_session import cluster

    await cluster.connect(cluster_id="<cluster_id>")

    # Code execution (serialized)
    result = await cluster.execute("print('hello')")

    # Stateless I/O (concurrent, pool)
    exists = await cluster.file_exists("/tmp/foo.txt")
    ok = await cluster.upload("content", "/tmp/foo.txt")
    text = await cluster.download("/tmp/foo.txt")

    # Explicit stateless (for tools)
    result = await cluster.run_stateless("import os; print(os.listdir('/'))")

    await cluster.close()
"""

import asyncio
import time
import json
import base64
from typing import Optional, List

from aidp_executor import AIDPSession


# ─── Stateless Pool ─────────────────────────────────────────────────────


class StatelessPool:
    """Pool of independent WebSocket sessions for concurrent stateless I/O.

    Each session has its own Python namespace — no shared kernel state.
    Operations: file exists/read/write, path checks, catalog queries.
    """

    def __init__(self, cluster_id: str, size: int = 3, session_prefix: str = "aidp_pool"):
        self._cluster_id = cluster_id
        self._size = size
        self._session_prefix = session_prefix
        self._queue: Optional[asyncio.Queue] = None

    async def start(self):
        self._queue = asyncio.Queue()
        for i in range(self._size):
            # Each pool session uses a unique path so AIDP creates independent kernels
            s = AIDPSession(cluster_id=self._cluster_id,
                            session_name=f"{self._session_prefix}_session_{i}")
            await s.connect()
            await self._queue.put(s)
        print(f"[pool] Started {self._size} stateless sessions")

    async def run(self, code: str, timeout: float = 30) -> dict:
        """Run code on any available pool session. Concurrent, no ordering guarantee."""
        session = await self._queue.get()
        try:
            if not session._ws_alive:
                old_name = session._session_name  # preserve unique name → independent kernel
                try:
                    await session.close()
                except Exception:
                    pass
                session = AIDPSession(cluster_id=self._cluster_id, session_name=old_name)
                await session.connect()
            return await session._execute_locked(code, timeout, first_byte_timeout=15)
        finally:
            await self._queue.put(session)

    async def stop(self):
        if not self._queue:
            return
        sessions = []
        while not self._queue.empty():
            sessions.append(await self._queue.get())
        for s in sessions:
            try:
                await s.close()
            except Exception:
                pass


# ─── Cluster Session Singleton ───────────────────────────────────────────


class ClusterSession:
    """Singleton: owns the AIDP main executor + stateless pool.

    Main executor: asyncio.Lock serializes one WS round-trip at a time.
    Stateless pool: 3 sessions for concurrent file I/O / path checks.
    Recycle: asyncio.Task (cooperative, no threading, no isolated loops).
    """

    _instance: Optional['ClusterSession'] = None
    _init_lock = asyncio.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._session: Optional[AIDPSession] = None
        self._lock = asyncio.Lock()          # asyncio.Lock: cooperative wait, no event loop freeze
        self._cluster_id: Optional[str] = None
        self._recycle_task: Optional[asyncio.Task] = None
        self._pool: Optional[StatelessPool] = None
        self._running = False
        self._recycle_count = 0
        self._last_recycle = 0
        self._lock_acquired_at: float = 0   # timestamp when execute() acquired the lock (0 = free)
        self._kwargs: dict = {}              # saved kwargs for fresh session creation
        self._initialized = True

    def register_bootstrap(self, code: str):
        """Register a bootstrap code snippet to replay after fresh session creation.
        Call this right after each bootstrap execute() so fresh sessions get the same setup."""
        if not hasattr(self, '_bootstrap_snippets'):
            self._bootstrap_snippets = []
        self._bootstrap_snippets.append(code)

    async def connect(self, cluster_id: str, **kwargs):
        """Initialize the session and pool. Call once at startup."""
        self._cluster_id = cluster_id
        self._kwargs = kwargs               # save for fresh session recreation
        self._bootstrap_snippets = []       # reset bootstrap on new connection
        self._session = AIDPSession(cluster_id=cluster_id, **kwargs)
        await self._session.connect()
        self._running = True
        self._last_recycle = time.time()

        # Start stateless pool (3 concurrent sessions)
        pool_prefix = kwargs.get("session_name", "aidp_pool").replace("aidp_mig_", "aidp_pool_")
        self._pool = StatelessPool(cluster_id=cluster_id, size=1, session_prefix=pool_prefix)
        await self._pool.start()

        # Start async recycle task (replaces threading.Thread)
        if self._recycle_task and not self._recycle_task.done():
            self._recycle_task.cancel()
            try:
                await self._recycle_task
            except asyncio.CancelledError:
                pass
        self._recycle_task = asyncio.create_task(self._recycle_loop_async())

    async def execute(self, code: str, timeout: float = 300) -> dict:
        """Execute code on the main kernel. Serialized via asyncio.Lock.

        Lock window: acquire → ws.send → await response → release.
        With asyncio.Lock, waiting coroutines yield cooperatively — the event
        loop stays alive for Opus API calls, asyncio tasks, etc.

        No first_byte_timeout: a large Spark query may produce no output for hours.
        _ws_alive (set by the listener thread on WS CLOSE) detects genuine connection death.
        _lock_acquired_at tracks when the lock was taken so the recycle loop can detect
        a frozen kernel (lock held >2 min) and force-close the WS to unblock this method.
        """
        async with self._lock:
            self._lock_acquired_at = time.time()
            try:
                if not self._session or not self._session._ws_alive:
                    return {"status": "error", "outputs": [{"type": "error",
                             "ename": "SessionDead",
                             "evalue": "Session not connected. Recycle should restore it."}]}
                return await self._session._execute_locked(code, timeout, first_byte_timeout=None)
            finally:
                self._lock_acquired_at = 0

    async def run_stateless(self, code: str, timeout: float = 30) -> dict:
        """Run stateless I/O code on the pool. Concurrent, no lock.

        Use for: file exists/read/write, path checks, catalog queries,
        inspect_package_source, explore_path, describe_table, etc.
        Do NOT use for: code that depends on main kernel state (variables, imports).
        """
        if self._pool:
            return await self._pool.run(code, timeout)
        # Fallback if pool not started (shouldn't happen)
        return await self.execute(code, timeout)

    async def upload(self, local_content: str, remote_path: str) -> bool:
        """Upload content to a file on the cluster filesystem. Uses stateless pool."""
        b64 = base64.b64encode(local_content.encode("utf-8")).decode("ascii")
        CHUNK = 45000

        if len(b64) <= CHUNK:
            result = await self.run_stateless(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{b64}'))
print('OK')
""", timeout=30)
        else:
            chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]
            await self.run_stateless(f"""
import base64, builtins, os
os.makedirs(os.path.dirname('{remote_path}'), exist_ok=True)
with builtins.open('{remote_path}', 'wb') as f:
    f.write(base64.b64decode('{chunks[0]}'))
""", timeout=30)
            for chunk in chunks[1:]:
                await self.run_stateless(f"""
import base64, builtins
with builtins.open('{remote_path}', 'ab') as f:
    f.write(base64.b64decode('{chunk}'))
""", timeout=30)
            result = {"status": "ok"}

        return result.get("status") == "ok"

    async def download(self, remote_path: str) -> Optional[str]:
        """Read a file from the cluster filesystem. Uses stateless pool."""
        result = await self.run_stateless(f"""
import builtins
with builtins.open('{remote_path}', 'r') as f:
    print(f.read())
""", timeout=30)
        if result.get("status") == "ok":
            from context_tools import _unwrap_aidp_text
            from aidp_executor import format_outputs
            raw = format_outputs(result.get("outputs", []))
            # _unwrap_aidp_text handles the AIDP JSON wrapper; deduplicate duplicates
            unwrapped = _unwrap_aidp_text(raw)
            # If unwrap didn't work (multiple JSON chunks), try first chunk only
            if unwrapped.startswith('[{"type"') or unwrapped.startswith('[{"type":'):
                outputs = result.get("outputs", [])
                if outputs:
                    first = outputs[0].get("text", "")
                    unwrapped = _unwrap_aidp_text(first)
            return unwrapped
        return None

    async def file_exists(self, remote_path: str) -> bool:
        """Check if a file exists on the cluster. Uses stateless pool."""
        result = await self.run_stateless(
            f"import os; print(os.path.exists('{remote_path}'))", timeout=15)
        if result.get("status") == "ok":
            from context_tools import _unwrap_aidp_text
            from aidp_executor import format_outputs
            output = _unwrap_aidp_text(format_outputs(result.get("outputs", [])))
            return "True" in output
        return False

    async def close(self):
        """Shut down executor, pool, and recycle task."""
        self._running = False
        if self._recycle_task and not self._recycle_task.done():
            self._recycle_task.cancel()
            try:
                await self._recycle_task
            except asyncio.CancelledError:
                pass
        if self._pool:
            await self._pool.stop()
            self._pool = None
        if self._session:
            await self._session.close()
            self._session = None
        self._cluster_id = None

    # ─── Cluster Switching ─────────────────────────────────────────────

    async def switch_cluster(self, new_cluster_id: str, session_name: str = None):
        """Switch to a different AIDP cluster.

        Closes the current session/pool, ensures the target cluster is running,
        installs aidp_compat if missing, connects, and replays bootstrap snippets.

        No-op if already connected to the same cluster.
        """
        if new_cluster_id == self._cluster_id:
            return

        from cluster_lifecycle import ensure_cluster_running, ensure_aidp_compat_installed

        # Save bootstrap before close() — connect() resets _bootstrap_snippets
        old_bootstrap = list(getattr(self, '_bootstrap_snippets', []))

        # Tear down current connection
        await self.close()

        # Ensure target cluster is running (start if needed, poll up to 30 min)
        await ensure_cluster_running(new_cluster_id, timeout=1800)

        # Ensure aidp_compat is installed on the target cluster
        await ensure_aidp_compat_installed(new_cluster_id)

        # Connect to new cluster
        kwargs = dict(self._kwargs)
        if session_name:
            kwargs["session_name"] = session_name
        await self.connect(cluster_id=new_cluster_id, **kwargs)

        # Replay bootstrap snippets (aidp_compat import, output dir, etc.)
        if old_bootstrap:
            print(f"[cluster] Replaying {len(old_bootstrap)} bootstrap snippet(s) on new cluster...")
            for i, snippet in enumerate(old_bootstrap):
                try:
                    result = await self.execute(snippet, timeout=60)
                    if result.get("status") == "ok":
                        self.register_bootstrap(snippet)
                    else:
                        print(f"[cluster] Bootstrap snippet {i+1} failed: {result.get('status')}")
                        self.register_bootstrap(snippet)  # still register for future recycles
                except Exception as e:
                    print(f"[cluster] Bootstrap snippet {i+1} error: {e}")
                    self.register_bootstrap(snippet)
            print(f"[cluster] Bootstrap replay complete on {new_cluster_id[:12]}...")

    # ─── Async Recycle Loop ───────────────────────────────────────────

    async def _recycle_loop_async(self):
        """Async task: health ping every 30s, scheduled recycle every 5 min,
        REST keepalive every 10 min.

        Runs as asyncio.create_task() — cooperative, no threading.Thread,
        no asyncio.new_event_loop(). Acquires asyncio.Lock cooperatively.

        REST keepalive: AIDP clusters have idle timeouts (e.g. 30 min). The idle
        detector counts REST API calls, NOT WebSocket traffic. During long-running
        cells (Spark jobs), there are zero REST calls, so the cluster appears idle
        and auto-terminates. A periodic GET to the sessions list endpoint resets the
        idle timer without any side effects.

        Freeze detection: if execute() holds the lock for >2 min (kernel frozen,
        not a long Spark job), force-closes the WS WITHOUT acquiring the lock.
        This trips _ws_alive=False in the listener thread, which causes the
        _execute_locked() poll loop to return immediately with a ConnectionDied error,
        releasing the lock so the recycle can proceed normally.
        """
        RECYCLE_INTERVAL = 300   # 5 minutes
        PING_INTERVAL = 30       # ping every 30s
        FREEZE_THRESHOLD = 600   # 10 min lock-held → frozen kernel (not a long Spark job)
        KEEPALIVE_INTERVAL = 600 # REST keepalive every 10 min (well within 30-min idle timeout)
        last_keepalive = time.time()

        while self._running:
            await asyncio.sleep(PING_INTERVAL)
            if not self._running:
                break

            now = time.time()
            needs_recycle = False

            # Freeze detection: lock held too long AND WS has received no traffic.
            # A legitimately busy Spark job receives server pings ~every 30s, so
            # _last_recv_time stays fresh even when no cell output is produced.
            # A truly frozen kernel produces no WS traffic at all — ws_idle grows.
            # Both conditions must be true to avoid killing long-running Spark jobs.
            if self._lock.locked() and self._lock_acquired_at > 0:
                held = now - self._lock_acquired_at
                ws_idle = now - self._session._last_recv_time
                if held > FREEZE_THRESHOLD and ws_idle > FREEZE_THRESHOLD and self._session:
                    print(f"[cluster] FREEZE DETECTED: lock held {held:.0f}s, "
                          f"WS idle {ws_idle:.0f}s. Force-closing WS to unblock execute()...")
                    try:
                        self._session._ws_alive = False  # signal poll loop to return
                        if self._session.ws:
                            self._session.ws.close()     # listener confirms _ws_alive=False
                    except Exception as e:
                        print(f"[cluster] Force-close error: {e}")
                    # execute() will return shortly; skip ping, let next iteration recycle
                    continue

            # REST keepalive — prevent AIDP idle timeout from terminating cluster.
            # GET sessions list is lightweight and resets the idle timer.
            if (now - last_keepalive) >= KEEPALIVE_INTERVAL and self._session:
                last_keepalive = now
                try:
                    import requests as _req
                    s = self._session
                    keepalive_url = (
                        f"{s.aidp_endpoint}/20240831/dataLakes/{s.lake_ocid}"
                        f"/workspaces/{s.workspace_id}/notebook/api/sessions"
                    )
                    _r = _req.get(keepalive_url, auth=s.signer,
                                  headers={"Accept": "application/json"}, timeout=15)
                    print(f"[cluster] REST keepalive ping: HTTP {_r.status_code} "
                          f"({len(_r.json()) if _r.ok else 0} sessions)", flush=True)
                except Exception as e:
                    print(f"[cluster] REST keepalive error: {e}", flush=True)

            # Health ping — acquires lock cooperatively (no event loop freeze)
            if self._session and self._session._ws_alive:
                try:
                    async with self._lock:
                        result = await self._session._execute_locked(
                            "print('pong')", timeout=30, first_byte_timeout=25,
                            status_busy_timeout=None)
                    if result.get("status") != "ok":
                        print(f"[cluster] Health ping failed ({result.get('status')}), recycling...")
                        needs_recycle = True
                except Exception as e:
                    print(f"[cluster] Ping error: {e}, recycling...")
                    needs_recycle = True
            else:
                print(f"[cluster] Session dead, recycling...")
                needs_recycle = True

            # Scheduled 5-min recycle — only when WS is unhealthy.
            # If the ping just passed, the WS is alive; forcibly disconnecting a healthy
            # WS connection and reconnecting risks a 500 from AIDP and wastes kernel state.
            # Only recycle when the ping failed (needs_recycle=True above) or when the WS
            # is already dead. On a healthy WS, just reset the timer.
            if not needs_recycle and (now - self._last_recycle) >= RECYCLE_INTERVAL:
                if self._session and not self._session._ws_alive:
                    print(f"[cluster] Scheduled 5-min recycle (WS dead)...")
                    needs_recycle = True
                else:
                    # WS is alive and ping passed — skip the reconnect, just reset timer
                    self._last_recycle = time.time()
                    print(f"[cluster] Skipping scheduled recycle — WS alive after ping")

            if needs_recycle:
                await self._do_recycle_async()

    async def _do_recycle_async(self):
        """Reconnect WS to existing session (preserves kernel state), or create fresh session.

        After reconnecting, runs a quick execution test to verify the kernel is actually
        free to accept new requests. A kernel can be WS-alive but execution-hung (e.g.
        it's still processing a previous long-running request). In that case, deletes the
        session and creates a completely new one.
        """
        async with self._lock:
            self._recycle_count += 1
            sid = self._session.session_id if self._session else "none"
            print(f"[cluster] Reconnecting WS #{self._recycle_count} (session {sid})...")

            # Step 1: Try to reconnect WebSocket to the same kernel.
            # Retry up to 6 times with exponential backoff — AIDP occasionally returns
            # HTTP 500 transiently (server overload) but recovers quickly. Creating a
            # fresh session loses ALL kernel state, so we retry aggressively first.
            ws_reconnected = False
            for attempt in range(6):
                try:
                    await self._session.reconnect_ws()
                    self._last_recycle = time.time()
                    print(f"[cluster] WS reconnected! Session: {self._session.session_id}")
                    ws_reconnected = True
                    break
                except Exception as e:
                    wait = min(5 * (2 ** attempt), 60)  # 5, 10, 20, 40, 60, 60
                    print(f"[cluster] WS reconnect attempt {attempt + 1} failed: {e}. "
                          f"Retrying in {wait}s...")
                    await asyncio.sleep(wait)

            if not ws_reconnected:
                # WS reconnect failed entirely after all retries — kernel truly dead
                await self._do_fresh_session("WS reconnect failed after 6 attempts")
                return

            # Step 2: Verify kernel is free to execute — not just WS-alive.
            # A kernel responds to kernel_info_request (WS-level) even while busy
            # executing a previous request, so reconnect_ws() can "succeed" while
            # the kernel is actually hung. Run a real execute to confirm it's free.
            print(f"[cluster] Verifying kernel can execute...")
            test = await self._session._execute_locked(
                "print('alive')", timeout=20, first_byte_timeout=15)
            if test.get("status") == "ok":
                print(f"[cluster] Kernel execution verified.")
                return

            # Kernel is WS-alive but execution-hung — delete it and start fresh
            ename = ""
            for o in test.get("outputs", []):
                ename = o.get("ename", "")
                break
            print(f"[cluster] Kernel hung after reconnect ({ename}) — creating fresh session...")
            await self._do_fresh_session("kernel execution-hung after WS reconnect")

    async def _do_fresh_session(self, reason: str = ""):
        """Delete the current session and create a brand-new kernel.

        IMPORTANT: Caller must hold self._lock.
        All kernel state (variables, imports, DataFrames) is lost.
        """
        old_sid = self._session.session_id if self._session else "none"
        print(f"[cluster] Fresh session (reason: {reason}, deleting {old_sid})...")
        try:
            await self._session.close()
        except Exception as e:
            print(f"[cluster] Error closing old session: {e}")

        # Explicitly delete the dead session so list-sessions cannot return it and
        # cause connect() to reuse a dead kernel on the next fresh session attempt.
        if self._session and self._session.session_id and self._session.signer:
            try:
                import requests as _requests
                delete_url = (
                    f"{self._session.aidp_endpoint}/20240831/dataLakes/{self._session.lake_ocid}"
                    f"/workspaces/{self._session.workspace_id}/notebook/api/sessions"
                    f"/{self._session.session_id}"
                )
                _requests.delete(delete_url, auth=self._session.signer)
                print(f"[cluster] Dead session {old_sid} deleted from server.")
            except Exception as e:
                print(f"[cluster] Could not delete dead session {old_sid}: {e}")

        for attempt in range(3):
            try:
                self._session = AIDPSession(cluster_id=self._cluster_id,
                                            **self._kwargs)
                await self._session.connect()
                self._last_recycle = time.time()
                print(f"[cluster] Fresh session ready: {self._session.session_id}")

                # Replay bootstrap snippets to restore kernel state
                bootstrap = getattr(self, '_bootstrap_snippets', [])
                if bootstrap:
                    print(f"[cluster] Replaying {len(bootstrap)} bootstrap snippet(s)...")
                    for i, snippet in enumerate(bootstrap):
                        try:
                            result = await self._session._execute_locked(
                                snippet, timeout=60, first_byte_timeout=30)
                            if result.get("status") != "ok":
                                print(f"[cluster] Bootstrap snippet {i+1} failed: {result.get('status')}")
                            else:
                                print(f"[cluster] Bootstrap snippet {i+1} OK")
                        except Exception as be:
                            print(f"[cluster] Bootstrap snippet {i+1} error: {be}")
                    print(f"[cluster] Bootstrap replay complete")
                return
            except Exception as e:
                print(f"[cluster] Fresh session attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(5)
        print(f"[cluster] All fresh session attempts failed — cluster may be unavailable")

    async def force_reconnect(self):
        """Immediately reconnect the WebSocket transport on demand.

        Call this when cell output contains "Compute cluster ... is not running".
        Reconnecting the WS to the existing kernel may trigger AIDP to resume the
        underlying Dataflow compute cluster. If reconnect fails (cluster truly dead),
        falls through to _do_fresh_session which creates a new kernel.

        Safe to call from outside the lock — _do_recycle_async acquires the lock.
        """
        print("[cluster] force_reconnect() called — triggering WS reconnect...")
        await self._do_recycle_async()

    @property
    def session_id(self):
        return self._session.session_id if self._session else None

    @property
    def cluster_id(self):
        return self._cluster_id


# Global singleton
cluster = ClusterSession()