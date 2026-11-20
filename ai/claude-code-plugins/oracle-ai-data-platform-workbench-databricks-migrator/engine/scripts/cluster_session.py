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

    await cluster.connect(cluster_id="ca4a71e0-...")

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
from bootstrap_execution import execute_bootstrap_snippets


# WebSocket recycle cadence (seconds). Overwritten in job_migrate.main() from
# cfg.runtime.ws_recycle_interval_s. Kept as a module attribute (not a local in
# the recycle loop) so it's a single, discoverable source of truth.
RECYCLE_INTERVAL = 300
RECONNECT_DEADLINE = 360  # includes all six handshakes and their backoffs


# ─── Stateless Pool ─────────────────────────────────────────────────────


class StatelessPool:
    """Pool of independent WebSocket sessions for concurrent stateless I/O.

    Each session has its own Python namespace — no shared kernel state.
    Operations: file exists/read/write, path checks, catalog queries.
    """

    def __init__(self, cluster_id: str, size: int = 3, session_prefix: str = "aidp_pool",
                 session_kwargs: Optional[dict] = None):
        self._cluster_id = cluster_id
        self._size = size
        self._session_prefix = session_prefix
        self._session_kwargs = session_kwargs or {}
        self._queue: Optional[asyncio.Queue] = None

    async def start(self):
        self._queue = asyncio.Queue()
        for i in range(self._size):
            # Each pool session uses a unique path so AIDP creates independent kernels
            s = AIDPSession(cluster_id=self._cluster_id,
                            session_name=f"{self._session_prefix}_session_{i}",
                            **self._session_kwargs)
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
                session = AIDPSession(cluster_id=self._cluster_id, session_name=old_name,
                                      **self._session_kwargs)
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
        self._recovery_task: Optional[asyncio.Task] = None
        self._interrupted_msg_id: Optional[str] = None
        self._recovery_unresolved = False
        self._pool: Optional[StatelessPool] = None
        self._running = False
        self._recycle_count = 0
        self._last_recycle = 0
        self._lock_acquired_at: float = 0   # timestamp when execute() acquired the lock (0 = free)
        self._kwargs: dict = {}              # saved kwargs for fresh session creation
        self._readiness_clusters: set[str] = set()
        # Counter incremented every time _do_fresh_session runs — i.e. every time
        # kernel state is destroyed and a brand-new session is created. Task
        # processors capture this counter at task start and check at task end;
        # a delta > 0 means state was lost mid-task and the migrated output for
        # this task should not be trusted (rerun the task manually).
        self._state_loss_count = 0
        self._initialized = True

    def register_bootstrap(self, code: str):
        """Register a bootstrap code snippet to replay after fresh session creation.
        Call this right after each bootstrap execute() so fresh sessions get the same setup."""
        if not hasattr(self, '_bootstrap_snippets'):
            self._bootstrap_snippets = []
        self._bootstrap_snippets.append(code)

    def mark_cluster_ready(self, cluster_id: str):
        """Record a cluster that passed the complete gate in this migration."""
        self._readiness_clusters.add(cluster_id)

    async def connect(self, cluster_id: str, **kwargs):
        """Initialize the session and pool. Call once at startup."""
        self._cluster_id = cluster_id
        self._recovery_unresolved = False
        self._kwargs = kwargs               # save for fresh session recreation
        self._bootstrap_snippets = []       # reset bootstrap on new connection
        self._session = AIDPSession(cluster_id=cluster_id, **kwargs)
        await self._session.connect()
        self._running = True
        self._last_recycle = time.time()

        # Start stateless pool. NOTE: class default is size=3 (see StatelessPool.__init__),
        # but we intentionally use size=1 here to reduce concurrent WS load on the cluster —
        # a single pool session serializes file I/O + path checks alongside the main
        # kernel session. Bump only after validating cluster stability under higher concurrency.
        pool_prefix = kwargs.get("session_name", "aidp_pool").replace("aidp_mig_", "aidp_pool_")
        pool_kwargs = {k: v for k, v in kwargs.items() if k != "session_name"}
        self._pool = StatelessPool(cluster_id=cluster_id, size=1,
                                   session_prefix=pool_prefix,
                                   session_kwargs=pool_kwargs)
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
                if self._interrupted_msg_id:
                    return self._recovery_error(
                        "RecoveryUnresolved", "Previous cell is awaiting its execution result")
                if self._recovery_unresolved:
                    return self._recovery_error(
                        "RecoveryUnresolved", "Previous cell outcome is still unknown")
                if not self._session or not self._session._ws_alive:
                    return {"status": "error", "outputs": [{"type": "error",
                             "ename": "SessionDead",
                             "evalue": "Session not connected. Recycle should restore it."}]}
                session = self._session
                session_id, kernel_id = session.session_id, session.kernel_id
                try:
                    result = await session._execute_locked(
                        code, timeout, first_byte_timeout=None,
                        preserve_on_disconnect=True)
                except asyncio.CancelledError:
                    # _execute_locked first yields only after sending the
                    # request. Cancellation here leaves its outcome unknown.
                    self._recovery_unresolved = True
                    raise
            finally:
                self._lock_acquired_at = 0

        msg_id = result.get("_interrupted_msg_id")
        if not msg_id:
            return result
        # Keep the original request reference even if reconnect cleanup
        # changes its tracking entry after the poll loop observes the drop.
        pending = result.get("_interrupted_pending") or session._pending.get(msg_id)
        if pending is None:
            self._recovery_unresolved = True
            return self._recovery_error("RecoveryUnresolved", "Original execution request was lost")
        # The old kernel may still be executing. Keep the original request and
        # wait for the same background recovery task instead of replaying code.
        self._interrupted_msg_id = msg_id
        try:
            recovery = self._request_recovery()
            try:
                outcome = await asyncio.wait_for(recovery, timeout=RECONNECT_DEADLINE)
            except asyncio.TimeoutError:
                outcome = "reconnect_timeout"
            except Exception as exc:
                outcome = f"reconnect_error: {type(exc).__name__}: {exc}"

            if (self._session is not session or session.session_id != session_id
                    or session.kernel_id != kernel_id):
                self._recovery_unresolved = True
                return self._recovery_error("KernelLost", "Kernel replaced during WebSocket recovery")
            if outcome != "same_kernel":
                self._recovery_unresolved = True
                return self._recovery_error("RecoveryUnresolved", str(outcome))

            event = pending["event"]
            deadline = result["_execution_deadline"]
            while not event.is_set() and time.time() < deadline:
                await asyncio.sleep(min(0.1, max(0, deadline - time.time())))
                if self._session is not session or session.kernel_id != kernel_id:
                    self._recovery_unresolved = True
                    return self._recovery_error("KernelLost", "Kernel replaced before execution completed")
            if event.is_set():
                recovered = pending["result"].get("result")
                if recovered:
                    # A real reply from the original kernel can arrive during
                    # reconnect, after the original cell deadline. Accept it;
                    # only the post-reconnect reply wait uses that deadline.
                    return recovered
                self._recovery_unresolved = True
                return self._recovery_error("RecoveryUnresolved", "Original execution reply had no result")
            self._recovery_unresolved = True
            return self._recovery_error("RecoveryUnresolved", "Original execution deadline expired without a reply")
        except asyncio.CancelledError:
            # The caller stopped waiting, but the original request may still
            # execute. Keep the session and block every subsequent request.
            self._recovery_unresolved = True
            raise
        finally:
            session._pending.pop(msg_id, None)
            session._outputs.pop(msg_id, None)
            self._interrupted_msg_id = None

    @staticmethod
    def _recovery_error(name: str, detail: str) -> dict:
        return {"status": "error", "outputs": [{"type": "error", "ename": name, "evalue": detail}]}

    def _request_recovery(self) -> asyncio.Task:
        if self._recovery_task is None or self._recovery_task.done():
            self._recovery_task = asyncio.create_task(self._do_recycle_async())
        return self._recovery_task

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
        recycle_task = self._recycle_task
        self._recycle_task = None
        if recycle_task:
            if not recycle_task.done():
                recycle_task.cancel()
            # Cancellation is the normal way to stop the keepalive loop.  Use
            # gather(return_exceptions=True) so the expected CancelledError is
            # consumed instead of becoming the exception context for a later
            # migration failure raised by a notebook-hosted runner.
            await asyncio.gather(recycle_task, return_exceptions=True)
        recovery_task = self._recovery_task
        self._recovery_task = None
        if recovery_task and not recovery_task.done():
            recovery_task.cancel()
            await asyncio.gather(recovery_task, return_exceptions=True)
        if self._pool:
            await self._pool.stop()
            self._pool = None
        if self._session:
            if self._recovery_unresolved:
                print(f"[cluster] Retaining unresolved notebook session {self._session.session_id}; "
                      "no REST DELETE sent")
            await self._session.close(delete_session=not self._recovery_unresolved)
            self._session = None
        self._cluster_id = None

    # ─── Cluster Switching ─────────────────────────────────────────────

    async def switch_cluster(self, new_cluster_id: str, session_name: str = None):
        """Switch to a different AIDP cluster.

        Closes the current session/pool, runs the complete migration-readiness
        gate once per target cluster, connects, and replays bootstrap snippets.

        No-op if already connected to the same cluster.
        """
        if new_cluster_id == self._cluster_id:
            return

        # Save bootstrap before close() — connect() resets _bootstrap_snippets
        old_bootstrap = list(getattr(self, '_bootstrap_snippets', []))

        # Tear down current connection
        await self.close()

        if new_cluster_id not in self._readiness_clusters:
            import cluster_lifecycle
            from phase1_cluster_readiness import prepare_cluster

            await prepare_cluster(cluster_lifecycle, new_cluster_id)
            self.mark_cluster_ready(new_cluster_id)
        else:
            import cluster_lifecycle

            print(
                f"[cluster] Cluster {new_cluster_id[:12]}... already passed "
                "library readiness in this migration; rechecking that it is "
                "running before reconnecting."
            )
            # The cache covers dependency installation only. A previously ready
            # cluster can idle-stop while another task cluster is in use.
            await cluster_lifecycle.ensure_cluster_running(new_cluster_id)

        # Connect to new cluster
        kwargs = dict(self._kwargs)
        if session_name:
            kwargs["session_name"] = session_name
        await self.connect(cluster_id=new_cluster_id, **kwargs)

        # Replay bootstrap snippets (aidp_compat import, output dir, etc.)
        if old_bootstrap:
            print(f"[cluster] Replaying {len(old_bootstrap)} bootstrap snippet(s) on new cluster...")
            # PR90 review: switching must fail before migration proceeds if
            # setup fails; failed statements must not enter the replay list.
            try:
                await execute_bootstrap_snippets(self, old_bootstrap)
            except Exception:
                # Review 5631346397: a later job must not take the same-cluster
                # no-op on this half-bootstrapped kernel. Dispose it, retaining
                # the ORIGINAL replay plan (not just the successful prefix) for
                # a subsequent explicitly requested connection. No auto retry.
                try:
                    await self.close()
                finally:
                    self._cluster_id = None
                    self._bootstrap_snippets = old_bootstrap
                raise
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
        # RECYCLE_INTERVAL is a module-global (see top of file) so job_migrate.main()
        # can override it from cfg.runtime.ws_recycle_interval_s. Other intervals stay
        # local — they're internal invariants, not customer-tunable.
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
            if self._interrupted_msg_id or self._recovery_unresolved:
                # The foreground owns this in-flight request and awaits the
                # shared recovery. A health probe could queue behind the cell
                # and incorrectly replace its still-live kernel.
                continue
            if self._session and self._session._ws_alive:
                try:
                    async with self._lock:
                        # A ping can have queued behind the foreground before
                        # cancellation latched its unknown outcome.
                        if self._interrupted_msg_id or self._recovery_unresolved:
                            continue
                        result = await self._session._execute_locked(
                            "print('pong')", timeout=30, first_byte_timeout=25,
                            status_busy_timeout=None)
                    if result.get("status") != "ok":
                        # Smart-skip: if iopub was recently active (a long cell
                        # producing output), the kernel is legit busy and the ping
                        # queued behind it — recycling here would destroy state.
                        # Only recycle if iopub has been silent AND ping failed.
                        _since_recv = time.time() - self._session._last_recv_time
                        if self._session._ws_alive and _since_recv < 60:
                            print(f"[cluster] Health ping timed out but iopub fresh "
                                  f"({_since_recv:.0f}s since last message) — kernel "
                                  f"likely busy with a long cell, skipping recycle")
                        else:
                            print(f"[cluster] Health ping failed ({result.get('status')}), "
                                  f"iopub stale {_since_recv:.0f}s — recycling...")
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
                try:
                    await self._request_recovery()
                except asyncio.CancelledError:
                    if not self._running:
                        raise
                except Exception as exc:
                    print(f"[cluster] Recovery failed: {type(exc).__name__}: {exc}")

    async def _do_recycle_async(self):
        """Reconnect WS to existing session (preserves kernel state), or create fresh session.

        After reconnecting, runs a quick execution test to verify the kernel is actually
        free to accept new requests. A kernel can be WS-alive but execution-hung (e.g.
        it's still processing a previous long-running request). In that case, deletes the
        session and creates a completely new one.
        """
        async with self._lock:
            # A recovery task may have queued behind the cancelled foreground.
            # Never replace or delete a kernel with an unconfirmed cell.
            if self._recovery_unresolved:
                return "outcome_unresolved"
            self._recycle_count += 1
            sid = self._session.session_id if self._session else "none"
            print(f"[cluster] Reconnecting WS #{self._recycle_count} (session {sid})...")
            preserve_msg_id = self._interrupted_msg_id

            # Step 1: Try to reconnect WebSocket to the same kernel.
            # Retry up to 6 times with exponential backoff — AIDP occasionally returns
            # HTTP 500 transiently (server overload) but recovers quickly. Creating a
            # fresh session loses ALL kernel state, so we retry aggressively first.
            ws_reconnected = False
            for attempt in range(6):
                try:
                    if preserve_msg_id:
                        await self._session.reconnect_ws(preserve_msg_id=preserve_msg_id)
                    else:
                        await self._session.reconnect_ws()
                    self._last_recycle = time.time()
                    print(f"[cluster] WS reconnected! Session: {self._session.session_id}")
                    ws_reconnected = True
                    break
                except Exception as e:
                    if attempt == 5:
                        break
                    wait = min(5 * (2 ** attempt), 60)  # 5, 10, 20, 40, 60, 60
                    print(f"[cluster] WS reconnect attempt {attempt + 1} failed: {e}. "
                          f"Retrying in {wait}s...")
                    await asyncio.sleep(wait)

            if not ws_reconnected:
                if preserve_msg_id:
                    # A failed handshake does not prove the old kernel died.
                    # Never delete it while its cell may still be running.
                    return "reconnect_failed"
                # WS reconnect failed entirely after all retries — kernel truly dead
                await self._do_fresh_session("WS reconnect failed after 6 attempts")
                return "new_kernel"

            if preserve_msg_id:
                # The foreground retains the original msg_id and deadline. A
                # probe here could queue behind that cell and time out falsely.
                return "same_kernel"

            # Step 2: Wait for the kernel to drain any pre-drop in-flight cell.
            #
            # After WS reconnect to the SAME kernel_id, a legitimately busy cell
            # (Spark shuffle, long compute) may still be running in-kernel; its
            # iopub messages replay/stream on the new WS. Submitting a verification
            # cell now queues it behind the busy work and hits false-positive
            # timeout — the OLD code did exactly that and killed sessions with
            # valid globals (v12h scenario).
            #
            # Detect drain via _last_recv_time: if no WS message arrives for
            # QUIET_WINDOW seconds, the kernel is quiet. Wait up to TOTAL_WAIT
            # for this (covers legitimate long cells finishing after reconnect).
            #
            # Active foreground cells take the preserve_msg_id path above and
            # own their outcome. This drain/verification path is for recovery
            # without an outstanding foreground execution.
            print(f"[cluster] Waiting for kernel to drain any pre-drop cell...")
            QUIET_WINDOW = 10        # seconds of no WS traffic ⇒ drained
            TOTAL_WAIT = 1800        # 30 min patience
            POLL_INTERVAL = 5
            wait_start = time.time()
            drained = False
            while (time.time() - wait_start) < TOTAL_WAIT:
                since_last_recv = time.time() - self._session._last_recv_time
                if since_last_recv >= QUIET_WINDOW:
                    print(f"[cluster] Kernel drained (quiet {since_last_recv:.0f}s, "
                          f"total wait {time.time() - wait_start:.0f}s).")
                    drained = True
                    break
                await asyncio.sleep(POLL_INTERVAL)
            if not drained:
                print(f"[cluster] Kernel still producing after {TOTAL_WAIT}s — "
                      f"proceeding with verification anyway.")

            # Step 3: Verify kernel is free to execute — not just WS-alive.
            # A kernel responds to kernel_info_request (WS-level) even while busy
            # executing a previous request, so reconnect_ws() can "succeed" while
            # the kernel is actually hung. Run a real execute to confirm it's free.
            # Timeouts raised from 20s/15s to 120s/60s — the drain-wait above should
            # have made this fast, but give headroom for a Spark job still winding
            # down after the drain window closes.
            print(f"[cluster] Verifying kernel can execute...")
            test = await self._session._execute_locked(
                "print('alive')", timeout=120, first_byte_timeout=60)
            if test.get("status") == "ok":
                print(f"[cluster] Kernel execution verified.")
                return "same_kernel"

            # Kernel is WS-alive but execution-hung — delete it and start fresh
            ename = ""
            for o in test.get("outputs", []):
                ename = o.get("ename", "")
                break
            print(f"[cluster] Kernel hung after reconnect ({ename}) — creating fresh session...")
            await self._do_fresh_session("kernel execution-hung after WS reconnect")
            return "new_kernel"

    async def _do_fresh_session(self, reason: str = ""):
        """Delete the current session and create a brand-new kernel.

        IMPORTANT: Caller must hold self._lock.
        All kernel state (variables, imports, DataFrames) is lost.
        Increments _state_loss_count so the task processor can detect that
        a mid-task state loss happened and annotate the task report accordingly.
        """
        self._state_loss_count += 1
        old_sid = self._session.session_id if self._session else "none"
        print(f"[cluster] Fresh session (reason: {reason}, deleting {old_sid}) "
              f"[state_loss_count={self._state_loss_count}]...")
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
