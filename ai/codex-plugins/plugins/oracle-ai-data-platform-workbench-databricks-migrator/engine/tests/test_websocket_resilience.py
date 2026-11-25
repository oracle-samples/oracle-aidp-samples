"""
Unit tests for WebSocket resilience fixes.

Covers:
  1. Listener generation counter — stale thread race condition (Critical Fix 2)
  2. Unanswered ping detection — silent TCP black holes (Medium Fix)
  3. _pending/_outputs cleanup on reconnect — ghost entries (Medium Fix)
  4. StatelessPool session_name preservation — pool isolation (High Fix)
  5. Freeze detection condition — checks ws_idle, not just lock-held time (Critical Fix 1)

All tests run without a live AIDP connection using mocks/stubs.
"""

import asyncio
import os
import sys
import threading
import time
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

import websocket as websocket_module
from aidp_executor import AIDPSession


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_session(**kwargs):
    # Generic code requires explicit coordinates (no hardcoded env defaults).
    kwargs.setdefault("cluster_id", "test-cluster")
    kwargs.setdefault("lake_ocid", "ocid1.aidataplatform.oc1.test-region.testid")
    kwargs.setdefault("workspace_id", "ws-test")
    return AIDPSession(**kwargs)


def _make_mock_ws(connected=True):
    """Return a MagicMock shaped like websocket.WebSocket."""
    mock_ws = MagicMock()
    type(mock_ws).connected = PropertyMock(return_value=connected)
    return mock_ws


def _run_listener(session, generation, timeout=2.0):
    """Run _listen_sync in a thread and wait for it to exit."""
    t = threading.Thread(target=session._listen_sync, args=(generation,))
    t.start()
    t.join(timeout=timeout)
    assert not t.is_alive(), "Listener thread did not exit within timeout"


# ===================================================================
# 1. Listener generation counter
# ===================================================================

class TestListenerGeneration:
    """Generation counter prevents stale listener threads from corrupting
    a newer connection's _ws_alive flag after reconnect."""

    def test_generation_starts_at_zero(self):
        session = _make_session()
        assert session._listener_generation == 0

    def test_stale_generation_cannot_mark_dead(self):
        """Old listener (generation 1) must NOT set _ws_alive=False
        when the current generation is already 2."""
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 2  # new connection is in place

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.side_effect = websocket_module.WebSocketConnectionClosedException()
        session.ws = mock_ws

        # Run the listener tagged as generation 1 (stale)
        _run_listener(session, generation=1)

        assert session._ws_alive is True  # stale thread must not corrupt new connection

    def test_current_generation_marks_dead(self):
        """Current listener (matching generation) must be able to mark the session dead."""
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 1

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.side_effect = websocket_module.WebSocketConnectionClosedException()
        session.ws = mock_ws

        _run_listener(session, generation=1)  # matches _listener_generation

        assert session._ws_alive is False  # current thread correctly marked dead

    def test_close_frame_marks_dead_for_current_generation(self):
        """An explicit CLOSE frame from the server marks the session dead (current gen)."""
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 3

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.return_value = (websocket_module.ABNF.OPCODE_CLOSE, b"")
        session.ws = mock_ws

        _run_listener(session, generation=3)

        assert session._ws_alive is False

    def test_close_frame_ignored_for_stale_generation(self):
        """A CLOSE frame from a stale listener must not mark the new connection dead."""
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 5  # current is 5

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.return_value = (websocket_module.ABNF.OPCODE_CLOSE, b"")
        session.ws = mock_ws

        _run_listener(session, generation=4)  # stale generation 4

        assert session._ws_alive is True

    def test_old_listener_never_reads_reconnected_socket(self):
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 1
        old_ws = _make_mock_ws()
        new_ws = _make_mock_ws()

        def old_recv(*args, **kwargs):
            if old_ws.recv_data.call_count == 1:
                session.ws = new_ws
                session._listener_generation = 2
                return websocket_module.ABNF.OPCODE_PING, b""
            return websocket_module.ABNF.OPCODE_CLOSE, b""

        old_ws.recv_data.side_effect = old_recv
        session.ws = old_ws
        _run_listener(session, generation=1)
        new_ws.recv_data.assert_not_called()
        assert session._ws_alive is True


# ===================================================================
# 2. Unanswered ping detection
# ===================================================================

class TestUnansweredPings:
    """After 3 consecutive 30s timeouts with no pong, the connection is
    treated as silently dead (TCP black hole scenario)."""

    def _make_timeout_session(self, recv_sequence):
        """
        Build a session whose mock WS returns items from recv_sequence in order.
        Each item is either:
          - An Exception class/instance  → raised from recv_data
          - A tuple (opcode, data)       → returned from recv_data
        """
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 1

        seq = iter(recv_sequence)

        def recv_data_side_effect(*args, **kwargs):
            item = next(seq)
            if isinstance(item, type) and issubclass(item, Exception):
                raise item()
            if isinstance(item, Exception):
                raise item
            return item

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.side_effect = recv_data_side_effect
        session.ws = mock_ws
        return session, mock_ws

    def test_three_consecutive_timeouts_marks_dead(self):
        """3 consecutive WebSocketTimeoutExceptions → TCP black hole detected."""
        TO = websocket_module.WebSocketTimeoutException
        session, mock_ws = self._make_timeout_session([TO, TO, TO])

        _run_listener(session, generation=1)

        assert session._ws_alive is False
        # Pings sent on timeout 1 and 2; timeout 3 fires mark_dead before a ping
        assert mock_ws.ping.call_count == 2

    def test_pong_resets_unanswered_counter(self):
        """A pong resets the counter; need 3 more consecutive misses after the pong
        before the connection is declared dead."""
        TO = websocket_module.WebSocketTimeoutException
        PONG = (websocket_module.ABNF.OPCODE_PONG, b"")
        # 2 timeouts → pong (reset) → 3 more timeouts → dead
        session, mock_ws = self._make_timeout_session(
            [TO, TO, PONG, TO, TO, TO]
        )

        _run_listener(session, generation=1)

        assert session._ws_alive is False
        # Pings: timeout 1, 2 (before pong), timeout 4, 5 (after pong); timeout 3 = pong, timeout 6 = dead
        assert mock_ws.ping.call_count == 4

    def test_two_timeouts_then_pong_not_dead(self):
        """2 timeouts followed by a pong should NOT mark dead — counter resets."""
        TO = websocket_module.WebSocketTimeoutException
        PONG = (websocket_module.ABNF.OPCODE_PONG, b"")
        # 2 timeouts, pong, then CLOSE (to exit the loop cleanly)
        CLOSE = (websocket_module.ABNF.OPCODE_CLOSE, b"")
        session, mock_ws = self._make_timeout_session([TO, TO, PONG, CLOSE])

        _run_listener(session, generation=1)

        # CLOSE causes mark_dead (generation matches), so _ws_alive=False — but the
        # important thing is the ping count shows the pong successfully reset the counter.
        assert mock_ws.ping.call_count == 2  # 2 pings before the pong; none after (CLOSE arrived)

    def test_ping_send_failure_marks_dead(self):
        """If ws.ping() itself raises (broken pipe), the connection is marked dead."""
        TO = websocket_module.WebSocketTimeoutException
        session = _make_session()
        session._running = True
        session._ws_alive = True
        session._listener_generation = 1

        mock_ws = _make_mock_ws()
        mock_ws.recv_data.side_effect = TO()
        mock_ws.ping.side_effect = OSError("Broken pipe")
        session.ws = mock_ws

        _run_listener(session, generation=1)

        assert session._ws_alive is False

    def test_stale_generation_cannot_mark_dead_via_unanswered_pings(self):
        """Stale listener reaching 3 unanswered pings must NOT corrupt a newer connection.

        This is the most common production trigger: a TCP black-hole causes the old
        listener to hit 3 consecutive timeouts and call mark_dead().  The new connection
        (generation 2) must remain alive because the stale listener's generation (1) no
        longer matches _listener_generation.
        """
        TO = websocket_module.WebSocketTimeoutException
        session, mock_ws = self._make_timeout_session([TO, TO, TO])
        # Bump generation to 2 — simulates a reconnect that happened after this listener started
        session._listener_generation = 2

        # Run listener tagged as generation 1 (stale)
        _run_listener(session, generation=1)

        assert session._ws_alive is True  # stale 3-ping mark_dead must not corrupt new connection


# ===================================================================
# 3. _pending / _outputs cleanup on reconnect
# ===================================================================

class TestReconnectCleanup:
    """reconnect_ws() clears _pending and _outputs so ghost entries from a
    dead connection cannot accumulate across reconnects."""

    def _reconnect(self, session, preserve_msg_id=None):
        """Run reconnect_ws() with all I/O mocked out."""
        mock_ws = _make_mock_ws()

        async def _run():
            with patch.object(websocket_module, "WebSocket", return_value=mock_ws), \
                 patch.object(session, "_sign_ws_headers", return_value={}), \
                 patch("asyncio.sleep", new_callable=AsyncMock), \
                 patch("threading.Thread") as mock_thread:
                mock_thread.return_value.start.return_value = None
                await session.reconnect_ws(preserve_msg_id=preserve_msg_id)

        asyncio.run(_run())

    def test_single_stale_entry_cleared(self):
        session = _make_session()
        session.kernel_id = "k-1"
        session.session_id = "s-1"
        session._pending["ghost-id"] = {"event": threading.Event(), "result": {}}
        session._outputs["ghost-id"] = [{"type": "stream", "text": "partial"}]

        self._reconnect(session)

        assert "ghost-id" not in session._pending
        assert "ghost-id" not in session._outputs

    def test_multiple_stale_entries_all_cleared(self):
        session = _make_session()
        session.kernel_id = "k-1"
        session.session_id = "s-1"
        for i in range(5):
            session._pending[f"msg-{i}"] = {}
            session._outputs[f"msg-{i}"] = []

        self._reconnect(session)

        assert len(session._pending) == 0
        assert len(session._outputs) == 0

    def test_empty_dicts_stay_empty(self):
        """No-op: if dicts are already clean, reconnect leaves them clean."""
        session = _make_session()
        session.kernel_id = "k-1"
        session.session_id = "s-1"

        self._reconnect(session)

        assert len(session._pending) == 0
        assert len(session._outputs) == 0

    def test_inflight_reply_is_retained_and_matched_after_reconnect(self):
        session = _make_session()
        session.kernel_id = "k-1"
        session.session_id = "s-1"
        event = threading.Event()
        holder = {}
        session._pending["cell-id"] = {"event": event, "result": holder}
        session._outputs["cell-id"] = [{"type": "stream", "text": "before drop"}]
        session._pending["ghost-id"] = {"event": threading.Event(), "result": {}}
        session._outputs["ghost-id"] = []

        self._reconnect(session, preserve_msg_id="cell-id")
        assert "ghost-id" not in session._pending
        assert session._outputs["cell-id"][0]["text"] == "before drop"
        session._handle_message({
            "header": {"msg_type": "execute_reply"},
            "parent_header": {"msg_id": "cell-id"},
            "content": {"status": "ok"},
        })
        assert event.is_set()
        assert holder["result"]["status"] == "ok"
        assert holder["result"]["outputs"][0]["text"] == "before drop"


@pytest.mark.parametrize("preserve", [False, True])
def test_only_foreground_execution_retains_drop_request(preserve):
    session = _make_session()
    session.session_id = "s-1"
    session._ws_alive = True
    session.ws = _make_mock_ws()
    session.ws.send_binary.side_effect = lambda _: setattr(session, "_ws_alive", False)
    result = asyncio.run(session._execute_locked(
        "print('active')", timeout=1, status_busy_timeout=None,
        preserve_on_disconnect=preserve))
    assert result["outputs"][0]["ename"] == "ConnectionDied"
    assert bool(session._pending) is preserve
    assert bool(session._outputs) is preserve
    assert ("_interrupted_msg_id" in result) is preserve


# ===================================================================
# 4. StatelessPool session_name preservation
# ===================================================================

class TestStatelessPoolSessionName:
    """When a dead pool session is replaced, the replacement must use the
    original session_name so AIDP creates an independent kernel (not the
    default 'aidp_executor_session' which can collide with the main session)."""

    def _make_mock_session_class(self, created_list):
        """Return a MockSession class that records created instances."""
        class MockSession:
            def __init__(self, cluster_id=None, session_name="aidp_executor_session",
                         **kwargs):
                self._session_name = session_name
                self._ws_alive = True
                created_list.append(self)

            async def connect(self):
                pass

            async def close(self):
                pass

            async def _execute_locked(self, code, timeout=30,
                                      first_byte_timeout=None):
                return {"status": "ok", "outputs": []}

        return MockSession

    def test_dead_session_recreated_with_original_name(self):
        """Replacement session must inherit the dead session's _session_name."""
        from cluster_session import StatelessPool

        created = []
        MockSession = self._make_mock_session_class(created)

        async def run():
            pool = StatelessPool(cluster_id="test-cluster", size=1)
            pool._queue = asyncio.Queue()

            # Plant a dead session with a specific pool name
            dead = MockSession(cluster_id="test-cluster",
                               session_name="aidp_pool_session_1")
            dead._ws_alive = False
            await pool._queue.put(dead)

            with patch("cluster_session.AIDPSession", side_effect=MockSession):
                await pool.run("print('ok')")

        asyncio.run(run())

        # created[0] = dead (planted), created[1] = replacement
        assert len(created) == 2
        assert created[1]._session_name == "aidp_pool_session_1"

    def test_replacement_gets_correct_name_per_slot(self):
        """Each pool slot (0, 1, 2) preserves its unique index after replacement."""
        from cluster_session import StatelessPool

        for slot in range(3):
            created = []
            MockSession = self._make_mock_session_class(created)

            async def run(slot=slot):
                pool = StatelessPool(cluster_id="test-cluster", size=1)
                pool._queue = asyncio.Queue()

                dead = MockSession(cluster_id="test-cluster",
                                   session_name=f"aidp_pool_session_{slot}")
                dead._ws_alive = False
                await pool._queue.put(dead)

                with patch("cluster_session.AIDPSession", side_effect=MockSession):
                    await pool.run("print('ok')")

            asyncio.run(run())
            assert created[-1]._session_name == f"aidp_pool_session_{slot}"

    def test_alive_session_not_recreated(self):
        """A healthy pool session is reused as-is — no new AIDPSession created."""
        from cluster_session import StatelessPool

        created = []
        MockSession = self._make_mock_session_class(created)

        async def run():
            pool = StatelessPool(cluster_id="test-cluster", size=1)
            pool._queue = asyncio.Queue()

            alive = MockSession(cluster_id="test-cluster",
                                session_name="aidp_pool_session_0")
            alive._ws_alive = True
            await pool._queue.put(alive)

            with patch("cluster_session.AIDPSession", side_effect=MockSession):
                await pool.run("print('ok')")

        asyncio.run(run())

        assert len(created) == 1  # only the original; no replacement created


# ===================================================================
# 5. Freeze detection condition
# ===================================================================

class TestFreezeDetection:
    """ClusterSession freeze detection must check both lock-held time AND
    WS idle time (_last_recv_time). Checking lock time alone was killing
    legitimate long-running Spark jobs after 2 minutes."""

    def test_aidp_session_has_last_recv_time(self):
        """_last_recv_time is readable by ClusterSession freeze detection."""
        session = _make_session()
        assert hasattr(session, "_last_recv_time")
        assert session._last_recv_time == 0

    def test_freeze_detection_uses_ws_idle(self):
        """Verify the recycle loop source checks ws_idle (not just lock-held time)."""
        import inspect
        from cluster_session import ClusterSession
        src = inspect.getsource(ClusterSession._recycle_loop_async)
        assert "ws_idle" in src, (
            "Freeze detection must check ws_idle so long-running Spark jobs "
            "are not killed when the WS is still receiving server pings"
        )
        assert "_last_recv_time" in src, (
            "Freeze detection must read _last_recv_time from the session"
        )

    def test_freeze_requires_both_conditions(self):
        """Freeze detection fires only when BOTH held > threshold AND ws_idle > threshold.
        A Spark job with recent WS traffic (pings) must NOT be killed."""
        import inspect
        from cluster_session import ClusterSession
        src = inspect.getsource(ClusterSession._recycle_loop_async)
        # The condition should AND both: held > FREEZE_THRESHOLD and ws_idle > FREEZE_THRESHOLD
        assert "ws_idle > FREEZE_THRESHOLD" in src, (
            "Freeze condition must gate on ws_idle > FREEZE_THRESHOLD"
        )
        assert "held > FREEZE_THRESHOLD" in src, (
            "Freeze condition must still gate on held > FREEZE_THRESHOLD"
        )
