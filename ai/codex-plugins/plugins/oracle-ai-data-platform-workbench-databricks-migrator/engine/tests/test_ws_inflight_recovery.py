"""September 17 incident: an active cell loses its socket, then the session recovers.

Drive ClusterSession.execute through its real recovery coordinator. The fake
transport only supplies the socket events; no AIDP session is created.
"""

import asyncio
import threading
import time

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm
import cluster_session
from cluster_session import ClusterSession
from aidp_executor import AIDPSession


class DroppedCell:
    session_id = "session-1"
    kernel_id = "kernel-1"
    _ws_alive = True

    def __init__(self, *, reply=False, reply_before_return=False,
                 reconnect_error=False, deadline=1):
        self._pending = {}
        self._outputs = {}
        self.reply = reply
        self.reply_before_return = reply_before_return
        self.reconnect_error = reconnect_error
        self.deadline = deadline
        self.sent = 0
        self.reconnects = 0
        self.deleted = None

    async def _execute_locked(self, code, timeout, first_byte_timeout,
                              preserve_on_disconnect):
        assert preserve_on_disconnect
        self.sent += 1
        self._ws_alive = False
        self._pending["cell-id"] = {"event": threading.Event(), "result": {}}
        self._outputs["cell-id"] = [{"type": "stream", "text": "before drop"}]
        pending = self._pending["cell-id"]
        if self.reply_before_return:
            pending["result"]["result"] = {"status": "ok", "outputs": self._outputs["cell-id"]}
            pending["event"].set()
            self._pending.pop("cell-id")
        return {"status": "error", "outputs": [{"type": "error", "ename": "ConnectionDied"}],
                "_interrupted_msg_id": "cell-id", "_interrupted_pending": pending,
                "_execution_deadline": time.time() + self.deadline}

    async def reconnect_ws(self, preserve_msg_id=None):
        assert preserve_msg_id == "cell-id"
        self.reconnects += 1
        if self.reconnect_error:
            raise OSError("handshake failed")
        self._ws_alive = True
        if self.reply:
            pending = self._pending["cell-id"]
            pending["result"]["result"] = {"status": "ok", "outputs": self._outputs["cell-id"]}
            pending["event"].set()

    async def close(self, delete_session=True):
        self.deleted = delete_session


def _cluster(monkeypatch, session):
    monkeypatch.setattr(ClusterSession, "_instance", None)
    cluster = ClusterSession()
    cluster._session = session
    cluster._running = True
    return cluster


def test_same_kernel_reply_resumes_original_cell_without_replay(monkeypatch):
    session = DroppedCell(reply=True)
    cluster = _cluster(monkeypatch, session)
    result = asyncio.run(cluster.execute("print('cell 6')", timeout=100))
    assert result == {"status": "ok", "outputs": [{"type": "stream", "text": "before drop"}]}
    assert session.sent == 1
    assert session.reconnects == 1
    assert not cluster._recovery_unresolved


def test_reply_racing_with_drop_is_not_lost(monkeypatch):
    session = DroppedCell(reply_before_return=True)
    cluster = _cluster(monkeypatch, session)
    result = asyncio.run(cluster.execute("print('cell 6')"))
    assert result["status"] == "ok"
    assert session.sent == 1


def test_transport_retains_pending_reference_after_tracking_is_cleared():
    class Socket:
        connected = True

        def send_binary(self, message):
            pass

    session = object.__new__(AIDPSession)
    session.session_id = "session-1"
    session.ws = Socket()
    session._ws_alive = True
    session._pending = {}
    session._outputs = {}

    async def run():
        executing = asyncio.create_task(session._execute_locked(
            "print('cell 6')", 10, first_byte_timeout=None,
            preserve_on_disconnect=True))
        while not session._pending:
            await asyncio.sleep(0)
        pending = next(iter(session._pending.values()))
        # Reconnect cleanup can clear tracking before execute observes the
        # disconnected socket. Its original event/result must remain reachable.
        session._pending.clear()
        session._ws_alive = False
        result = await executing
        assert result["_interrupted_pending"] is pending

    asyncio.run(run())


def test_cancelling_foreground_during_recovery_latches_unknown_outcome(monkeypatch):
    session = DroppedCell()
    cluster = _cluster(monkeypatch, session)
    reconnect_started = asyncio.Event()
    finish_reconnect = asyncio.Event()

    async def delayed_reconnect(preserve_msg_id=None):
        reconnect_started.set()
        await finish_reconnect.wait()

    session.reconnect_ws = delayed_reconnect

    async def run():
        foreground = asyncio.create_task(cluster.execute("print('cell 6')"))
        await reconnect_started.wait()
        foreground.cancel()
        with pytest.raises(asyncio.CancelledError):
            await foreground
        blocked = await cluster.execute("print('cell 7')")
        assert blocked["outputs"][0]["ename"] == "RecoveryUnresolved"
        await cluster.close()
        finish_reconnect.set()

    asyncio.run(run())
    assert session.sent == 1
    assert session.deleted is False


def test_cancelling_initial_execution_wait_latches_unknown_outcome(monkeypatch):
    class InFlightCell(DroppedCell):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()

        async def _execute_locked(self, code, timeout, first_byte_timeout,
                                  preserve_on_disconnect):
            self.sent += 1
            if self.sent == 1:
                self.started.set()  # request sent; reply has not arrived
                await asyncio.Event().wait()
            return {"status": "ok", "outputs": []}

    session = InFlightCell()
    cluster = _cluster(monkeypatch, session)

    async def run():
        foreground = asyncio.create_task(cluster.execute("print('cell 6')"))
        await session.started.wait()
        foreground.cancel()
        with pytest.raises(asyncio.CancelledError):
            await foreground
        blocked = await cluster.execute("print('cell 7')")
        assert blocked["outputs"][0]["ename"] == "RecoveryUnresolved"
        await cluster.close()

    asyncio.run(run())
    assert session.sent == 1
    assert session.deleted is False


def test_queued_background_recycle_preserves_cancelled_cell(monkeypatch):
    class InFlightCell(DroppedCell):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()

        async def _execute_locked(self, code, timeout, first_byte_timeout,
                                  preserve_on_disconnect):
            self.sent += 1
            self.started.set()
            await asyncio.Event().wait()

    session = InFlightCell()
    cluster = _cluster(monkeypatch, session)

    async def run():
        foreground = asyncio.create_task(cluster.execute("print('cell 6')"))
        await session.started.wait()
        recycler = asyncio.create_task(cluster._do_recycle_async())
        await asyncio.sleep(0)  # queue the recycler behind the foreground lock
        foreground.cancel()
        with pytest.raises(asyncio.CancelledError):
            await foreground
        assert await recycler == "outcome_unresolved"
        await cluster.close()

    asyncio.run(run())
    assert session.sent == 1
    assert session.reconnects == 0
    assert session.deleted is False


def test_same_kernel_without_reply_stops_at_original_deadline(monkeypatch):
    session = DroppedCell(deadline=0.02)
    cluster = _cluster(monkeypatch, session)

    async def run():
        result = await cluster.execute("print('cell 6')", timeout=100)
        assert result["outputs"][0]["ename"] == "RecoveryUnresolved"
        assert "deadline" in result["outputs"][0]["evalue"]
        blocked = await cluster.execute("print('cell 7')")
        assert blocked["outputs"][0]["ename"] == "RecoveryUnresolved"
        await cluster.close()

    asyncio.run(run())
    assert session.sent == 1
    assert session.deleted is False


def test_second_caller_cannot_queue_behind_interrupted_cell(monkeypatch):
    session = DroppedCell(deadline=0.1)
    cluster = _cluster(monkeypatch, session)

    async def run():
        first = asyncio.create_task(cluster.execute("print('cell 6')"))
        while not cluster._interrupted_msg_id:
            await asyncio.sleep(0)
        second = await cluster.execute("print('cell 7')")
        assert second["outputs"][0]["ename"] == "RecoveryUnresolved"
        assert (await first)["outputs"][0]["ename"] == "RecoveryUnresolved"

    asyncio.run(run())
    assert session.sent == 1


def test_reconnect_exhaustion_preserves_uncertain_kernel(monkeypatch):
    session = DroppedCell(reconnect_error=True)
    cluster = _cluster(monkeypatch, session)

    async def no_delay(_):
        return None

    monkeypatch.setattr("cluster_session.asyncio.sleep", no_delay)

    async def run():
        result = await cluster.execute("print('cell 6')")
        assert result["outputs"][0]["ename"] == "RecoveryUnresolved"
        await cluster.close()

    asyncio.run(run())
    assert session.reconnects == 6
    assert session.sent == 1
    assert session.deleted is False


def test_replacement_kernel_never_certifies_old_cell(monkeypatch):
    session = DroppedCell()
    cluster = _cluster(monkeypatch, session)
    replacement = DroppedCell()

    async def replaced():
        cluster._session = replacement
        return "new_kernel"

    monkeypatch.setattr(cluster, "_do_recycle_async", replaced)
    result = asyncio.run(cluster.execute("print('cell 6')"))
    assert result["outputs"][0]["ename"] == "KernelLost"
    assert cluster._recovery_unresolved
    assert session.sent == 1


@pytest.mark.parametrize("error_name,state_loss", [
    ("RecoveryUnresolved", False), ("KernelLost", True),
])
def test_phase2_marks_unknown_cell_failed_and_does_not_execute_tail(
        monkeypatch, tmp_path, error_name, state_loss):
    notebook = {"cells": [
        {"cell_type": "code", "source": ["print('drop_target')"], "metadata": {},
         "outputs": [], "execution_count": None},
        {"cell_type": "code", "source": ["print('tail_target')"], "metadata": {},
         "outputs": [], "execution_count": None},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    original_count = cluster_session.cluster._state_loss_count
    monkeypatch.setattr(cluster_session.cluster, "_state_loss_count", original_count)

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if "drop_target" in code and "base64.b64decode" not in code:
                self.executed.append(code)
                if state_loss:
                    cluster_session.cluster._state_loss_count += 1
                return {"status": "error", "outputs": [{"type": "error",
                        "ename": error_name, "evalue": "original reply unavailable"}]}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))
    assert result["status"] == "FAIL", result
    assert result["failed"] == 1
    assert not any("tail_target" in code for code in kernel.executed)


@pytest.mark.parametrize("source", [
    "%sql SELECT 'DROP_TARGET'", "%scala\nprintln(\"DROP_TARGET\")",
])
def test_native_cell_transport_failure_never_retries_or_calls_model(
        monkeypatch, tmp_path, source):
    notebook = {"cells": [
        {"cell_type": "code", "source": [source], "metadata": {},
         "outputs": [], "execution_count": None},
        {"cell_type": "code", "source": ["print('TAIL_TARGET')"], "metadata": {},
         "outputs": [], "execution_count": None},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])

    async def forbidden_fix(*args, **kwargs):
        raise AssertionError("transport outcome must never reach call_fix")

    monkeypatch.setattr(jm, "call_fix", forbidden_fix)

    class Kernel(_Kernel):
        _recovery_unresolved = False

        async def execute(self, code, **kwargs):
            if "DROP_TARGET" in code and "base64.b64decode" not in code:
                self.executed.append(code)
                self._recovery_unresolved = True
                return {"status": "error", "outputs": [{"type": "error",
                        "ename": "RecoveryUnresolved", "evalue": "reply unavailable"}]}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))
    assert result["status"] == "FAIL", result
    assert result["failed"] == 1
    assert result["kind"] == "WS_RECOVERY_UNRESOLVED"
    assert sum("DROP_TARGET" in code and "base64.b64decode" not in code
               for code in kernel.executed) == 1
    assert not any("TAIL_TARGET" in code and "base64.b64decode" not in code
                   for code in kernel.executed)


@pytest.mark.parametrize("error_name", ["RecoveryUnresolved", "KernelLost"])
def test_parent_cluster_down_retry_transport_failure_is_terminal(
        monkeypatch, tmp_path, error_name):
    notebook = {"cells": [
        {"cell_type": "code", "source": ["print('DROP_TARGET')"],
         "metadata": {}, "outputs": [], "execution_count": None},
        {"cell_type": "code", "source": ["print('TAIL_TARGET')"],
         "metadata": {}, "outputs": [], "execution_count": None},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    fix_calls = []

    async def forbidden_fix(*args, **kwargs):
        fix_calls.append(args)
        raise AssertionError("transport retry must never reach call_fix")

    monkeypatch.setattr(jm, "call_fix", forbidden_fix)

    class Kernel(_Kernel):
        _recovery_unresolved = False

        def __init__(self):
            super().__init__()
            self.cell_sends = 0
            self.reconnects = 0

        async def force_reconnect(self):
            self.reconnects += 1

        async def execute(self, code, **kwargs):
            if "DROP_TARGET" in code and "base64.b64decode" not in code:
                self.executed.append(code)
                self.cell_sends += 1
                if self.cell_sends == 1:
                    return {"status": "ok", "outputs": [{"type": "stream",
                            "text": "Compute cluster cluster-1 is not running"}]}
                self._recovery_unresolved = True
                return {"status": "error", "outputs": [{"type": "error",
                        "ename": error_name, "evalue": "retry reply unavailable"}]}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))
    assert result["status"] == "FAIL", result
    assert result["kind"] == "WS_RECOVERY_UNRESOLVED"
    assert result["failed"] == 1
    assert result["job_should_stop"] is True
    assert kernel.cell_sends == 2
    assert kernel.reconnects == 1
    assert not fix_calls
    assert not any("TAIL_TARGET" in code and "base64.b64decode" not in code
                   for code in kernel.executed)


@pytest.mark.parametrize("error_name", ["RecoveryUnresolved", "KernelLost"])
def test_child_cluster_down_retry_transport_failure_is_terminal(
        monkeypatch, tmp_path, error_name):
    from test_child_exit_conversion import _ExitKernel, _run

    original_execute = _ExitKernel.execute
    sends = []
    fix_calls = []

    async def retry_execute(self, code, **kwargs):
        if "DROP_TARGET" in code and "base64.b64decode" not in code:
            self.executed.append(code)
            sends.append(code)
            if len(sends) == 1:
                return {"status": "ok", "outputs": [{"type": "stream",
                        "text": "Compute cluster cluster-1 is not running"}]}
            self._recovery_unresolved = True
            return {"status": "error", "outputs": [{"type": "error",
                    "ename": error_name, "evalue": "retry reply unavailable"}]}
        return await original_execute(self, code, **kwargs)

    async def force_reconnect(self):
        return None

    async def forbidden_fix(*args, **kwargs):
        fix_calls.append(args)
        raise AssertionError("child transport retry must never reach call_fix")

    monkeypatch.setattr(_ExitKernel, "execute", retry_execute)
    monkeypatch.setattr(_ExitKernel, "force_reconnect", force_reconnect, raising=False)
    monkeypatch.setattr(jm, "call_fix", forbidden_fix)
    result, _, kernel, _, _ = _run(
        monkeypatch, tmp_path,
        {"child": ["print('DROP_TARGET')", "print('TAIL_TARGET')"]})
    assert result["status"] == "error", result
    assert result["kind"] == "WS_RECOVERY_UNRESOLVED"
    assert len(sends) == 2
    assert not fix_calls
    assert not any("TAIL_TARGET" in code for code in kernel.executed)


def test_unknown_outcome_uploads_reports_through_independent_pool(
        monkeypatch, tmp_path):
    notebook = {"cells": [
        {"cell_type": "code", "source": ["print('DROP_TARGET')"],
         "metadata": {}, "outputs": [], "execution_count": None},
        {"cell_type": "code", "source": ["print('TAIL_TARGET')"],
         "metadata": {}, "outputs": [], "execution_count": None},
    ], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])

    async def forbidden_registry(*args, **kwargs):
        raise AssertionError("unresolved notebook must not enter the registry")

    monkeypatch.setattr(jm, "registry_update", forbidden_registry)

    class Kernel(_Kernel):
        _recovery_unresolved = False

        def __init__(self):
            super().__init__()
            self._pool = object()
            self.main_after_drop = []
            self.artifact_codes = []

        async def execute(self, code, **kwargs):
            if "DROP_TARGET" in code and "base64.b64decode" not in code:
                self.executed.append(code)
                self._recovery_unresolved = True
                return {"status": "error", "outputs": [{"type": "error",
                        "ename": "RecoveryUnresolved", "evalue": "original reply unavailable"}]}
            if self._recovery_unresolved:
                self.main_after_drop.append(code)
                raise AssertionError("artifact write reached uncertain main kernel")
            return await super().execute(code, **kwargs)

        async def run_stateless(self, code, **kwargs):
            self.artifact_codes.append(code)
            return await _Kernel.execute(self, code, **kwargs)

    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))
    assert result["status"] == "FAIL", result
    assert result["kind"] == "WS_RECOVERY_UNRESOLVED"
    assert result["failed"] == 1
    assert result["job_should_stop"] is True
    assert kernel.main_after_drop == []
    assert any("base64.b64decode" in code for code in kernel.artifact_codes)
    assert any(path.endswith("test_report.md") for path in kernel.files)
    assert not any("TAIL_TARGET" in code and "base64.b64decode" not in code
                   for code in kernel.executed)


@pytest.mark.parametrize("parent", [False, True])
def test_inlined_child_transport_failure_returns_terminal_kind(monkeypatch, tmp_path, parent):
    from test_child_exit_conversion import _ExitKernel, _run

    original_execute = _ExitKernel.execute

    async def dropped_execute(self, code, **kwargs):
        if "DROP_TARGET" in code and "base64.b64decode" not in code:
            self.executed.append(code)
            self._recovery_unresolved = True
            return {"status": "error", "outputs": [{"type": "error",
                    "ename": "KernelLost", "evalue": "original kernel replaced"}]}
        return await original_execute(self, code, **kwargs)

    async def forbidden_fix(*args, **kwargs):
        raise AssertionError("child transport outcome must never reach call_fix")

    monkeypatch.setattr(_ExitKernel, "execute", dropped_execute)
    monkeypatch.setattr(jm, "call_fix", forbidden_fix)
    result, _, kernel, _, _ = _run(
        monkeypatch, tmp_path,
        {"child": ["print('DROP_TARGET')", "print('TAIL_TARGET')"]},
        parent=["%run /Workspace/child", "print('PARENT_TAIL')"] if parent else None)
    assert result["status"] == ("FAIL" if parent else "error"), result
    if not parent:
        assert result["kind"] == "WS_RECOVERY_UNRESOLVED"
    assert sum("DROP_TARGET" in code for code in kernel.executed) == 1
    assert not any("TAIL_TARGET" in code for code in kernel.executed)
    assert not any("PARENT_TAIL" in code for code in kernel.executed)
