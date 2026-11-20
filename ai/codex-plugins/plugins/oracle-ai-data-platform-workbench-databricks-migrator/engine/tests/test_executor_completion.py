"""REF-295: run 20261029T020654Z-c138be7c lost an acknowledged reply."""
import asyncio
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import aidp_executor as ae
from cluster_session import ClusterSession


def _session():
    session = ae.AIDPSession(
        cluster_id='test-cluster', workspace_id='test-workspace',
        lake_ocid='ocid1.aidataplatform.oc1.test-region.fixture')
    session.session_id = 'completion-fixture'
    session._ws_alive = True
    session.ws = SimpleNamespace(connected=True, send_binary=lambda _: None)
    return session


def _message(msg_id, kind, **content):
    return {'header': {'msg_type': kind}, 'parent_header': {'msg_id': msg_id},
            'content': content}


PRELUDE = ('----> 1 sc.setJobGroup("fixture", "fixture")\n'
           "AttributeError: 'function' object has no attribute 'setJobGroup'")


@pytest.mark.parametrize('busy', [False, True])
@pytest.mark.parametrize('consumer,busy_timeout,first_timeout,preserve', [
    ('foreground', 60, None, True),
    ('direct', 60, None, False),
    ('health', None, 60, False),
    ('combined', 60, 60, False),
])
@pytest.mark.parametrize('reply_status,output,expected_status,expected_error', [
    ('ok', None, 'ok', None),
    ('error', None, 'error', None),
    ('aborted', None, 'aborted', None),
    ('ok', {'type': 'stream', 'text': 'finished'}, 'ok', None),
    ('ok', {'type': 'error', 'ename': 'ValueError', 'evalue': 'fixture'},
     'error', 'ValueError'),
    ('ok', {'type': 'stream', 'text': PRELUDE}, 'error', 'KernelPreexecutionError'),
])
def test_reply_validation_retains_request_until_consumed(
        monkeypatch, busy, consumer, busy_timeout, first_timeout, preserve,
        reply_status, output, expected_status, expected_error):
    session = _session()
    clock = [0.0]
    retained = []
    threads, failures = [], []
    validating, release = threading.Event(), threading.Event()
    original_validation = ae._kernel_prelude_error
    real_sleep = asyncio.sleep
    polls = 0

    def validate(outputs):
        validating.set()
        if not release.wait(5):
            raise RuntimeError('reply validation barrier timed out')
        return original_validation(outputs)

    def reply(msg_id):
        try:
            session._handle_message(_message(msg_id, 'execute_reply', status=reply_status))
        except BaseException as exc:
            failures.append(exc)

    async def tick(_):
        nonlocal polls
        polls += 1
        if polls == 1:
            msg_id = next(iter(session._pending))
            retained.append((msg_id, session._pending[msg_id], session._outputs[msg_id]))
            if busy:
                session._handle_message(_message(msg_id, 'status', execution_state='busy'))
        elif polls == 2:
            clock[0] = 61.0
            msg_id = retained[0][0]
            if output:
                session._outputs[msg_id].append(dict(output))
            thread = threading.Thread(target=reply, args=(msg_id,), daemon=True)
            threads.append(thread)
            thread.start()
            assert validating.wait(2), 'reply handler did not enter validation'
            assert session._pending[msg_id] is retained[0][1]
            assert session._outputs[msg_id] is retained[0][2]
            assert not retained[0][1]['event'].is_set()
            assert not retained[0][1]['result']
            # The waiter now runs a complete timeout-check iteration while
            # the actual reply handler is paused before result publication.
        else:
            release.set()
            threads[0].join(2)
            assert not threads[0].is_alive()
        await real_sleep(0)

    monkeypatch.setattr(ae, 'time', SimpleNamespace(
        time=lambda: clock[0], strftime=lambda _: '00:00:00'))
    monkeypatch.setattr(ae, 'asyncio', SimpleNamespace(sleep=tick, CancelledError=asyncio.CancelledError))
    monkeypatch.setattr(ae, '_kernel_prelude_error', validate)
    monkeypatch.setattr(ClusterSession, '_instance', None)
    cluster = ClusterSession()
    cluster._session = session
    try:
        if consumer == 'foreground':
            result = asyncio.run(cluster.execute('pass', timeout=120))
        else:
            result = asyncio.run(session._execute_locked(
                'pass', timeout=120, status_busy_timeout=busy_timeout,
                first_byte_timeout=first_timeout, preserve_on_disconnect=preserve))
    finally:
        release.set()
        for thread in threads:
            thread.join(2)
    assert not failures
    assert result['status'] == expected_status, result
    if expected_error:
        assert any(out.get('ename') == expected_error for out in result['outputs'])
    if output:
        assert output in result['outputs']
    assert polls >= 3, result
    assert retained[0][1]['event'].is_set()
    assert session._ws_alive
    assert not session._pending and not session._outputs


@pytest.mark.parametrize('busy,output,busy_timeout,first_timeout,expected_error', [
    (False, False, 60, None, 'KernelHung'),
    (False, True, 60, None, 'KernelHung'),
    (True, False, 60, 60, 'FirstByteTimeout'),
    (False, False, None, 60, 'FirstByteTimeout'),
    (True, True, 60, None, 'TimeoutError'),
    (True, False, 60, None, 'TimeoutError'),
    (False, False, None, None, 'TimeoutError'),
])
def test_genuine_deadlines_still_fail_and_clean_up(
        monkeypatch, busy, output, busy_timeout, first_timeout, expected_error):
    session = _session()
    clock = [0.0]
    real_sleep = asyncio.sleep

    async def tick(_):
        msg_id = next(iter(session._pending))
        if busy:
            session._handle_message(_message(msg_id, 'status', execution_state='busy'))
        if output:
            session._outputs[msg_id].append({'type': 'stream', 'text': 'working'})
        clock[0] += 61
        await real_sleep(0)

    monkeypatch.setattr(ae, 'time', SimpleNamespace(
        time=lambda: clock[0], strftime=lambda _: '00:00:00'))
    monkeypatch.setattr(ae, 'asyncio', SimpleNamespace(sleep=tick, CancelledError=asyncio.CancelledError))
    result = asyncio.run(session._execute_locked(
        'pass', timeout=120, status_busy_timeout=busy_timeout,
        first_byte_timeout=first_timeout))
    assert result['outputs'][0]['ename'] == expected_error
    assert session._ws_alive is (expected_error != 'KernelHung')
    assert not session._pending and not session._outputs


def test_reply_receipt_does_not_disable_overall_execution_deadline(monkeypatch):
    session = _session()
    clock = [0.0]
    validating, release = threading.Event(), threading.Event()
    threads, failures = [], []
    retained = []
    real_sleep = asyncio.sleep

    def validate(outputs):
        validating.set()
        if not release.wait(5):
            raise RuntimeError('reply validation barrier timed out')

    def reply(msg_id):
        try:
            session._handle_message(_message(msg_id, 'execute_reply', status='ok'))
        except BaseException as exc:
            failures.append(exc)

    async def tick(_):
        msg_id = next(iter(session._pending))
        retained.append(session._pending[msg_id])
        thread = threading.Thread(target=reply, args=(msg_id,), daemon=True)
        threads.append(thread)
        thread.start()
        assert validating.wait(2)
        clock[0] = 121
        await real_sleep(0)

    monkeypatch.setattr(ae, 'time', SimpleNamespace(
        time=lambda: clock[0], strftime=lambda _: '00:00:00'))
    monkeypatch.setattr(ae, 'asyncio', SimpleNamespace(sleep=tick, CancelledError=asyncio.CancelledError))
    monkeypatch.setattr(ae, '_kernel_prelude_error', validate)
    try:
        result = asyncio.run(session._execute_locked(
            'pass', timeout=120, status_busy_timeout=60, first_byte_timeout=60))
        assert not retained[0]['event'].is_set()
        assert not session._pending and not session._outputs
    finally:
        release.set()
        for thread in threads:
            thread.join(2)
    assert not failures
    assert result['outputs'][0]['ename'] == 'TimeoutError'
    assert session._ws_alive
    assert retained[0]['event'].is_set()


def test_fast_reply_snapshot_and_duplicate_reply_before_consumption():
    session = _session()

    def send(_):
        msg_id = next(iter(session._pending))
        session._handle_message(_message(msg_id, 'stream', text='finished'))
        session._handle_message(_message(msg_id, 'execute_reply', status='ok'))
        # The buffer stays tracked until consumption, but output after the
        # reply must not enter the already-validated result snapshot.
        session._handle_message(_message(msg_id, 'error', ename='LateError', evalue='late'))
        session._handle_message(_message(msg_id, 'execute_reply', status='error'))

    session.ws.send_binary = send
    result = asyncio.run(session.execute('pass', timeout=120))
    assert result == {'status': 'ok', 'outputs': [
        {'type': 'stream', 'name': 'stdout', 'text': 'finished'}]}
    assert session._ws_alive
    assert not session._pending and not session._outputs


@pytest.mark.parametrize('kind,content', [
    ('stream', {'text': 'late output'}),
    ('execute_result', {'data': {'text/plain': 'late result'}}),
    ('display_data', {'data': {'text/plain': 'late display'}}),
    ('error', {'ename': 'LateError', 'evalue': 'late error'}),
    ('status', {'execution_state': 'busy'}),
    ('status', {'execution_state': 'idle'}),
])
def test_waiter_cleanup_during_message_lookup_does_not_break_listener(kind, content):
    session = _session()
    pending = {'event': threading.Event(), 'result': {'result': {'status': 'ok', 'outputs': []}}}
    pending['event'].set()
    cleaned = []

    class ConsumedTracking(dict):
        def consume(self):
            cleaned.append(True)
            # This is the successful waiter's cleanup, interleaved with a
            # listener lookup: membership/indexing cannot span that cleanup.
            dict.clear(session._pending)
            dict.clear(session._outputs)

        def __contains__(self, key):
            present = super().__contains__(key)
            if present:
                self.consume()
            return present

        def get(self, key, default=None):
            value = super().get(key, default)
            if value is not default:
                self.consume()
            return value

    session._pending = ConsumedTracking({'fixture': pending})
    session._outputs = ConsumedTracking({'fixture': []})
    session._handle_message(_message('fixture', kind, **content))
    assert cleaned
    assert pending['result']['result'] == {'status': 'ok', 'outputs': []}
    assert not session._pending and not session._outputs


def test_stream_traceback_survives_transport_and_fails_migration(monkeypatch, tmp_path):
    # The live REF-295 control returned wire ok with its ValueError in stream
    # output. Preserve that transport contract and let the existing migration
    # verifier assess the uncaught exception; never manufacture transport errors.
    from test_eval_verdict_gate import _notebook, jm
    from test_conditional_exit_below_execute import _Kernel, _patch_common

    source = '# ISU295_UNCAUGHT\nraise ValueError("ISU295 deliberate error control")'
    text = 'Traceback (most recent call last):\nValueError: ISU295 deliberate error control\n'
    _patch_common(monkeypatch, tmp_path, _notebook(source))
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 1)
    monkeypatch.setattr(jm, 'fetch_recent_spark_errors', lambda *a, **k: '')
    monkeypatch.setattr(jm, '_cell_history', [])
    feedback, delivered = [], []

    async def evaluate(system, user, **kwargs):
        feedback.append(user)
        return 'ISSUE: uncaught ValueError | FIX: investigate the deliberate failure', {
            'input': 0, 'output': 0}

    async def fix(code, *args, **kwargs):
        return code

    class StreamErrorKernel(_Kernel):
        async def execute(self, code, **kwargs):
            if '# ISU295_UNCAUGHT' in code and 'base64.b64decode' not in code:
                wire = _session()
                pending = {'event': threading.Event(), 'result': {}}
                wire._pending['fixture'] = pending
                wire._outputs['fixture'] = []
                wire._handle_message(_message('fixture', 'stream', text=text))
                wire._handle_message(_message('fixture', 'execute_reply', status='ok'))
                result = pending['result']['result']
                delivered.append(result)
                return result
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(jm, 'call_opus_with_tools', evaluate)
    monkeypatch.setattr(jm, 'call_fix', fix)
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', StreamErrorKernel(),
        'completion_fixture', 'task', {}, run_all=True))
    assert result['status'] == 'FAIL', result
    assert delivered and delivered[0]['status'] == 'ok'
    assert ae.format_outputs(delivered[0]['outputs']) == text
    assert feedback and all(text.strip() in user for user in feedback)


@pytest.mark.parametrize('consumer', ['direct', 'pool'])
@pytest.mark.parametrize('timing', ['before_reply', 'during_validation', 'after_completion'])
@pytest.mark.parametrize('reply_status', ['ok', 'error'])
def test_cancelled_requests_release_tracking_and_pool_remains_usable(
        monkeypatch, consumer, timing, reply_status):
    from cluster_session import StatelessPool

    session = _session()
    validation_started, release_validation = threading.Event(), threading.Event()
    original_validation = ae._kernel_prelude_error
    failures = []

    def validate(outputs):
        if timing == 'during_validation':
            validation_started.set()
            if not release_validation.wait(5):
                raise RuntimeError('cancelled reply validation barrier timed out')
        return original_validation(outputs)

    monkeypatch.setattr(ae, '_kernel_prelude_error', validate)

    def complete(msg_id):
        try:
            session._handle_message(_message(msg_id, 'execute_reply', status=reply_status))
        except BaseException as exc:
            failures.append(exc)

    async def run():
        pool = StatelessPool('test-cluster', size=1)
        pool._queue = asyncio.Queue()
        await pool._queue.put(session)

        async def execute():
            if consumer == 'pool':
                return await pool.run('pass')
            return await session.execute('pass')

        for _ in range(3):
            old = set(session._pending)
            task = asyncio.create_task(execute())
            while not set(session._pending) - old:
                await asyncio.sleep(0)
            msg_id = (set(session._pending) - old).pop()
            pending = session._pending[msg_id]
            session._handle_message(_message(msg_id, 'stream', text='x' * (1024 * 1024)))
            thread = None
            validation_started.clear()
            release_validation.clear()
            if timing == 'after_completion':
                complete(msg_id)
                assert pending['event'].is_set()
            elif timing == 'during_validation':
                thread = threading.Thread(target=complete, args=(msg_id,), daemon=True)
                thread.start()
                assert validation_started.wait(2)
                assert not pending['event'].is_set()
            try:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            finally:
                release_validation.set()
                if thread is not None:
                    thread.join(2)
                    assert not thread.is_alive()
            if timing == 'before_reply':
                complete(msg_id)
            assert not failures
            assert not session._pending and not session._outputs
            assert session._ws_alive
            assert pool._queue.qsize() == 1

        def immediate_reply(_):
            msg_id = next(iter(session._pending))
            session._handle_message(_message(msg_id, 'execute_reply', status='ok'))

        session.ws.send_binary = immediate_reply
        result = await execute()
        assert result['status'] == 'ok'
        assert not session._pending and not session._outputs
        assert pool._queue.qsize() == 1

    asyncio.run(run())


@pytest.mark.parametrize('reply_first', [False, True])
def test_actual_foreground_cancellation_cleans_tracking_and_latches_unknown_outcome(
        monkeypatch, reply_first):
    monkeypatch.setattr(ClusterSession, '_instance', None)
    cluster = ClusterSession()
    session = _session()
    cluster._session = session

    async def run():
        task = asyncio.create_task(cluster.execute('pass'))
        while not session._pending:
            await asyncio.sleep(0)
        msg_id = next(iter(session._pending))
        if reply_first:
            session._handle_message(_message(msg_id, 'execute_reply', status='ok'))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if not reply_first:
            session._handle_message(_message(msg_id, 'execute_reply', status='ok'))
        assert not session._pending and not session._outputs
        assert cluster._recovery_unresolved
        result = await cluster.execute('pass')
        assert result['outputs'][0]['ename'] == 'RecoveryUnresolved'

    asyncio.run(run())


def test_disconnect_handoff_keeps_tracking_until_actual_recovery_consumes_reply(monkeypatch):
    monkeypatch.setattr(ClusterSession, '_instance', None)
    cluster = ClusterSession()
    session = _session()
    cluster._session = session
    sent, recovered = [], []

    def disconnect(_):
        msg_id = next(iter(session._pending))
        sent.append(msg_id)
        session._handle_message(_message(msg_id, 'stream', text='before disconnect'))
        session._ws_alive = False

    async def reconnect(preserve_msg_id=None):
        assert preserve_msg_id == sent[0]
        assert preserve_msg_id in session._pending
        assert preserve_msg_id in session._outputs
        assert not session._pending[preserve_msg_id]['event'].is_set()
        session._ws_alive = True
        session._handle_message(_message(preserve_msg_id, 'execute_reply', status='ok'))
        recovered.append(preserve_msg_id)

    session.ws.send_binary = disconnect
    monkeypatch.setattr(session, 'reconnect_ws', reconnect)
    result = asyncio.run(cluster.execute('pass', timeout=10))
    assert result == {'status': 'ok', 'outputs': [
        {'type': 'stream', 'name': 'stdout', 'text': 'before disconnect'}]}
    assert len(sent) == 1 and recovered == sent
    assert not session._pending and not session._outputs
    assert not cluster._recovery_unresolved
