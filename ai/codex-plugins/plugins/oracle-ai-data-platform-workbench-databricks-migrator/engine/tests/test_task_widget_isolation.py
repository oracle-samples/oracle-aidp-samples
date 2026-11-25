"""O1 -> O2 isolation, run cccc52ed (2026-10-06).

Drive process_notebook, including its post-%run path, with native text()
PRESERVE semantics. Category 2: task parameter plumbing, not model semantics.
Matrix: overlap / omitted / empty / None / nested JSON, fresh or shared stores,
with and without %run; clear/set/readback failures must stop before customer code.
"""
import asyncio
import contextlib
import io
import json
import os
from types import SimpleNamespace

import pytest

from test_conditional_exit_below_execute import _Kernel, _patch_common
from test_cohort_widget_seed import _PreserveWidgets
import job_migrate as jm


class Kernel(_Kernel):
    def __init__(self):
        super().__init__()
        self.ns = {'aidputils': SimpleNamespace(widgets=_PreserveWidgets()),
                   'dbutils': SimpleNamespace(widgets=_PreserveWidgets())}
        self.observed = []

    async def execute(self, code, **kwargs):
        if code.startswith('import os, json; os.environ[') or '_task_expected =' in code:
            self.executed.append(code)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                exec(code, self.ns)
            return {'status': 'ok', 'outputs': [
                {'type': 'stream', 'name': 'stdout', 'text': out.getvalue()}]}
        if 'print("TASK_WIDGETS")' in code:
            self.observed.append([dict(self.ns[name].widgets.getAll())
                                  for name in ('aidputils', 'dbutils')])
        return await super().execute(code, **kwargs)


def notebook(with_run=False):
    sources = ['print("TASK_WIDGETS")']
    if with_run:
        sources.insert(0, '%run /Workspace/example/helper')
    return {'cells': [{'cell_type': 'code', 'source': [s], 'metadata': {},
                      'outputs': [], 'execution_count': None} for s in sources],
            'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}


@pytest.mark.parametrize('with_run', [False, True])
@pytest.mark.parametrize('second', [
    {'source_table': 'O2_REQLOGS', 'save_path': 'o2_output', 'TOF': '{"UNIT":"o2"}'},
    {'source_table': 'O2_REQLOGS'},
    {},
    {'source_table': '', 'save_path': None},
])
def test_sequential_tasks_use_current_widgets(monkeypatch, tmp_path, with_run, second):
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    _patch_common(monkeypatch, tmp_path, notebook(with_run))
    kernel = Kernel()
    # A reused kernel may contain notebook-declared widgets with no ownership
    # tracker (including kernels created before this fix). Clear these too.
    for name in ('aidputils', 'dbutils'):
        kernel.ns[name].widgets.text('prior_notebook_widget', 'stale')

    async def child(*args, **kwargs):
        # A child replaces both stores, holding incorrect current task values.
        for name in ('aidputils', 'dbutils'):
            prior = kernel.ns[name].widgets.getAll()
            kernel.ns[name].widgets = _PreserveWidgets({k: 'child' for k in prior})
            kernel.ns[name].widgets.text('current_helper_widget', 'keep')
        return {'status': 'ok', 'did_exit': False, 'cells_ok': 1, 'cells_fixed': 0}
    monkeypatch.setattr(jm, '_inline_child_notebook', child)
    first = {'source_table': 'O1_TEAMX', 'save_path': 'o1_output',
             'TOF': '{"UNIT":"o1"}', 'only_o1': 'must disappear'}
    for key, params in [('01_o1', first), ('02_o2', second)]:
        result = asyncio.run(jm.process_notebook(
            '/Workspace/example/source.ipynb', kernel, 'example_job', key, params))
        assert result['status'] == 'PASS', result
        expected = {k: '' if v is None else str(v) for k, v in params.items()}
        for actual in kernel.observed[-1]:
            helper = {'current_helper_widget': 'keep'} if with_run else {}
            assert actual == {**expected, **helper}
            assert 'prior_notebook_widget' not in actual
        for name in ('aidputils', 'dbutils'):
            for missing in set(first) - set(params):
                with pytest.raises(Exception, match='widget not found'):
                    kernel.ns[name].widgets.get(missing)
        assert json.loads(os.environ['AIDP_PARAMS']) == params


@pytest.mark.parametrize('failure', ['remove', 'text', 'readback'])
def test_bad_widget_setup_stops_before_customer_execution(monkeypatch, tmp_path, failure):
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    _patch_common(monkeypatch, tmp_path, notebook())
    kernel = Kernel()
    widgets = kernel.ns['aidputils'].widgets
    widgets.text('source_table', 'O1_TEAMX')
    if failure == 'readback':
        monkeypatch.setattr(widgets, 'get', lambda key: 'O1_TEAMX')
    else:
        def fail(*args):
            raise RuntimeError('widget operation failed')
        monkeypatch.setattr(widgets, failure, fail)
    with pytest.raises(RuntimeError, match='task widget parameter setup failed'):
        asyncio.run(jm.process_notebook('/Workspace/example/source.ipynb', kernel,
                    'example_job', '02_o2', {'source_table': 'O2_REQLOGS'}))
    assert not kernel.observed
    assert not kernel.files


def test_missing_setup_ack_stops_execution():
    class NoAck:
        async def execute(self, *args, **kwargs):
            return {'status': 'ok', 'outputs': []}
    with pytest.raises(RuntimeError, match='task widget parameter setup failed'):
        asyncio.run(jm._seed_task_validation_widgets(NoAck(), SimpleNamespace(log=lambda s: None)))


@pytest.mark.parametrize('override', [False, True])
def test_parent_created_widget_survives_child_and_restoration(monkeypatch, tmp_path, override):
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    nb = notebook()
    nb['cells'] = [
        {'cell_type': 'code', 'source': ['# PARAM_PARENT_DEFINE\naidputils.widgets.text("parent_only", "from-parent")'],
         'metadata': {}, 'outputs': [], 'execution_count': None},
        {'cell_type': 'code', 'source': ['%run /Workspace/example/helper' +
                                       (' $start_date="child-date"' if override else '')],
         'metadata': {}, 'outputs': [], 'execution_count': None},
        {'cell_type': 'code', 'source': ['print("TASK_WIDGETS")'],
         'metadata': {}, 'outputs': [], 'execution_count': None},
    ]
    _patch_common(monkeypatch, tmp_path, nb)
    monkeypatch.setattr(jm, 'MIGRATE_OUTPUT', str(tmp_path / 'output'))

    class ScopeKernel(Kernel):
        def __init__(self):
            super().__init__()
            values = {}
            self.ns['spark'] = SimpleNamespace(conf=SimpleNamespace(
                get=lambda key, default=None: values.get(key, default),
                set=lambda key, value: values.__setitem__(key, value),
                unset=lambda key: values.pop(key, None)))

        async def execute(self, code, **kwargs):
            if ('# PARAM_PARENT_DEFINE' in code and 'base64.b64decode' not in code
                    or '_pr_stack =' in code or '_pr_previous =' in code):
                self.executed.append(code)
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    exec(code, self.ns)
                return {'status': 'ok', 'outputs': [
                    {'type': 'stream', 'name': 'stdout', 'text': out.getvalue()}]}
            return await super().execute(code, **kwargs)
    kernel = ScopeKernel()
    child_reads = []

    async def child(*args, **kwargs):
        widgets = kernel.ns['aidputils'].widgets
        child_reads.append((widgets.get('parent_only'), widgets.get('start_date')))
        return {'status': 'ok', 'did_exit': False, 'cells_ok': 1, 'cells_fixed': 0}
    monkeypatch.setattr(jm, '_inline_child_notebook', child)
    result = asyncio.run(jm.process_notebook('/Workspace/example/parent.ipynb', kernel,
                'example_job', '02_o2', {'start_date': 'parent-date'}))
    assert result['status'] == 'PASS', result
    assert child_reads == [('from-parent', 'child-date' if override else 'parent-date')]
    assert kernel.ns['aidputils'].widgets.getAll() == {
        'parent_only': 'from-parent', 'start_date': 'parent-date'}


def test_unresolved_child_transport_does_not_refresh_widgets(monkeypatch, tmp_path):
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    _patch_common(monkeypatch, tmp_path, notebook(with_run=True))
    kernel = Kernel()

    async def unresolved(*args, **kwargs):
        return {'status': 'error', 'kind': 'WS_RECOVERY_UNRESOLVED',
                'error': 'child outcome unknown', 'cells_ok': 0, 'cells_fixed': 0}
    monkeypatch.setattr(jm, '_inline_isolated_percent_run_child', unresolved)
    result = asyncio.run(jm.process_notebook('/Workspace/example/parent.ipynb', kernel,
                'example_job', '02_o2', {'source_table': 'O2_REQLOGS'}))
    assert result['status'] == 'FAIL', result
    assert result['kind'] == 'WS_RECOVERY_UNRESOLVED'
    assert sum('_task_expected =' in code for code in kernel.executed) == 1
    assert not kernel.observed


@pytest.mark.parametrize("params", [{}, {"source_table": "current_task"}])
@pytest.mark.parametrize("setup_status", ["ok", "error"])
def test_task_environment_setup_uses_thirty_seconds_and_stays_fail_closed(
        monkeypatch, tmp_path, params, setup_status):
    """REF-209: run ccccbbd2/cccca825 timed out at the task boundary."""
    monkeypatch.delenv("AIDP_PARAMS", raising=False)
    _patch_common(monkeypatch, tmp_path, notebook())
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))

    class ParameterKernel(Kernel):
        parameter_timeouts = None

        async def execute(self, code, **kwargs):
            if code.startswith("import os, json; os.environ["):
                self.parameter_timeouts.append(kwargs.get("timeout"))
                if setup_status == "error":
                    return {"status": "error", "outputs": [{"type": "error",
                            "ename": "TimeoutError",
                            "evalue": "Execution timed out after 30s"}]}
            return await super().execute(code, **kwargs)

    kernel = ParameterKernel()
    kernel.parameter_timeouts = []
    call = jm.process_notebook("/Workspace/example/source.ipynb", kernel,
                               "example_job", "02_current", params)
    if setup_status == "error":
        with pytest.raises(RuntimeError, match="parameter environment setup failed"):
            asyncio.run(call)
        assert not kernel.observed
        assert not kernel.files
        assert not any("_task_expected =" in code for code in kernel.executed)
    else:
        result = asyncio.run(call)
        assert result["status"] == "PASS", result
        assert json.loads(os.environ["AIDP_PARAMS"]) == params
        assert kernel.observed
    assert kernel.parameter_timeouts == [30]


# REF-225, run cccc0cdd (2026-10-16): the first task lost its exception
# after a child; subsequent tasks lost it at initial setup. Exercise both
# production call sites, both widget stores and every operation boundary.
@pytest.mark.parametrize('after_child', [False, True])
@pytest.mark.parametrize('store', ['aidputils', 'dbutils'])
@pytest.mark.parametrize('operation', ['getAll', 'remove', 'text', 'get'])
def test_widget_failure_diagnostic_through_task_loop(
        monkeypatch, tmp_path, after_child, store, operation):
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    _patch_common(monkeypatch, tmp_path, notebook(with_run=after_child))
    kernel = Kernel()
    secret = 'synthetic-private-parameter'

    def install_failure():
        widgets = kernel.ns[store].widgets
        widgets.text('private_param', secret)
        def fail(*args):
            raise ValueError('backend rejected ' + secret)
        monkeypatch.setattr(widgets, operation, fail)

    async def child(*args, **kwargs):
        install_failure()
        return {'status': 'ok', 'did_exit': False, 'cells_ok': 1, 'cells_fixed': 0}

    if after_child:
        monkeypatch.setattr(jm, '_inline_child_notebook', child)
    else:
        install_failure()
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(jm.process_notebook('/Workspace/example/source.ipynb', kernel,
                    'example_job', 'current', {'private_param': secret}))
    message = str(caught.value)
    assert ('post-child refresh' if after_child else 'initial setup') in message
    assert 'ValueError' in message and 'backend rejected <redacted>' in message
    assert secret not in message
    assert not kernel.observed
    assert not kernel.files


@pytest.mark.parametrize('reset', [False, True])
@pytest.mark.parametrize('status', ['ok', 'error'])
def test_no_ack_diagnostic_does_not_forward_widget_stdout(reset, status):
    class NoAck:
        async def execute(self, *args, **kwargs):
            return {'status': status, 'outputs': [{'type': 'stream',
                    'text': '[{"type":"WIDGET","value":"private-value"}]'}]}
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(jm._seed_task_validation_widgets(
            NoAck(), SimpleNamespace(log=lambda s: None), reset=reset))
    message = str(caught.value)
    assert f'status={status}' in message
    assert 'remote diagnostic unavailable' in message
    assert 'private-value' not in message


@pytest.mark.parametrize('raw,error', [('{invalid', 'JSONDecodeError'),
                                      ('[]', 'AttributeError')])
def test_invalid_parameter_json_keeps_original_exception(monkeypatch, raw, error):
    monkeypatch.setenv('AIDP_PARAMS', raw)
    with pytest.raises(RuntimeError, match=error) as caught:
        asyncio.run(jm._seed_task_validation_widgets(
            Kernel(), SimpleNamespace(log=lambda s: None), reset=True))
    assert 'phase=initial setup' in str(caught.value)


def test_widget_success_log_excludes_native_value_echo():
    messages = []
    class Echo:
        async def execute(self, *args, **kwargs):
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                '[{"type":"WIDGET","value":"private-value [task-params] private"}]'
                '[task-params] verified 1 parameters; cleared 0 stale widget keys\n'
                + jm._ISO_OK + '\n'}]}
    asyncio.run(jm._seed_task_validation_widgets(
        Echo(), SimpleNamespace(log=messages.append), reset=True))
    assert messages == ['[task-params] verified 1 parameters; cleared 0 stale widget keys']
