"""REF-225, run ccccda37: customer collision -> model repair, no kernel reset."""
import asyncio
import json
import threading
from types import SimpleNamespace

import pytest
from test_fixup_cell_persistence import child_replay_flow
import job_migrate as jm
from aidp_executor import AIDPSession, format_outputs, _kernel_prelude_error

ORIGINAL = '# CHILD_REPLAY_COLLISION\ndef sc(value):\n    return value * 2\ndef hdrs(value):\n    return sc(value)\n'
REPAIRED = '''# CHILD_REPLAY_COLLISION
# def sc(value):
#     return value * 2
# def hdrs(value):
#     return sc(value)
# Oracle tool modification: preserve SparkContext; rename helper and uses.
def format_cell(value):
    return value * 2
def hdrs(value):
    return format_cell(value)
'''


@pytest.mark.parametrize('repair', [True, False])
def test_child_collision_reaches_agent_and_saved_artifact(monkeypatch, child_replay_flow, repair):
    notebook, run, Kernel, *rest = child_replay_flow
    context = SimpleNamespace(setJobGroup=lambda *args: None)
    feedback = []
    caller = '# CHILD_REPLAY_CALLER\nanswer = sc(3)\nheader = hdrs(4)'
    path = notebook(sources=[ORIGINAL, caller])

    class PlatformKernel(Kernel):
        async def execute(self, code, **kwargs):
            # Real AIDP runs this BEFORE submitted code, including repairs.
            self.namespace['sc'].setJobGroup('fixture', 'fixture')
            if '# CHILD_REPLAY_COLLISION' in code or '# CHILD_REPLAY_CALLER' in code:
                self.executed.append(code)
                try:
                    exec(code, self.namespace)
                except Exception as exc:
                    return {'status': 'error', 'outputs': [{'type': 'error',
                        'ename': type(exc).__name__, 'evalue': str(exc)}]}
                return {'status': 'ok', 'outputs': []}
            return await super().execute(code, **kwargs)

    kernel = PlatformKernel()
    kernel.namespace['sc'] = context
    async def fix(code, error, *args, **kwargs):
        feedback.append(error)
        assert kernel.namespace['sc'] is context
        await kwargs['session'].execute('print("diagnostic")')
        if not repair:
            return code
        if 'def sc(' in code:
            assert 'BLOCKED before execution' in error and 'sc.setJobGroup' in error
            return REPAIRED
        assert 'not callable' in error
        return ('# CHILD_REPLAY_CALLER\n# answer = sc(3)\n'
                '# Oracle tool modification: use renamed helper.\n'
                'answer = format_cell(3)\nheader = hdrs(4)')
    monkeypatch.setattr(jm, 'call_fix', fix)
    result = asyncio.run(run(path, kernel))
    assert kernel.namespace['sc'] is context
    assert not any(code == ORIGINAL.rstrip() for code in kernel.executed)
    assert 1 <= len(feedback) <= 2 * jm.MAX_FIX_ATTEMPTS
    if repair:
        assert result['status'] == 'ok', result
        assert kernel.namespace['answer'] == 6 and kernel.namespace['header'] == 8
        saved = json.loads(path.read_text())
        sources = [''.join(c['source']) for c in saved['cells']]
        assert 'def format_cell' in sources[0]
        assert 'answer = format_cell(3)' in sources[1]
        cached = json.loads(jm._notebook_content_cache[str(path)])
        assert [c['source'] for c in cached['cells']] == [c['source'] for c in saved['cells']]
    else:
        assert result.get('failed_cells'), result


@pytest.mark.parametrize('source', [
    'def sc(x):\n    return x', 'import math as sc', 'sc = 42',
    'sc = spark.sparkContext', 'sc = SparkContext.getOrCreate()',
    '%python\ndef sc(x):\n    return x',
    '%pyspark\ndef sc(x):\n    return x',
])
@pytest.mark.parametrize('tag', ['cell', 'child', 'replay', 'cell-retry'])
def test_shared_execution_returns_fixable_rejection(source, tag):
    class NeverExecute:
        async def execute(self, *args, **kwargs):
            raise AssertionError('poisoning code reached the kernel')
    result = asyncio.run(jm._safe_execute(NeverExecute(), source, tag=tag, log_fn=lambda s: None))
    assert result['status'] == 'error' and result['code_executed'] is False
    assert 'BLOCKED' in format_outputs(result['outputs'])


@pytest.mark.parametrize('source', [
    'from aidp_compat import dbutils', 'from pyspark.sql import functions as F',
    'def helper():\n    sc = 1\n    return sc', 'print(sc.applicationId)',
    '# sc = spark.sparkContext\n# Oracle tool modification: use provided sc.\nprint(sc.applicationId)',
    '# sc = SparkContext.getOrCreate()\n# Oracle tool modification: use provided sc.\nprint(sc.applicationId)',
    '%scala\nval sc = 42', '%sql\nSELECT 1 AS sc',
])
def test_legitimate_customer_code_reaches_execution(source):
    calls = []
    class Kernel:
        async def execute(self, code, **kwargs):
            calls.append(code)
            return {'status': 'ok', 'outputs': []}
    assert asyncio.run(jm._safe_execute(Kernel(), source))['status'] == 'ok'
    assert calls == [source]


PRELUDE_ERROR = ('Cell In[49], line 1\n'
                 '----> 1 sc.setJobGroup("fixture", "fixture")\n'
                 "AttributeError: 'function' object has no attribute 'setJobGroup'")


@pytest.mark.parametrize('output_type', ['stream', 'display_data', 'error'])
@pytest.mark.parametrize('color', [False, True])
def test_transport_preserves_prelude_failure_despite_ok_reply(output_type, color, capsys):
    session = object.__new__(AIDPSession)
    pending = {'event': threading.Event(), 'result': {}}
    session._pending = {'fixture': pending}
    text = PRELUDE_ERROR
    if color:
        text = '\x1b[31m' + text.replace('AttributeError:', '\x1b[31mAttributeError:\x1b[0m') + '\x1b[0m'
    if output_type == 'stream':
        output = {'type': 'stream', 'text': text}
    elif output_type == 'display_data':
        output = {'type': 'display_data', 'data': {'text/plain': text}}
    else:
        output = {'type': 'error', 'ename': 'AttributeError', 'evalue': 'setJobGroup',
                  'traceback': text.splitlines()}
    session._outputs = {'fixture': [output]}
    session._handle_message({'header': {'msg_type': 'execute_reply'},
                            'parent_header': {'msg_id': 'fixture'},
                            'content': {'status': 'ok'}})
    result = pending['result']['result']
    assert pending['event'].is_set() and result['status'] == 'error'
    assert 'status=error wire_status=ok' in capsys.readouterr().out
    assert result['outputs'][-1]['ename'] == 'KernelPreexecutionError'
    assert 'function' in format_outputs(result['outputs'])
    class Reply:
        async def execute(self, *args, **kwargs): return result
    with pytest.raises(RuntimeError, match='setJobGroup'):
        asyncio.run(jm._seed_task_validation_widgets(
            Reply(), SimpleNamespace(log=lambda s: None), reset=False))


def test_tracebackless_errors_and_non_platform_prose():
    assert format_outputs([{'type': 'error', 'ename': 'TypeError', 'evalue': 'original error'}]) == 'TypeError: original error'
    assert _kernel_prelude_error([{'type': 'stream', 'text': 'print(sc.setJobGroup)'}]) is None
    assert _kernel_prelude_error([{'type': 'stream', 'text': 'AttributeError: ordinary error'}]) is None



@pytest.mark.parametrize('run_all', [False, True])
def test_initial_migration_receives_kernel_context(monkeypatch, tmp_path, run_all):
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    nb = {'cells': [{'cell_type': 'code', 'source': [ORIGINAL], 'metadata': {},
                     'outputs': [], 'execution_count': None}],
          'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, nb)
    # This cell has no Databricks/storage markers. Do not force a model call:
    # the kernel safety routing itself must defeat the as-is shortcut.
    assert not jm._has_databricks_markers(ORIGINAL)
    delivered = []
    async def migrate(system_prompt, user_prompt, **kwargs):
        delivered.append(system_prompt)
        assert 'sc.setJobGroup(...) BEFORE each submitted' in system_prompt
        assert 'known parent /' in system_prompt and 'dependency notebooks' in system_prompt
        assert 'Do not rename legitimate SparkContext reads' in system_prompt
        assert 'Do not reset sc after the cell' in system_prompt
        assert ORIGINAL.rstrip() in user_prompt
        return REPAIRED, {'input': 0, 'output': 0}
    monkeypatch.setattr(jm, 'call_opus_with_tools', migrate)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/example/source.ipynb', kernel, 'fixture', 'task', {}, run_all=run_all))
    assert result['status'] == ('PASS' if run_all else 'CONVERTED_NOT_VALIDATED'), result
    assert delivered == [jm.CELL_MIGRATE_PROMPT]
    saved = next(json.loads(t) for p, t in kernel.files.items() if p.endswith('.ipynb'))
    assert 'def format_cell' in ''.join(saved['cells'][0]['source'])


def test_parent_execution_sends_collision_to_model(monkeypatch, tmp_path):
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    nb = {'cells': [{'cell_type': 'code', 'source': [ORIGINAL], 'metadata': {},
                     'outputs': [], 'execution_count': None}],
          'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, nb)
    # If initial migration returns the unsafe source unchanged, the execution
    # backstop must still feed it to the bounded repair loop without running it.
    async def unchanged_migration(*args, **kwargs):
        return ORIGINAL, {'input': 0, 'output': 0}
    monkeypatch.setattr(jm, 'call_opus_with_tools', unchanged_migration)
    feedback = []
    sc = SimpleNamespace(setJobGroup=lambda *args: None)
    namespace = {'sc': sc}
    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            namespace['sc'].setJobGroup('fixture', 'fixture')
            if '# CHILD_REPLAY_COLLISION' in code and 'base64.b64decode' not in code:
                exec(code, namespace)
            return await super().execute(code, **kwargs)
    async def fix(code, error, *args, **kwargs):
        feedback.append(error)
        assert namespace['sc'] is sc
        assert 'BLOCKED before execution' in error
        return REPAIRED
    monkeypatch.setattr(jm, 'call_fix', fix)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/example/source.ipynb', kernel, 'fixture', 'task', {}))
    assert result['status'] == 'PASS', result
    assert len(feedback) == 1
    assert namespace['sc'] is sc and namespace['hdrs'](4) == 8
    saved = next(json.loads(t) for p, t in kernel.files.items() if p.endswith('.ipynb'))
    assert 'def format_cell' in ''.join(saved['cells'][0]['source'])


def test_replay_sends_collision_to_model_and_retains_repair(monkeypatch):
    import agent_migrate as am
    monkeypatch.setattr(am, '_cell_history', [])
    namespace = {'sc': SimpleNamespace(setJobGroup=lambda *args: None)}
    context = namespace['sc']
    source = [ORIGINAL]
    feedback = []
    class Kernel:
        async def execute(self, code, **kwargs):
            namespace['sc'].setJobGroup('fixture', 'fixture')
            exec(code, namespace)
            return {'status': 'ok', 'outputs': []}
    async def fix(code, error, *args, **kwargs):
        feedback.append(error)
        assert 'BLOCKED before execution' in error
        return REPAIRED
    monkeypatch.setattr(jm, 'call_fix', fix)
    result = asyncio.run(jm._replay_cell_entry(
        {'notebook_path': '/Workspace/example/parent.ipynb', 'cell_idx': 0,
         'final_code': ORIGINAL, 'replay_code': ORIGINAL,
         '_persist_active': True, '_persist_source': source},
        'repair kernel name collision', Kernel(), log_fn=lambda s: None))
    assert result['status'] == 'ok', result
    assert len(feedback) == 1
    assert namespace['sc'] is context and namespace['hdrs'](4) == 8
    assert 'def format_cell' in ''.join(source)



@pytest.mark.parametrize('source', [
    'def helper():\n    sc = 1\n    return sc',
    'print(sc.applicationId)',
])
def test_safe_no_marker_cells_keep_as_is_shortcut(monkeypatch, tmp_path, source):
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    nb = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                     'outputs': [], 'execution_count': None}],
          'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, nb)
    # _patch_common makes an unexpected model call fail loudly.
    result = asyncio.run(jm.process_notebook(
        '/Workspace/example/source.ipynb', _Kernel(), 'fixture', 'task', {}, run_all=False))
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result


@pytest.mark.parametrize('binding', [
    'sc = spark.sparkContext', 'sc = SparkContext.getOrCreate()',
])
@pytest.mark.parametrize('mode', ['code-only', 'initial-migration', 'repair', 'unchanged-repair'])
def test_sparkcontext_rederivation_uses_existing_context(monkeypatch, tmp_path, binding, mode):
    """Review #278: model guidance removes redundant binding, never SparkContext users.

    Both observed idioms go through the real initial-migration or bounded fix
    path. An unchanged model reply stays blocked; no semantic allowlist is added.
    """
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    source = '# SPARK_CONTEXT_REDERIVATION\n' + binding + '\nanswer = sc.applicationId\n'
    repaired = ('# SPARK_CONTEXT_REDERIVATION\n# ' + binding + '\n'
                '# Oracle tool modification: sc is already supplied by AIDP.\n'
                'answer = sc.applicationId\n')
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                           'outputs': [], 'execution_count': None}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    context = SimpleNamespace(applicationId='fixture-application', setJobGroup=lambda *args: None)
    namespace = {'sc': context}
    delivered, feedback, executed = [], [], []

    def assert_guidance(text):
        assert 'sc = spark.sparkContext' in text and 'sc = SparkContext.getOrCreate()' in text
        assert 'redundant assignment' in text and 'existing sc' in text
        assert 'Oracle tool modification' in text
        assert 'consumers unchanged' in text

    async def migrate(system_prompt, user_prompt, **kwargs):
        assert_guidance(system_prompt)
        assert source.rstrip() in user_prompt
        delivered.append(system_prompt)
        code = source if mode in ('repair', 'unchanged-repair') else repaired
        return code, {'input': 0, 'output': 0}

    async def fix(code, error, *args, **kwargs):
        assert_guidance(error)
        assert namespace['sc'] is context
        feedback.append(error)
        return source if mode == 'unchanged-repair' else repaired

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            namespace['sc'].setJobGroup('fixture', 'fixture')
            if '# SPARK_CONTEXT_REDERIVATION' in code and 'base64.b64decode' not in code:
                # Any remaining active assignment would reference missing spark /
                # SparkContext here. The model must use the supplied sc instead.
                exec(code, namespace)
                executed.append(code)
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(jm, 'call_opus_with_tools', migrate)
    monkeypatch.setattr(jm, 'call_fix', fix)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/example/source.ipynb', kernel, 'fixture', 'task', {},
        run_all=mode != 'code-only'))
    assert delivered == [jm.CELL_MIGRATE_PROMPT]
    assert namespace['sc'] is context
    if mode == 'unchanged-repair':
        assert result['status'] != 'PASS', result
        assert 1 <= len(feedback) <= jm.MAX_FIX_ATTEMPTS
        assert not executed and 'answer' not in namespace
    else:
        assert result['status'] == ('CONVERTED_NOT_VALIDATED' if mode == 'code-only' else 'PASS'), result
        assert len(feedback) == (1 if mode == 'repair' else 0)
        saved = next(json.loads(t) for p, t in kernel.files.items() if p.endswith('.ipynb'))
        saved_source = ''.join(saved['cells'][0]['source'])
        assert '# ' + binding in saved_source
        assert 'answer = sc.applicationId' in saved_source
        assert jm._customer_kernel_global_rejection(saved_source) is None
        if mode == 'code-only':
            assert not executed and 'answer' not in namespace
        else:
            assert executed and namespace['answer'] == context.applicationId


@pytest.mark.parametrize('helper_name', ['write_cell', 'format_cell'])
@pytest.mark.parametrize('run_all', [False, True])
def test_caller_migration_receives_existing_dependency_names(monkeypatch, tmp_path, helper_name, run_all):
    """Live 2026-10-22: callers must see the name already saved by dependency conversion."""
    from test_conditional_exit_below_execute import _Kernel, _patch_common
    dependency = '/Workspace/test_cond/notebooks/helper.ipynb'
    source = ("# CALL_EXISTING_HELPER\nREPORT_ROOT = '/dbfs/FileStore/report'\n"
              'def render(value):\n    return sc(value)\nanswer = render(3)\n')
    repaired = ("# CALL_EXISTING_HELPER\n# REPORT_ROOT = '/dbfs/FileStore/report'\n"
                '# Oracle tool modification: translate the report path.\n'
                "REPORT_ROOT = '/Volumes/default/default/dbfs/FileStore/report'\n"
                'def render(value):\n    # return sc(value)\n'
                '    # Oracle tool modification: reuse the migrated dependency helper.\n'
                f'    return {helper_name}(value)\nanswer = render(3)\n')
    helper_source = f'# KNOWN_DEPENDENCY_HELPER\ndef {helper_name}(value):\n    return value * 2\n'
    def notebook(*sources):
        return {'cells': [{'cell_type': 'code', 'source': [code], 'metadata': {},
                           'outputs': [], 'execution_count': None} for code in sources],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, notebook('%run ' + dependency, source))
    monkeypatch.setitem(jm._notebook_content_cache, dependency, json.dumps(notebook(helper_source)))
    # The original-source download stub carries no migrated definition. The
    # real scope preparation must supply the converted artifact's facts.
    delivered = []
    async def migrate(system_prompt, user_prompt, **kwargs):
        assert 'do not independently invent another name per cell' in system_prompt
        assert 'RESOLVED MIGRATED DEPENDENCY SOURCES' in user_prompt
        assert dependency in user_prompt
        assert helper_name in user_prompt
        delivered.append(user_prompt)
        return repaired, {'input': 0, 'output': 0}
    monkeypatch.setattr(jm, 'call_opus_with_tools', migrate)
    context = SimpleNamespace(setJobGroup=lambda *args: None)
    namespace = {'sc': context}
    executed = []
    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            namespace['sc'].setJobGroup('fixture', 'fixture')
            if ('# KNOWN_DEPENDENCY_HELPER' in code or '# CALL_EXISTING_HELPER' in code) and 'base64.b64decode' not in code:
                exec(code, namespace)
                executed.append(code)
            return await super().execute(code, **kwargs)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/example/source.ipynb', kernel, 'fixture', 'task', {},
        run_all=run_all, dep_path_map={dependency: dependency}))
    assert result['status'] == ('PASS' if run_all else 'CONVERTED_NOT_VALIDATED'), result
    assert len(delivered) == 1
    assert namespace['sc'] is context
    saved = next(json.loads(t) for p, t in kernel.files.items() if p.endswith('.ipynb'))
    assert f'return {helper_name}(value)' in ''.join(saved['cells'][1]['source'])
    if run_all:
        assert namespace['answer'] == 6 and len(executed) == 2
    else:
        assert not executed and 'answer' not in namespace
