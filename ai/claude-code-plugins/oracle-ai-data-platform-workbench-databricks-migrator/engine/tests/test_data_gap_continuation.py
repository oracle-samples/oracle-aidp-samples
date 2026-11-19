"""Run c77c44c6: a recorded data gap must gate every later execution path.

Drive process_notebook, including its real dispatch, counters and artifact saves.
Only the kernel, catalog and model boundary are faked; no new error classifier.
"""
import asyncio

import pytest

from test_attempt_loop_terminal_result import _SequenceKernel, _error
from test_conditional_exit_below_execute import _patch_common, _saved_sources
from test_eval_verdict_gate import _notebook
import job_migrate as jm


TAIL = (
    '%scala\nval SENTINEL_SCALA = spark.conf.get("baseS3Path")\n',
    '%sql\nSELECT * FROM SENTINEL_SQL\n',
    '%pip install SENTINEL_PACKAGE\n',
    '%run /Workspace/test/SENTINEL_CHILD\n',
    'SENTINEL_PYTHON = 1\n',
    'SENTINEL_CALL = dbutils.notebook.run("/Workspace/test/child", 60)\n',
    '%scala\nval SENTINEL_CALL_SCALA = dbutils.notebook.run("/Workspace/test/child", 60)\n',
)


@pytest.mark.parametrize('origin', ['python_note', 'sql_table', 'scala_table', 'child_table'])
@pytest.mark.parametrize('strict', [False, True])
@pytest.mark.parametrize('native_scala', [False, True])
def test_data_gap_stops_all_later_dispatch(monkeypatch, tmp_path, origin, strict, native_scala):
    source = {
        'python_note': 'BLOCKED_LOOKUP = get_table_location()\n',
        'sql_table': '%sql\nSELECT * FROM BLOCKED_LOOKUP\n',
        'scala_table': '%scala\nval BLOCKED_LOOKUP = spark.table("missing.table")\n',
        'child_table': '%run /Workspace/test/BLOCKED_CHILD.ipynb\n',
    }[origin]
    notebook = _notebook(source, *TAIL)
    if native_scala:
        notebook['metadata'] = {'language_info': {'name': 'scala'}}
        if origin == 'python_note':
            notebook['cells'][0]['source'] = ['%python\n' + source]
        notebook['cells'][5]['source'] = ['%python\n' + TAIL[4]]
        notebook['cells'][6]['source'] = ['%python\n' + TAIL[5]]
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', strict)
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 2)
    if native_scala and origin == 'python_note':
        # The existing native-notebook conversion path asks the model to keep
        # this explicit Python cell. Preserve it; exercise classification below.
        async def migrate(*a, **k):
            assert 'BLOCKED_LOOKUP' in a[1]
            return '%python\n' + source, {'input': 0, 'output': 0}
        monkeypatch.setattr(jm, 'call_opus_with_tools', migrate)
    fixes = []

    async def fix(code, *a, **k):
        fixes.append(code)
        if origin == 'python_note':
            jm._current_cell_notes.append('DATA_UNAVAIL: required catalog tables unavailable')
        return code

    async def no_inline(child_path, *a, **k):
        if origin == 'child_table' and 'BLOCKED_CHILD' in child_path:
            return {'status': 'error', 'kind': 'DATA_UNAVAILABLE',
                    'error': 'required table unavailable', 'cells_ok': 0, 'cells_fixed': 0}
        pytest.fail('child executed after upstream data gap')

    monkeypatch.setattr(jm, 'call_fix', fix)
    monkeypatch.setattr(jm, '_inline_child_notebook', no_inline)
    kernel = _SequenceKernel({'BLOCKED_LOOKUP': [
        _error('[TABLE_OR_VIEW_NOT_FOUND] missing.table', 'AnalysisException')]})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {}))

    assert result['status'] == 'PARTIAL_DATA_UNAVAILABLE', result
    assert result['data_unavailable'] == 1
    assert result['failed'] == 0
    assert result['cell_results'][0]['status'] == 'data_unavailable'
    assert all('BLOCKED_LOOKUP' in code for code in fixes)
    direct = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'SENTINEL_' not in direct
    if strict:
        assert result['job_should_stop']
        assert len(result['cell_results']) == 1
    else:
        # The task summary intentionally keeps only entries with notes. Inspect
        # the complete persisted report for the per-cell continuation verdicts.
        report = next(text for path, text in kernel.files.items()
                      if path.endswith('test_report.md'))
        assert report.count(' - MIGRATED_NOT_VALIDATED') == len(TAIL)
        assert f'| Migrated code-only (after data-unavailable) | {len(TAIL)} |' in report
        saved, nb = _saved_sources(kernel)
        assert len(nb['cells']) == len(notebook['cells'])
        for marker in ('SENTINEL_SCALA', 'SENTINEL_SQL', 'SENTINEL_PACKAGE',
                       'SENTINEL_CHILD', 'SENTINEL_PYTHON', 'SENTINEL_CALL'):
            assert marker in saved
        assert 'CONVERTED_NOT_VALIDATED_BELOW_EXIT' not in report


def test_ordinary_scala_error_still_repairs_and_executes_tail(monkeypatch, tmp_path):
    notebook = _notebook('%scala\nval RETRY_ME = broken\n', 'TAIL_OK = 1\n')
    _patch_common(monkeypatch, tmp_path, notebook)
    kernel = _SequenceKernel({'RETRY_ME': [_error('not found: value broken'),
                                         {'status': 'ok', 'outputs': []}]})
    fixes = []

    async def fix(code, *a, **k):
        fixes.append(code)
        # Notes alone must not gate a later cell after a successful repair.
        jm._current_cell_notes.append('DATA_UNAVAIL: stale tentative diagnosis')
        return code

    monkeypatch.setattr(jm, 'call_fix', fix)
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {}))
    assert result['status'] == 'PASS', result
    assert result['data_unavailable'] == 0
    assert kernel.runs['RETRY_ME'] == 2
    assert len(fixes) == 1
    assert any('TAIL_OK = 1' in c for c in kernel.executed)


def test_existing_non_data_validation_stop_keeps_its_attribution(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path, _notebook('BAD = unknown_input\n', *TAIL))
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    kernel = _SequenceKernel({'BAD =': [
        _error("name 'unknown_input' is not defined", 'NameError')]})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {}))
    assert result['status'] == 'FAIL', result
    assert result['failed'] == 1 and result['data_unavailable'] == 0
    direct = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'SENTINEL_' not in direct
    report = next(text for path, text in kernel.files.items()
                  if path.endswith('test_report.md'))
    assert report.count(' - MIGRATED_NOT_VALIDATED') == len(TAIL)
    log = next(text for path, text in kernel.files.items()
               if path.endswith('execution_log.txt'))
    assert 'task validation halted earlier' in log
    assert 'upstream data unavailable' not in log


@pytest.mark.parametrize('model_error', [None, RuntimeError('model unavailable')])
@pytest.mark.parametrize('path_cell', [
    'PATH_SENTINEL = "s3://fixture-bucket/input"\n',
    'API_SENTINEL = boto3.client("glue")\n',
])
def test_data_gap_converts_python_without_execution_tools(monkeypatch, tmp_path, model_error, path_cell):
    marker_check = jm._has_databricks_markers
    _patch_common(monkeypatch, tmp_path, _notebook('BLOCKED_LOOKUP = 1\n', path_cell))
    monkeypatch.setattr(jm, '_has_databricks_markers', marker_check)
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 2)

    async def fix(code, *a, **k):
        jm._current_cell_notes.append('DATA_UNAVAIL: required catalog tables unavailable')
        return code

    model_calls = []

    async def model(prompt, content, **kwargs):
        model_calls.append((content, kwargs))
        if model_error:
            raise model_error
        return path_cell, {'input': 0, 'output': 0}

    monkeypatch.setattr(jm, 'call_fix', fix)
    monkeypatch.setattr(jm, 'call_opus_with_tools', model)
    kernel = _SequenceKernel({'BLOCKED_LOOKUP': [_error('catalog lookup failed')]})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {}))
    assert result['status'] == 'PARTIAL_DATA_UNAVAILABLE', result
    assert result['data_unavailable'] == 1 and result['failed'] == 0
    assert len(model_calls) == 1
    content, kwargs = model_calls[0]
    assert 'CODE-ONLY: upstream data unavailable' in content
    assert set(jm._TOOLS_THAT_RUN_CODE) <= set(kwargs['exclude_tools'])
    direct = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'PATH_SENTINEL' not in direct and 'API_SENTINEL' not in direct
    saved, _ = _saved_sources(kernel)
    assert path_cell.strip() in saved
    report = next(text for path, text in kernel.files.items()
                  if path.endswith('migration_report.md'))
    if 's3://' in path_cell:
        assert 'UNRESOLVED storage paths' in report
    assert 'notebook.exit' not in report


def test_dependency_code_only_mode_does_not_acquire_a_data_gap(monkeypatch, tmp_path):
    notebook = _notebook('CONVERT_ONLY_PYTHON = 1\n', '%scala\nval CONVERT_ONLY_SCALA = 1\n')
    _patch_common(monkeypatch, tmp_path, notebook)
    kernel = _SequenceKernel({})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {},
        run_all=False))
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
    assert result['data_unavailable'] == 0 and result['failed'] == 0
    direct = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'CONVERT_ONLY_' not in direct


@pytest.mark.parametrize('strict', [False, True])
@pytest.mark.parametrize('tail', [
    'DIRECT_PATH = "s3://fixture-bucket/input"\n',
    'DIRECT_API = boto3.client("glue")\n',
])
def test_direct_execute_data_gap_never_calls_model(monkeypatch, tmp_path, strict, tail):
    marker_check = jm._has_databricks_markers
    _patch_common(monkeypatch, tmp_path, _notebook(
        '%sql\nSELECT * FROM DIRECT_MISSING_TABLE\n', tail, *TAIL))
    monkeypatch.setattr(jm, '_has_databricks_markers', marker_check)
    monkeypatch.setattr(jm, 'DIRECT_EXECUTE', True)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', strict)
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_registry_cache', {'test_data_gap': {}})

    async def forbidden(*a, **k):
        pytest.fail('direct-execute data-gap continuation called a model or child')

    monkeypatch.setattr(jm, 'call_opus_with_tools', forbidden)
    monkeypatch.setattr(jm, 'call_fix', forbidden)
    monkeypatch.setattr(jm, '_inline_child_notebook', forbidden)
    kernel = _SequenceKernel({'DIRECT_MISSING_TABLE': [
        _error('[TABLE_OR_VIEW_NOT_FOUND] direct_missing_table', 'AnalysisException')]})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'test_data_gap', '01_task', {}))
    assert result['status'] == 'PARTIAL_DATA_UNAVAILABLE', result
    assert result['data_unavailable'] == 1 and result['failed'] == 0
    direct = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert 'DIRECT_PATH' not in direct and 'DIRECT_API' not in direct
    assert 'SENTINEL_' not in direct
    if strict:
        assert result['job_should_stop'] and len(result['cell_results']) == 1
    else:
        saved, _ = _saved_sources(kernel)
        assert tail.strip() in saved
        report = next(text for path, text in kernel.files.items()
                      if path.endswith('test_report.md'))
        assert report.count(' - MIGRATED_NOT_VALIDATED') == 1 + len(TAIL)
        if 's3://' in tail:
            migration = next(text for path, text in kernel.files.items()
                             if path.endswith('migration_report.md'))
            assert 'UNRESOLVED storage paths' in migration
