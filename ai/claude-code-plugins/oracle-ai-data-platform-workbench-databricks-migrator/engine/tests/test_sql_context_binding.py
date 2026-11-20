"""AutoRefit 2026-10-08: missing sqlContext must reach repair, not parameter halt.

Keep the original binding/preamble preservation lessons, but no longer assert
deterministic initialization. Drive the real parent and child retry loops.
"""
import asyncio
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm


@pytest.mark.parametrize("source", [
    'df = sqlContext.read.format("org.apache.hudi").load(path)\n',
    'def read():\n    return sqlContext.read.table("t")\n',
    'if True:\n    df = sqlContext.read.table("t")\n',
])
def test_global_usage_not_preemptively_rewritten(source):
    result = jm._add_missing_imports(source)
    assert result == source
    assert jm._add_missing_imports(result) == result


@pytest.mark.parametrize("source", [
    '# sqlContext.read.table("t")\n',
    'text = "sqlContext.read"\n',
    'df = obj.sqlContext.read\n',
    'def read(sqlContext):\n    return sqlContext.read\n',
    'def read():\n    sqlContext = custom\n    return sqlContext.read\n',
    '%scala\nval df = sqlContext.read\n',
    '%sql\nselect sqlContext from t\n',
    '%run ./sqlContext\n',
])
def test_unrelated_source_unchanged(source):
    assert jm._add_missing_imports(source) == source


@pytest.mark.parametrize("prefix", [
    '', '%python\n', '%%time\n', '%python\n%%time\n',
    '"""Module docstring."""\nfrom __future__ import annotations\n',
])
def test_preamble_preserved(prefix):
    source = prefix + 'df = sqlContext.read\n'
    assert jm._add_missing_imports(source) == source


def test_existing_context_not_overwritten():
    source = jm._add_missing_imports('result = sqlContext\n')
    value = object()
    env = {'sqlContext': value}
    exec(source, env)
    assert env['result'] is value
    with pytest.raises(NameError):
        exec(source, {})  # No fabricated binding: execution supplies feedback.


@pytest.mark.parametrize('symbol', ['customer_parameter', 'SQLContext', 'sqlcontext'])
def test_genuine_missing_names_still_fail_fast(symbol):
    verdict = jm._check_name_error_can_recover(
        f"NameError: name '{symbol}' is not defined", {'defined': set()})
    assert verdict['kind'] == 'PARAMETER_MISSING'
    assert verdict['symbol'] == symbol


@pytest.mark.parametrize('defined', [set(), {'sqlContext'}])
def test_runtime_binding_is_recoverable_with_or_without_static_definition(defined):
    assert jm._check_name_error_can_recover(
        "NameError: name 'sqlContext' is not defined", {'defined': defined}) is None


def _drive(monkeypatch, tmp_path, *, path, repaired, symbol='sqlContext',
           repair_error=None, attempts=3):
    from test_pip_magic_gate import _Kernel, _patch_common

    source = f'result = {symbol}.read.table("fixture_table")\n'
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                          'outputs': [], 'execution_count': None}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    repairs, feedback = [], []

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if (repair_error and 'result = spark.read.table(' in code
                    and 'base64.b64decode' not in code):
                self.executed.append(code)
                return {'status': 'error', 'outputs': [{
                    'type': 'error', 'ename': repair_error,
                    'evalue': 'repaired reader still fails',
                    'traceback': [f'{repair_error}: repaired reader still fails']}]}
            if (re.search(rf'(?m)^result = {symbol}\.read\.', code)
                    and 'base64.b64decode' not in code):
                self.executed.append(code)
                return {'status': 'error', 'outputs': [{
                    'type': 'error', 'ename': 'NameError',
                    'evalue': f"name '{symbol}' is not defined",
                    'traceback': [f"NameError: name '{symbol}' is not defined"]}]}
            return await super().execute(code, **kwargs)

    async def fix(code, output, *args, **kwargs):
        repairs.append(code)
        feedback.append(output)
        # This is a simulated MODEL choice, not a production rewrite rule.
        return ('# ' + source.strip() + '\n'
                '# Oracle tool modification: use existing SparkSession reader\n'
                'result = spark.read.table("fixture_table")\n') if repaired else code

    async def summarize(*args, **kwargs):
        return 'fixture read'

    async def monitor(*args, **kwargs):
        return 'local', 'fixture NameError'

    monkeypatch.setattr(jm, 'call_fix', fix)
    monkeypatch.setattr(jm, '_summarize_cell_code', summarize)
    monkeypatch.setattr(jm, 'monitor_analyze_failures', monitor)
    monkeypatch.setattr(jm, 'fetch_recent_spark_errors', lambda *a, **k: '')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_inlined_nb_run_children', set())
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', attempts)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', False)
    kernel = Kernel()
    if path == 'parent':
        result = asyncio.run(jm.process_notebook(
            '/Workspace/test_pip/source.ipynb', kernel, 'test_pip', '01_task', {}))
    else:
        child = '/Workspace/test_pip/notebooks/child.ipynb'
        jm._notebook_content_cache[child] = json.dumps(notebook)
        result = asyncio.run(jm._inline_child_notebook(
            child, kernel, jm.NotebookLog(child, 'test_pip', 'child'),
            'test_pip', '', '', '', {}))
    return result, repairs, feedback, kernel


@pytest.mark.parametrize('path', ['parent', 'child'])
def test_actual_repair_loop_receives_error_and_reexecutes(monkeypatch, tmp_path, path):
    result, repairs, feedback, kernel = _drive(
        monkeypatch, tmp_path, path=path, repaired=True)
    assert len(repairs) == 1
    assert all("NameError: name 'sqlContext' is not defined" in x for x in feedback)
    assert result['status'] == ('PASS' if path == 'parent' else 'ok'), result
    assert any('result = spark.read.table("fixture_table")' in x
               and 'base64.b64decode' not in x for x in kernel.executed)
    saved = '\n'.join(kernel.files.values() if path == 'parent'
                      else jm._notebook_content_cache.values())
    assert 'spark.read.table' in saved
    assert 'SQLContext.getOrCreate' not in saved


@pytest.mark.parametrize('path', ['parent', 'child'])
def test_unsuccessful_model_repair_remains_bounded_and_not_pass(monkeypatch, tmp_path, path):
    result, repairs, feedback, _ = _drive(
        monkeypatch, tmp_path, path=path, repaired=False)
    assert 1 <= len(repairs) <= 3
    assert len(feedback) == len(repairs)
    assert result['status'] not in ('PASS', 'ok'), result
    assert result.get('kind') != 'PARAMETER_MISSING'


@pytest.mark.parametrize('path', ['parent', 'child'])
def test_missing_customer_parameter_does_not_reach_repair(monkeypatch, tmp_path, path):
    result, repairs, _, _ = _drive(
        monkeypatch, tmp_path, path=path, repaired=True, symbol='customer_parameter')
    assert repairs == []
    assert result['status'] not in ('PASS', 'ok'), result


@pytest.mark.parametrize('repair_error', ['RuntimeError', 'TypeError'])
def test_child_sqlcontext_error_changing_repair_cannot_return_ok(
        monkeypatch, tmp_path, repair_error):
    result, repairs, feedback, kernel = _drive(
        monkeypatch, tmp_path, path='child', repaired=True,
        repair_error=repair_error)
    assert len(repairs) == 2  # Three executions, bounded model-repair budget.
    assert "NameError: name 'sqlContext' is not defined" in feedback[0]
    assert repair_error in feedback[-1]
    assert result['status'] == 'error', result
    assert result['kind'] == 'CHILD_EXECUTION'
    assert result['failed_cell'] == 0
    assert result['attempts'] == 3
    assert any('result = spark.read.table(' in code for code in kernel.executed)


def test_child_sqlcontext_exhaustion_with_one_attempt_still_fails(monkeypatch, tmp_path):
    result, repairs, _, _ = _drive(
        monkeypatch, tmp_path, path='child', repaired=False, attempts=1)
    assert repairs == []
    assert result['status'] == 'error'
    assert result['kind'] == 'CHILD_EXECUTION'
