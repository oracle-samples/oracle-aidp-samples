"""Conversion-only regression for the Scala date-decoding failure in run cccc0271.

Exercise process_notebook -> call_fix -> execute -> save with scripted model and
kernel responses. This pins orchestration, not the quality of a real model reply.
Run the live companion separately to validate actual Scala and model behaviour.
"""
import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import agent_migrate as am
import job_migrate as jm
from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources
from test_eval_verdict_gate import _notebook

ORIGINAL = '''%scala
val latestDate = js1.withColumn("date", col("date").cast(DateType))
  .agg(max("date").alias("latestDate"))
  .as[java.sql.Date].collect()(0).toString
'''
ERROR = ('Command ID 1234-abcd failed with java.lang.IllegalAccessError: '
         'SparkDateTimeUtils cannot access sun.util.calendar.ZoneInfo '
         'because module java.base does not export sun.util.calendar')
REPAIRED = '%scala\n// Original:\n' + '\n'.join(
    '// ' + line for line in ORIGINAL.splitlines()[1:]) + '''
// Oracle tool modification: decode the date string instead of a JVM Date.
val latestDate = js1.withColumn("date", col("date").cast(DateType))
  .agg(max("date").cast(StringType).alias("latestDate"))
  .collect()(0).getString(0)
require(latestDate != null, "No non-null latestDate")
'''

class DateKernel(_Kernel):
    async def execute(self, code, **kwargs):
        if code.lstrip().startswith('%scala') and 'val latestDate' in code:
            self.executed.append(code)
            active = '\n'.join(l for l in code.splitlines()
                               if not l.lstrip().startswith('//'))
            if '.as[java.sql.Date]' in active:
                return {'status': 'ok', 'outputs': [
                    {'type': 'stream', 'text': ERROR}]}
            assert '.cast(StringType)' in active and '.getString(0)' in active
            return {'status': 'ok', 'outputs': [
                {'type': 'stream', 'text': 'latestDate: String = 2026-10-06'}]}
        return await super().execute(code, **kwargs)

@pytest.mark.parametrize('unchanged_first', [False, True])
def test_date_conversion_execute_repair_save(monkeypatch, tmp_path, unchanged_first):
    _patch_common(monkeypatch, tmp_path, _notebook(ORIGINAL))
    seen = []
    async def model(system, user_content, **kwargs):
        assert '```scala\n%scala' in user_content
        assert 'sun.util.calendar.ZoneInfo' in user_content
        assert 'starting with %scala' in user_content
        assert 'cast the aggregate to StringType BEFORE collect()' in system
        assert 'ONLY valid executable Python' not in system
        seen.append(user_content)
        return (ORIGINAL if unchanged_first and len(seen) == 1 else REPAIRED), {}
    monkeypatch.setattr(am, 'call_opus_with_tools', model)
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    kernel = DateKernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'date_conversion', 'task', {}, run_all=True))
    assert result['failed'] == 0, result
    assert result['fixed'] == 1, result
    assert len(seen) == (2 if unchanged_first else 1)
    if unchanged_first:
        assert 'PREVIOUS FAILED SCALA EXECUTIONS' in seen[1]
        assert ORIGINAL.strip() in seen[1]
    saved, _ = _saved_sources(kernel)
    assert REPAIRED.strip() in saved
    # The original failure must execute before the model's repair is accepted.
    assert any(ORIGINAL.strip() in c for c in kernel.executed)


def test_submit_contract_accepts_native_language():
    tool = next(t for t in am.OCI_PATH_TOOLS if t['name'] == 'submit_code')
    assert 'original language' in tool['description']
    assert 'original language' in tool['input_schema']['properties']['code']['description']


def test_exhausted_date_repairs_remain_failed(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path, _notebook(ORIGINAL))
    seen = []
    async def model(system, user_content, **kwargs):
        seen.append(user_content)
        return ORIGINAL, {}
    monkeypatch.setattr(am, 'call_opus_with_tools', model)
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    kernel = DateKernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'date_conversion', 'task', {}, run_all=True))
    assert result['failed'] == 1 and result['fixed'] == 0, result
    assert result['status'] != 'PASS'
    assert len(seen) == 4  # five executions, no unverified repair after the last
    assert 'Attempt 3:' in seen[-1]
    saved, _ = _saved_sources(kernel)
    assert ORIGINAL.strip() in saved


def test_working_date_string_cell_does_not_request_repair(monkeypatch, tmp_path):
    _patch_common(monkeypatch, tmp_path, _notebook(REPAIRED))
    async def unexpected(*args, **kwargs):
        pytest.fail('Working Scala must execute as-is')
    monkeypatch.setattr(jm, 'call_fix', unexpected)
    kernel = DateKernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'date_conversion', 'task', {}, run_all=True))
    assert result['failed'] == 0 and result['fixed'] == 0, result
