"""Issue #194: Hudi guidance reaches the repair loop and saved consumer.

The model replies here are fixtures, not proof of LLM behavior. Live evidence
must show the real model repairing the marker failure with this product prompt.
"""
import asyncio
import json
from types import SimpleNamespace as NS

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm
import agent_migrate as am


@pytest.mark.parametrize("repair", [True, False])
@pytest.mark.parametrize("provider,load", [
    ("hudi", 'f"{base_path}/*"'),
    ("org.apache.hudi", 'base_path + "/*"'),
])
def test_marker_repair_reaches_saved_notebook(monkeypatch, tmp_path, repair, provider, load):
    source = (
        '# HUDI_REPAIR_FIXTURE\n'
        'def read_hudi(spark, base_path, partition=None):\n'
        f'    reader = spark.read.format("{provider}")\n'
        '    reader = reader.option("hoodie.datasource.read.basepath", base_path)\n'
        '    if partition is not None:\n'
        '        return reader.load(partition)\n'
        f'    return reader.load({load})\n'
        'result = read_hudi(spark, "/fixture/table")\n'
        'assert result == "row"\n'
    )
    fixed = source.replace(
        f'    return reader.load({load})',
        f'    # return reader.load({load})\n'
        '    # Oracle tool modification: read the declared Hudi root\n'
        '    return reader.load(base_path)',
    )
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                           'outputs': [], 'execution_count': None}], 'metadata': {},
                'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, 'call_fix', am.call_fix)
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 2)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', True)
    monkeypatch.setattr(jm, 'fetch_recent_spark_errors', lambda *a, **k: '')
    monkeypatch.setattr(am, '_cell_history', [])
    monkeypatch.setattr(jm, '_cell_history', am._cell_history)
    monkeypatch.setattr(am, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_current_cell_notes', am._current_cell_notes)
    monkeypatch.setattr(am, 'FIX_PROMPT', am.FIX_PROMPT)
    am._rebake_fix_prompt()  # YAML cap rebaking must retain the guidance.
    requests = []

    def create(**kwargs):
        requests.append(kwargs)
        return NS(content=[NS(type='tool_use', name='submit_code', id='repair',
                              input={'code': fixed if repair else source})],
                  stop_reason='tool_use', usage=NS(input_tokens=1, output_tokens=1))

    monkeypatch.setattr(am, 'claude', lambda: NS(messages=NS(create=create)))

    class Reader:
        def format(self, value):
            assert value == provider
            return self

        def option(self, key, value):
            assert (key, value) == ('hoodie.datasource.read.basepath', '/fixture/table')
            return self

        def load(self, path):
            if path.endswith('/*'):
                raise RuntimeError('HoodieException: Error checking path: date=20200422_$folder$')
            assert path == '/fixture/table'
            return 'row'

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if '# HUDI_REPAIR_FIXTURE' in code and 'base64.b64decode' not in code:
                self.executed.append(code)
                try:
                    exec(code, {'spark': NS(read=Reader())})
                except Exception as exc:
                    return {'status': 'error', 'outputs': [
                        {'type': 'error', 'ename': type(exc).__name__, 'evalue': str(exc),
                         'traceback': [str(exc)]}]}
                return {'status': 'ok', 'outputs': []}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/source.ipynb', kernel, 'hudi_fixture', 'hudi_read', {}, run_all=True))
    assert requests, 'the real call_fix/model boundary was not exercised'
    for request in requests:
        prompt = '\n'.join(block['text'] for block in request['system'])
        assert 'HUDI DECLARED-ROOT READS' in prompt
        assert 'hoodie.datasource.read.basepath' in prompt
        assert 'date/partition-specific globs' in prompt
        assert 'Genuine missing data, authorization,' in prompt
        assert 'folder$' in str(request['messages'])
    assert result['status'] == ('PASS' if repair else 'FAIL'), result
    if repair:
        saved = next(json.loads(value) for path, value in kernel.files.items()
                     if path.endswith('.ipynb'))
        delivered = '\n'.join(''.join(c['source']) for c in saved['cells'])
        assert 'return reader.load(partition)' in delivered
        # Consume the delivered code with a new namespace, not repaired state.
        exec(delivered, {'spark': NS(read=Reader())})
    else:
        assert result['failed'] > 0
