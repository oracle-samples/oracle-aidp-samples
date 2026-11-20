"""Run c8fc04e0: valid message backticks must not undo storage conversion.

Drive the real root/code-only/repair loops and check saved code plus the actual
writer destination. Only external services and Spark's writer are simulated.
"""
import asyncio
import contextlib
import io
import json

import pytest

from test_pip_magic_gate import _Kernel, _patch_common, jm
import context_tools  # test_pip_magic_gate adds scripts/ to the test import path


SOURCE_PATH = 's3a://fixture-bucket/missing_transactions/'
OCI_PATH = 'oci://oci-fixture-bucket@fixture_ns/missing_transactions/'
SANDBOX_PATH = 'oci://sandbox@fixture_ns/oci-fixture-bucket/missing_transactions/'
SOURCE = (
    '# FENCE_STORAGE_REPRO\n'
    'if missing_count > 0:\n'
    '    (df.write.mode("append").format("parquet")\n'
    '     .partitionBy("created_at").save("' + SOURCE_PATH + '"))\n'
    '    path_msg = f"*Missing IDs Path:*' + SOURCE_PATH + 'created_at={stamp}```"\n'
    '    send_data_to_slack(comment=path_msg)\n'
)


@pytest.mark.parametrize('mode,response', [
    ('first_attempt', 'valid'), ('code_only', 'valid'),
    ('root_repair', 'valid'), ('child_repair', 'valid'),
    ('root_repair', 'docstring'), ('root_repair', 'comments'),
    ('child_repair', 'docstring'), ('child_repair', 'comments'),
])
def test_storage_conversion_survives_candidate_validation(monkeypatch, tmp_path, mode, response):
    # Rejected repairs retain the already-converted child supplied by Pass 1;
    # unlike the root path, child inlining does not perform the initial rewrite.
    source = SOURCE.replace(SOURCE_PATH, OCI_PATH) if mode == 'child_repair' and response != 'valid' else SOURCE
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                          'outputs': [], 'execution_count': None}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    original_guard = jm._inject_write_guard
    original_markers = jm._has_databricks_markers
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, '_inject_write_guard', original_guard)
    # The S3 path is a Databricks marker, so the root cell meets the model,
    # which converts it (REF-158: Python storage paths are the model's).
    monkeypatch.setattr(jm, '_has_databricks_markers', original_markers)
    migrations = []

    async def migrate(prompt, content, **kwargs):
        # The model gets the cell after Slack neutralisation; convert that.
        cell = content.split('```python\n', 1)[1].rsplit('\n```', 1)[0]
        assert '# FENCE_STORAGE_REPRO' in cell and SOURCE_PATH in cell
        migrations.append(cell)
        return cell.replace(SOURCE_PATH, OCI_PATH), {'input': 0, 'output': 0}

    monkeypatch.setattr(jm, 'call_opus_with_tools', migrate)
    # Earlier tests may leave the mapping cache empty; repair code validates
    # OCI namespaces through this shared index before it can execute.
    monkeypatch.setattr(context_tools, '_BUCKET_NS_INDEX',
                        {'oci-fixture-bucket': 'fixture_ns', 'sandbox': 'fixture_ns'})
    monkeypatch.setattr(jm, '_ns_for_oci_bucket', lambda bucket: 'fixture_ns')
    monkeypatch.setattr(jm, '_REDIRECT_BUCKET', 'sandbox')
    monkeypatch.setattr(jm, '_REDIRECT_NAMESPACE', 'fixture_ns')
    monkeypatch.setattr(jm, '_REDIRECT_TABLE_PREFIX', 'default.sandbox')
    monkeypatch.setattr(jm, 'fetch_recent_spark_errors', lambda *a, **kw: '')
    monkeypatch.setattr(jm, 'MAX_FIX_ATTEMPTS', 2)
    monkeypatch.setattr(jm, '_notebook_content_cache', {})
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_write_redirect_path_map', {})
    monkeypatch.setattr(jm, '_write_redirect_table_map', {})
    monkeypatch.setattr(jm, '_write_redirect_log', [])
    monkeypatch.setattr(jm, '_write_redirect_collisions', [])
    repairs = []

    async def fix(*args, **kwargs):
        repairs.append(args[0])
        if response == 'docstring':
            return '"""\n```python\nexplanation only\n```\n"""'
        if response == 'comments':
            return '# Explanation only\n# ```python\n# ```'
        return SOURCE.replace(SOURCE_PATH, OCI_PATH)

    monkeypatch.setattr(jm, 'call_fix', fix)

    class Frame:
        def __init__(self):
            self.paths = []
            self.write = self

        def mode(self, value):
            assert value == 'append'
            return self

        def format(self, value):
            assert value == 'parquet'
            return self

        def partitionBy(self, value):
            assert value == 'created_at'
            return self

        def save(self, path):
            self.paths.append(path)

    frame = Frame()

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__()
            self.attempts = 0
            self.namespace = {'df': frame, 'missing_count': 1, 'stamp': 'test'}

        async def execute(self, code, **kwargs):
            if '__AIDP_CHILD_COUNT__' in code or "all_cells = payload.get" in code:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': output.getvalue()}]}
            if '# FENCE_STORAGE_REPRO' in code and 'base64.b64decode' not in code:
                self.executed.append(code)
                self.attempts += 1
                if mode.endswith('repair') and self.attempts == 1:
                    return {'status': 'error', 'outputs': [{'type': 'error',
                            'ename': 'RuntimeError', 'evalue': 'fixture repair trigger'}]}
                with contextlib.redirect_stdout(io.StringIO()):
                    exec(code, self.namespace)
                return {'status': 'ok', 'outputs': []}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    if mode == 'child_repair':
        child = tmp_path / 'child.ipynb'
        child.write_text(json.dumps(notebook), encoding='utf-8')
        monkeypatch.setattr(jm, 'MIGRATED_BASE', str(tmp_path))
        jm._notebook_content_cache[str(child)] = json.dumps(notebook)
        log = jm.NotebookLog(str(child), 'fixture', 'task_child')
        result = asyncio.run(jm._inline_child_notebook(
            str(child), kernel, log, 'fixture', '', '', '', {}))
        assert result['status'] == 'ok', result
        saved = json.loads(child.read_text(encoding='utf-8'))
    else:
        result = asyncio.run(jm.process_notebook(
            '/Workspace/source.ipynb', kernel, 'fixture', '01_task', {},
            run_all=mode != 'code_only'))
        assert result['status'] == ('CONVERTED_NOT_VALIDATED' if mode == 'code_only' else 'PASS'), result
        saved = next(json.loads(v) for k,v in kernel.files.items() if k.endswith('.ipynb'))
    sources = '\n'.join(''.join(c['source']) for c in saved['cells'])
    assert SOURCE_PATH not in sources
    assert OCI_PATH in sources
    assert SANDBOX_PATH not in sources, 'execution-only sandbox must not leak into saved source'
    assert '```' in sources
    assert '# ' in sources and 'send_data_to_slack' in sources
    assert len(repairs) == (1 if mode.endswith('repair') else 0)
    assert len(migrations) == (0 if mode == 'child_repair' else 1)
    assert frame.paths == ([] if mode == 'code_only' else [SANDBOX_PATH])
