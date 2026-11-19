"""Native validation executes protected files, leaving delivered source intact."""
import asyncio
import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_child_exit_conversion import jm
from native_validation import prepare
from nested_run_dispatch import UnsupportedDispatch


class FilesystemSession:
    def __init__(self, corrupt=False):
        self.corrupt = corrupt

    async def execute(self, code, **kwargs):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exec(code, {})
        text = output.getvalue()
        if self.corrupt and '__AIDP_VALIDATION_SAVED__' in text:
            text = '__AIDP_VALIDATION_SAVED__wrong\n'
        # Real AIDP may repeat identical stream responses.
        return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': text + text}]}


def notebook(path, cells, language='python'):
    path.write_text(json.dumps({'nbformat': 4, 'nbformat_minor': 5,
        'metadata': {'language_info': {'name': language}},
        'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {},
                   'outputs': [], 'execution_count': None} for source in cells]}))
    return path.read_bytes()


@pytest.fixture
def protection(monkeypatch):
    monkeypatch.setattr(jm, '_REDIRECT_TABLE_PREFIX', 'default.fixture_sandbox')
    monkeypatch.setattr(jm, '_REDIRECT_BUCKET', 'fixture-bucket')
    monkeypatch.setattr(jm, '_REDIRECT_NAMESPACE', 'fixture-namespace')
    monkeypatch.setattr(jm, '_write_redirect_table_map', {})
    monkeypatch.setattr(jm, '_write_redirect_path_map', {})
    monkeypatch.setattr(jm, '_write_redirect_log', [])


def protect(root, target, session=None):
    return asyncio.run(prepare(str(target), session or FilesystemSession(),
        migrated_base=str(root), redirect=jm._prepare_cell_redirects,
        language=jm._native_lang_from_nb, canonical=jm._native_notebook_path,
        parse_run=jm._parse_percent_run_arguments, quote_run=jm._quote_percent_run_target,
        render=lambda r: ''.join(o['text'] for o in r['outputs'])))


def run_python(path, writes, arguments=None):
    class Exit(Exception):
        pass
    class Writer:
        def mode(self, value):
            assert value == 'append'
            return self
        def saveAsTable(self, table):
            writes.append(table)
    def exit_notebook(value):
        raise Exit(value)
    def run(child, timeout, arguments=None):
        assert timeout == 60
        return run_python(child, writes, arguments)
    namespace = {'spark': SimpleNamespace(range=lambda n: SimpleNamespace(write=Writer())),
                 'aidputils': SimpleNamespace(notebook=SimpleNamespace(run=run, exit=exit_notebook))}
    try:
        for cell in json.loads(Path(path).read_text())['cells']:
            exec(''.join(cell['source']), namespace)
    except Exit as value:
        return value.args[0]


def test_native_write_and_nested_return_are_protected(protection, tmp_path):
    leaf = tmp_path / 'leaf.ipynb'
    wrapper = tmp_path / 'wrapper.ipynb'
    original_leaf = notebook(leaf, ['spark.range(1).write.mode("append").saveAsTable("sales.orders")',
                                    'aidputils.notebook.exit("kept")'])
    original_wrapper = notebook(wrapper, [
        'before = 1\nresult = aidputils.notebook.run("./leaf", 60, {"arg": "value"})\n'
        'assert before == 1 and result == "kept"\naidputils.notebook.exit(result)'])
    protected = protect(tmp_path, wrapper)
    writes = []
    assert run_python(protected, writes) == 'kept'
    assert writes == ['default.fixture_sandbox.sales_orders']
    assert leaf.read_bytes() == original_leaf
    assert wrapper.read_bytes() == original_wrapper
    # A new repaired child must yield fresh execution bytes, not a stale copy.
    notebook(leaf, ['aidputils.notebook.exit("repaired")'])
    repaired = protect(tmp_path, leaf)
    assert run_python(repaired, []) == 'repaired'


@pytest.mark.parametrize('language,source', [
    ('scala', 'spark.range(1).write.mode("append").saveAsTable("sales.orders")\noidlUtils.notebook.exit("kept")'),
    ('sql', 'INSERT INTO sales.orders SELECT 1')])
def test_native_metadata_redirected_in_copy_only(protection, tmp_path, language, source):
    target = tmp_path / 'leaf.ipynb'
    original = notebook(target, [source], language)
    copied = json.loads(Path(protect(tmp_path, target)).read_text())
    code = ''.join(copied['cells'][0]['source'])
    assert code.startswith('%' + language + '\n')
    assert 'fixture_sandbox.sales_orders' in code
    assert target.read_bytes() == original


@pytest.mark.parametrize('source', [
    'target = "./leaf"\naidputils.notebook.run(target, 60)',
    'aidputils.notebook.run("/outside/leaf", 60)'])
def test_unprotected_nested_destination_refused_before_any_copy(protection, tmp_path, source):
    target = tmp_path / 'wrapper.ipynb'
    original = notebook(target, [source])
    with pytest.raises(UnsupportedDispatch):
        protect(tmp_path, target)
    assert target.read_bytes() == original
    assert not list(tmp_path.glob('*.__aidp_validation_*'))


def test_copy_receipt_mismatch_prevents_dispatch(protection, tmp_path):
    target = tmp_path / 'leaf.ipynb'
    original = notebook(target, ['aidputils.notebook.exit("kept")'])
    with pytest.raises(UnsupportedDispatch, match='hash mismatch'):
        protect(tmp_path, target, FilesystemSession(corrupt=True))
    assert target.read_bytes() == original


def test_percent_run_routes_to_protected_child(protection, tmp_path):
    leaf = tmp_path / 'leaf.ipynb'
    notebook(leaf, ['spark.range(1).write.mode("append").saveAsTable("sales.orders")'])
    wrapper = tmp_path / 'wrapper.ipynb'
    notebook(wrapper, ['%run ./leaf'])
    code = ''.join(json.loads(Path(protect(tmp_path, wrapper)).read_text())['cells'][0]['source'])
    child, trailing = jm._parse_percent_run_arguments(code.removeprefix('%run '))
    assert '.__aidp_validation_' in child
    assert 'fixture_sandbox.sales_orders' in Path(child).read_text()


def test_production_dispatch_reads_protected_copy(protection, monkeypatch, tmp_path):
    import test_child_exit_conversion as harness
    original_execute = harness._ExitKernel.execute
    writes, native_paths = [], []

    async def execute(self, code, **kwargs):
        if '__AIDP_VALIDATION_' in code:
            return await FilesystemSession().execute(code, **kwargs)
        if code.startswith('result = aidputils.notebook.run('):
            namespace = self.namespace
            def run(path, timeout):
                native_paths.append(path)
                return run_python(path, writes)
            namespace['aidputils'] = SimpleNamespace(notebook=SimpleNamespace(run=run))
            exec(code, namespace)
        if code.startswith('assert result'):
            exec(code, self.namespace)
        return await original_execute(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    target = str(tmp_path / 'leaf.ipynb')
    source = f'result = aidputils.notebook.run({target!r}, 60)\nassert result == "kept"'
    result, saved, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source],
        'leaf': ['spark.range(1).write.mode("append").saveAsTable("sales.orders")',
                 'aidputils.notebook.exit("kept")']},
        real_paths=True, native_protection=True)
    assert result['status'] == 'ok', result
    assert writes == ['default.fixture_sandbox.sales_orders']
    assert len(native_paths) == 1 and '.__aidp_validation_' in native_paths[0]
    assert saved['child'] == [source]
    assert 'fixture_sandbox' not in '\n'.join(saved['leaf'])
