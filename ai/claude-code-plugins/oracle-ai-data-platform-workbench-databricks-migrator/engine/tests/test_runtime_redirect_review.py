"""PR110 September 16 review: isolation, idempotency and cell failure contracts."""
import asyncio
import contextlib
import io
import sys
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _declare_test_sandbox(monkeypatch, tmp_path):
    """These tests exercise fs-shim behaviour, not the sandbox gate: the
    notebook_policy runtime assertions (fail-closed since the policy gate)
    need a declared sandbox before any dbutils.fs / write helper runs."""
    monkeypatch.setenv("AIDP_SANDBOX_CATALOG", "default")
    monkeypatch.setenv("AIDP_SANDBOX_SCHEMA", "sbx")
    monkeypatch.setenv("AIDP_SANDBOX_PREFIX", "oci://b@ns/,oci://sandbox@testns/,/")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import job_migrate as jm


@pytest.fixture(autouse=True)
def config(monkeypatch):
    import context_tools
    monkeypatch.setattr(context_tools, '_BUCKET_NS_INDEX', {'source': 'tenant', 'sandbox': 'testns'})
    monkeypatch.setattr(jm, '_REDIRECT_BUCKET', 'sandbox')
    monkeypatch.setattr(jm, '_REDIRECT_NAMESPACE', 'testns')
    monkeypatch.setattr(jm, '_REDIRECT_TABLE_PREFIX', 'default.custom_sandbox')
    jm.clear_write_redirect_map()
    yield
    jm.clear_write_redirect_map()


@pytest.mark.parametrize('uri,already', [
    ('oci://sandbox@testns/x', True), ('oci://sandbox@testns', True),
    ('oci://sandbox@other/x', False), ('oci://sandbox@testns-extra/x', False),
    ('oci://sandbox-other@testns/x', False), ('/tmp/oci://sandbox@testns/x', False),
])
def test_namespace_boundary(uri, already):
    assert jm._is_already_redirected_path(uri) is already
    ns = {}
    exec(jm._py_write_guard_fn(), ns)
    actual = ns['_aidp__write_guard__'](uri)
    if uri.startswith('oci://') and not already:
        assert actual.startswith('oci://sandbox@testns/') and actual != uri
    else:
        assert actual == uri


@pytest.mark.parametrize('literal', [False, True])
def test_foreign_namespace_writer_and_consumer_pipeline(literal):
    from test_runtime_path_redirect import execute, kernel
    fs, namespace = kernel()
    uri = 'oci://sandbox@other/review/tmp'
    destination = repr(uri) if literal else 'path'
    execute(f'path={uri!r}\nx2.write.json({destination})\nrows=dbutils.fs.ls(path)', namespace)
    assert namespace['rows'][0].path == 'oci://sandbox@testns/sandbox/review/tmp/part-000.json'
    assert all('@other/' not in call[1] for call in fs.calls)


@pytest.mark.parametrize('writer', [
    'df.write.saveAsTable("db.tbl")', 'df.write.json(out)',
    'df.write.mode("overwrite").saveAsTable("db.tbl")',
])
@pytest.mark.parametrize('reader', ['spark.read.csv(path)', 'spark.read.option("path", path).load()'])
@pytest.mark.parametrize('scala', [False, True])
def test_writer_does_not_capture_later_read(writer, reader, scala):
    source = writer + '\n' + reader
    result = jm._wrap_write_dests(source, scala)
    assert result.splitlines()[-1] == reader


def test_custom_table_prefix_is_idempotent():
    source = 'df.write.saveAsTable("db.tbl")'
    first = jm._apply_write_redirects(source)
    assert 'default.custom_sandbox.db_tbl' in first
    assert jm._apply_write_redirects(first) == first
    assert not jm._is_already_redirected_table('another.custom_sandbox.db_tbl')


# schema.json permits exactly two unquoted identifier components.
@pytest.mark.parametrize('prefix', ['default.custom', 'other_catalog.custom', 'default.Custom_2'])
@pytest.mark.parametrize('code', ['df.write.saveAsTable("db.tbl")',
                                'spark.sql("INSERT INTO db.tbl SELECT 1")'])
def test_table_prefix_configuration_matrix(monkeypatch, prefix, code):
    monkeypatch.setattr(jm, '_REDIRECT_TABLE_PREFIX', prefix)
    once = jm._apply_write_redirects(code)
    assert once != code and jm._apply_write_redirects(once) == once


@pytest.mark.parametrize('source,expected', [
    ('df.write.mode("overwrite").json(path)', 'json(_aidp__write_guard__(path))'),
    ('df.write.csv(path, mode="overwrite", header=True)', 'csv(_aidp__write_guard__(path), mode='),
    ('df.write.save(path=path, mode="append")', 'path=_aidp__write_guard__(path)'),
    ('df.write.option("path", path).save()', 'option("path", _aidp__write_guard__(path))'),
    ('(df.write\n .mode(make_mode())\n .json(make_path(a, b)))', 'json(_aidp__write_guard__(make_path(a, b)))'),
    ('label="é"; df.write.json(path)', 'json(_aidp__write_guard__(path))'),
])
def test_python_writer_chain_arguments_and_idempotency(source, expected):
    result = jm._wrap_write_dests(source, False)
    assert expected in result
    compile(result, '<writer>', 'exec')
    assert jm._wrap_write_dests(result, False) == result


@pytest.mark.parametrize('source', [
    '# df.write.json(path)\nspark.read.csv(path)',
    'doc = "df.write.json(path)"',
    'df.write_metadata.json(path)',
    'df.write.mode(spark.read.csv(path)).saveAsTable("db.tbl")',
])
def test_non_writer_expressions_unchanged(source):
    assert jm._wrap_write_dests(source, False) == source


def test_scala_guard_matches_full_authority_and_writer_chain():
    guard = jm._scala_write_guard_fn()
    assert 'p == "oci://sandbox@testns"' in guard
    assert 'p.startsWith("oci://sandbox@testns/")' in guard
    source = 'df.write\n.mode("overwrite").option("path", path).save()\nspark.read.csv(input)'
    wrapped = jm._wrap_write_dests(source, True)
    assert '.option("path", _aidp__write_guard__(path)).save()' in wrapped
    assert wrapped.endswith('spark.read.csv(input)')
    assert jm._wrap_write_dests(wrapped, True) == wrapped


class Kernel:
    def __init__(self):
        self.ns = {}
        self.codes = []

    async def execute(self, code, **kwargs):
        self.codes.append(code)
        stream = io.StringIO()
        outputs = []
        status = 'ok'
        with contextlib.redirect_stdout(stream):
            try:
                exec(code, self.ns)
            except Exception as exc:
                status = 'error'
                outputs.append({'type': 'error', 'ename': type(exc).__name__, 'evalue': str(exc),
                                'traceback': [type(exc).__name__ + ': ' + str(exc)]})
        return {'status': status, 'outputs': [{'type': 'stream', 'text': stream.getvalue()}] + outputs}


@pytest.mark.parametrize('operation', ['rm', 'mv', 'cp'])
@pytest.mark.parametrize('wrapped', [False, True])
def test_caught_refusal_still_fails_replay_without_model(monkeypatch, operation, wrapped):
    from types import SimpleNamespace
    from test_runtime_path_redirect import FS
    import agent_migrate as am
    monkeypatch.setattr(am, '_cell_history', [])
    async def no_fix(*a, **k):
        pytest.fail('Safety refusal must never enter model repair: ' + repr(a))
    monkeypatch.setattr(jm, 'call_fix', no_fix)
    class WrappedKernel(Kernel):
        async def execute(self, code, **kwargs):
            import json
            result = await super().execute(code, **kwargs)
            if wrapped:
                for out in result['outputs']:
                    if out['type'] == 'stream':
                        out['text'] = json.dumps([{'type': 'TEXT_PLAIN', 'value': out['text']}])
            return result
    kernel = WrappedKernel()
    kernel.ns['dbutils'] = SimpleNamespace(fs=FS())
    args = "'oci://source@tenant/tmp', True" if operation == 'rm' else "'oci://source@tenant/tmp/', 'oci://source@tenant/out', True"
    source = f"print('customer prefix', end='')\ntry:\n    dbutils.fs.{operation}({args})\nexcept RuntimeError:\n    pass"
    result = asyncio.run(jm._replay_cell_entry(
        {'final_code': source, 'cell_idx': 0, 'notebook_path': '/Workspace/test/review.ipynb'},
        'caught refusal', kernel))
    assert result['status'] == 'error' and result['kind'] == 'REDIRECT_REFUSED'
    assert not kernel.ns['dbutils'].fs.calls


def test_caught_refusal_main_loop_is_reported_without_model(monkeypatch, tmp_path):
    import agent_migrate as am
    from types import SimpleNamespace
    from test_conditional_exit_below_execute import _patch_common, _Kernel
    from test_runtime_path_redirect import FS
    source = "try:\n    dbutils.fs.rm('oci://source@tenant/tmp', True)\nexcept RuntimeError:\n    pass"
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    guard = jm._inject_write_guard
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, '_inject_write_guard', guard)
    monkeypatch.setattr(am, '_cell_history', [])
    monkeypatch.setattr(jm, '_cell_history', am._cell_history)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', True)
    async def no_fix(*args, **kwargs):
        pytest.fail('Caught policy refusal reached the model')
    monkeypatch.setattr(jm, 'call_fix', no_fix)
    kernel = Kernel()
    kernel.ns['dbutils'] = SimpleNamespace(fs=FS())
    class Session(_Kernel):
        async def execute(self, code, **kwargs):
            if '_aidp__fs_proxy__(dbutils.fs).rm(' in code:
                return await kernel.execute(code, **kwargs)
            return await super().execute(code, **kwargs)
    result = asyncio.run(jm.process_notebook('/Workspace/test_cond/source.ipynb', Session(),
                                           'test_cond', '01_task', {}))
    assert not result['status'].startswith('PASS')
    assert result['cell_results'][0]['kind'] == 'REDIRECT_REFUSED'
    assert any(h.get('kind') == 'REDIRECT_REFUSED' for h in am._cell_history)
    assert not kernel.ns['dbutils'].fs.calls


def test_parser_failure_enters_replay_repair_without_executing_customer_code(monkeypatch):
    import agent_migrate as am
    monkeypatch.setattr(am, '_cell_history', [])
    source = 'side_effect()\n!echo mixed magic\ndbutils.fs.ls(tmp)'
    repaired = '# Oracle tool modification: test repair\nresult = 1'
    received = []
    async def fix(code, error, *a, **k):
        received.append((code, error))
        return repaired
    monkeypatch.setattr(jm, 'call_fix', fix)
    kernel = Kernel()
    result = asyncio.run(jm._replay_cell_entry(
        {'final_code': source, 'cell_idx': 0, 'notebook_path': '/Workspace/test/review.ipynb',
         '_persist_source': [source], '_persist_active': True},
        'mixed magic', kernel))
    assert result['status'] == 'ok', (result, received)
    assert received[0][0] == source and 'Cannot safely wrap' in received[0][1]
    assert 'side_effect' not in kernel.ns and kernel.ns['result'] == 1
    assert am._cell_history[-1]['replay_code'] == repaired


@pytest.mark.parametrize('sandbox', [False, True])
def test_main_loop_parse_failure_repaired_and_saved(monkeypatch, tmp_path, sandbox):
    from test_conditional_exit_below_execute import _patch_common, _Kernel, _saved_sources
    source = 'side_effect()\n!echo mixed magic\ndbutils.fs.ls(tmp)'
    repaired = '# Oracle tool modification: test repair\nreview_repaired = 1'
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    guard = jm._inject_write_guard
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, '_inject_write_guard', guard)
    if not sandbox:
        monkeypatch.setattr(jm, '_REDIRECT_BUCKET', None)
        monkeypatch.setattr(jm, '_REDIRECT_NAMESPACE', None)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', True)
    received = []
    async def fix(code, *args, **kwargs):
        received.append(code)
        assert '_aidp__' not in code
        return repaired
    monkeypatch.setattr(jm, 'call_fix', fix)
    class Session(_Kernel):
        async def execute(self, code, **kwargs):
            if code.startswith('raise SyntaxError(') or '!echo mixed magic' in code:
                if sandbox:
                    assert code.startswith('raise SyntaxError(')
                return {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'SyntaxError',
                    'evalue': 'Cannot safely wrap', 'traceback': ['SyntaxError: Cannot safely wrap']}]}
            return await super().execute(code, **kwargs)
    session = Session()
    result = asyncio.run(jm.process_notebook('/Workspace/test_cond/source.ipynb', session,
                                           'test_cond', '01_task', {}))
    assert result['status'].startswith('PASS'), result
    assert len(received) == 1 and 'side_effect()' in received[0]
    saved, _ = _saved_sources(session)
    assert 'review_repaired = 1' in saved and '_aidp__' not in saved


@pytest.mark.parametrize('code', ['x = spark.read.csv(path)\ndf.write.json(out)',
                                'broken!\ndbutils.fs.ls(path)'])
def test_non_sandbox_guard_is_exact_noop(monkeypatch, code):
    monkeypatch.setattr(jm, '_REDIRECT_BUCKET', None)
    assert jm._inject_write_guard(code) == code


@pytest.mark.parametrize('with_refusal', [False, True])
def test_customer_json_is_not_decoded_as_a_tool_event(with_refusal):
    import json
    text = json.dumps([{'value': 1}]) + json.dumps([{'value': 2}])
    result = {'status': 'ok', 'outputs': [{'type': 'stream', 'text': text}]}
    if with_refusal:
        result['outputs'].append({'type': 'stream', 'text': json.dumps([
            {'type': 'TEXT_PLAIN', 'value': '\n__AIDP_REDIRECT_REFUSED__: test refusal\n'}])})
    assert jm._redirect_refusal(result) == ('REDIRECT_REFUSED: test refusal' if with_refusal else '')
    assert result['outputs'][0]['text'] == text
