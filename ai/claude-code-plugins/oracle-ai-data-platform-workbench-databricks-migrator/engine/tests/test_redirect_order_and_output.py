"""PR110 re-review and live mixed-output incident, September 16."""
import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import job_migrate as jm
from context_tools import _unwrap_aidp_text


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


@pytest.mark.parametrize('prefix', ['', '# comment\n', '"""module docstring"""\n', '%python\n', '%pyspark # hi\n'])
@pytest.mark.parametrize('separator', ['\n', '; '])
def test_future_import_guard_compiles_and_executes(prefix, separator):
    from test_runtime_path_redirect import kernel
    fs, ns = kernel()
    source = prefix + 'from __future__ import annotations' + separator + 'x2.write.json("oci://source@tenant/out")'
    guarded = jm._inject_write_guard(source)
    python = '\n'.join(line for line in guarded.splitlines() if not line.startswith('%'))
    exec(compile(python, '<future-import-guard>', 'exec'), ns)
    assert fs.calls == [('write', 'oci://sandbox@testns/source/out')]
    assert guarded.index('from __future__') < guarded.index('def _aidp__write_guard__')
    assert jm._inject_write_guard(guarded) == guarded


@pytest.mark.parametrize('value', [None, 123, [], {}, False, 'ordinary error'])
def test_non_string_structured_error_is_not_a_refusal(value):
    result = {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'ValueError', 'evalue': value}]}
    assert jm._redirect_refusal(result) == ''
    assert result['outputs'][0]['evalue'] == value


@pytest.mark.parametrize('value', [1, None, {'nested': 'value'}, ['x'], 'customer text'])
@pytest.mark.parametrize('wrapped', [False, True])
def test_safe_execute_preserves_customer_json_and_extracts_mixed_events(value, wrapped):
    customer = json.dumps([{'value': value}]) + json.dumps([{'value': 2}]) + '\n'
    marker = '__AIDP_PATH_REDIRECT__' + json.dumps(['oci://source@tenant/x', 'oci://sandbox@testns/source/x']) + '\n'
    text = customer + marker + 'tail\n'
    stream = json.dumps([{'type': 'TEXT_PLAIN', 'value': text}]) if wrapped else text
    class Session:
        async def execute(self, code, **kwargs):
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': stream}]}
    logs = []
    result = asyncio.run(jm._safe_execute(Session(), 'pass', log_fn=logs.append))
    assert result['outputs'][0]['text'] == customer + 'tail\n'
    assert len(logs) == 1 and '[AIDP write-redirect]' in logs[0]
    assert _unwrap_aidp_text(result['outputs'][0]['text']) == customer + 'tail\n'
    assert jm._redirect_refusal(result) == ''


@pytest.mark.parametrize('text', ['[{"value":1}][{"value":2}]', '[{"type":"TEXT_PLAIN","value":null}]',
                                '[{"type":"TEXT_PLAIN","value":1}]', 'broken [not json', '[1,2]', 'plain text'])
def test_customer_or_malformed_json_is_preserved(text):
    assert _unwrap_aidp_text(text) == text


def test_envelope_decode_respects_brackets_and_platform_double_send():
    envelope = json.dumps([{'type': 'TEXT_PLAIN', 'value': 'a][b\n'}])
    assert _unwrap_aidp_text(envelope + envelope) == 'a][b\n'
    assert _unwrap_aidp_text('same\nsame\n') == 'same\nsame\n'


@pytest.mark.parametrize('payload', ['[not json]', '[1,2]', '[[1],[2]]', '["a","b","c"]', '[]', '[null,"b"]'])
@pytest.mark.parametrize('wrapped', [False, True])
def test_malformed_sentinel_is_reported_and_preserved(payload, wrapped):
    plain = 'customer\n__AIDP_PATH_REDIRECT__' + payload + '\n'
    stream = json.dumps([{'type': 'TEXT_PLAIN', 'value': plain}]) if wrapped else plain
    class Session:
        async def execute(self, code, **kwargs):
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': stream}]}
    logs = []
    result = asyncio.run(jm._safe_execute(Session(), 'pass', log_fn=logs.append))
    assert result['outputs'][0]['text'] == plain
    assert len(logs) == 1 and 'UNRESOLVED: malformed sentinel' in logs[0]
    assert jm.get_write_redirect_summary()['paths'] == {}


@pytest.mark.parametrize('writer_path', ['oci://source@tenant/order', 'oci://sandbox@testns/source/order'])
def test_later_directory_writer_updates_existing_file_alias(writer_path):
    from test_runtime_path_redirect import kernel
    fs, ns = kernel()
    exec(jm._py_write_guard_fn(), ns)
    source = 'oci://source@tenant/order'
    target = 'oci://sandbox@testns/source/order'
    ns['_aidp__write_guard__'](source, directory=False)
    ns['_aidp__write_guard__'](target, directory=False)
    exec(jm._prepare_cell_redirects(f'x2.write.json({writer_path!r})'), ns)
    assert ns['_aidp__consume_output__'](source + '/part-000.json') == target + '/part-000.json'
    assert ns['_aidp__output_paths__'][source] == (target, True)
    assert ns['_aidp__output_paths__'][target] == (target, True)


SOURCE = 'oci://source@tenant/order'
TARGET = 'oci://sandbox@testns/source/order'


@pytest.mark.parametrize('source', [
    f'spark.read.option("path", "{SOURCE}").load()',
    f'# x2.write.json("{SOURCE}")\nspark.read.json("{SOURCE}")',
    f'example = \'x2.write.json("{SOURCE}")\'\nspark.read.json("{SOURCE}")',
    f'"""x2.write.json("{SOURCE}")"""\nspark.read.json("{SOURCE}")',
])
def test_non_writer_does_not_register_static_path(source):
    assert jm._apply_write_redirects(source) == source
    assert jm._apply_read_redirects(source) == source
    assert jm.get_write_redirect_summary()['paths'] == {}


def ordered_kernel():
    from test_runtime_path_redirect import kernel
    from types import SimpleNamespace
    fs, ns = kernel()
    class Reader:
        def json(self, path):
            fs.calls.append(('read', path))
            return ns['x2']
        def option(self, key, value):
            assert key == 'path'
            self.path = value
            return self
        def load(self, path=None):
            return self.json(path if path is not None else self.path)
        def parquet(self, *paths):
            for path in paths:
                self.json(path)
    ns['spark'] = SimpleNamespace(read=Reader())
    return fs, ns


@pytest.mark.parametrize('separator', ['\n', '; '])
@pytest.mark.parametrize('read', [f'spark.read.json("{SOURCE}")', f'spark.read.option("path", "{SOURCE}").load()'])
def test_real_pipeline_preserves_read_write_read_order(separator, read):
    fs, ns = ordered_kernel()
    source = separator.join([read, f'x2.write.json("{SOURCE}")', read])
    exec(jm._prepare_cell_redirects(source), ns)
    assert fs.calls == [('read', SOURCE), ('write', TARGET), ('read', TARGET)]
    # A later cell sees the actual earlier write, not merely source discovery.
    exec(jm._prepare_cell_redirects(read), ns)
    assert fs.calls[-1] == ('read', TARGET)


@pytest.mark.parametrize('condition', [False, True])
def test_conditional_writer_only_redirects_after_execution(condition):
    fs, ns = ordered_kernel()
    source = f'if {condition}:\n    x2.write.json("{SOURCE}")\nspark.read.json("{SOURCE}")'
    exec(jm._prepare_cell_redirects(source), ns)
    assert fs.calls[-1] == ('read', TARGET if condition else SOURCE)


def test_deferred_function_and_reader_writer_chain_order():
    fs, ns = ordered_kernel()
    source = f'def produce():\n    x2.write.json("{SOURCE}")\nspark.read.json("{SOURCE}")\nproduce()\nspark.read.json("{SOURCE}")'
    exec(jm._prepare_cell_redirects(source), ns)
    assert fs.calls == [('read', SOURCE), ('write', TARGET), ('read', TARGET)]
    jm.clear_write_redirect_map()
    fs, ns = ordered_kernel()
    exec(jm._prepare_cell_redirects(f'spark.read.json("{SOURCE}").write.json("{SOURCE}")'), ns)
    assert fs.calls == [('read', SOURCE), ('write', TARGET)]


def test_static_writer_edits_only_owned_argument():
    source = f'spark.read.json("{SOURCE}").write.json("{SOURCE}")'
    actual = jm._apply_write_redirects(source)
    assert actual == f'spark.read.json("{SOURCE}").write.json("{TARGET}")'


@pytest.mark.parametrize('read', [f'spark.read.load(path="{SOURCE}")',
    f'spark.read.json(["{SOURCE}"])', f'spark.read.parquet(*["{SOURCE}"])',
    f'spark.read.parquet("{SOURCE}", "{SOURCE}")'])
def test_reader_argument_shapes_preserve_runtime_order(read):
    fs, ns = ordered_kernel()
    exec(jm._prepare_cell_redirects(f'x2.write.json("{SOURCE}")\n' + read), ns)
    assert all(call[1] == TARGET or call[1] == [TARGET] for call in fs.calls)


def test_static_table_read_before_write_order():
    source = 'a = spark.read.table("db.tbl")\ndf.write.saveAsTable("db.tbl")\nb = spark.read.table("db.tbl")'
    actual = jm._prepare_cell_redirects(source)
    assert 'a = spark.read.table("db.tbl")' in actual
    assert 'b = spark.read.table("default.custom_sandbox.db_tbl")' in actual


@pytest.mark.parametrize('wrapped', [False, True])
def test_refusal_same_stream_as_customer_json(wrapped):
    text = '[{"value":1}][{"value":2}]\n__AIDP_REDIRECT_REFUSED__: not allowed\n'
    raw = json.dumps([{'type': 'TEXT_PLAIN', 'value': text}]) if wrapped else text
    assert jm._redirect_refusal({'outputs': [{'type': 'stream', 'text': raw}]}) == 'REDIRECT_REFUSED: not allowed'


def test_read_order_real_replay_and_output_pipeline(monkeypatch):
    import agent_migrate as am
    from test_runtime_redirect_review import Kernel
    fs, ns = ordered_kernel()
    kernel = Kernel()
    kernel.ns = ns
    monkeypatch.setattr(am, '_cell_history', [])
    async def no_fix(*args, **kwargs):
        pytest.fail('Read ordering/output decoding must not need model repair')
    monkeypatch.setattr(jm, 'call_fix', no_fix)
    source = ('print(\'[{"value":1}][{"value":2}]\')\n'
              f'spark.read.json("{SOURCE}")\nx2.write.json("{SOURCE}")\nspark.read.json("{SOURCE}")')
    result = asyncio.run(jm._replay_cell_entry(
        {'final_code': source, 'cell_idx': 0, 'notebook_path': '/Workspace/test/ordering.ipynb'},
        'read order regression', kernel))
    assert result['status'] == 'ok', result
    assert fs.calls == [('read', SOURCE), ('write', TARGET), ('read', TARGET)]
    assert result['final_code'] == source
    assert am._cell_history[-1]['replay_code'] == source
    assert jm.get_write_redirect_summary()['paths'][SOURCE] == TARGET
