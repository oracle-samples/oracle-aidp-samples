"""REF-202 B2: the complete live-verified Scala widget API maps directly.

Contract grid: eight methods × active/inert source; constructor argument
forms; task preprocessing/save-back and pass-1 migrated child execution.
Live API evidence: 2026-10-15, session aaaaaaaa-0000-4000-8000-000000000108.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import job_migrate as jm


CALLS = [
    ('get', '"mode"'),
    ('text', '"mode", "daily"'),
    ('dropdown', '"mode", "daily", Seq("daily", "weekly")'),
    ('combobox', '"mode", "custom", List("daily", "weekly")'),
    ('multiselect', '"mode", "daily,weekly", Seq("daily", "weekly")'),
    ('getAll', ''),
    ('remove', '"mode"'),
    ('removeAll', ''),
]


@pytest.mark.parametrize('method,args', CALLS)
def test_every_scala_widget_method_preserves_arguments_and_original(method, args):
    call = f'dbutils.widgets.{method}({args})'
    source = '%scala\n' + call
    migrated = jm._preprocess_scala_cell_source(source)
    assert f'aidputils.widgets.{method}({args})' in migrated
    assert '// ' + call in migrated
    assert 'Oracle tool modification:' in migrated
    assert jm._preprocess_scala_cell_source(migrated) == migrated


@pytest.mark.parametrize('method', ['text', 'dropdown', 'combobox', 'multiselect'])
@pytest.mark.parametrize('label', [False, True])
def test_named_multiline_constructor_arguments_are_unchanged(method, label):
    args = '  name = keyFor("x,y)"),\n  defaultValue = currentValue()'
    if method != 'text':
        args += ',\n  choices = Seq("a", nested("b,c)"))'
    if label:
        args += ',\n  label = "Choose (mode)"'
    call = f'dbutils.widgets.{method}(\n{args}\n)'
    migrated = jm._preprocess_scala_cell_source('%scala\n' + call)
    assert call.replace('dbutils.widgets.', 'aidputils.widgets.', 1) in migrated
    assert '// ' + call.splitlines()[0] in migrated
    assert jm._preprocess_scala_cell_source(migrated) == migrated


@pytest.mark.parametrize('method,args', CALLS)
@pytest.mark.parametrize('context', ['line', 'block', 'nested_block', 'string', 'triple', 'inline'])
def test_widget_text_in_strings_and_comments_is_untouched(method, args, context):
    call = f'dbutils.widgets.{method}({args})'
    source = {
        'line': '// ' + call,
        'block': '/*\n' + call + '\n*/',
        'nested_block': '/* outer /* nested */\n' + call + '\n*/',
        'string': 'val example = ' + json.dumps(call),
        'triple': 'val example = """\n' + call + '\n"""',
        'inline': 'val n = 1 // ' + call,
    }[context]
    source = '%scala\n' + source
    assert jm._preprocess_scala_cell_source(source) == source


@pytest.mark.parametrize('receiver', ['mydbutils', 'other.dbutils', 'my$dbutils', 'aidputils'])
@pytest.mark.parametrize('method,args', CALLS)
def test_similarly_named_receivers_and_native_calls_are_untouched(receiver, method, args):
    source = f'%scala\n{receiver}.widgets.{method}({args})'
    assert jm._preprocess_scala_cell_source(source) == source


def test_multiple_calls_preserve_comment_and_string_arguments():
    source = ('%scala\nval literal = """dbutils.widgets.removeAll()"""\n'
              'dbutils.widgets.text("mode", dbutils.widgets.get(keyFor("x,y)"))); '
              'dbutils.widgets.getAll() // dbutils.widgets.removeAll()')
    migrated = jm._preprocess_scala_cell_source(source)
    assert 'val literal = """dbutils.widgets.removeAll()"""' in migrated
    assert ('aidputils.widgets.text("mode", aidputils.widgets.get(keyFor("x,y)"))); '
            'aidputils.widgets.getAll() // dbutils.widgets.removeAll()') in migrated
    assert jm._preprocess_scala_cell_source(migrated) == migrated


def test_unknown_widget_method_is_left_for_the_model():
    source = '%scala\ndbutils.widgets.getArgument("mode", "daily")'
    assert jm._preprocess_scala_cell_source(source) == source


@pytest.mark.parametrize('method,args', CALLS)
@pytest.mark.parametrize('child', [False, True])
def test_all_methods_execute_and_save_through_the_real_cell_loop(monkeypatch, tmp_path, method, args, child):
    from test_child_exit_conversion import _run
    source = f'%scala\ndbutils.widgets.{method}({args})'
    if child:
        # Dependencies enter inlining after pass-1 migration. Exercise that same
        # preprocessor, then assert both actual child execution and save-back.
        migrated = jm._preprocess_scala_cell_source(source)
        result, saved, kernel, _, _ = _run(monkeypatch, tmp_path, {'child': [migrated]})
        saved_sources = saved['child']
    else:
        result, _, kernel, _, _ = _run(monkeypatch, tmp_path, {}, parent=[source])
        notebook = next(json.loads(text) for path, text in kernel.files.items() if path.endswith('final.ipynb'))
        saved_sources = [''.join(c['source']) for c in notebook['cells']]
    assert result['status'] in ('PASS', 'ok'), result
    expected = f'aidputils.widgets.{method}({args})'
    assert any(expected in code for code in kernel.executed)
    assert any(expected in code and '// dbutils.widgets.' in code for code in saved_sources)
    for code in saved_sources:
        active = jm._blank_scala_comments(code)
        assert f'dbutils.widgets.{method}(' not in active


def test_nested_comment_scanner_keeps_existing_fs_consumer_safe():
    comment = '/* outer /* inner */ OciFs.ls("inert") */\n'
    assert not jm._has_active_ocifs_ls(comment + 'dbutils.fs.ls("unresolved")')
    assert jm._has_active_ocifs_ls(comment + 'OciFs.ls("active")')
