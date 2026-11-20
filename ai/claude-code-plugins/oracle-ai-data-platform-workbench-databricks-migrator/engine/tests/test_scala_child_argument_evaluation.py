"""PR #248 B1: Scala child arguments observe caller widgets before mutation.

Drive the actual parent/child orchestration and execute its Python scope code.
The fake Scala boundary models eager Map/Seq construction and native text()
preservation; the same expression shapes are also exercised on live Scala.
"""
import asyncio
import contextlib
import io
from types import SimpleNamespace

import pytest

import test_child_exit_conversion as child_tests
import job_migrate as jm
from test_cohort_widget_seed import _PreserveWidgets


@pytest.mark.parametrize('mapping, expected', [
    ('Map("mode" -> aidputils.widgets.get("mode"))',
     [('mode', 'aidputils.widgets.get("mode")')]),
    ('Map("mode" -> decorate(aidputils.widgets.get("mode"), "x,y)"))',
     [('mode', 'decorate(aidputils.widgets.get("mode"), "x,y)")')]),
    ('Map("mode" -> { val x = read("mode"); x }, "n" -> count(1, 2))',
     [('mode', '{ val x = read("mode"); x }'), ('n', 'count(1, 2)')]),
    ('Map("mode" -> first(), "mode" -> last())',
     [('mode', 'first()'), ('mode', 'last()')]),
    ('{"mode": read("mode"), "n": str(count(1, 2))}',
     [('mode', 'read("mode")'), ('n', 'str(count(1, 2))')]),
    ('Map.empty[String, String]', []),
    ('{}', []),
])
def test_child_map_parser_preserves_complete_argument_expressions(mapping, expected):
    call = f'dbutils.notebook.run("/Workspace/child", 60, {mapping})'
    assert jm._parse_run_map_args(call) == expected


CASES = [
    ('literal', [('mode', '"child"')], {'mode': 'child'}),
    ('eager', [('mode', 'eagerMode')], {'mode': 'child'}),
    ('forward', [('mode', 'aidputils.widgets.get("mode")')], {'mode': 'parent'}),
    ('lazy', [('mode', 'lazyMode')], {'mode': 'parent'}),
    ('derived', [('mode', 'aidputils.widgets.get("mode").toUpperCase')], {'mode': 'PARENT'}),
    ('cross', [('mode', 'aidputils.widgets.get("other")'),
               ('other', 'aidputils.widgets.get("mode")')], {'mode': 'second', 'other': 'parent'}),
    ('duplicate', [('mode', 'first()'), ('mode', 'last()')], {'mode': 'last'}),
    ('empty', [], {'mode': 'parent'}),
]


def run_argument_case(monkeypatch, tmp_path, bindings, *, capture_failure=False, python_call=False, mapping_source=None, setup_failure=None, restore_failure=False):
    parent_widgets = {'mode': 'parent', 'other': 'second'}
    instances = []

    class Kernel(child_tests._ExitKernel):
        def __init__(self, fire=True):
            super().__init__(fire)
            self.widgets = _PreserveWidgets()
            self.ns = {'aidputils': SimpleNamespace(widgets=self.widgets)}
            self.captured = []
            self.reads = []
            self.evaluated = []
            self.child_values = []
            self.phase_events = []
            self.setup_failure = setup_failure
            self.restore_attempted = False
            instances.append(self)

        def evaluate(self, expression):
            self.evaluated.append(expression)
            self.reads.append(dict(self.widgets.getAll()))
            if capture_failure:
                raise RuntimeError('Widget not found: missing-caller-widget')
            values = {'"child"': 'child', 'eagerMode': 'child',
                      'first()': 'first', 'last()': 'last'}
            if expression in values:
                return values[expression]
            if expression == 'aidputils.widgets.get("other")':
                return self.widgets.get('other')
            value = self.widgets.get('mode')
            return value.upper() if expression.endswith('.toUpperCase') else value

        async def execute(self, code, **kwargs):
            if code.startswith('%scala') and '_aidpScalaWidgetArgs' in code:
                self.executed.append(code)
                try:
                    if 'val _aidpScalaWidgetArgs' in code:
                        self.phase_events.append('evaluate')
                        for key, expression in bindings:
                            assert f'String.valueOf({expression})' in code, code
                        captured = [(key, self.evaluate(expression)) for key, expression in bindings]
                        # The fake boundary preserves the language's Map last-wins
                        # contract; Seq deliberately retains duplicates.
                        self.captured = (list(dict(captured).items())
                                         if '= Map' in code else captured)
                    if '_aidpScalaWidgetArgs.foreach' in code:
                        self.phase_events.append('seed')
                        for key, value in self.captured:
                            self.widgets.text(key, value)
                            if self.widgets.get(key) != value:
                                raise RuntimeError('Scala child widget seed mismatch')
                    output = jm._ISO_OK + '\n'
                except (AssertionError, KeyError, RuntimeError) as error:
                    output = 'Command ID fixture failed with ' + str(error)
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': output}]}
            if code.startswith('%scala') and 'CHILD_OBSERVE' in code:
                self.child_values.append(dict(self.widgets.getAll()))
            execute_python = (
                code.startswith('import os, json; os.environ[')
                or '_task_expected =' in code or '_pr_stack =' in code
                or '_pr_previous =' in code or '_scala_widget_keys =' in code
                or '_mp_snap_path =' in code or '_iso_p =' in code
                or '__NBRUN_RESOLVE__:' in code
                or 'for _k, _v in _wid_orig.items()' in code
                or code.startswith('%python\n# INIT_PARENT_WIDGETS')
                or code.startswith('%python\n# ASSERT_PARENT_WIDGETS')
            )
            if execute_python:
                self.executed.append(code)
                if '_scala_widget_keys =' in code:
                    self.phase_events.append('clear')
                output = io.StringIO()
                remove, text = self.widgets.remove, self.widgets.text
                if '_pr_stack =' in code and self.setup_failure:
                    if self.setup_failure == "after_remove":
                        def fail_remove(key):
                            remove(key)
                            raise RuntimeError("injected failure after removal")
                        def fail_text(key, value):
                            raise RuntimeError("injected failure before re-seeding")
                        self.widgets.remove, self.widgets.text = fail_remove, fail_text
                    elif self.setup_failure == "after_seed":
                        def fail_text(key, value):
                            text(key, value)
                            raise RuntimeError("injected failure after partial re-seeding")
                        self.widgets.text = fail_text
                if '_pr_previous =' in code:
                    self.restore_attempted = True
                    if restore_failure:
                        raise RuntimeError("injected restore failure")
                try:
                    with contextlib.redirect_stdout(output):
                        exec(code.removeprefix('%python\n'), self.ns)
                finally:
                    self.widgets.remove, self.widgets.text = remove, text
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': output.getvalue()}]}
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(child_tests, '_ExitKernel', Kernel)
    original_patch_common = child_tests._patch_common

    def patch_common(*args, **kwargs):
        original_patch_common(*args, **kwargs)
        # The scope scripts execute for real: keep their manifest under pytest.
        monkeypatch.setattr(jm, 'MIGRATE_OUTPUT', str(tmp_path / 'output'))

    monkeypatch.setattr(child_tests, '_patch_common', patch_common)
    monkeypatch.delenv('AIDP_PARAMS', raising=False)
    mapping = ('Map(' + ', '.join(f'"{key}" -> {value}' for key, value in bindings) + ')'
               if bindings else 'Map.empty[String, String]')
    if python_call:
        mapping = '{' + ', '.join(f'\"{key}\": {value}' for key, value in bindings) + '}'
    if mapping_source is not None:
        mapping = mapping_source
    call_magic = '%python' if python_call else '%scala'
    parent = [
        '%python\n# INIT_PARENT_WIDGETS\n'
        + '\n'.join(f'aidputils.widgets.text({key!r}, {value!r})' for key, value in parent_widgets.items()),
        '%scala\nval eagerMode = "child"\nlazy val lazyMode = aidputils.widgets.get("mode")',
        f'{call_magic}\ndbutils.notebook.run("/Workspace/child", 60, {mapping})',
        '%python\n# ASSERT_PARENT_WIDGETS\n'
        + f'assert dict(aidputils.widgets.getAll()) == {parent_widgets!r}',
    ]
    result, saved, _, _, _ = child_tests._run(
        monkeypatch, tmp_path,
        {'child': ['%scala\n// CHILD_OBSERVE\nval childMode = aidputils.widgets.get("mode")']},
        parent=parent, fire=False,
    )
    return result, instances[0], parent_widgets


@pytest.mark.parametrize('name, bindings, expected', CASES, ids=[case[0] for case in CASES])
def test_real_child_loop_evaluates_arguments_against_parent_state(monkeypatch, tmp_path, name, bindings, expected):
    result, kernel, parent = run_argument_case(monkeypatch, tmp_path, bindings)
    assert result['status'] == 'PASS', result
    assert kernel.child_values == [{**parent, **expected}]
    assert kernel.evaluated == [expression for _, expression in bindings]
    assert all(values == parent for values in kernel.reads)
    assert kernel.widgets.getAll() == parent
    assert kernel.phase_events == ['evaluate', 'clear', 'seed']


def test_argument_evaluation_failure_never_enters_child_and_restores_parent(monkeypatch, tmp_path):
    result, kernel, parent = run_argument_case(
        monkeypatch, tmp_path, [('mode', 'lazyMode')], capture_failure=True)
    assert result['status'] in ('PARTIAL', 'FAIL'), result
    assert kernel.child_values == []
    assert kernel.widgets.getAll() == parent
    assert kernel.phase_events == ['evaluate']


@pytest.mark.parametrize('expression, expected', [
    ('"value(a,b)"', 'value(a,b)'),
    (repr('{"nested": [1, 2]}'), '{"nested": [1, 2]}'),
    ('aidputils.widgets.get("other")', 'second'),
])
def test_python_child_consumer_preserves_complete_expression(monkeypatch, tmp_path, expression, expected):
    result, kernel, parent = run_argument_case(
        monkeypatch, tmp_path, [('mode', expression)], python_call=True)
    assert result['status'] == 'PASS', result
    assert kernel.child_values == [{**parent, 'mode': expected}]
    assert kernel.widgets.getAll() == parent
