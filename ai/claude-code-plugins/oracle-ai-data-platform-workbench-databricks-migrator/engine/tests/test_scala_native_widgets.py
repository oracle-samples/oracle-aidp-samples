"""REF-202: dynamically-created widgets share native Python/Scala storage.

Evidence: Payments_Feature_Creation, migration run aaaaaaaa-0000-4000-8000-000000000211;
Python created base_data_db at 04:04:29 UTC, Scala read an absent Spark param at
04:04:38 UTC. Native cross-language get/text verified live on 2026-10-13.
"""
import asyncio
import contextlib
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm
import agent_migrate


@pytest.mark.parametrize("expression", ['"base_data_db"', 'key', '"quoted-key"'])
def test_scala_required_widget_read_uses_native_api(expression):
    source = f'%scala\nval db = dbutils.widgets.get({expression})'
    migrated = jm._preprocess_scala_cell_source(source)
    assert f'val db = aidputils.widgets.get({expression})' in migrated
    assert '// val db = dbutils.widgets.get(' in migrated
    assert 'getParameter' not in migrated
    assert 'spark.aidp.param' not in migrated
    assert jm._preprocess_scala_cell_source(migrated) == migrated


def test_commented_scala_widget_read_is_preserved():
    source = '%scala\n// val old = dbutils.widgets.get("old")\nval x = 1'
    assert jm._preprocess_scala_cell_source(source) == source


@pytest.mark.parametrize("child", [False, True])
def test_native_widget_read_executes_and_saves_through_real_cell_loop(monkeypatch, tmp_path, child):
    from test_child_exit_conversion import _run
    sources = [
        '%python\naidputils.widgets.text("base_data_db", "fixture_db")',
        '%scala\nval db = dbutils.widgets.get("base_data_db")',
        '%scala\nrequire(db == "fixture_db")',
    ]
    if child:
        # Inline executes the pass-1 migrated dependency; model services are mocked
        # by this harness, so prepare the Scala fixture with the real preprocessor.
        sources = [jm._preprocess_scala_cell_source(s) if s.startswith('%scala') else s
                   for s in sources]
    kwargs = {} if child else {"parent": sources}
    result, saved, kernel, _, _ = _run(monkeypatch, tmp_path, {"child": sources}, **kwargs)
    assert result['status'] in ('ok', 'PASS'), result
    executed = '\n'.join(kernel.executed)
    assert 'val db = aidputils.widgets.get("base_data_db")' in executed
    assert 'spark.aidp.param.' not in executed
    if child:
        final_sources = saved['child']
    else:
        nb = next(json.loads(text) for path, text in kernel.files.items() if path.endswith('final.ipynb'))
        final_sources = [''.join(c['source']) for c in nb['cells']]
    assert any('val db = aidputils.widgets.get("base_data_db")' in s for s in final_sources)
    assert not any('getParameter("base_data_db"' in s for s in final_sources)


@pytest.mark.parametrize('name', ['CELL_MIGRATE_PROMPT'])
def test_job_prompts_support_scala_native_widgets(name):
    prompt = getattr(jm, name)
    assert 'aidputils.widgets' in prompt
    assert 'aidputils is Python-only' not in prompt
    assert 'aidputils not in Scala kernel' not in prompt


@pytest.mark.parametrize('name', ['ANALYSIS_PROMPT', 'MIGRATION_PROMPT', 'FIX_PROMPT'])
def test_agent_prompts_support_scala_native_widgets(name):
    prompt = getattr(agent_migrate, name)
    assert 'aidputils.widgets' in prompt
    assert 'oidlUtils.parameters.getParameter("name", <default>)' not in prompt
    assert 'aidputils is NOT available in Scala' not in prompt


@pytest.mark.parametrize('bindings', [[], [('mode', 'scalaMode')], [('mode', 'scalaMode'), ('count', '42')]])
def test_scala_child_argument_setup_uses_widgets_and_checks_completion(bindings):
    requests = []
    widgets = {'mode': 'parent'}
    class Widgets:
        def getAll(self): return dict(widgets)
        def remove(self, key): widgets.pop(key)
    namespace = {'aidputils': SimpleNamespace(widgets=Widgets())}
    class Session:
        async def execute(self, code, **kwargs):
            requests.append(code)
            if code.startswith('%scala'):
                output = jm._ISO_OK + '\n'
            else:
                out = io.StringIO()
                with contextlib.redirect_stdout(out): exec(code, namespace)
                output = out.getvalue()
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': output}]}
    asyncio.run(jm._seed_scala_notebook_run_validation_bindings(Session(), bindings, 'fixture-job'))
    assert len(requests) == 3
    assert 'aidputils.widgets.text(key, value)' in requests[2]
    assert 'require(aidputils.widgets.get(key) == value' in requests[2]
    assert 'spark.aidp.param' not in '\n'.join(requests)
    for key, value in bindings:
        assert f'({json.dumps(key)}, String.valueOf({value}))' in requests[0]


@pytest.mark.parametrize('failure_stage', ['evaluate', 'clear', 'seed'])
def test_scala_child_argument_setup_fails_closed_on_stream_only_errors(failure_stage):
    requests = []
    class Session:
        async def execute(self, code, **kwargs):
            requests.append(code)
            stage = ('evaluate' if 'val _aidpScalaWidgetArgs' in code else
                     'seed' if code.startswith('%scala') else 'clear')
            fail = stage == failure_stage
            output = 'Command ID fixture failed with widget error' if fail else jm._ISO_OK + '\n'
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': output}]}
    with pytest.raises(RuntimeError, match='Scala notebook.run widget'):
        asyncio.run(jm._seed_scala_notebook_run_validation_bindings(
            Session(), [('mode', 'scalaMode')], 'fixture-job'))
    assert len(requests) == {'evaluate': 1, 'clear': 2, 'seed': 3}[failure_stage]


@pytest.mark.parametrize('empty_sibling', [False, True])
def test_scala_notebook_run_arguments_enter_and_restore_scope_in_real_loop(monkeypatch, tmp_path, empty_sibling):
    from test_child_exit_conversion import _run
    notebooks = {'childA': ['%scala\nval childMode = dbutils.widgets.get("mode")'],
                 'childB': ['%scala\nval siblingMode = dbutils.widgets.get("mode")']}
    parent = ['%scala\nval mode = "runtime_value"',
              '%scala\ndbutils.notebook.run("/Workspace/childA", 60, Map("mode" -> mode))']
    if empty_sibling:
        parent.append('%scala\ndbutils.notebook.run("/Workspace/childB", 60, Map.empty[String, String])')
    result, saved, kernel, _, _ = _run(monkeypatch, tmp_path, notebooks, parent=parent)
    assert result['status'] == 'PASS', result
    executed = '\n'.join(kernel.executed)
    assert 'String.valueOf(mode)' in executed
    assert 'aidputils.widgets.text(key, value)' in executed
    assert '_aidp_percent_run_iso_stack.pop()' in executed
    assert 'spark.aidp.param.' not in executed
    if empty_sibling:
        assert 'Map.empty[String, String]' in executed


@pytest.mark.parametrize('magic', ['%python', '%scala'])
@pytest.mark.parametrize('operation', ['get("key")', 'text("key", "customer_default")'])
def test_static_analyzer_guidance_uses_native_widgets_and_preserves_registration(magic, operation):
    import cell_analyzer
    plan = cell_analyzer.analyze_cell_static(0, f'{magic}\ndbutils.widgets.{operation}', [], [])
    guidance = '\n'.join(plan['changes_needed'])
    assert 'aidputils.widgets' in guidance
    assert 'oidlUtils.parameters.getParameter' not in guidance
    assert 'widget registration not supported' not in guidance
    if operation.startswith('text'):
        assert 'preserve registration and customer defaults' in guidance
