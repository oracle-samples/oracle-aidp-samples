"""PR #248 R3: complete commented arguments and restoration after partial setup.

The regression contract is argument transport, not model rewrite semantics.
Both supported languages are exercised through the actual parent/child loop.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import job_migrate as jm
from test_scala_child_argument_evaluation import run_argument_case


SCALA_MAPS = [
    'Map("mode" -> "child", // execution mode\n "other" -> "child")',
    'Map("mode" -> "child", "other" -> "child" // target\n)',
    'Map(\n // leading comment: ) , "\n "mode" -> "child",\n "other" -> "child"\n)',
    'Map("mode" -> /* outer /* inner */ ) , " */ "child", "other" -> "child")',
    'Map[String, String]("mode" -> "child", "other" -> "child",\n)',
]
PYTHON_MAPS = [
    '{"mode": "child", # execution mode\n "other": "child"}',
    '{"mode": "child", "other": "child" # target\n}',
    '{\n # leading comment: ) , "\n "mode": "child", "other": "child"\n}',
]


@pytest.mark.parametrize('language,mapping', [('scala', m) for m in SCALA_MAPS] + [('python', m) for m in PYTHON_MAPS])
def test_comments_preserve_every_child_argument_in_real_loop(monkeypatch, tmp_path, language, mapping):
    bindings = [('mode', '"child"'), ('other', '"child"')]
    result, kernel, parent = run_argument_case(monkeypatch, tmp_path, bindings,
        mapping_source=mapping, python_call=language == 'python')
    assert result['status'] == 'PASS', result
    assert kernel.child_values == [{**parent, 'mode': 'child', 'other': 'child'}]
    assert kernel.widgets.getAll() == parent
    assert kernel.ns.get('_aidp_percent_run_iso_stack', []) == []
    if language == 'scala':
        calls = [code for code in kernel.executed if code.startswith('%scala')]
        assert not any(jm._find_notebook_run_calls(code) for code in calls)


@pytest.mark.parametrize('language,mapping', [
    ('scala', 'Map("mode" -> "child", computedKey -> "child")'),
    ('scala', 'Map("mode" -> "child", unsupportedEntry)'),
    ('scala', 'Map("mode" -> "child", "other" -> )'),
    ('scala', 'argumentMap'),
    ('python', '{"mode": "child", computed_key: "child"}'),
    ('python', '{"mode": "child", **argument_map}'),
    ('python', 'argument_map'),
])
def test_unsupported_mapping_never_seeds_a_subset(monkeypatch, tmp_path, language, mapping):
    result, kernel, parent = run_argument_case(monkeypatch, tmp_path, [('mode', '"child"')],
        mapping_source=mapping, python_call=language == 'python')
    assert result['status'] in ('PARTIAL', 'FAIL'), result
    assert kernel.child_values == []
    assert kernel.widgets.getAll() == parent
    assert kernel.ns.get('_aidp_percent_run_iso_stack', []) == []
    assert any('could not be completely parsed' in cell.get('note', '') for cell in result['cell_results'])
    assert kernel.phase_events == []


@pytest.mark.parametrize('stage', ['after_remove', 'after_seed'])
def test_partial_setup_failure_restores_parent_and_balances_stack(monkeypatch, tmp_path, stage):
    result, kernel, parent = run_argument_case(monkeypatch, tmp_path, [('mode', '"child"')], setup_failure=stage)
    assert result['status'] in ('PARTIAL', 'FAIL'), result
    assert kernel.child_values == []
    assert kernel.restore_attempted
    assert kernel.widgets.getAll() == parent
    assert kernel.ns['_aidp_percent_run_iso_stack'] == []


def test_restore_failure_is_a_counted_parent_failure(monkeypatch, tmp_path):
    result, kernel, _ = run_argument_case(monkeypatch, tmp_path, [('mode', '"child"')],
        setup_failure='after_remove', restore_failure=True)
    assert result['status'] in ('PARTIAL', 'FAIL'), result
    assert kernel.restore_attempted
    assert kernel.child_values == []
    assert any('restoration failed' in cell.get('note', '') for cell in result['cell_results'])


@pytest.mark.parametrize('language,expression', [
    ('scala', '"https://example.invalid/a/*b*/#c,d)"'),
    ('scala', '"""a " quote , ) // literal"""'),
    ('python', '"""a " quote , ) # literal"""'),
    ('python', '"https://example.invalid/a/*b*/#c,d)"'),
])
def test_literal_contents_survive_comment_masking(language, expression):
    mapping = f'Map("mode" -> {expression})' if language == 'scala' else f'{{"mode": {expression}}}'
    call = f'dbutils.notebook.run("/Workspace/child", 60, {mapping})'
    assert jm._parse_run_map_args(call, language) == [('mode', expression)]
    assert jm._find_notebook_run_calls(call, language)[0]['full'] == call


@pytest.mark.parametrize('language,mapping', [('scala', 'Map()'), ('scala', 'Map.empty[String, String]'), ('python', '{}')])
def test_genuinely_empty_arguments_stay_valid(language, mapping):
    assert jm._parse_run_map_args(f'dbutils.notebook.run("child", 60, {mapping})', language) == []
    assert jm._parse_run_map_args('dbutils.notebook.run("child", 60)', language) == []


def test_python_named_arguments_and_duplicate_keys_are_preserved():
    call = 'dbutils.notebook.run("child", 60, arguments={"mode": "first", # note\n "mode": "last"})'
    assert jm._parse_run_map_args(call, 'python') == [('mode', '"first"'), ('mode', '"last"')]
