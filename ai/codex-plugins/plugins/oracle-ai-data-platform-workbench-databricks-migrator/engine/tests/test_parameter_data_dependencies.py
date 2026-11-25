import importlib.util
from pathlib import Path
import unittest

BASE = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, BASE / 'scripts' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


extractor = load('data_check_extract')


def extract(source, parameters=None):
    return extractor.extract_from_notebook(
        {'cells': [{'cell_type': 'code', 'source': source, 'metadata': {}}]},
        runtime_parameters=parameters or {},
    )



class DependencyTests(unittest.TestCase):
    def test_widget_and_parameter_apis(self):
        for getter in ['dbutils.widgets.get', 'aidputils.widgets.get', 'oidlUtils.parameters.getParameter']:
            for reader in ['spark.read.table', 'spark.table']:
                with self.subTest(getter=getter, reader=reader):
                    result = extract(f'base = eval({getter}("base_data"))', {'base_data': f'{reader}("schema.input")'})
                    self.assertIn('schema.input', result['tables'])
                    self.assertEqual(result['read_references'][0]['cell_number'], 1)

    def test_unknown_or_unsupported_eval_is_visible(self):
        for value in [None, '', 'custom_loader()', 'spark.read.table(', 'eval("nested")', 'x' * 10001]:
            with self.subTest(value=str(value)[:40]):
                result = extract('base = eval(dbutils.widgets.get("base_data"))', {'base_data': value})
                self.assertTrue(result['not_analyzed'])
                self.assertFalse(result['tables'])

    def test_dynamic_table_argument_is_unresolved(self):
        result = extract('base = eval(dbutils.widgets.get("base_data"))', {'base_data': 'spark.read.table(unknown_name)'})
        self.assertTrue(result['unresolved_scopes'])

    def test_eval_string_is_never_executed(self):
        result = extract('base = eval(dbutils.widgets.get("base_data"))', {'base_data': '__import__("sys").exit(1)'})
        self.assertTrue(result['not_analyzed'])

    def test_direct_read_and_lineage_preserved(self):
        for source, params in [
            ('base = spark.read.table("schema.input")\nfiltered = base.filter("id > 0")', {}),
            ('base = eval(dbutils.widgets.get("base_data"))\nfiltered = base.filter("id > 0")', {'base_data': 'spark.read.table("schema.input")'}),
        ]:
            result = extract(source, params)
            self.assertIn('schema.input', result['tables'])
            self.assertTrue(any(r.get('filters') for r in result['read_references']))


def test_manifest_parameter_reaches_scanner_and_missing_table_report(monkeypatch):
    """The existing manifest path must work without runbook changes."""
    import asyncio
    import sys
    sys.path.insert(0, str(BASE / 'scripts'))
    import check_data_availability_for_workflow as checker

    notebook_path = '/Workspace/input.ipynb'
    notebook = {'cells': [{'cell_type': 'code', 'metadata': {}, 'source':
        'base = eval(dbutils.widgets.get("base_data"))'}]}
    parameters = {'base_data': 'spark.read.table("schema.input")'}
    task = {'notebook_path': notebook_path, 'base_parameters': {}}
    by_notebook = checker.build_notebook_runtime_parameters([task], parameters)
    monkeypatch.setattr(checker, 'get_oci_signer', lambda _profile: ({}, object()))
    monkeypatch.setattr(checker, '_download_notebook_json', lambda *_args: notebook)
    refs = asyncio.run(checker.extract_data_refs(
        None, [notebook_path], aidp_base='https://example.invalid',
        lake_ocid='ocid1.aidataplatform.oc1.iad.example', workspace_id='workspace',
        runtime_parameters=parameters, runtime_parameters_by_notebook=by_notebook,
    ))
    assert 'schema.input' in refs[notebook_path]['tables']
    assert refs[notebook_path]['read_references'][0]['object_name'] == 'schema.input'

    # The report builder must retain the discovered dependency and probe error.
    report = checker.generate_report(
        refs, {'schema.input': {'exists': False, 'error': 'TABLE_OR_VIEW_NOT_FOUND'}},
        {}, {}, {}, 'example',
    )
    entry = report['notebooks'][notebook_path]['read_tables'][0]
    assert entry['table'] == 'schema.input'
    assert entry['exists'] is False
    assert report['summary']['tables_missing'] == 1


def _production_report(monkeypatch, source, expression):
    """Exercise real parameter binding, extraction, reporting and verdicts."""
    import asyncio
    import contextlib
    import io
    import sys
    sys.path.insert(0, str(BASE / 'scripts'))
    import check_data_availability_for_workflow as checker

    path = '/Workspace/input.ipynb'
    notebook = {'cells': [{'cell_type': 'code', 'metadata': {}, 'source': source}]}
    parameters = {'base_data': expression}
    by_notebook = checker.build_notebook_runtime_parameters(
        [{'notebook_path': path, 'base_parameters': {}}], parameters)
    monkeypatch.setattr(checker, 'get_oci_signer', lambda *_: ({}, object()))
    monkeypatch.setattr(checker, '_download_notebook_json', lambda *_: notebook)
    with contextlib.redirect_stdout(io.StringIO()):
        refs = asyncio.run(checker.extract_data_refs(
            None, [path], aidp_base='https://example.invalid',
            lake_ocid='fixture', workspace_id='fixture',
            runtime_parameters=parameters, runtime_parameters_by_notebook=by_notebook))
        tables = {item['table'] if isinstance(item, dict) else item
                  for item in refs[path]['tables']}
        report = checker.generate_report(
            refs, {table: {'exists': table == 'schema.available'} for table in tables},
            {}, {}, {}, 'fixture')
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        checker.print_report(report)
    verdict = next(line.strip() for line in output.getvalue().splitlines()
                   if 'RESULT:' in line)
    return refs[path], report, verdict


import pytest


@pytest.mark.parametrize('source', [
    'df = eval(dbutils.widgets.get("base_data"))',
    'df = eval(dbutils.widgets.get("base_data")).filter("id > 0")',
    'df = spark.table("schema.available").join('
    'eval(dbutils.widgets.get("base_data")), "id")',
])
@pytest.mark.parametrize('reader', ['spark.read.table', 'spark.table'])
def test_unsupported_eval_stays_unverified_through_assignment_chain_join(
    monkeypatch, source, reader,
):
    refs, report, verdict = _production_report(
        monkeypatch, source, f'{reader}("schema.missing").filter("id > 0")')
    gaps = [item for item in refs['not_analyzed'] if item.get('kind') == 'dynamic_eval']
    assert len(gaps) == 1
    assert report['summary']['unresolved'] >= 1
    assert 'manual review required' in verdict.lower()
    assert 'ALL DEPENDENCIES AVAILABLE' not in verdict


@pytest.mark.parametrize('namespace_arguments', [
    ', {"spark": spark, "table_name": "schema.missing"}',
    ', {"spark": spark}, {"table_name": "schema.missing"}',
    ', globals={"spark": spark, "table_name": "schema.missing"}',
    ', locals={"table_name": "schema.missing"}',
])
def test_explicit_eval_namespaces_never_certify_notebook_binding(
    monkeypatch, namespace_arguments,
):
    source = ('table_name = "schema.available"\n'
              'df = eval(dbutils.widgets.get("base_data")' + namespace_arguments + ')')
    refs, report, verdict = _production_report(
        monkeypatch, source, 'spark.read.table(table_name)')
    assert 'schema.available' not in refs['tables']
    assert 'schema.missing' not in refs['tables']
    assert any('namespace' in item['reason'] for item in refs['not_analyzed'])
    assert report['summary']['unresolved'] >= 1
    assert 'manual review required' in verdict.lower()
    assert 'ALL DEPENDENCIES AVAILABLE' not in verdict


@pytest.mark.parametrize('expression', [None, 'spark.read.table(unknown_name)'])
def test_missing_chained_eval_input_stays_unverified(monkeypatch, expression):
    _, report, verdict = _production_report(
        monkeypatch, 'df = eval(dbutils.widgets.get("base_data")).filter("id > 0")',
        expression)
    assert report['summary']['unresolved'] >= 1
    assert 'ALL DEPENDENCIES AVAILABLE' not in verdict


@pytest.mark.parametrize('source', [
    'df = eval(dbutils.widgets.get("base_data")).filter("id > 0")',
    'df = spark.table("schema.available").join('
    'eval(dbutils.widgets.get("base_data")), "id")',
])
def test_supported_eval_through_chain_join_reports_missing_table(monkeypatch, source):
    refs, report, verdict = _production_report(
        monkeypatch, source, 'spark.read.table("schema.missing")')
    assert not any(item.get('kind') == 'dynamic_eval' for item in refs['not_analyzed'])
    assert report['summary']['tables_missing'] == 1
    assert 'MISSING' in verdict
    assert 'ALL DEPENDENCIES AVAILABLE' not in verdict


def test_repeated_lineage_walk_deduplicates_eval_gap():
    import ast
    analyzer = extractor._PythonReferenceAnalyzer(
        {'base_data': 'custom_loader()'})
    node = ast.parse('eval(dbutils.widgets.get("base_data"))', mode='eval').body
    analyzer.cell_number = 1
    analyzer._lineage_from_expr(node, record=False)
    analyzer._lineage_from_expr(node, record=True)
    analyzer._lineage_from_expr(node, record=False)
    assert len(analyzer.result()['not_analyzed']) == 1


@pytest.mark.parametrize('source', [
    'def load():\n    return eval(dbutils.widgets.get("base_data"))\ndf = load()',
    'load = lambda: eval(dbutils.widgets.get("base_data"))\ndf = load()',
    'assert eval(dbutils.widgets.get("base_data")) is not None',
])
@pytest.mark.parametrize('expression', [
    'spark.read.table("schema.missing")',
    'spark.table("schema.missing")',
    'custom_loader()',
    None,
])
def test_unvisited_eval_positions_require_manual_review(monkeypatch, source, expression):
    refs, report, verdict = _production_report(monkeypatch, source, expression)
    gaps = [item for item in refs['not_analyzed'] if item.get('kind') == 'dynamic_eval']
    assert len(gaps) == 1
    assert report['summary']['unresolved'] >= 1
    assert 'manual review required' in verdict.lower()
    assert 'ALL DEPENDENCIES AVAILABLE' not in verdict


def test_unvisited_eval_does_not_resolve_against_wrong_scope(monkeypatch):
    source = ('table_name = "schema.available"\n'
              'def load(table_name):\n'
              '    return eval(dbutils.widgets.get("base_data"))\n'
              'df = load("schema.missing")')
    refs, report, verdict = _production_report(
        monkeypatch, source, 'spark.read.table(table_name)')
    assert 'schema.available' not in refs['tables']
    assert report['summary']['unresolved'] >= 1
    assert 'manual review required' in verdict.lower()


@pytest.mark.parametrize('inline', [False, True])
def test_oversized_eval_has_explicit_reason_and_bounded_finding(monkeypatch, inline):
    expression = 'x' * 10001
    source = ('df = eval(' + repr(expression) + ')' if inline else
              'df = eval(dbutils.widgets.get("base_data"))')
    refs, report, verdict = _production_report(monkeypatch, source, expression)
    gap = next(item for item in refs['not_analyzed'] if item.get('kind') == 'dynamic_eval')
    assert '10000' in gap['reason']
    assert len(gap['expression']) <= 1000
    if inline:
        assert gap['expression'].endswith('...')
    assert report['summary']['unresolved'] >= 1
    assert 'manual review required' in verdict.lower()


def test_whole_tree_eval_findings_keep_cell_and_call_locations():
    nb = {'cells': [
        {'cell_type': 'code', 'source': 'assert eval("unknown()") or eval("unknown()")'},
        {'cell_type': 'code', 'source': 'assert eval("unknown()")'},
    ]}
    result = extractor.extract_from_notebook(nb)
    gaps = [item for item in result['not_analyzed'] if item.get('kind') == 'dynamic_eval']
    assert len(gaps) == 3
    assert [item['cell_number'] for item in gaps].count(1) == 2
    assert [item['cell_number'] for item in gaps].count(2) == 1
