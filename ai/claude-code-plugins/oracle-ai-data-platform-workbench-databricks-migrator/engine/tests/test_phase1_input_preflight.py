"""Regression coverage for static Phase 1 inputs; fixtures never execute."""
import json
from pathlib import Path
import unittest
import pytest
import asyncio

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import data_check_extract as extractor


def scan(source, parameters=None, modules=None):
    return extractor.extract_from_notebook(
        {'cells': [{'cell_type': 'code', 'source': source}]},
        runtime_parameters=parameters, import_namespaces={
            name: extractor.module_namespace(source)[0]
            for name, source in (modules or {}).items()})


class InputPreflightTests(unittest.TestCase):
    def test_widget_defaults_become_read_dependencies(self):
        result = scan('dbutils.widgets.text("inputTable", "test.input")\n'
                      'table = dbutils.widgets.get("inputTable").strip()\n'
                      'df = spark.read.table(table)')
        self.assertIn('test.input', result['tables'])

    def test_explicit_runtime_overrides_default(self):
        result = scan('dbutils.widgets.text("inputTable", "test.default")\n'
                      'spark.table(dbutils.widgets.get("inputTable"))',
                      {'inputTable': 'test.override'})
        self.assertIn('test.override', result['tables'])
        self.assertNotIn('test.default', result['tables'])

    def test_explicit_blank_required_input_is_invalid(self):
        result = scan('dbutils.widgets.text("inputTable", "test.default")\n'
                      'table = dbutils.widgets.get("inputTable").strip()\n'
                      'spark.table(table)', {'inputTable': ''})
        self.assertTrue(any(item['type'] == 'INVALID_REQUIRED_INPUT'
                            for item in result['analysis_errors']))
        self.assertNotIn('test.default', result['tables'])

    def test_optional_blank_widget_is_valid(self):
        result = scan('dbutils.widgets.text("optional", "")\n'
                      'optional = dbutils.widgets.get("optional")')
        self.assertFalse(result['analysis_errors'])

    def test_secret_widget_values_never_propagate(self):
        result = scan('dbutils.widgets.text("serviceApiKey", "synthetic-secret")\n'
                      'token = dbutils.widgets.get("serviceApiKey")',
                      {'serviceApiKey': 'synthetic-runtime-secret'})
        result.pop('cells_source')  # internal source text is intentionally preserved
        self.assertNotIn('synthetic-secret', json.dumps(result))
        self.assertNotIn('synthetic-runtime-secret', json.dumps(result))

    @pytest.mark.xfail(strict=True, reason='helper inference assigned to #306; maintainer accepted scope residual in review 5680660725')
    def test_helper_parameter_and_constant_reads(self):
        modules = {'source': '''
PROMPTS = "test.prompts"
def resolve_source(spark, table=""):
    if table and table.lower() != "auto":
        return table
    return spark.table("test.registry").first()[0]
def load(spark, table):
    return spark.read.table(table)
def prompt(spark):
    return spark.read.table(PROMPTS).first()
'''}
        result = scan('import source\n'
                      'table = source.resolve_source(spark, "test.input")\n'
                      'df = source.load(spark, table)\n'
                      'version = source.prompt(spark)', modules=modules)
        self.assertIn('test.input', result['tables'])
        self.assertIn('test.prompts', result['tables'])
        self.assertNotIn('test.registry', result['tables'])
        self.assertTrue(result['not_analyzed'])  # row content is not verified

    @pytest.mark.xfail(strict=True, reason='helper inference assigned to #306; maintainer accepted scope residual in review 5680660725')
    def test_aliased_from_import_and_nested_call(self):
        result = scan('from helper import load as fetch\nfetch(spark, "test.input")',
                      modules={'helper': 'def read(spark, t):\n return spark.table(t)\n'
                                           'def load(spark, table):\n return read(spark, table)'})
        self.assertIn('test.input', result['tables'])

    def test_helper_source_is_never_executed(self):
        result = scan('import helper\nspark.table(helper.load(spark))', modules={'helper':
                      'raise RuntimeError("must not execute")\n'
                      'def load(spark):\n return spark.table("test.input")'})
        self.assertNotIn('test.input', result['tables'])
        self.assertTrue(result['unresolved_scopes'])

    def test_unknown_branch_does_not_fabricate_return(self):
        result = scan('import helper\nt = helper.resolve()\nspark.table(t)',
                      modules={'helper': 'def resolve():\n if external():\n  return "test.a"\n if True:\n  return "test.b"'})
        self.assertNotIn('t', result['resolved_symbols'])
        self.assertTrue(result['unresolved_scopes'])

    def test_recursive_helper_is_bounded_and_reported(self):
        result = scan('import helper\nspark.table(helper.load(spark))',
                      modules={'helper': 'def load(spark):\n return load(spark)'})
        self.assertTrue(result['unresolved_scopes'])

    def test_credential_availability_is_explicitly_unverified(self):
        result = scan('key = dbutils.secrets.get(scope="scope-ref", key="key-ref")')
        self.assertTrue(any(item['kind'] == 'credential_availability'
                            for item in result['not_analyzed']))
        self.assertNotIn('key', result['resolved_symbols'])




@pytest.mark.xfail(strict=True, reason='helper inference assigned to #306; maintainer accepted scope residual in review 5680660725')
def test_imported_helper_reads_reach_workflow_report_inputs(monkeypatch):
    import asyncio
    import check_data_availability_for_workflow as checker
    folder = '/Workspace/export/notebooks/Users/test/project'
    path = folder + '/input.ipynb'
    notebook = {'cells': [{'cell_type': 'code', 'source':
                'import helper\n'
                'dbutils.widgets.text("input", "test.default")\n'
                't = dbutils.widgets.get("input")\n'
                'helper.load(spark, "test.input")\n'
                'spark.table(t).withColumn("x", expression)\n'
                'secret = dbutils.secrets.get("scope-ref", "key-ref")'}]}
    downloads = []
    monkeypatch.setattr(checker, 'get_oci_signer', lambda _: ({}, None))
    monkeypatch.setattr(checker, '_download_notebook_json', lambda *args: notebook)
    monkeypatch.setattr(checker, '_list_workspace_entries', lambda *args:
                        (frozenset({'helper.py'}) if args[-1] == folder else frozenset(),
                         frozenset()))

    def download(*args):
        downloads.append(args[-1])
        return b'def load(spark, table):\n return spark.table(table)'

    monkeypatch.setattr(checker, '_download_workspace_file', download)
    results = asyncio.run(checker.extract_data_refs(
        None, [path], aidp_base='https://example.invalid',
        lake_ocid='test-lake', workspace_id='test-workspace',
        runtime_parameters={'input': ''}))
    result = results[path]
    assert 'test.input' in result['tables']
    assert any(item.get('type') == 'INVALID_REQUIRED_INPUT' for item in result['unresolved'])
    assert any(item.get('kind') == 'credential_availability' for item in result['not_analyzed'])
    assert downloads == [folder + '/helper.py']


def test_blank_date_parameter_does_not_fabricate_partition_path():
    result = scan('day = dbutils.widgets.get("run_date")\n'
                  'spark.read.parquet(f"s3://test-bucket/day={day}")',
                  {'run_date': ''})
    assert not any('day=' in str(path) for path in result['paths'])
    assert result['unresolved_scopes']


# A reviewer's 2026-10-28 finding: a present old table hid the new
# widget input; opaque/rebound helper calls were incorrectly made concrete.
# Exercise the real resolver -> extraction -> report path, stubbing only I/O.
# #306 owns helper inference; these uncertainty pins remain in #295.
def _workflow_report(monkeypatch, cells, parameters=None, helpers=None):
    import check_data_availability_for_workflow as checker
    folder = '/Workspace/export/notebooks/Users/test/project'
    path = folder + '/input.ipynb'
    notebook = {'cells': [{'cell_type': 'code', 'source': source} for source in cells]}
    helpers = helpers or {}
    monkeypatch.setattr(checker, 'get_oci_signer', lambda _: ({}, None))
    monkeypatch.setattr(checker, '_download_notebook_json', lambda *args: notebook)
    monkeypatch.setattr(checker, '_list_workspace_entries', lambda *args:
                        (frozenset(name + '.py' for name in helpers)
                         if args[-1] == folder else frozenset(), frozenset()))
    monkeypatch.setattr(checker, '_download_workspace_file', lambda *args:
                        helpers[args[-1].rsplit('/', 1)[-1][:-3]].encode())
    refs = asyncio.run(checker.extract_data_refs(
        None, [path], aidp_base='https://example.invalid',
        lake_ocid='test-lake', workspace_id='test-workspace',
        runtime_parameters=parameters or {}))
    # If identity selection is wrong, the old table's existence must not
    # turn an actually missing/unknown input into a clean report.
    names = [item if isinstance(item, str) else item['table']
             for item in refs[path].get('tables', [])]
    tables = {name: {'exists': name != 'demo.new'} for name in names}
    report = checker.generate_report(refs, tables, {}, {},
                                    {'python': {}, 'scala': {}}, 'widget-contract')
    return report, report['notebooks'][path]


@pytest.mark.parametrize('api', ['dbutils', 'aidputils'])
@pytest.mark.parametrize('method', ['text', 'dropdown', 'combobox', 'multiselect'])
@pytest.mark.parametrize('lifecycle', ['unchanged', 'remove', 'removeAll'])
@pytest.mark.parametrize('runtime,expected', [
    ('absent', 'demo.new'), ('demo.override', 'demo.override'), ('', None),
    ('   ', None), (None, None),
])
def test_widget_lifecycle_report_matrix(monkeypatch, api, method, lifecycle, runtime, expected):
    def register(default):
        choices = ', ["demo.old", "demo.new"]' if method != 'text' else ''
        return f'{api}.widgets.{method}("input", {default!r}{choices})'
    cells = [register('demo.old')]
    if lifecycle != 'unchanged':
        cells.append(f'{api}.widgets.remove("input")' if lifecycle == 'remove'
                     else f'{api}.widgets.removeAll()')
    cells.append(register('demo.new') + f'\nspark.table({api}.widgets.get("input"))')
    parameters = {} if runtime == 'absent' else {'input': runtime}
    report, entry = _workflow_report(monkeypatch, cells, parameters)
    if runtime == 'absent' and lifecycle == 'unchanged':
        expected = 'demo.old'  # Duplicate registration retains an existing widget.
    if expected:
        assert [item['table'] for item in entry['read_tables']] == [expected]
        assert report['summary']['tables_missing'] == (expected == 'demo.new')
        assert report['summary']['unresolved'] == 0
    else:
        assert not entry['read_tables']
        assert report['summary']['tables_found'] == 0
        assert report['summary']['unresolved'] > 0
        if isinstance(runtime, str):
            assert any(item.get('type') == 'INVALID_REQUIRED_INPUT'
                       for item in entry['analysis_errors'])


@pytest.mark.parametrize('operation', ['remove("input")', 'removeAll()', 'remove(dynamic_name)'])
@pytest.mark.parametrize('conditional', [False, True])
def test_removed_or_uncertain_widget_cannot_report_old_table(monkeypatch, operation, conditional):
    mutation = 'dbutils.widgets.' + operation
    if conditional:
        mutation = 'if external():\n    ' + mutation
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text("input", "demo.old")', mutation,
        'spark.table(dbutils.widgets.get("input"))'])
    assert not entry['read_tables']
    assert report['summary']['tables_found'] == 0
    assert report['summary']['unresolved'] > 0


@pytest.mark.parametrize('operation', ['remove("input")', 'removeAll()'])
def test_conditional_removal_and_recreation_stays_uncertain(monkeypatch, operation):
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text("input", "demo.old")',
        'if external():\n    dbutils.widgets.' + operation,
        'dbutils.widgets.text("input", "demo.new")\nspark.table(dbutils.widgets.get("input"))'])
    assert not entry['read_tables']
    assert report['summary']['tables_found'] == 0
    assert report['summary']['unresolved'] > 0


@pytest.mark.parametrize('control', [
    'if external():\n    dbutils.widgets.removeAll()',
    'for item in external():\n    dbutils.widgets.removeAll()',
    'def reset():\n    dbutils.widgets.removeAll()',
])
def test_uncertain_widget_mutation_recovers_after_definite_remove(monkeypatch, control):
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text("input", "demo.old")', control,
        'dbutils.widgets.remove("input")\ndbutils.widgets.text("input", "demo.new")',
        'spark.table(dbutils.widgets.get("input"))'])
    assert [item['table'] for item in entry['read_tables']] == ['demo.new']
    assert report['summary']['tables_missing'] == 1


@pytest.mark.parametrize('style', ['import helper', 'from helper import resolve'])
@pytest.mark.parametrize('rebound', [False, True])
def test_reviewed_helper_false_clean_regressions_remain_unresolved(monkeypatch, style, rebound):
    source = ('def resolve(flag=None):\n    table = "demo.default"\n'
              '    if flag:\n        table = "demo.alternate"\n    return table')
    name = 'helper.resolve' if style == 'import helper' else 'resolve'
    mutation = f'{name} = lambda: external()\n' if rebound else ''
    call = f'{name}()' if rebound else f'{name}(external())'
    report, entry = _workflow_report(monkeypatch,
        [f'{style}\n{mutation}table = {call}\nspark.table(table)'],
        helpers={'helper': source})
    assert not entry['read_tables']
    assert report['summary']['tables_found'] == 0
    assert report['summary']['unresolved'] > 0


@pytest.mark.parametrize('reader', ['spark.table(value)', 'spark.read.parquet(value)',
                                    'spark.read.parquet("s3://bucket/known", value)',
                                    'spark.read.parquet(*["s3://bucket/known", value])'])
@pytest.mark.parametrize('blank', ['', '   '])
def test_blank_widget_read_preserves_307_all_or_nothing_report(monkeypatch, reader, blank):
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text("input", "demo.default")\n'
        'value = dbutils.widgets.get("input")\n' + reader], {'input': blank})
    assert any(item.get('type') == 'INVALID_REQUIRED_INPUT' for item in entry['analysis_errors'])
    assert not entry['read_tables']
    assert all(item.get('source', '').strip() for item in entry['read_paths'])
    assert report['summary']['unresolved'] > 0


def test_widget_keyword_registration_and_named_remove(monkeypatch):
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text(name="input", defaultValue="demo.old")',
        'dbutils.widgets.remove(name="input")',
        'dbutils.widgets.text(name="input", defaultValue="demo.new")\n'
        'spark.table(dbutils.widgets.get("input"))'])
    assert [item['table'] for item in entry['read_tables']] == ['demo.new']
    assert report['summary']['tables_missing'] == 1


@pytest.mark.parametrize('deferred', [
    'reset = lambda: dbutils.widgets.removeAll()',
    'resets = (dbutils.widgets.removeAll() for item in external())',
    'try:\n    dbutils.widgets.removeAll()\n    external()\nexcept Exception:\n    spark.table(dbutils.widgets.get("input"))',
])
def test_scoped_or_exceptional_widget_mutation_preserves_uncertainty(monkeypatch, deferred):
    report, entry = _workflow_report(monkeypatch, [
        'dbutils.widgets.text("input", "demo.old")', deferred,
        'spark.table(dbutils.widgets.get("input"))'])
    assert not entry['read_tables']
    assert report['summary']['tables_found'] == 0
    assert report['summary']['unresolved'] > 0


# Live run aaaaaaaa-0000-4000-8000-000000000202: try/except registration
# hid an explicitly blank runtime input behind uncertain widget existence.
@pytest.mark.parametrize('api', ['dbutils', 'aidputils'])
@pytest.mark.parametrize('state', ['try_registration', 'conditional_registration',
                                   'conditional_removal', 'removed'])
@pytest.mark.parametrize('runtime', ['absent', '', ' \t', None, 'demo.override'])
@pytest.mark.parametrize('reader', ['spark.table(value)', 'spark.read.parquet(value)'])
def test_explicit_blank_survives_uncertain_widget_state(monkeypatch, api, state, runtime, reader):
    register = f'{api}.widgets.text("input", "demo.old")'
    if state == 'try_registration':
        cells = [f'try:\n    {register}\nexcept Exception:\n    pass']
    elif state == 'conditional_registration':
        cells = [f'if external():\n    {register}']
    else:
        mutation = f'{api}.widgets.remove("input")'
        if state == 'conditional_removal':
            mutation = 'if external():\n    ' + mutation
        cells = [register, mutation]
    cells.append(f'value = {api}.widgets.get("input")\n' + reader)
    parameters = {} if runtime == 'absent' else {'input': runtime}
    report, entry = _workflow_report(monkeypatch, cells, parameters)
    assert not entry['read_tables'] and not entry['read_paths']
    assert report['summary']['unresolved'] > 0
    errors = [item for item in entry['analysis_errors']
              if item.get('type') == 'INVALID_REQUIRED_INPUT']
    if isinstance(runtime, str) and not runtime.strip():
        assert len(errors) == 1
        assert errors[0]['argument_expression'] == 'value'
        assert errors[0]['message'] == 'Blank table/path consumed by Spark read'
    else:
        # A nonblank override cannot certify widget existence after removal
        # or on an unknown branch; preserve the existing uncertainty contract.
        assert not errors


def test_original_blank_input_after_exception_wrapped_registration(monkeypatch):
    report, entry = _workflow_report(monkeypatch, [
        'try:\n    dbutils.widgets.text("other", "unused")\n'
        '    dbutils.widgets.text("o1_Path", "demo.old")\nexcept Exception:\n    pass\n'
        'O1_PATH = dbutils.widgets.get("o1_Path")',
        'result = spark.table(O1_PATH).withColumn("hr", external())',
    ], {'o1_Path': ''})
    assert entry['analysis_errors'] == [{
        'type': 'INVALID_REQUIRED_INPUT', 'cell_number': 2, 'line_number': 1,
        'message': 'Blank table/path consumed by Spark read',
        'argument_expression': 'O1_PATH',
    }]
    assert not entry['read_tables']
    assert report['summary']['unresolved'] > 0


def test_sensitive_blank_runtime_stays_excluded_with_uncertain_widgets(monkeypatch):
    _, entry = _workflow_report(monkeypatch, [
        'try:\n    dbutils.widgets.text("serviceApiKey", "planted-secret")\n'
        'except Exception:\n    pass\n'
        'spark.table(dbutils.widgets.get("serviceApiKey"))',
    ], {'serviceApiKey': ''})
    assert not entry['analysis_errors']
    assert not entry['read_tables']
    assert entry['unresolved_scopes']
    assert 'planted-secret' not in json.dumps(entry)


# A reviewer's 2026-10-29 reporting repro: call enumeration cannot establish
# whether a widget mutation executes. Unsupported contexts must not choose
# a definite identity, even if that chosen table happens to exist.
@pytest.mark.parametrize('api', ['dbutils', 'aidputils'])
@pytest.mark.parametrize('context', [
    '{call}',
    'if {call} is None:\n    pass',
    'for item in ({call},):\n    pass',
    'False and {call}',
    'external() and {call}',
    '{call} if external() else None',
])
def test_expression_widget_mutation_report_contract(monkeypatch, api, context):
    report, entry = _workflow_report(monkeypatch, [
        f'{api}.widgets.text("input", "demo.old")',
        context.format(call=f'{api}.widgets.removeAll()'),
        f'{api}.widgets.text("input", "demo.new")\n'
        f'spark.table({api}.widgets.get("input"))',
    ])
    if context == '{call}':
        assert [item['table'] for item in entry['read_tables']] == ['demo.new']
        assert report['summary']['tables_missing'] == 1
    else:
        assert not entry['read_tables']
        assert entry['unresolved_scopes']
        assert report['summary']['unresolved'] > 0


@pytest.mark.parametrize('api', ['dbutils', 'aidputils'])
@pytest.mark.parametrize('operation', [
    'removeAll()', 'remove("input")',
    'text("input", "demo.conditional")',
    'dropdown("input", "demo.conditional", ["demo.conditional"])',
    'combobox("input", "demo.conditional", ["demo.conditional"])',
    'multiselect("input", "demo.conditional", ["demo.conditional"])',
])
@pytest.mark.parametrize('context', [
    'result = external() and {call}',
    'if {call} is None:\n    pass',
    'for item in ({call},):\n    pass',
    'while {call}:\n    break',
    'with external({call}):\n    pass',
    'assert {call} is None',
    'counter += ({call}, 1)[1]',
    'del values[({call}, 0)[1]]',
    'def deferred(value={call}):\n    pass',
    'class Example(({call}, Base)[1]):\n    pass',
    'match external():\n    case _:\n        {call}',
    'def deferred():\n    return {call}',
    'values = [{call} for item in external()]',
    'values = ({call}, {call})',
])
def test_unsupported_widget_mutation_boundary_matrix(monkeypatch, api, operation, context):
    report, entry = _workflow_report(monkeypatch, [
        f'{api}.widgets.text("input", "demo.old")',
        context.format(call=f'{api}.widgets.{operation}'),
        f'{api}.widgets.text("input", "demo.new")\n'
        f'spark.table({api}.widgets.get("input"))',
    ])
    assert not entry['read_tables']
    assert entry['unresolved_scopes']
    assert report['summary']['tables_found'] == 0
    assert report['summary']['unresolved'] > 0


@pytest.mark.parametrize('api', ['dbutils', 'aidputils'])
@pytest.mark.parametrize('context', ['{call}', 'result = {call}'])
def test_direct_widget_statement_still_establishes_state(monkeypatch, api, context):
    report, entry = _workflow_report(monkeypatch, [
        context.format(call=f'{api}.widgets.text("input", "demo.old")'),
        context.format(call=f'{api}.widgets.removeAll()'),
        context.format(call=f'{api}.widgets.text("input", "demo.new")'),
        f'spark.table({api}.widgets.get("input"))',
    ])
    assert [item['table'] for item in entry['read_tables']] == ['demo.new']
    assert report['summary']['tables_missing'] == 1
    assert report['summary']['unresolved'] == 0
