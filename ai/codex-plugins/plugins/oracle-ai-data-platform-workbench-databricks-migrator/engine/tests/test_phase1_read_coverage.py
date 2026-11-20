"""Pin the complete registry/file read-position review matrix (PR #339).

Customer expressions are source data. Only downloads and metadata I/O
boundaries are stubbed; default static extraction/report/plan are real.
"""
import ast
import asyncio
import json
import pytest
from test_phase1_no_llm import default_flow, run_report, checker, ROOT
from data_check_extract import ModuleNamespace


def test_module_read_metadata_does_not_change_symbol_equality():
    plain = ModuleNamespace(values={'path': '/dbfs/FileStore/targets.csv'}, names=frozenset({'path'}))
    with_gap = ModuleNamespace(values=plain.values, names=plain.names,
                               input_gaps=({'kind': 'module_input', 'api': 'pd.read_csv'},))
    assert plain == with_gap
    assert plain.star_names() == with_gap.star_names()

WRAPPERS = {'assign': 'value = READ',
 'annassign_value': 'value: object = READ',
 'expr': 'READ',
 'if_test': 'if READ:\n    pass',
 'while_test': 'while READ:\n    break',
 'for_iter': 'for value in READ:\n    pass',
 'for_target': 'for cache[READ] in [1]:\n    pass',
 'with_context': 'with READ as value:\n    pass',
 'with_target': 'with context as cache[READ]:\n    pass',
 'assert': 'assert READ',
 'augassign': 'values += READ',
 'delete': 'del cache[READ]',
 'raise': 'raise RuntimeError(READ)',
 'match_subject': 'match READ:\n    case _: pass',
 'match_guard': 'match flag:\n    case _ if READ: pass',
 'match_body': 'match flag:\n    case _: value = READ',
 'try_body': 'try:\n    value = READ\nexcept Exception:\n    pass',
 'except_type': 'try:\n    trigger()\nexcept error_type(READ):\n    pass',
 'except_star_type': 'try:\n    trigger()\nexcept* error_type(READ):\n    pass',
 'except_body': 'try:\n    trigger()\nexcept Exception:\n    value = READ',
 'finally': 'try:\n    pass\nfinally:\n    value = READ',
 'function_body': 'def load():\n    value = READ\nload()',
 'return': 'def load():\n    return READ\nload()',
 'function_default': 'def load(value=READ):\n    pass',
 'decorator': '@decorate(READ)\ndef load():\n    pass',
 'class_base': 'class Sample(base(READ)):\n    pass',
 'lambda_body': 'load = lambda: READ\nload()',
 'lambda_default': 'load = lambda value=READ: value',
 'list_comprehension': 'values = [READ for x in [1]]',
 'generator': 'values = list(READ for x in [1])',
 'fstring': "text = f'{READ}'",
 'call_arg': 'consume(READ)',
 'assign_target': 'cache[READ] = 1',
 'annotation': 'value: value_type(READ) = 1',
 'namedexpr_test': 'if (value := READ):\n    pass',
 'conditional': 'value = READ if flag else None',
 'async_with': 'async with READ:\n    pass',
 'async_for': 'async for value in READ:\n    pass'}
READS = {'registry': 'client.get_registered_model("FAMILY_A")',
 'csv': 'pd.read_csv("/dbfs/FileStore/targets.csv")',
 'glob': 'glob.glob("/dbfs/FileStore/targets/*_targets_df.csv")'}


@pytest.mark.parametrize('kind,read', list(READS.items()), ids=list(READS))
@pytest.mark.parametrize('position,template', list(WRAPPERS.items()), ids=list(WRAPPERS))
def test_every_recognized_read_reaches_evidence(monkeypatch, tmp_path, kind, read, position, template):
    source = template.replace('READ', read)
    ast.parse(source)  # A syntax-error finding must not satisfy this coverage test.
    flow = default_flow(monkeypatch, tmp_path, [source])
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    data = report['notebooks'][ROOT]
    assert data['read_paths'] or data['not_analyzed'] or data['unresolved_scopes']
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    assert flow.requests == []


@pytest.mark.parametrize('source', [
    'df = pd.read_csv(filepath_or_buffer="/dbfs/FileStore/targets.csv")',
    'paths = glob.glob(pathname="/dbfs/FileStore/targets/*.csv")',
    'stream = open(file="/dbfs/FileStore/targets.csv")',
    'model = client.get_registered_model(name="FAMILY_A")',
])
def test_keyword_reads_never_certify_an_empty_inventory(monkeypatch, tmp_path, source):
    flow = default_flow(monkeypatch, tmp_path, [source])
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    assert report['data_migration']['unresolved']
    assert flow.requests == []


@pytest.mark.parametrize('source', [
    '# client.get_registered_model("COMMENT")\ntext="pd.read_csv(path)"',
    'if False:\n    assert client.get_registered_model("INACTIVE")',
    'if True:\n    pass\nelse:\n    pd.read_csv(path)',
    'client.create_registered_model("OUTPUT")',
    'assert open("/dbfs/FileStore/output.csv", "w")',
    'df = pd.read_csv(io.StringIO("id\\n1"))',
    'assert not pd.read_csv(io.StringIO("id\\n1")).empty',
    'def identity(value):\n    return value\nx = identity(1)',
])
def test_non_inputs_and_pure_helpers_keep_empty_inventory(monkeypatch, tmp_path, source):
    flow = default_flow(monkeypatch, tmp_path, [source])
    report = run_report(flow)
    assert not report['notebooks'].get(ROOT, {}).get('unresolved')
    assert not report['data_migration']['requirements']
    assert not report['data_migration']['unresolved']


@pytest.mark.parametrize('api', ['glob.glob', 'glob.iglob'])
def test_known_glob_keeps_pattern_through_plan(monkeypatch, tmp_path, api):
    pattern = '/dbfs/FileStore/targets/*_targets_df.csv'
    flow = default_flow(monkeypatch, tmp_path, [f'files = {api}({pattern!r})'])
    report = run_report(flow)
    entry, = report['data_migration']['requirements']
    assert entry['object_type'] == 'DIRECTORY'
    assert entry['source_pattern'] == pattern
    assert entry['source_path'] == '/dbfs/FileStore/targets/'
    assert '*' not in entry['object_name']
    assert report['notebooks'][ROOT]['read_paths'][0]['source'] == pattern


@pytest.mark.parametrize('read,api', [
    ('client.get_registered_model("FAMILY_A")', 'get_registered_model'),
    ('pd.read_csv("/dbfs/FileStore/targets.csv")', 'pd.read_csv'),
    ('glob.glob("/dbfs/FileStore/targets/*.csv")', 'glob.glob'),
])
@pytest.mark.parametrize('in_helper', [False, True])
def test_imported_module_findings_survive_cache_and_plan(monkeypatch, tmp_path, read, api, in_helper):
    notebook_paths = ['/Workspace/export/notebooks/GIT/root.ipynb',
                      '/Workspace/export/notebooks/GIT/second.ipynb']
    flow = default_flow(monkeypatch, tmp_path, ['from helpers import load\nload()'])
    original = flow.notebooks.pop(ROOT)
    for path in notebook_paths:
        flow.notebooks[path] = original
    task = flow.manifest['jobs'][0]['tasks'][0]
    task['notebook_path'] = notebook_paths[0]
    flow.manifest['jobs'][0]['tasks'].append({**task, 'task_key': 'second', 'notebook_path': notebook_paths[1]})
    flow.manifest_path.write_text(json.dumps(flow.manifest))
    source = ('def load():\n    return ' + read if in_helper
              else 'loaded = ' + read + '\ndef load():\n    return loaded')
    monkeypatch.setattr(checker, '_list_workspace_entries', lambda *args:
        (frozenset({'helpers.py'}), frozenset()) if args[-1] == '/Workspace/export/notebooks/GIT'
        else (frozenset(), frozenset()))
    monkeypatch.setattr(checker, '_download_workspace_file', lambda *args: source.encode())
    async def libraries_present(*args, **kwargs):
        return {'python': {}, 'scala': {}}
    monkeypatch.setattr(checker, 'check_libraries', libraries_present)
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    for path in notebook_paths:
        gap, = report['notebooks'][path]['not_analyzed']
        assert gap['kind'] == 'module_input' and gap['api'] == api
        assert gap['path'].endswith('/helpers.py') and gap['module_line_number'] > 0
        assert any(entry['notebook_path'] == path and entry.get('api') == api
                   for entry in report['data_migration']['unresolved'])
    assert not report['data_migration']['requirements']
    assert flow.requests == []


@pytest.mark.parametrize('api', ['glob.glob', 'glob.iglob'])
@pytest.mark.parametrize('pattern', [
    '/dbfs/FileStore/targets/{first,second}.csv',
    '/dbfs/FileStore/targets/{literal}.csv',
    '/dbfs/FileStore/targets/{first,{second,third}}.csv',
])
def test_python_brace_patterns_cannot_use_hadoop_expansion(monkeypatch, tmp_path, api, pattern):
    flow = default_flow(monkeypatch, tmp_path, [f'files = {api}({pattern!r})'])
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    assert not report['data_migration']['requirements']
    assert any('brace' in entry.get('reason', '') for entry in report['data_migration']['unresolved'])


def _glob_report(source, probe):
    from data_check_extract import extract_from_notebook
    from test_phase1_no_llm import notebook
    refs = {ROOT: extract_from_notebook(notebook(source))}
    asyncio.run(checker.classify_refs_on_cluster(None, [ROOT], refs))
    paths = {entry['path']: dict(probe) for entry in refs[ROOT]['paths']}
    report = checker.generate_report(refs, {}, paths, paths, {}, 'glob review')
    checker.attach_data_migration_requirements(report, '12345')
    return refs, report


@pytest.mark.parametrize('api', ['glob.glob', 'glob.iglob'])
@pytest.mark.parametrize('contents', [[], ['other.csv'], ['first_targets_df.csv']])
def test_directory_probe_cannot_certify_python_glob_matches(tmp_path, capsys, api, contents):
    import contextlib
    import glob
    import io
    from types import SimpleNamespace
    directory = tmp_path / 'targets'
    directory.mkdir()
    for name in contents:
        (directory / name).write_text('id\n1\n')
    assert len(glob.glob(str(directory / '*_targets_df.csv'))) == int('first_targets_df.csv' in contents)
    pattern = 'oci://test-bucket@test-namespace/targets/*_targets_df.csv'
    probe_path = checker._concrete_glob_base_path(pattern)

    # Execute the unchanged tool-owned probe, using local files for Hadoop I/O.
    # It only sees the directory; even its positive result cannot prove matches.
    class FileSystem:
        def exists(self, path):
            assert path == probe_path and '*' not in path
            return directory.exists()

        def listStatus(self, path):
            assert path == probe_path
            return list(directory.iterdir())

    filesystem = FileSystem()
    jvm = SimpleNamespace(
        java=SimpleNamespace(net=SimpleNamespace(URI=str)),
        org=SimpleNamespace(apache=SimpleNamespace(hadoop=SimpleNamespace(
            fs=SimpleNamespace(Path=str, FileSystem=SimpleNamespace(get=lambda *_: filesystem))))))
    spark = SimpleNamespace(_jvm=jvm, sparkContext=SimpleNamespace(
        _jsc=SimpleNamespace(hadoopConfiguration=lambda: None)))
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        exec(checker._oci_path_probe_code(json.dumps([{'path': pattern, 'probe_path': probe_path}])),
             {'spark': spark})
    probe = json.loads(stream.getvalue())[pattern]
    assert probe['exists'] is True
    _, report = _glob_report(f'files = {api}({pattern!r})', probe)
    path, = report['notebooks'][ROOT]['read_paths']
    assert path['exists'] == 'unverified' and path['probe_status'] == 'UNVERIFIED'
    assert report['summary']['paths_found'] == 0
    assert report['summary']['paths_unverified'] == 1
    requirement, = report['data_migration']['requirements']
    assert requirement['currently_exists'] == 'unverified'
    assert requirement['source_pattern'] == pattern
    gap, = report['data_migration']['unresolved']
    assert gap['kind'] == 'UNVERIFIED_GLOB_MATCH' and gap['api'] == api
    assert gap['cell_number'] == 1 and gap['line_number'] == 1
    checker.print_report(report)
    assert 'ALL DEPENDENCIES AVAILABLE' not in capsys.readouterr().out


@pytest.mark.parametrize('api', ['glob.glob', 'glob.iglob'])
@pytest.mark.parametrize('prefix', ['/Volumes/default/default/data/', '/Workspace/data/',
                                    'oci://test-bucket@test-namespace/', 's3://test-bucket/', '/mnt/source/'])
def test_glob_manual_review_persists_through_cli_json_and_plan(monkeypatch, tmp_path, api, prefix):
    pattern = prefix + '*_targets_df.csv'
    flow = default_flow(monkeypatch, tmp_path, [f'files = {api}({pattern!r})'])
    monkeypatch.setattr(checker, 'map_to_oci_path', lambda path: {
        'source': path, 'oci_path': 'oci://test-bucket@test-namespace/targets/*_targets_df.csv'})
    async def directory_probe(session, paths, **kwargs):
        return {path: {'exists': True, 'items': 1} for path in paths}
    monkeypatch.setattr(checker, 'check_oci_paths', directory_probe)
    monkeypatch.setattr(checker, 'check_local_paths', directory_probe)
    report = run_report(flow)
    assert report == json.loads(flow.output.read_text())
    assert report['summary']['paths_found'] == 0
    assert report['summary']['paths_unverified'] == 1
    requirement, = report['data_migration']['requirements']
    assert requirement['currently_exists'] == 'unverified'
    assert requirement['source_pattern'] == pattern
    gap, = report['data_migration']['unresolved']
    assert gap['kind'] == 'UNVERIFIED_GLOB_MATCH'
    assert gap['object_name'] == pattern and gap['api'] == api


@pytest.mark.parametrize('first', [True, False])
@pytest.mark.parametrize('reader', ['spark.read.csv', 'pd.read_csv'])
@pytest.mark.parametrize('api', ['glob.glob', 'glob.iglob'])
def test_other_reader_of_same_path_cannot_discard_glob_evidence(api, reader, first):
    pattern = 'oci://test-bucket@test-namespace/targets/*.csv'
    sources = [f'files = {api}({pattern!r})', f'data = {reader}({pattern!r})']
    if not first:
        sources.reverse()
    _, report = _glob_report('\n'.join(sources), {'exists': True})
    assert report['summary']['paths_found'] == 0
    assert report['data_migration']['requirements'][0]['currently_exists'] == 'unverified'
    gap, = report['data_migration']['unresolved']
    assert gap['api'] == api and gap['line_number'] == (1 if first else 2)


@pytest.mark.parametrize('exists', [True, False, 'unverified', 'access_denied', 'access_error', 'probe_error'])
def test_glob_keeps_access_diagnostics_and_never_claims_availability(exists):
    pattern = 'oci://test-bucket@test-namespace/targets/*.csv'
    probe = {'exists': exists, 'error': 'probe detail'}
    refs, report = _glob_report(f'glob.glob({pattern!r})', probe)
    path, = report['notebooks'][ROOT]['read_paths']
    expected = exists if exists in checker._PROBE_FAILURE_STATUSES else 'unverified'
    assert path['exists'] == expected
    assert path['error'] == 'probe detail'
    assert report['summary']['paths_found'] == 0
    if exists in checker._PROBE_FAILURE_STATUSES:
        assert path['probe_status'] == checker._PROBE_FAILURE_STATUSES[exists]
        assert report['summary']['paths_access_errors'] == 1
    # Repeated rendering is stable and must not mutate the source inventory.
    again = checker.generate_report(refs, {}, {pattern: probe}, {}, {}, 'glob review')
    checker.attach_data_migration_requirements(again, '12345')
    assert report['notebooks'] == again['notebooks']
    assert report['summary'] == again['summary']
    for field in ('requirements', 'unresolved', 'exclusions'):
        assert report['data_migration'][field] == again['data_migration'][field]
    assert not refs[ROOT]['unresolved_scopes']


@pytest.mark.parametrize('api', ['spark.read.csv', 'pd.read_csv', 'open'])
def test_non_glob_read_verdict_is_unchanged(api):
    _, report = _glob_report(f'{api}("oci://test-bucket@test-namespace/targets/*.csv")', {'exists': True})
    assert report['summary']['paths_found'] == 1
    assert not report['data_migration']['unresolved']


def test_unmapped_glob_keeps_mapping_requirement(monkeypatch):
    monkeypatch.setattr(checker, 'map_to_oci_path', lambda path: {'source': path, 'oci_path': None})
    _, report = _glob_report('glob.glob("s3://unmapped-bucket/targets/*.csv")', {'exists': True})
    path, = report['notebooks'][ROOT]['read_paths']
    assert path['exists'] == 'unmapped'
    assert report['summary']['paths_unmapped'] == 1
    assert report['data_migration']['requirements'][0]['status'] == 'MAPPING_REQUIRED'
    assert report['data_migration']['unresolved'][0]['kind'] == 'UNVERIFIED_GLOB_MATCH'


@pytest.mark.parametrize('glob_first', [True, False])
def test_shared_plan_row_keeps_glob_unverified_across_notebooks(glob_first):
    from data_check_extract import extract_from_notebook
    from test_phase1_no_llm import notebook
    pattern = 'oci://test-bucket@test-namespace/targets/*.csv'
    sources = [('glob', f'glob.glob({pattern!r})'), ('spark', f'spark.read.load({pattern!r})')]
    if not glob_first:
        sources.reverse()
    refs = {name: extract_from_notebook(notebook(source)) for name, source in sources}
    asyncio.run(checker.classify_refs_on_cluster(None, list(refs), refs))
    report = checker.generate_report(refs, {}, {pattern: {'exists': True}}, {}, {}, 'shared glob')
    checker.attach_data_migration_requirements(report, '12345')
    assert report['notebooks']['spark']['read_paths'][0]['exists'] is True
    assert report['notebooks']['glob']['read_paths'][0]['exists'] == 'unverified'
    requirement, = report['data_migration']['requirements']
    assert requirement['currently_exists'] == 'unverified'
    assert set(requirement['notebook_paths']) == {'glob', 'spark'}


def test_glob_manual_finding_survives_excel_export(tmp_path):
    from build_proactive_data_migration_plan import write_excel
    from openpyxl import load_workbook
    _, report = _glob_report('glob.glob("oci://test-bucket@test-namespace/targets/*.csv")', {'exists': True})
    output = tmp_path / 'glob-plan.xlsx'
    write_excel(output, report['data_migration'])
    workbook = load_workbook(output, read_only=True)
    rows = list(workbook['Unresolved References'].values)
    finding = dict(zip(rows[0], rows[1]))
    assert finding['kind'] == 'UNVERIFIED_GLOB_MATCH'
    assert finding['cell_number'] == 1 and 'Manual review required' in finding['reason']
    requirements = list(workbook['Proactive Requirements'].values)
    assert dict(zip(requirements[0], requirements[1]))['currently_exists'] == 'unverified'
    workbook.close()


def test_classified_glob_provenance_is_sufficient_without_raw_reads():
    pattern = 'oci://test-bucket@test-namespace/targets/*.csv'
    refs = {ROOT: {'paths': [{'path': pattern, 'usage': 'read', 'api': 'glob.glob',
                             'cell_number': 1, 'line_number': 1}]}}
    report = checker.generate_report(refs, {}, {pattern: {'exists': True}}, {}, {}, 'classified glob')
    assert report['notebooks'][ROOT]['read_paths'][0]['exists'] == 'unverified'
    assert report['notebooks'][ROOT]['unresolved_scopes'][0]['kind'] == 'UNVERIFIED_GLOB_MATCH'
