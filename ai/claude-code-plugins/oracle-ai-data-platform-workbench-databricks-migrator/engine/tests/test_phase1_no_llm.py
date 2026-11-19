"""Scoped registry/file discovery is static; unrelated analysis owners are unchanged.

Pipeline tests exercise the current default flow with model creation forbidden.
Only workspace downloads and metadata I/O boundaries are replaced.
"""
import asyncio
import json
from pathlib import Path

import pytest
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate
import check_data_availability_for_workflow as checker
from data_check_extract import extract_from_notebook
import contextlib
import io
from types import SimpleNamespace

ROOT = "/Workspace/export/notebooks/root.ipynb"


def notebook(*sources):
    return {"metadata": {"language_info": {"name": "python"}},
            "cells": [{"cell_type": "code", "source": source} for source in sources]}


def setup_flow(monkeypatch, tmp_path):
    notebooks = {ROOT: notebook()}
    manifest = {"jobs": [{"job_name": "test_job", "databricks_job_id": "12345",
                          "tasks": [{"task_key": "root", "notebook_path": ROOT,
                                     "cluster_id": "job-cluster", "run_deps": []}]}]}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    output = tmp_path / "report.json"
    cfg = SimpleNamespace(
        aidp=SimpleNamespace(region="ap-mumbai-1", lake_ocid="lake", workspace_id="workspace",
                             oci_profile="DEFAULT"),
        bucket_mapping=SimpleNamespace(path="mapping.csv"),
        llm=SimpleNamespace(provider="anthropic", model="unused-model"))
    monkeypatch.setattr("aidp_config.load_config", lambda **_: cfg)
    monkeypatch.setattr(checker, "get_oci_signer", lambda *_: ({}, object()))
    monkeypatch.setattr(checker, "load_bucket_ns_mapping", lambda *_: None)
    monkeypatch.setattr(checker, "_download_notebook_json", lambda *args: notebooks[args[-1]])
    monkeypatch.setattr(checker, "_list_workspace_entries", lambda *_: [])
    sessions, requests = [], []

    class Spark:
        def __init__(self):
            self.catalog = self

        def tableExists(self, table):
            return False

        def sql(self, sql):
            raise RuntimeError("table not found")

    class Session:
        def __init__(self, **kwargs):
            self.cluster_id = kwargs["cluster_id"]
            self.closed = False
            sessions.append(self)

        async def connect(self):
            pass

        async def close(self):
            self.closed = True

        async def execute(self, code, timeout):
            # Only tool-owned metadata probes execute, never customer source.
            text = io.StringIO()
            with contextlib.redirect_stdout(text):
                exec(code, {"spark": Spark()})
            return {"status": "ok", "outputs": [{"type": "stream", "text": text.getvalue()}]}

    monkeypatch.setattr(checker, "AIDPSession", Session)
    monkeypatch.setattr(sys, "argv", [
        "check_data_availability_for_workflow.py", "--config", "test-config.yaml",
        "--manifest", str(manifest_path), "--cluster", "control-cluster",
        "--output", str(output)])
    return SimpleNamespace(output=output, requests=requests, sessions=sessions,
                           notebooks=notebooks, manifest=manifest, manifest_path=manifest_path)


def run_report(flow):
    asyncio.run(checker.main())
    return json.loads(flow.output.read_text())

def default_flow(monkeypatch, tmp_path, sources):
    flow = setup_flow(monkeypatch, tmp_path)
    flow.notebooks.clear()
    flow.notebooks[ROOT] = notebook(*sources)
    flow.manifest['jobs'][0]['tasks'][0]['run_deps'] = []
    flow.manifest_path.write_text(json.dumps(flow.manifest))
    def forbidden(*args, **kwargs):
        pytest.fail('Phase 1 attempted LLM use')
    monkeypatch.setattr(agent_migrate, "claude", forbidden)
    monkeypatch.delenv('ANTHROPIC_API_KEY', raising=False)
    import aidp_config
    cfg = aidp_config.load_config()
    del cfg.llm
    monkeypatch.setattr(aidp_config, 'load_config', lambda **_: cfg)
    return flow


@pytest.mark.parametrize('configured', [False, True])
def test_default_main_reports_both_backscoring_dependencies_without_llm(monkeypatch, tmp_path, capsys, configured):
    sources = [
        'client = mlflow.tracking.MlflowClient()\nclient.get_registered_model("FAMILY_A")',
        'def load_targets():\n    files = glob.glob(os.path.join(targets_dir, "*_targets_df.csv"))\n    return pd.read_csv(files[-1])',
        'spark.read.format("org.apache.spark.sql.cassandra").options(keyspace=ks, table=table).load()',
    ]
    flow = default_flow(monkeypatch, tmp_path, sources)
    if configured:
        import aidp_config
        from types import SimpleNamespace
        aidp_config.load_config().llm = SimpleNamespace(provider='anthropic', model='unused-model')
        monkeypatch.setenv('ANTHROPIC_API_KEY', 'unused-test-value')
    report = run_report(flow)
    assert report == json.loads(flow.output.read_text())
    registry = [v for v in report['notebooks'][ROOT]['not_analyzed'] if v.get('kind') == 'mlflow_registry']
    assert registry[0]['candidate_name'] == 'FAMILY_A'
    gaps = report['notebooks'][ROOT]['unresolved_scopes']
    assert {'glob.glob', 'pd.read_csv', 'load'} <= {v.get('api') for v in gaps}
    assert any('*_targets_df.csv' in v.get('argument_expression', '') for v in gaps)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    unresolved = report['data_migration']['unresolved']
    assert any(v.get('candidate_name') == 'FAMILY_A' for v in unresolved)
    assert any(v.get('api') == 'pd.read_csv' for v in unresolved)
    assert not report['data_migration']['requirements']  # No invented names/copy rows.
    assert 'FAMILY_A' in capsys.readouterr().out
    assert flow.requests == []
    assert all(session.closed for session in flow.sessions)


def test_known_static_read_keeps_probe_behavior_without_model_config(monkeypatch, tmp_path):
    flow = default_flow(monkeypatch, tmp_path, ['spark.read.table("input_db.customers")'])
    report = run_report(flow)
    assert report['summary']['tables_missing'] == 1
    assert report['data_migration']['requirements'][0]['object_name'] == 'input_db.customers'
    assert flow.requests == []


def test_large_source_no_longer_has_model_request_limit(monkeypatch, tmp_path):
    flow = default_flow(monkeypatch, tmp_path, ['#' + 'x' * 280_000,
                                              'client = mlflow.tracking.MlflowClient()\nclient.get_registered_model("FAMILY_A")'])
    report = run_report(flow)
    assert report['notebooks'][ROOT]['not_analyzed'][0]['candidate_name'] == 'FAMILY_A'
    assert flow.requests == []


def test_static_extraction_cannot_call_unrelated_analysis(monkeypatch):
    def forbidden(*args):
        pytest.fail('Completion callback must not run')
    monkeypatch.setattr(agent_migrate, 'claude', forbidden)
    result = extract_from_notebook(notebook('client.get_registered_model("FAMILY_A")'))
    assert result['not_analyzed'][0]['kind'] == 'mlflow_registry'


@pytest.mark.parametrize('source,api,known', [
    ('def load():\n    return pd.read_csv("/dbfs/FileStore/targets.csv")', 'pd.read_csv', True),
    ('def load(path):\n    return pd.read_csv(path)', 'pd.read_csv', False),
    ('files = glob.glob(dynamic)', 'glob.glob', False),
    ('files = glob.iglob(dynamic)', 'glob.iglob', False),
])
def test_returned_reads_and_selectors_are_not_omitted(source, api, known):
    data = extract_from_notebook(notebook(source))
    findings = data['read_references'] if known else data['unresolved_scopes']
    assert any(v.get('api') == api and v['cell_number'] == 1 for v in findings)


def test_unknown_registry_receiver_does_not_certify_a_named_model():
    data = extract_from_notebook(notebook('def helper(client, name):\n    return client.get_registered_model(name)'))
    registry = [v for v in data['not_analyzed'] if v['kind'] == 'mlflow_registry']
    assert registry and 'object_name' not in registry[0]


def test_comments_strings_bare_returns_and_known_inactive_reads_are_not_registry_calls():
    source = '''# client.get_registered_model("COMMENT")
text = 'client.get_registered_model("STRING")'
def no_read():
    return
if False:
    client.get_registered_model("INACTIVE")
'''
    assert not extract_from_notebook(notebook(source))['not_analyzed']


def test_registry_identity_uses_existing_token_scrubbing(monkeypatch, tmp_path):
    token = 'xox' + 'b-' + '1234567890-' + 'a' * 30
    flow = default_flow(monkeypatch, tmp_path, [
        f'client = mlflow.tracking.MlflowClient()\nclient.get_registered_model({token!r})'])
    report = run_report(flow)
    assert token not in json.dumps(report)
    assert token not in flow.output.read_text()


@pytest.mark.parametrize('source', [
    'if client.get_registered_model("FAMILY_A"):\n    pass',
    'while client.get_model_version("FAMILY_A", "1"):\n    break',
    'for model in client.search_registered_models():\n    pass',
    'with client.get_registered_model("FAMILY_A"):\n    pass',
])
def test_registry_reads_in_control_expressions_stay_visible(source):
    gaps = extract_from_notebook(notebook(source))['not_analyzed']
    assert any(v.get('kind') == 'mlflow_registry' for v in gaps)


def test_source_parse_failure_remains_unresolved_without_model_fallback(monkeypatch, tmp_path):
    flow = default_flow(monkeypatch, tmp_path, ['def broken('])
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    assert report['notebooks'][ROOT]['unresolved']
    assert flow.requests == []


def test_registry_output_api_is_not_a_registry_read():
    data = extract_from_notebook(notebook('client.create_registered_model("NEW_FAMILY")'))
    assert not data['not_analyzed']


def test_unreadable_source_cannot_certify_an_empty_review(monkeypatch, tmp_path):
    flow = default_flow(monkeypatch, tmp_path, [])
    flow.notebooks[ROOT] = None
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    assert report['notebooks'][ROOT]['unresolved']


@pytest.mark.parametrize('options', ['root_dir="/dbfs/other"', 'dir_fd=fd', '**options'])
def test_glob_options_cannot_guess_a_different_root(options):
    data = extract_from_notebook(notebook(f'files = glob.glob("*.csv", {options})'))
    assert any(v.get('api') == 'glob.glob' and 'manual review' in v['reason']
               for v in data['unresolved_scopes'])
    assert not data['read_references']


@pytest.mark.parametrize('source,api,statement_type', [
    ('assert client.get_registered_model("FAMILY_A")', 'get_registered_model', 'Assert'),
    ('match client.get_registered_model("FAMILY_A"):\n    case _: pass', 'get_registered_model', 'Match'),
    ('models = []\nmodels += client.search_registered_models()', 'search_registered_models', 'AugAssign'),
    ('assert not pd.read_csv("/dbfs/FileStore/targets.csv").empty', 'pd.read_csv', 'Assert'),
    ('raise RuntimeError(client.get_model_version("FAMILY_A", "1"))', 'get_model_version', 'Raise'),
    ('del cache[client.get_model_version("FAMILY_A", "1")]', 'get_model_version', 'Delete'),
    ('match flag:\n    case True:\n        files = glob.glob(pattern)', 'glob.glob', 'Match'),
])
def test_unsupported_read_statements_reach_report_and_plan(
        monkeypatch, tmp_path, source, api, statement_type):
    flow = default_flow(monkeypatch, tmp_path, [source])
    report = run_report(flow)
    assert report['data_migration']['requirements'] or report['data_migration']['unresolved']
    findings = [v for v in report['notebooks'][ROOT]['not_analyzed'] if v.get('kind') == 'unsupported_read']
    finding, = findings
    assert finding['api'] == api
    assert finding['statement_type'] == statement_type
    assert finding['cell_number'] == 1
    assert finding['line_number'] > 0
    assert 'candidate_name' not in finding and 'object_name' not in finding
    assert any(v.get('api') == api and v.get('category') == 'DYNAMIC_REFERENCE'
               for v in report['data_migration']['unresolved'])
    assert not report['data_migration']['requirements']
    assert flow.requests == []


def test_unsupported_scope_does_not_reuse_outer_identity():
    source = '''name = "OUTER_MODEL"
match value:
    case {"model": name}:
        client.get_registered_model(name)
'''
    gaps = extract_from_notebook(notebook(source))['not_analyzed']
    finding, = [v for v in gaps if v.get('kind') == 'unsupported_read']
    assert finding['api'] == 'get_registered_model'
    assert 'candidate_name' not in finding and 'object_name' not in finding


@pytest.mark.parametrize('source', [
    '# assert client.get_registered_model("COMMENT")\ntext = "pd.read_csv(path)"',
    'if False:\n    assert client.get_registered_model("INACTIVE")',
    'assert client.create_registered_model("OUTPUT")',
    'assert open("/dbfs/FileStore/output.csv", "w")',
    'assert open("/dbfs/FileStore/output.csv", mode="a")',
    'count = 1\ncount += 1\nassert count == 2',
])
def test_unsupported_statement_backstop_does_not_invent_reads(source):
    assert not extract_from_notebook(notebook(source))['not_analyzed']


def test_supported_read_is_not_duplicated_by_statement_backstop():
    data = extract_from_notebook(notebook('client.get_registered_model("FAMILY_A")'))
    finding, = data['not_analyzed']
    assert finding['kind'] == 'mlflow_registry'
