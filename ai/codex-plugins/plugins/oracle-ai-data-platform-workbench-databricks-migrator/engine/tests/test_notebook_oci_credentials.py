"""Incident: generated SDK code chose a default identity on standalone replay."""
import ast
import asyncio
import copy
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from notebook_oci_credentials import (
    OCI_ENV_KEYS, credential_env_snippet, ensure_artifact_oci_credentials,
    rewrite_oci_credential_reads,
)

INCIDENT = '''import os
_CRED = os.environ.get('AIDP_CRED_OCI_NAME', 'wrong_example_identity')
_user = aidputils.secrets.get(_CRED, key='user').strip()
_key_content = aidputils.secrets.get(_CRED, key='key_file_content')
'''


def cell(source):
    return {"cell_type": "code", "source": [source], "metadata": {}}


@pytest.fixture
def configured(monkeypatch):
    values = {key: "example_" + key.lower() for key in OCI_ENV_KEYS}
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


def test_incident_is_rewritten_and_original_preserved(configured, monkeypatch):
    code = rewrite_oci_credential_reads(INCIDENT)
    calls = []
    def get(name, key):
        calls.append((name, key))
        return "test-value"
    exec(code, {"aidputils": SimpleNamespace(secrets=SimpleNamespace(get=get))})
    assert calls == [(configured[OCI_ENV_KEYS[0]], configured['AIDP_CRED_OCI_KEY_USER']),
                     (configured[OCI_ENV_KEYS[0]], configured['AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT'])]
    assert "# Original: _CRED =" in code
    assert rewrite_oci_credential_reads(code) == code
    monkeypatch.delenv('AIDP_CRED_OCI_NAME')
    with pytest.raises(KeyError, match='AIDP_CRED_OCI_NAME'):
        exec(code, {})


@pytest.mark.parametrize('source', [
    "# " + INCIDENT.replace('\n', '\n# '),
    'text = ' + repr(INCIDENT),
    "items = []; items.append(1)",
    "aidputils.secrets.get('other-service', key='user')",
    "%scala\nval key = \"AIDP_CRED_OCI_NAME\"",
])
def test_unrelated_code_unchanged(source):
    assert rewrite_oci_credential_reads(source) == source


@pytest.mark.parametrize('form', [
    "os.getenv('AIDP_CRED_OCI_NAME', 'old')",
    "os.environ.get(\n    'AIDP_CRED_OCI_NAME',\n    'old'\n)",
    "os.environ['AIDP_CRED_OCI_NAME']",
])
def test_equivalent_credential_reads(form):
    src = INCIDENT.replace("os.environ.get('AIDP_CRED_OCI_NAME', 'wrong_example_identity')", form)
    converted = rewrite_oci_credential_reads(src)
    ast.parse(converted)
    assert "key=os.environ['AIDP_CRED_OCI_KEY_USER']" in converted


def test_unrelated_same_named_variable_in_other_function_unchanged():
    other = "def other():\n    _CRED = 'other-service'\n    return aidputils.secrets.get(_CRED, key='user')\n"
    assert other in rewrite_oci_credential_reads(INCIDENT + other)


def test_unknown_key_fails_loud():
    with pytest.raises(ValueError, match='Unmapped OCI credential field'):
        rewrite_oci_credential_reads(INCIDENT.replace("key='user'", "key='unexpected'"))


def test_standalone_bootstrap_overrides_stale_cluster_environment(configured, monkeypatch):
    cells = ensure_artifact_oci_credentials([cell(rewrite_oci_credential_reads(INCIDENT))])
    assert ensure_artifact_oci_credentials(cells) == cells
    for key in OCI_ENV_KEYS:
        monkeypatch.setenv(key, 'stale-cluster-reference')
    exec(''.join(cells[0]['source']), {})
    assert {key: os.environ[key] for key in OCI_ENV_KEYS} == configured


def test_no_config_rejects_auth_artifact_but_not_unrelated_notebook(monkeypatch):
    for key in OCI_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match='aidp.oci_credentials'):
        ensure_artifact_oci_credentials([cell(INCIDENT)])
    cells = [cell('print(1)')]
    assert ensure_artifact_oci_credentials(cells) == cells


def test_kernel_bootstrap_clears_stale_config_when_yaml_block_absent(monkeypatch):
    for key in OCI_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    code = credential_env_snippet()
    for key in OCI_ENV_KEYS:
        monkeypatch.setenv(key, 'stale')
    exec(code, {})
    assert all(key not in os.environ for key in OCI_ENV_KEYS)


def test_explicit_python_magic_in_scala_notebook(configured):
    converted = rewrite_oci_credential_reads('%python\n' + INCIDENT)
    assert converted.startswith('%python\n')
    cells = ensure_artifact_oci_credentials([cell(converted)], 'scala')
    assert cells[0]['source'][0].startswith('%python\n')


@pytest.mark.parametrize('task_key', ['01_task', 'dep_helper'])
def test_real_cell_loop_persists_yaml_refs_for_parent_and_dependency(monkeypatch, tmp_path, task_key):
    from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources
    import job_migrate as jm
    from aidp_config import load_config
    import yaml
    fixture = Path(__file__).parent / 'config/fixtures/valid_oci_credentials_block.yaml'
    document = yaml.safe_load(fixture.read_text(encoding='utf-8'))
    document['bucket_mapping']['path'] = str(tmp_path / 'mapping.csv')
    (tmp_path / 'mapping.csv').write_text('s3_bucket,oci_bucket,oci_namespace\n')
    document['aidp']['oci_credentials']['name'] = 'example_yaml_identity'
    document['aidp']['oci_credentials']['keys']['user'] = 'example_user_key'
    config_path = tmp_path / 'customer.yaml'
    config_path.write_text(yaml.safe_dump(document))
    for key in OCI_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    jm.apply_cfg_to_modules(load_config(str(config_path)))
    notebook = {'cells': [cell(INCIDENT)], 'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/credentials.ipynb', kernel, 'example_job', task_key, {}))
    assert result['status'] == 'PASS', result
    _, artifact = _saved_sources(kernel)
    credential_cells = [c for c in artifact['cells'] if c.get('metadata', {}).get('aidp_yaml_oci_credentials')]
    assert len(credential_cells) == 1
    monkeypatch.setenv('AIDP_CRED_OCI_NAME', 'wrong_standalone_identity')
    exec(''.join(credential_cells[0]['source']), {})
    assert os.environ['AIDP_CRED_OCI_NAME'] == 'example_yaml_identity'
    assert os.environ['AIDP_CRED_OCI_KEY_USER'] == 'example_user_key'


def test_fix_loop_cannot_reintroduce_model_fallback(monkeypatch):
    import agent_migrate as am
    async def model(*args, **kwargs):
        return INCIDENT, {}
    monkeypatch.setattr(am, 'call_opus_with_tools', model)
    code = asyncio.run(am.call_fix('print(1)', 'BucketNotFound', [], 1, session=object()))
    assert 'wrong_example_identity' not in ast.unparse(ast.parse(code))
    assert "AIDP_CRED_OCI_KEY_USER" in code


def test_inline_job_helper_uses_yaml_keys(configured, monkeypatch):
    import job_migrate as jm
    monkeypatch.setattr(jm, 'AIDP_BASE', 'https://aidp.example.invalid')
    monkeypatch.setattr(jm, 'DATALAKE_OCID', 'example-lake')
    monkeypatch.setattr(jm, 'WORKSPACE_ID', 'example-workspace')
    helper = jm._build_aidp_invoke_helper()
    ast.parse(helper)
    assert "os.environ['AIDP_CRED_OCI_NAME']" in helper
    assert "os.environ['AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT']" in helper
    cells = ensure_artifact_oci_credentials([cell(helper)])
    assert cells[0]['metadata']['aidp_yaml_oci_credentials']
