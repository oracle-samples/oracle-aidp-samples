"""PR #290 review: stopping execution must retain conversion and saved operations."""
import asyncio

import pytest

from test_attempt_loop_terminal_result import _SequenceKernel, _error
from test_conditional_exit_below_execute import _patch_common, _saved_sources
from test_eval_verdict_gate import _notebook
import job_migrate as jm


COLLISION = 'sc = spark.sparkContext\nanswer = sc.applicationId\n'
REPAIRED = '# sc = spark.sparkContext\n# Oracle tool modification: use supplied context\nanswer = sc.applicationId\n'
WRITE = 'df.write.parquet("s3://fixture-bucket/output")\n'


def _run(monkeypatch, tmp_path, source, *, reply=None, direct=False, stop='data',
         model_error=None, expected_status=None):
    marker_check = jm._has_databricks_markers
    first = {'data': '%sql\nSELECT * FROM BLOCKED_LOOKUP\n',
             'validation': 'BAD = missing_input\n',
             'exit': "if MIGRATION_TIME_FLAG:\n    aidputils.notebook.exit('done')\n"}[stop]
    _patch_common(monkeypatch, tmp_path, _notebook(first, source))
    monkeypatch.setattr(jm, '_has_databricks_markers',
                        marker_check if stop != 'exit' else lambda source: False)
    monkeypatch.setattr(jm, 'DIRECT_EXECUTE', direct)
    monkeypatch.setattr(jm, '_cell_history', [])
    monkeypatch.setattr(jm, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_registry_cache', {'contract': {}})
    monkeypatch.setattr(jm, '_STAGED_RESOURCE_PATHS', {
        'GIT/repo/my_pkg/internal.py', 'GIT/repo/foo_bar_baz.yaml'})
    calls = []

    async def model(prompt, content, **kwargs):
        assert not direct, 'DIRECT_EXECUTE invoked model'
        calls.append({**kwargs, 'content': content})
        if model_error:
            raise model_error
        return source if reply is None else reply, {'input': 0, 'output': 0}

    async def forbidden(*a, **k):
        pytest.fail('downstream repair or child execution')

    monkeypatch.setattr(jm, 'call_opus_with_tools', model)
    monkeypatch.setattr(jm, 'call_fix', forbidden)
    monkeypatch.setattr(jm, '_inline_child_notebook', forbidden)
    monkeypatch.setattr(jm, '_resolve_pip_cell_packages', forbidden)
    executed_cells = []
    safe_execute = jm._safe_execute

    async def audited_execute(session, code, **kwargs):
        executed_cells.append(code)
        return await safe_execute(session, code, **kwargs)

    monkeypatch.setattr(jm, '_safe_execute', audited_execute)
    kernel = _SequenceKernel({
        'BLOCKED_LOOKUP': [_error('[TABLE_OR_VIEW_NOT_FOUND] blocked_lookup', 'AnalysisException')],
        'BAD =': [_error("name 'missing_input' is not defined", 'NameError')],
    })
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'contract', '01_task', {}))
    expected = {'data': 'PARTIAL_DATA_UNAVAILABLE', 'validation': 'FAIL',
                'exit': 'PASS_CONDITIONAL_EXIT_COVERAGE'}[stop]
    assert result['status'] == (expected_status or expected), result
    assert result['failed'] == (1 if stop == 'validation' else 0)
    assert executed_cells
    assert all('BLOCKED_LOOKUP' in code or 'MIGRATION_TIME_FLAG' in code or 'BAD =' in code
               for code in executed_cells), 'downstream cell reached execution'
    _, saved = _saved_sources(kernel)
    tail = ''.join(saved['cells'][1]['source'])
    direct_code = '\n'.join(c for c in kernel.executed if 'base64.b64decode' not in c)
    assert source.strip() not in direct_code, 'tail executed'
    for call in calls:
        assert set(jm._TOOLS_THAT_RUN_CODE) <= set(call['exclude_tools'])
    assert any(h.get('replay_code', '').strip() == tail.strip() for h in jm._cell_history)
    logs = '\n'.join(text for path, text in kernel.files.items()
                     if path.endswith(('.md', '.txt')))
    return tail, calls, logs


SLACK_POST = (
    'from slack_sdk import WebClient\n'
    'client = WebClient(token=tok)\n'
    'client.chat_postMessage(channel=ch, text="fixture")\n'
)


@pytest.mark.parametrize('magic', ['', '%python\n', '%pyspark\n'])
@pytest.mark.parametrize('restore_slack', [False, True])
@pytest.mark.parametrize('stop', ['data', 'validation'])
@pytest.mark.parametrize('source,converted', [
    (COLLISION, REPAIRED),
    ('API = boto3.client("glue")\n',
     '# API = boto3.client("glue")\n'
     '# Oracle tool modification: use catalog metadata\n'
     'API = spark.catalog.listTables()\n'),
])
def test_neutralised_slack_does_not_skip_stopped_python_conversion(
        monkeypatch, tmp_path, magic, restore_slack, stop, source, converted):
    from test_slack_execution_boundary import _live_calls
    reply_slack = (SLACK_POST if restore_slack else
                   jm._neutralize_for_execution(SLACK_POST, tag='fixture'))
    tail, calls, logs = _run(
        monkeypatch, tmp_path, magic + source + SLACK_POST,
        reply=('%python\n' if magic else '') + converted + reply_slack, stop=stop)
    assert len(calls) == 1
    assert converted.strip() in tail
    assert jm._customer_kernel_global_rejection(tail) is None
    assert 'chat_postMessage' in tail, 'preserve the neutralised statement'
    assert 'chat_postMessage' not in _live_calls(jm._python_cell_ast_source(tail))
    # The model receives the already neutralised source, not a live Slack call.
    assert '# client.chat_postMessage' in calls[0]['content']
    assert 'not converted by the model because the Slack guard changed it' not in logs


@pytest.mark.parametrize('magic', ['', '%python\n', '%pyspark\n'])
@pytest.mark.parametrize('failure', ['unavailable', 'invalid', 'masking'])
def test_mixed_slack_conversion_failure_preserves_source_and_non_success(
        monkeypatch, tmp_path, magic, failure):
    from test_slack_execution_boundary import _live_calls
    reply = ('this is not valid python @\n' if failure == 'invalid' else
             'print("Skipping write because data is unavailable")\n')
    tail, calls, logs = _run(
        monkeypatch, tmp_path, magic + WRITE + SLACK_POST, reply=reply,
        model_error=RuntimeError('fixture model unavailable') if failure == 'unavailable' else None)
    assert len(calls) == 1
    assert WRITE.strip() in tail
    assert 'chat_postMessage' in tail
    assert 'chat_postMessage' not in _live_calls(jm._python_cell_ast_source(tail))
    assert 'UNRESOLVED code-only conversion' in logs


@pytest.mark.parametrize('direct', [False, True])
def test_mixed_slack_direct_execute_keeps_model_disabled(monkeypatch, tmp_path, direct):
    tail, calls, _ = _run(monkeypatch, tmp_path, COLLISION + SLACK_POST,
                          reply=REPAIRED + SLACK_POST, direct=direct)
    assert len(calls) == (0 if direct else 1)
    assert bool(jm._customer_kernel_global_rejection(tail)) == direct


@pytest.mark.parametrize('restriction', ['unresolved_slack', 'pip', 'conditional_exit'])
def test_mixed_slack_existing_conversion_restrictions_remain(
        monkeypatch, tmp_path, restriction):
    extra = {
        'unresolved_slack': 'getattr(client, method)(channel=ch)\n',
        'pip': '!pip install fixture-package\nPATH = "s3://fixture-bucket/input"\n',
        'conditional_exit': 'PATH = "s3://fixture-bucket/input"\n',
    }[restriction]
    reply_extra = extra.replace('!pip install fixture-package', '# !pip install fixture-package')
    tail, calls, logs = _run(
        monkeypatch, tmp_path, COLLISION + SLACK_POST + extra,
        stop='exit' if restriction == 'conditional_exit' else 'data',
        reply=REPAIRED + SLACK_POST + reply_extra)
    assert len(calls) == (0 if restriction == 'conditional_exit' else 1)
    assert (COLLISION.strip() if restriction == 'conditional_exit' else REPAIRED.strip()) in tail
    if restriction == 'conditional_exit':
        assert 'the Slack guard changed it' in logs
    elif restriction == 'unresolved_slack':
        assert '[slack:dynamic-dispatch]' in logs
    else:
        assert 'pip install neutralised' in logs
        assert not jm._find_pip_installs(tail)[0]


@pytest.mark.parametrize('install', ['%pip install fixture-package', '!pip install fixture-package',
                                   '!python -m pip install fixture-package'])
@pytest.mark.parametrize('magic', ['', '%python\n', '%pyspark\n'])
@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('source,converted', [
    (COLLISION, REPAIRED),
    ('API = boto3.client("glue")\n', 'API = spark.catalog.listTables()\n'),
])
def test_stopped_pip_remainder_retains_conversion_without_installing(
        monkeypatch, tmp_path, install, magic, direct, source, converted):
    tail, calls, logs = _run(
        monkeypatch, tmp_path, magic + install + '\n' + source,
        reply=('%python\n' if magic else '') + '# AIDP: pip neutralised; installation not attempted — '
              + install + '\n' + converted,
        direct=direct)
    assert len(calls) == (0 if direct else 1)
    assert (source.strip() if direct else converted.strip()) in tail
    assert not jm._find_pip_installs(tail)[0]
    assert 'installation not attempted' in tail
    assert 'pip install neutralised' in logs


@pytest.mark.parametrize('finding', ['read', 'dynamic', 'binding', 'combined'])
def test_pip_and_slack_reporting_do_not_veto_remainder(monkeypatch, tmp_path, finding):
    slack = SLACK_FINDING_CASES[finding]
    tail, calls, logs = _run(
        monkeypatch, tmp_path, '!pip install fixture-package\n' + COLLISION + slack,
        reply='# !pip install fixture-package\n' + REPAIRED + slack)
    assert len(calls) == 1
    assert jm._customer_kernel_global_rejection(tail) is None
    assert not jm._find_pip_installs(tail)[0]
    assert 'pip install neutralised' in logs


def test_unresolved_pip_dependency_keeps_finding_and_converts_remainder(monkeypatch, tmp_path):
    tail, calls, logs = _run(
        monkeypatch, tmp_path, '!pip install -r requirements.txt\n' + COLLISION,
        reply='# !pip install -r requirements.txt\n' + REPAIRED, expected_status='PARTIAL')
    assert len(calls) == 1
    assert jm._customer_kernel_global_rejection(tail) is None
    assert not jm._find_pip_installs(tail)[0]
    assert 'dependency resolution remains incomplete' in logs


def test_invalid_pip_remainder_does_not_reach_the_model(monkeypatch, tmp_path):
    tail, calls, logs = _run(
        monkeypatch, tmp_path, '!pip install -r requirements.txt\n%time x = 1\n',
        expected_status='PARTIAL')
    assert not calls
    assert not jm._find_pip_installs(tail)[0]
    assert 'cannot be neutralised' in logs


def test_stopped_pip_only_does_not_invoke_model(monkeypatch, tmp_path):
    tail, calls, _ = _run(monkeypatch, tmp_path, '!pip install fixture-package\n')
    assert not calls
    assert not jm._find_pip_installs(tail)[0]
    assert 'installation not attempted' in tail


@pytest.mark.parametrize('install', ['%pip install fixture-package', '!pip install fixture-package',
                                   '!python -m pip install fixture-package'])
def test_model_cannot_reactivate_pip_after_validation_stops(monkeypatch, tmp_path, install):
    tail, calls, logs = _run(monkeypatch, tmp_path, COLLISION, reply=install + '\n' + REPAIRED)
    assert len(calls) == 1
    assert not jm._find_pip_installs(tail)[0]
    if install.startswith('%pip'):
        assert jm._customer_kernel_global_rejection(tail) is None
        assert 'installation not attempted' in tail
    else:
        # Invalid shell syntax is rejected by the existing candidate check;
        # retain source and report the incomplete conversion honestly.
        assert COLLISION.strip() in tail
        assert 'UNRESOLVED code-only conversion' in logs


_SLACK_CLIENT = 'from slack_sdk import WebClient\nclient = WebClient(token=tok)\n'
SLACK_FINDING_CASES = {
    'none': '',
    'post': SLACK_POST,
    'read': _SLACK_CLIENT + 'history = client.conversations_history(channel=ch)\n',
    'dynamic': _SLACK_CLIENT + 'getattr(client, method)(channel=ch)\n',
    'unclassified': _SLACK_CLIENT + 'client.fixture_action()\n',
    'binding': _SLACK_CLIENT + 'response = client.chat_postMessage(channel=ch, text="fixture")\n',
    'weak': 'client = get_slack_config()\nvalue = client.get("channel")\n',
    'indirect': _SLACK_CLIENT + 'clients["s"].files_upload_v2(channel=ch, file=f)\n',
    'combined': SLACK_POST + 'history = client.conversations_history(channel=ch)\n'
                'getattr(client, method)(channel=ch)\n',
}


@pytest.mark.parametrize('finding', SLACK_FINDING_CASES)
@pytest.mark.parametrize('stop,direct', [('data', False), ('data', True), ('validation', False)])
@pytest.mark.parametrize('magic', ['', '%python\n', '%pyspark\n'])
@pytest.mark.parametrize('source,converted', [
    (COLLISION, REPAIRED),
    ('API = boto3.client("glue")\n',
     '# API = boto3.client("glue")\n# Oracle tool modification: use catalog metadata\n'
     'API = spark.catalog.listTables()\n'),
])
def test_validation_stop_conversion_is_independent_of_slack_reporting(
        monkeypatch, tmp_path, finding, stop, direct, magic, source, converted):
    from test_slack_execution_boundary import _live_calls
    slack = SLACK_FINDING_CASES[finding]
    tail, calls, logs = _run(
        monkeypatch, tmp_path, magic + source + slack, direct=direct, stop=stop,
        reply=('%python\n' if magic else '') + converted + slack)
    assert len(calls) == (0 if direct else 1)
    assert (source.strip() if direct else converted.strip()) in tail
    assert bool(jm._customer_kernel_global_rejection(tail)) == (direct and source == COLLISION)
    assert not {'chat_postMessage', 'fixture_action'} & set(
        _live_calls(jm._python_cell_ast_source(tail)))
    if finding in ('read', 'combined'):
        assert 'conversations_history' in _live_calls(jm._python_cell_ast_source(tail))
        assert '[slack:read-left-running]' in logs
    if finding in ('dynamic', 'combined'):
        assert '[slack:dynamic-dispatch]' in logs
    if finding == 'indirect':
        assert '[slack:indirect-receiver]' in logs
    if not direct:
        assert 'not converted by the model because the Slack guard' not in logs


@pytest.mark.parametrize('finding', SLACK_FINDING_CASES)
def test_slack_reporting_does_not_add_conversion_to_ordinary_python(
        monkeypatch, tmp_path, finding):
    tail, calls, _ = _run(monkeypatch, tmp_path, 'answer = 7\n' + SLACK_FINDING_CASES[finding])
    assert not calls
    assert 'answer = 7' in tail


@pytest.mark.parametrize('finding', SLACK_FINDING_CASES)
def test_slack_reporting_retains_conditional_exit_policy(monkeypatch, tmp_path, finding):
    source = 'PATH = "s3://fixture-bucket/input"\n' + COLLISION + SLACK_FINDING_CASES[finding]
    tail, calls, _ = _run(monkeypatch, tmp_path, source, stop='exit', reply=source)
    assert len(calls) == (1 if finding == 'none' else 0)
    assert COLLISION.strip() in tail


@pytest.mark.parametrize('finding', SLACK_FINDING_CASES)
@pytest.mark.parametrize('failure', ['unavailable', 'invalid', 'masking'])
def test_reported_slack_conversion_failure_preserves_source(monkeypatch, tmp_path, finding, failure):
    reply = 'invalid python @\n' if failure == 'invalid' else 'print("Skipping write")\n'
    tail, calls, logs = _run(
        monkeypatch, tmp_path, WRITE + SLACK_FINDING_CASES[finding], reply=reply,
        model_error=RuntimeError('fixture unavailable') if failure == 'unavailable' else None)
    assert len(calls) == 1
    assert WRITE.strip() in tail
    assert 'UNRESOLVED code-only conversion' in logs


@pytest.mark.parametrize('finding', SLACK_FINDING_CASES)
@pytest.mark.parametrize('direct', [False, True])
def test_strict_stop_never_dispatches_mixed_slack_tail(monkeypatch, tmp_path, finding, direct):
    _patch_common(monkeypatch, tmp_path, _notebook(
        '%sql\nSELECT * FROM BLOCKED_LOOKUP\n', COLLISION + SLACK_FINDING_CASES[finding]))
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', True)
    monkeypatch.setattr(jm, 'DIRECT_EXECUTE', direct)
    monkeypatch.setattr(jm, '_registry_cache', {'strict_contract': {}})

    async def forbidden(*args, **kwargs):
        pytest.fail('strict stop dispatched model or repair')

    monkeypatch.setattr(jm, 'call_opus_with_tools', forbidden)
    monkeypatch.setattr(jm, 'call_fix', forbidden)
    kernel = _SequenceKernel({'BLOCKED_LOOKUP': [
        _error('[TABLE_OR_VIEW_NOT_FOUND] blocked_lookup', 'AnalysisException')]})
    result = asyncio.run(jm.process_notebook(
        '/Workspace/test_cond/source.ipynb', kernel, 'strict_contract', '01_task', {}))
    assert result['status'] == 'PARTIAL_DATA_UNAVAILABLE'
    assert result['job_should_stop']
    assert len(result['cell_results']) == 1
    assert not any('answer = sc.applicationId' in code for code in kernel.executed
                   if 'base64.b64decode' not in code)


def test_neutralised_slack_alone_does_not_add_a_model_call(monkeypatch, tmp_path):
    tail, calls, _ = _run(monkeypatch, tmp_path, 'answer = 7\n' + SLACK_POST)
    assert not calls
    assert 'answer = 7' in tail


@pytest.mark.parametrize('magic', ['', '%python\n', '%pyspark\n'])
@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('source,expected', [
    (COLLISION, REPAIRED.strip()),
    ('from my_pkg._internal import value\n', 'from my_pkg.internal import value'),
    ('config_path = "foo:bar+baz.yaml"\n', 'config_path = "foo_bar_baz.yaml"'),
    ('import pandas as pd\ndf = pd.DataFrame({\"x\": [1]})\nother = pd.DataFrame({\"x\": [2]})\ndf = df.append(other)\n', 'pd.concat'),
])
def test_data_gap_retains_established_conversion(monkeypatch, tmp_path, direct, source, expected, magic):
    tail, calls, _ = _run(monkeypatch, tmp_path, magic + source,
                          reply=("%python\n" if magic else "") + REPAIRED if source == COLLISION else None,
                          direct=direct)
    if source == COLLISION:
        assert (source.strip() if direct else expected) in tail
        assert len(calls) == (0 if direct else 1)
    else:
        # The existing pandas rewriter conservatively leaves magic-bearing
        # source alone; retain that behavior rather than widening its parser.
        expected = source.strip() if magic and expected == 'pd.concat' else expected
        assert expected in tail
        assert not calls


@pytest.mark.parametrize('stop', ['data', 'exit'])
@pytest.mark.parametrize('reply', [
    'print("Skipping write because upstream data is unavailable")\n',
    '# df.write.parquet("s3://fixture-bucket/output")\nprint("Skipping write")\n',
])
def test_model_cannot_remove_continuation_write(monkeypatch, tmp_path, stop, reply):
    tail, calls, logs = _run(monkeypatch, tmp_path, WRITE, reply=reply, stop=stop)
    assert len(calls) == 1
    assert WRITE.strip() in tail
    assert 'REJECTED masking fix' in logs
    assert 'UNRESOLVED' in logs


def test_real_write_conversion_is_retained(monkeypatch, tmp_path):
    reply = 'df.write.parquet("oci://fixture@test/output")\n'
    tail, calls, logs = _run(monkeypatch, tmp_path, WRITE, reply=reply)
    assert len(calls) == 1 and reply.strip() in tail
    assert 'REJECTED masking fix' not in logs


def test_unstaged_resource_names_are_preserved(monkeypatch, tmp_path):
    source = 'from unstaged._private import value\npath = "not:staged.yaml"\n'
    tail, calls, _ = _run(monkeypatch, tmp_path, source)
    assert source.strip() in tail and not calls


def test_model_restored_upload_is_neutralised_before_saving(monkeypatch, tmp_path):
    from test_slack_execution_boundary import RESTORED_UPLOAD, _live_calls
    source = 'PATH = "s3://fixture-bucket/input"\n'
    tail, calls, _ = _run(monkeypatch, tmp_path, source, reply=RESTORED_UPLOAD)
    assert len(calls) == 1
    assert 'files_upload_v2' in tail, 'original upload must remain as a comment'
    assert 'files_upload_v2' not in _live_calls(tail)


@pytest.mark.parametrize('stop', ['data', 'exit'])
@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('source', [
    '%sql\nSELECT \'foo:bar+baz.yaml\' AS business_label\n',
    '%scala\nval business_label = "foo:bar+baz.yaml"\n',
])
def test_foreign_language_literals_do_not_enter_python_finalization(
        monkeypatch, tmp_path, source, direct, stop):
    tail, calls, _ = _run(monkeypatch, tmp_path, source, direct=direct, stop=stop)
    assert tail.strip() == source.strip()
    assert not calls


@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('quote', ["'", '"', "r'"])
@pytest.mark.parametrize('install', ['!pip install fixture-package', '%pip install fixture-package',
                                   '!python -m pip install -r requirements.txt'])
@pytest.mark.parametrize('stop', ['data', 'exit'])
def test_native_sql_pip_literal_is_data_after_stop(
        monkeypatch, tmp_path, direct, quote, install, stop):
    source = f'%sql\nSELECT {quote}hello\n{install}\nworld{quote[-1]} AS label\n'
    # Audit both source routing and the returned-code pass: neither may send
    # native-language source to the Python pip lexer.
    scanner = jm._find_pip_installs

    def python_pip_scanner(code, **kwargs):
        assert not code.lstrip().startswith(('%sql', '%scala'))
        return scanner(code, **kwargs)

    monkeypatch.setattr(jm, '_find_pip_installs', python_pip_scanner)
    tail, calls, logs = _run(monkeypatch, tmp_path, source, direct=direct, stop=stop)
    assert tail.strip() == source.strip()
    assert not calls
    assert 'PIP UNRESOLVED' not in logs
    assert 'pip install neutralised' not in logs


@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('stop', ['data', 'exit'])
@pytest.mark.parametrize('language', ['%sql', '%scala'])
def test_native_triple_literal_bypasses_python_pip_scanner(
        monkeypatch, tmp_path, direct, stop, language):
    body = ('SELECT """hello\n!pip install fixture-package\nworld""" AS label\n'
            if language == '%sql' else
            'val label = """hello\n!pip install fixture-package\nworld"""\n')
    source = language + '\n' + body
    scanner = jm._find_pip_installs

    def python_pip_scanner(code, **kwargs):
        assert not code.lstrip().startswith(('%sql', '%scala'))
        return scanner(code, **kwargs)

    monkeypatch.setattr(jm, '_find_pip_installs', python_pip_scanner)
    tail, calls, logs = _run(monkeypatch, tmp_path, source, direct=direct, stop=stop)
    assert tail.strip() == source.strip()
    assert not calls
    assert 'PIP UNRESOLVED' not in logs


@pytest.mark.parametrize('direct', [False, True])
@pytest.mark.parametrize('stop', ['data', 'exit'])
@pytest.mark.parametrize('language', ['%sql', '%scala'])
@pytest.mark.parametrize('install', ['%pip install fixture-package', '!pip install fixture-package'])
def test_real_pip_prefix_with_native_remainder_still_neutralized(
        monkeypatch, tmp_path, direct, stop, language, install):
    body = ("SELECT 'foo:bar+baz.yaml' AS label\n" if language == '%sql' else
            'val label = "foo:bar+baz.yaml"\n')
    tail, calls, logs = _run(
        monkeypatch, tmp_path, install + '\n' + language + '\n' + body,
        direct=direct, stop=stop)
    assert tail.startswith(language + '\n')
    assert body.strip() in tail
    assert not jm._find_pip_installs(tail)[0]
    assert install in tail, 'original install is retained as a comment'
    assert 'pip install neutralised' in logs
    assert not calls
