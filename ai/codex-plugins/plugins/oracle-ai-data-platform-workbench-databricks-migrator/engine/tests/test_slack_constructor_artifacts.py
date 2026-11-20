"""Constructor binding contract at both production artifact destinations."""
import ast
import asyncio
import json

import pytest

from test_slack_token_artifacts import runtime, jm
from slack_ast_neutralize import neutralize_cell, new_bindings, update_bindings


_SETUP = ('from slack_sdk.errors import SlackApiError as Ctor\n'
          'from slack_sdk import WebClient\n')
_CALL = "obj = Ctor(token='fixture-authentication')\nobj.files_upload_v2(file=path)\n"
_APP = "obj = Ctor(token='application-value')\nobj.update(other)\n"
_CASES = [
    ('branch_import', 'class Notifier:\n    def send(self):\n'
     '        if self.token:\n            from slack_sdk import WebClient as Ctor\n'
     "            obj = Ctor(token='fixture-authentication')\n"
     '            obj.files_upload_v2(file=path)\n', 'protect'),
    ('branch_dictionary', 'Ctor = WebClient\nif condition:\n    Ctor = dict\n'
     + ''.join('    ' + line + '\n' for line in _APP.splitlines()), 'preserve'),
    ('parameter_client', 'def use(Ctor):\n'
     + ''.join('    ' + line + '\n' for line in _CALL.splitlines())
     + 'use(WebClient)\n', 'protect'),
    ('parameter_dictionary', 'Ctor = WebClient\ndef use(Ctor):\n'
     + ''.join('    ' + line + '\n' for line in _APP.splitlines())
     + 'use(dict)\n', 'preserve'),
    ('parameter_mixed', 'def use(Ctor):\n'
     + ''.join('    ' + line + '\n' for line in _CALL.splitlines())
     + 'use(WebClient)\nuse(dict)\n', 'refuse'),
    ('global_client', 'def bind():\n    global Ctor\n    Ctor = WebClient\nbind()\n'
     + _CALL, 'protect'),
    ('global_dictionary', 'Ctor = WebClient\ndef bind():\n    global Ctor\n'
     '    Ctor = dict\nbind()\n' + _APP, 'preserve'),
    ('nonlocal_client', 'def outer():\n    Ctor = dict\n'
     '    def bind():\n        nonlocal Ctor\n        Ctor = WebClient\n    bind()\n'
     + ''.join('    ' + line + '\n' for line in _CALL.splitlines())
     + 'outer()\n', 'protect'),
    ('nonlocal_dictionary', 'def outer():\n    Ctor = WebClient\n'
     '    def bind():\n        nonlocal Ctor\n        Ctor = dict\n    bind()\n'
     + ''.join('    ' + line + '\n' for line in _APP.splitlines())
     + 'outer()\n', 'preserve'),
    ('client_subclass', 'class Ctor(WebClient):\n    pass\n' + _CALL, 'protect'),
    ('dictionary_factory', 'Ctor = WebClient\ndef choose():\n    return dict\n'
     'Ctor = choose()\n' + _APP, 'preserve'),
    ('conditional_factories', 'def choose():\n    return WebClient\n'
     'if condition:\n    def choose():\n        return dict\nCtor = choose()\n'
     + _CALL, 'refuse'),
    ('container_mutation', 'choices = [dict]\nchoices[0] = WebClient\n'
     'Ctor = choices[0]\n' + _CALL, 'refuse'),
    ('container_alias_mutation', 'choices = [dict]\nalias = choices\n'
     'alias[0] = WebClient\nCtor = choices[0]\n' + _CALL, 'refuse'),
    ('decorated_replacement', '@decorate\ndef Ctor():\n    pass\n' + _CALL, 'refuse'),
]

_CASES += [
    ('mixed_module', 'import slack_sdk as sdk\nif condition:\n    import internal_http as sdk\n'
     "obj = sdk.WebClient(token='application-value')\nobj.update(other)\n", 'refuse'),
    ('method_free_name', 'Ctor = WebClient\nclass Application:\n    Ctor = dict\n'
     '    def use(self):\n'
     + ''.join('        ' + line + '\n' for line in _CALL.splitlines()), 'protect'),
    ('qualified_exception', 'import slack_sdk.errors as errors\n'
     "raise errors.SlackApiError(f'application error: {error}', response)\n", 'preserve'),
]
_CASES += [(case + suffix, body.replace('use(WebClient)', client_call).replace('use(dict)', app_call), outcome)
           for case, body, outcome in list(_CASES) if case in {'parameter_client', 'parameter_dictionary'}
           for suffix, client_call, app_call in [
               ('_star', 'use(*[WebClient])', 'use(*[dict])'),
               ('_kwargs', "use(**{'Ctor': WebClient})", "use(**{'Ctor': dict})")]]


@pytest.mark.parametrize('split_cells', [False, True])
@pytest.mark.parametrize('case, body, outcome', _CASES, ids=[case[0] for case in _CASES])
def test_constructor_scope_contract_at_artifact_boundary(
        monkeypatch, runtime, case, body, outcome, split_cells):
    sources = [_SETUP, body] if split_cells else [_SETUP + body]
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}
                          for source in sources],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith('.ipynb')]
    if outcome == 'refuse':
        assert result['status'] == 'FAIL', result
        assert 'UNRESOLVED Slack constructor identity' in result['error']
        assert finals == []
        assert all('fixture-authentication' not in text for text in runtime.files.values())
        return
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
    assert len(finals) == 2
    for saved in finals:
        source = '\n'.join(''.join(cell['source']) for cell in saved['cells'])
        tree = ast.parse(source)
        if outcome == 'preserve':
            assert ast.dump(tree) == ast.dump(ast.parse(_SETUP + body))
        else:
            assert '<redacted>' in source
            assert 'fixture-authentication' not in source
            assert not any(isinstance(node, ast.Call)
                           and getattr(node.func, 'attr', '') == 'files_upload_v2'
                           for node in ast.walk(tree))
    if outcome == 'protect':
        assert all('fixture-authentication' not in text for text in runtime.files.values())


# PR #285 re-review: keep this matrix at the production artifact boundary.
# Unsupported call boundaries may refuse; silently delivering credentials may not.
@pytest.mark.parametrize('credential', [
    "'fixture-authentication'",
    "'xox' + 'b-' + '1234567890-fixture'",
], ids=['literal', 'assembled'])
@pytest.mark.parametrize('invocation', ['separate_cell', 'separate_cell_alias', 'alias', 'escaped', 'factory', 'factory_keyword', 'factory_no_return', 'factory_nested'])
def test_review_call_boundaries_do_not_publish_credentials(
        monkeypatch, runtime, credential, invocation):
    if invocation in {'separate_cell', 'separate_cell_alias', 'alias', 'escaped'}:
        parameter = 'Ctor' if invocation in {'separate_cell_alias', 'escaped'} else 'WebClient'
        definition = (_SETUP + f'def use({parameter}):\n'
                      f'    obj = {parameter}(token={credential})\n'
                      '    obj.files_upload_v2(file=path)\n')
        if invocation == 'escaped':
            sources = [definition + 'use(dict)\nregister(use)\n']
        elif invocation.startswith('separate_cell'):
            sources = [definition, 'use(WebClient)\n']
        else:
            sources = [definition + 'f = use\nf(WebClient)\n']
    else:
        actual = credential if invocation == 'factory' else f'token={credential}'
        definition = 'def Ctor(token):\n    return WebClient(token=token)\n'
        if invocation == 'factory_no_return':
            definition = ('def Ctor(token):\n    obj = WebClient(token=token)\n'
                          '    obj.files_upload_v2(file=path)\n')
        elif invocation == 'factory_nested':
            definition = ('def Ctor(token):\n    def make():\n'
                          '        return WebClient(token=token)\n    return make()\n')
        sources = [_SETUP + definition
                   + f'client = Ctor({actual})\nclient.files_upload_v2(file=path)\n']
    # Delivery refusal happens after conversion/validation. The runtime guard
    # must still disable uploads in these bodies before artifact redaction.
    bindings = new_bindings()
    for source in sources:
        guarded = neutralize_cell(source, bindings=bindings).source
        assert not any(isinstance(node, ast.Call)
                       and getattr(node.func, 'attr', '') == 'files_upload_v2'
                       for node in ast.walk(ast.parse(guarded)))
        update_bindings(source, bindings)
    notebook = {'cells': [{'cell_type': 'code', 'source': [src], 'metadata': {}, 'outputs': []}
                          for src in sources],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith('.ipynb')]
    if invocation != 'alias':
        assert result['status'] == 'FAIL', result
        assert 'UNRESOLVED' in result['error']
        assert finals == []
    else:
        assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
        assert len(finals) == 2
        for saved in finals:
            source = '\n'.join(''.join(cell['source']) for cell in saved['cells'])
            assert '<redacted>' in source
            assert not any(isinstance(node, ast.Call)
                           and getattr(node.func, 'attr', '') == 'files_upload_v2'
                           for node in ast.walk(ast.parse(source)))
    for text in runtime.files.values():
        assert 'fixture-authentication' not in text
        assert '1234567890-fixture' not in text


@pytest.mark.parametrize('split_cells', [False, True])
@pytest.mark.parametrize('setup, body, preserve', [
    ('from helpers import get_slack_client\n',
     'client = get_slack_client()\nclient.files_upload_v2(file=path)\n', False),
    ('from helpers import get_slack_config\n',
     'cfg = get_slack_config()\nvalue = cfg.get("channel")\n', True),
    ('from application import WebClient\n',
     'client = WebClient(token="application-value")\nclient.update(other)\n', True),
    ('from slack_sdk.errors import SlackApiError\n',
     'raise SlackApiError(f"application error: {error}", response)\n', True),
    ('from slack_sdk import WebClient\n',
     'def use(WebClient):\n    obj = WebClient(token="application-value")\n'
     '    obj.update(other)\nf = use\nf(dict)\n', True),
    ('from slack_sdk import WebClient\n',
     'slack_builder = dict\nobj = slack_builder(token="application-value")\n'
     'obj.update(other)\n', True),
    ('from slack_sdk import WebClient\n',
     'def make():\n    return WebClient(token="fixture-authentication")\n'
     'client = make()\nclient.files_upload_v2(file=path)\n', False),
    ('from slack_sdk import WebClient\n',
     'def make(token):\n    return dict(token=token)\n'
     'obj = make("application-value")\nobj.update(other)\n', True),
], ids=['imported_factory', 'config_read', 'ordinary_import', 'exception',
        'ordinary_alias_argument', 'ordinary_assignment', 'no_argument_factory', 'ordinary_factory'])
def test_review_factory_guard_and_application_preservation(
        monkeypatch, runtime, split_cells, setup, body, preserve):
    sources = [setup, body] if split_cells else [setup + body]
    notebook = {'cells': [{'cell_type': 'code', 'source': [src], 'metadata': {}, 'outputs': []}
                          for src in sources],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    monkeypatch.setattr(jm, 'download_notebook', lambda *a, **kw: json.dumps(notebook).encode())
    result = asyncio.run(jm.process_notebook(
        '/Workspace/redaction_fixture/source.ipynb', runtime, 'fixture', 'task_current', {},
        run_all=False))
    assert result['status'] == 'CONVERTED_NOT_VALIDATED', result
    finals = [json.loads(text) for path, text in runtime.files.items() if path.endswith('.ipynb')]
    assert len(finals) == 2
    assert all('fixture-authentication' not in text for text in runtime.files.values())
    for saved in finals:
        source = '\n'.join(''.join(cell['source']) for cell in saved['cells'])
        tree = ast.parse(source)
        if preserve:
            assert ast.dump(tree) == ast.dump(ast.parse(setup + body))
        else:
            assert not any(isinstance(node, ast.Call)
                           and getattr(node.func, 'attr', '') == 'files_upload_v2'
                           for node in ast.walk(tree))
