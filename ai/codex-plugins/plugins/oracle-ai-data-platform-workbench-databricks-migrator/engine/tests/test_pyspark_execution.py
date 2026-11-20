"""AIDP returned success without running a %pyspark body on 2026-10-05."""
import asyncio
import pytest
from test_leading_magic_injection import jm, _drive_child_under_migrated_base


@pytest.mark.parametrize('header', ['%pyspark', '%pyspark # original note', '%PYSPARK'])
def test_child_alias_executes_and_persists_python(monkeypatch, header):
    source = header + '\nassert 6 * 7 == 42\n'
    result, kernel, cached = _drive_child_under_migrated_base(monkeypatch, source)
    assert result['status'] == 'ok', result
    executed = [c for c in kernel.executed if 'assert 6 * 7' in c and 'base64.b64decode' not in c]
    assert len(executed) == 1
    assert executed[0].splitlines()[0] == '%python'
    assert '# ' + header in executed[0]
    saved = ''.join(cached['cells'][0]['source'])
    assert saved == executed[0] == kernel.save_payloads[-1]['cells']['0']
    assert jm._promote_leading_magic(saved).source == saved


@pytest.mark.parametrize('header', ['%pyspark', '%pyspark # original note'])
def test_repair_and_replay_boundary_runs_body_and_propagates_failure(header):
    class Kernel:
        async def execute(self, code, **kwargs):
            assert code.splitlines()[0] == '%python'
            exec(code.partition('\n')[2], {})
    with pytest.raises(RuntimeError, match='BODY_EXECUTED'):
        asyncio.run(jm._safe_execute(Kernel(), header + '\nraise RuntimeError("BODY_EXECUTED")'))


@pytest.mark.parametrize('header', ['%pyspark --bad', '%pyspark -h'])
def test_supported_alias_arguments_never_reach_kernel(header):
    class Kernel:
        async def execute(self, *args, **kwargs):
            pytest.fail('unsupported declaration reached AIDP')
    with pytest.raises(ValueError, match='UNRESOLVED'):
        asyncio.run(jm._safe_execute(Kernel(), header + '\npass'))


@pytest.mark.parametrize('source', [
    '%python\nx = 1', '%scala\nval x = 1', '%sql\nSELECT 1',
    '%pyspark-extra\nx = 1', '%%pyspark\nx = 1', '', None,
    '# %pyspark\nx = 1', 'text = """\n%pyspark\n"""',
])
def test_other_languages_and_inert_text_remain_identical(source):
    assert jm._canonicalize_pyspark_magic(source) == source


@pytest.mark.parametrize('source', ['%pyspark', '%pyspark\r\nx = 1\r\n', '%pyspark # note\nx = 1'])
def test_canonicalization_preserves_body_header_and_is_idempotent(source):
    out = jm._canonicalize_pyspark_magic(source)
    header, separator, body = source.partition('\n')
    assert '# ' + header in out
    if separator:
        assert out.endswith(separator + body)
    assert jm._canonicalize_pyspark_magic(out) == out


@pytest.mark.parametrize(('native', 'prefix'), [('python', ''), ('scala', ''), ('python', '# prior tool note\n')])
@pytest.mark.parametrize('run_all', [True, False])
@pytest.mark.parametrize('pip', ['', '%pip install real_pkg\n', 'if True:\n    !pip install real_pkg\n'])
def test_parent_conversion_execution_and_delivery(monkeypatch, tmp_path, native, run_all, prefix, pip):
    import re
    from test_pip_magic_gate import _patch_common, _Kernel, _saved_notebook
    sources = [prefix + '%pyspark\n' + pip + 'pyspark_scope_probe = 41\n',
               '%python\nassert pyspark_scope_probe == 41\n']
    notebook = {'cells': [{'cell_type': 'code', 'source': [s], 'metadata': {},
                          'outputs': [], 'execution_count': None} for s in sources],
                'metadata': {'language_info': {'name': native}},
                'nbformat': 4, 'nbformat_minor': 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    # Scala metadata enters the converter; transport boundary is identity here.
    async def identity_conversion(system, prompt, **kwargs):
        body = re.findall(r"```(?:python|scala)\n(.*?)\n```", prompt, re.S)[-1]
        return body, {"input": 0, "output": 0}
    monkeypatch.setattr(jm, 'call_opus_with_tools', identity_conversion)
    class Kernel(_Kernel):
        def __init__(self):
            super().__init__(); self.scope = {}; self.executed_bodies = 0
        async def execute(self, code, **kwargs):
            if 'pyspark_scope_probe' in code and 'base64.b64decode' not in code:
                assert code.startswith('%python\n'), code
                exec(code.partition('\n')[2], self.scope)
                self.executed_bodies += 1
            return await super().execute(code, **kwargs)
    kernel = Kernel()
    result = asyncio.run(jm.process_notebook('/Workspace/test_pip/source.ipynb', kernel,
                                             'test_pip', '01_task', {}, run_all=run_all))
    assert result['failed'] == 0, result
    assert kernel.executed_bodies == (2 if run_all else 0)
    assert installs == (['real_pkg'] if pip and run_all else [])
    saved = _saved_notebook(kernel)
    source = next(''.join(c['source']) for c in saved['cells']
                  if 'pyspark_scope_probe = 41' in ''.join(c['source']))
    assert source.startswith('%python\n')
    assert '# %pyspark' in source


@pytest.mark.parametrize('pip', ['%pip install real_pkg\n', 'if True:\n    !pip install real_pkg\n'])
def test_child_pip_alias_executes_body_and_shares_state(monkeypatch, pip):
    from test_pip_magic_gate import _drive_child, _Kernel

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__()
            self.scope = {'pyspark_child_probe': 41}
            self.executed_bodies = 0

        async def execute(self, code, **kwargs):
            if 'pyspark_child_probe' in code and 'base64.b64decode' not in code:
                assert code.startswith('%python\n'), code
                exec(code.partition('\n')[2], self.scope)
                self.executed_bodies += 1
            return await super().execute(code, **kwargs)

    monkeypatch.setattr(jm, '_notebook_content_cache', {})
    kernel = Kernel()
    result, installs = _drive_child(monkeypatch, [
        '%pyspark\n' + pip + 'pyspark_child_probe += 1\n',
        '%python\nassert pyspark_child_probe == 42\n',
    ], True, True, kernel=kernel)
    assert result['status'] == 'ok', result
    assert result['cells_ok'] == kernel.executed_bodies == 2
    assert installs == ['real_pkg']
    assert kernel.scope['pyspark_child_probe'] == 42
