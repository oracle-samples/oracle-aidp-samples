"""#232: real nested cell loop must preserve execution and delivered source."""
import pytest
from types import SimpleNamespace
import json
import test_child_exit_conversion as harness
from nested_run_dispatch import plan, UnsupportedDispatch


@pytest.fixture(autouse=True)
def no_model_repair(monkeypatch):
    async def keep_code(code, *args, **kwargs):
        return code
    monkeypatch.setattr(harness.jm, 'call_fix', keep_code)
    monkeypatch.setattr(harness.jm, 'MAX_FIX_ATTEMPTS', 1)


@pytest.mark.parametrize('language,comment,before,after', [
    ('python','#','BEFORE_SENTINEL = 1','AFTER_SENTINEL = 1'),
    ('scala','//','val BEFORE_SENTINEL = 1','val AFTER_SENTINEL = 1')])
@pytest.mark.parametrize('shape', ['before', 'after', 'both', 'multiple'])
@pytest.mark.parametrize('api', ['dbutils', 'oidlUtils', 'aidputils'])
def test_surrounding_statements_and_calls(monkeypatch,tmp_path,language,comment,before,after,shape,api):
    lines=[f'%{language}',f'{comment} dbutils.notebook.run("/Workspace/dead", 60)']
    if shape in ('before','both','multiple'): lines.append(before)
    lines.append('dbutils.notebook.run("/Workspace/leaf", 60)')
    if shape=='multiple': lines.extend([after,'dbutils.notebook.run("/Workspace/leaf", 60)'])
    elif shape in ('after','both'): lines.append(after)
    source='\n'.join(lines).replace('dbutils.notebook.run', api + '.notebook.run')
    result,saved,kernel,_,_=harness._run(monkeypatch,tmp_path,{
        'child':[source],'leaf':['LEAF_SENTINEL = 1'], 'dead':['DEAD_SENTINEL = 1']})
    assert result['status']=='ok',result
    assert saved['child']==[source]
    executed=[s for s in kernel.executed if 'base64.b64decode' not in s]
    assert not any('DEAD_SENTINEL' in s for s in executed)
    positions=lambda marker:[i for i,s in enumerate(executed) if marker in s]
    leaves=positions('LEAF_SENTINEL')
    assert len(leaves)==(2 if shape=='multiple' else 1)
    if before in source: assert positions('BEFORE_SENTINEL')[0]<leaves[0]
    if after in source: assert leaves[0]<positions('AFTER_SENTINEL')[0]
    if shape=='multiple': assert positions('AFTER_SENTINEL')[0]<leaves[1]
    native = [i for i, code in enumerate(executed)
              if api + '.notebook.run(' in code
              and not code.lstrip().startswith(('#', '//'))
              and 'dead' not in code]
    assert len(native) == len(leaves)
    for validation, dispatch in zip(leaves, native):
        assert validation < dispatch
    assert not any(code.lstrip().startswith('%run') for code in executed)


def test_python_arguments_and_return_execute_in_order(monkeypatch,tmp_path):
    original=harness._ExitKernel.execute
    async def execute(self,code,**kwargs):
        self.namespace=getattr(self,'namespace',{})
        def run(path, timeout, arguments):
            assert json.loads((tmp_path / 'leaf.ipynb').read_text())['cells']
            assert (path, timeout, arguments) == ('/Workspace/leaf', 60, {'arg': 'fixture'})
            self.native_calls = getattr(self, 'native_calls', 0) + 1
            return 'exit_fixture'
        self.namespace['dbutils'] = SimpleNamespace(notebook=SimpleNamespace(run=run))
        if code.startswith('target =') or code.startswith('result =') or code.startswith('assert result'):
            exec(code,self.namespace)
        return await original(self,code,**kwargs)
    monkeypatch.setattr(harness._ExitKernel,'execute',execute)
    source=('target = "/Workspace/leaf"\n'
            'result = dbutils.notebook.run(target, 60, {"arg": "fixture"})\n'
            'assert result == "exit_fixture"')
    result,saved,kernel,_,_=harness._run(monkeypatch,tmp_path,{
        'child':[source],'leaf':[harness.UNCONDITIONAL]})
    assert result['status']=='ok',result
    assert 'result' in kernel.namespace, result
    assert kernel.namespace['result']=='exit_fixture'
    assert kernel.native_calls == 1
    assert saved['child']==[source]


@pytest.mark.parametrize('source', [
    'if False:\n    dbutils.notebook.run("/Workspace/leaf",60)',
    '%scala\nif (false) { dbutils.notebook.run("/Workspace/leaf",60) }',
    'print(dbutils.notebook.run("/Workspace/leaf",60))',
])
def test_unsupported_control_flow_never_dispatches_or_deletes(monkeypatch,tmp_path,source):
    result,saved,kernel,_,_=harness._run(monkeypatch,tmp_path,{
        'child':[source],'leaf':['LEAF_SENTINEL = 1']})
    assert result['status']=='error',result
    assert result['kind']=='UNRESOLVED_NOTEBOOK_RUN'
    assert saved['child']==[source]
    assert not any('LEAF_SENTINEL' in c for c in kernel.executed)


@pytest.mark.parametrize('language,source', [
 ('python','label="é"; result=dbutils.notebook.run("child",60); print(result)'),
 ('scala','val label="é"; val result=oidlUtils.notebook.run("child",60); println(result)'),
])
def test_offsets_preserve_unicode_and_semicolons(language,source):
    _,body,calls=plan(source,language)
    assert len(calls)==1
    c=calls[0]
    assert body[c.call_start:c.call_end].endswith('( "child",60)'.replace('( ', '('))


@pytest.mark.parametrize('language', ['python', 'scala'])
def test_replay_prepares_child_and_executes_native_call_preserving_source(monkeypatch, tmp_path, language):
    import asyncio
    import agent_migrate
    source = f'%{language}\ndbutils.notebook.run("/Workspace/leaf", 60)\n' + (
        'val AFTER_SENTINEL = 1' if language == 'scala' else 'AFTER_SENTINEL = 1')
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['LEAF_SENTINEL = 1']})
    assert result['status'] == 'ok', result
    entry = next(e for e in harness.jm._cell_history if e['notebook_path'].endswith('/child.ipynb'))
    monkeypatch.setattr(agent_migrate, '_cell_history', harness.jm._cell_history)
    replay = asyncio.run(harness.jm._replay_cell_entry(entry, 'verify ordered replay', kernel))
    assert replay['status'] == 'ok', replay
    assert replay['final_code'] == source
    assert saved['child'] == [source]
    assert sum('LEAF_SENTINEL' in s for s in kernel.executed) == 2
    assert sum('AFTER_SENTINEL' in s for s in kernel.executed) == 2
    assert sum('dbutils.notebook.run(' in s and not s.lstrip().startswith('#')
               for s in kernel.executed if 'base64' not in s) == 2


@pytest.mark.parametrize('language', ['python', 'scala'])
@pytest.mark.parametrize('failure', ['prefix', 'child', 'native', 'native_exception', 'suffix'])
@pytest.mark.parametrize('strict', [False, True])
def test_failed_work_prevents_later_statements(monkeypatch, tmp_path, language, failure, strict):
    async def keep_code(code, *args, **kwargs):
        return code
    monkeypatch.setattr(harness.jm, 'call_fix', keep_code)
    monkeypatch.setattr(harness.jm, 'MAX_FIX_ATTEMPTS', 1)
    original = harness._ExitKernel.execute
    async def execute(self, code, **kwargs):
        if failure == 'native_exception' and 'dbutils.notebook.run(' in code and 'base64' not in code:
            raise RuntimeError('native transport failed')
        if ('FAIL_SENTINEL' in code or (failure == 'native' and 'dbutils.notebook.run(' in code)) and 'base64' not in code:
            self.executed.append(code)
            return {'status': 'error', 'outputs': [
                {'type': 'error', 'ename': 'ValueError', 'evalue': 'fixture failure'}]}
        return await original(self, code, **kwargs)
    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    prefix = 'val ' if language == 'scala' else ''
    source = f'%{language}\n' + (prefix + 'FAIL_SENTINEL = 1\n' if failure == 'prefix' else '')
    source += 'dbutils.notebook.run("/Workspace/leaf", 60)\n' + prefix + ('FAIL_SENTINEL = 1' if failure == 'suffix' else 'AFTER_SENTINEL = 1')
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source, 'MUST_NOT_EXECUTE = 1'], 'leaf': [('FAIL_SENTINEL' if failure == 'child' else 'LEAF_SENTINEL') + ' = 1']}, strict=strict)
    assert result['status'] == 'error', result
    assert saved['child'] == [source, 'MUST_NOT_EXECUTE = 1']
    assert not any('AFTER_SENTINEL' in s for s in kernel.executed if 'base64' not in s)
    if failure == 'prefix':
        assert not any('LEAF_SENTINEL' in s for s in kernel.executed)
    assert not any('MUST_NOT_EXECUTE' in s for s in kernel.executed if 'base64' not in s)


def test_scala_interpolation_cannot_bypass_nested_dispatch(monkeypatch, tmp_path):
    source = '%scala\nval result = s"${dbutils.notebook.run(\"/Workspace/leaf\", 60)}"'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['LEAF_SENTINEL = 1']})
    assert result['status'] == 'error', result
    assert result['kind'] == 'UNRESOLVED_NOTEBOOK_RUN'
    assert saved['child'] == [source]
    assert not any('LEAF_SENTINEL' in s for s in kernel.executed)


def test_argument_values_are_not_interpreted_as_kernel_errors(monkeypatch, tmp_path):
    async def keep_code(code, *args, **kwargs):
        return code
    monkeypatch.setattr(harness.jm, 'call_fix', keep_code)
    monkeypatch.setattr(harness.jm, 'MAX_FIX_ATTEMPTS', 1)
    source = 'dbutils.notebook.run("/Workspace/leaf", 60, {"message": "Exception: expected text"})\nAFTER_SENTINEL = 1'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['LEAF_SENTINEL = 1']}, strict=True)
    assert result['status'] == 'ok', result
    assert saved['child'] == [source]
    assert any('AFTER_SENTINEL' in s for s in kernel.executed)


@pytest.mark.parametrize('value', ["quotes ' and \" and newline\n", 'x' * 500, 'Exception: text'])
def test_native_return_is_used_without_substitution_or_truncation(monkeypatch, tmp_path, value):
    original = harness._ExitKernel.execute
    async def execute(self, code, **kwargs):
        self.namespace = getattr(self, 'namespace', {})
        if code.startswith('result ='):
            def run(path, timeout):
                assert json.loads((tmp_path / 'leaf.ipynb').read_text())['cells']
                self.native_calls = getattr(self, 'native_calls', 0) + 1
                return value
            self.namespace['aidputils'] = SimpleNamespace(notebook=SimpleNamespace(run=run))
            exec(code, self.namespace)
        if 'notebook.exit("RETURN_FIXTURE")' in code:
            self.executed.append(code)
            return {'status': 'ok', 'outputs': [
                {'type': 'stream', 'text': '__AIDP_NOTEBOOK_EXIT__ value=' + repr(value) + '\n'}]}
        return await original(self, code, **kwargs)
    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    source = 'result = aidputils.notebook.run("/Workspace/leaf", 60)'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['aidputils.notebook.exit("RETURN_FIXTURE")', 'MUST_NOT_EXECUTE = 1']})
    assert result['status'] == 'ok', result
    assert kernel.namespace['result'] == value
    assert kernel.native_calls == 1
    assert any(code.startswith('result = aidputils.notebook.run(') for code in kernel.executed)
    assert saved['child'] == [source]
    assert not any('MUST_NOT_EXECUTE' in s for s in kernel.executed if 'base64' not in s)


@pytest.mark.parametrize('decoy', [
    '',
    '// oidlUtils.notebook.exit("unused")',
    '// dbutils.notebook.exit("unused")',
    'if (false) { oidlUtils.notebook.exit("unused") }',
    'val description = "oidlUtils.notebook.exit(unused)"',
])
def test_scala_child_exit_is_not_rewritten_for_return_capture(monkeypatch, tmp_path, decoy):
    source = '%scala\noidlUtils.notebook.run("/Workspace/leaf", 60)\nprintln("after")'
    exit_source = '%scala\n' + (decoy + '\n' if decoy else '') + 'oidlUtils.notebook.exit(nextValue())'
    original = harness._ExitKernel.execute

    async def execute(self, code, **kwargs):
        if 'notebook.exit(nextValue())' in code:
            self.executed.append(code)
            return {'status': 'error', 'outputs': [
                {'type': 'error', 'ename': 'NotebookExit', 'evalue': 'NotebookExit: fixture', 'traceback': ['NotebookExit: fixture']}]}
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': [exit_source, 'MUST_NOT_EXECUTE = 1']})
    assert result['status'] == 'ok', result
    assert exit_source in kernel.executed
    assert saved['leaf'][0] == exit_source
    assert saved['child'] == [source]
    assert any('oidlUtils.notebook.run(' in code for code in kernel.executed)
    assert not any('MUST_NOT_EXECUTE' in code for code in kernel.executed
                   if 'base64' not in code)


def test_repaired_child_is_saved_before_native_dispatch(monkeypatch, tmp_path):
    original = harness._ExitKernel.execute

    async def repair(code, *args, **kwargs):
        return 'REPAIRED_LEAF = 1' if code == 'BROKEN_LEAF' else code

    async def execute(self, code, **kwargs):
        if code == 'BROKEN_LEAF':
            self.executed.append(code)
            return {'status': 'error', 'outputs': [
                {'type': 'error', 'traceback': ['NameError: BROKEN_LEAF']}]}
        if code.startswith('aidputils.notebook.run('):
            child = json.loads((tmp_path / 'leaf.ipynb').read_text())
            assert ''.join(child['cells'][0]['source']) == 'REPAIRED_LEAF = 1'
            self.native_saw_repair = True
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness.jm, 'call_fix', repair)
    monkeypatch.setattr(harness.jm, 'MAX_FIX_ATTEMPTS', 2)
    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    source = 'aidputils.notebook.run("/Workspace/leaf", 60)\nAFTER_SENTINEL = 1'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['BROKEN_LEAF']})
    assert result['status'] == 'ok', result
    assert kernel.native_saw_repair
    assert saved['child'] == [source]
    assert saved['leaf'] == ['REPAIRED_LEAF = 1']


@pytest.mark.parametrize("repair", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_dynamic_target_mismatch_requires_durable_repair(monkeypatch, tmp_path, repair, strict):
    original_execute = harness._ExitKernel.execute
    feedback = []
    migrated = str(tmp_path / "leaf.ipynb")
    source = ('target = "/Workspace/leaf"\n'
              'result = aidputils.notebook.run(target, 60)\n'
              'observed = result')

    def native_run(path, timeout):
        return "MIGRATED" if path == migrated else "ORIGINAL"

    async def execute(self, code, **kwargs):
        self.namespace = getattr(self, "namespace", {})
        self.namespace["aidputils"] = SimpleNamespace(
            notebook=SimpleNamespace(run=native_run))
        if code.startswith(("target =", "result =", "observed =")):
            exec(code, self.namespace)
        return await original_execute(self, code, **kwargs)

    async def fix(code, output, *args, **kwargs):
        feedback.append(output)
        return code.replace('"/Workspace/leaf"', repr(migrated)) if repair else code

    monkeypatch.setattr(harness._ExitKernel, "execute", execute)
    monkeypatch.setattr(harness.jm, "call_fix", fix)
    monkeypatch.setattr(harness.jm, "MAX_FIX_ATTEMPTS", 2)
    result, saved, kernel, _, _ = harness._run(
        monkeypatch, tmp_path, {"child": [source], "leaf": ["MIGRATED_LEAF = 1"]},
        strict=strict, real_paths=True)
    assert feedback, "Target mismatch must reach the existing model repair loop"
    assert "/Workspace/leaf" in feedback[0] and migrated in feedback[0]
    if repair:
        assert result["status"] == "ok", result
        delivered = {}
        delivered["aidputils"] = SimpleNamespace(notebook=SimpleNamespace(run=native_run))
        exec(saved["child"][0], delivered)
        assert delivered["observed"] == kernel.namespace["observed"] == "MIGRATED"
        assert "notebook.run(target, 60)" in saved["child"][0]
    else:
        assert result["status"] == "error", result
        assert saved["child"] == [source]
        assert "observed" not in kernel.namespace
        assert not any("MIGRATED_LEAF" in c for c in kernel.executed)


@pytest.mark.parametrize('language', ['python', 'scala'])
@pytest.mark.parametrize('path_form', ['absolute', 'relative', 'extensionless', 'original', 'spaces'])
def test_native_target_identity_through_child_loop(monkeypatch, tmp_path, language, path_form):
    migrated = str(tmp_path / 'leaf.ipynb')
    target = {
        'absolute': migrated,
        'relative': './leaf.ipynb',
        'extensionless': migrated.removesuffix('.ipynb'),
        'original': '/Workspace/leaf',
        'spaces': str(tmp_path / 'leaf name.ipynb'),
    }[path_form]
    matched = path_form in ('absolute', 'relative', 'extensionless')
    source = f'%{language}\naidputils.notebook.run({json.dumps(target)}, 60)\n' + (
        'val AFTER_SENTINEL = 1' if language == 'scala' else 'AFTER_SENTINEL = 1')
    # The real resolver sanitizes the space to an underscore; these are distinct
    # runtime paths even if discovery maps them to the same dependency.
    result, saved, kernel, _, _ = harness._run(
        monkeypatch, tmp_path,
        {'child': [source], 'leaf': ['LEAF_SENTINEL = 1'],
         'leaf_name': ['LEAF_SENTINEL = 1']}, real_paths=True)
    assert (result['status'] == 'ok') is matched, result
    assert saved['child'] == [source]
    assert any('AFTER_SENTINEL' in c for c in kernel.executed if 'base64' not in c) is matched
    assert any('LEAF_SENTINEL' in c for c in kernel.executed) is matched


@pytest.mark.parametrize('shape', ['single', 'same_cell', 'separate_cells', 'nested'])
@pytest.mark.parametrize('start_inside_child', [False, True])
@pytest.mark.parametrize('supplied_code', ['omitted', 'unchanged', 'trailing_whitespace'])
def test_full_fixup_replays_each_invocation_without_overlapping_children(
        monkeypatch, tmp_path, shape, start_inside_child, supplied_code):
    import asyncio
    import agent_migrate as am

    original = harness._ExitKernel.execute
    rows = []

    async def execute(self, code, **kwargs):
        self.namespace = getattr(self, 'namespace', {})
        self.namespace['rows'] = rows
        def run(path, timeout):
            rows.append('native:' + path.rsplit('/', 1)[-1])
            return 'done'
        self.namespace['aidputils'] = SimpleNamespace(notebook=SimpleNamespace(run=run))
        if code.startswith(('rows.', 'aidputils.notebook.run(')):
            exec(code, self.namespace)
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    call = 'aidputils.notebook.run("/Workspace/leaf", 60)'
    calls = [call]
    if shape == 'same_cell':
        calls = [call + '\n' + call]
    elif shape == 'separate_cells':
        calls = [call, call]
    elif shape == 'nested':
        calls = ['aidputils.notebook.run("/Workspace/middle", 60)']
    result, _, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': ['rows.clear()', *calls],
        'middle': [call], 'leaf': ['rows.append("prepared")']})
    assert result['status'] == 'ok', result
    expected = list(rows)
    assert expected.count('prepared') == (2 if shape in ('same_cell', 'separate_cells') else 1)
    monkeypatch.setattr(am, '_cell_history', harness.jm._cell_history)
    monkeypatch.setattr(am, '_replay_cell_fn', harness.jm._replay_cell_entry)
    # An overlapping repair must not be silently discarded by replay planning.
    history_before = list(am._cell_history)
    leaf_index = next(i for i, e in enumerate(am._cell_history)
                      if e['notebook_path'].endswith('/leaf.ipynb'))
    rejected = asyncio.run(am._handle_tool_call(
        'fixup_cell', {'start_index': leaf_index, 'fixed_code': 'rows.append("changed")'},
        session=kernel))
    assert rejected.startswith('ERROR:'), rejected
    assert am._cell_history == history_before
    assert rows == expected
    # An existing unsaved edit must still block even if this request is unchanged.
    leaf_entry = am._cell_history[leaf_index]
    leaf_entry['_force_persist'] = True
    pending = asyncio.run(am._handle_tool_call(
        'fixup_cell', {'start_index': leaf_index, 'fixed_code': leaf_entry['replay_code']},
        session=kernel))
    assert pending.startswith('ERROR:'), pending
    assert am._cell_history == history_before
    assert rows == expected
    leaf_entry.pop('_force_persist')
    for _ in range(2):
        start = 0
        if start_inside_child:
            start = next(i for i, e in enumerate(am._cell_history)
                         if e['notebook_path'].endswith('/leaf.ipynb'))
            rows.clear()
        request = {'start_index': start, 'why': 'rebuild reset then append'}
        if supplied_code != 'omitted':
            request['fixed_code'] = am._cell_history[start]['replay_code']
            if supplied_code == 'trailing_whitespace':
                request['fixed_code'] += '\n  '
        reply = asyncio.run(am._handle_tool_call('fixup_cell', request, session=kernel))
        assert reply.startswith('OK:'), reply
        assert rows == expected, (shape, rows, expected, reply)


@pytest.mark.parametrize('child_language', ['python', 'scala'])
@pytest.mark.parametrize('value', [
    '</report>', '', 'quotes " and apostrophe \'', '\\path\\file',
    'first\nsecond\tcolumn', 'é 日本語 😀', '<\\/report>', 'x' * 500,
])
def test_native_return_preserves_exact_value_across_child_languages(
        monkeypatch, tmp_path, child_language, value):
    original = harness._ExitKernel.execute
    api = 'oidlUtils' if child_language == 'scala' else 'aidputils'
    leaf = ('%scala\n' if child_language == 'scala' else '') + (
        api + '.notebook.exit(' + json.dumps(value) + ')')
    source = ('result = aidputils.notebook.run("/Workspace/leaf", 60)\n'
              'observed = result')

    async def execute(self, code, **kwargs):
        self.namespace = getattr(self, 'namespace', {})
        def run(path, timeout):
            self.native_calls = getattr(self, 'native_calls', 0) + 1
            return value
        self.namespace['aidputils'] = SimpleNamespace(notebook=SimpleNamespace(run=run))
        if code.startswith(('result =', 'observed =')):
            exec(code, self.namespace)
        if '.notebook.exit(' in code and 'base64' not in code:
            # The protocol response is simulated; the Scala/JVM is not running
            # in this harness. Assert the tool submits unchanged exit source.
            assert code == leaf
            self.executed.append(code)
            if child_language == 'scala':
                return {'status': 'error', 'outputs': [{
                    'type': 'error', 'ename': 'NotebookExit',
                    'evalue': 'NotebookExit: native return', 'traceback': ['NotebookExit']}]}
            return {'status': 'ok', 'outputs': [{
                'type': 'stream', 'text': '__AIDP_NOTEBOOK_EXIT__ value=' + repr(value)}]}
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': [leaf, 'MUST_NOT_EXECUTE = 1']})
    assert result['status'] == 'ok', result
    assert kernel.namespace['observed'] == value
    assert kernel.native_calls == 1
    assert saved['child'] == [source]
    assert saved['leaf'][0] == leaf
    assert not any('MUST_NOT_EXECUTE' in c for c in kernel.executed if 'base64' not in c)


@pytest.mark.parametrize("language", ["python", "scala"])
@pytest.mark.parametrize("response", ["duplicate", "wrapped_duplicate", "conflicting", "missing"])
def test_repeated_call_argument_probe_transport(monkeypatch, tmp_path, language, response):
    """Live AIDP repeats the second probe's stdout after a native call."""
    original = harness._ExitKernel.execute

    async def execute(self, code, **kwargs):
        result = await original(self, code, **kwargs)
        if "__AIDP_NESTED_RUN_ARGS__" not in code:
            return result
        self.probes = getattr(self, "probes", 0) + 1
        if self.probes != 2:
            return result
        text = result["outputs"][0]["text"].rstrip("\n") + "\n"
        if response == "wrapped_duplicate":
            text = json.dumps([{"type": "TEXT_PLAIN", "value": text}])
        second = text
        if response == "conflicting":
            second = text.replace("leaf", "other")
        result["outputs"] = ([{"type": "stream", "text": "[]"}] * 6 + [
            {"type": "stream", "text": text},
            {"type": "stream", "text": second}]) if response != "missing" else []
        return result

    monkeypatch.setattr(harness._ExitKernel, "execute", execute)
    source = (f"%{language}\n"
              'dbutils.notebook.run("/Workspace/leaf", 60)\n'
              + ("val " if language == "scala" else "") + 'BETWEEN_SENTINEL = 1\n'
              'dbutils.notebook.run("/Workspace/leaf", 60)\n'
              + ("val " if language == "scala" else "") + 'AFTER_SENTINEL = 1')
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        "child": [source], "leaf": ["LEAF_SENTINEL = 1"]})
    accepted = response in ("duplicate", "wrapped_duplicate")
    assert result["status"] == ("ok" if accepted else "error"), result
    assert kernel.probes == 2
    assert saved["child"] == [source]
    native = [code for code in kernel.executed if 'dbutils.notebook.run(' in code]
    assert len(native) == (2 if accepted else 1)
    assert any("BETWEEN_SENTINEL" in code for code in kernel.executed)
    assert any("AFTER_SENTINEL" in code for code in kernel.executed) == accepted


@pytest.mark.parametrize('include_arguments', [False, True])
@pytest.mark.parametrize('native_failure', [False, True])
def test_invocation_values_evaluate_once_and_survive_preparation(
        monkeypatch, tmp_path, include_arguments, native_failure):
    import asyncio
    import agent_migrate as am

    original = harness._ExitKernel.execute
    events, native_calls = [], []
    argument_map = {}
    path_calls = []

    def next_path():
        path = str(tmp_path / ('first.ipynb' if len(path_calls) % 2 == 0 else 'second.ipynb'))
        path_calls.append(path)
        events.append('path')
        return path

    def next_timeout():
        events.append('timeout')
        return 60 + len(path_calls)

    def next_arguments():
        events.append('arguments')
        argument_map['arg'] = 'value-' + str(len(path_calls))
        return argument_map

    def run(path, timeout, *arguments):
        native_calls.append((path, timeout, arguments))
        if native_failure:
            raise RuntimeError('native failure fixture')
        return 'native-return'

    async def execute(self, code, **kwargs):
        self.namespace = getattr(self, 'namespace', {})
        self.namespace.update(next_path=next_path, next_timeout=next_timeout,
                              next_arguments=next_arguments, argument_map=argument_map,
                              aidputils=SimpleNamespace(notebook=SimpleNamespace(run=run)))
        if code.startswith(('result =', 'observed =', 'argument_map[')):
            exec(code, self.namespace)
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    args = 'next_path(), next_timeout()' + (', next_arguments()' if include_arguments else '')
    source = 'result = aidputils.notebook.run(' + args + ')\nobserved = result'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source],
        'first': ['argument_map["arg"] = "changed during preparation"'],
        'second': ['argument_map["arg"] = "changed during preparation"'],
    }, real_paths=True, strict=True)
    assert result['status'] == ('error' if native_failure else 'ok'), result
    expected_events = ['path', 'timeout'] + (['arguments'] if include_arguments else [])
    assert events == expected_events
    assert native_calls == [(str(tmp_path / 'first.ipynb'), 61,
                             ({'arg': 'value-1'},) if include_arguments else ())]
    assert saved['child'] == [source]
    assert not any(k.startswith('_aidp_run_args_') for k in kernel.namespace)
    assert not any('value-1' in code for code in kernel.executed
                   if code.startswith('result = aidputils.notebook.run('))
    if native_failure:
        assert 'observed' not in kernel.namespace
        return
    assert kernel.namespace['observed'] == 'native-return'

    # Each rewind evaluates the original expressions once again. Frozen values
    # belong to one execution and must never become saved or replay source.
    monkeypatch.setattr(am, '_cell_history', harness.jm._cell_history)
    monkeypatch.setattr(am, '_replay_cell_fn', harness.jm._replay_cell_entry)
    for number in (2, 3):
        reply = asyncio.run(am._handle_tool_call(
            'fixup_cell', {'start_index': 0}, session=kernel))
        assert reply.startswith('OK:'), reply
        assert events == expected_events * number
        assert not any(k.startswith('_aidp_run_args_') for k in kernel.namespace)
        expected_path = tmp_path / ('second.ipynb' if number == 2 else 'first.ipynb')
        assert native_calls[-1] == (str(expected_path), 60 + number,
                                   ({'arg': 'value-' + str(number)},) if include_arguments else ())
        assert any(entry.get('replay_code') == source for entry in harness.jm._cell_history)
        assert json.loads((tmp_path / 'child.ipynb').read_text())['cells'][0]['source'] == [source]


@pytest.mark.parametrize('failure', ['probe', 'target', 'child', 'cleanup'])
def test_captured_arguments_cleaned_on_failure(monkeypatch, tmp_path, failure):
    original = harness._ExitKernel.execute
    cleanup_calls, native_calls = [], []

    async def execute(self, code, **kwargs):
        if code.startswith("globals().pop('_aidp_run_args_"):
            cleanup_calls.append(code)
            result = await original(self, code, **kwargs)
            if failure == 'cleanup':
                return {'status': 'error', 'outputs': [{'type': 'error',
                        'ename': 'CleanupFailure', 'evalue': 'fixture', 'traceback': []}]}
            return result
        if code.startswith('aidputils.notebook.run('):
            native_calls.append(code)
        if code.startswith('FAIL_SENTINEL'):
            return {'status': 'error', 'outputs': [{'type': 'error',
                    'ename': 'ChildFailure', 'evalue': 'fixture', 'traceback': []}]}
        result = await original(self, code, **kwargs)
        if '__AIDP_NESTED_RUN_ARGS__' in code and failure == 'probe':
            # Capture succeeded, but the response could not be read back.
            return {'status': 'ok', 'outputs': []}
        return result

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    target = '/Workspace/leaf' if failure == 'target' else str(tmp_path / 'leaf.ipynb')
    source = f'aidputils.notebook.run({json.dumps(target)}, 60)\nAFTER_SENTINEL = 1'
    result, saved, kernel, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source],
        'leaf': [('FAIL_SENTINEL' if failure == 'child' else 'LEAF_SENTINEL') + ' = 1'],
    }, real_paths=True, strict=True)
    assert result['status'] == 'error', result
    assert len(cleanup_calls) == 1
    assert not any(k.startswith('_aidp_run_args_') for k in kernel.namespace)
    assert len(native_calls) == (1 if failure == 'cleanup' else 0)
    assert not any('AFTER_SENTINEL' in code for code in kernel.executed if 'base64' not in code)
    assert saved['child'] == [source]


@pytest.mark.parametrize('with_arguments', [False, True])
@pytest.mark.parametrize('native_failure', [False, True])
def test_scala_native_dispatch_uses_and_clears_captured_invocation(
        monkeypatch, tmp_path, with_arguments, native_failure):
    import re
    original = harness._ExitKernel.execute
    probes, native, cleanup, declarations = [], [], [], []
    target = str(tmp_path / 'leaf.ipynb')

    async def execute(self, code, **kwargs):
        if re.fullmatch(r'%scala\nvar _aidp_run_args_[0-9a-f]+: '
                        r'\(String, Int, Map\[String, String\]\) = null', code):
            declarations.append(code)
        if '__AIDP_NESTED_RUN_ARGS__' in code:
            probes.append(code)
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                '__AIDP_NESTED_RUN_ARGS__' + json.dumps({
                    'path': target, 'arguments': {'arg': 'runtime-value'} if with_arguments else {}}) + '\n'}]}
        if code.startswith('%scala\nval result ='):
            native.append(code)
            if native_failure:
                return {'status': 'error', 'outputs': [{'type': 'error',
                        'ename': 'NativeFailure', 'evalue': 'fixture', 'traceback': []}]}
        if re.fullmatch(r'%scala\n_aidp_run_args_[0-9a-f]+ = null', code):
            cleanup.append(code)
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    expressions = 'nextTarget(), nextTimeout()' + (', nextArguments()' if with_arguments else '')
    source = '%scala\nval result = oidlUtils.notebook.run(' + expressions + ')'
    result, saved, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['%scala\nval LEAF_SENTINEL = 1']},
        real_paths=True, strict=True)
    assert result['status'] == ('error' if native_failure else 'ok'), result
    assert len(declarations) == len(probes) == len(native) == len(cleanup) == 1
    binding = re.search(r'var (_aidp_run_args_[0-9a-f]+):', declarations[0])[1]
    # Declared separately, so a failed evaluation still leaves cleanup valid.
    assert probes[0].startswith('%scala\n' + binding + ' = ((target: String')
    for expression in ('nextTarget()', 'nextTimeout()', *(['nextArguments()'] if with_arguments else [])):
        assert probes[0].count(expression) == 1
        assert expression not in native[0]
    expected = ', '.join(['"/Workspace/leaf"', *[f'{binding}._{i}' for i in range(2, 4 if with_arguments else 3)]])
    assert native[0] == '%scala\nval result = oidlUtils.notebook.run(' + expected + ')'
    assert cleanup[0] == '%scala\n' + binding + ' = null'
    assert 'runtime-value' not in native[0]
    assert saved['child'] == [source]


@pytest.mark.parametrize('language', ['python', 'scala'])
@pytest.mark.parametrize('parent', ['%run /Workspace/child', 'dbutils.notebook.run("/Workspace/child", 60)'])
def test_double_execution_disclosure_reaches_migration_report(monkeypatch, tmp_path, language, parent):
    source = f'%{language}\ndbutils.notebook.run("/Workspace/leaf", 60)'
    result, saved, kernel, _, _ = harness._run(
        monkeypatch, tmp_path, {'child': [source, source], 'leaf': ['LEAF_SENTINEL = 1']},
        parent=[parent])
    assert result['status'] == 'PASS', result
    report = next(text for path, text in kernel.files.items() if path.endswith('migration_report.md'))
    for cell in (0, 1):
        assert f'child.ipynb cell {cell}: child' in report
    assert 'executed twice during migration' in report
    assert saved['child'] == [source, source]


def _scala_failure(command, error):
    """AIDP's reply to a failed %scala command (runs c586eb65 2026-10-19 and
    2026-10-20, spend_category_tickets_automation): status=ok, empty chunks,
    then the failing command echoed verbatim (all lines) and the error."""
    return [{'type': 'stream', 'text': '[]'}] * 6 + [{'type': 'stream', 'text':
            'Command ID aaaaaaaa-0000-4000-8000-000000000201 failed with '
            'java.lang.RuntimeException: java.util.concurrent.ExecutionException: '
            f'java.lang.Exception: Failing command: {command} : {error}\n'}]


def _scala_repl(monkeypatch, *, probe_fails, cleanup_failure=None, printed=''):
    """Model the Scala REPL contract: a failed definition binds nothing, and
    assigning to an unbound name is itself a (status=ok, stream-only) failure.
    cleanup_failure injects a cleanup that replies with a failure ('returned')
    or whose transport raises ('raised', as AIDPSession.execute does). printed
    is stdout the argument expression writes before it fails."""
    import re
    from websocket import WebSocketTimeoutException
    original = harness._ExitKernel.execute
    bound, calls = set(), []

    async def execute(self, code, **kwargs):
        if not code.startswith('%scala\n'):
            return await original(self, code, **kwargs)
        body = code[len('%scala\n'):]
        declared = re.fullmatch(
            r'var (_aidp_run_args_[0-9a-f]+): \(String, Int, Map\[String, String\]\) = null', body)
        if declared:
            calls.append('declare')
            bound.add(declared[1])
            return {'status': 'ok', 'outputs': []}
        if '__AIDP_NESTED_RUN_ARGS__' in body:
            calls.append('probe')
            if probe_fails:
                # The echo includes the probe's own println(marker + ...) line.
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': printed}] * bool(printed)
                        + _scala_failure(body,
                        'org.apache.spark.sql.AnalysisException: [PATH_NOT_FOUND] Path does not exist: '
                        'oci://fixture-bucket@fixture_namespace/missing')}
            bound.add(re.match(r'(?:var )?(_aidp_run_args_[0-9a-f]+)', body)[1])
            return await original(self, code, **kwargs)
        cleared = re.fullmatch(r'(_aidp_run_args_[0-9a-f]+) = null', body)
        if cleared:
            calls.append('cleanup')
            if cleanup_failure == 'raised':
                raise WebSocketTimeoutException('')
            if cleanup_failure == 'returned':
                return {'status': 'ok', 'outputs': _scala_failure(
                        body, 'java.lang.IllegalStateException: fixture cleanup failure')}
            if cleared[1] not in bound:
                return {'status': 'ok', 'outputs': _scala_failure(
                        body, f'error: not found: value {cleared[1]}')}
            return {'status': 'ok', 'outputs': []}
        if 'oidlUtils.notebook.run(' in body:
            calls.append('native')
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    return calls


@pytest.mark.parametrize('language', ['python', 'scala'])
@pytest.mark.parametrize('strict', [False, True])
def test_failed_argument_evaluation_reaches_fix_loop_with_kernel_error(
        monkeypatch, tmp_path, language, strict):
    """A caller's argument expression failing is a cell failure the model repairs,
    never an UNRESOLVED dispatch that hides the kernel error."""
    seen = []

    async def record_fix(code, output, *args, **kwargs):
        seen.append(output)
        return code

    monkeypatch.setattr(harness.jm, 'call_fix', record_fix)
    # Two attempts: the model gets exactly one repair turn on the failure.
    monkeypatch.setattr(harness.jm, 'MAX_FIX_ATTEMPTS', 2)
    if language == 'scala':
        calls = _scala_repl(monkeypatch, probe_fails=True)
        source = '%scala\noidlUtils.notebook.run("/Workspace/leaf", 60, Map("count" -> finDf.count.toString))'
        leaf = '%scala\nval LEAF_SENTINEL = 1'
    else:
        original = harness._ExitKernel.execute
        calls = []

        async def execute(self, code, **kwargs):
            if '__AIDP_NESTED_RUN_ARGS__' in code:
                calls.append('probe')
                return {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'AnalysisException',
                        'evalue': '[PATH_NOT_FOUND] Path does not exist', 'traceback': [
                            'AnalysisException: [PATH_NOT_FOUND] Path does not exist']}]}
            if code.startswith("globals().pop('_aidp_run_args_"):
                calls.append('cleanup')
            if code.startswith('aidputils.notebook.run('):
                calls.append('native')
            return await original(self, code, **kwargs)

        monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
        source = 'aidputils.notebook.run("/Workspace/leaf", 60, {"count": str(finDf.count())})'
        leaf = 'LEAF_SENTINEL = 1'
    result, saved, kernel, _, logs = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': [leaf]}, strict=strict)
    assert result['status'] == 'error', result
    assert result.get('kind') != 'UNRESOLVED_NOTEBOOK_RUN', result
    assert len(seen) == 1 and 'PATH_NOT_FOUND' in seen[0], seen
    assert any('PATH_NOT_FOUND' in message for message in logs)
    assert 'native' not in calls
    assert calls.count('probe') == calls.count('cleanup') == 2
    assert not any('LEAF_SENTINEL' in code for code in kernel.executed)
    assert saved['child'] == [source]


def test_scala_cleanup_failure_after_unbound_capture_is_not_reported_as_cleanup(monkeypatch, tmp_path):
    """The Scala binding is declared before argument evaluation, so cleanup is valid
    even when evaluation fails and never replaces the evaluation error."""
    calls = _scala_repl(monkeypatch, probe_fails=True)
    source = '%scala\noidlUtils.notebook.run("/Workspace/leaf", 60, Map("count" -> finDf.count.toString))'
    result, _, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['%scala\nval LEAF_SENTINEL = 1']}, strict=True)
    assert 'Could not clear captured nested notebook arguments' not in str(result.get('error')), result
    assert calls[:3] == ['declare', 'probe', 'cleanup'], calls


@pytest.mark.parametrize('entries', [1, 60])
@pytest.mark.parametrize('cleanup_failure,cleanup_detail', [
    ('returned', 'fixture cleanup failure'),
    ('raised', 'WebSocketTimeoutException'),
])
def test_scala_argument_failure_survives_cleanup_failure(
        monkeypatch, tmp_path, cleanup_failure, cleanup_detail, entries):
    """When both evaluation and cleanup fail, the report keeps the evaluation's
    kernel error. AIDP appends that error to the echoed println(marker ...) line,
    after the echoed probe source (long for a large argument map)."""
    calls = _scala_repl(monkeypatch, probe_fails=True, cleanup_failure=cleanup_failure)
    pairs = ", ".join([f'"key{i}" -> "value{i}"' for i in range(entries - 1)] + ['"count" -> finDf.count.toString'])
    source = f'%scala\noidlUtils.notebook.run("/Workspace/leaf", 60, Map({pairs}))'
    result, _, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['%scala\nval LEAF_SENTINEL = 1']}, strict=True)
    assert result.get('kind') == 'UNRESOLVED_NOTEBOOK_RUN', result
    assert '[PATH_NOT_FOUND] Path does not exist' in result['error'], result['error']
    assert 'Could not clear captured nested notebook arguments' in result['error']
    assert cleanup_detail in result['error'], result['error']
    assert calls == ['declare', 'probe', 'cleanup'], calls


_LONG_OUTPUT = 'row | ok\n' * 700  # printed before the failure; longer than the quote


@pytest.mark.parametrize('cleanup_failure,cleanup_detail', [
    ('returned', 'fixture cleanup failure'),
    ('raised', 'WebSocketTimeoutException'),
])
@pytest.mark.parametrize('printed_by', ['argument', 'cell'])
@pytest.mark.parametrize('language', ['python', 'scala'])
def test_argument_failure_survives_cleanup_failure_after_long_output(
        monkeypatch, tmp_path, language, printed_by, cleanup_failure, cleanup_detail):
    """The kernel reports a failure after everything printed before it, by the
    cell's own code before the call or by the argument expression, so the report
    keeps the end of the reply when evaluation and cleanup both fail."""
    import re
    from websocket import WebSocketTimeoutException
    if language == 'scala':
        _scala_repl(monkeypatch, probe_fails=True, cleanup_failure=cleanup_failure,
                    printed=_LONG_OUTPUT if printed_by == 'argument' else '')
        source = '%scala\noidlUtils.notebook.run("/Workspace/leaf", 60, Map("count" -> finDf.count.toString))'
        leaf = '%scala\nval LEAF_SENTINEL = 1'
    else:
        original = harness._ExitKernel.execute

        async def execute(self, code, **kwargs):
            if '__AIDP_NESTED_RUN_ARGS__' in code:
                printed = [{'type': 'stream', 'text': _LONG_OUTPUT}] * (printed_by == 'argument')
                return {'status': 'error', 'outputs': printed + [{'type': 'error',
                        'ename': 'AnalysisException', 'evalue': '[PATH_NOT_FOUND] Path does not exist',
                        'traceback': []}]}
            if code.startswith("globals().pop('_aidp_run_args_"):
                if cleanup_failure == 'raised':
                    raise WebSocketTimeoutException('')
                return {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'CleanupFailure',
                        'evalue': 'fixture cleanup failure', 'traceback': []}]}
            return await original(self, code, **kwargs)

        monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
        source = 'aidputils.notebook.run("/Workspace/leaf", 60, {"count": str(finDf.count())})'
        leaf = 'LEAF_SENTINEL = 1'
    if printed_by == 'cell':
        kernel_execute = harness._ExitKernel.execute

        async def print_first(self, code, **kwargs):
            if re.match(r'(%scala\n)?print(ln)?\(PRINT_SENTINEL\)', code):
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': _LONG_OUTPUT}]}
            return await kernel_execute(self, code, **kwargs)

        monkeypatch.setattr(harness._ExitKernel, 'execute', print_first)
        head, call = source.split('\n') if language == 'scala' else ('', source)
        printer = 'println(PRINT_SENTINEL)' if language == 'scala' else 'print(PRINT_SENTINEL)'
        source = '\n'.join(filter(None, [head, printer, call]))
    result, _, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': [leaf]}, strict=True)
    assert result.get('kind') == 'UNRESOLVED_NOTEBOOK_RUN', result
    assert '[PATH_NOT_FOUND] Path does not exist' in result['error'], result['error'][-400:]
    assert 'Could not clear captured nested notebook arguments' in result['error']
    assert cleanup_detail in result['error'], result['error']
    # The quote stays bounded: it keeps the end of the reply, not all of it.
    assert len(result['error']) < len(_LONG_OUTPUT), len(result['error'])


@pytest.mark.parametrize('response,expected', [
    ({'status': 'ok', 'outputs': [{'type': 'stream', 'text': 'unexpected kernel reply\n'}]},
     'unexpected kernel reply'),
    ({'status': 'ok', 'outputs': [{'type': 'stream', 'text':
        '__AIDP_NESTED_RUN_ARGS__{"path": "/Workspace/leaf", "arguments": {"k": "value-A"}}\n'
        '__AIDP_NESTED_RUN_ARGS__{"path": "/Workspace/leaf", "arguments": {"k": "value-B"}}\n'}]},
     '2 argument payloads'),
])
def test_unreadable_argument_capture_reports_kernel_reply_without_values(
        monkeypatch, tmp_path, response, expected):
    original = harness._ExitKernel.execute

    async def execute(self, code, **kwargs):
        if '__AIDP_NESTED_RUN_ARGS__' in code:
            return response
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    source = 'aidputils.notebook.run("/Workspace/leaf", 60)'
    result, _, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source], 'leaf': ['LEAF_SENTINEL = 1']}, strict=True)
    assert result.get('kind') == 'UNRESOLVED_NOTEBOOK_RUN', result
    assert 'Could not read evaluated nested notebook arguments' in result['error']
    assert expected in result['error'], result['error']
    assert 'value-A' not in result['error'] and 'value-B' not in result['error']


@pytest.mark.parametrize('cleanup,cleanup_detail', [
    ('returned', 'fixture-cleanup-reply'),
    ('raised', 'WebSocketTimeoutException'),
])
@pytest.mark.parametrize('primary,expected', [
    ('none', None),
    ('unreadable', 'Could not read evaluated nested notebook arguments'),
    ('child', 'failed; parent cell not executed'),
    ('target', 'NOTEBOOK_RUN_TARGET_MISMATCH'),
])
def test_cleanup_failure_keeps_primary_outcome_and_kernel_reply(
        monkeypatch, tmp_path, primary, expected, cleanup, cleanup_detail):
    """A cleanup failure (a failed reply or a raised transport error) is surfaced
    and never discards the outcome (raised or returned) in flight when cleanup ran."""
    from websocket import WebSocketTimeoutException
    original = harness._ExitKernel.execute

    async def execute(self, code, **kwargs):
        if code.startswith("globals().pop('_aidp_run_args_"):
            if cleanup == 'raised':
                raise WebSocketTimeoutException('')
            await original(self, code, **kwargs)
            return {'status': 'error', 'outputs': [{'type': 'error',
                    'ename': 'CleanupFailure', 'evalue': 'fixture-cleanup-reply', 'traceback': []}]}
        if '__AIDP_NESTED_RUN_ARGS__' in code and primary == 'unreadable':
            await original(self, code, **kwargs)
            return {'status': 'ok', 'outputs': []}
        if code.startswith('FAIL_SENTINEL'):
            return {'status': 'error', 'outputs': [{'type': 'error',
                    'ename': 'ChildFailure', 'evalue': 'fixture', 'traceback': []}]}
        return await original(self, code, **kwargs)

    monkeypatch.setattr(harness._ExitKernel, 'execute', execute)
    target = '/Workspace/leaf' if primary == 'target' else str(tmp_path / 'leaf.ipynb')
    source = f'aidputils.notebook.run({json.dumps(target)}, 60)'
    result, _, _, _, _ = harness._run(monkeypatch, tmp_path, {
        'child': [source],
        'leaf': [('FAIL_SENTINEL' if primary == 'child' else 'LEAF_SENTINEL') + ' = 1'],
    }, real_paths=True, strict=True)
    assert result.get('kind') == 'UNRESOLVED_NOTEBOOK_RUN', result
    assert 'Could not clear captured nested notebook arguments' in result['error']
    assert cleanup_detail in result['error'], result['error']
    if expected:
        assert expected in result['error'], result['error']
