"""Forecasting Sep 13/14: execute the guarded producer/consumer sequence.

No Spark, OCI, or production filesystem calls: the fake fs enforces the same
directory-vs-file distinction that caused the live cleanup failure.
"""
import sys
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def _declare_test_sandbox(monkeypatch, tmp_path):
    """These tests exercise fs-shim behaviour, not the sandbox gate: the
    notebook_policy runtime assertions (fail-closed since the policy gate)
    need a declared sandbox before any dbutils.fs / write helper runs."""
    monkeypatch.setenv("AIDP_SANDBOX_CATALOG", "default")
    monkeypatch.setenv("AIDP_SANDBOX_SCHEMA", "sbx")
    monkeypatch.setenv("AIDP_SANDBOX_PREFIX", "oci://b@ns/,oci://sandbox@testns/,/")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm

SOURCE = "oci://source@tenant/reports/tmp"
TARGET = "oci://sandbox@testns/source/reports/tmp"


@pytest.fixture(autouse=True)
def configuration(monkeypatch):
    import context_tools
    monkeypatch.setattr(context_tools, '_BUCKET_NS_INDEX', {'source': 'tenant', 'sandbox': 'testns'})
    monkeypatch.setattr(jm, "_REDIRECT_BUCKET", "sandbox")
    monkeypatch.setattr(jm, "_REDIRECT_NAMESPACE", "testns")
    jm.clear_write_redirect_map()
    yield
    jm.clear_write_redirect_map()


class FS:
    def __init__(self):
        self.files = {}
        self.calls = []

    def ls(self, path):
        self.calls.append(("ls", path))
        parts = [SimpleNamespace(path=p, name=p.rsplit("/", 1)[-1])
                 for p in self.files if p.startswith(path.rstrip("/") + "/")]
        if not parts:
            raise FileNotFoundError(path)
        return parts

    def cp(self, src, dst, recurse=False):
        self.calls.append(("cp", src, dst, recurse))
        self.files[dst] = self.files[src]
        return True

    def mv(self, src, dst, recurse=False):
        self.calls.append(("mv", src, dst, recurse))
        self.cp(src, dst, recurse)
        self.rm(src, recurse)
        return True

    def rm(self, path, recurse=False):
        self.calls.append(("rm", path, recurse))
        if recurse and path.endswith("/"):
            for key in list(self.files):
                if key.startswith(path):
                    del self.files[key]
        else:
            del self.files[path]
        return True

    def head(self, path, max_bytes=65536):
        self.calls.append(("head", path, max_bytes))
        return self.files[path][:max_bytes]

    def put(self, path, contents, overwrite=False):
        self.calls.append(("put", path, overwrite))
        self.files[path] = contents
        return True

    def mkdirs(self, path):
        self.calls.append(("mkdirs", path))
        self.files[path.rstrip('/') + '/'] = ''
        return True


class Writer:
    def __init__(self, fs):
        self.fs = fs
        self.write = self

    def coalesce(self, count):
        return self

    def mode(self, mode):
        return self

    def json(self, path):
        self.fs.calls.append(("write", path))
        self.fs.files[path.rstrip("/") + "/part-000.json"] = "payload"


def kernel():
    fs = FS()
    return fs, {"x2": Writer(fs), "dbutils": SimpleNamespace(fs=fs)}


def execute(code, namespace, child_order=False):
    # Legacy transform-order compatibility probe. Canonical parent/child/replay
    # now use _prepare_cell_redirects, exercised in test_redirect_order_and_output.
    rewritten = jm._apply_write_redirects(code)
    if child_order:
        rewritten = jm._apply_read_redirects(jm._inject_write_guard(rewritten))
    else:
        rewritten = jm._inject_write_guard(jm._apply_read_redirects(rewritten))
    exec(compile(rewritten, "<migration-exec>", "exec"), namespace)
    return rewritten


@pytest.mark.parametrize("child_order", [False, True])
@pytest.mark.parametrize("cross_cell", [False, True])
@pytest.mark.parametrize("slash", ["", "/"])
def test_full_forecasting_sequence(child_order, cross_cell, slash):
    fs, ns = kernel()
    producer = f"tmp = {SOURCE!r} + {slash!r}\nx2.coalesce(1).write.mode('overwrite').json(tmp)"
    consumer = """
parts = dbutils.fs.ls(tmp)
final = 'oci://source@tenant/reports/metric.json'
dbutils.fs.cp(src=parts[0].path, dst=final, recurse=True)
assert dbutils.fs.head(final) == 'payload'
dbutils.fs.rm(path=tmp, recurse=True)
"""
    def run():
        if cross_cell:
            execute(producer, ns, child_order)
            execute(consumer, ns, child_order)
        else:
            execute(producer + "\n" + consumer, ns, child_order)
    if not slash:
        # Live PR110 replay: the real shim fails without a slash. The EXEC
        # proxy must expose the same error, not silently certify saved code.
        with pytest.raises(KeyError, match="reports/tmp"):
            run()
        assert ("rm", TARGET, True) in fs.calls
        assert TARGET + "/part-000.json" in fs.files
        return
    run()
    assert fs.files == {"oci://sandbox@testns/source/reports/metric.json": "payload"}
    assert ("rm", TARGET + "/", True) in fs.calls
    assert "_aidp__" not in producer + consumer  # source is never edited


def test_no_source_read_redirect_and_component_boundary():
    fs, ns = kernel()
    fs.files[SOURCE + "-input/part-000.json"] = "input"
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    execute(f"rows=dbutils.fs.ls({SOURCE + '-input'!r})", ns)
    assert ns["rows"][0].path.startswith(SOURCE + "-input/")


def test_file_cleanup_does_not_acquire_directory_slash():
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    execute("part=dbutils.fs.ls(tmp)[0].path\ndbutils.fs.rm(part, True)", ns)
    assert ("rm", TARGET + "/part-000.json", True) in fs.calls


def test_move_produced_file_and_follow_read():
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    execute("part=dbutils.fs.ls(tmp)[0].path\n"
            "final='oci://source@tenant/reports/moved.json'\n"
            "dbutils.fs.mv(part, final)\nvalue=dbutils.fs.head(final, 3)", ns)
    assert ns["value"] == "pay"


def test_new_job_does_not_inherit_previous_runtime_mapping():
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    jm.clear_write_redirect_map()
    with pytest.raises(FileNotFoundError, match="source@tenant"):
        execute("dbutils.fs.ls(tmp)", ns)


@pytest.mark.parametrize("op", ["rm", "mv"])
def test_untracked_destructive_source_fails_loudly(op):
    fs, ns = kernel()
    args = f"{SOURCE!r}, True" if op == "rm" else f"{SOURCE!r}, 'oci://source@tenant/dst'"
    with pytest.raises(RuntimeError, match="Untracked OCI"):
        execute(f"dbutils.fs.{op}({args})", ns)
    assert fs.calls == []


def test_failures_are_not_swallowed(monkeypatch):
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    def fail(*args, **kwargs):
        raise PermissionError("cleanup denied")
    monkeypatch.setattr(fs, "rm", fail)
    with pytest.raises(PermissionError, match="cleanup denied"):
        execute("dbutils.fs.rm(tmp, True)", ns)


def test_rewrite_preserves_comments_unicode_and_is_idempotent():
    code = "# dbutils.fs.rm(tmp, True)\nlabel='Ã©'; rows=dbutils.fs.ls(tmp)\n"
    transformed = jm._inject_write_guard(code)
    assert code.splitlines()[0] in transformed
    assert "label='Ã©'; rows=_aidp__fs_proxy__(dbutils.fs).ls(tmp)" in transformed
    assert jm._inject_write_guard(transformed) == transformed


def test_disabled_redirect_is_unchanged(monkeypatch):
    monkeypatch.setattr(jm, "_REDIRECT_BUCKET", None)
    code = "dbutils.fs.ls(tmp)\nx2.write.json(tmp)"
    assert jm._inject_write_guard(code) == code


def test_scala_and_aliases_do_not_receive_python_proxy():
    scala = '%scala\nframe.write.json(tmp)'
    assert "_aidp__fs_proxy__" not in jm._inject_write_guard(scala)
    assert jm._inject_write_guard("fs_alias.ls(tmp)") == "fs_alias.ls(tmp)"


@pytest.mark.parametrize("child_order", [False, True])
def test_literal_write_then_variable_consumer(child_order):
    fs, ns = kernel()
    execute(f"x2.write.json({SOURCE!r})", ns, child_order)
    execute(f"tmp={SOURCE!r}\nrows=dbutils.fs.ls(tmp)\ndbutils.fs.rm(tmp + '/', True)", ns, child_order)
    assert ("ls", TARGET) in fs.calls
    assert not fs.files


def test_retry_does_not_double_redirect_or_reset_mapping():
    fs, ns = kernel()
    source = f"tmp={SOURCE!r}\nx2.write.json(tmp)\nrows=dbutils.fs.ls(tmp)"
    execute(source, ns)
    execute(source, ns)
    assert list(fs.files) == [TARGET + "/part-000.json"]
    assert fs.calls.count(("ls", TARGET)) == 2


def test_same_prefix_file_is_not_treated_as_directory():
    fs, ns = kernel()
    fs.files["oci://source@tenant/input.json"] = "input"
    execute("final='oci://source@tenant/result.json'\n"
            "dbutils.fs.cp('oci://source@tenant/input.json', final)", ns)
    child = "oci://source@tenant/result.json/nested"
    fs.files[child] = "separate"
    execute(f"value=dbutils.fs.head({child!r})", ns)
    assert ns["value"] == "separate"


def test_python_magic_is_preserved():
    wrapped = jm._inject_write_guard("%python\nrows=dbutils.fs.ls(tmp)")
    assert wrapped.startswith("%python\n")
    compile(wrapped.split("\n", 1)[1], "<python-magic-body>", "exec")


def test_real_replay_orchestration_preserves_saved_source(monkeypatch):
    import agent_migrate as am
    monkeypatch.setattr(am, "_cell_history", [])
    fs, ns = kernel()
    async def safe_execute(session, code, **kwargs):
        exec(compile(code, "<replay>", "exec"), ns)
        return {"status": "ok", "outputs": []}
    async def no_llm(*args, **kwargs):
        pytest.fail("The deterministic incident sequence must not require LLM repair")
    monkeypatch.setattr(jm, "_safe_execute", safe_execute)
    monkeypatch.setattr(jm, "call_fix", no_llm)
    source = (f"tmp={SOURCE!r}\nx2.write.json(tmp)\n"
              "parts=dbutils.fs.ls(tmp)\n"
              "dbutils.fs.cp(parts[0].path, 'oci://source@tenant/final.json')\n"
              "dbutils.fs.rm(tmp + '/', True)")
    result = asyncio.run(jm._replay_cell_entry({
        "final_code": source, "cell_idx": 13, "notebook_path": "/Workspace/test/notebook.ipynb"
    }, "forecasting path-consistency regression", object()))
    assert result["status"] == "ok"
    assert result["final_code"] == source
    assert am._cell_history[-1]["replay_code"] == source
    assert fs.files == {"oci://sandbox@testns/source/final.json": "payload"}


@pytest.mark.parametrize("stream_only", [False, True])
def test_replay_refusal_never_reaches_model(monkeypatch, stream_only):
    import agent_migrate as am
    monkeypatch.setattr(am, "_cell_history", [])
    message = "__AIDP_REDIRECT_REFUSED__: Untracked OCI cleanup/move: " + SOURCE
    class Session:
        async def execute(self, code, **kwargs):
            if stream_only:
                return {"status": "ok", "outputs": [{"type": "stream", "text": "RuntimeError: " + message}]}
            return {"status": "error", "outputs": [{"type": "error", "ename": "RuntimeError", "evalue": message}]}
    async def no_fix(*args, **kwargs):
        pytest.fail("Policy refusal must not enter the model repair loop")
    monkeypatch.setattr(jm, "call_fix", no_fix)
    source = f"dbutils.fs.rm({SOURCE!r}, True)"
    result = asyncio.run(jm._replay_cell_entry(
        {"final_code": source, "cell_idx": 0, "notebook_path": "/Workspace/test/refusal.ipynb"},
        "review refusal regression", Session()))
    assert result["status"] == "error"
    assert result["kind"] == "REDIRECT_REFUSED"
    assert am._cell_history[-1]["kind"] == "REDIRECT_REFUSED"
    assert result["final_code"] == source


def test_runtime_decisions_are_host_logs_not_saved_stdout():
    import json
    event = "__AIDP_PATH_REDIRECT__" + json.dumps([SOURCE, TARGET]) + "\n"
    result = {"status": "ok", "outputs": [{"type": "stream", "text": event + "customer output\n"}]}
    logs = []
    cleaned = jm._extract_redirect_events(result, logs.append)
    assert cleaned["outputs"][0]["text"] == "customer output\n"
    assert logs == [f"[AIDP write-redirect] {SOURCE} -> {TARGET}"]


@pytest.mark.parametrize("operation", ["cp", "mv"])
@pytest.mark.parametrize("slash", ["", "/"])
def test_directory_copy_move_refused_before_shim(operation, slash):
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r} + {slash!r}\nx2.write.json(tmp)", ns)
    before = dict(fs.files)
    with pytest.raises(RuntimeError, match="__AIDP_REDIRECT_REFUSED__"):
        execute(f"dbutils.fs.{operation}(tmp, 'oci://source@tenant/dst', True)", ns)
    assert fs.files == before
    assert not any(call[0] in ("cp", "mv", "rm") for call in fs.calls)


@pytest.mark.parametrize("child_order", [False, True])
@pytest.mark.parametrize("slash", ["", "/"])
def test_literal_seed_normalizes_both_sides(child_order, slash):
    fs, ns = kernel()
    execute(f"x2.write.json({SOURCE + slash!r})", ns, child_order)
    execute(f"tmp={SOURCE!r}\nrows=dbutils.fs.ls(tmp)", ns, child_order)
    assert ns["rows"][0].path == TARGET + "/part-000.json"


@pytest.mark.parametrize("magic", ["%python ", "%python # comment", "%python\t", "%pyspark", "%python\r"])
def test_python_magic_variants_wrap_consumers(magic):
    wrapped = jm._inject_write_guard(magic + "\nrows=dbutils.fs.ls(tmp)")
    assert wrapped.splitlines()[0] == magic.rstrip('\r')
    assert "rows=_aidp__fs_proxy__(dbutils.fs).ls(tmp)" in wrapped
    compile(wrapped.split('\n', 1)[1], '<body>', 'exec')


@pytest.mark.parametrize("magic", ["%python-extra", "%pyspark-extra"])
@pytest.mark.parametrize(
    ("statement", "reason"),
    [
        ("rows=dbutils.fs.ls(tmp)", "Cannot safely wrap Python dbutils.fs calls"),
        ("df.write.json(tmp)", "Cannot safely wrap Python writer calls"),
    ],
)
def test_write_guard_python_magic_alias_prefixes_fail_closed(magic, statement, reason):
    guarded = jm._inject_write_guard(f"{magic}\n{statement}")
    assert guarded.startswith("raise SyntaxError(")
    assert reason in guarded
    assert "def _aidp__write_guard__" not in guarded


def test_helper_before_magic_is_parsed_and_invalid_python_is_loud():
    source = "helper = 1\n%python # comment\nrows=dbutils.fs.ls(tmp)"
    assert "rows=_aidp__fs_proxy__(dbutils.fs).ls(tmp)" in jm._wrap_output_consumers(source)
    with pytest.raises(SyntaxError, match="Cannot safely wrap"):
        exec(jm._inject_write_guard("broken syntax!\ndbutils.fs.rm(tmp, True)"))
    for magic in ('%sql', '%md', '%sh'):
        code = magic + '\ndbutils.fs.rm(tmp, True)'
        assert jm._inject_write_guard(code) == code


def test_direct_put_mkdirs_only_touch_sandbox():
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\ndbutils.fs.mkdirs(tmp)\ndbutils.fs.put(tmp + '/done', 'ok')", ns)
    assert set(fs.files) == {TARGET + '/', TARGET + '/done'}
    execute("assert dbutils.fs.head(tmp + '/done') == 'ok'", ns)


def test_read_redirect_cannot_corrupt_encoded_seed():
    fs, ns = kernel()
    jm._write_redirect_path_map[SOURCE + '/'] = TARGET + '/'
    fs.files[TARGET + '/part-000.json'] = 'payload'
    # Force the later global literal-read rewrite over injected source too.
    code = f"rows=dbutils.fs.ls(tmp)\n# spark.read.json('{SOURCE}/')"
    ns['tmp'] = SOURCE
    transformed = jm._apply_read_redirects(jm._inject_write_guard(code))
    exec(transformed, ns)
    assert ns['rows'][0].path == TARGET + '/part-000.json'


def test_seed_does_not_reset_runtime_file_metadata():
    fs, ns = kernel()
    fs.files['oci://source@tenant/input'] = 'payload'
    execute(f"dbutils.fs.cp('oci://source@tenant/input', {SOURCE!r})", ns)
    jm._write_redirect_path_map[SOURCE] = TARGET
    execute(f"assert dbutils.fs.head({SOURCE!r}) == 'payload'", ns)
    assert ns['_aidp__output_paths__'][SOURCE] == (TARGET, False)


def test_longest_prefix_wins_with_nested_registered_roots():
    fs, ns = kernel()
    execute(f"x2.write.json({SOURCE!r})", ns)
    # Literal mappings can supply a more-specific destination than the parent.
    inner = SOURCE + '/inner'
    other = 'oci://sandbox@testns/specific/inner'
    jm._write_redirect_path_map[inner] = other
    fs.files[other + '/part.json'] = 'nested'
    execute(f"dbutils.fs.rm({inner + '/'!r}, True)", ns)
    assert ('rm', other + '/', True) in fs.calls
    assert TARGET + '/part-000.json' in fs.files


@pytest.mark.parametrize('refusal,sandbox', [(False, True), (True, True), (False, False)])
def test_child_real_loop_clean_fix_source_and_refusal(monkeypatch, refusal, sandbox):
    import base64
    import json
    import re
    import agent_migrate as am
    path = '/Workspace/test_migrated/child.ipynb'
    source = 'rows = dbutils.fs.ls(tmp)'
    if not sandbox:
        monkeypatch.setattr(jm, '_REDIRECT_BUCKET', None)
        monkeypatch.setattr(jm, '_REDIRECT_NAMESPACE', None)
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {path: json.dumps({
        'cells': [{'cell_type': 'code', 'source': [source]}]})})
    monkeypatch.setattr(am, '_cell_history', [])
    monkeypatch.setattr(jm, '_cell_history', am._cell_history)
    monkeypatch.setattr(am, '_current_cell_notes', [])
    monkeypatch.setattr(jm, '_current_cell_notes', am._current_cell_notes)
    monkeypatch.setattr(jm, 'get_bucket_mapping_context', lambda: '')
    async def summary(*a, **k): return 'child'
    monkeypatch.setattr(am, '_summarize_cell_code', summary)
    received = []
    async def fix(code, *a, **k):
        assert not refusal, 'policy refusal reached model'
        assert '_aidp__' not in code and 'sandbox@testns' not in code
        received.append(code)
        return code + '\n# Oracle tool modification: repaired test condition'
    monkeypatch.setattr(jm, 'call_fix', fix)
    class Session:
        count = 0
        payload = None
        async def execute(self, code, **kwargs):
            if '__AIDP_CHILD_COUNT__' in code:
                return {'status': 'ok', 'outputs': [
                    {'type': 'stream', 'text': '__AIDP_CHILD_COUNT__1\n'}]}
            if 'payload = json.loads(base64.b64decode(' in code:
                encoded = re.search(r"base64.b64decode\('([^']+)'\)", code).group(1)
                self.payload = json.loads(base64.b64decode(encoded))
                replaced = len(self.payload['cells'])
                return {
                    'status': 'ok',
                    'outputs': [{
                        'type': 'stream',
                        'text': (f'Replaced {replaced}/{replaced} cells '
                                 '(config_offset=0)'),
                    }],
                }
            if ("_aidp__fs_proxy__(dbutils.fs).ls(tmp)" if sandbox else source) in code:
                self.count += 1
                if self.count == 1:
                    msg = ('__AIDP_REDIRECT_REFUSED__: Untracked OCI cleanup/move: ' + SOURCE
                           if refusal else 'synthetic first-attempt failure')
                    return {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'RuntimeError', 'evalue': msg}]}
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}
    session = Session()
    result = asyncio.run(jm._inline_child_notebook(path, session, SimpleNamespace(log=lambda *a: None),
                                                'test_job', '', '', '', {}))
    if refusal:
        assert result['kind'] == 'REDIRECT_REFUSED' and result['status'] == 'error'
        assert session.count == 1 and not received and session.payload is None
    else:
        assert result['status'] == 'ok' and result['cells_fixed'] == 1
        assert len(received) == 1 and session.count == 2
        assert session.payload and source in session.payload['cells']['0']
        assert '_aidp__' not in json.dumps(session.payload)
    assert am._cell_history
    assert all('_aidp__' not in h['replay_code'] for h in am._cell_history)


def test_child_filesystem_read_failure_uses_migrated_copy(monkeypatch):
    """A failed Source-2 cell read must not be treated as notebook source."""
    path = '/Workspace/test_migrated/child.ipynb'
    resolved = '/Workspace/test_migrated/child "resolved"\\path.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_attempted = False
        cell_read_command = ''

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 1, 'indices': [0],
                                'resolved_path': resolved, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                self.cell_read_command = code
                return {'status': 'error', 'outputs': [{'type': 'error', 'text':
                    'Traceback: transient FUSE read failure'}]}
            if f"with open('{resolved}') as f:" in code:
                self.source3_attempted = True
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'ok': True, 'count': 1, 'lang': 'python',
                                'cells': [{'src': 'value = 1'}]})}]}
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.source3_attempted
    assert json.dumps(resolved) in session.cell_read_command
    assert result['status'] == 'ok'
    assert result['cells_ok'] == 1


def test_child_source3_tries_resolved_probe_path_before_original(monkeypatch):
    """Source 3 must preserve the exact path already proven by Source 2."""
    path = '/Workspace/test_migrated/A+B.ipynb'
    resolved = '/Workspace/test_migrated/A_B.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_paths = []

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 1, 'indices': [0],
                                'resolved_path': resolved, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                return {'status': 'ok', 'outputs': []}
            if "with open('" in code:
                opened = code.split("with open('", 1)[1].split("')", 1)[0]
                self.source3_paths.append(opened)
                if opened == resolved:
                    return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                        json.dumps({'ok': True, 'count': 1, 'lang': 'python',
                                    'cells': [{'src': 'value = 1'}]})}]}
                pytest.fail('Source 3 tried the original path before resolved_path')
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.source3_paths == [resolved]
    assert result['status'] == 'ok'
    assert result['resolved_path'] == resolved
    assert resolved in jm._notebook_content_cache
    assert path not in jm._notebook_content_cache
    assert result['cells_ok'] == 1


def test_child_filesystem_empty_ok_output_uses_migrated_copy(monkeypatch):
    """A status=ok Source-2 read with no output must not drop a child cell."""
    path = '/Workspace/test_migrated/child.ipynb'
    resolved = '/Workspace/test_migrated/child_resolved.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_attempted = False

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 1, 'indices': [0],
                                'resolved_path': resolved, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                return {'status': 'ok', 'outputs': []}
            if f"with open('{path}') as f:" in code:
                self.source3_attempted = True
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'ok': True, 'count': 1, 'lang': 'python',
                                'cells': [{'src': 'recovered = 1'}]})}]}
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.source3_attempted
    assert result['status'] == 'ok'
    assert result['cells_ok'] == 1


def test_child_filesystem_partial_multi_cell_read_uses_migrated_copy(monkeypatch):
    """One missing result in a multi-cell Source-2 read triggers fallback."""
    path = '/Workspace/test_migrated/child.ipynb'
    resolved = '/Workspace/test_migrated/child_resolved.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_attempted = False
        cell_reads = 0

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 2, 'indices': [0, 1],
                                'resolved_path': resolved, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                self.cell_reads += 1
                if self.cell_reads == 1:
                    return {'status': 'ok', 'outputs': [
                        {'type': 'stream', 'text': 'first = 1'}]}
                return {'status': 'ok', 'outputs': []}
            if f"with open('{path}') as f:" in code:
                self.source3_attempted = True
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'ok': True, 'count': 2, 'lang': 'python',
                                'cells': [{'src': 'first = 1'},
                                          {'src': 'second = 2'}]})}]}
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.cell_reads == 2
    assert session.source3_attempted
    assert result['status'] == 'ok'
    assert result['cells_ok'] == 2


def test_child_filesystem_complete_output_keeps_source2(monkeypatch):
    """Complete Source-2 output is used without invoking fallback readers."""
    path = '/Workspace/test_migrated/child.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_attempted = False
        cell_reads = 0

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 2, 'indices': [0, 1],
                                'resolved_path': path, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                self.cell_reads += 1
                source = 'first = 1' if self.cell_reads == 1 else 'second = 2'
                return {'status': 'ok', 'outputs': [
                    {'type': 'stream', 'text': source}]}
            if f"with open('{path}') as f:" in code:
                self.source3_attempted = True
                pytest.fail('complete Source 2 unexpectedly invoked Source 3')
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.cell_reads == 2
    assert not session.source3_attempted
    assert result['status'] == 'ok'
    assert result['cells_ok'] == 2


def test_child_filesystem_config_cell_excluded_from_expected_count(monkeypatch):
    """The probe's non-config indices define the reconciliation count."""
    path = '/Workspace/test_migrated/child.ipynb'
    monkeypatch.setattr(jm, 'MIGRATED_BASE', '/Workspace/test_migrated')
    monkeypatch.setattr(jm, '_notebook_content_cache', {})

    class Session:
        source3_attempted = False
        cell_reads = 0

        async def execute(self, code, **kwargs):
            if 'found_path = None' in code:
                assert "_CFG_MARKER not in" in code
                # Index 0 is the generated config cell and is deliberately
                # absent. Only the customer cell at index 1 must be read.
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    json.dumps({'count': 1, 'indices': [1],
                                'resolved_path': path, 'lang': 'python'})}]}
            if "src = ''.join" in code:
                self.cell_reads += 1
                assert "['cells'][1]" in code
                return {'status': 'ok', 'outputs': [
                    {'type': 'stream', 'text': 'customer_value = 1'}]}
            if f"with open('{path}') as f:" in code:
                self.source3_attempted = True
                pytest.fail('excluded config cell caused a false fallback')
            return {'status': 'ok', 'outputs': [{'type': 'stream', 'text': '1'}]}

    session = Session()
    result = asyncio.run(jm._inline_child_notebook(
        path, session, SimpleNamespace(log=lambda *a: None),
        'test_job', '', '', '', {}))

    assert session.cell_reads == 1
    assert not session.source3_attempted
    assert result['status'] == 'ok'
    assert result['cells_ok'] == 1


def test_main_loop_refusal_is_reported_without_model(monkeypatch, tmp_path):
    import agent_migrate as am
    from test_conditional_exit_below_execute import _patch_common, _Kernel
    source = f"dbutils.fs.rm({SOURCE!r}, True)"
    notebook = {'cells': [{'cell_type': 'code', 'source': [source], 'metadata': {}, 'outputs': []}],
                'metadata': {}, 'nbformat': 4, 'nbformat_minor': 5}
    guard = jm._inject_write_guard
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, '_inject_write_guard', guard)
    monkeypatch.setattr(jm, 'STRICT_CELL_GATING', True)
    monkeypatch.setattr(am, '_cell_history', [])
    monkeypatch.setattr(jm, '_cell_history', am._cell_history)
    async def no_fix(*a, **k): pytest.fail('policy refusal reached model')
    monkeypatch.setattr(jm, 'call_fix', no_fix)
    class Session(_Kernel):
        async def execute(self, code, **kwargs):
            if '_aidp__fs_proxy__(dbutils.fs).rm(' in code:
                return {'status': 'ok', 'outputs': [{'type': 'stream', 'text':
                    'RuntimeError: __AIDP_REDIRECT_REFUSED__: Untracked OCI cleanup/move: ' + SOURCE}]}
            return await super().execute(code, **kwargs)
    result = asyncio.run(jm.process_notebook('/Workspace/test_cond/source.ipynb', Session(),
                                           'test_cond', '01_task', {}))
    assert not result['status'].startswith('PASS')
    assert any(h.get('kind') == 'REDIRECT_REFUSED' for h in am._cell_history)
    assert result['cell_results'][0]['kind'] == 'REDIRECT_REFUSED'
    assert SOURCE in result['cell_results'][0]['note']


@pytest.mark.parametrize('operation', ['rm', 'cp', 'mv'])
def test_real_shim_cannot_hide_cleanup_or_lose_directory_data(monkeypatch, operation):
    import importlib.util
    path = Path(__file__).resolve().parents[1] / 'aidp_compat' / 'fs.py'
    spec = importlib.util.spec_from_file_location('_pr110_real_fs', path)
    fs_module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, fs_module)
    spec.loader.exec_module(fs_module)
    class Store:
        objects = {'source/reports/tmp/': b'', 'source/reports/tmp/part.json': b'payload'}
        calls = []
        def delete_object(self, namespace, bucket, key):
            self.calls.append(('delete', key))
            if key not in self.objects: raise FileNotFoundError('ObjectNotFound')
            del self.objects[key]
        def copy_object(self, **kwargs):
            pytest.fail('Directory copy must be refused before the real shim')
    store = Store()
    monkeypatch.setattr(fs_module, '_get_oci_storage_client', lambda: store)
    shim = fs_module.AIDPFileSystemUtils()
    ns = {'dbutils': SimpleNamespace(fs=shim), 'tmp': SOURCE}
    jm._write_redirect_path_map[SOURCE] = TARGET
    if operation == 'rm':
        with pytest.raises(OSError, match='Remove failed'):
            execute('dbutils.fs.rm(tmp, True)', ns)
        assert store.calls == [('delete', 'source/reports/tmp')]
    else:
        with pytest.raises(RuntimeError, match='__AIDP_REDIRECT_REFUSED__'):
            execute(f"dbutils.fs.{operation}(tmp, 'oci://source@tenant/dst', True)", ns)
        assert not store.calls
    assert store.objects['source/reports/tmp/part.json'] == b'payload'


@pytest.mark.parametrize('wrapped', [False, True])
def test_event_extraction_preserves_neighboring_customer_text(wrapped):
    import json
    event = '__AIDP_PATH_REDIRECT__' + json.dumps([SOURCE, TARGET]) + '\n'
    text = 'before\n' + event + 'after\n'
    if wrapped:
        text = json.dumps([{'type': 'TEXT_PLAIN', 'value': text}])
    result = {'outputs': [{'type': 'stream', 'text': text}]}
    logs = []
    result = jm._extract_redirect_events(result, logs.append)
    assert result['outputs'][0]['text'] == 'before\nafter\n'
    assert len(logs) == 1


@pytest.mark.parametrize('method,original_name', [('saveAsTable', '_aidp_rr_orig_sat'), ('insertInto', '_aidp_rr_orig_ii')])
def test_table_writer_diagnostics_are_stripped_too(method, original_name, capsys):
    import ast
    tree = ast.parse(jm.get_aidp_topandas_safety_cell())
    function = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == '_aidp_rr_' + method)
    calls = []
    namespace = {'_aidp_rr_write_target': lambda name: 'sandbox.' + name,
                 original_name: lambda self, name, *a, **k: calls.append(name)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<table-wrapper>', 'exec'), namespace)
    namespace[function.name](None, 'output')
    text = capsys.readouterr().out
    logs = []
    result = jm._extract_redirect_events({'outputs': [{'type': 'stream', 'text': text}]}, logs.append)
    assert result['outputs'][0]['text'] == ''
    assert calls == ['sandbox.output'] and logs


def test_cleanup_repair_is_persisted_and_runs_without_proxy(monkeypatch):
    import agent_migrate as am
    fs, ns = kernel()
    execute(f"tmp={SOURCE!r}\nx2.write.json(tmp)", ns)
    source = 'dbutils.fs.rm(tmp, True)'
    repaired = ('# Oracle tool modification: directory cleanup requires prefix semantics\n'
                '# dbutils.fs.rm(tmp, True)\ndbutils.fs.rm(tmp + "/", True)')
    saved = [source]
    monkeypatch.setattr(am, '_cell_history', [])
    class Session:
        async def execute(self, code, **kwargs):
            try:
                exec(code, ns)
                return {'status': 'ok', 'outputs': []}
            except KeyError as e:
                return {'status': 'error', 'outputs': [{'type': 'error', 'ename': 'KeyError', 'evalue': str(e)}]}
    async def fix(code, *a, **k):
        assert code == source and '_aidp__' not in code
        return repaired
    monkeypatch.setattr(jm, 'call_fix', fix)
    result = asyncio.run(jm._replay_cell_entry({
        'final_code': source, 'cell_idx': 0, 'notebook_path': '/Workspace/test/cleanup.ipynb',
        '_persist_source': saved, '_persist_active': True}, 'cleanup failure', Session()))
    assert result['status'] == 'ok' and saved == [repaired]
    assert '_aidp__' not in saved[0]
    delivered = FS()
    delivered.files[TARGET + '/part.json'] = 'payload'
    exec(saved[0], {'dbutils': SimpleNamespace(fs=delivered), 'tmp': TARGET})
    assert not delivered.files
