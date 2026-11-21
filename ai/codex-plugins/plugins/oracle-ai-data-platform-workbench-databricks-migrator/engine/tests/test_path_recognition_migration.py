"""REF-158: the model's Python path code is what runs and what is saved.

The observed Model_Monitoring failure affected both S3 aliases and DBFS
aliases: the S3/DBFS literal sweep rewrote the strings path checks use to
recognize their input, in customer source and in the model's own repairs.
Python cells are no longer swept. A Python cell with an S3/DBFS path is a
Databricks marker, so it meets the model, and the model's reply is executed,
verified and saved without being translated again. A Python cell below a
fired notebook.exit meets the model code-only (it is not run); cells
converted without the model keep their storage paths and report them.

Exercise the real cell loop with model/platform boundaries offline; judge the
saved functions by calling them, rather than checking their shape. Case A
reads its location itself (SaveTableHelpers asks the metastore); Case B is a
path helper (Data_Aggregation's).
"""
import asyncio
import contextlib
import io
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import agent_migrate as am
import job_migrate as jm
from test_child_exit_conversion import CONDITIONAL, _run as _run_child_exit
from test_path_checks_never_swept import (
    B1_LOCATED, REVIEW_SHAPES, SAVE_FSTRING, SAVE_TRANSLATED, _Spark, sweep as _literal_sweep)
from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources


GOLD = "oci://oci-test-gold@test_namespace/"
VOLUME = "/Volumes/default/default/dbfs/"
# A location the function reads itself. (DESCRIBE is a Databricks marker,
# which would send every cell to the model; this kernel call is not.)
LOCATION = "spark.table(table).inputFiles()[0]"
CALL = "RESULT = resolve_path(PATH_INPUT)\n"
CASES = ["gold_tuple", "gold_normalize", "dbfs", "dbfs_early_return"]


def _cell(source):
    return {"cell_type": "code", "source": [source], "metadata": {},
            "outputs": [], "execution_count": None}


def _gold_source(form):
    if form == "normalize":
        body = (
            f'    path = {LOCATION}\n'
            '    path = path.replace("s3a://", "s3://").replace("s3n://", "s3://")\n'
            '    prefix = "s3://test-gold/"\n'
            '    if not path.startswith(prefix):\n'
            '        raise ValueError("wrong bucket")\n'
            '    return TARGET + path[len(prefix):]\n'
        )
        setup = ""
    else:
        setup = ('GOLD_BUCKET_LEGACY_PREFIXES = ("s3://test-gold/", '
                 '"s3a://test-gold/", "s3n://test-gold/")\n')
        body = (
            f'    path = {LOCATION}\n'
            '    for prefix in GOLD_BUCKET_LEGACY_PREFIXES:\n'
            '        if path.startswith(prefix):\n'
            '            return TARGET + path[len(prefix):]\n'
            '    if path.startswith(TARGET):\n'
            '        return path\n'
            '    raise ValueError("wrong bucket")\n'
        )
    return setup + f'TARGET = {GOLD!r}\ndef resolve_path(table):\n' + body


def _model_gold_reply(form):
    if form == "tuple":
        return '# Oracle tool modification: preserve S3 input aliases\n' + _gold_source(form)
    # Existing normalization remains valid; translate its VALUE before
    # comparing with the OCI target. This fake reply is independently called.
    return (
        '# Oracle tool modification: translate normalized location before validation\n'
        f'TARGET = {GOLD!r}\ndef resolve_path(table):\n'
        f'    path = {LOCATION}\n'
        '    path = path.replace("s3a://", "s3://").replace("s3n://", "s3://")\n'
        '    if path.startswith("s3://test-gold/"):\n'
        '        path = TARGET + path[len("s3://test-gold/"):]\n'
        '    if not path.startswith(TARGET):\n'
        '        raise ValueError("wrong bucket")\n'
        '    return path\n'
    )


def _dbfs_source():
    return (
        'def resolve_path(path):\n'
        f'    for prefix in ("dbfs:/", "/dbfs/", {VOLUME!r}):\n'
        '        if path.startswith(prefix):\n'
        f'            return {VOLUME!r} + path[len(prefix):]\n'
        '    return path\n' + CALL
    )


def _dbfs_early_return_source():
    # Data_Aggregation's helper: the first check returns at once.
    return (
        'def resolve_path(path):\n'
        "    if '/dbfs/FileStore/' in path:\n"
        '        return path\n'
        "    if 'dbfs:' in path:\n"
        "        return path.replace('dbfs:', '/dbfs')\n"
        "    if path.startswith('/FileStore'):\n"
        "        return '/dbfs' + path\n" + CALL
    )


def _model_dbfs_early_return_reply():
    # The helper is only right on AIDP once migrated: every DBFS form must
    # reach the Volume.
    return (
        '# Oracle tool modification: DBFS paths live under the AIDP Volume\n'
        'def resolve_path(path):\n'
        f'    if path.startswith({VOLUME!r}):\n'
        '        return path\n'
        '    for prefix in ("dbfs:/", "/dbfs/"):\n'
        '        if path.startswith(prefix):\n'
        f'            return {VOLUME!r} + path[len(prefix):]\n'
        "    if path.startswith('/FileStore'):\n"
        f'        return {VOLUME[:-1]!r} + path\n' + CALL
    )


def _source(case):
    if case.startswith("gold"):
        return _gold_source(case.removeprefix("gold_"))
    return _dbfs_early_return_source() if case == "dbfs_early_return" else _dbfs_source()


def _model_code(reply):
    """The model's code as the cell loop reads it from the reply."""
    return jm._extract_code_from_response(reply)


def _reply(case):
    """What the model returns for the case's source."""
    if case.startswith("gold"):
        return _model_gold_reply(case.removeprefix("gold_"))
    return _model_dbfs_early_return_reply() if case == "dbfs_early_return" else _dbfs_source()


def _resolver(sources, case):
    """The saved code as a function of its input: the location the function
    reads (Case A), or the argument of the helper's call (Case B)."""
    def resolve(value):
        namespace = {"spark": _Spark(value), "PATH_INPUT": value}
        for source in sources:
            exec(source, namespace)
        return namespace["RESULT"] if case.startswith("dbfs") else namespace["resolve_path"]("ds.t")
    return resolve


def _probe(source, case):
    _probe_resolver(_resolver([source], case), case)


def _probe_resolver(resolve, case, gold=GOLD, schemes=("s3", "s3a", "s3n")):
    if case.startswith("gold"):
        for scheme in schemes:
            assert resolve(scheme + "://test-gold/folder/table") == gold + "folder/table"
        assert resolve(gold + "folder/table") == gold + "folder/table"
        assert resolve(resolve(gold + "folder/table")) == gold + "folder/table"
        for bad in ("s3://other/table", "s3a://other/table", "s3n://other/table",
                    "oci://other@test_namespace/table", "s3://test-gold-lookalike/table"):
            with pytest.raises(ValueError):
                resolve(bad)
    elif case == "dbfs_early_return":
        for form in ("/dbfs/FileStore/", "dbfs:/FileStore/", "/FileStore/", VOLUME + "FileStore/"):
            assert resolve(form + "probe.txt") == VOLUME + "FileStore/probe.txt"
    else:
        for prefix in ("dbfs:/", "/dbfs/", VOLUME):
            assert resolve(prefix + "FileStore/probe.txt") == VOLUME + "FileStore/probe.txt"
        assert resolve(resolve("dbfs:/FileStore/probe.txt")) == VOLUME + "FileStore/probe.txt"
        assert resolve("/ordinary/probe.txt") == "/ordinary/probe.txt"


@pytest.mark.parametrize("case", CASES)
def test_the_sweep_breaks_the_models_recognizer_so_python_is_never_swept(monkeypatch, case):
    monkeypatch.setattr(jm, "_ns_for_oci_bucket", lambda bucket: "test_namespace")
    reply = _reply(case)
    _probe(reply, case)
    # The REF-158 mechanism (run c3ce3ae3): the literal sweep re-breaks the
    # model's recognizer...
    with pytest.raises((AssertionError, ValueError)):
        _probe(_literal_sweep(reply), case)
    # ...so no Python cell, source or reply, is swept.
    for code in (_source(case), reply):
        assert jm._sweep_storage_literals(code) == code


@pytest.mark.parametrize("case", CASES)
@pytest.mark.parametrize("mode", ["parent", "dependency", "child"])
def test_the_models_reply_is_what_runs_and_is_saved(monkeypatch, tmp_path, case, mode):
    marker_check = jm._has_databricks_markers
    source, reply = _source(case), _reply(case)
    notebook = {"cells": [_cell(source)], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "_has_databricks_markers", marker_check)
    monkeypatch.setattr(jm, "_ns_for_oci_bucket", lambda bucket: "test_namespace")
    monkeypatch.setattr(jm, "_apply_namespace_from_mapping", lambda code: code)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append((prompt, content))
        assert source in content
        return reply, {"input": 0, "output": 0}

    async def repair(*args, **kwargs):
        pytest.fail("the model's correct reply must not need repair")

    async def summarize(*args, **kwargs):
        return "path recognizer"

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    monkeypatch.setattr(jm, "call_fix", repair)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)

    class Kernel(_Kernel):
        async def execute(self, code, **kwargs):
            if "__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})
                return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": output.getvalue()}]}
            if "def resolve_path(" in code and "base64.b64decode" not in code:
                try:
                    _probe(code, case)
                except (AssertionError, ValueError) as exc:
                    return {"status": "error", "outputs": [{"type": "error", "ename": type(exc).__name__,
                            "evalue": str(exc), "traceback": [str(exc)]}]}
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    if mode == "child":
        # Inline children execute first. A migrated dependency is its input.
        notebook['cells'][0]['source'] = [reply]
        path = tmp_path / "child.ipynb"
        path.write_text(json.dumps(notebook))
        monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
        monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
        monkeypatch.setitem(jm._notebook_content_cache, str(path), json.dumps(notebook))
        log = type("Log", (), {"log": lambda self, message: None})()
        result = asyncio.run(jm._inline_child_notebook(str(path), kernel, log, "fixture", "", "", "", {}))
        assert result['status'] == 'ok', result
        saved = json.loads(path.read_text())
    else:
        dependency = mode == "dependency"
        result = asyncio.run(jm.process_notebook(
            "/Workspace/test_cond/source.ipynb", kernel, "fixture",
            "dep_paths" if dependency else "01_paths", {}, run_all=not dependency,
            defer_filestore_assignments=dependency,
        ))
        assert result['status'] == ('CONVERTED_NOT_VALIDATED' if dependency else 'PASS'), result
        _, saved = _saved_sources(kernel)
        assert len(calls) == 1
        assert "PATH RECOGNITION" in calls[0][0]
    saved_source = next(''.join(cell['source']) for cell in saved['cells']
                        if 'def resolve_path(' in ''.join(cell['source']))
    # Verified and saved: the same bytes the model returned.
    if mode == "child":
        assert saved_source == reply
    else:
        assert saved_source == _model_code(reply)
    if mode != "dependency":
        assert any(saved_source.rstrip("\n") in code for code in kernel.executed)
    _probe(saved_source, case)


def test_runtime_translator_does_not_map_s3_without_configuration(monkeypatch, tmp_path):
    # The guidance below routes S3 to suggest_oci_path because nothing in the
    # tool provisions S3 mappings for the cluster-side translator: an S3 value
    # comes back unchanged while DBFS still converts.
    from aidp_compat.path_translator import PathTranslator

    for key in ("AIDP_S3_MAPPINGS", "AIDP_MOUNT_MAPPINGS", "AIDP_DBFS_ROOT",
                "AIDP_DBFS_PREFIX", "AIDP_DBFS_CATALOG", "AIDP_DBFS_SCHEMA"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AIDP_PATH_CONFIG", str(tmp_path / "absent.json"))
    translator = PathTranslator()
    for path in ("s3://test-gold/folder/table", "s3a://test-gold/folder/table"):
        assert translator.translate(path) == path
    assert translator.translate("dbfs:/FileStore/probe.txt") == VOLUME + "FileStore/probe.txt"


def _model_prompts():
    return {
        f"{module.__name__}.{name}": value
        for module in (am, jm)
        for name, value in vars(module).items()
        if name.endswith("_PROMPT") and isinstance(value, str)
    }


def test_no_model_prompt_wraps_an_s3_literal_in_translate_path():
    s3_wrap = re.compile(r"translate_path\(\s*[fr]?['\"]s3[an]?://")
    offenders = [name for name, text in _model_prompts().items() if s3_wrap.search(text)]
    assert offenders == []


def test_s3_guidance_names_suggest_oci_path_in_migration_and_fix_prompts():
    guidance = " ".join(am.PATH_PREFIX_MIGRATION_GUIDANCE.split())
    assert "take the OCI bucket and namespace ONLY from the suggest_oci_path tool" in guidance
    assert "Do NOT use translate_path() for S3" in guidance
    assert "Use translate_path() only for DBFS paths" in guidance
    assert am.PATH_PREFIX_MIGRATION_GUIDANCE in jm.CELL_MIGRATE_PROMPT
    assert am.PATH_PREFIX_MIGRATION_GUIDANCE in am.FIX_PROMPT


ORDERS = ["same", "later", "earlier"]
ALIASES = ("s3", "s3a", "s3n")


def _cross_cell_sources(gold=GOLD, order="later"):
    """Case A with the prefix tuple in the function's cell, a later cell or an
    earlier cell. On AIDP the original is right as it is."""
    function = (
        f'TARGET = {gold!r}\ndef resolve_path(table):\n'
        f'    path = {LOCATION}\n'
        '    for prefix in GOLD_BUCKET_LEGACY_PREFIXES:\n'
        '        if path.startswith(prefix):\n'
        '            return TARGET + path[len(prefix):]\n'
        '    if path.startswith(TARGET):\n'
        '        return path\n'
        '    raise ValueError("wrong bucket")\n')
    constant = ("GOLD_BUCKET_LEGACY_PREFIXES = ("
                + ", ".join(f'"{a}://test-gold/"' for a in ALIASES) + ")\n")
    return {"same": [constant + function], "later": [function, constant],
            "earlier": [constant, function]}[order]


def _constant_index(sources):
    return next(i for i, s in enumerate(sources) if "GOLD_BUCKET_LEGACY_PREFIXES = (" in s)


def _probe_saved_cells(sources, gold=GOLD):
    cells = [s for s in sources if "resolve_path" in s or "GOLD_BUCKET_LEGACY_PREFIXES =" in s]
    _probe_resolver(_resolver(cells, "gold"), "gold", gold)


class _SnippetKernel(_Kernel):
    async def execute(self, code, **kwargs):
        if "__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                exec(code, {})
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                                                 "text": output.getvalue()}]}
        return await super().execute(code, **kwargs)


def _patch_cell_loop(monkeypatch, tmp_path, notebook):
    marker_check = jm._has_databricks_markers
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "_has_databricks_markers", marker_check)
    monkeypatch.setattr(jm, "_ns_for_oci_bucket", lambda bucket: "test_namespace")
    monkeypatch.setattr(jm, "_apply_namespace_from_mapping", lambda code: code)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])

    async def summarize(*args, **kwargs):
        return "path recognizer"

    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)


def _notebook(sources):
    return {"cells": [_cell(s) for s in sources], "metadata": {},
            "nbformat": 4, "nbformat_minor": 5}


def _saved_cell_sources(kernel):
    _, saved = _saved_sources(kernel)
    return ["".join(c["source"]) for c in saved["cells"]]


def _without_final_newlines(cells):
    # Save-back ends every cell without a trailing newline.
    return [cell.rstrip("\n") for cell in cells]


def _migration_report(kernel):
    return next(text for path, text in kernel.files.items() if path.endswith("migration_report.md"))


def _keeping_model(calls):
    async def migrate(prompt, content, **kwargs):
        # A model that keeps the cell as it is (it is already right on AIDP).
        source = content.split("```python\n", 1)[1].rsplit("\n```", 1)[0]
        calls.append(source)
        return source, {"input": 0, "output": 0}
    return migrate


@pytest.mark.parametrize("form", jm._STORAGE_PATH_MARKERS)
def test_a_python_cell_with_a_storage_path_meets_the_model(monkeypatch, tmp_path, form):
    source = f'DATA_PATH = "{form}fixture/input"\n'
    reply = ('# Oracle tool modification: fixture translation\n'
             'DATA_PATH = "oci://oci-fixture@test_namespace/fixture/input"\n')
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([source, "COUNT = 1\n"]))
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append(content)
        return reply, {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    kernel = _SnippetKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture", "01_paths", {}))
    assert result["status"] == "PASS", result
    # Only the cell with the storage path went to the model; the other ran as-is.
    assert len(calls) == 1 and source in calls[0]
    assert _saved_cell_sources(kernel) == [_model_code(reply), "COUNT = 1"]


# DBFS roots without a slash are no Databricks marker. The literal sweep used to
# rewrite them on the first-try path; without it they must meet the model rather
# than run raw (os.path / glob on a raw /dbfs path answer False / [] silently).
MARKERLESS_DBFS = {
    "root plus name": ('df = pd.read_csv("/dbfs" + FEATURES)\n',
                       f'df = pd.read_csv({VOLUME[:-1]!r} + FEATURES)\n'),
    "os.path.join root": ('present = os.path.exists(os.path.join("/dbfs", FEATURES))\n',
                          f'present = os.path.exists(os.path.join({VOLUME[:-1]!r}, FEATURES))\n'),
    "f-string root": ('files = glob.glob(f"/dbfs{FOLDER}/*.csv")\n',
                      f'files = glob.glob(f"{VOLUME[:-1]}{{FOLDER}}/*.csv")\n'),
    "f-string scheme": ('data = spark.read.csv(f"dbfs:{FOLDER}/x.csv")\n',
                        f'data = spark.read.csv(f"{VOLUME}{{FOLDER}}/x.csv")\n'),
}


@pytest.mark.parametrize("title", sorted(MARKERLESS_DBFS))
def test_a_dbfs_root_that_is_no_marker_meets_the_model(monkeypatch, tmp_path, title):
    source, translated = MARKERLESS_DBFS[title]
    reply = "# Oracle tool modification: fixture translation\n" + translated
    assert not jm._has_databricks_markers(source)
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([source]))
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append(content)
        return reply, {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    kernel = _SnippetKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture", "01_paths", {}))
    assert result["status"] == "PASS", result
    assert len(calls) == 1 and source in calls[0]
    assert _saved_cell_sources(kernel) == [_model_code(reply)]
    assert not any(source.strip() in code for code in kernel.executed), "the raw cell must not run"


# A pandas S3 read used to be wrapped in translate_path() before the model saw
# the cell. translate_path() maps no S3 bucket on the cluster (see
# test_runtime_translator_does_not_map_s3_without_configuration), so the wrapper
# changed nothing, while the prompt tells the model not to use it for S3.
PANDAS_S3_READS = [
    f'df = pd.{reader}("{scheme}://fixture-bucket/input/x.{ext}", columns=COLUMNS)\n'
    for reader, ext in (("read_csv", "csv"), ("read_parquet", "parquet"), ("read_json", "json"))
    for scheme in ("s3", "s3a")]


@pytest.mark.parametrize("mode", ["parent", "dependency"])
@pytest.mark.parametrize("source", PANDAS_S3_READS)
def test_a_pandas_s3_read_meets_the_model_as_written(monkeypatch, tmp_path, source, mode):
    reply = ("# Oracle tool modification: fixture translation\n"
             + re.sub(r"s3a?://fixture-bucket", "oci://oci-fixture-bucket@test_namespace", source))
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([source]))
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append(content.split("```python\n", 1)[1].rsplit("\n```", 1)[0])
        return reply, {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    kernel = _SnippetKernel()
    dependency = mode == "dependency"
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture",
        "dep_paths" if dependency else "01_paths", {}, run_all=not dependency,
        defer_filestore_assignments=dependency,
    ))
    assert result["status"] == ("CONVERTED_NOT_VALIDATED" if dependency else "PASS"), result
    # The model gets the cell as written: no translate_path() wrapper, no import.
    assert calls == [source]
    assert _saved_cell_sources(kernel) == [_model_code(reply)]


def test_a_pandas_s3_read_below_a_fired_child_exit_is_kept_as_written_and_reported(
        monkeypatch, tmp_path):
    tail = 'FEATURES = pd.read_csv("s3://fixture-bucket/input/f.csv")'
    result, saved, *_ = _run_child_exit(monkeypatch, tmp_path, {"child": [CONDITIONAL, tail]})
    assert saved["child"][1] == tail
    notes = [n for n in result.get("notes", []) if "UNRESOLVED storage paths" in n]
    assert len(notes) == 1 and "s3://" in notes[0], notes


@pytest.mark.parametrize("source,forms", [
    ('x = "s3a://bucket/key"\n', ["s3a://"]),
    ('x = read("/dbfs" + p)\n', ["/dbfs"]),
    ('x = f"dbfs:{p}"\n', ["dbfs:"]),
    ('x = "/mnt/raw/a"\n', ["/mnt/"]),
    # A translated Volume path and a comment are not storage paths left behind.
    ('x = "/Volumes/default/default/dbfs/FileStore/a.csv"\n', []),
    ('# read from /dbfs/FileStore/a.csv\nx = 1\n', []),
    ("%scala\nval x = \"s3://bucket/key\"\n", []),
])
def test_storage_path_forms(source, forms):
    assert jm._storage_path_forms(source) == forms


@pytest.mark.parametrize("order", ORDERS)
@pytest.mark.parametrize("mode", ["parent", "dependency"])
def test_a_prefix_constant_in_another_cell_survives_the_cell_loop(
        monkeypatch, tmp_path, order, mode):
    sources = _cross_cell_sources(order=order)
    _patch_cell_loop(monkeypatch, tmp_path, _notebook(sources))
    calls = []

    async def repair(*args, **kwargs):
        pytest.fail("a correct recognizer must not need repair")

    monkeypatch.setattr(jm, "call_opus_with_tools", _keeping_model(calls))
    monkeypatch.setattr(jm, "call_fix", repair)
    kernel = _SnippetKernel()
    dependency = mode == "dependency"
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture",
        "dep_paths" if dependency else "01_paths", {}, run_all=not dependency,
        defer_filestore_assignments=dependency,
    ))
    assert result['status'] == ('CONVERTED_NOT_VALIDATED' if dependency else 'PASS'), result
    # The cell with the S3 aliases met the model; nothing rewrote them.
    index = _constant_index(sources)
    assert calls == [sources[index]]
    saved = _saved_cell_sources(kernel)
    assert saved == _without_final_newlines(sources)
    _probe_saved_cells(saved)


@pytest.mark.parametrize("order", ORDERS)
def test_a_prefix_constant_below_a_fired_exit_meets_the_model_and_is_saved_as_returned(
        monkeypatch, tmp_path, order):
    sources = _cross_cell_sources(order=order)
    exit_cell = "if MIGRATION_TIME_FLAG:\n    aidputils.notebook.exit('done')\n"
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([exit_cell, *sources]))
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: False)
    calls = []
    monkeypatch.setattr(jm, "call_opus_with_tools", _keeping_model(calls))
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_cond", "01_task", {}))
    assert result["status"] == "PASS_CONDITIONAL_EXIT_COVERAGE", result
    # Only the cell with the S3 aliases met the model, code-only; nothing
    # rewrote the aliases it recognizes its input with.
    index = _constant_index(sources)
    assert calls == [sources[index]]
    saved = _saved_cell_sources(kernel)
    assert saved[1:] == _without_final_newlines(sources)
    _probe_saved_cells(saved[1:])
    report = _migration_report(kernel)
    assert f"Cell {1 + index}: converted by the model without running it" in report
    # Nothing ran the cell, so nothing tells a recognizer the model kept on
    # purpose (these aliases) from a destination it missed: the report names
    # the forms without calling them unconverted.
    assert (f"Cell {1 + index}: UNRESOLVED storage paths — the model's code still contains "
            "S3/DBFS path forms s3://, s3a://, s3n://") in report
    assert "kept as in the source" not in report


CHILD_GOLD = "oci://oci-test-gold@fixture_namespace/"


def _seed_child_mapping(monkeypatch):
    import context_tools

    # The OCI literal is namespace-checked against the shared mapping index,
    # which earlier tests may leave empty.
    monkeypatch.setattr(context_tools, "_BUCKET_NS_INDEX", {"oci-test-gold": "fixture_namespace"})


@pytest.mark.parametrize("order", ORDERS)
def test_a_prefix_constant_below_a_fired_child_exit_is_kept_and_reported(monkeypatch, tmp_path, order):
    _seed_child_mapping(monkeypatch)
    sources = _cross_cell_sources(CHILD_GOLD, order)
    result, saved, *_ = _run_child_exit(monkeypatch, tmp_path, {"child": [CONDITIONAL, *sources]})
    assert result["exit_value"] == "exit_fixture"
    assert saved["child"][1:] == sources
    _probe_saved_cells(saved["child"][1:], CHILD_GOLD)
    index = 1 + _constant_index(sources)
    assert any(note.startswith(f"child.ipynb cell {index}: UNRESOLVED storage paths")
               for note in result["notes"]), result["notes"]


@pytest.mark.parametrize("tail,reported", [
    ('FEATURES = pd.read_csv("/dbfs" + PATH)\n', True),
    ('FEATURES = pd.read_csv("/Volumes/default/default/dbfs/FileStore/f.csv")\n', False),
])
def test_a_child_tail_reports_dbfs_paths_left_raw_and_only_those(monkeypatch, tmp_path, tail, reported):
    result, saved, *_ = _run_child_exit(monkeypatch, tmp_path, {"child": [CONDITIONAL, tail]})
    assert saved["child"][1] == tail.rstrip("\n") or saved["child"][1] == tail
    notes = [n for n in result.get("notes", []) if "UNRESOLVED storage paths" in n]
    assert bool(notes) is reported, notes


def test_a_child_tail_keeps_a_migrated_dependency_byte_for_byte(monkeypatch, tmp_path):
    # Inline children execute the migrated dependency, so its tail is
    # converted a second time. The model's translations, f-string S3
    # recognizer included, must come through unchanged.
    _seed_child_mapping(monkeypatch)
    migrated = [CONDITIONAL, SAVE_TRANSLATED,
                _model_gold_reply("normalize").replace(GOLD, CHILD_GOLD)]
    _, saved, *_ = _run_child_exit(monkeypatch, tmp_path, {"child": migrated})
    assert saved["child"] == migrated


@pytest.mark.parametrize("transform", ["works", "raises"])
@pytest.mark.parametrize("order", ORDERS)
def test_cells_after_a_failed_cell_are_kept_and_reported(monkeypatch, tmp_path, order, transform):
    sources = _cross_cell_sources(order=order)
    failing = 'raise RuntimeError("upstream data missing")\n'
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([failing, *sources]))
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: False)
    if transform == "raises":
        # A code-only transform that raises leaves the cell as it is; its
        # storage paths are reported all the same.
        add_missing_imports = jm._add_missing_imports

        def raising(code, *args, **kwargs):
            if "GOLD_BUCKET_LEGACY_PREFIXES" in code or "def resolve_path" in code:
                raise ValueError("fixture transform failure")
            return add_missing_imports(code, *args, **kwargs)

        monkeypatch.setattr(jm, "_add_missing_imports", raising)

    class FailingKernel(_SnippetKernel):
        async def execute(self, code, **kwargs):
            if "upstream data missing" in code and "base64.b64decode" not in code:
                self.executed.append(code)
                return {"status": "error", "outputs": [{
                    "type": "error", "ename": "RuntimeError", "evalue": "upstream data missing",
                    "traceback": ["RuntimeError: upstream data missing"]}]}
            return await super().execute(code, **kwargs)

    async def repair(*args, **kwargs):
        raise RuntimeError("fix step unavailable")

    monkeypatch.setattr(jm, "call_fix", repair)
    kernel = FailingKernel()
    asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_cond", "01_task", {}))
    assert not any("def resolve_path" in c for c in kernel.executed if "base64.b64decode" not in c)
    saved = _saved_cell_sources(kernel)
    assert saved[1:] == _without_final_newlines(sources)
    _probe_saved_cells(saved[1:])
    index = 1 + _constant_index(sources)
    report = _migration_report(kernel)
    assert f"Cell {index}: UNRESOLVED storage paths" in report
    assert "not executed: the task failed at cell 0" in report


@pytest.mark.parametrize("mode", ["parent", "dependency"])
def test_the_models_save_table_utils_translation_is_saved_unchanged(monkeypatch, tmp_path, mode):
    # Model_Monitoring SaveTableHelpers: the model translates the metastore
    # location and keeps an f-string S3 recognizer, which the literal sweep
    # used to rewrite (run cd189909).
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([SAVE_FSTRING]))
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append(content)
        return SAVE_TRANSLATED, {"input": 0, "output": 0}

    async def repair(*args, **kwargs):
        pytest.fail("the translated cell must not need repair")

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    monkeypatch.setattr(jm, "call_fix", repair)
    kernel = _SnippetKernel()
    dependency = mode == "dependency"
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture",
        "dep_paths" if dependency else "01_paths", {}, run_all=not dependency,
        defer_filestore_assignments=dependency,
    ))
    assert result['status'] == ('CONVERTED_NOT_VALIDATED' if dependency else 'PASS'), result
    assert calls, "the original cell carries S3 markers and must meet the model"
    saved = next(s for s in _saved_cell_sources(kernel) if "def save(" in s)
    assert saved == _model_code(SAVE_TRANSLATED)

    def save(location):
        namespace = {"spark": _Spark(location)}
        exec(saved, namespace)
        return namespace["save"]("ds.t")

    gold = "oci://oci-example-gold@test_namespace/tables/ds/t/"
    for location in ("s3://example-gold/tables/ds/t/", "s3a://example-gold/tables/ds/t/",
                     "s3n://example-gold/tables/ds/t/", gold):
        assert save(location) == ("written", gold)
    with pytest.raises(ValueError, match="Write blocked"):
        save("s3://other/tables/t/")


def _run_review_shape(code, function, value):
    written = []
    namespace = {"writer": written.append}
    exec(code, namespace)
    try:
        return namespace[function](value), written
    except ValueError as exc:
        return str(exc), written


def _run_with_model_reply(monkeypatch, tmp_path, source, reply):
    _patch_cell_loop(monkeypatch, tmp_path, _notebook([source]))
    calls = []

    async def migrate(prompt, content, **kwargs):
        calls.append(content)
        return reply, {"input": 0, "output": 0}

    async def repair(*args, **kwargs):
        pytest.fail("a cell that only defines functions must not need repair")

    monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
    monkeypatch.setattr(jm, "call_fix", repair)
    kernel = _SnippetKernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "fixture", "01_paths", {}))
    assert result["status"] == "PASS", result
    assert len(calls) == 1
    return _saved_cell_sources(kernel)


@pytest.mark.parametrize("title", sorted(REVIEW_SHAPES))
def test_review_shapes_are_saved_as_the_model_returns_them(monkeypatch, tmp_path, title):
    # PR #243 reviews: a check on a value something else still holds. The
    # tool never rewrites the check, so the saved cell behaves exactly as the
    # model's reply.
    source, function, value = REVIEW_SHAPES[title]
    saved = _run_with_model_reply(monkeypatch, tmp_path, source, source)
    assert saved == [_model_code(source)]
    assert _run_review_shape(saved[0], function, value) == _run_review_shape(source, function, value)


def test_a_dict_held_alias_writes_what_the_models_code_writes(monkeypatch, tmp_path):
    # Third review (B1): with the check rewritten, `held["raw"]` kept the
    # unconverted location and reached the writer.
    saved = _run_with_model_reply(monkeypatch, tmp_path, B1_LOCATED, B1_LOCATED)
    assert saved == [_model_code(B1_LOCATED)]

    def written(code, location):
        paths = []
        writer = type("Writer", (), {"mode": lambda self, mode: self,
                                     "save": lambda self, path: paths.append(path)})()
        namespace = {"spark": _Spark(location)}
        exec(code, namespace)
        namespace["append_gold"](type("Frame", (), {"write": writer})(), "ds.t")
        return paths

    for location in ("s3://example-gold/tbl/part1", "oci://oci-example-gold@test_namespace/tbl/part1"):
        assert written(saved[0], location) == written(B1_LOCATED, location)
