"""REF-158: a raw /dbfs path a cell actually used reaches the model.

Model replies are saved as returned. A raw /dbfs path the model leaves works
in the migration kernel only through aidp_compat's open() redirect, or answers
False / [] in os.path and glob checks, and the saved notebook has that
redirect only if it imports aidp_compat itself. The migration kernel reports
such a path when the code uses it; the report is moved out of the cell output
and given to the output check and the fix loop, which decide. Nothing rewrites
the code.
"""
import asyncio
import builtins
import contextlib
import copy
import glob
import inspect
import io
import json
import os
import pickle
import subprocess
import sys
import traceback
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm  # noqa: E402
import test_child_exit_conversion as child_harness  # noqa: E402
from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources  # noqa: E402

MARKER = "__AIDP_RAW_DBFS__"
VOLUME = "/Volumes/default/default/dbfs/"


def _markers(text):
    return [json.loads(line[len(MARKER):]) for line in text.splitlines() if line.startswith(MARKER)]


# ── The migration-kernel recorder ───────────────────────────────────────────

def _volume(tmp_path):
    volume = tmp_path / "Volumes"
    (volume / "FileStore").mkdir(parents=True)
    (volume / "FileStore" / "a.csv").write_text("x\n")
    return volume


def _fake_aidp_compat(volume):
    fake = types.ModuleType("aidp_compat")

    def translate_path(path):
        for prefix in ("/dbfs/", "dbfs:/"):
            if path.startswith(prefix):
                return str(volume / path[len(prefix):])
        return path

    fake.translate_path = translate_path
    return fake


def _restore_after_test(monkeypatch):
    """The recorder patches process-wide functions; put every one back."""
    for owner, name in ((builtins, "open"), (os.path, "exists"), (os.path, "isfile"),
                        (os.path, "isdir"), (os, "listdir"), (glob, "glob"), (glob, "iglob")):
        monkeypatch.setattr(owner, name, getattr(owner, name))
    monkeypatch.setattr(builtins, "_aidp__raw_dbfs_recorder__", False, raising=False)


def _local_pattern(tmp_path):
    """Three local files to iterate over; none of them is a /dbfs path."""
    local = tmp_path / "local"
    local.mkdir()
    for name in ("one.txt", "two.txt", "three.txt"):
        (local / name).write_text(name)
    return str(local / "*.txt")


@pytest.fixture
def recorder(monkeypatch, tmp_path):
    """Install the recorder in this process the way the bootstrap installs it
    in the migration kernel, with translate_path pointing at a local Volume."""
    volume = _volume(tmp_path)
    monkeypatch.setitem(sys.modules, "aidp_compat", _fake_aidp_compat(volume))
    _restore_after_test(monkeypatch)
    namespace = {"open": builtins.open}
    exec(jm._RAW_DBFS_RECORDER_BOOTSTRAP, namespace)
    return namespace, volume


def test_a_raw_path_that_only_exists_under_the_volume_is_reported(recorder, capsys):
    namespace, volume = recorder
    target = str(volume / "FileStore" / "a.csv")
    # Every call still answers exactly what it answers without the recorder.
    assert os.path.exists("/dbfs/FileStore/a.csv") is False
    assert os.path.isfile(Path("/dbfs/FileStore/a.csv")) is False
    assert os.path.isdir("/dbfs/FileStore") is False
    assert glob.glob("/dbfs/FileStore/*.csv") == []
    with pytest.raises(FileNotFoundError):
        os.listdir("/dbfs/FileStore")
    with pytest.raises(FileNotFoundError):
        namespace["open"]("/dbfs/FileStore/a.csv")
    with pytest.raises(FileNotFoundError):
        namespace["open"]("/dbfs/FileStore/new.csv", "w")  # a write: its Volume folder exists
    assert _markers(capsys.readouterr().out) == [
        ["os.path.exists", "/dbfs/FileStore/a.csv", target],
        ["os.path.isfile", "/dbfs/FileStore/a.csv", target],
        ["os.path.isdir", "/dbfs/FileStore", str(volume / "FileStore")],
        ["glob", "/dbfs/FileStore/*.csv", str(volume / "FileStore" / "*.csv")],
        ["os.listdir", "/dbfs/FileStore", str(volume / "FileStore")],
        ["open", "/dbfs/FileStore/a.csv", target],
        ["open", "/dbfs/FileStore/new.csv", str(volume / "FileStore" / "new.csv")],
    ]


def test_other_paths_are_not_reported(recorder, capsys, tmp_path):
    namespace, volume = recorder
    assert os.path.exists("/dbfs/FileStore/missing.csv") is False   # no Volume copy either
    assert os.path.exists(str(tmp_path)) is True
    with namespace["open"](str(volume / "FileStore" / "a.csv")) as handle:
        assert handle.read() == "x\n"
    assert isinstance(os.listdir(), list)                          # no path argument
    assert glob.glob(str(volume / "FileStore" / "*.csv")) == [str(volume / "FileStore" / "a.csv")]
    assert _markers(capsys.readouterr().out) == []


def test_a_raw_path_that_exists_itself_is_not_reported(recorder, capsys, monkeypatch, tmp_path):
    # Where the raw path answers too (a /dbfs mount or a compatibility link),
    # the code works as it is: nothing is reported, whatever the Volume holds.
    # Here a relative dbfs: path exists below the working folder.
    cwd = tmp_path / "cwd"
    (cwd / "dbfs:" / "FileStore").mkdir(parents=True)
    (cwd / "dbfs:" / "FileStore" / "a.csv").write_text("x\n")
    monkeypatch.chdir(cwd)
    assert os.path.exists("dbfs:/FileStore/a.csv") is True
    assert os.path.exists("/dbfs/FileStore/a.csv") is False  # the control: missing, so reported
    assert [marker[1] for marker in _markers(capsys.readouterr().out)] == ["/dbfs/FileStore/a.csv"]


def test_the_open_note_says_only_what_the_recorder_saw(recorder, capsys):
    # open() is reported before it runs, when its raw path is missing and its
    # Volume location, or that location's folder (a file about to be written),
    # exists. Here the folder exists, the file does not, and open() raises.
    namespace, volume = recorder
    with pytest.raises(FileNotFoundError):
        namespace["open"]("/dbfs/FileStore/missing.txt")
    target = str(volume / "FileStore" / "missing.txt")
    events = [tuple(marker) for marker in _markers(capsys.readouterr().out)]
    assert events == [("open", "/dbfs/FileStore/missing.txt", target)]
    note = jm._raw_dbfs_note(events)
    assert f"- open('/dbfs/FileStore/missing.txt'): the raw path is absent; its translated " in note
    assert target in note
    assert "does not prove that the file loaded" in note
    assert "not proof that the saved notebook lacks the redirect" in note



def test_a_call_from_another_thread_is_reported(recorder, capsys):
    import threading
    worker = threading.Thread(target=lambda: os.path.exists("/dbfs/FileStore/a.csv"))
    worker.start()
    worker.join()
    assert len(_markers(capsys.readouterr().out)) == 1


def test_a_path_argument_the_recorder_cannot_read_fails_as_it_would_without_it(recorder, capsys):
    class Unreadable(os.PathLike):
        def __fspath__(self):
            raise RuntimeError("fixture path")

    with pytest.raises(RuntimeError, match="fixture path"):
        os.path.exists(Unreadable())
    assert _markers(capsys.readouterr().out) == []


def test_the_recorder_installs_once(recorder, capsys):
    namespace, _ = recorder
    exec(jm._RAW_DBFS_RECORDER_BOOTSTRAP, namespace)
    os.path.exists("/dbfs/FileStore/a.csv")
    assert len(_markers(capsys.readouterr().out)) == 1


# A glob.iglob iterator hands items back to the cell's code one at a time: the
# calls that code makes between two items are the cell's, and are reported.

def test_a_check_between_the_items_of_a_paused_iterator_is_reported(recorder, capsys, tmp_path):
    files = glob.iglob(_local_pattern(tmp_path))
    next(files)
    assert os.path.exists("/dbfs/FileStore/a.csv") is False
    next(files)
    assert os.path.isfile("/dbfs/FileStore/a.csv") is False
    assert [marker[0] for marker in _markers(capsys.readouterr().out)] == ["os.path.exists", "os.path.isfile"]


def test_a_check_on_every_pass_of_a_loop_over_iglob_is_reported(recorder, capsys, tmp_path):
    seen = []
    for _ in glob.iglob(_local_pattern(tmp_path)):
        seen.append(os.path.exists("/dbfs/FileStore/a.csv"))
    assert seen == [False, False, False]
    assert len(_markers(capsys.readouterr().out)) == 3


def test_a_check_in_a_later_cell_while_an_iterator_stays_paused_is_reported(recorder, capsys, tmp_path):
    namespace, _ = recorder
    namespace["LOCAL_PATTERN"] = _local_pattern(tmp_path)
    exec("import glob\nfiles = glob.iglob(LOCAL_PATTERN)\nfirst = next(files)\n", namespace)  # one cell
    exec("import os\npresent = os.path.exists('/dbfs/FileStore/a.csv')\n", namespace)        # a later one
    assert namespace["present"] is False
    assert len(_markers(capsys.readouterr().out)) == 1


def test_iterating_a_raw_pattern_reports_it_once(recorder, capsys):
    # glob's own calls while it advances (os.path.isdir on the raw folder for **)
    # are not the cell's: only the call itself is reported.
    _, volume = recorder
    assert list(glob.iglob("/dbfs/FileStore/*.csv")) == []
    assert list(glob.iglob("/dbfs/FileStore/**", recursive=True)) == []
    assert _markers(capsys.readouterr().out) == [
        ["glob", "/dbfs/FileStore/*.csv", str(volume / "FileStore" / "*.csv")],
        ["glob", "/dbfs/FileStore/**", str(volume / "FileStore" / "**")],
    ]


def test_an_iterator_closed_on_another_thread_leaves_the_recorder_working(recorder, capsys, tmp_path):
    import threading
    files = glob.iglob(_local_pattern(tmp_path))
    next(files)
    errors = []

    def close():
        try:
            files.close()
        except Exception as exc:  # anything the recorder raises would reach the cell
            errors.append(exc)

    worker = threading.Thread(target=close)
    worker.start()
    worker.join()
    assert errors == []
    assert os.path.exists("/dbfs/FileStore/a.csv") is False
    assert len(_markers(capsys.readouterr().out)) == 1


# A path passed by keyword, under the name the function itself declares
# (os.path.isdir's is "s"), is the same path as a positional one.

@pytest.mark.parametrize("call, raw", [
    ("open", "/dbfs/FileStore/a.csv"),
    ("os.path.exists", "/dbfs/FileStore/a.csv"),
    ("os.path.isfile", "/dbfs/FileStore/a.csv"),
    ("os.path.isdir", "/dbfs/FileStore"),
    ("os.listdir", "/dbfs/FileStore"),
    ("glob.glob", "/dbfs/FileStore/*.csv"),
    ("glob.iglob", "/dbfs/FileStore/*.csv"),
])
def test_a_keyword_call_is_reported_like_a_positional_one(recorder, capsys, call, raw):
    namespace, _ = recorder
    function = {"open": namespace["open"], "os.path.exists": os.path.exists,
                "os.path.isfile": os.path.isfile, "os.path.isdir": os.path.isdir,
                "os.listdir": os.listdir, "glob.glob": glob.glob, "glob.iglob": glob.iglob}[call]
    keyword = next(iter(inspect.signature(function).parameters))
    for use in (lambda: function(raw), lambda: function(**{keyword: raw})):
        try:
            answer = use()
        except FileNotFoundError:
            continue
        if call == "glob.iglob":
            list(answer)
        elif call == "open":
            answer.close()
    markers = _markers(capsys.readouterr().out)
    assert len(markers) == 2 and markers[0] == markers[1], (keyword, markers)
    assert markers[0][1] == raw


def test_a_raw_root_dir_is_reported_with_the_pattern_below_it(recorder, capsys):
    _, volume = recorder
    assert glob.glob("*.csv", root_dir="/dbfs/FileStore") == []
    assert list(glob.iglob("*.csv", root_dir=Path("/dbfs/FileStore"))) == []
    # An absolute pattern does not use root_dir, in glob or here.
    assert glob.glob(str(volume / "FileStore" / "*.csv"), root_dir="/dbfs/FileStore") == [
        str(volume / "FileStore" / "a.csv")]
    target = str(volume / "FileStore" / "*.csv")
    assert _markers(capsys.readouterr().out) == [
        ["glob", "/dbfs/FileStore/*.csv", target],
        ["glob", "/dbfs/FileStore/*.csv", target],
    ]


# glob is probed the way the call itself looks: with its own recursive and
# include_hidden options. Each Volume copy holds one CSV that only those
# options find.
GLOB_OPTIONS = {
    "recursive": ("deep/year/month/deep.csv", "**/*.csv", {"recursive": True}),
    "include_hidden": ("hidden/.only.csv", "*.csv", {"include_hidden": True}),
    "both": ("both/.dir/sub/deep.csv", "**/*.csv", {"recursive": True, "include_hidden": True}),
    "none": ("plain/top.csv", "*.csv", {}),
}


@pytest.mark.parametrize("function", ["glob", "iglob"])
@pytest.mark.parametrize("form", ["absolute", "root_dir"])
@pytest.mark.parametrize("case", list(GLOB_OPTIONS))
def test_glob_is_probed_with_the_calls_own_options(recorder, capsys, function, form, case):
    _, volume = recorder
    layout, pattern, options = GLOB_OPTIONS[case]
    folder = layout.split("/", 1)[0]
    (volume / layout).parent.mkdir(parents=True)
    (volume / layout).write_text("x\n")
    call = getattr(glob, function)
    if form == "absolute":
        answer = call(f"/dbfs/{folder}/{pattern}", **options)
    else:
        answer = call(pattern, root_dir=f"/dbfs/{folder}", **options)
    assert list(answer) == []
    assert _markers(capsys.readouterr().out) == [
        ["glob", f"/dbfs/{folder}/{pattern}", str(volume / folder / pattern)]]


def test_a_bytes_path_is_reported_like_a_text_one(recorder, capsys):
    _, volume = recorder
    assert os.path.exists(b"/dbfs/FileStore/a.csv") is False
    assert glob.glob(b"*.csv", root_dir=b"/dbfs/FileStore") == []
    assert _markers(capsys.readouterr().out) == [
        ["os.path.exists", "/dbfs/FileStore/a.csv", str(volume / "FileStore" / "a.csv")],
        ["glob", "/dbfs/FileStore/*.csv", str(volume / "FileStore" / "*.csv")],
    ]


# ── Functions Spark ships to its workers ────────────────────────────────────
# Spark sends a Python function to its workers (a UDF, mapPartitions,
# foreachPartition, applyInPandas) by pickling it with cloudpickle, together
# with what it refers to: `open`, or a name imported from os.path, os or glob,
# is then a recorded call. A worker is a fresh process. A recorded call pickles
# as the call it wraps, found again by its import name, so a worker gets the
# original and the recorder, with its per-thread state, stays in the kernel.

REPO_ROOT = str(Path(__file__).resolve().parents[1])
# How a fresh process calls each recorded call on the Volume copy of FileStore/a.csv.
WORKER_CALLS = {
    "open": "call(PATH).read()",
    "os.path.exists": "call(PATH)",
    "os.path.isfile": "call(PATH)",
    "os.path.isdir": "call(FOLDER)",
    "os.listdir": "sorted(call(FOLDER))",
    "glob.glob": "call(FOLDER + '/*.csv')",
    "glob.iglob": "list(call(FOLDER + '/*.csv'))",
}
UNPICKLE = '''
import builtins, pickle, pkgutil, sys
call = pickle.loads(sys.stdin.buffer.read())
FOLDER, PATH, NAME, EXPR = sys.argv[1:5]
print(getattr(builtins, "_aidp__raw_dbfs_recorder__", False), call is pkgutil.resolve_name(NAME),
      repr(eval(EXPR)))
'''


def _recorded_calls(namespace):
    return {"open": namespace["open"], "os.path.exists": os.path.exists,
            "os.path.isfile": os.path.isfile, "os.path.isdir": os.path.isdir,
            "os.listdir": os.listdir, "glob.glob": glob.glob, "glob.iglob": glob.iglob}


def _in_a_fresh_process(script, payload, *args):
    # A new interpreter, as a worker is: no recorder, its own modules.
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1",
               PYTHONPATH=os.pathsep.join(p for p in (REPO_ROOT, os.environ.get("PYTHONPATH")) if p))
    out = subprocess.run([sys.executable, "-c", script, *args], input=payload,
                         capture_output=True, timeout=120, env=env)
    assert out.returncode == 0, out.stderr.decode(errors="replace")[-3000:]
    return out.stdout.decode().strip()


@pytest.mark.parametrize("name", list(WORKER_CALLS))
def test_a_recorded_call_pickles_as_the_call_it_wraps(recorder, name):
    namespace, volume = recorder
    call = _recorded_calls(namespace)[name]
    original = call.__wrapped__
    folder, path = str(volume / "FileStore"), str(volume / "FileStore" / "a.csv")
    expected = {"open": "x\n", "os.path.exists": True, "os.path.isfile": True,
                "os.path.isdir": True, "os.listdir": ["a.csv"], "glob.glob": [path],
                "glob.iglob": [path]}[name]
    # Copies keep the recorded call itself, as they keep a function.
    assert copy.copy(call) is call and copy.deepcopy(call) is call
    out = _in_a_fresh_process(UNPICKLE, pickle.dumps(call), folder, path,
                              f"{original.__module__}:{original.__qualname__}", WORKER_CALLS[name])
    assert out == f"False True {expected!r}"


# A kernel with the real aidp_compat, whose open() shim the recorder wraps.
SHIM_KERNEL = '''
import ast, io, pickle, sys
import aidp_compat  # installs the open() shim, as the migration kernel's bootstrap does
source = io.open(sys.argv[1] + "/scripts/job_migrate.py", encoding="utf-8").read()
bootstrap = next(ast.literal_eval(node.value) for node in ast.parse(source).body
                 if isinstance(node, ast.Assign)
                 and getattr(node.targets[0], "id", None) == "_RAW_DBFS_RECORDER_BOOTSTRAP")
exec(bootstrap, globals())
assert getattr(open, "__wrapped__", None) is aidp_compat.dbfs_open_hook._dbfs_aware_open
with io.open(sys.argv[2], "wb") as fh:
    fh.write(pickle.dumps(open))
'''
SHIM_WORKER = '''
import pickle, sys
call = pickle.loads(sys.stdin.buffer.read())
import aidp_compat.dbfs_open_hook as hook
print(call is hook._dbfs_aware_open)
'''


def test_a_call_not_found_by_its_import_name_leaves_nothing_installed(monkeypatch, tmp_path, capsys):
    # A wrapper that could not be pickled as its original is never installed:
    # the recorder reports that it failed, and every call stays as it was.
    monkeypatch.setitem(sys.modules, "aidp_compat", _fake_aidp_compat(_volume(tmp_path)))
    _restore_after_test(monkeypatch)
    monkeypatch.setattr(glob, "iglob", lambda pathname, **options: iter(()))  # the last one wrapped
    before = {name: call for name, call in _recorded_calls({"open": builtins.open}).items()}
    namespace = {"open": builtins.open}
    exec(jm._RAW_DBFS_RECORDER_BOOTSTRAP, namespace)
    assert capsys.readouterr().out.startswith(jm._RAW_DBFS_RECORDER_FAILED + " ")
    assert _recorded_calls(namespace) == before
    assert getattr(builtins, "_aidp__raw_dbfs_recorder__", False) is False


def test_open_pickles_as_the_aidp_compat_shim_the_recorder_wraps(tmp_path):
    payload = tmp_path / "open.pickle"
    _in_a_fresh_process(SHIM_KERNEL, b"", REPO_ROOT, str(payload))
    assert _in_a_fresh_process(SHIM_WORKER, payload.read_bytes()) == "True"


WORKER = '''
from glob import glob, iglob
from os import listdir
from os.path import exists, isdir, isfile


def worker(folder, name):
    path = folder + "/" + name
    with open(path) as fh:
        text = fh.read()
    return [text, exists(path), isfile(path), isdir(folder), sorted(listdir(folder)),
            glob(folder + "/*.csv"), list(iglob(folder + "/*.csv"))]
'''
RUN_SHIPPED = '''
import builtins, pickle, sys
worker = pickle.loads(sys.stdin.buffer.read())
print(getattr(builtins, "_aidp__raw_dbfs_recorder__", False), repr(worker(sys.argv[1], "a.csv")))
'''


def test_a_function_spark_ships_to_its_workers_pickles_and_runs_there(recorder):
    cloudpickle = pytest.importorskip("cloudpickle", reason="Spark's function serializer; CI installs it")
    namespace, volume = recorder
    exec(WORKER, namespace)
    folder = str(volume / "FileStore")
    out = _in_a_fresh_process(RUN_SHIPPED, cloudpickle.dumps(namespace["worker"]), folder)
    path = folder + "/a.csv"
    assert out == "False " + repr(["x\n", True, True, True, ["a.csv"], [path], [path]])


# ── The reports leave the cell output ───────────────────────────────────────

def _report(call, raw, target):
    return MARKER + json.dumps([call, raw, target]) + "\n"


def test_reports_leave_the_cell_output_and_reach_the_result():
    report = _report("open", "/dbfs/FileStore/a.csv", VOLUME + "FileStore/a.csv")
    result = {"status": "ok", "outputs": [
        {"type": "stream", "name": "stdout", "text": "rows 3\n" + report + "more\n" + report}]}
    logs = []
    jm._extract_raw_dbfs_events(result, logs.append)
    assert result["outputs"][0]["text"] == "rows 3\nmore\n"
    assert result["raw_dbfs"] == [("open", "/dbfs/FileStore/a.csv", VOLUME + "FileStore/a.csv")]
    assert any("/dbfs/FileStore/a.csv" in line for line in logs)


def test_a_malformed_report_is_kept_and_logged():
    text = MARKER + "[1, 2]\n"
    result = {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}
    logs = []
    jm._extract_raw_dbfs_events(result, logs.append)
    assert result["outputs"][0]["text"] == text
    assert "raw_dbfs" not in result
    assert any("UNRESOLVED" in line for line in logs)


def test_every_execution_moves_the_reports_out():
    class Session:
        async def execute(self, code, **kwargs):
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                    "text": "ok\n" + _report("os.path.exists", "/dbfs/x", VOLUME + "x")}]}

    result = asyncio.run(jm._safe_execute(Session(), "x = 1", tag="cell", log_fn=lambda m: None))
    assert result["outputs"][0]["text"] == "ok\n"
    assert result["raw_dbfs"] == [("os.path.exists", "/dbfs/x", VOLUME + "x")]


# ── The cell loop: the output check and the fix loop get the report ─────────

RAW_READ = 'df = pd.read_csv("/dbfs/FileStore/a.csv")  # SENTINEL_RAW_DBFS\n'
TRANSLATED_READ = 'df = pd.read_csv(translate_path("/dbfs/FileStore/a.csv"))  # SENTINEL_RAW_DBFS\n'
RAW_FAILING = 'model = pickle.load(open("/dbfs/FileStore/model.pkl", "rb"))  # SENTINEL_RAW_FAIL\n'
# Prints nothing itself: the output check normally skips a cell with no output.
RAW_QUIET = 'present = os.path.exists("/dbfs/FileStore/model.pkl")  # SENTINEL_RAW_QUIET\n'


class _DbfsKernel(_Kernel):
    async def execute(self, code, **kwargs):
        if "SENTINEL_RAW_DBFS" in code:
            self.executed.append(code)
            text = "rows read: (120, 8)\n"
            if "translate_path" not in code:
                text += _report("open", "/dbfs/FileStore/a.csv", VOLUME + "FileStore/a.csv")
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}
        if "SENTINEL_RAW_QUIET" in code:
            self.executed.append(code)
            text = "" if "translate_path" in code else _report(
                "os.path.exists", "/dbfs/FileStore/model.pkl", VOLUME + "FileStore/model.pkl")
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}
        if "SENTINEL_RAW_FAIL" in code:
            self.executed.append(code)
            if "translate_path" in code:
                return {"status": "ok", "outputs": []}
            return {"status": "error", "outputs": [
                {"type": "stream", "name": "stdout",
                 "text": _report("open", "/dbfs/FileStore/model.pkl", VOLUME + "FileStore/model.pkl")},
                {"type": "error", "ename": "FileNotFoundError",
                 "evalue": "[Errno 2] No such file or directory: '/dbfs/FileStore/model.pkl'",
                 "traceback": ["Traceback (most recent call last):",
                               "FileNotFoundError: [Errno 2] No such file or directory"]}]}
        return await super().execute(code, **kwargs)


def _run_cell(monkeypatch, tmp_path, source, eval_replies, fixed):
    return _run_notebook(monkeypatch, tmp_path, [source], eval_replies, fixed, _DbfsKernel())


def _run_notebook(monkeypatch, tmp_path, sources, eval_replies, fixed, kernel):
    notebook = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                           "outputs": [], "execution_count": None} for source in sources],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 2)
    replies = iter(eval_replies)
    evals, fixes = [], []

    async def model(system, user_content, *a, **k):
        if system.startswith("You are verifying a migrated Spark cell"):
            evals.append(user_content)
            return next(replies), {"input": 0, "output": 0}
        # The migration call: a model that leaves the raw /dbfs path in its reply.
        assert system == jm.CELL_MIGRATE_PROMPT, system[:80]
        return user_content.split("```python\n", 1)[1].rsplit("\n```", 1)[0], {"input": 0, "output": 0}

    async def fix(code, error, context, attempt, extra_context="", **k):
        fixes.append({"code": code, "error": error, "context": extra_context})
        return fixed

    monkeypatch.setattr(jm, "call_opus_with_tools", model)
    monkeypatch.setattr(jm, "call_fix", fix)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_raw_dbfs", "01_task", {}, run_all=True))
    return result, kernel, evals, fixes


def _report_text(kernel):
    return next(text for path, text in kernel.files.items() if path.endswith("migration_report.md"))


def test_a_cell_that_used_a_raw_path_gets_the_report_and_is_fixed(monkeypatch, tmp_path):
    result, kernel, evals, fixes = _run_cell(
        monkeypatch, tmp_path, RAW_READ,
        ["ISSUE: the read relies on the migration-time redirect | FIX: use translate_path()",
         "PASS: the read uses the Volume"], TRANSLATED_READ)
    assert result["status"] == "PASS", result
    assert "Raw DBFS paths used at runtime" in evals[0]
    assert "/dbfs/FileStore/a.csv" in evals[0] and VOLUME + "FileStore/a.csv" in evals[0]
    assert MARKER not in evals[0], "the report must not stay in the cell output"
    assert len(fixes) == 1 and "Raw DBFS paths used at runtime" in fixes[0]["context"]
    assert "Raw DBFS paths used at runtime" not in evals[1]
    saved, _ = _saved_sources(kernel)
    assert "translate_path(" in saved


def test_without_a_report_the_output_check_gets_what_it_got_before(monkeypatch, tmp_path):
    result, _, evals, fixes = _run_cell(
        monkeypatch, tmp_path, TRANSLATED_READ, ["PASS: the read uses the Volume"], TRANSLATED_READ)
    assert result["status"] == "PASS", result
    assert "Raw DBFS paths" not in evals[0]
    assert fixes == []


def test_a_cell_the_output_check_accepts_despite_a_report_is_noted(monkeypatch, tmp_path):
    result, kernel, _, fixes = _run_cell(
        monkeypatch, tmp_path, RAW_READ,
        ["PASS: the notebook imports aidp_compat itself, so the redirect exists in the job"], RAW_READ)
    assert result["status"] == "PASS", result
    assert fixes == []
    report = _report_text(kernel)
    assert "Cell 0: used raw /dbfs paths at runtime" in report
    assert "the output check accepted the cell" in report


@pytest.mark.parametrize("setup", ["before", "current", "after", "bootstrap_only", "conditional"])
@pytest.mark.parametrize("operation", ["open", "exists", "glob"])
def test_raw_path_verification_gets_ordered_saved_setup_not_a_bootstrap_verdict(
        monkeypatch, tmp_path, setup, operation):
    # c36b0e22: a successful pandas pickle read was rejected as bootstrap-only
    # without checking the saved caller's earlier aidp_compat import. These are
    # context-wiring assertions; mocked verdicts do not certify model semantics.
    import_cell = "from aidp_compat import translate_path  # DURABLE_SETUP_SENTINEL\n"
    earlier = import_cell if setup == "before" else (
        "if ENABLE_COMPAT:\n    " + import_cell if setup == "conditional" else "setup_value = 1\n")
    read = RAW_READ if operation == "open" else RAW_QUIET
    if operation == "glob":
        read = 'files = glob.glob("/dbfs/FileStore/*.pkl")  # SENTINEL_RAW_QUIET\n'
    if setup == "current":
        read = import_cell + read
    sources = [earlier, read]
    if setup == "after":
        sources.append(import_cell)
    class SetupKernel(_DbfsKernel):
        async def execute(self, code, **kwargs):
            if "SENTINEL_RAW_" in code and "base64" not in code:
                self.executed.append(code)
                call = {"open": "open", "exists": "os.path.exists", "glob": "glob"}[operation]
                text = "rows read: (120, 8)\n" + _report(
                    call, "/dbfs/FileStore/a.pkl", VOLUME + "FileStore/a.pkl")
                return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}
            return await super().execute(code, **kwargs)

    # Keep the negative verdict closed, with an unchanged repair, so both eval
    # and fix contexts can be inspected without inventing a repair policy.
    result, _, evals, fixes = _run_notebook(
        monkeypatch, tmp_path, sources, ["ISSUE: durability requires verification"] * 2,
        read, SetupKernel())
    assert result["status"] != "PASS", result
    context = evals[0]
    assert "Raw DBFS observations are not a portability verdict" in context
    assert "saved caller or an executed %run dependency" in context
    assert "before the reported operation" in context
    assert "os.path.exists/isfile/isdir, os.listdir, glob or JVM/Spark reads" in context
    assert "Do not import aidp_compat in a probe to manufacture that evidence" in context
    assert "Earlier converted caller cells (execution order; migration bootstrap excluded)" in context
    assert "Caller source path: /Workspace/test_cond/source.ipynb" in context
    assert earlier.rstrip() in context
    assert "not-yet-saved" in context
    prefix = context.split("Earlier converted caller cells (execution order; migration bootstrap excluded):", 1)[1]
    prefix = prefix.split("End of earlier converted caller cells.", 1)[0]
    assert read.rstrip() not in prefix
    assert ("DURABLE_SETUP_SENTINEL" in prefix) == (setup in ("before", "conditional"))
    assert ("DURABLE_SETUP_SENTINEL" in context) == (setup in ("before", "current", "conditional"))
    assert fixes and earlier.rstrip() in fixes[0]["context"]
    assert "Raw DBFS observations are not a portability verdict" in fixes[0]["context"]


def test_raw_path_context_discloses_truncation_instead_of_claiming_no_import():
    prefix = [{"cell_type": "code", "source": ["x = 1\n" * 4000]},
              {"cell_type": "code", "source": ["import aidp_compat\n"]}]
    note = jm._raw_dbfs_note(
        [("open", "/dbfs/a.pkl", VOLUME + "a.pkl")],
        notebook_path="/Workspace/example.ipynb", prior_cells=prefix)
    assert "TRUNCATED" in note
    assert "do not infer missing setup from omitted code" in note
    assert "get_cell_history/get_history_entry" in note
    assert "import aidp_compat" not in note.split("Earlier converted caller cells", 1)[1]


def test_a_quiet_cell_with_a_report_still_meets_the_output_check(monkeypatch, tmp_path):
    fixed = 'present = os.path.exists(translate_path("/dbfs/FileStore/model.pkl"))  # SENTINEL_RAW_QUIET\n'
    result, _, evals, fixes = _run_cell(
        monkeypatch, tmp_path, RAW_QUIET,
        ["ISSUE: the check looks at the raw path | FIX: use translate_path()"], fixed)
    assert result["status"] == "PASS", result
    assert len(evals) == 1 and "os.path.exists('/dbfs/FileStore/model.pkl')" in evals[0]
    assert fixes and "Raw DBFS paths used at runtime" in fixes[0]["context"]


def test_a_failing_cell_gets_the_report_in_its_fix_request(monkeypatch, tmp_path):
    fixed = 'model = pickle.load(open(translate_path("/dbfs/FileStore/model.pkl"), "rb"))  # SENTINEL_RAW_FAIL\n'
    result, _, evals, fixes = _run_cell(
        monkeypatch, tmp_path, RAW_FAILING, ["PASS: loads the Volume copy"], fixed)
    assert result["status"] == "PASS", result
    assert fixes and "Raw DBFS paths used at runtime" in fixes[0]["context"]
    assert "/dbfs/FileStore/model.pkl" in fixes[0]["context"]


def test_the_migration_kernel_installs_the_recorder(monkeypatch, tmp_path):
    _, kernel, _, _ = _run_cell(
        monkeypatch, tmp_path, TRANSLATED_READ, ["PASS: ok"], TRANSLATED_READ)
    assert any("_aidp__install_raw_dbfs_recorder__" in code for code in kernel.executed)


class _RecorderFailedKernel(_DbfsKernel):
    async def execute(self, code, **kwargs):
        if "_aidp__install_raw_dbfs_recorder__" in code:
            self.executed.append(code)
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text":
                    jm._RAW_DBFS_RECORDER_FAILED + " ImportError: fixture: no aidp_compat\n"}]}
        return await super().execute(code, **kwargs)


def test_a_recorder_that_did_not_install_is_reported_unresolved(monkeypatch, tmp_path):
    result, kernel, _, _ = _run_notebook(
        monkeypatch, tmp_path, [TRANSLATED_READ], ["PASS: the read uses the Volume"],
        TRANSLATED_READ, _RecorderFailedKernel())
    assert result["status"] == "PASS", result
    assert ("UNRESOLVED: the raw /dbfs recorder was not installed "
            "(ImportError: fixture: no aidp_compat)") in _report_text(kernel)


# ── The real recorder through the cell loop ─────────────────────────────────

class _ExecutingKernel(_Kernel):
    """Runs what the kernels above only replay: the recorder the bootstrap
    installs, then each cell's code, in one namespace as the migration kernel
    does (translate_path is there from the bootstrap's aidp_compat import)."""

    def __init__(self, volume, namespace):
        super().__init__()
        self.compat = _fake_aidp_compat(volume)
        self.namespace = namespace

    async def execute(self, code, **kwargs):
        if "_aidp__install_raw_dbfs_recorder__" in code:
            assert jm._RAW_DBFS_RECORDER_BOOTSTRAP in code
            self.executed.append(code)
            self.namespace["translate_path"] = self.compat.translate_path
            with pytest.MonkeyPatch.context() as patch:
                patch.setitem(sys.modules, "aidp_compat", self.compat)
                return self._run(jm._RAW_DBFS_RECORDER_BOOTSTRAP)
        if "SENTINEL_EXECUTED" in code and "base64" not in code:
            self.executed.append(code)
            return self._run(code)
        return await super().execute(code, **kwargs)

    def _run(self, code):
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                exec(code, self.namespace)
        except Exception as exc:
            return {"status": "error", "outputs": [
                {"type": "stream", "name": "stdout", "text": out.getvalue()},
                {"type": "error", "ename": type(exc).__name__, "evalue": str(exc),
                 "traceback": traceback.format_exc().splitlines()}]}
        return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": out.getvalue()}]}


def test_the_real_recorder_reaches_the_output_check_while_an_iterator_stays_paused(monkeypatch, tmp_path):
    # One cell keeps a glob.iglob iterator paused; the next checks a raw /dbfs
    # location that only arrives at runtime, so neither cell's code shows it.
    _restore_after_test(monkeypatch)
    volume = _volume(tmp_path)
    namespace = {"SCHEMA_FILE": "/dbfs/FileStore/a.csv", "LOCAL_PATTERN": _local_pattern(tmp_path)}
    kernel = _ExecutingKernel(volume, namespace)
    check = "import os\npresent = os.path.exists(SCHEMA_FILE)  # SENTINEL_EXECUTED\n"
    result, _, evals, fixes = _run_notebook(
        monkeypatch, tmp_path,
        ["import glob\nfiles = glob.iglob(LOCAL_PATTERN)\nfirst = next(files)  # SENTINEL_EXECUTED\n", check],
        ["ISSUE: the check looks at the raw /dbfs path | FIX: use translate_path()"],
        "SCHEMA_FILE = translate_path(SCHEMA_FILE)\n" + check, kernel)
    assert result["status"] == "PASS", result
    assert len(evals) == 1, "the check cell's report must reach the output check"
    assert "os.path.exists('/dbfs/FileStore/a.csv')" in evals[0]
    assert str(volume / "FileStore" / "a.csv") in evals[0]
    assert len(fixes) == 1 and "Raw DBFS paths used at runtime" in fixes[0]["context"]
    assert namespace["present"] is True
    saved, _ = _saved_sources(kernel)
    assert "SCHEMA_FILE = translate_path(SCHEMA_FILE)" in saved


def test_the_real_recorder_reads_a_path_passed_by_its_keyword_name(monkeypatch, tmp_path):
    # os.path.isdir names its path "s"; the folder arrives at runtime as a raw
    # /dbfs path whose Volume copy exists.
    _restore_after_test(monkeypatch)
    volume = _volume(tmp_path)
    namespace = {"FOLDER": "/dbfs/FileStore"}
    kernel = _ExecutingKernel(volume, namespace)
    check = "import os\npresent = os.path.isdir(s=FOLDER)  # SENTINEL_EXECUTED\n"
    result, _, evals, fixes = _run_notebook(
        monkeypatch, tmp_path, [check],
        ["ISSUE: the check looks at the raw /dbfs folder | FIX: use translate_path()"],
        "FOLDER = translate_path(FOLDER)\n" + check, kernel)
    assert result["status"] == "PASS", result
    assert len(evals) == 1, "the keyword call's report must reach the output check"
    assert "os.path.isdir('/dbfs/FileStore')" in evals[0]
    assert str(volume / "FileStore") in evals[0]
    assert len(fixes) == 1 and "Raw DBFS paths used at runtime" in fixes[0]["context"]
    assert namespace["present"] is True
    saved, _ = _saved_sources(kernel)
    assert "FOLDER = translate_path(FOLDER)" in saved


def test_the_real_recorder_probes_glob_the_way_the_call_looks(monkeypatch, tmp_path):
    # The folder arrives at runtime as a raw /dbfs path; its Volume copy holds a
    # CSV two folders down, which only a recursive glob finds.
    _restore_after_test(monkeypatch)
    volume = _volume(tmp_path)
    (volume / "deep" / "year" / "month").mkdir(parents=True)
    (volume / "deep" / "year" / "month" / "deep.csv").write_text("x\n")
    namespace = {"ROOT": "/dbfs/deep"}
    kernel = _ExecutingKernel(volume, namespace)
    cell = 'import glob\nfiles = glob.glob("**/*.csv", root_dir=ROOT, recursive=True)  # SENTINEL_EXECUTED\n'
    result, _, evals, fixes = _run_notebook(
        monkeypatch, tmp_path, [cell],
        ["ISSUE: the glob looks below the raw /dbfs folder | FIX: use translate_path()"],
        "ROOT = translate_path(ROOT)\n" + cell, kernel)
    assert result["status"] == "PASS", result
    assert len(evals) == 1, "the recursive glob's report must reach the output check"
    assert "glob('/dbfs/deep/**/*.csv')" in evals[0]
    assert len(fixes) == 1 and "Raw DBFS paths used at runtime" in fixes[0]["context"]
    assert namespace["files"] == ["year/month/deep.csv"]
    saved, _ = _saved_sources(kernel)
    assert "ROOT = translate_path(ROOT)" in saved


def test_the_real_recorder_lets_a_cell_ship_a_function_to_workers(monkeypatch, tmp_path):
    # Valid customer code that hands a function to Spark (a UDF, mapPartitions):
    # Spark pickles it with cloudpickle. The cell pickles it the same way and runs
    # the copy; it must pass first time, with no fix round.
    pytest.importorskip("cloudpickle", reason="Spark's function serializer; CI installs it")
    _restore_after_test(monkeypatch)
    volume = _volume(tmp_path)
    namespace = {"FOLDER": str(volume / "FileStore")}
    kernel = _ExecutingKernel(volume, namespace)
    cell = ("import pickle\nimport cloudpickle\nfrom glob import glob\nfrom os.path import exists\n\n"
            "def worker(name):\n"
            "    path = FOLDER + '/' + name\n"
            "    with open(path) as fh:\n"
            "        return [fh.read(), exists(path), glob(FOLDER + '/*.csv')]\n\n"
            "shipped = pickle.loads(cloudpickle.dumps(worker))\n"
            "RESULT = shipped('a.csv')  # SENTINEL_EXECUTED\n")
    result, _, _, fixes = _run_notebook(monkeypatch, tmp_path, [cell], ["PASS: ok"], cell, kernel)
    assert result["status"] == "PASS", result
    assert fixes == []
    assert namespace["RESULT"] == ["x\n", True, [str(volume / "FileStore" / "a.csv")]]


# ── Child cells: no output check, so a report on a passing cell is UNRESOLVED ──

class _ChildDbfsKernel(child_harness._ExitKernel):
    async def execute(self, code, **kwargs):
        if "SENTINEL_CHILD_RAW" in code and "base64" not in code and "__AIDP_" not in code:
            self.executed.append(code)
            text = "" if "translate_path" in code else _report(
                "os.path.exists", "/dbfs/FileStore/model.pkl", VOLUME + "FileStore/model.pkl")
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}
        return await super().execute(code, **kwargs)


def test_a_child_cell_that_used_a_raw_path_is_reported(monkeypatch, tmp_path):
    monkeypatch.setattr(child_harness, "_ExitKernel", _ChildDbfsKernel)
    child = 'FOUND = os.path.exists("/dbfs/FileStore/model.pkl")  # SENTINEL_CHILD_RAW'
    result, *_ = child_harness._run(monkeypatch, tmp_path, {"child": [child]}, fire=False)
    notes = [note for note in result.get("notes", []) if "raw /dbfs" in note]
    assert notes and notes[0].startswith("child.ipynb cell 0: UNRESOLVED raw /dbfs paths at runtime")
    assert "/dbfs/FileStore/model.pkl" in notes[0]


@pytest.mark.parametrize("setup,operation,expected", [
    ("before", "open", "loaded"),
    ("after", "open", "missing"),
    ("none", "open", "missing"),
    ("before", "exists", "False"),
    ("before", "glob", "[]"),
])
def test_saved_setup_and_operation_in_a_fresh_interpreter(tmp_path, setup, operation, expected):
    # Ground truth for the model's support distinction. No recorder, no migration
    # bootstrap: the saved setup itself must provide the hook, in the right order.
    volume = tmp_path / "volume"
    (volume / "FileStore").mkdir(parents=True)
    (volume / "FileStore" / "schema.pkl").write_bytes(pickle.dumps({"field": "string"}))
    code = """
import glob, os, pickle, sys
os.environ["AIDP_DBFS_PREFIX"] = sys.argv[1]
setup, operation = sys.argv[2:4]
if setup == "before":
    from aidp_compat import translate_path
path = "/dbfs/FileStore/schema.pkl"
if operation == "open":
    try:
        with open(path, "rb") as fh:
            assert pickle.load(fh) == {"field": "string"}
        print("loaded")
    except FileNotFoundError:
        print("missing")
elif operation == "exists":
    print(os.path.exists(path))
else:
    print(glob.glob("/dbfs/FileStore/*.pkl"))
if setup == "after":
    from aidp_compat import translate_path
"""
    assert _in_a_fresh_process(code, b"", str(volume), setup, operation) == expected
