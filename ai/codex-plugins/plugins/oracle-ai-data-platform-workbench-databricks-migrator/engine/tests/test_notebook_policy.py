"""SEC-AIDP-SAMPLES-005: mandatory sandbox gate at the notebook execution sink.

Covers the Jira acceptance criteria:
- sandbox (catalog, schema, prefix, network posture) must be declared before
  any notebook code runs; undeclared = refuse, fail closed, clear message;
- AST gate refuses environment reads, local file reads, process/network
  imports, destructive dbutils.fs calls, dynamic code execution and writes
  outside the sandbox; dynamic write targets need a reviewed exception;
- runtime assertions on dbutils.fs.rm/mv/cp/put and the safe_io write helpers;
- every refusal / allowed exception is recorded with notebook path, cell
  index, rule and remediation for the migration report.
"""
import json
import os

import nbformat
import pytest

from aidp_compat import notebook_policy as npol
from aidp_compat.notebook import AIDPNotebookUtils, NotebookExit
from aidp_compat.notebook_policy import (
    PolicyViolation,
    SandboxPolicy,
    SandboxUndeclaredError,
)


# ── helpers ────────────────────────────────────────────────────────────
def write_notebook(tmp_path, name, cells):
    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_code_cell(src) if not src.startswith("#md ")
                else nbformat.v4.new_markdown_cell(src[4:]) for src in cells]
    path = tmp_path / f"{name}.ipynb"
    nbformat.write(nb, str(path))
    return str(path)


def runner(tmp_path):
    return AIDPNotebookUtils(spark=None, workspace_root=str(tmp_path))


class _Writer:
    """Stand-in for DataFrame.write so refused cells can also be executable."""

    def __init__(self):
        self.calls = []

    def mode(self, *_):
        return self

    def format(self, *_):
        return self

    def partitionBy(self, *_):
        return self

    def saveAsTable(self, name, **_):
        self.calls.append(("saveAsTable", name))

    def insertInto(self, name, **_):
        self.calls.append(("insertInto", name))

    def parquet(self, path, **_):
        self.calls.append(("parquet", path))

    def save(self, path=None, **_):
        self.calls.append(("save", path))


class _FakeDF:
    def __init__(self):
        self.write = _Writer()

    def cache(self):
        return self

    def count(self):
        return 0

    def unpersist(self):
        return self

    def coalesce(self, *_):
        return self

    @property
    def rdd(self):
        class _R:
            def getNumPartitions(self):
                return 1
        return _R()


# ── sandbox declaration ────────────────────────────────────────────────
def test_undeclared_sandbox_refuses_before_any_cell_runs(tmp_path):
    path = write_notebook(tmp_path, "child", ["UNDECLARED_MARK = 1"])
    with pytest.raises(SandboxUndeclaredError) as ei:
        runner(tmp_path).run(path)
    msg = str(ei.value)
    assert "no sandbox policy declared" in msg
    for var in ("AIDP_SANDBOX_CATALOG", "AIDP_SANDBOX_SCHEMA", "AIDP_SANDBOX_PREFIX"):
        assert var in msg
    assert isinstance(ei.value, PermissionError)
    assert "UNDECLARED_MARK" not in globals()


def test_partial_declaration_names_the_missing_variable(tmp_path, monkeypatch):
    monkeypatch.setenv("AIDP_SANDBOX_CATALOG", "default")
    monkeypatch.setenv("AIDP_SANDBOX_SCHEMA", "sbx")
    path = write_notebook(tmp_path, "child", ["PARTIAL_MARK = 1"])
    with pytest.raises(SandboxUndeclaredError) as ei:
        runner(tmp_path).run(path)
    assert "AIDP_SANDBOX_PREFIX" in str(ei.value)
    assert "AIDP_SANDBOX_CATALOG" not in str(ei.value).split("Set", 1)[1].split("(")[0]
    assert "PARTIAL_MARK" not in globals()


def test_undeclared_refusal_precedes_missing_notebook_check(tmp_path):
    # Fail closed even for a path that does not exist: the gate runs first.
    with pytest.raises(SandboxUndeclaredError):
        runner(tmp_path).run("does-not-exist")


def test_explicit_policy_object_is_enough(tmp_path):
    policy = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx")
    path = write_notebook(tmp_path, "child", ["EXPLICIT_MARK = 41 + 1"])
    assert AIDPNotebookUtils(workspace_root=str(tmp_path), policy=policy).run(path) == "ok"
    assert globals().pop("EXPLICIT_MARK") == 42


def test_set_sandbox_policy_process_wide(tmp_path):
    npol.set_sandbox_policy(SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/"))
    path = write_notebook(tmp_path, "child", ["PROCESS_MARK = 1"])
    assert runner(tmp_path).run(path) == "ok"
    assert globals().pop("PROCESS_MARK") == 1
    declared = [e for e in npol.get_policy_log() if e["event"] == "sandbox_declared"]
    assert declared and declared[0]["detail"]["schema"] == "sbx"


def test_sandbox_policy_requires_every_field():
    with pytest.raises(ValueError):
        SandboxPolicy(catalog="default", schema="", prefix="oci://b@ns/x/")


def test_prefix_is_a_directory_boundary():
    p = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx")
    assert p.path_in_sandbox("oci://b@ns/sbx/a/b")
    assert p.path_in_sandbox("oci://b@ns/sbx")
    assert not p.path_in_sandbox("oci://b@ns/sbx-prod/a")
    assert not p.path_in_sandbox("oci://other@ns/sbx/a")


def test_table_membership_handles_quoting_and_case():
    p = SandboxPolicy(catalog="Default", schema="SBX", prefix="oci://b@ns/sbx/")
    assert p.table_in_sandbox("default.sbx.t")
    assert p.table_in_sandbox("`default`.`sbx`.`t`")
    assert p.table_in_sandbox("sbx.T")
    assert not p.table_in_sandbox("prod.sbx.t")
    assert not p.table_in_sandbox("other.t")
    assert not p.table_in_sandbox("t")          # 1-part: unknown schema -> outside


# ── AST gate via notebook.run ──────────────────────────────────────────
REFUSED_CELLS = [
    ("import subprocess", "NBP-IMPORT-PROCESS", "subprocess"),
    ("from subprocess import run", "NBP-IMPORT-PROCESS", "subprocess"),
    ("import ctypes", "NBP-IMPORT-PROCESS", "ctypes"),
    ("import pty", "NBP-IMPORT-PROCESS", "pty"),
    ("import socket", "NBP-IMPORT-NETWORK", "socket"),
    ("import requests", "NBP-IMPORT-NETWORK", "requests"),
    ("import urllib.request", "NBP-IMPORT-NETWORK", "urllib"),
    ("import http.client", "NBP-IMPORT-NETWORK", "http.client"),
    ("from http import client", "NBP-IMPORT-NETWORK", "http.client"),
    ("import paramiko", "NBP-IMPORT-NETWORK", "paramiko"),
    ("import os\nTOKEN = os.environ.get('AIDP_TOKEN')", "NBP-OS-ENVIRON", "os.environ"),
    ("import os\nTOKEN = os.getenv('AIDP_TOKEN')", "NBP-OS-ENVIRON", "os.getenv"),
    ("import os\nos.putenv('A', 'b')", "NBP-OS-ENVIRON", "os.putenv"),
    ("from os import environ", "NBP-OS-ENVIRON", "os.environ"),
    ("import os\nos.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import os\nos.popen('id')", "NBP-OS-PROCESS", "os.popen"),
    ("import os\nos.execvp('id', ['id'])", "NBP-OS-PROCESS", "os.execvp"),
    ("import shutil\nshutil.rmtree('/Volumes/prod')", "NBP-SHUTIL-RMTREE", "shutil.rmtree"),
    ("from shutil import rmtree", "NBP-SHUTIL-RMTREE", "shutil.rmtree"),
    ("eval('1+1')", "NBP-BUILTIN-EXEC", "eval"),
    ("exec('x=1')", "NBP-BUILTIN-EXEC", "exec"),
    ("compile('1', 'f', 'eval')", "NBP-BUILTIN-EXEC", "compile"),
    ("__import__('subprocess')", "NBP-BUILTIN-EXEC", "__import__"),
    ("open('/etc/passwd').read()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("open('/Workspace/.oci/config', 'r')", "NBP-OPEN-PATH", "/Workspace/.oci/config"),
    ("p = dbutils.widgets.get('p')\nopen(p, 'w')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("p = dbutils.widgets.get('p')\nopen(p).read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("_p = '/etc/passwd'\nopen(_p).read()", "NBP-OPEN-PATH", "/etc/passwd"),             # resolved constant
    ("p = 'a'\np = '/etc/passwd'\nopen(p).read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),       # rebound: unknown
    ("import pathlib\npathlib.Path('/etc/passwd').read_text()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from pathlib import Path\nP = Path('/Workspace/.oci/config')\nP.read_bytes()", "NBP-OPEN-PATH",
     "/Workspace/.oci/config"),
    ("from pathlib import Path\nPath('/Volumes/prod/x').write_text('a')", "NBP-OPEN-PATH", "/Volumes/prod/x"),
    ("from pathlib import Path\nPath('/Volumes/prod/x').unlink()", "NBP-FS-PATH", "/Volumes/prod/x"),
    ("from pathlib import Path\nPath(dbutils.widgets.get('p')).read_text()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("import os\nos.remove('/Volumes/prod/x')", "NBP-FS-PATH", "/Volumes/prod/x"),
    ("import os\nos.rename('/Volumes/prod/a', '/Volumes/prod/b')", "NBP-FS-PATH", "/Volumes/prod/a"),
    ("import os\nos.makedirs('/Volumes/prod/new', exist_ok=True)", "NBP-FS-PATH", "/Volumes/prod/new"),
    ("import os\nos.remove(dbutils.widgets.get('p'))", "NBP-FS-DYNAMIC", "<dynamic>"),
    ("import shutil\nshutil.move('/Volumes/prod/a', '/Volumes/prod/b')", "NBP-FS-PATH", "/Volumes/prod/a"),
    ("import shutil\nshutil.copy2('/tmp/model.pt', '/Volumes/prod/model.pt')", "NBP-FS-PATH", "/Volumes/prod/model.pt"),
    ("dbutils.fs.rm('oci://b@ns/prod/data', True)", "NBP-DBFS-PATH", "oci://b@ns/prod/data"),
    ("dbutils.fs.rm('dbfs:/mnt/prod/data', recurse=True)", "NBP-DBFS-PATH", "dbfs:/mnt/prod/data"),
    ("dbutils.fs.rm('oci://b@ns/sbx/../prod/data', True)", "NBP-DBFS-PATH", "oci://b@ns/sbx/../prod/data"),
    ("dbutils.fs.mv('oci://b@ns/sbx/a', 'oci://b@ns/prod/a')", "NBP-DBFS-PATH", "oci://b@ns/prod/a"),
    ("dbutils.fs.mv('oci://b@ns/prod/a', 'oci://b@ns/sbx/a')", "NBP-DBFS-PATH", "oci://b@ns/prod/a"),
    ("dbutils.fs.cp('oci://b@ns/prod/a', 'oci://b@ns/prod/b')", "NBP-DBFS-PATH", "oci://b@ns/prod/b"),
    ("dbutils.fs.put('oci://b@ns/prod/x.txt', 'hi', True)", "NBP-DBFS-PATH", "oci://b@ns/prod/x.txt"),
    ("dbutils.fs.mkdirs('oci://b@ns/prod/newdir')", "NBP-DBFS-PATH", "oci://b@ns/prod/newdir"),
    ("_fs = dbutils.fs\n_fs.rm('oci://b@ns/prod/data')", "NBP-DBFS-PATH", "oci://b@ns/prod/data"),   # shim alias
    ("p = dbutils.widgets.get('p')\ndbutils.fs.rm(p, True)", "NBP-DBFS-DYNAMIC", "<dynamic>"),
    ("_p = 'oci://b@ns/prod/data'\ndbutils.fs.rm(_p, True)", "NBP-DBFS-PATH", "oci://b@ns/prod/data"),
    ("df.write.mode('overwrite').saveAsTable('prod.t')", "NBP-TABLE-TARGET", "prod.t"),
    ("df.write.saveAsTable('cat.prod.t')", "NBP-TABLE-TARGET", "cat.prod.t"),
    ("df.write.insertInto('t')", "NBP-TABLE-TARGET", "t"),
    ("df.writeTo('prod.t').createOrReplace()", "NBP-TABLE-TARGET", "prod.t"),
    ("df.writeTo('prod.t').append()", "NBP-TABLE-TARGET", "prod.t"),
    ("df.write.format('delta').option('path', 'oci://b@ns/prod/x').saveAsTable('sbx.t')",
     "NBP-PATH-WRITE", "oci://b@ns/prod/x"),
    ("df.write.options(path='oci://b@ns/prod/x').saveAsTable('sbx.t')", "NBP-PATH-WRITE", "oci://b@ns/prod/x"),
    ("t = dbutils.widgets.get('t')\ndf.write.saveAsTable(t)", "NBP-TABLE-DYNAMIC", "<dynamic>"),
    ("_t = 'prod.t'\ndf.write.saveAsTable(_t)", "NBP-TABLE-TARGET", "prod.t"),
    ("spark.sql('CREATE TABLE prod.t (a INT)')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('CREATE OR REPLACE TABLE cat.prod.t AS SELECT 1')", "NBP-SQL-TARGET", "cat.prod.t"),
    ("spark.sql('INSERT INTO t VALUES (1)')", "NBP-SQL-TARGET", "t"),
    ("spark.sql('INSERT OVERWRITE TABLE prod.t SELECT 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('DROP TABLE IF EXISTS prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('MERGE INTO prod.t USING s ON 1=1 WHEN MATCHED THEN DELETE')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('DELETE FROM prod.t WHERE 1=1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('CREATE SCHEMA other')", "NBP-SQL-TARGET", "other"),
    ("spark.sql(\"CREATE TABLE sbx.t USING parquet LOCATION 'oci://b@ns/prod/t'\")",
     "NBP-SQL-TARGET", "oci://b@ns/prod/t"),
    ("sql('INSERT INTO prod.t SELECT 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('CREATE TABLE sbx.t USING parquet LOCATION \"oci://b@ns/prod/t\"')",
     "NBP-SQL-TARGET", "oci://b@ns/prod/t"),
    ("spark.sql(\"CREATE TABLE sbx.t USING parquet OPTIONS (path 'oci://b@ns/prod/t')\")",
     "NBP-SQL-TARGET", "oci://b@ns/prod/t"),
    ("spark.sql(\"CREATE TABLE sbx.t USING csv OPTIONS (header 'true', 'path' = 'oci://b@ns/prod/t')\")",
     "NBP-SQL-TARGET", "oci://b@ns/prod/t"),
    ("spark.sql(\"CREATE TABLE sbx.t USING parquet LOCATION 'oci://b@ns/sbx/../prod/t'\")",
     "NBP-SQL-TARGET", "oci://b@ns/sbx/../prod/t"),
    ("t = 'x'\nspark.sql(f'INSERT INTO {t} SELECT 1')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("t = 'x'\nspark.sql('CREATE TABLE ' + t + ' (a INT)')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("_q = 'DROP TABLE prod.customers'\nspark.sql(_q)", "NBP-SQL-TARGET", "prod.customers"),   # resolved constant
    ("q = dbutils.widgets.get('q')\nspark.sql(q)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(build_query())", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("df.write.parquet('oci://b@ns/prod/out')", "NBP-PATH-WRITE", "oci://b@ns/prod/out"),
    ("df.write.format('delta').save('oci://b@ns/prod/out')", "NBP-PATH-WRITE", "oci://b@ns/prod/out"),
    ("df.write.parquet('oci://b@ns/sbx/../prod/out')", "NBP-PATH-WRITE", "oci://b@ns/sbx/../prod/out"),
    ("p = dbutils.widgets.get('p')\ndf.write.mode('append').parquet(p)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    # -- indirection: aliases, importlib, builtins, sys.modules, getattr, star import
    ("import os as _o\n_o.environ.get('AIDP_TOKEN')", "NBP-OS-ENVIRON", "os.environ"),
    ("import os as _o\n_o.environ['AIDP_SANDBOX_PREFIX'] = '/'", "NBP-OS-ENVIRON", "os.environ"),
    ("from os import environ as _e", "NBP-OS-ENVIRON", "os.environ"),
    ("from os import *", "NBP-OS-ENVIRON", "os.*"),
    ("import os\nos.fork()", "NBP-OS-PROCESS", "os.fork"),
    ("import asyncio\nasyncio.create_subprocess_shell('id')", "NBP-OS-PROCESS", "asyncio.create_subprocess_shell"),
    ("from asyncio import create_subprocess_exec", "NBP-OS-PROCESS", "asyncio.create_subprocess_exec"),
    ("import multiprocessing", "NBP-IMPORT-PROCESS", "multiprocessing"),
    ("import urllib3", "NBP-IMPORT-NETWORK", "urllib3"),
    ("import httpx", "NBP-IMPORT-NETWORK", "httpx"),
    ("import aiohttp", "NBP-IMPORT-NETWORK", "aiohttp"),
    ("import ftplib", "NBP-IMPORT-NETWORK", "ftplib"),
    ("import smtplib", "NBP-IMPORT-NETWORK", "smtplib"),
    ("import importlib\nimportlib.import_module('subprocess')", "NBP-INDIRECT", "importlib"),
    ("from importlib import import_module", "NBP-INDIRECT", "importlib"),
    ("import builtins\nbuiltins.exec('X=1')", "NBP-INDIRECT", "builtins"),
    ("import sys\nsys.modules['os'].environ['X']", "NBP-INDIRECT", "sys.modules"),
    ("import os\ngetattr(os, 'environ')['X']", "NBP-INDIRECT", "os"),
    ("import os as _o\nvars(_o)['environ']", "NBP-INDIRECT", "os"),
    # -- policy tampering: compat internals, set_sandbox_policy, module attribute assignment
    ("from aidp_compat.notebook_policy import set_sandbox_policy", "NBP-POLICY-TAMPER",
     "aidp_compat.notebook_policy"),
    ("from aidp_compat import SandboxPolicy", "NBP-POLICY-TAMPER", "aidp_compat.SandboxPolicy"),
    ("from aidp_compat import set_sandbox_policy", "NBP-POLICY-TAMPER", "aidp_compat.set_sandbox_policy"),
    ("from aidp_compat import fs", "NBP-POLICY-TAMPER", "aidp_compat.fs"),
    ("from aidp_compat.fs import AIDPFileSystemUtils", "NBP-POLICY-TAMPER", "aidp_compat.fs.AIDPFileSystemUtils"),
    ("from aidp_compat.fs import _get_oci_storage_client", "NBP-POLICY-TAMPER",
     "aidp_compat.fs._get_oci_storage_client"),
    ("import aidp_compat", "NBP-POLICY-TAMPER", "aidp_compat"),
    ("import aidp_compat.fs as _f\n_f.assert_path_in_sandbox = lambda p, **k: p", "NBP-POLICY-TAMPER",
     "aidp_compat.fs"),
    ("import aidp_compat.fs as _f\nsetattr(_f, 'assert_path_in_sandbox', None)", "NBP-POLICY-TAMPER",
     "aidp_compat.fs"),
    ("import os\nos.environ = {}", "NBP-OS-ENVIRON", "os.environ"),
    ("import os as _o\n_o.remove = print", "NBP-POLICY-TAMPER", "os.remove"),
    ("import sys\nsys.modules['os'] = None", "NBP-INDIRECT", "sys.modules"),
    ("dbutils.fs.rm = lambda *a, **k: True", "NBP-POLICY-TAMPER", "dbutils.fs.rm"),
    ("dbutils.fs = None", "NBP-POLICY-TAMPER", "dbutils.fs"),
    ("dbutils.notebook._policy = None", "NBP-POLICY-TAMPER", "dbutils.notebook._policy"),
    ("del dbutils.fs", "NBP-POLICY-TAMPER", "dbutils.fs"),
    ("import builtins as _b\n_b.open = print", "NBP-INDIRECT", "builtins"),
]


@pytest.mark.parametrize("source,rule,target", REFUSED_CELLS, ids=[c[1] + ":" + c[0][:30] for c in REFUSED_CELLS])
def test_refused_cell_is_not_executed_and_is_logged(tmp_path, sandbox_env, source, rule, target):
    path = write_notebook(tmp_path, "child", ["#md header", "SETUP_OK = 1", source + "\nREFUSED_MARK = 1"])
    globals().pop("SETUP_OK", None)
    globals().pop("REFUSED_MARK", None)
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    err = ei.value
    assert isinstance(err, PermissionError)
    assert err.rule_id == rule
    assert err.cell_index == 2
    assert err.notebook_path == path
    assert err.target == target
    assert err.remediation
    msg = str(err)
    assert path in msg and "cell 2" in msg and rule in msg and "Remediation:" in msg
    # Earlier cells ran; the refused cell did not.
    assert globals().pop("SETUP_OK") == 1
    assert "REFUSED_MARK" not in globals()
    refused = [e for e in npol.get_policy_log() if e["event"] == "refused"]
    assert refused, "refusal must be recorded in the policy log"
    entry = refused[0]
    assert entry["notebook_path"] == path
    assert entry["cell_index"] == 2
    assert entry["rule"] == rule
    assert entry["target"] == target
    assert entry["remediation"]


CLEAN_CELLS = [
    "import os\nP = os.path.join('a', 'b')",
    "import json\nJ = json.dumps({'a': 1})",
    "CLEAN_SQL = spark.sql('SELECT * FROM prod.t')",
    "spark.sql('CREATE OR REPLACE TEMP VIEW v AS SELECT 1')",
    "spark.sql('CREATE GLOBAL TEMPORARY VIEW gv AS SELECT 1')",
    "v = 'v'\nspark.sql(f'CREATE OR REPLACE TEMP VIEW {v} AS SELECT 1')",
    "t = 'prod.t'\nspark.sql(f'SELECT count(*) FROM {t}')",
    "spark.sql('CREATE TABLE IF NOT EXISTS default.sbx.t (a INT)')",
    "spark.sql('INSERT INTO `sbx`.`t` VALUES (1)')",
    "spark.sql('CREATE SCHEMA IF NOT EXISTS sbx')",
    "spark.sql(\"CREATE TABLE sbx.t2 USING parquet LOCATION 'oci://b@ns/sbx/t2'\")",
    "df.write.mode('overwrite').saveAsTable('sbx.t')",
    "df.write.saveAsTable('default.sbx.t')",
    "dbutils.fs.rm('oci://b@ns/sbx/tmp/', True)",
    "dbutils.fs.cp('oci://b@ns/prod/src', 'oci://b@ns/sbx/copy')",   # reading prod is fine
    "df.write.parquet('oci://b@ns/sbx/out')",
    "open('relative.txt', 'w').close()",
    "p = 'relative.txt'\nopen(p).read()",
    "spark.sql('SELECT 1')",
    "x = spark.read.json('oci://b@ns/prod/in')",
    "q = 'SELECT count(*) FROM prod.t'\nspark.sql(q)",                       # resolved read statement
    "from aidp_compat import dbutils, translate_path, safe_pickle_dump\nP = translate_path('/dbfs/x')",
    "from aidp_compat.safe_io import safe_pickle_dump",
    "import pandas as pd\npd.options.display.max_rows = 200",              # third-party module options
    "import os\nimport sys as _s\nX = _s.version",
    "import pathlib\nR = pathlib.Path('rel.txt').read_text()",
    "from pathlib import Path\nPath('oci://b@ns/sbx/x').write_text('a')",
    "import os\nos.makedirs('oci://b@ns/sbx/newdir', exist_ok=True)",
    "s = 'a-b'\nt = s.replace('-', '_')",                                   # str.replace is not Path.replace
    "df.write.option('header', 'true').parquet('oci://b@ns/sbx/out')",
    "df.write.format('delta').option('path', 'oci://b@ns/sbx/x').saveAsTable('sbx.t')",
    "df.writeTo('sbx.t').createOrReplace()",
    "spark.sql('CREATE TABLE sbx.t USING parquet LOCATION \"oci://b@ns/sbx/t\"')",
    "dbutils.fs.mkdirs('oci://b@ns/sbx/newdir')",
    "_fs = dbutils.fs\n_fs.rm('oci://b@ns/sbx/tmp/')",
    "self_like = object()\nsetattr(self_like, 'x', 1)",
    "class C:\n    def __init__(self):\n        self.x = 1",
]


@pytest.mark.parametrize("source", CLEAN_CELLS, ids=[c[:40] for c in CLEAN_CELLS])
def test_clean_cell_passes_the_gate(sandbox_env, source):
    assert npol.scan_source(source) == []


def test_clean_notebook_runs_and_returns_exit_value(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", [
        "#md ## heading",
        "%sql SELECT 1",                       # magic: skipped, as before
        "RESULT_A = 40",
        "RESULT_A = RESULT_A + 2",
        "dbutils.notebook.exit(str(RESULT_A))",
    ])
    g = globals()
    g["dbutils"] = type("D", (), {"notebook": runner(tmp_path)})()
    try:
        assert runner(tmp_path).run(path) == "42"
    finally:
        g.pop("dbutils", None)
        g.pop("RESULT_A", None)
    assert [e for e in npol.get_policy_log() if e["event"] == "refused"] == []


def test_allowlisted_rule_is_logged_as_exception_and_executes(tmp_path, sandbox_env, monkeypatch):
    monkeypatch.setenv("AIDP_NOTEBOOK_POLICY_ALLOW", "NBP-TABLE-DYNAMIC")
    path = write_notebook(tmp_path, "child", [
        "TARGET = ''.join(['sbx', '.t'])",      # not statically resolvable -> needs the exception
        "df.write.saveAsTable(TARGET)",
    ])
    globals()["df"] = _FakeDF()
    try:
        assert runner(tmp_path).run(path) == "ok"
        assert globals()["df"].write.calls == [("saveAsTable", "sbx.t")]
    finally:
        globals().pop("df", None)
        globals().pop("TARGET", None)
    log = npol.get_policy_log()
    allowed = [e for e in log if e["event"] == "allowed_exception"]
    assert len(allowed) == 1
    assert allowed[0]["rule"] == "NBP-TABLE-DYNAMIC"
    assert allowed[0]["cell_index"] == 1
    assert allowed[0]["notebook_path"] == path
    assert not [e for e in log if e["event"] == "refused"]


def test_name_bound_once_to_a_literal_is_resolved_across_cells(tmp_path, sandbox_env):
    # No exception needed: the target is a name bound once to a sandbox literal in an earlier cell.
    path = write_notebook(tmp_path, "child", ["TARGET = 'sbx.t'", "df.write.saveAsTable(TARGET)"])
    globals()["df"] = _FakeDF()
    try:
        assert runner(tmp_path).run(path) == "ok"
    finally:
        globals().pop("df", None)
        globals().pop("TARGET", None)
    assert [e for e in npol.get_policy_log() if e["event"] in ("refused", "allowed_exception")] == []
    # ... and a name bound once to a literal OUTSIDE the sandbox is refused as a literal target.
    path = write_notebook(tmp_path, "child2", ["TARGET2 = 'prod.t'", "df.write.saveAsTable(TARGET2)"])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-TABLE-TARGET" and ei.value.target == "prod.t"
    globals().pop("TARGET2", None)


def test_allowlist_does_not_bleed_across_rules(tmp_path, sandbox_env, monkeypatch):
    monkeypatch.setenv("AIDP_NOTEBOOK_POLICY_ALLOW", "NBP-TABLE-DYNAMIC")
    path = write_notebook(tmp_path, "child", ["p = dbutils.widgets.get('p')\ndbutils.fs.rm(p)"])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-DBFS-DYNAMIC"


def test_allow_network_flag_permits_network_imports_only(tmp_path, sandbox_env, monkeypatch):
    monkeypatch.setenv("AIDP_SANDBOX_ALLOW_NETWORK", "1")
    ok = write_notebook(tmp_path, "net", ["import socket\nNET_MARK = socket.AF_INET is not None"])
    assert runner(tmp_path).run(ok) == "ok"
    assert globals().pop("NET_MARK") is True
    bad = write_notebook(tmp_path, "proc", ["import subprocess"])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(bad)
    assert ei.value.rule_id == "NBP-IMPORT-PROCESS"


def test_first_refusal_wins_but_all_violations_are_logged(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", ["import subprocess\nimport socket\nos.environ['X']"])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-IMPORT-PROCESS"
    assert "+2 more" in str(ei.value)
    rules = sorted(e["rule"] for e in npol.get_policy_log() if e["event"] == "refused")
    assert rules == ["NBP-IMPORT-NETWORK", "NBP-IMPORT-PROCESS", "NBP-OS-ENVIRON"]


def test_syntax_error_cell_does_not_execute(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", ["SYNTAX_MARK = (1"])
    with pytest.raises(SyntaxError):
        runner(tmp_path).run(path)
    assert "SYNTAX_MARK" not in globals()


def test_compiled_cells_carry_the_notebook_filename(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", ["raise ValueError('boom')"])
    with pytest.raises(ValueError) as ei:
        runner(tmp_path).run(path)
    tb = ei.value.__traceback__
    filenames = []
    while tb is not None:
        filenames.append(tb.tb_frame.f_code.co_filename)
        tb = tb.tb_next
    assert path in filenames


# ── policy log surfaces ────────────────────────────────────────────────
def test_policy_log_markdown_and_jsonl(tmp_path, sandbox_env, monkeypatch):
    log_file = tmp_path / "policy.jsonl"
    monkeypatch.setenv("AIDP_NOTEBOOK_POLICY_LOG", str(log_file))
    path = write_notebook(tmp_path, "child", ["import subprocess"])
    with pytest.raises(PolicyViolation):
        runner(tmp_path).run(path)
    md = npol.policy_log_markdown()
    assert "| Event | Notebook | Cell | Rule | Target | Remediation |" in md
    assert "NBP-IMPORT-PROCESS" in md and path in md
    entries = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    # The sandbox identifiers are recorded once (audit), then the refusal.
    assert [e["event"] for e in entries] == ["sandbox_declared", "refused"]
    assert entries[0]["detail"]["schema"] == "sbx" and entries[0]["detail"]["source"] == "environment"
    entry = entries[1]
    assert entry["rule"] == "NBP-IMPORT-PROCESS"
    assert entry["cell_index"] == 0 and entry["notebook_path"] == path
    assert [e["rule"] for e in json.loads(npol.policy_log_json()) if e["event"] == "refused"] == ["NBP-IMPORT-PROCESS"]
    npol.clear_policy_log()
    assert npol.policy_log_markdown() == ""


# ── runtime assertions: dbutils.fs ─────────────────────────────────────
def _fs():
    from aidp_compat.fs import AIDPFileSystemUtils
    return AIDPFileSystemUtils(spark=None)


def test_fs_rm_outside_prefix_is_refused_and_logged(tmp_path, sandbox_env):
    victim = tmp_path / "prod.txt"
    victim.write_text("keep me")
    with pytest.raises(PolicyViolation) as ei:
        _fs().rm(str(victim))
    assert ei.value.rule_id == "NBP-RUNTIME-PATH"
    assert victim.exists()
    entry = [e for e in npol.get_policy_log() if e["event"] == "runtime_refused"][0]
    assert entry["target"] == str(victim) and entry["detail"]["operation"] == "dbutils.fs.rm"


def test_fs_rm_inside_prefix_works(tmp_path, sandbox_env):
    f = tmp_path / "sandbox" / "tmp.txt"
    f.write_text("scratch")
    assert _fs().rm(str(f)) is True
    assert not f.exists()


def test_fs_rm_undeclared_sandbox_is_refused(tmp_path):
    victim = tmp_path / "prod.txt"
    victim.write_text("keep me")
    with pytest.raises(SandboxUndeclaredError):
        _fs().rm(str(victim))
    assert victim.exists()


def test_fs_put_outside_prefix_is_refused(tmp_path, sandbox_env):
    target = tmp_path / "prod-out.txt"
    with pytest.raises(PolicyViolation):
        _fs().put(str(target), "data", True)
    assert not target.exists()
    inside = tmp_path / "sandbox" / "out.txt"
    assert _fs().put(str(inside), "data", True) is True
    assert inside.read_text() == "data"


def test_fs_mv_requires_both_ends_inside(tmp_path, sandbox_env):
    src_out = tmp_path / "prod-src.txt"
    src_out.write_text("x")
    with pytest.raises(PolicyViolation):
        _fs().mv(str(src_out), str(tmp_path / "sandbox" / "dst.txt"))
    assert src_out.exists()
    src_in = tmp_path / "sandbox" / "src.txt"
    src_in.write_text("y")
    with pytest.raises(PolicyViolation):
        _fs().mv(str(src_in), str(tmp_path / "prod-dst.txt"))
    assert src_in.exists()
    assert _fs().mv(str(src_in), str(tmp_path / "sandbox" / "moved.txt")) is True
    assert (tmp_path / "sandbox" / "moved.txt").read_text() == "y"


def test_fs_cp_destination_must_be_inside(tmp_path, sandbox_env):
    src = tmp_path / "prod-src.txt"            # reading from outside is allowed
    src.write_text("z")
    with pytest.raises(PolicyViolation):
        _fs().cp(str(src), str(tmp_path / "prod-copy.txt"))
    assert not (tmp_path / "prod-copy.txt").exists()
    assert _fs().cp(str(src), str(tmp_path / "sandbox" / "copy.txt")) is True
    assert (tmp_path / "sandbox" / "copy.txt").read_text() == "z"


# ── runtime assertions: safe_io write helpers ─────────────────────────
def test_safe_save_as_table_asserts_sandbox_schema(sandbox_env):
    from aidp_compat.safe_io import safe_save_as_table, safe_save_as_table_coalesced
    df = _FakeDF()
    with pytest.raises(PolicyViolation) as ei:
        safe_save_as_table(df, "prod.t")
    assert ei.value.rule_id == "NBP-RUNTIME-TABLE"
    with pytest.raises(PolicyViolation):
        safe_save_as_table_coalesced(df, "default.other.t")
    assert df.write.calls == []
    safe_save_as_table(df, "sbx.t")
    safe_save_as_table_coalesced(df, "default.sbx.t2", mode="append")
    assert df.write.calls == [("saveAsTable", "sbx.t"), ("saveAsTable", "default.sbx.t2")]


def test_safe_write_parquet_asserts_sandbox_prefix(sandbox_env):
    from aidp_compat.safe_io import safe_write_parquet, safe_write_parquet_coalesced
    df = _FakeDF()
    with pytest.raises(PolicyViolation):
        safe_write_parquet(df, "oci://b@ns/prod/out", mode="append")
    with pytest.raises(PolicyViolation):
        safe_write_parquet_coalesced(df, "oci://b@ns/prod/out")
    assert df.write.calls == []
    safe_write_parquet(df, "oci://b@ns/sbx/out", mode="append")
    assert df.write.calls == [("parquet", "oci://b@ns/sbx/out")]


def test_safe_local_writers_assert_sandbox_prefix(tmp_path, sandbox_env, monkeypatch):
    from aidp_compat import safe_io
    monkeypatch.setattr(safe_io, "FUSE_WRITE_DELAY", 0)
    outside = tmp_path / "prod.pkl"
    with pytest.raises(PolicyViolation):
        safe_io.safe_pickle_dump({"a": 1}, str(outside))
    assert not outside.exists()
    inside = tmp_path / "sandbox" / "ok.pkl"
    safe_io.safe_pickle_dump({"a": 1}, str(inside), delay=0)
    assert inside.exists()

    class _PD:
        def __init__(self):
            self.calls = []

        def to_csv(self, p, **_):
            self.calls.append(p)
    pd = _PD()
    with pytest.raises(PolicyViolation):
        safe_io.safe_pandas_to_csv(pd, str(tmp_path / "prod.csv"), delay=0)
    assert pd.calls == [], "refusal must happen before the DataFrame is written"
    # (the inside-prefix happy path of safe_pandas_to_csv needs a POSIX fsync;
    # safe_pickle_dump above covers the allowed case for local writers)


def test_runtime_assertions_fail_closed_without_a_sandbox():
    from aidp_compat.safe_io import safe_save_as_table
    df = _FakeDF()
    with pytest.raises(SandboxUndeclaredError):
        safe_save_as_table(df, "sbx.t")
    assert df.write.calls == []


def test_fs_mkdirs_asserts_sandbox_prefix(tmp_path, sandbox_env):
    outside = tmp_path / "outside-dir"
    with pytest.raises(PolicyViolation) as ei:
        _fs().mkdirs(str(outside))
    assert ei.value.rule_id == "NBP-RUNTIME-PATH"
    assert not outside.exists()
    inside = tmp_path / "sandbox" / "new" / "dir"
    assert _fs().mkdirs(str(inside)) is True
    assert inside.is_dir()


def test_fs_mount_extra_configs_never_reach_the_environment(sandbox_env):
    fs = _fs()
    before = dict(os.environ)
    assert fs.mount("oci://b@ns/data", "/mnt/data", extra_configs={"fs.key": "mount-secret-value"}) is True
    assert dict(os.environ) == before
    assert not any("mount-secret-value" in v for v in os.environ.values())
    assert fs.mounts()[0].source == "oci://b@ns/data"


# ── path canonicalisation: traversal and symlinks ─────────────────────
@pytest.mark.parametrize("path", [
    "oci://b@ns/sbx/../prod/x",
    "oci://b@ns/sbx/a/../../prod/x",
    "oci://b@ns/sbx/..",
    "/Volumes/sbx/../prod/x",
])
def test_parent_segment_is_never_inside_the_sandbox(path):
    p = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/", extra_prefixes=("/Volumes/sbx/",))
    assert not p.path_in_sandbox(path)
    assert p.path_in_sandbox("oci://b@ns/sbx/./a//b")      # canonical form is still under the prefix
    assert p.path_in_sandbox("/Volumes/sbx/a/./b")


def test_local_traversal_is_refused_at_runtime(tmp_path, sandbox_env):
    from aidp_compat import safe_io
    policy = npol.require_policy()
    local_prefix = sandbox_env["local_prefix"]
    assert not policy.path_in_sandbox(local_prefix + ".." + os.sep + "victim.txt")
    assert not policy.path_in_sandbox(local_prefix + ".." + os.sep + ".." + os.sep + "etc")
    escaped = tmp_path / "escaped.pkl"
    with pytest.raises(PolicyViolation) as ei:
        safe_io.safe_pickle_dump({"a": 1}, local_prefix + ".." + os.sep + "escaped.pkl", delay=0)
    assert ei.value.rule_id == "NBP-RUNTIME-PATH" and "parent-directory" in str(ei.value)
    assert not escaped.exists()
    with pytest.raises(PolicyViolation):
        _fs().rm(local_prefix + ".." + os.sep + "victim.txt")


def test_sandbox_prefix_itself_rejects_parent_segments():
    with pytest.raises(ValueError):
        SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/../")


@pytest.mark.skipif(os.name != "posix", reason="symlink resolution is enforced on POSIX hosts only")
def test_symlink_inside_prefix_pointing_outside_is_refused(tmp_path, sandbox_env):
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "sandbox" / "link"
    link.symlink_to(outside, target_is_directory=True)
    policy = npol.require_policy()
    assert not policy.path_in_sandbox(str(link / "x.txt"))
    with pytest.raises(PolicyViolation):
        _fs().put(str(link / "x.txt"), "data", True)
    assert not (outside / "x.txt").exists()


# ── one-shot policy: redeclaration, environment mutation, aliases ──────
def test_policy_is_frozen_after_first_resolution(tmp_path, sandbox_env, monkeypatch):
    victim = tmp_path / "prod.txt"
    victim.write_text("keep me")
    first = npol.require_policy()
    # Environment changes after the first resolution are ignored by every assertion.
    monkeypatch.setenv("AIDP_SANDBOX_PREFIX", str(tmp_path) + os.sep)
    monkeypatch.setenv("AIDP_NOTEBOOK_POLICY_ALLOW", "NBP-DBFS-DYNAMIC")
    assert npol.require_policy() == first
    with pytest.raises(PolicyViolation):
        _fs().rm(str(victim))
    assert victim.exists()
    # A redeclaration that widens (or clears) the sandbox is refused and logged.
    wider = SandboxPolicy(catalog="default", schema="sbx", prefix=str(tmp_path) + os.sep)
    with pytest.raises(PolicyViolation) as ei:
        npol.set_sandbox_policy(wider)
    assert ei.value.rule_id == "NBP-POLICY-TAMPER"
    with pytest.raises(PolicyViolation):
        npol.set_sandbox_policy(None)
    assert npol.require_policy() == first
    tamper = [e for e in npol.get_policy_log() if e["rule"] == "NBP-POLICY-TAMPER"]
    assert len(tamper) == 2 and tamper[0]["detail"]["operation"] == "set_sandbox_policy"
    # Redeclaring the identical policy (bootstrap replay) is a no-op.
    npol.set_sandbox_policy(SandboxPolicy.from_env(dict(os.environ, AIDP_SANDBOX_PREFIX="oci://b@ns/sbx/,"
                                                        + sandbox_env["local_prefix"],
                                                        AIDP_NOTEBOOK_POLICY_ALLOW="")))
    assert npol.require_policy() == first


def test_cell_cannot_redeclare_the_sandbox(tmp_path, sandbox_env):
    victim = tmp_path / "prod" / "victim.txt"
    victim.parent.mkdir()
    victim.write_text("keep me")
    prod_prefix = str(tmp_path / "prod") + os.sep
    path = write_notebook(tmp_path, "child", [
        "from aidp_compat.notebook_policy import set_sandbox_policy, SandboxPolicy\n"
        f"set_sandbox_policy(SandboxPolicy('default', 'sbx', {prod_prefix!r}))",
        f"dbutils.fs.rm({str(victim)!r})",
    ])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-POLICY-TAMPER" and ei.value.cell_index == 0
    assert victim.exists()
    # Even if the import were smuggled past the gate, the runtime refuses the redeclaration.
    with pytest.raises(PolicyViolation):
        npol.set_sandbox_policy(SandboxPolicy("default", "sbx", prod_prefix))
    with pytest.raises(PolicyViolation):
        _fs().rm(str(victim))
    assert victim.exists()


def test_cell_cannot_widen_the_sandbox_through_the_environment(tmp_path, sandbox_env):
    victim = tmp_path / "prod" / "victim.txt"
    victim.parent.mkdir()
    victim.write_text("keep me")
    g = globals()
    g["dbutils"] = type("D", (), {"fs": _fs()})()
    try:
        # Aliasing os does not hide the environment write from the gate (cross-cell alias tracking).
        path = write_notebook(tmp_path, "child", [
            "import os as _o",
            f"_o.environ['AIDP_SANDBOX_PREFIX'] = {str(tmp_path) + os.sep!r}",
            f"dbutils.fs.rm({str(victim)!r})",
        ])
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
        assert ei.value.rule_id == "NBP-OS-ENVIRON" and ei.value.cell_index == 1
        assert victim.exists()
        # And the frozen snapshot ignores the environment anyway.
        os.environ["AIDP_SANDBOX_PREFIX"] = str(tmp_path) + os.sep
        with pytest.raises(PolicyViolation):
            _fs().rm(str(victim))
        assert victim.exists()
    finally:
        g.pop("dbutils", None)


def test_shim_alias_bound_in_an_earlier_cell_is_gated(tmp_path, sandbox_env):
    victim = tmp_path / "prod.txt"
    victim.write_text("keep me")
    g = globals()
    g["dbutils"] = type("D", (), {"fs": _fs()})()
    try:
        path = write_notebook(tmp_path, "child", ["_fs = dbutils.fs", f"_fs.rm({str(victim)!r})"])
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
        assert ei.value.rule_id == "NBP-DBFS-PATH" and ei.value.cell_index == 1
        assert victim.exists()
    finally:
        g.pop("dbutils", None)
        g.pop("_fs", None)


def test_monkeypatching_the_runtime_assertion_is_refused(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", [
        "import aidp_compat.fs as _f",
        "_f.assert_path_in_sandbox = lambda p, **k: p",
    ])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-POLICY-TAMPER" and ei.value.cell_index == 0
    path = write_notebook(tmp_path, "child2", ["dbutils.fs.rm = lambda *a, **k: True"])
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == "NBP-POLICY-TAMPER"
