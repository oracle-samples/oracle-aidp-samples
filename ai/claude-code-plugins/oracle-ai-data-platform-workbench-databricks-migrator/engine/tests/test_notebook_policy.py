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
import sys

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
    # (dynamic read statements need the runtime SQL guard: see SQL_DYNAMIC_READ_CELLS)
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
        "TARGET = ''.join(['sbx', '.t']).strip()",      # a call the gate does not fold -> needs the exception
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


# ══════════════════════════════════════════════════════════════════════
# SEC-AIDP-SAMPLES-005 revalidation: every route to the same capability
# ══════════════════════════════════════════════════════════════════════
# The security reviewer's residual probes, verbatim.
@pytest.mark.parametrize("source,rule,target", [
    ("__builtins__['__import__']('os').system('id')", "NBP-BUILTIN-EXEC", "__import__"),
    ("__builtins__['open']('/etc/passwd').read()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("globals()['__builtins__']['open']('/etc/passwd').read()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("get_ipython().system('id')", "NBP-OS-PROCESS", "get_ipython.system"),
    ("spark._jvm.java.lang.Runtime.getRuntime().exec('id')", "NBP-POLICY-TAMPER", "spark._jvm"),
    ("sc._jvm.java.lang.Runtime.getRuntime().exec('id')", "NBP-POLICY-TAMPER", "sc._jvm"),
])
def test_residual_notebook_policy_bypasses_are_refused(tmp_path, sandbox_env, source, rule, target):
    path = write_notebook(tmp_path, "child", [source + "\nREFUSED_MARK = 1"])
    globals().pop("REFUSED_MARK", None)
    with pytest.raises(PolicyViolation) as ei:
        runner(tmp_path).run(path)
    assert ei.value.rule_id == rule
    assert target in ei.value.target
    assert "REFUSED_MARK" not in globals()


def _assert_refused(tmp_path, cells, rule, target, cell_index=0, stubs=None):
    """Run ``cells`` as a notebook; the cell at ``cell_index`` must be refused
    with exactly ``rule`` / ``target`` before any of its lines (or any later
    cell) executes, and the refusal must be in the policy log with notebook
    path, cell index, rule, target and remediation."""
    g = globals()
    saved = dict(g)
    g.update(stubs or {})
    cells = list(cells)
    cells[cell_index] = cells[cell_index] + "\nREFUSED_MARK = 1"
    cells.append("LATER_MARK = 1")
    path = write_notebook(tmp_path, "child", cells)
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
        err = ei.value
        assert isinstance(err, PermissionError)
        assert (err.rule_id, err.target, err.cell_index) == (rule, target, cell_index)
        assert err.notebook_path == path and err.remediation
        assert "REFUSED_MARK" not in g and "LATER_MARK" not in g
        entries = [e for e in npol.get_policy_log() if e["event"] == "refused" and e["cell_index"] == cell_index]
        assert entries, "refusal must be recorded in the policy log"
        assert (entries[0]["rule"], entries[0]["target"], entries[0]["notebook_path"]) == (rule, target, path)
        assert entries[0]["remediation"]
        return err
    finally:
        # Earlier cells executed in this module's namespace; put it back.
        for k in set(g) - set(saved):
            g.pop(k, None)
        for k, v in saved.items():
            if g.get(k) is not v:
                g[k] = v


# (1) Builtin indirection: the builtins namespace through any door is checked
#     like the direct form, and a key / attribute that is not a literal is refused.
BUILTIN_INDIRECTION_CELLS = [
    ("__builtins__.open('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("__builtins__.__dict__['open']('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("getattr(__builtins__, 'open')('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("globals()['__builtins__']['__import__']('os').system('id')", "NBP-BUILTIN-EXEC", "__import__"),
    ("locals()['__builtins__']['eval']('1')", "NBP-BUILTIN-EXEC", "eval"),
    ("vars()['__builtins__']['compile']('1', 'f', 'eval')", "NBP-BUILTIN-EXEC", "compile"),
    ("getattr(__builtins__, '__import__')('os')", "NBP-BUILTIN-EXEC", "__import__"),
    ("__builtins__['exec']('x=1')", "NBP-BUILTIN-EXEC", "exec"),
    ("g = globals\ng()['__builtins__']['exec']('x=1')", "NBP-BUILTIN-EXEC", "exec"),
    ("vars(__builtins__)['open']('rel.txt')", "NBP-INDIRECT", "__builtins__['open']"),       # relative, still indirect
    ("__builtins__['op' + 'en']('/etc/passwd')", "NBP-INDIRECT", "__builtins__[<dynamic>]"),
    ("k = 'open'\n__builtins__[k]('/etc/passwd')", "NBP-INDIRECT", "__builtins__[<dynamic>]"),
    ("getattr(__builtins__, 'op' + 'en')", "NBP-INDIRECT", "__builtins__"),
    ("import builtins\nbuiltins.open('/etc/passwd')", "NBP-INDIRECT", "builtins"),
    ("print(__builtins__)", "NBP-INDIRECT", "__builtins__"),
    ("d = globals()", "NBP-INDIRECT", "globals()"),
    ("ns = locals()", "NBP-INDIRECT", "locals()"),
    ("for k, v in globals().items():\n    pass", "NBP-INDIRECT", "globals()"),
    ("__builtins__['print'] = lambda *a: None", "NBP-POLICY-TAMPER", "__builtins__"),
    ("globals()['__builtins__'] = {}", "NBP-POLICY-TAMPER", "globals()['__builtins__']"),
    ("globals()['dbutils'] = None", "NBP-POLICY-TAMPER", "globals()['dbutils']"),
    ("list(map(open, ['/etc/passwd']))", "NBP-INDIRECT", "open"),                          # builtin handed around
    ("import functools\nfunctools.partial(open, '/etc/passwd')()", "NBP-INDIRECT", "open"),
    ("f = exec\nf('x=1')", "NBP-BUILTIN-EXEC", "exec"),
    ("o = open\no('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
]


@pytest.mark.parametrize("source,rule,target", BUILTIN_INDIRECTION_CELLS,
                         ids=[c[1] + ":" + c[0][:30] for c in BUILTIN_INDIRECTION_CELLS])
def test_builtin_indirection_is_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


# (2) Dynamic attribute / key access on protected roots and import machinery.
DYNAMIC_ACCESS_CELLS = [
    ("import os\ngetattr(os, 'sys' + 'tem')('id')", "NBP-INDIRECT", "os"),
    ("import os\nn = 'system'\ngetattr(os, n)('id')", "NBP-INDIRECT", "os"),
    ("getattr(obj, name)", "NBP-INDIRECT", "getattr(obj, <dynamic>)"),
    ("setattr(obj, key, value)", "NBP-INDIRECT", "setattr(obj, <dynamic>)"),
    ("delattr(obj, 'x' + 'y')", "NBP-INDIRECT", "delattr(obj, <dynamic>)"),
    ("getattr(spark, '_jvm')", "NBP-INDIRECT", "spark"),
    ("import pkgutil", "NBP-INDIRECT", "pkgutil"),
    ("import runpy\nrunpy.run_path('/tmp/x.py')", "NBP-BUILTIN-EXEC", "runpy"),
    ("import code", "NBP-BUILTIN-EXEC", "code"),
    ("import sys\nsys.modules['o' + 's'].system('id')", "NBP-INDIRECT", "sys.modules"),
    ("import zipimport", "NBP-INDIRECT", "zipimport"),
    ("import _io\n_io.open('/etc/passwd')", "NBP-INDIRECT", "_io"),
    ("import importlib.util", "NBP-INDIRECT", "importlib"),
    ("from importlib import import_module as im\nim('os')", "NBP-INDIRECT", "importlib"),
    ("import sys\nsys.settrace(lambda *a: None)", "NBP-INDIRECT", "sys.settrace"),
    ("import sys\nsys.meta_path.insert(0, finder)", "NBP-INDIRECT", "sys.meta_path"),
    ("import inspect\nfr = inspect.currentframe()", "NBP-INDIRECT", "inspect.currentframe"),
    ("import gc\ngc.get_objects()", "NBP-INDIRECT", "gc.get_objects"),
    ("from shutil import *", "NBP-INDIRECT", "shutil.*"),
    ("from pickle import *", "NBP-INDIRECT", "pickle.*"),
]


@pytest.mark.parametrize("source,rule,target", DYNAMIC_ACCESS_CELLS,
                         ids=[c[1] + ":" + c[0][:30] for c in DYNAMIC_ACCESS_CELLS])
def test_dynamic_access_on_protected_roots_is_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


# (3) Dunder / frame introspection chains (attribute ACCESS; dunder definitions stay fine).
DUNDER_ESCAPE_CELLS = [
    ("().__class__.__bases__[0].__subclasses__()", "NBP-DUNDER-ESCAPE", "<expr>.__subclasses__"),
    ("x.__class__.__mro__", "NBP-DUNDER-ESCAPE", "x.__class__.__mro__"),
    ("f.__globals__['__builtins__']", "NBP-DUNDER-ESCAPE", "f.__globals__"),
    ("f.__code__.co_code", "NBP-DUNDER-ESCAPE", "f.__code__"),
    ("obj.__reduce_ex__(2)", "NBP-DUNDER-ESCAPE", "obj.__reduce_ex__"),
    ("e.__traceback__.tb_frame.f_back.f_globals", "NBP-DUNDER-ESCAPE", "e.__traceback__"),
    ("import sys\nsys.exc_info()[2].tb_frame.f_globals", "NBP-DUNDER-ESCAPE", "tb_frame.f_globals"),
    ("type(x).mro()", "NBP-DUNDER-ESCAPE", "type.mro"),
    ("obj.__dict__['secret']", "NBP-DUNDER-ESCAPE", "obj.__dict__"),
    ("getattr(obj, '__subclasses__')()", "NBP-DUNDER-ESCAPE", "obj.__subclasses__"),
    ("getattr(obj, '__globals__')", "NBP-DUNDER-ESCAPE", "obj.__globals__"),
    ("\"{0.__class__.__mro__}\".format(x)", "NBP-DUNDER-ESCAPE", "{0.__class__.__mro__}"),
    ("spark.__dict__", "NBP-DUNDER-ESCAPE", "spark.__dict__"),
    ("fn.__closure__[0].cell_contents", "NBP-DUNDER-ESCAPE", "fn.__closure__"),
    ("obj.__getattribute__('x')", "NBP-DUNDER-ESCAPE", "obj.__getattribute__"),
    ("m.__loader__.exec_module(m)", "NBP-DUNDER-ESCAPE", "m.__loader__"),
    ("m.__spec__.loader", "NBP-DUNDER-ESCAPE", "m.__spec__"),
    ("gen.gi_frame.f_globals", "NBP-DUNDER-ESCAPE", "gen.gi_frame"),
    ("import inspect\ninspect.stack()[1][0].f_globals", "NBP-DUNDER-ESCAPE", "<expr>.f_globals"),
    ("cls.__init_subclass__", "NBP-DUNDER-ESCAPE", "cls.__init_subclass__"),
    ("bound.__self__", "NBP-DUNDER-ESCAPE", "bound.__self__"),
    ("bound.__func__.__globals__", "NBP-DUNDER-ESCAPE", "bound.__func__"),
    ("obj.__reduce__()", "NBP-DUNDER-ESCAPE", "obj.__reduce__"),
    ("x.__class__.__base__", "NBP-DUNDER-ESCAPE", "x.__class__.__base__"),
]


@pytest.mark.parametrize("source,rule,target", DUNDER_ESCAPE_CELLS,
                         ids=[c[1] + ":" + c[0][:30] for c in DUNDER_ESCAPE_CELLS])
def test_dunder_escape_chains_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


# (4) IPython: the shell object, its imports, and %magic / !command lines inside a cell body.
IPYTHON_CELLS = [
    ("import IPython", "NBP-IPYTHON-ESCAPE", "IPython"),
    ("from IPython import get_ipython", "NBP-IPYTHON-ESCAPE", "IPython"),
    ("from IPython.core.interactiveshell import InteractiveShell", "NBP-IPYTHON-ESCAPE", "IPython"),
    ("ip = get_ipython()", "NBP-IPYTHON-ESCAPE", "get_ipython"),
    ("get_ipython().getoutput('id')", "NBP-OS-PROCESS", "get_ipython.getoutput"),
    ("get_ipython().run_line_magic('sh', 'id')", "NBP-OS-PROCESS", "get_ipython.run_line_magic"),
    ("get_ipython().run_cell_magic('bash', '', 'id')", "NBP-OS-PROCESS", "get_ipython.run_cell_magic"),
    ("get_ipython().magic('sh id')", "NBP-OS-PROCESS", "get_ipython.magic"),
    ("get_ipython().system_raw('id')", "NBP-OS-PROCESS", "get_ipython.system_raw"),
    ("get_ipython().ev('__import__(\"os\")')", "NBP-IPYTHON-ESCAPE", "get_ipython.ev"),
    ("get_ipython().ex('import os')", "NBP-IPYTHON-ESCAPE", "get_ipython.ex"),
    ("get_ipython().run_cell('import os')", "NBP-IPYTHON-ESCAPE", "get_ipython.run_cell"),
    ("get_ipython().user_ns['dbutils']", "NBP-IPYTHON-ESCAPE", "get_ipython.user_ns"),
    ("InteractiveShell.instance().system('id')", "NBP-IPYTHON-ESCAPE", "InteractiveShell"),
    ("gi = get_ipython\ngi().system('id')", "NBP-OS-PROCESS", "get_ipython.system"),
    ("x = 1\n!id", "NBP-OS-PROCESS", "!id"),
    ("x = 1\n%%bash\nid", "NBP-OS-PROCESS", "%%bash"),
    ("x = 1\n%sh id", "NBP-OS-PROCESS", "%sh"),
    ("x = 1\n%pip install requests", "NBP-OS-PROCESS", "%pip"),
    ("x = 1\n%matplotlib inline", "NBP-IPYTHON-ESCAPE", "%matplotlib"),
    ("x = 1\n%run ./other", "NBP-IPYTHON-ESCAPE", "%run"),
]


@pytest.mark.parametrize("source,rule,target", IPYTHON_CELLS, ids=[c[1] + ":" + c[0][:30] for c in IPYTHON_CELLS])
def test_ipython_escapes_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_magic_led_cell_is_skipped_whole_and_logged(tmp_path, sandbox_env):
    # A cell whose FIRST line is a magic is skipped as a unit: no line of it
    # is compiled or executed, and the skip is visible in the policy log.
    path = write_notebook(tmp_path, "child", ["%sh id\nMAGIC_MARK = 1", "!id\nBANG_MARK = 1", "AFTER_MARK = 1"])
    g = globals()
    for k in ("MAGIC_MARK", "BANG_MARK", "AFTER_MARK"):
        g.pop(k, None)
    try:
        assert runner(tmp_path).run(path) == "ok"
        assert "MAGIC_MARK" not in g and "BANG_MARK" not in g
        assert g["AFTER_MARK"] == 1
    finally:
        g.pop("AFTER_MARK", None)
    skipped = [e for e in npol.get_policy_log() if e["event"] == "skipped_magic"]
    assert [(e["cell_index"], e["target"]) for e in skipped] == [(0, "%sh id"), (1, "!id")]
    assert all(e["notebook_path"] == path and e["remediation"] for e in skipped)
    assert [e for e in npol.get_policy_log() if e["event"] == "refused"] == []


# (5) Spark JVM / py4j bridges: private Spark attributes and py4j handles.
JVM_BRIDGE_CELLS = [
    ("spark.sparkContext._jsc.sc()", "NBP-POLICY-TAMPER", "spark.sparkContext._jsc"),
    ("spark._jsparkSession", "NBP-POLICY-TAMPER", "spark._jsparkSession"),
    ("spark._gateway", "NBP-POLICY-TAMPER", "spark._gateway"),
    ("sc._gateway.jvm.java.lang.Runtime", "NBP-POLICY-TAMPER", "sc._gateway"),
    ("spark.sparkContext._gateway", "NBP-POLICY-TAMPER", "spark.sparkContext._gateway"),
    ("spark.sparkContext._conf._jconf", "NBP-POLICY-TAMPER", "spark.sparkContext._conf"),
    ("SparkContext._active_spark_context._jvm", "NBP-POLICY-TAMPER", "SparkContext._active_spark_context"),
    ("SparkSession._instantiatedSession._jvm", "NBP-POLICY-TAMPER", "SparkSession._instantiatedSession"),
    ("SparkSession._activeSession", "NBP-POLICY-TAMPER", "SparkSession._activeSession"),
    ("SparkSession.getActiveSession()._jvm", "NBP-POLICY-TAMPER", "SparkSession.getActiveSession._jvm"),
    ("from pyspark.sql import SparkSession\nSparkSession._instantiatedSession", "NBP-POLICY-TAMPER",
     "pyspark.sql.SparkSession._instantiatedSession"),
    ("df._jdf.sparkSession()", "NBP-POLICY-TAMPER", "df._jdf"),
    ("df.rdd._jrdd", "NBP-POLICY-TAMPER", "df.rdd._jrdd"),
    ("sqlContext._jsqlContext", "NBP-POLICY-TAMPER", "sqlContext._jsqlContext"),
    ("import py4j", "NBP-POLICY-TAMPER", "py4j"),
    ("from py4j.java_gateway import JavaGateway", "NBP-POLICY-TAMPER", "py4j"),
    ("JavaGateway()", "NBP-POLICY-TAMPER", "JavaGateway"),
    ("sp = spark\nsp._jvm", "NBP-POLICY-TAMPER", "spark._jvm"),
    ("ctx = spark.sparkContext\nctx._jvm", "NBP-POLICY-TAMPER", "spark.sparkContext._jvm"),
    ("spark.sparkContext.addPyFile('oci://b@ns/x/evil.zip')", "NBP-BUILTIN-EXEC", "spark.sparkContext.addPyFile"),
    ("sc.addJar('oci://b@ns/x/evil.jar')", "NBP-BUILTIN-EXEC", "sc.addJar"),
    ("spark.udf.registerJavaFunction('f', 'com.x.Evil')", "NBP-POLICY-TAMPER", "spark.udf.registerJavaFunction"),
    ("session = make()\nsession._jvm", "NBP-POLICY-TAMPER", "session._jvm"),
    ("spark._jvm = None", "NBP-POLICY-TAMPER", "spark._jvm"),
]


@pytest.mark.parametrize("source,rule,target", JVM_BRIDGE_CELLS, ids=[c[1] + ":" + c[0][:30] for c in JVM_BRIDGE_CELLS])
def test_jvm_bridges_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


# (6) Other process / exec / open routes.
PROCESS_EXEC_CELLS = [
    ("from concurrent.futures import ProcessPoolExecutor", "NBP-IMPORT-PROCESS",
     "concurrent.futures.ProcessPoolExecutor"),
    ("import concurrent.futures\nconcurrent.futures.ProcessPoolExecutor()", "NBP-IMPORT-PROCESS",
     "concurrent.futures.ProcessPoolExecutor"),
    ("from concurrent import futures\nfutures.ProcessPoolExecutor(2)", "NBP-IMPORT-PROCESS",
     "concurrent.futures.ProcessPoolExecutor"),
    ("import concurrent.futures.process", "NBP-IMPORT-PROCESS", "concurrent.futures.process"),
    ("import os\nos.posix_spawn('/bin/sh', ['sh'], {})", "NBP-OS-PROCESS", "os.posix_spawn"),
    ("import os\nos.forkpty()", "NBP-OS-PROCESS", "os.forkpty"),
    # (references only: if a regression ever let these through, the suite must not kill itself)
    ("import os\nkill = os.kill", "NBP-OS-PROCESS", "os.kill"),
    ("import os\nbye = os._exit", "NBP-POLICY-TAMPER", "os._exit"),
    ("import pickle\npickle.loads(b)", "NBP-DESERIALIZE", "pickle.loads"),
    ("import pickle\npickle.load(fh)", "NBP-DESERIALIZE", "pickle.load"),
    ("import pickle\npickle.Unpickler(fh).load()", "NBP-DESERIALIZE", "pickle.Unpickler"),
    ("import marshal\nmarshal.loads(b)", "NBP-DESERIALIZE", "marshal.loads"),
    ("import shelve\nshelve.open(p)", "NBP-DESERIALIZE", "shelve.open"),
    ("import dill\ndill.loads(b)", "NBP-DESERIALIZE", "dill.loads"),
    ("import cloudpickle\ncloudpickle.load(fh)", "NBP-DESERIALIZE", "cloudpickle.load"),
    ("import joblib\njoblib.load('m.pkl')", "NBP-DESERIALIZE", "joblib.load"),
    ("import pandas as pd\npd.read_pickle('x.pkl')", "NBP-DESERIALIZE", "pandas.read_pickle"),
    ("import yaml\nyaml.load(s)", "NBP-DESERIALIZE", "yaml.load"),
    ("import yaml\nyaml.load(s, Loader=yaml.Loader)", "NBP-DESERIALIZE", "yaml.load"),
    ("import yaml\nyaml.unsafe_load(s)", "NBP-DESERIALIZE", "yaml.unsafe_load"),
    ("import yaml\nyaml.full_load(s)", "NBP-DESERIALIZE", "yaml.full_load"),
    ("import torch\ntorch.load('m.pt')", "NBP-DESERIALIZE", "torch.load"),
    ("import torch\ntorch.load('m.pt', weights_only=False)", "NBP-DESERIALIZE", "torch.load"),
    ("import numpy as np\nnp.load('a.npy', allow_pickle=True)", "NBP-DESERIALIZE", "numpy.load(allow_pickle=True)"),
    ("import jsonpickle\njsonpickle.decode(s)", "NBP-DESERIALIZE", "jsonpickle.decode"),
    ("from pickle import loads\nloads(b)", "NBP-DESERIALIZE", "pickle.loads"),
    ("import cffi", "NBP-IMPORT-PROCESS", "cffi"),
    ("import signal", "NBP-IMPORT-PROCESS", "signal"),
    ("import resource", "NBP-IMPORT-PROCESS", "resource"),
    ("import psutil", "NBP-IMPORT-PROCESS", "psutil"),
    ("import pexpect", "NBP-IMPORT-PROCESS", "pexpect"),
    ("import _posixsubprocess", "NBP-IMPORT-PROCESS", "_posixsubprocess"),
    ("import code\ncode.interact()", "NBP-BUILTIN-EXEC", "code"),
    ("import pdb\npdb.set_trace()", "NBP-BUILTIN-EXEC", "pdb"),
    ("breakpoint()", "NBP-BUILTIN-EXEC", "breakpoint"),
    ("import timeit\ntimeit.timeit('__import__(\"os\")')", "NBP-BUILTIN-EXEC", "timeit"),
    ("import cProfile\ncProfile.run('import os')", "NBP-BUILTIN-EXEC", "cProfile"),
    ("import doctest", "NBP-BUILTIN-EXEC", "doctest"),
    ("import webbrowser", "NBP-IMPORT-PROCESS", "webbrowser"),
    ("import antigravity", "NBP-IMPORT-PROCESS", "antigravity"),
    ("import io\nio.open('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import io\nio.FileIO('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import os\nos.open('/etc/passwd', os.O_RDONLY)", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import codecs\ncodecs.open('/etc/passwd', encoding='utf-8')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import gzip\ngzip.open('/Volumes/prod/x.gz')", "NBP-OPEN-PATH", "/Volumes/prod/x.gz"),
    ("import tarfile\ntarfile.open('/Volumes/prod/x.tar')", "NBP-OPEN-PATH", "/Volumes/prod/x.tar"),
    ("import zipfile\nzipfile.ZipFile('/Workspace/.oci/config')", "NBP-OPEN-PATH", "/Workspace/.oci/config"),
    ("import io\nio.open(p)", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("import sqlite3\nsqlite3.connect('/Volumes/prod/x.db')", "NBP-OPEN-PATH", "/Volumes/prod/x.db"),
    ("import linecache\nlinecache.getline('/etc/passwd', 1)", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import os\nos.chdir('/Volumes/prod')", "NBP-FS-PATH", "/Volumes/prod"),
    ("import os\nos.chdir('no-such-dir/..')", "NBP-FS-PATH", "no-such-dir/.."),       # relative: cwd must stay put
    ("import os\nos.chdir(p)", "NBP-FS-DYNAMIC", "<dynamic>"),
    ("import types\ntypes.FunctionType(co, {})", "NBP-BUILTIN-EXEC", "types.FunctionType"),
    ("from types import CodeType", "NBP-BUILTIN-EXEC", "types.CodeType"),
    ("import asyncio\nasyncio.open_connection('h', 80)", "NBP-IMPORT-NETWORK", "asyncio.open_connection"),
    ("import socketserver", "NBP-IMPORT-NETWORK", "socketserver"),
    ("import ssl", "NBP-IMPORT-NETWORK", "ssl"),
]


@pytest.mark.parametrize("source,rule,target", PROCESS_EXEC_CELLS,
                         ids=[c[1] + ":" + c[0][:30] for c in PROCESS_EXEC_CELLS])
def test_other_process_exec_routes_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


# (7) Namespace / alias tracking across the cells of one run; a name rebound
#     to something unknown and then used is refused (fail closed).
class _Stub:
    def __init__(self, **kw):
        self.__dict__.update(kw)


ALIAS_TRACKING_CASES = [
    # (cells, rule, target, refused cell index, stub globals for the cells that run)
    (["x = __builtins__", "x['open']('/etc/passwd')"], "NBP-OPEN-PATH", "/etc/passwd", 1, {}),
    (["f = getattr", "f(os, 'system')('id')"], "NBP-INDIRECT", "os", 1, {}),
    (["sp = spark", "sp._jvm"], "NBP-POLICY-TAMPER", "spark._jvm", 1, {"spark": _Stub()}),
    (["ctx = spark.sparkContext", "ctx.addPyFile('oci://b@ns/x/e.zip')"], "NBP-BUILTIN-EXEC",
     "spark.sparkContext.addPyFile", 1, {"spark": _Stub(sparkContext=_Stub())}),
    (["b = __builtins__", "d = b.__dict__", "d['open']('/etc/passwd')"], "NBP-DUNDER-ESCAPE",
     "__builtins__.__dict__", 1, {}),
    (["g = globals", "ns = g()"], "NBP-INDIRECT", "globals()", 1, {}),
    (["gi = get_ipython", "gi().system('id')"], "NBP-OS-PROCESS", "get_ipython.system", 1,
     {"get_ipython": lambda: None}),
    (["ipy = get_ipython()", "ipy.system('id')"], "NBP-IPYTHON-ESCAPE", "get_ipython", 0, {}),
    (["from pickle import loads as unpack", "unpack(blob)"], "NBP-DESERIALIZE", "pickle.loads", 0, {}),
    (["import pickle", "P = pickle", "P.loads(b)"], "NBP-DESERIALIZE", "pickle.loads", 2, {}),
    (["import os as o", "o.popen('id')"], "NBP-OS-PROCESS", "os.popen", 1, {}),
    # rebound to something the gate cannot resolve -> every later use refused
    (["f = getattr", "f = make_getter()", "f(obj, 'x')"], "NBP-INDIRECT", "f", 2,
     {"make_getter": lambda: None}),
    (["sp = spark", "sp = build_session()", "sp.sparkContext.getConf()"], "NBP-INDIRECT", "sp", 2,
     {"spark": _Stub(), "build_session": lambda: None}),
    (["o = open", "o = my_open", "o('rel.txt')"], "NBP-INDIRECT", "o", 2, {"my_open": None}),
    (["import os", "os = fake_os()", "os.path.join('a', 'b')"], "NBP-INDIRECT", "os", 2,
     {"fake_os": lambda: None}),
    (["b = __builtins__", "b = {}", "b['open']"], "NBP-INDIRECT", "b", 2, {}),
]


@pytest.mark.parametrize("cells,rule,target,idx,stubs", ALIAS_TRACKING_CASES,
                         ids=[c[1] + ":" + " | ".join(c[0])[:40] for c in ALIAS_TRACKING_CASES])
def test_aliases_are_followed_across_cells_and_rebinding_fails_closed(tmp_path, sandbox_env, cells, rule,
                                                                        target, idx, stubs):
    _assert_refused(tmp_path, cells, rule, target, cell_index=idx, stubs=stubs)


def test_refused_cell_count_covers_the_revalidation_classes():
    refused = BUILTIN_INDIRECTION_CELLS + DYNAMIC_ACCESS_CELLS + DUNDER_ESCAPE_CELLS + IPYTHON_CELLS \
        + JVM_BRIDGE_CELLS + PROCESS_EXEC_CELLS
    assert len(refused) + len(ALIAS_TRACKING_CASES) >= 40
    for rule in ("NBP-DUNDER-ESCAPE", "NBP-IPYTHON-ESCAPE", "NBP-DESERIALIZE"):
        assert rule in npol.ALL_RULES


# (8) Clean notebooks keep working.
CLEAN_CELLS_2 = [
    "import pandas as pd\nimport numpy as np\ndf = pd.DataFrame({'a': [1]})\nx = np.array([1])",
    "import matplotlib.pyplot as plt\nplt.plot([1, 2])",
    "import plotly.express as px\nfig = px.bar(df, x='a')",
    "pd.options.display.max_columns = 50",
    "df2 = df.alias('d')",
    "x = spark.read.parquet('oci://b@ns/prod/in')",
    "df.write.mode('overwrite').parquet('oci://b@ns/sbx/out')",
    "dbutils.widgets.text('p', '1')\nv = dbutils.widgets.get('p')",
    "dbutils.notebook.exit('ok')",
    "display(df)",
    "s = 'a {}'.format(1)",
    "n = f'{x} rows'",
    "d = {'key': 1}\nv = d['key']",
    "class Model:\n    def __init__(self, n):\n        self.n = n\n    def __repr__(self):\n        return f'Model({self.n})'",
    "class B(A):\n    def __init__(self):\n        super().__init__()",
    "name = type(df).__name__",
    "m = df.__class__.__name__",
    "import yaml\ncfg = yaml.safe_load(s)",
    "import yaml\ncfg = yaml.load(s, Loader=yaml.SafeLoader)",
    "from yaml import load, SafeLoader\ncfg = load(s, Loader=SafeLoader)",
    "import torch\nm = torch.load('m.pt', weights_only=True)",
    "import numpy as np\na = np.load('a.npy')",
    "import pickle\npickle.dump(obj, fh)",
    "from concurrent.futures import ThreadPoolExecutor",
    "import json\nj = json.loads(s)",
    "v = globals()['my_var']",
    "globals()['result'] = 1",
    "import os\np = os.path.join('a', 'b')",
    "import sys\nprint(sys.version)",
    "with open('rel.txt', 'w') as fh:\n    fh.write('x')",
    "import sqlite3\nc = sqlite3.connect(':memory:')",
    "import io\nbuf = io.BytesIO(b'x')",
    "import inspect\nsig = inspect.signature(fn)",
    "row.__fields__",
    "spark.sparkContext.setLogLevel('WARN')",
    "spark.conf.get('spark.sql.shuffle.partitions')",
    "from pyspark.sql import SparkSession, functions as F\nc = F.col('a')",
    "from pyspark.sql.functions import col, lit",
    "import logging\nlog = logging.getLogger(__name__)",
    "setattr(obj, 'x', 1)",
    "y = getattr(df, 'count')()",
    "x = hasattr(df, 'write')",
    "def helper(spark, df):\n    return df.count()",
    "import gc\ngc.collect()",
    "sc = spark.sparkContext\nn = sc.defaultParallelism",
    "import os as o\nimport sys as s\no2 = o\nE = o2.path.sep",
    "try:\n    risky()\nexcept Exception as e:\n    print(str(e))",
    "total = sum(len(x) for x in rows)",
    "pdf = df.toPandas()",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_2, ids=[c[:40] for c in CLEAN_CELLS_2])
def test_clean_cell_passes_the_hardened_gate(sandbox_env, source):
    assert npol.scan_source(source) == []


def test_chdir_inside_the_sandbox_prefix_is_allowed(sandbox_env):
    inside = sandbox_env["local_prefix"] + "work"
    assert npol.scan_source(f"import os\nos.chdir({inside!r})") == []


def test_clean_notebook_with_dunder_definitions_and_aliases_executes(tmp_path, sandbox_env):
    path = write_notebook(tmp_path, "child", [
        "import json\nimport os as _o",
        "class _Acc:\n    def __init__(self):\n        self.items = []\n    def __len__(self):\n"
        "        return len(self.items)\n    def __repr__(self):\n        return 'Acc({})'.format(len(self))",
        "class _Sub(_Acc):\n    def __init__(self):\n        super().__init__()\n        self.items.append(1)",
        "acc = _Sub()\nsetattr(acc, 'tag', 'x')\nkind = type(acc).__name__",
        "globals()['CLEAN_MARK'] = json.dumps({'n': len(acc), 'kind': kind, 'tag': getattr(acc, 'tag'),"
        " 'sep': _o.path.sep})",
    ])
    g = globals()
    for k in ("CLEAN_MARK", "acc", "kind", "_Acc", "_Sub"):
        g.pop(k, None)
    try:
        assert runner(tmp_path).run(path) == "ok"
        assert json.loads(g["CLEAN_MARK"]) == {"n": 1, "kind": "_Sub", "tag": "x", "sep": os.path.sep}
    finally:
        for k in ("CLEAN_MARK", "acc", "kind", "_Acc", "_Sub", "_o"):
            g.pop(k, None)
    assert [e for e in npol.get_policy_log() if e["event"] in ("refused", "allowed_exception")] == []


# ══════════════════════════════════════════════════════════════════════
# SEC-AIDP-SAMPLES-005 round 2: gaps from the static review of the round-2 baseline gate
# ══════════════════════════════════════════════════════════════════════
# (9) Each gap is closed by generalising the rule, not the spelling: posix / nt
#     are os; the indirect (getattr) route consults the same rule table as the
#     direct one; str.format templates are resolved and walked; library readers
#     refuse literal local paths; a dynamic spark.sql statement needs a literal
#     read-keyword head.
ROUND2_GAP_CELLS = [
    # -- (1) posix / nt are the modules os re-exports: every os rule applies
    ("import posix\nposix.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import posix\nTOKEN = posix.environ.get('AIDP_TOKEN')", "NBP-OS-ENVIRON", "os.environ"),
    ("import posix\nposix.open('/etc/passwd', 0)", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import posix\nposix.remove('/Volumes/prod/x')", "NBP-FS-PATH", "/Volumes/prod/x"),
    ("import posix\nposix.fork()", "NBP-OS-PROCESS", "os.fork"),
    ("import posix\nposix.environ = {}", "NBP-OS-ENVIRON", "os.environ"),
    ("from posix import system", "NBP-OS-PROCESS", "os.system"),
    ("from posix import environ as _e", "NBP-OS-ENVIRON", "os.environ"),
    ("from posix import *", "NBP-OS-ENVIRON", "os.*"),
    ("import posix as p\np.popen('id')", "NBP-OS-PROCESS", "os.popen"),
    ("import nt\nnt.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import nt as n\nn.environ['X']", "NBP-OS-ENVIRON", "os.environ"),
    # -- (2) the indirect route consults the same rule table as direct access
    ("import asyncio\nasyncio.run(getattr(asyncio, 'create_subprocess_exec')('id'))", "NBP-OS-PROCESS",
     "asyncio.create_subprocess_exec"),
    ("import asyncio\nf = getattr(asyncio, 'create_subprocess_shell')", "NBP-OS-PROCESS",
     "asyncio.create_subprocess_shell"),
    ("import asyncio\nloop = asyncio.get_event_loop()\nloop.subprocess_exec(lambda: None, 'id')", "NBP-OS-PROCESS",
     "loop.subprocess_exec"),
    ("import asyncio\nloop = asyncio.get_event_loop()\ngetattr(loop, 'subprocess_shell')", "NBP-OS-PROCESS",
     "loop.subprocess_shell"),
    ("getattr(spark.sparkContext, 'addPyFile')('oci://b@ns/x/e.zip')", "NBP-BUILTIN-EXEC",
     "spark.sparkContext.addPyFile"),
    ("import os\nfrom os import system as run_cmd\nrun_cmd('id')", "NBP-OS-PROCESS", "os.system"),
    # -- (3) str.format: templates are resolved and every field is walked
    ("t = '{0.__class__.__mro__}'\nt.format(obj)", "NBP-DUNDER-ESCAPE", "{0.__class__.__mro__}"),
    ("'{x.__init__.__globals__[os].environ}'.format_map(dict(x=obj))", "NBP-DUNDER-ESCAPE",
     "{x.__init__.__globals__[os].environ}"),
    ("t = '{x.__init__.__globals__}'\nt.format_map(dict(x=obj))", "NBP-DUNDER-ESCAPE", "{x.__init__.__globals__}"),
    ("str.format('{0.__class__.__mro__}', obj)", "NBP-DUNDER-ESCAPE", "{0.__class__.__mro__}"),
    ("str.format_map('{x.__globals__}', dict(x=f))", "NBP-DUNDER-ESCAPE", "{x.__globals__}"),
    ("import string\nstring.Formatter().format('{0.__subclasses__}', obj)", "NBP-DUNDER-ESCAPE",
     "{0.__subclasses__}"),
    ("import string\nstring.Formatter().vformat('{0.__code__}', (f,), {})", "NBP-DUNDER-ESCAPE", "{0.__code__}"),
    ("import textwrap\ntextwrap.dedent('  {0.__class__.__mro__}').format(obj)", "NBP-DUNDER-ESCAPE",
     "{0.__class__.__mro__}"),
    ("('{0.__class__.' + '__mro__}').format(obj)", "NBP-DUNDER-ESCAPE", "{0.__class__.__mro__}"),
    ("'{0:{1.__class__.__mro__}}'.format(a, b)", "NBP-DUNDER-ESCAPE", "{1.__class__.__mro__}"),   # nested spec
    ("'{0.f_globals}'.format(frame)", "NBP-DUNDER-ESCAPE", "{0.f_globals}"),
    ("tpl = dbutils.widgets.get('tpl')\ntpl.format(obj)", "NBP-DUNDER-ESCAPE", "<dynamic>"),        # fail closed
    ("TEMPLATES['x'].format(row=row)", "NBP-DUNDER-ESCAPE", "<dynamic>"),
    ("tpl = 'a {0}'\ntpl = 'b {0}'\ntpl.format(obj)", "NBP-DUNDER-ESCAPE", "<dynamic>"),            # rebound: unknown
    # -- (4) local file reads through library readers (literal local path outside the prefix)
    ("import pandas as pd\npd.read_csv('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import pandas as pd\npd.read_parquet('~/.oci/config')", "NBP-OPEN-PATH", "~/.oci/config"),
    ("import pandas as pd\npd.read_json(path_or_buf='/Workspace/.oci/config')", "NBP-OPEN-PATH",
     "/Workspace/.oci/config"),
    ("import pandas as pd\npd.read_csv('C:\\\\Users\\\\x\\\\secret.csv')", "NBP-OPEN-PATH", "C:\\Users\\x\\secret.csv"),
    ("from pandas import read_csv\nread_csv('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import pandas as pd\nP = '/etc/passwd'\npd.read_csv(P)", "NBP-OPEN-PATH", "/etc/passwd"),      # resolved constant
    ("spark.read.text('file:///etc/passwd')", "NBP-OPEN-PATH", "file:///etc/passwd"),
    ("spark.read.csv('/dbfs/mnt/prod/x.csv')", "NBP-OPEN-PATH", "/dbfs/mnt/prod/x.csv"),
    ("spark.read.format('csv').load('/Workspace/.oci/config')", "NBP-OPEN-PATH", "/Workspace/.oci/config"),
    ("spark.read.option('path', '/etc/passwd').load()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.read.options(path='/etc/passwd').load()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.read.load(path='/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.readStream.parquet('/tmp/other/in')", "NBP-OPEN-PATH", "/tmp/other/in"),
    ("rdr = spark.read\nrdr.json('/etc/x.json')", "NBP-OPEN-PATH", "/etc/x.json"),
    ("sc.textFile('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sparkContext.wholeTextFiles('/Volumes/prod/docs')", "NBP-OPEN-PATH", "/Volumes/prod/docs"),
    ("import numpy as np\nnp.loadtxt('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("import numpy as np\nnp.load('/Volumes/prod/a.npy')", "NBP-OPEN-PATH", "/Volumes/prod/a.npy"),
    ("import pyarrow.parquet as pq\npq.read_table('/Volumes/prod/x.parquet')", "NBP-OPEN-PATH",
     "/Volumes/prod/x.parquet"),
    ("import polars as pl\npl.scan_csv('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from PIL import Image\nImage.open('/Workspace/.ssh/id_rsa')", "NBP-OPEN-PATH", "/Workspace/.ssh/id_rsa"),
    ("import xml.etree.ElementTree as ET\nET.parse('/etc/x.xml')", "NBP-OPEN-PATH", "/etc/x.xml"),
    ("import h5py\nh5py.File('/Volumes/prod/m.h5', 'r')", "NBP-OPEN-PATH", "/Volumes/prod/m.h5"),
    ("open('~/.oci/config').read()", "NBP-OPEN-PATH", "~/.oci/config"),
    ("open('C:\\\\Users\\\\x\\\\.oci\\\\config').read()", "NBP-OPEN-PATH", "C:\\Users\\x\\.oci\\config"),
    # -- (c) a dynamic spark.sql statement needs a LITERAL read-keyword head
    ("spark.sql(f'{verb} TABLE prod.t')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('  ' + q)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('/* c */ ' + q)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('-- c\\n' + q)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(f'USE {db}')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(verb + ' TABLE prod.t')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('SELECTX ' + q)", "NBP-SQL-DYNAMIC", "<dynamic>"),
]


@pytest.mark.parametrize("source,rule,target", ROUND2_GAP_CELLS,
                         ids=[c[1] + ":" + c[0][:30] for c in ROUND2_GAP_CELLS])
def test_round2_gap_cells_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_posix_alias_bound_in_an_earlier_cell_is_followed(sandbox_env):
    # (scanned, not executed: ``posix`` only exists on POSIX hosts)
    ctx = npol.ScanContext()
    assert npol.scan_source("import posix as p", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("p.system('id')", context=ctx)] \
        == [("NBP-OS-PROCESS", "os.system")]
    ctx = npol.ScanContext()
    assert npol.scan_source("import nt", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("nt.environ['X']", context=ctx)] \
        == [("NBP-OS-ENVIRON", "os.environ")]


def test_format_template_bound_in_an_earlier_cell_is_resolved(tmp_path, sandbox_env):
    _assert_refused(tmp_path, ["TPL = 'kind: {0.__class__.__mro__}'", "TPL.format(obj)"], "NBP-DUNDER-ESCAPE",
                    "{0.__class__.__mro__}", cell_index=1)


# (10) False refusals fixed: legitimate code the gate wrongly refused.
CLEAN_CELLS_3 = [
    # (a) format fields consult _DUNDER_ALLOW
    "'{0.__class__.__name__}'.format(obj)",
    "'{0.__class__}'.format(obj)",
    "'{x[__class__]}'.format(x=d)",                                           # item lookup, not attribute access
    "'{0:>{1}}'.format(a, 8)",
    "msg = 'rows: {n}'\nout = msg.format(n=count)",
    "import textwrap\nmsg = textwrap.dedent('''\n  rows: {n}''').format(n=count)",
    "w = df.write.mode('overwrite')\nw.format('delta').save('oci://b@ns/sbx/x')",
    "fmt = 'delta'\nw = df.write.mode('overwrite')\nw.format(fmt).save('oci://b@ns/sbx/x')",
    "df.write.format(fmt).save('oci://b@ns/sbx/x')",
    "spark.read.format(fmt).load(src)",
    # (b) getattr on a Spark object with a literal public name
    "getattr(spark, 'version')",
    "v = getattr(spark, 'version')",
    "n = getattr(sc, 'defaultParallelism')",
    "getattr(spark.conf, 'get')('spark.app.name')",
    # (c) dynamic read-only SQL with a literal read-keyword head: moved to
    #     SQL_DYNAMIC_READ_CELLS, allowed only with the runtime SQL guard active
    # (4) dynamic, object-storage, catalog and relative reads stay allowed
    "import pandas as pd\npdf = pd.read_csv(src_path)",
    "spark.read.csv(path, header=True)",
    "spark.read.format('parquet').load(src)",
    "import pandas as pd\npdf = pd.read_csv('oci://b@ns/prod/x.csv')",
    "spark.read.parquet('abfss://c@a.dfs.core.windows.net/prod/in')",
    "spark.read.parquet('s3://bucket/prod/in')",
    "spark.read.parquet('oci://b@ns/sbx/in')",
    "spark.read.table('prod.t')",
    "import pandas as pd\npdf = pd.read_csv('data/local.csv')",
    "import pandas as pd\npdf = pd.read_sql('SELECT 1', con)",
    "import numpy as np\na = np.load('a.npy')",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_3, ids=[c[:40] for c in CLEAN_CELLS_3])
def test_round2_false_refusals_pass_the_gate(sandbox_env, source):
    assert npol.scan_source(source) == []


def test_local_reads_inside_the_sandbox_prefix_are_allowed(sandbox_env):
    inside = sandbox_env["local_prefix"] + "in.csv"
    assert npol.scan_source(f"import pandas as pd\npdf = pd.read_csv({inside!r})") == []
    pol = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/", extra_prefixes=("/tmp/sbx/",))
    assert npol.scan_source("spark.read.csv('file:///tmp/sbx/in.csv')", policy=pol) == []
    assert npol.scan_source("spark.read.csv('/tmp/sbx/in.csv')", policy=pol) == []
    assert [v.rule for v in npol.scan_source("spark.read.csv('file:///tmp/other/in.csv')", policy=pol)] \
        == ["NBP-OPEN-PATH"]


def test_unverified_reads_are_allowed_and_recorded(sandbox_env):
    src = ("import pandas as pd\n"
           "pdf = pd.read_csv(src_path)\n"
           "rows = spark.sql('SELECT * FROM sbx.t WHERE id=' + str(cid))")
    # the dynamic SELECT is admitted only while the runtime SQL guard is active for the run
    ctx = npol.ScanContext(runtime_sql_guard=True)
    assert npol.scan_source(src, context=ctx) == []
    assert npol.enforce_cell_policy(src, notebook_path="nb", cell_index=3,
                                    context=npol.ScanContext(runtime_sql_guard=True)) == []
    notes = [e for e in npol.get_policy_log() if e["event"] in npol.POLICY_NOTES]
    assert [(e["event"], e["target"], e["cell_index"], e["notebook_path"]) for e in notes] == [
        ("unverified_read_path", "pandas.read_csv(<dynamic>)", 3, "nb"),
        ("unverified_sql_read", "SELECT * FROM sbx.t WHERE id=", 3, "nb"),
    ]
    assert [e["detail"]["line"] for e in notes] == [2, 3]
    assert all(e["rule"] == "" and e["remediation"] == "" for e in notes)
    assert "unverified_read_path" in npol.policy_log_markdown()
    assert [e for e in npol.get_policy_log() if e["event"] in ("refused", "allowed_exception")] == []


def test_verified_reads_are_not_recorded(sandbox_env):
    npol.enforce_cell_policy("x = spark.read.parquet('oci://b@ns/prod/in')\ny = spark.sql('SELECT 1')",
                             notebook_path="nb", cell_index=0)
    assert [e for e in npol.get_policy_log() if e["event"] in npol.POLICY_NOTES] == []


# (11) Review-derived gap classes (automated review of the round-2 commits plus
#      a probe run against the round-2 baseline gate), each closed as a CLASS of route:
#      A  a gated module / Spark object handed around as a value;
#      B  a process / introspection sink name on a receiver the gate cannot resolve;
#      C  RDD.pipe(cmd) and string-typed .pipe() arguments;
#      D  string-keyed attribute accessors (attrgetter / methodcaller / reduce(getattr) /
#         inspect.getattr_static / getmembers);
#      E  re-rooting at a module named mid-chain (shutil.os, pd.io.common.os, self.spark);
#      F  deserialisers: pickle deny-by-default, private attributes of gated modules,
#         torch.load at any depth, numpy allow_pickle, yaml loaders / constructors / hooks;
#      G  globals()[k] with a tainted / alias / protected key or a dereferenced result;
#      H  open() with *args / **kwargs; I  pydoc joins the process modules.
#      The 100 probe cells below are the acceptance test, verbatim, with the first
#      refusal the runner reports; every entry passed the gate at the round-2 baseline gate or was
#      refused for the wrong reason there.
REVIEW_GAP_CELLS = [
    ("f = os\nf.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("[os][0].system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("(os if True else sys).system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("def run(m):\n    m.system('id')\nrun(os)", 'NBP-INDIRECT', 'm.system'),
    ("m = {'a': os}['a']\nm.system('id')", 'NBP-INDIRECT', 'os'),
    ("def get():\n    return os\nget().system('id')", 'NBP-INDIRECT', 'os'),
    ("(lambda: os)().system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("import os as _o\nd = dict(m=_o)\nd['m'].system('id')", 'NBP-INDIRECT', 'os'),
    ("for m in (os,):\n    m.system('id')", 'NBP-INDIRECT', 'os'),
    ("mods = [os]\nmods[0].popen('id')", 'NBP-INDIRECT', 'os'),
    ("helper().system('id')", 'NBP-INDIRECT', 'helper.system'),
    ("obj.popen('id')", 'NBP-INDIRECT', 'obj.popen'),
    ("thing.open('/etc/passwd').read()", 'NBP-OPEN-PATH', '/etc/passwd'),
    ('pickle._loads(b)', 'NBP-DESERIALIZE', 'pickle._loads'),
    ('pickle._load(f)', 'NBP-DESERIALIZE', 'pickle._load'),
    ('pickle._Unpickler(f).load()', 'NBP-DESERIALIZE', 'pickle._Unpickler'),
    ('pickle.Unpickler(f).load()', 'NBP-DESERIALIZE', 'pickle.Unpickler'),
    ('import yaml\nL = yaml.Loader\nyaml.load(s, Loader=L)', 'NBP-DESERIALIZE', 'yaml.Loader'),
    ("yaml.load(s, Loader=getattr(yaml, 'Loader'))", 'NBP-DESERIALIZE', 'yaml.load'),
    ('yaml.load(s, yaml.UnsafeLoader)', 'NBP-DESERIALIZE', 'yaml.load'),
    ('yaml.full_load(s)', 'NBP-DESERIALIZE', 'yaml.full_load'),
    ('yaml.unsafe_load(s)', 'NBP-DESERIALIZE', 'yaml.unsafe_load'),
    ('yaml.load_all(s)', 'NBP-DESERIALIZE', 'yaml.load_all'),
    ('yaml.full_load_all(s)', 'NBP-DESERIALIZE', 'yaml.full_load_all'),
    ('torch.load(p)', 'NBP-DESERIALIZE', 'torch.load'),
    ('torch.load(p, weights_only=flag)', 'NBP-DESERIALIZE', 'torch.load'),
    ('torch.serialization.load(p)', 'NBP-DESERIALIZE', 'torch.serialization.load'),
    ('torch.jit.load(p)', 'NBP-DESERIALIZE', 'torch.jit.load'),
    ('np.load(p, allow_pickle=flag)', 'NBP-DESERIALIZE', 'np.load(allow_pickle=<dynamic>)'),
    ('np.load(p, allow_pickle=not False)', 'NBP-DESERIALIZE', 'np.load(allow_pickle=<dynamic>)'),
    ('marshal.loads(b)', 'NBP-DESERIALIZE', 'marshal.loads'),
    ('shelve.open(p)', 'NBP-DESERIALIZE', 'shelve.open'),
    ('dill.loads(b)', 'NBP-DESERIALIZE', 'dill.loads'),
    ('cloudpickle.loads(b)', 'NBP-DESERIALIZE', 'cloudpickle.loads'),
    ('jsonpickle.decode(s)', 'NBP-DESERIALIZE', 'jsonpickle.decode'),
    ('pd.read_pickle(p)', 'NBP-DESERIALIZE', 'pd.read_pickle'),
    ('joblib.load(p)', 'NBP-DESERIALIZE', 'joblib.load'),
    ("sc.parallelize(['x']).pipe('id').collect()", 'NBP-OS-PROCESS', 'sc.parallelize.pipe()'),
    ("rdd.pipe('id')", 'NBP-OS-PROCESS', 'rdd.pipe()'),
    ("spark.sparkContext.textFile('/tmp/x').pipe('id')", 'NBP-OPEN-PATH', '/tmp/x'),
    ("import pydoc\npydoc.pipepager('x', 'id')", 'NBP-IMPORT-PROCESS', 'pydoc'),
    ("pydoc.tempfilepager('x', 'id')", 'NBP-OS-PROCESS', 'pydoc.tempfilepager'),
    ("pydoc.pager('x')", 'NBP-OS-PROCESS', 'pydoc.pager'),
    ("o = os\nglobals()['o'].system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("s = spark\nglobals()['s']._jvm", 'NBP-INDIRECT', 'globals()'),
    ("import importlib as il\nglobals()['il'].import_module('os').system('id')", 'NBP-INDIRECT', 'importlib'),
    ("b = __builtins__\nglobals()['b']['open']('/etc/passwd')", 'NBP-INDIRECT', 'globals()'),
    ('from yaml import Loader\nyaml.load(s, Loader)', 'NBP-DESERIALIZE', 'yaml.Loader'),
    ('from yaml import Loader as SafeLoader\nyaml.load(s, Loader=SafeLoader)', 'NBP-DESERIALIZE', 'yaml.Loader'),
    ('yaml.load(s, Loader=yaml.loader.Loader)', 'NBP-DESERIALIZE', 'yaml.load'),
    ('yaml.load(s, Loader=yaml.loader.UnsafeLoader)', 'NBP-DESERIALIZE', 'yaml.load'),
    ("class L(yaml.SafeLoader):\n    pass\nL.add_constructor('!x', lambda l, n: __import__('os').system('id'))\n"
     "yaml.load(s, Loader=L)", 'NBP-DESERIALIZE', 'yaml.SafeLoader'),
    ('class C(yaml.constructor.UnsafeConstructor):\n    pass', 'NBP-DESERIALIZE', 'yaml.constructor.UnsafeConstructor'),
    ('yaml.constructor.FullConstructor', 'NBP-DESERIALIZE', 'yaml.constructor'),
    ("getattr(pickle, 'loads')(b)", 'NBP-DESERIALIZE', 'pickle.loads'),
    ('torch.serialization._load(p)', 'NBP-DESERIALIZE', 'torch.serialization._load'),
    ("import operator\noperator.attrgetter('_jvm')(spark).java.lang.Runtime.getRuntime().exec('id')",
     'NBP-INDIRECT', 'spark'),
    ("operator.attrgetter('__class__.__base__.__subclasses__')(())()", 'NBP-DUNDER-ESCAPE', '<expr>.__base__'),
    ("operator.methodcaller('system', 'id')(os)", 'NBP-INDIRECT', "operator.methodcaller('system')"),
    ("from operator import attrgetter\nattrgetter('system')(os)('id')", 'NBP-INDIRECT', "operator.attrgetter('system')"),
    ("ag = operator.attrgetter\nag('_gateway')(sc)", 'NBP-INDIRECT', 'sc'),
    ("import shutil\nshutil.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import subprocess\nsubprocess.os.system('id')", 'NBP-IMPORT-PROCESS', 'subprocess'),
    ("import pathlib\npathlib.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import pandas as pd\npd.io.common.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import numpy\nnumpy.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("vars(os)['system']('id')", 'NBP-INDIRECT', 'os'),
    ("vars(shutil)['os'].system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("kw = {'file': '/etc/passwd'}\nopen(**kw).read()", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ("args = ['/etc/passwd']\nopen(*args).read()", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('io.open(**kw)', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ("for m in [os]:\n    m.system('id')", 'NBP-INDIRECT', 'os'),
    ("with os as m:\n    m.system('id')", 'NBP-INDIRECT', 'os'),
    ("[m.system('id') for m in (os,)]", 'NBP-INDIRECT', 'm.system'),
    ("def f(m=os):\n    m.system('id')\nf()", 'NBP-INDIRECT', 'os'),
    ("(m := os).system('id')", 'NBP-INDIRECT', '<expr>.system'),
    ("import functools\nfunctools.reduce(getattr, ['_jvm', 'java', 'lang', 'Runtime'], spark).getRuntime().exec('id')",
     'NBP-INDIRECT', 'functools.reduce(getattr, ...)'),
    ("import inspect\ninspect.getattr_static(spark, '_jvm')", 'NBP-INDIRECT', 'spark'),
    ("dict(inspect.getmembers(spark))['_jvm']", 'NBP-INDIRECT', 'inspect.getmembers'),
    ('inspect.getmembers(os)', 'NBP-INDIRECT', 'inspect.getmembers'),
    ("open(p, 'w').write(data)", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('open(p, mode).write(data)', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('with open(f"{work_dir}/out.txt", \'a\') as fh:\n    fh.write(data)', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ("p = '/etc/' + 'passwd'\nopen(p).read()", 'NBP-OPEN-PATH', '/etc/passwd'),
    ("parts = ['/etc', 'passwd']\nopen('/'.join(parts)).read()", 'NBP-OPEN-PATH', '/etc/passwd'),
    ("open('/etc/passwd', 'r').read()", 'NBP-OPEN-PATH', '/etc/passwd'),
    ("io.open(path_var, 'wb').write(data)", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    # open() with a path the gate cannot resolve is refused in EVERY mode: a read of an
    # arbitrary local file leaves the sandbox as much as a write (the library-reader
    # scope decision does not extend to open() and its stdlib / pathlib twins)
    ('open(path_var).read()', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('import json\nwith open(f"{work_dir}/manifest.json") as fh:\n    manifest = json.load(fh)',
     'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ("open(path_var, 'r').read()", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('io.open(path_var).read()', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ("import codecs\ncodecs.open(path_var, encoding='utf-8').read()", 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('from pathlib import Path\nPath(path_var).open().read()', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    ('Path(path_var).read_text()', 'NBP-OPEN-DYNAMIC', '<dynamic>'),
    # re-rooting through a module's re-export of os, public or private (tempfile._os, random._os)
    ("import tempfile\ntempfile._os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import random\nrandom._os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import glob\nglob.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import posixpath\nposixpath.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import platform\nplatform.os.system('id')", 'NBP-OS-PROCESS', 'os.system'),
    ("import tarfile\ntarfile.os.environ['X']", 'NBP-OS-ENVIRON', 'os.environ'),
]

# Further cells per class, so each rule is exercised on its own and not only
# through the first refusal of a probe.
REVIEW_GAP_CELLS_2 = [
    # A: value positions
    ("run(*[spark])", "NBP-INDIRECT", "spark"),
    ("def get_session():\n    return spark", "NBP-INDIRECT", "spark"),
    ("mods = {'o': sys}", "NBP-INDIRECT", "sys"),
    ("import pickle\nloaders = (pickle,)", "NBP-INDIRECT", "pickle"),
    ("fn = lambda: dbutils", "NBP-INDIRECT", "dbutils"),
    # B: sinks on unresolved receivers
    ("get_module().putenv('A', 'b')", "NBP-INDIRECT", "get_module.putenv"),
    ("mods[0].execvp('id', ['id'])", "NBP-INDIRECT", "<expr>.execvp"),
    ("x.fork()", "NBP-INDIRECT", "x.fork"),
    ("x.getattr_static(spark, 'version')", "NBP-INDIRECT", "x.getattr_static"),
    ("platform.system()", "NBP-INDIRECT", "platform.system"),      # no import in view: unknown root
    ("fs.open('/etc/passwd').read()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("fs.open('/Volumes/prod/' + name)", "NBP-OPEN-PATH", "/Volumes/prod/"),
    # C: .pipe
    ("spark.sparkContext.textFile('oci://b@ns/x').pipe('id')", "NBP-OS-PROCESS", "spark.sparkContext.textFile.pipe()"),
    ("rdd = sc.parallelize(xs)\nrdd.pipe(fn)", "NBP-OS-PROCESS", "rdd.pipe()"),
    ("r = df.rdd\nr.pipe(fn)", "NBP-OS-PROCESS", "df.rdd.pipe()"),       # alias followed
    ("df.rdd.pipe(fn)", "NBP-OS-PROCESS", "df.rdd.pipe()"),
    ("cmd = 'id'\ndf.pipe(cmd)", "NBP-OS-PROCESS", "df.pipe()"),
    ("df.pipe(f'{tool} -x')", "NBP-OS-PROCESS", "df.pipe()"),
    ("df.pipe('/bin/' + tool)", "NBP-OS-PROCESS", "df.pipe()"),
    ("df.pipe('{} -x'.format(tool))", "NBP-OS-PROCESS", "df.pipe()"),
    ("df.pipe(' '.join(argv))", "NBP-OS-PROCESS", "df.pipe()"),
    # D: string-keyed accessors
    ("import operator\noperator.attrgetter(name)(obj)", "NBP-INDIRECT", "operator.attrgetter(<dynamic>)"),
    ("import operator\noperator.methodcaller('__reduce__')(obj)", "NBP-DUNDER-ESCAPE", "<expr>.__reduce__"),
    ("import operator\noperator.attrgetter('a.f_globals')(obj)", "NBP-DUNDER-ESCAPE", "<expr>.f_globals"),
    ("import operator\noperator.attrgetter('_conf')(sc)", "NBP-INDIRECT", "operator.attrgetter('_conf')"),
    ("import operator\noperator.attrgetter('x._secret')(obj)", "NBP-INDIRECT", "operator.attrgetter('x._secret')"),
    ("from functools import reduce\nreduce(getattr, path, obj)", "NBP-INDIRECT", "functools.reduce(getattr, ...)"),
    ("import functools, operator\nfunctools.reduce(operator.attrgetter, path, obj)", "NBP-INDIRECT",
     "functools.reduce(getattr, ...)"),
    ("import inspect\ninspect.getattr_static(os, 'system')", "NBP-INDIRECT", "os"),
    ("import inspect\ninspect.getattr_static(obj, name)", "NBP-INDIRECT", "getattr(obj, <dynamic>)"),
    ("from inspect import getmembers\ngetmembers(obj)", "NBP-INDIRECT", "inspect.getmembers"),
    ("obj.__getattribute__('x')", "NBP-DUNDER-ESCAPE", "obj.__getattribute__"),
    # E: re-rooting and vars()
    ("self.spark._jvm", "NBP-POLICY-TAMPER", "spark._jvm"),
    ("ctx.sc.addPyFile('oci://b@ns/x/e.zip')", "NBP-BUILTIN-EXEC", "sc.addPyFile"),
    ("import pandas as pd\npd.io.common.os.environ['X']", "NBP-OS-ENVIRON", "os.environ"),
    ("import shutil\nshutil.sys.modules['os']", "NBP-INDIRECT", "sys.modules"),
    ("import pickle\nvars(pickle)['loads']", "NBP-INDIRECT", "pickle"),
    ("import yaml\nvars(yaml)", "NBP-INDIRECT", "yaml"),
    ("vars(sc)", "NBP-INDIRECT", "sc"),
    # F: deserialisers
    ("import pickle\npickle.Unpickler", "NBP-DESERIALIZE", "pickle.Unpickler"),
    ("import pickle\npickle.load_session(p)", "NBP-DESERIALIZE", "pickle.load_session"),
    ("import dill\ndill.load_module(p)", "NBP-DESERIALIZE", "dill.load_module"),
    ("import pickle\npickle._getattribute(obj, 'x')", "NBP-DESERIALIZE", "pickle._getattribute"),
    ("import yaml\nyaml._x", "NBP-DESERIALIZE", "yaml._x"),
    ("import operator\noperator._compare_digest", "NBP-INDIRECT", "operator._compare_digest"),
    ("import torch\ntorch.jit.load(p, map_location='cpu')", "NBP-DESERIALIZE", "torch.jit.load"),
    ("import torch\ntorch.serialization.load(p, weights_only=False)", "NBP-DESERIALIZE", "torch.serialization.load"),
    ("import numpy as np\nnp.load(p, allow_pickle=cfg['pickle'])", "NBP-DESERIALIZE", "numpy.load(allow_pickle=<dynamic>)"),
    ("import yaml\nyaml.load(s, Loader=SafeLoader)", "NBP-DESERIALIZE", "yaml.load"),      # unresolved name
    ("import yaml\nSL = yaml.FullLoader\nyaml.load(s, Loader=SL)", "NBP-DESERIALIZE", "yaml.FullLoader"),
    ("import yaml\nyaml.add_constructor('!x', ctor)", "NBP-DESERIALIZE", "yaml.add_constructor"),
    ("import yaml\nyaml.SafeLoader.add_constructor('!x', ctor)", "NBP-DESERIALIZE", "yaml.SafeLoader.add_constructor"),
    ("import yaml\nyaml.add_multi_constructor('!', ctor)", "NBP-DESERIALIZE", "yaml.add_multi_constructor"),
    ("import yaml\nclass L(yaml.loader.SafeLoader):\n    pass", "NBP-DESERIALIZE", "yaml.loader.SafeLoader"),
    ("from yaml import SafeLoader\nclass L(SafeLoader):\n    pass", "NBP-DESERIALIZE", "yaml.SafeLoader"),
    ("import yaml\nyaml.loader.SafeLoader", "NBP-DESERIALIZE", "yaml.loader"),
    ("import yaml\nyaml.constructor.SafeConstructor", "NBP-DESERIALIZE", "yaml.constructor"),
    ("from yaml import load_all", "NBP-DESERIALIZE", "yaml.load_all"),
    ("from yaml import FullLoader", "NBP-DESERIALIZE", "yaml.FullLoader"),
    ("from yaml import UnsafeConstructor", "NBP-DESERIALIZE", "yaml.UnsafeConstructor"),
    ("import yaml\nyaml.load_all(s, Loader=yaml.SafeLoader)", "NBP-DESERIALIZE", "yaml.load_all"),
    # G: namespace subscripts
    ("v = globals()['my_var'].attr", "NBP-INDIRECT", "globals()"),
    ("globals()['my_fn']()", "NBP-INDIRECT", "globals()"),
    ("globals()['my_var']['k']", "NBP-INDIRECT", "globals()"),
    ("import os as o\nlocals()['o']", "NBP-INDIRECT", "locals()"),
    ("globals()['spark']", "NBP-INDIRECT", "globals()"),
    ("globals()['open']", "NBP-INDIRECT", "globals()"),
    ("globals()['pickle']", "NBP-INDIRECT", "globals()"),
    # H: *args / **kwargs on the open family
    ("import codecs\ncodecs.open(**kw)", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("open(*parts, mode='r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('/tmp/x').open(**kw)", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    # I: pydoc
    ("import pydoc", "NBP-IMPORT-PROCESS", "pydoc"),
    ("from pydoc import pipepager", "NBP-IMPORT-PROCESS", "pydoc"),
    # read-mode open: the literal head and folded literals are still path-checked
    ("open(f'/etc/{name}').read()", "NBP-OPEN-PATH", "/etc/"),
    ("open('/Volumes/prod/' + name, 'rb').read()", "NBP-OPEN-PATH", "/Volumes/prod/"),
    ("import os\nopen(os.path.join('/etc', 'passwd')).read()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("p = '/'.join(['/Workspace', '.oci', 'config'])\nopen(p).read()", "NBP-OPEN-PATH", "/Workspace/.oci/config"),
    ("open(p, 'r+').read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("open(p, mode='w')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("m = 'w'\nopen(p, m)", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).open('w')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).read_text()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
]


@pytest.mark.parametrize("source,rule,target", REVIEW_GAP_CELLS + REVIEW_GAP_CELLS_2,
                         ids=[c[1] + ":" + c[0][:30] for c in REVIEW_GAP_CELLS + REVIEW_GAP_CELLS_2])
def test_review_gap_cells_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_review_gap_cell_count():
    assert len(REVIEW_GAP_CELLS) == 100


# Coverage round 1 on the review-probe fix gate: the tester's ten gap cells verbatim (first
# entry of each block) and the rest of the class each one belongs to. Every cell
# passed the gate at the review-probe fix.
REVIEW_GAP_CELLS_3 = [
    # J: a list literal mutated in place is no longer the literal it was bound to
    #    (subscript store, append / insert / extend / pop, aliasing, a helper call, *parts)
    ("p=['oci://bucket@ns/sandbox','a']\np[0]='/etc'\ndf.write.parquet('/'.join(p))",
     "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("parts=['oci://bucket@ns/sandbox']\nparts.insert(1,'..')\nopen('/'.join(parts)).read()",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'x.txt']\nparts[0] = '/etc'\nparts[1] = 'passwd'\n"
     "open('/'.join(parts)).read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox']\nparts.append('..')\nparts.append('..')\nparts.append('etc')\n"
     "parts.append('passwd')\nopen('/'.join(parts)).read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox']\nparts.extend(['..', '..', 'etc', 'passwd'])\nopen('/'.join(parts)).read()",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a', 'b']\nparts.pop()\nparts[1] = '..'\nopen('/'.join(parts))",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a']\ndel parts[0]\nopen('/'.join(parts))", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a']\nq = parts\nq[0] = '/etc'\nopen('/'.join(parts))",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a']\nrewrite(parts)\nopen('/'.join(parts))", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a']\nparts.__setitem__(0, '/etc')\nopen('/'.join(parts))",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("parts = ['oci://bucket@ns/sandbox', 'a']\nfor i in range(2):\n    parts[i] = '/etc'\nopen('/'.join(parts))",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("import os\nparts = ['oci://bucket@ns/sandbox', 'a']\nparts[0] = '/etc'\nopen(os.path.join(*parts))",
     "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("p = ['oci://bucket@ns/sandbox', 'a']\np[0] = '/etc'\ndbutils.fs.rm('/'.join(p))", "NBP-DBFS-DYNAMIC", "<dynamic>"),
    ("import shutil\np = ['oci://bucket@ns/sandbox', 'a']\np[0] = '/etc'\nshutil.move('a.txt', '/'.join(p))",
     "NBP-FS-DYNAMIC", "<dynamic>"),
    ("p = ['oci://bucket@ns/sandbox', 'a']\np[0] = '/etc'\ndf.write.option('path', '/'.join(p)).save()",
     "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    # K: keyword-gated loaders with the switch passed positionally or hidden in a splat
    ("import numpy as np\nnp.load(p, None, True)", "NBP-DESERIALIZE", "numpy.load(<positional>)"),
    ("import numpy as np\nnp.load(p, **kw)", "NBP-DESERIALIZE", "numpy.load(<dynamic>)"),
    ("import numpy as np\nnp.load(p, None, flag)", "NBP-DESERIALIZE", "numpy.load(<positional>)"),
    ("import numpy as np\nnp.load(p, *rest)", "NBP-DESERIALIZE", "numpy.load(<dynamic>)"),
    ("import numpy\nnumpy.load(p, None, 1)", "NBP-DESERIALIZE", "numpy.load(<positional>)"),
    ("from numpy import load\nload(p, None, True)", "NBP-DESERIALIZE", "numpy.load(<positional>)"),
    ("np.load(p, None, True)", "NBP-DESERIALIZE", "np.load(<positional>)"),          # no import in view
    ("np.load(p, **kw)", "NBP-DESERIALIZE", "np.load(<dynamic>)"),
    ("loader.load(p, **kw)", "NBP-DESERIALIZE", "loader.load(<dynamic>)"),            # receiver never bound
    ("import torch\ntorch.load(p, pickle_module=evil, weights_only=True)", "NBP-DESERIALIZE", "torch.load"),
    ("import torch\ntorch.load(p, None, evil, weights_only=True)", "NBP-DESERIALIZE", "torch.load(<positional>)"),
    ("import torch\ntorch.load(p, weights_only=True, **kw)", "NBP-DESERIALIZE", "torch.load(<dynamic>)"),
    ("import torch\ntorch.load(p, **kw)", "NBP-DESERIALIZE", "torch.load(<dynamic>)"),
    ("import torch\ntorch.serialization.add_safe_globals([evil])", "NBP-DESERIALIZE", "torch.serialization.add_safe_globals"),
    ("from torch.serialization import add_safe_globals", "NBP-DESERIALIZE", "torch.serialization.add_safe_globals"),
    ("import torch\nwith torch.serialization.safe_globals([evil]):\n    m = torch.load(p, weights_only=True)",
     "NBP-DESERIALIZE", "torch.serialization.safe_globals"),
    # L: the state of a gated module is read-only (a deserialiser's constructor registry first)
    ("import yaml\nyaml.SafeLoader.yaml_constructors['!x']=handler\nyaml.safe_load(s)",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors"),
    ("yaml.SafeLoader.yaml_multi_constructors[''] = handler\nyaml.safe_load(s)",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_multi_constructors"),
    ("import yaml\nL = yaml.SafeLoader\nL.yaml_constructors['!x'] = handler",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors"),
    ("import yaml\nyaml.SafeLoader.yaml_constructors.update({'!x': handler})",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors.update"),
    ("import yaml\nyaml.SafeLoader.yaml_constructors.setdefault('!x', handler)",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors.setdefault"),
    ("import yaml\nyaml.SafeLoader.yaml_implicit_resolvers.pop('!')",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_implicit_resolvers.pop"),
    ("import yaml\nyaml.SafeLoader.yaml_constructors = {'!x': handler}", "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors"),
    ("import yaml\ndel yaml.SafeLoader.yaml_constructors['!x']", "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors"),
    ("import yaml\nsetattr(yaml.SafeLoader, 'yaml_constructors', {})", "NBP-POLICY-TAMPER", "yaml.SafeLoader"),
    ("import yaml\ngetattr(yaml.SafeLoader, 'yaml_constructors')['!x'] = handler",
     "NBP-DESERIALIZE", "yaml.SafeLoader.yaml_constructors"),
    ("import yaml\nyaml.safe_load = yaml.unsafe_load", "NBP-DESERIALIZE", "yaml.safe_load"),
    ("import torch\ntorch.serialization.DEFAULT_PROTOCOL = 0", "NBP-DESERIALIZE", "torch.serialization.DEFAULT_PROTOCOL"),
    ("import torch\ntorch.backends.registry['x'] = evil", "NBP-DESERIALIZE", "torch.backends.registry"),
    ("import operator\noperator.attrgetter = evil", "NBP-POLICY-TAMPER", "operator.attrgetter"),
    ("import asyncio\nasyncio.events._event_loop_policy = evil", "NBP-INDIRECT", "asyncio.events._event_loop_policy"),
    ("import functools\nfunctools.WRAPPER_ASSIGNMENTS = ()", "NBP-POLICY-TAMPER", "functools.WRAPPER_ASSIGNMENTS"),
    # M: a sink name on an imported module the gate has no rules for (it may re-export os)
    ("import mymod\nmymod.popen('id')", "NBP-INDIRECT", "mymod.popen"),
    ("import mymod\nmymod.system('id')", "NBP-INDIRECT", "mymod.system"),
    ("import helpers\nhelpers.fork()", "NBP-INDIRECT", "helpers.fork"),
    ("import mymod\nmymod.sub.popen('id')", "NBP-INDIRECT", "mymod.sub.popen"),
    ("import mymod\nmymod.execvp('id', ['id'])", "NBP-INDIRECT", "mymod.execvp"),
    ("import mymod\nmymod.putenv('A', 'b')", "NBP-INDIRECT", "mymod.putenv"),
    # N: private attributes of ANY imported module, and a private segment makes the receiver opaque
    ("import mymod\nmymod._run('id')", "NBP-INDIRECT", "mymod._run"),
    ("import numpy as np\nnp._core.multiarray", "NBP-INDIRECT", "numpy._core"),
    ("from mymod import _helper", "NBP-INDIRECT", "mymod._helper"),
    ("import mymod\nmymod.sub._impl.system('id')", "NBP-INDIRECT", "mymod.sub._impl"),
    ("import pandas as pd\npd._libs.system('id')", "NBP-INDIRECT", "pandas._libs"),
    ("import tempfile\ntempfile._os.system('id')", "NBP-OS-PROCESS", "os.system"),       # re-rooted, still os
    ("import mymod\nmymod._sys.modules", "NBP-INDIRECT", "sys.modules"),
    ("import mymod\nmymod._subprocess.run('id')", "NBP-OS-PROCESS", "subprocess.run"),
    # O: code loaded from a path at run time (site .pth processing, pip)
    ("import site\nsite.addsitedir('/tmp/evil')", "NBP-BUILTIN-EXEC", "site"),
    ("site.addsitedir(d)", "NBP-BUILTIN-EXEC", "site.addsitedir"),                     # import out of view
    ("from site import addsitedir", "NBP-BUILTIN-EXEC", "site"),
    ("import site as s\ns.addsitedir(d)", "NBP-BUILTIN-EXEC", "site"),
    ("import pip\npip.main(['install', 'evil'])", "NBP-BUILTIN-EXEC", "pip"),
    ("pip.main(['install', 'evil'])", "NBP-BUILTIN-EXEC", "pip.main"),
    ("import ensurepip\nensurepip.bootstrap()", "NBP-BUILTIN-EXEC", "ensurepip"),
    ("import sitecustomize", "NBP-BUILTIN-EXEC", "sitecustomize"),
    # P: a receiver the notebook never bound, with an argument the gate cannot type
    ("thing.open(path_var).read()", "NBP-OPEN-DYNAMIC", "thing.open(<dynamic>)"),
    ("result = thing.pipe(cmd)", "NBP-OS-PROCESS", "thing.pipe()"),
    ("cfg.fs.open(name)", "NBP-OPEN-DYNAMIC", "cfg.fs.open(<dynamic>)"),
    ("with fs.open(object_path, 'rb') as fh:\n    payload = fh.read()", "NBP-OPEN-DYNAMIC", "fs.open(<dynamic>)"),
]


@pytest.mark.parametrize("source,rule,target", REVIEW_GAP_CELLS_3,
                         ids=[c[1] + ":" + c[0][:30] for c in REVIEW_GAP_CELLS_3])
def test_coverage_round1_gap_cells_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_list_literal_mutated_in_an_earlier_cell_is_not_folded(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("parts = ['oci://bucket@ns/sandbox', 'a']", context=ctx) == []
    assert npol.scan_source("parts.insert(1, '..')", context=ctx) == []
    assert [v.rule for v in npol.scan_source("open('/'.join(parts)).read()", context=ctx)] == ["NBP-OPEN-DYNAMIC"]
    ctx = npol.ScanContext()
    assert npol.scan_source("parts = ['oci://b@ns/sbx', 'a']", context=ctx) == []
    assert npol.scan_source("df.write.parquet('/'.join(parts))", context=ctx) == []      # join-only use: folded
    assert npol.scan_source("parts.append('..')", context=ctx) == []
    assert [v.rule for v in npol.scan_source("df.write.parquet('/'.join(parts))", context=ctx)] \
        == ["NBP-PATH-WRITE-DYNAMIC"]


def test_sink_on_an_imported_module_is_refused_across_cells(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("import mymod", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("mymod.system('id')", context=ctx)] \
        == [("NBP-INDIRECT", "mymod.system")]
    assert npol.scan_source("mymod.run('id')", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("mymod._run('id')", context=ctx)] \
        == [("NBP-INDIRECT", "mymod._run")]


# Everyday code next to each round-1 class that must not become a false refusal.
CLEAN_CELLS_6 = [
    # J: list literals used only for the join still fold; mutation without a path sink is fine
    "parts = ['oci://bucket@ns/sandbox', 'a']\ndf.write.parquet('/'.join(parts))",
    "parts = ['data', 'in.csv']\nrows = open('/'.join(parts)).read()",
    "items = []\nitems.append(1)\nitems.insert(0, 2)\nitems[0] = 3\nitems.extend([4])\nprint(len(items))",
    "cols = ['a', 'b']\ncols.append('c')\nout = df.select(*cols)",
    # K: loaders with their switch as a literal keyword, or positional False / None
    "import numpy as np\narr = np.load(array_path, None, False)",
    "import numpy as np\narr = np.load(array_path, 'r')",
    "import numpy as np\narr = np.load(array_path, mmap_mode='r', allow_pickle=False)",
    "import torch\nm = torch.load('m.pt', map_location='cpu', weights_only=True)",
    "import json\ncfg = json.load(fh, **opts)",
    "spark.read.format('csv').load(src_path, **options)",
    "spark.read.load(src_path, 'parquet')",
    # L: reading a gated module's state, torch configuration flags, pandas options
    "import yaml\ntags = list(yaml.SafeLoader.yaml_constructors)",
    "import torch\ntorch.backends.cudnn.benchmark = True\ntorch.set_grad_enabled(False)",
    "import pandas as pd\npd.options.display.max_columns = 50",
    "import sys\nsys.path.insert(0, '/Workspace/Repos/x')\nsys.path.append(lib_dir)",
    # M / N: public names on imported modules, homonyms
    "import platform\nosname = platform.system()",
    "import mymod\nmymod.run('id')\nresult = mymod.process(df)",
    "import helpers as h\nout = h.clean(df)",
    "from mymod import helper\nhelper(df)",
    # O: a variable that happens to be called site
    "site = row['site']\nname = site.lower()\nprint(site)",
    # P: receivers the notebook bound
    "import fsspec\nfs = fsspec.filesystem('oci')\nwith fs.open(object_path, 'rb') as fh:\n    payload = fh.read()",
    "fs = get_filesystem()\ndata = fs.open(object_path).read()",
    "import zipfile\nwith zipfile.ZipFile('rel.zip') as zf:\n    data = zf.open(member).read()",
    "def transform(frame):\n    return frame.pipe(step)",
    "import pandas as pd\ndf = pd.read_parquet(src_path)\nout = df.pipe(clean_frame).pipe(add_keys, keys=['id'])",
    "for frame in frames:\n    out = frame.pipe(step)",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_6, ids=[c[:40] for c in CLEAN_CELLS_6])
def test_coverage_round1_adjacent_everyday_code_passes_the_gate(source):
    assert npol.scan_source(source, policy=REVIEW_POLICY) == []


# The 19 ordinary notebook cells from the same review, which must stay allowed
# (scanned with the review's sandbox: catalog sbx / schema mig / prefix oci://bucket@ns/sandbox/).
REVIEW_POLICY = SandboxPolicy(catalog="sbx", schema="mig", prefix="oci://bucket@ns/sandbox/")
CLEAN_CELLS_4 = [
    "import pandas as pd\ndf = pd.read_csv(source_path)\ndf = df.pipe(clean_frame).pipe(add_keys, keys=['id'])",
    'items = [1, 2, 3]\nitems.remove(2)',
    "print(getattr(spark, 'version'))",
    'label = "{0.__class__.__name__}".format(obj)',
    'import yaml\ncfg = yaml.safe_load(text)\ncfg2 = yaml.load(text, Loader=yaml.SafeLoader)',
    'import numpy as np\narr = np.load(array_path, allow_pickle=False)',
    "import operator\nrows.sort(key=operator.itemgetter(1))\nnames = list(map(operator.attrgetter('name'), rows))",
    'import functools\ntotal = functools.reduce(lambda a, b: a + b, values, 0)',
    'import inspect\nsig = inspect.signature(transform)',
    # (the dynamic SELECT of the review's legit list is in SQL_DYNAMIC_READ_CELLS: guard required)
    "df.write.mode('overwrite').saveAsTable('sbx.mig.orders_clean')",
    "spark.read.table('sbx.mig.orders').filter('amount > 0').write.format('delta')"
    ".save('oci://bucket@ns/sandbox/orders_pos')",
    'from dataclasses import dataclass\nfrom typing import Optional\n@dataclass\nclass Row:\n    id: int\n'
    '    name: Optional[str] = None\n    def __repr__(self):\n        return f"Row({self.id})"',
    "import fsspec\nfs = fsspec.filesystem('oci')\nwith fs.open(object_path, 'rb') as fh:\n    payload = fh.read()",
    "import matplotlib.pyplot as plt\nplt.plot(df['x'], df['y']); plt.show()",
    'rdd = sc.parallelize(range(10)).map(lambda x: x * 2)\nprint(rdd.collect())',
    'result = spark.sql("SHOW TABLES IN sbx.mig")',
    'import datetime, re, math\nts = datetime.datetime.now(datetime.timezone.utc).isoformat()',
]
# Everyday code next to each closed class that must not become a false refusal.
CLEAN_CELLS_5 = [
    # A: tolerated value positions
    "x = spark",
    "print(spark)",
    "spark",
    "ok = isinstance(session, SparkSession)",
    "t = type(sc)",
    "has = hasattr(dbutils, 'secrets')",
    "import os\nsep = os.sep\nhome = os.path.expanduser('~')",
    "import functools\n@functools.lru_cache(maxsize=None)\ndef f(x):\n    return x",
    "import operator\nfrom operator import add, itemgetter\ntotal = functools.reduce(add, values)\nkey = itemgetter(0)",
    # B: homonyms on resolved or everyday receivers
    "import platform\nosname = platform.system()",
    "cursor.execute(q)\ncursor.executemany(q, rows)",
    "loader.load()\nmodel.load_state()",
    "import pyarrow as pa\nok = pa.types.is_string(t)",
    "from scipy import signal\nb, a = signal.butter(1, 0.5)",
    "code = resp.code\nprofile = user.profile.name",
    "fs = get_filesystem()\nwith fs.open(object_path, 'wb') as fh:\n    fh.write(payload)",
    "import zipfile\nwith zipfile.ZipFile('rel.zip') as z:\n    data = z.open('member.txt').read()",
    # C: DataFrame.pipe with a callable
    "import pandas as pd\nout = df.pipe(lambda d: d.dropna())",
    "def clean(d):\n    return d.dropna()\nout = df.pipe(clean)",
    "out = df.pipe(helpers.clean, keep='first')",
    # D: everyday operator / functools / inspect
    "import operator\ngetter = operator.attrgetter('name', 'id')\nvals = [getter(r) for r in rows]",
    "import operator\ncall = operator.methodcaller('strip')\nvals = list(map(call, names))",
    "import functools\np = functools.partial(int, base=2)",
    "import inspect\nsrc_ok = inspect.isfunction(transform)",
    # F: the dump side and safe loaders
    "import pickle\npickle.dump(obj, fh)\nb = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)",
    "import pickle\npickle.Pickler(fh).dump(obj)",
    "import joblib\njoblib.dump(model, 'model.joblib')\nres = joblib.Parallel(n_jobs=2)(joblib.delayed(f)(x) for x in xs)",
    "import yaml\ndocs = list(yaml.safe_load_all(text))\nout = yaml.safe_dump(cfg)",
    "from yaml import SafeLoader\nimport yaml\ncfg = yaml.load(text, Loader=SafeLoader)",
    "import yaml\ncfg = yaml.load(text, yaml.CSafeLoader)",
    "import torch\nm = torch.load('m.pt', weights_only=True)\nd = torch.device('cpu')",
    "import numpy as np\na = np.load('a.npy', allow_pickle=False)\nb = np.load(p)",
    # G: the tolerated namespace read
    "v = globals()['my_var']",
    "present = 'my_var' in globals()",
    # folded literal paths that stay relative / inside the sandbox
    "with open('/'.join(['data', 'in.csv'])) as fh:\n    rows = fh.read()",
    "import os\nwith open(os.path.join('data', 'in.csv')) as fh:\n    rows = fh.read()",
    "p = 'data/' + 'in.csv'\nrows = open(p).read()",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_4, ids=[c[:40] for c in CLEAN_CELLS_4])
def test_review_legit_cells_pass_the_gate(source):
    assert npol.scan_source(source, policy=REVIEW_POLICY) == []


@pytest.mark.parametrize("source", CLEAN_CELLS_5, ids=[c[:40] for c in CLEAN_CELLS_5])
def test_review_adjacent_everyday_code_passes_the_gate(sandbox_env, source):
    assert npol.scan_source(source) == []


def test_dynamic_open_is_refused_in_every_mode(sandbox_env):
    for src in ("open(p)", "open(p).read()", "open(p, 'r')", "open(p, 'rb')", "open(p, 'w')", "open(p, 'a')",
                "open(p, mode)", "open(p, mode='wb')", "open(f'{d}/x.json')", "import io\nio.open(p)",
                "import codecs\ncodecs.open(p, encoding='utf-8')", "from pathlib import Path\nPath(p).open()",
                "from pathlib import Path\nPath(p).read_text()"):
        assert [v.rule for v in npol.scan_source(src)] == ["NBP-OPEN-DYNAMIC"], src


def test_open_on_a_bound_receiver_is_recorded_not_refused(sandbox_env):
    # <object-store fs>.open(path) on a receiver the notebook bound: not the builtin, not a
    # Path; a literal local path is still checked, a dynamic one is allowed and recorded
    # (the receiver is not the local filesystem).
    src = "import fsspec\nfs = fsspec.filesystem('oci')\nwith fs.open(object_path, 'rb') as fh:\n    payload = fh.read()"
    assert npol.scan_source(src) == []
    assert npol.enforce_cell_policy(src, notebook_path="nb", cell_index=2) == []
    notes = [e for e in npol.get_policy_log() if e["event"] == "unverified_read_path"]
    assert [(e["target"], e["detail"]["line"], e["cell_index"]) for e in notes] == [("fs.open(<dynamic>)", 3, 2)]
    ctx = npol.ScanContext()
    assert npol.scan_source("fs = get_filesystem()", context=ctx) == []       # bound in an earlier cell
    assert npol.scan_source("data = fs.open(object_path).read()", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("fs.open('/etc/passwd')")] == [("NBP-OPEN-PATH", "/etc/passwd")]
    assert [(v.rule, v.target) for v in npol.scan_source("fs = get_filesystem()\nfs.open(**kw)")] \
        == [("NBP-OPEN-DYNAMIC", "<dynamic>")]


def test_open_on_a_receiver_the_notebook_never_bound_is_refused(sandbox_env):
    # thing.open(path_var): the receiver is a name the run never bound, so the gate cannot
    # tell it from the builtin or a Path and the dynamic path is refused as for open().
    for src in ("thing.open(path_var).read()", "with fs.open(object_path, 'rb') as fh:\n    payload = fh.read()",
                "cfg.fs.open(name)"):
        assert [v.rule for v in npol.scan_source(src)] == ["NBP-OPEN-DYNAMIC"], src
        assert not [e for e in npol.get_policy_log() if e["event"] == "unverified_read_path"]


def test_pipe_residual_is_recorded(sandbox_env):
    src = "import pandas as pd\ndf = pd.read_parquet(src_path)\nout = df.pipe(clean_frame)"
    assert npol.scan_source(src) == []
    npol.enforce_cell_policy(src, notebook_path="nb", cell_index=1)
    notes = [e for e in npol.get_policy_log() if e["event"] == "unverified_pipe_callable"]
    assert [(e["target"], e["detail"]["line"]) for e in notes] == [("df.pipe(clean_frame)", 3)]
    assert "unverified_pipe_callable" in npol.POLICY_NOTES


def test_pipe_on_a_receiver_the_notebook_never_bound_is_refused(sandbox_env):
    # thing.pipe(cmd): neither the receiver nor the argument can be typed; RDD.pipe(cmd) is a shell
    assert [(v.rule, v.target) for v in npol.scan_source("result = thing.pipe(cmd)")] \
        == [("NBP-OS-PROCESS", "thing.pipe()")]
    ctx = npol.ScanContext()
    assert npol.scan_source("frame = load_frame()", context=ctx) == []
    assert npol.scan_source("out = frame.pipe(step)", context=ctx) == []        # bound earlier: recorded


def test_module_value_rule_follows_aliases_across_cells(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("import os as o", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("mods = [o]", context=ctx)] == [("NBP-INDIRECT", "os")]
    ctx = npol.ScanContext()
    assert npol.scan_source("sp = spark", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("run(sp)", context=ctx)] == [("NBP-INDIRECT", "spark")]


def test_rdd_alias_bound_in_an_earlier_cell_cannot_pipe(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("rdd = sc.parallelize(xs)", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("rdd.pipe(fn)", context=ctx)] \
        == [("NBP-OS-PROCESS", "rdd.pipe()")]


def test_imported_module_receiver_is_resolved_across_cells(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("import platform", context=ctx) == []
    assert npol.scan_source("osname = platform.system()", context=ctx) == []


# Coverage round 2 on the coverage-round-1 fix gate: the tester's gap cells verbatim (first entry
# of each block) and the rest of the class each one belongs to. Every cell passed
# the gate at the coverage-round-1 fix.
REVIEW_GAP_CELLS_4 = [
    # Q: local-filesystem writes through module-rooted library writers (the mirror of the reader table);
    #    a literal local path outside the prefix is NBP-PATH-WRITE, a dynamic one NBP-PATH-WRITE-DYNAMIC
    ("import numpy as np\nnp.save('/etc/evil.npy', a)", "NBP-PATH-WRITE", "/etc/evil.npy"),
    ("import numpy as np\nnp.savez('/etc/evil.npz', a=a)", "NBP-PATH-WRITE", "/etc/evil.npz"),
    ("import numpy as np\nnp.savez_compressed('/etc/evil.npz', a)", "NBP-PATH-WRITE", "/etc/evil.npz"),
    ("import numpy as np\nnp.savetxt('/etc/evil.txt', a)", "NBP-PATH-WRITE", "/etc/evil.txt"),
    ("np.save('/etc/evil.npy', a)", "NBP-PATH-WRITE", "/etc/evil.npy"),                   # import out of view
    ("from numpy import save\nsave('/etc/evil.npy', a)", "NBP-PATH-WRITE", "/etc/evil.npy"),
    ("import torch\ntorch.save(model, '/etc/evil.pt')", "NBP-PATH-WRITE", "/etc/evil.pt"),
    ("import torch\ntorch.save(model, f='/etc/evil.pt')", "NBP-PATH-WRITE", "/etc/evil.pt"),
    ("import torch\ntorch.jit.save(scripted, '/etc/evil.pt')", "NBP-PATH-WRITE", "/etc/evil.pt"),
    ("import joblib\njoblib.dump(obj, '/etc/evil.joblib')", "NBP-PATH-WRITE", "/etc/evil.joblib"),
    ("import scipy.io\nscipy.io.savemat('/etc/evil.mat', d)", "NBP-PATH-WRITE", "/etc/evil.mat"),
    ("from scipy.io import savemat\nsavemat('/etc/evil.mat', d)", "NBP-PATH-WRITE", "/etc/evil.mat"),
    ("import cv2\ncv2.imwrite('/etc/evil.png', img)", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("import matplotlib.pyplot as plt\nplt.savefig('/etc/evil.png')", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("from matplotlib import pyplot as plt\nplt.savefig('/etc/evil.png')", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("import imageio\nimageio.imwrite('/etc/evil.png', img)", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("import pyarrow.parquet as pq\npq.write_table(table, '/etc/evil.parquet')", "NBP-PATH-WRITE",
     "/etc/evil.parquet"),
    ("import pandas as pd\nwriter = pd.ExcelWriter('/etc/evil.xlsx')", "NBP-PATH-WRITE", "/etc/evil.xlsx"),
    ("import mlflow.sklearn\nmlflow.sklearn.save_model(model, '/etc/evil')", "NBP-PATH-WRITE", "/etc/evil"),
    ("import numpy as np\nnp.save('~/evil.npy', a)", "NBP-PATH-WRITE", "~/evil.npy"),
    ("import numpy as np\nnp.save('file:///etc/evil.npy', a)", "NBP-PATH-WRITE", "file:///etc/evil.npy"),
    ("import numpy as np\nnp.save('oci://b@ns/prod/x.npy', a)", "NBP-PATH-WRITE", "oci://b@ns/prod/x.npy"),
    ("import numpy as np\nout = '/etc/evil.npy'\nnp.save(out, a)", "NBP-PATH-WRITE", "/etc/evil.npy"),
    ("import numpy as np\nnp.save(f'/etc/{name}.npy', a)", "NBP-PATH-WRITE", "/etc/"),
    ("import numpy as np\nnp.save(out_path, a)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("import torch\ntorch.save(model, ckpt)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("import numpy as np\nnp.save(*args)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    # R: receiver-typed frame writers (pandas / polars / xarray / numpy / matplotlib / plotly methods)
    ("df.to_csv('/etc/evil.csv')", "NBP-PATH-WRITE", "/etc/evil.csv"),
    ("import pandas as pd\npd.DataFrame(x).to_csv('/etc/out.csv')", "NBP-PATH-WRITE", "/etc/out.csv"),
    ("df.to_parquet('/etc/evil.parquet')", "NBP-PATH-WRITE", "/etc/evil.parquet"),
    ("df.to_pickle('/etc/evil.pkl')", "NBP-PATH-WRITE", "/etc/evil.pkl"),
    ("df.to_json('/etc/evil.json')", "NBP-PATH-WRITE", "/etc/evil.json"),
    ("df.to_hdf('/etc/evil.h5', key='df')", "NBP-PATH-WRITE", "/etc/evil.h5"),
    ("df.to_excel('/etc/evil.xlsx')", "NBP-PATH-WRITE", "/etc/evil.xlsx"),
    ("df.to_csv(path_or_buf='/etc/evil.csv')", "NBP-PATH-WRITE", "/etc/evil.csv"),
    ("df.to_csv('/dbfs/mnt/prod/' + name)", "NBP-PATH-WRITE", "/dbfs/mnt/prod/"),
    ("pl_df.write_parquet('/etc/evil.parquet')", "NBP-PATH-WRITE", "/etc/evil.parquet"),
    ("ds.to_netcdf('/etc/evil.nc')", "NBP-PATH-WRITE", "/etc/evil.nc"),
    ("arr.tofile('/etc/evil.bin')", "NBP-PATH-WRITE", "/etc/evil.bin"),
    ("fig.savefig('/etc/evil.png')", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("fig.write_html('/etc/evil.html')", "NBP-PATH-WRITE", "/etc/evil.html"),
    ("df.to_csv(out_path)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("df.to_parquet(f'{out_dir}/x.parquet')", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("df.to_csv(**kw)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    # S: generic .save(path) receivers (openpyxl / Keras / PIL / transformers / docx) with a literal local path
    ("import openpyxl\nwb.save('/etc/evil.xlsx')", "NBP-PATH-WRITE", "/etc/evil.xlsx"),
    ("model.save('/etc/evil.h5')", "NBP-PATH-WRITE", "/etc/evil.h5"),
    ("img.save('/etc/evil.png')", "NBP-PATH-WRITE", "/etc/evil.png"),
    ("model.save_pretrained('/etc/evil')", "NBP-PATH-WRITE", "/etc/evil"),
    ("model.export('/etc/evil')", "NBP-PATH-WRITE", "/etc/evil"),
    ("wb.save(filename='/etc/evil.xlsx')", "NBP-PATH-WRITE", "/etc/evil.xlsx"),
    ("doc.save('/Workspace/Shared/evil.docx')", "NBP-PATH-WRITE", "/Workspace/Shared/evil.docx"),
    ("wb.save('/etc/' + name)", "NBP-PATH-WRITE", "/etc/"),
    # T: tempfile creation outside the sandbox (literal dir= / absolute prefix= / tempfile.tempdir)
    ("import tempfile\ntempfile.NamedTemporaryFile(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.TemporaryFile(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.mkstemp(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.mkdtemp(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.mkdtemp('', 'x', '/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.TemporaryDirectory(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.NamedTemporaryFile(prefix='/etc/evil')", "NBP-FS-PATH", "/etc/evil"),
    ("import tempfile\ntempfile.mkstemp(dir='/etc/' + sub)", "NBP-FS-PATH", "/etc/"),
    ("import tempfile as tf\ntf.mkdtemp(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("from tempfile import NamedTemporaryFile\nNamedTemporaryFile(dir='/etc')", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.tempdir = '/etc'", "NBP-FS-PATH", "/etc"),
    ("import tempfile\ntempfile.tempdir = d", "NBP-FS-DYNAMIC", "<dynamic>"),
    # U: live frame / traceback dumps (faulthandler, traceback.walk_*, capture_locals, Task.get_stack)
    ("import faulthandler\nfaulthandler.dump_traceback_later(1)", "NBP-INDIRECT", "faulthandler"),
    ("faulthandler.dump_traceback()", "NBP-INDIRECT", "faulthandler.dump_traceback"),    # import out of view
    ("from faulthandler import dump_traceback", "NBP-INDIRECT", "faulthandler"),
    ("import traceback\nframes = list(traceback.walk_stack(None))", "NBP-INDIRECT", "traceback.walk_stack"),
    ("import traceback\ntraceback.walk_tb(tb)", "NBP-INDIRECT", "traceback.walk_tb"),
    ("from traceback import walk_stack\nwalk_stack(None)", "NBP-INDIRECT", "traceback.walk_stack"),
    ("import traceback\ns = traceback.StackSummary.extract(gen, capture_locals=True)", "NBP-INDIRECT",
     "traceback.StackSummary.extract(capture_locals=True)"),
    ("import traceback\nte = traceback.TracebackException(t, v, tb, capture_locals=True)", "NBP-INDIRECT",
     "traceback.TracebackException(capture_locals=True)"),
    ("task.get_stack()", "NBP-INDIRECT", "task.get_stack"),
]


@pytest.mark.parametrize("source,rule,target", REVIEW_GAP_CELLS_4,
                         ids=[c[1] + ":" + c[0][:30] for c in REVIEW_GAP_CELLS_4])
def test_coverage_round2_gap_cells_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_coverage_round2_gap_cell_count():
    assert len(REVIEW_GAP_CELLS_4) >= 70


def test_writer_path_bound_in_an_earlier_cell_is_resolved(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("out = '/etc/evil.npy'", context=ctx) == []
    assert [(v.rule, v.target) for v in npol.scan_source("import numpy as np\nnp.save(out, a)", context=ctx)] \
        == [("NBP-PATH-WRITE", "/etc/evil.npy")]
    ctx = npol.ScanContext()
    assert npol.scan_source("out = 'oci://b@ns/sbx/arr.npy'", context=ctx) == []
    assert npol.scan_source("import numpy as np\nnp.save(out, a)", context=ctx) == []


def test_write_handle_bound_in_an_earlier_cell_is_followed_until_rebound(sandbox_env):
    ctx = npol.ScanContext()
    assert npol.scan_source("import io\nbuf = io.StringIO()", context=ctx) == []
    assert npol.scan_source("df.to_csv(buf)", context=ctx) == []
    assert npol.scan_source("buf = resolve_target()", context=ctx) == []
    assert [v.rule for v in npol.scan_source("df.to_csv(buf)", context=ctx)] == ["NBP-PATH-WRITE-DYNAMIC"]


def test_tempfile_inside_the_sandbox_prefix_is_allowed(sandbox_env):
    local = sandbox_env["local_prefix"] + "stage"
    assert npol.scan_source(f"import tempfile\ntempfile.mkdtemp(dir={local!r})") == []
    assert npol.scan_source("import tempfile\ntempfile.mkdtemp(dir='oci://b@ns/sbx/tmp')") == []
    assert npol.scan_source(f"import tempfile\ntempfile.tempdir = {local!r}") == []


# Everyday code next to each round-2 class that must not become a false refusal.
CLEAN_CELLS_7 = [
    # Q / R: writes under the sandbox prefix or to a relative path, in-memory buffers, no-path forms
    "import numpy as np\nnp.save('oci://bucket@ns/sandbox/arr.npy', a)",
    "import numpy as np\nnp.save('arr.npy', a)\nnp.savetxt('out/arr.txt', a)",
    "import joblib\njoblib.dump(model, 'model.joblib')",
    "import torch\ntorch.save(model.state_dict(), 'ckpt.pt')",
    "out = 'oci://bucket@ns/sandbox/out.npy'\nimport numpy as np\nnp.save(out, a)",
    "df.to_csv('oci://bucket@ns/sandbox/out.csv', index=False)",
    "df.to_parquet('out/part.parquet')",
    "csv_text = df.to_csv(index=False)\njs = df.to_json(orient='records')",
    "import io\nbuf = io.StringIO()\ndf.to_csv(buf)",
    "from io import BytesIO\nfig.savefig(BytesIO(), format='png')",
    "import pandas as pd\nwith pd.ExcelWriter('oci://bucket@ns/sandbox/report.xlsx') as writer:\n"
    "    df.to_excel(writer, sheet_name='a')",
    "import matplotlib.pyplot as plt\nplt.plot([1, 2]); plt.savefig('plot.png')",
    "import sys\ndf.to_csv(sys.stdout)",
    "import cv2\ncv2.imwrite('oci://bucket@ns/sandbox/img.png', img)",
    # S: generic .save receivers with a dynamic / relative / absent path
    "model.save(model_uri)\nrow.save()\nwb.save('report.xlsx')",
    # T: tempfile defaults, a dir the gate cannot resolve, a relative dir
    "import tempfile\ntmp = tempfile.mkdtemp()\nd = tempfile.gettempdir()",
    "import tempfile\nwith tempfile.TemporaryDirectory() as d:\n    print(d)",
    "import tempfile\nfh = tempfile.NamedTemporaryFile(dir=work_dir, suffix='.csv')",
    "import tempfile\npath = tempfile.mkstemp(prefix='stage_', dir='.')[1]",
    # U: everyday traceback use returns text, not frames
    "import traceback\ntry:\n    run()\nexcept Exception:\n    traceback.print_exc()\n    msg = traceback.format_exc()",
    "import traceback\nframes = traceback.extract_stack()\nlines = traceback.format_exception(e)",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_7, ids=[c[:40] for c in CLEAN_CELLS_7])
def test_coverage_round2_adjacent_everyday_code_passes_the_gate(source):
    assert npol.scan_source(source, policy=REVIEW_POLICY) == []


# Coverage round 3 (on the coverage-round-2 fix): a re-export back to the chain's own root, a DataFrameWriter /
# DataFrameReader held in a variable (builder-call result, alias of it, streaming writer) and the
# server modules; each gap cell verbatim plus the rest of its class.
REVIEW_GAP_CELLS_5 = [
    # V: os.path re-exports os; a segment that names the chain's own root re-roots the chain
    ("import os.path as op\nop.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import os.path as op\nop.os.environ['X']", "NBP-OS-ENVIRON", "os.environ"),
    ("import os.path as op\nop.os.popen('id')", "NBP-OS-PROCESS", "os.popen"),
    ("import os.path\nos.path.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import os\nos.path.genericpath.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import os\nos.path.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("from os.path import os as o\no.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("from os import path\npath.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("from os import path as p\np.os.environ['X']", "NBP-OS-ENVIRON", "os.environ"),
    ("import glob\nglob.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import posixpath\nposixpath.os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import tempfile\ntempfile._os.system('id')", "NBP-OS-PROCESS", "os.system"),
    ("import random\nrandom._os.system('id')", "NBP-OS-PROCESS", "os.system"),
    # W: a DataFrameWriter / DataFrameReader held in a variable (builder-call result, alias, streaming)
    ("wr = df.write.mode('overwrite')\nwr.parquet('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.write.mode('overwrite').option('x', 'y')\nwr.csv('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.write.format('delta')\nwr.save('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.coalesce(1).write\nwr.parquet('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.write.mode('overwrite')\nwr.saveAsTable('otherc.s.t')", "NBP-TABLE-TARGET", "otherc.s.t"),
    ("wr = df.write.mode('overwrite')\nwr.format('delta').saveAsTable('otherc.s.t')", "NBP-TABLE-TARGET",
     "otherc.s.t"),
    ("wr = df.write.mode('overwrite')\nwr.option('path', '/etc/x').save()", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.write.mode('overwrite')\nwr.parquet(dest)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    ("wr = df.write.mode('overwrite')\nw2 = wr\nw2.parquet('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("wr = df.writeTo('otherc.s.t').using('delta')\nwr.createOrReplace()", "NBP-TABLE-TARGET", "otherc.s.t"),
    ("rd = spark.read.format('csv')\nrd.load('/etc/passwd')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("wr = df.writeStream.format('delta')\nwr.option('path', '/etc/x').start()", "NBP-PATH-WRITE", "/etc/x"),
    ("df.writeStream.format('delta').option('path', '/etc/x').start()", "NBP-PATH-WRITE", "/etc/x"),
    ("df.writeStream.format('delta').start('/etc/x')", "NBP-PATH-WRITE", "/etc/x"),
    ("df.writeStream.toTable('otherc.s.t')", "NBP-TABLE-TARGET", "otherc.s.t"),
    ("df.writeStream.format('delta').option('checkpointLocation', '/etc/ck').start()", "NBP-PATH-WRITE", "/etc/ck"),
    ("df.writeStream.options(checkpointLocation='/etc/ck').start()", "NBP-PATH-WRITE", "/etc/ck"),
    ("df.writeStream.start(path_var)", "NBP-PATH-WRITE-DYNAMIC", "<dynamic>"),
    # X: servers bind a listening socket; the dotted network modules are judged at every door
    ("import http.server\nhttp.server.HTTPServer(('',0),h)", "NBP-IMPORT-NETWORK", "http.server"),
    ("from http.server import HTTPServer", "NBP-IMPORT-NETWORK", "http.server"),
    ("import http.server as hs", "NBP-IMPORT-NETWORK", "http.server"),
    ("import http\nhttp.server.HTTPServer(('',0),h)", "NBP-IMPORT-NETWORK", "http.server"),
    ("from http import server", "NBP-IMPORT-NETWORK", "http.server"),
    ("import http\nhttp.client.HTTPConnection('h')", "NBP-IMPORT-NETWORK", "http.client"),
    ("import wsgiref.simple_server as w\nw.make_server('', 0, app)", "NBP-IMPORT-NETWORK", "wsgiref.simple_server"),
    ("from wsgiref.simple_server import make_server", "NBP-IMPORT-NETWORK", "wsgiref.simple_server"),
    ("import wsgiref\nwsgiref.simple_server.make_server('', 0, app)", "NBP-IMPORT-NETWORK", "wsgiref.simple_server"),
    ("import smtpd", "NBP-IMPORT-NETWORK", "smtpd"),
    ("import flask", "NBP-IMPORT-NETWORK", "flask"),
    ("from fastapi import FastAPI", "NBP-IMPORT-NETWORK", "fastapi"),
    ("import uvicorn", "NBP-IMPORT-NETWORK", "uvicorn"),
    ("import tornado.web", "NBP-IMPORT-NETWORK", "tornado"),
]


@pytest.mark.parametrize("source,rule,target", REVIEW_GAP_CELLS_5,
                         ids=[c[1] + ":" + c[0][:30] for c in REVIEW_GAP_CELLS_5])
def test_coverage_round3_gap_cells_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_coverage_round3_gap_cell_count():
    assert len(REVIEW_GAP_CELLS_5) >= 40


def test_writer_held_in_a_variable_is_followed_across_cells():
    ctx = npol.ScanContext()

    def scan(src):
        return [(v.rule, v.target) for v in npol.scan_source(src, policy=REVIEW_POLICY, context=ctx)]

    assert scan("wr = df.write.mode('overwrite').format('delta')") == []
    assert scan("wr.save('/etc/x')") == [("NBP-PATH-WRITE", "/etc/x")]
    assert scan("wr.saveAsTable('otherc.s.t')") == [("NBP-TABLE-TARGET", "otherc.s.t")]
    assert scan("wr.save('oci://bucket@ns/sandbox/out')") == []
    assert scan("w2 = wr") == []
    assert scan("w2.parquet(dest)") == [("NBP-PATH-WRITE-DYNAMIC", "<dynamic>")]
    ctx = npol.ScanContext()
    assert scan("rd = spark.read.format('csv').option('header', True)") == []
    assert scan("rd.load('/etc/passwd')") == [("NBP-OPEN-PATH", "/etc/passwd")]
    assert scan("df = rd.load(src_path)") == []


def test_allow_network_lifts_the_server_modules_at_every_door():
    open_policy = SandboxPolicy(catalog="sbx", schema="mig", prefix="oci://bucket@ns/sandbox/", allow_network=True)
    for src in ("import http.server", "from http.server import HTTPServer", "from http import server",
                "import http\nhttp.server.HTTPServer(('', 0), h)", "import wsgiref.simple_server as w",
                "import wsgiref\nwsgiref.simple_server.make_server('', 0, app)", "import flask"):
        assert npol.scan_source(src, policy=open_policy) == [], src


# Everyday code next to each round-3 class that must not become a false refusal.
CLEAN_CELLS_8 = [
    # V: os.path through an alias is everyday path code; every re-root shortens the chain and ends
    "import os.path as op\np = op.join(base, 'x')\nprint(op.basename(p), op.sep)",
    "import os\nprint(os.path.sep, os.path.basename(p))",
    "from os import path\nprint(path.join('a', 'b'))",
    "import os\np = os.path.os.path.join('a', 'b')",
    # W: writers / readers held in a variable that stay inside the sandbox; builders with run-time arguments
    "wr = df.write.mode('overwrite')\nwr.parquet('oci://bucket@ns/sandbox/out')",
    "wr = df.write.mode('overwrite').format('delta')\nwr.save('oci://bucket@ns/sandbox/out')",
    "wr = df.write.mode('overwrite')\nwr.format('delta').saveAsTable('sbx.mig.t')",
    "wr = df.write.mode('overwrite')\nwr.option('compression', 'snappy').parquet('oci://bucket@ns/sandbox/out')",
    "writer = df.write.mode('overwrite')\nwriter.format(fmt).save('oci://bucket@ns/sandbox/out')",
    "w = df.write.mode('overwrite')\nw.partitionBy('d').parquet('oci://bucket@ns/sandbox/out')",
    "wr = df.coalesce(1).write\nwr.mode('overwrite').csv('oci://bucket@ns/sandbox/out')",
    "wr = df.write.mode('overwrite')\nwr.saveAsTable('sbx.mig.out')",
    "rd = spark.read.format('csv').option('header', True)\ndf = rd.load(src_path)",
    "rd = spark.read.format('csv')\ndf = rd.load('oci://bucket@ns/prod/in.csv')",
    "q = df.writeStream.format('delta').option('checkpointLocation', 'oci://bucket@ns/sandbox/ck')"
    ".start('oci://bucket@ns/sandbox/out')",
    "df.writeStream.format('console').start()",
    "df.writeStream.toTable('sbx.mig.events')",
    "wr = df.writeStream.format('delta').outputMode('append')\n"
    "wr.option('checkpointLocation', 'oci://bucket@ns/sandbox/ck').start('oci://bucket@ns/sandbox/out')",
    "q = df.writeStream.foreachBatch(fn).start()\nq.awaitTermination()",
    "import threading\nt = threading.Thread(target=run)\nt.start()",
    # X: the harmless parents of the dotted network modules
    "import http\nprint(http.HTTPStatus.OK)",
    "from http import HTTPStatus",
    "import wsgiref.util",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_8, ids=[c[:40] for c in CLEAN_CELLS_8])
def test_coverage_round3_adjacent_everyday_code_passes_the_gate(source):
    assert npol.scan_source(source, policy=REVIEW_POLICY) == []


# Regression review on the coverage-round-3 fix: Path.open() / .rename() / .replace() on a Path the gate
# did not recognise as one (a Path-returning step, a derived Path, a name bound to either)
# fell through to the generic <object>.open(path) rule, which read the MODE string as the
# path and allowed the call. Every receiver rooted at a Path is a Path: the identity steps
# keep the constructor literal (judged as NBP-OPEN-PATH / NBP-FS-PATH), everything else is
# an unresolvable path and fails closed (NBP-OPEN-DYNAMIC / NBP-FS-DYNAMIC) in every mode.
# The reviewer's thirteen cells come first, verbatim.
REVIEW_GAP_CELLS_6 = [
    ("from pathlib import Path\nPath(p).resolve().open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nq = Path(p).resolve()\nq.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).parent.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('/etc/passwd').resolve().open()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from pathlib import Path\nPath('/etc/passwd').absolute().open('r')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from pathlib import Path\nPath('/etc/passwd').expanduser().open('r')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from pathlib import Path\n(Path('/etc') / 'passwd').open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('/etc').joinpath('passwd').open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('/etc/passwd').with_name('shadow').open()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nwith Path('/etc/cron.d/job').absolute().open('w') as f:\n    f.write('x')",
     "NBP-OPEN-PATH", "/etc/cron.d/job"),
    ("from pathlib import Path\nPath('/etc/passwd').absolute().rename('/tmp/x')", "NBP-FS-PATH", "/etc/passwd"),
    ("from pathlib import Path\nPath('/tmp/x').absolute().replace('/etc/passwd')", "NBP-FS-PATH", "/tmp/x"),
    ("from pathlib import Path\nPath(p).resolve().rename(t)", "NBP-FS-DYNAMIC", "<dynamic>"),
    # the same routes through the module, an alias, the classmethods and the other derived steps
    ("import pathlib\npathlib.Path(p).resolve().open('r').read()", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path as P\nP(p).resolve().open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath.cwd().open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath.home().joinpath('.ssh', 'id_rsa').open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath().open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('~root/.ssh/id_rsa').expanduser().open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).parents[1].open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).parent.parent.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\n('/etc' / Path('passwd')).open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).with_suffix('.bak').open('w')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath(p).with_suffix('.bak').replace(t)", "NBP-FS-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nPath('/etc').joinpath('passwd').rename(t)", "NBP-FS-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\n(Path('/tmp') / name).replace('/etc/passwd')", "NBP-FS-DYNAMIC", "<dynamic>"),
    # a name bound to a derived Path, rebound, bound by a loop / comprehension, or annotated
    ("from pathlib import Path\nq = Path('/etc') / 'passwd'\nq.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nq = Path.home()\nq.joinpath('.ssh', 'id_rsa').open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nq = Path(a)\nq = q.resolve()\nq.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nq = Path('/etc/passwd').absolute()\nq.open()", "NBP-OPEN-PATH", "/etc/passwd"),
    ("from pathlib import Path\nq = Path('/etc/passwd').resolve()\nq.rename('/tmp/x')", "NBP-FS-PATH", "/etc/passwd"),
    ("from pathlib import Path\nfor f in Path('/etc').iterdir():\n    f.open('r').read()", "NBP-OPEN-DYNAMIC",
     "<dynamic>"),
    ("from pathlib import Path\n[f.open('r').read() for f in Path('/etc').glob('*')]", "NBP-OPEN-DYNAMIC",
     "<dynamic>"),
    ("from pathlib import Path\ndef dump(src: Path):\n    return src.open('r').read()", "NBP-OPEN-DYNAMIC",
     "<dynamic>"),
    ("from pathlib import Path\ndef move(src: Path, dst):\n    src.rename(dst)", "NBP-FS-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\nq: Path = Path(p).resolve()\nq.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    ("from pathlib import Path\na, b = Path(p).resolve(), 'x'\na.open('r')", "NBP-OPEN-DYNAMIC", "<dynamic>"),
    # a splat on a derived receiver still hides the mode and path
    ("from pathlib import Path\nPath('/tmp/x').resolve().open(*mode)", "NBP-OPEN-DYNAMIC", "<dynamic>"),
]


@pytest.mark.parametrize("source,rule,target", REVIEW_GAP_CELLS_6,
                         ids=[c[1] + ":" + c[0][:30] for c in REVIEW_GAP_CELLS_6])
def test_derived_path_receivers_are_refused(tmp_path, sandbox_env, source, rule, target):
    _assert_refused(tmp_path, [source], rule, target)


def test_derived_path_receiver_cell_count():
    assert len(REVIEW_GAP_CELLS_6) >= 38


def test_path_bound_in_an_earlier_cell_is_followed():
    ctx = npol.ScanContext()

    def scan(src):
        return [(v.rule, v.target) for v in npol.scan_source(src, policy=REVIEW_POLICY, context=ctx)]

    assert scan("from pathlib import Path\nq = Path(src_dir).resolve()") == []
    assert scan("q.open('r')") == [("NBP-OPEN-DYNAMIC", "<dynamic>")]
    assert scan("q.joinpath('x').open('r')") == [("NBP-OPEN-DYNAMIC", "<dynamic>")]
    assert scan("q.rename(dst)") == [("NBP-FS-DYNAMIC", "<dynamic>")]
    assert scan("p = Path('/etc/passwd').absolute()") == []
    assert scan("p.open()") == [("NBP-OPEN-PATH", "/etc/passwd")]
    assert scan("p = p.resolve()") == []
    assert scan("p.open()") == [("NBP-OPEN-DYNAMIC", "<dynamic>")]
    # rebound to a string the name is a string again: str.replace is not a Path operation
    assert scan("p = 'a-b'") == []
    assert scan("p.replace('-', '_')") == []


# Everyday pathlib / str / DataFrame code next to the class that must not become a false refusal.
CLEAN_CELLS_9 = [
    "s = 'a-b'\ns.replace('-', '_')",
    "df.rename(columns={'a': 'b'})",
    "df.replace({'a': 1})",
    "from pathlib import Path\nstem = Path(p).stem.replace('-', '_')",
    "from pathlib import Path\nname = Path(p).name\nname.replace('.csv', '')",
    "from pathlib import Path\nprint(Path(p).resolve().name, Path(p).parent.stem, Path(p).suffix)",
    "from pathlib import Path\nif Path(p).resolve().exists():\n    print(Path(p).stat().st_size)",
    "from pathlib import Path\nfor f in Path(d).iterdir():\n    print(f.name, f.suffix)",
    "from pathlib import Path\nnames = [f.name for f in Path(d).glob('*.csv')]",
    "from pathlib import Path\nPath('oci://bucket@ns/sandbox/x').resolve().open('w')",
    "from pathlib import Path\nq = Path('oci://bucket@ns/sandbox/out')\nq.absolute().open('w')",
    "from pathlib import Path\nPath('oci://bucket@ns/sandbox/a').absolute().rename('oci://bucket@ns/sandbox/b')",
    "from pathlib import Path\nPath('rel/x.txt').resolve().open()",
    "from pathlib import Path\nPath('rel/x.txt').expanduser().read_text()",
    "import zipfile\nzf = zipfile.ZipFile('oci://bucket@ns/sandbox/x.zip')\nzf.open(member)",
    "import fsspec\nfs = fsspec.filesystem('oci')\nwith fs.open(object_path, 'rb') as fh:\n    data = fh.read()",
]


@pytest.mark.parametrize("source", CLEAN_CELLS_9, ids=[c[:40] for c in CLEAN_CELLS_9])
def test_pathlib_adjacent_everyday_code_passes_the_gate(source):
    assert npol.scan_source(source, policy=REVIEW_POLICY) == []


# ══════════════════════════════════════════════════════════════════════
# SEC-AIDP-SAMPLES-005 round 3 (B1): the SQL gate is an ALLOWLIST
# ══════════════════════════════════════════════════════════════════════
# Review finding: the SQL handling was a blocklist of write verbs and assumed
# that a statement starting with WITH is a read, so SELECT reflect(..),
# SELECT * FROM text.`/etc/passwd`, ADD JAR and WITH .. INSERT INTO prod.t all
# passed. Now one classifier (classify_sql) resolves the TOP-LEVEL verb behind
# the CTE list on a masked copy of the statement (comments, string literals and
# backtick identifiers neutralised), accepts only known read forms and known
# write forms whose target is inside the sandbox, and refuses everything else.
# The same classifier runs again at run time: dbutils.notebook.run installs a
# guard on the session's SQL entry points for the duration of the run, so a
# statement with run-time fragments is judged on its final text before Spark
# executes it, and the static gate admits such a statement only while that
# guard is active (ScanContext.runtime_sql_guard) and its literal head verb is
# a read verb. Every cell in SQL_REFUSED_CELLS is a NEW refusal: against the
# round-2 tree (1356a56) it either runs or is refused under a different rule or
# target. Cells the round-2 blocklist already refused with the same rule and
# target live in SQL_REFUSED_CELLS_BASELINE_GUARDS below: they are regression
# guards for the forms the old gate knew and prove nothing about this round.

# The four statements named in the review, verbatim.
SQL_REVIEWER_CELLS = [
    ("spark.sql(\"SELECT reflect('java.lang.Runtime', 'getRuntime')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql('SELECT * FROM text.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"ADD JAR 'oci://b@ns/prod/evil.jar'\")", "NBP-SQL-REFUSED", "ADD JAR"),
    ("spark.sql('WITH x AS (SELECT 1) INSERT INTO prod.table SELECT * FROM x')", "NBP-SQL-TARGET", "prod.table"),
]
SQL_REFUSED_CELLS = SQL_REVIEWER_CELLS + [
    # -- the Java-call functions anywhere, case-insensitively: CTEs, subqueries, expressions, other verbs
    ("spark.sql(\"SELECT java_method('java.lang.System', 'getenv', 'PATH')\")", "NBP-SQL-REFUSED", "java_method("),
    ("spark.sql(\"WITH c AS (SELECT REFLECT('a', 'b') r) SELECT * FROM c\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"SELECT * FROM sbx.t WHERE x = (SELECT Java_Method('a', 'b'))\")", "NBP-SQL-REFUSED",
     "java_method("),
    ("spark.sql(\"SELECT 'x' FROM sbx.t WHERE reflect ('a', 'b') = 1\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"CACHE TABLE c AS SELECT reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"DESCRIBE QUERY SELECT reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"EXPLAIN SELECT java_method('a', 'b')\")", "NBP-SQL-REFUSED", "java_method("),
    ("spark.sql(\"INSERT INTO sbx.t SELECT reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"CREATE OR REPLACE TEMP VIEW v AS SELECT reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    # -- the Hive script transform runs a shell command
    ("spark.sql(\"SELECT TRANSFORM(a) USING 'cat' AS (b) FROM sbx.t\")", "NBP-SQL-REFUSED", "TRANSFORM ... USING"),
    ("spark.sql(\"SELECT TRANSFORM(a, b) ROW FORMAT DELIMITED USING '/bin/sh -c id' FROM sbx.t\")",
     "NBP-SQL-REFUSED", "TRANSFORM ... USING"),
    ("spark.sql(\"WITH c AS (SELECT TRANSFORM(a) USING 'id' AS (b) FROM sbx.t) SELECT * FROM c\")",
     "NBP-SQL-REFUSED", "TRANSFORM ... USING"),
    # -- path tables / path sources on the cluster's local filesystem outside the prefix
    ("spark.sql('SELECT * FROM parquet.`/dbfs/mnt/prod/x`')", "NBP-OPEN-PATH", "/dbfs/mnt/prod/x"),
    ("spark.sql('SELECT * FROM delta.`/Workspace/Repos/x/t`')", "NBP-OPEN-PATH", "/Workspace/Repos/x/t"),
    ("spark.sql('SELECT * FROM csv.`/tmp/other/x.csv`')", "NBP-OPEN-PATH", "/tmp/other/x.csv"),
    ("spark.sql('SELECT * FROM json.`/Volumes/prod/x`')", "NBP-OPEN-PATH", "/Volumes/prod/x"),
    ("spark.sql('SELECT * FROM text.`file:///etc/shadow`')", "NBP-OPEN-PATH", "file:///etc/shadow"),
    ("spark.sql('SELECT * FROM json.`~/.ssh/id_rsa`')", "NBP-OPEN-PATH", "~/.ssh/id_rsa"),
    ("spark.sql('SELECT * FROM csv.`C:/Users/x/s.csv`')", "NBP-OPEN-PATH", "C:/Users/x/s.csv"),
    ("spark.sql('SELECT * FROM binaryFile.`/etc`')", "NBP-OPEN-PATH", "/etc"),
    ("spark.sql('SELECT * FROM sbx.t JOIN text.`/etc/passwd` p ON 1 = 1')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('WITH p AS (SELECT * FROM text.`/etc/passwd`) SELECT * FROM p')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('INSERT INTO sbx.t SELECT * FROM text.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('EXPLAIN SELECT * FROM text.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"COPY INTO sbx.t FROM '/etc/passwd' FILEFORMAT = CSV\")", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"CREATE TEMP VIEW v USING csv OPTIONS (path '/etc/passwd')\")", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"CREATE TEMPORARY VIEW v USING parquet OPTIONS ('path' = '/dbfs/mnt/prod/x')\")",
     "NBP-OPEN-PATH", "/dbfs/mnt/prod/x"),
    ("spark.sql(\"CREATE TEMP VIEW v USING parquet LOCATION '/etc/x'\")", "NBP-SQL-TARGET", "/etc/x"),
    ("spark.sql('INSERT INTO delta.`/tmp/other/x` SELECT 1')", "NBP-SQL-TARGET", "/tmp/other/x"),
    ("spark.sql('INSERT INTO delta.`oci://b@ns/prod/x` SELECT 1')", "NBP-SQL-TARGET", "oci://b@ns/prod/x"),
    ("spark.sql('OPTIMIZE delta.`/dbfs/mnt/prod/x`')", "NBP-SQL-TARGET", "/dbfs/mnt/prod/x"),
    ("spark.sql(\"LOAD DATA LOCAL INPATH '/etc/passwd' INTO TABLE sbx.t\")", "NBP-SQL-REFUSED", "LOAD DATA"),
    ("spark.sql(\"LOAD DATA INPATH 'oci://b@ns/sbx/in' INTO TABLE sbx.t\")", "NBP-SQL-REFUSED", "LOAD DATA"),
    ("spark.sql(\"INSERT OVERWRITE DIRECTORY '/tmp/out' SELECT 1\")", "NBP-SQL-REFUSED",
     "INSERT OVERWRITE DIRECTORY"),
    ("spark.sql(\"INSERT OVERWRITE LOCAL DIRECTORY 'oci://b@ns/sbx/out' USING parquet SELECT 1\")",
     "NBP-SQL-REFUSED", "INSERT OVERWRITE DIRECTORY"),
    # -- session resources, functions, privileges, procedures, scripting, multiple statements
    ("spark.sql(\"ADD FILE '/etc/passwd'\")", "NBP-SQL-REFUSED", "ADD FILE"),
    ("spark.sql(\"ADD ARCHIVE 'oci://b@ns/sbx/x.zip'\")", "NBP-SQL-REFUSED", "ADD ARCHIVE"),
    ("spark.sql(\"ADD JARS 'oci://b@ns/sbx/a.jar' 'oci://b@ns/sbx/b.jar'\")", "NBP-SQL-REFUSED", "ADD JARS"),
    ("spark.sql('LIST JAR')", "NBP-SQL-REFUSED", "LIST JAR"),
    ("spark.sql('LIST FILES')", "NBP-SQL-REFUSED", "LIST FILES"),
    ("spark.sql(\"CREATE FUNCTION sbx.f AS 'com.evil.Udf'\")", "NBP-SQL-REFUSED", "CREATE FUNCTION"),
    ("spark.sql(\"CREATE OR REPLACE TEMPORARY FUNCTION f AS 'com.evil.Udf' USING JAR 'oci://b@ns/x.jar'\")",
     "NBP-SQL-REFUSED", "CREATE FUNCTION"),
    ("spark.sql('CREATE FUNCTION sbx.f(x INT) RETURNS INT RETURN x + 1')", "NBP-SQL-REFUSED", "CREATE FUNCTION"),
    ("spark.sql('DROP FUNCTION IF EXISTS sbx.f')", "NBP-SQL-REFUSED", "DROP FUNCTION"),
    ("spark.sql('DROP TEMPORARY FUNCTION f')", "NBP-SQL-REFUSED", "DROP FUNCTION"),
    ("spark.sql('GRANT SELECT ON TABLE sbx.t TO `u`')", "NBP-SQL-REFUSED", "GRANT"),
    ("spark.sql('REVOKE ALL PRIVILEGES ON SCHEMA sbx FROM `u`')", "NBP-SQL-REFUSED", "REVOKE"),
    ("spark.sql('CALL sbx.proc()')", "NBP-SQL-REFUSED", "CALL"),
    ("spark.sql('DECLARE v STRING')", "NBP-SQL-REFUSED", "DECLARE"),
    ("spark.sql(\"EXECUTE IMMEDIATE 'DROP TABLE prod.t'\")", "NBP-SQL-REFUSED", "EXECUTE"),
    ("spark.sql('BEGIN END')", "NBP-SQL-REFUSED", "BEGIN"),
    ("spark.sql('BEGIN SELECT 1; END')", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql('SELECT 1; DROP TABLE prod.t')", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql('SELECT 1;\\nADD JAR x.jar;')", "NBP-SQL-REFUSED", "<multiple statements>"),
    # -- SET / RESET outside the conf allowlist, USE outside the sandbox
    ("spark.sql('SET spark.sql.extensions = com.evil.Ext')", "NBP-SQL-REFUSED", "SET spark.sql.extensions"),
    ("spark.sql('SET spark.hadoop.fs.oci.client.auth.secret = x')", "NBP-SQL-REFUSED",
     "SET spark.hadoop.fs.oci.client.auth.secret"),
    ("spark.sql('SET spark.driver.extraJavaOptions = -javaagent:x.jar')", "NBP-SQL-REFUSED",
     "SET spark.driver.extraJavaOptions"),
    ("spark.sql('SET spark.executor.extraClassPath = /tmp/x')", "NBP-SQL-REFUSED",
     "SET spark.executor.extraClassPath"),
    ("spark.sql('SET spark.databricks.io.cache.enabled = true')", "NBP-SQL-REFUSED",
     "SET spark.databricks.io.cache.enabled"),
    ("spark.sql('SET spark.sql.adaptive.token = x')", "NBP-SQL-REFUSED", "SET spark.sql.adaptive.token"),
    ("spark.sql('SET spark.sql.warehouse.dir = /tmp/x')", "NBP-SQL-REFUSED", "SET spark.sql.warehouse.dir"),
    ("spark.sql('SET')", "NBP-SQL-REFUSED", "SET"),
    ("spark.sql('SET -v')", "NBP-SQL-REFUSED", "SET -v"),
    ("spark.sql('RESET')", "NBP-SQL-REFUSED", "RESET"),
    ("spark.sql('RESET spark.sql.extensions')", "NBP-SQL-REFUSED", "RESET spark.sql.extensions"),
    ("spark.sql('SET VAR x = 1')", "NBP-SQL-REFUSED", "SET"),
    ("spark.sql('USE prod')", "NBP-SQL-TARGET", "prod"),
    ("spark.sql('USE CATALOG prod')", "NBP-SQL-TARGET", "prod"),
    ("spark.sql('USE prod.sbx')", "NBP-SQL-TARGET", "prod.sbx"),
    ("spark.sql('USE SCHEMA other')", "NBP-SQL-TARGET", "other"),
    # -- writes outside the sandbox through the verbs the old blocklist did not know
    ("spark.sql('OPTIMIZE prod.t ZORDER BY (a)')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('VACUUM prod.t RETAIN 0 HOURS')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql(\"VACUUM '/dbfs/mnt/prod/t'\")", "NBP-SQL-TARGET", "/dbfs/mnt/prod/t"),
    ("spark.sql('RESTORE TABLE prod.t TO VERSION AS OF 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('MSCK REPAIR TABLE prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('ANALYZE TABLE prod.t COMPUTE STATISTICS')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('ANALYZE TABLES IN prod COMPUTE STATISTICS')", "NBP-SQL-TARGET", "prod"),
    ("spark.sql('ANALYZE TABLES COMPUTE STATISTICS')", "NBP-SQL-TARGET", "ANALYZE TABLES COMPUTE STATISTICS"),
    ("spark.sql(\"COPY INTO prod.t FROM 'oci://b@ns/sbx/x' FILEFORMAT = CSV\")", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('REPLACE TABLE prod.t AS SELECT 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('MERGE WITH SCHEMA EVOLUTION INTO prod.t USING sbx.s ON 1 = 1 WHEN MATCHED THEN DELETE')",
     "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('CREATE MATERIALIZED VIEW prod.mv AS SELECT 1')", "NBP-SQL-TARGET", "prod.mv"),
    ("spark.sql('CREATE NAMESPACE prod')", "NBP-SQL-TARGET", "prod"),
    ("spark.sql('EXPLAIN INSERT INTO prod.t SELECT 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('CREATE CATALOG evil')", "NBP-SQL-REFUSED", "CREATE CATALOG"),
    ("spark.sql('CREATE VOLUME sbx.v')", "NBP-SQL-REFUSED", "CREATE VOLUME"),
    ("spark.sql('DROP CATALOG prod')", "NBP-SQL-REFUSED", "DROP CATALOG"),
    # -- the WITH head is NOT a read: the verb behind the CTE list is the one judged
    ("spark.sql('WITH x (a) AS (SELECT 1), y AS NOT MATERIALIZED (SELECT 2) "
     "DELETE FROM prod.t WHERE a IN (SELECT a FROM x)')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('WITH RECURSIVE x AS (SELECT 1) MERGE INTO prod.t USING x ON 1 = 1 WHEN MATCHED THEN DELETE')",
     "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('WITH x AS (SELECT 1) UPDATE prod.t SET a = 1')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('WITH x AS (SELECT 1) INSERT OVERWRITE TABLE prod.t SELECT * FROM x')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('WITH x AS (SELECT 1) ADD JAR x.jar')", "NBP-SQL-REFUSED", "ADD JAR"),
    ("spark.sql('WITH x AS (SELECT 1)')", "NBP-SQL-REFUSED", "WITH x AS (SELECT 1)"),
    # -- quoted text cannot steer the verdict (comment-led writes: see the baseline guards)
    ("spark.sql(\"SELECT '--' AS c FROM text.`/etc/passwd`\")", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"SELECT '/*' AS c, reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    # -- Hive multi-insert and unknown verbs: not on the allowlist
    ("spark.sql('FROM sbx.src INSERT OVERWRITE TABLE prod.t SELECT *')", "NBP-SQL-REFUSED", "FROM"),
    ("spark.sql('FROM sbx.t SELECT *')", "NBP-SQL-REFUSED", "FROM"),
    ("spark.sql('IMPORT TABLE x FROM y')", "NBP-SQL-REFUSED", "IMPORT"),
    ("spark.sql(\"REFRESH '/dbfs/mnt/prod/x'\")", "NBP-SQL-REFUSED", "REFRESH"),
    # -- the bare sql() helper and sqlContext go through the same classifier
    ("sql(\"SELECT reflect('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("sqlContext.sql(\"ADD JAR 'x.jar'\")", "NBP-SQL-REFUSED", "ADD JAR"),
    ("Q = \"SELECT java_method('a', 'b')\"\nspark.sql(Q)", "NBP-SQL-REFUSED", "java_method("),
    # -- dynamic statements whose literal fragments already carry a refused token
    ("spark.sql(\"SELECT reflect('java.lang.Runtime', 'getRuntime') FROM \" + t)", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(f'SELECT * FROM text.`/etc/passwd` WHERE a = {v}')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM sbx.t; ' + q)", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql(f'VALUES ({x}); DROP TABLE prod.t')", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql(f\"SELECT TRANSFORM(a) USING 'cat' FROM {t}\")", "NBP-SQL-REFUSED", "TRANSFORM ... USING"),
    # -- a dynamic WITH head is not a read verb either (other non-read heads: see the baseline guards)
    ("spark.sql(f'WITH x AS ({sub}) SELECT * FROM x')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    # -- a fresh / fetched session object would not carry the runtime SQL guard
    ("spark.newSession().sql('SELECT 1')", "NBP-POLICY-TAMPER", "spark.newSession"),
    ("s2 = spark.newSession()", "NBP-POLICY-TAMPER", "spark.newSession"),
    ("spark.builder.getOrCreate()", "NBP-POLICY-TAMPER", "spark.builder.getOrCreate"),
    ("from pyspark.sql import SparkSession\nSparkSession.builder.appName('x').getOrCreate()", "NBP-POLICY-TAMPER",
     "pyspark.sql.SparkSession.builder.appName.getOrCreate"),
    ("from pyspark.sql import SparkSession\ns = SparkSession.getActiveSession()", "NBP-POLICY-TAMPER",
     "pyspark.sql.SparkSession.getActiveSession"),
    ("import pyspark\npyspark.sql.SparkSession.builder.getOrCreate()", "NBP-POLICY-TAMPER",
     "pyspark.sql.SparkSession.builder.getOrCreate"),
]


# Regression guards: the round-2 tree (1356a56) ALREADY refused every cell below
# with the same rule and target (verified by running this file against that
# tree), so they prove nothing about the allowlist. They stay so that the
# allowlist can never regress below the old blocklist: the write verbs it knew
# on an out-of-sandbox target, comment-led writes, and dynamic statements whose
# literal head was never a read verb.
SQL_REFUSED_CELLS_BASELINE_GUARDS = [
    ("spark.sql('CREATE TABLE prod.t SHALLOW CLONE sbx.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql(\"ALTER TABLE sbx.t SET LOCATION 'oci://b@ns/prod/x'\")", "NBP-SQL-TARGET", "oci://b@ns/prod/x"),
    ("spark.sql('DROP VIEW tmp_v')", "NBP-SQL-TARGET", "tmp_v"),
    ("spark.sql('/* SELECT */ DROP TABLE prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('-- SELECT\\nDROP TABLE prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('ADD JAR ' + jar)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('SET ' + conf)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(f'OPTIMIZE {t}')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(f'COPY INTO sbx.t FROM {src} FILEFORMAT = CSV')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(f'CREATE FUNCTION {f} AS {cls}')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(*ARGS)", "NBP-SQL-DYNAMIC", "<dynamic>"),       # from adversarial round 2
]


@pytest.mark.parametrize("source,rule,target", SQL_REFUSED_CELLS + SQL_REFUSED_CELLS_BASELINE_GUARDS,
                         ids=[c[1] + ":" + c[0][:40] for c in SQL_REFUSED_CELLS + SQL_REFUSED_CELLS_BASELINE_GUARDS])
def test_sql_allowlist_refuses_the_cell_before_it_runs(tmp_path, sandbox_env, source, rule, target):
    # Through dbutils.notebook.run, i.e. with the runtime SQL guard active: the
    # static verdict alone must refuse these, the guard is the second line.
    path = write_notebook(tmp_path, "child", ["SQL_SETUP_OK = 1", source + "\nSQL_REFUSED_MARK = 1"])
    globals().pop("SQL_SETUP_OK", None)
    globals().pop("SQL_REFUSED_MARK", None)
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
    finally:
        globals().pop("Q", None)
        globals().pop("s2", None)
        globals().pop("s", None)
    err = ei.value
    assert err.rule_id == rule
    assert err.target == target
    assert err.cell_index == 1
    assert err.remediation
    assert globals().pop("SQL_SETUP_OK") == 1
    assert "SQL_REFUSED_MARK" not in globals()
    refused = [e for e in npol.get_policy_log() if e["event"] == "refused"]
    assert refused and refused[0]["rule"] == rule and refused[0]["target"] == target
    assert refused[0]["notebook_path"] == path and refused[0]["cell_index"] == 1


# Legitimate reads and in-sandbox writes (sandbox: default.sbx, prefix oci://b@ns/sbx/).
SQL_ALLOWED_CELLS = [
    # reads
    "spark.sql('SELECT * FROM prod.t')",
    "spark.sql('VALUES (1, 2), (3, 4)')",
    "spark.sql('TABLE prod.t')",
    "spark.sql('WITH x AS (SELECT 1) SELECT * FROM x')",
    "spark.sql('WITH RECURSIVE x AS (SELECT 1), y (a) AS NOT MATERIALIZED (SELECT 2) SELECT * FROM x JOIN y')",
    "spark.sql('SHOW TABLES IN prod')",
    "spark.sql('SHOW CREATE TABLE prod.t')",
    "spark.sql('SHOW TBLPROPERTIES prod.t')",
    "spark.sql('DESCRIBE prod.t')",
    "spark.sql('DESC EXTENDED prod.t')",
    "spark.sql('DESCRIBE FORMATTED prod.t')",
    "spark.sql('DESCRIBE FUNCTION EXTENDED upper')",
    "spark.sql('DESCRIBE DATABASE prod')",
    "spark.sql('DESCRIBE QUERY SELECT 1')",
    "spark.sql('EXPLAIN SELECT * FROM prod.t')",
    "spark.sql('EXPLAIN EXTENDED WITH x AS (SELECT 1) SELECT * FROM x')",
    "spark.sql('EXPLAIN FORMATTED INSERT INTO sbx.t SELECT 1')",
    "spark.sql('CACHE TABLE c AS SELECT * FROM prod.t')",
    "spark.sql('CACHE LAZY TABLE prod.t')",
    "spark.sql('UNCACHE TABLE IF EXISTS c')",
    "spark.sql('CLEAR CACHE')",
    "spark.sql('REFRESH TABLE prod.t')",
    "spark.sql('REFRESH FUNCTION upper')",
    "spark.sql('CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM prod.t')",
    "spark.sql('CREATE GLOBAL TEMPORARY VIEW gv AS SELECT 1')",
    "spark.sql('CREATE TEMPORARY VIEW IF NOT EXISTS v (a, b) AS SELECT 1, 2')",
    "spark.sql(\"CREATE TEMPORARY VIEW v USING parquet OPTIONS (path 'oci://b@ns/prod/in')\")",
    "spark.sql(\"CREATE TEMPORARY VIEW v USING csv OPTIONS (header 'true', path 'oci://b@ns/prod/in.csv')\")",
    # path sources: object storage anywhere, local paths under a prefix, an alias that looks like a format
    "spark.sql('SELECT * FROM parquet.`oci://b@ns/prod/in`')",
    "spark.sql('SELECT * FROM delta.`abfss://c@a.dfs.core.windows.net/prod/in`')",
    "spark.sql('SELECT * FROM csv.`s3://bucket/prod/in.csv`')",
    "spark.sql('SELECT * FROM json.`oci://b@ns/sbx/in`')",
    "spark.sql('SELECT text.a FROM prod.t text')",
    "spark.sql(\"COPY INTO sbx.t FROM 'oci://b@ns/prod/in' FILEFORMAT = PARQUET\")",
    # in-sandbox writes through every write verb
    "spark.sql('CREATE TABLE IF NOT EXISTS default.sbx.t (a INT)')",
    "spark.sql('CREATE OR REPLACE TABLE `default`.`sbx`.`t` AS SELECT 1')",
    "spark.sql(\"CREATE EXTERNAL TABLE sbx.t2 USING parquet LOCATION 'oci://b@ns/sbx/t2'\")",
    "spark.sql(\"CREATE TABLE sbx.t3 USING csv OPTIONS (header 'true', 'path' = 'oci://b@ns/sbx/t3')\")",
    "spark.sql('CREATE SCHEMA IF NOT EXISTS default.sbx')",
    "spark.sql('CREATE DATABASE IF NOT EXISTS sbx')",
    "spark.sql('REPLACE TABLE sbx.t AS SELECT 1')",
    "spark.sql('INSERT INTO sbx.t VALUES (1)')",
    "spark.sql('INSERT INTO TABLE sbx.t PARTITION (d = 1) SELECT 1')",
    "spark.sql('INSERT OVERWRITE sbx.t SELECT * FROM prod.src')",
    "spark.sql('INSERT INTO sbx.t SELECT * FROM parquet.`oci://b@ns/prod/in`')",
    "spark.sql('WITH x AS (SELECT 1 AS a) INSERT INTO sbx.t SELECT a FROM x')",
    "spark.sql('WITH x AS (SELECT 1) MERGE INTO sbx.t USING x ON 1 = 1 WHEN MATCHED THEN DELETE')",
    "spark.sql('MERGE INTO default.sbx.t t USING prod.s s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *')",
    "spark.sql('UPDATE sbx.t SET a = 1 WHERE a = 0')",
    "spark.sql('DELETE FROM sbx.t WHERE a = 0')",
    "spark.sql('DROP TABLE IF EXISTS sbx.t')",
    "spark.sql('DROP VIEW IF EXISTS sbx.v')",
    "spark.sql('DROP SCHEMA IF EXISTS sbx CASCADE')",
    "spark.sql('ALTER TABLE sbx.t ADD COLUMNS (b STRING)')",
    "spark.sql(\"ALTER TABLE sbx.t SET LOCATION 'oci://b@ns/sbx/t_moved'\")",
    "spark.sql(\"ALTER SCHEMA default.sbx SET DBPROPERTIES (owner = 'mig')\")",
    "spark.sql('TRUNCATE TABLE sbx.t')",
    "spark.sql('OPTIMIZE sbx.t ZORDER BY (a)')",
    "spark.sql('VACUUM default.sbx.t RETAIN 168 HOURS')",
    "spark.sql(\"VACUUM 'oci://b@ns/sbx/t'\")",
    "spark.sql('RESTORE TABLE sbx.t TO VERSION AS OF 1')",
    "spark.sql('MSCK REPAIR TABLE sbx.t')",
    "spark.sql('ANALYZE TABLE sbx.t COMPUTE STATISTICS FOR ALL COLUMNS')",
    "spark.sql('ANALYZE TABLES IN default.sbx COMPUTE STATISTICS')",
    "spark.sql('CREATE TABLE sbx.t2 SHALLOW CLONE prod.t')",
    "spark.sql('CREATE TABLE sbx.t2 DEEP CLONE delta.`oci://b@ns/prod/t`')",
    "spark.sql('INSERT INTO delta.`oci://b@ns/sbx/x` SELECT 1')",
    "spark.sql('OPTIMIZE delta.`oci://b@ns/sbx/x`')",
    # session confs on the allowlist, USE of the sandbox
    "spark.sql('SET spark.sql.shuffle.partitions = 8')",
    "spark.sql('SET spark.sql.adaptive.enabled = true')",
    "spark.sql('SET spark.sql.adaptive.coalescePartitions.enabled = false')",
    "spark.sql('SET spark.sql.autoBroadcastJoinThreshold = -1')",
    "spark.sql('SET spark.sql.session.timeZone = UTC')",
    "spark.sql('SET spark.sql.shuffle.partitions')",
    "spark.sql(\"SET TIME ZONE 'Europe/London'\")",
    "spark.sql('RESET spark.sql.shuffle.partitions')",
    "spark.sql('USE default')",
    "spark.sql('USE CATALOG default')",
    "spark.sql('USE default.sbx')",
    "spark.sql('USE SCHEMA sbx')",
    "spark.sql('USE sbx')",
    # quoted text, comments and identifiers cannot trigger a refusal
    "spark.sql('SELECT 1;')",
    "spark.sql(\"SELECT ';' AS semi FROM prod.t\")",
    "spark.sql(\"SELECT 'reflect(' AS s, 'ADD JAR' AS j FROM prod.t\")",
    "spark.sql('SELECT `reflect(` FROM prod.t')",
    "spark.sql('SELECT transform(arr, x -> x + 1) FROM prod.t')",
    "spark.sql('SELECT * FROM prod.t -- reflect(1)')",
    "spark.sql('SELECT * FROM prod.t /* ADD JAR */')",
    "spark.sql('SELECT * FROM prod.`text.`')",
    # parameterised SQL keeps a literal statement; the other spellings of the call
    "spark.sql('SELECT * FROM prod.t WHERE id = :id', args={'id': 1})",
    "spark.sql(sqlQuery='SELECT 1')",
    "sql('SELECT 1')",
    "sqlContext.sql('SHOW TABLES')",
    "Q = 'WITH x AS (SELECT 1) SELECT * FROM x'\nspark.sql(Q)",
]


@pytest.mark.parametrize("source", SQL_ALLOWED_CELLS, ids=[c[:48] for c in SQL_ALLOWED_CELLS])
def test_sql_allowlist_passes_legitimate_reads_and_in_sandbox_writes(sandbox_env, source):
    assert npol.scan_source(source) == []
    assert [e for e in npol.get_policy_log() if e["event"] in npol.POLICY_NOTES] == []


# Dynamic statements with a literal read head: admitted ONLY while the runtime
# SQL guard is active (recorded as unverified_sql_read); a bare scan fails closed.
SQL_DYNAMIC_READ_CELLS = [
    "t = 'prod.t'\nspark.sql(f'SELECT count(*) FROM {t}')",
    "v = 'v'\nspark.sql(f'CREATE OR REPLACE TEMP VIEW {v} AS SELECT 1')",
    "q = 'SELECT * FROM sbx.t WHERE id=' + str(cid)\nspark.sql(q)",
    "for cid in ids:\n    q = 'SELECT * FROM sbx.t WHERE id=' + str(cid)\n    spark.sql(q)",
    "if flag:\n    q = f'SELECT count(*) FROM sbx.t WHERE d={d}'\n    spark.sql(q)",
    "spark.sql('SELECT * FROM sbx.t WHERE id=' + str(cid))",
    "spark.sql(f'WITH c AS (SELECT 1) SELECT * FROM c WHERE x={v}')",
    "spark.sql('SHOW TABLES IN {}'.format(s))",
    "spark.sql('-- filter\\nSELECT 1 FROM {}'.format(s))",
    "spark.sql('/* cols */ DESCRIBE {}'.format(s))",
    "spark.sql('DESC ' + t)",
    "spark.sql('EXPLAIN ' + q)",
    "spark.sql(f'VALUES ({x})')",
    "spark.sql(f'TABLE {t}')",
    "spark.sql(f'CACHE TABLE c AS SELECT * FROM {t}')",
    "spark.sql(f'REFRESH TABLE {t}')",
    "spark.sql('SELECT * FROM %s' % t)",
    'q = "SELECT id, amount FROM sbx.mig.orders WHERE region = \'" + region + "\'"\nspark.sql(q).show()',
]


@pytest.mark.parametrize("source", SQL_DYNAMIC_READ_CELLS, ids=[c[:48] for c in SQL_DYNAMIC_READ_CELLS])
def test_dynamic_read_is_admitted_only_with_the_runtime_sql_guard(sandbox_env, source):
    assert npol.scan_source(source, context=npol.ScanContext(runtime_sql_guard=True)) == []
    assert [v.rule for v in npol.scan_source(source)] == ["NBP-SQL-DYNAMIC"]
    assert [v.rule for v in npol.scan_source(source, context=npol.ScanContext())] == ["NBP-SQL-DYNAMIC"]


def test_dynamic_read_with_the_guard_is_recorded_as_unverified(sandbox_env):
    npol.enforce_cell_policy("spark.sql('SELECT * FROM sbx.t WHERE id = ' + str(cid))", notebook_path="nb",
                             cell_index=4, context=npol.ScanContext(runtime_sql_guard=True))
    notes = [e for e in npol.get_policy_log() if e["event"] == "unverified_sql_read"]
    assert len(notes) == 1
    assert notes[0]["target"] == "SELECT * FROM sbx.t WHERE id ="
    assert "runtime SQL guard" in notes[0]["detail"]["detail"]
    assert notes[0]["cell_index"] == 4


def test_classify_sql_reviewer_statements_and_verbs():
    pol = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/")
    v = npol.classify_sql("SELECT reflect('java.lang.Runtime', 'getRuntime')", pol)
    assert (v.allowed, v.kind, v.rule, v.target) == (False, "refused", "NBP-SQL-REFUSED", "reflect(")
    v = npol.classify_sql("SELECT * FROM text.`/etc/passwd`", pol)
    assert (v.allowed, v.rule, v.target, v.verb) == (False, "NBP-OPEN-PATH", "/etc/passwd", "SELECT")
    v = npol.classify_sql("ADD JAR 'oci://b@ns/x.jar'", pol)
    assert (v.allowed, v.rule, v.verb) == (False, "NBP-SQL-REFUSED", "ADD JAR")
    v = npol.classify_sql("WITH x AS (SELECT 1) INSERT INTO prod.table SELECT * FROM x", pol)
    assert (v.allowed, v.kind, v.rule, v.verb, v.target) == (False, "refused", "NBP-SQL-TARGET", "INSERT INTO",
                                                             "prod.table")
    # the CTE walk: the statement verb, not WITH, is the verb
    v = npol.classify_sql("WITH x (a) AS (SELECT 1), y AS (SELECT 2) INSERT INTO sbx.t SELECT * FROM x", pol)
    assert (v.allowed, v.kind, v.verb, v.target) == (True, "write", "INSERT INTO", "sbx.t")
    v = npol.classify_sql("WITH x AS (SELECT (1)) SELECT * FROM x", pol)
    assert (v.allowed, v.kind, v.verb) == (True, "read", "SELECT")
    v = npol.classify_sql("EXPLAIN COST WITH x AS (SELECT 1) SELECT * FROM x", pol)
    assert (v.allowed, v.kind, v.verb) == (True, "read", "SELECT")
    v = npol.classify_sql("  -- note\n  /* c */ CREATE OR REPLACE TEMP VIEW v AS SELECT 1", pol)
    assert (v.allowed, v.kind, v.verb) == (True, "read", "CREATE TEMP VIEW")
    assert npol.classify_sql("SELECT 1; SELECT 2", pol).target == "<multiple statements>"
    assert npol.classify_sql("", pol).rule == "NBP-SQL-REFUSED"
    assert npol.classify_sql(None, pol).rule == "NBP-SQL-REFUSED"
    # expressions: only the Java calls and the script transform matter
    assert npol.classify_sql_expression("a + 1", pol).allowed
    assert npol.classify_sql_expression("transform(arr, x -> x + 1)", pol).allowed
    assert npol.classify_sql_expression("Reflect('java.lang.Runtime', 'getRuntime')", pol).target == "reflect("
    assert npol.classify_sql_expression("java_method ('a', 'b')", pol).target == "java_method("


def test_assert_sql_allowed_records_and_raises_before_spark(sandbox_env):
    for stmt, rule in [("SELECT reflect('java.lang.Runtime', 'getRuntime')", "NBP-SQL-REFUSED"),
                       ("SELECT * FROM text.`/etc/passwd`", "NBP-OPEN-PATH"),
                       ("ADD JAR 'oci://b@ns/x.jar'", "NBP-SQL-REFUSED"),
                       ("WITH x AS (SELECT 1) INSERT INTO prod.table SELECT * FROM x", "NBP-SQL-TARGET")]:
        with pytest.raises(PolicyViolation) as ei:
            npol.assert_sql_allowed(stmt, operation="spark.sql", notebook_path="nb", cell_index=2)
        assert ei.value.rule_id == rule and ei.value.cell_index == 2 and ei.value.notebook_path == "nb"
        assert isinstance(ei.value, PermissionError)
    assert npol.assert_sql_allowed("SELECT 1", operation="spark.sql").kind == "read"
    assert npol.assert_sql_allowed("INSERT INTO sbx.t SELECT 1", operation="spark.sql").kind == "write"
    with pytest.raises(PolicyViolation) as ei:
        npol.assert_sql_expression_allowed("java_method('a', 'b')", operation="DataFrame.selectExpr")
    assert ei.value.rule_id == "NBP-SQL-REFUSED"
    log = [e for e in npol.get_policy_log() if e["event"] == "runtime_refused"]
    assert [e["rule"] for e in log] == ["NBP-SQL-REFUSED", "NBP-OPEN-PATH", "NBP-SQL-REFUSED", "NBP-SQL-TARGET",
                                        "NBP-SQL-REFUSED"]
    assert log[0]["detail"]["operation"] == "spark.sql" and log[0]["cell_index"] == 2
    assert log[-1]["detail"]["operation"] == "DataFrame.selectExpr"


# -- runtime SQL guard through dbutils.notebook.run, driven with fakes --------
class _FakeSpark:
    """Stand-in for a SparkSession: records the statements that reached it."""

    def __init__(self):
        self.executed = []

    def sql(self, sqlQuery, *args, **kwargs):
        self.executed.append(sqlQuery)
        return _FakeDF()

    def newSession(self):
        return _FakeSpark()


class _FakeSqlContext:
    def __init__(self):
        self.executed = []

    def sql(self, sqlQuery):
        self.executed.append(sqlQuery)
        return _FakeDF()


RUNTIME_GUARD_CELLS = [
    # (cells, rule, target): the static gate admits the dynamic read head, the guard refuses the final text
    (["T = 'text.`/etc/passwd`'", "spark.sql('SELECT * FROM ' + T)"], "NBP-OPEN-PATH", "/etc/passwd"),
    (["FN = 'reflect'", "spark.sql(f\"SELECT {FN}('java.lang.Runtime', 'getRuntime')\")"], "NBP-SQL-REFUSED",
     "reflect("),
    (["FN = 'java_method'", "spark.sql(f\"SELECT {FN}('java.lang.System', 'getenv', 'PATH')\")"],
     "NBP-SQL-REFUSED", "java_method("),
    (["TAIL = \"; ADD JAR 'oci://b@ns/x.jar'\"", "spark.sql('SELECT 1' + TAIL)"], "NBP-SQL-REFUSED",
     "<multiple statements>"),
    (["TAIL = \"; WITH x AS (SELECT 1) INSERT INTO prod.table SELECT * FROM x\"", "spark.sql('SELECT 1' + TAIL)"],
     "NBP-SQL-REFUSED", "<multiple statements>"),
    (["CMD = \"TRANSFORM(a) USING 'cat'\"", "spark.sql(f'SELECT {CMD} FROM sbx.t')"], "NBP-SQL-REFUSED",
     "TRANSFORM ... USING"),
    (["SRC = \"'/etc/passwd'\"", "spark.sql(f'SELECT * FROM csv.`/etc/passwd`'.replace('x', 'y'))"],
     "NBP-SQL-DYNAMIC", "<dynamic>"),      # a call the gate cannot fold: refused statically, fail closed
                                           # (so already at 1356a56: a regression guard, not a new refusal)
]


@pytest.mark.parametrize("cells,rule,target", RUNTIME_GUARD_CELLS, ids=[c[1] + ":" + c[0][-1][:40]
                                                                        for c in RUNTIME_GUARD_CELLS])
def test_runtime_sql_guard_refuses_the_final_statement_before_spark(tmp_path, sandbox_env, cells, rule, target):
    fake = _FakeSpark()
    original = _FakeSpark.sql
    g = globals()
    g["spark"] = fake
    path = write_notebook(tmp_path, "child", cells + ["GUARD_MARK = 1"])
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
    finally:
        for name in ("spark", "T", "FN", "TAIL", "CMD", "SRC", "GUARD_MARK"):
            g.pop(name, None)
    err = ei.value
    assert err.rule_id == rule and err.target == target
    assert err.notebook_path == path and err.cell_index == 1
    assert fake.executed == []                      # Spark never saw the statement
    assert "GUARD_MARK" not in globals()
    # the guard is removed again, whatever the outcome of the run
    assert "sql" not in vars(fake) and _FakeSpark.sql is original
    log = npol.get_policy_log()
    if rule == "NBP-SQL-DYNAMIC":
        assert [e["rule"] for e in log if e["event"] == "refused"] == [rule]
    else:
        assert [e["event"] for e in log if e["event"] in ("unverified_sql_read", "runtime_refused")] == \
            ["unverified_sql_read", "runtime_refused"]
        rr = [e for e in log if e["event"] == "runtime_refused"][0]
        assert rr["rule"] == rule and rr["target"] == target
        assert rr["notebook_path"] == path and rr["cell_index"] == 1
        assert rr["detail"]["operation"] == "spark.sql"


def test_runtime_sql_guard_lets_legitimate_dynamic_reads_through_and_restores_the_session(tmp_path, sandbox_env):
    fake, ctx = _FakeSpark(), _FakeSqlContext()
    original = _FakeSpark.sql
    g = globals()
    g["spark"], g["sqlContext"] = fake, ctx
    path = write_notebook(tmp_path, "child", [
        "TBL = 'prod.orders'",
        "rows = spark.sql(f'WITH x AS (SELECT 1) SELECT * FROM x UNION ALL SELECT 1 FROM {TBL}')",
        "spark.sql('INSERT INTO sbx.t SELECT 1')",
        "sqlContext.sql(f'SHOW TABLES IN {TBL}')",
        "spark.sql(sqlQuery=f'DESCRIBE {TBL}')",
        "dbutils.notebook.exit('done')",
    ])
    g["dbutils"] = type("D", (), {"notebook": runner(tmp_path)})()
    try:
        assert runner(tmp_path).run(path) == "done"
    finally:
        for name in ("spark", "sqlContext", "dbutils", "TBL", "rows"):
            g.pop(name, None)
    assert fake.executed == ["WITH x AS (SELECT 1) SELECT * FROM x UNION ALL SELECT 1 FROM prod.orders",
                             "INSERT INTO sbx.t SELECT 1", "DESCRIBE prod.orders"]
    assert ctx.executed == ["SHOW TABLES IN prod.orders"]
    log = npol.get_policy_log()
    assert [e["event"] for e in log if e["event"] != "sandbox_declared"] == ["unverified_sql_read"] * 3
    assert [e for e in log if e["event"] in ("refused", "runtime_refused")] == []
    # restored: the migrator's own code is unaffected after the run
    assert "sql" not in vars(fake) and "sql" not in vars(ctx) and _FakeSpark.sql is original
    fake.sql("ADD JAR 'x.jar'")
    assert fake.executed[-1] == "ADD JAR 'x.jar'"


def test_sql_runtime_guard_wraps_session_objects_and_classes(sandbox_env):
    from aidp_compat.notebook import SqlRuntimeGuard
    fake, ctx = _FakeSpark(), _FakeSqlContext()
    original = _FakeSpark.sql
    ns = {"spark": fake, "sqlContext": ctx, "sc": object(), "pyspark": npol, "SparkSession": _FakeSpark,
          "unrelated": _FakeSpark()}
    policy = npol.require_policy()
    with SqlRuntimeGuard(policy, ns, notebook_path="nb") as guard:
        # the class bound as `SparkSession` is wrapped under that name, once, and covers every instance
        assert {"spark.sql", "sqlContext.sql", "SparkSession.sql", "_FakeSqlContext.sql"} <= set(guard.patched)
        assert len([p for p in guard.patched if p in ("SparkSession.sql", "_FakeSpark.sql")]) == 1
        with pytest.raises(PolicyViolation) as ei:
            fake.sql("ADD JAR 'x.jar'")
        assert ei.value.rule_id == "NBP-SQL-REFUSED" and ei.value.notebook_path == "nb"
        with pytest.raises(PolicyViolation):
            _FakeSpark.sql(fake, "SELECT reflect('a', 'b')")          # class-level call
        with pytest.raises(PolicyViolation):
            ns["unrelated"].sql("SELECT java_method('a', 'b')")          # another instance of the class
        with pytest.raises(PolicyViolation):
            fake.newSession().sql("SELECT reflect('a', 'b')")          # a new session of the same class
        with pytest.raises(PolicyViolation) as ei:
            ctx.sql("SELECT * FROM text.`/etc/passwd`")
        assert ei.value.rule_id == "NBP-OPEN-PATH"
        with pytest.raises(PolicyViolation):
            fake.sql(sqlQuery="WITH x AS (SELECT 1) INSERT INTO prod.table SELECT * FROM x")
        fake.sql("SELECT 1")
        fake.sql("INSERT INTO sbx.t SELECT 1", args={"a": 1})
        assert fake.executed == ["SELECT 1", "INSERT INTO sbx.t SELECT 1"]
    assert "sql" not in vars(fake) and "sql" not in vars(ctx) and _FakeSpark.sql is original
    assert _FakeSqlContext.sql.__name__ == "sql"
    fake.sql("ADD JAR 'x.jar'")                                         # untouched after the block
    assert fake.executed[-1] == "ADD JAR 'x.jar'"
    assert [e["rule"] for e in npol.get_policy_log() if e["event"] == "runtime_refused"] == \
        ["NBP-SQL-REFUSED"] * 4 + ["NBP-OPEN-PATH", "NBP-SQL-TARGET"]


def test_sql_runtime_guard_is_a_no_op_for_dataframes_without_pyspark(sandbox_env, monkeypatch):
    from aidp_compat.notebook import SqlRuntimeGuard
    monkeypatch.setitem(sys.modules, "pyspark", None)                   # import pyspark -> ImportError
    for name in [m for m in sys.modules if m.startswith("pyspark.")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    guard = SqlRuntimeGuard(npol.require_policy(), {}).install()
    try:
        assert guard.patched == []
    finally:
        guard.restore()


def _stub_pyspark(monkeypatch):
    """A minimal pyspark stand-in: the classes / functions the guard patches."""
    import types
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")

    class DataFrame:
        def __init__(self):
            self.calls = []

        def selectExpr(self, *exprs):
            self.calls.append(("selectExpr", exprs))
            return self

        def filter(self, condition):
            self.calls.append(("filter", condition))
            return self

        def where(self, condition):
            self.calls.append(("where", condition))
            return self

    class SparkSession:
        def __init__(self):
            self.executed = []

        def sql(self, sqlQuery, **kwargs):
            self.executed.append(sqlQuery)
            return DataFrame()

    def expr(str):
        return ("expr", str)

    sql.DataFrame, sql.SparkSession, sql.functions, functions.expr = DataFrame, SparkSession, functions, expr
    pyspark.sql = sql
    for name, mod in (("pyspark", pyspark), ("pyspark.sql", sql), ("pyspark.sql.functions", functions)):
        monkeypatch.setitem(sys.modules, name, mod)
    for name in [m for m in sys.modules if m.startswith("pyspark.sql.c")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    return pyspark


FRAME_EXPRESSION_CELLS = [
    "df.selectExpr('a', \"reflect('java.lang.Runtime', 'getRuntime')\")",
    "df.selectExpr([\"java_method('java.lang.System', 'getenv', 'PATH') AS p\"])",
    "df.filter(\"java_method('a', 'b') = 1\")",
    "df.where(\"REFLECT('a', 'b') IS NOT NULL\")",
    "from pyspark.sql import functions as F\nC = F.expr(\"reflect('a', 'b')\")",
    "from pyspark.sql.functions import expr\nC = expr(\"java_method('a', 'b')\")",
    "import pyspark.sql.functions as F\ndf.select(F.expr(\"reflect('a', 'b')\"))",
    "C = expr(\"reflect('a', 'b')\")",            # expr imported into the namespace before the run
]


@pytest.mark.parametrize("source", FRAME_EXPRESSION_CELLS, ids=[c[:48] for c in FRAME_EXPRESSION_CELLS])
def test_runtime_guard_refuses_java_calls_in_dataframe_expressions(tmp_path, sandbox_env, monkeypatch, source):
    stub = _stub_pyspark(monkeypatch)
    DataFrame, functions = stub.sql.DataFrame, stub.sql.functions
    originals = (DataFrame.selectExpr, DataFrame.filter, DataFrame.where, functions.expr)
    df = DataFrame()
    df.select = lambda *cols: df
    g = globals()
    g["df"], g["expr"] = df, functions.expr
    path = write_notebook(tmp_path, "child", [source, "FRAME_MARK = 1"])
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
        assert g["expr"] is functions.expr                        # the namespace binding is restored
    finally:
        for name in ("df", "expr", "F", "C", "FRAME_MARK"):
            g.pop(name, None)
    assert ei.value.rule_id == "NBP-SQL-REFUSED"
    assert ei.value.target in ("reflect(", "java_method(")
    assert ei.value.cell_index == 0 and ei.value.notebook_path == path
    assert df.calls == []
    assert (DataFrame.selectExpr, DataFrame.filter, DataFrame.where, functions.expr) == originals
    rr = [e for e in npol.get_policy_log() if e["event"] == "runtime_refused"]
    assert len(rr) == 1 and rr[0]["detail"]["operation"] in ("DataFrame.selectExpr", "DataFrame.filter",
                                                             "DataFrame.where", "functions.expr", "expr()")


def test_runtime_guard_lets_legitimate_dataframe_expressions_through(tmp_path, sandbox_env, monkeypatch):
    stub = _stub_pyspark(monkeypatch)
    DataFrame, functions = stub.sql.DataFrame, stub.sql.functions
    session = stub.sql.SparkSession()
    df = DataFrame()
    g = globals()
    g["df"], g["spark"] = df, session
    path = write_notebook(tmp_path, "child", [
        "df.selectExpr('a + 1 AS b').filter('b > 0').where('b < 10')",
        "from pyspark.sql import functions as F\nC = F.expr('transform(arr, x -> x + 1)')",
        "df.filter(C)",                                              # a Column, not a string: not classified
        "spark.sql('SELECT 1')",
        "TBL = 'prod.t'",
        "spark.sql(f'SELECT * FROM {TBL}')",
    ])
    try:
        assert runner(tmp_path).run(path) == "ok"
        # the pyspark SparkSession class was patched for the run: every instance was guarded
        assert session.executed == ["SELECT 1", "SELECT * FROM prod.t"]
    finally:
        for name in ("df", "spark", "F", "C", "TBL"):
            g.pop(name, None)
    assert [c[0] for c in df.calls] == ["selectExpr", "filter", "where", "filter"]
    assert [e for e in npol.get_policy_log() if e["event"] in ("refused", "runtime_refused")] == []
    assert stub.sql.SparkSession.sql.__name__ == "sql" and "sql" not in vars(session)
    assert DataFrame.selectExpr.__name__ == "selectExpr" and functions.expr.__name__ == "expr"


def test_runtime_guard_patches_the_pyspark_session_class_for_sessions_made_elsewhere(sandbox_env, monkeypatch):
    from aidp_compat.notebook import SqlRuntimeGuard
    stub = _stub_pyspark(monkeypatch)
    original = stub.sql.SparkSession.sql
    other = stub.sql.SparkSession()        # e.g. the session sql_magic.sql() fetches with getOrCreate()
    with SqlRuntimeGuard(npol.require_policy(), {}) as guard:
        assert "pyspark.sql.SparkSession.sql" in guard.patched
        assert {"pyspark.sql.DataFrame.selectExpr", "pyspark.sql.DataFrame.filter", "pyspark.sql.DataFrame.where",
                "pyspark.sql.functions.expr"} <= set(guard.patched)
        with pytest.raises(PolicyViolation):
            other.sql("ADD JAR 'x.jar'")
        other.sql("SELECT 1")
    assert stub.sql.SparkSession.sql is original
    assert other.executed == ["SELECT 1"]


# -- %sql cells: sql_magic turns them into spark.sql(\"\"\"..\"\"\") literals, so the static classifier applies
SQL_MAGIC_CELLS = [
    ("%sql SELECT reflect('java.lang.Runtime', 'getRuntime')", "NBP-SQL-REFUSED", "reflect("),
    ("%sql\nADD JAR 'oci://b@ns/prod/evil.jar'", "NBP-SQL-REFUSED", "ADD JAR"),
    ("%sql SELECT * FROM text.`/etc/passwd`", "NBP-OPEN-PATH", "/etc/passwd"),
    ("%sql\nWITH x AS (SELECT 1)\nINSERT INTO prod.table\nSELECT * FROM x", "NBP-SQL-TARGET", "prod.table"),
]


@pytest.mark.parametrize("magic,rule,target", SQL_MAGIC_CELLS, ids=[c[1] + ":" + c[0][:30] for c in SQL_MAGIC_CELLS])
def test_sql_magic_cells_are_classified_after_transformation(sandbox_env, magic, rule, target):
    from aidp_compat.sql_magic import transform_sql_cells
    nb = {"cells": [{"cell_type": "code", "source": [magic]}]}
    src = "".join(transform_sql_cells(nb)["cells"][0]["source"])
    assert src.startswith('spark.sql("""')
    assert [(v.rule, v.target) for v in npol.scan_source(src)] == [(rule, target)]


def test_sql_magic_read_and_in_sandbox_write_cells_pass(sandbox_env):
    from aidp_compat.sql_magic import transform_sql_cells
    nb = {"cells": [{"cell_type": "code", "source": ["%sql SELECT * FROM prod.t WHERE a = 1"]},
                    {"cell_type": "code", "source": ["%sql\nWITH x AS (SELECT 1)\nINSERT INTO sbx.t SELECT * FROM x"]}]}
    for cell in transform_sql_cells(nb)["cells"]:
        assert npol.scan_source("".join(cell["source"])) == []


# ── SQL allowlist, review round 3 / adversarial round 2 ────────────────
# The reviewer's bypasses, closed as classes (sandbox: default.sbx, prefix
# oci://b@ns/sbx/). Every cell below fails against the round-1 allowlist (24b7a6c)
# and the round-2 tree (1356a56): nearly all pass those gates outright, the rest
# are refused there under a different rule or target. The one round-2 probe the
# round-2 tree already refused with the same verdict (spark.sql(*ARGS)) sits in
# SQL_REFUSED_CELLS_BASELINE_GUARDS. See the probe files and the proof-of-failure
# run in the review record.
SQL_REFUSED_CELLS_R2 = [
    # -- the Java-call functions as backtick-quoted identifiers (the masker used to hide them) and try_reflect
    ("spark.sql(\"SELECT `reflect`('java.lang.Runtime', 'getRuntime')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"SELECT `java_method`('java.lang.System', 'getenv', 'PATH')\")", "NBP-SQL-REFUSED", "java_method("),
    ("spark.sql(\"SELECT `REFLECT`('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"SELECT `reflect` ('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"SELECT ` reflect `('a', 'b')\")", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql(\"WITH c AS (SELECT `java_method`('a', 'b') r) SELECT * FROM c\")", "NBP-SQL-REFUSED",
     "java_method("),
    ("spark.sql(\"SELECT try_reflect('java.lang.Runtime', 'getRuntime')\")", "NBP-SQL-REFUSED", "try_reflect("),
    ("spark.sql(\"SELECT `try_reflect`('a', 'b')\")", "NBP-SQL-REFUSED", "try_reflect("),
    # -- path tables are structural: ANY provider (quoted short name, provider class) before a backtick path
    ("spark.sql('SELECT * FROM `text`.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM `TEXT`.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM `parquet`.`/dbfs/mnt/prod/x`')", "NBP-OPEN-PATH", "/dbfs/mnt/prod/x"),
    ("spark.sql('SELECT * FROM `delta`.`/tmp/other/x`')", "NBP-OPEN-PATH", "/tmp/other/x"),
    ("spark.sql('SELECT * FROM org.apache.spark.sql.execution.datasources.text.TextFileFormat.`/etc/passwd`')",
     "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM `org.apache.spark.sql.execution.datasources.text.TextFileFormat`.`/etc/passwd`')",
     "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM `com.databricks.spark.csv`.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT * FROM whatever.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('INSERT INTO `delta`.`/tmp/other/x` SELECT 1')", "NBP-SQL-TARGET", "/tmp/other/x"),
    ("spark.sql(f'SELECT * FROM `{fmt}`.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(f'SELECT * FROM {fmt}.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    # -- raw string literals have no escape processing: r'\' is one character and the rest is live SQL
    ("spark.sql(\"SELECT * FROM sbx.t WHERE a = r'\\\\' AND b = reflect('a', 'b')\")", "NBP-SQL-REFUSED",
     "reflect("),
    ("spark.sql(\"SELECT r'\\\\'; DROP TABLE prod.t\")", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql(\"SELECT r'\\\\'; ADD JAR 'x.jar'\")", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql(\"SELECT r'\\\\' FROM text.`/etc/passwd`\")", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql('SELECT R\"\\\\\"; DROP TABLE prod.t')", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql(\"SELECT * FROM sbx.t WHERE a = r'\\\\' AND b = `java_method`('a', 'b')\")", "NBP-SQL-REFUSED",
     "java_method("),
    # -- spark.sql keyword arguments are statement text (pyspark renders {name} before parsing); IDENTIFIER(..)
    ("spark.sql('SELECT * FROM {t}', t='text.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.sql(\"SELECT {f}('a', 'b')\", f='reflect')", "NBP-SQL-REFUSED", "reflect("),
    ("spark.sql('DROP TABLE {t}', t='prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('SELECT 1 {tail}', tail='; ADD JAR x.jar')", "NBP-SQL-REFUSED", "<multiple statements>"),
    ("spark.sql('SELECT * FROM IDENTIFIER(:t)', args={'t': 'text.`/etc/passwd`'})", "NBP-SQL-DYNAMIC",
     "IDENTIFIER("),
    ("spark.sql(\"SELECT * FROM IDENTIFIER('text.`/etc/passwd`')\")", "NBP-SQL-DYNAMIC", "IDENTIFIER("),
    ("spark.sql(f'SELECT * FROM IDENTIFIER({t})')", "NBP-SQL-DYNAMIC", "IDENTIFIER("),
    ("spark.sql('SELECT * FROM {t}', **{'t': 'text.`/etc/passwd`'})", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql(**{'sqlQuery': \"ADD JAR 'x.jar'\"})", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("KW = {'sqlQuery': \"ADD JAR 'x.jar'\"}\nspark.sql(**KW)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('DROP TABLE {t}', t=tbl)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.sql('SELECT * FROM {t.__class__}', t='x')", "NBP-SQL-DYNAMIC", "<dynamic>"),
    # -- ALTER .. RENAME TO: the destination is a write target
    ("spark.sql('ALTER TABLE sbx.t RENAME TO prod.t')", "NBP-SQL-TARGET", "prod.t"),
    ("spark.sql('ALTER VIEW sbx.v RENAME TO prod.v')", "NBP-SQL-TARGET", "prod.v"),
    ("spark.sql('ALTER TABLE default.sbx.t RENAME TO prod.other.t')", "NBP-SQL-TARGET", "prod.other.t"),
    ("spark.sql('ALTER TABLE sbx.t RENAME TO t2')", "NBP-SQL-TARGET", "t2"),
    ("spark.sql('ALTER TABLE `sbx`.`t` RENAME TO `prod`.`t`')", "NBP-SQL-TARGET", "`prod`.`t`"),
    # -- the DataFrame-API Java calls (no SQL string at all)
    ("from pyspark.sql import functions as F\ndf.select(F.reflect(F.lit('java.lang.Runtime'), F.lit('getRuntime')))",
     "NBP-SQL-REFUSED", "pyspark.sql.functions.reflect"),
    ("import pyspark.sql.functions as F\nC = F.java_method(F.lit('java.lang.System'), F.lit('getenv'), F.lit('PATH'))",
     "NBP-SQL-REFUSED", "pyspark.sql.functions.java_method"),
    ("from pyspark.sql.functions import reflect", "NBP-SQL-REFUSED", "pyspark.sql.functions.reflect"),
    ("from pyspark.sql.functions import try_reflect, lit", "NBP-SQL-REFUSED", "pyspark.sql.functions.try_reflect"),
    ("import pyspark\npyspark.sql.functions.reflect(pyspark.sql.functions.lit('a'))", "NBP-SQL-REFUSED",
     "pyspark.sql.functions.reflect"),
    ("from pyspark.sql import functions as F\ndf.select(F.call_function('reflect', F.lit('a'), F.lit('b')))",
     "NBP-SQL-REFUSED", "reflect("),
    ("from pyspark.sql import functions as F\ndf.select(F.call_function('JAVA_METHOD', F.lit('a')))",
     "NBP-SQL-REFUSED", "java_method("),
    ("from pyspark.sql.functions import call_function\ncall_function('try_reflect', C)", "NBP-SQL-REFUSED",
     "try_reflect("),
    ("from pyspark.sql import functions as F\nF.call_function(fn, F.lit('a'))", "NBP-SQL-REFUSED", "<non-string>"),
    # -- table reads through the DataFrame table API
    ("spark.table('text.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.read.table('`text`.`/etc/passwd`')", "NBP-OPEN-PATH", "/etc/passwd"),
    ("spark.readStream.table('delta.`/dbfs/mnt/prod/x`')", "NBP-OPEN-PATH", "/dbfs/mnt/prod/x"),
    ("sqlContext.table('parquet.`/tmp/other/x`')", "NBP-OPEN-PATH", "/tmp/other/x"),
    ("spark.read.format('delta').table('delta.`/Volumes/prod/x`')", "NBP-OPEN-PATH", "/Volumes/prod/x"),
    # -- USE / SET / CREATE TABLE .. LOCATION / ADD FILE through the Python API
    ("spark.catalog.setCurrentCatalog('prod')", "NBP-SQL-TARGET", "prod"),
    ("spark.catalog.setCurrentDatabase('other')", "NBP-SQL-TARGET", "other"),
    ("spark.catalog.setCurrentDatabase('prod.sbx')", "NBP-SQL-TARGET", "prod.sbx"),
    ("spark.catalog.setCurrentCatalog(cat)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.conf.set('spark.sql.defaultCatalog', 'prod')", "NBP-SQL-REFUSED", "spark.sql.defaultCatalog"),
    ("spark.conf.set('spark.sql.extensions', 'org.x.Ext')", "NBP-SQL-REFUSED", "spark.sql.extensions"),
    ("spark.conf.set('spark.sql.parser.escapedStringLiterals', 'true')", "NBP-SQL-REFUSED",
     "spark.sql.parser.escapedStringLiterals"),
    ("spark.conf.set('spark.hadoop.fs.oci.client.auth.secret', 'x')", "NBP-SQL-REFUSED",
     "spark.hadoop.fs.oci.client.auth.secret"),
    ("spark.conf.unset('spark.sql.extensions')", "NBP-SQL-REFUSED", "spark.sql.extensions"),
    ("sqlContext.setConf('spark.sql.warehouse.dir', '/tmp/x')", "NBP-SQL-REFUSED", "spark.sql.warehouse.dir"),
    ("spark.conf.set(key, value)", "NBP-SQL-DYNAMIC", "<dynamic>"),
    ("spark.catalog.createTable('sbx.t', path='/outside/f', source='csv')", "NBP-PATH-WRITE", "/outside/f"),
    ("spark.catalog.createExternalTable('sbx.t', '/outside/f')", "NBP-PATH-WRITE", "/outside/f"),
    ("spark.catalog.createTable('prod.t', path='oci://b@ns/sbx/t')", "NBP-TABLE-TARGET", "prod.t"),
    ("spark.catalog.createTable(name, path='oci://b@ns/sbx/t')", "NBP-TABLE-DYNAMIC", "<dynamic>"),
    ("spark.catalog.createTable('sbx.t', source='jdbc', url='jdbc:x', dbtable='t')", "NBP-IMPORT-NETWORK", "jdbc"),
    ("sc.addFile('/outside/f')", "NBP-BUILTIN-EXEC", "sc.addFile"),
    ("spark.sparkContext.addFile('oci://b@ns/sbx/x.txt')", "NBP-BUILTIN-EXEC", "spark.sparkContext.addFile"),
    # -- network data sources while allow_network is off: SQL and the DataFrame reader / writer
    ("spark.sql(\"CREATE TEMP VIEW v USING jdbc OPTIONS (url 'jdbc:postgresql://h/db', dbtable 't')\")",
     "NBP-IMPORT-NETWORK", "jdbc"),
    ("spark.sql(\"CREATE OR REPLACE TEMPORARY VIEW v USING kafka OPTIONS (`kafka.bootstrap.servers` 'h:9092')\")",
     "NBP-IMPORT-NETWORK", "kafka"),
    ("spark.sql(\"CREATE TEMP VIEW v USING socket OPTIONS (host 'h', port '9999')\")", "NBP-IMPORT-NETWORK",
     "socket"),
    ("spark.sql(\"CREATE TABLE sbx.t (a INT) USING jdbc OPTIONS (url 'jdbc:x', dbtable 't')\")",
     "NBP-IMPORT-NETWORK", "jdbc"),
    ("spark.sql('CREATE TABLE sbx.t USING org.x.CustomSource')", "NBP-IMPORT-NETWORK", "org.x.CustomSource"),
    ("spark.sql('CREATE TABLE sbx.t USING `JDBC`')", "NBP-IMPORT-NETWORK", "JDBC"),
    ("spark.read.format('jdbc').option('url', 'jdbc:x').option('dbtable', 't').load()", "NBP-IMPORT-NETWORK",
     "jdbc"),
    ("spark.read.jdbc('jdbc:postgresql://h/db', 't')", "NBP-IMPORT-NETWORK", "spark.read.jdbc"),
    ("spark.read.format('kafka').option('kafka.bootstrap.servers', 'h:9092').load()", "NBP-IMPORT-NETWORK",
     "kafka"),
    ("spark.readStream.format('socket').option('host', 'h').option('port', 9999).load()", "NBP-IMPORT-NETWORK",
     "socket"),
    ("df.write.format('jdbc').option('url', 'jdbc:x').save()", "NBP-IMPORT-NETWORK", "jdbc"),
    ("df.write.jdbc('jdbc:x', 't')", "NBP-IMPORT-NETWORK", "df.write.jdbc"),
    ("df.writeStream.format('kafka').option('topic', 't').start()", "NBP-IMPORT-NETWORK", "kafka"),
    ("df.writeTo('sbx.t').using('jdbc').create()", "NBP-IMPORT-NETWORK", "jdbc"),
    ("spark.read.format('com.custom.Source').load('oci://b@ns/prod/x')", "NBP-IMPORT-NETWORK", "com.custom.Source"),
    ("FMT = 'jd' + 'bc'\nspark.read.format(FMT).load()", "NBP-IMPORT-NETWORK", "jdbc"),
    # -- Delta metadata of a path, and the cluster-filesystem URI schemes
    ("spark.sql(\"DESCRIBE DETAIL '/etc'\")", "NBP-OPEN-PATH", "/etc"),
    ("spark.sql(\"DESCRIBE HISTORY '/dbfs/mnt/prod/x'\")", "NBP-OPEN-PATH", "/dbfs/mnt/prod/x"),
    ("spark.sql(\"DESC DETAIL 'file:///tmp/other/x'\")", "NBP-OPEN-PATH", "file:///tmp/other/x"),
    ("spark.sql('SELECT * FROM parquet.`dbfs:/mnt/prod/x`')", "NBP-OPEN-PATH", "dbfs:/mnt/prod/x"),
    ("spark.sql('SELECT * FROM parquet.`hdfs:///user/x`')", "NBP-OPEN-PATH", "hdfs:///user/x"),
    ("spark.sql('SELECT * FROM parquet.`local:/tmp/other/x`')", "NBP-OPEN-PATH", "local:/tmp/other/x"),
    ("spark.read.text('dbfs:/mnt/prod/x')", "NBP-OPEN-PATH", "dbfs:/mnt/prod/x"),
    ("spark.read.parquet('hdfs:///user/x')", "NBP-OPEN-PATH", "hdfs:///user/x"),
    ("spark.table('delta.`dbfs:/mnt/prod/x`')", "NBP-OPEN-PATH", "dbfs:/mnt/prod/x"),
]


@pytest.mark.parametrize("source,rule,target", SQL_REFUSED_CELLS_R2,
                         ids=[c[1] + ":" + c[0][:40] for c in SQL_REFUSED_CELLS_R2])
def test_sql_round2_bypass_classes_are_refused_before_the_cell_runs(tmp_path, sandbox_env, source, rule, target):
    # Through dbutils.notebook.run, i.e. with the runtime SQL guard active: the
    # static verdict alone must refuse these, the guard is the second line.
    path = write_notebook(tmp_path, "child", ["SQL_SETUP_OK = 1", source + "\nSQL_REFUSED_MARK = 1"])
    g = globals()
    for name in ("SQL_SETUP_OK", "SQL_REFUSED_MARK"):
        g.pop(name, None)
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
    finally:
        for name in ("KW", "FMT", "C", "F"):
            g.pop(name, None)
    err = ei.value
    assert err.rule_id == rule
    assert err.target == target
    assert err.cell_index == 1
    assert err.remediation
    assert g.pop("SQL_SETUP_OK") == 1
    assert "SQL_REFUSED_MARK" not in g
    refused = [e for e in npol.get_policy_log() if e["event"] == "refused"]
    assert refused and refused[0]["rule"] == rule and refused[0]["target"] == target


def test_sql_round2_refused_cell_count():
    # 100 probes closed in adversarial round 2; spark.sql(*ARGS) moved to the baseline guards
    assert len(SQL_REFUSED_CELLS_R2) >= 99
    assert len(SQL_REFUSED_CELLS_BASELINE_GUARDS) == 11


# The reviewer's false refusals and the legitimate neighbours of every new rule.
SQL_ALLOWED_CELLS_R2 = [
    # <function> .. FROM '<string>' is not a path source; parenthesised query terms
    "spark.sql(\"SELECT trim(LEADING '/' FROM '/tmp/x') AS p\")",
    "spark.sql(\"SELECT trim(BOTH '/' FROM path) AS p FROM prod.t\")",
    "spark.sql(\"SELECT substring('abc' FROM 2 FOR 1), extract(YEAR FROM d) FROM prod.t\")",
    "spark.sql('(SELECT 1) UNION ALL (SELECT 2)')",
    "spark.sql('(SELECT * FROM prod.t) ORDER BY 1 LIMIT 5')",
    "spark.sql('((SELECT 1))')",
    "spark.sql('(SELECT 1) UNION (SELECT 2) UNION ALL (SELECT 3)')",
    "spark.sql('(WITH x AS (SELECT 1) SELECT * FROM x) UNION ALL (SELECT 2)')",
    # raw strings with ordinary content; quoted identifiers that are not functions / paths
    "spark.sql(\"SELECT r'\\\\d+' AS re, R'\\\\s' AS ws FROM prod.t\")",
    "spark.sql(\"SELECT * FROM sbx.t WHERE a = r'\\\\'\")",
    "spark.sql(\"SELECT 'r' AS x, r AS y FROM prod.t\")",
    "spark.sql('SELECT * FROM `default`.`sbx`.`t`')",
    "spark.sql('SELECT `tbl`.`col` FROM prod.t tbl')",
    "spark.sql('SELECT * FROM `reflect`')",
    "spark.sql('SELECT `java_method` FROM prod.t')",
    "spark.sql('SELECT * FROM prod.`text.`')",
    # RENAME TO inside the sandbox, partition / column renames
    "spark.sql('ALTER TABLE sbx.t RENAME TO sbx.t2')",
    "spark.sql('ALTER TABLE default.sbx.t RENAME TO `default`.`sbx`.`t3`')",
    "spark.sql('ALTER VIEW sbx.v RENAME TO sbx.v2')",
    "spark.sql('ALTER TABLE sbx.t PARTITION (a = 1) RENAME TO PARTITION (a = 2)')",
    "spark.sql('ALTER TABLE sbx.t RENAME COLUMN a TO b')",
    # providers that are not network sources; USING as a join / MERGE keyword
    "spark.sql('CREATE TABLE sbx.t USING delta AS SELECT * FROM a JOIN b USING (id)')",
    "spark.sql(\"CREATE TABLE sbx.t USING aidataplatform OPTIONS (`catalog.id` 'x', `pushdown.sql` 'SELECT 1')\")",
    "spark.sql(\"CREATE TEMP VIEW v USING csv OPTIONS (header 'true', path 'oci://b@ns/prod/in.csv')\")",
    "spark.sql('CREATE TEMPORARY VIEW v USING iceberg')",
    "spark.sql('MERGE INTO sbx.t USING jdbc ON jdbc.id = t.id WHEN MATCHED THEN DELETE')",
    "spark.sql('CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM a JOIN b USING (id)')",
    # keyword templates and parameter markers that render to a literal read / in-sandbox write
    "spark.sql('SELECT * FROM {t} WHERE n = {n}', t='prod.t', n=5)",
    "spark.sql('INSERT INTO {t} SELECT 1', t='sbx.t')",
    "spark.sql('SELECT * FROM {{prod}}.t')",
    "T = 'prod.t'\nspark.sql('SELECT * FROM {t}', t=T)",
    "spark.sql('SELECT * FROM prod.t WHERE id = :id AND d = :d', args={'id': 1, 'd': '2020-01-01'})",
    "spark.sql('SELECT * FROM prod.t WHERE id = ?', args=[1])",
    # table reads, session state and catalog DDL inside the sandbox
    "spark.table('prod.t')",
    "spark.read.table('`default`.`sbx`.`t`')",
    "spark.readStream.table('sbx.events')",
    "spark.table('parquet.`oci://b@ns/prod/in`')",
    "spark.catalog.setCurrentCatalog('default')",
    "spark.catalog.setCurrentDatabase('sbx')",
    "spark.catalog.setCurrentDatabase('default.sbx')",
    "spark.conf.set('spark.sql.shuffle.partitions', '8')",
    "spark.conf.set('spark.sql.adaptive.enabled', True)",
    "spark.conf.set('spark.sql.session.timeZone', 'UTC')",
    "spark.conf.unset('spark.sql.shuffle.partitions')",
    "sqlContext.setConf('spark.sql.autoBroadcastJoinThreshold', '-1')",
    "spark.conf.get('spark.sql.shuffle.partitions')",
    "spark.catalog.createTable('sbx.t', path='oci://b@ns/sbx/t', source='parquet')",
    "spark.catalog.createExternalTable('default.sbx.t', 'oci://b@ns/sbx/t', 'delta')",
    "spark.catalog.createTable('sbx.t', schema=schema)",
    "spark.catalog.listTables('sbx')",
    # readers / writers with built-in or platform providers, object-storage paths
    "spark.read.format('parquet').load('oci://b@ns/prod/in')",
    "spark.read.format('aidataplatform').option('catalog.id', 'x').load()",
    "spark.read.format('delta').option('versionAsOf', 1).load('oci://b@ns/prod/t')",
    "df.write.format('delta').mode('overwrite').save('oci://b@ns/sbx/out')",
    "df.writeTo('sbx.t').using('delta').create()",
    "spark.readStream.format('rate').load()",
    "df.writeStream.format('console').start()",
    "spark.sql('DESCRIBE DETAIL sbx.t')",
    "spark.sql('DESCRIBE HISTORY delta.`oci://b@ns/prod/t`')",
    "spark.sql(\"DESCRIBE DETAIL 'oci://b@ns/prod/t'\")",
    "spark.sql(\"COPY INTO sbx.t FROM (SELECT a FROM 'oci://b@ns/prod/in') FILEFORMAT = CSV\")",
]


@pytest.mark.parametrize("source", SQL_ALLOWED_CELLS_R2, ids=[c[:48] for c in SQL_ALLOWED_CELLS_R2])
def test_sql_round2_false_refusals_and_neighbours_pass(sandbox_env, source):
    assert npol.scan_source(source) == []
    assert [e for e in npol.get_policy_log() if e["event"] in npol.POLICY_NOTES] == []


def test_dynamic_provider_is_recorded_not_refused_and_lifted_by_allow_network(sandbox_env, monkeypatch):
    for source in ("spark.read.format(fmt).load(src)", "df.write.format(fmt).save('oci://b@ns/sbx/x')"):
        assert npol.scan_source(source) == []
        assert npol.enforce_cell_policy(source, notebook_path="nb", cell_index=1) == []
    notes = [e for e in npol.get_policy_log() if e["event"] == "unverified_format"]
    assert [n["target"] for n in notes] == ["spark.read.format(<dynamic>)", "df.write.format(<dynamic>)"]
    assert "network" in notes[0]["detail"]["detail"]
    npol._reset_policy_state()
    npol.clear_policy_log()
    monkeypatch.setenv("AIDP_SANDBOX_ALLOW_NETWORK", "1")
    for source in ("spark.read.format('jdbc').load()", "spark.read.jdbc('jdbc:x', 't')",
                   "spark.sql(\"CREATE TEMP VIEW v USING jdbc OPTIONS (url 'jdbc:x')\")",
                   "spark.readStream.format('kafka').load()", "spark.read.format(fmt).load(src)"):
        assert npol.scan_source(source) == [], source
    assert [e for e in npol.get_policy_log() if e["event"] in npol.POLICY_NOTES] == []


def test_table_read_with_a_dynamic_name_is_recorded_as_unverified(sandbox_env):
    assert npol.scan_source("df = spark.table(name)") == []
    assert npol.enforce_cell_policy("df = spark.table(name)", notebook_path="nb", cell_index=1) == []
    notes = [e for e in npol.get_policy_log() if e["event"] == "unverified_read_path"]
    assert [n["target"] for n in notes] == ["spark.table(<dynamic>)"]


# A {name} template whose keyword argument is not a literal: a statement with run-time
# fragments, admitted only with the runtime SQL guard (which renders the final text).
SQL_DYNAMIC_READ_CELLS_R2 = [
    "spark.sql('SELECT * FROM {t}', t=tbl)",
    "spark.sql('SELECT * FROM {t} WHERE n = {n}', t=str(tbl), n=count)",
    "spark.sql(f'SELECT count(*) FROM {t}', args={'d': 1})",
]


@pytest.mark.parametrize("source", SQL_DYNAMIC_READ_CELLS_R2, ids=[c[:48] for c in SQL_DYNAMIC_READ_CELLS_R2])
def test_keyword_template_with_runtime_values_is_a_dynamic_read(sandbox_env, source):
    assert npol.scan_source(source, context=npol.ScanContext(runtime_sql_guard=True)) == []
    assert [v.rule for v in npol.scan_source(source)] == ["NBP-SQL-DYNAMIC"]


def test_classify_sql_round2_statements():
    pol = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/")
    v = npol.classify_sql("SELECT `reflect`('java.lang.Runtime', 'getRuntime')", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-SQL-REFUSED", "reflect(")
    v = npol.classify_sql("SELECT * FROM `text`.`/outside/f`", pol)
    assert (v.allowed, v.rule, v.target, v.verb) == (False, "NBP-OPEN-PATH", "/outside/f", "SELECT")
    v = npol.classify_sql("SELECT * FROM sbx.t WHERE a = r'\\' AND b = reflect('a','b')", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-SQL-REFUSED", "reflect(")
    v = npol.classify_sql("SELECT r'\\'; DROP TABLE prod.t", pol)
    assert (v.allowed, v.target) == (False, "<multiple statements>")
    v = npol.classify_sql("SELECT * FROM IDENTIFIER(:t)", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-SQL-DYNAMIC", "IDENTIFIER(")
    v = npol.classify_sql("ALTER TABLE sbx.t RENAME TO prod.t", pol)
    assert (v.allowed, v.rule, v.verb, v.target) == (False, "NBP-SQL-TARGET", "ALTER TABLE RENAME TO", "prod.t")
    v = npol.classify_sql("ALTER TABLE sbx.t RENAME TO sbx.t2", pol)
    assert (v.allowed, v.kind, v.verb, v.target) == (True, "write", "ALTER TABLE", "sbx.t")
    v = npol.classify_sql("CREATE TEMP VIEW v USING jdbc OPTIONS (url 'jdbc:x')", pol)
    assert (v.allowed, v.rule, v.verb, v.target) == (False, "NBP-IMPORT-NETWORK", "CREATE TEMP VIEW", "jdbc")
    net = SandboxPolicy(catalog="default", schema="sbx", prefix="oci://b@ns/sbx/", allow_network=True)
    assert npol.classify_sql("CREATE TEMP VIEW v USING jdbc OPTIONS (url 'jdbc:x')", net).allowed
    v = npol.classify_sql("(SELECT 1) UNION ALL (SELECT 2)", pol)
    assert (v.allowed, v.kind, v.verb) == (True, "read", "SELECT")
    v = npol.classify_sql("(SELECT 1) INSERT INTO prod.t", pol)
    assert (v.allowed, v.rule) == (False, "NBP-SQL-REFUSED")
    v = npol.classify_sql("(INSERT INTO prod.t SELECT 1)", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-SQL-TARGET", "prod.t")
    assert npol.classify_sql("SELECT trim(LEADING '/' FROM '/tmp/x') AS p", pol).allowed
    v = npol.classify_sql("DESCRIBE DETAIL '/outside/f'", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-OPEN-PATH", "/outside/f")
    v = npol.classify_sql("SELECT * FROM parquet.`dbfs:/outside/f`", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-OPEN-PATH", "dbfs:/outside/f")
    # the structural path-table rule in a write target, and a legit backtick 3-part name
    v = npol.classify_sql("INSERT INTO `delta`.`/tmp/other/x` SELECT 1", pol)
    assert (v.allowed, v.rule, v.target) == (False, "NBP-SQL-TARGET", "/tmp/other/x")
    assert npol.classify_sql("INSERT INTO `default`.`sbx`.`t` SELECT 1", pol).allowed
    # expressions and function names
    assert npol.classify_sql_expression("`reflect`('a', 'b')", pol).target == "reflect("
    assert npol.classify_sql_expression("try_reflect('a', 'b')", pol).target == "try_reflect("
    assert npol.classify_sql_function("reflect", pol).target == "reflect("
    assert npol.classify_sql_function("`JAVA_METHOD`", pol).target == "java_method("
    assert npol.classify_sql_function(None, pol).target == "<non-string>"
    assert npol.classify_sql_function("upper", pol).allowed


def test_render_sql_template_matches_pyspark_substitution():
    assert npol.render_sql_template("SELECT * FROM {t} WHERE n = {n}", {"t": "prod.t", "n": 5}) == \
        "SELECT * FROM prod.t WHERE n = 5"
    assert npol.render_sql_template("SELECT {{a}} FROM {t}", {"t": "x"}) == "SELECT {a} FROM x"
    assert npol.render_sql_template("SELECT * FROM {u}", {"t": "x"}) is None          # unknown field
    assert npol.render_sql_template("SELECT * FROM {0}", {"t": "x"}) is None          # positional field
    assert npol.render_sql_template("SELECT * FROM {t.__class__}", {"t": "x"}) is None
    assert npol.render_sql_template("SELECT * FROM {t[0]}", {"t": "x"}) is None
    assert npol.render_sql_template("SELECT * FROM {t", {"t": "x"}) is None           # malformed

    class Column:                       # a pyspark object stands in as a neutral identifier
        pass
    Column.__module__ = "pyspark.sql.column"
    assert npol.render_sql_template("SELECT {c} FROM {t}", {"c": Column(), "t": "prod.t"}) == \
        "SELECT _aidp_param_c FROM prod.t"
    assert npol.sql_template_fragments("SELECT * FROM {t} WHERE n = {n} AND x") == \
        ["SELECT * FROM ", " WHERE n = ", " AND x"]
    assert npol.sql_template_fragments("SELECT {{x}} FROM {t}") == ["SELECT {x} FROM ", ""]
    assert npol.sql_template_fragments("SELECT * FROM {t") == []


class _FakeSparkKw:
    """SparkSession stand-in with pyspark's sql(sqlQuery, args=None, **kwargs) and table()."""

    def __init__(self):
        self.executed = []

    def sql(self, sqlQuery, args=None, **kwargs):
        self.executed.append((sqlQuery, args, kwargs))
        return _FakeDF()

    def table(self, tableName):
        self.executed.append(("table", tableName))
        return _FakeDF()


RUNTIME_GUARD_CELLS_R2 = [
    # (cells, rule, target): the static gate admits the cell, the guard refuses the final text
    (["T = str('text.`/etc/passwd`')", "spark.sql('SELECT * FROM {t}', t=T)"], "NBP-OPEN-PATH", "/etc/passwd"),
    (["FN = str('reflect')", "spark.sql(\"SELECT {f}('java.lang.Runtime', 'getRuntime')\", f=FN)"],
     "NBP-SQL-REFUSED", "reflect("),
    (["FN = str('java_method')", "spark.sql(f\"SELECT `{FN}`('a', 'b')\")"], "NBP-SQL-REFUSED", "java_method("),
    (["P = str('/etc/passwd')", "spark.sql(f'SELECT * FROM text.`{P}`')"], "NBP-OPEN-PATH", "/etc/passwd"),
    (["FMT = str('text')", "spark.sql(f'SELECT * FROM `{FMT}`.`/etc/passwd`'.strip())"], "NBP-SQL-DYNAMIC",
     "<dynamic>"),                       # a call the gate cannot fold: refused statically, fail closed
                                         # (so already at 1356a56: a regression guard, not a new refusal)
    (["W = str('reflect')", "spark.sql(f\"SELECT * FROM sbx.t WHERE a = r'\\\\' AND b = {W}('a', 'b')\")"],
     "NBP-SQL-REFUSED", "reflect("),
    (["T = str('text.`/etc/passwd`')", "spark.table(T)"], "NBP-OPEN-PATH", "/etc/passwd"),
    (["T = str('`delta`.`dbfs:/mnt/prod/x`')", "spark.table(tableName=T)"], "NBP-OPEN-PATH", "dbfs:/mnt/prod/x"),
    (["TAIL = str('; DROP TABLE prod.t')", "spark.sql('SELECT 1 {x}', x=TAIL)"], "NBP-SQL-REFUSED",
     "<multiple statements>"),
    (["spark.sql('SELECT * FROM {t.__class__}', t=str('x'))"], "NBP-SQL-DYNAMIC", "<dynamic>"),
]


@pytest.mark.parametrize("cells,rule,target", RUNTIME_GUARD_CELLS_R2, ids=[c[1] + ":" + c[0][-1][:40]
                                                                           for c in RUNTIME_GUARD_CELLS_R2])
def test_runtime_sql_guard_judges_keyword_templates_and_table_reads(tmp_path, sandbox_env, cells, rule, target):
    fake = _FakeSparkKw()
    originals = (_FakeSparkKw.sql, _FakeSparkKw.table)
    g = globals()
    g["spark"] = fake
    path = write_notebook(tmp_path, "child", cells + ["GUARD_MARK = 1"])
    try:
        with pytest.raises(PolicyViolation) as ei:
            runner(tmp_path).run(path)
    finally:
        for name in ("spark", "T", "FN", "P", "FMT", "W", "TAIL", "GUARD_MARK"):
            g.pop(name, None)
    err = ei.value
    assert err.rule_id == rule and err.target == target
    assert err.notebook_path == path and err.cell_index == len(cells) - 1
    assert fake.executed == []                      # Spark never saw the statement / table name
    assert "GUARD_MARK" not in globals()
    assert "sql" not in vars(fake) and "table" not in vars(fake)
    assert (_FakeSparkKw.sql, _FakeSparkKw.table) == originals
    log = npol.get_policy_log()
    refused_static = [e for e in log if e["event"] == "refused"]
    if rule == "NBP-SQL-DYNAMIC" and refused_static:
        assert [e["rule"] for e in refused_static] == [rule]
    else:
        rr = [e for e in log if e["event"] == "runtime_refused"]
        assert len(rr) == 1 and rr[0]["rule"] == rule and rr[0]["target"] == target
        assert rr[0]["detail"]["operation"] in ("spark.sql", "spark.table")


def test_runtime_sql_guard_passes_rendered_templates_and_plain_table_reads(tmp_path, sandbox_env):
    fake = _FakeSparkKw()
    g = globals()
    g["spark"] = fake
    path = write_notebook(tmp_path, "child", [
        "T = str('prod.orders')",
        "spark.sql('SELECT * FROM {t} WHERE n = {n}', t=T, n=5)",
        "spark.sql('SELECT * FROM prod.t WHERE id = :id', args={'id': 1})",
        "spark.table(T)",
        "spark.sql('INSERT INTO {t} SELECT 1', t='sbx.t')",
    ])
    try:
        assert runner(tmp_path).run(path) == "ok"
    finally:
        for name in ("spark", "T"):
            g.pop(name, None)
    # the call reaches Spark untouched: pyspark performs the substitution itself
    assert fake.executed == [("SELECT * FROM {t} WHERE n = {n}", None, {"t": "prod.orders", "n": 5}),
                             ("SELECT * FROM prod.t WHERE id = :id", {"id": 1}, {}),
                             ("table", "prod.orders"),
                             ("INSERT INTO {t} SELECT 1", None, {"t": "sbx.t"})]
    assert [e for e in npol.get_policy_log() if e["event"] in ("refused", "runtime_refused")] == []
    assert "sql" not in vars(fake) and "table" not in vars(fake)


def _stub_pyspark_r2(monkeypatch):
    """The round-2 stand-in: DataFrameReader.table, SparkSession.table and the Java-call functions."""
    stub = _stub_pyspark(monkeypatch)
    functions = stub.sql.functions

    class DataFrameReader:
        def table(self, tableName):
            return ("read.table", tableName)

    def table(self, tableName):
        self.executed.append(("table", tableName))
        return stub.sql.DataFrame()

    def reflect(*cols):
        return ("reflect", cols)

    def java_method(*cols):
        return ("java_method", cols)

    def try_reflect(*cols):
        return ("try_reflect", cols)

    def call_function(funcName, *cols):
        return ("call_function", funcName, cols)

    def lit(value):
        return ("lit", value)

    stub.sql.DataFrameReader = DataFrameReader
    stub.sql.SparkSession.table = table
    stub.sql.SparkSession.read = property(lambda self: DataFrameReader())
    functions.reflect, functions.java_method, functions.try_reflect = reflect, java_method, try_reflect
    functions.call_function, functions.lit = call_function, lit
    return stub


def test_runtime_guard_refuses_dataframe_api_java_calls_and_path_table_reads(sandbox_env, monkeypatch):
    from aidp_compat.notebook import SqlRuntimeGuard
    stub = _stub_pyspark_r2(monkeypatch)
    functions, Reader = stub.sql.functions, stub.sql.DataFrameReader
    originals = (functions.reflect, functions.java_method, functions.try_reflect, functions.call_function,
                 Reader.table, stub.sql.SparkSession.table)
    other = stub.sql.SparkSession()        # a session made elsewhere (getOrCreate())
    ns = {"reflect": functions.reflect, "call_function": functions.call_function, "F": functions}
    with SqlRuntimeGuard(npol.require_policy(), ns, notebook_path="nb") as guard:
        assert {"pyspark.sql.functions.reflect", "pyspark.sql.functions.java_method",
                "pyspark.sql.functions.try_reflect", "pyspark.sql.functions.call_function",
                "pyspark.sql.DataFrameReader.table", "<namespace>.reflect", "<namespace>.call_function"} \
            <= set(guard.patched)
        for call in (lambda: functions.reflect(functions.lit("java.lang.Runtime"), functions.lit("getRuntime")),
                     lambda: functions.java_method(functions.lit("a"), functions.lit("b")),
                     lambda: functions.try_reflect(functions.lit("a"), functions.lit("b")),
                     lambda: functions.call_function("reflect", functions.lit("a")),
                     lambda: functions.call_function("`JAVA_METHOD`", functions.lit("a")),
                     lambda: functions.call_function(funcName="try_reflect"),
                     lambda: ns["reflect"](functions.lit("a")),
                     lambda: ns["call_function"]("reflect", functions.lit("a")),
                     lambda: stub.sql.DataFrame().selectExpr("`reflect`('a', 'b')"),
                     lambda: stub.sql.DataFrame().where("`JAVA_METHOD`('a', 'b') = 1")):
            with pytest.raises(PolicyViolation) as ei:
                call()
            assert ei.value.rule_id == "NBP-SQL-REFUSED" and ei.value.notebook_path == "nb"
            assert ei.value.target in ("reflect(", "java_method(", "try_reflect(")
        with pytest.raises(PolicyViolation) as ei:
            functions.call_function(None, functions.lit("a"))
        assert ei.value.target == "<non-string>"
        with pytest.raises(PolicyViolation) as ei:
            other.read.table("text.`/etc/passwd`")
        assert ei.value.rule_id == "NBP-OPEN-PATH" and ei.value.target == "/etc/passwd"
        with pytest.raises(PolicyViolation):
            other.table("`delta`.`/dbfs/mnt/prod/x`")
        with pytest.raises(PolicyViolation):
            other.sql("SELECT * FROM {t}", t="text.`/etc/passwd`")
        assert functions.call_function("upper", functions.lit("a"))[1] == "upper"
        assert other.read.table("prod.t") == ("read.table", "prod.t")
        other.table("parquet.`oci://b@ns/prod/in`")
        other.sql("SELECT * FROM {t}", t="prod.t")
    assert (functions.reflect, functions.java_method, functions.try_reflect, functions.call_function,
            Reader.table, stub.sql.SparkSession.table) == originals
    assert ns["reflect"] is functions.reflect and ns["call_function"] is functions.call_function
    assert other.executed == [("table", "parquet.`oci://b@ns/prod/in`"), "SELECT * FROM {t}"]
    assert [e["rule"] for e in npol.get_policy_log() if e["event"] == "runtime_refused"] == \
        ["NBP-SQL-REFUSED"] * 11 + ["NBP-OPEN-PATH"] * 3
    functions.reflect(functions.lit("a"))                              # untouched after the block


def test_runtime_guard_function_patches_are_no_ops_without_the_functions(sandbox_env, monkeypatch):
    from aidp_compat.notebook import SqlRuntimeGuard
    stub = _stub_pyspark(monkeypatch)              # the minimal stub: no reflect / call_function / DataFrameReader
    with SqlRuntimeGuard(npol.require_policy(), {}) as guard:
        assert not [p for p in guard.patched if "reflect" in p or "call_function" in p or "table" in p]
        assert "pyspark.sql.functions.expr" in guard.patched
    assert stub.sql.functions.expr.__name__ == "expr"


# -- %sql cells for the round-2 classes
SQL_MAGIC_CELLS_R2 = [
    ("%sql SELECT `reflect`('java.lang.Runtime', 'getRuntime')", "NBP-SQL-REFUSED", "reflect("),
    ("%sql\nSELECT * FROM `text`.`/etc/passwd`", "NBP-OPEN-PATH", "/etc/passwd"),
    ("%sql SELECT * FROM sbx.t WHERE a = r'\\' AND b = reflect('a', 'b')", "NBP-SQL-REFUSED", "reflect("),
    ("%sql ALTER TABLE sbx.t RENAME TO prod.t", "NBP-SQL-TARGET", "prod.t"),
    ("%sql CREATE TEMP VIEW v USING jdbc OPTIONS (url 'jdbc:x')", "NBP-IMPORT-NETWORK", "jdbc"),
    ("%sql SELECT * FROM IDENTIFIER('text.`/etc/passwd`')", "NBP-SQL-DYNAMIC", "IDENTIFIER("),
]


@pytest.mark.parametrize("magic,rule,target", SQL_MAGIC_CELLS_R2,
                         ids=[c[1] + ":" + c[0][:30] for c in SQL_MAGIC_CELLS_R2])
def test_sql_magic_round2_cells_are_classified_after_transformation(sandbox_env, magic, rule, target):
    from aidp_compat.sql_magic import transform_sql_cells
    nb = {"cells": [{"cell_type": "code", "source": [magic]}]}
    src = "".join(transform_sql_cells(nb)["cells"][0]["source"])
    assert src.startswith('spark.sql("""')
    assert [(v.rule, v.target) for v in npol.scan_source(src)] == [(rule, target)]


def test_sql_magic_round2_legitimate_cells_pass(sandbox_env):
    from aidp_compat.sql_magic import transform_sql_cells
    nb = {"cells": [{"cell_type": "code", "source": ["%sql SELECT trim(LEADING '/' FROM '/tmp/x') AS p"]},
                    {"cell_type": "code", "source": ["%sql\n(SELECT 1) UNION ALL (SELECT 2)"]},
                    {"cell_type": "code", "source": ["%sql ALTER TABLE sbx.t RENAME TO sbx.t2"]}]}
    for cell in transform_sql_cells(nb)["cells"]:
        assert npol.scan_source("".join(cell["source"])) == []


# ── source hygiene of the gate modules ──────────────────────────────────
# A regression review found three `\`-newline continuations collapsed into a
# run of spaces and a lost space in a module-level assignment (both accepted by
# the parser, so no test caught them). No linter is configured for the plugin;
# this keeps the two shapes out of the gate modules.
def test_gate_module_sources_have_no_collapsed_continuations_or_lost_spaces():
    import re
    from aidp_compat import notebook as nbmod
    collapsed = re.compile(r"\S {8,}(and|or|not|in|if|else) ")
    lost_space = re.compile(r"^[A-Za-z_][A-Za-z0-9_]* =\S")
    for mod in (npol, nbmod):
        with open(mod.__file__, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        bad = [(n, line) for n, line in enumerate(lines, 1)
               if not line.lstrip().startswith("#") and (collapsed.search(line) or lost_space.match(line))]
        assert bad == [], f"{os.path.basename(mod.__file__)}: collapsed continuation / lost space at {bad}"
