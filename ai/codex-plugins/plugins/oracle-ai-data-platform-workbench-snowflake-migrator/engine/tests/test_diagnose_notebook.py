"""The environment-diagnosis notebook, generated like the stages, run offline.

README step 8 tells the operator to open it from `scripts/` and run it before
the jobs. It used to be hand-maintained, never uploaded, importing a module
that is inlined elsewhere and no longer on the mount, reading a file nothing
creates -- and, on the documented nested config, printing the whole
`snowflake:` block into cell output, password and PEM included, under a header
promising "never the secret".

These tests build the notebook the way `provision` does and execute every
code cell with Spark and the network stubbed out, so the verdicts -- and what
they never print -- are pinned without a cluster.
"""
import json
import pathlib
import socket
import sys
import types

import pytest

from target.stage_notebooks import (
    DIAGNOSE_NOTEBOOK_NAME, build_diagnose_notebook, write_stage_notebooks)

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMMITTED = ROOT / "data-migration-scripts" / DIAGNOSE_NOTEBOOK_NAME

SECRET = "S3CRET-PASSWORD-VALUE"
PEM_BODY = "FAKEKEYBYTES"
PEM = f"-----BEGIN PRIVATE KEY-----\n{PEM_BODY}\n-----END PRIVATE KEY-----"
# The documented shape: the one migration config, connection under
# `snowflake:`, AIDP coordinates under `aidp:`. `provision` uploads its
# `snowflake:` block as JSON; the notebook reads either form.
NESTED = {"snowflake": {"account": "ACC", "user": "u", "warehouse": "WH",
                        "database": "DB", "schema": "S1", "auth": "keypair",
                        "password": SECRET, "private_key": PEM},
          "aidp": {"datalake_ocid": "ocid1.aidataplatform.oc1..fake"}}
NESTED_YAML = ("snowflake:\n  account: ACC\n  user: u\n  warehouse: WH\n"
               "  database: DB\n  schema: S1\n  auth: password\n"
               f"  password: {SECRET}\n  private_key: |\n"
               "    -----BEGIN PRIVATE KEY-----\n"
               f"    {PEM_BODY}\n    -----END PRIVATE KEY-----\n"
               "aidp:\n  datalake_ocid: ocid1.aidataplatform.oc1..fake\n")


class _StubSpark:
    """No cluster: every read and every SQL raises where the real one would
    talk to Snowflake, so the connector cell reaches its except branch."""

    @property
    def read(self):
        return self

    def format(self, *_):
        return self

    def options(self, **_):
        return self

    def option(self, *_):
        return self

    def load(self):
        raise RuntimeError("stub: no cluster")

    def sql(self, *_):
        raise RuntimeError("stub: no cluster")


def _stub_pyspark(monkeypatch, spark):
    sql_mod = types.ModuleType("pyspark.sql")

    class SparkSession:
        class builder:
            @staticmethod
            def getOrCreate():
                return spark

    sql_mod.SparkSession = SparkSession
    pkg = types.ModuleType("pyspark")
    pkg.sql = sql_mod
    monkeypatch.setitem(sys.modules, "pyspark", pkg)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql_mod)


def _no_network(*_, **__):
    raise OSError("stub: no network")


def _run(nb, monkeypatch, capsys, config_path=None) -> tuple[str, dict]:
    """Execute every code cell in one namespace; return (stdout, namespace).

    `config_path`, when given, is forced in right after the cell that sets
    `CONFIG_PATH` -- how the committed notebook is pointed at a test file.
    """
    spark = _StubSpark()
    _stub_pyspark(monkeypatch, spark)
    monkeypatch.setattr(socket, "create_connection", _no_network)
    ns = {"__name__": "snowmig_diagnose", "spark": spark}
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        compile(src, "<cell>", "exec")
        exec(src, ns)
        if config_path is not None and "CONFIG_PATH =" in src:
            ns["CONFIG_PATH"] = str(config_path)
    return capsys.readouterr().out, ns


def _cluster_verdicts(out: str) -> list[str]:
    return [line for line in out.splitlines()
            if line.startswith("[PASS] cluster") or line.startswith("[FAIL] cluster")]


@pytest.mark.parametrize("name,text", [
    ("snowmig-config.yaml", NESTED_YAML),
    ("snowmig-config.json", json.dumps(NESTED)),
    ("flat.json", json.dumps(NESTED["snowflake"])),
], ids=["nested-yaml", "nested-json", "flat-json"])
def test_the_notebook_never_echoes_the_secret(tmp_path, monkeypatch, capsys,
                                              name, text):
    cfg = tmp_path / name
    cfg.write_text(text, encoding="utf-8")
    nb = build_diagnose_notebook(overrides={"source-config": str(cfg)})
    out, ns = _run(nb, monkeypatch, capsys)
    assert SECRET not in out and PEM_BODY not in out, out
    assert "BEGIN PRIVATE KEY" not in out
    assert "[PASS] source config readable" in out
    # The shape IS shown: the operator sees which fields the file carries.
    assert '"account": "<set>"' in out and '"password": "<set>"' in out
    assert "ACC" not in out.split("host to be used")[0], \
        "the echo shows key names only, never a value"
    # What the connector cell hands to SnowflakeSource is the unwrapped block.
    assert ns["config"]["account"] == "ACC" and "aidp" not in ns["config"]
    assert len(_cluster_verdicts(out)) == 1, out
    assert "[FAIL] connector pushdown" in out
    assert "ModuleNotFoundError" not in out


def test_an_unreadable_config_still_yields_every_verdict(tmp_path, monkeypatch,
                                                         capsys):
    nb = build_diagnose_notebook(
        overrides={"source-config": str(tmp_path / "plan" / "nope.yaml")})
    out, _ = _run(nb, monkeypatch, capsys)
    assert "[FAIL] source config readable" in out
    cluster = _cluster_verdicts(out)
    assert len(cluster) == 1 and "host unknown" in cluster[0], out
    # The run still reaches the connector cell and reports there too.
    assert "[FAIL] connector pushdown" in out
    assert "Traceback" not in out


def test_a_malformed_config_is_not_quoted_back(tmp_path, monkeypatch, capsys):
    """The inlined loader hands a YAML parse error straight up, and PyYAML's
    message quotes the offending line -- here, the password line."""
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text("snowflake:\n  account: ACC\n  auth: password\n"
                   f"  password: {{{SECRET}\naidp:\n  workspace: ws\n",
                   encoding="utf-8")
    nb = build_diagnose_notebook(overrides={"source-config": str(cfg)})
    out, _ = _run(nb, monkeypatch, capsys)
    assert "[FAIL] source config readable" in out
    assert SECRET not in out, out
    assert "withheld" in out


def test_the_notebook_is_self_contained_and_generated():
    nb = build_diagnose_notebook()
    body = json.dumps(nb)
    assert "from snowmig_source import" not in body
    assert "sys.path.insert" not in body
    assert "def load_source_config" in body, "the helpers are inlined"
    assert "def SnowflakeSource" not in body and "class SnowflakeSource" in body
    assert nb["metadata"]["snowmig"]["generated"] is True
    assert nb["nbformat"] == 4
    header = "".join(nb["cells"][0]["source"])
    assert nb["cells"][0]["cell_type"] == "markdown"
    assert "build-notebooks" in header, "say how it is regenerated"
    assert "never a value" in header
    params = "".join(nb["cells"][1]["source"])
    config_line = next(line for line in params.splitlines()
                       if line.startswith("CONFIG_PATH = "))
    # provision uploads the `snowflake:` block as <stem>.json under plan/,
    # and the default points there -- not at a file nothing creates.
    assert "/Workspace/backup-snowflake-migration/plan/snowmig-config.json" \
        in config_line
    assert "snowmig-config.yaml" not in config_line
    assert "snowmig_source_config.json" not in body, \
        "nothing creates that file; provision writes <stem>.json under plan/"
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            compile("".join(cell["source"]), "<cell>", "exec")
    # The reading guide survives as the closing markdown cell.
    assert nb["cells"][-1]["cell_type"] == "markdown"
    assert "Reading the result" in "".join(nb["cells"][-1]["source"])


def test_provision_s_overrides_land_in_the_parameters_cell():
    nb = build_diagnose_notebook(overrides={
        "source-config": "/Workspace/backup-snowflake-migration/plan/my.yaml",
        "source-catalog": "ext", "session-schema": "S1",
        "target-catalog": "not-a-diagnose-parameter", "reports-dir": None})
    params = "".join(nb["cells"][1]["source"])
    assert "CONFIG_PATH = '/Workspace/backup-snowflake-migration/plan/my.yaml'" \
           in params
    assert "EXTERNAL_CATALOG = 'ext'" in params
    assert "SESSION_SCHEMA = 'S1'" in params
    assert "not-a-diagnose-parameter" not in params


def test_write_stage_notebooks_writes_the_diagnosis_too(tmp_path):
    written = {p.name: p for p in write_stage_notebooks(tmp_path)}
    assert DIAGNOSE_NOTEBOOK_NAME in written
    raw = written[DIAGNOSE_NOTEBOOK_NAME].read_bytes()
    assert b"\r\n" not in raw, "generated notebooks are LF on every platform"
    assert json.loads(raw.decode("utf-8"))["metadata"]["snowmig"]["stage"] \
        == "diagnose"


def test_the_committed_notebook_is_the_generated_one():
    """It ships committed, like the stage notebooks, and is regenerated rather
    than edited: `snowmig.py build-notebooks` must reproduce it exactly."""
    raw = COMMITTED.read_bytes()
    assert b"\r\n" not in raw
    assert json.loads(raw.decode("utf-8")) == build_diagnose_notebook()


def test_the_committed_notebook_never_echoes_the_secret(tmp_path, monkeypatch,
                                                        capsys):
    """Executes the file as shipped, pointed at a nested JSON config -- the
    input README steers an operator without PyYAML to."""
    cfg = tmp_path / "snowmig-config.json"
    cfg.write_text(json.dumps(NESTED), encoding="utf-8")
    nb = json.loads(COMMITTED.read_text(encoding="utf-8"))
    out, _ = _run(nb, monkeypatch, capsys, config_path=cfg)
    assert SECRET not in out and PEM_BODY not in out, out
    assert "[PASS] source config readable" in out
    assert len(_cluster_verdicts(out)) == 1, out
    assert "ModuleNotFoundError" not in out
