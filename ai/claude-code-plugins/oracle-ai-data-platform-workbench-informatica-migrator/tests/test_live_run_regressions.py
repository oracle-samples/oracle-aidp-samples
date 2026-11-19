"""Regressions from the first deploy to a real AIDP workspace (2026-09-24).

Upload, job creation and the job run worked on the first try. What did not
was everything that only shows up against a live service or a fresh
workspace: a target table that does not exist yet, a job that already
does, a workspace path the shell rewrote, a Spark 3.5 function signature,
and a Joiner whose master side is spelled the way real exports spell it.
Each test here reproduces one of those against fakes; the CHANGELOG entry
"Live run (fourth pass)" records what was observed on the cluster.
"""
from __future__ import annotations

import json
import os
import sys
import types

import pytest

from infa2aidp.converters.expression_converter import ExpressionConverter
from infa2aidp.deployer.aidp_client import AIDPClient
from infa2aidp.deployer.deployer import AIDPDeployer
from infa2aidp.deployer.models import DeployConfig
from infa2aidp.migrator import run_migration
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

ROOT = os.path.dirname(os.path.dirname(__file__))
JOINER_NO_MASTER = os.path.join(ROOT, "tests", "fixtures", "powercenter", "joiner_no_master_flag.xml")
JOINER_MASTER = os.path.join(ROOT, "tests", "fixtures", "powercenter", "joiner_master_porttype.xml")
MULTI_STAGE = os.path.join(ROOT, "tests", "fixtures", "powercenter", "multi_stage_invoice_dw.xml")


# ---------------------------------------------------------------------------
# 1. DeployConfig refuses a workspace path outside /Workspace
# ---------------------------------------------------------------------------

class TestWorkspacePathValidation:
    def test_default_and_nested_paths_are_accepted_and_normalised(self):
        assert DeployConfig().workspace_path == "/Workspace/Migrated"
        assert DeployConfig(workspace_path="/Workspace/Team/A/").workspace_path == "/Workspace/Team/A"
        # Git Bash leaves a leading double slash alone; Windows callers may
        # type backslashes. Both normalise to the canonical spelling.
        assert DeployConfig(workspace_path="//Workspace/X").workspace_path == "/Workspace/X"
        assert DeployConfig(workspace_path="\\Workspace\\X").workspace_path == "/Workspace/X"
        assert DeployConfig(workspace_path="/Workspace").workspace_path == "/Workspace"

    def test_git_bash_rewrite_is_refused_with_a_hint(self):
        with pytest.raises(ValueError, match="MSYS_NO_PATHCONV"):
            DeployConfig(workspace_path="C:/Program Files/Git/Workspace/Migrated")

    def test_other_roots_are_refused(self):
        with pytest.raises(ValueError, match="/Workspace"):
            DeployConfig(workspace_path="/CustomWorkspace")
        with pytest.raises(ValueError):
            DeployConfig(workspace_path="/Workspaces/Migrated")  # a prefix is not a folder


# ---------------------------------------------------------------------------
# 2. Job lifecycle: look up, update in place, or skip -- never a duplicate POST
# ---------------------------------------------------------------------------

class _Client:
    """Records every call. ``jobs`` is the set of job names that already
    exist in the workspace."""

    def __init__(self, jobs=()):
        self.workspace_key = "ws-1"
        self.jobs = set(jobs)
        self.calls: list[tuple] = []

    def mkdir(self, path, workspace_key=None):
        self.calls.append(("mkdir", path))

    def object_exists(self, path, workspace_key=None):
        return False

    def upload_file(self, workspace_key, local_path, remote_path, overwrite=True):
        self.calls.append(("upload", remote_path))

    def find_job_by_name(self, name, workspace_key=None):
        self.calls.append(("find_job", name))
        return {"key": f"key-{name}", "name": name} if name in self.jobs else None

    def create_job(self, workspace_key, job_config):
        self.calls.append(("create_job", job_config["name"]))
        return {"key": "new-key"}

    def update_job(self, workspace_key, job_key, job_config):
        self.calls.append(("update_job", job_key, json.loads(json.dumps(job_config))))
        return {"key": job_key}


@pytest.fixture(scope="module")
def generic_out(tmp_path_factory):
    out = tmp_path_factory.mktemp("generic_etl")
    run_migration([MULTI_STAGE], str(out), use_llm=False,
                  skip_lineage=True, skip_optimize=True, score_confidence=False)
    return str(out)


def _deploy(generic_out, client, **cfg):
    config = DeployConfig(region="r", instance_id="i", workspace_key="ws-1",
                          cluster_key="c-1", **cfg)
    return AIDPDeployer(config, client=client).deploy(
        generic_out, os.path.join(generic_out, "workflows"))


class TestJobLifecycle:
    def test_new_job_is_created(self, generic_out):
        client = _Client()
        result = _deploy(generic_out, client)
        assert [wf.status for wf in result.workflows] == ["created"]
        assert ("create_job", "wf_finance_analytics") in client.calls
        assert not [c for c in client.calls if c[0] == "update_job"]

    def test_existing_job_without_overwrite_is_skipped_not_reposted(self, generic_out):
        client = _Client(jobs={"wf_finance_analytics"})
        result = _deploy(generic_out, client)
        wf = result.workflows[0]
        assert wf.status == "skipped"
        assert wf.job_id == "key-wf_finance_analytics"
        assert not [c for c in client.calls if c[0] in ("create_job", "update_job")]
        assert result.total_skipped == 1 and result.total_failed == 0

    def test_existing_job_with_overwrite_is_updated_in_place(self, generic_out):
        client = _Client(jobs={"wf_finance_analytics"})
        result = _deploy(generic_out, client, overwrite=True)
        wf = result.workflows[0]
        assert wf.status == "updated"
        updates = [c for c in client.calls if c[0] == "update_job"]
        assert len(updates) == 1 and updates[0][1] == "key-wf_finance_analytics"
        body = updates[0][2]
        # The PUT body is the same shape as the POST body, re-pointed at the
        # uploaded notebook and carrying the deployment cluster.
        assert body["tasks"][0]["notebookPath"].startswith("/Workspace/Migrated/FINANCE_DW/")
        assert body["jobClusters"] == [{"clusterKey": "c-1"}]
        assert result.total_updated == 1
        assert not [c for c in client.calls if c[0] == "create_job"]

    def test_dry_run_counts_would_deploy_not_skipped(self, generic_out):
        result = AIDPDeployer(DeployConfig(dry_run=True)).deploy(
            generic_out, os.path.join(generic_out, "workflows"))
        assert result.total_skipped == 0
        assert result.total_dry_run == len(result.notebooks) + len(result.workflows) == 4
        report = os.path.join(generic_out, "dry.md")
        AIDPDeployer(DeployConfig(dry_run=True)).generate_deploy_report(result, report)
        text = open(report, encoding="utf-8").read()
        assert "Would deploy (dry run): 4" in text
        assert "Skipped (already present, no --overwrite): 0" in text


# ---------------------------------------------------------------------------
# 3. The client follows the job list's pages
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, items, next_page=None):
        self._items = items
        self.headers = {"opc-next-page": next_page} if next_page else {}
        self.ok = True

    def json(self):
        return {"items": self._items}

    def raise_for_status(self):
        pass


def test_list_jobs_follows_opc_next_page_and_find_job_sees_page_two():
    client = AIDPClient.__new__(AIDPClient)
    client.workspace_key = "ws-1"
    seen = []

    def fake_request(method, path, **kw):
        seen.append((method, path, kw.get("params")))
        if kw["params"].get("page") is None:
            return _Resp([{"name": "a", "key": "1"}, {"name": "b", "key": "2"}], next_page="TOKEN")
        return _Resp([{"name": "wf_on_page_two", "key": "3"}])

    client._request = fake_request
    jobs = client.list_jobs()
    assert [j["name"] for j in jobs] == ["a", "b", "wf_on_page_two"]
    assert seen[0][2] == {"limit": 100}
    assert seen[1][2] == {"limit": 100, "page": "TOKEN"}
    assert client.find_job_by_name("wf_on_page_two")["key"] == "3"
    assert client.find_job_by_name("nope") is None


# ---------------------------------------------------------------------------
# 4. Generated notebook survives a first run against a missing target
# ---------------------------------------------------------------------------

def _cells(out_dir, notebook=None):
    """Code cells of a generated notebook under ``out_dir``.

    Discovered rather than hardcoded: these tests run over more than one
    fixture now, and a hardcoded folder/notebook name silently became a
    FileNotFoundError the moment a second one was added.
    """
    import glob
    pattern = os.path.join(out_dir, "**", notebook or "*.ipynb")
    paths = sorted(glob.glob(pattern, recursive=True))
    assert paths, f"no notebook generated under {out_dir}"
    cells = []
    for path in paths:
        nb = json.load(open(path, encoding="utf-8"))
        cells += ["".join(c["source"]) for c in nb["cells"]
                  if c["cell_type"] == "code"]
    return cells


class _Catalog:
    def __init__(self, existing=()):
        self.existing = set(existing)

    def tableExists(self, name):
        return name in self.existing


class _Spark:
    """Just enough of SparkSession for the catalog-type assertion cell:
    DESCRIBE DETAIL raises on a table that does not exist, as it does on
    the cluster."""

    def __init__(self, existing=()):
        self.catalog = _Catalog(existing)
        self.described = []

    def sql(self, stmt):
        self.described.append(stmt)
        raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND]")


class TestFirstRunCells:
    def test_catalog_assertion_skips_a_missing_target_instead_of_raising(self, generic_out):
        cell = next(c for c in _cells(generic_out, "nb_m_stage_invoice_txn.ipynb")
                    if "_assumption_targets" in c)
        assert "spark.catalog.tableExists(_tbl)" in cell
        warnings = []
        ns = {"spark": _Spark(), "logger": types.SimpleNamespace(warning=warnings.append)}
        exec(compile(cell, "catalog_cell", "exec"), ns)  # must not raise
        assert ns["spark"].described == []            # DESCRIBE DETAIL never issued
        assert warnings and "does not exist yet" in warnings[0]

    def test_catalog_assertion_still_fails_on_a_non_delta_existing_target(self, generic_out):
        cell = next(c for c in _cells(generic_out, "nb_m_stage_invoice_txn.ipynb")
                    if "_assumption_targets" in c)
        spark = _Spark(existing={"STGFIN.STG.stg_invoice_txn"})   # exists, DESCRIBE DETAIL raises -> not delta
        with pytest.raises(RuntimeError, match="not a Delta table"):
            exec(compile(cell, "catalog_cell", "exec"), {"spark": spark, "logger": types.SimpleNamespace(warning=lambda *_: None)})

    def test_write_cell_creates_the_target_before_merging(self, generic_out):
        tbl = "STGFIN.STG.stg_invoice_txn"
        cell = next(c for c in _cells(generic_out, "nb_m_stage_invoice_txn.ipynb")
                    if "Write to target" in c and tbl in c)
        assert f'spark.catalog.tableExists("{tbl}")' in cell
        create = cell.index(f'.limit(0).write.format("delta").saveAsTable("{tbl}")')
        merge = cell.index(f'DeltaTable.forName(spark, "{tbl}")')
        assert create < merge, "the create-if-missing branch must precede the MERGE"
        assert "REVIEW" in cell

    def test_joiner_with_porttype_master_is_joined_not_skipped(self, tmp_path):
        """A master side declared via PORTTYPE must resolve, so the join is
        applied rather than skipped with a review comment."""
        run_migration([JOINER_MASTER], str(tmp_path), use_llm=False,
                      skip_lineage=True, skip_optimize=True,
                      score_confidence=False)
        cells = _cells(str(tmp_path))
        joiner = next(c for c in cells if "Joiner: JNR_ship_order" in c)
        assert ".join(" in joiner
        assert "join skipped" not in joiner
        # the master-side column survives into the write
        assert any("ORDER_STATUS" in c for c in cells)


def test_parser_reads_master_from_porttype():
    result = InformaticaXMLParser().parse(JOINER_MASTER)
    mapping = result.mappings[0]
    joiner = next(t for t in mapping.transformations if t.name == "JNR_ship_order")
    masters = sorted(f.name for f in joiner.fields if f.is_master)
    assert masters == ["ORDER_ID_M", "ORDER_STATUS"]


# ---------------------------------------------------------------------------
# 5. infa_compat.write_update_strategy on a fresh workspace
# ---------------------------------------------------------------------------

class _Chain:
    """Any attribute access or call returns a chainable object; records
    the names touched so a test can assert what was invoked."""

    def __init__(self, log, prefix=""):
        self._log, self._prefix = log, prefix

    def __getattr__(self, name):
        return _Chain(self._log, f"{self._prefix}.{name}")

    def __call__(self, *a, **k):
        self._log.append((self._prefix, a))
        return self


def test_write_delta_creates_a_missing_target_before_the_merges(monkeypatch):
    from infa_compat import update_strategy as us

    log: list = []
    fake_delta = types.ModuleType("delta")
    fake_tables = types.ModuleType("delta.tables")
    fake_tables.DeltaTable = _Chain(log, "DeltaTable")
    fake_delta.tables = fake_tables
    monkeypatch.setitem(sys.modules, "delta", fake_delta)
    monkeypatch.setitem(sys.modules, "delta.tables", fake_tables)

    partitions = us.UpdateStrategyResult(
        inserts=_Chain(log, "inserts"), updates=_Chain(log, "updates"),
        deletes=_Chain(log, "deletes"), rejects=_Chain(log, "rejects"))
    spark = types.SimpleNamespace(catalog=_Catalog())          # target missing
    us._write_delta(partitions, "cat.sch.tgt", ["ID"], spark)

    names = [n for n, _ in log]
    create = names.index("inserts.limit.write.format.saveAsTable")
    first_merge = next(i for i, n in enumerate(names) if n.startswith("DeltaTable.forName"))
    assert create < first_merge
    assert log[create][1] == ("cat.sch.tgt",)

    # An existing target is left alone.
    log.clear()
    us._write_delta(partitions, "cat.sch.tgt", ["ID"], types.SimpleNamespace(catalog=_Catalog({"cat.sch.tgt"})))
    assert "inserts.limit.write.format.saveAsTable" not in [n for n, _ in log]


# ---------------------------------------------------------------------------
# 6. INDEXOF runs on Spark 3.5, not just Spark 4
# ---------------------------------------------------------------------------

def test_indexof_is_a_case_chain_not_array_position():
    code = ExpressionConverter().convert("INDEXOF(S, 'a', 'b')")
    assert "array_position" not in code
    assert code.count(".when(") == 3 and ".otherwise(F.lit(0))" in code
    assert "F.lit(1)" in code and "F.lit(2)" in code


def test_indexof_returns_null_for_a_null_search_value():
    pytest.importorskip("pyspark")
    from tests.conftest_spark import run_expr, spark as _spark_fixture  # noqa: F401
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("indexof-null").getOrCreate()
    out = run_expr(spark, "INDEXOF(S, 'a', 'b')", [("b",), (None,), ("q",)], "S string")
    assert out == [2, None, 0], out
