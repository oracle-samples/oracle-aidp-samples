"""The deploy path, end to end against a recording fake of ``AIDPClient``.

Before this file existed the non-dry-run deployer could not even be
constructed (``AIDPClient(config.host, config.token)`` against a
``(region, instance_id, signer, ...)`` constructor), called four client
methods that did not exist, and globbed ``*.py`` while ``migrate`` writes
``*.ipynb`` -- so ``--dry-run`` reported zero notebooks and a real deploy
failed on its first request. Nothing in ``tests/`` exercised any of it.

These tests do not talk to AIDP. They pin the request shapes to those of
the AIDP MCP server this plugin sits beside: the three-step
``uploadFileMeta`` PAR upload, ``actions/mkdir``, and ``POST .../jobs`` with
``taskKey``/``NOTEBOOK_TASK``/``notebookPath``/``dependsOn``/``cluster``.
"""
from __future__ import annotations

import json
import os

import pytest

from infa2aidp.deployer.aidp_client import AIDPClient
from infa2aidp.deployer.deployer import AIDPDeployer
from infa2aidp.deployer.models import DeployConfig
from infa2aidp.migrator import run_migration

ROOT = os.path.dirname(os.path.dirname(__file__))
ORDERS = os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")
WORKFLOWS = os.path.join(ROOT, "tests", "fixtures", "orchestration", "workflow_orchestration.xml")


class FakeClient:
    """Records every call; pretends nothing exists yet."""

    def __init__(self, workspace_key="ws-1", existing=()):
        self.workspace_key = workspace_key
        self.calls: list[tuple] = []
        self.existing = set(existing)

    def mkdir(self, path, workspace_key=None):
        self.calls.append(("mkdir", path))

    def object_exists(self, path, workspace_key=None):
        self.calls.append(("exists", path))
        return path in self.existing

    def upload_file(self, workspace_key, local_path, remote_path, overwrite=True):
        self.calls.append(("upload", workspace_key, os.path.basename(local_path), remote_path, overwrite))

    def create_job(self, workspace_key, job_config):
        self.calls.append(("create_job", workspace_key, json.loads(json.dumps(job_config))))
        return {"key": "job-123", "name": job_config["name"]}

    def find_job_by_name(self, name, workspace_key=None):
        self.calls.append(("find_job", name))
        return {"key": f"existing-{name}", "name": name} if name in self.existing else None

    def update_job(self, workspace_key, job_key, job_config):
        self.calls.append(("update_job", workspace_key, job_key, json.loads(json.dumps(job_config))))
        return {"key": job_key, "name": job_config["name"]}


@pytest.fixture(scope="module")
def migrate_out(tmp_path_factory):
    out = tmp_path_factory.mktemp("deploy_in")
    run_migration([ORDERS, WORKFLOWS], str(out), use_llm=False,
                  skip_lineage=True, skip_optimize=True, score_confidence=False)
    return str(out)


def test_dry_run_finds_the_ipynb_notebooks_migrate_wrote(migrate_out):
    result = AIDPDeployer(DeployConfig(dry_run=True)).deploy(
        migrate_out, os.path.join(migrate_out, "workflows")
    )
    assert [nb.mapping_name for nb in result.notebooks] == ["nb_m_ORDERS_TRANSFORM"]
    assert result.notebooks[0].remote_path == "/Workspace/Migrated/SALES/nb_m_ORDERS_TRANSFORM.ipynb"
    assert result.notebooks[0].status == "dry_run"
    assert len(result.workflows) == 3
    assert {wf.status for wf in result.workflows} == {"dry_run"}


def test_non_dry_run_requires_a_workspace_key():
    with pytest.raises(ValueError, match="workspace key"):
        AIDPDeployer(DeployConfig(), client=FakeClient(workspace_key=""))


def test_live_deploy_uploads_via_the_client_and_creates_aidp_shaped_jobs(migrate_out):
    fake = FakeClient()
    deployer = AIDPDeployer(
        DeployConfig(region="us-ashburn-1", instance_id="ocid1.x", workspace_key="ws-1",
                     cluster_key="cluster-9", overwrite=True),
        client=fake,
    )
    result = deployer.deploy(migrate_out, os.path.join(migrate_out, "workflows"))

    assert result.total_uploaded == 1 and result.total_failed == 0, [
        (nb.status, nb.error) for nb in result.notebooks
    ] + [(wf.status, wf.error) for wf in result.workflows]
    uploads = [c for c in fake.calls if c[0] == "upload"]
    assert uploads == [(
        "upload", "ws-1", "nb_m_ORDERS_TRANSFORM.ipynb",
        "/Workspace/Migrated/SALES/nb_m_ORDERS_TRANSFORM.ipynb", True,
    )]
    assert ("mkdir", "/Workspace/Migrated") in fake.calls
    assert ("mkdir", "/Workspace/Migrated/SALES") in fake.calls

    jobs = [c[2] for c in fake.calls if c[0] == "create_job"]
    assert len(jobs) == 3
    by_name = {j["name"]: j for j in jobs}
    job = by_name["wf_nightly_sales"]
    assert job["path"] == "jobs"
    assert job["jobClusters"] == [{"clusterKey": "cluster-9"}]
    for task in job["tasks"]:
        assert task["type"] == "NOTEBOOK_TASK"
        assert task["cluster"] == {"clusterKey": "cluster-9"}
        assert set(task) >= {"taskKey", "notebookPath", "dependsOn", "runIf"}
        # never the Databricks 2.x vocabulary
        assert "task_key" not in task and "notebook_task" not in task
    assert job["schedule"] == {"quartzCronExpression": "0 30 3 * * ?", "timezoneId": "UTC",
                               "pauseStatus": "PAUSED"}
    assert all(wf.job_id == "job-123" and wf.status == "created" for wf in result.workflows)


def test_existing_notebook_is_skipped_without_overwrite(migrate_out):
    fake = FakeClient(existing={"/Workspace/Migrated/SALES/nb_m_ORDERS_TRANSFORM.ipynb"})
    deployer = AIDPDeployer(
        DeployConfig(region="r", instance_id="i", workspace_key="ws-1", cluster_key="c"),
        client=fake,
    )
    result = deployer.deploy(migrate_out)
    assert result.total_skipped == 1 and result.total_uploaded == 0
    assert not [c for c in fake.calls if c[0] == "upload"]


def test_job_creation_without_a_cluster_key_is_a_reported_failure(migrate_out):
    fake = FakeClient()
    deployer = AIDPDeployer(
        DeployConfig(region="r", instance_id="i", workspace_key="ws-1"), client=fake,
    )
    result = deployer.deploy(migrate_out, os.path.join(migrate_out, "workflows"))
    assert result.total_uploaded == 1
    assert {wf.status for wf in result.workflows} == {"failed"}
    assert all("cluster" in wf.error.lower() for wf in result.workflows)
    assert not [c for c in fake.calls if c[0] == "create_job"]


def test_workspace_relative_strips_the_workspace_root():
    assert AIDPClient.workspace_relative("/Workspace/Migrated/x.ipynb") == "Migrated/x.ipynb"
    assert AIDPClient.workspace_relative("Migrated/x.ipynb") == "Migrated/x.ipynb"
    assert AIDPClient.workspace_relative("/Workspace") == ""
    assert AIDPClient.workspace_relative("/Migrated") == "Migrated"


def test_deploy_report_names_the_infa_compat_cluster_library(migrate_out, tmp_path):
    deployer = AIDPDeployer(DeployConfig(dry_run=True))
    result = deployer.deploy(migrate_out)
    report = tmp_path / "deploy_report.md"
    deployer.generate_deploy_report(result, str(report))
    text = report.read_text(encoding="utf-8")
    assert "infa_compat" in text and "cluster library" in text
    assert "nb_m_ORDERS_TRANSFORM" in text


# ---------------------------------------------------------------------------
# The DEFAULT master-catalog cluster: found by a live run, not by these fakes
# ---------------------------------------------------------------------------

class ClusterAwareFake(FakeClient):
    """A FakeClient that answers ``list_clusters`` like the service does."""

    def __init__(self, clusters, **kw):
        super().__init__(**kw)
        self._clusters = clusters

    def list_clusters(self, workspace_key=None):
        self.calls.append(("list_clusters",))
        return self._clusters


def _deploy_with(fake, migrate_out, cluster_key="cluster-9"):
    deployer = AIDPDeployer(
        DeployConfig(region="us-ashburn-1", instance_id="ocid1.x",
                     workspace_key="ws-1", cluster_key=cluster_key, overwrite=True),
        client=fake,
    )
    return deployer.deploy(migrate_out, os.path.join(migrate_out, "workflows"))


def test_default_master_catalog_cluster_is_refused_before_the_job_is_created(migrate_out):
    """AIDP accepts the job definition and fails every *run*:

        WORKFLOW_EXECUTION_0071 - Default Cluster "Default Master Catalog
        Compute" used for non-system task s_m_EmployeeSummary.

    A deploy that creates a job which can never run is a failed deploy, so
    this is refused up front. The shape was valid, which is exactly why the
    recording fake above could not catch it -- only the service knows the
    cluster's type. Found by deploying to a live instance and running the job.
    """
    fake = ClusterAwareFake([
        {"key": "cluster-9", "displayName": "Default Master Catalog Compute",
         "type": "DEFAULT", "state": "ACTIVE"},
    ])
    with pytest.raises(ValueError, match="DEFAULT master-catalog"):
        _deploy_with(fake, migrate_out)
    assert not [c for c in fake.calls if c[0] == "create_job"], (
        "refused after creating the job -- the point is to refuse before"
    )


def test_a_user_cluster_is_accepted(migrate_out):
    fake = ClusterAwareFake([
        {"key": "cluster-9", "displayName": "InfaMigrator_Demo",
         "type": "USER", "state": "ACTIVE"},
    ])
    result = _deploy_with(fake, migrate_out)
    assert result.total_failed == 0
    assert len([c for c in fake.calls if c[0] == "create_job"]) == 3


def test_an_unlistable_cluster_only_warns(migrate_out, caplog):
    """A read failure is not evidence the cluster is wrong. Deploy proceeds."""
    class Broken(FakeClient):
        def list_clusters(self, workspace_key=None):
            raise RuntimeError("403 listing clusters")

    fake = Broken()
    result = _deploy_with(fake, migrate_out)
    assert result.total_failed == 0
    assert len([c for c in fake.calls if c[0] == "create_job"]) == 3, (
        "a cluster the deployer cannot read is not a cluster it knows is wrong"
    )


def test_the_cluster_is_checked_once_not_per_workflow(migrate_out):
    fake = ClusterAwareFake([
        {"key": "cluster-9", "displayName": "u", "type": "USER", "state": "ACTIVE"},
    ])
    _deploy_with(fake, migrate_out)
    assert len([c for c in fake.calls if c[0] == "list_clusters"]) == 1, (
        "three workflows must not mean three cluster lookups"
    )


# ---------------------------------------------------------------------------
# Re-deploying a workflow: AIDP refuses a duplicate job name
# ---------------------------------------------------------------------------

class JobAwareFake(ClusterAwareFake):
    """Answers ``find_job_by_name`` / ``update_job`` like the service does."""

    def __init__(self, existing_jobs=(), **kw):
        super().__init__([{"key": "cluster-9", "displayName": "u",
                           "type": "USER", "state": "ACTIVE"}], **kw)
        self._jobs = {j["name"]: j for j in existing_jobs}

    def find_job_by_name(self, name, workspace_key=None):
        self.calls.append(("find_job", name))
        return self._jobs.get(name)

    def update_job(self, workspace_key, job_key, job_config):
        self.calls.append(("update_job", job_key, job_config.get("name")))
        return {"key": job_key}


def test_redeploy_updates_the_existing_job_instead_of_failing(migrate_out):
    """AIDP rejects a create whose name exists:

        JOB_VALIDATE_0031 - Job with name wf_EmployeeSummary.job already
        exists in workspaceId ...

    Before this, every deploy after the first failed on job creation while
    reporting 12 successful notebook uploads. Re-deploying is the normal
    case during a migration. Found on a live instance.
    """
    fake = JobAwareFake(existing_jobs=[{"name": "wf_nightly_sales", "key": "job-old"}])
    result = _deploy_with(fake, migrate_out)

    updated = [c for c in fake.calls if c[0] == "update_job"]
    assert updated == [("update_job", "job-old", "wf_nightly_sales")]
    # the other two workflows did not exist, so they are created
    created = [c[2]["name"] for c in fake.calls if c[0] == "create_job"]
    assert "wf_nightly_sales" not in created
    statuses = {wf.name: wf.status for wf in result.workflows}
    assert statuses["wf_nightly_sales"] == "updated"
    assert result.total_failed == 0


def test_without_overwrite_an_existing_job_is_skipped_not_failed(migrate_out):
    fake = JobAwareFake(existing_jobs=[{"name": "wf_nightly_sales", "key": "job-old"}])
    deployer = AIDPDeployer(
        DeployConfig(region="r", instance_id="i", workspace_key="ws-1",
                     cluster_key="cluster-9", overwrite=False),
        client=fake,
    )
    result = deployer.deploy(migrate_out, os.path.join(migrate_out, "workflows"))
    assert not [c for c in fake.calls if c[0] == "update_job"]
    statuses = {wf.name: wf.status for wf in result.workflows}
    assert statuses["wf_nightly_sales"] == "skipped"
    assert result.total_failed == 0


def test_a_job_that_does_not_exist_is_still_created(migrate_out):
    fake = JobAwareFake(existing_jobs=())
    result = _deploy_with(fake, migrate_out)
    assert len([c for c in fake.calls if c[0] == "create_job"]) == 3
    assert not [c for c in fake.calls if c[0] == "update_job"]
    assert {wf.status for wf in result.workflows} == {"created"}


# ---------------------------------------------------------------------------
# The cluster must be new enough for the notebooks it is being given
# ---------------------------------------------------------------------------

class SparkVersionFake(ClusterAwareFake):
    """A USER cluster reporting a given Spark version."""

    def __init__(self, spark_version, **kw):
        super().__init__([{
            "key": "cluster-9", "displayName": "c", "type": "USER",
            "state": "ACTIVE",
            "clusterRuntimeConfig": {"sparkVersion": spark_version},
        }], **kw)


def test_a_cluster_older_than_the_notebooks_require_is_refused(migrate_out):
    """The notebooks declare _GENERATED_FOR_SPARK_MIN = 3.5.0, so a 3.4
    cluster cannot be trusted with them -- and it is refused before any
    upload, like the DEFAULT-cluster guard."""
    fake = SparkVersionFake("3.4.1")
    with pytest.raises(ValueError, match="at least 3.5.0"):
        _deploy_with(fake, migrate_out)
    assert not [c for c in fake.calls if c[0] == "upload"], (
        "refused after uploading -- the point is to refuse before"
    )


def test_the_same_version_is_accepted():
    """A floor, not a match."""
    from infa2aidp.spark_target import parse_spark_version
    assert parse_spark_version("3.5.0") == (3, 5, 0)


def test_a_newer_cluster_is_accepted(migrate_out):
    """This is why there is no per-version code generation: one notebook is
    valid on 3.5 and on 4.x, so it can move between clusters."""
    result = _deploy_with(SparkVersionFake("4.0.0"), migrate_out)
    assert result.total_failed == 0
    assert result.total_uploaded >= 1


def test_an_unparseable_cluster_version_only_warns(migrate_out):
    """Not evidence the cluster is too old."""
    result = _deploy_with(SparkVersionFake("custom-build"), migrate_out)
    assert result.total_failed == 0
    assert result.total_uploaded >= 1


def test_a_missing_cluster_version_only_warns(migrate_out):
    result = _deploy_with(SparkVersionFake(None), migrate_out)
    assert result.total_failed == 0


def test_the_notebooks_actually_declare_a_floor(migrate_out):
    """Proof the guard is not vacuous: if the generator stopped emitting the
    marker, every version check above would pass by finding nothing."""
    import glob
    import re as _re
    nbs = glob.glob(os.path.join(migrate_out, "**", "*.ipynb"), recursive=True)
    assert nbs
    # parse the notebook: in .ipynb JSON the quotes are escaped, so a regex
    # over the raw file text finds nothing -- the bug this test caught in
    # the deployer's own first implementation
    from infa2aidp.deployer.deployer import _notebook_code
    found = [n for n in nbs
             if _re.search(r'_GENERATED_FOR_SPARK_MIN\s*=\s*"[0-9.]+"',
                           _notebook_code(n))]
    assert found == nbs, "some notebooks declare no Spark floor"
