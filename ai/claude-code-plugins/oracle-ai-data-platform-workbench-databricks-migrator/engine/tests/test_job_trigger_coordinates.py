"""REF-208: the migrated cross-job trigger must find its AIDP endpoint
coordinates in the JOB's runtime, not only in the migration kernel.

Incident (2026-10-19, Example_Monitoring_Generic_Job run 6, driver job run
aaaaaaaa-0000-4000-8000-000000000103): every migrated notebook carrying the
trigger hard-read os.environ['AIDP_REGION'/'AIDP_LAKE_OCID'/'AIDP_WORKSPACE_ID'].
Those were seeded only by build_env_bootstrap_snippet() into the tool's own
WebSocket session; nothing put them into the artifact or the job cluster, so
replacing TARGET_JOB_KEY (the one edit the template asks for) produced
KeyError: 'AIDP_REGION' after the Credential Store fetches succeeded.

Contract pinned here, end to end:
  env AIDP sets on every cluster ($AIDP_ID, else $DATALAKE_ID; $AIDP_WORKSPACE_KEY
  — probed on a job cluster with nothing injected, 2026-10-22)
    -> job kernel os.environ
    -> the trigger template's own resolution block (region: $AIDP_REGION, else
       the lake OCID's region segment, else $OCI_RESOURCE_PRINCIPAL_REGION)
with the tool/operator vars ($AIDP_LAKE_OCID / $AIDP_WORKSPACE_ID / $AIDP_REGION)
taking precedence, no dependency on the installed aidp_compat wheel, and, where
nothing supplies a coordinate, a named error before any Credential Store read.
"""
from __future__ import annotations

import os
import sys
import textwrap
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(_REPO_ROOT), str(_REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import job_migrate as jm  # noqa: E402
from dbx_export.cluster_converter import (  # noqa: E402
    convert_dbx_cluster, recommended_env_vars,
)

LAKE = "ocid1.aidataplatform.oc1.ap-mumbai-1.markerlake208"
WS = "aaaaaaaa-0000-4000-8000-000000000208"
EXPECTED_URL = (f"https://aidp.ap-mumbai-1.oci.oraclecloud.com/20240831/"
                f"dataLakes/{LAKE}/workspaces/{WS}/jobRuns")
_COORD_ENV = ("AIDP_LAKE_OCID", "AIDP_WORKSPACE_ID", "AIDP_REGION")
_PLATFORM_ENV = ("AIDP_ID", "DATALAKE_ID", "AIDP_WORKSPACE_KEY", "OCI_RESOURCE_PRINCIPAL_REGION")


def _cfg(lake=LAKE, ws=WS):
    return SimpleNamespace(aidp=SimpleNamespace(lake_ocid=lake, workspace_id=ws),
                           spark_config={}, autoscale_config={})


def _trigger_template() -> str:
    """The exact shape CELL_MIGRATE_PROMPT tells the model to emit."""
    section = jm.CELL_MIGRATE_PROMPT.split(
        "DATABRICKS JOB TRIGGERS -> AIDP WORKFLOW TRIGGER", 1)[1]
    section = section.split("UNFIXABLE DATABRICKS INFRASTRUCTURE", 1)[0]
    shape = section[section.index("    # Original: jobrunapi.trigger_run"):]
    return textwrap.dedent(shape)


class _Runtime:
    """Fake AIDP job kernel: aidputils global + recording oci/requests."""

    def __init__(self, monkeypatch):
        self.posts = []
        self.secret_reads = 0
        runtime = self

        class _Resp:
            def raise_for_status(self):
                return None

            def json(self):
                return {"key": "run-208"}

        requests_mod = types.ModuleType("requests")

        def _post(url, auth=None, json=None):
            runtime.posts.append((url, json))
            return _Resp()

        requests_mod.post = _post
        oci_mod = types.ModuleType("oci")
        oci_mod.signer = SimpleNamespace(Signer=lambda **kw: ("signer", kw))
        monkeypatch.setitem(sys.modules, "requests", requests_mod)
        monkeypatch.setitem(sys.modules, "oci", oci_mod)

        def _secret(name, key=None):
            runtime.secret_reads += 1
            return "value"

        self.aidputils = SimpleNamespace(secrets=SimpleNamespace(get=_secret))

    def run(self, source):
        exec(compile(source, "<migrated-trigger>", "exec"),
             {"aidputils": self.aidputils, "__name__": "__main__"})


@pytest.fixture
def job_env(monkeypatch):
    for key in _COORD_ENV + _PLATFORM_ENV:
        monkeypatch.delenv(key, raising=False)
    for key in ("AIDP_CRED_OCI_NAME", "AIDP_CRED_OCI_KEY_USER",
                "AIDP_CRED_OCI_KEY_FINGERPRINT", "AIDP_CRED_OCI_KEY_TENANCY",
                "AIDP_CRED_OCI_KEY_PRIVATE_KEY_CONTENT"):
        monkeypatch.setenv(key, "ref_" + key.lower())
    return monkeypatch


def _armed(template):
    armed = template.replace('TARGET_JOB_KEY = "REPLACE_WITH_AIDP_WORKFLOW_KEY"',
                             'TARGET_JOB_KEY = "wf-key-208"', 1)
    assert armed != template
    return armed


# ── cluster env: only the workspace id is injected; AIDP supplies the lake ─

def test_recommended_env_vars_injects_workspace_but_not_lake():
    out = recommended_env_vars(_cfg())
    assert out["AIDP_WORKSPACE_ID"] == WS
    assert "AIDP_LAKE_OCID" not in out


def test_converted_cluster_payload_adds_no_lake_env():
    payload = convert_dbx_cluster({"node_type_id": "m5.xlarge"}, "t",
                                  prefix="ai_tool", cfg=_cfg()).aidp_payload
    env = payload["clusterRuntimeConfig"]["sparkEnvVariables"]
    assert env["AIDP_WORKSPACE_ID"] == WS
    assert "AIDP_LAKE_OCID" not in env


# ── trigger template: which env supplies each coordinate ───────────────────

OTHER_LAKE = "ocid1.aidataplatform.oc1.us-ashburn-1.otherlake208"
OTHER_WS = "22222222-3333-4444-8555-666666666666"
NO_REGION_LAKE = "ocid1.aidataplatform.oc1..noregionlake208"


def _url(region, lake, ws):
    return (f"https://aidp.{region}.oci.oraclecloud.com/20240831/"
            f"dataLakes/{lake}/workspaces/{ws}/jobRuns")


@pytest.mark.parametrize("env, want", [
    ({"AIDP_ID": LAKE, "AIDP_WORKSPACE_KEY": WS}, EXPECTED_URL),
    ({"DATALAKE_ID": LAKE, "AIDP_WORKSPACE_KEY": WS}, EXPECTED_URL),
    ({"AIDP_ID": LAKE, "DATALAKE_ID": OTHER_LAKE, "AIDP_WORKSPACE_KEY": WS}, EXPECTED_URL),
    ({"AIDP_LAKE_OCID": OTHER_LAKE, "AIDP_ID": LAKE, "AIDP_WORKSPACE_ID": OTHER_WS,
      "AIDP_WORKSPACE_KEY": WS}, _url("us-ashburn-1", OTHER_LAKE, OTHER_WS)),
    ({"AIDP_ID": LAKE, "AIDP_WORKSPACE_KEY": WS, "AIDP_REGION": "us-phoenix-1"},
     _url("us-phoenix-1", LAKE, WS)),
    ({"AIDP_ID": LAKE, "AIDP_WORKSPACE_KEY": WS, "OCI_RESOURCE_PRINCIPAL_REGION": "us-phoenix-1"},
     EXPECTED_URL),
    ({"AIDP_ID": NO_REGION_LAKE, "AIDP_WORKSPACE_KEY": WS,
      "OCI_RESOURCE_PRINCIPAL_REGION": "ap-mumbai-1"}, _url("ap-mumbai-1", NO_REGION_LAKE, WS)),
], ids=["aidp_id", "datalake_id_fallback", "aidp_id_before_datalake_id",
        "tool_env_before_platform_env", "aidp_region_override",
        "lake_region_before_platform_region", "platform_region_when_lake_has_none"])
def test_armed_trigger_env_precedence(job_env, env, want):
    for key, value in env.items():
        job_env.setenv(key, value)
    runtime = _Runtime(job_env)
    runtime.run(_armed(_trigger_template()))
    assert runtime.posts == [(want, {"jobKey": "wf-key-208"})]


# ── trigger template: runs on a plain AIDP job cluster ─────────────────────

def test_template_reads_no_bare_coordinate_env_index():
    template = _trigger_template()
    for key in _COORD_ENV + _PLATFORM_ENV:
        assert f"os.environ['{key}']" not in template, key


def test_template_does_not_depend_on_the_installed_aidp_compat():
    """The saved notebook must work whatever aidp_compat wheel the job cluster has."""
    assert "aidp_compat" not in _trigger_template()


def test_armed_trigger_on_an_exported_cluster_posts_to_the_same_url(job_env):
    """Exporter env (AIDP_WORKSPACE_ID) on top of the platform env agrees."""
    job_env.setenv("AIDP_ID", LAKE)
    job_env.setenv("AIDP_WORKSPACE_KEY", WS)
    for key, value in recommended_env_vars(_cfg()).items():
        job_env.setenv(key, value)
    runtime = _Runtime(job_env)
    runtime.run(_armed(_trigger_template()))
    assert runtime.posts == [(EXPECTED_URL, {"jobKey": "wf-key-208"})]


@pytest.mark.parametrize("env, named", [
    ({"AIDP_WORKSPACE_KEY": WS}, ("AIDP_LAKE_OCID", "AIDP_ID", "DATALAKE_ID")),
    ({"AIDP_ID": LAKE}, ("AIDP_WORKSPACE_ID", "AIDP_WORKSPACE_KEY")),
    ({"AIDP_ID": NO_REGION_LAKE, "AIDP_WORKSPACE_KEY": WS},
     ("AIDP_REGION", "the lake OCID", "OCI_RESOURCE_PRINCIPAL_REGION")),
], ids=["lake", "workspace", "region"])
def test_armed_trigger_names_a_missing_coordinate_before_posting(job_env, env, named):
    """No env supplies a coordinate: fail naming every variable tried, no POST."""
    for key, value in env.items():
        job_env.setenv(key, value)
    runtime = _Runtime(job_env)
    with pytest.raises(ValueError) as raised:
        runtime.run(_armed(_trigger_template()))
    for name in named:
        assert name in str(raised.value), name
    assert runtime.posts == []
    assert runtime.secret_reads == 0  # not mistakable for an auth failure


def test_placeholder_trigger_touches_neither_auth_nor_network(job_env):
    runtime = _Runtime(job_env)
    runtime.run(_trigger_template())
    assert runtime.posts == []
    assert runtime.secret_reads == 0


def test_prompt_credential_comments_name_the_artifact_cell():
    """The old comment sent operators to a 'cluster bootstrap' that does not
    exist in the migrated job; the credential cell is saved in the notebook
    (notebook_oci_credentials.ensure_artifact_oci_credentials)."""
    assert "cluster bootstrap sets this" not in jm.CELL_MIGRATE_PROMPT
    assert ("set by the tool's credential cell at the top of this notebook"
            in _trigger_template())
