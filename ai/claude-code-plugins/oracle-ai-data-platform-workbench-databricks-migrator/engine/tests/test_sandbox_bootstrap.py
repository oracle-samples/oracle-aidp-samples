"""SEC-AIDP-SAMPLES-005 / 006: the cluster bootstrap emitted by ``job_migrate.py``.

- The declared sandbox admits the staging areas the migrator itself steers
  notebooks into (``/Volumes/default/default/dbfs/``, ``/tmp/``) and the tool's
  output directory, next to the write-redirect bucket; everything else stays
  refused.
- The bootstrap freezes the policy for the kernel; an operator-provided
  ``AIDP_SANDBOX_PREFIX`` wins.
- ``dbutils.secrets`` is restricted to the scope/key literals found while
  planning the job's notebooks (``AIDP_SECRET_SCOPES`` / ``AIDP_SECRET_KEYS``);
  before planning nothing is allowed; an operator-provided value wins.
"""
import os
import sys

import pytest

pytest.importorskip("anthropic")
pytest.importorskip("oci")
pytest.importorskip("nbformat")

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import job_migrate as jm  # noqa: E402

from aidp_compat import notebook_policy as npol  # noqa: E402
from aidp_compat.fs import AIDPFileSystemUtils  # noqa: E402
from aidp_compat.secrets import AIDPSecretsUtils  # noqa: E402

OUTPUT_BASE = "/Workspace/Users/u@example.com/aidp_migration"
BUCKET = f"oci://{jm._REDIRECT_BUCKET}@{jm._REDIRECT_NAMESPACE}/"


def _bootstrap(snippet: str) -> None:
    """Run the kernel snippet the way the cluster session does (fresh namespace)."""
    exec(compile(snippet, "<bootstrap>", "exec"), {"__name__": "__bootstrap__"})


def _code(src):
    return {"cell_type": "code", "source": src}


@pytest.fixture(autouse=True)
def _clean_plan(monkeypatch):
    jm.clear_planned_secret_refs()
    for key in list(os.environ):
        if key.startswith("_AIDP_PLANNED_"):
            monkeypatch.delenv(key, raising=False)
    yield
    jm.clear_planned_secret_refs()


# ── sandbox prefixes ───────────────────────────────────────────────────
def test_bootstrap_declares_bucket_staging_areas_and_output_dir():
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))
    policy = npol.require_policy()
    catalog, schema = jm._REDIRECT_TABLE_PREFIX.split(".", 1)
    assert (policy.catalog, policy.schema, policy.prefix) == (catalog, schema, BUCKET)
    assert policy.extra_prefixes == ("/Volumes/default/default/dbfs/", "/tmp/", OUTPUT_BASE + "/")
    for allowed in (
        BUCKET + "src-bucket/out/part-0.parquet",
        "/Volumes/default/default/dbfs/FileStore/u/model.pkl",     # /dbfs translation target
        "/tmp/out.csv",                                             # torch / h5py / sqlite staging
        "/tmp/x",                                                   # dbutils.fs.rm('dbfs:/tmp/x')
        OUTPUT_BASE + "/job/task_values.json",
    ):
        assert policy.path_in_sandbox(allowed), allowed
        assert npol.assert_path_in_sandbox(allowed, operation="test") == allowed
    for refused in (
        f"oci://other-bucket@{jm._REDIRECT_NAMESPACE}/x",
        f"oci://{jm._REDIRECT_BUCKET}@other-namespace/x",
        "/Volumes/default/default/other_volume/x",
        "/Volumes/prod/x",
        "/Workspace/Users/u@example.com/other/notebook.ipynb",
        "/Workspace/.oci/config",
        "/tmp/../etc/passwd",
        "/Volumes/default/default/dbfs/../../prod/x",
    ):
        assert not policy.path_in_sandbox(refused), refused
    # The bootstrap itself froze the policy and recorded the sandbox identifiers.
    declared = [e for e in npol.get_policy_log() if e["event"] == "sandbox_declared"]
    assert len(declared) == 1 and declared[0]["detail"]["source"] == "environment"


def test_bootstrap_sandbox_admits_the_cells_the_migrator_generates():
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))
    for cell in (
        "with open('/Volumes/default/default/dbfs/FileStore/u/model.pkl', 'rb') as f:\n    m = f.read()",
        "dbutils.fs.rm('/Volumes/default/default/dbfs/tmp/stage', True)",
        "df.write.mode('overwrite').parquet('/Volumes/default/default/dbfs/out/')",
        "dbutils.fs.put('/tmp/x.txt', 'hi', True)",
        f"dbutils.fs.mkdirs('{OUTPUT_BASE}/job/scratch')",
        "import shutil\nshutil.copy2('/tmp/model.pt', '/Volumes/default/default/dbfs/models/model.pt')",
    ):
        assert npol.scan_source(cell) == [], cell
    for cell, rule in (
        ("dbutils.fs.rm('/Volumes/prod/data', True)", "NBP-DBFS-PATH"),
        ("open('/Workspace/.oci/config').read()", "NBP-OPEN-PATH"),
        ("df.write.parquet('oci://other-bucket@ns/out')", "NBP-PATH-WRITE"),
    ):
        assert [v.rule for v in npol.scan_source(cell)] == [rule], cell
    fs = AIDPFileSystemUtils(spark=None)
    assert fs._translate_path("dbfs:/tmp/x") == "/tmp/x"
    assert npol.assert_path_in_sandbox(fs._translate_path("dbfs:/tmp/x"), operation="dbutils.fs.rm") == "/tmp/x"


def test_bootstrap_without_output_base_and_prefix_list_shape():
    assert jm.sandbox_prefixes() == [BUCKET, "/Volumes/default/default/dbfs/", "/tmp/"]
    assert jm.sandbox_prefixes("/Workspace/out/") == [BUCKET, "/Volumes/default/default/dbfs/", "/tmp/",
                                                      "/Workspace/out/"]
    _bootstrap(jm.build_sandbox_policy_snippet())
    assert npol.require_policy().extra_prefixes == ("/Volumes/default/default/dbfs/", "/tmp/")


def test_operator_provided_sandbox_prefix_wins_and_bootstrap_replay_is_a_noop(monkeypatch):
    monkeypatch.setenv("AIDP_SANDBOX_PREFIX", BUCKET + "ops/,/Volumes/default/default/dbfs/")
    snippet = jm.build_sandbox_policy_snippet(OUTPUT_BASE)
    _bootstrap(snippet)
    policy = npol.require_policy()
    assert policy.prefixes == (BUCKET + "ops/", "/Volumes/default/default/dbfs/")
    assert not policy.path_in_sandbox("/tmp/x")
    _bootstrap(snippet)              # replayed on reconnect: same frozen policy, no refusal
    assert npol.require_policy() == policy
    assert [e for e in npol.get_policy_log() if e["rule"] == "NBP-POLICY-TAMPER"] == []


# ── planned secrets allowlist ──────────────────────────────────────────
def test_secret_refs_are_collected_from_literals_only():
    cells = [
        _code("pw = dbutils.secrets.get('db', 'password')\n"
              "tok = dbutils.secrets.get(scope='api', key='token')\n"
              "raw = dbutils.secrets.getBytes('db', 'cert')"),
        _code(["%sql\n", "SELECT 1"]),                                  # magic cell: skipped
        _code("%pip install x\nx = dbutils.secrets.get('after_magic', 'k')"),
        {"cell_type": "markdown", "source": "dbutils.secrets.get('md', 'x')"},
        _code("k = dbutils.widgets.get('k')\nv = dbutils.secrets.get('dyn', k)\n"
              "names = dbutils.secrets.list('listed')\n"
              "s = dbutils.widgets.get('s')\nw = dbutils.secrets.get(s, 'k')"),     # dynamic scope: not planned
        _code("oidlUtils.secrets.get('renamed', 'k')"),
    ]
    assert jm.collect_secret_refs(jm._cell_sources(cells)) == {
        "db": {"password", "cert"}, "api": {"token"}, "after_magic": {"k"},
        "dyn": {"*"}, "listed": set(), "renamed": {"k"},
    }
    jm.record_planned_secret_refs(cells[:1])
    assert jm.secret_allowlists() == ("api,db", "api/token,db/cert,db/password")


def test_bootstrap_restricts_secrets_to_the_planned_scopes_and_keys(monkeypatch):
    jm.record_planned_secret_refs([
        _code("pw = dbutils.secrets.get('db', 'password')"),
        _code("k = dbutils.widgets.get('k')\nv = dbutils.secrets.get('dyn', k)\n"
              "names = dbutils.secrets.list('listed')"),
    ])
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))
    assert os.environ["AIDP_SECRET_SCOPES"] == "db,dyn,listed"
    assert os.environ["AIDP_SECRET_KEYS"] == "db/password,dyn/*"
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault", lambda self, s, k: f"vault:{s}/{k}")
    s = AIDPSecretsUtils()
    assert s.get("db", "password") == "vault:db/password"
    assert s.get("dyn", "anything") == "vault:dyn/anything"      # literal scope, non-literal key
    assert s.list("listed") == []
    for scope, key in (("db", "other"), ("prod", "password"), ("listed", "x")):
        with pytest.raises(PermissionError) as ei:
            s.get(scope, key)
        assert "vault:" not in str(ei.value)
    refusals = [e for e in npol.get_policy_log() if e["rule"] == "NBP-RUNTIME-SECRET"]
    assert [e["target"] for e in refusals] == ["db/other", "prod", "listed/x"]
    assert all(e["detail"]["operation"] == "dbutils.secrets.get" for e in refusals)


def test_bootstrap_before_planning_allows_no_secret(monkeypatch):
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))
    assert (os.environ["AIDP_SECRET_SCOPES"], os.environ["AIDP_SECRET_KEYS"]) == ("none", "none")
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault", lambda self, s, k: f"vault:{s}/{k}")
    s = AIDPSecretsUtils()
    with pytest.raises(PermissionError):
        s.get("db", "password")
    with pytest.raises(PermissionError):
        s.list("db")
    assert s.listScopes() == []


def test_operator_secret_allowlist_wins_but_the_plan_replaces_its_own_value(monkeypatch):
    monkeypatch.setenv("AIDP_SECRET_SCOPES", "ops")
    monkeypatch.setenv("AIDP_SECRET_KEYS", "ops/*")
    jm.record_planned_secret_refs([_code("dbutils.secrets.get('db', 'password')")])
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))
    assert (os.environ["AIDP_SECRET_SCOPES"], os.environ["AIDP_SECRET_KEYS"]) == ("ops", "ops/*")

    monkeypatch.delenv("AIDP_SECRET_SCOPES")
    monkeypatch.delenv("AIDP_SECRET_KEYS")
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))             # task 1 (no operator value)
    assert os.environ["AIDP_SECRET_SCOPES"] == "db"
    jm.record_planned_secret_refs([_code("dbutils.secrets.get('api', 'token')")])
    _bootstrap(jm.build_sandbox_policy_snippet(OUTPUT_BASE))             # task 2 widens the plan
    assert (os.environ["AIDP_SECRET_SCOPES"], os.environ["AIDP_SECRET_KEYS"]) == ("api,db", "api/token,db/password")
    monkeypatch.setattr(AIDPSecretsUtils, "_get_from_oci_vault", lambda self, s, k: f"vault:{s}/{k}")
    assert AIDPSecretsUtils().get("api", "token") == "vault:api/token"   # allowlist is read per call
