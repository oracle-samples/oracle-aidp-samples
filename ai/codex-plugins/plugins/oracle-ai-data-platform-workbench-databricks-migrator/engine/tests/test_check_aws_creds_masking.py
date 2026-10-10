"""SEC-NEW-DATABRICKS-02: ``check_aws_creds.py`` reports where credentials
live, never their values.

The cluster cells are executed here against a fake ``spark`` and a planted
environment / init script, and their printed output is grepped for every
planted secret value. Names, paths, the endpoint and the region must still
appear -- the script stays a useful diagnostic.
"""
import ast
import contextlib
import importlib
import io
import os
import sys
import types

import pytest

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
SCRIPT = os.path.join(SCRIPTS_DIR, "check_aws_creds.py")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

PLANTED_ENV = {
    "AWS_ACCESS_KEY_ID": "AKIAPLANTEDACCESSKEY",
    "AWS_SECRET_ACCESS_KEY": "PlantedSecretAccessKey/Value+0123456789ab",
    "AWS_SESSION_TOKEN": "PlantedSessionTokenValue.FQoGZXIvYXdzE",
    "FUSE_DECRYPT_PASSPHRASE": "planted-passphrase-value",
    "AWS_NEW_UNKNOWN_SETTING": "planted-unknown-aws-value",
    # AWS in the name without the AWS_ prefix: selected by the env cell, so masked
    "AWSPASS": "env-awspass-planted-value-1",
    "S3_AWS_SIGNATURE": "env-sig-planted-value-4",
}
CLEAR_ENV = {"AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "eu-west-1"}

SPARK_SECRETS = {
    "spark.hadoop.fs.s3a.access.key": "AKIAPLANTEDSPARKKEY1",
    "spark.hadoop.fs.s3a.secret.key": "PlantedSparkSecretKey+Value/0123456789abcdef",
    "spark.hadoop.fs.s3a.session.token": "PlantedSparkSessionTokenValue",
}
SPARK_CLEAR = {
    "spark.hadoop.fs.s3a.endpoint": "s3.us-east-1.amazonaws.com",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}
HADOOP_SECRETS = {
    "fs.s3a.access.key": "AKIAPLANTEDHADOOPKEY",
    "fs.s3a.secret.key": "PlantedHadoopSecretKey+Value/0123456789abc",
    "fs.s3a.session.token": "PlantedHadoopSessionTokenValue",
}
HADOOP_CLEAR = {
    "fs.s3a.endpoint": "s3.eu-west-1.amazonaws.com",
    "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "fs.s3a.assumed.role.arn": "arn:aws:iam::123456789012:role/planted",
}
INIT_SCRIPT_SECRETS = {
    "export": "PlantedInitScriptSecret/Value0123456789",
    "configure": "PlantedConfigureSecretValue",
    "header": "PlantedBearerTokenValue",
    # the value is the second word / the lhs / a yaml-style fragment
    "echo": "PlantedEchoSecretValue99",
    "yaml": "PlantedYamlSecretValue77",
    "valuefirst": "PlantedBareKeyValue",
}
INIT_SCRIPT = (
    "#!/bin/bash\n"
    f"export AWS_SECRET_ACCESS_KEY={INIT_SCRIPT_SECRETS['export']}\n"
    f"aws configure set aws_secret_access_key {INIT_SCRIPT_SECRETS['configure']}\n"
    f'curl -H "Authorization: Bearer {INIT_SCRIPT_SECRETS["header"]}" https://keys.example\n'
    f"echo {INIT_SCRIPT_SECRETS['echo']} > /tmp/aws_secret\n"
    "cat > ~/.aws/credentials <<EOF\n"
    f"aws_secret_access_key: {INIT_SCRIPT_SECRETS['yaml']}\n"
    "EOF\n"
    f"{INIT_SCRIPT_SECRETS['valuefirst']}=key\n"
    "echo done\n"
)

ALL_SECRET_VALUES = (list(PLANTED_ENV.values()) + list(SPARK_SECRETS.values())
                     + list(HADOOP_SECRETS.values()) + list(INIT_SCRIPT_SECRETS.values()))


# ── fake cluster ────────────────────────────────────────────────────────
class _Conf:
    def __init__(self, values):
        self._v = values

    def get(self, key):
        if key not in self._v:
            raise Exception(f"{key} is not set")
        return self._v[key]


class _HadoopConf(_Conf):
    def get(self, key):
        return self._v.get(key)


def _fake_spark():
    hconf = _HadoopConf({**HADOOP_SECRETS, **HADOOP_CLEAR})
    sc = types.SimpleNamespace(_jsc=types.SimpleNamespace(hadoopConfiguration=lambda: hconf))
    return types.SimpleNamespace(conf=_Conf({**SPARK_SECRETS, **SPARK_CLEAR}), sparkContext=sc)


def _load_cells(workspace_root):
    """The cluster cells, from ``build_cells`` when the script has it.

    Pre-fix the cells were a ``cells = [...]`` literal inside ``main()`` and the
    module connected to a cluster at import time, so that layout is read
    through ``ast`` instead of imported.
    """
    with open(SCRIPT, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    if any(isinstance(n, ast.FunctionDef) and n.name == "build_cells" for n in tree.body):
        mod = importlib.import_module("check_aws_creds")
        return mod.build_cells(workspace_root=workspace_root)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "cells" for t in node.targets):
            cells = ast.literal_eval(node.value)
            return [c.replace('"/Workspace"', repr(workspace_root)) for c in cells]
    raise AssertionError("no cluster cells found in check_aws_creds.py")


def _run_cell(code, spark):
    ns = {"__name__": "__cell__", "spark": spark}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        exec(compile(code, "<cell>", "exec"), ns)
    return buf.getvalue()


@pytest.fixture
def planted(monkeypatch, tmp_path):
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key, raising=False)
    for k, v in {**PLANTED_ENV, **CLEAR_ENV}.items():
        monkeypatch.setenv(k, v)
    init_dir = tmp_path / "Shared" / "init"
    init_dir.mkdir(parents=True)
    (init_dir / "setup_s3.sh").write_text(INIT_SCRIPT, encoding="utf-8")
    (init_dir / "aws_credentials.properties").write_text("x=y\n", encoding="utf-8")
    return str(tmp_path).replace(os.sep, "/")


def _run_offline_cells(workspace_root):
    """Every cell that does not shell out (``find`` / ``jar`` are cluster-only)."""
    spark = _fake_spark()
    out = ""
    for code in _load_cells(workspace_root):
        if "subprocess" in code:
            continue
        out += _run_cell(code, spark)
    return out


# ── the finding ─────────────────────────────────────────────────────────
def test_cells_never_print_a_planted_secret_value(planted):
    out = _run_offline_cells(planted)
    leaked = [v for v in ALL_SECRET_VALUES if v in out]
    assert not leaked, f"secret values printed by the cells: {leaked}"
    # not even a prefix of a secret (the old cells printed value[:80])
    assert not [v for v in ALL_SECRET_VALUES if v[:12] in out]


def test_cells_still_report_where_credentials_live(planted):
    out = _run_offline_cells(planted)
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                 "FUSE_DECRYPT_PASSPHRASE", "spark.hadoop.fs.s3a.secret.key",
                 "fs.s3a.secret.key", "setup_s3.sh", "aws_credentials.properties",
                 "export AWS_SECRET_ACCESS_KEY", "aws_secret_access_key:", "AWSPASS"):
        assert name in out, name
    # non-secret settings print in clear
    assert "AWS_REGION=us-east-1" in out
    assert SPARK_CLEAR["spark.hadoop.fs.s3a.endpoint"] in out
    assert HADOOP_CLEAR["fs.s3a.assumed.role.arn"] in out
    assert HADOOP_CLEAR["fs.s3a.aws.credentials.provider"] in out
    # and secrets as presence + length only
    assert f"AWS_SECRET_ACCESS_KEY=<set, {len(PLANTED_ENV['AWS_SECRET_ACCESS_KEY'])} chars>" in out
    assert f"AWSPASS=<set, {len(PLANTED_ENV['AWSPASS'])} chars>" in out
    assert f"fs.s3a.secret.key=<set, {len(HADOOP_SECRETS['fs.s3a.secret.key'])} chars>" in out


# ── the policy helpers ─────────────────────────────────────────────────
@pytest.fixture
def mod():
    return importlib.import_module("check_aws_creds")


@pytest.mark.parametrize("name,secret", [
    ("AWS_ACCESS_KEY_ID", True),
    ("AWS_SECRET_ACCESS_KEY", True),
    ("AWS_SESSION_TOKEN", True),
    ("AWS_SOMETHING_NEW", True),            # unknown AWS_* defaults to masked
    ("AWSPASS", True),                      # AWS without the AWS_ prefix too
    ("S3_AWS_SIGNATURE", True),
    ("AWS", True),
    ("AWS_REGION", False),
    ("AWS_SHARED_CREDENTIALS_FILE", False),  # a path, allowlisted
    ("AWS_WEB_IDENTITY_TOKEN_FILE", False),
    ("spark.hadoop.fs.s3a.access.key", True),
    ("spark.hadoop.fs.s3a.secret.key", True),
    ("fs.s3a.session.token", True),
    ("fs.s3a.endpoint", False),
    ("fs.s3a.aws.credentials.provider", False),
    ("fs.s3a.assumed.role.arn", False),
    ("MY_DECRYPT_PASSPHRASE", True),
    ("DB_PASSWORD", True),
    ("api_token", True),
    ("PATH", False),
    ("JAVA_HOME", False),
])
def test_is_secret_name_policy(mod, name, secret):
    assert mod.is_secret_name(name) is secret


def test_mask_reveals_only_presence_and_length(mod):
    assert mod.mask("abcdef") == "<set, 6 chars>"
    assert "abc" not in mod.mask("abcdef")
    assert mod.mask("") == "<empty>"
    assert mod.show("AWS_REGION", "us-east-1") == "us-east-1"
    assert mod.show("AWS_SECRET_ACCESS_KEY", "abcdef") == "<set, 6 chars>"


def test_redact_line_keeps_the_head_and_drops_the_value(mod):
    assert mod.redact_line("export AWS_SECRET_ACCESS_KEY=abc123") == \
        "export AWS_SECRET_ACCESS_KEY=<6 chars redacted>"
    # only the command survives of a non-assignment line: the value may be any word
    assert mod.redact_line("aws configure set aws_secret_access_key abc123") == \
        "aws <46 chars, rest redacted>"
    assert mod.redact_line("spark.hadoop.fs.s3a.secret.key  abc") == \
        "spark.hadoop.fs.s3a.secret.key <35 chars, rest redacted>"
    assert mod.redact_line("-Dfs.s3a.secret.key=abc") == "-Dfs.s3a.secret.key=<3 chars redacted>"
    assert mod.redact_line("  # keys below  ") == "# <12 chars, rest redacted>"
    assert mod.redact_line("key") == "key"
    assert mod.redact_line("") == ""


@pytest.mark.parametrize("line,secret", [
    ("echo PlantedEchoSecretValue99 > /tmp/aws_secret", "PlantedEchoSecretValue99"),
    ("aws_secret_access_key: PlantedYamlSecretValue77", "PlantedYamlSecretValue77"),
    ("PlantedBareKeyValue=key", "PlantedBareKeyValue"),
    ("echo PlantedBareKeyValue=key >> ~/.aws/credentials", "PlantedBareKeyValue"),
    ("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
])
def test_redact_line_never_keeps_a_value_in_the_head(mod, line, secret):
    out = mod.redact_line(line)
    assert secret not in out and secret[:8] not in out, out
    assert "redacted" in out


def test_scrub_masks_aws_access_key_ids_in_returned_text(mod):
    text = "found AKIAPLANTEDACCESSKEY in file ASIAPLANTEDSESSION12 and AKIAshort"
    out = mod.scrub(text)
    assert "AKIAPLANTEDACCESSKEY" not in out and "ASIAPLANTEDSESSION12" not in out
    assert out.count("<aws-key-id redacted>") == 2
    assert "AKIAshort" in out  # not key-id shaped, left alone


def test_prelude_matches_the_module_helpers(mod):
    ns = {}
    exec(compile(mod._mask_prelude(), "<prelude>", "exec"), ns)
    for name in ("AWS_SECRET_ACCESS_KEY", "AWS_REGION", "fs.s3a.aws.credentials.provider",
                 "spark.hadoop.fs.s3a.secret.key"):
        assert ns["is_secret_name"](name) == mod.is_secret_name(name)
    for line in ("A=b", "echo Secret99Value > f", "PlantedBareKeyValue=key", "aws_key: v"):
        assert ns["redact_line"](line) == mod.redact_line(line)


def test_import_does_not_open_a_cluster_session(monkeypatch):
    class _Boom:
        def __init__(self, *a, **k):
            raise AssertionError("AIDPSession constructed at import time")

    monkeypatch.setitem(sys.modules, "aidp_executor", types.SimpleNamespace(AIDPSession=_Boom))
    sys.modules.pop("check_aws_creds", None)
    mod = importlib.import_module("check_aws_creds")
    assert callable(mod.build_cells)
