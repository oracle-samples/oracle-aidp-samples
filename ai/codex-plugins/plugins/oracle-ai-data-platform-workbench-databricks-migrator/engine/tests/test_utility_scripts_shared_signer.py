"""Every standalone utility builds its signer through oci_auth — no local
`oci.config.from_file` auth sites remain (maintainer policy 2026-10-12: the
AIDP Credential Store is canonical; config files are the laptop fallback,
reachable only through oci_auth.get_oci_signer).

Hermetic: oci_auth.get_oci_signer is monkeypatched; oci.config.from_file is
rigged to fail so any regression back to a module-local file-only builder
fails loudly. Also pins the auth-source surfacing lines (one print per build;
WARN when the file path serves a cluster-like environment).
"""
import importlib
import os
import sys

import pytest

_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import oci_auth as oa  # noqa: E402


UTILITIES = [
    # (module, factory attr, returns_pair)
    ("analyze_jars", "get_oci_signer", False),
    ("deploy_aidp_compat", "get_signer", False),
    ("enumerate_and_download", "get_oci_signer", True),
    ("fetch_spark_logs", "get_signer", False),
    ("install_cluster_libraries", "get_signer", False),
    ("job_preflight", "get_signer", False),
    ("scan_workspace_utils", "get_signer", False),
]


@pytest.mark.parametrize("module_name,factory,returns_pair", UTILITIES)
def test_utility_delegates_to_oci_auth(monkeypatch, module_name, factory, returns_pair):
    mod = importlib.import_module(module_name)
    sentinel_signer = object()
    sentinel_cfg = {"region": "unit"}
    monkeypatch.setattr(oa, "get_oci_signer",
                    lambda profile=None, config_path=None: (sentinel_cfg, sentinel_signer))
    # Regression tripwire: the old module-local file-only path must be dead.
    monkeypatch.setattr(
        oa.oci.config, "from_file",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError(f"{module_name} must not read oci config files directly")),
    )
    result = getattr(mod, factory)()
    if returns_pair:
        assert result == (sentinel_cfg, sentinel_signer)
    else:
        assert result is sentinel_signer


def test_no_module_local_from_file_auth_sites():
    """Source-level pin (AST, so prompt strings and docstrings don't count):
    the only sanctioned `oci.config.from_file` CALL site under scripts/ is
    oci_auth itself. Caught 3 utilities the first survey missed (job_preflight,
    prepare_validation_cluster, scan_workspace_utils) — fix the class."""
    import ast

    offenders = []
    scripts_dir = os.path.join(os.path.dirname(__file__), "..", "scripts")
    for name in sorted(os.listdir(scripts_dir)):
        if not name.endswith(".py") or name == "oci_auth.py":
            continue
        with open(os.path.join(scripts_dir, name), encoding="utf-8", errors="replace") as f:
            try:
                tree = ast.parse(f.read())
            except SyntaxError:
                continue
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute) and node.func.attr == "from_file"
                    and isinstance(node.func.value, ast.Attribute) and node.func.value.attr == "config"
                    and isinstance(node.func.value.value, ast.Name) and node.func.value.value.id == "oci"):
                offenders.append(f"{name}:{node.lineno}")
    assert not offenders, (
        "new file-based auth sites (delegate to oci_auth.get_oci_signer): "
        + ", ".join(offenders))


def test_auth_source_surfacing(monkeypatch, capsys):
    monkeypatch.delenv("AIDP_ID", raising=False)
    monkeypatch.delenv("AIDP_AUTH", raising=False)
    oa._surface_auth_source("file")
    assert "laptop fallback" in capsys.readouterr().out
    oa._surface_auth_source("credential-store")
    assert "Credential Store" in capsys.readouterr().out
    monkeypatch.setenv("AIDP_ID", "unit-cluster-marker")
    oa._surface_auth_source("file")
    assert "WARNING" in capsys.readouterr().out


def test_pass_phrase_reaches_api_key_signer(monkeypatch):
    """PR #234 review F2: encrypted laptop keys — pass_phrase must flow to the
    Signer (prepare_validation_cluster passed it before consolidation)."""
    captured = {}

    class FakeSigner:
        def __init__(self, **kw):
            captured.update(kw)

    monkeypatch.setattr(oa.oci, "signer", type("S", (), {"Signer": FakeSigner}))
    monkeypatch.setattr(oa, "_apply_cred_env_from_config", lambda *a, **k: None)
    monkeypatch.setitem(sys.modules, "oci_credential_helper", None)  # import fails → laptop
    monkeypatch.setattr(oa.oci.config, "from_file", lambda **kw: {
        "tenancy": "t", "user": "u", "fingerprint": "f",
        "key_file": "/k", "pass_phrase": "unit-secret-phrase"})
    monkeypatch.delenv("OCI_CONFIG_FILE", raising=False)
    monkeypatch.delenv("AIDP_ID", raising=False)
    monkeypatch.delenv("AIDP_AUTH", raising=False)
    cfg, signer = oa.get_oci_signer("P")
    assert captured["pass_phrase"] == "unit-secret-phrase"


def test_non_aidputils_importerror_fails_loud_on_cluster(monkeypatch):
    """PR #234 review F1 (narrowed): a broken store path on a cluster must not
    silently degrade to file auth; only the aidputils-missing laptop signal may."""
    import types

    broken = types.ModuleType("oci_credential_helper")
    def _boom():
        raise ImportError("oci.signer missing in broken cluster env")
    broken.get_signer = _boom
    monkeypatch.setitem(sys.modules, "oci_credential_helper", broken)
    monkeypatch.setattr(oa, "_apply_cred_env_from_config", lambda *a, **k: None)
    monkeypatch.setenv("AIDP_ID", "unit-cluster-marker")
    with pytest.raises(ImportError, match="broken cluster env"):
        oa.get_oci_signer("P")
    # Same ImportError on a laptop (no markers) still falls through to file auth.
    monkeypatch.delenv("AIDP_ID", raising=False)
    monkeypatch.delenv("AIDP_AUTH", raising=False)
    monkeypatch.delenv("OCI_CONFIG_FILE", raising=False)
    monkeypatch.setattr(oa.oci.config, "from_file", lambda **kw: {
        "tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"})
    captured = {}
    class FakeSigner:
        def __init__(self, **kw):
            captured.update(kw)
    monkeypatch.setattr(oa.oci, "signer", type("S", (), {"Signer": FakeSigner}))
    cfg, _ = oa.get_oci_signer("P")
    assert captured["tenancy"] == "t"


def test_prepare_validation_cluster_config_arg_optional():
    """PR #234 review F3: store-only invocation must parse without
    --oci-config-path. Pinned at the argparse DEFINITION via AST (the module
    builds its parser inside main(), so invoking it would run side effects):
    the --oci-config-path add_argument call must not carry required=True."""
    import ast

    path = os.path.join(_SCRIPTS, "prepare_validation_cluster.py")
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    found = False
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_argument" and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "--oci-config-path"):
            found = True
            for kw in node.keywords:
                assert not (kw.arg == "required" and isinstance(kw.value, ast.Constant)
                            and kw.value.value is True), \
                    "--oci-config-path must stay optional (PR #234 review F3)"
    assert found, "--oci-config-path argument definition not found"


def test_accepted_exception_cluster_file_fallback_warns(monkeypatch, capsys):
    """Maintainer decision (2026-10-12, PR #234): cleanly-absent aidputils on a
    cluster-like host falls through to file auth WITH the WARN — accepted
    TEMPORARY exception, to be revisited with the Scala aidputils migration
    (grep: ACCEPTED EXCEPTION). This pin makes any change to that behavior a
    deliberate decision, in either direction."""
    import types

    absent = types.ModuleType("oci_credential_helper")
    def _laptop_signal():
        raise ImportError("aidputils global not found on __main__ — cluster-only")
    absent.get_signer = _laptop_signal
    monkeypatch.setitem(sys.modules, "oci_credential_helper", absent)
    monkeypatch.setattr(oa, "_apply_cred_env_from_config", lambda *a, **k: None)
    monkeypatch.setenv("AIDP_ID", "unit-cluster-marker")
    monkeypatch.delenv("OCI_CONFIG_FILE", raising=False)
    monkeypatch.setattr(oa.oci.config, "from_file", lambda **kw: {
        "tenancy": "t", "user": "u", "fingerprint": "f", "key_file": "/k"})
    captured = {}
    class FakeSigner:
        def __init__(self, **kw):
            captured.update(kw)
    monkeypatch.setattr(oa.oci, "signer", type("S", (), {"Signer": FakeSigner}))
    cfg, _ = oa.get_oci_signer("P")
    assert captured["tenancy"] == "t"                      # fallback succeeded
    assert "WARNING" in capsys.readouterr().out            # and it was loud


def _fake_aidp_config(monkeypatch, selected_marker):
    """Install a fake aidp_config whose load_config proves WHICH YAML was
    selected: cli_config_path 'B.yaml' → credential name 'cred_from_B';
    discovery (None) → 'cred_from_discovered_A'."""
    import types

    fake = types.ModuleType("aidp_config")

    class ConfigError(Exception):
        pass

    def discover_config_path(cli_config_path=None):
        return cli_config_path or "/discovered/A.yaml"

    def load_config(cli_config_path=None, cli_overrides=None):
        # Model the real loader's precedence: explicit CLI path > $AIDP_CONFIG_PATH.
        eff = cli_config_path or os.environ.get("AIDP_CONFIG_PATH")
        name = {"B.yaml": "cred_from_B", "A.yaml": "cred_from_A"}.get(eff, "cred_from_discovered_A")
        selected_marker.append(cli_config_path)
        keys = types.SimpleNamespace(user=None, fingerprint=None, tenancy=None,
                                     region=None, private_key_content=None)
        oc = types.SimpleNamespace(name=name, keys=keys)
        return types.SimpleNamespace(aidp=types.SimpleNamespace(oci_credentials=oc))

    fake.ConfigError = ConfigError
    fake.discover_config_path = discover_config_path
    fake.load_config = load_config
    monkeypatch.setitem(sys.modules, "aidp_config", fake)
    return fake


def test_explicit_config_path_selects_its_credential_mapping(monkeypatch):
    """PR #234 Param review (BLOCKING): a caller's --config B.yaml must select
    B's credential mapping on the store path — never a discovered/env YAML A's."""
    import types

    selected = []
    _fake_aidp_config(monkeypatch, selected)
    monkeypatch.delenv("AIDP_CRED_OCI_NAME", raising=False)
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)
    sentinel = object()
    store = types.ModuleType("oci_credential_helper")
    store.get_signer = lambda: sentinel
    monkeypatch.setitem(sys.modules, "oci_credential_helper", store)

    cfg, signer = oa.get_oci_signer("P", config_path="B.yaml")
    assert signer is sentinel
    assert selected == ["B.yaml"]                       # explicit CLI path reached the loader
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_B"

    # control: no CLI path → discovery path, A's mapping
    selected.clear()
    oa.get_oci_signer("P")
    assert selected == [None]
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_discovered_A"


@pytest.mark.parametrize("module_name,invoke", [
    ("install_cluster_libraries",
     lambda mod: mod.get_signer("PROF", config_path="B.yaml")),
    ("deploy_aidp_compat",
     lambda mod: mod.get_signer()),
])
def test_utility_entry_handoff_selects_cli_yaml(monkeypatch, module_name, invoke):
    """PR #234 Param review: both utility entry points hand the CLI-selected
    YAML to the signer. deploy_aidp_compat publishes it via SELECTED_CONFIG_PATH
    (set in main() beside cluster_lifecycle.configure)."""
    import importlib, types

    selected = []
    _fake_aidp_config(monkeypatch, selected)
    monkeypatch.delenv("AIDP_CRED_OCI_NAME", raising=False)
    sentinel = object()
    store = types.ModuleType("oci_credential_helper")
    store.get_signer = lambda: sentinel
    monkeypatch.setitem(sys.modules, "oci_credential_helper", store)

    mod = importlib.import_module(module_name)
    if module_name == "deploy_aidp_compat":
        monkeypatch.setattr(mod, "SELECTED_CONFIG_PATH", "B.yaml")
    signer = invoke(mod)
    assert signer is sentinel
    assert selected == ["B.yaml"]
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_B"


def test_deploy_publication_covers_upload_session_signer(monkeypatch):
    """PR #234 review round 4 (BLOCKING): deploy_wheel's AIDPSession builds its
    own signer via a bare get_oci_signer(profile). _publish_selected_config must
    therefore make the CLI YAML authoritative process-wide ($AIDP_CONFIG_PATH),
    so BOTH the direct signer and the session-shaped construction select B even
    when the environment previously pointed at A."""
    import importlib, types

    selected = []
    _fake_aidp_config(monkeypatch, selected)
    monkeypatch.delenv("AIDP_CRED_OCI_NAME", raising=False)
    sentinel = object()
    store = types.ModuleType("oci_credential_helper")
    store.get_signer = lambda: sentinel
    monkeypatch.setitem(sys.modules, "oci_credential_helper", store)
    mod = importlib.import_module("deploy_aidp_compat")

    monkeypatch.setenv("AIDP_CONFIG_PATH", "A.yaml")      # pre-existing env YAML A
    monkeypatch.setattr(mod, "SELECTED_CONFIG_PATH", None)

    # BEFORE publication: the session-shaped construction (bare profile, no
    # config argument — exactly AIDPSession.connect's call) selects A. This is
    # the reported defect reproduced.
    oa.get_oci_signer("P")
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_A"

    mod._publish_selected_config("B.yaml")
    assert os.environ["AIDP_CONFIG_PATH"] == "B.yaml"

    # AFTER publication: the same session-shaped construction now selects B.
    oa.get_oci_signer("P")
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_B"

    # and the direct module signer still selects B explicitly
    selected.clear()
    assert mod.get_signer() is sentinel
    assert selected == ["B.yaml"]
    assert os.environ["AIDP_CRED_OCI_NAME"] == "cred_from_B"
