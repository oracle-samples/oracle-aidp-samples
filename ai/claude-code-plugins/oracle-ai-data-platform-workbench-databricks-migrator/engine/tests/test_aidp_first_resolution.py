"""AIDP-first dependency resolution: prefer an already-staged jar/wheel in the
shared repository over a Databricks/proxy fetch. Match by artifact name —
exact version preferred, else highest; unparseable jar names match by exact
basename only (never a fuzzy highest-version guess)."""

from types import SimpleNamespace

from dbx_export.__main__ import (
    DEFAULT_AIDP_LIB_DIR,
    _LibRepoIndex,
    _parse_jar_name_version,
    _parse_wheel_name_version,
    configured_lib_repository_dir,
    phase1_fetch_files,
    phase2_upload_files,
)
from dbx_export.dependency_resolver import FileNode, Graph
from dbx_export.report import Report

LIB = "/Workspace/migration-dependencies/libs"


class _ListAidp:
    """Minimal AidpClient stand-in exposing only list_workspace_objects.

    Accepts either a list of basenames (served for every path — the common
    single-dir case) or a {dir_path: [basenames]} dict for legacy-dir tests.
    """

    def __init__(self, basenames):
        self._dirs = basenames if isinstance(basenames, dict) else None
        self._items = (
            None if self._dirs is not None
            else [{"path": f"{LIB}/{b}"} for b in basenames]
        )
        self.listed = []

    def list_workspace_objects(self, path):
        self.listed.append(path)
        if self._dirs is not None:
            if path not in self._dirs:
                from dbx_export.aidp_client import AidpApiError
                raise AidpApiError("list_workspace_objects", 404, "not found")
            return [{"path": f"{path}/{b}"} for b in self._dirs[path]]
        return list(self._items)


class _NoUploadAidp:
    def upload_file(self, *a, **k):
        raise AssertionError("upload_file must not be called for an AIDP-first hit")

    def object_exists(self, path):
        raise AssertionError("object_exists must not be called for an AIDP-first hit")


class _RecordingDbx:
    def __init__(self):
        self.fetched = []

    def fetch_file_uri(self, uri):
        self.fetched.append(uri)
        return b"dbx-bytes", "workspace"


def _report():
    return Report(
        run_id="test-run", dbx_root_job_id="123",
        export_output="/Workspace/ai_tool_exported", prefix="ai_tool",
    )


# ─── parse helpers ───────────────────────────────────────────────────────────

def test_parse_wheel_name_version():
    assert _parse_wheel_name_version("aidp_compat-0.5.13-py3-none-any.whl") == (
        "aidp-compat", __import__("packaging.version", fromlist=["parse"]).parse("0.5.13"),
    )
    name, ver = _parse_wheel_name_version(
        "scikit_learn-1.5.2-cp311-cp311-manylinux_2_17_aarch64.whl"
    )
    assert name == "scikit-learn" and str(ver) == "1.5.2"
    assert _parse_wheel_name_version("not-a-wheel.jar") == (None, None)


def test_parse_jar_name_version():
    name, ver = _parse_jar_name_version("helperlib-1.6.1.jar")
    assert name == "helperlib" and str(ver) == "1.6.1"
    name, ver = _parse_jar_name_version("scala-logging_2.12-3.9.5.jar")
    assert name == "scala-logging_2.12" and str(ver) == "3.9.5"
    # git-hash tail is not a PEP-440 version → unparseable (exact-basename only).
    assert _parse_jar_name_version(
        "hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar"
    ) == (None, None)


# ─── _LibRepoIndex.resolve ───────────────────────────────────────────────────

def test_resolve_wheel_exact_version_preferred():
    idx = _LibRepoIndex(_ListAidp([
        "aidp_compat-0.5.11-py3-none-any.whl",
        "aidp_compat-0.5.13-py3-none-any.whl",
    ]), LIB)
    path, prov = idx.resolve("dbfs:/FileStore/jars/aidp_compat-0.5.11-py3-none-any.whl", "whl")
    assert path == f"{LIB}/aidp_compat-0.5.11-py3-none-any.whl"
    assert "exact" in prov


def test_resolve_wheel_highest_when_exact_absent():
    idx = _LibRepoIndex(_ListAidp([
        "aidp_compat-0.5.9-py3-none-any.whl",
        "aidp_compat-0.5.13-py3-none-any.whl",
        "aidp_compat-0.5.12-py3-none-any.whl",
    ]), LIB)
    path, prov = idx.resolve("dbfs:/x/aidp_compat-0.5.6-py3-none-any.whl", "whl")
    assert path == f"{LIB}/aidp_compat-0.5.13-py3-none-any.whl"
    assert "highest" in prov


def test_resolve_jar_unparseable_matches_exact_basename_only():
    base = "hudi-spark3.5-bundle_2.12-0.15.0-3975ca4.jar"
    idx = _LibRepoIndex(_ListAidp([base]), LIB)
    # exact basename present → matched
    path, prov = idx.resolve(f"dbfs:/jars/{base}", "jar")
    assert path == f"{LIB}/{base}"
    # a DIFFERENT git hash of the "same" bundle must NOT fuzzy-match to it
    other = "hudi-spark3.5-bundle_2.12-0.15.0-deadbee.jar"
    assert idx.resolve(f"dbfs:/jars/{other}", "jar") is None


def test_resolve_jar_exact_then_highest_when_parseable():
    idx = _LibRepoIndex(_ListAidp(["helperlib-1.6.1.jar", "helperlib-1.7.0.jar"]), LIB)
    # exact
    assert idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")[0] == f"{LIB}/helperlib-1.6.1.jar"
    # highest when exact absent
    path, prov = idx.resolve("dbfs:/jars/helperlib-1.5.0.jar", "jar")
    assert path == f"{LIB}/helperlib-1.7.0.jar" and "highest" in prov


def test_resolve_no_match_returns_none():
    idx = _LibRepoIndex(_ListAidp(["helperlib-1.6.1.jar"]), LIB)
    assert idx.resolve("dbfs:/jars/unrelated-2.0.0.jar", "jar") is None


def test_resolve_lists_repository_once():
    aidp = _ListAidp(["helperlib-1.6.1.jar"])
    idx = _LibRepoIndex(aidp, LIB)
    idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")
    idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")
    # Loaded lazily, exactly once (primary dir + one legacy-sibling probe).
    assert aidp.listed == [LIB, "/Workspace/migration-dependencies/jars"]


# ─── wheel tag safety (BLOCKER regression: arch/ABI substitution) ───────────

def test_resolve_wheel_rejects_architecture_mismatch(capsys):
    """The reviewer's executed repro: an x86_64 wheel staged, an aarch64 wheel
    requested, same name+version. Must NOT substitute — loud MANUAL ACTION
    log, then None (falls through to the normal Databricks fetch)."""
    staged = "numpy-1.26.4-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
    requested = "numpy-1.26.4-cp311-cp311-manylinux_2_17_aarch64.manylinux2014_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)

    assert idx.resolve(f"dbfs:/wheels/{requested}", "whl") is None
    out = capsys.readouterr().out
    assert "MANUAL ACTION REQUIRED" in out
    assert staged in out


def test_resolve_wheel_rejects_abi_mismatch():
    staged = "somelib-2.0.0-cp310-cp310-manylinux_2_17_aarch64.whl"
    requested = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    assert idx.resolve(f"dbfs:/wheels/{requested}", "whl") is None


def test_resolve_wheel_rejects_python2_pure_wheel(capsys):
    """A pure wheel is platform-independent, NOT Python-version-independent:
    py2-none-any must never substitute for a CPython 3 request."""
    staged = "somelib-2.0.0-py2-none-any.whl"
    requested = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    assert idx.resolve(f"dbfs:/wheels/{requested}", "whl") is None
    assert "MANUAL ACTION REQUIRED" in capsys.readouterr().out


def test_resolve_wheel_rejects_version_specific_pure_wheel():
    """py36-none-any is version-specific — outside the provably-safe subset
    (only the generic major, py3-none-any, substitutes automatically)."""
    staged = "somelib-2.0.0-py36-none-any.whl"
    requested = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    assert idx.resolve(f"dbfs:/wheels/{requested}", "whl") is None


def test_resolve_wheel_accepts_py2py3_pure_wheel():
    """py2.py3-none-any carries the generic py3 tag — safe for a py3 request."""
    staged = "somelib-2.0.0-py2.py3-none-any.whl"
    requested = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    path, _prov = idx.resolve(f"dbfs:/wheels/{requested}", "whl")
    assert path == f"{LIB}/{staged}"


def test_resolve_wheel_py2py3_request_never_narrows_to_py2_only(capsys):
    """Round-3 review repro: a py2.py3 request runs on AIDP's Python 3, but a
    py2-only staged wheel would not — the substitution must be rejected
    (decision anchored to the AIDP target major, py3)."""
    staged = "somelib-2.0.0-py2-none-any.whl"
    requested = "somelib-1.0.0-py2.py3-none-any.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    assert idx.resolve(f"dbfs:/wheels/{requested}", "whl") is None
    assert "MANUAL ACTION REQUIRED" in capsys.readouterr().out


def test_resolve_wheel_py2py3_request_accepts_py3_staged():
    """The Python-3 side of the target-runtime contract: py3-none-any staged
    IS acceptable for a py2.py3 request (both install on AIDP's Python 3)."""
    staged = "somelib-2.0.0-py3-none-any.whl"
    requested = "somelib-1.0.0-py2.py3-none-any.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    path, _prov = idx.resolve(f"dbfs:/wheels/{requested}", "whl")
    assert path == f"{LIB}/{staged}"


def test_resolve_wheel_pure_python_staged_is_universal():
    """A staged py3-none-any wheel installs anywhere — reusable even when the
    requested wheel was platform-specific."""
    staged = "somelib-2.0.0-py3-none-any.whl"
    requested = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    path, prov = idx.resolve(f"dbfs:/wheels/{requested}", "whl")
    assert path == f"{LIB}/{staged}"


def test_resolve_wheel_identical_platform_tags_reused():
    staged = "somelib-2.0.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    # Same tags, different (sanitized) source path — exact-basename already
    # covers the identical filename; force the by-name branch with a distinct
    # requested version staged alongside.
    idx = _LibRepoIndex(_ListAidp([staged]), LIB)
    requested = "somelib-1.9.0-cp311-cp311-manylinux_2_17_aarch64.whl"
    path, prov = idx.resolve(f"dbfs:/wheels/{requested}", "whl")
    assert path == f"{LIB}/{staged}"
    assert "highest" in prov


def test_resolve_wheel_version_substitution_logs_loudly(capsys):
    """Silent version drift regression: highest-version substitution must emit
    a highlighted warning naming both versions."""
    idx = _LibRepoIndex(_ListAidp([
        "aidp_compat-0.5.13-py3-none-any.whl",
    ]), LIB)
    path, _prov = idx.resolve("dbfs:/x/aidp_compat-0.5.6-py3-none-any.whl", "whl")
    assert path == f"{LIB}/aidp_compat-0.5.13-py3-none-any.whl"
    out = capsys.readouterr().out
    assert "VERSION SUBSTITUTION" in out
    assert "0.5.6" in out and "0.5.13" in out


def test_resolve_jar_version_substitution_logs_loudly(capsys):
    idx = _LibRepoIndex(_ListAidp(["helperlib-1.6.1.jar", "helperlib-1.7.0.jar"]), LIB)
    idx.resolve("dbfs:/jars/helperlib-1.5.0.jar", "jar")
    out = capsys.readouterr().out
    assert "VERSION SUBSTITUTION" in out


# ─── legacy /jars transition ─────────────────────────────────────────────────

def test_legacy_jars_sibling_still_indexed_with_warning(capsys):
    """Upgrades must not strand artifacts under the pre-consolidation /jars
    directory: it is indexed at lower precedence with a deprecation warning."""
    legacy_dir = "/Workspace/migration-dependencies/jars"
    idx = _LibRepoIndex(_ListAidp({
        LIB: ["helperlib-1.6.1.jar"],
        legacy_dir: ["oldlib-2.2.0.jar"],
    }), LIB)

    path, prov = idx.resolve("dbfs:/jars/oldlib-2.2.0.jar", "jar")
    assert path == f"{legacy_dir}/oldlib-2.2.0.jar"
    assert "DEPRECATED LAYOUT" in capsys.readouterr().out
    # Primary dir still wins for its own artifacts.
    assert idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")[0] == f"{LIB}/helperlib-1.6.1.jar"


def test_no_legacy_probe_for_custom_dir_name():
    """The legacy sibling probe only fires for a .../libs directory."""
    custom = "/Workspace/team/shared-deps"
    aidp = _ListAidp({custom: ["helperlib-1.6.1.jar"]})
    idx = _LibRepoIndex(aidp, custom)
    idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")
    assert aidp.listed == [custom]


def test_missing_legacy_dir_is_silent_on_clean_install(capsys):
    """Round-3 review LOW: on a clean installation the legacy /jars sibling
    correctly does not exist (404) — the OPTIONAL probe must not warn."""
    aidp = _ListAidp({LIB: ["helperlib-1.6.1.jar"]})  # only /libs exists → /jars 404s
    idx = _LibRepoIndex(aidp, LIB)
    idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")
    out = capsys.readouterr().out
    assert "cannot list" not in out
    # The probe still happened — suppression is log-only, not a behavior change.
    assert "/Workspace/migration-dependencies/jars" in aidp.listed


def test_primary_dir_list_failure_still_warns(capsys):
    """404 suppression applies ONLY to the optional legacy probe — a failure
    listing the primary repository still warns."""
    aidp = _ListAidp({})  # every dir 404s, including the primary
    idx = _LibRepoIndex(aidp, LIB)
    idx.resolve("dbfs:/jars/helperlib-1.6.1.jar", "jar")
    out = capsys.readouterr().out
    assert f"cannot list {LIB}" in out


# ─── phase1 gate ─────────────────────────────────────────────────────────────

def test_phase1_aidp_first_skips_dbx_fetch():
    uri = "dbfs:/FileStore/jars/helperlib-1.6.1.jar"
    graph = Graph(files={uri: FileNode(dbx_source=uri, kind="jar")})
    dbx = _RecordingDbx()
    lib_repo = _LibRepoIndex(_ListAidp(["helperlib-1.6.1.jar"]), LIB)

    phase1_fetch_files(dbx, graph, lib_repo)

    assert dbx.fetched == []  # never touched Databricks
    node = graph.files[uri]
    assert node.aidp_repo_path == f"{LIB}/helperlib-1.6.1.jar"
    assert node.content is None and node.fetch_error is None


def test_phase1_falls_through_to_dbx_when_no_repo_match():
    uri = "dbfs:/FileStore/jars/other-9.9.9.jar"
    graph = Graph(files={uri: FileNode(dbx_source=uri, kind="jar")})
    dbx = _RecordingDbx()
    lib_repo = _LibRepoIndex(_ListAidp(["helperlib-1.6.1.jar"]), LIB)

    phase1_fetch_files(dbx, graph, lib_repo)

    assert dbx.fetched == [uri]
    assert graph.files[uri].aidp_repo_path is None
    assert graph.files[uri].content == b"dbx-bytes"


def test_phase1_no_lib_repo_is_plain_dbx_fetch():
    uri = "dbfs:/FileStore/jars/helperlib-1.6.1.jar"
    graph = Graph(files={uri: FileNode(dbx_source=uri, kind="jar")})
    dbx = _RecordingDbx()

    phase1_fetch_files(dbx, graph, None)

    assert dbx.fetched == [uri]


# ─── phase2 attach ───────────────────────────────────────────────────────────

def test_phase2_attaches_aidp_first_hit_without_upload():
    uri = "dbfs:/FileStore/jars/aidp_compat-0.5.13-py3-none-any.whl"
    hit = f"{LIB}/aidp_compat-0.5.13-py3-none-any.whl"
    graph = Graph(files={uri: FileNode(
        dbx_source=uri, kind="whl",
        aidp_repo_path=hit, aidp_repo_provenance="AIDP-first: exact version",
    )})
    report = _report()

    mapping, failed = phase2_upload_files(
        _NoUploadAidp(), graph, "/Workspace/ai_tool_exported", LIB, False, report,
    )

    assert mapping == {uri: hit}
    assert failed is False
    assert report.files[-1].status == "SKIPPED_EXISTS"
    assert report.files[-1].aidp_path == hit


# ─── config loader ───────────────────────────────────────────────────────────

def test_configured_lib_repository_dir_prefers_new_key(monkeypatch):
    import aidp_config

    monkeypatch.setattr(aidp_config, "load_config", lambda cli_config_path=None: SimpleNamespace(
        paths=SimpleNamespace(
            lib_fallback_dir="/Workspace/team/libs",
            jar_fallback_dir="/Workspace/team/old-jars",
        )
    ))
    assert configured_lib_repository_dir("/Workspace/c.yaml") == "/Workspace/team/libs"


def test_configured_lib_repository_dir_falls_back_to_deprecated_key(monkeypatch):
    import aidp_config

    monkeypatch.setattr(aidp_config, "load_config", lambda cli_config_path=None: SimpleNamespace(
        paths=SimpleNamespace(jar_fallback_dir="/Workspace/team/old-jars")
    ))
    assert configured_lib_repository_dir("/Workspace/c.yaml") == "/Workspace/team/old-jars"


# ─── end-to-end: one real YAML routes the dir to ALL consumers ───────────────
# (No mocked load_config — a real config file through the real loader, for both
# the exporter getter and cluster_lifecycle's module constant.)

def _write_real_yaml(tmp_path, extra_paths_line):
    import pathlib
    fixture = pathlib.Path(__file__).resolve().parent / "config" / "fixtures" / "minimal.yaml"
    text = fixture.read_text().replace(
        "  export_output: /Workspace/test/target",
        "  export_output: /Workspace/test/target\n" + extra_paths_line,
    )
    cfg = tmp_path / "aidp_migrator.yaml"
    cfg.write_text(text)
    (tmp_path / "mapping.json").write_text("{}")
    return cfg


def _cluster_lifecycle_ns():
    """Extract the config-routing surface of cluster_lifecycle via AST (same
    pattern as test_verify_pandas_pin) so the test doesn't import oci/requests.
    Returns the exec namespace with _resolve_lib_dir + configure + the consts
    they derive."""
    import ast
    import os as _os
    import pathlib
    src = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "cluster_lifecycle.py"
    tree = ast.parse(src.read_text())
    wanted_consts = {"AIDP_COMPAT_DEPS_DIR", "AIDP_LIB_DIR", "_BASELINE_JAR_BASENAMES",
                     "BASELINE_JARS", "AIDP_COMPAT_PREFIX"}
    wanted_funcs = {"_resolve_lib_dir", "configure"}
    ns: dict = {"os": _os, "AIDP_COMPAT_VERSION": "0.5.13", "TARGET_WHEEL_PATH": None}
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") in wanted_consts for t in node.targets
        ):
            exec(compile(ast.Module([node], []), str(src), "exec"), ns)
        if isinstance(node, ast.FunctionDef) and node.name in wanted_funcs:
            exec(compile(ast.Module([node], []), str(src), "exec"), ns)
    return ns


def _cluster_lifecycle_resolve_lib_dir():
    return _cluster_lifecycle_ns()["_resolve_lib_dir"]


def test_e2e_yaml_lib_dir_reaches_exporter_and_cluster_lifecycle(tmp_path, monkeypatch):
    cfg = _write_real_yaml(tmp_path, "  lib_repository_dir: /Workspace/team/custom-deps/")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(cfg))

    # Exporter getter — real loader, real file.
    assert configured_lib_repository_dir(str(cfg)) == "/Workspace/team/custom-deps"
    # cluster_lifecycle resolver — the SAME value, so the Phase-2 aidp_compat
    # prerequisite and baseline-jar lookups agree with the export side.
    assert _cluster_lifecycle_resolve_lib_dir()() == "/Workspace/team/custom-deps"


def test_e2e_yaml_deprecated_jar_key_reaches_both_consumers(tmp_path, monkeypatch):
    cfg = _write_real_yaml(tmp_path, "  jar_fallback_dir: /Workspace/team/old-shared-jars")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(cfg))

    assert configured_lib_repository_dir(str(cfg)) == "/Workspace/team/old-shared-jars"
    assert _cluster_lifecycle_resolve_lib_dir()() == "/Workspace/team/old-shared-jars"


def test_e2e_yaml_without_key_uses_default(tmp_path, monkeypatch):
    cfg = _write_real_yaml(tmp_path, "")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(cfg))

    assert configured_lib_repository_dir(str(cfg)) == DEFAULT_AIDP_LIB_DIR
    assert _cluster_lifecycle_resolve_lib_dir()() == DEFAULT_AIDP_LIB_DIR


def test_e2e_cli_config_without_env_reaches_both_consumers(tmp_path, monkeypatch):
    """The actual --config custom-name.yaml workflow: NO $AIDP_CONFIG_PATH, a
    non-discoverable filename — the CLI path alone must reach the exporter
    getter AND cluster_lifecycle (the round-2 review's executed repro)."""
    cfg = _write_real_yaml(tmp_path, "  lib_repository_dir: /Workspace/team/custom-deps/")
    custom = tmp_path / "custom-name.yaml"
    custom.write_text(cfg.read_text())
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)

    assert configured_lib_repository_dir(str(custom)) == "/Workspace/team/custom-deps"
    assert _cluster_lifecycle_resolve_lib_dir()(str(custom)) == "/Workspace/team/custom-deps"


def test_e2e_configure_recomputes_derived_constants(tmp_path, monkeypatch):
    """cluster_lifecycle.configure(--config) must update every derived constant
    (TARGET_WHEEL_PATH / AIDP_COMPAT_PREFIX / BASELINE_JARS), not just the dir."""
    cfg = _write_real_yaml(tmp_path, "  lib_repository_dir: /Workspace/team/custom-deps/")
    custom = tmp_path / "custom-name.yaml"
    custom.write_text(cfg.read_text())
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)

    ns = _cluster_lifecycle_ns()
    ns["configure"](str(custom))
    assert ns["AIDP_LIB_DIR"] == "/Workspace/team/custom-deps"
    assert ns["TARGET_WHEEL_PATH"].startswith("/Workspace/team/custom-deps/aidp_compat-")
    assert ns["AIDP_COMPAT_PREFIX"] == "/Workspace/team/custom-deps/aidp_compat-"
    assert all(j.startswith("/Workspace/team/custom-deps/") for j in ns["BASELINE_JARS"])


def test_explicit_config_errors_are_not_swallowed(tmp_path, monkeypatch):
    """An explicitly-given config that fails to load must RAISE (both the CLI
    path and $AIDP_CONFIG_PATH), never silently fall back to the default."""
    import pytest

    monkeypatch.chdir(tmp_path)  # no discoverable yaml here
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)
    resolve = _cluster_lifecycle_resolve_lib_dir()

    with pytest.raises(Exception):
        resolve(str(tmp_path / "does-not-exist.yaml"))
    monkeypatch.setenv("AIDP_CONFIG_PATH", str(tmp_path / "also-missing.yaml"))
    with pytest.raises(Exception):
        _cluster_lifecycle_resolve_lib_dir()()
    # With nothing explicit at all, the tool default applies (no raise).
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)
    assert resolve() == DEFAULT_AIDP_LIB_DIR


def test_new_key_wins_over_both_deprecated_aliases(monkeypatch):
    import aidp_config

    monkeypatch.setattr(aidp_config, "load_config", lambda cli_config_path=None: SimpleNamespace(
        paths=SimpleNamespace(
            lib_repository_dir="/Workspace/team/repo",
            lib_fallback_dir="/Workspace/team/interim",
            jar_fallback_dir="/Workspace/team/old-jars",
        )
    ))
    assert configured_lib_repository_dir("/Workspace/c.yaml") == "/Workspace/team/repo"


# ─── proxy basic-auth secrets (resolved from YAML refs) ──────────────────────

def _proxy_secret(ref, aidputils):
    """Call _resolve_proxy_secret with a stubbed kernel-aidputils lookup."""
    import dbx_export.auth as auth_mod
    from dbx_export.__main__ import _resolve_proxy_secret
    import unittest.mock as mock

    with mock.patch.object(auth_mod, "_get_aidputils_from_kernel", return_value=aidputils):
        return _resolve_proxy_secret(ref, "dbx_proxy_basic_user")


class _FakeAidputils:
    def __init__(self, values):
        self._values = values
        class _S:
            def __init__(s2): s2.calls = []
        self.secrets = SimpleNamespace(get=self._get)

    def _get(self, name, key=None):
        try:
            return self._values[(name, key)]
        except KeyError:
            raise RuntimeError(f"credential {name}/{key} not found")


def test_proxy_secret_absent_key_is_none():
    assert _proxy_secret(None, None) is None


def test_proxy_secret_plaintext_string_used_as_is():
    assert _proxy_secret("  someuser  ", None) == "someuser"


def test_proxy_secret_ref_resolves_via_aidputils():
    ref = SimpleNamespace(name="dbx_proxy_credential", key="dbx-proxy-basic-user")
    setattr(ref, "from", "aidp_credential_store")
    au = _FakeAidputils({("dbx_proxy_credential", "dbx-proxy-basic-user"): "pilotjobs"})
    assert _proxy_secret(ref, au) == "pilotjobs"


def test_proxy_secret_ref_without_aidputils_warns_and_returns_none(capsys):
    ref = SimpleNamespace(name="dbx_proxy_credential", key="dbx-proxy-basic-user")
    assert _proxy_secret(ref, None) is None
    assert "aidputils is unavailable" in capsys.readouterr().out


def test_proxy_secret_fetch_failure_raises():
    import pytest
    ref = SimpleNamespace(name="missing_credential", key="nope")
    au = _FakeAidputils({})
    with pytest.raises(RuntimeError, match="credential-store fetch failed"):
        _proxy_secret(ref, au)


def test_proxy_secret_empty_value_raises():
    import pytest
    ref = SimpleNamespace(name="dbx_proxy_credential", key="dbx-proxy-basic-user")
    au = _FakeAidputils({("dbx_proxy_credential", "dbx-proxy-basic-user"): "   "})
    with pytest.raises(RuntimeError, match="empty value"):
        _proxy_secret(ref, au)
