"""Explicit pip cells keep distribution semantics without a workspace scan."""
import asyncio
from types import SimpleNamespace

import pytest

from test_pip_magic_gate import jm, cluster_lifecycle as lifecycle


@pytest.fixture(autouse=True)
def configured_migration_output(monkeypatch):
    # Model the public runner's configured output, not pre-main module state.
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_output")


@pytest.mark.parametrize("package,variable", [("xgboost", "xgb_version"), ("lightgbm", "lgb_version")])
def test_dynamic_version_restores_package_only_migration(package, variable):
    from pip_commands import parse_pip_arguments

    spec = f"{package}=={{{variable}}}"
    # Export remains strict: this opt-in belongs to the migration gate only.
    assert parse_pip_arguments(spec).unresolved
    notices = []
    assert jm._parse_pip_args(spec + " --quiet", notices=notices) == ([package], [])
    assert any(spec in notice and "version is not enforced" in notice for notice in notices)


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("outcome", [True, False])
def test_dynamic_version_uses_existing_requirements_installer(monkeypatch, cached, outcome):
    installs = []

    async def install(cluster, session, requirement, output, **kwargs):
        installs.append(requirement)
        return outcome

    monkeypatch.setattr(lifecycle, "install_missing_package", install)
    failed = asyncio.run(jm._resolve_pip_cell_packages(
        ["xgboost"], SimpleNamespace(cluster_id="test"), "job",
        {"xgboost"} if cached else set(), SimpleNamespace(log=lambda message: None),
        "%pip install xgboost=={xgb_version} --quiet"))
    assert installs == ([] if cached else ["xgboost"])
    assert failed == ([] if cached or outcome else ["xgboost"])


@pytest.mark.parametrize("spec", ["{pkg}", "xgboost=={versions[0]}", "xgboost=={get_version()}",
                                  "xgboost[extra]=={version}", "https://host/{wheel}.whl",
                                  "invalid-=={version}", "pip=={version}"])
def test_dynamic_version_compatibility_does_not_guess_other_forms(spec):
    packages, unresolved = jm._parse_pip_args(spec)
    assert not packages
    assert unresolved


def test_dynamic_version_does_not_strip_hash_integrity_requirement():
    packages, unresolved = jm._parse_pip_args("xgboost=={xgb_version} --require-hashes")
    assert not packages
    assert any("unhashed installation not attempted" in item for item in unresolved)


def test_dynamic_version_parent_preserves_notice_and_following_code(monkeypatch, tmp_path):
    from test_pip_magic_gate import _drive

    result, installs, saved, saved_src, executed = _drive(monkeypatch, tmp_path, [
        "%pip install xgboost=={xgb_version} --quiet\nSENTINEL = 42"])
    assert result["status"] == "PASS", result
    assert installs == ["xgboost"]
    assert "version is not enforced" in saved_src
    assert "%pip install xgboost=={xgb_version}" in saved_src
    assert "SENTINEL = 42" in executed


def test_dynamic_version_child_uses_same_compatibility(monkeypatch):
    from test_pip_magic_gate import _drive_child

    result, installs = _drive_child(monkeypatch,
        "%pip install xgboost=={xgb_version} --quiet\nSENTINEL = 42", True, True)
    assert result["status"] == "ok", result
    assert installs == ["xgboost"]
    assert not result.get("unresolved_pip")


def test_dynamic_version_code_only_does_not_install(monkeypatch, tmp_path):
    from test_pip_magic_gate import _drive_strict, _saved_notebook

    kernel, installs, coro = _drive_strict(monkeypatch, tmp_path,
        ["%pip install xgboost=={xgb_version} --quiet"], True, run_all=False)
    result = asyncio.run(coro)
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    assert installs == []
    source = "\n".join("".join(cell["source"]) for cell in _saved_notebook(kernel)["cells"])
    assert "version is not enforced" in source
    assert not jm._find_pip_installs(source)[0]


@pytest.mark.parametrize("package", ["pytest", "pr105_missing_fixture"])
def test_dynamic_version_persists_bare_requirement_through_real_installer(monkeypatch, tmp_path, package):
    from test_customization_pack import _ExecSession, _pip_env, _req_file

    _, installs = _pip_env(monkeypatch)
    session = _ExecSession()
    session.cluster_id = "test"
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", tmp_path.as_posix())
    failed = asyncio.run(jm._resolve_pip_cell_packages(
        [package], session, "job", set(), SimpleNamespace(log=lambda message: None),
        f"%pip install {package}=={{requested_version}}"))
    assert failed == []
    assert len(installs) == (0 if package == "pytest" else 1)
    assert package in _req_file(tmp_path / "job").read_text().splitlines()
    assert "requested_version" not in _req_file(tmp_path / "job").read_text()


@pytest.mark.parametrize("package", ["prophet", "requests", "mlflow", "typing_extensions", "slack_sdk", "pulp"])
@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize("outcome", [True, False, RuntimeError("install rejected")])
def test_explicit_packages_use_existing_installer_without_source_scan(monkeypatch, package, cached, outcome):
    installs, messages, scans = [], [], []

    async def discover(*args, **kwargs):
        scans.append(args)
        raise RuntimeError("local-module scan parse failed: no local-module scan result")

    async def install(cluster, session, requirement, output, **kwargs):
        assert not kwargs.get("notebook_paths")
        installs.append(requirement)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(lifecycle, "_discover_local_modules", discover)
    monkeypatch.setattr(lifecycle, "install_missing_package", install)
    installed = {package} if cached else set()
    failed = asyncio.run(jm._resolve_pip_cell_packages(
        [package], SimpleNamespace(cluster_id="test"), "job", installed,
        SimpleNamespace(log=messages.append), f"# old install\n!pip install {package}"))
    assert scans == []
    assert installs == ([] if cached else [package])
    assert failed == ([] if cached or outcome is True else [package])
    assert (package in installed) == (cached or outcome is True)


@pytest.mark.parametrize("separator", ["", "\v", "\f", "\r", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"])
def test_version_constraints_keep_physical_line_indices(monkeypatch, separator):
    source = (f"# context{separator}\n%pip install first_pkg==1.0 second_pkg>=2\n"
              f"# more{separator}\n!pip install third_pkg==3.0")
    installs = []

    async def install(cluster, session, spec, output, **kwargs):
        installs.append(spec)
        return True

    monkeypatch.setattr(lifecycle, "install_missing_package", install)
    failed = asyncio.run(jm._resolve_pip_cell_packages(
        jm._find_pip_installs(source)[1], SimpleNamespace(cluster_id="test"), "job", set(),
        SimpleNamespace(log=lambda message: None), source))
    assert failed == []
    assert installs == ["first_pkg==1.0", "second_pkg>=2", "third_pkg==3.0"]
