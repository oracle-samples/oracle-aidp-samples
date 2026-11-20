import asyncio
from types import SimpleNamespace

import pytest

from scripts import cluster_lifecycle as lifecycle


def test_upgrade_path_materializes_missing_target_runtime_file(
    monkeypatch, tmp_path
):
    source = tmp_path / "source-requirements.txt"
    target = tmp_path / "workspace" / "target-runtime.txt"
    source.write_text("pandas<3\n", encoding="utf-8")
    monkeypatch.setattr(
        lifecycle, "TARGET_RUNTIME_REQUIREMENTS_PATH", str(target)
    )
    monkeypatch.setenv("AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE", str(source))

    lifecycle._ensure_target_runtime_requirements_file()

    assert target.read_text(encoding="utf-8") == "pandas<3\n"


def test_upgrade_path_reads_target_runtime_source_from_yaml(monkeypatch, tmp_path):
    import aidp_config

    source = tmp_path / "yaml-requirements.txt"
    target = tmp_path / "workspace" / "target-runtime.txt"
    source.write_text("typing-extensions==4.16.0\n", encoding="utf-8")
    monkeypatch.setattr(
        lifecycle, "TARGET_RUNTIME_REQUIREMENTS_PATH", str(target)
    )
    monkeypatch.delenv("AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE", raising=False)
    monkeypatch.setattr(
        aidp_config,
        "load_config",
        lambda: SimpleNamespace(paths=SimpleNamespace(
            target_runtime_requirements_file=str(source)
        )),
    )

    lifecycle._ensure_target_runtime_requirements_file()

    assert target.read_text(encoding="utf-8") == (
        "typing-extensions==4.16.0\n"
    )


def test_runtime_append_preserves_existing_constraint():
    original = "# resolved\ntensorflow==2.13.0\ntyping-extensions==4.5.0\n"
    updated, additions = lifecycle._append_missing_requirements(
        original, ["tensorflow", "typing_extensions", "seaborn"]
    )

    assert additions == ["seaborn"]
    assert updated.count("tensorflow") == 1
    assert updated.count("typing-extensions") == 1
    assert updated.endswith("seaborn\n")


def test_cluster_uses_its_single_attached_resolved_file(monkeypatch):
    path = "/Workspace/export/python_dependencies/resolved_requirements_42_cpu.txt"
    monkeypatch.setattr(
        lifecycle,
        "get_installed_libraries",
        lambda _cluster_id: [{"path": path, "status": "INSTALLED"}],
    )

    assert lifecycle._canonical_requirements_path("cluster", "/fallback") == path


def test_multiple_resolved_files_are_rejected(monkeypatch):
    monkeypatch.setattr(
        lifecycle,
        "get_installed_libraries",
        lambda _cluster_id: [
            {"path": "/a/resolved_requirements_one.txt"},
            {"path": "/b/resolved_requirements_two.txt"},
        ],
    )

    try:
        lifecycle._canonical_requirements_path("cluster", "/fallback")
    except RuntimeError as exc:
        assert "multiple resolved requirements" in str(exc)
    else:
        raise AssertionError("expected duplicate resolved files to fail")


def test_pending_canonical_requirements_are_polled_until_installed(monkeypatch):
    path = "/Workspace/export/resolved_requirements_42_cpu.txt"
    states = iter([
        [{"path": path, "status": "PENDING"}],
        [{"path": path, "status": "INSTALLED"}],
    ])
    monkeypatch.setattr(
        lifecycle, "get_installed_libraries", lambda _cluster_id: next(states)
    )

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    restarted = asyncio.run(
        lifecycle.ensure_pinned_requirements_installed("cluster", timeout=15)
    )
    assert restarted is False


def test_install_on_restart_canonical_returns_restart_gate_without_polling(
    monkeypatch,
):
    path = "/Workspace/export/resolved_requirements_42_cpu.txt"
    monkeypatch.setattr(
        lifecycle,
        "get_installed_libraries",
        lambda _cluster_id: [{"path": path, "status": "INSTALL_ON_RESTART"}],
    )

    async def must_not_sleep(_seconds):
        raise AssertionError("INSTALL_ON_RESTART is terminal until restart")

    monkeypatch.setattr(lifecycle.asyncio, "sleep", must_not_sleep)
    assert asyncio.run(
        lifecycle.ensure_pinned_requirements_installed("cluster", timeout=15)
    ) is True


def test_missing_canonical_uses_target_safe_baseline_not_control_requirements(
    monkeypatch,
):
    installed = []
    states = iter([
        [],
        [{"path": lifecycle.TARGET_RUNTIME_REQUIREMENTS_PATH, "status": "INSTALLED"}],
    ])
    monkeypatch.setattr(
        lifecycle, "get_installed_libraries", lambda _cluster_id: next(states)
    )
    monkeypatch.setattr(
        lifecycle,
        "install_library",
        lambda _cluster_id, path: installed.append(path),
    )
    monkeypatch.setattr(
        lifecycle, "_ensure_target_runtime_requirements_file", lambda: None
    )

    async def no_sleep(_seconds):
        return None

    monkeypatch.setattr(lifecycle.asyncio, "sleep", no_sleep)
    restarted = asyncio.run(
        lifecycle.ensure_pinned_requirements_installed("cluster", timeout=15)
    )

    assert installed == [lifecycle.TARGET_RUNTIME_REQUIREMENTS_PATH]
    assert installed[0] != lifecycle.DEFAULT_REQUIREMENTS_PATH
    assert restarted is False
