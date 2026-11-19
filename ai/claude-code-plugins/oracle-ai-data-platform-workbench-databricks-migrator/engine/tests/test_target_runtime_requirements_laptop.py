"""Laptop runners resolve the target-runtime-requirements preflight via API.

Faced 2026-10-13 (EXBRIDGE for-each migration driven from a laptop): the local
`os.path.isfile("/Workspace/...")` check is blind without the cluster FUSE
mount, and the materialize fallback wrote into the laptop filesystem (macOS
read-only root → RuntimeError), although the workspace copy existed on AIDP
and the install step only ever references the WORKSPACE path.
"""
import os
import sys

import pytest

_SCRIPTS = os.path.join(os.path.dirname(__file__), "..", "scripts")
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

import cluster_lifecycle as cl  # noqa: E402


@pytest.fixture(autouse=True)
def _no_local_mount(monkeypatch):
    monkeypatch.setattr(cl.os.path, "isfile", lambda _p: False)
    # Any attempt to materialize locally is the incident behavior.
    monkeypatch.setattr(
        cl.os, "makedirs",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("laptop flow must not write a local /Workspace copy")),
    )


def test_workspace_copy_satisfies_the_preflight_from_a_laptop(monkeypatch):
    monkeypatch.setattr(cl, "_list_workspace_objects", lambda _p: [
        {"path": "/Workspace/migration-dependencies/target_runtime_requirements.txt",
         "type": "FILE"},
    ])
    cl._ensure_target_runtime_requirements_file()  # returns, no local write


def test_missing_workspace_copy_still_fails_loudly(monkeypatch):
    monkeypatch.setattr(cl, "_list_workspace_objects", lambda _p: [])
    monkeypatch.setattr(cl.os.environ, "get", lambda *_a: "")
    with pytest.raises(RuntimeError, match="setup_migration_cluster|materialize|missing"):
        cl._ensure_target_runtime_requirements_file()
