"""Unit tests for the self-bootstrapping entry point (``__main__.py``).

Covers the plan's Testing Strategy at unit speed (mocked pip, no real install):
version gating, ABI/platform key isolation, atomic partial-write recovery,
install-via-sys.executable, child-process PYTHONPATH propagation, the
double-checked lock, and the manual-command form. The real cold-start install is
exercised by tests/integration/test_bootstrap_cold_start.py (gated).
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading

import pytest

from oracle_ai_data_platform_fusion_autopilot import __main__ as boot


@pytest.fixture
def req_file(tmp_path):
    """A throwaway requirements.txt; point the module at it."""
    f = tmp_path / "requirements.txt"
    f.write_text("click>=8.0\npydantic>=2.0\n", encoding="utf-8")
    return f


# --- version gating --------------------------------------------------------

def test_require_min_python_raises_below_floor(monkeypatch):
    monkeypatch.setattr(boot, "_MIN_PYTHON", (99, 0))
    with pytest.raises(SystemExit) as ei:
        boot._require_min_python()
    assert ei.value.code == 1


def test_require_min_python_passes_on_current(monkeypatch):
    monkeypatch.setattr(boot, "_MIN_PYTHON", (3, 10))
    boot._require_min_python()  # no raise


# --- deps key: ABI / platform / requirements isolation ---------------------

def test_key_changes_with_abi_platform_and_requirements(monkeypatch, req_file):
    monkeypatch.setattr(boot, "_REQUIREMENTS", req_file)
    monkeypatch.setattr(boot.sys.implementation, "cache_tag", "cpython-310", raising=False)
    monkeypatch.setattr(boot.sys, "platform", "linux")
    monkeypatch.setattr(boot.platform, "machine", lambda: "x86_64")
    base = boot._deps_key("0.1.0a0")

    # Different ABI -> different dir (prevents loading cp310 wheels under cp312).
    monkeypatch.setattr(boot.sys.implementation, "cache_tag", "cpython-312", raising=False)
    assert boot._deps_key("0.1.0a0") != base

    # Different platform -> different dir.
    monkeypatch.setattr(boot.sys.implementation, "cache_tag", "cpython-310", raising=False)
    monkeypatch.setattr(boot.sys, "platform", "darwin")
    assert boot._deps_key("0.1.0a0") != base

    # Different requirements content -> different dir.
    monkeypatch.setattr(boot.sys, "platform", "linux")
    req_file.write_text("click>=8.0\npydantic>=2.0\nrich>=13.0\n", encoding="utf-8")
    assert boot._deps_key("0.1.0a0") != base


def test_key_stable_for_same_inputs(monkeypatch, req_file):
    monkeypatch.setattr(boot, "_REQUIREMENTS", req_file)
    assert boot._deps_key("0.1.0a0") == boot._deps_key("0.1.0a0")


# --- child-process visibility (PYTHONPATH propagation) ---------------------

def test_activate_deps_sets_syspath_and_pythonpath(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delenv("PYTHONPATH", raising=False)
    deps = tmp_path / "deps"
    deps.mkdir()
    boot._activate_deps(deps)
    assert sys.path[0] == str(deps)
    entries = os.environ["PYTHONPATH"].split(os.pathsep)
    assert str(deps) in entries
    assert str(boot._SCRIPTS_DIR) in entries  # so `python -m build` children see deps


def test_deps_reach_child_processes(monkeypatch, tmp_path):
    """BLOCKING: a child process (like `python -m build`) must see the deps dir.

    `sys.path.insert` alone does NOT propagate to children; only
    `os.environ["PYTHONPATH"]` does. Put a dummy module in the deps dir and
    confirm a fresh subprocess imports it via the inherited env.
    """
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delenv("PYTHONPATH", raising=False)
    deps = tmp_path / "deps"
    (deps / "fakebuild").mkdir(parents=True)
    (deps / "fakebuild" / "__init__.py").write_text("VALUE = 'from-deps'\n")

    boot._activate_deps(deps)

    # Child inherits os.environ (incl. PYTHONPATH) -> import succeeds.
    ok = subprocess.run(
        [sys.executable, "-c", "import fakebuild; print(fakebuild.VALUE)"],
        cwd=str(tmp_path.parent), env={**os.environ}, capture_output=True, text=True,
    )
    assert ok.returncode == 0, ok.stderr
    assert "from-deps" in ok.stdout

    # Regression guard: WITHOUT the PYTHONPATH entry the child cannot find it.
    clean_env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    missing = subprocess.run(
        [sys.executable, "-c", "import fakebuild"],
        cwd=str(tmp_path.parent), env=clean_env, capture_output=True, text=True,
    )
    assert missing.returncode != 0
    assert "No module named" in missing.stderr


# --- install uses the selected interpreter, never bare pip -----------------

def test_install_invokes_sys_executable_not_bare_pip(monkeypatch, tmp_path, req_file):
    monkeypatch.setattr(boot, "_REQUIREMENTS", req_file)
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if "pip" in argv:  # the install
            target = Pathlike_target(argv)
            (target / "marker.txt").write_text("x")
            return _Proc(0)
        return _Proc(0)  # the validate `-c import pydantic`

    monkeypatch.setattr(boot.subprocess, "run", fake_run)
    deps = tmp_path / "pylib" / "key"
    boot._atomic_install(deps)

    install_argv = next(a for a in calls if "pip" in a)
    assert install_argv[0] == sys.executable
    assert install_argv[1:3] == ["-m", "pip"]


def test_manual_command_uses_python_m_pip(tmp_path):
    cmd = boot._manual_command(tmp_path / "deps")
    assert sys.executable in cmd
    assert "-m pip install" in cmd
    assert not cmd.lstrip().startswith("pip ")


# --- atomic install / partial-write recovery -------------------------------

def test_partial_write_then_failure_does_not_promote(monkeypatch, tmp_path, req_file):
    monkeypatch.setattr(boot, "_REQUIREMENTS", req_file)

    def fake_run(argv, **kwargs):
        if "pip" in argv:
            target = Pathlike_target(argv)
            (target / "half.txt").write_text("partial")  # partial write
            return _Proc(1)  # then fail
        return _Proc(0)

    monkeypatch.setattr(boot.subprocess, "run", fake_run)
    deps = tmp_path / "pylib" / "key"
    with pytest.raises(RuntimeError):
        boot._atomic_install(deps)
    assert not deps.exists()  # not promoted
    assert not (deps / ".complete").exists()
    # temp dir cleaned
    leftovers = list((tmp_path / "pylib").glob(".tmp-*"))
    assert leftovers == []


def test_second_run_rebuilds_cleanly(monkeypatch, tmp_path, req_file):
    monkeypatch.setattr(boot, "_REQUIREMENTS", req_file)

    def good_run(argv, **kwargs):
        if "pip" in argv:
            target = Pathlike_target(argv)
            (target / "pkg.txt").write_text("ok")
            return _Proc(0)
        return _Proc(0)

    monkeypatch.setattr(boot.subprocess, "run", good_run)
    deps = tmp_path / "pylib" / "key"
    boot._atomic_install(deps)
    assert (deps / ".complete").exists()
    assert (deps / "pkg.txt").exists()


# --- double-checked lock / concurrency -------------------------------------

def test_ensure_deps_fast_path_skips_install(monkeypatch, tmp_path):
    deps = tmp_path / "pylib" / "key"
    deps.mkdir(parents=True)
    (deps / ".complete").write_text("done")
    called = []
    monkeypatch.setattr(boot, "_atomic_install", lambda d: called.append(d))
    boot._ensure_deps(deps)
    assert called == []  # marker present -> no install


def test_concurrent_cold_start_installs_once(monkeypatch, tmp_path):
    deps = tmp_path / "pylib" / "key"
    counter = {"n": 0}
    lock = threading.Lock()

    def fake_install(d):
        with lock:
            counter["n"] += 1
        d.mkdir(parents=True, exist_ok=True)
        (d / ".complete").write_text("done")

    monkeypatch.setattr(boot, "_atomic_install", fake_install)

    threads = [threading.Thread(target=boot._ensure_deps, args=(deps,)) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert counter["n"] == 1  # the file lock + double-check serialized them
    assert (deps / ".complete").exists()


def test_validate_deps_rejects_when_sentinel_not_from_target(tmp_path):
    """An empty target must NOT validate just because pydantic is global.

    pydantic is importable in the test environment, but PYTHONPATH only prepends
    the (empty) target, so a naive `import pydantic` would succeed from
    site-packages. The __file__-under-target check must reject it.
    """
    empty = tmp_path / "empty"
    empty.mkdir()
    assert boot._validate_deps(empty) is False


def test_validate_deps_accepts_sentinel_from_target(tmp_path):
    target = tmp_path / "t"
    (target / "pydantic").mkdir(parents=True)
    (target / "pydantic" / "__init__.py").write_text("")  # sentinel lives in target
    assert boot._validate_deps(target) is True


def test_ensure_deps_adopts_manual_install_without_marker(monkeypatch, tmp_path):
    """Manually-installed deps (no .complete) must be adopted, not reinstalled.

    The printed manual fallback (`<py> -m pip install --target <key dir> ...`)
    populates the keyed dir but does not write .complete. A later run must adopt
    it instead of trying to auto-install again.
    """
    deps = tmp_path / "pylib" / "key"
    deps.mkdir(parents=True)
    (deps / "somepkg.txt").write_text("manually installed")  # populated, no marker

    monkeypatch.setattr(boot, "_validate_deps", lambda d: True)  # pretend it imports
    reinstalled = []
    monkeypatch.setattr(boot, "_atomic_install", lambda d: reinstalled.append(d))

    boot._ensure_deps(deps)

    assert reinstalled == []  # adopted, not reinstalled
    assert (deps / ".complete").exists()  # marker now written


def test_ensure_deps_does_not_adopt_invalid_dir(monkeypatch, tmp_path):
    """A populated-but-broken dir (validation fails) must NOT be adopted."""
    deps = tmp_path / "pylib" / "key"
    deps.mkdir(parents=True)
    (deps / "half.txt").write_text("partial")

    monkeypatch.setattr(boot, "_validate_deps", lambda d: False)  # broken
    reinstalled = []
    monkeypatch.setattr(boot, "_atomic_install", lambda d: reinstalled.append(d))

    boot._ensure_deps(deps)

    assert reinstalled == [deps]  # fell through to a real install
    assert not (deps / ".complete").exists()


def _inject_fake_cli(monkeypatch):
    """Replace `.cli` with a stub so main() needs no real runtime deps."""
    import types

    called = {}
    fake = types.ModuleType("oracle_ai_data_platform_fusion_autopilot.cli")
    fake.main = lambda **kw: called.update(kw) or called.setdefault("_ran", True)
    monkeypatch.setitem(sys.modules, "oracle_ai_data_platform_fusion_autopilot.cli", fake)
    return called


def test_main_bypasses_bootstrap_in_wheel_mode(monkeypatch, tmp_path):
    """Installed-wheel mode (no requirements.txt) must NOT bootstrap; deps are
    already pip-installed. Dep-free: stubs the CLI so it runs on any interpreter.
    """
    monkeypatch.setattr(boot, "_REQUIREMENTS", tmp_path / "nope.txt")  # absent

    def boom(*a, **k):
        raise AssertionError("bootstrap must not run in wheel mode")

    monkeypatch.setattr(boot, "_ensure_deps", boom)
    monkeypatch.setattr(boot, "_deps_key", boom)
    monkeypatch.setattr(boot, "_activate_deps", boom)
    called = _inject_fake_cli(monkeypatch)

    boot.main()

    assert called.get("_ran") is True
    assert called.get("prog_name") == "aidp-fusion-autopilot"


def test_main_bootstraps_in_source_mode(monkeypatch, tmp_path):
    """Run-from-source mode (requirements.txt present) DOES bootstrap."""
    req = tmp_path / "requirements.txt"
    req.write_text("click>=8.0\n")
    monkeypatch.setattr(boot, "_REQUIREMENTS", req)

    ensured = []
    activated = []
    monkeypatch.setattr(boot, "_ensure_deps", lambda d: ensured.append(d))
    monkeypatch.setattr(boot, "_activate_deps", lambda d: activated.append(d))
    called = _inject_fake_cli(monkeypatch)

    boot.main()

    assert ensured and activated  # bootstrap path taken
    assert called.get("_ran") is True


def test_no_autoinstall_skips_install(monkeypatch, tmp_path):
    monkeypatch.setattr(boot, "_NO_AUTOINSTALL", True)
    deps = tmp_path / "pylib" / "key"
    called = []
    monkeypatch.setattr(boot, "_atomic_install", lambda d: called.append(d))
    boot._ensure_deps(deps)
    assert called == []  # opted out -> never installs


# --- helpers ---------------------------------------------------------------

class _Proc:
    def __init__(self, rc):
        self.returncode = rc


def Pathlike_target(argv):
    """Extract the --target dir from a faked pip argv and ensure it exists."""
    from pathlib import Path

    target = Path(argv[argv.index("--target") + 1])
    target.mkdir(parents=True, exist_ok=True)
    return target
