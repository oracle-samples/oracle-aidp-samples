"""P1.5ε §Step 3 — dispatch/wheel_builder.py cache-key + build-failure tests."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_autopilot.dispatch.wheel_builder import (
    DispatchWheelBuildError,
    _compute_source_hash,
    build_wheel,
)


def test_wheel_builder_reuses_canonical_dispatch_error() -> None:
    """``dispatch.wheel_builder.DispatchWheelBuildError`` MUST be the same
    class as ``dispatch.errors.DispatchWheelBuildError`` (a DispatchError
    subclass) so the CLI's ``except (DispatchError, OrchestratorConfigError)``
    catch covers wheel-build failures with the stable DISPATCH_* code instead
    of letting a raw RuntimeError escape as a traceback to the operator."""
    from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
        DispatchError,
        DispatchWheelBuildError as CanonicalError,
    )

    assert DispatchWheelBuildError is CanonicalError
    assert issubclass(DispatchWheelBuildError, DispatchError)
    assert DispatchWheelBuildError.code == "DISPATCH_WHEEL_BUILD_FAILED"


def _make_checkout(root: Path) -> Path:
    """Build a minimal plugin-checkout layout the hash function can walk."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "pyproject.toml").write_text("[project]\nname = 'fake'\nversion = '0.1.0'\n")
    pkg = root / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("__version__ = '0.1.0'\n")
    (pkg / "module_a.py").write_text("VALUE = 1\n")
    sub = pkg / "sub"
    sub.mkdir()
    (sub / "module_b.py").write_text("VALUE = 2\n")
    return root


def _make_fake_build(outdir_capture: list[Path]) -> "subprocess.CompletedProcess[str]":
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout="ok", stderr=""
    )


class TestSourceHash:
    def test_stable_across_calls(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path)
        a = _compute_source_hash(checkout)
        b = _compute_source_hash(checkout)
        assert a == b
        assert len(a) == 16

    def test_source_change_changes_hash(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path)
        before = _compute_source_hash(checkout)
        (checkout / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "module_a.py").write_text(
            "VALUE = 99\n"
        )
        after = _compute_source_hash(checkout)
        assert before != after

    def test_pyproject_change_changes_hash(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path)
        before = _compute_source_hash(checkout)
        (checkout / "pyproject.toml").write_text(
            "[project]\nname = 'fake'\nversion = '0.2.0'\n"
        )
        after = _compute_source_hash(checkout)
        assert before != after

    def test_test_file_change_does_not_change_hash(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path)
        tests_dir = checkout / "tests" / "unit"
        tests_dir.mkdir(parents=True)
        (tests_dir / "test_thing.py").write_text("def test_one(): pass\n")
        before = _compute_source_hash(checkout)
        (tests_dir / "test_thing.py").write_text("def test_two(): pass\n")
        after = _compute_source_hash(checkout)
        assert before == after

    def test_pycache_change_does_not_change_hash(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path)
        pycache = checkout / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "__pycache__"
        pycache.mkdir()
        (pycache / "module_a.cpython-313.pyc").write_text("bytecode-stand-in")
        # Hashing skips __pycache__ contents entirely — the file's presence
        # or content can't change the result.
        before = _compute_source_hash(checkout)
        (pycache / "module_a.cpython-313.pyc").write_text("different-bytecode")
        after = _compute_source_hash(checkout)
        assert before == after


class TestBuildWheelCache:
    def _patch_build_subprocess(
        self,
        *,
        rc: int = 0,
        produce_wheel: bool = True,
        stderr: str = "",
    ):
        """Returns a context-manager-like object that fakes
        ``subprocess.run`` and (on success) drops a fake wheel into the
        captured ``outdir``.
        """

        def fake_run(cmd, *, cwd, capture_output, text, timeout, check):
            outdir_str = cmd[cmd.index("--outdir") + 1]
            outdir = Path(outdir_str)
            if rc == 0 and produce_wheel:
                (outdir / "oracle_ai_data_platform_fusion_autopilot-0.1.0-py3-none-any.whl").write_bytes(
                    b"PK\x03\x04 fake wheel bytes"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=rc, stdout="", stderr=stderr
            )

        return patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.wheel_builder.subprocess.run",
            side_effect=fake_run,
        )

    def test_first_build_invokes_subprocess_and_caches(
        self, tmp_path: Path
    ) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess() as mock_run:
            wheel = build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
        assert wheel.exists()
        # Cache layout: <cache_dir>/<hash>/<original-wheel-name>.whl
        # Per-hash subdirectory preserves the PEP-427-conformant wheel
        # filename pip's installer requires; a renamed wheel like
        # `<name>-<hash>.whl` would be rejected with "Invalid wheel
        # filename (wrong number of parts)".
        assert wheel.parent.parent == cache_dir
        assert wheel.name.endswith(".whl")
        assert mock_run.call_count == 1

    def test_second_build_uses_cache(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess() as mock_run:
            build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
            build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
        assert mock_run.call_count == 1

    def test_source_change_invalidates_cache(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess() as mock_run:
            build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
            (checkout / "scripts" / "oracle_ai_data_platform_fusion_autopilot" / "module_a.py").write_text(
                "VALUE = 999\n"
            )
            build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
        assert mock_run.call_count == 2

    def test_force_rebuild_bypasses_cache(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess() as mock_run:
            build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)
            build_wheel(
                plugin_checkout=checkout,
                cache_dir=cache_dir,
                force_rebuild=True,
            )
        assert mock_run.call_count == 2

    def test_build_failure_raises_with_stderr(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess(
            rc=1, produce_wheel=False, stderr="missing module foo"
        ):
            with pytest.raises(DispatchWheelBuildError, match="missing module foo"):
                build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)

    def test_no_wheel_produced_raises(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with self._patch_build_subprocess(rc=0, produce_wheel=False):
            with pytest.raises(DispatchWheelBuildError, match="no .whl found"):
                build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)

    def test_missing_build_package_fails_fast_with_install_hint(
        self, tmp_path: Path
    ) -> None:
        """A vanilla install without `build` must NOT shell out to a
        subprocess that prints `No module named build`. The pre-spawn
        importlib check surfaces a copy-pasteable `pip install build`
        remediation through the canonical DISPATCH_WHEEL_BUILD_FAILED
        code, and `subprocess.run` is never invoked."""
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        with (
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.wheel_builder.importlib.util.find_spec",
                return_value=None,  # simulate `build` not installed
            ),
            patch(
                "oracle_ai_data_platform_fusion_autopilot.dispatch.wheel_builder.subprocess.run",
                side_effect=AssertionError("subprocess.run must not be reached"),
            ),
        ):
            with pytest.raises(DispatchWheelBuildError, match="pip install build"):
                build_wheel(plugin_checkout=checkout, cache_dir=cache_dir)

    def test_log_callback_invoked(self, tmp_path: Path) -> None:
        checkout = _make_checkout(tmp_path / "checkout")
        cache_dir = tmp_path / "cache"
        logs: list[str] = []
        with self._patch_build_subprocess():
            build_wheel(
                plugin_checkout=checkout, cache_dir=cache_dir, log=logs.append
            )
            build_wheel(
                plugin_checkout=checkout, cache_dir=cache_dir, log=logs.append
            )
        # First build: cache miss + cached. Second build: cache hit.
        assert any("cache miss" in m for m in logs)
        assert any("wheel cached" in m for m in logs)
        assert any("cache hit" in m for m in logs)
