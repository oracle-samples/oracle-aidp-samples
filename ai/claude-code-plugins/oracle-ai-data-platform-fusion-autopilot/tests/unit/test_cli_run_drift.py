"""CLI-level tests for the Phase 3c drift gate.

Round-2 owner-fix invariants:

* Drift maps to exit code 14 (EXIT_CODE_SCHEMA_DRIFT).
* Hand-off message lands on STDERR (not stdout) via the dedicated
  `error_console = Console(stderr=True)` — round-4 finding.
* The drift artifact's `run_id` == the run's `run_id` (set by
  `_run_content_pack_backend`'s mint, which the orchestrator passes
  into the SchemaDriftDetectedError).
* On drift: no state rows written.
* `--force-fingerprint-skip` writes the audit row + proceeds.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot import orchestrator
from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
    EXIT_CODE_SCHEMA_DRIFT,
    SchemaDriftDetectedError,
)


# ---------------------------------------------------------------------------
# Direct unit tests on the SchemaDriftDetectedError wiring
# ---------------------------------------------------------------------------


class TestExitCodeMapping:
    def test_exit_code_constant_is_14(self) -> None:
        """Reserved exit codes Phase 3a/3b/3c: 11 / 12 / 13 / 14."""
        assert EXIT_CODE_SCHEMA_DRIFT == 14

    def test_schema_drift_detected_error_carries_run_id(self) -> None:
        """Round-2 audit-correlation: error's run_id matches what
        _run_content_pack_backend will have minted."""
        exc = SchemaDriftDetectedError(
            run_id="cp-20260606120000-abcdef12",
            diagnostic_path=Path("/tmp/.aidp/diagnostics/cp-x/AIDPF-2012.json"),
            summary="drift hand-off",
            prior_fingerprint="sha256:" + "a" * 64,
            current_fingerprint="sha256:" + "b" * 64,
        )
        assert exc.run_id == "cp-20260606120000-abcdef12"
        assert "AIDPF-2012" in str(exc)

    def test_schema_drift_detected_error_does_not_inherit_config_error(self) -> None:
        """CRITICAL: must NOT inherit from OrchestratorConfigError —
        otherwise the existing CLI catch arm at commands/run.py:252
        would swallow it and return exit 2 instead of 14."""
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            OrchestratorConfigError,
        )
        assert not issubclass(SchemaDriftDetectedError, OrchestratorConfigError)


# ---------------------------------------------------------------------------
# CLI-level integration via _run_inline
# ---------------------------------------------------------------------------


def _make_bundle_dir(tmp_path: Path) -> Path:
    """Minimal bundle.yaml + aidp.config.yaml for the CLI to load."""
    bundle = tmp_path / "bundle.yaml"
    bundle.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "aidp-fusion-autopilot/v1",
                "version": "0.2.0",
                "project": "test",
                "fusion": {
                    "serviceUrl": "https://example.invalid",
                    "username": "stub",
                    "password": "stub",
                    "externalStorage": "stub",
                },
                "aidp": {
                    "catalog": "cat",
                    "bronzeSchema": "bronze",
                    "silverSchema": "silver",
                    "goldSchema": "gold",
                    "storageFormat": "delta",
                },
                "datasets": [{"id": "erp_suppliers", "mode": "full"}],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    config = tmp_path / "aidp.config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "aidp-fusion-autopilot/v1",
                "project": "test",
                "environments": {
                    "dev": {
                        "workspaceKey": "stub",
                        "dataLakeOcid": "ocid1.placeholder",
                        "region": "us-ashburn-1",
                        "ociProfile": "DEFAULT",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return tmp_path


class TestRunInlineDriftCatch:
    """`_run_inline` MUST catch SchemaDriftDetectedError and return
    exit 14 via the new arm, BEFORE the existing OrchestratorConfigError
    arm."""

    def test_drift_returns_exit_14(self, tmp_path: Path) -> None:
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import _run_inline

        bundle_dir = _make_bundle_dir(tmp_path)
        console = Console()

        def _raise_drift(**kwargs):
            raise SchemaDriftDetectedError(
                run_id="cp-test-drift-1",
                diagnostic_path=tmp_path / ".aidp" / "diagnostics" / "cp-test-drift-1" / "AIDPF-2012.json",
                summary="drift hand-off message",
                prior_fingerprint="sha256:" + "a" * 64,
                current_fingerprint="sha256:" + "b" * 64,
            )

        with patch.object(orchestrator, "run", side_effect=_raise_drift):
            exit_code = _run_inline(
                bundle_dir / "bundle.yaml",
                mode="incremental",
                datasets=None,
                layers=None,
                resume_run_id=None,
                dry_run=False,
                console=console,  # avoid the content-pack pack-load
                force_fingerprint_skip=False,
            )
        assert exit_code == EXIT_CODE_SCHEMA_DRIFT
        assert exit_code == 14

    def test_drift_catch_arm_precedes_config_error(self, tmp_path: Path) -> None:
        """If SchemaDriftDetectedError inherited from
        OrchestratorConfigError or the catch arm was placed AFTER
        the existing one, drift would surface as exit 2. Verify exit
        14 vs 2 distinction."""
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import _run_inline
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            OrchestratorConfigError,
        )

        bundle_dir = _make_bundle_dir(tmp_path)
        console = Console()

        # OrchestratorConfigError → exit 2.
        with patch.object(
            orchestrator, "run",
            side_effect=OrchestratorConfigError("test config error"),
        ):
            assert _run_inline(
                bundle_dir / "bundle.yaml",
                mode="incremental",
                datasets=None, layers=None,
                resume_run_id=None, dry_run=False,
                console=console,
            ) == 2

        # SchemaDriftDetectedError → exit 14 (NOT 2).
        with patch.object(
            orchestrator, "run",
            side_effect=SchemaDriftDetectedError(
                run_id="r",
                diagnostic_path=Path("/tmp/x.json"),
                summary="drift",
                prior_fingerprint="p",
                current_fingerprint="c",
            ),
        ):
            assert _run_inline(
                bundle_dir / "bundle.yaml",
                mode="incremental",
                datasets=None, layers=None,
                resume_run_id=None, dry_run=False,
                console=console,
            ) == 14


class TestForceFingerprintSkipFlag:
    def test_flag_accepted_by_click_group(self) -> None:
        """Click's --force-fingerprint-skip flag is wired + flows
        through to commands.run.run."""
        from click.testing import CliRunner
        from oracle_ai_data_platform_fusion_autopilot.cli import main

        runner = CliRunner()
        # --help shows the flag is accepted (hidden=True keeps it out
        # of --help output, but parser still accepts it). We probe by
        # checking --version succeeds with the flag passed via a
        # subcommand that rejects unknown flags.
        result = runner.invoke(main, ["run", "--force-fingerprint-skip", "--help"])
        # --help short-circuits before any validation; exit 0 means
        # the flag was accepted by the parser.
        assert result.exit_code == 0

    def test_flag_threaded_to_orchestrator_run(self, tmp_path: Path) -> None:
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import _run_inline

        bundle_dir = _make_bundle_dir(tmp_path)
        console = Console()

        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
                RunSummary,
            )
            return RunSummary.empty(bundle_project="test", mode=kwargs.get("mode", "seed"))

        with patch.object(orchestrator, "run", side_effect=_capture):
            _run_inline(
                bundle_dir / "bundle.yaml",
                mode="incremental",
                datasets=None, layers=None,
                resume_run_id=None, dry_run=False,
                console=console,
                force_fingerprint_skip=True,
            )
        assert captured.get("force_fingerprint_skip") is True
