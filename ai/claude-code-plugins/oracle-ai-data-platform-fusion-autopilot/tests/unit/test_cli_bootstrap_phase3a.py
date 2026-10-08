"""CliRunner tests for the Phase 3a `bootstrap` click entry point.

Drives ``aidp-fusion-autopilot bootstrap`` through ``CliRunner.invoke()``
to validate the click flag surface + the path that produces an exit
code (the rest of the variation-phase coverage lives in
:mod:`test_bootstrap_variation_phase` + :mod:`test_bootstrap_refresh_drift`).

The phase-1 probes (BICC + AIDP REST) are skipped via
``--skip-preonboarding-probes`` so the test doesn't need a live pod.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from oracle_ai_data_platform_fusion_autopilot.cli import main


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


DEMO_POD_BRONZE: dict[str, list[str]] = {
    "erp_suppliers": ["VENDORID", "SEGMENT1"],
    "ap_invoices": ["ApInvoicesInvoiceCurrencyCode", "ApInvoicesCancelledDate"],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
    ],
    "gl_period_balances": ["PeriodNetCredit"],
}


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark() -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = DEMO_POD_BRONZE.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


@pytest.fixture
def bundle_files(tmp_path: Path) -> tuple[Path, Path]:
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
                "datasets": [
                    {"id": "erp_suppliers", "mode": "full"},
                    {"id": "ap_invoices", "mode": "incremental"},
                    {"id": "gl_coa", "mode": "full"},
                    {"id": "gl_period_balances", "mode": "full"},
                ],
                "contentPack": {
                    "name": "fusion-finance-starter",
                    "path": str(PACK_ROOT),
                    "profile": "finance-default",
                },
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
                        "workspaceKey": "stub-workspace",
                        "dataLakeOcid": "ocid1.placeholder",
                        "region": "us-ashburn-1",
                        "ociProfile": "DEFAULT",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return bundle, config


class TestClickEntryPoint:
    def test_help_lists_new_flags(self) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["bootstrap", "--help"])
        assert result.exit_code == 0
        assert "--refresh" in result.output
        assert "--operator" in result.output
        assert "--non-interactive" in result.output
        assert "--resolutions" in result.output
        assert "--skip-preonboarding-probes" in result.output
        # Phase 4.1 / D3 — cluster-dispatch flags ship in --help.
        assert "--dispatch-mode" in result.output
        assert "--cluster-key" in result.output
        assert "--cluster-name" in result.output
        assert "--workspace-dir" in result.output

    def test_happy_path_writes_profile_and_evidence(
        self,
        bundle_files: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle, config = bundle_files

        # Patch Spark acquisition to return our mock.
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.variation_phase._acquire_local_spark",
            return_value=_mock_spark(),
        ):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--bundle", str(bundle),
                    "--config", str(config),
                    "bootstrap",
                    # Phase 4.1: explicit opt-in to laptop-Spark mode
                    # — the CLI default flipped to `--dispatch-mode=cluster`
                    # per Decision 2; this test uses a mocked local Spark
                    # session, so it stays on the local path.
                    "--dispatch-mode", "local",
                    "--skip-preonboarding-probes",
                    "--non-interactive",
                    # Conventional shipped pack, non-interactive CI bootstrap:
                    # accept the COA convention (else COA resolver fails closed).
                    "--accept-coa-convention",
                ],
            )
        assert result.exit_code == 0, result.output
        profile = bundle.parent / "profiles" / "finance-default.yaml"
        evidence_dir = bundle.parent / "evidence" / "finance-default"
        assert profile.exists()
        assert evidence_dir.exists()
        loaded = yaml.safe_load(profile.read_text(encoding="utf-8"))
        assert loaded["resolved"]["column"]["supplier_natural_key"] == "SEGMENT1"
        assert loaded["resolved"]["semantic"]["cancelled_status"] == "cancelled_date"

    def test_missing_identity_exits_one(
        self,
        bundle_files: tuple[Path, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.delenv("USER", raising=False)
        bundle, config = bundle_files

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.variation_phase._acquire_local_spark",
            return_value=_mock_spark(),
        ):
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "--bundle", str(bundle),
                    "--config", str(config),
                    "bootstrap",
                    # Phase 4.1: explicit opt-in to laptop-Spark mode
                    # — the CLI default flipped to `--dispatch-mode=cluster`
                    # per Decision 2; this test uses a mocked local Spark
                    # session, so it stays on the local path.
                    "--dispatch-mode", "local",
                    "--skip-preonboarding-probes",
                    "--non-interactive",
                ],
            )
        assert result.exit_code == 1
        diag = bundle.parent / ".aidp" / "diagnostics"
        # exactly one run directory was created, containing the 1020 artifact.
        run_dirs = list(diag.iterdir()) if diag.exists() else []
        assert len(run_dirs) == 1
        assert (run_dirs[0] / "AIDPF-1020.json").exists()
