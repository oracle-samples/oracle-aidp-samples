"""Unit tests for the new orchestration CLI command bodies."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from oracle_ai_data_platform_fusion_autopilot import cli

# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


class TestInit:
    def test_default_template_is_current_full_starter_bundle(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli.main, ["init"])
        assert result.exit_code == 0

        bundle = (tmp_path / "bundle.yaml").read_text(encoding="utf-8")
        config = (tmp_path / "aidp.config.yaml").read_text(encoding="utf-8")
        assert "contentPack:" in bundle
        assert "fusion-finance-starter" in bundle
        assert "supplier_spend" in bundle
        assert "ap_aging" in bundle
        assert "gl_balance" in bundle
        assert "dim_account" in bundle
        assert "gl_period_balances" in bundle
        assert "project: my-fusion-lake" in config

    def test_writes_minimal_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli.main, ["init", "--template", "minimal"])
        assert result.exit_code == 0
        assert (tmp_path / "bundle.yaml").exists()
        assert (tmp_path / "aidp.config.yaml").exists()

    def test_refuses_overwrite_without_force(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "bundle.yaml").write_text("existing")
        result = CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        assert result.exit_code == 1
        assert (tmp_path / "bundle.yaml").read_text() == "existing"

    def test_force_overwrites(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "bundle.yaml").write_text("existing")
        result = CliRunner().invoke(cli.main, ["init", "--template", "minimal", "--force"])
        assert result.exit_code == 0
        assert "existing" not in (tmp_path / "bundle.yaml").read_text()


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


class TestValidate:
    def test_default_init_template_validates(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        init_result = CliRunner().invoke(cli.main, ["init"])
        assert init_result.exit_code == 0

        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 0
        assert "validation passed" in result.output

    def test_passes_for_minimal_template(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 0
        assert "validation passed" in result.output

    def test_fails_when_bundle_missing(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 1

    def test_fails_for_unknown_dataset_id(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        bundle = tmp_path / "bundle.yaml"
        text = bundle.read_text(encoding="utf-8")
        # swap one dataset id to an unknown one. encoding="utf-8" is load-
        # bearing: the template carries an em-dash; without it write_text uses
        # the platform default (cp1252 on Windows), and the subsequent UTF-8
        # read in validate raises UnicodeDecodeError.
        bundle.write_text(
            text.replace("gl_journal_lines", "definitely_not_in_catalog"),
            encoding="utf-8",
        )
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 1
        assert "definitely_not_in_catalog" in result.output

    def test_fails_when_declared_contentpack_path_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Round-9 review fix: a bundle that DECLARES contentPack
        but whose path doesn't resolve must fail validate with
        AIDPF-1037/AIDPF-1038, NOT silently fall back to the legacy
        fusion_catalog membership check. Pre-fix the legacy fallback
        gave a false-green here while the run command would later
        reject the bundle with the same code.
        """
        monkeypatch.chdir(tmp_path)
        # erp_suppliers IS in fusion_catalog.CATALOG, so the legacy
        # fallback would silently pass. The point of this test is
        # that declaring contentPack with a bad path must surface
        # the AIDPF-1038 error instead of falling back.
        (tmp_path / "bundle.yaml").write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: validate-bad-pack\n"
            "fusion:\n"
            "  serviceUrl: https://example.com\n"
            "  username: u\n  password: p\n  externalStorage: x\n"
            "aidp:\n"
            "  catalog: fusion_catalog\n"
            "  bronzeSchema: bronze\n  silverSchema: silver\n  goldSchema: gold\n"
            "contentPack:\n"
            "  name: fusion-finance-starter\n"
            "  path: ./does-not-exist\n"
            "  profile: demo\n"
            "datasets:\n"
            "  - id: erp_suppliers\n"
        )
        (tmp_path / "aidp.config.yaml").write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: validate-bad-pack\n"
            "environments:\n"
            "  dev:\n"
            "    workspaceKey: ws\n"
        )
        result = CliRunner().invoke(cli.main, ["validate"])
        assert result.exit_code == 1, (
            f"validate must exit 1 on bad contentPack.path; got "
            f"exit={result.exit_code} output={result.output!r}"
        )
        # The error must be the same AIDPF code the run command would
        # raise (AIDPF-1037 for installed-pack miss, AIDPF-1038 for
        # resolved-root-no-pack.yaml). Local relative path → 1038.
        assert (
            "AIDPF-1037" in result.output
            or "AIDPF-1038" in result.output
        ), (
            f"validate output must surface AIDPF-1037/1038; got "
            f"{result.output!r}"
        )
        # And it must NOT silently fall through to the legacy catalog —
        # erp_suppliers is in the catalog, so the legacy fallback
        # would have printed "validation passed".
        assert "validation passed" not in result.output


# ---------------------------------------------------------------------------
# catalog list / probe
# ---------------------------------------------------------------------------


class TestCatalog:
    def test_list_runs(self) -> None:
        result = CliRunner().invoke(cli.main, ["catalog", "list"])
        assert result.exit_code == 0
        assert "PVO catalog" in result.output
        # Some known ids present
        assert "erp_suppliers" in result.output

    def test_probe_requires_creds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Deterministic "no creds" environment, independent of the dev shell / CI
        # runner: clear ambient BICC creds AND run in an isolated cwd so the CLI's
        # auto-loaded `.env` (load_dotenv at startup) can't re-supply them.
        # Otherwise probe skips the missing-creds branch, hits the network, and
        # exits 1 instead of 2.
        monkeypatch.delenv("FUSION_BICC_USER", raising=False)
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli.main, [
                "catalog", "probe", "--pod", "https://example.com",
            ])
        assert result.exit_code == 2
        assert "missing creds" in result.output

    def test_probe_reconciles_when_all_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.fusion_catalog import CATALOG
        # Build a fake live response that contains every confirmed datastore name
        live_names = [{"name": e.datastore} for e in CATALOG.values()]
        fake_response = MagicMock(status_code=200)
        fake_response.json.return_value = {"items": live_names}
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.catalog.requests.get",
            return_value=fake_response,
        ):
            result = CliRunner().invoke(cli.main, [
                "catalog", "probe", "--pod", "https://example.com",
                "--user", "u", "--password", "p",
            ])
        assert result.exit_code == 0
        assert "all" in result.output and "reconcile" in result.output


# ---------------------------------------------------------------------------
# bootstrap (network probes mocked)
# ---------------------------------------------------------------------------


class TestBootstrap:
    def test_requires_bundle_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli.main, ["bootstrap"])
        assert result.exit_code == 1
        assert "bundle.yaml" in result.output

    def test_skips_bicc_probe_without_creds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        # Ensure FUSION_BICC_* env vars are absent so the probe SKIPs
        monkeypatch.delenv("FUSION_BICC_USER", raising=False)
        monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)
        result = CliRunner().invoke(cli.main, ["bootstrap"])
        # bundle.yaml + aidp.config.yaml load PASS but env=dev not in template -> FAIL on env-lookup
        # OR the templated env is named 'dev' and matches -> probes proceed
        # We don't assert exit code; only that bicc-auth was reported as SKIP.
        assert "bicc-auth" in result.output

    def test_bicc_probe_skips_aidp_secret_reference(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.bootstrap import (
            _ProbeResult,
            _probe_bicc,
        )

        monkeypatch.setenv("FUSION_BICC_USER", "user")
        monkeypatch.setenv(
            "FUSION_BICC_PASSWORD",
            "${aidp:secret:fusion_bicc_password.password}",
        )
        results: list[_ProbeResult] = []

        _probe_bicc(MagicMock(name="Bundle"), results)

        assert results[0].name == "bicc-auth"
        assert results[0].status == "SKIP"
        assert "AIDP credential-store" in results[0].detail


# ---------------------------------------------------------------------------
# run / status
# ---------------------------------------------------------------------------


class TestRun:
    def test_dispatch_without_inline_runs_real_preflight(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P1.5ε §Step 7b: ``run`` without ``--inline`` is no longer a stub —
        it actually runs the dispatch preflight. With the minimal init
        template (no env vars set, no dispatch coords filled in), Phase A
        preflight fails and the CLI exits 2 with a structured DISPATCH_*
        error code in the message.
        """
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 2
        # The new dispatch path raises a DispatchError; the code is
        # rendered as [DISPATCH_*] in the error message.
        assert "DISPATCH_" in result.output

    def test_dataset_filter_threaded_through_dispatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The ``--datasets`` filter is parsed by the CLI and reaches
        the dispatch entry point. Confirmed indirectly: the dispatch
        layer still exits 2 (preflight failure on minimal template), but
        the CLI accepted the flag and didn't error at Click parse."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--datasets", "gl_journal_lines"
        ])
        assert result.exit_code == 2

    def test_resume_without_inline_dispatches_via_rest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Phase 5 P1.5ε-fix5 — non-inline ``--resume`` is now
        supported through the REST-dispatch path. The CLI no longer
        rejects it with a "requires --inline" hint, and prints the cyan
        ``Resuming run`` banner before dispatching."""
        from unittest.mock import patch

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.commands.run."
            "_run_via_aidp_dispatch",
            return_value=0,
        ) as mock_dispatch:
            result = CliRunner().invoke(cli.main, [
                "run", "--mode", "seed", "--resume", "some-run-id",
            ])
        assert result.exit_code == 0, f"got {result.exit_code}: {result.output}"
        assert mock_dispatch.call_args is not None
        assert (
            mock_dispatch.call_args.kwargs.get("resume_run_id")
            == "some-run-id"
        )
        # P1.5ε-fix5 banner printed before dispatch (not under --dry-run).
        assert "Resuming run" in result.output
        assert "some-run-id" in result.output

    def test_dry_run_with_resume_omits_banner(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P1.5ε-fix5: under ``--dry-run``, dispatch short-circuits
        before any resume work; the cyan banner is gated on
        ``resume_run_id is not None and not dry_run`` so the operator
        doesn't see ``Resuming run X`` followed by a dry-run plan
        table (would imply work was done)."""
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=RunSummary.empty("test", "seed"),
        ) as mock_dispatch:
            result = CliRunner().invoke(cli.main, [
                "run", "--mode", "seed",
                "--resume", "some-id", "--dry-run",
            ])
        assert result.exit_code == 0, f"got {result.exit_code}: {result.output}"
        assert mock_dispatch.call_args.kwargs["resume_run_id"] == "some-id"
        assert mock_dispatch.call_args.kwargs["dry_run"] is True
        assert "Resuming run" not in result.output

    def test_run_dispatch_invokes_dispatch_via_rest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The non-inline path threads CLI flags through to
        ``dispatch_via_rest(...)`` with the correct kwarg shape."""
        from unittest.mock import MagicMock, patch

        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        fake_summary = RunSummary.empty("test", "seed")
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=fake_summary,
        ) as mock_dispatch:
            result = CliRunner().invoke(cli.main, [
                "run", "--mode", "seed", "--datasets", "erp_suppliers"
            ])
        assert result.exit_code == 0, f"got {result.exit_code}: {result.output}"
        assert mock_dispatch.called
        kwargs = mock_dispatch.call_args.kwargs
        assert kwargs["mode"] == "seed"
        assert kwargs["datasets"] == ["erp_suppliers"]
        assert kwargs["env_name"] == "dev"

    def test_dispatch_marker_degraded_renders_recovered_run_id_exit_2(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P1.5ε-fix5: when dispatch_via_rest raises
        DispatchMarkerDegradedError (the TC27 trap path), the CLI
        renders the message in red and exits 2. The recovered run_id
        is copy-pastable from the error block: contains
        "recovered run_id=<id>" and "--resume <id>". Crucially, the
        CLI does NOT fall into _render_summary's "Empty plan" branch.
        """
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
            DispatchMarkerDegradedError,
        )

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        msg = (
            "marker JSON parse failed (jobRunKey=jr-xyz); "
            "cluster job reached terminal status SUCCESS but the "
            "summary marker is unparseable. Recovered "
            "run_id=abc-123 from regex fallback — re-run "
            "with --resume abc-123 to continue."
        )
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            side_effect=DispatchMarkerDegradedError(
                msg, recovered_run_id="abc-123",
            ),
        ):
            result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 2, f"got {result.exit_code}: {result.output}"
        assert "DISPATCH_MARKER_DEGRADED" in result.output
        assert "recovered" in result.output.lower()
        assert "abc-123" in result.output
        assert "--resume abc-123" in result.output
        # The "Empty plan… nothing to do" miscapture must NOT happen —
        # the degraded path is a typed exception, not a summary render.
        assert "Empty plan" not in result.output

    # ------------------------------------------------------------------
    # P1.5ε-fix7 — --poll-timeout flag
    # ------------------------------------------------------------------

    def test_poll_timeout_default_is_3600(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No --poll-timeout flag → default 3600 (1 h) propagated to
        dispatch_via_rest. Bumped from P1.5ε's 1800 per TC29 evidence."""
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=RunSummary.empty("test", "seed"),
        ) as mock_dispatch:
            CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert mock_dispatch.call_args.kwargs["poll_timeout_s"] == 3600

    def test_poll_timeout_override_propagated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--poll-timeout 7200 reaches dispatch_via_rest(poll_timeout_s=7200)."""
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=RunSummary.empty("test", "seed"),
        ) as mock_dispatch:
            CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--poll-timeout", "7200"]
            )
        assert mock_dispatch.call_args.kwargs["poll_timeout_s"] == 7200

    def test_poll_timeout_below_min_rejected_at_click_parse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--poll-timeout 30 < 60 (min) → Click parse error, exits 2."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["run", "--mode", "seed", "--poll-timeout", "30"]
        )
        assert result.exit_code == 2
        # Click's range-rejection message names the bound.
        assert "60" in result.output

    def test_poll_timeout_above_max_rejected_at_click_parse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--poll-timeout 99999 > 14400 (max) → Click parse error, exits 2."""
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["run", "--mode", "seed", "--poll-timeout", "99999"]
        )
        assert result.exit_code == 2
        assert "14400" in result.output

    def test_poll_timeout_help_text_mentions_default_and_slow_tenant(
        self,
    ) -> None:
        """Locks the BACKLOG acceptance criterion that --poll-timeout's help
        text mentions the default + the slow-tenant use case — not just a
        bare flag declaration. Operator-actionable."""
        result = CliRunner().invoke(cli.main, ["run", "--help"])
        assert result.exit_code == 0
        # Default value present (Click renders default via show_default=True).
        assert "3600" in result.output
        # Operator-meaningful context — covers BICC / slow / cold-cache /
        # tenant. The plan asks for the slow-tenant rationale; any of these
        # tokens evidences it.
        assert any(
            tok in result.output.lower()
            for tok in ("slow", "tenant", "cold-cache", "bicc")
        )

    def test_run_dispatch_error_exits_2(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A DispatchError raised from dispatch_via_rest surfaces as a
        red one-liner and exits 2 (no traceback)."""
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
            DispatchPreflightError,
        )

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            side_effect=DispatchPreflightError("synthetic preflight fail"),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed"]
            )
        assert result.exit_code == 2
        assert "DISPATCH_PREFLIGHT_FAILED" in result.output
        assert "synthetic preflight fail" in result.output
        # No Python traceback.
        assert "Traceback" not in result.output

    def test_run_dispatch_wheel_build_error_exits_2(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A wheel-build failure must surface as DISPATCH_WHEEL_BUILD_FAILED
        through the CLI's `except DispatchError` catch — not as a raw
        RuntimeError traceback. Catches a regression where a future
        refactor reintroduces a local exception class in wheel_builder
        that doesn't inherit from DispatchError.
        """
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
            DispatchWheelBuildError,
        )

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            side_effect=DispatchWheelBuildError("`python -m build` failed: rc=1"),
        ):
            result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 2
        assert "DISPATCH_WHEEL_BUILD_FAILED" in result.output
        assert "python -m build" in result.output
        # No traceback — wheel build errors must round-trip through the
        # taxonomy, not escape as raw RuntimeError.
        assert "Traceback" not in result.output

    def test_run_dispatch_failed_steps_exits_1(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A RunSummary with ``failed > 0`` exits 1, not 2 — same
        contract as ``_run_inline``. Exit 2 is reserved for
        dispatch-layer errors (config, preflight, network)."""
        from unittest.mock import patch

        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
            RunStep,
            RunSummary,
        )

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        # Build a RunSummary with one failed step.
        from datetime import datetime, timezone

        failed_step = RunStep(
            run_id="x",
            dataset_id="ap_invoices",
            layer="bronze",
            mode="seed",
            status="failed",
            row_count=None,
            duration_seconds=1.0,
            error_message="boom",
            watermark_used=None,
        )
        summary = RunSummary(
            run_id="x",
            started_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 3, tzinfo=timezone.utc),
            bundle_project="test",
            mode="seed",
            steps=(failed_step,),
        )
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            return_value=summary,
        ):
            result = CliRunner().invoke(cli.main, ["run", "--mode", "seed"])
        assert result.exit_code == 1, f"got {result.exit_code}: {result.output}"

    def test_run_inline_invokes_orchestrator_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run --inline` calls orchestrator.run(bundle_path=..., mode=..., datasets=...)
        with the correct kwarg shape and exits 0 on a clean RunSummary.

        Replaces the pre-P1.5α stub-only test (which was marked skip
        in Phase 3). Mocks `orchestrator.run` to return a synthetic
        empty RunSummary so we don't need Spark.
        """
        from unittest.mock import MagicMock, patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 0, f"expected exit 0, got {result.exit_code}: {result.output}"
        # Assert the call shape — Path object, mode kwarg, datasets=None default
        assert mock_run.called
        call_kwargs = mock_run.call_args.kwargs
        assert isinstance(call_kwargs["bundle_path"], Path)
        assert call_kwargs["mode"] == "seed"
        assert call_kwargs["datasets"] is None

    def test_run_inline_passes_datasets_csv_as_raw_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--datasets "a,b,c"` is parsed by the CLI into ["a","b","c"]
        (whitespace trimmed, empty segments dropped) and threaded as a
        raw list — NOT pre-resolved against bundle.datasets[] (P1.5α-fix7).
        """
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            CliRunner().invoke(cli.main, [
                "run", "--mode", "seed", "--inline",
                "--datasets", " ap_aging , dim_supplier ,,"
            ])
        # Whitespace trimmed; empty segments dropped
        assert mock_run.call_args.kwargs["datasets"] == ["ap_aging", "dim_supplier"]

    # ----------------------------------------------------------------------
    # P1.5α-fix13 — --layers Click option threaded through the CLI
    # ----------------------------------------------------------------------

    def test_run_inline_with_layers_filter_passes_through(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run --inline --layers gold` reaches orchestrator.run with
        layers=["gold"] and datasets=None. P1.5α-fix13.

        Before fix13, Click rejected --layers at parse time with
        "Error: No such option: --layers" — defeating the "CLI is the
        contract" principle.
        """
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            result = CliRunner().invoke(
                cli.main,
                ["run", "--mode", "seed", "--inline", "--layers", "gold"],
            )
        assert result.exit_code == 0, (
            f"expected exit 0, got {result.exit_code}: {result.output}"
        )
        assert mock_run.called
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["layers"] == ["gold"], (
            f"--layers gold must parse to ['gold']; got {call_kwargs.get('layers')!r}"
        )
        assert call_kwargs["datasets"] is None, (
            f"--datasets unspecified must remain None; got {call_kwargs.get('datasets')!r}"
        )

    def test_run_inline_with_layers_and_datasets_combined(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Both filters are mutually compatible — the CLI help text says so.
        ``--layers bronze --datasets ap_invoices`` → orchestrator.run gets
        layers=['bronze'], datasets=['ap_invoices']. P1.5α-fix13.
        """
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import RunSummary
        fake_summary = RunSummary.empty("minimal", "seed")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            return_value=fake_summary,
        ) as mock_run:
            CliRunner().invoke(cli.main, [
                "run", "--mode", "seed", "--inline",
                "--layers", "bronze, silver",
                "--datasets", "ap_invoices"
            ])
        # Both filters reach orchestrator.run; CSV whitespace trimmed
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["layers"] == ["bronze", "silver"]
        assert call_kwargs["datasets"] == ["ap_invoices"]

    @pytest.mark.parametrize("exc_cls,msg_fragment", [
        ("BundleLoadError", "test bundle load failure"),
        ("UnsupportedModeError", "mode='full' is not supported"),
        ("MissingDependencyError", "Unknown dim 'dim_typo'"),
        ("CredentialResolutionError", "Env var 'FOO' is not set"),
        ("PrerequisiteError", "Extra-plan dependencies missing on disk"),
    ])
    def test_run_inline_exits_2_on_orchestrator_config_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        exc_cls: str, msg_fragment: str,
    ) -> None:
        """Every OrchestratorConfigError subclass surfaces as exit 2 with the
        message printed verbatim — no Python traceback."""
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        from oracle_ai_data_platform_fusion_autopilot.orchestrator import errors
        ExceptionCls = getattr(errors, exc_cls)

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            side_effect=ExceptionCls(msg_fragment),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 2, f"expected exit 2, got {result.exit_code}"
        assert msg_fragment in result.output
        # The load-bearing assertion: NO Python traceback leaked through.
        assert "Traceback (most recent call last)" not in result.output

    def test_run_inline_exits_2_on_not_implemented(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`NotImplementedError` (e.g. mode='incremental') is caught alongside
        OrchestratorConfigError."""
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            side_effect=NotImplementedError("Incremental mode is P1.5β"),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )
        assert result.exit_code == 2
        assert "P1.5β" in result.output
        assert "Traceback" not in result.output

    def test_run_cli_rejects_mode_full_at_parse_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`--mode full` is rejected by Click's Choice BEFORE the orchestrator
        is touched (P1.5α-fix2 Option A surface defense).

        Parse-time rejection is load-bearing — if a typo'd mode reached
        ``_run_inline``, the orchestrator's entry guard (Option D
        defense-in-depth) would catch it with a richer message, but Click's
        parser is the cheap front-line filter. The patched ``orchestrator.run``
        confirms the front line works — the orchestrator is never invoked.
        """
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        ) as mock_run:
            result = CliRunner().invoke(cli.main, ["run", "--mode", "full", "--inline"])
        assert result.exit_code == 2
        # Click's standard error format
        assert "'full' is not one of" in result.output or "Invalid value" in result.output
        # Parse-time rejection — orchestrator never invoked
        mock_run.assert_not_called()

    def test_run_inline_propagates_non_config_bugs_with_traceback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P1.5α-fix6 counter-test: the OrchestratorConfigError marker is a
        positive filter — only its subclasses surface as exit-2-with-
        friendly-message. A bare ``RuntimeError`` / ``KeyError`` /
        ``AssertionError`` (real orchestrator bugs, not user-facing config
        errors) must propagate with a Python exception so the operator
        can triage.

        Guards against a future contributor broadening the CLI catch
        clause to ``except Exception`` for "robustness", which would
        silently absorb real bugs as friendly exit-2 messages — hostile
        UX, hidden defects.
        """
        from unittest.mock import patch
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
            side_effect=RuntimeError("simulated orchestrator bug"),
        ):
            result = CliRunner().invoke(
                cli.main, ["run", "--mode", "seed", "--inline"],
            )

        # Bug must NOT silently become exit 2 — that would mask real defects.
        assert result.exit_code != 2, (
            f"non-OrchestratorConfigError must propagate as a bug, NOT exit 2. "
            f"Got exit_code={result.exit_code}, output={result.output!r}"
        )
        # Click surfaces the uncaught exception via result.exception.
        assert result.exception is not None, (
            "Click must surface the uncaught exception via result.exception"
        )
        assert isinstance(result.exception, RuntimeError), (
            f"the propagated exception must be the original RuntimeError; "
            f"got {type(result.exception).__name__}"
        )
        assert "simulated orchestrator bug" in str(result.exception)

    @pytest.mark.skip(reason="Phase 9: tested v1 resolve_plan typo detection; content-pack equivalent lives in test_content_pack_plan_resolver.")
    def test_run_inline_typoed_datasets_exits_2_no_traceback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """P1.5α-fix12 — end-to-end coverage: a typoed --datasets value
        reaches the orchestrator (Click's Choice doesn't validate it since
        --datasets is a free-form CSV), resolve_plan raises
        MissingDependencyError, the CLI's OrchestratorConfigError catch
        surfaces it as exit 2 with the typo named, no traceback.

        This is the load-bearing operator-UX test: without the fix in
        resolve_plan, the run would exit 0 with an empty RunSummary and
        the operator would believe a scoped refresh ran.
        """
        monkeypatch.chdir(tmp_path)
        # Stub env vars referenced by the minimal template — load_bundle must
        # succeed for the test to exercise resolve_plan (the actual fix site).
        # Real values not needed: the run halts at resolve_plan well before
        # any BICC call.
        monkeypatch.setenv("FUSION_BICC_BASE_URL", "https://stub.example.com")
        monkeypatch.setenv("FUSION_BICC_USER", "stub-user")
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "stub-pw")
        monkeypatch.setenv("FUSION_BICC_EXTERNAL_STORAGE", "stub_external_storage")
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--inline",
            "--datasets", "ap_invoies",  # typo of ap_invoices,
        ])
        assert result.exit_code == 2, (
            f"typoed --datasets must hard-fail exit 2 (NOT exit 0 with empty "
            f"plan); got exit_code={result.exit_code}, output={result.output!r}"
        )
        assert "ap_invoies" in result.output, (
            f"error output must name the offending --datasets value; "
            f"got: {result.output!r}"
        )
        assert "Traceback (most recent call last)" not in result.output, (
            "MissingDependencyError must be caught via OrchestratorConfigError "
            "marker and surfaced cleanly — no traceback leak"
        )


class TestMigrateBundle:
    """`migrate-bundle --from X --to Y` — scaffolded for Option L (§4.4d).

    Today only v0.2.0 exists; any non-no-op invocation exits 2 with a
    "no migration path" message. Blocker-2 fix: this is a top-level CLI
    verb that returns exit codes directly (not via NotImplementedError,
    which only `_run_inline` catches).
    """

    def test_same_version_is_noop_exit_0(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["migrate-bundle", "--from", "0.2.0", "--to", "0.2.0"],
        )
        assert result.exit_code == 0
        assert "already at version" in result.output

    def test_unknown_migration_exits_2(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        result = CliRunner().invoke(
            cli.main, ["migrate-bundle", "--from", "0.1.0", "--to", "0.2.0"],
        )
        assert result.exit_code == 2
        assert "No migration path" in result.output
        # Critical: NOT a Python traceback. Blocker-2 fix.
        assert "Traceback" not in result.output


class TestDashboardMcpSetup:
    def _connector(self, tmp_path: Path) -> Path:
        connector = tmp_path / "oac-mcp-connect.js"
        connector.write_text("// connector\n", encoding="utf-8")
        return connector

    def _summary(self, tmp_path: Path, *, user: str) -> dict[str, str]:
        return {
            "config_file": str(tmp_path / "config.json"),
            "oac_url": "https://oac.example.com",
            "user": user,
            "connector": str(tmp_path / "staged" / "oac-mcp-connect.js"),
            "mcp_json": str(tmp_path / ".mcp.json"),
            "connector_arg": "${HOME}/.oac-connect/oac-mcp-connect.js",
        }

    def test_uses_oac_mcp_env_before_legacy_admin_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OAC_URL", "https://oac.example.com")
        monkeypatch.setenv("OAC_MCP_USER", "least-priv-user")
        monkeypatch.setenv("OAC_MCP_PASSWORD", "least-priv-password")
        monkeypatch.setenv("OAC_ADMIN_USER", "admin-user")
        monkeypatch.setenv("OAC_ADMIN_PASSWORD", "admin-password")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.oac.mcp_token.setup_basic_auth",
            return_value=self._summary(tmp_path, user="least-priv-user"),
        ) as setup:
            result = CliRunner().invoke(
                cli.main,
                [
                    "dashboard",
                    "mcp-setup",
                    "--connector-js",
                    str(self._connector(tmp_path)),
                    "--mcp-json",
                    str(tmp_path / ".mcp.json"),
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = setup.call_args.kwargs
        assert kwargs["user"] == "least-priv-user"
        assert kwargs["password"] == "least-priv-password"

    def test_loads_customer_project_dotenv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("OAC_URL", raising=False)
        monkeypatch.delenv("OAC_MCP_USER", raising=False)
        monkeypatch.delenv("OAC_MCP_PASSWORD", raising=False)
        monkeypatch.delenv("OAC_ADMIN_USER", raising=False)
        monkeypatch.delenv("OAC_ADMIN_PASSWORD", raising=False)
        (tmp_path / ".env").write_text(
            "OAC_URL=https://dotenv-oac.example.com\n"
            "OAC_MCP_USER=dotenv-user\n"
            "OAC_MCP_PASSWORD=dotenv-password\n",
            encoding="utf-8",
        )

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.oac.mcp_token.setup_basic_auth",
            return_value=self._summary(tmp_path, user="dotenv-user"),
        ) as setup:
            result = CliRunner().invoke(
                cli.main,
                [
                    "dashboard",
                    "mcp-setup",
                    "--connector-js",
                    str(self._connector(tmp_path)),
                    "--mcp-json",
                    str(tmp_path / ".mcp.json"),
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = setup.call_args.kwargs
        assert kwargs["oac_url"] == "https://dotenv-oac.example.com"
        assert kwargs["user"] == "dotenv-user"
        assert kwargs["password"] == "dotenv-password"

    def test_legacy_admin_env_is_backward_compatible_fallback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("OAC_URL", "https://oac.example.com")
        monkeypatch.delenv("OAC_MCP_USER", raising=False)
        monkeypatch.delenv("OAC_MCP_PASSWORD", raising=False)
        monkeypatch.setenv("OAC_ADMIN_USER", "legacy-user")
        monkeypatch.setenv("OAC_ADMIN_PASSWORD", "legacy-password")

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.oac.mcp_token.setup_basic_auth",
            return_value=self._summary(tmp_path, user="legacy-user"),
        ) as setup:
            result = CliRunner().invoke(
                cli.main,
                [
                    "dashboard",
                    "mcp-setup",
                    "--connector-js",
                    str(self._connector(tmp_path)),
                    "--mcp-json",
                    str(tmp_path / ".mcp.json"),
                ],
            )

        assert result.exit_code == 0, result.output
        kwargs = setup.call_args.kwargs
        assert kwargs["user"] == "legacy-user"
        assert kwargs["password"] == "legacy-password"

    def test_missing_credentials_names_oac_mcp_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("OAC_URL", "https://oac.example.com")
        monkeypatch.delenv("OAC_MCP_USER", raising=False)
        monkeypatch.delenv("OAC_MCP_PASSWORD", raising=False)
        monkeypatch.delenv("OAC_ADMIN_USER", raising=False)
        monkeypatch.delenv("OAC_ADMIN_PASSWORD", raising=False)

        result = CliRunner().invoke(
            cli.main,
            [
                "dashboard",
                "mcp-setup",
                "--connector-js",
                str(self._connector(tmp_path)),
            ],
        )

        assert result.exit_code == 2
        assert "$OAC_MCP_USER" in result.output
        assert "$OAC_MCP_PASSWORD" in result.output
        assert "$OAC_ADMIN_USER" not in result.output
        assert "$OAC_ADMIN_PASSWORD" not in result.output


class TestStatus:
    def test_pyspark_unavailable_falls_back(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import sys

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        # Force the pyspark import to fail so `status` deterministically takes
        # its no-pyspark fallback (print the query, exit 0) regardless of
        # whether pyspark is installed in this environment. Setting the module
        # entries to None makes `from pyspark.sql import SparkSession` raise
        # ImportError, which is exactly what the fallback branch catches.
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)
        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0

    def test_reads_configured_bronze_schema(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """P1.5b — ``status()`` must read ``fusion_autopilot_state`` from the
        tenant's ``aidp.bronzeSchema`` (not the hardcoded ``'bronze'``).

        The scaffolded template (``examples/minimal_gl_only.yaml``) uses
        ``apiVersion`` and already has a full ``aidp:`` block with all
        four keys defaulted. We parse the YAML and *mutate* the existing
        ``aidp`` mapping in-place, then dump it back — a string-replace
        would either no-op (the template uses camelCase ``apiVersion``,
        not ``api_version``) or produce duplicate ``aidp:`` blocks where
        PyYAML would keep the later default one.

        After the mutation we sanity-check the parsed fixture before
        invoking ``status`` so a future template rename doesn't silently
        make the assertion vacuous.
        """
        import sys

        import yaml

        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])

        bundle_path = tmp_path / "bundle.yaml"
        bundle = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        # The scaffolded template MUST have the aidp block — pin that
        # contract so a template rename surfaces here, not as a confusing
        # status-test failure.
        assert isinstance(bundle.get("aidp"), dict), (
            "scaffolded template must already carry an `aidp:` block; "
            "if the template shape changes, this test (and the "
            "TablePaths.from_bundle contract) needs updating."
        )

        # Mutate the existing aidp mapping in place.
        bundle["aidp"]["catalog"]      = "my_lake"
        bundle["aidp"]["bronzeSchema"] = "raw"
        bundle["aidp"]["silverSchema"] = "clean"
        bundle["aidp"]["goldSchema"]   = "marts"

        bundle_path.write_text(
            yaml.safe_dump(bundle, sort_keys=False), encoding="utf-8"
        )

        # Sanity: round-trip the YAML and verify the mutation actually took.
        reread = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
        assert reread["aidp"]["catalog"]      == "my_lake"
        assert reread["aidp"]["bronzeSchema"] == "raw"

        # Force the fallback-print path (no pyspark).
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)

        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0
        assert "my_lake.raw.fusion_autopilot_state" in result.output
        # Critically, the pre-P1.5b hardcoded shape must NOT appear.
        assert "my_lake.bronze.fusion_autopilot_state" not in result.output

    def test_query_uses_latest_per_dataset_and_includes_skip_reason(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Should-fix-5: status query is ROW_NUMBER() OVER (PARTITION BY
        dataset_id ORDER BY last_run_at DESC) — one row per dataset — and
        includes the skip_reason column for cascade-vs-abort discrimination.

        Asserts on the SQL the fallback-print emits (the pyspark-unavailable
        path) since that's the surface the unit tests can reach without
        Spark.
        """
        import sys
        monkeypatch.chdir(tmp_path)
        CliRunner().invoke(cli.main, ["init", "--template", "minimal"])
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)

        result = CliRunner().invoke(cli.main, ["status"])
        assert result.exit_code == 0
        # Window function + partition-by-dataset assertion (the load-bearing
        # behavior — pre-fix the query returned every historical row).
        assert "ROW_NUMBER()" in result.output
        assert "PARTITION BY dataset_id" in result.output
        assert "ORDER BY last_run_at DESC" in result.output
        assert "WHERE rn = 1" in result.output
        # The new column makes cascade vs aborted visible to dashboards.
        assert "skip_reason" in result.output


# ---------------------------------------------------------------------------
# P1.5α-fix19 — CLI rendering contract: _render_summary prints the
# recommendations footer when present, omits the header when empty.
# ---------------------------------------------------------------------------


class TestRenderRecommendations:
    def _build_summary(self, recommendations: tuple[str, ...]):
        """Build a minimal RunSummary fixture with the given recommendations."""
        from datetime import datetime, UTC
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import (
            RunStep, RunSummary,
        )
        # Minimal one-step summary so _render_summary's main table branch runs.
        # Phase 9 follow-up: the spec-typed ``RunStep.success(spec, ...)``
        # factory was deleted; the live content-pack dispatcher constructs
        # RunStep directly with ``node.id`` / ``node.layer`` literals, so the
        # fixture mirrors that shape.
        now = datetime.now(UTC)
        step = RunStep(
            run_id="rid",
            dataset_id="erp_suppliers",
            layer="bronze",
            mode="seed",
            status="success",
            row_count=10,
            duration_seconds=1.0,
            error_message=None,
            watermark_used=None,
        )
        return RunSummary(
            run_id="rid", started_at=now, finished_at=now,
            bundle_project="test", mode="seed", steps=(step,),
            recommendations=recommendations,
        )

    def test_render_summary_prints_recommendations_footer(self) -> None:
        """When summary.recommendations is non-empty, _render_summary prints
        the header AND each recommendation line."""
        import io
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import _render_summary

        summary = self._build_summary((
            "consider adding schemaOverrides.po_receipts: Financial to bundle.yaml",
        ))
        buf = io.StringIO()
        console = Console(file=buf, force_terminal=False, no_color=True, width=200)
        _render_summary(console, summary)
        output = buf.getvalue()
        # Header + recommendation text both appear
        assert "Recommendations" in output, (
            f"recommendations header must be printed; got:\n{output}"
        )
        assert "schemaOverrides.po_receipts: Financial" in output, (
            f"recommendation text must be printed verbatim; got:\n{output}"
        )

    def test_render_summary_omits_footer_when_recommendations_empty(self) -> None:
        """Clean runs with no auto-corrections must NOT print the recommendations
        header — avoid noise on the happy path."""
        import io
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import _render_summary

        summary = self._build_summary(())  # empty tuple
        buf = io.StringIO()
        console = Console(file=buf, force_terminal=False, no_color=True, width=200)
        _render_summary(console, summary)
        output = buf.getvalue()
        assert "Recommendations" not in output, (
            f"recommendations header must NOT be printed on clean runs; got:\n{output}"
        )
