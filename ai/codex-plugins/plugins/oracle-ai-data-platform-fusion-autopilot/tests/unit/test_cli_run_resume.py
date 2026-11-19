"""P1.5α-fix21 CLI tests for ``--resume``.

Two surfaces:
  * ``cli.py`` — Click option declaration: ``--resume <run_id>`` is
    threaded through to ``commands.run.run`` as the ``resume_run_id``
    kwarg.
  * ``commands/run.py`` — the resume banner is printed and the
    orchestrator-side exceptions (``ResumeRunNotFoundError``,
    ``ResumeRunNotResumableError``, ``ResumeBundleMismatchError``)
    surface as exit code 2 with a single-line message (no traceback).

These tests verify the CLI plumbing without spinning up Spark or
exercising the orchestrator's resume flow internals — those live in
``test_orchestrator_resume.py``.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from oracle_ai_data_platform_fusion_autopilot import cli
from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
    ResumeBundleMismatchError,
    ResumeRunNotFoundError,
    ResumeRunNotResumableError,
)


def _init_minimal_bundle(monkeypatch) -> None:
    monkeypatch.setenv("FUSION_BICC_BASE_URL", "https://stub.example.com")
    monkeypatch.setenv("FUSION_BICC_USER", "stub-user")
    monkeypatch.setenv("FUSION_BICC_PASSWORD", "stub-pw")
    monkeypatch.setenv("FUSION_BICC_EXTERNAL_STORAGE", "stub_external_storage")
    CliRunner().invoke(cli.main, ["init", "--template", "minimal"])


def test_resume_option_appears_in_help() -> None:
    result = CliRunner().invoke(cli.main, ["run", "--help"])
    assert result.exit_code == 0
    assert "--resume" in result.output
    assert "run_id" in result.output.lower() or "run id" in result.output.lower()


def test_resume_help_describes_inline_and_rest_support() -> None:
    """Help text MUST match runtime: ``--resume`` works on both
    --inline AND REST dispatch since P1.5ε-fix5. The previous text
    said REST resume was BACKLOG, which sent operators to the wrong
    invocation (the dispatcher already threads ``resume_run_id`` into
    the generated notebook cell — see ``test_resume_without_inline_
    dispatches_via_rest`` below)."""
    result = CliRunner().invoke(
        cli.main, ["run", "--help"],
    )
    assert result.exit_code == 0
    # Normalise whitespace — Click wraps help text across lines.
    flat = " ".join(result.output.split())
    # Stale wording must be gone.
    assert "BACKLOG P1.5ε-fix5" not in flat
    assert "Requires --inline" not in flat
    # New wording must mention both supported paths.
    assert "REST" in flat
    assert "inline" in flat.lower()


def test_resume_without_inline_dispatches_via_rest(
    tmp_path: Path, monkeypatch,
) -> None:
    """Phase 5 P1.5ε-fix5 — non-inline ``--resume`` now dispatches via
    REST. The CLI no longer rejects it; the dispatcher's notebook
    cell threads the supplied id into the cluster-side
    ``orchestrator.run(...)`` call, and the cyan ``Resuming run`` banner
    is printed before dispatching (not under --dry-run).
    """
    from unittest.mock import patch

    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    # Mock the dispatch path so we don't actually try to hit AIDP.
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.commands.run."
        "_run_via_aidp_dispatch",
        return_value=0,
    ) as mock_dispatch:
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--resume", "some-run-id",
        ])
    assert result.exit_code == 0, f"got {result.exit_code}: {result.output}"
    # The resume id flowed into the dispatcher.
    assert mock_dispatch.call_args is not None
    assert mock_dispatch.call_args.kwargs.get("resume_run_id") == "some-run-id"
    # P1.5ε-fix5 banner printed before dispatch (not under --dry-run).
    assert "Resuming run" in result.output
    assert "some-run-id" in result.output


def test_resume_run_not_found_exits_2_no_traceback(
    tmp_path: Path, monkeypatch,
) -> None:
    """``ResumeRunNotFoundError`` subclasses ``OrchestratorConfigError``
    so the CLI's exit-2 path catches it and prints the message without
    a Python traceback."""
    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        side_effect=ResumeRunNotFoundError("--resume: no rows for run_id='ghost'"),
    ):
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--inline", "--resume", "ghost"
        ])
    assert result.exit_code == 2
    assert "ghost" in result.output
    # No traceback markers.
    assert "Traceback" not in result.output


def test_manifest_error_exits_2_no_traceback(
    tmp_path: Path, monkeypatch,
) -> None:
    """A manifest/resume-guard failure (AIDPF-4022 / AIDPF-104x) must surface as
    the clean exit-2 path, not a traceback — ``ManifestError`` subclasses
    ``OrchestratorConfigError`` so the CLI boundary catches it."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.run_manifest import (
        ManifestInvalidError,
    )

    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        side_effect=ManifestInvalidError(
            "AIDPF-4022: the __run_manifest__ row has a missing/empty payload."
        ),
    ):
        result = CliRunner().invoke(cli.main, [
            "run", "--inline", "--resume", "some-run",
        ])
    assert result.exit_code == 2
    assert "AIDPF-4022" in result.output
    assert "Traceback" not in result.output


def test_resume_run_not_resumable_exits_2_no_traceback(
    tmp_path: Path, monkeypatch,
) -> None:
    """Same exit-2 contract for the not-resumable subcase
    (``plan_hash IS NULL`` / ``plan_snapshot IS NULL``)."""
    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        side_effect=ResumeRunNotResumableError(
            "--resume: run_id='legacy' is not resumable — plan_hash NULL."
        ),
    ):
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--inline", "--resume", "legacy"
        ])
    assert result.exit_code == 2
    assert "not resumable" in result.output
    assert "Traceback" not in result.output


def test_resume_bundle_mismatch_exits_2_no_traceback(
    tmp_path: Path, monkeypatch,
) -> None:
    """Drift gate raises → CLI exits 2 with the rendered diff message
    (identity changes, dataset changes, hash echo)."""
    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    msg = (
        "--resume: bundle drift detected against run_id='abc-123'.\n"
        "\n"
        "Identity changes:\n"
        "  aidp.silverSchema: 'silver_v1' → 'silver_v2'"
    )
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        side_effect=ResumeBundleMismatchError(msg),
    ):
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--inline", "--resume", "abc-123"
        ])
    assert result.exit_code == 2
    # The rendered diff sections surface to the operator.
    assert "Identity changes" in result.output
    assert "aidp.silverSchema" in result.output
    assert "Traceback" not in result.output


def test_resume_banner_printed_before_orchestrator_call(
    tmp_path: Path, monkeypatch,
) -> None:
    """When ``--resume`` is set, the CLI prints a banner so the
    operator sees we're entering resume mode (before any
    orchestrator-side I/O happens)."""
    monkeypatch.chdir(tmp_path)
    _init_minimal_bundle(monkeypatch)
    # Mock orchestrator to no-op (return an empty RunSummary).
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.runtime import RunSummary
    with patch(
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.run",
        return_value=RunSummary.empty("test-bundle", "seed"),
    ):
        result = CliRunner().invoke(cli.main, [
            "run", "--mode", "seed", "--inline", "--resume", "abc-123"
        ])
    assert "Resuming run" in result.output
    assert "abc-123" in result.output
