"""``run_reconcile`` — the job-SUCCESS-masks-abort matrix (design §9.1/§15).

Pure input/output pairs, no Spark, no CLI. The load-bearing assertions:

* a ``SUCCESS`` job status can never turn a failure into a pass (R1–R4 win);
* dry-runs reconcile to ``planned``/exit 0 (R0 — ``RunSummary.empty()`` is
  zero steps + a populated plan, exactly the shape R4 would misfire on);
* mart-only runs complete: lineage bronze nodes are absent from the
  orchestrator-declared expected set, not "missing";
* ``AIDPF-4023`` is actually emitted (FR-15.10), not just documented;
* no verdict line ever advertises an unregistered CLI option (§9.3.4b) and
  the resume hint never pins ``--mode`` (D-12).
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile import (
    AIDPF_4023_RUN_RECONCILIATION,
    StepView,
    classify_run_completeness,
    reconcile_run_outcome,
)


def _step(dataset_id: str, status: str = "success", *, skip_reason: str | None = None,
          error_message: str | None = None) -> StepView:
    return StepView(
        dataset_id=dataset_id, layer="bronze", status=status,
        skip_reason=skip_reason, error_message=error_message,
    )


def _reconcile(steps, expected, *, job_status="SUCCESS", dry_run=False,
               marker_present=True, marker_degraded=False, mode="seed",
               run_id="run-1"):
    return reconcile_run_outcome(
        job_status=job_status, marker_present=marker_present,
        marker_degraded=marker_degraded, steps=steps, mode=mode,
        expected_terminal_node_ids=expected, dry_run=dry_run, run_id=run_id,
    )


COA_GATE_FAILED = _step(
    "__coa_gate__", "failed",
    error_message=(
        "AIDPF-2018: gl_coa has 41 active charts of accounts but the profile "
        "has only a singleton COA mapping.\nAIDPF-2017: chart '138': the "
        "column bound as naturalAccountSegment does not classify."
    ),
)


class TestSuccessMasksAbortMatrix:
    def test_job_success_plus_coa_gate_failed_is_aborted(self) -> None:
        out = _reconcile(
            [_step("gl_coa"), COA_GATE_FAILED], frozenset({"gl_coa"}),
        )
        assert out.verdict == "aborted"
        assert out.exit_code == 1
        assert out.codes == (
            AIDPF_4023_RUN_RECONCILIATION, "AIDPF-2018", "AIDPF-2017",
        )

    def test_job_success_plus_all_steps_skipped_aborted(self) -> None:
        steps = [_step("a", "skipped", skip_reason="aborted"),
                 _step("b", "skipped", skip_reason="aborted")]
        out = _reconcile(steps, frozenset({"a", "b"}))
        assert out.verdict == "aborted"
        assert out.exit_code == 1

    def test_job_success_plus_expected_node_with_no_step_is_unproven(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa", "dim_account"}))
        assert out.verdict == "unproven"
        assert out.exit_code == 1
        assert AIDPF_4023_RUN_RECONCILIATION in out.codes
        assert any("dim_account" in line for line in out.lines)

    def test_job_success_every_expected_node_terminal_is_completed(self) -> None:
        steps = [_step("gl_coa"), _step("dim_account", "resumed_skipped"),
                 _step("gl_balance", "deferred")]
        out = _reconcile(steps, frozenset({"gl_coa", "dim_account", "gl_balance"}))
        assert out.verdict == "completed"
        assert out.exit_code == 0
        assert out.lines == ()

    def test_reserved_step_not_success_is_aborted(self) -> None:
        out = _reconcile(
            [_step("gl_coa"), _step("__coa_gate__", "deferred")],
            frozenset({"gl_coa"}),
        )
        assert out.verdict == "aborted"


class TestDryRunR0:
    def test_dry_run_zero_steps_populated_plan_is_planned_exit_0(self) -> None:
        out = _reconcile([], frozenset({"gl_coa", "dim_account"}), dry_run=True)
        assert out.verdict == "planned"
        assert out.exit_code == 0
        assert out.lines == ()

    def test_dry_run_short_circuits_even_with_failed_steps(self) -> None:
        out = _reconcile([COA_GATE_FAILED], frozenset(), dry_run=True)
        assert out.verdict == "planned"
        assert out.exit_code == 0


class TestMartOnlyAndExpectedSet:
    def test_mart_only_lineage_bronze_absent_from_expected_set_completes(self) -> None:
        # Bronze stays in the resolved plan for lineage but the orchestrator
        # never executes it — and never declares it expected.
        steps = [_step("supplier_spend"), _step("ap_aging")]
        out = _reconcile(steps, frozenset({"supplier_spend", "ap_aging"}))
        assert out.verdict == "completed"
        assert out.exit_code == 0

    def test_no_expected_set_is_honest_not_checked_exit_0(self) -> None:
        out = _reconcile([_step("gl_coa")], None)
        assert out.verdict == "not_checked"
        assert out.exit_code == 0
        assert any("unproven-not-checked" in line for line in out.lines)

    def test_zero_failure_but_incomplete_agrees_with_shared_core(self) -> None:
        steps = [_step("gl_coa")]
        expected = frozenset({"gl_coa", "dim_account"})
        assert classify_run_completeness(steps, expected) == "unproven"
        assert _reconcile(steps, expected).verdict == "unproven"


class TestDefensiveR5R6:
    def test_marker_missing_is_unproven(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa"}),
                         marker_present=False)
        assert out.verdict == "unproven"
        assert out.exit_code == 1

    def test_marker_degraded_is_unproven(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa"}),
                         marker_degraded=True)
        assert out.verdict == "unproven"

    def test_job_not_success_is_aborted(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa"}),
                         job_status="FAILED")
        assert out.verdict == "aborted"

    def test_inline_path_job_status_none_is_neutral(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa"}),
                         job_status=None)
        assert out.verdict == "completed"


class TestCodesAndFourOhTwoThree:
    def test_unproven_always_carries_4023(self) -> None:
        out = _reconcile([_step("gl_coa")], frozenset({"gl_coa", "x"}))
        assert out.codes[0] == AIDPF_4023_RUN_RECONCILIATION

    def test_4023_never_duplicated(self) -> None:
        out = _reconcile(
            [_step("bad", "failed", error_message="AIDPF-4023 already here")],
            frozenset({"bad"}),
        )
        assert out.codes.count(AIDPF_4023_RUN_RECONCILIATION) == 1

    def test_codes_deduplicated_in_first_seen_order(self) -> None:
        steps = [
            _step("a", "failed", error_message="AIDPF-2018 first"),
            _step("b", "failed", error_message="AIDPF-2017 then AIDPF-2018"),
        ]
        out = _reconcile(steps, frozenset({"a", "b"}))
        assert out.codes == (
            AIDPF_4023_RUN_RECONCILIATION, "AIDPF-2018", "AIDPF-2017",
        )


class TestVerdictLinesContract:
    def test_coa_abort_prints_manual_remediation_without_p2_flag(self) -> None:
        out = _reconcile([COA_GATE_FAILED], frozenset())
        text = "\n".join(out.lines)
        assert "RUN VERDICT: ABORTED" in text
        assert "NOT the run's verdict" in text
        # §9.3.4b: P2 registered the flag — the hint is now the
        # executable remediation loop (introspection test proves every
        # advertised option is registered).
        assert "bootstrap --refresh --resolve-coa-from-metadata" in text
        # D-12: the resume hint never pins a mode.
        assert "run --resume run-1" in text
        assert "--mode" not in text.replace("no --mode", "")

    def test_non_coa_abort_has_no_coa_remediation_hint(self) -> None:
        out = _reconcile(
            [_step("x", "failed", error_message="AIDPF-4071 column missing")],
            frozenset({"x"}),
        )
        text = "\n".join(out.lines)
        assert "medallion-author" not in text


# ── status() banner: supersession semantics (FR-15.8 / design §9.2.5) ───────

from oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile import (  # noqa: E402
    banner_verdict,
)


def _row(run_id: str, dataset_id: str, status: str, ts: int, *,
         error_message: str | None = None) -> dict:
    return {
        "run_id": run_id, "dataset_id": dataset_id, "status": status,
        "error_message": error_message, "last_run_at": ts,
    }


class TestBannerVerdict:
    def test_no_reserved_rows_no_banner(self) -> None:
        assert banner_verdict([]) is None

    def test_completed_run(self) -> None:
        banner = banner_verdict([
            _row("r1", "__run_manifest__", "deferred", 1),
            _row("r1", "__run_outcome__", "success", 2),
        ])
        assert banner is not None
        assert (banner.run_id, banner.label) == ("r1", "COMPLETED")

    def test_gate_aborted_run(self) -> None:
        banner = banner_verdict([
            _row("r1", "__run_manifest__", "deferred", 1),
            _row("r1", "__coa_gate__", "failed", 2,
                 error_message="AIDPF-2018 multi-COA. AIDPF-2017 chart 138."),
            _row("r1", "__run_outcome__", "failed", 3,
                 error_message="aborted: AIDPF-2018, AIDPF-2017"),
        ])
        assert banner.label == "ABORTED"
        assert banner.codes == ("AIDPF-2018", "AIDPF-2017")

    def test_manifest_without_outcome_is_unproven(self) -> None:
        banner = banner_verdict([_row("r1", "__run_manifest__", "deferred", 1)])
        assert banner.label == "UNPROVEN"
        assert "no completion record" in banner.detail

    def test_preloop_abort_without_manifest_row_still_banners(self) -> None:
        # Pre-feature/4022 edge: gate + outcome rows exist, no manifest row.
        banner = banner_verdict([
            _row("r1", "__coa_gate__", "failed", 1,
                 error_message="AIDPF-2018 x"),
            _row("r1", "__run_outcome__", "failed", 2,
                 error_message="aborted: AIDPF-2018"),
        ])
        assert (banner.run_id, banner.label) == ("r1", "ABORTED")

    def test_abort_then_successful_resume_supersedes(self) -> None:
        banner = banner_verdict([
            _row("r1", "__run_manifest__", "deferred", 1),
            _row("r1", "__coa_gate__", "failed", 2,
                 error_message="AIDPF-2018 x"),
            _row("r1", "__run_outcome__", "failed", 3,
                 error_message="aborted: AIDPF-2018"),
            _row("r1", "__run_outcome__", "success", 9),  # the resume
        ])
        assert banner.label == "COMPLETED"

    def test_abort_then_failed_resume_stays_aborted_newest_codes(self) -> None:
        banner = banner_verdict([
            _row("r1", "__run_outcome__", "failed", 3,
                 error_message="aborted: AIDPF-2018"),
            _row("r1", "__run_outcome__", "failed", 9,
                 error_message="aborted: AIDPF-2017"),
        ])
        assert banner.label == "ABORTED"
        assert banner.codes == ("AIDPF-2017",)

    def test_gate_newer_than_latest_outcome_wins(self) -> None:
        # Gate abort whose best-effort outcome write failed.
        banner = banner_verdict([
            _row("r1", "__run_outcome__", "success", 5),
            _row("r1", "__coa_gate__", "failed", 9,
                 error_message="AIDPF-2074 unprovable"),
        ])
        assert banner.label == "ABORTED"
        assert banner.codes == ("AIDPF-2074",)

    def test_latest_run_selected_across_run_ids_by_any_row_kind(self) -> None:
        banner = banner_verdict([
            _row("old", "__run_outcome__", "success", 5),
            _row("new", "__coa_gate__", "failed", 9,
                 error_message="AIDPF-2018 y"),
        ])
        assert (banner.run_id, banner.label) == ("new", "ABORTED")
