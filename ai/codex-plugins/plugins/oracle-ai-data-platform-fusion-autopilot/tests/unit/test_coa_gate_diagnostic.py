"""Gate-abort diagnostic truthfulness (FR-15.9), the reserved-row mode rung
(FR-15.12(b)), diagnostics-persistence parity (FR-15.13), and the
CLI-introspection invariant (§9.3.4b).

The checkpoint aborts on seven different codes — the diagnostic must be keyed
on the ACTUAL primary code (a 2013-only or 2074 abort mislabeled as 2018
would trigger the wrong automated remediation), populated from structured
probe evidence, with chart fields ABSENT (not fabricated) when the probes
never ran.
"""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.node_preflight import (
    CoaGateDiagnostic,
    evaluate_coa_checkpoint,
    select_primary_coa_code,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.run_manifest import (
    ResumeModeConflictError,
    resolve_run_mode,
)
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import ColumnAlias

GL_COA_COLUMNS = [
    "CodeCombinationChartOfAccountsId",
    "CodeCombinationSegment1",
    "CodeCombinationSegment2",
    "CodeCombinationSegment3",
    "CodeCombinationAccountType",
    "CodeCombinationEnabledFlag",
]

SINGLETON_COA = {
    "default": {
        "balancingSegment": "CodeCombinationSegment1",
        "costCenterSegment": "CodeCombinationSegment2",
        "naturalAccountSegment": "CodeCombinationSegment3",
    }
}


def _pack():
    pack = MagicMock()
    pack.pack.column_aliases = {
        "coa_balancing_segment": ColumnAlias(
            appliesTo="bronze.gl_coa", candidates=["CodeCombinationSegment1"],
            resolution="semanticRole", role="coa.balancing",
        ),
        "coa_cost_center_segment": ColumnAlias(
            appliesTo="bronze.gl_coa", candidates=["CodeCombinationSegment2"],
            resolution="semanticRole", role="coa.cost_center",
        ),
        "coa_natural_account_segment": ColumnAlias(
            appliesTo="bronze.gl_coa", candidates=["CodeCombinationSegment3"],
            resolution="semanticRole", role="coa.natural_account",
        ),
    }
    return pack


def _profile(coa):
    prof = MagicMock()
    prof.profile = {"chartOfAccounts": coa}
    return prof


def _fake_spark(chart_rows: dict[str, int], na_ambiguous: int = 0,
                na_total: int = 500, raise_on_probe: bool = False):
    spark = MagicMock()

    def _sql(query: str):
        df = MagicMock()
        q = " ".join(query.split())
        if raise_on_probe and not q.startswith("DESCRIBE TABLE"):
            raise RuntimeError("constrained session")
        if q.startswith("DESCRIBE TABLE"):
            df.collect.return_value = [(c, "string", None) for c in GL_COA_COLUMNS]
        elif "GROUP BY CAST(CodeCombinationChartOfAccountsId AS STRING)" in q:
            df.collect.return_value = list(chart_rows.items())
        else:
            df.collect.return_value = [(na_total, na_ambiguous)]
        return df

    spark.sql.side_effect = _sql
    return spark


def _checkpoint(coa, spark, *, structural_only=False, allow_unprovable=False):
    return evaluate_coa_checkpoint(
        spark,
        pack=_pack(),
        profile=_profile(coa),
        bronze_table_for_source={"gl_coa": "cat.bronze.gl_coa"},
        coa_sources={"gl_coa"},
        allow_unprovable=allow_unprovable,
        structural_only=structural_only,
    )


class TestPrimaryCodeSelection:
    def test_priority_order(self) -> None:
        assert select_primary_coa_code(["AIDPF-2017", "AIDPF-2018"]) == "AIDPF-2018"
        assert select_primary_coa_code(["AIDPF-2013"]) == "AIDPF-2013"
        assert select_primary_coa_code(["AIDPF-2074"]) == "AIDPF-2074"
        assert select_primary_coa_code(["AIDPF-9999"]) == "AIDPF-9999"


class TestDiagnosticTruthfulness:
    def test_structural_2013_only_chart_fields_absent(self) -> None:
        res = _checkpoint(None, _fake_spark({}))  # no mapping at all
        assert not res.ok
        d = res.diagnostic
        assert isinstance(d, CoaGateDiagnostic)
        assert d.primary_code == "AIDPF-2013"
        assert d.active_charts is None        # probes never ran —
        assert d.mapped_charts is None        # nothing fabricated
        assert d.singleton_accepted is None

    def test_2018_plus_2017_primary_is_2018_with_evidence(self) -> None:
        spark = _fake_spark(
            {"138": 5000, "22625": 300}, na_ambiguous=300, na_total=500,
        )
        res = _checkpoint(SINGLETON_COA, spark)
        d = res.diagnostic
        assert d is not None
        assert d.primary_code == "AIDPF-2018"
        assert "AIDPF-2017" in d.codes
        assert d.active_charts == ("138", "22625")
        assert d.active_chart_count == 2
        assert d.mapped_charts == ()          # no byChart declared
        assert d.singleton_accepted is False
        assert set(d.contradicted_charts or ()) == {"138", "22625"}

    def test_2017_only_when_singleton_accepted(self) -> None:
        coa = dict(SINGLETON_COA)
        coa["singletonAccepted"] = True
        spark = _fake_spark({"138": 5000}, na_ambiguous=300, na_total=500)
        res = _checkpoint(coa, spark)
        d = res.diagnostic
        assert d is not None
        assert d.primary_code == "AIDPF-2017"
        assert d.singleton_accepted is True
        assert d.contradicted_charts == ("138",)

    def test_2074_unprovable_probe(self) -> None:
        spark = _fake_spark({}, raise_on_probe=True)
        res = _checkpoint(SINGLETON_COA, spark)
        d = res.diagnostic
        assert d is not None
        assert d.primary_code == "AIDPF-2074"
        assert d.active_charts is None        # probe raised — no evidence

    def test_ok_checkpoint_has_no_diagnostic(self) -> None:
        res = _checkpoint(SINGLETON_COA, _fake_spark({"138": 5000}))
        assert res.ok
        assert res.diagnostic is None


class TestReservedRowModeRung:
    def _resolve(self, *, explicit=None, reserved=None):
        return resolve_run_mode(
            explicit, is_resume=True, manifest_mode=None,
            historical_exec_modes=[], reserved_row_modes=reserved,
        )

    def test_single_reserved_mode_adopted_bare(self) -> None:
        assert self._resolve(reserved=["incremental"]) == "incremental"
        assert self._resolve(reserved=["seed"]) == "seed"

    def test_explicit_matching_reserved_ok(self) -> None:
        assert self._resolve(explicit="seed", reserved=["seed"]) == "seed"

    def test_explicit_conflicting_reserved_raises_1046(self) -> None:
        with pytest.raises(ResumeModeConflictError, match="AIDPF-1046"):
            self._resolve(explicit="seed", reserved=["incremental"])

    def test_inconsistent_reserved_modes_raise_1046(self) -> None:
        with pytest.raises(ResumeModeConflictError, match="INCONSISTENT"):
            self._resolve(reserved=["seed", "incremental"])

    def test_no_reserved_keeps_existing_rejection(self) -> None:
        with pytest.raises(ResumeModeConflictError, match="cannot infer"):
            self._resolve(reserved=[])

    def test_manifest_still_wins_over_reserved(self) -> None:
        assert resolve_run_mode(
            None, is_resume=True, manifest_mode="incremental",
            historical_exec_modes=[], reserved_row_modes=["seed"],
        ) == "incremental"


class TestDiagnosticsPersistenceParity:
    def test_coa_gate_payload_persisted_with_primary_code_filename(
        self, tmp_path: Path,
    ) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            persist_run_diagnostics,
        )

        summary = MagicMock()
        summary.run_id = "run-1"
        summary.diagnostics = (
            {"kind": "coa-gate", "errorCode": "AIDPF-2013", "codes": ["AIDPF-2013"]},
        )
        persist_run_diagnostics(tmp_path, summary)
        target = tmp_path / ".aidp" / "diagnostics" / "run-1" / (
            "AIDPF-2013__coa-gate.json"
        )
        assert target.exists()
        assert '"AIDPF-2013"' in target.read_text(encoding="utf-8")

    def test_malformed_entry_is_best_effort_skipped(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
            persist_run_diagnostics,
        )

        summary = MagicMock()
        summary.run_id = "run-1"
        summary.diagnostics = ("not-a-dict",)
        persist_run_diagnostics(tmp_path, summary)  # must not raise


class TestCliIntrospectionInvariant:
    """Every command/option a diagnostic or verdict block advertises must be
    registered on the shipped click CLI (§9.3.4b) — no phase may emit a
    command that does not exist yet."""

    def _registered_options(self) -> dict[str, set[str]]:
        from oracle_ai_data_platform_fusion_autopilot import cli

        options: dict[str, set[str]] = {}
        for name, cmd in cli.main.commands.items():
            opts: set[str] = set()
            for param in cmd.params:
                opts.update(o for o in param.opts if o.startswith("--"))
            options[name] = opts
        return options

    def _assert_advertised_commands_registered(self, text: str) -> None:
        registered = self._registered_options()
        for cmd, flags in re.findall(
            r"`?\b(bootstrap|run)\b((?:\s+--[a-z0-9-]+)*)", text
        ):
            for flag in re.findall(r"--[a-z0-9-]+", flags):
                assert flag in registered[cmd], (
                    f"advertised `{cmd} {flag}` is not registered on the CLI"
                )

    def test_verdict_block_advertises_only_registered_commands(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile import (
            StepView,
            reconcile_run_outcome,
        )

        out = reconcile_run_outcome(
            job_status="SUCCESS", marker_present=True, marker_degraded=False,
            steps=[StepView(dataset_id="__coa_gate__", status="failed",
                            error_message="AIDPF-2018 x AIDPF-2017 y")],
            mode="seed", expected_terminal_node_ids=frozenset(),
            dry_run=False, run_id="r1",
        )
        self._assert_advertised_commands_registered("\n".join(out.lines))

    def test_p0_sources_never_emit_the_p2_flag(self) -> None:
        # The P2 PR that registers --resolve-coa-from-metadata upgrades these
        # surfaces in the same change; until then no EMITTED string literal
        # may mention it (docstrings/comments describing the plan are fine —
        # they are never printed or written to a diagnostic).
        import ast

        import oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile as rr
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as orch

        registered = self._registered_options()
        if "--resolve-coa-from-metadata" in registered.get("bootstrap", set()):
            pytest.skip("P2 flag registered — upgrade path active")

        def _emitted_strings(module) -> list[str]:
            tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
            docstrings: set[int] = set()
            for node in ast.walk(tree):
                if isinstance(
                    node,
                    (ast.Module, ast.ClassDef, ast.FunctionDef,
                     ast.AsyncFunctionDef),
                ):
                    body = getattr(node, "body", [])
                    if body and isinstance(body[0], ast.Expr) and isinstance(
                        body[0].value, ast.Constant
                    ) and isinstance(body[0].value.value, str):
                        docstrings.add(id(body[0].value))
            return [
                node.value
                for node in ast.walk(tree)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and id(node) not in docstrings
            ]

        for module in (rr, orch):
            for literal in _emitted_strings(module):
                assert "--resolve-coa-from-metadata" not in literal, (
                    f"{module.__name__} emits the unregistered P2 flag"
                )
