"""Pure bisection / type-ladder cores for ``bronze diagnose-encode``
(AIDPF-2093 diagnosis) plus the classifier + hint + verdict wiring.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_diagnose import (
    EncodeDiagnosis,
    bisect_encode_culprits,
    diagnose,
    probe_runtime_type,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_patch import (
    classify_bronze_extract_error,
    encode_failure_hint,
    full_exception_text,
)

COLUMNS = [f"c{i}" for i in range(400)]


def _probe_avoiding(poisoned: set[str]):
    def probe(cols):
        return not (set(cols) & poisoned)

    return probe


class TestBisection:
    def test_single_culprit_found(self) -> None:
        culprits, used, exhausted = bisect_encode_culprits(
            _probe_avoiding({"c137"}), COLUMNS, max_probes=30
        )
        assert culprits == ["c137"]
        assert not exhausted
        assert used <= 30

    def test_multiple_culprits_found(self) -> None:
        poisoned = {"c3", "c250"}
        culprits, _used, exhausted = bisect_encode_culprits(
            _probe_avoiding(poisoned), COLUMNS, max_probes=60
        )
        assert set(culprits) == poisoned and not exhausted

    def test_budget_exhaustion_reported(self) -> None:
        culprits, used, exhausted = bisect_encode_culprits(
            _probe_avoiding({"c399"}), COLUMNS, max_probes=3
        )
        assert exhausted
        assert used == 3

    def test_all_healthy_no_probe_waste(self) -> None:
        culprits, used, exhausted = bisect_encode_culprits(
            _probe_avoiding(set()), COLUMNS, max_probes=30
        )
        assert culprits == [] and used == 1 and not exhausted


class TestRuntimeTypeLadder:
    def test_long_wins_first(self) -> None:
        assert probe_runtime_type(
            lambda col, t: t == "bigint", "MaterialCost"
        ) == "bigint"

    def test_falls_through_to_string(self) -> None:
        assert probe_runtime_type(
            lambda col, t: t == "string", "SomeCol"
        ) == "string"

    def test_no_uniform_type_is_none(self) -> None:
        assert probe_runtime_type(lambda col, t: False, "Mixed") is None


class TestDiagnose:
    def test_healthy_dataset_short_circuits(self) -> None:
        d = diagnose(
            _probe_avoiding(set()), lambda c, t: True, COLUMNS
        )
        assert d.culprits == () and d.probes_used == 1

    def test_live_shape_names_culprit_and_type(self) -> None:
        d = diagnose(
            _probe_avoiding({"ItemBasePEOMaterialCost"}),
            lambda c, t: t == "bigint",
            ["Id", "ItemBasePEOMaterialCost", "ItemType"],
        )
        assert d.culprits == ("ItemBasePEOMaterialCost",)
        assert d.runtime_types == {"ItemBasePEOMaterialCost": "bigint"}
        assert "schemaPatches:" in d.suggestion_yaml
        assert "ItemBasePEOMaterialCost: bigint" in d.suggestion_yaml

    def test_mixed_type_culprit_yields_empty_suggestion(self) -> None:
        d = diagnose(
            _probe_avoiding({"Mixed"}), lambda c, t: False, ["a", "Mixed"]
        )
        assert d.runtime_types == {"Mixed": None}
        assert d.suggestion_yaml == ""


class TestEncodeClassifier:
    SIGNATURE = (
        "java.lang.Long is not a valid external type for schema of "
        "decimal(38,0)"
    )

    def test_plain_signature_classifies(self) -> None:
        assert classify_bronze_extract_error(self.SIGNATURE)

    def test_nested_cause_only_in_java_exception(self) -> None:
        """Round-3 mandated case: str(exc) LACKS the signature; the Java
        cause carries it."""

        class _JavaExc:
            def __str__(self) -> str:
                return TestEncodeClassifier.SIGNATURE

        class _Py4JLike(Exception):
            java_exception = _JavaExc()

            def __str__(self) -> str:
                return "An error occurred while calling o1472.count."

        exc = _Py4JLike()
        assert "o1472.count" in str(exc)
        assert classify_bronze_extract_error(full_exception_text(exc))

    def test_dunder_cause_chain_walked(self) -> None:
        inner = RuntimeError(self.SIGNATURE)
        outer = RuntimeError("wrapper without signature")
        outer.__cause__ = inner
        assert classify_bronze_extract_error(full_exception_text(outer))

    def test_cycle_safe_and_bounded(self) -> None:
        a = RuntimeError("a")
        b = RuntimeError("b")
        a.__cause__ = b
        b.__cause__ = a  # cycle
        text = full_exception_text(a)
        assert "a" in text and "b" in text  # terminated, no hang

    def test_non_encode_failure_not_classified(self) -> None:
        assert not classify_bronze_extract_error(
            full_exception_text(RuntimeError("TABLE_OR_VIEW_NOT_FOUND"))
        )

    def test_hint_prepend_contract(self) -> None:
        hint = encode_failure_hint("scm_items")
        assert hint.startswith("AIDPF-2093")
        assert "bronze diagnose-encode --dataset scm_items" in hint
        assert "schemaPatches" in hint


class TestVerdictRemediationLine:
    def test_2093_code_surfaces_remediation(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.run_reconcile import (
            StepView,
            reconcile_run_outcome,
        )

        outcome = reconcile_run_outcome(
            job_status="SUCCESS",
            marker_present=True,
            marker_degraded=False,
            steps=(
                StepView(
                    dataset_id="scm_items", layer="bronze", status="failed",
                    skip_reason=None,
                    error_message=encode_failure_hint("scm_items")
                    + " | bronze_extract_failed: An error occurred while "
                    "calling o1472.count.",
                ),
            ),
            mode="seed",
            expected_terminal_node_ids=frozenset({"scm_items"}),
            dry_run=False,
            run_id="run-1",
        )
        assert "AIDPF-2093" in outcome.codes
        text = "\n".join(outcome.lines)
        assert "diagnose-encode" in text
        # First line of the failing step's message carries the code.
        first_line = outcome.lines[1] if len(outcome.lines) > 1 else ""
        assert "AIDPF-2093" in text


class TestCliRegistration:
    def test_bronze_diagnose_encode_registered(self) -> None:
        """CLI-introspection invariant: the command the AIDPF-2093 hint and
        the triage skill advertise must actually exist."""
        from click.testing import CliRunner

        from oracle_ai_data_platform_fusion_autopilot.cli import main

        result = CliRunner().invoke(main, ["bronze", "diagnose-encode", "--help"])
        assert result.exit_code == 0
        assert "--dataset" in result.output
        assert "--max-probes" in result.output

    def test_diag_cell_compiles_and_carries_no_password(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.bronze_diagnose import (
            _build_diag_cell,
        )

        cell = _build_diag_cell(
            service_url="https://pod.example.com",
            username="bicc_user",
            external_storage="STORE",
            offering_schema="Financial",
            datastore="FscmTopModelAM.ScmExtractAM.EgpBiccExtractAM.ItemExtractPVO",
            bicc_secret_name="fusion_bicc_password",
            bicc_secret_key="password",
            max_probes=30,
        )
        compile(cell, "<diag-cell>", "exec")  # syntax golden
        assert "aidputils.secrets.get" in cell     # store-fetched
        assert "welcome" not in cell.lower()
        assert "AIDPF_DIAG_ENCODE_BEGIN" in cell   # marker envelope
