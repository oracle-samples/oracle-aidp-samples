"""Phase 2 of external-catalog-system-table-hardening: engine-level
qualifire-internal-failure classification.

Two suppression sites covered:

* **Persistence-infrastructure failures** — ``_persist_data_rows``
  returns ``PersistenceOutcome(kind=INFRA_FAILURE, exc=...)``;
  the engine main loop skips notifications, best-effort retries
  the engine_warning, and re-raises ``QualifireValidationError``
  with PEP 3134 chained ``__cause__`` plus a data-findings summary.

* **Validator-execution exceptions** — both engine call sites
  (parallel-pipeline at ``engine.py:533`` and sequential at
  ``~793``) wrap unhandled exceptions in a synthesised
  ``ValidationResult`` carrying
  ``details['qualifire_internal_failure'] = True``. The
  notification dispatcher's filter skips marked rows so library
  bugs don't page on-call.

Both sites share the helper ``_validator_execution_error_result``
and the predicate ``_is_qualifire_internal_failure``. These tests
pin the contracts so future refactors don't quietly re-introduce
the false-positive-paging behaviour.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.context import QualifireContext
from qualifire.core.engine import (
    PersistenceOutcome,
    PersistenceOutcomeKind,
    QualifireEngine,
    _is_qualifire_internal_failure,
    _validator_execution_error_result,
)
from qualifire.core.exceptions import (
    QualifireInternalError,
    QualifireValidationError,
)
from qualifire.core.models import Severity, ValidationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine(datasets, *, storage=None, notifiers=None):
    """Construct a QualifireEngine with a mock backend that returns
    a passing aggregation. Engine internals are exercised
    end-to-end up through the persistence + notification phases.
    """
    backend = MagicMock()
    backend.spark = MagicMock()
    backend.get_aggregations.return_value = {"row_count": 100}
    config = QualifireConfig(
        owner="o", bu="b", system_table="t",
        datasets=datasets,
    )
    return QualifireEngine(
        backend=backend,
        storage=storage,
        context=QualifireContext(),
        config=config,
        notifiers=notifiers or {},
    )


def _passing_dataset(name: str = "ds") -> DatasetConfig:
    """A dataset with a single threshold rule that passes given
    backend.get_aggregations -> {row_count: 100}. Used as the
    happy-path fixture so tests focus on engine-level classifier
    behaviour, not validator semantics."""
    return DatasetConfig(
        name=name,
        table=f"db.{name}",
        validations=[
            ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(),
                )],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Predicate behaviour
# ---------------------------------------------------------------------------


class TestIsQualifireInternalFailure:
    def test_marker_true_dict_payload(self):
        vr = ValidationResult(
            validation_name="x_error",
            validation_type="threshold",
            severity=Severity.ERROR,
            message="oops",
            validation_base_name="x_error",
            details={"qualifire_internal_failure": True, "error": "boom"},
        )
        assert _is_qualifire_internal_failure(vr) is True

    def test_marker_false_when_absent(self):
        vr = ValidationResult(
            validation_name="x", validation_type="threshold",
            severity=Severity.ERROR, message="real finding",
            validation_base_name="x",
        )
        assert _is_qualifire_internal_failure(vr) is False

    def test_marker_handles_string_payload(self):
        """Round-7 codex review RISK fix: rows re-read from the
        persisted system table arrive with ``details_json`` as a
        JSON string. Predicate must JSON-decode and still classify
        correctly."""
        import json
        vr = ValidationResult(
            validation_name="x", validation_type="threshold",
            severity=Severity.ERROR, message="oops",
            validation_base_name="x",
            details=json.dumps({"qualifire_internal_failure": True}),
        )
        assert _is_qualifire_internal_failure(vr) is True

    def test_marker_handles_malformed_json_string_safely(self):
        """Malformed JSON string in details must not raise; predicate
        returns False (defensive — corrupted persistence rows
        shouldn't crash dashboards / dispatcher)."""
        vr = ValidationResult(
            validation_name="x", validation_type="threshold",
            severity=Severity.ERROR, message="oops",
            validation_base_name="x",
            details="not-json {{{",
        )
        assert _is_qualifire_internal_failure(vr) is False

    def test_marker_false_when_value_is_falsy(self):
        """Explicit ``qualifire_internal_failure=False`` is treated
        as not-an-internal-failure (boolean coercion)."""
        vr = ValidationResult(
            validation_name="x", validation_type="threshold",
            severity=Severity.ERROR, message="real",
            validation_base_name="x",
            details={"qualifire_internal_failure": False},
        )
        assert _is_qualifire_internal_failure(vr) is False


# ---------------------------------------------------------------------------
# Helper output shape
# ---------------------------------------------------------------------------


class TestValidatorExecutionErrorResult:
    def test_marker_set_on_synthesised_row(self):
        ds = _passing_dataset()
        val = ds.validations[0]
        result = _validator_execution_error_result(
            val, ds, RuntimeError("boom"),
        )
        assert result.severity == Severity.ERROR
        assert result.validation_name == "threshold_error"
        assert "Validation execution error: boom" in result.message
        assert result.details["qualifire_internal_failure"] is True
        assert result.details["error"] == "boom"

    def test_validation_name_override_for_dataset_layer(self):
        """The dataset-level wrapper passes
        ``validation_name='qualifire.dataset_error'`` so dataset
        outages are forensically distinguishable from validator
        outages even though both carry the marker."""
        ds = _passing_dataset()
        val = ds.validations[0]
        result = _validator_execution_error_result(
            val, ds, RuntimeError("boom"),
            validation_name="qualifire.dataset_error",
        )
        assert result.validation_name == "qualifire.dataset_error"
        assert result.details["qualifire_internal_failure"] is True


# ---------------------------------------------------------------------------
# Persistence-infra-failure suppression
# ---------------------------------------------------------------------------


class TestPersistenceInfraFailureSuppression:
    def test_infra_failure_skips_notifier_dispatch(self):
        """Storage write fails → notifications are NOT sent.
        Operators don't get false-positive Slack/email pages for
        what is actually a system-table outage, not a data-quality
        finding.
        """
        notifier = MagicMock()
        notifier.send.return_value = MagicMock(
            channel="slack", severity=Severity.ERROR,
            status="sent", message="",
        )

        storage = MagicMock()
        storage.write_results.side_effect = RuntimeError("Storage full")
        storage.read_validation_history_bulk.return_value = {}

        ds = _passing_dataset("infra_fail_ds")
        engine = _make_engine([ds], storage=storage,
                               notifiers={"slack": notifier})

        # Persistence-infrastructure outage raises
        # QualifireInternalError — distinct sibling class from
        # QualifireValidationError so engineering on-call queues
        # route differently from data-team queues.
        with pytest.raises(QualifireInternalError) as excinfo:
            engine.run()

        # NOT caught by `except QualifireValidationError` — the two
        # are siblings, not parent/child. Pin the contract.
        assert not isinstance(excinfo.value, QualifireValidationError)
        # Original storage exception chained as __cause__ (PEP 3134).
        assert isinstance(excinfo.value.__cause__, RuntimeError)
        assert "Storage full" in str(excinfo.value.__cause__)
        # Exception message references infra failure, not validation.
        assert "persistence failed" in str(excinfo.value).lower()
        # Notifier was NEVER invoked — the whole point of this fix.
        notifier.send.assert_not_called()

    def test_infra_failure_persists_engine_warning_via_best_effort_retry(self):
        """Even though the first persist call failed, the engine
        warning row is recorded in ``result.engine_warnings`` for
        operators inspecting the run programmatically."""
        storage = MagicMock()
        storage.write_results.side_effect = RuntimeError("Storage full")
        storage.read_validation_history_bulk.return_value = {}

        ds = _passing_dataset()
        engine = _make_engine([ds], storage=storage)

        with pytest.raises(QualifireInternalError) as excinfo:
            engine.run()

        result = excinfo.value.result
        persistence_warnings = [
            vr for vr in result.engine_warnings
            if vr.validation_base_name == "qualifire.persistence"
        ]
        assert len(persistence_warnings) >= 1
        # Carries the qualifire-internal-failure marker so any
        # downstream consumer (dashboard, audit query) can
        # distinguish it from a real data-quality finding.
        assert persistence_warnings[0].details.get(
            "qualifire_internal_failure"
        ) is True


# ---------------------------------------------------------------------------
# Validator-execution exception suppression
# ---------------------------------------------------------------------------


class TestValidatorExecutionExceptionSuppression:
    """The two engine call sites at ``engine.py:533`` (parallel
    pipeline) and ``~793`` (sequential) both wrap thrown exceptions
    via the shared ``_validator_execution_error_result`` helper.
    Tests exercise the sequential path here (parallel uses the
    same helper, so the assertion-shape is identical)."""

    def test_validator_exception_marked_and_skipped_from_notifications(
        self, monkeypatch,
    ):
        """A validator that throws an unhandled exception:
          1. produces a synthesised marked ``ValidationResult``
          2. notifications skip that row (configured channel
             sees zero invocations)
          3. ``QualifireInternalError`` is raised because the
             synthesised row is the only ERROR — no real data-
             quality findings exist alongside.
        """
        from qualifire.validation.threshold import ThresholdValidator

        # Force the validator to throw.
        def _explode(self, *a, **kw):
            raise RuntimeError("validator code blew up")
        monkeypatch.setattr(ThresholdValidator, "validate", _explode)

        notifier = MagicMock()
        notifier.send.return_value = MagicMock(
            channel="slack", severity=Severity.ERROR,
            status="sent", message="",
        )

        storage = MagicMock()
        storage.read_validation_history_bulk.return_value = {}

        ds = DatasetConfig(
            name="ds", table="db.t",
            validations=[
                ThresholdValidationConfig(
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"},
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(error={"min": 1}),
                    )],
                    notify={"error": ["slack"]},
                ),
            ],
        )
        engine = _make_engine([ds], storage=storage,
                               notifiers={"slack": notifier})

        # Run raises QualifireInternalError because the synthesised
        # row is severity=ERROR and there are no real data-quality
        # findings alongside (all-internal classifier branch).
        with pytest.raises(QualifireInternalError) as excinfo:
            engine.run()

        # Sibling, not subclass — confirm the dispatch contract.
        assert not isinstance(excinfo.value, QualifireValidationError)
        # Notifier was NOT invoked — qualifire-internal failure
        # rows are filtered from dispatch.
        notifier.send.assert_not_called()

    def test_validator_exception_row_reaches_persisted_history(
        self, monkeypatch,
    ):
        """The synthesised marked row is still persisted so
        operators inspecting the system table can audit the
        failure. Verify ``write_results`` was called with the
        marked row."""
        from qualifire.validation.threshold import ThresholdValidator

        def _explode(self, *a, **kw):
            raise RuntimeError("validator code blew up")
        monkeypatch.setattr(ThresholdValidator, "validate", _explode)

        captured_rows = []
        storage = MagicMock()
        storage.write_results.side_effect = lambda rows: captured_rows.extend(rows)
        storage.read_validation_history_bulk.return_value = {}

        ds = _passing_dataset()
        engine = _make_engine([ds], storage=storage)

        with pytest.raises(QualifireInternalError):
            engine.run()

        # Synthesised row was persisted (marker visible in the
        # details_json column the storage backend sees).
        threshold_error_rows = [
            r for r in captured_rows
            if r.get("validation_name") == "threshold_error"
        ]
        assert len(threshold_error_rows) >= 1, \
            "Synthesised threshold_error row should be persisted for forensics"
        details = threshold_error_rows[0].get("details_json")
        # details_json may be a dict (in-memory) or a string after
        # storage backends serialise it; predicate handles both.
        if isinstance(details, str):
            import json
            details = json.loads(details)
        assert details.get("qualifire_internal_failure") is True


# ---------------------------------------------------------------------------
# Sibling-not-subclass exception contract + mixed-error routing
# ---------------------------------------------------------------------------


class TestExceptionClassRouting:
    """Pin the contract that ``QualifireValidationError`` and
    ``QualifireInternalError`` are SIBLINGS, not parent/child, so
    ``except QualifireValidationError`` does NOT catch internal
    failures and vice versa. Operators routing one to the data-team
    queue and the other to engineering on-call rely on this."""

    def test_sibling_classes_not_caught_by_each_other(self):
        from qualifire.core.exceptions import QualifireError

        # Both subclass the top-level QualifireError for catch-all.
        assert issubclass(QualifireValidationError, QualifireError)
        assert issubclass(QualifireInternalError, QualifireError)
        # But neither subclasses the other.
        assert not issubclass(QualifireInternalError, QualifireValidationError)
        assert not issubclass(QualifireValidationError, QualifireInternalError)

    def test_mixed_findings_and_internal_raises_validation_error(
        self, monkeypatch,
    ):
        """When real data-quality findings AND qualifire-internal
        failures BOTH surface in the same run, the run raises
        ``QualifireValidationError`` (the data-team's exception
        class). The data findings take precedence — they're the
        actionable result for the operator's monitoring; the
        internal failures are forensic context surfaced via
        ``e.result``.

        The exception MESSAGE includes the count of internal
        failures so operators who want to also page engineering
        on-call can introspect.
        """
        from qualifire.validation.threshold import ThresholdValidator

        # Force the FIRST validator to throw; the SECOND validator
        # produces a real data finding (row_count=0 < error min=1).
        original_validate = ThresholdValidator.validate
        call_count = {"n": 0}

        def _explode_first_only(self, *a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("validator code blew up")
            return original_validate(self, *a, **kw)

        monkeypatch.setattr(ThresholdValidator, "validate", _explode_first_only)

        backend = MagicMock()
        backend.spark = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 0}

        storage = MagicMock()
        storage.read_validation_history_bulk.return_value = {}

        ds = DatasetConfig(
            name="ds", table="db.t",
            validations=[
                # Validator #1 — will throw (synthesised internal
                # failure with severity=ERROR).
                ThresholdValidationConfig(
                    name="v1",
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"},
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(error={"min": 1}),
                    )],
                ),
                # Validator #2 — runs cleanly, finds the row_count=0
                # is below threshold, ERROR severity (real finding).
                ThresholdValidationConfig(
                    name="v2",
                    collection=AggregationCollectionConfig(
                        expressions={"row_count": "COUNT(*)"},
                    ),
                    rules=[ThresholdRuleConfig(
                        metric="row_count",
                        thresholds=ThresholdLevels(error={"min": 1}),
                    )],
                ),
            ],
        )
        config = QualifireConfig(
            owner="o", bu="b", system_table="t", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(),
            config=config, notifiers={},
        )

        # Mixed: real data finding + internal failure → routes to
        # data-team queue (QualifireValidationError).
        with pytest.raises(QualifireValidationError) as excinfo:
            engine.run()

        # Sibling contract: NOT also caught by QualifireInternalError.
        assert not isinstance(excinfo.value, QualifireInternalError)
        # Message mentions internal-failure count alongside.
        assert "qualifire-internal" in str(excinfo.value)


# ---------------------------------------------------------------------------
# PersistenceOutcome shape
# ---------------------------------------------------------------------------


class TestPersistenceOutcomeShape:
    def test_ok_outcome_no_exception(self):
        outcome = PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
        assert outcome.kind == PersistenceOutcomeKind.OK
        assert outcome.exc is None

    def test_infra_failure_carries_exception(self):
        e = RuntimeError("boom")
        outcome = PersistenceOutcome(
            kind=PersistenceOutcomeKind.INFRA_FAILURE, exc=e,
        )
        assert outcome.kind == PersistenceOutcomeKind.INFRA_FAILURE
        assert outcome.exc is e

    def test_outcome_is_frozen(self):
        outcome = PersistenceOutcome(kind=PersistenceOutcomeKind.OK)
        with pytest.raises(Exception):  # FrozenInstanceError or similar
            outcome.kind = PersistenceOutcomeKind.INFRA_FAILURE  # type: ignore[misc]
