"""Quality test runner skeleton for the content-pack execution backend.

Runs the per-node ``quality.tests`` declared in the node YAML against
the materialised target DataFrame.

**v0.3 implementation status:**

* Fully implemented (run against the target DataFrame, produce a real
  pass/fail):
    * ``not_null``        — every cell in declared columns is non-null
    * ``unique``          — declared columns form a unique key
    * ``accepted_values`` — column values are a subset of declared list
    * ``row_count_min``   — target row count ≥ declared minimum
* Deferred (recognised by the schema but reported as
  ``status='deferred'`` — caller treats them as soft warnings, never as
  pass/fail):
    * ``row_count_delta``      — needs prior-run row count from state
    * ``freshness``            — needs wall-clock + max age math
    * ``reconcile_to``         — cross-source aggregate diff
    * ``referential_integrity``— FK probe against parent table
    * ``custom``               — third-party hook

**Hard-cursor-commit gate:** the caller in ``execute_node`` MUST refuse to
advance the cursor (write a success state row) when ``report.failures`` is
non-empty. Deferred reports do NOT block the cursor; they're informational.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..schema.medallion_pack import NodeYaml, QualityTest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from .sql_renderer import RunContext


# ---------------------------------------------------------------------------
# AIDPF error codes
# ---------------------------------------------------------------------------

AIDPF_8010_QUALITY_TEST_FAILED = "AIDPF-8010"
"""A fully-implemented quality test produced one or more failing rows."""

AIDPF_8011_QUALITY_TEST_DEFERRED = "AIDPF-8011"
"""A quality test type is recognised by the schema but not executed in v0.3."""


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TestResult:
    """One quality test's outcome.

    Attributes:
        test_type: ``"not_null"`` / ``"unique"`` / etc.
        status: ``"passed"`` / ``"failed"`` / ``"deferred"``.
        message: human-readable diagnostic (failure detail, deferral
            reason, or passing-row summary).
        failing_row_count: rows that violated the test (0 for passed/
            deferred). Used for triage; the caller doesn't read this.
    """

    test_type: str
    status: str
    message: str = ""
    failing_row_count: int = 0


@dataclass(frozen=True)
class QualityReport:
    """Aggregated quality-test outcome for a node.

    The caller (``execute_node``) treats:

    * ``failures`` non-empty → write a ``status='quality_failed'`` state
      row, preserve prior watermark, return failure.
    * ``failures`` empty regardless of ``deferred`` count → advance the
      cursor (deferred tests are informational, not blocking).
    """

    failures: tuple[TestResult, ...] = ()
    passed: tuple[TestResult, ...] = ()
    deferred: tuple[TestResult, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.failures


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


# Test types currently executed.
_IMPLEMENTED_TEST_TYPES: frozenset[str] = frozenset(
    {"not_null", "unique", "accepted_values", "row_count_min"}
)

# Test types recognised by the schema but deferred by the runner.
_DEFERRED_TEST_TYPES: frozenset[str] = frozenset(
    {
        "row_count_delta",
        "freshness",
        "reconcile_to",
        "referential_integrity",
        "custom",
    }
)


def run_quality_tests(
    spark: "SparkSession",
    node: NodeYaml,
    target_df: "DataFrame",
    ctx: "RunContext",
) -> QualityReport:
    """Run every declared quality test against the materialised target.

    Args:
        spark: live Spark session for the materialised-row probes.
        node: validated NodeYaml. ``node.quality.tests`` is the list of
            declared tests (empty list = no tests, returns an empty
            passing report).
        target_df: the materialised target as a Spark DataFrame
            (typically ``spark.table(target)`` from the executor).
        ctx: render context (unused in v0.3 — threaded through for
            future ``reconcile_to``/``freshness`` implementations).

    Returns:
        :class:`QualityReport` with passed / failed / deferred test
        outcomes.

    Notes:
        Does NOT raise on a quality-test failure — collects them so the
        caller can write a soft state row and preserve the prior
        watermark. Programmer-error conditions (Spark exception, target
        DataFrame ``None``) still raise.
    """
    quality = getattr(node, "quality", None)
    if quality is None or not quality.tests:
        return QualityReport()

    passed: list[TestResult] = []
    failed: list[TestResult] = []
    deferred: list[TestResult] = []

    for test in quality.tests:
        test_type = test.type
        if test_type in _IMPLEMENTED_TEST_TYPES:
            result = _run_implemented_test(test, target_df)
            if result.status == "failed":
                failed.append(result)
            else:
                passed.append(result)
        elif test_type in _DEFERRED_TEST_TYPES:
            deferred.append(
                TestResult(
                    test_type=test_type,
                    status="deferred",
                    message=(
                        f"{AIDPF_8011_QUALITY_TEST_DEFERRED}: test type "
                        f"{test_type!r} is recognised by the v0.3 schema but "
                        f"deferred to a later release. The runner reports this "
                        f"test as 'deferred' and continues; this does NOT "
                        f"block cursor advancement."
                    ),
                )
            )
        else:
            # Defensive — the discriminated union should reject unknown
            # types at schema-load time. Surface as a deferred result so
            # the caller doesn't crash on a future schema extension.
            deferred.append(
                TestResult(
                    test_type=test_type,
                    status="deferred",
                    message=(
                        f"{AIDPF_8011_QUALITY_TEST_DEFERRED}: unknown test "
                        f"type {test_type!r}. Treating as deferred."
                    ),
                )
            )

    return QualityReport(
        failures=tuple(failed),
        passed=tuple(passed),
        deferred=tuple(deferred),
    )


# ---------------------------------------------------------------------------
# Per-test-type runners (implemented only)
# ---------------------------------------------------------------------------


def _run_implemented_test(test: QualityTest, target_df: "DataFrame") -> TestResult:
    """Dispatch to the implementation for a v0.3 fully-implemented type."""
    if test.type == "not_null":
        return _run_not_null(test, target_df)
    if test.type == "unique":
        return _run_unique(test, target_df)
    if test.type == "accepted_values":
        return _run_accepted_values(test, target_df)
    if test.type == "row_count_min":
        return _run_row_count_min(test, target_df)
    # Should be unreachable given _IMPLEMENTED_TEST_TYPES filtering.
    return TestResult(
        test_type=test.type,
        status="deferred",
        message=f"internal: no runner for {test.type!r}",
    )


def _run_not_null(test, target_df: "DataFrame") -> TestResult:
    """Fail if any cell in the declared columns is NULL."""
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    cols = test.columns
    # OR-joined NULL predicate across all declared columns.
    null_predicate = F.lit(False)
    for c in cols:
        null_predicate = null_predicate | F.col(c).isNull()
    fail_count = int(target_df.filter(null_predicate).count())
    if fail_count > 0:
        return TestResult(
            test_type="not_null",
            status="failed",
            message=(
                f"{AIDPF_8010_QUALITY_TEST_FAILED}: not_null test found "
                f"{fail_count} row(s) with at least one NULL in columns "
                f"{cols!r}."
            ),
            failing_row_count=fail_count,
        )
    return TestResult(
        test_type="not_null",
        status="passed",
        message=f"not_null({cols!r}) passed.",
    )


def _run_unique(test, target_df: "DataFrame") -> TestResult:
    """Fail if any (declared-columns) tuple appears more than once."""
    cols = test.columns
    duplicate_count = int(
        target_df.groupBy(*cols).count().filter("count > 1").count()
    )
    if duplicate_count > 0:
        return TestResult(
            test_type="unique",
            status="failed",
            message=(
                f"{AIDPF_8010_QUALITY_TEST_FAILED}: unique test found "
                f"{duplicate_count} duplicate key(s) on {cols!r}."
            ),
            failing_row_count=duplicate_count,
        )
    return TestResult(
        test_type="unique",
        status="passed",
        message=f"unique({cols!r}) passed.",
    )


def _run_accepted_values(test, target_df: "DataFrame") -> TestResult:
    """Fail if the column contains a value outside the declared set."""
    from pyspark.sql import functions as F  # type: ignore[import-not-found]

    col = test.column
    fail_count = int(
        target_df.filter(~F.col(col).isin(list(test.values))).count()
    )
    if fail_count > 0:
        return TestResult(
            test_type="accepted_values",
            status="failed",
            message=(
                f"{AIDPF_8010_QUALITY_TEST_FAILED}: accepted_values test "
                f"found {fail_count} row(s) where {col!r} is outside "
                f"{list(test.values)!r}."
            ),
            failing_row_count=fail_count,
        )
    return TestResult(
        test_type="accepted_values",
        status="passed",
        message=f"accepted_values({col!r} in {list(test.values)!r}) passed.",
    )


def _run_row_count_min(test, target_df: "DataFrame") -> TestResult:
    """Fail if target has fewer rows than the declared minimum."""
    actual = int(target_df.count())
    if actual < test.min:
        return TestResult(
            test_type="row_count_min",
            status="failed",
            message=(
                f"{AIDPF_8010_QUALITY_TEST_FAILED}: row_count_min test "
                f"failed — actual {actual} < min {test.min}."
            ),
            failing_row_count=test.min - actual,
        )
    return TestResult(
        test_type="row_count_min",
        status="passed",
        message=f"row_count_min(actual={actual} >= min={test.min}) passed.",
    )
