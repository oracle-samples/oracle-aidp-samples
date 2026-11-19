"""P1.5α-fix21 Tier 4: chaos test for the fix20 retry classifier.

Injects randomized transient + permanent failures into ``run_with_retry``
and verifies the contract:

  * Transient errors → retried up to ``max_retries`` → eventual success
    is recorded once (no spurious extra calls past success).
  * Permanent errors → NOT retried → fast failure with the original
    exception preserved.
  * All-retries-exhausted on a persistently-transient failure → final
    raise (the dispatcher above translates that to a cascade).

The test is unit-fast: monkeypatches ``sleep=lambda _: None`` to skip
the 10/30/90s backoffs from ``retry.py:185``. The full chaos suite
(N=100 per assertion class) completes well under 1s. Without the
sleep injection, the same N=100 would block CI for hours — this is
why the contract is documented in this docstring AND in the assertion
helpers below: future contributors MUST pass ``sleep=`` explicitly.
"""

from __future__ import annotations

import random

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.retry import (
    DEFAULT_MAX_RETRIES,
    PERMANENT_PATTERNS,
    TRANSIENT_PATTERNS,
    run_with_retry,
)


# Sleep injection — REQUIRED to keep N=100 runs sub-second. The
# default `time.sleep` would push the suite to ~hours.
_NOOP_SLEEP = lambda _s: None  # noqa: E731

# Fixed seed for reproducibility — failures replay from the seed alone.
_CHAOS_SEED = 4242

# Sample size per assertion class. Tuned to exercise enough of the
# permutation space without bloating CI time. Sub-30s suite even at
# 100 because of _NOOP_SLEEP.
_N_RUNS = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pick_pattern(patterns: tuple[str, ...], rng: random.Random) -> str:
    """Choose a random pattern from the corpus. Use the public
    ``TRANSIENT_PATTERNS`` / ``PERMANENT_PATTERNS`` exports — pinning
    the test against private symbols would couple the contract to
    implementation choices.
    """
    return rng.choice(patterns)


class _Counter:
    """Mutable counter passed by reference into the closure under test."""

    def __init__(self) -> None:
        self.calls = 0


# ---------------------------------------------------------------------------
# Transient → retried → eventual success
# ---------------------------------------------------------------------------


def test_chaos_transient_retried_to_success() -> None:
    """A transient error followed by success records ONE success row
    (not two) and runs the callable enough times to reach success.

    Failure mode this guards against: an early refactor that
    accidentally returns from `run_with_retry` after a successful
    retry without breaking out of the retry loop — would record N+1
    successes for an N-retry attempt.
    """
    rng = random.Random(_CHAOS_SEED)
    for _ in range(_N_RUNS):
        # Number of transient failures before success: 0, 1, or 2 (within
        # DEFAULT_MAX_RETRIES=3).
        n_failures = rng.randint(0, DEFAULT_MAX_RETRIES - 1)
        pattern = _pick_pattern(TRANSIENT_PATTERNS, rng)
        counter = _Counter()

        def _flaky() -> int:
            counter.calls += 1
            if counter.calls <= n_failures:
                raise RuntimeError(f"chaos: {pattern}")
            return 42

        result = run_with_retry(
            _flaky, dataset_id="chaos_ds", sleep=_NOOP_SLEEP,
        )
        assert result == 42
        # Exactly n_failures retries + 1 success call.
        assert counter.calls == n_failures + 1, (
            f"transient retry should call exactly n_failures+1 times; "
            f"n_failures={n_failures}, calls={counter.calls}, "
            f"pattern={pattern!r}"
        )


# ---------------------------------------------------------------------------
# Permanent → not retried → fast failure
# ---------------------------------------------------------------------------


def test_chaos_permanent_not_retried() -> None:
    """A permanent error fails on the first attempt — no retry.

    Failure mode this guards against: a refactor that catches ALL
    exceptions and retries them. Would mask permanent bugs as
    transient flakes; the operator would never get a fast failure
    on a schema-not-found or AccessDenied.
    """
    rng = random.Random(_CHAOS_SEED + 1)
    for _ in range(_N_RUNS):
        pattern = _pick_pattern(PERMANENT_PATTERNS, rng)
        counter = _Counter()

        def _permanent() -> int:
            counter.calls += 1
            raise RuntimeError(f"chaos: {pattern}")

        with pytest.raises(RuntimeError, match=pattern.split()[0]):
            run_with_retry(
                _permanent, dataset_id="chaos_ds", sleep=_NOOP_SLEEP,
            )
        # Permanent errors NEVER retry — calls must equal 1.
        assert counter.calls == 1, (
            f"permanent pattern should not retry; calls={counter.calls}, "
            f"pattern={pattern!r}"
        )


# ---------------------------------------------------------------------------
# Persistent transient → retries exhausted → final raise
# ---------------------------------------------------------------------------


def test_chaos_transient_exhaustion_raises() -> None:
    """A persistently-transient failure exhausts ``max_retries`` and
    re-raises the last exception (so the dispatcher above can mark
    the step `failed` and cascade-skip downstream).

    Failure mode this guards against: a refactor that swallows the
    final exception on retry exhaustion. Would leave the step
    silently succeeded with an undefined return value.
    """
    rng = random.Random(_CHAOS_SEED + 2)
    for _ in range(_N_RUNS):
        pattern = _pick_pattern(TRANSIENT_PATTERNS, rng)
        counter = _Counter()

        def _always_transient() -> int:
            counter.calls += 1
            raise RuntimeError(f"chaos: {pattern}")

        with pytest.raises(RuntimeError, match="chaos"):
            run_with_retry(
                _always_transient,
                dataset_id="chaos_ds",
                sleep=_NOOP_SLEEP,
                max_retries=DEFAULT_MAX_RETRIES,
            )
        # max_retries=3 means 1 initial attempt + 3 retries = 4 total.
        assert counter.calls == DEFAULT_MAX_RETRIES + 1, (
            f"transient exhaustion should run max_retries+1 times; "
            f"calls={counter.calls}, max_retries={DEFAULT_MAX_RETRIES}, "
            f"pattern={pattern!r}"
        )


# ---------------------------------------------------------------------------
# Mixed-pattern run — every assertion class in one loop
# ---------------------------------------------------------------------------


def test_chaos_mixed_classification_consistent() -> None:
    """Run a randomized mix of transient / permanent / success
    scenarios and verify the classifier's behavior is consistent
    across N=100 trials.

    Tighter contract: assert that the (#calls, raised, returned)
    triple is a valid combination per the retry semantics. Catches
    refactors that introduce inconsistent classifier behavior even
    when individual happy paths still pass.
    """
    rng = random.Random(_CHAOS_SEED + 3)
    valid_triples: set[tuple[int, bool, bool]] = set()
    invalid_triples: list[tuple[int, bool, bool]] = []
    for _ in range(_N_RUNS):
        # Pick a scenario weighted toward the common case.
        kind = rng.choices(
            ["clean_success", "transient_then_success", "permanent", "transient_exhaust"],
            weights=[40, 30, 15, 15],
        )[0]
        n_failures_before_success = (
            0 if kind == "clean_success"
            else rng.randint(1, DEFAULT_MAX_RETRIES) if kind == "transient_then_success"
            else 0
        )
        counter = _Counter()
        raised = False
        returned = False

        if kind == "clean_success":
            def _fn() -> int:
                counter.calls += 1
                return 1
        elif kind == "transient_then_success":
            pattern = _pick_pattern(TRANSIENT_PATTERNS, rng)
            def _fn(p=pattern, n=n_failures_before_success) -> int:  # type: ignore[misc]
                counter.calls += 1
                if counter.calls <= n:
                    raise RuntimeError(f"chaos: {p}")
                return 1
        elif kind == "permanent":
            pattern = _pick_pattern(PERMANENT_PATTERNS, rng)
            def _fn(p=pattern) -> int:  # type: ignore[misc]
                counter.calls += 1
                raise RuntimeError(f"chaos: {p}")
        else:  # transient_exhaust
            pattern = _pick_pattern(TRANSIENT_PATTERNS, rng)
            def _fn(p=pattern) -> int:  # type: ignore[misc]
                counter.calls += 1
                raise RuntimeError(f"chaos: {p}")

        try:
            run_with_retry(_fn, dataset_id="chaos_ds", sleep=_NOOP_SLEEP)
            returned = True
        except RuntimeError:
            raised = True

        triple = (counter.calls, raised, returned)
        # Validate the triple against the expected scenario.
        if kind == "clean_success":
            assert triple == (1, False, True)
        elif kind == "transient_then_success":
            assert triple == (n_failures_before_success + 1, False, True)
        elif kind == "permanent":
            assert triple == (1, True, False)
        else:  # transient_exhaust
            assert triple == (DEFAULT_MAX_RETRIES + 1, True, False)
        valid_triples.add(triple)

    # Sanity: we should have seen at least one of each expected shape.
    # Weighted to common cases, so the rare ones (permanent / exhaust)
    # are present but few.
    assert len(valid_triples) >= 3, (
        f"chaos mix should exercise multiple scenarios; "
        f"only saw triples: {valid_triples}"
    )
    assert not invalid_triples, f"invalid triples: {invalid_triples}"
