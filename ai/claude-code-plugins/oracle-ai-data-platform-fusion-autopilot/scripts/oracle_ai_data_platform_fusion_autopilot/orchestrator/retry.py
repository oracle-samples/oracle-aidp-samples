"""Transient-exception classification + retry-with-backoff helper.

:func:`run_with_retry` retries a thunk on transient infrastructure hiccups
(OCI Object Storage 5xx, Spark executor loss, BICC connection reset) with
exponential backoff, while permanent errors (schema-not-found, auth denied,
Delta merge failures) skip retry entirely and fail fast — so real bugs are
never masked. The classifier (:func:`is_transient`) and backoff loop are
covered by ``tests/integration/test_retry_chaos.py``.

NOTE — not yet wired into the content-pack node loop. The current per-node
runner (``orchestrator/__init__.py`` → ``cp_execute_node``) dispatches each
node directly, WITHOUT this wrapper, so transient failures currently fail the
node rather than being retried. Wrapping ``cp_execute_node`` in
``run_with_retry`` is a tracked follow-up (it changes live extract/MERGE
runtime behavior and needs its own chaos-test validation); until then this
module provides the classifier + helper for callers that opt in, not an
always-on resilience tier. Resume-from-checkpoint (``--resume <run_id>``) is a
separate, live capability in ``orchestrator/resume.py``.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Callable, TypeVar

_LOG = logging.getLogger("oracle_ai_data_platform_fusion_autopilot.orchestrator.retry")

T = TypeVar("T")


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

# Substrings that mark an exception as worth retrying. Bias toward
# infrastructure-y / transport-y patterns; specifically NOT "TimeoutException"
# alone (BICC can take ~17min on a 10M-row pull legitimately).
TRANSIENT_PATTERNS: tuple[str, ...] = (
    "Connection reset",
    "Connection refused",       # only if NOT during preflight (preflight classifies separately)
    "Read timed out",
    "Read timeout",
    "Broken pipe",
    "Executor lost",
    "TaskKilled",
    "503 Service Unavailable",
    "ServiceUnavailable",
    "SlowDown",                # OCI Object Storage / S3 throttle
    "RequestTimeout",
    "Throttling",
    "Temporarily unavailable",
    "Object storage temporarily unavailable",
    "Could not get block locations",  # transient HDFS-style read after partial commit
    "lost task",
    "lost executor",
)


# Substrings that mark an exception as DEFINITELY permanent — never retry these.
# Listed even if they wouldn't match TRANSIENT_PATTERNS, as a safety net for
# error messages that combine transient-looking words with a permanent root
# cause.
#
# TRANSIENT_PATTERNS and PERMANENT_PATTERNS are public so chaos tests
# can import them as the contract corpus — pinning behavior against a
# private symbol would couple the test to implementation choices.
PERMANENT_PATTERNS: tuple[str, ...] = (
    # BICC catalog / schema bugs.
    "DATA_ACCESS_LAYER_0031",   # Schema X not found
    "DATA_ACCESS_LAYER_0032",   # PVO not found (inferred variant)
    # Auth / authorization
    "401 Unauthorized",
    "403 Forbidden",
    "AccessDenied",
    "NotAuthorizedOrNotFound",
    "authentication failed",
    # Delta correctness errors (schema, constraint, protocol)
    "DELTA_FAILED_TO_MERGE_FIELDS",
    "DELTA_INVARIANT_VIOLATED",
    "NOT NULL constraint",
    "AnalysisException",        # Spark SQL / Catalog errors are not transient
    # Plan logic
    "MissingDependencyError",
    "BundleVersionMismatchError",
    "UnsupportedModeError",
    "CredentialResolutionError",
    # Resource exhaustion that retry won't fix
    "OutOfMemoryError",
    "Disk quota exceeded",
)


# Python builtin exceptions that are intrinsically transient — OS-layer
# connection/network failures where retry is the canonical response.
_TRANSIENT_EXCEPTION_TYPES: tuple[type[BaseException], ...] = (
    ConnectionResetError,
    ConnectionRefusedError,
    ConnectionAbortedError,
    BrokenPipeError,
    TimeoutError,  # Python's builtin TimeoutError (not the misnamed Spark-side one)
)


def is_transient(exc: BaseException) -> bool:
    """Classify an exception as transient (retry-worthy) vs permanent.

    Conservative on the transient side: anything that looks like a real bug —
    schema mismatch, auth, Delta correctness — is permanent. Anything that
    looks like infrastructure flakiness (timeouts, throttles, executor loss,
    connection resets) is transient.

    The classifier walks:
      1. The Python exception type (matches OS-level transients like
         ``ConnectionResetError`` even when the message is empty).
      2. ``str(exc)`` for known transient/permanent patterns.
      3. Py4JJavaError's nested ``.java_exception.getMessage()`` + class name
         (raw Py4JJavaError repr is often empty / class-name only; the Java
         side has the real signal).

    Permanent patterns ALWAYS win over transient — a message containing
    "Connection reset" and "AccessDenied" classifies as permanent because
    retry can't fix authorization.
    """
    candidates: list[str] = [str(exc), type(exc).__name__]
    # Py4JJavaError exposes the underlying Java exception with the useful
    # message. The repr() at the Python level often loses it.
    java_exc = getattr(exc, "java_exception", None)
    if java_exc is not None:
        try:
            jmsg = java_exc.getMessage()
            if jmsg:
                candidates.append(str(jmsg))
            jcls = java_exc.getClass().getName()
            candidates.append(jcls)
        except Exception:
            # Defensive: introspection of the Py4J wrapper can fail in weird
            # ways. We still want the classifier to return — fall back to the
            # Python-side ``str(exc)`` candidates already collected. NOT
            # ``except BaseException`` — KeyboardInterrupt during introspection
            # must propagate so the user can actually Ctrl-C.
            pass

    blob = "\n".join(candidates)

    # Permanent wins over transient — never retry a known-permanent pattern,
    # even if the message happens to also contain a transient-looking word.
    for pattern in PERMANENT_PATTERNS:
        if pattern in blob:
            return False

    # OS-layer transients are recognized by type even when the message is empty.
    if isinstance(exc, _TRANSIENT_EXCEPTION_TYPES):
        return True

    for pattern in TRANSIENT_PATTERNS:
        if pattern in blob:
            return True
    # Default: NOT transient. Bias toward fail-fast so unknown failure modes
    # surface to the operator instead of being silently retried.
    return False


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

# Exponential backoff: 10s → 30s → 90s. Total worst-case wall time for 3
# attempts: ~130s + actual operation time. Tuned to handle OCI Object Storage
# rate-limits + Spark executor recovery without bloating a clean run.
DEFAULT_MAX_RETRIES: int = 3
DEFAULT_BACKOFF_BASE_S: float = 10.0
DEFAULT_BACKOFF_FACTOR: float = 3.0


def _backoff_seconds(attempt: int, *, base_s: float, factor: float) -> float:
    """Compute the sleep duration BEFORE retry ``attempt`` (1-indexed)."""
    return base_s * (factor ** (attempt - 1))


def run_with_retry(
    fn: Callable[[], T],
    *,
    dataset_id: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base_s: float = DEFAULT_BACKOFF_BASE_S,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run ``fn`` with retry on transient exceptions.

    Each attempt is wrapped in try/except. On a transient exception (per
    :func:`is_transient`), sleep with exponential backoff then retry. On a
    permanent exception OR if retries are exhausted, re-raise — the caller's
    outer try/except in ``_execute_node`` will mark the step ``failed``.

    Args:
        fn: Zero-arg callable performing the actual work (extract → enrich →
            write, or builder() for silver/gold). Closure captures spark, paths,
            bundle, run_id.
        dataset_id: For log messages — never used in retry logic itself.
        max_retries: Number of retries AFTER the first attempt. 0 means no
            retries (one attempt total). Default 3 = up to 4 attempts.
        backoff_base_s: Sleep before retry attempt #1.
        backoff_factor: Multiplier for each subsequent retry. Default 3 ×
            base 10s → 10s, 30s, 90s.
        sleep: Injected for testability — defaults to :func:`time.sleep`.

    Returns:
        Whatever ``fn`` returned on the first successful attempt.

    Raises:
        The last exception encountered, after retries are exhausted OR
        immediately on a permanent exception.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 2):  # 1, 2, ..., max_retries+1
        try:
            return fn()
        except (KeyboardInterrupt, SystemExit):
            # Operator Ctrl-C / sys.exit() must propagate IMMEDIATELY. Without
            # this re-raise, ``except Exception`` widened to ``except BaseException``
            # would catch the interrupt and either retry (transient classifier
            # may match e.g. "Interrupted") or treat it as a permanent failure
            # — both wrong. The user wants the process to stop.
            raise
        except Exception as exc:
            last_exc = exc
            if not is_transient(exc):
                _LOG.debug(
                    "%s: permanent exception, not retrying: %s",
                    dataset_id, type(exc).__name__,
                )
                raise
            if attempt > max_retries:
                _LOG.warning(
                    "%s: exhausted %d retries after transient exception (%s); failing step",
                    dataset_id, max_retries, type(exc).__name__,
                )
                raise
            sleep_s = _backoff_seconds(attempt, base_s=backoff_base_s, factor=backoff_factor)
            _LOG.warning(
                "%s: attempt %d/%d failed with transient %s: %s; sleeping %.0fs before retry",
                dataset_id, attempt, max_retries + 1,
                type(exc).__name__, str(exc)[:200], sleep_s,
            )
            sleep(sleep_s)

    # Unreachable: loop either returns on success or raises on exhaustion.
    raise RuntimeError("run_with_retry: loop exited without return/raise") from last_exc


__all__ = [
    "is_transient",
    "run_with_retry",
    "DEFAULT_MAX_RETRIES",
    "TRANSIENT_PATTERNS",
    "PERMANENT_PATTERNS",
]
