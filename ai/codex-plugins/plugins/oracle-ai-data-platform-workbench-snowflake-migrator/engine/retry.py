"""One retry helper for every network call in the engine.

Backoff is exponential -- base_delay * multiplier ** (attempt - 1), capped at
max_delay, max_attempts in total -- and the numbers come from `retry:` in
snowmig-config.yaml (migration_config.retry_block), set once per process by
`snowmig.main`. No call site carries its own delays.

WHAT is retried is the caller's decision, because it depends on whether the
call is safe to repeat. `is_retryable(read=...)` encodes the rule:

  * a READ is retried on any transient failure: 429, 5xx, a timeout, a
    dropped connection;
  * a WRITE is retried only on a response that proves it was NOT applied:
    429 (throttled) and 409 "not in an active state" (the target was not
    ready to accept it). A write that answered 5xx may have been applied,
    and repeating it is how a create runs twice -- so it is not retried.

Every retry is logged as a retry -- attempt, delay, the error that caused it
-- and recorded, so the phase report can say how many happened inside each
phase rather than burying them as debug noise.
"""
from __future__ import annotations

import dataclasses
import re
import sys
import time
from typing import Callable, TypeVar

__all__ = ["RetryPolicy", "current_policy", "drain_events", "is_retryable",
           "note_retry", "retry_call", "set_policy"]

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class RetryPolicy:
    base_delay: float = 2.0
    multiplier: float = 2.0
    max_attempts: int = 4
    max_delay: float = 60.0

    def delay(self, attempt: int) -> float:
        """The wait before retry number `attempt` (1-based)."""
        return float(min(self.max_delay,
                         self.base_delay * self.multiplier ** (attempt - 1)))

    def schedule(self) -> tuple[float, ...]:
        """Every wait, in order: one fewer than max_attempts."""
        return tuple(self.delay(n) for n in range(1, self.max_attempts))


_POLICY = RetryPolicy()
_EVENTS: list[dict] = []


def set_policy(policy: RetryPolicy) -> None:
    global _POLICY
    _POLICY = policy


def current_policy() -> RetryPolicy:
    return _POLICY


def drain_events() -> list[dict]:
    """The retries recorded since the last drain, and clear them."""
    out = list(_EVENTS)
    _EVENTS.clear()
    return out


def note_retry(label: str, attempt: int, max_attempts: int, delay: float,
               error: BaseException | str) -> None:
    """Say, as a retry, that `label` failed and will be tried again."""
    text = " ".join(str(error).split())[:200]
    # stdout, with the rest of the progress output: a retry is not a
    # failure, and putting it on stderr buried the one-line `error:` a
    # caller reads when the retries are finally exhausted.
    print(f"  RETRY {label}: attempt {attempt + 1}/{max_attempts} in "
          f"{delay:.1f}s after: {text}", flush=True)
    _EVENTS.append({"label": label, "attempt": attempt + 1,
                    "max_attempts": max_attempts, "delay_seconds": delay,
                    "error": text,
                    "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


_TRANSIENT = re.compile(
    r"\b(429|500|502|503|504)\b|too ?many ?requests|throttl|timed? ?out|"
    r"timeout|temporar|unavailable|connection (reset|refused|aborted)|"
    r"internalerror|service ?unavailable", re.IGNORECASE)
_NOT_APPLIED = re.compile(
    r"\b429\b|too ?many ?requests|throttl|not in an active state",
    re.IGNORECASE)


def is_retryable(*, read: bool) -> Callable[[BaseException], bool]:
    """The rule for one call: reads on anything transient, writes only on a
    response that proves nothing was applied."""
    def check(exc: BaseException) -> bool:
        text = str(exc)
        if _NOT_APPLIED.search(text):
            return True
        return bool(read and _TRANSIENT.search(text))
    return check


def retry_call(fn: Callable[[], T], *, label: str,
               retryable: Callable[[BaseException], bool],
               policy: RetryPolicy | None = None,
               sleep: Callable[[float], None] | None = None) -> T:
    """Call `fn`, retrying what `retryable` allows, per `policy`."""
    policy = policy or _POLICY
    sleep = sleep or time.sleep
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt >= policy.max_attempts or not retryable(exc):
                raise
            wait = policy.delay(attempt)
            note_retry(label, attempt, policy.max_attempts, wait, exc)
            sleep(wait)
    raise AssertionError("unreachable")  # pragma: no cover
