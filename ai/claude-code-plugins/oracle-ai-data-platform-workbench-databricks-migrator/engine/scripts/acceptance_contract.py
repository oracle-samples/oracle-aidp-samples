"""Optional batch/streaming acceptance contract for migrated notebooks.

Lets a notebook author declare what "drained" or "complete" means for that
specific workload, beyond "all cells executed without error". A contract is
opt-in: notebooks without one continue to use the existing PASS/PARTIAL/FAIL
verification path unchanged.

Pattern adapted from the prior internal pattern
`aidp-batch-stream-acceptance` skill — specifically the consecutive-zero-window
convergence rule, which guards against false positives caused by brief
zero-readings between streaming micro-batches.

Contract format (sibling YAML file or inline in manifest):

    acceptance_contract:
      pending_count_sql: |
        SELECT COUNT(*) FROM example_schema.file_state
        WHERE state IN ('PENDING_SILVER', 'PROCESSING')
      zero_window: 2          # require N consecutive zero readings
      sleep_between_s: 30     # seconds between probes
      max_attempts: 10        # give up after N tries

Outcomes:
  - PASS: pending_count reached 0 for `zero_window` consecutive readings
  - VIOLATED: max_attempts exhausted with non-zero pending_count
  - SKIPPED: no contract declared (caller decides what to do)

Behavior:
  - Contract is OPTIONAL; absent = no-op (back-compat for callers without a contract)
  - Runs AFTER the final cell of a notebook has passed all existing verification
  - Reuses the migrator's existing AIDPSession singleton — no new connections
  - Reports each attempt's count + final verdict in JOB_REPORT.md
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional


# Default values — opinionated for streaming use case where small backlogs
# need a moment to drain between micro-batches.
DEFAULTS = {
    "zero_window": 1,
    "sleep_between_s": 30,
    "max_attempts": 10,
}


class ContractParseError(ValueError):
    """Raised when a contract YAML/dict fails validation."""
    pass


@dataclass
class AcceptanceContract:
    """Parsed acceptance contract for a single notebook."""
    pending_count_sql: str
    zero_window: int = 1
    sleep_between_s: int = 30
    max_attempts: int = 10

    @classmethod
    def from_dict(cls, raw: dict) -> "AcceptanceContract":
        """Parse a contract dict (from YAML or manifest JSON).

        Raises ContractParseError on missing or bad fields.
        """
        if not isinstance(raw, dict):
            raise ContractParseError(f"contract must be a dict, got {type(raw).__name__}")
        sql = raw.get("pending_count_sql")
        if not sql or not isinstance(sql, str) or not sql.strip():
            raise ContractParseError("contract.pending_count_sql is required and must be a non-empty string")

        # Allow ints or numeric strings for numeric fields (YAML quirks)
        def _as_int(key: str, default: int) -> int:
            v = raw.get(key, default)
            if v is None:
                return default
            try:
                iv = int(v)
            except (TypeError, ValueError):
                raise ContractParseError(f"contract.{key} must be int-compatible, got {v!r}")
            if iv < 1:
                raise ContractParseError(f"contract.{key} must be >= 1, got {iv}")
            return iv

        return cls(
            pending_count_sql=sql.strip(),
            zero_window=_as_int("zero_window", DEFAULTS["zero_window"]),
            sleep_between_s=_as_int("sleep_between_s", DEFAULTS["sleep_between_s"]),
            max_attempts=_as_int("max_attempts", DEFAULTS["max_attempts"]),
        )


@dataclass
class ContractResult:
    """Result of evaluating an acceptance contract."""
    passed: bool
    attempts: int                  # how many probe attempts were made
    readings: list[int] = field(default_factory=list)  # pending_count per attempt
    elapsed_s: float = 0.0
    reason: str = ""               # human-readable PASS / VIOLATED summary
    error: Optional[str] = None    # set if a probe SQL itself failed
    contract: Optional[AcceptanceContract] = None

    @property
    def status(self) -> str:
        """One-token status for reports: PASSED | VIOLATED | ERROR | SKIPPED."""
        if self.error:
            return "ERROR"
        if self.passed:
            return "PASSED"
        return "VIOLATED"

    @property
    def diagnostic(self) -> str:
        """Short hint for why a non-PASS verdict occurred (or why a PASS was clean)."""
        if self.passed:
            return f"drained in {self.attempts} attempts ({self.elapsed_s:.0f}s)"
        if self.error:
            return f"contract probe failed: {self.error[:200]}"
        # VIOLATED — characterize the readings
        tail = self.readings[-5:] if self.readings else []
        if len(set(tail)) == 1:
            return (f"count stable at {tail[0]} across last {len(tail)} attempts — "
                    "pipeline is stuck, not slow")
        if len(tail) >= 2 and all(tail[i] >= tail[i+1] for i in range(len(tail)-1)):
            return (f"count decreasing but not yet zero (last 5: {tail}); "
                    "consider raising max_attempts or sleep_between_s")
        return f"count not converging to zero (last 5: {tail})"


async def run_contract(
    contract: AcceptanceContract,
    execute_sql_fn,
    *,
    log_fn=None,
    sleep_fn=None,
) -> ContractResult:
    """Run an acceptance contract against a live session.

    execute_sql_fn: an async callable that takes a SQL string and returns the
        first column of the first row as an int (the pending count). The caller
        is responsible for wrapping their session (e.g., AIDPSession) into this
        adapter — this module stays pure and easy to mock.

    log_fn: optional sync callback (str) -> None for per-attempt progress.
    sleep_fn: optional async callable (seconds) -> None — defaults to asyncio.sleep.
        Tests inject a no-op to avoid real waits.
    """
    if log_fn is None:
        log_fn = lambda _m: None
    if sleep_fn is None:
        sleep_fn = asyncio.sleep

    t0 = time.time()
    readings: list[int] = []
    consecutive_zeros = 0
    log_fn(f"acceptance contract starting (zero_window={contract.zero_window}, "
           f"max_attempts={contract.max_attempts}, sleep={contract.sleep_between_s}s)")

    for attempt in range(1, contract.max_attempts + 1):
        try:
            count = await execute_sql_fn(contract.pending_count_sql)
        except Exception as exc:
            elapsed = time.time() - t0
            return ContractResult(
                passed=False,
                attempts=attempt,
                readings=readings,
                elapsed_s=elapsed,
                reason=f"probe SQL failed: {type(exc).__name__}: {str(exc)[:200]}",
                error=f"{type(exc).__name__}: {exc}",
                contract=contract,
            )

        try:
            count = int(count)
        except (TypeError, ValueError):
            elapsed = time.time() - t0
            return ContractResult(
                passed=False,
                attempts=attempt,
                readings=readings,
                elapsed_s=elapsed,
                reason=f"probe SQL returned non-integer: {count!r}",
                error=f"non-int result: {count!r}",
                contract=contract,
            )

        readings.append(count)
        if count == 0:
            consecutive_zeros += 1
            log_fn(f"  attempt {attempt}/{contract.max_attempts}: "
                   f"pending_count = 0 (zero #{consecutive_zeros} of {contract.zero_window})")
            if consecutive_zeros >= contract.zero_window:
                elapsed = time.time() - t0
                return ContractResult(
                    passed=True,
                    attempts=attempt,
                    readings=readings,
                    elapsed_s=elapsed,
                    reason=(f"converged: {contract.zero_window} consecutive zero readings "
                            f"in {attempt} attempts ({elapsed:.1f}s)"),
                    contract=contract,
                )
        else:
            consecutive_zeros = 0
            log_fn(f"  attempt {attempt}/{contract.max_attempts}: pending_count = {count}")

        # No sleep after the final attempt
        if attempt < contract.max_attempts:
            await sleep_fn(contract.sleep_between_s)

    # Exhausted max_attempts without converging
    elapsed = time.time() - t0
    return ContractResult(
        passed=False,
        attempts=contract.max_attempts,
        readings=readings,
        elapsed_s=elapsed,
        reason=(f"VIOLATED: pending_count={readings[-1]} after {contract.max_attempts} "
                f"attempts (max={contract.max_attempts}, zero_window={contract.zero_window})"),
        contract=contract,
    )


def format_result_for_report(result: ContractResult) -> str:
    """Markdown-friendly multi-line report block for JOB_REPORT.md."""
    if result.contract is None:
        return "  no acceptance contract\n"
    c = result.contract
    last5 = result.readings[-5:] if result.readings else []
    sql_snippet = c.pending_count_sql.replace("\n", " ").strip()
    if len(sql_snippet) > 100:
        sql_snippet = sql_snippet[:100] + "..."
    lines = [
        f"- **Acceptance contract status**: {result.status}",
        f"- Contract SQL: `{sql_snippet}`",
        f"- Required: {c.zero_window} consecutive zero reading(s), max {c.max_attempts} attempts, "
        f"{c.sleep_between_s}s apart",
        f"- Attempts made: {result.attempts}",
        f"- Last 5 readings: {last5}",
        f"- Elapsed: {result.elapsed_s:.1f}s",
        f"- Diagnostic: {result.diagnostic}",
    ]
    if result.reason:
        lines.append(f"- Outcome: {result.reason}")
    return "\n".join(lines) + "\n"


def load_contract_from_manifest_entry(entry: dict) -> Optional[AcceptanceContract]:
    """Convenience: pull `acceptance_contract` from a manifest job/task entry.

    Returns None if no contract is declared. Raises ContractParseError on
    a malformed contract.
    """
    raw = entry.get("acceptance_contract")
    if raw is None:
        return None
    return AcceptanceContract.from_dict(raw)
