"""Operator-identity resolution gate.

Every artifact bootstrap writes — profile, evidence snapshot, diagnostic —
carries an ``approvedBy.{operator, timestamp, mechanism}`` block. The
*operator* value comes from one of three sources in strict precedence:

1. The ``--operator <string>`` CLI flag (explicit override).
2. The ``AIDP_OPERATOR`` environment variable.
3. The ``USER`` environment variable (POSIX-floor fallback).

If all three are unset / empty / whitespace-only, bootstrap MUST refuse to
write any artifact and raise ``AIDPF-1020``. This floor is intentionally not a
cryptographic identity; SOX-strict environments layer their own controls
(IAM-gated deployment pipelines + PR review on ``profiles/``).
"""

from __future__ import annotations

import os


class OperatorIdentityUnresolved(Exception):
    """Raised when ``--operator`` / ``AIDP_OPERATOR`` / ``USER`` are all
    unresolved (unset, empty, or whitespace-only).

    Carries the ``AIDPF-1020`` error code in its message. The CLI wires
    this to write a ``.aidp/diagnostics/<run_id>/AIDPF-1020.json``
    artifact and exit non-zero.
    """

    def __init__(self, probed_sources: list[str]) -> None:
        self.probed_sources = probed_sources
        super().__init__(
            "AIDPF-1020: operator identity not resolvable from any source "
            f"({', '.join(probed_sources)}). Set --operator, export "
            "AIDP_OPERATOR, or run from a shell where $USER is set."
        )


def resolve_operator(cli_flag: str | None) -> str:
    """Resolve operator identity using the configured precedence chain.

    Args:
        cli_flag: value of ``--operator``; ``None`` when the flag was
            omitted. Empty or whitespace-only strings are treated the
            same as "not set".

    Returns:
        Non-empty operator name.

    Raises:
        OperatorIdentityUnresolved: every precedence rung produced an
            empty / whitespace-only value.
    """
    probed: list[str] = []

    candidate = _normalise(cli_flag)
    probed.append("--operator")
    if candidate is not None:
        return candidate

    candidate = _normalise(os.environ.get("AIDP_OPERATOR"))
    probed.append("AIDP_OPERATOR")
    if candidate is not None:
        return candidate

    candidate = _normalise(os.environ.get("USER"))
    probed.append("USER")
    if candidate is not None:
        return candidate

    raise OperatorIdentityUnresolved(probed_sources=probed)


def _normalise(value: str | None) -> str | None:
    """Return ``None`` for unset / empty / whitespace-only; otherwise the
    trimmed string.

    Whitespace-only values are treated as unset on purpose — operators
    occasionally export an env var to ``" "`` to "clear" it, and a
    leading-space operator name would be near-useless in audit logs.
    """
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    return trimmed


__all__ = ["OperatorIdentityUnresolved", "resolve_operator"]
