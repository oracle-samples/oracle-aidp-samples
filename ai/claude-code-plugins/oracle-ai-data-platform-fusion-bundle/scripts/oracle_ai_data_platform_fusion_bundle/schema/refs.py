"""Variable + Vault-reference resolution for bundle.yaml strings.

A bundle.yaml string can contain three kinds of references:
- ``${env}``  — environment block name from aidp.config.yaml (rendered up-front)
- ``${VAR}``  — generic env variable lookup (from os.environ)
- ``${vault:OCID}`` — OCI Vault secret OCID; resolved late, never stored in plaintext

The resolver renders ``${env}`` and ``${VAR}`` eagerly during parse, but
returns ``${vault:...}`` references unresolved (wrapped in :class:`VaultRef`)
so downstream code can decide when to fetch — typically once, at orchestrator
startup — and never log values.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterator

_VAR_RE = re.compile(r"\$\{(?!vault:)([A-Za-z_][A-Za-z0-9_]*)\}")
_VAULT_RE = re.compile(r"\$\{vault:(?P<ocid>ocid1\.vaultsecret\.[A-Za-z0-9._-]+)\}")


@dataclass(frozen=True)
class VaultRef:
    """A reference to an OCI Vault secret. Never logged or hashed; resolve once at use."""

    ocid: str


def render_vars(value: str, *, extra: dict[str, str] | None = None) -> str:
    """Render ``${VAR}`` references against os.environ + ``extra``.

    Leaves ``${vault:...}`` untouched. Raises :class:`KeyError` for any
    unresolved ``${VAR}``.

    Args:
        value: A string from bundle.yaml.
        extra: Additional context for the lookup (e.g. ``{"env": "prod"}``).

    Returns:
        The string with ``${VAR}`` expanded; ``${vault:...}`` preserved.
    """
    extra = extra or {}

    def _sub(match: re.Match[str]) -> str:
        key = match.group(1)
        if key in extra:
            return extra[key]
        if key in os.environ:
            return os.environ[key]
        raise KeyError(f"variable ${{{key}}} not found in env or extra")

    return _VAR_RE.sub(_sub, value)


def find_vault_refs(value: str) -> Iterator[VaultRef]:
    """Yield every :class:`VaultRef` embedded in ``value``."""
    for match in _VAULT_RE.finditer(value):
        yield VaultRef(ocid=match.group("ocid"))


def replace_vault_refs(value: str, resolver: dict[str, str]) -> str:
    """Replace each ``${vault:OCID}`` with the cached secret value.

    Args:
        value: Source string.
        resolver: Mapping of ``ocid -> secret value`` already fetched.

    Returns:
        The string with vault references replaced.

    Raises:
        KeyError: If any vault OCID has no entry in ``resolver``.
    """

    def _sub(match: re.Match[str]) -> str:
        ocid = match.group("ocid")
        if ocid not in resolver:
            raise KeyError(f"vault secret {ocid} not in resolver cache")
        return resolver[ocid]

    return _VAULT_RE.sub(_sub, value)
