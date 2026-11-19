"""Shared config-loading helpers for the CLI commands.

Today's ``commands/validate.py`` has a private ``_load_config`` that
accumulates errors into a shared list — that shape works for ``validate``
(which prints every issue at the end) but is wrong for ``run`` (which
should exit immediately on the first config error).

This module exposes:

- :func:`load_aidp_config` — raise-on-failure variant for ``run``.
- :func:`env_or_error` — look up an environment block by name.

Both raise :class:`OrchestratorConfigError` so the CLI's existing
``except OrchestratorConfigError: console.print(...); return 2`` catch
covers them too — no new error class, no new CLI branching.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from ..schema.bundle import AidpConfig, EnvSpec
from ..schema.errors import OrchestratorConfigError


def load_aidp_config(path: Path) -> AidpConfig:
    """Read + parse ``aidp.config.yaml``. Raises
    :class:`OrchestratorConfigError` on any failure.

    Failure modes mapped to a single error class:
      - file not found → ``aidp.config.yaml not found at <path>``
      - YAML parse error → ``aidp.config.yaml YAML parse error: <msg>``
      - Pydantic schema → ``aidp.config.yaml schema errors:\\n<msg>``
    """
    if not path.exists():
        raise OrchestratorConfigError(f"aidp.config.yaml not found at {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise OrchestratorConfigError(
            f"aidp.config.yaml YAML parse error: {exc}"
        ) from exc
    try:
        return AidpConfig.model_validate(raw)
    except ValidationError as exc:
        raise OrchestratorConfigError(
            f"aidp.config.yaml schema errors:\n{exc}"
        ) from exc


def env_or_error(config: AidpConfig, env_name: str) -> EnvSpec:
    """Look up an environment block by name. Raises
    :class:`OrchestratorConfigError` listing every available env name on
    miss so operators see the typo + the valid alternatives in one line."""
    if env_name in config.environments:
        return config.environments[env_name]
    available = sorted(config.environments.keys())
    raise OrchestratorConfigError(
        f"environment {env_name!r} not found in aidp.config.yaml; "
        f"available: {available}"
    )


__all__ = ["env_or_error", "load_aidp_config"]
