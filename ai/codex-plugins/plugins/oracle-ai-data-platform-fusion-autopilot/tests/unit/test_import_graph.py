"""P1.17d D2b — import-graph smoke test.

Catches future regressions where someone moves the explicit-column-list
MERGE clause helpers (or any other cross-module helper) back into
``orchestrator/__init__.py`` and reintroduces the circular import.

The cycle this guards against:

  orchestrator/__init__.py
    └─ imports registry
       └─ imports dim_supplier / dim_account / gl_balance
          └─ (if they import from orchestrator/__init__.py)
             ── orchestrator/__init__.py is still initializing
                ── ImportError or partial-module binding

The neutral module ``orchestrator/merge_sql.py`` breaks the cycle by
not importing from ``__init__.py`` or ``registry.py`` itself.

This test passes when:
  - ``from oracle_ai_data_platform_fusion_autopilot.orchestrator import run``
    completes without ImportError.
  - Each silver/gold builder module can be imported directly.
  - The merge_sql helpers are importable from the neutral module.
"""
from __future__ import annotations


def test_orchestrator_run_imports_without_cycle() -> None:
    # The public entry point — drives the full __init__.py initialization
    # chain through registry → builders.
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import run  # noqa: F401


def test_dim_calendar_imports_standalone() -> None:
    """Phase 9: only dim_calendar remains under dimensions/ (ADR-0011 —
    the genuine Python builtin). The v1 dim_supplier / dim_account /
    transforms.gold.* modules were deleted (ADR-0022)."""
    from oracle_ai_data_platform_fusion_autopilot.dimensions import (  # noqa: F401
        dim_calendar as _dc,
    )


def test_merge_sql_helpers_importable_from_neutral_module() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.merge_sql import (
        build_explicit_when_matched_clause,
        build_explicit_when_not_matched_clause,
    )

    # Smoke-call to confirm they're callable functions, not partial
    # bindings from a half-initialized module.
    assert callable(build_explicit_when_matched_clause)
    assert callable(build_explicit_when_not_matched_clause)
