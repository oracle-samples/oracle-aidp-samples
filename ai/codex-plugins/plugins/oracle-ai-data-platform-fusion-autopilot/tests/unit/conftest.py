"""Autouse fixtures for orchestrator unit tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_literal_warn_flag():
    """Reset the ``_LITERAL_WARN_EMITTED`` module flag before/after every test
    (B1.3 fix). Otherwise warn-count assertions are order-dependent — a test
    that runs after another literal-credential test sees the flag still
    flipped and asserts zero WARNs when it expected one.

    Autouse (no opt-in) to prevent future contributors from accidentally
    introducing flaky tests. Cost is ~microseconds per test.
    """
    try:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import runtime
    except ImportError:
        # Test modules that don't transitively import orchestrator
        # shouldn't fail on this fixture.
        yield
        return
    runtime._LITERAL_WARN_EMITTED = False
    yield
    runtime._LITERAL_WARN_EMITTED = False
