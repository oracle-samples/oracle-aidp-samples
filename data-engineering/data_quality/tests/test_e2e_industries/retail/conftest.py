"""Retail pack-scoped fixtures. Autouse report finalizer ensures one
``report.html`` per pack regardless of whether tests request it."""

from __future__ import annotations

from typing import Iterator

import pytest

from tests.test_e2e_industries.conftest import make_pack_storage, render_pack_report

_INDUSTRY = "retail"


@pytest.fixture(scope="package")
def industry_storage():
    """Shared across every test in the retail pack."""
    return make_pack_storage()


@pytest.fixture(scope="package", autouse=True)
def _industry_report(industry_storage, tmp_path_factory) -> Iterator[None]:
    """Finalizer: render exactly one report.html after every retail
    test has recorded into ``industry_storage``. Autouse so a new test
    module added to the pack cannot forget it.
    """
    yield
    render_pack_report(_INDUSTRY, industry_storage, tmp_path_factory)
