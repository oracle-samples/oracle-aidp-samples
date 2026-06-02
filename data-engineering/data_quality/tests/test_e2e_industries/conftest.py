"""Pack-level fixtures and collection hook for the industry packs.

Contracts (see plan §Step 5 and §Backend skips are explicit):

1. **One report per industry, generated once per pack run.**
   The ``industry_report`` fixture is ``scope="package"`` and is
   **autouse at the package level** via each industry's own
   ``conftest.py``. Package-scoped finalizers run in LIFO after every
   test in the package (even on failure), so each industry emits its
   single ``report.html`` whether its tests pass or fail.

2. **Shared SQLiteStorage per pack** — the ``industry_storage``
   fixture is pack-scoped so every collection/validation row in a
   pack lands in one system table, and the finalizer's report reads
   that table once.

3. **Output paths** — local: pytest tmp_path_factory. CI: under
   ``$QUALIFIRE_ARTIFACTS_DIR/industry-reports/<industry>/report.html``.

4. **Spark session pre-warm** — session-scoped autouse at this
   package level so the JVM boot cost (~8-12s on ubuntu-latest) is
   amortized across all three packs instead of being charged to the
   first-collected pack. Without this, first-pack budget could blow
   on collection order alone.

5. **Coverage verification** — ``pytest_collection_modifyitems``
   inspects ``session.items`` and fails the run at collection time
   if the manifest's expected (industry, scenario_key, backend)
   tuples are not all present.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Iterator

import pytest

from tests._e2e_support.backends import _make_storage
from tests._e2e_support.reporting import render_report
from tests.test_e2e_industries._skip_manifest import missing_from_collection


# ---------------------------------------------------------------------------
# Coverage enforcement (in-process, runs during single pytest tests/ run).
# ---------------------------------------------------------------------------


def pytest_collection_modifyitems(config, items):
    """Fail collection if any manifest-required test is missing.

    Only runs when industry tests are part of the collection (e.g. the
    user runs ``pytest tests/``), not when the user runs an unrelated
    subset like ``pytest tests/test_validation/``.
    """
    industry_items = [
        i for i in items
        if "/tests/test_e2e_industries/" in str(getattr(i, "fspath", ""))
        or "tests/test_e2e_industries/" in getattr(i, "nodeid", "")
    ]
    if not industry_items:
        return
    node_ids = [item.nodeid for item in industry_items]
    missing = missing_from_collection(node_ids)
    if missing:
        lines = [
            f"  - {industry}.{scenario}[{backend}]"
            for industry, scenario, backend in sorted(missing)
        ]
        pytest.exit(
            "industry-pack manifest coverage error — the following "
            "(industry, scenario_key, backend) tuples are declared in "
            "tests/test_e2e_industries/_skip_manifest.py but not "
            "present in the pytest collection:\n"
            + "\n".join(lines)
            + "\n\nEither add the missing test cases or update the "
            "manifest in the same commit.",
            returncode=1,
        )


# ---------------------------------------------------------------------------
# Spark session pre-warm (session-scoped autouse at package level).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=True)
def _prewarm_spark_session(request) -> Iterator[None]:
    """Boot a SparkSession before any industry pack runs so JVM startup
    cost is amortized across packs and does not get attributed to the
    pack that pytest happens to collect first.

    The prewarm has two guards so that a ``pandas``-only run (or a
    broken/installed-but-unusable Spark) cannot abort the whole
    session in fixture setup:

    1. **Skip when no Spark test is collected.** If the session's
       items list has no ``[spark]`` parametrization, there is
       nothing to amortize.
    2. **Degrade on startup failure.** If ``SparkSession.getOrCreate``
       raises (JVM missing, socket bind blocked, misconfigured env),
       emit a ``RuntimeWarning`` and yield. The per-test Spark backend
       will then fail or skip on its own terms — which is the correct
       blast radius for a Spark-only runtime issue.
    """
    wants_spark = any(
        "[spark]" in getattr(item, "nodeid", "")
        for item in getattr(request.session, "items", [])
    )
    if not wants_spark:
        yield
        return
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        # pyspark is not installed. Each Spark-parametrized test later
        # calls `_make_backend(backend_type, ...)`, which dispatches to
        # `_make_spark_backend()` (tests/_e2e_support/backends.py:54-58);
        # that function imports pyspark lazily, so the ImportError
        # surfaces at backend construction during test execution and
        # pytest records it as an ERROR on the individual test, not a
        # skip. The try/except here is purely defensive session-scope
        # plumbing — it lets the prewarm yield cleanly instead of
        # crashing the whole session, leaving each test free to surface
        # its own ImportError. Prewarm itself has nothing to do.
        yield
        return
    try:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("qualifire-e2e-industries-prewarm")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        _ = spark.version  # force JVM init
    except Exception as exc:  # noqa: BLE001 — Spark failure modes are broad
        warnings.warn(
            f"Spark prewarm failed: {exc!r}; Spark-backed tests will "
            "attempt to start Spark on their own and may skip or fail.",
            RuntimeWarning,
            stacklevel=2,
        )
        yield
        return
    yield


# ---------------------------------------------------------------------------
# Output path helpers.
# ---------------------------------------------------------------------------


def _report_path_for(industry: str, tmp_path_factory) -> Path:
    """CI path if ``QUALIFIRE_ARTIFACTS_DIR`` is set, else pytest tmp."""
    env_dir = os.environ.get("QUALIFIRE_ARTIFACTS_DIR")
    if env_dir:
        base = Path(env_dir) / "industry-reports" / industry
        base.mkdir(parents=True, exist_ok=True)
        return base / "report.html"
    return tmp_path_factory.mktemp(f"{industry}_reports") / "report.html"


# ---------------------------------------------------------------------------
# Report finalizer helper.
#
# Each industry subpackage has its own conftest.py that declares a
# ``_industry_report`` autouse fixture (scope="package") which calls
# ``render_pack_report(industry, storage, tmp_path_factory)``. The
# finalizer runs once per pack at teardown.
# ---------------------------------------------------------------------------


def render_pack_report(
    industry: str,
    storage,
    tmp_path_factory,
) -> Path:
    """Render the pack's single report.html. Used by each industry's
    autouse package finalizer. Raises on empty HTML or missing file.
    """
    out_path = _report_path_for(industry, tmp_path_factory)
    html = render_report(storage, out_path)
    assert html, f"{industry} report HTML was empty"
    assert "<html" in html, (
        f"{industry} report does not contain <html — writer contract changed"
    )
    assert out_path.exists(), (
        f"{industry} report file was not written to {out_path}"
    )
    assert out_path.stat().st_size > 0, (
        f"{industry} report file at {out_path} is zero bytes"
    )
    return out_path


# ---------------------------------------------------------------------------
# Shared storage factory — pack-scoped storage is declared per-industry
# in each subpackage's conftest.py (so the pack scope has a well-defined
# boundary). We expose a constructor helper here to keep behavior
# identical across packs.
# ---------------------------------------------------------------------------


def make_pack_storage():
    """Return a fresh SQLiteStorage for a pack's shared use."""
    return _make_storage()
