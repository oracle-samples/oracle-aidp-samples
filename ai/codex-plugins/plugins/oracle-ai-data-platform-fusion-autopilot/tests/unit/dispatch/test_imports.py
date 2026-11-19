"""P1.5ε §Step 6b — dispatch import-boundary regression test.

Asserts the §4.3 separation: ``import dispatch`` MUST NOT pull
``orchestrator/*``, ``extractors/*``, ``dimensions/*``, or ``transforms/*``
into ``sys.modules``. Crossing this boundary means:

- The dispatch package can never accidentally couple to engine internals.
- A future MCP / Airflow dispatcher can slot in as a sibling under
  ``dispatch/`` without code surgery.
- P1.17's incremental MERGE rework can ship without touching dispatch.

Permitted schema-level imports (these are the explicit cross-boundary
modules):

- ``schema.bundle`` (AidpConfig, EnvSpec, Bundle, load_bundle)
- ``schema.errors`` (OrchestratorConfigError + cross-boundary subclasses)
- ``schema.refs`` (env-var rendering)
- ``schema.run_summary`` (RunStep, RunSummary, PlanNode, PrereqNode, marker serializers)
- ``schema.plan_resolver`` (P1.5ε-fix9 — dry-run plan resolver consumed by
  ``dispatch.dispatch_via_rest`` under ``--dry-run``)
"""

from __future__ import annotations

import subprocess
import sys


FORBIDDEN_PREFIXES = (
    "oracle_ai_data_platform_fusion_autopilot.orchestrator",
    "oracle_ai_data_platform_fusion_autopilot.extractors",
    "oracle_ai_data_platform_fusion_autopilot.dimensions",
    "oracle_ai_data_platform_fusion_autopilot.transforms",
)


def _modules_loaded_by(import_spec: str) -> set[str]:
    """Run a fresh Python subprocess that imports ``import_spec`` and emits
    the set of ``oracle_ai_data_platform_fusion_autopilot.*`` modules in
    ``sys.modules`` afterwards. Using a subprocess guarantees we don't
    pollute the test runner's import graph (which already has the whole
    orchestrator loaded)."""
    code = (
        f"import sys\n"
        f"{import_spec}\n"
        "for m in sorted(sys.modules):\n"
        "    if m.startswith('oracle_ai_data_platform_fusion_autopilot'):\n"
        "        print(m)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    return set(proc.stdout.split())


def test_import_dispatch_package_does_not_pull_engine() -> None:
    """``import dispatch`` is the entry point most consumers go through."""
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot import dispatch"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, (
        f"dispatch package leaked engine imports into sys.modules: {leaks}. "
        "Check dispatch/__init__.py + submodules for stray "
        "`from ..orchestrator import` lines."
    )


def test_import_dispatch_rest_client_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client "
        "import AidpRestClient"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"rest_client leaked: {leaks}"


def test_import_dispatch_notebook_builder_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_builder "
        "import build_notebook"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"notebook_builder leaked: {leaks}"


def test_import_dispatch_preflight_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight "
        "import run_local_preflight, run_remote_preflight"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"preflight leaked: {leaks}"


def test_import_dispatch_wheel_builder_does_not_pull_engine() -> None:
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot.dispatch.wheel_builder "
        "import build_wheel"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"wheel_builder leaked: {leaks}"


def test_import_dispatch_notebook_dispatch_does_not_pull_engine() -> None:
    """Phase 4.1 / D3 — the neutral end-to-end helper that wraps the
    AidpRestClient sequence. The bootstrap-specific orchestration lives
    in ``commands/cluster_bootstrap_probe.py``; this helper stays
    orchestrator-free per the boundary."""
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_dispatch "
        "import dispatch_notebook_and_fetch_marker"
    )
    leaks = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not leaks, f"notebook_dispatch leaked: {leaks}"


def test_schema_imports_are_permitted() -> None:
    """Sanity check — the schema-level imports the dispatch package
    relies on are reachable from the same clean subprocess. If this test
    fails the boundary tests above are likely false-passing because the
    schema packages themselves got renamed.

    P1.5ε-fix9 added ``schema.plan_resolver`` to the allow-list — it's
    consumed by ``dispatch_via_rest`` on the dry-run path and must
    remain clean of engine imports. (``schema.registry_metadata`` was
    also in this allow-list pre-Phase-9-followup; it was deleted along
    with the legacy registry — see
    ``test_deleted_modules_remain_unfindable`` below.)
    """
    loaded = _modules_loaded_by(
        "from oracle_ai_data_platform_fusion_autopilot import dispatch\n"
        "from oracle_ai_data_platform_fusion_autopilot.schema import "
        "bundle, errors, refs, run_summary, plan_resolver"
    )
    expected = {
        "oracle_ai_data_platform_fusion_autopilot.schema.bundle",
        "oracle_ai_data_platform_fusion_autopilot.schema.errors",
        "oracle_ai_data_platform_fusion_autopilot.schema.refs",
        "oracle_ai_data_platform_fusion_autopilot.schema.run_summary",
        "oracle_ai_data_platform_fusion_autopilot.schema.plan_resolver",
    }
    assert expected.issubset(loaded), (
        f"expected schema modules not loaded: missing={expected - loaded}"
    )


def test_deleted_modules_remain_unfindable() -> None:
    """Phase 9 follow-up — these modules were deleted on purpose; a
    future re-introduction must be a deliberate decision, not a
    cargo-culted resurrection."""
    import importlib.util
    for name in (
        "oracle_ai_data_platform_fusion_autopilot.orchestrator.registry",
        "oracle_ai_data_platform_fusion_autopilot.schema.registry_metadata",
    ):
        assert importlib.util.find_spec(name) is None, (
            f"{name} was deleted in the Phase-9 follow-up but is "
            f"importable again — see docs/features/v2-phase-9-followup-"
            f"registry-deletion/idea.md for why."
        )


def test_render_summary_with_plan_nodes_does_not_import_orchestrator_registry() -> None:
    """P1.5ε-fix9 — locks the ``_layer_for_spec`` lazy-import fallback
    removal in ``commands/run.py:_render_summary``. Renders a RunSummary
    whose ``plan`` is a tuple of ``PlanNode`` instances, and asserts the
    in-process render path did NOT pull ``orchestrator.registry`` (or any
    other engine module) into ``sys.modules``.

    Runs in a fresh subprocess because the test runner already has
    ``orchestrator.registry`` loaded from earlier engine-side tests —
    in-process the assertion would be order-dependent and trivially pass
    even if ``_render_summary`` accidentally imported engine code.
    """
    spec = (
        "from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import "
        "RunSummary, PlanNode\n"
        "from oracle_ai_data_platform_fusion_autopilot.commands.run import "
        "_render_summary\n"
        "from rich.console import Console\n"
        "import io\n"
        "summary = RunSummary.empty(\n"
        "    bundle_project='x', mode='seed',\n"
        "    plan=(PlanNode(dataset_id='ap_invoices', layer='bronze'),),\n"
        "    prereqs=(),\n"
        ")\n"
        "_render_summary(Console(file=io.StringIO()), summary)"
    )
    loaded = _modules_loaded_by(spec)
    forbidden = {m for m in loaded if any(m.startswith(p) for p in FORBIDDEN_PREFIXES)}
    assert not forbidden, (
        f"_render_summary leaked engine imports: {forbidden}. "
        "The `_layer_for_spec` lazy-import fallback was supposed to be "
        "removed in P1.5ε-fix9 Step 6 — check commands/run.py."
    )
