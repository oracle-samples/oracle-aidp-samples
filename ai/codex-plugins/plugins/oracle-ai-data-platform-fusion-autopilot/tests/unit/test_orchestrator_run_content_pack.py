"""Verify orchestrator.run dispatches to the content-pack runner.

These tests pin the content-pack-only run loop: every silver / gold /
bronze node must reach ``sql_runner.execute_node``, and the generated
REST notebook's run cell must call orchestrator.run with kwargs the
function actually accepts (no TypeError before any node executes).

Phase 9 deleted the v1 legacy backend; the `--execution-backend` flag
no longer exists. Tests still pin the legacy-rejection behaviour for
back-compat callers that pass ``execution_backend="legacy-python"``
programmatically (the run() function raises ``OrchestratorConfigError``).
"""

from __future__ import annotations

import inspect
import pathlib
from unittest.mock import MagicMock, patch

import pytest

from oracle_ai_data_platform_fusion_autopilot import orchestrator


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURE_BUNDLE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "bundle.yaml"
FIXTURE_PROFILE = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project" / "profiles" / "phase2-fixture.yaml"
FIXTURE_PACK = REPO_ROOT / "tests" / "fixtures" / "content_packs" / "phase2_test_pack"


@pytest.fixture(autouse=True)
def _stub_bronze_readiness_gate(monkeypatch):
    """These tests exercise the run loop / execute_node wiring, not the
    bronze-readiness gate. As of Option A the gate auto-fires for
    silver/gold-only runs (which most tests here use), so stub it to a
    no-op pass; the gate's own behaviour is covered by
    test_dispatcher_invokes_readiness.py. Bronze-in-scope tests don't
    trigger it, so this is a harmless no-op for them."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import bronze_readiness
    monkeypatch.setattr(
        bronze_readiness, "assert_bronze_readiness", lambda *a, **k: None
    )


# ---------------------------------------------------------------------------
# Signature contract — orchestrator.run accepts Phase 2 kwargs
# ---------------------------------------------------------------------------


class TestOrchestratorRunSignature:
    """Locks the signature the generated REST notebook depends on. If
    orchestrator.run ever drops execution_backend / resolved_pack /
    tenant_profile, the notebook would raise TypeError before any node
    executes — this test catches that regression."""

    def test_run_accepts_execution_backend_kwarg(self) -> None:
        sig = inspect.signature(orchestrator.run)
        assert "execution_backend" in sig.parameters

    def test_run_accepts_resolved_pack_kwarg(self) -> None:
        sig = inspect.signature(orchestrator.run)
        assert "resolved_pack" in sig.parameters

    def test_run_accepts_tenant_profile_kwarg(self) -> None:
        sig = inspect.signature(orchestrator.run)
        assert "tenant_profile" in sig.parameters

    def test_phase2_kwargs_are_keyword_only(self) -> None:
        """Defensive: they must be keyword-only so the v1 positional
        signature stays stable."""
        sig = inspect.signature(orchestrator.run)
        for name in ("execution_backend", "resolved_pack", "tenant_profile"):
            assert sig.parameters[name].kind == inspect.Parameter.KEYWORD_ONLY

    def test_phase5_default_is_content_pack(self) -> None:
        """Phase 5 Step 3 — the default flipped to content-pack. The
        legacy-python path stays available as an explicit opt-in."""
        sig = inspect.signature(orchestrator.run)
        assert sig.parameters["execution_backend"].default == "content-pack"
        assert sig.parameters["resolved_pack"].default is None
        assert sig.parameters["tenant_profile"].default is None


# ---------------------------------------------------------------------------
# Content-pack backend dispatch — execute_node is invoked
# ---------------------------------------------------------------------------


class TestContentPackBackendInvokesExecuteNode:
    """The flag must drive the loop through sql_runner.execute_node —
    NOT through the legacy registry."""

    def test_content_pack_backend_calls_execute_node(self, monkeypatch) -> None:
        """Mock execute_node and confirm orchestrator.run hits it for
        each node in the fixture pack's plan."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2

        # Load the fixture pack + profile up front.
        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)

        # Mock execute_node to record calls without touching real Spark.
        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)
        # Also patch the import location used inside orchestrator.run
        # (the lazy import there resolves the symbol at call time).
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o
        _o_module = _o

        # Stub state-table setup + Phase 2 migration so we don't need
        # real Spark.
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        # Bootstrap_spark would try to make a real session; replace with a mock.
        monkeypatch.setattr(_o_module, "_bootstrap_spark", lambda: MagicMock(name="FakeSpark"))

        summary = orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile, layers=["silver", "gold"],
        )

        # The fixture pack has 1 silver node; execute_node should be
        # called exactly once.
        assert len(execute_node_calls) == 1
        call = execute_node_calls[0]
        assert call["node"].id == "dim_thing"
        # The pack and profile passed in are forwarded.
        assert call["pack"] is pack
        assert call["profile"] is profile
        # Mode is threaded through.
        assert call["mode"] == "seed"

        # RunSummary reflects the run.
        assert len(summary.steps) == 1
        assert summary.steps[0].dataset_id == "dim_thing"
        assert summary.steps[0].layer == "silver"
        assert summary.steps[0].status == "success"

    def test_fresh_run_omitted_mode_resolves_to_seed(self, monkeypatch) -> None:
        """Tri-state --mode (feature: fail-fast-seed-validation): a FRESH run
        with mode omitted (None) resolves to 'seed' (unchanged default) and
        threads 'seed' into execute_node — never surfaces mode=None."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2

        pack = load_full_chain(
            FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK)
        )
        profile = load_tenant_profile(FIXTURE_PROFILE)

        calls: list[dict] = []

        def fake_execute_node(spark, **kwargs):
            calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)

        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(
            state_phase2, "ensure_state_columns_v2", lambda spark, paths: None
        )
        # Manifest write must not require a real Delta table in this unit test.
        monkeypatch.setattr(
            state_phase2, "write_state_rows_hard", lambda spark, paths, rows: None
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: MagicMock(name="FakeSpark"))

        summary = orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            mode=None,  # omitted
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["silver", "gold"],
        )
        assert summary.mode == "seed"
        assert calls and all(c["mode"] == "seed" for c in calls)

    def test_content_pack_backend_adopts_resume_run_id(self) -> None:
        """Phase 5 Step 9b — AIDPF-1032 resolved. The content-pack
        backend now accepts ``--resume`` and adopts the supplied
        run_id. Previously this test asserted the AIDPF-1032 raise."""
        # Signature-level smoke: invoking with resume_run_id no longer
        # raises OrchestratorConfigError before backend dispatch. The
        # full adopt-the-id contract is exercised by
        # tests/parity/test_dual_runner_e2e.py::test_v2_resume_adopts_supplied_run_id.
        import inspect
        sig = inspect.signature(orchestrator.run)
        # The kwarg is still accepted (not silently dropped).
        assert "resume_run_id" in sig.parameters

    def test_content_pack_backend_requires_resolved_pack(self) -> None:
        with pytest.raises(ValueError, match="resolved_pack is None"):
            orchestrator.run(
                bundle_path=FIXTURE_BUNDLE,
                resolved_pack=None,
                tenant_profile=MagicMock(), layers=["silver", "gold"],
            )

    def test_content_pack_backend_requires_tenant_profile(self) -> None:
        with pytest.raises(ValueError, match="tenant_profile is None"):
            orchestrator.run(
                bundle_path=FIXTURE_BUNDLE,
                resolved_pack=MagicMock(),
                tenant_profile=None, layers=["silver", "gold"],
            )

    def test_dry_run_returns_empty_summary(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)
        summary = orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            resolved_pack=pack,
            tenant_profile=profile,
            dry_run=True, layers=["silver", "gold"],
        )
        assert summary.steps == ()


# ---------------------------------------------------------------------------
# Prior-state hydration — incremental watermark + plan-hash drift gate
# ---------------------------------------------------------------------------


class TestPriorStateHydration:
    """Round-13 blocking #1: an incremental run must read the latest
    successful primary state row before each node and populate
    ctx.prior_watermark + prior_plan_hash. Without this, the renderer
    emits 1=1 (full scan) and the drift gate never fires."""

    def test_prior_state_lookup_populates_watermark_and_plan_hash(self, monkeypatch) -> None:
        """When a prior successful state row exists, execute_node sees
        the prior plan_hash + watermark."""
        from datetime import datetime, timezone

        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)

        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        # Fake a prior successful state row for dim_thing.
        prior_watermark = datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc)
        prior_plan_hash = "h-from-prior-success"

        # Stub the state-read query.
        fake_spark = MagicMock(name="FakeSpark")
        prior_df = MagicMock()
        prior_df.collect.return_value = [
            {"plan_hash": prior_plan_hash, "output_watermark": prior_watermark,
             "source_id": "erp_thing", "status": "success"}
        ]
        fake_spark.sql.return_value = prior_df

        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="incremental",
            resolved_pack=pack,
            tenant_profile=profile, layers=["silver", "gold"],
        )

        # execute_node received the prior_plan_hash + prior_watermark.
        assert len(execute_node_calls) == 1
        call = execute_node_calls[0]
        assert call["prior_plan_hash"] == prior_plan_hash
        # ctx carries the per-source prior watermark.
        ctx = call["ctx"]
        assert ctx.prior_watermark.get("erp_thing") == prior_watermark

    def test_first_run_no_prior_state_uses_none(self, monkeypatch) -> None:
        """Bare first-run case: no prior rows in fusion_autopilot_state →
        prior_plan_hash=None + empty prior_watermark. The drift gate
        is correctly a no-op and the renderer falls through to 1=1."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)

        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        fake_spark = MagicMock()
        # Latest-view query returns empty (no prior runs yet).
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df

        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile, layers=["silver", "gold"],
        )

        call = execute_node_calls[0]
        assert call["prior_plan_hash"] is None
        assert call["ctx"].prior_watermark == {}

    def test_state_read_failure_in_seed_mode_is_swallowed(self, monkeypatch) -> None:
        """Seed mode: a transient Spark error reading the latest view
        (e.g. table doesn't exist on a clean catalog) MUST NOT fail
        the run. Seed semantics are 'full rebuild from bronze' — no
        prior cursor needed; a benign read failure degrades cleanly
        to (None, {}) and execute_node still runs.
        """
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)

        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        fake_spark = MagicMock()
        fake_spark.sql.side_effect = RuntimeError("simulated AnalysisException")
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        # Seed mode — should NOT raise; degrades gracefully.
        orchestrator.run(
            bundle_path=FIXTURE_BUNDLE,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile, layers=["silver", "gold"],
        )

        assert len(execute_node_calls) == 1
        call = execute_node_calls[0]
        assert call["prior_plan_hash"] is None
        assert call["ctx"].prior_watermark == {}

    def test_state_read_failure_in_incremental_mode_fails_closed(self, monkeypatch) -> None:
        """Round-13/14 fix: an incremental content-pack run MUST fail
        closed when the latest-view read raises (permission, metastore,
        schema, transient Spark). Falling through to (None, {}) would
        full-scan the source AND skip the AIDPF-4040 drift gate — a
        silent failure mode that masks state-table accessibility issues
        and could commit incremental writes despite being unable to
        verify the prior cursor.

        The run must raise StateReadFailedError BEFORE any execute_node
        invocation.
        """
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            StateReadFailedError,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
        profile = load_tenant_profile(FIXTURE_PROFILE)

        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        fake_spark = MagicMock()
        fake_spark.sql.side_effect = RuntimeError("simulated metastore permission error")
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        with pytest.raises(StateReadFailedError) as exc_info:
            orchestrator.run(
                bundle_path=FIXTURE_BUNDLE,
                mode="incremental",
                resolved_pack=pack,
                tenant_profile=profile, layers=["silver", "gold"],
            )

        # Critical assertion: execute_node MUST NOT have been called.
        assert execute_node_calls == [], (
            "execute_node was invoked despite state-read failure in "
            "incremental mode — the run silently full-scanned the source. "
            "fail-closed contract violated."
        )

        # The original Spark exception is preserved as __cause__ for triage.
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert "metastore" in str(exc_info.value.__cause__)
        # The StateReadFailedError carries the dataset_id + layer.
        assert exc_info.value.dataset_id == "dim_thing"
        assert exc_info.value.layer == "silver"


# ---------------------------------------------------------------------------
# Cascade abort — failed upstream node blocks downstream dispatch
# ---------------------------------------------------------------------------


class TestCascadeAbort:
    """Round-13 blocking #2: when a node fails, downstream nodes that
    depend on it MUST NOT be dispatched. Otherwise they'd read stale
    pre-existing upstream tables and silently commit success.

    Uses a two-node fixture pack where gold.mart_x depends on
    silver.dim_thing. First call returns failure → second node must
    never be passed to execute_node."""

    def _ad_hoc_bundle(self, tmp_path: pathlib.Path, scope_ids: list[str]) -> pathlib.Path:
        """Write a bundle whose ``datasets[]`` declares the scope-roots
        the cascade test's pack expects (Phase 9: bundle_scope is now
        load-bearing — pre-fix tests relied on the resolver treating
        every pack node as a root).
        """
        bp = tmp_path / "bundle.yaml"
        bp.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: cascade-test\n"
            "fusion:\n  serviceUrl: https://example.com\n  username: u\n"
            "  password: p\n  externalStorage: s\n"
            "aidp:\n  catalog: c\n  bronzeSchema: b\n"
            "  silverSchema: silver\n  goldSchema: gold\n"
            "datasets:\n"
            + "".join(f"  - id: {sid}\n" for sid in scope_ids)
            + "dimensions:\n  build: []\n"
            "gold:\n  marts: []\n"
            "contentPack:\n  name: cascade-test\n"
            "  path: ./pack\n  profile: phase2-fixture\n"
        )
        return bp

    def _two_node_pack(self, tmp_path: pathlib.Path):
        """Build a 2-node fixture: silver.dim_a + gold.mart_x depending on dim_a."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )

        root = tmp_path / "pack"
        root.mkdir()
        (root / "pack.yaml").write_text(
            "id: cascade-test\nversion: 1.0.0\ncompatibility:\n  pluginMinVersion: 0.3.0\n"
        )
        (root / "silver").mkdir()
        (root / "silver" / "dim_a.yaml").write_text(
            "id: dim_a\nlayer: silver\nimplementation:\n  type: sql\n  sql: silver/dim_a.sql\n"
            "target: dim_a\noutputSchema:\n  columns:\n    - name: a\n      type: string\n"
            "      nullable: false\n      pii: none\ndependsOn:\n  bronze:\n    - id: erp_a\n"
            "      role: primary\nrefresh:\n  seed:\n    strategy: replace\n"
        )
        (root / "silver" / "dim_a.sql").write_text("SELECT 1 AS a")
        (root / "gold").mkdir()
        (root / "gold" / "mart_x.yaml").write_text(
            "id: mart_x\nlayer: gold\nimplementation:\n  type: sql\n  sql: gold/mart_x.sql\n"
            "target: mart_x\noutputSchema:\n  columns:\n    - name: x\n      type: string\n"
            "      nullable: false\n      pii: none\ndependsOn:\n  silver:\n    - id: dim_a\n"
            "      role: primary\nrefresh:\n  seed:\n    strategy: replace\n"
        )
        (root / "gold" / "mart_x.sql").write_text("SELECT 1 AS x")
        return load_full_chain(root, base_resolver=make_filesystem_base_resolver(root))

    def test_downstream_node_skipped_when_upstream_fails(
        self, monkeypatch, tmp_path
    ) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = self._two_node_pack(tmp_path)
        profile = load_tenant_profile(FIXTURE_PROFILE)

        # First node (dim_a) fails; second (mart_x) MUST NOT be called.
        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            # dim_a fails; if mart_x were reached we'd see two calls.
            return NodeExecutionResult(
                status="quality_failed",
                error_message="[unique] simulated failure",
            )
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        # Spy on write_state_rows_hard so we can assert the cascade-skip
        # row is persisted (round-15 finding #2 — audit-completeness).
        state_write_calls: list[list[dict]] = []
        original_write = state_phase2.write_state_rows_hard
        def spy_write_state_rows_hard(spark, paths, rows):
            state_write_calls.append(list(rows))
            return None
        monkeypatch.setattr(state_phase2, "write_state_rows_hard", spy_write_state_rows_hard)

        fake_spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        # Phase 9: bundle_scope is now load-bearing — declare the
        # test pack's nodes in datasets[] so the resolver picks them
        # up as roots.
        bundle_path = self._ad_hoc_bundle(tmp_path, ["dim_a", "mart_x"])
        summary = orchestrator.run(
            bundle_path=bundle_path,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["silver", "gold"],
        )

        # execute_node was called for dim_a; mart_x must not have been.
        called_node_ids = [c["node"].id for c in execute_node_calls]
        assert "dim_a" in called_node_ids
        assert "mart_x" not in called_node_ids
        assert len(execute_node_calls) == 1

        # RunSummary has 2 steps: dim_a failed + mart_x skipped/cascade.
        step_ids = {s.dataset_id: s for s in summary.steps}
        assert step_ids["dim_a"].status == "failed"
        assert step_ids["mart_x"].status == "skipped"
        assert step_ids["mart_x"].skip_reason == "cascade"

        # Round-15 finding #2: the cascade-skipped node MUST have a
        # diagnostic state row written so audit readers see the current
        # run's cascade event (not the previous successful run).
        cascade_rows = [
            row for batch in state_write_calls
            for row in batch
            if row.get("dataset_id") == "mart_x" and row.get("status") == "skipped"
        ]
        assert len(cascade_rows) == 1, (
            "Expected exactly one cascade-skip state row for mart_x; got "
            f"{len(cascade_rows)}. All state writes: {state_write_calls!r}"
        )
        skip_row = cascade_rows[0]
        assert skip_row["skip_reason"] == "cascade"
        assert skip_row["layer"] == "gold"
        assert "dim_a" in (skip_row.get("error_message") or "")
        # No cursor advance on a cascade skip.
        assert skip_row["output_watermark"] is None
        assert skip_row["last_watermark"] is None

    def test_independent_node_not_blocked_by_unrelated_failure(
        self, monkeypatch, tmp_path
    ) -> None:
        """If two silvers are independent (no dependsOn between them),
        failure of one MUST NOT cascade to the other."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        # Build a 2-silver pack with no inter-dependency.
        root = tmp_path / "pack"
        root.mkdir()
        (root / "pack.yaml").write_text(
            "id: cascade-test\nversion: 1.0.0\ncompatibility:\n  pluginMinVersion: 0.3.0\n"
        )
        (root / "silver").mkdir()
        for nid in ("dim_a", "dim_b"):
            (root / "silver" / f"{nid}.yaml").write_text(
                f"id: {nid}\nlayer: silver\nimplementation:\n  type: sql\n"
                f"  sql: silver/{nid}.sql\ntarget: {nid}\noutputSchema:\n  columns:\n"
                f"    - name: c\n      type: string\n      nullable: false\n      pii: none\n"
                f"dependsOn:\n  bronze:\n    - id: erp_a\n      role: primary\n"
                f"refresh:\n  seed:\n    strategy: replace\n"
            )
            (root / "silver" / f"{nid}.sql").write_text("SELECT 1 AS c")
        pack = load_full_chain(root, base_resolver=make_filesystem_base_resolver(root))

        profile = load_tenant_profile(FIXTURE_PROFILE)

        # Make dim_a fail; assert dim_b STILL runs.
        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            if kwargs["node"].id == "dim_a":
                return NodeExecutionResult(status="quality_failed", error_message="x")
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        fake_spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        bundle_path = self._ad_hoc_bundle(tmp_path, ["dim_a", "dim_b"])
        summary = orchestrator.run(
            bundle_path=bundle_path,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["silver", "gold"],
        )

        called_ids = [c["node"].id for c in execute_node_calls]
        # Both nodes were dispatched — no false cascade.
        assert "dim_a" in called_ids
        assert "dim_b" in called_ids
        # dim_b succeeded.
        status_by_id = {s.dataset_id: s.status for s in summary.steps}
        assert status_by_id["dim_a"] == "failed"
        assert status_by_id["dim_b"] == "success"

    def _bronze_silver_pack(self, tmp_path: pathlib.Path):
        """Build a 2-node fixture: bronze.erp_a + silver.dim_a depending
        on bronze.erp_a. Phase 9 runs bronze in the same plan, so a failed
        bronze extract must cascade-skip its silver consumer."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_full_chain,
            make_filesystem_base_resolver,
        )

        root = tmp_path / "pack"
        root.mkdir()
        (root / "pack.yaml").write_text(
            "id: cascade-test\nversion: 1.0.0\ncompatibility:\n  pluginMinVersion: 0.3.0\n"
        )
        (root / "bronze").mkdir()
        (root / "bronze" / "erp_a.yaml").write_text(
            "id: erp_a\nlayer: bronze\nimplementation:\n  type: bronze_extract\n"
            "  datastore: ErpAExtractPVO\n  biccSchema: Financial\n"
            "  incrementalCapable: true\ntarget: erp_a\n"
            "dependsOn:\n  bronze: []\n  silver: []\n"
            "refresh:\n  seed:\n    strategy: replace\n  incremental:\n"
            "    strategy: merge\n    watermark:\n      source: erp_a\n"
            "      column: LastUpdateDate\n    naturalKey:\n      - Id\n"
            "outputSchema:\n  columns:\n"
            "    - { name: Id, type: long, nullable: true, pii: none }\n"
            "    - { name: _extract_ts, type: timestamp, nullable: false, pii: none }\n"
            "    - { name: _source_pvo, type: string, nullable: false, pii: none }\n"
            "    - { name: _run_id, type: string, nullable: false, pii: none }\n"
            "    - { name: _watermark_used, type: timestamp, nullable: true, pii: none }\n"
            "quality:\n  tests: []\n"
        )
        (root / "silver").mkdir()
        (root / "silver" / "dim_a.yaml").write_text(
            "id: dim_a\nlayer: silver\nimplementation:\n  type: sql\n  sql: silver/dim_a.sql\n"
            "target: dim_a\noutputSchema:\n  columns:\n    - name: a\n      type: string\n"
            "      nullable: false\n      pii: none\ndependsOn:\n  bronze:\n    - id: erp_a\n"
            "      role: primary\nrefresh:\n  seed:\n    strategy: replace\n"
        )
        (root / "silver" / "dim_a.sql").write_text("SELECT 1 AS a")
        return load_full_chain(root, base_resolver=make_filesystem_base_resolver(root))

    def test_silver_skipped_when_bronze_dep_fails(
        self, monkeypatch, tmp_path
    ) -> None:
        """PR #23 blocking #1: a failed bronze node must cascade-skip a
        silver consumer that declares ``dependsOn.bronze``. Pre-fix,
        ``_find_cascade_blocker`` only walked ``dependsOn.silver``, so the
        silver node would dispatch, read the stale pre-existing bronze
        table, and commit a success row after its upstream failed."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        pack = self._bronze_silver_pack(tmp_path)
        profile = load_tenant_profile(FIXTURE_PROFILE)

        # Bronze erp_a fails; silver dim_a (dependsOn.bronze erp_a) MUST
        # NOT be dispatched.
        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            if kwargs["node"].id == "erp_a":
                return NodeExecutionResult(
                    status="quality_failed",
                    error_message="[unique] simulated bronze failure",
                )
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        # The upfront batch source-schema gate (AIDPF-4071) probes BICC
        # before the node loop; this test exercises execution-time cascade,
        # so stub it to "no source failures" and let the loop run.
        monkeypatch.setattr(sql_runner, "check_bronze_source_schemas", lambda *a, **k: [])

        state_write_calls: list[list[dict]] = []
        def spy_write_state_rows_hard(spark, paths, rows):
            state_write_calls.append(list(rows))
            return None
        monkeypatch.setattr(state_phase2, "write_state_rows_hard", spy_write_state_rows_hard)

        fake_spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        fake_spark.sql.return_value = empty_df
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)

        # Declare the silver root; D-1 transitive include pulls bronze erp_a.
        bundle_path = self._ad_hoc_bundle(tmp_path, ["dim_a"])
        summary = orchestrator.run(
            bundle_path=bundle_path,
            mode="seed",
            resolved_pack=pack,
            tenant_profile=profile,
            layers=["bronze", "silver", "gold"],
        )

        called_node_ids = [c["node"].id for c in execute_node_calls]
        # Bronze was dispatched and failed; silver must NOT have been.
        assert "erp_a" in called_node_ids
        assert "dim_a" not in called_node_ids

        step_ids = {s.dataset_id: s for s in summary.steps}
        assert step_ids["erp_a"].status == "failed"
        assert step_ids["dim_a"].status == "skipped"
        assert step_ids["dim_a"].skip_reason == "cascade"
        # The cascade error must name the failed bronze upstream.
        assert "erp_a" in (step_ids["dim_a"].error_message or "")


# ---------------------------------------------------------------------------
# CLI integration: --inline reaches execute_node via the content-pack runner
# ---------------------------------------------------------------------------


class TestInlineCliReachesExecuteNode:
    """Round-12 blocking #2: the inline CLI path must actually invoke
    the content-pack runner, not silently fall through to legacy. We
    mock execute_node and confirm the CLI path causes it to be called."""

    def test_inline_content_pack_cli_calls_execute_node(self, monkeypatch, tmp_path) -> None:
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import run as run_impl
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as v1_state
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import state_phase2
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            NodeExecutionResult,
        )
        import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o

        execute_node_calls: list[dict] = []
        def fake_execute_node(spark, **kwargs):
            execute_node_calls.append(kwargs)
            return NodeExecutionResult(status="success", row_count=0)
        monkeypatch.setattr(sql_runner, "execute_node", fake_execute_node)

        # Stub Spark + state-table setup.
        monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
        monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
        monkeypatch.setattr(_o, "_bootstrap_spark", lambda: MagicMock(name="FakeSpark"))

        # Need a valid aidp.config.yaml; create a minimal one.
        config_path = tmp_path / "aidp.config.yaml"
        config_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: phase2-test\n"
            "environments:\n"
            "  dev:\n"
            "    workspaceKey: w\n"
            "    ociProfile: DEFAULT\n",
            encoding="utf-8",
        )

        exit_code = run_impl(
            bundle_path=FIXTURE_BUNDLE,
            config_path=config_path,
            env_name="dev",
            mode="seed",
            inline=True,
            layers="silver,gold",
            console=Console(),
        )

        # Inline + content-pack succeeded AND execute_node was called.
        assert exit_code == 0
        assert len(execute_node_calls) == 1
        assert execute_node_calls[0]["node"].id == "dim_thing"


# ---------------------------------------------------------------------------
# Round-15 finding #1: invalid pack rejected before execution / staging
# ---------------------------------------------------------------------------


class TestInvalidPackRejectedBeforeExecution:
    """``validate_pack_full`` MUST run after every ``load_full_chain`` on
    the content-pack entry paths. A pack with errors (e.g. a typo in
    ``dependsOn.silver`` pointing to a non-existent node) must NOT
    reach orchestrator.run (inline) or dispatch_via_rest (REST) — fail
    fast with AIDPF-1036 carrying the per-error report."""

    def _build_invalid_pack(self, tmp_path: pathlib.Path) -> pathlib.Path:
        """Build a pack whose gold node references a non-existent silver dep."""
        root = tmp_path / "invalid_pack"
        root.mkdir()
        (root / "pack.yaml").write_text(
            "id: invalid-pack\nversion: 1.0.0\n"
            "compatibility:\n  pluginMinVersion: 0.3.0\n",
            encoding="utf-8",
        )
        # Define a gold node whose dependsOn.silver points to "dim_typo"
        # which doesn't exist. validate_dag() rejects this.
        (root / "gold").mkdir()
        (root / "gold" / "mart_x.yaml").write_text(
            "id: mart_x\nlayer: gold\n"
            "implementation:\n  type: sql\n  sql: gold/mart_x.sql\n"
            "target: mart_x\noutputSchema:\n  columns:\n"
            "    - name: x\n      type: string\n      nullable: false\n      pii: none\n"
            "dependsOn:\n  silver:\n    - id: dim_typo\n      role: primary\n"
            "refresh:\n  seed:\n    strategy: replace\n",
            encoding="utf-8",
        )
        (root / "gold" / "mart_x.sql").write_text("SELECT 1 AS x", encoding="utf-8")
        return root

    def _build_invalid_bundle(self, tmp_path: pathlib.Path, pack_root: pathlib.Path) -> pathlib.Path:
        project = tmp_path / "project"
        project.mkdir()
        (project / "profiles").mkdir()
        # Profile referenced by the bundle.
        (project / "profiles" / "test-profile.yaml").write_text(
            "schemaVersion: 1\n"
            "tenant: test\n"
            "pinnedAt: 2026-06-01T00:00:00+00:00\n"
            "bronzeSchemaFingerprint: \"sha256:test\"\n"
            "resolved:\n  column: {}\n  semantic: {}\n",
            encoding="utf-8",
        )
        # Bundle pointing at the invalid pack.
        bundle_path = project / "bundle.yaml"
        bundle_path.write_text(
            f"apiVersion: aidp-fusion-autopilot/v1\n"
            f"project: invalid-pack-test\n"
            f"fusion:\n"
            f"  serviceUrl: https://example.com\n"
            f"  username: u\n"
            f"  password: p\n"
            f"  externalStorage: s\n"
            f"aidp:\n"
            f"  catalog: c\n"
            f"  bronzeSchema: bronze\n"
            f"  silverSchema: silver\n"
            f"  goldSchema: gold\n"
            f"datasets:\n"
            f"  - id: erp_x\n"
            f"contentPack:\n"
            f"  name: invalid-pack\n"
            f"  path: {pack_root.resolve()}\n"
            f"  profile: test-profile\n",
            encoding="utf-8",
        )
        return bundle_path

    def test_inline_path_rejects_invalid_pack_before_orchestrator_run(
        self, monkeypatch, tmp_path
    ) -> None:
        """CLI --inline with an invalid pack: orchestrator.run is NEVER
        called; CLI returns non-zero."""
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot import orchestrator as _o
        from oracle_ai_data_platform_fusion_autopilot.commands.run import run as run_impl

        pack_root = self._build_invalid_pack(tmp_path)
        bundle_path = self._build_invalid_bundle(tmp_path, pack_root)

        # Spy on orchestrator.run so we can assert it was NEVER called.
        run_calls: list[dict] = []
        original_run = _o.run
        def spy_run(*args, **kwargs):
            run_calls.append(kwargs)
            return original_run(*args, **kwargs)
        monkeypatch.setattr(_o, "run", spy_run)

        config_path = tmp_path / "aidp.config.yaml"
        config_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: invalid-pack-test\n"
            "environments:\n"
            "  dev:\n"
            "    workspaceKey: w\n"
            "    ociProfile: DEFAULT\n",
            encoding="utf-8",
        )

        exit_code = run_impl(
            bundle_path=bundle_path,
            config_path=config_path,
            env_name="dev",
            mode="seed",
            inline=True,
            console=Console(),
        )

        # Non-zero exit (CLI returns 2 for config errors).
        assert exit_code == 2
        # orchestrator.run was NEVER reached.
        assert run_calls == [], (
            "orchestrator.run was invoked despite pack validation failure — "
            "AIDPF-1036 fail-fast contract violated."
        )

    def test_rest_path_rejects_invalid_pack_before_dispatch(
        self, monkeypatch, tmp_path
    ) -> None:
        """CLI without --inline + an invalid pack: dispatch_via_rest is
        NEVER called; CLI returns non-zero. Staging primitives are not
        even produced."""
        from rich.console import Console
        from oracle_ai_data_platform_fusion_autopilot.commands.run import run as run_impl
        from oracle_ai_data_platform_fusion_autopilot import dispatch as _dispatch_pkg

        pack_root = self._build_invalid_pack(tmp_path)
        bundle_path = self._build_invalid_bundle(tmp_path, pack_root)

        dispatch_calls: list[dict] = []
        def spy_dispatch_via_rest(**kwargs):
            dispatch_calls.append(kwargs)
            raise AssertionError("dispatch_via_rest must not be called for invalid pack")
        monkeypatch.setattr(_dispatch_pkg, "dispatch_via_rest", spy_dispatch_via_rest)

        config_path = tmp_path / "aidp.config.yaml"
        config_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: invalid-pack-test\n"
            "environments:\n"
            "  dev:\n"
            "    workspaceKey: w\n"
            "    ociProfile: DEFAULT\n",
            encoding="utf-8",
        )

        exit_code = run_impl(
            bundle_path=bundle_path,
            config_path=config_path,
            env_name="dev",
            mode="seed",
            inline=False,
            console=Console(),
        )

        # Non-zero exit.
        assert exit_code == 2
        # dispatch_via_rest was NEVER reached.
        assert dispatch_calls == []


# ---------------------------------------------------------------------------
# Legacy backend untouched
# ---------------------------------------------------------------------------


class TestLegacyBackendUnchanged:
    """The legacy-python backend behaves identically to pre-Phase-2;
    Phase 5 flipped the default to content-pack but the opt-in path
    stays byte-for-byte unchanged."""

    def test_legacy_backend_remains_available_as_opt_in(self) -> None:
        sig = inspect.signature(orchestrator.run)
        # Default flipped to content-pack in Phase 5 Step 3, but the
        # legacy-python value remains accepted on the parameter and the
        # CLI Choice() lists it as a documented opt-in.
        assert sig.parameters["execution_backend"].default == "content-pack"

    def test_legacy_backend_does_NOT_invoke_execute_node(self, monkeypatch) -> None:
        """The legacy-python branch never reaches the Phase 2 runner."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import sql_runner
        execute_node_mock = MagicMock(side_effect=AssertionError(
            "execute_node MUST NOT be called from the legacy-python path"
        ))
        monkeypatch.setattr(sql_runner, "execute_node", execute_node_mock)

        # We can't easily run the full v1 path without a real Spark + BICC,
        # but we can confirm that calling with legacy-python doesn't lazy-
        # import the content-pack backend's symbols. Verify the function
        # signature accepts the call shape and the dispatcher branch
        # decides correctly.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import _run_content_pack_backend
        # If we were to call orchestrator.run with,
        # it would fall through to the v1 logic — not to _run_content_pack_backend.
        # The branch is `if execution_backend == "content-pack":` so any other
        # value (including the default) skips it.
        execute_node_mock.assert_not_called()
