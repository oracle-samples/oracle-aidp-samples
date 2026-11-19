"""Phase 5 Step 9b — content-pack ``--resume`` correctness.

Companion to ``test_orchestrator_resume.py`` (v1 backend resume).
Covers the dispatcher-side wiring for the default content-pack
backend:

  * Unknown ``resume_run_id`` → ``ResumeRunNotFoundError`` (CLI maps
    to exit 2).
  * Bare ``--resume`` reconstructs ``(datasets, layers)`` from the
    stored ``plan_snapshot`` so the resumed run gates over the SAME
    scope as the original.
  * Already-succeeded silver/gold nodes emit
    ``status='resumed_skipped'`` steps under the original ``run_id``;
    ``sql_runner.execute_node`` is NOT called for them.
  * Failed / not-yet-attempted nodes retry under the same run_id.

Uses a ``_FakeSpark`` that returns canned rows for the
``read_resumable_state`` SQL pattern; mirrors the v1 resume test
infrastructure.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot import orchestrator
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
    ResumeRunNotFoundError,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
)


# ---------------------------------------------------------------------------
# Inline fixture — a one-silver content-pack bundle on a real bronze id.
# ---------------------------------------------------------------------------


_PACK_YAML = """\
id: content-pack-resume-test
version: 1.0.0
description: Phase 5 content-pack resume test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

_SILVER_NODE = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

_GOLD_NODE = """\
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_key
      type: bigint
      nullable: false
      pii: none
"""

_PROFILE_YAML = """\
schemaVersion: 1
tenant: cp-resume-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:cp-resume-fixture"
resolved:
  column: {}
  semantic: {}
profile:
  calendar:
    fiscalStartMonth: 1
    startDate: "2024-01-01"
"""

_BUNDLE_YAML = """\
apiVersion: aidp-fusion-autopilot/v1
project: cp-resume-test
fusion:
  serviceUrl: https://example.com
  username: alice@oracle
  password: literal-password
  externalStorage: oci://bucket@ns/path
aidp:
  catalog: cp_resume_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
datasets:
  - id: erp_suppliers
    mode: full
  # Phase 9 cross-layer datasets[]: declare silver + gold roots so
  # the resolver's bundle_scope picks them up.
  - id: dim_supplier
  - id: supplier_spend
contentPack:
  name: content-pack-resume-test
  path: ./pack
  profile: cp-resume-tenant
"""


@pytest.fixture
def fixture(tmp_path: Path):
    pack_root = tmp_path / "pack"
    silver = pack_root / "silver"
    gold = pack_root / "gold"
    silver.mkdir(parents=True)
    gold.mkdir(parents=True)
    (pack_root / "pack.yaml").write_text(_PACK_YAML, encoding="utf-8")
    (silver / "dim_supplier.yaml").write_text(_SILVER_NODE, encoding="utf-8")
    (silver / "dim_supplier.sql").write_text("SELECT 1\n", encoding="utf-8")
    (gold / "supplier_spend.yaml").write_text(_GOLD_NODE, encoding="utf-8")
    (gold / "supplier_spend.sql").write_text("SELECT 1\n", encoding="utf-8")

    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "cp-resume-tenant.yaml").write_text(_PROFILE_YAML, encoding="utf-8")

    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text(_BUNDLE_YAML, encoding="utf-8")

    pack = load_full_chain(
        pack_root, base_resolver=make_filesystem_base_resolver(pack_root),
    )
    profile = load_tenant_profile(profiles / "cp-resume-tenant.yaml")
    return bundle_path, pack, profile


# ---------------------------------------------------------------------------
# Fake Spark — minimal but enough to satisfy read_resumable_state +
# the cp backend's downstream Spark calls (all mocked elsewhere).
# ---------------------------------------------------------------------------


class _FakeRow:
    def __init__(self, **kwargs: Any) -> None:
        self._data = kwargs

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __getattr__(self, key: str) -> Any:
        try:
            return self._data[key]
        except KeyError as exc:
            raise AttributeError(key) from exc


class _FakeDataFrame:
    def __init__(self, rows: list[_FakeRow] | None = None) -> None:
        self._rows = rows or []

    def collect(self) -> list[_FakeRow]:
        return list(self._rows)


class _FakeSpark:
    """Spark fake that returns canned rows for the
    ``read_resumable_state`` SQL pattern and empty results for
    everything else.
    """

    def __init__(self, state_rows: list[_FakeRow] | None = None) -> None:
        self._state_rows = state_rows or []
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> _FakeDataFrame:
        self.sql_calls.append(query)
        if (
            "ranked AS" in query
            and "fusion_autopilot_state" in query
            and "WHERE run_id" in query
        ):
            if "row_count IS NOT NULL" in query:
                return _FakeDataFrame(
                    [r for r in self._state_rows
                     if getattr(r, "_data", {}).get("row_count") is not None]
                )
            return _FakeDataFrame(self._state_rows)
        return _FakeDataFrame([])


def _identity_for_bundle() -> dict[str, str]:
    """Mirrors plan_hash._identity_dict for the fixture bundle."""
    from oracle_ai_data_platform_fusion_autopilot import __version__ as _pv
    return {
        "fusion.serviceUrl": "https://example.com",
        "fusion.externalStorage": "oci://bucket@ns/path",
        "fusion.username": "alice@oracle",
        "aidp.catalog": "cp_resume_catalog",
        "aidp.bronzeSchema": "bronze",
        "aidp.silverSchema": "silver",
        "aidp.goldSchema": "gold",
        "plugin_version": _pv,
    }


def _snapshot(nodes: list[dict[str, str]]) -> str:
    return json.dumps({"identity": _identity_for_bundle(), "nodes": nodes})


def _state_rows(
    *,
    succeeded: list[tuple[str, str]],
    failed: list[tuple[str, str]],
    snapshot: str | None,
    plan_hash: str = "cp-resume-test-hash",
    succeeded_row_count: int = 42,
) -> list[_FakeRow]:
    """Build canned state rows. Pass ``snapshot=None`` AND a varying
    ``plan_hash`` (via callers) to model the real CP write path's
    shape (per-node hash + null snapshot)."""
    from datetime import datetime
    rows: list[_FakeRow] = []
    base_time = datetime(2026, 5, 21, 12, 0, 0)
    for ds_id, layer in succeeded:
        rows.append(_FakeRow(
            dataset_id=ds_id, layer=layer, status="success", mode="seed",
            row_count=succeeded_row_count, last_watermark=None,
            plan_hash=plan_hash, plan_snapshot=snapshot,
            last_run_at=base_time,
        ))
    for ds_id, layer in failed:
        rows.append(_FakeRow(
            dataset_id=ds_id, layer=layer, status="failed", mode="seed",
            row_count=None, last_watermark=None,
            plan_hash=plan_hash, plan_snapshot=snapshot,
            last_run_at=base_time,
        ))
    return rows


def _cp_shape_state_rows(
    *,
    succeeded: list[tuple[str, str]],
    failed: list[tuple[str, str]],
    succeeded_row_count: int = 42,
) -> list[_FakeRow]:
    """Build rows in the shape ``sql_runner._write_success_rows`` actually
    persists for the content-pack write path:

    * ``plan_snapshot=None`` (CP doesn't store a run-level snapshot).
    * Per-node ``plan_hash`` (each row gets its own hash) — modelled
      by appending the node id, mimicking real per-node hash variance.

    The v1 ``read_resumable_state`` rejects this shape; the CP-tolerant
    reader (``read_content_pack_resumable_state``) MUST accept it.
    """
    from datetime import datetime
    rows: list[_FakeRow] = []
    base_time = datetime(2026, 5, 21, 12, 0, 0)
    for ds_id, layer in succeeded:
        rows.append(_FakeRow(
            dataset_id=ds_id, layer=layer, status="success", mode="seed",
            row_count=succeeded_row_count, last_watermark=None,
            plan_hash=f"cp-node-hash-{ds_id}", plan_snapshot=None,
            last_run_at=base_time,
        ))
    for ds_id, layer in failed:
        rows.append(_FakeRow(
            dataset_id=ds_id, layer=layer, status="failed", mode="seed",
            row_count=None, last_watermark=None,
            plan_hash=f"cp-node-hash-{ds_id}", plan_snapshot=None,
            last_run_at=base_time,
        ))
    return rows


# ---------------------------------------------------------------------------
# Mock helpers — bypass spark-heavy downstream calls.
# ---------------------------------------------------------------------------


def _stub_downstream(monkeypatch, fake_spark: _FakeSpark) -> list[dict]:
    """Mock the spark-dependent dispatcher hooks so the resume test
    doesn't need a real backend. Returns a list that captures every
    ``cp_execute_node`` call (so the test can assert dispatch happened
    only for reattempt nodes).
    """
    import oracle_ai_data_platform_fusion_autopilot.orchestrator as _o
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
        bronze_readiness, sql_runner, state as v1_state, state_phase2,
        preflight_evidence,
    )
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight_evidence import (
        PreflightOutcome,
    )
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
        NodeExecutionResult,
    )

    execute_calls: list[dict] = []

    def _capture(*a, **kw):
        execute_calls.append(kw)
        return NodeExecutionResult(status="success", row_count=0)

    monkeypatch.setattr(_o, "_bootstrap_spark", lambda: fake_spark)
    monkeypatch.setattr(v1_state, "ensure_state_table", lambda spark, paths: None)
    monkeypatch.setattr(state_phase2, "ensure_state_columns_v2", lambda spark, paths: None)
    monkeypatch.setattr(state_phase2, "write_state_rows_hard", lambda spark, paths, rows: None)
    monkeypatch.setattr(sql_runner, "execute_node", _capture)
    monkeypatch.setattr(
        bronze_readiness, "assert_bronze_readiness",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        preflight_evidence, "check_bronze_fingerprint_drift",
        lambda **kw: PreflightOutcome(
            kind="ok",
            diagnostic_path=None, summary=None,
            prior_fingerprint=None, current_fingerprint=None,
        ),
    )
    # Stub the dispatcher-level PVO drift gate. The full drift gate
    # would require a real BICC probe.
    monkeypatch.setattr(_o, "_run_fusion_pvo_drift_gate", lambda **kw: None)
    # Stub the dispatcher's bronze branch (legacy recursive run) so we
    # don't need real BICC. The CP-only paths exercised here should
    # not even hit the bronze branch (resume narrows it out), but stub
    # defensively. AssertionError if called would surface as a test
    # failure; here we just no-op return an empty bronze summary.
    original_run = _o.run

    def _maybe_stub_run(*args, **kwargs):
        if kwargs.get("execution_backend") == "legacy-python":
            from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
                RunSummary,
            )
            from datetime import datetime as _dt, timezone as _tz
            now = _dt.now(_tz.utc)
            return RunSummary(
                run_id=kwargs.get("_forced_run_id", "stub"),
                started_at=now, finished_at=now,
                bundle_project="cp-resume-test",
                mode=kwargs.get("mode", "seed"),
                steps=(),
            )
        return original_run(*args, **kwargs)

    monkeypatch.setattr(_o, "run", _maybe_stub_run)

    return execute_calls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestContentPackResume:
    def test_unknown_resume_run_id_raises_not_found(
        self, monkeypatch, fixture,
    ) -> None:
        """An unknown resume_run_id triggers the v1 state-read contract:
        ``ResumeRunNotFoundError``. The pre-fix dispatcher silently
        adopted any string as the run_id; the fix invokes
        ``read_resumable_state`` which raises this error cleanly."""
        bundle_path, pack, profile = fixture
        # _FakeSpark with no rows → read_resumable_state raises.
        fake_spark = _FakeSpark(state_rows=[])
        _stub_downstream(monkeypatch, fake_spark)

        with pytest.raises(ResumeRunNotFoundError):
            orchestrator.run(
                bundle_path=bundle_path,
                spark=fake_spark,
                resolved_pack=pack,
                tenant_profile=profile,
                resume_run_id="this-run-was-never-recorded",
                layers=["silver"],
                mode="seed",
            )

    def test_succeeded_node_emits_resumed_skip_not_dispatched(
        self, monkeypatch, fixture,
    ) -> None:
        """The silver node already succeeded under the prior run.
        Resume should emit a ``resumed_skipped`` step and NEVER call
        ``execute_node`` for it.
        """
        bundle_path, pack, profile = fixture
        original_run_id = "cp-original-run-id"
        snapshot = _snapshot([
            {"dataset_id": "dim_supplier", "layer": "silver",
             "mode": "seed", "effective_schema": ""},
        ])
        rows = _state_rows(
            succeeded=[("dim_supplier", "silver")],
            failed=[],
            snapshot=snapshot,
        )
        fake_spark = _FakeSpark(state_rows=rows)
        execute_calls = _stub_downstream(monkeypatch, fake_spark)

        summary = orchestrator.run(
            bundle_path=bundle_path,
            spark=fake_spark,
            resolved_pack=pack,
            tenant_profile=profile,
            resume_run_id=original_run_id,
            layers=["silver"],
            mode="seed",
        )

        assert summary.run_id == original_run_id
        # The silver step was carried forward, not dispatched.
        silver_steps = [s for s in summary.steps if s.dataset_id == "dim_supplier"]
        assert len(silver_steps) == 1
        assert silver_steps[0].status == "resumed_skipped"
        assert silver_steps[0].run_id == original_run_id
        # ``execute_node`` was never called for dim_supplier.
        dim_supplier_calls = [
            c for c in execute_calls
            if getattr(c.get("node"), "id", None) == "dim_supplier"
        ]
        assert dim_supplier_calls == [], (
            "execute_node was called for an already-succeeded node — "
            "the resume short-circuit must prevent re-dispatch"
        )

    def test_failed_node_retries_under_original_run_id(
        self, monkeypatch, fixture,
    ) -> None:
        """A node whose latest terminal status is 'failed' is NOT in
        ``ResumeContext.succeeded`` — resume MUST re-dispatch it. The
        prior failure surfaces alongside the retry under the same
        run_id, so the audit trail joins on a single id.
        """
        bundle_path, pack, profile = fixture
        original_run_id = "cp-failed-retry-id"
        snapshot = _snapshot([
            {"dataset_id": "dim_supplier", "layer": "silver",
             "mode": "seed", "effective_schema": ""},
        ])
        rows = _state_rows(
            succeeded=[],
            failed=[("dim_supplier", "silver")],
            snapshot=snapshot,
        )
        fake_spark = _FakeSpark(state_rows=rows)
        execute_calls = _stub_downstream(monkeypatch, fake_spark)

        summary = orchestrator.run(
            bundle_path=bundle_path,
            spark=fake_spark,
            resolved_pack=pack,
            tenant_profile=profile,
            resume_run_id=original_run_id,
            layers=["silver"],
            mode="seed",
        )
        assert summary.run_id == original_run_id
        # dim_supplier was re-dispatched.
        dim_calls = [
            c for c in execute_calls
            if getattr(c.get("node"), "id", None) == "dim_supplier"
        ]
        assert len(dim_calls) == 1, (
            f"expected dim_supplier to retry once, got {len(dim_calls)} "
            f"call(s); execute_calls={execute_calls!r}"
        )
        # The step in the summary records the retry success (stubbed).
        retry_step = next(s for s in summary.steps if s.dataset_id == "dim_supplier")
        assert retry_step.status == "success"
        assert retry_step.run_id == original_run_id

    def test_real_cp_shape_state_rows_accepted(
        self, monkeypatch, fixture,
    ) -> None:
        """Real content-pack runs persist per-node ``plan_hash`` values
        with ``plan_snapshot=None`` (see
        ``sql_runner._write_success_rows``). The v1 resume reader
        rejects that shape; the CP-tolerant reader the dispatcher uses
        MUST accept it and treat the run as resumable.

        Scope reconstruction in this case falls back to the
        ``(dataset_id, layer)`` set observed in the rows since there's
        no snapshot to parse.
        """
        bundle_path, pack, profile = fixture
        original_run_id = "cp-real-shape-id"
        # NO snapshot, per-node hashes — the actual CP write-path shape.
        rows = _cp_shape_state_rows(
            succeeded=[("dim_supplier", "silver")],
            failed=[],
        )
        fake_spark = _FakeSpark(state_rows=rows)
        execute_calls = _stub_downstream(monkeypatch, fake_spark)

        summary = orchestrator.run(
            bundle_path=bundle_path,
            spark=fake_spark,
            resolved_pack=pack,
            tenant_profile=profile,
            resume_run_id=original_run_id,
            # No explicit filters — must reconstruct from row set.
            mode="seed",
        )
        assert summary.run_id == original_run_id
        # The silver node carried forward; ``execute_node`` never fired.
        silver_steps = [s for s in summary.steps if s.dataset_id == "dim_supplier"]
        assert len(silver_steps) == 1
        assert silver_steps[0].status == "resumed_skipped"
        assert execute_calls == [], (
            "execute_node was called even though the only node in the "
            "reconstructed scope already succeeded — the CP resume "
            "reader's null-snapshot path mis-classified the row"
        )

    def test_bare_resume_reconstructs_scope_from_snapshot(
        self, monkeypatch, fixture,
    ) -> None:
        """When ``--resume`` is supplied with no explicit ``--datasets``
        / ``--layers``, the dispatcher reconstructs the original scope
        from the stored ``plan_snapshot``. Without scope
        reconstruction, the resumed run would silently widen scope to
        the default (full medallion) and dispatch nodes the original
        run never planned to run.
        """
        bundle_path, pack, profile = fixture
        original_run_id = "cp-bare-resume-id"
        # Original run scoped to silver only.
        snapshot = _snapshot([
            {"dataset_id": "dim_supplier", "layer": "silver",
             "mode": "seed", "effective_schema": ""},
        ])
        rows = _state_rows(
            succeeded=[("dim_supplier", "silver")],
            failed=[],
            snapshot=snapshot,
        )
        fake_spark = _FakeSpark(state_rows=rows)
        execute_calls = _stub_downstream(monkeypatch, fake_spark)

        summary = orchestrator.run(
            bundle_path=bundle_path,
            spark=fake_spark,
            resolved_pack=pack,
            tenant_profile=profile,
            resume_run_id=original_run_id,
            # NO datasets, NO layers — must come from snapshot.
            mode="seed",
        )
        assert summary.run_id == original_run_id
        # Every step is silver-only — the gold node from the pack was
        # NOT included in the reconstructed scope.
        layers_seen = {s.layer for s in summary.steps}
        assert "gold" not in layers_seen, (
            f"bare-resume widened scope: gold steps in summary "
            f"despite original snapshot being silver-only: "
            f"steps={[(s.dataset_id, s.layer) for s in summary.steps]!r}"
        )
        # dim_supplier was carried forward, not re-dispatched.
        assert execute_calls == [], (
            "execute_node was called during a no-op all-succeeded "
            "bare resume — reconstructed scope should have driven all "
            "in-scope nodes through the resume short-circuit"
        )


# ---------------------------------------------------------------------------
# Reader: __*__ exclusion + execution-row predicate + manifest read
# (feature: fail-fast-seed-validation)
# ---------------------------------------------------------------------------


class _ManifestAwareSpark:
    """Fake spark answering the ranked resume query, DESCRIBE (column probe),
    the unranked distinct-mode query, and the manifest read.

    ``manifest_rows`` (list of payload values, each str or None) drives the
    ``__run_manifest__`` read; ``manifest_raises`` makes that read raise;
    ``has_manifest_col`` toggles whether DESCRIBE reports the run_manifest
    column (False → column-absent legacy table)."""

    def __init__(
        self,
        rows: list[_FakeRow],
        manifest_raw: str | None = None,
        *,
        manifest_rows: "list[str | None] | None" = None,
        manifest_raises: bool = False,
        has_manifest_col: bool = True,
    ) -> None:
        self._rows = rows
        if manifest_rows is not None:
            self._manifest_rows = manifest_rows
        elif manifest_raw is not None:
            self._manifest_rows = [manifest_raw]
        else:
            self._manifest_rows = []
        self._manifest_raises = manifest_raises
        self._has_manifest_col = has_manifest_col

    def sql(self, query: str):
        if query.strip().startswith("DESCRIBE TABLE"):
            cols = [
                "run_id", "dataset_id", "layer", "mode", "status", "last_run_at",
            ]
            if self._has_manifest_col:
                cols.append("run_manifest")
            return _FakeDataFrame([_FakeRow(col_name=c) for c in cols])
        if "SELECT run_manifest FROM" in query and "__run_manifest__" in query:
            if self._manifest_raises:
                raise RuntimeError("spark read failure on manifest column")
            return _FakeDataFrame(
                [_FakeRow(run_manifest=p) for p in self._manifest_rows]
            )
        if "SELECT DISTINCT mode FROM" in query:
            # Unranked distinct execution modes over real-node exec rows.
            modes = {
                r._data.get("mode")
                for r in self._rows
                if r._data.get("mode") in ("seed", "incremental")
                and not (
                    r._data["dataset_id"].startswith("__")
                    and r._data["dataset_id"].endswith("__")
                )
            }
            return _FakeDataFrame([_FakeRow(mode=m) for m in sorted(modes)])
        if "ranked AS" in query and "row_count IS NOT NULL" in query:
            return _FakeDataFrame(
                [r for r in self._rows
                 if getattr(r, "_data", {}).get("row_count") is not None]
            )
        if "ranked AS" in query:
            return _FakeDataFrame(self._rows)
        return _FakeDataFrame([])


def _row(dataset_id, layer, status, mode, **extra):
    from datetime import datetime
    base = dict(
        dataset_id=dataset_id, layer=layer, status=status, mode=mode,
        row_count=None, last_watermark=None, plan_hash="h", plan_snapshot=None,
        last_run_at=datetime(2026, 5, 21, 12, 0, 0),
    )
    base.update(extra)
    return _FakeRow(**base)


def test_reader_excludes_reserved_ids_and_reads_manifest() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    rows = [
        _row("gl_coa", "bronze", "success", "seed"),
        _row("dim_account", "silver", "failed", "seed"),
        # Reserved manifest row (status deferred) — must NOT count as a node.
        _row("__run_manifest__", "silver", "deferred", "seed"),
        # Audit row on a real node — success but audit mode → not an exec row.
        _row("dim_account", "silver", "success", "plan_hash_repin",
             last_run_at=__import__("datetime").datetime(2026, 5, 21, 11, 0, 0)),
    ]
    spark = _ManifestAwareSpark(rows, manifest_raw='{"schemaVersion":1}')
    paths = MagicMock()
    paths.bronze.return_value = "cat.bronze.fusion_autopilot_state"
    ctx = _state.read_content_pack_resumable_state(spark, paths, "run-1")

    # Reserved id excluded from succeeded + scope.
    assert "__run_manifest__" not in ctx.succeeded
    assert "__run_manifest__" not in ctx.scope_datasets
    # gl_coa succeeded (exec row); dim_account did not (latest exec row failed).
    assert "gl_coa" in ctx.succeeded
    # Manifest raw surfaced.
    assert ctx.run_manifest_raw == '{"schemaVersion":1}'
    # Only 'seed' is an execution mode here (audit mode excluded).
    assert ctx.historical_exec_modes == ("seed",)


def test_reader_surfaces_mixed_execution_modes() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    rows = [
        _row("gl_coa", "bronze", "success", "seed"),
        _row("ap_invoices", "bronze", "success", "incremental"),
    ]
    spark = _ManifestAwareSpark(rows, manifest_raw=None)
    paths = MagicMock()
    paths.bronze.return_value = "cat.bronze.fusion_autopilot_state"
    ctx = _state.read_content_pack_resumable_state(spark, paths, "run-2")
    assert set(ctx.historical_exec_modes) == {"seed", "incremental"}
    assert ctx.run_manifest_raw is None


# ---------------------------------------------------------------------------
# Finding 1 (round 2): manifest ingestion FAILS CLOSED, never fails open
# ---------------------------------------------------------------------------

from oracle_ai_data_platform_fusion_autopilot.orchestrator.run_manifest import (  # noqa: E402
    ManifestInvalidError,
)


def _paths_mock():
    paths = MagicMock()
    paths.bronze.return_value = "cat.bronze.fusion_autopilot_state"
    return paths


def _one_exec_row():
    return [_row("gl_coa", "bronze", "success", "seed")]


def test_manifest_null_payload_row_raises_4022() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(_one_exec_row(), manifest_rows=[None])
    with pytest.raises(ManifestInvalidError):
        _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")


@pytest.mark.parametrize("payload", ["", "   ", "\n"])
def test_manifest_empty_payload_row_raises_4022(payload) -> None:
    """An empty/blank string payload is corruption, NOT 'no manifest' — it must
    fail closed (AIDPF-4022), not fall back to the legacy path."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(_one_exec_row(), manifest_rows=[payload])
    with pytest.raises(ManifestInvalidError):
        _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")


def test_manifest_conflicting_duplicate_rows_raise_4022() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(
        _one_exec_row(),
        manifest_rows=['{"schemaVersion":1,"a":1}', '{"schemaVersion":1,"a":2}'],
    )
    with pytest.raises(ManifestInvalidError):
        _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")


def test_manifest_read_failure_raises_4022_not_legacy() -> None:
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(_one_exec_row(), manifest_raises=True)
    with pytest.raises(ManifestInvalidError):
        _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")


def test_manifest_absent_column_is_legacy_not_error() -> None:
    """A pre-feature table WITHOUT the run_manifest column → legitimate legacy
    path (raw=None), NOT an error."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(_one_exec_row(), has_manifest_col=False)
    ctx = _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")
    assert ctx.run_manifest_raw is None


def test_manifest_no_rows_is_legacy() -> None:
    """Column present but zero __run_manifest__ rows → legacy (raw=None)."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    spark = _ManifestAwareSpark(_one_exec_row(), manifest_rows=[])
    ctx = _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")
    assert ctx.run_manifest_raw is None


def test_manifest_single_duplicate_identical_payload_ok() -> None:
    """Two rows with the SAME payload are benign (idempotent) — not a conflict."""
    from oracle_ai_data_platform_fusion_autopilot.orchestrator import state as _state

    raw = '{"schemaVersion":1}'
    spark = _ManifestAwareSpark(_one_exec_row(), manifest_rows=[raw, raw])
    ctx = _state.read_content_pack_resumable_state(spark, _paths_mock(), "r")
    assert ctx.run_manifest_raw == raw
