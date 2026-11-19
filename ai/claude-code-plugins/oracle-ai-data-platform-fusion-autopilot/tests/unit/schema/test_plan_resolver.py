"""Phase 9 (ADR-0022): schema.plan_resolver walks ResolvedPack.

Replaces the v1 tests that exercised the resolver against
``BRONZE_EXTRACT_METADATA`` / ``SILVER_DIM_METADATA`` /
``GOLD_MART_METADATA``. The resolver now consumes a ``ResolvedPack``
(loaded by the caller — ``commands/run.py`` for both inline and REST
dispatch paths) and walks ``pack.bronze ∪ pack.silver ∪ pack.gold``
plus each node's ``dependsOn`` edges.
"""

from __future__ import annotations

import pathlib

import pytest

from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
    MissingDependencyError,
)
from oracle_ai_data_platform_fusion_autopilot.schema.plan_resolver import (
    resolve_dry_run_plan,
)


PACK_YAML = """\
id: plan-resolver-test-pack
version: 0.1.0
compatibility:
  pluginMinVersion: 0.1.0
"""


def _bronze_yaml(node_id: str) -> str:
    return f"""\
id: {node_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {node_id.upper()}_PVO
  biccSchema: Financial
target: {node_id}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - {{ name: ID, type: long, nullable: false, pii: none }}
    - {{ name: _extract_ts, type: timestamp, nullable: false, pii: none }}
    - {{ name: _source_pvo, type: string, nullable: false, pii: none }}
    - {{ name: _run_id, type: string, nullable: false, pii: none }}
    - {{ name: _watermark_used, type: timestamp, nullable: true, pii: none }}
"""


DIM_SUPPLIER = """\
id: dim_supplier
layer: silver
implementation:
  type: sql
  sql: silver/dim_supplier.sql
target: dim_supplier
dependsOn:
  bronze:
    - id: erp_suppliers
  silver: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_id
      type: long
      nullable: false
      pii: none
"""


SUPPLIER_SPEND = """\
id: supplier_spend
layer: gold
implementation:
  type: sql
  sql: gold/supplier_spend.sql
target: supplier_spend
dependsOn:
  bronze:
    - id: ap_invoices
  silver:
    - id: dim_supplier
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: supplier_id
      type: long
      nullable: false
      pii: none
"""


_BUNDLE_BASE = """\
apiVersion: aidp-fusion-autopilot/v1
project: plan-resolver-test
fusion:
  serviceUrl: https://example.com
  username: u
  password: p
  externalStorage: x
aidp:
  catalog: fusion_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
"""


def _bundle(extra: str):
    from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
    import yaml as _yaml
    return Bundle.model_validate(_yaml.safe_load(_BUNDLE_BASE + extra))


@pytest.fixture
def pack(tmp_path: pathlib.Path):
    from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
        load_pack,
    )

    root = tmp_path / "pack"
    root.mkdir()
    (root / "pack.yaml").write_text(PACK_YAML)
    (root / "bronze").mkdir()
    (root / "bronze" / "erp_suppliers.yaml").write_text(_bronze_yaml("erp_suppliers"))
    (root / "bronze" / "ap_invoices.yaml").write_text(_bronze_yaml("ap_invoices"))
    (root / "silver").mkdir()
    (root / "silver" / "dim_supplier.yaml").write_text(DIM_SUPPLIER)
    (root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_id")
    (root / "gold").mkdir()
    (root / "gold" / "supplier_spend.yaml").write_text(SUPPLIER_SPEND)
    (root / "gold" / "supplier_spend.sql").write_text("SELECT 1 AS supplier_id")
    return load_pack(root)


@pytest.fixture
def bundle():
    return _bundle(
        """\
datasets:
  - id: erp_suppliers
  - id: ap_invoices
dimensions:
  build:
    - dim_supplier
gold:
  marts:
    - supplier_spend
"""
    )


@pytest.fixture
def paths():
    return TablePaths(
        catalog="fusion_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
    )


class TestResolveDryRunPlan:
    def test_basic_topo_sort(self, pack, bundle, paths):
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=None,
        )
        ids = [n.dataset_id for n in plan]
        assert set(ids) == {
            "erp_suppliers", "ap_invoices", "dim_supplier", "supplier_spend",
        }
        # bronze deps before silver consumers; silver before gold.
        assert ids.index("erp_suppliers") < ids.index("dim_supplier")
        assert ids.index("dim_supplier") < ids.index("supplier_spend")
        assert prereqs == ()

    def test_layers_silver_filter_pulls_bronze_via_d1_closure(
        self, pack, bundle, paths,
    ):
        """Round-6 review fix: ``--layers silver`` filters declared
        ROOTS to silver, but D-1 transitive deps remain in the plan
        (matches runtime ``resolve_content_pack_plan``). Pre-fix this
        emitted ``erp_suppliers`` as a prereq instead of including it
        in the plan, which diverged from the real runtime plan."""
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=["silver"],
        )
        plan_ids = sorted(n.dataset_id for n in plan)
        # silver root: dim_supplier (the only silver in the bundle)
        # D-1 transitive: erp_suppliers (dim_supplier depends on it)
        assert plan_ids == ["dim_supplier", "erp_suppliers"]
        # bronze before silver in topo-sort.
        plan_order = [n.dataset_id for n in plan]
        assert plan_order.index("erp_suppliers") < plan_order.index("dim_supplier")
        # Prereqs is empty under the new mirror-runtime contract; every
        # materializable dep lands in the plan instead.
        assert prereqs == ()

    def test_datasets_filter_pulls_full_d1_closure_into_plan(
        self, pack, bundle, paths,
    ):
        """Round-6 review fix: ``--datasets supplier_spend`` on a
        bundle that declares the full chain must surface the SAME
        plan as runtime — the D-1 closure (bronze + silver + gold),
        not just the CLI dataset. Pre-fix returned only
        ``[supplier_spend]`` in the plan with the upstreams as
        prereqs, which silently diverged from the real runtime
        materialization plan."""
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=["supplier_spend"], layers=None,
        )
        plan_ids = sorted(n.dataset_id for n in plan)
        # CLI root: supplier_spend. D-1 closure:
        #   supplier_spend → ap_invoices (bronze) + dim_supplier (silver)
        #   dim_supplier → erp_suppliers (bronze, transitive)
        assert plan_ids == [
            "ap_invoices", "dim_supplier", "erp_suppliers", "supplier_spend",
        ]
        # Topological invariants: bronze before silver before gold.
        plan_order = [n.dataset_id for n in plan]
        assert plan_order.index("ap_invoices") < plan_order.index("supplier_spend")
        assert plan_order.index("erp_suppliers") < plan_order.index("dim_supplier")
        assert plan_order.index("dim_supplier") < plan_order.index("supplier_spend")
        assert prereqs == ()

    def test_strict_scope_cli_filter_raises_when_chain_declared_in_bundle(
        self, pack, bundle, paths,
    ):
        """Round-6 review fix: ``--datasets supplier_spend
        --strict-scope`` on a bundle that declares the full chain
        MUST raise AIDPF-1042 — effective_roots is the CLI dataset
        set (``{supplier_spend}``), and ap_invoices / dim_supplier
        aren't in that set even though they're in the bundle. This
        mirrors the runtime ``resolve_content_pack_plan`` contract;
        pre-fix dry-run silently accepted while runtime raised."""
        with pytest.raises(MissingDependencyError, match="AIDPF-1042"):
            resolve_dry_run_plan(
                pack, bundle, paths,
                datasets=["supplier_spend"], layers=None,
                strict_scope=True,
            )

    def test_unknown_dataset_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets:
  - id: totally_unknown
dimensions:
  build: []
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="totally_unknown"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_typoed_datasets_filter_raises(self, pack, bundle, paths):
        with pytest.raises(MissingDependencyError, match="dim_typo"):
            resolve_dry_run_plan(
                pack, bundle, paths, datasets=["dim_typo"], layers=None,
            )

    def test_typoed_layers_filter_raises(self, pack, bundle, paths):
        with pytest.raises(MissingDependencyError, match="unknown_layer"):
            resolve_dry_run_plan(
                pack, bundle, paths, datasets=None, layers=["unknown_layer"],
            )

    def test_disabled_dataset_excluded(self, pack, paths):
        # Override Pydantic defaults — empty dimensions/gold to keep
        # the test scoped to bronze enable/disable behavior.
        b = _bundle(
            """\
datasets:
  - id: erp_suppliers
  - id: ap_invoices
    enabled: false
dimensions:
  build: []
gold:
  marts: []
"""
        )
        plan, _ = resolve_dry_run_plan(
            pack, b, paths, datasets=None, layers=None,
        )
        ids = {n.dataset_id for n in plan}
        assert "ap_invoices" not in ids
        assert "erp_suppliers" in ids

    def test_undeclared_bronze_upstream_auto_included_by_default(self, pack, paths):
        """Phase 9 default (strict_scope=False): D-1 auto-includes
        the bronze dep transitively — matches the inline resolver's
        contract so REST dispatch dry-run agrees with --inline."""
        b = _bundle(
            """\
datasets: []
dimensions:
  build:
    - dim_supplier
gold:
  marts: []
"""
        )
        plan, _ = resolve_dry_run_plan(
            pack, b, paths, datasets=None, layers=None,
        )
        ids = {n.dataset_id for n in plan}
        assert "dim_supplier" in ids
        assert "erp_suppliers" in ids, (
            "D-1 must auto-include the bronze dep of dim_supplier; got "
            f"{sorted(ids)!r}"
        )

    def test_undeclared_bronze_upstream_raises_with_strict_scope(
        self, pack, paths,
    ):
        """Phase 9 ``strict_scope=True`` opts out of D-1 — undeclared
        upstreams raise AIDPF-1042."""
        b = _bundle(
            """\
datasets: []
dimensions:
  build:
    - dim_supplier
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="erp_suppliers"):
            resolve_dry_run_plan(
                pack, b, paths, datasets=None, layers=None,
                strict_scope=True,
            )

    def test_unknown_silver_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets: []
dimensions:
  build:
    - dim_does_not_exist
gold:
  marts: []
"""
        )
        with pytest.raises(MissingDependencyError, match="dim_does_not_exist"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_unknown_gold_in_bundle_raises(self, pack, paths):
        b = _bundle(
            """\
datasets: []
dimensions:
  build: []
gold:
  marts:
    - mart_does_not_exist
"""
        )
        with pytest.raises(MissingDependencyError, match="mart_does_not_exist"):
            resolve_dry_run_plan(pack, b, paths, datasets=None, layers=None)

    def test_layer_filter_empties_plan_raises_aidpf_1045(
        self, pack, bundle, paths,
    ):
        """Round-8 review fix: ``--datasets supplier_spend --layers
        silver`` filters the single gold root out before D-1
        expansion, leaving effective_roots empty. Runtime raises
        ``AIDPF-1045_LAYER_FILTER_EMPTIED_PLAN``; pre-fix the schema
        resolver silently returned an empty plan, so REST dispatch
        --dry-run reported success on a run the cluster would
        reject."""
        with pytest.raises(MissingDependencyError, match="AIDPF-1045"):
            resolve_dry_run_plan(
                pack, bundle, paths,
                datasets=["supplier_spend"], layers=["silver"],
            )

    def test_layer_filter_empties_plan_bronze_only_bundle_raises(
        self, pack, paths,
    ):
        """Round-8: bronze-only bundle (no silver/gold declared) with
        ``--layers gold`` removes every root → AIDPF-1045. Runtime
        raises the same; pre-fix dry-run silently returned an empty
        plan."""
        b = _bundle(
            """\
datasets:
  - id: erp_suppliers
  - id: ap_invoices
"""
        )
        with pytest.raises(MissingDependencyError, match="AIDPF-1045"):
            resolve_dry_run_plan(
                pack, b, paths, datasets=None, layers=["gold"],
            )

    def test_layers_gold_on_gold_root_still_pulls_d1_deps(
        self, pack, bundle, paths,
    ):
        """Round-8 positive test: ``--layers gold`` against a bundle
        with a gold root keeps the gold mart in effective_roots and
        D-1 still pulls bronze + silver deps into the plan. The
        AIDPF-1045 guard must NOT fire here."""
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=["gold"],
        )
        plan_ids = {n.dataset_id for n in plan}
        assert plan_ids == {
            "supplier_spend", "ap_invoices", "dim_supplier", "erp_suppliers",
        }
        assert prereqs == ()

    def test_omitted_legacy_blocks_dont_fold_pydantic_defaults(
        self, pack, paths,
    ):
        """Round-7 review fix: a Phase-9 bundle that omits the legacy
        ``dimensions:`` and ``gold:`` blocks entirely must NOT have
        ``DimensionsSpec.build`` / ``GoldSpec.marts`` Pydantic defaults
        (``dim_supplier``, ``dim_account``, ``dim_calendar``,
        ``dim_org``; ``ar_aging``, ``ap_aging``, ``gl_balance``,
        ``po_backlog``) folded into the resolver's classification —
        those would either raise MissingDependencyError against a
        starter pack that doesn't ship them all, or surface unintended
        silver/gold roots in the dry-run plan that runtime will never
        dispatch.

        Pre-fix dry-run iterated bundle.dimensions.build /
        bundle.gold.marts unconditionally; runtime's
        ``_effective_bundle_scope`` filtered by
        ``bundle.model_fields_set`` so the two diverged. Now both
        honor the same rule.
        """
        b = _bundle(
            """\
datasets:
  - id: erp_suppliers
"""
        )
        plan, prereqs = resolve_dry_run_plan(
            pack, b, paths, datasets=None, layers=None,
        )
        plan_ids = {n.dataset_id for n in plan}
        assert plan_ids == {"erp_suppliers"}, (
            f"omitted-blocks bundle must yield single-root plan; got "
            f"{sorted(plan_ids)} — Pydantic defaults leaking through?"
        )
        assert prereqs == ()

    def test_omitted_legacy_blocks_match_inline_dry_run_plan(
        self, pack, paths,
    ):
        """Round-7 review fix: REST/inline dry-run parity for the
        omitted-blocks case. Bundle with only ``datasets:`` must
        produce the same plan ids in both resolvers."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _build_content_pack_dry_run_plan,
            _effective_bundle_scope,
        )
        b = _bundle(
            """\
datasets:
  - id: erp_suppliers
"""
        )
        scope = _effective_bundle_scope(b)
        inline_plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=None, layers=None,
            bundle_scope=scope,
        )
        inline_ids = {n.dataset_id for n in inline_plan}

        rest_plan, rest_prereqs = resolve_dry_run_plan(
            pack, b, paths, datasets=None, layers=None,
        )
        rest_ids = {n.dataset_id for n in rest_plan}
        assert rest_prereqs == ()
        assert inline_ids == rest_ids, (
            f"REST/inline divergence on omitted-blocks bundle: "
            f"inline={sorted(inline_ids)} rest={sorted(rest_ids)}"
        )

    def test_resolve_dry_run_plan_accepts_custom_table_paths(self, pack, bundle):
        """``paths`` is retained in the signature for back-compat after
        the round-6 mirror-runtime refactor (prereqs is empty so the
        path argument is no longer load-bearing in the typical case).
        Passing a custom TablePaths must not crash."""
        custom = TablePaths(
            catalog="custom_cat",
            bronze_schema="custom_bronze",
            silver_schema="custom_silver",
            gold_schema="custom_gold",
        )
        plan, prereqs = resolve_dry_run_plan(
            pack, bundle, custom, datasets=["supplier_spend"], layers=None,
        )
        # Prereqs always empty under the mirror-runtime contract.
        assert prereqs == ()
        # Plan still contains the D-1 closure.
        plan_ids = {n.dataset_id for n in plan}
        assert plan_ids == {
            "supplier_spend", "ap_invoices", "dim_supplier", "erp_suppliers",
        }
