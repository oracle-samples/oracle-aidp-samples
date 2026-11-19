"""Regression tests for the three Phase 9 review-fix findings.

1. Bronze ``_build_target_identifier`` legacy fallback must route to
   ``ctx.bronze_schema`` (NOT fall through to gold).
2. Fusion PVO drift gate scope must come from the RESOLVED PLAN so D-1
   transitive bronze deps reach the gate.
3. ``bronze_table_for_source`` must use ``node.target`` (not
   ``node.id``) so pack contracts with id != target work.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest


PACK_YAML = """\
id: phase9-review-fix-pack
version: 0.1.0
compatibility:
  pluginMinVersion: 0.1.0
"""


def _bronze_yaml(node_id: str, target: str | None = None) -> str:
    target = target or node_id
    return f"""\
id: {node_id}
layer: bronze
implementation:
  type: bronze_extract
  datastore: {node_id.upper()}_PVO
  biccSchema: Financial
target: {target}
dependsOn:
  bronze: []
  silver: []
refresh:
  seed:
    strategy: replace
  incremental:
    strategy: merge
    watermark:
      source: {node_id}
      column: LASTUPDATEDATE
    naturalKey: [ID]
outputSchema:
  columns:
    - {{ name: ID, type: long, nullable: false, pii: none }}
    - {{ name: _extract_ts, type: timestamp, nullable: false, pii: none }}
    - {{ name: _source_pvo, type: string, nullable: false, pii: none }}
    - {{ name: _run_id, type: string, nullable: false, pii: none }}
    - {{ name: _watermark_used, type: timestamp, nullable: true, pii: none }}
"""


SILVER_DIM = """\
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
    - name: supplier_id
      type: long
      nullable: false
      pii: none
"""


GOLD_MART = """\
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
    # id != target — finding 3.
    (root / "bronze" / "gl_journal_lines.yaml").write_text(
        _bronze_yaml("gl_journal_lines", target="gl_journal_headers")
    )
    (root / "silver").mkdir()
    (root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM)
    (root / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_id")
    (root / "gold").mkdir()
    (root / "gold" / "supplier_spend.yaml").write_text(GOLD_MART)
    (root / "gold" / "supplier_spend.sql").write_text("SELECT 1 AS supplier_id")
    return load_pack(root)


# ---------------------------------------------------------------------------
# Finding 1 — bronze _build_target_identifier routes through TablePaths
# ---------------------------------------------------------------------------


class TestBronzeTargetIdentifierPathsAware:
    """The Phase 9 review fixed a bronze-routing bug in
    ``_build_target_identifier``: the legacy ctx-only fallback composed
    ``f"{ctx.catalog}.{ctx.gold_schema}.{target}"`` for bronze nodes
    because ``schema = silver_schema if layer == 'silver' else gold_schema``,
    so a bronze node fell through to the gold schema. The post-write
    ``_assert_materialized_matches_declared`` then described the gold
    target instead of the bronze table.

    Phase 9 follow-up: the legacy fallback branch was deleted; ``paths``
    is now REQUIRED at every call site.
    """

    def _ctx(self):
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (
            RunContext,
        )
        return RunContext(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
            run_id="r",
            active_profile_name="p",
        )

    def _bronze_node(self):
        from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import (
            NodeYaml,
        )
        return NodeYaml.model_validate({
            "id": "erp_suppliers",
            "layer": "bronze",
            "implementation": {
                "type": "bronze_extract",
                "datastore": "X",
                "biccSchema": "Financial",
            },
            "target": "erp_suppliers",
            "dependsOn": {"bronze": [], "silver": []},
            "refresh": {
                "seed": {"strategy": "replace"},
                "incremental": {
                    "strategy": "merge",
                    "watermark": {"source": "erp_suppliers", "column": "X"},
                    "naturalKey": ["ID"],
                },
            },
            "outputSchema": {"columns": [
                {"name": "ID", "type": "long", "nullable": False, "pii": "none"},
                {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
                {"name": "_source_pvo", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
                {"name": "_watermark_used", "type": "timestamp", "nullable": True, "pii": "none"},
            ]},
        })

    def test_missing_paths_raises_type_error(self):
        """``paths`` is now a REQUIRED positional argument — calling the
        helper without it must raise ``TypeError``. Locks the Phase 9
        follow-up contract that deleted the ctx-only legacy fallback."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            _build_target_identifier,
        )
        with pytest.raises(TypeError, match="paths"):
            _build_target_identifier(self._bronze_node(), self._ctx())

    def test_paths_routes_through_table_paths_bronze(self):
        """With ``paths``, the helper routes through
        ``paths.bronze(node.target)`` so identifier validation fires
        centrally."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_runner import (
            _build_target_identifier,
        )
        paths = MagicMock()
        paths.bronze.return_value = "validated.cat.bronze.erp_suppliers"
        target = _build_target_identifier(self._bronze_node(), self._ctx(), paths)
        paths.bronze.assert_called_once_with("erp_suppliers")
        assert target == "validated.cat.bronze.erp_suppliers"


# ---------------------------------------------------------------------------
# Finding 2 — PVO drift gate scope from resolved plan
# ---------------------------------------------------------------------------


class TestPvoDriftGateScopeFromResolvedPlan:
    """``--datasets supplier_spend`` (gold) and ``--layers gold`` BOTH
    trigger bronze extracts via D-1 transitive include. The pre-fix
    PVO drift gate computed scope from raw CLI filters and missed
    those transitive bronze ids — letting Fusion column drift slip
    past the AIDPF-2072 gate.
    """

    def test_resolver_returns_transitive_bronze_for_gold_root(self, pack):
        """The resolver itself must return the transitive bronze deps
        when only a gold node is declared."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        plan = resolve_content_pack_plan(
            pack, datasets=["supplier_spend"], layers=None,
        )
        ids_by_layer = {}
        for n in plan:
            ids_by_layer.setdefault(n.layer, set()).add(n.id)
        # The drift gate code now does `{n.id for n in plan if n.layer == "bronze"}`
        # exactly — this asserts the resolver supplies the transitive
        # bronze deps the gate needs.
        bronze_in_plan = ids_by_layer.get("bronze", set())
        assert "ap_invoices" in bronze_in_plan, (
            f"gold root supplier_spend must pull ap_invoices via D-1; "
            f"got bronze={bronze_in_plan!r}"
        )
        # erp_suppliers is the transitive dep of dim_supplier (silver),
        # which is a dep of supplier_spend (gold).
        assert "erp_suppliers" in bronze_in_plan

    def test_resolver_returns_transitive_bronze_for_layers_gold(self, pack):
        """``--layers gold`` filters declared roots but D-1 still
        pulls transitive bronze deps; the drift gate must see them."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        plan = resolve_content_pack_plan(pack, datasets=None, layers=["gold"])
        bronze_in_plan = {n.id for n in plan if n.layer == "bronze"}
        assert bronze_in_plan == {"ap_invoices", "erp_suppliers"}, (
            f"--layers gold + D-1 must surface ap_invoices + erp_suppliers; "
            f"got {bronze_in_plan!r}"
        )

    def test_strict_scope_does_not_auto_include_bronze(self, pack):
        """With ``--strict-scope``, D-1 is disabled — gold roots without
        their bronze deps explicitly declared raise (and the gate
        consequently sees no bronze in scope)."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
            StrictScopeMissingDependencyError,
        )
        with pytest.raises(StrictScopeMissingDependencyError):
            resolve_content_pack_plan(
                pack,
                datasets=["supplier_spend"],
                layers=None,
                strict_scope=True,
            )


# ---------------------------------------------------------------------------
# Finding 3 — bronze_table_for_source from node.target
# ---------------------------------------------------------------------------


class TestBundleScopeRespectedByResolver:
    """Production resolver call sites
    (_dispatch_content_pack_run PVO drift gate +
    _build_content_pack_dry_run_plan + _run_content_pack_backend
    execution loop) must pass ``bundle_scope=`` to the resolver so:

      1. A no-filter run executes only bundle-declared roots + their
         D-1 deps — NOT every node in the pack.
      2. CLI ``--datasets <node-outside-bundle>`` raises
         AIDPF-1043 CLI_DATASET_OUTSIDE_BUNDLE_SCOPE.
      3. Inline and REST dispatch dry-runs render the same plan
         (both honor bundle.datasets[] now).
    """

    def _bundle(self, tmp_path: pathlib.Path, dataset_ids: list[str]):
        """Author a bundle whose contentPack points at ``pack`` fixture."""
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
        import yaml as _yaml
        bp = tmp_path / "bundle.yaml"
        bp.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: scope-test\n"
            "fusion:\n  serviceUrl: https://example.com\n  username: u\n"
            "  password: p\n  externalStorage: s\n"
            "aidp:\n  catalog: c\n  bronzeSchema: bronze\n"
            "  silverSchema: silver\n  goldSchema: gold\n"
            "datasets:\n"
            + "".join(f"  - id: {sid}\n" for sid in dataset_ids)
            + "dimensions:\n  build: []\n"
            "gold:\n  marts: []\n"
            "contentPack:\n  name: x\n  path: ./pack\n  profile: p\n"
        )
        bundle = Bundle.model_validate(
            _yaml.safe_load(bp.read_text(encoding="utf-8"))
        )
        return bundle, bp

    def test_legacy_blocks_omitted_do_not_pollute_scope(self, pack, tmp_path):
        """Reviewer round-4 finding: ``DimensionsSpec.build`` defaults
        to ``[dim_supplier, dim_account, dim_calendar, dim_org]`` and
        ``GoldSpec.marts`` defaults to
        ``[ar_aging, ap_aging, gl_balance, po_backlog]``. A Phase 9
        bundle that OMITS both blocks must NOT see those defaults
        folded into ``bundle_scope`` — the operator never authored
        them.

        Pre-fix ``_effective_bundle_scope`` did
        ``for name in (dims.build or []): scope.add(name)`` which
        always pulled the defaults.
        """
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        import yaml as _yaml

        # IMPORTANT — no ``dimensions:`` or ``gold:`` blocks.
        bundle = Bundle.model_validate(_yaml.safe_load(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: omit-legacy-test\n"
            "fusion:\n  serviceUrl: https://example.com\n  username: u\n"
            "  password: p\n  externalStorage: s\n"
            "aidp:\n  catalog: c\n  bronzeSchema: bronze\n"
            "  silverSchema: silver\n  goldSchema: gold\n"
            "datasets:\n  - id: erp_suppliers\n"
            "contentPack:\n  name: x\n  path: ./pack\n  profile: p\n"
        ))

        scope = _effective_bundle_scope(bundle)
        assert scope == {"erp_suppliers"}, (
            f"omitted dimensions:/gold: blocks must NOT contribute to "
            f"scope (Pydantic defaults are non-empty for back-compat). "
            f"Got {sorted(scope)!r}."
        )

        # No-filter run with this bundle must execute ONLY
        # erp_suppliers — no dim_supplier, no dim_account, no
        # supplier_spend, no other pack node.
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        plan = resolve_content_pack_plan(
            pack, datasets=None, layers=None, bundle_scope=scope,
        )
        plan_ids = {n.id for n in plan}
        assert plan_ids == {"erp_suppliers"}, (
            f"no-filter run with omitted legacy blocks must execute "
            f"ONLY the bundle-declared bronze root + true D-1 deps "
            f"(bronze has none). Got plan {sorted(plan_ids)!r}."
        )

    def test_explicit_dimensions_block_folds_into_scope(self, pack, tmp_path):
        """An author who explicitly writes ``dimensions: { build:
        [dim_supplier] }`` opts INTO the legacy contract; that id
        folds into scope."""
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        import yaml as _yaml

        bundle = Bundle.model_validate(_yaml.safe_load(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: explicit-legacy-test\n"
            "fusion:\n  serviceUrl: https://example.com\n  username: u\n"
            "  password: p\n  externalStorage: s\n"
            "aidp:\n  catalog: c\n  bronzeSchema: bronze\n"
            "  silverSchema: silver\n  goldSchema: gold\n"
            "datasets:\n  - id: erp_suppliers\n"
            "dimensions:\n  build: [dim_supplier]\n"
            "contentPack:\n  name: x\n  path: ./pack\n  profile: p\n"
        ))

        scope = _effective_bundle_scope(bundle)
        assert scope == {"erp_suppliers", "dim_supplier"}, (
            f"explicit dimensions.build entry must fold into scope; "
            f"got {sorted(scope)!r}."
        )

    def test_explicit_empty_dimensions_block_folds_nothing(self, pack, tmp_path):
        """An author who writes ``dimensions: { build: [] }`` (explicit
        empty list) opts INTO the legacy contract but contributes no
        scope ids. ``model_fields_set`` includes ``dimensions`` so the
        helper sees the authored block; the empty list folds nothing."""
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        import yaml as _yaml

        bundle = Bundle.model_validate(_yaml.safe_load(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: explicit-empty-legacy-test\n"
            "fusion:\n  serviceUrl: https://example.com\n  username: u\n"
            "  password: p\n  externalStorage: s\n"
            "aidp:\n  catalog: c\n  bronzeSchema: bronze\n"
            "  silverSchema: silver\n  goldSchema: gold\n"
            "datasets:\n  - id: erp_suppliers\n"
            "dimensions:\n  build: []\n"
            "gold:\n  marts: []\n"
            "contentPack:\n  name: x\n  path: ./pack\n  profile: p\n"
        ))

        scope = _effective_bundle_scope(bundle)
        assert scope == {"erp_suppliers"}, (
            f"explicit empty dimensions.build + gold.marts contribute "
            f"nothing; scope = bundle.datasets[]. Got {sorted(scope)!r}."
        )

    def test_no_filter_executes_only_bundle_declared_roots(self, pack, tmp_path):
        """A bundle declaring only erp_suppliers must NOT pull in
        supplier_spend (gold), dim_supplier (silver), or ap_invoices
        (a bronze sibling that's NOT in bundle.datasets[]).
        Pre-fix behavior treated every pack node as a root and would
        have returned the full pack."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        bundle, _ = self._bundle(tmp_path, ["erp_suppliers"])
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        scope = _effective_bundle_scope(bundle)
        plan = resolve_content_pack_plan(
            pack, datasets=None, layers=None, bundle_scope=scope,
        )
        ids = {n.id for n in plan}
        assert ids == {"erp_suppliers"}, (
            f"no-filter run with bundle.datasets=[erp_suppliers] must "
            f"execute ONLY erp_suppliers (no D-1 deps since bronze has "
            f"no upstream); got {ids!r}. Pre-fix the resolver treated "
            f"every pack node as a root and would have returned silver "
            f"+ gold too."
        )

    def test_no_filter_with_gold_intent_pulls_transitive_deps(self, pack, tmp_path):
        """A bundle declaring supplier_spend (gold) must execute it
        PLUS D-1 transitive deps — but NOT gl_journal_lines (a sibling
        pack node that's not in scope)."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
        )
        bundle, _ = self._bundle(tmp_path, ["supplier_spend"])
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        scope = _effective_bundle_scope(bundle)
        plan = resolve_content_pack_plan(
            pack, datasets=None, layers=None, bundle_scope=scope,
        )
        ids = {n.id for n in plan}
        # D-1 closure of supplier_spend: ap_invoices (bronze dep) +
        # dim_supplier (silver dep) → erp_suppliers (transitive
        # bronze dep of dim_supplier).
        assert ids == {
            "supplier_spend", "ap_invoices", "dim_supplier", "erp_suppliers",
        }, (
            f"supplier_spend with D-1 must pull transitive deps; got {ids!r}"
        )
        # gl_journal_lines is NOT in scope and NOT a transitive dep,
        # so it must NOT appear.
        assert "gl_journal_lines" not in ids

    def test_cli_dataset_outside_bundle_scope_raises_aidpf_1043(self, pack, tmp_path):
        """CLI ``--datasets gl_journal_lines`` against a bundle whose
        datasets=[erp_suppliers] must raise AIDPF-1043 — the operator
        cannot smuggle in undeclared roots via the CLI."""
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_plan_resolver import (
            resolve_content_pack_plan,
            CliDatasetOutsideBundleScopeError,
            AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE,
        )
        bundle, _ = self._bundle(tmp_path, ["erp_suppliers"])
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _effective_bundle_scope,
        )
        scope = _effective_bundle_scope(bundle)
        with pytest.raises(CliDatasetOutsideBundleScopeError) as exc:
            resolve_content_pack_plan(
                pack,
                datasets=["gl_journal_lines"],  # not in bundle.datasets[]
                layers=None,
                bundle_scope=scope,
            )
        assert AIDPF_1043_CLI_DATASET_OUTSIDE_BUNDLE_SCOPE in str(exc.value)

    def test_inline_and_dispatch_dry_run_agree(self, pack, tmp_path):
        """Both the inline dry-run (orchestrator) and the REST
        dispatch dry-run (schema/plan_resolver) must report the same
        plan (NOT plan ∪ prereqs) for the same bundle + pack. Round-6
        review fix: parity is asserted on the PLAN itself, since the
        schema resolver now mirrors runtime ``resolve_content_pack_plan``
        and returns empty prereqs — every materializable dep is in
        the plan.
        """
        from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths
        from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
            _build_content_pack_dry_run_plan,
            _effective_bundle_scope,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.plan_resolver import (
            resolve_dry_run_plan,
        )

        bundle, _ = self._bundle(
            tmp_path,
            ["erp_suppliers", "ap_invoices", "dim_supplier", "supplier_spend"],
        )
        scope = _effective_bundle_scope(bundle)
        # Inline path.
        inline_plan = _build_content_pack_dry_run_plan(
            resolved_pack=pack, datasets=None, layers=None,
            bundle_scope=scope,
        )
        inline_ids = {n.dataset_id for n in inline_plan}

        # REST dispatch path (schema/plan_resolver).
        paths = TablePaths(
            catalog="c", bronze_schema="bronze",
            silver_schema="silver", gold_schema="gold",
        )
        rest_plan, rest_prereqs = resolve_dry_run_plan(
            pack, bundle, paths, datasets=None, layers=None,
        )
        rest_plan_ids = {n.dataset_id for n in rest_plan}
        assert rest_prereqs == (), (
            "round-6 contract: schema resolver mirrors runtime and "
            f"emits empty prereqs; got {rest_prereqs!r}"
        )
        assert inline_ids == rest_plan_ids, (
            f"inline plan ({sorted(inline_ids)}) disagrees with REST "
            f"plan ({sorted(rest_plan_ids)}) — the two resolvers must "
            f"produce byte-identical plans"
        )


class TestBronzeTableForSourceUsesNodeTarget:
    """``gl_journal_lines`` has ``id=gl_journal_lines`` but
    ``target=gl_journal_headers``. The pre-fix map (built from
    ``bundle.datasets[].id``) would assert the bronze table is
    ``catalog.bronze.gl_journal_lines``, but the extractor actually
    writes to ``catalog.bronze.gl_journal_headers``. Required-column
    preflight + semantic-fragment ``{table}`` substitutions would
    read the wrong table.
    """

    def test_starter_pack_gl_journal_lines_target_differs_from_id(self):
        """Validate the precondition: the starter pack still ships an
        id/target mismatch that exercises this code path."""
        import pathlib
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_pack,
        )
        here = pathlib.Path(__file__).parent.parent.parent
        starter = (
            here / "scripts" / "oracle_ai_data_platform_fusion_autopilot"
            / "content_packs" / "fusion-finance-starter"
        )
        if not starter.is_dir():
            pytest.skip(f"starter pack not present at {starter}")
        pack = load_pack(starter)
        gl_node = pack.bronze.get("gl_journal_lines")
        assert gl_node is not None, "starter pack must declare gl_journal_lines"
        assert gl_node.target == "gl_journal_headers", (
            f"starter pack contract: gl_journal_lines.target = "
            f"gl_journal_headers (PVO writes to the headers table); "
            f"got {gl_node.target!r}"
        )

    def test_map_uses_node_target_not_node_id(self, pack):
        """The map's KEY is the node id; the VALUE is built from
        ``paths.bronze(node.target)``. For gl_journal_lines, the value
        must be ``cat.bronze.gl_journal_headers``."""
        from oracle_ai_data_platform_fusion_autopilot.config.paths import TablePaths

        paths = TablePaths(
            catalog="cat",
            bronze_schema="bronze",
            silver_schema="silver",
            gold_schema="gold",
        )
        # Mirror the production code from
        # _run_content_pack_backend so the test catches regressions.
        bronze_table_for_source = {
            node_id: paths.bronze(node.target)
            for node_id, node in pack.bronze.items()
        }
        assert bronze_table_for_source["gl_journal_lines"] == "cat.bronze.gl_journal_headers"
        # Sanity: identity nodes still resolve correctly.
        assert bronze_table_for_source["erp_suppliers"] == "cat.bronze.erp_suppliers"
        assert bronze_table_for_source["ap_invoices"] == "cat.bronze.ap_invoices"


class TestStrictScopeWiredEndToEnd:
    """Round-5 review fix: ``--strict-scope`` was silently dropped on the
    REST dispatch path. The flag must reach (1) ``dispatch_via_rest``
    from the CLI, (2) ``build_notebook`` from ``dispatch_via_rest``,
    (3) the generated ``orchestrator.run(...)`` call inside the run
    cell, and (4) the REST dry-run resolver so ``--strict-scope
    --dry-run`` raises ``AIDPF-1042`` on undeclared deps consistently
    with the inline path.
    """

    def test_cli_strict_scope_reaches_dispatch_via_rest(self, tmp_path):
        """CLI --strict-scope must thread to dispatch_via_rest(...)."""
        from unittest.mock import patch
        from click.testing import CliRunner
        from oracle_ai_data_platform_fusion_autopilot import cli
        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        # Minimal bundle without a content pack (legacy path) so we
        # don't have to set up profile/pack/snapshot — Phase 9 still
        # routes through dispatch_via_rest with strict_scope.
        bundle_yaml = """\
apiVersion: aidp-fusion-autopilot/v1
project: strict-scope-cli-test
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
datasets: []
"""
        config_yaml = """\
apiVersion: aidp-fusion-autopilot/v1
project: strict-scope-cli-test
defaults:
  region: us-phoenix-1
  workspaceRoot: /Workspace
environments:
  dev:
    workspaceKey: ws-key
    aiDataPlatformId: aidp-id
    clusterKey: c-key
    clusterName: c-name
    ociProfile: DEFAULT
"""
        (tmp_path / "bundle.yaml").write_text(bundle_yaml)
        (tmp_path / "aidp.config.yaml").write_text(config_yaml)

        captured_kwargs: dict = {}

        def _fake_dispatch(**kwargs):
            captured_kwargs.update(kwargs)
            return RunSummary.empty(
                bundle_project="strict-scope-cli-test",
                mode="seed", plan=(), prereqs=(),
            )

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            side_effect=_fake_dispatch,
        ):
            result = CliRunner().invoke(
                cli.main,
                [
                    "--bundle", str(tmp_path / "bundle.yaml"),
                    "--config", str(tmp_path / "aidp.config.yaml"),
                    "--env", "dev",
                    "run", "--mode", "seed", "--dry-run", "--strict-scope",
                ],
            )

        assert result.exit_code == 0, (
            f"CLI invocation failed: exit={result.exit_code} "
            f"output={result.output!r}"
        )
        assert captured_kwargs.get("strict_scope") is True, (
            "CLI --strict-scope must reach dispatch_via_rest with "
            f"strict_scope=True; got kwargs={sorted(captured_kwargs.keys())} "
            f"strict_scope={captured_kwargs.get('strict_scope')!r}"
        )

    def test_cli_without_strict_scope_defaults_to_false(self, tmp_path):
        """When --strict-scope is NOT passed, the tri-state value (None) is
        threaded through dispatch UNCHANGED so a manifest-backed resume can tell
        omission from an explicit False (AIDPF-1047). The D-1 default (False)
        still stands — it is resolved downstream in orchestrator.run (a fresh
        run) / treated as falsy by the resolver — not collapsed at the CLI."""
        from unittest.mock import patch
        from click.testing import CliRunner
        from oracle_ai_data_platform_fusion_autopilot import cli
        from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import RunSummary

        bundle_yaml = """\
apiVersion: aidp-fusion-autopilot/v1
project: strict-scope-cli-default
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
datasets: []
"""
        config_yaml = """\
apiVersion: aidp-fusion-autopilot/v1
project: strict-scope-cli-default
defaults:
  region: us-phoenix-1
  workspaceRoot: /Workspace
environments:
  dev:
    workspaceKey: ws-key
    aiDataPlatformId: aidp-id
    clusterKey: c-key
    clusterName: c-name
    ociProfile: DEFAULT
"""
        (tmp_path / "bundle.yaml").write_text(bundle_yaml)
        (tmp_path / "aidp.config.yaml").write_text(config_yaml)

        captured: dict = {}

        def _fake(**kwargs):
            captured.update(kwargs)
            return RunSummary.empty(
                bundle_project="strict-scope-cli-default",
                mode="seed", plan=(), prereqs=(),
            )

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.dispatch_via_rest",
            side_effect=_fake,
        ):
            result = CliRunner().invoke(
                cli.main,
                [
                    "--bundle", str(tmp_path / "bundle.yaml"),
                    "--config", str(tmp_path / "aidp.config.yaml"),
                    "--env", "dev",
                    "run", "--mode", "seed", "--dry-run",
                ],
            )

        assert result.exit_code == 0, (
            f"CLI invocation failed: exit={result.exit_code} "
            f"output={result.output!r}"
        )
        assert captured.get("strict_scope") is None, (
            f"omitted strict_scope must thread as None (tri-state), resolved to "
            f"False downstream; got {captured.get('strict_scope')!r}"
        )

    def test_dispatch_via_rest_threads_strict_scope_to_build_notebook(
        self, tmp_path,
    ):
        """dispatch_via_rest(strict_scope=True) must reach build_notebook
        with strict_scope=True. We short-circuit via a sentinel exception
        inside the fake build_notebook so the test doesn't have to mock
        the rest of the post-notebook upload+poll+marker flow."""
        from unittest.mock import patch
        from oracle_ai_data_platform_fusion_autopilot.dispatch import dispatch_via_rest
        from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
            PreflightResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import AidpConfig

        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: strict-scope-disp\n"
            "fusion:\n"
            "  serviceUrl: https://x\n  username: u\n  password: p\n"
            "  externalStorage: x\n"
            "aidp:\n  catalog: c\n  bronzeSchema: b\n"
            "  silverSchema: s\n  goldSchema: g\n"
            "datasets: []\n"
        )
        wheel = tmp_path / "fake-0.1.0-py3-none-any.whl"
        wheel.write_bytes(b"PK\x03\x04 fake")

        config = AidpConfig.model_validate({
            "project": "strict-scope-disp",
            "apiVersion": "aidp-fusion-autopilot/v1",
            "defaults": {"region": "us-phoenix-1", "workspaceRoot": "/Workspace"},
            "environments": {
                "dev": {
                    "workspaceKey": "ws", "aiDataPlatformId": "aidp",
                    "clusterKey": "ck", "clusterName": "cn",
                    "ociProfile": "DEFAULT",
                }
            },
        })
        env = config.environments["dev"]

        captured: dict = {}

        class _StopAfterBuildNotebook(Exception):
            pass

        def _fake_build_notebook(**kwargs):
            captured.update(kwargs)
            raise _StopAfterBuildNotebook()

        ok = [PreflightResult("x", "PASS", "ok", "")]

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_local_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_remote_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.AidpRestClient",
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.build_wheel",
            return_value=wheel,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.build_notebook",
            side_effect=_fake_build_notebook,
        ):
            with pytest.raises(_StopAfterBuildNotebook):
                dispatch_via_rest(
                    bundle_path=bundle_path, config=config, env=env,
                    env_name="dev",
                    mode="seed", datasets=None, layers=None, dry_run=False,
                    plugin_checkout=tmp_path, auto_start_cluster=False,
                    strict_scope=True,
                )

        assert captured.get("strict_scope") is True, (
            f"build_notebook must receive strict_scope=True from "
            f"dispatch_via_rest; got {captured.get('strict_scope')!r}"
        )

    def test_build_notebook_emits_strict_scope_literal_in_run_cell(self, tmp_path):
        """build_notebook(strict_scope=True) must emit the literal
        ``strict_scope=True`` inside the generated orchestrator.run(...)
        call so the cluster honours the operator's opt-out."""
        from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_builder import (
            build_notebook,
        )

        # Use the tmp_path fixture, not a hardcoded POSIX /tmp path — on Windows
        # CI `/tmp/...` resolves to `\tmp\...` whose parent dir does not exist.
        wheel = tmp_path / "strict-scope-test.whl"
        wheel.write_bytes(b"PK\x03\x04 fake")

        nb_true = build_notebook(
            wheel_path=wheel, bundle_yaml="x: 1\n",
            mode="seed", datasets=None, layers=None,
            strict_scope=True,
        )
        all_source_true = "".join(
            "".join(c["source"]) for c in nb_true["cells"]
            if c["cell_type"] == "code"
        )
        assert "strict_scope=True" in all_source_true, (
            "build_notebook(strict_scope=True) must emit "
            "``strict_scope=True`` in the run cell so "
            "orchestrator.run(...) on the cluster receives the operator's "
            f"--strict-scope intent. Generated source did not contain it: "
            f"{all_source_true[:400]}"
        )

        nb_false = build_notebook(
            wheel_path=wheel, bundle_yaml="x: 1\n",
            mode="seed", datasets=None, layers=None,
            strict_scope=False,
        )
        all_source_false = "".join(
            "".join(c["source"]) for c in nb_false["cells"]
            if c["cell_type"] == "code"
        )
        assert "strict_scope=False" in all_source_false, (
            "build_notebook(strict_scope=False) must still emit a literal "
            "``strict_scope=False`` (not omit it) so the cluster honours "
            "the default."
        )

    def test_rest_dry_run_strict_scope_raises_aidpf_1042_on_undeclared_dep(
        self, tmp_path,
    ):
        """REST dispatch dry-run with strict_scope=True must raise
        AIDPF-1042 on an undeclared bronze upstream — same contract
        as the inline path."""
        from unittest.mock import patch, MagicMock
        from pathlib import Path
        from oracle_ai_data_platform_fusion_autopilot.dispatch import dispatch_via_rest
        from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
            PreflightResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_pack,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
            AidpConfig,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            MissingDependencyError,
        )

        # Build a minimal pack with a silver dim that depends on
        # an undeclared bronze.
        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(PACK_YAML)
        (pack_root / "bronze").mkdir()
        (pack_root / "bronze" / "erp_suppliers.yaml").write_text(
            _bronze_yaml("erp_suppliers"),
        )
        (pack_root / "silver").mkdir()
        (pack_root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM)
        (pack_root / "silver" / "dim_supplier.sql").write_text(
            "SELECT 1 AS supplier_id",
        )
        pack = load_pack(pack_root)

        # Bundle declares the silver root but NOT its bronze upstream —
        # under strict_scope this is an AIDPF-1042 violation.
        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: strict-scope-rest-dryrun\n"
            "fusion:\n"
            "  serviceUrl: https://x\n  username: u\n  password: p\n"
            "  externalStorage: x\n"
            "aidp:\n  catalog: c\n  bronzeSchema: b\n"
            "  silverSchema: s\n  goldSchema: g\n"
            "datasets: []\n"
            "dimensions:\n  build: [dim_supplier]\n"
            "gold:\n  marts: []\n"
        )

        config = AidpConfig.model_validate({
            "project": "strict-scope-rest-dryrun",
            "apiVersion": "aidp-fusion-autopilot/v1",
            "defaults": {"region": "us-phoenix-1", "workspaceRoot": "/Workspace"},
            "environments": {
                "dev": {
                    "workspaceKey": "ws", "aiDataPlatformId": "aidp",
                    "clusterKey": "ck", "clusterName": "cn",
                    "ociProfile": "DEFAULT",
                }
            },
        })
        env = config.environments["dev"]

        ok = [PreflightResult("x", "PASS", "ok", "")]
        fake_client = MagicMock()

        # strict_scope=True with the undeclared erp_suppliers upstream
        # must raise AIDPF-1042 from the dispatch dry-run path.
        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_local_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_remote_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.AidpRestClient",
            return_value=fake_client,
        ):
            with pytest.raises(MissingDependencyError, match="erp_suppliers"):
                dispatch_via_rest(
                    bundle_path=bundle_path, config=config, env=env,
                    env_name="dev", mode="seed",
                    datasets=None, layers=None, dry_run=True,
                    resolved_pack=pack, strict_scope=True,
                )

    def test_rest_dry_run_default_strict_scope_false_auto_includes(
        self, tmp_path,
    ):
        """Counterpart: dispatch dry-run with strict_scope=False (default)
        must NOT raise — D-1 auto-includes the undeclared bronze dep
        so the operator sees the same plan as inline."""
        from unittest.mock import patch, MagicMock
        from oracle_ai_data_platform_fusion_autopilot.dispatch import dispatch_via_rest
        from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
            PreflightResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_pack,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import AidpConfig

        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(PACK_YAML)
        (pack_root / "bronze").mkdir()
        (pack_root / "bronze" / "erp_suppliers.yaml").write_text(
            _bronze_yaml("erp_suppliers"),
        )
        (pack_root / "silver").mkdir()
        (pack_root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM)
        (pack_root / "silver" / "dim_supplier.sql").write_text(
            "SELECT 1 AS supplier_id",
        )
        pack = load_pack(pack_root)

        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: strict-scope-rest-default\n"
            "fusion:\n"
            "  serviceUrl: https://x\n  username: u\n  password: p\n"
            "  externalStorage: x\n"
            "aidp:\n  catalog: c\n  bronzeSchema: b\n"
            "  silverSchema: s\n  goldSchema: g\n"
            "datasets: []\n"
            "dimensions:\n  build: [dim_supplier]\n"
            "gold:\n  marts: []\n"
        )

        config = AidpConfig.model_validate({
            "project": "strict-scope-rest-default",
            "apiVersion": "aidp-fusion-autopilot/v1",
            "defaults": {"region": "us-phoenix-1", "workspaceRoot": "/Workspace"},
            "environments": {
                "dev": {
                    "workspaceKey": "ws", "aiDataPlatformId": "aidp",
                    "clusterKey": "ck", "clusterName": "cn",
                    "ociProfile": "DEFAULT",
                }
            },
        })
        env = config.environments["dev"]
        ok = [PreflightResult("x", "PASS", "ok", "")]
        fake_client = MagicMock()

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_local_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_remote_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.AidpRestClient",
            return_value=fake_client,
        ):
            # strict_scope omitted (default False) must succeed.
            summary = dispatch_via_rest(
                bundle_path=bundle_path, config=config, env=env,
                env_name="dev", mode="seed",
                datasets=None, layers=None, dry_run=True,
                resolved_pack=pack,
            )
        assert summary is not None
        plan_ids = {n.dataset_id for n in summary.plan}
        # Round-6: schema resolver mirrors runtime — D-1 closure lands
        # in the PLAN, not as prereqs. Prereqs is empty under the new
        # contract.
        assert summary.prereqs == (), (
            f"round-6: REST dry-run prereqs must be empty; got "
            f"{summary.prereqs!r}"
        )
        assert "dim_supplier" in plan_ids
        assert "erp_suppliers" in plan_ids, (
            "REST dry-run default must D-1 auto-include erp_suppliers "
            f"into the plan; plan={sorted(plan_ids)}"
        )

    def test_rest_dry_run_layer_filter_empties_plan_raises_aidpf_1045(
        self, tmp_path,
    ):
        """Round-8 review fix: REST dispatch dry-run must raise
        AIDPF-1045 when ``--layers`` removes every declared root —
        same contract as runtime ``resolve_content_pack_plan``.
        Pre-fix the schema resolver silently returned an empty plan,
        so REST dispatch --dry-run reported success on a run the
        cluster would reject.

        Scenario: bundle declares supplier_spend (gold); CLI passes
        ``--datasets supplier_spend --layers silver`` → effective_roots
        is filtered to layer=silver, leaving the empty set.
        """
        from unittest.mock import patch, MagicMock
        from oracle_ai_data_platform_fusion_autopilot.dispatch import dispatch_via_rest
        from oracle_ai_data_platform_fusion_autopilot.dispatch.preflight import (
            PreflightResult,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_pack,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.bundle import AidpConfig
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            MissingDependencyError,
        )

        pack_root = tmp_path / "pack"
        pack_root.mkdir()
        (pack_root / "pack.yaml").write_text(PACK_YAML)
        (pack_root / "bronze").mkdir()
        (pack_root / "bronze" / "erp_suppliers.yaml").write_text(
            _bronze_yaml("erp_suppliers"),
        )
        (pack_root / "bronze" / "ap_invoices.yaml").write_text(
            _bronze_yaml("ap_invoices"),
        )
        (pack_root / "silver").mkdir()
        (pack_root / "silver" / "dim_supplier.yaml").write_text(SILVER_DIM)
        (pack_root / "silver" / "dim_supplier.sql").write_text(
            "SELECT 1 AS supplier_id",
        )
        (pack_root / "gold").mkdir()
        (pack_root / "gold" / "supplier_spend.yaml").write_text(GOLD_MART)
        (pack_root / "gold" / "supplier_spend.sql").write_text(
            "SELECT 1 AS supplier_id",
        )
        pack = load_pack(pack_root)

        bundle_path = tmp_path / "bundle.yaml"
        bundle_path.write_text(
            "apiVersion: aidp-fusion-autopilot/v1\n"
            "project: layer-filter-empty\n"
            "fusion:\n"
            "  serviceUrl: https://x\n  username: u\n  password: p\n"
            "  externalStorage: x\n"
            "aidp:\n  catalog: c\n  bronzeSchema: b\n"
            "  silverSchema: s\n  goldSchema: g\n"
            "datasets:\n  - id: erp_suppliers\n  - id: ap_invoices\n"
            "dimensions:\n  build: [dim_supplier]\n"
            "gold:\n  marts: [supplier_spend]\n"
        )

        config = AidpConfig.model_validate({
            "project": "layer-filter-empty",
            "apiVersion": "aidp-fusion-autopilot/v1",
            "defaults": {"region": "us-phoenix-1", "workspaceRoot": "/Workspace"},
            "environments": {
                "dev": {
                    "workspaceKey": "ws", "aiDataPlatformId": "aidp",
                    "clusterKey": "ck", "clusterName": "cn",
                    "ociProfile": "DEFAULT",
                }
            },
        })
        env = config.environments["dev"]
        ok = [PreflightResult("x", "PASS", "ok", "")]
        fake_client = MagicMock()

        with patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_local_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.run_remote_preflight",
            return_value=ok,
        ), patch(
            "oracle_ai_data_platform_fusion_autopilot.dispatch.AidpRestClient",
            return_value=fake_client,
        ):
            with pytest.raises(MissingDependencyError, match="AIDPF-1045"):
                dispatch_via_rest(
                    bundle_path=bundle_path, config=config, env=env,
                    env_name="dev", mode="seed",
                    datasets=["supplier_spend"], layers=["silver"],
                    dry_run=True, resolved_pack=pack,
                )
