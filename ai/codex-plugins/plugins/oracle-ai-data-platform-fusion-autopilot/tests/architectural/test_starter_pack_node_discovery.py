"""Architectural test — every starter-pack node loads cleanly + has a
valid ``implementation.type`` (Phase 5 Step 7).

This is the discovery-side regression guard. The dispatch-side
regression guards live in the per-implementation-type tests
(``test_sql_runner``, ``test_sql_runner_builtin_dispatch``,
``test_bronze_extract_adapter`` once added).

If a future YAML edit drops a required field or introduces an unknown
``implementation.type``, this test catches it before the dispatcher
sees the malformed pack.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _resolve_node_from_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


STARTER_PACK_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


VALID_IMPL_TYPES = {"sql", "builtin", "bronze_extract"}


@pytest.fixture(scope="module")
def starter_pack():
    """Load the shipped fusion-finance-starter pack once per module.

    Skips the entire module if the pack root is missing — protects
    against accidental layout shifts during local dev.
    """
    if not STARTER_PACK_ROOT.is_dir():
        pytest.skip(f"starter pack root missing: {STARTER_PACK_ROOT}")
    return load_pack(STARTER_PACK_ROOT)


class TestStarterPackLoads:
    def test_pack_loads_without_error(self, starter_pack) -> None:
        assert starter_pack is not None
        # Pack carries non-trivial silver + gold node sets.
        assert len(starter_pack.silver) > 0, "starter pack has no silver nodes"
        assert len(starter_pack.gold) > 0, "starter pack has no gold nodes"

    def test_every_silver_node_has_valid_impl_type(self, starter_pack) -> None:
        for node_id, node in starter_pack.silver.items():
            assert node.implementation.type in VALID_IMPL_TYPES, (
                f"silver/{node_id} has unknown implementation.type="
                f"{node.implementation.type!r}; expected one of "
                f"{VALID_IMPL_TYPES!r}."
            )

    def test_every_gold_node_has_valid_impl_type(self, starter_pack) -> None:
        for node_id, node in starter_pack.gold.items():
            assert node.implementation.type in VALID_IMPL_TYPES, (
                f"gold/{node_id} has unknown implementation.type="
                f"{node.implementation.type!r}; expected one of "
                f"{VALID_IMPL_TYPES!r}."
            )


class TestStarterPackDiscoveryViaHelper:
    def test_resolve_node_for_every_silver_id(self, starter_pack) -> None:
        for node_id in starter_pack.silver:
            node = _resolve_node_from_pack(starter_pack, "silver", node_id)
            assert node.id == node_id
            assert node.layer == "silver"

    def test_resolve_node_for_every_gold_id(self, starter_pack) -> None:
        for node_id in starter_pack.gold:
            node = _resolve_node_from_pack(starter_pack, "gold", node_id)
            assert node.id == node_id
            assert node.layer == "gold"

    def test_starter_pack_uses_sql_or_builtin_only_for_silver_gold(
        self, starter_pack,
    ) -> None:
        """The SHIPPED starter pack uses sql / builtin for silver+gold.
        Bronze uses bronze_extract (Phase 9). If a future starter-pack
        edit introduces an unexpected impl type, this test fails so the
        architectural decision gets explicit review.
        """
        for node_id, node in starter_pack.silver.items():
            assert node.implementation.type in {"sql", "builtin"}, (
                f"silver/{node_id} declares unexpected "
                f"implementation.type={node.implementation.type!r} "
                f"in the shipped starter pack."
            )
        for node_id, node in starter_pack.gold.items():
            assert node.implementation.type in {"sql", "builtin"}, (
                f"gold/{node_id} declares unexpected "
                f"implementation.type={node.implementation.type!r} "
                f"in the shipped starter pack."
            )

    def test_every_bronze_node_has_bronze_extract_impl(self, starter_pack) -> None:
        """Phase 9: starter pack bronze nodes ship as bronze_extract."""
        assert len(starter_pack.bronze) > 0, (
            "starter pack must declare bronze nodes (Phase 9)"
        )
        for node_id, node in starter_pack.bronze.items():
            assert node.implementation.type == "bronze_extract", (
                f"bronze/{node_id} must be type=bronze_extract; got "
                f"{node.implementation.type!r}"
            )
