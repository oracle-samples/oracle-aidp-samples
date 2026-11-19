"""Regression for AIDPF-1040: overlay packs must stage their inherited base-pack
nodes, i.e. ``_resolve_chain_roots`` must include every distinct source root
(not just the overlay's own root) when ``chain_roots`` is absent.

Before the fix, ``load_full_chain`` never populated ``chain_roots`` so staging
fell back to ``(resolved_pack.root,)`` — the overlay root only — and any
inherited node raised AIDPF-1040 at seed time. No overlay could be seeded.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging import (  # noqa: E402
    _resolve_chain_roots,
)

BASE = Path("/packs/fusion-finance-starter")
OVERLAY = Path("/work/overlays/fusion-finance-ar-ext")


def test_chain_roots_explicit_wins():
    pack = SimpleNamespace(chain_roots=(BASE, OVERLAY), root=OVERLAY, source_roots={})
    assert _resolve_chain_roots(pack) == (BASE, OVERLAY)


def test_chain_roots_derived_from_source_roots_base_first():
    # merge_overlay seeds source_roots base-first, then adds overlay nodes.
    source_roots = {
        "silver/dim_account": BASE,
        "silver/dim_supplier": BASE,
        "gold/gl_balance": BASE,
        "gold/ar_invoice_summary": OVERLAY,  # the overlay's new node
    }
    pack = SimpleNamespace(chain_roots=None, root=OVERLAY, source_roots=source_roots)
    result = _resolve_chain_roots(pack)
    # Both roots present, base first, overlay last (entry_layer_index contract).
    assert result == (BASE, OVERLAY)
    assert result[-1] == OVERLAY


def test_chain_roots_distinct_preserves_first_seen_order():
    source_roots = {"a": BASE, "b": BASE, "c": OVERLAY, "d": BASE}
    pack = SimpleNamespace(chain_roots=None, root=OVERLAY, source_roots=source_roots)
    assert _resolve_chain_roots(pack) == (BASE, OVERLAY)


def test_chain_roots_single_pack_falls_back_to_root():
    # Non-overlay pack: no chain_roots, no source_roots -> just its root.
    pack = SimpleNamespace(chain_roots=None, root=BASE, source_roots={})
    assert _resolve_chain_roots(pack) == (BASE,)
