"""Unit tests for P1.5α-fix21 ``orchestrator.resume.render_drift_error``.

The drift renderer is invoked by the orchestrator's resume flow when
the current plan hash diverges from the stored hash. These tests
exercise the renderer directly (no spark, no fixtures) so the
three-section contract is pinned independent of the resume flow's
plumbing.

Sections, in order:
  1. Identity diff (rendered first) — one line per changed identity
     field. Catches the high-frequency case (bumped schema, switched
     principal, upgraded plugin) the operator needs to see before
     anything else.
  2. Dataset diff — added / removed dataset_ids + per-dataset deltas
     for nodes present on both sides with diverging
     ``(layer, mode, effective_schema)``.
  3. Hash echo — truncated stored vs current hashes.
"""

from __future__ import annotations

import json

from oracle_ai_data_platform_fusion_autopilot.orchestrator.resume import (
    render_drift_error,
)


def _make_stored_snapshot(
    *,
    identity_overrides: dict[str, str] | None = None,
    nodes: list[dict[str, str]] | None = None,
) -> str:
    """Build a stored-snapshot JSON with sane defaults."""
    base_identity = {
        "fusion.serviceUrl": "https://pod-a.example.com",
        "fusion.externalStorage": "oci://bucket-a@ns/path",
        "fusion.username": "alice@oracle",
        "aidp.catalog": "fusion_catalog",
        "aidp.bronzeSchema": "bronze",
        "aidp.silverSchema": "silver_v1",
        "aidp.goldSchema": "gold",
        "plugin_version": "0.1.0a0",
    }
    if identity_overrides:
        base_identity.update(identity_overrides)
    base_nodes = nodes if nodes is not None else [
        {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
        {"dataset_id": "dim_supplier", "layer": "silver", "mode": "seed", "effective_schema": ""},
    ]
    return json.dumps({"identity": base_identity, "nodes": base_nodes})


# ---------------------------------------------------------------------------
# Identity diff section
# ---------------------------------------------------------------------------


def test_identity_diff_renders_before_dataset_diff_silver_schema_case() -> None:
    """The high-frequency case: customer bumped ``aidp.silverSchema``
    between runs."""
    stored = _make_stored_snapshot(identity_overrides={"aidp.silverSchema": "silver_v1"})
    current_identity = {
        "fusion.serviceUrl": "https://pod-a.example.com",
        "fusion.externalStorage": "oci://bucket-a@ns/path",
        "fusion.username": "alice@oracle",
        "aidp.catalog": "fusion_catalog",
        "aidp.bronzeSchema": "bronze",
        "aidp.silverSchema": "silver_v2",  # ← drifted
        "aidp.goldSchema": "gold",
        "plugin_version": "0.1.0a0",
    }
    current_nodes = json.loads(stored)["nodes"]  # same plan shape
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=current_identity,
        current_node_tuples=current_nodes,
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-A",
    )
    # Identity changes section present + names the field.
    assert "Identity changes:" in msg
    assert "aidp.silverSchema" in msg
    assert "'silver_v1'" in msg and "'silver_v2'" in msg
    # Identity changes appear before any dataset-changes section.
    if "Dataset changes:" in msg:
        assert msg.index("Identity changes:") < msg.index("Dataset changes:")
    # No false dataset-diff lines when nothing diverged dataset-side.
    assert "added:" not in msg
    assert "removed:" not in msg


def test_identity_diff_renders_username_case() -> None:
    """Mixed-authorization guard rendering — principal swap shown
    explicitly so the operator catches the cross-tenant scenario."""
    stored = _make_stored_snapshot(identity_overrides={"fusion.username": "alice@oracle"})
    current_identity = json.loads(stored)["identity"].copy()
    current_identity["fusion.username"] = "bob@oracle"
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=current_identity,
        current_node_tuples=json.loads(stored)["nodes"],
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-B",
    )
    assert "fusion.username" in msg
    assert "'alice@oracle'" in msg
    assert "'bob@oracle'" in msg


# ---------------------------------------------------------------------------
# Dataset diff section
# ---------------------------------------------------------------------------


def test_dataset_diff_renders_added_and_removed() -> None:
    stored = _make_stored_snapshot(nodes=[
        {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
        {"dataset_id": "removed_node", "layer": "bronze", "mode": "seed", "effective_schema": "X"},
    ])
    current_identity = json.loads(stored)["identity"]
    current_nodes = [
        {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
        {"dataset_id": "added_node", "layer": "bronze", "mode": "seed", "effective_schema": "Y"},
    ]
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=current_identity,
        current_node_tuples=current_nodes,
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-C",
    )
    assert "Dataset changes:" in msg
    assert "added:" in msg
    assert "'added_node'" in msg
    assert "removed:" in msg
    assert "'removed_node'" in msg


def test_dataset_diff_renders_per_dataset_delta() -> None:
    """Node present on both sides with diverging effective_schema →
    per-dataset delta line."""
    stored = _make_stored_snapshot(nodes=[
        {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "Financial"},
    ])
    current_nodes = [
        {"dataset_id": "ap_invoices", "layer": "bronze", "mode": "seed", "effective_schema": "FinancialV2"},
    ]
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=json.loads(stored)["identity"],
        current_node_tuples=current_nodes,
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-D",
    )
    assert "per-dataset deltas:" in msg
    assert "ap_invoices" in msg
    assert "effective_schema" in msg
    assert "'Financial'" in msg
    assert "'FinancialV2'" in msg


# ---------------------------------------------------------------------------
# Hash echo + structural invariants
# ---------------------------------------------------------------------------


def test_hash_echo_truncates_for_readability() -> None:
    stored = _make_stored_snapshot()
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=json.loads(stored)["identity"],
        current_node_tuples=json.loads(stored)["nodes"],
        stored_hash="abc" + "1" * 61,  # length 64
        current_hash="def" + "2" * 61,
        run_id="run-E",
    )
    # Truncated 12-char prefixes appear.
    assert "abc111111111" in msg
    assert "def222222222" in msg


def test_run_id_appears_in_error_message() -> None:
    stored = _make_stored_snapshot()
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=json.loads(stored)["identity"],
        current_node_tuples=json.loads(stored)["nodes"],
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="abc-123-def",
    )
    assert "abc-123-def" in msg


def test_renderer_handles_empty_identity_diff_gracefully() -> None:
    """No identity drift + plan-shape drift → no Identity changes
    section, dataset changes only. Catches a regression that would
    print 'Identity changes:' even when nothing changed."""
    stored = _make_stored_snapshot()
    current_identity = json.loads(stored)["identity"]  # identical
    current_nodes = [
        {"dataset_id": "totally_different", "layer": "gold", "mode": "seed", "effective_schema": ""},
    ]
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=current_identity,
        current_node_tuples=current_nodes,
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-F",
    )
    assert "Identity changes:" not in msg
    assert "Dataset changes:" in msg


def test_renderer_still_prints_hashes_when_no_visible_diff() -> None:
    """Defensive: hash mismatch with empty identity + dataset diff
    (canonical-payload bug). Renderer still echoes the hashes so the
    operator has something to file a bug with."""
    stored = _make_stored_snapshot()
    msg = render_drift_error(
        stored_snapshot_json=stored,
        current_identity=json.loads(stored)["identity"],
        current_node_tuples=json.loads(stored)["nodes"],
        stored_hash="a" * 64,
        current_hash="b" * 64,
        run_id="run-G",
    )
    # No diff sections rendered.
    assert "Identity changes:" not in msg
    assert "Dataset changes:" not in msg
    # But hashes still print.
    assert "aaaa" in msg and "bbbb" in msg
