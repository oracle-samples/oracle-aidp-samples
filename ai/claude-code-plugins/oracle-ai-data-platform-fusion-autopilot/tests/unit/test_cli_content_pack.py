"""CLI integration tests for `aidp-fusion-autopilot content-pack {list,info,validate}`.

Tests invoke the CLI via subprocess so the end-to-end click → command →
loader/validator chain is exercised. Run with the editable-install Python
interpreter so the installed entry point is found.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Run `aidp-fusion-autopilot <args>` via the current Python's module entry."""
    cmd = [
        sys.executable,
        "-m",
        "oracle_ai_data_platform_fusion_autopilot.cli",
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PLUGIN_ROOT)


def test_cli_list_human_readable() -> None:
    result = _run_cli("content-pack", "list")
    assert result.returncode == 0, result.stderr
    # Starter pack should appear.
    assert "fusion-finance-starter" in result.stdout
    assert "0.1.0" in result.stdout


def test_cli_list_json() -> None:
    result = _run_cli("content-pack", "list", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    packs = {p["name"]: p for p in data["packs"]}
    assert "fusion-finance-starter" in packs
    assert packs["fusion-finance-starter"]["version"] == "0.1.0"


def test_cli_info_human_readable() -> None:
    result = _run_cli("content-pack", "info", "fusion-finance-starter")
    assert result.returncode == 0, result.stderr
    assert "fusion-finance-starter" in result.stdout
    assert "3 silver, 3 gold" in result.stdout


def test_cli_info_json() -> None:
    result = _run_cli("content-pack", "info", "fusion-finance-starter", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["id"] == "fusion-finance-starter"
    assert data["version"] == "0.1.0"
    assert set(data["nodes"]["silver"]) == {"dim_supplier", "dim_account", "dim_calendar"}
    assert set(data["nodes"]["gold"]) == {"gl_balance", "supplier_spend", "ap_aging"}
    assert set(data["dashboards"]) == {"executive_cfo", "payables"}
    assert len(data["pack_hash"]) == 64  # sha256 hex


def test_cli_info_unknown_pack_exits_1() -> None:
    result = _run_cli("content-pack", "info", "nonexistent-pack")
    assert result.returncode == 1


def test_cli_validate_starter_pack_passes() -> None:
    result = _run_cli("content-pack", "validate", "fusion-finance-starter")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "validates clean" in result.stdout


def test_cli_validate_starter_pack_json() -> None:
    result = _run_cli("content-pack", "validate", "fusion-finance-starter", "--json")
    assert result.returncode == 0, result.stderr
    data = json.loads(result.stdout)
    assert data["pack"] == "fusion-finance-starter"
    assert data["ok"] is True
    assert data["errors"] == []


def test_cli_validate_bad_semver_pack_exits_2(tmp_path: Path) -> None:
    """A syntactically broken pack (bad SemVer) → exit 2 + AIDPF-2002 in JSON."""
    import yaml

    bad_path = tmp_path / "bad"
    bad_path.mkdir()
    (bad_path / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "bad-pack",
                "version": "not-semver",
                "compatibility": {"pluginMinVersion": "0.3.0"},
            }
        )
    )
    result = _run_cli("content-pack", "validate", str(bad_path), "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    data = json.loads(result.stdout)
    assert data["errors"], "expected at least one validation error"
    assert any("AIDPF-2002" in e.get("message", "") for e in data["errors"])


def test_cli_validate_broken_overlay_surfaces_orphan_override(tmp_path: Path) -> None:
    """A broken overlay (orphan override) validated via the CLI → exit 2 + AIDPF-2001.

    Regression test for Finding 2 — previously the CLI called `load_pack` only,
    which does NOT resolve `extends:` chains. As a result, orphan-override
    failures from `merge_overlay` never surfaced. The fix calls
    `resolve_overlay_chain` + `merge_overlay` before `validate_pack_full`.
    """
    import yaml

    # Sibling base pack — the CLI's base resolver looks for siblings first.
    base_root = tmp_path / "sibling-base"
    base_root.mkdir()
    (base_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "sibling-base",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            }
        )
    )

    overlay_root = tmp_path / "broken-overlay"
    overlay_root.mkdir()
    (overlay_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "broken-overlay",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
                "extends": "sibling-base@0.1.0",
                # The base has no silver/dim_nonexistent — orphan override.
                "overrides": {"silver/dim_nonexistent": {"profile": "finance-default"}},
            }
        )
    )

    result = _run_cli("content-pack", "validate", str(overlay_root), "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    data = json.loads(result.stdout)
    assert data["errors"], "expected at least one validation error"
    assert any(
        "AIDPF-2001" in e.get("message", "") for e in data["errors"]
    ), f"expected AIDPF-2001 in errors, got: {data['errors']!r}"


def test_cli_validate_overlay_wrong_base_version_surfaces_AIDPF_2004(tmp_path: Path) -> None:
    """Overlay declares extends: name@9.9.9 but sibling pack is 0.1.0 → AIDPF-2004.

    Regression test for Finding 4 — the CLI's base resolver finds bases by
    name (sibling directory or installed packs dir) but does not itself
    verify the version. ``resolve_overlay_chain`` enforces the version
    invariant centrally and raises ``ExtendsVersionMismatchError``.
    """
    import yaml

    # Sibling base whose actual version is 0.1.0.
    base_root = tmp_path / "sibling-base"
    base_root.mkdir()
    (base_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "sibling-base",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            }
        )
    )

    # Overlay declares extends: sibling-base@9.9.9 — version mismatch.
    overlay_root = tmp_path / "overlay-wrong-version"
    overlay_root.mkdir()
    (overlay_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "overlay-wrong-version",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
                "extends": "sibling-base@9.9.9",
            }
        )
    )

    result = _run_cli("content-pack", "validate", str(overlay_root), "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    data = json.loads(result.stdout)
    assert data["errors"], "expected at least one validation error"
    assert any(
        "AIDPF-2004" in e.get("message", "") for e in data["errors"]
    ), f"expected AIDPF-2004 in errors, got: {data['errors']!r}"


def test_cli_validate_valid_overlay_exits_0(tmp_path: Path) -> None:
    """A valid overlay (no orphan override, inherits base cleanly) → exit 0."""
    import yaml

    base_root = tmp_path / "sibling-base"
    base_root.mkdir()
    (base_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "sibling-base",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
            }
        )
    )

    overlay_root = tmp_path / "good-overlay"
    overlay_root.mkdir()
    (overlay_root / "pack.yaml").write_text(
        yaml.safe_dump(
            {
                "id": "good-overlay",
                "version": "0.1.0",
                "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
                "extends": "sibling-base@0.1.0",
            }
        )
    )

    result = _run_cli("content-pack", "validate", str(overlay_root), "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    data = json.loads(result.stdout)
    assert data["ok"] is True
    assert data["errors"] == []


# ---------------------------------------------------------------------------
# refresh-fork + replaceNode (guarded same-id full replacement)
# ---------------------------------------------------------------------------

import copy  # noqa: E402

import yaml as _yaml  # noqa: E402

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (  # noqa: E402
    load_pack,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.sql_renderer import (  # noqa: E402
    compute_contract_fingerprint,
    compute_fork_fingerprint,
)

_RF_BRONZE = {
    "id": "erp_suppliers",
    "layer": "bronze",
    "implementation": {
        "type": "bronze_extract",
        "datastore": "Fscm.X.SupplierExtractPVO",
        "biccSchema": "Financial",
        "incrementalCapable": True,
        "auditColumnsMode": "bronze_v1",
    },
    "target": "erp_suppliers",
    "dependsOn": {"bronze": [], "silver": []},
    "refresh": {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "watermark": {"source": "erp_suppliers", "column": "LASTUPDATEDATE"},
            "naturalKey": ["SEGMENT1"],
        },
    },
    "requiredColumns": {"erp_suppliers": ["SEGMENT1"]},
    "outputSchema": {
        "columns": [
            {"name": "SEGMENT1", "type": "string", "nullable": True, "pii": "low"},
            {"name": "_extract_ts", "type": "timestamp", "nullable": False, "pii": "none"},
            {"name": "_run_id", "type": "string", "nullable": False, "pii": "none"},
        ]
    },
    "quality": {"tests": []},
}

_RF_SILVER = {
    "id": "dim_supplier",
    "layer": "silver",
    "implementation": {"type": "sql", "sql": "silver/dim_supplier.sql"},
    "target": "dim_supplier",
    "dependsOn": {"bronze": [{"id": "erp_suppliers", "watermark": {"column": "_extract_ts"}}]},
    "refresh": {
        "seed": {"strategy": "replace"},
        "incremental": {
            "strategy": "merge",
            "watermark": {"source": "erp_suppliers", "column": "_extract_ts"},
            "naturalKey": ["supplier_key"],
        },
    },
    "requiredColumns": {"erp_suppliers": ["SEGMENT1"]},
    "outputSchema": {
        "columns": [{"name": "supplier_key", "type": "bigint", "nullable": False, "pii": "none"}]
    },
    "quality": {"tests": []},
}


def _w(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_yaml.safe_dump(data, sort_keys=False))


def _build_replace_fixture(tmp_path: Path, *, stamps: dict | None = None) -> tuple[Path, Path]:
    """Write a base + replaceNode overlay as siblings. Returns (base, overlay)."""
    base = tmp_path / "fusion-finance-starter"
    _w(base / "pack.yaml", {
        "id": "fusion-finance-starter", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
    })
    _w(base / "bronze" / "erp_suppliers.yaml", copy.deepcopy(_RF_BRONZE))
    _w(base / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")

    if stamps is None:
        bp = load_pack(base)
        n = bp.silver["dim_supplier"]
        stamps = {
            "sqlSha256": compute_fork_fingerprint(n, bp),
            "contractSha256": compute_contract_fingerprint(n),
            "packVersion": bp.pack.version,
        }

    overlay = tmp_path / "acme-finance"
    _w(overlay / "pack.yaml", {
        "id": "acme-finance", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
        "overrides": {
            "silver/dim_supplier": {"replaceNode": {"reason": "rewrite; T-1", "forkedFrom": stamps}}
        },
    })
    _w(overlay / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (overlay / "silver" / "dim_supplier.sql").write_text("SELECT 2 AS supplier_key\n")
    return base, overlay


def test_cli_refresh_fork_help() -> None:
    result = _run_cli("content-pack", "refresh-fork", "--help")
    assert result.returncode == 0, result.stderr
    assert "forkedFrom" in result.stdout
    # Help must name the current fork-base-drift code, not the stale/cluster one.
    assert "AIDPF-2064" in result.stdout
    assert "AIDPF-2047" not in result.stdout


def test_cli_refresh_fork_refuses_wrong_base_version(tmp_path: Path) -> None:
    """refresh-fork must NOT re-stamp from a base whose version != the overlay's
    `extends` ref (AIDPF-2004) — it would corrupt fork provenance."""
    # Base shipped at 0.2.0 ...
    base = tmp_path / "fusion-finance-starter"
    _w(base / "pack.yaml", {
        "id": "fusion-finance-starter", "version": "0.2.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
    })
    _w(base / "bronze" / "erp_suppliers.yaml", copy.deepcopy(_RF_BRONZE))
    _w(base / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")
    # ... but the overlay says it forked from 0.1.0 (stale placeholder stamps).
    overlay = tmp_path / "acme-finance"
    placeholder = {"sqlSha256": "x", "contractSha256": "y", "packVersion": "0.1.0"}
    _w(overlay / "pack.yaml", {
        "id": "acme-finance", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
        "overrides": {
            "silver/dim_supplier": {"replaceNode": {"reason": "x", "forkedFrom": placeholder}}
        },
    })
    _w(overlay / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (overlay / "silver" / "dim_supplier.sql").write_text("SELECT 2 AS supplier_key\n")
    before = (overlay / "pack.yaml").read_text()

    result = _run_cli("content-pack", "refresh-fork", str(overlay), "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    assert "AIDPF-2004" in [e["code"] for e in json.loads(result.stdout)["errors"]]
    # The leaf pack.yaml must be left untouched (no re-stamp from the wrong base).
    assert (overlay / "pack.yaml").read_text() == before


def test_cli_refresh_fork_multilevel_chain(tmp_path: Path) -> None:
    """leaf overlay -> parent OVERLAY -> starter: the version guard must validate
    the leaf's DIRECT parent (raw id/version), not the merged root identity."""
    # Root base.
    starter = tmp_path / "fusion-finance-starter"
    _w(starter / "pack.yaml", {
        "id": "fusion-finance-starter", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
    })
    _w(starter / "bronze" / "erp_suppliers.yaml", copy.deepcopy(_RF_BRONZE))
    _w(starter / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (starter / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key\n")
    # Mid overlay (no overrides) — the leaf's direct parent.
    tenant = tmp_path / "tenant-base"
    _w(tenant / "pack.yaml", {
        "id": "tenant-base", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "fusion-finance-starter@0.1.0",
    })
    # Leaf overlay extends the mid overlay, with a stale replaceNode stamp.
    leaf = tmp_path / "acme-customer"
    placeholder = {"sqlSha256": "stale", "contractSha256": "stale", "packVersion": "0.0.0"}
    _w(leaf / "pack.yaml", {
        "id": "acme-customer", "version": "0.1.0",
        "compatibility": {"pluginMinVersion": "0.3.0", "fusionFamilies": ["ERP"]},
        "extends": "tenant-base@0.1.0",
        "overrides": {
            "silver/dim_supplier": {"replaceNode": {"reason": "x", "forkedFrom": placeholder}}
        },
    })
    _w(leaf / "silver" / "dim_supplier.yaml", copy.deepcopy(_RF_SILVER))
    (leaf / "silver" / "dim_supplier.sql").write_text("SELECT 2 AS supplier_key\n")

    result = _run_cli("content-pack", "refresh-fork", str(leaf), "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    # forkedFrom must be re-stamped with real fingerprints from the parent chain.
    rew = _yaml.safe_load((leaf / "pack.yaml").read_text())
    ff = rew["overrides"]["silver/dim_supplier"]["replaceNode"]["forkedFrom"]
    assert len(ff["sqlSha256"]) == 64 and len(ff["contractSha256"]) == 64
    assert ff["packVersion"] == "0.1.0"


def test_cli_replace_node_validates_clean(tmp_path: Path) -> None:
    _, overlay = _build_replace_fixture(tmp_path)
    result = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["ok"] is True


def test_cli_sql_drift_then_refresh_recovers(tmp_path: Path) -> None:
    base, overlay = _build_replace_fixture(tmp_path)
    # Edit the base SQL → fork-base drift (logic variant).
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 1 AS supplier_key -- fix\n")
    drift = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert drift.returncode == 2
    codes = [e["code"] for e in json.loads(drift.stdout)["errors"]]
    assert "AIDPF-2064" in codes
    # refresh-fork re-stamps → validate clean.
    rf = _run_cli("content-pack", "refresh-fork", str(overlay))
    assert rf.returncode == 0, rf.stdout + rf.stderr
    ok = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert ok.returncode == 0, ok.stdout
    assert json.loads(ok.stdout)["ok"] is True


def test_cli_contract_drift_then_refresh_recovers(tmp_path: Path) -> None:
    base, overlay = _build_replace_fixture(tmp_path)
    # Edit ONLY the base YAML contract (PII) → contract-variant drift.
    node = copy.deepcopy(_RF_SILVER)
    node["outputSchema"]["columns"][0]["pii"] = "high"
    _w(base / "silver" / "dim_supplier.yaml", node)
    drift = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert drift.returncode == 2
    assert "AIDPF-2064" in [e["code"] for e in json.loads(drift.stdout)["errors"]]
    # refresh-fork must re-stamp the CONTRACT hash too (not just sql) → recovers.
    rf = _run_cli("content-pack", "refresh-fork", str(overlay))
    assert rf.returncode == 0, rf.stdout + rf.stderr
    ok = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert ok.returncode == 0, ok.stdout


def test_cli_refresh_fork_idempotent(tmp_path: Path) -> None:
    base, overlay = _build_replace_fixture(tmp_path)
    (base / "silver" / "dim_supplier.sql").write_text("SELECT 3 AS supplier_key\n")
    first = _run_cli("content-pack", "refresh-fork", str(overlay), "--json")
    assert first.returncode == 0
    assert any(c["changed"] for c in json.loads(first.stdout)["refreshed"])
    second = _run_cli("content-pack", "refresh-fork", str(overlay), "--json")
    assert second.returncode == 0
    assert all(not c["changed"] for c in json.loads(second.stdout)["refreshed"])


def test_cli_identity_change_emits_2048(tmp_path: Path) -> None:
    base, overlay = _build_replace_fixture(tmp_path)
    # Change target in the overlay replacement → identity change.
    node = copy.deepcopy(_RF_SILVER)
    node["target"] = "dim_supplier_v2"
    _w(overlay / "silver" / "dim_supplier.yaml", node)
    result = _run_cli("content-pack", "validate", str(overlay), "--json")
    assert result.returncode == 2
    assert "AIDPF-2065" in [e["code"] for e in json.loads(result.stdout)["errors"]]
