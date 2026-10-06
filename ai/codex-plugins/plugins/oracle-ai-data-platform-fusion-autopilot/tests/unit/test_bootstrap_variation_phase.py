"""Integration tests for the bootstrap variation phase
(:mod:`oracle_ai_data_platform_fusion_autopilot.commands.variation_phase`).

Drives the full pipeline (probe → walk → write profile + evidence) with
an injected mock Spark and asserts:

* Happy path: profile + evidence files produced; resolutions match the
  starter-pack expectations.
* AIDPF-1020: missing operator identity → exit 1 + identity diagnostic
  written; no profile or evidence.
* AIDPF-2010 aggregation: two unresolved required columnAliases →
  two distinct diagnostic files; no profile or evidence.
* Multi-match with ``--non-interactive``: auto-picks first candidate
  deterministically.
* Multi-match with ``--resolutions`` JSON: scripted choice flows through.
* Workdir anchor: artifacts land relative to ``bundle.yaml`` parent,
  not ``cwd``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from oracle_ai_data_platform_fusion_autopilot.commands.variation_phase import (
    VariationPhaseOptions,
    run_variation_phase,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import Bundle


REPO_ROOT = Path(__file__).resolve().parents[2]
PACK_ROOT = (
    REPO_ROOT
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


DEMO_POD_BRONZE: dict[str, list[str]] = {
    "erp_suppliers": ["VENDORID", "SEGMENT1"],
    "ap_invoices": [
        "ApInvoicesInvoiceCurrencyCode",
        "ApInvoicesCurrencyCode",  # → MultiMatch on invoice_currency_code
        "ApInvoicesCancelledDate",
    ],
    "gl_coa": [
        "CodeCombinationSegment1",
        "CodeCombinationSegment2",
        "CodeCombinationSegment3",
    ],
    "gl_period_balances": ["PeriodNetCredit"],
}


def _row(col_name: str, data_type: str = "string"):
    return {"col_name": col_name, "data_type": data_type, "comment": None}


def _mock_spark(per_table_columns: dict[str, list[str]]) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        dataset = target.split(".")[-1]
        cols = per_table_columns.get(dataset, [])
        df = MagicMock(name=f"df_{dataset}")
        df.collect.return_value = [_row(c) for c in cols]
        return df

    spark.sql.side_effect = _sql
    return spark


@pytest.fixture
def bundle_dir(tmp_path: Path) -> Path:
    """Create a tmp bundle dir with bundle.yaml referencing the starter pack."""
    bundle_yaml = tmp_path / "bundle.yaml"
    bundle_yaml.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "aidp-fusion-autopilot/v1",
                "version": "0.2.0",
                "project": "test",
                "fusion": {
                    "serviceUrl": "https://example.invalid",
                    "username": "stub",
                    "password": "stub",
                    "externalStorage": "stub",
                },
                "aidp": {
                    "catalog": "cat",
                    "bronzeSchema": "bronze",
                    "silverSchema": "silver",
                    "goldSchema": "gold",
                    "storageFormat": "delta",
                },
                "datasets": [
                    {"id": "erp_suppliers", "mode": "full"},
                    {"id": "ap_invoices", "mode": "incremental"},
                    {"id": "gl_coa", "mode": "full"},
                    {"id": "gl_period_balances", "mode": "full"},
                ],
                "contentPack": {
                    "name": "fusion-finance-starter",
                    "path": str(PACK_ROOT),
                    "profile": "finance-default",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return tmp_path


def _load_bundle(bundle_path: Path) -> Bundle:
    raw = yaml.safe_load(bundle_path.read_text(encoding="utf-8"))
    return Bundle.model_validate(raw)


# ---------------------------------------------------------------------------
# Happy path with ResolutionsInput
# ---------------------------------------------------------------------------


class TestHappyPathWithScriptedResolutions:
    def test_writes_profile_and_evidence_with_resolved_currency(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle_path = bundle_dir / "bundle.yaml"
        bundle = _load_bundle(bundle_path)

        resolutions_file = bundle_dir / "resolutions.json"
        resolutions_file.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "tenant": "finance-default",
                    "resolutions": [
                        {
                            "name": "invoice_currency_code",
                            "kind": "columnAliases",
                            "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        outcome = run_variation_phase(
            bundle,
            bundle_path,
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                resolutions_path=resolutions_file,
            ),
        )
        assert outcome.exit_code == 0
        assert outcome.profile_path == bundle_dir / "profiles" / "finance-default.yaml"
        assert outcome.profile_path.exists()
        assert outcome.evidence_path is not None
        assert outcome.evidence_path.parent == bundle_dir / "evidence" / "finance-default"

        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Every variation point resolved to the demo-pod conventional value.
        assert profile["resolved"]["column"] == {
            "supplier_natural_key": "SEGMENT1",
            "vendor_id": "VENDORID",
            "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode",
            "coa_balancing_segment": "CodeCombinationSegment1",
            "coa_cost_center_segment": "CodeCombinationSegment2",
            "coa_natural_account_segment": "CodeCombinationSegment3",
        }
        assert profile["resolved"]["semantic"] == {"cancelled_status": "cancelled_date"}
        # Approval metadata recorded.
        approval = profile["provenance"]["approvedBy"]
        assert approval["operator"] == "alice@oracle.com"
        # Mechanism precedence: cli_flag (scripted) wins over auto_resolve.
        assert approval["mechanism"] == "cli_flag"


# ---------------------------------------------------------------------------
# AIDPF-1020 — missing operator
# ---------------------------------------------------------------------------


class TestAidpf1020IdentityGate:
    def test_missing_identity_writes_1020_artifact(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("AIDP_OPERATOR", raising=False)
        monkeypatch.delenv("USER", raising=False)
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 1
        assert len(outcome.diagnostic_paths) == 1
        assert outcome.diagnostic_paths[0].name == "AIDPF-1020.json"
        # No profile / evidence on identity-gate failure.
        assert outcome.profile_path is None
        assert outcome.evidence_path is None


# ---------------------------------------------------------------------------
# AIDPF-2010 aggregation — multiple unresolved required columnAliases
# ---------------------------------------------------------------------------


class TestAidpf2010Aggregation:
    def test_two_unresolved_columnaliases_write_two_artifacts(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        # Drop VENDORID + ApInvoicesInvoiceCurrencyCode → 2 NoMatch.
        drifted = {
            "erp_suppliers": ["SEGMENT1"],
            "ap_invoices": ["UnrelatedCurrencyCol", "ApInvoicesCancelledDate"],
            "gl_coa": [
                "CodeCombinationSegment1",
                "CodeCombinationSegment2",
                "CodeCombinationSegment3",
            ],
            "gl_period_balances": [],
        }
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted),
            ),
        )
        assert outcome.exit_code == 1
        names = sorted(p.name for p in outcome.diagnostic_paths)
        assert names == [
            "AIDPF-2010__invoice_currency_code.json",
            "AIDPF-2010__vendor_id.json",
        ]
        # Profile + evidence MUST NOT be written when any required no-match fired.
        assert outcome.profile_path is None
        assert outcome.evidence_path is None


# ---------------------------------------------------------------------------
# --non-interactive multi-match auto-pick
# ---------------------------------------------------------------------------


class TestNonInteractiveMultiMatch:
    def test_auto_picks_first_candidate(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Priority order says ApInvoicesInvoiceCurrencyCode comes first.
        assert profile["resolved"]["column"]["invoice_currency_code"] == (
            "ApInvoicesInvoiceCurrencyCode"
        )
        # Mechanism: non_interactive (multi-match auto-picked).
        assert profile["provenance"]["approvedBy"]["mechanism"] == "non_interactive"


# ---------------------------------------------------------------------------
# Workdir anchor — artifacts land beside bundle.yaml, not cwd
# ---------------------------------------------------------------------------


class TestWorkdirAnchor:
    def test_writes_under_bundle_parent_not_cwd(
        self,
        bundle_dir: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")

        other_cwd = tmp_path_factory.mktemp("elsewhere")
        monkeypatch.chdir(other_cwd)

        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        # Artifacts under bundle_dir, NOT other_cwd.
        assert outcome.profile_path is not None
        assert outcome.profile_path.is_relative_to(bundle_dir)
        assert outcome.evidence_path.is_relative_to(bundle_dir)
        # And nothing accidentally written under cwd.
        assert not (other_cwd / "profiles").exists()
        assert not (other_cwd / "evidence").exists()


# ---------------------------------------------------------------------------
# Phase 3d — pinned bronze-schema snapshot + bootstrap --refresh back-fill
# ---------------------------------------------------------------------------


from datetime import datetime, timezone

from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
    ColumnInfo,
    compute_bronze_fingerprint,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_snapshot import (
    from_observed as snapshot_from_observed,
    load_bronze_schema_snapshot,
    resolve_snapshot_path,
    write_bronze_schema_snapshot,
)


def _bronze_observed_for_starter() -> dict[str, list[ColumnInfo]]:
    """Reproduce the demo-pod bronze observation the variation phase
    would see for the starter pack — used to compute the fingerprint
    that the simulated prior profile pinned."""
    return {
        dataset: [ColumnInfo(name=c, type="string") for c in cols]
        for dataset, cols in DEMO_POD_BRONZE.items()
    }


class TestPhase3dInitialPinWritesSnapshot:
    def test_initial_pin_writes_snapshot_at_expected_path(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        assert snapshot_path.exists()
        loaded = load_bronze_schema_snapshot(snapshot_path)
        # The snapshot's fingerprint must equal the value pinned on the
        # profile (single bootstrap-commit consistency).
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        assert loaded.bronze_schema_fingerprint == profile["bronzeSchemaFingerprint"]
        # No leftover .tmp file in profiles/.
        leftovers = [
            p
            for p in (bundle_dir / "profiles").iterdir()
            if p.name.endswith(".tmp")
        ]
        assert leftovers == []

    def test_refresh_with_drift_rewrites_snapshot(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        # Initial pin
        run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        # Refresh against a DIFFERENT bronze shape → full re-pin path.
        drifted_bronze: dict[str, list[str]] = {
            **DEMO_POD_BRONZE,
            "ap_invoices": [
                "ApInvoicesInvoiceCurrencyCode",
                "ApInvoicesCurrencyCode",
                "ApInvoicesCancelledDate",
                "ApInvoicesNewColumn",  # introduces drift
            ],
        }
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(drifted_bronze),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        loaded = load_bronze_schema_snapshot(snapshot_path)
        col_names = {
            c.name for d in loaded.datasets if d.dataset_id == "ap_invoices"
            for c in d.columns
        }
        assert "ApInvoicesNewColumn" in col_names


class TestPhase3dRefreshBackfill:
    """Step 3a — `--refresh` no-drift back-fills missing/desynced snapshots."""

    def _initial_pin_then_delete_snapshot(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        """Helper: do an initial pin, then delete the snapshot file +
        return the profile mtime so callers can confirm it's unchanged."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        snapshot_path.unlink()
        return outcome.profile_path

    def test_refresh_backfills_missing_snapshot(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        profile_path = self._initial_pin_then_delete_snapshot(
            bundle_dir, monkeypatch
        )
        evidence_dir = bundle_dir / "evidence" / "finance-default"
        evidence_files_before = sorted(p.name for p in evidence_dir.iterdir())
        profile_mtime_before = profile_path.stat().st_mtime_ns

        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        assert "back-filled" in outcome.summary
        # Snapshot back exists.
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        assert snapshot_path.exists()
        # Profile + evidence untouched.
        assert profile_path.stat().st_mtime_ns == profile_mtime_before
        assert (
            sorted(p.name for p in evidence_dir.iterdir())
            == evidence_files_before
        )

    def test_refresh_backfills_metadata_fingerprint_desync(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Snapshot file present, but its `bronzeSchemaFingerprint` field
        was hand-edited to a stale value (e.g. partial-failure history)."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        # Hand-edit the metadata fingerprint to something stale.
        raw = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
        raw["bronzeSchemaFingerprint"] = "sha256:" + "0" * 64
        snapshot_path.write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
        profile_mtime_before = outcome.profile_path.stat().st_mtime_ns

        outcome2 = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome2.exit_code == 0
        assert "back-filled" in outcome2.summary
        # Snapshot rewritten with the live fingerprint.
        loaded = load_bronze_schema_snapshot(snapshot_path)
        profile = yaml.safe_load(
            outcome.profile_path.read_text(encoding="utf-8")
        )
        assert (
            loaded.bronze_schema_fingerprint == profile["bronzeSchemaFingerprint"]
        )
        # Profile untouched.
        assert outcome.profile_path.stat().st_mtime_ns == profile_mtime_before

    def test_refresh_backfills_content_handedit(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Snapshot metadata fingerprint matches live, but the
        `datasets[]` contents were hand-edited so a fresh recompute
        disagrees. Without this back-fill, the runtime preflight would
        later reject the snapshot, ask for `--refresh`, and the refresh
        would no-op forever."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        # Hand-edit: drop a column from one dataset's list without
        # touching the metadata fingerprint.
        raw = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
        # Find the first non-empty dataset
        for ds in raw["datasets"]:
            if ds["columns"]:
                ds["columns"].pop(0)
                break
        snapshot_path.write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")

        outcome2 = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome2.exit_code == 0
        assert "back-filled" in outcome2.summary
        # Snapshot is now self-consistent again.
        loaded = load_bronze_schema_snapshot(snapshot_path)
        from oracle_ai_data_platform_fusion_autopilot.schema.bronze_schema_snapshot import (
            snapshot_to_observed,
        )
        assert compute_bronze_fingerprint(
            observed=snapshot_to_observed(loaded)
        ) == loaded.bronze_schema_fingerprint

    def test_refresh_backfills_unparseable_snapshot(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        snapshot_path.write_text("not: [valid: yaml", encoding="utf-8")

        outcome2 = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome2.exit_code == 0
        assert "back-filled" in outcome2.summary
        # Snapshot now parses again.
        load_bronze_schema_snapshot(snapshot_path)

    def test_refresh_backfill_then_preflight_read_when_profile_name_differs_from_tenant_field(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Reviewer-caught (round 1, BLOCKING): when
        ``bundle.contentPack.profile`` differs from a pre-3d profile
        YAML's ``tenant:`` field, ``bootstrap --refresh`` back-fills
        the snapshot under the **active profile name**
        (``contentPack.profile``), and preflight must read it from
        the SAME key. End-to-end probe to prove the file written by
        the back-fill is the file preflight reads.
        """
        from unittest.mock import MagicMock

        from oracle_ai_data_platform_fusion_autopilot.orchestrator.preflight_evidence import (
            check_bronze_fingerprint_drift,
        )

        monkeypatch.setenv("USER", "alice@oracle.com")
        # Initial pin via bootstrap — uses contentPack.profile="finance-default"
        # for both file key and the in-YAML tenant field.
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        # Simulate a hand-authored pre-3d profile shape: rewrite the
        # `tenant:` field to a different value than contentPack.profile.
        # Also delete the snapshot so we exercise the back-fill path.
        profile_path = bundle_dir / "profiles" / "finance-default.yaml"
        profile_doc = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
        profile_doc["tenant"] = "acme-prod"
        profile_path.write_text(
            yaml.safe_dump(profile_doc, sort_keys=False), encoding="utf-8"
        )
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        snapshot_path.unlink()

        # bootstrap --refresh — no drift, back-fills the snapshot under
        # the contentPack.profile key.
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome.exit_code == 0
        assert "back-filled" in outcome.summary
        # File appeared at the contentPack.profile-keyed path.
        assert snapshot_path.exists()
        # The legacy hand-authored `tenant:` field is preserved (no-drift
        # branch does NOT rewrite the profile).
        profile_after = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
        assert profile_after["tenant"] == "acme-prod"

        # Now exercise preflight against drifted bronze. If preflight
        # had keyed on profile.tenant="acme-prod" it would have looked
        # at profiles/acme-prod.schema-snapshot.yaml (which doesn't
        # exist) and emitted empty datasetDeltas. With the fix, it
        # keys on bundle.contentPack.profile="finance-default" and
        # finds the back-filled snapshot — populating datasetDeltas.
        from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
            load_tenant_profile,
        )

        loaded_profile = load_tenant_profile(profile_path)
        assert loaded_profile.tenant == "acme-prod"

        # Build a mock pack + spark with the same dataset shape as the
        # backfill, but drift one column to trigger the gate.
        drifted = {**DEMO_POD_BRONZE, "ap_invoices": ["RenamedCurrencyCol"]}
        spark = _mock_spark(drifted)

        pack_mock = MagicMock(name="pack")
        pack_mock.bronze_yaml = {
            "datasets": [{"id": k} for k in DEMO_POD_BRONZE.keys()]
        }
        pack_mock.pack.semantic_variants = {}

        result = check_bronze_fingerprint_drift(
            spark=spark,
            bundle=bundle,  # bundle.content_pack.profile == "finance-default"
            bundle_path=bundle_dir / "bundle.yaml",
            pack=pack_mock,
            profile=loaded_profile,  # profile.tenant == "acme-prod"
            run_id="cp-3d-divergent-key",
            mode="incremental",
            workdir=bundle_dir,
        )
        assert result.kind == "drift"
        import json

        body = json.loads(result.diagnostic_path.read_text(encoding="utf-8"))
        deltas = body["schemaDrift"]["datasetDeltas"]
        # Snapshot was found via the contentPack.profile key — diff
        # populated. (Empty here would mean the regression came back.)
        assert any(d["datasetId"] == "ap_invoices" for d in deltas), (
            "Phase 3d snapshot keyed on contentPack.profile was not "
            "read — preflight is keying on profile.tenant again."
        )

    def test_refresh_genuine_noop_when_snapshot_matches(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Healthy snapshot + matching fingerprint → genuine no-op
        (no `back-filled` in the summary, snapshot mtime preserved)."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        snapshot_path = resolve_snapshot_path(
            bundle_dir / "bundle.yaml", "finance-default"
        )
        snapshot_mtime_before = snapshot_path.stat().st_mtime_ns

        outcome2 = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
        )
        assert outcome2.exit_code == 0
        assert "back-filled" not in outcome2.summary
        assert outcome2.summary == "bootstrap --refresh: no drift detected"
        assert snapshot_path.stat().st_mtime_ns == snapshot_mtime_before


# ---------------------------------------------------------------------------
# Fresh tenant: bronze not landed → local probe resolves via the SOURCE
# producer (no AIDPF-2049, no "land bronze first"). Step 6 seal.
# ---------------------------------------------------------------------------


def _mock_spark_absent() -> MagicMock:
    """Spark whose DESCRIBE ... .take(1) reports table-or-view-not-found —
    the strict absence detector routes every node to the source producer."""
    spark = MagicMock(name="spark")

    def _sql(_query: str):
        df = MagicMock(name="df")
        df.take.side_effect = Exception(
            "[TABLE_OR_VIEW_NOT_FOUND] bronze table not landed yet"
        )
        return df

    spark.sql.side_effect = _sql
    return spark


class TestFreshTenantSourceProbe:
    def test_absent_bronze_resolves_via_source_no_2049(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands import bronze_probe
        from oracle_ai_data_platform_fusion_autopilot.schema.bronze_fingerprint import (
            ColumnInfo,
        )

        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")

        # The BICC source produces the same columns a landed table would —
        # so the variation walkers resolve identically to the landed path.
        def _fake_source(spark, *, pack, bundle, resolved_password, dataset_ids=None):
            return {
                ds: [ColumnInfo(name=c, type="string") for c in DEMO_POD_BRONZE[ds]]
                for ds in (dataset_ids or [])
                if ds in DEMO_POD_BRONZE
            }

        monkeypatch.setattr(bronze_probe, "describe_bronze_from_source", _fake_source)

        resolutions_file = bundle_dir / "resolutions.json"
        resolutions_file.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "tenant": "finance-default",
                    "resolutions": [
                        {
                            "name": "invoice_currency_code",
                            "kind": "columnAliases",
                            "chosenCandidate": "ApInvoicesInvoiceCurrencyCode",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark_absent(),
                resolutions_path=resolutions_file,
            ),
        )

        # Fresh tenant succeeds via source probe — no error, no AIDPF-2049.
        assert outcome.exit_code == 0
        assert outcome.profile_path is not None and outcome.profile_path.exists()
        assert all(
            "2049" not in p.name for p in outcome.diagnostic_paths
        )
        profile = yaml.safe_load(outcome.profile_path.read_text(encoding="utf-8"))
        # Walkers resolved from the source-derived observed columns.
        assert profile["resolved"]["column"]["vendor_id"] == "VENDORID"
        assert profile["resolved"]["column"]["supplier_natural_key"] == "SEGMENT1"


# ---------------------------------------------------------------------------
# metadata-driven-coa-resolution — COA advisory wiring (Step 3)
#
# The advisory itself is unit-tested in test_coa_advisory.py; these tests pin
# the WIRING: both call paths invoke it, its lines render on the injected
# console, and NO failure mode can change the phase's exit code or summary.
# ---------------------------------------------------------------------------

from io import StringIO

from rich.console import Console

from oracle_ai_data_platform_fusion_autopilot.commands import (
    coa_advisory as coa_advisory_module,
)
from oracle_ai_data_platform_fusion_autopilot.commands.coa_advisory import (
    CoaAdvisoryResult,
)


@pytest.fixture(autouse=True)
def _no_fusion_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hermetic tests: never let a developer shell's real Fusion credentials
    leak in (the advisory would then attempt live HTTP)."""
    monkeypatch.delenv("FUSION_BICC_USER", raising=False)
    monkeypatch.delenv("FUSION_BICC_PASSWORD", raising=False)


def _capture_console() -> tuple[Console, StringIO]:
    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=400), buf


class _AdvisoryResp:
    """Minimal requests.Response stand-in for the advisory fetchers."""

    def __init__(self, items: list[dict], has_more: bool = False) -> None:
        self._payload = {"items": items, "hasMore": has_more}
        self.status_code = 200

    def raise_for_status(self) -> None:  # pragma: no cover - never errors here
        pass

    def json(self) -> dict:
        return self._payload


class _AdvisorySession:
    """Fake authenticated session: chart LOV + per-chart activity probes."""

    def __init__(
        self,
        charts: list[tuple[str, bool]],
        active_chart_ids: set[str],
    ) -> None:
        self._charts = charts
        self._active = active_chart_ids
        self.requests: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float) -> _AdvisoryResp:
        self.requests.append((url, dict(params)))
        if "chartOfAccountsLOV" in url:
            return _AdvisoryResp(
                [
                    {"StructureInstanceNumber": cid, "EnabledFlag": enabled}
                    for cid, enabled in self._charts
                ]
            )
        assert "accountCombinationsLOV" in url, url
        chart_id = params["q"].rsplit("=", 1)[1]
        if chart_id in self._active:
            return _AdvisoryResp([{"_CODE_COMBINATION_ID": "1"}])
        return _AdvisoryResp([])


class TestCoaAdvisoryWiring:
    def _initial_pin(
        self, bundle_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Path:
        monkeypatch.setenv("USER", "alice@oracle.com")
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
        )
        assert outcome.exit_code == 0
        return outcome.profile_path

    def test_no_drift_refresh_emits_advisory_finding(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Finding-1 regression: an UNCHANGED bronze fingerprint must not
        suppress the advisory — a newly active chart doesn't alter existing
        bronze schemas, so the no-drift `--refresh` early return is exactly
        where the miss would happen. End-to-end through the REAL
        run_coa_advisory with a fake REST session."""
        profile_path = self._initial_pin(bundle_dir, monkeypatch)

        # Simulate a mapped multi-COA tenant: give the pinned profile a
        # byChart arm for chart 100 (profile.profile is free-form; the
        # bronze fingerprint is untouched, so refresh takes the no-drift
        # early-return branch).
        data = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
        data["profile"].setdefault("chartOfAccounts", {})["byChart"] = {
            "100": {"balancingSegment": "CodeCombinationSegment1"}
        }
        profile_path.write_text(yaml.safe_dump(data), encoding="utf-8")

        # Fusion now shows an ADDITIONAL enabled chart 205 with enabled
        # combinations — active-but-unmapped, the advisory's core finding.
        session = _AdvisorySession(
            charts=[("100", True), ("205", True)],
            active_chart_ids={"100", "205"},
        )
        real = coa_advisory_module.run_coa_advisory
        monkeypatch.setattr(
            coa_advisory_module,
            "run_coa_advisory",
            lambda **kw: real(**{**kw, "session": session}),
        )

        console, buf = _capture_console()
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
            console=console,
        )

        # Still the no-drift early return — advisory changed NOTHING.
        assert outcome.exit_code == 0
        assert "no drift" in outcome.summary
        out = buf.getvalue()
        assert "205" in out and "ACTIVE" in out
        assert "bootstrap --refresh" in out  # remediation text rendered
        assert "No drift detected" in out  # original message intact
        # The candidate probe used the Step-1-validated SEMICOLON grammar.
        probe_qs = [
            p["q"] for (u, p) in session.requests if "accountCombinations" in u
        ]
        assert "EnabledFlag=Y;_CHART_OF_ACCOUNTS_ID=205" in probe_qs

    def test_full_path_advisory_runs_after_profile_write(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The full (non-refresh) path calls the advisory AFTER the profile
        is durable, passing the freshly resolved chartOfAccounts."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        calls: list[dict] = []

        def _recorder(**kw) -> CoaAdvisoryResult:
            profile_path = (
                bundle_dir / "profiles" / "finance-default.yaml"
            )
            calls.append(
                {
                    "chart_of_accounts": kw["chart_of_accounts"],
                    "profile_durable": profile_path.exists(),
                }
            )
            return CoaAdvisoryResult(kind="skipped", skip_reason="recorder")

        monkeypatch.setattr(
            coa_advisory_module, "run_coa_advisory", _recorder
        )

        console, buf = _capture_console()
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
            console=console,
        )

        assert outcome.exit_code == 0
        assert len(calls) == 1
        assert calls[0]["profile_durable"] is True
        # The kwarg is the resolved profile's chartOfAccounts, verbatim.
        written = yaml.safe_load(
            outcome.profile_path.read_text(encoding="utf-8")
        )
        assert calls[0]["chart_of_accounts"] == written["profile"].get(
            "chartOfAccounts"
        )
        assert "COA advisory skipped: recorder" in buf.getvalue()

    def test_advisory_crash_never_blocks_no_drift_refresh(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Even a bug INSIDE the advisory (raise instead of fail-soft) must
        not change the no-drift early return's exit code or summary."""
        self._initial_pin(bundle_dir, monkeypatch)

        def _boom(**kw):
            raise RuntimeError("advisory bug")

        monkeypatch.setattr(coa_advisory_module, "run_coa_advisory", _boom)

        console, buf = _capture_console()
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
                refresh=True,
            ),
            console=console,
        )

        assert outcome.exit_code == 0
        assert "no drift" in outcome.summary
        assert (
            "COA advisory skipped: unexpected RuntimeError" in buf.getvalue()
        )

    def test_missing_creds_end_to_end_skip(
        self,
        bundle_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Default test/CI environment (no Fusion env creds): the REAL
        advisory chain skips before any HTTP — labelled, non-blocking."""
        monkeypatch.setenv("USER", "alice@oracle.com")
        console, buf = _capture_console()
        bundle = _load_bundle(bundle_dir / "bundle.yaml")
        outcome = run_variation_phase(
            bundle,
            bundle_dir / "bundle.yaml",
            options=VariationPhaseOptions(
                spark_session=_mock_spark(DEMO_POD_BRONZE),
                non_interactive=True, accept_coa_convention=True,
            ),
            console=console,
        )

        assert outcome.exit_code == 0
        assert (
            "COA advisory skipped: FUSION_BICC_USER / FUSION_BICC_PASSWORD "
            "not set" in buf.getvalue()
        )
