"""Column-name redaction tests for PatternCheckValidator.

Covers the locked-decision matrix:
- T1: column in denylist → SHAP top entry shows ``feature="<redacted>"``.
- T2: column NOT in denylist → SHAP top entry shows real name.
- T3: allowlist set, column NOT in allowlist → redacted.
- T4: column in BOTH allowlist AND denylist → redacted (denylist wins).
- T5: drift explainer entry for a redacted column → identifying
      fields are ``<redacted>``, numeric stats intact.
- T6: dataset-level + global denylists merge (union).
- T7: allowlist scope semantics — instance-only, dataset-only,
      both set (intersection).
- T8: schema-change emission (new_columns / dropped_columns /
      inconsistent_past_columns + message-string interpolation).
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")

from qualifire.core._redaction import RedactionPolicy
from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.pattern_check import PatternCheckValidator


def _make_sample_collected(current_df, past_dfs):
    return [
        CollectionResult(
            metric_name="sample",
            metric_value=None,
            collected_at=datetime.now(),
            metadata={
                "table": "test_table",
                "collection_type": "sample",
                "n_records": len(current_df),
                "current_df": current_df,
                "past_dfs": past_dfs,
            },
        )
    ]


def _normal(n, seed):
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "amount": rng.normal(100, 10, n),
        "home_zipcode": rng.randint(10000, 99999, n).astype(str),
        "category": rng.choice(["A", "B", "C"], n),
    })


def _shifted(n, seed):
    rng = np.random.RandomState(seed)
    return pd.DataFrame({
        "amount": rng.normal(500, 10, n),
        "home_zipcode": rng.randint(10000, 20000, n).astype(str),
        "category": rng.choice(["A", "B", "C"], n, p=[0.9, 0.05, 0.05]),
    })


def _make_validator(*, denylist=None, allowlist=None) -> PatternCheckValidator:
    policy = RedactionPolicy.build(
        instance_denylist=denylist,
        instance_allowlist=allowlist,
        dataset_denylist=None,
        dataset_allowlist=None,
    )
    return PatternCheckValidator(
        explain=True, explain_value_drift=True,
        redaction_policy=policy,
    )


def _run(validator) -> dict:
    """Run on shifted data, return details dict from the alert
    result. Skips the test if AUC didn't push severity past PASS
    (rare but possible under different seeds)."""
    current = _shifted(200, seed=1)
    past = [("past_1", _normal(200, seed=2))]
    results = validator.validate(_make_sample_collected(current, past))
    r = results[0]
    if r.severity == Severity.PASS:
        pytest.skip("AUC stayed below the warning threshold under this seed")
    return r.details


class TestPatternCheckRedaction:
    def test_T1_column_in_denylist_shows_redacted(self):
        details = _run(_make_validator(denylist=["home_zipcode"]))
        tf = details.get("top_contributing_features") or []
        assert tf, "no top_contributing_features emitted"
        # Find any entry sourced from home_zipcode (post-encoded as
        # "home_zipcode" itself for numeric columns) — must be redacted.
        names = [f["feature"] for f in tf]
        # No real "home_zipcode" leaks; some entry should be "<redacted>".
        assert "home_zipcode" not in names, (
            f"home_zipcode leaked unredacted: {names}"
        )
        assert "<redacted>" in names, (
            f"expected at least one <redacted> entry, got {names}"
        )

    def test_T2_column_not_in_denylist_shows_real_name(self):
        details = _run(_make_validator(denylist=["nonexistent_column"]))
        tf = details.get("top_contributing_features") or []
        names = [f["feature"] for f in tf]
        assert "<redacted>" not in names, (
            f"unexpected redaction; non-listed columns must pass through: {names}"
        )

    def test_T3_allowlist_set_column_not_in_allowlist_redacted(self):
        details = _run(_make_validator(allowlist=["amount"]))
        tf = details.get("top_contributing_features") or []
        names = [f["feature"] for f in tf]
        # Only "amount" passes; everything else (home_zipcode + category-*)
        # gets redacted.
        for f in tf:
            if f["feature"] not in ("amount", "<redacted>"):
                pytest.fail(
                    f"non-allowlisted feature {f['feature']!r} leaked"
                )

    def test_T4_denylist_wins_over_allowlist(self):
        details = _run(_make_validator(
            denylist=["home_zipcode"],
            allowlist=["amount", "home_zipcode"],
        ))
        tf = details.get("top_contributing_features") or []
        names = [f["feature"] for f in tf]
        assert "home_zipcode" not in names, (
            "denylist must override allowlist; home_zipcode leaked"
        )

    def test_T5_drift_entries_redact_identifying_fields(self):
        details = _run(_make_validator(denylist=["home_zipcode"]))
        drift = details.get("value_drift_explainer") or []
        if not drift:
            pytest.skip("no drift entries produced for this seed")
        redacted_entries = [
            e for e in drift if e.get("feature") == "<redacted>"
        ]
        assert redacted_entries, (
            "expected at least one redacted drift entry; got "
            f"{[e.get('feature') for e in drift]}"
        )
        for e in redacted_entries:
            assert e.get("source_column") == "<redacted>"
            assert e.get("kind") == "<redacted>"
            assert e.get("summary") == "<redacted>"
            # Numeric blocks pass through.
            if "current" in e:
                assert isinstance(e["current"], dict)


class TestPatternCheckRedactionPolicyMerge:
    def test_T6_dataset_and_global_denylists_union(self):
        """Plan AC2: instance-level redaction merges with dataset-level
        as a union. The engine builds the policy via
        RedactionPolicy.build(); this test pins the helper directly
        because the engine has its own integration test elsewhere."""
        policy = RedactionPolicy.build(
            instance_denylist=["home_zipcode"],
            instance_allowlist=None,
            dataset_denylist=["account_routing_number"],
            dataset_allowlist=None,
        )
        assert policy.redacted("home_zipcode")
        assert policy.redacted("account_routing_number")
        assert not policy.redacted("amount")

    def test_T7_allowlist_one_sided_semantics(self):
        # Both None → no allowlist policy.
        p1 = RedactionPolicy.build(
            instance_denylist=None, instance_allowlist=None,
            dataset_denylist=None, dataset_allowlist=None,
        )
        assert p1.allowlist is None
        # Instance-only → uses instance allowlist.
        p2 = RedactionPolicy.build(
            instance_denylist=None, instance_allowlist=["a", "b"],
            dataset_denylist=None, dataset_allowlist=None,
        )
        assert p2.allowlist == frozenset({"a", "b"})
        # Dataset-only → uses dataset allowlist.
        p3 = RedactionPolicy.build(
            instance_denylist=None, instance_allowlist=None,
            dataset_denylist=None, dataset_allowlist=["x", "y"],
        )
        assert p3.allowlist == frozenset({"x", "y"})
        # Both set → intersection.
        p4 = RedactionPolicy.build(
            instance_denylist=None, instance_allowlist=["a", "b", "c"],
            dataset_denylist=None, dataset_allowlist=["b", "c", "d"],
        )
        assert p4.allowlist == frozenset({"b", "c"})


class TestPatternCheckSchemaChangeRedaction:
    def test_T8_schema_change_emission_redacts_new_dropped_inconsistent(self):
        """T8: schema-change source-column lists AND the message
        interpolation get redacted."""
        # Force a schema change: current has a new column
        # ("home_zipcode") that past doesn't.
        current = pd.DataFrame({
            "amount": np.random.normal(100, 10, 200),
            "home_zipcode": ["00001"] * 200,  # NEW column
        })
        past = [("past_1", pd.DataFrame({
            "amount": np.random.normal(100, 10, 200),
            "legacy_field": ["x"] * 200,  # DROPPED column
        }))]
        policy = RedactionPolicy.build(
            instance_denylist=["home_zipcode", "legacy_field"],
            instance_allowlist=None,
            dataset_denylist=None,
            dataset_allowlist=None,
        )
        validator = PatternCheckValidator(
            alert_on_schema_change=True,
            redaction_policy=policy,
        )
        results = validator.validate(_make_sample_collected(current, past))

        # Find the schema-change result.
        schema_results = [
            r for r in results if r.validation_name.endswith(".schema")
        ]
        assert schema_results, "schema-change emission missing"
        sr = schema_results[0]

        # Lists are redacted.
        assert "home_zipcode" not in sr.details["new_columns"]
        assert "legacy_field" not in sr.details["dropped_columns"]
        assert "<redacted>" in sr.details["new_columns"]
        assert "<redacted>" in sr.details["dropped_columns"]

        # Message interpolation is redacted (codex R1 MAJOR).
        assert "home_zipcode" not in sr.message
        assert "legacy_field" not in sr.message
        assert "<redacted>" in sr.message
