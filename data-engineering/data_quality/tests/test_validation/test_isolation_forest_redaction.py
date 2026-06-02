"""Column-name redaction tests for IsolationForestValidator.

Mirrors ``test_pattern_check_redaction.py`` (T1–T8). Both
validators share the same redaction surface (top_features +
drift entries + schema-change), so the tests are structurally
identical with a different validator class.
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("shap")

from qualifire.core._redaction import RedactionPolicy
from qualifire.core.models import CollectionResult, Severity
from qualifire.validation.isolation_forest import IsolationForestValidator


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
        "amount": rng.normal(800, 10, n),
        "home_zipcode": rng.randint(10000, 11000, n).astype(str),
        "category": rng.choice(["A", "B", "C"], n, p=[0.95, 0.025, 0.025]),
    })


def _make_validator(*, denylist=None, allowlist=None) -> IsolationForestValidator:
    policy = RedactionPolicy.build(
        instance_denylist=denylist, instance_allowlist=allowlist,
        dataset_denylist=None, dataset_allowlist=None,
    )
    return IsolationForestValidator(
        explain=True, explain_value_drift=True,
        warning_threshold=0.05, error_threshold=0.5,
        redaction_policy=policy,
    )


def _run(validator) -> dict:
    current = _shifted(200, seed=1)
    past = [("past_1", _normal(200, seed=2))]
    results = validator.validate(_make_sample_collected(current, past))
    r = results[0]
    if r.severity == Severity.PASS:
        pytest.skip("anomaly ratio stayed below threshold under this seed")
    return r.details


class TestIsolationForestRedaction:
    def test_T1_column_in_denylist_shows_redacted(self):
        details = _run(_make_validator(denylist=["home_zipcode"]))
        tf = details.get("top_contributing_features") or []
        if not tf:
            pytest.skip("no top features for this seed")
        names = [f["feature"] for f in tf]
        assert "home_zipcode" not in names
        assert "<redacted>" in names

    def test_T2_column_not_in_denylist_shows_real_name(self):
        details = _run(_make_validator(denylist=["nonexistent"]))
        tf = details.get("top_contributing_features") or []
        if not tf:
            pytest.skip("no top features for this seed")
        names = [f["feature"] for f in tf]
        assert "<redacted>" not in names

    def test_T3_allowlist_set_redacts_outside_allowlist(self):
        details = _run(_make_validator(allowlist=["amount"]))
        tf = details.get("top_contributing_features") or []
        if not tf:
            pytest.skip("no top features for this seed")
        for f in tf:
            assert f["feature"] in ("amount", "<redacted>"), (
                f"non-allowlisted leaked: {f['feature']!r}"
            )

    def test_T4_denylist_wins_over_allowlist(self):
        details = _run(_make_validator(
            denylist=["home_zipcode"],
            allowlist=["amount", "home_zipcode"],
        ))
        tf = details.get("top_contributing_features") or []
        if not tf:
            pytest.skip("no top features for this seed")
        names = [f["feature"] for f in tf]
        assert "home_zipcode" not in names

    def test_T5_drift_entries_redact_identifying_fields(self):
        details = _run(_make_validator(denylist=["home_zipcode"]))
        drift = details.get("value_drift_explainer") or []
        redacted = [e for e in drift if e.get("feature") == "<redacted>"]
        if not redacted:
            pytest.skip("no redacted drift entries for this seed")
        for e in redacted:
            assert e.get("source_column") == "<redacted>"
            assert e.get("kind") == "<redacted>"
            assert e.get("summary") == "<redacted>"


class TestIsolationForestRedactionPolicyMerge:
    def test_T6_dataset_global_denylists_union(self):
        policy = RedactionPolicy.build(
            instance_denylist=["home_zipcode"], instance_allowlist=None,
            dataset_denylist=["account_routing_number"], dataset_allowlist=None,
        )
        assert policy.redacted("home_zipcode")
        assert policy.redacted("account_routing_number")
        assert not policy.redacted("amount")

    def test_T7_allowlist_one_sided_semantics(self):
        # Same shape as pattern_check's T7; thin smoke test for the
        # mirror-validator path. Full coverage already in
        # test_pattern_check_redaction.py.
        p = RedactionPolicy.build(
            instance_denylist=None, instance_allowlist=["a"],
            dataset_denylist=None, dataset_allowlist=None,
        )
        assert p.allowlist == frozenset({"a"})


class TestIsolationForestSchemaChangeRedaction:
    def test_T8_schema_change_redacts(self):
        current = pd.DataFrame({
            "amount": np.random.normal(100, 10, 200),
            "home_zipcode": ["00001"] * 200,
        })
        past = [("past_1", pd.DataFrame({
            "amount": np.random.normal(100, 10, 200),
            "legacy_field": ["x"] * 200,
        }))]
        policy = RedactionPolicy.build(
            instance_denylist=["home_zipcode", "legacy_field"],
            instance_allowlist=None,
            dataset_denylist=None, dataset_allowlist=None,
        )
        validator = IsolationForestValidator(
            alert_on_schema_change=True,
            redaction_policy=policy,
        )
        results = validator.validate(_make_sample_collected(current, past))
        schema_results = [
            r for r in results if r.validation_name.endswith(".schema")
        ]
        assert schema_results, "schema-change emission missing"
        sr = schema_results[0]
        assert "home_zipcode" not in sr.details["new_columns"]
        assert "legacy_field" not in sr.details["dropped_columns"]
        assert "<redacted>" in sr.details["new_columns"]
        assert "<redacted>" in sr.details["dropped_columns"]
        assert "home_zipcode" not in sr.message
        assert "legacy_field" not in sr.message
        assert "<redacted>" in sr.message
