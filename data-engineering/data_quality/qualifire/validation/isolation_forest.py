"""Isolation Forest + SHAP anomaly detection validator.

Requires: qualifire[anomaly] (scikit-learn, shap)
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np

from qualifire.core.models import CollectionResult, Severity, ValidationResult
from qualifire.validation._drift_explainer import explain_value_drift
from qualifire.validation._encoding import encode_columns
from qualifire.validation.base import Validator

logger = logging.getLogger(__name__)


def _import_sklearn():
    try:
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import LabelEncoder, OneHotEncoder

        return IsolationForest, LabelEncoder, OneHotEncoder
    except ImportError:
        raise ImportError(
            "scikit-learn is required for anomaly detection. "
            "Install it with: pip install 'qualifire[anomaly]'"
        )


def _import_shap():
    try:
        import shap

        return shap
    except ImportError:
        raise ImportError(
            "SHAP is required for anomaly explainability. "
            "Install it with: pip install 'qualifire[anomaly]'"
        )


class IsolationForestValidator(Validator):
    """Detects anomalies using Isolation Forest on sampled data.

    Compares current period data against historical periods. Uses column
    intersection for schema changes. Explains top contributing columns via SHAP.

    Args:
        n_estimators: Number of trees in the forest.
        contamination: Expected proportion of outliers, or "auto".
        warning_threshold: Anomaly ratio for WARNING.
        error_threshold: Anomaly ratio for ERROR.
        explain: Whether to compute SHAP explanations.
        drop_complex: Drop complex types (arrays, maps, structs) instead of flattening.
        alert_on_schema_change: Trigger alert on schema differences.
        name: Validation name.
    """

    CARDINALITY_THRESHOLD = 20

    def __init__(
        self,
        n_estimators: int = 100,
        contamination: str | float = "auto",
        warning_threshold: float = 0.6,
        error_threshold: float = 0.8,
        explain: bool = True,
        drop_complex: bool = False,
        alert_on_schema_change: bool = False,
        exclude_columns: list[str] | None = None,
        name: str = "shape_check",
        on_empty_data: Severity = Severity.WARNING,
        on_missing_history: str = "ignore",
        explain_value_drift: bool = True,
        redaction_policy: Any = None,
        drift_breakdown_by_slice: bool = False,
    ):
        super().__init__(on_empty_data=on_empty_data)
        self.drift_breakdown_by_slice = drift_breakdown_by_slice
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.warning_threshold = warning_threshold
        self.error_threshold = error_threshold
        self.explain = explain
        self.drop_complex = drop_complex
        self.alert_on_schema_change = alert_on_schema_change
        self.explain_value_drift = explain_value_drift
        if redaction_policy is None:
            from qualifire.core._redaction import RedactionPolicy
            redaction_policy = RedactionPolicy()
        self.redaction_policy = redaction_policy
        # Columns to drop from the feature matrix before fitting.
        # The IF tree splits on the partition column trivially when
        # included (today's rows have ``slice_column = '<today>'``,
        # all past rows have other dates → perfect separator → 100%
        # current-period rows flagged as anomalies). Per-row IDs
        # (``sale_id``) and ingestion timestamps (``updated_at``)
        # leak the same way.
        self.exclude_columns = list(exclude_columns or [])
        self.name = name
        self.on_missing_history = on_missing_history

    # ---- webhook-payload-redaction helpers (same shape as PatternCheckValidator) ----

    def _redact_columns(self, columns: list[str]) -> list[str]:
        from qualifire.core._redaction import REDACTED_PLACEHOLDER

        return [
            REDACTED_PLACEHOLDER
            if self.redaction_policy.redacted(col)
            else col
            for col in columns
        ]

    def _redact_top_features(
        self,
        top_features: list[dict[str, Any]],
        encoding_map: dict[str, Any],
    ) -> list[dict[str, Any]]:
        from qualifire.core._redaction import REDACTED_PLACEHOLDER

        out: list[dict[str, Any]] = []
        for f in top_features:
            name = f.get("feature", "")
            spec = encoding_map.get(name)
            source = spec.source_column if spec is not None else name
            if self.redaction_policy.redacted(source):
                out.append({**f, "feature": REDACTED_PLACEHOLDER})
            else:
                out.append(f)
        return out

    def _redact_drift_entries(
        self,
        drift_entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        from qualifire.core._redaction import REDACTED_PLACEHOLDER

        out: list[dict[str, Any]] = []
        for entry in drift_entries:
            source = entry.get("source_column")
            if source and self.redaction_policy.redacted(source):
                redacted = dict(entry)
                for key in (
                    "feature", "source_column", "kind",
                    "category", "summary",
                ):
                    if key in redacted:
                        redacted[key] = REDACTED_PLACEHOLDER
                out.append(redacted)
            else:
                out.append(entry)
        return out

    def validate(
        self,
        collected: list[CollectionResult],
        **kwargs: Any,
    ) -> list[ValidationResult]:
        IsolationForest, LabelEncoder, OneHotEncoder = _import_sklearn()
        import pandas as pd

        results: list[ValidationResult] = []

        for cr in collected:
            if cr.metric_name != "sample":
                continue

            meta = cr.metadata
            current_df = meta["current_df"]
            past_dfs = meta["past_dfs"]

            # Convert Spark DataFrames to pandas if needed
            if hasattr(current_df, "toPandas"):
                current_pdf = current_df.toPandas()
            else:
                current_pdf = current_df

            past_pdfs = []
            for label, pdf in past_dfs:
                if hasattr(pdf, "toPandas"):
                    past_pdfs.append((label, pdf.toPandas()))
                else:
                    past_pdfs.append((label, pdf))

            if current_pdf.empty:
                results.append(self._empty_data_result(
                    self.name, "shape",
                    "Insufficient data for anomaly detection: no current data",
                ))
                continue

            if not past_pdfs:
                severity_map = {"ignore": Severity.PASS, "warn": Severity.WARNING, "error": Severity.ERROR}
                results.append(
                    ValidationResult(
                        validation_name=self.name,
                        validation_type="shape",
                        severity=severity_map[self.on_missing_history],
                        message="No historical data for anomaly detection, skipping comparison",
                        validation_base_name=self.name,
                        metric_name="anomaly_ratio",
                        details={"cold_start": True},
                    )
                )
                continue

            # Intersect across every past slice (NOT union) so that a
            # column present in only some past slices is dropped before
            # encoding. Using union would silently retain such columns
            # in keep_cols and then KeyError when reindexed against the
            # slice that is missing them — exactly the failure mode
            # this validator claims to tolerate via schema-change
            # detection. Mirrors the round-3 fix in pattern_check.
            current_cols = set(current_pdf.columns)
            past_col_sets = [set(pdf.columns) for _, pdf in past_pdfs]
            past_cols_all = set().union(*past_col_sets) if past_col_sets else set()
            past_cols_common = (
                set.intersection(*past_col_sets) if past_col_sets else set()
            )

            common_cols = sorted(current_cols & past_cols_common)
            new_cols = current_cols - past_cols_all
            dropped_cols = past_cols_all - current_cols
            inconsistent_past = past_cols_all - past_cols_common

            if new_cols:
                logger.info("New columns detected: %s", new_cols)
            if dropped_cols:
                logger.info("Dropped columns detected: %s", dropped_cols)
            if inconsistent_past:
                logger.info(
                    "Columns missing from some past slices (excluded from features): %s",
                    inconsistent_past,
                )

            if self.alert_on_schema_change and (
                new_cols or dropped_cols or inconsistent_past
            ):
                redacted_new = self._redact_columns(list(new_cols))
                redacted_dropped = self._redact_columns(list(dropped_cols))
                redacted_inconsistent = self._redact_columns(
                    list(inconsistent_past),
                )
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.schema",
                        validation_type="shape",
                        severity=Severity.WARNING,
                        message=(
                            f"Schema change: new={redacted_new}, "
                            f"dropped={redacted_dropped}, "
                            f"inconsistent_past={redacted_inconsistent}"
                        ),
                        validation_base_name=self.name,
                        details={
                            "new_columns": redacted_new,
                            "dropped_columns": redacted_dropped,
                            "inconsistent_past_columns": redacted_inconsistent,
                        },
                    )
                )

            # Use common columns only
            current_pdf = current_pdf[common_cols]
            past_pdfs = [(label, pdf[common_cols]) for label, pdf in past_pdfs]

            # Capture pre-encoding pandas for the value-drift
            # explainer before the synthetic ``_qf_period`` column
            # gets injected. Read .copy() now so later mutations on
            # ``current_pdf`` (label injection, exclude-column drops)
            # don't bleed back into the explainer's view.
            current_for_explain = current_pdf.copy()
            past_for_explain = [(label, pdf.copy()) for label, pdf in past_pdfs]

            # Label periods
            current_pdf = current_pdf.copy()
            current_pdf["_qf_period"] = "current"
            all_past = []
            for label, pdf in past_pdfs:
                pdf = pdf.copy()
                pdf["_qf_period"] = label
                all_past.append(pdf)

            combined = pd.concat([current_pdf] + all_past, ignore_index=True)
            period_labels = combined["_qf_period"].values
            combined = combined.drop(columns=["_qf_period"])

            # Drop leakage-prone columns before encoding. The
            # ``slice_column`` and per-row ID columns (``sale_id``,
            # ``updated_at``, etc.) split today vs past trivially —
            # the IF would otherwise flag every current-period row
            # as anomalous regardless of true distribution drift.
            # Pattern handles this in `keep_cols`; mirroring here
            # so the two sample-based validators behave identically.
            if self.exclude_columns:
                drop_cols = [c for c in self.exclude_columns if c in combined.columns]
                if drop_cols:
                    combined = combined.drop(columns=drop_cols)

            # Encode columns (shared with PatternCheckValidator so the
            # two sample-based validators cannot disagree on what a
            # given DataFrame means as a feature matrix).
            encoded, feature_names, encoding_map = encode_columns(
                combined,
                drop_complex=self.drop_complex,
                cardinality_threshold=self.CARDINALITY_THRESHOLD,
            )
            if encoded.shape[1] == 0:
                results.append(self._empty_data_result(
                    self.name, "shape",
                    "No encodable columns for anomaly detection",
                ))
                continue

            # Fit Isolation Forest on all data
            model = IsolationForest(
                n_estimators=self.n_estimators,
                contamination=self.contamination if self.contamination != "auto" else "auto",
                random_state=42,
            )
            model.fit(encoded)

            # Score current period
            current_mask = period_labels == "current"
            current_encoded = encoded[current_mask]
            predictions = model.predict(current_encoded)
            anomaly_count = int((predictions == -1).sum())
            total_current = len(current_encoded)
            anomaly_ratio = anomaly_count / max(total_current, 1)

            # Determine severity
            warning_violated = anomaly_ratio >= self.warning_threshold
            error_violated = anomaly_ratio >= self.error_threshold
            severity = self.determine_severity(
                anomaly_ratio, warning_violated, error_violated
            )

            details: dict[str, Any] = {
                "anomaly_count": anomaly_count,
                "total_current": total_current,
                "anomaly_ratio": anomaly_ratio,
            }

            # SHAP explanations
            if self.explain and anomaly_count > 0:
                try:
                    shap = _import_shap()
                    explainer = shap.TreeExplainer(model)
                    shap_values = explainer.shap_values(current_encoded)
                    mean_abs_shap = np.abs(shap_values).mean(axis=0)
                    top_indices = np.argsort(mean_abs_shap)[-5:][::-1]
                    top_features = [
                        {"feature": feature_names[i], "importance": float(mean_abs_shap[i])}
                        for i in top_indices
                    ]

                    if self.explain_value_drift and top_features:
                        try:
                            # IMPORTANT: feed REAL feature names to the
                            # explainer so its encoding_map lookup works.
                            # Apply redaction AFTER the explainer returns.
                            drift_entries, mapping_errors = explain_value_drift(
                                top_features,
                                current_for_explain,
                                past_for_explain,
                                encoding_map,
                                breakdown_by_slice=self.drift_breakdown_by_slice,
                            )
                            drift_entries = self._redact_drift_entries(drift_entries)
                            details["value_drift_explainer"] = drift_entries
                            if any(
                                e.get("kind") == "truncated" for e in drift_entries
                            ):
                                details["value_drift_explainer_truncated"] = True
                            if mapping_errors:
                                details["value_drift_explainer_mapping_errors"] = (
                                    int(mapping_errors)
                                )
                        except Exception as drift_exc:
                            logger.warning(
                                "Value drift explainer failed: %s", drift_exc,
                            )
                            details["value_drift_explainer_error"] = (
                                f"{type(drift_exc).__name__}: {drift_exc}"
                            )

                    details["top_contributing_features"] = (
                        self._redact_top_features(top_features, encoding_map)
                    )
                except Exception as e:
                    logger.warning("SHAP explanation failed: %s", e)

            message = (
                f"Anomaly ratio: {anomaly_ratio:.2%} "
                f"({anomaly_count}/{total_current} records flagged)"
            )
            # Append top-3 SHAP contributors so the dashboard's
            # message column shows *which features* drove the
            # anomaly score, not just "X% flagged". Same rationale
            # as the pattern validator — full top-5 list still
            # lives on ``details.top_contributing_features`` for
            # notification rendering.
            top_feats = details.get("top_contributing_features") or []
            if top_feats:
                top3 = ", ".join(
                    f"{f['feature']}({f['importance']:+.3f})"
                    for f in top_feats[:3]
                )
                message = f"{message} · top: {top3}"

            results.append(
                ValidationResult(
                    validation_name=self.name,
                    validation_type="shape",
                    severity=severity,
                    message=message,
                    validation_base_name=self.name,
                    metric_name="anomaly_ratio",
                    expected_value={
                        "warning_threshold": self.warning_threshold,
                        "error_threshold": self.error_threshold,
                    },
                    actual_value=anomaly_ratio,
                    details=details,
                    collected_at=cr.collected_at,
                )
            )

        return results
