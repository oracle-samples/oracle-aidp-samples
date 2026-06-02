"""Random Forest two-sample pattern-check validator.

Answers "does the multivariate shape of the current run differ from
past runs?" by training a Random Forest classifier to distinguish
past rows (label 0) from current rows (label 1). Cross-validated AUC
is the drift signal; SHAP over the final full-data model names the
features driving the separation.

This validator sits **alongside** ``shape`` (per-row outlier detection
via Isolation Forest) and ``drift`` (single-metric historical
deviation). It shares the ``sample`` collector and the shared
``encode_columns`` helper with ``shape`` — divergent encoding between
the two would give operators conflicting "top feature" attributions
for the same run.

Requires: ``qualifire[anomaly]`` (scikit-learn; SHAP optional for
``explain=True``).
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
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import StratifiedKFold, cross_val_score

        return RandomForestClassifier, StratifiedKFold, cross_val_score
    except ImportError:
        raise ImportError(
            "scikit-learn is required for pattern detection. "
            "Install it with: pip install 'qualifire[anomaly]'"
        )


def _import_shap():
    try:
        import shap

        return shap
    except ImportError:
        raise ImportError(
            "SHAP is required for pattern explainability. "
            "Install it with: pip install 'qualifire[anomaly]'"
        )


class PatternCheckValidator(Validator):
    """Detects multivariate drift via a two-sample Random Forest classifier.

    Trains a classifier where past samples are label 0 and current
    samples are label 1, cross-validates with StratifiedKFold, and
    alerts when the mean CV AUC exceeds the configured thresholds.
    """

    CARDINALITY_THRESHOLD = 20

    def __init__(
        self,
        n_estimators: int = 200,
        max_depth: int | None = 8,
        class_weight: str | None = "balanced",
        random_state: int = 42,
        cv_folds: int = 5,
        warning_threshold: float = 0.65,
        error_threshold: float = 0.80,
        explain: bool = True,
        drop_complex: bool = False,
        alert_on_schema_change: bool = False,
        exclude_columns: list[str] | None = None,
        name: str = "pattern_check",
        on_empty_data: Severity = Severity.WARNING,
        on_missing_history: str = "ignore",
        explain_value_drift: bool = True,
        redaction_policy: Any = None,
        drift_breakdown_by_slice: bool = False,
    ):
        super().__init__(on_empty_data=on_empty_data)
        self.drift_breakdown_by_slice = drift_breakdown_by_slice
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.class_weight = class_weight
        self.random_state = random_state
        self.cv_folds = cv_folds
        self.warning_threshold = warning_threshold
        self.error_threshold = error_threshold
        self.explain = explain
        self.drop_complex = drop_complex
        self.alert_on_schema_change = alert_on_schema_change
        self.exclude_columns = list(exclude_columns or [])
        self.name = name
        self.on_missing_history = on_missing_history
        self.explain_value_drift = explain_value_drift
        # Lazy import to avoid a hard dep on _redaction in places
        # that import only the validator types. Use a default no-op
        # policy when None — keeps the call sites unconditional.
        if redaction_policy is None:
            from qualifire.core._redaction import RedactionPolicy
            redaction_policy = RedactionPolicy()
        self.redaction_policy = redaction_policy

    # ---- webhook-payload-redaction helpers ----

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
        """Replace ``feature`` with the redaction placeholder when
        the post-encoded feature's source column is in the
        denylist (or outside the allowlist)."""
        from qualifire.core._redaction import REDACTED_PLACEHOLDER

        out: list[dict[str, Any]] = []
        for f in top_features:
            name = f.get("feature", "")
            spec = encoding_map.get(name)
            source = spec.source_column if spec is not None else name
            if self.redaction_policy.redacted(source):
                out.append(
                    {**f, "feature": REDACTED_PLACEHOLDER},
                )
            else:
                out.append(f)
        return out

    def _redact_drift_entries(
        self,
        drift_entries: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Replace identifying fields (``feature``, ``source_column``,
        ``kind``, ``category``, ``summary``) with the redaction
        placeholder when ``source_column`` is redacted. Numeric
        ``current`` / ``past`` / ``delta`` blocks pass through."""
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
        RandomForestClassifier, StratifiedKFold, cross_val_score = _import_sklearn()
        import pandas as pd

        results: list[ValidationResult] = []

        for cr in collected:
            if cr.metric_name != "sample":
                continue

            meta = cr.metadata
            current_df = meta["current_df"]
            past_dfs = meta["past_dfs"]

            if hasattr(current_df, "toPandas"):
                current_pdf = current_df.toPandas()
            else:
                current_pdf = current_df

            past_pdfs: list[tuple[str, Any]] = []
            for label, pdf in past_dfs:
                if hasattr(pdf, "toPandas"):
                    past_pdfs.append((label, pdf.toPandas()))
                else:
                    past_pdfs.append((label, pdf))

            if current_pdf.empty:
                results.append(self._empty_data_result(
                    self.name, "pattern",
                    "Insufficient data for pattern detection: no current data",
                ))
                continue

            if not past_pdfs:
                severity_map = {
                    "ignore": Severity.PASS,
                    "warn": Severity.WARNING,
                    "error": Severity.ERROR,
                }
                results.append(
                    ValidationResult(
                        validation_name=self.name,
                        validation_type="pattern",
                        severity=severity_map[self.on_missing_history],
                        message="No historical data for pattern detection, skipping comparison",
                        validation_base_name=self.name,
                        metric_name="auc",
                        details={"cold_start": True},
                    )
                )
                continue

            current_cols = set(current_pdf.columns)
            # Intersect across every past slice (NOT union) so that a
            # column present in only some past slices is dropped before
            # encoding. Using union would silently retain such columns
            # in ``keep_cols`` and then KeyError when reindexed against
            # the slice that is missing them — which is exactly the
            # failure mode this validator claims to tolerate via
            # schema-change detection. Plan.md Design §3 specifies
            # intersection.
            past_col_sets = [set(pdf.columns) for _, pdf in past_pdfs]
            past_cols_all = set().union(*past_col_sets) if past_col_sets else set()
            past_cols_common = (
                set.intersection(*past_col_sets) if past_col_sets else set()
            )

            common_cols = sorted(current_cols & past_cols_common)
            # Schema-change reporting uses the union for "new" / "dropped"
            # so operators see the full schema diff, not just the
            # columns that happened to be consistent across every past
            # slice.
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
                # Redact source-column names from the schema-change
                # emission. Both the message-string interpolation
                # (codex R1 MAJOR) and the persisted lists go through
                # the same policy.
                _redact = self._redact_columns
                redacted_new = _redact(list(new_cols))
                redacted_dropped = _redact(list(dropped_cols))
                redacted_inconsistent = _redact(list(inconsistent_past))
                results.append(
                    ValidationResult(
                        validation_name=f"{self.name}.schema",
                        validation_type="pattern",
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

            # Drop leakage columns before intersecting — a column listed in
            # exclude_columns must never leak into the classifier, even
            # when present in both slices.
            keep_cols = [c for c in common_cols if c not in self.exclude_columns]

            current_pdf = current_pdf[keep_cols].copy()
            past_pdfs = [(label, pdf[keep_cols].copy()) for label, pdf in past_pdfs]

            # Capture pre-encoding pandas for the value-drift explainer
            # before the synthetic ``_qf_label`` column gets injected.
            # The explainer needs the raw column values so it can
            # compute per-feature shifts (mean/p99/rate/etc.) directly
            # from the source data — encoded integer matrices are
            # useless for "category top-N" or "p99 of amount".
            current_for_explain = current_pdf.copy()
            past_for_explain = [(label, pdf.copy()) for label, pdf in past_pdfs]

            # Build labeled training set: current=1, past=0.
            current_pdf["_qf_label"] = 1
            labeled_past = []
            for _, pdf in past_pdfs:
                pdf = pdf.copy()
                pdf["_qf_label"] = 0
                labeled_past.append(pdf)

            combined = pd.concat([current_pdf] + labeled_past, ignore_index=True)
            y = combined["_qf_label"].values.astype(int)
            feature_df = combined.drop(columns=["_qf_label"])

            encoded, feature_names, encoding_map = encode_columns(
                feature_df,
                drop_complex=self.drop_complex,
                cardinality_threshold=self.CARDINALITY_THRESHOLD,
            )

            if encoded.shape[1] == 0:
                results.append(self._empty_data_result(
                    self.name, "pattern",
                    "No encodable columns for pattern detection",
                ))
                continue

            n_current = int((y == 1).sum())
            n_past = int((y == 0).sum())
            min_per_class = min(n_current, n_past)
            # StratifiedKFold's hard floor is `min_per_class >= n_splits` — it
            # needs at least one row per class per fold. Don't demand more.
            required = self.cv_folds
            if min_per_class < required:
                results.append(self._empty_data_result(
                    self.name, "pattern",
                    (
                        f"Insufficient samples for {self.cv_folds}-fold CV: "
                        f"min class size={min_per_class}, required>={required} "
                        f"(n_current={n_current}, n_past={n_past})"
                    ),
                ))
                continue

            model = RandomForestClassifier(
                n_estimators=self.n_estimators,
                max_depth=self.max_depth,
                class_weight=self.class_weight,
                random_state=self.random_state,
                n_jobs=1,
            )
            skf = StratifiedKFold(
                n_splits=self.cv_folds,
                shuffle=True,
                random_state=self.random_state,
            )
            # error_score="raise" turns fold-level sklearn failures into real
            # exceptions instead of silently propagating NaN through the mean
            # and rendering a healthy-looking PASS result with NaN details.
            try:
                auc_scores = cross_val_score(
                    model, encoded, y, cv=skf, scoring="roc_auc",
                    error_score="raise",
                )
            except Exception as exc:
                results.append(
                    ValidationResult(
                        validation_name=self.name,
                        validation_type="pattern",
                        severity=Severity.ERROR,
                        message=f"Pattern cross-validation failed: {exc}",
                        validation_base_name=self.name,
                        metric_name="auc",
                        details={
                            "error": str(exc),
                            "n_current": n_current,
                            "n_past": n_past,
                            "cv_folds": int(self.cv_folds),
                        },
                        collected_at=cr.collected_at,
                    )
                )
                continue

            if not np.all(np.isfinite(auc_scores)):
                results.append(
                    ValidationResult(
                        validation_name=self.name,
                        validation_type="pattern",
                        severity=Severity.ERROR,
                        message=(
                            "Pattern cross-validation produced non-finite scores: "
                            f"{[float(s) for s in auc_scores]}"
                        ),
                        validation_base_name=self.name,
                        metric_name="auc",
                        details={
                            "auc_folds": [float(s) for s in auc_scores],
                            "n_current": n_current,
                            "n_past": n_past,
                            "cv_folds": int(self.cv_folds),
                        },
                        collected_at=cr.collected_at,
                    )
                )
                continue

            auc = float(auc_scores.mean())
            auc_std = float(auc_scores.std())

            warning_violated = auc >= self.warning_threshold
            error_violated = auc >= self.error_threshold
            severity = self.determine_severity(
                auc, warning_violated, error_violated
            )

            # Leakage-control tripwire. A near-perfect AUC with no
            # exclude_columns is almost certainly a partition / date /
            # ID column leaking into the classifier rather than a real
            # distribution shift — the validator unit tests demonstrate
            # this (test_without_exclude_columns_date_leaks_trivially).
            # Warn so operators don't silently interpret a known
            # failure mode as drift. We intentionally warn instead of
            # erroring so legitimate near-perfect separations (e.g.
            # integration tests) still surface as ERROR alerts.
            if severity != Severity.PASS and auc > 0.95 and not self.exclude_columns:
                logger.warning(
                    "pattern_check '%s' produced AUC=%.3f with empty exclude_columns. "
                    "This is often caused by a partition / date / ingestion-timestamp / ID "
                    "column leaking into the feature matrix rather than a real drift. "
                    "See docs/pattern_check.md 'Leakage Control' and set "
                    "model.exclude_columns explicitly if this is a legitimate signal.",
                    self.name, auc,
                )

            details: dict[str, Any] = {
                "auc": auc,
                "auc_std": auc_std,
                "auc_folds": [float(s) for s in auc_scores],
                "n_current": n_current,
                "n_past": n_past,
                "n_features": int(encoded.shape[1]),
                "cv_folds": int(self.cv_folds),
            }

            if self.explain and severity != Severity.PASS:
                # SHAP is optional via the `anomaly` extra — treat an import
                # failure as a soft miss (log + mark in details), but surface
                # real runtime failures inside the explain pipeline in the
                # result itself so operators notice their diagnostics are gone.
                try:
                    shap = _import_shap()
                except ImportError as e:
                    logger.info("SHAP unavailable, skipping explanation: %s", e)
                    details["explanation_error"] = f"shap not installed: {e}"
                else:
                    try:
                        full_model = RandomForestClassifier(
                            n_estimators=self.n_estimators,
                            max_depth=self.max_depth,
                            class_weight=self.class_weight,
                            random_state=self.random_state,
                            n_jobs=1,
                        )
                        full_model.fit(encoded, y)
                        explainer = shap.TreeExplainer(full_model)
                        raw = explainer.shap_values(encoded)

                        arr = np.asarray(raw)
                        # SHAP's TreeExplainer returns different shapes across
                        # versions: list of per-class arrays (older), stacked
                        # (n, f, 2) (newer), or a single (n, f) when there is
                        # only one output. Select the positive-class slice.
                        if isinstance(raw, list):
                            positive = raw[1] if len(raw) > 1 else raw[0]
                            positive = np.asarray(positive)
                        elif arr.ndim == 3:
                            positive = (
                                arr[..., 1] if arr.shape[-1] > 1 else arr[..., 0]
                            )
                        else:
                            positive = arr

                        mean_abs_shap = np.abs(positive).mean(axis=0)
                        top_indices = np.argsort(mean_abs_shap)[-5:][::-1]
                        top_features = [
                            {
                                "feature": str(feature_names[i]),
                                "importance": float(mean_abs_shap[i]),
                            }
                            for i in top_indices
                        ]

                        if self.explain_value_drift and top_features:
                            try:
                                # IMPORTANT: feed REAL feature names to
                                # the explainer so its encoding_map
                                # lookup works. Apply redaction AFTER
                                # the explainer returns.
                                drift_entries, mapping_errors = explain_value_drift(
                                    top_features,
                                    current_for_explain,
                                    past_for_explain,
                                    encoding_map,
                                    breakdown_by_slice=self.drift_breakdown_by_slice,
                                )
                                drift_entries = self._redact_drift_entries(
                                    drift_entries,
                                )
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

                        # Redact top_features post-explainer (the
                        # explainer needed the real names; details
                        # don't). Always set the key so the
                        # dashboard / notification consumers see the
                        # redacted view.
                        details["top_contributing_features"] = (
                            self._redact_top_features(top_features, encoding_map)
                        )
                    except Exception as e:
                        logger.warning("SHAP explanation failed: %s", e)
                        details["explanation_error"] = f"{type(e).__name__}: {e}"

            message = f"Pattern AUC: {auc:.3f} (+/-{auc_std:.3f})"
            # Append the top-3 SHAP contributors so the dashboard's
            # message column carries the *why*, not just the AUC.
            # Notifications still render the full top-5 list out of
            # ``details.top_contributing_features`` via
            # ``format_validation_details`` — duplication here is
            # intentional (operators reading the dashboard table
            # shouldn't need to drop into raw storage to learn
            # which feature drove the score).
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
                    validation_type="pattern",
                    severity=severity,
                    message=message,
                    validation_base_name=self.name,
                    metric_name="auc",
                    expected_value={
                        "warning_threshold": float(self.warning_threshold),
                        "error_threshold": float(self.error_threshold),
                    },
                    actual_value=auc,
                    details=details,
                    collected_at=cr.collected_at,
                )
            )

        return results
