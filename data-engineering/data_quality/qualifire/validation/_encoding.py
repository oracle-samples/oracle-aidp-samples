"""Shared column-encoding for sample-based validators.

Both ``IsolationForestValidator`` (``shape``) and
``PatternCheckValidator`` (``pattern``) feed pandas DataFrames into
sklearn estimators and therefore need the same column-to-feature
encoding rules. Keeping a single implementation prevents the two
validators from disagreeing on what a given DataFrame *means* as a
feature matrix — which would produce inconsistent top-feature
attributions for the same run and be a nightmare to debug.

Encoding rules (mirror ``docs/validators/shape.md``):

- Numeric → pass-through, median-impute nulls.
- Boolean → 0/1 ints, null → 0.
- Datetime → Unix seconds (``int64 / 1e9``), null → 0.
- Object/string with cardinality <= ``cardinality_threshold`` → one-hot
  (``pandas.get_dummies`` with ``dummy_na=True``).
- Object/string with higher cardinality → ``LabelEncoder`` over the
  filled string column.
- Unhashable object columns (dicts, lists) → stringify + label-encode,
  or drop when ``drop_complex=True``.
- Other complex dtypes → stringify + label-encode, or drop when
  ``drop_complex=True``.

The encoder also returns an ``encoding_map`` keyed by every entry in
``feature_names``, mapping back to the originating column, kind, and
category (when applicable). Downstream consumers (the value-drift
explainer) need this to surface **value shifts** for the top SHAP
features without doing fragile string-splitting on encoded names.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EncodedFeatureSpec:
    """Reverse-mapping from an encoded feature name to its source.

    Populated by ``encode_columns`` at the encoder branch where the
    original column / category is still in scope, so the
    drift-explainer never has to reverse-engineer pandas naming.

    Attributes:
        source_column: The pre-encoding DataFrame column.
        kind: One of "numeric", "boolean", "datetime", "onehot",
            "label_encoded", "unknown".
        category: For ``kind="onehot"``, the raw category value
            rendered as a string. ``None`` for every other kind.
            A real category whose value is the literal string
            ``"__NULL__"`` carries that string here with
            ``is_null_bin=False`` — the null-bin discriminator is
            ``is_null_bin``, not ``category``.
        is_null_bin: ``True`` only for the column produced by
            ``pd.get_dummies(..., dummy_na=True)`` representing rows
            where the source column was NULL/NaN.
    """

    source_column: str
    kind: str
    category: str | None = None
    is_null_bin: bool = False


def encode_columns(
    df: Any,
    *,
    drop_complex: bool = False,
    cardinality_threshold: int = 20,
) -> tuple[np.ndarray, list[str], dict[str, EncodedFeatureSpec]]:
    """Encode a pandas DataFrame into a float numpy matrix.

    Returns ``(X, feature_names, encoding_map)``. ``feature_names``
    may be longer than ``df.columns`` because one-hot columns expand.
    When every column is unencodable, returns a zero-width matrix
    with the original row count so callers can detect the case with
    ``X.shape[1] == 0``. ``encoding_map`` always has one entry per
    name in ``feature_names``.
    """
    import pandas as pd

    # Fail fast on duplicate labels. `df[col]` on a DataFrame with
    # non-unique columns returns a DataFrame (not a Series), which
    # breaks every dtype branch below with AttributeError. Surface a
    # clear error before the encoder starts instead of letting an
    # opaque pandas traceback bubble up.
    if not df.columns.is_unique:
        dupes = df.columns[df.columns.duplicated()].unique().tolist()
        raise ValueError(
            f"encode_columns requires unique column labels; duplicates: {dupes}"
        )

    encoded_parts: list[Any] = []
    feature_names: list[str] = []
    encoding_map: dict[str, EncodedFeatureSpec] = {}

    for col in df.columns:
        series = df[col]
        dtype = series.dtype

        # Boolean must be checked before numeric. Pandas' nullable
        # ``boolean`` dtype is_numeric_dtype() → True, so the numeric
        # branch would median-impute pd.NA with 0.5 and then raise
        # TypeError when writing back into a boolean container.
        if pd.api.types.is_bool_dtype(dtype):
            encoded_parts.append(
                series.astype("boolean").fillna(False).astype(int).values.reshape(-1, 1)
            )
            feature_names.append(col)
            encoding_map[col] = EncodedFeatureSpec(
                source_column=col, kind="boolean",
            )

        elif pd.api.types.is_numeric_dtype(dtype):
            filled = series.fillna(series.median() if not series.isna().all() else 0)
            # ``to_numpy(dtype=float64)`` instead of ``.values`` so
            # nullable Float64 / Int64 (extension dtypes — what
            # ``spark.toPandas()`` with Arrow returns by default)
            # collapse to a plain numpy float array. ``.values`` on
            # an ExtensionArray returns the extension array itself,
            # which np.hstack later upcasts to object dtype and
            # breaks every downstream sklearn check.
            encoded_parts.append(filled.to_numpy(dtype=np.float64).reshape(-1, 1))
            feature_names.append(col)
            encoding_map[col] = EncodedFeatureSpec(
                source_column=col, kind="numeric",
            )

        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # Capture the NaT mask BEFORE the int64 cast — afterwards
            # NaT becomes iinfo(int64).min and fillna(0) cannot detect
            # it, leaving a huge negative feature value for null
            # datetimes.
            isnat = series.isna().to_numpy()
            # Normalize tz-aware series to UTC naive so the int64 view
            # is always epoch-nanoseconds-UTC. `.astype("datetime64[ns]")`
            # canonicalizes the storage unit (pandas >= 2.0 may store
            # us / ms / s).
            if getattr(series.dt, "tz", None) is not None:
                normalized = series.dt.tz_convert("UTC").dt.tz_localize(None)
            else:
                normalized = series
            try:
                ns = normalized.astype("datetime64[ns]").astype(np.int64)
            except (OverflowError, pd.errors.OutOfBoundsDatetime):
                # Out-of-range for ns precision — fall back to us and
                # scale manually.
                ns = normalized.astype("datetime64[us]").astype(np.int64) * 1000
            seconds = ns // 10**9
            seconds = np.where(isnat, 0, seconds).astype(np.int64)
            encoded_parts.append(seconds.reshape(-1, 1))
            feat_name = f"{col}_timestamp"
            feature_names.append(feat_name)
            encoding_map[feat_name] = EncodedFeatureSpec(
                source_column=col, kind="datetime",
            )

        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            try:
                nunique = series.nunique()
            except TypeError:
                # Unhashable (dict/list) — treat as complex.
                if drop_complex:
                    logger.debug("Dropping unhashable column: %s", col)
                else:
                    try:
                        from sklearn.preprocessing import LabelEncoder
                        le = LabelEncoder()
                        filled = series.astype(str).fillna("__NULL__")
                        encoded_parts.append(le.fit_transform(filled).reshape(-1, 1))
                        feature_names.append(col)
                        encoding_map[col] = EncodedFeatureSpec(
                            source_column=col, kind="label_encoded",
                        )
                    except Exception:
                        logger.debug("Skipping unencodable column: %s", col)
                continue
            if nunique <= cardinality_threshold:
                # Only emit the dummy_na column when the source series
                # actually contains nulls. Without this, ``get_dummies``
                # always produces an empty ``col_nan`` bin which would
                # collide with a real category whose suffix is ``"nan"``
                # — the encoder's uniqueness assertion would then raise
                # on data with no nulls present (codex impl-review R2).
                series_has_null = bool(series.isna().any())
                dummies = pd.get_dummies(
                    series, prefix=col, dummy_na=series_has_null,
                )
                # ``to_numpy(dtype=float64)`` instead of ``.values``:
                # on Arrow-backed ``string`` columns get_dummies
                # returns nullable BooleanDtype, whose ``.values`` is
                # an object array — which would propagate to the
                # full hstack and break sklearn's array checks. Cast
                # to float64 here so every part is a homogeneous
                # numeric numpy array regardless of input dtype.
                encoded_parts.append(dummies.to_numpy(dtype=np.float64))
                # Iterate dummies.columns positionally (label access
                # would return a DataFrame when pandas produces a
                # duplicate column name — e.g. when a real category
                # is the literal string ``"nan"`` AND ``dummy_na=True``
                # adds another ``col_nan`` column). The dummy_na bin
                # is detected via the original series's null mask;
                # its feature_name is rewritten to the unambiguous
                # sentinel ``f"{col}__qf_null"`` so it never collides
                # with a real category whose suffix happens to be
                # the same as pandas's NaN spelling.
                isnull = series.isna()
                isnull_arr = isnull.to_numpy()
                prefix = f"{col}_"
                _NULL_BIN_SUFFIX = "__qf_null"
                for pos, dummy_name in enumerate(dummies.columns.tolist()):
                    bin_col = dummies.iloc[:, pos].astype(bool)
                    bin_arr = bin_col.to_numpy()
                    is_null_bin = bool(
                        series_has_null and np.array_equal(bin_arr, isnull_arr)
                    )
                    cat_str = (
                        dummy_name[len(prefix):]
                        if dummy_name.startswith(prefix)
                        else dummy_name
                    )
                    feature_name = (
                        f"{col}{_NULL_BIN_SUFFIX}" if is_null_bin else dummy_name
                    )
                    feature_names.append(feature_name)
                    encoding_map[feature_name] = EncodedFeatureSpec(
                        source_column=col,
                        kind="onehot",
                        category=cat_str,
                        is_null_bin=is_null_bin,
                    )
            else:
                from sklearn.preprocessing import LabelEncoder

                le = LabelEncoder()
                filled = series.fillna("__NULL__").astype(str)
                encoded_parts.append(le.fit_transform(filled).reshape(-1, 1))
                feature_names.append(col)
                encoding_map[col] = EncodedFeatureSpec(
                    source_column=col, kind="label_encoded",
                )

        elif drop_complex:
            logger.debug("Dropping complex column: %s", col)
            continue
        else:
            try:
                from sklearn.preprocessing import LabelEncoder

                le = LabelEncoder()
                filled = series.astype(str).fillna("__NULL__")
                encoded_parts.append(le.fit_transform(filled).reshape(-1, 1))
                feature_names.append(col)
                encoding_map[col] = EncodedFeatureSpec(
                    source_column=col, kind="label_encoded",
                )
            except Exception:
                logger.debug("Skipping unencodable column: %s", col)

    if not encoded_parts:
        return np.empty((len(df), 0)), [], {}

    # Belt-and-braces: every branch above already produces numpy
    # numeric arrays, but a defensive ``astype(np.float64)`` on the
    # final hstack guarantees sklearn never sees an object dtype
    # even if a future encoder branch slips an extension array in.
    stacked = np.hstack(encoded_parts)
    if stacked.dtype == object:
        stacked = stacked.astype(np.float64)

    if len(set(feature_names)) != len(feature_names):
        from collections import Counter
        dups = [n for n, c in Counter(feature_names).items() if c > 1]
        raise ValueError(
            "encode_columns produced duplicate feature_names "
            f"(prefix collision): {dups}. Rename source columns to "
            "disambiguate (e.g. avoid both `region_us` column and a "
            "`region` column with category `us`)."
        )

    return stacked, feature_names, encoding_map
