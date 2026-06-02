"""Regression tests for the shared ``encode_columns`` helper.

These exercise pathological inputs that Codex adversarial review
round 3 flagged as silent-failure / hard-crash paths: duplicate column
labels, pandas nullable ``boolean`` dtype with ``pd.NA``, ``NaT`` in
datetime columns, and tz-aware datetimes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from qualifire.validation._encoding import encode_columns


def test_duplicate_columns_raises_value_error():
    df = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "a"])
    with pytest.raises(ValueError, match="unique column labels"):
        encode_columns(df)


def test_nullable_boolean_with_pd_na_encodes_to_int():
    df = pd.DataFrame({"flag": pd.array([True, pd.NA, False], dtype="boolean")})
    X, names, _ = encode_columns(df)
    assert names == ["flag"]
    assert X.shape == (3, 1)
    # pd.NA must become False → 0, not 0.5 or an int sentinel.
    assert list(X[:, 0]) == [1, 0, 0]


def test_plain_bool_column_encodes_to_int():
    df = pd.DataFrame({"flag": [True, False, True]})
    X, names, _ = encode_columns(df)
    assert names == ["flag"]
    assert list(X[:, 0]) == [1, 0, 1]


def test_naive_datetime_encodes_to_epoch_seconds():
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2024-01-01 00:00:00", "2024-01-02 00:00:00"]),
    })
    X, names, _ = encode_columns(df)
    assert names == ["ts_timestamp"]
    # 2024-01-01T00:00:00Z == 1704067200
    assert int(X[0, 0]) == 1704067200
    assert int(X[1, 0]) == 1704067200 + 86400


def test_nat_becomes_zero_not_huge_negative():
    df = pd.DataFrame({
        "ts": pd.to_datetime(["2024-01-01", pd.NaT, "2024-01-02"]),
    })
    X, _, _ = encode_columns(df)
    # Before the round-3 fix, the middle row was iinfo(int64).min // 1e9,
    # i.e. a huge negative feature value. Contract: null → 0.
    assert int(X[1, 0]) == 0
    assert int(X[0, 0]) == 1704067200


def test_tz_aware_datetime_encodes_as_utc_epoch_seconds():
    # 2024-01-01T00:00:00 US/Eastern == 2024-01-01T05:00:00 UTC
    ts_et = pd.Timestamp("2024-01-01 00:00:00", tz="US/Eastern")
    df = pd.DataFrame({"ts": [ts_et]})
    X, names, _ = encode_columns(df)
    assert names == ["ts_timestamp"]
    expected = int(ts_et.tz_convert("UTC").timestamp())
    assert int(X[0, 0]) == expected


def test_empty_dataframe_returns_zero_width_matrix():
    df = pd.DataFrame()
    X, names, _ = encode_columns(df)
    assert X.shape == (0, 0)
    assert names == []


def test_all_null_numeric_column_does_not_crash():
    df = pd.DataFrame({"x": [np.nan, np.nan, np.nan]})
    X, names, _ = encode_columns(df)
    assert names == ["x"]
    assert X.shape == (3, 1)
    # All-null median is undefined; we fall back to 0.
    assert list(X[:, 0]) == [0, 0, 0]


def test_extension_dtypes_produce_float_matrix_not_object():
    """Arrow-backed string + nullable Float64 columns (what
    ``spark.toPandas()`` returns by default with Arrow enabled)
    must encode to a homogeneous float numpy matrix.

    Regression for the "Cannot interpret 'dtype('O')' as a data
    type" sklearn error: ``.values`` on extension arrays returns
    the extension array itself, which np.hstack upcasts to object
    dtype, breaking every downstream estimator's ``check_array``.
    """
    df = pd.DataFrame({
        "amount": pd.array([1.5, 2.0, None, 3.0, 1.8], dtype="Float64"),
        "channel": pd.array(["email", "paid", None, "organic", "paid"],
                             dtype="string"),
    })
    X, names, _ = encode_columns(df)
    assert X.dtype == np.float64, f"got dtype={X.dtype}, expected float64"
    assert X.shape[0] == 5
    # ``amount`` + 4 one-hot columns (3 distinct strings + NaN).
    assert "amount" in names
    assert any(n.startswith("channel_") for n in names)

    # Final guard: sklearn should accept it without complaint.
    from sklearn.ensemble import RandomForestClassifier
    y = np.array([0, 1, 0, 1, 0])
    RandomForestClassifier(n_estimators=5, random_state=0).fit(X, y)
