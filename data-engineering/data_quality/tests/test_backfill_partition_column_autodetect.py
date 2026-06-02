"""Item 2 (backfill-followups-and-polish): WAPConfig.partition_column
auto-detect from a bare-identifier ``effective_partition_ts``.

Auto-detect uses the stricter ``_BARE_IDENT_RE``
(``^[A-Za-z_][A-Za-z0-9_]*$``) — the permissive ``_SAFE_NAME_RE``
on the existing explicit-set validator stays.

T2.1 — bare partition_ts derives partition_column.
T2.3 — explicit value wins (no override).
T2.4-T2.7 — non-bare forms rejected (constants, hyphens, leading
digits, dot-qualified).
T2.8 — run-level inheritance honoured.
T2.9 — config object not mutated.
T2.10 — non-WAP also consults effective_partition_ts.
"""
from __future__ import annotations

import pytest

from qualifire.core.backfill import (
    _BARE_IDENT_RE,
    _resolve_partition_anchors,
)
from qualifire.core.config import (
    DatasetConfig,
    QualifireConfig,
    WAPConfig,
)
from qualifire.core.exceptions import QualifireConfigError


def _make_run_config(*, partition_ts: str | None = None,
                     partition_step: str | None = None,
                     datasets=None) -> QualifireConfig:
    return QualifireConfig(
        owner="t", bu="b",
        system_table="qf",
        partition_ts=partition_ts,
        partition_step=partition_step,
        datasets=datasets or [],
    )


class TestResolvePartitionAnchors:
    def test_bare_partition_ts_derives_column(self):
        # T2.1: partition_ts='event_date', partition_column unset
        ds = DatasetConfig(
            name="d", partition_ts="event_date",
            partition_step="P1D",
            wap=WAPConfig(target_table="w", sql="SELECT 1"),
            validations=[],
        )
        run = _make_run_config(datasets=[ds])
        col, pt = _resolve_partition_anchors(ds, run)
        assert col == "event_date"
        assert pt == "event_date"

    def test_explicit_partition_column_wins(self):
        # T2.3: both set → explicit wins, auto-detect doesn't override
        ds = DatasetConfig(
            name="d", partition_ts="event_date",
            partition_step="P1D",
            wap=WAPConfig(target_table="w", sql="SELECT 1",
                          partition_column="explicit_col"),
            validations=[],
        )
        run = _make_run_config(datasets=[ds])
        col, _ = _resolve_partition_anchors(ds, run)
        assert col == "explicit_col"

    def test_jinja_constant_rejected(self):
        # T2.2 / T2.4: constant Jinja can't auto-derive
        ds = DatasetConfig(
            name="d", partition_ts="{{ ds }}",
            partition_step="P1D",
            wap=WAPConfig(target_table="w", sql="SELECT 1"),
            validations=[],
        )
        run = _make_run_config(datasets=[ds])
        col, pt = _resolve_partition_anchors(ds, run)
        assert col is None
        # pt is still returned (it's the effective expr) for caller
        # to inspect; just doesn't auto-derive a column.
        assert pt == "{{ ds }}"

    def test_leading_digit_rejected(self):
        # T2.5: leading digit fails the stricter regex
        # (the existing _SAFE_NAME_RE would accept; auto-detect uses
        # _BARE_IDENT_RE which forbids).
        assert _BARE_IDENT_RE.match("123abc") is None
        # But '_SAFE_NAME_RE' would have allowed it. Confirm so
        # the contract is documented.
        from qualifire.core.config import _SAFE_NAME_RE
        assert _SAFE_NAME_RE.match("123abc") is not None

    def test_hyphen_rejected(self):
        # T2.6: hyphen fails the stricter regex
        assert _BARE_IDENT_RE.match("my-col") is None

    def test_dotted_rejected(self):
        # T2.7: dot-qualified fails the stricter regex
        assert _BARE_IDENT_RE.match("t.col") is None

    def test_run_level_inheritance(self):
        # T2.8: dataset-level partition_ts unset, run-level set
        ds = DatasetConfig(
            name="d",
            wap=WAPConfig(target_table="w", sql="SELECT 1"),
            validations=[],
        )
        run = _make_run_config(
            partition_ts="event_date", partition_step="P1D",
            datasets=[ds],
        )
        col, pt = _resolve_partition_anchors(ds, run)
        assert col == "event_date"
        assert pt == "event_date"

    def test_does_not_mutate_wap_config(self):
        # T2.9: post-resolve, ds.wap.partition_column still None
        ds = DatasetConfig(
            name="d", partition_ts="event_date",
            partition_step="P1D",
            wap=WAPConfig(target_table="w", sql="SELECT 1"),
            validations=[],
        )
        run = _make_run_config(datasets=[ds])
        col, _ = _resolve_partition_anchors(ds, run)
        assert col == "event_date"
        # The Pydantic model retains its original (None) value.
        assert ds.wap.partition_column is None

    def test_non_wap_returns_effective_partition_ts(self):
        # T2.10: non-WAP path returns (None, effective_partition_ts).
        ds = DatasetConfig(
            name="d", table="t",
            partition_step="P1D",
            validations=[],
        )
        run = _make_run_config(
            partition_ts="event_date", partition_step="P1D",
            datasets=[ds],
        )
        col, pt = _resolve_partition_anchors(ds, run)
        assert col is None  # non-WAP has no partition_column
        assert pt == "event_date"  # run-level inherited
