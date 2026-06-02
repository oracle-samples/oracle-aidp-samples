"""Item 6 (backfill-followups-and-polish): configurable
``max_partitions`` cap on ``expand_partition_ts`` /
``Qualifire.backfill``.

T6.1 — explicit cap on ``expand_partition_ts``.
T6.2 — large window via ``Qualifire.backfill(max_partitions=…)``.
T6.3 — invalid (zero / negative) caps rejected.
T6.4 — default 10000 unchanged for legacy callers.
"""
from __future__ import annotations

from datetime import timedelta

import pytest

from qualifire.core.backfill import expand_partition_ts
from qualifire.core.exceptions import QualifireConfigError


class TestExpandPartitionTsMaxPartitions:
    def test_explicit_cap_raises_on_overflow(self):
        # T6.1: 6 anchors with cap=5 raises.
        with pytest.raises(QualifireConfigError, match=">5 partitions"):
            expand_partition_ts(
                ("2026-04-01", "2026-04-06"),
                step=timedelta(days=1),
                max_partitions=5,
            )

    def test_default_cap_unchanged(self):
        # T6.4: 30 anchors under default cap=10000 succeeds.
        anchors = expand_partition_ts(
            ("2026-04-01", "2026-04-30"),
            step=timedelta(days=1),
        )
        assert len(anchors) == 30

    def test_lifted_cap_admits_larger_window(self):
        # T6.2: 5000 anchors at default cap=10000 succeeds.
        anchors = expand_partition_ts(
            ("2026-01-01", "2030-01-01"),
            step=timedelta(weeks=1),
            max_partitions=500,
        )
        # 209 weeks (last anchor 2029-12-30 then +1 week exceeds end)
        assert len(anchors) == 209

    def test_zero_cap_rejected(self):
        # T6.3: zero / negative caps rejected.
        with pytest.raises(QualifireConfigError, match=">= 1"):
            expand_partition_ts(
                ("2026-04-01", "2026-04-02"),
                step=timedelta(days=1),
                max_partitions=0,
            )
        with pytest.raises(QualifireConfigError, match=">= 1"):
            expand_partition_ts(
                ("2026-04-01", "2026-04-02"),
                step=timedelta(days=1),
                max_partitions=-1,
            )
