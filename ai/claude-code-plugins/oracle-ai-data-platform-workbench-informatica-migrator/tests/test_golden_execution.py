"""Golden-output execution: generated notebooks produce Informatica's rows.

Each case under ``tests/golden/cases/`` is a simulated Informatica export
with seed rows and the target rows Informatica writes for them (see
``tests/golden/harness.py`` for the case format and what is stood in).
A case fails if the notebook raises on Spark or writes different rows.

pyspark is a test-only dependency; without it these tests skip. Run them on
pyspark 3.5 -- the version AIDP runs -- because pyspark 4 accepts call
signatures 3.5 rejects (`F.substring` with a Column position was one).
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark", reason="golden execution tests need pyspark (test-only)")

from tests.conftest_spark import spark  # noqa: E402,F401  (session fixture)
from tests.golden.harness import cases_root, compare, run_case  # noqa: E402

CASES = sorted(p for p in cases_root().iterdir() if (p / "case.json").exists())


@pytest.mark.parametrize("case_dir", CASES, ids=[c.name for c in CASES])
def test_golden_case(spark, case_dir, tmp_path):  # noqa: F811
    expected = json.loads((case_dir / "expected.json").read_text(encoding="utf-8"))
    actual = run_case(spark, case_dir, tmp_path)
    problems = compare(expected, actual)
    assert not problems, "\n".join(problems)
