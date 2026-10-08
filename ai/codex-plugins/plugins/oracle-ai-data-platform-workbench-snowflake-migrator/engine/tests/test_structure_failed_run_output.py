"""A failed S10 run writes its own S10_structure.json.

01_create_structure returned at `if failures: return 1` -- and at the
"created 0 table(s)" refusal -- before write_step_output, so a structure
run that ended in TYPE DRIFT left report/output holding the PREVIOUS run's
S10: `failures: 0`, `{'created': 1}`, the old `written_at`. It sat next to
the CLI's snapshot with exit_code 1 and the updated structure report, and
the step outputs contradicted the latest run. The payload's `failures`
field could never be non-zero. 00, 02 and 03 all write their step output
before they return a failure.
"""
import json

from test_data_migration_scripts import (
    _CatalogSpark, _inject_spark, _load, _write_estate)

_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"}]
_FQN = "`lake`.`SALES`.`ORDERS`"


def _run(monkeypatch, reports, out, spark, *extra):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES",
         "--reports-dir", str(reports), "--output-dir", str(out), *extra])


def _s10(out):
    return json.loads((out / "S10_structure.json").read_text(encoding="utf-8"))


def test_a_type_drift_rerun_replaces_the_earlier_s10(monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            plan={("SALES", "ORDERS"): _COLS})
    out = tmp_path / "report" / "output"
    spark = _CatalogSpark()
    assert _run(monkeypatch, reports, out, spark) == 0
    first = _s10(out)
    assert first["failures"] == 0 and first["outcome"] == "ok"

    # The table is changed out of band; the forced re-check finds drift.
    spark.catalog[_FQN] = [("ID", "string")]
    assert _run(monkeypatch, reports, out, spark, "--force") == 1
    second = _s10(out)
    assert second["written_at"] > first["written_at"], \
        "the failed run wrote its own S10"
    assert second["failures"] == 1
    assert second["outcome"] == "failed"
    assert second["schemas"]["SALES"] == {"type_drift": 1}


def test_a_run_that_created_nothing_says_so_in_s10(monkeypatch, tmp_path):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS"]},
                            plan={("OTHER", "ORDERS"): _COLS})
    out = tmp_path / "report" / "output"
    assert _run(monkeypatch, reports, out, _CatalogSpark()) == 1
    s10 = _s10(out)
    assert s10["outcome"] == "created_nothing"
    assert s10["schemas"]["SALES"] == {"not_in_plan": 1}
    assert "do not overlap" in s10["error"]
