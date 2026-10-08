"""A copy that could not LOOK at its target never records the target absent.

`_target_exists` wrapped DESCRIBE in `except Exception: return False`, so any
error -- a metastore timeout, a persistent INSUFFICIENT_PERMISSIONS -- on a
table 01 had just created became `target_missing`, and the error text was
thrown away. `target_missing` was not counted as a failure, so 02 exited 0
with 0 rows copied. In 03, `created` + `target_missing` + present in the
catalog fell through to STRUCTURE_ONLY, which is not a problem verdict.
Reproduced with the real 02/03 main():

    [copy] SALES.ITEMS: target_missing (? row(s))          02 exit 0
    [reconcile] totals: {MIGRATED_VERIFIED: 1, STRUCTURE_ONLY: 1}   03 exit 0
    No table is in a problem state ...
    | ITEMS | created | target_missing | yes | STRUCTURE_ONLY | ... does not exist

The permission variant stayed silent on every re-run.
"""
import json

import pytest

from test_data_migration_scripts import (
    _CatalogSpark, _FakeSource, _copy_run, _inject_spark, _load, _write_estate)

_TIMEOUT = ("[HIVE_METASTORE] org.apache.thrift.transport.TTransportException:"
            " java.net.SocketTimeoutException: Read timed out")
_DENIED = "[INSUFFICIENT_PERMISSIONS] User does not have USE on lake.SALES"


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


@pytest.fixture(scope="module")
def reconcile():
    return _load("03_reconcile")


class _DescribeFails(_CatalogSpark):
    """DESCRIBE of `fqn` raises `error`; everything else is the catalog."""

    def __init__(self, catalog, fqn, error):
        super().__init__(catalog)
        self.fqn, self.error = fqn, error

    def sql(self, statement):
        if statement.lower().startswith("describe") and self.fqn in statement:
            self.statements.append(" ".join(statement.split()))
            raise RuntimeError(self.error)
        return super().sql(statement)


@pytest.mark.parametrize("error", [_TIMEOUT, _DENIED])
def test_a_describe_that_errors_is_a_failure_with_its_text(copy_schema, error):
    source = _FakeSource()
    source.spark = _DescribeFails({}, "`lake`.`s`.`items`", error)
    out = copy_schema.copy_table(source, "SALES", "ITEMS",
                                 "`lake`.`s`.`items`", mode="append",
                                 verify="counts", retries=0, retry_base_delay=0)
    assert out["status"] == "failed", out
    assert error.split("]")[0] + "]" in out["reason"], \
        "the error text is the finding; it was discarded"
    assert not any("INSERT" in s.upper() for s in source.spark.statements)


@pytest.mark.parametrize("error", [
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `lake`.`s`.`t` cannot be found.",
    "[SCHEMA_NOT_FOUND] The schema `lake`.`s` cannot be found.",
    "org.apache.spark.sql.catalyst.analysis.NoSuchTableException: t"])
def test_spark_saying_not_there_is_still_target_missing(copy_schema, error):
    source = _FakeSource()
    source.spark = _DescribeFails({}, "`lake`.`s`.`t`", error)
    out = copy_schema.copy_table(source, "SALES", "T", "`lake`.`s`.`t`",
                                 mode="append", verify="counts",
                                 retries=0, retry_base_delay=0)
    assert out["status"] == "target_missing"


@pytest.mark.parametrize("error", [_TIMEOUT, _DENIED])
def test_the_copy_run_exits_one_when_it_could_not_look(
        copy_schema, monkeypatch, tmp_path, error):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS", "ITEMS"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"ORDERS": {"status": "created"},
                     "ITEMS": {"status": "created"}}}), encoding="utf-8")
    catalog = {f"`{c}`.`SALES`.`{t}`": [("A", "string")]
               for c in ("ext", "lake") for t in ("ORDERS", "ITEMS")}
    spark = _DescribeFails(catalog, "`lake`.`SALES`.`ITEMS`", error)
    spark.counts = {"`ext`.`SALES`.`ORDERS`": 2, "`ext`.`SALES`.`ITEMS`": 3}
    rc, report = _copy_run(monkeypatch, reports, spark)
    assert report["tables"]["ORDERS"]["status"] == "verified"
    items = report["tables"]["ITEMS"]
    assert items["status"] == "failed", items
    assert error[:20] in items["reason"]
    assert rc == 1


def test_reconcile_flags_target_missing_for_a_table_the_catalog_lists(
        reconcile, monkeypatch, tmp_path, capsys):
    reports = _write_estate(tmp_path / "reports", {"SALES": ["ORDERS", "ITEMS"]})
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "objects": {"ORDERS": {"status": "created"},
                     "ITEMS": {"status": "created"}}}), encoding="utf-8")
    (reports / "copy_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES",
         "tables": {"ORDERS": {"status": "verified"},
                    "ITEMS": {"status": "target_missing",
                              "reason": "`lake`.`SALES`.`ITEMS` does not "
                                        "exist, so there is nothing to copy "
                                        "into."}}}), encoding="utf-8")
    _inject_spark(monkeypatch, _CatalogSpark(
        {"`lake`.`SALES`.`ORDERS`": [("A", "string")],
         "`lake`.`SALES`.`ITEMS`": [("A", "string")]}))
    rc = _load("03_reconcile").main(["--target-catalog", "lake",
                                     "--reports-dir", str(reports)])
    rec = json.loads((reports / "reconciliation.json").read_text(encoding="utf-8"))
    items = next(t for t in rec["schemas"][0]["tables"] if t["table"] == "ITEMS")
    assert items["exists_in_target"] is True
    assert items["verdict"] in reconcile.PROBLEM_VERDICTS, items
    assert "catalog lists it" in items["reason"], items["reason"]
    md = (reports / "MIGRATION_REPORT.md").read_text(encoding="utf-8")
    assert "No table is in a problem state" not in md
    assert rc == 1
