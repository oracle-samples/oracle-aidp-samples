import pytest

from nightshift.merge_adapt import adapt_merge


def test_adapts_plain_merge_and_infers_keys():
    sql = """MERGE INTO sales.orders_silver t USING sales.orders_bronze s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.updated_at >= t.updated_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;"""
    a = adapt_merge(sql)
    assert a.target == "sales.orders_silver" and a.source == "sales.orders_bronze"
    assert a.target_alias == "t" and a.source_alias == "s"
    assert a.key_cols == ("order_id",)
    assert a.ts_guard == "s.updated_at >= t.updated_at" and a.has_ordering_guard
    assert a.template.startswith("MERGE INTO {target} t USING {batch} s")
    assert "sales." not in a.template and a.template.endswith("INSERT *")


def test_composite_key_without_guard_and_as_aliases():
    sql = ("merge into demo.hr.employee_summary as tgt using demo.hr.employee_staging as src "
           "on tgt.department_name = src.department_name and src.region = tgt.region "
           "when matched then update set tgt.employee_count = src.employee_count "
           "when not matched then insert *")
    a = adapt_merge(sql)
    assert a.key_cols == ("department_name", "region")
    assert a.ts_guard is None and not a.has_ordering_guard
    assert a.template.startswith("merge into {target} as tgt using {batch} as src")


def test_backticks_and_no_aliases():
    sql = "MERGE INTO `cat`.`sch`.`tbl` USING `cat`.`sch`.`src` ON tbl.id = src.id WHEN NOT MATCHED THEN INSERT *"
    a = adapt_merge(sql)
    assert a.target == "`cat`.`sch`.`tbl`" and a.target_alias is None and a.source_alias is None
    assert a.key_cols == ("id",)
    assert a.template == "MERGE INTO {target} USING {batch} ON tbl.id = src.id WHEN NOT MATCHED THEN INSERT *"


def test_rejects_subquery_source_and_non_merge():
    with pytest.raises(ValueError):
        adapt_merge("MERGE INTO t USING (SELECT * FROM s) s ON t.id = s.id WHEN MATCHED THEN DELETE")
    with pytest.raises(ValueError):
        adapt_merge("INSERT INTO t SELECT * FROM s")


def test_template_works_with_harness_placeholders():
    from nightshift import replay
    a = adapt_merge("MERGE INTO p.t t USING p.s s ON t.k = s.k WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    plan = replay.spark_sql_plan("p.t", "sbx", a.template, list(a.key_cols), "ts", ["duplicate"])
    assert any("MERGE INTO sbx." in s for s in plan)
