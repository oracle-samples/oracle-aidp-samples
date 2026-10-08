"""Create structure through the AIDP catalog REST API instead of SQL.

`POST /workspaces/<ws>/sql/execute` does not exist (404, verified live), so the
SQL transport cannot work. Schema/table/view CRUD is GA on the catalog API, and
for a STRUCTURE-ONLY clone that is strictly better: no Spark cluster is needed
at all -- which matters, since every cluster in the target environment is
stopped and starting one costs money.

The field shape below was read off a real table in the target environment
(`lake.bronze.basic_tab`), not from documentation.
"""
import pytest

from target.catalog_api import (
    UnmappableFieldType, build_schema_body, build_table_body, build_view_body,
    field_from_spark_type,
)


# --------------------------------------------------------- type -> field

@pytest.mark.parametrize("spark,expected", [
    ("STRING", {"fieldType": "string"}),
    ("BOOLEAN", {"fieldType": "boolean"}),
    ("DATE", {"fieldType": "date"}),
    ("DOUBLE", {"fieldType": "double"}),
    ("BINARY", {"fieldType": "binary"}),
    ("TIMESTAMP", {"fieldType": "timestamp"}),
])
def test_simple_types_map_to_a_field_type(spark, expected):
    f = field_from_spark_type("C", spark)
    assert f["fieldName"] == "C"
    assert f["fieldType"] == expected["fieldType"]


def test_decimal_carries_precision_and_scale_as_strings():
    # The live table reports fieldPrecision/fieldScale as STRINGS.
    f = field_from_spark_type("AMOUNT", "DECIMAL(38,2)")
    assert f["fieldType"] == "decimal"
    assert f["fieldPrecision"] == "38"
    assert f["fieldScale"] == "2"
    assert isinstance(f["fieldPrecision"], str)


def test_decimal_without_a_scale_defaults_to_zero_scale():
    f = field_from_spark_type("ID", "DECIMAL(38)")
    assert (f["fieldPrecision"], f["fieldScale"]) == ("38", "0")


def test_an_unmappable_type_is_refused_not_guessed():
    with pytest.raises(UnmappableFieldType):
        field_from_spark_type("C", "STRUCT<a:INT>")
    with pytest.raises(UnmappableFieldType):
        field_from_spark_type("C", None)


# ------------------------------------------------------------- bodies

def test_schema_body_names_the_catalog_and_schema():
    body = build_schema_body("lake", "TEST_DB")
    assert body["displayName"] == "TEST_DB"
    assert body["catalogName"] == "lake"


def test_table_body_is_managed_delta_with_a_qualified_schema_key():
    cols = [{"name": "ORDER_ID", "type": "DECIMAL(38,0)"},
            {"name": "NOTE", "type": "STRING"}]
    body = build_table_body("lake", "TEST_DB", "ORDERS", cols)
    assert body["displayName"] == "ORDERS"
    assert body["catalogKey"] == "lake"
    # A bare schemaKey returns 400 InvalidParameter -- it must be qualified.
    assert body["schemaKey"] == "lake.TEST_DB"
    assert body["tableType"] == "MANAGED"
    assert body["managedTableDefinition"]["managedTableDataFormat"] == "DELTA"
    assert [f["fieldName"] for f in body["tableFields"]] == ["ORDER_ID", "NOTE"]


def test_table_body_preserves_column_order():
    cols = [{"name": f"C{i}", "type": "STRING"} for i in range(6)]
    body = build_table_body("lake", "S", "T", cols)
    assert [f["fieldName"] for f in body["tableFields"]] == \
        [c["name"] for c in cols]


def test_a_table_with_no_columns_is_refused():
    with pytest.raises(ValueError):
        build_table_body("lake", "S", "T", [])


def test_no_data_format_other_than_delta_is_emitted():
    body = build_table_body("lake", "S", "T", [{"name": "A", "type": "STRING"}])
    assert "CSV" not in repr(body).upper()


def test_view_body_carries_the_view_text_and_fields():
    body = build_view_body("lake", "TEST_DB", "V_ORDERS",
                           "select 1 as a", [{"name": "A", "type": "STRING"}])
    assert body["displayName"] == "V_ORDERS"
    assert body["schemaKey"] == "lake.TEST_DB"
    assert body["viewText"] == "select 1 as a"
    assert [f["fieldName"] for f in body["viewFields"]] == ["A"]


def test_a_view_with_no_text_is_refused():
    with pytest.raises(ValueError):
        build_view_body("lake", "S", "V", "", [])


def test_bodies_never_contain_row_data():
    # Structure only, always.
    body = build_table_body("lake", "S", "T", [{"name": "A", "type": "STRING"}])
    for banned in ("INSERT", "VALUES", "SELECT *", "COPY"):
        assert banned not in repr(body).upper()


# ==========================================================================
# `timestamp_ntz` is NOT a valid catalog fieldType. Verified live: the POST
# returns 202 Accepted and the async create then fails SILENTLY -- the table
# never appears and nothing reports why. Every other standard type
# (timestamp, date, boolean, binary, double, bigint, int, float) is accepted.
#
# This collides with a deliberate fidelity choice: Snowflake TIMESTAMP_NTZ is
# mapped to Spark TIMESTAMP_NTZ precisely because bare TIMESTAMP is
# session-timezone-dependent, and the wrong choice shifts every timestamp. So
# it is a decision, not a default.
# ==========================================================================

def test_timestamp_ntz_is_refused_by_default_on_this_transport():
    with pytest.raises(UnmappableFieldType) as exc:
        field_from_spark_type("TS", "TIMESTAMP_NTZ")
    assert "timestamp_ntz" in str(exc.value).lower()
    assert "cannot be sent as planned" in str(exc.value)
    assert "--timestamp-ntz timestamp" in str(exc.value), "names the decision"


def test_timestamp_ntz_can_be_downgraded_explicitly():
    f = field_from_spark_type("TS", "TIMESTAMP_NTZ",
                              timestamp_ntz_as_timestamp=True)
    assert f["fieldType"] == "timestamp"


def test_the_downgrade_is_recorded_on_the_field():
    f = field_from_spark_type("TS", "TIMESTAMP_NTZ",
                              timestamp_ntz_as_timestamp=True)
    assert "timezone" in (f.get("fieldDescription") or "").lower()


def test_plain_timestamp_is_unaffected():
    assert field_from_spark_type("TS", "TIMESTAMP")["fieldType"] == "timestamp"


def test_the_table_body_passes_the_flag_through():
    cols = [{"name": "TS", "type": "TIMESTAMP_NTZ"}]
    with pytest.raises(UnmappableFieldType):
        build_table_body("lake", "s", "t", cols)
    body = build_table_body("lake", "s", "t", cols,
                            timestamp_ntz_as_timestamp=True)
    assert body["tableFields"][0]["fieldType"] == "timestamp"
