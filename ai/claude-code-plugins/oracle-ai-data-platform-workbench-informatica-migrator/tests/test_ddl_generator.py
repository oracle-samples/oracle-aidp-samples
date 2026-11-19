"""Target DDL comes from the export's declared types, not from a DataFrame.

The runtime create-if-missing guard makes a first run complete but takes its
column types from the batch, so ``NUMBER(10,0)`` lands as ``BIGINT`` and the
target's declared precision is gone without anything failing. These tests
pin the faithful translation, and the last one executes the generated DDL on
a real Spark session rather than inspecting the string -- a CREATE TABLE
that looks right and does not parse is the failure mode a string assertion
cannot see.
"""
from __future__ import annotations

import pytest

from infa2aidp.generators.ddl_generator import (
    _DDL_TYPE,
    _ddl_type,
    mapping_ddl,
    target_ddl,
)
from infa2aidp.models import FieldMapping, TargetDefinition, TransformationField
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

from tests.conftest_spark import spark  # noqa: F401


def _target(fields, name="TGT", db="DW_DB", owner="ANALYTICS", table="TGT_T"):
    return TargetDefinition(name=name, db_name=db, owner=owner,
                            table_name=table, fields=fields)


class _Mapping:
    def __init__(self, transformations=(), targets=(), name="m_test"):
        self.name = name
        self.transformations = list(transformations)
        self.targets = list(targets)


class _Agg:
    """Minimal stand-in for an Aggregator transformation."""

    type = "AGGREGATOR"

    def __init__(self, fields):
        self.name = "AGG_X"
        self.fields = fields


# ---------------------------------------------------------------------------
# Faithful type translation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("declared,prec,scale,expected", [
    ("NUMBER", 10, 0, "DECIMAL(10,0)"),     # the case that became BIGINT
    ("NUMBER", 12, 2, "DECIMAL(12,2)"),
    ("VARCHAR2", 100, 0, "STRING"),
    ("DATE", 19, 0, "TIMESTAMP"),
    ("TIMESTAMP", 26, 6, "TIMESTAMP"),
    ("FLOAT", 0, 0, "DOUBLE"),
    ("BLOB", 0, 0, "BINARY"),
    ("BIT", 0, 0, "BOOLEAN"),
    ("INTEGER", 0, 0, "INT"),
])
def test_declared_type_maps_to_its_faithful_spark_type(declared, prec, scale, expected):
    got, note = _ddl_type(declared, prec, scale)
    assert got == expected
    assert note is None


def test_precision_is_preserved_rather_than_widened_to_bigint():
    """The specific loss this module exists to prevent."""
    assert _ddl_type("NUMBER", 10, 0)[0] == "DECIMAL(10,0)"
    assert "BIGINT" not in _ddl_type("NUMBER", 10, 0)[0]


def test_missing_precision_falls_back_to_the_documented_maximum():
    got, note = _ddl_type("NUMBER", 0, 4)
    assert got == "DECIMAL(38,4)"
    assert "no PRECISION" in note


def test_precision_above_deltas_maximum_is_clamped_and_said_so():
    got, note = _ddl_type("NUMBER", 45, 2)
    assert got == "DECIMAL(38,2)"
    assert "exceeds Delta's maximum" in note and "truncate" in note


def test_scale_larger_than_precision_is_clamped_and_said_so():
    got, note = _ddl_type("NUMBER", 5, 9)
    assert got == "DECIMAL(5,5)"
    assert "exceeds precision" in note


def test_an_unknown_datatype_becomes_string_with_a_note_not_a_guess():
    """A wrong numeric type truncates silently; STRING cannot lose digits."""
    got, note = _ddl_type("SDO_GEOMETRY", 0, 0)
    assert got == "STRING"
    assert "unknown datatype" in note


def test_a_missing_datatype_is_reported_too():
    got, note = _ddl_type("", 0, 0)
    assert got == "STRING" and "no DATATYPE" in note


# ---------------------------------------------------------------------------
# Aggregate widening is reported, not silently resolved
# ---------------------------------------------------------------------------

def test_aggregate_fed_columns_are_flagged_and_min_max_are_not():
    """COUNT gives BIGINT and SUM/AVG widen a decimal, so the declared type
    will not match what the notebook computes. MIN and MAX return their
    argument's type, so flagging them would be noise."""
    agg = _Agg([
        TransformationField(name="EMPLOYEE_COUNT", expression="COUNT(EMPLOYEE_ID)"),
        TransformationField(name="TOTAL_SALARY", expression="SUM(SALARY)"),
        TransformationField(name="AVG_SALARY", expression="AVG(SALARY)"),
        TransformationField(name="MIN_SALARY", expression="MIN(SALARY)"),
        TransformationField(name="MAX_SALARY", expression="MAX(SALARY)"),
    ])
    tgt = _target([
        FieldMapping(source_field="x", target_field=n, datatype="NUMBER",
                     precision=12, scale=2)
        for n in ("EMPLOYEE_COUNT", "TOTAL_SALARY", "AVG_SALARY",
                  "MIN_SALARY", "MAX_SALARY")
    ])
    d = target_ddl(_Mapping([agg], [tgt]), tgt)
    flagged = {w.split(" ")[0] for w in d.widened}
    assert flagged == {"EMPLOYEE_COUNT", "TOTAL_SALARY", "AVG_SALARY"}
    assert "REVIEW" in d.sql and "MERGE" in d.sql


def test_a_target_with_no_aggregate_carries_no_review_block():
    tgt = _target([FieldMapping(source_field="a", target_field="A",
                                datatype="VARCHAR2", precision=10)])
    d = target_ddl(_Mapping([], [tgt]), tgt)
    assert d.widened == []
    assert "REVIEW" not in d.sql
    assert not d.has_warnings


# ---------------------------------------------------------------------------
# Shape of the emitted statement
# ---------------------------------------------------------------------------

def test_not_null_only_where_the_export_says_so():
    """Defaulting to NOT NULL would reject rows the source produced."""
    tgt = _target([
        FieldMapping(source_field="a", target_field="K", datatype="VARCHAR2",
                     precision=10, nullable=False),
        FieldMapping(source_field="b", target_field="V", datatype="VARCHAR2",
                     precision=10, nullable=True),
    ])
    sql = target_ddl(_Mapping([], [tgt]), tgt).sql
    assert "K STRING NOT NULL" in sql
    assert "V STRING" in sql and "V STRING NOT NULL" not in sql


def test_the_table_is_addressed_the_way_the_notebook_addresses_it():
    tgt = _target([FieldMapping(source_field="a", target_field="A",
                                datatype="VARCHAR2", precision=1)])
    assert target_ddl(_Mapping([], [tgt]), tgt).table == "DW_DB.ANALYTICS.TGT_T"
    two = target_ddl(_Mapping([], [tgt]), tgt, catalog_qualified=False)
    assert two.table == "ANALYTICS.TGT_T"


def test_create_table_is_if_not_exists_and_delta():
    tgt = _target([FieldMapping(source_field="a", target_field="A",
                                datatype="VARCHAR2", precision=1)])
    sql = target_ddl(_Mapping([], [tgt]), tgt).sql
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "USING delta" in sql


def test_a_target_with_no_columns_is_reported_not_emitted_empty():
    tgt = _target([])
    d = target_ddl(_Mapping([], [tgt]), tgt)
    assert "CREATE TABLE" not in d.sql
    assert any("no column definitions" in n for n in d.notes)


# ---------------------------------------------------------------------------
# Drift guard against the comparator's parallel table
# ---------------------------------------------------------------------------

def test_every_base_type_the_comparator_knows_is_known_here_too():
    """Two tables that must agree are a drift risk.

    ``reconciler.comparators._TYPE_MAP`` maps the same base types to Spark
    type class names for schema comparison. If a type is added there and not
    here, that column silently becomes STRING in the DDL.
    """
    from infa2aidp.reconciler.comparators import _TYPE_MAP

    # Compare BASE types: both modules strip a parenthesised precision
    # before lookup, so a key like "TINYINT(1)" is unreachable in either
    # table and comparing raw keys would demand dead entries here.
    base = lambda d: {k.split("(")[0].strip() for k in d}
    missing = sorted(base(_TYPE_MAP) - base(_DDL_TYPE))
    assert not missing, (
        f"known to the comparator but not to the DDL generator: {missing}. "
        "Each would silently become STRING in the generated DDL."
    )


# ---------------------------------------------------------------------------
# The whole corpus, and the DDL actually executing
# ---------------------------------------------------------------------------

def test_the_corpus_fixture_produces_the_declared_types():
    m = InformaticaXMLParser().parse(
        "tests/fixtures/powercenter/joiner_no_master_flag.xml").mappings[0]
    ddls = mapping_ddl(m)
    assert len(ddls) == 1
    sql = ddls[0].sql
    # SALARY-derived columns are NUMBER(12,2)/NUMBER(15,2) in the export
    assert "MIN_SALARY DECIMAL(12,2)" in sql
    assert "TOTAL_SALARY DECIMAL(15,2)" in sql
    assert "TEAM_NAME STRING" in sql


def test_generated_ddl_parses_and_creates_a_table_on_real_spark(spark, tmp_path):  # noqa: F811
    """Execute it, do not just read it.

    A CREATE TABLE can look correct and fail to parse -- a clamped decimal,
    a reserved word, a stray comma. This runs every corpus target's DDL
    against a real Spark session. It uses plain Parquet tables in a scratch
    warehouse because `USING delta` needs the Delta jars, which the offline
    test environment does not carry; the column type list is what is being
    checked here, and it is identical either way.
    """
    import re as _re

    parser = InformaticaXMLParser()
    statements = []
    for fx in ("powercenter/joiner_no_master_flag.xml", "corpus/orders_transform.xml",
               "corpus/star_schema_fact.xml", "corpus/scd_type2.xml"):
        try:
            res = parser.parse(f"tests/fixtures/{fx}")
        except Exception:
            continue
        for m in res.mappings:
            for d in mapping_ddl(m):
                if "CREATE TABLE" in d.sql:
                    statements.append(d.sql)
    assert statements, "no DDL produced from the corpus"

    spark.sql("CREATE DATABASE IF NOT EXISTS ddl_probe")
    created = 0
    for sql in statements:
        body = "\n".join(l for l in sql.splitlines() if not l.startswith("--"))
        # scratch database, and Parquet instead of Delta (see docstring)
        body = _re.sub(r"CREATE TABLE IF NOT EXISTS \S+",
                       f"CREATE TABLE IF NOT EXISTS ddl_probe.t{created}", body)
        body = body.replace("USING delta", "USING parquet").rstrip().rstrip(";")
        spark.sql(body)          # raises on a parse or type error
        cols = spark.table(f"ddl_probe.t{created}").schema
        assert len(cols) > 0
        created += 1
    assert created == len(statements)


# ---------------------------------------------------------------------------
# A Sequence Generator's NEXTVAL is BIGINT whatever the target declares
# ---------------------------------------------------------------------------

from infa2aidp.models import Connector  # noqa: E402


class _Seq:
    """Minimal stand-in for a Sequence Generator transformation."""

    type = "Sequence Generator"

    def __init__(self, name="SEQ_EMP_SK", port="NEXTVAL"):
        self.name = name
        self.fields = [TransformationField(name=port)]


def _scd2_mapping(connectors, target):
    m = _Mapping([_Seq()], [target], name="m_dim_history_fixture")
    m.connectors = connectors
    return m


def test_a_sequence_fed_target_column_is_flagged():
    """The live failure from PR #11.

    infa_compat.sequence(...).assign() ends in value.cast("long"), so the
    key column arrives as bigint while the export declares NUMBER(10,0).
    Delta refuses the MERGE with DELTA_FAILED_TO_MERGE_FIELDS. The DDL
    generator can see this from the export, so it says so.
    """
    tgt = _target([
        FieldMapping(source_field="x", target_field="EMP_SK",
                     datatype="NUMBER", precision=10, scale=0),
        FieldMapping(source_field="y", target_field="NAME",
                     datatype="VARCHAR2", precision=50),
    ], name="TGT_EMPLOYEES_HIST", table="TGT_EMPLOYEES_HIST")
    conns = [Connector(from_instance="SEQ_EMP_SK", from_field="NEXTVAL",
                       to_instance="TGT_EMPLOYEES_HIST", to_field="EMP_SK")]
    d = target_ddl(_scd2_mapping(conns, tgt), tgt)

    assert any(w.startswith("EMP_SK (SEQUENCE") for w in d.widened), d.widened
    assert not any(w.startswith("NAME") for w in d.widened)
    assert "NEXTVAL is always BIGINT" in d.sql
    assert "DELTA_FAILED_TO_MERGE_FIELDS" in d.sql


def test_the_sequence_is_traced_through_a_rename_not_matched_by_name():
    """Follows CONNECTORs rather than matching port names.

    The NEXTVAL goes through an Expression and reaches the target under a
    different name. A name match would miss it -- the same class of bug as
    the _M/_D suffix guessing the Joiner used to do.
    """
    tgt = _target([FieldMapping(source_field="x", target_field="SURROGATE_KEY",
                                datatype="NUMBER", precision=10, scale=0)],
                  name="TGT_DIM", table="TGT_DIM")
    conns = [
        Connector(from_instance="SEQ_EMP_SK", from_field="NEXTVAL",
                  to_instance="EXP_KEYS", to_field="SK_IN"),
        Connector(from_instance="EXP_KEYS", from_field="SK_IN",
                  to_instance="TGT_DIM", to_field="SURROGATE_KEY"),
    ]
    d = target_ddl(_scd2_mapping(conns, tgt), tgt)
    assert any(w.startswith("SURROGATE_KEY (SEQUENCE") for w in d.widened), d.widened


def test_a_sequence_feeding_a_different_target_is_not_flagged_here():
    """Each target's DDL reports only its own columns."""
    other = _target([FieldMapping(source_field="x", target_field="EMP_SK",
                                  datatype="NUMBER", precision=10, scale=0)],
                    name="TGT_OTHER", table="TGT_OTHER")
    conns = [Connector(from_instance="SEQ_EMP_SK", from_field="NEXTVAL",
                       to_instance="TGT_EMPLOYEES_HIST", to_field="EMP_SK")]
    d = target_ddl(_scd2_mapping(conns, other), other)
    assert d.widened == []


def test_a_mapping_with_no_connectors_does_not_crash_or_guess():
    tgt = _target([FieldMapping(source_field="x", target_field="EMP_SK",
                                datatype="NUMBER", precision=10, scale=0)])
    m = _Mapping([_Seq()], [tgt])
    m.connectors = []
    assert target_ddl(m, tgt).widened == []


def test_the_golden_scd2_case_flags_its_surrogate_key():
    """Against the case that failed on the cluster, not a hand-built fixture.

    PR #11 found DELTA_FAILED_TO_MERGE_FIELDS on an SCD2 surrogate key at
    run time. c10_scd2_history is that shape, and this asserts the DDL
    generator predicts it from the export -- which is the whole point of
    emitting DDL rather than letting the runtime guard infer types.
    """
    import glob
    cases = glob.glob("tests/golden/cases/c10_scd2_history/*.xml")
    if not cases:
        pytest.skip("golden case c10_scd2_history not present")
    flagged = []
    for fx in cases:
        for m in InformaticaXMLParser().parse(fx).mappings:
            for d in mapping_ddl(m):
                flagged += [w for w in d.widened if "SEQUENCE" in w]
    assert flagged, "the SCD2 surrogate key was not flagged"
    assert any("CUST_SK" in w for w in flagged), flagged
