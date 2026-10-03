"""Source Qualifier SQL handling: regressions from a review of the override,
User Defined Join and refusal paths, plus workflow task resolution.

One test (or one small group) per defect. Where the claim is about what the
generated code DOES, it is executed on a local Spark session over temp
views -- "it emitted spark.sql" is not the claim.

1. A SQL Query overrides the User Defined Join, Source Filter, Number Of
   Sorted Ports and Select Distinct; a UDJ used to replace the override.
2. A UDJ was ``SELECT * FROM a, b``, which returns the join key twice.
3. Table qualification rewrote string literals equal to a table name.
4. ``'$$X'`` became ``''EU''``; braces in an f-string query were evaluated.
5. Schema-qualified tables passed the gate but were never qualified.
6. A multi-line unconvertible condition broke the notebook's syntax.
7. Informatica's ``{ A LEFT OUTER JOIN B ON ... }`` UDJ was emitted as SQL.
8. Non-reusable <TASK>s resolved by name across workflows; VALUEPAIR
   commands were never read.
"""
from __future__ import annotations

import ast
import textwrap

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.generators.workflow_generator import WorkflowGenerator
from infa2aidp.models import Mapping, Session, Transformation, TransformationType
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

from tests.conftest_spark import spark  # noqa: F401

SOURCES = {"ORDERS": "oltp.app.orders", "CUSTOMERS": "oltp.app.customers"}
COLUMNS = {
    "ORDERS": ["ORDER_ID", "CUST_ID", "CUSTOMERS"],
    "CUSTOMERS": ["CUST_ID", "NAME", "SEGMENT", "ZIP"],
}


def _sq(sql=None, udj=None, sf=None, distinct=False, sorted_ports=0,
        sources=SOURCES, columns=COLUMNS) -> list[str]:
    tx = Transformation(name="SQ_ORDERS", type=TransformationType.SOURCE_QUALIFIER)
    tx.sql_override = sql or ""
    tx.user_defined_join = udj or ""
    tx.source_filter = sf or ""
    tx.select_distinct = distinct
    if sorted_ports:
        tx.properties["Number Of Sorted Ports"] = str(sorted_ports)
    return TransformationConverter().source_read_lines(
        tx, sources["ORDERS"], "df", sources, columns
    )


def _code(lines: list[str]) -> str:
    return "\n".join(l for l in lines if not l.strip().startswith("#"))


def _refused(lines: list[str]) -> bool:
    return any("REVIEW REQUIRED" in l for l in lines)


@pytest.fixture(scope="module")
def views(spark):  # noqa: F811
    """ORDER 3 points at a customer that does not exist; ORDERS carries a
    column named CUSTOMERS, the same as a table."""
    spark.createDataFrame(
        [(1, 10, 7), (2, 11, 8), (3, 99, 9), (4, 10, 7)],
        "ORDER_ID int, CUST_ID int, CUSTOMERS int",
    ).createOrReplaceTempView("sqf_orders")
    spark.createDataFrame(
        [(10, "O'Brien Ltd", "CUSTOMERS", "12345"),
         (11, "Globex", "RETAIL", "1234")],
        "CUST_ID int, NAME string, SEGMENT string, ZIP string",
    ).createOrReplaceTempView("sqf_customers")
    return {"ORDERS": "sqf_orders", "CUSTOMERS": "sqf_customers"}


def _run(spark, lines: list[str], params: dict | None = None):  # noqa: F811
    """Execute the read as a notebook would: the generator's own parameter
    cell first, so _param_text/_sql_lit are the real definitions."""
    ns = {"spark": spark}
    mapping = Mapping(name="m", parameters={f"$${k}": v for k, v in (params or {}).items()})
    exec(NotebookGenerator()._parameters_cell(mapping, Session(name="s")), ns)  # noqa: S102
    exec(_code(lines), ns)  # noqa: S102
    return ns["df"]


# ── 1. SQL Query overrides UDJ / Source Filter / sorted ports / distinct ──

JOIN_SQL = (
    "SELECT o.ORDER_ID, c.NAME FROM ORDERS o JOIN CUSTOMERS c "
    "ON o.CUST_ID = c.CUST_ID"
)


def test_1_sql_query_overrides_the_other_qualifier_settings(spark, views):  # noqa: F811
    lines = _sq(sql=JOIN_SQL, udj="ORDERS.CUST_ID = CUSTOMERS.CUST_ID",
                sf="ORDERS.ORDER_ID > 1", distinct=True, sorted_ports=1,
                sources=views)
    code = _code(lines)
    assert code.count("spark.sql(") == 1 and "c.NAME" in code
    assert ".filter(" not in code and ".distinct()" not in code
    assert ".orderBy(" not in code
    ignored = [l for l in lines if "NOT applied" in l and "SQL Query overrides" in l]
    assert len(ignored) == 1, lines
    for name in ("User Defined Join", "Source Filter", "Number Of Sorted Ports",
                 "Select Distinct"):
        assert name in ignored[0]
    # The override's rows: orders 1, 2, 4 (4 duplicates 1's customer, so a
    # distinct or a Source Filter applied on top would change the result).
    rows = _run(spark, lines).orderBy("ORDER_ID").collect()
    assert [r["ORDER_ID"] for r in rows] == [1, 2, 4]


def test_1_a_refused_override_is_not_replaced_by_the_udj():
    lines = _sq(
        sql="SELECT o.ORDER_ID FROM ORDERS o, CUSTOMERS c WHERE o.CUST_ID = c.CUST_ID(+)",
        udj="ORDERS.CUST_ID = CUSTOMERS.CUST_ID",
    )
    assert any("has a SQL override" in l and "REVIEW REQUIRED" in l for l in lines)
    assert "spark.sql(" not in _code(lines)


# ── 2. UDJ projects each column once ─────────────────────────────────

def test_2_udj_returns_each_column_once_and_runs(spark, views):  # noqa: F811
    cols = {"ORDERS": ["ORDER_ID", "CUST_ID"], "CUSTOMERS": ["CUST_ID", "NAME"]}
    lines = _sq(udj="ORDERS.CUST_ID = CUSTOMERS.CUST_ID", sources=views, columns=cols)
    assert not _refused(lines) and "SELECT *" not in _code(lines)
    df = _run(spark, lines)
    assert df.columns == ["ORDER_ID", "CUST_ID", "NAME"]
    # The first use of the join key used to be AMBIGUOUS_REFERENCE.
    rows = df.select("ORDER_ID", "CUST_ID", "NAME").orderBy("ORDER_ID").collect()
    assert [tuple(r) for r in rows] == [
        (1, 10, "O'Brien Ltd"), (2, 11, "Globex"), (4, 10, "O'Brien Ltd")]


def test_2_a_shared_column_the_join_does_not_equate_is_refused():
    cols = {"ORDERS": ["ORDER_ID", "CUST_ID", "STATUS"],
            "CUSTOMERS": ["CUST_ID", "STATUS"]}
    lines = _sq(udj="ORDERS.CUST_ID = CUSTOMERS.CUST_ID", columns=cols)
    assert _refused(lines) and "spark.sql(" not in _code(lines)
    assert any("STATUS" in l and "REVIEW REQUIRED" in l for l in lines)


def test_2_without_source_columns_the_udj_is_refused_not_select_star():
    lines = _sq(udj="ORDERS.CUST_ID = CUSTOMERS.CUST_ID", columns=None)
    assert _refused(lines)
    assert "spark.sql(" not in _code(lines)


# ── 3. string literals are not table references ──────────────────────

def test_3_a_literal_equal_to_a_table_name_is_left_alone(spark, views):  # noqa: F811
    lines = _sq(sql=JOIN_SQL + " WHERE c.SEGMENT = 'CUSTOMERS'", sources=views)
    assert "'CUSTOMERS'" in _code(lines)
    rows = _run(spark, lines).orderBy("ORDER_ID").collect()
    assert [r["ORDER_ID"] for r in rows] == [1, 4]


# ── 4. $$PARAM inside literals, and braces in an f-string query ──────

def test_4_quoted_and_embedded_params_and_braces_run(spark, views):  # noqa: F811
    sql = (
        JOIN_SQL + " WHERE c.SEGMENT = '$$SEG' AND c.NAME LIKE '%$$NM%' "
        "AND c.ZIP RLIKE '^[0-9]{5}$'"
    )
    lines = _sq(sql=sql, sources=views)
    code = _code(lines)
    assert "'{_sql_lit" not in code  # quoted twice: ''CUSTOMERS''
    ast.parse(code)
    # The quote in O'Brien must survive: Spark reads 'O''Brien' as 'OBrien'.
    df = _run(spark, lines, {"SEG": "CUSTOMERS", "NM": "O'Brien"})
    assert sorted(r["ORDER_ID"] for r in df.collect()) == [1, 4]


# ── 5. schema-qualified tables are qualified ─────────────────────────

def test_5_schema_qualified_tables_and_qualifiers_are_replaced_whole(spark, views):  # noqa: F811
    sql = (
        "SELECT APP.ORDERS.ORDER_ID, APP.CUSTOMERS.NAME FROM APP.ORDERS "
        "JOIN APP.CUSTOMERS ON APP.ORDERS.CUST_ID = APP.CUSTOMERS.CUST_ID"
    )
    code = _code(_sq(sql=sql))
    assert "FROM oltp.app.orders JOIN oltp.app.customers ON" in code
    assert "oltp.app.orders.CUST_ID = oltp.app.customers.CUST_ID" in code
    assert "APP." not in code
    rows = _run(spark, _sq(sql=sql, sources=views)).orderBy("ORDER_ID").collect()
    assert [r["ORDER_ID"] for r in rows] == [1, 2, 4]


def test_5_an_alias_column_sharing_a_table_name_is_not_rewritten(spark, views):  # noqa: F811
    sql = (
        "SELECT o.ORDER_ID, o.CUSTOMERS, c.NAME FROM APP.ORDERS o "
        "JOIN APP.CUSTOMERS c ON o.CUST_ID = c.CUST_ID"
    )
    code = _code(_sq(sql=sql))
    assert "FROM oltp.app.orders o JOIN oltp.app.customers c" in code
    assert "o.CUSTOMERS," in code
    rows = _run(spark, _sq(sql=sql, sources=views)).orderBy("ORDER_ID").collect()
    assert [(r["ORDER_ID"], r["CUSTOMERS"]) for r in rows] == [(1, 7), (2, 8), (4, 7)]


# ── 6. a multi-line unconvertible condition still compiles ───────────

@pytest.mark.parametrize("kind", ["filter", "router"])
def test_6_multiline_refusal_compiles_and_raises(kind):
    cond = "NOT_A_REAL_FUNC(AMOUNT)\r\n  > 0 → x"
    if kind == "filter":
        tx = Transformation(name="FIL_X", type=TransformationType.FILTER)
        tx.filter_condition = cond
    else:
        tx = Transformation(name="RTR_X", type=TransformationType.ROUTER)
        tx.router_groups = [{"name": "bad", "condition": cond, "type": "OUTPUT"}]
    code = "\n".join(TransformationConverter().convert(tx, "df_in", "df_out"))
    ast.parse(code)
    with pytest.raises(NotImplementedError):
        exec(code, {"df_in": None, "F": None})  # noqa: S102


# ── 7. Informatica outer-join syntax in a UDJ ────────────────────────

def test_7_informatica_outer_join_syntax_is_refused_and_named():
    lines = _sq(udj="{ ORDERS LEFT OUTER JOIN CUSTOMERS ON ORDERS.CUST_ID = CUSTOMERS.CUST_ID }")
    assert _refused(lines) and "spark.sql(" not in _code(lines)
    assert any("Informatica's outer-join syntax" in l for l in lines)
    ast.parse("\n".join(lines))


# ── 8. workflow-local <TASK>s and VALUEPAIR commands ─────────────────

_TWO_WORKFLOWS = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <POWERMART REPOSITORY_VERSION="189.1">
      <REPOSITORY NAME="REP" VERSION="189" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
        <FOLDER NAME="OPS" GROUP="" OWNER="dev" SHARED="NOTSHARED">
          <TASK NAME="cmd_shared" TYPE="Command" REUSABLE="YES" VERSIONNUMBER="1">
            <VALUEPAIR EXECORDER="1" NAME="command1" REVERSEASSIGNMENT="NO" VALUE="touch /infa/flag"/>
          </TASK>
          <WORKFLOW NAME="wf_sales" ISENABLED="YES">
            <TASK NAME="cmd_archive" TYPE="Command" REUSABLE="NO" VERSIONNUMBER="1">
              <ATTRIBUTE NAME="Fail task if any command fails" VALUE="NO"/>
              <VALUEPAIR EXECORDER="1" NAME="command1" REVERSEASSIGNMENT="NO" VALUE="mv /infa/tgt/sales*.out /arch/"/>
            </TASK>
            <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
            <TASKINSTANCE NAME="cmd_archive" TASKNAME="cmd_archive" TASKTYPE="Command"/>
            <TASKINSTANCE NAME="cmd_shared" TASKNAME="cmd_shared" TASKTYPE="Command"/>
            <WORKFLOWLINK FROMTASK="Start" TOTASK="cmd_archive" CONDITION=""/>
            <WORKFLOWLINK FROMTASK="cmd_archive" TOTASK="cmd_shared" CONDITION=""/>
          </WORKFLOW>
          <WORKFLOW NAME="wf_hr" ISENABLED="YES">
            <TASK NAME="cmd_archive" TYPE="Command" REUSABLE="NO" VERSIONNUMBER="1">
              <ATTRIBUTE NAME="Fail task if any command fails" VALUE="YES"/>
              <VALUEPAIR EXECORDER="2" NAME="command2" REVERSEASSIGNMENT="NO" VALUE="gzip /arch/hr*.out"/>
              <VALUEPAIR EXECORDER="1" NAME="command1" REVERSEASSIGNMENT="NO" VALUE="rm -f /infa/tgt/hr*.tmp"/>
            </TASK>
            <TASKINSTANCE NAME="Start" TASKNAME="Start" TASKTYPE="Start"/>
            <TASKINSTANCE NAME="cmd_archive" TASKNAME="cmd_archive" TASKTYPE="Command"/>
            <WORKFLOWLINK FROMTASK="Start" TOTASK="cmd_archive" CONDITION=""/>
          </WORKFLOW>
        </FOLDER>
      </REPOSITORY>
    </POWERMART>
    """)


def test_8_each_workflow_gets_its_own_task_definition(tmp_path):
    p = tmp_path / "wf_two.xml"
    p.write_bytes(_TWO_WORKFLOWS.encode("utf-8"))
    wfs = {wf.name: wf for wf in InformaticaXMLParser().parse(str(p)).workflows}

    def props(wf, name):
        return next(t["properties"] for t in wfs[wf].tasks if t["name"] == name)

    sales, hr = props("wf_sales", "cmd_archive"), props("wf_hr", "cmd_archive")
    assert sales["Fail task if any command fails"] == "NO"
    assert hr["Fail task if any command fails"] == "YES"
    assert sales["command1"] == "mv /infa/tgt/sales*.out /arch/"
    assert hr["Command"] == "rm -f /infa/tgt/hr*.tmp\ngzip /arch/hr*.out"
    # A folder-level reusable task still resolves.
    assert props("wf_sales", "cmd_shared")["Command"] == "touch /infa/flag"
    # ... and the command reaches the review an operator reads.
    review = " ".join(WorkflowGenerator._review_untranslated_tasks(wfs["wf_hr"]))
    assert "rm -f /infa/tgt/hr*.tmp" in review and "sales" not in review


# ── _sql_lit: a parameter value survives Spark's literal syntax ──────

@pytest.mark.parametrize("value", ["it's", "O'Brien", "a\\b", "ends\\", "plain"])
def test_sql_lit_round_trips_quotes_and_backslashes(spark, value):  # noqa: F811
    """Oracle's doubled quote is two adjacent literals in Spark ('it''s'
    reads as 'its'), and a bare backslash starts an escape sequence, so
    a Source Filter or override comparing against such a value matched
    nothing. The notebook's own _sql_lit is executed here."""
    ns = {"spark": spark}
    exec(NotebookGenerator()._parameters_cell(Mapping(name="m"), Session(name="s")), ns)  # noqa: S102
    assert spark.sql("SELECT " + ns["_sql_lit"](value) + " AS v").collect()[0]["v"] == value
