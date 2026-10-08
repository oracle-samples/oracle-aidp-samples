"""Regressions pinned by the 2026-09 review of this migrator.

Every test here reproduces a defect that was confirmed by running the tool
over a real-shaped PowerCenter export (the bundled full-shape fixture with
one construct added), not by reading the code. Each docstring says what
the old behaviour was; the assertion is the behaviour a PowerCenter
developer would expect.
"""
from __future__ import annotations

import ast
import json
import os

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.code_validation import unresolved_names
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.models import LoadStrategy, Session
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

ROOT = os.path.dirname(os.path.dirname(__file__))
ORDERS = os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")


def _generate(xml_text: str, tmp_path, name="probe"):
    p = tmp_path / f"{name}.xml"
    p.write_text(xml_text, encoding="utf-8")
    result = InformaticaXMLParser().parse(str(p))
    mapping = result.mappings[0]
    session = next(iter(result.sessions), Session(name="s"))
    conv = TransformationConverter()
    tr = {tx.name: "\n".join(conv.convert(tx)) for tx in mapping.transformations}
    nb = NotebookGenerator().generate(
        mapping, session, {"transformations": tr, "source_reads": [], "target_write": ""}
    )
    code = "\n".join(
        "".join(c["source"]) for c in json.loads(nb)["cells"] if c["cell_type"] == "code"
    )
    return result, mapping, session, code


@pytest.fixture
def orders_xml():
    with open(ORDERS, encoding="utf-8") as f:
        return f.read()


# ── parser: real-export constructs ────────────────────────────────────

def test_reusable_transformation_at_folder_level_is_resolved_into_the_mapping(orders_xml, tmp_path):
    """PowerCenter stores a REUSABLE="YES" transformation once, under the
    FOLDER; the mapping carries only an INSTANCE pointing at it. Reading
    only MAPPING/TRANSFORMATION dropped it: the notebook generated with the
    Expression's logic silently absent."""
    x = orders_xml.replace('NAME="EXP_TRANSFORM" OBJECTVERSION="1" REUSABLE="NO"',
                           'NAME="EXP_TRANSFORM" OBJECTVERSION="1" REUSABLE="YES"')
    start = x.index("    <!-- Expression Transformation -->")
    end = x.index("    <!-- Instances -->")
    block = x[start:end]
    x = (x[:start] + x[end:]).replace("  <!-- MAPPING -->", block + "  <!-- MAPPING -->")
    x = x.replace('<INSTANCE NAME="EXP_TRANSFORM" REUSABLE="NO"', '<INSTANCE NAME="EXP_TRANSFORM" REUSABLE="YES"')

    _, mapping, _, code = _generate(x, tmp_path)
    assert [t.name for t in mapping.transformations] == ["SQ_ORDERS", "EXP_TRANSFORM"]
    assert "F.upper(F.col('CUSTOMER_NAME'))" in code
    assert any("reusable transformation EXP_TRANSFORM" in n for n in mapping.notes)


def test_reusable_instance_with_a_different_name_is_named_after_the_instance(orders_xml, tmp_path):
    """Connectors reference the INSTANCE name, so the resolved copy must
    carry it or the DAG comes out disconnected."""
    x = orders_xml.replace('NAME="EXP_TRANSFORM" OBJECTVERSION="1" REUSABLE="NO"',
                           'NAME="EXP_TRANSFORM" OBJECTVERSION="1" REUSABLE="YES"')
    start = x.index("    <!-- Expression Transformation -->")
    end = x.index("    <!-- Instances -->")
    block = x[start:end]
    x = (x[:start] + x[end:]).replace("  <!-- MAPPING -->", block + "  <!-- MAPPING -->")
    x = x.replace('<INSTANCE NAME="EXP_TRANSFORM" REUSABLE="NO"\n      TRANSFORMATION_NAME="EXP_TRANSFORM"',
                  '<INSTANCE NAME="EXP_TAX" REUSABLE="YES"\n      TRANSFORMATION_NAME="EXP_TRANSFORM"')
    x = x.replace('TOINSTANCE="EXP_TRANSFORM"', 'TOINSTANCE="EXP_TAX"').replace(
        'FROMINSTANCE="EXP_TRANSFORM"', 'FROMINSTANCE="EXP_TAX"')
    _, mapping, _, code = _generate(x, tmp_path)
    assert "EXP_TAX" in [t.name for t in mapping.transformations]
    assert "F.upper(F.col('CUSTOMER_NAME'))" in code
    assert unresolved_names(code) == []


def test_mapplet_instance_is_expanded_inline_and_rewired(tmp_path):
    """A MAPPLET is a folder-level object with its own transformations and
    Input/Output boundary transformations. The mapping's connectors address
    the mapplet instance by name. Before: the instance was not a mapping
    transformation at all, so every connector through it dangled."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART CREATION_DATE="01/01/2024" REPOSITORY_VERSION="188.97">
<REPOSITORY NAME="R" VERSION="188" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
<FOLDER NAME="F" GROUP="" OWNER="o" SHARED="NOTSHARED">
  <SOURCE DATABASETYPE="Oracle" DBDNAME="S" NAME="T" OWNERNAME="SC">
    <SOURCEFIELD DATATYPE="number" NAME="ID" PRECISION="10" SCALE="0" NULLABLE="NOT NULL" KEYTYPE="PRIMARY KEY"/>
    <SOURCEFIELD DATATYPE="varchar" NAME="NM" PRECISION="50" SCALE="0" NULLABLE="NULL"/>
  </SOURCE>
  <TARGET DATABASETYPE="Oracle" DBDNAME="D" NAME="TGT">
    <TARGETFIELD DATATYPE="number" NAME="ID" PRECISION="10" SCALE="0" KEYTYPE="PRIMARY KEY" NULLABLE="NOT NULL"/>
    <TARGETFIELD DATATYPE="varchar" NAME="NM_UP" PRECISION="50" SCALE="0"/>
  </TARGET>
  <MAPPLET NAME="mplt_clean">
    <TRANSFORMATION NAME="INPUT" TYPE="Input Transformation" REUSABLE="NO">
      <TRANSFORMFIELD NAME="IN_NM" DATATYPE="varchar" PORTTYPE="OUTPUT" PRECISION="50" SCALE="0"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="EXP_UP" TYPE="Expression" REUSABLE="NO">
      <TRANSFORMFIELD NAME="IN_NM" DATATYPE="varchar" PORTTYPE="INPUT" PRECISION="50" SCALE="0"/>
      <TRANSFORMFIELD NAME="OUT_NM" DATATYPE="varchar" PORTTYPE="OUTPUT" PRECISION="50" SCALE="0" EXPRESSION="UPPER(IN_NM)"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="OUTPUT" TYPE="Output Transformation" REUSABLE="NO">
      <TRANSFORMFIELD NAME="CLEAN_NM" DATATYPE="varchar" PORTTYPE="INPUT" PRECISION="50" SCALE="0"/>
    </TRANSFORMATION>
    <INSTANCE NAME="INPUT" TRANSFORMATION_NAME="INPUT" TRANSFORMATION_TYPE="Input Transformation" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="EXP_UP" TRANSFORMATION_NAME="EXP_UP" TRANSFORMATION_TYPE="Expression" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="OUTPUT" TRANSFORMATION_NAME="OUTPUT" TRANSFORMATION_TYPE="Output Transformation" TYPE="TRANSFORMATION"/>
    <CONNECTOR FROMINSTANCE="INPUT" FROMFIELD="IN_NM" TOINSTANCE="EXP_UP" TOFIELD="IN_NM"/>
    <CONNECTOR FROMINSTANCE="EXP_UP" FROMFIELD="OUT_NM" TOINSTANCE="OUTPUT" TOFIELD="CLEAN_NM"/>
  </MAPPLET>
  <MAPPING NAME="m_use_mplt" ISVALID="YES">
    <TRANSFORMATION NAME="SQ_T" TYPE="Source Qualifier" REUSABLE="NO">
      <TRANSFORMFIELD NAME="ID" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD NAME="NM" DATATYPE="varchar" PORTTYPE="OUTPUT" PRECISION="50" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Source Table" VALUE="T"/>
    </TRANSFORMATION>
    <INSTANCE NAME="T" TRANSFORMATION_NAME="T" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
    <INSTANCE NAME="SQ_T" TRANSFORMATION_NAME="SQ_T" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="mplt_clean1" TRANSFORMATION_NAME="mplt_clean" TRANSFORMATION_TYPE="Mapplet" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="TGT" TRANSFORMATION_NAME="TGT" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <CONNECTOR FROMINSTANCE="T" FROMFIELD="ID" TOINSTANCE="SQ_T" TOFIELD="ID"/>
    <CONNECTOR FROMINSTANCE="T" FROMFIELD="NM" TOINSTANCE="SQ_T" TOFIELD="NM"/>
    <CONNECTOR FROMINSTANCE="SQ_T" FROMFIELD="NM" TOINSTANCE="mplt_clean1" TOFIELD="IN_NM"/>
    <CONNECTOR FROMINSTANCE="SQ_T" FROMFIELD="ID" TOINSTANCE="TGT" TOFIELD="ID"/>
    <CONNECTOR FROMINSTANCE="mplt_clean1" FROMFIELD="CLEAN_NM" TOINSTANCE="TGT" TOFIELD="NM_UP"/>
  </MAPPING>
</FOLDER></REPOSITORY></POWERMART>
"""
    _, mapping, _, code = _generate(xml, tmp_path, "mplt")
    names = [t.name for t in mapping.transformations]
    assert "mplt_clean1__EXP_UP" in names
    assert "INPUT" not in names and "OUTPUT" not in names
    edges = {(c.from_instance, c.from_field, c.to_instance, c.to_field) for c in mapping.connectors}
    assert ("SQ_T", "NM", "mplt_clean1__EXP_UP", "IN_NM") in edges
    assert ("mplt_clean1__EXP_UP", "OUT_NM", "TGT", "NM_UP") in edges
    assert not any(c.to_instance == "mplt_clean1" or c.from_instance == "mplt_clean1" for c in mapping.connectors)
    assert "F.upper(F.col('IN_NM'))" in code
    assert any("expanded inline" in n for n in mapping.notes)


def test_source_filter_and_select_distinct_reach_the_read(orders_xml, tmp_path):
    """"Source Filter" is how an incremental PowerCenter load limits its
    extract; it was not read at all, so the notebook read the whole table."""
    x = orders_xml.replace(
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE=""/>',
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE=""/>\n'
        '      <TABLEATTRIBUTE NAME="Source Filter" VALUE="ORDERS.STATUS = \'OPEN\' AND ORDERS.ORDER_DATE &gt;= TO_DATE(\'$$LAST_RUN\',\'YYYY-MM-DD\')"/>\n'
        '      <TABLEATTRIBUTE NAME="Select Distinct" VALUE="YES"/>',
    )
    _, mapping, _, code = _generate(x, tmp_path)
    sq = next(t for t in mapping.transformations if t.name == "SQ_ORDERS")
    assert sq.source_filter.startswith("ORDERS.STATUS")
    assert sq.select_distinct is True
    assert "df_source = df_source.distinct()" in code
    # the qualifier's own table prefix is stripped, $$LAST_RUN resolves via _param
    assert "STATUS = 'OPEN'" in code and "ORDERS.STATUS" not in code.split("Source Filter")[1].split("\n")[1]
    # the parameter's TEXT goes into the filter SQL, as Informatica substitutes it
    assert "_param_text('LAST_RUN')" in code
    assert "to_timestamp(" in code
    ast.parse(code)


def test_sql_override_with_a_join_is_a_review_item_not_a_silent_single_table_read(orders_xml, tmp_path):
    """The override joined two tables; the notebook read one and applied the
    WHERE (with an alias Spark does not know). Now the base table is read
    and the override is quoted under REVIEW REQUIRED."""
    x = orders_xml.replace(
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE=""/>',
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT o.ORDER_ID, c.CUSTOMER_NAME, o.AMOUNT, o.ORDER_DATE '
        'FROM SALES.ORDERS o JOIN SALES.CUSTOMERS c ON c.CUST_ID = o.CUST_ID WHERE o.STATUS = \'OPEN\'"/>',
    )
    _, _, _, code = _generate(x, tmp_path)
    assert "REVIEW REQUIRED: Source Qualifier 'SQ_ORDERS' has a SQL override" in code
    assert "SQL> SELECT o.ORDER_ID" in code
    assert 'filter(F.expr("""o.STATUS' not in code


def test_single_table_sql_override_where_becomes_a_filter_without_the_alias(orders_xml, tmp_path):
    x = orders_xml.replace(
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE=""/>',
        '<TABLEATTRIBUTE NAME="Sql Query" VALUE="SELECT o.ORDER_ID, o.CUSTOMER_NAME, o.AMOUNT, o.ORDER_DATE '
        'FROM SALES.ORDERS o WHERE o.STATUS = \'OPEN\' AND o.AMOUNT &gt; 0"/>',
    )
    _, _, _, code = _generate(x, tmp_path)
    assert "df_source = df_source.filter(F.expr(\"STATUS = 'OPEN' AND AMOUNT > 0\"))" in code


# ── generator: notebooks that could not run ───────────────────────────

def test_session_attributes_do_not_become_python_identifiers(orders_xml, tmp_path):
    """Every real session export has top-level ATTRIBUTEs ("Treat source
    rows as", "Commit Interval", ...). They were filed as parameters and
    emitted as `TREAT SOURCE ROWS AS = spark.conf.get(...)`: a SyntaxError
    in every notebook generated for a mapping with a session."""
    x = orders_xml.replace(
        "</MAPPING>",
        '</MAPPING>\n  <SESSION NAME="s_m_ORDERS_TRANSFORM" MAPPINGNAME="m_ORDERS_TRANSFORM">\n'
        '    <ATTRIBUTE NAME="Treat source rows as" VALUE="Insert"/>\n'
        '    <ATTRIBUTE NAME="Commit Interval" VALUE="10000"/>\n'
        '    <ATTRIBUTE NAME="$$RUN_ID" VALUE="42"/>\n'
        "  </SESSION>",
    )
    result, _, session, code = _generate(x, tmp_path)
    ast.parse(code)
    assert session.properties["Treat source rows as"] == "Insert"
    assert session.parameters == {"$$RUN_ID": "42"}
    assert "'RUN_ID': '42'" in code
    assert "TREAT SOURCE ROWS AS" not in code


def test_missing_target_column_default_lands_on_the_pipeline_dataframe(orders_xml, tmp_path):
    """SQ -> Expression -> Target stays on df_source; the LOAD_DATE default
    was emitted as `df = df.withColumn(...)` -- NameError at run time."""
    x = orders_xml.replace(
        '<TARGETFIELD DATATYPE="varchar" NAME="ORDER_YEAR"',
        '<TARGETFIELD DATATYPE="date" NAME="LOAD_DATE" PRECISION="29" SCALE="9"/>\n'
        '    <TARGETFIELD DATATYPE="varchar" NAME="ORDER_YEAR"',
    )
    _, _, _, code = _generate(x, tmp_path)
    assert unresolved_names(code) == []
    # on the DataFrame that becomes df_final (whatever it is named)
    import re as _re
    m = _re.search(r'(\w+) = \1\.withColumn\("LOAD_DATE", F\.lit\(_SESSION_START_TIME\)\)', code)
    assert m, code
    assert f"df_final = {m.group(1)}" in code


def test_zero_rows_is_a_warning_not_a_failed_notebook(orders_xml, tmp_path):
    """An incremental load with nothing new is a successful Informatica
    session; the generated notebook used to assert row_count > 0."""
    _, _, _, code = _generate(orders_xml, tmp_path)
    assert "assert row_count > 0" not in code
    assert "if row_count == 0:" in code


def test_parameters_cell_defines_param_helper_the_expressions_use(orders_xml, tmp_path):
    x = orders_xml.replace('EXPRESSION="AMOUNT * 1.08"', 'EXPRESSION="AMOUNT * $$TAX_RATE"')
    x = x.replace("<MAPPING DESCRIPTION", '<MAPPING DESCRIPTION').replace(
        '    <!-- Source Qualifier -->',
        '    <MAPPINGVARIABLE NAME="$$TAX_RATE" DEFAULTVALUE="1.08" DATATYPE="decimal"/>\n    <!-- Source Qualifier -->',
    )
    _, _, _, code = _generate(x, tmp_path)
    assert "def _param(name, default=None):" in code
    assert "'TAX_RATE': '1.08'" in code
    assert "F.col('AMOUNT') * F.lit(_param('TAX_RATE'))" in code
    assert unresolved_names(code) == []


def test_notebook_carries_no_databricks_metadata(orders_xml, tmp_path):
    p = tmp_path / "o.xml"
    p.write_text(orders_xml, encoding="utf-8")
    result = InformaticaXMLParser().parse(str(p))
    nb = NotebookGenerator().generate(result.mappings[0], Session(name="s"),
                                      {"transformations": {}, "source_reads": [], "target_write": ""})
    assert "databricks" not in nb.lower()
    assert "dbutils" not in nb


# ── update strategy + lookup, end to end ──────────────────────────────

UPD_XML = """<?xml version="1.0" encoding="UTF-8"?>
<POWERMART CREATION_DATE="01/01/2024 10:00:00" REPOSITORY_VERSION="188.97">
<REPOSITORY NAME="R" VERSION="188" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
<FOLDER NAME="DW" GROUP="" OWNER="dev" SHARED="NOTSHARED">
  <SOURCE DATABASETYPE="Oracle" DBDNAME="SRC_DB" NAME="CUSTOMERS" OWNERNAME="CRM">
    <SOURCEFIELD DATATYPE="number" NAME="CUST_ID" NULLABLE="NOT NULL" PRECISION="10" SCALE="0" KEYTYPE="PRIMARY KEY"/>
    <SOURCEFIELD DATATYPE="varchar" NAME="NAME" NULLABLE="NULL" PRECISION="100" SCALE="0"/>
  </SOURCE>
  <TARGET DATABASETYPE="Oracle" DBDNAME="DW_DB" NAME="DIM_CUSTOMER" CONSTRAINT="">
    <TARGETFIELD DATATYPE="number" KEYTYPE="PRIMARY KEY" NAME="CUST_ID" NULLABLE="NOT NULL" PRECISION="10" SCALE="0"/>
    <TARGETFIELD DATATYPE="varchar" NAME="NAME" PRECISION="100" SCALE="0"/>
  </TARGET>
  <MAPPING NAME="m_DIM_CUSTOMER" ISVALID="YES">
    <TRANSFORMATION NAME="SQ_CUSTOMERS" REUSABLE="NO" TYPE="Source Qualifier">
      <TRANSFORMFIELD DATATYPE="number" NAME="CUST_ID" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="varchar" NAME="NAME" PORTTYPE="OUTPUT" PRECISION="100" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Sql Query" VALUE=""/>
      <TABLEATTRIBUTE NAME="Source Table" VALUE="CUSTOMERS"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="LKP_DIM_CUSTOMER" REUSABLE="NO" TYPE="Lookup Procedure">
      <TRANSFORMFIELD DATATYPE="number" NAME="LKP_CUST_ID" PORTTYPE="LOOKUP/OUTPUT" PRECISION="10" SCALE="0" EXPRESSION="DIM_CUSTOMER.CUST_ID"/>
      <TRANSFORMFIELD DATATYPE="number" NAME="IN_CUST_ID" PORTTYPE="INPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Lookup table name" VALUE="DIM_CUSTOMER"/>
      <TABLEATTRIBUTE NAME="Lookup condition" VALUE="LKP_CUST_ID = IN_CUST_ID"/>
      <TABLEATTRIBUTE NAME="Lookup policy on multiple match" VALUE="Report Error"/>
      <TABLEATTRIBUTE NAME="Dynamic Lookup Cache" VALUE="NO"/>
    </TRANSFORMATION>
    <TRANSFORMATION NAME="UPD_STRATEGY" REUSABLE="NO" TYPE="Update Strategy">
      <TRANSFORMFIELD DATATYPE="number" NAME="CUST_ID" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="varchar" NAME="NAME" PORTTYPE="INPUT/OUTPUT" PRECISION="100" SCALE="0"/>
      <TRANSFORMFIELD DATATYPE="number" NAME="LKP_CUST_ID" PORTTYPE="INPUT" PRECISION="10" SCALE="0"/>
      <TABLEATTRIBUTE NAME="Update Strategy Expression" VALUE="IIF(ISNULL(LKP_CUST_ID), DD_INSERT, DD_UPDATE)"/>
    </TRANSFORMATION>
    <INSTANCE NAME="CUSTOMERS" TRANSFORMATION_NAME="CUSTOMERS" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
    <INSTANCE NAME="SQ_CUSTOMERS" TRANSFORMATION_NAME="SQ_CUSTOMERS" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="LKP_DIM_CUSTOMER" TRANSFORMATION_NAME="LKP_DIM_CUSTOMER" TRANSFORMATION_TYPE="Lookup Procedure" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="UPD_STRATEGY" TRANSFORMATION_NAME="UPD_STRATEGY" TRANSFORMATION_TYPE="Update Strategy" TYPE="TRANSFORMATION"/>
    <INSTANCE NAME="DIM_CUSTOMER" TRANSFORMATION_NAME="DIM_CUSTOMER" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
    <CONNECTOR FROMFIELD="CUST_ID" FROMINSTANCE="CUSTOMERS" TOFIELD="CUST_ID" TOINSTANCE="SQ_CUSTOMERS"/>
    <CONNECTOR FROMFIELD="NAME" FROMINSTANCE="CUSTOMERS" TOFIELD="NAME" TOINSTANCE="SQ_CUSTOMERS"/>
    <CONNECTOR FROMFIELD="CUST_ID" FROMINSTANCE="SQ_CUSTOMERS" TOFIELD="IN_CUST_ID" TOINSTANCE="LKP_DIM_CUSTOMER"/>
    <CONNECTOR FROMFIELD="CUST_ID" FROMINSTANCE="SQ_CUSTOMERS" TOFIELD="CUST_ID" TOINSTANCE="UPD_STRATEGY"/>
    <CONNECTOR FROMFIELD="NAME" FROMINSTANCE="SQ_CUSTOMERS" TOFIELD="NAME" TOINSTANCE="UPD_STRATEGY"/>
    <CONNECTOR FROMFIELD="LKP_CUST_ID" FROMINSTANCE="LKP_DIM_CUSTOMER" TOFIELD="LKP_CUST_ID" TOINSTANCE="UPD_STRATEGY"/>
    <CONNECTOR FROMFIELD="CUST_ID" FROMINSTANCE="UPD_STRATEGY" TOFIELD="CUST_ID" TOINSTANCE="DIM_CUSTOMER"/>
    <CONNECTOR FROMFIELD="NAME" FROMINSTANCE="UPD_STRATEGY" TOFIELD="NAME" TOINSTANCE="DIM_CUSTOMER"/>
  </MAPPING>
  <SESSION NAME="s_m_DIM_CUSTOMER" MAPPINGNAME="m_DIM_CUSTOMER">
    <SESSTRANSFORMATIONINST SINSTANCENAME="DIM_CUSTOMER" TRANSFORMATIONNAME="DIM_CUSTOMER" TRANSFORMATIONTYPE="Target Definition">
      <ATTRIBUTE NAME="Insert" VALUE="YES"/>
      <ATTRIBUTE NAME="Update as Update" VALUE="YES"/>
      <ATTRIBUTE NAME="Truncate target table option" VALUE="NO"/>
    </SESSTRANSFORMATIONINST>
    <ATTRIBUTE NAME="Treat source rows as" VALUE="Data driven"/>
  </SESSION>
</FOLDER></REPOSITORY></POWERMART>
"""


def test_update_strategy_mapping_routes_through_the_dd_strategy_partitions(tmp_path):
    """Single-target mapping with Lookup -> Update Strategy -> Target.
    Before: (1) the IIF was inverted and referenced columns named
    DD_INSERT/DD_UPDATE, (2) the select dropped DD_STRATEGY, (3) the write
    was the generic keyed MERGE, upserting deletes and inserting rejects,
    (4) the lookup joined on IN_CUST_ID which the pipeline does not carry,
    (5) 'Report Error' was silently 'first'."""
    result, mapping, session, code = _generate(UPD_XML, tmp_path, "upd")
    ast.parse(code)
    assert unresolved_names(code) == []
    assert "F.when(F.isnull(F.col('LKP_CUST_ID')), F.lit(0)).otherwise(F.lit(1))" in code
    # joined on the pipeline's CUST_ID (not the lookup's IN_CUST_ID port),
    # Report Error kept
    assert "condition='`LKP_DIM_CUSTOMER__LKP_CUST_ID` = `CUST_ID`', policy='error'" in code
    assert 'df_final = df_final.select("CUST_ID", "NAME", "DD_STRATEGY")' in code
    assert 'infa_compat.apply_update_strategy(df_final, strategy_col="DD_STRATEGY")' in code
    assert "infa_compat.write_update_strategy(" in code
    assert "whenMatchedUpdateAll" not in code
    assert session.treat_source_rows_as == "Data driven"
    assert session.load_strategy_for("DIM_CUSTOMER") is LoadStrategy.UPSERT


def test_session_level_load_options_decide_the_strategy():
    s = Session(name="s")
    assert s.load_strategy_for("T") is None
    s.properties["Treat source rows as"] = "Insert"
    s.target_load_options["T"] = {"Insert": "YES", "Truncate target table option": "YES"}
    assert s.load_strategy_for("T") is LoadStrategy.TRUNCATE_INSERT
    s.target_load_options["T"] = {"Insert": "YES", "Truncate target table option": "NO"}
    assert s.load_strategy_for("T") is LoadStrategy.INSERT
    s.properties["Treat source rows as"] = "Update"
    s.target_load_options["T"] = {"Update else Insert": "YES"}
    assert s.load_strategy_for("T") is LoadStrategy.UPSERT
    s.target_load_options["T"] = {"Update as Update": "YES"}
    assert s.load_strategy_for("T") is LoadStrategy.UPDATE
    s.properties["Treat source rows as"] = "Delete"
    assert s.load_strategy_for("T") is LoadStrategy.DELETE


def test_target_constraint_attribute_is_not_a_load_strategy(orders_xml, tmp_path):
    """TARGET/@CONSTRAINT is DDL text; 'ON UPDATE CASCADE' in it used to
    make the load strategy UPDATE."""
    x = orders_xml.replace('CONSTRAINT=""', 'CONSTRAINT="FOREIGN KEY (CUST_ID) REFERENCES CUST ON UPDATE CASCADE"')
    result, mapping, _, _ = _generate(x, tmp_path)
    assert mapping.targets[0].load_strategy is LoadStrategy.INSERT
