"""Wiring regressions: which DataFrame feeds what, in a real export's shape.

The first review pass fixed what the parser dropped. This pass is about
what the generator wires wrongly once everything is parsed: Router groups
to their consumers, Joiner ports to their upstream columns, the
previous-row variable-port idiom, and workflow links that pass through a
task the job cannot represent. Each test reproduces a defect confirmed by
running the generator over the mapping in the test.
"""
from __future__ import annotations

import ast
import json
import os

import pytest

from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.code_validation import unresolved_names
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.generators.workflow_generator import WorkflowGenerator
from infa2aidp.models import Session, Workflow
from infa2aidp.parsers.xml_parser import InformaticaXMLParser

HDR = ('<?xml version="1.0"?><POWERMART REPOSITORY_VERSION="188.97"><REPOSITORY NAME="R" VERSION="188" '
       'CODEPAGE="UTF-8" DATABASETYPE="Oracle"><FOLDER NAME="F" GROUP="" OWNER="o" SHARED="NOTSHARED">')
FTR = '</FOLDER></REPOSITORY></POWERMART>'

SRC = '''<SOURCE DATABASETYPE="Oracle" DBDNAME="S" NAME="TXN" OWNERNAME="SC">
 <SOURCEFIELD DATATYPE="number" NAME="TXN_ID" PRECISION="10" SCALE="0" NULLABLE="NOT NULL" KEYTYPE="PRIMARY KEY"/>
 <SOURCEFIELD DATATYPE="number" NAME="AMT" PRECISION="10" SCALE="2" NULLABLE="NULL"/></SOURCE>'''
SQ = '''<TRANSFORMATION NAME="SQ_TXN" TYPE="Source Qualifier" REUSABLE="NO">
 <TRANSFORMFIELD NAME="TXN_ID" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="2"/>
 <TABLEATTRIBUTE NAME="Source Table" VALUE="TXN"/></TRANSFORMATION>
<INSTANCE NAME="TXN" TRANSFORMATION_NAME="TXN" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
<INSTANCE NAME="SQ_TXN" TRANSFORMATION_NAME="SQ_TXN" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
<CONNECTOR FROMINSTANCE="TXN" FROMFIELD="TXN_ID" TOINSTANCE="SQ_TXN" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="TXN" FROMFIELD="AMT" TOINSTANCE="SQ_TXN" TOFIELD="AMT"/>'''


def TGT(name, cols=(("TXN_ID", "number", "10", "0", True), ("AMT", "number", "10", "2", False))):
    fields = "".join(
        f'<TARGETFIELD DATATYPE="{t}" NAME="{n}" PRECISION="{p}" SCALE="{s}"'
        + (' KEYTYPE="PRIMARY KEY" NULLABLE="NOT NULL"' if k else "") + "/>"
        for n, t, p, s, k in cols
    )
    return f'<TARGET DATABASETYPE="Oracle" DBDNAME="D" NAME="{name}">{fields}</TARGET>'


def generate(body, tmp_path, name="m"):
    p = tmp_path / f"{name}.xml"
    p.write_text(HDR + body + FTR, encoding="utf-8")
    result = InformaticaXMLParser().parse(str(p))
    mapping = result.mappings[0]
    conv = TransformationConverter()
    tr = {tx.name: "\n".join(conv.convert(tx)) for tx in mapping.transformations}
    nb = NotebookGenerator().generate(mapping, Session(name="s"),
                                      {"transformations": tr, "source_reads": [], "target_write": ""})
    code = "\n".join("".join(c["source"]) for c in json.loads(nb)["cells"] if c["cell_type"] == "code")
    return mapping, code


# ── Router ────────────────────────────────────────────────────────────

ROUTER = SRC + TGT("TGT_BIG") + TGT("TGT_SMALL") + '<MAPPING NAME="m_router" ISVALID="YES">' + SQ + '''
<TRANSFORMATION NAME="RTR" TYPE="Router" REUSABLE="NO">
 <TRANSFORMFIELD NAME="TXN_ID" GROUP="INPUT" DATATYPE="number" PORTTYPE="INPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT" GROUP="INPUT" DATATYPE="number" PORTTYPE="INPUT" PRECISION="10" SCALE="2"/>
 <TRANSFORMFIELD NAME="TXN_ID1" GROUP="GRP_SMALL" REF_FIELD="TXN_ID" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT1" GROUP="GRP_SMALL" REF_FIELD="AMT" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="2"/>
 <TRANSFORMFIELD NAME="TXN_ID2" GROUP="GRP_BIG" REF_FIELD="TXN_ID" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT2" GROUP="GRP_BIG" REF_FIELD="AMT" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="2"/>
 <GROUP NAME="INPUT" TYPE="INPUT" ORDER="1"/>
 <GROUP NAME="GRP_SMALL" TYPE="OUTPUT" ORDER="2" EXPRESSION="AMT &lt; 100"/>
 <GROUP NAME="GRP_BIG" TYPE="OUTPUT" ORDER="3" EXPRESSION="AMT &gt;= 100"/>
</TRANSFORMATION>
<INSTANCE NAME="RTR" TRANSFORMATION_NAME="RTR" TRANSFORMATION_TYPE="Router" TYPE="TRANSFORMATION"/>
<INSTANCE NAME="TGT_BIG" TRANSFORMATION_NAME="TGT_BIG" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
<INSTANCE NAME="TGT_SMALL" TRANSFORMATION_NAME="TGT_SMALL" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="TXN_ID" TOINSTANCE="RTR" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="AMT" TOINSTANCE="RTR" TOFIELD="AMT"/>
<CONNECTOR FROMINSTANCE="RTR" FROMFIELD="TXN_ID1" TOINSTANCE="TGT_SMALL" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="RTR" FROMFIELD="AMT1" TOINSTANCE="TGT_SMALL" TOFIELD="AMT"/>
<CONNECTOR FROMINSTANCE="RTR" FROMFIELD="TXN_ID2" TOINSTANCE="TGT_BIG" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="RTR" FROMFIELD="AMT2" TOINSTANCE="TGT_BIG" TOFIELD="AMT"/>
</MAPPING>'''


def test_router_input_group_is_not_an_output_group(tmp_path):
    """Every PowerCenter Router carries a <GROUP TYPE="INPUT">. It was parsed
    as a group with no condition, i.e. as the DEFAULT group, giving every
    Router a phantom output that received the rows matching nothing."""
    mapping, _ = generate(ROUTER, tmp_path)
    rtr = next(t for t in mapping.transformations if t.name == "RTR")
    assert [g["name"] for g in rtr.router_groups] == ["GRP_SMALL", "GRP_BIG"]


def test_router_group_dataframe_names_agree_between_converter_and_generator(tmp_path):
    """The converter emitted df_<GroupName> verbatim while the generator wired
    consumers to df_<lower_case>; with an upper-case group name (every real
    export) the consumer read a name never assigned -- NameError."""
    _, code = generate(ROUTER, tmp_path)
    ast.parse(code)
    assert "df_grp_small = df_source.filter(" in code
    assert "df_grp_big = df_source.filter(" in code
    assert "df_GRP_SMALL" not in code
    assert unresolved_names(code) == []


def test_router_groups_reach_their_targets_through_port_groups_not_position(tmp_path):
    """TGT_BIG is fed by the GRP_BIG ports (TXN_ID2/AMT2) and TGT_SMALL by
    GRP_SMALL -- but the target order and the alphabetical downstream order
    are both the REVERSE of the group order, so the old positional pairing
    wrote the small transactions to TGT_BIG."""
    _, code = generate(ROUTER, tmp_path)
    assert "df_tgt_tgt_big = df_grp_big" in code
    assert "df_tgt_tgt_small = df_grp_small" in code
    # the single-target 'df_final' path traces the first target the same way
    assert "df_final = df_grp_big" in code
    # Router output ports (TXN_ID1, AMT2...) are REF_FIELD copies of the
    # input ports; nothing tries to rename columns that do not exist.
    assert "('TXN_ID1'," not in code and "('AMT2'," not in code


# ── Expression: previous-row idiom ────────────────────────────────────

PREV = SRC + TGT("TGT_FLAGS") + '<MAPPING NAME="m_prev" ISVALID="YES">' + SQ + '''
<TRANSFORMATION NAME="EXP_PREV" TYPE="Expression" REUSABLE="NO">
 <TRANSFORMFIELD NAME="TXN_ID" DATATYPE="number" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT" DATATYPE="number" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="2"/>
 <TRANSFORMFIELD NAME="v_PREV_ID" DATATYPE="number" PORTTYPE="LOCAL VARIABLE" PRECISION="10" SCALE="0" EXPRESSION="v_CURR_ID"/>
 <TRANSFORMFIELD NAME="v_CURR_ID" DATATYPE="number" PORTTYPE="LOCAL VARIABLE" PRECISION="10" SCALE="0" EXPRESSION="TXN_ID"/>
 <TRANSFORMFIELD NAME="IS_FIRST" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="1" SCALE="0" EXPRESSION="IIF(v_PREV_ID = v_CURR_ID, 0, 1)"/>
</TRANSFORMATION>
<INSTANCE NAME="EXP_PREV" TRANSFORMATION_NAME="EXP_PREV" TRANSFORMATION_TYPE="Expression" TYPE="TRANSFORMATION"/>
<INSTANCE NAME="TGT_FLAGS" TRANSFORMATION_NAME="TGT_FLAGS" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="TXN_ID" TOINSTANCE="EXP_PREV" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="AMT" TOINSTANCE="EXP_PREV" TOFIELD="AMT"/>
<CONNECTOR FROMINSTANCE="EXP_PREV" FROMFIELD="TXN_ID" TOINSTANCE="TGT_FLAGS" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="EXP_PREV" FROMFIELD="AMT" TOINSTANCE="TGT_FLAGS" TOFIELD="AMT"/>
</MAPPING>'''


def test_variable_port_referencing_a_later_variable_is_previous_row_state(tmp_path):
    """v_PREV_ID = v_CURR_ID written ABOVE v_CURR_ID = TXN_ID is the standard
    PowerCenter idiom for 'the previous row's key'. It was emitted as a plain
    withColumn reading v_CURR_ID -- the CURRENT row -- so IS_FIRST was 0 for
    every row."""
    _, code = generate(PREV, tmp_path)
    # Now translated: the PREVIOUS row's v_CURR_ID (lag over the row order),
    # the numeric initial value 0 on the first row.
    assert 'F.when(F.row_number().over(_w) == 1, F.lit(0)).otherwise(F.lag("v_CURR_ID").over(_w))' in code
    assert 'withColumn("v_PREV_ID", F.col(\'v_CURR_ID\'))' not in code
    # the ordinary variable still converts
    assert 'withColumn("v_CURR_ID", F.col(\'TXN_ID\'))' in code


# ── Joiner ────────────────────────────────────────────────────────────

JOINER = SRC + '''<SOURCE DATABASETYPE="Oracle" DBDNAME="S" NAME="CUST" OWNERNAME="SC">
 <SOURCEFIELD DATATYPE="number" NAME="ID" PRECISION="10" SCALE="0" NULLABLE="NOT NULL" KEYTYPE="PRIMARY KEY"/>
 <SOURCEFIELD DATATYPE="varchar" NAME="NM" PRECISION="50" SCALE="0" NULLABLE="NULL"/></SOURCE>''' + TGT(
    "TGT_J", (("TXN_ID", "number", "10", "0", True), ("AMT", "number", "10", "2", False),
              ("CUST_NAME", "varchar", "50", "0", False))) + '<MAPPING NAME="m_join" ISVALID="YES">' + SQ + '''
<TRANSFORMATION NAME="SQ_CUST" TYPE="Source Qualifier" REUSABLE="NO">
 <TRANSFORMFIELD NAME="ID" DATATYPE="number" PORTTYPE="OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="NM" DATATYPE="varchar" PORTTYPE="OUTPUT" PRECISION="50" SCALE="0"/>
 <TABLEATTRIBUTE NAME="Source Table" VALUE="CUST"/></TRANSFORMATION>
<TRANSFORMATION NAME="JNR" TYPE="Joiner" REUSABLE="NO">
 <TRANSFORMFIELD NAME="TXN_ID" DATATYPE="number" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0"/>
 <TRANSFORMFIELD NAME="AMT" DATATYPE="number" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="2"/>
 <TRANSFORMFIELD NAME="CUST_KEY" DATATYPE="number" PORTTYPE="INPUT/OUTPUT" PRECISION="10" SCALE="0" MASTER="YES"/>
 <TRANSFORMFIELD NAME="CUST_NAME" DATATYPE="varchar" PORTTYPE="INPUT/OUTPUT" PRECISION="50" SCALE="0" MASTER="YES"/>
 <TABLEATTRIBUTE NAME="Join Condition" VALUE="CUST_KEY = TXN_ID"/>
 <TABLEATTRIBUTE NAME="Join Type" VALUE="Normal Join"/>
</TRANSFORMATION>
<INSTANCE NAME="CUST" TRANSFORMATION_NAME="CUST" TRANSFORMATION_TYPE="Source Definition" TYPE="SOURCE"/>
<INSTANCE NAME="SQ_CUST" TRANSFORMATION_NAME="SQ_CUST" TRANSFORMATION_TYPE="Source Qualifier" TYPE="TRANSFORMATION"/>
<INSTANCE NAME="JNR" TRANSFORMATION_NAME="JNR" TRANSFORMATION_TYPE="Joiner" TYPE="TRANSFORMATION"/>
<INSTANCE NAME="TGT_J" TRANSFORMATION_NAME="TGT_J" TRANSFORMATION_TYPE="Target Definition" TYPE="TARGET"/>
<CONNECTOR FROMINSTANCE="CUST" FROMFIELD="ID" TOINSTANCE="SQ_CUST" TOFIELD="ID"/>
<CONNECTOR FROMINSTANCE="CUST" FROMFIELD="NM" TOINSTANCE="SQ_CUST" TOFIELD="NM"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="TXN_ID" TOINSTANCE="JNR" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="SQ_TXN" FROMFIELD="AMT" TOINSTANCE="JNR" TOFIELD="AMT"/>
<CONNECTOR FROMINSTANCE="SQ_CUST" FROMFIELD="ID" TOINSTANCE="JNR" TOFIELD="CUST_KEY"/>
<CONNECTOR FROMINSTANCE="SQ_CUST" FROMFIELD="NM" TOINSTANCE="JNR" TOFIELD="CUST_NAME"/>
<CONNECTOR FROMINSTANCE="JNR" FROMFIELD="TXN_ID" TOINSTANCE="TGT_J" TOFIELD="TXN_ID"/>
<CONNECTOR FROMINSTANCE="JNR" FROMFIELD="AMT" TOINSTANCE="TGT_J" TOFIELD="AMT"/>
<CONNECTOR FROMINSTANCE="JNR" FROMFIELD="CUST_NAME" TOINSTANCE="TGT_J" TOFIELD="CUST_NAME"/>
</MAPPING>'''


def test_joiner_sides_are_renamed_to_the_joiner_port_names_before_the_join(tmp_path):
    """The master source's columns are ID/NM; the Joiner calls them
    CUST_KEY/CUST_NAME and its condition is CUST_KEY = TXN_ID. The old code
    joined df_source_1["CUST_KEY"] -- a column that did not exist (the
    Source Qualifier's connector renames were never emitted) -- and the
    target select then asked for CUST_NAME, which did not exist either."""
    _, code = generate(JOINER, tmp_path)
    ast.parse(code)
    assert "df_in_jnr_sq_cust = _rename_cols(df_source_1, [('ID', 'CUST_KEY'), ('NM', 'CUST_NAME')])" in code
    assert 'on=df_source["TXN_ID"] == df_in_jnr_sq_cust["CUST_KEY"]' in code
    assert 'df_final = df_final.select("TXN_ID", "AMT", "CUST_NAME")' in code
    assert unresolved_names(code) == []


def test_joiner_condition_keeps_port_names_and_orders_master_first():
    tc = TransformationConverter()
    assert tc._parse_joiner_condition("PRODUCT_ID_M = PRODUCT_ID_D") == [("PRODUCT_ID_M", "PRODUCT_ID_D")]
    assert tc._parse_joiner_condition("A1 = A AND B1 = B") == [("A1", "A"), ("B1", "B")]
    from infa2aidp.models import DataFlowDirection, Transformation, TransformationField, TransformationType
    tx = Transformation(name="J", type=TransformationType.JOINER, fields=[
        TransformationField(name="A", direction=DataFlowDirection.INPUT_OUTPUT),
        TransformationField(name="A_M", direction=DataFlowDirection.INPUT_OUTPUT, is_master=True),
    ])
    # written detail = master: the MASTER flag decides, not the position
    assert tc._parse_joiner_condition("A = A_M", tx) == [("A_M", "A")]


# ── Workflow ──────────────────────────────────────────────────────────

def test_workflow_dependency_survives_a_dropped_task_between_two_sessions():
    """s_a -> cmd_archive -> s_b: the Command task is reported as dropped,
    but s_b still depends on s_a. The old graph kept only direct
    session->session links, so s_b had no predecessor and could run first."""
    wf = Workflow(name="wf", sessions=["s_a", "s_b"])
    wf.tasks = [{"name": "Start", "type": "Start"}, {"name": "s_a", "type": "Session"},
                {"name": "cmd", "type": "Command"}, {"name": "s_b", "type": "Session"}]
    wf.dependencies = [{"from_task": "Start", "to_task": "s_a", "condition": ""},
                       {"from_task": "s_a", "to_task": "cmd", "condition": ""},
                       {"from_task": "cmd", "to_task": "s_b", "condition": ""}]
    tr = WorkflowGenerator().generate(wf, {"s_a": "/a", "s_b": "/b"})
    deps = {t["taskKey"]: [d["taskKey"] for d in t["dependsOn"]] for t in tr.job["tasks"]}
    assert deps == {"s_a": [], "s_b": ["s_a"]}
    assert any("COMMAND" in r and "cmd" in r for r in tr.not_translated)


def test_workflow_direct_session_links_still_work():
    wf = Workflow(name="wf", sessions=["s_a", "s_b", "s_c"])
    wf.tasks = [{"name": n, "type": "Session"} for n in ("s_a", "s_b", "s_c")]
    wf.dependencies = [{"from_task": "s_a", "to_task": "s_b", "condition": ""},
                       {"from_task": "s_a", "to_task": "s_c", "condition": ""},
                       {"from_task": "s_b", "to_task": "s_c", "condition": ""}]
    tr = WorkflowGenerator().generate(wf, {n: f"/{n}" for n in ("s_a", "s_b", "s_c")})
    deps = {t["taskKey"]: sorted(d["taskKey"] for d in t["dependsOn"]) for t in tr.job["tasks"]}
    assert deps == {"s_a": [], "s_b": ["s_a"], "s_c": ["s_a", "s_b"]}


# ── Key columns ───────────────────────────────────────────────────────

def test_key_columns_are_not_duplicated_across_targets(tmp_path):
    _, code = generate(ROUTER, tmp_path)
    # Each target's write cell uses its OWN keys, once each.
    import re as _re
    key_lists = (_re.findall(r"keys=(\[[^\]]*\])", code)
                 + _re.findall(r"key_columns = (\[[^\]]*\])", code))
    for kl in key_lists:
        keys = __import__("ast").literal_eval(kl)
        assert len(keys) == len(set(keys)), kl
    # Both targets are INSERT loads: they append, one cell each, and no
    # longer MERGE on keys (INSERT was upgraded to UPSERT for multi-target).
    assert code.count('.write.mode("append").saveAsTable(') == 2, code
