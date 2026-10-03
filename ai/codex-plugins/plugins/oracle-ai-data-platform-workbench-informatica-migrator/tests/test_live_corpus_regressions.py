"""Regressions from executing all 12 corpus notebooks on AIDP (2026-09-25).

The first live run (PR "Live run") executed one notebook. This one ran every
corpus notebook as an AIDP job on Spark 3.5.0 against scratch Delta tables,
deployed the orchestration fixture's multi-task workflows, and probed how a
notebook task receives job parameters. Six of twelve notebooks failed on
the cluster although the offline suite called them clean; the job and
parameter shapes were rejected by the jobs API. Each test here reproduces
one of those findings against fakes, and where pyspark is installed, runs
the generated expression on a local Spark of the AIDP version.
"""
from __future__ import annotations

import logging
import os
import types

import pytest

from infa2aidp.converters.expression_converter import ExpressionConverter
from infa2aidp.converters.transformation_converter import (
    TransformationConverter,
    _null_of,
    _port_spark_type,
)
from infa2aidp.deployer.aidp_client import AIDPClient
from infa2aidp.generators.code_validation import unresolved_names
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.generators.workflow_generator import WorkflowGenerator
from infa2aidp.models import (
    DataFlowDirection,
    Session,
    Transformation,
    TransformationField,
    TransformationType,
    Workflow,
)
from infa2aidp.parsers.format_detector import detect_and_parse_file

ROOT = os.path.dirname(os.path.dirname(__file__))
CORPUS = os.path.join(ROOT, "tests", "fixtures", "corpus")


def _code(tx: Transformation, **extra) -> str:
    return "\n".join(TransformationConverter().convert(tx, input_df="df_source", output_df="df_source", **extra))


# ---------------------------------------------------------------------------
# 1. Lookup conditions with instance-qualified names
# ---------------------------------------------------------------------------

def _lookup(name: str, condition: str, fields=()) -> Transformation:
    return Transformation(name=name, type=TransformationType.LOOKUP, lookup_table="DIM",
                          lookup_condition=condition, fields=list(fields))


class TestQualifiedLookupCondition:
    def test_own_name_qualifier_marks_the_lookup_side_and_is_stripped(self):
        code = _code(_lookup("LKP_CURRENT",
                             "LKP_CURRENT.EMP_ID = SQ_EMPLOYEES.EMP_ID AND LKP_CURRENT.IS_CURRENT = 1"))
        assert "F.col('IS_CURRENT') == F.lit(1)" in code
        assert "on=['EMP_ID']" in code
        # the LKP_ rule used to fire on the qualifier: LKP_CURRENT.X -> CURRENT.X
        assert "CURRENT." not in code.replace("LKP_CURRENT", "")
        assert "SQ_EMPLOYEES." not in code

    def test_reversed_sides_with_different_names_rename_to_the_pipeline_column(self):
        code = _code(_lookup("LKP_T", "SQ_X.EMP_ID = LKP_T.EMP_NO"))
        assert '.withColumnRenamed("EMP_NO", "EMP_ID")' in code
        assert "on=['EMP_ID']" in code

    def test_unqualified_lkp_prefix_convention_still_applies(self):
        code = _code(_lookup("LKP_CUST", "LKP_CUSTOMER_ID = CUSTOMER_ID AND LKP_CURRENT_FLAG = 'Y'"))
        assert "on=['CUSTOMER_ID']" in code
        assert "F.col('CURRENT_FLAG') == F.lit('Y')" in code

    def test_corpus_lookups_emit_no_dotted_column_names(self, tmp_path):
        for fixture in ("scd_type2.xml", "star_schema_fact.xml"):
            mapping = detect_and_parse_file(os.path.join(CORPUS, fixture)).mappings[0]
            for tx in mapping.transformations:
                if tx.type == TransformationType.LOOKUP:
                    code = _code(tx)
                    assert "cached_lookup(" in code
                    for bad in ("F.col(\"CURRENT.", "F.col('CURRENT.", "on=['SQ_", "on=['LKP_"):
                        assert bad not in code, (fixture, tx.name, code)


# ---------------------------------------------------------------------------
# 2. IDMC Lookup condition carried as joinCondition
# ---------------------------------------------------------------------------

def test_iics_lookup_reads_join_condition():
    mapping = detect_and_parse_file(os.path.join(CORPUS, "star_schema_fact.json")).mappings[0]
    lkp = next(t for t in mapping.transformations if t.type == TransformationType.LOOKUP)
    assert lkp.lookup_condition == "LKP_PRODUCT.PRODUCT_CODE = SRC_SALES.PRODUCT_CODE"
    code = _code(lkp)
    assert "TODO: add lookup condition" not in code
    assert "on=['PRODUCT_CODE']" in code


# ---------------------------------------------------------------------------
# 3. SUBSTR with a computed start runs on Spark 3.5
# ---------------------------------------------------------------------------

class TestSubstrOnSpark35:
    def test_literal_positions_keep_f_substring(self):
        assert ExpressionConverter().convert("SUBSTR(S, 2, 3)") == "F.substring(F.col('S'), 2, 3)"

    def test_computed_start_uses_column_substr_with_column_arguments(self):
        code = ExpressionConverter().convert("SUBSTR(EMAIL, INSTR(EMAIL, '@') + 1)")
        assert "F.substring(" not in code
        assert ".substr(" in code and "F.lit(2147483647)" in code

    def test_computed_start_executes_on_local_spark(self):
        pytest.importorskip("pyspark")
        from tests.conftest_spark import run_expr
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("substr35").getOrCreate()
        out = run_expr(spark, "SUBSTR(EMAIL, INSTR(EMAIL, '@') + 1)",
                       [("ann@example.com",), ("nobody",), (None,)], "EMAIL string")
        # INSTR returns 0 when absent; 0 + 1 = 1 -> the whole string, as in Informatica.
        assert out == ["example.com", "nobody", None], out


# ---------------------------------------------------------------------------
# 4. Normalizer: SQL-valid stack(), or an honest marker
# ---------------------------------------------------------------------------

def _normalizer(fields) -> Transformation:
    return Transformation(name="NRM", type=TransformationType.NORMALIZER, fields=[
        TransformationField(name=n, datatype=t, direction=d) for n, t, d in fields])


class TestNormalizer:
    def test_same_typed_inputs_stack_with_sql_column_references(self):
        code = _code(_normalizer([
            ("Q1", "decimal", DataFlowDirection.INPUT), ("Q2", "decimal", DataFlowDirection.INPUT),
            ("SALES", "decimal", DataFlowDirection.OUTPUT)]))
        assert "stack(2, 'Q1', `Q1`, 'Q2', `Q2`) as (source_col, SALES)" in code
        assert "F.col('Q1')" not in code   # Python inside SQL: UNRESOLVED_ROUTINE `F`.`col`

    def test_mixed_typed_inputs_are_flagged_not_stacked(self):
        code = _code(_normalizer([
            ("PRODUCT_ID", "integer", DataFlowDirection.INPUT), ("SALES_ARRAY", "string", DataFlowDirection.INPUT),
            ("MONTH_SALE", "decimal", DataFlowDirection.OUTPUT)]))
        assert "REVIEW REQUIRED: Normalizer 'NRM' has no OCCURS metadata" in code
        assert "selectExpr(" not in code   # no stack() call emitted

    def test_stack_executes_on_local_spark(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession, functions as F  # noqa: F401
        spark = SparkSession.builder.master("local[1]").appName("nrm").getOrCreate()
        code = _code(_normalizer([
            ("Q1", "integer", DataFlowDirection.INPUT), ("Q2", "integer", DataFlowDirection.INPUT),
            ("SALES", "integer", DataFlowDirection.OUTPUT)]))
        ns = {"F": F, "df_source": spark.createDataFrame([(1, 10, 20)], "ID int, Q1 int, Q2 int")}
        exec(code, ns)
        rows = sorted((r["source_col"], r["SALES"]) for r in ns["df_source"].collect())
        assert rows == [("Q1", 10), ("Q2", 20)]


# ---------------------------------------------------------------------------
# 5. Typed NULL placeholders
# ---------------------------------------------------------------------------

class TestTypedNullPlaceholder:
    @pytest.mark.parametrize("dt,p,s,expected", [
        ("string", 0, 0, "string"), ("varchar2", 30, 0, "string"), ("integer", 10, 0, "int"),
        ("number", 12, 2, "decimal(12,2)"), ("number", 5, 0, "int"), ("number", 15, 0, "bigint"),
        ("number", 0, 2, "decimal(38,2)"),  # unknown precision is not "tiny"
        ("decimal", 0, 0, "decimal(38,10)"), ("date", 0, 0, "date"), ("date/time", 0, 0, "timestamp"),
        ("double", 0, 0, "double"),
    ])
    def test_port_type_mapping(self, dt, p, s, expected):
        assert _port_spark_type(TransformationField(name="X", datatype=dt, precision=p, scale=s)) == expected

    def test_unconvertible_expression_port_gets_a_typed_null(self):
        tx = Transformation(name="EXP", type=TransformationType.EXPRESSION, fields=[
            TransformationField(name="PHONE", datatype="string", direction=DataFlowDirection.INPUT),
            TransformationField(name="CLEAN_PHONE", datatype="string", direction=DataFlowDirection.OUTPUT,
                                expression="REPLACE(PHONE, '-', '')"),
        ])
        code = _code(tx)
        assert 'withColumn("CLEAN_PHONE", F.lit(None).cast("string"))' in code
        assert 'F.lit(None))' not in code

    def test_null_helper(self):
        assert _null_of(TransformationField(name="A", datatype="number", precision=10, scale=2)) == \
            'F.lit(None).cast("decimal(10,2)")'


# ---------------------------------------------------------------------------
# 6. Job parameters: the shape the API accepts, and where the notebook reads them
# ---------------------------------------------------------------------------

def test_session_parameters_become_a_name_value_list():
    wf = Workflow(name="wf", sessions=[Session(name="s_m", mapping_name="m",
                                               parameters={"$$LOAD_DATE": "2026-01-01", "$$BATCH_ID": "7"})])
    job = WorkflowGenerator().generate(wf, {"s_m": "/Workspace/Migrated/nb_m.ipynb"}).job
    params = job["tasks"][0]["parameters"]
    assert isinstance(params, list)
    assert {"name": "migration.load_date", "value": "2026-01-01"} in params
    assert {"name": "migration.batch_id", "value": "7"} in params


class _Conf:
    def __init__(self, d):
        self.d = d

    def get(self, k, default=None):
        return self.d.get(k, default)


class _Params:
    def __init__(self, d):
        self.d = d

    def getParameter(self, k, default):  # the two-argument form AIDP supports
        return self.d.get(k, default)


class TestParamLookupOrder:
    def _param_fn(self, spark_conf, job_params=None):
        mapping = detect_and_parse_file(os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")).mappings[0]
        cell = NotebookGenerator()._parameters_cell(mapping, Session(name="s"))
        ns = {"spark": types.SimpleNamespace(conf=_Conf(spark_conf))}
        if job_params is not None:
            ns["oidlUtils"] = types.SimpleNamespace(parameters=_Params(job_params))
        exec(cell, ns)
        return ns["_param"]

    def test_aidp_job_parameter_wins(self):
        f = self._param_fn({"migration.batch_id": "conf"}, {"migration.batch_id": "job"})
        assert f("BATCH_ID") == "job"

    def test_bare_name_job_parameter_is_accepted(self):
        assert self._param_fn({}, {"BATCH_ID": "bare"})("BATCH_ID") == "bare"

    def test_spark_conf_then_default_when_no_job_parameter(self):
        assert self._param_fn({"migration.batch_id": "conf"}, {})("BATCH_ID") == "conf"
        assert self._param_fn({}, {})("BATCH_ID") == "0"

    def test_outside_aidp_without_oidlutils(self):
        assert self._param_fn({"migration.batch_id": "conf"})("BATCH_ID") == "conf"

    def test_validator_accepts_oidlutils_as_a_kernel_global(self):
        assert unresolved_names("x = oidlUtils.parameters.getParameter('a', 'b')\n") == []


# ---------------------------------------------------------------------------
# 7. Schedule keys and pause state
# ---------------------------------------------------------------------------

def test_previously_generated_schedule_is_upgraded_to_the_accepted_keys():
    wf = Workflow(name="wf", scheduler={"cronExpression": "0 0 1 * * ?", "timezone": "UTC"})
    sched = WorkflowGenerator().generate(wf, {}).job.get("schedule")
    assert sched == {"quartzCronExpression": "0 0 1 * * ?", "timezoneId": "UTC", "pauseStatus": "PAUSED"}


# ---------------------------------------------------------------------------
# 8. Client: retry transient statuses, stay quiet on an expected 409
# ---------------------------------------------------------------------------

class _Resp:
    def __init__(self, status, text="", headers=None):
        self.status_code, self.text, self.headers = status, text, headers or {}
        self.ok = 200 <= status < 300

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError(self.status_code)


def _client(responses):
    c = AIDPClient.__new__(AIDPClient)
    c.base_url, c.workspace_key = "https://x", "ws"
    seq = iter(responses)
    c._session = types.SimpleNamespace(request=lambda *a, **k: next(seq))
    return c


def test_transient_503_is_retried(monkeypatch):
    monkeypatch.setattr("infa2aidp.deployer.aidp_client.time.sleep", lambda s: None)
    c = _client([_Resp(503, "Service Unavailable"), _Resp(200)])
    assert c._request("POST", "/workspaces/ws/actions/uploadFileMeta").status_code == 200


def test_mkdir_on_an_existing_folder_is_not_logged_as_an_error(caplog):
    c = _client([_Resp(409, '{"code":"Conflict","message":"Directory already exists "}')])
    with caplog.at_level(logging.ERROR, logger="infa2aidp.deployer.aidp_client"):
        c.mkdir("/Workspace/Migrated")
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# ---------------------------------------------------------------------------
# 9. A Lookup returns only its own ports
# ---------------------------------------------------------------------------

def test_plainly_named_output_ports_project_the_lookup_table():
    tx = _lookup("LKP_CUSTOMER_DIM", "LKP_CUSTOMER_DIM.CUSTOMER_CODE = SQ_SALES_STAGE.CUSTOMER_CODE", fields=[
        TransformationField(name="CUSTOMER_CODE", datatype="string", direction=DataFlowDirection.INPUT),
        TransformationField(name="CUSTOMER_SK", datatype="integer", direction=DataFlowDirection.OUTPUT),
        TransformationField(name="REGION", datatype="string", direction=DataFlowDirection.OUTPUT),
    ])
    code = _code(tx)
    assert 'select(F.col("CUSTOMER_CODE"), F.col("CUSTOMER_SK"), F.col("REGION"))' in code


def test_lkp_prefixed_ports_keep_their_alias():
    tx = _lookup("LKP_C", "LKP_C.EMP_ID = SQ.EMP_ID", fields=[
        TransformationField(name="LKP_DEPT", datatype="string", direction=DataFlowDirection.OUTPUT),
    ])
    assert 'F.col("DEPT").alias("LKP_DEPT")' in _code(tx)


def test_two_lookups_on_one_table_do_not_collide_on_local_spark():
    pytest.importorskip("pyspark")
    import sys
    from pyspark.sql import SparkSession, functions as F
    sys.path.insert(0, os.path.join(ROOT, "engine"))
    import infa_compat  # noqa: F401
    spark = SparkSession.builder.master("local[1]").appName("lkp2").getOrCreate()
    dim = spark.createDataFrame([("C1", 11, "EU", "P1", 21)],
                                "CUSTOMER_CODE string, CUSTOMER_SK int, REGION string, PRODUCT_CODE string, PRODUCT_SK int")
    ns = {"F": F, "spark": spark, "infa_compat": infa_compat,
          "df_source": spark.createDataFrame([(1, "C1", "P1")], "SALE_ID int, CUSTOMER_CODE string, PRODUCT_CODE string")}
    for name, key, outs in (("LKP_CUSTOMER_DIM", "CUSTOMER_CODE", ("CUSTOMER_SK", "REGION")),
                            ("LKP_PRODUCT_DIM", "PRODUCT_CODE", ("PRODUCT_SK",))):
        tx = _lookup(name, f"{name}.{key} = SQ.{key}", fields=[
            TransformationField(name=key, datatype="string", direction=DataFlowDirection.INPUT)] + [
            TransformationField(name=o, datatype="string", direction=DataFlowDirection.OUTPUT) for o in outs])
        code = _code(tx).replace('spark.table("DIM")', "_dim")
        ns["_dim"] = dim
        exec(code, ns)
    row = ns["df_source"].select("SALE_ID", "CUSTOMER_SK", "REGION", "PRODUCT_SK").collect()[0]
    assert tuple(row) == (1, 11, "EU", 21)


# ---------------------------------------------------------------------------
# 10. Self-review fixes (PR #6)
# ---------------------------------------------------------------------------

def _normalizer_with_key():
    return _normalizer([
        ("PRODUCT_ID", "integer", DataFlowDirection.INPUT),
        ("SALES_1", "integer", DataFlowDirection.INPUT), ("SALES_2", "integer", DataFlowDirection.INPUT),
        ("SALES", "integer", DataFlowDirection.OUTPUT)])


def test_normalizer_never_stacks_a_same_typed_key():
    code = _code(_normalizer_with_key())
    assert "stack(2, 'SALES_1', `SALES_1`, 'SALES_2', `SALES_2`)" in code
    assert "'PRODUCT_ID'" not in code
    assert "drop('SALES_1', 'SALES_2')" in code


def test_normalizer_carries_the_key_on_every_occurrence_on_local_spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession, functions as F
    spark = SparkSession.builder.master("local[1]").appName("nrmkey").getOrCreate()
    ns = {"F": F, "df_source": spark.createDataFrame([(101, 10, 20)], "PRODUCT_ID int, SALES_1 int, SALES_2 int")}
    exec(_code(_normalizer_with_key()), ns)
    rows = sorted(tuple(r) for r in ns["df_source"].select("PRODUCT_ID", "source_col", "SALES").collect())
    assert rows == [(101, "SALES_1", 10), (101, "SALES_2", 20)]


def test_normalizer_without_a_numbered_group_is_flagged_even_when_types_match():
    code = _code(_normalizer([
        ("REGION", "string", DataFlowDirection.INPUT), ("CITY", "string", DataFlowDirection.INPUT),
        ("PLACE", "string", DataFlowDirection.OUTPUT)]))
    assert "REVIEW REQUIRED: Normalizer 'NRM' has no OCCURS metadata" in code
    assert "selectExpr(" not in code


def test_lookup_table_name_qualifier_marks_the_lookup_side():
    tx = Transformation(name="LKP_CUSTOMER_DIM", type=TransformationType.LOOKUP, lookup_table="DW.CUSTOMER_DIM",
                        lookup_condition="SQ.CUST_NO = CUSTOMER_DIM.CUSTOMER_ID")
    code = _code(tx)
    assert '.withColumnRenamed("CUSTOMER_ID", "CUST_NO")' in code
    assert "on=['CUST_NO']" in code


def test_key_only_lookup_is_still_projected():
    tx = _lookup("LKP_EXISTS", "LKP_EXISTS.CUSTOMER_ID = SQ.CUSTOMER_ID", fields=[
        TransformationField(name="CUSTOMER_ID", datatype="integer", direction=DataFlowDirection.OUTPUT)])
    assert 'select(F.col("CUSTOMER_ID"))' in _code(tx)


def test_post_is_not_replayed_after_a_gateway_error(monkeypatch):
    monkeypatch.setattr("infa2aidp.deployer.aidp_client.time.sleep", lambda s: None)
    c = _client([_Resp(504, "Gateway Timeout"), _Resp(201)])
    assert c._request("POST", "/workspaces/ws/jobs").status_code == 504  # returned, not retried


def test_get_is_retried_after_a_gateway_error(monkeypatch):
    monkeypatch.setattr("infa2aidp.deployer.aidp_client.time.sleep", lambda s: None)
    c = _client([_Resp(502), _Resp(200)])
    assert c._request("GET", "/workspaces/ws/jobs").status_code == 200


def test_retry_after_as_http_date_does_not_crash(monkeypatch):
    slept = []
    monkeypatch.setattr("infa2aidp.deployer.aidp_client.time.sleep", slept.append)
    c = _client([_Resp(503, headers={"retry-after": "Fri, 25 Sep 2026 09:00:00 GMT"}), _Resp(200)])
    assert c._request("POST", "/workspaces/ws/actions/uploadFileMeta").status_code == 200
    assert slept and 0 <= slept[0] <= 30


class _BrokenParams:
    def getParameter(self, k, default):
        raise RuntimeError("py4j: outside a job context")


def test_param_falls_back_when_the_aidp_lookup_raises():
    mapping = detect_and_parse_file(os.path.join(ROOT, "tests", "fixtures", "corpus", "orders_transform.xml")).mappings[0]
    cell = NotebookGenerator()._parameters_cell(mapping, Session(name="s"))
    ns = {"spark": types.SimpleNamespace(conf=_Conf({"migration.batch_id": "conf"})),
          "oidlUtils": types.SimpleNamespace(parameters=_BrokenParams())}
    exec(cell, ns)
    assert ns["_param"]("BATCH_ID") == "conf"
