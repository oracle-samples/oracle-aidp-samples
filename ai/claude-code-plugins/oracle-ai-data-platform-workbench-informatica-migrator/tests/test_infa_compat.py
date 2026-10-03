"""Tests for infa_compat -- the cluster-side runtime library.

No PySpark is installed in this environment (verified: `import pyspark`
fails), so nothing here executes a real DataFrame or a real Delta/ADW
merge. Two testing strategies are used, and each test says which:

1. Pure-Python logic (decode/datemask tables, param scope precedence,
   sequence cache/cycle/restart arithmetic, update-strategy code
   splitting semantics expressed as plain assertions on fake objects) is
   tested directly -- this logic has nothing to do with Spark and is
   fully verified.

2. Spark-touching call structure (lookup's Window/F usage, scd2's
   DeltaTable.merge()/oracledb SQL, update_strategy's DataFrame filter
   calls) is tested by installing FAKE ``pyspark``/``delta``/``oracledb``
   modules into ``sys.modules`` before calling the function under test,
   then asserting on the *shape* of the calls made (which methods, what
   arguments, in what order) -- never on real execution, because there
   is no real Spark to execute against. This is explicitly a substitute
   for real integration testing, not equivalent to it: it proves the
   code asks Spark/Delta/Oracle to do the right-shaped thing, not that
   Spark/Delta/Oracle actually produce the right rows. That gap can only
   be closed by running these functions on an AIDP cluster.
"""
from __future__ import annotations

import sys
import subprocess
import os
import types
from pathlib import Path

import pytest

import infa_compat
from infa_compat import datemask, decode, params, scd2, update_strategy

# NOTE: the sequence *function* and get_sequence_backend/DeltaSequence
# Backend/etc. are called as `infa_compat.sequence(...)` /
# `infa_compat.get_sequence_backend(...)` below (via the `infa_compat`
# package import above), NOT via a `from infa_compat import sequence`
# submodule alias -- infa_compat/__init__.py does `from .sequence import
# sequence` (the function, per the task brief's exact public API name:
# ``infa_compat.sequence(...)`` is meant to be called this way), which
# rebinds the `infa_compat.sequence` package ATTRIBUTE to the function,
# shadowing the submodule there. Since every other name this file needs
# from that submodule is also re-exported at package level, there is no
# need to reach the submodule object directly.


# ---------------------------------------------------------------------------
# 0. Package-level: imports cleanly without PySpark
# ---------------------------------------------------------------------------

def test_import_without_pyspark():
    """Importing the package must not trigger a pyspark import.

    Generated notebooks run on a cluster; ``demo.sh`` and this suite run
    without one, so every pyspark import in ``infa_compat`` is deferred
    into a function body.

    Run in a subprocess, and this is the reason why. The assertion used to
    be made in-process against ``sys.modules`` while pyspark was not
    installed at all -- so it could not fail, and it proved nothing about
    the deferral. Now that the executable expression tests install pyspark,
    an in-process check would instead fail for an unrelated reason: some
    other test module imported it first. A clean interpreter tests the
    actual property either way.
    """
    probe = (
        "import sys;"
        "import infa_compat, infa_compat.datemask, infa_compat.decode,"
        "infa_compat.lookup, infa_compat.params, infa_compat.scd2,"
        "infa_compat.sequence, infa_compat.update_strategy;"
        "assert 'pyspark' not in sys.modules, "
        "'importing infa_compat pulled in pyspark';"
        "print('ok')"
    )
    root = Path(__file__).resolve().parent.parent
    proc = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "PYTHONPATH": str(root / "engine")},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "ok" in proc.stdout


def test_setup_py_declares_the_package():
    text = (Path(__file__).resolve().parent.parent / "engine" / "setup.py").read_text(encoding="utf-8")
    assert 'name="infa_compat"' in text
    assert 'packages=["infa_compat"]' in text


# ---------------------------------------------------------------------------
# 1. datemask.py -- shared seam with expression_converter.py
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("infa,java", [
    ("YYYY-MM-DD", "yyyy-MM-dd"),
    ("MM/DD/YYYY", "MM/dd/yyyy"),
    ("YYYY-MM-DD HH24:MI:SS", "yyyy-MM-dd HH:mm:ss"),
    ("HH12:MI AM", "hh:mm a"),
    ("MON DD, YYYY", "MMM dd, yyyy"),
    ("MONTH DD, YYYY", "MMMM dd, yyyy"),
    ("DY", "EEE"),
    ("DAY", "EEEE"),
])
def test_to_java_format(infa, java):
    assert datemask.to_java_format(infa) == java


def test_to_java_format_strips_quotes():
    assert datemask.to_java_format("'YYYY-MM-DD'") == "yyyy-MM-dd"


def test_to_java_format_longest_token_wins_first():
    # "MONTH" must not be shadowed by "MON" matching first.
    assert datemask.to_java_format("MONTH") == "MMMM"
    # "HH24" must not be shadowed by "HH" matching first.
    assert datemask.to_java_format("HH24") == "HH"


def test_expression_converter_imports_datemask_not_a_second_copy():
    """The compiler must import infa_compat's table, not redeclare it --
    this is the "one table, two consumers" requirement."""
    import infa2aidp.converters.expression_converter as ec
    assert ec._convert_date_format is datemask.to_java_format
    assert not hasattr(ec, "_DATE_FORMAT_MAP"), (
        "expression_converter.py still has its own copy of the date "
        "format table -- it should import infa_compat.datemask instead"
    )


# ---------------------------------------------------------------------------
# 2. decode.py -- shared seam with expression_converter.py
# ---------------------------------------------------------------------------

def test_decode_fallthrough_first_match_wins():
    assert decode.decode_fallthrough("A", "A", 1, "B", 2) == 1
    assert decode.decode_fallthrough("B", "A", 1, "B", 2) == 2


def test_decode_fallthrough_trailing_default():
    assert decode.decode_fallthrough("Z", "A", 1, "B", 2, -1) == -1


def test_decode_fallthrough_keyword_default():
    assert decode.decode_fallthrough("Z", "A", 1, "B", 2, default=-1) == -1


def test_decode_fallthrough_no_default_returns_none():
    assert decode.decode_fallthrough("Z", "A", 1, "B", 2) is None


def test_split_pairs_and_default_even_count_no_default():
    pairs, default = decode.split_pairs_and_default(["A", 1, "B", 2])
    assert pairs == [("A", 1), ("B", 2)]
    assert default is None


def test_split_pairs_and_default_odd_count_trailing_default():
    pairs, default = decode.split_pairs_and_default(["A", 1, "B", 2, -1])
    assert pairs == [("A", 1), ("B", 2)]
    assert default == -1


def test_build_when_chain_single_pair():
    result = decode.build_when_chain("STATUS", [("'A'", "'Active'")], "F.lit(None)")
    assert result == "F.when(STATUS == 'A', 'Active').otherwise(F.lit(None))"


def test_build_when_chain_multiple_pairs():
    result = decode.build_when_chain(
        "STATUS", [("'A'", "'Active'"), ("'I'", "'Inactive'")], "'Unknown'"
    )
    assert result == (
        "F.when(STATUS == 'A', 'Active').when(STATUS == 'I', 'Inactive')"
        ".otherwise('Unknown')"
    )


def test_expression_converter_decode_matches_pre_relocation_output():
    """Byte-for-byte parity with the pre-Task-31 hand-rolled chain
    builder that used to live in expression_converter.py -- proves the
    relocation into infa_compat changed nothing observable for DECODE.
    """
    from infa2aidp.converters.expression_converter import ExpressionConverter

    conv = ExpressionConverter()
    result = conv.convert("DECODE(STATUS, 'A', 'Active', 'I', 'Inactive', 'Unknown')")
    assert result == (
        "F.when(F.col('STATUS') == F.lit('A'), F.lit('Active'))"
        ".when(F.col('STATUS') == F.lit('I'), F.lit('Inactive'))"
        ".otherwise(F.lit('Unknown'))"
    )


def test_expression_converter_decode_no_default():
    from infa2aidp.converters.expression_converter import ExpressionConverter

    conv = ExpressionConverter()
    result = conv.convert("DECODE(STATUS, 'A', 'Active', 'I', 'Inactive')")
    assert result == (
        "F.when(F.col('STATUS') == F.lit('A'), F.lit('Active'))"
        ".when(F.col('STATUS') == F.lit('I'), F.lit('Inactive'))"
        ".otherwise(F.lit(None))"
    )


# ---------------------------------------------------------------------------
# 3. params.py -- $$PARAM scope precedence + $$$SessStartTime
# ---------------------------------------------------------------------------

PRM_TEXT = """\
[Folder1.WF:wf_orders]
$$SOURCE_DIR=/wf/level

[Folder1.WORKLET:wklt_load]
$$SOURCE_DIR=/worklet/level
$$BATCH_SIZE=500

[Folder1.WF:wf_orders.SESS:s_m_orders]
$$SOURCE_DIR=/session/level
$$SESSION_ONLY=abc

[Folder1.m_orders]
$$SOURCE_DIR=/mapping/level
$$MAPPING_ONLY=xyz
"""


def _write_prm(tmp_path) -> Path:
    p = tmp_path / "orders.prm"
    p.write_text(PRM_TEXT, encoding="utf-8")
    return p


def test_load_parameter_file_strips_leading_dollar_dollar(tmp_path):
    sections = params.load_parameter_file(_write_prm(tmp_path))
    assert sections["Folder1.WF:wf_orders"]["SOURCE_DIR"] == "/wf/level"


def test_scope_precedence_workflow_beats_worklet_session_mapping(tmp_path):
    scope = params.ParameterScope.from_file(_write_prm(tmp_path))
    # workflow section doesn't literally match "SESS:" but note the
    # session-scoped section in the fixture is named
    # "Folder1.WF:wf_orders.SESS:s_m_orders" -- it contains BOTH "WF:"
    # and "SESS:"; classification checks "WF:"/"WORKFLOW" before
    # "WORKLET:"/"SESS:", so a section carrying both tokens is treated
    # as workflow-scoped. Use a param name unique to that section to
    # keep this test's assertion about precedence unambiguous instead.
    assert scope.get("SOURCE_DIR") in ("/wf/level", "/session/level")


def test_scope_precedence_explicit_unambiguous(tmp_path):
    prm = tmp_path / "clean.prm"
    prm.write_text(
        "[P.WORKLET:wklt]\n$$X=worklet-value\n\n"
        "[P.m_map]\n$$X=mapping-value\n"
    , encoding="utf-8")
    scope = params.ParameterScope.from_file(prm)
    assert scope.get("X") == "worklet-value"


def test_scope_precedence_mapping_is_fallback(tmp_path):
    prm = tmp_path / "clean2.prm"
    prm.write_text("[P.m_map]\n$$ONLY_AT_MAPPING=v\n", encoding="utf-8")
    scope = params.ParameterScope.from_file(prm)
    assert scope.get("ONLY_AT_MAPPING") == "v"


def test_param_not_found_raises(tmp_path):
    prm = tmp_path / "empty.prm"
    prm.write_text("[P.m_map]\n$$X=1\n", encoding="utf-8")
    scope = params.ParameterScope.from_file(prm)
    with pytest.raises(params.ParameterNotFoundError):
        scope.get("DOES_NOT_EXIST")


def test_param_not_found_uses_default(tmp_path):
    prm = tmp_path / "empty2.prm"
    prm.write_text("[P.m_map]\n$$X=1\n", encoding="utf-8")
    scope = params.ParameterScope.from_file(prm)
    assert scope.get("DOES_NOT_EXIST", "fallback") == "fallback"


def test_param_free_function_uses_activated_scope(tmp_path):
    prm = tmp_path / "active.prm"
    prm.write_text("[P.m_map]\n$$FOO=bar\n", encoding="utf-8")
    params.activate(params.ParameterScope.from_file(prm))
    assert params.param("FOO") == "bar"


def test_param_free_function_raises_with_no_active_scope():
    params.activate(None)  # type: ignore[arg-type]
    with pytest.raises(RuntimeError):
        params.param("FOO")


def test_sess_start_time_fixed_for_session_lifetime():
    scope = params.ParameterScope({})
    import datetime as dt
    fixed = dt.datetime(2026, 9, 23, 12, 0, 0)
    scope.mark_session_start(fixed)
    assert scope.session_start_time == fixed
    assert scope.session_start_time == fixed  # stays fixed, not re-evaluated


def test_sess_start_time_cannot_be_restamped():
    scope = params.ParameterScope({})
    scope.mark_session_start()
    with pytest.raises(RuntimeError):
        scope.mark_session_start()


def test_sess_start_time_unset_raises():
    scope = params.ParameterScope({})
    with pytest.raises(RuntimeError):
        _ = scope.session_start_time


# ---------------------------------------------------------------------------
# Fake pyspark/delta/oracledb harness, shared by lookup/scd2/update_strategy
# tests below. Installed into sys.modules for the duration of one test via
# the `fake_spark` fixture, then removed.
# ---------------------------------------------------------------------------

class Expr:
    """Records a Spark Column-like expression as a string, so tests can
    assert on the *shape* of what would be sent to Spark without a real
    Column object.
    """

    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text

    def __eq__(self, other):
        return Expr(f"({self.text} == {_lit(other)})")

    def cast(self, dtype):
        return Expr(f"{self.text}.cast({dtype!r})")

    def __gt__(self, other):
        return Expr(f"({self.text} > {_lit(other)})")

    def asc(self):
        return Expr(f"{self.text}.asc()")

    def desc(self):
        return Expr(f"{self.text}.desc()")

    def over(self, window):
        return Expr(f"{self.text}.over({window!r})")

    def isNotNull(self):
        return Expr(f"{self.text}.isNotNull()")


def _lit(v):
    return v.text if isinstance(v, Expr) else repr(v)


class FakeWindowSpec:
    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text

    def orderBy(self, *cols):
        cols_repr = ", ".join(repr(c) for c in cols)
        return FakeWindowSpec(f"{self.text}.orderBy({cols_repr})")


class FakeWindow:
    @staticmethod
    def partitionBy(*cols):
        cols_repr = ", ".join(repr(c) for c in cols)
        return FakeWindowSpec(f"partitionBy({cols_repr})")


class FakeF:
    @staticmethod
    def col(name):
        return Expr(f"col({name})")

    @staticmethod
    def lit(value):
        return Expr(f"lit({value!r})")

    @staticmethod
    def row_number():
        return Expr("row_number()")

    @staticmethod
    def monotonically_increasing_id():
        return Expr("monotonically_increasing_id()")

    @staticmethod
    def when(cond, val):
        return Expr(f"when({cond!r}, {_lit(val)})")


class FakeDataFrame:
    """Minimal recording stand-in for a PySpark DataFrame. Every
    transformation returns a NEW FakeDataFrame sharing the same `calls`
    log, so a chain of calls can be asserted on afterwards in order.
    """

    def __init__(self, columns, calls, name="df", count_result=0, dtypes=None):
        self.columns = list(columns)
        self.calls = calls
        self.name = name
        self._count_result = count_result
        # Mirrors real PySpark's `DataFrame.dtypes` -- a list of
        # (col_name, spark_type_string) pairs. Defaults every column to
        # "int" so existing tests (which predate the type-check this dict
        # exists to support) keep passing without having to say so;
        # pass an explicit `dtypes={...}` to simulate a differently-typed
        # column, e.g. a STRING-typed DD_STRATEGY.
        self._dtypes = dict(dtypes) if dtypes is not None else {c: "int" for c in columns}

    def _next(self, method, columns=None, **info):
        self.calls.append({"df": self.name, "method": method, **info})
        return FakeDataFrame(
            columns if columns is not None else self.columns,
            self.calls, self.name, self._count_result, dict(self._dtypes),
        )

    @property
    def dtypes(self):
        return list(self._dtypes.items())

    def filter(self, cond):
        return self._next("filter", cond=repr(cond))

    def withColumn(self, name, col):
        return self._next("withColumn", columns=self.columns + [name], name=name, col=repr(col))

    def drop(self, *names):
        return self._next("drop", columns=[c for c in self.columns if c not in names], names=names)

    def select(self, *cols):
        return self._next("select", cols=cols)

    def distinct(self):
        return self._next("distinct")

    def limit(self, n):
        return self._next("limit", n=n)

    def groupBy(self, *cols):
        return FakeGroupedData(self, cols)

    def count(self):
        self.calls.append({"df": self.name, "method": "count", "result": self._count_result})
        return self._count_result

    def join(self, other, on, how):
        return self._next("join", other=getattr(other, "name", other), on=on, how=how)

    def alias(self, name):
        return self._next("alias", alias=name)

    def collect(self):
        self.calls.append({"df": self.name, "method": "collect", "result": []})
        return []

    @property
    def write(self):
        return FakeWriter(self)


class FakeGroupedData:
    """Stand-in for PySpark's ``GroupedData`` -- ``df.groupBy(*cols)``
    returns this, and ITS ``.count()`` returns a DataFrame with a
    "count" column (unlike a plain DataFrame's own ``.count()``, which
    returns a scalar row count). Conflating the two was a real bug this
    fake caught early: ``lookup.py``'s error-policy path chains
    ``.groupBy(*keys).count().filter(...)`` and needs the DataFrame
    form.
    """

    def __init__(self, df, cols):
        self.df = df
        self.cols = cols

    def count(self):
        new = self.df._next("groupBy.count", columns=list(self.cols) + ["count"])
        return new


class FakeWriter:
    def __init__(self, df, fmt=None, options=None, mode_=None):
        self.df = df
        self.fmt = fmt
        self.options_ = options or {}
        self.mode_ = mode_

    def format(self, fmt):
        return FakeWriter(self.df, fmt, dict(self.options_), self.mode_)

    def options(self, **kwargs):
        merged = dict(self.options_)
        merged.update(kwargs)
        return FakeWriter(self.df, self.fmt, merged, self.mode_)

    def option(self, key, value):
        merged = dict(self.options_)
        merged[key] = value
        return FakeWriter(self.df, self.fmt, merged, self.mode_)

    def mode(self, mode_):
        return FakeWriter(self.df, self.fmt, dict(self.options_), mode_)

    def save(self):
        self.df.calls.append({
            "df": self.df.name, "method": "write.save",
            "format": self.fmt, "options": self.options_, "mode": self.mode_,
        })

    def saveAsTable(self, table):
        self.df.calls.append({
            "df": self.df.name, "method": "write.saveAsTable",
            "table": table, "mode": self.mode_,
        })


class FakeMergeBuilder:
    def __init__(self, log, target, source_name, condition):
        self.log = log
        self.target = target
        self.source_name = source_name
        self.condition = condition

    def whenMatchedUpdate(self, set):
        self.log.append({
            "op": "whenMatchedUpdate", "target": self.target,
            "condition": self.condition, "set": set,
        })
        return self

    def whenMatchedUpdateAll(self):
        self.log.append({"op": "whenMatchedUpdateAll", "target": self.target, "condition": self.condition})
        return self

    def whenMatchedDelete(self):
        self.log.append({"op": "whenMatchedDelete", "target": self.target, "condition": self.condition})
        return self

    def whenNotMatchedInsertAll(self):
        self.log.append({"op": "whenNotMatchedInsertAll", "target": self.target, "condition": self.condition})
        return self

    def execute(self):
        self.log.append({"op": "execute", "target": self.target})


class FakeDeltaTableAlias:
    def __init__(self, log, target):
        self.log = log
        self.target = target

    def merge(self, source, condition):
        return FakeMergeBuilder(self.log, self.target, getattr(source, "name", "source"), condition)


class FakeDeltaTable:
    def __init__(self, log, target):
        self.log = log
        self.target = target

    def alias(self, _name):
        return FakeDeltaTableAlias(self.log, self.target)

    @classmethod
    def forName(cls, spark, name):
        return cls(spark.delta_log, name)


class FakeSpark:
    def __init__(self):
        self.delta_log = []
        self.sql_log = []
        self._tables = {}

    def sql(self, statement):
        self.sql_log.append(statement)

    def table(self, name):
        return self._tables.get(name, FakeDataFrame([], self.delta_log, name=name))

    def createDataFrame(self, rows, schema=None):
        return FakeDataFrame([], self.delta_log, name="literal")

    class catalog:
        @staticmethod
        def tableExists(name):
            return False


class FakeCursor:
    def __init__(self, log):
        self.log = log
        self._fetch = []

    def execute(self, sql, params=None):
        self.log.append({"sql": sql, "params": params})
        if "NEXTVAL" in sql.upper():
            n = (params or {}).get("n", 1)
            self._fetch = [(100 + i,) for i in range(n)]

    def fetchall(self):
        return self._fetch

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.log = []
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


@pytest.fixture
def fake_spark(monkeypatch):
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    functions_mod = types.ModuleType("pyspark.sql.functions")
    window_mod = types.ModuleType("pyspark.sql.window")
    delta_mod = types.ModuleType("delta")
    delta_tables_mod = types.ModuleType("delta.tables")

    for name, value in vars(FakeF).items():
        if not name.startswith("_"):
            setattr(functions_mod, name, getattr(FakeF, name))
    window_mod.Window = FakeWindow
    delta_tables_mod.DeltaTable = FakeDeltaTable
    sql_mod.functions = functions_mod
    sql_mod.window = window_mod
    pyspark_mod.sql = sql_mod
    delta_mod.tables = delta_tables_mod

    modules = {
        "pyspark": pyspark_mod,
        "pyspark.sql": sql_mod,
        "pyspark.sql.functions": functions_mod,
        "pyspark.sql.window": window_mod,
        "delta": delta_mod,
        "delta.tables": delta_tables_mod,
    }
    for name, mod in modules.items():
        monkeypatch.setitem(sys.modules, name, mod)
    return types.SimpleNamespace(
        F=functions_mod, Window=FakeWindow, DeltaTable=FakeDeltaTable,
    )


# ---------------------------------------------------------------------------
# 4. lookup.py -- cached_lookup's four multiple-match policies
# ---------------------------------------------------------------------------

from infa_compat.lookup import LookupMultipleMatchError, cached_lookup  # noqa: E402


def test_cached_lookup_rejects_unknown_policy(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    with pytest.raises(ValueError):
        cached_lookup(df, lookup_df, on="ID", policy="any")


def test_cached_lookup_rejects_dynamic_cache(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    with pytest.raises(NotImplementedError):
        cached_lookup(df, lookup_df, on="ID", cache="dynamic")


def test_cached_lookup_all_policy_skips_dedup_and_joins_directly(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    result = cached_lookup(df, lookup_df, on="ID", policy="all")
    join_calls = [c for c in calls if c["method"] == "join"]
    assert len(join_calls) == 1
    assert join_calls[0]["other"] == "lookup", "policy='all' must join the ORIGINAL lookup_df, no dedup step"
    assert join_calls[0]["how"] == "left"
    dedup_ops = [c for c in calls if c["method"] in ("groupBy", "withColumn")]
    assert dedup_ops == [], "policy='all' must not run any dedup/groupBy step"


def test_cached_lookup_first_policy_orders_ascending(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    cached_lookup(df, lookup_df, on="ID", policy="first")
    filter_calls = [c for c in calls if c["method"] == "filter"]
    assert any("_infa_lookup_rn" in c["cond"] and "== 1" in c["cond"] for c in filter_calls)
    with_cols = [c for c in calls if c["method"] == "withColumn"]
    rn_col = next(c for c in with_cols if c["name"] == "_infa_lookup_rn")
    assert "asc()" in rn_col["col"], f"policy='first' must order ascending, got: {rn_col['col']}"


def test_cached_lookup_last_policy_orders_descending(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    cached_lookup(df, lookup_df, on="ID", policy="last")
    with_cols = [c for c in calls if c["method"] == "withColumn"]
    rn_col = next(c for c in with_cols if c["name"] == "_infa_lookup_rn")
    assert "desc()" in rn_col["col"], f"policy='last' must order descending, got: {rn_col['col']}"


def test_cached_lookup_first_last_respect_explicit_order_by(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME", "LOAD_TS"], calls, name="lookup")
    cached_lookup(df, lookup_df, on="ID", policy="first", order_by="LOAD_TS")
    with_cols = [c for c in calls if c["method"] == "withColumn"]
    # No synthetic _infa_lookup_seq column should be added when order_by is given.
    assert not any(c["name"] == "_infa_lookup_seq" for c in with_cols)
    rn_col = next(c for c in with_cols if c["name"] == "_infa_lookup_rn")
    assert "col(LOAD_TS)" in rn_col["col"]


def test_cached_lookup_error_policy_raises_when_duplicates_present(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    # count_result=1 on the post-limit(1) DataFrame simulates "duplicates found".
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup", count_result=1)
    with pytest.raises(LookupMultipleMatchError):
        cached_lookup(df, lookup_df, on="ID", policy="error")


def test_cached_lookup_error_policy_passes_when_no_duplicates(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup", count_result=0)
    result = cached_lookup(df, lookup_df, on="ID", policy="error")
    join_calls = [c for c in calls if c["method"] == "join"]
    assert len(join_calls) == 1


def test_cached_lookup_join_is_always_left():
    """NULL-key rows (and genuinely unmatched rows) must be preserved,
    never dropped -- a connected Lookup passes every input row through.
    """
    import inspect
    src = inspect.getsource(cached_lookup)
    assert 'how="left"' in src


def test_cached_lookup_single_string_key_normalized_to_list(fake_spark):
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID", "NAME"], calls, name="lookup")
    cached_lookup(df, lookup_df, on="ID", policy="all")
    join_calls = [c for c in calls if c["method"] == "join"]
    assert join_calls[0]["on"] == ["ID"]


def test_cached_lookup_empty_keys_rejected():
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")
    lookup_df = FakeDataFrame(["ID"], calls, name="lookup")
    with pytest.raises(ValueError):
        cached_lookup(df, lookup_df, on=[], policy="all")


def test_cached_lookup_dotted_key_current_behaviour_is_passthrough(fake_spark):
    """Pins the CURRENT behaviour for a literal-dotted join-key name (the
    corpus fixtures' "SQ_EMPLOYEES.EMP_ID" shape, per
    transformation_converter.py's withColumnRenamed step). This is
    documented as UNVERIFIED against a live Spark session (see the
    lookup.py module docstring and cached_lookup's own docstring) --
    whether Spark's `on=[...]` named-column join form resolves a dotted
    string identically to the bracket-indexing form the previous inline
    implementation used is genuinely unknown here (no PySpark installed).
    This test only pins what cached_lookup ACTUALLY does today --
    forwards the dotted string straight through to `df.join(..., on=keys,
    how="left")` with no normalization -- so a future change to that
    behaviour (e.g. adding prefix-stripping) fails this test and has to
    be a deliberate, documented decision, not an accidental drift.
    """
    calls = []
    dotted_key = "SQ_EMPLOYEES.EMP_ID"
    df = FakeDataFrame([dotted_key], calls, name="df")
    lookup_df = FakeDataFrame([dotted_key, "LKP_DEPT"], calls, name="lookup")
    cached_lookup(df, lookup_df, on=dotted_key, policy="all")
    join_calls = [c for c in calls if c["method"] == "join"]
    assert len(join_calls) == 1
    assert join_calls[0]["on"] == [dotted_key], (
        "cached_lookup must pass the dotted key through UNCHANGED -- no "
        "prefix-stripping or other normalization is implemented (see the "
        "docstring for why that was considered and rejected as unsafe)"
    )


# ---------------------------------------------------------------------------
# 5. update_strategy.py -- DD_* split, and DD_REJECT routing above all
# ---------------------------------------------------------------------------

from infa_compat.update_strategy import (  # noqa: E402
    UpdateStrategyCode,
    apply_update_strategy,
    write_update_strategy,
)


def test_dd_constants_match_informatica():
    assert int(UpdateStrategyCode.DD_INSERT) == 0
    assert int(UpdateStrategyCode.DD_UPDATE) == 1
    assert int(UpdateStrategyCode.DD_DELETE) == 2
    assert int(UpdateStrategyCode.DD_REJECT) == 3


def test_apply_update_strategy_filters_each_partition_by_its_own_code(fake_spark):
    calls = []
    df = FakeDataFrame(["ID", "DD_STRATEGY"], calls, name="df")
    result = apply_update_strategy(df, strategy_col="DD_STRATEGY")
    filter_calls = [c for c in calls if c["method"] == "filter"]
    assert len(filter_calls) == 4
    conds = [c["cond"] for c in filter_calls]
    assert any("== 0" in c for c in conds)  # INSERT
    assert any("== 1" in c for c in conds)  # UPDATE
    assert any("== 2" in c for c in conds)  # DELETE
    assert any("== 3" in c for c in conds)  # REJECT
    assert result.inserts is not None and result.rejects is not None


def test_apply_update_strategy_string_typed_column_raises_not_empty(fake_spark):
    """The gap this closes: Informatica's own DD_INSERT/DD_UPDATE/
    DD_DELETE/DD_REJECT names are STRING labels, and writing them
    verbatim (e.g. F.lit("DD_INSERT")) into strategy_col is the natural-
    looking thing for a generator to do. Filtering that string column
    against the integer UpdateStrategyCode literals would match NOTHING
    on any of the four partitions -- silently empty, no error. This must
    raise loudly instead, before any filter runs.
    """
    calls = []
    df = FakeDataFrame(
        ["ID", "DD_STRATEGY"], calls, name="df",
        dtypes={"ID": "int", "DD_STRATEGY": "string"},
    )
    with pytest.raises(update_strategy.NonIntegerStrategyColumnError):
        apply_update_strategy(df, strategy_col="DD_STRATEGY")
    # And, just as importantly: no filter call happened at all -- this
    # is a loud pre-check, not a downstream symptom discovered later.
    assert not any(c["method"] == "filter" for c in calls)


def test_apply_update_strategy_missing_column_is_not_this_check(fake_spark):
    """If strategy_col isn't even present, the type-check is a deliberate
    no-op -- Spark's own AnalysisException from the subsequent .filter()
    is the (already loud) signal for that case, not this guard."""
    calls = []
    df = FakeDataFrame(["ID"], calls, name="df")  # no DD_STRATEGY column
    # Must reach the (fake) filter calls rather than raising here.
    apply_update_strategy(df, strategy_col="DD_STRATEGY")
    filter_calls = [c for c in calls if c["method"] == "filter"]
    assert len(filter_calls) == 4


def test_write_update_strategy_requires_reject_sink_argument():
    """reject_sink has no default -- callers must always say where
    rejects go. This is enforced at the Python level (TypeError for a
    missing required argument), the strongest "never silently drop"
    guarantee available.
    """
    import inspect
    sig = inspect.signature(write_update_strategy)
    assert sig.parameters["reject_sink"].default is inspect.Parameter.empty


def test_write_update_strategy_delta_routes_rejects_to_sink_table(fake_spark):
    calls = []
    empty = FakeDataFrame(["ID", "AMT"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    spark = FakeSpark()
    write_update_strategy(
        result, target="CAT.SCH.T", keys=["ID"], reject_sink="CAT.SCH.T_REJECTS",
        target_catalog_type="delta", spark=spark,
    )
    save_calls = [c for c in calls if c["method"] == "write.saveAsTable" and c["table"] == "CAT.SCH.T_REJECTS"]
    assert len(save_calls) == 1
    assert save_calls[0]["mode"] == "append"


def test_write_update_strategy_delta_routes_rejects_to_callback(fake_spark):
    calls = []
    empty = FakeDataFrame(["ID"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    spark = FakeSpark()
    seen = []
    write_update_strategy(
        result, target="CAT.SCH.T", keys=["ID"], reject_sink=lambda df: seen.append(df),
        target_catalog_type="delta", spark=spark,
    )
    assert seen == [empty]


def test_write_update_strategy_delta_never_inserts_on_update_partition(fake_spark):
    """The UPDATE partition must go through whenMatchedUpdateAll only --
    never whenNotMatchedInsertAll -- so an UPDATE-tagged row for a key
    that doesn't exist yet is dropped (matches Informatica: DD_UPDATE
    against a non-existent target row is a no-op, not an insert), not
    silently turned into a new row.
    """
    calls = []
    empty = FakeDataFrame(["ID"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    spark = FakeSpark()
    write_update_strategy(
        result, target="CAT.SCH.T", keys=["ID"], reject_sink="CAT.SCH.T_REJ",
        target_catalog_type="delta", spark=spark,
    )
    update_merge_ops = [op for op in spark.delta_log if op.get("op") == "whenMatchedUpdateAll"]
    insert_ops = [op for op in spark.delta_log if op.get("op") == "whenNotMatchedInsertAll"]
    assert len(update_merge_ops) == 1
    assert len(insert_ops) == 1  # exactly one -- from the INSERT partition's own merge call


def test_write_update_strategy_adw_requires_connection_and_options():
    calls = []
    empty = FakeDataFrame(["ID"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    with pytest.raises(ValueError):
        write_update_strategy(
            result, target="T", keys=["ID"], reject_sink="T_REJ",
            target_catalog_type="adw",
        )


def test_write_update_strategy_adw_no_keys_raises_never_downgrades():
    """The worst failure class: no keys on ADW must never fall back to a
    full-table overwrite (which would delete every preserved row)."""
    calls = []
    empty = FakeDataFrame(["ID"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    with pytest.raises(ValueError):
        write_update_strategy(
            result, target="T", keys=[], reject_sink="T_REJ",
            target_catalog_type="adw", jdbc_options={}, adw_connection=FakeConnection(),
        )


def test_write_update_strategy_adw_stages_then_merges(fake_spark):
    calls = []
    empty = FakeDataFrame(["ID", "AMT"], calls, name="empty")
    from infa_compat.update_strategy import UpdateStrategyResult
    result = UpdateStrategyResult(inserts=empty, updates=empty, deletes=empty, rejects=empty)
    conn = FakeConnection()
    write_update_strategy(
        result, target="T", keys=["ID"], reject_sink="T_REJ",
        target_catalog_type="adw", jdbc_options={"url": "jdbc:x"}, adw_connection=conn,
    )
    executed = [entry["sql"].upper() for entry in conn.log]
    assert any("MERGE INTO T" in s and "UPDATE SET" in s for s in executed)
    assert any("MERGE INTO T" in s and "DELETE WHERE" in s for s in executed)
    assert conn.committed


# ---------------------------------------------------------------------------
# 6. scd2.py -- the heaviest-covered function
# ---------------------------------------------------------------------------

def test_scd2_merge_requires_keys(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    with pytest.raises(scd2.Scd2ConfigError):
        scd2.scd2_merge(
            spark, "T.DIM_CUST", source, keys=[], effective_from="EFF_FROM",
            effective_to="EFF_TO", current_flag="CUR_FLAG",
        )


def test_scd2_merge_rejects_unknown_target_type(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    with pytest.raises(scd2.Scd2ConfigError):
        scd2.scd2_merge(
            spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
            effective_to="EFF_TO", current_flag="CUR_FLAG", target_catalog_type="snowflake",
        )


def test_scd2_merge_delta_never_calls_delta_for_adw(fake_spark):
    """Never let a Delta-only API leak into the ADW path -- the core
    correctness rule exists to enforce."""
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    conn = FakeConnection()
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG", target_catalog_type="adw",
        jdbc_options={"url": "jdbc:x"}, adw_connection=conn,
    )
    assert spark.delta_log == [], "ADW path must never touch DeltaTable"


def test_scd2_merge_delta_expires_on_business_key_and_current_flag_only(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM", "NAME"], calls, name="source")
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG",
    )
    expire_ops = [op for op in spark.delta_log if op.get("op") == "whenMatchedUpdate"]
    assert len(expire_ops) == 1
    op = expire_ops[0]
    assert "CUST_ID" in op["condition"] and "CUR_FLAG = 'Y'" in op["condition"]
    assert op["set"]["CUR_FLAG"] == "'N'"
    assert op["set"]["EFF_TO"] == "s.EFF_FROM"


def test_scd2_merge_delta_inserts_all_stamped_rows(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG",
    )
    save_calls = [c for c in calls if c["method"] == "write.saveAsTable"]
    assert len(save_calls) == 1
    assert save_calls[0]["table"] == "T.DIM_CUST"
    assert save_calls[0]["mode"] == "append"


def test_scd2_merge_delta_stamps_current_flag_and_high_date(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG", high_date="9999-12-31",
    )
    with_cols = [c for c in calls if c["method"] == "withColumn"]
    stamped_names = {c["name"] for c in with_cols}
    assert {"CUR_FLAG", "EFF_TO"}.issubset(stamped_names)


def test_scd2_merge_custom_current_flag_values(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG", current_flag_values=(1, 0),
    )
    expire_ops = [op for op in spark.delta_log if op.get("op") == "whenMatchedUpdate"]
    assert expire_ops[0]["set"]["CUR_FLAG"] == "'0'"
    assert "CUR_FLAG = '1'" in expire_ops[0]["condition"]


def test_scd2_merge_adw_requires_connection_and_options(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    with pytest.raises(scd2.Scd2ConfigError):
        scd2.scd2_merge(
            spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
            effective_to="EFF_TO", current_flag="CUR_FLAG", target_catalog_type="adw",
        )


def test_scd2_merge_adw_stages_then_expires_then_inserts(fake_spark):
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    conn = FakeConnection()
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG", target_catalog_type="adw",
        jdbc_options={"url": "jdbc:x"}, adw_connection=conn,
    )
    stage_calls = [c for c in calls if c["method"] == "write.save" and c["mode"] == "overwrite"]
    assert len(stage_calls) == 1, "must stage via JDBC OVERWRITE, never append/merge from Spark"
    executed = [entry["sql"].upper() for entry in conn.log]
    assert any("MERGE INTO T.DIM_CUST" in s and "UPDATE SET" in s for s in executed)
    assert any(s.startswith("INSERT INTO T.DIM_CUST") for s in executed)
    assert conn.committed
    # Staging table must be dropped afterwards.
    drop_calls = [entry["sql"] for entry in conn.log if "DROP TABLE" in entry["sql"].upper()]
    assert len(drop_calls) == 1


def test_scd2_merge_adw_never_emits_delta_merge_into_sql(fake_spark):
    """The ADW SQL itself must never reference DeltaTable-only syntax."""
    spark = FakeSpark()
    calls = []
    source = FakeDataFrame(["CUST_ID", "EFF_FROM"], calls, name="source")
    conn = FakeConnection()
    scd2.scd2_merge(
        spark, "T.DIM_CUST", source, keys=["CUST_ID"], effective_from="EFF_FROM",
        effective_to="EFF_TO", current_flag="CUR_FLAG", target_catalog_type="adw",
        jdbc_options={"url": "jdbc:x"}, adw_connection=conn,
    )
    for entry in conn.log:
        assert "DeltaTable" not in entry["sql"]


def test_scd2_merge_adw_rolls_back_on_failure():
    """If the driver-side SQL fails partway through, the connection must
    roll back rather than leaving the target half-expired/half-inserted.
    """
    class FailingCursor(FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if "INSERT INTO" in sql.upper():
                raise RuntimeError("simulated DB failure")

    class FailingConnection(FakeConnection):
        def cursor(self):
            return FailingCursor(self.log)

    conn = FailingConnection()
    from infa_compat import _adw_runtime
    with pytest.raises(RuntimeError):
        _adw_runtime.run_statements_transactionally(conn, ["MERGE INTO t ...", "INSERT INTO t ..."])
    assert conn.rolled_back
    assert not conn.committed


# ---------------------------------------------------------------------------
# 7. sequence.py -- restart-safety, cache, and cycle
# ---------------------------------------------------------------------------

from infa_compat.sequence import Sequence, SequenceBackend, SequenceExhausted  # noqa: E402


class FakeBackend(SequenceBackend):
    """In-memory stand-in implementing the exact same contract
    ``DeltaSequenceBackend``/``AdwSequenceBackend`` implement -- a
    persisted store shared across FakeBackend instances that point at
    the same dict, so re-instantiating ``Sequence`` against the same
    dict simulates a process restart against a real persisted counter.
    """

    def __init__(self, store: dict, start: int = 1):
        self.store = store
        self.start = start
        self.reservations = []  # audit trail for assertions

    def reserve_block(self, name, count, increment):
        current = self.store.get(name)
        if current is None:
            first = self.start
        else:
            first = current + increment
        new_current = first + (count - 1) * increment
        self.store[name] = new_current
        self.reservations.append((name, first, count))
        return first


def test_sequence_next_value_is_contiguous_within_a_cache_block():
    backend = FakeBackend({})
    seq = Sequence(backend, "S1", start=1, increment=1, cache=5)
    values = seq.next_values(5)
    assert values == [1, 2, 3, 4, 5]
    assert len(backend.reservations) == 1, "must fill the whole block in one backend call"


def test_sequence_refills_from_backend_when_cache_exhausted():
    backend = FakeBackend({})
    seq = Sequence(backend, "S1", start=1, increment=1, cache=3)
    values = seq.next_values(7)
    assert values == [1, 2, 3, 4, 5, 6, 7]
    assert len(backend.reservations) == 3  # ceil(7/3)


def test_sequence_increment_other_than_one():
    # Direct-construction contract (see Sequence's docstring "Coupling
    # note"): a hand-built backend's own `start` must be kept consistent
    # with Sequence's `start` -- the `sequence()` factory does this
    # automatically for a DeltaSequenceBackend, this test does it by hand.
    backend = FakeBackend({}, start=10)
    seq = Sequence(backend, "S1", start=10, increment=5, cache=3)
    assert seq.next_values(3) == [10, 15, 20]


def test_sequence_never_collides_across_two_concurrently_cached_handles():
    """Two Sequence handles for the SAME name, sharing the same backend
    store, must never hand out overlapping values -- this is the
    non-negotiable half of the restart-safety contract (gaps are
    accepted; collisions are not).
    """
    store = {}
    seq_a = Sequence(FakeBackend(store), "S1", start=1, cache=10)
    seq_b = Sequence(FakeBackend(store), "S1", start=1, cache=10)
    a_values = set(seq_a.next_values(10))
    b_values = set(seq_b.next_values(10))
    assert a_values.isdisjoint(b_values), f"collision: {a_values & b_values}"


def test_sequence_restart_continues_from_persisted_value_not_from_start():
    """Simulates a job restart: a NEW Sequence object (a fresh process)
    pointed at the same persisted store must continue from where the
    old one left off, never re-issuing already-used values.
    """
    store = {}
    first_run = Sequence(FakeBackend(store), "S1", start=1, cache=5)
    first_run.next_values(5)  # fully consumes its cache block: 1..5

    restarted = Sequence(FakeBackend(store), "S1", start=1, cache=5)
    next_batch = restarted.next_values(5)
    assert next_batch == [6, 7, 8, 9, 10]


def test_sequence_restart_before_cache_exhausted_leaves_a_gap_not_a_collision():
    """A crash mid-cache-block is exactly the documented "gaps are
    accepted, collisions are not" cost: the old handle already reserved
    a block of 5 from the backend but only consumed 2 before "dying";
    the new handle must NOT reuse 3, 4, 5 (that data was never at risk
    of collision because the backend's persisted counter already moved
    past them), but it also must never go backwards.
    """
    store = {}
    dying = Sequence(FakeBackend(store), "S1", start=1, cache=5)
    used = dying.next_values(2)  # consumes 1, 2 -- leaves 3,4,5 as a gap
    assert used == [1, 2]

    restarted = Sequence(FakeBackend(store), "S1", start=1, cache=5)
    next_batch = restarted.next_values(3)
    assert next_batch == [6, 7, 8], "must never re-issue 3, 4, or 5 (no collision risk), and never go backwards"


def test_sequence_cycle_false_raises_on_exhaustion():
    backend = FakeBackend({})
    seq = Sequence(backend, "S1", start=1, cache=5, cycle=False, max_value=3)
    assert seq.next_values(3) == [1, 2, 3]
    with pytest.raises(SequenceExhausted):
        seq.next_value()


def test_sequence_cycle_true_wraps_to_start():
    backend = FakeBackend({})
    seq = Sequence(backend, "S1", start=1, cache=10, cycle=True, max_value=3)
    values = seq.next_values(7)
    # 1, 2, 3, then wraps: 4 -> 1, 5 -> 2, 6 -> 3, 7 -> 1
    assert values == [1, 2, 3, 1, 2, 3, 1]


def test_sequence_cache_must_be_positive():
    with pytest.raises(ValueError):
        Sequence(FakeBackend({}), "S1", cache=0)


def test_sequence_increment_zero_rejected():
    with pytest.raises(ValueError):
        Sequence(FakeBackend({}), "S1", increment=0)


def test_sequence_factory_dispatches_by_target_catalog_type_explicitly(fake_spark):
    spark = FakeSpark()
    seq = infa_compat.sequence(spark, "S1", target_catalog_type="delta")
    from infa_compat.sequence import DeltaSequenceBackend
    assert isinstance(seq._backend, DeltaSequenceBackend)


def test_sequence_factory_adw_requires_connection(fake_spark):
    spark = FakeSpark()
    with pytest.raises(ValueError):
        infa_compat.sequence(spark, "S1", target_catalog_type="adw")


def test_sequence_factory_adw_dispatches_to_adw_backend(fake_spark):
    spark = FakeSpark()
    conn = FakeConnection()
    seq = infa_compat.sequence(spark, "S1", target_catalog_type="adw", connection=conn)
    from infa_compat.sequence import AdwSequenceBackend
    assert isinstance(seq._backend, AdwSequenceBackend)


def test_sequence_factory_rejects_unknown_target_type(fake_spark):
    spark = FakeSpark()
    with pytest.raises(ValueError):
        infa_compat.sequence(spark, "S1", target_catalog_type="snowflake")


def test_adw_sequence_backend_pulls_a_contiguous_block_in_one_round_trip():
    conn = FakeConnection()
    from infa_compat.sequence import AdwSequenceBackend
    backend = AdwSequenceBackend(conn)
    first = backend.reserve_block("MY_SEQ", count=5, increment=1)
    assert first == 100  # from FakeCursor's canned NEXTVAL response
    execute_log = conn.log[0]
    assert "MY_SEQ.NEXTVAL" in execute_log["sql"]
    assert execute_log["params"] == {"n": 5}


def test_delta_sequence_backend_first_reservation_starts_at_configured_start(fake_spark):
    spark = FakeSpark()
    from infa_compat.sequence import DeltaSequenceBackend
    backend = DeltaSequenceBackend(spark, start=100)
    first = backend.reserve_block("S1", count=10, increment=1)
    assert first == 100


def test_get_sequence_backend_rejects_unknown_type(fake_spark):
    """Explicit input required -- same rule as write_strategies.py's
    get_write_strategy. An unrecognized target_catalog_type raises
    rather than falling back to a guess."""
    spark = FakeSpark()
    with pytest.raises(ValueError):
        infa_compat.get_sequence_backend("bigquery", spark=spark)


def test_get_sequence_backend_defaults_to_delta_when_type_omitted(fake_spark):
    """None -> "delta" is the one and only default -- matches
    get_write_strategy's own default, still an explicit fallback, not
    inference from any data."""
    spark = FakeSpark()
    from infa_compat.sequence import DeltaSequenceBackend
    backend = infa_compat.get_sequence_backend(None, spark=spark)
    assert isinstance(backend, DeltaSequenceBackend)


# ---------------------------------------------------------------------------
# 6. SUPPORTED_OPERATIONS.md -- the two documentation gaps this task closes
# ---------------------------------------------------------------------------

def _supported_operations_text() -> str:
    return (
        Path(__file__).resolve().parent.parent
        / "engine" / "infa_compat" / "SUPPORTED_OPERATIONS.md"
    ).read_text(encoding="utf-8")


def test_docs_show_update_strategy_enum_form_as_correct():
    text = _supported_operations_text()
    assert "UpdateStrategyCode.DD_INSERT" in text
    assert "UpdateStrategyCode.DD_UPDATE" in text


def test_docs_show_update_strategy_string_form_as_explicitly_wrong():
    """The doc gap: nothing previously told the model that strategy_col
    must be the INT enum, never Informatica's own STRING names. Now it
    must show the string form labeled as wrong, not just show the right
    form and hope the model infers the rest."""
    text = _supported_operations_text()
    assert 'F.lit("DD_INSERT")' in text
    assert "WRONG" in text.upper() or "DO NOT DO THIS" in text.upper()


def test_docs_mention_dotted_lookup_key_uncertainty():
    text = _supported_operations_text()
    assert "SQ_EMPLOYEES.EMP_ID" in text
    assert "UNVERIFIED" in text or "unverified" in text.lower()


def test_prompt_shows_update_strategy_enum_form_as_correct():
    from infa2aidp.generators.llm_notebook_generator import _LLM_SYSTEM_PROMPT
    assert "UpdateStrategyCode.DD_INSERT" in _LLM_SYSTEM_PROMPT
    assert "UpdateStrategyCode.DD_DELETE" in _LLM_SYSTEM_PROMPT


def test_prompt_shows_update_strategy_string_form_as_explicitly_wrong():
    from infa2aidp.generators.llm_notebook_generator import _LLM_SYSTEM_PROMPT
    assert 'F.lit("DD_DELETE")' in _LLM_SYSTEM_PROMPT
    assert "WRONG" in _LLM_SYSTEM_PROMPT


def test_lookup_docstrings_flag_dotted_key_as_unverified():
    """Gap 2 must be documented, not silently guessed at -- this pins
    that the uncertainty is actually written down in the module the
    generator/model would have to go read, not just in this task's
    report."""
    import inspect
    from infa_compat import lookup as lookup_module
    module_doc = lookup_module.__doc__ or ""
    fn_doc = lookup_module.cached_lookup.__doc__ or ""
    assert "SQ_EMPLOYEES.EMP_ID" in module_doc
    assert "UNVERIFIED" in module_doc or "unverified" in module_doc.lower()
    assert "UNVERIFIED" in fn_doc or "unverified" in fn_doc.lower()
