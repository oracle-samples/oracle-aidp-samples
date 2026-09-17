"""Static conformance check: does each medallion demo's generated frame match its DDL?

The defect this guards against, fixed for `telecommunications` in the PR that added this
file: the bronze DDL declares narrow types (`DECIMAL(p,s)`, `INT`) while the generating
cell builds its DataFrame with `spark.createDataFrame(data)` and no schema. Spark then
infers `double` for every Python float and `bigint` for every int, and a Delta
`overwrite` through `saveAsTable` raises `DELTA_FAILED_TO_MERGE_FIELDS` -- decimal and
double are not implicitly reconciled. The notebook cannot complete a Run All.

Six demos still carry it. They are `xfail(strict=False)` rather than skipped, so each
flips to XPASS the moment it is fixed and this suite reports that the gap closed. See the
tracking issue referenced in AFFECTED below.

Everything here is parsed from the notebook JSON -- no Spark, no cluster, no network.

Run:
    pytest test_medallion_ddl_conformance.py -v
    pytest test_medallion_ddl_conformance.py -v -rX     # show which demos now XPASS
"""
import glob
import json
import os
import re

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))

# Demos whose generating cell does not cast to the declared bronze types.
# Remove a name from this set when its notebook is fixed; the test then enforces it.
AFFECTED = {
    "energy",
    "financial_services",
    "manufacturing",
    "real_estate",
    "retail",
    "transportation",
}

NARROW_TYPE = re.compile(r"(\w+)\s+(DECIMAL\s*\(\s*\d+\s*,\s*\d+\s*\)|\bINT\b)", re.I)
BRONZE_DDL = re.compile(
    r"CREATE\s+TABLE[^(]*?bronze\.(\w+)\s*\((.*?)\)\s*\n?USING\s+DELTA", re.S | re.I
)


def _demo_names():
    paths = sorted(glob.glob(os.path.join(HERE, "*_medallion_architecture_demo.ipynb")))
    return [os.path.basename(p).replace("_medallion_architecture_demo.ipynb", "") for p in paths]


def _code_cells(demo):
    path = os.path.join(HERE, "%s_medallion_architecture_demo.ipynb" % demo)
    with open(path, encoding="utf-8") as fh:
        nb = json.load(fh)
    return ["".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"]


def _narrow_bronze_columns(cells):
    """-> {column: declared_type} for every narrow-typed column in a bronze CREATE TABLE."""
    declared = {}
    for _table, body in BRONZE_DDL.findall("\n".join(cells)):
        for col, typ in NARROW_TYPE.findall(body):
            declared[col] = typ.upper().replace(" ", "")
    return declared


def _generating_cell(cells):
    """The cell that builds the raw DataFrame and writes it to bronze."""
    for cell in cells:
        if "createDataFrame" in cell and re.search(r"bronze", cell):
            return cell
    for cell in cells:
        if "createDataFrame" in cell:
            return cell
    return None


DEMOS = _demo_names()


def test_all_twelve_demos_are_discovered():
    """Guards the glob: if a demo is renamed, the parametrised tests must not vanish."""
    assert len(DEMOS) == 12, "expected 12 medallion demos, found %d: %s" % (len(DEMOS), DEMOS)
    assert AFFECTED <= set(DEMOS), "AFFECTED names a demo that does not exist: %s" % (
        AFFECTED - set(DEMOS)
    )


@pytest.mark.parametrize("demo", DEMOS)
def test_bronze_frame_is_cast_to_declared_types(demo, request):
    """A narrow-typed bronze DDL requires the generating cell to cast to it."""
    if demo in AFFECTED:
        request.node.add_marker(
            pytest.mark.xfail(
                reason="%s: generated frame is not cast to the bronze DDL (tracked)" % demo,
                strict=False,
            )
        )
    cells = _code_cells(demo)
    declared = _narrow_bronze_columns(cells)
    if not declared:
        pytest.skip("%s declares no narrow bronze types" % demo)

    cell = _generating_cell(cells)
    assert cell is not None, "%s has no createDataFrame cell" % demo
    assert re.search(r"\.cast\(|CAST\s*\(", cell), (
        "%s declares %s in bronze but its generating cell casts nothing, so "
        "createDataFrame's inferred double/bigint frame will fail the Delta schema check"
        % (demo, sorted(declared))
    )


# ----------------------------------------------------------------------------------
# telecommunications -- the reference fix. These enforce it, and must not xfail.
# ----------------------------------------------------------------------------------
TELECOM = "telecommunications"


def test_telecom_declares_widened_total_call_minutes():
    """The two gold tables that SUM across a service type need DECIMAL(12,2).

    Summed that way the value reaches ~1.24M against a DECIMAL(8,2) ceiling of
    999,999.99, and under the default spark.sql.ansi.enabled=false the overflow is a
    silent NULL rather than an error.

    `subscriber_analytics` is deliberately left at DECIMAL(8,2): it is a per-subscriber
    total and cannot overflow, so this asserts the widths per table rather than globally.
    """
    src = "\n".join(_code_cells(TELECOM))
    widened = len(re.findall(r"total_call_minutes\s+DECIMAL\(12,2\)", src))
    per_subscriber = len(re.findall(r"total_call_minutes\s+DECIMAL\(8,2\)", src))
    assert widened == 2, (
        "expected network_infrastructure and service_performance to declare "
        "total_call_minutes DECIMAL(12,2); found %d such declarations" % widened
    )
    assert per_subscriber == 1, (
        "expected exactly one narrow declaration (subscriber_analytics); found %d"
        % per_subscriber
    )


def test_telecom_does_not_use_overwrite_schema():
    """overwriteSchema silently replaces the declared DDL with inferred types, which made
    the gold CREATE TABLE cells dead code.

    Looks for the actual write option, not the bare word -- the notebook's comments
    mention overwriteSchema precisely to explain why it is not used.
    """
    src = "\n".join(_code_cells(TELECOM))
    calls = re.findall(r"""option\s*\(\s*['"]overwriteSchema['"]""", src)
    assert not calls, "a write still passes overwriteSchema: %s" % calls


def test_telecom_drops_tables_before_creating_them():
    """A workspace that ran an earlier version holds these tables with the inferred
    schema, so CREATE TABLE IF NOT EXISTS would leave them and the cast write would fail."""
    src = "\n".join(_code_cells(TELECOM))
    for layer in ("bronze.network_usage_raw", "silver.network_usage_clean"):
        assert "DROP TABLE IF EXISTS telecom.%s" % layer in src, "%s is not dropped first" % layer
    assert not re.search(r"CREATE TABLE IF NOT EXISTS\s+telecom\.", src), (
        "a CREATE TABLE IF NOT EXISTS survives; it contradicts the drop-first approach"
    )


def test_telecom_avoids_wildcard_function_import():
    """`from pyspark.sql.functions import *` binds pyspark's round/max/min over the
    builtins the generator cell calls on plain Python numbers."""
    for cell in _code_cells(TELECOM):
        assert "from pyspark.sql.functions import *" not in cell


def test_telecom_primary_service_type_has_a_tie_breaker():
    """Without one, ROW_NUMBER picks an arbitrary winner that can change between runs."""
    src = "\n".join(_code_cells(TELECOM))
    m = re.search(
        r"ROW_NUMBER\(\) OVER \(PARTITION BY subscriber_id ORDER BY (.*?)\)\s+as\s+rn",
        src,
        re.S,
    )
    assert m, "the primary_service_type window function was not found"
    order_by = m.group(1).strip()
    assert "," in order_by, "ORDER BY %s has no tie-breaker" % order_by
    assert "service_type" in order_by, "expected service_type as the tie-breaker: %s" % order_by
