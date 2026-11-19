r"""CLUSTER BY is written with bare column names -- AIDP's Delta rejects a
backticked one.

Live 2026-09-29, coverage run on AIDP (Delta 3.1.0): the structure job ran
`CREATE TABLE ... USING DELTA CLUSTER BY (\`SEGMENT\`)` for CORE.CUSTOMERS
and got

    [UNSUPPORTED_FEATURE.PARTITION_WITH_NESTED_COLUMN_IS_UNSUPPORTED]
    Invalid partitioning: ```SEGMENT``` is missing or is in a map or array.

The clustering parser keeps the backticks as part of the name. Probe 1 had
passed only because it wrote `CLUSTER BY (seg)`. The table failed, and so
did the two views built on it. A key whose column name cannot be written
bare is therefore not carried: it stays a deferred decision, with the
reason.
"""
import importlib.util
import pathlib
import sys

from target.ddl import classify_table_properties, render_cluster_by

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _structure():
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location("snowmig_s01_cluster", SCRIPTS / "01_create_structure.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _rec(cluster_by, name="SEGMENT"):
    return {"source_metadata": {"cluster_by": cluster_by},
            "columns": [{"COLUMN_NAME": "ID", "ORDINAL_POSITION": 1, "target_type": "DECIMAL(38,0)"},
                        {"COLUMN_NAME": name, "ORDINAL_POSITION": 2, "target_type": "STRING"}]}


def test_the_clause_names_the_column_bare():
    assert render_cluster_by({"cluster_by": ["SEGMENT"]}) == "CLUSTER BY (SEGMENT)"


def test_the_structure_job_renders_the_same_bare_clause():
    assert _structure()._cluster_by_sql({"cluster_by": ["SEGMENT"]}) == "CLUSTER BY (SEGMENT)"


def test_a_key_that_needs_quoting_is_not_carried():
    """`"Seg Code"` cannot be written bare, and a quoted one is refused
    live, so the setting is deferred, never emitted in a form AIDP rejects."""
    out = classify_table_properties(_rec('LINEAR("Seg Code")', name="Seg Code"))
    assert "cluster_by" not in out["features"]
    why = " ".join(str(d) for d in out["deferred"])
    assert "bare" in why and "Seg Code" in why


def test_a_plain_key_is_still_carried():
    out = classify_table_properties(_rec("LINEAR(SEGMENT)"))
    assert out["features"]["cluster_by"] == ["SEGMENT"]
