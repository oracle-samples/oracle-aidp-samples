"""Target writes must differ by catalog type, and never silently downgrade."""
import pytest
from infa2aidp.models import LoadStrategy


def test_delta_upsert_emits_delta_merge():
    """SCD2 write is Class C -- it must call the tested
    infa_compat.scd2_merge() rather than hand-roll a second copy of the
    same expire-then-insert MERGE."""
    from infa2aidp.generators.write_strategies import DeltaWriteStrategy
    code = "\n".join(DeltaWriteStrategy().emit_write(
        mapping=None, target=None, table="CAT.SCH.DIM_CUSTOMER",
        load_strategy=LoadStrategy.SCD_TYPE2, key_columns=["CUST_CODE"]))
    assert "infa_compat.scd2_merge(" in code
    assert "DeltaTable.forName" not in code and ".whenMatchedUpdate(" not in code


def test_adw_upsert_does_NOT_emit_delta_merge():
    """DeltaTable is Delta-only. Emitting it for ADW fails at runtime."""
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="MY_SCHEMA.DIM_CUSTOMER",
        load_strategy=LoadStrategy.SCD_TYPE2, key_columns=["CUST_CODE"]))
    assert "DeltaTable" not in code, "Delta API emitted for an ADW target"
    assert 'format("jdbc")' in code


def test_adw_full_load_uses_jdbc_overwrite():
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="MY_SCHEMA.FACT_ORDERS",
        load_strategy=LoadStrategy.TRUNCATE_INSERT, key_columns=[]))
    assert 'format("jdbc")' in code and '"overwrite"' in code


def test_adw_upsert_stages_then_merges_on_the_database():
    """External catalogs support overwrite only, so an upsert must stage
    then run Oracle MERGE from the driver - not attempt it in Spark."""
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="MY_SCHEMA.DIM_CUSTOMER",
        load_strategy=LoadStrategy.UPSERT, key_columns=["CUST_CODE"]))
    assert "MERGE INTO" in code.upper()
    assert "oracledb" in code, "Oracle MERGE must run from the driver"


def test_adw_upsert_never_silently_downgrades_to_overwrite():
    """A silent downgrade would delete rows the mapping meant to preserve."""
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="T", load_strategy=LoadStrategy.UPSERT,
        key_columns=["K"]))
    lowered = code.lower()
    assert not ('mode("overwrite")' in lowered and "merge into" not in lowered)


def test_adw_upsert_with_no_keys_is_a_review_item():
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="T", load_strategy=LoadStrategy.UPSERT,
        key_columns=[]))
    assert "REVIEW REQUIRED" in code.upper()


def test_adw_wallet_is_staged_under_tmp_not_workspace():
    """/Workspace is FUSE-mounted; the JDBC driver process can't read it."""
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="T",
        load_strategy=LoadStrategy.TRUNCATE_INSERT, key_columns=[]))
    assert "/Workspace" not in code
    assert "/tmp" in code


def test_adw_strategy_marks_itself_unverified():
    """Derived from the connector reference, never run against a live ADW."""
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    code = "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="T",
        load_strategy=LoadStrategy.TRUNCATE_INSERT, key_columns=[]))
    assert "UNVERIFIED" in code.upper()


# ── Spark -> Oracle type fidelity (hazard 30) ────────────────────────

def _adw_code(load_strategy=None, keys=None):
    from infa2aidp.generators.write_strategies import AdwWriteStrategy
    from infa2aidp.models import LoadStrategy
    return "\n".join(AdwWriteStrategy().emit_write(
        mapping=None, target=None, table="T",
        load_strategy=load_strategy or LoadStrategy.TRUNCATE_INSERT,
        key_columns=keys or [],
    ))


def test_the_adw_write_checks_spark_to_oracle_type_fidelity():
    """Oracle's published mapping loses data for several Spark types, and
    storeAssignmentPolicy=LEGACY is pinned for the write -- so Spark
    truncates instead of raising. Without this check the loss is silent
    exactly where a reconciliation would care."""
    code = _adw_code()
    assert "_ORACLE_TYPE_RISK" in code
    assert "Spark->Oracle type fidelity" in code


def test_a_map_column_blocks_the_write_rather_than_truncating():
    """MapType has no Oracle target type at all, so this is not a warning."""
    code = _adw_code()
    assert '"map": ("BLOCKED"' in code
    assert "raise ValueError" in code


def test_double_and_boolean_are_called_out_as_lossy():
    code = _adw_code()
    assert '"double": ("LOSSY"' in code
    assert '"boolean": ("LOSSY"' in code
    assert "decimal(p,s) instead" in code, "the fix should be named, not just the risk"


def test_the_check_is_emitted_on_every_write_path():
    from infa2aidp.models import LoadStrategy
    for strategy in (LoadStrategy.TRUNCATE_INSERT, LoadStrategy.UPSERT,
                     LoadStrategy.INSERT):
        code = _adw_code(strategy, keys=["ID"])
        assert "_ORACLE_TYPE_RISK" in code, f"missing on {strategy}"


# ---------------------------------------------------------------------------
# The declared-range check: executed, not just emitted
# ---------------------------------------------------------------------------

from infa2aidp.generators.write_strategies import (  # noqa: E402
    _declared_numeric_ranges,
    _range_check_lines,
)
from infa2aidp.models import FieldMapping, TargetDefinition  # noqa: E402

from tests.conftest_spark import spark  # noqa: F401,E402


def _numeric_target():
    return TargetDefinition(
        name="TGT", table_name="TGT",
        fields=[
            FieldMapping(source_field="a", target_field="QTY",
                         datatype="NUMBER", precision=3, scale=0),
            FieldMapping(source_field="b", target_field="NOTE",
                         datatype="VARCHAR2", precision=50, scale=0),
        ],
    )


def test_only_declared_numerics_are_range_checked():
    """A string column cannot overflow the same silent way."""
    assert _declared_numeric_ranges(_numeric_target()) == [("QTY", 3, 0)]


def test_a_target_with_no_declared_numerics_emits_nothing():
    tgt = TargetDefinition(name="T", table_name="T", fields=[
        FieldMapping(source_field="a", target_field="NOTE",
                     datatype="VARCHAR2", precision=10, scale=0)])
    assert _range_check_lines("df_final", tgt) == []


def _run_check(spark, rows, target):
    """Execute the emitted check over `rows`, as the notebook would."""
    from pyspark.sql import functions as F
    df_final = spark.createDataFrame(rows, "QTY int, NOTE string")
    code = "\n".join(l[4:] if l.startswith("    ") else l
                     for l in _range_check_lines("df_final", target))
    exec(compile(code, "<range_check>", "exec"),
         {"F": F, "df_final": df_final}, {})


def test_an_overflowing_value_raises_instead_of_being_wrapped(spark):  # noqa: F811
    """The behaviour this exists to prevent.

    storeAssignmentPolicy is pinned LEGACY so migrated expressions keep
    Informatica's permissive evaluation; on the write that same setting
    makes Spark WRAP -- 3000000000 into an INT stores -1294967296. The pin
    stays and the write becomes loud instead of lossy.
    """
    import pytest as _pytest
    with _pytest.raises(ValueError) as exc:
        _run_check(spark, [(1, "ok"), (5000, "too wide")], _numeric_target())
    msg = str(exc.value)
    assert "QTY" in msg and "NUMBER(3,0)" in msg
    assert "1 row(s)" in msg
    assert "wrap these silently" in msg


def test_in_range_values_do_not_raise(spark):  # noqa: F811
    """999 fits NUMBER(3,0); 1000 would not. The boundary matters, because a
    check that fires on good data gets switched off."""
    _run_check(spark, [(999, "max"), (-999, "min"), (0, "zero"), (None, "null")],
               _numeric_target())


def test_the_check_precedes_the_write_in_every_strategy():
    """Prepended in emit_write rather than per branch, so a write path added
    later cannot skip it."""
    from infa2aidp.generators.write_strategies import DeltaWriteStrategy
    from infa2aidp.models import LoadStrategy

    for strategy in (LoadStrategy.INSERT, LoadStrategy.UPSERT,
                     LoadStrategy.TRUNCATE_INSERT, LoadStrategy.SCD_TYPE2):
        lines = DeltaWriteStrategy().emit_write(
            None, _numeric_target(), "db.TGT", strategy, ["QTY"], "df_final")
        text = "\n".join(lines)
        assert "_DECLARED_RANGES" in text, strategy
        # It is PREPENDED, so it precedes whatever the strategy does -- a
        # saveAsTable, a Delta merge, or a delegated infa_compat call (SCD2
        # hands the write to infa_compat.scd_type2, which is why asserting
        # on a saveAsTable literal would have been wrong).
        work = [i for i, l in enumerate(lines)
                if "infa_compat." in l or ".write" in l or ".merge(" in l]
        assert work, f"{strategy} emitted no write at all"
        check_at = next(i for i, l in enumerate(lines) if "_DECLARED_RANGES" in l)
        assert check_at < min(work), strategy
