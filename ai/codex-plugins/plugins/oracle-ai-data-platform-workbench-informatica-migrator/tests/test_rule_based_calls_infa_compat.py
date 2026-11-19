"""The rule-based (deterministic, no-LLM) generator must
CALL infa_compat's tested Class C functions -- sequence generator, lookup,
SCD Type-2, update strategy -- the same way wired the LLM prompt to.
Before this task, ``infa_compat`` had zero references in
``generators/write_strategies.py``, ``converters/transformation_converter.py``,
and ``generators/notebook_generator.py`` -- every one of those Class C
semantics was hand-rolled a second time inline (SCD2 MERGE,
``F.monotonically_increasing_id()`` "sequences", a hand-built broadcast
join with no multiple-match policy, and DD_DELETE via a raw
``DeltaTable.merge()``), duplicating the exact semantics
``engine/infa_compat`` already implements and tests.

PySpark is not installed in this environment (see repo-wide convention) --
these tests assert on the emitted CALL SHAPE (the literal ``infa_compat.X(``
text and its arguments), never on executing the generated code.
"""
from __future__ import annotations

import infa_compat
from infa2aidp.converters.transformation_converter import TransformationConverter
from infa2aidp.generators.notebook_generator import NotebookGenerator
from infa2aidp.generators.write_strategies import DeltaWriteStrategy
from infa2aidp.models import (
    Connector,
    DataFlowDirection,
    FieldMapping,
    LoadStrategy,
    Mapping,
    TargetDefinition,
    Transformation,
    TransformationField,
    TransformationType,
)


# ---------------------------------------------------------------------------
# Sequence Generator -- transformation_converter.py
# ---------------------------------------------------------------------------

def test_sequence_generator_calls_infa_compat_sequence_not_monotonically_increasing_id():
    tx = Transformation(
        name="SEQ_CUSTOMER_SK",
        type=TransformationType.SEQUENCE_GENERATOR,
        fields=[TransformationField(name="NEXTVAL")],
        start_value=1,
        increment_by=1,
    )
    lines = TransformationConverter().convert(tx, input_df="df", output_df="df")
    code = "\n".join(lines)
    assert "infa_compat.sequence(" in code
    assert ".assign(" in code
    assert "F.monotonically_increasing_id()" not in code
    assert "row_number()" not in code


def test_sequence_generator_current_value_offset_still_reported():
    """'s pinned regression (Current Value spelling) must survive
    the switch to infa_compat.sequence() -- the comment line is the
    contract, not the counter implementation underneath it."""
    tx = Transformation(
        name="SEQ_T",
        type=TransformationType.SEQUENCE_GENERATOR,
        fields=[TransformationField(name="NEXTVAL")],
    )
    tx.properties["Current Value"] = "500"
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    assert "Informatica Current Value: 500" in code
    # The first NEXTVAL is the Current Value itself (it used to be 501).
    assert "start=500" in code


# ---------------------------------------------------------------------------
# Lookup -- transformation_converter.py
# ---------------------------------------------------------------------------

def _lookup_tx(condition: str, lookup_table: str = "LKP_TABLE") -> Transformation:
    return Transformation(
        name="LKP_T",
        type=TransformationType.LOOKUP,
        lookup_table=lookup_table,
        lookup_condition=condition,
    )


def test_lookup_with_differently_named_keys_renames_then_calls_cached_lookup():
    # The keys genuinely differ: the lookup column is EMP_NO, the pipeline
    # column EMP_ID. (This test used "LKP_EMP_ID = SQ_EMPLOYEES.EMP_ID" and
    # asserted a rename -- which pinned a rename of EMP_ID to the dotted name
    # "SQ_EMPLOYEES.EMP_ID", a column Spark cannot resolve; on AIDP the
    # notebook failed there. A qualifier is not part of the column name.)
    tx = _lookup_tx("LKP_EMP_NO = SQ_EMPLOYEES.EMP_ID")
    tx.fields = [TransformationField(
        name="LKP_DEPT", expression="DEPT_TABLE.DEPT",
        direction=DataFlowDirection.OUTPUT,
    )]
    code = "\n".join(TransformationConverter().convert(tx, input_df="df_source", output_df="df_source"))
    assert "infa_compat.cached_lookup(" in code
    assert "policy='first'" in code  # export sets no policy -> Informatica default
    assert '.withColumnRenamed("EMP_NO", "EMP_ID")' in code
    assert "SQ_EMPLOYEES." not in code.replace("# ", "")  # no dotted column name reaches Spark
    assert ".drop(" not in code  # cached_lookup's named join needs no manual drop


def test_lookup_multiple_match_policy_comment_present():
    """The Class C reason this isn't a hand-rolled join: which lookup row
    wins on a duplicate key is a documented, callable policy."""
    tx = _lookup_tx("LKP_EMP_ID = SQ_EMPLOYEES.EMP_ID")
    code = "\n".join(TransformationConverter().convert(tx, input_df="df_source", output_df="df_source"))
    assert "Lookup policy on multiple match" in code


def test_lookup_policy_comes_from_the_export_not_a_constant():
    """'Lookup policy on multiple match' is a TABLEATTRIBUTE on every
    PowerCenter Lookup. It used to be ignored: a "Report Error" lookup ran
    as "Use First Value" and a duplicate key silently picked a row where
    the session would have failed."""
    for raw, expected in (
        ("Use Last Value", "policy='last'"),
        ("Report Error", "policy='error'"),
        ("Use Any Value", "policy='first'"),
        ("Use First Value", "policy='first'"),
    ):
        tx = _lookup_tx("LKP_EMP_ID = SQ_EMPLOYEES.EMP_ID")
        tx.lookup_policy = raw
        code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
        assert expected in code, (raw, code)


def test_dynamic_lookup_cache_is_a_review_item_not_a_static_join_in_disguise():
    tx = _lookup_tx("LKP_EMP_ID = SQ_EMPLOYEES.EMP_ID")
    tx.lookup_dynamic = True
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    assert "REVIEW REQUIRED" in code and "DYNAMIC" in code


def test_lookup_join_key_resolves_the_input_port_to_the_pipeline_column():
    """The lookup condition names the Lookup's OWN input port (IN_CUST_ID);
    the pipeline DataFrame carries the upstream column (CUST_ID). The
    generator passes the connector-derived port map; the join must be on
    the pipeline column or Spark raises 'column not found'."""
    tx = _lookup_tx("LKP_CUST_ID = IN_CUST_ID", lookup_table="DIM_CUSTOMER")
    code = "\n".join(TransformationConverter().convert(
        tx, input_df="df", output_df="df",
        extra_inputs={"__port_map__": {"IN_CUST_ID": "CUST_ID"}},
    ))
    assert "on=['CUST_ID']" in code
    assert 'withColumnRenamed("CUST_ID", "CUST_ID")' not in code


# ---------------------------------------------------------------------------
# Update Strategy -- transformation_converter.py
# ---------------------------------------------------------------------------

def test_update_strategy_dd_delete_tags_the_real_int_enum_not_a_string():
    """infa_compat.apply_update_strategy() filters DD_STRATEGY against the
    literal infa_compat.UpdateStrategyCode INT value, never a string label
    -- see engine/infa_compat/update_strategy.py. Tagging the column with
    F.lit("DD_DELETE") would silently produce four empty partitions."""
    tx = Transformation(
        name="UPD1", type=TransformationType.UPDATE_STRATEGY,
        update_strategy_expression="DD_DELETE",
    )
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    # The expression converter emits Informatica's integer code for
    # DD_DELETE, which is the value apply_update_strategy() filters on.
    assert f"F.lit({int(infa_compat.UpdateStrategyCode.DD_DELETE)})" in code
    assert '"DD_STRATEGY"' in code
    assert 'F.lit("DD_DELETE")' not in code
    assert int(infa_compat.UpdateStrategyCode.DD_DELETE) == 2  # sanity: real enum, not guessed


def test_update_strategy_mixed_insert_update_tags_conditionally():
    """IIF(ISNULL(TGT_ID), DD_INSERT, DD_UPDATE): a lookup MISS inserts,
    a hit updates. The previous keyword scan hard-wired
    'condition true -> DD_UPDATE' and INVERTED this, the most common form
    of the transformation; it also converted the whole IIF as the
    condition and referenced columns named DD_INSERT/DD_UPDATE."""
    tx = Transformation(
        name="UPD2", type=TransformationType.UPDATE_STRATEGY,
        update_strategy_expression="IIF(ISNULL(TGT_ID), DD_INSERT, DD_UPDATE)",
    )
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    ins = int(infa_compat.UpdateStrategyCode.DD_INSERT)
    upd = int(infa_compat.UpdateStrategyCode.DD_UPDATE)
    assert f"F.when(F.isnull(F.col('TGT_ID')), F.lit({ins})).otherwise(F.lit({upd}))" in code
    assert "F.col('DD_INSERT')" not in code and "F.col('DD_UPDATE')" not in code
    assert '.cast("int")' in code


def test_update_strategy_reject_is_tagged_not_silently_dropped():
    """Pre-existing bug this replaces: the old code ran
    ``.filter(~(cond))`` for DD_REJECT with no reject-sink routing at all
    -- rejected rows just vanished. Now they're tagged so
    infa_compat.write_update_strategy()'s required reject_sink can catch
    them downstream."""
    tx = Transformation(
        name="UPD3", type=TransformationType.UPDATE_STRATEGY,
        update_strategy_expression="DD_REJECT",
    )
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    assert f"F.lit({int(infa_compat.UpdateStrategyCode.DD_REJECT)})" in code
    assert ".filter(" not in code


def test_update_strategy_unconvertible_expression_rejects_every_row():
    """An expression the converter cannot translate must not fall back to
    'insert everything': every row is tagged DD_REJECT and a REVIEW
    REQUIRED marker names the expression."""
    tx = Transformation(
        name="UPD4", type=TransformationType.UPDATE_STRATEGY,
        update_strategy_expression="IIF(SOME_UNKNOWN_FN(X), DD_INSERT, DD_UPDATE)",
    )
    code = "\n".join(TransformationConverter().convert(tx, input_df="df", output_df="df"))
    assert "REVIEW REQUIRED" in code
    assert f"F.lit({int(infa_compat.UpdateStrategyCode.DD_REJECT)})" in code


# ---------------------------------------------------------------------------
# Notebook header: version pin, mirroring the LLM path
# ---------------------------------------------------------------------------

def test_notebook_generator_pins_infa_compat_version():
    cell = NotebookGenerator()._infa_compat_header_cell()
    assert "import infa_compat" in cell
    assert repr(infa_compat.__version__) in cell
    assert "infa_compat.__version__" in cell


def test_generate_round_trip_includes_infa_compat_header():
    mapping = Mapping(name="M_SIMPLE")
    from infa2aidp.models import Session

    nb = NotebookGenerator().generate(
        mapping, Session(name="s_M_SIMPLE"), {"transformations": {}, "source_reads": [], "target_write": ""},
        output_format="ipynb",
    )
    assert "import infa_compat" in nb
    assert repr(infa_compat.__version__) in nb


# ---------------------------------------------------------------------------
# Target write: Update Strategy routing (notebook_generator.py)
# ---------------------------------------------------------------------------

def _mapping_with_update_strategy(expr: str, has_key: bool = True) -> Mapping:
    fields = [FieldMapping(source_field="A", target_field="A")]
    if has_key:
        fields.append(FieldMapping(source_field="ID", target_field="ID", is_key=True))
    target = TargetDefinition(name="TGT1", table_name="TGT1", fields=fields)
    tx = Transformation(
        name="UPD1", type=TransformationType.UPDATE_STRATEGY,
        update_strategy_expression=expr,
    )
    return Mapping(
        name="M",
        targets=[target],
        transformations=[tx],
        connectors=[Connector(from_instance="UPD1", from_field="A", to_instance="TGT1", to_field="A")],
    )


def test_target_write_cell_for_update_strategy_calls_infa_compat():
    mapping = _mapping_with_update_strategy("DD_DELETE")
    gen = NotebookGenerator(target_catalog_type="delta")
    cell = gen._target_write_cell_for(mapping, mapping.targets[0], {}, 0)
    assert "infa_compat.apply_update_strategy(" in cell
    assert "infa_compat.write_update_strategy(" in cell
    assert "DeltaTable" not in cell
    assert "whenMatchedDelete" not in cell


def test_target_write_cell_for_update_strategy_no_keys_is_review_required():
    """Never silently downgrade to a full-table write when there's no key
    to build a MERGE match condition from -- that's the worst failure
    class this project guards against everywhere else."""
    mapping = _mapping_with_update_strategy("DD_DELETE", has_key=False)
    gen = NotebookGenerator(target_catalog_type="delta")
    cell = gen._target_write_cell_for(mapping, mapping.targets[0], {}, 0)
    assert "REVIEW REQUIRED" in cell
    assert "infa_compat.write_update_strategy(" not in cell


def test_target_write_cell_for_without_update_strategy_is_unchanged_default():
    """No Update Strategy transformation feeds this target -- infa_compat
    has no "just do a generic upsert" call (apply_update_strategy needs a
    real DD_STRATEGY column), so a keyed UPSERT stays on the keys-based
    MERGE default, not diverted through infa_compat. (An INSERT appends:
    it used to be upgraded to UPSERT for multi-target mappings only.)"""
    fields = [FieldMapping(source_field="A", target_field="A"),
              FieldMapping(source_field="ID", target_field="ID", is_key=True)]
    target = TargetDefinition(name="TGT1", table_name="TGT1", fields=fields,
                              load_strategy=LoadStrategy.UPSERT)
    mapping = Mapping(name="M", targets=[target], transformations=[], connectors=[])
    gen = NotebookGenerator(target_catalog_type="delta")
    cell = gen._target_write_cell_for(mapping, mapping.targets[0], {}, 0)
    assert "apply_update_strategy" not in cell  # delta_name() is infa_compat too
    assert "DeltaTable.forName" in cell


# ---------------------------------------------------------------------------
# SCD2 write -- write_strategies.py
# ---------------------------------------------------------------------------

def test_scd2_write_calls_infa_compat_scd2_merge():
    code = "\n".join(DeltaWriteStrategy().emit_write(
        mapping=None, target=None, table="CAT.SCH.DIM_CUSTOMER",
        load_strategy=LoadStrategy.SCD_TYPE2, key_columns=["CUST_CODE"],
        df_var="df_final",
    ))
    assert "infa_compat.scd2_merge(" in code
    assert "source=df_final" in code
    assert "DeltaTable" not in code


def test_scd2_write_respects_df_var_not_hardcoded_df_final():
    """Pre-existing bug this replaces: the old inline _scd2_write ignored
    its own df_var parameter and always wrote literal "df_final" /
    "df_changed_records" -- a non-default df_var was silently dropped."""
    code = "\n".join(DeltaWriteStrategy().emit_write(
        mapping=None, target=None, table="CAT.SCH.DIM_CUSTOMER",
        load_strategy=LoadStrategy.SCD_TYPE2, key_columns=["CUST_CODE"],
        df_var="df_router_changed",
    ))
    assert "source=df_router_changed" in code
    assert "df_final" not in code
    assert "df_changed_records" not in code


def test_scd2_write_generates_surrogate_key_via_infa_compat_sequence():
    """_merge_write() prefers natural keys and drops any surrogate key
    from `keys` whenever a natural key is also present -- so the only way
    to exercise _scd2_write's surrogate-key branch through the public
    emit_write() entry point is a key list that is ENTIRELY surrogate."""
    code = "\n".join(DeltaWriteStrategy().emit_write(
        mapping=None, target=None, table="CAT.SCH.DIM_CUSTOMER",
        load_strategy=LoadStrategy.SCD_TYPE2, key_columns=["CUST_SK"],
        df_var="df_final",
    ))
    assert "infa_compat.sequence(" in code
    assert "Window.orderBy(F.monotonically_increasing_id())" not in code
