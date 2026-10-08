"""Deployment gating, batching, and per-statement verification."""
import pytest

from target.coords import resolve_target
from target.deploy import RefusedToExecute, deploy

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="bronze")


def plan(n=1):
    return {"statements": [
        {"source_identifier": f"D.S.T{i}", "target_fqn": f"bronze.S.T{i}",
         "object_type": "TABLE",
         # Carried so deploy can verify the STRUCTURE, not just the name.
         "expected_columns": [{"name": "A", "type": "STRING"}],
         "sql": f"CREATE TABLE IF NOT EXISTS `bronze`.`S`.`T{i}` (`A` STRING)\nUSING DELTA"}
        for i in range(n)], "blocked": []}


class Recorder:
    def __init__(self, existing=None, fail_on=None):
        self.calls = []
        self.existing = existing
        self.fail_on = fail_on or ()

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        if any(f in sql for f in self.fail_on):
            raise RuntimeError("boom")
        if sql.lstrip().upper().startswith("SHOW TABLES"):
            name = sql.split("LIKE")[1].strip().strip("'").replace("\\", "")
            if self.existing is None or name in self.existing:
                return [{"tableName": name}]
            return []
        if sql.lstrip().upper().startswith("DESCRIBE"):
            return [{"col_name": "A", "data_type": "string"}]
        return [{"status": "ok"}]


def test_dry_run_is_the_default_and_touches_nothing():
    rec = Recorder()
    out = deploy(plan(2), run_sql=rec)
    assert out["dry_run"] is True
    assert out["executed"] == 0
    assert rec.calls == [], "dry run must not issue a single statement"
    assert len(out["statements"]) == 2


def test_execute_without_a_target_is_refused():
    with pytest.raises(RefusedToExecute, match="target"):
        deploy(plan(), execute=True, run_sql=Recorder())


def test_execute_without_run_sql_is_refused():
    with pytest.raises(RefusedToExecute, match="run_sql"):
        deploy(plan(), execute=True, target=TARGET)


def test_execute_creates_the_schema_before_the_tables():
    rec = Recorder()
    deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    joined = " || ".join(rec.calls)
    assert "CREATE SCHEMA IF NOT EXISTS" in joined
    assert joined.index("CREATE SCHEMA") < joined.index("CREATE TABLE")


def test_statements_are_batched_into_one_execution_per_chunk():
    # AIDP discards per-statement DDL on session close, so DDL is batched.
    rec = Recorder()
    deploy(plan(5), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    batches = [c for c in rec.calls if c.count("CREATE TABLE") >= 1
               and not c.lstrip().upper().startswith("SHOW")]
    assert len(batches) == 3, "5 statements at chunk_size 2 -> 3 batches"


def test_every_statement_is_probed_individually_after_its_chunk():
    # A chunk can report success while individual statements inside it failed.
    rec = Recorder()
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=3)
    probes = [c for c in rec.calls if c.lstrip().upper().startswith("SHOW TABLES")]
    assert len(probes) == 3
    assert out["verified"] == 3
    assert out["failed"] == []


def test_missing_table_after_a_successful_chunk_is_reported_failed():
    rec = Recorder(existing={"T0"})
    out = deploy(plan(2), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    assert out["verified"] == 1
    assert [f["target_fqn"] for f in out["failed"]] == ["bronze.S.T1"]


def test_already_exists_is_not_a_failure():
    rec = Recorder()
    out = deploy(plan(1), execute=True, target=TARGET, run_sql=rec)
    assert out["failed"] == []


def test_chunk_error_does_not_abort_remaining_chunks():
    rec = Recorder(existing={"T2"}, fail_on=("`T0`",))
    out = deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=1)
    assert out["verified"] == 1
    assert len(out["failed"]) == 2
    assert out["chunk_errors"], "the failing chunk must be recorded, not swallowed"


def test_blocked_statements_are_never_executed():
    p = plan(1)
    p["blocked"] = [{"source_identifier": "D.S.BAD", "reason": "VARIANT"}]
    rec = Recorder()
    out = deploy(p, execute=True, target=TARGET, run_sql=rec)
    assert "BAD" not in " ".join(rec.calls)
    assert out["blocked_count"] == 1


def multi_catalog_plan():
    return {"statements": [
        {"source_identifier": "D1.S.T", "target_fqn": "D1.S.T",
         "sql": "CREATE TABLE IF NOT EXISTS `D1`.`S`.`T` (`A` STRING)\nUSING DELTA"},
        {"source_identifier": "D2.S.U", "target_fqn": "D2.S.U",
         "sql": "CREATE TABLE IF NOT EXISTS `D2`.`S`.`U` (`A` STRING)\nUSING DELTA"},
    ], "blocked": []}


def _target(catalog):
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="ws", cluster_id="cl", catalog=catalog)


def test_only_the_confirmed_catalog_is_deployed():
    # Bronze mirrors the source, so a multi-database estate spans catalogs.
    # One confirmation must not fan out across all of them.
    rec = Recorder()
    out = deploy(multi_catalog_plan(), execute=True, target=_target("D1"),
                 run_sql=rec)
    assert out["statement_count"] == 1
    assert out["catalog_in_scope"] == "D1"
    assert out["out_of_scope_count"] == 1
    assert out["out_of_scope_catalogs"] == ["D2"]
    assert "`D2`" not in " ".join(rec.calls)


def test_dry_run_reports_every_statement_regardless_of_catalog():
    out = deploy(multi_catalog_plan(), run_sql=Recorder())
    assert out["statement_count"] == 2
    assert out["out_of_scope_count"] == 0


def test_per_target_lists_are_emitted_for_status_reporting():
    rec = Recorder(existing={"T0"})
    out = deploy(plan(2), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    assert out["attempted_targets"] == ["D.S.T0", "D.S.T1"]
    assert out["verified_targets"] == ["D.S.T0"]
    assert out["failed_targets"] == ["D.S.T1"]


def test_dry_run_leaves_the_target_lists_empty():
    out = deploy(plan(2), run_sql=Recorder())
    assert out["attempted_targets"] == []
    assert out["verified_targets"] == []


def test_no_generated_or_executed_statement_moves_data():
    # The plugin must not move a byte. Nothing it runs may be DML.
    rec = Recorder()
    deploy(plan(3), execute=True, target=TARGET, run_sql=rec, chunk_size=2)
    for call in rec.calls:
        upper = " " + " ".join(call.split()).upper() + " "
        for verb in (" INSERT ", " COPY INTO ", " MERGE ", " UPDATE ",
                     " DELETE ", " TRUNCATE ", " DROP ", " UNLOAD ",
                     " CREATE TABLE AS ", " AS SELECT "):
            assert verb not in upper, f"{verb.strip()} in: {call[:120]}"


# ==========================================================================
# Verification means "the right structure is there".
#
# The probe used to be `SHOW TABLES ... LIKE '<name>'` followed by `if rows:`.
# Three defects compounded:
#   * `_` is a single-character LIKE wildcard and is in most real table names,
#     so the pattern was not the name
#   * the returned name was never compared to the target
#   * DDL is CREATE TABLE IF NOT EXISTS, so a pre-existing table with
#     DIFFERENT columns is left alone and was then reported as verified
# ==========================================================================

def _stmt(name="T", kind="TABLE", cols=(("ID", "DECIMAL(38,0)"), ("NAME", "STRING"))):
    return {"source_identifier": f"DB.PUBLIC.{name}", "object_type": kind,
            "target_fqn": f"CAT.PUBLIC.{name}",
            "sql": f"CREATE TABLE IF NOT EXISTS `CAT`.`PUBLIC`.`{name}` (...)",
            "expected_columns": [{"name": n, "type": t} for n, t in cols]}


class Probe:
    """run_sql double: records statements, answers SHOW and DESCRIBE."""

    def __init__(self, *, present=("T",), describe=None, show_key="tableName",
                 fail_describe=False):
        self.present, self.describe = list(present), describe
        self.show_key, self.fail_describe = show_key, fail_describe
        self.calls: list[str] = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        low = sql.lower()
        if low.startswith("show "):
            return [{self.show_key: n} for n in self.present
                    if _matches(sql, n)]
        if low.startswith("describe "):
            if self.fail_describe:
                raise RuntimeError("DESCRIBE not supported here")
            cols = self.describe if self.describe is not None else [
                ("ID", "decimal(38,0)"), ("NAME", "string")]
            return [{"col_name": n, "data_type": t} for n, t in cols]
        return []


def _matches(sql, candidate):
    """Crude LIKE evaluation, honouring backslash escapes and `_` wildcards."""
    import re as _re
    body = sql.split("LIKE '", 1)[1].rsplit("'", 1)[0]
    pattern, i = "", 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            pattern += _re.escape(body[i + 1]); i += 2; continue
        pattern += "." if ch == "_" else (".*" if ch == "%" else _re.escape(ch))
        i += 1
    return _re.fullmatch(pattern, candidate, _re.IGNORECASE) is not None


def test_matching_structure_verifies():
    probe = Probe()
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == ["DB.PUBLIC.T"]
    assert out["verified"] == 1
    assert out["mismatched_targets"] == []


def test_an_underscore_in_the_name_is_escaped_so_the_probe_is_not_a_wildcard():
    # ORDER_ITEMS must not be satisfied by ORDERxITEMS.
    probe = Probe(present=["ORDERxITEMS"])
    out = deploy({"statements": [_stmt("ORDER_ITEMS")]}, target=_target("CAT"),
                 execute=True, run_sql=probe)
    assert out["verified_targets"] == []
    assert out["failed_targets"] == ["DB.PUBLIC.ORDER_ITEMS"]
    show = [c for c in probe.calls if c.lower().startswith("show")][0]
    assert r"ORDER\_ITEMS" in show


def test_a_different_name_in_the_result_does_not_count_as_present():
    probe = Probe(present=["SOMETHING_ELSE"])
    out = deploy({"statements": [_stmt("T")]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == []


def test_a_pre_existing_table_with_different_columns_is_a_mismatch_not_a_clone():
    # IF NOT EXISTS left it untouched. Reporting SHALLOW_CLONE here would be a
    # false report: the object in AIDP is not the object we planned.
    probe = Probe(describe=[("ID", "string")])
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == []
    assert out["mismatched_targets"] == ["DB.PUBLIC.T"]
    detail = out["mismatches"][0]
    assert "NAME" in detail["reason"] or "ID" in detail["reason"]


def test_a_column_type_difference_is_a_mismatch():
    probe = Probe(describe=[("ID", "decimal(38,0)"), ("NAME", "int")])
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["mismatched_targets"] == ["DB.PUBLIC.T"]
    assert "NAME" in out["mismatches"][0]["reason"]


def test_column_order_difference_is_a_mismatch():
    probe = Probe(describe=[("NAME", "string"), ("ID", "decimal(38,0)")])
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["mismatched_targets"] == ["DB.PUBLIC.T"]


def test_describe_metadata_rows_are_ignored():
    probe = Probe(describe=[("ID", "decimal(38,0)"), ("NAME", "string"),
                            ("", ""), ("# Partitioning", ""), ("Not partitioned", "")])
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == ["DB.PUBLIC.T"]


def test_a_view_is_probed_with_show_views_not_show_tables():
    probe = Probe(present=["V"], show_key="viewName")
    out = deploy({"statements": [_stmt("V", kind="VIEW", cols=(("ID", "DECIMAL(38,0)"),
                                                               ("NAME", "STRING")))]},
                 target=_target("CAT"), execute=True, run_sql=probe)
    show = [c for c in probe.calls if c.lower().startswith("show")][0]
    assert "SHOW VIEWS" in show
    assert out["verified_targets"] == ["DB.PUBLIC.V"]


def test_structure_that_cannot_be_compared_is_not_reported_as_verified():
    # The AIDP surface is unverified, so DESCRIBE may not answer. Existence
    # alone is not the claim we make.
    probe = Probe(fail_describe=True)
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == []
    assert out["unverified_structure_targets"] == ["DB.PUBLIC.T"]
    assert out["verified"] == 0


def test_unrecognised_show_output_is_reported_rather_than_guessed():
    probe = Probe(present=["T"], show_key="somethingUnexpected")
    out = deploy({"statements": [_stmt()]}, target=_target("CAT"), execute=True,
                 run_sql=probe)
    assert out["verified_targets"] == []
    assert out["failed_targets"] == ["DB.PUBLIC.T"]
    assert "name" in out["failed"][0]["reason"].lower()
