"""Deploy structure through the catalog API. I/O injected as `call`."""
import pytest

from target.catalog_deploy import RefusedToExecute, deploy_catalog
from target.coords import resolve_target

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="lake")


def _plan(n=1, views=0):
    stmts = [{"source_identifier": f"DB.PUBLIC.T{i}", "object_type": "TABLE",
              "target_fqn": f"lake.DB.T{i}", "sql": "CREATE TABLE ...",
              "expected_columns": [{"name": "A", "type": "STRING"}]}
             for i in range(n)]
    stmts += [{"source_identifier": f"DB.PUBLIC.V{i}", "object_type": "VIEW",
               "target_fqn": f"lake.DB.V{i}", "sql": "CREATE VIEW ...",
               "view_text": "select 1 as a",
               "expected_columns": [{"name": "A", "type": "STRING"}]}
              for i in range(views)]
    return {"statements": stmts, "blocked": []}


class Recorder:
    """Injected transport double. Records every operation."""

    def __init__(self, *, exists=(), fail_on=()):
        self.ops: list[tuple] = []
        self.created: set[str] = set(exists)
        self.fail_on = set(fail_on)

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        cat = kw.get("catalog")
        name = kw.get("table") or kw.get("view") or kw.get("schema")
        if operation == "list_catalogs":
            # The deploy resolves the catalog TYPE before its first create.
            return {"items": [{"displayName": "lake",
                               "catalogType": "STANDARD"}]}
        if operation in ("create_table", "create_view", "create_schema"):
            if name in self.fail_on:
                raise RuntimeError(f"boom on {name}")
            self.created.add(name)
            return {"key": f'{cat}.{kw.get("schema")}.{name}'}
        if operation == "list_schemas":
            # Only reports schemas that have actually been created, so a fresh
            # Recorder starts empty like a real catalog.
            return {"items": [{"key": f"{cat}.{s}", "lifecycleState": "ACTIVE"}
                              for s in ("DB",) if s in self.created]}
        if operation in ("list_tables_in", "list_views_in"):
            # A real list entry carries no fields.
            return {"items": [{"key": f'{kw["schema"]}.{n}'}
                              for n in sorted(self.created)]}
        if operation in ("get_table", "get_view"):
            if name not in self.created:
                raise RuntimeError("404 not found")
            fields = [{"fieldName": "A", "fieldType": "string"}]
            return {"key": name, "tableFields": fields, "viewFields": fields}
        raise AssertionError(f"unexpected operation {operation}")


def test_dry_run_calls_nothing():
    rec = Recorder()
    out = deploy_catalog(_plan(2), target=TARGET, execute=False, call=rec, retry_delays=(), verify_delays=())
    assert rec.ops == []
    assert out["dry_run"] is True
    assert out["statement_count"] == 2


def test_execute_creates_the_schema_before_its_tables():
    rec = Recorder()
    deploy_catalog(_plan(2), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    kinds = [o[0] for o in rec.ops]
    # The catalog TYPE gates everything: an EXTERNAL catalog is refused before
    # the first create. Then it LOOKS -- re-POSTing an existing schema drops
    # the table creates that follow -- and creates the schema once if absent.
    assert kinds[0] == "list_catalogs"
    assert kinds[1] == "list_schemas"
    assert kinds.count("create_schema") == 1, "one schema, created once"
    assert kinds.index("create_schema") < kinds.index("create_table")
    assert kinds.count("create_table") == 2


def test_views_are_created_after_tables():
    rec = Recorder()
    deploy_catalog(_plan(2, views=1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    kinds = [o[0] for o in rec.ops]
    assert kinds.index("create_view") > max(
        i for i, k in enumerate(kinds) if k == "create_table")


def test_each_object_is_verified_by_reading_it_back():
    rec = Recorder()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    # Resolved by LISTING, not by GET-on-a-guessed-key: the server's key case
    # is not the one we asked for.
    assert "list_tables_in" in [o[0] for o in rec.ops]
    assert out["verified_targets"] == ["DB.PUBLIC.T0"]
    assert out["verified"] == 1


def test_a_structure_mismatch_on_read_back_is_not_verified():
    class Mismatch(Recorder):
        def __call__(self, operation, **kw):
            out = super().__call__(operation, **kw)
            if operation == "get_table":
                return {"key": "T0",
                        "tableFields": [{"fieldName": "DIFFERENT",
                                         "fieldType": "string"}]}
            return out

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Mismatch(), retry_delays=(), verify_delays=())
    assert out["verified_targets"] == []
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]
    assert "DIFFERENT" in out["mismatches"][0]["reason"]


def test_a_create_failure_does_not_abort_the_rest():
    rec = Recorder(fail_on={"T0"})
    out = deploy_catalog(_plan(3), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    assert out["verified"] == 2
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]
    assert out["errors"], "the failure must be recorded, not swallowed"


def test_an_existing_object_is_reported_not_replaced():
    # The create fails because it already exists; the read-back still matches
    # the plan, so it counts as verified. An object that already matches is
    # indistinguishable from one we made.
    rec = Recorder(exists={"T0"}, fail_on={"T0"})
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    # Create failed because it exists; the read-back still matches the plan.
    assert out["verified_targets"] == ["DB.PUBLIC.T0"]
    assert any("already" in n.lower() or "boom" in n.lower()
               for n in out["errors"])


def test_execute_without_a_target_is_refused():
    with pytest.raises(Exception):
        deploy_catalog(_plan(1), target=None, execute=True, call=Recorder(), retry_delays=(), verify_delays=())


def test_out_of_scope_catalogs_are_not_touched():
    plan = _plan(1)
    plan["statements"].append(
        {"source_identifier": "DB2.PUBLIC.X", "object_type": "TABLE",
         "target_fqn": "other_catalog.DB2.X", "sql": "",
         "expected_columns": [{"name": "A", "type": "STRING"}]})
    rec = Recorder()
    out = deploy_catalog(plan, target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    assert out["out_of_scope_count"] == 1
    assert all(kw["catalog"] == "lake" for _, kw in rec.ops if "catalog" in kw)


def test_no_row_data_is_ever_sent():
    rec = Recorder()
    deploy_catalog(_plan(2, views=1), target=TARGET, execute=True, call=rec, retry_delays=(), verify_delays=())
    blob = repr(rec.ops).upper()
    for banned in ("INSERT", "VALUES ", "COPY ", "MERGE "):
        assert banned not in blob


# ==========================================================================
# AIDP LOWER-CASES IDENTIFIERS. Verified live: a schema created as
# "SNOWMIG_TESTDB" comes back as "lake.snowmig_testdb", so every
# schemaKey and read-back key built from the REQUESTED case was wrong and all
# seven objects reported failed.
#
# The fix is to discover the case rather than assume it.
# ==========================================================================

class Folding:
    """Transport double that lower-cases names, as the real API does."""

    def __init__(self, *, conflict_times=0):
        self.ops: list[tuple] = []
        self.schemas: dict[str, str] = {}
        self.tables: dict[str, dict] = {}
        self.conflict_times = conflict_times

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        cat = kw.get("catalog")
        if operation == "list_catalogs":
            return {"items": [{"displayName": "lake",
                               "catalogType": "STANDARD"}]}
        if operation == "create_schema":
            key = f'{cat}.{kw["schema"]}'.lower()
            self.schemas[key] = key
            return {"key": key}
        if operation == "list_schemas":
            return {"items": [{"key": k} for k in self.schemas]}
        if operation in ("create_table", "create_view"):
            if self.conflict_times > 0:
                self.conflict_times -= 1
                raise RuntimeError("backend returned 409 Conflict: ongoing operation")
            name = kw.get("table") or kw.get("view")
            key = f'{cat}.{kw["schema"]}.{name}'.lower()
            fields = kw["body"].get("tableFields") or kw["body"].get("viewFields")
            self.tables[key] = {"key": key, "tableFields": fields,
                                "viewFields": fields}
            return {"key": key}
        if operation in ("list_tables_in", "list_views_in"):
            return {"items": [dict(v) for k, v in self.tables.items()
                              if k.startswith(kw["schema"].lower() + ".")]}
        if operation in ("get_table", "get_view"):
            name = kw.get("table") or kw.get("view")
            key = f'{cat}.{kw["schema"]}.{name}'.lower()
            full = self.tables.get(key)
            if full is None:
                raise RuntimeError("404 not found")
            return dict(full)
        raise AssertionError(f"unexpected op {operation}")


def test_the_schema_key_is_resolved_from_the_server_not_assumed():
    fold = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    # Every table body must carry the key the SERVER reported, lower-cased.
    creates = [kw for op, kw in fold.ops if op == "create_table"]
    assert creates[0]["body"]["schemaKey"] == "lake.db"
    assert out["resolved_schema_keys"] == {"lake.DB": "lake.db"}


def test_objects_are_verified_despite_the_case_change():
    fold = Folding()
    out = deploy_catalog(_plan(2), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 2, out["failed"] + out["mismatches"]
    assert out["failed_targets"] == []


def test_a_view_is_verified_despite_the_case_change():
    fold = Folding()
    out = deploy_catalog(_plan(1, views=1), target=TARGET, execute=True,
                         call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 2


def test_field_names_are_compared_case_insensitively():
    # The server lower-cases field names too; that is a case fold, not a
    # structure mismatch.
    fold = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == []


def test_a_409_conflict_is_retried_rather_than_reported_as_failure():
    # Schema creation is asynchronous, so the first table create can land
    # while the schema is still settling.
    fold = Folding(conflict_times=2)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold,
                         retry_delays=(0, 0, 0))
    assert out["verified"] == 1
    assert any("409" in e for e in out["errors"]), "the retry must be visible"


def test_retries_are_bounded_and_the_failure_is_reported():
    fold = Folding(conflict_times=99)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=fold,
                         retry_delays=(0, 0), verify_delays=())
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]


def test_a_genuine_structure_difference_is_still_a_mismatch():
    class Wrong(Folding):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                return {"key": "t0",
                        "tableFields": [{"fieldName": "different",
                                         "fieldType": "string"}]}
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Wrong(), retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]


# ==========================================================================
# Creation is ASYNCHRONOUS. Verified live: POST returns 202 Accepted with an
# empty body, and the object appears seconds later -- or never, if the async
# work fails, which it does silently. So the read-back must POLL, and a
# never-appearing object is a failure with a real explanation.
# ==========================================================================

class Delayed(Folding):
    """Appears only after `appear_after` list calls, as an async create does."""

    def __init__(self, *, appear_after=2, never=False):
        super().__init__()
        self.list_calls = 0
        self.appear_after = appear_after
        self.never = never

    def __call__(self, operation, **kw):
        if operation in ("list_tables_in", "list_views_in"):
            self.list_calls += 1
            if self.never or self.list_calls <= self.appear_after:
                self.ops.append((operation, kw))
                return {"items": []}
        return super().__call__(operation, **kw)


def test_the_read_back_polls_until_the_object_appears():
    d = Delayed(appear_after=2)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                         verify_delays=(0, 0, 0))
    assert out["verified"] == 1, out["failed"]
    assert d.list_calls >= 3, "it must have polled, not asked once"


def test_an_object_that_never_appears_is_a_failure_with_a_reason():
    d = Delayed(never=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                         verify_delays=(0, 0))
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]
    reason = out["failed"][0]["reason"].lower()
    assert "202" in reason or "async" in reason or "accepted" in reason


def test_polling_is_bounded():
    d = Delayed(never=True)
    deploy_catalog(_plan(1), target=TARGET, execute=True, call=d,
                   verify_delays=(0, 0))
    assert d.list_calls <= 4, "polling must not loop forever"


def test_a_blocked_column_type_is_reported_before_any_create():
    # timestamp_ntz is silently rejected by the API, so it must be caught
    # here rather than becoming an accepted-then-vanished table.
    plan = {"statements": [
        {"source_identifier": "DB.PUBLIC.T", "object_type": "TABLE",
         "target_fqn": "lake.DB.T", "sql": "",
         "expected_columns": [{"name": "TS", "type": "TIMESTAMP_NTZ"}]}],
        "blocked": []}
    fold = Folding()
    out = deploy_catalog(plan, target=TARGET, execute=True, call=fold, retry_delays=(), verify_delays=())
    assert out["verified"] == 0
    assert out["failed_targets"] == ["DB.PUBLIC.T"]
    assert "timestamp_ntz" in out["failed"][0]["reason"].lower()
    assert not any(o[0] == "create_table" for o in fold.ops), \
        "an unbuildable body must never be POSTed"


# ==========================================================================
# Do not re-create a schema that already exists.
#
# Verified live: POSTing a schema that is already there re-triggers async
# work, and table creates issued during that window are ACCEPTED (202) and
# then silently dropped -- six tables returned 202 and none appeared, while
# the identical bodies posted against a settled schema all landed.
#
# So: resolve first, create only if absent, and wait for ACTIVE.
# ==========================================================================

class Settled(Folding):
    """Schema already exists and is ACTIVE."""

    def __init__(self, state="ACTIVE"):
        super().__init__()
        self.schemas["lake.db"] = "lake.db"
        self.state = state

    def __call__(self, operation, **kw):
        if operation == "list_schemas":
            self.ops.append((operation, kw))
            return {"items": [{"key": "lake.db", "lifecycleState": self.state}]}
        return super().__call__(operation, **kw)


def test_an_existing_schema_is_not_re_created():
    s = Settled()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=())
    assert not any(op == "create_schema" for op, _ in s.ops), \
        "re-POSTing an existing schema drops the table creates that follow"
    assert out["schemas_created"] == []
    assert out["schemas_reused"] == ["lake.db"]
    assert out["verified"] == 1


def test_a_missing_schema_is_still_created():
    f = Folding()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=f,
                         retry_delays=(), verify_delays=())
    assert any(op == "create_schema" for op, _ in f.ops)
    assert out["schemas_created"] == ["lake.DB"]


def test_a_schema_that_is_not_active_is_waited_for():
    s = Settled(state="CREATING")
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=(),
                         schema_wait=(0, 0))
    # It never became ACTIVE, so this is reported rather than pushed through.
    assert any("ACTIVE" in e or "active" in e for e in out["errors"])


# --- a schema still settling gets NOTHING posted into it ---------------------
#
# Found on review, reproduced with the real deploy_catalog. The non-ACTIVE
# branch after the schema wait only appended to out["errors"] and then fell
# through to the create loop: every table was POSTed into the settling
# schema -- exactly what the error text itself says gets creates accepted
# and then dropped. When the object then never appeared, the burned-name
# diagnosis ran after the schema had settled, its novel-name probe
# succeeded, and SOFT_CLONE_SUMMARY.md told the operator the names were
# burned, "confirmed for this run ... neither the request nor the catalog is
# at fault", and to change the target schema. The real cause -- a schema
# slower than the ~18 s wait -- was only in deploy_result.json; the summary
# never rendered out["errors"]. A longer wait verified every table.

class SettlesLate(Folding):
    """The schema reports CREATING for `creating_lists` listings, then
    ACTIVE. A create POSTed while it is CREATING is accepted and dropped.
    With `settle_on_first_create`, it settles right after the first such
    create -- the review's timing: the planned table is dropped, and the
    diagnosis probe that follows lands in a schema that is ACTIVE by then."""

    def __init__(self, creating_lists, *, settle_on_first_create=False):
        super().__init__()
        self.schemas["lake.db"] = "lake.db"
        self.creating_lists = creating_lists
        self.active = False
        self.settle_on_first_create = settle_on_first_create

    def __call__(self, operation, **kw):
        if operation == "list_schemas":
            self.ops.append((operation, kw))
            if self.creating_lists > 0 and not self.active:
                self.creating_lists -= 1
                state = "CREATING"
            else:
                self.active, state = True, "ACTIVE"
            return {"items": [{"key": "lake.db", "lifecycleState": state}]}
        if operation in ("create_table", "create_view") and not self.active:
            self.ops.append((operation, kw))
            if self.settle_on_first_create:
                self.active = True
            return {}
        return super().__call__(operation, **kw)


def test_nothing_is_posted_into_a_schema_that_never_became_active():
    s = SettlesLate(creating_lists=99, settle_on_first_create=True)
    out = deploy_catalog(_plan(2), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=(), schema_wait=(0, 0))
    assert not [op for op, _ in s.ops if op in ("create_table", "create_view")]
    assert out["poisoned_names"] == [], "the run's own timing is not a burn"
    assert out["diagnosis_probes"] == []
    assert out["executed"] == 0
    assert sorted(out["failed_targets"]) == ["DB.PUBLIC.T0", "DB.PUBLIC.T1"]
    for failure in out["failed"]:
        assert "CREATING" in failure["reason"]
        assert "nothing was posted" in failure["reason"]
        assert "re-run" in failure["reason"].lower()


def test_a_schema_that_settles_within_the_wait_is_deployed_normally():
    s = SettlesLate(creating_lists=2)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=(),
                         schema_wait=(0, 0, 0))
    assert any(op == "create_table" for op, _ in s.ops)
    assert out["failed"] == []
    assert out["verified"] == 1


def test_the_soft_clone_summary_shows_the_schema_state_and_the_errors():
    from report.render import render_soft_clone_summary
    s = SettlesLate(creating_lists=99, settle_on_first_create=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=(), schema_wait=(0,))
    md = render_soft_clone_summary({}, out)
    assert "lake.DB" in md and "CREATING" in md
    assert "These names are burned" not in md
    assert "confirmed for this run" not in md
    for error in out["errors"]:
        assert error[:60] in md, error


# ==========================================================================
# The LIST response omits tableFields; GET-by-key includes them.
#
# Verified live: six tables were created correctly as managed DELTA with the
# right field types, and the plugin called all six a MISMATCH because it
# compared the plan against a list entry that carries no fields at all.
# Existence comes from the list (which is how the real key case is found);
# STRUCTURE has to come from a GET on that key.
# ==========================================================================

class Summarised(Folding):
    """List returns summaries with no fields; GET returns the full object."""

    def __call__(self, operation, **kw):
        if operation in ("list_tables_in", "list_views_in"):
            self.ops.append((operation, kw))
            return {"items": [{"key": k} for k in self.tables]}   # no fields
        return super().__call__(operation, **kw)


def test_structure_is_read_with_a_get_not_from_the_list():
    s = Summarised()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=s,
                         retry_delays=(), verify_delays=())
    assert out["verified"] == 1, out["mismatches"] + out["failed"]
    assert any(op == "get_table" for op, _ in s.ops), \
        "the list carries no fields, so structure needs a GET"


def test_a_get_that_fails_leaves_the_structure_unverified_not_mismatched():
    class NoGet(Summarised):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                raise RuntimeError("DESCRIBE unavailable")
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=NoGet(),
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == []
    assert out["unverified_structure_targets"] == ["DB.PUBLIC.T0"]


def test_a_full_type_string_from_the_server_still_compares_equal():
    # The server returns fieldType "decimal(38,0)" AND fieldPrecision 38;
    # the plan says fieldType "decimal" with precision "38". Same type.
    class FullType(Summarised):
        def __call__(self, operation, **kw):
            if operation in ("get_table", "get_view"):
                self.ops.append((operation, kw))
                return {"key": "x", "tableFields": [
                    {"fieldName": "a", "fieldType": "string",
                     "fieldPrecision": 20, "fieldScale": 0}]}
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=FullType(),
                         retry_delays=(), verify_delays=())
    assert out["verified"] == 1, out["mismatches"]


# ==========================================================================
# A VIEW's column types are DERIVED by the target engine, not declared by us.
#
# Verified live: ACME_ORDER_360_VW was created correctly with all 17 columns,
# and three aggregate columns came back with different types --
#   ITEM_COUNT           decimal(18,0) -> bigint
#   TOTAL_ITEM_QUANTITY  decimal(22,0) -> decimal(20,0)
#   ITEM_TOTAL_AMOUNT    decimal(30,2) -> decimal(28,2)
# because Snowflake reports a view's DECLARED output types while AIDP
# re-derives them from the SQL.
#
# That is a fidelity finding worth reporting loudly -- decimal(22,0) to
# decimal(20,0) is a narrowing -- but it is NOT "someone else's object that we
# left alone", which is what a table mismatch means. The two must not be
# reported as the same thing.
# ==========================================================================

def _view_plan(found_types):
    return {"statements": [
        {"source_identifier": "DB.PUBLIC.V", "object_type": "VIEW",
         "target_fqn": "lake.DB.V", "sql": "", "view_text": "select 1 as a",
         "expected_columns": [{"name": "N", "type": "DECIMAL(22,0)"}]}],
        "blocked": []}, found_types


class DerivedTypes(Folding):
    def __init__(self, found):
        super().__init__()
        self.found = found

    def __call__(self, operation, **kw):
        if operation in ("get_table", "get_view"):
            self.ops.append((operation, kw))
            return {"key": "v", "viewFields": self.found,
                    "tableFields": self.found}
        return super().__call__(operation, **kw)


def test_a_view_whose_types_were_re_derived_is_not_called_uncloned():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == [], \
        "the view WAS created; it is not someone else's object"
    assert out["derived_type_drift_targets"] == ["DB.PUBLIC.V"]
    drift = out["derived_type_drift"][0]
    assert "derive" in drift["reason"].lower()
    assert "NOT been cloned" not in drift["reason"]


def test_the_drift_names_the_columns_and_both_types():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["derived_type_drift"][0]["reason"]
    assert "N" in reason and "DECIMAL(22,0)" in reason.upper()
    assert "DECIMAL(20,0)" in reason.upper()


def test_a_narrowing_is_called_out_as_an_overflow_risk():
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "n", "fieldType": "decimal(20,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert "narrow" in out["derived_type_drift"][0]["reason"].lower()


def test_a_view_with_missing_or_extra_columns_is_still_a_mismatch():
    # Column DRIFT is derivation. A different column LIST is not.
    plan, _ = _view_plan(None)
    call = DerivedTypes([{"fieldName": "somethingelse",
                          "fieldType": "decimal(22,0)"}])
    out = deploy_catalog(plan, target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.V"]
    assert out["derived_type_drift_targets"] == []


def test_a_table_type_difference_is_still_a_hard_mismatch():
    # Tables are created FROM our field list, so a type difference there means
    # the object is not ours.
    call = DerivedTypes([{"fieldName": "A", "fieldType": "int"}])
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["mismatched_targets"] == ["DB.PUBLIC.T0"]
    assert out["derived_type_drift_targets"] == []


# ==========================================================================
# Tell the user WHY nothing appeared (P1).
#
# A failed async create permanently poisons that name in that schema: every
# later create returns 202 and is silently dropped, and DELETE does not
# recover it. The signature is distinguishable -- a NOVEL name in the same
# schema succeeds -- so the plugin probes once and says which situation the
# user is in, instead of leaving them hunting a body problem that is not
# there.
# ==========================================================================

class NeverAppears(Folding):
    """Creates are accepted; the planned name never shows up.

    `probe_works` decides whether a novel name in the same schema succeeds,
    which is exactly what separates a poisoned name from a broken request.
    """

    def __init__(self, *, probe_works=True):
        super().__init__()
        self.probe_works = probe_works

    def __call__(self, operation, **kw):
        if operation == "create_table":
            name = kw.get("table") or ""
            self.ops.append((operation, kw))
            if name.startswith("snowmig_probe_") and self.probe_works:
                key = f'{kw["catalog"]}.{kw["schema"]}.{name}'.lower()
                # The real API returns the FULL key, not a bare name.
                self.tables[key] = {"key": key, "tableFields": []}
            return {}
        return super().__call__(operation, **kw)


def test_a_poisoned_name_is_named_as_such():
    call = NeverAppears(probe_works=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "cannot be reused" in reason.lower()
    assert "fresh schema" in reason.lower() or "new schema" in reason.lower()
    assert out["poisoned_names"] == ["lake.DB.T0"]


def test_when_a_novel_name_also_fails_the_diagnosis_is_different():
    call = NeverAppears(probe_works=False)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "poison" not in reason.lower()
    assert out["poisoned_names"] == []
    # A novel name failing too points at the request or the permissions.
    assert "request" in reason.lower() or "permission" in reason.lower()


def test_the_probe_uses_a_unique_name_and_is_cleaned_up():
    call = NeverAppears(probe_works=True)
    deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                   retry_delays=(), verify_delays=())
    probes = [kw.get("table") for op, kw in call.ops
              if op == "create_table" and str(kw.get("table", "")).startswith(
                  "snowmig_probe_")]
    assert len(probes) == 1
    assert len(set(probes)) == 1
    # It must delete what it made -- and a fixed name would itself get burned.
    assert any(op == "delete_table" for op, _ in call.ops)


def test_the_probe_runs_once_per_schema_not_once_per_object():
    call = NeverAppears(probe_works=True)
    deploy_catalog(_plan(4), target=TARGET, execute=True, call=call,
                   retry_delays=(), verify_delays=())
    probes = [kw for op, kw in call.ops
              if op == "create_table" and str(kw.get("table", "")).startswith(
                  "snowmig_probe_")]
    assert len(probes) == 1, "one diagnosis per schema is enough"
    assert len(call.ops) < 40, "the diagnosis must not multiply the work"


def test_the_probe_can_be_disabled():
    call = NeverAppears(probe_works=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=(), diagnose=False)
    assert not any(str(kw.get("table", "")).startswith("snowmig_probe_")
                   for _, kw in call.ops)
    assert out["poisoned_names"] == []


def test_a_successful_run_never_probes():
    f = Folding()
    deploy_catalog(_plan(2), target=TARGET, execute=True, call=f,
                   retry_delays=(), verify_delays=())
    assert not any(str(kw.get("table", "")).startswith("snowmig_probe_")
                   for _, kw in f.ops), "nothing failed, so nothing to diagnose"


def test_the_diagnosis_probe_is_always_named_in_the_result():
    """Deletes are asynchronous too, so cleanup is best-effort.

    The probe object must therefore be NAMED whether or not the delete took,
    so it is never silently abandoned in a customer's catalog.
    """
    call = NeverAppears(probe_works=True)
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"], "the probe object must be reported"
    probe = out["diagnosis_probes"][0]
    assert probe["name"].startswith("snowmig_probe_")
    # The SERVER's key, which is folded -- that is the one to go look for.
    assert probe["schema"] == "lake.db"
    assert "deleted" in probe


# ==========================================================================
# The target catalog's TYPE gates the whole deploy.
#
# An EXTERNAL catalog is a registered, read-only pointer at the live Snowflake
# source. It cannot hold the managed Delta this deploy creates -- the
# control-plane creates would return 202 Accepted and silently produce nothing
# -- and since 0.16.0 EXTERNAL is the DEFAULT catalog type, so nothing
# stopping `deploy --execute` from targeting one was a live footgun, not a
# theoretical one.
# ==========================================================================

class ExternalCatalog(Folding):
    def __call__(self, operation, **kw):
        if operation == "list_catalogs":
            self.ops.append((operation, kw))
            return {"items": [{"displayName": "lake",
                               "catalogType": "EXTERNAL"}]}
        return super().__call__(operation, **kw)


def test_deploy_refuses_an_external_catalog_before_any_create():
    call = ExternalCatalog()
    with pytest.raises(RefusedToExecute, match="EXTERNAL"):
        deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                       retry_delays=(), verify_delays=())
    assert [op for op, _ in call.ops] == ["list_catalogs"], \
        "the refusal must come before anything is created or even listed"


def test_deploy_refuses_when_the_target_catalog_does_not_exist():
    class NoCatalogs(Folding):
        def __call__(self, operation, **kw):
            if operation == "list_catalogs":
                return {"items": []}
            return super().__call__(operation, **kw)

    with pytest.raises(RefusedToExecute, match="does not exist"):
        deploy_catalog(_plan(1), target=TARGET, execute=True,
                       call=NoCatalogs(), retry_delays=(), verify_delays=())


def test_deploy_refuses_when_the_catalog_type_cannot_be_read():
    # "Could not look" and "safe to write" are different claims: a transient
    # listing failure must not fall through to the creates.
    class BrokenList(Folding):
        def __call__(self, operation, **kw):
            if operation == "list_catalogs":
                raise RuntimeError("503 service unavailable")
            return super().__call__(operation, **kw)

    with pytest.raises(RefusedToExecute, match="could not read"):
        deploy_catalog(_plan(1), target=TARGET, execute=True,
                       call=BrokenList(), retry_delays=(), verify_delays=())


def test_the_catalog_is_matched_case_insensitively_for_the_guard():
    class UpperCased(Folding):
        def __call__(self, operation, **kw):
            if operation == "list_catalogs":
                return {"items": [{"displayName": "LAKE",
                                   "catalogType": "EXTERNAL"}]}
            return super().__call__(operation, **kw)

    with pytest.raises(RefusedToExecute, match="EXTERNAL"):
        deploy_catalog(_plan(1), target=TARGET, execute=True,
                       call=UpperCased(), retry_delays=(), verify_delays=())


def test_the_resolved_catalog_type_is_recorded_in_the_result():
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Folding(),
                         retry_delays=(), verify_delays=())
    assert out["catalog_type"] == "STANDARD"


def test_a_dry_run_never_asks_for_the_catalog_type():
    rec = Recorder()
    out = deploy_catalog(_plan(1), target=TARGET, execute=False, call=rec,
                         retry_delays=(), verify_delays=())
    assert rec.ops == []
    assert out["catalog_type"] is None


# ==========================================================================
# A listing that FAILS is not a listing that came back empty.
#
# `_find_schema` and `_resolve_object` used to swallow every exception from
# the list call and return None, so a 403 on list_schemas read as "absent"
# and the schema was re-POSTed -- the exact write the module docstring says
# drops the table creates that follow -- while a 403 on list_tables_in made
# every table that WAS created report "never appeared", burned 33 s of
# polling per table, and left a diagnosis probe behind in the customer's
# schema because its own read-back was blind too. None of it recorded the
# 403 anywhere.
# ==========================================================================

class DeniedList(Folding):
    """A principal that may create but not list. `deny` names the operations
    that raise, the way `oci raw-request` does on a 403."""

    def __init__(self, *deny, **kw):
        super().__init__(**kw)
        self.deny = set(deny)

    def __call__(self, operation, **kw):
        if operation in self.deny:
            self.ops.append((operation, kw))
            raise RuntimeError(
                f"{operation} failed (exit 1): 403 NotAuthorizedOrNotFound")
        return super().__call__(operation, **kw)


def test_a_failed_schema_listing_refuses_before_any_write():
    call = DeniedList("list_schemas")
    with pytest.raises(RefusedToExecute, match="could not list"):
        deploy_catalog(_plan(2), target=TARGET, execute=True, call=call,
                       retry_delays=(), verify_delays=())
    assert not any(op in ("create_schema", "create_table", "create_view")
                   for op, _ in call.ops), "nothing may be written blind"


def test_an_existing_schema_is_never_re_posted_when_listing_fails():
    call = DeniedList("list_schemas")
    call.schemas["lake.db"] = "lake.db"     # it is already there
    with pytest.raises(RefusedToExecute):
        deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                       retry_delays=(), verify_delays=())
    assert [op for op, _ in call.ops].count("create_schema") == 0, \
        "a schema that cannot be listed is not known to be absent"


def test_a_failed_table_listing_is_reported_as_unknown_not_never_appeared():
    call = DeniedList("list_tables_in", "list_views_in")
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=())
    assert out["failed_targets"] == ["DB.PUBLIC.T0"]
    reason = out["failed"][0]["reason"]
    assert "could not be read back" in reason and "403" in reason
    assert "never appeared" not in reason
    assert "unsupported field type" not in reason
    # No probe: the schema cannot even be listed, so a probe would be
    # created blind and judged blind.
    assert not any(op == "create_table"
                   and str(kw.get("table", "")).startswith("snowmig_probe_")
                   for op, kw in call.ops)
    assert out["diagnosis_probes"] == []
    assert any("403" in e for e in out["errors"]), \
        "the actual error must be on the record"


def test_a_permission_error_on_listing_does_not_poll(monkeypatch):
    from target import catalog_deploy
    slept = []
    monkeypatch.setattr(catalog_deploy.time, "sleep", slept.append)
    call = DeniedList("list_tables_in")
    deploy_catalog(_plan(3), target=TARGET, execute=True, call=call,
                   retry_delays=(), verify_delays=(3, 5, 10, 15))
    assert [op for op, _ in call.ops].count("list_tables_in") == 3, \
        "one look per table: a 403 reads the same on every attempt"
    assert sum(slept) == 0


def test_a_transient_listing_error_is_polled_like_a_slow_create(monkeypatch):
    from target import catalog_deploy
    slept = []
    monkeypatch.setattr(catalog_deploy.time, "sleep", slept.append)

    class Flaky(Folding):
        def __init__(self):
            super().__init__()
            self.failures_left = 2

        def __call__(self, operation, **kw):
            if operation == "list_tables_in" and self.failures_left:
                self.failures_left -= 1
                self.ops.append((operation, kw))
                raise RuntimeError("backend returned 503 Service Unavailable")
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=Flaky(),
                         retry_delays=(), verify_delays=(3, 5, 10, 15))
    assert out["verified_targets"] == ["DB.PUBLIC.T0"]
    assert slept == [3, 5]


def test_a_schema_listing_that_fails_mid_poll_is_recorded_not_read_as_absent():
    class BlindAfterCreate(Folding):
        def __call__(self, operation, **kw):
            if operation == "list_schemas" and self.schemas:
                self.ops.append((operation, kw))
                raise RuntimeError("list_schemas failed (exit 1): 403 denied")
            return super().__call__(operation, **kw)

    call = BlindAfterCreate()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=call,
                         retry_delays=(), verify_delays=(), schema_wait=())
    assert out["schemas_created"] == ["lake.DB"]
    assert any("403" in e and "list" in e.lower() for e in out["errors"])


def test_diagnosis_probe_is_deleted_when_its_listing_fails():
    from target.catalog_deploy import _diagnose_never_appeared

    class ProbeBlind(Folding):
        def __call__(self, operation, **kw):
            if operation in ("list_tables_in", "list_views_in"):
                self.ops.append((operation, kw))
                raise RuntimeError("503 service unavailable")
            if operation == "delete_table":
                self.ops.append((operation, kw))
                return {}
            return super().__call__(operation, **kw)

    call = ProbeBlind()
    probes = []
    assert _diagnose_never_appeared(call, "lake", "lake.db", probes) is None
    deleted = [kw["table"] for op, kw in call.ops if op == "delete_table"]
    assert deleted == [probes[0]["name"]], \
        "never leave the probe behind because we could not look"
    assert probes[0]["created"] is None
    assert "503" in probes[0]["list_error"]


# ------------------------------------------- an execute that matches nothing
#
# Live 2026-09-22: `deploy --execute --catalog snowmig_coverage_internal` on a
# plan whose every object targets catalog `snowmig_coverage` created nothing,
# recorded `executed: 0, errors: []`, and exited 0. The report's only trace
# was the ordinary "Not deployed in this run" note. A caller reading the exit
# code would have called that a successful structural clone of 11 objects.

def _elsewhere(n=2):
    return {"statements": [
        {"source_identifier": f"DB.PUBLIC.T{i}", "object_type": "TABLE",
         "target_fqn": f"other_catalog.DB.T{i}", "sql": "CREATE TABLE ...",
         "expected_columns": [{"name": "A", "type": "STRING"}]}
        for i in range(n)], "blocked": []}


def test_a_plan_that_matches_no_statement_is_not_a_clean_empty_deploy():
    out = deploy_catalog(_elsewhere(2), target=TARGET, execute=True,
                         call=Recorder(), retry_delays=(), verify_delays=())
    assert out["matched_nothing"] is True
    assert out["out_of_scope_count"] == 2
    assert out["executed"] == 0


def test_an_empty_plan_is_not_a_run_that_matched_nothing():
    """Nothing to do and nothing matching are different facts: only the
    second means the operator named a catalog the plan never mentions."""
    out = deploy_catalog({"statements": [], "blocked": []}, target=TARGET,
                         execute=True, call=Recorder(), retry_delays=(),
                         verify_delays=())
    assert out["matched_nothing"] is False


def test_a_partial_match_is_not_a_run_that_matched_nothing():
    plan = _plan(1)
    plan["statements"] += _elsewhere(1)["statements"]
    out = deploy_catalog(plan, target=TARGET, execute=True, call=Recorder(),
                         retry_delays=(), verify_delays=())
    assert out["matched_nothing"] is False
    assert out["out_of_scope_count"] == 1


def test_the_report_says_the_catalog_name_is_what_did_not_match():
    from report.render import render_soft_clone_summary
    md = render_soft_clone_summary(
        {"can_migrate": [], "blocked": []},
        {"dry_run": False, "executed": 0, "verified": 0, "statements": [],
         "statement_count": 0, "blocked_count": 0, "errors": [],
         "catalog_in_scope": "wrong_name", "matched_nothing": True,
         "out_of_scope_count": 11,
         "out_of_scope_catalogs": ["snowmig_coverage"]})
    assert "wrong_name" in md
    assert "snowmig_coverage" in md
    assert "nothing was created" in md.lower()


# ------------------------- no property gap for an object that was not created
#
# Live 2026-09-22: `v_customer_totals` failed to create (the backend returned
# 500) and the run still reported its two NOT NULL columns as "created
# NULLABLE here". The gap is recorded before the create on purpose, because
# it is a property of this transport rather than a discovery -- but it may
# not survive into the artifact for an object that does not exist.

def _plan_not_null(fqn="lake.DB.T0", ident="DB.PUBLIC.T0"):
    return {"statements": [
        {"source_identifier": ident, "object_type": "TABLE",
         "target_fqn": fqn, "sql": "CREATE TABLE ...",
         "expected_columns": [{"name": "A", "type": "STRING",
                               "nullable": False}]}], "blocked": []}


def test_a_not_null_gap_is_reported_for_an_object_that_was_created():
    out = deploy_catalog(_plan_not_null(), target=TARGET, execute=True,
                         call=Recorder(), retry_delays=(), verify_delays=())
    assert out["verified"] == 1
    assert [p["property"] for p in out["properties_not_applied"]] == ["NOT NULL"]


def test_no_not_null_gap_is_reported_for_an_object_that_failed_to_create():
    out = deploy_catalog(_plan_not_null(), target=TARGET, execute=True,
                         call=Recorder(fail_on=("T0",)), retry_delays=(),
                         verify_delays=())
    assert out["failed"], "the create must have failed for this to mean anything"
    assert out["properties_not_applied"] == []
    assert out["properties_not_applied_targets"] == []


# ----------------------------------- a refused create is not a burned name
#
# Live 2026-09-22: AIDP answered one create with 400 `Invalid name` and four
# with 500 `InternalError`. Every one of them was then reported as "the
# create returned 202 Accepted ... a NOVEL name was created successfully, so
# the schema and your request are both fine and this NAME IS BURNED ... Retry
# into a FRESH SCHEMA". The create had raised, not returned 202; the request
# was demonstrably not fine, the target had said so; and a fresh schema
# would have produced the same 400 and the same 500.

class Refusing(Recorder):
    """A backend that rejects one create outright, as a real one does."""

    def __init__(self, *, refuse=(), message="400 Bad Request: Invalid name",
                 **kw):
        super().__init__(**kw)
        self.refuse = set(refuse)
        self.message = message

    def __call__(self, operation, **kw):
        name = kw.get("table") or kw.get("view") or kw.get("schema")
        if operation in ("create_table", "create_view") and name in self.refuse:
            raise RuntimeError(self.message)
        return super().__call__(operation, **kw)


def test_a_refused_create_reports_what_the_target_said():
    out = deploy_catalog(_plan(1), target=TARGET, execute=True,
                         call=Refusing(refuse=("T0",)), retry_delays=(),
                         verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "REFUSED" in reason
    assert "Invalid name" in reason, "the target's own answer has to be in it"
    assert "202" not in reason, "the create raised; it did not return 202"


def test_a_refused_create_is_not_called_a_burned_name():
    out = deploy_catalog(_plan(1), target=TARGET, execute=True,
                         call=Refusing(refuse=("T0",)), retry_delays=(),
                         verify_delays=())
    reason = out["failed"][0]["reason"].lower()
    assert "cannot be reused" not in reason
    assert "fresh schema" not in reason or "would not help" in reason
    assert out["poisoned_names"] == []


def test_a_refused_create_does_not_claim_the_request_was_fine():
    out = deploy_catalog(_plan(1), target=TARGET, execute=True,
                         call=Refusing(refuse=("T0",),
                                       message="500 Server Error: InternalError"),
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"].lower()
    assert "your request are both fine" not in reason
    assert "internalerror" in reason


def test_a_refused_create_runs_no_diagnosis_probe():
    """The probe exists to tell a burned name from a bad request. The target
    already answered that question for this object, so the probe is a write
    to the customer's catalog for nothing."""
    rec = Refusing(refuse=("T0",))
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec,
                         retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"] == []
    assert not [op for op, kw in rec.ops
                if op == "create_table"
                and str(kw.get("table", "")).startswith("snowmig_probe")]


def test_an_accepted_create_that_vanishes_is_still_a_burned_name():
    """The inference is sound where it belongs: the target took the create,
    reported nothing, and the object never appeared."""
    class Vanishing(Recorder):
        def __call__(self, operation, **kw):
            name = kw.get("table") or kw.get("view")
            if operation == "create_table" and name == "T0":
                return {"key": "accepted"}      # 202, and nothing created
            return super().__call__(operation, **kw)

    out = deploy_catalog(_plan(1), target=TARGET, execute=True,
                         call=Vanishing(), retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "was accepted" in reason
    assert "CANNOT BE REUSED" in reason
    assert out["poisoned_names"] == ["lake.DB.T0"]


# ------------------------------- the diagnosis probe must be of the kind
#                                 that failed
#
# Live 2026-09-22: four `create view` calls failed on a DataLake where every
# view create returned 500. The probe created a TABLE, it landed, and the
# verdict "a NOVEL name in this schema was created successfully, so the
# schema and your request are both fine" was applied to the views. A table
# landing says nothing about whether a view can be created here.

class ViewsVanish(Recorder):
    """Tables work. Views are accepted and never appear -- including the
    probe, which is what a catalog that cannot make views looks like."""

    def __call__(self, operation, **kw):
        if operation in ("create_view", "list_views_in", "delete_view"):
            self.ops.append((operation, kw))    # record, like the base does
            return {"items": []} if operation == "list_views_in" else {
                "key": "accepted"}
        return super().__call__(operation, **kw)


def _one_view_plan():
    return {"statements": [
        {"source_identifier": "DB.PUBLIC.V0", "object_type": "VIEW",
         "target_fqn": "lake.DB.V0", "sql": "CREATE VIEW ...",
         "view_text": "select 1 as a",
         "expected_columns": [{"name": "A", "type": "STRING"}]}],
        "blocked": []}


def test_a_failed_view_is_probed_with_a_view():
    rec = ViewsVanish()
    deploy_catalog(_one_view_plan(), target=TARGET, execute=True, call=rec,
                   retry_delays=(), verify_delays=())
    probes = [kw for op, kw in rec.ops if op == "create_view"
              and str(kw.get("view", "")).startswith("snowmig_probe")]
    assert probes, "the probe for a failed view has to be a view"
    assert not [kw for op, kw in rec.ops if op == "create_table"
                and str(kw.get("table", "")).startswith("snowmig_probe")]


def test_a_catalog_that_cannot_make_views_is_not_a_burned_name():
    out = deploy_catalog(_one_view_plan(), target=TARGET, execute=True,
                         call=ViewsVanish(), retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "CANNOT BE REUSED" not in reason, reason
    assert "novel view name" in reason.lower()
    assert out["poisoned_names"] == []


def test_the_probe_records_which_kind_it_was():
    out = deploy_catalog(_one_view_plan(), target=TARGET, execute=True,
                         call=ViewsVanish(), retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"][0]["kind"] == "VIEW"


def test_a_failed_table_is_still_probed_with_a_table():
    class TablesVanish(Recorder):
        def __call__(self, operation, **kw):
            if operation == "create_table" and kw.get("table") == "T0":
                return {"key": "accepted"}
            return super().__call__(operation, **kw)

    rec = TablesVanish()
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec,
                         retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"][0]["kind"] == "TABLE"
    assert "CANNOT BE REUSED" in out["failed"][0]["reason"]
    assert out["poisoned_names"] == ["lake.DB.T0"]


# ------------------------- a 4xx explains itself; a 5xx needs the probe
#
# Live 2026-09-22: `create table mixed case table` returned 400 with the rule
# it broke -- nothing to investigate. Four `create view` calls returned 500
# `InternalError` with no detail, where the operator's next move depends
# entirely on something the message does not say: was THIS view rejected, or
# can this catalog not create a view at all? On that DataLake it was the
# second, and the answer changes the plan from "fix the SQL" to "create the
# structure on compute instead".

def test_a_client_error_is_taken_at_its_word_and_costs_no_probe():
    rec = Refusing(refuse=("T0",),
                   message="400 Bad Request: Invalid name: mixed case table")
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec,
                         retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"] == []
    reason = out["failed"][0]["reason"]
    assert "Invalid name" in reason
    assert "named what was wrong" in reason


def test_a_server_error_is_probed_because_it_explains_nothing():
    rec = Refusing(refuse=("T0",), message="500 Server Error: InternalError")
    out = deploy_catalog(_plan(1), target=TARGET, execute=True, call=rec,
                         retry_delays=(), verify_delays=())
    assert out["diagnosis_probes"], "a 5xx leaves the real question open"
    assert out["diagnosis_probes"][0]["kind"] == "TABLE"


def test_a_server_error_with_a_working_probe_points_at_this_object():
    out = deploy_catalog(_plan(1), target=TARGET, execute=True,
                         call=Refusing(refuse=("T0",),
                                       message="500 Server Error: InternalError"),
                         retry_delays=(), verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "about THIS object" in reason
    assert "can still be used" in reason


def test_a_catalog_that_refuses_every_view_says_so_and_names_the_way_round():
    """The live case. Every create_view returns 500, including the probe."""
    class NoViewsAtAll(Recorder):
        def __call__(self, operation, **kw):
            if operation == "create_view":
                raise RuntimeError("500 Server Error: InternalError")
            if operation == "list_views_in":
                return {"items": []}
            return super().__call__(operation, **kw)

    out = deploy_catalog(_one_view_plan(), target=TARGET, execute=True,
                         call=NoViewsAtAll(), retry_delays=(),
                         verify_delays=())
    reason = out["failed"][0]["reason"]
    assert "cannot create a view through the catalog API at all" in reason
    assert "provision" in reason and "run" in reason
    assert "fresh schema" in reason.lower()
    assert out["poisoned_names"] == []
