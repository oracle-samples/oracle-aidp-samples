"""Security posture: what protects the data today, and what arrives without it.

This is the only gap with a data-EXPOSURE consequence. A Snowflake table whose
columns carry a masking policy lands on AIDP as plain Delta with no policy, so
values that were masked for most roles become readable to anyone who can read
the table. Nothing in the plugin said so.

Reports; changes nothing. AIDP's answer is restricted views plus ontology
sensitivity -- there is no masking API -- so an equivalent cannot be generated
here even in principle.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.security import build_security


def _inv():
    return {"databases_in_scope": ["DB"],
            "inventory": [
                {"source_identifier": "DB.PUBLIC.CUSTOMERS",
                 "object_type": "TABLE", "source_database": "DB",
                 "source_schema": "PUBLIC", "source_metadata": {}},
                {"source_identifier": "DB.PUBLIC.V_SECURE",
                 "object_type": "VIEW", "source_database": "DB",
                 "source_schema": "PUBLIC",
                 "source_metadata": {"is_secure": "true"}}]}


# Two different sources answer the same question and must not share a needle:
# SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES is account-wide and lags ~2 h, and
# <db>.INFORMATION_SCHEMA.POLICY_REFERENCES(...) is per object and does not.
_ACCOUNT_USAGE_NEEDLE = {
    "policy_references": "account_usage.policy_references",
    "tag_references": "account_usage.tag_references",
}


def _responses(**over):
    base = {
        "show masking policies": [],
        "show row access policies": [],
        "show aggregation policies": [],
        "show projection policies": [],
        "show tags": [],
        "information_schema.policy_references": [],
        "tag_references_all_columns": [],
        "account_usage.policy_references": [],
        "account_usage.tag_references": [],
        "grants_to_roles": [],
    }
    for key, rows in over.items():
        base[_ACCOUNT_USAGE_NEEDLE.get(key, key)] = rows
    return base


def _policy_ref(obj="CUSTOMERS", col="EMAIL", kind="MASKING_POLICY",
                name="PII_MASK"):
    return {"REF_DATABASE_NAME": "DB", "REF_SCHEMA_NAME": "PUBLIC",
            "REF_ENTITY_NAME": obj, "REF_COLUMN_NAME": col,
            "POLICY_KIND": kind, "POLICY_NAME": name}


# ------------------------------------------------------------------ exposure

def test_a_clean_estate_reports_no_exposure():
    # No secure view here: a secure view is itself a finding, so including one
    # would not be a clean estate.
    clean = {"databases_in_scope": ["DB"],
             "inventory": [{"source_identifier": "DB.PUBLIC.CUSTOMERS",
                            "object_type": "TABLE", "source_database": "DB",
                            "source_schema": "PUBLIC", "source_metadata": {}}]}
    s = build_security(FakeSql(_responses()), clean)
    assert s["exposures"] == []
    assert s["exposure_count"] == 0
    assert "no masking" in s["statement"].lower()


def test_a_masked_column_on_a_migrating_table_is_an_exposure():
    s = build_security(FakeSql(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert s["exposure_count"] == 1
    e = s["exposures"][0]
    assert e["object"] == "DB.PUBLIC.CUSTOMERS"
    assert e["column"] == "EMAIL"
    assert e["policy"] == "PII_MASK"
    assert e["severity"] == "HIGH"
    assert "unmask" in e["consequence"].lower() or "readable" in e["consequence"].lower()


def test_a_policy_on_an_object_outside_the_migration_is_not_an_exposure():
    # It is still reported as context, but it is not a regression we cause.
    s = build_security(FakeSql(_responses(
        policy_references=[_policy_ref(obj="NOT_MIGRATING")])), _inv())
    assert s["exposure_count"] == 0
    assert s["policy_references_out_of_scope"] == 1


def test_a_row_access_policy_is_an_exposure_at_row_level():
    s = build_security(FakeSql(_responses(policy_references=[
        _policy_ref(col=None, kind="ROW_ACCESS_POLICY", name="REGION_RLS")])),
        _inv())
    e = s["exposures"][0]
    assert e["policy_kind"] == "ROW_ACCESS_POLICY"
    assert e["column"] is None
    assert "row" in e["consequence"].lower()


def test_the_statement_is_loud_when_there_are_exposures():
    s = build_security(FakeSql(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert "1" in s["statement"]
    assert s["statement"].isupper() is False        # not shouting in caps
    assert "exposure" in s["statement"].lower() or "unprotected" in s["statement"].lower()


# ------------------------------------------------------------- secure views

def test_a_secure_view_is_flagged_because_secure_is_not_carried_over():
    s = build_security(FakeSql(_responses()), _inv())
    assert "DB.PUBLIC.V_SECURE" in [v["object"] for v in s["secure_views"]]
    assert s["secure_views"][0]["severity"] == "HIGH"


# ------------------------------------------------------------------- grants

def test_grants_are_summarised_per_object_when_readable():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "PUBLIC", "DATABASE_NAME": "DB",
         "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST", "GRANTS": 1}])),
        _inv())
    g = s["grants"]
    assert g["measured"] is True
    assert g["by_object"]["DB.PUBLIC.CUSTOMERS"][0]["role"] == "ANALYST"


def test_unreadable_grants_are_not_measured_rather_than_none_existing():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "grants_to_roles" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _inv())
    assert s["grants"]["measured"] is False
    assert s["grants"]["by_object"] == {}
    assert any("grant" in u.lower() for u in s["unreadable"])


def test_no_grants_are_ever_translated_into_target_ddl():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        {"NAME": "CUSTOMERS", "TABLE_SCHEMA": "PUBLIC", "DATABASE_NAME": "DB",
         "PRIVILEGE": "SELECT", "GRANTEE_NAME": "ANALYST", "GRANTS": 1}])),
        _inv())
    blob = repr(s).upper()
    assert "GRANT " not in blob, "reporting grants is not the same as replaying them"


# --------------------------------------------------------------- degradation

def test_one_denied_probe_does_not_lose_the_others():
    class Partial(FakeSql):
        def __call__(self, sql, params=None):
            if "show masking policies" in sql.lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    s = build_security(Partial(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert s["exposure_count"] == 1, "policy references still read"
    assert s["policies"]["masking"]["readable"] is False


def test_when_no_attachment_source_can_be_read_the_answer_is_unknown():
    # The dangerous case: we cannot prove there are no policies, so we must
    # not imply there are none. It takes BOTH sources failing to get here.
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "policy_references" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _inv())
    assert s["exposure_count"] is None
    assert "could not" in s["statement"].lower() or "unknown" in s["statement"].lower()
    assert "no masking" not in s["statement"].lower()


def test_the_per_object_read_answers_when_the_account_view_is_denied():
    """ACCOUNT_USAGE needs IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE.
    INFORMATION_SCHEMA does not, so a role without that grant is no longer
    left with UNKNOWN."""
    class DeniedAccountUsage(FakeSql):
        def __call__(self, sql, params=None):
            if "account_usage.policy_references" in sql.lower():
                raise RuntimeError("Insufficient privileges on ACCOUNT_USAGE")
            return super().__call__(sql, params)

    # One object: FakeSql answers by substring, so a two-object inventory
    # would hand the same canned row to both.
    s = build_security(DeniedAccountUsage(_responses(**{
        "information_schema.policy_references": [_live_policy()]})),
        _table_only_inv())
    assert s["exposure_count"] == 1, "the per-object read stands on its own"
    assert s["exposures"][0]["source"] == "live"
    assert "unknown" not in s["statement"].lower()


# ------------------------------------------------- defined, but not seen attached
#
# The only attachment source is ACCOUNT_USAGE.POLICY_REFERENCES, which lags
# up to ~2 hours. A policy attached inside that window is invisible there
# while SHOW MASKING/ROW ACCESS POLICIES already lists the policy object. An
# estate prepared shortly before the run is exactly this case, and the
# statement must not read "nothing is protected" on it.

def _table_only_inv():
    return {"databases_in_scope": ["DB"],
            "inventory": [{"source_identifier": "DB.PUBLIC.CUSTOMERS",
                           "object_type": "TABLE", "source_database": "DB",
                           "source_schema": "PUBLIC", "source_metadata": {}}]}


def _masking_policy(name="MASK_SSN"):
    return {"name": name, "database_name": "DB", "schema_name": "PUBLIC",
            "kind": "MASKING_POLICY"}


def test_a_defined_policy_with_no_visible_attachment_is_not_reported_clean():
    # With the per-object read unavailable (here: ruled out by budget), the
    # ~2 h-stale account view is the only source and cannot settle this.
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()]})), _table_only_inv(),
        live_attachment_budget=0)
    assert s["exposure_count"] == 0, "still an int: nothing was SEEN attached"
    assert s["policies_defined_without_attachment"] == 1
    st = s["statement"].lower()
    assert "no masking" not in st
    assert "unconfirmed" in st
    assert "lag" in st
    assert "nothing is protected" not in st


def test_a_defined_row_access_policy_triggers_the_same_hedge():
    s = build_security(FakeSql(_responses(**{
        "show row access policies": [{"name": "RAP_REGION", "database_name": "DB",
                                      "schema_name": "PUBLIC",
                                      "kind": "ROW_ACCESS_POLICY"}]})),
        _table_only_inv(), live_attachment_budget=0)
    assert s["policies_defined_without_attachment"] == 1
    assert "unconfirmed" in s["statement"].lower()


def test_the_clean_statement_names_the_source_and_its_lag():
    s = build_security(FakeSql(_responses()), _table_only_inv(),
                       live_attachment_budget=0)
    st = s["statement"].lower()
    assert "no masking" in st
    assert "policy_references" in st and "lag" in st, \
        "a clean verdict says where it looked and how stale that can be"
    assert s["policies_defined_without_attachment"] == 0
    assert "lag" in s["attachment_source"].lower()


def test_a_policy_attached_out_of_scope_does_not_trigger_the_hedge():
    # The defined policy IS accounted for: it is attached to something we are
    # not migrating, so the empty in-scope list is corroborated.
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()],
        "policy_references": [_policy_ref(obj="NOT_MIGRATING")]})),
        _table_only_inv())
    assert s["exposure_count"] == 0
    assert s["policy_references_out_of_scope"] == 1
    assert s["policies_defined_without_attachment"] == 0
    assert "unconfirmed" not in s["statement"].lower()


def test_unenumerable_policy_objects_cannot_corroborate_an_empty_attachment_list():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show masking policies" in sql.lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _table_only_inv())
    assert s["exposure_count"] == 0
    st = s["statement"].lower()
    assert "no masking" not in st
    assert "could not be enumerated" in st or "cannot corroborate" in st


def test_zero_visible_policy_objects_are_noted_as_role_filtered():
    # SHOW MASKING / ROW ACCESS POLICIES lists only policies the role owns or
    # can APPLY. The account-wide verdict comes from ACCOUNT_USAGE; this table
    # is context, and its zero must say what it is.
    s = build_security(FakeSql(_responses()), _inv())
    for key in ("masking", "row_access"):
        info = s["policies"][key]
        assert info["count"] == 0 and info["readable"] is True
        assert "visible" in info["note"], info["note"]


# ------------------------------ aggregation and projection policy OBJECTS
#
# The worst governance blind spot in the tool: the report could state that no
# aggregation or projection policy is attached to anything being migrated
# while neither kind was ever enumerated, and the ~2 h POLICY_REFERENCES
# staleness tripwire summed only masking + row_access, so a defined-but-
# unattached aggregation policy could not raise the UNCONFIRMED verdict.

def _agg_policy(name="AGG_ONLY"):
    return {"name": name, "database_name": "DB", "schema_name": "PUBLIC",
            "kind": "AGGREGATION_POLICY"}


def _proj_policy(name="NO_PROJECT"):
    return {"name": name, "database_name": "DB", "schema_name": "PUBLIC",
            "kind": "PROJECTION_POLICY"}


def test_aggregation_and_projection_policy_objects_are_enumerated():
    sql = FakeSql(_responses(**{
        "show aggregation policies": [_agg_policy()],
        "show projection policies": [_proj_policy()]}))
    s = build_security(sql, _table_only_inv())
    assert s["policies"]["aggregation"]["count"] == 1
    assert s["policies"]["projection"]["count"] == 1
    issued = " | ".join(c.lower() for c in sql.calls)
    assert "show aggregation policies in database" in issued
    assert "show projection policies in database" in issued


def test_a_defined_aggregation_policy_with_no_attachment_is_not_clean():
    s = build_security(FakeSql(_responses(**{
        "show aggregation policies": [_agg_policy()]})), _table_only_inv(),
        live_attachment_budget=0)
    assert s["policies_defined_without_attachment"] == 1, \
        "the staleness tripwire must cover aggregation policies too"
    st = s["statement"].lower()
    assert "unconfirmed" in st
    assert "no masking" not in st


def test_a_defined_projection_policy_with_no_attachment_is_not_clean():
    s = build_security(FakeSql(_responses(**{
        "show projection policies": [_proj_policy()]})), _table_only_inv(),
        live_attachment_budget=0)
    assert s["policies_defined_without_attachment"] == 1
    assert "unconfirmed" in s["statement"].lower()


def test_an_unreadable_aggregation_show_is_not_visible_rather_than_zero():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show aggregation policies" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _table_only_inv())
    info = s["policies"]["aggregation"]
    assert info["readable"] is False
    assert info["count"] is None, "could-not-look is not 0"
    assert any("aggregation" in u.lower() for u in s["unreadable"])


def test_zero_visible_aggregation_and_projection_policies_are_role_filtered():
    s = build_security(FakeSql(_responses()), _inv())
    for key in ("aggregation", "projection"):
        info = s["policies"][key]
        assert info["count"] == 0 and info["readable"] is True
        assert "visible" in info["note"], info["note"]


# ----------------------------------------------------------- tag attachments
#
# Tag OBJECTS were counted and no attachment was ever read, so a
# classification-driven governance model rendered as empty. A tag count with
# no attachment list is a number with no verdict.

def _tag_ref(obj="CUSTOMERS", col="EMAIL", tag="PII", value="EMAIL",
             domain="COLUMN"):
    return {"TAG_DATABASE": "DB", "TAG_SCHEMA": "PUBLIC", "TAG_NAME": tag,
            "TAG_VALUE": value, "OBJECT_DATABASE": "DB",
            "OBJECT_SCHEMA": "PUBLIC", "OBJECT_NAME": obj,
            "COLUMN_NAME": col, "DOMAIN": domain}


def test_tag_attachments_on_migrated_objects_are_read():
    s = build_security(FakeSql(_responses(tag_references=[_tag_ref()])), _inv())
    t = s["tag_references"]
    assert t["measured"] is True
    a = t["attachments"][0]
    assert a["object"] == "DB.PUBLIC.CUSTOMERS"
    assert a["column"] == "EMAIL"
    assert a["tag"] == "DB.PUBLIC.PII"
    assert a["value"] == "EMAIL"
    assert a["consequence"] and a["aidp_path"]


def test_a_tag_attached_outside_the_migration_is_context_not_a_finding():
    s = build_security(FakeSql(_responses(
        tag_references=[_tag_ref(obj="NOT_MIGRATING")])), _inv())
    assert s["tag_references"]["attachments"] == []
    assert s["tag_references"]["out_of_scope"] == 1


def test_unreadable_tag_references_are_not_measured_rather_than_zero():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "tag_references" in sql.lower():
                raise RuntimeError("Insufficient privileges on ACCOUNT_USAGE")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _inv())
    t = s["tag_references"]
    assert t["measured"] is False
    assert t["count"] is None, "could-not-look is not 0"
    assert any("tag_references" in u.lower() for u in s["unreadable"])


def test_the_tag_attachment_source_carries_its_latency_caveat():
    s = build_security(FakeSql(_responses()), _inv())
    src = s["tag_references"]["source"].lower()
    assert "tag_references" in src and "lag" in src


def test_tag_attachments_are_never_replayed_on_the_target():
    s = build_security(FakeSql(_responses(tag_references=[_tag_ref()])), _inv())
    assert s["tag_references"]["carried_over"] is False


# ------------------------------------------------------------------- grants
#
# Grants were read for TABLE / VIEW / MATERIALIZED_VIEW only, so every grant
# on a schema, database, warehouse, stage, procedure or function was
# invisible and the target access model could not be reconstructed.

def _grant(name="CUSTOMERS", schema="PUBLIC", db="DB", granted_on="TABLE",
           role="ANALYST", priv="SELECT"):
    return {"NAME": name, "TABLE_SCHEMA": schema, "DATABASE_NAME": db,
            "GRANTED_ON": granted_on, "PRIVILEGE": priv,
            "GRANTEE_NAME": role, "GRANTS": 1}


def test_grants_are_read_beyond_tables_and_views():
    sql = FakeSql(_responses(grants_to_roles=[
        _grant(),
        _grant(name="PUBLIC", schema=None, granted_on="SCHEMA",
               priv="USAGE", role="LOADER"),
        _grant(name="WH_ETL", schema=None, db=None, granted_on="WAREHOUSE",
               priv="USAGE", role="LOADER")]))
    s = build_security(sql, _inv())
    g = s["grants"]
    assert set(g["by_class"]) >= {"TABLE", "SCHEMA", "WAREHOUSE"}
    assert g["by_class"]["SCHEMA"]["roles"] == ["LOADER"]
    issued = " ".join(c.lower() for c in sql.calls)
    for cls in ("'schema'", "'database'", "'warehouse'", "'stage'",
                "'procedure'", "'function'"):
        assert cls in issued, cls


def test_the_grant_classes_that_were_asked_for_are_recorded():
    s = build_security(FakeSql(_responses(grants_to_roles=[_grant()])), _inv())
    requested = s["grants"]["classes_requested"]
    assert {"TABLE", "VIEW", "MATERIALIZED_VIEW", "SCHEMA", "DATABASE",
            "WAREHOUSE", "STAGE", "PROCEDURE", "FUNCTION"} <= set(requested)


def test_a_grant_on_a_database_outside_the_scope_is_not_counted_in_scope():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        _grant(name="OTHER", schema="S", db="OTHERDB", granted_on="SCHEMA")])),
        _inv())
    assert s["grants"]["by_class"] == {}
    assert s["grants"]["out_of_scope"] == 1


def test_widened_grants_are_still_never_replayed():
    s = build_security(FakeSql(_responses(grants_to_roles=[
        _grant(), _grant(name="PUBLIC", schema=None, granted_on="SCHEMA")])),
        _inv())
    assert s["grants"]["carried_over"] is False
    blob = repr(s).upper()
    assert "GRANT " not in blob, "reporting grants is not replaying them"


# ------------------------------------- the statement matches what was asked
#
# I3: "could not look" never renders as zero. The summary sentence must not
# name a policy kind whose enumeration failed or never ran without saying so.

def test_the_clean_sentence_names_exactly_the_kinds_that_were_enumerated():
    s = build_security(FakeSql(_responses()), _table_only_inv())
    st = s["statement"].lower()
    for kind in ("masking", "row-access", "aggregation", "projection"):
        assert kind in st, kind
    assert s["policy_kinds_unenumerated"] == []
    assert s["policy_kinds_enumerated"] == ["masking", "row-access",
                                            "aggregation", "projection"]


def test_the_statement_does_not_read_clean_when_one_kind_is_unreadable():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show projection policies" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses()), _table_only_inv())
    st = s["statement"].lower()
    assert ("no masking, row-access, aggregation or projection policy is "
            "attached") not in st, \
        "a clean verdict must not cover a kind that was never enumerated"
    assert "projection" in st and "could not be enumerated" in st
    assert s["policy_kinds_unenumerated"] == ["projection"]
    assert "projection" not in " ".join(s["policy_kinds_enumerated"])


def test_an_exposure_statement_still_flags_a_kind_that_could_not_be_read():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show aggregation policies" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses(policy_references=[_policy_ref()])),
                       _inv())
    st = s["statement"].lower()
    assert "exposure" in st
    assert "aggregation" in st and "could not be enumerated" in st


# ------------------------------------- the lag, and the read that does not lag
#
# Live 2026-09-22: a masking policy, a row-access policy and a tag were
# attached to a seeded table, and SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES /
# TAG_REFERENCES returned nothing -- both views lag up to ~2 hours. The report
# hedged correctly ("defined, not seen attached, treat as UNCONFIRMED") but the
# truth was readable the whole time: <db>.INFORMATION_SCHEMA.POLICY_REFERENCES
# and TAG_REFERENCES_ALL_COLUMNS answer per object with no lag, and need no
# IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE. So the per-object read is what
# decides, and the account view becomes corroboration.

def _live_policy(obj="CUSTOMERS", col="EMAIL", kind="MASKING_POLICY",
                 name="PII_MASK", schema="PUBLIC", db="DB"):
    return {"POLICY_DB": db, "POLICY_SCHEMA": schema, "POLICY_NAME": name,
            "POLICY_KIND": kind, "REF_DATABASE_NAME": db,
            "REF_SCHEMA_NAME": schema, "REF_ENTITY_NAME": obj,
            "REF_ENTITY_DOMAIN": "TABLE", "REF_COLUMN_NAME": col,
            "POLICY_STATUS": "ACTIVE"}


def _live_tag(obj="CUSTOMERS", col="EMAIL", name="PII", value="EMAIL",
              level="COLUMN", schema="PUBLIC", db="DB"):
    return {"TAG_DATABASE": db, "TAG_SCHEMA": schema, "TAG_NAME": name,
            "TAG_VALUE": value, "LEVEL": level, "OBJECT_DATABASE": db,
            "OBJECT_SCHEMA": schema, "OBJECT_NAME": obj, "DOMAIN": "TABLE",
            "COLUMN_NAME": col}


def test_an_attachment_the_lagging_view_has_not_caught_up_to_is_still_found():
    """The exact live failure: ACCOUNT_USAGE empty, the policy attached."""
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()],
        "information_schema.policy_references": [_live_policy()]})),
        _table_only_inv())
    assert s["exposure_count"] == 1, "the per-object read decides"
    assert s["exposures"][0]["object"] == "DB.PUBLIC.CUSTOMERS"
    assert s["exposures"][0]["column"] == "EMAIL"
    assert s["policies_defined_without_attachment"] == 0, \
        "the policy is accounted for: it is attached"
    st = s["statement"].lower()
    assert "unconfirmed" not in st, "nothing is unconfirmed once it was read"
    assert "exposure" in st


def test_the_per_object_read_is_what_the_report_names_as_its_source():
    s = build_security(FakeSql(_responses(**{
        "information_schema.policy_references": [_live_policy()]})), _inv())
    src = s["attachment_source"].lower()
    assert "information_schema" in src
    assert "per object" in src or "no lag" in src
    assert s["live_attachments"]["complete"] is True


def test_an_empty_per_object_read_corroborates_the_empty_account_view():
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()]})), _table_only_inv())
    assert s["exposure_count"] == 0
    assert s["policies_defined_without_attachment"] == 0, \
        "a defined policy attached to nothing was READ as attached to nothing"
    st = s["statement"].lower()
    assert "unconfirmed" not in st
    assert "no masking" in st or "not attached" in st


def test_a_refused_per_object_read_leaves_the_lagging_hedge_standing():
    """I3: the fallback is the old uncertainty, never a clean verdict."""
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "information_schema.policy_references" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    s = build_security(Denied(_responses(**{
        "show masking policies": [_masking_policy()]})), _table_only_inv())
    assert s["live_attachments"]["complete"] is False
    assert s["policies_defined_without_attachment"] == 1
    st = s["statement"].lower()
    assert "unconfirmed" in st and "lag" in st


def test_one_unreadable_object_does_not_discard_the_others():
    class PartlyDenied(FakeSql):
        def __call__(self, sql, params=None):
            if "V_SECURE" in sql:
                raise RuntimeError("does not exist or not authorized")
            return super().__call__(sql, params)

    s = build_security(PartlyDenied(_responses(**{
        "information_schema.policy_references": [_live_policy()]})), _inv())
    live = s["live_attachments"]
    assert live["probed"] == 1 and len(live["failed"]) == 1
    assert live["complete"] is False
    assert s["exposure_count"] == 1, "what was read is still reported"
    assert "V_SECURE" in s["statement"] or "1 object" in s["statement"].lower(), \
        "an object that could not be read has to be named, not dropped"


def test_a_tag_the_lagging_view_missed_is_still_reported():
    s = build_security(FakeSql(_responses(**{
        "tag_references_all_columns": [_live_tag()]})), _table_only_inv())
    tags = s["tag_references"]
    assert tags["measured"] is True
    assert tags["count"] == 1
    assert tags["attachments"][0]["column"] == "EMAIL"
    assert "information_schema" in tags["source"].lower()


def test_a_table_level_tag_is_not_reported_once_per_column():
    """Live: TAG_REFERENCES_ALL_COLUMNS repeats a table-level tag on every
    column, with LEVEL=TABLE. Four columns must not read as four findings."""
    rows = [_live_tag(col=c, name="SENSITIVITY", value="RESTRICTED",
                      level="TABLE")
            for c in ("EMPLOYEE_ID", "SSN", "SALARY", "REGION")]
    rows.append(_live_tag(col="SALARY", name="SENSITIVITY",
                          value="CONFIDENTIAL", level="COLUMN"))
    s = build_security(FakeSql(_responses(**{
        "tag_references_all_columns": rows})), _table_only_inv())
    attached = s["tag_references"]["attachments"]
    table_level = [a for a in attached if a.get("level") == "TABLE"]
    assert len(table_level) == 1, table_level
    assert table_level[0]["column"] is None, "a table tag has no column"
    assert len([a for a in attached if a.get("level") == "COLUMN"]) == 1


def test_the_per_object_read_asks_for_the_domain_the_object_has():
    """Live: POLICY_REFERENCES takes VIEW for a view; TAG_REFERENCES_ALL_COLUMNS
    rejects VIEW and wants 'table' for every table-like object."""
    fake = FakeSql(_responses())
    build_security(fake, _inv())
    pol = [c for c in fake.calls
           if "information_schema.policy_references" in c.lower()]
    tag = [c for c in fake.calls if "tag_references_all_columns" in c.lower()]
    assert any("'VIEW'" in c for c in pol), "the view is asked for as a VIEW"
    assert any("'TABLE'" in c for c in pol)
    assert tag and all("'table'" in c for c in tag), \
        "TAG_REFERENCES_ALL_COLUMNS refuses any domain but table"


def test_every_per_object_statement_passes_the_read_only_guard():
    from snowflake_source.conn import assert_read_only
    fake = FakeSql(_responses())
    build_security(fake, _inv())
    for call in fake.calls:
        assert_read_only(call)


def test_a_quoted_name_is_escaped_into_the_literal_the_function_wants():
    inv = {"databases_in_scope": ["DB"],
           "inventory": [{"source_identifier": 'DB.EDGE.Mixed "Case"',
                          "object_type": "TABLE", "source_database": "DB",
                          "source_schema": "EDGE", "source_metadata": {}}]}
    fake = FakeSql(_responses())
    build_security(fake, inv)
    call = next(c for c in fake.calls
                if "information_schema.policy_references" in c.lower())
    assert '"Mixed ""Case"""' in call, call


def test_an_estate_over_the_budget_is_not_probed_object_by_object():
    """A per-object read costs a round trip. Past the budget it is skipped and
    said to be skipped -- never quietly, and never as a clean verdict."""
    inv = {"databases_in_scope": ["DB"],
           "inventory": [{"source_identifier": f"DB.PUBLIC.T{i}",
                          "object_type": "TABLE", "source_database": "DB",
                          "source_schema": "PUBLIC", "source_metadata": {}}
                         for i in range(5)]}
    s = build_security(FakeSql(_responses(**{
        "show masking policies": [_masking_policy()]})), inv,
        live_attachment_budget=4)
    live = s["live_attachments"]
    assert live["attempted"] is False
    assert live["complete"] is False
    assert "budget" in live["reason"].lower() or "5" in live["reason"]
    assert s["policies_defined_without_attachment"] == 1, \
        "unprobed is unconfirmed, which is the pre-existing hedge"
    assert "unconfirmed" in s["statement"].lower()


def test_an_aggregation_policy_read_per_object_settles_the_same_hedge():
    """The tripwire fires on an unresolved question, not on a resolved one:
    once every in-scope object has been read, a defined-but-unattached
    aggregation policy is a fact rather than a maybe."""
    s = build_security(FakeSql(_responses(**{
        "show aggregation policies": [_agg_policy()]})), _table_only_inv())
    assert s["policies_defined_without_attachment"] == 0
    assert "unconfirmed" not in s["statement"].lower()
    assert s["live_attachments"]["policy_complete"] is True


def test_the_clean_statement_names_the_per_object_read_when_it_answered():
    s = build_security(FakeSql(_responses()), _table_only_inv())
    st = s["statement"]
    assert "INFORMATION_SCHEMA" in st
    assert "lag" in st.lower(), "it still says what it is NOT subject to"


def test_a_stale_account_row_the_live_read_contradicts_is_kept_and_flagged():
    """The lag runs both ways: the account view also keeps showing an
    attachment that has just been removed. Neither source may erase the
    other -- the row is kept, attributed, and marked for confirmation."""
    s = build_security(FakeSql(_responses(policy_references=[_policy_ref()])),
                       _inv())
    assert s["exposure_count"] == 1
    e = s["exposures"][0]
    assert e["source"] == "account_usage"
    assert "confirm" in (e.get("needs_confirmation") or "").lower()


# ------------------------------------------ the headline names the tags too
#
# Review 2026-09-25. _statement() never received tag_references, so on the
# commonest governance pattern -- PII-tagged columns with no masking policy
# -- the per-object read settled cleanly and the first line of SECURITY.md
# read "No masking ... policy and no tag is attached to anything being
# migrated", while the "## Tag attachments" table a few lines below listed
# GOV.TAGS.PII on CUSTOMERS.EMAIL. The report contradicted itself and stated
# a fact the run had just disproved.

def test_a_tag_attachment_is_named_in_the_headline_not_denied():
    s = build_security(FakeSql(_responses(**{
        "tag_references_all_columns": [_live_tag()]})), _table_only_inv())
    assert s["tag_references"]["count"] == 1
    st = s["statement"]
    assert "no tag is attached" not in st.lower(), st
    assert "1 tag attachment(s)" in st, st
    assert "do not travel" in st
    # A tag is not a stripped protection: the policy verdict still leads.
    assert st.lower().startswith("no masking"), st


def test_a_tag_seen_only_in_the_account_view_is_named_too():
    s = build_security(FakeSql(_responses(tag_references=[_tag_ref()])),
                       _table_only_inv(), live_attachment_budget=0)
    assert s["tag_references"]["count"] == 1
    assert "1 tag attachment(s)" in s["statement"], s["statement"]


def test_zero_tags_and_zero_policies_still_read_clean():
    s = build_security(FakeSql(_responses()), _table_only_inv())
    assert s["tag_references"]["count"] == 0
    assert "no tag is attached" in s["statement"].lower(), s["statement"]


def test_the_rendered_report_agrees_with_its_own_tag_table():
    from report.render import render_security
    md = render_security(build_security(FakeSql(_responses(**{
        "tag_references_all_columns": [_live_tag()]})), _table_only_inv()))
    head = md.split("\n## ", 1)[0]
    assert "no tag is attached" not in head.lower()
    assert "DB.PUBLIC.PII" in md
