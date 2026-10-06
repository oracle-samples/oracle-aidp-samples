"""A restriction that matches nothing must not read as one in force.

validate_restrictions checks keys, types and regexes, and its docstring
promises that a value is rejected rather than "applied as a no-op that the
report then lists as in force". But it never checked that a value matched
anything: a typo'd exclude_objects entry excluded nothing, the object was
planned, deployed and copied, and PLANNED_OBJECTS.md listed the entry
under "Restrictions in force / Applied at your request" while the CLI
exited 0.

Worse, exclude_databases / exclude_schemas upper-cased the entry with its
quote characters, so `["\\"sales_eu\\""]` -- the quoted form the
TargetCollision remedy teaches for exclude_objects -- could never match
anything: `{'exclude_databases': ['"sales_eu"']} -> kept [all 3]`, while
the unquoted form excluded.

Now every list entry carries a match count into plan.json, a zero is
flagged in the report, and database/schema entries follow the same
quoting rule as exclude_objects.
"""
from plan.build import build_plan
from plan.restrictions import apply_restrictions, restriction_matches
from report.render import render_planned_objects
from test_restrictions import rec

RECS = [rec("sales_eu.PUBLIC.ORDERS"), rec("SALES_US.PUBLIC.ORDERS"),
        rec("SALES_US.PUBLIC.ITEMS")]


def test_a_quoted_database_entry_matches_that_database_exactly():
    kept, excluded = apply_restrictions(RECS, {"exclude_databases": ['"sales_eu"']})
    assert [e["source_identifier"] for e in excluded] == ["sales_eu.PUBLIC.ORDERS"]
    assert len(kept) == 2


def test_a_quoted_entry_is_case_sensitive_and_an_unquoted_one_folds():
    _, excluded = apply_restrictions(RECS, {"exclude_databases": ['"SALES_EU"']})
    assert excluded == [], "quoted is exact: SALES_EU is not sales_eu"
    _, excluded = apply_restrictions(RECS, {"exclude_databases": ["sales_us"]})
    assert len(excluded) == 2


def test_a_quoted_schema_entry_matches_too():
    _, excluded = apply_restrictions(RECS, {"include_schemas": ['"PUBLIC"']})
    assert excluded == []


def test_every_list_entry_carries_a_match_count():
    counts = restriction_matches(RECS, {
        "exclude_objects": ["SALES_US.PUBLIC.TYPO", "SALES_US.PUBLIC.ITEMS"],
        "exclude_name_patterns": ["^ORD"], "max_rows": 100})
    assert counts == {"exclude_objects": {"SALES_US.PUBLIC.TYPO": 0,
                                          "SALES_US.PUBLIC.ITEMS": 1},
                      "exclude_name_patterns": {"^ORD": 2}}


def test_the_plan_records_and_the_report_flags_an_entry_that_matched_nothing():
    plan = build_plan({"inventory": RECS}, {"edges": []},
                      restrictions={"exclude_objects": ["SALES_US.PUBLIC.TYPO"]})
    assert plan["restriction_matches"] == {
        "exclude_objects": {"SALES_US.PUBLIC.TYPO": 0}}
    md = render_planned_objects(plan)
    section = md.split("## Restrictions in force", 1)[1].split("\n## ", 1)[0]
    line = next(l for l in section.splitlines() if "exclude_objects" in l)
    assert "SALES_US.PUBLIC.TYPO" in line
    assert "matched nothing" in line and "check the spelling" in line


def test_an_entry_that_matched_is_not_flagged():
    plan = build_plan({"inventory": RECS}, {"edges": []},
                      restrictions={"exclude_objects": ["SALES_US.PUBLIC.ITEMS"]})
    md = render_planned_objects(plan)
    assert "matched nothing" not in md
    assert "(1 object)" in md
