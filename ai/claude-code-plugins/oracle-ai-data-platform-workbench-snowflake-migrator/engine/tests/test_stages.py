"""The stage board: what is supposed to run, what has run, and what it found.

Asked for so the run can be understood before it is executed. Reads only the
artifacts already on disk -- it touches no environment and makes no decisions.
"""
import json

from report.stages import STAGES, build_stage_board
from report.render import render_stages


def _write(tmp, name, payload):
    (tmp / name).write_text(json.dumps(payload), encoding="utf-8")


def test_every_stage_is_listed_even_on_an_empty_directory(tmp_path):
    board = build_stage_board(tmp_path)
    assert [s["stage"] for s in board["stages"]] == [s["stage"] for s in STAGES]
    assert all(s["status"] == "NOT_RUN" for s in board["stages"])


def test_a_stage_with_its_artifact_reads_as_done(tmp_path):
    _write(tmp_path, "inventory.json",
           {"object_count": 7, "counts_by_type": {"TABLE": 6, "VIEW": 1},
            "extraction_notes": []})
    board = build_stage_board(tmp_path)
    assess = next(s for s in board["stages"] if s["stage"] == "assess")
    assert assess["status"] == "DONE"
    assert "7" in assess["found"]


def test_findings_are_summarised_per_stage(tmp_path):
    _write(tmp_path, "inventory.json", {"object_count": 7,
                                        "counts_by_type": {}, "extraction_notes": []})
    _write(tmp_path, "security.json", {"exposure_count": 2, "secure_views": [],
                                       "grants": {}})
    _write(tmp_path, "maintenance.json", {"objects_with_signals": 3,
                                          "tables": [1, 2, 3],
                                          "account_usage": {"readable": True}})
    board = build_stage_board(tmp_path)
    found = {s["stage"]: s["found"] for s in board["stages"]}
    assert "2" in found["security"]
    assert "3" in found["maintenance"]


def test_a_stage_that_found_a_problem_is_flagged(tmp_path):
    _write(tmp_path, "security.json", {"exposure_count": 2, "secure_views": [],
                                       "grants": {}})
    board = build_stage_board(tmp_path)
    sec = next(s for s in board["stages"] if s["stage"] == "security")
    assert sec["attention"] is True


def test_a_clean_stage_is_not_flagged(tmp_path):
    _write(tmp_path, "security.json", {"exposure_count": 0, "secure_views": [],
                                       "grants": {}})
    sec = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "security")
    assert sec["attention"] is False


def test_an_unreadable_probe_is_flagged_rather_than_read_as_clean(tmp_path):
    # "we could not look" must never present as "nothing found".
    _write(tmp_path, "security.json", {"exposure_count": None,
                                       "secure_views": [], "grants": {}})
    sec = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "security")
    assert sec["attention"] is True
    assert "unknown" in sec["found"].lower() or "not" in sec["found"].lower()


def test_the_board_says_which_stage_comes_next(tmp_path):
    _write(tmp_path, "inventory.json", {"object_count": 1, "counts_by_type": {},
                                        "extraction_notes": []})
    board = build_stage_board(tmp_path)
    assert board["next_stage"] == "deps"


def test_a_write_stage_is_marked_as_writing(tmp_path):
    deploy = next(s for s in STAGES if s["stage"] == "deploy")
    assert deploy["writes"] is True
    assess = next(s for s in STAGES if s["stage"] == "assess")
    assert assess["writes"] is False


def test_the_rendered_board_is_a_table_with_every_stage(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    assert "| Stage |" in md
    for s in STAGES:
        assert s["stage"] in md


def test_the_rendered_board_marks_every_writing_stage(tmp_path):
    # The one-writer claim once made the (then-new) default stage invisible;
    # the writer set is now pinned so a new writer cannot ship unlisted.
    md = render_stages(build_stage_board(tmp_path))
    assert "writes" in md.lower()
    assert "read-only" in md.lower()
    assert "`provision`" in md and "`catalog`" in md and "`deploy`" in md
    assert "`run`" in md
    writers = {s["stage"] for s in STAGES if s["writes"]}
    assert writers == {"provision", "catalog", "deploy",
                       "structure-workflow", "copy-workflow", "publish",
                       "teardown"}
    assert "`publish`" in md
    assert "`structure-workflow`" in md and "`copy-workflow`" in md


def test_a_provision_run_with_failures_is_flagged(tmp_path):
    _write(tmp_path, "provision_result.json",
           {"dry_run": False, "workspace": {"name": "acme_prod"},
            "steps": [{"step": "workspace", "action": "reused",
                       "verified": True, "detail": ""},
                      {"step": "upload", "action": "failed",
                       "verified": False, "detail": "boom"}]})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "provision")
    assert row["attention"] is True
    assert "1 failed" in row["found"]


def test_the_catalog_stage_is_on_the_board_and_reads_its_artifact(tmp_path):
    cat = next(s for s in STAGES if s["stage"] == "catalog")
    assert cat["writes"] is True
    assert cat["artifact"] == "catalog_result.json"
    _write(tmp_path, "catalog_result.json",
           {"dry_run": False, "catalog": "lake", "catalog_type": "EXTERNAL",
            "action": "created", "verified": True})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "catalog")
    assert row["status"] == "DONE"
    assert "created" in row["found"]
    assert row["attention"] is False


def test_a_catalog_registration_that_stayed_pending_is_flagged(tmp_path):
    # 202 Accepted is not the claim: a create that never became visible must
    # not read as success on the board.
    _write(tmp_path, "catalog_result.json",
           {"dry_run": False, "catalog": "lake", "catalog_type": "EXTERNAL",
            "action": "create_requested", "verified": False})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "catalog")
    assert row["attention"] is True
    assert "never became visible" in row["found"]


def test_an_optional_stage_is_never_proposed_as_next(tmp_path):
    # data-options feeds `plan` when run, but `plan` runs without it, so the
    # board must not stall on it as "next".
    _write(tmp_path, "inventory.json",
           {"object_count": 1, "counts_by_type": {}, "extraction_notes": []})
    _write(tmp_path, "dependencies.json", {"source_used": "x", "cycles": []})
    _write(tmp_path, "maintenance.json",
           {"objects_with_signals": 0, "tables": [],
            "account_usage": {"readable": True}})
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {}})
    _write(tmp_path, "compute.json", {"proposals": []})
    board = build_stage_board(tmp_path)
    assert board["next_stage"] == "plan"


def test_a_smoke_run_that_skipped_the_destination_is_flagged(tmp_path):
    # Only the source was checked. That is not a pass, and the board must
    # not show the row as clean.
    _write(tmp_path, "smoke.json",
           {"ok": True, "complete": False, "verdict": "PARTIAL",
            "source": {"reachable": True, "checks": []},
            "destination": {"skipped": True, "checks": []}})
    board = build_stage_board(tmp_path)
    row = next(s for s in board["stages"] if s["stage"] == "smoke")
    assert "PARTIAL" in row["found"]
    assert row["attention"] is True
    assert "smoke" in board["needs_attention"]


def test_a_legacy_smoke_result_with_a_skipped_destination_is_not_shown_clean(tmp_path):
    _write(tmp_path, "smoke.json",
           {"ok": True, "source": {"reachable": True, "checks": []},
            "destination": {"skipped": True, "checks": []}})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "smoke")
    assert row["found"] != "PASS" and row["attention"] is True


def test_a_smoke_run_that_checked_both_ends_reads_pass(tmp_path):
    _write(tmp_path, "smoke.json",
           {"ok": True, "complete": True, "verdict": "PASS",
            "source": {"reachable": True, "checks": []},
            "destination": {"skipped": False, "checks": []}})
    row = next(s for s in build_stage_board(tmp_path)["stages"]
               if s["stage"] == "smoke")
    assert row["found"] == "PASS" and row["attention"] is False


# --------------------------------------------------------------------------
# "A stage that could not look is FLAGGED, never shown as clean" -- the rule
# at the top of report/stages.py, checked against the artifacts as their
# producers actually write them.
# --------------------------------------------------------------------------

def _row(tmp_path, stage):
    return next(s for s in build_stage_board(tmp_path)["stages"]
                if s["stage"] == stage)


def test_parsed_ddl_lineage_is_flagged_as_partial(tmp_path):
    # dependencies.json never carries `cycles` (that key lives in plan.json),
    # so the old rule could never fire. A graph parsed from view DDL alone is
    # partial, and the plan skill says so; the board must too.
    _write(tmp_path, "dependencies.json",
           {"edges": [], "source_used": "parsed_ddl",
            "coverage_note": "views only", "unresolved_references": ["DB.S.T"]})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "partial" in row["found"].lower()
    assert "1 unresolved" in row["found"]


def test_not_extracted_lineage_is_flagged(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [], "source_used": "not_extracted",
            "coverage_note": "manifest ingest", "unresolved_references": []})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "not extracted" in row["found"].lower()


def test_a_lineage_artifact_without_a_source_is_flagged_not_guessed(tmp_path):
    # No `source_used` means the board cannot tell how the graph was got.
    # Refuse rather than guess: flag it, and do not label it "view DDL only",
    # which is a statement about parsed_ddl.
    _write(tmp_path, "dependencies.json", {"edges": []})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "view ddl only" not in row["found"].lower(), row["found"]
    assert "not recorded" in row["found"].lower(), row["found"]


def test_account_usage_lineage_is_clean(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [{"from": "A", "to": "B"}], "source_used": "account_usage",
            "coverage_note": "", "unresolved_references": []})
    row = _row(tmp_path, "deps")
    assert row["attention"] is False
    assert "1 edge" in row["found"]


def test_blocked_warehouses_are_counted_and_flagged(tmp_path):
    # compute.json is written from propose_all: `proposals` and `blocked`.
    _write(tmp_path, "compute.json",
           {"proposals": [{"name": "WH_A"}],
            "blocked": [{"name": "WH_B", "reason": "no metering"}]})
    row = _row(tmp_path, "compute")
    assert row["attention"] is True
    assert "1 warehouse(s) sized" in row["found"]
    assert "1 blocked" in row["found"]


def test_a_provision_step_left_unconfirmed_is_flagged(tmp_path):
    # `libraries: install_requested + restart` records verified None in
    # execute mode: not confirmed, not assumed. That is not clean.
    _write(tmp_path, "provision_result.json",
           {"dry_run": False, "workspace": {"name": "ws"},
            "steps": [{"step": "workspace", "action": "created",
                       "verified": True, "detail": ""},
                      {"step": "libraries", "action": "install_requested + restart",
                       "verified": None, "detail": "NOT confirmed, not assumed"}]})
    row = _row(tmp_path, "provision")
    assert row["attention"] is True
    assert "1 not confirmed" in row["found"]


def test_unverified_structure_and_drift_are_not_hidden_on_the_board(tmp_path):
    # The default transport (catalog_api) is the one that produces these two
    # outcomes, and the board summed only failed + mismatched.
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 4, "verified": 2,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": ["X"],
            "derived_type_drift_targets": ["Y"], "errors": []})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is True
    assert "verified 2/4" in row["found"]
    assert "1 structure not verified" in row["found"]
    assert "1 created with derived type drift" in row["found"]
    assert "deploy" in build_stage_board(tmp_path)["needs_attention"]


def test_deploy_transport_errors_are_flagged(tmp_path):
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 2, "verified": 1,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": [], "derived_type_drift_targets": [],
            "errors": [{"schema": "c.s", "error": "409 ongoing operation"}]})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is True
    assert "1 error" in row["found"]


def test_a_fully_verified_deploy_is_clean(tmp_path):
    _write(tmp_path, "deploy_result.json",
           {"dry_run": False, "statement_count": 3, "verified": 3,
            "failed": [], "mismatched_targets": [],
            "unverified_structure_targets": [], "derived_type_drift_targets": [],
            "errors": []})
    row = _row(tmp_path, "deploy")
    assert row["attention"] is False
    assert row["found"] == "**verified 3/3**"


# --------------------------------------------------------------------------
# The preamble must agree with the CLI it describes: `smoke --write-probe`
# writes only with `--execute`, and `notebook --upload` never writes (a dry
# run without `--execute`, refused with it). The old carve-out named both as
# "narrow opt-ins" that can write, while the board's own rows carried no
# (writes) marker for either.
# --------------------------------------------------------------------------

def test_the_preamble_does_not_call_smoke_and_notebook_opt_in_writers(tmp_path):
    md = render_stages(build_stage_board(tmp_path))
    preamble = md.split("| Stage |", 1)[0]
    assert "narrow opt-ins" not in preamble
    assert "read-only" in preamble.lower()
    assert "`smoke --write-probe --execute`" in preamble, \
        "the probe writes only with --execute, and the preamble must say so"
    assert "`notebook --upload`" in preamble and "refused" in preamble, \
        "--upload sends nothing; the preamble must say it is refused"


# --------------------------------------------------------------------------
# deps: the producer now writes four source_used values. "view DDL only" is a
# statement about parsed_ddl and must not be applied to a merged graph whose
# edges came mostly from ACCOUNT_USAGE; a value the board does not recognise
# is flagged, not read as clean.
# --------------------------------------------------------------------------

def test_merged_account_usage_and_parsed_lineage_is_flagged_but_not_called_view_ddl_only(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [{"from": "DB.S.V1", "to": "DB.S.T", "source": "account_usage"},
                      {"from": "DB.S.V", "to": "DB.S.V1", "source": "parsed_ddl"}],
            "source_used": "account_usage+parsed_ddl",
            "coverage_note": "partly lagged",
            "unresolved_references": ["OTHER.S.X"],
            "views_without_account_usage_edge": ["DB.S.V"],
            "warning": "1 view(s) have no ACCOUNT_USAGE lineage edge (the view "
                       "lags DDL by up to ~3 h); their DDL was parsed instead. "
                       "Re-run `deps` after the lag before relying on the wave "
                       "order."})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "view ddl only" not in row["found"].lower(), row["found"]
    assert "1 view(s)" in row["found"]
    assert "1 unresolved" in row["found"]


def test_empty_account_usage_lineage_is_flagged(tmp_path):
    _write(tmp_path, "dependencies.json",
           {"edges": [{"from": "DB.S.V", "to": "DB.S.T", "source": "parsed_ddl"}],
            "source_used": "account_usage_empty", "coverage_note": "empty",
            "unresolved_references": [],
            "views_without_account_usage_edge": ["DB.S.V"],
            "warning": "1 view(s) have no ACCOUNT_USAGE lineage edge; their DDL "
                       "was parsed instead. Re-run `deps` after the lag."})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "view ddl only" not in row["found"].lower(), row["found"]
    assert "1 view(s)" in row["found"]


def test_unrecognised_lineage_source_is_flagged_not_guessed(tmp_path):
    _write(tmp_path, "dependencies.json", {"edges": [], "source_used": "x"})
    row = _row(tmp_path, "deps")
    assert row["attention"] is True
    assert "view ddl only" not in row["found"].lower(), row["found"]


# --------------------------------------------------------------------------
# security: a policy object that exists while POLICY_REFERENCES shows no
# attachment is UNCONFIRMED, not clean (security.py's own invariant). So is
# an empty attachment list when SHOW MASKING/ROW ACCESS POLICIES was denied.
# --------------------------------------------------------------------------

def test_a_defined_but_unattached_policy_is_flagged(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies_defined_without_attachment": 1})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is True
    assert "unconfirmed" in sec["found"].lower(), sec["found"]
    assert "security" in build_stage_board(tmp_path)["needs_attention"]


def test_unenumerable_policy_objects_are_flagged(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies": {"masking": {"count": None, "readable": False},
                         "row_access": {"count": 0, "readable": True},
                         "tags": {"count": 0, "readable": True}}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is True
    assert "could not be enumerated" in sec["found"].lower(), sec["found"]


def test_enumerated_and_unattached_policies_read_clean(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies_defined_without_attachment": 0,
            "policies": {"masking": {"count": 0, "readable": True},
                         "row_access": {"count": 0, "readable": True},
                         "tags": {"count": 0, "readable": True}}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is False


def test_an_unenumerable_aggregation_policy_is_flagged(tmp_path):
    # The kind that used to be missing entirely: a denied SHOW AGGREGATION
    # POLICIES must reach the board, or the row reads clean about a question
    # nobody asked.
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies": {"masking": {"count": 0, "readable": True},
                         "row_access": {"count": 0, "readable": True},
                         "aggregation": {"count": None, "readable": False},
                         "projection": {"count": 0, "readable": True},
                         "tags": {"count": 0, "readable": True}}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is True
    assert "could not be enumerated" in sec["found"].lower(), sec["found"]


def test_an_unenumerable_projection_policy_is_flagged(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies": {"masking": {"count": 0, "readable": True},
                         "row_access": {"count": 0, "readable": True},
                         "aggregation": {"count": 0, "readable": True},
                         "projection": {"count": None, "readable": False},
                         "tags": {"count": 0, "readable": True}}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is True


def test_unreadable_tag_attachments_are_flagged_rather_than_read_as_none(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies": {"masking": {"count": 0, "readable": True},
                         "row_access": {"count": 0, "readable": True},
                         "aggregation": {"count": 0, "readable": True},
                         "projection": {"count": 0, "readable": True},
                         "tags": {"count": 0, "readable": True}},
            "tag_references": {"measured": False, "count": None}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is True
    assert "tag attachment" in sec["found"].lower(), sec["found"]


def test_all_four_policy_kinds_enumerated_and_clean_reads_clean(tmp_path):
    _write(tmp_path, "security.json",
           {"exposure_count": 0, "secure_views": [], "grants": {},
            "policies_defined_without_attachment": 0,
            "policies": {"masking": {"count": 0, "readable": True},
                         "row_access": {"count": 0, "readable": True},
                         "aggregation": {"count": 0, "readable": True},
                         "projection": {"count": 0, "readable": True},
                         "tags": {"count": 0, "readable": True}},
            "tag_references": {"measured": True, "count": 0}})
    sec = _row(tmp_path, "security")
    assert sec["attention"] is False
