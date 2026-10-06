"""What a migration allocated, which phase allocated it, and what bills.

Built from the run's own artifacts -- provision_result.json, the catalog
ledger, the structure results, teardown_result.json -- so it can only list
what this migration created. Billing is CLASSIFIED, never priced: a price
that is not on the operator's rate card would read as authoritative.
"""
import json

from report.resources import build_resources, render_resources_section
from report.tokens import record_stage_run

PROV = {"dry_run": False,
        "workspace": {"name": "mig_ws", "key": "ws-1"},
        "cluster": {"name": "migration_assets", "key": "cl-1",
                    "created": True},
        "warehouse_clusters": [{"warehouse": "COMPUTE_WH", "name": "compute",
                                "key": "wc-1", "created": True}],
        "external_catalog": "src_ext", "target_catalog": "tgt_int",
        "steps": [{"step": "job", "action": "created", "verified": True,
                   "detail": "snowmig_00_discover"},
                  {"step": "job", "action": "reused", "verified": True,
                   "detail": "snowmig_01_structure (stage notebook refreshed)"},
                  {"step": "upload", "action": "uploaded", "verified": True,
                   "detail": "x"},
                  {"step": "notebook", "action": "uploaded", "verified": True,
                   "detail": "y"}]}


def _out(tmp_path, teardown=None):
    (tmp_path / "provision_result.json").write_text(json.dumps(PROV))
    record_stage_run(tmp_path, "provision", "2026-09-24T19:22:00+00:00",
                     "2026-09-24T19:23:00+00:00", 0, None)
    if teardown:
        (tmp_path / "teardown_result.json").write_text(json.dumps(teardown))
        record_stage_run(tmp_path, "teardown", "2026-09-24T21:30:00+00:00",
                         "2026-09-24T21:35:00+00:00", 0, None)
    return tmp_path


def _by(res, kind):
    return [r for r in res["resources"] if r["kind"] == kind]


def test_everything_provision_created_is_listed_with_its_phase(tmp_path):
    res = build_resources(_out(tmp_path))
    assert _by(res, "workspace")[0]["key"] == "ws-1"
    clusters = {c["key"]: c for c in _by(res, "cluster")}
    assert set(clusters) == {"cl-1", "wc-1"}
    assert clusters["cl-1"]["allocated_by"] == "provision"
    assert clusters["cl-1"]["phase"] == "setup"
    assert {j["name"] for j in _by(res, "job")} == {"snowmig_00_discover",
                                                     "snowmig_01_structure"}
    assert _by(res, "workspace_files")[0]["count"] == 2


def test_both_catalogs_are_listed_with_their_types(tmp_path):
    # provision only NAMES them in the job parameters; the catalog stage
    # creates them (test_resources_catalogs.py), so without its evidence
    # they are listed apart, not billed as allocated.
    res = build_resources(_out(tmp_path))
    assert _by(res, "catalog") == []
    cats = {c["name"]: c for c in res["not_allocated"]
            if c["kind"] == "catalog"}
    assert cats["src_ext"]["type"] == "EXTERNAL"
    assert cats["tgt_int"]["type"] == "INTERNAL"


def test_the_catalog_ledger_adds_what_the_overwritten_result_lost(tmp_path):
    out = _out(tmp_path)
    (out / "resources.jsonl").write_text(json.dumps(
        {"at": "2026-09-24T19:30:00Z", "stage": "catalog", "kind": "catalog",
         "name": "extra_cat", "type": "INTERNAL", "key": "extra_cat",
         "action": "created"}) + "\n")
    names = {c["name"] for c in _by(build_resources(out), "catalog")}
    assert names == {"extra_cat"}


def test_billing_is_classified_never_priced(tmp_path):
    out = _out(tmp_path)
    (out / "resources.jsonl").write_text(json.dumps(
        {"at": "2026-09-24T19:30:00Z", "stage": "catalog", "kind": "catalog",
         "name": "tgt_int", "type": "INTERNAL", "key": "tgt_int",
         "action": "created"}) + "\n")
    res = build_resources(out)
    by = {r["kind"]: r for r in res["resources"]}
    assert by["cluster"]["billing"] == "compute while ACTIVE"
    assert by["job"]["billing"] == "via cluster compute"
    int_cat = next(c for c in _by(res, "catalog") if c["type"] == "INTERNAL")
    assert int_cat["billing"] == "storage for data held"
    assert "$" not in json.dumps(res)


def test_a_cluster_never_torn_down_is_still_accruing(tmp_path):
    res = build_resources(_out(tmp_path))
    cl = next(c for c in _by(res, "cluster") if c["key"] == "cl-1")
    assert cl["state"] == "ACTIVE (not torn down)"
    assert "cl-1" in {r["key"] for r in res["accruing_now"]}


def test_teardown_is_reflected_and_the_compute_window_measured(tmp_path):
    res = build_resources(_out(tmp_path, teardown={
        "dry_run": False, "action": "stop",
        "steps": [{"cluster": "cl-1", "action": "stopped", "verified": True,
                   "state": "STOPPED"},
                  {"cluster": "wc-1", "action": "stopped", "verified": True,
                   "state": "STOPPED"}]}))
    cl = next(c for c in _by(res, "cluster") if c["key"] == "cl-1")
    assert cl["state"] == "STOPPED" and cl["released_by"] == "teardown"
    assert cl["running_hours"] == round((21 * 60 + 35 - (19 * 60 + 23)) / 60, 2)
    lo, hi = cl["ocpu_hours"]
    assert lo == round(cl["running_hours"] * 4, 2)
    assert hi == round(cl["running_hours"] * 6, 2)
    assert not [r for r in res["accruing_now"] if r["kind"] == "cluster"]


def test_a_dry_run_teardown_releases_nothing(tmp_path):
    res = build_resources(_out(tmp_path, teardown={
        "dry_run": True, "action": "stop",
        "steps": [{"cluster": "cl-1", "action": "would stop"}]}))
    cl = next(c for c in _by(res, "cluster") if c["key"] == "cl-1")
    assert cl["state"] == "ACTIVE (not torn down)"


def test_no_provision_means_nothing_allocated(tmp_path):
    res = build_resources(tmp_path)
    assert res["resources"] == [] and "nothing" in res["note"].lower()


def test_snowflake_side_usage_is_named(tmp_path):
    out = _out(tmp_path)
    record_stage_run(out, "assess", "2026-09-24T19:00:00+00:00",
                     "2026-09-24T19:07:00+00:00", 0, None)
    (out / "inventory.json").write_text(json.dumps(
        {"row_count_mode": "exact", "session": {"WH": "COMPUTE_WH"},
         "inventory": [{}] * 3}))
    usage = build_resources(out)["snowflake_usage"]
    assert usage["warehouse"] == "COMPUTE_WH"
    assert any("exact" in u for u in usage["drivers"])


def test_the_final_section_names_what_bills(tmp_path):
    md = "\n".join(render_resources_section(build_resources(_out(tmp_path))))
    assert "## Allocated resources and billing" in md
    assert "`migration_assets`" in md and "compute while ACTIVE" in md
    assert "Still accruing now" in md and "rate card" in md


def test_phases_md_lists_the_resources_each_phase_allocated(tmp_path):
    from report.render import render_phase_report
    from report.stages import phase_report
    md = render_phase_report(phase_report(_out(tmp_path)))
    setup = md[md.index("## Phase: setup"):md.index("## Phase: discovery")]
    assert "Resources allocated" in setup and "`migration_assets`" in setup
    assert "| Resources |" in md


def test_the_summary_carries_the_billing_section(tmp_path):
    from report.render import render_summary
    res = build_resources(_out(tmp_path))
    md = render_summary({"can_migrate": [], "cannot_migrate": []},
                        {"inventory": [], "session": {}}, None, None,
                        resources=res)
    assert "## Allocated resources and billing" in md


def test_the_catalog_stage_records_what_it_creates(tmp_path):
    from report.resources import record_resource
    record_resource(tmp_path, stage="catalog", kind="catalog", name="c1",
                    type="INTERNAL", key="c1", action="created")
    rec = json.loads((tmp_path / "resources.jsonl").read_text())
    assert rec["name"] == "c1" and rec["stage"] == "catalog" and rec["at"]


def test_an_observed_creation_time_beats_the_run_log_estimate(tmp_path):
    """Found live: the first provision run with exit 0 was a DRY run, so the
    estimate started the compute clock before the cluster existed."""
    from report.resources import record_resource
    out = _out(tmp_path)
    cl = next(c for c in _by(build_resources(out), "cluster")
              if c["key"] == "cl-1")
    assert cl["created_at_source"] == "approximate (end of first provision run)"
    record_resource(out, stage="provision", kind="cluster", key="cl-1",
                    created_at="2026-09-24T19:22:18.880000+00:00",
                    source="aidp cluster timeCreated")
    cl = next(c for c in _by(build_resources(out), "cluster")
              if c["key"] == "cl-1")
    assert cl["created_at"].startswith("2026-09-24T19:22:18")
    assert cl["created_at_source"] == "aidp cluster timeCreated"


def test_provision_records_when_it_created_a_cluster():
    import inspect
    from target import provisioning
    assert '"created_at"' in inspect.getsource(provisioning.provision)
