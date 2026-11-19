"""A catalog is billed to this migration only when it created it.

Found in review. build_resources seeded its catalogs from provision's
`external_catalog` / `target_catalog` -- names written into the job
parameters; provision creates no catalog -- and a dry-run catalog_result
did not remove the seed. The ledger's `reused` rows counted the same as
`created`. So "Everything this migration allocated" listed a target
catalog that was never registered (the stage board said "would be
registered; nothing was"), or one that existed before the migration, as
"ACTIVE (kept)" and accruing storage.

Now a catalog is allocated only on evidence of creation: a ledger row, or
an executed catalog_result, with action `created`. A reused catalog and a
name that only appears in the job parameters are listed apart, as not
allocated by this migration.
"""
import json

from report.resources import build_resources, render_resources_section
from test_resources import PROV, _by, _out


def _ledger(out, **row):
    with (out / "resources.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"at": "2026-09-24T19:30:00Z", "stage": "catalog",
                             "kind": "catalog", **row}) + "\n")


def test_job_parameter_names_are_not_allocated_catalogs(tmp_path):
    out = _out(tmp_path)
    (out / "catalog_result.json").write_text(json.dumps(
        {"dry_run": True, "catalog": "tgt_int", "catalog_type": "INTERNAL",
         "action": "would create"}))
    res = build_resources(out)
    assert _by(res, "catalog") == []
    assert not [r for r in res["accruing_now"] if r["kind"] == "catalog"]
    named = {r["name"]: r for r in res["not_allocated"]
             if r["kind"] == "catalog"}
    assert set(named) == {PROV["external_catalog"], PROV["target_catalog"]}
    assert "not verified to exist" in named["tgt_int"]["why"]
    md = "\n".join(render_resources_section(res))
    assert "not allocated by this migration" in md.lower()


def test_a_reused_catalog_is_pre_existing_not_allocated(tmp_path):
    out = _out(tmp_path)
    _ledger(out, name="shared_cat", type="INTERNAL", key="shared_cat",
            action="reused")
    res = build_resources(out)
    assert "shared_cat" not in {c["name"] for c in _by(res, "catalog")}
    reused = next(r for r in res["not_allocated"]
                  if r["name"] == "shared_cat")
    assert "existed before" in reused["why"]


def test_a_created_catalog_is_allocated_and_accrues(tmp_path):
    out = _out(tmp_path)
    _ledger(out, name="tgt_int", type="INTERNAL", key="tgt_int",
            action="created")
    res = build_resources(out)
    assert {c["name"] for c in _by(res, "catalog")} == {"tgt_int"}
    assert "tgt_int" in {r["name"] for r in res["accruing_now"]}
    assert "tgt_int" not in {r["name"] for r in res["not_allocated"]}


def test_an_executed_catalog_result_that_created_it_counts(tmp_path):
    out = _out(tmp_path)
    (out / "catalog_result.json").write_text(json.dumps(
        {"dry_run": False, "catalog": "src_ext", "catalog_type": "EXTERNAL",
         "action": "created"}))
    res = build_resources(out)
    assert {c["name"] for c in _by(res, "catalog")} == {"src_ext"}
