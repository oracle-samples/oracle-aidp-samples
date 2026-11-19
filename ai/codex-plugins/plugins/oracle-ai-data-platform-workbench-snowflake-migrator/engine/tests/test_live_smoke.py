"""End-to-end against a real Snowflake estate. Skipped unless SNOWMIG_LIVE=1.

Read-only. Runs assess -> deps -> plan -> ddl and asserts the pipeline holds
together on real metadata. It does NOT deploy: that needs AIDP coordinates,
which are supplied per conversation and never stored.
"""
import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SNOWMIG_LIVE") != "1",
    reason="set SNOWMIG_LIVE=1 plus SNOWFLAKE_* to run against a live estate")

from snowmig import main  # noqa: E402


PLUGIN_ROOT = Path(__file__).resolve().parents[2]


def _migration_config() -> dict:
    """The Snowflake block of the ONE migration config, if there is one.

    There used to be a second file for this (`local-test-account.yaml`, with
    its own tracked template). One config file is the whole design, so the
    live tests read the same file every stage reads -- and skip cleanly when
    it is absent.
    """
    try:
        from migration_config import (discover_config, load_config,
                                      snowflake_block)
    except ImportError:
        return {}
    try:
        return snowflake_block(load_config(
            discover_config(plugin_root=PLUGIN_ROOT)))
    except Exception:
        return {}


DB = os.environ.get("SNOWMIG_LIVE_DB") or _migration_config().get(
    "database", "SNOWMIG_TESTDB")


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    return tmp_path_factory.mktemp("live")


def auth_args():
    return ["--account", os.environ["SNOWFLAKE_ACCOUNT"],
            "--user", os.environ["SNOWFLAKE_USER"],
            "--auth", "keypair",
            "--key-path", os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            "--warehouse", os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")]


def test_assess_finds_tables_and_views(out):
    rc = main(["assess", "--out-dir", str(out), "--database", DB] + auth_args())
    assert rc == 0, "exit 3 would mean an identifier-case collision"
    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    assert inv["counts_by_type"].get("TABLE", 0) >= 1
    assert inv["counts_by_type"].get("VIEW", 0) >= 1
    assert inv["extraction_notes"] == []
    # Default mode is `metadata`: tables carry Snowflake's maintained count,
    # views carry none, because counting a view means executing it.
    assert inv["row_count_mode"] == "metadata"
    for r in inv["inventory"]:
        if r["object_type"] == "TABLE":
            assert r["row_count_exact"] is not None
            assert r["row_count_source"] == "show_metadata"
        else:
            assert r["row_count_source"] == "not_counted"
            assert r["row_count_note"]


def test_decimal_columns_map_with_precision(out):
    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    decimals = [c for r in inv["inventory"] for c in r["columns"]
                if (c.get("DATA_TYPE") or "").upper() == "NUMBER"]
    assert decimals, "the estate should contain NUMBER columns"
    for c in decimals:
        assert c["target_type"].startswith("DECIMAL("), c
        assert c["NUMERIC_PRECISION"] is not None


def test_timestamp_ntz_maps_to_resolved_mode(out):
    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    ntz = [c for r in inv["inventory"] for c in r["columns"]
           if (c.get("DATA_TYPE") or "").upper() == "TIMESTAMP_NTZ"]
    assert ntz, "the estate should contain TIMESTAMP_NTZ columns"
    # The default mapping downgrades to TIMESTAMP; only `preserve` keeps NTZ.
    want = ("TIMESTAMP_NTZ" if inv["timestamp_ntz_mode"] == "preserve"
            else "TIMESTAMP")
    assert all(c["target_type"] == want for c in ntz), want


def test_deps_and_plan(out):
    assert main(["deps", "--out-dir", str(out)] + auth_args()) == 0
    deps = json.loads((out / "dependencies.json").read_text(encoding="utf-8"))
    assert deps["source_used"] in ("account_usage", "parsed_ddl",
                                   "account_usage_empty",
                                   "account_usage+parsed_ddl")
    assert main(["plan", "--out-dir", str(out)]) == 0
    plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    assert plan["waves"], "at least one wave expected"
    assert plan["clone_targets"], "objects should be clone targets"
    # Bronze mirrors the source, so the target name equals the source name --
    # except in CASE. AIDP lower-cases identifiers (verified live), so the plan
    # carries the folded name, which is what the destination will really use.
    for ident, target in plan["target_names"].items():
        assert target == ident.lower(), (ident, target)
    assert plan["catalogs_to_create"] == [DB.lower()]
    # Every inventoried object lands in exactly one verdict.
    ids = ([c["source_identifier"] for c in plan["can_migrate"]]
           + [c["source_identifier"] for c in plan["cannot_migrate"]])
    assert len(ids) == len(set(ids)) == plan["summary"]["objects_inventoried"]
    for c in plan["cannot_migrate"]:
        assert c["category"] and c["reason"], c


def test_view_lands_after_its_base_tables(out):
    plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    deps = json.loads((out / "dependencies.json").read_text(encoding="utf-8"))
    if not deps["edges"]:
        pytest.skip("no edges resolved; ordering assertion not meaningful")
    wave_of = {n: i for i, w in enumerate(plan["waves"]) for n in w}
    for edge in deps["edges"]:
        if edge["from"] in wave_of and edge["to"] in wave_of:
            assert wave_of[edge["to"]] < wave_of[edge["from"]], edge


def test_ddl_generates_delta_tables_and_views_and_no_replace(out):
    # `--timestamp-ntz timestamp` keeps this module passing whichever mode
    # `assess` resolved: under `preserve` the estate's TIMESTAMP_NTZ columns
    # (asserted earlier) would halt a bare `ddl` on the type the metastore
    # refuses, and under the default the re-map changes nothing.
    assert main(["ddl", "--out-dir", str(out),
                 "--timestamp-ntz", "timestamp"]) == 0
    ddl = json.loads((out / "ddl_plan.json").read_text(encoding="utf-8"))
    assert ddl["statements"]
    kinds = {s["object_type"] for s in ddl["statements"]}
    assert "TABLE" in kinds
    for st in ddl["statements"]:
        assert "IF NOT EXISTS" in st["sql"]
        assert "OR REPLACE" not in st["sql"]
        if st["object_type"] == "TABLE":
            assert "USING DELTA" in st["sql"]
        else:
            assert st["sql"].startswith("CREATE VIEW IF NOT EXISTS")


def test_the_view_is_emitted_after_its_base_tables(out):
    ddl = json.loads((out / "ddl_plan.json").read_text(encoding="utf-8"))
    kinds = [s["object_type"] for s in ddl["statements"]]
    if "VIEW" not in kinds:
        pytest.skip("estate has no migratable view")
    assert kinds.index("VIEW") > max(
        i for i, k in enumerate(kinds) if k == "TABLE")


def test_silver_and_gold_jobs_are_planned_but_disabled(out):
    plan = json.loads((out / "plan.json").read_text(encoding="utf-8"))
    jobs = plan["silver_gold_jobs"]
    assert jobs, "one silver + one gold job per migratable schema"
    assert all(j["enabled"] is False for j in jobs)
    assert all(j["trigger"] == "MANUAL_NEVER_TRIGGERED" for j in jobs)


def test_compute_proposal_maps_warehouses_to_clusters(out):
    assert main(["compute", "--out-dir", str(out)] + auth_args()) == 0
    sizing = json.loads((out / "compute.json").read_text(encoding="utf-8"))
    assert sizing["warehouse_count"] >= 1
    assert sizing["proposals"], "at least one cluster proposal"
    for p in sizing["proposals"]:
        assert p["worker_count"] >= 1
        assert p["shape_confirmation_required"] is True
    # No credit price was passed, so no cost model may be asserted.
    assert sizing["cost_model"] is None


def test_deploy_dry_run_creates_nothing(out):
    assert main(["deploy", "--out-dir", str(out)]) == 0
    res = json.loads((out / "deploy_result.json").read_text(encoding="utf-8"))
    assert res["dry_run"] is True and res["executed"] == 0


def test_show_pagination_resumes_exclusively_and_in_name_order(tmp_path):
    """Assumption B9, verified live.

    The inventory pages past SHOW's 10k cap with `LIMIT n FROM '<name>'`. If
    the resume were inclusive an object would be listed twice; if the order
    were not by name, paging would silently MISS objects -- the worst outcome
    available to an assessment, because the result still looks complete.
    """
    import os as _os
    from snowflake_source import conn as _conn

    kw = _conn.build_connect_kwargs(
        "keypair", account=_os.environ["SNOWFLAKE_ACCOUNT"],
        user=_os.environ["SNOWFLAKE_USER"],
        key_path=_os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        warehouse=_os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"))
    cx = _conn.connect(**kw)
    try:
        run = _conn.make_run_sql(cx)
        show = f'show tables in schema "{DB}"."PUBLIC"'
        full = [r["name"] for r in run(show)]
        assert len(full) >= 3, "need a few objects to page through"
        assert full == sorted(full), "SHOW must be name-ordered for paging to work"

        # Walk it in pages of 2 and require the result to equal the unpaged list.
        paged, cursor = [], None
        while True:
            sql = f"{show} limit 2" + (f" from '{cursor}'" if cursor else "")
            page = [r["name"] for r in run(sql)]
            assert cursor not in page, "resume must be EXCLUSIVE of the cursor"
            paged += page
            if len(page) < 2:
                break
            cursor = page[-1]
        assert paged == full
    finally:
        cx.close()


def test_semi_structured_switch_against_real_variant_columns(tmp_path):
    """The semi-structured switch, against genuine Snowflake columns.

    A test database may have none, so this reads SNOWFLAKE.ACCOUNT_USAGE, whose views
    carry real VARIANT, OBJECT and ARRAY columns. Read-only: the plugin cannot
    create a VARIANT column to test with, and must not.
    """
    import os as _os
    from snowflake_source import conn as _conn
    from snowflake_source.extract.catalog import build_inventory

    kw = _conn.build_connect_kwargs(
        "keypair", account=_os.environ["SNOWFLAKE_ACCOUNT"],
        user=_os.environ["SNOWFLAKE_USER"],
        key_path=_os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        warehouse=_os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        role="ACCOUNTADMIN")
    cx = _conn.connect(**kw)
    try:
        real = _conn.make_run_sql(cx)
        targets = ("ACCESS_HISTORY", "AGGREGATE_ACCESS_HISTORY")

        def narrowed(sql, params=None):
            rows = real(sql, params)
            low = " ".join(sql.split()).lower()
            if low.startswith("show schemas in database"):
                return [r for r in rows if r["name"] == "ACCOUNT_USAGE"]
            if low.startswith(("show views in schema", "show tables in schema")):
                return [r for r in rows if r["name"] in targets]
            return rows

        try:
            blocked = build_inventory(narrowed, ["SNOWFLAKE"], row_counts="none")
        except Exception as exc:                      # pragma: no cover
            pytest.skip(f"ACCOUNT_USAGE not readable by this role: {exc}")
        if not blocked["inventory"]:
            pytest.skip("ACCOUNT_USAGE returned no objects for this role")

        # Default: blocked, with a per-column reason naming the actual type.
        for rec in blocked["inventory"]:
            assert rec["compatibility_status"] == "blocked"
            assert rec["blocked_reasons"]
            assert any(t in " ".join(rec["blocked_reasons"])
                       for t in ("VARIANT", "OBJECT", "ARRAY"))

        # Escape hatch: carried as STRING, and every one of them warned about.
        carried = build_inventory(narrowed, ["SNOWFLAKE"], row_counts="none",
                                  semi_structured="string")
        for rec in carried["inventory"]:
            assert rec["compatibility_status"] == "supported"
            semi = [c for c in rec["columns"]
                    if c["DATA_TYPE"] in ("VARIANT", "OBJECT", "ARRAY")]
            assert semi, "expected real semi-structured columns here"
            for col in semi:
                assert col["target_type"] == "STRING"
                assert any(w.startswith(col["COLUMN_NAME"] + ":")
                           for w in rec["warnings"]), \
                    f'{col["COLUMN_NAME"]} carried as text with no warning'
    finally:
        cx.close()


def test_census_and_security_stages_run_live(tmp_path):
    """Both new stages, end to end against the real account.

    Neither may fabricate a clean bill of health: the census reports what it
    could not read, and security reports UNKNOWN rather than zero when
    ACCOUNT_USAGE is denied.
    """
    out = tmp_path / "live"
    assert main(["assess", "--out-dir", str(out), "--database", DB]
                + auth_args()) == 0

    inv = json.loads((out / "inventory.json").read_text(encoding="utf-8"))
    census = inv.get("census")
    assert census is not None, "the census must run inside assess by default"
    # Every declared kind is accounted for, readable or explicitly not.
    for kind, info in census["kinds"].items():
        assert info["readable"] or info["count"] is None, \
            f"{kind}: an unreadable kind must not report a count"
    assert census["scope_statement"]
    assert (out / "CENSUS.md").exists()
    # Nothing in the census is ever migratable.
    for obj in census["objects"]:
        assert obj["migratable"] is False

    assert main(["security", "--out-dir", str(out)] + auth_args()) == 0
    sec = json.loads((out / "security.json").read_text(encoding="utf-8"))
    assert (out / "SECURITY.md").exists()
    if sec["exposure_count"] is None:
        assert "could not" in sec["statement"].lower()
    else:
        for e in sec["exposures"]:
            assert e["severity"] == "HIGH"
            assert e["consequence"]
    # Grants are reported, never replayed.
    assert sec["grants"].get("carried_over") in (False, None)


def test_maintenance_stage_runs_live(tmp_path):
    out = tmp_path / "live"
    assert main(["assess", "--out-dir", str(out), "--database", DB]
                + auth_args()) == 0
    assert main(["maintenance", "--out-dir", str(out)] + auth_args()) == 0
    maint = json.loads((out / "maintenance.json").read_text(encoding="utf-8"))
    assert (out / "MAINTENANCE.md").exists()
    # Reports, never proposes.
    assert "RETAIN" not in json.dumps(maint).upper()
    for t in maint["tables"]:
        rec = t["reclustering"]
        if not rec["measured"]:
            assert rec["credits"] is None, "unmeasured must not read as zero"
