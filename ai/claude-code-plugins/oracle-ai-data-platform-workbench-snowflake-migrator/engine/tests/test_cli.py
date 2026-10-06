"""CLI wiring. The offline subcommands are tested with no connection."""
import json
import pathlib
import sys

import pytest

from snowmig import main

INV = {"probed_at": "t", "session": {}, "databases_in_scope": ["D"],
       "object_count": 1, "counts_by_type": {"TABLE": 1},
       "identifier_case_collisions": {}, "extraction_notes": [],
       "inventory": [{
           "source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
           "source_database": "D", "source_schema": "PUBLIC",
           "compatibility_status": "supported", "blocked_reasons": [],
           "row_count_exact": 100, "source_metadata": {"bytes": 10},
           "warnings": [],
           "columns": [{"COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER",
                        "target_type": "DECIMAL(38,0)", "IS_NULLABLE": "NO",
                        "ORDINAL_POSITION": 1, "COMMENT": None}]}]}
DEPS = {"edges": [], "source_used": "parsed_ddl", "coverage_note": "views only"}


def write(tmp, name, payload):
    p = tmp / name
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def test_plan_subcommand_writes_both_artifacts(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    rc = main(["plan", "--out-dir", str(tmp_path)])
    assert rc == 0
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["clone_targets"] == ["D.PUBLIC.ORDERS"]
    assert "planned to move" in (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8").lower()


def test_plan_bronze_mirrors_the_source_by_default(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "d.public.orders"
    assert plan["catalogs_to_create"] == ["d"]


def test_plan_honours_the_bronze_catalog_prefix(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path), "--bronze-catalog-prefix", "bronze"])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["target_names"]["D.PUBLIC.ORDERS"] == "bronze.d_public.orders"


def test_plan_applies_a_restrictions_file(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_databases": ["D"]}), encoding="utf-8")
    main(["plan", "--out-dir", str(tmp_path), "--restrictions",
          str(tmp_path / "r.json")])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["can_migrate"] == []
    assert plan["cannot_migrate"][0]["category"] == "restriction"


def test_bad_restriction_key_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"exclude_datbases": ["D"]}), encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    assert "exclude_datbases" in capsys.readouterr().err


def test_separate_databases_no_longer_collide_under_the_bronze_mirror(tmp_path):
    # layer-catalog used to fold the database away and merge these. The mirror
    # keeps all three parts, so they are distinct by construction.
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D2.PUBLIC.ORDERS"
    second["source_database"] = "D2"
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 0
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert sorted(plan["catalogs_to_create"]) == ["d", "d2"]


def test_plan_exits_3_on_a_case_variant_collision(tmp_path):
    # The collision that CAN still happen: names differing only by case.
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second["source_identifier"] = "D.PUBLIC.orders"
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 3


def test_assess_exits_3_on_case_collision(tmp_path, monkeypatch):
    inv = json.loads(json.dumps(INV))
    inv["identifier_case_collisions"] = {"D.PUBLIC.ORDERS": ["D.PUBLIC.ORDERS",
                                                             "D.PUBLIC.orders"]}
    monkeypatch.setattr("snowmig._assess_inventory", lambda args: inv)
    assert main(["assess", "--out-dir", str(tmp_path), "--account", "a",
                 "--user", "u", "--auth", "keypair", "--key-path", "/k"]) == 3


def test_ddl_subcommand_generates_sql_offline(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    rc = main(["ddl", "--out-dir", str(tmp_path)])
    assert rc == 0
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text(encoding="utf-8"))
    assert len(ddl["statements"]) == 1
    assert "CREATE TABLE IF NOT EXISTS" in ddl["statements"][0]["sql"]
    assert "USING DELTA" in ddl["statements"][0]["sql"]


def test_ddl_emits_views_too_after_their_tables(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V", object_type="VIEW",
                view_ddl_get_ddl="create view V as select ORDER_ID from D.PUBLIC.ORDERS")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json",
          {"edges": [{"from": "D.PUBLIC.V", "to": "D.PUBLIC.ORDERS"}],
           "source_used": "parsed_ddl", "coverage_note": "views only"})
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    ddl = json.loads((tmp_path / "ddl_plan.json").read_text(encoding="utf-8"))
    idents = [s["source_identifier"] for s in ddl["statements"]]
    assert idents == ["D.PUBLIC.ORDERS", "D.PUBLIC.V"], "view emitted last"
    assert "CREATE VIEW IF NOT EXISTS" in ddl["statements"][1]["sql"]


def test_ddl_blocks_a_snowflake_only_view_with_a_reason(tmp_path):
    inv = json.loads(json.dumps(INV))
    view = json.loads(json.dumps(inv["inventory"][0]))
    view.update(source_identifier="D.PUBLIC.V2", object_type="VIEW",
                view_ddl_get_ddl="create view V2 as select * from t "
                                 "qualify row_number() over (order by a) = 1")
    inv["inventory"].append(view)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert any("QUALIFY" in c["reason"] for c in plan["cannot_migrate"])


def test_deploy_defaults_to_dry_run(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 0
    res = json.loads((tmp_path / "deploy_result.json").read_text(encoding="utf-8"))
    assert res["dry_run"] is True and res["executed"] == 0
    assert "DRY RUN" in (tmp_path / "SOFT_CLONE_SUMMARY.md").read_text(encoding="utf-8").upper()


def test_deploy_execute_without_coordinates_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["deploy", "--out-dir", str(tmp_path), "--execute"])
    assert rc == 1
    assert "ask the user" in capsys.readouterr().err


def test_missing_input_artifact_is_a_clear_error(tmp_path, capsys):
    assert main(["plan", "--out-dir", str(tmp_path)]) == 1
    assert "inventory.json" in capsys.readouterr().err


def test_snowflake_and_aidp_run_sql_helpers_do_not_shadow_each_other():
    # Both modules export make_run_sql. An unqualified import of the second
    # shadowed the first and broke every Snowflake stage at runtime while every
    # unit test still passed, because the tests patch _assess_inventory.
    import inspect

    import snowmig
    from snowflake_source.conn import make_run_sql as sf_make

    assert snowmig.make_run_sql is sf_make, "the Snowflake helper must win"
    assert "backend" in inspect.signature(snowmig.make_aidp_run_sql).parameters


def test_run_sql_from_args_builds_a_snowflake_callable(tmp_path, monkeypatch):
    # Covers the path the patched-out tests skip.
    import snowmig
    captured = {}

    monkeypatch.setattr(snowmig, "build_connect_kwargs",
                        lambda *a, **k: {"account": "a"})
    monkeypatch.setattr(snowmig, "connect", lambda **kw: captured.setdefault("kw", kw))
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: "CALLABLE")

    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--account", "a", "--user", "u",
         "--auth", "keypair", "--key-path", "/k"])
    assert snowmig._run_sql_from_args(args) == "CALLABLE"
    assert captured["kw"] == {"account": "a"}


def _plan_with_two_catalogs(tmp_path):
    inv = json.loads(json.dumps(INV))
    second = json.loads(json.dumps(inv["inventory"][0]))
    second.update(source_identifier="D2.PUBLIC.ORDERS", source_database="D2")
    inv["inventory"].append(second)
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])


def test_notebook_refuses_to_pick_a_catalog_when_the_plan_spans_several(
        tmp_path, capsys):
    _plan_with_two_catalogs(tmp_path)
    rc = main(["notebook", "--out-dir", str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "pass --catalog" in err
    assert "not assumed" in err


def test_notebook_accepts_an_explicit_catalog_choice(tmp_path):
    _plan_with_two_catalogs(tmp_path)
    assert main(["notebook", "--out-dir", str(tmp_path), "--catalog", "D2"]) == 0
    assert (tmp_path / "snowmig_shallow_clone_D2.ipynb").is_file()


def test_notebook_needs_no_catalog_flag_when_there_is_only_one(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["notebook", "--out-dir", str(tmp_path)]) == 0


def test_data_options_stage_presents_options_and_implements_nothing(tmp_path):
    assert main(["data-options", "--out-dir", str(tmp_path)]) == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))
    assert payload["implemented"] is False
    assert len(payload["options"]) >= 3
    md = (tmp_path / "DATA_MOVEMENT_OPTIONS.md").read_text(encoding="utf-8")
    # `implemented: False` describes the CLI. The one path the data plane
    # does implement is named, so the flag cannot be read as "no rows move".
    assert "control-plane cli moves no bytes" in md.lower()
    assert "snowmig_02_copy_schema" in md


def test_data_options_records_a_choice_without_executing(tmp_path):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A2_FEDERATE_EXTERNAL_CATALOG",
               "--chosen-by", "navid", "--rationale", "no bulk transfer yet"])
    assert rc == 0
    payload = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))
    assert payload["choice"]["executed"] is False
    assert payload["choice"]["unknowns_outstanding"]


def test_data_options_choice_requires_a_rationale(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A1_UNLOAD_OBJECT_STORAGE"])
    assert rc == 1
    assert "rationale" in capsys.readouterr().err


def test_plan_always_reports_the_architecture_state(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    assert "architecture: UNDECIDED" in capsys.readouterr().out
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "Data-movement architecture" in md
    assert "A1_UNLOAD_OBJECT_STORAGE" in md and "A5_HYBRID_WAVES" in md


def test_plan_picks_up_a_recorded_architecture_choice(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A2_FEDERATE_EXTERNAL_CATALOG",
          "--chosen-by", "navid", "--rationale", "federate first"])
    main(["plan", "--out-dir", str(tmp_path)])
    assert "architecture: A2_FEDERATE_EXTERNAL_CATALOG" in capsys.readouterr().out
    plan = json.loads((tmp_path / "plan.json").read_text(encoding="utf-8"))
    assert plan["architecture_choice"]["option_id"] == "A2_FEDERATE_EXTERNAL_CATALOG"
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "federate first" in md
    assert "✅" in md, "the chosen option is marked in the table"


def test_data_options_records_a_deferral(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
               "--rationale", "platform team decides next month"])
    assert rc == 0
    assert "DEFERRED" in capsys.readouterr().out
    choice = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))["choice"]
    assert choice["deferred"] is True and choice["custom_architecture"] is None


def test_data_options_records_a_customer_architecture_verbatim(tmp_path):
    desc = tmp_path / "arch.md"
    desc.write_text("Debezium off Snowflake into OCI Streaming, then Iceberg.\n", encoding="utf-8")
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
               "--rationale", "their team already runs this",
               "--custom-name", "Kafka CDC into Iceberg",
               "--custom-description-file", str(desc)])
    assert rc == 0
    choice = json.loads((tmp_path / "data_options.json").read_text(encoding="utf-8"))["choice"]
    assert choice["custom_architecture"]["name"] == "Kafka CDC into Iceberg"
    assert "Debezium" in choice["custom_architecture"]["description"]
    assert choice["deferred"] is False


def test_a_custom_architecture_needs_both_flags(tmp_path, capsys):
    rc = main(["data-options", "--out-dir", str(tmp_path),
               "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "x",
               "--rationale", "y", "--custom-name", "N"])
    assert rc == 1
    assert "custom-description-file" in capsys.readouterr().err


def test_a_deferral_flows_into_the_plan_reports(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "navid",
          "--rationale", "decide later"])
    main(["plan", "--out-dir", str(tmp_path)])
    md = (tmp_path / "PLANNED_OBJECTS.md").read_text(encoding="utf-8")
    assert "deliberately deferred" in md.lower()
    assert "A6_CUSTOMER_DEFINED" in md
    assert "does not have to be one of the others" in md


def test_plan_stdout_distinguishes_deferred_from_undecided(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    assert "UNDECIDED" in capsys.readouterr().out

    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "n",
          "--rationale", "later"])
    main(["plan", "--out-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "DEFERRED by the customer — not a gap" in out
    assert "UNDECIDED" not in out


def test_plan_stdout_names_a_custom_architecture(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "a.md").write_text("their design", encoding="utf-8")
    main(["data-options", "--out-dir", str(tmp_path),
          "--choose", "A6_CUSTOMER_DEFINED", "--chosen-by", "n",
          "--rationale", "r", "--custom-name", "Their Pattern",
          "--custom-description-file", str(tmp_path / "a.md")])
    main(["plan", "--out-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "Their Pattern" in out and "not assessed" in out


def test_deps_warns_when_views_lack_account_usage_lineage(tmp_path, monkeypatch,
                                                          capsys):
    # OBJECT_DEPENDENCIES is readable but has not caught up with a fresh view.
    # The operator running `deps` must be told on stderr, not left to read
    # dependencies.json to find out that the lineage is not authoritative.
    import snowmig
    from fake_sql import FakeSql
    inv = dict(INV)
    inv["inventory"] = INV["inventory"] + [{
        "source_identifier": "D.PUBLIC.V", "object_type": "VIEW",
        "source_database": "D", "source_schema": "PUBLIC",
        "compatibility_status": "supported", "blocked_reasons": [],
        "warnings": [], "columns": [],
        "view_ddl_get_ddl": "create view V as select * from D.PUBLIC.ORDERS"}]
    write(tmp_path, "inventory.json", inv)
    monkeypatch.setattr(snowmig, "_run_sql_from_args",
                        lambda args: FakeSql({"object_dependencies": []}))
    rc = main(["deps", "--out-dir", str(tmp_path)])
    assert rc == 0, "a warning, not a halt: the parsed edges make the plan right"
    assert "no ACCOUNT_USAGE lineage edge" in capsys.readouterr().err
    deps = json.loads((tmp_path / "dependencies.json").read_text(encoding="utf-8"))
    assert deps["source_used"] == "account_usage_empty"
    assert deps["edges"][0]["source"] == "parsed_ddl"


def test_empty_include_allowlist_exits_1_and_writes_no_plan(tmp_path, capsys):
    # `{"include_objects": []}` used to plan the whole estate and print the
    # allowlist under "Restrictions in force".
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text(json.dumps({"include_objects": []}), encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    assert "include_objects" in capsys.readouterr().err
    assert not (tmp_path / "plan.json").exists()


def test_smoke_without_target_coordinates_is_partial_and_exits_1(tmp_path, monkeypatch, capsys):
    # The example config ships workspace/cluster_id/catalog commented out, so
    # this is the default first run: only Snowflake gets checked. That used
    # to print `verdict: PASS` and exit 0.
    import snowmig
    from tests.test_smoke import sf_ok
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: sf_ok)
    monkeypatch.setattr(snowmig, "_aidp_from_config", lambda args: {})
    rc = main(["smoke", "--out-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 1
    assert "PARTIAL" in out and "verdict: PASS" not in out
    smoke = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["verdict"] == "PARTIAL"
    md = (tmp_path / "SMOKE_TEST.md").read_text(encoding="utf-8")
    assert "Verdict: **PASS**" not in md and "PARTIAL" in md


# --- --out-dir placement and `clean` --------------------------------------

def test_out_dir_is_honoured_before_and_after_the_subcommand(tmp_path):
    # The top-level usage line advertises `snowmig [--out-dir X] <stage>`, and
    # the subparser used to re-apply its own None default over the root value,
    # so that placement was silently discarded.
    import snowmig
    chosen = str(tmp_path / "chosen")
    after = snowmig.build_parser().parse_args(["stages", "--out-dir", chosen])
    before = snowmig.build_parser().parse_args(["--out-dir", chosen, "stages"])
    assert after.out_dir == chosen
    assert before.out_dir == chosen


def _clean_fixture(tmp_path, monkeypatch):
    import snowmig
    work = tmp_path / "work"
    default = work / snowmig.ARTIFACTS_DIRNAME
    default.mkdir(parents=True)
    (default / "plan.json").write_text("{}", encoding="utf-8")
    chosen = tmp_path / "chosen"
    chosen.mkdir()
    (chosen / "x").write_text("x", encoding="utf-8")
    monkeypatch.chdir(work)
    return default, chosen


def test_clean_refuses_a_chosen_directory_regardless_of_flag_position(
        tmp_path, monkeypatch, capsys):
    default, chosen = _clean_fixture(tmp_path, monkeypatch)
    assert main(["clean", "--out-dir", str(chosen)]) == 1
    assert "refusing" in capsys.readouterr().err
    assert (default / "plan.json").is_file()
    # Same flag, before the subcommand: the same refusal, not a deletion of
    # the default directory the operator did not name.
    assert main(["--out-dir", str(chosen), "clean"]) == 1
    assert "refusing" in capsys.readouterr().err
    assert (default / "plan.json").is_file()
    assert (chosen / "x").is_file()


def test_clean_removes_only_the_default_directory(tmp_path, monkeypatch):
    default, chosen = _clean_fixture(tmp_path, monkeypatch)
    assert main(["clean"]) == 0
    assert not default.exists()
    assert (chosen / "x").is_file()
    # The working directory itself is the user's and stays, empty or not.
    assert default.parent.is_dir()


def test_clean_removes_a_demo_directory_only_with_its_marker(
        tmp_path, monkeypatch):
    default, _ = _clean_fixture(tmp_path, monkeypatch)
    demo = default.parent / "snowmig_demo"
    demo.mkdir()
    (demo / "emulation.json").write_text("{}", encoding="utf-8")
    other = default.parent / "snowmig_demo_enterprise"
    other.mkdir()
    (other / "mine.txt").write_text("x", encoding="utf-8")
    assert main(["clean"]) == 0
    assert not demo.exists()
    assert (other / "mine.txt").is_file()


def test_default_out_dir_is_the_working_directory(tmp_path, monkeypatch):
    import snowmig
    monkeypatch.chdir(tmp_path)
    assert snowmig.default_out_dir() == tmp_path / snowmig.ARTIFACTS_DIRNAME


def test_a_new_or_empty_out_dir_ignores_itself_but_a_full_one_is_left(
        tmp_path):
    import snowmig
    new = snowmig.prepare_out_dir(tmp_path / "snowmig_out")
    assert (new / ".gitignore").read_text(encoding="utf-8").endswith("*\n")
    assert not (new / "README.md").exists()
    full = tmp_path / "project"
    full.mkdir()
    (full / "notes.md").write_text("mine", encoding="utf-8")
    snowmig.prepare_out_dir(full)
    assert not (full / ".gitignore").exists()


# --- bad inputs are one `error:` line, never a traceback -------------------

def test_out_dir_that_is_a_file_exits_1_with_an_error_line(tmp_path, capsys):
    f = tmp_path / "afile.txt"
    f.write_text("not a directory", encoding="utf-8")
    assert main(["stages", "--out-dir", str(f)]) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "afile.txt" in err
    assert "Traceback" not in err


def test_list_shaped_restrictions_file_exits_1(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text("[1, 2]", encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error:" in err and "JSON object" in err
    assert "Traceback" not in err


def test_unparseable_restrictions_file_names_the_file(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    (tmp_path / "r.json").write_text("{not json", encoding="utf-8")
    rc = main(["plan", "--out-dir", str(tmp_path), "--restrictions",
               str(tmp_path / "r.json")])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error:" in err and "r.json" in err


def test_corrupt_artifact_names_the_file(tmp_path, capsys):
    (tmp_path / "inventory.json").write_text("{not json", encoding="utf-8")
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 1
    err = capsys.readouterr().err
    assert "error:" in err and "inventory.json" in err


# --- a dry run never overwrites an executed record -------------------------

EXECUTED_DEPLOY = {"dry_run": False, "executed": 4, "verified": 2,
                   "statement_count": 4, "poisoned_names": ["x"],
                   "failed_targets": ["y"], "failed": ["y"],
                   "mismatched_targets": []}


def test_dry_run_deploy_refuses_to_overwrite_an_executed_record(tmp_path, capsys):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    write(tmp_path, "deploy_result.json", EXECUTED_DEPLOY)
    (tmp_path / "PREFLIGHT.md").unlink(missing_ok=True)
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 1
    err = capsys.readouterr().err
    assert "EXECUTED" in err and "deploy_result.json" in err
    assert "--out-dir" in err
    kept = json.loads((tmp_path / "deploy_result.json").read_text(encoding="utf-8"))
    assert kept["dry_run"] is False and kept["poisoned_names"] == ["x"]
    # Re-reading PREFLIGHT.md is the reason people re-run a dry run; it is
    # still rendered before the refusal.
    assert (tmp_path / "PREFLIGHT.md").is_file()


def test_the_stage_board_still_shows_the_executed_deploy_after_a_refused_dry_run(
        tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    write(tmp_path, "deploy_result.json", EXECUTED_DEPLOY)
    assert main(["deploy", "--out-dir", str(tmp_path)]) == 1
    assert main(["stages", "--out-dir", str(tmp_path)]) == 0
    board = (tmp_path / "STAGES.md").read_text(encoding="utf-8")
    deploy_row = next(l for l in board.splitlines()
                      if l.startswith("| `deploy`"))
    assert "verified 2/4" in deploy_row
    assert "DRY RUN" not in deploy_row


def test_dry_run_provision_refuses_to_overwrite_an_executed_record(
        tmp_path, capsys):
    write(tmp_path, "provision_result.json",
          {"dry_run": False, "workspace": {"name": "w"},
           "steps": [{"step": "workspace", "action": "created",
                      "verified": True, "detail": "w"}]})
    rc = main(["provision", "--out-dir", str(tmp_path), "--workspace-name",
               "w", "--skip-libraries"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "EXECUTED" in err and "provision_result.json" in err
    kept = json.loads((tmp_path / "provision_result.json").read_text(encoding="utf-8"))
    assert kept["dry_run"] is False
    assert kept["steps"][0]["verified"] is True


# --- the two opt-in writes are gated by --execute like every other write ---

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"
TARGET_FLAGS = ["--datalake-ocid", OCID, "--workspace", "ws-fake",
                "--cluster-id", "cl-fake"]


def _sf_ok(sql, params=None):
    low = sql.lower()
    if "current_user" in low:
        return [{"U": "SVC", "A": "ORGACCT", "R": "AWS_US_EAST_2",
                 "ROLE": "READER"}]
    if "show databases" in low:
        return [{"name": "SALES_DB"}]
    if "information_schema" in low:
        return [{"N": 7}]
    return []


class _DestRecorder:
    """Catalog-API double for `smoke`: one INTERNAL catalog, creates visible."""

    def __init__(self):
        self.ops: list[str] = []
        self.schemas: list[str] = []

    def __call__(self, operation, **kw):
        self.ops.append(operation)
        if operation == "list_catalogs":
            return {"items": [{"displayName": "lake", "key": "lake",
                               "catalogType": "INTERNAL"}]}
        if operation == "list_schemas":
            return {"items": [{"key": f"lake.{s}"} for s in self.schemas]}
        if operation == "create_schema":
            self.schemas.append(kw["schema"])
            return {}
        if operation == "delete_schema":
            self.schemas.remove(kw["schema"])
            return {}
        raise AssertionError(operation)


def _smoke_env(monkeypatch):
    import snowmig
    rec = _DestRecorder()
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: _sf_ok)
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    monkeypatch.setattr(snowmig, "make_call",
                        lambda target, *, backend, **kw: rec)
    return rec


def test_smoke_write_probe_without_execute_issues_no_writes(
        tmp_path, monkeypatch, capsys):
    rec = _smoke_env(monkeypatch)
    rc = main(["smoke", "--write-probe", "--out-dir", str(tmp_path),
               "--account", "a", "--user", "u", "--auth", "password",
               "--password-path", "/p", *TARGET_FLAGS, "--catalog", "lake"])
    assert rc == 0
    assert "create_schema" not in rec.ops and "delete_schema" not in rec.ops
    out = capsys.readouterr().out
    assert "dry run" in out and "--execute" in out
    smoke = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["destination"]["write_verified"] is False
    assert "--execute" in smoke["destination"]["write_note"]


def test_smoke_write_probe_with_execute_creates_then_deletes(
        tmp_path, monkeypatch):
    rec = _smoke_env(monkeypatch)
    rc = main(["smoke", "--write-probe", "--execute", "--out-dir",
               str(tmp_path), "--account", "a", "--user", "u", "--auth",
               "password", "--password-path", "/p", *TARGET_FLAGS,
               "--catalog", "lake"])
    assert rc == 0
    assert rec.ops.index("create_schema") < rec.ops.index("delete_schema")
    smoke = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
    assert smoke["destination"]["write_verified"] is True


def _no_subprocess(monkeypatch):
    import subprocess

    def refuse(*a, **k):
        raise AssertionError("no CLI may be invoked from this test")
    monkeypatch.setattr(subprocess, "run", refuse)


def test_notebook_upload_without_execute_is_a_dry_run(
        tmp_path, monkeypatch, capsys):
    _no_subprocess(monkeypatch)
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["notebook", "--upload", "--out-dir", str(tmp_path),
               *TARGET_FLAGS, "--catalog", "d"])
    assert rc == 0
    assert (tmp_path / "snowmig_shallow_clone_d.ipynb").is_file()
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "dry run" in md.lower()
    assert "Uploaded to" not in md
    out = capsys.readouterr().out
    assert "dry run" in out and "--execute" in out


def test_notebook_upload_with_execute_is_refused_and_points_at_provision(
        tmp_path, monkeypatch, capsys):
    # The Jupyter-contents transport 200s and cannot read the file back
    #. Refusing is honest; "uploaded" was not.
    _no_subprocess(monkeypatch)
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    rc = main(["notebook", "--upload", "--execute", "--out-dir",
               str(tmp_path), *TARGET_FLAGS, "--catalog", "d"])
    assert rc == 1
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "provision" in err and "run" in err
    assert "aidp notebook run" not in err
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "Uploaded to" not in md and "aidp notebook run" not in md


def test_notebook_still_generates_offline_without_upload(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    main(["ddl", "--out-dir", str(tmp_path)])
    assert main(["notebook", "--out-dir", str(tmp_path)]) == 0
    md = (tmp_path / "NOTEBOOK.md").read_text(encoding="utf-8")
    assert "aidp notebook run" not in md


def test_every_writing_subcommand_accepts_execute():
    import snowmig
    parser = snowmig.build_parser()
    for argv in (["deploy", "--execute"],
                 ["provision", "--workspace-name", "w", "--execute"],
                 ["catalog", "--execute"],
                 ["smoke", "--write-probe", "--execute"],
                 ["notebook", "--upload", "--execute"]):
        assert parser.parse_args(argv).execute is True, argv


# --- ddl --timestamp-ntz: the mapping decision, made offline ----------------

NTZ_COLUMN = {"COLUMN_NAME": "CREATED_AT", "DATA_TYPE": "TIMESTAMP_NTZ",
              "target_type": "TIMESTAMP_NTZ", "IS_NULLABLE": "YES",
              "ORDINAL_POSITION": 2, "COMMENT": None}


def _inv_with_ntz(mode="preserve", target_type="TIMESTAMP_NTZ"):
    inv = json.loads(json.dumps(INV))
    col = dict(NTZ_COLUMN, target_type=target_type)
    inv["inventory"][0]["columns"].append(col)
    inv["timestamp_ntz_mode"] = mode
    return inv


def _plan_ntz(tmp_path, inv):
    write(tmp_path, "inventory.json", inv)
    write(tmp_path, "dependencies.json", DEPS)
    assert main(["plan", "--out-dir", str(tmp_path)]) == 0
    return (tmp_path / "inventory.json").read_bytes()


def _ddl_plan(tmp_path):
    return json.loads((tmp_path / "ddl_plan.json").read_text(encoding="utf-8"))


def test_ddl_halts_on_timestamp_ntz_and_names_the_offline_remedy(tmp_path, capsys):
    _plan_ntz(tmp_path, _inv_with_ntz())
    assert main(["ddl", "--out-dir", str(tmp_path)]) == 3
    err = capsys.readouterr().err
    assert "CREATED_AT -> TIMESTAMP_NTZ" in err
    assert "ddl --timestamp-ntz timestamp" in err
    assert _ddl_plan(tmp_path)["target_rejected"]


def test_ddl_timestamp_ntz_flag_remaps_offline_without_touching_inventory(
        tmp_path, capsys):
    before = _plan_ntz(tmp_path, _inv_with_ntz())
    rc = main(["ddl", "--out-dir", str(tmp_path), "--timestamp-ntz", "timestamp"])
    assert rc == 0
    plan = _ddl_plan(tmp_path)
    stmt = plan["statements"][0]
    assert "`CREATED_AT` TIMESTAMP\n" in stmt["sql"]
    assert "TIMESTAMP_NTZ" not in stmt["sql"]
    # expected_columns also carries `nullable` and `description` (the
    # properties the reviewed SQL shows), so this asserts the re-map, not the
    # whole spec.
    assert {"name": "CREATED_AT", "type": "TIMESTAMP"}.items() <= next(
        c for c in stmt["expected_columns"]
        if c["name"] == "CREATED_AT").items()
    caveats = [w for w in stmt["warnings"] if w.startswith("CREATED_AT:")]
    assert len(caveats) == 1 and "timezone" in caveats[0].lower()
    assert plan["timestamp_ntz_mode"] == "timestamp"
    assert plan["remapped_columns"] == ["D.PUBLIC.ORDERS.CREATED_AT"]
    assert plan["target_rejected"] == []
    # inventory.json is the record of what `assess` observed; it is not
    # rewritten by a mapping decision taken at `ddl`.
    assert (tmp_path / "inventory.json").read_bytes() == before
    assert "re-mapped 1" in capsys.readouterr().out


def test_ddl_timestamp_ntz_preserve_never_reupgrades(tmp_path, capsys):
    _plan_ntz(tmp_path, _inv_with_ntz(mode="timestamp", target_type="TIMESTAMP"))
    rc = main(["ddl", "--out-dir", str(tmp_path), "--timestamp-ntz", "preserve"])
    assert rc == 0
    stmt = _ddl_plan(tmp_path)["statements"][0]
    assert "`CREATED_AT` TIMESTAMP\n" in stmt["sql"]
    assert "not re-upgraded" in capsys.readouterr().err


def test_ddl_remap_is_a_noop_when_no_ntz_columns(tmp_path):
    write(tmp_path, "inventory.json", INV)
    write(tmp_path, "dependencies.json", DEPS)
    main(["plan", "--out-dir", str(tmp_path)])
    rc = main(["ddl", "--out-dir", str(tmp_path), "--timestamp-ntz", "timestamp"])
    assert rc == 0
    assert _ddl_plan(tmp_path)["remapped_columns"] == []


# --- accepted `aidp:` keys are consumed, not just accepted -----------------

def _cwd_config(tmp_path, monkeypatch, aidp_lines):
    """A discoverable config in the working directory: `provision` and
    `catalogs` take no --config flag, so this is how the file reaches them."""
    cfg = tmp_path / "snowmig-config.yaml"
    body = ("snowflake:\n  account: ORG-ACC\n  user: SVC\n  warehouse: WH\n"
            "  database: SALES_DB\n  auth: password\n"
            "  password: not-a-real-password\naidp:\n")
    body += "".join(f"  {line}\n" for line in aidp_lines)
    cfg.write_text(body, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return cfg


def test_the_profile_and_auth_mode_reach_both_clis(
        tmp_path, monkeypatch, capsys):
    """Live 2026-09-22: the `aidp` CLI takes `-p/--profile` and `--auth`
    (api_key | security_token | instance_principal | resource_principal), and
    its own default is security_token. Forcing api_key on it and passing
    nothing to `oci` makes every call on a session profile a 401 that reads
    as a permissions problem."""
    import subprocess
    import types

    import snowmig
    _cwd_config(tmp_path, monkeypatch,
                [f"datalake_ocid: {OCID}", "oci_profile: FAKE_PROFILE",
                 "oci_auth: security_token"])
    seen = []

    def fake_run(argv, **kw):
        seen.append(list(argv))
        return types.SimpleNamespace(returncode=0, stdout="[]", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)

    args = snowmig.build_parser().parse_args(["catalogs", "--out-dir", str(tmp_path)])
    runner = snowmig._oci_runner(args)
    runner(["oci", "raw-request", "--http-method", "GET",
            "--target-uri", "https://example.invalid/x"])
    runner(["aidp", "workspace", "list", "--auth", "api_key"])
    assert seen[0][:5] == ["oci", "--auth", "security_token",
                           "--profile", "FAKE_PROFILE"]
    assert seen[0][5:] == ["raw-request", "--http-method", "GET",
                           "--target-uri", "https://example.invalid/x"]
    # An --auth the transport already appended is CORRECTED, never doubled.
    assert seen[1].count("--auth") == 1
    assert "api_key" not in seen[1]
    # An --auth the transport already appended is corrected where it stands.
    assert seen[1] == ["aidp", "--profile", "FAKE_PROFILE", "workspace",
                       "list", "--auth", "security_token"]
    out = capsys.readouterr().out
    assert "FAKE_PROFILE" in out and "security_token" in out


def test_the_auth_mode_is_read_from_the_profile_when_the_config_is_silent(
        tmp_path, monkeypatch):
    """A profile naming a security_token_file is a session profile. Only the
    key NAMES are read; no credential is loaded to decide this."""
    import snowmig
    oci_dir = tmp_path / "oci"
    oci_dir.mkdir()
    (oci_dir / "config").write_text(
        "[DEFAULT]\nuser=ocid1.user.oc1..x\nkey_file=~/.oci/k.pem\n\n"
        "[SESSIONY]\nsecurity_token_file=~/.oci/token\n", encoding="utf-8")
    monkeypatch.setenv("OCI_CONFIG_FILE", str(oci_dir / "config"))
    assert snowmig._profile_auth_mode("SESSIONY") == "security_token"
    assert snowmig._profile_auth_mode("DEFAULT") == "api_key"
    assert snowmig._profile_auth_mode("ABSENT") == "api_key"


def test_the_shell_profile_and_auth_are_honoured_when_the_config_is_silent(
        tmp_path, monkeypatch):
    import snowmig
    oci_dir = tmp_path / "oci"
    oci_dir.mkdir()
    (oci_dir / "config").write_text(
        "[DEFAULT]\nuser=ocid1.user.oc1..x\nkey_file=~/.oci/k.pem\n\n"
        "[SESS]\nsecurity_token_file=~/.oci/token\n", encoding="utf-8")
    monkeypatch.setenv("OCI_CONFIG_FILE", str(oci_dir / "config"))
    _cwd_config(tmp_path, monkeypatch, [f"datalake_ocid: {OCID}"])
    monkeypatch.setenv("OCI_CLI_PROFILE", "SESS")
    monkeypatch.delenv("OCI_CLI_AUTH", raising=False)
    args = snowmig.build_parser().parse_args(["catalogs", "--out-dir", str(tmp_path)])
    assert snowmig._oci_auth_mode(args) == "security_token"
    monkeypatch.setenv("OCI_CLI_AUTH", "api_key")
    args = snowmig.build_parser().parse_args(["catalogs", "--out-dir", str(tmp_path)])
    assert snowmig._oci_auth_mode(args) == "api_key"


def test_a_cli_child_never_inherits_our_interpreter_variables(
        tmp_path, monkeypatch):
    """Live 2026-09-22: a venv built from Microsoft Store Python exports
    PYTHONUSERBASE. Inherited by the `oci` CLI it repoints that CLI's own
    interpreter, which then dies with `ModuleNotFoundError: No module named
    'cryptography'` -- reported as "list_catalogs failed (exit 1)" plus a
    traceback, which reads as a broken OCI install."""
    import subprocess
    import types

    import snowmig
    _cwd_config(tmp_path, monkeypatch, [f"datalake_ocid: {OCID}"])
    for name in ("PYTHONUSERBASE", "PYTHONPATH", "PYTHONHOME"):
        monkeypatch.setenv(name, "poison")
    monkeypatch.setenv("PATH", "keep-me")
    captured = {}

    def fake_run(argv, **kw):
        captured["env"] = kw.get("env")
        return types.SimpleNamespace(returncode=0, stdout="[]", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)

    args = snowmig.build_parser().parse_args(["catalogs", "--out-dir", str(tmp_path)])
    runner = snowmig._oci_runner(args)
    assert runner is not None, "the env hygiene applies with or without a profile"
    runner(["oci", "--version"])
    env = captured["env"]
    assert env is not None, "the child must be given a cleaned environment"
    for name in ("PYTHONUSERBASE", "PYTHONPATH", "PYTHONHOME"):
        assert name not in env, name
    assert env.get("PATH") == "keep-me", "only the interpreter vars are dropped"


def test_catalogs_hands_the_profile_runner_to_the_transport(tmp_path, monkeypatch):
    import snowmig
    _cwd_config(tmp_path, monkeypatch,
                [f"datalake_ocid: {OCID}", "oci_profile: FAKE_PROFILE"])
    captured = {}

    def fake_make_call(target, *, backend, run_process=None):
        captured["run_process"] = run_process
        return lambda operation, **kw: {"items": []}
    monkeypatch.setattr(snowmig, "make_call", fake_make_call)
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    assert main(["catalogs", "--out-dir", str(tmp_path)]) == 0
    assert captured["run_process"] is not None


class _CallCaptured(Exception):
    pass


@pytest.mark.parametrize("stage", ["teardown", "publish", "stage-publish"])
def test_teardown_and_publish_state_the_profile_and_auth_mode(
        tmp_path, monkeypatch, stage):
    """Teardown, publish and the per-stage publish built their transport
    without the runner, so on a session-token or non-DEFAULT profile they
    failed while every other stage worked."""
    import subprocess
    import types

    import snowmig
    import target.provisioning as provisioning
    _cwd_config(tmp_path, monkeypatch,
                [f"datalake_ocid: {OCID}", "workspace: ws-key",
                 "oci_profile: FAKE_PROFILE", "oci_auth: security_token"])
    if stage == "stage-publish":
        with (tmp_path / "snowmig-config.yaml").open("a", encoding="utf-8") as fh:
            fh.write("reporting:\n  publish_each_stage: true\n")
    write(tmp_path, "provision_result.json",
          {"dry_run": False, "workspace": {"key": "ws-key"}})
    captured = {}

    def fake_make_call(ocid, *, backend="oci_raw", run_process=None):
        captured["run_process"] = run_process
        raise _CallCaptured
    monkeypatch.setattr(provisioning, "make_provision_call", fake_make_call)
    seen = []
    monkeypatch.setattr(subprocess, "run", lambda argv, **kw: (
        seen.append(list(argv))
        or types.SimpleNamespace(returncode=0, stdout="{}", stderr="")))

    out = ["--out-dir", str(tmp_path)]
    if stage == "stage-publish":
        monkeypatch.delenv("SNOWMIG_NO_STAGE_PUBLISH", raising=False)
        args = snowmig.build_parser().parse_args(["teardown", *out])
        snowmig._publish_stage(args)
    else:
        try:
            main([stage, *out, "--execute"])
        except _CallCaptured:
            pass
    run_process = captured.get("run_process")
    assert run_process is not None, f"{stage} built its call without the runner"
    run_process(["oci", "raw-request", "--http-method", "GET"])
    assert seen[-1][:5] == ["oci", "--auth", "security_token",
                            "--profile", "FAKE_PROFILE"]


def test_provision_dry_run_takes_the_catalogs_from_the_config(
        tmp_path, monkeypatch, capsys):
    _cwd_config(tmp_path, monkeypatch,
                [f"datalake_ocid: {OCID}", "external_catalog: cfg_external_cat",
                 "target_catalog: cfg_target_cat",
                 "subnet_id: ocid1.subnet.oc1.iad.fakesubnet"])
    rc = main(["provision", "--workspace-name", "w", "--out-dir", str(tmp_path),
               "--skip-libraries"])
    assert rc == 0
    res = json.loads((tmp_path / "provision_result.json").read_text(encoding="utf-8"))
    assert res["external_catalog"] == "cfg_external_cat"
    assert res["target_catalog"] == "cfg_target_cat"
    out = capsys.readouterr().out
    assert "external_catalog=cfg_external_cat" in out
    assert "target_catalog=cfg_target_cat" in out
    assert "subnet_id=ocid1.subnet.oc1.iad.fakesubnet (not used by any stage yet)" in out


def test_provision_flag_overrides_the_config_catalog(tmp_path, monkeypatch):
    _cwd_config(tmp_path, monkeypatch,
                [f"datalake_ocid: {OCID}", "external_catalog: cfg_external_cat",
                 "target_catalog: cfg_target_cat"])
    rc = main(["provision", "--workspace-name", "w", "--out-dir", str(tmp_path),
               "--skip-libraries", "--target-catalog", "flag_cat"])
    assert rc == 0
    res = json.loads((tmp_path / "provision_result.json").read_text(encoding="utf-8"))
    assert res["target_catalog"] == "flag_cat"
    assert res["external_catalog"] == "cfg_external_cat"


# --- no Snowflake secret is ever taken inline on the command line ----------

def _every_option_string():
    import snowmig
    subs = snowmig.build_parser()._subparsers._group_actions[0].choices
    return {o for sp in subs.values() for a in sp._actions
            for o in a.option_strings}


def test_no_stage_accepts_a_secret_inline():
    # PRIVACY.md: secrets are read from files named by path, never as
    # inline arguments. Any secret-looking flag must therefore be a *-path.
    secretish = {o for o in _every_option_string()
                 if any(w in o for w in ("pass", "token", "secret",
                                         "private-key", "pat-"))}
    assert secretish, "the guard found no secret-looking flags at all"
    assert all(o.endswith("-path") for o in secretish), sorted(secretish)
    assert "--key-passphrase" not in _every_option_string()


def test_the_removed_passphrase_flag_is_refused_and_names_the_config_keys(
        tmp_path, capsys):
    rc = main(["assess", "--out-dir", str(tmp_path), "--account", "a",
               "--user", "u", "--key-path", "/k", "--key-passphrase",
               "NOT-A-REAL-PASSPHRASE"])
    assert rc == 1
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "key_passphrase_path" in err and "key_passphrase" in err
    assert "NOT-A-REAL-PASSPHRASE" not in err, "the value is never echoed"
    # The = spelling is the same leak.
    rc = main(["assess", "--out-dir", str(tmp_path), "--account", "a",
               "--user", "u", "--key-path", "/k",
               "--key-passphrase=NOT-A-REAL-PASSPHRASE"])
    assert rc == 1
    assert "NOT-A-REAL-PASSPHRASE" not in capsys.readouterr().err


def _capture_connect_kwargs(monkeypatch):
    import snowmig
    captured = {}

    def fake_build(auth, **kw):
        captured.update(kw)
        return {}
    monkeypatch.setattr(snowmig, "build_connect_kwargs", fake_build)
    monkeypatch.setattr(snowmig, "connect", lambda **kw: "CONN")
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: "CALLABLE")
    return captured


def _keypair_config(tmp_path, *extra_lines):
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("snowflake:\n  account: ORG-ACC\n  user: SVC\n"
                   "  auth: keypair\n  key_path: /k\n"
                   + "".join(f"  {line}\n" for line in extra_lines),
                   encoding="utf-8")
    return str(cfg)


def test_key_passphrase_is_read_from_the_config_inline(tmp_path, monkeypatch):
    import snowmig
    captured = _capture_connect_kwargs(monkeypatch)
    cfg = _keypair_config(tmp_path, "key_passphrase: from-config")
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", cfg])
    assert snowmig._run_sql_from_args(args) == "CALLABLE"
    assert captured["key_passphrase"] == "from-config"


def test_key_passphrase_is_read_from_the_file_the_config_names(
        tmp_path, monkeypatch):
    import snowmig
    captured = _capture_connect_kwargs(monkeypatch)
    pp = tmp_path / "pp"
    pp.write_text("  from-file\n", encoding="utf-8")
    cfg = _keypair_config(tmp_path, f"key_passphrase_path: {pp}")
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", cfg])
    snowmig._run_sql_from_args(args)
    assert captured["key_passphrase"] == "from-file"


# --- host from the config reaches the laptop-side connector ----------------

def test_run_sql_from_args_threads_host_from_config(tmp_path, monkeypatch):
    # The same `host:` is what the EXTERNAL catalog body registers, so a
    # preflight that ignored it PASSed against a different endpoint than
    # AIDP would then use -- and a wrong host surfaced minutes into a job.
    import snowmig
    from migration_config import load_config, snowflake_block
    from target.snowflake_catalog_connection import (
        build_snowflake_connection_details)
    captured = _capture_connect_kwargs(monkeypatch)
    host = "ORG-ACC.us-east-2.aws.snowflakecomputing.com"
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(f"snowflake:\n  account: ORG-ACC\n  host: {host}\n"
                   "  user: SVC\n  warehouse: WH\n  database: SALES_DB\n"
                   "  auth: password\n  password: not-a-real-password\n",
                   encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["preflight", "--test-source", "--config", str(cfg),
         "--out-dir", str(tmp_path)])
    snowmig._run_sql_from_args(args)
    assert captured["host"] == host
    registered = build_snowflake_connection_details(
        snowflake_block(load_config(cfg)))["SNOWFLAKE_HOST"]
    assert captured["host"] == registered, "preflight and catalog must agree"


def test_run_sql_from_args_leaves_host_unset_when_the_config_has_none(
        tmp_path, monkeypatch):
    import snowmig
    captured = _capture_connect_kwargs(monkeypatch)
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("snowflake:\n  account: ORG-ACC\n  user: SVC\n"
                   "  auth: password\n  password: not-a-real-password\n",
                   encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["assess", "--out-dir", str(tmp_path), "--config", str(cfg)])
    snowmig._run_sql_from_args(args)
    assert not captured.get("host")


# --- security: the console line hedges like SECURITY.md does ---------------

def test_security_console_hedges_when_a_policy_exists_but_no_attachment_shows(
        tmp_path, monkeypatch, capsys):
    # SHOW lists a masking policy; ACCOUNT_USAGE.POLICY_REFERENCES (which lags
    # ~2 h) shows nothing attached, and the per-object read that would settle
    # it is denied to this role. SECURITY.md and the board call that
    # UNCONFIRMED; the console printed a bare "0 policy exposure(s)".
    import snowmig
    write(tmp_path, "inventory.json", INV)

    def fake(sql, params=None):
        low = " ".join(sql.split()).lower()
        if "information_schema.policy_references" in low:
            raise RuntimeError("Insufficient privileges")
        if "masking policies" in low:
            return [{"name": "MASK_SSN", "database_name": "D",
                     "schema_name": "PUBLIC", "kind": "MASKING_POLICY"}]
        return []

    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: fake)
    rc = main(["security", "--out-dir", str(tmp_path), "--no-grants"])
    out, err = capsys.readouterr()
    assert rc == 0
    assert "0 policy exposure(s)" in out
    assert "UNCONFIRMED" in err and "POLICY_REFERENCES" in err
    assert "could not be read directly" in err


def test_security_console_says_when_the_answer_carries_no_lag(
        tmp_path, monkeypatch, capsys):
    """The opposite case, and the one that should now be normal: every object
    was read directly, so the operator is told the count is current."""
    import snowmig
    write(tmp_path, "inventory.json", INV)
    monkeypatch.setattr(snowmig, "_run_sql_from_args",
                        lambda args: (lambda sql, params=None: []))
    rc = main(["security", "--out-dir", str(tmp_path), "--no-grants"])
    out, err = capsys.readouterr()
    assert rc == 0
    assert "read per object" in out
    assert "UNCONFIRMED" not in err


def test_security_console_stays_quiet_when_nothing_is_defined(
        tmp_path, monkeypatch, capsys):
    import snowmig
    write(tmp_path, "inventory.json", INV)
    monkeypatch.setattr(snowmig, "_run_sql_from_args",
                        lambda args: (lambda sql, params=None: []))
    rc = main(["security", "--out-dir", str(tmp_path), "--no-grants"])
    out, err = capsys.readouterr()
    assert rc == 0 and "0 policy exposure(s)" in out
    assert "UNCONFIRMED" not in err and "UNCORROBORATED" not in err


# --------------------------- an inline PAT, like an inline password or key
#
# `_snowflake_coords` resolved
# `password` and `private_key` from either an inline value or a path, but a
# PAT only from `pat_path`. The config already lists `token` as a secret
# field, so an inline one was accepted, validated and redacted -- and then
# ignored at connect time, which reads to the operator as "the PAT is wrong".

_PAT_CONFIG = (
    "snowflake:\n"
    "  account: AC\n"
    "  user: U\n"
    "  auth: pat\n"
    "  token: the-pat-value\n"
    "  database: D\n"
)


def test_an_inline_token_is_resolved_like_any_other_secret(tmp_path):
    import snowmig
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_PAT_CONFIG, encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", str(cfg)])
    coords = snowmig._snowflake_coords(args)
    assert coords["token"] == "the-pat-value"


def test_an_inline_token_reaches_the_connector(tmp_path, monkeypatch):
    """conn.py reads credentials from paths by design, so an inline secret
    is spooled to a temp file for the life of the call -- exactly the
    treatment an inline password already gets."""
    import snowmig
    seen = {}

    def fake_build(auth, **kw):
        seen.update(kw)
        seen["auth"] = auth
        return {}

    monkeypatch.setattr(snowmig, "build_connect_kwargs", fake_build)
    monkeypatch.setattr(snowmig, "connect", lambda **kw: object())
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: (lambda *a, **k: []))
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_PAT_CONFIG, encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", str(cfg)])
    snowmig._run_sql_from_args(args)
    assert seen["auth"] == "pat"
    assert seen["pat_path"], "an inline token has to reach the connector"
    assert pathlib.Path(seen["pat_path"]).name.startswith("snowmig_secret_")


def test_the_spooled_token_file_is_removed_after_the_call(tmp_path,
                                                          monkeypatch):
    import snowmig
    seen = {}

    def fake_build(auth, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(snowmig, "build_connect_kwargs", fake_build)
    monkeypatch.setattr(snowmig, "connect", lambda **kw: object())
    monkeypatch.setattr(snowmig, "make_run_sql", lambda conn: (lambda *a, **k: []))
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_PAT_CONFIG, encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", str(cfg)])
    snowmig._run_sql_from_args(args)
    assert not pathlib.Path(seen["pat_path"]).exists(), \
        "a spooled secret may not outlive the call"


def test_a_pat_path_still_works_unchanged(tmp_path):
    import snowmig
    pat = tmp_path / "pat.txt"
    pat.write_text("from-a-file", encoding="utf-8")
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "snowflake:\n  account: AC\n  user: U\n  auth: pat\n"
        f"  pat_path: {pat.as_posix()}\n  database: D\n", encoding="utf-8")
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", str(cfg)])
    coords = snowmig._snowflake_coords(args)
    assert coords["pat_path"] == pat.as_posix()
    assert coords["token"] is None


# ------------- an explicit --auth wins over the config, however it is spelled
#
# Found on review. `--auth` defaulted to "keypair", so _snowflake_coords
# guessed whether the flag had been typed with `"--auth" not in sys.argv`.
# `--auth=keypair` and the prefix `--au keypair` never put that literal
# token in argv, and main(argv) does not set sys.argv at all -- so with
# `auth: password` in the config, a one-run `--auth=keypair --key-path ...`
# silently connected with the password, against the --config help ("Any
# flag below overrides what it says").

_PASSWORD_CONFIG = ("snowflake:\n  account: AC\n  user: U\n  auth: password\n"
                    "  password: not-a-real-password\n  database: D\n")


@pytest.mark.parametrize("spelling", [["--auth", "keypair"],
                                      ["--auth=keypair"],
                                      ["--au", "keypair"]],
                         ids=["separate", "equals", "prefix"])
def test_an_explicit_auth_flag_overrides_the_config(tmp_path, monkeypatch,
                                                    spelling):
    import snowmig
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(_PASSWORD_CONFIG, encoding="utf-8")
    # What the process was started with is not what main(argv) parses.
    monkeypatch.setattr(sys, "argv", ["snowmig"])
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", str(cfg), *spelling])
    assert snowmig._snowflake_coords(args)["auth"] == "keypair"


def test_without_the_flag_the_config_auth_holds_and_keypair_is_the_default(
        tmp_path, monkeypatch):
    import snowmig
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(_PASSWORD_CONFIG, encoding="utf-8")
    args = snowmig.build_parser().parse_args(["assess", "--config", str(cfg)])
    assert snowmig._snowflake_coords(args)["auth"] == "password"
    bare = tmp_path / "bare.yaml"
    bare.write_text("snowflake:\n  account: AC\n  user: U\n", encoding="utf-8")
    args = snowmig.build_parser().parse_args(["assess", "--config", str(bare)])
    assert snowmig._snowflake_coords(args)["auth"] == "keypair"


# ------------- exit 3 is "resolve with the user", and the docs say which kinds
#
# Found on review (contested; kept as a doc-versus-code defect). `ddl` exits
# 3 -- deliberately -- when a column uses a type the target refuses at
# CREATE TABLE, and on a default-assessed estate that is TIMESTAMP_NTZ on
# nearly every table: assess rc 0, no collisions, ddl rc 3. Every stated
# contract said 3 means a collision: the root --help epilogue ("3 HALT
# (identifier-case or target-name collision)"), README's exit-code line, and
# the overview's shared agent rule 8 ("Exit code 3 means an identifier-case
# or target-name collision. Show the collisions and stop"). An agent
# following the rule on a default estate went looking for collisions that
# do not exist, when the remedy is `ddl --timestamp-ntz timestamp`. The code
# is right -- a refused type is a decision for the user, not an error, and
# the chained shell needs to tell it from one -- so the docs follow it.

def _plugin_text(rel):
    root = pathlib.Path(__file__).resolve().parents[2]
    return " ".join((root / rel).read_text(encoding="utf-8").split())


def test_the_help_states_every_kind_of_halt():
    import snowmig
    text = " ".join(snowmig.build_parser().format_help().split())
    line = text[text.index("Exit codes:"):]
    assert "collision" in line
    assert "type the target refuses" in line


def test_the_readme_and_the_agent_rule_state_the_refused_type_halt():
    readme = _plugin_text("README.md")
    line = readme[readme.index("**Exit codes:**"):][:400]
    assert "type the target refuses" in line and "--timestamp-ntz" in line
    overview = _plugin_text("skills/snowflake-migrator-overview/SKILL.md")
    rule = overview[overview.index("**A halt is a halt.**"):][:700]
    assert "type the target refuses" in rule
    assert "ddl --timestamp-ntz timestamp" in rule
    clone = _plugin_text("skills/snowflake-medallion-clone/SKILL.md")
    phase_b = clone[clone.index("## Phase B"):clone.index("## Phase C")]
    assert "exit" in phase_b.lower() and "3" in phase_b
    assert "--timestamp-ntz timestamp" in phase_b
