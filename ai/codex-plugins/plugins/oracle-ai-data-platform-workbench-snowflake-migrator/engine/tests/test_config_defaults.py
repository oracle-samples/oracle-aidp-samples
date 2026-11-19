"""Config-driven mapping defaults.

Two decisions were asked on every real migration and answered the same way:
VARIANT -> STRING, and TIMESTAMP_NTZ -> TIMESTAMP (the AIDP metastore refuses
TIMESTAMP_NTZ). They are now the defaults of the config/CLI path, settable in
snowmig-config.yaml under `mapping:`, with the stricter modes still one flag
or one line away. The pure mapper keeps its refuse-rather-than-guess default.
"""
import pathlib

import pytest

import snowmig
from migration_config import (ConfigError, MAPPING_DEFAULTS, load_config,
                              mapping_block)

PLUGIN = pathlib.Path(__file__).resolve().parents[2]


def _cfg(tmp_path, body):
    path = tmp_path / "c.yaml"
    path.write_text(body)
    return str(path)


def _resolve(argv, key):
    args = snowmig.build_parser().parse_args(argv)
    return snowmig._mapping(args, key)


# ----------------------------------------------------------- semi-structured

def test_variant_defaults_to_string_on_the_config_cli_path(tmp_path):
    assert MAPPING_DEFAULTS["semi_structured"] == "string"
    cfg = _cfg(tmp_path, "snowflake:\n  account: A\n")
    assert _resolve(["assess", "--config", cfg], "semi_structured") == "string"


def test_the_config_can_set_it_back_to_block(tmp_path):
    cfg = _cfg(tmp_path, "snowflake:\n  account: A\nmapping:\n"
                         "  semi_structured: block\n")
    assert _resolve(["assess", "--config", cfg], "semi_structured") == "block"


def test_the_flag_beats_the_config(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  semi_structured: string\n")
    assert _resolve(["assess", "--config", cfg, "--semi-structured", "block"],
                    "semi_structured") == "block"


def test_ingest_resolves_the_same_way(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  semi_structured: block\n")
    assert _resolve(["ingest", "--config", cfg, "--manifest", "m",
                     "--database-name", "D"], "semi_structured") == "block"


def test_an_unknown_mode_in_the_config_is_refused(tmp_path):
    with pytest.raises(ConfigError, match="semi_structured"):
        mapping_block({"mapping": {"semi_structured": "json"}})


def test_an_unknown_mapping_key_is_refused():
    with pytest.raises(ConfigError, match="unknown key"):
        mapping_block({"mapping": {"variant": "string"}})


def test_the_pure_mapper_still_refuses_by_default():
    from snowflake_source.dialect.types import map_type
    assert map_type("VARIANT").blocked is True


def test_the_example_config_documents_the_block():
    cfg = load_config(PLUGIN / "snowmig-config.example.yaml")
    assert mapping_block(cfg)["semi_structured"] == "string"


# ------------------------------------------------------------- TIMESTAMP_NTZ

def test_timestamp_ntz_defaults_to_timestamp_on_the_config_cli_path(tmp_path):
    assert MAPPING_DEFAULTS["timestamp_ntz"] == "timestamp"
    cfg = _cfg(tmp_path, "snowflake:\n  account: A\n")
    assert _resolve(["assess", "--config", cfg], "timestamp_ntz") == "timestamp"


def test_preserve_is_still_an_override(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  timestamp_ntz: preserve\n")
    assert _resolve(["assess", "--config", cfg], "timestamp_ntz") == "preserve"
    cfg = _cfg(tmp_path, "mapping:\n  timestamp_ntz: timestamp\n")
    assert _resolve(["assess", "--config", cfg, "--timestamp-ntz",
                     "preserve"], "timestamp_ntz") == "preserve"


def test_ingest_resolves_timestamp_the_same_way(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  timestamp_ntz: preserve\n")
    assert _resolve(["ingest", "--config", cfg, "--manifest", "m",
                     "--database-name", "D"], "timestamp_ntz") == "preserve"


def test_an_unknown_timestamp_mode_is_refused():
    with pytest.raises(ConfigError, match="timestamp_ntz"):
        mapping_block({"mapping": {"timestamp_ntz": "utc"}})


def test_the_example_config_documents_timestamp():
    cfg = load_config(PLUGIN / "snowmig-config.example.yaml")
    assert mapping_block(cfg)["timestamp_ntz"] == "timestamp"


# ----------------------------------------- cluster per warehouse, or existing

from migration_config import compute_block  # noqa: E402
from sizing.warehouse_map import cluster_base_name, propose_all, propose_cluster  # noqa: E402

WH = {"name": "COMPUTE_WH", "size": "X-Small"}


def test_new_is_the_default_mode():
    assert compute_block({}) == {"warehouse_clusters": "new", "cluster_id": None}


def test_existing_mode_needs_a_cluster_id_and_falls_back_to_aidp():
    with pytest.raises(ConfigError, match="cluster_id"):
        compute_block({"compute": {"warehouse_clusters": "existing"}})
    blk = compute_block({"compute": {"warehouse_clusters": "existing"},
                         "aidp": {"cluster_id": "abc"}})
    assert blk == {"warehouse_clusters": "existing", "cluster_id": "abc"}
    blk = compute_block({"compute": {"warehouse_clusters": "existing",
                                     "cluster_id": "own"},
                         "aidp": {"cluster_id": "abc"}})
    assert blk["cluster_id"] == "own"


def test_an_unknown_mode_is_refused():
    with pytest.raises(ConfigError, match="warehouse_clusters"):
        compute_block({"compute": {"warehouse_clusters": "shared"}})


@pytest.mark.parametrize("name,base", [("COMPUTE_WH", "compute"),
                                       ("ETL-WAREHOUSE", "etl"),
                                       ("Reporting_wh", "reporting"),
                                       ("ANALYTICS", "analytics"),
                                       ("SYSTEM$STREAMLIT_NOTEBOOK_WH",
                                        "system_streamlit_notebook")])
def test_a_new_cluster_is_named_from_the_warehouse_base_name(name, base):
    assert cluster_base_name(name) == base


def test_new_mode_proposes_creating_a_named_cluster():
    p = propose_cluster(WH)
    assert p["cluster_action"] == "create" and p["target_cluster"] == "compute"


def test_existing_mode_proposes_the_existing_cluster_and_creates_nothing():
    p = propose_cluster(WH, mode="existing", existing_cluster_id="abc")
    assert p["cluster_action"] == "use_existing"
    assert p["target_cluster"] == "abc"
    assert "not resized" in p["notes"]
    allp = propose_all([WH], mode="existing", existing_cluster_id="abc")
    assert allp["cluster_mode"] == "existing"
    assert allp["clusters_to_create"] == 0


def test_provision_names_warehouse_clusters_from_the_base_name():
    from target.provisioning import provision
    calls = []

    def call(op, **kw):
        calls.append((op, kw))
        return {"items": []}
    res = provision(call=call, workspace_name="ws", cluster_name="c",
                    warehouse_clusters=[WH], execute=False, scripts=[],
                    plan_files=[])
    assert res["warehouse_clusters"][0]["name"] == "compute"


def test_provision_in_existing_mode_creates_no_warehouse_cluster():
    from target.provisioning import provision
    calls = []

    def call(op, **kw):
        calls.append(op)
        if op == "list_workspaces":
            return {"items": [{"displayName": "ws", "key": "w"}]}
        if op == "list_clusters":
            return {"items": [{"displayName": "c", "key": "k"}]}
        return {"items": []}
    res = provision(call=call, workspace_name="ws", cluster_name="c",
                    warehouse_clusters=[WH], execute=True, scripts=[],
                    plan_files=[], reuse_existing=True,
                    warehouse_cluster_mode="existing",
                    existing_cluster_id="abc", delays=())
    assert calls.count("create_cluster") == 0
    step = next(s for s in res["steps"] if s["step"] == "warehouse-cluster")
    assert step["action"] == "uses_existing" and "abc" in step["detail"]


def test_the_compute_proposal_says_create_or_existing():
    from report.render import render_compute
    md = render_compute(propose_all([WH]))
    assert "| Action | Cluster |" in md and "create" in md and "`compute`" in md
    md = render_compute(propose_all([WH], mode="existing",
                                    existing_cluster_id="abc"))
    assert "use existing" in md and "`abc`" in md
    assert "no cluster is created" in md.lower()


# ----------------------------------------- approved decisions as engine inputs

from migration_config import decisions_block  # noqa: E402


def test_decisions_default_to_todays_behaviour():
    assert decisions_block({}) == {"allow_new_objects": True,
                                   "warehouse_clusters": "create_on_confirmation"}


def test_an_unknown_decision_is_refused():
    with pytest.raises(ConfigError, match="unknown key"):
        decisions_block({"decisions": {"yolo": True}})
    with pytest.raises(ConfigError, match="warehouse_clusters"):
        decisions_block({"decisions": {"warehouse_clusters": "maybe"}})


def _provision_argv(cfg, tmp_path, *extra):
    return ["provision", "--out-dir", str(tmp_path / "o"), "--config", cfg,
            "--workspace-name", "ws", "--datalake-ocid", "ocid1.x",
            "--execute", *extra]


def test_proposal_only_refuses_to_create_warehouse_clusters(tmp_path, capsys):
    cfg = _cfg(tmp_path, "decisions:\n  warehouse_clusters: proposal_only\n")
    assert snowmig.main(_provision_argv(cfg, tmp_path,
                                        "--warehouse-clusters")) == 1
    assert "proposal_only" in capsys.readouterr().err


def test_no_new_objects_refuses_provision_and_catalog(tmp_path, capsys):
    cfg = _cfg(tmp_path, "decisions:\n  allow_new_objects: false\n")
    assert snowmig.main(_provision_argv(cfg, tmp_path)) == 1
    assert "allow_new_objects" in capsys.readouterr().err
    assert snowmig.main(["catalog", "--out-dir", str(tmp_path / "o"),
                         "--config", cfg, "--catalog", "c", "--execute",
                         "--datalake-ocid", "o", "--workspace", "w",
                         "--cluster-id", "k"]) == 1
    assert "allow_new_objects" in capsys.readouterr().err


def test_a_dry_run_is_never_refused_by_a_decision(tmp_path):
    cfg = _cfg(tmp_path, "decisions:\n  allow_new_objects: false\n")
    argv = [a for a in _provision_argv(cfg, tmp_path) if a != "--execute"]
    assert snowmig.main(argv) == 0


# ------------------------------------------ a master toggle, and provenance

def test_the_toggle_is_on_by_default():
    blk = mapping_block({})
    assert blk["enabled"] is True and blk["semi_structured"] == "string"


def test_disabling_it_restores_the_strict_modes_and_ignores_the_fields():
    blk = mapping_block({"mapping": {"enabled": False,
                                     "semi_structured": "string",
                                     "timestamp_ntz": "timestamp"}})
    # geospatial joined the block as a third decision (`wkt` is new); its
    # strict mode is the `block` it always defaulted to.
    assert blk == {"enabled": False, "semi_structured": "block",
                   "timestamp_ntz": "preserve", "geospatial": "block",
                   "source_type_drift": "refuse"}


def test_source_type_drift_defaults_to_refuse_and_accepts_convert():
    assert mapping_block({})["source_type_drift"] == "refuse"
    assert mapping_block({"mapping": {"source_type_drift": "convert"}})[
        "source_type_drift"] == "convert"
    with pytest.raises(ConfigError, match="source_type_drift"):
        mapping_block({"mapping": {"source_type_drift": "ignore"}})


def test_the_toggle_must_be_a_boolean():
    with pytest.raises(ConfigError, match="enabled"):
        mapping_block({"mapping": {"enabled": "yes"}})


def test_the_cli_toggle_overrides_the_config_for_one_run(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  enabled: true\n")
    assert _resolve(["assess", "--config", cfg, "--mapping-defaults", "off"],
                    "semi_structured") == "block"
    cfg = _cfg(tmp_path, "mapping:\n  enabled: false\n")
    assert _resolve(["assess", "--config", cfg, "--mapping-defaults", "on"],
                    "timestamp_ntz") == "timestamp"


def test_an_explicit_flag_still_wins_when_the_toggle_is_off(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  enabled: false\n")
    assert _resolve(["assess", "--config", cfg, "--semi-structured", "string"],
                    "semi_structured") == "string"


def test_the_resolution_and_its_source_are_recorded(tmp_path):
    cfg = _cfg(tmp_path, "mapping:\n  timestamp_ntz: preserve\n")
    args = snowmig.build_parser().parse_args(
        ["assess", "--config", cfg, "--semi-structured", "block"])
    res = snowmig._mapping_resolution(args)
    assert res["enabled"] is True
    assert res["semi_structured"] == {"value": "block", "source": "flag"}
    assert res["timestamp_ntz"] == {"value": "preserve", "source": "config"}
    cfg = _cfg(tmp_path, "mapping:\n  enabled: false\n")
    args = snowmig.build_parser().parse_args(["assess", "--config", cfg])
    res = snowmig._mapping_resolution(args)
    assert res["semi_structured"]["source"] == "disabled"


def test_the_inventory_report_states_the_mapping():
    from report.render import render_inventory
    md = render_inventory({"session": {}, "inventory": [],
                           "mapping_resolution": {
                               "enabled": True,
                               "semi_structured": {"value": "string",
                                                   "source": "config"},
                               "timestamp_ntz": {"value": "timestamp",
                                                 "source": "default"}}})
    line = next(l for l in md.splitlines() if l.startswith("Type mapping"))
    assert "VARIANT → string (config)" in line
    assert "TIMESTAMP_NTZ → timestamp (default)" in line
    assert "defaults ON" in line
