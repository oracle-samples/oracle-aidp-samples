"""A re-push keeps the source mode the migration was provisioned with.

Found in review. The re-push inheritance copied the catalogs and the
credential path but not `--source-mode`, which defaulted to `connector`.
Every re-push recorded `source_mode: connector` next to external-catalog
notebooks, and a --refresh-notebooks re-push (the one --stage-param needs)
regenerated every stage notebook as connector with source-config None
beside the inherited source-catalog. It exited 0 saying the coordinates
were inherited, and every stage then failed at run time with
SourceConfigError.

--source-mode now has no default at the CLI: an inheriting re-push takes
the earlier record's mode, and `connector` applies only when nothing
supplies one.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provisioning import SCRIPTS_FOLDER
from test_teardown_provenance import World

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _params(world, notebook):
    nb = json.loads(world.contents[f"{SCRIPTS_FOLDER}/{notebook}"]["body"])
    return "".join(nb["cells"][1]["source"])


def test_a_refresh_re_push_keeps_external_catalog_mode(tmp_path, monkeypatch):
    world = World()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)
    base = ["provision", "--datalake-ocid", OCID, "--workspace-name", "acme",
            "--skip-libraries", "--out-dir", str(tmp_path), "--execute"]
    assert snowmig.main(base + ["--source-mode", "external-catalog",
                                "--external-catalog", "sf_ext"]) == 0
    assert snowmig.main(base + ["--reuse-existing",
                                "--refresh-notebooks"]) == 0
    rec = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert rec["source_mode"] == "external-catalog"
    assert rec["external_catalog"] == "sf_ext"
    for notebook in ("00_discover_snowflake.ipynb",
                     "01_create_structure.ipynb", "02_copy_schema.ipynb"):
        params = _params(world, notebook)
        assert "'source-mode': 'external-catalog'" in params, notebook
        assert "'source-catalog': 'sf_ext'" in params, notebook


def test_an_explicit_mode_still_wins(tmp_path, monkeypatch):
    world = World()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)
    base = ["provision", "--datalake-ocid", OCID, "--workspace-name", "acme",
            "--skip-libraries", "--out-dir", str(tmp_path), "--execute"]
    assert snowmig.main(base + ["--source-mode", "external-catalog",
                                "--external-catalog", "sf_ext"]) == 0
    assert snowmig.main(base + ["--reuse-existing", "--refresh-notebooks",
                                "--source-mode", "connector"]) == 0
    rec = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert rec["source_mode"] == "connector"


def test_with_nothing_to_inherit_the_mode_is_connector(tmp_path, monkeypatch):
    world = World()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)
    assert snowmig.main(["provision", "--datalake-ocid", OCID,
                         "--workspace-name", "acme", "--skip-libraries",
                         "--out-dir", str(tmp_path), "--execute"]) == 0
    rec = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert rec["source_mode"] == "connector"
    assert "'source-mode': 'connector'" in _params(
        world, "00_discover_snowflake.ipynb")
