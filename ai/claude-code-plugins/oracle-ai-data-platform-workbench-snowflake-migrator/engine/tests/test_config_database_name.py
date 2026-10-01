"""The config's `database:` resolves to the same name on the laptop
(`assess`, `preflight`) as in the data-plane notebooks."""
import pytest

import snowmig
from dataplane.snowmig_source import _database_name
from plan.preflight import run_preflight
from snowflake_source.dialect import lexer


@pytest.mark.parametrize("value", [
    "SALES_DB", "sales_db", '"MyDb"', '"a""b"', "my-db", "  sales_db  "])
def test_laptop_and_data_plane_resolve_the_same_database(value):
    assert lexer.config_name(value) == _database_name(value)


def test_assess_queries_the_resolved_config_database(monkeypatch):
    seen = {}

    def fake_build_inventory(run_sql, databases, **kw):
        seen["databases"] = databases
        return {"objects": []}

    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda a: None)
    monkeypatch.setattr(snowmig, "build_inventory", fake_build_inventory)
    monkeypatch.setattr(snowmig, "_snowflake_coords",
                        lambda a: {"database": '"MyDb"'})
    monkeypatch.setattr(snowmig, "_mapping", lambda a, k: None)
    monkeypatch.setattr(snowmig, "_mapping_resolution", lambda a: {})

    class Args:
        database = None
        no_census = True
    snowmig._assess_inventory(Args())
    assert seen["databases"] == ["MyDb"]


@pytest.mark.parametrize("value,qualified", [
    ("sales_db", '"SALES_DB"'), ('"MyDb"', '"MyDb"')])
@pytest.mark.parametrize("schema", ["PUBLIC", None])
def test_preflight_names_the_resolved_database(value, qualified, schema):
    sent = []

    def run_sql(sql):
        sent.append(sql)
        return [{"U": "u", "R": "r", "W": "w", "D": "d", "name": "PUBLIC",
                 "S": "PUBLIC", "N": 1}]

    config = {"database": value, "account": "a", "user": "u"}
    if schema:
        config["schema"] = schema
    run_preflight(config, run_sql=run_sql)
    assert f"show schemas in database {qualified}" in sent
    # Every statement that names the database names the same one: the
    # session-schema check too, with or without `schema:`.
    named = [s for s in sent if "INFORMATION_SCHEMA" in s]
    assert named and all(f"{qualified}.INFORMATION_SCHEMA" in s
                         for s in named), named


def test_preflight_escapes_the_schema_literal_for_snowflake():
    sent = []

    def run_sql(sql):
        sent.append(sql)
        return [{"N": 1}]

    run_preflight({"database": "DB", "schema": "WEIRD\\", "account": "a",
                   "user": "u"}, run_sql=run_sql)
    assert any("TABLE_SCHEMA = 'WEIRD\\\\'" in s for s in sent), sent
