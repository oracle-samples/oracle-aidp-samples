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


def test_preflight_names_the_resolved_database():
    sent = []

    def run_sql(sql):
        sent.append(sql)
        return [{"U": "u", "R": "r", "W": "w", "D": "d", "name": "PUBLIC"}]

    run_preflight({"database": "sales_db", "account": "a", "user": "u"},
                  run_sql=run_sql)
    assert 'show schemas in database "SALES_DB"' in sent
