"""The two read-only guards -- `conn.assert_read_only` on the laptop and
`assert_pushdown_read_only` in the notebooks -- refuse the same inputs, and
object names reach SQL literals escaped for the dialect that reads them."""
import sys

import pytest

from snowflake_source.conn import SourceWriteRefused, assert_read_only
from snowflake_source.dialect import lexer
from snowflake_source.extract.security import _entity_literal
from target.external_registration import _external_entry
from test_data_migration_scripts import _load

_load("02_copy_schema")
_src = sys.modules["snowmig_source"]

REFUSED = [
    "select 1 /* /* */ ; delete from t",
    "SHOW TABLES ->> DELETE FROM T",
    "select 1 ->> drop database prod",
    "select 'unclosed",
    "select 1 /* unclosed",
    "select $$unclosed",
]
ALLOWED = [
    "select '->>' as x",
    "select 1 /* a comment */",
    "select 'a;drop table t'",
]


@pytest.mark.parametrize("sql", REFUSED)
def test_both_guards_refuse(sql):
    with pytest.raises(SourceWriteRefused):
        assert_read_only(sql)
    with pytest.raises(_src.SourceWriteRefused):
        _src.assert_pushdown_read_only(sql)


@pytest.mark.parametrize("sql", ALLOWED)
def test_both_guards_allow(sql):
    assert_read_only(sql)
    _src.assert_pushdown_read_only(sql)


def test_a_backslash_quote_name_cannot_close_the_security_literal():
    literal = _entity_literal("DB", "S", "x\\' or 1=1 --")
    assert literal == lexer.sql_literal('"DB"."S"."x\\\' or 1=1 --"')
    # Every quote in the literal is doubled, and no backslash is left to
    # escape one: the literal stays open until its closing quote.
    assert "\\\\''" in literal
    assert_read_only(f"select * from t where n = '{literal}'")


def test_an_external_location_with_a_quote_stays_one_spark_literal():
    entry = _external_entry(
        {"source_identifier": "DB.S.EXT"},
        {"location": "s3://bkt/raw/o'brien/", "file_format_type": "PARQUET"},
        "cat.s.ext")
    assert entry["statement"].endswith(
        "LOCATION 'oci://<bucket>@<namespace>/raw/o\\'brien/'")


def test_a_config_parse_error_withholds_the_line(tmp_path):
    bad = tmp_path / "cfg.yaml"
    bad.write_text("snowflake:\n  password: {hunter2: x\n", encoding="utf-8")
    with pytest.raises(_src.SourceConfigError) as exc:
        _src.load_source_config(bad)
    assert "hunter2" not in str(exc.value) and "withheld" in str(exc.value)
    bad_json = tmp_path / "cfg.json"
    bad_json.write_text('{"snowflake": {"password": "hunter2"', encoding="utf-8")
    with pytest.raises(_src.SourceConfigError) as exc:
        _src.load_source_config(bad_json)
    assert "hunter2" not in str(exc.value)
