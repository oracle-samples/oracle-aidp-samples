"""The catalog API refuses a view whose query carries a comment.

Seen live on AIDP: V_SLASH_COMMENT -- a Snowflake view
with a `//` line comment, which the translator correctly rewrote to `--` --
was refused by the catalog CRUD API:

    400 InvalidParameter: "Invalid Invalid Sql Query: inline SQL comments
    are not allowed"

Spark SQL takes the comment; the catalog API's viewText does not, in any
style. A comment carries no meaning, so the query sent as viewText is the
view's code with its comments removed. The reviewed CREATE VIEW in
DDL_PLAN.md keeps them.
"""
import pytest

from target.ddl import _view_text

_HEAD = "CREATE VIEW IF NOT EXISTS `lake`.`s`.`v` AS\n"


@pytest.mark.parametrize("body", [
    "SELECT ID -- don't count cancelled\nFROM lake.s.orders",
    "SELECT ID /* why: audit */ FROM lake.s.orders",
    "SELECT ID\n-- whole-line note\nFROM lake.s.orders",
])
def test_the_catalog_view_text_carries_no_comment(body):
    text = _view_text(_HEAD + body)
    assert "--" not in text and "/*" not in text, text
    assert "SELECT ID" in text and "FROM lake.s.orders" in text


def test_a_comment_marker_inside_a_literal_is_data_and_stays():
    text = _view_text(_HEAD + "SELECT 'a--b' AS x, '/*' AS y FROM lake.s.t")
    assert "'a--b'" in text and "'/*'" in text


def test_a_named_column_list_still_wraps_the_uncommented_body():
    sql = ("CREATE VIEW IF NOT EXISTS `lake`.`s`.`v` (`CUSTOMER`, `TOTAL`) AS\n"
           "SELECT CUST_ID, SUM(AMT) -- per customer\nFROM lake.s.orders "
           "GROUP BY CUST_ID")
    text = _view_text(sql)
    assert "--" not in text
    assert "named_columns(`CUSTOMER`, `TOTAL`)" in text
