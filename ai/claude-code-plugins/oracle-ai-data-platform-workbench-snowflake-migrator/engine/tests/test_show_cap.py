"""A SHOW that stopped at its row cap is not a complete read.

Review 2026-09-25. Snowflake truncates SHOW output at 10,000 rows.
catalog._show_all pages past that with `LIMIT n FROM '<name>'`, but only
build_inventory used it: census._read_show and security._show each issued
one bare SHOW. On an account with more than 10k roles, or a database with
more than 10k streams, tasks or tags, CENSUS.md and SECURITY.md said
"10000 found" and Read = yes, the objects past the cap were absent from the
tables and from the writes= load linkage, and the lower-bound note blamed
only privileges -- promising that grants would make the count complete,
which they would not.

Now a SHOW that comes back at the cap is paged, and the paging is only
trusted when the rows prove it can be: sorted by name, each page starting
strictly after the cursor, the cursor name not shared by another row. A
read that cannot be paged that way -- the statement refuses LIMIT/FROM, or
the output is ordered schema-first so a name cursor would skip rows -- is
reported as CAPPED: its count is a lower bound whatever the grants, and the
reports say so instead of "yes".
"""
import re

from snowflake_source.extract.catalog import SHOW_PAGE_SIZE, show_paged
from snowflake_source.extract.census import build_census
from snowflake_source.extract.security import build_security

from fake_sql import FakeSql
from test_census_coverage import _responses as _census_responses
from test_security import _responses as _security_responses, _table_only_inv

_FROM = re.compile(r"limit (\d+) from '((?:[^']|'')*)'", re.IGNORECASE)


class CappedShow:
    """A SHOW with Snowflake's cap, and (optionally) LIMIT/FROM paging.

    Rows are served in the order given. A bare statement returns the first
    SHOW_PAGE_SIZE; `limit n from 'x'` returns the n rows after the FIRST row
    named x -- the documented cursor. Anything else falls through to `base`.
    """

    def __init__(self, needle, rows, base, *, pageable=True):
        self.needle, self.rows, self.base = needle, rows, base
        self.pageable = pageable
        self.calls = []

    def __call__(self, sql, params=None):
        flat = " ".join(sql.split()).lower()
        if self.needle not in flat:
            return self.base(sql, params)
        self.calls.append(flat)
        if " limit " in flat:
            if not self.pageable:
                raise RuntimeError("SQL compilation error: syntax error "
                                   "line 1 at position 30 unexpected 'limit'")
            m = _FROM.search(sql)
            n = int(re.search(r"limit (\d+)", flat).group(1))
            if not m:
                return self.rows[:n]
            cursor = m.group(2).replace("''", "'")
            at = next(i for i, r in enumerate(self.rows)
                      if r["name"] == cursor)
            return self.rows[at + 1:at + 1 + n]
        return self.rows[:SHOW_PAGE_SIZE]


def _tasks(n, schema="S"):
    return [{"name": f"T{i:06d}", "schema_name": schema, "state": "started"}
            for i in range(n)]


# ------------------------------------------------------------ the helper

def test_a_result_under_the_cap_is_one_statement_and_complete():
    calls = []

    def run_sql(sql, params=None):
        calls.append(sql)
        return [{"name": "A"}, {"name": "B"}]

    rows, capped = show_paged(run_sql, "show tasks in database \"DB\"")
    assert len(rows) == 2 and capped is None
    assert calls == ['show tasks in database "DB"'], \
        "the common case costs exactly what it cost before"


def test_a_result_at_the_cap_is_paged_to_the_true_total():
    fake = CappedShow("show tasks", _tasks(12_500), None)
    rows, capped = show_paged(fake, 'show tasks in database "DB"')
    assert len(rows) == 12_500 and capped is None
    assert len({r["name"] for r in rows}) == 12_500
    assert any("limit 10000 from 't009999'" in c for c in fake.calls)


def test_a_statement_that_refuses_limit_is_capped_not_complete():
    fake = CappedShow("show tasks", _tasks(12_500), None, pageable=False)
    rows, capped = show_paged(fake, 'show tasks in database "DB"')
    assert len(rows) == SHOW_PAGE_SIZE
    assert capped and "limit" in capped.lower()


def test_schema_first_ordering_is_not_trusted_to_a_name_cursor():
    """IN DATABASE output ordered by schema then name: a name cursor could
    resume in the wrong schema and skip rows no check would see."""
    rows = _tasks(6_000, schema="A") + _tasks(6_000, schema="B")
    fake = CappedShow("show tasks", rows, None)
    got, capped = show_paged(fake, 'show tasks in database "DB"')
    assert capped and "order" in capped.lower()
    assert len(got) == SHOW_PAGE_SIZE


# ------------------------------------------------------------ the census

def test_the_census_counts_past_the_cap():
    fake = CappedShow("show tasks", _tasks(12_500),
                      FakeSql(_census_responses()))
    census = build_census(fake, ["DB"])
    assert census["kinds"]["TASK"]["count"] == 12_500
    assert not census["kinds"]["TASK"].get("capped")
    assert any(" from '" in c for c in fake.calls), \
        "a second, paged statement was issued"


def test_an_unpageable_capped_kind_is_a_lower_bound_everywhere():
    from report.render import render_census
    fake = CappedShow("show roles", [{"name": f"R{i:06d}"}
                                     for i in range(12_500)],
                      FakeSql(_census_responses()), pageable=False)
    census = build_census(fake, ["DB"])
    role = census["kinds"]["ROLE"]
    assert role["count"] == SHOW_PAGE_SIZE
    assert role["capped"] is True
    assert "10,000" in role["note"] and "lower bound" in role["note"]
    assert "ROLE" in census["scope_statement"]
    assert "cap" in census["scope_statement"].lower()
    md = render_census(census)
    row = next(line for line in md.splitlines() if line.startswith("| Role"))
    assert "| yes |" not in row and "capped" in row.lower(), row


# ------------------------------------------------------------ security

def test_security_tags_are_paged_past_the_cap():
    tags = [{"name": f"TAG{i:06d}", "database_name": "DB",
             "schema_name": "S"} for i in range(12_500)]
    fake = CappedShow("show tags", tags, FakeSql(_security_responses()))
    s = build_security(fake, _table_only_inv(), include_grants=False)
    assert s["policies"]["tags"]["count"] == 12_500
    assert not s["policies"]["tags"].get("capped")


def test_security_says_capped_rather_than_yes():
    from report.render import render_security
    tags = [{"name": f"TAG{i:06d}", "database_name": "DB",
             "schema_name": "S"} for i in range(12_500)]
    fake = CappedShow("show tags", tags, FakeSql(_security_responses()),
                      pageable=False)
    s = build_security(fake, _table_only_inv(), include_grants=False)
    info = s["policies"]["tags"]
    assert info["count"] == SHOW_PAGE_SIZE and info["capped"] is True
    assert "lower bound" in info["note"]
    row = next(line for line in render_security(s).splitlines()
               if line.startswith("| Tags"))
    assert "| yes |" not in row and "capped" in row.lower(), row
