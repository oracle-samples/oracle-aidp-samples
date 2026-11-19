"""DEMO.md must agree with the reports the same demo run wrote.

DEMO.md is the file the demo skill presents first, and three of its lines
were stale against the artifacts beside it:

- "the backup-snowflake-migration/ folder with 0 script(s)": runbook.py
  counted data-migration-scripts/*.py, but that folder now holds only the
  generated .ipynb, while PROVISION.md from the same run listed 5 uploads.
- "census found 6 object(s) that cannot migrate (a task, a stream, a
  procedure, a JavaScript UDF)": a hard-coded parenthetical naming 4 of 6
  kinds, leaving out the alert and the outbound share the demo estate was
  extended to teach.
- "AIDP coordinates ... are asked for per conversation, never stored or
  assumed": the README's config contract has an `aidp:` block that holds
  them.
"""
import json
import re

import pytest

from emulation.runbook import run_demo


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo_narrative")
    run_demo(out)
    return out


def _demo_md(out):
    return (out / "DEMO.md").read_text(encoding="utf-8")


def test_the_upload_count_is_the_provision_reports(demo):
    # The generated notebooks only: the demo now pushes the plan too, as
    # provision does, and a plan file is not a notebook.
    uploads = [l for l in (demo / "PROVISION.md").read_text(
        encoding="utf-8").splitlines()
        if "| would upload |" in l and "(generated" in l]
    line = next(l for l in _demo_md(demo).splitlines() if "provision (dry run)" in l)
    m = re.search(r"folder with (\d+) ", line)
    assert m, line
    assert int(m.group(1)) == len(uploads) == 5


def test_the_census_line_names_every_kind_it_found(demo):
    census = json.loads((demo / "inventory.json").read_text(
        encoding="utf-8"))["census"]
    line = next(l for l in _demo_md(demo).splitlines() if "census found" in l)
    for kind, count in census["by_kind"].items():
        if count:
            assert kind.lower() in line.lower(), (kind, line)


def test_the_prod_notes_match_the_config_contract(demo):
    md = _demo_md(demo)
    assert "never stored" not in md
    assert "`aidp:` block" in md
