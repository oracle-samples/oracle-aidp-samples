"""data_options.json and DATA_MOVEMENT_OPTIONS.md must tell one story.

The two artifacts of one data-options run disagreed. The markdown, from
render_data_options, said the control-plane CLI moves no bytes and that one
path IS implemented by the data plane, the `snowmig_02_copy_schema` job.
The JSON, from a note hard-coded where it was written, said "Proposal only.
This plugin moves no bytes and implements no transfer path." -- the wording
the CHANGELOG said every surface had dropped. The demo (emulation/runbook.py)
carried its own copy of the stale note, so DEMO runs taught it too.

The sentence now lives once, as report.render.DATA_OPTIONS_NOTE; the
markdown opens with it and the demo's JSON note is it.
"""
import json

import pytest

from emulation.runbook import run_demo
from plan.data_movement import OPTIONS as DATA_OPTIONS
from report.render import DATA_OPTIONS_NOTE, render_data_options

STALE = "moves no bytes and implements no transfer"


def test_the_shared_note_names_the_implemented_path():
    assert "snowmig_02_copy_schema" in DATA_OPTIONS_NOTE
    assert "control-plane CLI moves no bytes" in DATA_OPTIONS_NOTE
    assert STALE not in DATA_OPTIONS_NOTE


def test_the_markdown_carries_the_same_sentences():
    md = " ".join(render_data_options(list(DATA_OPTIONS)).split())
    for sentence in DATA_OPTIONS_NOTE.split(". ")[1:]:
        assert sentence.rstrip(".") in md.replace("**", ""), sentence


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo_options")
    run_demo(out)
    return out


def test_the_demo_json_note_agrees_with_its_markdown(demo):
    payload = json.loads((demo / "data_options.json").read_text(encoding="utf-8"))
    assert STALE not in payload["note"]
    assert payload["note"] == DATA_OPTIONS_NOTE
    md = (demo / "DATA_MOVEMENT_OPTIONS.md").read_text(encoding="utf-8")
    assert "snowmig_02_copy_schema" in md and "snowmig_02_copy_schema" in payload["note"]
