"""Regression tests: a leading cell magic must stay on line 0.

Incident (Example_Feats_Retrain_Pipeline, 2026-09-16, control cluster
aaaaaaaa-0000-4000-8000-000000000104): `_add_missing_imports` prepended an
injected import at line 0 of `00_example_utils.ipynb` cell 4, pushing that cell's
`%python` magic to line 1. Every magic handler reads only the first line, so the
magic was no longer stripped, reached the kernel as a statement and aborted the
cell. The cell was the notebook's import preamble, so `Row` was never bound and
the first cell to define `def convert_to_row(d: dict) -> Row:` failed at def
time with `NameError: name 'Row' is not defined` — a return annotation is
evaluated when the `def` runs. `03_Eval_Base_Prep_Prod` reported FAIL and six
dependent tasks were BLOCKED behind it: 7 of 9 tasks lost to one line.

The Scala branch of the same function already inserted after its magic; only the
Python branch blind-prepended. Two victim classes follow from that asymmetry:

  * `%python` cells — magic displaced, injected import still wanted.
  * `%sql`/`%r`/`%md` cells — the function classifies them as Python
    (`lang = "scala" if is_scala else "python"`), so the rule table matches bare
    words like `date` and `to_date` in ordinary SQL and injects imports that can
    never be valid there.

Prevention lives in `_add_missing_imports`; repair of source already written to
disk lives in `_promote_leading_magic`.
"""

import asyncio
import base64
import contextlib
import io
import json
import os
import re
import stat
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import job_migrate as jm  # noqa: E402


class _Kernel:
    """Minimal fake kernel: records executed code, captures save-back writes.

    Defined here rather than imported from another test module so this file
    stands alone — a shared helper would couple it to an unrelated suite.
    """

    cluster_id = "test-cluster"

    def __init__(self, on_disk_cells=1, *, save_result=None,
                 save_exception=None, save_replaced=None):
        self.executed = []
        self.files = {}
        # The save-back tripwire asks the cluster how many real code cells the
        # file already has and refuses to write on a mismatch, so a test that
        # wants to observe a save-back has to answer it.
        self.on_disk_cells = on_disk_cells
        self.save_payloads = []
        self.save_result = save_result
        self.save_exception = save_exception
        self.save_replaced = save_replaced

    async def execute(self, code, **kwargs):
        self.executed.append(code)
        match = re.search(r"with builtins.open\('([^']+)', 'w[ba]'\) as f:", code)
        encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
        if match and encoded:
            self.files[match.group(1)] = base64.b64decode(encoded.group(1)).decode()
        # The child save-back merges cluster-side: it ships a base64 payload of
        # {cells, outputs, setup_cells} and rewrites the file there, so the
        # payload — not a captured file write — is what carries the healed cell.
        if "payload = json.loads(base64.b64decode(" in code and encoded:
            payload = json.loads(base64.b64decode(encoded.group(1)).decode())
            self.save_payloads.append(payload)
            if self.save_exception is not None:
                raise self.save_exception
            if self.save_result is not None:
                return self.save_result
            replaced = (len(payload["cells"]) if self.save_replaced is None
                        else self.save_replaced)
            return {
                "status": "ok",
                "outputs": [{
                    "type": "stream",
                    "name": "stdout",
                    "text": (f"Replaced {replaced}/{self.on_disk_cells} cells "
                             "(config_offset=0)"),
                }],
            }
        if "_CFG_MARKER = '# AIDP performance configuration'" in code and "_count" in code:
            return {"status": "ok",
                    "outputs": [{"type": "stream", "name": "stdout",
                                 "text": ("__AIDP_CHILD_COUNT__"
                                          f"{self.on_disk_cells}\n")}]}
        if "__AIDP_ISO_OK__" in code:
            return {"status": "ok",
                    "outputs": [{"type": "stream", "name": "stdout",
                                 "text": "__AIDP_ISO_OK__\n"}]}
        return {"status": "ok", "outputs": []}

    async def run_stateless(self, code, **kwargs):
        return {"status": "ok", "outputs": []}


class _ExecutingSaveKernel(_Kernel):
    """Execute the generated save program against a real local notebook.

    The ordinary fake deliberately simulates only the transport contract.  This
    variant exercises the file transaction itself so a partial ``json.dump`` or
    failed ``os.replace`` cannot hide behind a manufactured kernel result.
    """

    def __init__(self, on_disk_cells=1, *, fail_dump=False,
                 fail_replace=False):
        super().__init__(on_disk_cells=on_disk_cells)
        self.fail_dump = fail_dump
        self.fail_replace = fail_replace

    async def execute(self, code, **kwargs):
        if "payload = json.loads(base64.b64decode(" not in code:
            return await super().execute(code, **kwargs)

        self.executed.append(code)
        encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
        assert encoded, "generated save program must carry its payload"
        payload = json.loads(base64.b64decode(encoded.group(1)).decode())
        self.save_payloads.append(payload)

        real_dump = json.dump
        real_replace = os.replace

        def partial_dump(_obj, file_obj, *args, **kwargs):
            file_obj.write('{"partial":')
            file_obj.flush()
            raise OSError("injected mid-dump failure")

        def failed_replace(_source, _destination):
            raise OSError("injected replace failure")

        if self.fail_dump:
            json.dump = partial_dump
        if self.fail_replace:
            os.replace = failed_replace
        stdout = io.StringIO()
        try:
            with contextlib.redirect_stdout(stdout):
                exec(compile(code, "<child-save-back>", "exec"), {})
        except Exception as exc:
            return {
                "status": "error",
                "outputs": [{
                    "type": "error",
                    "ename": type(exc).__name__,
                    "evalue": str(exc),
                }],
            }
        finally:
            json.dump = real_dump
            os.replace = real_replace
        return {
            "status": "ok",
            "outputs": [{
                "type": "stream",
                "name": "stdout",
                "text": stdout.getvalue(),
            }],
        }


TAG = "Oracle tool modification:"


# ── Prevention: _add_missing_imports ────────────────────────────────────────

def test_python_magic_stays_on_line_0_and_import_lands_below():
    """The incident shape: a %python cell needing an injected import."""
    out = jm._add_missing_imports("%python\nd = OrderedDict()\n")
    lines = out.split("\n")
    assert lines[0].strip() == "%python", "magic must remain the first line"
    assert TAG in lines[1], "injected import belongs directly below the magic"
    assert "from collections import OrderedDict" in out


def test_preamble_still_binds_its_names_after_injection():
    """End-to-end guard on the real failure: with the magic stripped, the cell
    must parse, so the imports below it actually execute and bind `Row`."""
    source = (
        "%python\n"
        "import pandas as pd\n"
        "from pyspark.sql.types import *\n"
        "from pyspark.sql import Row\n"
        "import datetime\n"
        "from collections import OrderedDict, deque\n"
    )
    out = jm._add_missing_imports(source)
    assert out.split("\n")[0].strip() == "%python"
    assert "from pyspark.sql import Row" in out, "customer import must survive"
    body = "\n".join(out.split("\n")[1:])  # what the kernel runs once stripped
    import ast
    ast.parse(body)  # raises if the magic leaked into the executed body


@pytest.mark.parametrize(
    "magic", ["%sql", "%r", "%md", "%md-sandbox", "%sh", "%fs", "%pip"]
)
def test_non_python_language_and_action_magics_get_no_injection(magic):
    """A cell whose body is not Python must not receive Python imports.

    `%sql` is the live case: `to_date(...)` and a bare `date` column both match
    the rule table, so before the language gate a plain SELECT attracted three
    imports above its magic.
    """
    source = f"{magic}\nSELECT to_date(created_at) FROM t WHERE date >= '2026-01-01'\n"
    assert jm._add_missing_imports(source) == source


def test_run_magic_is_not_touched_even_when_its_path_matches_a_rule():
    """`%run /Users/x/date_utils` contains `date`; the cell must stay intact."""
    source = "%run /Users/x/date_utils\n"
    assert jm._add_missing_imports(source) == source


@pytest.mark.parametrize(("source", "token"), [
    ("%conf\nspark.setting = to_date\n", "%conf"),
    ("%matplotlib inline\nd = OrderedDict()\n", "%matplotlib"),
    ("%customer_magic\nd = OrderedDict()\n", "%customer_magic"),
    ("%customer-magic\nd = OrderedDict()\n", "%customer-magic"),
])
def test_unlisted_single_percent_magics_fail_closed(source, token):
    """Unknown magic syntax is not Python merely because it is unlisted.

    The migration cannot enumerate extension and future magics.  It must
    recognise their lexical shape, preserve the cell byte-for-byte, and avoid
    inserting a Python import above the directive that has to remain on line 0.
    """
    assert jm._leading_cell_magic(source) == token
    assert jm._add_missing_imports(source) == source


@pytest.mark.parametrize(("source", "magic_line"), [
    ("%time result = OrderedDict(records)\n",
     "%time result = OrderedDict(records)"),
    ("%timeit -n 1 to_date(c)\n", "%timeit -n 1 to_date(c)"),
    ("%load_ext customer_extension\nd = OrderedDict()\n",
     "%load_ext customer_extension"),
    ("%autoreload 2\nd = OrderedDict()\n", "%autoreload 2"),
])
def test_known_python_line_magic_receives_missing_import_before_first_use(
        source, magic_line):
    """A Python line magic is a statement, not a line-0 language declaration."""
    out = jm._add_missing_imports(source)
    lines = out.splitlines()

    assert TAG in lines[0]
    assert magic_line in lines[1:]


def test_displaced_magic_fails_closed_in_missing_import_injector():
    """Do not persist a new import above a magic we cannot safely promote."""
    source = "x = compute()\n%python\nd = OrderedDict()\n"
    assert jm._promote_leading_magic(source).advisories
    assert jm._add_missing_imports(source) == source


def test_mid_cell_line_magic_does_not_block_missing_import_injection():
    source = "import time\n%time result = OrderedDict(records)\n"
    assert jm._promote_leading_magic(source).advisories == []

    out = jm._add_missing_imports(source)

    assert TAG in out.splitlines()[0]
    assert "%time result = OrderedDict(records)" in out


def test_scala_cell_behaviour_is_unchanged():
    out = jm._add_missing_imports("%scala\nval j = new JSONObject()\n")
    lines = out.split("\n")
    assert lines[0].strip() == "%scala"
    assert "import org.json.JSONObject" in lines[1]


@pytest.mark.parametrize("source", [
    "d = OrderedDict()\nx = 1",
    "# a comment\nd = OrderedDict()",
    "\n\nd = defaultdict()",
    "   d = OrderedDict()",
    "x = 5 % 2\nd = OrderedDict()",
])
def test_cells_without_a_leading_magic_are_byte_identical_to_a_prepend(source):
    """Regression fence for the overwhelming majority of cells.

    The fix changed how the block is placed, so prove the placement is
    unchanged when there is no magic: the result must equal the old
    `added_block + source`.
    """
    out = jm._add_missing_imports(source)
    if out == source:
        pytest.skip("no import injected for this source")
    injected = [ln for ln in out.split("\n") if TAG in ln]
    assert out == "\n".join(injected) + "\n" + source


def test_a_modulo_expression_is_not_mistaken_for_a_magic():
    """Only a column-zero first-line directive has the lexical magic shape."""
    assert jm._leading_cell_magic("x = 5 % 2") is None
    assert jm._leading_cell_magic("   %notamagic = 1") is None
    assert jm._leading_cell_magic("   %sql\nSELECT 1") is None
    assert jm._leading_cell_magic("value = '%time result'") is None
    assert jm._leading_cell_magic("# %time result") is None
    assert jm._leading_cell_magic("%python\nx = 1") == "%python"
    assert jm._leading_cell_magic("%sql\nSELECT 1") == "%sql"
    assert jm._leading_cell_magic("%md-sandbox\ntext") == "%md-sandbox"
    assert jm._leading_cell_magic("%customer-magic\ntext") == "%customer-magic"
    assert jm._leading_cell_magic("%bad-\ntext") is None


# ── Repair: _promote_leading_magic ──────────────────────────────────────────

def test_repair_promotes_magic_and_keeps_a_valid_python_import():
    """The on-disk corruption from the incident heals without losing the import."""
    corrupted = (
        "from datetime import datetime  # Oracle tool modification: added missing import\n"
        "%python\n"
        "from pyspark.sql import Row\n"
    )
    pro = jm._promote_leading_magic(corrupted)
    out, reason, dropped = pro.source, pro.reason, pro.dropped
    assert reason == "tool-injected line above magic", (
        "the log must name WHICH displacement was healed; a bare 'promoted' "
        "line cannot be told apart from the pre-existing comment path, which "
        "made a live promotion event unattributable")
    assert dropped == []
    lines = out.split("\n")
    assert lines[0].strip() == "%python"
    assert "from datetime import datetime" in out, "valid Python import must be kept"
    assert "from pyspark.sql import Row" in out


def test_repair_drops_injected_imports_that_cannot_be_valid_in_a_sql_cell():
    """Promoting the magic alone would leave Python inside the SQL body.

    Restoring the magic to line 0 makes AIDP parse the cell as SQL, so an
    injected Python import becomes a SQL statement and the cell still fails.
    Those lines are the tool's own and were never legal here, so they go — and
    the result is the cell as Databricks exported it.
    """
    original = "%sql\nSELECT to_date(x) FROM t\n"
    corrupted = (
        "from datetime import date  # Oracle tool modification: added missing import\n"
        "from pyspark.sql.functions import to_date  # Oracle tool modification: added missing import\n"
        + original
    )
    pro = jm._promote_leading_magic(corrupted)
    out, reason, dropped = pro.source, pro.reason, pro.dropped
    assert reason == "tool-injected line above magic"
    assert len(dropped) == 2, "both injected lines must be reported, not silently cut"
    assert out.rstrip("\n") == original.rstrip("\n"), "must restore the exported cell"


def test_repair_keeps_a_scala_import_whose_tag_uses_the_scala_comment_marker():
    """The tag is emitted as `//` in Scala and `--` in SQL, not only `#`."""
    corrupted = (
        "import org.json.JSONObject  // Oracle tool modification: added missing import\n"
        "%scala\nval j = new JSONObject()\n"
    )
    pro = jm._promote_leading_magic(corrupted)
    out, reason, dropped = pro.source, pro.reason, pro.dropped
    assert reason == "tool-injected line above magic"
    assert dropped == []
    assert out.split("\n")[0].strip() == "%scala"
    assert "import org.json.JSONObject" in out


def test_untagged_customer_code_above_a_magic_is_left_alone_and_reported():
    """Only the tool's own edits may be relocated — but silence is not an option.

    A Databricks export always has the magic on line 0, so untagged code above
    one was not put there by this tool and reordering it could change which
    language the cell is parsed in and what runs first. The cell is still
    broken: the magic is off line 0, so it reaches the kernel as a statement.
    Leaving it alone is right; leaving it UNREPORTED let it through with no
    operator-visible trace, and the import injector then pushed the magic
    further down, because its language gate also reads only line 0.
    """
    source = "x = compute()\n%python\nd = 1\n"
    pro = jm._promote_leading_magic(source)
    assert (pro.source, pro.reason, pro.dropped) == (source, "", []), (
        "the source must not be touched")
    assert pro.advisories, "a guaranteed cell abort must not pass silently"
    assert "%python" in pro.advisories[0], (
        f"the advisory must name the stranded magic: {pro.advisories}")


@pytest.mark.parametrize("magic", ["%python", "%pyspark"])
def test_same_language_python_declaration_is_promoted_with_native_context(magic):
    source = f"x = 1\n{magic}\nvalue = dbutils.widgets.get('name')\n"

    pro = jm._promote_leading_magic(source, native_lang="")

    assert pro.reason.startswith("native-language declaration below same-language code")
    assert pro.advisories == []
    expected_header = (["%python",
                        "# Oracle tool modification: select AIDP Python interpreter; original:",
                        "# %pyspark"] if magic == "%pyspark" else [magic])
    assert pro.source.splitlines() == expected_header + [
        "x = 1",
        "value = dbutils.widgets.get('name')",
    ]


@pytest.mark.parametrize("magic", ["%scala", "%sql", "%r", "%md", "%%bash"])
def test_mismatched_or_cell_magic_is_not_promoted_in_a_python_notebook(magic):
    source = f"x = 1\n{magic}\ny = 2\n"

    pro = jm._promote_leading_magic(source, native_lang="")

    assert pro.source == source
    assert pro.reason == ""
    assert pro.advisories


@pytest.mark.parametrize(("native_lang", "magic"), [("scala", "%scala"), ("sql", "%sql")])
def test_non_python_native_displacement_remains_advisory(native_lang, magic):
    """Their pre-loop prefixing has a separate contract; do not half-fix it."""
    source = f"x = 1\n{magic}\ny = 2\n"

    pro = jm._promote_leading_magic(source, native_lang=native_lang)

    assert pro.source == source
    assert pro.reason == ""
    assert pro.advisories


def test_multiple_language_declarations_are_not_promoted():
    source = "x = 1\n%python\ny = 2\n%sql\nSELECT 1\n"

    pro = jm._promote_leading_magic(source, native_lang="")

    assert pro.source == source
    assert pro.reason == ""
    assert pro.advisories


def test_future_import_sharing_a_line_with_code_fails_closed():
    source = (
        "x = 1\n"
        "%python\n"
        "from __future__ import annotations; y = x + 1\n"
    )

    pro = jm._promote_leading_magic(source, native_lang="python")

    assert pro.source == source
    assert pro.reason == ""
    assert pro.advisories


def test_two_future_imports_sharing_a_line_are_moved_once():
    source = (
        "x = 1\n"
        "%python\n"
        "from __future__ import annotations; "
        "from __future__ import generator_stop\n"
    )

    pro = jm._promote_leading_magic(source, native_lang="python")

    assert pro.reason
    assert pro.source.count("from __future__ import annotations") == 1
    assert pro.source.count("from __future__ import generator_stop") == 1
    assert pro.source.splitlines()[-1] == "x = 1"
    _compiles(pro.source)


@pytest.mark.parametrize(
    ("metadata", "expected"),
    [
        ({}, "python"),
        ({"language_info": {"name": "python3"}}, "python"),
        ({"kernelspec": {"language": "pyspark"}}, "python"),
        ({"language_info": {"name": "r"}}, "r"),
        ({"kernelspec": {"language": "julia"}}, "julia"),
    ],
)
def test_magic_promotion_language_preserves_explicit_non_python_metadata(
        metadata, expected):
    assert jm._magic_promotion_language_from_nb(
        {"metadata": metadata}
    ) == expected


def test_an_action_line_magic_is_never_relocated_or_reported_as_a_language_magic():
    """A statement-level `%run` is not a displaced language declaration."""
    source = "import os  # Oracle tool modification: added missing import\n%run /Users/x/Utils\n"
    pro = jm._promote_leading_magic(source)
    assert (pro.source, pro.reason, pro.dropped) == (source, "", []), (
        "an action magic must never be relocated")
    assert pro.advisories == []


def test_the_pre_existing_comment_above_magic_path_still_works():
    """Guard test for the behaviour that existed before this fix."""
    pro = jm._promote_leading_magic(
        "# Oracle tool modification: something\n%sql\nSELECT 1\n")
    out, reason, dropped = pro.source, pro.reason, pro.dropped
    assert reason == "annotation comment above magic", (
        "the pre-existing branch must stay distinguishable from the new one")
    assert dropped == []
    lines = out.split("\n")
    assert lines[0].strip() == "%sql"
    assert lines[1].startswith("--"), "annotation re-commented in SQL syntax"


def test_comment_above_md_sandbox_is_promoted_without_losing_content():
    source = "# Oracle tool modification: retained note\n%md-sandbox\n# Heading\n"

    pro = jm._promote_leading_magic(source)

    assert pro.reason == "annotation comment above magic"
    assert pro.source.splitlines() == [
        "%md-sandbox",
        "# Oracle tool modification: retained note",
        "# Heading",
    ]
    assert pro.dropped == []
    assert pro.advisories == []


@pytest.mark.parametrize("source", [
    "%python\nd = 1\n",          # already correct
    "import os\nx = 1\n",        # no magic at all
    "",
])
def test_repair_is_a_no_op_for_healthy_cells(source):
    assert jm._promote_leading_magic(source) == (source, "", [], [])


# ── Orchestration: the child path, where the incident actually landed ───────

def _drive_child(monkeypatch, child_source):
    """Drive the real `_inline_child_notebook` against an in-memory child."""
    path = "/Workspace/test_magic/child.ipynb"
    nb = {"cells": [{"cell_type": "code", "source": [child_source], "metadata": {},
                     "outputs": [], "execution_count": None}],
          "metadata": {}, "nbformat": 4, "nbformat_minor": 5}

    async def noop(*a, **k):
        return None

    monkeypatch.setitem(jm._notebook_content_cache, path, json.dumps(nb))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)   # no LLM in the child path
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_magic")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_magic/notebooks")
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    kernel = _Kernel()
    log = jm.NotebookLog(path, "test_magic", "child")
    result = asyncio.run(jm._inline_child_notebook(
        path, kernel, log, "test_magic", "", "", "", {}))
    executed = [e for e in kernel.executed if "base64.b64decode" not in e]
    return result, executed


def test_child_path_heals_a_displaced_magic_before_the_magic_gate(monkeypatch):
    """The gap that let this reach production.

    The child loop injected imports but never ran the promotion guard, so a
    magic displaced by an earlier run stayed on line 1. The guard must run
    before `stripped` is taken, since the magic gate keys off the first line —
    otherwise the cell is misread as ordinary Python and executed with the
    magic still in it.
    """
    corrupted = (
        "from collections import OrderedDict  # Oracle tool modification: added missing import\n"
        "%python\n"
        "d = OrderedDict()\n"
    )
    _result, executed = _drive_child(monkeypatch, corrupted)
    cell = next(e for e in executed if "OrderedDict" in e)
    # `%python` is an executable language magic: the child path hands it to the
    # kernel deliberately, and AIDP honours it ON LINE 0. The defect was the
    # position, so that is what this asserts — the magic must lead the code the
    # kernel receives, with the injected import below it.
    assert cell.split("\n")[0].strip() == "%python", (
        f"displaced magic reached the kernel off line 0: {cell.split(chr(10))[0]!r}")
    assert "d = OrderedDict()" in cell, "the cell's real work must still run"


def test_child_path_does_not_execute_a_python_magic_on_a_healthy_cell(monkeypatch):
    """Control for the test above: a correct cell is passed through unchanged."""
    _result, executed = _drive_child(monkeypatch, "%python\nd = OrderedDict()\n")
    cell = next(e for e in executed if "OrderedDict" in e)
    assert cell.split("\n")[0].strip() == "%python"
    assert "d = OrderedDict()" in cell


def test_both_paths_route_through_the_promotion_guard():
    """Structural guard: the injector has five call sites and the repair had
    one. Assert both loops call the guard so the child path cannot silently
    lose it again.
    """
    src = Path(jm.__file__).read_text()
    child_body = src[src.index("async def _inline_child_notebook("):
                     src.index("async def process_notebook(")]
    parent_body = src[src.index("async def process_notebook("):]
    assert "_promote_leading_magic(" in child_body, "child loop lost the guard"
    assert "_promote_leading_magic(" in parent_body, "parent loop lost the guard"


# ── The rest of the class: every injector that prepends ─────────────────────
#
# `_add_missing_imports` was only the injector that fired in the incident. The
# defect class is "an injector prepends at line 0", and three more members were
# found by execution after the first fix landed:
#
#   * `%pyspark` — a source alias canonicalized to AIDP `%python`
#     (`_EXEC_MAGICS`), but which none of the magic token sets listed, so the
#     import was still placed above it and the repair did not recognise it.
#   * `_preprocess_cell_source` — prepends the table-ref note; only its `%sql`
#     branch was magic-safe. It runs AFTER the promotion guard in the parent
#     loop, so a note it displaces is this run's kernel payload, not something
#     the next read can heal.
#   * `_ensure_dbutils_import` — prepends the aidp_compat import into the first
#     code cell. It runs on the DELIVERED artifact after every cell already
#     executed, so migration reports PASS and the displaced magic aborts on the
#     customer's first run.
#
# Plus a latent `SyntaxError` in the original fix: `from __future__` imports
# must precede every other statement, so inserting an import above one breaks
# the cell. Note `ast.parse` does NOT enforce future-import placement — only
# `compile(..., "exec")` does, which is what the kernel runs. These tests
# therefore compile.


def _compiles(source: str) -> None:
    """Compile a cell body the way the kernel does, magic line stripped.

    `ast.parse` accepts a misplaced `from __future__` import; `compile` with
    mode "exec" is what raises, so only this catches the real failure.
    """
    body = source
    if jm._leading_cell_magic(source) is not None:
        body = source.split("\n", 1)[1] if "\n" in source else ""
    compile(body, "<cell>", "exec")


# ── %pyspark is an alias for %python ────────────────────────────────────────

def test_pyspark_is_recognised_as_a_leading_cell_magic():
    assert jm._leading_cell_magic("%pyspark\nx = 1") == "%pyspark"


@pytest.mark.parametrize("magic", jm._PYTHON_MAGICS)
def test_python_magic_overrides_scala_notebook_default(magic):
    assert jm._cell_language(f"{magic}\nx = 1\n", "scala") == "python"


def test_python_magic_alias_boundary_does_not_claim_unknown_extensions():
    assert jm._cell_language("%pyspark-extra\nx = 1\n", "scala") == "scala"


def test_md_sandbox_is_classified_as_markdown_not_python():
    source = "%md-sandbox\nto_date(created_at)\n"

    assert jm._cell_language(source, "python") == "md"
    assert jm._add_missing_imports(source) == source


def test_generic_inserter_keeps_md_sandbox_on_line_zero():
    source = "%md-sandbox\n# Heading\n"

    out = jm._insert_below_leading_magic(
        source, "# Oracle tool modification: retained note"
    )

    assert out.splitlines() == [
        "%md-sandbox",
        "# Oracle tool modification: retained note",
        "# Heading",
    ]


def test_pyspark_magic_stays_on_line_0_and_import_lands_below():
    out = jm._add_missing_imports("%pyspark\nx = to_date(c)\n")
    lines = out.split("\n")
    assert lines[0].strip() == "%pyspark", "magic must remain the first line"
    assert TAG in lines[1], "injected import belongs directly below the magic"


def test_repair_promotes_a_displaced_pyspark_magic_and_keeps_the_import():
    source = (
        "from pyspark.sql.functions import to_date  # Oracle tool modification: added missing import\n"
        "%pyspark\nx = to_date(c)\n"
    )
    pro = jm._promote_leading_magic(source)
    out, reason, dropped = pro.source, pro.reason, pro.dropped
    assert reason, "a displaced %pyspark magic must be reported as healed"
    assert out.split("\n")[0].strip() == "%python"
    assert "# %pyspark" in out
    assert "from pyspark.sql.functions import to_date" in out, "valid Python must be kept"
    assert dropped == []


def test_the_python_magic_sets_cannot_drift_apart():
    """Structural: every Python-language magic must appear in all four sets.

    `%pyspark` was missing from three of them while the child exec gate ran it,
    which is exactly how the import kept landing above it.
    """
    for magic in jm._PYTHON_MAGICS:
        assert magic in jm._CELL_MAGIC_TOKENS, magic
        assert magic in jm._LANG_DECL_MAGICS, magic
        assert magic in jm._INJECTABLE_MAGICS, magic
        assert magic in jm._EXEC_MAGICS, magic


# ── from __future__ must stay ahead of an injected import ───────────────────

def test_future_import_stays_first_under_a_python_magic():
    out = jm._add_missing_imports(
        "%python\nfrom __future__ import annotations\nx = to_date(c)\n")
    assert out.split("\n")[0].strip() == "%python"
    assert TAG in out, "the import must still be injected"
    _compiles(out)


def test_future_import_stays_first_with_no_magic():
    """The same defect without a magic — insertion at line 0 breaks it too."""
    out = jm._add_missing_imports("from __future__ import annotations\nx = to_date(c)\n")
    assert TAG in out
    _compiles(out)


def test_a_docstring_above_a_future_import_keeps_both_ahead_of_the_injection():
    """A module docstring must stay above the `__future__` import that needs it."""
    out = jm._add_missing_imports(
        '%python\n"""Cell docs."""\nfrom __future__ import annotations\nx = to_date(c)\n')
    assert out.split("\n")[0].strip() == "%python"
    assert TAG in out
    _compiles(out)


def test_a_docstring_without_a_future_import_does_not_move_the_injection():
    """Fence: only a real `__future__` import may shift the insertion point.

    Treating every leading string literal as a preamble would change placement
    for ordinary cells and break the byte-identity guarantee above.
    """
    source = '"""Cell docs."""\nx = to_date(c)\n'
    out = jm._add_missing_imports(source)
    injected = [ln for ln in out.split("\n") if TAG in ln]
    assert out == "\n".join(injected) + "\n" + source


def test_unparseable_source_still_receives_its_import():
    """Opus repair code can be mid-edit; the injector must not start raising."""
    out = jm._add_missing_imports("%python\nx = to_date(  # unclosed\n")
    assert TAG in out


# ── _preprocess_cell_source: the table-ref note ─────────────────────────────

def test_table_ref_note_lands_below_a_leading_python_magic():
    out = jm._preprocess_cell_source('%python\ndf = spark.table("db.t")\n')
    lines = out.split("\n")
    assert lines[0].strip() == "%python", "the note must not displace the magic"
    assert TAG in lines[1], "the note belongs directly below the magic"


def test_table_ref_note_below_a_python_magic_survives_the_import_injector():
    """The stacked failure: note at line 0, then the import gate sees a comment
    on line 0 and stacks its block on top, leaving the magic on line 2."""
    out = jm._add_missing_imports(
        jm._preprocess_cell_source('%python\ndf = spark.table("db.t")\nd = OrderedDict()\n'))
    assert out.split("\n")[0].strip() == "%python"
    _compiles(out)


def test_table_ref_note_still_lands_below_a_sql_magic_as_a_sql_comment():
    out = jm._preprocess_cell_source("%sql\nSELECT * FROM db.t\n")
    lines = out.split("\n")
    assert lines[0].strip() == "%sql"
    assert lines[1].startswith("--"), "a Python '#' note breaks a SQL cell"


def test_table_ref_note_is_unchanged_for_a_cell_without_a_magic():
    out = jm._preprocess_cell_source('df = spark.table("db.t")\n')
    assert out.split("\n")[0].startswith("#"), "no magic — the note stays on line 0"


def test_preprocessor_fails_closed_for_a_displaced_magic():
    """A table rewrite must not turn a reported malformed cell into a new one."""
    source = 'y = 2\n%python\ndf = spark.table("db.t")\n'
    assert jm._promote_leading_magic(source).advisories
    assert jm._preprocess_cell_source(source) == source


def test_preprocessor_runs_for_a_healthy_mid_cell_matplotlib_magic():
    source = (
        'import matplotlib.pyplot as plt\n'
        '%matplotlib inline\n'
        'df = spark.table("db.t")\n'
    )
    assert jm._promote_leading_magic(source).advisories == []

    out = jm._preprocess_cell_source(source)

    assert "matplotlib.use('Agg')" in out
    assert "# %matplotlib inline" in out
    assert 'spark.table("default.db.t")' in out


def test_percent_directive_text_in_a_string_does_not_block_preprocessing():
    source = (
        'query = """\n%sql\nSELECT 1\n"""\n'
        'df = spark.table("db.t")\n'
    )
    assert jm._promote_leading_magic(source).advisories == []

    out = jm._preprocess_cell_source(source)

    assert 'spark.table("default.db.t")' in out


# ── _ensure_dbutils_import: the delivered artifact ──────────────────────────

def _one_code_cell(src: str) -> list:
    return [{"cell_type": "code", "source": [src]}]


@pytest.mark.parametrize("magic", ["%python", "%pyspark"])
def test_dbutils_import_lands_below_a_leading_python_magic(magic):
    """Skipping the cell is not an option: the import must precede the first
    dbutils use, which is in this very cell."""
    cells = jm._ensure_dbutils_import(
        _one_code_cell(f"{magic}\nv = dbutils.widgets.get('x')\n"))
    out = "".join(cells[0]["source"])
    lines = out.split("\n")
    assert lines[0].strip() == "%python", "the AIDP declaration must remain first"
    if magic == "%pyspark":
        assert "# %pyspark" in lines
    assert "from aidp_compat import" in lines[1]
    assert len(cells) == 1, "no extra cell may be inserted"


def test_an_r_cell_never_receives_the_python_dbutils_import():
    """`%r` was absent from the prefix list, so an R cell got a Python import."""
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": ["%r\nlibrary(dplyr)\n"]},
        {"cell_type": "code", "source": ["x = dbutils.fs.ls('/')\n"]},
    ])
    assert "aidp_compat" not in "".join(cells[0]["source"])
    assert "from aidp_compat import" in "".join(cells[1]["source"])


@pytest.mark.parametrize("magic", ["%run /Users/x/Utils", "%sql", "%scala", "%sh", "%fs", "%pip"])
def test_dbutils_import_still_skips_non_python_and_action_cells(magic):
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [f"{magic}\n"]},
        {"cell_type": "code", "source": ["x = dbutils.fs.ls('/')\n"]},
    ])
    assert "aidp_compat" not in "".join(cells[0]["source"])
    assert "from aidp_compat import" in "".join(cells[1]["source"])


def test_dbutils_import_is_byte_identical_for_a_plain_first_python_cell():
    source = "v = dbutils.widgets.get('x')\n"
    cells = jm._ensure_dbutils_import(_one_code_cell(source))
    assert "".join(cells[0]["source"]) == jm._AIDP_IMPORT_LINE + source


def test_dbutils_import_repairs_a_displaced_python_magic_in_the_same_cell():
    """The declaration moves; executable statements and cell count do not."""
    source = "x = 1\n%python\nv = dbutils.widgets.get('k')\n"
    assert jm._promote_leading_magic(source).advisories

    cells = jm._ensure_dbutils_import(_one_code_cell(source))

    assert len(cells) == 1
    out = "".join(cells[0]["source"])
    lines = out.splitlines()
    assert lines[0] == "%python"
    assert "from aidp_compat import" in lines[1]
    assert lines.index("x = 1") < lines.index("v = dbutils.widgets.get('k')")


def test_dbutils_import_repairs_the_first_cell_instead_of_using_a_later_cell():
    displaced = "x = 1\n%python\nv = dbutils.widgets.get('k')\n"
    later = "answer = 42\n"
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [displaced]},
        {"cell_type": "code", "source": [later]},
    ])

    assert len(cells) == 2
    assert "".join(cells[0]["source"]).startswith("%python\n" + jm._AIDP_IMPORT_LINE)
    assert "".join(cells[1]["source"]) == later


def test_dbutils_import_uses_a_later_safe_cell_after_an_advisory():
    displaced = "x = 1\n%sql\nSELECT dbutils_col FROM t\n"
    later = "answer = dbutils.widgets.get('k')\n"
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [displaced]},
        {"cell_type": "code", "source": [later]},
    ])

    assert len(cells) == 2
    assert "".join(cells[0]["source"]) == displaced
    assert "".join(cells[1]["source"]) == jm._AIDP_IMPORT_LINE + later


def test_dbutils_import_does_not_add_a_cell_when_only_candidate_is_advisory():
    displaced = "x = 1\n%sql\nSELECT dbutils_col FROM t\n"

    cells = jm._ensure_dbutils_import(_one_code_cell(displaced))

    assert len(cells) == 1
    assert "".join(cells[0]["source"]) == displaced


def test_advisory_only_dbutils_notebook_fails_final_validation_unchanged():
    """No import/new cell may hide an unresolved displaced language magic."""
    displaced = (
        "value = dbutils.widgets.get('k')\n"
        "%sql\n"
        "SELECT 1\n"
    )
    cells = _one_code_cell(displaced)

    delivered = jm._ensure_dbutils_import(cells)

    assert len(delivered) == 1
    assert "".join(delivered[0]["source"]) == displaced
    with pytest.raises(
        jm.FinalArtifactValidationError,
        match=r"unresolved displaced cell magic in cell\(s\) 1 .*%sql sits below code",
    ):
        jm._assert_final_notebook_cells(delivered, native_lang="python")


def test_child_finalization_rejects_advisory_without_mutating_source():
    """The shared child artifact boundary must enforce the same fail-closed rule."""
    displaced = (
        "value = dbutils.widgets.get('k')\n"
        "%sql\n"
        "SELECT 1\n"
    )

    with pytest.raises(
        jm.FinalArtifactValidationError,
        match=r"unresolved displaced cell magic in cell\(s\) 1 .*%sql sits below code",
    ):
        jm._finalize_child_artifact_sources(
            [displaced], native_lang="python"
        )

    assert displaced == (
        "value = dbutils.widgets.get('k')\n"
        "%sql\n"
        "SELECT 1\n"
    )


def test_dbutils_import_is_a_no_op_for_explicit_non_python_metadata():
    source = "x <- dbutils.widgets.get('k')\n%python\ny = 2\n"

    cells = jm._ensure_dbutils_import(_one_code_cell(source), native_lang="r")

    assert len(cells) == 1
    assert "".join(cells[0]["source"]) == source


@pytest.mark.parametrize(
    "source",
    [
        "%python\nfrom __future__ import annotations\n"
        "v = dbutils.widgets.get('k')\n",
        "x = 1\n%python\nfrom __future__ import annotations\n"
        "v = dbutils.widgets.get('k')\n",
    ],
)
def test_dbutils_import_stays_below_future_preamble(source):
    cells = jm._ensure_dbutils_import(_one_code_cell(source))

    assert len(cells) == 1
    out = "".join(cells[0]["source"])
    lines = out.splitlines()
    assert lines[0] == "%python"
    assert lines.index("from __future__ import annotations") < next(
        i for i, line in enumerate(lines) if "from aidp_compat import" in line
    )
    _compiles(out)


def test_dbutils_import_does_not_skip_a_healthy_mid_cell_line_magic():
    source = "import os\n%load_ext autoreload\nv = dbutils.widgets.get('k')\n"
    later = "answer = 42\n"
    assert jm._promote_leading_magic(source).advisories == []

    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [source]},
        {"cell_type": "code", "source": [later]},
    ])

    assert "".join(cells[0]["source"]) == jm._AIDP_IMPORT_LINE + source
    assert "".join(cells[1]["source"]) == later


@pytest.mark.parametrize("source", [
    "%time files = dbutils.fs.ls('/tmp')\n",
    "%timeit -n 1 dbutils.fs.ls('/tmp')\n",
    "%load_ext autoreload\nfiles = dbutils.fs.ls('/tmp')\n",
    "%autoreload 2\nfiles = dbutils.fs.ls('/tmp')\n",
])
def test_dbutils_import_precedes_a_leading_python_line_magic_in_same_cell(
        source, capsys):
    later = "answer = 42\n"
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [source]},
        {"cell_type": "code", "source": [later]},
    ])

    assert len(cells) == 2, "the repair must not split or add a cell"
    assert "".join(cells[0]["source"]) == jm._AIDP_IMPORT_LINE + source
    assert "".join(cells[1]["source"]) == later
    log = capsys.readouterr().out
    assert "before its" in log and "Python line magic" in log
    assert "below its" not in log


def test_preprocess_to_delivery_keeps_time_dbutils_fs_self_contained():
    """Drive the real ordering: preprocess first, final import injection last."""
    source = "%time files = dbutils.fs.ls('/tmp')\n"
    preprocessed = jm._preprocess_cell_source(source)
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": [preprocessed]},
        {"cell_type": "code", "source": ["answer = 42\n"]},
    ])

    delivered = "".join(cells[0]["source"])
    assert len(cells) == 2
    assert delivered == jm._AIDP_IMPORT_LINE + source
    assert "from aidp_compat import dbutils" in delivered.splitlines()[0]
    assert delivered.splitlines()[-1] == "%time files = dbutils.fs.ls('/tmp')"


def test_leading_python_line_magic_import_is_idempotent():
    source = "%time files = dbutils.fs.ls('/tmp')\n"
    cells = [{"cell_type": "code", "source": [source]}]
    once = jm._ensure_dbutils_import(cells)
    twice = jm._ensure_dbutils_import(once)

    assert twice == once
    assert "".join(twice[0]["source"]).count(
        "from aidp_compat import dbutils"
    ) == 1


def test_all_magic_cells_still_get_a_dedicated_import_cell():
    """Unchanged fallback: with no Python cell to land in, insert one."""
    cells = jm._ensure_dbutils_import([
        {"cell_type": "code", "source": ["%sql\nSELECT 1\n"]},
        {"cell_type": "code", "source": ["%scala\nval x = dbutils.fs.ls(\"/\")\n"]},
    ])
    assert len(cells) == 3
    assert "".join(cells[0]["source"]) == jm._AIDP_IMPORT_LINE


# ── Persistence: the repair has to reach the file, not just the kernel ──────
#
# Promotion healed `child_source` AND `child_cells[ci]`, so both the kernel
# payload and `migrated_child_cells` (what save-back ships) already carried the
# corrected cell. Two gates at the end of `_inline_child_notebook` threw it
# away:
#
#   if cells_fixed == 0:                            -> return before saving
#   if child_path not in _notebook_content_cache:   -> cache never refreshed
#
# A promotion is a DETERMINISTIC rewrite: it never goes through the LLM fix
# loop, so it never moves `cells_fixed`. Both gates have to key off "did
# anything change", not "did the LLM fix something". Measured before the fix:
# with a promotion as the only change the payload count was 0 and the disk kept
# the displacement forever; add any unrelated change (a /tmp setup cell) and
# the very same payload already carried `%python` on line 0 — the healing was
# never the problem, only the gate.
#
# The cache matters as much as the disk here: it is ALSO the source the child
# was loaded from, so a stale entry re-serves the displacement to the next
# `%run` for the rest of the job.

_LOG_SINK: list = []

_CORRUPTED_CHILD = (
    "from collections import OrderedDict  # Oracle tool modification: added missing import\n"
    "%python\n"
    "d = OrderedDict()\n"
)


def _drive_child_under_migrated_base(
        monkeypatch, child_source, on_disk_cells=None, metadata=None,
        kernel=None, strict=False, base=None):
    """Drive the real child inliner with the child under MIGRATED_BASE.

    `_drive_child` deliberately sits OUTSIDE MIGRATED_BASE, where save-back is
    refused to protect original notebooks, so persistence cannot be observed
    there at all.
    """
    base = str(base or "/Workspace/test_magic/notebooks")
    path = base + "/child.ipynb"
    child_sources = ([child_source] if isinstance(child_source, str)
                     else list(child_source))
    nb = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                     "outputs": [], "execution_count": None}
                    for source in child_sources],
          "metadata": metadata or {}, "nbformat": 4, "nbformat_minor": 5}

    async def noop(*a, **k):
        return None

    monkeypatch.setitem(jm._notebook_content_cache, path, json.dumps(nb))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(Path(base).parent))
    monkeypatch.setattr(jm, "MIGRATED_BASE", base)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    if on_disk_cells is None:
        on_disk_cells = len(child_sources)
    kernel = kernel or _Kernel(on_disk_cells=on_disk_cells)
    log = jm.NotebookLog(path, "test_magic", "child")
    _LOG_SINK.clear()
    _real_log = log.log
    log.log = lambda msg, *a, **k: (_LOG_SINK.append(str(msg)), _real_log(msg, *a, **k))[1]
    result = asyncio.run(jm._inline_child_notebook(
        path, kernel, log, "test_magic", "", "", "", {}))
    cached = json.loads(jm._notebook_content_cache[path])
    return result, kernel, cached


def test_a_promotion_alone_is_written_back_to_the_file(monkeypatch):
    """The repair's whole stated purpose: heal a copy already on disk."""
    _result, kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, _CORRUPTED_CHILD)
    assert kernel.save_payloads, (
        "a promotion was the only change and nothing was written — "
        "the repair healed memory only")
    cells = kernel.save_payloads[-1]["cells"]
    assert cells["0"].split("\n")[0].strip() == "%python", (
        f"save-back payload still carries the displacement: {cells['0']!r}")


def test_a_promotion_refreshes_the_in_memory_cache(monkeypatch):
    """The cache is the source the next `%run` reads first.

    Left stale, it re-serves the displacement for the rest of the job even
    after the file on disk has been healed.
    """
    _result, _kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, _CORRUPTED_CHILD)
    first_line = "".join(cached["cells"][0]["source"]).split("\n")[0].strip()
    assert first_line == "%python", (
        f"cache still holds the pre-promotion source: {first_line!r}")


def test_same_language_customer_prefix_promotion_is_saved_in_place(monkeypatch):
    """Drive the real child loop for the new semantic promotion branch."""
    source = "x = 1\n%python\ny = x + 1\n"

    result, kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, source, on_disk_cells=1
    )

    assert result["status"] == "ok", result
    assert kernel.save_payloads
    saved = kernel.save_payloads[-1]["cells"]["0"]
    assert saved.splitlines() == ["%python", "x = 1", "y = x + 1"]
    assert len(kernel.save_payloads[-1]["cells"]) == 1
    assert "".join(cached["cells"][0]["source"]) == saved


def test_a_healthy_child_still_writes_nothing(monkeypatch):
    """Control: the gate must open for a real change, not for every child."""
    _result, kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, "%python\nx = 1\n")
    assert not kernel.save_payloads, "unchanged child must not be rewritten"


@pytest.mark.parametrize(
    ("child_source", "saved_fragment"),
    [
        (
            "%python\nd = OrderedDict()\n",
            "from collections import OrderedDict",
        ),
        (
            "%scala\nval j = new JSONObject()\n",
            "import org.json.JSONObject",
        ),
        (
            "%python\n"
            "value = dbutils.jobs.taskValues.get(taskKey='upstream', key='value')\n",
            "oidlUtils.parameters.getParameter",
        ),
        (
            "left = pd.DataFrame(values)\n"
            "result = left.append(right, ignore_index=True)\n",
            "result = pd.concat([left, right], ignore_index=True)",
        ),
        (
            "%python\nfrom transformers import AdamW\n",
            "from torch.optim import AdamW",
        ),
        (
            "%python\n"
            "from slack_sdk import WebClient\n"
            "client = WebClient(token=tok)\n"
            "client.chat_postMessage(channel='C1', text='x')\n",
            "# client.chat_postMessage",
        ),
    ],
)
def test_each_deterministic_child_rewrite_is_written_back(
        monkeypatch, child_source, saved_fragment):
    """Every persistable pre-execution rewrite must open the save gate.

    These cover both languages and each deterministic transform family around
    execution: Slack neutralisation, missing imports, API renames, learned
    swaps, and pandas compatibility.  None increments ``cells_fixed``.
    """
    result, kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, child_source)

    assert result["status"] == "ok", result
    assert result["cells_fixed"] == 0, result
    assert kernel.save_payloads, "deterministic source change was not persisted"
    assert saved_fragment in kernel.save_payloads[-1]["cells"]["0"]
    assert saved_fragment in "".join(cached["cells"][0]["source"])


def test_child_line_magic_keeps_deterministic_rewrites_and_has_no_false_note(
        monkeypatch):
    """Exercise the real child loop, not only the classifier in isolation."""
    source = "import os\n%load_ext autoreload\nd = OrderedDict()\n"

    result, kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, source)

    assert result["status"] == "ok", result
    assert not [note for note in result.get("notes", [])
                if "%load_ext sits below code" in note]
    assert kernel.save_payloads, "the missing-import rewrite must be persisted"
    saved = kernel.save_payloads[-1]["cells"]["0"]
    assert "from collections import OrderedDict" in saved
    assert "%load_ext autoreload" in saved
    assert saved == "".join(cached["cells"][0]["source"])


@pytest.mark.parametrize(("source", "import_fragment", "magic_line"), [
    (
        "%time result = OrderedDict(records)\n",
        "from collections import OrderedDict",
        "%time result = OrderedDict(records)",
    ),
    (
        "%timeit -n 1 to_date(c)\n",
        "from pyspark.sql.functions import to_date",
        "%timeit -n 1 to_date(c)",
    ),
    (
        "%load_ext customer_extension\nd = OrderedDict()\n",
        "from collections import OrderedDict",
        "%load_ext customer_extension",
    ),
    (
        "%autoreload 2\nd = OrderedDict()\n",
        "from collections import OrderedDict",
        "%autoreload 2",
    ),
])
def test_child_leading_python_line_magic_executes_and_persists_import(
        monkeypatch, source, import_fragment, magic_line):
    """The child gate and import classifier must share one line-magic policy.

    A leading Python line magic used to be counted as OK without execution,
    before the child reached `_add_missing_imports`.  That left names defined
    by `%time` unbound and persisted the original missing-import source.
    """
    result, kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, source, on_disk_cells=1
    )

    assert result["status"] == "ok", result
    executed = [
        code for code in kernel.executed
        if magic_line in code and "base64.b64decode" not in code
    ]
    assert len(executed) == 1, kernel.executed
    assert executed[0].splitlines()[0].startswith(import_fragment)
    assert magic_line in executed[0].splitlines()[1:]

    assert kernel.save_payloads, "the injected import must reach the child artifact"
    saved_cells = kernel.save_payloads[-1]["cells"]
    assert len(saved_cells) == 1, "the repair must not split or add a child cell"
    saved = saved_cells["0"]
    assert saved == executed[0]
    assert saved == "".join(cached["cells"][0]["source"])


def test_deterministic_child_rewrite_is_idempotent(monkeypatch):
    """A healed cache must not trigger another write on the next ``%run``."""
    source = "%python\nd = OrderedDict()\n"
    _first, first_kernel, cached = _drive_child_under_migrated_base(
        monkeypatch, source)
    assert len(first_kernel.save_payloads) == 1

    base = "/Workspace/test_magic/notebooks"
    path = base + "/child.ipynb"
    second_kernel = _Kernel(on_disk_cells=1)
    log = jm.NotebookLog(path, "test_magic", "child")
    second = asyncio.run(jm._inline_child_notebook(
        path, second_kernel, log, "test_magic", "", "", "", {}))

    assert second["status"] == "ok", second
    assert not second_kernel.save_payloads, (
        "an already-persisted deterministic rewrite must be a no-op")
    assert json.loads(jm._notebook_content_cache[path]) == cached


def test_native_scala_prefix_is_written_back(monkeypatch):
    """Metadata-driven language normalization is also a source rewrite."""
    result, kernel, cached = _drive_child_under_migrated_base(
        monkeypatch,
        "val answer = 42\n",
        metadata={"language_info": {"name": "scala"}},
    )

    assert result["status"] == "ok", result
    assert kernel.save_payloads[-1]["cells"]["0"].startswith("%scala\n")
    assert "".join(cached["cells"][0]["source"]).startswith("%scala\n")


def test_nested_run_path_normalization_is_written_back(monkeypatch):
    """The generic diff also covers transforms that bypass the cell fix loop."""
    base = "/Workspace/test_magic/notebooks"
    parent_path = base + "/parent.ipynb"
    nested_path = base + "/dep.ipynb"

    def _notebook(source):
        return json.dumps({
            "cells": [{"cell_type": "code", "source": [source],
                       "metadata": {}, "outputs": [],
                       "execution_count": None}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
        })

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setitem(jm._notebook_content_cache, parent_path,
                        _notebook("%run dep\n"))
    monkeypatch.setitem(jm._notebook_content_cache, nested_path,
                        _notebook("x = compute()\n%python\ny = 1\n"))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_magic")
    monkeypatch.setattr(jm, "MIGRATED_BASE", base)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    kernel = _Kernel(on_disk_cells=1)
    log = jm.NotebookLog(parent_path, "test_magic", "parent")

    result = asyncio.run(jm._inline_child_notebook(
        parent_path, kernel, log, "test_magic", "", "", "", {}))

    assert result["status"] == "ok", result
    assert kernel.save_payloads, "normalized nested %run path was not persisted"
    assert kernel.save_payloads[-1]["cells"]["0"] == f"%run {nested_path}"
    assert not any("%python sits below code" in note
                   for note in result.get("notes", [])), result


def test_dropped_tool_line_is_returned_as_a_child_note(monkeypatch):
    """A language-invalid tool line must reach the report, not only stdout."""
    source = (
        "from pyspark.sql.functions import to_date  "
        "# Oracle tool modification: added missing import\n"
        "%sql\n"
        "SELECT to_date(created_at) FROM events\n"
    )
    result, _kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, source)

    assert result["status"] == "ok", result
    assert any("dropped tool-injected line" in note and "to_date" in note
               for note in result.get("notes", [])), result


def test_child_notes_survive_a_later_strict_error(monkeypatch):
    """An early advisory must not disappear when a later cell aborts."""
    result, _kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch,
        [
            "x = compute()\n%sql\nSELECT 1\n",
            "%pip install customer-package\n",
        ],
        strict=True,
    )

    assert result["status"] == "error", result
    assert any("%sql sits below code" in note
               for note in result.get("notes", [])), result


def test_a_promotion_does_not_trip_the_cell_count_drift_guard(monkeypatch):
    """Promotion rewrites a cell in place; it must not add or drop one.

    A count mismatch makes save-back refuse and return `status: error`, so
    this is what decides whether opening the gate is safe at all.
    """
    result, kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, _CORRUPTED_CHILD, on_disk_cells=1)
    assert result["status"] == "ok", result.get("error")
    assert len(kernel.save_payloads[-1]["cells"]) == 1, "cell count must be preserved"


def test_the_save_back_log_names_the_rewrite_not_zero_fixes(monkeypatch):
    """Opening the gate for a non-fix must not make the log lie.

    `cells_fixed` is still 0 for a promotion-only save, so the pre-existing
    line reported "Saved 0 fixed cells back" — during triage that reads as a
    no-op write. The log has to name what actually caused the save; attributing
    a promotion event was already the reason the guard returns a reason string
    rather than a bare boolean.
    """
    _result, _kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, _CORRUPTED_CHILD)
    log_lines = [ln for ln in _LOG_SINK if "back to:" in ln]
    assert log_lines, "a save-back must be logged"
    assert "Saved 0 fixed cells" not in " ".join(log_lines), (
        f"misleading save-back log: {log_lines}")
    assert "rewrite" in " ".join(log_lines).lower(), (
        f"the log must name what caused the save: {log_lines}")


@pytest.mark.parametrize(
    "kernel",
    [
        _Kernel(save_result={
            "status": "error",
            "outputs": [{"type": "error", "ename": "OSError",
                         "evalue": "disk write failed"}],
        }),
        _Kernel(save_exception=OSError("disk write raised")),
        _Kernel(save_replaced=0),
        _Kernel(save_result={
            "status": "ok",
            "outputs": [{"type": "stream", "name": "stdout",
                         "text": ("Command ID abc-123 failed with "
                                  "java.lang.RuntimeException")}],
        }),
        _Kernel(save_result={
            "status": "ok",
            "outputs": [{"type": "stream", "name": "stdout",
                         "text": "Replaced 1/2 cells (config_offset=0)"}],
        }),
        _Kernel(save_result={
            "status": "ok",
            "outputs": [{"type": "stream", "name": "stdout",
                         "text": ("Replaced 1/1 cells (config_offset=0)\n"
                                  "Replaced 1/2 cells (config_offset=1)")}],
        }),
        _Kernel(save_result="not-a-result-dict"),
    ],
    ids=["returned-error", "exception", "partial", "wrapped-stream-error",
         "wrong-visited-count", "contradictory-receipts", "malformed-result"],
)
def test_child_save_failure_is_loud_and_does_not_publish_cache(
        monkeypatch, kernel):
    """A failed/ambiguous disk write must remain visible and retryable."""
    source = "%python\nd = OrderedDict()\n"
    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, source, kernel=kernel)

    assert attempted.save_payloads, "the save path was not exercised"
    assert result["status"] == "error", result
    assert result.get("kind") == "CHILD_PERSISTENCE", result
    cached_source = "".join(cached["cells"][0]["source"])
    assert "OrderedDict  # Oracle tool modification" not in cached_source
    assert cached_source.strip() == source.strip()
    assert not [line for line in _LOG_SINK if "Saved " in line], _LOG_SINK
    assert any("ERROR" in line and "save" in line.lower()
               for line in _LOG_SINK), _LOG_SINK


def test_child_save_accepts_duplicate_consistent_receipts(monkeypatch):
    """AIDP's double-send transport must not turn a good write into failure."""
    receipt = "Replaced 1/1 cells (config_offset=0)"
    kernel = _Kernel(save_result={
        "status": "ok",
        "outputs": [
            {"type": "stream", "name": "stdout", "text": receipt},
            {"type": "stream", "name": "stdout", "text": receipt},
        ],
    })

    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, "%python\nd = OrderedDict()\n", kernel=kernel
    )

    assert result["status"] == "ok", result
    assert attempted.save_payloads
    assert not [note for note in result["notes"]
                if "SAVE RECEIPT UNAVAILABLE" in note]
    assert "from collections import OrderedDict" in (
        "".join(cached["cells"][0]["source"]))


def test_child_save_missing_receipt_fails_without_publishing_cache(monkeypatch):
    """A missing persistence acknowledgement must fail closed."""
    kernel = _Kernel(save_result={"status": "ok", "outputs": []})

    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, "%python\nd = OrderedDict()\n", kernel=kernel
    )

    assert result["status"] == "error", result
    assert result.get("kind") == "CHILD_PERSISTENCE", result
    assert attempted.save_payloads
    assert "from collections import OrderedDict" not in (
        "".join(cached["cells"][0]["source"]))
    assert not [line for line in _LOG_SINK if "Saved " in line]


def test_child_save_checks_alignment_before_atomic_replacement(monkeypatch):
    """Validate alignment before creating the replacement transaction."""
    result, kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, "%python\nd = OrderedDict()\n"
    )

    assert result["status"] == "ok", result
    save_code = next(
        code for code in kernel.executed
        if "payload = json.loads(base64.b64decode(" in code
    )
    compile(save_code, "<child-save-back>", "exec")
    guard_at = save_code.index("child save replacement mismatch")
    temp_at = save_code.index("tempfile.NamedTemporaryFile(")
    dump_at = save_code.index("json.dump(nb, temp_file, indent=1)")
    replace_at = save_code.index("os.replace(temp_path, path)")
    assert guard_at < temp_at < dump_at < replace_at
    assert "builtins.open(path, 'w')" not in save_code


def test_child_save_atomically_replaces_complete_notebook(monkeypatch, tmp_path):
    """The real generated save program preserves structure and file mode."""
    source = "%python\nd = OrderedDict()\n"
    base = tmp_path / "notebooks"
    base.mkdir()
    path = base / "child.ipynb"
    original = {
        "cells": [
            {"cell_type": "markdown", "source": ["customer notes"],
             "metadata": {"keep": True}},
            {"cell_type": "code", "source": [source], "metadata": {},
             "outputs": [], "execution_count": None},
        ],
        "metadata": {"customer": "preserved"},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(original, indent=2), encoding="utf-8")
    path.chmod(0o640)
    kernel = _ExecutingSaveKernel(on_disk_cells=1)

    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, source, kernel=kernel, base=base,
        metadata=original["metadata"])

    assert result["status"] == "ok", result
    assert attempted.save_payloads
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["metadata"] == original["metadata"]
    assert saved["cells"][0] == original["cells"][0]
    assert "from collections import OrderedDict" in (
        "".join(saved["cells"][1]["source"]))
    assert "from collections import OrderedDict" in (
        "".join(cached["cells"][0]["source"]))
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert not list(base.glob(".child.ipynb.*.aidp-tmp"))


@pytest.mark.parametrize("failure", ["dump", "replace"])
def test_atomic_child_save_failure_preserves_original_bytes(
        monkeypatch, tmp_path, failure):
    """Neither a partial dump nor failed replace may damage the destination."""
    source = "%python\nd = OrderedDict()\n"
    base = tmp_path / "notebooks"
    base.mkdir()
    path = base / "child.ipynb"
    original = {
        "cells": [{"cell_type": "code", "source": [source], "metadata": {},
                   "outputs": [], "execution_count": None}],
        "metadata": {"customer": "original"},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    original_bytes = (json.dumps(original, indent=2) + "\n").encode("utf-8")
    path.write_bytes(original_bytes)
    path.chmod(0o640)
    kernel = _ExecutingSaveKernel(
        on_disk_cells=1,
        fail_dump=failure == "dump",
        fail_replace=failure == "replace",
    )

    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, source, kernel=kernel, base=base,
        metadata=original["metadata"])

    assert attempted.save_payloads
    assert result["status"] == "error", result
    assert result.get("kind") == "CHILD_PERSISTENCE", result
    assert path.read_bytes() == original_bytes
    assert stat.S_IMODE(path.stat().st_mode) == 0o640
    assert "".join(cached["cells"][0]["source"]).strip() == source.strip()
    assert not list(base.glob(".child.ipynb.*.aidp-tmp"))


def test_child_save_accepts_complete_legacy_config_offset_receipt(monkeypatch):
    """A prepended config cell raises visited count but is not a child cell."""
    kernel = _Kernel(save_result={
        "status": "ok",
        "outputs": [{"type": "stream", "name": "stdout",
                     "text": "Replaced 1/2 cells (config_offset=1)"}],
    })
    result, attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, "%python\nd = OrderedDict()\n", kernel=kernel)

    assert result["status"] == "ok", result
    assert attempted.save_payloads
    assert "from collections import OrderedDict" in (
        "".join(cached["cells"][0]["source"]))


def test_failed_child_save_is_retried_from_the_original_cache(monkeypatch):
    """A failed first write must not make the next run look already healed."""
    source = "%python\nd = OrderedDict()\n"
    failed_kernel = _Kernel(save_result={
        "status": "error",
        "outputs": [{"type": "error", "ename": "OSError",
                     "evalue": "disk write failed"}],
    })
    first, _attempted, cached = _drive_child_under_migrated_base(
        monkeypatch, source, kernel=failed_kernel)
    assert first["status"] == "error", first
    assert "".join(cached["cells"][0]["source"]).strip() == source.strip()

    base = "/Workspace/test_magic/notebooks"
    path = base + "/child.ipynb"
    retry_kernel = _Kernel(on_disk_cells=1)
    log = jm.NotebookLog(path, "test_magic", "child-retry")
    second = asyncio.run(jm._inline_child_notebook(
        path, retry_kernel, log, "test_magic", "", "", "", {}))

    assert second["status"] == "ok", second
    assert retry_kernel.save_payloads, "retry trusted a cache that was never saved"
    assert "from collections import OrderedDict" in (
        retry_kernel.save_payloads[-1]["cells"]["0"])


# ── Reporting a displacement we must NOT touch ─────────────────────────────
#
# One `return src, False, []` served three different situations — untagged
# code below which a language magic is stranded, an action magic we refuse to
# relocate, and "no magic here at all" — so the caller could only see "nothing
# happened". The first two are broken cells; the third is normal.
#
# Silence was not merely a missing log line. `_add_missing_imports` reads line
# 0 to decide whether a cell has a magic, so a cell the repair declined to
# touch looks magic-free to the injector, which then prepends ABOVE the
# already-displaced magic and pushes it down one more line per matching rule.
# Driven end to end, the kernel received the magic on line 2 with `status: ok`,
# `cells_fixed: 0` and no operator-visible trace.
#
# The report must NOT travel on `reason`: that channel means the promotion
# changed source. An advisory is separate precisely so reporting a non-change
# cannot trigger a write; the final source comparison remains unchanged.


def test_no_advisory_when_there_is_no_magic_to_strand():
    assert jm._promote_leading_magic("x = 1\ny = 2\n").advisories == []


def test_no_advisory_for_a_healthy_cell():
    assert jm._promote_leading_magic("%python\nd = 1\n").advisories == []


@pytest.mark.parametrize("source", [
    "%run child\n",
    "%pip install package\n",
    "%sh\necho ok\n",
    "%fs ls /\n",
    "%conf\nspark.setting value\n",
    "%customer_magic argument\n",
    "%%time\nx = 1\n",
])
def test_healthy_action_and_unknown_magics_do_not_get_false_advisories(source):
    """A magic already on line 0 is not displaced, regardless of policy."""
    assert jm._promote_leading_magic(source).advisories == []


@pytest.mark.parametrize("magic", [
    "%conf", "%time", "%timeit", "%matplotlib inline", "%load_ext autoreload",
    "%autoreload 2", "%pip install package", "%run child", "%customer_magic",
])
def test_mid_cell_line_magic_is_not_a_displaced_language_advisory(magic):
    """Statement-level IPython magics may legally follow ordinary Python."""
    source = f"x = compute()\n{magic}\ny = 1\n"
    pro = jm._promote_leading_magic(source)
    assert pro.source == source
    assert pro.reason == ""
    assert pro.dropped == []
    assert pro.advisories == []


@pytest.mark.parametrize("magic", [
    "%python", "%pyspark", "%scala", "%sql", "%r", "%md", "%md-sandbox",
    "%%time",
])
def test_displaced_language_and_cell_magics_remain_reported(magic):
    source = f"x = compute()\n{magic}\ny = 1\n"
    pro = jm._promote_leading_magic(source)
    assert pro.source == source
    assert pro.reason == ""
    assert pro.dropped == []
    assert pro.advisories and magic in pro.advisories[0]


@pytest.mark.parametrize("token", ["%sql", "%python", "%s", "%Y", "%md"])
def test_percent_directive_text_inside_a_string_is_not_an_advisory(token):
    source = f'query = """\n{token}\nSELECT 1\n"""\nprint(query)\n'
    assert jm._logical_ipython_magic_lines(source) == set()
    assert jm._promote_leading_magic(source).advisories == []


def test_no_advisory_when_the_repair_actually_heals():
    """A healed cell reports through `reason`; the advisory channel stays empty."""
    pro = jm._promote_leading_magic(
        "import os  # Oracle tool modification: added missing import\n%python\nd = 1\n")
    assert pro.reason and pro.advisories == []


def test_an_advisory_never_opens_the_child_save_gate(monkeypatch):
    """The M3 interaction: reporting a non-change must not rewrite the file.

    Routing the advisory through `reason` would turn it into a source change
    and write the notebook — while logging a promotion — for a cell nothing
    touched.
    """
    _result, kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, "x = compute()\n%sql\nSELECT 1\n")
    assert not kernel.save_payloads, (
        "an unchanged cell must not be written back")
    advisories = [ln for ln in _LOG_SINK if "%sql" in ln and "reorder" in ln]
    assert advisories, f"the advisory must be logged: {_LOG_SINK}"


def test_the_child_returns_advisories_so_the_parent_can_report_them(monkeypatch):
    """The child loop cannot reach `migration_notes`; the parent appends."""
    result, _kernel, _cached = _drive_child_under_migrated_base(
        monkeypatch, "x = compute()\n%sql\nSELECT 1\n")
    assert result.get("notes"), (
        f"advisories must reach the caller, not only stdout: {result}")
    assert any("%sql" in n for n in result["notes"])


def test_both_loops_surface_advisories():
    """Structural: the child path lost the promotion guard once already."""
    src = Path(jm.__file__).read_text()
    child_body = src[src.index("async def _inline_child_notebook("):
                     src.index("async def process_notebook(")]
    parent_body = src[src.index("async def process_notebook("):]
    assert ".advisories" in child_body, "child loop drops advisories"
    assert ".advisories" in parent_body, "parent loop drops advisories"


# ── The tag must be a real tool edit, not a substring ──────────────────────
#
# The old generic tag check matched anywhere, including inside a customer string
# literal, and every tagged line above a non-injectable magic was dropped
# wholesale regardless of whether it was an import or an annotation. Kept lines
# also stayed in Python comment syntax under `%sql`/`%scala`, where `#` is not
# a comment at all.


def test_a_customer_string_containing_a_comment_shaped_tag_is_not_relocated():
    """A comment-shaped tool tag inside a string literal is customer data.

    This is the destructive regression: a generic regex saw the ``#`` inside
    the string, classified the assignment as tool-authored, then dropped the
    whole line when promoting a non-Python magic.
    """
    source = 's = "# Oracle tool modification: fake"\n%sql\nSELECT 1\n'
    pro = jm._promote_leading_magic(source)
    assert pro.source == source, "a string literal must not be treated as a tool edit"
    assert pro.dropped == [], "customer code must never enter the destructive drop path"
    assert pro.advisories, "and the stranded magic must still be reported"


def test_arbitrary_tagged_code_is_not_treated_as_missing_import_provenance():
    """The tag alone never authorises relocation or deletion of executable code."""
    source = (
        "x = compute()  # Oracle tool modification: model-authored replacement\n"
        "%python\n"
        "d = 1\n"
    )
    pro = jm._promote_leading_magic(source)
    assert pro.source == source
    assert pro.reason == ""
    assert pro.dropped == []
    assert pro.advisories


@pytest.mark.parametrize(
    "emitted_line,language",
    [(line, language) for _pattern, line, language in jm._IMPLICIT_IMPORT_RULES],
)
def test_every_deterministic_missing_import_has_exact_repair_provenance(
        emitted_line, language):
    """Every deterministic injector output satisfies the repair provenance check."""
    expected = "//" if language == "scala" else "#"
    assert jm._missing_import_edit_marker(emitted_line) == expected


def test_a_tagged_annotation_above_a_sql_magic_is_kept_not_dropped():
    """An annotation is not dead text: the comment branch keeps and re-comments
    one, so the tool-edit branch must not silently delete it."""
    corrupted = (
        "-- Oracle tool modification: table refs tagged to default catalog/schema\n"
        "from datetime import date  # Oracle tool modification: added missing import\n"
        "%sql\nSELECT to_date(x) FROM t\n"
    )
    pro = jm._promote_leading_magic(corrupted)
    assert pro.source.split("\n")[0].strip() == "%sql"
    assert "table refs tagged" in pro.source, "the annotation must survive"
    assert len(pro.dropped) == 1, (
        f"only the import is dead text in a SQL cell: {pro.dropped}")


def test_a_kept_comment_is_re_commented_in_the_magic_language():
    """`#` is not a SQL comment — keeping it verbatim breaks the cell."""
    corrupted = (
        "# hdr\n"
        "from datetime import date  # Oracle tool modification: added missing import\n"
        "%sql\nSELECT to_date(x) FROM t\n"
    )
    pro = jm._promote_leading_magic(corrupted)
    lines = pro.source.split("\n")
    assert lines[0].strip() == "%sql"
    assert lines[1].startswith("--"), f"Python comment left inside SQL: {lines[1]!r}"
    assert "hdr" in lines[1], "the comment's text must survive"


def test_a_python_tagged_import_above_a_scala_magic_is_dropped():
    """A `#`-tagged Python import cannot run in a Scala cell.

    The tag's own marker identifies the language it was written for, so a
    mismatch against the magic means the line was never valid here.
    """
    corrupted = (
        "import os  # Oracle tool modification: added missing import\n"
        "%scala\nval x = 1\n"
    )
    pro = jm._promote_leading_magic(corrupted)
    assert pro.source.split("\n")[0].strip() == "%scala"
    assert pro.dropped == ["import os  # Oracle tool modification: added missing import"]
    assert "import os" not in pro.source, "Python left live inside a Scala cell"
