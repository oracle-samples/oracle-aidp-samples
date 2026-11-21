"""Tests for `%pip`/`!pip` cell handling in both migration paths.

AIDP's kernel rejects shell-escape `!pip` outright ("Invalid pip command
format! Supported format: %pip install <package>") and accepts `%pip` only as a
cell's sole statement, so an install line can never be executed during
migration. Both paths — task cells in `process_notebook` and dependency cells
in `_inline_child_notebook` — therefore install the packages through the
cluster-libraries API and comment the install line out.

Two properties make this easy to get wrong, and both are pinned here:

* **Detection must consider every line of a cell, not just its first
  statement.** Anything above the install hides it, and the common case is an
  older commented-out pip line left above the live one. A missed line reaches
  the kernel, is rejected, and burns the LLM repair budget on a rewrite the
  prompt forbids.
* **Detection must ignore inert text.** Commented lines, and lines inside
  string literals, are not commands. Rewriting a string literal would change
  the value the notebook computes, not just the code it runs.

The contract asserted is deliberately NOT "this notebook converts": repair
outcomes are non-deterministic (GAP-12 / playbook rule 4.26), so a conversion
verdict is not a trustworthy assertion. These tests assert observable
behaviour instead:

  1. the handler fires for an install anywhere in the cell, and only for a
     real install;
  2. no executable install line is ever sent to the kernel;
  3. no executable install line survives into a saved notebook, including
     dependency notebooks migrated code-only;
  4. co-located Python still reaches the kernel and keeps its bindings;
  5. a failed install, or a cell that cannot be rewritten safely, is reported
     as a failure rather than as success.

Per AGENTS.md ("fix the class, not the instance"), the shape grid is asserted
on the shared helpers AND at the orchestration level, driving the real cell
loop with a fake kernel — resolver-in-isolation tests do not count as matrix
coverage. The named cases in `_SHAPES` and
`test_real_job_pip_cells_install_and_never_execute` are the cell shapes
observed in customer notebooks.

Usage:
    pytest tests/test_pip_magic_gate.py -v
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import cell_analyzer  # noqa: E402
import cluster_lifecycle  # noqa: E402
import job_migrate as jm  # noqa: E402


# Any line that would still be handed to the AIDP kernel as a pip install.
# Uses the production pattern so a widened detector cannot leave this guard
# behind (it previously hard-coded `%pip|!pip` and would have missed `!pip3`).
_EXECUTABLE_PIP_RE = jm._PIP_INSTALL_LINE_RE


def _executable_pip_lines(source: str) -> list:
    """Pip install lines that are NOT commented out — i.e. would still run."""
    return [ln for ln in source.split("\n") if _EXECUTABLE_PIP_RE.match(ln)]


# ─────────────────────────────────────────────────────────────────────────
# Part A — the shape grid, on the shared detection helper
# ─────────────────────────────────────────────────────────────────────────

# (id, cell source, expected package list)
_SHAPES = [
    # --- fires before the fix too (pip is the opening statement) ---
    ("pip_only_first", "!pip install xgboost", ["xgboost"]),
    ("pip_magic_first", "%pip install xgboost", ["xgboost"]),
    ("leading_blank_line", "\n!pip install XlsxWriter  --quiet", ["XlsxWriter"]),
    ("pip_then_python",
     "!pip install dataframe_image\nimport dataframe_image as dfi",
     ["dataframe_image"]),

    # --- the regressions: anything before the pip line hid the handler ---
    ("leading_commented_pip__merge_rate_check",
     "# !pip install mlflow  --quiet\n"
     "# dbutils.library.restartPython()\n"
     "!pip install XlsxWriter  --quiet",
     ["XlsxWriter"]),
    ("leading_plain_comment__feat_selection",
     "# prod_model_modelling_data_scored_\n"
     "!pip install -qq mlflow\n"
     "dbutils.library.restartPython()",
     ["mlflow"]),
    ("leading_commented_pip_multi__helperlib_app",
     "# !pip install trino\n"
     "!pip install gspread\n"
     "!pip install gspread_dataframe",
     ["gspread", "gspread_dataframe"]),
    ("leading_import", "import pandas as pd\n!pip install XlsxWriter --quiet",
     ["XlsxWriter"]),
    ("leading_docstring", '"""prep"""\n!pip install mlflow', ["mlflow"]),

    # --- arg parsing ---
    ("multi_pkg_one_line__reporting_excel",
     "!pip install -qq mlflow XlsxWriter openpyxl",
     ["mlflow", "XlsxWriter", "openpyxl"]),
    ("version_pin_name_detected", "!pip install mlflow==2.21.0", ["mlflow"]),
    ("extras_stripped", "%pip install pkg[extra]", ["pkg"]),
    ("flags_dropped", "!pip install --upgrade -q tabulate", ["tabulate"]),
    ("bounded_pin_name_detected", "%pip install 'pydantic<2'", ["pydantic"]),

    # --- must NOT fire ---
    ("all_pip_commented", "# !pip install trino\n# !pip install gspread", []),
    ("already_neutralised",
     "# AIDP: installed via cluster libraries API — !pip install mlflow", []),
    ("no_pip_at_all", "import pandas as pd\ndf = pd.DataFrame()", []),
    ("pip_word_in_string", "msg = 'run !pip install foo yourself'", []),
    ("bare_pip_not_magic", "pip install xgboost", []),

    # --- argument forms that must NOT become package names ---
    ("trailing_shell_comment", "!pip install foo # needed for reporting", ["foo"]),
    ("option_with_value", "%pip install --index-url https://foo/simple x", ["x"]),
    ("quoted_exact_pin", '!pip install "mlflow==2.1"', ["mlflow"]),
    ("semicolon_cuts_the_list", "!pip install x; import x", ["x"]),
    ("pip_itself_skipped", "!pip install --upgrade pip", []),
    ("requirements_file_not_a_package", "%pip install -r /dbfs/requirements.txt", []),
    ("vcs_spec_not_a_package", "!pip install git+https://example/repo.git", []),
    ("wheel_path_not_a_package", "!pip install /dbfs/libs/pkg-1.0.whl", []),
    ("interpolation_rejected", "!pip install {pkg_name}", []),

    # --- other spellings AIDP rejects identically ---
    ("pip3", "!pip3 install foo", ["foo"]),
    ("python_m_pip", "!python -m pip install foo", ["foo"]),
    ("python3_m_pip", "!python3 -m pip install foo", ["foo"]),

    # --- attached option values must not be mistaken for packages ---
    ("attached_eq_requirement", "!pip install --requirement=requirements.txt", []),
    ("attached_short_requirement", "!pip install -rrequirements.txt", []),
    ("attached_short_editable", "!pip install -e./local_pkg", []),
    ("attached_short_constraint_plus_pkg", "!pip install -c/dbfs/c.txt pkg", ["pkg"]),
    ("unknown_option_swallows_its_value", "!pip install --log /tmp/pip.log foo", ["foo"]),
]


# (id, cell source, expected unsupported-substring) — forms whose real package
# list the line cannot express must be REPORTED, never guessed at.
_UNSUPPORTED_SHAPES = [
    ("requirements_file", "%pip install -r /dbfs/requirements.txt", "-r"),
    ("editable", "!pip install -e /dbfs/src/mypkg", "-e"),
    ("constraints", "!pip install -c /dbfs/constraints.txt", "-c"),
    ("vcs", "!pip install git+https://example/repo.git", "not installable by name"),
    ("direct_url", "!pip install https://example/pkg.tar.gz", "not installable by name"),
    ("wheel_path", "!pip install /dbfs/libs/pkg-1.0.whl", "not installable by name"),
    ("interpolated", "!pip install {pkg}", "interpolation"),
    ("continuation", "!pip install foo \\", "continuation"),
    # attached option values — pip accepts these and they previously vanished
    # silently: no package, no finding, artifact claiming success
    ("requirement_attached_eq", "!pip install --requirement=requirements.txt", "--requirement"),
    ("requirement_attached_short", "!pip install -rrequirements.txt", "-r"),
    ("editable_attached_short", "!pip install -e./local_pkg", "-e"),
    ("constraint_attached_short", "!pip install -c/dbfs/constraints.txt pkg", "-c"),
    ("unknown_option_value", "!pip install --log /tmp/x.log /tmp/y.log", "not a valid package name"),
]


@pytest.mark.parametrize("src,expect_sub", [(s, e) for _, s, e in _UNSUPPORTED_SHAPES],
                         ids=[i for i, _, _ in _UNSUPPORTED_SHAPES])
def test_unsupported_pip_forms_are_reported_not_guessed(src, expect_sub):
    _, _pkgs, unsupported = jm._find_pip_installs(src)
    assert unsupported, f"form silently accepted: {src!r} -> packages {_pkgs}"
    assert any(expect_sub in u for u in unsupported), unsupported


_DIRECT_REFERENCE_CASES = [
    pytest.param(pattern.format(name=name, url=url), id=f"{kind}-{extra}-{spacing}")
    for kind, url in (
        ("https", "https://example.com/custom.whl#sha256=abc"),
        ("git", "git+https://example.com/tools.git@v1"),
        ("file", "file:///tmp/custom.whl"),
    )
    for extra, name in (("plain", "custom_lib"), ("extras", "custom_lib[fast,io]"))
    for spacing, pattern in (
        ("separated", "{name} @ {url}"),
        ("attached", "{name}@{url}"),
        ("at_left", "{name}@ {url}"),
        ("at_right", "{name} @{url}"),
        ("single_quoted", "'{name} @ {url}'"),
        ("double_quoted", '"{name} @ {url}"'),
    )
]


@pytest.mark.parametrize("reference", _DIRECT_REFERENCE_CASES)
def test_direct_reference_is_one_unresolved_dependency(reference):
    packages, unsupported = jm._parse_pip_args(f"requests {reference} pandas")
    assert packages == ["requests", "pandas"]
    assert len(unsupported) == 1
    assert "custom_lib" in unsupported[0] and "@" in unsupported[0]
    assert "direct reference" in unsupported[0]


@pytest.mark.parametrize("args,expected", [
    ("custom_lib @", []),
    ("custom_lib@", []),
    ("custom_lib[fast] @ # missing location", []),
    ("custom_lib @ --quiet pandas", ["pandas"]),
    ("custom_lib @ ; echo ignored", []),
    ("custom_lib @ https://example.com/a.whl; echo ignored", []),
    ('"custom_lib @ https://example.com/a.whl; python_version < \'3.12\'" pandas',
     ["pandas"]),
    ("custom_lib [fast, io] @ file:///tmp/a.whl pandas", ["pandas"]),
    ("custom_lib @ relative/path pandas", ["pandas"]),
    ("custom_lib @ https://example.com/a.whl other_lib @ file:///tmp/b.whl pandas",
     ["pandas"]),
])
def test_direct_reference_boundaries(args, expected):
    packages, unsupported = jm._parse_pip_args(args)
    assert packages == expected
    assert unsupported
    assert all("direct reference" in finding for finding in unsupported)


@pytest.mark.parametrize("args", [
    "--index-url https://user@example.com/simple requests pandas",
    "--proxy http://user@example.com:8080 requests pandas",
    "requests pandas # custom_lib @ https://example.com/a.whl",
])
def test_at_in_option_values_or_comments_is_not_a_dependency(args):
    assert jm._parse_pip_args(args) == (["requests", "pandas"], [])


@pytest.mark.parametrize("reference", _DIRECT_REFERENCE_CASES)
@pytest.mark.parametrize("mixed", [False, True], ids=["pip_only", "with_python"])
def test_direct_reference_never_installs_bare_name_in_either_path(
        monkeypatch, tmp_path, reference, mixed):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    source = f"!pip install requests {reference} pandas\n"
    if mixed:
        source += "REFERENCE_SENTINEL = 42\n"
    result, installs, saved, saved_src, direct_exec = _drive(
        monkeypatch, tmp_path, [source])
    assert installs == ["requests", "pandas"]
    if mixed:
        assert result["status"] == "PARTIAL", result
        assert result.get("pip_unresolved", 0) >= 1
    else:
        assert result["status"] == "FAIL", result
        assert any("direct reference" in cell.get("note", "")
                   for cell in result["cell_results"])
    assert len(saved["cells"]) == 1
    assert reference in saved_src
    assert jm._PIP_NOTE_UNRESOLVED in saved_src
    assert not _executable_pip_lines(saved_src)
    assert not _executable_pip_lines(direct_exec)
    if mixed:
        assert "REFERENCE_SENTINEL = 42" in direct_exec

    child, child_installs = _drive_child(
        monkeypatch, source, install_result=True, strict=False)
    assert child_installs == ["requests", "pandas"]
    assert any("direct reference" in finding
               for finding in child.get("unresolved_pip", [])), child
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    cached_source = "\n".join("".join(cell["source"]) for cell in cached["cells"])
    assert reference in cached_source
    assert not _executable_pip_lines(cached_source)


def test_code_only_direct_reference_remains_unresolved(monkeypatch, tmp_path):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    source = "!pip install custom_lib @ file:///tmp/custom.whl\n"
    notebook = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                            "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/dep.ipynb", kernel, "test_pip", "dep_x", {}, run_all=False))
    assert installs == []
    assert result.get("pip_unresolved", 0) >= 1
    saved_src = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    assert "custom_lib @ file:///tmp/custom.whl" in saved_src
    assert jm._PIP_NOTE_UNRESOLVED in saved_src
    assert not _executable_pip_lines(saved_src)


def test_real_child_direct_reference_reaches_parent_verdict(monkeypatch, tmp_path):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    parent = {"cells": [{"cell_type": "code", "source": ["%run ./dep\n"],
                        "metadata": {}, "outputs": [], "execution_count": None}],
              "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    child = {"cells": [{"cell_type": "code", "metadata": {},
                       "source": ["!pip install custom_lib @ https://example.com/custom.whl\n"],
                       "outputs": [], "execution_count": None}],
             "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, parent, installs)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    child_path = "/Workspace/test_pip/notebooks/dep.ipynb"
    monkeypatch.setitem(jm._notebook_content_cache, child_path, json.dumps(child))
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {},
        dep_path_map={"/Workspace/test_pip/dep.ipynb": child_path}))
    assert installs == []
    assert result["status"] in ("FAIL", "PARTIAL"), result
    assert any("direct reference" in cell.get("note", "")
               for cell in result["cell_results"]), result


_REVIEW_OPTIONS = [
    ("--root-user-action", "ignore"), ("--use-deprecated", "legacy-resolver"),
    ("--use-feature", "fast-deps"), ("--keyring-provider", "auto"),
    ("--resume-retries", "5"),
]
_REVIEW_REDIRECTS = [
    ">/tmp/pip.log", "> /tmp/pip.log", "2>/dev/null", "2> /dev/null",
    "> /dev/null 2>&1", "2>&1", ">>/tmp/pip.log", "2>> /tmp/pip.log",
    "&>/tmp/pip.log", "&>> /tmp/pip.log", ">|/tmp/pip.log", "2>&-",
    '>"/tmp/pip log"', r">/tmp/pip\ log", ">/tmp/a> /tmp/b",
]


@pytest.mark.parametrize("args,has_notice", [
    (f"{flag}{separator}{value} requests", False)
    for flag, value in _REVIEW_OPTIONS for separator in (" ", "=")
] + [(f"requests {redirect}", True) for redirect in _REVIEW_REDIRECTS])
@pytest.mark.parametrize("mixed", [False, True])
def test_reviewed_pip_arguments_in_parent_and_child(
        monkeypatch, tmp_path, capsys, args, has_notice, mixed):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    source = "!pip install " + args + "\n"
    if mixed:
        source += "REVIEW_SENTINEL = 42\n"
    result, installs, _, saved_source, executed = _drive(monkeypatch, tmp_path, [source])
    assert installs == ["requests"]
    assert result["status"] == "PASS", result
    assert not _executable_pip_lines(saved_source)
    assert args in saved_source
    if mixed:
        assert "REVIEW_SENTINEL = 42" in executed
    child, child_installs = _drive_child(monkeypatch, source, install_result=True, strict=False)
    assert child_installs == ["requests"]
    assert child["status"] == "ok"
    assert not child.get("unresolved_pip")
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    child_source = "".join(cached["cells"][0]["source"])
    assert not _executable_pip_lines(child_source)
    if has_notice:
        assert "redirection" in saved_source and "not reproduced" in saved_source
        assert "not reproduced" in child_source
        assert "not reproduced" in capsys.readouterr().out


@pytest.mark.parametrize("flag", ["--index-url", "--log", "-r", "--root-user-action"])
@pytest.mark.parametrize("separator", [";", "&&", "||", "|", "&"])
def test_missing_option_value_never_installs_next_command(
        monkeypatch, tmp_path, flag, separator):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    source = f"!pip install {flag}{separator} import x\n"
    result, installs, _, saved_source, _ = _drive(monkeypatch, tmp_path, [source])
    assert installs == []
    assert result["status"] == "FAIL"
    assert any("missing option value" in cell.get("note", "")
               for cell in result["cell_results"])
    assert not _executable_pip_lines(saved_source)
    child, child_installs = _drive_child(monkeypatch, source, install_result=True, strict=False)
    assert child_installs == []
    assert any("missing option value" in item for item in child["unresolved_pip"])


@pytest.mark.parametrize("mode", ["execute", "code_only", "below_exit"])
def test_redirection_notice_survives_into_artifact_and_report(monkeypatch, tmp_path, mode):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    sources = ["!pip install requests > /tmp/pip.log 2>&1\n"]
    if mode == "below_exit":
        sources.insert(0, "if FLAG:\n    aidputils.notebook.exit('done')\n")
    notebook = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                            "outputs": [], "execution_count": None} for source in sources],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel(fire_exit=mode == "below_exit")
    asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "task", {},
        run_all=mode != "code_only"))
    assert installs == (["requests"] if mode == "execute" else [])
    saved_source = "\n".join("".join(cell["source"]) for cell in _saved_notebook(kernel)["cells"])
    assert "not reproduced" in saved_source
    report = "\n".join(content for path, content in kernel.files.items()
                       if path.endswith("migration_report.md"))
    assert "redirection" in report and "not reproduced" in report


def test_pip_parser_agrees_with_the_exporter():
    """The migrator and `dbx_export` must read a pip line the same way.

    The exporter's harvest is what puts these packages on the cluster in the
    first place, so a disagreement means the migrator comments out a line
    trusting a library the exporter never collected — or installs a token the
    exporter correctly ignored.
    """
    from dbx_export.__main__ import _parse_pip_install_line
    from packaging.requirements import Requirement
    for line, args in [
        ("!pip install foo # needed", "foo # needed"),
        ('!pip install "mlflow==2.1"', '"mlflow==2.1"'),
        ("%pip install 'pydantic<2'", "'pydantic<2'"),
        ("%pip install --index-url https://foo/simple x", "--index-url https://foo/simple x"),
    ]:
        mine, _ = jm._parse_pip_args(args)
        # name-only comparison: the migrator installs by name, the exporter
        # writes requirement lines, so pins legitimately differ in form.
        exported = _parse_pip_install_line(args)
        assert mine == [Requirement(spec).name for spec in exported], line


@pytest.mark.parametrize("src,expected", [(s, e) for _, s, e in _SHAPES],
                         ids=[i for i, _, _ in _SHAPES])
def test_find_pip_installs_shape_grid(src, expected):
    _, pkgs, _unsup = jm._find_pip_installs(src)
    assert pkgs == expected


def test_leading_comment_shapes_would_have_been_missed_by_an_anchored_match():
    """Pin the exact regression: `re.match` on the stripped cell finds nothing
    for the shapes that actually failed in production, while the line scan does.

    This is the discriminator, stated as an executable claim so the anchor
    cannot quietly come back.
    """
    anchored = re.compile(r'^\s*(?:%pip|!pip)\s+install\s+(.+)')
    regressions = [src for name, src, exp in _SHAPES
                   if exp and name.startswith(("leading_commented", "leading_plain",
                                               "leading_import", "leading_docstring"))]
    assert regressions, "no regression shapes in the grid"
    for src in regressions:
        assert anchored.match(src.strip()) is None, f"shape no longer anchored-blind: {src!r}"
        _, pkgs, _unsup = jm._find_pip_installs(src)
        assert pkgs, f"line scan failed to find the install in {src!r}"


def test_commented_pip_lines_do_not_trigger_a_spurious_install():
    """A cell whose pip references are ALL commented out — e.g. a notebook
    already hand-fixed by an operator — must be left completely alone."""
    hand_fixed = (
        "# !pip install mlflow  --quiet\n"
        "# Oracle human fix (2026-08-30): `!pip` is rejected by the AIDP kernel\n"
        "# !pip install XlsxWriter  --quiet\n"
    )
    idxs, pkgs, _unsup = jm._find_pip_installs(hand_fixed)
    assert (idxs, pkgs) == (set(), [])


# ─────────────────────────────────────────────────────────────────────────
# Part B — neutralisation: no executable pip may survive
# ─────────────────────────────────────────────────────────────────────────

def test_comment_pip_lines_neutralises_every_line_not_just_the_first():
    """Regression for the single-`#` bug.

    The pure-pip branch used to emit `f"# AIDP: ...\\n# {stripped_source}\\n"`,
    which prefixes ONE `#` to a MULTI-LINE cell — so only the first line was
    commented and every later `!pip` line stayed LIVE in the saved notebook.
    Example_Lookalike_Prod `Helper_App` cell 2 carries three pip lines, so this
    shipped an executable install into the delivered artifact.
    """
    src = ("# !pip install trino\n"
           "!pip install gspread\n"
           "!pip install gspread_dataframe")
    idxs, pkgs, _unsup = jm._find_pip_installs(src)
    out = jm._comment_pip_lines(src, idxs)
    assert out is not None
    assert _executable_pip_lines(out) == [], out
    # The old buggy form, for contrast: only line 1 gets commented.
    old = f"# AIDP: installed via cluster libraries API\n# {src}\n"
    assert _executable_pip_lines(old), "the old form was already safe — revisit this test"


def test_comment_pip_lines_preserves_non_pip_code():
    src = ("# prod_model_modelling_data_scored_\n"
           "!pip install -qq mlflow\n"
           "import mlflow\n"
           "TRACKING = 'x'")
    idxs, _, _unsup = jm._find_pip_installs(src)
    out = jm._comment_pip_lines(src, idxs)
    assert "import mlflow" in out
    assert "TRACKING = 'x'" in out
    assert "# prod_model_modelling_data_scored_" in out
    assert _executable_pip_lines(out) == []
    compile(out, "<t>", "exec")


def test_comment_pip_lines_stubs_an_emptied_block_instead_of_giving_up():
    """A pip line that is the SOLE body of an indented block leaves an empty
    block once commented, which cannot compile.

    Earlier this returned None, which pushed callers into destructive
    fallbacks (comment out the WHOLE cell / keep the live pip line and report
    success). It now inserts an indented `pass` so the block stays valid and
    every surrounding statement survives.
    """
    src = "if NEEDS_DEPS:\n    !pip install mlflow"
    idxs, pkgs, _unsup = jm._find_pip_installs(src)
    assert pkgs == ["mlflow"]
    out = jm._comment_pip_lines(src, idxs)
    assert out is not None
    assert _executable_pip_lines(out) == []
    assert "pass" in out
    compile(out, "<t>", "exec")


def test_emptied_block_stub_preserves_unrelated_trailing_code():
    """The reproducer from review: `if NEEDS:` / indented pip / `SENTINEL = 42`.

    The old task-path fallback commented out the entire cell, so SENTINEL was
    silently disabled while the task reported PASS.
    """
    src = "if NEEDS:\n    !pip install mlflow\nSENTINEL = 42"
    idxs, _, _unsup = jm._find_pip_installs(src)
    out = jm._comment_pip_lines(src, idxs)
    assert out is not None, "no safe rewrite found"
    assert "SENTINEL = 42" in out
    # and it must still be EXECUTABLE, not commented out
    assert any(l.strip() == "SENTINEL = 42" for l in out.split("\n")), out
    assert _executable_pip_lines(out) == []
    compile(out, "<t>", "exec")


# ── String literals are data, not commands ─────────────────────────────

def test_pip_line_inside_a_triple_quoted_string_is_not_a_command():
    """`HELP = \"\"\"\\n!pip install example_pkg\\n\"\"\"` must be left ALONE.

    Detection is per-line, so a triple-quoted string can carry a line that
    looks exactly like an install command. Treating it as one both installed
    the package and REWROTE the string's value — silently changing what the
    program prints.
    """
    src = 'HELP = """\n!pip install example_pkg\n"""\nprint(HELP)'
    idxs, pkgs, _unsup = jm._find_pip_installs(src)
    assert pkgs == [], f"string contents detected as a command: {pkgs}"
    assert idxs == set()
    # nothing to rewrite → the source must be byte-identical
    assert jm._comment_pip_lines(src, idxs) == src


def test_real_pip_still_found_when_a_string_also_mentions_pip():
    """The string-literal exclusion must not blind the detector to real
    installs elsewhere in the same cell."""
    src = ('DOC = """\n!pip install decoy\n"""\n'
           '!pip install real_pkg\n'
           'X = 1')
    idxs, pkgs, _unsup = jm._find_pip_installs(src)
    assert pkgs == ["real_pkg"], pkgs
    out = jm._comment_pip_lines(src, idxs)
    # The REAL command is neutralised...
    assert not any(l.strip().startswith("!pip install real_pkg")
                   for l in out.split("\n")), out
    assert f"{jm._PIP_NEUTRALISED_PREFIX} — !pip install real_pkg" in out
    # ...while the string's contents are byte-identical. Note the blanket
    # `_executable_pip_lines` guard deliberately stays string-unaware (any
    # pip-looking line in a saved artifact is worth flagging), so it is not
    # the right assertion here.
    assert '\n!pip install decoy\n' in out
    assert "X = 1" in out
    compile(out, "<t>", "exec")


@pytest.mark.parametrize("src,expected", [
    # single-quoted strings cannot span a newline → the pip line is real code
    ("X = 'a'\n!pip install real", ["real"]),
    # triple-single quotes
    ("T = '''\n!pip install decoy\n'''", []),
    # string closed before the pip line → real command
    ('T = """x"""\n!pip install real', ["real"]),
    # pip inside a docstring at the top of the cell
    ('"""\n!pip install decoy\n"""\nY = 2', []),
    # escaped quote inside the string must not close it early
    ('T = """\\"\n!pip install decoy\n"""', []),
    # a `#` comment containing a triple quote must not open a string region
    ('# """\n!pip install real', ["real"]),
])
def test_string_literal_boundary_grid(src, expected):
    assert jm._find_pip_installs(src)[1] == expected


def test_comment_pip_lines_keeps_indented_block_with_a_sibling_statement():
    src = "if NEEDS_DEPS:\n    !pip install mlflow\n    import mlflow"
    idxs, _, _unsup = jm._find_pip_installs(src)
    out = jm._comment_pip_lines(src, idxs)
    assert out is not None
    assert _executable_pip_lines(out) == []
    compile(out, "<t>", "exec")


# ─────────────────────────────────────────────────────────────────────────
# Part C — orchestration: the real process_notebook cell loop
# ─────────────────────────────────────────────────────────────────────────

class _Kernel:
    cluster_id = "test-cluster"

    def __init__(self, fire_exit=False):
        self.executed = []
        self.files = {}
        self.child_count = 1
        # When True, a cell calling notebook.exit() reports the AIDP exit
        # marker, so the conditional-exit path can be driven end to end.
        self.fire_exit = fire_exit

    async def execute(self, code, **kwargs):
        self.executed.append(code)
        if "all_cells = payload.get" in code:
            encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
            count = len(json.loads(base64.b64decode(encoded.group(1)))["cells"])
            return {"status": "ok", "outputs": [{
                "type": "stream", "name": "stdout",
                "text": f"Replaced {count}/{count} cells (config_offset=0)\n",
            }]}
        if "__AIDP_CHILD_COUNT__" in code:
            count = self.child_count
            path_match = re.search(r"with open\(([^\n]+?)\) as f:", code)
            if path_match:
                try:
                    path = json.loads(path_match.group(1))
                    content = jm._notebook_content_cache.get(path)
                    if content is None and Path(path).is_file():
                        content = Path(path).read_text(encoding="utf-8")
                    if content is not None:
                        notebook = json.loads(content)
                        count = sum(
                            1
                            for cell in notebook.get("cells", [])
                            if cell.get("cell_type") == "code"
                            and "".join(cell.get("source", [])).strip()
                            and "# AIDP performance configuration"
                            not in "".join(cell.get("source", []))
                        )
                except (OSError, TypeError, ValueError, json.JSONDecodeError):
                    pass
            return {"status": "ok", "outputs": [{
                "type": "stream", "name": "stdout",
                "text": f"__AIDP_CHILD_COUNT__{count}\n",
            }]}
        match = re.search(r"with builtins.open\('([^']+)', 'w[ba]'\) as f:", code)
        encoded = re.search(r"base64.b64decode\('([^']+)'\)", code)
        if "payload = json.loads(base64.b64decode(" in code and encoded:
            payload = json.loads(base64.b64decode(encoded.group(1)).decode())
            replaced = len(payload.get("cells", {}))
            return {
                "status": "ok",
                "outputs": [{
                    "type": "stream",
                    "name": "stdout",
                    "text": f"Replaced {replaced}/{replaced} cells (config_offset=0)",
                }],
            }
        # Job-level report files are written whole (temporary sibling, then
        # os.replace) and confirmed by a printed marker; record the final path.
        report = re.search(r"^_path = '([^']+)'$", code, re.M)
        if report and encoded and "__AIDP_REPORT_WRITTEN__" in code:
            self.files[report.group(1)] = base64.b64decode(encoded.group(1)).decode()
            return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout",
                                                 "text": "__AIDP_REPORT_WRITTEN__\n"}]}
        if match and encoded:
            self.files[match.group(1)] = base64.b64decode(encoded.group(1)).decode()
            return {"status": "ok", "outputs": []}
        if self.fire_exit and "notebook.exit(" in code and not code.lstrip().startswith("#"):
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout",
                 "text": "__AIDP_NOTEBOOK_EXIT__ done\n"}]}
        if "__AIDP_ISO_OK__" in code:
            # Per-child param isolation is `_isolation_guarded`: the real kernel prints
            # the OK marker on a clean reset+seed. Model that here so the notebook.run
            # inline happy path proceeds (the caller fails-closed when the marker is
            # absent). Failure paths are covered by direct exec-of-snippet unit tests
            # and by tests that monkeypatch this to withhold the marker.
            return {"status": "ok", "outputs": [
                {"type": "stream", "name": "stdout", "text": "__AIDP_ISO_OK__\n"}]}
        return {"status": "ok", "outputs": []}

    async def run_stateless(self, code, **kwargs):
        return {"status": "ok", "outputs": []}


def _patch_common(monkeypatch, tmp_path, notebook, installs):
    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)
    async def analyze(cells, *a, **k):
        return [{"cell_index": i, "action": "execute", "description": "x",
                 "risks": [], "changes_needed": [], "storage_paths": [],
                 "table_refs": [], "fuse_risks": []} for i in range(len(cells))]

    async def noop(*a, **k):
        return None

    async def verified(*a, **k):
        return True

    async def fake_install(cluster_id, session, pkg, out_path, **kwargs):
        installs.append(pkg)
        return True

    def _no_llm(*a, **k):
        raise AssertionError(
            "Opus must not be invoked for a pip cell — the handler is deterministic")

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: False)
    monkeypatch.setattr(jm, "call_opus_with_tools", _no_llm)
    monkeypatch.setattr(jm, "download_notebook",
                        lambda *a, **k: json.dumps(notebook).encode())
    monkeypatch.setattr(cell_analyzer, "analyze_notebook_cells", analyze)
    monkeypatch.setattr(jm, "load_catalog_snapshot", lambda: "")
    monkeypatch.setattr(jm, "get_bucket_mapping_context", lambda: "")
    monkeypatch.setattr(jm, "_inject_write_guard", lambda code: code)
    monkeypatch.setattr(jm, "registry_update", noop)
    monkeypatch.setattr(jm, "verify_upload", verified)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", False)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_pip")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_pip/notebooks")
    monkeypatch.setattr(jm.tempfile, "mkdtemp", lambda **k: str(tmp_path))
    monkeypatch.setattr(jm.shutil, "rmtree", lambda *a, **k: None)
    monkeypatch.setattr(jm.asyncio, "sleep", noop)


def _saved_notebook(kernel):
    finals = [json.loads(t) for p, t in kernel.files.items() if p.endswith(".ipynb")]
    assert finals, "no notebook artifact saved"
    return finals[0]


def _drive(monkeypatch, tmp_path, sources):
    notebook = {
        "cells": [{"cell_type": "code", "source": [s], "metadata": {},
                   "outputs": [], "execution_count": None} for s in sources],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))
    saved = _saved_notebook(kernel)
    saved_src = "\n".join("".join(c["source"]) for c in saved["cells"])
    direct_exec = "\n".join(e for e in kernel.executed
                            if "base64.b64decode" not in e)
    return result, installs, saved, saved_src, direct_exec


@pytest.mark.parametrize("name,source,expected_pkgs", [
    ("merge_rate_check_cell_1",
     "# !pip install mlflow  --quiet\n"
     "# dbutils.library.restartPython()\n"
     "!pip install XlsxWriter  --quiet\n",
     ["XlsxWriter"]),
    ("feat_selection_cell_2",
     "# prod_model_modelling_data_scored_\n"
     "!pip install -qq mlflow\n"
     "dbutils.library.restartPython()\n",
     ["mlflow"]),
    ("helperlib_app_cell_2",
     "# !pip install trino\n"
     "!pip install gspread\n"
     "!pip install gspread_dataframe\n",
     ["gspread", "gspread_dataframe"]),
    ("reporting_excel_cell_1",
     "!pip install -qq mlflow XlsxWriter openpyxl\n"
     "dbutils.library.restartPython()\n",
     ["mlflow", "XlsxWriter", "openpyxl"]),
])
def test_real_job_pip_cells_install_and_never_execute(
        monkeypatch, tmp_path, name, source, expected_pkgs):
    """The four real failing/at-risk cells from Example_Feats_Retrain_Pipeline and
    Example_Lookalike_Prod, driven through the real cell loop."""
    result, installs, saved, saved_src, direct_exec = _drive(
        monkeypatch, tmp_path, [source])

    # 1. every package reached the cluster-libraries API
    assert installs == expected_pkgs, f"{name}: installs={installs}"
    # 2. no EXECUTABLE pip line was ever sent to the kernel (a commented
    #    `# AIDP: ... — !pip install x` line inside a kept cell body is inert)
    assert _executable_pip_lines(direct_exec) == [], f"{name}: pip executed on kernel"
    # 3. nothing executable survives in the saved notebook
    assert _executable_pip_lines(saved_src) == [], f"{name}: live pip in saved notebook"
    # 4. the original intent is preserved, not deleted
    assert "pip install" in saved_src, f"{name}: original pip line was deleted"
    # 5. no cell loss
    assert len(saved["cells"]) == 1
    assert result["status"] in ("PASS", "PASS_CONDITIONAL_EXIT_COVERAGE"), result


def test_pip_cell_preserves_co_located_python(monkeypatch, tmp_path):
    """A mixed cell must still land its imports/variables in the kernel — the
    behaviour a381b5f added, which the anchored gate made unreachable whenever
    a comment sat above the pip line."""
    source = ("# leading comment that used to hide this cell\n"
              "!pip install dataframe_image\n"
              "SENTINEL_CO_LOCATED = 1\n")
    result, installs, saved, saved_src, direct_exec = _drive(
        monkeypatch, tmp_path, [source])

    assert installs == ["dataframe_image"]
    assert "SENTINEL_CO_LOCATED = 1" in direct_exec, "co-located Python was dropped"
    # The cell body IS executed (that is the point), but the pip line inside it
    # must be commented — an inert `# AIDP: ... — !pip install x` line is fine,
    # an executable one is the defect.
    assert _executable_pip_lines(direct_exec) == [], direct_exec
    assert _executable_pip_lines(saved_src) == []
    assert "SENTINEL_CO_LOCATED = 1" in saved_src


def test_code_only_dep_migration_still_neutralises_pip(monkeypatch, tmp_path):
    """Dependency notebooks are migrated with run_all=False (code-only).

    Gating the rewrite on `run_all` would save a dependency notebook with its
    `!pip` line still EXECUTABLE while the task reports PASS; that notebook then
    fails at job-run time with `Invalid pip command format!`.

    Commenting the line is a migration decision — the package is supplied as a
    cluster library — so it must happen without a live session. Only the
    install itself requires run_all.
    """
    source = ("# !pip install another_obsolete_thing\n"
              "!pip install inflection\n"
              "CHILD_SENTINEL = \"child-kept\"\n")
    notebook = {
        "cells": [{"cell_type": "code", "source": [source], "metadata": {},
                   "outputs": [], "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/dep.ipynb", kernel, "test_pip", "dep_x", {},
        run_all=False))

    saved = _saved_notebook(kernel)
    saved_src = "\n".join("".join(c["source"]) for c in saved["cells"])
    # THE contract: no executable pip survives into a code-only artifact.
    assert _executable_pip_lines(saved_src) == [], saved_src
    assert "pip install inflection" in saved_src, "original intent was deleted"
    assert "CHILD_SENTINEL" in saved_src, "co-located Python lost"
    # No live session in code-only mode → no install may be attempted.
    assert installs == [], f"code-only mode must not install: {installs}"
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result


def test_all_commented_pip_cell_triggers_no_install(monkeypatch, tmp_path):
    """An operator-hand-fixed cell (every pip line already commented) must not
    trigger a cluster-library install."""
    source = ("# !pip install mlflow  --quiet\n"
              "# Oracle human fix (2026-08-30): disabled, already a cluster library\n"
              "# !pip install XlsxWriter  --quiet\n")
    _, installs, _, saved_src, _ = _drive(monkeypatch, tmp_path, [source])
    assert installs == [], f"spurious install: {installs}"
    assert _executable_pip_lines(saved_src) == []


# ── Orchestration-level failure paths ──────────────────────────────────

def _drive_strict(monkeypatch, tmp_path, sources, install_result, run_all=True):
    """Drive the real cell loop with STRICT_CELL_GATING on and a controllable
    install outcome."""
    notebook = {
        "cells": [{"cell_type": "code", "source": [s], "metadata": {},
                   "outputs": [], "execution_count": None} for s in sources],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)

    async def fake_install(cluster_id, session, pkg, out_path, **kwargs):
        installs.append(pkg)
        return install_result

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", True)
    kernel = _Kernel()
    return kernel, installs, jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {},
        run_all=run_all)


def test_failed_install_fails_the_task_under_strict_gating(monkeypatch, tmp_path):
    """`install_missing_package` returns False on install OR import-verification
    failure — it does NOT raise. The return value was ignored, so a package
    that never landed must not be reported as `ok`.
    """
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path,
        ["# leading comment\n!pip install nonexistent_pkg\nX = 1\n"],
        install_result=False)
    with pytest.raises(RuntimeError, match="were not installed"):
        asyncio.run(coro)
    assert installs == ["nonexistent_pkg"]


def test_failed_install_is_not_recorded_as_installed(monkeypatch, tmp_path):
    """A failed package must NOT enter the auto-installed set, or the Fix 7f
    ModuleNotFoundError retry is suppressed for the rest of the job. Two cells
    asking for the same package must therefore both attempt it."""
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path,
        ["# c\n!pip install flaky_pkg\nA = 1\n", "# c\n!pip install flaky_pkg\nB = 2\n"],
        install_result=False)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    asyncio.run(coro)
    assert installs == ["flaky_pkg", "flaky_pkg"], (
        f"a failed install was cached as done: {installs}")


def test_unresolvable_pip_cell_is_reported_failed_not_ok(monkeypatch, tmp_path):
    """When no safe rewrite exists the cell must be reported as a failure with
    the customer's code preserved — never commented out wholesale and counted
    `ok`, which silently disabled unrelated Python."""
    # `try:`-only body: commenting the pip line empties the block, and a `pass`
    # stub still leaves `try` without except/finally → genuinely unresolvable.
    source = "try:\n    !pip install mlflow\nSENTINEL_TRAILING = 42\n"
    idxs, pkgs, _unsup = jm._find_pip_installs(source)
    assert pkgs == ["mlflow"]
    if jm._comment_pip_lines(source, idxs) is not None:
        pytest.skip("shape is now resolvable; unresolvable branch needs a new fixture")

    result, installs, saved, saved_src, _ = _drive(monkeypatch, tmp_path, [source])
    assert result["status"] in ("PARTIAL", "FAIL"), result
    cr = result.get("cell_results") or []
    assert any(c.get("status") == "fail" for c in cr), cr
    # customer code preserved verbatim (safety contract 2), not blanket-commented
    assert "SENTINEL_TRAILING = 42" in saved_src
    assert any(l.strip() == "SENTINEL_TRAILING = 42" for l in saved_src.split("\n")), (
        "unrelated Python was commented out")


def _drive_child(monkeypatch, child_source, install_result, strict, *, kernel=None):
    """Drive the real `_inline_child_notebook` against an in-memory child."""
    path = "/Workspace/test_pip/notebooks/child.ipynb"
    sources = child_source if isinstance(child_source, list) else [child_source]
    nb = {"cells": [{"cell_type": "code", "source": [source], "metadata": {},
                     "outputs": [], "execution_count": None} for source in sources],
          "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []

    async def fake_install(cluster_id, session, pkg, out_path, **kwargs):
        installs.append(pkg)
        return install_result

    async def noop(*a, **k):
        return None

    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)
    monkeypatch.setitem(jm._notebook_content_cache, path, json.dumps(nb))
    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)   # no LLM in the child path
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_pip")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_pip/notebooks")
    monkeypatch.setattr(jm.asyncio, "sleep", noop)
    kernel = kernel if kernel is not None else _Kernel()
    kernel.child_count = len(sources)
    log = jm.NotebookLog(path, "test_pip", "child")
    result = asyncio.run(jm._inline_child_notebook(
        path, kernel, log, "test_pip", "", "", "", {}))
    return result, installs


_HASH_GROUP_COMMANDS = [
    ("real_pkg==1.0 --hash sha256:abc", [], "unhashed installation not attempted"),
    ("real_pkg==1.0 --hash=sha256:abc", [], "unhashed installation not attempted"),
    ("--require-hashes real_pkg==1.0 second_pkg==2.0", [], "--require-hashes"),
    ("real_pkg==1.0 second_pkg==2.0 --require-hashes", [], "--require-hashes"),
    ("--group dev", [], "dependency group not resolved"),
    ("real_pkg --group=dev", ["real_pkg"], "dependency group not resolved"),
]


@pytest.mark.parametrize("args,expected_installs,reason", _HASH_GROUP_COMMANDS)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_child_hash_group_findings_do_not_claim_install_failure(
        monkeypatch, args, expected_installs, reason, strict, mixed):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    source = f"%pip install {args}" + ("\nSENTINEL = 42" if mixed else "")
    result, installs = _drive_child(
        monkeypatch, source, True, strict)
    assert installs == expected_installs
    assert not result.get("failed_installs"), result
    assert any(reason in f for f in result["unresolved_pip"])
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    saved = "".join(cached["cells"][0]["source"])
    assert jm._PIP_NOTE_UNRESOLVED in saved
    assert jm._PIP_NOTE_FAILED not in saved
    assert not _executable_pip_lines(saved)


@pytest.mark.parametrize("args,expected_installs,reason", _HASH_GROUP_COMMANDS)
@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_parent_hash_group_findings_do_not_claim_install_failure(
        monkeypatch, tmp_path, args, expected_installs, reason, run_all, strict, mixed):
    source = f"!pip install {args}" + ("\nSENTINEL = 42" if mixed else "")
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source], True,
        run_all=run_all)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    result = asyncio.run(coro)
    assert result["status"] in ("PARTIAL", "FAIL", "CONVERTED_NOT_VALIDATED"), result
    assert installs == (expected_installs if run_all else [])
    assert any(reason in c.get("note", "") for c in result["cell_results"])
    if strict and run_all:
        assert result["status"] == "FAIL"
        assert not any(path.endswith('.ipynb') for path in kernel.files)
        if mixed:
            assert any(jm._PIP_NOTE_UNRESOLVED in code for code in kernel.executed)
        return
    saved = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    assert jm._PIP_NOTE_UNRESOLVED in saved
    assert jm._PIP_NOTE_FAILED not in saved
    assert not _executable_pip_lines(saved)
    if not run_all:
        assert not any("SENTINEL = 42" in s for s in kernel.executed
                       if 'base64.b64decode' not in s)


def test_real_install_failure_with_group_still_uses_failed_annotation(monkeypatch, tmp_path):
    source = "!pip install real_pkg --group dev\nSENTINEL = 42"
    kernel, installs, coro = _drive_strict(monkeypatch, tmp_path, [source], False)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    result = asyncio.run(coro)
    assert result['status'] == 'PARTIAL'
    assert installs == ['real_pkg']
    assert jm._PIP_NOTE_FAILED in ''.join(_saved_notebook(kernel)['cells'][0]['source'])
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    child, child_installs = _drive_child(monkeypatch, source, False, False)
    assert child_installs == ['real_pkg']
    assert child['failed_installs'] == ['real_pkg']
    assert child['unresolved_pip']


_UNREWRITABLE_PIP_CELLS = [
    "!pip install real_pkg\n!ls /tmp\nSENTINEL = 42\n",
    "!pip install real_pkg\n%time x = 1\n",
    "try:\n    !pip install real_pkg\nSENTINEL = 42\n",
    "!pip install real_pkg\n!pip install second_pkg\nif:\n",
    "!pip install real_pkg\n%restart_python unexpected_argument\n",
    "!pip install real_pkg\nX = 1\n%sql SELECT 42\n",
    "!pip install real_pkg\n%sql SELECT 42\n!ls /tmp\n",
    "%python\n%pip install real_pkg\nif:\n",
    "%pyspark\n!pip install real_pkg\ntry:\n    x = 1\n",
]


_SUPPORTED_MIXED_PIP = [
    "%pip install real_pkg\n%restart_python",
    "%pip install real_pkg\n%restart_python\nSENTINEL = 42",
    "!pip install real_pkg\n%sql SELECT 42 AS SENTINEL",
    "!pip install real_pkg\n%scala\nval SENTINEL = 42",
    "# original comment\n!pip install real_pkg\n%sql\nSELECT 42 AS SENTINEL",
    "if True:\n    !pip install real_pkg\n    %restart_python\nSENTINEL = 42",
]


def test_restart_examples_in_strings_are_unchanged():
    source = 'HELP = """\n%restart_python\n%sql SELECT 1\n"""\n!pip install real_pkg'
    rewritten = jm._comment_pip_lines(source, jm._find_pip_installs(source)[0])
    assert 'HELP = """\n%restart_python\n%sql SELECT 1\n"""' in rewritten
    compile(rewritten, '<fixture>', 'exec')


@pytest.mark.parametrize("source", [
    "%sql\nSELECT '\n!pip install documentation_only\n' AS example",
    "%scala\n/*\n!pip install documentation_only\n*/\nval example = 42",
])
def test_original_native_cells_do_not_enter_the_new_pip_gate(monkeypatch, tmp_path, source):
    result, installs, _, saved, _ = _drive(monkeypatch, tmp_path, [source])
    assert result["status"] == "PASS", result
    assert installs == []
    assert saved == source


@pytest.mark.parametrize("magic", ["%sql", "%scala"])
@pytest.mark.parametrize("error_kind", ["status", "structured", "stream"])
@pytest.mark.parametrize("child", [False, True])
def test_mixed_pip_native_execution_errors_are_not_success(
        monkeypatch, tmp_path, magic, error_kind, child):
    original_execute = _Kernel.execute

    async def execute(self, code, **kwargs):
        if code.startswith(magic + '\n'):
            self.executed.append(code)
            return {"status": "error" if error_kind == "status" else "ok", "outputs": [
                {"type": "stream", "text": "Command ID abcd failed with syntax error. Failing command: fixture"}
                if error_kind == "stream" else {"type": "error", "ename": "SyntaxError", "evalue": "fixture"}]}
        return await original_execute(self, code, **kwargs)

    async def keep_code(code, *args, **kwargs):
        return code

    monkeypatch.setattr(_Kernel, "execute", execute)
    monkeypatch.setattr(jm, "call_fix", keep_code)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 1)
    source = '!pip install real_pkg\n' + magic + '\nINVALID_NATIVE_BODY'
    if child:
        monkeypatch.setattr(jm, "_notebook_content_cache", {})
        result, installs = _drive_child(monkeypatch, source, True, False)
        assert result["cells_ok"] == 0, result
        assert result["unresolved_pip"], result
        assert "remaining code failed execution" in result["unresolved_pip"][0]
    else:
        result, installs, _, saved, _ = _drive(monkeypatch, tmp_path, [source])
        assert result["status"] in ("PARTIAL", "FAIL"), result
        assert not _executable_pip_lines(saved)
    assert installs == ["real_pkg"]


@pytest.mark.parametrize("source", _SUPPORTED_MIXED_PIP)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("run_all", [False, True])
@pytest.mark.parametrize("install_ok", [False, True])
def test_parent_mixed_pip_dispatch_and_install_outcome(
        monkeypatch, tmp_path, source, strict, run_all, install_ok):
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source], install_ok, run_all=run_all)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    if strict and run_all and not install_ok:
        with pytest.raises(RuntimeError, match="were not installed"):
            asyncio.run(coro)
        assert installs == ["real_pkg"]
        return
    result = asyncio.run(coro)
    assert installs == (["real_pkg"] if run_all else [])
    if not run_all:
        assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    elif install_ok:
        assert result["status"] == "PASS", result
    else:
        assert result["status"] in ("PARTIAL", "FAIL"), result
    saved = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    assert not _executable_pip_lines(saved)
    expected_note = (jm._PIP_NOTE_CODE_ONLY if not run_all else
                     jm._PIP_NOTE_INSTALLED if install_ok else jm._PIP_NOTE_FAILED)
    assert expected_note.lstrip('# ') in saved
    executed = [s for s in kernel.executed if "base64.b64decode" not in s]
    if not run_all:
        assert not any("SENTINEL" in s for s in executed)
        assert not any("%restart_python" in s for s in executed)
    if "%sql" in source or "%scala" in source:
        magic = "%sql" if "%sql" in source else "%scala"
        assert saved.startswith(magic + "\n"), saved
        assert not any(l.startswith('#') for l in saved.splitlines()), saved
        if run_all:
            assert any(s.startswith(magic + "\n") and "SENTINEL" in s for s in executed)
    else:
        compile(saved, "<saved>", "exec")
        assert "restart omitted" in saved


@pytest.mark.parametrize("source", _SUPPORTED_MIXED_PIP)
@pytest.mark.parametrize("strict", [False, True])
def test_child_mixed_pip_failed_install_not_success(monkeypatch, source, strict):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    result, installs = _drive_child(monkeypatch, source, False, strict)
    assert installs == ["real_pkg"]
    if strict:
        assert result["status"] == "error", result
        assert result["cells_ok"] == 0
    else:
        assert result["failed_installs"] == ["real_pkg"], result
        cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
        saved = "".join(cached["cells"][0]["source"])
        assert not _executable_pip_lines(saved)
        assert jm._PIP_NOTE_FAILED.lstrip('# ') in saved


@pytest.mark.parametrize("source", _SUPPORTED_MIXED_PIP)
@pytest.mark.parametrize("strict", [False, True])
def test_child_mixed_pip_routes_supported_remainder(monkeypatch, source, strict):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    kernel = _Kernel()
    result, installs = _drive_child(monkeypatch, source, True, strict, kernel=kernel)
    assert result["status"] == "ok", result
    assert not result.get("unresolved_pip"), result
    assert result["cells_ok"] == 1
    assert installs == ["real_pkg"]
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    saved = "".join(cached["cells"][0]["source"])
    assert not _executable_pip_lines(saved)
    if "%sql" in source or "%scala" in source:
        magic = "%sql" if "%sql" in source else "%scala"
        assert saved.startswith(magic + "\n"), saved
        assert any(s.startswith(magic + "\n") and "SENTINEL" in s for s in kernel.executed)
        assert not any(l.startswith('#') for l in saved.splitlines()), saved
    else:
        compile(saved, "<saved>", "exec")
        assert "restart omitted" in saved


@pytest.mark.parametrize("source", _UNREWRITABLE_PIP_CELLS)
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("run_all", [False, True])
def test_parent_checks_pip_rewrite_before_installing(
        monkeypatch, tmp_path, source, strict, run_all):
    """Rejected cells must not install anything, even when installs succeed."""
    tail = "FOLLOWING_CELL = 99\n"
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source, tail], True, run_all=run_all)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    if strict:
        with pytest.raises(RuntimeError, match="cannot be neutralised"):
            asyncio.run(coro)
    else:
        result = asyncio.run(coro)
        assert result["status"] in (
            ("PARTIAL", "FAIL") if run_all else ("FAIL",)), result
        assert any(c["status"] == "fail" for c in result["cell_results"])
        saved_sources = ["".join(c["source"]) for c in _saved_notebook(kernel)["cells"]]
        rejected = next(s for s in saved_sources if "pip install real_pkg" in s)
        assert not _executable_pip_lines(rejected)
        assert jm._PIP_NOTE_NOT_ATTEMPTED in rejected
        for line in source.splitlines():
            if not jm._PIP_INSTALL_LINE_RE.match(line):
                if line == "%pyspark":
                    line = "# %pyspark"  # Original retained beside AIDP's %python.
                assert line in rejected.splitlines(), "unrelated code must remain intact"
        assert any(tail.strip() in s for s in saved_sources)
        if run_all:
            assert any(tail.strip() in s for s in kernel.executed)
    assert installs == [], "syntax rejection happened after changing dependencies"
    direct = "\n".join(s for s in kernel.executed if "base64.b64decode" not in s)
    assert "!pip install real_pkg" not in direct


@pytest.mark.parametrize("source", _UNREWRITABLE_PIP_CELLS)
@pytest.mark.parametrize("strict", [False, True])
def test_child_checks_pip_rewrite_before_installing(monkeypatch, source, strict):
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    kernel = _Kernel()
    tail = "FOLLOWING_CELL = 99\n"
    result, installs = _drive_child(
        monkeypatch, [source, tail], True, strict, kernel=kernel)
    assert installs == [], "syntax rejection happened after changing dependencies"
    assert result["cells_ok"] == (0 if strict else 1), result
    if strict:
        assert result["status"] == "error", result
        assert "cannot be neutralised" in result["error"]
    else:
        assert result["unresolved_pip"], result
        assert not result.get("failed_installs"), "no installation was attempted"
        assert any(tail.strip() in s for s in kernel.executed)
        cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
        rejected = "".join(cached["cells"][0]["source"])
        assert not _executable_pip_lines(rejected)
        assert jm._PIP_NOTE_NOT_ATTEMPTED in rejected
        for line in source.splitlines():
            if not jm._PIP_INSTALL_LINE_RE.match(line):
                if line == "%pyspark":
                    line = "# %pyspark"
                assert line in rejected.splitlines()
    direct = "\n".join(s for s in kernel.executed if "base64.b64decode" not in s)
    assert "!pip install real_pkg" not in direct


_REWRITABLE_PIP_CELLS = [
    "!pip install real_pkg\n",
    "%pip install real_pkg\nSENTINEL = 42\n",
    "if True:\n    !pip install real_pkg\nSENTINEL = 42\n",
    'HELP = """\n%time example\n!ls /tmp\n"""\n!pip install real_pkg\nSENTINEL = 42\n',
] + [
    f"{magic}\n{body}"
    for magic in ("%python", "%pyspark")
    for pip in ("%pip", "!pip")
    for body in (
        f"{pip} install real_pkg\n",
        f"{pip} install real_pkg\nSENTINEL = 42\n",
        f"if True:\n    {pip} install real_pkg\nSENTINEL = 42\n",
    )
]


def _assert_saved_python_pip_cell(source, saved):
    """Validate the artifact body without treating its language header as Python."""
    if source.startswith(("%python\n", "%pyspark\n")):
        assert saved.splitlines()[0] == "%python"
        if source.startswith("%pyspark\n"):
            assert "# %pyspark" in saved.splitlines()
        saved = saved.partition("\n")[2]
    compile(saved, "<saved-pip-cell>", "exec")


@pytest.mark.parametrize("magic", ["%python", "%pyspark"])
def test_python_pip_declarations_preserve_comments_strings_and_idempotence(magic):
    source = (f"{magic}  # Python cell\n%pip install matplotlib\n"
              'HELP = """\n%python\n!pip install decoy\n"""\n')
    indices, packages, unsupported = jm._find_pip_installs(source)
    assert packages == ["matplotlib"] and not unsupported
    saved, issue = jm._rewrite_pip_cell(source, indices)
    assert issue is None
    assert saved.splitlines()[0] == source.splitlines()[0]
    namespace = {}
    exec(saved.partition("\n")[2], namespace)
    assert namespace["HELP"] == "\n%python\n!pip install decoy\n"
    assert jm._rewrite_pip_cell(saved, jm._find_pip_installs(saved)[0]) == (saved, None)


@pytest.mark.parametrize("header", [
    "%python-extra", "%pyspark-extra", "%python unexpected_argument",
    "%pyspark print('must not discard')", "%%python", "x = 1\n%python",
])
def test_pip_validation_does_not_discard_unsupported_directives(header):
    source = header + "\n!pip install matplotlib"
    saved, issue = jm._rewrite_pip_cell(source, jm._find_pip_installs(source)[0])
    assert issue and "cannot be neutralised" in issue
    assert saved.startswith(header + "\n")
    assert not _executable_pip_lines(saved)


@pytest.mark.parametrize("source", [
    "%python\n%pip install matplotlib", "%python\n!pip install slack_sdk",
])
def test_incident_python_pip_in_scala_dependency(monkeypatch, tmp_path, source):
    """Oct 5 run: exact failed cells, Scala default, real dependency cell loop."""
    notebook = {"cells": [{"cell_type": "code", "source": [source],
                           "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {"language_info": {"name": "scala"}},
                "nbformat": 4, "nbformat_minor": 5}
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)

    async def preserve_converted_cell(_system, prompt, **kwargs):
        # Scala-default notebooks still invoke the converter after the pip
        # gate. Model an identity response; exercise the actual gate/save loop.
        converted = prompt.rsplit("```scala\n", 1)[1].rsplit("\n```", 1)[0]
        assert not _executable_pip_lines(converted)
        assert jm._PIP_NOTE_CODE_ONLY in converted
        return converted, {"input": 0, "output": 0}

    monkeypatch.setattr(jm, "call_opus_with_tools", preserve_converted_cell)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "dep_incident", {},
        run_all=False))
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    saved = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    _assert_saved_python_pip_cell(source, saved)
    assert jm._PIP_NOTE_CODE_ONLY in saved
    assert not _executable_pip_lines(saved)
    assert installs == []


@pytest.mark.parametrize("source", _REWRITABLE_PIP_CELLS)
@pytest.mark.parametrize("install_result", [False, True])
@pytest.mark.parametrize("strict", [False, True])
def test_preflight_preserves_valid_pip_cells_and_install_outcomes(
        monkeypatch, tmp_path, source, install_result, strict):
    """Both paths keep their success/failure policy and final annotations."""
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source], install_result)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    if strict and not install_result:
        with pytest.raises(RuntimeError, match="were not installed"):
            asyncio.run(coro)
    else:
        result = asyncio.run(coro)
        assert (result["status"] == "PASS") == install_result, result
        saved_source = "".join(_saved_notebook(kernel)["cells"][0]["source"])
        expected_note = jm._PIP_NOTE_INSTALLED if install_result else jm._PIP_NOTE_FAILED
        assert expected_note in saved_source
        assert "pip rewrite preflight" not in saved_source
        assert _executable_pip_lines(saved_source) == []
        _assert_saved_python_pip_cell(source, saved_source)
        if "SENTINEL" in source:
            assert "SENTINEL = 42" in saved_source.splitlines()
            assert any("SENTINEL = 42" in s for s in kernel.executed)
        if source.startswith("if True:"):
            assert "    pass" in saved_source
    assert installs == ["real_pkg"]

    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    child_kernel = _Kernel()
    child, child_installs = _drive_child(
        monkeypatch, source, install_result, strict, kernel=child_kernel)
    assert child_installs == ["real_pkg"]
    if strict and not install_result:
        assert child["status"] == "error", child
        assert "not installed" in child["error"]
    else:
        assert child["status"] == "ok", child
        assert bool(child.get("failed_installs")) == (not install_result)
        assert not child.get("unresolved_pip"), child
        cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
        child_source = "".join(cached["cells"][0]["source"])
        assert expected_note in child_source
        assert "pip rewrite preflight" not in child_source
        assert _executable_pip_lines(child_source) == []
        _assert_saved_python_pip_cell(source, child_source)
        if "SENTINEL" in source:
            assert "SENTINEL = 42" in child_source.splitlines()
            assert any("SENTINEL = 42" in s for s in child_kernel.executed)
        if source.startswith("if True:"):
            assert "    pass" in child_source


@pytest.mark.parametrize("source", _REWRITABLE_PIP_CELLS)
def test_preflight_keeps_code_only_pip_conversion_install_free(monkeypatch, tmp_path, source):
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source], True, run_all=False)
    result = asyncio.run(coro)
    assert result["status"] == "CONVERTED_NOT_VALIDATED", result
    assert installs == []
    saved_source = "".join(_saved_notebook(kernel)["cells"][0]["source"])
    assert jm._PIP_NOTE_CODE_ONLY in saved_source
    assert "pip rewrite preflight" not in saved_source
    assert _executable_pip_lines(saved_source) == []
    _assert_saved_python_pip_cell(source, saved_source)
    direct = "\n".join(s for s in kernel.executed if "base64.b64decode" not in s)
    assert "SENTINEL = 42" not in direct


def test_rejected_cell_does_not_block_later_valid_install(monkeypatch, tmp_path):
    bad = "!pip install real_pkg\n%time x = 1\n"
    good = "!pip install real_pkg\nSENTINEL = 42\n"
    result, installs, _, saved_source, direct = _drive(monkeypatch, tmp_path, [bad, good])
    assert result["status"] in ("PARTIAL", "FAIL"), result
    assert installs == ["real_pkg"]
    assert "SENTINEL = 42" in saved_source.splitlines()
    assert "SENTINEL = 42" in direct
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    kernel = _Kernel()
    child, child_installs = _drive_child(
        monkeypatch, [bad, good], True, False, kernel=kernel)
    assert child_installs == ["real_pkg"]
    assert child["unresolved_pip"], child
    assert child["cells_ok"] == 1
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    # Save-back/cache refresh normalizes trailing newlines when another cell changed.
    rejected = "".join(cached["cells"][0]["source"])
    assert not _executable_pip_lines(rejected)
    assert "%time x = 1" in rejected
    assert jm._PIP_NOTE_NOT_ATTEMPTED in rejected
    assert jm._PIP_NOTE_INSTALLED in "".join(cached["cells"][1]["source"])
    assert any("SENTINEL = 42" in s for s in kernel.executed)


def test_pip_text_in_a_string_does_not_skip_the_rest_of_the_cell(monkeypatch, tmp_path):
    """A `!pip` line inside a string literal must not be counted as "another
    magic".

    Detection already ignores it, but the other-magic check decides whether the
    cell can be partly executed. Treating string text as a magic neutralises the
    whole cell, so every other statement in it is silently dropped while the
    task still reports success.
    """
    source = ('HELP = """\n'
              '!pip install decoy\n'
              '"""\n'
              '!pip install real_pkg\n'
              'SENTINEL_AFTER_STRING = 42\n'
              'print("PROBE:", SENTINEL_AFTER_STRING)\n')
    result, installs, saved, saved_src, direct_exec = _drive(
        monkeypatch, tmp_path, [source])

    assert installs == ["real_pkg"], f"decoy inside the string was installed: {installs}"
    # the remaining Python actually ran
    assert "SENTINEL_AFTER_STRING = 42" in direct_exec, (
        "cell was skipped — unrelated Python never executed")
    # the real install is neutralised, the string's contents are untouched
    assert f"{jm._PIP_NEUTRALISED_PREFIX} — !pip install real_pkg" in saved_src
    assert '\n!pip install decoy\n' in saved_src, "string literal was rewritten"
    assert result["status"] in ("PASS", "PASS_CONDITIONAL_EXIT_COVERAGE"), result


def test_child_pip_rewrite_is_saved_when_it_is_the_only_change(monkeypatch, tmp_path):
    """A dependency whose ONLY change is pip neutralisation must still be
    written back, and its cache entry replaced.

    Save-back and the cache refresh keyed off `cells_fixed`, which deterministic
    rewrites never move — so the neutralised body stayed in memory only and the
    saved notebook kept an executable install line.
    """
    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)
    path = "/Workspace/test_pip/notebooks/childonly.ipynb"
    nb = {"cells": [{"cell_type": "code",
                     "source": ["# c\n!pip install real_pkg\n"],
                     "metadata": {}, "outputs": [], "execution_count": None}],
          "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    installs = []

    async def fake_install(cluster_id, session, pkg, out_path, **kwargs):
        installs.append(pkg)
        return True

    async def noop(*a, **k):
        return None

    monkeypatch.setitem(jm._notebook_content_cache, path, json.dumps(nb))
    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_pip")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_pip/notebooks")
    monkeypatch.setattr(jm.asyncio, "sleep", noop)

    kernel = _Kernel()
    log = jm.NotebookLog(path, "test_pip", "childonly")
    result = asyncio.run(jm._inline_child_notebook(
        path, kernel, log, "test_pip", "", "", "", {}))

    assert installs == ["real_pkg"]
    assert result["status"] == "ok", result
    # 1. the in-memory cache must now hold the neutralised body — the next %run
    #    reads from here.
    cached = json.loads(jm._notebook_content_cache[path])
    cached_src = "\n".join("".join(c["source"]) for c in cached["cells"])
    assert _executable_pip_lines(cached_src) == [], f"stale cache: {cached_src!r}"
    assert jm._PIP_NEUTRALISED_PREFIX in cached_src
    # 2. save-back must have been attempted despite cells_fixed being 0
    assert result.get("cells_fixed", 0) == 0, "expected no LLM fixes in this path"
    wrote = [c for c in kernel.executed if "base64.b64decode" in c]
    assert wrote, "no save-back attempted — neutralisation would be lost"


def test_child_pip_and_finalizer_rewrites_are_both_cached_and_saved(monkeypatch):
    """The #105 source rewrite and final-artifact rewrite are additive.

    A child containing both changes must refresh the reusable cache and reach
    save-back with neither rewrite lost.  This pins the manual rebase conflict
    between ``child_source_rewritten`` and ``_child_finalizer_changed``.
    """
    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    async def fake_install(cluster_id, session, pkg, out_path, **kwargs):
        return True

    path = "/Workspace/test_pip/notebooks/combined.ipynb"
    nb = {
        "cells": [
            {
                "cell_type": "code",
                "source": ["!pip install real_pkg\n"],
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
            {
                "cell_type": "code",
                "source": [
                    'oidlUtils.parameters.setTaskValue("flag", flag)\n'
                ],
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)
    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", fake_install)
    monkeypatch.setitem(jm._notebook_content_cache, path, json.dumps(nb))
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", "/Workspace/test_pip")
    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/test_pip/notebooks")

    kernel = _Kernel()
    log = jm.NotebookLog(path, "test_pip", "combined")
    result = asyncio.run(jm._inline_child_notebook(
        path, kernel, log, "test_pip", "", "", "", {}))

    assert result["status"] == "ok", result
    cached = json.loads(jm._notebook_content_cache[path])
    cached_src = "\n".join("".join(c["source"]) for c in cached["cells"])
    assert _executable_pip_lines(cached_src) == []
    assert jm._PIP_NEUTRALISED_PREFIX in cached_src
    assert 'setTaskValue("flag", str(flag))' in cached_src
    wrote = [code for code in kernel.executed if "base64.b64decode" in code]
    assert wrote, "combined source/finalizer rewrite was not persisted"


@pytest.mark.parametrize("child_source,install_result,field", [
    # Pip can be commented, but `%time` remains an unsupported executable magic.
    ("!pip install real_pkg\n%time x = 1\n", True, "unresolved_pip"),
    # install reported failure
    ("# c\n!pip install real_pkg\nZ = 1\n", False, "failed_installs"),
])
def test_child_reports_pip_findings_outside_strict_mode(
        monkeypatch, child_source, install_result, field):
    """Outside strict gating the child keeps going — but the finding must reach
    the caller.

    Both shapes previously returned a bare `status: "ok"`, so the parent read a
    dependency that cannot run as a clean success.
    """
    result, installs = _drive_child(
        monkeypatch, child_source, install_result=install_result, strict=False)
    assert result["status"] == "ok", "non-strict mode should keep processing"
    assert result.get(field), (
        f"{field} missing from child result — the failure was erased: {result}")


def test_parent_counts_unresolved_child_pip_as_a_failure(monkeypatch, tmp_path):
    """A child that completes with unresolved pip must not leave the parent's
    %run cell reported OK — otherwise the task PASSes over a dependency whose
    saved notebook still carries an executable install line."""
    notebook = {
        "cells": [{"cell_type": "code", "source": ["%run ./dep\n"], "metadata": {},
                   "outputs": [], "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)

    async def child_with_findings(child_path, *a, **k):
        return {"status": "ok", "did_exit": False, "cells_ok": 1, "cells_fixed": 0,
                "failed_installs": ["real_pkg"],
                "unresolved_pip": ["cell 0: pip line cannot be neutralised"]}

    monkeypatch.setattr(jm, "_inline_child_notebook", child_with_findings)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))

    assert result["status"] != "PASS", f"task passed over unresolved child pip: {result}"
    cr = result.get("cell_results") or []
    assert any(c.get("status") == "fail" and "unresolved pip" in str(c.get("note", ""))
               for c in cr), cr


_CHILD_WITH_FINDINGS = {
    "status": "ok", "did_exit": False, "cells_ok": 1, "cells_fixed": 0,
    "failed_installs": ["real_pkg"],
    "unresolved_pip": ["cell 0: pip line cannot be neutralised"],
}


@pytest.mark.parametrize("invocation,source", [
    ("%run", "%run ./dep\n"),
    ("python_notebook_run", 'dbutils.notebook.run("./dep", 60)\n'),
    ("scala_notebook_run", '%scala\ndbutils.notebook.run("./dep", 60)\n'),
])
def test_every_child_invocation_path_reports_unresolved_pip(
        monkeypatch, tmp_path, invocation, source):
    """Children are inlined from four places; every one must surface a child's
    unresolved pip findings.

    Only the `%run` path consumed the returned result — the `notebook.run`
    paths (Python direct, cohort fan-out, and %scala) discarded it, so a
    dependency that cannot run as saved was reported as a clean PASS.
    """
    notebook = {
        "cells": [{"cell_type": "code", "source": [source], "metadata": {},
                   "outputs": [], "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)

    called = []

    async def child_with_findings(child_path, *a, **k):
        called.append(child_path)
        return dict(_CHILD_WITH_FINDINGS)

    monkeypatch.setattr(jm, "_inline_child_notebook", child_with_findings)
    monkeypatch.setattr(jm, "_resolve_nbrun_child_path",
                        lambda *a, **k: "/Workspace/test_pip/notebooks/dep.ipynb")
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))

    assert called, f"{invocation}: child was never inlined — harness no longer covers this path"
    assert result["status"] != "PASS", (
        f"{invocation}: task PASSed while the inlined child left pip unresolved: {result}")
    assert result.get("pip_unresolved", 0) >= 1, (
        f"{invocation}: finding never counted: {result}")


def test_task_path_failed_install_does_not_report_pass(monkeypatch, tmp_path):
    """A failed install on the TASK path must gate the verdict, not just log.

    The child path already counted this. The task path only wrote a migration
    note nothing gated on, so the identical failure reported PASS here and
    PARTIAL one level down — and the saved cell still claimed the package was
    "installed via cluster libraries API".
    """
    source = "# old\n!pip install shortuuid\nimport shortuuid\n"
    notebook = {
        "cells": [{"cell_type": "code", "source": [source], "metadata": {},
                   "outputs": [], "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)

    async def failing_install(cluster_id, session, pkg, out_path, **kwargs):
        installs.append(pkg)
        return False

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", failing_install)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))

    assert installs == ["shortuuid"]
    assert result["status"] != "PASS", f"failed install reported PASS: {result}"
    assert result.get("pip_unresolved", 0) >= 1, result
    # and the artifact must not claim the package was installed
    saved = _saved_notebook(kernel)
    saved_src = "\n".join("".join(c["source"]) for c in saved["cells"])
    assert jm._PIP_NOTE_FAILED in saved_src, (
        f"artifact still claims an install happened: {saved_src!r}")
    assert jm._PIP_NOTE_INSTALLED not in saved_src


def test_code_only_artifact_does_not_claim_an_install_happened(monkeypatch, tmp_path):
    """In code-only mode nothing is installed, so the annotation must say the
    packages come from cluster libraries rather than asserting an install."""
    notebook = {
        "cells": [{"cell_type": "code", "source": ["# c\n!pip install real_pkg\nZ = 1\n"],
                   "metadata": {}, "outputs": [], "execution_count": None}],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel()
    asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/dep.ipynb", kernel, "test_pip", "dep_x", {}, run_all=False))
    saved = _saved_notebook(kernel)
    saved_src = "\n".join("".join(c["source"]) for c in saved["cells"])
    assert installs == [], "code-only mode must not install"
    assert jm._PIP_NOTE_CODE_ONLY in saved_src, saved_src
    assert _executable_pip_lines(saved_src) == []


def test_pip_below_a_conditional_exit_is_neutralised(monkeypatch, tmp_path):
    """Cells below a fired conditional exit are the un-taken branch this path
    exists to PRESERVE — a live install line there fails with "Invalid pip
    command format!" exactly when that branch finally runs."""
    notebook = {
        "cells": [{"cell_type": "code", "source": [s], "metadata": {},
                   "outputs": [], "execution_count": None} for s in (
            "if MIGRATION_TIME_FLAG:\n    aidputils.notebook.exit('done')\n",
            "# old\n!pip install below_exit_pkg\nSENTINEL_BELOW = 1\n",
        )],
        "metadata": {}, "nbformat": 4, "nbformat_minor": 5,
    }
    installs = []
    _patch_common(monkeypatch, tmp_path, notebook, installs)
    kernel = _Kernel(fire_exit=True)
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", "01_task", {}))

    saved = _saved_notebook(kernel)
    saved_src = "\n".join("".join(c["source"]) for c in saved["cells"])
    assert _executable_pip_lines(saved_src) == [], (
        f"live pip survived below the conditional exit: {saved_src!r}")
    # nothing below the exit runs, so nothing may be installed
    assert installs == [], f"installed while below a fired exit: {installs}"
    # the preserved branch keeps its code
    assert "SENTINEL_BELOW = 1" in saved_src
    assert result["status"] in ("PASS_CONDITIONAL_EXIT_COVERAGE", "PARTIAL"), result


@pytest.mark.parametrize("magic", ["%md", "%md-sandbox"])
@pytest.mark.parametrize("body", ["!pip install demo_pkg", "%pip install -r requirements.txt",
                                  "<div>Documentation without an install</div>"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("run_all", [False, True])
def test_markdown_cell_quoting_pip_installs_nothing(
        monkeypatch, tmp_path, magic, body, strict, run_all):
    """Own-line commands in either markdown magic are examples, not installs."""
    source = f"{magic}\nExample command:\n{body}"
    kernel, installs, coro = _drive_strict(
        monkeypatch, tmp_path, [source], True, run_all=run_all)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    result = asyncio.run(coro)
    assert installs == [], f"{magic} prose triggered an install: {installs}"
    assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
    assert result["failed"] == 0
    assert result["pip_unresolved"] == 0
    assert "".join(_saved_notebook(kernel)["cells"][0]["source"]) == source


@pytest.mark.parametrize("magic", ["%md", "%md-sandbox"])
@pytest.mark.parametrize("body", ["!pip install demo_pkg", "%pip install -r requirements.txt",
                                  "<div>Documentation without an install</div>"])
@pytest.mark.parametrize("strict", [False, True])
def test_child_markdown_cell_quoting_pip_installs_nothing(monkeypatch, magic, body, strict):
    """A `%md` cell in a DEPENDENCY that quotes an install command in prose is
    documentation.

    The task path exempted `%md` but the child path scanned pip lines first, so
    a docs cell attempted a real install AND then raised an unresolved-pip
    finding that pushed the parent to a failure verdict.
    """
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    child = f"{magic}\nExample command:\n{body}"
    kernel = _Kernel()
    result, installs = _drive_child(
        monkeypatch, child, install_result=True, strict=strict, kernel=kernel)
    assert installs == [], f"{magic} prose in a child triggered an install: {installs}"
    assert not result.get("failed_installs"), result
    assert not result.get("unresolved_pip"), result
    assert result["status"] == "ok", result
    assert result["cells_ok"] == 1
    assert not any(child in code for code in kernel.executed)
    cached = json.loads(jm._notebook_content_cache["/Workspace/test_pip/notebooks/child.ipynb"])
    assert "".join(cached["cells"][0]["source"]) == child


def test_sandbox_markdown_exemption_does_not_hide_real_pip(monkeypatch, tmp_path):
    source = 'DOC = "%md-sandbox"\n!pip install demo_pkg\nSENTINEL = 42'
    result, installs, _, saved_source, direct = _drive(monkeypatch, tmp_path, [source])
    assert installs == ["demo_pkg"]
    assert result["status"] == "PASS", result
    assert "SENTINEL = 42" in direct
    assert _executable_pip_lines(saved_source) == []
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    child, child_installs = _drive_child(monkeypatch, source, True, True)
    assert child_installs == ["demo_pkg"]
    assert child["status"] == "ok", child


def test_child_failed_install_errors_under_strict_gating(monkeypatch, tmp_path):
    """The dependency path ignored `install_missing_package`'s boolean, so a
    package that never installed still yielded status ok / cells_ok 1 even with
    STRICT_CELL_GATING=True, so the boolean must be checked explicitly.
    """
    result, installs = _drive_child(
        monkeypatch, "# c\n!pip install nonexistent_pkg\nZ = 1\n",
        install_result=False, strict=True)
    assert installs == ["nonexistent_pkg"]
    assert result["status"] == "error", result
    assert "not installed" in result.get("error", ""), result
    assert result.get("cells_ok", 0) == 0, result


# ─────────────────────────────────────────────────────────────────────────
# Part D — generated reports retain existing pip failure details
# ─────────────────────────────────────────────────────────────────────────

class _ReportingKernel(_Kernel):
    async def run_stateless(self, code, **kwargs):
        return await self.execute(code, **kwargs)


def _drive_reporting_job(monkeypatch, tmp_path, sources, *, code_only=False,
                         fire_exit=False, install_result=True, strict=False,
                         child_source=None, backfill=None):
    """Real job + notebook reporting, with all remote operations mocked."""
    def notebook(contents):
        return {"cells": [{"cell_type": "code", "source": [s], "metadata": {},
                           "outputs": [], "execution_count": None} for s in contents],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}

    installs = []
    _patch_common(monkeypatch, tmp_path, notebook(sources), installs)

    async def noop(*a, **kw):
        return None

    async def no_backfill(*a, **kw):
        return {"scanned": 0, "missing": 0, "migrated": 0, "failed": []}

    async def install(cluster_id, session, pkg, out_path, **kw):
        installs.append(pkg)
        return install_result

    child_path = "/Workspace/test_pip/test_pip/notebooks/dep.ipynb"

    async def mapped_child(*a, **kw):
        return child_path

    monkeypatch.setattr(cluster_lifecycle, "install_missing_package", install)
    monkeypatch.setattr(jm, "_get_throttle_coord", lambda: None)
    monkeypatch.setattr(jm, "stage_export_resources", noop)
    monkeypatch.setattr(jm, "_ensure_redirect_schema", noop)
    monkeypatch.setattr(jm, "backfill_missing_deps", backfill or no_backfill)
    monkeypatch.setattr(jm, "ensure_migrated", mapped_child)
    monkeypatch.setattr(jm, "_resolve_nbrun_child_path", lambda *a, **kw: child_path)
    monkeypatch.setattr(jm, "CODE_ONLY", code_only)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", strict)
    monkeypatch.setattr(jm, "SKIP_MIGRATED", False)
    monkeypatch.setattr(jm, "START_TASK", None)
    monkeypatch.setattr(jm, "ONLY_TASKS", [])
    monkeypatch.setattr(jm, "JOB_ROOT", "")
    monkeypatch.setattr(jm, "_migration_cache", {})
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_inlined_nb_run_children", set())
    if child_source is not None:
        monkeypatch.setitem(jm._notebook_content_cache, child_path,
                            json.dumps(notebook(child_source if isinstance(child_source, list)
                                                else [child_source])))
        monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    kernel = _ReportingKernel(fire_exit=fire_exit)
    job = {"job_name": "test_pip", "parameters": {}, "root": "",
           "execution_layers": [["task"]],
           "tasks": [{"task_key": "task", "notebook_path": "/Workspace/test_pip/source.ipynb",
                      "depends_on": [], "run_deps": []}]}
    result = asyncio.run(jm.process_job(job, kernel))
    reports = {Path(p).name: content for p, content in kernel.files.items()
               if p.endswith(".md")}
    return result, reports, installs, kernel


@pytest.mark.parametrize("source,mode,install_ok,status,ok,failed,pip_count,reasons", [
    ("!pip install real_pkg --hash=sha256:abc\nSENTINEL = 42", "execute", True,
     "PARTIAL", 1, 0, 1, ["--hash", "unhashed installation not attempted"]),
    ("!pip install --group dev\nSENTINEL = 42", "execute", True,
     "PARTIAL", 1, 0, 1, ["--group dev", "dependency group not resolved"]),
    ("!pip install real_pkg --hash sha256:abc\nSENTINEL = 42", "code_only", True,
     "CONVERTED_NOT_VALIDATED", 1, 0, 1, ["--hash", "unhashed installation not attempted"]),
    ("!pip install --group=dev\nSENTINEL = 42", "below_exit", True,
     "PARTIAL", 1, 0, 1, ["--group dev", "dependency group not resolved"]),
    ("!pip install -r requirements.txt", "execute", True, "FAIL", 0, 1, 0,
     ["-r requirements.txt"]),
    ("!pip install -r requirements.txt\nSENTINEL = 42", "execute", True, "PARTIAL", 1, 0, 1,
     ["-r requirements.txt"]),
    ("!pip install real_pkg\nSENTINEL = 42", "execute", False, "PARTIAL", 1, 0, 1,
     ["package not installed: real_pkg"]),
    ("!pip install real_pkg\n%time x = 1", "execute", True, "FAIL", 0, 1, 0,
     ["cannot be neutralised"]),
    ("!pip install -r requirements.txt\n%time x = 1", "execute", True, "FAIL", 0, 1, 0,
     ["-r requirements.txt", "cannot be neutralised"]),
    ("!pip install -r requirements.txt\n%time x = 1", "code_only", True,
     "FAIL", 0, 1, 0, ["-r requirements.txt", "cannot be neutralised"]),
    ("!pip install -r requirements.txt\nSENTINEL = 42", "code_only", True,
     "CONVERTED_NOT_VALIDATED", 1, 0, 1, ["-r requirements.txt"]),
    ("!pip install -r requirements.txt\nSENTINEL = 42", "below_exit", True,
     "PARTIAL", 1, 0, 1, ["-r requirements.txt"]),
    ("!pip install -r requirements.txt\n%time x = 1", "below_exit", True,
     "PARTIAL", 1, 0, 1, ["-r requirements.txt", "cannot be neutralised"]),
])
def test_unresolved_pip_details_reach_all_reports_without_changing_verdicts(
        monkeypatch, tmp_path, source, mode, install_ok, status, ok, failed, pip_count, reasons):
    sources = [source]
    if mode == "below_exit":
        sources.insert(0, "if FLAG:\n    aidputils.notebook.exit('done')")
    result, reports, installs, kernel = _drive_reporting_job(
        monkeypatch, tmp_path, sources, code_only=mode == "code_only",
        fire_exit=mode == "below_exit", install_result=install_ok)
    task = result["task_results"]["task"]
    # Reporting must not reclassify cells, double-count failures, or install more.
    assert (task["status"], task["ok"], task["failed"], task["pip_unresolved"]) == (
        status, ok, failed, pip_count)
    assert installs == ([] if install_ok else ["real_pkg"])
    for name in ("JOB_REPORT.md", "test_report.md", "migration_report.md"):
        for reason in reasons:
            assert reason in reports[name], (name, reason, reports[name])
    assert f"**task** cell {len(sources) - 1}:" in reports["JOB_REPORT.md"]
    if mode != "execute":
        direct = "\n".join(s for s in kernel.executed if "base64.b64decode" not in s)
        assert "SENTINEL = 42" not in direct


@pytest.mark.parametrize("source", ["%run ./dep", 'dbutils.notebook.run("./dep", 60)',
                                    '%scala\ndbutils.notebook.run("./dep", 60)'])
def test_real_child_pip_reason_reaches_job_report(monkeypatch, tmp_path, source):
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, [source],
        child_source="!pip install -r requirements.txt\nCHILD_SENTINEL = 1")
    assert result["task_results"]["task"]["status"] in ("FAIL", "PARTIAL"), result
    assert installs == []
    for name in ("JOB_REPORT.md", "test_report.md", "migration_report.md"):
        assert "-r requirements.txt" in reports[name], (name, reports[name])
    assert "**task** cell 0:" in reports["JOB_REPORT.md"]
    assert reports["JOB_REPORT.md"].count("-r requirements.txt") == 1
    if source.startswith("%scala"):
        assert "%scala" in result["task_results"]["task"]["cell_results"][0]["note"]


def test_strict_install_error_reaches_job_report(monkeypatch, tmp_path):
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, ["!pip install real_pkg"], install_result=False, strict=True)
    assert result["task_results"]["task"]["status"] == "ERROR", result
    assert result["total_failed"] == 1
    assert installs == ["real_pkg"]
    assert "real_pkg" in reports["JOB_REPORT.md"]
    assert "were not installed" in reports["JOB_REPORT.md"]


@pytest.mark.parametrize("code_only,fire_exit,status", [
    (False, False, "PASS"), (True, False, "CONVERTED_NOT_VALIDATED"),
    (False, True, "PASS_CONDITIONAL_EXIT_COVERAGE"),
])
def test_clean_pip_reports_do_not_invent_findings(
        monkeypatch, tmp_path, code_only, fire_exit, status):
    sources = ["!pip install real_pkg\nSENTINEL = 42"]
    if fire_exit:
        sources.insert(0, "if FLAG:\n    aidputils.notebook.exit('done')")
    result, reports, installs, _ = _drive_reporting_job(
        monkeypatch, tmp_path, sources, code_only=code_only, fire_exit=fire_exit)
    task = result["task_results"]["task"]
    assert task["status"] == status, task
    assert task["pip_unresolved"] == 0
    assert installs == ([] if code_only or fire_exit else ["real_pkg"])
    assert "## Errors & Warnings" not in reports["JOB_REPORT.md"]
    assert all(not cr.get("pip_unresolved_reason") for cr in task["cell_results"])


@pytest.mark.parametrize("cell_status,show_note", [
    ("error", True), ("fail", True), ("ok", False), ("migrated", False),
])
def test_job_report_preserves_other_cell_note_behavior(
        monkeypatch, tmp_path, cell_status, show_note):
    original = {"status": "PARTIAL" if show_note else "PASS", "ok": 0,
                "failed": int(show_note), "fixed": 0, "tokens": 7,
                "cell_results": [{"cell": 0, "status": cell_status,
                                  "note": "existing diagnostic"}]}

    async def notebook_result(*a, **kw):
        return json.loads(json.dumps(original))

    monkeypatch.setattr(jm, "process_notebook", notebook_result)
    result, reports, installs, _ = _drive_reporting_job(monkeypatch, tmp_path, ["X = 1"])
    assert result["task_results"]["task"] == original, "reporting mutated the result"
    assert ("existing diagnostic" in reports["JOB_REPORT.md"]) == show_note
    assert "**Total tokens across all tasks:** 7" in reports["JOB_REPORT.md"]
    assert installs == []


# ─────────────────────────────────────────────────────────────────────────
# Part E — both paths use the shared line scan (the anchor cannot return)
# ─────────────────────────────────────────────────────────────────────────

_JM_SRC = (Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py").read_text()


def test_no_pip_gate_anchors_on_the_whole_cell():
    """Guard: no pip gate may match against a whole-cell string again.

    `stripped_source` (process_notebook) and `stripped` (_inline_child_notebook)
    are whole-cell values; matching a pip pattern against either reintroduces
    the first-statement anchoring this fix removed.
    """
    offenders = [
        ln for ln in _JM_SRC.split("\n")
        if re.search(r'%pip\|!pip', ln)
        and re.search(r'\b(?:stripped_source|stripped)\b', ln)
    ]
    assert offenders == [], offenders


def test_both_migration_paths_share_the_detection_helper():
    """process_notebook AND _inline_child_notebook must both route through
    `_find_pip_installs`. The dependency path having no pip handling at all is
    how a live `!pip` shipped into saved child notebooks while counting `ok`.
    """
    assert _JM_SRC.count("_find_pip_installs(") >= 3, (
        "expected 1 definition + >=2 call sites (task path + child path)")

    def _body(func_name):
        start = _JM_SRC.index(f"async def {func_name}(")
        rest = _JM_SRC[start + 10:]
        nxt = re.search(r'\n(?:async def |def )', rest)
        return rest[:nxt.start()] if nxt else rest

    for func in ("process_notebook", "_inline_child_notebook"):
        assert "_find_pip_installs(" in _body(func), f"{func} does not detect pip cells"
        assert "_comment_pip_lines(" in _body(func), f"{func} does not neutralise pip lines"


def test_child_path_never_appends_raw_source_for_a_pip_cell():
    """Even unresolved remainders must keep pip neutralised, not revert it."""
    body_start = _JM_SRC.index("async def _inline_child_notebook(")
    body = _JM_SRC[body_start:_JM_SRC.index("\nasync def ", body_start + 10)]
    pip_block = body[body.index("_find_pip_installs("):]
    pip_block = pip_block[:pip_block.index("# Skip ONLY non-executable magics")]
    assert "migrated_child_cells.append(child_source)" not in pip_block
    assert "migrated_child_cells.append(_child_kept)" in pip_block
    assert "if _child_issue:" in pip_block
