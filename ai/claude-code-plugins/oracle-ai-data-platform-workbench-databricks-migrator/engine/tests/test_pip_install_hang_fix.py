"""Regression tests for Fix 7c-ext (2026-07-28): pip install hang prevention.

Two layers under test:
  L1 (dbx_export): _extract_subprocess_pip_specs, _extract_os_system_pip_specs,
       _extract_pip_main_specs — scan cell sources for non-magic pip invocations
       (subprocess, os.system, pip.main) so their packages get bundled into
       pip_deps.txt.
  L2 (job_migrate): _rewrite_subprocess_pip_install — comment out residual
       subprocess/os.system pip install calls in %python cells at migration
       time. Defensive backstop in case L1 missed a variant.

Both layers must be string/comment safe (no false-positive rewrites inside
Python string literals or `#` comments).

Root cause of hang: 001_library_importing.ipynb ran a recursive
import_libraries() that on ImportError did
subprocess.check_call([sys.executable, '-m', 'pip', 'install', lib]) then
recursed. On AIDP, pip install hangs on network → 17-minute cell timeout
until manual intervention.

Usage:
    pytest tests/test_pip_install_hang_fix.py -v
    python3 tests/test_pip_install_hang_fix.py
"""
import ast
import os
import sys
import types

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBX_SRC = os.path.join(REPO, "dbx_export/__main__.py")
JM_SRC = os.path.join(REPO, "scripts/job_migrate.py")


def _extract(path, names):
    with open(path) as f:
        tree = ast.parse(f.read())
    kept = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Assign)):
            n = getattr(node, "name", None) or (
                node.targets[0].id if (isinstance(node, ast.Assign)
                                       and isinstance(node.targets[0], ast.Name))
                else None
            )
            if n in names:
                kept.append(node)
    mod = types.ModuleType("m")
    mod.__file__ = path
    import re
    from typing import List as _List
    mod.re = re
    mod.List = _List
    src = ast.unparse(ast.Module(body=kept, type_ignores=[]))
    exec(src, mod.__dict__)
    return mod


# ─── L1: dbx_export extractors ──────────────────────────────────────────
# Use the real exporter so its shared-parser imports are exercised too.
sys.path.insert(0, REPO)
from dbx_export import __main__ as _DBX


def test_l1_subprocess_literal_single_pkg():
    src = 'subprocess.check_call([sys.executable, "-m", "pip", "install", "slack_sdk"])'
    assert _DBX._extract_subprocess_pip_specs(src) == ["slack_sdk"]


def test_l1_subprocess_literal_multi_pkg():
    src = 'subprocess.check_call([sys.executable, "-m", "pip", "install", "slack_sdk", "knockknock"])'
    assert _DBX._extract_subprocess_pip_specs(src) == ["slack_sdk", "knockknock"]


def test_l1_subprocess_with_flags_stripped():
    src = 'subprocess.run(["python", "-m", "pip", "install", "--quiet", "mlflow==2.14.2", "-U"])'
    specs = _DBX._extract_subprocess_pip_specs(src)
    assert "mlflow==2.14.2" in specs
    # Flags NOT in specs
    assert "--quiet" not in specs
    assert "-U" not in specs


def test_l1_subprocess_variant_methods():
    """Detect all subprocess method variants."""
    for method in ("check_call", "check_output", "run", "call", "Popen"):
        src = f'subprocess.{method}([sys.executable, "-m", "pip", "install", "requests"])'
        assert _DBX._extract_subprocess_pip_specs(src) == ["requests"], f"method {method}"


def test_l1_loop_over_literal_list():
    """The exact the pilot deployment 001_library_importing.ipynb pattern."""
    src = '''
libraries_to_install = ["slack_sdk", "knockknock", "openpyxl"]
for lib in libraries_to_install:
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
    except:
        pass
'''
    specs = _DBX._extract_subprocess_pip_specs(src)
    assert set(specs) == {"slack_sdk", "knockknock", "openpyxl"}


def test_l1_os_system_pip():
    src = 'os.system("pip install requests urllib3")'
    specs = _DBX._extract_os_system_pip_specs(src)
    assert "requests" in specs and "urllib3" in specs


def test_l1_pip_main():
    src = 'pip.main(["install", "black", "flake8"])'
    specs = _DBX._extract_pip_main_specs(src)
    assert "black" in specs and "flake8" in specs


def test_l1_no_false_positive_on_non_pip_subprocess():
    """subprocess call that isn't pip install must return empty."""
    for src in [
        'subprocess.check_call(["ls", "-la"])',
        'subprocess.run(["bash", "install.sh"])',
        'subprocess.check_call([sys.executable, "-m", "pipenv", "install"])',  # pipenv, not pip
    ]:
        assert _DBX._extract_subprocess_pip_specs(src) == [], src


def test_l1_no_false_positive_inside_string_literal():
    """An example is not harmless: it can poison cluster requirements before
    the migrator ever runs. Assert the notebook boundary shared by both export
    consumers, including an unescaped call inside a triple-quoted string.
    """
    src = '\'\'\'\nsubprocess.check_call([sys.executable, "-m", "pip", "install", "foo"])\n\'\'\''
    assert list(_DBX._notebook_cell_pip_specs({"cell_type": "code", "source": src})) == []


def test_l1_parse_pip_args_handles_install_position():
    """Args list where 'install' is at various positions."""
    args = 'sys.executable, "-m", "pip", "install", "X"'
    assert _DBX._parse_subprocess_pip_args(args) == ["X"]


def test_l1_parse_pip_args_ignores_boilerplate():
    """`pip`, `-m`, `python` tokens (defensive) must not appear in output."""
    args = '"python", "-m", "pip", "install", "actual_pkg"'
    specs = _DBX._parse_subprocess_pip_args(args)
    assert specs == ["actual_pkg"]


def test_l1_magic_pip_strips_double_quotes_from_requirement():
    """Shell quotes must not be copied into generated requirements files."""
    assert _DBX._parse_pip_install_line('"pydantic<2"') == ["pydantic<2"]


def test_l1_magic_pip_strips_single_quotes_from_requirement():
    """Single-quoted constraints receive the same normalization."""
    assert _DBX._parse_pip_install_line("'urllib3<2'") == ["urllib3<2"]


def test_l1_magic_pip_preserves_unquoted_and_mismatched_quotes():
    """Do not change normal specs or guess how malformed mixed quotes close."""
    assert _DBX._normalize_shell_quoted_pip_spec("tqdm==4.67.1") == "tqdm==4.67.1"
    assert _DBX._normalize_shell_quoted_pip_spec("'pydantic<2\"") == "'pydantic<2\""


# ─── L2: job_migrate rewriter ───────────────────────────────────────────
_JM = _extract(JM_SRC, {
    "_SUBPROCESS_PIP_ANCHOR_RE",
    "_OS_SYSTEM_PIP_INSTALL_RE",
    "_python_active_ranges",
    "_pos_in_active_range",
    "_cell_is_python",
    "_find_matching_paren",
    "_rewrite_subprocess_pip_install",
})


def test_l2_rewrites_subprocess_check_call():
    src = 'subprocess.check_call([sys.executable, "-m", "pip", "install", "slack_sdk"])'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "subprocess.check_call" not in out
    assert "None" in out
    assert "Oracle tool modification: pip install stripped" in out


def test_l2_rewrites_all_variants():
    for method in ("check_call", "check_output", "run", "call", "Popen"):
        src = f'subprocess.{method}([sys.executable, "-m", "pip", "install", "X"])'
        out = _JM._rewrite_subprocess_pip_install(src)
        assert f"subprocess.{method}" not in out, method


def test_l2_rewrites_os_system_pip():
    src = 'os.system("pip install requests")'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "os.system" not in out
    assert "None" in out


def test_l2_preserves_surrounding_try_except():
    """The rewrite substitutes ONLY the subprocess call, keeps try/except."""
    src = '''try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "X"])
except Exception:
    print("oops")'''
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "try:" in out and "except Exception:" in out
    assert "subprocess.check_call" not in out


def test_l2_skips_string_literal_contents():
    """A subprocess.check_call() reference INSIDE a string literal must not
    be rewritten — that's just log data, not executable code."""
    src = 'log = "sample call: subprocess.check_call([sys.executable, \\"-m\\", \\"pip\\", \\"install\\", \\"X\\"])"'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src, "content inside string literal must be unchanged"


def test_l2_skips_comment_contents():
    """A subprocess.check_call() reference INSIDE a Python comment must not
    be rewritten — comments aren't executable."""
    src = "# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'X'])"
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src


def test_l2_no_op_when_no_pattern():
    """Code with no subprocess pip install must return byte-identical."""
    for src in [
        'x = 1',
        'from slack_sdk import WebClient',
        'subprocess.check_call(["ls", "-la"])',
        'subprocess.run(["bash", "install.sh"])',
        'os.system("ls -la /")',
    ]:
        assert _JM._rewrite_subprocess_pip_install(src) == src, src


def test_l2_idempotent_on_second_pass():
    """Rewriter must be idempotent — running twice = same result as once."""
    src = 'subprocess.check_call([sys.executable, "-m", "pip", "install", "X"])'
    once = _JM._rewrite_subprocess_pip_install(src)
    twice = _JM._rewrite_subprocess_pip_install(once)
    assert once == twice


def test_l2_multiline_call_with_kwargs():
    """subprocess.run(..., capture_output=True, text=True) — kwargs allowed."""
    src = '''subprocess.run(
    [sys.executable, "-m", "pip", "install", "X"],
    capture_output=True, text=True
)'''
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "subprocess.run" not in out
    assert "None" in out


def test_l2_examplecorp_loop_pattern():
    """The exact the pilot deployment 001_library_importing.ipynb pattern — rewrite the
    inner subprocess call, keep the loop + try/except structure."""
    src = '''def import_libraries():
  try:
    from slack_sdk import WebClient
  except:
    for lib in ["slack_sdk", "knockknock"]:
      try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
      except:
        print(f"Failed to install: {lib}")
    import_libraries()

import_libraries()'''
    out = _JM._rewrite_subprocess_pip_install(src)
    # Structural elements preserved
    assert "def import_libraries():" in out
    assert 'for lib in ["slack_sdk", "knockknock"]:' in out
    assert "except:" in out
    # subprocess.check_call is gone
    assert "subprocess.check_call" not in out
    # No-op marker in place
    assert "pip install stripped" in out


def test_l2_active_ranges_smoketest():
    """Sanity-check _python_active_ranges on mixed content."""
    src = '''# comment
code_line = 1
"string with # not a comment"
# another comment
end = 2'''
    ranges = _JM._python_active_ranges(src)
    # At least the code_line and end should be in active ranges
    active_text = "".join(src[s:e] for s, e in ranges)
    assert "code_line = 1" in active_text
    assert "end = 2" in active_text
    # Comment text should NOT be in active
    assert "# comment" not in active_text
    assert "# another comment" not in active_text


# ═══════════════════════════════════════════════════════════════════════
# Deep-review defect regressions (2026-07-28)
# ═══════════════════════════════════════════════════════════════════════
def test_p0_kwargs_with_nested_paren_call():
    """P0 regression: `env=os.environ.copy()` has nested `()` that broke the
    original `[^)]*?` regex → orphaned outer `)` → syntax error in output.

    Fix: paren-balance walk via _find_matching_paren finds the correct
    outer `)` regardless of nested calls inside kwargs.
    """
    src = 'subprocess.run([sys.executable, "-m", "pip", "install", "X"], env=os.environ.copy())'
    out = _JM._rewrite_subprocess_pip_install(src)
    # Must NOT leave an orphaned `)`; must fully replace the whole call.
    assert "subprocess.run" not in out
    assert out.count("(") == out.count(")"), \
        f"unbalanced parens in output:\n{out!r}"
    assert "None" in out and "pip install stripped" in out


def test_p0_kwargs_with_multiple_nested_paren_calls():
    """More adversarial: two nested paren calls in kwargs."""
    src = ('subprocess.check_call(\n'
           '    [sys.executable, "-m", "pip", "install", "X"],\n'
           '    env=os.environ.copy(),\n'
           '    cwd=Path.cwd()\n'
           ')')
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "subprocess.check_call" not in out
    assert out.count("(") == out.count(")")
    assert "None" in out


def test_p0_lambda_kwarg():
    """`preexec_fn=lambda: os.setpgrp()` — nested `()` inside a lambda."""
    src = 'subprocess.Popen([sys.executable, "-m", "pip", "install", "X"], preexec_fn=lambda: os.setpgrp())'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "subprocess.Popen" not in out
    assert out.count("(") == out.count(")")


def test_p1_scala_cell_guard():
    """P1 regression: `_apply_field_learned_swaps` may run on %scala cells in
    the child-inline path. `_python_active_ranges` treats `#` as Python
    comment (wrong for Scala where `//` is comment). Rewriter must
    short-circuit on non-Python cells."""
    src = '%scala\n// subprocess.check_call([sys.executable, "-m", "pip", "install", "X"])\nval a = 1'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src, "must not touch %scala cells"


def test_p1_sql_cell_guard():
    """Similarly for %sql cells."""
    src = '%sql\nSELECT * FROM t -- subprocess.check_call([...pip install...])'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src


def test_p1_mixed_subprocess_and_os_system_in_one_cell():
    """P1 regression: os.system pass ran on POST-substituted source with
    ranges from ORIGINAL source. Fix: collect all replacements against the
    original source, apply from end→start. Both must rewrite correctly."""
    src = '''subprocess.check_call([sys.executable, "-m", "pip", "install", "A"])
"""some docstring in between"""
os.system("pip install B")'''
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "subprocess.check_call" not in out
    assert "os.system" not in out
    assert out.count("Oracle tool modification") == 2
    # Docstring content untouched
    assert '"""some docstring in between"""' in out


def test_find_matching_paren_simple():
    src = "(hello)"
    assert _JM._find_matching_paren(src, 0) == 6


def test_find_matching_paren_nested():
    src = "foo(a, b(c, d), e)"
    # Outer opens at 3, closes at 17
    assert _JM._find_matching_paren(src, 3) == 17


def test_find_matching_paren_string_with_paren():
    """`)` inside a string literal must not be counted."""
    src = 'call("has ) inside")'
    # opens at 4, closes at 19 (real close), NOT at 10 (the ) in string)
    assert _JM._find_matching_paren(src, 4) == 19


def test_find_matching_paren_comment_with_paren():
    """`)` inside a `#` comment must not be counted."""
    src = 'call(  # trailing )\n  x, y\n)'
    close = _JM._find_matching_paren(src, 4)
    # The `)` after `\n` at end is the real close
    assert src[close] == ")"
    assert close > 20  # past the comment's fake `)`


def test_cell_is_python_default():
    """Cells with no explicit magic are treated as Python."""
    assert _JM._cell_is_python("import x")
    assert _JM._cell_is_python("dbutils.fs.ls('/foo')")
    assert _JM._cell_is_python("")   # empty → default Python


def test_cell_is_python_rejects_magics():
    for magic in ("%scala", "%sql", "%r", "%sh", "%md", "%md-sandbox", "%fs"):
        assert not _JM._cell_is_python(f"{magic}\nfoo"), magic


# ═══════════════════════ P1-2: f-string spec filter ══════════════════════
def test_l1_rejects_fstring_interpolation_specs():
    """Extractor must NOT emit `f'{lib}=={ver}'` as a literal spec — that
    would poison pip_deps.txt with a syntactically invalid pip identifier."""
    # Simulate the extractor seeing an f-string arg. The regex captures the
    # LITERAL text; if it happens to contain `{...}`, _is_static_pkg_spec
    # rejects it.
    assert not _DBX._is_static_pkg_spec("{lib}==2.14.2")
    assert not _DBX._is_static_pkg_spec("mlflow=={version}")
    assert not _DBX._is_static_pkg_spec("")
    assert not _DBX._is_static_pkg_spec("   ")


def test_l1_accepts_normal_specs():
    """Normal specs including version pins, extras, etc. must pass through."""
    for spec in ("mlflow==2.14.2", "slack_sdk", "openpyxl>=3.0",
                 "requests[security]", "package~=1.0"):
        assert _DBX._is_static_pkg_spec(spec), spec


def test_l1_subprocess_with_fstring_pkg_is_filtered():
    """subprocess.check_call([...pip install f'{lib}=={ver}'...]) — the
    literal captured is `{lib}=={ver}` (the regex doesn't consume the `f`
    prefix but gets what's inside quotes). Filter must reject it."""
    src = 'subprocess.check_call([sys.executable, "-m", "pip", "install", "{lib}=={ver}"])'
    specs = _DBX._extract_subprocess_pip_specs(src)
    assert "{lib}=={ver}" not in specs


# ══════════════════ P2-1: short loop-var false positive ══════════════════
def test_p2_short_loopvar_i_no_false_positive():
    """P2-1 regression: `for i in xs:` — 'i' is a substring of 'install',
    'pip', 'sys'. Without word-boundary checks, ANY subprocess pip call
    within 500 chars of the loop would falsely inject `xs`'s items."""
    src = '''
xs = ["a", "b", "c"]  # unrelated list
for i in xs:
    print(i)
# unrelated subprocess call
subprocess.check_call([sys.executable, "-m", "pip", "install", "actual_pkg"])
'''
    specs = _DBX._extract_subprocess_pip_specs(src)
    # Must extract only 'actual_pkg' (from the direct subprocess call), not
    # the xs list (a, b, c) — those aren't referenced in a pip install loop.
    assert "actual_pkg" in specs
    assert "a" not in specs and "b" not in specs and "c" not in specs, \
        "P2-1 regression: unrelated list injected via short-loopvar match"


def test_p2_short_loopvar_p_no_false_positive():
    """Even worse: loop var 'p' — substring of 'pip' too."""
    src = '''
xs = ["x", "y"]
for p in xs:
    print(p)
subprocess.check_call([sys.executable, "-m", "pip", "install", "real_pkg"])
'''
    specs = _DBX._extract_subprocess_pip_specs(src)
    assert "real_pkg" in specs
    assert "x" not in specs and "y" not in specs


def test_p2_valid_loop_var_still_matches():
    """Positive path — `for lib in libs:` with subprocess pip using `lib`
    must still extract the list."""
    src = '''
libs = ["A", "B"]
for lib in libs:
    subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
'''
    specs = _DBX._extract_subprocess_pip_specs(src)
    assert set(specs) == {"A", "B"}


# ══════════════════ P2-3: case-insensitive fast-path ═════════════════════
def test_p2_uppercase_pip_install_still_triggers_gate():
    """Very unusual but possible: `Pip Install X` in a string arg to
    os.system. The fast-path substring check `_lower` allows the regex
    layer to run; the regex itself is still case-sensitive (matches only
    lowercase 'pip install' in os.system arg per the OS_SYSTEM regex)."""
    src = 'os.system("pip install foo")'  # normal lowercase case
    out = _JM._rewrite_subprocess_pip_install(src)
    assert "os.system" not in out and "None" in out


def test_p2_case_insensitive_fastpath_no_false_negative():
    """Fast-path shouldn't skip cells that have 'Pip' or 'Install' in code
    (e.g. class names, log messages). Regex still handles them correctly
    (no match if lowercase pattern not present)."""
    src = 'my_pipeline = Pipeline()  # not a pip install'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src


# ══════════════════ P2-4: pip.main(args=[...]) kwarg form ════════════════
def test_p2_pip_main_positional():
    """Baseline: positional form."""
    src = 'pip.main(["install", "black"])'
    specs = _DBX._extract_pip_main_specs(src)
    assert "black" in specs


def test_p2_pip_main_kwarg_args():
    """P2-4 fix: `pip.main(args=['install', 'X'])` kwarg form."""
    src = 'pip.main(args=["install", "black", "flake8"])'
    specs = _DBX._extract_pip_main_specs(src)
    assert "black" in specs and "flake8" in specs


# ══════════════════ P2-7: coverage gaps ══════════════════════════════════
def test_p2_l2_raw_string_pip_install_skipped():
    """A raw string `r"pip install X"` (no interpolation) — regex CAN match
    inside a raw string, but the active-position scanner correctly identifies
    string literals regardless of `r` prefix (prefix is normal code, string
    starts at the quote)."""
    src = 'msg = r"call pattern: pip install X"'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src


def test_p2_l2_byte_string_pip_install_skipped():
    """Same for byte strings — prefix `b` is normal code, string starts at quote."""
    src = 'buf = b"pip install foo\\n"'
    out = _JM._rewrite_subprocess_pip_install(src)
    assert out == src


def test_p2_l2_cell_starts_with_docstring():
    """Cell whose position 0 is a triple-quoted docstring. Active-position
    scanner must handle this — first active range starts AFTER docstring
    close, not at 0."""
    src = '''"""module docstring
mentions subprocess.check_call([sys.executable, "-m", "pip", "install", "X"])
"""
subprocess.check_call([sys.executable, "-m", "pip", "install", "real_pkg"])
'''
    out = _JM._rewrite_subprocess_pip_install(src)
    # Docstring content untouched (subprocess reference inside is inactive)
    assert 'mentions subprocess.check_call' in out
    # But the REAL call after the docstring IS rewritten
    lines = out.split("\n")
    real_line_after_docstring = [l for l in lines if l.startswith("subprocess.")]
    assert not real_line_after_docstring, \
        "real subprocess call must be rewritten to None comment"


def test_p2_l1_variable_loop_with_expression_iterable_NOT_matched():
    """`for lib in sorted(libs):` — extended-form iterable. Currently NOT
    matched (documented gap; sacrificing recall for zero false positives).
    Test locks this behavior — if we later add expression-iterable support,
    update this test."""
    src = '''
libs = ["A", "B"]
for lib in sorted(libs):
    subprocess.check_call([sys.executable, "-m", "pip", "install", lib])
'''
    specs = _DBX._extract_subprocess_pip_specs(src)
    # Documented gap: 'A'/'B' NOT extracted from `sorted(libs)` — the direct
    # subprocess call inside the loop has `lib` variable, and there's no
    # `for lib in libs:` (bare form) shape that matches _for_re.
    assert "A" not in specs and "B" not in specs


# ═══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    fns = [(n, f) for n, f in globals().items()
           if n.startswith("test_") and callable(f)]
    failures = []
    for name, fn in fns:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures.append((name, str(e)))
            print(f"  ✗ {name}\n      {e}")
        except Exception as e:
            failures.append((name, f"{type(e).__name__}: {e}"))
            print(f"  ✗ {name}\n      {type(e).__name__}: {e}")
    print("=" * 60)
    if failures:
        print(f"✗ {len(failures)} FAILURE(S) out of {len(fns)}")
        sys.exit(1)
    print(f"✓ ALL {len(fns)} TESTS PASSED")
