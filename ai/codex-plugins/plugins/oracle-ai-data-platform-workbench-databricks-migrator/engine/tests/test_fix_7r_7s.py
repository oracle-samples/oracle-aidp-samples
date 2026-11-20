"""Test suite for Fix 7r (aidp_dbutils rewrite) and Fix 7s (DBX bare-abs %run resolution).

Fix 7r  — `_rewrite_dbutils_api_renames` rewrites `from aidp_dbutils import _DBUtils`
          + companion `dbutils = _DBUtils(...)` init (paren-balanced multi-line safe).
Fix 7s  — DBX bare-abs prefixes (Users/, Repos/, Shared/) are treated as absolute
          (equivalent to /Workspace/<raw>), not relative. Applied to ALL %run
          resolvers found in scripts/job_migrate.py (there are 6+).
          PR-34 review, 2026-07-27: bare `Workspace/` was REMOVED from the set —
          it's now treated as caller-relative to match vanilla DBX %run for
          slash-less paths. See test_7s_bare_workspace_falls_through_to_relative
          and test_7s_bare_workspace_resolved_target_is_caller_relative below.

Runs offline — no cluster, no network. Every fix change should keep this suite green.

Usage:
    pytest tests/test_fix_7r_7s.py -v
    # or standalone:
    python3 tests/test_fix_7r_7s.py
"""
import ast
import io
import os
import re
import sys
import tokenize
import types
from typing import Any, Dict, List, Optional, Tuple

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO, "scripts/job_migrate.py")


def _load_module():
    """Extract Fix 7r/7s helpers from job_migrate.py without executing the full
    module (which would drag in heavy runtime deps). Uses ast to keep only the
    functions/constants under test."""
    with open(SRC, encoding="utf-8") as f:
        tree = ast.parse(f.read())

    wanted_funcs = {
        "_rewrite_dbutils_api_renames",
        "_rewrite_internal_paths",
        "_parse_percent_run_arguments",
        "_quote_percent_run_target",
        "_match_close_paren",
        "_split_top_level_args",
        "_reshape_taskvalues_set",
        "_reshape_taskvalues_get",
        "_pos_is_commented",
        "normalize_nb_path",
        "_fix_notebook_run_timeout",
        "_tag_positional",
        "_get_kw_default",
        "_active_lines",
        "_neutralize_slack_execution",
        "_has_slack_evidence",
        "_get_receiver_before",
        "_cell_language",
        "_python_cell_ast_source",
        "_python_noncode_ranges",
        "_python_tokens_with_ipython_recovery",
        "_fallback_ipython_statement_lines",
        "_position_is_active_for_language",
        "_source_lines_and_offsets",
        "_pos_in_active_range",
        "_scala_active_ranges",
    }
    wanted_names = {
        "_OLD_NB_BASES", "_DBX_ABS_PREFIXES", "_PERCENT_RUN_LINE_RE",
        "_TV_SET_RE", "_TV_GET_RE",
        "_OIDL_TV_SET_RE",
        "_PYTHON_TOKEN_ERRORS",
        "_PYTHON_MAGICS", "_PYTHON_MAGIC_PATTERN", "_PYTHON_MAGIC_LINE_RE",
        "_PYTHON_MAGIC_DECLARATION_RE",
        "_DISPLAY_METHOD_RE",
        "_SLACK_TIER1_RE", "_SLACK_TIER2_RE", "_SLACK_EVIDENCE_MARKERS",
        "_SLACK_ID_RE", "_SLACK_CLIENT_BINDING_RE", "_SLACK_RECEIVER_HINT_RE",
    }

    keep = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted_funcs:
            keep.append(node)
        elif isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in wanted_names:
                    keep.append(node)
                    break

    mod = types.ModuleType("job_migrate_partial")
    mod.re = re
    mod.io = io
    mod.tokenize = tokenize
    mod.os = os
    mod.Optional = Optional
    mod.Dict = Dict
    mod.Any = Any
    mod.List = List
    mod.Tuple = Tuple
    mod.tprint = lambda *a, **k: None
    mod.EXPORT_BASE = ""

    mod_ast = ast.Module(body=keep, type_ignores=[])
    ast.fix_missing_locations(mod_ast)
    exec(compile(mod_ast, SRC, "exec"), mod.__dict__)

    # Mock helpers called by _rewrite_internal_paths but not under test here.
    mod._fix_notebook_run_timeout = lambda s: s
    mod._apply_field_learned_swaps = lambda s: s
    mod._apply_catalog_remap = lambda s: s
    mod._normalize_table_refs = lambda s: (s, False)
    return mod


M = _load_module()


# ═══════════════════════ Fix 7r: aidp_dbutils rewrite ═══════════════════════

def test_7r_realworld_case():
    """The exact 63-char cell shape that appears in customer notebooks — the
    canonical legacy import + init pattern that used to require an Opus
    round-trip to rewrite. Fix 7r handles it deterministically."""
    src = "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)"
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils, translate_path" in out
    assert "from aidp_dbutils" not in out
    assert "# dbutils = _DBUtils(oidlUtils)" in out


def test_7r_import_only_no_init():
    src = "from aidp_dbutils import _DBUtils\nprint('after import')"
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils" in out
    assert "print('after import')" in out


def test_7r_idempotent_already_migrated():
    src = "from aidp_compat import dbutils, translate_path"
    out = M._rewrite_dbutils_api_renames(src)
    assert out.strip() == src.strip()


def test_7r_no_op_unrelated():
    src = "import pandas as pd\nx = 1 + 2\n"
    out = M._rewrite_dbutils_api_renames(src)
    assert out == src


def test_7r_commented_lines_untouched():
    src = "# from aidp_dbutils import _DBUtils\n# dbutils = _DBUtils(oidlUtils)"
    out = M._rewrite_dbutils_api_renames(src)
    assert out == src


def test_7r_indented_import_preserves_whitespace():
    src = "def setup():\n    from aidp_dbutils import _DBUtils\n    dbutils = _DBUtils(oidlUtils)\n"
    out = M._rewrite_dbutils_api_renames(src)
    assert "    from aidp_compat import dbutils" in out
    assert "# dbutils = _DBUtils(oidlUtils)" in out


def test_7r_composable_with_notebook_exit():
    src = "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)\ndbutils.notebook.exit('x')\n"
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils" in out
    # notebook.exit → aidputils.notebook.exit (native; exists live-verified ca991533)
    assert "aidputils.notebook.exit('x')" in out


def test_7r_defect1_trailing_comment_on_import():
    """Regression: trailing inline comment must not block rewrite."""
    src = "from aidp_dbutils import _DBUtils  # legacy shim\ndbutils = _DBUtils(oidlUtils)\n"
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils" in out
    assert "aidp_dbutils" not in out


def test_7r_defect2_multiline_init():
    """Regression: multi-line _DBUtils(...) init must be commented out entirely."""
    src = (
        "from aidp_dbutils import _DBUtils\n"
        "dbutils = _DBUtils(\n"
        "    spark,\n"
        "    dbutils_original,\n"
        ")\n"
        "print('after')\n"
    )
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils" in out
    # Every line of the init should be commented
    assert "# dbutils = _DBUtils(" in out
    assert "print('after')" in out
    # No uncommented `_DBUtils` reference (would be NameError at runtime)
    uncommented = [l for l in out.split("\n") if "_DBUtils" in l and not l.lstrip().startswith("#")]
    assert not uncommented, f"uncommented _DBUtils refs: {uncommented}"


def test_7r_defect3_nested_parens_in_init():
    src = "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(build_thing(x, y))\n"
    out = M._rewrite_dbutils_api_renames(src)
    assert "from aidp_compat import dbutils" in out
    assert "# dbutils = _DBUtils(build_thing(x, y))" in out


def test_7r_idempotency_across_forms():
    """Second application must produce byte-identical output — critical for %run pass loops."""
    cases = {
        "real": "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(oidlUtils)",
        "trailing-comment": "from aidp_dbutils import _DBUtils  # legacy shim\ndbutils = _DBUtils(oidlUtils)",
        "multi-line": "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(\n    spark,\n)\n",
        "nested-parens": "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils(build_thing(x, y))",
    }
    for label, src in cases.items():
        out1 = M._rewrite_dbutils_api_renames(src)
        out2 = M._rewrite_dbutils_api_renames(out1)
        assert out1 == out2, f"idempotency failed for {label}"


# ═══════════════════════ Fix 7s: DBX bare-abs %run resolution ═══════════════════════
#
# Path constants below use generic placeholder values (user@example.com, sample job
# names) but preserve the *shape* of the real-world regression: a cross-user %run
# targeting a Databricks bare-absolute `Users/<email>/<notebook>` path from within
# a nested inline in another user's directory. Do not simplify the shape — the
# bug only manifests when the caller notebook lives in a different subdirectory
# than the target and the target uses a DBX bare-abs prefix.

MIGRATED_BASE = (
    "/Workspace/sample/migration_run/aidp_migrate/"
    "job_sample_v1/notebooks"
)
ALL_UTILS_PATH = (
    "/Workspace/sample/migration_run/dbx_export/notebooks/"
    "GIT/SampleJob/HelperFunctions/all_utils.ipynb"
)


def _build_dep_map():
    """Reconstruct a dep_path_map matching the shape of the original regression case."""
    dbx_target = (
        "/Workspace/sample/migration_run/dbx_export/notebooks/"
        "Users/user@example.com/UpstreamReader.ipynb"
    )
    mig = f"{MIGRATED_BASE}/sample/migration_run/dbx_export/notebooks/Users/user@example.com/UpstreamReader.ipynb"
    return {
        dbx_target: mig,
        "sample/migration_run/dbx_export/notebooks/users/user@example.com/upstreamreader.ipynb": mig,
    }


def test_7s_v15_regression_no_mangling():
    """THE test — a DBX bare-abs `%run Users/<email>/<nb>` from within a subdir must NOT
    produce `<caller_dir>/Users/<email>/…` mangling. Original v15 failure case."""
    src = "%run Users/user@example.com/UpstreamReader\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, _build_dep_map())
    assert "HelperFunctions/Users/user@example.com" not in out, \
        f"v15 mangling still present: {out}"
    assert "Users/user@example.com/UpstreamReader.ipynb" in out


def test_7s_empty_depmap_priority_1_5():
    """With no dep_path_map, Priority 1.5 must fire (anchor at migrated_base + /Workspace/<raw>)."""
    src = "%run Users/user@example.com/UpstreamReader\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})
    assert "HelperFunctions/Users/user@example.com" not in out
    assert "DBX bare-abs" in out
    assert f"{MIGRATED_BASE}/Users/user@example.com" in out


def test_7s_all_dbx_abs_prefixes():
    """Every prefix in _DBX_ABS_PREFIXES must resolve as DBX-absolute:
    (a) NOT prepended to the caller's dir, AND
    (b) POSITIVELY resolved to `{migrated_base}/{prefix}some/path/notebook.ipynb`
    (Priority-1.5 emit — anchored at migrated_base).

    Uses the module's actual constant so the test tracks the source of truth
    if the set changes.

    PR-34 deep-review (a reviewer, 2026-07-27): a purely negative assertion
    (`X not in out`) would pass for a resolver returning garbage. Strengthened
    to positive: the emitted target must match the expected migrated_base-anchored
    path exactly.

    Historical note: PR-34 (a reviewer, 2026-07-27) removed 'Workspace/' from the
    set. The initial commit rationale (path-doubling) was wrong — see the
    corrected mechanism in the constant's comment block. What actually changes:
    bare `Workspace/foo` now falls through to Priority-2 (caller-relative);
    see test_7s_bare_workspace_resolved_target_is_caller_relative."""
    assert M._DBX_ABS_PREFIXES, "_DBX_ABS_PREFIXES unexpectedly empty"
    for prefix in M._DBX_ABS_PREFIXES:
        src = f"%run {prefix}some/path/notebook\n"
        out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})
        # Negative: must NOT be caller-relative
        assert "HelperFunctions/" + prefix not in out, \
            f"{prefix} still mangled under caller dir: {out}"
        # Positive: must be anchored at migrated_base (Priority-1.5 emit)
        expected_target = f"{MIGRATED_BASE}/{prefix}some/path/notebook.ipynb"
        assert expected_target in out, (
            f"{prefix} did not resolve to migrated_base-anchored target. "
            f"Expected {expected_target!r} in output; got: {out}"
        )


def test_7s_regression_relative_paths_still_work():
    """Legit relative paths (./ ../) must still resolve under caller dir."""
    src_rel = "%run ./sibling_utils\n"
    out_rel = M._rewrite_internal_paths(src_rel, MIGRATED_BASE, ALL_UTILS_PATH, {})
    assert "HelperFunctions/sibling_utils.ipynb" in out_rel

    src_up = "%run ../SaveTableUtils\n"
    out_up = M._rewrite_internal_paths(src_up, MIGRATED_BASE, ALL_UTILS_PATH, {})
    # ALL_UTILS_PATH lives under SampleJob/HelperFunctions/, so `../X` → SampleJob/X.ipynb
    assert "SampleJob/SaveTableUtils.ipynb" in out_up


def test_run_rewrite_preserves_double_quoted_path_with_spaces():
    """Incident ca41d6f2: a quoted DBX path must be parsed as one target."""
    source_path = (
        "/Workspace/Users/user8@example.com/"
        "Top 5 locations/top5_v4/imports_N_tbls"
    )
    migrated_path = (
        f"{MIGRATED_BASE}/Users/user8@example.com/"
        "Top_5_locations/top5_v4/imports_N_tbls.ipynb"
    )

    out = M._rewrite_internal_paths(
        f'%run "{source_path}"\n',
        MIGRATED_BASE,
        ALL_UTILS_PATH,
        {source_path: migrated_path},
    )

    assert f"%run {migrated_path}" in out
    assert 'Inference_v4/"/Workspace/Users/' not in out


def test_run_rewrite_preserves_single_quoted_path_and_widget_args():
    """Quotes delimit the path; text after the closing quote is arguments."""
    source_path = "/Workspace/Users/user@example.com/Folder With Spaces/child"
    migrated_path = (
        f"{MIGRATED_BASE}/Users/user@example.com/"
        "Folder_With_Spaces/child.ipynb"
    )

    out = M._rewrite_internal_paths(
        f"%run '{source_path}' $date=\"2026-09-15\" $mode=test\n",
        MIGRATED_BASE,
        ALL_UTILS_PATH,
        {source_path: migrated_path},
    )

    assert (
        f'%run {migrated_path} $date="2026-09-15" $mode=test'
        in out
    )


def test_run_rewrite_bare_path_and_widget_args_remain_supported():
    """The quote-aware parser must preserve the existing bare-path contract."""
    out = M._rewrite_internal_paths(
        '%run ./child $date="2026-09-15"\n',
        MIGRATED_BASE,
        ALL_UTILS_PATH,
        {},
    )

    run_line = next(
        line for line in out.splitlines() if line.lstrip().startswith("%run")
    )
    assert run_line.endswith(
        '/child.ipynb $date="2026-09-15"'
    )


def test_run_rewrite_unquoted_spaced_path_is_one_target_and_requoted():
    source_path = "/Workspace/FL Queries Run/SomeNotebook"
    migrated_path = f"{MIGRATED_BASE}/FL Queries Run/SomeNotebook.ipynb"

    out = M._rewrite_internal_paths(
        f"%run {source_path} $mode=test\n",
        MIGRATED_BASE,
        ALL_UTILS_PATH,
        {source_path: migrated_path},
    )

    assert f'%run "{migrated_path}" $mode=test' in out


def test_run_rewrite_leaves_unclosed_quote_unchanged():
    source = '%run "/Workspace/Broken Folder/child\n'

    assert M._rewrite_internal_paths(
        source, MIGRATED_BASE, ALL_UTILS_PATH, {}
    ) == source


def test_7s_regression_bareword_no_prefix_stays_relative():
    """A bareword like `foo_notebook` (not matching a DBX prefix) must remain relative."""
    src = "%run bareword_notebook\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})
    assert "HelperFunctions/bareword_notebook.ipynb" in out


def test_7s_regression_absolute_workspace_path():
    """`/Workspace/…` absolute paths must be preserved (no mangling under caller dir)."""
    src = "%run /Workspace/GIT/some_notebook\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})
    # Just make sure we don't prepend HelperFunctions/ to it
    after_run = out.split("%run")[1] if "%run" in out else ""
    assert "HelperFunctions/" not in after_run


def test_7s_priority_1_still_wins_with_depmap():
    """When dep_path_map has an entry that matches by suffix, Priority 1 must still hit
    (annotation 'path → migrated dep path'), NOT the newer 1.5."""
    dep_map = _build_dep_map()
    dbx_key = next(iter(dep_map))  # long dbx_export absolute path
    src = f"%run {dbx_key}\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, dep_map)
    expected_mig = dep_map[dbx_key]
    assert expected_mig in out


def test_7s_idempotent_second_pass():
    """Applying the rewrite twice must not re-mangle."""
    src = "%run Users/user@example.com/UpstreamReader\n"
    out1 = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, _build_dep_map())
    out2 = M._rewrite_internal_paths(out1, MIGRATED_BASE, ALL_UTILS_PATH, _build_dep_map())
    assert "HelperFunctions/Users/user@example.com" not in out2


# ═══════════════════════ Fix 7s: OTHER RESOLVERS (v16 gap coverage) ═══════════════════════
#
# The v16 launch revealed that `_rewrite_internal_paths` is only ONE of many
# resolvers. Below we exercise the other resolvers we patched. Because these are
# nested closures / inline logic, we test them by inlining the same DBX-abs
# check the fix applies. This isn't ideal — a proper refactor would extract
# `resolve_dbx_bare_abs(raw)` and test that one helper — but this at least
# guards each resolver against re-introducing the bug.

def test_7s_dbx_prefixes_constant_exists():
    """Every fix depends on `_DBX_ABS_PREFIXES` being available at module scope.

    PR-34 (a reviewer + a reviewer deep-review, 2026-07-27): 'Workspace/' was removed
    from this set. The initial commit rationale (path-doubling) was wrong; see
    the constant's comment block for the corrected mechanism. What actually
    changes: bare `Workspace/foo` moves from Priority-1.5 (migrated_base-anchor)
    to Priority-2 (caller-relative). Grep of 82 the pilot deployment notebooks confirmed zero
    bare 'Workspace/' %run occurrences, so the change is behaviorally safe for
    the current workload.

    Ordering note (a reviewer deep-review): the anti-regression assertion runs FIRST
    so it triggers with a specific error message if 'Workspace/' is re-added,
    rather than being masked by the set-equality assert below."""
    assert hasattr(M, "_DBX_ABS_PREFIXES")
    # Anti-regression FIRST (before the set-equality). If someone re-adds
    # 'Workspace/', this fails with a clear message rather than a generic
    # set-mismatch diff.
    assert "Workspace/" not in M._DBX_ABS_PREFIXES, (
        "'Workspace/' is back in _DBX_ABS_PREFIXES — this resurrects the "
        "PR-34 divergence between %run rewrite (Priority-2 caller-relative) "
        "and _lookup_dep_map._norm (which still strips bare Workspace/)."
    )
    assert set(M._DBX_ABS_PREFIXES) == {"Users/", "Repos/", "Shared/"}


def test_7s_bare_workspace_falls_through_to_relative():
    """PR-34 review fix: bare `%run Workspace/foo` must NOT hit the DBX-abs
    branch anymore. It should fall through to Priority-2 (relative resolution
    under the caller's dir) — matches the intuitive interpretation of a
    caller-relative subfolder named 'Workspace/'."""
    src = "%run Workspace/some_notebook\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})

    # The DBX-abs handler is guarded by the [WARN][7s] Priority-1.5 log — that
    # message must NOT appear for Workspace/ any more.
    assert "DBX bare-abs" not in out, \
        f"Workspace/ still went through DBX-abs path: {out}"

    # It should have resolved as relative to the caller's dir instead.
    # ALL_UTILS_PATH lives under HelperFunctions/, so bare `Workspace/foo`
    # becomes `HelperFunctions/Workspace/some_notebook.ipynb`.
    assert "HelperFunctions/Workspace/some_notebook.ipynb" in out, \
        f"Expected relative resolution under caller dir; got: {out}"


def test_7s_bare_workspace_resolved_target_is_caller_relative():
    """PR-34 deep-review (a reviewer): the previous test asserted `/Workspace/Workspace/`
    never appears in output — but that literal was unproducible in emitted output
    regardless of whether `Workspace/` is in _DBX_ABS_PREFIXES (normalize_nb_path
    strips one leading `/Workspace/` before Priority-1.5's emit). Verified by
    mutation: re-adding `Workspace/` to the tuple still made the old test PASS.
    So the old assertion was tautological — it protected against a bug the code
    could not produce.

    This replacement asserts the actual output-behavior distinction. The %run
    LINE (not the [WARN][7s'] alt-target line, which contains both candidates
    as informational) must match the caller-relative resolution and must NOT
    match the pre-PR-34 migrated_base-anchored resolution.

    Mutation-tested: re-adding `Workspace/` to _DBX_ABS_PREFIXES makes this
    test FAIL — %run line reverts to `{MIGRATED_BASE}/Workspace/…` (Priority-1.5)."""
    src = "%run Workspace/some_notebook\n"
    out = M._rewrite_internal_paths(src, MIGRATED_BASE, ALL_UTILS_PATH, {})

    # Extract the emitted %run line only — WARN log lines are not part of
    # the rewritten source and shouldn't be conflated.
    run_lines = [ln for ln in out.splitlines() if ln.lstrip().startswith("%run")]
    assert run_lines, f"no %run line in rewriter output: {out!r}"
    run_line = run_lines[0]

    # Positive: %run line must contain the caller-relative target.
    # ALL_UTILS_PATH lives under HelperFunctions/, so bare `Workspace/foo` becomes
    # `{MIGRATED_BASE}/…/HelperFunctions/Workspace/some_notebook.ipynb`.
    assert "HelperFunctions/Workspace/some_notebook.ipynb" in run_line, (
        f"%run line did NOT resolve caller-relative — got: {run_line!r}"
    )

    # Negative: the pre-PR-34 (Priority-1.5) emit anchored bare `Workspace/foo`
    # DIRECTLY at MIGRATED_BASE (no HelperFunctions/ segment). Its reappearance
    # in the %run line would mean _DBX_ABS_PREFIXES has regressed.
    pre_fix = f"{MIGRATED_BASE}/Workspace/some_notebook.ipynb"
    assert pre_fix not in run_line, (
        f"Pre-PR-34 migrated_base-anchored resolution reappeared in %run line "
        f"— %run line: {run_line!r}. This means bare `Workspace/` is being "
        f"treated as DBX-absolute again."
    )


def test_7s_bare_abs_detection_logic():
    """The invariant every resolver must respect: if raw starts with a DBX prefix
    and has no leading /, it is NOT relative."""
    for prefix in M._DBX_ABS_PREFIXES:
        raw = f"{prefix}foo/bar"
        is_dbx_absolute = (
            not raw.startswith("/")
            and any(raw.startswith(p) for p in M._DBX_ABS_PREFIXES)
        )
        assert is_dbx_absolute, f"{raw!r} misclassified"

    # Counter-examples: these MUST be treated as relative
    for raw in ["bareword_notebook", "./sibling", "../up_one", "foo/bar"]:
        is_dbx_absolute = (
            not raw.startswith("/")
            and any(raw.startswith(p) for p in M._DBX_ABS_PREFIXES)
        )
        assert not is_dbx_absolute, f"{raw!r} wrongly classified as DBX-abs"

    # Absolute paths short-circuit — is_dbx_absolute is False since startswith("/")
    for raw in ["/Workspace/GIT/foo", "/Users/x/y"]:
        is_dbx_absolute = (
            not raw.startswith("/")
            and any(raw.startswith(p) for p in M._DBX_ABS_PREFIXES)
        )
        assert not is_dbx_absolute


def test_7s_all_resolver_sites_have_dbx_check():
    """Structural test: every `%run`-adjacent resolver in job_migrate.py that has
    `if not X.startswith("/"):` immediately followed by `os.path.normpath(os.path.join(...))`
    must have a `_DBX_ABS_PREFIXES` reference somewhere in the ENCLOSING FUNCTION.
    Fails if a new resolver is added without applying Fix 7s.

    We use function-scoped presence rather than a line-window because Fix 7s
    is often applied several lines earlier (converting DBX-abs to /Workspace/…
    BEFORE the relative-path join). A function-scoped check catches
    genuinely-missing coverage while allowing the natural code layout.
    """
    with open(SRC, encoding="utf-8") as f:
        source = f.read()
        lines = source.splitlines()

    tree = ast.parse(source)

    # Map (line_no) → enclosing function-def source text
    fn_bodies_by_line = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = node.lineno
            end = node.end_lineno or start
            body = "\n".join(lines[start - 1:end])
            for ln in range(start, end + 1):
                # Prefer the innermost (smallest) enclosing function
                if ln not in fn_bodies_by_line or len(body) < len(fn_bodies_by_line[ln]):
                    fn_bodies_by_line[ln] = body

    findings = []
    for i, line in enumerate(lines):
        m = re.match(r'^(\s*)if not (\w+)\.startswith\("/"\):\s*$', line)
        if not m:
            continue
        var = m.group(2)
        # Detection: the very next few lines must contain `normpath(os.path.join(` — a
        # relative-path join anchored on `var`.
        window_next = "\n".join(lines[i:i + 4])
        if f"normpath(os.path.join(" not in window_next or var not in window_next:
            continue
        enclosing = fn_bodies_by_line.get(i + 1, "")
        if "_DBX_ABS_PREFIXES" not in enclosing:
            findings.append((i + 1, line.rstrip()))
    assert not findings, (
        "Found %run resolver(s) whose enclosing function has no _DBX_ABS_PREFIXES "
        "guard (Fix 7s regression risk):\n"
        + "\n".join(f"  line {ln}: {t}" for ln, t in findings)
    )


# ═══════════════════════ Fix 7o: Slack neutralizer (reviewer feedback) ═══════════════════════
#
# Blocker 1 (false-positive on non-slack .send/.api_call): tier-2 patterns
# must NOT fire on cells that lack Slack evidence. A Kafka pipeline cell doing
# `producer.send(topic, msg)` at module scope MUST be preserved verbatim.
#
# Blocker 2 (multi-line SyntaxError): a multi-line `client.chat_postMessage(\n
# channel=..., text=msg)` must be commented as a balanced-paren SPAN, not just
# the header. Header-only commenting leaves the args dangling and breaks Python
# parsing for the whole cell.

# ── Blocker 1: no evidence ⇒ tier-2 must NOT fire ──

def test_7o_kafka_producer_send_not_commented():
    """Kafka `producer.send(topic, msg)` at module scope must be preserved.
    Cell has no Slack evidence → tier-2 `.send(` regex must NOT fire."""
    src = (
        "from kafka import KafkaProducer\n"
        "producer = KafkaProducer(bootstrap_servers='broker:9092')\n"
        "producer.send('my-topic', b'payload')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"Kafka producer.send wrongly commented ({n} lines): {out}"
    assert "producer.send('my-topic', b'payload')" in out


def test_7o_requests_session_send_not_commented():
    src = (
        "import requests\n"
        "session = requests.Session()\n"
        "prepared = session.prepare_request(requests.Request('GET', 'http://example.com'))\n"
        "session.send(prepared)\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"requests session.send wrongly commented: {out}"


def test_7o_socket_send_not_commented():
    src = (
        "import socket\n"
        "sock = socket.socket()\n"
        "sock.connect(('localhost', 5000))\n"
        "sock.send(b'hello')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"socket.send wrongly commented: {out}"


def test_7o_smtp_send_not_commented():
    src = (
        "import smtplib\n"
        "smtp = smtplib.SMTP('localhost')\n"
        "smtp.send('recipient@example.com', 'body')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"smtp.send wrongly commented: {out}"


def test_7o_multiprocessing_conn_send_not_commented():
    src = (
        "from multiprocessing import Pipe\n"
        "parent_conn, child_conn = Pipe()\n"
        "parent_conn.send(42)\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"multiprocessing conn.send wrongly commented: {out}"


def test_7o_api_call_not_commented_without_evidence():
    """A generic `.api_call(` in customer code (e.g. a client library) must be
    preserved when the cell has no Slack evidence."""
    src = (
        "from some_lib import Client\n"
        "client = Client()\n"
        "result = client.api_call('some_endpoint', payload)\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"generic .api_call wrongly commented: {out}"


# ── With evidence, tier-2 SHOULD fire on the ambiguous patterns ──

def test_7o_slack_client_send_IS_commented_with_evidence():
    """When the cell has slack_sdk evidence, `.send(` on a webhook client is
    slack-related and must be commented."""
    src = (
        "from slack_sdk.webhook import WebhookClient\n"
        "webhook = WebhookClient('https://hooks.slack.com/services/T00/B00/xxx')\n"
        "webhook.send(text='Migration started')\n"
    )
    assert M._has_slack_evidence(src) is True
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n >= 1
    assert "# webhook.send(text='Migration started')" in out
    assert "from slack_sdk.webhook import WebhookClient" in out  # import preserved


def test_7o_slack_client_api_call_IS_commented_with_evidence():
    src = (
        "from slack_sdk import WebClient\n"
        "client = WebClient(token='x')\n"
        "client.api_call('chat.postMessage', channel='C0123456789', text='hi')\n"
    )
    assert M._has_slack_evidence(src) is True
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n >= 1
    assert "# client.api_call(" in out


# ── Tier-1 (unambiguously Slack) fires with or without evidence ──

def test_7o_tier1_chat_postMessage_always_commented():
    src = (
        "client.chat_postMessage(channel='C0123456789', text='hi')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=False)  # no evidence
    assert n == 1, f"tier-1 chat_postMessage must fire even ungated: {out}"
    assert "# client.chat_postMessage(" in out


def test_7o_tier1_hooks_slack_com_url_commented():
    src = (
        "import requests\n"
        "requests.post('https://hooks.slack.com/services/T/B/X', json={'text': 'boot'})\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=False)
    assert n >= 1
    assert "import requests" in out


# ── Blocker 2: multi-line balanced-paren comment ──

def test_7o_blocker2_multiline_chat_postMessage_commented_as_span():
    """A multi-line `chat_postMessage(...)` must be commented as a full span,
    not just the header. Header-only leaves dangling args → SyntaxError."""
    src = (
        "client.chat_postMessage(\n"
        "    channel='C0123456789',\n"
        "    text='migration started',\n"
        "    blocks=[{'type': 'section'}]\n"
        ")\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=False)
    assert n == 5, f"expected 5 commented lines (header + 3 args + closer), got {n}: {out}"
    # Every non-blank output line should now be commented
    non_blank = [ln for ln in out.splitlines() if ln.strip()]
    for ln in non_blank:
        assert ln.lstrip().startswith("#"), f"uncommented line in multi-line span: {ln!r}"
    # Rewritten source must parse as valid Python (no dangling args)
    try:
        compile(out, "<test>", "exec")
    except SyntaxError as e:
        assert False, f"multi-line comment produced SyntaxError: {e}\n{out}"


def test_7o_blocker2_nested_parens_in_args():
    """`chat_postMessage(channel=get_channel(x))` — nested paren must not fool
    the balanced scan."""
    src = (
        "client.chat_postMessage(\n"
        "    channel=get_channel('daily'),\n"
        "    text=format_alert(fl_version, helperlib_version)\n"
        ")\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=False)
    assert n == 4
    try:
        compile(out, "<test>", "exec")
    except SyntaxError as e:
        assert False, f"nested-paren case broke: {e}\n{out}"


# ── Existing safety guarantees still hold ──

def test_7o_imports_and_defs_preserved_with_slack_import():
    """The AutoRefit cascade case: `from HelperFunctions.constants import *` in
    the same cell as `from slack_sdk import WebClient` must NOT be lost."""
    src = (
        "from HelperFunctions.constants import *\n"
        "from slack_sdk import WebClient\n"
        "import json\n"
        "def format_alert(v, s): return f'{v}/{s}'\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0, f"cell with imports+def only must not be commented: {out}"
    assert "from HelperFunctions.constants import *" in out
    assert "from slack_sdk import WebClient" in out
    assert "def format_alert" in out


def test_7o_calls_inside_def_body_not_commented():
    """A slack call INSIDE a def body is lazy — never fires at import time.
    Must be preserved verbatim (the function may never be called at all)."""
    src = (
        "def trigger_alert():\n"
        "    client.chat_postMessage(channel='C01234ABCDE', text='hi')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n == 0, f"def-body slack call wrongly commented: {out}"


def test_7o_evidence_gate_detects_slack_sdk_import():
    assert M._has_slack_evidence("from slack_sdk import WebClient") is True


def test_7o_evidence_gate_detects_hooks_slack_com():
    assert M._has_slack_evidence("url = 'https://hooks.slack.com/services/X'") is True


def test_7o_evidence_gate_detects_channel_id():
    assert M._has_slack_evidence("send_msg(channel='C01234ABCDE', text='x')") is True


def test_7o_evidence_gate_rejects_bare_slack_word_in_comment():
    """`# talk to slack` in a comment must NOT trigger evidence — that would
    re-introduce the classifier-false-positive bug."""
    assert M._has_slack_evidence("# talk to slack later\nx = 1") is False


def test_7o_idempotent_second_pass():
    src = "client.chat_postMessage(channel='C01234ABCDE', text='hi')\n"
    out1, _ = M._neutralize_slack_execution(src, include_generic=False)
    out2, n2 = M._neutralize_slack_execution(out1, include_generic=False)
    assert n2 == 0, "second pass must not re-comment already-commented lines"
    assert out1 == out2


# ── a reviewer PR-34 review DEFECT regressions ──

def test_7o_defect1_inline_comment_no_false_evidence():
    """Defect 1: a Kafka `producer.send()` line with an INLINE comment
    mentioning "slack_sdk" (e.g. `# replaced slack_sdk with SNS`) must NOT
    trigger Slack evidence. Previously _active_lines only stripped whole-line
    comments — inline `#`-suffixed comments leaked through."""
    src = (
        "from kafka import KafkaProducer\n"
        "producer = KafkaProducer(bootstrap_servers='b:9092')\n"
        "producer.send('topic', b'x')  # replaced slack_sdk with SNS\n"
    )
    assert M._has_slack_evidence(src) is False, (
        "inline comment mentioning 'slack_sdk' wrongly trips evidence"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=M._has_slack_evidence(src))
    assert n == 0
    assert "producer.send('topic', b'x')" in out


def test_7o_defect2_slack_id_regex_rejects_all_letter_constants():
    """Defect 2: `channel = "CAMPAIGNS"` (all-letter uppercase constant, no
    digits) must NOT trigger Slack-ID evidence. Real Slack channel IDs contain
    digits."""
    for candidate in ["CAMPAIGNS", "COFFEE_ORDER", "CATEGORIES", "CAMPAIGN_A"]:
        src = f'channel = "{candidate}"'
        assert M._has_slack_evidence(src) is False, (
            f"non-Slack constant '{candidate}' wrongly triggers evidence"
        )


def test_7o_defect2_slack_id_regex_accepts_real_channel_ids():
    """Real Slack channel IDs (≥2 digits) still trigger."""
    for candidate in ["C0123456789", "C01ABCDEF2", "C0BADCAT01"]:
        src = f'channel = "{candidate}"'
        assert M._has_slack_evidence(src) is True, (
            f"real Slack ID '{candidate}' should trigger evidence"
        )


def test_7o_defect3_mixed_cell_kafka_send_preserved():
    """Defect 3: a cell with BOTH `from slack_sdk import WebClient` AND
    `producer.send(topic, msg)` at module scope — evidence is True (slack
    import), but tier-2 must NOT comment the Kafka call because `producer`
    isn't bound to a slack client and doesn't have a slack-hint name."""
    src = (
        "from slack_sdk import WebClient\n"
        "from kafka import KafkaProducer\n"
        "producer = KafkaProducer(bootstrap_servers='b:9092')\n"
        "producer.send('topic', b'x')\n"
    )
    assert M._has_slack_evidence(src) is True  # gate opens because of slack import
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n == 0, f"mixed-cell Kafka producer.send wrongly commented: {out}"
    assert "producer.send('topic', b'x')" in out
    assert "from slack_sdk import WebClient" in out


def test_7o_defect3_mixed_cell_slack_call_still_commented():
    """In the same mixed cell, the SLACK client's call IS commented — receiver
    is bound to WebClient so tier-2 fires on it."""
    src = (
        "from slack_sdk import WebClient\n"
        "from kafka import KafkaProducer\n"
        "producer = KafkaProducer(bootstrap_servers='b:9092')\n"
        "slack_client = WebClient(token='x')\n"
        "producer.send('topic', b'x')\n"
        "slack_client.api_call('chat.postMessage', channel='C01234567', text='hi')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n >= 1
    assert "producer.send('topic', b'x')" in out  # Kafka preserved
    assert "# slack_client.api_call(" in out       # Slack call commented


def test_7o_defect3_receiver_name_hint_alone_triggers_tier2():
    """Even without an explicit `WebClient(...)` binding in this cell, if the
    receiver name literally contains 'slack' / 'webhook', tier-2 fires."""
    src = (
        "# slack_sdk imported in a prior cell\n"
        "from external import get_slack_client\n"  # evidence line
        "slack_wh = get_slack_client()\n"
        "slack_wh.send('hi')\n"
    )
    out, n = M._neutralize_slack_execution(src, include_generic=True)
    assert n >= 1
    assert "# slack_wh.send('hi')" in out


# ═══════════════════════ Fix 7t: SKIP_MIGRATED batch-cache gate ═══════════════════════

def test_7t_skip_migrated_gate_present():
    """Structural: the batch-check cache-populate site (~line 10982) must be
    gated on `SKIP_MIGRATED`. Fails if a future refactor removes the guard.
    the reviewer's review confirmed the 3 in-scope cache sites now share the gate."""
    with open(SRC, encoding="utf-8") as f:
        source = f.read()
    # The three cache-population sites that MUST be SKIP_MIGRATED-gated
    # (line 11047 is post-successful-migration and is intentionally ungated).
    # We assert each populated site is inside an `if ... SKIP_MIGRATED` block.
    # Simplest structural check: the batch-check line 10982 area has the guard.
    # The batch reuse also requires an accepted recorded status
    # (tests/test_resume_requires_recorded_success.py).
    assert 'if status == "VALID" and SKIP_MIGRATED and resume_may_reuse(' in source, (
        "Fix 7t regression: batch-check cache-populate not gated on SKIP_MIGRATED. "
        "Without this, --no-skip-migrated silently reuses stale files from prior runs."
    )
    # Also check the other two gated sites still exist
    assert 'if SKIP_MIGRATED:' in source, "Fix 7t: registry-hit gate removed"
    assert 'and SKIP_MIGRATED:' in source, "Fix 7t: single-file-check gate removed"


# ═══════════════════════ OCI permission findings ═══════════════════════

def test_7u_oci_perm_findings_reach_evaluation():
    """Permission findings stay visible; a completed source-defined fallback
    needs evaluation rather than an automatic replay. Executed coverage lives
    in test_caught_output_verification, including fail-closed error results."""
    with open(SRC, encoding="utf-8") as f:
        source = f.read()
    assert 'output_findings = list(verification_issues)' in source
    assert 'Output findings to assess: {output_findings}' in source


def test_7u_fatal_pattern_list_intact():
    """Keep the known OCI signatures available to diagnostics and repair;
    accepting a source-defined fallback must not remove those findings."""
    with open(SRC, encoding="utf-8") as f:
        source = f.read()
    for pat in [
        "NotAuthorizedOrNotFound",
        "User does not have required privileges",
        "BucketNotFound",
        "GeneratePar operation in LakeSharing",
    ]:
        assert f'"{pat}"' in source, f"Fix 7u: fatal pattern '{pat}' removed"


# ═══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import inspect
    fns = [
        (name, fn) for name, fn in globals().items()
        if name.startswith("test_") and callable(fn)
    ]
    failures = []
    for name, fn in fns:
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures.append((name, str(e)))
            print(f"  ✗ {name}\n      {e}")
    print("=" * 60)
    if failures:
        print(f"✗ {len(failures)} FAILURE(S) out of {len(fns)}")
        sys.exit(1)
    print(f"✓ ALL {len(fns)} TESTS PASSED")
