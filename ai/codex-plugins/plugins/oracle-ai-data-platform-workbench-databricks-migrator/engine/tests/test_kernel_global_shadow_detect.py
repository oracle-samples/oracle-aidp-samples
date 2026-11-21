"""Tests for _detect_kernel_global_shadow in scripts/agent_migrate.py.

Prevents Opus's `run_on_cluster` diagnostic from rebinding reserved kernel
globals (sc / spark / dbutils / oidlUtils) at module scope. See the module-
level comment in agent_migrate.py for the failure mode this prevents.

Two focus areas:
  1. TRUE POSITIVE — every shape that would shadow gets detected
  2. TRUE NEGATIVE — legitimate diagnostic patterns are NOT flagged
     (regression safety: over-blocking would break Opus's normal workflow)
"""

from __future__ import annotations

import ast
import pathlib
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
AGENT_MIGRATE = REPO / "scripts" / "agent_migrate.py"


def _extract_shadow_detector():
    """Pull _detect_kernel_global_shadow + _names_bound_at_module_scope +
    _RESERVED_KERNEL_GLOBALS out of agent_migrate.py without importing the
    full module (heavy OCI / anthropic deps)."""
    src = AGENT_MIGRATE.read_text()
    tree = ast.parse(src)
    wanted_funcs = {
        "_detect_kernel_global_shadow",
        "_names_bound_at_module_scope",
        # PR-35 reviewer feedback: added walrus (ast.NamedExpr) + match/case
        # (ast.MatchAs / MatchStar / etc.) coverage. Both helpers live in
        # agent_migrate.py and are called from _names_bound_at_module_scope.
        "_walrus_target_names_at_this_scope",
        "_collect_match_pattern_names",
    }
    wanted_assigns = {"_RESERVED_KERNEL_GLOBALS"}
    picked = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_funcs:
            picked.append(node)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in wanted_assigns:
                    picked.append(node)
                    break
    module = ast.Module(body=picked, type_ignores=[])
    ns = {"__name__": "extracted", "ast": ast, "frozenset": frozenset}
    exec(compile(module, str(AGENT_MIGRATE), "exec"), ns)
    return ns


NS = _extract_shadow_detector()
_detect_kernel_global_shadow = NS["_detect_kernel_global_shadow"]
_names_bound_at_module_scope = NS["_names_bound_at_module_scope"]
_RESERVED = NS["_RESERVED_KERNEL_GLOBALS"]


class TestReservedSet(unittest.TestCase):

    def test_reserved_set_is_correct(self):
        # If someone drops one of these, sc-shadow returns silently.
        self.assertEqual(_RESERVED, frozenset({"sc", "spark", "dbutils", "oidlUtils"}))


class TestShadowDetected(unittest.TestCase):
    """Every one of these SHOULD be blocked."""

    def _assert_blocked(self, code, expected_name):
        msg = _detect_kernel_global_shadow(code)
        self.assertIsNotNone(msg, f"expected block for:\n{code}")
        self.assertIn(expected_name, msg)
        self.assertIn("BLOCKED", msg)

    def test_simple_assign_sc(self):
        # The exact Model_Mon 2026-07-28 incident
        self._assert_blocked(
            "sc = [c for c in df.columns if 'source' in c]",
            "sc",
        )

    def test_simple_assign_spark(self):
        self._assert_blocked("spark = 42", "spark")

    def test_simple_assign_dbutils(self):
        self._assert_blocked("dbutils = None", "dbutils")

    def test_simple_assign_oidlUtils(self):
        self._assert_blocked("oidlUtils = 'x'", "oidlUtils")

    def test_import_as_sc(self):
        # The AutoRefit 2026-07-26 incident
        self._assert_blocked(
            "import aidp_compat.bucket_shard as sc",
            "sc",
        )

    def test_from_import_as_sc(self):
        self._assert_blocked(
            "from mymodule import mytool as sc",
            "sc",
        )

    def test_annotated_assign(self):
        self._assert_blocked("sc: int = 5", "sc")

    def test_augmented_assign(self):
        self._assert_blocked("sc += 1", "sc")

    def test_tuple_unpacking(self):
        self._assert_blocked("sc, other = get_values()", "sc")

    def test_list_unpacking(self):
        self._assert_blocked("[sc, other] = [1, 2]", "sc")

    def test_starred_unpacking(self):
        self._assert_blocked("first, *sc = get_values()", "sc")

    def test_for_loop_variable(self):
        self._assert_blocked(
            "for sc in some_iterable:\n    print(sc)",
            "sc",
        )

    def test_with_as_clause(self):
        self._assert_blocked(
            "with open('/tmp/x') as sc:\n    print(sc.read())",
            "sc",
        )

    def test_function_def_named_sc(self):
        self._assert_blocked(
            "def sc(x):\n    return x + 1",
            "sc",
        )

    def test_class_def_named_sc(self):
        self._assert_blocked("class sc:\n    pass", "sc")

    def test_shadow_inside_if_body(self):
        # If at module scope, the if body binds at module scope
        self._assert_blocked(
            "if True:\n    sc = 42",
            "sc",
        )

    def test_shadow_inside_try_body(self):
        self._assert_blocked(
            "try:\n    sc = 42\nexcept Exception:\n    pass",
            "sc",
        )

    def test_shadow_inside_try_finally(self):
        self._assert_blocked(
            "try:\n    x = 1\nfinally:\n    sc = 42",
            "sc",
        )

    def test_multiple_reserved_names_in_one_snippet(self):
        # Both sc AND dbutils shadowed — both should appear
        msg = _detect_kernel_global_shadow("sc = 1\ndbutils = 2")
        self.assertIsNotNone(msg)
        self.assertIn("sc", msg)
        self.assertIn("dbutils", msg)

    def test_shadow_inside_for_body_model_mon_incident(self):
        # THE EXACT Model_Mon 2026-07-28 shape — sc assigned inside `for` body.
        # Regression test: my initial impl missed this because it only checked
        # the loop VARIABLE, not the loop BODY. Fix recurses into node.body.
        code = (
            "base = '/Volumes/x/y/z'\n"
            "for path in [base+'/a', base+'/b']:\n"
            "    try:\n"
            "        df = spark.read.csv(path)\n"
            "        sc = [c for c in df.columns if 'source' in c]\n"
            "        if sc: print(sc)\n"
            "    except Exception as e:\n"
            "        print('ERR', e)\n"
        )
        self._assert_blocked(code, "sc")

    def test_shadow_inside_while_body(self):
        code = "while True:\n    sc = 42\n    break"
        self._assert_blocked(code, "sc")

    def test_shadow_inside_with_body(self):
        code = "with open('/tmp/x') as f:\n    sc = f.read()"
        self._assert_blocked(code, "sc")

    def test_shadow_inside_except_body(self):
        code = "try:\n    x = 1\nexcept Exception:\n    sc = 42"
        self._assert_blocked(code, "sc")

    def test_shadow_in_nested_for_try_for(self):
        code = (
            "for a in [1, 2]:\n"
            "    try:\n"
            "        for b in [3, 4]:\n"
            "            sc = a + b\n"
            "    except:\n"
            "        pass\n"
        )
        self._assert_blocked(code, "sc")

    # ─── PR-35 reviewer feedback — walrus (NamedExpr) + match/case coverage ─────

    def test_walrus_at_module_scope_in_if_test(self):
        # `if (sc := compute()):` — walrus in if.test binds sc at module scope.
        # This is the reviewer's specific example from the PR-35 review.
        code = "if (sc := 42):\n    print(sc)"
        self._assert_blocked(code, "sc")

    def test_walrus_at_module_scope_in_while_test(self):
        code = "while (sc := next_batch()):\n    process(sc)"
        self._assert_blocked(code, "sc")

    def test_walrus_in_module_level_list_comp(self):
        # Walrus in comprehension binds in enclosing scope (module here).
        code = "result = [x for x in xs if (sc := f(x))]"
        self._assert_blocked(code, "sc")

    def test_walrus_in_assignment_rhs(self):
        # `x = (sc := 5)` — walrus RHS binds sc at module scope.
        code = "x = (sc := compute())"
        self._assert_blocked(code, "sc")

    def test_match_case_capture_pattern_binds_module(self):
        # `case pattern as sc:` binds sc in the enclosing scope.
        code = "match value:\n    case (x, y) as sc:\n        print(sc)"
        self._assert_blocked(code, "sc")

    def test_match_case_bare_capture_binds_module(self):
        # `case sc:` (bare name in pattern position) is a capture — binds sc.
        code = "match value:\n    case sc:\n        print(sc)"
        self._assert_blocked(code, "sc")

    def test_match_case_star_capture_binds_module(self):
        # `case [head, *sc]:` — star capture binds sc.
        code = "match value:\n    case [head, *sc]:\n        print(sc)"
        self._assert_blocked(code, "sc")

    def test_match_case_mapping_double_star_binds_module(self):
        # `case {**sc}:` — mapping-rest binds sc.
        code = "match value:\n    case {'k': v, **sc}:\n        print(sc)"
        self._assert_blocked(code, "sc")

    def test_match_body_binding_leaks_to_module(self):
        # Bindings inside a case body execute at module scope.
        code = "match value:\n    case 1:\n        sc = compute()"
        self._assert_blocked(code, "sc")



class TestFalsePositivesGuarded(unittest.TestCase):
    """These SHOULD NOT be blocked — over-blocking would break Opus workflow."""

    def _assert_ok(self, code, hint=""):
        msg = _detect_kernel_global_shadow(code)
        self.assertIsNone(
            msg,
            f"UNEXPECTED BLOCK ({hint}):\ncode:\n{code}\nmsg:\n{msg}",
        )

    def test_probe_prefix_ok(self):
        self._assert_ok(
            "_probe_sc = [c for c in df.columns if 'source' in c]\nprint(_probe_sc)",
            "_probe_-prefixed name",
        )

    def test_underscore_prefix_ok(self):
        self._assert_ok(
            "_sc_local = spark.sparkContext\nprint(_sc_local.appName)",
            "single-underscore prefix",
        )

    def test_bare_reference_ok(self):
        # Just USING sc — not rebinding
        self._assert_ok(
            "print(sc.appName)\nprint(spark.version)",
            "read-only access",
        )

    def test_attribute_assignment_ok(self):
        # obj.sc = x mutates obj, doesn't rebind sc global
        self._assert_ok(
            "class Foo: pass\nfoo = Foo()\nfoo.sc = 42",
            "attribute assign on unrelated obj",
        )

    def test_subscript_assignment_ok(self):
        # d["sc"] = x mutates dict, doesn't rebind sc
        self._assert_ok(
            "d = {}\nd['sc'] = 42",
            "dict subscript",
        )

    def test_binding_inside_function_body_ok(self):
        # Function-local `sc = ...` is contained — doesn't leak
        self._assert_ok(
            "def _probe():\n    sc = 42\n    print(sc)\n_probe()",
            "function-local binding",
        )

    def test_binding_inside_lambda_ok(self):
        # Lambda body is function-scope — sc binding doesn't leak.
        # (Note: `sc=42` in lambda is a default arg, still local.)
        self._assert_ok(
            "f = lambda sc=42: sc + 1",
            "lambda default arg",
        )

    def test_binding_inside_class_body_ok(self):
        # Class body has its own scope — `sc = X` becomes a class attribute
        self._assert_ok(
            "class Foo:\n    sc = 42",
            "class attribute",
        )

    def test_binding_inside_method_body_ok(self):
        self._assert_ok(
            "class Foo:\n    def bar(self):\n        sc = 42",
            "method-local binding",
        )

    def test_typical_opus_diagnostic_ok(self):
        # A realistic Opus probe using function-scoping (what the prompt tells
        # them to do)
        code = (
            "def _probe():\n"
            "    import aidp_compat.bucket_shard as _scmp\n"
            "    return dir(_scmp)\n"
            "print(_probe())"
        )
        self._assert_ok(code, "canonical function-scoped probe")

    def test_typical_opus_read_only_ok(self):
        code = (
            "import os\n"
            "print(os.listdir('/tmp'))\n"
            "print('spark version:', spark.version)\n"
            "print('sc appName:', sc.appName)"
        )
        self._assert_ok(code, "typical read-only diagnostic")

    def test_pandas_dataframe_ok(self):
        # `df = ...` at module scope — `df` is NOT reserved
        self._assert_ok(
            "import pandas as pd\ndf = pd.read_csv('/tmp/foo.csv')\nprint(df.head())",
            "df is not reserved",
        )

    def test_import_regular_module_ok(self):
        self._assert_ok(
            "import os\nimport sys\nfrom pathlib import Path",
            "regular imports",
        )

    def test_variable_containing_sc_ok(self):
        # `source_cols` contains 'sc' as substring, but the name is distinct
        self._assert_ok(
            "source_cols = ['a', 'b']\nsc_list = []\nprint(source_cols)",
            "sc as substring in longer name",
        )

    # ─── PR-35 reviewer feedback — walrus scope-boundary false-positive guards ─

    def test_walrus_inside_function_body_ok(self):
        # Walrus inside a def body is function-local — MUST NOT be flagged.
        code = "def foo():\n    if (sc := compute()):\n        return sc"
        self._assert_ok(code, "walrus inside def body is function-local")

    def test_walrus_inside_lambda_ok(self):
        # Lambda body is its own scope.
        code = "f = lambda: (sc := 42)"
        self._assert_ok(code, "walrus in lambda body")

    def test_walrus_inside_class_body_ok(self):
        # Class body is a scope; names inside stay on the class, not module.
        code = "class Foo:\n    if (sc := 42):\n        pass"
        self._assert_ok(code, "walrus inside class body")

    def test_match_inside_function_body_ok(self):
        # Whole match statement inside a def is function-local.
        code = (
            "def foo():\n"
            "    match value:\n"
            "        case x as sc:\n"
            "            return sc"
        )
        self._assert_ok(code, "match inside function body")


class TestErrorMessageQuality(unittest.TestCase):
    """The message shown to Opus must be actionable."""

    def test_message_lists_shadowed_names(self):
        msg = _detect_kernel_global_shadow("sc = 1\nspark = 2")
        self.assertIn("sc", msg)
        self.assertIn("spark", msg)

    def test_message_includes_fix_hint(self):
        msg = _detect_kernel_global_shadow("sc = 1")
        self.assertIn("_probe_", msg)
        self.assertIn("function", msg.lower())
        self.assertIn("re-submit", msg.lower())

    def test_message_explains_why(self):
        msg = _detect_kernel_global_shadow("sc = 1")
        self.assertIn("setJobGroup", msg)
        self.assertIn("kernel", msg.lower())


class TestEdgeCases(unittest.TestCase):

    def test_empty_code_ok(self):
        self.assertIsNone(_detect_kernel_global_shadow(""))

    def test_whitespace_only_ok(self):
        self.assertIsNone(_detect_kernel_global_shadow("   \n  \n"))

    def test_syntax_error_returns_none(self):
        # Broken code shouldn't crash the detector; let AIDP surface the error
        self.assertIsNone(_detect_kernel_global_shadow("def foo(:"))

    def test_comment_only_ok(self):
        self.assertIsNone(_detect_kernel_global_shadow("# sc = 42 (in comment, not code)"))

    def test_string_literal_only_ok(self):
        # `sc = ...` inside a string literal is not a binding
        self.assertIsNone(_detect_kernel_global_shadow('doc = "sc = 42"'))

    def test_docstring_with_reserved_name_ok(self):
        self.assertIsNone(_detect_kernel_global_shadow('"""tell Opus about sc = ..."""'))


if __name__ == "__main__":
    unittest.main()
