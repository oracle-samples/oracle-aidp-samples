"""Tests for _apply_dbfs_translations in scripts/job_migrate.py.

Covers the /dbfs/... and dbfs:/... → /Volumes/{catalog}/{schema}/dbfs/...
static rewriter used at every S3-translation site + the child-inline path.
Extracts the target function via AST (matches the pattern already used in
tests/test_pip_install_hang_fix.py) so we don't import the whole module and
trigger its heavy top-level imports."""

from __future__ import annotations

import ast
import os
import pathlib
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
JOB_MIGRATE = REPO / "scripts" / "job_migrate.py"


def _extract_symbols_from_job_migrate():
    """Grab _apply_dbfs_translations + its regex helpers + _cell_is_python
    from job_migrate.py without importing the full module (which requires
    OCI SDK, aiohttp, ..., not available in a bare test env)."""
    src = JOB_MIGRATE.read_text()
    tree = ast.parse(src)

    wanted_funcs = {"_apply_dbfs_translations", "_cell_is_python"}
    wanted_assigns = {"_DBFS_SLASH_RE", "_DBFS_COLON_RE"}

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
    ns = {"__name__": "extracted", "re": __import__("re"), "os": os}
    exec(compile(module, str(JOB_MIGRATE), "exec"), ns)
    return ns


NS = _extract_symbols_from_job_migrate()
_apply_dbfs_translations = NS["_apply_dbfs_translations"]


DEFAULT_TARGET = "/Volumes/default/default/dbfs"


class TestBasicRewrites(unittest.TestCase):

    def test_slash_dbfs_with_path(self):
        src = 'path = "/dbfs/FileStore/foo.csv"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'path = "{DEFAULT_TARGET}/FileStore/foo.csv"',
        )

    def test_dbfs_colon_scheme(self):
        src = 'path = "dbfs:/FileStore/foo.csv"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'path = "{DEFAULT_TARGET}/FileStore/foo.csv"',
        )

    def test_slash_dbfs_bare_string_end(self):
        # "/dbfs" + suffix — concat pattern
        src = 'p = "/dbfs" + rest'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'p = "{DEFAULT_TARGET}" + rest',
        )

    def test_fstring_literal_prefix(self):
        src = 'p = f"/dbfs/FileStore/{name}.csv"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'p = f"{DEFAULT_TARGET}/FileStore/{{name}}.csv"',
        )

    def test_dict_value_literal(self):
        src = 'schema_map = {"col_a": "/dbfs/FileStore/schemas/a.pkl"}'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'schema_map = {{"col_a": "{DEFAULT_TARGET}/FileStore/schemas/a.pkl"}}',
        )

    def test_multiple_literals_same_line(self):
        src = 'a = "/dbfs/x"; b = "dbfs:/y"'
        got = _apply_dbfs_translations(src)
        self.assertEqual(
            got,
            f'a = "{DEFAULT_TARGET}/x"; b = "{DEFAULT_TARGET}/y"',
        )

    def test_task2_style_literal(self):
        # From Task 2 EXPORT cell 16
        src = 'sc_df.to_csv(f"/dbfs/FileStore/model_monitoring/{model_name}_scores.csv", index=False)'
        got = _apply_dbfs_translations(src)
        self.assertIn(f"{DEFAULT_TARGET}/FileStore/model_monitoring/", got)
        self.assertNotIn('"/dbfs/', got)

    def test_task1_dict_literal_schema_map(self):
        # From Task 1 style — schema_map built as dict literal in source
        src = (
            "schema_map = {\n"
            '    "col_a": "/dbfs/FileStore/schemas/col_a.pkl",\n'
            '    "col_b": "/dbfs/FileStore/schemas/col_b.pkl",\n'
            "}\n"
            "for c in cols:\n"
            "    pickle.load(open(schema_map[c], 'rb'))\n"
        )
        got = _apply_dbfs_translations(src)
        self.assertIn(f'"{DEFAULT_TARGET}/FileStore/schemas/col_a.pkl"', got)
        self.assertIn(f'"{DEFAULT_TARGET}/FileStore/schemas/col_b.pkl"', got)
        # runtime open() call is unchanged (that's the accepted static limit)
        self.assertIn("pickle.load(open(schema_map[c], 'rb'))", got)


class TestIdempotency(unittest.TestCase):

    def test_already_translated_unchanged(self):
        src = f'path = "{DEFAULT_TARGET}/FileStore/foo.csv"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_double_apply_stable(self):
        src = 'path = "/dbfs/FileStore/foo.csv"'
        once = _apply_dbfs_translations(src)
        twice = _apply_dbfs_translations(once)
        self.assertEqual(once, twice)

    def test_triple_apply_stable(self):
        src = 'path = "dbfs:/FileStore/foo.csv"'
        once = _apply_dbfs_translations(src)
        twice = _apply_dbfs_translations(once)
        thrice = _apply_dbfs_translations(twice)
        self.assertEqual(once, twice)
        self.assertEqual(twice, thrice)


class TestFalsePositiveGuards(unittest.TestCase):

    def test_identifier_dbfs_client_unchanged(self):
        # word starting with dbfs — not a path — must not be rewritten
        src = "dbfs_client = get_dbfs_client()"
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_variable_containing_dbfs_unchanged(self):
        src = "my_dbfs_path = 'foo'"
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_dbfs_folder_inside_other_path_unchanged(self):
        # /some/user/dbfs/x — user's own folder named dbfs, NOT the mount
        src = 'p = "/some/user/dbfs/x"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_dbfs_colon_bare_not_rewritten(self):
        # customer's own .replace("dbfs:", ...) — must not be rewritten
        src = 'clean = path.replace("dbfs:", "/mnt/x")'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_startswith_dbfs_colon_bare_not_rewritten(self):
        # customer's own startswith("dbfs:") check
        src = 'if path.startswith("dbfs:"): print("dbfs path")'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_dbfs_dot_extension_not_rewritten(self):
        # /dbfs.log — followed by '.' — not a path boundary; guard blocks
        src = 'log_path = "/dbfs.log"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_dbfs_followed_by_word_char_not_rewritten(self):
        # /dbfs2 — followed by a digit — not the DBFS mount
        src = 'p = "/dbfs2/foo"'
        self.assertEqual(_apply_dbfs_translations(src), src)


class TestLanguageGuard(unittest.TestCase):

    def test_scala_cell_skipped(self):
        src = '%scala\nval p = "/dbfs/FileStore/foo.csv"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_sql_cell_skipped(self):
        src = "%sql\nSELECT * FROM parquet.`/dbfs/FileStore/foo`"
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_r_cell_skipped(self):
        src = '%r\np <- "/dbfs/FileStore/foo.csv"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_sh_cell_skipped(self):
        src = "%sh\ncat /dbfs/FileStore/foo"
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_md_cell_skipped(self):
        src = "%md\n# /dbfs/FileStore/foo"
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_python_magic_still_translated(self):
        # Explicit %python cell should still be rewritten (Python default path)
        src = '%python\np = "/dbfs/FileStore/foo.csv"'
        got = _apply_dbfs_translations(src)
        self.assertIn(f"{DEFAULT_TARGET}/FileStore/foo.csv", got)


class TestConfigOverrides(unittest.TestCase):

    def setUp(self):
        self._saved = {}
        for k in ("AIDP_DBFS_PREFIX", "AIDP_DBFS_CATALOG", "AIDP_DBFS_SCHEMA"):
            self._saved[k] = os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_prefix_env_override(self):
        os.environ["AIDP_DBFS_PREFIX"] = "/custom/dbfs"
        src = 'p = "/dbfs/foo"'
        self.assertEqual(_apply_dbfs_translations(src), 'p = "/custom/dbfs/foo"')

    def test_prefix_trailing_slash_stripped(self):
        os.environ["AIDP_DBFS_PREFIX"] = "/custom/dbfs/"
        src = 'p = "/dbfs/foo"'
        # trailing slash must be normalized — otherwise result would be "/custom/dbfs//foo"
        self.assertEqual(_apply_dbfs_translations(src), 'p = "/custom/dbfs/foo"')

    def test_catalog_schema_override(self):
        os.environ["AIDP_DBFS_CATALOG"] = "mycatalog"
        os.environ["AIDP_DBFS_SCHEMA"] = "myschema"
        src = 'p = "/dbfs/foo"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            'p = "/Volumes/mycatalog/myschema/dbfs/foo"',
        )

    def test_prefix_wins_over_catalog_schema(self):
        os.environ["AIDP_DBFS_PREFIX"] = "/xyz"
        os.environ["AIDP_DBFS_CATALOG"] = "mycatalog"
        os.environ["AIDP_DBFS_SCHEMA"] = "myschema"
        src = 'p = "/dbfs/foo"'
        self.assertEqual(_apply_dbfs_translations(src), 'p = "/xyz/foo"')


class TestFastPath(unittest.TestCase):

    def test_no_dbfs_returns_source_unchanged(self):
        src = "df = spark.read.parquet('s3a://bucket/path')"
        # Object identity check would be too strict; check equality is fine
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_empty_string_returns_empty(self):
        self.assertEqual(_apply_dbfs_translations(""), "")

    def test_none_returns_none(self):
        self.assertIsNone(_apply_dbfs_translations(None))


class TestS3RegressionSafety(unittest.TestCase):
    """Ensure the dbfs rewriter never touches s3:// or oci:// URIs (those
    are S3's territory)."""

    def test_s3_uri_untouched(self):
        src = 'p = "s3a://bucket/dbfs-lookalike/file.parquet"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_oci_uri_untouched(self):
        src = 'p = "oci://mybucket@myns/dbfs-lookalike/file.parquet"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_https_url_untouched(self):
        src = 'p = "https://dbfs.example.com/path"'
        self.assertEqual(_apply_dbfs_translations(src), src)


class TestMultilineAndPatterns(unittest.TestCase):

    def test_multiline_dict_all_translated(self):
        src = (
            "paths = {\n"
            '    "a": "/dbfs/x",\n'
            '    "b": "/dbfs/y",\n'
            '    "c": "/dbfs/z",\n'
            "}"
        )
        got = _apply_dbfs_translations(src)
        for k in ("x", "y", "z"):
            self.assertIn(f'"{DEFAULT_TARGET}/{k}"', got)

    def test_triple_quoted_string_translated(self):
        src = "sql = '''\nSELECT * FROM parquet.`/dbfs/FileStore/foo`\n'''"
        got = _apply_dbfs_translations(src)
        self.assertIn(f"{DEFAULT_TARGET}/FileStore/foo", got)

    def test_mixed_scheme_and_path_forms(self):
        src = (
            'a = "/dbfs/A"\n'
            'b = "dbfs:/B"\n'
            'c = "/dbfs/nested/deep/path.csv"\n'
        )
        got = _apply_dbfs_translations(src)
        self.assertIn(f'"{DEFAULT_TARGET}/A"', got)
        self.assertIn(f'"{DEFAULT_TARGET}/B"', got)
        self.assertIn(f'"{DEFAULT_TARGET}/nested/deep/path.csv"', got)

    def test_concat_pattern_kept_intact(self):
        # Task 2 style: pd.read_csv("/dbfs" + feat_imp_path[5:])
        src = 'df = pd.read_csv("/dbfs" + feat_imp_path[5:])'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'df = pd.read_csv("{DEFAULT_TARGET}" + feat_imp_path[5:])',
        )


class TestDocumentedEdgeCases(unittest.TestCase):
    """Lock in behavior for edge cases surfaced during the review pass.
    These aren't defects — they're intentionally-accepted trade-offs. The
    tests prevent silent behavior drift."""

    def test_dbfs_double_slash_becomes_target_double_slash(self):
        # dbfs://foo → target/ + /foo = target//foo. POSIX filesystems normalize
        # // to / so this is cosmetic; leaving as-is avoids extra regex.
        src = 'p = "dbfs://foo"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'p = "{DEFAULT_TARGET}//foo"',
        )

    def test_comment_containing_dbfs_gets_rewritten_cosmetically(self):
        # Comments are rewritten too (same behavior as _apply_s3_translations).
        # Cosmetic-only — no runtime impact.
        src = "# /dbfs/FileStore/foo\ncode = 1"
        self.assertEqual(
            _apply_dbfs_translations(src),
            f"# {DEFAULT_TARGET}/FileStore/foo\ncode = 1",
        )

    def test_escape_sequence_before_dbfs_blocked(self):
        # "\n/dbfs/foo" — the regex sees 'n' immediately before /dbfs (the
        # backslash-n escape is two source chars). 'n' is in [A-Za-z0-9_] →
        # negative lookbehind blocks. Accepted limit; use dbfs:/ form or a
        # separate line if this pattern comes up in real notebooks.
        src = '"\\n/dbfs/foo"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_raw_string_prefix_still_translated(self):
        src = 'p = r"/dbfs/foo"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'p = r"{DEFAULT_TARGET}/foo"',
        )

    def test_bytes_string_prefix_still_translated(self):
        # b"..." bytes literals — unusual for paths but should still translate.
        src = 'p = b"/dbfs/foo"'
        self.assertEqual(
            _apply_dbfs_translations(src),
            f'p = b"{DEFAULT_TARGET}/foo"',
        )


class TestNestedPathAvoidance(unittest.TestCase):
    """Guard against re-translation of already-migrated paths inside a longer
    literal (e.g., an audit log that quotes the target path back)."""

    def test_target_prefix_inside_larger_string_untouched(self):
        # Path already migrated, quoted inside another string — must not be
        # re-translated (would produce /Volumes/.../Volumes/.../dbfs/...).
        src = f'msg = "Wrote to {DEFAULT_TARGET}/FileStore/x"'
        self.assertEqual(_apply_dbfs_translations(src), src)

    def test_workspace_dbfs_style_path_untouched(self):
        # /Workspace/dbfs — Workspace folder that happens to be named dbfs.
        # The negative lookbehind blocks it (preceded by 'e' of "Workspace").
        src = 'p = "/Workspace/user/dbfs/x"'
        self.assertEqual(_apply_dbfs_translations(src), src)


if __name__ == "__main__":
    unittest.main()
