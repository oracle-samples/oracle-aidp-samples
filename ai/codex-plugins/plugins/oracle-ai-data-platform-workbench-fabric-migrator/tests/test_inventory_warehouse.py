import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import warehouse as wh
from fabric_aidp.inventory.git_workspace import discover_items


def _warehouse(root: Path, display: str, files: dict) -> None:
    d = root / f"{display}.Warehouse"
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "config": {"logicalId": f"id-{display}"},
        "metadata": {"type": "Warehouse", "displayName": display},
    }), encoding="utf-8")
    for rel, body in files.items():
        p = d / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        # bytes go down verbatim: the whole point of the encoding tests is
        # what the scanner does with a file it did not write itself.
        if isinstance(body, bytes):
            p.write_bytes(body)
        else:
            p.write_text(body, encoding="utf-8")


class CommentStrippingTests(unittest.TestCase):
    def test_line_comment_is_removed_but_length_preserved(self):
        out = wh.strip_sql_comments("SELECT 1 -- pick one\nSELECT 2")
        self.assertEqual(len(out), len("SELECT 1 -- pick one\nSELECT 2"))
        self.assertNotIn("pick one", out)
        self.assertIn("SELECT 2", out)

    def test_block_comment_is_removed(self):
        self.assertNotIn("hidden", wh.strip_sql_comments("/* hidden */ CREATE TABLE t"))

    def test_comment_markers_inside_a_string_survive(self):
        out = wh.strip_sql_comments("SELECT '-- not a comment'")
        self.assertIn("-- not a comment", out)

    def test_escaped_quote_inside_a_literal(self):
        out = wh.strip_sql_comments("SELECT 'it''s -- fine'")
        self.assertIn("-- fine", out)


class ClassifyTests(unittest.TestCase):
    def test_bracketed_table(self):
        self.assertEqual(wh.classify_sql_object("CREATE TABLE [dbo].[claim] (id INT)"),
                         ("table", "dbo", "claim"))

    def test_unbracketed_table(self):
        self.assertEqual(wh.classify_sql_object("CREATE TABLE dbo.claim (id INT)"),
                         ("table", "dbo", "claim"))

    def test_double_quoted_table(self):
        # This used to come back as ('table', '   ', ' '): `"..."` was masked
        # as a string literal, so the classifier read the blanked body as the
        # name. T-SQL's default is QUOTED_IDENTIFIER ON, so it is a name.
        self.assertEqual(
            wh.classify_sql_object('CREATE TABLE "dbo"."claim" (id INT)'),
            ("table", "dbo", "claim"))

    def test_double_quoted_name_with_a_space(self):
        self.assertEqual(
            wh.classify_sql_object('CREATE VIEW "my view" AS SELECT 1'),
            ("view", "dbo", "my view"))

    def test_table_without_a_schema_defaults_to_dbo(self):
        self.assertEqual(wh.classify_sql_object("CREATE TABLE claim (id INT)"),
                         ("table", "dbo", "claim"))

    def test_view(self):
        self.assertEqual(wh.classify_sql_object("CREATE VIEW dbo.v_claims AS SELECT 1"),
                         ("view", "dbo", "v_claims"))

    def test_create_or_alter_procedure(self):
        self.assertEqual(
            wh.classify_sql_object("CREATE OR ALTER PROCEDURE dbo.sp_load AS BEGIN END"),
            ("procedure", "dbo", "sp_load"))

    def test_proc_abbreviation(self):
        self.assertEqual(wh.classify_sql_object("CREATE PROC dbo.sp_x AS BEGIN END")[0],
                         "procedure")

    def test_function(self):
        self.assertEqual(wh.classify_sql_object("CREATE FUNCTION dbo.fn_x() RETURNS INT")[0],
                         "function")

    def test_leading_comment_does_not_hide_the_object(self):
        self.assertEqual(
            wh.classify_sql_object("-- header\n/* more */\nCREATE TABLE dbo.t (id INT)"),
            ("table", "dbo", "t"))

    def test_unclassifiable_sql_is_other_not_dropped(self):
        self.assertEqual(wh.classify_sql_object("EXEC sp_whatever"), ("other", "", ""))

    def test_create_inside_a_string_is_not_the_object(self):
        self.assertEqual(wh.classify_sql_object("EXEC('CREATE TABLE x')"), ("other", "", ""))


class ScanTests(unittest.TestCase):
    def _scan(self, files):
        with TemporaryDirectory() as t:
            root = Path(t)
            _warehouse(root, "AcmeDW", files)
            return wh.scan(discover_items(root))

    def test_objects_are_collected_with_kind_and_sql(self):
        out = self._scan({
            "schemas/dbo/tables/claim.sql": "CREATE TABLE [dbo].[claim] (id INT)",
            "schemas/dbo/views/v_claims.sql": "CREATE VIEW dbo.v_claims AS SELECT 1",
        })
        objects = out["items"]["warehouses"][0]["objects"]
        self.assertEqual([o["kind"] for o in objects], ["table", "view"])
        self.assertIn("CREATE TABLE", objects[0]["sql"])

    def test_flat_layout_is_handled_too(self):
        out = self._scan({"claim.sql": "CREATE TABLE dbo.claim (id INT)"})
        self.assertEqual(out["items"]["warehouses"][0]["objects"][0]["name"], "claim")

    def test_summary_counts_by_kind(self):
        out = self._scan({
            "a.sql": "CREATE TABLE dbo.a (id INT)",
            "b.sql": "CREATE TABLE dbo.b (id INT)",
            "p.sql": "CREATE PROCEDURE dbo.p AS BEGIN END",
        })
        self.assertEqual(out["summary"]["object_counts"], {"table": 2, "procedure": 1})
        self.assertEqual(out["summary"]["warehouse_count"], 1)

    def test_file_path_is_recorded_relative_and_posix(self):
        out = self._scan({"schemas/dbo/tables/claim.sql": "CREATE TABLE dbo.claim (id INT)"})
        self.assertEqual(out["items"]["warehouses"][0]["objects"][0]["file"],
                         "schemas/dbo/tables/claim.sql")

    def test_object_order_is_deterministic(self):
        out = self._scan({"z.sql": "CREATE TABLE dbo.z (id INT)",
                          "a.sql": "CREATE TABLE dbo.a (id INT)"})
        self.assertEqual([o["file"] for o in out["items"]["warehouses"][0]["objects"]],
                         ["a.sql", "z.sql"])

    def test_sqlproj_and_non_sql_files_are_ignored(self):
        out = self._scan({"AcmeDW.sqlproj": "<Project/>",
                          "t.sql": "CREATE TABLE dbo.t (id INT)"})
        self.assertEqual(len(out["items"]["warehouses"][0]["objects"]), 1)

    def test_no_warehouses_yields_an_empty_but_valid_shape(self):
        self.assertEqual(wh.scan([]), {
            "summary": {"warehouse_count": 0, "unreadable_object_count": 0,
                        "unterminated_object_count": 0,
                        "object_counts": {}},
            "items": {"warehouses": []},
        })



class ReferencedObjectTests(unittest.TestCase):
    """`plan/planner.py` opens by saying the plan is ordered "so a producer
    precedes its consumers". For warehouse objects `depends_on` was the
    literal `[]`, so a view could be emitted before the table it selects
    from. The names have to come out of the SQL before an edge can exist.
    """

    def test_from_and_join_are_both_references(self):
        self.assertEqual(
            wh.referenced_objects(
                "CREATE VIEW dbo.v AS SELECT * FROM dbo.a JOIN sales.b ON 1=1"),
            ["dbo.a", "sales.b"])

    def test_a_write_target_is_a_reference_too(self):
        """The table has to exist before the procedure that fills it."""
        self.assertEqual(
            wh.referenced_objects("INSERT INTO dbo.t SELECT 1"), ["dbo.t"])

    def test_a_derived_table_is_not_an_object(self):
        self.assertEqual(wh.referenced_objects("SELECT * FROM (SELECT 1) x"), [])

    def test_a_function_call_is_not_an_object(self):
        self.assertEqual(
            wh.referenced_objects("SELECT * FROM STRING_SPLIT(a, ',')"), [])

    def test_a_common_table_expression_is_not_an_object(self):
        self.assertEqual(
            wh.referenced_objects(
                "WITH cte AS (SELECT 1) SELECT * FROM cte JOIN dbo.b ON 1=1"),
            ["dbo.b"])

    def test_a_temp_table_is_not_an_object(self):
        self.assertEqual(wh.referenced_objects("SELECT * FROM #staging"), [])

    def test_a_name_in_a_comment_is_not_a_reference(self):
        self.assertEqual(
            wh.referenced_objects("-- FROM dbo.old\nSELECT * FROM dbo.new"),
            ["dbo.new"])

    def test_a_name_in_a_string_literal_is_not_a_reference(self):
        self.assertEqual(
            wh.referenced_objects("SELECT 'FROM dbo.old' FROM dbo.new"),
            ["dbo.new"])

    def test_bracket_quoting_comes_off(self):
        self.assertEqual(
            wh.referenced_objects("SELECT * FROM [dbo].[My Table]"),
            ["dbo.My Table"])

    def test_the_scan_records_them_on_the_object(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _warehouse(root, "DW", {
                "base.sql": "CREATE TABLE dbo.base (id INT)",
                "v.sql": "CREATE VIEW dbo.v AS SELECT * FROM dbo.base",
            })
            objects = {o["name"]: o for o in
                       wh.scan(discover_items(root))["items"]["warehouses"][0]["objects"]}
            self.assertEqual(objects["v"]["reads"], ["dbo.base"])
            self.assertEqual(objects["base"]["reads"], [])
DDL = "CREATE TABLE dbo.claim (id INT IDENTITY(1,1) PRIMARY KEY);"


class EncodingDetectionTests(unittest.TestCase):
    """T4(a). The scanner did `path.read_text(encoding="utf-8-sig")` inside
    an `except UnicodeError`, which handles the encodings that raise and is
    blind to the one that does not. Measured before the fix, on DDL:

        utf-16-le  DECODED  classify -> ('other','','')  sql full of NULs
        utf-16-be  DECODED  classify -> ('other','','')  sql full of NULs
        utf-16 (BOM) RAISED   utf-32 RAISED
        cp1252 RAISED   latin-1 RAISED

    A BOM-less UTF-16 `.sql` -- what `sqlcmd -u` and PowerShell's `>` write
    -- is ASCII interleaved with NUL, and that is valid UTF-8. It did not
    raise, so the CREATE was invisible, the object was misclassified
    `other`, the name fell back to the file stem, and NUL-laden text was
    carried to the artifact with zero findings.
    """

    def test_every_unicode_encoding_of_the_same_ddl_classifies_the_same(self):
        for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le",
                         "utf-16-be", "utf-32", "utf-32-le", "utf-32-be"):
            with self.subTest(encoding=encoding):
                data = DDL.encode(encoding)
                detected = wh._detect_encoding(data)
                self.assertEqual(
                    wh.classify_sql_object(data.decode(detected)),
                    ("table", "dbo", "claim"))

    def test_a_bom_less_utf16_file_is_not_carried_as_utf8(self):
        for encoding in ("utf-16-le", "utf-16-be"):
            with self.subTest(encoding=encoding):
                out = ScanTests()._scan({"claim.sql": DDL.encode(encoding)})
                obj = out["items"]["warehouses"][0]["objects"][0]
                self.assertEqual(obj["kind"], "table")
                self.assertEqual(obj["name"], "claim")
                self.assertNotIn("\x00", obj["sql"])
                self.assertEqual(obj["read_error"], "")

    def test_plain_utf8_is_still_the_common_case(self):
        out = ScanTests()._scan({"claim.sql": DDL})
        self.assertEqual(out["items"]["warehouses"][0]["objects"][0]["sql"], DDL)

    def test_a_utf8_bom_is_still_stripped(self):
        out = ScanTests()._scan({"claim.sql": b"\xef\xbb\xbf" + DDL.encode("utf-8")})
        self.assertEqual(out["items"]["warehouses"][0]["objects"][0]["sql"], DDL)

    def test_nul_bytes_that_are_no_known_encoding_are_refused_not_guessed(self):
        """Better an unreadable file reported loudly than text nobody can
        use carried as if it were SQL."""
        with self.assertRaises(UnicodeError):
            wh._detect_encoding(b"\x00\x00\x00\x00\xff\xfe\xfd\x00\x00\x00")


class UnreadableObjectIdentityTests(unittest.TestCase):
    """T4(b). An undecodable file became
    `name = f"<unreadable: {exc}>"`, and the planner puts a warehouse
    object's name straight into its asset id. A codec error names the
    OFFSET of the first bad byte, so two different files that break at the
    same offset got the same id. Reproduced end to end before the fix, two
    files both ending `DEFAULT 'caf\xe9');` with the bad byte at 46:

        $ fabric-aidp inventory /tmp/t4ws -o m.json     # exit 0, silent
        $ fabric-aidp plan m.json -o p.json
        error: duplicate asset id(s): warehouse.AcmeDW.other.<unreadable:
          'utf-8' codec can't decode byte 0xe9 in position 46: invalid
          continuation byte>
        exit 2

    The whole workspace was refused, and the message the operator got was a
    Python codec error wearing an asset id.
    """

    BAD_A = b"CREATE TABLE dbo.a (n VARCHAR(10) DEFAULT 'caf\xe9');"
    BAD_B = b"CREATE TABLE dbo.b (n VARCHAR(10) DEFAULT 'caf\xe9');"

    def _objects(self, files):
        return ScanTests()._scan(files)["items"]["warehouses"][0]["objects"]

    def test_two_files_breaking_at_the_same_offset_get_different_names(self):
        names = [o["name"] for o in
                 self._objects({"dupA.sql": self.BAD_A, "dupB.sql": self.BAD_B})]
        self.assertEqual(names, ["dupA", "dupB"])

    def test_the_name_is_never_an_exception_string(self):
        for obj in self._objects({"dupA.sql": self.BAD_A}):
            self.assertNotIn("unreadable:", obj["name"])
            self.assertNotIn("codec", obj["name"])

    def test_the_name_is_stable_across_runs(self):
        first = self._objects({"dupA.sql": self.BAD_A})[0]["name"]
        second = self._objects({"dupA.sql": self.BAD_A})[0]["name"]
        self.assertEqual(first, second)

    def test_a_nested_path_keeps_the_two_apart(self):
        """The stem alone would collide for two `claim.sql` in different
        schema folders, which is the same defect with a different cause."""
        names = [o["name"] for o in self._objects({
            "schemas/dbo/tables/claim.sql": self.BAD_A,
            "schemas/sales/tables/claim.sql": self.BAD_B})]
        self.assertEqual(len(set(names)), 2, names)

    def test_the_reason_is_recorded_on_the_object(self):
        obj = self._objects({"dupA.sql": self.BAD_A})[0]
        self.assertIn("codec", obj["read_error"])
        self.assertEqual(obj["sql"], "")
        self.assertEqual(obj["kind"], "other")

    def test_a_readable_object_carries_an_empty_read_error(self):
        """Always present, never absent: a key that appears only on failure
        is a key every reader forgets to check."""
        obj = self._objects({"claim.sql": DDL})[0]
        self.assertEqual(obj["read_error"], "")

    def test_the_summary_says_how_many_could_not_be_read(self):
        out = ScanTests()._scan({"dupA.sql": self.BAD_A, "dupB.sql": self.BAD_B,
                                 "claim.sql": DDL})
        self.assertEqual(out["summary"]["unreadable_object_count"], 2)

    def test_the_scan_log_names_each_one(self):
        lines = []
        with TemporaryDirectory() as t:
            root = Path(t)
            _warehouse(root, "AcmeDW", {"dupA.sql": self.BAD_A})
            wh.scan(discover_items(root), log=lines.append)
        self.assertTrue(any("UNREADABLE" in line and "dupA.sql" in line
                            for line in lines), lines)

    def test_the_object_is_still_carried_never_dropped(self):
        out = ScanTests()._scan({"dupA.sql": self.BAD_A, "claim.sql": DDL})
        self.assertEqual(len(out["items"]["warehouses"][0]["objects"]), 2)


if __name__ == "__main__":
    unittest.main()
