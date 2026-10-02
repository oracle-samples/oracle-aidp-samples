import unittest

from fabric_aidp.translate import m_nav, m_parser
from fabric_aidp.translate import m_to_pyspark


def _steps(*rows):
    """rows are (name, fn, nav_fragments, args)."""
    out = []
    for name, fn, nav, args in rows:
        out.append({"name": name, "fn": fn, "nav": list(nav), "args": list(args),
                    "inputs": [], "raw": ""})
    return out


# Shape B, observed in 006.pq -- one navigation hop per step.
CHAIN_PER_STEP = _steps(
    ("Pattern", "Lakehouse.Contents", [], ["[CreateNavigationProperties = false]"]),
    ("Navigation_1", "Pattern", ['{[workspaceId = "ws-1"]}', "[Data]"], []),
    ("Navigation_2", "Navigation_1", ['{[lakehouseId = "lh-1"]}', "[Data]"], []),
    ("TableNavigation", "Navigation_2",
     ['{[Id = "DimDateCustom", ItemKind = "Table"]}?', "[Data]?"], []),
)

# Shape A, observed in 004.pq -- navigation collapsed onto the source step.
CHAIN_COLLAPSED = _steps(
    ("Source", "Lakehouse.Contents",
     ['{[workspaceId = "ws-1"]}', "[Data]", '{[lakehouseId = "lh-1"]}', "[Data]"], ["[]"]),
    ('#"Navigation 1"', "Source",
     ['{[Id = "gold_dimvideos", ItemKind = "Table"]}', "[Data]"], []),
)

# Observed in 031.pq -- a FILE under Files/, not a table; Csv.Document consumes it.
CHAIN_FILE = _steps(
    ("Source", "Lakehouse.Contents", [], ["[]"]),
    ("Navigation", "Source", ['{[workspaceId = "ws-1"]}', "[Data]"], []),
    ('#"Navigation 1"', "Navigation", ['{[lakehouseId = "lh-1"]}', "[Data]"], []),
    ('#"Navigation 2"', '#"Navigation 1"', ['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"], []),
    ('#"Navigation 3"', '#"Navigation 2"', ['{[Name = "DimDate.csv"]}', "[Content]"], []),
    ('#"Imported CSV"', "Csv.Document", [], ['#"Navigation 3"']),
)


class FoldChainTests(unittest.TestCase):
    def test_one_hop_per_step_chain_finds_the_table(self):
        read = m_nav.fold_lakehouse_chain(CHAIN_PER_STEP, 0)
        self.assertEqual(read.kind, "table")
        self.assertEqual(read.table, "DimDateCustom")
        self.assertEqual(read.lakehouse_id, "lh-1")
        self.assertEqual(read.workspace_id, "ws-1")

    def test_one_hop_per_step_chain_consumes_every_navigation_step(self):
        read = m_nav.fold_lakehouse_chain(CHAIN_PER_STEP, 0)
        self.assertEqual(read.consumed,
                         ["Pattern", "Navigation_1", "Navigation_2", "TableNavigation"])

    def test_collapsed_chain_finds_the_same_table(self):
        read = m_nav.fold_lakehouse_chain(CHAIN_COLLAPSED, 0)
        self.assertEqual(read.kind, "table")
        self.assertEqual(read.table, "gold_dimvideos")
        self.assertEqual(read.consumed, ["Source", '#"Navigation 1"'])

    def test_files_navigation_is_a_file_read_not_a_table(self):
        read = m_nav.fold_lakehouse_chain(CHAIN_FILE, 0)
        self.assertEqual(read.kind, "file")
        self.assertEqual(read.file, "DimDate.csv")
        self.assertEqual(read.folder, "Files")
        self.assertIsNone(read.table)

    def test_file_chain_stops_before_the_csv_document_call(self):
        read = m_nav.fold_lakehouse_chain(CHAIN_FILE, 0)
        self.assertNotIn('#"Imported CSV"', read.consumed)

    def test_bare_lakehouse_contents_is_unresolved_not_a_table(self):
        steps = _steps(("Source", "Lakehouse.Contents", [], ["[]"]))
        self.assertEqual(m_nav.fold_lakehouse_chain(steps, 0).kind, "unresolved")

    def test_file_name_with_a_space_survives(self):
        steps = _steps(
            ("Source", "Lakehouse.Contents", [], []),
            ("N1", "Source", ['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"], []),
            ("N2", "N1", ['{[Name = "global risk 2026 2.csv"]}', "[Content]"], []))
        self.assertEqual(m_nav.fold_lakehouse_chain(steps, 0).file,
                         "global risk 2026 2.csv")

    def test_lakehouse_read_indexes_finds_every_source(self):
        steps = CHAIN_PER_STEP + CHAIN_COLLAPSED
        self.assertEqual(m_nav.lakehouse_read_indexes(steps), [0, 4])


def _files_chain(*hops):
    """Lakehouse root -> Files, then `hops` (nav fragment lists), one per step."""
    rows = [("Source", "Lakehouse.Contents", [], ["[]"]),
            ("N1", "Source", ['{[lakehouseId = "lh-1"]}', "[Data]"], []),
            ("N2", "N1", ['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"], [])]
    for i, nav in enumerate(hops, start=3):
        rows.append(("N%d" % i, "N%d" % (i - 1), list(nav), []))
    return _steps(*rows)


def _name(value):
    return ['{[Name = "%s"]}' % value, "[Content]"]


def _folder(value):
    return ['{[Id = "%s", ItemKind = "Folder"]}' % value, "[Data]"]


def _csv_over(steps):
    """The chain plus the `Csv.Document` the corpus always puts over it.

    031.pq ends its `Files/` navigation with
    `#"Imported CSV" = Csv.Document(#"Navigation 3")` -- see `CHAIN_FILE`
    above and its note -- and so does every lakehouse file read in the
    corpora vendored here, 2 of 2. Without it the query's value is the
    `[Content]` binary rather than a table, and `m_to_pyspark` refuses the
    query instead of inventing a CSV reader for it, so a test about the
    *path* has to carry the document function to reach the reader at all.
    """
    return steps + _steps(
        ("Imported", "Csv.Document", [], [steps[-1]["name"]]))


class NestedFolderTests(unittest.TestCase):
    """H14. Only the first Folder Id and the last Name reached the path.

    Measured on the probe export (m_pl_probe, F01 and F05): `Files` > Name
    "raw" > Name "orders_2024.csv" emitted `.csv('oci://lh@ns/Files/raw')`,
    which reads every file in the folder under a PASS; `Files` > Id "raw"
    (Folder) > Name "orders_2024.csv" emitted `.../Files/orders_2024.csv`,
    a file that is not there. Both are `Files/raw/orders_2024.csv`.
    """

    def _path(self, steps):
        read = m_nav.fold_lakehouse_chain(steps, 0)
        self.assertEqual(read.kind, "file")
        return "%s/%s" % (read.folder, read.file)

    def test_a_folder_navigated_by_name_stays_on_the_path(self):
        # F01: the folder is a Name hop, like the file.
        steps = _files_chain(_name("raw"), _name("orders_2024.csv"))
        self.assertEqual(self._path(steps), "Files/raw/orders_2024.csv")

    def test_a_second_folder_id_stays_on_the_path(self):
        # F05: the folder is an Id/ItemKind hop, like Files.
        steps = _files_chain(_folder("raw"), _name("orders_2024.csv"))
        self.assertEqual(self._path(steps), "Files/raw/orders_2024.csv")

    def test_deeper_nesting_keeps_every_hop_in_order(self):
        steps = _files_chain(_folder("raw"), _name("2024"), _folder("q1"),
                             _name("eu"), _name("orders.csv"))
        self.assertEqual(self._path(steps), "Files/raw/2024/q1/eu/orders.csv")

    def test_hops_collapsed_onto_one_step_keep_their_order(self):
        steps = _files_chain(_name("raw") + _name("sub") + _name("f.csv"))
        self.assertEqual(self._path(steps), "Files/raw/sub/f.csv")

    def test_a_name_holding_a_bracket_is_still_a_literal(self):
        steps = _files_chain(_name("raw"), _name("data[1].csv"))
        self.assertEqual(self._path(steps), "Files/raw/data[1].csv")

    def test_a_folder_only_chain_stays_unresolved_not_a_directory_read(self):
        # No Name/[Content] hop: nothing says which file. It was refused
        # before this change and still is -- never widened to the directory.
        # `folder` is None, not "Files/raw": an unresolved read has no
        # folder/file split, and a value here would be one nobody can use.
        read = m_nav.fold_lakehouse_chain(_files_chain(_folder("raw")), 0)
        self.assertEqual(read.kind, "unresolved")
        self.assertIsNone(read.folder)

    def test_an_unresolved_chain_never_splices_the_file_into_the_folder(self):
        # Before: an unexpected key zeroed the file name but `folder` was
        # still the join of every hop, 'Files/f.csv' -- the file as a
        # directory.
        steps = _files_chain(
            ['{[Id = "raw", ItemKind = "Folder", X = 1]}', "[Data]"],
            _name("f.csv"))
        read = m_nav.fold_lakehouse_chain(steps, 0)
        self.assertEqual(read.kind, "unresolved")
        self.assertIsNone(read.file)
        self.assertIsNone(read.folder)

    def test_an_unexpected_key_is_reported_as_a_key_not_as_a_non_literal(self):
        # Before: "... X = 1] is not a literal Name or Folder Id" -- every
        # value in that selector IS a literal; the key is what is wrong.
        steps = _files_chain(
            ['{[Id = "raw", ItemKind = "Folder", X = 1]}', "[Data]"],
            _name("f.csv"))
        reason = m_nav.fold_lakehouse_chain(steps, 0).reason
        self.assertIn("has key X", reason)
        self.assertNotIn("literal", reason)

    def test_an_unexpected_key_on_a_name_selector_is_named(self):
        steps = _files_chain(['{[Name = "f.csv", Kind = "File"]}', "[Content]"])
        reason = m_nav.fold_lakehouse_chain(steps, 0).reason
        self.assertIn("has key Kind", reason)
        self.assertNotIn("literal", reason)

    def test_a_missing_or_reordered_key_is_reported_as_the_shape(self):
        for nav in (['{[ItemKind = "Folder"]}', "[Data]"],
                    ['{[ItemKind = "Folder", Id = "raw"]}', "[Data]"]):
            with self.subTest(nav=nav):
                reason = m_nav.fold_lakehouse_chain(
                    _files_chain(nav, _name("f.csv")), 0).reason
                self.assertIn("does not have the shape of a Folder selector",
                              reason)
                self.assertNotIn("literal", reason)

    def test_a_non_literal_value_is_reported_as_a_non_literal(self):
        for nav, key, value in (
                (["{[Name = FileName]}", "[Content]"], "Name", "FileName"),
                (['{[Id = Dir, ItemKind = "Folder"]}', "[Data]"], "Id", "Dir")):
            with self.subTest(nav=nav):
                reason = m_nav.fold_lakehouse_chain(
                    _files_chain(nav, _name("f.csv")), 0).reason
                self.assertIn("selects %s by %s, which is not a literal string"
                              % (key, value), reason)
                self.assertNotIn("key", reason)

    def test_a_non_literal_name_is_unresolved_not_skipped(self):
        # Skipping `{[Name = FileName]}` would read `Files/raw` again.
        steps = _files_chain(_name("raw"), ["{[Name = FileName]}", "[Content]"])
        read = m_nav.fold_lakehouse_chain(steps, 0)
        self.assertEqual(read.kind, "unresolved")
        self.assertIn("FileName", read.reason)

    def test_a_non_literal_folder_id_is_unresolved_not_skipped(self):
        steps = _files_chain(['{[Id = Dir, ItemKind = "Folder"]}', "[Data]"],
                             _name("f.csv"))
        read = m_nav.fold_lakehouse_chain(steps, 0)
        self.assertEqual(read.kind, "unresolved")
        self.assertIn("Dir", read.reason)

    def test_the_table_chain_is_untouched(self):
        self.assertEqual(m_nav.fold_lakehouse_chain(CHAIN_PER_STEP, 0).table,
                         "DimDateCustom")

    def _translate(self, steps):
        query = {"name": "Q", "attrs": None, "steps": steps,
                 "final": steps[-1]["name"]}
        return m_to_pyspark.translate_query(query, lakehouses={"lh-1": "lh"},
                                            namespace="ns")

    def test_the_reader_and_the_m14_detail_carry_the_full_path(self):
        for hops in ((_name("raw"), _name("orders_2024.csv")),
                     (_folder("raw"), _name("orders_2024.csv"))):
            with self.subTest(hops=hops):
                result = self._translate(_csv_over(_files_chain(*hops)))
                self.assertIn(".csv('oci://lh@ns/Files/raw/orders_2024.csv')",
                              result.translated_sql)
                m14 = [f for f in result.findings
                       if f.rule == "M14_SOURCE_LAKEHOUSE_FILE"]
                self.assertEqual(len(m14), 1)
                self.assertIn("Files/raw/orders_2024.csv", m14[0].detail)

    def test_a_non_literal_hop_blocks_the_query_with_its_reason(self):
        result = self._translate(_files_chain(["{[Name = FileName]}", "[Content]"]))
        self.assertEqual(result.translated_sql, "")
        blocked = [f for f in result.findings if f.rule == "M90_UNSUPPORTED_STEP"]
        self.assertEqual(len(blocked), 1)
        self.assertIn("FileName", blocked[0].detail)

    @unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
    def test_the_probe_shapes_through_the_parser(self):
        head = ('section S; shared Q = let '
                'Source = Lakehouse.Contents([CreateNavigationProperties = false]), '
                'N1 = Source{[workspaceId = "ws-1"]}[Data], '
                'N2 = N1{[lakehouseId = "lh-1"]}[Data], '
                'N3 = N2{[Id = "Files", ItemKind = "Folder"]}[Data], ')
        tail = ('N5 = N4{[Name = "orders_2024.csv"]}[Content], '
                'Csv = Csv.Document(N5, [Delimiter = ","]) in Csv;')
        for n4 in ('N4 = N3{[Name = "raw"]}[Content], ',
                   'N4 = N3{[Id = "raw", ItemKind = "Folder"]}[Data], '):
            with self.subTest(n4=n4):
                queries = m_parser.parse_text(head + n4 + tail)["queries"]
                result = m_to_pyspark.translate_query(
                    queries[0], lakehouses={"lh-1": "lh"}, namespace="ns")
                self.assertIn(".csv('oci://lh@ns/Files/raw/orders_2024.csv')",
                              result.translated_sql)


ATTRS_REPLACE = ('[DataDestinations = {[Definition = [Kind = "Reference", '
                 'QueryName = "DimDateCustom_DataDestination", IsNewTarget = true], '
                 'Settings = [Kind = "Manual", UpdateMethod = [Kind = "Replace"], '
                 'TypeSettings = [Kind = "Table"]]]}]')
ATTRS_AUTOMATIC = ('[DataDestinations = {[Definition = [Kind = "Reference", '
                   'QueryName = "DimDateCustom_DataDestination"], '
                   'Settings = [Kind = "Automatic"]]}]')
HELPER = {"name": "DimDateCustom_DataDestination", "attrs": None,
          "steps": CHAIN_PER_STEP, "final": "TableNavigation"}


class DestinationTests(unittest.TestCase):
    def _resolve(self, attrs, helper_name="DimDateCustom_DataDestination"):
        helper = dict(HELPER, name=helper_name)
        query = {"name": "DimDateCustom", "attrs": attrs, "steps": [], "final": ""}
        return m_nav.resolve_destination(query, {helper_name: helper})

    def test_replace_becomes_overwrite(self):
        dest = self._resolve(ATTRS_REPLACE)
        self.assertEqual(dest.mode, "overwrite")
        self.assertEqual(dest.table, "DimDateCustom")
        self.assertEqual(dest.kind, "table")

    def test_append_becomes_append(self):
        dest = self._resolve(ATTRS_REPLACE.replace('"Replace"', '"Append"'))
        self.assertEqual(dest.mode, "append")

    def test_absent_update_method_defaults_to_overwrite(self):
        self.assertEqual(self._resolve(ATTRS_AUTOMATIC).mode, "overwrite")

    def test_quoted_helper_name_still_resolves(self):
        attrs = ATTRS_REPLACE.replace("DimDateCustom_DataDestination",
                                      "squirrel-data csv_DataDestination")
        dest = self._resolve(attrs, helper_name='#"squirrel-data csv_DataDestination"')
        self.assertEqual(dest.table, "DimDateCustom")

    def test_no_attrs_means_no_destination(self):
        self.assertIsNone(m_nav.resolve_destination(
            {"name": "T", "attrs": None, "steps": []}, {}))

    def test_non_lakehouse_destination_is_reported_with_its_connector(self):
        helper = {"name": "X_DataDestination", "attrs": None, "steps": _steps(
            ("Source", "FabricSql.Contents", [], ["null"])), "final": "Source"}
        query = {"name": "X", "attrs": ATTRS_REPLACE.replace(
            "DimDateCustom_DataDestination", "X_DataDestination"), "steps": []}
        dest = m_nav.resolve_destination(query, {"X_DataDestination": helper})
        self.assertEqual(dest.kind, "unsupported")
        self.assertEqual(dest.connector, "FabricSql.Contents")

    def test_missing_helper_is_reported_not_crashed(self):
        query = {"name": "X", "attrs": ATTRS_REPLACE, "steps": []}
        dest = m_nav.resolve_destination(query, {})
        self.assertEqual(dest.kind, "unresolved")


# The document attribute, observed verbatim at line 1 of 015, 020, 023, 031
# and 035.pq -- the only five corpus files that carry a default destination.
SECTION_DEFAULT = (
    '[DefaultOutputDestinationSettings = [DestinationDefinition = '
    '[Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], '
    'UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], '
    'StagingDefinition = [Kind = "FastCopy"]]')
# `shared DefaultDestination = Lakehouse.Contents(...){...}[Data]{...}[Data];`
# is not a `let`, so the parser hands it back with no steps and the whole
# expression in `raw`. Both corpus shapes of it look exactly like this.
DEFAULT_DESTINATION = {
    "name": "DefaultDestination", "attrs": None, "steps": [],
    "raw": ('Lakehouse.Contents([EnableFolding = false])'
            '{[workspaceId = "ws-1"]}[Data]{[lakehouseId = "lh-1"]}[Data]'),
    "note": "unsupported member kind RecursivePrimaryExpression"}
BIND = "[BindToDefaultDestination = true]"


class DefaultDestinationTests(unittest.TestCase):
    """`[BindToDefaultDestination = true]` -- 9 corpus queries carry it and
    nothing else saying where they write."""

    def _resolve(self, attrs=BIND, section=SECTION_DEFAULT, name="courses",
                 members=None):
        members = {"DefaultDestination": DEFAULT_DESTINATION} if members is None \
            else members
        query = {"name": name, "attrs": attrs, "steps": [], "final": ""}
        return m_nav.resolve_destination(query, members, section_attrs=section)

    def test_a_bound_query_resolves_to_a_destination(self):
        self.assertIsNotNone(self._resolve())

    def test_the_table_is_the_query_s_own_name(self):
        # Fabric's default destination creates one table per query, named
        # after the query. Corroborated inside 035.pq, the one corpus file
        # holding both shapes: its only explicitly-destined query, `students`,
        # resolves to lakehouse e513c730 table `students` -- the same
        # lakehouse `DefaultDestination` names, and the query's own name.
        self.assertEqual(self._resolve().table, "courses")

    def test_the_lakehouse_comes_from_the_default_destination_query(self):
        self.assertEqual(self._resolve().lakehouse_id, "lh-1")

    def test_it_is_marked_as_derived_not_declared(self):
        self.assertTrue(self._resolve().default_bound)

    def test_a_quoted_query_name_becomes_an_unquoted_table(self):
        self.assertEqual(self._resolve(name='#"DimDate csv"').table, "DimDate csv")

    def test_the_section_update_method_sets_the_mode(self):
        self.assertEqual(
            self._resolve(section=SECTION_DEFAULT.replace('"Replace"', '"Append"',
                                                          1)).mode,
            "append")

    def test_bind_false_is_not_a_destination(self):
        self.assertIsNone(self._resolve(attrs="[BindToDefaultDestination = false]"))

    def test_no_document_attribute_refuses_rather_than_guessing(self):
        dest = self._resolve(section="")
        self.assertEqual(dest.kind, "default_unresolved")
        self.assertIn("DefaultOutputDestinationSettings", dest.reason)

    def test_a_default_destination_query_not_in_the_export_refuses(self):
        dest = self._resolve(members={})
        self.assertEqual(dest.kind, "default_unresolved")
        self.assertIn("DefaultDestination", dest.reason)

    def test_a_default_destination_with_no_lakehouse_id_refuses(self):
        member = dict(DEFAULT_DESTINATION,
                      raw='Lakehouse.Contents([EnableFolding = false])')
        dest = self._resolve(members={"DefaultDestination": member})
        self.assertEqual(dest.kind, "default_unresolved")

    def test_a_non_lakehouse_default_destination_names_its_connector(self):
        member = dict(DEFAULT_DESTINATION,
                      raw='FabricSql.Contents(null){[sqlId = "s-1"]}[Data]')
        dest = self._resolve(members={"DefaultDestination": member})
        self.assertEqual(dest.kind, "default_unresolved")
        self.assertIn("FabricSql.Contents", dest.reason)

    def test_a_non_table_destination_type_refuses(self):
        section = SECTION_DEFAULT.replace(
            'DestinationTypeSettings = [Kind = "Table"]',
            'DestinationTypeSettings = [Kind = "Files"]')
        self.assertEqual(self._resolve(section=section).kind, "default_unresolved")

    def test_a_declared_destination_beats_the_default(self):
        # 020.pq has both shapes in one file; the member's own attribute wins.
        helper = dict(HELPER)
        query = {"name": "DimDateCustom", "attrs": ATTRS_REPLACE, "steps": [],
                 "final": ""}
        dest = m_nav.resolve_destination(
            query, {"DimDateCustom_DataDestination": helper,
                    "DefaultDestination": DEFAULT_DESTINATION},
            section_attrs=SECTION_DEFAULT)
        self.assertFalse(dest.default_bound)
        self.assertEqual(dest.table, "DimDateCustom")

    def test_a_default_destination_query_written_as_a_let_also_resolves(self):
        member = {"name": "DefaultDestination", "attrs": None, "raw": "",
                  "final": "Navigation_2", "steps": _steps(
                      ("Pattern", "Lakehouse.Contents", [], ["[]"]),
                      ("Navigation_1", "Pattern",
                       ['{[workspaceId = "ws-9"]}', "[Data]"], []),
                      ("Navigation_2", "Navigation_1",
                       ['{[lakehouseId = "lh-9"]}', "[Data]"], []))}
        dest = self._resolve(members={"DefaultDestination": member})
        self.assertEqual(dest.lakehouse_id, "lh-9")


# 033.pq's Fact_Messages attribute, cut to three columns. It is the one
# mapping in the corpus that renames -- `Date` -> `Message_Date` -- out of
# 136 mappings over 8 destinations in 6 files (009, 011, 017, 026, 031, 033).
def _mapping_attrs(pairs, dynamic="false"):
    records = ", ".join('[SourceColumnName = "%s", DestinationColumnName = "%s"]'
                        % pair for pair in pairs)
    return ('[DataDestinations = {[Definition = [Kind = "Reference", '
            'QueryName = "DimDateCustom_DataDestination", IsNewTarget = false], '
            'Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = '
            '[Mappings = {%s}], DynamicSchema = %s, '
            'UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]'
            % (records, dynamic))


MAPPING = [("Thread_ID", "Thread_ID"), ("Date", "Message_Date"),
           ("Sender", "Sender")]


class ColumnMappingTests(unittest.TestCase):
    def _resolve(self, attrs):
        query = {"name": "DimDateCustom", "attrs": attrs, "steps": [], "final": ""}
        return m_nav.resolve_destination(
            query, {"DimDateCustom_DataDestination": HELPER})

    def test_the_mapping_is_read_in_the_destination_s_order(self):
        self.assertEqual(self._resolve(_mapping_attrs(MAPPING)).column_map,
                         MAPPING)

    def test_a_destination_with_no_column_settings_has_no_mapping(self):
        self.assertIsNone(self._resolve(ATTRS_REPLACE).column_map)

    def test_the_dynamic_schema_flag_is_carried(self):
        self.assertFalse(self._resolve(_mapping_attrs(MAPPING)).dynamic_schema)
        self.assertTrue(
            self._resolve(_mapping_attrs(MAPPING, "true")).dynamic_schema)

    def test_column_settings_that_yield_no_pairs_are_not_read_as_no_mapping(self):
        # An empty mapping is not "write everything"; it is a mapping this
        # code could not read, and the two must not look the same.
        dest = self._resolve(_mapping_attrs([]))
        self.assertEqual(dest.column_map, [])


class HelperSuppressionTests(unittest.TestCase):
    def test_destination_helper_is_recognised(self):
        self.assertTrue(m_nav.is_destination_helper("DimDateCustom_DataDestination"))

    def test_quoted_destination_helper_is_recognised(self):
        self.assertTrue(m_nav.is_destination_helper('#"squirrel csv_DataDestination"'))

    def test_ordinary_query_is_not_a_helper(self):
        self.assertFalse(m_nav.is_destination_helper("DimDateCustom"))


if __name__ == "__main__":
    unittest.main()
