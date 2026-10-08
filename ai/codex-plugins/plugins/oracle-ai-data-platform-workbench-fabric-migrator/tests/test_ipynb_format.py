import json
import unittest

from fabric_aidp.translate.ipynb_format import (
    IpynbParseError, detect_style, parse_ipynb, serialize_ipynb,
)


def _nb(cells, **meta):
    base = {"kernel_info": {"name": "synapse_pyspark"},
            "language_info": {"name": "python"}}
    base.update(meta)
    return {"cells": cells, "metadata": base, "nbformat": 4, "nbformat_minor": 5}


CODE = {"cell_type": "code", "id": "a1", "metadata": {},
        "source": ["df = spark.table(\"claims\")\n", "df.show()"], "outputs": [],
        "execution_count": None}
MD = {"cell_type": "markdown", "id": "m1", "metadata": {}, "source": ["# Title"]}
INDENTED = json.dumps(_nb([MD, CODE]), indent=2) + "\n"
MINIFIED = json.dumps(_nb([MD, CODE]), separators=(",", ":"))


class ParseTests(unittest.TestCase):
    def test_blocks_mirror_the_py_parser_shape(self):
        nb = parse_ipynb(INDENTED)
        self.assertEqual([b.kind for b in nb.blocks], ["MARKDOWN", "CELL"])

    def test_code_blocks_exposes_only_code(self):
        nb = parse_ipynb(INDENTED)
        self.assertEqual(len(nb.code_blocks), 1)
        self.assertIn("spark.table", "\n".join(nb.code_blocks[0].lines))

    def test_source_is_split_into_lines_without_trailing_newlines(self):
        nb = parse_ipynb(INDENTED)
        self.assertEqual(nb.code_blocks[0].lines,
                         ['df = spark.table("claims")', "df.show()"])

    def test_source_given_as_a_plain_string_is_accepted(self):
        raw = json.dumps(_nb([{"cell_type": "code", "source": "x = 1\ny = 2",
                               "metadata": {}}]))
        self.assertEqual(parse_ipynb(raw).code_blocks[0].lines, ["x = 1", "y = 2"])

    def test_notebook_meta_is_the_metadata_object(self):
        self.assertEqual(parse_ipynb(INDENTED).notebook_meta["language_info"]["name"],
                         "python")

    def test_default_lakehouse_from_dependencies(self):
        raw = json.dumps(_nb([CODE], dependencies={
            "lakehouse": {"default_lakehouse_name": "SalesLake"}}))
        self.assertEqual(parse_ipynb(raw).default_lakehouse, "SalesLake")

    def test_default_lakehouse_falls_back_to_the_guid(self):
        raw = json.dumps(_nb([CODE], dependencies={
            "lakehouse": {"default_lakehouse": "guid-1"}}))
        self.assertEqual(parse_ipynb(raw).default_lakehouse, "guid-1")

    def test_absent_lakehouse_binding_is_none(self):
        self.assertIsNone(parse_ipynb(INDENTED).default_lakehouse)

    def test_meta_for_returns_the_cell_metadata(self):
        raw = json.dumps(_nb([{"cell_type": "code", "source": ["x=1"],
                               "metadata": {"language": "python"}}]))
        nb = parse_ipynb(raw)
        self.assertEqual(nb.meta_for(nb.code_blocks[0])["language"], "python")

    def test_not_json_is_a_parse_error(self):
        with self.assertRaises(IpynbParseError):
            parse_ipynb("# Fabric notebook source\n")

    def test_json_without_cells_is_a_parse_error(self):
        with self.assertRaises(IpynbParseError):
            parse_ipynb('{"metadata": {}}')

    def test_non_string_input_is_a_parse_error(self):
        with self.assertRaises(IpynbParseError):
            parse_ipynb(None)


class StyleTests(unittest.TestCase):
    def test_detects_indented_with_trailing_newline(self):
        style = detect_style(INDENTED, json.loads(INDENTED))
        self.assertEqual(style["kwargs"].get("indent"), 2)
        self.assertEqual(style["trailing"], "\n")

    def test_detects_minified(self):
        style = detect_style(MINIFIED, json.loads(MINIFIED))
        self.assertEqual(style["kwargs"].get("separators"), (",", ":"))
        self.assertEqual(style["trailing"], "")

    def test_unmatched_style_falls_back_and_says_so(self):
        odd = "   " + INDENTED
        style = detect_style(odd, json.loads(INDENTED))
        self.assertFalse(style["exact"])


class RoundTripTests(unittest.TestCase):
    def test_indented_round_trips_byte_exactly(self):
        self.assertEqual(serialize_ipynb(parse_ipynb(INDENTED)), INDENTED)

    def test_minified_round_trips_byte_exactly(self):
        self.assertEqual(serialize_ipynb(parse_ipynb(MINIFIED)), MINIFIED)

    def test_editing_a_cell_changes_only_that_cell(self):
        nb = parse_ipynb(INDENTED)
        nb.code_blocks[0].lines = ["df = spark.table('x')"]
        out = json.loads(serialize_ipynb(nb))
        self.assertEqual(out["cells"][1]["source"], ["df = spark.table('x')"])
        self.assertEqual(out["cells"][0]["source"], ["# Title"])

    def test_an_edited_multiline_cell_keeps_its_newlines(self):
        import ast
        nb = parse_ipynb(INDENTED)
        nb.code_blocks[0].lines = ["# load", "df = spark.table('x')", "df.show()", ""]
        source = json.loads(serialize_ipynb(nb))["cells"][1]["source"]
        self.assertEqual(source, ["# load\n", "df = spark.table('x')\n", "df.show()\n"])
        self.assertEqual(len(ast.parse("".join(source)).body), 2)

    def test_edits_preserve_the_detected_style(self):
        nb = parse_ipynb(MINIFIED)
        nb.code_blocks[0].lines = ["z = 3"]
        self.assertTrue(serialize_ipynb(nb).startswith('{"cells"'))

    def test_unedited_multiline_source_keeps_its_newline_split(self):
        out = json.loads(serialize_ipynb(parse_ipynb(INDENTED)))
        self.assertEqual(out["cells"][1]["source"], CODE["source"])


if __name__ == "__main__":
    unittest.main()
