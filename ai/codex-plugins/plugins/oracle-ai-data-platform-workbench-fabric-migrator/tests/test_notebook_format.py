import unittest

from fabric_aidp.translate.notebook_format import (
    NotebookParseError, check_meta, parse_notebook, serialize_notebook,
)

SAMPLE = (
    "# Fabric notebook source\n"
    "\n"
    "# METADATA ********************\n"
    "\n"
    "# META {\n"
    '# META   "kernel_info": {\n'
    '# META     "name": "synapse_pyspark"\n'
    "# META   },\n"
    '# META   "dependencies": {\n'
    '# META     "lakehouse": {\n'
    '# META       "default_lakehouse": "2925655f-0293-4f32-8bc6-86ab989099a7",\n'
    '# META       "default_lakehouse_name": "Sales"\n'
    "# META     }\n"
    "# META   }\n"
    "# META }\n"
    "\n"
    "# MARKDOWN ********************\n"
    "\n"
    "# # Heading\n"
    "# body text\n"
    "\n"
    "# CELL ********************\n"
    "\n"
    'df = spark.read.parquet("abfss://x")\n'
    "\n"
    "# METADATA ********************\n"
    "\n"
    "# META {\n"
    '# META   "language": "python",\n'
    '# META   "language_group": "synapse_pyspark"\n'
    "# META }\n"
)


class ParseTests(unittest.TestCase):
    def test_block_kinds_in_order(self):
        nb = parse_notebook(SAMPLE)
        self.assertEqual([b.kind for b in nb.blocks],
                         ["HEADER", "METADATA", "MARKDOWN", "CELL", "METADATA"])

    def test_notebook_meta_is_parsed_json(self):
        nb = parse_notebook(SAMPLE)
        self.assertEqual(nb.notebook_meta["kernel_info"]["name"], "synapse_pyspark")

    def test_default_lakehouse_name_is_reachable(self):
        nb = parse_notebook(SAMPLE)
        lh = nb.notebook_meta["dependencies"]["lakehouse"]
        self.assertEqual(lh["default_lakehouse_name"], "Sales")

    def test_code_blocks_exposes_only_cells(self):
        nb = parse_notebook(SAMPLE)
        self.assertEqual(len(nb.code_blocks), 1)
        self.assertIn("spark.read.parquet", "\n".join(nb.code_blocks[0].lines))

    def test_meta_for_returns_the_following_metadata_block(self):
        nb = parse_notebook(SAMPLE)
        self.assertEqual(nb.meta_for(nb.code_blocks[0])["language"], "python")

    def test_crlf_input_is_detected(self):
        nb = parse_notebook(SAMPLE.replace("\n", "\r\n"))
        self.assertEqual(nb.newline, "\r\n")
        self.assertEqual([b.kind for b in nb.blocks],
                         ["HEADER", "METADATA", "MARKDOWN", "CELL", "METADATA"])

    def test_missing_trailing_newline_is_recorded(self):
        self.assertTrue(parse_notebook(SAMPLE).trailing_newline)
        self.assertFalse(parse_notebook(SAMPLE.rstrip("\n")).trailing_newline)

    def test_marker_inside_a_string_literal_is_not_a_marker(self):
        text = (
            "# Fabric notebook source\n"
            "\n"
            "# CELL ********************\n"
            "\n"
            's = "# CELL ********************"\n'
        )
        nb = parse_notebook(text)
        self.assertEqual([b.kind for b in nb.blocks], ["HEADER", "CELL"])

    def test_empty_metadata_block_yields_empty_dict(self):
        text = (
            "# Fabric notebook source\n"
            "\n"
            "# METADATA ********************\n"
            "\n"
        )
        self.assertEqual(parse_notebook(text).notebook_meta, {})




from fabric_aidp.translate.notebook_format import serialize_notebook


class RoundTripTests(unittest.TestCase):
    CASES = {
        "full": SAMPLE,
        "crlf": SAMPLE.replace("\n", "\r\n"),
        "no_trailing_newline": SAMPLE.rstrip("\n"),
        "header_only": "# Fabric notebook source\n",
        "blank_lines_between_cells": (
            "# Fabric notebook source\n"
            "\n"
            "\n"
            "# CELL ********************\n"
            "\n"
            "\n"
            "x = 1\n"
            "\n"
            "\n"
        ),
        "consecutive_markers": (
            "# Fabric notebook source\n"
            "# CELL ********************\n"
            "# CELL ********************\n"
            "y = 2\n"
        ),
        "unicode": (
            "# Fabric notebook source\n"
            "\n"
            "# CELL ********************\n"
            "\n"
            'label = "café — naïve"\n'
        ),
    }

    # Every character `str.splitlines()` breaks on that `\n` is not. The
    # parser used `splitlines()` and the serializer rejoins with the file's
    # own terminator, so each of these came back as a newline and the
    # byte-exact invariant this module rests on was broken. Measured,
    # `x = "a\x0bb"` came out as `x = "a` + newline + `b"` -- a raw newline
    # inside a single-quoted literal, which is a SyntaxError, so the
    # corruption is not cosmetic. A form feed between two functions is
    # ordinary Python;   or \x85 inside a literal is ordinary data.
    OTHER_LINE_BREAKS = {
        "vertical_tab": "\x0b", "form_feed": "\x0c", "file_separator": "\x1c",
        "group_separator": "\x1d", "record_separator": "\x1e",
        "next_line": "\x85", "line_separator": " ",
        "paragraph_separator": " ", "lone_carriage_return": "\r",
    }

    def test_round_trip_is_byte_identical(self):
        for name, text in self.CASES.items():
            with self.subTest(case=name):
                self.assertEqual(serialize_notebook(parse_notebook(text)), text)

    def test_a_character_python_calls_a_line_break_survives_a_round_trip(self):
        for name, char in self.OTHER_LINE_BREAKS.items():
            text = ("# Fabric notebook source\n"
                    "\n"
                    "# CELL ********************\n"
                    "\n"
                    'x = "a%sb"\n' % char)
            with self.subTest(case=name):
                self.assertEqual(serialize_notebook(parse_notebook(text)), text)

    def test_a_file_ending_in_a_lone_carriage_return_does_not_gain_a_newline(self):
        text = ("# Fabric notebook source\n\n# CELL ********************\n\n"
                "x = 1\r")
        self.assertEqual(serialize_notebook(parse_notebook(text)), text)

    def test_mixed_line_endings_are_reproduced_rather_than_normalised(self):
        text = ("# Fabric notebook source\r\n\r\n"
                "# CELL ********************\r\n\r\nx = 1\n")
        self.assertEqual(serialize_notebook(parse_notebook(text)), text)

    def test_editing_a_cell_changes_only_that_cell(self):
        nb = parse_notebook(SAMPLE)
        nb.code_blocks[0].lines = ["df = spark.table('claims')"]
        out = serialize_notebook(nb)
        self.assertIn("df = spark.table('claims')", out)
        self.assertNotIn("abfss://x", out)
        self.assertIn("# # Heading", out)
        self.assertIn('# META   "language": "python",', out)

    def test_serialize_preserves_crlf_when_editing(self):
        nb = parse_notebook(SAMPLE.replace("\n", "\r\n"))
        nb.code_blocks[0].lines = ["z = 3"]
        out = serialize_notebook(nb)
        self.assertIn("\r\n", out)
        self.assertNotIn("\n\n\r", out)


class SqlNotebookMarkerTests(unittest.TestCase):
    """`notebook-content.sql` uses `--` for the same structure. Knowing only
    `#`, the parser found no header and reported the whole file unparseable --
    so a T-SQL notebook migrated as an empty notebook."""

    STAR = "*" * 20

    def _src(self, comment):
        return (f"{comment} Fabric notebook source\n\n"
                f"{comment} CELL {self.STAR}\n\nSELECT 1\n\n"
                f"{comment} METADATA {self.STAR}\n\n"
                f'{comment} META {{\n{comment} META   "language": "sparksql"\n'
                f"{comment} META }}\n")

    def test_a_dash_comment_notebook_parses(self):
        doc = parse_notebook(self._src("--"))
        self.assertEqual(len(doc.code_blocks), 1)

    def test_its_language_is_read_from_the_following_metadata(self):
        doc = parse_notebook(self._src("--"))
        self.assertEqual(doc.language_for(doc.code_blocks[0]), "sparksql")

    def test_round_trip_is_byte_exact_for_both_comment_styles(self):
        for comment in ("#", "--"):
            with self.subTest(comment=comment):
                source = self._src(comment)
                self.assertEqual(serialize_notebook(parse_notebook(source)), source)

    def test_the_header_says_which_kind_of_notebook_this_is(self):
        """A T-SQL notebook's cells carry no `%%sql` -- they are bare SQL --
        so the header is the only thing that says the file is SQL at all."""
        self.assertTrue(parse_notebook(self._src("--")).sql_source)
        self.assertFalse(parse_notebook(self._src("#")).sql_source)


class SynapseHeaderTests(unittest.TestCase):
    """Fabric grew out of Synapse and still exports Synapse-era notebooks
    under the older header. The block structure is identical, so rejecting
    the file over one word threw the whole notebook away as unparseable.
    Found by running a corpus of 32 real notebooks through the translator.
    """

    STAR = "*" * 20

    def _src(self, header):
        return (f"{header}\n\n# METADATA {self.STAR}\n\n"
                '# META {\n# META   "synapse": {\n# META     "lakehouse": {\n'
                '# META       "default_lakehouse_name": "casadinpadure"\n'
                "# META     }\n# META   }\n# META }\n\n"
                f"# CELL {self.STAR}\n\ndf = spark.table('t')\n")

    def test_a_synapse_notebook_parses(self):
        doc = parse_notebook(self._src("# Synapse Analytics notebook source"))
        self.assertEqual(len(doc.code_blocks), 1)

    def test_a_fabric_notebook_still_parses(self):
        doc = parse_notebook(self._src("# Fabric notebook source"))
        self.assertEqual(len(doc.code_blocks), 1)

    def test_lakehouse_is_read_from_the_synapse_block(self):
        doc = parse_notebook(self._src("# Synapse Analytics notebook source"))
        self.assertEqual(doc.default_lakehouse, "casadinpadure")

    def test_round_trip_stays_byte_exact(self):
        source = self._src("# Synapse Analytics notebook source")
        self.assertEqual(serialize_notebook(parse_notebook(source)), source)

    def test_a_genuinely_foreign_file_is_still_rejected(self):
        with self.assertRaises(NotebookParseError):
            parse_notebook("# Some other tool's notebook\n\nprint(1)\n")


class CheckMetaLabelTests(unittest.TestCase):
    """Which block `check_meta` blames.

    It named a bad block "the notebook-level METADATA block" whenever no
    METADATA block had decoded before it -- so in a notebook with no
    notebook-level block at all, cell 1's own block was reported as the
    notebook's. The notebook-level block is the one before the first cell.
    """

    def _error(self, text):
        with self.assertRaises(NotebookParseError) as caught:
            check_meta(parse_notebook(text))
        return str(caught.exception)

    def test_a_bad_block_after_cell_1_with_no_notebook_level_block_is_cell_1s(self):
        message = self._error("# Fabric notebook source\n\n"
                              "# CELL ********************\n\nx = 1\n\n"
                              "# METADATA ********************\n\n"
                              "# META {bad}\n")
        self.assertIn("(in the METADATA block of cell 1)", message)
        self.assertNotIn("notebook-level", message)

    def test_a_bad_block_before_any_cell_is_the_notebook_level_one(self):
        message = self._error("# Fabric notebook source\n\n"
                              "# METADATA ********************\n\n"
                              "# META {bad}\n\n"
                              "# CELL ********************\n\nx = 1\n")
        self.assertIn("(in the notebook-level METADATA block)", message)

    def test_a_bad_block_after_a_good_notebook_level_one_is_the_cells(self):
        message = self._error("# Fabric notebook source\n\n"
                              "# METADATA ********************\n\n"
                              "# META {}\n\n"
                              "# CELL ********************\n\nx = 1\n\n"
                              "# METADATA ********************\n\n"
                              "# META {bad}\n")
        self.assertIn("(in the METADATA block of cell 1)", message)


if __name__ == "__main__":
    unittest.main()
