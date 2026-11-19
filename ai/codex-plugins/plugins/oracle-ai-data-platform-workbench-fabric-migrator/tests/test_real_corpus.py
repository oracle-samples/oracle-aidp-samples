import json
import unittest
from pathlib import Path

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate.ipynb_format import parse_ipynb, serialize_ipynb
from fabric_aidp.translate.notebook_format import parse_any, serialize_any

CORPUS = sorted((Path(__file__).resolve().parent / "fixtures" / "real").glob("*.ipynb"))


class CorpusPresenceTests(unittest.TestCase):
    def test_the_corpus_is_vendored(self):
        self.assertGreaterEqual(len(CORPUS), 6)

    def test_attribution_is_present(self):
        notice = (Path(__file__).resolve().parent / "fixtures" / "real" / "NOTICE")
        text = notice.read_text(encoding="utf-8")
        self.assertIn("Microsoft", text)
        self.assertIn("MIT", text)

    def test_both_formatting_styles_are_represented(self):
        starts = {p.read_text(encoding="utf-8")[:2] for p in CORPUS}
        self.assertIn('{"', starts, "no minified notebook in the corpus")
        self.assertIn("{\n", starts, "no indented notebook in the corpus")


class RealRoundTripTests(unittest.TestCase):
    def test_every_real_notebook_round_trips_byte_exactly(self):
        for path in CORPUS:
            with self.subTest(notebook=path.name):
                raw = path.read_text(encoding="utf-8")
                self.assertEqual(serialize_ipynb(parse_ipynb(raw)), raw)

    def test_parse_any_dispatches_to_the_json_parser(self):
        for path in CORPUS:
            with self.subTest(notebook=path.name):
                raw = path.read_text(encoding="utf-8")
                self.assertEqual(serialize_any(parse_any(raw)), raw)

    def test_parse_any_still_handles_the_py_format(self):
        source = ("# Fabric notebook source\n\n"
                  "# CELL ********************\n\nx = 1\n")
        self.assertEqual(serialize_any(parse_any(source)), source)


class RealTranslationTests(unittest.TestCase):
    def test_every_real_notebook_translates_without_crashing(self):
        for path in CORPUS:
            with self.subTest(notebook=path.name):
                result = nb2spark.translate(path.read_text(encoding="utf-8"),
                                            namespace="acmens")
                self.assertIsInstance(result.translated_sql, str)

    def test_an_untranslated_notebook_is_returned_unchanged(self):
        """No rule should fire on a notebook with nothing to rewrite."""
        for path in CORPUS:
            raw = path.read_text(encoding="utf-8")
            result = nb2spark.translate(raw, namespace="acmens")
            if result.changes == 0:
                with self.subTest(notebook=path.name):
                    self.assertEqual(result.translated_sql, raw)

    def test_output_stays_valid_json(self):
        for path in CORPUS:
            with self.subTest(notebook=path.name):
                out = nb2spark.translate(path.read_text(encoding="utf-8"),
                                         namespace="acmens").translated_sql
                json.loads(out)

    def test_lakehouse_bindings_are_read_from_json_metadata(self):
        bound = [p for p in CORPUS
                 if parse_ipynb(p.read_text(encoding="utf-8")).default_lakehouse]
        self.assertGreaterEqual(len(bound), 4)


if __name__ == "__main__":
    unittest.main()
