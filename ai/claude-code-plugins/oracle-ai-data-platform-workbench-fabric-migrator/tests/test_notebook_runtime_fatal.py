"""The notebook rewrites that corrupted data or killed the notebook at run time.

Four defects, all reproduced through `translate()` on a real Fabric notebook
rather than a bare Python string: the translator keys off Fabric's `# CELL`
markers, so a bare string yields zero findings and zero rewrites and proves
nothing.
"""
import ast
import contextlib
import io
import tokenize
import unittest

from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
# The masker moved to its own module when the inventory catalog needed it
# too; the translator still re-exports the names it uses.
from fabric_aidp.translate import python_text

STAR = "*" * 20
CATALOG = {"tables": {
    "dbo.claim": {"tier": "supplied", "name": "dbo.claim"},
}}


def notebook(body, language="python"):
    """A Fabric notebook whose single code cell holds `body`."""
    return (
        "# Fabric notebook source\n"
        f"\n# METADATA {STAR}\n\n"
        "# META {\n"
        '# META   "dependencies": {\n'
        '# META     "lakehouse": {\n'
        '# META       "default_lakehouse_name": "SalesLake"\n'
        "# META     }\n"
        "# META   }\n"
        "# META }\n"
        f"\n# CELL {STAR}\n\n"
        f"{body}\n"
        f"\n# METADATA {STAR}\n\n"
        "# META {\n"
        f'# META   "language": "{language}"\n'
        "# META }\n")


def translate(body, **kw):
    kw.setdefault("namespace", "ns")
    kw.setdefault("default_lakehouse", "Sales")
    kw.setdefault("catalog", CATALOG)
    return nb2spark.translate(notebook(body), **kw)


def cell(result):
    """Just the code cell body of a translated notebook."""
    text = result.translated_sql
    start = text.index(f"# CELL {STAR}") + len(f"# CELL {STAR}")
    return text[start:text.index(f"# METADATA {STAR}", start)].strip("\n")


def rules(result, severity=None):
    return sorted({f.rule for f in result.findings
                   if severity is None or f.severity == severity})


class StringLiteralMaskingTests(unittest.TestCase):
    """D1: a rule that scans Python source must not see inside a literal.

    Measured before the fix, on this tree:

      in:  note = "we call display(x) in the docs"
      out: note = "we call x.show() in the docs"

    which changes data the user wrote. The same class as the T-SQL bug where
    a table name was rewritten inside a string literal.
    """

    def test_display_inside_a_string_literal_is_left_alone(self):
        body = 'note = "we call display(x) in the docs"'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB04_DISPLAY", rules(result))

    def test_display_in_fstring_text_is_left_alone(self):
        """On 3.12+ the text of an f-string is FSTRING_MIDDLE, not STRING, so
        masking STRING alone would miss it."""
        body = 'name = "x"\nmsg = f"display({name}) and more"'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB04_DISPLAY", rules(result))

    def test_display_in_an_fstring_expression_is_still_code(self):
        """The `{...}` parts of an f-string are ordinary tokens and must stay
        visible: a display() call in there is real code."""
        result = translate('msg = f"val {display(x)} end"')
        self.assertIn("NB04_DISPLAY", rules(result))

    def test_a_triple_quoted_docstring_is_not_rewritten(self):
        """A cell is processed line by line, so line 2 of a multi-line literal
        used to look like ordinary code with a quoted path on it."""
        body = ('doc = """\n'
                'Reads "/lakehouse/default/Files/x.csv" daily.\n'
                'Calls spark.table("dbo.claim") too.\n'
                'Use display(df) to look.\n'
                '"""')
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertEqual(rules(result), [])

    def test_a_table_call_inside_a_literal_is_not_rewritten(self):
        body = 'note = "we call spark.table(\'dbo.claim\') in the docs"'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB10_TABLE_REF", rules(result))

    def test_a_tables_path_inside_a_literal_is_not_rewritten(self):
        body = 'note = "we call spark.read.load(\'/lakehouse/default/Tables/claim\')"'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB20_TABLES_PATH", rules(result))

    def test_a_onelake_path_inside_a_literal_is_not_rewritten(self):
        """An escaped quote resynchronises the old regex scanner mid-literal,
        so the inner quoted run looked like a literal of its own."""
        body = "x = 'it\\'s \"/lakehouse/default/Files/x.csv\"'"
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB01_ONELAKE_PATH", rules(result))

    def test_an_import_inside_a_docstring_is_not_commented_out(self):
        body = 'doc = """\nimport notebookutils as nu\n"""'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB05_UTILS_IMPORT", rules(result))

    def test_a_cell_magic_inside_a_docstring_is_not_stripped(self):
        body = 'doc = """\n%%python\nhello\n"""'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB21_MAGIC_REDUNDANT", rules(result))

    def test_a_run_magic_inside_a_docstring_is_not_flagged(self):
        body = 'doc = """\n%run Other_Notebook\n"""'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB21_RUN_MAGIC", rules(result))

    def test_a_notebookutils_mention_inside_a_literal_is_not_flagged(self):
        result = translate('doc = "call notebookutils.fs.ls to list files"')
        self.assertNotIn("NB03_NOTEBOOKUTILS", rules(result))

    def test_a_display_call_in_a_comment_is_not_rewritten(self):
        """A comment is the user's text too, and it does not run. Measured on
        the bundled estate: `#display(dfDataChanged)` came out as
        `#dfDataChanged.show()`."""
        body = "#display(df)"
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertNotIn("NB04_DISPLAY", rules(result))

    def test_a_quoted_path_in_a_comment_is_not_rewritten(self):
        """Measured on the bundled estate: edkreuk_FMD_FRAMEWORK_004 carries
        `# e.g., "Files/raw" OR "Tables"` and both were reported as
        unmappable OneLake paths."""
        body = '# e.g., "Files/raw" OR "Tables"'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertEqual(rules(result), [])

    def test_real_code_on_the_same_line_as_a_literal_still_runs(self):
        """Masking must not blind the rules to the code around a literal."""
        result = translate(
            'df = spark.table("dbo.claim")  # "display(x)" in here\n'
            'display(df)')
        self.assertIn("default.Sales.claim", cell(result))
        self.assertIn("NB04_DISPLAY", rules(result))


class UntokenizableCellTests(unittest.TestCase):
    """D1: a cell need not tokenize, and `tokenize` raises when it does not.

    Chosen behaviour: flag the cell and leave it exactly as written. Falling
    back to the unmasked scan is the defect itself, and a cell that cannot be
    tokenized cannot run, so a rewrite in it buys nothing and risks silently
    corrupting text.
    """

    def test_an_unterminated_literal_is_flagged_and_nothing_is_rewritten(self):
        body = 'x = "unterminated\ndisplay(y)'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB07_CELL_UNTOKENIZABLE", rules(result, "flag"))
        self.assertNotIn("NB04_DISPLAY", rules(result))

    def test_the_flag_reports_what_was_seen_and_still_hedges(self):
        """It used to hedge unconditionally -- "cannot tell whether the cell
        is malformed or whether splitting the notebook cut one statement
        across two" -- because telling a user their notebook is broken when
        the tool broke it is worse than saying nothing.

        It can tell now (see `SplitStatementTests`), so this case is the
        one where the check came back negative: rejoining the cell with
        what follows does not tokenize either. The message says that, and
        still stops short of a verdict, because the lookahead is bounded
        and a marker can land in a metadata block too."""
        result = translate('x = "unterminated\ndisplay(y)')
        detail = next(f.detail for f in result.findings
                      if f.rule == "NB07_CELL_UNTOKENIZABLE")
        self.assertIn("unterminated string literal", detail)
        self.assertIn("left exactly as written", detail)
        self.assertIn("compare the cell boundary against the original",
                      detail)
        self.assertNotIn("cannot run", detail)

    def test_a_good_cell_beside_a_bad_one_is_still_translated(self):
        source = notebook('display(a)').rstrip("\n") + (
            f"\n\n# CELL {STAR}\n\nx = \"unterminated\ndisplay(b)\n")
        result = nb2spark.translate(source, namespace="ns",
                                    default_lakehouse="Sales")
        self.assertIn("NB07_CELL_UNTOKENIZABLE", rules(result, "flag"))
        self.assertIn("NB04_DISPLAY", rules(result))
        self.assertIn('x = "unterminated', result.translated_sql)


class SplitStatementTests(unittest.TestCase):
    """A `# CELL` marker inside a string literal cuts one statement in two.

    Fabric's `notebook-content.py` format marks a cell boundary with a whole
    line and has no way to escape one, so a notebook that builds notebook
    source in a string literal cannot round-trip through a Git export. The
    parser splits on the marker wherever it appears, and one statement
    becomes two halves that neither tokenize.

    Measured on this tree:

        sql = \"\"\"
        # CELL ********************
        SELECT 1
        \"\"\"
        print(sql)

      before  NB07_CELL_UNTOKENIZABLE, NB07_CELL_UNTOKENIZABLE
      after   NB07_CELL_UNTOKENIZABLE, once, saying which case it is

    It is the sole cause of NB07 in the bundled estate:
    `gbrueckl_Fabric.Toolbox_028` assigns
    `init_script = f\"\"\"# Fabric notebook source ...\"\"\"` with a `# CELL `
    marker inside it, and that one statement produced 2 of the 2 NB07
    findings `make demo` reported.
    """

    SPLIT = ('sql = """\n'
             f'# CELL {STAR}\n'
             'SELECT 1\n'
             '"""\n'
             'print(sql)')

    def flags(self, result):
        return [f for f in result.findings
                if f.rule == "NB07_CELL_UNTOKENIZABLE"]

    def test_the_two_halves_are_one_finding(self):
        result = translate(self.SPLIT)
        self.assertEqual(len(self.flags(result)), 1,
                         [f.detail for f in self.flags(result)])

    def test_the_finding_names_the_cause(self):
        detail = self.flags(translate(self.SPLIT))[0].detail
        self.assertIn("marker line inside a string literal", detail)
        self.assertIn("2 cells", detail)
        self.assertIn("left exactly as written", detail)

    def test_nothing_in_either_half_is_rewritten(self):
        """Nothing is merged: the halves still reach the output byte for
        byte, and the round-trip invariant the whole translator rests on
        is untouched."""
        result = translate(self.SPLIT)
        self.assertIn(f'# CELL {STAR}\nSELECT 1', result.translated_sql)
        self.assertIn('print(sql)', result.translated_sql)

    def test_three_markers_in_one_literal_are_still_one_finding(self):
        body = ('sql = """\n'
                f'# CELL {STAR}\n'
                'a\n'
                f'# CELL {STAR}\n'
                'b\n'
                '"""')
        result = translate(body)
        self.assertEqual(len(self.flags(result)), 1)
        self.assertIn("3 cells", self.flags(result)[0].detail)

    def test_a_cell_that_is_simply_broken_is_not_reported_as_a_split(self):
        result = translate('x = "unterminated\ndisplay(y)')
        self.assertNotIn("marker line inside a string literal",
                         self.flags(result)[0].detail)

    def test_two_independently_broken_cells_are_two_findings(self):
        """The dedup is for one statement in N pieces, not for "two NB07s
        in one notebook". Rejoining these two does not tokenize, so they
        stay two."""
        source = notebook('x = "unterminated\ny = 1').rstrip("\n") + (
            f"\n\n# CELL {STAR}\n\nz = 'also unterminated\nw = 2\n")
        result = nb2spark.translate(source, namespace="ns")
        self.assertEqual(len(self.flags(result)), 2)

    def test_a_notebookutils_surface_inside_the_split_is_still_listed(self):
        """`_flag_utils_usage` skipped a cell it could not mask, so a
        surface inside one of these halves was missing from the NB03 list
        with nothing saying so. The rejoin tokenizes -- that is how the
        split was recognised -- so the surfaces in it can be read even
        though no rule can rewrite them."""
        body = ('sql = """\n'
                f'# CELL {STAR}\n'
                'SELECT 1\n'
                '"""\n'
                'notebookutils.fs.ls("/")')
        result = translate(body)
        surfaces = [f.detail for f in result.findings
                    if f.rule == "NB03_NOTEBOOKUTILS"]
        self.assertIn("notebookutils.fs.ls", " ".join(surfaces))

    def test_a_surface_in_the_second_half_counts_too(self):
        body = ('sql = """\n'
                f'# CELL {STAR}\n'
                'SELECT 1\n'
                '"""\n'
                'mssparkutils.fs.mount("a", "b")')
        result = translate(body)
        surfaces = [f.detail for f in result.findings
                    if f.rule == "NB03_NOTEBOOKUTILS"]
        self.assertIn("mssparkutils.fs.mount", " ".join(surfaces))

    def test_a_truly_unmaskable_cell_says_its_surfaces_are_missing(self):
        """Nothing can be done for that one -- without the literal
        boundaries, `fs.ls("/")` written inside someone's string cannot be
        told from a call -- so the list stays short and the report stops
        being quiet about it."""
        result = translate('notebookutils.fs.ls("/")\nx = "unterminated')
        self.assertEqual([f.rule for f in result.findings
                          if f.rule == "NB03_NOTEBOOKUTILS"], [])
        detail = next(f.detail for f in result.findings
                      if f.rule == "NB07_CELL_UNTOKENIZABLE")
        self.assertIn("missing from the NB03 list", detail)


class MaskerPortabilityTests(unittest.TestCase):
    """The masker must behave the same on every Python the project supports.

    `pyproject.toml` says `requires-python = ">=3.9"`. Two things differ
    across that range and both used to change what the translator did:
    before 3.12 an f-string is one `STRING` token rather than
    FSTRING_START / FSTRING_MIDDLE / FSTRING_END, and before 3.12 the
    tokenizer does not raise on an unterminated single-quoted string -- it
    emits the quote as an `ERRORTOKEN` and carries on. Measured, 3.9.6
    against 3.14.6, before the fix:

      x = f"{spark.table('dbo.claim')}"   3.14 rewritten, 3.9 not
      unterminated literal + display(y)   3.14 NB07, 3.9 rewritten blind

    The assertions below are on the *outcome*, so they pin the same
    behaviour either side of 3.12 rather than testing whichever path this
    interpreter happens to take.
    """

    def test_the_fstring_token_names_are_looked_up_defensively(self):
        """Each is None on 3.9-3.11 and an int from 3.12; either way the
        module imports and the branches guarded by them are simply dead."""
        for name in ("_FSTRING_MIDDLE", "_FSTRING_START", "_FSTRING_END"):
            value = getattr(python_text, name)
            self.assertTrue(value is None or isinstance(value, int), name)
            self.assertEqual(value, getattr(tokenize, name[1:], None))

    def test_an_fstrings_literal_text_is_masked_and_its_code_is_not(self):
        source = 'a = f"display({x}) and display(y)"'
        view = nb2spark.masked_python(source)
        self.assertEqual(len(view.text), len(source))
        self.assertNotIn("display(y)", view.text)      # literal text
        self.assertIn("{x}", view.text)                # expression

    def test_a_call_in_an_fstring_expression_is_rewritten(self):
        """The case the 3.9 / 3.14 split was found on."""
        result = translate("""x = f"{spark.table('dbo.claim')}\"""")
        self.assertIn("default.Sales.claim", cell(result))
        self.assertIn("NB10_TABLE_REF", rules(result, "rewrite"))

    def test_a_literal_inside_an_fstring_expression_is_still_a_literal(self):
        """From 3.12 the tokenizer reports it as its own STRING token;
        before that the splitter has to find it, or a rule could rewrite
        inside it on one interpreter and not the other."""
        view = nb2spark.masked_python("""a = f"{g('display(z)')}\"""")
        self.assertNotIn("display(z)", view.text)
        self.assertIn("g(", view.text)

    def test_a_nested_fstring_still_exposes_its_expression(self):
        view = nb2spark.masked_python("""a = f"{f'{display(z)}'}\"""")
        self.assertIn("display(z)", view.text)

    def test_the_splitter_honours_doubled_braces(self):
        fields = []
        spans = python_text._fstring_parts("a{{b}}c{d}e", 0, fields)
        self.assertEqual(spans, [(0, 7), (10, 11)])
        self.assertEqual(fields, [(8, "d")])

    def test_the_splitter_counts_nested_braces_in_a_format_spec(self):
        fields = []
        python_text._fstring_parts("{x!r:>{w}} tail", 0, fields)
        self.assertEqual(fields, [(1, "x!r:>{w}")])

    def test_an_unbalanced_brace_masks_the_rest(self):
        """Pre-3.12 the tokenizer does not check an f-string's braces, so
        this is reachable. A scan that cannot see where the expression ends
        must not rewrite inside it."""
        fields = []
        spans = python_text._fstring_parts("a{display(b)", 0, fields)
        self.assertEqual(spans, [(0, 1), (1, 12)])
        self.assertEqual(fields, [])

    def test_an_unterminated_single_quoted_string_is_untokenizable(self):
        for source in ('x = "unterminated\ndisplay(y)',
                       "x = 'unterminated\ndisplay(y)",
                       'x = f"unterminated\ndisplay(y)'):
            with self.subTest(source=source):
                with self.assertRaises(nb2spark.Unmaskable):
                    nb2spark.masked_python(source)

    def test_a_shell_escape_is_not_untokenizable(self):
        """`!pip install x` yields an ERRORTOKEN on 3.9 and none on 3.12+,
        so refusing every ERRORTOKEN would trade one version-dependent
        behaviour for another. Only a quote counts."""
        view = nb2spark.masked_python("!pip install fabric\ndisplay(df)")
        self.assertIn("display(df)", view.text)

    def test_the_untokenizable_reason_does_not_name_the_interpreter(self):
        """3.9.6 says "EOF in multi-line string" where 3.14.6 says
        "unterminated triple-quoted f-string literal", at different columns.
        A finding whose text moves with the interpreter cannot be compared
        between runs."""
        reason = nb2spark.untokenizable_reason
        self.assertEqual(reason("EOF in multi-line string"),
                         "an unterminated string literal")
        self.assertEqual(reason("unterminated triple-quoted f-string literal"),
                         "an unterminated string literal")
        self.assertEqual(reason("EOF in multi-line statement"),
                         "an unclosed bracket")

    def test_the_mask_preserves_line_structure(self):
        source = 'a = """one\ntwo\nthree"""'
        view = nb2spark.masked_python(source)
        self.assertEqual(len(view.text), len(source))
        self.assertEqual(view.text.count("\n"), source.count("\n"))

    def test_an_untokenizable_source_raises(self):
        with self.assertRaises(nb2spark.Unmaskable):
            nb2spark.masked_python("a = (1,\nb = 2")


class DisplayShimTests(unittest.TestCase):
    """D2: `display()` is Fabric's builtin, not a Spark method.

    Measured before the fix:

      in:  import pandas as pd
           pdf = pd.DataFrame()
           display(pdf)
      out: pdf.show()   -> AttributeError: 'DataFrame' object has no
                           attribute 'show'

    Fabric renders a Spark DataFrame, a pandas DataFrame and more through the
    one name, so the translation is a shim, not a call-site rewrite.
    """

    def test_a_pandas_frame_is_no_longer_given_show(self):
        body = ('import pandas as pd\n'
                'pdf = pd.DataFrame()\n'
                'display(pdf)')
        # The shim's own docstring quotes `pdf.show()` as the defect it
        # exists for, so this has to look at the user's code, not the file.
        self.assertIn(body, cell(translate(body)))

    def test_the_shim_is_spliced_into_the_notebook(self):
        result = translate("display(df)")
        self.assertIn("def display(", result.translated_sql)

    def test_the_shim_is_spliced_once_for_many_calls(self):
        result = translate("display(a)\ndisplay(b)\ndisplay(c)")
        self.assertEqual(result.translated_sql.count("def display("), 1)

    def test_no_shim_when_the_notebook_never_calls_display(self):
        result = translate("df = spark.table('dbo.claim')")
        self.assertNotIn("def display(", result.translated_sql)

    def test_the_shim_is_defined_before_the_first_call(self):
        out = translate("x = 1\ndisplay(x)").translated_sql
        self.assertLess(out.index("def display("), out.index("display(x)"))

    def test_the_finding_is_kept_so_a_reviewer_learns_of_it(self):
        result = translate("display(df)")
        self.assertIn("NB04_DISPLAY", rules(result, "rewrite"))
        self.assertIn("NB08_DISPLAY_SHIM", rules(result, "rewrite"))

    def test_the_reported_call_survives_an_earlier_rewrite_on_the_line(self):
        """Every other rule in the per-line loop is followed by a re-mask;
        the last one was not, so it sliced the call text out of a mask built
        before NB10 lengthened the line. The notebook came out correct and
        only the report lied -- measured, the call was reported as
        `.claim"); d`."""
        result = translate('x = spark.table("dbo.claim"); display(df)')
        detail = next(f.detail for f in result.findings
                      if f.rule == "NB04_DISPLAY")
        self.assertTrue(detail.startswith("display(df) "), detail)

    def test_a_non_trivial_argument_is_no_longer_flagged(self):
        """The shim dispatches on the object, so there is nothing left for a
        human to decide -- the flag was a consequence of the call-site
        rewrite and goes with it."""
        result = translate("display(df.limit(10))")
        self.assertEqual(rules(result, "flag"), [])
        self.assertIn("NB04_DISPLAY", rules(result, "rewrite"))

    def test_a_display_the_user_defined_themselves_still_wins(self):
        """Their definition comes after ours, so it shadows it -- which is
        what happened in Fabric too, where `display` is a builtin."""
        out = translate("def display(x):\n    print(x)\n\ndisplay(1)").translated_sql
        self.assertLess(out.index("def display(obj"),
                        out.index("def display(x)"))

    def test_the_shim_lands_above_the_cell_that_calls_display(self):
        """In the first Python cell, not the calling one -- see
        ShimPlacementTests for why."""
        source = ("# Fabric notebook source\n"
                  f"\n# CELL {STAR}\n\nx = 1\n"
                  f"\n# CELL {STAR}\n\ndisplay(x)\n")
        out = nb2spark.translate(source, namespace="ns").translated_sql
        cells = out.split(f"# CELL {STAR}")
        self.assertIn("def display(", cells[1])
        self.assertNotIn("def display(", cells[2])

    def test_an_ipynb_notebook_gets_the_shim_too(self):
        import json
        source = json.dumps({"cells": [
            {"cell_type": "code", "source": ["display(df)"], "metadata": {}}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
        result = nb2spark.translate(source, namespace="ns")
        self.assertIn("def display(", result.translated_sql)
        self.assertEqual(json.loads(result.translated_sql)["cells"][0]
                         ["source"][-1], "display(df)")


class ShimPlacementTests(unittest.TestCase):
    """Where the `display` shim goes.

    Two things were wrong with "the top of the cell that makes the first
    call". It could land above a `from __future__` import, which stops the
    cell compiling -- and `ast.parse` does not notice, only `compile` does,
    so a whole-notebook parse gate would not have caught it either:

      cell:  from __future__ import annotations
             display(df)
      out:   SyntaxError: from __future__ imports must occur at the
             beginning of the file

    And it landed *below* a `display` the user had bound in an earlier cell,
    so the tool's rendering silently replaced theirs -- which is this
    defect's own shape one level up.
    """

    def notebook_of(self, *cells):
        body = "".join(f"\n# CELL {STAR}\n\n{c}\n" for c in cells)
        return "# Fabric notebook source\n" + body

    def result(self, *cells):
        return nb2spark.translate(self.notebook_of(*cells), namespace="ns")

    def translated(self, *cells):
        return self.result(*cells).translated_sql

    def test_a_future_import_stays_first(self):
        out = self.translated("from __future__ import annotations\ndisplay(df)")
        compile(out, "<notebook>", "exec")

    def test_a_future_import_below_a_docstring_stays_first(self):
        out = self.translated('"""Doc."""\n'
                              "from __future__ import annotations\n"
                              "display(df)")
        compile(out, "<notebook>", "exec")

    def test_a_parenthesised_future_import_stays_first(self):
        out = self.translated("from __future__ import (\n"
                              "    annotations,\n"
                              ")\n"
                              "display(df)")
        compile(out, "<notebook>", "exec")

    def test_a_future_import_stays_first_when_the_call_is_a_cell_below(self):
        """The import is in cell 1 and the `display()` call in cell 2, so
        the shim's cell is not the calling cell.

        Renamed: this was `test_a_future_import_in_a_later_cell_stays_first`,
        and the import is not in a later cell -- it is in the first one, and
        the *call* is later. "In a later cell" is the case
        `test_a_future_import_in_a_cell_below_the_shim_cell` covers, and that
        one failed until NB31, so the old name read as coverage of a case
        this test does not reach.
        """
        out = self.translated("from __future__ import annotations",
                              "display(df)")
        compile(out, "<notebook>", "exec")

    def test_an_import_in_the_shim_cell_and_another_below_both_stay_first(self):
        """The case the old name promised, in the form that discriminates.

        A `from __future__` in cell 1 *and* another in cell 2. Cell 1's is
        what `_shim_insertion_point` was always shown; cell 2's is what NB31
        exists to find. Satisfying either one alone is not enough, and a
        placement that took the *first* cell holding an import rather than
        the last would put the shim above cell 2's and satisfy only the
        first -- passing
        `test_a_future_import_in_a_cell_below_the_shim_cell`, which has no
        import in cell 1, and this one is what would say so.

        Both cells compiling on their own is what makes the input legal
        before the translation: Fabric compiles each cell separately, and a
        `from __future__` is the first statement of both.
        """
        cells = ("from __future__ import annotations",
                 "from __future__ import annotations\ndisplay(df)")
        compile(self.notebook_of(*cells), "<input>", "exec")
        out = self.translated(*cells)
        compile(out, "<notebook>", "exec")
        # Behind the second one, not merely behind the first.
        self.assertLess(out.rindex("from __future__ import annotations"),
                        out.index("def display(obj"))
        self.assertEqual(out.count("from __future__ import annotations"), 2)

    def test_a_future_import_in_a_cell_below_the_shim_cell(self):
        """NB31. The shim goes in the first Python cell and
        `_shim_insertion_point` gets it behind that cell's `from __future__`
        imports -- but it was never shown any other cell, so an import in
        cell 2 ended up below a `def` the tool had just added.

        The input here compiles as one file: cell 1 holds a comment, and
        comments may precede a `from __future__`. That is what makes this
        the tool's doing and not the notebook's. Measured before the fix:

            output  SyntaxError: from __future__ imports must occur at the
                    beginning of the file
            findings: NB04_DISPLAY, NB08_DISPLAY_SHIM -- nothing about it
        """
        source = self.notebook_of("# set up",
                                  "from __future__ import annotations\n"
                                  "display(df)")
        compile(source, "<input>", "exec")
        out = self.translated("# set up",
                              "from __future__ import annotations\n"
                              "display(df)")
        compile(out, "<notebook>", "exec")

    def test_the_shim_lands_after_that_import_and_not_before_it(self):
        out = self.translated("# set up",
                              "from __future__ import annotations\n"
                              "display(df)")
        self.assertLess(out.index("from __future__ import annotations"),
                        out.index("def display(obj"))

    def test_a_future_import_below_a_docstring_in_a_later_cell(self):
        """A docstring may precede a `from __future__` too, so this input
        also compiles before the tool touches it."""
        source = self.notebook_of('"""Doc."""',
                                  "from __future__ import annotations\n"
                                  "display(df)")
        compile(source, "<input>", "exec")
        compile(self.translated('"""Doc."""',
                                "from __future__ import annotations\n"
                                "display(df)"),
                "<notebook>", "exec")

    def test_the_move_is_reported(self):
        """Silently putting the shim somewhere other than the top of the
        notebook is the sort of thing the report has to carry: the placement
        rule is written down in the module docstring and this is a departure
        from it."""
        result = self.result("# set up",
                             "from __future__ import annotations\n"
                             "display(df)")
        moved = [f for f in result.findings
                 if f.rule == "NB31_DISPLAY_SHIM_AFTER_FUTURE"]
        self.assertEqual(len(moved), 1, [f.rule for f in result.findings])
        self.assertEqual(moved[0].severity, "info")
        self.assertIn("__future__", moved[0].detail)

    def test_nothing_is_reported_when_the_shim_does_not_move(self):
        for cells in ((
                "from __future__ import annotations\ndisplay(df)",),
                ("x = 1", "display(df)"),
                ("from __future__ import annotations", "display(df)")):
            with self.subTest(cells=cells):
                result = self.result(*cells)
                self.assertNotIn("NB31_DISPLAY_SHIM_AFTER_FUTURE",
                                 [f.rule for f in result.findings])

    def test_a_skipped_cell_that_binds_display_is_flagged_not_noted(self):
        """The two placement rules genuinely conflict here and no position
        satisfies both: the shim has to be below the import and above the
        user's binding, and the binding is above the import. It goes below
        -- a SyntaxError stops the notebook running at all, where a shadowed
        helper only renders differently -- and the severity says so."""
        result = self.result("def display(x):\n    print(x)",
                             "from __future__ import annotations\n"
                             "display(1)")
        moved = [f for f in result.findings
                 if f.rule == "NB31_DISPLAY_SHIM_AFTER_FUTURE"]
        self.assertEqual([f.severity for f in moved], ["flag"])
        self.assertIn("binds the name `display`", moved[0].detail)

    def test_a_future_import_quoted_in_a_docstring_is_not_one(self):
        """The scan is on the mask, like every other rule here."""
        result = self.result(
            "# set up",
            '"""Write `from __future__ import annotations` first."""\n'
            "display(df)")
        self.assertNotIn("NB31_DISPLAY_SHIM_AFTER_FUTURE",
                         [f.rule for f in result.findings])
        self.assertIn("def display(obj",
                      result.translated_sql.split(f"# CELL {STAR}")[1])

    def test_a_one_line_docstring_is_still_the_docstring(self):
        out = self.translated('"""Doc."""\ndisplay(df)')
        self.assertEqual(ast.get_docstring(ast.parse(out)), "Doc.")

    def test_a_multi_line_docstring_is_still_the_docstring(self):
        out = self.translated('"""Doc.\n\nMore.\n"""\ndisplay(df)')
        self.assertEqual(ast.get_docstring(ast.parse(out)), "Doc.\n\nMore.")

    def test_the_shim_goes_above_a_user_display_in_an_earlier_cell(self):
        out = self.translated("def display(x):\n    print(x)",
                              "display(1)")
        self.assertLess(out.index("def display(obj"),
                        out.index("def display(x)"))

    def test_the_shim_goes_above_an_ipython_display_import(self):
        out = self.translated("from IPython.display import display",
                              "display(1)")
        self.assertLess(out.index("def display(obj"),
                        out.index("from IPython.display import display"))

    def test_the_shim_goes_in_the_first_python_cell(self):
        out = self.translated("x = 1", "y = 2", "display(x)")
        cells = out.split(f"# CELL {STAR}")
        self.assertIn("def display(obj", cells[1])
        self.assertNotIn("def display(obj", cells[3])

    def test_a_non_python_magic_cell_is_no_place_for_it(self):
        out = self.translated("%%html\n<b>hello</b>", "display(df)")
        cells = out.split(f"# CELL {STAR}")
        self.assertNotIn("def display(obj", cells[1])
        self.assertIn("%%html", cells[1])
        self.assertIn("def display(obj", cells[2])

    def test_a_configure_cell_keeps_its_first_line(self):
        out = self.translated('%%configure\n{"driverMemory": "8g"}',
                              "display(df)")
        cells = out.split(f"# CELL {STAR}")
        self.assertTrue(cells[1].strip().startswith("%%configure"), cells[1])

    def test_a_display_in_a_non_python_magic_cell_is_not_a_call(self):
        """A `%%html` body is markup; `display(x)` in it is text."""
        result = nb2spark.translate(
            self.notebook_of("%%html\n<b>display(x)</b>"), namespace="ns")
        self.assertEqual([f.rule for f in result.findings
                          if f.rule.startswith("NB04")
                          or f.rule.startswith("NB08")], [])
        self.assertNotIn("def display(obj", result.translated_sql)

    def test_a_pyspark_magic_cell_is_still_python(self):
        """rule_magics strips `%%pyspark`, so that cell is ordinary Python
        and a fine place for the shim."""
        out = self.translated("%%pyspark\ndisplay(df)")
        self.assertIn("def display(obj", out)
        self.assertNotIn("%%pyspark", out)


class DisplayShimSourceTests(unittest.TestCase):
    """The shim is held as source text and has to run on a cluster that has
    never heard of this tool, so it is compiled and exercised here -- the same
    treatment `test_m_runtime` gives the M helpers."""

    def _display(self):
        """The shim's `display`, with its fallback print() swallowed.

        There is no IPython in this virtualenv, so the non-Spark branch falls
        through to print() -- which is the behaviour under test, not noise to
        leak into the test output.
        """
        namespace = {}
        exec(compile(nb2spark.DISPLAY_SHIM, "<shim>", "exec"), namespace)
        rendered = namespace["display"]

        def quiet(*args, **kwargs):
            with contextlib.redirect_stdout(io.StringIO()):
                return rendered(*args, **kwargs)

        return quiet

    def test_the_shim_compiles(self):
        ast.parse(nb2spark.DISPLAY_SHIM)

    def test_the_shim_defines_exactly_one_name(self):
        """It has to be called `display`, because the call sites are left as
        Fabric wrote them; so it must not bring anything else along."""
        tree = ast.parse(nb2spark.DISPLAY_SHIM)
        self.assertEqual([node.name for node in tree.body
                          if isinstance(node, ast.FunctionDef)], ["display"])
        self.assertEqual(len(tree.body), 1)

    def test_a_spark_frame_is_shown(self):
        class Sparkish:
            schema = "struct<a:int>"

            def __init__(self):
                self.shown = 0

            def show(self, *a, **k):
                self.shown += 1

        frame = Sparkish()
        self._display()(frame)
        self.assertEqual(frame.shown, 1)

    def test_a_pandas_frame_is_not_shown_and_does_not_raise(self):
        class Pandasish:
            columns = ("a",)

            def __repr__(self):
                return "<pandas>"

        self._display()(Pandasish())      # no AttributeError

    def test_something_with_show_but_no_schema_is_not_shown(self):
        """A matplotlib figure has `show` and no `schema`; calling it would
        open a window instead of rendering the object."""
        class Figure:
            def __init__(self):
                self.shown = 0

            def show(self):
                self.shown += 1

        figure = Figure()
        self._display()(figure)
        self.assertEqual(figure.shown, 0)

    def test_fabric_rendering_options_are_accepted_and_ignored(self):
        class Sparkish:
            schema = "s"

            def show(self, *a, **k):
                self.args = (a, k)

        frame = Sparkish()
        self._display()(frame, summary=True)
        self.assertEqual(frame.args, ((), {}))


class FuseAndLocalApiTests(unittest.TestCase):
    """D3: `/lakehouse/default/...` is Fabric's FUSE mount.

    Measured before the fix:

      in:  p = open("/lakehouse/default/Files/x.csv")
      out: p = open("oci://Sales@ns/Files/x.csv")

    which `open()` cannot read. Spark wants the oci:// URI; a local file API
    wants a local path, and AIDP may have no FUSE mount at all -- so there is
    nothing correct to substitute and the rewrite is refused instead.
    """

    def test_open_keeps_the_fuse_path_and_is_flagged(self):
        body = 'p = open("/lakehouse/default/Files/x.csv")'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))
        self.assertNotIn("NB01_ONELAKE_PATH", rules(result))

    def test_spark_still_gets_the_oci_uri(self):
        result = translate(
            'df = spark.read.parquet("/lakehouse/default/Files/x.parquet")')
        self.assertIn("oci://Sales@ns/Files/x.parquet", cell(result))
        self.assertIn("NB01_ONELAKE_PATH", rules(result, "rewrite"))

    def test_pandas_is_a_local_api(self):
        body = 'pdf = pd.read_csv("/lakehouse/default/Files/x.csv")'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_pathlib_is_a_local_api(self):
        body = 'root = Path("/lakehouse/default/Files")'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_os_path_exists_is_a_local_api(self):
        body = 'if os.path.exists("/lakehouse/default/Files/x.csv"):\n    pass'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_the_innermost_call_is_the_one_that_counts(self):
        body = 'df = spark.read.parquet(open("/lakehouse/default/Files/x.csv"))'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_a_bracket_inside_a_literal_does_not_confuse_the_scan(self):
        body = 'p = open("(", "/lakehouse/default/Files/x.csv")'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_a_string_manipulation_is_not_a_filesystem_call(self):
        """os.path.join only builds a string; it is not where an oci:// URI
        fails, and a joined path often goes on to Spark."""
        result = translate(
            'p = os.path.join("/lakehouse/default/Files", name)')
        self.assertIn("oci://Sales@ns/Files", cell(result))
        self.assertNotIn("NB09_FUSE_LOCAL_API", rules(result))

    def test_an_abfss_path_in_open_is_still_rewritten(self):
        """abfss:// is an object-store location `open()` could never read, so
        that notebook was broken before migration; leaving the Azure URI in
        place would not help anyone."""
        result = translate(
            'p = open("abfss://W@onelake.dfs.fabric.microsoft.com'
            '/Sales.Lakehouse/Files/x")')
        self.assertIn("oci://W_Sales_Lakehouse@ns/Files/x", cell(result))
        self.assertNotIn("NB09_FUSE_LOCAL_API", rules(result))

    def test_a_path_with_no_enclosing_call_is_still_rewritten(self):
        """The documented limit: detection is one line wide and reads the
        enclosing call, so a path bound to a name here and opened three cells
        later still gets the URI. NB33 is what says so on the artifact; see
        `FuseConsumerUnknownTests`."""
        result = translate('p = "/lakehouse/default/Files/x.csv"')
        self.assertIn("oci://Sales@ns/Files/x.csv", cell(result))
        self.assertNotIn("NB09_FUSE_LOCAL_API", rules(result))

    def test_the_flag_names_the_call_and_owns_up_to_its_limits(self):
        result = translate('p = open("/lakehouse/default/Files/x.csv")')
        detail = next(f.detail for f in result.findings
                      if f.rule == "NB09_FUSE_LOCAL_API")
        self.assertIn("open", detail)
        self.assertIn("one line", detail)
        self.assertIn("FUSE", detail)

    def test_a_tables_path_is_untouched_by_this_rule(self):
        """`/lakehouse/default/Tables/` is Spark's, so the local-API check
        must not intercept it.

        It used to come out as an oci:// URI rather than the
        `spark.table()` NB20 is written to produce, because NB01 ran
        before NB20 in `translate` and got there first -- recorded here
        and left alone as "a path to the right data, not a run-time
        failure". The `notebook-paths` batch reverses that: the ordering
        is the reason NB20 never fired anywhere, and `Tables/<schema>/
        <table>` came out with the schema read as a folder, which is not
        a path to the right data. NB20 runs first now. What this test
        still guards is unchanged: NB09 must not claim this line.
        """
        result = translate(
            'df = spark.read.load("/lakehouse/default/Tables/claim")')
        self.assertNotIn("NB09_FUSE_LOCAL_API", rules(result))
        self.assertIn('spark.table("default.Sales.claim")', cell(result))


class FuseConsumerUnknownTests(unittest.TestCase):
    """NB33: the caveat was printed to the wrong reader.

    NB09's detection is lexical and one line wide, and the finding says so
    -- "a path bound to a name here and opened later is still rewritten".
    That sentence lives inside the NB09 finding, which is only raised when
    the detection *succeeded*. So the person who gets a correct refusal is
    told about the limit, and the person whose FUSE path was rewritten
    anyway is told nothing.

    Measured on this tree, bound to Sales:

      with open("/lakehouse/default/Files/x.csv") as fh:
        -> unchanged, NB09_FUSE_LOCAL_API flag, caveat and all
      p = "/lakehouse/default/Files/x.csv"
      with open(p) as fh:
        -> p = "oci://Sales@ns/Files/x.csv"
           NB01_ONELAKE_PATH rewrite, and nothing else

    The second is a notebook that dies on its first read, reported as a
    clean rewrite.
    """

    def flags(self, result):
        return [f for f in result.findings
                if f.rule == "NB33_FUSE_PATH_CONSUMER_UNKNOWN"]

    def test_a_fuse_path_bound_to_a_name_is_flagged(self):
        result = translate('p = "/lakehouse/default/Files/x.csv"\nopen(p)')
        self.assertIn("oci://Sales@ns/Files/x.csv", cell(result))
        self.assertEqual([f.severity for f in self.flags(result)], ["flag"])

    def test_the_flag_names_the_path_and_what_to_do(self):
        detail = self.flags(
            translate('p = "/lakehouse/default/Files/x.csv"'))[0].detail
        self.assertIn("/lakehouse/default/Files/x.csv", detail)
        self.assertIn("oci://Sales@ns/Files/x.csv", detail)
        self.assertIn("no call encloses it", detail)

    def test_a_spark_read_needs_no_flag(self):
        """The call says who reads it and the rewrite is right, so there is
        nothing for a human to follow up."""
        result = translate(
            'df = spark.read.parquet("/lakehouse/default/Files/x.parquet")')
        self.assertEqual(self.flags(result), [])
        self.assertIn("NB01_ONELAKE_PATH", rules(result, "rewrite"))

    def test_a_notebookutils_fs_call_needs_no_flag(self):
        result = translate('fs.ls("/lakehouse/default/Files/x")')
        self.assertEqual(self.flags(result), [])

    def test_a_local_api_call_is_nb09_not_this(self):
        """One finding per path: NB09 refuses the rewrite outright, so
        there is no rewritten path whose reader is in doubt."""
        result = translate('open("/lakehouse/default/Files/x.csv")')
        self.assertEqual(self.flags(result), [])
        self.assertIn("NB09_FUSE_LOCAL_API", rules(result, "flag"))

    def test_only_the_fuse_spelling_qualifies(self):
        """An abfss:// or relative path is an object-store location under
        every reading -- `open()` could never have read one, so rewriting
        it breaks nothing that was working."""
        result = translate(
            'p = "abfss://W@onelake.dfs.fabric.microsoft.com'
            '/Sales.Lakehouse/Files/x"')
        self.assertEqual(self.flags(result), [])

    def test_a_path_only_joined_is_flagged(self):
        """`os.path.join` builds a string and says nothing about the
        reader, which is exactly the doubt this rule reports."""
        result = translate(
            'p = os.path.join("/lakehouse/default/Files", name)')
        self.assertIn("oci://Sales@ns/Files", cell(result))
        self.assertEqual([f.severity for f in self.flags(result)], ["flag"])
        self.assertIn("os.path.join", self.flags(result)[0].detail)


class AzureStoragePathTests(unittest.TestCase):
    """NB34: an Azure Storage location is not OneLake, and saying nothing
    about it made a clean report indistinguishable from a notebook whose
    reads all point outside the migration.

    Not mapping it is the decision PR #27 took and it stands -- Azure
    Storage is not OneLake and this tool maps OneLake. The two URIs differ
    only in the host:

      abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse/Files/x
        -> oci://..., NB01_ONELAKE_PATH
      abfss://c@acct.dfs.core.windows.net/p/x
        -> unchanged, findings: []   (measured, before)

    One letter of host apart, one reported and one silent.
    """

    def flags(self, result):
        return [f for f in result.findings
                if f.rule == "NB34_AZURE_STORAGE_PATH"]

    def test_every_azure_storage_spelling_is_reported(self):
        for uri in ("wasbs://c@acct.blob.core.windows.net/p/x.parquet",
                    "wasb://c@acct.blob.core.windows.net/p",
                    "abfss://c@acct.dfs.core.windows.net/p/x.parquet",
                    "abfs://c@acct.dfs.core.windows.net/p",
                    "adl://acct.azuredatalakestore.net/p"):
            with self.subTest(uri=uri):
                result = translate(f'df = spark.read.parquet("{uri}")')
                self.assertEqual([f.severity for f in self.flags(result)],
                                 ["flag"], uri)
                self.assertIn(uri, cell(result))

    def test_the_finding_does_not_claim_the_read_will_fail(self):
        """Whether an AIDP cluster reaches an Azure Storage account depends
        on its driver and credentials, which this tool cannot see. NB17 was
        just corrected for exactly that kind of confident guess."""
        detail = self.flags(translate(
            'spark.read.parquet("abfss://c@acct.dfs.core.windows.net/p")'
        ))[0].detail
        self.assertIn("this tool cannot see", detail)
        self.assertIn("not OneLake", detail)

    def test_a_onelake_uri_is_not_this(self):
        result = translate(
            'spark.read.parquet("abfss://ws@onelake.dfs.fabric.microsoft.com'
            '/lh.Lakehouse/Files/x")')
        self.assertEqual(self.flags(result), [])
        self.assertIn("NB01_ONELAKE_PATH", rules(result, "rewrite"))

    def test_another_clouds_scheme_is_left_alone(self):
        """The predicate exists because `abfss://` reads as OneLake at a
        glance and is not. No such confusion is possible with `s3://`, and
        widening this to every object store is a different rule."""
        result = translate('spark.read.parquet("s3://bucket/key")')
        self.assertEqual(self.flags(result), [])
        self.assertEqual(rules(result), [])

    def test_a_uri_in_someones_prose_is_not_a_path(self):
        result = translate(
            'note = "we read wasbs://c@acct.blob.core.windows.net/p here"')
        self.assertEqual(self.flags(result), [])

    def test_the_sql_carrier_reports_it_too(self):
        """`rule_sql_paths` had the same silence, and the two carriers have
        drifted apart on a path rule once already (D4)."""
        result = translate(
            'spark.sql("SELECT * FROM parquet.'
            '`wasbs://c@acct.blob.core.windows.net/p`")')
        self.assertEqual([f.severity for f in self.flags(result)], ["flag"])


class FusePredicateTests(unittest.TestCase):
    def test_only_the_fuse_form_is_the_fuse_form(self):
        from fabric_aidp.translate import onelake_to_oci as o
        self.assertTrue(o.is_fuse_path("/lakehouse/default/Files/x.csv"))
        self.assertTrue(o.is_fuse_path("/lakehouse/default"))
        self.assertFalse(o.is_fuse_path("Files/x.csv"))
        self.assertFalse(o.is_fuse_path(
            "abfss://W@onelake.dfs.fabric.microsoft.com/S.Lakehouse/Files/x"))
        self.assertFalse(o.is_fuse_path("/lakehouse/other/Files/x.csv"))
        self.assertFalse(o.is_fuse_path(None))
        self.assertFalse(o.is_fuse_path(""))


class NotebookUtilsUsageTests(unittest.TestCase):
    """D4: commenting out the import leaves every usage a NameError.

    Measured before the fix:

      in:  import notebookutils as nu
           tok = nu.credentials.getToken("x")
      out: # import notebookutils as nu
           tok = nu.credentials.getToken("x")   -> NameError: nu

    notebookutils is Fabric-only, so the run-time failure is unavoidable and
    the notebook genuinely needs a human. What was wrong is that the report
    did not say where the work was: the import was flagged and not one of its
    usage sites, so the error a user hits points at a symptom.
    """

    def surfaces(self, result):
        return [f.detail for f in result.findings
                if f.rule == "NB03_NOTEBOOKUTILS"]

    def test_an_aliased_usage_is_flagged(self):
        result = translate('import notebookutils as nu\n'
                           'tok = nu.credentials.getToken("x")')
        self.assertIn("NB03_NOTEBOOKUTILS", rules(result, "flag"))
        detail = " ".join(self.surfaces(result))
        self.assertIn("nu.credentials.getToken", detail)
        self.assertIn("notebookutils.credentials.getToken", detail)

    def test_the_import_is_still_commented_out(self):
        result = translate('import notebookutils as nu\nnu.fs.ls("/")')
        self.assertIn("# import notebookutils as nu", result.translated_sql)
        self.assertIn("NB05_UTILS_IMPORT", rules(result, "rewrite"))

    def test_a_from_import_binds_the_name_it_imports(self):
        result = translate('from notebookutils import credentials\n'
                           'credentials.getToken("x")')
        self.assertIn("credentials.getToken", " ".join(self.surfaces(result)))

    def test_a_from_import_as_binds_the_alias(self):
        result = translate('from notebookutils import credentials as cr\n'
                           'cr.getToken("x")')
        detail = " ".join(self.surfaces(result))
        self.assertIn("cr.getToken", detail)
        self.assertIn("notebookutils.credentials.getToken", detail)

    def test_a_dotted_submodule_import_is_handled(self):
        body = 'from notebookutils.mssparkutils import fs\nfs.ls("/")'
        result = translate(body)
        self.assertIn("# from notebookutils.mssparkutils import fs",
                      result.translated_sql)
        self.assertIn("fs.ls", " ".join(self.surfaces(result)))

    def test_a_parenthesised_import_is_commented_out_whole(self):
        """Commenting only the first line left `fs,` and `)` behind, which is
        a SyntaxError."""
        result = translate('from notebookutils import (\n'
                           '    fs,\n'
                           '    credentials,\n'
                           ')\n'
                           'fs.ls("/")')
        body = cell(result)
        for line in body.split("\n")[:4]:
            self.assertTrue(line.lstrip().startswith("#"), line)
        ast.parse(body)
        detail = " ".join(self.surfaces(result))
        self.assertIn("fs.ls", detail)

    def test_a_mixed_import_is_refused_by_name(self):
        """`import notebookutils, os` also binds `os`, so commenting the line
        out silently removes an unrelated import."""
        body = 'import notebookutils, os\nos.getcwd()'
        result = translate(body)
        self.assertEqual(cell(result), body)
        refusals = [f for f in result.findings
                    if f.rule == "NB05_UTILS_IMPORT" and f.severity == "flag"]
        self.assertEqual(len(refusals), 1)
        self.assertIn("os", refusals[0].detail)

    def test_a_star_import_is_refused_by_name(self):
        result = translate('from notebookutils import *\nfs.ls("/")')
        self.assertIn("# from notebookutils import *", result.translated_sql)
        detail = " ".join(self.surfaces(result))
        self.assertIn("enumerate", detail)

    def test_an_unimported_notebookutils_is_still_flagged(self):
        """Fabric injects `notebookutils` and `mssparkutils` without an
        import, so those two names are always in scope."""
        result = translate('mssparkutils.fs.mount("a", "b")')
        self.assertIn("mssparkutils.fs.mount", " ".join(self.surfaces(result)))

    def test_each_surface_is_reported_once_with_a_count(self):
        result = translate('import notebookutils as nu\n'
                           'nu.fs.ls("/a")\n'
                           'nu.fs.ls("/b")\n'
                           'nu.fs.cp("/a", "/b")')
        details = self.surfaces(result)
        self.assertEqual(len(details), 2)
        self.assertIn("2 uses", " ".join(details))

    def test_an_alias_bound_in_one_cell_is_seen_in_another(self):
        source = ("# Fabric notebook source\n"
                  f"\n# CELL {STAR}\n\nimport notebookutils as nu\n"
                  f"\n# CELL {STAR}\n\nnu.fs.ls(\"/\")\n")
        result = nb2spark.translate(source, namespace="ns")
        self.assertIn("nu.fs.ls", " ".join(self.surfaces(result)))

    def test_the_alias_on_the_import_line_is_not_itself_a_usage(self):
        result = translate('import notebookutils as nu\nx = 1')
        self.assertEqual(self.surfaces(result), [])

    def test_an_alias_inside_a_string_is_not_a_usage(self):
        result = translate('import notebookutils as nu\n'
                           'doc = "nu.fs.ls is what we used"')
        self.assertEqual(self.surfaces(result), [])

    def test_a_bare_reference_to_the_module_is_flagged(self):
        result = translate('import notebookutils as nu\nhandle = nu')
        self.assertIn("nu (notebookutils)", " ".join(self.surfaces(result)))

    def test_a_sql_cell_is_not_scanned_for_python_names(self):
        """`notebookutils` in a `%%sql` body is a column name."""
        result = translate("%%sql\nSELECT notebookutils.fs FROM t")
        self.assertEqual(self.surfaces(result), [])

    def test_an_import_inside_a_docstring_is_not_an_import(self):
        body = 'doc = """\nfrom notebookutils import (\n    fs,\n)\n"""'
        result = translate(body)
        self.assertEqual(cell(result), body)
        self.assertEqual(rules(result), [])


class NotebookUtilsScopeTests(unittest.TestCase):
    """NB03 reports a surface a human has to replace. A local that merely
    shares the import's name is not one, and noise on a flag that matters is
    how a flag stops being read.

    Measured before the fix:

      from notebookutils import fs
      def g():
          fs = open("x")
          return fs.read()

      findings: NB05_UTILS_IMPORT rewrite,
                NB03_NOTEBOOKUTILS flag, NB03_NOTEBOOKUTILS flag

    -- two flags, and neither `fs` inside `g` is Fabric's.
    """

    def surfaces(self, result):
        return [f.detail for f in result.findings
                if f.rule == "NB03_NOTEBOOKUTILS"]

    def test_a_local_that_shadows_the_import_is_not_a_surface(self):
        result = translate('from notebookutils import fs\n'
                           'def g():\n'
                           '    fs = open("x")\n'
                           '    return fs.read()')
        self.assertEqual(self.surfaces(result), [])
        self.assertIn("NB05_UTILS_IMPORT", rules(result, "rewrite"))

    def test_a_parameter_that_shadows_the_import_is_not_a_surface(self):
        result = translate('from notebookutils import fs\n'
                           'def g(fs):\n'
                           '    return fs.read()')
        self.assertEqual(self.surfaces(result), [])

    def test_a_comprehension_target_shadows_it_too(self):
        result = translate('from notebookutils import fs\n'
                           'xs = [fs for fs in range(3)]')
        self.assertEqual(self.surfaces(result), [])

    def test_a_with_as_target_shadows_it(self):
        result = translate('from notebookutils import fs\n'
                           'def g():\n'
                           '    with open("x") as fs:\n'
                           '        return fs.read()')
        self.assertEqual(self.surfaces(result), [])

    def test_a_real_use_inside_a_function_is_still_a_surface(self):
        """The direction that must not regress: a surface not reported is a
        call into Fabric nobody was told about."""
        result = translate('from notebookutils import fs\n'
                           'def g():\n'
                           '    return fs.ls("/x")')
        self.assertIn("fs.ls", " ".join(self.surfaces(result)))

    def test_global_puts_the_import_back(self):
        """`global fs` says the name means the module-level binding, which
        is the import."""
        result = translate('from notebookutils import fs\n'
                           'def g():\n'
                           '    global fs\n'
                           '    return fs.ls("/x")')
        self.assertIn("fs.ls", " ".join(self.surfaces(result)))

    def test_a_shadow_in_one_function_does_not_hide_a_use_in_another(self):
        result = translate('from notebookutils import fs\n'
                           'def a():\n'
                           '    fs = open("x")\n'
                           '    return fs.read()\n'
                           'def b():\n'
                           '    return fs.ls("/x")')
        self.assertEqual(len(self.surfaces(result)), 1)
        self.assertIn("fs.ls", " ".join(self.surfaces(result)))

    def test_a_cell_that_does_not_parse_still_reports_its_surfaces(self):
        """A Fabric cell holding `!pip install x` is ordinary and tokenizes;
        it does not parse. Losing a whole cell's surfaces to that would be
        the silent direction, so the text scan still runs -- scope-blind,
        which is what it was before and is still better than nothing."""
        result = translate('from notebookutils import fs\n'
                           '!pip install foo\n'
                           'fs.ls("/x")')
        self.assertIn("fs.ls", " ".join(self.surfaces(result)))

    def test_a_rebinding_is_not_itself_a_use(self):
        """`fs = open("x")` at module level is a store, not a call into
        Fabric. The text scan counted it as a surface."""
        result = translate('from notebookutils import fs\n'
                           'fs = open("x")')
        self.assertEqual(self.surfaces(result), [])


if __name__ == "__main__":
    unittest.main()
