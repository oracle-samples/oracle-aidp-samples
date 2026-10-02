"""The Dataflow CSV reader: the header row, the schema, and Csv.Document's options.

Four defects, every one of them silent data loss on a PASS. The reader was
written as one fixed string -- `header=True, inferSchema=True` -- and both
halves of that string are wrong for what M's `Csv.Document` actually does.

The measurements quoted in the assertions below were taken on Spark 4.2.0
against a two-row headerless CSV whose first field is `007`; they are
recorded in the comments so a later reader does not have to retake them.
"""
import unittest

from fabric_aidp.translate import m_parser
from fabric_aidp.translate import m_to_pyspark as m

requires_node = unittest.skipUnless(
    m_parser.parser_available(), "Node + mparse not installed")

CATALOG = {"lh-1": "sales"}
# What each QuoteStyle emits. Both quote with `"` and read `""` as a literal
# quote; the style decides only whether a quoted line break ends the row.
_QUOTING = '.option("quote", \'"\').option("escape", \'"\')'
QUOTE_NONE = _QUOTING + '.option("multiLine", False)'
QUOTE_CSV = _QUOTING + '.option("multiLine", True)'


def _step(name, fn, args=(), nav=(), inputs=(), raw=""):
    return {"name": name, "fn": fn, "args": list(args), "nav": list(nav),
            "inputs": list(inputs), "raw": raw}


# A lakehouse *file* read: the second of the two code paths that ends in
# `spark.read...csv(...)`. Everything asserted about the Web.Contents path
# has to hold here too -- that is D4.
#
# `FILE_SOURCE` is the navigation chain ALONE, whose value in M is the
# `[Content]` binary. It is not a CSV read and no longer translates as one:
# with nothing over it the query is refused, so every test below whose claim
# is about the *reader* uses `FILE_SOURCE_CSV`, the chain with the
# `Csv.Document` the corpus always puts over it -- 031.pq's
# `#"Imported CSV" = Csv.Document(#"Navigation 3")`, and both of the two
# lakehouse file reads in the corpora vendored here.
FILE_SOURCE = [
    _step("Src", "Lakehouse.Contents", ["[]"]),
    _step("N1", "Src", nav=['{[lakehouseId = "lh-1"]}', "[Data]"]),
    _step("N2", "N1", nav=['{[Id = "Files", ItemKind = "Folder"]}', "[Data]"]),
    _step("N3", "N2", nav=['{[Name = "DimPort.csv"]}', "[Content]"]),
]

URL = "https://h/x.csv"


def _translate(*steps, **kwargs):
    steps = list(steps)
    query = dict({"name": "Q", "attrs": None, "steps": steps,
                  "final": steps[-1]["name"]}, **kwargs)
    return m.translate_query(query, lakehouses=CATALOG, namespace="ns")


def _web(options=None, name="Source"):
    args = ['Web.Contents("%s")' % URL] + ([options] if options else [])
    return _step(name, "Csv.Document", args)


def _file(options=None, name="Imported"):
    return _step(name, "Csv.Document", ["N3"] + ([options] if options else []),
                 inputs=["N3"])


# The shape the corpus has: chain, then Csv.Document. See FILE_SOURCE above.
FILE_SOURCE_CSV = FILE_SOURCE + [_file()]


def _promote(source, name="Promoted"):
    return _step(name, "Table.PromoteHeaders", [source], inputs=[source])


def _parse_one(text):
    queries = m_parser.parse_text(text)["queries"]
    return m.translate_query(queries[0],
                             queries_by_name={q["name"]: q for q in queries},
                             lakehouses=CATALOG, namespace="ns")


# --------------------------------------------------------------------------
# D1 -- header=True was forced, and a headerless CSV lost its first row


class HeaderFollowsPromoteHeadersTests(unittest.TestCase):
    """`Table.PromoteHeaders` is the whole signal, and it was ignored.

    In M, `Csv.Document` on its own returns a table whose columns are
    `Column1..ColumnN` and whose *first row is data*. A header row exists
    only where a later `Table.PromoteHeaders` says so.

    Measured on Spark 4.2.0, two-row headerless file:

        header=True   -> columns ['007', '2024-01-05'], ONE row survives
        header=False  -> columns ['_c0', '_c1'], BOTH rows, '007' intact

    So the forced `header=True` deleted row 1 of every headerless CSV and
    renamed the frame's columns after it.
    """

    def test_a_web_csv_with_no_promote_headers_is_read_headerless(self):
        sql = _translate(_web()).translated_sql
        self.assertIn('.option("header", False)', sql)
        self.assertNotIn('.option("header", True)', sql)

    def test_a_web_csv_with_promote_headers_keeps_the_header(self):
        sql = _translate(_web(), _promote("Source")).translated_sql
        self.assertIn('.option("header", True)', sql)

    def test_a_lakehouse_file_with_no_promote_headers_is_read_headerless(self):
        # The chain plus its Csv.Document and no promotion. This used to run
        # on the chain alone, which is a binary and not a CSV read at all --
        # the wrong fixture for a claim about the header option.
        sql = _translate(*FILE_SOURCE_CSV).translated_sql
        self.assertIn('.option("header", False)', sql)
        self.assertNotIn('.option("header", True)', sql)

    def test_a_lakehouse_file_with_promote_headers_keeps_the_header(self):
        # The corpus shape: file -> Csv.Document -> Table.PromoteHeaders.
        sql = _translate(*(FILE_SOURCE + [_file(), _promote("Imported")])
                         ).translated_sql
        self.assertIn('.option("header", True)', sql)

    def test_a_promote_headers_behind_a_buffer_still_sets_the_header(self):
        # Table.Buffer returns its input, so the promotion is still the
        # reader's -- the alias chain the fold already tracked. Buffered over
        # the Csv.Document, because `Table.Buffer` of a binary is not a shape
        # M permits.
        sql = _translate(*(FILE_SOURCE_CSV + [
            _step("Held", "Table.Buffer", ["Imported"], inputs=["Imported"]),
            _promote("Held")])).translated_sql
        self.assertIn('.option("header", True)', sql)

    def test_a_promotion_that_is_not_the_readers_does_not_set_the_header(self):
        # Csv -> SelectRows -> PromoteHeaders. The promotion is over a
        # filtered frame, which is a real operation with no Spark reader to
        # fold into, so the query is refused. It must not instead reach back
        # and flip the reader's header on -- that would promote the header
        # of the *unfiltered* file.
        result = _translate(
            _web(),
            _step("Kept", "Table.SelectRows", ["Source", "each true"],
                  inputs=["Source"]),
            _promote("Kept"))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_a_headerless_read_names_the_column_naming_divergence(self):
        # M calls a headerless CSV's columns Column1..ColumnN; Spark calls
        # them _c0.._cN. Nothing here can rename them without knowing how
        # many there are, so it is flagged rather than left unsaid.
        findings = [f for f in _translate(_web()).findings
                    if f.rule == "M30_CSV_NO_HEADER"]
        self.assertEqual([f.severity for f in findings], ["flag"])
        self.assertIn("_c0", findings[0].detail)
        self.assertIn("Column1", findings[0].detail)

    def test_a_promoted_read_does_not_carry_the_no_header_flag(self):
        rules = [f.rule for f in
                 _translate(_web(), _promote("Source")).findings]
        self.assertNotIn("M30_CSV_NO_HEADER", rules)

    def test_the_fold_finding_no_longer_claims_a_header_that_is_not_read(self):
        # `Csv.Document folded into the reader, which already takes the
        # first row as the header` was emitted even with no promotion in the
        # query, describing the corrupting behaviour as if it were correct.
        detail = [f.detail for f in
                  _translate(*(FILE_SOURCE + [_file()])).findings
                  if f.rule == "M11_SOURCE_CSV"]
        self.assertEqual(len(detail), 1)
        self.assertNotIn("first row as the header", detail[0])


# --------------------------------------------------------------------------
# D2 -- inferSchema=True was forced, and leading zeros were destroyed


class SchemaIsNotInferredTests(unittest.TestCase):
    """M's CSV reader returns text. Types arrive later, from the query.

    `Csv.Document` produces all-text columns; a `Table.TransformColumnTypes`
    downstream is where a column becomes a number or a date. Asking Spark to
    guess instead is not the same operation. Measured on Spark 4.2.0:

        inferSchema=True   -> '007' becomes the integer 7
        inferSchema=False  -> '007' stays the string '007'

    Product codes, ZIP codes, account numbers and phone numbers all lose
    their leading zeros, and the `TransformColumnTypes` cast that follows
    runs on the already-corrupted value, so it cannot put them back.
    """

    def test_a_web_csv_does_not_ask_spark_to_guess(self):
        sql = _translate(_web()).translated_sql
        self.assertIn('.option("inferSchema", False)', sql)
        self.assertNotIn('.option("inferSchema", True)', sql)

    def test_a_lakehouse_file_does_not_ask_spark_to_guess(self):
        sql = _translate(*FILE_SOURCE_CSV).translated_sql
        self.assertIn('.option("inferSchema", False)', sql)
        self.assertNotIn('.option("inferSchema", True)', sql)

    def test_inference_is_refused_explicitly_not_merely_omitted(self):
        # `inferSchema` defaults to false in Spark, so leaving it out would
        # read the same. It is written out because it is a decision: the
        # generated file should say that the types come from M's
        # TransformColumnTypes and not from the file.
        self.assertIn("inferSchema", _translate(_web()).translated_sql)

    def test_the_types_still_arrive_from_transform_column_types(self):
        # The whole point: text out of the reader, typed by the query.
        sql = _translate(
            _web(), _promote("Source"),
            _step("Typed", "Table.TransformColumnTypes",
                  ["Promoted", '{{"n", Int64.Type}}'], inputs=["Promoted"])
        ).translated_sql
        self.assertIn('.option("inferSchema", False)', sql)
        # Int64.Type is not a cast: it rounds half to even, as Int64.From
        # does, and returns NULL outside bigint's range. Both live in the
        # generated `_m_int64`, which the file has to carry as well as call.
        self.assertIn('_m_int64(F.col("n"))', sql)
        self.assertIn("def _m_int64(", sql)


# --------------------------------------------------------------------------
# D3 -- Csv.Document's options were dropped, all but the delimiter


class CsvDocumentOptionTests(unittest.TestCase):
    """Only `Delimiter` survived the reader; the rest were read past.

    `Encoding = 1252` is Windows-1252 and the file was read as UTF-8 either
    way -- mojibake, or a decode error, on real European data.
    `QuoteStyle` was read as a quoting switch, and it is not one: it
    decides only whether a quoted line break ends the row (see
    test_quote_style_none_still_quotes_fields).

    What maps is mapped; what does not is named, as a flag or a refusal.
    Nothing is dropped.
    """

    def _read(self, options, **kwargs):
        return _translate(_web(options), _promote("Source"), **kwargs)

    def test_windows_1252_is_mapped(self):
        self.assertIn('.option("encoding", \'windows-1252\')',
                      self._read("[Encoding = 1252]").translated_sql)

    def test_utf_8_is_mapped(self):
        # 65001 is the code page the corpus actually carries.
        self.assertIn('.option("encoding", \'UTF-8\')',
                      self._read("[Encoding = 65001]").translated_sql)

    def test_utf_16le_is_mapped(self):
        self.assertIn('.option("encoding", \'UTF-16LE\')',
                      self._read("[Encoding = 1200]").translated_sql)

    def test_an_unmapped_code_page_is_refused_not_guessed(self):
        # 932 is Shift-JIS. It very likely has a Java charset name, but
        # "very likely" is how a file gets read as the wrong encoding.
        result = self._read("[Encoding = 932]")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("932", str(result.findings))
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_quote_style_none_still_quotes_fields(self):
        '''QuoteStyle.None is about line breaks; `"` still quotes a field.

        This test used to pin `.option("quote", '')`, on the reading that
        QuoteStyle.None means "quotes are not special". Microsoft's
        Csv.Document reference says otherwise: QuoteStyle "Specifies how
        quoted line breaks are handled. QuoteStyle.Csv (default): Quoted
        line breaks are treated as part of the data, not as the end of the
        current row. QuoteStyle.None: All line breaks are treated as the
        end of the current row, even when they occur inside a quoted
        value."

        `(default)` is on **Csv**, and an earlier draft of this docstring
        put it on None. It is not a detail: it is what decides the absent
        case, pinned by `test_an_absent_quote_style_reads_as_csv` below.
        The `QuoteStyle.Type` page contradicts all of this -- it says
        QuoteStyle.None means "Quote characters have no significance" --
        and Example 4 of the Csv.Document page refutes the enum page; see
        `_QUOTE_STYLES` in m_to_pyspark.py, and
        `test_microsofts_own_example_4_is_the_none_shape` below.

        Measured live on AIDP (Spark 3.5.0) over

            id,name,note
            1,"Smith, John","said ""hi"""
            2,Plain,x

        the old `quote=''` reader returned
        [['1', '"Smith', ' John"'], ['2', 'Plain', 'x']] -- the quoted field
        split on its comma and `note` was dropped, silently -- where
        `quote='"'`, `escape='"'` returned
        [['1', 'Smith, John', 'said "hi"'], ['2', 'Plain', 'x']].
        '''
        sql = self._read("[QuoteStyle = QuoteStyle.None]").translated_sql
        self.assertIn(QUOTE_NONE, sql)
        self.assertNotIn('.option("quote", \'\')', sql)

    def test_quote_style_csv_also_keeps_quoted_line_breaks(self):
        # Same quoting as None; the difference is multiLine. Spark's default
        # escape is a backslash, so `escape` is written out for M's `""`.
        sql = self._read("[QuoteStyle = QuoteStyle.Csv]").translated_sql
        self.assertIn(QUOTE_CSV, sql)

    def test_microsofts_own_example_4_is_the_none_shape(self):
        """The doc example that decides which Microsoft page to believe.

        `Csv.Document("1|Barb|""Smith#(cr)#(lf)2|Cal|Fisher",
        [Delimiter = "|", Columns = type table [...],
        QuoteStyle = QuoteStyle.None])` is documented as returning
        `Last Name = "Smith"` and `Last Name = "Fisher"` -- the leading
        quote consumed, the line break ending the row. Read through Spark
        4.2.0 over the same bytes, `quote='"' escape='"' multiLine=False`
        returns [['1','Barb','Smith'],['2','Cal','Fisher']] and the
        refuted `quote=''` returns [['1','Barb','"Smith'],...]. So this
        pins the delimiter travelling with the quoting, which is the pair
        the example exercises.
        """
        sql = self._read('[Delimiter = "|", QuoteStyle = QuoteStyle.None]'
                         ).translated_sql
        self.assertIn('.option("sep", \'|\')', sql)
        self.assertIn(QUOTE_NONE, sql)

    def test_an_absent_quote_style_reads_as_csv(self):
        """M's default is QuoteStyle.Csv, so no key is a Csv read.

        Before this, `[Delimiter = ","]` emitted `['.option("sep", \',\')']`
        and nothing else, so an options record that simply did not mention
        QuoteStyle kept both defects the explicit case had just been fixed
        for: Spark's backslash escape left M's `""` in the data, and
        multiLine off ended the row on a quoted line break.
        """
        sql = self._read('[Delimiter = ","]').translated_sql
        self.assertIn(QUOTE_CSV, sql)

    def test_an_absent_options_record_reads_as_csv_too(self):
        """`Csv.Document(source)` -- every option at its M default."""
        sql = _translate(_web()).translated_sql
        self.assertIn(QUOTE_CSV, sql)

    def test_an_unknown_quote_style_is_refused(self):
        result = self._read("[QuoteStyle = QuoteStyle.Sometimes]")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("QuoteStyle.Sometimes", str(result.findings))

    def test_columns_is_flagged_rather_than_dropped(self):
        # Spark takes its column count from the file and has no equivalent
        # option, so this one is named for a human instead of mapped. It is
        # a flag and not a refusal because every Csv.Document in the
        # 39-export corpus carries it: refusing would cost all 11 emitted
        # CSV pipelines to say something a flag says.
        findings = [f for f in self._read("[Columns = 13]").findings
                    if f.rule == "M32_CSV_OPTION_UNMAPPED"]
        self.assertEqual([f.severity for f in findings], ["flag"])
        self.assertIn("Columns", findings[0].detail)
        self.assertIn("13", findings[0].detail)

    def test_an_option_with_no_mapping_at_all_is_refused(self):
        # ExtraValues decides what M does with a row that has too many
        # fields -- error, ignore, or collect them into a list. Spark's
        # PERMISSIVE default is none of the three.
        result = self._read("[ExtraValues = ExtraValues.Ignore]")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("ExtraValues", str(result.findings))

    def test_positional_options_are_refused_not_read_past(self):
        # `Csv.Document(source, columns, delimiter, extraValues, encoding)`
        # is the positional form. Which argument is which cannot be read
        # off one of them, so the shape is named.
        result = self._read("13")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_the_reported_shape_maps_all_three(self):
        sql = self._read('[Delimiter = ",", Encoding = 1252, '
                         'QuoteStyle = QuoteStyle.None]').translated_sql
        self.assertIn('.option("sep", \',\')', sql)
        self.assertIn('.option("encoding", \'windows-1252\')', sql)
        self.assertIn(QUOTE_NONE, sql)

    def test_the_corpus_shape_maps_and_flags(self):
        result = self._read('[Delimiter = ",", Columns = 11, '
                            'Encoding = 65001, QuoteStyle = QuoteStyle.None]')
        sql = result.translated_sql
        self.assertIn('.option("sep", \',\')', sql)
        self.assertIn('.option("encoding", \'UTF-8\')', sql)
        self.assertIn(QUOTE_NONE, sql)
        self.assertIn("M32_CSV_OPTION_UNMAPPED", [f.rule for f in result.findings])

    def test_no_options_record_still_reads(self):
        self.assertIn("spark.read", _translate(_web()).translated_sql)


class RecordParsingTests(unittest.TestCase):
    """`_m_record_fields` -- the same top-level scan `_m_list_items` uses."""

    def test_a_plain_record(self):
        self.assertEqual(m._m_record_fields('[a = 1, b = "x"]'),
                         {"a": "1", "b": '"x"'})

    def test_a_comma_inside_a_string_is_not_a_separator(self):
        self.assertEqual(m._m_record_fields('[Delimiter = ","]'),
                         {"Delimiter": '","'})

    def test_an_equals_inside_a_string_is_not_the_separator(self):
        self.assertEqual(m._m_record_fields('[Delimiter = "="]'),
                         {"Delimiter": '"="'})

    def test_a_nested_record_keeps_its_own_commas(self):
        self.assertEqual(m._m_record_fields("[a = [b = 1, c = 2], d = 3]"),
                         {"a": "[b = 1, c = 2]", "d": "3"})

    def test_a_quoted_key_is_unquoted(self):
        self.assertEqual(m._m_record_fields('[#"Odd Key" = 1]'),
                         {"Odd Key": "1"})

    def test_not_a_record(self):
        for text in ("13", "{1, 2}", "", None, "[a = 1", "[a]"):
            self.assertIsNone(m._m_record_fields(text), text)

    def test_an_unbalanced_bracket_is_not_a_record(self):
        self.assertIsNone(m._m_record_fields("[a = [b = 1]"))


# --------------------------------------------------------------------------
# D4 -- the same three faults on the lakehouse-file path


def _options_of(sql):
    """The option chain of the one `spark.read...csv(...)` line in `sql`."""
    line = [l for l in sql.splitlines() if "spark.read" in l]
    assert len(line) == 1, line
    body = line[0].split("spark.read", 1)[1]
    return body[:body.rindex(".csv(")]


class LakehouseFilePathTests(unittest.TestCase):
    """The two reader paths have to be one path.

    On the `Web.Contents` path `Csv.Document` *is* the reader. On the
    lakehouse-file path the reader is the navigation chain, and
    `Csv.Document` is a separate step folded into it -- so its options were
    not merely mis-mapped there, they never reached a reader at all. 6 of
    the 11 emitted corpus readers are on this path.
    """

    OPTIONS = '[Delimiter = ";", Encoding = 1252, QuoteStyle = QuoteStyle.None]'

    def _file_read(self, options=OPTIONS):
        return _translate(*(FILE_SOURCE + [_file(options), _promote("Imported")]))

    def test_the_delimiter_reaches_the_reader(self):
        self.assertIn('.option("sep", \';\')', self._file_read().translated_sql)

    def test_the_encoding_reaches_the_reader(self):
        self.assertIn('.option("encoding", \'windows-1252\')',
                      self._file_read().translated_sql)

    def test_the_quote_style_reaches_the_reader(self):
        self.assertIn(QUOTE_NONE, self._file_read().translated_sql)

    def test_columns_is_flagged_here_too(self):
        result = self._file_read('[Columns = 14]')
        self.assertIn("M32_CSV_OPTION_UNMAPPED", [f.rule for f in result.findings])

    def test_an_unmapped_encoding_is_refused_here_too(self):
        result = self._file_read("[Encoding = 932]")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("932", str(result.findings))

    def test_positional_options_are_refused_here_too(self):
        result = self._file_read("14")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_both_paths_emit_the_same_options_for_the_same_m(self):
        # The point of the defect: whatever D1-D3 decided has to be one
        # decision. Same Csv.Document options, same promotion -- the two
        # readers may differ only in the URI they end on.
        web = _translate(_web(self.OPTIONS), _promote("Source"))
        self.assertEqual(_options_of(self._file_read().translated_sql),
                         _options_of(web.translated_sql))

    def test_a_headerless_file_read_is_headerless_with_its_options(self):
        sql = _translate(*(FILE_SOURCE + [_file(self.OPTIONS)])).translated_sql
        self.assertIn('.option("header", False)', sql)
        self.assertIn('.option("sep", \';\')', sql)

    def test_the_fold_finding_says_the_options_were_taken(self):
        detail = [f.detail for f in self._file_read().findings
                  if f.rule == "M11_SOURCE_CSV"]
        self.assertEqual(len(detail), 1)
        self.assertIn("folded into the reader", detail[0])
        self.assertIn("option", detail[0])

    def test_two_csv_documents_over_one_reader_are_refused(self):
        # Two option records for one `spark.read` is not representable, so
        # it is named rather than silently resolved by taking one of them.
        result = _translate(*(FILE_SOURCE + [
            _file('[Delimiter = ";"]', name="A"),
            _step("B", "Csv.Document", ["A", '[Delimiter = "|"]'], inputs=["A"]),
            _promote("B")]))
        self.assertEqual(result.translated_sql, "")
        self.assertIn("M90_UNSUPPORTED_STEP", [f.rule for f in result.findings])

    def test_a_file_read_with_no_csv_document_is_refused_not_called_csv(self):
        """It used to emit a CSV reader, and the comment here used to credit
        the shape to 031.pq. It is not 031's shape: `CHAIN_FILE` in
        tests/test_m_nav.py records 031 verbatim and it ends
        `#"Imported CSV" = Csv.Document(#"Navigation 3")`, with the note
        "Csv.Document consumes it". MEASURED across the corpora vendored
        here, every lakehouse file read has a document function over it --
        2 of 2 -- so this shape is a fixture and not an observation.

        MEASURED before this change, on the chain alone:

            n3 = spark.read.option("header", False)
                   .option("inferSchema", False).csv('oci://.../DimPort.csv')
            findings: M14_SOURCE_LAKEHOUSE_FILE, M30_CSV_NO_HEADER

        -- a CSV reader with no Csv.Document anywhere in the query, plus M30,
        whose entire sentence is about `Csv.Document`'s Column1..ColumnN
        naming. #35 left the reader deliberately and its reason was sound:
        with no options record there are no M defaults to apply, so filling
        them in asserts the file is CSV-with-M-defaults. The conclusion it
        did not draw is that the assertion is the problem, not the defaults.

        What the M says instead: the chain ends on `[Content]`, which in M is
        a binary, so with nothing over it the query's value is a binary and
        not a table. `spark.read.format("binaryFile")` is not a translation
        of that either -- it yields a DataFrame of
        path/modificationTime/length/content, a table *about* the file -- so
        the read is refused and the resolved URI is carried in the reason.
        """
        result = _translate(*FILE_SOURCE)
        self.assertEqual(result.translated_sql, "")
        self.assertNotIn("spark.read", result.translated_sql)
        blocked = [f for f in result.findings
                   if f.rule == "M90_UNSUPPORTED_STEP"]
        self.assertEqual([f.severity for f in blocked], ["flag"])
        # The location work is not thrown away with the read.
        self.assertIn("oci://", blocked[0].detail)
        self.assertIn("DimPort.csv", blocked[0].detail)
        self.assertIn("Csv.Document", blocked[0].detail)

    def test_it_no_longer_claims_a_csv_header_for_a_file_nothing_parses(self):
        """M30's sentence is about `Csv.Document`'s column naming, and it was
        being made about a query that contains no `Csv.Document`."""
        rules = [f.rule for f in _translate(*FILE_SOURCE).findings]
        self.assertNotIn("M30_CSV_NO_HEADER", rules)
        self.assertNotIn("M14_SOURCE_LAKEHOUSE_FILE", rules)

    def test_the_same_chain_with_the_document_function_still_reads(self):
        """The other side, so the refusal cannot be over-broad: the corpus
        shape is untouched."""
        sql = _translate(*FILE_SOURCE_CSV).translated_sql
        self.assertIn("spark.read", sql)
        self.assertIn(".csv('oci://sales@ns/Files/DimPort.csv')", sql)


@requires_node
class LakehouseFileThroughParserTests(unittest.TestCase):
    """The corpus shape, 009.pq: Lakehouse file -> Csv.Document -> promote."""

    M = ('section S; shared Q = let '
         'Source = Lakehouse.Contents(null), '
         'Nav = Source{[lakehouseId = "lh-1"]}[Data], '
         '#"Nav 2" = Nav{[Id = "Files", ItemKind = "Folder"]}[Data], '
         '#"Nav 3" = #"Nav 2"{[Name = "squirrel-data.csv"]}[Content], '
         '#"Imported CSV" = Csv.Document(#"Nav 3", [Delimiter = ",", '
         'Columns = 13, Encoding = 65001, QuoteStyle = QuoteStyle.None]), '
         '#"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", '
         '[PromoteAllScalars = true]) '
         'in #"Promoted headers";')

    def test_the_corpus_shape_carries_its_options_to_the_reader(self):
        sql = _parse_one(self.M).translated_sql
        self.assertIn('.option("header", True)', sql)
        self.assertIn('.option("inferSchema", False)', sql)
        self.assertIn('.option("sep", \',\')', sql)
        self.assertIn('.option("encoding", \'UTF-8\')', sql)
        self.assertIn(QUOTE_NONE, sql)


@requires_node
class HeaderThroughParserTests(unittest.TestCase):
    def test_the_reported_headerless_shape(self):
        result = _parse_one(
            'section S; shared Q = let Source = Csv.Document('
            'Web.Contents("%s")) in Source;' % URL)
        self.assertIn('.option("header", False)', result.translated_sql)

    def test_the_corpus_shape_still_promotes(self):
        result = _parse_one(
            'section S; shared Q = let Source = Csv.Document('
            'Web.Contents("%s")), Promoted = Table.PromoteHeaders(Source, '
            '[PromoteAllScalars = true]) in Promoted;' % URL)
        self.assertIn('.option("header", True)', result.translated_sql)


class ReaderFormatMismatchTests(unittest.TestCase):
    """The reader was `spark.read...csv(...)` whatever the file was called.

    MEASURED before this: a query navigating to `orders.parquet` and calling
    `Csv.Document` emitted

        spark.read.option("header", False).option("inferSchema", False)
             .csv('oci://SalesLake@ns/Files/orders.parquet')

    with M11_SOURCE_CSV as a *rewrite* -- a success. Reading Parquet as CSV
    does not raise: it yields one column of binary text, so nothing
    downstream catches it either.

    Flagged, not re-read with `spark.read.parquet`. `Csv.Document` over a
    `.parquet` is odd in the M before this tool sees it, and there are two
    readings -- the extension is wrong, or the M is. Choosing a reader would
    pick one silently and produce a frame that differs from what Fabric
    produces. Following the M and naming the disagreement is the only answer
    that is not a guess.
    """

    RULE = "M33_READER_FORMAT_MISMATCH"

    def _rules(self, filename):
        source = list(FILE_SOURCE[:-1]) + [
            _step("N3", "N2", nav=['{[Name = "%s"]}' % filename, "[Content]"])]
        return [f.rule for f in _translate(*source, _file()).findings]

    def test_a_parquet_file_read_as_csv_is_named(self):
        self.assertIn(self.RULE, self._rules("orders.parquet"))

    def test_the_finding_says_which_file_and_which_format(self):
        source = list(FILE_SOURCE[:-1]) + [
            _step("N3", "N2", nav=['{[Name = "orders.parquet"]}', "[Content]"])]
        detail = next(f.detail for f in _translate(*source, _file()).findings
                      if f.rule == self.RULE)
        self.assertIn("orders.parquet", detail)
        self.assertIn("Apache Parquet", detail)

    def test_the_reader_is_still_the_one_the_m_asks_for(self):
        # A flag, not a different reader: the emitted code must not change.
        source = list(FILE_SOURCE[:-1]) + [
            _step("N3", "N2", nav=['{[Name = "orders.parquet"]}', "[Content]"])]
        sql = _translate(*source, _file()).translated_sql
        self.assertIn(".csv(", sql)
        self.assertNotIn(".parquet(", sql)

    def test_every_format_in_the_list_is_named(self):
        for name, fmt in (("a.parquet", "Apache Parquet"), ("a.orc", "Apache ORC"),
                          ("a.avro", "Apache Avro"), ("a.xlsx", "Excel"),
                          ("a.json", "JSON"), ("a.xml", "XML")):
            with self.subTest(name=name):
                self.assertIn(self.RULE, self._rules(name))

    def test_a_csv_and_the_shapes_next_to_it_are_left_alone(self):
        """A closed list of formats this project is sure about, not
        "anything that is not .csv". `.csv.gz` especially: Spark's CSV
        reader decompresses it, so flagging it would be wrong."""
        for name in ("orders.csv", "orders.CSV", "orders.txt", "orders.tsv",
                     "orders.csv.gz", "orders"):
            with self.subTest(name=name):
                self.assertNotIn(self.RULE, self._rules(name))

    def test_the_web_path_makes_the_same_check(self):
        """Both paths end on `spark.read...csv(...)`, so a `.parquet` at the
        end of a URL is the same mismatch as one in a lakehouse."""
        rules = [f.rule for f in
                 _translate(_step("S", "Csv.Document",
                                  ['Web.Contents("https://h/x.parquet")'])).findings]
        self.assertIn(self.RULE, rules)

    def test_a_query_string_is_not_read_as_an_extension(self):
        for url, expected in (("https://h/x.csv?format=parquet", False),
                              ("https://h/x.parquet?token=1", True)):
            with self.subTest(url=url):
                rules = [f.rule for f in _translate(
                    _step("S", "Csv.Document",
                          ['Web.Contents("%s")' % url])).findings]
                self.assertEqual(self.RULE in rules, expected)


class LakehouseBucketNameTests(unittest.TestCase):
    """The Dataflow bucket goes through the notebook path's own builder.

    It used to be a format string here -- `"oci://%s@%s/%s/%s" % (...)` --
    which skipped `onelake_to_oci.bucket_name` and everything it checks.
    MEASURED before the change, with no finding of any kind about either:

        lakehouse "Sales Lake"  -> oci://Sales Lake@ns/Files/orders.csv
        lakehouse "Sales/Lake"  -> oci://Sales/Lake@ns/Files/orders.csv

    The first is not a legal OCI bucket name and not a legal URI; the
    notebook path refuses that exact input. The second is worse than
    illegal -- it is legal and means somewhere else: bucket `Sales`, key
    `Lake@ns/Files/orders.csv`.
    """

    def _read(self, lakehouse=None):
        """The lakehouse-file read, with `lh-1` resolved to `lakehouse`.

        `None` means the catalog does not resolve it at all, which is the
        ordinary case for a Git export.
        """
        return m.translate_query(
            {"name": "Q", "attrs": None,
             "steps": list(FILE_SOURCE) + [_file()], "final": "Imported"},
            lakehouses={} if lakehouse is None else {"lh-1": lakehouse},
            namespace="ns")

    def _m14(self, lakehouse=None):
        return next(f.detail for f in self._read(lakehouse).findings
                    if f.rule == "M14_SOURCE_LAKEHOUSE_FILE")

    def test_a_plain_item_name_is_the_bucket(self):
        self.assertIn("oci://SalesLake@ns/Files/DimPort.csv",
                      self._read("SalesLake").translated_sql)

    def test_a_space_in_the_lakehouse_name_is_refused_not_emitted(self):
        result = self._read("Sales Lake")
        self.assertEqual(result.translated_sql, "")
        self.assertIn("not a valid OCI bucket name", str(result.findings))

    def test_a_slash_in_the_lakehouse_name_is_refused_not_emitted(self):
        """The dangerous one: `oci://Sales/Lake@ns/...` is a legal URI that
        names a different bucket and a different key."""
        result = self._read("Sales/Lake")
        self.assertEqual(result.translated_sql, "")

    def test_an_unresolved_lakehouse_still_emits_under_its_guid(self):
        """Different from the notebook path on purpose. A notebook binding
        can name its lakehouse (`default_lakehouse_name`), so a GUID there
        means an incomplete export and is refused. A Dataflow's
        `lakehouseId` can never be resolved from a Git export at all, so
        refusing it would refuse every lakehouse file read; it is emitted
        and flagged instead."""
        self.assertIn("oci://lh-1@ns/Files/DimPort.csv",
                      self._read().translated_sql)
        self.assertIn("the lakehouse did not resolve at all", self._m14())

    def test_a_resolved_lakehouse_does_not_carry_the_unresolved_sentence(self):
        self.assertNotIn("did not resolve at all", self._m14("SalesLake"))

    def test_the_finding_names_the_disagreement_with_the_abfss_form(self):
        """The design decision, stated in the artifact rather than only in
        a commit message: the bare item name agrees with a notebook's FUSE
        and relative forms and disagrees with its `abfss://` one."""
        detail = self._m14("SalesLake")
        self.assertIn("<workspace>_<item>_Lakehouse", detail)
        self.assertIn("two buckets", detail)

    def test_it_agrees_with_the_notebook_path_for_the_same_information(self):
        """A Dataflow's `lakehouseId` carries an item and nothing else --
        the same as `/lakehouse/default/Files/x`. The two must produce the
        same bucket, and this is the assertion that keeps them together."""
        from fabric_aidp.translate.onelake_to_oci import map_onelake_path
        notebook = map_onelake_path("/lakehouse/default/Files/DimPort.csv",
                                    namespace="ns",
                                    default_lakehouse="SalesLake")
        self.assertIn(notebook.mapped,
                      self._read("SalesLake").translated_sql)

    def test_a_lakehouse_name_the_notebook_path_refuses_is_refused_here(self):
        """The two must agree about what cannot be named, not only about
        what can. This is the assertion the format string could not make."""
        from fabric_aidp.translate.onelake_to_oci import map_onelake_path
        notebook = map_onelake_path("/lakehouse/default/Files/DimPort.csv",
                                    namespace="ns",
                                    default_lakehouse="Sales Lake")
        self.assertIsNone(notebook.mapped)
        self.assertEqual(self._read("Sales Lake").translated_sql, "")


if __name__ == "__main__":
    unittest.main()
