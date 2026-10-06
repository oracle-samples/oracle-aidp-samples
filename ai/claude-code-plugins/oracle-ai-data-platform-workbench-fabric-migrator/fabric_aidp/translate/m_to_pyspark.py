"""Turn one parsed M query into a runnable PySpark script.

Steps become a straight-line sequence of DataFrame assignments, in source
order, because M's `let` is already ordered and every step names its input.

Three outcomes, and the difference between them is the point of the tool:

  clean    every step translated, no flags -- eligible for a PASS
  flagged  emitted, but something needs a human (an unresolved lakehouse GUID)
  blocked  a step has no proven mapping; we name it and emit nothing

A query is blocked rather than partially emitted because a PySpark file missing
a filter is worse than no file: it runs, and it is wrong.
"""
from __future__ import annotations

import ast
import keyword
import re
import unicodedata

from fabric_aidp.translate import m_nav, m_runtime, onelake_to_oci
from fabric_aidp.translate.m_expr import (Untranslatable, decode_escapes,
                                          translate_expression)
from fabric_aidp.translate.m_parser import unquote_identifier
from fabric_aidp.naming import (DEFAULT_CATALOG, aidp_table,
                               spark_column_ref, unaddressable_parts)
from fabric_aidp.translate.types import Finding, TranslationResult

_NOT_IDENTIFIER = re.compile(r"\W+")
_STRING = re.compile(r'"((?:[^"]|"")*)"')


class Blocked(Exception):
    """A step has no proven mapping. Named, never guessed."""


# --------------------------------------------------------------------------
# names


def sanitize(name) -> str:
    """M step name -> Python identifier. `#"Promoted headers"` -> `promoted_headers`.

    The `_NOT_IDENTIFIER` regex keeps every str.isalnum() character, and some of those (`³`, `½`) are
    not legal in identifiers. Python also NFKC-normalizes identifiers when it
    parses, so normalize first: `Größe³` and `Größe3` must collide here, where
    `_unique_names` can see it, not silently inside the generated file.
    """
    text = unicodedata.normalize("NFKC", unquote_identifier(name).lower())
    text = _NOT_IDENTIFIER.sub("_", text)
    text = "".join(ch if ("_" + ch).isidentifier() else "_" for ch in text).strip("_")
    if not text or text[0].isdigit():
        text = "step_" + text
    if keyword.iskeyword(text):
        text += "_"
    return text


def _unique_names(steps) -> dict:
    """Step name -> the Python variable the generated file binds it to.

    `taken` starts as `RESERVED` -- every name the emitted file already
    binds -- so a step cannot take one. `sanitize()` lowercases, so `Spark`,
    `SPARK` and `#"spark"` all reached the identifier the session is bound
    to, and the step's assignment did not shadow the session, it destroyed
    it: the next `spark.read` in the same file ran against a DataFrame.

    Every allocation then goes through `taken` as well, rather than through
    a counter per sanitized base. The counter could hand out a name that was
    already somebody else's: `#"a b"` took `a_b`, `#"a_b"` was renamed to
    `a_b_2`, and a third step actually named `#"a_b_2"` had a counter of its
    own still at zero, so it took `a_b_2` too. `translate_query` then
    skipped it as already emitted -- a real `Table.RemoveColumns` gone with
    no line, no rule and no finding. Numbering from the first free suffix
    keeps the existing `added_custom` / `added_custom_2` pairs identical and
    makes the collision unrepresentable.

    A step name that appears twice is refused rather than allocated. `let`
    cannot bind a name twice in M, the Power Query parser accepts it anyway,
    and this mapping is keyed by raw name -- so the two collapsed into one
    entry and the earlier step vanished. Which binding a later reference
    means is not knowable, so it is not guessed.
    """
    taken, mapping = set(RESERVED), {}
    for step in steps:
        if step["name"] in mapping:
            raise Blocked(
                "step %r is bound twice in this query; M binds a name once, "
                "so which one a later reference means cannot be read"
                % unquote_identifier(step["name"]))
        base = sanitize(step["name"])
        candidate, suffix = base, 1
        while candidate in taken:
            suffix += 1
            candidate = "%s_%d" % (base, suffix)
        taken.add(candidate)
        mapping[step["name"]] = candidate
    return mapping


def py_name(name) -> str:
    '''One name, as a Python string literal the generated file can hold.

    Every column name in this module used to be spliced in as `"%s"`. A
    column called `he said "hi"` -- which M spells
    `Table.SelectColumns(Tbl, {"he said ""hi"""})` -- then produced
    `.select("he said "hi"")`, and the whole query died on the `ast.parse`
    gate below. Measured before the fix, all five name-emitting rules:

        Table.SelectColumns        invalid syntax
        Table.RemoveColumns        invalid syntax
        Table.RenameColumns        unterminated string literal
        Table.AddColumn            invalid syntax
        Table.TransformColumnTypes unmatched ')'

    The naive double-quoted form is *proved* correct here rather than
    assumed, by reading the literal back: if `"<name>"` parses and means
    exactly `name`, it is what a reader sees, and every emission this
    project already has is unchanged. Anything else falls back to `repr`,
    which escapes whatever is in the way -- a quote, a backslash, a
    newline, a control character. Proving it beats listing the characters
    that need escaping: that list is the thing that was wrong.
    '''
    text = str(name)
    literal = '"%s"' % text
    try:
        if ast.literal_eval(literal) == text:
            return literal
    except (SyntaxError, ValueError):
        pass
    return repr(text)


# --------------------------------------------------------------------------
# M literal helpers


def m_strings(text) -> list:
    """Every double-quoted M string in argument text, in order."""
    return [m.group(1).replace('""', '"') for m in _STRING.finditer(text or "")]


def m_pairs(text) -> list:
    """`{{"a", type text}, {"b", Int64.Type}}` -> [("a", "type text"), ...]."""
    pairs = []
    for inner in re.findall(r"\{([^{}]*)\}", text or ""):
        parts = [p.strip() for p in inner.split(",")]
        if len(parts) >= 2 and parts[0].startswith('"'):
            pairs.append((parts[0].strip('"').replace('""', '"'), parts[1]))
    return pairs


M_TYPES = {
    "type number": "double", "Int64.Type": "bigint",
    "type date": "date", "type datetime": "timestamp",
    "type datetimezone": "timestamp", "type logical": "boolean",
    "Percentage.Type": "double", "Currency.Type": "decimal(19,4)",
}
# M types whose conversion is not a cast at all, mapped to the generated
# helper that performs it. `Int64.Type` is `Int64.From`: it rounds half to
# even where Spark's `cast("bigint")` truncates toward zero, and it is an
# error where Spark's overflow is a number. Both are measured, with the
# numbers, in `_m_int64`'s own docstring in m_runtime.py -- there rather
# than here because that is the text a reader of a generated file sees.
#
# It is a helper and not an expression because the range test needs the
# rounded value twice, and splicing `F.bround(...)` twice into one
# `withColumn` line is not something to hand a human to read.
#
# `M_TYPES` still holds Int64.Type -> bigint, and it is still the truth:
# `_m_int64` casts to bigint at the end. `_int64_helper_casts_to_the_type_
# the_table_maps` pins the two together so the table and the helper cannot
# drift.
_ROUNDED_CASTS = {"Int64.Type": "_m_int64"}
# `type text` is handled above, before this table is consulted: Spark's cast
# to string writes '3.0' where M writes '3', so it goes through _m_text
# instead. `type time`, `type duration` and `type any` used to map to
# "string" too. That is not a cast, it is a different value, so they are
# refused instead.


# --------------------------------------------------------------------------
# rules


def _reader_options(header, findings):
    """The options every `spark.read...csv(...)` this module emits starts from.

    One function for both reader paths -- the lakehouse file below and the
    `Web.Contents` URL in `_rule_csv_web` -- because the two were drifting:
    the same three faults had to be fixed twice, and only one of them ever
    was.

    `header` is not a default. It is `Table.PromoteHeaders`, read off the
    whole query by `_reader_folds`. M's `Csv.Document` returns a table
    whose columns are `Column1..ColumnN` and whose *first row is data*; a
    header row exists only where a later promotion says so. Measured on
    Spark 4.2.0 over a two-row headerless file:

        header=True   -> columns ['007', '2024-01-05'], one row survives
        header=False  -> columns ['_c0', '_c1'], both rows, '007' intact

    So the forced `header=True` deleted the first row of every headerless
    CSV and renamed the frame's columns after it, silently, under a PASS.

    `inferSchema` is not a choice at all: it is always False, and it is
    written out rather than left to Spark's default because it is a
    statement about what M does. `Csv.Document` returns all-text columns,
    and types arrive later from `Table.TransformColumnTypes`. Measured on
    Spark 4.2.0:

        inferSchema=True   -> '007' becomes the integer 7
        inferSchema=False  -> '007' stays the string '007'

    The leading zeros of a product code, ZIP code, account number or phone
    number are gone by the time the query's own cast runs, so the cast
    cannot recover them. Asking Spark to guess is also not a translation
    of anything in the query: nothing in the M says "infer".
    """
    if not header:
        findings.append(Finding(
            "M30_CSV_NO_HEADER",
            "no Table.PromoteHeaders over this read, so the file is read with "
            "no header row and its first row stays data, as in M; M names "
            "those columns Column1..ColumnN where Spark names them _c0.._cN, "
            "so confirm any column named downstream", "flag"))
    return ['.option("header", %s)' % header, '.option("inferSchema", False)']


def unresolved_lakehouse(guid) -> str:
    """Why a lakehouse GUID has no name here, and what to do about it.

    One sentence, used by the read rule and the write rule, so the two
    cannot drift. It used to say "lakehouse GUID <g> is not in the
    catalog", which reads as something the operator forgot to supply --
    and there is no catalog with a lakehouse-GUID field to supply it to.
    `--catalog` names the AIDP catalog, and `--tables-csv` lists tables;
    neither maps a lakehouse. `plan --lakehouses` does, and the message
    below names it: before that flag existed the only remedy on offer was
    to re-export the whole workspace, which the person running the tool
    often cannot do.

    The true position: M navigates by the lakehouse's *workspace item id*,
    and a Fabric Git export never writes that down for a Lakehouse item --
    `.platform` carries `config.logicalId`, which is a git-generated
    identifier (0 matches against the 29 distinct lakehouseIds in the
    Dataflow corpus). The one place the export does pair a lakehouse GUID
    with its name is a notebook's `default_lakehouse` /
    `default_lakehouse_name` binding, so the remedy is to have such a
    notebook in the export.
    """
    return (
        "Lakehouse %s is not named anywhere in this export: M navigates by a "
        "lakehouse's workspace item id, and a Git export records only the "
        "Lakehouse item's git logicalId, which is a different identifier. "
        "The export pairs a lakehouse GUID with its name in exactly one "
        "place -- a notebook bound to it, `default_lakehouse` beside "
        "`default_lakehouse_name` -- and no notebook here is. Name it with "
        "`plan --lakehouses <csv>` (columns `id,name`), export the whole "
        "workspace so that binding is present, or qualify the name by "
        "hand" % (guid or "<absent>"))


def _unaddressable_note(target, direction, findings):
    '''Flag a name part AIDP's metastore will not accept.

    `aidp_table` back-quotes any part that is not a plain identifier, which
    is right for the *parser* and says nothing about the catalog.

    MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30:
    CREATE SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-
    warehouse-test-wh` fails "MetaException(message:name: ... Only lower-case
    characters, numbers and underscores are allowed.)", and so do
    `R2 Sales Lake` and a non-ASCII name; `default.R2MixedCase` succeeds and
    is listed as `r2mixedcase`. Before that, on fabricTest, `saveAsTable` of a
    back-quoted hyphenated table name was rejected while an [A-Za-z0-9_] one
    was accepted. The alphabet is `naming.addressable`: [a-z0-9_] after
    case-folding.

    This was an `info` while only the local Hive rule had been measured; the
    cluster answered, so it is a flag. It covers every part -- schema and
    table -- because a Dataflow write creates both. One finding per read or
    write, since each names its own table.
    '''
    parts = unaddressable_parts(target)
    if not parts:
        return
    findings.append(Finding(
        "M34_NAME_NOT_ADDRESSABLE",
        "this %s uses %s, and %s. Measured on the AIDP cluster, not "
        "inferred: on fabricTest (Spark 3.5.0) `saveAsTable` of a "
        "back-quoted hyphenated name was REJECTED while the same call with "
        "an [A-Za-z0-9_] name was accepted, and on an AIDP cluster (Spark 3.5.0, "
        "2026-09-30) CREATE SCHEMA of a hyphenated, spaced or non-ASCII name "
        "failed \"Only lower-case characters, numbers and underscores are "
        "allowed\" -- so a schema so named cannot be created on AIDP, while "
        "upper case is folded silently. A read of the rejected name "
        "finds nothing, because no such table can have been created. This "
        "was an `info` while only the Hive rule had been measured and the "
        "AIDP catalog had not; the cluster answered, so it is a flag. The "
        "write is the end that matters: rename the Fabric item or table "
        "before running"
        % (direction, target,
           "%s is not a name that catalog accepts" % repr(parts[0])
           if len(parts) == 1 else
           "%s are not names that catalog accepts"
           % ", ".join(repr(p) for p in parts)),
        "flag"))


def _read_lakehouse(read, namespace, lakehouses, findings, catalog,
                    header=False, csv_step=None):
    if read.kind == "table":
        lakehouse = (lakehouses or {}).get(read.lakehouse_id)
        if not isinstance(lakehouse, str):
            # Defend the format string. `lakehouses` is {lakehouse_guid: name};
            # the project's resolved_catalog is a different shape entirely, and
            # passing it here would splice a dict into a table name.
            lakehouse = None
        if lakehouse:
            findings.append(Finding("M10_SOURCE_LAKEHOUSE",
                                    "reads %s.%s" % (lakehouse, read.table), "rewrite"))
            target = aidp_table(lakehouse, read.table, catalog=catalog)
        else:
            findings.append(Finding(
                "M10_SOURCE_LAKEHOUSE",
                "table %r resolved but its lakehouse did not, so this reads "
                "%s -- a two-part name, resolved against whatever database "
                "the session is pointed at. %s"
                % (read.table, aidp_table("", read.table, catalog=catalog),
                   unresolved_lakehouse(read.lakehouse_id)),
                "flag"))
            target = aidp_table("", read.table, catalog=catalog)
        _unaddressable_note(target, "read", findings)
        return "spark.table(%s)" % py_name(target), "M10_SOURCE_LAKEHOUSE"
    if read.kind == "file":
        # Emit the oci:// form the rest of the tool uses, never a bare relative
        # path. Verified on a live cluster: Spark resolves a relative path
        # against the executor's working directory (/opt/spark/work-dir), not
        # the notebook's -- so a relative path either fails confusingly or, if
        # a same-named directory happens to exist there, reads the wrong data.
        #
        # The bucket goes through `onelake_to_oci`, which is where the
        # notebook path builds one, rather than through a format string
        # here. This used to be `"oci://%s@%s/%s/%s" % (...)`, and the cost
        # of the second copy was everything `bucket_name` checks. MEASURED
        # before the change, with no finding of any kind about either:
        #
        #   lakehouse "Sales Lake"  -> oci://Sales Lake@ns/Files/orders.csv
        #                              a space; not a legal OCI bucket name
        #                              and not a legal URI. The notebook
        #                              path refuses this exact input.
        #   lakehouse "Sales/Lake"  -> oci://Sales/Lake@ns/Files/orders.csv
        #                              bucket `Sales`, key
        #                              `Lake@ns/Files/orders.csv` -- a
        #                              different location entirely.
        #
        # `item_location` gives the read the shape a `/lakehouse/default/`
        # FUSE path has, because it carries the same information: an item
        # and a key, no workspace name and no item type. See its docstring
        # for why neither is invented here.
        item = (lakehouses or {}).get(read.lakehouse_id)
        if not isinstance(item, str):
            item = read.lakehouse_id or "your-lakehouse"
        location = onelake_to_oci.item_location(
            item, "/%s/%s" % (read.folder or "Files", read.file))
        bucket, reason = onelake_to_oci.bucket_name(location)
        if reason:
            raise Blocked(
                "this read is a lakehouse file, so the object-store location "
                "*is* the translation and there is nothing to emit without "
                "one: %s" % reason)
        uri = onelake_to_oci.oci_uri(bucket, namespace, location.rest)
        if csv_step is None:
            # Nothing in this query says how to parse the file, so nothing
            # here may say it is a CSV. MEASURED before this refusal, on a
            # lakehouse-file chain with no `Csv.Document` over it:
            #
            #   n3 = spark.read.option("header", False)
            #          .option("inferSchema", False).csv('oci://.../x.csv')
            #   findings: M14_SOURCE_LAKEHOUSE_FILE, M30_CSV_NO_HEADER
            #
            # -- a CSV reader, plus M30, whose whole sentence is about
            # `Csv.Document`'s Column1..ColumnN naming, said about a query
            # that contains no `Csv.Document`. The extension is not the
            # answer either: `_reader_format_flag` below exists because an
            # extension and the M can disagree, and M is what this tool
            # translates.
            #
            # What the M says instead: the chain ends on `[Content]`, which
            # in M is a *binary*, and with nothing over it the query's value
            # is that binary rather than a table. `spark.read.format(
            # "binaryFile")` was considered and is not a translation of it --
            # that yields a DataFrame of path/modificationTime/length/content,
            # a table *about* the file, so no step written against the
            # binary's own contents would mean the same thing. Blocked is
            # this module's answer for a step with no proven mapping, and
            # the location is carried in the reason so the work of resolving
            # it is not lost.
            #
            # MEASURED across the corpora vendored here -- 14 corpus .pq
            # plus the bundled estate -- every lakehouse file read has a
            # `Csv.Document` over it: 2 of 2. 031.pq, the file read this
            # path was written for, has one too (see `CHAIN_FILE` in
            # tests/test_m_nav.py, and its note "Csv.Document consumes it").
            raise Blocked(
                "this query navigates to lakehouse file %s -- resolved to %s "
                "-- and then nothing parses it: there is no Csv.Document, or "
                "any other document function, over the navigation. In M that "
                "chain ends on [Content], which is a binary, so the query's "
                "value is a binary and not a table. A spark.read...csv(...) "
                "was emitted here before, which asserted the file is "
                "delimited text with M's Csv.Document defaults applied -- "
                "and with no options record there are no M defaults to "
                "apply, so that was a guess about the file's format wearing "
                "a rewrite finding. Add the document function the query is "
                "missing, or translate this read by hand against %s"
                % (read.file, uri, uri))
        findings.append(Finding(
            "M14_SOURCE_LAKEHOUSE_FILE",
            "reads %s/%s from lakehouse %s; confirm the migrated object-store "
            "path %s. The bucket is the bare item name, which is what a "
            "notebook's `/lakehouse/default/...` or `Files/...` path gives "
            "for the same lakehouse -- but an `abfss://` path in a notebook "
            "gives `<workspace>_<item>_Lakehouse`, because that form writes "
            "the workspace and the item type down and a Dataflow's "
            "`lakehouseId` writes neither. One lakehouse reached both ways "
            "therefore lands in two buckets, and reconciling them is a human "
            "decision this export cannot make%s"
            % (read.folder or "Files", read.file,
               read.lakehouse_id or "<unknown>", uri,
               "" if isinstance((lakehouses or {}).get(read.lakehouse_id), str)
               else "; here the lakehouse did not resolve at all, so the "
                    "bucket is the raw lakehouseId and is certainly not a "
                    "bucket anyone has created"),
            "flag"))
        # `csv_step` is the `Csv.Document` folded into this read. On this
        # path `Csv.Document` is a *separate* step over the navigation chain
        # rather than the reader itself, which is how its whole options
        # record used to end up nowhere: the fold emitted a pass-through line
        # and the reader was spelled out here with no reference to it. Both
        # paths now build their options with the same two calls, in the same
        # order. It is never None by here -- the branch above refuses that
        # query rather than inventing a reader for it.
        options = _reader_options(header, findings) + _csv_options(csv_step,
                                                                  findings)
        _reader_format_flag(read.file, findings)
        return _csv_read(uri, options), "M14_SOURCE_LAKEHOUSE_FILE"
    raise Blocked(read.reason or "Lakehouse.Contents with no resolvable navigation")


# Extensions naming a format that `spark.read...csv(...)` cannot be reading
# as delimited text. Deliberately a closed list of the ones this project is
# sure about rather than "anything that is not .csv": an extension not named
# here is left alone, so this can only ever narrow, never widen -- the same
# rule `_VALUE_NAMESPACES` and `_ENCODINGS` follow. `.csv.gz` is therefore
# not flagged, and should not be: Spark's CSV reader decompresses it.
_NOT_DELIMITED = {
    "parquet": "Apache Parquet", "orc": "Apache ORC", "avro": "Apache Avro",
    "xlsx": "Excel", "xlsm": "Excel", "xlsb": "Excel", "xls": "Excel",
    "json": "JSON", "xml": "XML", "pdf": "PDF", "zip": "a zip archive",
    "accdb": "Microsoft Access", "mdb": "Microsoft Access",
    "sas7bdat": "SAS", "sav": "SPSS", "dta": "Stata",
}


def _reader_format_flag(path, findings):
    """Name it when the file this read emits a CSV reader for is not one.

    There was no extension check anywhere. MEASURED before this: a query
    navigating to `orders.parquet` and calling `Csv.Document` emitted
    `spark.read.option("header", False).option("inferSchema", False)
    .csv('oci://SalesLake@ns/Files/orders.parquet')` under an M11_SOURCE_CSV
    *rewrite* -- graded a success. Reading Parquet as CSV does not raise; it
    yields one column of binary text, so nothing downstream catches it
    either.

    A flag and not a different reader, deliberately. The mismatch is in the
    M as exported -- `Csv.Document` over a `.parquet` file is odd before
    this tool sees it -- and there are two readings: the extension is
    wrong, or the M is. Choosing `spark.read.parquet` here would pick one,
    silently, and produce a frame that differs from what the Dataflow
    produces in Fabric. Following the M and naming the disagreement is the
    only answer that is not a guess.
    """
    name = str(path or "").rstrip("/").rsplit("/", 1)[-1]
    extension = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    fmt = _NOT_DELIMITED.get(extension)
    if fmt is None:
        return
    findings.append(Finding(
        "M33_READER_FORMAT_MISMATCH",
        "%s is read here with spark.read...csv(...), because that is what the "
        "query says, but its .%s extension names %s. Reading %s as CSV does "
        "not raise -- it yields one column of text -- so this is not caught "
        "at run time. The disagreement is in the Dataflow as exported; "
        "picking a different reader here would be a guess at which half is "
        "wrong. Confirm the file's real format before running"
        % (name, extension, fmt, fmt), "flag"))


def _csv_read(uri, options) -> str:
    return 'spark.read%s.csv(%r)' % ("".join(options), uri)


_WEB_URL = re.compile(r'Web\.Contents\(\s*"((?:[^"]|"")*)"')

# Fabric writes `Csv.Document`'s Encoding as a Windows code page number.
# Only the three whose Java charset name is certain are mapped; every other
# number is refused by name rather than guessed at, because reading a file
# as the wrong encoding is silent -- it produces characters, just not the
# ones in the file.
_ENCODINGS = {
    "65001": "UTF-8",
    "1252": "windows-1252",
    "1200": "UTF-16LE",
}

# QuoteStyle governs quoted LINE BREAKS, not whether quotes are special.
# Microsoft's `Csv.Document` reference, the options-record entry, verbatim:
#
#     QuoteStyle: Specifies how quoted line breaks are handled.
#       QuoteStyle.Csv (default): Quoted line breaks are treated as part of
#         the data, not as the end of the current row.
#       QuoteStyle.None: All line breaks are treated as the end of the
#         current row, even when they occur inside a quoted value.
#
# `(default)` is on **Csv**. That one word decides `_csv_options` below: an
# options record with no `QuoteStyle` is a `QuoteStyle.Csv` read, not an
# unquoted one, so the defaults are emitted rather than nothing.
#
# The other Microsoft page disagrees with this one. `QuoteStyle.Type`, also
# verbatim, is the whole of its "Allowed values" table:
#
#     QuoteStyle.None  0  Quote characters have no significance.
#     QuoteStyle.Csv   1  Quote characters indicate the start of a quoted
#                         string. Nested quotes are indicated by two quote
#                         characters.
#
# "no significance" is where this file's `.option("quote", '')` came from,
# and taking the enum page at its word is the mistake that shipped. It is
# refuted by Example 4 on the Csv.Document page -- a worked input/output
# pair, under `QuoteStyle.None`, whose input opens a field with a quote:
#
#     Csv.Document("1|Barb|""Smith#(cr)#(lf)2|Cal|Fisher",
#                  [Delimiter = "|", Columns = type table [...],
#                   QuoteStyle = QuoteStyle.None])
#       -> [ID = "1", First Name = "Barb", Last Name = "Smith"],
#          [ID = "2", First Name = "Cal", Last Name = "Fisher"]
#
# `Last Name` is `Smith`, not `"Smith`: the quote was consumed, so it WAS
# significant, and only the line break inside it ended the row. MEASURED,
# same bytes (`1|Barb|"Smith<LF>2|Cal|Fisher`) through Spark 4.2.0:
#
#     quote='"' escape='"' multiLine=False -> [['1','Barb','Smith'],
#                                              ['2','Cal','Fisher']]  == M
#     quote=''                             -> [['1','Barb','"Smith'],
#                                              ['2','Cal','Fisher']]  != M
#
# The function's own worked example beats the enum page's one-liner, and
# the enum page is generic to every function that takes a QuoteStyle.
#
# The live defect the old reading caused. MEASURED live on AIDP (Spark
# 3.5.0) and reproduced on 4.2.0, over
#
#     id,name,note
#     1,"Smith, John","said ""hi"""
#     2,Plain,x
#
#     quote=''                  [['1', '"Smith', ' John"'], ['2', ...]]
#     no option at all          [['1', 'Smith, John', '"said ""hi"""'], ...]
#     quote='"' escape='"'      [['1', 'Smith, John', 'said "hi"'], ...]
#
# -- the quoted field split on its comma and `note` was dropped, silently.
#
# So both styles quote with `"` and escape with `"` (Spark's default escape
# is a backslash, which is not M's doubled quote) and the style decides
# only `multiLine`: the value in this table IS the multiLine setting.
#
# `multiLine=True` is not free, and nothing reaches it today -- it arrives
# with the absent case below. Counted rather than assumed: the vendored
# corpus subset holds 4 `Csv.Document` steps (003, 005, 009, 026) and all 4
# say `QuoteStyle = QuoteStyle.None` outright, and the demo estate emits 3
# CSV readers, all three `multiLine=False`. The 39-export figure of 16 is
# `_csv_options`' and is not re-measurable here; the subset does not carry
# it. It costs splittability: Spark reads the whole file
# in one task, so a large CSV stops parallelising. And on a file with an
# unterminated quote it runs to the end of the file looking for the close
# -- measured on the Example 4 bytes above,
# `[['1', 'Barb', 'Smith\n2|Cal|Fisher\n']]`, one row where None gives
# two. Both are what `QuoteStyle.Csv` *means* in M as well, so they are the
# translation and not a defect; they are written down because a reader
# seeing `multiLine=True` in a generated file should know the cost is
# deliberate.
_QUOTE_STYLES = {"QuoteStyle.Csv": True, "QuoteStyle.None": False}
# The `(default)` marker quoted above, as a value this module can use.
_DEFAULT_QUOTE_STYLE = "QuoteStyle.Csv"


def _quote_options(style) -> list:
    """The three reader options one `QuoteStyle` implies.

    One function because there are two callers -- the key being present and
    the key being absent -- and they must not drift. `escape` used to be
    emitted only on the first, which left the absent case with exactly the
    defect the present case had just been fixed for.
    """
    return ['.option("quote", %r)' % '"',
            '.option("escape", %r)' % '"',
            '.option("multiLine", %s)' % _QUOTE_STYLES[style]]


def _arg_text(step) -> str:
    return " ".join(str(a) for a in (step.get("args") or []))


def _csv_options(step, findings) -> list:
    """`Csv.Document`'s options record -> Spark reader options.

    Every key is mapped, flagged or refused. None is dropped, which is what
    used to happen to all of them but `Delimiter`:

      Encoding = 1252              read as UTF-8 regardless -- mojibake, or
                                   a decode error, on real European data.
      QuoteStyle = QuoteStyle.Csv  read without multiLine, so a quoted line
                                   break ended the row; and Spark's default
                                   backslash escape, so M's `""` inside a
                                   quoted field was not a literal quote.
      QuoteStyle absent            the same, and worse: no quoting option
                                   was emitted at all. `QuoteStyle.Csv` is
                                   M's documented default, so an absent key
                                   is a Csv read -- see `_QUOTE_STYLES`.

    `Columns` is flagged rather than refused. Spark takes its column count
    from the file and has no equivalent option, and every one of the 16
    `Csv.Document` steps in the 39-export corpus carries `Columns`, so
    refusing it would cost all 11 emitted CSV pipelines to say something a
    flag says. (It said 17 here and 16 in `_rule_csv_web` eighty lines
    below; re-measured, it is 16, and 16 of 16 carry `Columns`.)
    """
    args = step.get("args") or []
    if len(args) < 2:
        # `Csv.Document(source)`. Every option is its M default, and the
        # quoting default is not Spark's -- so it is written out, not left
        # off. `Delimiter` is not: M's default is "," and so is Spark's.
        return _quote_options(_DEFAULT_QUOTE_STYLE)
    fields = _m_record_fields(args[1])
    if fields is None:
        # M also takes these positionally:
        # `Csv.Document(source, columns, delimiter, extraValues, encoding)`.
        # Which argument is which cannot be read off one of them, so the
        # shape is named rather than assumed.
        raise Blocked("Csv.Document options %s are not a literal record; the "
                      "positional form is not read rather than guessed at"
                      % str(args[1]).strip()[:80])
    options = []
    for key, value in fields.items():
        if key == "Delimiter":
            options.append('.option("sep", %r)' % _csv_delimiter(value))
        elif key == "Encoding":
            encoding = _ENCODINGS.get(value.strip())
            if encoding is None:
                raise Blocked(
                    "Csv.Document Encoding = %s is a code page with no "
                    "mapping proven here; it is refused rather than read as "
                    "UTF-8, which is what dropping it did" % value.strip()[:40])
            options.append('.option("encoding", %r)' % encoding)
        elif key == "QuoteStyle":
            style = value.strip()
            if style not in _QUOTE_STYLES:
                raise Blocked("Csv.Document QuoteStyle = %s is not a quoting "
                              "mode with a known Spark equivalent" % style[:40])
            options += _quote_options(style)
        elif key == "Columns" and value.strip().isdigit():
            findings.append(Finding(
                "M32_CSV_OPTION_UNMAPPED",
                "Csv.Document declares Columns = %s; Spark takes its column "
                "count from the file and has no equivalent option, so a row "
                "with a different number of fields is not handled the way M "
                "handles it -- confirm the file's shape" % value.strip(),
                "flag"))
        else:
            raise Blocked("Csv.Document option %s = %s has no proven Spark "
                          "mapping; it is refused rather than dropped"
                          % (key, value.strip()[:40]))
    if "QuoteStyle" not in fields:
        # M's default is `QuoteStyle.Csv`, quoted at `_QUOTE_STYLES`. It is
        # not Spark's -- Spark's default escape is a backslash, so `""`
        # inside a quoted field survived into the data, and multiLine is
        # off, so a quoted line break ended the row. Both are the defects
        # the explicit case was just fixed for, and dropping the key is how
        # they reached the absent case. Measured, `[Delimiter = ","]`:
        #
        #     before  ['.option("sep", \',\')']
        #     after   sep, quote, escape, multiLine=True
        #
        # M record keys are case-sensitive, so this is an exact-name test,
        # the same one the loop above makes.
        options += _quote_options(_DEFAULT_QUOTE_STYLE)
    return options


def _csv_delimiter(value) -> str:
    """`"#(tab)"` -> a real tab. Blocked on anything that is not a string."""
    literals = m_strings(value)
    if not literals or not str(value).strip().startswith('"'):
        raise Blocked("Csv.Document Delimiter %s is not a text literal"
                      % str(value).strip()[:40])
    # decode_escapes raises Untranslatable, named for m_expr callers; this
    # module's refusal type is Blocked, so translate it rather than let an
    # unrecognised escape in a delimiter surface the wrong exception type.
    try:
        return decode_escapes(literals[0])
    except Untranslatable as exc:
        raise Blocked("Csv.Document Delimiter: %s" % exc)


def _rule_csv_web(step, findings, header=False):
    """Csv.Document(Web.Contents(url), [Delimiter = ","]) -> spark.read.csv(url).

    8 of the 16 corpus Csv.Document steps take a URL directly; the other 8 read
    a OneLake file already folded into the reader by M14.

    `header` comes from `_reader_folds`, not from this step -- see
    `_reader_options`.
    """
    url = _WEB_URL.search(_arg_text(step))
    if url is None:
        raise Blocked("Csv.Document without a literal Web.Contents URL")
    options = _reader_options(header, findings) + _csv_options(step, findings)
    findings.append(Finding("M11_SOURCE_CSV",
                            "reads CSV over HTTP; Spark cannot read https:// directly, "
                            "stage the file to object storage first", "flag"))
    target = url.group(1).replace('""', '"')
    # The same extension check the lakehouse-file path makes. Both paths end
    # on `spark.read...csv(...)`, so a `.parquet` at the end of a URL is the
    # same mismatch as a `.parquet` in a lakehouse. The query string is cut
    # first: `?format=csv` is not an extension.
    _reader_format_flag(target.split("?", 1)[0].split("#", 1)[0], findings)
    return _csv_read(target, options)


def _rule_add_column(step, var, scope, findings, helpers):
    args = step.get("args") or []
    if len(args) < 3:
        raise Blocked("Table.AddColumn with %d arguments" % len(args))
    names = m_strings(args[1])
    if not names:
        raise Blocked("Table.AddColumn without a literal column name")
    try:
        column = translate_expression(args[2], scope, frame=var, helpers=helpers)
    except Untranslatable as exc:
        raise Blocked("Table.AddColumn: %s" % exc)
    # Spark's default session resolves column names case-insensitively, so
    # withColumn("X") over a frame holding `x` REPLACES x -- measured on the
    # AIDP cluster (Spark 3.5.0): columns ['x'] became ['X'], values
    # doubled in place -- where M, case-sensitive, keeps both. A column the
    # expression reads is known to exist at this step, so that collision is
    # refused here, casefolded like Table.TransformColumnTypes' check; an
    # exact match is an error in M itself. Any other existing column is
    # checked where the schema exists, by `_m_add_column`.
    clash = sorted(name for name in _referenced_columns(column)
                   if name.casefold() == names[0].casefold())
    if clash:
        raise Blocked(
            "Table.AddColumn adds %r but the table already has column %r; "
            "Spark compares column names case-insensitively, so withColumn "
            "would replace it, where M keeps both (or raises, on an exact "
            "match)" % (names[0], clash[0]))
    helpers.add("_m_add_column")
    findings.append(Finding(
        "M20_ADD_COLUMN",
        "adds column %r, checking first that no column of that name exists"
        % names[0], "rewrite"))
    return "_m_add_column(%s, %s, %s)" % (var, py_name(names[0]), column)


def _referenced_columns(expression) -> set:
    """Every literal `F.col(name)` in a translated expression."""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError:
        return set()
    return {node.args[0].value for node in ast.walk(tree)
            if isinstance(node, ast.Call) and len(node.args) == 1
            and isinstance(node.func, ast.Attribute) and node.func.attr == "col"
            and isinstance(node.func.value, ast.Name) and node.func.value.id == "F"
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)}


def _rule_transform_types(step, var, findings, helpers):
    """`type text` is not a cast, and neither is `Int64.Type`.

    Spark's cast to string writes '3.0' where M writes '3', so the text
    columns go through the generated `_m_text` helper, which reads the
    column's type at run time -- the one thing this tool cannot see.
    `Int64.Type` rounds half to even where Spark's cast truncates, and is
    an error outside bigint's range where the cast wraps the sign; both go
    through `_m_int64`. See `_ROUNDED_CASTS`.
    """
    pairs = m_pairs((step.get("args") or ["", ""])[1] if len(step.get("args") or []) > 1 else "")
    if not pairs:
        raise Blocked("Table.TransformColumnTypes without literal column/type pairs")
    columns = [column for column, _ in pairs]
    if len({column.casefold() for column in columns}) != len(columns):
        # M column names are case-sensitive, but Spark's default session
        # compares them case-insensitively, so {"A", type text} and
        # {"a", Int64.Type} collide in Spark even though M sees two distinct
        # columns -- measured: the hoist below inverts which type wins.
        # Casefold so a case-only collision is caught the same as an exact
        # one. The text columns also hoist into one call at the end, which
        # only preserves the source order when no column collides this way.
        raise Blocked("Table.TransformColumnTypes lists a column twice")
    out, text_columns = var, []
    for column, m_type in pairs:
        if m_type == "type text":
            text_columns.append(column)
            continue
        spark_type = M_TYPES.get(m_type)
        if spark_type is None:
            raise Blocked("unmapped M type %r" % m_type)
        # withColumn's first argument is a name and is never parsed;
        # F.col's is parsed. So the same column is spelled two ways, and
        # quoting the first or leaving the second bare are both wrong.
        source = "F.col(%s)" % py_name(spark_column_ref(column))
        helper = _ROUNDED_CASTS.get(m_type)
        if helper:
            # The helper casts; a `.cast()` after it would cast twice.
            helpers.add(helper)
            conversion = "%s(%s)" % (helper, source)
        else:
            conversion = "%s.cast(%s)" % (source, py_name(spark_type))
        out += ".withColumn(%s, %s)" % (py_name(column), conversion)
    if text_columns:
        helpers.add("_m_text_columns")
        out = "_m_text_columns(%s, %s)" % (
            out, ", ".join(py_name(column) for column in text_columns))
    # Honest about what happened to each column: `type text` is rendered
    # through _m_text, not cast, per this function's own docstring.
    cast_count = len(pairs) - len(text_columns)
    if cast_count and text_columns:
        message = ("casts %d column(s), renders %d as text"
                   % (cast_count, len(text_columns)))
    elif text_columns:
        message = "renders %d column(s) as text" % len(text_columns)
    else:
        message = "casts %d column(s)" % cast_count
    findings.append(Finding("M21_TRANSFORM_TYPES", message, "rewrite"))
    return out


def _rule_select_rows(step, var, scope, findings, helpers):
    args = step.get("args") or []
    if len(args) < 2:
        raise Blocked("Table.SelectRows with %d arguments" % len(args))
    try:
        condition = translate_expression(args[1], scope, frame=var, helpers=helpers)
    except Untranslatable as exc:
        raise Blocked("Table.SelectRows: %s" % exc)
    findings.append(Finding("M22_SELECT_ROWS", "filters rows", "rewrite"))
    return "%s.filter(%s)" % (var, condition)


def _rule_rename(step, var, findings, helpers):
    """Through `_m_rename`, which is simultaneous and raises.

    M applies every pair to the original column list at once, so
    `{{"a","b"},{"b","a"}}` swaps two columns. A `withColumnRenamed` chain
    does not, and Spark's own bulk `withColumnsRenamed` is not the fix
    either -- measured on 4.2.0 it returns two columns called `a`, the same
    wrong answer as the chain, and on **3.5.0, which the AIDP cluster
    runs**, it raises AnalysisException instead. Neither version gives M's
    answer, so the argument for not using it holds on both; it fails
    differently, not better. `_m_rename` renames by position instead.

    `withColumnRenamed` also does not raise on a column the frame lacks.
    Measured on Spark 4.2.0 it returns the frame unchanged, where M's
    Table.RenameColumns raises -- so a renamed upstream column produced a
    quietly different table instead of a stopped job. The schema is not
    knowable here, so both checks go where it is: the generated file.
    """
    pairs = m_pairs((step.get("args") or [""] )[1] if len(step.get("args") or []) > 1 else "")
    if not pairs:
        raise Blocked("Table.RenameColumns without literal name pairs")
    helpers.add("_m_rename")
    renames = ", ".join(
        "(%s, %s)" % (py_name(old),
                      py_name(new.strip('"').replace('""', '"')))
        for old, new in pairs)
    findings.append(Finding(
        "M23_RENAME_COLUMNS",
        "renames %d column(s) simultaneously, as M does, checking first that "
        "each exists and that no two end up with one name" % len(pairs),
        "rewrite"))
    return "_m_rename(%s, %s)" % (var, renames)


def _rule_select_columns(step, var, findings):
    args = step.get("args") or []
    names = m_strings(args[1]) if len(args) > 1 else []
    if not names:
        raise Blocked("Table.SelectColumns without literal column names")
    findings.append(Finding("M24_SELECT_COLUMNS", "selects %d column(s)" % len(names), "rewrite"))
    # `DataFrame.select(<str>)` goes through `F.col` and parses the name;
    # `_m_drop` below and `_m_rename`'s `toDF` do not. Measured on Spark
    # 4.2.0 against a flat column called `a.b`: `df.select("a.b")` raises
    # UNRESOLVED_COLUMN and `df.drop("a.b")` succeeds.
    return "%s.select(%s)" % (
        var, ", ".join(py_name(spark_column_ref(n)) for n in names))


def _rule_remove_columns(step, var, findings, helpers):
    """Through `_m_drop`, which raises on a column the frame does not have.

    `DataFrame.drop` does not. Measured on Spark 4.2.0 it returns the frame
    unchanged and raises nothing, where M's Table.RemoveColumns raises --
    the same silent divergence `_rule_rename` closes, and the same reason
    for closing it in the generated file rather than flagging it here.
    """
    args = step.get("args") or []
    names = m_strings(args[1]) if len(args) > 1 else []
    if not names:
        raise Blocked("Table.RemoveColumns without literal column names")
    helpers.add("_m_drop")
    findings.append(Finding(
        "M25_REMOVE_COLUMNS",
        "drops %d column(s), checking first that each exists" % len(names),
        "rewrite"))
    return "_m_drop(%s, %s)" % (var, ", ".join(py_name(n) for n in names))


def _split_top_level(inner):
    """Split at top-level commas. None when a bracket or a quote never closes.

    So a nested list, a record, a call or a string keeps its own commas. A
    doubled quote is M's way of escaping one inside a string, and toggling
    on every quote handles it: `""` toggles off and straight back on, which
    leaves the state where it started.
    """
    depth, start, in_string, items = 0, 0, False, []
    for index, char in enumerate(inner):
        if in_string:
            in_string = char != '"'
        elif char == '"':
            in_string = True
        elif char in "({[":
            depth += 1
        elif char in ")}]":
            depth -= 1
            if depth < 0:
                return None
        elif char == "," and depth == 0:
            items.append(inner[start:index])
            start = index + 1
    if depth or in_string:
        return None
    return items + [inner[start:]]


def _top_level_equals(text):
    """The index of the `=` that separates a record field's name from its
    value, skipping any inside a string or a nested bracket. None when the
    text has no such `=` -- `[a]` is not a field."""
    depth, in_string = 0, False
    for index, char in enumerate(text):
        if in_string:
            in_string = char != '"'
        elif char == '"':
            in_string = True
        elif char in "({[":
            depth += 1
        elif char in ")}]":
            depth -= 1
        elif char == "=" and depth == 0:
            return index
    return None


def _m_list_items(text):
    """`{A, B}` -> ["A", "B"]. None when `text` is not a literal M list."""
    text = str(text or "").strip()
    if not (text.startswith("{") and text.endswith("}")) or len(text) < 2:
        return None
    items = _split_top_level(text[1:-1])
    if items is None:
        return None
    items = [item.strip() for item in items]
    return [] if items == [""] else items


def _m_record_fields(text):
    """`[Delimiter = ",", Columns = 13]` -> {"Delimiter": '","', ...}.

    None when `text` is not a literal M record, which is the signal to
    refuse rather than to read past. Values come back as their raw M text:
    each option decides for itself what it will accept, so nothing is
    coerced here on the way through.
    """
    text = str(text or "").strip()
    if not (text.startswith("[") and text.endswith("]")) or len(text) < 2:
        return None
    parts = _split_top_level(text[1:-1])
    if parts is None:
        return None
    fields = {}
    for part in parts:
        if not part.strip():
            continue
        cut = _top_level_equals(part)
        if cut is None:
            return None
        key = unquote_identifier(part[:cut].strip())
        if not key or key in fields:
            return None
        fields[key] = part[cut + 1:].strip()
    return fields


def _rule_combine(step, names, findings):
    """The list of tables, parsed. Never matched for step names in the text.

    This used to collect a frame for every step name appearing as a whole
    word anywhere in the step's argument text. Word boundaries stop `Other`
    matching inside `OtherLong`, but nothing stops a name matching where it
    is not a table reference:

      Table.Combine({A, B}, {"Zed"})   `Zed` is a *column* here, and a step
          of the same name was unioned in as a third frame M does not union.

      Table.Combine({A, Table.SelectRows(B, each ...)})   `B` was found
          inside the nested call, so the *unfiltered* `b` was unioned and
          the filter vanished with no finding -- the same failure
          `_input_var` was fixed for, which this rule bypasses because it
          resolves its own inputs.

    Parsing the list also fixes two quieter things: the union order is the
    list's rather than the order the steps were declared in, and a table
    listed twice is unioned twice, which is what M does and what
    deduplicating the frames undid.

    The optional second argument -- M's explicit output column list -- is
    refused rather than ignored: `unionByName(allowMissingColumns=True)`
    keeps every column, where M keeps exactly the ones that argument names.
    """
    args = step.get("args") or []
    if len(args) > 1:
        raise Blocked("Table.Combine with an explicit column list; "
                      "unionByName keeps every column where M keeps only the "
                      "listed ones, so it is refused rather than dropped")
    items = _m_list_items(args[0]) if args else None
    if items is None:
        raise Blocked("Table.Combine over %s, which is not a literal list of "
                      "steps" % (str(args[0]).strip()[:80] if args else "no argument"))
    by_name = {unquote_identifier(name): var for name, var in names.items()}
    frames = []
    for item in items:
        key = _bare_reference(item)
        if key not in by_name:
            raise Blocked("Table.Combine lists %s, which is not a step of this "
                          "query (a nested call or another query); it is not "
                          "translated rather than guessed" % item[:80])
        frames.append(by_name[key])
    if len(frames) < 2:
        raise Blocked("Table.Combine over %d table(s)" % len(frames))
    findings.append(Finding("M27_COMBINE", "unions %d frames" % len(frames), "rewrite"))
    return ".unionByName(".join(frames[:1] + [f + ", allowMissingColumns=True)"
                                              for f in frames[1:]])


PASS_THROUGH = frozenset({"Table.PromoteHeaders", "Csv.Document",
                          "Table.Buffer"})
_FOLD_RULES = {"Table.PromoteHeaders": "M26_PROMOTE_HEADERS",
               "Csv.Document": "M11_SOURCE_CSV"}
# What the fold actually did, per function. `Csv.Document` used to claim the
# reader "already takes the first row as the header" whether or not the query
# promoted one, which described the D1 corruption as if it were the mapping.
_FOLD_DETAIL = {
    "Table.PromoteHeaders": "Table.PromoteHeaders folded into the reader, "
                            "which is read with header=True because of it",
    "Csv.Document": "Csv.Document folded into the reader, which parses the "
                    "file it was handed with this step's options",
}


def _reader_folds(steps, reader_names):
    """What the rest of the query says about how each reader must parse.

    Runs before anything is emitted, because a reader's options are decided
    by steps that come *after* it. In M the header promotion is a separate
    step -- and on the lakehouse-file path so is `Csv.Document`, which is
    where the whole options record used to vanish.

    Walks the query in `let` order following only the pass-throughs --
    `Csv.Document`, `Table.Buffer`, `Table.PromoteHeaders` -- so the same
    alias chain `_rule_pass_through` folds over is the one consulted here.
    Anything else between the read and the promotion breaks the chain, and
    then the promotion is not the reader's: `Csv -> SelectRows -> Promote`
    promotes the header of the *filtered* frame, which `_rule_pass_through`
    refuses. Reaching back to flip the reader's header on would promote the
    wrong row.

    Returns:
      promoted  reader names a `Table.PromoteHeaders` belongs to
      chain     every frame name that aliases a reader
      csv       reader name -> the `Csv.Document` step folded into it, for
                the readers that are not themselves one
    All three are keyed by the raw step name, so `_unique_names`' mapping
    applies to them.
    """
    alias = {unquote_identifier(name): name for name in reader_names}
    promoted, chain, csv = set(), set(reader_names), {}
    for step in steps:
        name = unquote_identifier(step["name"])
        function = str(step.get("fn") or "")
        if name in alias or function not in PASS_THROUGH:
            continue
        source = None
        for candidate in (list(step.get("inputs") or [])
                          + list(step.get("args") or [])):
            key = _bare_reference(candidate)
            if key in alias:
                source = alias[key]
                break
        if source is None:
            continue
        alias[name] = source
        chain.add(step["name"])
        if function == "Table.PromoteHeaders":
            promoted.add(source)
        elif function == "Csv.Document":
            if source in csv:
                # One `spark.read` cannot carry two options records, and
                # which one wins is not something to decide quietly.
                raise Blocked(
                    "two Csv.Document steps fold into the same read (%s and "
                    "%s); one reader cannot carry both options records"
                    % (unquote_identifier(csv[source]["name"]),
                       unquote_identifier(step["name"])))
            csv[source] = step
    return promoted, chain, csv


def _rule_pass_through(function, var, readers, findings):
    """A step that returns the frame it was handed. Say which of the two
    reasons applies, because they are not the same reason.

    `Table.Buffer` and `Table.PromoteHeaders` both used to report
    "<fn> folded into the reader". Over a lakehouse table there is no
    reader: `spark.table(...)` has no header option to fold a promotion
    into, so the finding described an operation that did not happen -- and
    `Table.Buffer` reported it under M11_SOURCE_CSV, which is not the wrong
    wording but the wrong rule, since Buffer reads nothing at all.

    `readers` holds the variables this translation bound to a
    `spark.read...csv(...)` -- a lakehouse file (M14) or a Csv over HTTP
    (M11) -- and anything aliasing one of those. It is the `chain` half of
    `_reader_folds`, computed over the same walk that decided each
    reader's `header`, so the two cannot disagree about what folds.

      Table.Buffer            always a pass-through: it forces
                              materialisation and returns the same value, so
                              the frame really is unchanged, reader or not.
      Csv.Document            folded, but only over a reader: the emitted
      Table.PromoteHeaders    `spark.read` already parses the CSV, and a
                              promotion in the query is what set its
                              `header=True`.

    Over anything else those last two are refused rather than passed
    through. `Table.PromoteHeaders` on a frame that already has column names
    is not a no-op in M -- it makes the first data row the header -- so
    returning the frame unchanged would drop a real operation silently.
    """
    if function == "Table.Buffer":
        findings.append(Finding(
            "M29_BUFFER",
            "Table.Buffer is a materialisation hint with no Spark "
            "equivalent; the frame is passed through unchanged", "rewrite"))
        return "M29_BUFFER"
    if var not in readers:
        raise Blocked(
            "%s over a frame this translation did not read with a header "
            "option, so there is no reader to fold it into and it is not a "
            "no-op here; it is refused rather than passed through" % function)
    rule = _FOLD_RULES[function]
    findings.append(Finding(rule, _FOLD_DETAIL[function], "rewrite"))
    return rule


# --------------------------------------------------------------------------
# emitter

HEADER = '''"""Generated by fabric-aidp-migrator from {source}.

Query: {query}
Rules applied: {rules}

Not execution-verified. Review before running.
"""
from pyspark.sql import SparkSession, functions as F
'''

# Follows HEADER, after any helper prelude has been spliced in between, so
# `from pyspark.sql import types as T` lands with the other imports instead
# of after the session line. When there is no prelude this collapses to
# exactly the blank-line-then-session text HEADER used to end with.
SESSION = '''
spark = SparkSession.builder.getOrCreate()

'''


def _module_level_names(text) -> set:
    """Every name `text` binds at module level, read with `ast`, not listed."""
    names: set = set()
    for node in ast.parse(text).body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update((alias.asname or alias.name).split(".")[0]
                         for alias in node.names)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets
                         if isinstance(target, ast.Name))
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            if isinstance(node.target, ast.Name):
                names.add(node.target.id)
    return names


# Every name a generated file already binds at module level: the header's
# imports, the session, every helper `m_runtime` can splice in, and the
# `result =` a query with no destination ends on. Read back out of that text
# rather than typed out here, so a new import or a new generated helper joins
# the set without anyone having to remember to add it.
#
# `result` is the one name not in the text above, because `_write_lines`
# emits it only for a query with no destination.
RESERVED = frozenset(
    _module_level_names(HEADER + m_runtime.prelude_for(m_runtime.HELPERS)
                        + SESSION)
    | {"result"})


# Fabric's own marker, written into the `meta` record of a parameter query:
# `#date(2024, 11, 1) meta [IsParameterQuery=true, Type="Date", ...]`. It is
# the export saying "this is a parameter", which is evidence and not a guess.
# The `meta` suffix is why the marker is needed at all: `m_expr` refuses a
# metadata expression, so the value under it cannot prove itself a scalar.
_IS_PARAMETER_QUERY = re.compile(r"IsParameterQuery\s*=\s*true")
_MEMBER_KIND = re.compile(r"^unsupported member kind (?P<v>\w+)$")


def member_kind(query) -> str:
    """The AST kind `parse.js` recorded for a non-`let` member, or ""."""
    match = _MEMBER_KIND.match(str(query.get("note") or "").strip())
    return match.group("v") if match else ""


def _is_parameter(query) -> bool:
    """Whether a stepless member is provably a constant.

    Two proofs, no guesses:

      the export says so   `meta [IsParameterQuery = true, ...]` is Fabric's
                           own marker for a parameter query, written by the
                           Power Query editor.
      this tool can read   the member's whole expression translates as an
      the value            M scalar. `#date(2026, 1, 1)` does;
                           `Table.Group(Src, ...)` does not, and neither
                           does `(tbl as table) as table => let ...`.

    The second proof is `m_expr` doing the same job `_bind_scalar` uses it
    for on a step, so "is this a value" is answered by the one component
    that knows, rather than by the member's AST kind -- which cannot tell
    `#date(2026, 1, 1)` from `Table.FromRows({{1, "a"}}, {"id", "name"})`:
    both are a `RecursivePrimaryExpression`.
    """
    raw = str(query.get("raw") or "")
    if _IS_PARAMETER_QUERY.search(raw):
        return True
    try:
        translate_expression(raw)
    except Untranslatable:
        return False
    return True


def classify(query, section_attrs="") -> str:
    """"helper" (a write target), "parameter" (a constant), "unread_member"
    (a member this translator did not read), or "pipeline".

    `unread_member` is the bucket this function did not have. Every member
    that is not a `let` expression arrives with `steps: []`, and the rule was
    "no steps, therefore a parameter" -- so a complete query written without
    `let` was called a constant and vanished. It emitted no plan asset, no
    file, no finding: the parser had already written down *why* it had no
    steps, in `note`, and nothing read it.

    Measured over the 39-export corpus, 22 members arrive stepless and they
    are three different things:

      10  named by the section's DefaultOutputDestinationSettings -- write
          targets, the same as `*_DataDestination`                 -> helper
       4  `meta [IsParameterQuery=true, ...]`, Fabric's own marker -> parameter
       8  4 `FunctionExpression` (custom M functions, one of which builds
          the table the other one reads), 2 `DateTime.Date(DateTime.LocalNow())`,
          and 039.pq's `AbsenceSource` and `Absence` -- a Lakehouse read and
          a `Table.Group` over it, a complete pipeline written without `let`
                                                              -> unread_member

    All 22 used to be "parameter". Each verdict is now read off something
    the export writes down, or off `m_expr` proving the member is a value,
    rather than inferred from an empty list that only means "this parser did
    not read the member".
    """
    if (m_nav.is_destination_helper(query.get("name"))
            or m_nav.is_default_destination_target(query.get("name"),
                                                   section_attrs)):
        return "helper"
    if query.get("steps"):
        return "pipeline"
    if not member_kind(query) or _is_parameter(query):
        return "parameter"
    return "unread_member"


def _mapped_var(destination, var, findings, lines):
    """Project the frame onto the destination's column mapping, if it has one.

    A `DataDestinations` entry can carry
    `ColumnSettings = [Mappings = {[SourceColumnName = "a",
    DestinationColumnName = "b"], ...}]`. That list *is* the destination's
    shape: its order, its names, and -- because a source column absent from
    it is not written -- its width. Writing the frame unchanged gives the
    target extra columns, and in 033.pq puts `Date` into a table whose
    column is `Message_Date`.

    Appends the projection line to `lines` and returns the variable the
    write should use. `result` is already in RESERVED, so no step can have
    taken it.
    """
    mapping = destination.column_map
    if mapping is None:
        return var
    if not mapping:
        raise Blocked(
            "the destination declares ColumnSettings this code cannot read, "
            "so which columns reach the target is unknown; writing the whole "
            "frame would be a guess at the target's shape")
    targets = [target for _, target in mapping]
    duplicates = sorted({t for t in targets if targets.count(t) > 1})
    if duplicates:
        raise Blocked(
            "the destination maps more than one source column onto %s; a "
            "column cannot hold two values and which one wins is not read "
            "from the export" % ", ".join(repr(d) for d in duplicates))
    renamed = [(source, target) for source, target in mapping if source != target]
    detail = ("writes %d column(s) in the destination's order, dropping any "
              "other column the query produces" % len(mapping))
    if renamed:
        detail += "; renames " + ", ".join("%s -> %s" % pair for pair in renamed)
    if destination.dynamic_schema:
        # DynamicSchema = true means Fabric re-reads the source schema at
        # run time, so the recorded mapping is a snapshot of the columns
        # that existed when the Dataflow was last saved. Applying it is the
        # only shape the export states, but a column added to the source
        # since then would reach the target in Fabric and not here.
        detail += ("; DynamicSchema = true, so Fabric refreshes this mapping "
                   "from the source at run time and it is a snapshot -- a "
                   "column added to the source since the Dataflow was saved "
                   "reaches the target in Fabric and not here")
    findings.append(Finding("M18_DESTINATION_COLUMN_MAP", detail,
                            "flag" if destination.dynamic_schema else "rewrite"))
    lines.append("result = %s.select(%s)" % (var, ", ".join(
        "F.col(%s).alias(%s)" % (py_name(spark_column_ref(source)),
                                 py_name(target))
        for source, target in mapping)))
    return "result"


def _write_lines(destination, var, findings, lakehouses=None,
                 catalog=DEFAULT_CATALOG):
    """The write at the end of the script.

    The destination goes through `aidp_table` for the same reason the reads
    above it do. It used to emit `saveAsTable("dim_agent")` -- a bare,
    unqualified name in a file whose every read was fully qualified, so the
    write landed wherever the session default happened to point. That is the
    destructive half of getting a name wrong.
    """
    lines: list = []
    if destination is None:
        findings.append(Finding("M12_DESTINATION",
                                "no DataDestinations declared; result is left as a DataFrame",
                                "flag"))
        return ["result = %s" % var]
    if destination.kind == "default_unresolved":
        # Emitting a DataFrame here is what nine corpus queries used to do:
        # compute the answer and drop it, and grade as a successful
        # migration. The bind says a write was intended, so not writing is
        # a refusal, not a pass.
        raise Blocked(destination.reason)
    if destination.kind == "table":
        if not destination.table:
            raise Blocked("DataDestinations resolves to a table with no name")
        # Before the table name is resolved: a mapping that cannot be
        # honoured refuses the query, and saying "writes X" first would put
        # a finding about a write that never happens ahead of the refusal.
        var = _mapped_var(destination, var, findings, lines)
        # Resolved exactly as `_read_lakehouse` resolves a read: `lakehouses`
        # is {lakehouse_guid: name}, and anything else in there would splice
        # a non-name into a table name.
        lakehouse = (lakehouses or {}).get(destination.lakehouse_id)
        if not isinstance(lakehouse, str):
            lakehouse = ""
        target = aidp_table(lakehouse, destination.table, catalog=catalog)
        # Before M12: if the catalog will not hold this name the write
        # does not happen at all, and saying "writes X" first would read as
        # if it did. This is the destructive end, so it goes first.
        _unaddressable_note(target, "write", findings)
        if lakehouse:
            findings.append(Finding(
                "M12_DESTINATION",
                "writes %s (mode=%s)" % (target, destination.mode), "rewrite"))
        else:
            findings.append(Finding(
                "M12_DESTINATION",
                "writes %s (mode=%s) -- a two-part name, so the write lands "
                "in whatever database the session is pointed at, and a write "
                "to the wrong place is not recoverable. %s"
                % (target, destination.mode,
                   unresolved_lakehouse(destination.lakehouse_id)),
                "flag"))
        if destination.default_bound:
            # Never a clean pass. The lakehouse is declared but the table
            # name is not: Fabric's default destination names the table
            # after the query, and the export records that nowhere. The one
            # corpus file holding both shapes (035.pq) agrees with the rule,
            # which is evidence, not proof.
            findings.append(Finding(
                "M17_DEFAULT_DESTINATION",
                "[BindToDefaultDestination = true]: no table name is written "
                "down anywhere in this export, so the query's own name is "
                "used and it writes %s (mode=%s) in the lakehouse the "
                "section's DefaultOutputDestinationSettings names -- confirm "
                "the table name before running"
                % (target, destination.mode),
                "flag"))
        return lines + ["%s.write.mode(%s).saveAsTable(%s)"
                        % (var, py_name(destination.mode), py_name(target))]
    if destination.kind == "unsupported":
        raise Blocked("destination connector %s is not supported"
                      % (destination.connector or "<unknown>"))
    raise Blocked("DataDestinations names %r, which this export does not contain"
                  % destination.query_name)


def translate_query(query, *, queries_by_name=None, namespace="default",
                    lakehouses=None, catalog=DEFAULT_CATALOG,
                    source="mashup.pq", section_attrs="") -> TranslationResult:
    """One parsed M query -> a PySpark script, or a blocked result naming the step."""
    # What the result carries as its source, which is what the report's
    # Source pane shows. It read `raw or final`, and the parser writes `raw`
    # only for a NON-`let` member -- so every `let` query, which is every
    # query that translates, fell through to `final`: the `in` step's name.
    # MEASURED on the demo: all four translated Dataflow queries showed
    # 16-22 characters (`#"Filtered rows"`, `#"Changed column type"`) beside
    # a 1944-character mashup.pq, leaving the side-by-side review with
    # nothing on the left. `source` is the member as written; `raw` keeps its
    # own meaning for the readers that parse it as a value expression.
    raw = (query.get("source") or query.get("raw") or query.get("final")
           or "")
    findings: list = []
    kind = classify(query, section_attrs)
    if kind == "unread_member":
        # Not a rewrite. A helper and a parameter emit no pipeline because
        # there is no pipeline to emit; this one emits none because the
        # member was not read, and the two must not grade the same.
        findings.append(Finding(
            "M19_UNREAD_MEMBER",
            "%r is a %s: not a `let` query, which is the only shape this "
            "translator turns into a pipeline, and not a value it can read as "
            "a constant either. So nothing was produced from it. If it "
            "computes a table -- a one-expression query, a custom M function, "
            "a `Table.Group` over a lakehouse read -- that work is not in the "
            "migration and has to be written by hand. Its source text is: %s"
            % (unquote_identifier(query.get("name")), member_kind(query),
               " ".join(str(raw).split())[:200] or "<absent>"),
            "flag"))
        return TranslationResult(raw, "", findings)
    if kind != "pipeline":
        findings.append(Finding(
            "M13_SUPPRESS_HELPER" if kind == "helper" else "M15_PARAMETER_QUERY",
            "%s query %r emits no pipeline" % (kind, unquote_identifier(query.get("name"))),
            "rewrite"))
        return TranslationResult(raw, "", findings)

    steps = query["steps"]
    try:
        names = _unique_names(steps)
    except Blocked as exc:
        return _blocked(raw, str(exc), findings)
    scope = {}
    # Helper names the generated file needs. The parser and the type rule add
    # to it; `m_runtime.prelude_for` turns it into the definitions spliced in
    # below.
    helpers: set = set()
    consumed: set = set()
    # Steps that already have a line. The lakehouse fold emits the last step
    # of its chain, and the step loop below must not emit it again. This was
    # "does any emitted line start with this variable?", which silently
    # skipped any *other* step that had been allocated the same variable --
    # the second half of the name-collision defect above.
    emitted: set = set()
    lines: list = []
    rules: list = []
    # The lakehouse navigation chains, resolved before anything is emitted.
    # The reads used to be emitted here, in this loop -- but a CSV reader's
    # options are decided by steps that come *after* it (a
    # `Table.PromoteHeaders` says the file has a header row), so resolving
    # and emitting have to be two passes.
    reads = []
    for index in m_nav.lakehouse_read_indexes(steps):
        if steps[index]["name"] in consumed:
            continue
        read = m_nav.fold_lakehouse_chain(steps, index)
        consumed.update(read.consumed[:-1])
        reads.append(read)

    # Every step that becomes a `spark.read...csv(...)`: a lakehouse *file*
    # read, or a `Csv.Document` over a literal `Web.Contents` URL.
    reader_names = {read.consumed[-1] for read in reads if read.kind == "file"}
    reader_names |= {step["name"] for step in steps
                     if str(step.get("fn") or "") == "Csv.Document"
                     and _WEB_URL.search(_arg_text(step))}
    try:
        promoted, chain, csv_steps = _reader_folds(steps, reader_names)
    except Blocked as exc:
        return _blocked(raw, str(exc), findings)
    # Variables bound to a `spark.read...csv(...)`, or to a pass-through
    # alias of one -- the only frames a header promotion can fold into. See
    # `_rule_pass_through`.
    readers = {names[name] for name in chain}

    for read in reads:
        last = read.consumed[-1]
        try:
            expression, rule = _read_lakehouse(read, namespace, lakehouses,
                                               findings, catalog,
                                               header=last in promoted,
                                               csv_step=csv_steps.get(last))
        except Blocked as exc:
            return _blocked(raw, str(exc), findings)
        lines.append("%s = %s" % (names[last], expression))
        emitted.add(last)
        rules.append(rule)

    try:
        for step in steps:
            name, function = step["name"], str(step.get("fn") or "")
            if name in consumed or name in emitted:
                continue
            if function in _SCALAR_KINDS:
                _bind_scalar(step, scope, findings)
                continue
            if name in reader_names and function == "Csv.Document":
                rules.append("M11_SOURCE_CSV")
                lines.append("%s = %s" % (
                    names[name],
                    _rule_csv_web(step, findings, header=name in promoted)))
                continue
            _check_supported(function)
            # Table.Combine resolves its own list of frames.
            var = None if function == "Table.Combine" else _input_var(step, names)
            expression = _apply(step, function, var, names, scope, findings,
                                rules, helpers, readers)
            lines.append("%s = %s" % (names[name], expression))
            emitted.add(name)
    except Blocked as exc:
        return _blocked(raw, str(exc), findings)

    if "_m_text" in helpers or "_m_text_columns" in helpers:
        rules.append("M28_TEXT_RENDERING")
        findings.append(Finding(
            "M28_TEXT_RENDERING",
            "renders column(s) as text; M's number rendering is reproduced "
            "except for NaN and the infinities, which render as Spark spells "
            "them, -0.0, which renders '0', and a decimal column, which is "
            "rendered as a decimal and so never goes scientific at any "
            "magnitude; and a date or timestamp column stops the job at run "
            "time because rendering one is culture-dependent in M",
            "flag"))

    destination = m_nav.resolve_destination(query, queries_by_name or {},
                                            section_attrs=section_attrs)
    try:
        final = _final_var(query, names)
        lines.extend(_write_lines(destination, final, findings,
                                  lakehouses=lakehouses, catalog=catalog))
    except Blocked as exc:
        return _blocked(raw, str(exc), findings)

    body = (HEADER.format(source=source, query=unquote_identifier(query["name"]),
                         rules=", ".join(dict.fromkeys(rules)) or "none")
           + m_runtime.prelude_for(helpers) + SESSION
           + "\n".join(lines) + "\n")
    try:
        ast.parse(body)
    except SyntaxError as exc:
        # A refusal, not an exception. This used to `raise AssertionError`,
        # and nothing above catches one: `migrate/runner.py`'s blanket
        # `except Exception` turned it into a result row with
        # `status: "error"` and a bare `str(exc)` -- no rule, no severity,
        # no finding -- which `verify` grades FAIL. So a column name holding
        # a `"` took the whole migration to FAIL and said only "invalid
        # syntax", with nothing naming the query or pointing at a cause.
        # The gate itself is right and stays: nothing that will not compile
        # is ever written.
        return _blocked(
            raw,
            "M31:the PySpark generated for this query does not compile, so "
            "nothing was written: %s. The gate that caught this is this "
            "translator's own and it is working; what it caught is a defect "
            "in the translation, not in the Dataflow. Please report it with "
            "the query" % exc, findings)
    return TranslationResult(raw, body, findings)


def _final_var(query, names):
    """The variable the query's `in` clause returns.

    M's `let ... in <expr>` returns the *expression*, not the last binding,
    and only a bare step reference folds onto a variable this module has
    already emitted. This used to be `names.get(final, <the last step>)`:
    any `in` clause that computed something -- `A + B`, `Table.Buffer(Kept)`
    -- missed the lookup and was replaced by whatever step happened to come
    last, with no finding. That substitutes an unrelated value for the
    query's result, which is the one failure mode this whole tool exists to
    avoid, so the expression is named and refused instead.

    `#"Sorted rows"`, `(Sorted rows)` and `Sorted rows` are the same
    reference, resolved here the way `_input_var` resolves a step argument.
    Measured over the 39-export corpus: every query's `in` clause is a bare
    step reference, so refusing the rest costs no coverage.
    """
    by_name = {unquote_identifier(name): var for name, var in names.items()}
    final = query.get("final")
    key = _bare_reference(final)
    if key in by_name:
        return by_name[key]
    if final is None:
        raise Blocked("this query records no `in` clause, so which step it "
                      "returns cannot be read; it is not guessed")
    raise Blocked("`in %s` returns an expression, not one of this query's "
                  "steps; it is refused rather than silently replaced by the "
                  "last step" % str(final).strip()[:120])


def _blocked(raw, reason, findings):
    """A refusal, under the rule id the reason asks for.

    `M31_UNCOMPILABLE_OUTPUT` has its own id rather than sharing M90's
    because the two say opposite things. M90 is "the Dataflow does
    something this tool has no mapping for" -- expected, and the reader's
    next step is to translate that step by hand. M31 is "this tool built
    Python it cannot parse" -- a defect here, and the reader's next step is
    to report it. Folding one into the other would hide a translator bug
    inside a routine refusal.
    """
    rule = "M90_UNSUPPORTED_STEP"
    for prefix, named in (("M91:", "M91_UNSUPPORTED_CONNECTOR"),
                          ("M31:", "M31_UNCOMPILABLE_OUTPUT")):
        if reason.startswith(prefix):
            rule, reason = named, reason[len(prefix):]
    findings.append(Finding(rule, reason, "flag"))
    return TranslationResult(raw, "", findings)


_SCALAR_KINDS = {"LiteralExpression", "IdentifierExpression"}


def _bind_scalar(step, scope, findings):
    """A step whose value is a literal or a bare identifier is a scalar, not a
    frame. Binding it into `scope` is what lets later each-expressions resolve
    it; on the corpus this is worth 38% -> 71% of expression coverage."""
    try:
        scope[step["name"]] = translate_expression(step.get("raw") or "", scope)
    except Untranslatable as exc:
        raise Blocked("scalar binding %r: %s" % (step["name"], exc))
    findings.append(Finding("M16_SCALAR_BINDING",
                            "binds %r as a scalar" % unquote_identifier(step["name"]),
                            "rewrite"))


_CONNECTOR = re.compile(r"^[A-Z][A-Za-z0-9]*\.[A-Za-z][A-Za-z0-9]*$")
_SUPPORTED = {
    "Table.AddColumn", "Table.TransformColumnTypes", "Table.SelectRows",
    "Table.RenameColumns", "Table.SelectColumns", "Table.RemoveColumns",
    "Table.Combine", "Table.PromoteHeaders", "Csv.Document", "Table.Buffer",
}


# M's standard-library namespaces that operate on values already in hand.
# Nothing in one of these is a data source, so a step calling one is an
# unsupported *function* (M90), never an unsupported connector (M91).
#
# The discriminator is the namespace, not whether the call happens to read an
# earlier step: `Excel.Workbook(Source)` is still a connector when `Source` is
# a step, and `Text.Upper(Nav2)` is still a function when it reads one.
#
# Deliberately a list of the namespaces this project is sure about rather
# than a list of connectors. Every namespace not named here keeps the M91
# verdict it has today, so this only narrows M91 and can never widen it.
# Checked against the 39-export corpus: its M91 refusals are AzureStorage,
# Databricks, Excel, FabricSql, GoogleSheets, PowerPlatform, SharePoint,
# Snowflake and Sql -- all genuine connectors, none in this set.
_VALUE_NAMESPACES = frozenset({
    "Binary", "BinaryFormat", "Byte", "Character", "Combiner", "Comparer",
    "Currency", "Date", "DateTime", "DateTimeZone", "Decimal", "Diagnostics",
    "Double", "Duration", "Error", "Expression", "Function", "Guid", "Int8",
    "Int16", "Int32", "Int64", "List", "Logical", "Number", "Percentage",
    "Record", "Replacer", "Single", "Splitter", "Table", "Text", "Time",
    "Type", "Uri", "Value",
})


def _check_supported(function):
    """Name the real cause. A connector is unsupported *as a connector*; saying
    it "has no resolvable input" is true and useless -- a source step has no
    input by definition.

    `Text.Upper` used to come out as M91_UNSUPPORTED_CONNECTOR because the
    rule was "namespaced and not `Table.`". It is a function, not a
    connector, and the wrong reason sends the reader looking for a data
    source that is not in the Dataflow at all.
    """
    if function in _SUPPORTED:
        return
    namespace = function.split(".")[0] if _CONNECTOR.match(function) else ""
    if namespace and namespace not in _VALUE_NAMESPACES:
        raise Blocked("M91:unsupported connector %s" % function)
    raise Blocked("unsupported step function %s" % function)


def _bare_reference(text) -> str:
    """`((#"Source"))` -> `Source`; anything else unchanged but stripped."""
    text = str(text or "").strip()
    while text.startswith("(") and text.endswith(")") and _balanced(text[1:-1]):
        text = text[1:-1].strip()
    return unquote_identifier(text)


def _balanced(text) -> bool:
    depth = 0
    for char in text:
        depth += {"(": 1, ")": -1}.get(char, 0)
        if depth < 0:
            return False
    return depth == 0


def _input_var(step, names):
    """The frame a step transforms. Never guessed.

    This used to fall back to the previous frame when the table argument was
    not a bare step name -- which is exactly the Power Query editor's nested
    shape, `Table.TransformColumnTypes(Table.AddColumn(Prev, ...), ...)`. Only
    the outer call was translated and the inner one vanished with no finding,
    so a nested `Table.SelectRows` meant unfiltered rows written under PASS.
    """
    # `#"Source"` and `Source` name the same step in M, and `(Source)` is the
    # same value; compare on the unquoted, unparenthesised form.
    by_name = {unquote_identifier(name): var for name, var in names.items()}
    for candidate in list(step.get("inputs") or []) + list(step.get("args") or []):
        key = _bare_reference(candidate)
        if key in by_name:
            return by_name[key]
    table = str((step.get("args") or ["<none>"])[0]).strip()
    raise Blocked("%s reads %s, which is not a step of this query (a nested call "
                  "or another query); it is not translated rather than guessed"
                  % (step.get("fn"), table[:80]))


def _apply(step, function, var, names, scope, findings, rules, helpers,
           readers):
    if function == "Table.AddColumn":
        rules.append("M20_ADD_COLUMN")
        return _rule_add_column(step, var, scope, findings, helpers)
    if function == "Table.TransformColumnTypes":
        rules.append("M21_TRANSFORM_TYPES")
        return _rule_transform_types(step, var, findings, helpers)
    if function == "Table.SelectRows":
        rules.append("M22_SELECT_ROWS")
        return _rule_select_rows(step, var, scope, findings, helpers)
    if function == "Table.RenameColumns":
        rules.append("M23_RENAME_COLUMNS")
        return _rule_rename(step, var, findings, helpers)
    if function == "Table.SelectColumns":
        rules.append("M24_SELECT_COLUMNS")
        return _rule_select_columns(step, var, findings)
    if function == "Table.RemoveColumns":
        rules.append("M25_REMOVE_COLUMNS")
        return _rule_remove_columns(step, var, findings, helpers)
    if function == "Table.Combine":
        rules.append("M27_COMBINE")
        return _rule_combine(step, names, findings)
    if function in PASS_THROUGH:
        rules.append(_rule_pass_through(function, var, readers, findings))
        return var
    raise Blocked("unsupported step function %s" % function)
