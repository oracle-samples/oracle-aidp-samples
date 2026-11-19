"""Scan Warehouse items from a Fabric Git export.

A Warehouse commits as a DacFx database project: one `.sql` file per object.
The on-disk layout varies with the tooling that produced it, so this globs
`**/*.sql` rather than assuming `schemas/<s>/tables/<t>.sql`.

Classification is by the first `CREATE` outside comments and string literals.
A file that cannot be classified becomes `other` and is carried through to the
report as REVIEW — never dropped, because an unrecognised object is exactly
the thing a migration must not lose. A file that cannot be *decoded* is
carried too, with `read_error` set and a name derived from its path; see
`_detect_encoding` and `scan` for why both halves of that matter.
"""
from __future__ import annotations

import codecs

from fabric_aidp.inventory.git_workspace import items_of_type
from fabric_aidp.translate.sql_text import (
    mask_literals, referenced_tables, strip_sql_comments, unterminated_span,
)
import re

_KIND_BY_KEYWORD = {
    "TABLE": "table",
    "VIEW": "view",
    "PROCEDURE": "procedure",
    "PROC": "procedure",
    "FUNCTION": "function",
}
_IDENT = r"(?:\[[^\]]+\]|\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_$#]*)"
_CREATE_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+ALTER\s+)?(?P<kw>TABLE|VIEW|PROCEDURE|PROC|FUNCTION)\s+"
    r"(?:(?P<schema>" + _IDENT + r")\s*\.\s*)?(?P<name>" + _IDENT + r")",
    re.IGNORECASE,
)
DEFAULT_SCHEMA = "dbo"

# The objects a statement reads or writes. `INTO`, `UPDATE` and `MERGE` are
# here beside `FROM` and `JOIN` because a procedure that writes a table
# needs that table to exist first, which is the same ordering edge.
_REFERENCE_KEYWORDS = ("FROM", "JOIN", "INTO", "UPDATE", "MERGE")
# UTF-32's BOMs come first: `BOM_UTF32_LE` is `BOM_UTF16_LE` followed by two
# NULs, so testing UTF-16 first would claim every UTF-32LE file.
_BOMS = (
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF8, "utf-8-sig"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
)
# Tried, in NUL-position order, when there is no BOM and a NUL byte.
_NUL_CANDIDATES = ("utf-16-le", "utf-16-be", "utf-32-le", "utf-32-be")


def _detect_encoding(data: bytes) -> str:
    """The encoding of a `.sql` file, from its bytes rather than by assumption.

    This read `path.read_text(encoding="utf-8-sig")` inside an
    `except UnicodeError`, which handles the encodings that *raise* and is
    blind to the one that does not. A BOM-less UTF-16LE or UTF-16BE file --
    what `sqlcmd -u` and PowerShell's `>` both write -- is ASCII interleaved
    with NUL, and that byte sequence is valid UTF-8. Measured on
    `CREATE TABLE dbo.claim (id INT IDENTITY(1,1) PRIMARY KEY);`:

        utf-16-le    DECODED   classify_sql_object -> ('other', '', '')
        utf-16-be    DECODED   classify_sql_object -> ('other', '', '')
        utf-16 (BOM) RAISED    utf-32 RAISED   cp1252 RAISED   latin-1 RAISED

    The two that decoded are the damaging ones: the CREATE is invisible, the
    object is misclassified `other`, the name falls back to the file stem,
    and NUL-laden text reaches the artifact with zero findings.

    A `.sql` file has no NUL *character* in it, so a NUL *byte* and no BOM
    says "this is not UTF-8" without needing to guess. Which UTF-16 order it
    is shows in where those NULs sit: LE puts the zero high byte at odd
    offsets, BE at even ones. The order that skews is tried first and then
    *checked* -- text that still holds a NUL character means the guess was
    wrong, so the other order gets its turn. Measured, on the DDL above:
    `utf-16-le` bytes decode as UTF-16BE to CJK with no NUL character, which
    is why the decode alone is not enough to tell them apart and the byte
    positions are consulted first.

    UTF-32 is in the candidate list for the same reason UTF-16 is: BOM-less
    UTF-32 is also NUL-heavy and also decodes as UTF-8 without raising
    (a NUL byte is a valid UTF-8 NUL character), so leaving it out would
    have closed the silent path for one encoding and left it open for
    another. Measured: BOM-less `utf-32-le` and `utf-32-be` both reached
    `classify_sql_object` as ('other', '', '') with the NULs intact.

    Two limits, stated rather than papered over. A BOM-less UTF-16 file
    with no character below U+0100 anywhere in it has no NUL byte and is
    not detected. And a NUL-bearing file that is none of these raises here,
    so it is reported unreadable rather than carried as text -- a `.sql`
    file has no NUL character in it, so "unreadable" is the true answer.
    """
    for bom, encoding in _BOMS:
        if data.startswith(bom):
            return encoding
    if b"\x00" not in data:
        return "utf-8-sig"
    head, candidates = data[:4096], list(_NUL_CANDIDATES)
    if head[1::2].count(0) <= head[0::2].count(0):
        candidates[0], candidates[1] = candidates[1], candidates[0]
    for encoding in candidates:
        try:
            if "\x00" not in data.decode(encoding):
                return encoding
        except UnicodeError:
            continue
    raise UnicodeError(
        "the file holds NUL bytes and decodes as none of UTF-16LE, "
        "UTF-16BE, UTF-32LE or UTF-32BE; a .sql file has no NUL character "
        "in it, so this is not SQL text")


def _unquote(ident: str) -> str:
    ident = ident.strip()
    if len(ident) >= 2 and ident[0] == "[" and ident[-1] == "]":
        return ident[1:-1]
    if len(ident) >= 2 and ident[0] == '"' and ident[-1] == '"':
        return ident[1:-1]
    return ident


def _header_end(sql: str, end: int) -> int:
    """The last offset a `CREATE ... name` header could still have run to.

    `end` is where the pattern stopped. A `.` directly after it (spaces
    allowed) means the name that was read is really a schema and the object's
    own name is what the pattern could not reach.
    """
    while end < len(sql) and sql[end] in " \t":
        end += 1
    if sql[end:end + 1] != ".":
        return end
    end += 1
    while end < len(sql) and sql[end] in " \t":
        end += 1
    return end


def classify_sql_object(sql: str):
    """(kind, schema, name) for a DacFx object file.

    A file whose quote, bracket or block comment is left open at end of input
    is refused rather than classified, and that is the whole of the second
    half of this function. The masking cannot tell a truncated file from a
    complete one -- it just reads the rest as literal or comment text -- so
    what came back was not "no answer" but a confident wrong one. Measured
    before this, on an export of three files:

      /* header                       -> ('other', '', '')
      CREATE TABLE dbo.address (id INT)   the CREATE is inside the comment

      CREATE TABLE dbo.[claim (id INT)  -> ('table', 'dbo', 'dbo')
      CREATE TABLE dbo.[policy (id INT) -> ('table', 'dbo', 'dbo')
        $ fabric-aidp plan m.json -o p.json
        error: duplicate asset id(s): warehouse.W.table.dbo.dbo
        exit 2

    The unterminated `[` swallows the name, the schema is read as the name,
    and two objects collapse onto one asset id -- so `plan` refuses the
    WHOLE workspace over two malformed files, which is the same end
    `_name_from_path` was written to prevent for the undecodable case.

    The test is positional rather than "is anything unterminated": a break
    after the header does not make the header wrong.
    `CREATE TABLE dbo.claim (id INT, n VARCHAR(10) DEFAULT 'abc` still
    classifies as ('table', 'dbo', 'claim'), which is the right answer, and
    `scan` records the malformation separately. The translator flags the
    file either way -- SQ01_UNTERMINATED_* -- so nothing is lost by
    declining to guess here.

    "After the header" means after the point the header could still have
    continued, which is the whole of the `[claim` case: on
    `CREATE TABLE dbo.[claim` the pattern matches only as far as `dbo`,
    offsets 0 to 16, reading the schema as the name, and the `[` that
    swallowed the real name sits at 17 -- one past the match, with the `.`
    in between. A `.` directly after the match means the name read is not
    the whole name, so the end of the header is taken to include it.
    """
    # T-SQL's reading of `"..."`: an identifier. A DacFx file is T-SQL under
    # QUOTED_IDENTIFIER ON, and the shared module defaults to Spark's reading,
    # where `"dbo"."t"` is a string literal -- which blanked the name and
    # classified the object as ('table', '   ', ' ').
    view = strip_sql_comments(sql, quoted_identifier=True)
    match = _CREATE_RE.search(mask_literals(view, quoted_identifier=True))
    if not match:
        return ("other", "", "")
    found = unterminated_span(sql, quoted_identifier=True)
    if found is not None and found[1] <= _header_end(sql, match.end()):
        return ("other", "", "")
    kind = _KIND_BY_KEYWORD[match.group("kw").upper()]
    schema = _unquote(match.group("schema")) if match.group("schema") else DEFAULT_SCHEMA
    return (kind, schema, _unquote(match.group("name")))


def referenced_objects(sql) -> list:
    """The objects one DacFx file reads or writes, as it spells them.

    Literal names only, and comments and string literals are masked off
    first, so a table named in prose is not a dependency. Resolving these
    against the warehouse's own objects is the planner's job -- the
    manifest records what the SQL says, the same way a notebook's `%run`
    edges are recorded as written and resolved later.

    Four things are deliberately not references: a derived table
    (`FROM (SELECT ...)`), a table-valued function call (the name is
    followed by `(`), a temp table or variable (`#t`, `@t` -- neither
    starts an identifier here), and a common table expression.
    """
    return referenced_tables(sql, keywords=_REFERENCE_KEYWORDS,
                             quoted_identifier=True)
def _name_from_path(relative: str) -> str:
    """A stable identity for a file whose contents could not be read.

    The name used to be `f"<unreadable: {exc}>"`, and the planner puts a
    warehouse object's name straight into its asset id. A codec error is not
    an identity: it names the *offset of the first bad byte*, so two
    different files that break at the same offset get the same id and `plan`
    refuses the whole workspace. Reproduced end to end -- two files, both
    `...DEFAULT 'caf\\xe9');`, first bad byte at 46:

        $ fabric-aidp inventory /tmp/t4ws -o m.json     # exit 0, no complaint
        $ fabric-aidp plan m.json -o p.json
        error: duplicate asset id(s): warehouse.AcmeDW.other.<unreadable:
          'utf-8' codec can't decode byte 0xe9 in position 46: invalid
          continuation byte>
        exit 2

    The path is the identity instead: it is unique within the item by
    definition, it is the same on every run, and for the usual flat layout
    it reads exactly like the stem the readable-but-unclassified files
    already fall back to.
    """
    stem = relative[:-4] if relative.lower().endswith(".sql") else relative
    return stem.replace("/", ".") or relative


# `unterminated_span`'s three kinds, spelled for a sentence.
_A_OR_AN = {"literal": "a string literal",
            "identifier": "an identifier",
            "comment": "a block comment"}


def scan(items, *, log=None) -> dict:
    warehouses = []
    object_counts: dict = {}
    unreadable_objects = 0
    unterminated_objects = 0

    for item in items_of_type(items, "Warehouse"):
        objects = []
        for path in sorted(item.path.rglob("*.sql"), key=lambda p: p.as_posix()):
            if not path.is_file():
                continue
            relative = path.relative_to(item.path).as_posix()
            read_error, unterminated = "", ""
            try:
                data = path.read_bytes()
                sql = data.decode(_detect_encoding(data))
            except (OSError, UnicodeError) as exc:
                sql = ""
                kind, schema, name = "other", "", _name_from_path(relative)
                read_error = str(exc)
                unreadable_objects += 1
                if log:
                    log(f"  UNREADABLE {item.name}.Warehouse/{relative} — {exc}")
            else:
                kind, schema, name = classify_sql_object(sql)
                found = unterminated_span(sql, quoted_identifier=True)
                if found is not None:
                    what, offset = found
                    unterminated = (
                        f"{_A_OR_AN.get(what, 'a ' + what)} opened at offset "
                        f"{offset} (line {sql.count(chr(10), 0, offset) + 1}) "
                        f"is never closed, so everything after it reads as "
                        f"{what} text -- a truncated file or a partial "
                        f"export")
                    unterminated_objects += 1
                    if not name:
                        # Same call as the undecodable case, and for the same
                        # reason: the path is a unique, stable identity and a
                        # guessed name is not. Two files whose `[` swallowed
                        # the table name both came out `dbo`, and `plan`
                        # refused the whole workspace on a duplicate asset id.
                        name = _name_from_path(relative)
                    if log:
                        log(f"  UNTERMINATED {item.name}.Warehouse/{relative}"
                            f" — {unterminated}")
            objects.append({
                "kind": kind,
                "schema": schema,
                "name": name or path.stem,
                "file": relative,
                # Always present, never None, like the Dataflow scanner's
                # `metadata_error`: a key that appears only on failure is a
                # key every reader forgets to check.
                "read_error": read_error,
                # Decoded, but malformed: a quote, bracket or block comment
                # left open at end of input. Separate from `read_error`
                # because the actions differ -- one needs the file's encoding
                # fixed, the other needs the missing half of the file -- and
                # because a file can be perfectly readable and still be half
                # of an object. The translator raises SQ01_UNTERMINATED_* for
                # the same condition at migrate time; this is the inventory
                # saying it before anything is planned.
                "unterminated": unterminated,
                "sql": sql,
                # A view over a table has to be created after it. The plan
                # claims to order a producer before its consumers and did
                # it for notebook `%run` edges only; `depends_on` was the
                # literal `[]` for every warehouse object, so a view could
                # be emitted first. The names are recorded here as written
                # and resolved against this warehouse's own objects in the
                # planner.
                "reads": referenced_objects(sql),
            })
            object_counts[kind] = object_counts.get(kind, 0) + 1
        warehouses.append({
            "name": item.name,
            "logical_id": item.logical_id,
            "folder": item.folder,
            "objects": objects,
        })
        if log:
            log(f"  warehouse {item.name} ({len(objects)} objects)")

    return {
        # `unreadable_object_count` is in the summary, which `summarize()`
        # prints on the source's own line, because that is the only place an
        # operator reliably looks. It is NOT added to the manifest's
        # top-level `unreadable_items`: that list is item-DIRECTORY scoped --
        # `discover_items` fills it with directories whose `.platform` will
        # not read, `summarize()` prints it as "item director(ies) could not
        # be read and were not scanned", and `planner._unreadable_assets`
        # turns every entry into a second asset (`unreadable.<path>`,
        # target `aidp_unmigrated`). A `.sql` file inside a Warehouse that
        # *did* read is neither of those: it already reaches the plan as
        # `warehouse.<item>.other.<name>`, so putting it in the list would
        # count one file as two assets and claim the Warehouse item was
        # unreadable when it was not. The precedent followed instead is the
        # Dataflow scanner's `metadata_unreadable`, which is exactly this
        # shape: an object inside a readable item that would not read.
        #
        # `unterminated_object_count` sits beside it for the same reason and
        # counts the other half of the same problem: a file that decoded and
        # is still not a whole object.
        "summary": {"warehouse_count": len(warehouses),
                    "unreadable_object_count": unreadable_objects,
                    "unterminated_object_count": unterminated_objects,
                    "object_counts": object_counts},
        "items": {"warehouses": warehouses},
    }
