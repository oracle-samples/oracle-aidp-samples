"""Parse and serialize the Fabric notebook Git source format.

Fabric commits notebooks as `notebook-content.py`, not `.ipynb`:

    # Fabric notebook source

    # METADATA ********************

    # META {
    # META   "kernel_info": { "name": "synapse_pyspark" }
    # META }

    # MARKDOWN ********************

    # # Heading

    # CELL ********************

    df = spark.table("claims")

    # METADATA ********************

    # META {
    # META   "language": "python"
    # META }

Markers are a comment, a space, the kind, a space, and a run of asterisks —
20 in every sample observed, but the count is an undocumented Fabric detail,
so the pattern accepts 16 or more. The marker line is stored verbatim and
re-emitted unchanged, so tolerating drift costs nothing and a future count
change does not break the parser.

The first METADATA block is notebook-level; every later one attaches to the
block immediately before it.

Everything is stored verbatim so `serialize_notebook(parse_notebook(x)) == x`
byte-for-byte. That invariant is the contract the whole notebook translator
rests on — see tests/test_notebook_format.py.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

# `PARAMETERS CELL` is a code cell too -- Fabric uses it for the parameter
# block. Omitting it hid one cell per parameterised notebook from every rule.
# A T-SQL notebook (`notebook-content.sql`) uses SQL comment markers for the
# same structure. Knowing only `#`, the parser found no header at all and the
# whole file was reported unparseable.
_C = r"(?:#|--)"
MARKER_RE = re.compile(rf"^{_C} (METADATA|PARAMETERS CELL|CELL|MARKDOWN) \*{{16,}}$")
# Fabric stores a non-Python cell as Python comments: every body line behind
# a `# MAGIC ` prefix. Read literally the cell looks like commentary and its
# code is never seen -- so a `%%sql` body reached the output untranslated.
_MAGIC_RE = re.compile(rf"^{_C} MAGIC ?(.*)$")
HEADER_LINE = "# Fabric notebook source"
# Fabric grew out of Synapse and still exports Synapse-era notebooks under the
# older header. The block structure is identical, so rejecting the file over
# one word threw away a notebook this tool can translate perfectly well.
HEADER_LINES = (HEADER_LINE, "-- Fabric notebook source",
                "# Synapse Analytics notebook source",
                "-- Synapse Analytics notebook source")
_META_PREFIXES = ("# META", "-- META")
# The leading `%%lang` of a Fabric cell, once its `# MAGIC ` prefix is off,
# and the languages it can name. They live here rather than in the notebook
# translator because the inventory catalog has to make the same
# Python-or-SQL call when it scans a notebook for the tables it writes, and
# two copies of this list would drift.
CELL_MAGIC_RE = re.compile(r"^\s*%%([\w-]+)")
SQL_LANGUAGES = frozenset({"sparksql", "sql", "tsql", "t-sql"})
PYTHON_LANGUAGES = frozenset({"python", "pyspark", "synapse_pyspark"})


class NotebookParseError(ValueError):
    """The file is not a well-formed Fabric notebook source file."""


@dataclass
class Block:
    kind: str            # HEADER | METADATA | CELL | PARAMETERS CELL | MARKDOWN
    marker: str          # exact marker line; "" for HEADER
    lines: list = field(default_factory=list)   # verbatim body lines

    @property
    def text(self) -> str:
        """Body as a single string, without the marker."""
        return "\n".join(self.lines)

    @property
    def is_magic(self) -> bool:
        """Whether every non-blank body line carries Fabric's `# MAGIC ` prefix."""
        lines = [line for line in self.lines if line.strip()]
        return bool(lines) and all(_MAGIC_RE.match(line) for line in lines)

    @property
    def magic_body(self) -> str:
        """The body with Fabric's `# MAGIC ` prefix stripped."""
        return "\n".join(
            match.group(1) if (match := _MAGIC_RE.match(line)) else line
            for line in self.lines)


@dataclass
class FabricNotebook:
    blocks: list = field(default_factory=list)
    newline: str = "\n"
    trailing_newline: bool = True

    CODE_KINDS = ("CELL", "PARAMETERS CELL")

    @property
    def code_blocks(self) -> list:
        return [b for b in self.blocks if b.kind in self.CODE_KINDS]

    @property
    def sql_source(self) -> bool:
        """Whether this is a `notebook-content.sql` item: a T-SQL notebook.

        Fabric's T-SQL notebook saves under that name and writes the same
        block structure behind `--` instead of `#`, so the header line is
        what says which one this is. Its cells are T-SQL and run against a
        Warehouse or SQL analytics endpoint -- there is no Python in one.

        The translator needs this because the file itself carries no cell
        magic: a T-SQL notebook's cells are bare SQL, so nothing in the body
        says "this is SQL" and every cell went down the Python path.
        """
        for block in self.blocks:
            if block.kind == "HEADER" and block.lines:
                return block.lines[0].lstrip().startswith("--")
        return False

    def language_for(self, block: Block):
        """The language Fabric recorded for `block`, e.g. `sparksql`.

        It lives in the *following* METADATA block, so a cell cannot answer
        for its own language alone.
        """
        value = self.meta_for(block).get("language")
        return str(value).strip().casefold() if value else None

    @property
    def notebook_meta(self) -> dict:
        """The first METADATA block, decoded. Empty dict when absent or blank."""
        for block in self.blocks:
            if block.kind == "METADATA":
                return _decode_meta(block)
        return {}

    def meta_for(self, block: Block) -> dict:
        """The METADATA block immediately following `block`, decoded."""
        try:
            index = self.blocks.index(block)
        except ValueError:
            return {}
        following = self.blocks[index + 1:index + 2]
        if following and following[0].kind == "METADATA":
            return _decode_meta(following[0])
        return {}

    @property
    def lakehouse_binding(self) -> dict:
        """The notebook's `lakehouse` metadata block, or {}."""
        return lakehouse_binding_of(self.notebook_meta)

    @property
    def default_lakehouse(self):
        """Display name of the bound default lakehouse, else its GUID, else None."""
        return default_lakehouse_of(self.lakehouse_binding)

    @property
    def default_lakehouse_id(self):
        """The bound lakehouse's workspace item id, or None.

        Read separately from `default_lakehouse`, which returns the display
        name when there is one and so threw the GUID away. The pair matters:
        Fabric writes `default_lakehouse` and `default_lakehouse_name` in the
        same object, and that id is the *workspace item id* -- the same
        identifier a Dataflow's `lakehouseId` navigates by. It is the only
        place a Fabric Git export writes a lakehouse GUID and its name down
        together, so discarding half of it left every Dataflow read and
        write unable to name the lakehouse it touches.

        Not to be confused with the Lakehouse item's `.platform`
        `config.logicalId`, which is a git-generated identifier: across the
        3 Lakehouse items available here it matches none of the 29 distinct
        lakehouseIds the Dataflow corpus navigates to.
        """
        return default_lakehouse_id_of(self.lakehouse_binding)


# The three lakehouse-binding questions, as functions rather than methods.
# Both notebook classes have to answer all three identically -- a caller
# holds whichever `parse_any` returned and cannot see the difference -- and
# they were implemented once each instead. `IpynbNotebook` then grew a
# `default_lakehouse` of its own that read only `dependencies`, and never
# grew a `default_lakehouse_id` at all, so the inventory raised
# AttributeError on the first .ipynb notebook it met. The methods above and
# on `IpynbNotebook` are thin wrappers over these now.

def lakehouse_binding_of(meta) -> dict:
    """The `lakehouse` object inside notebook-level metadata, or {}.

    Fabric nests it under `dependencies`; Synapse-era exports use
    `synapse`. Same keys underneath either way.
    """
    if not isinstance(meta, dict):
        return {}
    for parent in ("dependencies", "synapse"):
        block = meta.get(parent)
        if isinstance(block, dict) and isinstance(block.get("lakehouse"), dict):
            return block["lakehouse"]
    return {}


def default_lakehouse_of(lakehouse):
    """Display name of the bound default lakehouse, else its GUID, else None."""
    if not isinstance(lakehouse, dict):
        return None
    name = lakehouse.get("default_lakehouse_name")
    if isinstance(name, str) and name.strip():
        return name
    guid = lakehouse.get("default_lakehouse")
    return guid if isinstance(guid, str) and guid.strip() else None


def default_lakehouse_id_of(lakehouse):
    """The bound lakehouse's workspace item id, or None."""
    if not isinstance(lakehouse, dict):
        return None
    guid = lakehouse.get("default_lakehouse")
    return guid.strip() if isinstance(guid, str) and guid.strip() else None


def _decode_meta(block: Block) -> dict:
    payload_lines = []
    for line in block.lines:
        for prefix in _META_PREFIXES:
            if line.startswith(prefix + " "):
                payload_lines.append(line[len(prefix) + 1:])
                break
            if line == prefix:
                payload_lines.append("")
                break
    payload = "\n".join(payload_lines).strip()
    if not payload:
        return {}
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise NotebookParseError(f"malformed # META JSON: {exc}") from exc
    return value if isinstance(value, dict) else {}


def check_meta(nb) -> None:
    """Decode every `# META` block now, raising NotebookParseError on the first bad one.

    `notebook_meta` and `meta_for` decode lazily, so a notebook with one
    malformed block -- a trailing comma is enough -- parses cleanly and then
    raises from whichever property first touches that block. The inventory
    touched it outside its per-notebook guard: measured on a five-notebook
    export with one bad block, the whole notebook source reported FAILED and
    plan, migrate and verify saw no notebooks at all, each exiting 0. A
    caller that must not be ambushed later calls this first, while it can
    still attribute the failure to one notebook.

    An .ipynb notebook decoded its metadata with the rest of the JSON, so
    there is nothing left to check.
    """
    if not isinstance(nb, FabricNotebook):
        return
    cell = 0
    for block in nb.blocks:
        if block.kind in ("CELL", "PARAMETERS CELL", "MARKDOWN"):
            cell += 1
        if block.kind != "METADATA":
            continue
        try:
            _decode_meta(block)
        except NotebookParseError as exc:
            # Blamed by position: a block before the first cell is the
            # notebook's, one after cell N is cell N's. This used to ask
            # whether any METADATA block had decoded yet, which called cell
            # 1's block the notebook's whenever the notebook had none.
            where = ("the notebook-level METADATA block" if cell == 0
                     else f"the METADATA block of cell {cell}")
            raise NotebookParseError(f"{exc} (in {where})") from exc


def parse_notebook(text: str) -> FabricNotebook:
    """Split Fabric notebook source into verbatim blocks.

    Raises NotebookParseError when the leading header line is absent — that is
    the only structural requirement, and its absence means this is not a
    Fabric notebook source file.
    """
    if not isinstance(text, str):
        raise NotebookParseError("notebook source must be a string")
    newline = "\r\n" if "\r\n" in text else "\n"
    # Split on the file's own line terminator, not on `str.splitlines()`.
    # `splitlines()` also breaks on \x0b, \x0c, \x1c, \x1d, \x1e, \x85,
    #  ,   and a lone \r, and `serialize_notebook` rejoins with
    # `newline` -- so each of those nine characters came back as a newline
    # and the byte-exact round trip this module's docstring promises was
    # broken. Measured on `x = "a\x0bb"` in a CELL: out is `x = "a\nb"`, a
    # raw newline inside a single-quoted literal, which is a SyntaxError.
    # A form feed between two functions is ordinary Python and survives;
    #   and \x85 inside a string literal are ordinary data.
    #
    # `split` leaves a trailing "" where `splitlines` leaves nothing, and
    # `serialize_notebook` re-adds the terminator from `trailing_newline`,
    # so that entry is dropped here. Reading `trailing_newline` off the
    # detected terminator rather than off any newline character is part of
    # the same fix: a file whose last byte is a lone \r used to gain a \n.
    trailing_newline = text.endswith(newline)
    lines = text.split(newline)
    if trailing_newline:
        lines.pop()
    if not lines or lines[0].strip() not in HEADER_LINES:
        raise NotebookParseError(
            f"expected first line to be one of {HEADER_LINES!r}, "
            f"got {(lines[0] if lines else '')!r}"
        )

    blocks = [Block(kind="HEADER", marker="", lines=[lines[0]])]
    for line in lines[1:]:
        match = MARKER_RE.match(line)
        if match:
            blocks.append(Block(kind=match.group(1), marker=line, lines=[]))
        else:
            blocks[-1].lines.append(line)
    return FabricNotebook(blocks=blocks, newline=newline,
                          trailing_newline=trailing_newline)


def serialize_notebook(nb: FabricNotebook) -> str:
    """Rebuild the source file from blocks.

    Invariant: serialize_notebook(parse_notebook(x)) == x for any x that parses.
    Only `Block.lines` is ever mutated by a translator, so everything else is
    reproduced exactly as read.
    """
    out_lines = []
    for block in nb.blocks:
        if block.marker:
            out_lines.append(block.marker)
        out_lines.extend(block.lines)
    text = nb.newline.join(out_lines)
    if nb.trailing_newline and text:
        text += nb.newline
    return text


def parse_any(text):
    """Parse either notebook format.

    Fabric commits notebooks as `notebook-content.py` (the `# CELL` line
    format) or `notebook-content.ipynb` (Jupyter JSON). Most real exports use
    the latter, so dispatch on content rather than trusting a filename.
    """
    from fabric_aidp.translate.ipynb_format import IpynbParseError, parse_ipynb

    if isinstance(text, str) and text.lstrip().startswith("{"):
        return parse_ipynb(text)
    try:
        return parse_notebook(text)
    except NotebookParseError:
        # A .py-looking file that is not one may still be JSON with leading
        # blank lines; give the other parser a chance before failing.
        try:
            return parse_ipynb(text)
        except IpynbParseError:
            raise


def serialize_any(nb) -> str:
    """Serialize whichever notebook object `parse_any` returned."""
    from fabric_aidp.translate.ipynb_format import IpynbNotebook, serialize_ipynb

    if isinstance(nb, IpynbNotebook):
        return serialize_ipynb(nb)
    return serialize_notebook(nb)
