"""Resolve table references against a catalog inferred from the export.

Fabric exports no lakehouse table list, so `spark.table("claims")` has no
authoritative target. Treating every such reference as REVIEW would make
nearly every notebook REVIEW and drain the flag of meaning, so the catalog is
assembled from what the export does contain, in precedence order:

  1. warehouse_ddl        CREATE TABLE / CREATE VIEW      authoritative
  2. shortcut             shortcuts.metadata.json         authoritative
  3. supplied             --catalog CSV                   authoritative
  4. notebook_inferred    saveAsTable / CREATE TABLE      INFERRED, name only

Tier 4 is inference and every consumer must be able to see that: the entry
records `created_by`, never claims columns, and the summary counts tiers
separately so the ratio of guessed to known stays visible.

Lookup is suffix matching against an entry's full path (owning item, then
its own qualified name), never against the last part alone -- see
`candidates`. A reference that matches nothing and one that matches several
are different answers, and both are reported rather than guessed.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

from fabric_aidp.sources import is_table_section
from fabric_aidp.translate.ipynb_format import IpynbParseError
from fabric_aidp.translate.notebook_format import (
    CELL_MAGIC_RE, PYTHON_LANGUAGES, SQL_LANGUAGES, NotebookParseError,
    check_meta, parse_any,
)
from fabric_aidp.translate.python_text import Unmaskable, masked_python
from fabric_aidp.translate.sql_text import masked as masked_sql
from fabric_aidp.translate.sql_text import referenced_tables

TIER_WAREHOUSE_DDL = "warehouse_ddl"
TIER_SHORTCUT = "shortcut"
TIER_SUPPLIED = "supplied"
TIER_NOTEBOOK_INFERRED = "notebook_inferred"
TIER_ORDER = (TIER_WAREHOUSE_DDL, TIER_SHORTCUT, TIER_SUPPLIED, TIER_NOTEBOOK_INFERRED)
_RANK = {tier: index for index, tier in enumerate(TIER_ORDER)}

_QUOTED = r"""(?P<q>['"])(?P<val>(?:\\.|(?!(?P=q)).)*)(?P=q)"""
_SAVE_AS_TABLE_RE = re.compile(r"\.saveAsTable\s*\(\s*" + _QUOTED, re.S)
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[A-Za-z_][\w$]*(?:\s*\.\s*[A-Za-z_][\w$]*){0,2})",
    re.IGNORECASE,
)
_COLUMN_RE = re.compile(
    r"^\s*(?:\[(?P<b>[^\]]+)\]|\"(?P<q>[^\"]+)\"|(?P<n>[A-Za-z_][\w$]*))\s+(?P<type>[A-Za-z][\w]*(?:\s*\([^)]*\))?)"
)
_CREATE_TABLE_HEAD = re.compile(r"\bCREATE\s+TABLE\b[^(]*\(", re.IGNORECASE)
_ALTER_TABLE = re.compile(r"\bALTER\s+TABLE\b", re.IGNORECASE)
# `spark.table("x")` and `spark.read.table("x")`. The session is routinely
# bound to another name, so the receiver is any identifier rather than the
# literal `spark`.
_READ_TABLE_RE = re.compile(
    r"(?<![\w.])[A-Za-z_]\w*\.(?:read\.)?table\s*\(\s*" + _QUOTED, re.S)
# A notebook's *reads*: FROM and JOIN only. Its writes are `saveAsTable`
# and `CREATE TABLE`, which `extract_written_tables` already finds, and an
# `INSERT INTO` names a table this notebook is filling rather than one it
# consumes.
_READ_KEYWORDS = ("FROM", "JOIN")

# Every way a notebook registers a session-local view. A temp view is not a
# catalog table: a bare reference to it afterwards means the view, not a
# same-named table somewhere else, so it is neither a resolvable name nor a
# dependency on whoever writes that table. These live here rather than in
# the notebook translator because that module imports this one, and because
# "this name is not a catalog table" is precisely this module's subject.
TEMP_VIEW_PY_RE = re.compile(
    r"(?<!\w)create(?:OrReplace)?(?:Global)?TempView\s*\(\s*"
    r"(?P<q>['\"])(?P<name>[^'\"]+)(?P=q)")
TEMP_VIEW_SQL_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[A-Za-z_][\w$]*)",
    re.IGNORECASE)


def temp_view_names(text) -> frozenset:
    """Every name a run of notebook source registers as a temp view.

    Scanned over the raw text, unmasked and without regard to order,
    which over-approximates in two directions -- a registration inside a
    comment counts, and one that executes after the reference counts.
    Both errors point the same way: the name is left exactly as written
    and no dependency is drawn to it. The opposite error reads a
    different object.
    """
    names = set()
    if not isinstance(text, str):
        return frozenset()
    for pattern in (TEMP_VIEW_PY_RE, TEMP_VIEW_SQL_RE):
        for match in pattern.finditer(text):
            name = match.group("name").strip()
            # A temp view name is one unqualified identifier; anything
            # dotted is something else.
            if name and "." not in name:
                names.add(name.casefold())
    return frozenset(names)


def _normalise(name) -> str:
    """Casefolded, quote-stripped, whitespace-free identifier for lookups."""
    if not isinstance(name, str):
        return ""
    parts = []
    for part in name.split("."):
        part = part.strip()
        if len(part) >= 2 and part[0] == "[" and part[-1] == "]":
            part = part[1:-1]
        part = part.strip("`").strip('"').strip()
        if part:
            parts.append(part)
    return ".".join(parts).casefold()


def _plain_literal_spans(source: str, view) -> list:
    """`(value_start, value_end)` of every non-f-string literal in `source`.

    An f-string is excluded because its value is only half written down:
    `f"CREATE TABLE mart.{name}"` would contribute a table called `mart`,
    which is not a table at all.
    """
    return [(value_start, value_end)
            for start, _end, value_start, value_end in view.literals
            if "f" not in source[start:value_start].casefold()]


def _written_in_python(source: str) -> set:
    """Tables a run of Python source creates.

    Scanned against a mask of the source, so a `saveAsTable` that is
    commented out or quoted in someone's prose is not a write. Measured on
    the bundled estate before this: `#Check if Target exist, if not create
    table and exit` in edkreuk_FMD_FRAMEWORK_005 put a table called `and`
    in the catalog, and a reader of `and` would have resolved to it at the
    `notebook_inferred` tier instead of being flagged unknown -- a wrong
    answer dressed as a known one.
    """
    found: set = set()
    try:
        view = masked_python(source)
    except Unmaskable:
        # The literals cannot be located, so no match can be trusted to be
        # code rather than prose. Contributing nothing leaves a reader of
        # whatever this cell writes flagged NB12_TABLE_UNKNOWN, which is
        # the visible answer; guessing would be the invisible one.
        return found
    plain = _plain_literal_spans(source, view)

    for match in _SAVE_AS_TABLE_RE.finditer(source):
        # The argument has to be exactly one plain literal's value: a
        # comment contributes no literal at all, and a `saveAsTable(...)`
        # written inside a longer string is part of that string's value,
        # not its own.
        if (match.start("val"), match.end("val")) not in plain:
            continue
        value = match.group("val").strip()
        if value:
            found.add(value)

    for match in _CREATE_TABLE_RE.finditer(source):
        # CREATE TABLE in Python source is SQL handed to `spark.sql(...)`,
        # so unlike the call above it must be *inside* a literal. A `#`
        # comment is not one.
        if not any(start <= match.start() and match.end() <= end
                   for start, end in plain):
            continue
        name = re.sub(r"\s*\.\s*", ".", match.group("name").strip())
        if name:
            found.add(name)
    return found


def _written_in_sql(source: str) -> set:
    """Tables a SQL cell creates. `masked` blanks comments and literals."""
    found = set()
    for match in _CREATE_TABLE_RE.finditer(masked_sql(source)):
        name = re.sub(r"\s*\.\s*", ".", match.group("name").strip())
        if name:
            found.add(name)
    return found


def _cells(source: str) -> list:
    """`(body, "python" | "sql")` for each code cell of a notebook.

    Fabric's `.py` notebook format carries a non-Python cell as Python
    *comments*, every body line behind `# MAGIC `. So masking Python
    comments over the raw file would throw every `%%sql` cell away along
    with the commentary -- the source has to be split into cells first and
    each read in its own language.

    Anything that is not a notebook is one Python cell: this function is
    also called with a bare snippet, by tests and by anything outside the
    inventory.
    """
    try:
        notebook = parse_any(source)
        # `language_for` below decodes each cell's `# META` lazily; one bad
        # block raised out of build_catalog and took the whole inventory
        # down with it, after the scanner had recorded that very notebook
        # as a parse error. Treated like any notebook that will not parse.
        check_meta(notebook)
    except (NotebookParseError, IpynbParseError):
        return [(source, "python")]

    blocks = getattr(notebook, "blocks", None) or []
    header = blocks[0].lines[0] if blocks and blocks[0].lines else ""
    # A T-SQL notebook (`notebook-content.sql`) uses `--` for the same
    # markers, and its cells are SQL with no magic to say so.
    default_kind = "sql" if header.startswith("--") else "python"

    out = []
    for block in notebook.code_blocks:
        body = block.magic_body if getattr(block, "is_magic", False) else block.text
        first = next((line for line in body.split("\n") if line.strip()), "")
        declared = CELL_MAGIC_RE.match(first)
        magic = declared.group(1).casefold() if declared else None
        language = (notebook.language_for(block)
                    if hasattr(notebook, "language_for") else None)
        if magic in SQL_LANGUAGES or language in SQL_LANGUAGES:
            out.append((body, "sql"))
        elif magic and magic not in PYTHON_LANGUAGES:
            # %%markdown, %%r, %%configure and friends: whatever they hold,
            # it is not a table write this reads. The notebook translator
            # refuses them by name (NB22/NB23); nothing is dropped here
            # that is not refused there.
            continue
        else:
            out.append((body, default_kind if language is None and not magic
                        else "python"))
    return out


def _read_in_sql(source: str) -> set:
    """Tables a SQL cell reads. `masked` blanks comments and literals."""
    return set(referenced_tables(source, keywords=_READ_KEYWORDS))


def _read_in_python(source: str) -> set:
    """Tables a run of Python source reads.

    Two ways a name arrives: as the argument of a `table(...)` call, and
    inside a SQL string handed to `spark.sql(...)`. Both are checked
    against the same plain-literal spans `_written_in_python` uses, so a
    `spark.table("claims")` written inside someone's prose or behind a
    `#` is not a read.
    """
    found: set = set()
    try:
        view = masked_python(source)
    except Unmaskable:
        return found
    plain = _plain_literal_spans(source, view)

    for match in _READ_TABLE_RE.finditer(source):
        if (match.start("val"), match.end("val")) not in plain:
            continue
        value = match.group("val").strip()
        if value:
            found.add(value)

    # SQL handed to `spark.sql(...)`: the statement is the literal's own
    # text, so it is scanned per literal rather than over the whole cell.
    for start, end in plain:
        found |= _read_in_sql(source[start:end])
    return found


def extract_written_tables(source) -> set:
    """Table names a notebook creates. Literal targets only — never a variable."""
    found: set = set()
    if not isinstance(source, str) or not source:
        return found
    for body, kind in _cells(source):
        found |= _written_in_sql(body) if kind == "sql" else _written_in_python(body)
    return found


def extract_read_tables(source) -> set:
    """Table names a notebook reads. Literal targets only — never a variable.

    The other half of `extract_written_tables`, and the half that was
    missing: the writes were already known, so a notebook that writes
    `agg_claims` and one that reads it were two nodes with no edge, and
    the plan emitted the reader first whenever the names sorted that way.

    A view this notebook registers itself is not a read of anybody's
    table, so temp views are taken out.
    """
    found: set = set()
    if not isinstance(source, str) or not source:
        return found
    for body, kind in _cells(source):
        found |= _read_in_sql(body) if kind == "sql" else _read_in_python(body)
    local = temp_view_names(source)
    return {name for name in found if name.casefold() not in local}


def _columns_from_ddl(sql) -> list:
    """Best-effort column list from a CREATE TABLE body; [] when unknowable.

    An empty list reads as "columns unknown" downstream (`declared_columns`
    returns None), so every doubtful shape answers [] rather than a guess.
    MEASURED wrong lists before, each of which made SQ23 flag a real column:

      -- note (legacy INT) / CREATE TABLE dbo.t (a INT, b INT)   ['legacy']
      (a INT, -- first, then b / b INT)                          ['a', 'then']
      ("q c" INT, b INT)                                         ['b']
      CREATE TABLE dbo.ctas AS SELECT CAST(pid AS INT) AS a2 ... ['pid']
      ... and an ALTER TABLE ... ADD later in the file left `closed` out.

    So the body is read from a masked copy (comments and literals blanked,
    same offsets), the `(` is the one the CREATE TABLE header opens, a CTAS
    has no column list to read, and a file that also ALTERs the table may
    add columns this cannot see.
    """
    if not isinstance(sql, str):
        return []
    view = masked_sql(sql, quoted_identifier=True)
    head = _CREATE_TABLE_HEAD.search(view)
    if (not head or re.search(r"\bAS\b", head.group(0), re.IGNORECASE)
            or _ALTER_TABLE.search(view)):
        return []
    open_index = head.end() - 1
    depth, end = 0, -1
    for index in range(open_index, len(view)):
        if view[index] == "(":
            depth += 1
        elif view[index] == ")":
            depth -= 1
            if depth == 0:
                end = index
                break
    if end == -1:
        return []
    body, columns, depth, current = view[open_index + 1:end], [], 0, []
    for char in body:
        if char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
        if char == "," and depth == 0:
            columns.append("".join(current))
            current = []
        else:
            current.append(char)
    columns.append("".join(current))

    out = []
    for definition in columns:
        match = _COLUMN_RE.match(definition)
        if not match:
            continue
        name = match.group("b") or match.group("q") or match.group("n")
        if name and name.upper() not in {
            "CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "INDEX",
        }:
            out.append({"name": name, "type": " ".join(match.group("type").split())})
    return out


def load_supplied_catalog(path) -> dict:
    """Parse a `--tables-csv` file. Needs a `table` column; `column`/`type` optional.

    The header is matched case- and whitespace-insensitively, because
    `Table,Lakehouse` is what a person writes and what Excel produces.
    The *check* already folded case; the *read* did not -- `row["table"]`
    is `csv.DictReader`'s literal key -- so a capitalised header passed
    validation and then matched no row. `--tables-csv` did nothing, no
    error and no warning, and every table fell through to unknown.

    A file that yields no rows raises. `table;lakehouse` -- a semicolon
    export -- has always raised with a clear message, and it is the same
    mistake: the user handed this a catalog and got no catalog. Failing
    on one and not the other is the inconsistency, and silence is the
    wrong half of it.

    The BOM is handled by `utf-8-sig` above, which is separate and stays.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read catalog file {path}: {exc}") from exc
    reader = csv.DictReader(text.splitlines())
    # {folded header: the key DictReader actually built the row with}. First
    # spelling wins, so a file with both `Table` and `table` reads the one a
    # human would call the header.
    columns: dict = {}
    for field in reader.fieldnames or []:
        columns.setdefault((field or "").strip().casefold(), field)
    if "table" not in columns:
        raise ValueError(
            f"catalog file {path} must have a 'table' column; found: "
            f"{sorted(columns) or 'no header'}"
        )

    def value(row, folded):
        key = columns.get(folded)
        return "" if key is None else (row.get(key) or "").strip()

    supplied: dict = {}
    rows = 0
    for row in reader:
        rows += 1
        name = value(row, "table")
        if not name:
            continue
        entry = supplied.setdefault(name, {"columns": []})
        column = value(row, "column")
        if column:
            entry["columns"].append(
                {"name": column, "type": value(row, "type")})
    if not supplied:
        raise ValueError(
            f"catalog file {path} named no tables: its header is "
            f"{sorted(columns)} and it has {rows} data row(s), none of them "
            f"with a value in the 'table' column. An empty catalog would "
            f"leave every table unresolved with nothing saying why"
        )
    return supplied


def entry_owner(entry) -> str:
    """The Fabric item this entry belongs to, casefolded, or "".

    `owner` is what `_add` records. `warehouse` / `lakehouse` are the
    fields that carried it before there was an `owner`, and a plan written
    by an older release -- or a catalog assembled by hand in a test -- has
    only those, so they are still read.

    Public because the notebook translator has to answer the same
    question. `_known_items` there read `warehouse`/`lakehouse` and
    nothing else, so it and `resolve` disagreed about which Fabric items
    the export names: a Lakehouse known only as the owner of a table some
    notebook writes was visible to one and not the other. One
    implementation is the fix; two is how they came apart.
    """
    entry = entry or {}
    value = (entry.get("owner") or entry.get("warehouse")
             or entry.get("lakehouse") or "")
    return str(value).strip().casefold()


def _same_table(existing: dict, existing_key: str, key: str, owner: str) -> bool:
    """Whether an entry already in the catalog is this same table.

    Same qualified name, and owners that do not contradict. "No owner" is
    not a different owner -- a supplied CSV row and a notebook write say
    nothing about which item holds the table, so treating them as distinct
    from the Warehouse entry that declares it would put one table in the
    catalog twice and make every bare reference to it ambiguous.
    """
    if _normalise(existing.get("name") or existing_key) != key:
        return False
    other = entry_owner(existing)
    return not (other and owner and other != owner)


def _add(tables: dict, name: str, entry: dict, owner: str = "") -> None:
    """Insert `entry` unless an equal-or-higher-precedence entry already exists.

    The key carries the owning Fabric item. Keyed by bare table name, two
    items that each hold a table of the same name could not both be
    represented and one silently shadowed the other -- in the bundled
    estate, `WareSecondary` and `WareSecondary_2` both hold a
    `dbo.CurrentDate` and the catalog listed one of them. The naming design
    (docs/superpowers/specs/2026-09-28-table-naming-design.md) puts a
    Fabric item's tables in one AIDP schema under the item's own name, so
    the item is what makes the name unique there too.

    Nothing parses the key back apart: matching reads `name` and `owner`
    off the entry, so an item whose display name contains a dot --
    `gbrueckl_Fabric.Toolbox` is one in the bundled estate -- stays one
    atom.
    """
    key = _normalise(name)
    if not key:
        return
    owner_key = str(owner or "").strip().casefold()
    entry = dict(entry)
    entry["name"] = name
    entry["owner"] = owner or ""
    for existing_key, existing in list(tables.items()):
        if not _same_table(existing, existing_key, key, owner_key):
            continue
        if _RANK[existing["tier"]] <= _RANK[entry["tier"]]:
            return
        del tables[existing_key]
    tables[f"{owner_key}.{key}" if owner_key else key] = entry


def build_catalog(sources: dict, supplied=None) -> dict:
    tables: dict = {}
    # Every shortcut, whatever section it is in. `tables` below holds only
    # the `Tables` ones, because only those are tables -- but a path read
    # (`Files/<shortcut>/part.parquet`) can point at either, and the data
    # is behind the shortcut in both cases, so the rule that resolves such
    # a path needs the whole list. Kept beside `tables` rather than folded
    # into it: a `Files` shortcut is a folder and putting it where table
    # resolution looks would answer `spark.table("landing")` with it.
    shortcuts: list = []
    sources = sources if isinstance(sources, dict) else {}

    def collection(source_name, key):
        value = sources.get(source_name)
        if not isinstance(value, dict):
            return []
        items = value.get("items")
        if not isinstance(items, dict):
            return []
        rows = items.get(key)
        return rows if isinstance(rows, list) else []

    # Tier 1 — warehouse DDL.
    for warehouse in collection("warehouse", "warehouses"):
        for obj in warehouse.get("objects", []) or []:
            if not isinstance(obj, dict) or obj.get("kind") not in ("table", "view"):
                continue
            schema, name = obj.get("schema") or "", obj.get("name") or ""
            if not name:
                continue
            qualified = f"{schema}.{name}" if schema else name
            entry = {"tier": TIER_WAREHOUSE_DDL, "warehouse": warehouse.get("name", "")}
            if obj.get("kind") == "table":
                entry["columns"] = _columns_from_ddl(obj.get("sql"))
            _add(tables, qualified, entry, owner=warehouse.get("name", ""))

    # Tier 2 — shortcuts in the Tables section of a lakehouse that tracks them.
    for lakehouse in collection("lakehouse", "lakehouses"):
        tracking = lakehouse.get("tracking") or {}
        if tracking.get("shortcuts") != "tracked":
            continue
        for shortcut in lakehouse.get("shortcuts", []) or []:
            if not isinstance(shortcut, dict):
                continue
            if shortcut.get("name"):
                shortcuts.append({
                    "name": shortcut.get("name", ""),
                    "table_name": shortcut.get("table_name") or "",
                    "section": shortcut.get("section", ""),
                    # The folder under the section (`Tables/hr`, `Files/raw`):
                    # two shortcuts of one name in two folders are two objects,
                    # and a `Files` shortcut has no `table_name` to glue it into.
                    "schema": shortcut.get("schema") or "",
                    "lakehouse": lakehouse.get("name", ""),
                    "target": shortcut.get("target", ""),
                    "target_type": shortcut.get("target_type", ""),
                    "bucket": shortcut.get("bucket", ""),
                    "external": bool(shortcut.get("external")),
                })
            if not is_table_section(shortcut.get("section", "")):
                continue
            # Real shortcuts carry a schema in their path ("/Tables/dbo"), so
            # the catalog key is the qualified name the notebook will use.
            name = shortcut.get("table_name") or shortcut.get("name") or ""
            if not name:
                continue
            _add(tables, name, {
                "tier": TIER_SHORTCUT,
                "lakehouse": lakehouse.get("name", ""),
                # The section a path read has to match against: a `Tables`
                # shortcut named `x` and a real `Files/x` folder are two
                # different objects and only one of them is behind the
                # shortcut.
                "section": shortcut.get("section", ""),
                "target": shortcut.get("target", ""),
                "target_type": shortcut.get("target_type", ""),
                "external": bool(shortcut.get("external")),
            }, owner=lakehouse.get("name", ""))

    # Tier 3 — user-supplied.
    for name, value in (supplied or {}).items():
        entry = {"tier": TIER_SUPPLIED}
        columns = (value or {}).get("columns")
        if columns:
            entry["columns"] = columns
        _add(tables, name, entry)

    # Tier 4 — inferred from notebook writes. Name only, never columns.
    for notebook in collection("notebook", "notebooks"):
        # The write lands in the notebook's attached lakehouse, so that is
        # the item that owns the table. A notebook with no binding writes
        # somewhere this export cannot name, and the entry says so by
        # carrying no owner rather than by guessing one.
        owner = notebook.get("default_lakehouse") or ""
        # The scanner records `writes`; re-extracting would parse every
        # notebook a second time for the same answer. A hand-built source
        # dict -- a test, or a plan from an older release -- has only
        # `content`, so that path stays.
        written = (notebook.get("writes")
                   if isinstance(notebook.get("writes"), list)
                   else extract_written_tables(notebook.get("content")))
        for name in sorted(written):
            _add(tables, name, {
                "tier": TIER_NOTEBOOK_INFERRED,
                "created_by": notebook.get("name", ""),
            }, owner=owner)

    summary = {tier: 0 for tier in TIER_ORDER}
    summary["unresolved"] = 0
    for entry in tables.values():
        summary[entry["tier"]] += 1
    return {"summary": summary, "tables": tables, "shortcuts": shortcuts}


def _slots(key: str, owner: str = "") -> tuple:
    """`(item, schema, table)` for a dotted name, right-aligned; "" = unsaid.

    Fabric names a table `item.schema.table` and drops the parts it can
    infer, so the parts that ARE written are the rightmost ones. The owner
    fills the item slot when the name itself does not, and is one atom
    however many dots the item's display name contains --
    `gbrueckl_Fabric.Toolbox` is one item, not two containers.

    This reads a *reference* -- what someone wrote in a notebook or in
    T-SQL. A catalog entry goes through `_entry_slots` instead, which reads
    the entry rather than its key; the note there says what reading an
    entry through here cost.
    """
    parts = key.split(".")
    return (parts[-3] if len(parts) >= 3 else owner,
            parts[-2] if len(parts) >= 2 else "",
            parts[-1])


def _entry_slots(entry_key: str, entry) -> tuple:
    """`(item, schema, table)` for a catalog entry, read off the entry.

    Off `entry["name"]` and `entry["owner"]`, never off the key. `_add`
    builds the key as `<owner>.<qualified name>` and its docstring promises
    that "nothing parses the key back apart"; `candidates` did, through
    `_slots`, and Lakehouse tables paid for it. A Lakehouse table has no
    schema in Fabric, so its entry is `{name: "claims_raw_s3", owner:
    "SalesLake"}` and its key is `saleslake.claims_raw_s3` -- two parts,
    which right-aligned put the *item* name in the *schema* slot. The entry
    then claimed to be in a schema called `saleslake`, and any reference
    naming a real schema was rejected as a different table. Measured
    against the bundled estate's S3 shortcut before this existed:

        claims_raw_s3                1 candidate, shortcut
        SalesLake.claims_raw_s3      1 candidate, shortcut
        dbo.claims_raw_s3            0 candidates
        SalesLake.dbo.claims_raw_s3  0 candidates

    -- and the last two are the spellings that matter. Fabric's SQL
    analytics endpoint exposes Lakehouse tables under `dbo`, which is why
    `_owning_item` in the notebook translator already reads `dbo.claim` and
    `claim` as the same table; and a Warehouse cross-database query is
    three-part, `[Lakehouse].[dbo].[table]`. Both came back "this table was
    not found", from both translators.

    Read off the entry the schema slot is honestly unsaid, and `candidates`
    already has the rule for an unsaid slot: it matches anything. The item
    slot comes back whole for the same reason -- a display name containing
    a dot (`gbrueckl_Fabric.Toolbox` is one in the bundled estate) is one
    item, and slicing the key gave `toolbox`.

    `entry_key` is the fallback for an entry with no `name` of its own: a
    catalog assembled by hand in a test, or one from a release before
    `_add` recorded the field.
    """
    entry = entry if isinstance(entry, dict) else {}
    owner = entry_owner(entry)
    name = _normalise(entry.get("name") or "")
    if not name:
        return _slots(entry_key, owner)
    parts = name.split(".")
    return (parts[-3] if len(parts) >= 3 else owner,
            parts[-2] if len(parts) >= 2 else "",
            parts[-1])


def candidates(catalog: dict, name) -> list:
    """Every catalog entry `name` could be naming.

    Both sides are read as `(item, schema, table)` and compared slot for
    slot, which is the fix: matching on the last part alone made
    `other.claim` resolve to `dbo.claim` -- a different table in a
    different schema, which then got a confident AIDP name and no flag.
    A slot either side leaves unsaid matches anything, because a Fabric
    export records no schema for a Lakehouse table and a supplied CSV
    records no owning item, so demanding equality there would flag almost
    every two-part reference in the estate.

    The item slot is a hint, not a constraint: the catalog's recorded owner
    is authoritative over the item a notebook happens to write, which is
    what `owning_item` decides too -- see
    `test_the_entrys_warehouse_still_wins`. It is used to *choose between*
    candidates, in `resolve`, never to reject the only one.

    Note which slot a supplied row's item lands in, because it is the same
    slot twice: `_entry_slots` reads it off `entry["name"]`, since tier 3
    records no `owner`, and that is the only reason a three-part
    `--tables-csv` row resolves at all. `owning_item` reads it back out
    through `recorded_item`. Reading it here and not there is what had a
    reference resolved *by* the item the operator wrote and then named
    under somebody else's.
    """
    key = _normalise(name)
    if not key or not isinstance(catalog, dict):
        return []
    tables = catalog.get("tables") or {}
    if not isinstance(tables, dict):
        return []
    entry = tables.get(key)
    if entry is not None:
        # An exact key is the reference naming one entry outright; nothing
        # broader can be more specific than that.
        return [entry]
    parts = key.split(".")
    if len(parts) > 3:
        # `server.database.schema.object`: a linked-server reference, which
        # has no AIDP answer at all. The notebook translator refuses it by
        # name (NB17); matching its last three parts here would quietly
        # resolve it to a local table instead.
        return []
    _item, schema, table = _slots(key)
    out = []
    for entry_key, value in tables.items():
        # `_entry_slots`, not `_slots(key, owner)`: #32 found that parsing
        # the key back apart put a Lakehouse entry's item into the schema
        # slot, so `dbo.<table>` and `<lh>.dbo.<table>` -- the two spellings
        # Fabric actually uses -- resolved to nothing. It reads the slots
        # off `entry["name"]` and `entry["owner"]` instead.
        _e_item, e_schema, e_table = _entry_slots(entry_key, value)
        if e_table != table:
            continue
        if schema and e_schema and schema != e_schema:
            continue
        out.append(value)
    return out


def resolve(catalog: dict, name, owner=None):
    """The one entry `name` names, or None when it names none or several.

    None means "do not rewrite this". Two different situations reach it and
    the caller must tell them apart -- `candidates()` is how: an empty list
    is an unknown table (NB12), more than one is an ambiguous reference
    (NB20). Returning the highest-tier candidate, which is what this did,
    answered an ambiguous bare name with whichever entry sorted first and
    reported nothing.

    `owner` is the Fabric item the caller knows the reference is read in --
    a notebook's default lakehouse, or the item a followable `USE` moved it
    to. It only ever chooses between candidates; a single candidate is
    returned whatever the owner says, because the catalog's record of where
    a table lives outranks the binding a notebook happens to have.
    """
    matches = candidates(catalog, name)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        return None
    # Ambiguous. The reference's own item slot is the better hint when it
    # has one -- it is what the author wrote -- and the caller's binding is
    # the fallback.
    parts = _normalise(name).split(".")
    hint = (parts[-3] if len(parts) == 3 else "") or str(owner or "").strip().casefold()
    if not hint:
        return None
    narrowed = [m for m in matches if entry_owner(m) == hint]
    return narrowed[0] if len(narrowed) == 1 else None


# The catalog's verdict on one table reference. Both translators ask the same
# question -- "is this table known, and if so what kind of thing is it" -- and
# before these existed only the notebook one could answer it: warehouse T-SQL
# consulted no catalog at all, so a reference to a shortcut or to a table
# nothing declares was rewritten to a confident three-part name and graded
# PASS. Measured on the demo's resolved catalog, `FROM dbo.nowhere_at_all`
# in a Warehouse view:
#
#   notebook   NB12_TABLE_UNKNOWN  flag, name left as written
#   warehouse  SQ11_TWO_PART_NAME  rewrite -> default.AcmeDW.nowhere_at_all
#
# The verdicts are strings rather than an enum so a plan or a report can carry
# one unchanged; each translator maps them to its own rule ids and wording,
# because the sentence a notebook reader needs is not the sentence a T-SQL
# reader needs. What must not fork is the decision, which is here.
REF_KNOWN = "known"
REF_UNKNOWN = "unknown"
REF_AMBIGUOUS = "ambiguous"
REF_SHORTCUT = "shortcut"
REF_INFERRED = "inferred"


def names_no_tables(catalog) -> bool:
    """True when nothing can be resolved against `catalog` at all.

    The check `classify_reference`'s docstring below tells every caller to
    make *before* asking, written down once so the callers stop getting it
    wrong. They were all testing `not catalog`, which is right for `None` and
    for `{}` and wrong for the shape a plan actually carries:

        resolved_catalog = {"summary": {}, "tables": {}}

    -- a truthy dict that names nothing. MEASURED on 03f019b:

        classify_reference({"tables": {}, "shortcuts": []}, "dbo.claim", "W")
          ->  ('unknown', None, [])

    so every reference in the estate came back `unknown` and every object
    collected its own "no catalog tier knows this table" flag. "We resolved
    nothing" is a different statement from "we resolved this and it is
    absent", and the second is the one the finding made -- once per
    reference.

    `tables` is the whole of the test because `candidates` reads nothing
    else: a shortcut is recorded in `tables` under TIER_SHORTCUT as well as
    in the `shortcuts` map, so an empty `tables` is exactly the condition
    under which `classify_reference` can only ever answer REF_UNKNOWN.
    """
    if not isinstance(catalog, dict):
        return True
    tables = catalog.get("tables")
    return not isinstance(tables, dict) or not tables


def classify_reference(catalog: dict, name, owner=None) -> tuple:
    """`(verdict, entry, matches)` -- what the catalog knows about `name`.

    `verdict` is one of the `REF_*` constants above. `entry` is the single
    catalog entry the reference names, or None when there is not exactly one.
    `matches` is every entry it could be naming, and is only interesting for
    `REF_AMBIGUOUS`, where the caller has to name them for the reader.

    `owner` is the Fabric item the caller knows the reference is read in --
    a notebook's default lakehouse, or the Warehouse a DacFx export keeps in
    the folder name. It only ever chooses between candidates; see `resolve`.

    An empty or missing catalog answers `REF_UNKNOWN` for everything, so
    every caller must decide what to do with no catalog *before* asking. A
    caller that skips that check turns "nothing was supplied" into "this
    table does not exist" on every reference in the estate. `names_no_tables`
    above is that check; use it rather than a truth test on the dict, which
    is what let `{"summary": {}, "tables": {}}` through.
    """
    entry = resolve(catalog, name, owner=owner)
    if entry is None:
        # Two different answers arrive as None and the caller needs them
        # apart: nothing matched, or several did. Reporting "unknown" for an
        # ambiguous name sends the reader looking for a table the export
        # does contain -- twice.
        matches = candidates(catalog, name)
        return ((REF_AMBIGUOUS, None, matches) if len(matches) > 1
                else (REF_UNKNOWN, None, []))
    tier = entry.get("tier")
    if tier == TIER_SHORTCUT:
        return (REF_SHORTCUT, entry, [entry])
    if tier == TIER_NOTEBOOK_INFERRED:
        return (REF_INFERRED, entry, [entry])
    return (REF_KNOWN, entry, [entry])


def _normalise_column(name) -> str:
    """Casefolded, quote-stripped column name.

    Not `_normalise`, which splits on `.` to separate a table name's slots.
    A column name may legally contain one -- `[Customer.Name]` is what
    `Table.ExpandRecordColumn` produces by default, and the M corpus already
    has `Data.Column1` -- so running it through `_normalise` would cut
    `[a.b]` into `a` and `b]` and compare neither. Measured on the folded
    forms because AIDP runs with `spark.sql.caseSensitive=false`.
    """
    if not isinstance(name, str):
        return ""
    name = name.strip()
    if len(name) >= 2 and name[0] == "[" and name[-1] == "]":
        name = name[1:-1]
    elif len(name) >= 2 and name[0] == name[-1] and name[0] in "`\"'":
        name = name[1:-1]
    return name.strip().casefold()


def declared_columns(entry):
    """The column names an entry declares, folded, or None if it declares none.

    `None` and `frozenset()` are different answers and the difference is the
    whole point: `None` is "this catalog does not know what columns this
    table has", `frozenset()` would be "it has none", and a table with no
    columns is not a thing Fabric produces. Every caller that treats the two
    alike is the bug this exists to prevent.

    MEASURED on the bundled estate, which is why the distinction is not
    hypothetical:

        tier                tables   with a column list
        warehouse_ddl           19                   17
        notebook_inferred        3                    0
        shortcut                 2                    0

    `_columns_from_ddl` has populated this since the catalog landed, and
    nothing read it. Only `warehouse_ddl` ever carries one, because it is the
    only tier built from a CREATE TABLE. A shortcut points at storage this
    tool never opens, and an inferred table is known to exist because a
    notebook writes it -- neither says anything about shape.
    """
    entry = entry if isinstance(entry, dict) else {}
    columns = entry.get("columns")
    if not isinstance(columns, (list, tuple)) or not columns:
        return None
    names = set()
    for column in columns:
        if isinstance(column, dict):
            folded = _normalise_column(column.get("name"))
        else:
            folded = _normalise_column(column)
        if folded:
            names.add(folded)
    return frozenset(names) or None


def undeclared_columns(catalog: dict, name, referenced, owner=None):
    """Which of `referenced` this catalog says `name` does not have.

    Three states, and a caller must be able to tell them apart:

        None         no answer -- the table is not in the catalog, is
                     ambiguous, or carries no column list. NOT a pass.
        ()           checked, and every referenced column is declared.
        ("x", "y")   checked, and these are not declared. Display casing as
                     the caller passed them, so a finding can quote the text
                     that is actually in the file.

    Returning `()` for the unknown case is the shape this deliberately does
    not have. It is the pre-#32 warehouse defect exactly: the catalog was
    asked whether a table existed, said "I have no idea", and the answer was
    read as yes. Here it would mean a shortcut's every column reference
    silently passing, which reads as verified and is not.

    Order is preserved and duplicates collapse on the FOLDED name, so
    `SELECT id, ID` reports one undeclared column rather than two.
    """
    if names_no_tables(catalog):
        return None
    # `resolve` returns None for an unknown table AND for an ambiguous one,
    # and `declared_columns` folds None into None -- so both arrive at the
    # refusal below without a branch of their own. A reference this tool
    # cannot pin to a single entry is not one whose columns it can judge. An
    # explicit `if entry is None` here was dead: no mutation of it changed any
    # answer, which in this file is the reason to delete it rather than keep
    # it for decoration.
    declared = declared_columns(resolve(catalog, name, owner))
    if declared is None:
        return None
    out, seen = [], set()
    for column in referenced or ():
        folded = _normalise_column(column)
        if not folded or folded in declared or folded in seen:
            continue
        seen.add(folded)
        out.append(column)
    return tuple(out)


def recorded_item(entry) -> str:
    """The Fabric item an entry records, in the casing to name it under, or "".

    Not `entry_owner`, which exists to *compare* owners and therefore
    casefolds. This one feeds `naming.aidp_table`, so it has to hand back the
    display name: `SalesLake`, not `saleslake`. Returning the folded value
    from here instead renames every table in the estate -- measured at 77
    failures in the suite, `'dbo.ledger' -> 'default.otherdw.ledger'`.

    Three sources, most explicit first:

    * `owner`, which is what `_add` records on every entry it builds;
    * `warehouse` / `lakehouse`, the fields that carried it before `owner`
      existed, so a plan written by an older release -- or a catalog
      assembled by hand in a test -- still reads. Same order and same
      reason as `entry_owner`;
    * the item slot of the entry's own `name`. That is the only channel a
      `--tables-csv` row has: `load_supplied_catalog` reads `table`,
      `column` and `type` and nothing else, and `build_catalog`'s tier 3
      calls `_add` with no owner at all, so a supplied entry's `owner` is
      always `""`. An operator who writes `SalesLake.dbo.ledger` in the
      `table` column is saying where that table lives, on purpose and by
      the only means available. `_entry_slots` already reads the item off
      the name for matching -- that is how such a row resolves at all --
      so reading it here too is what stops the same slot being trusted to
      find the table and discarded to name it.

    `""` when the entry records nothing, which is a real state and not a
    gap: `build_catalog` leaves the owner empty for a table written by a
    notebook with no binding, "by carrying no owner rather than by guessing
    one". The bundled estate's `orphan_output` is one, and the caller's
    fallback -- then NB13 -- is right for it.
    """
    entry = entry if isinstance(entry, dict) else {}
    for field in ("owner", "warehouse", "lakehouse"):
        value = str(entry.get(field) or "").strip()
        if value:
            return value
    parts = str(entry.get("name") or "").split(".")
    return parts[-3].strip() if len(parts) >= 3 else ""


# The tiers whose recorded item outranks the caller's binding, which is the
# whole of issue #1's last finding. `shortcut` is deliberately absent and the
# absence is the answer, not an oversight -- see `owning_item` below.
_ITEM_TIERS = (TIER_WAREHOUSE_DDL, TIER_SUPPLIED, TIER_NOTEBOOK_INFERRED)


def owning_item(entry, reference_schema, fallback_item):
    """Which Fabric item this table lives in, and under which schema.

    `(item, schema)`, ready for `naming.aidp_table`. This is the second half
    of consulting the catalog and it is as load-bearing as the first:
    `classify_reference` answers "does this table exist", and answering that
    and then naming the table somewhere else is how one table ends up with
    two names.

    The catalog entry is the better answer when it has one: a `warehouse_ddl`
    entry records the Warehouse that declared the table, and the caller's
    binding -- a notebook's default lakehouse, or the folder a DacFx export
    put a `.sql` in -- is only where a table with no recorded owner would be.
    Preferring the binding unconditionally is what made the single demo table
    `dbo.claim`, declared in Warehouse `AcmeDW`, come out as
    `default.SalesLake.claim` from notebooks, `default.AcmeDW.claim` in the
    plan and `dbo.claim` in the warehouse artifact: three names, one table,
    all grading PASS.

    It lives here, next to `classify_reference`, because both translators
    need it and neither can import the other -- the notebook module imports
    the T-SQL one. It was the notebook translator's private `_owning_item`,
    and while it was, the warehouse path built every name from the folder's
    item and the written schema whatever the catalog said. Measured with an
    entry recorded `warehouse: OtherDW`, reference `FROM dbo.ledger`, binding
    `AcmeDW` on both sides:

        notebook   -> default.OtherDW.ledger     NB10_TABLE_REF
        warehouse  -> default.AcmeDW.ledger      SQ11_TWO_PART_NAME

    -- one table, two names, and the warehouse one names a table that does
    not exist. SQ19 was silent on it, because the catalog had been asked
    whether the table existed, had said yes, and was then ignored about
    where it was.

    Which tier, and why it is per-tier
    ----------------------------------
    This preferred a recorded item for `warehouse_ddl` alone, so the other
    three tiers resolved a reference *using* the catalog's item and then
    named the table under the caller's binding instead. Reproduced with an
    entry recorded `owner: SalesLake`, `FROM dbo.claims_daily`, binding
    `AcmeDW` on both sides:

        tier               before                       after
        warehouse_ddl      default.SalesLake.claims_daily      (unchanged)
        supplied           default.AcmeDW.claims_daily  default.SalesLake...
        notebook_inferred  default.AcmeDW.claims_daily  default.SalesLake...
        shortcut           dbo.claims_daily                   (unchanged)

    `supplied` -- the operator said so. Not in an `owner` field: the CSV has
    no column for one, so a three-part name in the `table` column is the
    statement, and `recorded_item` reads it. Resolving `dbo.ledger` *by* the
    `SalesLake` the operator wrote and then emitting `default.AcmeDW.ledger`
    is the same table-found-here-named-there defect, and it makes the half
    of `--tables-csv` that says *where* do nothing.

    `notebook_inferred` -- the table's existence is a guess; its location is
    not, and they are the same guess. The tier is built from a notebook's
    `saveAsTable`, and the owner is that notebook's attached lakehouse,
    which is where the write lands; `build_catalog` records no owner at all
    rather than guess one when the writer has no binding. So the recorded
    item is exactly as certain as the existence, while the reader's binding
    is supported by nothing -- keeping it invents a fact rather than
    withholding one. It also breaks the thing the tier exists to say: NB14
    and SQ19_TABLE_INFERRED both promise "notebook X writes this, it must
    run first", and under the binding they promised it about a name that
    notebook does not write. `01_Ingest_Claims`, bound `SalesLake`, writes
    `default.SalesLake.claims_daily`; an `AcmeDW` reader was told to read
    `default.AcmeDW.claims_daily`. Nothing writes that.

    `shortcut` -- absent on purpose. Both translators refuse a shortcut
    before any name is built (NB11, SQ19_TABLE_IS_SHORTCUT) because its data
    is outside the lakehouse and no three-part name reaches it, so the right
    answer for one is "there is no name", not "this lakehouse". Adding the
    tier here would change nothing any caller can observe, and a branch that
    cannot be reached reads as coverage.

    None of this moves the bundled estate or the demo: the 2861 tests that
    existed before were green unchanged, and the demo's 110 assets, 470 raw
    / 214 distinct findings and every emitted artifact byte came out
    identical -- as did the RUNBOOK's `ok=14 needs_review=10 blocked=1
    planned=3 error=0`. Reaching the case needs a read of a table whose
    recorded item differs from the reader's binding, and neither corpus has
    one, which is what `tests/test_one_name.py`'s own docstring says about
    this case. `test_tsql_catalog.OwningItemTests` and `AgreementTests` are
    what assert it; all 7 of the behavioural ones fail against the old rule.

    The blanket version was costed at "12 failures, 5 errors, `error=3`" and
    that cost was not the decision. Widening only the condition, to
    `if entry_owner(entry)`, leaves `return entry["warehouse"]` below it,
    which raises `KeyError: 'warehouse'` on every entry of the three other
    tiers: re-measured here at 14 failures, 6 errors, `error=3`, ok=14->11,
    blocked=1->2. Returning `entry_owner`'s folded value instead is the
    other way to pay for it, at 77 failures. Neither is what trusting the
    recorded item costs.
    """
    entry = entry if isinstance(entry, dict) else {}
    if entry.get("tier") in _ITEM_TIERS:
        item = recorded_item(entry)
        if item:
            schema = reference_schema
            if not schema:
                # A bare `claim` against an entry recorded as
                # `postgres_air.t` must keep that schema, or the notebook and
                # the DDL disagree again in the one case where the schema
                # matters. `[-2]`, not `[0]`: a supplied entry's name can be
                # three-part, and `[0]` read the *item* out of
                # `SalesLake.dbo.ledger` as the schema. Identical for the
                # two-part names every other tier records.
                known = str(entry.get("name") or "").split(".")
                schema = known[-2].strip() if len(known) >= 2 else ""
            return item, schema
    # A schema-qualified reference names the same table the bare name would,
    # inside the caller's own binding -- Fabric's SQL analytics endpoint
    # exposes Lakehouse tables under `dbo` too. Treating `dbo.claim` as an
    # opaque table name produced `default.dbo.claim` from notebooks while
    # every other caller called the identical table `default.SalesLake.claim`.
    return fallback_item, reference_schema
