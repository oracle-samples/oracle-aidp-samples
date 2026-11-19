"""Fold Power Query navigation chains into a single read, and resolve write targets.

A Lakehouse read is not one step. `Lakehouse.Contents(...)` returns a navigable
tree, and the table name appears in a *chain* of later steps whose `fn` is the
previous step's name:

    Pattern         = Lakehouse.Contents([...]),
    Navigation_1    = Pattern{[workspaceId = "..."]}[Data],
    Navigation_2    = Navigation_1{[lakehouseId = "..."]}[Data],
    TableNavigation = Navigation_2{[Id = "T", ItemKind = "Table"]}?[Data]?

Across the 39-dataflow corpus only 1 of 43 source steps carried the table name
itself. Reading the source step alone finds the table 1 time in 43; folding the
chain finds it 35 times, plus 7 file reads under `Files/`.

The same fold resolves the write side: a member's `DataDestinations` attribute
names a sibling `*_DataDestination` query, and that query is one of these chains.

A member can instead carry `[BindToDefaultDestination = true]` and nothing
else -- 9 of the 39-file corpus do. Their target is the *section*-level
`[DefaultOutputDestinationSettings = [...]]`, which names a `DefaultDestination`
member holding only a workspace and a lakehouse; the table is the query's own
name. Reading only `DataDestinations` left those 9 emitting a DataFrame and
discarding it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from fabric_aidp.translate.m_parser import unquote_identifier

_TABLE = re.compile(r'\[\s*Id\s*=\s*"(?P<v>(?:[^"]|"")*)"\s*,\s*ItemKind\s*=\s*"Table"\s*\]')
_FOLDER = re.compile(r'\[\s*Id\s*=\s*"(?P<v>(?:[^"]|"")*)"\s*,\s*ItemKind\s*=\s*"Folder"\s*\]')
_NAME = re.compile(r'\[\s*Name\s*=\s*"(?P<v>(?:[^"]|"")*)"\s*\]')
_CONTENT = re.compile(r'\[\s*Content\s*\]')
# A quoted value may itself hold `]` -- "data[1].csv" -- so strings are
# skipped whole rather than ending the selector at the first bracket.
_SELECTOR = re.compile(r'\{\s*(\[(?:"(?:[^"]|"")*"|[^\]"])*\])\s*\}')
_NAME_KEY = re.compile(r'\[\s*Name\s*=')
_FOLDER_KIND = re.compile(r'\bItemKind\s*=\s*"Folder"')
_LAKEHOUSE_ID = re.compile(r'\[\s*lakehouseId\s*=\s*"(?P<v>[^"]*)"\s*\]')
_WORKSPACE_ID = re.compile(r'\[\s*workspaceId\s*=\s*"(?P<v>[^"]*)"\s*\]')
_QUERY_NAME = re.compile(r'QueryName\s*=\s*"(?P<v>(?:[^"]|"")*)"')
_UPDATE_METHOD = re.compile(r'UpdateMethod\s*=\s*\[\s*Kind\s*=\s*"(?P<v>\w+)"')
# `[BindToDefaultDestination = true]` on a member, and the section-level
# `[DefaultOutputDestinationSettings = [...]]` it points at. Both are literal
# attribute records, so both are matched in text the way the ones above are.
_BIND_DEFAULT = re.compile(r'BindToDefaultDestination\s*=\s*(?P<v>true|false)')
_DESTINATION_TYPE = re.compile(
    r'DestinationTypeSettings\s*=\s*\[\s*Kind\s*=\s*"(?P<v>\w+)"')
_FUNCTION_CALL = re.compile(r'^\s*(?P<v>[A-Za-z_][\w.]*)\s*\(')
# `ColumnSettings = [Mappings = {[SourceColumnName = "a",
# DestinationColumnName = "b"], ...}]`. The records hold no nested braces --
# checked across all 136 mappings in the corpus's 6 mapping-bearing files,
# every one of which this reads back -- so the list body is everything
# between the one pair of braces.
_MAPPINGS = re.compile(r'Mappings\s*=\s*\{(?P<v>[^{}]*)\}')
_MAPPING_RECORD = re.compile(r'\[(?P<v>[^\[\]]*)\]')
_SOURCE_COLUMN = re.compile(r'SourceColumnName\s*=\s*"(?P<v>(?:[^"]|"")*)"')
_DESTINATION_COLUMN = re.compile(
    r'DestinationColumnName\s*=\s*"(?P<v>(?:[^"]|"")*)"')
_DYNAMIC_SCHEMA = re.compile(r'DynamicSchema\s*=\s*(?P<v>true|false)')
COLUMN_SETTINGS = "ColumnSettings"

LAKEHOUSE_SOURCE = "Lakehouse.Contents"
DESTINATION_SUFFIX = "_DataDestination"
DEFAULT_DESTINATION_SETTINGS = "DefaultOutputDestinationSettings"
_MODES = {"Replace": "overwrite", "Append": "append"}


@dataclass
class LakehouseRead:
    kind: str                       # "table" | "file" | "unresolved"
    table: str = None
    file: str = None
    # The directory `file` sits in. None when kind is "unresolved": with no
    # file hop resolved there is no folder/file split, and the join of every
    # hop put the file itself in the directory ('Files/f.csv').
    folder: str = None
    lakehouse_id: str = None
    workspace_id: str = None
    consumed: list = field(default_factory=list)
    reason: str = None              # why an "unresolved" read is unresolved


@dataclass
class Destination:
    query_name: str
    # "table" | "unsupported" | "unresolved" | "default_unresolved"
    kind: str
    table: str = None
    lakehouse_id: str = None
    mode: str = "overwrite"
    connector: str = None
    # True when nothing named this table: the query said only
    # `[BindToDefaultDestination = true]` and the name is its own. The
    # emitter flags on this, because a derived write target is not a
    # declared one.
    default_bound: bool = False
    # Why a "default_unresolved" cannot be resolved, in words the emitter
    # puts straight into the refusal.
    reason: str = ""
    # [(source column, destination column)] in the destination's order, or
    # None when the entry declares no ColumnSettings. An empty list is not
    # the same thing: it means a mapping was declared and could not be
    # read, and the emitter refuses on it.
    column_map: list = None
    # `DynamicSchema = true` -- Fabric refreshes the mapping from the source
    # schema at run time, so the recorded mapping is a snapshot.
    dynamic_schema: bool = False


def _group(pattern, text):
    match = pattern.search(text or "")
    return match.group("v").replace('""', '"') if match else None


def _path_segments(nav) -> list:
    """[("folder" | "name", value)] for every path hop, in navigation order.

    A Folder is navigated by `{[Id = "raw", ItemKind = "Folder"]}[Data]` or,
    one level down, by `{[Name = "raw"]}[Content]` -- Fabric writes both. So
    the hops are merged by position, not collected per pattern.
    """
    hops = [(m.start(), "folder", m.group("v").replace('""', '"'))
            for m in _FOLDER.finditer(nav or "")]
    hops += [(m.start(), "name", m.group("v").replace('""', '"'))
             for m in _NAME.finditer(nav or "")]
    return [(kind, value) for _, kind, value in sorted(hops)]


_STRING = re.compile(r'"(?:[^"]|"")*"')
# The keys each path-hop selector takes, as Fabric writes them.
_NAME_KEYS = ("Name",)
_FOLDER_KEYS = ("Id", "ItemKind")


def _selector_fields(selector):
    """[(key, value)] of a `[k = v, ...]` selector, split on top-level commas.

    Quoted strings are skipped whole and brackets counted, so a comma inside
    "a,b.csv" or inside a call's argument list does not split a field.
    """
    body = selector.strip()[1:-1]
    fields, depth, start, i = [], 0, 0, 0
    while i < len(body):
        if body[i] == '"':
            string = _STRING.match(body, i)
            if string is None:      # unterminated: the rest is one value
                break
            i = string.end()
            continue
        if body[i] in "({[":
            depth += 1
        elif body[i] in ")}]":
            depth -= 1
        elif body[i] == "," and depth == 0:
            fields.append(body[start:i])
            start = i + 1
        i += 1
    fields.append(body[start:])
    out = []
    for text in fields:
        key, _, value = text.partition("=")
        out.append((key.strip(), value.strip()))
    return out


def _nonliteral_segment(nav):
    """Why a path hop cannot be read, or None when every hop is a literal.

    `{[Name = FileName]}` matches neither literal pattern, so it used to be
    skipped -- leaving a path one level short that still looked resolved.

    Two different things fail the literal patterns, and the reason names
    which: a value that is not a quoted string (`Name = FileName`), or a
    selector whose keys are not the ones a Name or Folder hop takes
    (`[Id = "raw", ItemKind = "Folder", X = 1]`, every value literal). The
    second used to be reported as the first, which sends a reviewer looking
    for an expression that is not there.
    """
    for selector in _SELECTOR.findall(nav or ""):
        if not (_NAME_KEY.search(selector) or _FOLDER_KIND.search(selector)):
            continue
        shown = selector.strip()
        if _NAME.fullmatch(shown) or _FOLDER.fullmatch(shown):
            continue
        fields = _selector_fields(shown)
        keys = [key for key, _ in fields]
        kind, expected = (("Name", _NAME_KEYS) if "Name" in keys
                          else ("Folder", _FOLDER_KEYS))
        extra = [key for key in keys if key not in expected]
        if extra:
            return ("Lakehouse.Contents navigation %s has key %s, which a %s "
                    "selector does not take (it takes %s); the file path "
                    "cannot be resolved"
                    % (shown, ", ".join(extra), kind, " and ".join(expected)))
        for key, value in fields:
            if not _STRING.fullmatch(value):
                return ("Lakehouse.Contents navigation %s selects %s by %s, "
                        "which is not a literal string; the file path cannot "
                        "be resolved" % (shown, key, value))
        # Every key expected and every value a string, yet neither pattern
        # matched: a key missing or repeated, or Folder's keys out of the
        # order Fabric writes them in.
        return ("Lakehouse.Contents navigation %s does not have the shape of "
                "a %s selector (%s, in that order, once each); the file path "
                "cannot be resolved" % (shown, kind, " then ".join(expected)))
    return None


def _nav_text(step) -> str:
    return " ".join(fragment for fragment in (step.get("nav") or []) if fragment)


def lakehouse_read_indexes(steps) -> list:
    return [i for i, step in enumerate(steps or [])
            if str(step.get("fn") or "").strip() == LAKEHOUSE_SOURCE]


def fold_lakehouse_chain(steps, index) -> LakehouseRead:
    steps = list(steps or [])
    root = steps[index]
    chain = {root["name"]}
    consumed = [root["name"]]
    fragments = [_nav_text(root)]

    for step in steps[index + 1:]:
        parent = str(step.get("fn") or "").strip()
        if parent not in chain:
            continue
        if step.get("args"):
            break           # a real call consuming the chain -- the read ends here
        if not step.get("nav"):
            break
        chain.add(step["name"])
        consumed.append(step["name"])
        fragments.append(_nav_text(step))

    nav = " ".join(f for f in fragments if f)
    table = _group(_TABLE, nav)
    segments = _path_segments(nav)
    names = [value for kind, value in segments if kind == "name"]
    name = names[-1] if names and _CONTENT.search(nav) else None
    reason = _nonliteral_segment(nav)
    if reason is not None:
        kind, table, name = "unresolved", None, None
    elif table is not None:
        kind = "table"
    elif name is not None:
        kind = "file"
    else:
        kind = "unresolved"
    # Every Folder Id and every Name before the file, in navigation order.
    # Only the first Folder Id used to be kept, and no Name but the last:
    # measured on the probe export, `Files` > Name "raw" > Name
    # "orders_2024.csv" read oci://.../Files/raw -- every file in the
    # folder, silently -- and `Files` > Id "raw" (Folder) > Name
    # "orders_2024.csv" read oci://.../Files/orders_2024.csv, a file that is
    # not there. Both are Files/raw/orders_2024.csv.
    #
    # None when unresolved rather than the path up to the bad hop: nothing
    # reads it there (the emitter refuses on `reason`), and a partial path
    # is one more value a later reader could mistake for a location.
    folders = [value for _, value in (segments[:-1] if name is not None else segments)]
    folder = None if kind == "unresolved" else ("/".join(folders) or None)
    return LakehouseRead(kind=kind, table=table, file=name,
                         folder=folder,
                         lakehouse_id=_group(_LAKEHOUSE_ID, nav),
                         workspace_id=_group(_WORKSPACE_ID, nav),
                         consumed=consumed, reason=reason)


def is_destination_helper(name) -> bool:
    return unquote_identifier(name).endswith(DESTINATION_SUFFIX)


def is_default_destination_target(name, section_attrs) -> bool:
    """Whether the section's default destination is this member.

    The other write target. `*_DataDestination` is a naming convention a
    member follows; this one is a member the *section* points at by name,
    with no convention at all -- 10 of the 39 corpus files carry one and
    every one of them is called `DefaultDestination`, which is Fabric's
    default and not a rule.

    Both are the same thing to the emitter: an expression that names where
    another query writes, never a pipeline of its own. Reading only the
    suffix left the section-named ten looking like stepless queries, which
    is how they came to be counted as parameters.
    """
    attrs = section_attrs or ""
    if DEFAULT_DESTINATION_SETTINGS not in attrs:
        return False
    wanted = _group(_QUERY_NAME, attrs)
    if not wanted:
        return False
    return unquote_identifier(name) == unquote_identifier(wanted)


def member(queries_by_name, name):
    """A section member by name, quoted or not.

    `#"squirrel-data csv_DataDestination"` is how the parser reports the
    member and `squirrel-data csv_DataDestination` is how a `QueryName`
    attribute spells the same thing, so a raw dict lookup misses it and the
    caller concludes the export does not contain a query that is right
    there. 3 of the 24 corpus destination references are of this shape.
    """
    lookup = {unquote_identifier(k): v for k, v in (queries_by_name or {}).items()}
    return lookup.get(unquote_identifier(name))


def column_mapping(attrs):
    """[(source, destination)] for a DataDestinations entry, or None.

    A destination can name which source columns go to which target columns,
    and in which order. Writing the whole frame instead gives the target the
    wrong shape: every column the mapping leaves out, and -- once in the
    corpus, 033.pq's `Date` -> `Message_Date` -- data under a column name
    the destination does not have.

    None means the entry declared no `ColumnSettings` at all. `[]` means it
    declared one this could not read, which is a different thing and must
    not be rendered as "no mapping".
    """
    attrs = attrs or ""
    if COLUMN_SETTINGS not in attrs:
        return None
    body = _group(_MAPPINGS, attrs)
    if body is None:
        return []
    pairs = []
    for record in _MAPPING_RECORD.finditer(body):
        source = _group(_SOURCE_COLUMN, record.group("v"))
        target = _group(_DESTINATION_COLUMN, record.group("v"))
        if source is None or target is None:
            return []
        pairs.append((source, target))
    return pairs


def _lakehouse_of(query):
    """(lakehouse_id, connector) for a member that only navigates to an item.

    A `DefaultDestination` member is `shared X = Lakehouse.Contents(...){...}
    [Data]{...}[Data];` -- not a `let`, so the parser reports no steps and
    puts the whole expression in `raw`. The `let` spelling exists too, so
    both are read here.
    """
    steps = query.get("steps") or []
    indexes = lakehouse_read_indexes(steps)
    if indexes:
        return fold_lakehouse_chain(steps, indexes[0]).lakehouse_id, LAKEHOUSE_SOURCE
    if steps:
        return None, next((str(s.get("fn")) for s in steps
                           if "." in str(s.get("fn") or "")), None)
    raw = str(query.get("raw") or "")
    connector = _group(_FUNCTION_CALL, raw)
    if connector != LAKEHOUSE_SOURCE:
        return None, connector
    return _group(_LAKEHOUSE_ID, raw), LAKEHOUSE_SOURCE


def _default_destination(query, queries_by_name, section_attrs):
    """The write target of a `[BindToDefaultDestination = true]` member.

    Fabric's default destination lands one table per query, in the lakehouse
    the section-level `DefaultOutputDestinationSettings` names, under the
    query's own name -- the export never writes that table name down
    anywhere. 035.pq is the one corpus file carrying both shapes, and it
    corroborates the rule: its only explicitly-destined query, `students`,
    resolves to lakehouse e513c730 table `students`, which is exactly the
    lakehouse `DefaultDestination` names plus the query's own name.

    Anything short of that is refused rather than guessed -- a write is the
    one thing here that cannot be undone.
    """
    name = unquote_identifier(query.get("name"))
    settings = section_attrs or ""
    mode = _MODES.get(_group(_UPDATE_METHOD, settings) or "", "overwrite")
    def refuse(why):
        return Destination(query_name="", kind="default_unresolved", mode=mode,
                           default_bound=True, reason=why)

    if DEFAULT_DESTINATION_SETTINGS not in settings:
        return refuse(
            "the query is marked [BindToDefaultDestination = true] but this "
            "mashup.pq declares no %s, so nothing in the export says where "
            "it writes" % DEFAULT_DESTINATION_SETTINGS)
    kind = _group(_DESTINATION_TYPE, settings)
    if kind is not None and kind != "Table":
        return refuse(
            "the default destination is of kind %r, and only a Table "
            "destination has a proven mapping" % kind)
    wanted = _group(_QUERY_NAME, settings)
    if not wanted:
        return refuse("%s names no QueryName" % DEFAULT_DESTINATION_SETTINGS)
    target = member(queries_by_name, wanted)
    if target is None:
        return refuse(
            "the default destination is the query %r, which this export does "
            "not contain" % wanted)
    lakehouse_id, connector = _lakehouse_of(target)
    if connector != LAKEHOUSE_SOURCE:
        return refuse("the default destination query %r is a %s, and only "
                      "%s is supported"
                      % (wanted, connector or "unrecognised expression",
                         LAKEHOUSE_SOURCE))
    if not lakehouse_id:
        return refuse(
            "the default destination query %r names no lakehouseId, so the "
            "table name would be derived and the lakehouse guessed" % wanted)
    return Destination(query_name=wanted, kind="table", table=name,
                       lakehouse_id=lakehouse_id, mode=mode,
                       default_bound=True)


def resolve_destination(query, queries_by_name, section_attrs=""):
    attrs = query.get("attrs") or ""
    if "DataDestinations" not in attrs:
        # A member's own attribute wins; 020.pq carries both shapes at once.
        if _group(_BIND_DEFAULT, attrs) == "true":
            return _default_destination(query, queries_by_name, section_attrs)
        return None
    wanted = _group(_QUERY_NAME, attrs)
    mode = _MODES.get(_group(_UPDATE_METHOD, attrs) or "", "overwrite")
    if not wanted:
        return Destination(query_name="", kind="unresolved", mode=mode)

    helper = member(queries_by_name, wanted)
    if helper is None:
        return Destination(query_name=wanted, kind="unresolved", mode=mode)

    steps = helper.get("steps") or []
    indexes = lakehouse_read_indexes(steps)
    if not indexes:
        connector = next((str(s.get("fn")) for s in steps
                          if "." in str(s.get("fn") or "")), None)
        return Destination(query_name=wanted, kind="unsupported",
                           mode=mode, connector=connector)

    read = fold_lakehouse_chain(steps, indexes[0])
    if read.kind != "table":
        return Destination(query_name=wanted, kind="unresolved", mode=mode)
    return Destination(query_name=wanted, kind="table", table=read.table,
                       lakehouse_id=read.lakehouse_id, mode=mode,
                       column_map=column_mapping(attrs),
                       dynamic_schema=_group(_DYNAMIC_SCHEMA, attrs) == "true")
