"""Scan Dataflow Gen2 items from a Fabric Git export.

Counting Dataflows needs no Node; translating them does. So this scanner does
as much as it can without the optional parser and says plainly which it did:

    dataflow   dataflow_count=3, query_count=11, parser=unavailable
               (install Node + mparse to translate; 11 queries counted, 0 translated)

When the parser is unavailable `translatable_count` is None, never 0. An absent
tool is reported as absent, not rendered as an empty result -- the same rule the
shortcut-coverage code follows. `load_enabled` is None, never [], for the same
reason when queryMetadata.json could not be read.

Every per-item failure stays per-item. `build_manifest` catches a scanner
exception per *source*, so anything raised here costs every Dataflow in the
workspace, not the one that provoked it.
"""
from __future__ import annotations

import json
import re

from fabric_aidp.inventory.git_workspace import items_of_type
from fabric_aidp.translate import m_nav, m_parser
from fabric_aidp.translate.m_to_pyspark import classify

MASHUP = "mashup.pq"
QUERY_METADATA = "queryMetadata.json"
# Enough to count queries when Node is absent: `shared <name> =` at line start.
_SHARED = re.compile(r'^\s*shared\s+(#"(?:[^"]|"")*"|[A-Za-z_][\w.]*)\s*=', re.M)


def _declared_queries(text) -> list:
    return [match.group(1) for match in _SHARED.finditer(text or "")]


def _load_metadata(item):
    """(load-enabled query names, error). Either may be None/"" -- never both.

    `None` for the names means nobody read the file: it is absent, or it is
    there and unreadable. `[]` would be a claim -- "no query in this
    Dataflow is load-enabled" -- about a file nothing opened, which is the
    same shape of lie as reporting an unreadable shortcuts file as
    "tracked (0 found)".

    Nothing is raised. `build_manifest` catches a scanner exception per
    *source*, so `payload.get(...)` on a JSON list, string or null -- all
    valid JSON, none of them an object -- cost the whole dataflow source:
    measured on a two-Dataflow workspace with one bad file, the manifest
    went from 2 Dataflows to 0 for each of those three shapes. One
    malformed metadata file costs one Dataflow.
    """
    path = item.file(QUERY_METADATA)
    if not path.is_file():
        return None, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError) as exc:
        return None, f"cannot read {QUERY_METADATA}: {exc}"
    except json.JSONDecodeError as exc:
        return None, f"{QUERY_METADATA} is not valid JSON: {exc}"
    if not isinstance(payload, dict):
        return None, (f"{QUERY_METADATA} is valid JSON but not an object "
                      f"(found {type(payload).__name__}), so no query's "
                      f"loadEnabled could be read")
    metadata = payload.get("queriesMetadata")
    if metadata is None:
        return None, f"{QUERY_METADATA} has no 'queriesMetadata' object"
    if not isinstance(metadata, dict):
        return None, (f"{QUERY_METADATA} field 'queriesMetadata' is a "
                      f"{type(metadata).__name__}, not an object")
    # A key that is not a string cannot be a query name, and mixing types
    # makes `sorted` itself raise -- which would land back in the
    # whole-source failure this function exists to end.
    return sorted(str(name) for name in metadata), ""


def scan(items, *, log=None) -> dict:
    available = m_parser.parser_available()
    dataflows = []
    # One key per `m_to_pyspark.classify` verdict. `unread_member_count` is
    # the members this tool did not read -- they used to be added to
    # `parameter_count`, which reported a lost query as a constant.
    totals = {"query_count": 0, "pipeline_count": 0,
              "helper_count": 0, "parameter_count": 0,
              "unread_member_count": 0}
    translatable = 0

    metadata_unreadable = 0
    for item in items_of_type(items, "Dataflow"):
        path = item.file(MASHUP)
        load_enabled, metadata_error = _load_metadata(item)
        metadata_unreadable += bool(metadata_error)
        # `counted` and `translated` are per-Dataflow, and the plan reads
        # them: a Dataflow with a parse error or no parser has no queries to
        # plan, and used to vanish from the plan entirely rather than be
        # reported. `translated` is None, never 0, when the parser is
        # absent -- the same rule the summary's `translatable_count`
        # follows, and for the same reason.
        record = {"name": item.name, "logical_id": item.logical_id,
                  "folder": item.folder,
                  "queries": [], "load_enabled": load_enabled,
                  "metadata_error": metadata_error,
                  "parse_error": "", "counted": 0, "translated": None,
                  "parser": "available" if available else "unavailable"}
        if not path.is_file():
            record["parse_error"] = f"{MASHUP} is missing"
            dataflows.append(record)
            continue

        try:
            text = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            record["parse_error"] = f"cannot read {MASHUP}: {exc}"
            dataflows.append(record)
            continue

        if not available:
            # No Node: count the queries we can see, classify nothing.
            for name in _declared_queries(text):
                record["queries"].append({"name": name, "kind": "uncounted"})
            record["counted"] = len(record["queries"])
            totals["query_count"] += len(record["queries"])
            dataflows.append(record)
            continue

        try:
            parsed = m_parser.parse_file(path)
        except (m_parser.MParseError, m_parser.MParserUnavailable) as exc:
            record["parse_error"] = str(exc)
            dataflows.append(record)
            continue

        by_name = {query["name"]: query for query in parsed["queries"]}
        # The section's own literal attributes. A member marked
        # `[BindToDefaultDestination = true]` says where it writes only via
        # this, so an asset without it has no destination at all.
        section_attrs = parsed.get("section_attrs") or ""
        record["section_attrs"] = section_attrs
        for query in parsed["queries"]:
            kind = classify(query, section_attrs)
            entry = {"name": query["name"], "kind": kind}
            if kind == "unread_member":
                # Not translatable, but it still needs a plan asset: a member
                # this tool did not read is the one outcome that must not be
                # silent, and an asset is the only way a finding about it
                # reaches migrate and verify. No destination is resolved,
                # because `translate_query` refuses before it gets there.
                entry["parsed"] = query
                entry["helper"] = None
                entry["section_attrs"] = section_attrs
            if kind == "pipeline":
                # Carry what the translator needs so plan assets stay
                # self-contained, the way warehouse assets carry their SQL.
                destination = m_nav.resolve_destination(
                    query, by_name, section_attrs=section_attrs)
                # `m_nav.member`, not `by_name.get`. The attribute spells
                # the helper `squirrel-data csv_DataDestination` and the
                # member is declared `#"squirrel-data csv_DataDestination"`,
                # so the raw lookup missed it, the asset carried no helper,
                # and the translator reported the export did not contain a
                # query three lines below the attribute naming it. 3 of the
                # 24 corpus destination references are of this shape.
                helper = (m_nav.member(by_name, destination.query_name)
                          if destination else None)
                entry["parsed"] = query
                entry["helper"] = helper
                entry["section_attrs"] = section_attrs
                translatable += 1
            record["queries"].append(entry)
            totals["query_count"] += 1
            totals[f"{kind}_count"] += 1
        record["counted"] = len(record["queries"])
        record["translated"] = sum(1 for q in record["queries"]
                                   if q["kind"] == "pipeline")
        dataflows.append(record)

    if log:
        state = "available" if available else "unavailable"
        log(f"  {len(dataflows)} dataflow(s), {totals['query_count']} quer(ies), "
            f"parser={state}")
        for record in dataflows:
            if record["metadata_error"]:
                log(f"    {record['name']}.Dataflow — {record['metadata_error']}")

    return {
        "summary": dict(totals,
                        dataflow_count=len(dataflows),
                        parser="available" if available else "unavailable",
                        # Said out loud rather than left to whoever reads the
                        # per-item records: an unreadable metadata file used
                        # to take the entire source down, so its absence from
                        # the summary would be the quiet version of the same
                        # thing.
                        metadata_unreadable=metadata_unreadable,
                        translatable_count=translatable if available else None),
        "items": {"dataflows": dataflows},
    }
