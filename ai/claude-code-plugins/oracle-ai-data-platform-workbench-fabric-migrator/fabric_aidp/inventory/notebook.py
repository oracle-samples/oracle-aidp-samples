"""Scan Notebook items from a Fabric Git export.

Fabric stores a notebook as `notebook-content.py` (PySpark/Python/Scala/R) or
`notebook-content.sql`, alongside `.platform`. Cell outputs are never committed.

A notebook that will not parse is recorded with a `parse_error` and an empty
edge set rather than aborting the scan — one malformed item must not cost the
operator the other ninety-nine.
"""
from __future__ import annotations

from fabric_aidp.inventory.catalog import (
    extract_read_tables, extract_written_tables,
)
from fabric_aidp.inventory.edges import extract_notebook_edges
from fabric_aidp.inventory.git_workspace import items_of_type
from fabric_aidp.translate.ipynb_format import IpynbParseError
from fabric_aidp.translate.notebook_format import (
    NotebookParseError, check_meta, parse_any,
)

CONTENT_STEMS = ("notebook-content.py", "notebook-content.ipynb",
                 "notebook-content.sql")
_EMPTY_EDGES = {"run": [], "notebook_run": [], "run_multiple": [],
                "unresolved": []}
_KERNEL_LANGUAGE = {
    "synapse_pyspark": "python",
    "synapse_spark": "scala",
    "jupyter": "python",
}


def _content_file(item):
    for stem in CONTENT_STEMS:
        candidate = item.file(stem)
        if candidate.is_file():
            return candidate
    return None


def _cell_source(nb) -> str:
    """The code cells' own source, which is where an edge can live.

    Not the file text, which is what this used to scan. For
    `notebook-content.py` the two are near enough the same thing -- the
    markers and `# META` lines carry no `%run` -- but for
    `notebook-content.ipynb` the file text is JSON, and an edge written in
    it is not an edge any more. Measured on a two-notebook estate, the same
    two cells in each format:

      .py     -> run: ['Other'], notebook_run: ['Other2']
      .ipynb  -> run: [],        notebook_run: [],
                 unresolved: ['\\\\"Other2\\\\", 60)\\\\n"']

    `%run Other` is lost because the scan anchors the magic to the start of
    a line and in JSON the line starts with a quote and indentation. The
    `notebook.run` call is found -- that pattern has no anchor -- but its
    argument is read out of JSON-escaped text, so `\\"Other2\\"` is not a
    string literal this can evaluate and the edge lands in `unresolved` as
    a fragment of JSON. It resolves only when the notebook happened to use
    single quotes, which JSON does not escape.

    Most real exports are .ipynb: 39 of the 49 notebooks in
    microsoft/fabric-toolbox. So the estate whose dependency graph this
    silently empties is the common one.
    """
    return "\n".join(block.text for block in nb.code_blocks)


def _language(content_file, nb) -> str:
    if content_file.name.endswith(".sql"):
        return "sql"
    if nb is not None:
        info = nb.notebook_meta.get("language_info")
        if isinstance(info, dict) and isinstance(info.get("name"), str):
            return info["name"]
        kernel = nb.notebook_meta.get("kernel_info")
        if isinstance(kernel, dict):
            name = kernel.get("name")
            if isinstance(name, str) and name in _KERNEL_LANGUAGE:
                return _KERNEL_LANGUAGE[name]
        for block in nb.blocks:
            if block.kind != "CELL":
                continue
            language = nb.meta_for(block).get("language")
            if isinstance(language, str) and language:
                return language
    return "python"


def scan(items, *, log=None) -> dict:
    records = []
    language_counts: dict = {}
    parse_errors = 0

    for item in items_of_type(items, "Notebook"):
        record = {
            "name": item.name,
            "logical_id": item.logical_id,
            "folder": item.folder,
            "language": "python",
            "content": "",
            "content_file": "",
            "default_lakehouse": None,
            # The GUID as well as the name. They are only written down
            # together here, and `plan` turns the pair into the map that
            # lets a Dataflow name the lakehouse it reads and writes.
            "default_lakehouse_id": None,
            "edges": dict(_EMPTY_EDGES),
            "writes": [],
            "reads": [],
        }
        content_file = _content_file(item)
        if content_file is None:
            record["parse_error"] = (
                "no notebook-content.py or notebook-content.sql in the item directory"
            )
            parse_errors += 1
            records.append(record)
            continue

        record["content_file"] = content_file.name
        try:
            text = content_file.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            record["parse_error"] = f"cannot read {content_file.name}: {exc}"
            parse_errors += 1
            records.append(record)
            continue

        record["content"] = text
        try:
            nb = parse_any(text)
            # Every `# META` block is decoded here, inside this notebook's
            # guard. `_language` and `default_lakehouse` below decode them
            # lazily, and they used to run after the guard: one trailing
            # comma in one notebook's metadata escaped to the per-source
            # catch in manifest.py, the notebook source read FAILED, and
            # every other notebook vanished from plan, migrate and verify,
            # all of which exited 0.
            check_meta(nb)
            language = _language(content_file, nb)
            default_lakehouse = nb.default_lakehouse
            # Inside the guard with the other two, and for the same reason:
            # `default_lakehouse_id` reads `notebook_meta`, which decodes
            # lazily. Left at the record assignment below -- where the merge
            # put it -- it is a lazily-decoding property outside the
            # per-notebook try again, which is the exact bug this commit
            # fixes. It is safe today only because `check_meta` has already
            # proved every block decodes; the next property anyone adds
            # would not be.
            default_lakehouse_id = nb.default_lakehouse_id
        except (NotebookParseError, IpynbParseError) as exc:
            record["parse_error"] = str(exc)
            record["language"] = "sql" if content_file.name.endswith(".sql") else "python"
            parse_errors += 1
            records.append(record)
            if log:
                log(f"  notebook {item.name} — parse error: {exc}")
            continue

        record["language"] = language
        record["default_lakehouse"] = default_lakehouse
        record["default_lakehouse_id"] = default_lakehouse_id
        # `_cell_source(nb)`, not `text`, for all three. For a
        # `notebook-content.py` the two are near enough the same; for a
        # `.ipynb` the file text is JSON, and a `%run` or a `saveAsTable`
        # written inside it is not one any more. The edges rule learned
        # that on this branch; the writes and reads arrived from main
        # still reading `text`, and would have been blind to every
        # `.ipynb` in exactly the same way.
        cells = _cell_source(nb)
        record["edges"] = extract_notebook_edges(cells)
        # The tables this notebook writes and the tables it reads. The
        # writes were already found -- `build_catalog` has called
        # `extract_written_tables` since the catalog landed -- but they
        # were never written down per notebook and the reads were never
        # found at all, so a notebook that writes `agg_claims` and one
        # that reads it were two nodes with no edge between them and the
        # plan emitted the reader first whenever the names sorted that
        # way.
        record["writes"] = sorted(extract_written_tables(cells))
        record["reads"] = sorted(extract_read_tables(cells))
        records.append(record)
        if log:
            log(f"  notebook {item.name} ({record['language']})")

    for record in records:
        if "parse_error" not in record:
            language_counts[record["language"]] = language_counts.get(record["language"], 0) + 1

    return {
        "summary": {
            "notebook_count": len(records),
            "language_counts": language_counts,
            "parse_error_count": parse_errors,
        },
        "items": {"notebooks": records},
    }
