"""Parse and serialize Fabric notebooks committed as Jupyter JSON.

Microsoft's Git source-code-format page documents `notebook-content.py`. Real
exports mostly use `notebook-content.ipynb`: of 49 notebooks in
microsoft/fabric-toolbox, 39 are .ipynb and 10 are .py. Both must work.

This produces the same `Block` list `notebook_format` produces, so every
downstream rule is unchanged and does not know which format it came from.

Real exports also differ in formatting -- most are indented, some minified --
so the serializer detects the style that reproduces the input byte-exactly
rather than imposing one. Reformatting someone's whole notebook because one
cell changed makes the diff unreviewable.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from fabric_aidp.translate.notebook_format import (
    Block, SQL_LANGUAGES, default_lakehouse_id_of, default_lakehouse_of,
    lakehouse_binding_of,
)

# Ordered most-likely-first. Each is tried with and without a trailing newline.
_STYLE_CANDIDATES = (
    {"indent": 2, "ensure_ascii": False},
    {"indent": 2, "ensure_ascii": True},
    {"separators": (",", ":"), "ensure_ascii": False},
    {"separators": (",", ":"), "ensure_ascii": True},
    {"indent": 1, "ensure_ascii": False},
    {"indent": 1, "ensure_ascii": True},
    {"indent": 4, "ensure_ascii": False},
    {"indent": 4, "ensure_ascii": True},
    {"ensure_ascii": False},
    {"ensure_ascii": True},
)
_DEFAULT_STYLE = {"kwargs": {"indent": 2, "ensure_ascii": False},
                  "trailing": "\n", "exact": False}


class IpynbParseError(ValueError):
    """The file is not a well-formed Jupyter notebook."""


def detect_style(text: str, data) -> dict:
    """The dumps kwargs + trailing newline that reproduce `text` exactly.

    Returns `exact: False` with a sensible default when nothing matches, so a
    caller can report honest reformatting instead of silently reflowing.
    """
    for kwargs in _STYLE_CANDIDATES:
        try:
            rendered = json.dumps(data, **kwargs)
        except (TypeError, ValueError):
            continue
        for trailing in ("\n", ""):
            if rendered + trailing == text:
                return {"kwargs": dict(kwargs), "trailing": trailing, "exact": True}
    return dict(_DEFAULT_STYLE)


def _source_list(lines) -> list:
    """Lines -> an nbformat `source` list.

    nbformat joins list entries with no delimiter, so every entry but the last
    carries its own newline. Writing the bare lines back glued every line of an
    edited cell onto the previous one: a leading comment swallowed the whole
    cell, and anything else became a SyntaxError.
    """
    parts = "\n".join(lines).split("\n")
    out = [part + "\n" for part in parts[:-1]]
    if parts[-1]:
        out.append(parts[-1])
    return out


def _source_lines(source) -> list:
    if isinstance(source, str):
        return source.split("\n")
    if isinstance(source, list):
        return "".join(str(part) for part in source).split("\n")
    return []


@dataclass
class IpynbNotebook:
    data: dict = field(default_factory=dict)
    blocks: list = field(default_factory=list)
    style: dict = field(default_factory=lambda: dict(_DEFAULT_STYLE))
    # Parallel to `blocks`: the index of each block's cell in data["cells"].
    _cell_index: list = field(default_factory=list)

    @property
    def code_blocks(self) -> list:
        return [b for b in self.blocks if b.kind == "CELL"]

    @property
    def notebook_meta(self) -> dict:
        meta = self.data.get("metadata")
        return meta if isinstance(meta, dict) else {}

    def meta_for(self, block: Block) -> dict:
        try:
            position = self.blocks.index(block)
        except ValueError:
            return {}
        cell = self.data["cells"][self._cell_index[position]]
        meta = cell.get("metadata")
        return meta if isinstance(meta, dict) else {}

    # The three lakehouse-binding questions, answered out of the same
    # helpers `FabricNotebook` uses. A caller holds whichever `parse_any`
    # returned and cannot see which class it has, so the two must agree.
    # They did not: this class read only `dependencies`, missing the
    # Synapse-era `synapse` spelling, and had no `default_lakehouse_id` at
    # all. `inventory/notebook.py` reads that property inside a guard that
    # catches only parse errors, so it raised AttributeError on the first
    # .ipynb notebook in an estate; that escaped to the per-source catch,
    # the whole notebook source reported FAILED, and plan, migrate and
    # verify saw no notebooks at all -- every one of them exiting 0.
    @property
    def lakehouse_binding(self) -> dict:
        return lakehouse_binding_of(self.notebook_meta)

    @property
    def default_lakehouse(self):
        return default_lakehouse_of(self.lakehouse_binding)

    @property
    def sql_source(self) -> bool:
        """Whether the whole notebook is recorded as SQL rather than Python.

        The block format answers this from its header line -- a
        `notebook-content.sql` item opens with `--`. An `.ipynb` has no such
        line, so this reads the recorded language instead:
        `metadata.language_info.name`, then the kernelspec's `language`.

        Not a measurement. Fabric's T-SQL notebook saves as
        `notebook-content.sql`, and an `.ipynb` spelling of one has not been
        seen in any export here -- all 25 vendored and all 31 migrated demo
        notebooks are Python. So this applies the block path's rule to the
        field an `.ipynb` records the same thing in, rather than reproducing
        a shape anyone has held. It exists because `FabricNotebook` has it
        and this class did not, and every caller reaches it through
        `getattr(nb, "sql_source", False)` -- which returns a default that
        looks like an answer. A wrong answer here is visible; a default is
        not.
        """
        meta = self.data.get("metadata")
        if not isinstance(meta, dict):
            return False
        info = meta.get("language_info")
        name = (info or {}).get("name") if isinstance(info, dict) else None
        if not name:
            spec = meta.get("kernelspec")
            name = (spec or {}).get("language") if isinstance(spec, dict) else None
        return str(name or "").strip().casefold() in SQL_LANGUAGES

    @property
    def default_lakehouse_id(self):
        return default_lakehouse_id_of(self.lakehouse_binding)


def parse_ipynb(text) -> IpynbNotebook:
    if not isinstance(text, str):
        raise IpynbParseError("notebook source must be a string")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise IpynbParseError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("cells"), list):
        raise IpynbParseError("not a Jupyter notebook: no 'cells' array")

    blocks, index = [], []
    for position, cell in enumerate(data["cells"]):
        if not isinstance(cell, dict):
            continue
        kind = "CELL" if cell.get("cell_type") == "code" else "MARKDOWN"
        blocks.append(Block(kind=kind, marker="", lines=_source_lines(cell.get("source"))))
        index.append(position)
    return IpynbNotebook(data=data, blocks=blocks,
                         style=detect_style(text, data), _cell_index=index)


def serialize_ipynb(nb: IpynbNotebook) -> str:
    """Rebuild the file, writing back only edited cell sources.

    A cell whose lines are unchanged keeps its original `source` value, so an
    untouched notebook round-trips byte-exactly even though ipynb permits both
    a string and a list of strings.
    """
    for position, block in enumerate(nb.blocks):
        cell = nb.data["cells"][nb._cell_index[position]]
        if _source_lines(cell.get("source")) == block.lines:
            continue
        cell["source"] = _source_list(block.lines)
    return json.dumps(nb.data, **nb.style["kwargs"]) + nb.style["trailing"]
