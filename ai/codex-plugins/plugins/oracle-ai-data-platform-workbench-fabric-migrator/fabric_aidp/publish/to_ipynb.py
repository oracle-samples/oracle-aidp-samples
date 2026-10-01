"""Migrated Fabric notebook source -> a .ipynb AIDP can run as a task.

The artifacts this tool writes keep Fabric's block structure -- `# CELL`,
`# MARKDOWN`, `# META` -- because the translator round-trips it. So the
conversion is a re-serialisation, not a guess: our own parser reads the blocks
and each one becomes the matching Jupyter cell.

A NOTEBOOK_TASK needs a notebook. Uploading the `.py` verbatim gives AIDP a
file, which a task cannot run.

Two inputs, not one. `migrate` writes every notebook artifact under a `.py`
name whatever it read, so the text handed to `to_ipynb` is Fabric's `# CELL`
format *or* Jupyter JSON -- of 49 notebooks in microsoft/fabric-toolbox, 39
are the latter. An `.ipynb` on the way in is re-emitted from its own document
rather than rebuilt from blocks. Rebuilding lost everything the block format
has no place for; measured on a two-cell notebook, `# Heading` came back as
`Heading` -- the `# ` read as Fabric's comment marker, so the heading was
silently demoted to body text -- both cell ids vanished while the output
still declared `nbformat_minor` 5, where `id` is required, and the
`parameters` tag, `jupyter.source_hidden` and the top-level `widgets`
metadata went with them.

`outputs` and `execution_count` are the deliberate exception: they are
cleared, not carried. They record what a *Fabric* session computed, against
Fabric tables, before this tool rewrote the code to read OCI buckets
instead. Shipping them would show a reviewer results the emitted notebook
did not produce and cannot reproduce, and a saved plot or a rendered
DataFrame is also most of the bytes. Clearing them is what
`nbconvert --clear-output` does, for the same reason, and it leaves a
notebook that still runs: both fields stay present and nbformat-valid
(`outputs: []`, `execution_count: null`), which is what a code cell is
required to carry.

A Fabric parameters cell is plain assignments that Fabric overwrites with the
pipeline's values. AIDP does not: it hands a task's parameters to the
notebook through `oidlUtils.parameters.getParameter(name, default)` and runs
the cell as written. Measured on a live workspace, a job whose task carried
run_date=2026-09-01 and limit=100: the cell's `run_date` stayed "2000-01-01"
and `limit` stayed 5 -- tagging the cell `parameters` changed nothing --
while getParameter returned '2026-09-01' and '100'. So each literal
assignment in the parameters cell is followed by one that reads the task
parameter, falling back to the Fabric default.

A SQL cell is spelled `%%sql` in Fabric and `%sql` on AIDP, and the AIDP
kernel does not reject the Fabric spelling -- it skips the cell. Measured on a
live workspace: a `%%sql CREATE TABLE ...` cell created nothing, a `%%sql`
SELECT from a table that does not exist raised nothing, and the job reported
SUCCESS; the same cells as `%sql` ran. The translator leaves SQL cells
byte-for-byte alone on purpose, so the spelling is fixed here, where the
notebook is written for AIDP and nowhere else. Every one of those probes put
the SQL on the magic line; `_aidp_sql_magic` records what that does and does
not establish about the multi-line cell a migration actually emits.

`%%sparksql` is not rewritten, and that is a decision rather than an
oversight. It is not a Fabric cell magic: Fabric spells a Spark SQL cell
`%%sql` and records `sparksql` as the cell's *language* in `# META`.
Measured on this tree, `%%sparksql` is not routed to the translator's SQL
path either -- `_SQL_MAGIC_RE` is `^\\s*%%t?sql\\b`, which does not match it
-- so a cell opening with that line is translated as Python, raises no
finding, and is not a SQL cell in this tool's model at all. There is
nothing here to map to `%sql` and nothing to refuse by name; rewriting it
would be this module inventing a construct the rest of the tool does not
recognise. The recorded-language path, which is the real one, needs no
rewrite: those cells carry no magic line to correct. Zero occurrences in
the shipped corpora either way.
"""
from __future__ import annotations

import ast
import hashlib
import json
import re

from fabric_aidp.translate.ipynb_format import IpynbNotebook
from fabric_aidp.translate.notebook_format import SQL_LANGUAGES, parse_any
from fabric_aidp.translate.types import Finding

# papermill's tag, which Fabric writes too. On the `# CELL` format the
# parameters cell is named by its marker; on the `.ipynb` format the marker
# is gone and this tag is the only thing that says which cell it was.
# Measured: `ipynb_format` labels every code cell "CELL" and never
# "PARAMETERS CELL", so before this the `.ipynb` half of the corpus got no
# re-read at all and nothing said so.
PARAMETERS_TAG = "parameters"

_KERNEL = {
    "kernelspec": {"display_name": "Python 3", "language": "python",
                   "name": "python3"},
    "language_info": {"name": "python"},
}
# nbformat 4.5 made `id` a required property of every cell, constrained to
# 1-64 characters of `[a-zA-Z0-9-_]`. Jupyter generates a random one; this
# derives it from the cell's own content and position instead, so converting
# one artifact twice produces the same bytes and a re-publish diffs to
# nothing. 12 hex characters of a SHA-256, with the position in front, which
# is what makes it unique rather than the digest.
_ID_DIGEST_LENGTH = 12

# The one global the re-read block brings with it, held as source text
# because it has to run on a cluster that has never heard of this tool --
# the same arrangement as `m_runtime.HELPERS` and `NB08`'s DISPLAY_SHIM.
#
# On the guard. `oidlUtils` is injected by AIDP the way `notebookutils` is
# injected by Fabric: no import, just present. A bare
# `oidlUtils.parameters.getParameter(...)` in the parameters cell is
# therefore fine on AIDP and fatal anywhere else -- `NameError`, raised in
# the *first* cell, killing a notebook that until the migration ran happily
# on its defaults. That is a worse failure than the one this rewrite fixes,
# and it lands on every reader who opens the artifact locally to check it.
# So the name is resolved at call time inside a `try`, and the default --
# the value Fabric itself wrote in the cell -- is what a runtime without
# oidlUtils gets, which is exactly the behaviour the notebook had before.
#
# Two divergences, named rather than hidden, as DISPLAY_SHIM names its own:
#
#   * `AttributeError` is caught with `NameError`. They are the two ways
#     "this runtime has no oidlUtils" arrives -- the name absent, or present
#     without `parameters`/`getParameter`. Nothing broader is caught: a
#     getParameter that raises for its own reasons must still be seen.
#   * The helper is a global named `_aidp_parameter`. A notebook that
#     already binds that name has it overwritten here, because the block is
#     appended after the cell's own lines. The leading underscore is the
#     whole of the protection; `m_runtime` can prefix `_m_` and know nothing
#     collides, and there is no equivalent guarantee over a user's cell.
AIDP_PARAMETER_HELPER = '''def _aidp_parameter(name, default):
    """This task's parameter `name`, or `default` where AIDP is not running.

    AIDP injects `oidlUtils` into the notebook namespace with no import.
    Off AIDP the name is simply absent, and referring to it bare would end
    the notebook on NameError in its first cell -- where before the
    migration it ran on the defaults below. Resolved at call time, so the
    same file works in both places.
    """
    try:
        return oidlUtils.parameters.getParameter(name, default)
    except (NameError, AttributeError):
        return default
'''


def _note(findings, rule: str, detail: str, severity: str) -> None:
    """Append a finding, if the caller asked for any.

    `to_ipynb` had no findings channel at all: it is the second translator
    in the tool, it runs after `verify`, and everything it decided -- a
    parameters cell it could not parse, a parameter it could not re-read --
    it decided in silence. This is the channel. `publish` collects it; the
    older callers pass nothing and are unaffected.
    """
    if findings is not None:
        findings.append(Finding(rule, detail, severity))

# Horizontal whitespace only. `source` is a list of lines in one input shape
# and a single string in the other; `\s*` would let the indent run over a
# newline in the second, matching a `%%sql` on line 2, which is not a magic.
#
# `IGNORECASE` and `t?` match what the translator already routes to its SQL
# path, so publish cannot skip a cell the translator called SQL. Measured on
# this tree: `%%SQL` and `%%Sql` raise NB24_TSQL_IN_SQL_CELL, and `%%tsql` /
# `%%TSQL` raise SQ50_TOP and come back with `TOP 5` rewritten to `LIMIT 5`.
# Zero uppercase and zero `%%tsql` in the shipped corpora -- one `%%sql`,
# lowercase, is the only magic there -- so this is hardening, not a measured
# defect. `%%sparksql` is deliberately absent; see the module docstring.
#
# The cell's recorded language is not the hook here, tempting as it looks now
# that `metadata.fabric` carries it. Measured on the demo's `06_Sql_Summary`:
# a Fabric cell that opens with `%%sql` carries no `# META` block of its own,
# so it publishes with `metadata: {}` and nothing to read a language out of.
# The two are near-complements -- the language is recorded when the magic is
# absent -- and a `# META` language with no magic line needs no rewrite,
# because there is no magic in the source to correct.
_FABRIC_SQL_MAGIC = re.compile(r"^([ \t]*)%%t?sql(?=\s|$)", re.IGNORECASE)


def _aidp_sql_magic(body):
    """A Fabric SQL cell magic on line 1 -> `%sql`, the spelling AIDP runs.

    `%%sql` and `%%tsql`, either case, both become lowercase `%sql`. A
    `%%tsql` body is Spark SQL by the time it reaches here -- the translator
    converted the dialect -- so the T-SQL label is stale, and AIDP has no
    `%tsql` to map it to.

    **Measured on the AIDP cluster, 2026-09-29.** `%sql` is a CELL magic:
    it takes the cell BODY and ignores whatever else is on the magic's own
    line. Probe notebook published to /Workspace and run as a NOTEBOOK_TASK
    on cluster fabricTest (Spark 3.5.0); the job reported SUCCESS, every
    cell executed, and the marker tables it wrote say:

        %sql                                            -> table CREATED
        CREATE TABLE fabric_probe.r1_multiline_WORKED (a INT)

        %sql CREATE TABLE fabric_probe.r1_oneline_WORKED (a INT)
                                                        -> NOTHING created

    So the shape every real migrated cell has -- the magic alone on line 1
    with the SQL below, which is what `06_Sql_Summary` emits -- is the shape
    that works, and this rewrite is correct for it.

    The one-liner is the shape that does not. It runs without error and
    creates nothing, which is the same silent-no-op this whole function
    exists to remove, one shape over. Every probe in the PR that introduced
    this rewrite was a one-liner, so the evidence and the conclusion pointed
    opposite ways and only the live run separated them. Do not "simplify"
    this by folding a multi-line body onto the magic line: that would turn
    a working cell into a no-op.

    So a Fabric one-liner, `%%sql SELECT ...`, is split: `%sql` alone on
    line 1 and the statement below it. And the magic is written at column 0
    whatever its indent was. Measured on the AIDP cluster, 2026-09-30
    (Spark 3.5.0), two single-cell notebooks run as NOTEBOOK_TASKs:

        %sql CREATE TABLE default.saleslake.r2_oneliner AS SELECT 1 AS x
                                     -> task SUCCESS, table NOT created
        "  %sql" + newline + CREATE TABLE default.saleslake.r2_indent ...
                                     -> task FAILED (the cell ran as Python)

    Both shapes came out of this function before: the one-liner kept its
    statement on the magic line, and `\1` carried the indent through.

    nbformat lets `source` be a list of lines or one string, and a real
    export uses both, so both are rewritten. Only the first line is looked
    at either way: `%%sql` anywhere else is not a magic.
    """
    if isinstance(body, str):
        first, newline, rest = body.partition("\n")
        return _split_magic_line(first) + newline + rest if _FABRIC_SQL_MAGIC.match(first) else body
    if isinstance(body, list) and body and isinstance(body[0], str):
        if not _FABRIC_SQL_MAGIC.match(body[0]):
            return body
        ending = "\n" if body[0].endswith("\n") else ""
        fixed = _split_magic_line(body[0][:len(body[0]) - len(ending)]) + ending
        return fixed.splitlines(keepends=True) + body[1:]
    return body


def _split_magic_line(line: str) -> str:
    """`  %%sql SELECT 1` -> `%sql` + newline + `SELECT 1`; `%%sql` -> `%sql`."""
    statement = _FABRIC_SQL_MAGIC.sub("", line, count=1).strip()
    return "%sql\n" + statement if statement else "%sql"


def _aidp_sql_prefix(body, language, findings=None):
    """A SQL cell with no magic line at all -> `%sql` prepended.

    `_aidp_sql_magic` corrects a magic that is there. This adds one that
    never was, which is a different defect with the same consequence.

    Three shapes reach publish as bare SQL with no magic anywhere, and the
    translator has already applied its SQL rules to all three -- so the tool
    knows they are SQL and then wrote them into a Python cell. Measured on
    this tree before this existed, every one published as
    `cell_type: "code"` under a Python kernelspec:

        notebook-content.sql            ["SELECT * FROM dbo.claim LIMIT 5"]
        cell `language: tsql`           ["SELECT * FROM dbo.claim LIMIT 5"]
        cell `language: sparksql`       ["SELECT * FROM claim"]

    An AIDP NOTEBOOK_TASK runs that as Python. `SELECT ... FROM ...` is not
    Python, so the task dies on a SyntaxError -- loudly, which is the one
    mercy here, but a T-SQL notebook could never run after migration.

    The comment above `_FABRIC_SQL_MAGIC` used to say a recorded language
    with no magic line "needs no rewrite, because there is no magic in the
    source to correct". That was true of correcting and wrong about the
    cell: what it needs is a magic ADDED.

    `%sql` on its own line with the body below is the shape measured to
    work on the AIDP cluster -- see `_aidp_sql_magic`, where the run is
    recorded. Do not fold the body onto the magic line.
    """
    first = (body[0] if body else "")
    if first.lstrip().startswith("%"):
        # Already carries a magic: `_aidp_sql_magic` ran, or the author
        # wrote one. Prepending a second would make the first line data.
        return body
    if not any(line.strip() for line in body):
        return body
    if findings is not None:
        findings.append(Finding(
            "NB38_SQL_CELL_NEEDS_MAGIC",
            "this cell is %s and carried no cell magic, so AIDP would have "
            "run it as Python; `%%sql` prepended so the AIDP kernel runs it "
            "as SQL. The cell body is unchanged" % (
                "a T-SQL notebook's" if not language else
                "recorded as %r" % language),
            "rewrite"))
    return ["%sql\n"] + body


def _cell_id(position: int, source) -> str:
    text = "".join(source) if isinstance(source, list) else str(source or "")
    digest = hashlib.sha256(("%d\0%s" % (position, text)).encode("utf-8")).hexdigest()
    return "c%d-%s" % (position, digest[:_ID_DIGEST_LENGTH])


def _declares_cell_ids(data: dict) -> bool:
    """Whether this document's own nbformat version requires a cell `id`.

    4.5 requires one. Earlier 4.x does not define the property, so adding it
    there would put a field in the file that the reader's schema has never
    heard of.
    """
    try:
        version = (int(data.get("nbformat", 4)), int(data.get("nbformat_minor", 0)))
    except (TypeError, ValueError):
        return False
    return version >= (4, 5)


def _read_task_parameter(name: str, default) -> str:
    """`name = <default>` re-read from the AIDP task, keeping the default's type.

    getParameter returns text; the Fabric cell's literal says what type the
    notebook expects, so the value is converted back to it."""
    call = f"_aidp_parameter({name!r}, {str(default)!r})"
    if isinstance(default, bool):
        return f"{name} = str({call}).strip().lower() in ('true', '1')"
    if isinstance(default, (int, float)):
        return f"{name} = {type(default).__name__}({call})"
    return f"{name} = {call}"


def _literal(node):
    """(True, value) for a node whose Python type is written down, else (False, None).

    `ast.Constant` alone is not "a literal". `offset = -1` parses as
    `UnaryOp(USub, Constant(1))`, and the pipeline half carries -1 happily:
    the job supplied `offset` and the notebook ignored it, which is the
    defect this rewrite exists to close, for every negative default. `+1`
    is here for the same reason and costs nothing.

    `True` is an `int` to `isinstance`, so bool is separated first wherever
    the numeric branch would otherwise swallow it.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, int, float, bool)):
        return True, node.value
    if (isinstance(node, ast.UnaryOp)
            and isinstance(node.op, (ast.UAdd, ast.USub))
            and isinstance(node.operand, ast.Constant)
            and isinstance(node.operand.value, (int, float))
            and not isinstance(node.operand.value, bool)):
        value = node.operand.value
        return True, (-value if isinstance(node.op, ast.USub) else value)
    return False, None


def _assignment(node):
    """(name, value node) for a top-level `name = ...`, else None.

    `AnnAssign` is included: `limit: int = 5` is the same declaration with
    the type spelt out, and the pipeline half -- which reads Fabric's own
    `parameters` mapping, not this file -- cannot tell the two apart. An
    annotation carrying no value (`limit: int`) declares nothing to re-read.

    A target that is not a bare name (`cfg.limit = 5`, `a, b = 1, 2`) is not
    a parameter: Fabric overrides a parameter by name, and neither of those
    has one.
    """
    if isinstance(node, ast.Assign):
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            return node.targets[0].id, node.value
        return None
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return (node.target.id, node.value) if node.value is not None else None
    return None


def _construct(node) -> str:
    """What an un-re-readable value is, in words a reader can act on."""
    if isinstance(node, ast.JoinedStr):
        return "an f-string"
    if isinstance(node, ast.Constant) and node.value is None:
        return "None"
    if isinstance(node, (ast.List, ast.Tuple, ast.Set, ast.Dict)):
        return f"a {type(node).__name__.lower()}"
    if isinstance(node, ast.Call):
        return "a call"
    if isinstance(node, (ast.Name, ast.Attribute)):
        return "a reference to another name"
    if isinstance(node, ast.BinOp):
        return "an expression"
    return f"an {type(node).__name__}"


def _parameter_lines(lines, findings=None) -> list:
    """The parameters cell, with every literal assignment re-read from AIDP.

    Two things are deliberate about where the re-reads go.

    They are *appended*, after the cell's own assignments, not substituted
    for them. papermill -- whose `parameters` tag this is, and which Fabric
    follows -- injects its overrides as a new cell placed immediately after
    the tagged one. So a name derived inside the cell, `run_date =
    "2000-01-01"` then `start = run_date`, is already stale on Fabric for
    exactly the same reason. Appending reproduces the source's behaviour;
    substituting would silently make the migrated notebook behave better
    than the one it came from, and this tool does not do that.

    They cover only assignments whose type is written down (see `_literal`).
    Anything else is reported, not guessed: an AIDP task parameter is a
    string, and turning it back into an f-string, a list or None would be
    inventing a value.
    """
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError as exc:
        # Compare NB07_CELL_UNTOKENIZABLE, which names the construct rather
        # than passing the cell through in silence. Without this the cell
        # emerged unchanged, graded PASS, with every task parameter inert.
        _note(findings, "NB36_PARAMETER_CELL_UNPARSEABLE",
              f"the parameters cell does not parse ({exc.msg} at line "
              f"{exc.lineno}), so it is emitted exactly as written and no "
              f"task parameter reaches it", "flag")
        return lines
    reads = []
    for node in tree.body:
        assignment = _assignment(node)
        if assignment is None:
            continue
        name, value = assignment
        ok, literal = _literal(value)
        if ok:
            reads.append(_read_task_parameter(name, literal))
            continue
        _note(findings, "NB35_PARAMETER_NOT_RE_READ",
              f"the parameters cell sets {name!r} to {_construct(value)}, not a "
              f"literal, so its type cannot be restored from the task's string "
              f"and it is left as written; a job passing {name!r} is ignored here",
              "flag")
    if not reads:
        return lines
    return (lines + ["", "# AIDP passes task parameters through oidlUtils, not into",
                     "# this cell, and runs the cell as written. Appended rather than",
                     "# substituted because papermill -- whose `parameters` tag this",
                     "# is, and which Fabric follows -- injects its overrides in a",
                     "# cell *below* this one, so a name derived here from another",
                     "# is exactly as stale on AIDP as it already was on Fabric."]
            + AIDP_PARAMETER_HELPER.splitlines() + reads)


def _literal_names(lines) -> set:
    """The names in one parameters cell whose value this can re-read."""
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError:
        return set()
    names = set()
    for node in tree.body:
        assignment = _assignment(node)
        if assignment is not None and _literal(assignment[1])[0]:
            names.add(assignment[0])
    return names


def _parameter_cells(notebook) -> list:
    """Every parameters cell of a parsed notebook, as lists of lines.

    Two formats, one question. The `# CELL` format names the cell with a
    marker; the `.ipynb` format lost the marker and carries papermill's tag
    instead.
    """
    if isinstance(notebook, IpynbNotebook):
        cells = notebook.data.get("cells")
        return [str("".join(c.get("source"))
                    if isinstance(c.get("source"), list) else c.get("source") or "").split("\n")
                for c in (cells if isinstance(cells, list) else [])
                if isinstance(c, dict) and _is_parameters_cell(c)]
    return [list(block.lines) for block in notebook.blocks
            if block.kind == "PARAMETERS CELL"]


def task_parameters_read(source: str) -> frozenset:
    """The names a converted notebook reads back from its AIDP task.

    `publish` is the only place in the tool holding both halves of this PR
    at once -- the job, which says what a task passes, and the notebook,
    which says what it reads. Everywhere else the two are in different
    files and cannot disagree out loud. Exported so `plan_publish` can
    check one against the other.
    """
    notebook = parse_any(source)
    names = set()
    for lines in _parameter_cells(notebook):
        names |= _literal_names(lines)
    return frozenset(names)


def _cell(kind: str, lines, *, position: int, meta=None, findings=None,
          sql_source: bool = False) -> dict:
    if kind == "PARAMETERS CELL":
        lines = _parameter_lines(lines, findings)
    body = [line + "\n" for line in lines[:-1]] + lines[-1:] if lines else []
    # Fabric's per-cell `# META` block, under its own key for the same reason
    # the notebook-level one is: it is Fabric's, no nbformat schema knows it,
    # and a reader comparing the artifact to the original needs somewhere to
    # read the language the cell was recorded under.
    metadata = {"fabric": dict(meta)} if isinstance(meta, dict) and meta else {}
    if kind == "PARAMETERS CELL":
        # papermill and Fabric both key off this exact tag. Dropping it did
        # not break the notebook, which is what made it worth fixing: the
        # cell still runs, always with the defaults, and nothing says so.
        metadata["tags"] = ["parameters"]
    if kind == "MARKDOWN":
        # Fabric stores markdown as `# ` comments; strip one leading marker.
        body = [line[2:] if line.startswith("# ") else
                (line[1:] if line.startswith("#") else line) for line in body]
        return {"cell_type": "markdown", "id": _cell_id(position, body),
                "metadata": metadata, "source": body}
    # After the rewrite, not before: the id labels the source that is
    # actually emitted, so re-reading the published `.ipynb` and converting
    # it again lands on the same id rather than a second one.
    body = _aidp_sql_magic(body)
    # After the magic correction, so a cell that had one is not given a
    # second. `sql_source` is the notebook-level fact -- a
    # `notebook-content.sql` item's cells are bare SQL by construction --
    # and the recorded language covers a SQL cell inside a Python notebook.
    language = str((meta or {}).get("language") or "").strip().casefold()
    if sql_source or language in SQL_LANGUAGES:
        body = _aidp_sql_prefix(body, language if not sql_source else "",
                                findings)
    return {"cell_type": "code", "id": _cell_id(position, body),
            "execution_count": None, "metadata": metadata,
            "outputs": [], "source": body}


def _is_parameters_cell(cell: dict) -> bool:
    """A code cell papermill -- and so Fabric -- treats as the parameters cell."""
    if cell.get("cell_type") != "code":
        return False
    metadata = cell.get("metadata")
    tags = metadata.get("tags") if isinstance(metadata, dict) else None
    return isinstance(tags, list) and PARAMETERS_TAG in tags


def _from_ipynb(notebook: IpynbNotebook, findings=None) -> str:
    """Re-emit an `.ipynb` as its own document, with run state cleared.

    Everything the input wrote down is kept: cell ids, cell metadata, the
    `parameters` tag, notebook metadata including `widgets`, and each cell's
    `source` list exactly as it arrived. The module docstring says why
    `outputs` and `execution_count` are the exception.

    The parameters cell is the one exception to "source exactly as it
    arrived", and it is the same exception the `# CELL` format makes. This
    path used to make none: `ipynb_format` labels every code cell "CELL",
    never "PARAMETERS CELL", so the marker the other format keys off does
    not survive into this one and the re-read never happened. The papermill
    tag does survive -- it is kept here deliberately -- and it says the same
    thing, so it is what this keys off. Both formats now behave the same.

    `source` list as it arrived. `outputs` and `execution_count` are one
    deliberate exception -- the module docstring says why -- and a code
    cell's leading `%%sql` is the other, rewritten here for the same reason
    it is rewritten on the block path.
    """
    data = notebook.data
    cells = data.get("cells")
    wants_ids = _declares_cell_ids(data)
    nb_is_sql = bool(getattr(notebook, "sql_source", False))
    for position, cell in enumerate(cells if isinstance(cells, list) else []):
        if not isinstance(cell, dict):
            continue
        if _is_parameters_cell(cell):
            cell["source"] = _source_with_reads(cell.get("source"), findings)
        if wants_ids and not str(cell.get("id") or "").strip():
            cell["id"] = _cell_id(position, cell.get("source"))

        if cell.get("cell_type") == "code":
            # Before the id is derived, so a generated id labels the source
            # that is emitted. Of 49 notebooks in microsoft/fabric-toolbox
            # 39 arrive on this path, so skipping the rewrite here would
            # leave the fix covering the minority shape only.
            cell["source"] = _aidp_sql_magic(cell.get("source"))
            # And the same addition the block path makes: a SQL cell that
            # carries no magic at all needs one, or AIDP runs it as Python.
            # The cell's own `metadata.fabric.language` is what this tool
            # writes on the way out, so a round trip keeps working; the
            # notebook-level language covers a whole-notebook SQL source.
            cell_meta = cell.get("metadata")
            fabric_meta = (cell_meta or {}).get("fabric") if isinstance(cell_meta, dict) else None
            language = str((fabric_meta or {}).get("language") or "").strip().casefold()
            if nb_is_sql or language in SQL_LANGUAGES:
                cell["source"] = _aidp_sql_prefix(
                    cell["source"], language if not nb_is_sql else "", findings)
            cell["outputs"] = []
            cell["execution_count"] = None
        if wants_ids and not str(cell.get("id") or "").strip():
            cell["id"] = _cell_id(position, cell.get("source"))
    return json.dumps(data, **notebook.style["kwargs"]) + notebook.style["trailing"]


def _source_with_reads(source, findings=None) -> list:
    """An nbformat `source` with the re-reads appended, keeping its shape.

    nbformat stores source as a list of lines each ending in "\n", except
    the last. Rebuilt in that shape rather than as one string, because a
    reader diffing the published notebook against the input should see only
    the appended lines.
    """
    text = "".join(source) if isinstance(source, list) else str(source or "")
    lines = text.split("\n")
    # A source ending in "\n" splits to a trailing "". Keeping it would put
    # two blank lines before the appended block, where the `# CELL` format --
    # which strips its own blank edges before `_cell` sees them -- puts one.
    trailing = bool(lines) and lines[-1] == ""
    if trailing:
        lines.pop()
    rewritten = _parameter_lines(lines, findings)
    if rewritten == lines:
        return source if isinstance(source, list) else [text]
    return [line + "\n" for line in rewritten[:-1]] + rewritten[-1:]


def to_ipynb(source: str, *, findings=None) -> str:
    """Fabric notebook source text -> .ipynb JSON text.

    `findings` is an optional list this appends `Finding`s to, for what the
    conversion noticed and could not fix. It is optional so that every
    caller that only wants the bytes stays unchanged; `publish` passes one.
    """
    notebook = parse_any(source)
    if isinstance(notebook, IpynbNotebook):
        return _from_ipynb(notebook, findings)
    cells = []
    for block in notebook.blocks:
        if block.kind in ("CELL", "PARAMETERS CELL", "MARKDOWN"):
            lines = [line for line in block.lines]
            while lines and not lines[0].strip():
                lines.pop(0)
            while lines and not lines[-1].strip():
                lines.pop()
            if lines:
                cells.append(_cell(block.kind, lines, position=len(cells),
                                   meta=notebook.meta_for(block),
                                   findings=findings,
                                   sql_source=bool(getattr(
                                       notebook, "sql_source", False))))
    payload = {"cells": cells, "metadata": dict(_KERNEL),
               "nbformat": 4, "nbformat_minor": 5}
    meta = notebook.notebook_meta
    if isinstance(meta, dict) and meta:
        payload["metadata"]["fabric"] = meta
    return json.dumps(payload, indent=1) + "\n"
