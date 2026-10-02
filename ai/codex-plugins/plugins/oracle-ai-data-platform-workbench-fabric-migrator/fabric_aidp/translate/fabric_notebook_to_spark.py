"""Translate a Fabric notebook to Spark that runs on AIDP.

Plan 1 rule set (Plan 2 deepens it):

  NB01  OneLake path literal          -> oci:// URI                 rewrite
  NB02  OneLake path not mappable     -> left in place              flag
  NB03  a notebookutils surface,
        however the import spelt it   -> left in place              flag
  NB04  display(df)                   -> left as written, see NB08  rewrite
  NB05  import notebookutils          -> commented out              rewrite
  NB06  notebook will not parse       -> returned unchanged         flag
  NB07  cell will not tokenize        -> left as written            flag
  NB08  display() is called           -> a display shim is spliced  rewrite
  NB09  FUSE path in a local file API -> left in place              flag
  NB16  a name the notebook registered
        as a temp view                -> left as written            info
  NB17  a four-part name: a linked
        server, or an item whose
        display name has a dot in it  -> left as written             flag
  NB18  the current database is moved
        somewhere this tool cannot
        follow                        -> nothing resolved after it  flag
  NB19  a bare name after NB18        -> left as written            flag
  NB20  a name matching two catalog
        entries at once               -> left as written            flag
  NB25  a workspace-qualified URI
        naming the bound lakehouse,
        with an unqualified path for
        it as well                    -> both rewritten, two buckets flag
        with no unqualified path      -> rewritten, one bucket       info
  NB26  a bare `Files/`/`Tables/`
        string with nothing saying
        it is a path                  -> left as written            flag
  NB27  a path inside a shortcut      -> the shortcut's own target  rewrite
  NB28  a path inside a shortcut whose
        target cannot be named        -> left as written            flag
  NB30  an `import sempy` -- Semantic
        Link, which is Fabric-only    -> left as written            flag
  NB31  a `from __future__` import
        below the cell NB08's shim
        would go in                   -> the shim goes below it     info
        and a cell above it binds
        `display`                     -> the shim still goes below  flag
  NB32  a `.sql()` argument built at
        run time from an f-string     -> left as written, unscanned flag
  NB33  a FUSE path rewritten with
        nothing on the line saying
        what reads it                 -> rewritten, reader unknown  flag
  NB34  an Azure Storage location,
        which is not OneLake          -> left as written, unmapped  flag
  NB39  a table name this notebook
        emits whose AIDP schema AIDP
        will not create (a hyphen, a
        space, a non-ASCII letter)    -> emitted as built, one per
                                         notebook                   flag

NB01/NB02 reach SQL as well as Python: a `%%sql` cell and the string
argument of a `.sql()` call both go through `apply_sql_rules`, which is the
one place the SQL rules live so the two carriers cannot drift apart again.

Markdown cells, metadata blocks and cell markers are never touched: the
translator edits `Block.lines` of code cells only and re-serializes, so the
round-trip invariant from Task 3 carries the rest through byte-for-byte. The
one thing ever *added* is NB08's display shim, which goes inside the lines of
an existing code cell for that reason.
"""
from __future__ import annotations

import ast
import bisect
import re
from dataclasses import dataclass

from fabric_aidp.translate.ipynb_format import IpynbParseError
from fabric_aidp.translate.notebook_format import (
    CELL_MAGIC_RE as _CELL_MAGIC_RE, NotebookParseError,
    PYTHON_LANGUAGES as _MAGIC_PYTHON_LANGUAGES,
    SQL_LANGUAGES as _MAGIC_SQL_LANGUAGES, check_meta, parse_any, serialize_any,
)
from fabric_aidp.translate.onelake_to_oci import (
    FORM_RELATIVE, is_azure_storage_path, is_fuse_path, is_onelake_path,
    map_onelake_path, parse_onelake_path,
)
from fabric_aidp.translate import tsql_to_spark_sql as tsql
from fabric_aidp.translate.python_text import (
    MASK_CHAR as _MASK_CHAR, MaskedPython, STRING_RE as _STRING_RE, Unmaskable,
    cell_views, masked_python, remasked as _remasked, sql_scan_text,
    untokenizable_reason, view_of as _view_of,
)
from fabric_aidp.translate.sql_text import (
    apply_replacements, cte_names, in_a_from_taking_call, masked,
    quoted_identifier_spans, string_literal_spans, strip_sql_comments,
)
from fabric_aidp.inventory.catalog import (
    REF_AMBIGUOUS, REF_INFERRED, REF_SHORTCUT, REF_UNKNOWN,
    TIER_NOTEBOOK_INFERRED, TIER_SHORTCUT, TIER_WAREHOUSE_DDL, candidates,
    classify_reference, entry_owner, names_no_tables, owning_item, resolve,
    temp_view_names,
)
from fabric_aidp.translate.shortcut_to_oci import map_target
from fabric_aidp.naming import (
    DEFAULT_CATALOG, aidp_table, note_unaddressable_schema,
)
from fabric_aidp.translate.types import Finding, TranslationResult

RULESET_COVERAGE = "notebook-v1"

# NB39: the notebook twin of SQ24. See `naming.note_unaddressable_schema`;
# measured on the AIDP cluster 2026-09-30, [a-z0-9_] after case-folding.
SCHEMA_NOT_ADDRESSABLE = "NB39_SCHEMA_NOT_ADDRESSABLE"

# Fabric injects both of these into the notebook namespace with no import at
# all, so they are in scope whether or not one appears.
_UTILS_MODULES = ("notebookutils", "mssparkutils")
_UTILS_IMPORT_HEAD_RE = re.compile(
    r"^\s*(?:import\s+(?:notebookutils|mssparkutils)\b"
    r"|from\s+(?:notebookutils|mssparkutils)(?:\.\w+)*\s+import\b)")
# Semantic Link. Unlike notebookutils it is an ordinary importable library,
# not something Fabric injects, so the import statement is always written
# down and is a complete anchor -- there is no equivalent of NB03's search
# for injected surfaces. It is Fabric-only all the same: `sempy` talks to the
# Power BI XMLA endpoint and the Fabric REST API, neither of which exists on
# AIDP, and the import itself fails there.
_SEMPY_IMPORT_RE = re.compile(
    r"^\s*(?:import\s+sempy\b|from\s+sempy(?:\.\w+)*\s+import\b)")
# `def` is captured rather than excluded: a variable-width lookbehind is not
# available, and `def display(...)` is the user defining their own, not a call.
_DISPLAY_CALL_RE = re.compile(r"(?<![\w.])(?P<def>def\s+)?display\s*\(")

# The shim a migrated notebook carries, held as source text because it has to
# run on a cluster that has never heard of this tool -- the same arrangement
# as `m_runtime.HELPERS`, spliced only into the notebooks that call display().
#
# On the name: `m_runtime` prefixes its helpers `_m_` because `sanitize()`
# strips leading underscores, so no generated variable can ever collide with
# one. There is no such freedom here. Fabric's `display` is a *builtin*, so
# every call site spells it `display`, and the whole point of this fix is to
# leave those call sites as written. What is controlled instead: the shim
# brings exactly one global name with it -- no module-level import, no helper
# -- and it is spliced at the top of the cell that makes the first call, so a
# `def display` of the user's own still shadows it, exactly as it shadowed
# Fabric's builtin.
DISPLAY_SHIM = '''def display(obj, *_args, **_kwargs):
    """Fabric's `display()` builtin. AIDP does not have one.

    Fabric renders a Spark DataFrame, a pandas DataFrame, a matplotlib figure
    and more through this one name. Rewriting each call site to `.show()` was
    right for the first and fatal for the second: a pandas DataFrame has no
    `show`, so `display(pdf)` became `pdf.show()` and the notebook died on
    AttributeError. So the dispatch moved here, where the object's type is
    known, and the call sites were left alone.

    Duck-typed rather than isinstance-checked, on purpose: pyspark may not be
    importable when this is defined, and Spark Connect's DataFrame is a
    different class from the classic one in Spark 3.4 and 3.5. A callable
    `show` together with a `schema` separates a Spark DataFrame (both) from a
    pandas DataFrame (neither) and from a matplotlib figure (`show` only).

    Two divergences from Fabric, neither of them fixable from here:
    `.show()` prints 20 rows and truncates wide columns where Fabric renders
    a scrollable grid, and Fabric's rendering options -- `summary=` and the
    extra arguments its overloads take -- are accepted and ignored.
    """
    show = getattr(obj, "show", None)
    if callable(show) and hasattr(obj, "schema"):
        show()
        return
    try:
        from IPython.display import display as _render
    except ImportError:
        print(obj)
    else:
        _render(obj)
'''



# Calls whose quoted argument is a *table name*. The `spark.catalog` methods
# are here and not on the SQL path below because none of them takes SQL: they
# take the same name `spark.table()` does, so they have to come out of this
# tool spelt the same way. Measured before they were added:
#
#   spark.table("dbo.claim")               -> spark.table("default.Sales.claim")
#   spark.catalog.listColumns("dbo.claim") -> unchanged, no finding
#
# -- one notebook, one table, two names, and the second does not resolve.
# Deliberately not here: `setCurrentDatabase` (a database, see NB18),
# `dropTempView` / `dropGlobalTempView` (a session-local view name) and
# `refreshByPath` (a path).
_CATALOG_TABLE_CALLS = (
    "cacheTable", "uncacheTable", "isCached", "refreshTable",
    "recoverPartitions", "listColumns", "getTable", "tableExists",
    "createTable", "createExternalTable",
)
# Methods that take a table name whatever the receiver is. Each one is
# distinctive enough to match on the method name alone: no widely used
# library spells a method this way and puts something other than a table
# name in that argument.
#
# `saveAsTable` was the only entry here, so this list *was* the read side
# plus one write. Measured on this tree with the demo catalog and
# `default_lakehouse="SalesLake"`:
#
#   spark.table("claim")               -> spark.table("default.AcmeDW.claim")
#   df.write.saveAsTable("claim")      -> saveAsTable("default.AcmeDW.claim")
#   df.write.insertInto("claim")       -> unchanged, no finding
#   df.writeTo("claim").append()       -> unchanged, no finding
#   df.writeStream.toTable("claim")    -> unchanged, no finding
#   spark.readStream.table("claim")    -> unchanged, no finding
#   DeltaTable.forName(spark, "claim") -> unchanged, no finding
#
# -- so a notebook that read with `spark.table` and wrote with any of the
# last five shipped two names for one table in one file, which is the
# invariant `tests/test_one_name.py` and the README's "One name for one
# table" section exist to hold.
#
# That test reads this tuple rather than repeating it, so the next method
# added here is covered by the invariant the day it is added -- the way the
# five above were not.
#
# Deliberately not here: `tableName` (a builder step on DeltaTable's
# create/replace API, whose argument is a table name but whose call is
# bounded by a `.execute()` several lines later) and `table` on its own,
# which is far too common a method name to match without a receiver -- see
# the read branch of the pattern below.
_TABLE_NAME_METHODS = (
    "saveAsTable",     # DataFrameWriter.saveAsTable
    "insertInto",      # DataFrameWriter.insertInto
    "toTable",         # DataFrameWriter.toTable and DataStreamWriter.toTable
    "writeTo",         # DataFrame.writeTo -- the DataFrameWriterV2 entry point
)
# The read surfaces, which are all `.table(...)` on some receiver:
# `<session>.table`, `<session>.read.table` and `<session>.readStream.table`.
#
# The receiver is any dotted name rather than the literal `spark`. Binding
# the session to another name is ordinary Fabric notebook code
# (`ss = SparkSession.builder.getOrCreate()`), `_SQL_CALL_RE` below already
# matches `.sql()` on any receiver for exactly that reason, and
# `inventory/catalog.py`'s `_READ_TABLE_RE` -- which answers the same
# question for the dependency graph -- has matched any receiver since it
# was written. The three disagreeing was a one-name defect of its own: the
# inventory recorded `ss.table("claim")` as a read of `claim` while this
# rule left that name unresolved beside a `spark.table("claim")` it had
# rewritten in the cell above.
_TABLE_READ_CALL = (r"(?<![\w.])[A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*"
                    r"\s*\.\s*table")
_TABLE_CALL_RE = re.compile(
    r"(?P<call>(?:" + _TABLE_READ_CALL
    + r"|(?<!\w)(?:" + "|".join(_TABLE_NAME_METHODS) + r")"
    + r"|(?<!\w)spark\s*\.\s*catalog\s*\.\s*(?:"
    + "|".join(_CATALOG_TABLE_CALLS) + r"))\s*\(\s*)"
    r"(?P<q>['\"])(?P<name>[^'\"]+)(?P=q)")
# `DeltaTable.forName(spark, "claim")`. The session is the first argument
# and the table name the second, so this one cannot join the list above:
# every other call here has the name in the first position. Anchored on the
# `DeltaTable` receiver because `forName` on its own is Java's
# `Class.forName` at least as often as it is Delta's. `forPath` is not here
# -- that takes a path, and `_SPARK_PATH_CALLS` has it.
_DELTA_FOR_NAME_RE = re.compile(
    r"(?P<call>(?<!\w)DeltaTable\s*\.\s*forName\s*\(\s*[^,'\"()]+,\s*)"
    r"(?P<q>['\"])(?P<name>[^'\"]+)(?P=q)")
# Every pattern whose `name` group is a table reference in Python source.
# `rule_table_refs` walks this rather than one pattern, so adding a call
# shape is adding an entry here.
_TABLE_REFERENCE_PATTERNS = (_TABLE_CALL_RE, _DELTA_FOR_NAME_RE)


# Up to *four* parts, not three. The pattern used to stop at three, so
# `FROM a.b.c.d` matched `a.b.c` and left `.d` dangling behind it -- harmless
# only for as long as a three-part name was declined outright. Once one
# resolves, the same match becomes `default.a_b.c.d`: a name assembled from
# half a reference, emitted as an ordinary rewrite. The name is matched whole
# so `_resolved_table_name` can see how many parts there really are.
_SQL_TABLE_RE = re.compile(
    r"\b(?P<kw>FROM|JOIN|INTO|UPDATE)\s+(?P<name>[A-Za-z_][\w$]*"
    r"(?:\s*\.\s*[A-Za-z_][\w$]*){0,3})",
    re.IGNORECASE)


def _clean_part(part: str) -> str:
    """One name part with whatever quoting it arrived in taken off."""
    return str(part or "").strip().strip("`\"[]").strip()


def _note_azure_storage(value: str, findings: list) -> bool:
    """NB34 when `value` is an Azure Storage location. True when it was.

    Azure Storage is not OneLake and this tool does not map it. PR #27
    decided that deliberately and the decision stands -- what did not was
    saying nothing at all, so a clean report could not be told apart from
    a notebook whose reads all point at a system outside the migration.
    Measured on this tree before:

      spark.read.parquet("wasbs://c@acct.blob.core.windows.net/p/x")  []
      spark.read.parquet("abfss://c@acct.dfs.core.windows.net/p/x")   []

    -- the second one letter of host away from a URI that is rewritten and
    reported.

    What the finding must not do is claim the read will fail. Whether an
    AIDP cluster can reach an Azure Storage account depends on the driver
    and the credentials configured on it, and this tool has no way to
    know; asserting either answer would be the kind of confident guess
    NB17 was just corrected for.
    """
    if not is_azure_storage_path(value):
        return False
    findings.append(Finding(
        "NB34_AZURE_STORAGE_PATH",
        f"{value!r} is an Azure Storage location, not OneLake -- the host "
        f"is an Azure Storage account and not "
        f"`onelake.dfs.fabric.microsoft.com`. This tool maps OneLake "
        f"locations and nothing else, so it was left exactly as written; "
        f"that is the right answer and it is now a stated one, because "
        f"the two URIs differ only in the host and silence read as `there "
        f"was no path here`. Whether this read works from AIDP depends on "
        f"the driver and the credentials configured on that cluster, "
        f"which this tool cannot see -- confirm it before running, or "
        f"copy the data into the estate being migrated",
        "flag"))
    return True


def _excerpt(text: str, limit: int = 70) -> str:
    """`text` on one line, short enough to sit inside a finding.

    A finding carries no line number, so a query has to be recognisable
    from its own text; a triple-quoted one runs to dozens of lines, which
    would make the report unreadable instead.
    """
    flat = " ".join(str(text or "").split())
    return repr(flat if len(flat) <= limit else flat[:limit - 1] + "…")

# Every way a notebook registers a session-local view now lives in
# `inventory.catalog`, which this module already imports: a temp view is not
# a catalog table, and the inventory needs the same answer to decide that a
# notebook reading its own view does not depend on whoever writes a table of
# that name. It matters here because of NB10-on-SQL-strings: before that rule
# existed a bare name inside `spark.sql(...)` was never rewritten, so a view
# could not be mistaken for a table. Now it can, and the mistake is silent --
# the query still runs, against a different object.


# `USE <db>` and its Spark spellings, one statement to a line. Anchored to
# the start of a line and required to end at a `;` or the line end, which is
# the filter that keeps prose out: three cells in the bundled corpus open a
# comment with `# Use first load when no data exists yet`, and a looser
# pattern reads that as a database switch.
_USE_STATEMENT_RE = re.compile(
    r"^[ \t]*USE[ \t]+(?:CATALOG[ \t]+|DATABASE[ \t]+|SCHEMA[ \t]+"
    r"|NAMESPACE[ \t]+)?(?P<name>[A-Za-z_][\w$]*|`[^`]+`)[ \t]*(?:;|$)",
    re.IGNORECASE | re.MULTILINE)
# The Python half. Matched on the method name alone rather than on
# `spark.catalog.`, because the session is routinely bound to another name
# and `setCurrentDatabase` belongs to nothing else.
_SET_CURRENT_DB_RE = re.compile(
    r"(?<!\w)setCurrent(?:Database|Catalog)\s*\(\s*"
    r"(?P<q>['\"])(?P<name>[^'\"]+)(?P=q)")


@dataclass
class NotebookScope:
    """What a table reference needs to know beyond the line it sits on.

    Rules are per line (or, for a SQL string, per cell), but three facts are
    notebook-wide: which names are session-local temp views, which Fabric
    items the export knows about, and where the notebook has moved the
    current database to. They are carried here rather than in module state
    so `translate` stays re-entrant and each rule can still be called on its
    own -- a rule given no scope behaves exactly as it did before there was
    one.

    `current_item` and `unresolvable_database` are mutually exclusive and
    are updated once per cell, before that cell's rules run. So a switch
    governs the cell that contains it, whole, including the lines above it.
    That is imprecise by one cell and deliberately so: the rules run per
    line and per cell, statements do not carry positions across that
    boundary, and the imprecision only ever makes a bare name *less*
    resolved, never differently resolved.
    """

    temp_views: frozenset = frozenset()
    known_items: frozenset = frozenset()   # casefolded Lakehouse/Warehouse names
    current_item: str = ""                 # a switch we can follow: resolve here
    unresolvable_database: str = ""        # a switch we cannot follow


def _known_items(catalog) -> frozenset:
    """Every Fabric item the catalog names, casefolded.

    A `USE <name>` is followable when `<name>` is one of these: the export
    says what the item is, so the AIDP name is the one the naming rule
    already builds for it. Anything else is not -- Fabric's Spark exposes an
    attached Lakehouse as a database and a schema-enabled Lakehouse's
    schemas as databases too, and those two produce different AIDP names
    from the same statement.

    Read through `catalog.entry_owner`, which is the same function
    `resolve` uses to decide which item an entry belongs to. This asked
    only for `warehouse`/`lakehouse`, the two fields that predate `owner`,
    so the two disagreed about one tier: a notebook-inferred entry records
    the writing notebook's lakehouse in `owner` and in neither of the
    other two. Measured on this tree, a catalog built from one notebook
    bound to `Bronze` that writes `events`, and a reader doing
    `spark.sql("USE Bronze")` then `spark.table("events")`:

      before  known_items = {}
              NB18_CURRENT_DATABASE flag  "...is not a Lakehouse or
                Warehouse named anywhere in this export"  -- which is
                false; the export names it, as that notebook's binding
              NB19_NAME_AFTER_DATABASE_SWITCH flag, `events` left bare
      after   known_items = {'bronze'}
              NB14_TABLE_INFERRED rewrite, 'events' -> 'default.Bronze.events'

    The writer already spells that table `default.Bronze.events` from its
    own `saveAsTable`, so the old answer was two names for one table as
    well as two flags that should not exist.

    Guarded by `names_no_tables`, the same predicate the two SQL rules in
    this file already ask and the same one `migrate` asks once per run.
    This was `(catalog or {}).get("tables") or {}` and then `.values()`,
    which assumes a dict there. MEASURED on 2a223bd:

        {'tables': 'dbo.claim'} -> AttributeError: 'str' object has no
                                   attribute 'values'
        {'tables': ['a']}       -> AttributeError: 'list' ...
        {'tables': 42}          -> AttributeError: 'int' ...
        {'tables': {...}}       -> frozenset({'acmedw'})

    and end to end, through `migrate` with that dict as the plan's
    `resolved_catalog` and one notebook doing `spark.table("dbo.claim")`:

        tables='dbo.claim'   status=error  rules=[]
                             error="'str' object has no attribute 'values'"
        tables={}            status=ok     rules=[]

    -- the #35 shape: an uncaught exception becomes a status `error` row
    with no rule attached, so the report cannot say what went wrong.

    `frozenset()` is not an answer invented for those shapes. It is the
    answer `names_no_tables` already gives every other reader of the same
    catalog: `not isinstance(tables, dict) or not tables` is its whole
    body, so the three shapes above are already "this catalog resolves
    nothing" everywhere else in this file, and the T-SQL translator already
    returns normally for all three. This function was the one reader that
    disagreed, and it disagreed by raising before any rule ran.

    Nor is it silent. `migrate` computes
    `catalog_resolved_nothing = names_no_tables(resolved_catalog)` off the
    same plan, and MEASURED on the same three shapes that is True for all
    of them, with `catalog_resolved_nothing: true` in report.json and
    exactly one "names no tables" line in report.md -- identical to what
    `{"tables": {}}` produces. That is the scope at which a catalog naming
    nothing is worth saying, which is why an assertion here would be a
    second and contradictory answer to a question already answered.

    No plan this tool writes produces those shapes; a hand-edited one can,
    and `resolved_catalog` is read straight off the plan JSON.
    """
    if names_no_tables(catalog):
        return frozenset()
    return frozenset(
        owner for owner in (entry_owner(entry)
                            for entry in catalog["tables"].values())
        if owner)


def _scan_database_switches(text: str, findings: list, *, default_lakehouse,
                            scope) -> None:
    """Apply one cell's `USE` / `setCurrentDatabase` statements to `scope`.

    Scanned on the raw cell text, unmasked, which is the opposite of what
    every other rule here does and is the right call for this one: the
    switch that matters most is `spark.sql("USE other")`, and the Python
    mask blanks exactly that. The cost of the looser scan is a switch
    matched where none runs, and its only effect is to leave bare names as
    written -- the safe direction, and the one the NB18 flag makes visible.
    """
    switches = sorted(
        [(m.start(), _clean_part(m.group("name")))
         for m in _USE_STATEMENT_RE.finditer(text)]
        + [(m.start(), _clean_part(m.group("name")))
           for m in _SET_CURRENT_DB_RE.finditer(text)])
    default = str(default_lakehouse or "").casefold()
    for _at, database in switches:
        folded = database.casefold()
        if default and folded == default:
            scope.current_item, scope.unresolvable_database = "", ""
            continue
        if folded in scope.known_items:
            scope.current_item, scope.unresolvable_database = database, ""
            continue
        if scope.unresolvable_database == database:
            # Already in force. One finding per change of state, not per
            # statement: a cell is scanned raw and its SQL bodies are
            # scanned again on their own, so a `USE` written on its own line
            # inside a triple-quoted query is seen twice.
            continue
        findings.append(Finding(
            "NB18_CURRENT_DATABASE",
            f"this notebook sets the current database to {database!r}, which "
            f"is not the default lakehouse and is not a Lakehouse or "
            f"Warehouse named anywhere in this export. Fabric's Spark "
            f"exposes an attached Lakehouse as a database and a "
            f"schema-enabled Lakehouse's schemas as databases too, so "
            f"{database!r} could be either and the two give different AIDP "
            f"names -- rather than guess, every unqualified table reference "
            f"from here on was left exactly as written. Qualify those names, "
            f"or confirm which item {database!r} is",
            "flag"))
        scope.current_item, scope.unresolvable_database = "", database


def _temp_view_names(notebook) -> frozenset:
    """Every name this notebook registers as a temp view, in any cell.

    Scanned over the *raw* cell text, unmasked and without regard to cell
    order, which over-approximates in two directions -- a registration
    inside a comment counts, and one that executes after the reference
    counts. Both errors point the same way: the name is left exactly as
    written, which is the safe answer. The opposite error, rewriting a view
    reference into a three-part table name, silently reads a different
    object and is the one worth spending precision on.
    """
    names = set()
    for block in getattr(notebook, "code_blocks", ()) or ():
        names |= temp_view_names("\n".join(getattr(block, "lines", ()) or ()))
    return frozenset(names)


# Where a resolved table lives, and under which schema. This was private to
# this module. The warehouse T-SQL rules need the identical answer and
# cannot import it from here -- this module imports that one -- so it lives
# in `inventory.catalog` now, next to `classify_reference`. The two halves
# of consulting the catalog belong in one place: "does this table exist" and
# "where does it live" were answered in different modules, and the warehouse
# path only ever asked the first, so it named tables it had just confirmed
# were somewhere else. Kept under the old name here so the ten call sites
# and the tests that reach for it read unchanged.
_owning_item = owning_item


def _resolved_table_name(name: str, findings: list, default_lakehouse, catalog,
                         aidp_catalog=DEFAULT_CATALOG, scope=None):
    """The AIDP three-part name for `name`, or None to leave it alone.

    Shared by the Python-call and SQL-clause rules so both obey the same
    shortcut, inference and unknown-table contracts.

    `catalog` is the tier-resolution map (name -> {tier, ...});
    `aidp_catalog` is the AIDP catalog the name is built under. Two different
    things that shared one word, which is how `plan --catalog myc` came to
    reach the plan, the warehouse SQL and the dataflows while notebooks kept
    saying `default`.
    """
    parts = name.split(".")
    if len(parts) >= 4:
        # Four parts is the one shape here with no AIDP answer at all, and
        # refusing it is right. What the message used to get wrong is
        # *why*: it asserted T-SQL's linked-server form,
        # `server.database.schema.object`, as though that were the only
        # reading. It is not. A Fabric item's display name may contain a
        # dot, so `my.item.dbo.t` is a three-part reference to a table in
        # an item called `my.item` at least as readily as it is a
        # four-part reference to a remote server -- and the two want
        # opposite treatment, one resolved and one refused. Nothing in the
        # name says which, so the refusal stands and the finding now says
        # both. It used to return early alongside the three-part case and
        # reach the output unmentioned.
        findings.append(Finding(
            "NB17_FOUR_PART_NAME",
            f"table {name!r} has {len(parts)} dot-separated parts, which "
            f"is ambiguous and was left exactly as written. It reads two "
            f"ways: T-SQL's linked-server form "
            f"(server.database.schema.object), which has no AIDP "
            f"equivalent at all -- there is no linked server to resolve "
            f"the first part against, and dropping it would silently point "
            f"the query at a local table of the same name; or a Fabric "
            f"item whose display name contains a dot, which is an ordinary "
            f"three-part reference this tool would resolve if the export "
            f"named that item. Nothing in the reference says which, so "
            f"rather than guess: if it is a remote object, migrate it and "
            f"reference the migrated table, or read the remote system "
            f"directly from Spark; if the item is really called "
            f"{'.'.join(parts[:-2])!r}, qualify the reference with the "
            f"owning Lakehouse or Warehouse so it is not read as a server",
            "flag"))
        return None
    if len(parts) == 3 and _clean_part(parts[0]).casefold() == str(
            aidp_catalog or DEFAULT_CATALOG).strip().casefold():
        # Already an AIDP name -- catalog.schema.table. Resolving it again
        # would fold the catalog into the schema position and produce
        # `default.default.claim`.
        return None
    if (scope is not None and "." not in name
            and name.strip("`\"[]").casefold() in scope.temp_views):
        findings.append(Finding(
            "NB16_TEMP_VIEW",
            f"{name!r} is registered as a temp view by this notebook, so it "
            f"names a session-local view rather than a catalog table and was "
            f"left as written. The scan is notebook-wide and does not follow "
            f"cell order, so a reference that really does run before the "
            f"registration is left as written too",
            "info"))
        return None
    if scope is not None and scope.unresolvable_database and len(parts) == 1:
        # Only a bare name: `USE` sets the database an unqualified name is
        # looked up in, and a two- or three-part reference carries its own.
        findings.append(Finding(
            "NB19_NAME_AFTER_DATABASE_SWITCH",
            f"table {name!r} is unqualified and this notebook set the "
            f"current database to {scope.unresolvable_database!r} (NB18), so "
            f"resolving it against the default lakehouse would name a "
            f"different table; left exactly as written",
            "flag"))
        return None
    # Which item this reference reads in, as far as the notebook says. It
    # is worked out before resolution now because the catalog needs it: two
    # Fabric items may each hold a table of this name, and the binding is
    # what picks between them.
    if len(parts) == 3:
        item_hint, reference_schema, table = parts
    else:
        # A followable `USE <item>` moves where a *bare* name lives; a
        # two-part name names its own container, so the switch leaves it be.
        item_hint = default_lakehouse
        if len(parts) == 1 and scope is not None and scope.current_item:
            item_hint = scope.current_item
        reference_schema, table = (parts[0], parts[1]) if len(parts) == 2 else ("", name)

    # One question -- "is this table known, and what kind of thing is it" --
    # asked in one place. The warehouse T-SQL translator asks the same one
    # through the same call and maps the verdict onto its own SQ19 ids;
    # until this batch it asked nothing at all, and the two paths disagreed
    # about the same reference. Keeping the tier tests inline here as well
    # would be a second copy of a decision this codebase has already paid
    # twice for forking.
    verdict, entry, matched = classify_reference(catalog, name, owner=item_hint)
    if verdict == REF_AMBIGUOUS:
        named = ", ".join(sorted(
            repr(str(m.get("name") or "?")) for m in matched))
        findings.append(Finding(
            "NB20_TABLE_AMBIGUOUS",
            f"table {name!r} matches {len(matched)} catalog entries "
            f"({named}) and this reference does not say which; it was "
            f"left exactly as written. Qualify it with the owning "
            f"Lakehouse or Warehouse, or its schema",
            "flag"))
        return None
    if verdict == REF_UNKNOWN:
        findings.append(Finding(
            "NB12_TABLE_UNKNOWN",
            f"table {name!r} was not found in warehouse DDL, shortcuts, a supplied "
            f"catalog, or any notebook write; confirm it exists on AIDP before "
            f"running this",
            "flag"))
        return None
    if verdict == REF_SHORTCUT:
        findings.append(Finding(
            "NB11_TABLE_IS_SHORTCUT",
            f"table {name!r} is a shortcut to {entry.get('target', '?')!r}; its data "
            f"lives outside this lakehouse, so it was not rewritten to a three-part "
            f"name",
            "flag"))
        return None
    # Fabric's three-part name is `item.schema.table` -- the item being the
    # Lakehouse or Warehouse -- which is exactly the two containers
    # `aidp_table` needs, so the reference answers both. It goes through
    # `_owning_item` all the same, with the reference's item standing in for
    # the notebook's default binding: a catalog entry that records an owning
    # item still wins -- the Warehouse that declared the table, the Lakehouse
    # a notebook write landed in, or the item a `--tables-csv` row named --
    # or the same table read as `dbo.ledger` in one cell and
    # `Sales.dbo.ledger` in the next would come out under two names again.
    item, schema = _owning_item(entry, reference_schema, item_hint)
    if not item:
        findings.append(Finding(
            "NB13_NO_DEFAULT_LAKEHOUSE",
            f"table {name!r} is "
            f"{'schema-qualified' if reference_schema else 'unqualified'}, its "
            f"catalog entry ({entry.get('tier', '?')}) does not record an owning "
            f"Lakehouse or Warehouse, and this notebook has no default lakehouse "
            f"binding, so the target item cannot be determined",
            "flag"))
        return None
    if len(parts) == 3 and item_hint and str(item).casefold() != str(item_hint).casefold():
        # The owner-wins rule above is deliberate, but here it overrode an
        # item the author WROTE, and did it silently: `ArchiveLake.dbo.claims`
        # came out as `default.SalesLake.claims`, a rewrite graded PASS.
        # Lakehouse tables are not in a Git export, so the written item may
        # well hold a `claims` the catalog never saw -- which answer is right
        # cannot be decided from the export, and it must not read as settled.
        findings.append(Finding(
            "NB40_ITEM_OVERRIDDEN",
            f"table {name!r} names item {item_hint!r}, but the only catalog "
            f"entry for {table!r} belongs to {item!r}, so it was migrated under "
            f"{item!r}. Lakehouse tables are not in a Fabric Git export, so "
            f"{item_hint!r} may hold its own {table!r}; confirm which one this "
            f"notebook reads",
            "flag"))
    resolved = aidp_table(item, table, schema=schema, catalog=aidp_catalog)
    if verdict == REF_INFERRED:
        # `rewrite`, not `info`. The name IS rewritten here -- `resolved` is
        # returned and substituted, exactly as for NB10 -- and only the
        # severity differed. `TranslationResult.changes` counts `rewrite`
        # findings alone and `format_verify` prints `changes=` only when
        # that count is non-zero, so a notebook whose only findings were
        # NB14 reported zero changes on an artifact it had edited. Measured
        # on the demo: `notebook.02_Build_Aggregates` has two NB14 rewrites
        # and verify printed `PASS notebook.02_Build_Aggregates` with no
        # change count beside it at all.
        #
        # What `info` was reaching for -- this one is less certain than an
        # NB10, because the table's existence is inferred from another
        # notebook's write rather than read out of DDL -- stays in the
        # detail, where it names that notebook and says it must run first.
        # It is not a `flag`: nothing here needs a human to rewrite it, and
        # the rewrite is the same one NB10 would make.
        findings.append(Finding(
            "NB14_TABLE_INFERRED",
            f"table {name!r} -> {resolved!r}; its existence is inferred from a write "
            f"in notebook {entry.get('created_by', '?')!r}, which must run first",
            "rewrite"))
    else:
        findings.append(Finding(
            "NB10_TABLE_REF", f"table {name!r} -> {resolved!r}", "rewrite"))
    note_unaddressable_schema(findings, SCHEMA_NOT_ADDRESSABLE, item, schema)
    return resolved


def rule_sql_table_refs(source: str, findings: list, *, default_lakehouse=None,
                        catalog=None, aidp_catalog=DEFAULT_CATALOG,
                        scope=None, python_escaped: bool = False) -> str:
    """NB15: resolve bare table references in a Spark-SQL cell body.

    Runs on a masked copy so a name inside a string literal or comment is never
    matched.

    `python_escaped` says the body was read out of a Python string literal,
    where a line break is spelt `\\n` -- see `python_text.sql_scan_text`.
    Only the scan is decoded; the rewrite goes back into `source` with its
    escapes as written, which works because the decoding preserves every
    offset.

    The masking takes the shared module's default, which is Spark's reading of
    `"..."`: a string literal, because that is what a `%%sql` cell runs on.
    When the T-SQL reading leaked in here -- `"..."` as an identifier, so its
    body was visible -- this rule rewrote a table name *inside* a user's
    string:

      in:  SELECT "text FROM dbo.claim" AS c FROM dbo.claim
      out: SELECT "text FROM default.Sales.claim" AS c FROM default.Sales.claim

    reported as two ordinary NB10_TABLE_REF rewrites rather than one rewrite
    and one corrupted label.

    Two things after the keyword FROM are not tables and are skipped before
    resolution is attempted: a name a `WITH` clause in this body defines, and
    the second operand of `TRIM(... FROM ...)` / `EXTRACT(... FROM ...)` and
    the two other SQL functions whose argument list uses that keyword. Both
    resolved to nothing and were reported as NB12_TABLE_UNKNOWN -- a flag,
    on something that is not a table, which grades an otherwise clean
    notebook REVIEW. `sql_text` holds both tests because the T-SQL two-part
    rule anchors on FROM as well and had the same defect.

    `names_no_tables` and not `not catalog`: a plan whose resolution found
    nothing carries `{"summary": {}, "tables": {}}`, a truthy dict naming
    nothing, and the old test let it through. Every table reference in the
    estate then collected its own NB12_TABLE_UNKNOWN -- "we resolved nothing"
    reported as "this table does not exist", once per reference. `migrate`
    says it once per run instead, which is the only scope at which it is true.
    """
    if names_no_tables(catalog):
        return source
    scan = sql_scan_text(source) if python_escaped else source
    if scope is not None:
        # Before the first FROM is resolved, not after: a `USE` earlier in
        # the same body moves where a bare name lives. This is the one place
        # both SQL paths meet -- a `%%sql` cell and a `.sql()` string -- so
        # `USE other` written inside a query string is seen here even though
        # the Python mask blanks it everywhere else.
        _scan_database_switches(scan, findings,
                                default_lakehouse=default_lakehouse,
                                scope=scope)
    # Spark's reading, deliberately: see the docstring.
    view = masked(scan)
    defined_here = cte_names(view)
    replacements = []
    for match in _SQL_TABLE_RE.finditer(view):
        if _DATASOURCE_PATH_RE.match(view, match.end("name")):
            # `FROM delta.`<path>`` is Spark's read-by-path syntax, so
            # `delta` is a format name, not a table. The clause pattern
            # stops at the backquote, so it read `delta` as the whole name
            # and NB12 reported a table that does not exist -- 1 spurious
            # flag per path read. `rule_sql_paths` handles the path itself.
            continue
        if in_a_from_taking_call(view, match.start("kw")):
            # `TRIM(BOTH ' ' FROM name)`: the FROM belongs to the call and
            # `name` is a column.
            continue
        name = re.sub(r"\s*\.\s*", ".", source[match.start("name"):match.end("name")])
        if "." not in name and name.strip("`\"[]").casefold() in defined_here:
            # A CTE this body declares. It names a result set that exists
            # only for this statement, so there is nothing to resolve and
            # nothing to report: the reference is already correct.
            continue
        resolved = _resolved_table_name(name, findings, default_lakehouse,
                                        catalog, aidp_catalog, scope)
        if resolved is not None:
            replacements.append((match.start("name"), match.end("name"), resolved))
    return apply_replacements(source, replacements)


@dataclass(frozen=True)
class _EmittedBucket:
    """One lakehouse bucket name a path rewrite produced, and where from."""
    item: str
    bucket: str
    workspace: str
    spelling: str


def _record_bucket(emitted, location, value, mapped) -> None:
    """Note that `value` came out as a path in some lakehouse bucket.

    Collected rather than judged on the spot: whether two spellings of one
    lakehouse disagree is a property of the whole notebook, and each rule
    sees one literal. `emitted` is None when a rule is called on its own
    rather than through `translate`, and then nothing is collected -- there
    is no notebook to judge.
    """
    if emitted is None or location is None or not mapped:
        return
    bucket = mapped.split("://", 1)[-1].split("@", 1)[0]
    emitted.append(_EmittedBucket(location.item, bucket, location.workspace,
                                  value))


def _note_two_bucket_spellings(emitted, default_lakehouse, findings) -> None:
    """NB25: this notebook may spell one lakehouse two ways, or already does.

    `onelake_to_oci` qualifies a bucket by the workspace when the location
    writes one down and leaves it unqualified when nothing does -- and a
    Fabric export records the bound lakehouse's workspace nowhere, so the
    relative and FUSE forms can never be qualified. A notebook bound to
    `Sales` that also writes `abfss://ws@.../Sales.Lakehouse/...` can
    therefore reach one lakehouse under two bucket names.

    "Can" is the whole of the severity, and grading both shapes the same
    was the defect here:

      qualified path only          one bucket, nothing to reconcile   info
      unqualified path only        one bucket, and a qualified
                                   spelling of the same lakehouse
                                   anywhere else is on another        info
      qualified + relative/FUSE    two buckets for one file, and only
                                   one of them will exist             flag

    The second fails at run time and used to grade PASS. Only the paths
    the notebook actually produced can tell them apart, which is why this
    runs once at the end of `translate` over what `_record_bucket`
    collected rather than beside each rewrite.

    Two qualified paths naming one *item name* in two workspaces are two
    different lakehouses, and two buckets is the answer D1 exists to give,
    so they are not this: the flag needs an unqualified spelling opposite
    a qualified one.
    """
    if not emitted or not default_lakehouse:
        return
    item = str(default_lakehouse).strip()
    rows = [row for row in emitted if row.item.casefold() == item.casefold()]
    qualified = [row for row in rows if row.workspace]
    plain = [row for row in rows if not row.workspace]
    if not qualified:
        # MEASURED: this returned, so a notebook reaching its bound lakehouse
        # ONLY by `Files/...` or `/lakehouse/default/...` got no finding at
        # all, while the notebook reaching the same lakehouse by a qualified
        # URI got the `info` below -- which already tells its reader that
        # "any other artifact in this estate that does reach the same
        # lakehouse that way is on the other bucket". The warning existed and
        # only one of the two ends carried it, so whichever end a reviewer
        # opened decided whether they heard about the split at all.
        #
        # This is the mirror, not a new claim: same rule, same severity, same
        # single bucket emitted. Which of the two buckets is the right one
        # still cannot be decided here -- that needs the bound lakehouse's
        # workspace *name*, which the Fabric REST API carries and a Git
        # export does not record anywhere.
        if not plain:
            return
        plain_buckets = sorted({row.bucket for row in plain})
        findings.append(Finding(
            "NB25_LAKEHOUSE_TWO_BUCKETS",
            f"{', '.join(repr(row.spelling) for row in plain)} names no "
            f"workspace, so it resolves through this notebook's default "
            f"lakehouse {item!r} and maps to bucket "
            f"{', '.join(repr(b) for b in plain_buckets)}. A Fabric export "
            f"records the bound lakehouse's workspace nowhere, so this "
            f"spelling cannot be qualified and this is the only bucket this "
            f"notebook can emit. Worth knowing all the same: an "
            f"`abfss://<workspace>@.../{item}.Lakehouse/...` path to the "
            f"same lakehouse maps to bucket "
            f"'<workspace>_{item}_Lakehouse' instead. Any other artifact in "
            f"this estate that reaches this lakehouse that way is on that "
            f"other bucket, only one of the two will exist, and which one is "
            f"right cannot be decided from a Git export",
            "info"))
        return
    qualified_buckets = sorted({row.bucket for row in qualified})
    if plain:
        plain_buckets = sorted({row.bucket for row in plain})
        findings.append(Finding(
            "NB25_LAKEHOUSE_TWO_BUCKETS",
            f"this notebook emits {len(qualified_buckets) + len(plain_buckets)} "
            f"bucket names for one lakehouse, {item!r}: "
            f"{', '.join(repr(b) for b in qualified_buckets)} from "
            f"{', '.join(repr(row.spelling) for row in qualified)}, and "
            f"{', '.join(repr(b) for b in plain_buckets)} from "
            f"{', '.join(repr(row.spelling) for row in plain)}. A Fabric "
            f"export records the bound lakehouse's workspace nowhere, so the "
            f"second spelling cannot be qualified and the two cannot be made "
            f"to agree here. Only one of these buckets will exist, so one of "
            f"these reads fails at run time: pick the bucket this lakehouse "
            f"is migrated to and spell every path in this notebook the same "
            f"way before running it",
            "flag"))
        return
    findings.append(Finding(
        "NB25_LAKEHOUSE_TWO_BUCKETS",
        f"{', '.join(repr(row.spelling) for row in qualified)} names a "
        f"workspace as well as item {item!r}, which is this notebook's "
        f"default lakehouse, so it maps to bucket "
        f"{', '.join(repr(b) for b in qualified_buckets)}. Nothing else here "
        f"reaches that lakehouse, so this notebook emits one bucket and there "
        f"is nothing to reconcile in it. Worth knowing all the same: a "
        f"`Files/...` or `/lakehouse/default/...` path would map to bucket "
        f"{item!r} instead, because a Fabric export records the bound "
        f"lakehouse's workspace nowhere and the unqualified spelling cannot "
        f"be qualified. Any other artifact in this estate that does reach "
        f"the same lakehouse that way is on the other bucket",
        # `info`, not `flag`: one bucket is emitted and both rewrites are
        # applied, so nothing in this notebook is unresolved. The shape
        # that does fail -- both spellings in one notebook -- is the
        # branch above, and it flags. Raised to `flag` here this would
        # demote the commonest notebook shape in the bundled estate: 1 of
        # the 31 notebooks writes its own default lakehouse out longhand,
        # it has no relative path beside it, and it is the only NB01
        # rewrite there.
        "info"))


def _shortcut_entries(catalog) -> list:
    """Every shortcut the catalog knows, from both places it records them.

    `build_catalog` writes the whole list under `shortcuts`; the `Tables`
    half also lands in `tables` at the shortcut tier, because those are
    tables and table resolution needs them. Reading both means a catalog
    built by an older inventory -- and the hand-written ones in the tests
    -- still resolve.
    """
    if not isinstance(catalog, dict):
        return []
    out = [entry for entry in (catalog.get("shortcuts") or [])
           if isinstance(entry, dict)]
    tables = catalog.get("tables")
    if isinstance(tables, dict):
        out += [entry for entry in tables.values()
                if isinstance(entry, dict) and entry.get("tier") == TIER_SHORTCUT]
    return out


def _last_part(name) -> str:
    return str(name or "").strip().strip("`").rsplit(".", 1)[-1].casefold()


def _entry_schema(entry) -> str:
    """The schema segment of a shortcut's path, casefolded; "" if unrecorded.

    `build_catalog` keeps it as `schema` and, for a `Tables` shortcut, glues
    it into `table_name` (`hr.ext`); an older or hand-written catalog may
    carry only the glued form, or neither."""
    schema = str(entry.get("schema") or "").strip().strip("`")
    if not schema:
        glued = str(entry.get("table_name") or "").strip().strip("`")
        schema = glued.rsplit(".", 1)[0] if "." in glued else ""
    return schema.casefold()


def _shortcut_under(catalog, item, rest):
    """(entry, remainder) when `rest` points inside a shortcut, else (None, "").

    `rest` is the path under the Fabric item -- "/Files/claims_raw/part.parquet"
    -- so the first segment is the section and the second names the
    shortcut. A schema-enabled lakehouse puts a table shortcut at
    `Tables/<schema>/<name>`, so the two-segment name is tried first and
    the one-segment name second; more specific wins, exactly as it does in
    `candidates`.

    The owner and the section are constraints, not hints: a `Tables`
    shortcut called `x` and a real `Files/x` folder are different objects,
    and so are two lakehouses' shortcuts of the same name. Where the entry
    records neither -- an older catalog, or a hand-written one -- the name
    alone has to do, which is the behaviour that existed before this
    resolved anything at all.

    At depth 2 the schema segment is a constraint too. Two `Tables`
    shortcuts both named `ext`, one under `Tables/sales` and one under
    `Tables/hr`, are different data; comparing only the last segment sent
    `Tables/hr/ext` to whichever came first in the list -- measured:
    `oci://acme@ns/sales`, the sales shortcut, graded as an NB27 rewrite.
    """
    segments = [segment for segment in str(rest or "").split("/") if segment]
    if len(segments) < 2 or segments[0].casefold() not in ("files", "tables"):
        return (None, "")
    section, owner = segments[0].casefold(), str(item or "").strip().casefold()
    entries = _shortcut_entries(catalog)
    for depth in (2, 1):
        if len(segments) < 1 + depth:
            continue
        wanted = segments[depth].casefold()
        remainder = "/".join(segments[1 + depth:])
        for entry in entries:
            entry_section = str(entry.get("section") or "").casefold()
            if entry_section and entry_section != section:
                continue
            entry_owner = str(entry.get("lakehouse") or entry.get("owner")
                              or "").strip().casefold()
            if entry_owner and owner and entry_owner != owner:
                continue
            schema = _entry_schema(entry)
            if depth == 2 and schema and schema != segments[1].casefold():
                continue
            names = (entry.get("name"), entry.get("table_name"))
            if any(name and _last_part(name) == wanted for name in names):
                return (entry, remainder)
    return (None, "")


def _resolve_through_shortcut(value, catalog, item, rest, findings, *, namespace):
    """The oci:// URI for a path that lands inside a shortcut, or None.

    Returns the string to substitute, or None when `value` is not under a
    shortcut. A shortcut whose target cannot be named is *refused* here --
    the finding is appended and the empty string comes back -- because the
    lakehouse bucket is not where that data is and emitting it would be a
    rewrite pointing somewhere the bytes have never been.
    """
    entry, remainder = _shortcut_under(catalog, item, rest)
    if entry is None:
        return None
    name = entry.get("name") or entry.get("table_name") or "?"
    target = entry.get("target", "")
    oci_uri, reason = map_target(entry.get("target_type"), target,
                                 namespace=namespace,
                                 bucket=entry.get("bucket"))
    if not oci_uri:
        findings.append(Finding(
            "NB28_PATH_UNDER_SHORTCUT",
            f"{value!r} is inside shortcut {name!r}, whose data lives at "
            f"{target or '(no recorded target)'} and not in this lakehouse, so "
            f"it was left exactly as written: {reason}. Resolve the shortcut's "
            f"target by hand and point this read at it",
            "flag"))
        return ""
    mapped = oci_uri + (f"/{remainder}" if remainder else "")
    findings.append(Finding(
        "NB27_PATH_VIA_SHORTCUT",
        f"{value!r} -> {mapped!r}; {name!r} is a shortcut, so this data is at "
        f"the shortcut's own target ({target}) and never in the lakehouse "
        f"bucket. This tool does not copy data -- confirm the objects are at "
        f"the target before running this",
        "rewrite"))
    return mapped


# `delta`, `parquet`, `json`, ... followed by a backquoted path. Matched by
# shape rather than by a list of format names: the list grows with every
# Spark release and a wrong entry here costs a spurious NB12, while the
# shape `<word>.`<anything>`` is not a table reference under any reading.
_DATASOURCE_PATH_RE = re.compile(r"\s*\.\s*`")

# The SQL clauses whose next literal is a location. Anchored at the end so
# it only matches immediately before the quoted run being considered, and
# the `=` is optional because `OPTIONS (path '...')` writes none.
_SQL_LOCATION_CLAUSE_RE = re.compile(
    r"\b(?:LOCATION|INPATH|PATH|URL)\s*(?:=>|=)?\s*$", re.IGNORECASE)


def rule_sql_paths(source: str, findings: list, *, namespace,
                   default_lakehouse=None, guid_index=None, catalog=None,
                   emitted=None, python_escaped: bool = False) -> str:
    """NB01/NB02 for a OneLake URI written inside SQL.

    Python source has had this since NB01 shipped; SQL never did, in either
    of the two places SQL arrives. A `%%sql` cell went straight to the table
    rules, and a `spark.sql(...)` string was not read as SQL at all until
    D1. So:

      spark.sql("SELECT * FROM delta.`abfss://...onelake.../lh.Lakehouse/
                 Tables/claim`")          -> unchanged, no finding

    while the identical URI one line above, in `spark.read.load(...)`, came
    out as `oci://`. One notebook, one location, two answers.

    A URI in SQL is always inside a quoted run -- a literal for `LOCATION
    '...'`, a quoted identifier for ``delta.`...` `` -- so both are asked
    for by name and the *whole* quoted body has to be the path. That is
    what keeps this from reaching into a sentence: a literal that merely
    contains a URI is not one. Comments go first, so a URI in a `--` line
    is prose.

    Spark's dialect throughout, which is the reading the `%%sql` cell and
    the `.sql()` string both need: `"..."` is a string literal. An explicit
    `%%tsql` cell is not routed here; its `"..."` means the other thing and
    a path rule reading it wrongly would edit inside an identifier.

    The relative form needs more than that, and for the same reason it
    does in Python: `SELECT 'Tables/claim' AS label` is a whole quoted run
    that is not a path at all. A backquoted run is always the read-by-path
    syntax, and a literal counts only in a clause that takes a location.

    `python_escaped` says the body came out of a Python string literal, so
    the comment scan below has to know that a line break is spelt `\\n` --
    see `python_text.sql_scan_text`. A URI is read out of `source` all the
    same: the decoding preserves every offset, and a path holding one of
    those escapes is not a path.
    """
    if not namespace:
        return source
    scan = sql_scan_text(source) if python_escaped else source
    stripped = strip_sql_comments(scan)
    identifiers = set(quoted_identifier_spans(stripped))
    spans = sorted(string_literal_spans(stripped) + sorted(identifiers))
    replacements = []
    for start, end in spans:
        # An unterminated literal has no closer to step back over, and its
        # "body" runs to the end of the text. Rewriting inside one would be
        # rewriting a guess; `unterminated_span` is the rule that reports
        # the shape.
        if end - 1 <= start or source[end - 1] != source[start]:
            continue
        value = source[start + 1:end - 1]
        if not is_onelake_path(value):
            _note_azure_storage(value, findings)
            continue
        location = parse_onelake_path(value, default_lakehouse=default_lakehouse,
                                      guid_index=guid_index)
        if (location is not None and location.form == FORM_RELATIVE
                and (start, end) not in identifiers
                and not _SQL_LOCATION_CLAUSE_RE.search(stripped[:start])):
            findings.append(Finding(
                "NB26_RELATIVE_PATH_UNVERIFIED",
                _RELATIVE_PATH_UNVERIFIED.format(
                    value=value,
                    found="Here it is a SQL string literal outside any clause "
                          "that takes a location."),
                "flag"))
            continue
        if location is not None and not location.reason:
            through = _resolve_through_shortcut(
                value, catalog, location.item, location.rest, findings,
                namespace=namespace)
            if through is not None:
                if through:
                    replacements.append((start + 1, end - 1, through))
                continue
        mapping = map_onelake_path(value, namespace=namespace,
                                   default_lakehouse=default_lakehouse,
                                   guid_index=guid_index)
        if mapping.mapped is None:
            findings.append(Finding("NB02_ONELAKE_UNMAPPED",
                                    f"{value!r}: {mapping.reason}", "flag"))
            continue
        findings.append(Finding("NB01_ONELAKE_PATH",
                                f"{value!r} -> {mapping.mapped!r}", "rewrite"))
        _record_bucket(emitted, mapping.location, value, mapping.mapped)
        replacements.append((start + 1, end - 1, mapping.mapped))
    return apply_replacements(source, replacements)


def apply_sql_rules(source: str, findings: list, *, namespace=None,
                    default_lakehouse=None, guid_index=None, catalog=None,
                    aidp_catalog=DEFAULT_CATALOG, scope=None,
                    emitted=None, python_escaped: bool = False) -> str:
    """Every rule that applies to a body of Spark SQL, whatever carried it.

    The two carriers are a `%%sql` cell and the string argument of a
    `.sql()` call, and they have drifted apart twice now -- once on table
    names (D1) and once on OneLake URIs (D4). One entry point is the fix
    for both: a rule added here reaches both callers or neither.

    `python_escaped` is the one thing the two carriers do not share: a
    `%%sql` cell body is SQL, and a `.sql()` argument is the *source text*
    of a Python literal, where a line break is `\\n`. It is a property of
    the carrier, so it arrives from the caller rather than being guessed
    here.
    """
    source = rule_sql_paths(source, findings, namespace=namespace,
                            default_lakehouse=default_lakehouse,
                            guid_index=guid_index, catalog=catalog,
                            emitted=emitted, python_escaped=python_escaped)
    return rule_sql_table_refs(source, findings,
                               default_lakehouse=default_lakehouse,
                               catalog=catalog, aidp_catalog=aidp_catalog,
                               scope=scope, python_escaped=python_escaped)


# A dotted callee whose last attribute is `sql`: `spark.sql`, `ss.sql`,
# `self.spark.sql`. Matching the receiver rather than the literal name
# `spark` is deliberate -- binding the session to another name is ordinary
# Fabric notebook code (`ss = SparkSession.builder.getOrCreate()`), and the
# alternative, a list of blessed receiver names, is a list that is always out
# of date. The cost of matching too widely is bounded: the SQL rules only
# touch a name that follows FROM/JOIN/INTO/UPDATE *and* resolves in the
# catalog, so someone else's `.sql()` taking a non-SQL string comes out
# unchanged. `pd.read_sql(...)` cannot match at all -- `read_sql` has no dot
# before `sql`.
_SQL_CALL_RE = re.compile(
    r"(?<![\w.])[A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*\s*\.\s*sql\s*\(\s*"
    r"(?P<prefix>[rRbBuUfF]{0,2})(?P<q>'''|\"\"\"|'|\")")


def rule_sql_call_strings(source: str, findings: list, *,
                          default_lakehouse=None, catalog=None,
                          aidp_catalog=DEFAULT_CATALOG, scope=None,
                          namespace=None, guid_index=None, emitted=None,
                          view=None) -> str:
    """NB10 et al. inside the string argument of a `.sql()` call.

    `spark.sql("SELECT * FROM dbo.claim")` reads exactly the table
    `spark.table("dbo.claim")` reads. Only the second was resolved, so one
    notebook using both APIs shipped two names for one table and the
    unresolved one does not exist on AIDP. The body is handed to
    `rule_sql_table_refs` -- the same machinery a `%%sql` cell goes through
    -- rather than to a second set of patterns, so the two paths cannot
    drift apart the way they just did.

    The masking is two layers and they read the same text oppositely, which
    is the whole difficulty. The *Python* mask blanks every literal body, so
    a `spark.sql(...)` written inside someone's prose or comment is
    invisible here, exactly as it is to every other rule. But the literal
    this rule does match is the one place where a masked body is code: the
    SQL text is read back out of the real source and scanned again, this
    time with the *SQL* mask, which blanks what is a literal to Spark. So
    `note = "SELECT * FROM dbo.claim"` is untouched prose while
    `spark.sql("SELECT * FROM dbo.claim")` is rewritten, and inside the
    latter `'...'` and `"..."` are still the user's strings.

    An f-string argument holding a replacement field is not translated --
    a partial rewrite, `dbo` out of `dbo.{name}`, would leave one query
    naming its tables under two conventions -- but it is now *reported*,
    NB32. Declining is right and saying nothing was not: a reviewer reading
    the report had no way to learn that this notebook contains a query the
    tool never looked at. Measured before, with the demo catalog:

      spark.sql(f"SELECT * FROM {tbl}")   -> unchanged, findings: []

    An f-string with no `{` in it is an ordinary literal wearing an `f`
    -- its SQL is fully determined -- so it goes through the rules like
    any other, rather than collecting a flag that says its names are
    substituted at run time when none of them is. The test is a `{`
    rather than a parsed replacement field, so `f"SELECT {{1}}"`, where
    the brace is literal, is reported anyway: that errs towards the flag,
    which is the direction that cannot corrupt anything.

    A `b`-prefixed literal is not SQL text at all and `.sql()` would raise
    on it, so it is neither translated nor reported.

    The body handed on is the literal's *source text*, where a line break
    is the two characters `\\n`, so the SQL rules are told to decode those
    for scanning -- see `python_text.sql_scan_text`. Not for an `r"..."`
    literal, where those two characters are exactly what Spark receives.
    """
    if not catalog and not namespace:
        return source
    view = _view_of(source, view)
    # The literal is located by its token start rather than re-parsed out of
    # the match: `MaskedPython` already knows where every literal's value
    # begins and ends, including a triple-quoted one spanning lines, and a
    # second parser here could disagree with the mask.
    literal_at = {literal[0]: literal for literal in view.literals}
    replacements = []
    for match in _SQL_CALL_RE.finditer(view.text):
        literal = literal_at.get(match.start("prefix"))
        if literal is None:
            continue
        prefix = match.group("prefix").casefold()
        start, end = literal[2], literal[3]
        body = source[start:end]
        if not body.strip():
            continue
        if "b" in prefix:
            # Not SQL text at all -- `.sql()` would raise on it.
            continue
        if "f" in prefix and "{" in body:
            call = source[match.start():match.start("prefix")].strip()
            findings.append(Finding(
                "NB32_SQL_BUILT_AT_RUNTIME",
                f"{call}...) takes an f-string, so the query this runs is "
                f"not the text in this notebook -- "
                f"{_excerpt(body)} has a value substituted into it at run "
                f"time. None of the SQL rules ran on it: rewriting only the "
                f"names that are visible would leave one query naming its "
                f"tables under two conventions, and the tool cannot see "
                f"what the other names will be. Read this query and confirm "
                f"that every name it builds exists on AIDP under the "
                f"three-part name the plan gives it. Same contract as "
                f"NB02's f-string OneLake path",
                "flag"))
            continue
        rewritten = apply_sql_rules(
            body, findings, namespace=namespace, guid_index=guid_index,
            default_lakehouse=default_lakehouse, catalog=catalog,
            aidp_catalog=aidp_catalog, scope=scope, emitted=emitted,
            python_escaped="r" not in prefix)
        if rewritten != body:
            replacements.append((start, end, rewritten))
    return apply_replacements(source, replacements)


def rule_table_refs(source: str, findings: list, *, default_lakehouse=None,
                    catalog=None, aidp_catalog=DEFAULT_CATALOG,
                    scope=None, view=None) -> str:
    """NB10: resolve table references in Python call forms to three-part names.

    Matched on a copy with the string-literal bodies masked. The delimiters
    survive the mask, so `spark.table("dbo.claim")` still matches and the name
    is read back out of the real source; but a call quoted inside someone's
    prose -- `"we call spark.table('dbo.claim') in the docs"` -- is invisible,
    where it used to be rewritten.

    Every call shape lives in `_TABLE_REFERENCE_PATTERNS` and they are
    walked in order. The matches cannot overlap -- each pattern ends at the
    quoted name and no two of them start at the same place -- so the
    replacements are collected across all of them and applied once.

    `names_no_tables` and not `not catalog`, for the reason spelt out on
    `rule_sql_table_refs` above: an empty-but-present catalog is not the
    statement that a table does not exist.
    """
    if names_no_tables(catalog):
        return source
    view = _view_of(source, view)
    replacements = []
    for pattern in _TABLE_REFERENCE_PATTERNS:
        for match in pattern.finditer(view.text):
            name = source[match.start("name"):match.end("name")]
            resolved = _resolved_table_name(name, findings, default_lakehouse,
                                            catalog, aidp_catalog, scope)
            if resolved is not None:
                replacements.append((match.start("name"), match.end("name"),
                                     resolved))
    return apply_replacements(source, replacements)


# Calls that reach the *local* filesystem. `/lakehouse/default/...` is
# Fabric's FUSE mount and these read it; Spark reads the oci:// URI and they
# cannot, so rewriting a path they consume kills the notebook at run time.
#
# Matched on the last component of the dotted callee, which is enough to
# separate `pd.read_csv` from `spark.read.csv` and survives any import alias.
#
# Deliberately only the calls that *touch* the filesystem. `os.path.join`,
# `basename`, `dirname` and `splitext` manipulate a string and are not where
# an oci:// URI fails; a joined path is as likely to go on to Spark, and
# flagging it would be a guess. `str.replace` is off the list for the same
# reason and because the name is far too common.
_LOCAL_FILE_CALLS = frozenset({
    # builtins and the standard library
    "open", "Path", "PosixPath", "PurePath",
    "exists", "isfile", "isdir", "islink", "getsize", "getmtime",
    "listdir", "scandir", "walk", "makedirs", "mkdir", "removedirs",
    "remove", "unlink", "rmdir", "rename", "stat", "chmod",
    "glob", "iglob",
    "copy", "copy2", "copyfile", "copytree", "move", "rmtree",
    "unpack_archive", "make_archive",
    # pandas, which every Fabric notebook reaches for. Spelt in full because
    # `spark.read.csv` shares the short name `csv` and must not match.
    "read_csv", "read_parquet", "read_json", "read_excel", "read_table",
    "read_feather", "read_orc", "read_pickle", "read_html", "read_fwf",
    "read_sas", "read_stata", "read_xml",
    "to_csv", "to_parquet", "to_json", "to_excel", "to_pickle", "to_feather",
})
_DOTTED_NAME_RE = re.compile(r"[A-Za-z_]\w*(?:\s*\.\s*[A-Za-z_]\w*)*\s*$")


def _enclosing_call(text: str, index: int):
    """The dotted callee of the innermost call still open at `index`, or None.

    `text` must be a masked view: a bracket inside a literal or a comment is
    blanked there, so only real brackets are counted.
    """
    depth = 0
    for position in range(index - 1, -1, -1):
        char = text[position]
        if char in ")]}":
            depth += 1
        elif char in "([{":
            if depth:
                depth -= 1
                continue
            if char != "(":
                return None          # a list or dict display, not a call
            match = _DOTTED_NAME_RE.search(text[:position])
            return re.sub(r"\s+", "", match.group(0)) if match else None
    return None


def _is_local_file_call(callee) -> bool:
    return bool(callee) and callee.rsplit(".", 1)[-1] in _LOCAL_FILE_CALLS


# Calls whose quoted argument is an *object-store path*, which is the one
# thing that can make `"Files/raw/x.csv"` a OneLake location rather than a
# string that starts with the same seven characters. Only the relative form
# needs this: `abfss://...` and `/lakehouse/default/...` are paths under
# every reading, so nothing has to vouch for them.
#
# Names that mean one thing only. `load`, `save` and the format shorthands
# are DataFrameReader/DataFrameWriter terminals; `textFile` and friends are
# SparkContext's. Deliberately not here: `table` and `saveAsTable` (a table
# name, resolved by NB10), `format` and `option` (neither argument is a
# path), and `head`, `exists` and `copy`, which collide with `df.head()`,
# `os.path.exists` and `shutil.copy` -- those reach OneLake only through
# the `fs.` surface below, where the receiver disambiguates them.
_SPARK_PATH_CALLS = frozenset({
    "load", "save",
    "parquet", "csv", "json", "orc", "text", "avro",
    "textFile", "wholeTextFiles", "binaryFiles", "objectFile",
    "forPath", "refreshByPath",
})
# `notebookutils.fs` / `mssparkutils.fs`: Fabric's own filesystem surface,
# and the one place a relative `Files/...` is documented to resolve against
# the default lakehouse. Matched on the receiver rather than the method so
# `fs.exists("Files/x")` counts and `os.path.exists("Files/x")` does not --
# the second is a local path relative to the process working directory,
# which is not OneLake at all.
_FS_RECEIVER_RE = re.compile(r"(?:^|\.)fs\.[A-Za-z_]\w*$")


def _is_path_argument_call(callee) -> bool:
    """Whether `callee`'s quoted argument is an object-store path."""
    if not callee:
        return False
    return (callee.rsplit(".", 1)[-1] in _SPARK_PATH_CALLS
            or bool(_FS_RECEIVER_RE.search(callee)))


_RELATIVE_PATH_UNVERIFIED = (
    "{value!r} was left as written. It starts with `Files/` or `Tables/`, but "
    "that prefix is not evidence: a relative filesystem path, a dictionary "
    "key, a label and a log message all spell it the same way, and rewriting "
    "one to an oci:// URI corrupts it. {found} A call that takes an "
    "object-store path is what says otherwise -- spark.read/write "
    "(load, save, parquet, csv, json, orc, text), SparkContext.textFile, "
    "DeltaTable.forPath, or notebookutils.fs / mssparkutils.fs. Spell the "
    "location in full -- abfss://<workspace>@onelake.dfs.fabric.microsoft.com/"
    "<item>.<type>/... -- or map it by hand")


# A Spark read-by-path call whose whole argument is one quoted run. The
# path is no longer spelled out here: every OneLake spelling of
# `<item>/Tables/<table>` is the same location and `parse_onelake_path` is
# the one thing that knows them all, so the pattern asks for the call
# shape and the location is read out of the literal.
_TABLES_PATH_RE = re.compile(
    r"(?:spark\.read\.load|spark\.read\.parquet|spark\.read\.format\([^)]*\)\.load)"
    r"\s*\(\s*(?P<q>['\"])(?P<name>[^'\"]+)(?P=q)\s*\)")
_SQL_MAGIC_RE = re.compile(r"^\s*%%t?sql\b", re.IGNORECASE)
_TSQL_MAGIC_RE = re.compile(r"^\s*%%tsql\b", re.IGNORECASE)
# The cell languages that decide the dialect when no magic line does --
# a `notebook-content.sql` item, whose cells are bare SQL. Subsets of
# `SQL_LANGUAGES`, which is the routing test; these two pick the dialect
# inside it. Plain `sql` is in neither: in a `.sql` notebook it is T-SQL and
# the `sql_source` test below covers that, and anywhere else it is
# ambiguous enough to be worth flagging rather than rewriting.
_TSQL_CELL_LANGUAGES = frozenset({"tsql", "t-sql"})
_SPARK_SQL_CELL_LANGUAGES = frozenset({"sparksql"})
_REDUNDANT_MAGIC_RE = re.compile(r"^(\s*)%%(?:pyspark|python)\b[^\n]*\n?",
                                 re.IGNORECASE | re.MULTILINE)
_FLAGGED_MAGICS = {
    "configure": "%%configure sets cluster resources; on AIDP compute is provisioned "
                 "outside the notebook, so this has no effect and must be applied to "
                 "the cluster instead",
    "spark": "%%spark runs Scala; port the cell to PySpark or a Scala job",
    "csharp": "%%csharp has no AIDP equivalent; port the cell",
    "r": "%%r has no AIDP equivalent in this toolchain; port the cell",
    "sparkr": "%%sparkr has no AIDP equivalent in this toolchain; port the cell",
}
_MAGIC_LINE_RE = re.compile(r"^\s*%%(?P<name>[A-Za-z_]+)\b", re.MULTILINE)
_RUN_LINE_RE = re.compile(r"^\s*%run\s+(?P<target>\S.*?)\s*$", re.MULTILINE)


def _table_under_tables(rest, catalog):
    """(schema, table) when `rest` names a table, else None.

    `rest` is the path under the Fabric item. A Lakehouse keeps its tables
    at `Tables/<table>`, and a schema-enabled one at
    `Tables/<schema>/<table>` -- the layout that used to be flattened into
    `oci://.../Tables/dbo/claim`, with `dbo` read as a folder.

    Two segments are ambiguous by shape alone: `Tables/ledger/part-0.parquet`
    is a file inside one table and `Tables/sales/claim` is a table inside
    one schema, and nothing in the path separates them. The catalog can,
    where it knows the first segment, and the `_` prefix Delta and Spark
    reserve for their own directories (`_delta_log`, `_temporary`,
    `_SUCCESS`) settles the rest. Neither is certain; both are better than
    reading every second segment as a schema.
    """
    segments = [segment for segment in str(rest or "").split("/") if segment]
    if len(segments) < 2 or segments[0].casefold() != "tables":
        return None
    if len(segments) == 2:
        return ("", segments[1])
    if len(segments) > 3:
        return None          # deeper than any table name goes: a path
    schema, table = segments[1], segments[2]
    if table.startswith("_") or "." in table:
        # A Delta/Spark metadata directory, or a file with an extension.
        return None
    if catalog and candidates(catalog, f"{schema}.{table}"):
        return (schema, table)
    if catalog and candidates(catalog, schema):
        # The first segment is itself a table, so the second is inside its
        # storage rather than beside it under a schema.
        return None
    return (schema, table)


def rule_tables_path(source: str, findings: list, *, default_lakehouse=None,
                     catalog=None, aidp_catalog=DEFAULT_CATALOG,
                     guid_index=None, view=None) -> str:
    """NB20: `<item>/Tables/<table>` is a table, not an object-storage path.

    AIDP reaches a table through the catalog, so this becomes a three-part name
    rather than an oci:// URI.

    This rule was unreachable until the `notebook-paths` batch. It ran
    *after* `_rewrite_paths` in the per-line loop, and that rule had
    already turned the literal into `oci://...`, so the pattern -- which
    asked for the FUSE spelling in full -- could never match. Measured on
    this tree before the fix: `spark.read.load("/lakehouse/default/Tables/
    claim")` came out as `spark.read.load("oci://Sales@ns/Tables/claim")`
    with one NB01 finding, while `rule_tables_path` called on the same
    line on its own returned `spark.table("default.Sales.claim")` with
    NB20. The rule was right and never ran, so it was made reachable
    rather than rewritten: it goes first in the loop now, and it reads the
    location through `parse_onelake_path`, which covers the abfss and
    relative spellings the old pattern could not see.

    A shortcut is deliberately left alone here even though it is a table:
    its data is at the shortcut's own target, which the path rule resolves
    (NB27), and a three-part name would send the read to a lakehouse that
    does not hold it.

    The path is legitimately inside a literal, so this reads the real source --
    but only accepts a match whose quoted run *is* a literal's value. Inside
    `"we call spark.read.load('/lakehouse/default/Tables/claim')"` it is not,
    and the old scan rewrote the user's prose into a spark.table() call.
    """
    view = _view_of(source, view)

    def replace(match):
        if not view.is_literal_value(match.start("q") + 1, match.end("name")):
            return match.group(0)
        value = match.group("name")
        location = parse_onelake_path(value, default_lakehouse=default_lakehouse,
                                      guid_index=guid_index)
        if location is None:
            return match.group(0)
        if location.reason:
            # The only way here is a FUSE or relative path with no binding:
            # the item cannot be named, so neither can the table.
            if _table_under_tables(location.rest, catalog) is None:
                return match.group(0)
            findings.append(Finding(
                "NB20_TABLES_PATH_UNBOUND",
                f"{value} refers to a table, but this notebook has no default "
                f"lakehouse binding to resolve it against",
                "flag"))
            return match.group(0)
        if _shortcut_under(catalog, location.item, location.rest)[0] is not None:
            return match.group(0)
        parts = _table_under_tables(location.rest, catalog)
        if parts is None:
            return match.group(0)
        schema, table = parts
        resolved = aidp_table(location.item, table, schema=schema,
                              catalog=aidp_catalog)
        findings.append(Finding(
            "NB20_TABLES_PATH",
            f"{value} -> spark.table({resolved!r}); it is a table, reached "
            f"through the AIDP catalog rather than as a path",
            "rewrite"))
        note_unaddressable_schema(findings, SCHEMA_NOT_ADDRESSABLE,
                                  location.item, schema)
        quote = match.group("q")
        return f"spark.table({quote}{resolved}{quote})"

    return _TABLES_PATH_RE.sub(replace, source)


def rule_magics(source: str, findings: list, *, default_lakehouse=None,
                catalog=None, view=None) -> str:
    """NB21: handle Fabric cell magics.

    Scanned on the masked copy. A `%%python` or `%run` line inside a
    triple-quoted docstring is text, not a magic: the cell is processed a line
    at a time, so the old raw scan stripped the `%%python` out of a user's
    string and flagged the `%run` as a dependency.
    """
    view = _view_of(source, view)
    result = source
    for match in _MAGIC_LINE_RE.finditer(view.text):
        name = match.group("name").casefold()
        if name in _FLAGGED_MAGICS:
            findings.append(Finding(
                f"NB21_MAGIC_{name.upper()}", _FLAGGED_MAGICS[name], "flag"))
    redundant = [(match.start(), match.end(), match.group(1))
                 for match in _REDUNDANT_MAGIC_RE.finditer(view.text)]
    if redundant:
        findings.append(Finding(
            "NB21_MAGIC_REDUNDANT",
            "%%pyspark / %%python removed; Python is the default on AIDP",
            "rewrite"))
        result = apply_replacements(source, redundant)
    for match in _RUN_LINE_RE.finditer(view.text):
        target = source[match.start("target"):match.end("target")]
        findings.append(Finding(
            "NB21_RUN_MAGIC",
            f"%run {target} has no AIDP equivalent; the dependency is "
            f"recorded in the plan, but the inline execution must be replaced",
            "flag"))
    return result


def rule_sempy(line: str, findings: list, view=None) -> str:
    """NB30: a Semantic Link import, left exactly as written.

    `sempy` / `sempy.fabric` is Fabric-only. It reads semantic models over
    the Power BI XMLA endpoint and calls the Fabric REST API, and neither
    exists on AIDP -- the import itself raises there, so the notebook dies
    on the first cell that carries one and every cell after it is never
    reached. Same class as NB03, and flagged the same way: left in place,
    named, and handed to a human.

    The import statement is the anchor rather than the usage sites, which is
    where this differs from NB03. `notebookutils` is injected into the
    notebook namespace with no import at all, so there may be no statement
    to point at and the uses have to be hunted; `sempy` is an ordinary
    library and cannot be reached without one.

    Scanned on the masked copy, so `sempy` named in a docstring or a comment
    is prose. Nothing is rewritten: there is no AIDP equivalent to rewrite
    to, and commenting the import out would only move the failure from
    ImportError to NameError further down the same cell.
    """
    view = _view_of(line, view)
    if _SEMPY_IMPORT_RE.match(view.text):
        findings.append(Finding(
            "NB30_SEMPY",
            f"{line.strip()!r} imports Semantic Link (sempy), which is "
            f"Fabric-only: it reads semantic models through the Power BI "
            f"XMLA endpoint and calls the Fabric REST API, and on AIDP the "
            f"import fails outright. Left exactly as written -- there is no "
            f"AIDP equivalent. Replace the model read with a read of the "
            f"migrated tables, or run the extract on Fabric and land the "
            f"result where this notebook can reach it",
            "flag"))
    return line


def _rewrite_paths(line: str, findings: list, *, namespace, default_lakehouse,
                   guid_index, catalog=None, emitted=None, view=None) -> str:
    """NB01/NB02: a OneLake path literal becomes an oci:// URI.

    Only a run the tokenizer calls a literal value is considered. The old
    regex scan resynchronised after an escaped quote, so in

      x = 'it\\'s "/lakehouse/default/Files/x.csv"'

    it read the inner quoted run as a literal of its own and rewrote inside
    the user's string.
    """
    view = _view_of(line, view)
    replacements = []
    for match in _STRING_RE.finditer(line):
        if not view.is_literal_value(match.start("val"), match.end("val")):
            continue
        prefix, quote, value = (match.group("prefix"), match.group("q"),
                                match.group("val"))
        if not is_onelake_path(value):
            _note_azure_storage(value, findings)
            continue
        callee = _enclosing_call(view.text, match.start())
        if is_fuse_path(value) and _is_local_file_call(callee):
            findings.append(Finding(
                "NB09_FUSE_LOCAL_API",
                f"{value!r} is Fabric's FUSE mount and {callee}() reads the "
                f"local filesystem, which cannot open an oci:// URI -- so "
                f"this was left as written rather than rewritten. There is "
                f"no correct substitute: AIDP may have no FUSE mount at all. "
                f"Stage the file locally, or move the read to Spark. "
                f"Detection is lexical and one line wide -- it reads the "
                f"call enclosing the literal -- so a path bound to a name "
                f"here and opened later is still rewritten, and a path that "
                f"passes through {callee}() on its way to Spark is flagged "
                f"when it need not be",
                "flag"))
            continue
        location = parse_onelake_path(value, default_lakehouse=default_lakehouse,
                                      guid_index=guid_index)
        if (location is not None and location.form == FORM_RELATIVE
                and not _is_path_argument_call(callee)):
            findings.append(Finding(
                "NB26_RELATIVE_PATH_UNVERIFIED",
                _RELATIVE_PATH_UNVERIFIED.format(
                    value=value,
                    found=(f"Here it is an argument of {callee}(), which is "
                           f"not such a call." if callee
                           else "Here no call encloses it.")),
                "flag"))
            continue
        if location is not None and not location.reason and "{" not in value:
            through = _resolve_through_shortcut(
                value, catalog, location.item, location.rest, findings,
                namespace=namespace)
            if through is not None:
                if through:
                    replacements.append((match.start(), match.end(),
                                         f"{prefix}{quote}{through}{quote}"))
                continue
        if "f" in prefix.lower() and "{" in value:
            findings.append(Finding(
                "NB02_ONELAKE_UNMAPPED",
                f"f-string OneLake path {value!r} is built at runtime; map it by hand",
                "flag"))
            continue
        mapping = map_onelake_path(value, namespace=namespace,
                                   default_lakehouse=default_lakehouse,
                                   guid_index=guid_index)
        if mapping.mapped is None:
            findings.append(Finding("NB02_ONELAKE_UNMAPPED",
                                    f"{value!r}: {mapping.reason}", "flag"))
            continue
        findings.append(Finding("NB01_ONELAKE_PATH",
                                f"{value!r} -> {mapping.mapped!r}", "rewrite"))
        if is_fuse_path(value) and not _is_path_argument_call(callee):
            # NB09's detection is lexical and one line wide, and its own
            # caveat says so -- inside the NB09 finding, which is only
            # raised when the detection *succeeded*. So the reader who
            # needs that sentence is precisely the one who never sees it.
            # Measured on this tree, bound to SalesLake:
            #
            #   with open("/lakehouse/default/Files/x.csv") as fh:
            #     -> unchanged, NB09 flag, caveat and all
            #   p = "/lakehouse/default/Files/x.csv"
            #   with open(p) as fh:
            #     -> p = "oci://SalesLake@demons/Files/x.csv"
            #        NB01_ONELAKE_PATH rewrite, and nothing else
            #
            # The second is a notebook that dies on the first read, and
            # the report calls it a clean rewrite. The FUSE mount is the
            # spelling a Fabric notebook uses precisely so that `open` and
            # pandas can reach OneLake, so an unattributed one is more
            # likely headed for a local API than for Spark -- which is why
            # this is a flag and not an info.
            findings.append(Finding(
                "NB33_FUSE_PATH_CONSUMER_UNKNOWN",
                f"{value!r} is Fabric's FUSE mount and was rewritten to "
                f"{mapping.mapped!r}, but nothing on this line says what "
                f"reads it: "
                + (f"it is an argument of {callee}(), which is not one of "
                   f"the calls that take an object-store path"
                   if callee else "no call encloses it")
                + f". Spark can read the oci:// URI and the local "
                f"filesystem cannot, so if this path reaches `open`, "
                f"pandas or `os` further down -- which is what the FUSE "
                f"mount exists for -- the rewrite is what breaks it, and "
                f"NB09 cannot see that from one line. Follow the value to "
                f"its reader: if Spark, this is correct as it stands; if a "
                f"local API, put the path back and stage the file instead",
                "flag"))
        _record_bucket(emitted, mapping.location, value, mapping.mapped)
        replacements.append((match.start(), match.end(),
                             f"{prefix}{quote}{mapping.mapped}{quote}"))
    return apply_replacements(line, replacements)


def _closing_paren(text: str, index: int):
    """Index just past the `)` matching the `(` at `index`, or None.

    Balanced on a masked view, where a bracket inside a literal or a comment
    is already blanked, so only real brackets are counted.
    """
    depth = 0
    for position in range(index, len(text)):
        if text[position] in "([{":
            depth += 1
        elif text[position] in ")]}":
            depth -= 1
            if depth == 0:
                return position + 1
    return None


def _note_display(line: str, findings: list, view=None) -> int:
    """NB04: record every `display()` call. Returns how many there were.

    The call is left exactly as written; `DISPLAY_SHIM` explains why, and
    NB08 records that the shim went in. Scanned on the masked copy, so
    `note = "we call display(x) in the docs"` is prose and stays prose.
    """
    view = _view_of(line, view)
    calls = 0
    for match in _DISPLAY_CALL_RE.finditer(view.text):
        if match.group("def"):
            continue            # `def display(...)`: the user's own, not a call
        calls += 1
        end = _closing_paren(view.text, match.end() - 1)
        # A call split across lines has no closing paren on this one. The
        # rules run per line, so name what is visible rather than guess.
        call = line[match.start():end] if end else "display(...)"
        findings.append(Finding(
            "NB04_DISPLAY",
            f"{call} left as written; Fabric's display() renders a Spark "
            f"DataFrame, a pandas DataFrame and more, so it is translated by "
            f"the shim NB08 adds rather than by a call-site rewrite",
            "rewrite"))
    return calls




# Constructs that are T-SQL and are either invalid in Spark SQL or mean
# something else there. Deliberately narrow: `%%sql` in a Fabric notebook is
# Spark SQL, so anything ambiguous stays off this list.
_TSQL_ONLY = (
    (re.compile(r"\bSELECT\s+TOP\s+\(?\s*\d+", re.I), "SELECT TOP n"),
    (re.compile(r"\bGETDATE\s*\(", re.I), "GETDATE()"),
    (re.compile(r"\bISNULL\s*\(", re.I), "ISNULL()"),
    (re.compile(r"\bCHARINDEX\s*\(", re.I), "CHARINDEX()"),
    (re.compile(r"\bCONVERT\s*\(", re.I), "CONVERT()"),
    (re.compile(r"\bDATEADD\s*\(", re.I), "DATEADD()"),
    (re.compile(r"\bDATEDIFF\s*\(\s*(?:year|quarter|month|week|day|hour|"
                r"minute|second|yy|qq|mm|wk|dd|hh|mi|ss)\b", re.I),
     "DATEDIFF(datepart, ...)"),
    (re.compile(r"\bINTO\s+[\w.\[\]`]+\s+FROM\b", re.I), "SELECT ... INTO"),
    (re.compile(r"\b(?:NVARCHAR|DATETIME2|UNIQUEIDENTIFIER|MONEY)\b", re.I),
     "a T-SQL-only type"),
    (re.compile(r"(?<![\w`\])])\[[A-Za-z_][\w ]*\]"), "[bracketed identifier]"),
)


def _flag_tsql_in_spark_sql(body: str, findings: list) -> None:
    """Flag T-SQL constructs in a `%%sql` cell without rewriting anything.

    `%%sql` in a Fabric notebook is **Spark SQL**, so the T-SQL rules are the
    wrong tool here. Measured, on this rule set: they turn `arr[0]` into
    ``arr`0` `` which Spark rejects, and rewrite `lh.silver.orders` to
    `default.silver.orders` -- a different table, which Spark accepts. Both
    are worse than the problem.

    Verifying the dialect properly needs a live Spark to parse against, which
    this tool deliberately does not have. So it reports what it recognises and
    leaves the text alone. An explicit `%%tsql` cell is translated; see
    `_TSQL_MAGIC_RE`.
    """
    # A `%%sql` cell is Spark dialect, so `"..."` is a string literal and its
    # body is masked -- the shared module's default. A `%%tsql` cell goes to
    # the T-SQL translator, which asks for the other reading.
    view = masked(body)
    for pattern, label in _TSQL_ONLY:
        if pattern.search(view):
            findings.append(Finding(
                "NB24_TSQL_IN_SQL_CELL",
                f"`%%sql` cell uses {label}, which is T-SQL; Fabric notebook "
                f"SQL runs on Spark, so this was left as written and needs a "
                f"human -- rerun the cell as `%%tsql` to have it translated",
                "flag"))


def _translate_sql_body(body: str, findings: list, *, is_tsql: bool,
                        aidp_catalog=DEFAULT_CATALOG,
                        magic_line: bool = True) -> str:
    """Translate a T-SQL body; only flag a Spark-SQL one.

    `magic_line` says whether the first non-blank line is the cell's
    `%%sql` / `%%tsql` declaration, which is held back from the translator
    and re-attached unchanged. A T-SQL notebook (`notebook-content.sql`)
    has no such line -- its cells are bare SQL -- and holding the first
    line back there would take a line of the query out of the translation
    and leave it untranslated in the middle of the result.
    """
    lines = body.split("\n")
    for index, line in enumerate(lines):
        if line.strip():
            break
    else:
        return body
    if magic_line:
        head, sql = lines[:index + 1], "\n".join(lines[index + 1:])
    else:
        head, sql = [], body
    if not sql.strip():
        return body
    if not is_tsql:
        _flag_tsql_in_spark_sql(sql, findings)
        return body
    # No `item`, and no three-part object resolution: every table name in a
    # `%%tsql` cell has already been through the catalog, in NB15 above,
    # which knows more than a name shape can. Where NB15 *declined*, SQ11
    # rewrote the name anyway. Measured, `%%tsql` bound to SalesLake with
    # `W3.dbo.thing` matching two catalog entries:
    #
    #   NB20_TABLE_AMBIGUOUS  flag     "...it was left exactly as written"
    #   SQ11_THREE_PART_NAME  rewrite  'W3.dbo.thing' -> 'default.W3.thing'
    #
    # -- a finding contradicting the artifact printed beside it, and a
    # confident three-part name for a reference the catalog says is
    # ambiguous. The same cell spelt `%%sql` left it alone, correctly.
    result = tsql.translate(sql, kind="other", catalog=aidp_catalog,
                            resolve_table_names=False)
    findings.extend(result.findings)
    return "\n".join(head + result.translated_sql.split("\n"))




def _unwrap_magic_cell(block, notebook, findings) -> None:
    """Strip Fabric's `# MAGIC ` prefix and route the cell by its language.

    SQL is unwrapped so the SQL rules can reach it. Python is unwrapped so the
    Python rules can. Anything else is left wrapped and refused by name --
    passing an R or Scala body through as inert commentary would report a
    clean migration of a cell that does nothing.
    """
    language = None
    if hasattr(notebook, "language_for"):
        language = notebook.language_for(block)
    body = block.magic_body
    first = next((ln for ln in body.split("\n") if ln.strip()), "")
    declared = _CELL_MAGIC_RE.match(first)
    magic_name = declared.group(1).casefold() if declared else None

    if magic_name in _MAGIC_SQL_LANGUAGES or language in _MAGIC_SQL_LANGUAGES:
        block.lines = body.split("\n")
        findings.append(Finding(
            "NB22_MAGIC_CELL",
            f"`# MAGIC` {language or magic_name or 'sql'} cell unwrapped so the "
            f"SQL rules apply; it was previously passed through as comments",
            "rewrite"))
        return
    if magic_name in _MAGIC_PYTHON_LANGUAGES or language in _MAGIC_PYTHON_LANGUAGES:
        # Unwrapped only. This used to drop every line matching `%%<name>`,
        # which is right for `%%pyspark`/`%%python` -- a redundant language
        # declaration -- and wrong for everything else, and the two are only
        # distinguishable by name. Fabric records a `%%configure` or `%%html`
        # cell with `"language": "python"` in its METADATA, so this branch is
        # what such a cell reaches. Measured on `"language": "python"` cells,
        # before:
        #
        #   %%configure / {"driverMemory": "8g"}  ->  {"driverMemory": "8g"}
        #       findings [NB22]. The cluster sizing is gone, what is left is a
        #       dict literal that evaluates and does nothing, and
        #       NB21_MAGIC_CONFIGURE -- the rule written for exactly this --
        #       never fires, because the line it matches was already deleted.
        #   %%html / <b>hi</b>                    ->  <b>hi</b>
        #       findings [NB22]. Inert markup became a Python SyntaxError:
        #       the cell was harmless before the migration and fatal after.
        #
        # `rule_magics` removes `%%pyspark`/`%%python` further down, with
        # NB21_MAGIC_REDUNDANT to say it did, and flags the rest by name. It
        # is the one place that knows which magics are which, so the decision
        # is left there rather than taken twice.
        block.lines = body.split("\n")
        findings.append(Finding(
            "NB22_MAGIC_CELL",
            f"`# MAGIC` {language or magic_name} cell unwrapped to plain Python",
            "rewrite"))
        return
    findings.append(Finding(
        "NB23_MAGIC_LANGUAGE",
        f"`# MAGIC` cell declares language "
        f"{language or magic_name or '<unknown>'!r}, which this tool does not "
        f"translate; left as written for a human",
        "flag"))


@dataclass(frozen=True)
class _UtilsImport:
    """One notebookutils import statement, as read off the cell."""

    span: int              # lines it occupies, so a caller can skip them
    text: str              # the statement, whitespace collapsed
    bound: dict            # bound name -> the notebookutils path it names
    refusal: str = ""      # non-empty: leave it as written and say why
    unenumerable: bool = False   # commented out, but its names are unknown


def _statement_end(masked_lines, index: int) -> int:
    """The line index just past the statement starting at `masked_lines[index]`.

    Bracket depth and backslash continuations, counted on a masked view --
    where a bracket inside a literal or a comment is already blanked, so only
    real ones are seen. A parenthesised import is one statement over several
    lines, and commenting out only its first line left `fs,` and `)` behind.
    """
    span, depth = 0, 0
    while index + span < len(masked_lines):
        text = masked_lines[index + span]
        depth += text.count("(") - text.count(")")
        span += 1
        if depth <= 0 and not text.rstrip().endswith("\\"):
            break
    return index + span


def _utils_import(lines, views, index):
    """The notebookutils import starting at `lines[index]`, or None.

    Fabric notebooks use every shape there is: `import notebookutils`,
    `import notebookutils as nu`, `from notebookutils import fs`,
    `from notebookutils import fs as f`, `from notebookutils.mssparkutils
    import fs`, and the parenthesised form over several lines. Commenting out
    only the first line of that last one left `fs,` and `)` behind, which is
    a SyntaxError -- so the statement is read whole, by paren depth.

    The head is matched on the masked view, so a line inside a docstring is
    not an import. The bound names then come from `ast`, which is the only
    thing that gets `import a.b as c` right.
    """
    if not _UTILS_IMPORT_HEAD_RE.match(views[index].text):
        return None
    span = _statement_end([view.text for view in views], index) - index
    statement = "\n".join(lines[index:index + span])
    collapsed = " ".join(statement.split())
    try:
        node = ast.parse(statement.lstrip()).body[0]
    except (SyntaxError, IndexError, ValueError) as exc:
        # The head said import and the statement will not parse. Refusing is
        # the only safe answer: commenting out lines chosen by paren counting
        # could cut a statement in half.
        return _UtilsImport(span, collapsed, {},
                            refusal=f"it does not parse ({exc})")

    if isinstance(node, ast.Import):
        bound, others = {}, []
        for alias in node.names:
            root = alias.name.split(".")[0]
            if root in _UTILS_MODULES:
                bound[alias.asname or root] = root
            else:
                others.append(alias.asname or alias.name)
        if others:
            return _UtilsImport(
                span, collapsed, bound,
                refusal=f"it also binds {', '.join(sorted(others))}, which "
                        f"commenting the line out would take with it")
        return _UtilsImport(span, collapsed, bound)

    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        if module.split(".")[0] not in _UTILS_MODULES:
            return None
        bound = {}
        for alias in node.names:
            if alias.name == "*":
                return _UtilsImport(span, collapsed, {}, unenumerable=True)
            bound[alias.asname or alias.name] = f"{module}.{alias.name}"
        return _UtilsImport(span, collapsed, bound)
    return None


def _comment_out_utils_import(statement, lines, findings: list) -> list:
    """The replacement lines for `lines`, and the NB05/NB03 findings for it."""
    if statement.refusal:
        findings.append(Finding(
            "NB05_UTILS_IMPORT",
            f"{statement.text!r} left as written: {statement.refusal}. "
            f"notebookutils does not exist on AIDP, so this statement still "
            f"has to be split or replaced by hand",
            "flag"))
        return list(lines)
    findings.append(Finding(
        "NB05_UTILS_IMPORT",
        f"{statement.text!r} commented out; notebookutils does not exist on AIDP",
        "rewrite"))
    if statement.unenumerable:
        findings.append(Finding(
            "NB03_NOTEBOOKUTILS",
            f"{statement.text!r} binds names this tool cannot enumerate, so "
            f"its usage sites are not listed below; find them by hand",
            "flag"))
    out = []
    for line in lines:
        if not line.strip():
            out.append(line)
            continue
        indent = line[:len(line) - len(line.lstrip())]
        out.append(f"{indent}# {line.strip()}")
    return out


def _utils_usage_re(names):
    """Matches any of `names` used as a value, with up to two attributes.

    Longest alternative first so a name that is a prefix of another cannot
    win; the lookaround either side is the `\\b` the alternation cannot
    express, and it also stops a *dotted* occurrence -- `self.fs` is not the
    `fs` the import bound.
    """
    ordered = sorted(names, key=lambda name: (-len(name), name))
    return re.compile(
        r"(?<![\w.])(?P<name>" + "|".join(re.escape(n) for n in ordered)
        + r")(?P<surface>(?:\s*\.\s*[A-Za-z_]\w*){0,2})(?![\w])")


# The nodes that open a scope of their own. A class body is one: `class C:
# fs = 1` makes `fs` local to it. A module is not in this list because
# module-level shadowing is a flow question -- the import may come after the
# rebinding -- and this analysis is not flow-sensitive; see `_locally_bound`.
_SCOPE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                ast.ClassDef, ast.ListComp, ast.SetComp, ast.DictComp,
                ast.GeneratorExp)
# What a surface is allowed to be: the name plus up to two attributes. The
# cap is the text scan's, kept so the two paths report the same spelling for
# the same line.
_SURFACE_DEPTH = 2


def _binding_targets(target, into: set) -> None:
    """Add every name `target` binds. Tuples and stars unpack."""
    if isinstance(target, ast.Name):
        into.add(target.id)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for element in target.elts:
            _binding_targets(element, into)
    elif isinstance(target, ast.Starred):
        _binding_targets(target.value, into)


def _locally_bound(scope) -> frozenset:
    """Every name `scope` binds itself, not counting nested scopes.

    A name bound here is not the one the notebookutils import bound, so a
    use of it in this scope is not a notebookutils surface -- which is the
    whole of NB03's scope-awareness.

    `global` and `nonlocal` take a name back out: they say the binding meant
    is the outer one, which is the import.

    Deliberately not flow-sensitive and deliberately not applied to the
    module scope. Inside a function, `fs = open(...)` makes every `fs` in
    that function local whatever line it is on -- that is Python's rule, not
    an approximation. At module level the same statement rebinds from that
    point on only, and the import may sit below it; guessing there would
    drop a real surface, which is the silent direction. So a module-level
    rebinding still reports, and the finding is the one a human reads.
    """
    bound: set = set()
    declared: set = set()

    if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
        args = scope.args
        for arg in (list(getattr(args, "posonlyargs", []) or []) + args.args
                    + args.kwonlyargs
                    + [a for a in (args.vararg, args.kwarg) if a]):
            bound.add(arg.arg)
    for generator in getattr(scope, "generators", ()) or ():
        _binding_targets(generator.target, bound)

    def walk(node) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, _SCOPE_NODES):
                # The nested scope's own name is bound out here; its body
                # is not this scope's business.
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                                      ast.ClassDef)):
                    bound.add(child.name)
                continue
            if isinstance(child, ast.Assign):
                for target in child.targets:
                    _binding_targets(target, bound)
            elif isinstance(child, (ast.AnnAssign, ast.AugAssign,
                                    ast.NamedExpr)):
                _binding_targets(child.target, bound)
            elif isinstance(child, (ast.For, ast.AsyncFor)):
                _binding_targets(child.target, bound)
            elif isinstance(child, ast.withitem):
                if child.optional_vars is not None:
                    _binding_targets(child.optional_vars, bound)
            elif isinstance(child, ast.ExceptHandler):
                if child.name:
                    bound.add(child.name)
            elif isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    bound.add(alias.asname or alias.name.split(".")[0])
            elif isinstance(child, (ast.Global, ast.Nonlocal)):
                declared.update(child.names)
            walk(child)

    walk(scope)
    return frozenset(bound - declared)


def _attribute_chain(node):
    """`(Name, [attr, ...])` for a pure `a.b.c`, else None."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.reverse()
        return node, parts
    return None


def _utils_uses_by_ast(source: str, names):
    """`{(name, surface): count}` for the notebookutils uses in `source`.

    None when the cell does not parse, so the caller falls back to the text
    scan -- a Fabric cell holding `!pip install x` or a `%%bash` body is
    ordinary and must not cost a whole cell's worth of surfaces.

    Only a *load* counts. `fs = open("x")` at module level rebinds the name
    and is not a call into Fabric's utilities; the text scan counted it as
    one.
    """
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError, RecursionError):
        return None
    counts: dict = {}

    def record(name, attrs) -> None:
        surface = ".".join(attrs[:_SURFACE_DEPTH])
        counts[(name, surface)] = counts.get((name, surface), 0) + 1

    def wanted(node, shadowed) -> bool:
        return (isinstance(node.ctx, ast.Load) and node.id in names
                and node.id not in shadowed)

    def visit(node, shadowed) -> None:
        if isinstance(node, _SCOPE_NODES):
            shadowed = shadowed | _locally_bound(node)
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Attribute):
                chain = _attribute_chain(child)
                if chain is not None:
                    base, attrs = chain
                    if wanted(base, shadowed):
                        record(base.id, attrs)
                    # A pure dotted chain holds nothing else to visit.
                    continue
            elif isinstance(child, ast.Name):
                if wanted(child, shadowed):
                    record(child.id, [])
                continue
            visit(child, shadowed)

    visit(tree, frozenset())
    return counts


def _utils_uses_by_text(source: str, names):
    """`{(name, surface): count}` from the mask, for a cell that will not parse.

    Scope-blind: it reports a local that merely shares the import's name.
    That is why `_utils_uses_by_ast` is tried first, and why this runs at
    all -- a surface not reported is a call into Fabric nobody was told
    about.
    """
    counts: dict = {}
    try:
        view = masked_python(source)
    except Unmaskable:
        return counts     # already flagged NB07; nothing here can be read
    for match in _utils_usage_re(names).finditer(view.text):
        surface = re.sub(r"\s+", "", match.group("surface")).lstrip(".")
        key = (match.group("name"), surface)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _flag_utils_usage(sources, utils_names: dict, findings: list) -> None:
    """NB03: every notebookutils surface a human has to replace.

    Runs after the rewrites, over the final text of the Python cells, for two
    reasons: the import lines are comments by then -- and comments are masked
    -- so the alias on `import notebookutils as nu` is not counted as a use
    of `nu`; and an alias bound in cell 1 has to be visible to a use in cell
    7, which a single forward pass cannot do.

    Only the Python cells: `notebookutils` in a `%%sql` body is a column
    name, not a call into Fabric's utilities.

    `sources` is text rather than cells because one entry may be several
    cells: a statement a `# CELL ` marker cut in two (NB07) has no half
    that tokenizes, so scanning the halves contributed nothing and the
    surface list was silently short. Rejoined they do tokenize, and the
    caller hands the rejoined text in. A cell that is untokenizable on its
    own is still skipped -- its literals cannot be located, so `fs.ls("/")`
    written inside someone's string would count as a use -- and the NB07
    finding on it now says that its surfaces are not in this list.

    Scope-aware where the cell parses, because the text scan is not and a
    flag that cries wolf stops being read. Measured on this tree:

        from notebookutils import fs
        def g():
            fs = open("x")
            return fs.read()

      before  NB05_UTILS_IMPORT rewrite,
              NB03_NOTEBOOKUTILS flag, NB03_NOTEBOOKUTILS flag
      after   NB05_UTILS_IMPORT rewrite

    The import binds `fs`, `g` rebinds it, and both of the `fs` inside `g`
    are the local file object. Two flags where none is right.

    One finding per (spelling, surface) rather than per site: a Finding
    carries no line number, so N identical findings would be noise. The
    surface list is what a human has to work through, and the count says how
    much of it there is.
    """
    counts: dict = {}
    for source in sources:
        block_counts = _utils_uses_by_ast(source, utils_names)
        if block_counts is None:
            block_counts = _utils_uses_by_text(source, utils_names)
        for key, count in block_counts.items():
            counts[key] = counts.get(key, 0) + count
    for (name, surface), count in counts.items():
        written = f"{name}.{surface}" if surface else name
        canonical = utils_names[name]
        full = f"{canonical}.{surface}" if surface else canonical
        spelling = written if written == full else f"{written} ({full})"
        uses = f"; {count} uses" if count > 1 else ""
        findings.append(Finding(
            "NB03_NOTEBOOKUTILS",
            f"{spelling} has no AIDP equivalent; migrate it by hand "
            f"(left in place){uses}",
            "flag"))


_FUTURE_IMPORT_RE = re.compile(r"^\s*from\s+__future__\s+import\b")
# A binding of the name `display` that the shim has to stay in front of:
# `def display`, `class display`, `display = ...`, `display: T = ...`, and
# any import that brings the name in -- `from IPython.display import
# display` being the one the shim's own docstring names. `display ==` is
# not a binding, hence the negative lookahead.
_DISPLAY_BINDING_RE = re.compile(
    r"^\s*(?:def\s+display\b|class\s+display\b"
    r"|display\s*(?::[^=\n]+)?=(?!=)"
    r"|(?:from\s+[\w.]+\s+)?import\s+[^\n#]*(?<![\w.])display(?![\w]))")


def _has_future_import(lines) -> bool:
    """Whether any line of `lines` opens a `from __future__` statement.

    Scanned on the mask when the text tokenizes, so one quoted inside a
    docstring is prose, and on the raw lines when it does not -- a cell that
    the tokenizer refuses is exactly the cell whose contents are unknown,
    and over-reporting here only pushes the shim later, which is the safe
    direction.
    """
    text = "\n".join(lines)
    try:
        candidates = masked_python(text).text.split("\n")
    except Unmaskable:
        candidates = list(lines)
    return any(_FUTURE_IMPORT_RE.match(line) for line in candidates)


def _binds_display(lines) -> bool:
    """Whether any line of `lines` binds the name `display`."""
    text = "\n".join(lines)
    try:
        candidates = masked_python(text).text.split("\n")
    except Unmaskable:
        candidates = list(lines)
    return any(_DISPLAY_BINDING_RE.match(line) for line in candidates)


def _shim_insertion_point(lines) -> int:
    """The first line of `lines` the shim may be inserted at.

    Three things have to stay in front of it.

    A leading cell magic: Fabric requires `%%configure` to be the first line
    of its cell.

    The module docstring, which inserting above would demote to a plain
    string expression.

    And every `from __future__` import, which must precede all other
    statements. `compile()` is the only thing that says so -- `ast.parse`
    accepts one anywhere -- so a whole-notebook parse gate would not catch
    it either. Measured before the fix, on a cell reading
    `from __future__ import annotations` / `display(df)`: the input compiled
    and the output raised "from __future__ imports must occur at the
    beginning of the file". They are looked for across the whole cell, not
    just at its head: a cell that does not parse can have one lower down,
    and what matters is getting behind it.

    Scanned on the mask rather than parsed, because a cell need not parse.
    """
    view = _view_of("\n".join(lines), None)
    masked = view.text.split("\n")
    starts, position = [], 0
    for line in lines:
        starts.append(position)
        position += len(line) + 1

    at = len(lines)
    for index, line in enumerate(lines):
        if line.strip():
            at = index + 1 if _CELL_MAGIC_RE.match(line) else index
            break
    if at < len(lines):
        # A docstring is a literal starting at the first code character.
        head = starts[at] + len(lines[at]) - len(lines[at].lstrip())
        for literal in view.literals:
            if literal[0] == head:
                at = bisect.bisect_right(starts, literal[1] - 1)
                break
    for index in range(len(lines)):
        if _FUTURE_IMPORT_RE.match(masked[index]):
            at = max(at, _statement_end(masked, index))
    return min(at, len(lines))


# How far ahead `_split_statement_runs` will look for the other half. A
# statement cut by one marker is two cells; a notebook source held in a
# string can carry several markers and so several cuts. The cap is what
# keeps a notebook whose cells are each independently broken from being
# retokenized once per pair.
_SPLIT_RUN_LIMIT = 8


def _split_statement_runs(notebook) -> dict:
    """`{id(first half): [every half]}` for each statement a marker cut.

    Fabric's `notebook-content.py` format marks a cell boundary with a
    whole line -- `# CELL ` and a run of asterisks -- and has no way to
    escape one. A notebook that *builds notebook source* in a string
    literal therefore cannot round-trip through a Git export: the parser
    splits on the marker wherever it appears, string literal included, and
    one statement becomes two halves that neither tokenize.

    This is not hypothetical and not rare enough to ignore. It is the sole
    cause of NB07 in the bundled estate: `gbrueckl_Fabric.Toolbox_028`
    assigns `init_script = f\"\"\"# Fabric notebook source ...\"\"\" ` with a
    `# CELL ` marker inside it, and that one statement produced 2 of the 2
    NB07 findings `make demo` reports.

    Told apart from a genuinely malformed cell by the only evidence there
    is: rejoin the halves with the marker line between them, exactly as
    they were in the file, and see whether the result tokenizes. If it
    does, the marker was inside a literal. Nothing is merged -- the halves
    are still left exactly as written, and the round trip still holds
    byte-for-byte -- but the report says one thing once instead of the same
    hedged thing twice.

    An `.ipynb` notebook cannot have this: its cells are JSON array
    entries, so there is no marker to collide with. It has no `marker` on
    its blocks either, which is what the guard below tests.
    """
    blocks = getattr(notebook, "blocks", None) or []
    code_kinds = tuple(getattr(notebook, "CODE_KINDS", ("CELL",)))
    runs: dict = {}
    consumed: set = set()
    for index, block in enumerate(blocks):
        if (block.kind not in code_kinds or id(block) in consumed
                or not hasattr(block, "marker")):
            continue
        try:
            masked_python("\n".join(block.lines))
            continue                      # tokenizes on its own: not this
        except Unmaskable:
            pass
        run = [block]
        for other in blocks[index + 1:index + _SPLIT_RUN_LIMIT]:
            if other.kind not in code_kinds or not other.marker:
                break
            run.append(other)
            try:
                masked_python(_rejoined(run))
            except Unmaskable:
                continue
            runs[id(block)] = run
            consumed.update(id(member) for member in run)
            break
    return runs


def _rejoined(run) -> str:
    """The halves of `run` put back together, markers and all.

    Exactly the text they occupied in the file, so the result tokenizes
    when the split was a marker inside a string literal -- which is both
    how `_split_statement_runs` recognises one and what lets NB03 read the
    notebookutils surfaces inside it.
    """
    text = "\n".join(run[0].lines)
    for member in run[1:]:
        text = f"{text}\n{member.marker}\n" + "\n".join(member.lines)
    return text


def _split_statement_finding(run) -> Finding:
    """The NB07 a marker inside a string literal earns, once for the run."""
    kinds = ", ".join(
        sorted({str(member.marker).strip().strip("* ").strip("#- ").strip()
                for member in run[1:]}) or ["CELL"])
    return Finding(
        "NB07_CELL_UNTOKENIZABLE",
        f"a `{kinds}` marker line inside a string literal split one "
        f"statement across {len(run)} cells of this file, so no half of it "
        f"tokenizes and none of them was translated -- they were left "
        f"exactly as written. Rejoined with the marker between them they "
        f"do tokenize, which is how this is told apart from a cell that is "
        f"malformed on its own. Fabric's `notebook-content.py` format has "
        f"no way to escape a marker, so a notebook that builds notebook "
        f"source in a string cannot round-trip through a Git export: what "
        f"is one cell in the workspace is {len(run)} in the file. Export "
        f"this notebook as `.ipynb`, where a cell is a JSON array entry "
        f"and there is no marker to collide with, and translate that",
        "flag")


def _shim_cell(cells, findings: list):
    """Which cell the shim goes into, and NB31 when that is not the first.

    `cells` is every Python cell in notebook order. The shim belongs at the
    top of the first of them: it stands in for a *builtin*, which existed
    before line 1, so anything the user binds to the name `display`
    anywhere has to come after it and shadow it -- exactly as their binding
    shadowed Fabric's builtin. Splicing at the first display() call got
    that backwards whenever the user's own `def display` or `from
    IPython.display import display` sat in an earlier cell, which is the
    natural place to put a helper: measured, the shim won and the user's
    rendering was silently replaced.

    One thing outranks that, and it is why this is a notebook-wide search
    rather than `cells[0]`. A `from __future__` import must precede every
    other statement in the *file*, and flattening a notebook into one `.py`
    is what makes the notebook's cells one file. `_shim_insertion_point`
    has always got behind the `from __future__` imports in the cell it was
    given; it could not get behind one in a cell it was never shown.
    Measured on this tree before the fix, cell 1 holding only a comment and
    cell 2 opening `from __future__ import annotations`:

        input   compile() ok
        output  SyntaxError: from __future__ imports must occur at the
                beginning of the file
        findings: NB04_DISPLAY, NB08_DISPLAY_SHIM -- nothing about it

    -- a file this tool made illegal, silently. Note which case that is: a
    `from __future__` in cell 2 with any *statement* in cell 1 does not
    compile as one file before the translation either, and is Fabric
    notebook source doing what Fabric notebook source does. A blanket
    `compile()` gate cannot tell those apart, and would refuse the 17 of
    the 31 `.py` files `make demo` emits that hold a `%%sql` cell. This
    rule is not a gate: it moves the one line the tool would otherwise add
    in the wrong place.

    The two constraints genuinely conflict when a cell the shim now skips
    binds `display` itself, and there is no placement that satisfies both.
    The shim still moves -- a SyntaxError is worse than a shadowed helper,
    and it is the one of the two that stops the notebook from running at
    all -- and NB31 is raised as a `flag` rather than an `info` to say the
    binding wins where it used to lose.
    """
    if not cells:
        return None
    at = 0
    for index, cell in enumerate(cells):
        if _has_future_import(cell.lines):
            at = index
    if at == 0:
        return cells[0]
    skipped = [cell for cell in cells[:at] if _binds_display(cell.lines)]
    findings.append(Finding(
        "NB31_DISPLAY_SHIM_AFTER_FUTURE",
        f"the `display` shim was put in Python cell {at + 1} rather than "
        f"cell 1, because that cell opens with a `from __future__` import "
        f"and one of those has to precede every other statement in the "
        f"file. Each Fabric cell compiles on its own, so the import is "
        f"legal where it is written; this tool flattens the notebook into "
        f"one `.py`, and the shim above it would have made that file a "
        f"SyntaxError."
        + (f" One earlier cell binds the name `display` itself, so the "
           f"shim no longer precedes it and their order is now the "
           f"reverse of Fabric's: move the `from __future__` import to "
           f"the first cell to get both" if skipped else
           " Nothing before it binds the name `display`, so the shim still "
           "loses every argument about that name"),
        "flag" if skipped else "info"))
    return cells[at]


def _splice_display_shim(block, findings: list) -> None:
    """Put `DISPLAY_SHIM` at the top of `block`. `_shim_cell` picks it.

    A new cell would read better but has to be threaded into
    `IpynbNotebook`'s parallel `data["cells"]` array as well; the top of an
    existing cell works for both formats.
    """
    at = _shim_insertion_point(block.lines)
    block.lines[at:at] = DISPLAY_SHIM.rstrip("\n").split("\n") + [""]
    findings.append(Finding(
        "NB08_DISPLAY_SHIM",
        "a `display` shim was added to this notebook; AIDP has no display() "
        "and Fabric's is a builtin that renders a Spark DataFrame, a pandas "
        "DataFrame and more, so rewriting each call to `.show()` broke every "
        "input but the first. Two divergences from Fabric remain: a Spark "
        "DataFrame goes to .show(), which prints 20 rows and truncates wide "
        "columns rather than drawing a grid, and Fabric's rendering options "
        "(summary=, and the extra arguments its overloads take) are accepted "
        "and ignored",
        "rewrite"))


def translate(content, *, namespace, default_lakehouse=None,
              guid_index=None, catalog=None,
              aidp_catalog=DEFAULT_CATALOG) -> TranslationResult:
    """Translate one Fabric notebook.

    `catalog` is the tier-resolution map built by `inventory.catalog`;
    `aidp_catalog` is the AIDP catalog table names are built under. They are
    unrelated things that shared a word, and the notebook translator had only
    the first -- so `plan --catalog myc` renamed tables everywhere except
    here.
    """
    source = content if isinstance(content, str) else ""
    findings: list = []
    try:
        notebook = parse_any(source)
        # A malformed `# META` block parses and then raises from
        # `language_for` in the middle of the cell loop, which migrate
        # reported as a bare `error` row with no rule. Decoded up front it is
        # the same refusal as any other notebook that will not parse.
        check_meta(notebook)
    except (NotebookParseError, IpynbParseError) as exc:
        return TranslationResult(source, source, [Finding(
            "NB06_UNPARSEABLE", f"cannot parse notebook source: {exc}", "flag")])

    # Fabric injects both module names with no import, so they are in scope
    # in every notebook; an import only adds the spellings it bound.
    utils_names = {name: name for name in _UTILS_MODULES}
    # Notebook-wide facts the per-line rules cannot see for themselves.
    scope = NotebookScope(temp_views=_temp_view_names(notebook),
                          known_items=_known_items(catalog))
    # Worked out before the loop, because it needs the cells as they were
    # read: a run of halves is recognised by rejoining them, and the loop
    # rewrites `block.lines` as it goes.
    split_runs = _split_statement_runs(notebook)
    split_members = {id(member) for run in split_runs.values()
                     for member in run[1:]}
    python_blocks: list = []
    # The rejoined text of each statement a marker cut across cells. No
    # half of one tokenizes, so no rule can rewrite it, but NB03 can still
    # read its notebookutils surfaces out of the rejoin.
    rejoined_python: list = []
    # Every lakehouse bucket name the path rules emit, in order. NB25 is a
    # property of the set, not of any one rewrite, so it is raised after the
    # loop over what this collected.
    emitted: list = []
    # Every cell the shim could go into, in notebook order. A list rather
    # than the first one: `_shim_cell` has to get behind a `from __future__`
    # import wherever in the notebook it is, and reading only the first cell
    # is how the shim came to be spliced above one.
    shim_cells: list = []
    display_calls = 0
    for block in notebook.code_blocks:
        # Fabric writes a non-Python cell as Python comments, every body line
        # behind `# MAGIC `. Read literally the cell is inert commentary, so a
        # `%%sql` body reached the output untranslated and unflagged -- valid
        # Python, invalid Spark. Unwrap it and route by the language Fabric
        # recorded in the FOLLOWING metadata block.
        if getattr(block, "is_magic", False):
            _unwrap_magic_cell(block, notebook, findings)
        # Before this cell's rules run, not during: the current database is
        # session state, and a rule that resolves a bare name has to know
        # where the session is pointing before it resolves the first one.
        _scan_database_switches("\n".join(block.lines), findings,
                                default_lakehouse=default_lakehouse,
                                scope=scope)
        first = next((ln for ln in block.lines if ln.strip()), "")
        # A cell magic is not the only thing that makes a cell SQL, and it
        # used to be the only thing this looked at. Fabric's T-SQL notebook
        # is committed as `notebook-content.sql`, its cells are bare SQL
        # with no magic line at all, and every one of them went down the
        # Python path below: the tokenizer accepts `SELECT TOP 5 * FROM
        # dbo.claim` -- tokenizing is not parsing -- so there was no NB07
        # either, no rule matched, and the notebook came back byte-identical
        # with zero findings and graded PASS. Measured on a one-cell
        # `notebook-content.sql` bound to SalesLake: `TOP`, `GETDATE()` and
        # `dbo.claim` all reached the artifact untranslated, and `publish`
        # then wrote that T-SQL into a *Python* cell of the .ipynb it
        # uploads, which a NOTEBOOK_TASK runs as Python.
        cell_language = (notebook.language_for(block)
                         if hasattr(notebook, "language_for") else None)
        sql_notebook = bool(getattr(notebook, "sql_source", False))
        sql_magic = bool(_SQL_MAGIC_RE.match(first))
        if sql_magic or cell_language in _MAGIC_SQL_LANGUAGES or sql_notebook:
            body = "\n".join(block.lines)
            body = rule_magics(body, findings, default_lakehouse=default_lakehouse,
                               catalog=catalog)
            # An explicit magic says which dialect; otherwise the cell's
            # recorded language does, and a `notebook-content.sql` item is
            # T-SQL by construction -- that is what the feature is -- unless
            # a cell says `sparksql` outright.
            if sql_magic:
                is_tsql = bool(_TSQL_MAGIC_RE.match(first))
            else:
                is_tsql = (cell_language in _TSQL_CELL_LANGUAGES
                           or (sql_notebook
                               and cell_language not in _SPARK_SQL_CELL_LANGUAGES))
            body = apply_sql_rules(
                body, findings,
                # A `%%tsql` body is not Spark dialect, and the path rule
                # reads `"..."` Spark's way; passing no namespace is how it
                # is switched off for that one cell kind.
                namespace=None if is_tsql else namespace,
                guid_index=guid_index,
                default_lakehouse=default_lakehouse, catalog=catalog,
                aidp_catalog=aidp_catalog, scope=scope, emitted=emitted)
            body = _translate_sql_body(
                body, findings, is_tsql=is_tsql, aidp_catalog=aidp_catalog,
                magic_line=sql_magic)
            block.lines = body.split("\n")
            continue
        # Every Python rule below scans source with a regular expression, so
        # every one of them could read inside a string literal. The cell is
        # tokenized once, whole -- a single line of a triple-quoted literal is
        # not parseable on its own -- and each rule scans its own line's mask.
        try:
            views = cell_views(block.lines)
        except Unmaskable as exc:
            if id(block) in split_members:
                # The other half of a statement a marker cut in two. It is
                # one defect, reported once, on the first half -- the same
                # cell reported N times is how a flag stops being read.
                continue
            if id(block) in split_runs:
                run = split_runs[id(block)]
                findings.append(_split_statement_finding(run))
                # No half of it tokenizes and the rejoin does, so NB03 can
                # read its notebookutils surfaces even though no rule can
                # rewrite them. Skipping it left the surface list short
                # with nothing saying so.
                rejoined_python.append(_rejoined(run))
                continue
            findings.append(Finding(
                "NB07_CELL_UNTOKENIZABLE",
                f"the tokenizer stops on this cell at "
                f"{untokenizable_reason(exc)}, so its string literals cannot "
                f"be located; every rewrite here needs to know where they "
                f"are, so the cell was left exactly as written. Rejoining "
                f"it with the code cells that follow does not tokenize "
                f"either, so this is not one statement cut across two by a "
                f"marker inside a string literal -- NB07 says so in as "
                f"many words when it is. The likelier reading is that the "
                f"cell is malformed as it stands; compare the cell "
                f"boundary against the original before assuming it. Any "
                f"notebookutils surface in this cell is missing from the "
                f"NB03 list below, for the same reason: without the "
                f"literal boundaries, a name inside one of them cannot be "
                f"told from a call",
                "flag"))
            continue
        python_blocks.append(block)
        # A `.sql()` argument is Spark SQL, and a query is routinely written
        # triple-quoted over several lines. The per-line loop below cannot
        # see such a literal at all -- `cell_views` deliberately gives a
        # multi-line literal to no single line -- so this one rule runs over
        # the whole cell, before the loop, and the line views are rebuilt
        # from the result.
        body = "\n".join(block.lines)
        rewritten_body = rule_sql_call_strings(
            body, findings, default_lakehouse=default_lakehouse,
            catalog=catalog, aidp_catalog=aidp_catalog, scope=scope,
            namespace=namespace, guid_index=guid_index, emitted=emitted)
        if rewritten_body != body:
            block.lines = rewritten_body.split("\n")
            views = cell_views(block.lines)
        # %%html, %%bash, %%writefile, %%configure: the body is not Python,
        # so a `display(` in it is markup rather than a call, and the cell is
        # no place to define a function. %%pyspark and %%python are Python;
        # rule_magics strips those lines below.
        magic = _CELL_MAGIC_RE.match(first)
        is_python = (magic is None
                     or magic.group(1).casefold() in _MAGIC_PYTHON_LANGUAGES)
        if is_python:
            shim_cells.append(block)
        rewritten = []
        index = -1
        # Indexed rather than a plain `for`: a parenthesised import is one
        # statement over several lines and has to be consumed as one.
        while index + 1 < len(block.lines):
            index += 1
            original, view = block.lines[index], views[index]
            line = original
            statement = _utils_import(block.lines, views, index)
            if statement is not None:
                rewritten.extend(_comment_out_utils_import(
                    statement, block.lines[index:index + statement.span],
                    findings))
                utils_names.update({name: path
                                    for name, path in statement.bound.items()
                                    if name not in utils_names})
                index += statement.span - 1
                continue
            # Before `_rewrite_paths`, not after: both rules read the same
            # literal and the path rule replaces it, so running second left
            # this one nothing its pattern could match -- NB20 did not fire
            # once in the bundled estate, or anywhere else, until this line
            # moved. A `Tables/` location is a table first and a path only
            # if this rule declines it.
            line = rule_tables_path(line, findings,
                                    default_lakehouse=default_lakehouse,
                                    catalog=catalog, guid_index=guid_index,
                                    aidp_catalog=aidp_catalog, view=view)
            # A rule reports offsets into the text it was handed, so a mask
            # built before a rewrite is no use after one. While the line is
            # untouched the cell-wide mask is kept, because it is the only one
            # that can see a literal spanning several lines -- re-masking line
            # 2 of a docstring on its own would read it as code again, which
            # is the defect.
            line, view = _remasked(line, original, view)
            line = _rewrite_paths(line, findings, namespace=namespace,
                                  default_lakehouse=default_lakehouse,
                                  guid_index=guid_index, catalog=catalog,
                                  emitted=emitted, view=view)
            line, view = _remasked(line, original, view)
            line = rule_magics(line, findings,
                               default_lakehouse=default_lakehouse,
                               catalog=catalog, view=view)
            line, view = _remasked(line, original, view)
            line = rule_sempy(line, findings, view=view)
            line, view = _remasked(line, original, view)
            line = rule_table_refs(line, findings,
                                   default_lakehouse=default_lakehouse,
                                   catalog=catalog,
                                   aidp_catalog=aidp_catalog, scope=scope,
                                   view=view)
            line, view = _remasked(line, original, view)
            if is_python:
                display_calls += _note_display(line, findings, view=view)
            rewritten.append(line)
        block.lines = rewritten

    _note_two_bucket_spellings(emitted, default_lakehouse, findings)
    _flag_utils_usage(["\n".join(block.lines) for block in python_blocks]
                      + rejoined_python, utils_names, findings)
    if display_calls:
        # After the loop, so the cells are in their final text: the shim's
        # placement has to be valid against what is written out, not
        # against what came in.
        shim_cell = _shim_cell(shim_cells, findings)
        if shim_cell is not None:
            _splice_display_shim(shim_cell, findings)
    return TranslationResult(source, serialize_any(notebook), findings)
