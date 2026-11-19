"""The six source slices, and the one list every verb reads them from.

`inventory --sources`, `migrate --filter` and `verify --filter` all name the
same slices, and until this module each kept its own copy. They drifted:
argparse took `--filter`'s choices from the inventory's list, so `migrate
--help` advertised `--filter dataflow`, while the runner's own list had five
entries and answered "unknown migration filter 'dataflow'". `verify --filter
dataflow` was refused for the same reason. Dataflows are migrated -- 16 of the
106 assets in the bundled estate are dataflow queries -- so the help was right
and both runners were wrong.

One ordered table below, three views derived from it, so a slice added in the
future is added once:

  ALL_SOURCES           the slice names, in scan order
  ITEM_TYPES_BY_SOURCE  Fabric item type -> the scanner that claims it. A type
                        in no scanner's list reaches the manifest as an
                        unsupported item rather than vanishing.
  PLAN_TYPES_BY_SOURCE  plan asset `source.type` -> the slice it belongs to;
                        what `migrate --filter` selects on.
  SOURCE_BY_PLAN_TYPE   the reverse, for attributing a report row to a slice.

`pipeline` also reads Notebook items, to resolve a pipeline's notebook
references; the items it *owns* are DataPipeline, which is what is mapped here.

Two plan source types are deliberately absent -- `fabric_unreadable_item` and
`fabric_unsupported_item`. They are items no scanner could claim, so they
belong to no slice; a filter of any kind leaves them out.

This module imports nothing from the package: the inventory, the migrate
runner and the verifier all read it, and any of them importing another would
be backwards.
"""
from __future__ import annotations

# source name -> (Fabric item types it scans, plan `source.type` values it owns)
_SOURCES = {
    "notebook": (
        ("Notebook",),
        ("fabric_notebook",),
    ),
    "warehouse": (
        ("Warehouse",),
        ("fabric_warehouse_table", "fabric_warehouse_view",
         "fabric_warehouse_procedure", "fabric_warehouse_function",
         "fabric_warehouse_other"),
    ),
    "lakehouse": (
        ("Lakehouse",),
        ("fabric_lakehouse", "fabric_shortcut"),
    ),
    "pipeline": (
        ("DataPipeline",),
        ("fabric_pipeline",),
    ),
    "semanticmodel": (
        ("SemanticModel",),
        ("fabric_semantic_model",),
    ),
    "dataflow": (
        ("Dataflow",),
        # The last two are whole Dataflows nothing could be read from --
        # unparseable mashup.pq, or no Node parser. They belong to this
        # slice, unlike `fabric_unreadable_item`, because the item's type
        # *is* known: it is a Dataflow, and `--filter dataflow` is exactly
        # the run that should report it.
        ("fabric_dataflow_query", "fabric_dataflow_unreadable",
         "fabric_dataflow_untranslated"),
    ),
}

# A lakehouse has two sections and only one of them holds tables. The test
# was written out three times -- in the shortcut scanner, in the catalog and
# in the planner -- before a fourth caller needed it, which is how the
# `--filter` lists above drifted. It lives here for the same reason they do.
TABLES_SECTION = "Tables"


def is_table_section(section) -> bool:
    """Whether a shortcut in this lakehouse section stands for a table.

    `Files/x` is a folder of objects: no schema, no columns, no rows, and
    nothing addresses it with a table name.
    """
    return str(section or "").strip().casefold() == TABLES_SECTION.casefold()


ALL_SOURCES = tuple(_SOURCES)
ITEM_TYPES_BY_SOURCE = {name: items for name, (items, _) in _SOURCES.items()}
PLAN_TYPES_BY_SOURCE = {name: frozenset(plan) for name, (_, plan) in _SOURCES.items()}
SOURCE_BY_PLAN_TYPE = {plan_type: name
                       for name, types in PLAN_TYPES_BY_SOURCE.items()
                       for plan_type in types}
