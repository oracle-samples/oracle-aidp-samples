"""Turn a manifest into an ordered list of plan assets.

Each asset names a source item, an AIDP target, the transform chain that would
produce it, and the assets it depends on. Ordering is a stable topological sort
so a producer precedes its consumers; cycles are reported and ordered last
rather than refused, because Fabric permits mutually-referencing notebooks.
"""
from __future__ import annotations

import csv
import re
import time
import uuid
from pathlib import Path

from fabric_aidp._atomic import write_json_atomic
from fabric_aidp.namespace import PLACEHOLDER, validated_namespace
from fabric_aidp.sources import is_table_section
from fabric_aidp.naming import (
    DEFAULT_CATALOG, DEFAULT_SCHEMAS, aidp_schema, aidp_table,
    validated_catalog,
)

OCI_NAMESPACE_DEFAULT = PLACEHOLDER
_TABLE_KINDS = {"table": "aidp_table", "view": "aidp_view"}


_validated_namespace = validated_namespace


def _rows(manifest: dict, source: str, key: str) -> list:
    value = manifest.get("sources", {}).get(source)
    if not isinstance(value, dict):
        return []
    items = value.get("items")
    if not isinstance(items, dict):
        return []
    rows = items.get(key)
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def _item_id(kind: str, row: dict) -> str:
    """`<kind>.<folder>/<name>`, or `<kind>.<name>` for an item at the root.

    A Fabric display name is unique inside a workspace *folder*, not inside
    a workspace, so the name alone is not an identity. Two
    `Shared_Load.Notebook` directories under `teamA/` and `teamB/` -- an
    ordinary Git-integration export -- both produced `notebook.Shared_Load`
    and `plan` refused the entire workspace over it.

    The folder is only spelt out when there is one. All 106 assets of the
    bundled demo estate sit at the export root, so all 106 ids are
    unchanged by this: the disambiguator costs nothing to read until it is
    needed. `/` is the separator because Fabric forbids it in an item
    name, so `<folder>/<name>` can never be spelt the same way twice.
    """
    name = str(row.get("name", "") or "")
    folder = str(row.get("folder", "") or "").strip("/")
    return f"{kind}.{folder}/{name}" if folder else f"{kind}.{name}"


def _item_ref(kind: str, row: dict) -> str:
    """The `<folder>/<name>` tail of `_item_id`, with no kind prefix."""
    return _item_id(kind, row)[len(kind) + 1:]


def _by_display_name(rows, kind: str) -> dict:
    """{display name: asset id} for every name exactly one item claims.

    A `%run Child`, a `notebookutils.notebook.run("Child")` and a
    pipeline's ExecutePipeline all name their target by display name and
    never by folder, so once the id carries a folder the literal
    `notebook.Child` stops being anybody's id. Resolving through this map
    is what keeps those edges pointing at a real asset.

    A name two folders both use is left out on purpose: the reference is
    ambiguous in the export itself, and choosing one of them would put a
    confident wrong edge in the plan. Such a reference falls back to the
    unqualified form, which matches no asset, so it is reported under
    `warnings.dangling_depends_on` -- visible rather than invented.
    """
    by_name: dict = {}
    for row in rows:
        by_name.setdefault(str(row.get("name", "") or ""), []).append(
            _item_id(kind, row))
    return {name: ids[0] for name, ids in by_name.items() if len(ids) == 1}


def _resolve(index: dict, kind: str, name) -> str:
    return index.get(str(name), f"{kind}.{name}")


def _table_parts(name) -> tuple:
    """A dotted table name as casefolded, unquoted parts."""
    return tuple(part.strip().strip('`"[]').casefold()
                 for part in str(name or "").split(".")
                 if part.strip().strip('`"[]'))


def _same_table(left: tuple, right: tuple) -> bool:
    """Whether two written names can be the same table.

    Suffix matching on dot boundaries, which is the rule `catalog.py`
    already resolves references by: a notebook writes `agg_claims` and
    another reads `SalesLake.agg_claims`, and those are one table.
    `dbo.claims` and `sales.claims` are not, and are not matched.
    """
    shortest = min(len(left), len(right))
    return shortest > 0 and left[-shortest:] == right[-shortest:]


def _table_producers(notebooks) -> list:
    """`[(name parts, asset id)]` -- who writes each table, once each."""
    out = []
    for nb in notebooks:
        writes = nb.get("writes")
        if not isinstance(writes, list):
            continue
        asset_id = _item_id("notebook", nb)
        for name in writes:
            parts = _table_parts(name)
            if parts:
                out.append((parts, asset_id))
    return out


def _producers_of(read, producers, own_id: str) -> list:
    """Every notebook that writes the table `read` names, except this one.

    Every one of them, not the first: if two notebooks write
    `agg_claims`, a reader has to come after both, and dropping the
    ambiguity would drop a real ordering constraint. Two notebooks that
    write each other's inputs make a cycle, which `_ordered` already
    reports and orders last rather than refusing.
    """
    parts = _table_parts(read)
    if not parts:
        return []
    return [asset_id for written, asset_id in producers
            if asset_id != own_id and _same_table(parts, written)]


def _warehouse_asset_id(warehouse: dict, obj: dict) -> str:
    """`warehouse.<item>.<kind>.<schema>.<name>`, or `.<file>` for `other`.

    The warehouse is in the id because two Warehouses in one workspace may
    each hold a `dbo.CurrentDate` -- legal in Fabric, and present in the
    real sample estate.

    An object the classifier could not read has no database identity to be
    named by: `name` fell back to the filename stem, so the two
    `helper.sql` files a DacFx export writes at `schemas/dbo/tables/` and
    `schemas/sales/tables/` -- an ordinary layout, and two different
    objects -- produced one id and `plan` refused the whole workspace. The
    file is what the export uses to tell them apart. A classified object
    keeps `schema.name`, which is the identity the database enforces and
    the one a reader greps for.
    """
    kind = obj.get("kind", "other")
    schema, name = obj.get("schema", ""), obj.get("name", "")
    qualified = f"{schema}.{name}" if schema else name
    tail = (obj.get("file") or qualified) if kind == "other" else qualified
    item = _item_ref("warehouse", warehouse)
    return (f"warehouse.{item}.{kind}.{tail}" if item
            else f"warehouse.{kind}.{tail}")


def _notebook_assets(manifest: dict) -> list:
    out = []
    notebooks = _rows(manifest, "notebook", "notebooks")
    by_notebook = _by_display_name(notebooks, "notebook")
    by_lakehouse = _by_display_name(
        _rows(manifest, "lakehouse", "lakehouses"), "lakehouse")
    # The producer side of the table edges. `extract_written_tables` has
    # found `saveAsTable` since the catalog landed, so the writes were
    # known and simply never became edges: two notebooks linked only by
    # `agg_claims` were ordered alphabetically, reader first.
    producers = _table_producers(notebooks)
    for nb in notebooks:
        edges = nb.get("edges") or {}
        # `run_multiple` is here with the other two: a notebook that
        # launches ten children with one `runMultiple` call depends on
        # every one of them exactly as a `%run` does.
        depends = [_resolve(by_notebook, "notebook", n) for n in
                   list(edges.get("run") or [])
                   + list(edges.get("notebook_run") or [])
                   + list(edges.get("run_multiple") or [])]
        if nb.get("default_lakehouse"):
            depends.append(_resolve(by_lakehouse, "lakehouse",
                                    nb["default_lakehouse"]))
        own_id = _item_id("notebook", nb)
        for read in nb.get("reads") or []:
            depends.extend(_producers_of(read, producers, own_id))
        out.append({
            "id": _item_id("notebook", nb),
            "source": {"type": "fabric_notebook", "name": nb.get("name", ""),
                       "language": nb.get("language", "python"),
                       "default_lakehouse": nb.get("default_lakehouse"),
                       "content": nb.get("content", ""),
                       # A reference whose target could not be read
                       # statically -- `notebookutils.notebook.run(nb)`.
                       # `edges.py` says in its own docstring that it
                       # records these "so the report can tell a reviewer
                       # that a dependency exists but its name could not
                       # be determined", and until this they reached no
                       # report: `grep unresolved plan/planner.py` found
                       # two comments and no code.
                       "unresolved_refs": sorted(
                           edges.get("unresolved") or []),
                       "parse_error": nb.get("parse_error")},
            "target": {"type": "aidp_notebook", "name": nb.get("name", "")},
            "transform_chain": ["fabric_notebook_to_spark", "create_aidp_notebook"],
            "depends_on": sorted(set(depends)),
        })
    return out


# Fabric's default Warehouse schema, and T-SQL's: an unqualified `FROM t`
# in a DacFx file means `dbo.t`, which is the same assumption
# `inventory.warehouse.classify_sql_object` already makes about an
# unqualified CREATE.
_DEFAULT_WAREHOUSE_SCHEMA = "dbo"


def _object_slot(schema, name) -> tuple:
    return (str(schema or "").strip().strip('[]"`').casefold(),
            str(name or "").strip().strip('[]"`').casefold())


def _warehouse_object_index(warehouse: dict, ids: dict) -> tuple:
    """`({(schema, name): id}, {name: id})` for one warehouse's own objects.

    Only tables and views: a `FROM` never reads a procedure, and a
    function call is already excluded by the scanner. The second map
    answers an unqualified reference whose default-schema guess missed,
    and holds only names exactly one object claims -- two `helper`s in two
    schemas make a bare `FROM helper` ambiguous, and an edge chosen out of
    an ambiguity is an ordering constraint nobody wrote.
    """
    qualified, bare = {}, {}
    for obj in warehouse.get("objects", []) or []:
        if not isinstance(obj, dict) or obj.get("kind") not in ("table", "view"):
            continue
        asset_id = ids.get(id(obj))
        if not asset_id:
            continue
        schema, name = _object_slot(obj.get("schema"), obj.get("name"))
        if not name:
            continue
        qualified[(schema, name)] = asset_id
        bare.setdefault(name, []).append(asset_id)
    return qualified, {n: v[0] for n, v in bare.items() if len(v) == 1}


def _warehouse_depends(obj: dict, item_name: str, qualified: dict,
                       bare: dict, own_id: str) -> list:
    """Sibling objects this one reads, as asset ids.

    Same warehouse only. A three-part reference is another database
    unless its first part is this warehouse, and a reference that
    resolves to nothing here is left out rather than emitted as a
    dangling edge -- a DacFx file legitimately reads tables that live
    somewhere else, and 40 warnings a reviewer cannot act on is noise.
    """
    found = []
    for reference in obj.get("reads") or []:
        parts = [part for part in str(reference).split(".") if part.strip()]
        if len(parts) == 3:
            if parts[0].strip().strip('[]"`').casefold() != item_name.casefold():
                continue
            parts = parts[1:]
        if len(parts) == 2:
            target = qualified.get(_object_slot(parts[0], parts[1]))
        elif len(parts) == 1:
            slot = _object_slot(_DEFAULT_WAREHOUSE_SCHEMA, parts[0])
            target = qualified.get(slot) or bare.get(slot[1])
        else:
            continue
        if target and target != own_id and target not in found:
            found.append(target)
    return found


def _warehouse_target(where, obj, kind, schema, name, catalog) -> dict:
    """What one Warehouse object becomes on AIDP.

    `other` is the classifier's "I could not tell what this is", and an
    object it could not read has no AIDP table to be. A `CREATE INDEX`
    script creates no table; a `GRANT` creates no table; the whole point of
    the kind is that nobody knows. The planner already said so in two of the
    three fields -- `target.type` is `aidp_sql_object`, not `aidp_table`,
    and `transform_chain` omits `create_aidp_table` -- and then contradicted
    itself in the third by running the name through `aidp_table` anyway.

    MEASURED, on the two-`helper.sql` DacFx layout that #26 fixed the id
    collision for: both files targeted `default.AcmeDW.helper`, a
    three-part name for a table the plan never promises to create, and both
    got the *same* one because `name` for an unclassified object is only
    the filename stem.

    The duplicate is the symptom. Making the name unique would have left
    two plausible table names for two scripts that create no tables, which
    is worse than one implausible name: it reads as a resolved answer.
    So an unclassified object is named by the export file instead -- its
    only identity, the same reading `_warehouse_asset_id` already takes --
    and the collision goes away as a consequence rather than as the goal.
    `schema` is "" for the same reason: it lands in no AIDP schema.

    Everything the classifier *did* read keeps a qualified AIDP name. A
    procedure and a function are not tables either, but they have a real
    `schema.name` the database itself enforces, so `<catalog>.<item>.<name>`
    is a name that means something for them.
    """
    if kind != "other":
        return {"type": _TABLE_KINDS.get(kind, "aidp_sql_object"),
                "schema": aidp_schema(where, schema),
                "name": aidp_table(where, name, schema=schema,
                                   catalog=catalog)}
    qualified = f"{schema}.{name}" if schema else name
    return {"type": "aidp_sql_object", "schema": "",
            "name": obj.get("file") or qualified}


def _warehouse_assets(manifest: dict, catalog: str) -> list:
    out = []
    for warehouse in _rows(manifest, "warehouse", "warehouses"):
        # Two passes: an object's id has to exist before another object
        # can depend on it, and a view may be read by a file the walk
        # reached first.
        ids = {id(obj): _warehouse_asset_id(warehouse, obj)
               for obj in warehouse.get("objects", []) or []
               if isinstance(obj, dict)}
        qualified, bare = _warehouse_object_index(warehouse, ids)
        for obj in warehouse.get("objects", []) or []:
            if not isinstance(obj, dict):
                continue
            kind = obj.get("kind", "other")
            schema, name = obj.get("schema", ""), obj.get("name", "")
            where = warehouse.get("name", "")
            asset_id = ids[id(obj)]
            out.append({
                "id": asset_id,
                # `read_error`: the inventory could not decode this file, so
                # `sql` is "". Without it the migration translates the empty
                # string, writes a 0-byte .spark.sql and grades it PASS --
                # the same shape as the unreadable notebook the runner now
                # blocks, reached through a bad encoding instead.
                "source": {"type": f"fabric_warehouse_{kind}", "warehouse":
                           warehouse.get("name", ""), "schema": schema, "name": name,
                           "file": obj.get("file", ""),
                           "read_error": obj.get("read_error", ""),
                           "sql": obj.get("sql", "")},
                "target": _warehouse_target(where, obj, kind, schema, name,
                                            catalog),
                "transform_chain": ["tsql_to_spark_sql"] + (
                    ["create_aidp_table"] if kind in _TABLE_KINDS else []),
                "depends_on": sorted(_warehouse_depends(
                    obj, where, qualified, bare, asset_id)),
            })
    return out


def _dataflow_assets(manifest: dict) -> list:
    """One asset per translatable query, plus one for a Dataflow nobody read.

    Only `kind == "pipeline"` queries become migrations, which is right --
    helpers and parameters are not; an `unread_member` becomes an asset that
    reports why it is not one. But a Dataflow whose mashup.pq will not
    parse has no queries at all, and one scanned without the Node parser has
    only `uncounted` ones, so both produced nothing and the item disappeared
    from plan, migrate and verify with the asset count quietly smaller.
    Same class as the unreadable `.platform` and the unreadable shortcuts
    file, and the same remedy: the item is in the export, it is not being
    migrated, so it is an asset that says so.

    An *empty* Dataflow -- `section Section1;` and nothing else, which 4 of
    the demo's 15 are -- gets no asset. Nothing went wrong and there is
    nothing in it to migrate; reporting it blocked would be noise.
    """
    source = manifest.get("sources", {}).get("dataflow", {})
    assets = []
    for flow in source.get("items", {}).get("dataflows", []):
        if flow.get("parse_error"):
            assets.append({
                "id": _item_id("dataflow", flow),
                "source": {"type": "fabric_dataflow_unreadable",
                           "name": flow["name"],
                           "reason": flow.get("parse_error", "")},
                "target": {"type": "aidp_unmigrated", "name": flow["name"]},
                "transform_chain": [],
                "depends_on": [],
            })
            continue
        if flow.get("parser") == "unavailable" and flow.get("queries"):
            assets.append({
                "id": _item_id("dataflow", flow),
                "source": {"type": "fabric_dataflow_untranslated",
                           "name": flow["name"],
                           "counted": flow.get("counted", 0)},
                "target": {"type": "aidp_unmigrated", "name": flow["name"]},
                "transform_chain": [],
                "depends_on": [],
            })
            continue
        for query in flow.get("queries", []):
            if query.get("kind") not in ("pipeline", "unread_member"):
                continue        # helpers and parameters are not migrations
            # `unread_member` is here for the opposite reason to `pipeline`:
            # it will never produce a file. It is a member `parse.js` could
            # not read into steps -- a custom M function, a one-expression
            # query, a `Table.Group` written without `let` -- and with no
            # asset it leaves no trace in plan, migrate or verify at all,
            # which is how a complete query came to vanish while the
            # Dataflow around it graded a success. The asset exists so that
            # the refusal is reported; `translate_query` blocks it on sight.
            assets.append({
                "id": f"{_item_id('dataflow', flow)}.{query['name']}",
                "source": {"type": "fabric_dataflow_query",
                           "dataflow": flow["name"], "name": query["name"],
                           "query": query.get("parsed"),
                           "helper": query.get("helper"),
                           # Where a `[BindToDefaultDestination = true]`
                           # query writes lives in the section attribute and
                           # nowhere else, so the asset has to carry it.
                           "section_attrs": query.get("section_attrs", "")},
                "target": {"type": "aidp_pyspark_job"},
                "depends_on": [],
                "actions": ["translate_m_to_pyspark"],
            })
    return assets


def _lakehouse_catalog(manifest: dict) -> dict:
    """{lakehouse workspace item id: display name} -- what a Dataflow needs.

    `migrate/runner.py` has read `plan["lakehouse_catalog"]` since Dataflows
    landed and nothing ever wrote it, so it was always None: every Dataflow
    read and every Dataflow write took the unresolved branch and emitted a
    two-part `<catalog>.<table>` name. A write with no schema lands in
    whatever the session's current database happens to be.

    A Fabric Git export writes a lakehouse GUID and its name down together
    in exactly one place -- a notebook's binding:

        "dependencies": {"lakehouse": {
            "default_lakehouse": "<workspace item id>",
            "default_lakehouse_name": "<display name>", ...}}

    and that id is the same identifier a Dataflow's `lakehouseId`
    navigates by. Both halves are written by Fabric, so this is a reading,
    not an inference.

    The Lakehouse item's own `.platform` is deliberately not used: it
    carries `config.logicalId`, a git-generated identifier, and across the
    3 Lakehouse items available here it equals none of the 29 distinct
    lakehouseIds the Dataflow corpus navigates to. A match there would be a
    coincidence, and spending it as a resolution would put a confident
    lakehouse name on a write that could not be resolved.

    A binding with a GUID and no name contributes nothing: `{guid: guid}`
    would splice the unresolved thing into the schema position of a table
    name and make an unknown look answered.
    """
    catalog = {}
    for nb in _rows(manifest, "notebook", "notebooks"):
        guid = nb.get("default_lakehouse_id")
        name = nb.get("default_lakehouse")
        if not isinstance(guid, str) or not guid.strip():
            continue
        if not isinstance(name, str) or not name.strip() or name.strip() == guid.strip():
            continue
        catalog.setdefault(guid.strip(), name.strip())
    return catalog


# A shortcut in the `Files` section of a lakehouse is a folder of objects.
# It has no schema, no columns and no rows; nothing reads it with a table
# name. AIDP's external-table registration describes a table, so there is
# nothing there to register, and planning one as `aidp_dcat_external_table`
# promised the operator an object the migration cannot produce. What the
# shortcut does have is a location, and this tool already maps that location
# onto OCI Object Storage -- so that is what the target says it is, and the
# artifact says in words that defining a table over the copied objects is a
# decision someone still has to make.
SHORTCUT_TABLE_TARGET = "aidp_dcat_external_table"
SHORTCUT_LOCATION_TARGET = "aidp_object_storage_location"


def _shortcut_tail(shortcut: dict) -> str:
    """`<path>/<name>`, the pair Fabric uses to identify one shortcut.

    `Files/x` and `Tables/x` are two different objects and both are legal
    in one lakehouse; so are `Tables/x` and `Tables/dbo/x`. The id used
    only the name, so each of those pairs collided and `plan` refused the
    whole workspace. `path` is already in the manifest -- it was simply
    not in the id.
    """
    location = "/".join(
        part for part in str(shortcut.get("path", "") or "").split("/") if part)
    name = str(shortcut.get("name", "") or "")
    return f"{location}/{name}" if location else name


def _lakehouse_assets(manifest: dict, namespace: str) -> list:
    out = []
    for lakehouse in _rows(manifest, "lakehouse", "lakehouses"):
        name = lakehouse.get("name", "")
        item = _item_ref("lakehouse", lakehouse)
        out.append({
            "id": _item_id("lakehouse", lakehouse),
            "source": {"type": "fabric_lakehouse", "name": name,
                       "tracking": lakehouse.get("tracking", {}),
                       "shortcut_count": lakehouse.get("shortcut_count"),
                       # Why the count is None, when it is. Without this the
                       # migration report has nothing to say about a
                       # shortcuts file that could not be read.
                       "shortcuts_error": lakehouse.get("shortcuts_error", "")},
            "target": {"type": "aidp_schema", "name": aidp_schema(name),
                       "namespace": namespace},
            "transform_chain": ["create_aidp_schema"],
            "depends_on": [],
        })
        for shortcut in lakehouse.get("shortcuts", []) or []:
            if not isinstance(shortcut, dict):
                continue
            schema = str(shortcut.get("schema", "") or "")
            is_table = is_table_section(shortcut.get("section"))
            if is_table:
                target = {"type": SHORTCUT_TABLE_TARGET,
                          "schema": aidp_schema(name, schema) if name else name,
                          "name": shortcut.get("name", ""),
                          "namespace": namespace}
                chain = ["shortcut_target_to_oci", "create_aidp_external_table"]
            else:
                target = {"type": SHORTCUT_LOCATION_TARGET,
                          "name": shortcut.get("name", ""),
                          "namespace": namespace}
                chain = ["shortcut_target_to_oci"]
            out.append({
                "id": f"lakehouse.shortcut.{item}.{_shortcut_tail(shortcut)}",
                "source": {"type": "fabric_shortcut", "lakehouse": name,
                           "name": shortcut.get("name", ""),
                           "section": shortcut.get("section", ""),
                           # `Tables/sales` says the shortcut is the table
                           # `sales.regional_claims`, and the plan used to
                           # carry only the section: the schema was read,
                           # written to the manifest and then dropped, so
                           # the target came out as `SalesLake
                           # .regional_claims` -- the same name a top-level
                           # `Tables/regional_claims` produces.
                           "path": shortcut.get("path", ""),
                           "schema": schema,
                           "table_name": shortcut.get("table_name") or "",
                           "target_type": shortcut.get("target_type", ""),
                           "target": shortcut.get("target", ""),
                           # S3Compatible records its bucket outside the URI;
                           # without it the translator can only guess.
                           "bucket": shortcut.get("bucket", ""),
                           "external": bool(shortcut.get("external"))},
                "target": target,
                "transform_chain": chain,
                "depends_on": [_item_id("lakehouse", lakehouse)],
            })
    return out


def _pipeline_assets(manifest: dict) -> list:
    pipelines = _rows(manifest, "pipeline", "pipelines")
    by_notebook = _by_display_name(
        _rows(manifest, "notebook", "notebooks"), "notebook")
    by_pipeline = _by_display_name(pipelines, "pipeline")
    return [{
        "id": _item_id("pipeline", p),
        "source": {"type": "fabric_pipeline", "name": p.get("name", ""),
                   "activities": p.get("activities", []),
                   "schedules": p.get("schedules", []),
                   "schedules_error": p.get("schedules_error", ""),
                   # The other error field. The planner carried one and
                   # dropped the other, so a pipeline whose
                   # pipeline-content.json is truncated arrived with
                   # `activities: []` and no reason, and the report told
                   # the operator it was empty. It is not empty; its
                   # definition could not be read, and those need
                   # different actions.
                   "content_error": p.get("content_error", ""),
                   # `@pipeline().parameters.X` in an activity resolves
                   # against this. Dropping it made the translator say the
                   # pipeline declares no such parameter, which is false.
                   "parameters": p.get("parameters", {})},
        "target": {"type": "aidp_job", "name": p.get("name", "")},
        "transform_chain": ["pipeline_to_aidp_job"],
        # An ExecutePipeline activity is an ordering edge as real as a
        # notebook reference: the callee has to exist first. 17 of 89
        # activities across 30 real pipelines are ExecutePipeline, and none
        # of them reached the plan before.
        "depends_on": sorted(
            {_resolve(by_notebook, "notebook", n)
             for n in p.get("notebook_refs", []) or []}
            | {_resolve(by_pipeline, "pipeline", n)
               for n in p.get("pipeline_refs", []) or []}),
    } for p in pipelines]


def _semantic_assets(manifest: dict) -> list:
    return [{
        "id": _item_id("semanticmodel", m),
        "source": {"type": "fabric_semantic_model", "name": m.get("name", "")},
        "target": {"type": "aidp_semantic_model", "name": m.get("name", "")},
        "transform_chain": [],
        "depends_on": [],
    } for m in _rows(manifest, "semanticmodel", "semantic_models")]


def _unreadable_assets(manifest: dict) -> list:
    """One asset per item directory discovery could not read.

    These carry no type, so no transform can run on them and the target is
    `aidp_unmigrated`. They are assets all the same: dropped, they made the
    asset count quietly smaller with nothing anywhere saying why, and that
    is precisely the case where the operator has to be told.
    """
    out = []
    for problem in manifest.get("unreadable_items") or []:
        if not isinstance(problem, dict):
            continue
        path = problem.get("path") or problem.get("name") or ""
        if not path:
            continue
        out.append({
            "id": f"unreadable.{path}",
            "source": {"type": "fabric_unreadable_item",
                       "name": problem.get("name", path),
                       "path": path,
                       "reason": problem.get("reason", "")},
            "target": {"type": "aidp_unmigrated", "name": problem.get("name", path)},
            "transform_chain": [],
            "depends_on": [],
        })
    return out


def _unsupported_assets(manifest: dict) -> list:
    """One asset per discovered item no scanner in this run will look at.

    Same reasoning as `_unreadable_assets`: the item is in the export, it
    is not being migrated, and leaving it out of the plan is how the
    operator came to see a smaller asset count with no explanation. The id
    carries the type because two items of different types can share a
    display name.
    """
    out = []
    for row in manifest.get("unsupported_items") or []:
        if not isinstance(row, dict):
            continue
        name = row.get("name") or row.get("path") or ""
        item_type = row.get("item_type") or "unknown"
        if not name:
            continue
        out.append({
            "id": f"unsupported.{item_type}.{name}",
            "source": {"type": "fabric_unsupported_item", "name": name,
                       "item_type": item_type,
                       "path": row.get("path", ""),
                       "reason": row.get("reason", "")},
            "target": {"type": "aidp_unmigrated", "name": name},
            "transform_chain": [],
            "depends_on": [],
        })
    return out


def _strongly_connected(nodes, edges) -> list:
    """Tarjan's SCC, iterative (no recursion limit). Returns sorted components."""
    index_of, low, on_stack, stack, result = {}, {}, set(), [], []
    counter = 0
    for root in nodes:
        if root in index_of:
            continue
        index_of[root] = low[root] = counter
        counter += 1
        stack.append(root)
        on_stack.add(root)
        work = [(root, iter(edges.get(root, ())))]
        while work:
            node, iterator = work[-1]
            descended = False
            for nxt in iterator:
                if nxt not in index_of:
                    index_of[nxt] = low[nxt] = counter
                    counter += 1
                    stack.append(nxt)
                    on_stack.add(nxt)
                    work.append((nxt, iter(edges.get(nxt, ()))))
                    descended = True
                    break
                if nxt in on_stack:
                    low[node] = min(low[node], index_of[nxt])
            if descended:
                continue
            work.pop()
            if work:
                low[work[-1][0]] = min(low[work[-1][0]], low[node])
            if low[node] == index_of[node]:
                component = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                result.append(sorted(component))
    return result


def _ordered(assets: list):
    """Stable topological sort.

    Returns (ordered_assets, cycle_ids, blocked_ids). Kahn cannot order a node
    that is in a cycle OR that merely depends on one, so the leftovers are
    split: `cycles` are genuine members of a strongly connected component,
    `blocked` are downstream of one. Reporting a blocked node as a cycle would
    send a reviewer hunting for a cycle that does not exist.
    """
    by_id = {a["id"]: a for a in assets}
    dependencies = {i: sorted({d for d in by_id[i]["depends_on"] if d in by_id})
                    for i in by_id}
    dependents: dict = {i: [] for i in by_id}
    indegree = {i: len(dependencies[i]) for i in by_id}
    for node, deps in dependencies.items():
        for dep in deps:
            dependents[dep].append(node)

    ready = sorted(i for i, count in indegree.items() if count == 0)
    order = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for dependent in sorted(dependents[node]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
        ready.sort()

    remaining = set(by_id) - set(order)
    sub_edges = {n: [d for d in dependencies[n] if d in remaining] for n in remaining}
    components = _strongly_connected(sorted(remaining), sub_edges)
    cycle_nodes = {n for component in components if len(component) > 1 for n in component}
    cycle_nodes |= {n for n in remaining if n in sub_edges.get(n, ())}
    cycles = sorted(cycle_nodes)
    blocked = sorted(remaining - cycle_nodes)
    return [by_id[i] for i in order + cycles + blocked], cycles, blocked


# One AIDP schema reached from two different Fabric containers. See
# `_schema_collisions`.
SCHEMA_COLLISION = "NM01_AIDP_SCHEMA_COLLISION"


def _schema_origin(asset: dict):
    """(AIDP schema, the Fabric container it came from) -- or None.

    The container is `(kind, item ref, schema)` with Fabric's default schema
    dropped, which is exactly what `aidp_schema` joins: a Lakehouse's own
    `aidp_schema` asset and a table shortcut in it are one container, a
    Warehouse `dbo` object and a schema-less one are one container, and a
    Lakehouse and a Warehouse that happen to share a name are two.
    """
    source, target = asset.get("source") or {}, asset.get("target") or {}
    kind = source.get("type", "")
    if kind == "fabric_lakehouse":
        item, schema, family = source.get("name", ""), "", "Lakehouse"
    elif kind == "fabric_shortcut":
        if target.get("type") != SHORTCUT_TABLE_TARGET:
            return None
        item, schema, family = (source.get("lakehouse", ""),
                                source.get("schema", ""), "Lakehouse")
    elif kind.startswith("fabric_warehouse_"):
        item, schema, family = (source.get("warehouse", ""),
                                source.get("schema", ""), "Warehouse")
    else:
        return None
    built = target.get("schema") if kind != "fabric_lakehouse" else target.get("name")
    if not built:
        return None
    schema = str(schema or "").strip().strip('[]"`')
    if schema.casefold() in DEFAULT_SCHEMAS:
        schema = ""
    return built, (family, str(item or ""), schema)


def _describe(origin) -> str:
    family, item, schema = origin
    return f"{family} {item!r}" + (f" schema {schema!r}" if schema else "")


def _schema_collisions(assets: list) -> list:
    """AIDP schemas that two distinct Fabric containers both land in.

    AIDP's metastore folds a schema name to lower case silently. MEASURED on
    an AIDP cluster (Spark 3.5.0), 2026-09-30: `CREATE SCHEMA IF NOT
    EXISTS default.R2MixedCase` succeeded, SHOW SCHEMAS listed `r2mixedcase`,
    and a following `CREATE SCHEMA default.r2mixedcase` failed
    [SCHEMA_ALREADY_EXISTS]. So Lakehouse `SalesLake` and Warehouse
    `saleslake` are one AIDP schema, and so are item `AcmeDW_sales` and item
    `AcmeDW` + schema `sales` -- which `aidp_schema` joins to the same string
    before any folding at all. Each pair's tables then share one namespace,
    and a table name both define is one table.

    Reported, not resolved: which container gets the name is the author's
    decision. Every affected asset carries a `flag` in `plan_findings`,
    which `migrate` adds to that asset's findings, and the plan lists each
    collision under `warnings.schema_collisions`.
    """
    by_folded: dict = {}
    for asset in assets:
        found = _schema_origin(asset)
        if found is None:
            continue
        built, origin = found
        entry = by_folded.setdefault(built.lower(), {})
        entry.setdefault(origin, []).append(asset)
    collisions = []
    for folded in sorted(by_folded):
        origins = by_folded[folded]
        if len(origins) < 2:
            continue
        named = sorted(_describe(o) for o in origins)
        detail = (
            f"AIDP schema {folded!r} is reached from {len(named)} different "
            f"Fabric containers ({', '.join(named)}). AIDP folds schema names "
            f"to lower case -- measured on an AIDP cluster (Spark 3.5.0), "
            f"2026-09-30: default.R2MixedCase was created as r2mixedcase and "
            f"a second CREATE SCHEMA default.r2mixedcase failed "
            f"[SCHEMA_ALREADY_EXISTS] -- and `<item>_<schema>` can spell "
            f"another item's name outright, so their tables share one "
            f"schema and a table name both use is one table. Rename one of "
            f"the Fabric items, or choose AIDP schemas by hand, before "
            f"running")
        ids = sorted(a["id"] for group in origins.values() for a in group)
        for group in origins.values():
            for asset in group:
                asset.setdefault("plan_findings", []).append(
                    {"rule": SCHEMA_COLLISION, "detail": detail,
                     "severity": "flag"})
        collisions.append({"aidp_schema": folded, "sources": named,
                           "assets": ids})
    return collisions


def load_supplied_lakehouses(path) -> dict:
    """Parse a `plan --lakehouses` file into {lakehouse GUID: display name}.

    Nothing on any verb mapped a lakehouse GUID to a name before this.
    `plan --catalog` is the AIDP catalog a table name is built under and
    `inventory --tables-csv` lists tables; neither is this, and
    `m_to_pyspark.unresolved_lakehouse` said as much while leaving the
    operator no way to fix it -- its only remedy was "export the whole
    workspace so that binding is present", which the person running the
    tool often cannot do.

    MEASURED on the bundled demo estate: 3 findings cite a lakehouse GUID
    the export does not name, and every one of them is a read or a write
    whose *table* resolved cleanly. A two-part name lands in whatever
    database the session is pointed at, so this is a real answer to give.

    Same file shape as `--tables-csv`, deliberately: `id,name`, header
    folded case- and whitespace-insensitively, and a file that yields
    nothing raises rather than doing nothing quietly -- which is how
    `--tables-csv` shipped broken for a release.

    A row whose name is the GUID again, or is empty, is dropped. `{guid:
    guid}` would splice the unresolved thing into the schema position of a
    table name and make an unknown look answered; `_lakehouse_catalog`
    drops the same shape for the same reason.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"cannot read lakehouse file {path}: {exc}") from exc
    reader = csv.DictReader(text.splitlines())
    columns: dict = {}
    for field in reader.fieldnames or []:
        columns.setdefault((field or "").strip().casefold(), field)
    for required in ("id", "name"):
        if required not in columns:
            raise ValueError(
                f"lakehouse file {path} must have an {required!r} column "
                f"(lakehouse GUID and display name); found: "
                f"{sorted(columns) or 'no header'}")

    def value(row, folded):
        key = columns.get(folded)
        return "" if key is None else (row.get(key) or "").strip()

    supplied: dict = {}
    for row in reader:
        guid, name = value(row, "id"), value(row, "name")
        if guid and name and name != guid:
            supplied[guid] = name
    return supplied


def build_plan(manifest, *, oci_namespace: str = OCI_NAMESPACE_DEFAULT,
               catalog: str = DEFAULT_CATALOG, lakehouses=None) -> dict:
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be a JSON object")
    namespace = _validated_namespace(oci_namespace)
    # Checked for the same reason the namespace is, and with more at stake:
    # the catalog is the only thing that appears in the first position of a
    # table name, so `--catalog a.b` did not produce an odd-looking name, it
    # produced a four-part one pointing at a different place. The rule lives
    # in naming.py because `migrate --catalog` has to apply the same one and
    # a runner importing the planner would be backwards.
    catalog = validated_catalog(catalog)

    assets = (_notebook_assets(manifest)
              + _warehouse_assets(manifest, catalog)
              + _lakehouse_assets(manifest, namespace)
              + _pipeline_assets(manifest)
              + _semantic_assets(manifest)
              + _dataflow_assets(manifest)
              + _unreadable_assets(manifest)
              + _unsupported_assets(manifest))

    seen, duplicates = set(), set()
    for asset in assets:
        if asset["id"] in seen:
            duplicates.add(asset["id"])
        seen.add(asset["id"])
    if duplicates:
        raise ValueError("duplicate asset id(s): " + ", ".join(sorted(duplicates)))

    dangling = sorted({d for a in assets for d in a["depends_on"] if d not in seen})
    for asset in assets:
        asset["depends_on"] = [d for d in asset["depends_on"] if d in seen]

    assets, cycles, blocked = _ordered(assets)
    schema_collisions = _schema_collisions(assets)

    by_target: dict = {}
    for asset in assets:
        key = asset["target"]["type"]
        by_target[key] = by_target.get(key, 0) + 1

    return {
        "plan_id": time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                   + f"-{uuid.uuid4().hex[:12]}",
        "source_workspace": manifest.get("workspace_name"),
        "source_scanned_at": manifest.get("scanned_at"),
        "target_aidp": {"namespace": namespace, "catalog": catalog},
        "summary": {"asset_count": len(assets), "by_target_type": by_target},
        "warnings": {"dangling_depends_on": dangling, "cycles": cycles,
                     "blocked_by_cycle": blocked,
                     "schema_collisions": schema_collisions},
        "resolved_catalog": manifest.get("resolved_catalog", {}),
        # {lakehouse GUID: display name}. Named apart from
        # `resolved_catalog`, which is the tier-ranked *table* catalog and a
        # different shape entirely -- `_read_lakehouse` defends against the
        # two being confused, which is how close they came to it.
        # The export's own readings first, then anything the operator
        # supplied on top. A flag someone typed beats what was read out of
        # a possibly stale or partial export -- the rule `cli._namespace`
        # already states for the namespace.
        "lakehouse_catalog": dict(_lakehouse_catalog(manifest),
                                  **(lakehouses or {})),
        "assets": assets,
    }


def write_plan(plan: dict, out) -> Path:
    path = Path(out)
    write_json_atomic(path, plan)
    return path


def summarize_plan(plan: dict) -> str:
    summary = plan.get("summary", {})
    lines = [
        f"plan {plan.get('plan_id', '?')}",
        f"  workspace:    {plan.get('source_workspace')}",
        f"  total assets: {summary.get('asset_count', 0)}",
        "",
        "  by target type:",
    ]
    # Said here as well as flagged per-artifact at migrate time, because this
    # is the run that chose the placeholder and the cheapest place to fix it.
    if (plan.get("target_aidp") or {}).get("namespace") == PLACEHOLDER:
        lines[3:3] = [
            "",
            f"  no --namespace: every oci:// path will say {PLACEHOLDER}.",
            "  Fine to review, and every artifact carrying it is graded",
            "  REVIEW rather than PASS. Re-plan with --namespace (or set",
            "  OCI_NAMESPACE) before anything here is run.",
        ]
    for key, value in sorted(summary.get("by_target_type", {}).items(),
                             key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"    {value:4d}  {key}")
    warnings = plan.get("warnings", {})
    if warnings.get("dangling_depends_on"):
        # Cap the list. A real estate cross-references notebooks by GUID, and
        # dumping 63 of them buried the rest of the summary and read as a wall
        # of errors. They are all in the plan JSON.
        dangling = warnings["dangling_depends_on"]
        lines.extend([
            "",
            f"  {len(dangling)} dangling depends_on "
            f"(referenced but not in this export):"])
        lines.extend(f"    {d}" for d in dangling[:8])
        if len(dangling) > 8:
            lines.append(f"    ... and {len(dangling) - 8} more "
                         f"(see warnings.dangling_depends_on in the plan)")
    if warnings.get("cycles"):
        lines.extend(["", "  dependency cycle (ordered last, not an error):"])
        lines.extend(f"    {c}" for c in warnings["cycles"])
    if warnings.get("blocked_by_cycle"):
        lines.extend(["", "  blocked by a cycle (not themselves cyclic):"])
        lines.extend(f"    {b}" for b in warnings["blocked_by_cycle"])
    if warnings.get("schema_collisions"):
        lines.extend(["", "  AIDP schema reached from more than one Fabric "
                      "container (AIDP folds case; each asset is flagged "
                      "NM01):"])
        lines.extend(f"    {c['aidp_schema']}: {', '.join(c['sources'])}"
                     for c in warnings["schema_collisions"])
    return "\n".join(lines)
