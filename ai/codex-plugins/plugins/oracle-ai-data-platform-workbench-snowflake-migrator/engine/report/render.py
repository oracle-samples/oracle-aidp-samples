"""Artifacts -> markdown. Pure functions, zero I/O.

Every report states which values are EXACT and which are estimated, and surfaces
anything that halted or was skipped rather than burying it.
"""
from __future__ import annotations

import collections

from snowflake_source.extract.census import secondary_roles_active
from plan.build import kind_before_types, object_kind_block, snapshot_kind
from plan.data_movement import MAINTENANCE_TRAPS, architecture_decision
from snowflake_source.extract.census import secondary_roles_active
from plan.smoke import smoke_verdict
from plan.status import assess_risk, deploy_failure, migration_status

__all__ = ["render_stages", "render_preflight", "render_census", "census_scope",
           "render_maintenance",
           "render_security",
           "render_inventory", "render_ddl_plan", "render_planned_objects",
           "render_soft_clone_summary", "render_catalog", "render_compute",
           "render_summary",
           "render_smoke", "render_data_options", "DATA_OPTIONS_NOTE",
           "architecture_section", "render_generated_jobs"]


def _bytes(n) -> str:
    if not n:
        return "-"
    n = float(n)
    step = 1024.0
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < step:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= step
    return f"{n:.1f} EB"


# How the row counts in an inventory were obtained, keyed by `row_count_mode`.
# One table for INVENTORY.md and SUMMARY.md, so the two cannot disagree about
# what a number is. The label is the Rows column header.
_ROW_COUNT_MODE_TEXT = {
    "metadata": {
        "label": "Rows (metadata)",
        "described": ("Snowflake's maintained row count, read from `SHOW` at "
                      "no cost. It agrees with `COUNT(*)` for a settled "
                      "standard table, but it can lag very recent DML and is "
                      "not maintained for external tables, so it is not a "
                      "verified number."),
        "inventory": ("Row counts are Snowflake's maintained `SHOW` count, "
                      "**not** a verified `COUNT(*)`; views carry none. "
                      "Re-run with `--row-counts exact` for a counted number."),
    },
    "exact": {
        "label": "Rows (exact)",
        "described": ("a `COUNT(*)` per object -- verified, and it executes "
                      "every view to get there."),
        "inventory": ("Row counts are **exact** (in-session `count(*)`), not "
                      "`SHOW` estimates."),
    },
    "none": {
        "label": "Rows",
        "described": "not collected; `--row-counts` was `none`.",
        "inventory": "Row counts were not requested (`--row-counts none`).",
    },
}


def _row_count_mode(inventory: dict | None) -> tuple[str, dict]:
    mode = (inventory or {}).get("row_count_mode", "metadata")
    text = _ROW_COUNT_MODE_TEXT.get(mode) or {
        "label": "Rows", "described": mode,
        "inventory": f"Row-count mode: {mode}."}
    return mode, text


def _row_cell(record: dict) -> str:
    """The Rows cell for one object. A blank always says what it is.

    `None` used to render as ERROR whatever the reason, so in the default
    metadata mode every view -- deliberately not counted -- read as an
    extraction failure to chase.
    """
    rows = record.get("row_count_exact")
    if rows is not None:
        return str(rows)
    source = record.get("row_count_source")
    if source == "error":
        return "ERROR"
    if source == "not_counted":
        return "not counted"
    return "-"


def _unreadable_databases(inv: dict) -> list[str]:
    """Databases in scope that yielded nothing because they were refused.

    The note is already written by the extractor; this finds it so the
    headline can carry the same fact. Matched on the extractor's own
    `database <name>: ` prefix rather than by searching for the name inside
    arbitrary error text, which would also match an object in that database.
    """
    out = []
    for note in inv.get("extraction_notes") or []:
        text = str(note)
        if text.startswith("database ") and ":" in text:
            name = text[len("database "):text.index(":")].strip()
            if name and name not in out:
                out.append(name)
    return out


def _role_cell(session: dict) -> str:
    """The primary role, plus any secondary roles that also served the reads."""
    role = f'`{session.get("ROLE", "-")}`'
    secondary = secondary_roles_active(session.get("SECONDARY_ROLES"))
    if secondary:
        role += (" **+ secondary " + ", ".join(f"`{r}`" for r in secondary)
                 + "**")
    return role


def render_inventory(inv: dict) -> str:
    s = inv.get("session", {})
    # A database in scope that answered nothing is not part of this count,
    # and the scope line may not imply it is.
    refused = _unreadable_databases(inv)
    scope = ", ".join(f"{d} (**NOT READ**)" if d in refused else d
                      for d in inv.get("databases_in_scope") or []) or "-"
    out = ["# Snowflake estate inventory", "",
           f'Probed **{inv.get("probed_at")}** · account `{s.get("A")}` · '
           f'region `{s.get("R")}` · role {_role_cell(s)}',
           f'Databases in scope: {scope}',
           "",
           f'**{inv.get("object_count", 0)} objects** — '
           + " · ".join(f"{k} {v}" for k, v in (inv.get("counts_by_type") or {}).items()),
           ""]
    res = inv.get("mapping_resolution")
    if res:
        sem, ts = res.get("semi_structured") or {}, res.get("timestamp_ntz") or {}
        out += [f'Type mapping: VARIANT → {sem.get("value")} '
                f'({sem.get("source")}) · TIMESTAMP_NTZ → {ts.get("value")} '
                f'({ts.get("source")}) · config defaults '
                f'{"ON" if res.get("enabled") else "OFF"}', ""]
    if refused:
        out += [f'> **{len(refused)} database(s) in scope could not be read '
                f'at all**: {", ".join(f"`{d}`" for d in refused)}. The count '
                f'above covers the rest. This is a privilege result, not an '
                f'empty database — see Extraction notes.', ""]

    if refused:
        out += [f'> **{len(refused)} database(s) in scope could not be read '
                f'at all**: {", ".join(f"`{d}`" for d in refused)}. The count '
                f'above covers the rest. This is a privilege result, not an '
                f'empty database — see Extraction notes.', ""]

    collisions = inv.get("identifier_case_collisions") or {}
    if collisions:
        out += ["## ⚠️ HALT — identifier-case collisions", "",
                "These differ only by case. Snowflake treats them as distinct objects; "
                "Spark folds to lower and would merge them. Resolve before migrating.",
                ""]
        out += [f"- `{k}` ← {', '.join('`' + x + '`' for x in v)}"
                for k, v in collisions.items()] + [""]

    _, mode_text = _row_count_mode(inv)
    records = inv.get("inventory", [])
    out += ["## Objects", "",
            f'{mode_text["inventory"]} Sizes are Snowflake-reported '
            "compressed bytes.", "",
            f'| Object | Type | {mode_text["label"]} | Size | Cols | Case form '
            "| Compatibility |",
            "|---|---|---:|---:|---:|---|---|"]
    for r in records:
        out.append(
            f'| `{r["source_identifier"]}` | {r["object_type"]} '
            f'| {_row_cell(r)} '
            f'| {_bytes((r.get("source_metadata") or {}).get("bytes"))} '
            f'| {len(r.get("columns") or [])} | {r.get("identifier_case_form")} '
            f'| {_compatibility_cell(r)} |')
    # By identity: `r not in registered` compared whole record dicts, an
    # O(N*M) scan at the scale of an estate with many external tables.
    registered = [r for r in records
                  if (kind_before_types(r) or ("",))[0] == "register_in_place"]
    registered_ids = {id(r) for r in registered}
    kind_blocked = [r for r in records
                    if r.get("compatibility_status") != "blocked"
                    and id(r) not in registered_ids
                    and object_kind_block(r) and not snapshot_kind(r)]
    if registered:
        out += ["", "`register in place (<kind>)` -- not copied: the files "
                "are registered as an AIDP table over OCI Object Storage once "
                "they are moved there; EXTERNAL_REGISTRATION.md has the "
                "statements and the prerequisites."]
    if kind_blocked:
        out += ["", "`blocked (<kind>)` -- the column types map, but the "
                "object kind has no plain Delta equivalent, so the plan "
                "refuses it; PLANNED_OBJECTS.md says what to do instead."]
    if any(r.get("compatibility_status") not in ("blocked", "unassessed")
           and snapshot_kind(r) for r in records):
        out += ["", "`table snapshot (<kind>)` -- a dynamic table or "
                "materialized view: its current contents migrate as a Delta "
                "table, and Snowflake's own refresh does not travel. `plan` "
                "decides, per object, whether a refresh job can be generated "
                "from its defining query (`refresh generated` / `refresh NOT "
                "generated: <why>` in PLANNED_OBJECTS.md); `snowmig jobs` "
                "writes it, unscheduled."]

    # Every blank in the Rows column carries its reason. "not counted" and
    # ERROR are different facts, and neither is a zero.
    not_counted = [r for r in records if r.get("row_count_source") == "not_counted"]
    errored = [r for r in records if r.get("row_count_source") == "error"]
    if not_counted or errored:
        out.append("")
    for note in sorted({r.get("row_count_note") for r in not_counted
                        if r.get("row_count_note")}):
        out.append(f"`not counted` -- {note}")
    if errored:
        out += ["", f"**{len(errored)} row count(s) FAILED** -- `ERROR` is an "
                    "error, not a zero:", ""]
        out += [f'- `{r["source_identifier"]}` -- '
                f'{r.get("row_count_note") or "no detail"}' for r in errored]

    notes = inv.get("extraction_notes") or []
    if notes:
        out += ["", "## Extraction notes", "",
                "Objects or scopes that could not be read. Absence below is not "
                "evidence the object does not exist.", ""]
        out += [f"- {n}" for n in notes]
    return "\n".join(out) + "\n"


def render_ddl_plan(ddl: dict) -> str:
    stmts = ddl.get("statements") or []
    out = ["# Target DDL plan", "",
           f"{len(stmts)} statement(s). Nothing has been executed.", ""]
    # LEAD with what the target will refuse. Buried at the bottom this reads
    # as a footnote; it is the reason the whole plan would fail.
    rejected = ddl.get("target_rejected") or []
    if rejected:
        cols = sum(len(r["columns"]) for r in rejected)
        out += [f"## ⛔ HALT — {cols} column(s) in {len(rejected)} table(s) "
                f"use a type the target refuses at `CREATE TABLE`", "",
                "**This plan cannot be created as it stands.** It is a "
                "refusal by the target catalog, not a mapping error, and it "
                "is caught here rather than minutes into a job run.", "",
                "| Target table | Columns | Type |", "|---|---|---|"]
        out += [f'| `{r["target_fqn"]}` | {", ".join(r["columns"])} '
                f'| `{r["type"]}` |' for r in rejected]
        out += [""]
        for remedy in dict.fromkeys(r["remedy"] for r in rejected):
            out += [f"- {remedy}"]
        out += ["", "Fix the **input** and re-run `ddl`. Never edit the "
                "generated SQL to work around this: the next regeneration "
                "overwrites it.", ""]
    for st in stmts:
        out += [f'## `{st["source_identifier"]}` → `{st["target_fqn"]}`', "",
                "```sql", st["sql"], "```", ""]
        rules = st.get("rules_applied") or []
        if rules:
            out += ["Rules applied:", ""]
            out += [f'- `{r["rule_id"]}` — {r["detail"]}' for r in rules] + [""]
        if st.get("omitted_properties"):
            out += ["Properties dropped (no AIDP equivalent): "
                    + ", ".join(f"`{p}`" for p in st["omitted_properties"]), ""]
        if st.get("carried_properties"):
            out += ["Carried into the CREATE TABLE: "
                    + ", ".join(f'`{c["property"]}`'
                                for c in st["carried_properties"])
                    + " (structure notebook; `snowmig deploy --execute` "
                      "cannot carry them, see R13)", ""]
        if st.get("deferred_properties"):
            out += ["Maintenance/layout settings NOT applied: "
                    + ", ".join(f'`{d["property"]}`'
                                for d in st["deferred_properties"]), ""]
        if st.get("warnings"):
            out += ["Warnings:", ""] + [f"- {w}" for w in st["warnings"]] + [""]
    if ddl.get("blocked"):
        out += ["## Blocked — no DDL generated", ""]
        out += [f'- `{b["source_identifier"]}` — {b["reason"]}' for b in ddl["blocked"]]

    carried = [(s["source_identifier"], c) for s in stmts
               for c in (s.get("carried_properties") or [])]
    if carried:
        out += ["", "## Carried into the CREATE TABLE", "",
                "Source settings Delta 3.1 on AIDP accepts (live-verified "
                "2026-09-29) are emitted in the CREATE TABLE above: a plain "
                "clustering key as `CLUSTER BY`, retention as "
                "`delta.deletedFileRetentionDuration` / "
                "`delta.logRetentionDuration` (raised above Delta's 7 / 30 "
                "day defaults only), change tracking or a stream as "
                "`delta.enableChangeDataFeed = true`. The structure notebook "
                "(`01_create_structure`) applies them and reads the "
                "properties back. **`snowmig deploy --execute` cannot**: the "
                "catalog API's table body has no clustering or property "
                "field, so on that path these stay to be applied with "
                "`ALTER TABLE`. Scheduling `OPTIMIZE` / `VACUUM` is still "
                "yours.", "",
                "| Object | Source setting | Value | Carried as |",
                "|---|---|---|---|"]
        out += [f'| `{ident}` | `{c["property"]}` | `{c["value"]}` | '
                f'{c["carried_as"]} |' for ident, c in carried]
        out.append("")

    deferred = [(s["source_identifier"], d) for s in stmts
                for d in (s.get("deferred_properties") or [])]
    if deferred:
        out += ["", "## Maintenance and layout — decisions, NOT applied", "",
                "These source settings have a real AIDP equivalent that this "
                "table cannot take as it stands (the reason is in each row). "
                "They are listed so the choice gets made deliberately rather "
                "than lost: a clustering key that quietly fails to arrive is "
                "a performance regression on the largest tables in the "
                "estate.", "",
                "The deeper difference is *who runs maintenance*. Snowflake "
                "maintains layout and reclaims storage in the background, "
                "un-asked. On AIDP the equivalents exist and are **explicit** "
                "— they have to be scheduled, and `VACUUM` is what bounds how "
                "far time travel can reach. See "
                "`references/maintenance-and-layout.md`.", "",
                "| Object | Source setting | Value | AIDP equivalent | Why not carried |",
                "|---|---|---|---|---|"]
        out += [f'| `{ident}` | `{d["property"]}` | `{d["value"]}` | '
                f'{d["aidp_equivalent"]} | {d.get("reason") or "—"} |'
                for ident, d in deferred]
        out.append("")
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The two headline reports the plugin exists to produce:
#   1. what objects are planned to move (and what cannot, with reasons)
#   2. what was actually created by the soft clone
# ---------------------------------------------------------------------------

_CATEGORY_TITLES = {
    "restriction": "Excluded by a restriction you set",
    "unmapped_type": "Column types with no Delta equivalent",
    "snowflake_only_sql": "View SQL that is Snowflake-only",
    "unsupported_object": "Object kinds with no AIDP equivalent",
    "dependency_not_migrated": "Depends on an object that is not migrating",
    "no_definition": "Definition could not be read",
    "unparseable_sql": "SQL could not be parsed",
    "columns_unread": "Columns could not be read",
    "register_in_place": "Registered in place over OCI Object Storage "
                         "(not copied; see EXTERNAL_REGISTRATION.md)",
}


def _compatibility_cell(rec: dict) -> str:
    """What the planner will do with the object, not only its column types.

    `compatibility_status` is the type mapping. A dynamic table whose types
    all map read `supported` here while the plan, from the same inventory,
    refused it. The kind comes from the planner's own table.
    """
    early = kind_before_types(rec)
    if early:
        # Decided by the kind before the types, exactly as the planner does.
        if early[0] == "register_in_place":
            return f"register in place ({early[1]})"
        return f"blocked ({early[1]})"
    status = rec.get("compatibility_status")
    if status == "blocked":
        return "blocked"
    if status == "unassessed":
        # The column read failed: Cols 0 here is a missing fact, and the
        # plan refuses it under `columns_unread`.
        return "not assessed (columns unread)"
    snapshot = snapshot_kind(rec)
    if snapshot:
        return f"table snapshot ({snapshot})"
    block = object_kind_block(rec)
    if block:
        return f"blocked ({block[0]})"
    return str(status)


def render_planned_objects(plan: dict) -> str:
    s = plan.get("summary", {})
    out = ["# Objects planned to move", "",
           f'Planned **{s.get("can_migrate", 0)}** of '
           f'{s.get("objects_inventoried", 0)} inventoried objects — '
           f'{s.get("tables", 0)} table(s), {s.get("views", 0)} view(s). '
           f'**{s.get("cannot_migrate", 0)}** cannot move.', "",
           f'Bronze mapping: {plan.get("bronze_mapping")}.', "",
           # Coverage caveat, never silent: the count above is of what was
           # EXAMINED, and absence of a census is exactly the bug being fixed.
           census_scope(plan), ""]

    if plan.get("restrictions_applied"):
        out += ["## Restrictions in force", "",
                "Applied at your request, before planning:", ""]
        # With the per-entry counts (an older plan.json has none), an entry
        # that matched nothing is flagged: it was listed here as in force
        # while the object it meant was planned, deployed and copied.
        matches = plan.get("restriction_matches") or {}
        for key, value in plan["restrictions_applied"].items():
            counts = matches.get(key)
            if counts is None or not isinstance(value, list):
                out.append(f"- `{key}`: {value}")
                continue
            out.append(f"- `{key}`: " + ", ".join(
                f"`{e}` (**matched nothing -- check the spelling**)"
                if not counts.get(e) else
                f"`{e}` ({counts[e]} object{'s' if counts[e] != 1 else ''})"
                for e in value))
        out.append("")

    out += ["## Target structure to exist first", ""]
    catalogs = ", ".join(f'`{c}`' for c in plan.get("catalogs_to_create") or [])
    schemas = ", ".join(f'`{a}.{b}`'
                        for a, b in plan.get("schemas_to_create") or [])
    note = plan.get("target_catalog_note")
    if not note:
        # An older plan.json carries no note; its two lines stay as they were.
        out += ["Catalogs (create these, or confirm they exist and are "
                f"INTERNAL): {catalogs}", "",
                f"Schemas the clone will create: {schemas}", ""]
    else:
        # The structure job (S10) and `deploy` both create the plan's names
        # as they stand, and S10 refuses a --target-catalog that is not the
        # plan's catalog. This section once said S10 kept the SOURCE schema
        # (`core`) while the job ran CREATE SCHEMA lake.snowdb_core, and with
        # no prefix it sent the reader to a --target-catalog S10 refuses.
        if plan.get("bronze_catalog_prefix") is None:
            out += [f"Catalogs: the Target column's catalog part ({catalogs}) "
                    "is the source database mirrored, not a catalog to create "
                    "-- under the runbook that name is the EXTERNAL pointer "
                    "at Snowflake.", "",
                    note, "",
                    f"Schemas the plan names: {schemas}. The structure job "
                    "(S10) creates them only under a `--target-catalog` "
                    "equal to the plan's catalog, so re-run `plan "
                    "--bronze-catalog-prefix <the INTERNAL catalog created "
                    "at S4>` and `ddl` before S10.", ""]
        else:
            out += ["Catalogs (create these, or confirm they exist and are "
                    f"INTERNAL): {catalogs}", "",
                    note, "",
                    "Schemas the structure job (S10) creates, and `deploy` "
                    f"too: {schemas}", ""]

    secure = plan.get("secure_views_as_views") or []
    if secure:
        # Before the object list, on purpose: the operator asked for this,
        # and whoever approves the plan has to see what it costs.
        warnings = {w["source_identifier"]: w["warning"]
                    for w in plan.get("table_kind_warnings") or []}
        out += ["## SECURITY WARNING — secure views planned as plain views", "",
                f"`--secure-views as-view` was given, so {len(secure)} Snowflake "
                "SECURE view(s) are planned as PLAIN views. Their definitions "
                "become visible, the secure-view optimizer barrier is gone, "
                "and row-visibility logic keyed to Snowflake roles does not "
                "carry. Restrict access to each before granting it; see "
                "SECURITY.md.", ""]
        out += [f"- `{v}` — {warnings.get(v, '')}" for v in secure]
        out.append("")

    out += ["## Can migrate", "",
            "| Object | Type | Target | Rows | Cols |", "|---|---|---|---:|---:|"]
    for c in plan.get("can_migrate") or []:
        out.append(f'| `{c["source_identifier"]}` | {c["object_type"]} '
                   f'| `{c["target"]}` | {c.get("rows") if c.get("rows") is not None else "-"} '
                   f'| {c.get("columns", "-")} |')
    out.append("")

    # TRANSIENT/TEMPORARY tables planned as permanent Delta tables. An older
    # plan.json carries no list and renders unchanged.
    kinds = plan.get("table_kind_warnings") or []
    if kinds:
        out += ["## Planned, but not as what they were", "",
                "These migrate as permanent Delta tables; confirm each is "
                "meant to persist.", ""]
        out += [f'- `{w["source_identifier"]}` ({w.get("kind")}) — {w["warning"]}'
                for w in kinds]
        out.append("")

    # Dynamic tables and materialized views carried as table snapshots. An
    # older plan.json carries no list and renders unchanged.
    snaps = plan.get("table_snapshots") or []
    if snaps:
        out += ["## Planned as table snapshots", "",
                "Their current contents migrate as Delta tables, read at copy "
                "time. Snowflake refreshed them; on AIDP they change only when "
                "a refresh job runs. `snowmig jobs` generates that job where "
                "the verdict says `refresh generated`, and creates it MANUAL "
                "-- the cadence below is recorded, never applied.", ""]
        for x in snaps:
            cadence = x.get("cadence") or {}
            when = (f'intended cadence: {cadence["source"]} {cadence["value"]}'
                    if cadence else x.get("cadence_note") or "no cadence")
            out.append(f'- `{x["source_identifier"]}` ({x["snapshot_of"]}) '
                       f'-> `{x["target"]}` — {x["verdict"]}; {when}')
        out.append("")

    # Migrating tables that a pipe or task keeps filling in Snowflake. An
    # older plan.json carries no list and renders unchanged.
    loads = plan.get("loads_that_stop") or []
    if loads:
        out += ["## Planned, but loaded by something that does not move", "",
                "The structure and today's rows migrate. The load does not: "
                "after cutover these tables stop receiving rows until each "
                "load is rebuilt on AIDP.", ""]
        out += [f'- `{x["table"]}` <- {x["kind"]} `{x["source_identifier"]}`'
                for x in loads]
        out.append("")

    cannot = plan.get("cannot_migrate") or []
    if cannot:
        out += ["## Cannot migrate", ""]
        grouped = {}
        for c in cannot:
            grouped.setdefault(c["category"], []).append(c)
        for category, items in grouped.items():
            out += [f'### {_CATEGORY_TITLES.get(category, category)} '
                    f'(`{category}`) — {len(items)}', ""]
            out += [f'- `{i["source_identifier"]}` ({i.get("object_type")}) — '
                    f'{i["reason"]}' for i in items]
            out.append("")

    if plan.get("cycles"):
        out += ["## Dependency cycles", "",
                "Excluded from the ordering; they need a human decision rather than "
                "an arbitrary broken edge.", ""]
        out += [f'- {", ".join(f"`{n}`" for n in c)}' for c in plan["cycles"]] + [""]
        # In no cycle, but depending on one: held back for the same reason,
        # and labelled apart so nobody hunts for a cycle it is not in.
        behind = plan.get("blocked_behind_cycle") or {}
        if behind:
            out += ["Blocked behind a cycle (not members; each depends on "
                    "the cycle named):", ""]
            out += [f'- `{n}` behind {", ".join(f"`{m}`" for m in c)}'
                    for n, c in sorted(behind.items())] + [""]

    out += ["## Order of creation", ""]
    unordered = plan.get("views_without_dependency_edge") or []
    if unordered:
        # A view with no edge from either source sorts by size and can land
        # in wave 1 ahead of its base table; the plan must not assert an
        # order it does not have.
        out += ["Objects are ordered by dependency where an edge is known. "
                "These views have no edge from ACCOUNT_USAGE or parsed DDL, are "
                "ordered by size only, and are NOT guaranteed to follow their "
                "base tables: " + ", ".join(f"`{v}`" for v in unordered) + ".",
                ""]
        if plan.get("dependency_warning"):
            out += [plan["dependency_warning"], ""]
    else:
        out += ["Dependencies land before their dependents, so views follow "
                "their base tables.", ""]
    for i, wave in enumerate(plan.get("waves") or [], 1):
        out.append(f'### Wave {i} — {len(wave)} object(s)')
        out += [f'- `{n}` → `{plan.get("target_names", {}).get(n, "?")}`'
                for n in wave]
        out.append("")

    jobs = plan.get("silver_gold_jobs") or []
    if jobs:
        out += ["## Silver and Gold jobs", "",
                "Created as placeholders and **never triggered**. Their content is a "
                "requirement to define with the customer, so no transformation logic "
                "is generated here.", "",
                "| Job | Layer | Reads from | Enabled | Body |", "|---|---|---|---|---|"]
        out += [f'| `{j["name"]}` | {j["layer"]} | `{j["reads_from"]}` '
                f'| {j["enabled"]} | {j["body_status"]} |' for j in jobs]
        out.append("")

    out += architecture_section(plan)

    if plan.get("dependency_source"):
        out += ['---', "",
                f'Lineage source: **{plan["dependency_source"]}** — '
                f'{plan.get("dependency_coverage_note") or ""}']
        if plan.get("dependency_warning"):
            out += ["", plan["dependency_warning"]]
    return "\n".join(out) + "\n"


def _empty_failure_reason(error) -> bool:
    """A connection-test error that names no reason ("Test connection
    failed: " and nothing after it)."""
    text = str(error or "").strip()
    return not text or text.rstrip(":").strip().lower() in (
        "test connection failed", "failed")


def render_catalog(res: dict) -> str:
    """Report on the target catalog. EXTERNAL registers; it copies nothing."""
    name = res.get("catalog")
    if res.get("dry_run"):
        fields = res.get("connection_fields") or []
        # The type is whatever was ASKED FOR. Hardcoding EXTERNAL here made a
        # `--catalog-type standard` dry run claim it would send the Snowflake
        # credential, which is the opposite of what the execute path does.
        catalog_type = str(res.get("catalog_type") or "EXTERNAL").upper()
        is_external = catalog_type == "EXTERNAL"
        headline = (f'Would register an **EXTERNAL** catalog of source type '
                    f'**{res.get("source_type")}**'
                    if is_external else
                    'Would create the **STANDARD** catalog **container**, and '
                    'nothing inside it — its schemas and tables are created '
                    'on AIDP compute by the structure workflow (runbook S10)')
        # The EXTERNAL catalog is the SOURCE pointer (S3); only the INTERNAL
        # one is the migration's target (S4).
        role = "Source" if is_external else "Target"
        out = [f"# {role} catalog `{name}` — DRY RUN", "",
               f'{headline}; **nothing was created**.', ""]
        if not is_external:
            out += ["A STANDARD catalog is managed storage, so it carries no "
                    "`sourceType` and no connection properties: no credential "
                    "is sent for it.", ""]
        elif fields:
            # The NAMES are what a human checks against their deployment, and
            # they carry no secret; the values never appear.
            out += ["Connection properties that would be sent, by name only "
                    "(every value is read from the config file at call time; "
                    "no value, and no secret, is written here):", ""]
            out += [f"- `{field}`" for field in fields]
            out.append("")
        else:
            out += ["No connection config was supplied, so no connection "
                    "properties are listed. `--execute` requires one.", ""]
        out.append("Re-run with `--execute` plus the AIDP target coordinates "
                   "to apply.")
        return "\n".join(out)

    # Only an EXTERNAL catalog has a source. Printing the CLI's default
    # source type beside an INTERNAL one implied a Snowflake link it has not
    # got.
    kind = str(res.get("catalog_type") or "").upper()
    source = (f' (source type `{res.get("source_type", "n/a")}`)'
              if kind == "EXTERNAL" else
              " — managed storage, no source")
    role = "Source" if kind == "EXTERNAL" else "Target"
    out = [f"# {role} catalog `{name}`", "",
           f'- Type: **{res.get("catalog_type")}**{source}',
           f'- Action: **{res.get("action")}**',
           f'- Key: `{res.get("key")}`', ""]

    if res.get("action") == "reused":
        out += ["The catalog already existed and was left exactly as it was "
                "found. Nothing about its connection was changed.", ""]
    elif res.get("verified"):
        out += ["Registered and read back from the server.", ""]
    else:
        out += ["**The create was accepted but the catalog has not appeared "
                "yet.** Catalog creation is asynchronous, so this is "
                "*pending*, not done — list the catalogs again before "
                "treating it as registered.", ""]

    if res.get("container_only"):
        # The container existing must never read as the structure existing.
        note = str(res.get("note") or "")
        out += ["**This is the CONTAINER only — it holds no schemas and no "
                "tables.** " + (note[:1].upper() + note[1:]), ""]
    else:
        out += ["An EXTERNAL catalog is a registered, read-only pointer at the "
                "live Snowflake source. It holds no managed tables of its own "
                "and copies no data, so there is nothing here to keep in "
                "sync.", ""]

    # `--test-connection` is the one step that catches a wrong role or a
    # rotated password before the live run, so its verdict belongs here and
    # not only in the JSON. PENDING is a budget that ran out, not a pass.
    test = res.get("test_connection")
    if test:
        status = str(test.get("status") or "PENDING").upper()
        out += ["## Connection test", ""]
        if status in ("SUCCEEDED", "SUCCESS"):
            out += [f"**{status}** — the API reached Snowflake with the "
                    f"registered connection details.", ""]
        elif status == "FAILED" and _empty_failure_reason(test.get("error")):
            # Live on the validated DataLake: FAILED with an EMPTY reason for
            # a catalog whose credentials the connector proves at S6 -- a
            # known platform issue (runbook S3). "Fix the credential" sent
            # operators chasing a credential that works.
            out += [f"**{status}** — the test returned no reason. Keep the "
                    "registration and continue: discovery (S6) validates the "
                    "connection through the connector. A FAILED test with a "
                    "reason should be investigated.", ""]
        elif status in ("FAILED", "CANCELED", "CANCELLED"):
            out += [f"**{status}**"
                    + (f" — {test.get('error')}" if test.get("error") else "")
                    + ". The registered connection does not work as it "
                      "stands; fix the credential or role before relying on "
                      "this catalog.", ""]
        else:
            out += [f"**{status}** — "
                    + str(test.get("error") or test.get("note")
                          or "the verdict was not read")
                    + ". **PENDING is not a pass.**", ""]
    recorded = res.get("catalogs_recorded") or []
    if len(recorded) > 1:
        # S3 and S4 are two runs of this stage; each keeps its own
        # CATALOG_<name>.md, and this list says what the migration holds.
        out += ["## Catalogs this migration has registered", "",
                "| Catalog | Type | Action | Connection test |",
                "|---|---|---|---|"]
        for c in recorded:
            t = c.get("test_connection") or {}
            out.append(f'| `{c.get("catalog")}` | {c.get("catalog_type")} | '
                       f'{c.get("action")} | {t.get("status") or "—"} |')
        out.append("")
    return "\n".join(out)


def render_soft_clone_summary(plan: dict, res: dict) -> str:
    total = res.get("statement_count", 0)
    scope = res.get("catalog_in_scope")

    if res.get("dry_run"):
        out = ["# Soft clone — DRY RUN", "",
               f"{total} object(s) would be created; **nothing was created**.", "",
               "Re-run with `--execute` plus the AIDP target coordinates to apply."]
    else:
        out = ["# Soft clone summary", "",
               f'Catalog in scope: **{scope}**', "",
               f'Created and **verified {res.get("verified", 0)}/{total}** '
               f'(executed {res.get("executed", 0)}/{total}).', "",
               "Verification probes each object individually, so a statement "
               "that failed inside a batch is reported. `verified` "
               "means the object exists **with the planned column list** — the "
               "DDL is `CREATE IF NOT EXISTS`, so a name that already belonged "
               "to a different table is reported below, not counted here.", "",
               "**These objects are empty — the clone copies structure, no data.**",
               ""]

    by_type = {}
    for c in plan.get("can_migrate") or []:
        if not scope or c["target"].split(".", 1)[0].upper() == str(scope).upper():
            by_type[c["object_type"]] = by_type.get(c["object_type"], 0) + 1
    if by_type:
        out += ["## What the clone covers", ""]
        out += [f"- {v} {k.lower()}(s)" for k, v in sorted(by_type.items())]
        out.append("")

    if res.get("matched_nothing"):
        out += ["## ⛔ Nothing in the plan belongs to this catalog", "",
                f'Every one of the {res.get("out_of_scope_count", 0)} planned '
                f'object(s) targets '
                + ", ".join(f'`{c}`' for c in
                            res.get("out_of_scope_catalogs") or [])
                + f', and this run was pointed at '
                  f'`{res.get("catalog_in_scope")}`. **Nothing was created, '
                  f'and nothing was wrong with the plan** — the catalog name '
                  f'does not match it.', "",
                "Point the run at the catalog the plan targets, or re-run "
                "`plan --bronze-catalog-prefix` to target this one.", ""]
    elif res.get("out_of_scope_count"):
        out += ["## Not deployed in this run", "",
                f'{res["out_of_scope_count"]} object(s) belong to other catalogs: '
                + ", ".join(f'`{c}`' for c in res.get("out_of_scope_catalogs") or []),
                "",
                "Each catalog is a separate, explicitly confirmed run.", ""]

    if res.get("blocked_count"):
        out += [f'{res["blocked_count"]} object(s) were blocked before deployment '
                "and never attempted. See the planned-objects report.", ""]

    if res.get("schemas_not_active"):
        out += ["## Schema still settling — nothing was posted into it", "",
                "These schemas had not reported ACTIVE when the wait ran out. "
                "The plugin posts creates only into an ACTIVE schema, so none "
                "was sent: **every planned name is still available**, and the "
                "objects below are failed only because they were not "
                "attempted. Re-run once the schema reports ACTIVE.", ""]
        out += [f"- `{name}` — {state}"
                for name, state in sorted(res["schemas_not_active"].items())]
        out.append("")

    if res.get("mismatches"):
        out += ["## Structure differs — left as found, NOT cloned", "",
                "These names already existed in AIDP with a different structure. "
                "`CREATE IF NOT EXISTS` left them exactly as they were, so they "
                "have **not been cloned** and nothing of theirs was altered. "
                "Resolve the name collision before re-running.", ""]
        out += [f'- `{m["target_fqn"]}` — {m["reason"]}'
                for m in res["mismatches"]]
        out.append("")

    if res.get("diagnosis_probes"):
        out += ["### Diagnosis probe", "",
                "To tell whether a failure is specific to these names or to "
                "the request, one throwaway object was created with a novel "
                "name in the affected schema. "
                "Deletes are asynchronous, so cleanup is best-effort and the "
                "name is reported either way.", ""]
        out += [f'- `{p["schema"]}.{p["name"]}` — {p["note"]}'
                for p in res["diagnosis_probes"]]
        out.append("")

    if res.get("poisoned_names"):
        out += ["## These names cannot be created in this schema — retry "
                "into a FRESH schema", "",
                "Creates for these names were accepted, but the objects did "
                "not appear, and a later create of the same name in this "
                "schema does not create it either. A **novel** name in the "
                "same schema was created successfully in this run, so the "
                "request and the catalog are in order.", "",
                "**Re-running into this schema will not create these "
                "names.** Change the target schema and run again.", ""]
        out += [f'- `{n}`' for n in res["poisoned_names"]]
        out.append("")

    if res.get("derived_type_drift"):
        out += ["## Created, but the target derived different column types", "",
                "These views **were created** with every planned column, in "
                "order. The target computed some column types from the view "
                "SQL rather than taking the declared ones — Snowflake reports "
                "a view's *declared* output types, and the target derives its "
                "own. Aggregates are where this shows up.", "",
                "**A narrowing can overflow.** Check any column marked below "
                "before anything depends on it.", ""]
        for d in res["derived_type_drift"]:
            out.append(f'- `{d["target_fqn"]}` — {d["reason"]}')
        out.append("")

    if res.get("unverified_structure"):
        out += ["## Structure not verified", "",
                "These exist, but their columns could not be compared against "
                "the plan, so they are not counted as verified.", ""]
        out += [f'- `{u["target_fqn"]}` — {u["reason"]}'
                for u in res["unverified_structure"]]
        out.append("")

    if res.get("properties_not_applied"):
        out += ["## Properties this transport cannot carry", "",
                "The reviewed DDL declares these. The catalog API body has no "
                "field for them, so the objects were created without them; the "
                "plan names the same gap per object (rule R21 for NOT NULL, "
                "R13 for the Delta CLUSTER BY / TBLPROPERTIES).", ""]
        out += [f'- `{p["target_fqn"]}` — {p["property"]}: {p["reason"]}'
                for p in res["properties_not_applied"]]
        out.append("")

    if res.get("description_drift"):
        out += ["## Descriptions not found on the target", "",
                "The structure matches the plan; the descriptions the plan "
                "showed were not found on the created objects.", ""]
        out += [f'- `{d["target_fqn"]}` — {d["reason"]}'
                for d in res["description_drift"]]
        out.append("")

    if res.get("failed"):
        out += ["## Failed verification", ""]
        out += [f'- `{f["target_fqn"]}` — {f["reason"]}' for f in res["failed"]]
        out.append("")
    if res.get("chunk_errors"):
        out += ["## Batch errors", ""] + [f"- {e}" for e in res["chunk_errors"]] + [""]
    if res.get("errors"):
        # Everything the run recorded as an error: a schema that never
        # settled, a listing that failed mid-poll, each refused create. It
        # used to live only in deploy_result.json, so the summary could
        # blame a burned name for what the errors said was the schema.
        out += ["## Errors recorded during the run", ""]
        out += [f"- {e}" for e in res["errors"]]
        out.append("")

    jobs = plan.get("silver_gold_jobs") or []
    if jobs and not res.get("dry_run"):
        out += ["## Silver/Gold jobs", "",
                f"{len(jobs)} job(s) defined in the plan, disabled and never "
                "triggered. Creating them on AIDP is a separate step.", ""]
    return "\n".join(out).rstrip() + "\n"


def render_compute(sizing: dict) -> str:
    out = ["# Compute proposal — warehouses to AIDP clusters", "",
           f'{sizing.get("warehouse_count", 0)} warehouse(s) · '
           f'{sizing.get("total_source_nodes", 0)} Snowflake node-equivalents · '
           f'max {sizing.get("max_concurrent_clusters", 0)} concurrent cluster(s) '
           "to absorb at peak", ""]

    credits = sizing.get("observed_credits_total")
    if credits is not None:
        out += [f'Observed credit consumption: **{credits:,.1f}** '
                f'({sizing.get("credits_basis")})', ""]
    else:
        out += [f'Credit consumption: not observable '
                f'({sizing.get("credits_basis")})', ""]

    cost = sizing.get("cost_model")
    if cost:
        out += ["## Cost", "",
                f'At ${cost["credit_price_usd"]}/credit: '
                f'**${cost["snowflake_monthly_usd"]:,.0f}/month** '
                f'(${cost["snowflake_annual_usd"]:,.0f}/year) on Snowflake, from '
                f'{cost["observed_credits"]:,.1f} credits over '
                f'{cost["observed_days"]} days.', "",
                cost["aidp_comparison"], ""]
    else:
        out += ["## Cost", "", sizing.get("cost_note", ""), ""]

    if sizing.get("cluster_mode") == "existing":
        out += [f'**Mode: existing cluster** (`compute.warehouse_clusters: '
                f'existing`). Every warehouse maps to cluster '
                f'`{sizing.get("existing_cluster_id")}`; no cluster is created '
                f'and it is not resized, so the sizing below is advisory.', ""]
    else:
        out += ["**Mode: new cluster per warehouse** (the default), each "
                "named from the warehouse's base name, created only on "
                "explicit confirmation.", ""]
    out += ["## Proposed clusters", "",
            "| Warehouse | Action | Cluster | Snowflake size | Nodes | Workers "
            "| Autoscale max | OCPU/worker | Shape family |",
            "|---|---|---|---|---:|---:|---:|---:|---|"]
    for p in sizing.get("proposals") or []:
        action = ("use existing" if p.get("cluster_action") == "use_existing"
                  else "create")
        out.append(f'| `{p["name"]}` | {action} | `{p.get("target_cluster")}` '
                   f'| {p["source_size"]} | {p["source_nodes"]} '
                   f'| {p["worker_count"]} | {p["autoscale_max_workers"]} '
                   f'| {p["worker_ocpus"]} | {p["worker_shape_family"]} |')
    out += ["",
            "⚠️ **Shape families require confirmation.** Shape availability varies "
            "by tenancy and region, so no exact SKU is asserted. Confirm against the "
            "target tenancy before provisioning.", ""]

    if sizing.get("blocked"):
        out += ["## Warehouses with no proposal", ""]
        out += [f'- `{b["name"]}` — {b["reason"]}' for b in sizing["blocked"]]
    return "\n".join(out).rstrip() + "\n"


# ---------------------------------------------------------------------------
# The migration summary: one row per object -- tables, views and jobs alike --
# with row count, migration risk, and migration status. Plus a brief
# source -> destination header.
# ---------------------------------------------------------------------------

def _row_count_provenance(inventory: dict | None) -> list[str]:
    """Explain every blank in the Rows column.

    A `-` for a view that was not counted and a `-` for a job that has no rows
    are different facts and must not render identically. A count that FAILED is
    a third thing again, and its reason used to be discarded entirely.
    """
    records = (inventory or {}).get("inventory") or []
    if not records:
        return []
    mode, mode_text = _row_count_mode(inventory)

    out = ["## Row counts", "",
           f'Mode: **{mode}** — {mode_text["described"]}', ""]

    not_counted = [r for r in records if r.get("row_count_source") == "not_counted"]
    errored = [r for r in records if r.get("row_count_source") == "error"]

    if not_counted:
        notes = {r.get("row_count_note") for r in not_counted if r.get("row_count_note")}
        out.append(f"**{len(not_counted)} object(s) have no row count because "
                   f"they were not counted**, not because they are empty:")
        out.append("")
        out += [f'- `{r["source_identifier"]}`' for r in not_counted[:20]]
        if len(not_counted) > 20:
            out.append(f"- …and {len(not_counted) - 20} more")
        out.append("")
        out += [f"Reason: {n}" for n in sorted(notes)]
        out.append("")

    if errored:
        out.append(f"**{len(errored)} row count(s) FAILED** — these blanks are "
                   f"an error, not a zero:")
        out.append("")
        out += [f'- `{r["source_identifier"]}` — {r.get("row_count_note", "no detail")}'
                for r in errored[:20]]
        out.append("")

    return out


def _tick(values) -> str:
    return ", ".join(f"`{v}`" for v in values) or "—"


def render_translation_map(tmap: dict) -> str:
    """Every translation the session made: types, dialect, names, DDL rules."""
    t = tmap.get("totals") or {}
    out = ["# Translation map — everything this session translated", "",
           f'**{t.get("objects", 0)} object(s)**, **{t.get("columns", 0)} '
           f'column(s)** across **{t.get("distinct_source_types", 0)}** '
           f'distinct Snowflake types. Views: **{t.get("views_translated", 0)}** '
           f'dialect-translated · **{t.get("views_verbatim", 0)}** carried '
           f'verbatim · **{t.get("views_refused", 0)}** refused · '
           f'**{t.get("views_blocked_by_kind", 0)}** refused by kind · '
           f'**{t.get("views_without_sql", 0)}** with no SQL captured · '
           f'**{t.get("views_unparseable", 0)}** unparseable. '
           f'**{t.get("table_snapshots", 0)}** migrate as a table snapshot '
           f'(materialized view / dynamic table)'
           + (f', **{t["snapshots_not_planned"]}** more not in the plan'
              if t.get("snapshots_not_planned") else "") + '.', "",
           "Every rule is either an exact rewrite or a refusal: nothing here "
           "is approximated. A refused view is left untouched and listed in "
           "`cannot_migrate` rather than translated into SQL that mostly "
           "works.", ""]
    if t.get("unmapped_columns"):
        out += [f'> ⚠️ **{t["unmapped_columns"]} column(s) have no target '
                f'type** and block their object. See the unmapped rows.', ""]
    kinds = tmap.get("views_blocked_by_kind") or []
    if kinds:
        out += ["Refused by kind, as the plan refuses them -- no dialect rule "
                "was run over these: " + ", ".join(
                    f'`{v["source_identifier"]}` ({v["kind"]})'
                    for v in kinds) + ".", ""]
    snaps = tmap.get("table_snapshots") or []
    if snaps:
        out += ["Migrate as a table snapshot, as the plan migrates them -- "
                "their rows are copied as a table, and their defining query "
                "is translated only for the generated refresh (plan.json, "
                "GENERATED_JOBS.md), so no dialect rule is counted for them "
                "here: " + ", ".join(
                    f'`{v["source_identifier"]}` ({v["kind"]}; '
                    f'{v.get("refresh") or "refresh verdict not in the plan"})'
                    for v in snaps) + ".", ""]

    out += ["## Types", "", "| Snowflake type | → | Spark type | Status | "
            "Columns | Objects |", "|---|---|---|---|---:|---:|"]
    for r in tmap.get("types") or []:
        status = "**unmapped**" if r["status"] == "unmapped" else r["status"]
        out.append(f'| `{r["source_type"]}` | → | '
                   f'{"`" + r["target_type"] + "`" if r["target_type"] else "—"} '
                   f'| {status} | {r["columns"]} | {len(r["objects"])} |')

    out += ["", "## Dialect rules", "",
            "Every rule the translator knows, and what this estate made of "
            "it. `not_encountered` means no view in scope used the construct.",
            "", "| Rule | Construct | Kind | Outcome | Objects |",
            "|---|---|---|---|---|"]
    for r in tmap.get("dialect_rules") or []:
        outcome = {"refused": "**refused**", "applied": "applied"}.get(
            r["outcome"], "not encountered")
        out.append(f'| `{r["rule_id"]}` | {r["construct"]} | {r["status"]} | '
                   f'{outcome} | {_tick(r["objects"])} |')
    refused = [r for r in tmap.get("dialect_rules") or [] if r["refusal_detail"]]
    if refused:
        out += ["", "Why each refusal:", ""]
        out += [f'- `{r["rule_id"]}` — {"; ".join(r["refusal_detail"])}'
                for r in refused]

    out += ["", "## Catalogs and schemas", "",
            "Where each source container lands. The plan maps objects, so "
            "these are read from where the objects' target keys put them.", "",
            "| Kind | Source | → | Target | Renamed |", "|---|---|---|---|---|"]
    out += [f'| catalog | `{c["source"]}` | → | `{c["target"]}` | '
            f'{"yes" if c["renamed"] else "case only"} |'
            for c in tmap.get("catalogs") or []]
    out += [f'| schema | `{c["source"]}` | → | `{c["target"]}` | '
            f'{"yes" if c["renamed"] else "case only"} |'
            for c in tmap.get("schemas") or []]

    names = tmap.get("names") or []
    folded = sum(1 for n in names if n["case_folded"])
    renamed = sum(1 for n in names if n["renamed"])
    out += ["", "## Names", "",
            f"{len(names)} identifier(s): **{folded}** only case-folded "
            f"(AIDP lower-cases identifiers), **{renamed}** mapped to a "
            f"different name.", "",
            "| Source | → | Target | Verdict |", "|---|---|---|---|"]
    out += [f'| `{n["source"]}` | → | '
            f'{"`" + n["target"] + "`" if n["target"] else "—"} | '
            f'{n["verdict"]} |' for n in names]

    out += ["", "## DDL rules", ""]
    if not tmap.get("ddl_ran"):
        out.append("`ddl` has not run in this session, so no DDL rule has "
                   "fired yet.")
    else:
        out += ["| Rule | Times applied |", "|---|---:|"]
        out += [f"| `{k}` | {v} |" for k, v in (tmap.get("ddl_rules") or {}).items()]
    return "\n".join(out).rstrip() + "\n"


def translation_map_section(tmap: dict | None) -> list[str]:
    """The map, condensed, for the summary. Full detail is TRANSLATION_MAP.md."""
    if not tmap:
        return []
    t = tmap.get("totals") or {}
    rules = tmap.get("dialect_rules") or []
    applied = [r["rule_id"] for r in rules if r["outcome"] == "applied"]
    refused = [r["rule_id"] for r in rules if r["outcome"] == "refused"]
    mapped = [r for r in tmap.get("types") or [] if r["status"] == "mapped"]
    unmapped = [r["source_type"] for r in tmap.get("types") or []
                if r["status"] == "unmapped"]
    out = ["## Translation map", "",
           f'{t.get("columns", 0)} column(s) over '
           f'{t.get("distinct_source_types", 0)} Snowflake type(s); views '
           f'{t.get("views_translated", 0)} translated · '
           f'{t.get("views_verbatim", 0)} verbatim · '
           f'{t.get("views_refused", 0)} refused · '
           f'{t.get("views_blocked_by_kind", 0)} refused by kind; '
           f'{t.get("table_snapshots", 0)} table snapshot(s) '
           f'(materialized view / dynamic table).', "",
           f"- Dialect rules applied: {_tick(applied)}",
           f"- Dialect rules that refused a view: {_tick(refused)}",
           "- Type mappings: " + (", ".join(
               f'`{r["source_type"]}`→`{r["target_type"]}` ({r["columns"]})'
               for r in mapped) or "—"),
           f"- Types with no target: {_tick(unmapped)}"]
    renamed = [c for c in (tmap.get("catalogs") or []) + (tmap.get("schemas")
               or []) if c["renamed"]]
    if renamed:
        out.append("- Renamed containers: " + ", ".join(
            f'`{c["source"]}` → `{c["target"]}`' for c in renamed))
    out.append(f'- Objects renamed beyond a case fold: '
               f'{t.get("renamed_objects", 0)} (each listed in the full map)')
    refused = [n["source"] for n in tmap.get("names") or []
               if n["verdict"] == "cannot_migrate: target_key_too_long"]
    if refused:
        out.append(f"- Refused, target key over 255 characters "
                   f"(target_key_too_long): {_tick(refused)}. Names are "
                   f"never shortened; shorten the catalog prefix instead.")
    out += ["", "Full map: `TRANSLATION_MAP.md` / `translation_map.json`.",
            ""]
    return out


def _protection_lost(security: dict | None, ident: str) -> list[str]:
    """What protected `ident` in Snowflake and arrives without it on AIDP,
    from security.json. Empty when there is no security artifact: absence of
    the report is not a clean bill, and the summary says nothing either way.
    """
    lost: list[str] = []
    for e in (security or {}).get("exposures") or []:
        if e.get("object") == ident:
            where = f' on {e["column"]}' if e.get("column") else ""
            lost.append(f'{e.get("policy_kind")} {e.get("policy")}{where}')
    for v in (security or {}).get("secure_views") or []:
        if v.get("object") == ident:
            lost.append("SECURE (definition and row-visibility guarantees)")
    return lost


def render_summary(plan: dict, inventory: dict, deployed: dict | None,
                   target: dict | None, *,
                   translation_map: dict | None = None,
                   resources: dict | None = None,
                   security: dict | None = None) -> str:
    session = (inventory or {}).get("session", {})
    dbs = ", ".join((inventory or {}).get("databases_in_scope") or []) or "-"

    out = ["# Migration summary", "", "## Source → destination", "",
           "| | Source (Snowflake) | Destination (AIDP) |", "|---|---|---|"]
    unset = "*not supplied*"
    if target:
        dest_id = target.get("datalake_ocid", unset)
        dest_ws = target.get("workspace", unset)
        dest_cl = target.get("cluster_id", unset)
        dest_cat = target.get("catalog", unset)
        dest_region = "*derived from the OCID*"
    else:
        dest_id = dest_ws = dest_cl = dest_cat = dest_region = unset
    out += [f'| Account / DataLake | `{session.get("A", "-")}` | `{dest_id}` |',
            f'| Region | `{session.get("R", "-")}` | {dest_region} |',
            f'| Role / workspace | {_role_cell(session)} | `{dest_ws}` |',
            f'| Version / cluster | `{session.get("V", "-")}` | `{dest_cl}` |',
            f'| Scope | {dbs} | catalog `{dest_cat}` |', ""]
    if not target:
        out += ["**No destination was supplied, so none is assumed.** The target "
                "names below are derived from the SOURCE (bronze mirrors it 1:1); "
                "they do not imply that any AIDP catalog, workspace or cluster "
                "exists.", ""]
    out += [f'Mapping: {plan.get("bronze_mapping")}.', ""]

    rows: list[tuple[str, str, str, str, str, str]] = []

    for c in plan.get("can_migrate") or []:
        level, note = assess_risk(c)
        status = migration_status(c["source_identifier"], deployed=deployed)
        failure = deploy_failure(c["source_identifier"], deployed)
        if failure:
            # The failure leads: the planning notes are about an object that
            # does not exist on the target. LOW means the note is only
            # "structure clones cleanly", which a failed create contradicts.
            note = (failure[0].upper() + failure[1:] + "."
                    + ("" if level == "LOW" else " " + note))
            level = "HIGH"
        lost = _protection_lost(security, c["source_identifier"])
        if lost:
            # SECURITY.md rates each of these HIGH; a summary row reading
            # LOW for the same object told its reader the opposite.
            level = "HIGH"
            note = ("Arrives WITHOUT its Snowflake protection: "
                    + "; ".join(lost) + " -- see SECURITY.md. " + note)
        rows.append((c["source_identifier"], c["object_type"],
                     "-" if c.get("rows") is None else f'{c["rows"]:,}',
                     level, status, note))

    for c in plan.get("cannot_migrate") or []:
        level, note = assess_risk(c, blocked=True)
        rows.append((c["source_identifier"], c.get("object_type") or "-", "-",
                     level, "BLOCKED", note))

    for j in plan.get("silver_gold_jobs") or []:
        rows.append((j["name"], "JOB", "-", "LOW",
                     migration_status(j["name"], deployed=deployed),
                     f'{j["layer"]} job: {j["body_status"]} body, disabled and '
                     "never triggered. Content is a requirement to define."))

    out += ["## Objects", "",
            "| Object | Type | Rows | Risk | Migration status | Notes |",
            "|---|---|---:|---|---|---|"]
    out += [f'| `{n}` | {t} | {r} | {lv} | {st} | {note} |'
            for n, t, r, lv, st, note in sorted(rows, key=lambda x: (x[1], x[0]))]
    out.append("")

    status_counts: dict[str, int] = {}
    for _, _, _, _, st, _ in rows:
        status_counts[st] = status_counts.get(st, 0) + 1
    risk_counts: dict[str, int] = {}
    for _, _, _, lv, _, _ in rows:
        risk_counts[lv] = risk_counts.get(lv, 0) + 1

    out += ["## Roll-up", "",
            "By migration status: "
            + " · ".join(f"**{k}** {v}" for k, v in sorted(status_counts.items())),
            "",
            "By risk: "
            + " · ".join(f"**{k}** {v}" for k, v in sorted(risk_counts.items())),
            "",
            "Status vocabulary: `NOT_YET_DONE` → `IN_PROGRESS` → `SHALLOW_CLONE` → "
            "`DATA_CLONE` → `DONE`, or `BLOCKED`.", "",
            "**`DATA_CLONE` and `DONE` are not reported by this summary.** It "
            "reads only the control-plane deploy result -- structure, not rows; "
            "whether rows were copied by the in-AIDP copy jobs "
            "(`snowmig_02_copy_<schema>`, one per schema of the pushed plan; "
            "`snowmig_02_copy_schema` before a plan is pushed) is reported by "
            "`snowmig_03_reconcile` in "
            "`MIGRATION_REPORT.md` / `reconciliation.json`. `SHALLOW_CLONE` "
            "means the object exists in AIDP with its columns; it says nothing "
            "about rows.", ""]

    out += _row_count_provenance(inventory)

    if deployed and not deployed.get("dry_run"):
        out += [f'Deployed against catalog '
                f'`{deployed.get("catalog_in_scope")}`; '
                f'{len(deployed.get("verified_targets") or [])} verified, '
                f'{len(deployed.get("failed_targets") or [])} failed.', ""]
    elif deployed:
        out += ["Last run was a **dry run** — nothing was created.", ""]
    else:
        out += ["No deployment has been attempted yet.", ""]

    out += translation_map_section(translation_map)
    out += architecture_section(plan)
    if resources is not None:
        from report.resources import render_resources_section
        out += render_resources_section(resources)
    return "\n".join(out).rstrip() + "\n"


def render_smoke(result: dict) -> str:
    src, dest = result.get("source", {}), result.get("destination", {})
    verdict = smoke_verdict(result)
    header = {
        "PASS": "Verdict: **PASS**",
        "FAIL": "Verdict: **FAIL**",
        "PARTIAL": ("Verdict: **PARTIAL** — the Snowflake source was checked; the "
                    "AIDP destination was not. This is not a pass."),
    }[verdict]
    out = ["# Smoke test — connectivity and permissions", "", header, "",
           "## Source (Snowflake)", ""]
    if not src.get("reachable"):
        out += [f'**Unreachable.** {src.get("error", "")}', ""]
    else:
        out += [f'Connected as `{src.get("user")}` / role `{src.get("role")}` on '
                f'account `{src.get("account")}` (`{src.get("region")}`)', "",
                "| Check | Result | Detail |", "|---|---|---|"]
        out += [f'| {c["name"]} | {"PASS" if c["ok"] else "FAIL"} | {c["detail"]} |'
                for c in src.get("checks") or []]
        out.append("")

    out += ["## Destination (AIDP)", ""]
    if dest.get("skipped"):
        out += [f'**Skipped.** {dest.get("reason")}', ""]
    else:
        # No cluster is named: the catalog API needs none, which is one of
        # its advantages over the SQL path.
        out += [f'Catalog `{dest.get("catalog")}` — checked through the '
                f'catalog API, which needs no Spark cluster', "",
                "| Check | Result | Detail |", "|---|---|---|"]
        out += [f'| {c["name"]} | {"PASS" if c["ok"] else "FAIL"} | {c["detail"]} |'
                for c in dest.get("checks") or []]
        out += ["",
                f'Write access: **{"verified" if dest.get("write_verified") else "not verified"}** '
                f'— {dest.get("write_note", "")}', ""]
        if dest.get("left_behind"):
            out += ["⚠️ Left behind by the write probe (this plugin never issues "
                    "`DROP`, so remove these yourself): "
                    + ", ".join(f'`{x}`' for x in dest["left_behind"]), ""]
    return "\n".join(out).rstrip() + "\n"


# What data_options.json says in its `note`, and what DATA_MOVEMENT_OPTIONS.md
# opens with. One constant, because the two artifacts of one run disagreed:
# the markdown named the implemented path while a note hard-coded where the
# JSON was written said the plugin "implements no transfer path".
DATA_OPTIONS_NOTE = (
    "Proposal only. The control-plane CLI moves no bytes. One path is "
    "implemented by the data plane: in-AIDP INSERT-SELECT from the EXTERNAL "
    "catalog, run schema by schema by the copy jobs "
    "`snowmig_02_copy_<schema>` (one per schema of the pushed plan; "
    "`snowmig_02_copy_schema` before a plan is pushed). The "
    "other options are not implemented.")


def render_data_options(options: list[dict]) -> str:
    _, headline, rest = DATA_OPTIONS_NOTE.split(". ", 2)
    out = ["# Data-movement options — for you to choose", "",
           f"**{headline}.** {rest} They are the realistic ways data "
           "could move, with the trade-offs and the open unknowns attached, so "
           "the choice is made deliberately.", "",
           "| Option | Catalog | Moves bytes | Phase |", "|---|---|---|---|"]
    for o in options:
        out.append(f'| **{o["id"]}** — {o["name"]} | {o["catalog_type"]} '
                   f'| {"yes" if o["moves_bytes"] else "no"} '
                   f'| {", ".join(o["phase"])} |')
    out.append("")

    for o in options:
        out += [f'## {o["id"]} — {o["name"]}', "",
                f'**Path:** {o["etl"]}', "",
                "**For**", ""]
        out += [f"- {x}" for x in o["pros"]]
        out += ["", "**Against**", ""]
        out += [f"- {x}" for x in o["cons"]]
        out += ["", "**Still unknown**", ""]
        out += [f"- {x}" for x in o["unknowns"]]
        out += ["", f'Status: `{o["status"]}`', ""]

    out += ["---", "",
            "## What happens after you choose", "",
            "`record_choice()` captures the option and the reasoning. It executes "
            "nothing. Every unknown listed against the chosen option should be "
            "resolved by a hand-run test on one representative table before any "
            "tooling is built — the one with the widest impact is whether "
            "`NUMBER(p,s)` survives an unload round trip with exact "
            "precision.", ""]
    return "\n".join(out).rstrip() + "\n"


def architecture_section(plan: dict) -> list[str]:
    """The data-movement architecture options. ALWAYS included, never optional.

    Rendered into every report that describes a migration, whether or not a
    choice has been made and whether or not a destination was supplied. A user
    who gave no instruction still has to be shown what the choices are; a user
    who chose still benefits from seeing what they chose against.
    """
    decision = architecture_decision(plan.get("architecture_choice"))
    out = ["## Data-movement architecture", "",
           "**The control-plane CLI moves no bytes.** Rows move only through "
           "the in-AIDP copy jobs, `snowmig_02_copy_<schema>` (one per schema "
           "of the pushed plan; `snowmig_02_copy_schema` before a plan is "
           "pushed), when the operator runs them; none of the other paths "
           "below is implemented.", "",
           decision["statement"], ""]

    if decision["decided"]:
        chosen = decision["chosen"]
        executed = ("yes" if chosen["executed"]
                    else "no — this plugin executes nothing")
        custom = chosen.get("custom_architecture")
        if custom:
            out += ["### The customer's own architecture", "",
                    f'**{custom["name"]}**', "", custom["description"], "",
                    f'- Recorded by: {chosen["chosen_by"]}',
                    f'- Because: {chosen["rationale"]}',
                    f'- Executed: **{executed}**', "",
                    "Recorded verbatim and **not mapped** to any option below. "
                    "This plugin has not assessed it, so none of the trade-offs, "
                    "costs or unknowns listed against `A1`–`A5` apply to it.", ""]
        else:
            out += [f'- Chosen: **{chosen["id"]}** — {chosen["name"]}',
                    f'- By: {chosen["chosen_by"]}',
                    f'- Because: {chosen["rationale"]}',
                    f'- Executed: **{executed}**', ""]
        if decision["unknowns_outstanding"]:
            out += ["Outstanding unknowns for that choice:", ""]
            out += [f"- {u}" for u in decision["unknowns_outstanding"]] + [""]

    out += ["| Option | Catalog | Moves bytes | Maintenance owner | Handles |",
            "|---|---|---|---|---|"]
    for o in decision["options"]:
        marker = " ✅" if (decision["decided"]
                          and o["id"] == decision["chosen"]["id"]) else ""
        moves = {True: "yes", False: "no", None: "*unknown*"}[o["moves_bytes"]]
        handles = ", ".join(o["handles"]) or "*unknown until described*"
        owner = (o.get("maintenance_ownership") or {}).get("owner")
        owns = {"customer": "**you**", "snowflake": "Snowflake",
                "shared": "whoever writes", "both": "**both**",
                None: "*unknown*"}.get(owner, str(owner))
        out.append(f'| **{o["id"]}**{marker} — {o["name"]} | {o["catalog_type"]} '
                   f'| {moves} | {owns} | {handles} |')
    out += ["",
            "**The maintenance column is a real operating cost, not a "
            "footnote.** Snowflake maintains layout and reclaims storage in "
            "the background; on AIDP, `OPTIMIZE`, `VACUUM`, `ZORDER BY` and "
            "liquid clustering are explicit operations you schedule. So the "
            "choice "
            "below decides *who inherits that work* — federating leaves it "
            "with Snowflake, landing Delta tables transfers it to you on day "
            "one.", "",
            "### What each choice does to maintenance", ""]
    for o in decision["options"]:
        own = o.get("maintenance_ownership") or {}
        applies = own.get("traps_apply")
        if applies is None:
            which = "unknown until the design is described"
        elif not applies:
            which = "**none of the Delta maintenance points below apply**"
        else:
            which = f"all {len(applies)} Delta maintenance points below apply"
        out.append(f'- **{o["id"]}** — {own.get("note", "")} ({which}.)')
    out += ["", "### Three maintenance points to plan for", "",
            "Snowflake handles each of these in the background; on AIDP each "
            "is a decision the customer makes and schedules.", ""]
    for i, trap in enumerate(MAINTENANCE_TRAPS, 1):
        out.append(f'{i}. **{trap["trap"]}** {trap["consequence"]}')
    out += ["",
            "Measured state for this estate — clustering keys, reclustering "
            "credits, churn and retention overrides — is in `MAINTENANCE.md`. "
            "This plugin proposes no cadence and applies nothing.", "",
            "`A6_CUSTOMER_DEFINED` is the open slot: **the eventual design does "
            "not have to be one of the others**, and \"not decided yet\" is a "
            "valid answer that blocks nothing here.", "",
            "Full trade-offs, open unknowns and what each option would take to "
            "build: `references/data-movement-options.md`, or run "
            "`snowmig data-options`.", ""]
    return out


# ---------------------------------------------------------------------------
# Maintenance and layout (item M2).
#
# Snowflake maintains layout and reclaims storage in the background; AIDP has
# the equivalents and runs none of them. This report exists so that difference
# is a decision on the table rather than something discovered in month three.
# ---------------------------------------------------------------------------

def _fmt(value, suffix: str = "") -> str:
    if value is None:
        return "*not measured*"
    if isinstance(value, float):
        return f"{value:,.2f}{suffix}"
    if isinstance(value, int):
        return f"{value:,}{suffix}"
    return f"{value}{suffix}"


def render_maintenance(maint: dict) -> str:
    tables = maint.get("tables") or []
    flagged = [t for t in tables if t.get("signals")]
    acct = maint.get("account_usage") or {}
    ret = (maint.get("retention") or {}).get("account") or {}

    out = ["# Maintenance and layout", "",
           "Snowflake exposes **no `OPTIMIZE` and no `VACUUM`** — it maintains "
           "layout through Automatic Clustering and reclaims storage in the "
           "background. AIDP provides `OPTIMIZE`, `VACUUM`, `ZORDER BY` and "
           "liquid clustering as **explicit operations you schedule**.",
           "",
           "Nothing is lost in the migration. The *responsibility* moves. This "
           "report records what the source does today; **it proposes no "
           "cadence and applies nothing** — that needs the customer's recovery "
           "requirements and query patterns.", "",
           f'Source Time Travel default: **{_fmt(ret.get("data_retention_time_in_days"))} '
           f'day(s)** (set at: {ret.get("set_at", "unknown")}); '
           f'max data extension: {_fmt(ret.get("max_data_extension_time_in_days"))} day(s).',
           ""]

    if not acct.get("readable", True):
        out += ["> **`ACCOUNT_USAGE` was not readable**, so reclustering credits "
                "and DML churn are **not measured** — that is not the same as "
                "zero, and the difference decides whether clustering matters "
                f'here. Reason: `{acct.get("note", "unknown")}`.', ""]

    if not flagged:
        out += ["## Nothing flagged", "",
                f"{len(tables)} table(s) examined; **no maintenance or layout "
                "signal found**. No clustering keys, no Search Optimization, no "
                "change tracking, no table-level retention overrides"
                + (" and no measured churn above the threshold."
                   if acct.get("readable", True)
                   else ", and churn could not be measured."), "",
                "That is a real finding, not an empty section: a lift-and-shift "
                "of this estate inherits no maintenance obligation beyond the "
                "AIDP defaults.", ""]
    else:
        out += [f"## {len(flagged)} of {len(tables)} table(s) need a "
                "maintenance decision", "",
                "A plain clustering key, retention and change tracking are "
                "**carried into the CREATE TABLE by `ddl`** (liquid "
                "`CLUSTER BY`, the Delta retention properties, "
                "`delta.enableChangeDataFeed`) on the structure-notebook "
                "path; `snowmig deploy --execute` cannot carry them, and "
                "`DDL_PLAN.md` names any key that could not be carried. What "
                "stays yours: **`OPTIMIZE` and `VACUUM` are applied by "
                "nobody** until you schedule them.", "",
                "| Table | Cluster key | Auto-cluster | Recluster credits | "
                "Rows rewritten | Retention (days) |",
                "|---|---|---|---:|---:|---|"]
        for t in flagged:
            rec, churn = t["reclustering"], t["dml_churn"]
            retention = _fmt(t.get("retention_days"))
            if t.get("retention_set_at") == "table":
                retention += f' *(table override; schema default ' \
                             f'{_fmt(t.get("retention_inherited_value"))})*'
            out.append(
                f'| `{t["source_identifier"]}` | `{t["cluster_by"] or "—"}` | '
                f'{"ON" if t["automatic_clustering"] else "off"} | '
                f'{_fmt(rec.get("credits")) if rec.get("measured") else "*not measured*"} | '
                f'{_fmt(churn.get("rows_rewritten")) if churn.get("measured") else "*not measured*"} | '
                f'{retention} |')
        out.append("")

        out += ["### What each signal will require on AIDP", ""]
        for t in flagged:
            out.append(f'**`{t["source_identifier"]}`**')
            out.append("")
            for s in t["signals"]:
                equivalent = s.get("aidp_equivalent") or "**no equivalent**"
                out.append(f'- {s["signal"]} — {s["detail"]}. '
                           f'AIDP: {equivalent}; requires {s["aidp_requires"]}.')
            out.append("")

    out += ["## Three things to settle before anyone commits", "",
            "1. **On Delta, `VACUUM` is what bounds time travel.** On Snowflake "
            "retention and storage reclamation are independent and automatic. A "
            "customer accustomed to reclaiming storage freely will delete their own "
            "recovery window.",
            "2. **`OPTIMIZE` increases storage until `VACUUM` runs.** It leaves "
            "the old files behind until retention expires, so compaction without "
            "reclamation is a cost regression.",
            "3. **Maintenance is scheduled.** Every `OPTIMIZE`/`VACUUM` runs "
            "as a scheduled AIDP Job, which the customer plans and owns.", ""]

    gaps = maint.get("no_equivalent") or []
    if gaps:
        out += ["## Capabilities with no AIDP equivalent", "",
                "Listed here so each one is planned for before cutover.", ""]
        for g in gaps:
            out += [f'**{g["capability"]}** — {g["snowflake"]}',
                    "", f'No equivalent: {g["impact"]}', ""]

    if maint.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in maint["unreadable"]] + [""]

    probing = ("on" if maint.get("table_parameters_probed") else
               "off — inferred from effective values, to avoid one query per table")
    out += ["---", "",
            f'History window: {maint.get("history_days")} day(s). '
            f'Per-table parameter probing: {probing}.']
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# The census: everything that is NOT a table or a view.
#
# `assess` looked at tables and views only, so "7 of 7 objects can move" was
# true of what had been examined and overstated coverage of the estate. The
# scope statement below is carried into every report that states coverage.
# ---------------------------------------------------------------------------

_NO_CENSUS_SCOPE = (
    "**Scope: tables and views only.** No census of the rest of the estate was "
    "run, so this count is not the size of the estate — procedures, UDFs, "
    "tasks, streams, materialized and dynamic tables, stages, pipes, sequences, "
    "file formats, alerts, secrets, network rules, Streamlit apps, notebooks "
    "and services, and the account's shares, roles, network policies, "
    "applications, compute pools and replication/failover groups, were not "
    "examined. Run `assess` with the "
    "census enabled to find out what else is there."
)


def census_scope(plan_or_inventory: dict) -> str:
    """The one-line coverage caveat. Never silent: absence is the bug."""
    census = (plan_or_inventory or {}).get("census")
    if not census:
        return _NO_CENSUS_SCOPE
    return census.get("scope_statement") or _NO_CENSUS_SCOPE


# `.title()` turns UDTF into "Udtf". Acronyms get spelled the way the source
# spells them; everything else keeps the generic rule.
_CENSUS_KIND_LABELS = {"UDTF": "UDTF", "STREAMLIT": "Streamlit"}


def _census_kind_label(kind: str) -> str:
    return _CENSUS_KIND_LABELS.get(kind, kind.replace("_", " ").title())


def render_census(census: dict) -> str:
    objects = census.get("objects") or []
    kinds = census.get("kinds") or {}

    out = ["# Estate census — what is not a table or a view", "",
           census.get("scope_statement", ""), "",
           "**None of these migrate as objects, and no procedure or UDF "
           "equivalent is generated.** Dynamic tables and materialized views "
           "migrate as table snapshots, and `snowmig jobs` generates MANUAL "
           "task and refresh jobs where a translation is exact. The rest are "
           "code, schedulers and storage definitions rather than structure. Each entry names the AIDP capability that "
           "would carry the workload — a pointer, not a promise: a "
           "plausible-but-wrong procedure translation is worse than an honest "
           "gap.", ""]

    if census.get("visibility_note"):
        out += [census["visibility_note"], ""]

    if kinds:
        out += ["## Counts by kind", "",
                "| Kind | Count | Read | Scope |", "|---|---:|---|---|"]
        for kind, info in sorted(kinds.items()):
            count = info.get("count")
            if info.get("capped"):
                # Answered, but truncated: "yes" would call it complete.
                read = "**capped** — SHOW row limit reached; lower bound"
            elif info.get("readable"):
                read = "yes" if count else "yes (0 visible; lower bound)"
            elif info.get("unread") == "partial":
                # A real count that is also incomplete. Calling this denied
                # would contradict the objects listed below it.
                missing = ", ".join(info.get("denied_databases") or [])
                read = f"**partial** — denied in {missing}; lower bound"
            elif info.get("unread") == "degraded":
                # The rows were read and counted under another kind. Saying
                # "not visible" would be a different, and false, claim.
                read = "**not distinguishable**"
            else:
                read = "**not visible to this role**"
            out.append(f'| {_census_kind_label(kind)} | '
                       f'{count if count is not None else "*not measured*"} | '
                       f'{read} | {info.get("scope") or "database"} |')
        out += ["", "Scope says where the read was aimed: a *database* kind is "
                "asked for once per database in scope, an *account* kind once "
                "for the whole account. A kind the role cannot read reports "
                "*not visible to this role* — never 0, because *we could not "
                "look* and *there are none* lead to opposite decisions.", ""]

    by_effort = census.get("by_effort") or {}
    if by_effort:
        out += ["## Rewrite effort, by triage band", "",
                " · ".join(f"**{k}** {v}" for k, v in sorted(by_effort.items())),
                "", "A band, not an estimate: it says which pile an object "
                "belongs in.", ""]

    by_lang = census.get("by_language") or {}
    if by_lang:
        out += ["Handler languages: "
                + " · ".join(f"**{k}** {v}" for k, v in sorted(by_lang.items())),
                ""]

    if objects:
        out += ["## Every object, and what it would take", "",
                "| Object | Kind | Detail | Language | Effort |",
                "|---|---|---|---|---|"]
        for o in sorted(objects, key=lambda x: (x["kind"], x["source_identifier"])):
            out.append(f'| `{o["source_identifier"]}` | {o["kind"]} | '
                       f'{o.get("detail") or "—"} | {o.get("language") or "—"} | '
                       f'{o.get("effort") or "—"} |')
        out.append("")

        out += ["## Why each kind cannot move, and where it would go", ""]
        # One paragraph per DISTINCT VERDICT, not per kind. `refine` gives
        # objects of one kind different reasons -- an internal stage needs
        # its files unloaded, an external one does not; an inbound share is
        # someone else's data, an outbound one is a live consumer contract.
        # Keeping the first per kind covered the second case with the
        # first's text, in the section a reader goes to for the verdict.
        seen: set[tuple[str, str]] = set()
        variants: dict[str, int] = collections.Counter(
            (o["kind"], o["reason"]) for o in objects)
        kinds_with_variants = {k for (k, _r), n in variants.items()
                               if sum(1 for (k2, _) in variants if k2 == k) > 1}
        for o in objects:
            key = (o["kind"], o["reason"])
            if key in seen:
                continue
            seen.add(key)
            heading = f'**{o["kind"]}**'
            if o["kind"] in kinds_with_variants:
                # Say which objects this paragraph is about, or two STAGE
                # paragraphs are indistinguishable.
                members = [m["source_identifier"] for m in objects
                           if (m["kind"], m["reason"]) == key]
                shown = ", ".join(f"`{m}`" for m in members[:4])
                if len(members) > 4:
                    shown += f" and {len(members) - 4} more"
                heading += f" ({shown})"
            out += [f'{heading} — {o["reason"]}', ""]
            if o.get("aidp_path"):
                out += [f'AIDP path: {o["aidp_path"]}', ""]

    if census.get("unreadable"):
        out += ["## Could not be read", "",
                "These counts are a floor, not a total.", ""]
        out += [f"- {n}" for n in census["unreadable"]] + [""]

    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Security posture. The only report here with an exposure consequence, so it
# leads with the finding rather than with the inventory.
# ---------------------------------------------------------------------------

def render_security(sec: dict) -> str:
    count = sec.get("exposure_count")
    exposures = sec.get("exposures") or []
    secure_views = sec.get("secure_views") or []

    out = ["# Security posture — what protects the data, and what arrives "
           "without it", "", sec["statement"], ""]

    if count is None:
        out += ["> The check that matters could not run. Everything below is "
                "partial, and **absence of a finding here is not evidence of "
                "absence**.", ""]
    elif count or secure_views:
        out += ["**This protection has to be re-created on AIDP.** The "
                "objects below are created on AIDP without these Snowflake "
                "policies — the clone succeeds, and the protection does not "
                "come with it. Until equivalent controls are in place, anyone "
                "who can read the target table sees the data these policies "
                "restrict on Snowflake.", ""]

    live = sec.get("live_attachments") or {}
    if sec.get("attachment_source"):
        out += [f'Attachment source: `{sec["attachment_source"]}`.', ""]
    if live.get("attempted"):
        if live.get("failed"):
            out += [f'> **{len(live["failed"])} of {live["objects"]} object(s) '
                    "could not be read directly.** Nothing in this report is a "
                    "verdict about them; for those, only the account view (up "
                    "to ~2 h behind) was read. They are listed under *Could "
                    "not be read* below.", ""]
        else:
            out += [f'Every one of the {live["objects"]} in-scope object(s) '
                    "was read directly, so this is current rather than "
                    "subject to the ~2 h `ACCOUNT_USAGE` lag.", ""]
    elif live.get("reason"):
        out += [f'> **Attachments were not read per object.** '
                f'{live["reason"]}', ""]

    if exposures:
        out += ["## Policies that do not travel", "",
                "| Object | Column | Policy | Kind | Seen by | Severity |",
                "|---|---|---|---|---|---|"]
        for e in exposures:
            seen = ("per-object read" if e.get("source") == "live"
                    else "stale account view")
            if e.get("needs_confirmation"):
                seen += " — **confirm**"
            out.append(f'| `{e["object"]}` | '
                       f'{("`" + e["column"] + "`") if e.get("column") else "*whole table*"} | '
                       f'`{e["policy"]}` | {e["policy_kind"]} | {seen} '
                       f'| **{e["severity"]}** |')
        out.append("")
        flagged = [e for e in exposures if e.get("needs_confirmation")]
        if flagged:
            out += [flagged[0]["needs_confirmation"], ""]
        seen: set[str] = set()
        for e in exposures:
            if e["policy_kind"] in seen:
                continue
            seen.add(e["policy_kind"])
            out += [f'**{e["policy_kind"]}** — {e["consequence"]}', "",
                    e["aidp_path"], ""]

    if secure_views:
        out += ["## Secure views", "",
                "| View | Severity |", "|---|---|"]
        out += [f'| `{v["object"]}` | **{v["severity"]}** |' for v in secure_views]
        out += ["", secure_views[0]["consequence"], "",
                secure_views[0]["aidp_path"], ""]

    pol = sec.get("policies") or {}
    if pol:
        unattached = sec.get("policies_defined_without_attachment") or 0
        if unattached:
            lead = (f"> **Defined, but not seen attached.** {unattached} policy "
                    "object(s) exist and `ACCOUNT_USAGE.POLICY_REFERENCES` "
                    "(up to ~2 h stale) shows no attachment to anything. Do "
                    "not read the table below as an all-clear.")
        else:
            lead = ("Defined is not the same as attached — an unattached "
                    "policy protects nothing, and an attached one is listed "
                    "above.")
        out += ["## Policy objects defined in the source", "", lead, "",
                "| Kind | Count | Read |", "|---|---:|---|"]
        for label, key in (("Masking", "masking"),
                           ("Row access", "row_access"),
                           ("Aggregation", "aggregation"),
                           ("Projection", "projection"),
                           ("Tags", "tags")):
            # Three different facts, three different cells. A kind that was
            # never asked for, a kind the role cannot see, and a kind that
            # answered zero are not interchangeable, and only the last one is
            # a zero.
            if key not in pol:
                out.append(f"| {label} | *not enumerated* | **not asked** |")
                continue
            info = pol.get(key) or {}
            c = info.get("count")
            if not info.get("readable"):
                out.append(f"| {label} | *not visible to this role* "
                           f"| **denied** |")
                continue
            read = ("**capped** — SHOW row limit reached; lower bound"
                    if info.get("capped") else "yes")
            out.append(f'| {label} | {c if c is not None else "*not measured*"} '
                       f"| {read} |")
        out.append("")
        missing = [label for label, key in (("aggregation", "aggregation"),
                                            ("projection", "projection"))
                   if key not in pol]
        if missing:
            out += [f'> This artefact predates {" and ".join(missing)} policy '
                    "enumeration, so those kinds were never asked for. Re-run "
                    "`security` before treating the statement above as "
                    "covering them.", ""]

    # A tag count with no attachment list is a number with no verdict, so the
    # attachments get their own section with the same unreadable handling and
    # the same latency caveat as the policy attachments above.
    tags = sec.get("tag_references")
    if tags is not None:
        out += ["## Tag attachments", ""]
        if not tags.get("measured"):
            out += [f'**Not measured** — {tags.get("note", "unknown")}. '
                    "Whether any migrated object carries a classification tag "
                    "is UNKNOWN, which is not the same as none.", ""]
        else:
            attached = tags.get("attachments") or []
            out += [f'Source: `{tags.get("source", "-")}`.', ""]
            if attached:
                out += [f"{len(attached)} tag attachment(s) on objects being "
                        "migrated. **No tag is recreated on the target.**", "",
                        "| Object | Column | Tag | Value |",
                        "|---|---|---|---|"]
                for a in attached:
                    col = (f'`{a["column"]}`' if a.get("column")
                           else "*whole object*")
                    out.append(f'| `{a["object"]}` | {col} '
                               f'| `{a.get("tag")}` | {a.get("value") or "-"} |')
                out += ["", attached[0].get("consequence", ""), "",
                        attached[0].get("aidp_path", ""), ""]
            else:
                out += ["No tag is attached to anything being migrated, as of "
                        "the lag named above.", ""]
            if tags.get("out_of_scope"):
                out += [f'{tags["out_of_scope"]} tag attachment(s) exist on '
                        "objects outside this migration. Context only.", ""]

    grants = sec.get("grants") or {}
    if grants.get("measured"):
        by_obj = grants.get("by_object") or {}
        out += ["## Who can read what today", "",
                f"{len(by_obj)} in-scope object(s) carry explicit grants. "
                "**No grant is replayed on the target** — AIDP roles and "
                "per-resource permissions are a separate model, so access has "
                "to be re-granted deliberately rather than copied.", ""]
        requested = grants.get("classes_requested") or []
        if requested:
            out += ["Object classes asked for in "
                    "`ACCOUNT_USAGE.GRANTS_TO_ROLES`: "
                    + ", ".join(f"`{c}`" for c in requested)
                    + ". A class that is absent from the table below was "
                    "asked for and returned nothing; a class absent from this "
                    "list was never asked for.", ""]
        by_class = grants.get("by_class") or {}
        if by_class:
            out += ["| Granted on | Privileges | Objects | Roles |",
                    "|---|---:|---:|---|"]
            for cls, info in sorted(by_class.items()):
                roles = info.get("roles") or []
                shown = ", ".join(f"`{r}`" for r in roles[:6])
                if len(roles) > 6:
                    shown += f" …and {len(roles) - 6} more"
                out.append(f'| {cls} | {info.get("grants", 0)} '
                           f'| {info.get("objects", 0)} | {shown or "-"} |')
            out.append("")
        if by_obj:
            out += ["| Object | Roles |", "|---|---|"]
            for ident, entries in sorted(by_obj.items()):
                roles = sorted({e["role"] for e in entries})
                shown = ", ".join(f"`{r}`" for r in roles[:6])
                if len(roles) > 6:
                    shown += f" …and {len(roles) - 6} more"
                out.append(f"| `{ident}` | {shown} |")
            out.append("")
    else:
        out += ["## Who can read what today", "",
                f'**Not measured** — {grants.get("note", "unknown")}. So the '
                "target cannot be checked against the source's access model.",
                ""]

    if sec.get("policy_references_out_of_scope"):
        out += [f'{sec["policy_references_out_of_scope"]} policy attachment(s) '
                "exist on objects outside this migration. Context only; this "
                "migration does not affect them.", ""]

    if sec.get("unreadable"):
        out += ["## Could not be read", ""]
        out += [f"- {n}" for n in sec["unreadable"]] + [""]

    out += ["---", "",
           "This plugin **changes nothing** here and generates no equivalent. "
           "On AIDP the equivalent is a restricted view plus ontology "
           "sensitivity classification granted per role, which is a design "
           "decision rather than a translation."]
    return "\n".join(out) + "\n"


# ---------------------------------------------------------------------------
# Pre-flight: what WOULD happen, stated before the first write.
#
# Required behaviour, not a convenience. Once both ends are known and before
# anything is created, every source -> destination mapping is on the page. A
# migration that begins without the user having seen this is one they did not
# actually approve.
# ---------------------------------------------------------------------------

def render_preflight(plan: dict, *, source: dict | None = None,
                     target: dict | None = None) -> str:
    can = plan.get("can_migrate") or []
    cannot = plan.get("cannot_migrate") or []
    schemas = plan.get("schemas_to_create") or []
    catalogs = plan.get("catalogs_to_create") or []
    jobs = plan.get("silver_gold_jobs") or []
    src = source or {}

    out = ["# Pre-flight — what this migration would do", ""]

    if target is None:
        out += ["> **No AIDP target was supplied, so nothing will be created.** "
                "Everything below is what *would* happen once the datalake "
                "OCID, workspace, cluster and catalog are given.", ""]
    else:
        out += ["Read this before approving. Nothing has been created yet.", ""]

    out += ["## Source → destination", "",
            "| | Source (Snowflake) | Destination (AIDP) |", "|---|---|---|"]
    unset = "*not supplied*"
    out += [
        f'| Account / DataLake | `{src.get("account", unset)}` | '
        f'`{(target or {}).get("datalake_ocid", unset)}` |',
        f'| Region | `{src.get("region", unset)}` | *derived from the OCID* |',
        f'| Role / workspace | `{src.get("role", unset)}` | '
        f'`{(target or {}).get("workspace", unset)}` |',
        f'| Scope / catalog | {", ".join(src.get("databases") or []) or unset} | '
        f'`{(target or {}).get("catalog", unset)}` |', ""]

    out += ["## Naming", "",
            "**Destination names are lower-cased.** AIDP folds identifiers, so "
            "a schema created as `TEST_DB` is stored as `test_db`. The targets "
            "below are the names the destination will really use, so the "
            "plan shows exactly what you will see on AIDP.", "",
            "Two source objects whose names differ only by case therefore fold "
            "into one, and the run **halts** rather than merging them.", ""]

    out += [f'## {len(can)} object(s) that would be created', ""]
    if can:
        out += ["| Source | | Destination | Type | Rows | Cols |",
                "|---|---|---|---|---:|---:|"]
        for c in can:
            rows = c.get("rows")
            out.append(
                f'| `{c["source_identifier"]}` | → | `{c["target"]}` | '
                f'{c.get("object_type", "?")} | '
                f'{rows if rows is not None else "—"} | '
                f'{c.get("columns", "—")} |')
        out.append("")

    if catalogs or schemas:
        parts = []
        if catalogs:
            parts.append(f'{len(catalogs)} catalog(s): '
                         + ", ".join(f"`{c}`" for c in catalogs))
        if schemas:
            parts.append(f'{len(schemas)} schema(s): '
                         + ", ".join(f"`{a}.{b}`" for a, b in schemas))
        out += ["## Structure that would be created first", "",
                " · ".join(parts), ""]

    out += ["## What would NOT happen", "",
            "- **No data moves.** Every table arrives with its columns and "
            "**zero rows**. This is a structural clone.",
            "- **Nothing is written to Snowflake.** The source is **read-only**, "
            "enforced at the transport, whatever the credential allows.",
            "- **Nothing existing is replaced or dropped.** An object that "
            "already exists with a different structure is reported and left "
            "exactly as found.", ""]
    if jobs:
        out.append(f"- **{len(jobs)} Silver/Gold job stub(s)** are defined, "
                   f"disabled and **never triggered**. Their bodies are a "
                   f"requirement still to define.")
        out.append("")

    if cannot:
        out += [f'## {len(cannot)} object(s) that would NOT be created', "",
                "| Object | Type | Why |", "|---|---|---|"]
        out += [f'| `{c["source_identifier"]}` | {c.get("object_type", "?")} | '
                f'{c.get("reason", "?")} |' for c in cannot]
        out.append("")

    out += ["---", "",
            "Nothing above has been executed. `deploy --execute` with the "
            "target coordinates is what applies it."]
    return "\n".join(out) + "\n"


def render_stages(board: dict) -> str:
    """The stage board as a table. Read the run before executing it."""
    rows = board.get("stages") or []
    out = ["# Stages — what runs, what has run, what it found", "",
           f'Artifacts read from `{board.get("out_dir")}`. This board makes no '
           f'decisions and touches nothing.', "",
           "**Seven stages write to AIDP. Three from here: `provision`** (workspace, "
           "cluster, scripts, jobs), **`catalog`** (registers the target "
           "catalog) **and `deploy`** (creates schemas, tables and views). "
           "All three are a dry run unless `--execute` is passed with the "
           "target coordinates. **Two AIDP workflows write too, when a human "
           "runs them: `structure-workflow`** (S10, creates the structure) "
           "**and `copy-workflow`** (S11, copies rows — registered, never run "
           "by the migrator). **A workflow has no dry run**: `run` takes no "
           "`--execute`, so invoking it IS the write. **`publish`** copies the finished report into "
           "the workspace and **`teardown`**, destructive, stops (or deletes) the clusters "
           "this migration allocated, both dry runs unless `--execute`. "
           "Every other stage is read-only. The one further write is "
           "`smoke --write-probe --execute`, which creates one probe schema "
           "and removes it again; `--write-probe` alone is a dry run. "
           "`notebook --upload` sends nothing: without `--execute` it is a "
           "dry run, with `--execute` it is refused; the structure is "
           "created by `run --job snowmig_01_structure` (S10).", "",
           "| Stage | Needs | Runs on | Status | What it found |",
           "|---|---|---|---|---|"]
    for r in rows:
        mark = " ⚠️" if r.get("attention") else ""
        writes = " **(writes)**" if r.get("writes") else ""
        out.append(f'| `{r["stage"]}`{writes} | {r["needs"]} | '
                   f'{r.get("runs_on", "—")} | {r["status"]}{mark} | '
                   f'{r["found"]} |')
    out.append("")

    attention = board.get("needs_attention") or []
    if attention:
        out += ["## Needs attention", "",
                "These stages found something, or could not look. A stage that "
                "**could not look is flagged, never shown as clean** — "
                "\"0 found\" and \"we could not read it\" are opposite "
                "findings.", ""]
        for r in rows:
            if r.get("attention"):
                out.append(f'- **`{r["stage"]}`** — {r["found"]}')
        out.append("")

    from plan.status import pipeline_status
    st = pipeline_status(board)
    out += ["## What can run now", "",
            f'**{len(st["complete"])} of {st["total"]} phase(s) complete.** '
            f'Unblocked now: {", ".join(f"`{u}`" for u in st["unblocked"]) or "none"}.'
            + (f' Suggested next: `{st["next"]}`.' if st["next"] else ""), ""]
    if st["blocked"]:
        out += ["Waiting on a prerequisite:", ""]
        out += [f'- `{k}` — needs {"; ".join(v)}'
                for k, v in st["blocked"].items()]
        out.append("")

    if board.get("next_stage"):
        out += [f'## Next: `{board["next_stage"]}`', "",
                next(f'{r["purpose"]}' for r in rows
                     if r["stage"] == board["next_stage"]), ""]
    elif board.get("waiting_on"):
        out += [f'## Waiting on `{board["waiting_on"]}`', "",
                "It is still running (or its state is not established). "
                "Start nothing that depends on it, and do not start it "
                "again: wait for it to end, or bring a stale record up to "
                "date with `snowmig run --job <job> --refresh`.", ""]
    elif board.get("route") == "runbook":
        out += ["## The runbook's steps have run", "",
                "Copying rows (`snowmig_02_copy_<schema>`) is the customer's "
                "decision and is never proposed here; run `reconcile` after "
                "any copy.", ""]
    else:
        out += ["## Every stage has run", ""]

    out += ["---", "", "Purposes:", ""]
    out += [f'- `{r["stage"]}` — {r["purpose"]}' for r in rows]
    return "\n".join(out) + "\n"


def _dur(seconds) -> str:
    if seconds is None:
        return "—"
    return f"{seconds:.1f}s" if seconds < 90 else f"{seconds / 60:.1f}m"


def render_phase_report(rep: dict) -> str:
    """Every phase, and inside it every stage: when it ran, how long, and
    whether it passed."""
    out = ["# Phase report — every phase, its stages, and their verdicts", "",
           "Read from `run_log.jsonl`. A **phase** groups the **stages** that "
           "do one part of the migration; a phase fails if any stage in it "
           "failed. A stage that did not run is listed, never omitted, and the "
           "last run of a stage decides its verdict (earlier failures are "
           "counted). A run logged with no exit code crashed or was "
           "interrupted: it is UNKNOWN, and so is its phase -- never PASS. A "
           "workflow is read from its job record, as RUN.md reads it: a job "
           "still going is STILL RUNNING, one in a state this plugin does "
           "not classify is UNKNOWN, neither is a FAIL or a PASS.", "",
           "## Phases at a glance", "",
           "| Phase | Verdict | Stages | Passed | Failed | Not run | Duration "
           "| Retries | Resources | Runbook steps |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|---|"]
    for ph in rep.get("phase_summary") or []:
        out.append(f'| **{ph["phase"]}** | {ph["verdict"]} | {ph["stages"]} | '
                   f'{ph["passed"]} | {ph["failed"]} | {ph["not_run"]} | '
                   f'{_dur(ph["duration_seconds"] or None)} | {ph["retries"]} | '
                   f'{len(ph.get("resources") or [])} | '
                   f'{ph["runbook"] or "—"} |')
    out.append("")
    rows = rep.get("phases") or []
    for ph in rep.get("phase_summary") or []:
        out += [f'## Phase: {ph["phase"]} — {ph["verdict"]}', "",
                "| Stage | Runbook | Runs on | Started | Ended | Duration | "
                "Result | Runs | Failed runs | Retries |",
                "|---|---|---|---|---|---:|---|---:|---:|---:|"]
        for p in (r for r in rows if r["phase"] == ph["phase"]):
            out.append(
                f'| `{p["stage"]}` | {p["runbook"]} | '
                f'{p.get("runs_on", "—")} | {p["started_at"] or "—"} | '
                f'{p["ended_at"] or "—"} | {_dur(p["duration_seconds"])} | '
                f'{p["result"]} | {p["runs"]} | {p["failed_runs"]}'
                + (f' (+{p["unknown_runs"]} unknown)'
                   if p.get("unknown_runs") else "")
                + f' | {p.get("retries", 0)} |')
        out.append("")
        allocated = ph.get("resources") or []
        out.append("Resources allocated in this phase: " + (
            "; ".join(f'`{r.get("name")}` ({r["kind"]}'
                      + (f', {r["type"]}' if r.get("type") else "")
                      + f', {r["state"]}, billing: {r["billing"]})'
                      for r in allocated) if allocated else "none."))
        out.append("")
    events = rep.get("retry_events") or []
    out += ["", "## Retries", ""]
    if events:
        out += ["Every retry, as it happened: which call, which attempt, how "
                "long it waited first, and the error that caused it.", "",
                "| Phase | Call | Attempt | Waited | Error | At |",
                "|---|---|---|---:|---|---|"]
        out += [f'| `{e.get("stage")}` | {e.get("label")} | attempt '
                f'{e.get("attempt")}/{e.get("max_attempts")} | '
                f'{float(e.get("delay_seconds") or 0):.1f}s | '
                f'{str(e.get("error") or "").replace("|", "/")[:120]} | '
                f'{e.get("at", "")} |' for e in events]
    else:
        out.append("No call was retried in any logged phase.")
    if rep.get("unattributed_runs"):
        out += ["", f'{rep["unattributed_runs"]} `run` invocation(s) could '
                f'not be matched to a workflow phase.']
    return "\n".join(out) + "\n"


def render_databases(res: dict) -> str:
    """The databases a role can see, and which of them can be migrated.

    A migration registers ONE database as ONE catalog, so this table is the
    input to a choice, not a report anybody reads for its own sake.
    """
    dbs = res.get("databases") or []
    out = [f"# Source databases — {len(dbs)} visible", "",
           "**One Snowflake database becomes one AIDP catalog.** Pick a "
           "single database for this migration; another database is another "
           "migration, run again from S1.", "",
           "| Database | Kind | Owner | Migratable | Why not |",
           "|---|---|---|---|---|"]
    why = {"system": "the system application database",
           "share": "an imported share, not owned here",
           "personal": "a per-user scratch database"}
    for d in dbs:
        cat = d.get("category")
        out.append(
            f'| `{d.get("name")}` | {d.get("kind") or "—"} '
            f'| {d.get("owner") or "—"} '
            f'| {"yes" if cat == "migratable" else "no"} '
            f'| {why.get(cat, "—")} |')
    out += ["", f'Migratable: {len(res.get("migratable") or [])}.']
    return "\n".join(out) + "\n"


def render_catalogs(res: dict) -> str:
    """Every catalog on the DataLake, with the type the SERVER reports.

    The types here are the authority. This plugin once sent
    `catalogType=STANDARD`, which AIDP rejects outright; reading this list is
    what showed the real vocabulary to be INTERNAL and EXTERNAL.
    """
    cats = res.get("catalogs") or []
    out = [f"# Catalogs on the target DataLake — {len(cats)}", "",
           "| Catalog | Type | Source type |", "|---|---|---|"]
    for c in cats:
        out.append(f'| `{c.get("name")}` | {c.get("catalog_type") or "—"} '
                   f'| {c.get("source_type") or "—"} |')
    seen = res.get("types_seen") or []
    out += ["", f'Types the server reports: {", ".join(seen) or "none"}. '
                f'An EXTERNAL catalog points at a live source; an INTERNAL '
                f'one holds managed Delta tables.']
    return "\n".join(out) + "\n"


def _cron_cell(proposal: dict | None, note: str | None) -> str:
    if proposal and proposal.get("quartzCronExpression"):
        return (f'`{proposal["quartzCronExpression"]}` (PAUSED, '
                f'{proposal.get("timezoneId") or "UTC"})')
    return f"none -- {note}" if note else "none"


def render_generated_jobs(res: dict, plan: dict | None = None) -> str:
    """GENERATED_JOBS.md: what `snowmig jobs` generated, and what it did not.

    Every job is MANUAL. The cadence columns are what Snowflake did, and a
    PAUSED Quartz proposal where one maps exactly; neither is ever sent.
    """
    s = res.get("summary") or {}
    jobs = res.get("jobs") or []
    refresh = [j for j in jobs if j.get("kind") == "refresh"]
    graphs = [j for j in jobs if j.get("kind") == "task_graph"]
    out = ["# Generated jobs", "",
           f'**{s.get("refresh_jobs", 0)}** refresh job(s) for table '
           f'snapshots · **{s.get("task_jobs", 0)}** task-graph job(s) '
           f'({s.get("tasks_translated", 0)} task(s) translated, '
           f'**{s.get("task_stubs", 0)}** stub(s)) · '
           f'**{s.get("refresh_not_generated", 0)}** refresh(es) NOT '
           f'generated.', "",
           "Every job is **MANUAL**: no schedule is sent, and nothing runs "
           "until someone runs it. The cadence Snowflake used is recorded "
           "here, **not applied**; where it maps exactly onto a Quartz cron, "
           "that cron is shown as a PAUSED proposal for whoever turns the "
           "schedule on, after the job's output has been checked against the "
           "source.", ""]
    reg = res.get("registration")
    if not res.get("registered"):
        out += ["**Not registered.** This run was offline: the notebooks and "
                "specs are files in this directory. `snowmig jobs --register` "
                "creates the jobs in AIDP, unscheduled.", ""]
    elif reg:
        out += [f'**Registered** in `{reg.get("folder")}`: '
                f'{len(reg.get("created") or [])} created, '
                f'{len(reg.get("unconfirmed") or [])} requested but not '
                f'confirmed, {len(reg.get("name_taken") or [])} name(s) '
                f'already taken (not adopted), '
                f'{len(reg.get("failed") or [])} failed. Schedule: '
                f'{reg.get("schedule")}.', ""]
        out += [f'- {st["step"]} · {st["outcome"]} · {st["detail"]}'
                for st in reg.get("steps") or []] + [""]
    if s.get("tasks_note"):
        out += [f'> {s["tasks_note"]}', ""]

    out += ["## Refresh jobs", ""]
    if refresh:
        out += ["Each notebook is one `INSERT OVERWRITE` of the snapshot from "
                "its translated defining query: a full refresh, not "
                "Snowflake's incremental one.", "",
                "| Job | Source | Target | Intended cadence | Proposed cron "
                "| Runs after |", "|---|---|---|---|---|---|"]
        for j in refresh:
            cadence = j.get("intended_cadence") or {}
            when = (f'{cadence["source"]} {cadence["value"]}' if cadence
                    else "none")
            out.append(
                f'| `{j["name"]}` | `{j["source_identifier"]}` '
                f'({j["snapshot_of"]}) | `{j["target"]}` | {when} '
                f'| {_cron_cell(j.get("proposed_schedule"), j.get("proposed_schedule_note") or j.get("cadence_note"))} '
                f'| {", ".join(f"`{n}`" for n in j.get("run_after") or []) or "-"} |')
    else:
        out.append("None: the plan carries no table snapshot whose refresh "
                   "could be generated.")
    out.append("")
    skipped = res.get("not_generated") or []
    if skipped:
        out += ["### Refresh NOT generated", "",
                "The table snapshot still migrates; only its refresh is "
                "withheld. Nothing refreshes these tables on AIDP.", ""]
        out += [f'- `{x["source_identifier"]}` ({x.get("snapshot_of")}) — '
                f'{x["verdict"]}' for x in skipped] + [""]

    out += ["## Task-graph jobs", ""]
    if not graphs:
        out += ["None: no task was read"
                + (f' ({s["tasks_note"]})' if s.get("tasks_note") else "")
                + ".", ""]
    for j in graphs:
        sched = j.get("intended_schedule") or {}
        out += [f'### `{j["name"]}` — root `{j["root"]}`', "",
                f'- Snowflake schedule: {sched.get("value") or "none"}'
                f' · state in Snowflake: {j.get("source_state") or "?"}'
                f' · warehouse: {j.get("warehouse") or "?"}',
                f'- Proposed cron: '
                f'{_cron_cell(j.get("proposed_schedule"), j.get("proposed_schedule_note"))}']
        out += [f"- {n}" for n in j.get("notes") or []]
        out += ["", "| Task | Key | Depends on | Verdict |",
                "|---|---|---|---|"]
        out += [f'| `{t["source_identifier"]}` | `{t["task_key"]}` '
                f'| {", ".join(f"`{d}`" for d in t["depends_on"]) or "-"} '
                f'| {t["verdict"]} |' for t in j["tasks"]]
        out.append("")

    loads = (plan or {}).get("loads_that_stop") or []
    if loads:
        by_task = {t["source_identifier"]: (j["name"], t)
                   for j in graphs for t in j["tasks"]}
        out += ["## Loads that stop at cutover", "",
                "Migrating tables a pipe or task fills in Snowflake "
                "(PLANNED_OBJECTS.md), and the generated job that would take "
                "over each load. A stub does not load anything until its "
                "body is rewritten; a pipe has no generated job.", ""]
        for x in loads:
            hit = by_task.get(x["source_identifier"])
            if hit:
                name, task = hit
                # The verdict, not a bare "translated": it says whether
                # the body was dialect-translated or carried verbatim.
                state = (task["verdict"] if task["generated"]
                         else "STUB -- rewrite the body first")
                out.append(f'- `{x["table"]}` <- {x["kind"]} '
                           f'`{x["source_identifier"]}` -> job `{name}`, task '
                           f'`{task["task_key"]}` ({state})')
            else:
                out.append(f'- `{x["table"]}` <- {x["kind"]} '
                           f'`{x["source_identifier"]}` -> no generated job')
        out.append("")

    out += ["## Not carried by any generated job", "",
            "- a schedule: recorded and proposed, never applied;",
            "- a task's WHEN condition: the job runs unconditionally;",
            "- a stream's offsets: a body that reads a stream is a stub, and "
            "the nearest equivalent is the Delta change data feed of the "
            "migrated base table (`table_changes`), which is not generated;",
            "- a finalizer task and overlapping execution;",
            "- an incremental refresh: every refresh is a full overwrite.", ""]
    return "\n".join(out) + "\n"
