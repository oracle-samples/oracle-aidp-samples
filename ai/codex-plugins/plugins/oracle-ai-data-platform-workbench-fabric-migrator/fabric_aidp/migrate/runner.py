"""Execute a migration plan. Demo mode only: translate and write artifacts locally.

Nothing here contacts AIDP. `migrate` without `demo=True` raises rather than
producing a successful-looking report for a migration that did not happen.

An in-progress marker is written before the first artifact and removed after
report.json, so a run interrupted anywhere in between is unambiguously
incomplete to `verify`.
"""
from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
import uuid
from pathlib import Path

from fabric_aidp._atomic import write_json_atomic, write_text_atomic
from fabric_aidp.inventory.catalog import names_no_tables
from fabric_aidp.namespace import ADVICE as _NAMESPACE_ADVICE
from fabric_aidp.namespace import PLACEHOLDER, carries_placeholder
from fabric_aidp.naming import DEFAULT_CATALOG, validated_catalog
from fabric_aidp.sources import PLAN_TYPES_BY_SOURCE, SOURCE_BY_PLAN_TYPE
from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate import shortcut_to_oci
from fabric_aidp.translate import m_to_pyspark
from fabric_aidp.translate.m_parser import MPARSE_DIR
from fabric_aidp.translate import pipeline_to_aidp_job
from fabric_aidp.translate import tsql_to_spark_sql as tsql
from fabric_aidp.translate.types import Finding
from fabric_aidp.migrate import setup as setup_step

WORKSPACE_PREFIX = "/Workspace"
IN_PROGRESS_MARKER = ".fabric-aidp-migration-in-progress"
# `--filter` slices the run by source. The map is shared with the inventory
# and the verifier (fabric_aidp/sources.py): a private copy here is what made
# `migrate --help` advertise `--filter dataflow` and `migrate --filter
# dataflow` refuse it.
FILTER_KINDS = PLAN_TYPES_BY_SOURCE
_UNFILTERABLE = ("shown despite --filter {kind}: this row belongs to no "
                 "source slice, so no filter can select it and a refusal is "
                 "never hidden by one")
# Written by this function, so never counted as something it did not write.
_REPORT_FILES = ("report.json", "report.md", "report.html")
_WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL",
                     *(f"COM{i}" for i in range(1, 10)),
                     *(f"LPT{i}" for i in range(1, 10))}


def _emit(line, log):
    if log:
        log(line)


def _safe_name(name) -> str:
    value = unicodedata.normalize("NFC", str(name or "unnamed"))
    value = re.sub(r"[\\/:*?\"<>|\x00-\x1f\x7f]", "_", value).strip().rstrip(". ")
    if value in ("", ".", ".."):
        value = "unnamed"
    if value.split(".", 1)[0].upper() in _WINDOWS_RESERVED:
        value = f"_{value}"
    while len(value.encode("utf-8")) > 180:
        value = value[:-1]
    return value or "unnamed"


def _artifact_path(out_dir: Path, category: str, name, suffix: str,
                   asset_id: str, used: set) -> Path:
    candidate = out_dir / category / f"{_safe_name(name)}{suffix}"
    key = lambda p: unicodedata.normalize("NFC", str(p)).casefold()
    taken = {key(p) for p in used}
    if key(candidate) in taken:
        digest = hashlib.sha256(asset_id.encode("utf-8")).hexdigest()[:10]
        candidate = out_dir / category / f"{_safe_name(name)}-{digest}{suffix}"
    counter = 2
    while key(candidate) in taken:
        candidate = out_dir / category / f"{_safe_name(name)}-{counter}{suffix}"
        counter += 1
    used.add(candidate)
    return candidate


def _excluded_by_filter(source_type, filter_kind) -> bool:
    """Whether `--filter <filter_kind>` leaves this asset out of the run.

    Only an asset that belongs to *another* slice is left out. An asset that
    belongs to no slice at all is always run, which is the whole of the fix
    below: `fabric_unreadable_item` and `fabric_unsupported_item` are
    deliberately absent from `PLAN_TYPES_BY_SOURCE` (see
    fabric_aidp/sources.py), so `source_type not in FILTER_KINDS[kind]` was
    true for all six slices and both refusals appeared in no filtered run.

    Same rule `verify --filter` already follows for a FAIL -- "a failure is
    never hidden by a filter" -- and the case here is worse, since a FAIL at
    least surfaces in its own slice. Free to include, too: every branch that
    handles one of these writes no artifact, so a filtered run's output
    directory is exactly what it was.
    """
    if not filter_kind:
        return False
    if source_type not in SOURCE_BY_PLAN_TYPE:
        return False
    return source_type not in FILTER_KINDS[filter_kind]


def _note_unfilterable(row: dict, filter_kind) -> None:
    """Say, on the row itself, why a filtered run is showing it.

    Appended to the last finding rather than set as `note`, because
    `verify` reads `note` in preference to the findings and would then print
    this sentence in place of the reason the item was refused -- which is
    the thing the reader actually needs.
    """
    sentence = _UNFILTERABLE.format(kind=filter_kind)
    findings = row.get("findings")
    if findings:
        findings[-1]["detail"] = f"{findings[-1]['detail']}; {sentence}"
    else:
        row["note"] = f"{row['note']}; {sentence}" if row.get("note") else sentence


def _unclaimed_artifacts(out_dir: Path, results: list, also=()) -> list:
    """Files in the output directory that this run did not write.

    `migrate` writes into an existing directory and deletes nothing, so a
    second run leaves the first run's artifacts beside a report that does
    not mention them. MEASURED on the bundled demo estate, two runs into
    one directory:

        migrate --filter warehouse   warehouse/ -> 22 files, 22 rows
        migrate --filter notebook    warehouse/ -> still 22 files
                                     report.json -> 31 rows, all notebooks

        artifacts on disk                  53
        artifacts the report vouches for   31
        orphaned (report does not mention) 22

    Nothing is deleted here, and that is the decision, not an omission.
    `-o` names a directory the user chose, and migrating slices into one
    directory is a workflow this tool *recommends*: PL17_NOTEBOOK_NOT_IN_
    THIS_RUN tells the reader to "migrate the notebook slice into the same
    output directory before publishing". A run that emptied the directory
    would break the advice the runner itself gives, and `rmtree` on a
    user-named path is not something to do by default -- or, given that
    workflow, at all.

    What was actually wrong is that the directory did not say so. `publish`
    was never at risk: it reads report.json rather than globbing (see
    publish/publisher.py::_reported, and RefusalTests::
    test_an_artifact_the_report_does_not_list_is_not_published), so a stale
    artifact cannot be sent. But a person opening the directory, and any
    tool that globs it, saw 53 files and no way to tell which 22 the report
    disowns. Naming them is what makes the directory self-describing.

    Dotfiles are skipped: the in-progress marker is one, and so is any
    `.<name>.<pid>.<hex>.tmp` an interrupted `_atomic` write left behind.
    """
    written = {(out_dir / row["output_path"]).resolve()
               for row in results
               if isinstance(row.get("output_path"), str) and row["output_path"]}
    # Files this run wrote that belong to no one asset: the setup script and
    # notebook. Without them here, every run reported its own setup files as
    # left behind by an earlier one.
    written |= {(out_dir / relative).resolve() for relative in also}
    found = []
    for path in out_dir.rglob("*"):
        if path.name.startswith(".") or not path.is_file():
            continue
        if path.parent == out_dir and path.name in _REPORT_FILES:
            continue
        if path.resolve() in written:
            continue
        found.append(path.relative_to(out_dir).as_posix())
    return sorted(found)


def _unreadable_notebook(source: dict) -> bool:
    """Whether the inventory failed to read this notebook's content at all.

    `parse_error` covers two cases and they need different answers: a file
    that would not open or decode (no content -- nothing to translate), and
    one that opened but would not parse as a Fabric notebook (content, and
    the line-based rules can still work on it).
    """
    return bool(source.get("parse_error")) and not str(source.get("content") or "").strip()


def _tasks_of(job_json: str) -> list:
    """The tasks of an emitted job payload, or [] if it will not read back."""
    try:
        payload = json.loads(job_json)
    except (TypeError, ValueError):
        return []
    tasks = payload.get("tasks") if isinstance(payload, dict) else None
    return [t for t in tasks if isinstance(t, dict)] if isinstance(tasks, list) else []


def _result(asset, kind, result, path, out_dir) -> dict:
    # Every artifact-producing branch ends here, which is why the placeholder
    # check lives in this function rather than in each translator. An oci://
    # path still spelling `<your-oci-namespace>` names a bucket that cannot
    # resolve, so the artifact must not grade `ok`. Measured on the bundled
    # estate with no `plan --namespace`: three artifacts carried it -- the
    # shortcut notes for SalesLake.adls_landing and SalesLake.claims_raw_s3
    # and the notebook 01_Ingest_Claims -- and all three reported
    # `status=ok  flags=0`. Flagged, not refused: the placeholder is the
    # documented way to review an estate with no tenancy (docs/RUNBOOK.md),
    # so the artifact is still written and reads REVIEW.
    if carries_placeholder(result.translated_sql):
        result.findings.append(
            Finding("NS01_NAMESPACE_PLACEHOLDER", _NAMESPACE_ADVICE, "flag"))
    return {
        "asset_id": asset["id"],
        "kind": kind,
        "status": "needs_manual_review" if result.needs_manual_review else "ok",
        "changes": result.changes,
        "flags": result.flags,
        "output_path": path.relative_to(out_dir).as_posix(),
        "findings": [{"rule": f.rule, "detail": f.detail, "severity": f.severity}
                     for f in result.findings],
        "source_sql": result.source_sql,
        "translated_sql": result.translated_sql,
    }


def _add_plan_findings(row: dict, asset: dict) -> None:
    """Carry the plan's estate-level findings onto this asset's row.

    `plan` can see what no single translator can -- two Fabric containers
    folding onto one AIDP schema (NM01) -- and records it per asset in
    `plan_findings`. A flag there turns an `ok` row into
    `needs_manual_review`, the same as a translator's flag would.
    """
    extra = [f for f in asset.get("plan_findings") or []
             if isinstance(f, dict) and f.get("rule")]
    if not extra or row.get("status") == "error":
        return
    row.setdefault("findings", []).extend(extra)
    flags = sum(1 for f in extra if f.get("severity") == "flag")
    if row.get("status") in ("ok", "needs_manual_review"):
        row["flags"] = row.get("flags", 0) + flags
        if flags:
            row["status"] = "needs_manual_review"


def _planned_notebook_paths(assets, out_dir: Path) -> dict:
    """notebook name -> the workspace path an unfiltered run would write it to.

    Walked over the whole plan with its own `used` set, in plan order, so the
    names -- including the collision suffix `_artifact_path` adds for two
    notebooks of one name -- are the ones the unfiltered run produces.
    `used` holds full paths and every category has its own directory, so a
    notebook can only ever collide with another notebook; nothing else in the
    run can change what these come out as.
    """
    used: set = set()
    paths = {}
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        source = asset.get("source") or {}
        if source.get("type") != "fabric_notebook":
            continue
        path = _artifact_path(out_dir, "notebooks", source.get("name"), ".py",
                              str(asset.get("id", "")), used)
        paths[source.get("name")] = f"{WORKSPACE_PREFIX}/{path.name}"
    return paths


def migrate(plan, *, out_dir, filter_kind=None, log=None,
            cluster_key=None, catalog=None) -> dict:
    out_dir = Path(out_dir)
    if filter_kind is not None and filter_kind not in FILTER_KINDS:
        raise ValueError(f"unknown migration filter {filter_kind!r}; expected one of: "
                         + ", ".join(sorted(FILTER_KINDS)))
    if not isinstance(plan, dict):
        raise ValueError("migration plan must be a JSON object")

    out_dir.mkdir(parents=True, exist_ok=True)
    namespace = (plan.get("target_aidp") or {}).get("namespace") or PLACEHOLDER
    # The AIDP catalog a table name sits in (naming.DEFAULT_CATALOG unless
    # `plan --catalog` set another). Read off the plan, not re-derived, so the
    # emitted SQL never disagrees with the plan that describes it. The
    # `catalog` parameter lets `migrate --catalog` re-target an existing plan
    # without re-planning; unset, the plan's own value is used untouched.
    # Validated here too: `migrate --catalog` bypasses `plan` entirely, and
    # a hand-edited plan can carry anything at all.
    aidp_catalog = validated_catalog(
        catalog or (plan.get("target_aidp") or {}).get("catalog")
        or DEFAULT_CATALOG)
    # `resolved_catalog`: the tier-3 table-catalog-resolution map (name ->
    # {tier, created_by, ...}), unrelated to the AIDP catalog above despite
    # the shared word. Kept under its own name so the two never collide.
    resolved_catalog = plan.get("resolved_catalog") or {}
    # Said once, here, because the run is the only scope at which it is true.
    # An empty catalog means "this run resolved nothing", which is a
    # different statement from "this table does not exist" -- and the second
    # is what the translators used to make, once per reference, because their
    # guards tested `not catalog` and a plan that resolved nothing carries
    # `{"summary": {}, "tables": {}}`. They now decline to answer at all
    # (see `catalog.names_no_tables`) and the run says the one true thing.
    catalog_resolved_nothing = names_no_tables(resolved_catalog)
    assets = plan.get("assets")
    assets = assets if isinstance(assets, list) else []

    report_id = uuid.uuid4().hex
    write_text_atomic(out_dir / IN_PROGRESS_MARKER, report_id)

    # The AIDP schemas this migration writes into, created before anything
    # runs (see migrate/setup.py and issue #50). Built from the whole plan,
    # not from the slice `--filter` selected: which schemas exist is a
    # property of the target estate, and a job migrated alone still needs
    # every schema its notebooks write.
    setup_entry = setup_step.write_setup(
        out_dir, setup_step.setup_plan(plan, aidp_catalog))

    results, used, counts = [], set(), {}
    # A pipeline task must point at the notebook this run just wrote.
    # Plan order is topological and pipelines depend on their notebooks,
    # so every path a pipeline needs is already in here by the time we
    # reach it. This is the Databricks validator's migration registry,
    # built as we go instead of loaded from a file.
    notebook_paths = {}
    # ...except when `--filter` excluded the notebook slice. The notebooks
    # are still in the plan and the path each one gets is a property of the
    # plan, not of which slice ran, so they are resolved up front instead.
    # Without this, `--filter pipeline` told every pipeline its notebook
    # "was not migrated" -- false, and on demo-input it blocked
    # pipeline.Daily_Claims, the one pipeline of 31 that translates, taking
    # blocked from 30 to 31.
    borrowed = _planned_notebook_paths(assets, out_dir) if (
        filter_kind and "fabric_notebook" not in FILTER_KINDS[filter_kind]) else {}
    notebook_paths.update(borrowed)
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        source = asset.get("source") or {}
        source_type = source.get("type", "")
        if _excluded_by_filter(source_type, filter_kind):
            continue
        try:
            if source_type == "fabric_notebook" and _unreadable_notebook(source):
                # The inventory already said why it could not read this
                # notebook's content file, and that reason never reached the
                # report. Translating the empty string it left behind wrote a
                # 0-byte .py, graded it `needs_manual_review` with
                # NB06_UNPARSEABLE ("not valid JSON") -- blaming the notebook
                # format for a file nothing had opened -- and `publish`
                # queued it. An empty notebook runs, and does nothing.
                row = {"asset_id": asset["id"], "kind": "notebook",
                       "status": "blocked",
                       "findings": [{
                           "rule": "NB07_SOURCE_UNREADABLE",
                           "detail": (
                               f"notebook {source.get('name', '?')!r} was not "
                               f"translated: {source.get('parse_error')}. The "
                               f"inventory recorded no content for it, so "
                               f"there was nothing to translate and no file "
                               f"was written. Fix or re-export the item and "
                               f"run the inventory again"),
                           "severity": "flag"}]}
            elif source_type == "fabric_notebook":
                translated = nb2spark.translate(
                    source.get("content", ""), namespace=namespace,
                    default_lakehouse=source.get("default_lakehouse"),
                    # The same `{workspace item id: display name}` map the
                    # Dataflow branch below passes as `lakehouses=`.
                    # `translate` has taken a `guid_index` since NB01
                    # shipped and this, its only caller, passed nothing, so
                    # a notebook bound only by GUID -- or reading
                    # `abfss://ws@onelake.../<guid>/Files/x` -- got NB02
                    # "recorded only as the GUID ..." while the plan beside
                    # it held that GUID's name. The producer existed, the
                    # parameter existed, and nothing joined them.
                    guid_index=plan.get("lakehouse_catalog"),
                    catalog=resolved_catalog, aidp_catalog=aidp_catalog)
                path = _artifact_path(out_dir, "notebooks", source.get("name"),
                                      ".py", asset["id"], used)
                write_text_atomic(path, translated.translated_sql)
                # Content that read but would not parse: the line-based rules
                # still have text to work on, so it is translated -- but the
                # inventory's reason rides along, because a reader comparing
                # the artifact to the original needs to know the structure
                # was never understood.
                # A dependency the inventory saw and could not name. The
                # notebook will still fail at run time if the thing it
                # launches is not there, and a reviewer reading only the
                # report had no way to know a reference existed at all.
                for reference in source.get("unresolved_refs") or []:
                    translated.findings.append(Finding(
                        "NB29_UNRESOLVED_NOTEBOOK_REF",
                        f"this notebook runs another notebook whose name is "
                        f"built at run time ({reference}), so the inventory "
                        f"could not say which one; the plan has no ordering "
                        f"edge for it. Check by hand that the notebook it "
                        f"launches is in this migration", "flag"))
                if source.get("parse_error"):
                    translated.findings.append(Finding(
                        "NB07_SOURCE_UNREADABLE",
                        f"the inventory could not parse this notebook: "
                        f"{source['parse_error']}; it was translated line by "
                        f"line, so anything that depends on cell structure "
                        f"(magics, cell languages) was not seen", "flag"))
                row = _result(asset, "notebook", translated, path, out_dir)
                notebook_paths[source.get("name")] = (
                    f"{WORKSPACE_PREFIX}/{path.name}")
            elif (source_type.startswith("fabric_warehouse_")
                  and source.get("read_error")):
                # Same refusal as NB07_SOURCE_UNREADABLE, for the same
                # reason. The inventory could not decode this `.sql`, so
                # `sql` is "": translating it wrote a 0-byte artifact,
                # graded PASS with zero findings, and `publish` queued a
                # file that creates nothing.
                row = {"asset_id": asset["id"],
                       "kind": f"warehouse_{source_type[len('fabric_warehouse_'):]}",
                       "status": "blocked",
                       "findings": [{
                           "rule": "SQ00_SOURCE_UNREADABLE",
                           "detail": (
                               f"{source.get('file', '?')} in Warehouse "
                               f"{source.get('warehouse', '?')!r} was not "
                               f"translated: {source.get('read_error')}. The "
                               f"inventory recorded no SQL for it, so there "
                               f"was nothing to translate and no file was "
                               f"written. Re-export it as UTF-8 and run the "
                               f"inventory again"),
                           "severity": "flag"}]}
            elif source_type.startswith("fabric_warehouse_"):
                kind = source_type[len("fabric_warehouse_"):]
                # `item`: the Warehouse these objects live in. A DacFx export
                # writes `CREATE TABLE dbo.claim` -- the Warehouse name is in
                # the folder, never in the SQL -- so without this the artifact
                # creates a differently-named table from the one the plan
                # promises.
                #
                # `table_catalog` is the same `resolved_catalog` the notebook
                # branch above passes as `catalog`, under the name the T-SQL
                # translator uses for it -- there `catalog` already means the
                # AIDP catalog string. Until it was passed, the warehouse path
                # consulted no catalog: a view over a shortcut, or over a table
                # nothing in the export declares, was rewritten to a confident
                # three-part name and graded PASS, where the notebook path
                # flagged the identical reference.
                translated = tsql.translate(source.get("sql", ""), kind=kind,
                                            catalog=aidp_catalog,
                                            item=source.get("warehouse"),
                                            table_catalog=resolved_catalog)
                name = f"{source.get('schema', '')}.{source.get('name', '')}".strip(".")
                path = _artifact_path(out_dir, "warehouse", name, ".spark.sql",
                                      asset["id"], used)
                write_text_atomic(path, translated.translated_sql)
                row = _result(asset, f"warehouse_{kind}", translated, path, out_dir)
            elif source_type == "fabric_shortcut":
                translated = shortcut_to_oci.translate(source, namespace=namespace)
                path = _artifact_path(
                    out_dir, "shortcuts",
                    f"{source.get('lakehouse', '')}.{source.get('name', '')}".strip("."),
                    ".md", asset["id"], used)
                write_text_atomic(path, translated.translated_sql)
                row = _result(asset, "shortcut", translated, path, out_dir)
            elif source_type == "fabric_pipeline":
                translated = pipeline_to_aidp_job.translate(
                    {"name": source.get("name", ""),
                     "activities": source.get("activities", []),
                     "schedules": source.get("schedules", []),
                     "schedules_error": source.get("schedules_error", ""),
                     "content_error": source.get("content_error", ""),
                     "parameters": source.get("parameters", {})},
                    notebook_paths=notebook_paths, cluster_key=cluster_key)
                if not translated.translated_sql:
                    # Blocked: no job file. A job holding only the notebook
                    # tasks would run, skip the Copy that fed them, and be wrong.
                    row = {"asset_id": asset["id"], "kind": "pipeline_job",
                           "status": "blocked",
                           "findings": [{"rule": f.rule, "detail": f.detail,
                                         "severity": f.severity}
                                        for f in translated.findings]}
                else:
                    if any(t.get("notebookPath") in borrowed.values()
                           for t in _tasks_of(translated.translated_sql)):
                        # Emitted, and honest about what is missing beside it:
                        # `publish` refuses a job whose notebooks this
                        # migration did not produce, and the reader should not
                        # have to reach `publish` to find that out.
                        translated.findings.append(Finding(
                            "PL17_NOTEBOOK_NOT_IN_THIS_RUN",
                            f"`--filter {filter_kind}` excluded the notebook "
                            f"slice, so this job points at notebook artifacts "
                            f"this run did not write; migrate the notebook "
                            f"slice into the same output directory before "
                            f"publishing", "flag"))
                    if setup_entry["notebook"]:
                        translated.translated_sql = setup_step.add_setup_task(
                            translated.translated_sql,
                            setup_step.setup_task_path(WORKSPACE_PREFIX))
                    path = _artifact_path(out_dir, "jobs", source.get("name"),
                                          ".job.json", asset["id"], used)
                    write_text_atomic(path, translated.translated_sql)
                    row = _result(asset, "pipeline_job", translated, path, out_dir)
            elif source_type == "fabric_dataflow_query":
                query = source.get("query")
                if not query:
                    raise ValueError(
                        "no parsed query on this asset; Node was unavailable at scan time")
                helper = source.get("helper")
                translated = m_to_pyspark.translate_query(
                    query, queries_by_name={helper["name"]: helper} if helper else {},
                    namespace=namespace, catalog=aidp_catalog,
                    # `lakehouses`, not the table catalog above. A lakehouse GUID
                    # cannot be resolved from a Git export at all -- the Lakehouse
                    # item's logicalId is a different identifier from the
                    # lakehouseId M navigates by. So these reads flag, correctly.
                    lakehouses=plan.get("lakehouse_catalog"),
                    section_attrs=source.get("section_attrs") or "",
                    source=f"{source.get('dataflow', '')}/mashup.pq")
                if not translated.translated_sql:
                    # A blocked query writes no file. _result would happily write
                    # an empty .py, and an empty .py runs.
                    row = {"asset_id": asset["id"], "kind": "dataflow_query",
                           "status": "blocked",
                           "findings": [{"rule": f.rule, "detail": f.detail,
                                         "severity": f.severity}
                                        for f in translated.findings]}
                else:
                    path = _artifact_path(
                        out_dir, "dataflows",
                        f"{source.get('dataflow', '')}.{source.get('name', '')}".strip("."),
                        ".py", asset["id"], used)
                    write_text_atomic(path, translated.translated_sql)
                    row = _result(asset, "dataflow_query", translated, path, out_dir)
            elif source_type == "fabric_dataflow_unreadable":
                # Same shape as INV01, one level down: the item's type is
                # known and its contents are not. Dropped -- which is what
                # happened before -- a whole Dataflow left no trace in the
                # plan, the report or `verify`.
                row = {"asset_id": asset["id"], "kind": "dataflow",
                       "status": "blocked",
                       "findings": [{
                           "rule": "INV04_DATAFLOW_UNREADABLE",
                           "detail": (
                               f"Dataflow {source.get('name', '?')!r} was not "
                               f"translated: "
                               f"{source.get('reason') or 'no reason recorded'}. "
                               f"None of its queries were read, so none were "
                               f"migrated and none are counted anywhere as "
                               f"outstanding. Fix or re-export the item and "
                               f"run the inventory again"),
                           "severity": "flag"}]}
            elif source_type == "fabric_dataflow_untranslated":
                # The common case for anyone who has not run `npm install`.
                # The queries were counted by text; nothing was translated.
                counted = source.get("counted", 0)
                row = {"asset_id": asset["id"], "kind": "dataflow",
                       "status": "blocked",
                       "findings": [{
                           "rule": "INV05_DATAFLOW_NOT_TRANSLATED",
                           "detail": (
                               f"Dataflow {source.get('name', '?')!r}: "
                               f"{counted} quer(ies) counted, 0 translated -- "
                               f"the Power Query parser was not available at "
                               f"scan time. Power Query has no Python parser, "
                               f"so translating a Dataflow needs Node 18+ and "
                               f"`npm install` in {MPARSE_DIR} -- the parser "
                               f"directory of this installation, which for a "
                               f"pip-installed wheel is inside site-packages, "
                               f"not a repository path. Install it and run the "
                               f"inventory again, or migrate this Dataflow by "
                               f"hand"),
                           "severity": "flag"}]}
            elif source_type == "fabric_unreadable_item":
                # Not "planned": there is nothing to plan. The item exists
                # in the export and this tool cannot tell what it is, which
                # is a refusal and reads as REVIEW. Reported as `planned`
                # it would read as "a later release will do this", and
                # dropped -- which is what happened before -- it read as
                # nothing at all.
                row = {"asset_id": asset["id"], "kind": "unreadable_item",
                       "status": "blocked",
                       "findings": [{
                           "rule": "INV01_ITEM_UNREADABLE",
                           "detail": (
                               f"{source.get('path') or source.get('name', '?')} "
                               f"is a Fabric item directory whose identity could "
                               f"not be read: "
                               f"{source.get('reason') or 'no reason recorded'}. "
                               f"Nothing was migrated from it. Fix or re-export "
                               f"the item and run the inventory again"),
                           "severity": "flag"}]}
            elif (source_type == "fabric_lakehouse"
                  and source.get("shortcuts_error")):
                # Same shape as INV01. A lakehouse whose shortcuts file could
                # not be read has an unknown number of external references,
                # and reporting it `planned` reads as "a later release will
                # do this" when the truth is that nobody looked. It used to
                # be logged as "tracked (0 found)", which is a claim that the
                # file was read and was empty.
                row = {"asset_id": asset["id"], "kind": "lakehouse",
                       "status": "blocked",
                       "findings": [{
                           "rule": "INV03_SHORTCUTS_UNREADABLE",
                           "detail": (
                               f"Lakehouse {source.get('name', '?')!r}: "
                               f"{source.get('shortcuts_error')}. Its "
                               f"shortcuts were not counted and none of them "
                               f"were migrated, so any data living outside "
                               f"OneLake behind this lakehouse is unaccounted "
                               f"for. Fix or re-export the item and run the "
                               f"inventory again"),
                           "severity": "flag"}]}
            elif source_type == "fabric_lakehouse":
                row = setup_step.lakehouse_row(asset, setup_entry)
            elif source_type == "fabric_unsupported_item":
                row = {"asset_id": asset["id"], "kind": "unsupported_item",
                       "status": "blocked",
                       "findings": [{
                           "rule": "INV02_ITEM_TYPE_UNSUPPORTED",
                           "detail": (
                               f"Fabric "
                               f"{source.get('item_type', 'item')} "
                               f"{source.get('name', '?')!r} was found in the "
                               f"export and not migrated: "
                               f"{source.get('reason') or 'no reason recorded'}. "
                               f"Migrate it by hand, or confirm it is not "
                               f"needed on AIDP"),
                           "severity": "flag"}]}
            else:
                row = {"asset_id": asset["id"],
                       "kind": asset.get("target", {}).get("type", "unknown"),
                       "status": "planned",
                       "note": "no translator for this asset type in this release"}
        except Exception as exc:
            row = {"asset_id": str(asset.get("id", "<missing-id>")),
                   "kind": source_type or "unknown",
                   "status": "error", "error": str(exc)}
        _add_plan_findings(row, asset)
        if filter_kind and source_type not in FILTER_KINDS[filter_kind]:
            # Reached only by an asset `_excluded_by_filter` let through:
            # one belonging to no slice. The reader asked for one slice and
            # is being shown a row from none, so the row says why.
            _note_unfilterable(row, filter_kind)
        counts[row["status"]] = counts.get(row["status"], 0) + 1
        results.append(row)
        if row["status"] in ("ok", "needs_manual_review"):
            _emit(f"  {'OK' if row['status'] == 'ok' else 'REVIEW':<6s} "
                  f"{row['asset_id']:<40s} changes={row.get('changes', 0)} "
                  f"flags={row.get('flags', 0)}", log)
        elif row["status"] == "error":
            _emit(f"  ERROR  {row['asset_id']:<40s} {row['error']}", log)

    # Taken before report.md and report.json are written, so this run's own
    # reports cannot appear in it and a previous run's are excluded by name.
    unclaimed = _unclaimed_artifacts(
        out_dir, results,
        also=[p for p in (setup_entry['sql'], setup_entry['notebook']) if p])
    if unclaimed:
        _emit(f"\n# note: {len(unclaimed)} file(s) in {out_dir} were "
              f"not written by this run, and this report does not vouch for "
              f"them. Nothing was deleted -- they are listed in report.json "
              f"under `unclaimed_artifacts` and at the end of report.md. "
              f"`publish` reads the report, so it will not send them; a "
              f"person or a script reading the directory has no such "
              f"protection. Migrate into an empty directory if you want one "
              f"that describes itself.", log)

    # Once per run, in the three places `unclaimed_artifacts` is said, and
    # for the same reason: it is a property of the whole run and not of any
    # one asset. Only worth saying when something was actually translated --
    # a run of nothing resolved nothing trivially.
    if catalog_resolved_nothing and results:
        _emit(f"\n# note: the resolved table catalog in this plan names no "
              f"tables, so no table reference in these {len(results)} "
              f"asset(s) was checked against one. That is not the same as "
              f"the tables being absent: the export declared no warehouse "
              f"DDL and no shortcuts, no --tables-csv was supplied and no "
              f"notebook write was inferred, so there was nothing to resolve "
              f"against. Table names were rewritten on name shape alone and "
              f"no object carries a 'no catalog tier knows this table' "
              f"finding, because this run is not in a position to say that "
              f"about any table. Re-run `inventory` over an export that "
              f"includes the Warehouse items, or pass `--tables-csv`, to get "
              f"those checks back.", log)

    if setup_entry["notebook"]:
        _emit(f"\n# setup: {setup_entry['statements']} statement(s) in "
              f"{setup_entry['sql']} and {setup_entry['notebook']}; every "
              f"migrated job runs them as its first task", log)
    if setup_entry["refused"]:
        _emit(f"# setup: left out {len(setup_entry['refused'])} schema(s) AIDP "
              f"will refuse: "
              + ", ".join(r["schema"] for r in setup_entry["refused"]), log)

    report = {
        "report_id": report_id,
        "complete": True,
        "plan_id": plan.get("plan_id"),
        "migrated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "demo",
        "filter": filter_kind,
        "counts": counts,
        # True when the plan's resolved table catalog names no tables at all,
        # so every table reference in this run was rewritten on name shape
        # with nothing consulted. A reader of report.json cannot otherwise
        # tell "no reference was questionable" from "nothing was checked".
        "catalog_resolved_nothing": catalog_resolved_nothing,
        # Files in `out_dir` this run did not write. Empty for a run into an
        # empty directory, which is what `demo.sh` and every test do.
        "unclaimed_artifacts": unclaimed,
        # The AIDP schemas this migration creates before anything runs, the
        # files that create them, and the schemas AIDP would refuse -- which
        # are the reason any table under them cannot exist. `publish` reads
        # `notebook` from here to upload it first.
        "setup": setup_entry,
        "results": results,
    }

    markdown = [f"# Migration report ({report['migrated_at']})", "",
                f"**Plan**: `{plan.get('plan_id')}`  **Mode**: `demo`  "
                f"**Filter**: `{filter_kind or 'all'}`", "",
                "| Status | Count |", "|---|---|"]
    markdown += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    markdown += ["",
                 "> `ok` means translated with no known issue detected — **not "
                 "execution-verified**. Nothing here parses or runs the generated "
                 "artifacts, so a construct no rule covers is reported clean. Review "
                 "them before running in production."]
    if setup_entry["notebook"]:
        markdown += [
            "",
            f"> **Setup runs first.** `{setup_entry['sql']}` creates the "
            f"{len(setup_entry['schemas'])} AIDP schema(s) this migration writes "
            f"into, and every migrated job runs the same statements as its first "
            f"task (`{setup_step.SETUP_TASK_KEY}`). AIDP cannot write a table into "
            f"a schema that does not exist; run the script yourself before any "
            f"notebook or script no job runs."]
    if setup_entry["refused"]:
        markdown += [
            "",
            "> **Schemas AIDP will refuse, left out of the setup script:** "
            + ", ".join(f"`{r['schema']}`" for r in setup_entry["refused"])
            + ". AIDP accepts only lower-case letters, digits and underscores in "
            "a schema name, so no table under these can be created until the "
            "Fabric item is renamed or another schema is chosen."]
    # Beside the `ok` caveat rather than in a section of its own: it is the
    # same kind of statement -- what this report is not in a position to say.
    if catalog_resolved_nothing and results:
        markdown += [
            "",
            "> **The resolved table catalog names no tables**, so no table "
            "reference in this run was checked against one. Table names were "
            "rewritten on name shape alone. This is not a claim that the "
            "tables are absent — there was nothing to resolve against, "
            "because the export declared no warehouse DDL and no shortcuts, "
            "no `--tables-csv` was supplied, and no notebook write was "
            "inferred. No object below carries a \"no catalog tier knows "
            "this table\" finding for that reason, and the absence of those "
            "findings is not evidence of anything."]
    markdown += ["", "## Per-asset findings"]
    for row in results:
        if row["status"] not in ("ok", "needs_manual_review"):
            continue
        markdown.append(f"### `{row['asset_id']}` ({row['status']}, "
                        f"{row['changes']} changes, {row['flags']} flags)")
        for finding in row.get("findings", []):
            markdown.append(f"- _{finding['severity']}_ **{finding['rule']}** — "
                            f"{finding['detail']}")
        markdown.append("")

    # Refusals and failures, which this file used to leave out entirely. The
    # counts table said `blocked | 42` and the body named none of the 42, so
    # the one thing a reader of report.md needed -- which objects were not
    # migrated, and why -- was only in report.json. An error row carries no
    # findings at all, only `error`, so it was invisible even by asset id.
    unmigrated = [r for r in results
                  if r["status"] not in ("ok", "needs_manual_review")]
    if unmigrated:
        markdown += ["## Not migrated", "",
                     "Refused (`blocked`), failed (`error`) or with no "
                     "translator in this release (`planned`). Nothing was "
                     "written for these.", ""]
    for row in unmigrated:
        markdown.append(f"### `{row['asset_id']}` ({row['status']})")
        if row.get("error"):
            markdown.append(f"- _error_ {row['error']}")
        for finding in row.get("findings", []):
            markdown.append(f"- _{finding['severity']}_ **{finding['rule']}** — "
                            f"{finding['detail']}")
        if row.get("note"):
            markdown.append(f"- _note_ {row['note']}")
        markdown.append("")

    # Last, because it is about the directory rather than about the plan.
    # A reader who has just read which assets were migrated needs to know
    # which files beside them were not part of that.
    if unclaimed:
        markdown += [
            "## Not written by this run", "",
            f"{len(unclaimed)} file(s) were already in `{out_dir}` and this "
            f"report does not vouch for them. `migrate` deletes nothing, so "
            f"an earlier run into the same directory -- with a different "
            f"`--filter`, or from a different plan -- leaves its artifacts "
            f"here. `publish` reads this report and will not send them; a "
            f"person or a script reading the directory has no such "
            f"protection.", ""]
        markdown += [f"- `{name}`" for name in unclaimed]
        markdown.append("")

    # report.md first: report.json is the machine-readable commit marker.
    write_text_atomic(out_dir / "report.md", "\n".join(markdown))
    write_json_atomic(out_dir / "report.json", report)
    # The HTML report is a convenience, not the record: report.json is. A
    # renderer bug must not lose a migration that already succeeded, so this
    # is allowed to fail and say so.
    try:
        from fabric_aidp.migrate.html_report import render as _render_html
        _render_html(report, out_dir / "report.html")
    except Exception as exc:  # pragma: no cover - defensive
        if log:
            log(f"# note: could not write report.html ({exc}); "
                f"report.json and report.md are complete")
    (out_dir / IN_PROGRESS_MARKER).unlink()
    return report
