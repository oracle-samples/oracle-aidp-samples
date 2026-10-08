"""Scan Lakehouse items from a Fabric Git export.

A Lakehouse export carries NO table DDL and no Spark view definitions — Fabric
does not track them. What it does carry is far more useful to a migration:

  shortcuts.metadata.json   pointers to data living in S3, ADLS Gen2, GCS,
                            S3-compatible, Dataverse, Blob, SharePoint, or
                            another OneLake location -- the eight target
                            types Diabetes_LH's alm.settings.json enumerates
  data-access-roles.json    OneLake security roles (preview)
  alm.settings.json         which object types are tracked at all

Tracking is opt-in per object type. A lakehouse with shortcut tracking disabled
exports zero shortcuts, which is NOT the same fact as having none. This module
reports `shortcut_count: None` with `tracking.shortcuts == "not_tracked"` in
that case, so no caller can mistake "we did not look" for "there is nothing".

The same distinction holds for the shortcuts file itself. A count is reported
only when the file is present AND parses to a list. Malformed JSON, valid JSON
of some other shape, and a file that is missing while alm.settings.json says
shortcuts are tracked all report `shortcut_count: None`, `"unknown"`, and a
`shortcuts_error` saying which -- they used to log "tracked (0 found)", which
claims the file was read and was empty.
"""
from __future__ import annotations

import json

from fabric_aidp.inventory.git_workspace import items_of_type
from fabric_aidp.sources import is_table_section

SHORTCUTS_FILE = "shortcuts.metadata.json"
DAR_FILE = "data-access-roles.json"
ALM_FILE = "alm.settings.json"

TRACKED, NOT_TRACKED, UNKNOWN = "tracked", "not_tracked", "unknown"

# Where each shape below comes from. Two different kinds of confidence sit
# in one table and they were not told apart, so a reader could not tell
# which field names had been seen in a real export and which were read off
# Microsoft's documentation and never observed.
#
# MEASURED 2026-09-29 across everything vendored here: `oneLake` is the only
# payload any real export in tests/fixtures/real/ actually contains. The
# demo-workspace fixture carries `amazonS3` and `adlsGen2`, but this project
# authored that fixture from the same documentation, so it exercises the code
# path without confirming the shape. The remaining five appear nowhere but in
# this table and in tests written beside it.
#
# One real reading does support the *type names*: Diabetes_LH's
# alm.settings.json is a genuine export and enumerates all eight
# `Shortcuts.*` subtypes. That is evidence the types exist and are spelled
# this way. It is not evidence about the payload key or the fields inside it,
# which is what this table is.
#
# Closing this needs a live-tenant export with an ADLS/GCS/S3-compatible
# shortcut in it, which nobody here has. Until one turns up, the honest thing
# is to say which row is which.
DOCUMENTED_ONLY = "microsoft documentation; not seen in any export vendored here"
FROM_REAL_EXPORT = "seen in a real Fabric Git export vendored under tests/fixtures/real"
FROM_AUTHORED_FIXTURE = ("authored by this project from Microsoft documentation "
                         "(fabric_aidp/fixtures/demo-workspace); the code path is "
                         "exercised, the shape is not confirmed")
SHAPE_PROVENANCE = {
    "onelake": FROM_REAL_EXPORT,
    "amazons3": FROM_AUTHORED_FIXTURE,
    "adlsgen2": FROM_AUTHORED_FIXTURE,
    "googlecloudstorage": DOCUMENTED_ONLY,
    "azureblobstorage": DOCUMENTED_ONLY,
    "s3compatible": DOCUMENTED_ONLY,
    "dataverse": DOCUMENTED_ONLY,
    "onedrivesharepoint": DOCUMENTED_ONLY,
}

# Payload key -> (location key, subpath key). OneLake is handled separately
# because it addresses an item, not a URL. The comment on each row is its
# entry in SHAPE_PROVENANCE above; tests/test_shortcut_to_oci.py checks that
# the two agree and that a "real export" claim is backed by a file on disk.
_EXTERNAL_TARGETS = {
    # authored fixture
    "amazons3": ("location", "subpath"),
    # authored fixture
    "adlsgen2": ("location", "subpath"),
    # documentation only
    "googlecloudstorage": ("location", "subpath"),
    # documentation only
    "azureblobstorage": ("location", "subpath"),
    # documentation only
    "s3compatible": ("location", "subpath"),
    # documentation only, and the one row whose field names are not the
    # `location`/`subpath` pair the rest share -- so it is also the row a
    # real export is most likely to disagree with.
    "dataverse": ("environmentDomain", "tableName"),
    # Documentation only for the *payload*. The type name is a real reading:
    # Diabetes_LH's alm.settings.json enumerates Shortcuts.OneDriveSharePoint
    # alongside the other seven. Without this row the payload fell through to
    # the unknown branch and a real SharePoint shortcut was reported as
    # "unrecognised shortcut target shape" with no URL at all, instead of as
    # a location this tool deliberately does not map.
    "onedrivesharepoint": ("location", "subpath"),
}
# Payload key -> the key holding an explicit bucket name. Only S3Compatible
# has one: Microsoft documents its `location` as "HTTP URL of the S3
# compatible endpoint... The URL must be in the non-bucket specific format;
# no bucket should be specified here", so the bucket cannot be recovered from
# the URI and has to travel as a field of its own. Documentation only, like
# the row it belongs to: no S3Compatible shortcut has been seen here.
_BUCKET_KEYS = {"s3compatible": "bucket"}
# Settings keys that might carry the tracked-type list. The schema is not
# fully documented, so several plausible spellings are accepted and anything
# unrecognised degrades to UNKNOWN rather than to a confident wrong answer.
_TRACKED_LIST_KEYS = ("trackedObjectTypes", "trackedObjects", "objectTypes")
_COVERAGE_KEYS = {
    "shortcuts": {"shortcuts", "shortcut"},
    "data_access_roles": {"dataaccessroles", "dar", "onelakesecurity"},
}


def _load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig")), None
    except FileNotFoundError:
        return None, None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, str(exc)


def _join(location, subpath) -> str:
    location = (location or "").rstrip("/")
    subpath = (subpath or "").lstrip("/")
    if not location:
        return ""
    return f"{location}/{subpath}" if subpath else location


# Fabric writes an all-zero GUID to mean "this item, this workspace". Passed
# through literally the drafted path reads
# `onelake://00000000-.../00000000-.../Files/raw`, which names nothing and
# cannot be reviewed.
_ZERO_GUID = "00000000-0000-0000-0000-000000000000"


def _is_zero(guid) -> bool:
    return str(guid or "").strip().casefold() in ("", _ZERO_GUID)


def shortcut_target(target, *, this_item=None):
    """(target_type, uri, external) for one shortcut's target block.

    `uri` is "" when the shape is not recognised — an unknown target is
    reported, never invented.
    """
    if not isinstance(target, dict):
        return ("", "", False)
    target_type = target.get("type")
    target_type = target_type if isinstance(target_type, str) else ""
    key = target_type.casefold()

    if key == "onelake":
        payload = target.get("oneLake") or target.get("onelake") or {}
        if not isinstance(payload, dict):
            return (target_type, "", False)
        workspace = payload.get("workspaceId", "")
        item = payload.get("itemId", "")
        path = str(payload.get("path", "")).lstrip("/")
        # Resolve the item half from the lakehouse that contains the shortcut.
        # The workspace half is not in the export at all, so it is named as
        # "this workspace" rather than printed as zeros.
        if _is_zero(item) and this_item:
            item = this_item
        if _is_zero(workspace):
            workspace = "<this-workspace>"
        uri = f"onelake://{workspace}/{item}/{path}" if item else ""
        return (target_type, uri, False)

    if key in _EXTERNAL_TARGETS:
        location_key, subpath_key = _EXTERNAL_TARGETS[key]
        payload = None
        for candidate_key, candidate in target.items():
            if candidate_key.casefold() == key and isinstance(candidate, dict):
                payload = candidate
                break
        if payload is None:
            return (target_type, "", True)
        return (target_type, _join(payload.get(location_key),
                                   payload.get(subpath_key)), True)

    # Unrecognised type: assume external (conservative — it prompts review)
    # but emit no URI.
    return (target_type, "", bool(target_type))


def shortcut_bucket(target) -> str:
    """The bucket a target records explicitly, or "" when it records none.

    Separate from `shortcut_target` because it is not part of the URI: for
    S3Compatible the endpoint URL and the bucket are two different fields,
    and joining them here would make it impossible to tell later whether a
    leading path segment was the bucket or a folder that shares its name.
    """
    if not isinstance(target, dict):
        return ""
    target_type = target.get("type")
    key = (target_type if isinstance(target_type, str) else "").casefold()
    bucket_key = _BUCKET_KEYS.get(key)
    if not bucket_key:
        return ""
    for candidate_key, candidate in target.items():
        if candidate_key.casefold() == key and isinstance(candidate, dict):
            value = candidate.get(bucket_key)
            return value.strip() if isinstance(value, str) else ""
    return ""


def tracking_coverage(item) -> dict:
    """Which object types this lakehouse tracks in Git, per alm.settings.json."""
    settings, _error = _load_json(item.file(ALM_FILE))
    if not isinstance(settings, dict):
        return {"shortcuts": UNKNOWN, "data_access_roles": UNKNOWN}

    tracked = _tracked_names(settings)
    if tracked is None:
        return {"shortcuts": UNKNOWN, "data_access_roles": UNKNOWN}
    return {field: (TRACKED if (tracked & aliases) else NOT_TRACKED)
            for field, aliases in _COVERAGE_KEYS.items()}


def _split_path(raw_path: str):
    """("Tables"|"Files"|"", schema-or-None) from a shortcut's `path`.

    Real exports write "/Tables/dbo" or "/Files", not "Tables". Comparing the
    raw value against "tables" silently excluded every real table shortcut
    from the catalog, which disabled the rule that stops a shortcut name being
    rewritten to a three-part name AIDP does not have.
    """
    parts = [p for p in (raw_path or "").split("/") if p]
    if not parts:
        return ("", None)
    return (parts[0], parts[1] if len(parts) > 1 else None)


def _normalise(name) -> str:
    return str(name).replace("_", "").replace("-", "").replace(".", "").casefold()


def _tracked_names(settings: dict):
    """The set of object types this lakehouse tracks, or None if unreadable.

    The real schema is `objectTypes: [{"name": "Shortcuts", "state": "Enabled"}]`
    -- a list of objects with an explicit state, not a list of enabled names.
    An earlier guess treated it as a list of strings, which silently reported
    an enabled type as not tracked. A list-of-strings form is still accepted in
    case older exports use one.
    """
    for key in _TRACKED_LIST_KEYS:
        value = settings.get(key)
        if not isinstance(value, list):
            continue
        names = set()
        for entry in value:
            if isinstance(entry, dict):
                if str(entry.get("state", "Enabled")).casefold() != "enabled":
                    continue
                name = entry.get("name")
                if isinstance(name, str):
                    # "Shortcuts.AdlsGen2" counts as "Shortcuts" too.
                    names.add(_normalise(name.split(".")[0]))
            elif isinstance(entry, str):
                names.add(_normalise(entry))
        return names
    return None


def _shortcut_records(raw, this_item=None):
    """(records, reason). `reason` is set when the file is not a shortcut list.

    Valid JSON of the wrong shape used to return `[]` with no complaint, so
    `{"foo": 1}` and a real `[]` were the same answer. They are not: one is a
    file this tool did not understand and the other is a lakehouse with no
    shortcuts.
    """
    if isinstance(raw, dict):
        inner = raw.get("shortcuts")
        if not isinstance(inner, list):
            return ([], "the file is a JSON object with no `shortcuts` list in "
                        "it, so it is not a shortcuts export this tool "
                        "understands")
        raw = inner
    if not isinstance(raw, list):
        return ([], f"expected a JSON list of shortcuts, found "
                    f"{type(raw).__name__}")
    records = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        target_type, uri, external = shortcut_target(
            entry.get("target"), this_item=this_item)
        name = entry.get("name") if isinstance(entry.get("name"), str) else ""
        raw_path = entry.get("path") if isinstance(entry.get("path"), str) else ""
        section, schema = _split_path(raw_path)
        record = {
            "name": name,
            "section": section,
            # The schema segment of the path, on its own. `table_name` glues
            # it to the name, and `plan` needs the two apart: a
            # `Tables/sales` shortcut belongs in the AIDP schema
            # `<lakehouse>_sales`, and splitting the glued form back up would
            # be a second parser for something already parsed here.
            "schema": schema or "",
            "path": raw_path,
            "table_name": (f"{schema}.{name}" if schema else name) if (
                is_table_section(section) and name) else None,
            "target_type": target_type,
            "target": uri,
            "bucket": shortcut_bucket(entry.get("target")),
            "external": external,
        }
        if not uri:
            record["target_error"] = (
                f"unrecognised shortcut target shape for type {target_type!r}; "
                f"resolve the target manually"
            )
        records.append(record)
    return (records, None)


# Fabric writes `[]` for a lakehouse with no shortcuts -- the vendored real
# export Diabetes_LH.Lakehouse has shortcut tracking enabled in
# alm.settings.json and ships a shortcuts.metadata.json containing exactly
# that. So a file that is missing while tracking is on is an incomplete
# export, not a zero, and must not be counted as one.
_MISSING_WHILE_TRACKED = (
    f"{SHORTCUTS_FILE} is missing although {ALM_FILE} says shortcuts are "
    f"tracked. Fabric writes an empty list for a lakehouse with no shortcuts, "
    f"so this is an incomplete export rather than a count of zero"
)


def scan(items, *, log=None) -> dict:
    lakehouses = []
    shortcut_total = external_total = coverage_unknown = 0

    for item in items_of_type(items, "Lakehouse"):
        coverage = tracking_coverage(item)
        path = item.file(SHORTCUTS_FILE)
        present = path.is_file()
        raw, read_error = _load_json(path)
        records, shape_error = _shortcut_records(
            raw, item.logical_id or item.name)

        # Three ways to end up with no usable list, all of which used to read
        # as "tracked (0 found)" -- a claim that the file was read and was
        # empty. Only a file that is present AND parses to a list may produce
        # a count.
        if present:
            error = read_error or shape_error
        elif coverage["shortcuts"] == TRACKED:
            error = _MISSING_WHILE_TRACKED
        else:
            # Tracking off or unknown already explains the absence, and both
            # states already report `shortcut_count: None`.
            error = None

        if error:
            # Not TRACKED: we did not manage to look. UNKNOWN is the state
            # this module already uses for "we did not look", and it is what
            # suppresses the count.
            coverage["shortcuts"] = UNKNOWN
            records = []
        elif present:
            # A file that is present and understood overrides the settings:
            # we can see the data.
            coverage["shortcuts"] = TRACKED

        counted = coverage["shortcuts"] == TRACKED
        record = {
            "name": item.name,
            "logical_id": item.logical_id,
            "folder": item.folder,
            "tracking": coverage,
            "shortcuts": records,
            "shortcut_count": len(records) if counted else None,
        }
        if error:
            record["shortcuts_error"] = f"cannot read {SHORTCUTS_FILE}: {error}"
        lakehouses.append(record)

        if counted:
            shortcut_total += len(records)
            external_total += sum(1 for r in records if r["external"])
        else:
            coverage_unknown += 1
        if log:
            shown = record["shortcut_count"]
            log(f"  lakehouse {item.name} — shortcuts: "
                f"{coverage['shortcuts']}"
                + (f" ({shown} found)" if shown is not None else ""))

    return {
        "summary": {
            "lakehouse_count": len(lakehouses),
            "shortcut_count": shortcut_total,
            "external_shortcut_count": external_total,
            "coverage_unknown_count": coverage_unknown,
        },
        "items": {"lakehouses": lakehouses},
    }
