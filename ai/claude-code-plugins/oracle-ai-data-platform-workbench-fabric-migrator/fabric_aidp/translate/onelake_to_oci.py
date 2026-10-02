"""Map Fabric OneLake locations onto OCI Object Storage URIs.

Five recognised forms:

  1. abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<Item>.<Type>/<rest>
  2. abfss://<ws-guid>@onelake.dfs.fabric.microsoft.com/<item-guid>/<rest>
  3. https://onelake.dfs.fabric.microsoft.com/<workspace>/<Item>.<Type>/<rest>
     (and the `blob` endpoint, which serves the same storage)
  4. /lakehouse/default/<rest>                      (the notebook's local FUSE mount)
  5. Files/<rest> or Tables/<rest>                  (relative to the default lakehouse)

Form 3 names exactly what form 1 names, with the workspace as a path
segment rather than the URI authority, and it is the spelling Fabric's own
portal hands you under "Copy URL". It was not recognised, so a literal in
it was neither rewritten nor flagged: it reached the artifact untouched,
with no finding, and the notebook graded PASS.

Deliberately still not here, and measured silent: `wasbs://...` and
`abfss://<container>@<account>.dfs.core.windows.net/...`. Those are Azure
Storage, not OneLake -- a different system with different credentials --
so this module has no mapping for them and inventing one would be a guess.
`shortcut_to_oci` maps them where a *shortcut* names them, which is where
the export says what they are. A notebook holding one bare is a real gap
and needs a rule of its own, not a branch here.

The bucket name is built from everything the location writes down about the
Fabric item, and nothing it does not:

    abfss://ws@.../Sales.Lakehouse/Files/x  ->  oci://ws_Sales_Lakehouse@ns/Files/x
    abfss://ws@.../Sales.Warehouse/Files/x  ->  oci://ws_Sales_Warehouse@ns/Files/x
    Files/x  (bound to Sales)               ->  oci://Sales@ns/Files/x

The item name alone used to be the whole bucket name, which is the defect
`shortcut_to_oci` fixed for Azure containers in PR #9 and the same fix
lands here: a Fabric display name is unique only inside one workspace and
one item type, so on its own it is not self-identifying, and a Lakehouse
and a Warehouse both called `Sales`, or the same `Sales` in two
workspaces, all came out as `oci://Sales@ns/...` with nothing said. The
join is `<authority>_<name>` with `_`, the same shape and the same
separator `shortcut_to_oci` uses, so the two mappings onto one object
store agree about how a bucket name is built.

Where `shortcut_to_oci` deliberately diverges from it: `_` is injective
there because no name it joins -- an Azure storage account, an Azure
container, a DNS host, an S3 bucket -- can contain one. A Fabric workspace
display name can. The composed name still splits back while the workspace
has no `_` (the item type never does, so the first and last `_` bound the
item however many it contains), and where that does not hold the mapping
refuses rather than emit a name two different items could both produce.

The other divergence, and it is a real cost: forms 3 and 4 name no
workspace, and a Fabric Git export records the bound lakehouse's workspace
nowhere -- `default_lakehouse_workspace_id` is a GUID and is usually
empty. Nothing is discarded there, so nothing is added, and the item name
stays the whole bucket name. One lakehouse reached both ways therefore
gets two bucket names. That is visible in the artifact and a human can
merge it; the collision it replaces -- two workspaces' data behind one
bucket -- was not visible at all. `fabric_notebook_to_spark` reports the
case it can see, NB25.

A name that is not a legal OCI bucket name is reported, never silently
mangled -- the same choice the AWS planner makes for a location URI it
cannot convert.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import unquote

from fabric_aidp.namespace import PLACEHOLDER, validated_namespace

# Workspace and item segments may contain spaces — Fabric display names allow
# them, and they appear both literally and %20-encoded in notebook source. Only
# the URI delimiters are excluded.
_ABFSS_RE = re.compile(
    r"^abfss://(?P<ws>[^/@?#]+)@onelake\.dfs\.fabric\.microsoft\.com"
    r"/(?P<item>[^/?#]+)(?P<rest>/.*)?$",
    re.IGNORECASE | re.DOTALL,
)
# The same location over OneLake's HTTPS endpoints. Fabric's portal gives
# this spelling under "Copy URL" / "Properties", the OneLake REST API
# documents it, and it is the only form `requests`, `pandas.read_parquet`,
# `duckdb` and `azcopy` can be handed -- none of them speaks `abfss://`, so
# a notebook that does anything but Spark with a OneLake file writes this
# one. It names exactly what the abfss form names, in the same order:
# `/<workspace>/<item>/<rest>`, with the workspace as a path segment rather
# than the URI authority. `dfs` and `blob` are the two endpoints OneLake
# serves; both resolve to the same storage.
#
# Measured before this was recognised: `is_onelake_path` said no, so the
# literal was neither rewritten nor flagged -- it reached the artifact
# untouched with no finding at all, and the notebook graded PASS. A silent
# miss, which is the worst of the three outcomes.
_HTTPS_RE = re.compile(
    r"^https://onelake\.(?:dfs|blob)\.fabric\.microsoft\.com"
    r"/(?P<ws>[^/?#]+)/(?P<item>[^/?#]+)(?P<rest>/.*)?$",
    re.IGNORECASE | re.DOTALL,
)
_FUSE_RE = re.compile(r"^/lakehouse/default(?P<rest>/.*)?$")
_RELATIVE_RE = re.compile(r"^(?P<rest>(?:Files|Tables)(?:/.*)?)$")
_GUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
_ITEM_SUFFIX_RE = re.compile(r"^(?P<name>.+)\.(?P<type>[A-Za-z]+)$")
_OCI_BUCKET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
# Both re-exported from fabric_aidp/namespace.py, which is where the OCI
# namespace rule lives now; the copies here and in plan/planner.py disagreed
# about nothing yet, which is the only reason they had not caused a bug.
NAMESPACE_PLACEHOLDER = PLACEHOLDER

# The same separator `shortcut_to_oci` joins `<authority>_<container>` with,
# for the same reason: the two mappings land in one object store and a bucket
# name has to mean one thing there. See the module docstring for where the
# injectivity argument holds here and where it is enforced instead.
BUCKET_SEP = "_"

FORM_ABFSS = "abfss"
FORM_FUSE = "fuse"
FORM_RELATIVE = "relative"


@dataclass(frozen=True)
class OneLakeLocation:
    """What a OneLake path says, before any OCI name is built from it.

    Split out of `map_onelake_path` because two callers need the parse and
    only one of them needs a bucket: the rule that recognises
    `<item>/Tables/<table>` as a *table* has to name the owning item and
    the table, and building an oci:// URI first and picking it apart again
    is how that rule came to disagree with this one.
    """
    form: str = ""           # FORM_ABFSS | FORM_FUSE | FORM_RELATIVE
    item: str = ""           # the Lakehouse or Warehouse the path is under
    item_type: str = ""      # "Lakehouse" / "Warehouse" / "" when unwritten
    workspace: str = ""      # "" for the forms that name none
    rest: str = ""           # "/Files/raw/x.csv", "" at the item root
    reason: str = None       # why the owning item could not be determined


@dataclass(frozen=True)
class PathMapping:
    mapped: str = None       # the oci:// URI when mapping succeeded
    lakehouse: str = None    # the lakehouse this resolved to, when known
    reason: str = None       # why a OneLake path could not be mapped
    location: OneLakeLocation = None   # the parse `mapped` was built from

    @property
    def is_onelake(self) -> bool:
        return self.mapped is not None or self.reason is not None


_NOT_ONELAKE = PathMapping(None, None, None)


_validated_namespace = validated_namespace


def is_fuse_path(path) -> bool:
    """Whether `path` is Fabric's local FUSE mount, `/lakehouse/default/...`.

    The one OneLake form a *local* file API can read: inside a Fabric
    notebook `open("/lakehouse/default/Files/x.csv")` works, and so do
    pathlib, os.path and pandas. The other three forms are object-store
    locations `open()` could never read, so a notebook handing one of those
    to a local API was already broken before any migration.

    Callers use this to decide whether rewriting to oci:// would break
    something that worked; it is not part of the mapping.
    """
    if not isinstance(path, str) or not path.strip():
        return False
    return bool(_FUSE_RE.match(path))


def is_onelake_path(path) -> bool:
    if not isinstance(path, str) or not path.strip():
        return False
    return bool(
        _ABFSS_RE.match(path) or _HTTPS_RE.match(path)
        or _FUSE_RE.match(path) or _RELATIVE_RE.match(path)
    )


# Azure Storage, which is not OneLake and is not mapped. Matched on the
# *host*, not the scheme: `abfss://` is the scheme OneLake uses too, and
# the whole point of this predicate is that `@acct.dfs.core.windows.net`
# and `@onelake.dfs.fabric.microsoft.com` are different systems written
# the same way. `adl://` is ADLS Gen1, whose host carries no `@`.
#
# Only Microsoft's storage hosts. `s3://` and `gs://` are deliberately
# absent: this predicate exists because an `abfss://` URI reads as a
# OneLake path at a glance and is not one, and no such confusion is
# possible with the other clouds' schemes.
_AZURE_STORAGE_RE = re.compile(
    r"^(?:abfss?|wasbs?)://[^/?#]*@[^/?#]*\.(?:blob|dfs)\.core\.windows\.net"
    r"(?:[:/?#].*)?$|^adl://[^/?#]*\.azuredatalakestore\.net(?:[:/?#].*)?$",
    re.IGNORECASE | re.DOTALL)


def is_azure_storage_path(path) -> bool:
    """Whether `path` is an Azure Storage location rather than a OneLake one.

    `abfss://container@account.dfs.core.windows.net/...` and
    `abfss://ws@onelake.dfs.fabric.microsoft.com/...` differ only in the
    host, and only the second is OneLake. The first is an ordinary Azure
    Storage account that happens to be reachable from a Fabric notebook,
    and this tool maps OneLake locations and nothing else -- so it is left
    exactly as written, which is right, and used to be left *silently*,
    which is not: a reader of the report could not tell "there was no such
    path" from "there was one and it was not mapped".
    """
    if not isinstance(path, str) or not path.strip():
        return False
    return bool(_AZURE_STORAGE_RE.match(path.strip()))


def _bound_item(default_lakehouse: str, guid_index: dict):
    """(item, reason) for a notebook's bound lakehouse. One is always None.

    `FabricNotebook.default_lakehouse` returns the display name when the
    binding has one and the GUID when it does not, and a Fabric Git export
    often writes only the GUID. A GUID is a legal OCI bucket name, so the
    forms that take their item from the binding -- FUSE and relative --
    produced `oci://2925655f-0293-4f32-8bc6-86ab989099a7@ns/Files/raw/x` and
    reported it as NB01_ONELAKE_PATH, a rewrite: a success, naming a bucket
    nobody would create, in a report that counts it as work done.

    The abfss branch above already refuses an unresolvable item GUID for
    exactly this reason. This is the same refusal for the other two forms,
    so the three agree: a GUID resolves through `guid_index` when the index
    knows it, and is reported rather than emitted when it does not.
    """
    if _GUID_RE.match(default_lakehouse.strip()):
        resolved = guid_index.get(default_lakehouse.strip().lower())
        if not resolved:
            return (None,
                    f"this notebook's default lakehouse is recorded only as "
                    f"the GUID {default_lakehouse.strip()}, and no item in "
                    f"this export names it, so the lakehouse this path is "
                    f"relative to cannot be named; a GUID is a legal bucket "
                    f"name and emitting one would look like a success. Bind "
                    f"the notebook to a named lakehouse, or map it by hand")
        return (resolved, None)
    return (default_lakehouse, None)


def parse_onelake_path(path, *, default_lakehouse=None,
                       guid_index=None) -> OneLakeLocation:
    """What `path` names, or None when it is not a OneLake location."""
    if not isinstance(path, str) or not path.strip():
        return None
    guid_index = {k.lower(): v for k, v in (guid_index or {}).items()}

    match = _ABFSS_RE.match(path) or _HTTPS_RE.match(path)
    if match:
        # Both are URIs, so both segments may be percent-encoded. Decode
        # before matching the ".Lakehouse" suffix, or "My%20Sales.Lakehouse"
        # and "My Sales.Lakehouse" would take different branches.
        workspace = unquote(match.group("ws"))
        item = unquote(match.group("item"))
        rest = match.group("rest") or ""
        suffix = _ITEM_SUFFIX_RE.match(item)
        if suffix:
            return OneLakeLocation(FORM_ABFSS, suffix.group("name"),
                                   suffix.group("type"), workspace, rest)
        if _GUID_RE.match(item):
            resolved = guid_index.get(item.lower())
            if not resolved:
                return OneLakeLocation(
                    FORM_ABFSS, "", "", workspace, rest,
                    f"item GUID {item} is not in this workspace export; "
                    f"cannot resolve the target lakehouse")
            # A GUID URI writes no item type, and the index is built from
            # notebook lakehouse bindings, so claiming one here would be
            # inventing it. The workspace GUID is written down and is kept.
            return OneLakeLocation(FORM_ABFSS, resolved, "", workspace, rest)
        return OneLakeLocation(FORM_ABFSS, item, "", workspace, rest)

    match = _FUSE_RE.match(path)
    if match:
        rest = match.group("rest") or ""
        if not default_lakehouse:
            return OneLakeLocation(
                FORM_FUSE, "", "", "", rest,
                "/lakehouse/default path, but this notebook has no default "
                "lakehouse binding")
        item, reason = _bound_item(default_lakehouse, guid_index)
        if reason:
            return OneLakeLocation(FORM_FUSE, "", "", "", rest, reason)
        return OneLakeLocation(FORM_FUSE, item, "", "", rest)

    match = _RELATIVE_RE.match(path)
    if match:
        rest = "/" + match.group("rest")
        if not default_lakehouse:
            return OneLakeLocation(
                FORM_RELATIVE, "", "", "", rest,
                "relative OneLake path, but this notebook has no default "
                "lakehouse binding")
        item, reason = _bound_item(default_lakehouse, guid_index)
        if reason:
            return OneLakeLocation(FORM_RELATIVE, "", "", "", rest, reason)
        return OneLakeLocation(FORM_RELATIVE, item, "", "", rest)

    return None


def bucket_name(location: OneLakeLocation):
    """(bucket, reason) for the item `location` names. One is always None."""
    parts = [part for part in (location.workspace, location.item,
                               location.item_type) if part]
    if location.workspace and BUCKET_SEP in location.workspace:
        # With a separator-free workspace the name splits back: the first
        # `_` ends the workspace and the last one begins the item type,
        # which leaves the item whatever it contains. A workspace with a
        # `_` breaks that, and `ws_a` + `b_c` and `ws` + `a_b_c` would be
        # the same bucket -- two different items, one name, which is the
        # defect this join exists to prevent.
        return (None,
                f"workspace {location.workspace!r} contains {BUCKET_SEP!r}, the "
                f"character that separates it from the item name, so the bucket "
                f"name it would produce could also come from a different "
                f"workspace and item; choose a target bucket manually")
    bucket = BUCKET_SEP.join(parts)
    if not _OCI_BUCKET_RE.fullmatch(bucket):
        return (None,
                f"item {location.item!r} gives target bucket {bucket!r}, which is "
                f"not a valid OCI bucket name; choose a target bucket manually")
    return (bucket, None)


def oci_uri(bucket, namespace, rest) -> str:
    """The one place an `oci://` URI is spelled.

    Two callers: `map_onelake_path` below, for a path written out in a
    notebook, and the Dataflow lakehouse-file read, which has no path to
    parse -- a Power Query navigation gives it an item and a key directly.
    The Dataflow read used to build its own
    `"oci://%s@%s/%s/%s" % (...)`, which is how it came to skip
    `bucket_name` and everything `bucket_name` checks.
    """
    return f"oci://{bucket}@{namespace}{rest}"


def item_location(item, rest) -> OneLakeLocation:
    """A location for a caller that has the item but no OneLake path.

    A Dataflow navigates `Lakehouse.Contents(){[lakehouseId = ...]}`, which
    writes down neither a workspace name nor an item type -- the same
    information a `/lakehouse/default/...` FUSE path carries, and it is
    given the same shape here so `bucket_name` treats the two alike. The
    result is the bare item name, which is exactly what the FUSE and
    relative notebook forms produce for the same lakehouse.

    The workspace is left empty although a Dataflow navigation often does
    carry a `workspaceId`. That is a GUID, not a workspace name; splicing
    it in would give `<guid>_SalesLake`, which matches neither the notebook
    abfss form (`<workspaceName>_<item>_Lakehouse`) nor the FUSE form
    (`<item>`) -- a third convention rather than agreement with either.
    The item type is left empty for the same reason: `Lakehouse.Contents`
    does say the item is a Lakehouse, but adding `_Lakehouse` here would
    stop the Dataflow agreeing with the FUSE and relative forms, which it
    agrees with today, and still not match the abfss one.
    """
    return OneLakeLocation(FORM_RELATIVE, str(item or ""), "", "", rest)


def map_onelake_path(path, *, namespace, default_lakehouse=None,
                     guid_index=None) -> PathMapping:
    """Map one OneLake location. Returns _NOT_ONELAKE for anything else."""
    namespace = _validated_namespace(namespace)
    location = parse_onelake_path(path, default_lakehouse=default_lakehouse,
                                  guid_index=guid_index)
    if location is None:
        return _NOT_ONELAKE
    if location.reason:
        return PathMapping(None, None, location.reason, location)
    bucket, reason = bucket_name(location)
    if reason:
        return PathMapping(None, location.item, reason, location)
    return PathMapping(oci_uri(bucket, namespace, location.rest),
                       location.item, None, location)
