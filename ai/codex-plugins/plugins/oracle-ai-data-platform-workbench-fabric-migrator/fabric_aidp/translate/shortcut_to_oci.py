"""Map a Fabric shortcut's target onto an OCI Object Storage location.

A shortcut makes data that lives in S3, ADLS Gen2, GCS or Blob appear as a
table or folder inside a lakehouse. Migrating one means naming where that data
will live on OCI.

`aws-aidp` preserves the source container name verbatim as the OCI bucket
name, and that convention is kept here wherever it is sound -- but it is only
sound when the source name already identifies the data on its own:

  AmazonS3            bucket names are unique across all of AWS        verbatim
  GoogleCloudStorage  bucket names are unique across all of GCS        verbatim
  AdlsGen2 / Blob     container names are unique only inside one       qualified
                      storage account
  S3Compatible        bucket names are unique only inside one          qualified
                      endpoint

aws-aidp only ever sees S3, where verbatim is right. Carried to Azure it is
not: two storage accounts each holding a container called `data` or `raw` is
an ordinary enterprise shape, and both shortcuts used to come out as
`oci://data@ns/...` -- one target for two genuinely different sources, with no
flag. Where the source name is not self-identifying the name that scopes it --
the storage account, the S3-compatible endpoint -- is prepended:

    https://accta.dfs.core.windows.net/data/x  ->  oci://accta_data@ns/x
    https://acctb.dfs.core.windows.net/data/x  ->  oci://acctb_data@ns/x

The separator is `_` because it is the one character OCI allows in a bucket
name (letters, digits, `-`, `_`, `.`) that cannot appear in any of the names
being joined: an Azure storage account (3-24 lowercase alphanumerics), an
Azure container (lowercase alphanumerics and `-`), a DNS hostname, or an S3
bucket (lowercase alphanumerics, `-` and `.`). So the join is reversible and
two distinct sources can never be spelled the same way.

The account, not the endpoint, is what scopes an Azure container: multi-
protocol access means `acct.blob.core.windows.net/fs` and
`acct.dfs.core.windows.net/fs` are two endpoints onto the same bytes, and
they must keep resolving to one bucket.

This tool does not move data (spec §3), so the artifact is a reviewable note
rather than executable code: source location, proposed target, and an explicit
reminder that the copy is a separate step.
"""
from __future__ import annotations

import re

from fabric_aidp.sources import is_table_section
from fabric_aidp.translate.types import Finding, TranslationResult

_OCI_BUCKET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
# target type -> pattern capturing `container` and an optional `key`
_PATTERNS = {
    "amazons3": (
        re.compile(r"^s3a?://(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
        re.compile(r"^https?://(?P<container>[^./]+)\.s3[.-][^/]*amazonaws\.com"
                   r"(?:/(?P<key>.*))?$", re.I),
    ),
    # `authority`, where a pattern captures one, is the name the container is
    # only unique inside; it is prepended to the bucket. See the module
    # docstring for why S3 and GCS deliberately capture none.
    "adlsgen2": (
        re.compile(r"^https?://(?P<authority>[^./]+)\.dfs\.core\.windows\.net/"
                   r"(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
        re.compile(r"^abfss?://(?P<container>[^@/]+)@(?P<authority>[^./]+)"
                   r"(?:\.[^/]*)?(?:/(?P<key>.*))?$", re.I),
    ),
    "azureblobstorage": (
        re.compile(r"^https?://(?P<authority>[^./]+)\.blob\.core\.windows\.net/"
                   r"(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
    ),
    "googlecloudstorage": (
        re.compile(r"^gs://(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
        re.compile(r"^https?://storage\.googleapis\.com/"
                   r"(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
        # The shape Fabric actually emits, and the only one that was
        # missing: `googleCloudStorage.location` is documented as
        # "https://[bucket-name].storage.googleapis.com". The two forms
        # above are valid GCS spellings a human might paste, but Fabric
        # writes neither, so every real GCS shortcut was refused as
        # unparseable. The path-style form above cannot be swallowed by
        # this one -- `storage.googleapis.com` leaves `.googleapis.com`
        # where `.storage.googleapis.com` is required.
        re.compile(r"^https?://(?P<container>[^./]+)\.storage\.googleapis\.com"
                   r"(?:/(?P<key>.*))?$", re.I),
    ),
    # S3Compatible is path-style: the endpoint is the whole host and the
    # bucket is the first path segment, or -- preferred -- the target's own
    # `bucket` field. The virtual-host pattern the other types use named the
    # endpoint's first host label, so `https://minio.internal.corp/acme-prod/x`
    # came out as bucket `minio`.
    "s3compatible": (
        # No endpoint in the scheme form, so nothing to qualify by; this is
        # not a shape Fabric emits, only one a human might hand-write.
        re.compile(r"^s3a?://(?P<container>[^/?#]+)(?:/(?P<key>.*))?$", re.I),
        re.compile(r"^https?://(?P<authority>[^/?#]+)(?:/(?P<path>.*))?$", re.I),
    ),
}


# The one character OCI allows in a bucket name that cannot occur in a storage
# account, an Azure container, a DNS host or an S3 bucket name -- so
# `<authority>_<container>` always splits back into the pair that made it.
_AUTHORITY_SEP = "_"


def _qualify(authority, container) -> str:
    """The OCI bucket name for `container` scoped by `authority`."""
    return f"{authority}{_AUTHORITY_SEP}{container}" if authority else container


def _split_path_style(path, declared):
    """(container, key, reason) for a path-style endpoint URL.

    The declared bucket wins outright and the path is then the key verbatim,
    with nothing stripped: Microsoft documents `s3Compatible.location` as
    "the non-bucket specific format; no bucket should be specified here", so
    a leading segment there is a folder and never a second bucket claim.
    Stripping it "in case" would silently drop a real folder whose name
    happens to match the bucket.

    With no declared bucket the first path segment is the bucket -- that is
    what path-style addressing means -- and an endpoint with no path at all
    names no bucket, so it is refused rather than guessed at.
    """
    parts = [p for p in (path or "").split("/") if p]
    if declared:
        return (declared, "/".join(parts), None)
    if not parts:
        return (None, None,
                "no bucket: the location is an endpoint with no path and the "
                "shortcut records no `bucket` field, so nothing names which "
                "bucket the data is in")
    return (parts[0], "/".join(parts[1:]), None)


def map_target(target_type, uri, *, namespace, bucket=None):
    """(oci_uri, reason). Exactly one is non-None.

    `bucket` is the target's own bucket field, which only S3Compatible has.
    It is preferred over anything read out of `uri`, because for that type
    the URI is documented not to carry a bucket at all.
    """
    kind = (target_type or "").casefold()
    if kind == "onelake":
        return (None, "internal OneLake shortcut: resolve the target lakehouse and "
                      "migrate that item, then re-point this reference")
    if kind not in _PATTERNS:
        return (None, f"{target_type or 'unknown'} is not object storage; there is no "
                      f"OCI Object Storage equivalent to map it to")
    if not isinstance(uri, str) or not uri.strip():
        return (None, "the shortcut has no recorded target location")
    declared = bucket.strip() if isinstance(bucket, str) else ""
    for pattern in _PATTERNS[kind]:
        match = pattern.match(uri.strip())
        if not match:
            continue
        groups = match.groupdict()
        if "path" in groups:
            container, key, reason = _split_path_style(groups.get("path"), declared)
            if reason:
                return (None, reason)
        else:
            container = groups["container"]
            key = (groups.get("key") or "").strip("/")
            if declared and declared != container:
                # Both halves name a bucket and they are different. Either
                # could be the stale one, so there is nothing to prefer.
                return (None, f"the shortcut records bucket {declared!r} but its "
                              f"location names {container!r}; resolve which "
                              f"bucket the data is in and set the target by hand")
        target_bucket = _qualify(groups.get("authority"), container)
        if not _OCI_BUCKET_RE.fullmatch(target_bucket):
            return (None, f"source container {container!r} gives target bucket "
                          f"{target_bucket!r}, which is not a valid OCI bucket "
                          f"name; choose a target bucket manually")
        return (f"oci://{target_bucket}@{namespace}" + (f"/{key}" if key else ""),
                None)
    return (None, f"could not parse {uri!r} as a {target_type} location")


def translate(shortcut, *, namespace) -> TranslationResult:
    """Produce a reviewable note for one shortcut."""
    shortcut = shortcut if isinstance(shortcut, dict) else {}
    name = shortcut.get("name", "<unnamed>")
    section = shortcut.get("section", "")
    path = shortcut.get("path", "")
    target_type = shortcut.get("target_type", "")
    source_uri = shortcut.get("target", "")
    bucket = shortcut.get("bucket", "")
    is_table = is_table_section(section)
    oci_uri, reason = map_target(target_type, source_uri, namespace=namespace,
                                 bucket=bucket)

    lines = [
        f"# Fabric shortcut: {name}",
        f"#   section:     {section}",
    ]
    # `Tables/dbo` and `Tables` put the same shortcut name in two different
    # places, so the section on its own does not say where it was.
    if path and path.strip("/") != section:
        lines.append(f"#   path:        {path}")
    lines += [
        f"#   target type: {target_type}",
        f"#   source:      {source_uri or '(none recorded)'}",
    ]
    # An S3Compatible endpoint URL is non-bucket-specific, so the source line
    # alone does not say where the data is. Print the bucket beside it.
    if bucket:
        lines.append(f"#   bucket:      {bucket}")
    if oci_uri:
        lines.append(f"#   AIDP target: {oci_uri}")
        lines.append("#")
        lines.append(
            "# This tool does not copy data. Move the objects to the target bucket,")
        lines.append(
            "# then register the location as an AIDP external table."
            if is_table else
            "# then decide what reads them: this is a folder, not a table.")
        findings = [Finding(
            "SC10_SHORTCUT_TARGET",
            f"{target_type} shortcut {name!r}: {source_uri} -> {oci_uri}", "rewrite")]
    else:
        lines += ["#   AIDP target: NOT MAPPED", f"#   reason:      {reason}"]
        findings = [Finding(
            "SC11_SHORTCUT_UNMAPPED",
            f"shortcut {name!r} could not be mapped: {reason}", "flag")]
    if not is_table:
        # Said in the report as well as in the note. The plan used to give
        # this asset the target type `aidp_dcat_external_table`, so a
        # reader of the plan or of the report was told a table would be
        # created where there is no table -- `Files/adls_landing` is a
        # folder of objects. It is migrated as a location and the decision
        # that has no automatic answer is handed back, named.
        findings.append(Finding(
            "SC12_NOT_A_TABLE",
            f"shortcut {name!r} is in the "
            f"{section or 'unrecognised'} section, not Tables: it is a folder "
            f"of objects, and AIDP has no external-table registration for a "
            f"folder. It is migrated as an object storage location; if "
            f"something read it as a table in Fabric, define that table over "
            f"the copied objects by hand", "flag"))
    return TranslationResult(source_uri, "\n".join(lines) + "\n", findings)
