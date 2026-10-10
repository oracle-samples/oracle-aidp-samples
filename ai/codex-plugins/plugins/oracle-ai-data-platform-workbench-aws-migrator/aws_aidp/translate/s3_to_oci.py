"""Generate a real, reviewable S3 → OCI Object Storage data-copy job.

We do NOT re-implement a byte-copy engine — that's a solved problem. Instead we
generate an [rclone](https://rclone.org) job: rclone has native backends for
both AWS S3 and OCI Object Storage, and gives parallel, resumable,
checksum-verified transfers for free.

Output is a self-contained shell script per bucket (source path + destination +
the exact rclone commands). Nothing moves until a human runs the script — which
keeps the project's "review before you trust" model, and means we're not on the
hook for maintaining a transfer engine.

The script is generated executable code, so it is made shell-safe by
construction rather than by trusting the inputs:

- every value is validated against the strict syntax of its field *before*
  anything is generated (`validate_transfer_inputs`); a name carrying a
  newline, quote, `$`, backtick, `;` or whitespace is refused with a
  ``ValueError`` and no script is produced;
- every value that crosses into bash command syntax additionally goes through
  one quoting helper (`shell_arg`, i.e. ``shlex.quote``), so the script stays
  safe even if a field's domain is widened later;
- the temporary rclone config is removed by an ``EXIT`` trap, so a failed or
  interrupted copy does not leave it behind.
"""
from __future__ import annotations

import re
import shlex
from dataclasses import dataclass, field

OCI_NAMESPACE_PLACEHOLDER = "<your-oci-namespace>"
COMPARTMENT_OCID_PLACEHOLDER = "<compartment-ocid>"

# S3 general-purpose bucket naming rules: 3–63 characters of lowercase letters,
# digits, dots and hyphens; starts and ends with a letter or digit; no adjacent
# dots; not an IPv4 address; no reserved prefixes/suffixes.
_S3_BUCKET_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?")
_S3_BUCKET_IPV4_RE = re.compile(r"\d{1,3}(?:\.\d{1,3}){3}")
_S3_BUCKET_BAD_PREFIXES = ("xn--", "sthree-", "amzn-s3-demo-")
_S3_BUCKET_BAD_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")
# OCI Object Storage bucket names: letters, digits, underscore, dot, hyphen.
_OCI_BUCKET_RE = re.compile(r"[A-Za-z0-9_.-]{1,256}")
# Same rule the planner and the Glue translator already enforce.
_OCI_NAMESPACE_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,254}")
# Region identifiers on both clouds: `us-east-1`, `us-ashburn-1`, `eu-frankfurt-1`.
_REGION_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,62}")
# OCI config profile section names.
_OCI_PROFILE_RE = re.compile(r"[A-Za-z0-9_.-]{1,128}")
# ocid1.<compartment|tenancy>.<realm>.[region].[future-use].<unique id>
_COMPARTMENT_OCID_RE = re.compile(
    r"ocid1\.(?:compartment|tenancy)\.oc[0-9]+(?:\.[a-z0-9-]*){1,2}\.[a-z0-9]+"
)


def shell_arg(value: str) -> str:
    """Quote one value for a bash command line (the single quoting helper)."""
    return shlex.quote(value)


def _s3_bucket_problem(bucket: str) -> str | None:
    if not 3 <= len(bucket) <= 63 or _S3_BUCKET_RE.fullmatch(bucket) is None:
        return (
            "S3 bucket name must be 3-63 lowercase letters, digits, dots or hyphens, "
            "starting and ending with a letter or digit"
        )
    if ".." in bucket:
        return "S3 bucket name must not contain adjacent dots"
    if _S3_BUCKET_IPV4_RE.fullmatch(bucket):
        return "S3 bucket name must not be formatted as an IP address"
    if bucket.startswith(_S3_BUCKET_BAD_PREFIXES):
        return f"S3 bucket name must not start with {', '.join(_S3_BUCKET_BAD_PREFIXES)}"
    if bucket.endswith(_S3_BUCKET_BAD_SUFFIXES):
        return f"S3 bucket name must not end with {', '.join(_S3_BUCKET_BAD_SUFFIXES)}"
    return None


def validate_transfer_inputs(
    bucket: object,
    oci_bucket: object,
    namespace: object,
    *,
    aws_region: object,
    oci_region: object,
    oci_profile: object,
    compartment_ocid: object,
) -> list[str]:
    """Return every syntax problem with the values about to enter the script.

    Each field is held to the strict grammar of what it names, which is far
    narrower than "no shell metacharacters": none of the accepted alphabets
    contain whitespace, newlines, quotes, ``$``, backticks or ``;``. The
    documented placeholders are accepted for the namespace and compartment so
    the demo path still works without an OCI tenancy.
    """
    problems: list[str] = []

    def check(name: str, value: object, pattern: re.Pattern[str], rule: str,
              placeholder: str | None = None) -> None:
        if not isinstance(value, str) or not value:
            problems.append(f"{name} must be a non-empty string")
            return
        if placeholder is not None and value == placeholder:
            return
        if pattern.fullmatch(value) is None:
            problems.append(f"{name} {value!r} is invalid: {rule}")

    if not isinstance(bucket, str) or not bucket:
        problems.append("bucket must be a non-empty string")
    else:
        bucket_problem = _s3_bucket_problem(bucket)
        if bucket_problem:
            problems.append(f"bucket {bucket!r} is invalid: {bucket_problem}")
    check("oci_bucket", oci_bucket, _OCI_BUCKET_RE,
          "OCI bucket names use letters, digits, '_', '.' or '-' (1-256 characters)")
    check("namespace", namespace, _OCI_NAMESPACE_RE,
          "OCI namespaces use lowercase letters, digits, '_' or '-' and start with a "
          "letter or digit", placeholder=OCI_NAMESPACE_PLACEHOLDER)
    check("aws_region", aws_region, _REGION_RE,
          "regions use lowercase letters, digits or '-' (e.g. us-east-1)")
    check("oci_region", oci_region, _REGION_RE,
          "regions use lowercase letters, digits or '-' (e.g. us-ashburn-1)")
    check("oci_profile", oci_profile, _OCI_PROFILE_RE,
          "OCI config profiles use letters, digits, '_', '.' or '-' (1-128 characters)")
    check("compartment_ocid", compartment_ocid, _COMPARTMENT_OCID_RE,
          "expected ocid1.compartment.oc1..<id> or ocid1.tenancy.oc1..<id>",
          placeholder=COMPARTMENT_OCID_PLACEHOLDER)
    return problems


@dataclass
class Finding:
    rule: str
    detail: str
    severity: str

    def __str__(self) -> str:
        return f"[{self.severity}] {self.rule}: {self.detail}"


@dataclass
class TransferResult:
    source_sql: str        # reused by the report as the "before" block
    translated_sql: str    # reused by the report as the "after" block (the script)
    findings: list[Finding] = field(default_factory=list)

    @property
    def changes(self) -> int:
        return sum(1 for f in self.findings if f.severity == "rewrite")

    @property
    def flags(self) -> int:
        return sum(1 for f in self.findings if f.severity == "flag")

    @property
    def needs_manual_review(self) -> bool:
        return self.flags > 0


def build_transfer(
    bucket: str,
    oci_bucket: str,
    namespace: str,
    *,
    aws_region: str | None = None,
    oci_region: str = "us-ashburn-1",
    oci_profile: str = "DEFAULT",
    compartment_ocid: str = COMPARTMENT_OCID_PLACEHOLDER,
) -> TransferResult:
    """Build the rclone transfer script for one bucket.

    Raises ``ValueError`` (listing every offending field) instead of generating
    a script when any value fails `validate_transfer_inputs`; the caller records
    the asset as an error and no ``.transfer.sh`` is written.
    """
    aws_region = aws_region or "us-east-1"
    ns = namespace or OCI_NAMESPACE_PLACEHOLDER

    problems = validate_transfer_inputs(
        bucket, oci_bucket, ns,
        aws_region=aws_region, oci_region=oci_region,
        oci_profile=oci_profile, compartment_ocid=compartment_ocid,
    )
    if problems:
        raise ValueError(
            "refusing to generate transfer script for unsafe input: " + "; ".join(problems)
        )

    script = _render_script(bucket, oci_bucket, ns, aws_region, oci_region,
                            oci_profile, compartment_ocid)
    findings = [
        Finding("rclone_transfer",
                f"Generated rclone copy job  s3://{bucket} → oci://{oci_bucket}@{ns} "
                "(parallel, resumable, checksum-verified — proven engine, not a custom copier)",
                "rewrite"),
        Finding("transfer_prereqs",
                "Before running: install rclone, set AWS creds (env/profile), and an "
                "~/.oci/config profile with a real compartment OCID.",
                "flag"),
    ]
    before = f"s3://{bucket}/   (AWS S3 bucket, {aws_region})"
    return TransferResult(source_sql=before, translated_sql=script, findings=findings)


def _render_script(
    bucket: str,
    oci_bucket: str,
    ns: str,
    aws_region: str,
    oci_region: str,
    oci_profile: str,
    compartment_ocid: str,
) -> str:
    """Render the bash script for already-validated values.

    Values inside the quoted ``<<'RCLONE'`` heredoc are never interpreted by
    bash; every value that reaches a command line is passed through
    `shell_arg` here, and only here. Comment lines are not shell syntax.
    """
    src = shell_arg(f"awssrc:{bucket}")
    dst = shell_arg(f"ocidest:{oci_bucket}")
    done = shell_arg(f"done: s3://{bucket} -> oci://{oci_bucket}@{ns}")

    return f"""#!/usr/bin/env bash
# Copy s3://{bucket}  →  oci://{oci_bucket}@{ns}
# Generated by aws-aidp-migrator. Uses rclone (native S3 + OCI backends).
# Review, then run:  bash {bucket}.transfer.sh
set -euo pipefail

# --- rclone config (written to a temp file; no global config touched) ---
CONF="$(mktemp)"
# Removed on every exit path (success, failure, Ctrl-C) so the config never
# outlives the job.
trap 'rm -f "$CONF"' EXIT
cat > "$CONF" <<'RCLONE'
# awssrc uses env_auth: AWS_PROFILE / ~/.aws/credentials / IAM role
[awssrc]
type = s3
provider = AWS
env_auth = true
region = {aws_region}

[ocidest]
type = oracleobjectstorage
provider = user_principal_auth
namespace = {ns}
region = {oci_region}
compartment = {compartment_ocid}
config_file = ~/.oci/config
config_profile = {oci_profile}
RCLONE

# --- ensure the destination bucket exists, then copy ---
rclone --config "$CONF" mkdir {dst} || true
rclone --config "$CONF" copy {src} {dst} \\
    --checksum --transfers 16 --fast-list --progress

echo {done}
"""
