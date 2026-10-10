"""Tests for the S3 → OCI rclone transfer generator.

    python3 -m tests.test_s3_to_oci
"""
from __future__ import annotations

import shlex

from aws_aidp.translate.s3_to_oci import (
    COMPARTMENT_OCID_PLACEHOLDER,
    OCI_NAMESPACE_PLACEHOLDER,
    _render_script,
    build_transfer,
    shell_arg,
    validate_transfer_inputs,
)

# Names that must never reach bash syntax unquoted: command substitution,
# both quote styles, a command separator, whitespace and a heredoc breakout.
HOSTILE_NAMES = [
    "bucket$(touch pwned)",
    "bucket`touch pwned`",
    "it's-a-bucket",
    'say "hi"',
    "bucket;rm -rf /",
    "two words",
    "tab\tname",
    "bucket\nRCLONE\nrm -rf /\n",
]

_FIELDS = ("bucket", "oci_bucket", "namespace", "aws_region",
           "oci_region", "oci_profile", "compartment_ocid")


def _build(**overrides):
    kwargs = dict(bucket="my-bucket", oci_bucket="my-bucket", namespace="ns123")
    kwargs.update(overrides)
    return build_transfer(kwargs.pop("bucket"), kwargs.pop("oci_bucket"),
                          kwargs.pop("namespace"), **kwargs)


def _refusal(**overrides) -> str | None:
    """Return the ValueError message, or None when a script was generated."""
    try:
        _build(**overrides)
    except ValueError as exc:
        return str(exc)
    return None


def _command_lines(script: str) -> list[str]:
    """Join the backslash-continued rclone commands into single lines."""
    joined: list[str] = []
    pending = ""
    for line in script.splitlines():
        if line.endswith("\\"):
            pending += line[:-1]
            continue
        joined.append(pending + line)
        pending = ""
    return [line for line in joined if line.lstrip().startswith("rclone ")]


def test_generates_rclone_copy():
    r = build_transfer("my-bucket", "my-bucket", "ns123", aws_region="eu-west-1")
    s = r.translated_sql
    assert "rclone" in s and "copy" in s
    assert "awssrc:my-bucket" in s
    assert "ocidest:my-bucket" in s
    assert "type = oracleobjectstorage" in s   # native OCI backend, not a custom copier
    assert "type = s3" in s
    assert "namespace = ns123" in s
    assert "region = eu-west-1" in s


def test_flags_prereqs():
    r = build_transfer("b-1", "b-1", "ns")
    assert r.flags >= 1
    assert any(f.rule == "transfer_prereqs" for f in r.findings)
    assert r.needs_manual_review


def test_reports_the_transfer_as_a_change():
    r = build_transfer("b-1", "b-1", "ns")
    assert r.changes >= 1
    assert any(f.rule == "rclone_transfer" for f in r.findings)


def test_shell_arg_is_shlex_quote():
    for name in HOSTILE_NAMES:
        assert shell_arg(name) == shlex.quote(name)
        assert shlex.split(shell_arg(name)) == [name]


def test_trap_cleanup_follows_mktemp():
    lines = _build().translated_sql.splitlines()
    conf = lines.index('CONF="$(mktemp)"')
    after = [ln for ln in lines[conf + 1:] if ln and not ln.startswith("#")]
    assert after[0] == "trap 'rm -f \"$CONF\"' EXIT"
    # Cleanup is owned by the trap; no trailing rm that a failure could skip.
    assert 'rm -f "$CONF"' not in [ln.strip() for ln in lines]


def test_rclone_arguments_are_quoted_remote_names():
    s = _build(bucket="my.src-bucket", oci_bucket="My_Dest.Bucket-1").translated_sql
    mkdir_line, copy_line = _command_lines(s)
    assert shlex.split(mkdir_line)[:5] == [
        "rclone", "--config", "$CONF", "mkdir", "ocidest:My_Dest.Bucket-1",
    ]
    argv = shlex.split(copy_line)
    assert argv[:6] == [
        "rclone", "--config", "$CONF", "copy",
        "awssrc:my.src-bucket", "ocidest:My_Dest.Bucket-1",
    ]
    assert "--checksum" in argv


def test_rendering_quotes_hostile_values_even_without_validation():
    # Defence in depth: if a field's validation is ever widened, the quoting
    # helper alone must still keep hostile names inside a single argument.
    for name in HOSTILE_NAMES:
        if "\n" in name:
            continue  # a newline is refused by validation, not quoted (see below)
        s = _render_script(name, name, "ns", "us-east-1", "us-ashburn-1",
                           "DEFAULT", COMPARTMENT_OCID_PLACEHOLDER)
        mkdir_line, copy_line = _command_lines(s)
        assert shlex.split(mkdir_line)[4] == f"ocidest:{name}"
        argv = shlex.split(copy_line)
        assert argv[4] == f"awssrc:{name}"
        assert argv[5] == f"ocidest:{name}"
        echo_line = [ln for ln in s.splitlines() if ln.startswith("echo ")][0]
        assert shlex.split(echo_line) == ["echo", f"done: s3://{name} -> oci://{name}@ns"]
        assert f'"awssrc:{name}"' not in s and f'"ocidest:{name}"' not in s


def test_refuses_hostile_values_in_every_field():
    for field_name in _FIELDS:
        for name in HOSTILE_NAMES:
            message = _refusal(**{field_name: name})
            assert message is not None, (field_name, name)
            assert "refusing to generate transfer script" in message
            assert field_name in message


def test_refusal_reports_every_bad_field_at_once():
    problems = validate_transfer_inputs(
        "Bad Bucket", "dest;rm", "NS\n", aws_region="us east", oci_region="x`y`",
        oci_profile="p$1", compartment_ocid="ocid1.user.oc1..abc",
    )
    assert len(problems) == len(_FIELDS)
    for field_name in _FIELDS:
        assert any(p.startswith(field_name) for p in problems), field_name


def test_newline_cannot_terminate_the_rclone_heredoc():
    for field_name in ("namespace", "aws_region", "oci_region", "oci_profile",
                       "compartment_ocid"):
        assert _refusal(**{field_name: "ok\nRCLONE\n[evil]\ntype = sftp"}) is not None
    s = _build().translated_sql
    body = s.split("<<'RCLONE'\n", 1)[1]
    assert body.count("\nRCLONE\n") == 1   # exactly one terminator, the generated one


def test_accepts_documented_placeholders_and_empty_namespace():
    r = _build(namespace="", compartment_ocid=COMPARTMENT_OCID_PLACEHOLDER)
    assert f"namespace = {OCI_NAMESPACE_PLACEHOLDER}" in r.translated_sql
    assert f"compartment = {COMPARTMENT_OCID_PLACEHOLDER}" in r.translated_sql
    assert _refusal(namespace=OCI_NAMESPACE_PLACEHOLDER) is None


def test_accepts_real_identifiers():
    assert _refusal(
        bucket="acme.raw-data.2024", oci_bucket="Acme_Raw.Data-2024", namespace="stress-ns",
        aws_region="eu-west-1", oci_region="eu-frankfurt-1", oci_profile="team_prod.1",
        compartment_ocid="ocid1.compartment.oc1..aaaaaaaaexample2kq3vzb6e5v4mz3dz",
    ) is None
    assert _refusal(
        compartment_ocid="ocid1.tenancy.oc1..aaaaaaaaexample2kq3vzb6e5v4mz3dz",
    ) is None
    assert _refusal(
        compartment_ocid="ocid1.compartment.oc2.us-langley-1.aaaaaaaaexample",
    ) is None


def test_rejects_invalid_s3_bucket_names():
    for bad in ("ab", "Upper-Case", "a..b", "192.168.0.1", "-leading", "trailing-",
                "xn--punycode", "x" * 64, "alias-s3alias", "under_score"):
        message = _refusal(bucket=bad)
        assert message is not None and "bucket" in message, bad


def test_rejects_malformed_identifiers():
    assert _refusal(oci_bucket="dest/with/slash") is not None
    assert _refusal(namespace="Upper") is not None
    assert _refusal(namespace="-leading") is not None
    assert _refusal(aws_region="US-EAST-1") is not None
    assert _refusal(oci_region="us_ashburn_1") is not None
    assert _refusal(oci_profile="DEFAULT PROFILE") is not None
    assert _refusal(compartment_ocid="ocid1.user.oc1..aaaa") is not None
    assert _refusal(compartment_ocid="ocid1.compartment.oc1..AAAA") is not None
    assert _refusal(compartment_ocid="<other-placeholder>") is not None


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")


if __name__ == "__main__":
    _run_all()
