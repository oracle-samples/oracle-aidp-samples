#!/usr/bin/env python3
"""Search an AIDP workspace for AWS credential *locations* -- never values.

Diagnostic for the FUSE / S3 bring-up: each cell runs on the cluster and
reports WHERE a credential is configured -- environment-variable names,
Spark / Hadoop config keys, credential-looking files, init-script line
heads, encryption-related JAR entries -- so the operator can move it into
the AIDP secret store.

Values are never printed (SEC-NEW-DATABRICKS-02). A credential-like value
is shown as ``<set, N chars>`` and nothing else; only documented non-secret
settings (region, endpoint, provider class, ARNs, config-file paths) print
in clear. Credential-like means: the name contains KEY, SECRET, TOKEN,
PASSWORD, PASSWD, PASSPHRASE, CREDENTIAL, ENCRYPT or DECRYPT, or contains
AWS anywhere (``AWS_*``, ``AWSPASS``, ``S3_AWS_SIGNATURE``) and is not in the
non-secret allowlist -- an AWS-related name the allowlist does not know is
masked, not shown. Init-script lines are printed as ``<name>=<N chars
redacted>`` or ``<command> <N chars, rest redacted>``; a name is kept only
when it is name-shaped, so a value-first ``<secret>=key`` line, an ``echo
<secret>`` command and an ``aws_secret_access_key: <secret>`` fragment print
no value either. The masking helpers are compiled into the
cluster cells themselves, so the value never leaves the kernel; as defence
in depth the returned text is additionally scrubbed of anything shaped
like an AWS access-key id before it is printed here.

Run with ``python check_aws_creds.py`` after setting the cluster id below;
importing the module does not connect anywhere.
"""
import asyncio
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

WORKSPACE_ROOT = "/Workspace"

# ---------- masking policy (also shipped into the cluster cells) ----------

#: Names that match a credential marker (or contain AWS) but are
#: documented non-secret AWS SDK settings: regions, paths, ARNs, switches.
NON_SECRET_NAMES = frozenset({
    "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_DEFAULT_OUTPUT",
    "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CA_BUNDLE",
    "AWS_STS_REGIONAL_ENDPOINTS", "AWS_EC2_METADATA_DISABLED",
    "AWS_METADATA_SERVICE_TIMEOUT", "AWS_METADATA_SERVICE_NUM_ATTEMPTS",
    "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_SDK_LOAD_CONFIG",
    "AWS_MAX_ATTEMPTS", "AWS_RETRY_MODE", "AWS_USE_FIPS_ENDPOINT",
    "AWS_USE_DUALSTACK_ENDPOINT", "AWS_PAGER", "AWS_CLI_AUTO_PROMPT",
})

#: Substrings (upper-cased comparison) that make a name credential-like.
SECRET_MARKERS = ("KEY", "SECRET", "TOKEN", "PASSWORD", "PASSWD", "PASSPHRASE",
                  "CREDENTIAL", "ENCRYPT", "DECRYPT")

#: Config-key suffixes that are never a secret even when the key says
#: ``credentials`` (``fs.s3a.aws.credentials.provider`` is a class name).
NON_SECRET_SUFFIXES = (".ENDPOINT", ".PROVIDER", ".ARN", ".REGION", ".ENABLED",
                       ".IMPL", ".PATH", ".ALGORITHM")


def is_secret_name(name):
    """True when a variable / config key named *name* must be masked."""
    up = name.upper()
    if up in NON_SECRET_NAMES or up.endswith(NON_SECRET_SUFFIXES):
        return False
    if "AWS" in up:
        return True  # any AWS-related name the allowlist does not know is masked
    return any(m in up for m in SECRET_MARKERS)


def mask(value):
    """The only thing ever printed for a secret: presence and length."""
    return "<set, %d chars>" % len(value) if value else "<empty>"


def show(name, value):
    """*value* if *name* is a documented non-secret setting, else its mask."""
    return mask(value) if is_secret_name(name) else value


#: Shell words that may stand before the name in an assignment (``export X=``).
ASSIGN_PREFIXES = frozenset({"export", "set", "setenv", "local", "declare",
                             "readonly", "typeset", "env"})

#: Shape of a word that is a *name* -- identifier, dotted config key, flag,
#: comment marker or ``name:`` label -- rather than a value.
_NAME_SHAPED = re.compile(r"^-{0,2}[A-Za-z_#][A-Za-z0-9_.:-]*$")


def is_name_shaped(word):
    """True when *word* may be printed in clear: it is identifier / dotted
    key / flag shaped and not long *and* mixed-case the way an encoded value
    is (``PlantedBareKeyValue``). Real names are upper-case (``AWS_...``),
    lower-case (``fs.s3a.secret.key``), short, or flags (``-Dfs.s3a.secret.key``,
    ``--secret-key``), which are names by construction."""
    if not _NAME_SHAPED.match(word):
        return False
    if word.startswith("-"):
        return True
    return len(word) < 16 or word == word.lower() or word == word.upper()


def redact_line(line):
    """An init-script line with every value removed.

    ``name=value`` keeps the name (``export AWS_SECRET_ACCESS_KEY``) and
    reports the value as a length -- but only when the left-hand side is a
    name or ``<prefix> <name>``; a value-first ``<secret>=key`` line or an
    ``echo <secret>=x`` command has its left-hand side redacted too. Any
    other line keeps its first word (the command or a ``name:`` label) and
    reports the rest as a length, so ``echo <secret> > file`` and
    ``aws_secret_access_key: <secret>`` print no value.
    """
    s = line.strip()
    if "=" in s:
        lhs, rhs = s.split("=", 1)
        words = lhs.split()
        tail = "=<%d chars redacted>" % len(rhs.strip())
        if len(words) == 1 and is_name_shaped(words[0]):
            return words[0][:80] + tail
        if (len(words) == 2 and words[0].lower() in ASSIGN_PREFIXES
                and is_name_shaped(words[1])):
            return " ".join(words)[:80] + tail
        if len(words) > 1 and is_name_shaped(words[0]):
            return "%s <%d chars redacted>%s" % (words[0][:80], len(lhs.strip()), tail)
        return "<%d chars redacted>%s" % (len(lhs.strip()), tail)
    words = s.split()
    if not words:
        return s
    head = words[0][:80] if is_name_shaped(words[0]) else "<%d chars redacted>" % len(words[0])
    if len(words) == 1:
        return head
    return "%s <%d chars, rest redacted>" % (head, len(s))


def _mask_prelude():
    """Source of the masking policy, prepended to every cluster cell that
    prints a value, so the cell and this module apply the same rules."""
    import inspect
    return "\n".join([
        "import re",
        "NON_SECRET_NAMES = frozenset(%r)" % (sorted(NON_SECRET_NAMES),),
        "SECRET_MARKERS = %r" % (SECRET_MARKERS,),
        "NON_SECRET_SUFFIXES = %r" % (NON_SECRET_SUFFIXES,),
        "ASSIGN_PREFIXES = frozenset(%r)" % (sorted(ASSIGN_PREFIXES),),
        "_NAME_SHAPED = re.compile(%r)" % (_NAME_SHAPED.pattern,),
        inspect.getsource(is_secret_name),
        inspect.getsource(mask),
        inspect.getsource(show),
        inspect.getsource(is_name_shaped),
        inspect.getsource(redact_line),
    ])


# Defence in depth on the returned text: anything shaped like an AWS access
# key id (AKIA..., ASIA..., 20 upper-case alphanumerics) is scrubbed even if
# a cell printed it inside a file name or a JAR entry.
_AWS_KEY_ID = re.compile(r"(?<![A-Z0-9])(?:AKIA|ASIA|AROA|AIDA|AGPA|ANPA|ANVA|ASCA)[A-Z0-9]{16}(?![A-Z0-9])")


def scrub(text):
    return _AWS_KEY_ID.sub("<aws-key-id redacted>", text)


def unwrap(outputs):
    text = ""
    for o in outputs:
        if o.get("type") == "stream":
            raw = o.get("text", "")
            try:
                items = json.loads(raw)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and "value" in item:
                            text += item["value"]
                    continue
            except Exception:
                pass
            text += raw
        elif o.get("type") == "error":
            text += "ERROR: " + o.get("evalue", "") + "\n"
    return text


def build_cells(workspace_root=WORKSPACE_ROOT):
    """The cluster-side cells, in run order. Cells that print a config value
    carry the masking prelude; the others only print names and paths."""
    prelude = _mask_prelude() + "\n"
    root = repr(workspace_root)
    return [
        # 1: Env vars -- names in clear, values masked unless allowlisted
        prelude +
        'import os\nprint("=== AWS env vars ===")\nfor k, v in sorted(os.environ.items()):\n    kup = k.upper()\n    if "AWS" in kup or any(m in kup for m in SECRET_MARKERS):\n        print(f"  {k}={show(k, v)}")',

        # 2: Spark configs -- access/secret/session keys masked, endpoint/provider in clear
        prelude +
        'print("=== AWS Spark configs ===")\nkeys = ["spark.hadoop.fs.s3a.access.key", "spark.hadoop.fs.s3a.secret.key", "spark.hadoop.fs.s3a.session.token", "spark.hadoop.fs.s3a.endpoint", "spark.hadoop.fs.s3a.aws.credentials.provider"]\nfor key in keys:\n    try:\n        val = spark.conf.get(key)\n    except Exception:\n        continue\n    print(f"  {key}={show(key, val)}")',

        # 3: Find credential files (paths and sizes only)
        'import os\nprint("=== Credential-related files ===")\nfor root, dirs, files in os.walk(' + root + '):\n    depth = root[len(' + root + '):].replace(os.sep, "/").count("/")\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        fl = f.lower()\n        if any(k in fl for k in ["credential", "aws", ".env", "decrypt", "encrypt", "secret"]):\n            full = os.path.join(root, f)\n            print(f"  {full} ({os.path.getsize(full)} bytes)")',

        # 4: Config/properties files (paths only)
        'import os\nprint("=== Config files ===")\nfor root, dirs, files in os.walk(' + root + '):\n    depth = root[len(' + root + '):].replace(os.sep, "/").count("/")\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        if f.endswith((".properties", ".conf", ".cfg", ".ini")):\n            full = os.path.join(root, f)\n            print(f"  {full}")',

        # 5: Decrypt JAR internals (entry names only)
        'import subprocess\nprint("=== Encryption-related JARs ===")\nresult = subprocess.run(["find", "/aidp/libraries", "-name", "*decrypt*", "-o", "-name", "*encrypt*", "-o", "-name", "*secret*"], capture_output=True, text=True, timeout=10)\nfor line in result.stdout.strip().split("\\n"):\n    if line.strip() and line.endswith(".jar"):\n        print(f"JAR: {line}")\n        r2 = subprocess.run(["jar", "tf", line], capture_output=True, text=True, timeout=10)\n        for entry in r2.stdout.strip().split("\\n"):\n            el = entry.lower()\n            if any(k in el for k in ["config", "properties", "secret", "aws", "application", "reference", "encrypt", "decrypt"]):\n                print(f"  {entry}")',

        # 6: Init scripts with AWS refs -- line heads only, values redacted
        prelude +
        'import os\nprint("=== Init scripts with AWS refs ===")\nfor root, dirs, files in os.walk(' + root + '):\n    depth = root[len(' + root + '):].replace(os.sep, "/").count("/")\n    if depth > 4:\n        dirs.clear()\n        continue\n    for f in files:\n        if f.endswith(".sh"):\n            full = os.path.join(root, f)\n            try:\n                with open(full) as fh:\n                    content = fh.read()\n                if "AWS" in content or "aws_" in content or "secret" in content.lower():\n                    print(f"  {full}")\n                    for line in content.split("\\n"):\n                        ll = line.lower()\n                        if "aws" in ll or "secret" in ll or "key" in ll:\n                            print(f"    {redact_line(line)}")\n            except Exception:\n                pass',

        # 7: Hadoop configs for AWS -- same policy as cell 2
        prelude +
        'print("=== Hadoop AWS configs ===")\nhc = spark.sparkContext._jsc.hadoopConfiguration()\nfor key in ["fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token", "fs.s3a.endpoint", "fs.s3a.aws.credentials.provider", "fs.s3a.assumed.role.arn"]:\n    val = hc.get(key)\n    if val:\n        print(f"  {key}={show(key, val)}")',

        # 8: Encryption-related classes (entry names only)
        'import subprocess\nprint("=== Inspect encryption-related classes ===")\nimport glob\njars = glob.glob("/aidp/libraries/java/jars/*decrypt*") + glob.glob("/aidp/libraries/java/jars/*encrypt*") + glob.glob("/aidp/libraries/java/jars/*secret*")\nfor jar in jars[:3]:\n    print(f"JAR: {jar}")\n    r = subprocess.run(["jar", "tf", jar], capture_output=True, text=True, timeout=10)\n    classes = [e for e in r.stdout.strip().split("\\n") if "decrypt" in e.lower() or "encrypt" in e.lower() or "secret" in e.lower() or "config" in e.lower()]\n    for c in classes[:20]:\n        print(f"  {c}")',
    ]


async def main():
    # Cluster-side dependency; imported here so the masking policy above can
    # be exercised (and tested) without the AIDP executor stack.
    from aidp_executor import AIDPSession

    session = AIDPSession(cluster_id="<your-cluster-id>")
    await session.connect()

    for i, code in enumerate(build_cells()):
        print(f"\n--- Cell {i+1} ---")
        result = await session.execute(code, timeout=30)
        print(scrub(unwrap(result.get("outputs", []))))

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
