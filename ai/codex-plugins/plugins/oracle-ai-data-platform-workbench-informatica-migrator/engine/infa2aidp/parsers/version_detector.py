"""Detect Informatica PowerCenter version from XML export metadata."""

import logging
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from infa2aidp.models import InfaVersion
from infa2aidp.parsers.security import (
    validate_input_size,
    validate_no_xxe,
    validate_xml_nesting_depth,
)

logger = logging.getLogger(__name__)

# SerializationSpecVersion -> release, per Informatica KB 516223.
#
# This is a DIFFERENT export format from the POWERMART/powrmart.dtd
# repository export this tool parses: it is the Informatica Developer /
# Model repository XML used by Data Quality and Data Engineering. The
# mapping below is authoritative -- unlike the REPOSITORY_VERSION table,
# which no Informatica documentation publishes and which is still
# disputed for 186/187.
#
# Detecting it does NOT mean the file can be migrated. The parser reads
# POWERMART; a Developer export has a different structure entirely. The
# point of recognising it is to say so, instead of reporting "no version
# found" and leaving the user to guess whether the file is corrupt.
_SERIALIZATION_SPEC_RELEASE = {
    "6.0": "9.6.x",
    "8.0": "10.1",
    "9.0": "10.1.1",
    "10.0": "10.2",
    "11.0": "10.2.1",
    "12.0": "10.2.2",
    "13.0": "10.4.x",
    "14.0": "10.5.x",
}

_SERIALIZATION_SPEC_RE = re.compile(
    r'SerializationSpecVersion\s*=\s*"?([0-9]+\.[0-9]+)"?'
)


# REPOSITORY_VERSION prefix -> InfaVersion mapping
# Repository version prefix -> release family.
#
# Scope is 10.x onwards. 10.1 and 10.2 are where a large share of surviving
# PowerCenter estates sit -- the customers who have not upgraded are exactly
# the ones who most need a migration path -- so refusing them would exclude
# much of the tool's own addressable population.
#
# 186 and 187 are DISPUTED and the dispute now decides an outcome rather than
# just a message. This table originally read them as 9.x. Two reviewers with
# Informatica experience place them at 10.1 and 10.2, with 9.6.1 at 184. They
# are mapped as 10.x here on that evidence, which is better than the only
# thing that supported the 9.x reading: a fixture we wrote ourselves.
#
# The risk this accepts: if the reviewers are wrong, a genuine 9.x export is
# converted instead of refused. Two things bound it. Under the reviewers'
# numbering, real 9.x exports carry 184 or below and are still refused by the
# pre-186 branch below. And the detail string carries the dispute, so an
# operator reading the report sees it rather than inheriting a silent
# assumption.
#
# One real export settles this permanently -- 186/187 from a known release,
# or a 10.5 export confirming 189. Until then every entry here is inference.
_VERSION_MAP = {
    "186": InfaVersion.V10,
    "187": InfaVersion.V10,
    "188": InfaVersion.V10,
    "189": InfaVersion.V10_5,
}


_VERSION_DETAIL = {
    "186": "10.1 (repository version 186.x; release disputed -- also "
           "reported as 9.x. If this export is 9.x, output is NOT supported)",
    "187": "10.2 (repository version 187.x; release disputed -- also "
           "reported as 9.6.x. If this export is 9.6.x, output is NOT "
           "supported)",
    "188": "10.4 or earlier 10.x (repository version 188.x)",
    "189": "10.5.x (repository version 189.x)",
}


def detect_version(xml_path: str) -> tuple[InfaVersion, str]:
    """Detect Informatica version from an XML export file.

    Inspects POWERMART or REPOSITORY elements for REPOSITORY_VERSION,
    POWERMART_ENDIAN_TYPE, and CODEPAGE attributes.

    Returns:
        Tuple of (InfaVersion enum, detail string describing the version).
    """
    path = Path(xml_path)
    if not path.is_file():
        logger.warning("File not found: %s", xml_path)
        return InfaVersion.UNKNOWN, "File not found"

    try:
        with open(xml_path, "r", encoding="utf-8", errors="replace") as fh:
            content = fh.read()
    except OSError as exc:
        logger.error("Could not read %s: %s", xml_path, exc)
        return InfaVersion.UNKNOWN, f"Could not read file: {exc}"

    # Security guards -- run before any XML parsing here
    # too, not just in xml_parser.py's own parse path. A security violation
    # (oversized input, XXE, pathological nesting depth) is deliberately
    # NOT caught below with the other, benign "this just isn't valid XML"
    # cases: it must propagate as a hard failure rather than being
    # downgraded to an UNKNOWN-version detail string that the caller might
    # treat as an ordinary, recoverable parse gap.
    validate_input_size(content)
    validate_no_xxe(content)
    validate_xml_nesting_depth(content)

    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        logger.error("Malformed XML in %s: %s", xml_path, exc)
        return InfaVersion.UNKNOWN, f"Malformed XML: {exc}"

    # Try root element first (POWERMART), then look for REPOSITORY child
    repo_version = root.attrib.get("REPOSITORY_VERSION", "")
    if not repo_version:
        repo_elem = root.find(".//REPOSITORY")
        if repo_elem is not None:
            repo_version = repo_elem.attrib.get("REPOSITORY_VERSION", "")

    if not repo_version:
        # No REPOSITORY_VERSION. Before giving up, check whether this is a
        # Developer / Model repository export rather than a PowerCenter
        # one -- those carry SerializationSpecVersion instead, and KB
        # 516223 maps it to a release unambiguously.
        #
        # Searched as text rather than as an attribute because the KB says
        # only "search the file for SerializationSpecVersion" and does not
        # state which element carries it. A text scan finds it wherever it
        # sits; the alternative is guessing an element name.
        spec = _SERIALIZATION_SPEC_RE.search(content)
        if spec:
            release = _SERIALIZATION_SPEC_RELEASE.get(spec.group(1))
            named = f"release {release}" if release else f"SerializationSpecVersion {spec.group(1)}"
            # Deliberately UNKNOWN, not the matching InfaVersion. Knowing
            # the release does not make the file migratable: this parser
            # reads POWERMART, and a Developer export is a different
            # structure. Returning 10.5 here would pass the version gate
            # and then fail obscurely in the parser.
            return InfaVersion.UNKNOWN, (
                f"Informatica Developer / Model repository export ({named}), "
                f"not a PowerCenter repository export. This tool parses "
                f"PowerCenter XML (POWERMART) and IDMC/IICS JSON; a Developer "
                f"export has a different structure and is not supported. "
                f"Export the mapping from PowerCenter, or migrate it as an "
                f"IDMC asset."
            )

    # Gather supplementary metadata
    endian = root.attrib.get("POWERMART_ENDIAN_TYPE", "")
    codepage = root.attrib.get("CODEPAGE", "")

    extras = []
    if endian:
        extras.append(f"endian={endian}")
    if codepage:
        extras.append(f"codepage={codepage}")
    extras_str = f" ({', '.join(extras)})" if extras else ""

    if not repo_version:
        logger.info("No REPOSITORY_VERSION found in %s", xml_path)
        return InfaVersion.UNKNOWN, f"No REPOSITORY_VERSION attribute found{extras_str}"

    # Match on the major prefix (first 3 digits)
    prefix = repo_version.split(".")[0] if "." in repo_version else repo_version[:3]

    version = _VERSION_MAP.get(prefix, InfaVersion.UNKNOWN)
    detail = _VERSION_DETAIL.get(prefix, f"Unknown repository version {repo_version}")
    detail += extras_str

    if version == InfaVersion.UNKNOWN:
        # Older versions (pre-9.x)
        try:
            major = int(prefix)
            if major < 186:
                version = InfaVersion.V8
                detail = f"8.x or earlier (repository version {repo_version}){extras_str}"
        except ValueError:
            pass

    logger.info("Detected version %s from %s (repo_version=%s)", version, xml_path, repo_version)
    return version, detail


# Versions this tool will convert. Older releases are still *detected*
# (see _VERSION_MAP above) so the tool can refuse with a clear message
# rather than silently mis-migrating a 9.x/8.x export. See the scope
# note in the README.
# This is the tool's real scope: PowerCenter 10.x and IDMC/IICS.
#
# Deliberately wider than the releases Informatica still supports. 10.4 left
# standard support in March 2024 and 10.5.x in March 2026, so every remaining
# PowerCenter estate is on an unsupported release by definition -- refusing
# them would refuse the entire population this tool exists to migrate. V10_5
# is kept as a distinct member so the report can name the release precisely
# even though both are accepted.
SUPPORTED: frozenset = frozenset({InfaVersion.V10, InfaVersion.V10_5})

# Test-only escape hatch. Honoured ONLY when the value is exactly "1" --
# not "true"/"yes"/anything else -- so it can never be flipped on by
# accident via a loosely-set env var. See require_supported() below.
_ALLOW_UNSUPPORTED_ENV = "INFA_ALLOW_UNSUPPORTED_VERSION"


def is_supported(version: InfaVersion) -> bool:
    """True if this tool will convert mappings from `version`."""
    return version in SUPPORTED


def require_supported(version: InfaVersion, detail: str) -> None:
    """Raise with an actionable message if `version` is out of scope.

    Honours INFA_ALLOW_UNSUPPORTED_VERSION as a TEST-ONLY escape hatch,
    bypassing the gate when (and only when) it is set to exactly "1".

    Rationale: no public PowerCenter 10.5 installer or trial exists --
    Informatica distributes installers only to licensed customers via a
    support ticket -- so a real PowerCenter export of any release is hard
    to come by, and one of a *supported* release harder still. If such an
    export turns up and is out of scope, testing the parser's structural
    handling against it is worth more than testing only against
    hand-written fixtures. So the gate is overridable for testing without
    being weakened for users: the env var is never read outside this
    function, defaults to off, and only the literal string "1" bypasses it.

    An earlier version of this note justified the hatch by pointing at an
    on-prem 9.0.1 reference environment. That environment was planned and
    never built, and the claim survived here long enough to be repeated as
    fact elsewhere.
    """
    if is_supported(version):
        return
    if os.environ.get(_ALLOW_UNSUPPORTED_ENV) == "1":
        logger.warning(
            "Version gate BYPASSED for %s via %s=1. Migration output is "
            "NOT supported for this release -- testing only.",
            detail, _ALLOW_UNSUPPORTED_ENV,
        )
        return
    raise ValueError(
        f"Unsupported Informatica version: {detail}. This tool targets "
        f"PowerCenter 10.x and IDMC/IICS. Upgrade the repository or "
        f"export from a supported release. (Set {_ALLOW_UNSUPPORTED_ENV}=1 "
        f"to bypass for testing only -- output is not supported.)"
    )
