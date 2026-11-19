"""Shared pip argument classification; no notebook scanning or installation.

The exporter needs complete requirements (pins, extras, markers and references).
The migrator needs names for discovery, version constraints for installation,
and explicit unresolved findings for unsupported dependency forms.
Both projections come from this parser so an option value cannot become a package
in just one of the two paths. Source/magic detection remains the caller's job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import shlex

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename


PIP_VALUE_FLAGS = frozenset({
    "-r", "--requirement", "-e", "--editable", "-c", "--constraint",
    "-f", "--find-links", "-i", "--index-url", "--extra-index-url",
    "-t", "--target", "--prefix", "--root", "--no-binary", "--only-binary",
    "-b", "--build", "-d", "--dest", "--src", "--log", "--cache-dir",
    "--proxy", "--timeout", "--retries", "--python-version", "--platform",
    "--abi", "--implementation", "--upgrade-strategy", "--progress-bar",
    "--exists-action", "--trusted-host", "--client-cert", "--cert",
    "--global-option", "--install-option", "--config-settings", "-C",
    "--report", "--python",
    "--root-user-action", "--use-deprecated", "--use-feature",
    "--keyring-provider", "--resume-retries",
    "--hash", "--group",
})
_SHORT_VALUE_FLAGS = frozenset({"-r", "-e", "-c", "-f", "-i", "-t", "-b", "-d", "-C"})
_INDIRECT_FLAGS = frozenset({"-r", "--requirement", "-e", "--editable", "-c", "--constraint"})
_NONNAME_PREFIXES = ("git+", "hg+", "svn+", "bzr+", "http://", "https://", "file://")
_NONNAME_SUFFIXES = (".txt", ".whl", ".tar.gz", ".zip")
_DIRECT_REFERENCE_RE = re.compile(
    r'^[A-Za-z0-9][A-Za-z0-9._-]*\s*(?:\[[^\]]*\]\s*)?@\s*')
_REDIRECT_RE = re.compile(r'\d*(?:>>|>\||>&|>|&>>|&>|<<<|<<|<&|<>|<)')


class _ShellBoundary(str):
    """An unquoted shell operator, never an option value or requirement."""


@dataclass(frozen=True)
class PipDependency:
    spec: str
    name: str | None = None
    name_only_reason: str = ""


@dataclass
class ParsedPipArguments:
    dependencies: list[PipDependency] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    notices: list[str] = field(default_factory=list)
    # Hash-protected requests (--hash / --require-hashes) cannot be projected
    # to plain names/specs without weakening the requested integrity checks.
    # Keep parsed metadata, but expose no installable deps to either consumer.
    installation_blocked: bool = False
    # True when the command contains a provider form whose dependencies cannot
    # be safely projected to the audit/install requirements set.
    inference_barrier: bool = False

    @property
    def requirements(self) -> list[str]:
        if self.installation_blocked:
            return []
        return [dependency.spec for dependency in self.dependencies]

    @property
    def names(self) -> list[str]:
        if self.installation_blocked:
            return []
        return [dependency.name for dependency in self.dependencies if dependency.name]

    @property
    def managed_names(self) -> list[str]:
        """Provider identities that will be installed in the target context."""
        if self.installation_blocked:
            return []
        return [dependency.name for dependency in self.dependencies if dependency.name]

    @property
    def name_only_findings(self) -> list[str]:
        return self.unresolved + [
            f"{dependency.spec} — {dependency.name_only_reason}"
            for dependency in self.dependencies if dependency.name_only_reason
        ]


def _normalise_tokens(tokens: list[str]) -> list[str]:
    out = []
    for token in tokens:
        if token.startswith("--") and "=" in token:
            key, _, value = token.partition("=")
            out.extend((key, value))
        elif (len(token) > 2 and token.startswith("-")
              and token[:2] in _SHORT_VALUE_FLAGS):
            out.extend((token[:2], token[2:]))
        else:
            out.append(token)
    return out


def _artifact_distribution_name(spec: str) -> str | None:
    """Derive an artifact distribution only when its filename proves one."""
    basename = (
        spec.split("?", 1)[0].split("#", 1)[0].replace("\\", "/")
        .rsplit("/", 1)[-1]
    )
    if basename.lower().endswith(".whl"):
        try:
            name, _, _, _ = parse_wheel_filename(basename)
        except InvalidWheelFilename:
            return None
        return canonicalize_name(str(name))
    for suffix in (".tar.gz", ".zip", ".egg"):
        if basename.lower().endswith(suffix):
            stem = basename[:-len(suffix)]
            match = re.match(r"^(.+?)-(?=\d)", stem)
            return canonicalize_name(match.group(1)) if match else None
    return None


def _classify(spec: str, result: ParsedPipArguments, *, legacy_dynamic_versions: bool = False) -> None:
    if not spec:
        return
    # Migration-only compatibility with pre-PR105 package-name installation.
    # Do not evaluate notebook expressions or weaken export/hash validation.
    if legacy_dynamic_versions:
        dynamic = re.fullmatch(
            r"([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)==\{[A-Za-z_][A-Za-z0-9_]*\}", spec)
        if dynamic and dynamic[1].lower() != "pip":
            name = dynamic[1]
            result.dependencies.append(PipDependency(name, name))
            result.notices.append(
                f"{spec}: using package-only requirement '{name}' (pre-PR105 behavior); "
                "dynamic version is not enforced")
            return
    if "{" in spec or "}" in spec:
        result.unresolved.append(f"{spec} — contains an unresolved interpolation")
        result.inference_barrier = True
        return
    # Preserve URL/archive requirements for the exporter, without EVER exposing
    # a guessed name to the migrator. This preserves the exporter's existing
    # ability to carry pip-specific requirements through consolidation.
    direct = _DIRECT_REFERENCE_RE.match(spec)
    if direct is None and (spec.lower().startswith(_NONNAME_PREFIXES)
                           or spec.lower().endswith(_NONNAME_SUFFIXES)):
        name = _artifact_distribution_name(spec)
        result.dependencies.append(PipDependency(
            spec, name, name_only_reason="not installable by name"))
        if name is None:
            result.inference_barrier = True
        return
    if spec.startswith(("/", "./", "../")):
        name = _artifact_distribution_name(spec)
        result.dependencies.append(PipDependency(
            spec, name, name_only_reason="not a valid package name (local path)"))
        if name is None:
            result.inference_barrier = True
        return
    try:
        requirement = Requirement(spec)
    except InvalidRequirement:
        name = _artifact_distribution_name(spec)
        if name is not None:
            result.dependencies.append(PipDependency(
                spec, name, name_only_reason="not installable by name"))
            return
        reason = ("direct reference is incomplete or invalid" if direct
                  else "not a valid package name or requirement")
        result.unresolved.append(f"{spec} — {reason}")
        result.inference_barrier = True
        return
    if requirement.url is not None:
        result.dependencies.append(PipDependency(
            spec, name_only_reason="direct reference not installable by name"))
    elif requirement.name.lower() != "pip":
        result.dependencies.append(PipDependency(spec, requirement.name))


def parse_pip_tokens(tokens: list[str], *, legacy_dynamic_versions: bool = False) -> ParsedPipArguments:
    """Classify arguments after 'install'; argv callers must not be re-split.

    Shell callers supply explicit boundary tokens; literal argv strings such
    as ";" are data, not shell syntax. No expressions are evaluated.
    """
    result = ParsedPipArguments()
    tokens = _normalise_tokens(tokens)
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if isinstance(token, _ShellBoundary):
            break
        if token.startswith("-"):
            if token == "--hash":
                result.installation_blocked = True
                result.unresolved.append(
                    "--hash — per-requirement hash option is unsupported in an install command; "
                    "use a hashed requirements file; unhashed installation not attempted")
            elif token == "--require-hashes":
                # A no-value flag: do not consume the next package. Retain
                # metadata, but never silently strip an integrity requirement.
                result.installation_blocked = True
                result.unresolved.append(
                    "--require-hashes — hash verification cannot be preserved by this command "
                    "conversion; unhashed installation not attempted")
            if token in PIP_VALUE_FLAGS and (
                    i + 1 == len(tokens) or isinstance(tokens[i + 1], _ShellBoundary)
                    or (token in ("--hash", "--group")
                        and (not tokens[i + 1] or tokens[i + 1].startswith('-')))):
                if token in _INDIRECT_FLAGS or token == "--group":
                    result.inference_barrier = True
                result.unresolved.append(f"{token} — missing option value")
                i += 1  # Leave the boundary to terminate the command.
            elif token == "--group":
                result.inference_barrier = True
                result.unresolved.append(
                    f"--group {tokens[i + 1]} — dependencies are declared in pyproject.toml; "
                    "dependency group not resolved")
                i += 2
            elif token in ("-e", "--editable"):
                result.inference_barrier = True
                value = tokens[i + 1] if i + 1 < len(tokens) else "?"
                # Preserve the customer's editable source for the exporter's
                # requirements transaction. The name-only migrator projection
                # still receives no guessed package name and reports why.
                result.dependencies.append(PipDependency(
                    f"{token} {value}",
                    name_only_reason="editable source not installable by name",
                ))
                i += 2
            elif token in _INDIRECT_FLAGS:
                result.inference_barrier = True
                value = tokens[i + 1] if i + 1 < len(tokens) else "?"
                result.unresolved.append(f"{token} {value} — dependencies are declared elsewhere")
                i += 2
            elif token in PIP_VALUE_FLAGS:
                i += 2
            else:
                i += 1
            continue
        # Group name/extras @ location before accepting any bare name. This
        # also handles quoted, attached and spaced-extras reference forms.
        reference, end = token, i + 1
        direct = _DIRECT_REFERENCE_RE.match(reference)
        while direct is None and end < len(tokens) and (
                tokens[end].startswith("@")
                or (tokens[end].startswith("[") and "[" not in reference)
                or ("[" in reference and "]" not in reference)):
            reference += " " + tokens[end]
            end += 1
            direct = _DIRECT_REFERENCE_RE.match(reference)
        if direct is not None:
            if not reference[direct.end():].strip() and end < len(tokens):
                location = tokens[end]
                if location and not location.startswith(("-", "#", ";", "&", "|")):
                    reference += " " + location
                    end += 1
            _classify(reference, result, legacy_dynamic_versions=legacy_dynamic_versions)
            i = end
        else:
            _classify(token, result, legacy_dynamic_versions=legacy_dynamic_versions)
            i += 1
    # A hash-protected direct artifact whose distribution identity cannot be
    # parsed must remain an explicit unresolved barrier.  Otherwise a later
    # import audit could infer an unhashed package with the same import name,
    # defeating the integrity requirement even though this install was
    # correctly suppressed.
    if result.installation_blocked and any(
        dependency.name is None for dependency in result.dependencies
    ):
        result.inference_barrier = True
        marker = (
            "hash-protected dependency has no parseable distribution identity; "
            "inference suppressed"
        )
        if marker not in result.unresolved:
            result.unresolved.append(marker)
    return result


def _shell_word_end(text: str, start: int, *, redirect_target: bool = False) -> int:
    """Locate a word without losing its quote/escape boundaries.

    Leave version operators inside ordinary words (``pydantic<2``). Only a
    redirection operand stops at another unquoted redirection operator.
    shlex, not this boundary scanner, decodes the resulting word.
    """
    i, quote = start, None
    while i < len(text):
        char = text[i]
        if char == "\\" and quote != "'":
            i += 2
            continue
        if quote:
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char.isspace() or char in ";&|" or (redirect_target and char in "<>"):
            break
        i += 1
    return min(i, len(text))


def _shell_arguments(text: str) -> tuple[list[str], list[str], list[str]]:
    """Separate output redirects before quote removal; never execute a shell."""
    tokens, notices, findings = [], [], []
    i = 0
    while i < len(text):
        if text[i].isspace():
            i += 1
            continue
        if text[i] == "#":
            break
        redirect = _REDIRECT_RE.match(text, i)
        if redirect:
            start, operator = i, redirect.group()
            i = redirect.end()
            while i < len(text) and text[i].isspace():
                i += 1
            end = _shell_word_end(text, i, redirect_target=True)
            if end == i or text[i:i + 1] == "#":
                findings.append(f"{operator} — missing redirection target")
                continue
            raw_target = text[i:end]
            i = end
            try:
                target = shlex.split(raw_target)  # Never open the path.
            except ValueError:
                findings.append(f"{text[start:end]} — invalid redirection target")
                continue
            if not target or not target[0]:
                findings.append(f"{operator} — missing redirection target")
                continue
            if "<" in operator:
                findings.append(f"{text[start:end]} — input redirection is unsupported")
            else:
                notices.append(
                    f"shell redirection {text[start:end]!r} not reproduced "
                    "by cluster-library installation")
            continue
        if text[i] in ";&|":
            # Retain the boundary for missing-option-value detection, but do
            # not parse a later shell command (or its quotes) as dependencies.
            tokens.append(_ShellBoundary(text[i]))
            break
        end = _shell_word_end(text, i)
        raw = text[i:end]
        try:
            tokens.extend(shlex.split(raw))
        except ValueError:
            findings.append("unbalanced shell quotes — arguments need manual review")
            tokens.extend(raw.split())  # Existing best-effort fallback, now loud.
        i = end
    return tokens, notices, findings


def parse_pip_arguments(args_text: str, *, legacy_dynamic_versions: bool = False) -> ParsedPipArguments:
    """Parse shell-style pip arguments, retaining complete requirement specs."""
    text = (args_text or "").strip()
    findings = []
    if text.endswith("\\"):
        findings.append("line continuation (`\\`) — command spans multiple lines")
        text = text.rstrip("\\").strip()
    tokens, notices, shell_findings = _shell_arguments(text)
    result = parse_pip_tokens(tokens, legacy_dynamic_versions=legacy_dynamic_versions)
    result.unresolved[:0] = findings
    result.unresolved.extend(shell_findings)
    if findings or shell_findings:
        # An incomplete shell parse may hide an arbitrary provider.  Keep the
        # barrier structured so every caller suppresses inference consistently.
        result.inference_barrier = True
    result.notices.extend(notices)
    return result
