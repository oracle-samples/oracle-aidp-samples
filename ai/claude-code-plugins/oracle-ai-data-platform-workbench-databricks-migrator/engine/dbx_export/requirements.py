"""Build one deterministic requirements file for each exported workflow.

The exporter discovers requirements from several places: Databricks cluster
libraries, notebook ``%pip`` commands, and the migration tool's own baseline.
Installing those sources as separate AIDP cluster libraries lets independent
pip transactions overwrite each other's dependency choices.  This module
combines their direct requirements into one resolver transaction instead.
"""

from __future__ import annotations

import re

from dataclasses import dataclass, field
import os
from pathlib import Path
import warnings
from typing import Any, Iterable, Mapping, Sequence

import yaml
from packaging.markers import Marker, default_environment
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version
from packaging.utils import canonicalize_name


CANONICAL_REQUIREMENTS_PREFIX = "/dbx_export_generated/resolved_requirements_"
CANONICAL_REQUIREMENTS_BASENAME_PREFIX = "resolved_requirements_"
COMPATIBILITY_POLICY_ENV = "AIDP_COMPATIBILITY_POLICY_FILE"


class RequirementConsolidationError(ValueError):
    """Raised when direct requirements cannot safely share one environment."""


@dataclass(frozen=True)
class RequirementSource:
    name: str
    content: str
    # Explicit provider identities which must suppress inference but must not
    # be copied into the install transaction (for example hash-blocked pip
    # commands and separately attached wheel/egg libraries).
    managed_names: frozenset[str] = frozenset()
    # A hash-protected source whose distribution identity could not be parsed
    # must still suppress import inference.  It cannot be represented in the
    # install file, but treating it as absent would silently add an unhashed
    # duplicate.
    unresolved_passthrough: bool = False
    # Structured marker: unresolved provider forms must suppress inferred
    # packages even when the source has no content or warning text.
    inference_barrier: bool = False


@dataclass(frozen=True)
class CompatibilityRule:
    """A reviewed conditional rewrite from the compatibility policy."""

    rule_id: str
    when: Mapping[str, SpecifierSet]
    requirements: Mapping[str, Requirement]
    reason: str = ""


@dataclass(frozen=True)
class CompatibilityPolicy:
    """Target-runtime pins and conditional workload compatibility rewrites."""

    source: str
    protected_packages: Mapping[str, Requirement]
    rules: Sequence[CompatibilityRule]
    # Packages the AIDP runtime ships with platform integration (e.g. mlflow):
    # every workload pin — including CONFLICTING pins across sources — collapses
    # to the bare name so pip leaves the runtime's integrated version in place.
    # Deliberately narrow and explicit: unlisted packages keep their pins.
    # Unlisted packages with CONFLICTING exact pins are consolidated
    # highest-wins (never block cluster creation; reported to the export report
    # as requirements-conflict) — a SUPERSEDING decision (2026-09-16, PR #109)
    # reversing the 2026-09-09 (#88) fail-loud gate. Only genuinely unsatisfiable
    # range combinations, or conflicting pins across overlapping markers, still
    # fail loudly. See consolidate_requirement_sources.
    runtime_provided: frozenset = frozenset()
    # Export-time import auditing is deliberately policy-driven.  Import
    # names are case-sensitive Python roots; distribution names are PEP 503
    # normalized by the loader.
    import_to_distribution: Mapping[str, Requirement] = field(default_factory=dict)
    import_distribution_aliases: Mapping[str, frozenset[str]] = field(
        default_factory=dict
    )
    provided_imports: frozenset[str] = frozenset()
    jvm_roots: frozenset[str] = frozenset()
    stdlib_imports: frozenset[str] = frozenset()


def baseline_requirements_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "default_requirements.txt"


def baseline_requirements_source() -> RequirementSource:
    path = baseline_requirements_path()
    return RequirementSource("migration baseline", path.read_text(encoding="utf-8"))


def target_runtime_requirements_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "target_runtime_requirements.txt"


def target_runtime_requirements_source(path: str | Path | None = None) -> RequirementSource:
    """Return the target-safe baseline, optionally overridden by the operator.

    The explicit argument wins, followed by
    ``AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE`` and then the repository default.
    This keeps control-plane dependencies out of workload clusters while still
    allowing legacy runtimes to provide a reviewed compatibility profile.
    """
    path = Path(
        path
        or os.environ.get("AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE")
        or target_runtime_requirements_path()
    ).expanduser()
    return RequirementSource("target runtime baseline", path.read_text(encoding="utf-8"))


def compatibility_policy_path() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "config"
        / "aidp_compatibility_policy.yaml"
    )


def _policy_package_name(value: Any, location: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RequirementConsolidationError(
            f"{location} must be a non-empty package name"
        )
    return canonicalize_name(value)


def _policy_requirement(
    package: str, value: Any, location: str, allow_url: bool = False
) -> Requirement:
    if not isinstance(value, str) or not value.strip():
        raise RequirementConsolidationError(
            f"{location} must be a non-empty requirement string"
        )
    canonical = _policy_package_name(package, location)
    text = value.strip()
    try:
        requirement = Requirement(text)
    except InvalidRequirement as exc:
        try:
            requirement = Requirement(f"{canonical}{text}")
        except InvalidRequirement:
            raise RequirementConsolidationError(
                f"{location} contains invalid requirement {value!r}"
            ) from exc
    if canonicalize_name(requirement.name) != canonical:
        raise RequirementConsolidationError(
            f"{location} must configure {canonical!r}, got {requirement.name!r}"
        )
    if requirement.marker:
        raise RequirementConsolidationError(
            f"{location} cannot contain environment markers"
        )
    if requirement.url and not allow_url:
        # Direct-URL requirements are permitted ONLY in rule `requirements`
        # (packages whose installable artifact exists outside the configured
        # index — flash-attn ships wheels solely on GitHub releases; the PyPI
        # mirror has sdists that cannot build in the isolated cluster-library
        # env: no torch at build time, incident 2026-10-05, job
        # STANDALONE_GENERIC_E2E 1000000000061_84953597). The consolidation
        # renderer already emits `name @ url` lines; protected_packages stay
        # specifier-only.
        raise RequirementConsolidationError(
            f"{location} cannot contain direct URLs"
        )
    return requirement


def _policy_import_name(value: Any, location: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or not value.strip().isidentifier()
    ):
        raise RequirementConsolidationError(
            f"{location} must be a non-empty Python import root"
        )
    return value.strip()


def _policy_import_names(value: Any, location: str) -> frozenset[str]:
    if not isinstance(value, list):
        raise RequirementConsolidationError(f"{location} must be a list")
    return frozenset(
        _policy_import_name(item, f"{location}[{index}]")
        for index, item in enumerate(value)
    )


def load_compatibility_policy(
    path: str | Path | None = None,
) -> CompatibilityPolicy:
    """Load the configurable AIDP package compatibility policy.

    The explicit path wins, followed by ``AIDP_COMPATIBILITY_POLICY_FILE`` and
    the repository default. Package names are normalized with PEP 503 rules,
    so ``typing_extensions`` and ``typing-extensions`` are one package.
    """
    selected = Path(
        path or os.environ.get(COMPATIBILITY_POLICY_ENV) or compatibility_policy_path()
    ).expanduser()
    try:
        raw = yaml.safe_load(selected.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise RequirementConsolidationError(
            f"unable to load compatibility policy {selected}: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise RequirementConsolidationError(
            f"compatibility policy {selected} must contain a YAML mapping"
        )
    if raw.get("schema_version") != 1:
        raise RequirementConsolidationError(
            f"compatibility policy {selected} requires schema_version: 1"
        )

    protected_raw = raw.get("protected_packages") or {}
    if not isinstance(protected_raw, dict):
        raise RequirementConsolidationError("protected_packages must be a mapping")
    protected = {
        _policy_package_name(name, "protected_packages"): _policy_requirement(
            name, value, f"protected_packages.{name}"
        )
        for name, value in protected_raw.items()
    }

    runtime_provided_raw = raw.get("runtime_provided_packages") or []
    if not isinstance(runtime_provided_raw, list):
        raise RequirementConsolidationError(
            "runtime_provided_packages must be a list of package names"
        )
    runtime_provided = frozenset(
        _policy_package_name(name, "runtime_provided_packages")
        for name in runtime_provided_raw
    )
    overlap = runtime_provided & set(protected)
    if overlap:
        raise RequirementConsolidationError(
            "a package cannot be both runtime-provided (unpinned) and "
            f"protected (pinned): {sorted(overlap)}"
        )

    import_map_raw = raw.get("import_to_distribution") or {}
    if not isinstance(import_map_raw, dict):
        raise RequirementConsolidationError(
            "import_to_distribution must be a mapping"
        )
    import_to_distribution: dict[str, Requirement] = {}
    import_distribution_aliases: dict[str, frozenset[str]] = {}
    for import_name, item in import_map_raw.items():
        root = _policy_import_name(
            import_name, "import_to_distribution"
        )
        location = f"import_to_distribution.{root}"
        aliases_raw: Any = []
        requirement_raw = item
        if isinstance(item, dict):
            unknown_keys = set(item) - {"requirement", "aliases"}
            if unknown_keys:
                raise RequirementConsolidationError(
                    f"{location} contains unsupported keys: "
                    f"{sorted(unknown_keys)}"
                )
            requirement_raw = item.get("requirement")
            aliases_raw = item.get("aliases") or []
        if not isinstance(requirement_raw, str) or not requirement_raw.strip():
            raise RequirementConsolidationError(
                f"{location}.requirement must be a non-empty requirement string"
            )
        try:
            requirement = Requirement(requirement_raw.strip())
        except InvalidRequirement as exc:
            raise RequirementConsolidationError(
                f"{location} contains invalid requirement {requirement_raw!r}"
            ) from exc
        if requirement.marker or requirement.url:
            raise RequirementConsolidationError(
                f"{location} cannot contain environment markers or direct URLs"
            )
        if not isinstance(aliases_raw, list):
            raise RequirementConsolidationError(
                f"{location}.aliases must be a list of distribution names"
            )
        import_to_distribution[root] = requirement
        import_distribution_aliases[root] = frozenset(
            _policy_package_name(alias, f"{location}.aliases")
            for alias in aliases_raw
        )

    provided_imports = _policy_import_names(
        raw.get("provided_imports") or [], "provided_imports"
    )
    jvm_roots = _policy_import_names(
        raw.get("jvm_roots") or [], "jvm_roots"
    )
    stdlib_imports = _policy_import_names(
        raw.get("stdlib_imports") or [], "stdlib_imports"
    )

    rules_raw = raw.get("rules") or []
    if not isinstance(rules_raw, list):
        raise RequirementConsolidationError("rules must be a list")
    rules: list[CompatibilityRule] = []
    seen_rule_ids: set[str] = set()
    for index, item in enumerate(rules_raw):
        location = f"rules[{index}]"
        if not isinstance(item, dict):
            raise RequirementConsolidationError(f"{location} must be a mapping")
        rule_id = item.get("id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            raise RequirementConsolidationError(f"{location}.id must be non-empty")
        if rule_id in seen_rule_ids:
            raise RequirementConsolidationError(
                f"duplicate compatibility rule id {rule_id!r}"
            )
        seen_rule_ids.add(rule_id)
        when_raw = item.get("when")
        requirements_raw = item.get("requirements")
        if not isinstance(when_raw, dict) or not when_raw:
            raise RequirementConsolidationError(
                f"{location}.when must be a non-empty mapping"
            )
        if not isinstance(requirements_raw, dict) or not requirements_raw:
            raise RequirementConsolidationError(
                f"{location}.requirements must be a non-empty mapping"
            )
        conditions: dict[str, SpecifierSet] = {}
        for name, value in when_raw.items():
            if not isinstance(value, str):
                raise RequirementConsolidationError(
                    f"{location}.when.{name} must be a specifier string"
                )
            try:
                canonical_name = _policy_package_name(name, f"{location}.when")
                conditions[canonical_name] = SpecifierSet(value)
            except InvalidSpecifier as exc:
                raise RequirementConsolidationError(
                    f"{location}.when.{name} has invalid specifier {value!r}"
                ) from exc
        replacements = {
            _policy_package_name(
                name, f"{location}.requirements"
            ): _policy_requirement(
                name, value, f"{location}.requirements.{name}", allow_url=True
            )
            for name, value in requirements_raw.items()
        }
        rules.append(CompatibilityRule(
            rule_id=rule_id,
            when=conditions,
            requirements=replacements,
            reason=str(item.get("reason") or ""),
        ))
    return CompatibilityPolicy(
        source=str(selected),
        protected_packages=protected,
        rules=tuple(rules),
        runtime_provided=runtime_provided,
        import_to_distribution=import_to_distribution,
        import_distribution_aliases=import_distribution_aliases,
        provided_imports=provided_imports,
        jvm_roots=jvm_roots,
        stdlib_imports=stdlib_imports,
    )


def _iter_requirement_lines(content: str) -> Iterable[str]:
    for raw in (content or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # A whitespace-prefixed # starts an inline comment in requirements
        # syntax (ANY whitespace — pip accepts tabs too; a tab-prefixed
        # comment previously smuggled its pin past conflict detection as a
        # passthrough line). Do not split URL fragments such as
        # pkg @ https://.../#sha256 (no whitespace before the #).
        _cm = re.search(r"\s#", line)
        if _cm:
            line = line[:_cm.start()].rstrip()
        if line:
            yield line


def _exact_versions(req: Requirement) -> set[str]:
    return {
        item.version
        for item in req.specifier
        if item.operator in ("==", "===") and "*" not in item.version
    }


@dataclass(frozen=True)
class _RequirementEntry:
    requirement: Requirement
    source: str
    line: str


def _source_details(entries: Sequence[_RequirementEntry]) -> str:
    return "; ".join(
        f"{entry.source}: {entry.line}"
        for entry in entries
    )


def _compatible_upper(version: Version) -> Version:
    release = list(version.release)
    if len(release) <= 2:
        return Version(f"{release[0] + 1}.0")
    prefix = release[:-1]
    prefix[-1] += 1
    return Version(".".join(str(part) for part in prefix))


def _constraint_bounds(entries: Sequence[_RequirementEntry]):
    lower = None
    upper = None
    exact_candidates: set[Version] = set()

    def update_lower(version: Version, inclusive: bool):
        nonlocal lower
        if lower is None or version > lower[0] or (
            version == lower[0] and lower[1] and not inclusive
        ):
            lower = (version, inclusive)

    def update_upper(version: Version, inclusive: bool):
        nonlocal upper
        if upper is None or version < upper[0] or (
            version == upper[0] and upper[1] and not inclusive
        ):
            upper = (version, inclusive)

    for entry in entries:
        for spec in entry.requirement.specifier:
            try:
                if spec.operator in ("==", "===") and "*" not in spec.version:
                    exact_candidates.add(Version(spec.version))
                elif spec.operator == "==" and spec.version.endswith(".*"):
                    prefix = [int(part) for part in spec.version[:-2].split(".")]
                    start = Version(".".join(str(part) for part in prefix))
                    end_parts = list(prefix)
                    end_parts[-1] += 1
                    end = Version(".".join(str(part) for part in end_parts))
                    update_lower(start, True)
                    update_upper(end, False)
                elif spec.operator == ">=":
                    update_lower(Version(spec.version), True)
                elif spec.operator == ">":
                    update_lower(Version(spec.version), False)
                elif spec.operator == "<=":
                    update_upper(Version(spec.version), True)
                elif spec.operator == "<":
                    update_upper(Version(spec.version), False)
                elif spec.operator == "~=":
                    version = Version(spec.version)
                    update_lower(version, True)
                    update_upper(_compatible_upper(version), False)
            except (InvalidVersion, ValueError):
                # Leave unusual PEP 440 forms to pip rather than rejecting a
                # requirement we cannot prove is impossible.
                continue
    return lower, upper, exact_candidates


def _validate_constraints(name: str, entries: Sequence[_RequirementEntry]) -> None:
    # Reject constraint combinations that NO version can satisfy: an exact pin
    # outside a co-asserted range, or disjoint ranges. This is used for the
    # no-strict-pin render path and for cross-overlapping-marker checks. Callers
    # that want highest-wins for a single group's multiple exact pins must NOT
    # route those raw pins here — _render_group resolves them directly, and the
    # cross-marker path passes highest-wins-RESOLVED entries (see
    # _effective_entries) so two groups that both resolve to the same version do
    # not false-trip the exact-viability check below.
    lower, upper, exact_candidates = _constraint_bounds(entries)
    combined = [
        spec
        for entry in entries
        for spec in entry.requirement.specifier
    ]
    if exact_candidates:
        viable = [
            candidate for candidate in exact_candidates
            if all(spec.contains(candidate, prereleases=True) for spec in combined)
        ]
        if not viable:
            raise RequirementConsolidationError(
                f"{name}: incompatible constraints; sources: {_source_details(entries)}"
            )
    if lower is not None and upper is not None:
        impossible = lower[0] > upper[0] or (
            lower[0] == upper[0] and (not lower[1] or not upper[1])
        )
        if impossible:
            raise RequirementConsolidationError(
                f"{name}: disjoint version ranges; sources: {_source_details(entries)}"
            )


def _markers_overlap(left: str, right: str) -> bool:
    if not left or not right or left == right:
        return True
    base = default_environment()
    # Self-consistent (sys_platform, platform_system, os_name) triples so the
    # overlap decision does not depend on the EXPORTING host: leaving
    # platform_system/os_name at the laptop's values made
    # `platform_system=="Linux"` vs `os_name=="posix"` evaluate differently on
    # Windows vs Linux (one renders both marker lines and pip fails on the AIDP
    # Linux target; the other raises at export). Overriding the whole triple
    # makes both cases raise everywhere.
    _os_triples = (
        ("linux", "Linux", "posix"),
        ("win32", "Windows", "nt"),
        ("darwin", "Darwin", "posix"),
    )
    environments = []
    for py_version in ("3.8", "3.9", "3.10", "3.11", "3.12", "3.13"):
        for sys_platform, platform_system, os_name in _os_triples:
            for machine in ("x86_64", "aarch64", "arm64"):
                env = dict(base)
                env.update({
                    "python_version": py_version,
                    "python_full_version": py_version + ".0",
                    "sys_platform": sys_platform,
                    "platform_system": platform_system,
                    "os_name": os_name,
                    "platform_machine": machine,
                    # Override the remaining host-derived marker vars too, so the
                    # overlap decision is fully independent of the exporting
                    # laptop (a marker on implementation_name / platform_release
                    # otherwise evaluated against the host).
                    "implementation_name": "cpython",
                    "implementation_version": py_version + ".0",
                    "platform_release": "",
                    "platform_version": "",
                    "platform_python_implementation": "CPython",
                })
                environments.append(env)
    return any(Marker(left).evaluate(env) and Marker(right).evaluate(env)
               for env in environments)


def _effective_entries(
    name: str, entries: Sequence[_RequirementEntry]
) -> list:
    """Highest-wins-RESOLVED entries for a single marker group, used only for
    cross-overlapping-marker conflict checks.

    A group's multiple strict ``==`` pins collapse to one ``==<highest>`` entry
    (exactly what _render_group emits), so two overlapping groups that both
    resolve to the same version are compatible instead of tripping the raw-pin
    viability check in _validate_constraints. Groups without a strict pin
    (ranges only, or arbitrary ``===``) keep their entries unchanged. Markers do
    not affect version bounds and are omitted from the synthetic entry.
    """
    pins: set[str] = set()
    has_arbitrary = False
    for entry in entries:
        for spec in entry.requirement.specifier:
            if spec.operator == "==" and "*" not in spec.version:
                pins.add(spec.version)
            elif spec.operator == "===":
                has_arbitrary = True
    if pins and not has_arbitrary:
        chosen = max(pins, key=lambda v: (Version(v), v))
        req = Requirement(f"{canonicalize_name(name)}=={chosen}")
        # Attribute to the distinct source names, not the full per-entry detail:
        # this synthetic entry can surface in a cross-marker conflict error, and
        # folding the whole `_source_details` string in as one entry's `source`
        # produces a garbled nested "src: line: line" message.
        srcs = []
        for entry in entries:
            if entry.source not in srcs:
                srcs.append(entry.source)
        return [_RequirementEntry(
            req, "; ".join(srcs) + " (resolved highest)", f"{name}=={chosen}")]
    return list(entries)


def _render_group(
    name: str, marker: str, entries: Sequence[_RequirementEntry],
    conflicts: list | None = None,
) -> str:
    requirements = [entry.requirement for entry in entries]
    urls = {req.url for req in requirements if req.url}
    if len(urls) > 1 or (urls and any(not req.url for req in requirements)):
        raise RequirementConsolidationError(
            f"{name}: incompatible direct URL and index requirements; "
            f"sources: {_source_details(entries)}"
        )

    extras = sorted({extra for req in requirements for extra in req.extras})
    rendered_name = canonicalize_name(name)
    if extras:
        rendered_name += "[" + ",".join(extras) + "]"

    if urls:
        rendered = f"{rendered_name} @ {next(iter(urls))}"
        if marker:
            # spaced separator: packaging rejects `url;marker` (see
            # _reparse_policy_render) and pip accepts both spacings.
            rendered += f" ; {marker}"
        return rendered

    # Only STRICT ``==`` pins (no wildcard) participate in highest-wins.
    # Arbitrary-equality ``===`` is a different operator whose right-hand side
    # need not be a PEP 440 version (e.g. ``foo===abc``); it is preserved
    # verbatim, never parsed as a Version or rewritten to ``==``.
    pin_versions: set[str] = set()
    arbitrary_versions: set[str] = set()
    for req in requirements:
        for spec in req.specifier:
            if spec.operator == "==" and "*" not in spec.version:
                pin_versions.add(spec.version)
            elif spec.operator == "===":
                arbitrary_versions.add(spec.version)
    has_arbitrary_equality = bool(arbitrary_versions)

    if has_arbitrary_equality and len(arbitrary_versions | pin_versions) > 1:
        # === is arbitrary-string equality: not orderable (cannot highest-win)
        # and never satisfiable alongside a different === or a strict == pin. A
        # SINGLE === is valid and preserved verbatim; conflicting values fail
        # loud (with attribution) instead of emitting an unsatisfiable line.
        raise RequirementConsolidationError(
            f"{name}: conflicting exact/arbitrary-equality pins "
            f"({', '.join(sorted(arbitrary_versions | pin_versions))}); "
            f"sources: {_source_details(entries)}"
        )

    if pin_versions and not has_arbitrary_equality:
        # An exact pin is authoritative. Render ONLY the (highest) pinned
        # version and drop redundant range bounds, so the line reads e.g.
        # `foo==1.5` instead of `foo==1.5,>=1.0` (clearer for operators). When
        # multiple DISTINCT exact pins collide across ANY sources (notebook
        # %pip, init-script pip, PyPI libraries, requirements files) — or the
        # chosen pin does not satisfy a range asserted elsewhere in the group —
        # keep the HIGHEST version and RECORD the collapse instead of failing:
        # a version clash must never block AIDP cluster creation. Markers keep
        # groups separate here; overlapping-marker exact conflicts are caught
        # before rendering (see the cross-marker check in
        # consolidate_requirement_sources + _effective_entries).
        # Tie-break on the raw string so the choice is deterministic even when
        # two spellings compare equal (e.g. "1.0" vs "1.0.0") — the generated
        # file must be byte-identical across re-runs.
        chosen = max(pin_versions, key=lambda v: (Version(v), v))
        combined = [spec for req in requirements for spec in req.specifier]
        chosen_satisfies_all = all(
            spec.contains(chosen, prereleases=True) for spec in combined
        )
        # Count DISTINCT versions, not raw spellings: "1.0" and "1.0.0" are the
        # same release, so they are not a conflict (only the deterministic
        # tie-break above needs the raw strings).
        distinct = {Version(v) for v in pin_versions}
        # Name the constraints the winner does NOT satisfy — the entries whose
        # requirement is dropped. Without this the header/report say a version
        # "won" but never say what it overrode (a lower pin, or a range/!= the
        # chosen version violates).
        dropped = sorted({
            f"{entry.source}: {entry.line}"
            for entry in entries
            if not all(sp.contains(chosen, prereleases=True)
                       for sp in entry.requirement.specifier)
        })
        if (len(distinct) > 1 or not chosen_satisfies_all) and conflicts is not None:
            # Two distinct pins => genuine highest-wins. A SINGLE pin that a
            # co-asserted range/exclusion forbids is not "highest" of anything —
            # it is the pin overriding that constraint; label it honestly.
            kind = "highest-wins" if len(distinct) > 1 else "pin-override"
            conflicts.append({
                "name": canonicalize_name(name),
                "marker": marker,
                "versions": sorted(pin_versions, key=lambda v: (Version(v), v)),
                "chosen": chosen,
                "dropped": dropped,
                "kind": kind,
                "sources": _source_details(entries),
            })
        rendered = f"{rendered_name}=={chosen}"
    else:
        # No exact pin — only ranges/bare. Reject genuinely disjoint ranges
        # (nothing to pick), otherwise intersect the specifiers onto one line.
        _validate_constraints(name, entries)
        specifiers = {
            str(specifier)
            for req in requirements
            for specifier in req.specifier
        }
        rendered = rendered_name
        if specifiers:
            rendered += ",".join(sorted(specifiers))

    if marker:
        rendered += f"; {marker}"
    return rendered


def _condition_matches(
    grouped: Mapping[tuple[str, str], Sequence[_RequirementEntry]],
    package: str,
    condition: SpecifierSet,
) -> bool:
    """Return true only when a discovered exact pin satisfies a rule condition.

    Compatibility rewrites are intentionally conservative: a VERSIONED rule
    condition cannot fire merely because an unpinned or ranged requirement
    *might* select a matching version. This prevents a future policy entry
    from silently rewriting a workload whose actual source version is
    ambiguous.

    An EMPTY condition (`when: {package: ""}`) means presence-of-package, any
    version including unpinned — for packages that are uninstallable from the
    index at EVERY version, where the version-agnostic rewrite is the point
    (flash-attn: PyPI/mirror carry only sdists whose build imports torch,
    absent from the isolated installer env; incident 2026-10-05, job
    STANDALONE_GENERIC_E2E 1000000000061_84953597 — the incident's trigger was
    exactly an UNPINNED flash-attn, which the exact-pin semantics miss).
    """
    present = False
    versions: set[str] = set()
    for (name, _marker), entries in grouped.items():
        if name != package:
            continue
        present = True
        for entry in entries:
            versions.update(_exact_versions(entry.requirement))
    if not condition:
        return present
    return bool(versions) and all(
        condition.contains(version, prereleases=True) for version in versions
    )


def _reparse_policy_render(rendered: str) -> Requirement:
    """Re-parse a policy-rendered requirement line, keeping the loud-but-
    structured failure contract: any render a future rule produces that
    packaging rejects must surface as RequirementConsolidationError (exit 15),
    never an uncaught InvalidRequirement traceback (PR #202 review, F1)."""
    try:
        return Requirement(rendered)
    except InvalidRequirement as exc:
        raise RequirementConsolidationError(
            f"compatibility-rule render is not a valid requirement: "
            f"{rendered!r} ({exc})"
        ) from exc


def _replace_policy_requirement(
    grouped: dict[tuple[str, str], list[_RequirementEntry]],
    package: str,
    requirement: Requirement,
    source: str,
    inject_markers: "list[str] | None" = None,
) -> None:
    existing = {
        key: entries
        for key, entries in grouped.items()
        if key[0] == package
    }
    for key in existing:
        del grouped[key]
    if not existing:
        # Injecting a package that was ABSENT from the source. Carry the marker(s)
        # of the requirement(s) that TRIGGERED this rule so the injected package
        # installs only where the trigger does. An unconditional inject would, for
        # a marker-gated trigger (e.g. `tensorflow; platform_machine == 'x86_64'`,
        # which is false on an ARM cluster), leave the trigger inactive while this
        # package installed anyway — wrong on AIDP's mixed ARM/AMD/Intel/GPU fleet.
        # An empty/absent marker set => unconditional inject, exactly as before
        # (the common no-marker case, so behaviour is unchanged there).
        markers: list[str] = []
        for m in (inject_markers or [""]):
            if m not in markers:
                markers.append(m)
        if not markers or "" in markers:
            markers = [""]  # an unconditional trigger => inject unconditionally
        for marker in markers:
            # packaging's URL token is greedy up to whitespace: `url;marker`
            # swallows the semicolon and fails to parse. pip accepts both
            # spacings; packaging only the spaced form (PR #202 review, F1).
            rendered = str(requirement) + (f" ; {marker}" if marker else "")
            grouped[(package, marker)] = [
                _RequirementEntry(_reparse_policy_render(rendered), source, rendered)
            ]
        return

    # A compatibility rule replaces only the package version/source.  Extras
    # and environment markers describe how the workload consumes the package
    # and must survive (for example tensorflow[and-cuda]; sys_platform...).
    for (name, marker), entries in existing.items():
        extras = sorted({
            extra
            for entry in entries
            for extra in entry.requirement.extras
        })
        rendered = requirement.name
        if extras:
            rendered += f"[{','.join(extras)}]"
        if requirement.url:
            rendered += f" @ {requirement.url}"
        else:
            rendered += str(requirement.specifier)
        if marker:
            rendered += f" ; {marker}"
        rewritten = _reparse_policy_render(rendered)
        grouped[(name, marker)] = [
            _RequirementEntry(rewritten, source, rendered)
        ]


def _apply_compatibility_policy(
    grouped: dict[tuple[str, str], list[_RequirementEntry]],
    policy: CompatibilityPolicy,
) -> list[str]:
    source_packages = {package for package, _marker in grouped}
    applied: list[str] = []
    for rule in policy.rules:
        if not all(
            _condition_matches(grouped, package, condition)
            for package, condition in rule.when.items()
        ):
            continue
        source = f"AIDP compatibility rule {rule.rule_id}"
        # Markers carried by the source requirement(s) that TRIGGERED this rule
        # (computed BEFORE we mutate `grouped`). An injected/absent rule package
        # inherits these so it can't install where the trigger doesn't (see
        # _replace_policy_requirement). Existing packages keep their own markers.
        trigger_markers = sorted({
            marker
            for (name, marker) in grouped
            for pkg in rule.when
            if name == pkg
        })
        dropped = sorted({
            entry.line
            for package in rule.requirements
            for (name, _marker), entries in grouped.items() if name == package
            for entry in entries
            if entry.line != str(rule.requirements[package])
        })
        for package, requirement in rule.requirements.items():
            _replace_policy_requirement(
                grouped, package, requirement, source,
                inject_markers=trigger_markers,
            )
        # Surface WHAT the rule overrode, not just that it fired — parity with
        # the protected-packages path below (PR #202 review, A3: a workload's
        # own torch/flash-attn pins silently vanished from the header, and the
        # torch highest-wins conflict WARN disappears when the rule fires).
        applied.append(
            rule.rule_id + (f" (replaced: {'; '.join(dropped)})" if dropped else "")
        )

    # Protected packages are enforced last so no source or conditional rule can
    # downgrade packages required for kernel-to-driver communication. Do not
    # inject a protected package that was absent from every source: the policy
    # constrains requested dependencies; it is not an implicit baseline.
    for package, requirement in policy.protected_packages.items():
        if package not in source_packages:
            continue
        # Report every replacement in the header (PR #88 review, major 3):
        # silently rewriting a workload pin — possibly violating the
        # workload's own bound — is the exact defect class the
        # runtime-provided loop below reports. Record what was dropped.
        dropped = sorted({
            entry.line
            for (name, _marker), entries in grouped.items() if name == package
            for entry in entries
            if entry.line != str(requirement)
        })
        _replace_policy_requirement(
            grouped,
            package,
            requirement,
            "AIDP protected runtime package",
        )
        applied.append(
            f"protected:{package}={requirement.specifier}"
            + (f" (replaced: {', '.join(dropped)})" if dropped else "")
        )

    # Runtime-provided packages (e.g. mlflow) are enforced last, like
    # protected ones: EVERY workload pin — even conflicting pins across
    # sources — collapses to the bare name, so pip sees the runtime's
    # integrated version as satisfied and leaves it alone (installing a
    # pinned version over it shadows the platform integration — the
    # manual-pip/numpy _ARRAY_API incident class). Never injected when
    # absent from every source; every collapse is visible in the header
    # via the applied list.
    for package in sorted(policy.runtime_provided):
        if package not in source_packages:
            continue
        dropped = sorted({
            entry.line
            for (name, _marker), entries in grouped.items() if name == package
            for entry in entries
            if str(entry.requirement.specifier) or entry.requirement.url
        })
        _replace_policy_requirement(
            grouped,
            package,
            Requirement(package),
            "AIDP runtime-provided package (unpinned; runtime version wins)",
        )
        applied.append(
            f"runtime-provided:{package}"
            + (f" (dropped: {', '.join(dropped)})" if dropped else "")
        )
    return applied


def _conflict_summary(c: dict) -> str:
    """One-line, honest description of a collapse for logs/headers/report.

    Distinguishes the two cases the collapse produces:
      - highest-wins: >=2 distinct pinned versions, highest kept.
      - pin-override: a single pin the chosen version satisfies but a
        co-asserted range/exclusion forbids — the pin overrides that
        constraint (it is NOT the "highest" of anything).
    Always names the dropped constraint so an operator can trace it.
    """
    dropped = "; ".join(c.get("dropped") or [])
    if c.get("kind") == "pin-override":
        base = (f"{c['name']}: exact pin {c['chosen']} overrides "
                f"{dropped or 'a co-asserted constraint'}")
    else:
        base = (f"{c['name']}: conflicting pins "
                f"{', '.join(c['versions'])} — kept highest {c['chosen']}")
        if dropped:
            base += f", dropped {dropped}"
    return base


def consolidate_requirement_sources(
    sources: Sequence[RequirementSource],
    compatibility_policy: CompatibilityPolicy | None = None,
    on_conflict=None,
) -> str:
    """Return a deterministic, single-file union of direct requirements.

    Duplicate bare requirements are absorbed by constrained forms. Distinct
    compatible specifiers are intersected onto one requirement line, and an
    exact pin collapses redundant bounds (``foo==1.5,>=1.0`` -> ``foo==1.5``).

    When the same package resolves to multiple distinct EXACT versions across
    PEP 508-parsable sources (notebook %pip, init-script pip, PyPI libraries,
    requirements files), the HIGHEST is kept (highest-wins); a single exact pin
    that a co-asserted range/exclusion forbids is kept as a *pin-override*. Both
    are reported via ``on_conflict``, a ``UserWarning``, and a header comment,
    and never block cluster creation; the operator edits the source if a lower
    version is required.

    Caveats:
      - A bare (marker-less) pin overlaps EVERY marker group, so a bare pin plus
        a differently-pinned marked pin is a genuine conflict and still fails
        loudly (there is no single version valid in the bare pin's universe).
      - Non-PEP-508 lines (``--hash=…``, ``-r file``, ``-e``, VCS URLs) are
        preserved verbatim and do NOT participate in the collapse — a version
        clash on such a line is not resolved here and can surface at pip time.
      - Genuinely unsatisfiable RANGE combinations, conflicting exact pins across
        OVERLAPPING markers, and conflicting ``===`` pins still fail loudly
        (nothing to pick).

    ``on_conflict``: optional callable invoked once per collapsed package with a
    dict ``{name, marker, versions, chosen, dropped, kind, sources}``
    (``kind`` is ``highest-wins`` or ``pin-override``).
    """
    grouped: dict[tuple[str, str], list[_RequirementEntry]] = {}
    passthrough: list[str] = []
    seen_passthrough: set[str] = set()
    used_sources: list[str] = []

    for source in sources:
        found = False
        for line in _iter_requirement_lines(source.content):
            found = True
            try:
                req = Requirement(line)
            except InvalidRequirement:
                # pip supports forms outside PEP 508: local/relative wheels,
                # requirement options (including --hash), VCS/editable entries,
                # and resolver flags. Preserve them and warn instead of aborting
                # the entire workflow export.
                if line not in seen_passthrough:
                    passthrough.append(line)
                    seen_passthrough.add(line)
                warnings.warn(
                    f"{source.name}: preserving pip-specific requirement "
                    f"without consolidation: {line!r}",
                    UserWarning,
                    stacklevel=2,
                )
                continue
            marker = str(req.marker) if req.marker else ""
            key = (canonicalize_name(req.name), marker)
            grouped.setdefault(key, []).append(
                _RequirementEntry(req, source.name, line)
            )
        if found:
            used_sources.append(source.name)

    applied_rules: list[str] = []
    if compatibility_policy is not None:
        applied_rules = _apply_compatibility_policy(grouped, compatibility_policy)

    by_name: dict[str, list[tuple[str, list[_RequirementEntry]]]] = {}
    for (name, marker), entries in grouped.items():
        by_name.setdefault(name, []).append((marker, entries))
    for name, marker_groups in by_name.items():
        for index, (left_marker, left_entries) in enumerate(marker_groups):
            for right_marker, right_entries in marker_groups[index + 1:]:
                if _markers_overlap(left_marker, right_marker):
                    # Two DIFFERENT marker groups that both apply in some
                    # environment cannot be collapsed to one line. Compare their
                    # HIGHEST-WINS-RESOLVED forms: if no single version satisfies
                    # both (e.g. ==2.0 vs <1.3, both active on py3.9), fail
                    # loudly at export rather than emit both lines and let pip
                    # fail during cluster provisioning. Resolving each side FIRST
                    # avoids a false positive when one side's multiple pins
                    # collapse to the other side's version (==1.0,==2.0 vs ==2.0
                    # -> both 2.0, compatible).
                    _validate_constraints(name, [
                        *_effective_entries(name, left_entries),
                        *_effective_entries(name, right_entries),
                    ])

    conflicts: list[dict] = []
    rendered = [
        _render_group(name, marker, entries, conflicts)
        for (name, marker), entries in sorted(grouped.items())
    ]
    rendered.extend(passthrough)

    # Surface every collapse (never silent). The message names the dropped
    # constraint and labels the two cases honestly: multiple distinct pins =>
    # highest-wins; a single pin that a co-asserted range/exclusion forbids =>
    # pin-override (NOT "highest" of anything).
    for c in sorted(conflicts, key=lambda c: c["name"]):
        warnings.warn(
            _conflict_summary(c) + f"; sources: {c['sources']}",
            UserWarning,
            stacklevel=2,
        )
        if on_conflict is not None:
            on_conflict(c)

    header = [
        "# Auto-generated consolidated requirements. Do not edit by hand.",
        "# Installed as the workflow's only Python requirements library.",
        "# Sources: " + (", ".join(used_sources) if used_sources else "none"),
        "# AIDP compatibility policy: " + (
            compatibility_policy.source if compatibility_policy else "not applied"
        ),
        "# Applied compatibility rules: " + (
            ", ".join(applied_rules) if applied_rules else "none"
        ),
    ]
    for c in sorted(conflicts, key=lambda c: c["name"]):
        header.append("# " + _conflict_summary(c))
    header.append("")
    return "\n".join(header + rendered) + "\n"
