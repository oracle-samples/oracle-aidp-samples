"""``$$PARAM`` resolution -- a loader, not an expression (Class C).

Informatica resolves a ``$$PARAM`` reference by reading a ``.prm``
parameter file and applying **scope precedence**: a workflow-level
setting for a parameter beats a worklet-level setting, which beats a
session-level setting, which beats whatever the mapping itself declared
as a default. A pure inline expression can't do that -- it needs a file
read and a precedence merge, which is exactly why this is a loader
module rather than something ``expression_converter.py`` inlines.

This module never touches PySpark and has no reason to defer any import
-- it is plain file/text parsing plus a dict merge. It is still shipped
inside ``infa_compat`` (not ``infa2aidp``) because it is cluster-side
runtime code: the generated notebook calls ``param()`` at execution time
against the ``.prm`` file staged alongside it on the cluster, the same
way it calls ``sequence()`` or ``cached_lookup()``.

Public API
----------
- :func:`load_parameter_file` -- parse one ``.prm`` file into a
  ``{section: {name: value}}`` dict.
- :class:`ParameterScope` -- holds the merged, precedence-resolved view
  for one workflow/worklet/session/mapping combination.
- :func:`param` -- module-level convenience that resolves a single
  ``$$PARAM`` name against a process-wide default :class:`ParameterScope`
  (set via :func:`load_parameter_file` + :meth:`ParameterScope.activate`,
  or passed explicitly).
- :func:`sess_start_time` -- Informatica's ``$$$SessStartTime`` built-in:
  the wall-clock time the session started, fixed for the lifetime of the
  session (not re-evaluated per row -- that is the entire point of the
  built-in, and why it is a function call here rather than
  ``datetime.now()`` inlined at every reference).
"""
from __future__ import annotations

import configparser
import datetime as _dt
from pathlib import Path
from typing import Optional, Union

# Scope precedence, most-specific first. A parameter declared at a more
# specific scope always wins over the same name at a less specific one --
# this mirrors the Integration Service's own resolution order for
# workflow/worklet/session variables and mapping parameters (Informatica
# Administrator Guide, "Parameter and Variable Precedence"). Assumption
# flagged here because it is asserted from documented behavior, not
# re-derived from a live Integration Service in this project: if a real
# customer ``.prm`` file is ever found to resolve differently, this order
# is the first thing to revisit.
SCOPE_PRECEDENCE: tuple[str, ...] = ("workflow", "worklet", "session", "mapping")


class ParameterNotFoundError(KeyError):
    """Raised by :func:`param` / :meth:`ParameterScope.get` when a name is
    not defined at any scope and no default was supplied. Distinct from
    a plain ``KeyError`` so callers can catch it specifically without
    also swallowing an unrelated dict-lookup bug elsewhere in the same
    call site.
    """


def load_parameter_file(path: Union[str, Path]) -> dict[str, dict[str, str]]:
    """Parse an Informatica ``.prm`` file into ``{section: {name: value}}``.

    ``.prm`` files are INI-shaped: a ``[folder.WF:workflow_name]`` (or
    ``WORKLET:``/``SESS:``) section header followed by ``$$NAME=value``
    lines. Section names are kept verbatim (they carry the
    folder/workflow/worklet/session identity needed for scope matching);
    parameter names are stored WITH their leading ``$$`` stripped, since
    every other function in this module addresses them by bare name.

    ``configparser`` is used in non-strict mode (duplicate sections across
    a large ``.prm`` file are common in practice) with no value
    interpolation (a raw ``%`` in a connection string or format mask must
    not be treated as a template token).
    """
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    # configparser lowercases option names by default (optionxform =
    # str.lower) -- Informatica parameter names are conventionally
    # UPPERCASE and referenced case-sensitively via $$NAME, so the
    # default would silently turn every lookup into a case mismatch.
    parser.optionxform = str
    # Informatica .prm files don't require a DEFAULT section; configparser
    # is fine with that, but we read via read_string so a missing file
    # raises a normal FileNotFoundError instead of configparser's own
    # (less obvious) "no sections" silence.
    text = Path(path).read_text(encoding="utf-8")
    parser.read_string(text)
    result: dict[str, dict[str, str]] = {}
    for section in parser.sections():
        result[section] = {}
        for raw_name, value in parser.items(section):
            name = raw_name[2:] if raw_name.startswith("$$") else raw_name
            result[section][name] = value
    return result


def _scope_kind_from_section(section: str) -> Optional[str]:
    """Best-effort classification of a ``.prm`` section header into one of
    :data:`SCOPE_PRECEDENCE`'s kinds, by the conventional Informatica
    section-name prefixes (``WF:``/``WORKLET:``/``SESS:``). A section that
    matches none of these is treated as mapping-level (the least specific
    scope) -- documented assumption: some exported ``.prm`` files use a
    bare folder name with no kind prefix at all for mapping-level
    parameters, and there is no live Integration Service here to confirm
    every export tool's convention.
    """
    upper = section.upper()
    if "WF:" in upper or upper.startswith("WORKFLOW"):
        return "workflow"
    if "WORKLET:" in upper:
        return "worklet"
    if "SESS:" in upper or "SESSION:" in upper:
        return "session"
    return "mapping"


class ParameterScope:
    """A precedence-resolved view over one or more ``.prm`` sections.

    Built from the raw ``{section: {name: value}}`` shape
    :func:`load_parameter_file` returns. ``get``/``__getitem__`` apply
    :data:`SCOPE_PRECEDENCE`: the same parameter name defined at
    ``workflow`` scope always wins over ``worklet``, then ``session``,
    then ``mapping``, regardless of section iteration order.
    """

    def __init__(self, sections: dict[str, dict[str, str]]):
        self._by_scope: dict[str, dict[str, str]] = {k: {} for k in SCOPE_PRECEDENCE}
        for section, values in sections.items():
            kind = _scope_kind_from_section(section)
            if kind not in self._by_scope:
                continue
            self._by_scope[kind].update(values)
        self._sess_start_time: Optional[_dt.datetime] = None

    @classmethod
    def from_file(cls, path: Union[str, Path]) -> "ParameterScope":
        return cls(load_parameter_file(path))

    def get(self, name: str, default: object = None) -> object:
        for kind in SCOPE_PRECEDENCE:
            if name in self._by_scope[kind]:
                return self._by_scope[kind][name]
        if default is not None:
            return default
        raise ParameterNotFoundError(
            f"$${name} is not defined at any scope ({', '.join(SCOPE_PRECEDENCE)}) "
            f"and no default was supplied"
        )

    def __getitem__(self, name: str) -> object:
        return self.get(name)

    def mark_session_start(self, when: Optional[_dt.datetime] = None) -> _dt.datetime:
        """Fix ``$$$SessStartTime`` for this scope's lifetime. Call once,
        at session start, before any row is processed -- calling it again
        mid-session would defeat the "fixed for the session" contract, so
        this raises if it has already been set rather than silently
        re-stamping.
        """
        if self._sess_start_time is not None:
            raise RuntimeError(
                "mark_session_start() was already called for this "
                "ParameterScope -- $$$SessStartTime is fixed for the "
                "lifetime of the session and must not be re-stamped"
            )
        self._sess_start_time = when if when is not None else _dt.datetime.now()
        return self._sess_start_time

    @property
    def session_start_time(self) -> _dt.datetime:
        if self._sess_start_time is None:
            raise RuntimeError(
                "session_start_time was read before mark_session_start() "
                "was called -- $$$SessStartTime has no value yet"
            )
        return self._sess_start_time


# Process-wide default scope. Generated notebooks call
# ``params.load_parameter_file(...)`` once per session and then use the
# plain ``param()``/``sess_start_time()`` free functions everywhere else,
# mirroring how a real Informatica expression just says ``$$MY_PARAM``
# with no scope object threaded through every call site.
_active_scope: Optional[ParameterScope] = None


def activate(scope: ParameterScope) -> None:
    """Install ``scope`` as the default for :func:`param` /
    :func:`sess_start_time`. One process-wide default is deliberate --
    Informatica sessions run one at a time within a single Integration
    Service process context, and generated notebooks are one session per
    Spark application.
    """
    global _active_scope
    _active_scope = scope


def param(name: str, default: object = None, *, scope: Optional[ParameterScope] = None) -> object:
    """Resolve ``$$name`` against ``scope`` (or the process-wide default
    installed by :func:`activate`) with workflow > worklet > session >
    mapping precedence.

    Raises :class:`ParameterNotFoundError` if the name is undefined at
    every scope and ``default`` is ``None`` -- callers that genuinely want
    ``None`` as an allowed resolved value should catch that explicitly
    rather than relying on ``default=None`` to mean "and don't complain if
    it's missing."
    """
    active = scope if scope is not None else _active_scope
    if active is None:
        raise RuntimeError(
            "param() called with no active ParameterScope -- call "
            "params.activate(ParameterScope.from_file(...)) first, or "
            "pass scope= explicitly"
        )
    return active.get(name, default)


def sess_start_time(*, scope: Optional[ParameterScope] = None) -> _dt.datetime:
    """Return the fixed ``$$$SessStartTime`` for the active (or given)
    scope. Raises if :meth:`ParameterScope.mark_session_start` was never
    called -- there is no sensible silent default for "when did this
    session start" if nothing ever recorded it.
    """
    active = scope if scope is not None else _active_scope
    if active is None:
        raise RuntimeError(
            "sess_start_time() called with no active ParameterScope -- "
            "call params.activate(...) first, or pass scope= explicitly"
        )
    return active.session_start_time
