"""Cross-boundary error classes.

Houses the subset of orchestrator error classes that schema-level modules
(``schema.bundle.load_bundle``, ``schema.plan_resolver.resolve_dry_run_plan``)
need to raise, and that downstream consumers (dispatch package, CLI) need to
catch. Schema-level callers raising from ``orchestrator.errors`` would
transitively import ``orchestrator/__init__.py`` and load every engine
subsystem — a direct violation of the dispatch import-boundary rule.

``orchestrator.errors`` re-exports these names for back-compat so every
existing engine-side import path continues to resolve. Identity is preserved:
``orchestrator.errors.BundleLoadError is schema.errors.BundleLoadError``.
"""

from __future__ import annotations


class OrchestratorConfigError(Exception):
    """Marker base class for user-facing config / pre-dispatch errors.

    The CLI catches ``OrchestratorConfigError`` and exits 2 with ``str(exc)``
    — no traceback. Subclasses must produce a self-explanatory ``__str__``
    (the CLI prints it verbatim without extra framing).

    Engine-only subclasses (``UnsupportedModeError``, ``PrerequisiteError``,
    ``CredentialResolutionError``, ``IncrementalCursorMissingError``, etc.)
    live in ``orchestrator.errors`` and still inherit from this class — the
    single ``except OrchestratorConfigError:`` clause in the CLI catches
    them all.
    """


class BundleLoadError(OrchestratorConfigError):
    """Wraps every bundle.yaml load failure into one class so the CLI's
    exit-2 path catches them uniformly. Failure modes: file unreadable,
    YAML parse error, env-var missing, Pydantic schema violation, bad
    ``aidp.*`` SQL identifier.
    """


class BundleVersionMismatchError(BundleLoadError):
    """Raised when ``bundle.version`` is unknown to this plugin build.
    The message names the offending version + the supported set + the
    ``aidp-fusion-autopilot migrate-bundle`` remediation command. Inherits
    from ``BundleLoadError`` because it is a load failure with a specific
    remediation; the CLI's exit-2 catch picks it up via the transitive
    ``OrchestratorConfigError`` ancestor.
    """


class MissingDependencyError(OrchestratorConfigError):
    """Logical missing dependency — a bundle.yaml name that doesn't
    resolve to any content-pack node (bronze / silver / gold) and is
    also not on the deferred-name list, or a ``--datasets`` / ``--layers``
    filter naming a value that doesn't exist. Raised by the schema-level
    plan resolver.
    """


# ---------------------------------------------------------------------------
# Runtime schema-fingerprint drift detection
# ---------------------------------------------------------------------------


EXIT_CODE_SCHEMA_DRIFT = 14
"""Process exit code emitted by the CLI when bronze-schema fingerprint
drift is detected at runtime.

Reserved bootstrap/runtime exit codes:
* 11 — AIDPF-1020 (operator identity unresolved)
* 12 — AIDPF-2010 (columnAlias unresolved)
* 13 — AIDPF-2011 (semanticVariant unresolved)
* 14 — AIDPF-2012 (bronze-schema fingerprint drift)
"""


class SchemaDriftDetectedError(Exception):
    """Raised when ``check_bronze_fingerprint_drift`` finds the live
    bronze fingerprint differs from the value pinned in the tenant
    profile (AIDPF-2012).

    **Boundary placement**: this exception lives in ``schema/errors.py``
    (neutral module) — NOT in ``orchestrator/`` — because
    ``dispatch/__init__.py:8`` explicitly forbids dispatch from
    importing ``orchestrator/*``. Both the inline path
    (``orchestrator.preflight_evidence``) and the REST-dispatch path
    (``dispatch/__init__.py``) need to raise this exception, so it
    has to live on the dispatch-allowed side. Inheriting from plain
    ``Exception`` (NOT :class:`OrchestratorConfigError`) prevents the
    existing CLI exit-2 catch arm from swallowing it; the CLI gets a
    dedicated arm that maps to :data:`EXIT_CODE_SCHEMA_DRIFT` (14).

    Carries enough context for the CLI hand-off message + audit
    correlation:
    """

    def __init__(
        self,
        *,
        run_id: str,
        diagnostic_path: "Path",
        summary: str,
        prior_fingerprint: str,
        current_fingerprint: str,
    ) -> None:
        self.run_id = run_id
        """Bootstrap-run identifier. SAME ``run_id`` as the eventual
        ``RunSummary``'s — the gate runs after the run's run_id mint
        inside ``_run_content_pack_backend`` so the drift artifact,
        any force-skip audit row, and the run-summary all correlate."""

        self.diagnostic_path = diagnostic_path
        """Absolute path to the written ``AIDPF-2012.json`` artifact.
        Under REST dispatch, the dispatcher reconstructs the file from
        the marker payload at the same shape before raising."""

        self.summary = summary
        """The multi-line hand-off message the CLI prints on stderr."""

        self.prior_fingerprint = prior_fingerprint
        self.current_fingerprint = current_fingerprint

        super().__init__(
            f"AIDPF-2012: bronze schema fingerprint drift detected "
            f"(run_id={run_id}; "
            f"prior={prior_fingerprint[:24]}... → "
            f"current={current_fingerprint[:24]}...)"
        )


__all__ = [
    "EXIT_CODE_SCHEMA_DRIFT",
    "OrchestratorConfigError",
    "BundleLoadError",
    "BundleVersionMismatchError",
    "MissingDependencyError",
    "SchemaDriftDetectedError",
]
