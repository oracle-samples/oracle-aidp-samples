"""P1.5ε §Step 1a — schema/errors.py identity + boundary tests.

These tests lock the cross-boundary error contract: the four neutral classes
live in ``schema.errors`` and are re-exported from ``orchestrator.errors`` for
back-compat. Any future refactor that re-defines one of the four classes in
``orchestrator.errors`` breaks the identity-test invariant and surfaces here.
"""

from __future__ import annotations

import pytest


class TestSchemaErrorsIdentity:
    """``orchestrator.errors`` re-exports the four neutral classes —
    identity must be preserved so ``except`` clauses in either path catch
    the same instances."""

    def test_orchestrator_config_error_identity(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            OrchestratorConfigError as FromOrchestrator,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            OrchestratorConfigError as FromSchema,
        )
        assert FromOrchestrator is FromSchema

    def test_bundle_load_error_identity(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            BundleLoadError as FromOrchestrator,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            BundleLoadError as FromSchema,
        )
        assert FromOrchestrator is FromSchema

    def test_bundle_version_mismatch_error_identity(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            BundleVersionMismatchError as FromOrchestrator,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            BundleVersionMismatchError as FromSchema,
        )
        assert FromOrchestrator is FromSchema

    def test_missing_dependency_error_identity(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            MissingDependencyError as FromOrchestrator,
        )
        from oracle_ai_data_platform_fusion_autopilot.schema.errors import (
            MissingDependencyError as FromSchema,
        )
        assert FromOrchestrator is FromSchema


class TestEngineOnlyErrorsStayOrchestratorSide:
    """Engine-only errors (``UnsupportedModeError``, ``PrerequisiteError``,
    ``CredentialResolutionError`` etc.) MUST NOT live in ``schema.errors``.
    The schema-level module carries the cross-boundary subset only."""

    @pytest.mark.parametrize(
        "name",
        [
            "UnsupportedModeError",
            "PrerequisiteError",
            "CredentialResolutionError",
            "IncrementalCursorMissingError",
            "IncrementalTargetMissingError",
            "BronzeSchemaProbeError",
            "ResumeRunNotFoundError",
            "ResumeRunNotResumableError",
            "ResumeBundleMismatchError",
            "StateReadFailedError",
            "SchemaEvolutionTypeConflictError",
            "MultipleNaturalKeyError",
            "DiscoveryProbeError",
            "OrchestratorRuntimeError",
            "WatermarkMonotonicityError",
            "MultipleUpstreamWatermarkError",
        ],
    )
    def test_engine_only_error_not_in_schema(self, name: str) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema import errors as schema_errors

        assert not hasattr(schema_errors, name), (
            f"{name} must stay engine-side; schema.errors only carries the "
            "cross-boundary subset (OrchestratorConfigError, BundleLoadError, "
            "BundleVersionMismatchError, MissingDependencyError)"
        )


class TestInheritancePreserved:
    """The CLI's single ``except OrchestratorConfigError:`` clause must catch
    every cross-boundary error class — isinstance chain must hold across the
    re-export shim."""

    def test_bundle_load_error_is_orchestrator_config_error(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            BundleLoadError,
            OrchestratorConfigError,
        )
        assert issubclass(BundleLoadError, OrchestratorConfigError)
        assert isinstance(BundleLoadError("x"), OrchestratorConfigError)

    def test_bundle_version_mismatch_is_bundle_load_error(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            BundleLoadError,
            BundleVersionMismatchError,
            OrchestratorConfigError,
        )
        assert issubclass(BundleVersionMismatchError, BundleLoadError)
        assert isinstance(BundleVersionMismatchError("x"), OrchestratorConfigError)

    def test_missing_dependency_is_orchestrator_config_error(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.errors import (
            MissingDependencyError,
            OrchestratorConfigError,
        )
        assert issubclass(MissingDependencyError, OrchestratorConfigError)
        assert isinstance(MissingDependencyError("x"), OrchestratorConfigError)
