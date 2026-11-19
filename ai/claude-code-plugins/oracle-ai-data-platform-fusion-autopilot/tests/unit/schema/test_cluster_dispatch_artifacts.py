"""Step 8 tests — Phase 4.1 cluster-dispatch diagnostic artifacts.

Covers:

* ``ClusterDispatchDiagnosticV1`` + ``ClusterMarkerDiagnosticV1``
  Pydantic round-trip + schemaVersion=1 + extra="forbid".
* ``write_cluster_dispatch_diagnostic`` writes the JSON artifact
  atomically.
* ``write_cluster_marker_diagnostic`` writes the JSON artifact AND
  the companion ``cluster_stdout.log`` in one call.
* The two artifacts are mutually exclusive in the run dir (marker
  parsing only fires on successful fetch).

The medallion-author reader scope branch is tested in
``tests/unit/medallion_author/test_reader_phase4_1_scope.py``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED,
    AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID,
    ClusterDispatchDiagnosticV1,
    ClusterDispatchFailure,
    ClusterMarkerDiagnosticV1,
    ClusterMarkerFailure,
    DiagnosticArtifactAlreadyExistsError,
    write_cluster_dispatch_diagnostic,
    write_cluster_marker_diagnostic,
)


def _dispatch_artifact(**overrides) -> ClusterDispatchDiagnosticV1:
    base = dict(
        runId="bootstrap-20260607-abc12345",
        tenant="demo-pod",
        errorCode=AIDPF_2048_CLUSTER_BOOTSTRAP_DISPATCH_FAILED,
        errorMessage="cluster dispatch failed at upload_notebook",
        generatedAt=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        clusterDispatch={
            "failedStep": "upload_notebook",
            "causeType": "DispatchUploadError",
            "causeMessage": "HTTP 500",
            "workspacePath": "/Workspace/Shared/x.ipynb",
            "clusterKey": "cluster-uuid",
        },
    )
    base.update(overrides)
    return ClusterDispatchDiagnosticV1.model_validate(base)


def _marker_artifact(**overrides) -> ClusterMarkerDiagnosticV1:
    base = dict(
        runId="bootstrap-20260607-abc12345",
        tenant="demo-pod",
        errorCode=AIDPF_2049_CLUSTER_BOOTSTRAP_MARKER_INVALID,
        errorMessage="cluster marker invalid (envelope_missing)",
        generatedAt=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        clusterMarker={
            "kind": "envelope_missing",
            "stdoutExcerpt": "no marker here",
            "stdoutLogPath": "cluster_stdout.log",
        },
    )
    base.update(overrides)
    return ClusterMarkerDiagnosticV1.model_validate(base)


# ---------------------------------------------------------------------------
# Pydantic invariants
# ---------------------------------------------------------------------------


class TestClusterDispatchFailure:
    def test_round_trip_via_alias(self) -> None:
        artifact = _dispatch_artifact()
        dumped = artifact.model_dump(by_alias=True, mode="json")
        assert dumped["errorCode"] == "AIDPF-2048"
        assert dumped["clusterDispatch"]["failedStep"] == "upload_notebook"
        assert dumped["clusterDispatch"]["causeType"] == "DispatchUploadError"
        # Re-validate.
        restored = ClusterDispatchDiagnosticV1.model_validate(dumped)
        assert restored.cluster_dispatch.failed_step == "upload_notebook"

    def test_failed_step_enum_rejects_unknown(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClusterDispatchFailure.model_validate(
                {
                    "failedStep": "rocket_launch",  # not in the Literal
                    "causeType": "X",
                    "causeMessage": "y",
                }
            )

    def test_extra_field_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClusterDispatchFailure.model_validate(
                {
                    "failedStep": "upload_notebook",
                    "causeType": "X",
                    "causeMessage": "y",
                    "rogueField": "no",
                }
            )


class TestClusterMarkerFailure:
    def test_kind_discriminator_enum(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClusterMarkerFailure.model_validate(
                {
                    "kind": "made_up_kind",  # not in the Literal
                    "stdoutExcerpt": "x",
                    "stdoutLogPath": "cluster_stdout.log",
                }
            )

    def test_validation_errors_default_empty(self) -> None:
        f = ClusterMarkerFailure.model_validate(
            {
                "kind": "envelope_missing",
                "stdoutExcerpt": "",
                "stdoutLogPath": "cluster_stdout.log",
            }
        )
        assert f.validation_errors == []


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


class TestWriteClusterDispatchDiagnostic:
    def test_writes_AIDPF_2048_json(self, tmp_path: Path) -> None:
        run_id = "bootstrap-20260607-abc12345"
        path = write_cluster_dispatch_diagnostic(
            tmp_path, run_id, _dispatch_artifact()
        )
        assert path.name == "AIDPF-2048.json"
        assert path.parent == tmp_path / ".aidp" / "diagnostics" / run_id
        # Round-trip the file content via the model.
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert loaded["errorCode"] == "AIDPF-2048"
        assert loaded["clusterDispatch"]["failedStep"] == "upload_notebook"
        # schemaVersion=1 enforced.
        assert loaded["schemaVersion"] == 1

    def test_refuses_to_overwrite(self, tmp_path: Path) -> None:
        run_id = "bootstrap-collision"
        write_cluster_dispatch_diagnostic(tmp_path, run_id, _dispatch_artifact())
        with pytest.raises(DiagnosticArtifactAlreadyExistsError):
            write_cluster_dispatch_diagnostic(tmp_path, run_id, _dispatch_artifact())

    def test_rejects_unsafe_run_id(self, tmp_path: Path) -> None:
        from oracle_ai_data_platform_fusion_autopilot.schema.path_segment import (
            UnsafePathSegmentError,
        )

        with pytest.raises(UnsafePathSegmentError):
            write_cluster_dispatch_diagnostic(
                tmp_path, "../escape", _dispatch_artifact()
            )


class TestWriteClusterMarkerDiagnostic:
    def test_writes_json_and_companion_log(self, tmp_path: Path) -> None:
        run_id = "bootstrap-20260607-abc12345"
        stdout_full = "line 1\nline 2\nlast line\n"
        path = write_cluster_marker_diagnostic(
            tmp_path, run_id, _marker_artifact(), stdout_full=stdout_full
        )
        assert path.name == "AIDPF-2049.json"
        log_path = path.parent / "cluster_stdout.log"
        assert log_path.exists()
        assert log_path.read_text(encoding="utf-8") == stdout_full

    def test_rejects_unexpected_stdout_log_path(self, tmp_path: Path) -> None:
        # Programmer-error guard: the artifact + log must be siblings.
        artifact = _marker_artifact(
            clusterMarker={
                "kind": "envelope_missing",
                "stdoutExcerpt": "x",
                "stdoutLogPath": "../escape.log",  # not allowed
            }
        )
        with pytest.raises(ValueError, match="stdoutLogPath"):
            write_cluster_marker_diagnostic(
                tmp_path,
                "run-1",
                artifact,
                stdout_full="anything",
            )

    def test_mutually_exclusive_with_2048(self, tmp_path: Path) -> None:
        # Writer convention: 2048 and 2049 fire from different parts of
        # the dispatch chain and never coexist in the same run dir.
        # This test sanity-checks that you CAN technically write both
        # (the writers don't enforce mutual exclusion), but documents
        # the convention by writing 2048 first and 2049 second to the
        # same dir without collision — proving file-name distinction.
        run_id = "run-both"
        write_cluster_dispatch_diagnostic(tmp_path, run_id, _dispatch_artifact())
        write_cluster_marker_diagnostic(
            tmp_path, run_id, _marker_artifact(), stdout_full=""
        )
        diag_dir = tmp_path / ".aidp" / "diagnostics" / run_id
        assert (diag_dir / "AIDPF-2048.json").exists()
        assert (diag_dir / "AIDPF-2049.json").exists()
        assert (diag_dir / "cluster_stdout.log").exists()
