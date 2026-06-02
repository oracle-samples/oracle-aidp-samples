"""Qualifire - Data quality validation library."""

__version__ = "0.1.0"

from qualifire.api import Qualifire
from qualifire.core.backfill_report import BackfillReport, PartitionDiff
from qualifire.core.exceptions import (
    QualifireConfigError,
    QualifireError,
    QualifireInternalError,
    QualifireNotificationError,
    QualifireReinitWarning,
    QualifireSystemTableError,
    QualifireValidationError,
)
from qualifire.core.models import (
    CollectionResult,
    DatasetResult,
    NotificationResult,
    QualifireResult,
    Severity,
    ValidationResult,
)

__all__ = [
    "Qualifire",
    "QualifireError",
    "QualifireConfigError",
    "QualifireSystemTableError",
    "QualifireValidationError",
    "QualifireInternalError",
    "QualifireNotificationError",
    "QualifireReinitWarning",
    "Severity",
    "CollectionResult",
    "ValidationResult",
    "DatasetResult",
    "NotificationResult",
    "QualifireResult",
    "BackfillReport",
    "PartitionDiff",
]
