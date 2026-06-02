"""Abstract base collector."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from qualifire.backends.base import Backend
from qualifire.core.context import QualifireContext
from qualifire.core.models import CollectionResult


class Collector(ABC):
    """Base class for all data collectors.

    Subclasses implement `collect()` to gather metrics from the backend.
    """

    @abstractmethod
    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        """Collect data and return one or more CollectionResult."""
        ...
