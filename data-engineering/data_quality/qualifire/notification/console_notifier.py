"""Console notifier — prints alert bodies to stdout.

The default channel for development / notebook workflows. Operators
who previously had to register a programmatic notifier (e.g. an
``inbox`` ``CapturingNotifier`` defined in a notebook cell) can drop
``"console"`` into a validation's ``notify:`` list and the engine
will route the formatted message to ``print()`` without any extra
wiring.

Differences from the other built-ins:

* No constructor args — the notifier holds no state, so the YAML
  shape ``notifications: { console: { type: console } }`` works,
  and ``Qualifire`` also auto-registers a default ``console``
  instance so the YAML block is optional.
* No external I/O — failures from ``print()`` (e.g. a closed
  stdout in a daemon) downgrade to ``status="failed"`` rather
  than raising; consistent with the other notifiers' fail-soft
  contract.

Output goes to ``sys.stdout`` and ends with a trailing newline.
The body is the same ``_format_message(...)`` text the email and
slack notifiers send — only the delivery channel differs.
"""

from __future__ import annotations

import logging
import sys

from qualifire.core.models import DatasetResult, NotificationResult, Severity
from qualifire.notification.base import Notifier, register_notifier

logger = logging.getLogger(__name__)


class ConsoleNotifier(Notifier):
    """Print notification bodies to stdout.

    Stateless. Constructed without arguments — both the YAML config
    path (``notifications: { foo: { type: console } }``) and the
    programmatic path (``ConsoleNotifier()``) work.
    """

    def __init__(self) -> None:
        # Intentionally no state. Kept as an explicit ``__init__`` so
        # the docstring + signature are self-documenting.
        pass

    @property
    def channel_name(self) -> str:
        return "console"

    def send(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> NotificationResult:
        text = self._format_message(dataset_result, severity, owner, bu, datasets)
        try:
            # Print to stdout via ``print`` so notebook stdout-capture
            # picks it up; using ``sys.stdout.write`` directly would
            # also work but skips the IPython displayhook hook on
            # some kernel implementations.
            print(text, file=sys.stdout)
            return NotificationResult(
                channel="console",
                severity=severity,
                status="sent",
                message="printed to stdout",
            )
        except Exception as e:
            # Fail-soft: a closed stdout (daemonized run, redirected
            # to /dev/null + sigpipe) shouldn't crash an alert path.
            # Log + return a structured failure so persistence rows
            # carry the diagnostic.
            logger.error("Console notification failed: %s", e)
            return NotificationResult(
                channel="console",
                severity=severity,
                status="failed",
                message=f"{type(e).__name__}: {e}",
            )


register_notifier("console", ConsoleNotifier)
