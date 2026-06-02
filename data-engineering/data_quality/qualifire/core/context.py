"""Jinja2 template environment with AIDP parameter bridge and built-in variables."""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from jinja2 import BaseLoader, StrictUndefined
from jinja2.sandbox import SandboxedEnvironment


def _date_add(value: str, days: int) -> str:
    """Add days to a date string (YYYY-MM-DD)."""
    d = date.fromisoformat(value)
    return (d + timedelta(days=days)).isoformat()


def _date_format(value: str, fmt: str) -> str:
    """Format a date string with strftime."""
    d = date.fromisoformat(value)
    return d.strftime(fmt)


class QualifireContext:
    """Manages Jinja2 rendering with built-in and user-provided variables.

    Built-in variables (always available):
        today, yesterday, now, run_id, ds, ds_nodash

    AIDP parameters (passed via context dict):
        job.*, task.*, hub.*, workspace.*
    """

    def __init__(self, extra_context: dict[str, object] | None = None, run_id: str | None = None):
        self._run_id = run_id or str(uuid.uuid4())
        self._extra = extra_context or {}
        self._env = SandboxedEnvironment(
            loader=BaseLoader(),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )
        self._env.filters["date_add"] = _date_add
        self._env.filters["date_format"] = _date_format
        # Backfill marker (plan S2 / S5). When True, engine row
        # builders stamp ``details_json.backfill = true`` (boolean) on
        # every row this run produces. Default False so normal runs are
        # unaffected.
        self.backfill: bool = False
        # Per-partition cache cells (plan H6 / S8). Populated by
        # ``Qualifire.validate(skip_recollection=True)``'s pre-pass and
        # consumed by collectors that check the cache before executing
        # SQL. Key shape: ``(table_name, metric_name, partition_ts_iso,
        # dimension_value)`` → dict (sufficient to rebuild a
        # ``CollectionResult``). ``None`` (default) means no pre-pass
        # ran — collectors execute as today.
        self.cached_metrics: dict[tuple, dict] | None = None
        # ``skip_recollection`` toggle (skip-recollection feature).
        # When True, the engine's collector dispatch reads persisted
        # collection rows at the dataset's resolved partition_ts;
        # if every expected ``(metric, dim)`` combination has an
        # active non-NULL row, replays them as
        # ``CollectionResult``s with ``metadata["from_cache"]=True``
        # instead of running the collector. Default False so normal
        # runs are unaffected. See
        # ``qualifire/core/engine.py:_try_skip_recollection``.
        self.skip_recollection: bool = False
        # ``skip_renotification`` toggle (skip-renotification feature).
        # When True, the engine's notification dispatch consults the
        # pre-fetched validation history and skips dispatch for any
        # (dataset, validation, metric, dim) key whose prior run
        # already carried the same severity — matching today's
        # `notification_status='sent'` semantics. Default False so
        # retries / replays re-page by default; the operator opts in
        # via Qualifire.run_config(skip_renotification=True) /
        # qualifire run --skip-renotification /
        # qualifire backfill --skip-renotification.
        self.skip_renotification: bool = False
        # ``skip_revalidation`` (skip-revalidation feature). When
        # True, the engine's ``_validate`` calls a pre-pass that
        # reads all persisted validation rows at the dataset's
        # resolved partition_ts; if any are found, replays them
        # as ``ValidationResult``s with ``details["from_cache"] =
        # True`` and skips the validator's compute. Default False
        # so re-validation runs on every invocation; opt in for
        # retries / replays where verdicts are deterministic and
        # already persisted. See
        # ``qualifire/core/engine.py:_try_skip_revalidation``.
        self.skip_revalidation: bool = False
        # webhook-payload-redaction: instance-level redaction lists
        # (``Qualifire(redacted_columns=..., allowlist_columns=...)``).
        # Combined additively with per-dataset
        # ``DatasetConfig.redacted_columns`` /
        # ``DatasetConfig.allowlist_columns`` at validator
        # instantiation time. See ``qualifire/core/_redaction.py``
        # for the resolution rules.
        self.instance_redacted_columns: list[str] | None = None
        self.instance_allowlist_columns: list[str] | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    def _built_in_vars(self) -> dict[str, object]:
        today = date.today()
        return {
            "today": today.isoformat(),
            "yesterday": (today - timedelta(days=1)).isoformat(),
            "now": datetime.now().isoformat(),
            "run_id": self._run_id,
            "ds": today.isoformat(),
            "ds_nodash": today.strftime("%Y%m%d"),
        }

    def get_variables(self) -> dict[str, object]:
        """Return merged variable dict (built-in + extra context)."""
        variables = self._built_in_vars()
        variables.update(self._extra)
        return variables

    def render(self, template_str: str, extra: dict[str, object] | None = None) -> str:
        """Render a Jinja2 template string with all available variables."""
        variables = self.get_variables()
        if extra:
            variables.update(extra)
        template = self._env.from_string(template_str)
        return template.render(**variables)
