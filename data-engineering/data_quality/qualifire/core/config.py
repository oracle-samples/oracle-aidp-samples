"""Pydantic configuration models for YAML/JSON config-driven usage."""

from __future__ import annotations

import logging
import re
import warnings
from pathlib import Path
from typing import Any, ClassVar, Literal, Union

import yaml
from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")
_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_.\-]+$")


def _validate_dimensions(v: list[str] | None) -> list[str] | None:
    """Validate each entry in ``dimensions`` against ``_SAFE_IDENTIFIER_RE``."""
    if v is not None:
        for col in v:
            if not _SAFE_IDENTIFIER_RE.match(col):
                raise ValueError(f"'dimensions' entry must be a valid SQL column identifier, got: {col!r}")
    return v


def _normalize_filter(v: str | None) -> str | None:
    """Coerce empty / whitespace-only filter values to ``None``.

    Static-config counterpart to the runtime whitespace
    normalisation in ``qualifire/collection/_filters.py``. Today
    a ``filter: ""`` or ``filter: "   "`` in YAML would either
    pass through as a truthiness-checked falsy / truthy string
    (depending on whitespace) and cause invalid SQL on
    composition. Coercing to ``None`` at config load means the
    collector — and the engine's ``and_combine_filters`` helper
    — never see an empty or whitespace-only string.

    Two layers exist by design: this one catches static YAML
    (``filter: ""``); the runtime ``_normalise`` inside
    ``_filters.py`` catches the dynamic case where a Jinja
    template resolves to empty / whitespace at render time.
    Both share the same ``not v.strip() → None`` rule so the
    contract is identical at both layers; sharing a single
    helper across the layers would create an awkward import
    direction (config → collection), so the duplication is
    deliberate.
    """
    if v is None:
        return None
    if not v.strip():
        return None
    return v


def _validate_step(v: str) -> str:
    """Validate a ``step`` duration at config-load time.

    The step fields on ``HistoricalCompareConfig``,
    ``ForecastModelConfig``, and ``SampleHistoryConfig`` are required
    (non-nullable). This validator runs at field-validation time so an
    invalid ISO duration (``"PnonsenseD"``, the rejected legacy
    compact form) surfaces in ``validate-config`` rather than as a
    mid-run ``parse_step_seconds()`` failure during a real pipeline
    run. Reuses the canonical parser so the grammar lives in one
    place.
    """
    # Import inside the function to avoid a circular import between
    # ``core.config`` and ``core.duration`` at module load time.
    from qualifire.core.duration import parse_step_seconds

    parse_step_seconds(v)
    return v


# --- Notification channel configs ---


class EmailNotificationConfig(BaseModel):
    type: Literal["email"] = "email"
    smtp_host: str
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_password: str | None = None
    use_tls: bool = True
    sender: str | None = None
    recipients: list[str]


class SlackNotificationConfig(BaseModel):
    type: Literal["slack"] = "slack"
    webhook_url: str


class WebhookNotificationConfig(BaseModel):
    type: Literal["webhook"] = "webhook"
    url: str
    method: Literal["POST", "PUT"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)


class ConsoleNotificationConfig(BaseModel):
    """Stdout console channel — no parameters required.

    Operators can declare a YAML block as an opt-in
    (``notifications: { console: { type: console } }``), but the
    typical usage is to skip the YAML entirely and let
    ``Qualifire.__init__`` auto-register a default ``console``
    notifier under the channel name ``"console"``. Validations'
    ``notify:`` lists can then reference ``"console"`` directly
    without a top-level config block — useful for notebook /
    development workflows that previously had to register a
    programmatic ``inbox`` notifier in a setup cell.
    """

    type: Literal["console"] = "console"


NotificationChannelConfig = Union[
    EmailNotificationConfig,
    SlackNotificationConfig,
    WebhookNotificationConfig,
    ConsoleNotificationConfig,
]


# --- JDBC connection config ---


class JDBCConfig(BaseModel):
    """Connection settings for the ``jdbc`` system-table backend.

    Only referenced when ``system_table_backend == 'jdbc'``. Passwords
    and other secrets should be supplied either via the
    :class:`~qualifire.core.secrets.SecretResolver` injected at
    ``Qualifire(secret_resolver=...)`` (using ``secret://name/key``
    refs in YAML or the ``from_secret:`` directive on this model)
    or via the Python API (constructing ``JDBCConfig`` with resolved
    values directly). Plain ``${ENV_VAR}`` interpolation is **not**
    supported by Qualifire — `load_config()` does not expand env
    references; an inline ``${SMTP_PASSWORD}`` would be sent
    verbatim.

    ``properties`` is passed through to Spark's JDBC writer as-is, so
    any additional driver-specific options (fetchsize, batchsize,
    sessionInitStatement, ...) belong there. Values inside
    ``properties`` may also use ``secret://...`` references.

    ``url`` is required when this config is actually used (backend is
    ``jdbc``), but is declared ``Optional`` here so a templated
    ``jdbc:`` block (user/password only, ``url`` filled in per
    deployment) can live in a config whose active backend is
    *not* ``jdbc`` without tripping Pydantic at load time. The
    top-level ``QualifireConfig._require_jdbc_when_needed`` validator
    is what actually enforces ``url`` for the ``jdbc`` backend, and
    accepts ``from_secret`` as a substitute for ``url`` because
    secret resolution happens after Pydantic validation.

    ``from_secret`` (optional) takes a plain secret name. When set,
    the resolver is called as ``resolver.get(name)`` and the returned
    ``dict[str, str]`` populates ``url`` / ``user`` / ``password`` /
    ``driver``. ``from_secret`` is mutually exclusive with literal
    values on those four fields and with per-field ``secret://``
    references on the same model.
    """

    url: str | None = None
    user: str | None = None
    password: str | None = None
    driver: str | None = None
    properties: dict[str, str] = Field(default_factory=dict)
    from_secret: str | None = None


# --- Collection configs ---


class RecencyCollectionConfig(BaseModel):
    strategy: Literal["max_column", "delta_log", "metadata", "custom_sql"]
    column: str | None = None
    sql: str | None = None

    @model_validator(mode="after")
    def _validate_strategy_fields(self) -> RecencyCollectionConfig:
        if self.strategy == "max_column" and not self.column:
            raise ValueError("'column' is required for max_column strategy")
        if self.strategy == "custom_sql" and not self.sql:
            raise ValueError("'sql' is required for custom_sql strategy")
        return self


class AggregationCollectionConfig(BaseModel):
    """Aggregation collector — produces metric values via SQL
    aggregate expressions.

    The most common collector. Use it for ``COUNT(*)``,
    ``AVG(col)``, ``SUM(col)``, ``MAX/MIN``, etc.; supports
    optional ``filter`` and ``dimensions`` (one row per dim
    value, dim values become the
    ``CollectionResult.dimension_value`` and end up on the
    system-table ``dimension_value`` column).

    The smallest legal config:

    .. code-block:: yaml

        type: aggregation
        expressions:
          row_count: 'COUNT(*)'
          avg_amount: 'AVG(amount)'

    ``expressions`` is a dict of ``{metric_name: sql_expression}``
    (sub-feature A; renamed from the ``list[str]`` form ``["AGG(col)
    AS name"]`` that required a fragile ``AS <name>`` lexer). The
    dict form makes metric names addressable as dict keys for the
    backfill loop's per-metric source-select; YAML and Python share
    one shape.

    ``filter`` (optional) is **AND-combined** with the dataset-level
    ``DatasetConfig.filter`` at the engine boundary, post-render —
    both predicates apply. Empty / whitespace-only values coerce to
    ``None`` at config load. See
    `docs/validators/validator_collector_matrix.md#filter-precedence`
    for the composition contract.
    """

    type: Literal["aggregation"] = "aggregation"
    expressions: dict[str, str]
    filter: str | None = None
    dimensions: list[str] | None = None

    @field_validator("expressions", mode="before")
    @classmethod
    def _validate_expressions(cls, v: Any) -> dict[str, str]:
        if not isinstance(v, dict):
            raise ValueError(
                "AggregationCollectionConfig.expressions must be a "
                "dict[str, str]"
            )
        if not v:
            raise ValueError(
                "AggregationCollectionConfig.expressions must not be empty"
            )
        return v

    _check_dims = field_validator("dimensions")(staticmethod(_validate_dimensions))
    _check_filter = field_validator("filter")(staticmethod(_normalize_filter))


class ColumnProfileOverride(BaseModel):
    """Per-column profiling overrides."""

    stats: list[str] | None = None  # include only these stats (overrides global)
    exclude_stats: list[str] | None = None  # exclude these stats


class ProfilingCollectionConfig(BaseModel):
    """Profiling collector config — single-pass per-column stats.

    ``filter`` (optional) is **AND-combined** with the dataset-level
    ``DatasetConfig.filter`` at the engine boundary, post-render —
    both predicates apply. Empty / whitespace-only values coerce to
    ``None`` at config load. See
    `docs/validators/validator_collector_matrix.md#filter-precedence`
    for the composition contract.
    """

    type: Literal["profiling"] = "profiling"
    columns: list[str] | None = None
    stats: list[str] | None = None  # global include filter (None = all)
    exclude_stats: list[str] | None = None  # global exclude filter
    filter: str | None = None
    top_k: int = 10
    quantiles: list[float] | None = None
    column_profiles: dict[str, ColumnProfileOverride] | None = None  # per-column overrides
    dimensions: list[str] | None = None

    _check_dims = field_validator("dimensions")(staticmethod(_validate_dimensions))
    _check_filter = field_validator("filter")(staticmethod(_normalize_filter))


class MetricsCollectionConfig(BaseModel):
    """Metrics collector config — named SQL aggregation expressions.

    ``filter`` (optional) is **AND-combined** with the dataset-level
    ``DatasetConfig.filter`` at the engine boundary, post-render —
    both predicates apply. Empty / whitespace-only values coerce to
    ``None`` at config load. See
    `docs/validators/validator_collector_matrix.md#filter-precedence`
    for the composition contract.
    """

    type: Literal["metrics"] = "metrics"
    metrics: dict[str, str]  # name -> SQL expression
    filter: str | None = None
    dimensions: list[str] | None = None

    _check_dims = field_validator("dimensions")(staticmethod(_validate_dimensions))
    _check_filter = field_validator("filter")(staticmethod(_normalize_filter))


class SampleCollectionConfig(BaseModel):
    """Sample collector — pulls per-row samples for SHAP-based
    pattern / shape (Isolation Forest) validators.

    Used by ``PatternValidationConfig`` and
    ``AnomalyDetectionValidationConfig`` to feed the
    classifier / IF model. Pulls ``n_records`` rows for the
    current slice and ``n_records`` rows from each of
    ``history.past_dates`` past slices (``past_dates`` controls
    how many slices, not the per-slice row count). The
    validator's compute consumes both current + past
    DataFrames. Sample-based validators fall through the
    ``skip_recollection`` pre-pass via the
    ``hasattr(val_config, "expected_metrics")`` guard — they
    don't expose that resolver, so the pre-pass returns None
    and re-collection runs.

    The smallest legal config (when ``slice_column`` is set):

    .. code-block:: yaml

        type: sample
        n_records: 1000
        slice_column: event_dt
        slice_value: '{{ ds }}'
        history:
          past_dates: 3
          step: 'P7D'

    Slicing model:

    * **``slice_column`` + ``slice_value``**: name the partition
      column and provide the value at the current slice. The sampler
      auto-generates ``<slice_column> = '<rendered slice_value>'`` for
      the current slice and ``<slice_column> = '<slice_value − i·step>'``
      for each past slice. ``slice_value`` is Jinja-rendered, so
      ``slice_value: "{{ ds }}"`` plugs in Airflow's logical date.

    Both fields go together (set both, or set neither). The pair may
    or may not equal the dataset's ``partition_ts`` — the sampler
    doesn't assume they match.

    Dataset-level filters (``DatasetConfig.filter``, e.g.
    ``region = 'us'``) AND-combine unchanged with the per-slice
    predicate before each ``backend.sample_records`` call.

    Escape hatch: ``history.filters`` lets callers supply explicit
    per-slice WHERE clauses verbatim. Use this when the partition
    isn't date-shaped (e.g. version IDs).
    """

    type: Literal["sample"] = "sample"
    n_records: int = 10000
    slice_column: str | None = None
    slice_value: str | None = None
    history: SampleHistoryConfig | None = None

    @field_validator("slice_column")
    @classmethod
    def _validate_slice_column_identifier(cls, v: str | None) -> str | None:
        """Defensive identifier check. ``slice_column`` is interpolated
        into the per-slice WHERE clause via f-string, so a malformed
        value in YAML (typo, accidental SQL fragment, copy-paste from
        a filter expression) would surface as a backend SQL error far
        from the YAML line that caused it.

        Accept the standard SQL identifier shape with optional
        dotted qualification for ``schema.column`` patterns:
        ``[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*``.
        Operators with non-standard column names (spaces, hyphens,
        reserved-keyword names) should switch to ``history.filters``
        with hand-written predicates and dialect-correct quoting.
        """
        if v is None:
            return v
        import re as _re
        if not _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", v):
            raise ValueError(
                f"slice_column={v!r} is not a valid SQL identifier. "
                "Expected a column name like 'event_dt' or "
                "'schema.event_dt'. For non-standard column names "
                "use history.filters with explicit per-slice predicates."
            )
        return v

    @model_validator(mode="after")
    def _slice_column_with_slice_value(self) -> SampleCollectionConfig:
        if (self.slice_column is None) != (self.slice_value is None):
            raise ValueError(
                "SampleCollectionConfig: slice_column and slice_value "
                "must be set together (or both omitted). Pass both for "
                "the common date-partition case, or use history.filters "
                "with explicit per-slice predicates."
            )
        return self


class SampleHistoryConfig(BaseModel):
    past_dates: int = 3
    # ``step`` is the partition cadence anchoring shape / pattern
    # past slices at ``cr.partition_ts − k·step``. Required and
    # non-nullable: callers cannot omit it, and ``null`` in YAML is
    # rejected at config-load time. (When ``filters`` is supplied
    # the sampler bypasses the anchor math entirely, but ``step``
    # is still required so the rule is reproducible — pass any
    # ISO duration; it isn't consulted.)
    step: str
    filters: list[str] | None = None

    _check_step = field_validator("step")(staticmethod(_validate_step))


class CustomQueryCollectionConfig(BaseModel):
    type: Literal["custom_query"] = "custom_query"
    sql: str
    dimensions: list[str] | None = None

    _check_dims = field_validator("dimensions")(staticmethod(_validate_dimensions))


CollectionConfig = Union[
    AggregationCollectionConfig,
    ProfilingCollectionConfig,
    MetricsCollectionConfig,
    SampleCollectionConfig,
    CustomQueryCollectionConfig,
]


# --- Notification routing ---


class NotifyConfig(BaseModel):
    on_success: list[str] = Field(default_factory=list)
    warning: list[str] = Field(default_factory=list)
    error: list[str] = Field(default_factory=list)


# --- Threshold rule configs ---


class ThresholdBounds(BaseModel):
    min: float | None = None
    max: float | None = None
    eq: float | None = None
    neq: float | None = None
    gt: float | None = None
    lt: float | None = None
    gte: float | None = None
    lte: float | None = None


class ThresholdLevels(BaseModel):
    warning: ThresholdBounds | None = None
    error: ThresholdBounds | None = None


class ThresholdRuleConfig(BaseModel):
    metric: str
    thresholds: ThresholdLevels


# --- Historical compare config ---


class HistoricalCompareConfig(BaseModel):
    past_values: int = 3
    # ``step`` is the partition cadence for drift's
    # partition-anchored history reads (anchor − k·step). Required
    # and non-nullable: every YAML and every programmatic config
    # must supply an ISO 8601 duration. ``null`` is rejected at
    # config-load time so the no-cadence ambiguity simply can't
    # exist in shipped configs.
    step: str
    missing_strategy: Literal["ignore", "substitute", "error"] = "ignore"
    on_missing_history: Literal["ignore", "warn", "error"] = "ignore"

    _check_step = field_validator("step")(staticmethod(_validate_step))


class HistoricalThresholds(BaseModel):
    """Per-severity bounds for HistoricalValidator measures.

    One accepted value form per measure — a dict with optional
    ``min`` and ``max`` keys:
    ``{"rate_of_change_pct": {"min": -10, "max": 25}}`` fails when
    the (signed) measure value falls below ``min`` or above
    ``max``. Either bound is optional, e.g. ``{"min": -10}`` only
    fires on drops.

    Supported measure names: ``deviation_pct``, ``deviation_abs``,
    ``z_score``, ``rate_of_change_pct``, ``rate_of_change_abs``.
    """

    warning: dict[str, dict[str, float]] | None = None
    error: dict[str, dict[str, float]] | None = None


class HistoricalRuleConfig(BaseModel):
    metric: str
    # ``compare`` is required because ``HistoricalCompareConfig.step``
    # is required — there is no all-defaults default. Pydantic
    # rejects a YAML rule that omits ``compare:`` with a clear
    # missing-fields error at config-load time.
    compare: HistoricalCompareConfig
    thresholds: HistoricalThresholds


# --- Forecast model config ---


class ForecastIntervalWidth(BaseModel):
    warning: float = 0.80
    error: float = 0.95


class ForecastModelConfig(BaseModel):
    history_count: int = 90
    # Required and non-nullable — same rationale as
    # ``HistoricalCompareConfig.step``. Forecast's
    # partition-anchored history read uses ``cr.partition_ts −
    # k·step`` and Prophet's ``ds`` axis is ``partition_ts``.
    step: str
    changepoint_prior_scale: float = 0.05
    seasonality_prior_scale: float = 10.0
    seasonality_mode: Literal["additive", "multiplicative"] = "additive"
    interval_width: ForecastIntervalWidth = Field(default_factory=ForecastIntervalWidth)
    on_missing_history: Literal["ignore", "warn", "error"] = "ignore"

    _check_step = field_validator("step")(staticmethod(_validate_step))


class ForecastRuleConfig(BaseModel):
    metric: str
    # ``model`` is required because ``ForecastModelConfig.step`` is
    # required — there is no all-defaults default.
    model: ForecastModelConfig


# --- Anomaly detection config ---


class AnomalyModelConfig(BaseModel):
    n_estimators: int = 100
    contamination: str | float = "auto"
    explain: bool = True
    explain_value_drift: bool = True
    # drift-explainer-per-slice-breakdown: when True, the first 3
    # ``value_drift_explainer`` entries (with kind in {numeric,
    # onehot, boolean}) carry a ``per_slice`` block with one entry
    # per past slice. Default False keeps the persisted payload
    # tight; opt in for triage-time slice-level trend.
    drift_breakdown_by_slice: bool = False
    drop_complex: bool = False
    alert_on_schema_change: bool = False
    on_missing_history: Literal["ignore", "warn", "error"] = "ignore"
    # Columns to drop from the feature matrix before fitting the
    # Isolation Forest. The sampler's ``slice_column`` is added
    # automatically by the engine — operators only need to list
    # per-row identifiers (e.g. ``sale_id``), ingestion timestamps
    # (``updated_at``), and other leakage-prone columns the IF
    # would split on trivially.
    exclude_columns: list[str] = Field(default_factory=list)


class AnomalyThresholdConfig(BaseModel):
    warning: dict[str, float] | None = Field(default=None)  # {"anomaly_score": 0.6}
    error: dict[str, float] | None = Field(default=None)


# --- Validation configs (discriminated by type) ---


class SLOValidationConfig(BaseModel):
    """An SLO (data-freshness) validation: assert
    ``now - recency < threshold``.

    Use when the question is "is the table fresh enough?"
    ``recency`` is computed by the configured strategy
    (max-column timestamp, delta-log read, file-system
    metadata, or custom SQL — see ``recency.strategy``); the
    validator computes ``delta_seconds = now - recency``
    against the configured ``warning`` / ``error`` ISO-8601
    duration thresholds.

    The smallest legal config:

    .. code-block:: yaml

        type: slo
        recency:
          strategy: max_column
          column: updated_at
        thresholds:
          warning: 'PT4H'
          error: 'PT8H'

    The ``recency.strategy`` field accepts ``max_column``
    (column holding the freshness clock), ``custom_sql`` (any
    SQL returning a single TIMESTAMP), ``delta_log`` (Delta
    transaction log read), or ``metadata`` (file-system /
    catalog metadata). See ``RecencyCollectionConfig``.

    Note: SLO is intentionally OUT of scope for the
    ``skip_recollection`` runtime flag — replaying a cached
    ``recency`` row defeats the freshness contract. See the
    ``slo-recency-skip-recollection`` backlog item if your
    workflow needs the carve-out.
    """
    type: Literal["slo"] = "slo"
    name: str | None = None
    # Free-text description surfaced on every persisted row produced by this
    # validation. Optional. Stamped on the system table's `validation_description`
    # column so dashboards can show intent without consulting the YAML.
    description: str | None = None
    recency: RecencyCollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(f"'name' must contain only alphanumeric characters, underscores, dots, or hyphens, got: {v!r}")
        return v
    thresholds: dict[str, str]  # {"warning": "PT4H", "error": "PT8H"}
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)


class ThresholdValidationConfig(BaseModel):
    """A static threshold validation: aggregated metric values
    must satisfy operator-set bounds.

    Use when the question is "is ``row_count >= 100``?" or "is
    ``avg(amount)`` between 50 and 200?". Bounds are static —
    same on every run; for a moving / partition-anchored bound
    see ``HistoricalValidationConfig`` (drift) or
    ``ForecastValidationConfig`` (trend).

    The smallest legal config:

    .. code-block:: yaml

        type: threshold
        collection:
          type: aggregation
          expressions: {row_count: 'COUNT(*)'}
        rules:
          - metric: row_count
            thresholds:
              error: {min: 1}

    Threshold bounds use the
    ``{min, max, eq, neq, gt, lt, gte, lte}`` operator shape on
    ``ThresholdBounds``. See ``ThresholdLevels`` for the full
    vocabulary.

    See ``docs/configuration.md`` (once
    ``configuration-reference-audit`` ships) for examples.
    """
    type: Literal["threshold"] = "threshold"
    name: str | None = None
    description: str | None = None
    collection: CollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(f"'name' must contain only alphanumeric characters, underscores, dots, or hyphens, got: {v!r}")
        return v
    rules: list[ThresholdRuleConfig]
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)

    def expected_metrics(self) -> list[str]:
        """Return the metric names this validator consumes per partition.

        Used by the engine's ``skip_recollection`` pre-pass to
        look up cached metric values keyed by ``(metric_name,
        dimension_value, partition_ts)``.

        Threshold validators consume one rule per metric; the
        rule's ``metric`` field names what the validator will
        read.
        """
        return [rule.metric for rule in self.rules]


class HistoricalValidationConfig(BaseModel):
    """A historical-comparison (drift) validation: each metric
    is compared against the same metric's recent history.

    Use when the question is "is today's value too far from
    the past N days?" The validator emits signed
    ``deviation_pct`` and ``z_score`` per metric so the alert
    distinguishes a 50% drop from a 50% spike. Pairs with
    ``HistoricalCompareConfig`` (``past_values``, ``step``,
    ``missing_strategy``) per rule.

    The smallest legal config:

    .. code-block:: yaml

        type: drift
        collection:
          type: aggregation
          expressions: {avg_sales: 'AVG(amount)'}
        rules:
          - metric: avg_sales
            compare: {past_values: 7, step: 'P1D'}
            thresholds:
              warning: {deviation_pct: 20}
              error: {deviation_pct: 50}

    Threshold bounds on a drift rule use a different shape
    than ``ThresholdValidationConfig``: each level is a
    ``dict[str, float | dict[str, float]]`` keyed on a measure
    (``deviation_pct``, ``z_score``, ``rate_of_change_pct``,
    ``deviation_abs``, ``rate_of_change_abs``). The value is
    either a bare number (symmetric absolute) or a
    ``{min, max}`` dict (signed bounds). The
    ``rate_of_change_pct`` measure compares against the
    immediate-prior partition (signed).

    Two distinct sparse-history knobs, both per-rule on
    ``compare``:

    - ``compare.missing_strategy`` controls behaviour when SOME
      of the requested past partitions are absent (e.g.
      ``ignore`` skips them; ``error`` aborts).
    - ``compare.on_missing_history`` controls behaviour when NO
      history exists at all (cold start).

    See ``docs/configuration.md`` (once
    ``configuration-reference-audit`` ships) for examples.
    """
    type: Literal["drift"] = "drift"
    name: str | None = None
    description: str | None = None
    collection: CollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(f"'name' must contain only alphanumeric characters, underscores, dots, or hyphens, got: {v!r}")
        return v
    rules: list[HistoricalRuleConfig]
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)

    def expected_metrics(self) -> list[str]:
        """Metric names this drift validator consumes per partition.

        See :meth:`ThresholdValidationConfig.expected_metrics`.
        Drift validators read the current-partition value (collection)
        and additionally fetch history; only the current value is
        cached by ``skip_recollection``.
        """
        return [rule.metric for rule in self.rules]


class ForecastValidationConfig(BaseModel):
    """A forecast (trend) validation: each metric is compared
    against a Prophet-fitted forecast band.

    Use when the question is "is today's value within the
    confidence interval of the trend Prophet learned from the
    last N partitions?" The validator persists Prophet model
    config (``changepoint_prior_scale``, ``seasonality_mode``)
    in ``details_json`` so dashboards can render the band.
    Requires the optional ``[forecast]`` extra (Prophet).

    The smallest legal config:

    .. code-block:: yaml

        type: trend
        collection:
          type: aggregation
          expressions: {row_count: 'COUNT(*)'}
        rules:
          - metric: row_count
            model: {history_count: 90, step: 'P1D'}

    See ``docs/configuration.md`` (once
    ``configuration-reference-audit`` ships) for examples.
    """
    type: Literal["trend"] = "trend"
    name: str | None = None
    description: str | None = None
    collection: CollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(f"'name' must contain only alphanumeric characters, underscores, dots, or hyphens, got: {v!r}")
        return v
    rules: list[ForecastRuleConfig]
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)

    def expected_metrics(self) -> list[str]:
        """Metric names this forecast validator consumes per partition.

        See :meth:`ThresholdValidationConfig.expected_metrics`.
        Forecast validators read the current-partition value
        (collection) and additionally evaluate the model; only the
        current value is cached by ``skip_recollection``.
        """
        return [rule.metric for rule in self.rules]


class AnomalyDetectionValidationConfig(BaseModel):
    """A shape (Isolation Forest) anomaly-detection validation:
    flags rows whose feature shape doesn't match the historical
    distribution.

    Use when "the metric we'd compute didn't break, but the
    underlying rows look weird" is the question. Concatenates
    current + past samples, encodes the combined matrix, fits
    an Isolation Forest on all of it, then scores the current
    rows; emits ``anomaly_ratio`` (fraction of current rows
    flagged anomalous) plus SHAP-explained
    ``top_contributing_features`` when ``model.explain=True``
    (requires the ``[anomaly]`` extra).

    The smallest legal config:

    .. code-block:: yaml

        type: shape
        collection:
          type: sample
          n_records: 1000
          slice_column: event_dt
          slice_value: '{{ ds }}'
          history:
            past_dates: 3
            step: 'P7D'

    Pairs with ``SampleCollectionConfig`` for the
    current/past-sample pull. ``model.exclude_columns`` is the
    load-bearing leakage-control knob — list any per-row IDs
    or ingestion-timestamps the IF would split on trivially.

    Pattern's ``redacted_columns`` policy applies — see
    ``docs/CHANGELOG.md`` § webhook-payload-redaction.
    """
    type: Literal["shape"] = "shape"
    name: str | None = None
    description: str | None = None
    collection: SampleCollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(f"'name' must contain only alphanumeric characters, underscores, dots, or hyphens, got: {v!r}")
        return v
    model: AnomalyModelConfig = Field(default_factory=AnomalyModelConfig)
    thresholds: AnomalyThresholdConfig = Field(default_factory=AnomalyThresholdConfig)
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)


# --- Pattern (Random Forest two-sample) config ---


class PatternModelConfig(BaseModel):
    n_estimators: int = 200
    max_depth: int | None = 8
    class_weight: Literal["balanced", "balanced_subsample"] | None = "balanced"
    random_state: int = 42
    cv_folds: int = 5
    explain: bool = True
    explain_value_drift: bool = True
    # drift-explainer-per-slice-breakdown: see AnomalyModelConfig
    # for the same flag. Mirror semantics across both validators
    # since they share `_drift_explainer.py`.
    drift_breakdown_by_slice: bool = False
    drop_complex: bool = False
    alert_on_schema_change: bool = False
    on_missing_history: Literal["ignore", "warn", "error"] = "ignore"
    # Columns the classifier MUST ignore because they define the
    # current-vs-past split (partition / date / ingestion-timestamp /
    # surrogate-ID). Without this, AUC trivially reaches 1.0 even
    # when the business distribution is unchanged — the docs call
    # this out as the load-bearing leakage-control knob.
    exclude_columns: list[str] = Field(default_factory=list)

    @field_validator("cv_folds")
    @classmethod
    def _validate_cv_folds(cls, v: int) -> int:
        if v < 2 or v > 10:
            raise ValueError(
                f"cv_folds must be in [2, 10]; got {v}. "
                "Below 2 is not cross-validation (single split); "
                "above 10 produces per-fold samples too small for "
                "a stable AUC estimate at default sample sizes."
            )
        return v

    @field_validator("n_estimators")
    @classmethod
    def _validate_n_estimators(cls, v: int) -> int:
        # Catch zero/negative values at preflight. Without this,
        # `validate-config` happily accepts n_estimators=0 and the error
        # only surfaces later inside cross_val_score as a runtime ERROR.
        if v < 1:
            raise ValueError(f"n_estimators must be >= 1; got {v}")
        return v

    @field_validator("max_depth")
    @classmethod
    def _validate_max_depth(cls, v: int | None) -> int | None:
        # None is fine (unlimited depth). Zero/negative is not.
        if v is not None and v < 1:
            raise ValueError(
                f"max_depth must be None or >= 1; got {v}"
            )
        return v

    @field_validator("exclude_columns")
    @classmethod
    def _validate_exclude_columns(cls, v: list[str]) -> list[str]:
        for col in v:
            if not _SAFE_IDENTIFIER_RE.match(col):
                raise ValueError(
                    f"exclude_columns entry must be a valid identifier, got: {col!r}"
                )
        return v


class PatternThresholdConfig(BaseModel):
    warning: dict[str, float] | None = None
    error: dict[str, float] | None = None

    _ALLOWED_KEYS: ClassVar[set[str]] = {"auc"}

    @field_validator("warning", "error")
    @classmethod
    def _validate_threshold_block(
        cls, v: dict[str, float] | None
    ) -> dict[str, float] | None:
        if v is None:
            return v
        bad_keys = set(v.keys()) - cls._ALLOWED_KEYS
        if bad_keys:
            raise ValueError(
                f"Unknown pattern threshold key(s) {sorted(bad_keys)}. "
                f"Allowed: {sorted(cls._ALLOWED_KEYS)}."
            )
        auc = v.get("auc")
        if auc is not None and not (0.5 <= auc <= 1.0):
            raise ValueError(
                f"auc threshold must be in [0.5, 1.0]; got {auc}. "
                "Below 0.5 means the classifier is worse than random; "
                "above 1.0 is mathematically impossible."
            )
        return v

    @model_validator(mode="after")
    def _warning_le_error(self) -> PatternThresholdConfig:
        w = (self.warning or {}).get("auc")
        e = (self.error or {}).get("auc")
        if w is not None and e is not None and w > e:
            raise ValueError(
                f"warning.auc ({w}) must be <= error.auc ({e}); "
                "an AUC that triggers ERROR should also trigger WARNING."
            )
        return self


class PatternValidationConfig(BaseModel):
    """A pattern (Random Forest two-sample) validation: trains
    a classifier to distinguish current from past samples; high
    AUC means the distribution shifted.

    Use when "the metric we'd compute is fine, but the joint
    feature distribution is different" is the question. AUC
    near 0.5 = current and past indistinguishable (good); AUC
    near 1.0 = clearly separable (drift). Emits
    ``top_contributing_features`` (SHAP), and
    ``value_drift_explainer`` per-feature; opt-in
    ``drift_breakdown_by_slice`` adds per-past-slice deltas
    for triage.

    The smallest legal config:

    .. code-block:: yaml

        type: pattern
        collection:
          type: sample
          n_records: 1000
          slice_column: event_dt
          slice_value: '{{ ds }}'
          history:
            past_dates: 3
            step: 'P7D'
        thresholds:
          warning: {auc: 0.65}
          error: {auc: 0.80}

    ``model.exclude_columns`` is THE load-bearing leakage-control
    knob: a partition / date / ID column will produce trivial
    AUC=1.0 even when the business distribution is unchanged.
    Document in ``docs/pattern_check.md`` Leakage Control. The
    ``redacted_columns`` policy on ``DatasetConfig`` applies.

    Requires the ``[anomaly]`` extra (sklearn + SHAP).
    """
    type: Literal["pattern"] = "pattern"
    name: str | None = None
    description: str | None = None
    collection: SampleCollectionConfig

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is not None and not _SAFE_NAME_RE.match(v):
            raise ValueError(
                f"'name' must contain only alphanumeric characters, "
                f"underscores, dots, or hyphens, got: {v!r}"
            )
        return v

    model: PatternModelConfig = Field(default_factory=PatternModelConfig)
    thresholds: PatternThresholdConfig = Field(default_factory=PatternThresholdConfig)
    on_empty_data: Literal["pass", "warning", "error"] = "warning"
    notify: NotifyConfig = Field(default_factory=NotifyConfig)

    @model_validator(mode="after")
    def _validate_history_filters_length(self) -> PatternValidationConfig:
        """If both ``past_dates`` and ``filters`` are set, their
        lengths must match. ``SamplerCollector`` silently prefers
        ``filters`` when present, so a mismatch (e.g. ``past_dates=3``
        + 5 filters) changes class balance and CV stability without
        the operator noticing. This validator lives on the pattern
        config — not ``SampleHistoryConfig`` — to avoid changing
        ``shape``'s validation semantics (scope gate 2 in plan.md).
        """
        history = self.collection.history
        if history is None or history.filters is None:
            return self
        fields_set = history.model_fields_set
        if "past_dates" in fields_set and len(history.filters) != history.past_dates:
            raise ValueError(
                f"pattern: history.past_dates ({history.past_dates}) and "
                f"history.filters length ({len(history.filters)}) must match. "
                "When both are specified, they describe the same set of past "
                "slices; a mismatch silently trains on the filters and "
                "changes class balance without an operator signal."
            )
        return self


ValidationConfig = Union[
    SLOValidationConfig,
    ThresholdValidationConfig,
    HistoricalValidationConfig,
    ForecastValidationConfig,
    AnomalyDetectionValidationConfig,
    PatternValidationConfig,
]


# --- WAP config ---


class WAPConfig(BaseModel):
    """Write-Audit-Publish configuration.

    Three source-of-data forms are pairwise mutually exclusive
    (sub-feature B / N2): ``df``, ``sql``, ``sql_file``. Setting any
    two raises a ``ValidationError`` at construction AND at
    assignment (``validate_assignment=True``) so callers cannot
    bypass the rule by mutating after the fact.

    ``sql_file`` is read once at config-load time, contents cached
    on a ``PrivateAttr`` (``_resolved_sql``); read failures map to
    :class:`QualifireConfigError`. Use :attr:`effective_sql` to get
    "the SQL that will be used" (resolves ``sql_file`` if set, else
    returns ``sql``).

    Renamed from ``write_sql`` (no shim — foundation pin #5: clean
    breaks over compat aliases).
    """

    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
    }

    sql: str | None = None  # SELECT query only — staging table is managed internally
    sql_file: str | None = None  # path to a .sql file; resolved at config-load time
    _resolved_sql: str | None = PrivateAttr(default=None)
    target_table: str
    write_options: dict[str, Any] = Field(default_factory=dict)
    # Runtime-only DataFrame handle for DataFrame-driven WAP. Excluded
    # from serialization (Spark/pandas DataFrames don't round-trip).
    df: Any = Field(default=None, exclude=True, repr=False)
    # Bare column name on ``target_table`` used to filter rows during
    # backfill (``Qualifire.backfill``). Required for target-mode
    # backfill on a WAP dataset; ignored during forward execution.
    # Auto-detection from ``DatasetConfig.partition_ts`` is deferred
    # to a later phase — always explicit in v1.
    partition_column: str | None = None
    # When True (default), publish creates the target table on first
    # run if it doesn't already exist. Operators in catalog/RBAC-
    # controlled environments set False to restore the strict
    # "target must pre-exist" contract.
    allow_create: bool = True

    @field_validator("partition_column")
    @classmethod
    def _validate_partition_column(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not _SAFE_NAME_RE.match(v):
            raise ValueError(
                f"WAPConfig.partition_column must contain only alphanumeric "
                f"characters, underscores, dots, or hyphens, got: {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _validate_source_mutually_exclusive(self) -> WAPConfig:
        """``df``, ``sql``, ``sql_file`` are pairwise mutually exclusive."""
        sources_set = [
            name for name, val in (
                ("df", self.df),
                ("sql", self.sql),
                ("sql_file", self.sql_file),
            ) if val is not None
        ]
        if len(sources_set) > 1:
            raise ValueError(
                f"WAPConfig source fields are mutually exclusive — set "
                f"exactly one of df, sql, sql_file (got {sources_set!r})."
            )
        return self

    @model_validator(mode="after")
    def _resolve_sql_file(self) -> WAPConfig:
        """Read ``sql_file`` from disk at config-load time, populate
        the ``_resolved_sql`` PrivateAttr. Errors map to
        ``QualifireConfigError`` so operators see config issues in
        the right exception bucket.

        Resolution policy:
        - Absolute paths used as-is.
        - Relative paths resolve against ``QUALIFIRE_CONFIG_BASE_DIR``
          env var if set, else CWD.
        - UTF-8 strict.
        """
        if self.sql_file is None:
            return self
        from os import environ
        from pathlib import Path
        from qualifire.core.exceptions import QualifireConfigError

        path = Path(self.sql_file)
        if not path.is_absolute():
            base = environ.get("QUALIFIRE_CONFIG_BASE_DIR")
            if base:
                path = Path(base) / path
        try:
            self._resolved_sql = path.read_text(encoding="utf-8")
        except FileNotFoundError as e:
            raise QualifireConfigError(
                f"WAPConfig.sql_file not found: {path} "
                f"(resolved from {self.sql_file!r})"
            ) from e
        except UnicodeDecodeError as e:
            raise QualifireConfigError(
                f"WAPConfig.sql_file is not valid UTF-8: {path}"
            ) from e
        except OSError as e:
            raise QualifireConfigError(
                f"WAPConfig.sql_file unreadable: {path} ({e})"
            ) from e
        return self

    @property
    def effective_sql(self) -> str | None:
        """The SQL that will actually be used at run time.

        Returns the resolved ``sql_file`` contents when set, else
        ``sql``. ``df`` callers don't go through this property.
        """
        return self._resolved_sql or self.sql


# --- Dataset config ---


class DatasetConfig(BaseModel):
    """A single dataset to validate — one ``DatasetConfig`` per
    table / query / DataFrame / WAP block in your config.

    Source forms:

    - ``table``: a fully-qualified table / view name. Most
      common.
    - ``query``: an arbitrary SELECT; materialised as a temp
      view. Mutually exclusive with ``table`` / ``df`` /
      ``wap``.
    - ``df``: an in-memory pandas / Spark DataFrame
      (programmatic-only, not in YAML). Can be combined with
      ``table`` — df supplies the data, table supplies the
      logical identity. Mutually exclusive with ``query`` /
      ``wap``.
    - ``wap``: WAP lifecycle (write to staging → audit →
      publish or rollback). See ``docs/architecture.md``
      §2 WAP lifecycle. Mutually exclusive with ``table`` /
      ``query`` / ``df``.

    Identity:

    - ``name`` is the logical identity used for system-table
      persistence + history lookups. Defaults to ``table`` for
      single-table datasets; required for ``query`` / ``df`` /
      ``wap`` (history would otherwise collapse unrelated
      workloads onto the same key).
    - ``description`` is operator-facing prose persisted on
      every row produced by this dataset.

    Partitioning:

    - ``partition_ts`` is the first-class partition identity.
      Accepts a column reference (e.g. ``event_dt``), a Jinja
      literal (``'{{ ds }}'``), or a SQL function
      (``CURRENT_TIMESTAMP``). Drives history-anchored reads at
      ``anchor − k·step``.
    - ``partition_step`` is the cadence (ISO-8601 duration:
      ``P7D``, ``PT1H``, etc.). Required for the
      partition-anchored history-backed validators
      (drift / forecast / pattern / shape).

    Compliance:

    - ``redacted_columns`` / ``allowlist_columns`` redact source
      column names from SHAP / drift / schema-change emission.
      See ``docs/CHANGELOG.md`` § webhook-payload-redaction.

    See ``docs/configuration.md`` (once
    ``configuration-reference-audit`` ships) for the full field
    reference + worked examples.
    """
    model_config = {"arbitrary_types_allowed": True}

    # `name` is the logical identity used for system-table persistence and
    # history lookups. When omitted, defaults to `table` for single-table
    # datasets — see `_default_name`. For query/wap/df-only datasets, the
    # user must supply a name explicitly (history would otherwise collapse
    # disparate workloads onto the same key).
    name: str | None = None
    # Optional human description; persisted on every system-table row produced
    # by this dataset (column `dataset_description`).
    description: str | None = None
    table: str | None = None
    query: str | None = None
    df: Any = Field(default=None, exclude=True, repr=False)
    dimensions: list[str] | None = None
    measures: list[str] | None = None
    filter: str | None = None
    # Partition timestamp expression. Rendered through Jinja first, then
    # injected by collectors as `<expr> AS qf_partition_ts` into the
    # collector SELECT. Supports column refs (`event_dt`,
    # `CAST(event_ts AS DATE)`), Jinja constants (`'{{ ds }}'`), and SQL
    # functions (`CURRENT_TIMESTAMP`). Validators that read history use
    # this as the anchor for partition-anchored lookbacks (anchor − k·step).
    partition_ts: str | None = None
    # Partition cadence (ISO 8601 duration). Required when partition_ts
    # is set on this dataset OR inherited from run-level. Forbidden
    # otherwise. See plan H3.
    partition_step: str | None = None
    cache: bool = False
    cache_storage_level: str = "MEMORY_AND_DISK"
    validation_parallelism: int = 1  # parallel collect+validate pipelines within this dataset
    wap: WAPConfig | None = None
    validations: list[ValidationConfig] = Field(default_factory=list)
    # webhook-payload-redaction: source columns listed here have
    # their names replaced with ``"<redacted>"`` in
    # ``details["top_contributing_features"]``,
    # ``details["value_drift_explainer"]``, and the schema-change
    # emission (`new_columns`/`dropped_columns`/
    # `inconsistent_past_columns` plus the validation message
    # interpolation). Compute-time redaction — applies to every
    # downstream egress (system-table, dashboard, programmatic
    # API, third-party webhook). Combined additively with the
    # instance-level ``Qualifire(redacted_columns=...)`` list.
    redacted_columns: list[str] = Field(default_factory=list)
    # When set, columns NOT in this allowlist are also redacted.
    # ``None`` = "no allowlist policy at this scope". See
    # ``qualifire/core/_redaction.py`` for the one-sided
    # configuration semantics (intersection when both instance
    # and dataset set the allowlist).
    allowlist_columns: list[str] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if not _SAFE_NAME_RE.match(v):
            raise ValueError(
                f"Dataset 'name' must contain only alphanumeric characters, "
                f"underscores, dots, or hyphens, got: {v!r}"
            )
        return v

    @field_validator("partition_step")
    @classmethod
    def _validate_partition_step(cls, v: str | None) -> str | None:
        if v is None:
            return v
        from qualifire.core.duration import parse_duration
        from qualifire.core.exceptions import QualifireConfigError

        try:
            parse_duration(v)
        except QualifireConfigError as e:
            raise ValueError(str(e)) from e
        return v

    @model_validator(mode="after")
    def _default_name(self) -> DatasetConfig:
        """Default `name` to `table` for single-table datasets.

        Only fires when `table` is set; query/wap/df-only datasets must
        name themselves explicitly so unrelated workloads can't share a
        history key under the default. Run after `_require_source` so we
        know the source mode is valid.
        """
        if self.name is None and self.table:
            object.__setattr__(self, "name", self.table)
        if self.name is None:
            raise ValueError(
                "Dataset 'name' is required for query/wap/df-only datasets — "
                "the dataset name is the system-table key for history lookups "
                "and alert suppression. For single-table datasets `table=` "
                "supplies the default."
            )
        return self

    @field_validator("dimensions", "measures")
    @classmethod
    def _validate_column_names(cls, v: list[str] | None) -> list[str] | None:
        if v is not None:
            for col in v:
                if not _SAFE_IDENTIFIER_RE.match(col):
                    raise ValueError(
                        f"Column name must be a valid SQL identifier, got: {col!r}"
                    )
        return v

    @model_validator(mode="after")
    def _require_source(self) -> DatasetConfig:
        """Validate the dataset's data source.

        There are three mutually-exclusive *source modes*:
          - ``query``: materialize a SQL SELECT as a temp view
          - ``wap``: write-audit-publish pattern
          - ``table`` and/or ``df``: direct table reference, optionally
            overridden by a pre-built DataFrame for this run

        ``df`` pairs with ``table``: when both are set, ``df`` provides
        the data for the run while ``table`` provides the stable logical
        identifier used for history lookups and system-table
        persistence (see ``engine._run_dataset`` for the threading).
        ``df`` alone is also valid — the dataset ``name`` becomes the
        logical identifier.

        Previously this validator only recognized ``table``, ``query``,
        and ``wap``, so ``DatasetConfig(df=...)`` raised even though
        the engine and public API supported it. That inconsistency made
        ``df`` usable through the API layer but broken at the model
        layer — an integration trap. Accepting ``df`` here aligns the
        model contract with the engine.
        """
        has_table_or_df = bool(self.table) or self.df is not None
        query_set = bool(self.query)
        wap_set = bool(self.wap)

        modes = sum([has_table_or_df, query_set, wap_set])
        if modes == 0:
            raise ValueError(
                "One of 'table', 'df', 'query', or 'wap' must be specified for a dataset"
            )
        if modes > 1:
            raise ValueError(
                "'query', 'wap', and 'table'/'df' are mutually exclusive source "
                "modes. ('table' and 'df' may be combined: df supplies the data, "
                "table supplies the logical identity.)"
            )
        return self


# --- Top-level config ---


class QualifireConfig(BaseModel):
    """Top-level qualifire configuration — the parsed shape of a
    ``qualifire run --config <file>`` YAML.

    The smallest legal config:

    .. code-block:: yaml

        owner: data-eng
        bu: finance
        system_table: catalog.schema.qf_history
        datasets: []  # populate with at least one DatasetConfig

    Container fields:

    - ``system_table`` is the name of the persistent state table
      (validation history, collection history, notification audit
      rows). All four backends (``external_catalog``, ``delta``,
      ``sqlite``, ``jdbc``) use the same logical schema; the
      backend-specific concerns live in ``system_table_backend``
      and (for JDBC only) ``jdbc``.
    - ``datasets`` is the list of ``DatasetConfig``s the engine
      iterates per run.
    - ``notifications`` declares notification channels by name;
      validations reference them via ``notify.warning`` /
      ``notify.error`` lists. See ``docs/notifications.md``.
    - ``engine_notify`` routes engine-level events (persistence
      failures, suppression-read failures, WAP cleanup leaks)
      separately from per-validation notify routing.
    - ``dataset_parallelism`` runs N datasets concurrently via a
      thread pool. Defaults to 1 (serial).

    See ``docs/architecture.md`` for end-to-end data flow and
    ``docs/CHANGELOG.md`` for recently-added fields (e.g.
    runtime skip-* flags surface on the public API, not on this
    class).
    """
    owner: str
    bu: str
    system_table: str
    # Constrained to known values so typos and unsupported backends
    # fail at ``load_config()`` / ``validate-config`` time rather than
    # deep inside ``Qualifire._init_storage`` with a ``ValueError`` the
    # CLI used to surface as a Python traceback.
    system_table_backend: Literal[
        "external_catalog", "delta", "sqlite", "jdbc"
    ] = "external_catalog"
    # JDBC connection settings. Required (and validated) only when
    # ``system_table_backend == 'jdbc'`` — see ``_require_jdbc_when_needed``.
    jdbc: JDBCConfig | None = None
    dataset_parallelism: int = 1
    notifications: dict[str, NotificationChannelConfig] = Field(default_factory=dict)
    # Routing for engine-level events (persistence failures, suppression-read
    # failures, WAP cleanup leaks). Distinct from per-validation `notify`
    # because these events aren't tied to a user-defined check. When unset,
    # engine warnings are returned in the result but not routed to any
    # channel.
    engine_notify: NotifyConfig | None = None
    # Run-level default partition_ts expression. Datasets without their
    # own `partition_ts` inherit this value. See `DatasetConfig.partition_ts`
    # for semantics and accepted forms.
    partition_ts: str | None = None
    # Run-level default partition cadence (ISO 8601 duration). Datasets
    # without their own `partition_step` inherit this value. See plan H3.
    partition_step: str | None = None
    datasets: list[DatasetConfig]
    context: dict[str, str] = Field(default_factory=dict)

    @field_validator("partition_step")
    @classmethod
    def _validate_partition_step(cls, v: str | None) -> str | None:
        if v is None:
            return v
        from qualifire.core.duration import parse_duration
        from qualifire.core.exceptions import QualifireConfigError

        try:
            parse_duration(v)
        except QualifireConfigError as e:
            raise ValueError(str(e)) from e
        return v

    @model_validator(mode="after")
    def _require_jdbc_when_needed(self) -> QualifireConfig:
        """When ``system_table_backend='jdbc'``, require a ``jdbc:`` block.

        Phase 2 P2.1 wires JDBC as a first-class backend, but the
        ``JDBCStorage`` constructor still needs a URL to connect. Fail
        at config-load time with a clear migration pointer instead of
        letting the storage layer raise a low-level error. Conversely,
        a stray ``jdbc:`` block paired with a non-jdbc backend is
        tolerated silently — it has no effect and rejecting it would
        be over-strict (operators may template the block in and out,
        keeping ``user``/``password`` in YAML while ``url`` lands only
        on deployments whose backend is ``jdbc``). ``JDBCConfig.url``
        is ``Optional`` precisely to make that templating path work;
        this validator enforces ``url`` only when the active backend
        needs it.
        """
        if self.system_table_backend == "jdbc":
            if self.jdbc is None or (not self.jdbc.url and not self.jdbc.from_secret):
                raise ValueError(
                    "system_table_backend='jdbc' requires a top-level "
                    "'jdbc:' block with at least a 'url' (or a "
                    "'from_secret:' directive that supplies one).\n"
                    "Example:\n"
                    "  jdbc:\n"
                    "    url: 'jdbc:postgresql://host:5432/db'\n"
                    "    user: 'qf_user'\n"
                    "    password: 'secret://prod_db/password'"
                )
        return self

    @model_validator(mode="after")
    def _validate_partition_step_invariant(self) -> QualifireConfig:
        """Cross-field check on ``partition_step`` ↔ ``partition_ts``.

        ``partition_ts`` alone is valid for any normal validation
        run — it just stamps the persisted rows and anchors history
        reads. ``partition_step`` is only load-bearing for backfill /
        deactivate-metric, which step through a partition range. So:

          - ``partition_ts`` WITHOUT ``partition_step`` is allowed.
            Backfill / deactivate are gated separately at invocation
            time and raise then if ``partition_step`` is missing.
          - ``partition_step`` WITHOUT ``partition_ts`` is rejected
            ("decorative cadence" — nothing to step through).
        """
        run_pts = self.partition_ts
        run_pstep = self.partition_step

        any_dataset_has_pts = run_pts is not None or any(
            ds.partition_ts is not None for ds in self.datasets
        )

        if run_pstep is not None and not any_dataset_has_pts:
            raise ValueError(
                "run-level 'partition_step' is set but no dataset has "
                "'partition_ts' (own or inherited). The cadence is "
                "decorative without a partition_ts. Either remove the "
                "run-level partition_step, or add partition_ts to the "
                "datasets that should be partition-anchored."
            )

        for ds in self.datasets:
            eff_pts = ds.partition_ts if ds.partition_ts is not None else run_pts
            eff_pstep = ds.partition_step if ds.partition_step is not None else run_pstep

            if eff_pts is None and eff_pstep is not None:
                raise ValueError(
                    f"dataset {ds.name!r}: 'partition_step' is set "
                    f"({eff_pstep!r}) but 'partition_ts' is not. "
                    "partition_step requires partition_ts — without "
                    "an anchor expression there is nothing to step "
                    "through. Either remove partition_step or add "
                    "partition_ts."
                )

        return self

    @model_validator(mode="after")
    def _require_unique_dataset_names(self) -> QualifireConfig:
        """Dataset names must be unique within a config.

        The dataset name is the logical identity threaded through the
        whole engine: system-table persistence keys on ``dataset_name``
        (``engine.py:855,889``), drift/forecast validators look up
        history by ``logical_table = dataset_name``
        (``engine.py:697,706``), the health report groups rows by
        ``dataset_name`` (``reporting/health.py:64``), and the
        notification deduper keys on dataset name. Duplicates mix
        history across unrelated datasets, collapse them in reports,
        and cross-suppress each other's alerts — silent data-integrity
        bugs that a deprecation warning cannot prevent.

        Hard-fail is intentional: any config with duplicate-named
        datasets is already producing ambiguous persisted data, and
        failing at load-time forces operators to rename the
        duplicates (the fix they would need regardless) before the
        corruption continues. The engine's per-instance temp-view
        disambiguation (``engine._make_view_name``) stays in place
        as belt-and-braces protection for any programmatic caller
        that bypasses this validator.
        """
        seen: set[str] = set()
        duplicates: list[str] = []
        for ds in self.datasets:
            if ds.name in seen:
                duplicates.append(ds.name)
            else:
                seen.add(ds.name)
        if duplicates:
            unique_dupes = sorted(set(duplicates))
            raise ValueError(
                f"Duplicate dataset name(s): {unique_dupes!r}. Dataset "
                "names are the persistence / history / reporting keys — "
                "duplicates will silently mix drift history, cross-suppress "
                "alerts, and collapse report rows across unrelated "
                "datasets. Rename each dataset to a distinct identifier."
            )
        return self


def effective_partition_step(
    dataset: DatasetConfig, run_config: QualifireConfig
) -> str | None:
    """Resolve effective partition_step (dataset → run → None).

    Plan H3: callers can treat ``None`` as "this dataset is not
    backfillable" without re-checking the invariant.
    """
    if dataset.partition_step is not None:
        return dataset.partition_step
    return run_config.partition_step


def effective_partition_ts(
    dataset: DatasetConfig, run_config: QualifireConfig
) -> str | None:
    """Resolve effective partition_ts (dataset → run → None)."""
    if dataset.partition_ts is not None:
        return dataset.partition_ts
    return run_config.partition_ts


def load_config(path: str | Path) -> QualifireConfig:
    """Load and validate a YAML or JSON config file.

    Raises:
        QualifireConfigError: If the file cannot be parsed or validation fails.
    """
    from qualifire.core.exceptions import QualifireConfigError

    # Unified ``Config validation failed:`` prefix on every raise from
    # this function. Gives operators a single grep anchor across all
    # failure modes (file missing, YAML parse error, df-from-YAML,
    # Pydantic validation). The CLI prints ``str(e)`` verbatim —
    # adding a second prefix there would produce the doubled-prefix
    # noise Codex called out in round 14.
    path = Path(path)
    if not path.exists():
        raise QualifireConfigError(f"Config validation failed: file not found: {path}")

    try:
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        raise QualifireConfigError(f"Config validation failed: failed to parse YAML: {e}") from e

    if not isinstance(data, dict):
        raise QualifireConfigError(
            "Config validation failed: config file must contain a YAML mapping at top level"
        )

    # ``df`` is a programmatic-only source: it must be a live, in-process
    # backend DataFrame (Spark/pandas/...) so the engine can register it
    # as a temp view. YAML/JSON can only produce scalars/lists/dicts, so
    # any *non-null* ``df:`` payload inside a file-loaded dataset is
    # necessarily a typo, a templating artifact, or a copy-paste error.
    # Before this gate, such values passed ``DatasetConfig._require_source``
    # (which only checks ``df is not None``) and failed later inside
    # the engine's ``_materialize_df`` with an opaque type error.
    #
    # Null tolerance: ``df: null`` is stripped silently so a templating
    # emitter writing ``df: null`` for absent fields doesn't trip the
    # gate. A non-null ``df`` payload still hard-fails below.
    raw_datasets = data.get("datasets")
    if isinstance(raw_datasets, list):
        for idx, ds in enumerate(raw_datasets):
            if isinstance(ds, dict) and "df" in ds:
                raw_df = ds["df"]
                if raw_df is None:
                    # Benign templating artifact — strip so the value
                    # never reaches model validation, preserving the
                    # model's "no df means no df" invariant.
                    ds.pop("df", None)
                    continue
                ds_name = ds.get("name", f"#{idx}")
                raise QualifireConfigError(
                    f"Config validation failed: dataset {ds_name!r}: "
                    "'df' cannot be set from a config file — it requires a "
                    "live in-process DataFrame and is programmatic-only. "
                    "Use 'table', 'query', or 'wap' as the dataset source "
                    "in YAML, or supply the DataFrame through the Python "
                    "API (qf.validate(df=...))."
                )

    try:
        return QualifireConfig.model_validate(data)
    except Exception as e:
        raise QualifireConfigError(f"Config validation failed: {e}") from e
