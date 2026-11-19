"""Content-pack adapter for the ``dim_calendar`` builtin.

The v1 builtin
(:func:`oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar.build`)
exposes a parameter-driven signature
``(spark, *, paths, start_date, end_date, fiscal_start_month, silver_table,
run_id) -> DataFrame``. The content-pack builtin-dispatch path (in
``sql_runner._execute_builtin_node``) calls every registered adapter with
the uniform shape ``(spark, node, pack, profile, ctx) -> DataFrame``.
This module bridges the two by extracting calendar settings from the
content-pack inputs (tenant profile override → pack profile defaults →
builtin hardcoded defaults), building the fully-qualified silver target
identifier from ``ctx``, and threading ``ctx.run_id`` into the audit
column.

Why the bridge exists at all
----------------------------

Calling the v1 builtin directly from ``_execute_builtin_node`` would
require the runner to know how to read the calendar block from the
profile — which would leak v1-builtin knowledge into the generic
dispatcher. With an adapter:

* The existing module signature stays untouched.
* The dispatcher only knows the uniform adapter shape.
* New builtins (if any) add their own adapter without changing
  ``sql_runner``.

Precedence for calendar settings
--------------------------------

For each of ``start_date`` / ``end_date`` / ``fiscal_start_month``:

1. **Tenant-profile override** — ``profile.profile.calendar.<key>``
   if the customer has supplied a per-tenant override.
2. **Pack profile default** — ``pack.pack.profiles[ctx.active_profile_name].calendar.<key>``
   from ``pack.yaml`` (e.g. ``finance-default``).
3. **Builtin hardcoded default** — the v1 builtin's own module-level
   ``DEFAULT_*`` constant.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from oracle_ai_data_platform_fusion_autopilot.dimensions import dim_calendar as _dim_calendar

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

    from ...schema.medallion_pack import NodeYaml
    from ...schema.tenant_profile import TenantProfile
    from ..content_pack import ResolvedPack
    from ..sql_renderer import RunContext


VERSION: str = "1.0.0"
"""Adapter version constant. Flows into the content-pack plan-hash
substitute for builtin nodes — bumping this triggers the same drift gate
as a SQL-template edit."""


def _read_calendar_setting(
    *,
    key: str,
    profile: "TenantProfile",
    pack: "ResolvedPack",
    active_profile_name: str,
    default: Any,
) -> Any:
    """Resolve one calendar setting via tenant-profile → pack-profile → default.

    Tenant profile override path: ``profile.profile.calendar.<key>``.
    Pack profile default path: ``pack.pack.profiles[<active>].calendar.<key>``.

    Both paths are tolerant of missing intermediate keys — a tenant that
    doesn't supply a ``calendar:`` block in their free-form profile, or
    a pack profile that omits a calendar setting, simply falls through
    to the next layer.
    """
    # Tenant override.
    profile_block = profile.profile if isinstance(profile.profile, dict) else {}
    tenant_calendar = profile_block.get("calendar") if isinstance(profile_block, dict) else None
    if isinstance(tenant_calendar, dict) and key in tenant_calendar:
        return tenant_calendar[key]

    # Pack default.
    pack_profile = pack.pack.profiles.get(active_profile_name) if pack.pack.profiles else None
    if pack_profile is not None:
        pack_calendar = pack_profile.calendar
        if pack_calendar is not None:
            # The pack-side calendar is a Pydantic CalendarProfile with snake_case
            # attrs; map the YAML/profile camelCase keys to them.
            attr_map = {
                "startDate": "start_date",
                "endDate": "end_date",
                "fiscalStartMonth": "fiscal_start_month",
            }
            attr = attr_map.get(key, key)
            value = getattr(pack_calendar, attr, None)
            if value is not None:
                return value

    return default


def run(
    spark: "SparkSession",
    *,
    node: "NodeYaml",
    pack: "ResolvedPack",
    profile: "TenantProfile",
    ctx: "RunContext",
) -> "DataFrame":
    """Run the ``dim_calendar`` builtin under content-pack dispatch.

    Resolves calendar settings (start/end date, fiscal start month) per
    the precedence above, constructs the silver target identifier from
    ``ctx``, and invokes
    :func:`oracle_ai_data_platform_fusion_autopilot.dimensions.dim_calendar.build`.

    Args:
        spark: live Spark session.
        node: the ``dim_calendar`` NodeYaml (``implementation.type: builtin``).
        pack: assembled ResolvedPack — provides ``pack.pack.profiles[<active>]``
            for default calendar settings.
        profile: validated TenantProfile — its ``profile.calendar`` block
            (if present) overrides pack defaults.
        ctx: RunContext — supplies ``catalog`` / ``silver_schema`` /
            ``run_id`` / ``active_profile_name``.

    Returns:
        The DataFrame backed by the freshly-written silver table (same
        shape :func:`dim_calendar.build` returns).
    """
    start_date = _read_calendar_setting(
        key="startDate",
        profile=profile,
        pack=pack,
        active_profile_name=ctx.active_profile_name,
        default=_dim_calendar.DEFAULT_START_DATE,
    )
    end_date = _read_calendar_setting(
        key="endDate",
        profile=profile,
        pack=pack,
        active_profile_name=ctx.active_profile_name,
        default=_dim_calendar.DEFAULT_END_DATE,
    )
    fiscal_start_month = _read_calendar_setting(
        key="fiscalStartMonth",
        profile=profile,
        pack=pack,
        active_profile_name=ctx.active_profile_name,
        default=_dim_calendar.DEFAULT_FISCAL_START_MONTH,
    )

    silver_table = f"{ctx.catalog}.{ctx.silver_schema}.{node.target}"

    return _dim_calendar.build(
        spark,
        start_date=start_date,
        end_date=end_date,
        fiscal_start_month=int(fiscal_start_month),
        silver_table=silver_table,
        run_id=ctx.run_id,
    )


__all__ = ["VERSION", "run"]
