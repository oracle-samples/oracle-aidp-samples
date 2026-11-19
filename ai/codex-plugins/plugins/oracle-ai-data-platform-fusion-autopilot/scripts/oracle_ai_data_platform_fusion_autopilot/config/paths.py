"""Tenant-aware Spark table-path resolution.

Bundle config (``bundle.yaml`` → ``aidp.{catalog, bronzeSchema, silverSchema,
goldSchema}``) is the tenant's source of truth for which Unity Catalog
catalog and schemas the medallion layers live in. ``TablePaths`` is the
one place those values resolve into the 3-part identifiers Spark consumes.

Each shipped mart / dim module accepts a ``paths: TablePaths | None``
kwarg on its ``build()``. ``None`` → ``DEFAULT_PATHS`` (catalog=
``"fusion_catalog"``, bronze_schema=``"bronze"``, silver_schema=``"silver"``,
gold_schema=``"gold"``). A non-None ``paths`` overrides per-table-name kwargs
only when those kwargs are themselves left at their sentinel defaults.

SQL-injection safety
--------------------

Every value flowing into ``TablePaths`` is interpolated unquoted into Spark
SQL (``CREATE OR REPLACE TABLE <catalog>.<schema>.<table>``). Names are
validated against ``^[A-Za-z_][A-Za-z0-9_]*$`` at construction. Non-string
inputs are rejected with a clear ``TypeError``; identifier-rule violations
raise ``ValueError`` naming the offending value. Same identifier regex
``dim_account._validate_segment_map`` uses for semantic aliases — single
source of truth for what counts as a safe unquoted SQL identifier.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

#: Strict SQL-identifier pattern — same as ``dim_account._SQL_IDENTIFIER_RE``
#: and ``gl_balance._SQL_IDENTIFIER_RE``. Catalog and schema names go straight
#: into SQL unquoted; this rule prevents both injection and cryptic Spark
#: parse errors from typos.
_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TablePaths:
    """Resolves ``aidp.{catalog,bronzeSchema,silverSchema,goldSchema}`` into
    Spark 3-part identifiers.

    Constructor validates every field against the strict SQL-identifier regex.
    Non-string values, empty strings, hyphens, leading digits, and injection
    attempts all raise at construction time so they can never reach SQL.
    """

    catalog:        str = "fusion_catalog"
    bronze_schema:  str = "bronze"
    silver_schema:  str = "silver"
    gold_schema:    str = "gold"

    def __post_init__(self) -> None:
        for field_name, value in (
            ("catalog",       self.catalog),
            ("bronze_schema", self.bronze_schema),
            ("silver_schema", self.silver_schema),
            ("gold_schema",   self.gold_schema),
        ):
            _validate_identifier(field_name, value)

    def bronze(self, table: str) -> str:
        _validate_identifier("table", table)
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        _validate_identifier("table", table)
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        _validate_identifier("table", table)
        return f"{self.catalog}.{self.gold_schema}.{table}"

    @classmethod
    def from_bundle(cls, bundle_data: Mapping[str, Any] | Any) -> TablePaths:
        """Construct from loaded ``bundle.yaml`` data.

        Reads the conventional ``aidp.{catalog, bronzeSchema, silverSchema,
        goldSchema}`` keys. Missing keys fall back to dataclass defaults;
        non-string values raise ``TypeError`` (NO ``str()`` coercion).
        Identifier-rule violations raise ``ValueError`` via
        :meth:`__post_init__`.

        Accepts either a raw mapping (e.g. ``yaml.safe_load(bundle_path)``)
        OR a Pydantic-style object that exposes ``.model_dump()``. The
        helper duck-types ``model_dump`` so importing this module doesn't
        couple it to pydantic.
        """
        if hasattr(bundle_data, "model_dump") and callable(bundle_data.model_dump):
            bundle_data = bundle_data.model_dump(by_alias=True)
        if not isinstance(bundle_data, Mapping):
            raise TypeError(
                "TablePaths.from_bundle expects a Mapping or a Pydantic "
                f"model with .model_dump(); got {type(bundle_data).__name__}"
            )
        aidp = bundle_data.get("aidp") or {}
        if not isinstance(aidp, Mapping):
            raise TypeError(
                "bundle_data['aidp'] must be a Mapping or None; got "
                f"{type(aidp).__name__}"
            )
        return cls(
            catalog       = _read(aidp, "catalog",       "catalog",       cls.catalog),
            bronze_schema = _read(aidp, "bronze_schema", "bronzeSchema",  cls.bronze_schema),
            silver_schema = _read(aidp, "silver_schema", "silverSchema",  cls.silver_schema),
            gold_schema   = _read(aidp, "gold_schema",   "goldSchema",    cls.gold_schema),
        )


def _read(d: Mapping[str, Any], key_snake: str, key_camel: str, default: str) -> str:
    """Read either snake_case or camelCase from a raw or Pydantic-dumped dict.

    Raises :class:`TypeError` when a non-string value is present (NO
    ``str()`` coercion). Empty dict / both keys absent returns the default.
    """
    if key_snake in d:
        value = d[key_snake]
    elif key_camel in d:
        value = d[key_camel]
    else:
        return default
    if not isinstance(value, str):
        raise TypeError(
            f"aidp.{key_camel} must be a string; got {type(value).__name__} "
            f"({value!r}). Configure as a quoted YAML string."
        )
    return value


def _validate_identifier(field_name: str, value: Any) -> None:
    """Reject anything that wouldn't safely interpolate into Spark SQL."""
    if not isinstance(value, str):
        raise TypeError(
            f"{field_name} must be a string; got {type(value).__name__} "
            f"({value!r})"
        )
    if not _SQL_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{field_name}={value!r} is not a valid unquoted SQL identifier — "
            "must match ^[A-Za-z_][A-Za-z0-9_]*$. Catalogs / schemas / tables "
            "with hyphens, dots, or other special characters require Unity-"
            "Catalog-side renaming."
        )


#: Conventional defaults for all shipped modules.
#: Module-level constants derive from this so a single source of truth governs.
DEFAULT_PATHS: Final[TablePaths] = TablePaths()


__all__ = ["DEFAULT_PATHS", "TablePaths"]
