"""Content-pack builtin adapters.

Each adapter exposes the uniform :class:`BuiltinCallable` shape
``(spark, node, pack, profile, ctx) -> DataFrame`` and translates to the
actual v1 builtin signature inside its body. The :mod:`sql_runner`
``_BUILTIN_REGISTRY`` keys adapter strings (the same dotted
``<module>:<func>`` form used in node YAML ``implementation.callable``)
to the adapter function PLUS a version constant that flows into the
content-pack plan-hash for drift detection.

Adding a new builtin:

1. Author the adapter module here (e.g. ``dim_widget_adapter.py``).
2. Expose ``run(spark, *, node, pack, profile, ctx) -> DataFrame`` plus
   a ``VERSION: str`` constant.
3. Register it in :data:`sql_runner._BUILTIN_REGISTRY` keyed by the
   YAML's ``implementation.callable`` string.

ADR-0011 keeps this set small: builtins are reserved for nodes that
genuinely cannot be expressed as pack-level SQL (parameter-driven, no
bronze source).
"""
