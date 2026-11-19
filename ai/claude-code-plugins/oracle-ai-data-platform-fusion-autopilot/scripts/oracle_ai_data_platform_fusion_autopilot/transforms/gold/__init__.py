"""Gold-layer business marts.

supplier_spend, gl_balance, and ap_aging ship as SQL templates under
``content_packs/<pack-id>/gold/`` and dispatch via
``orchestrator.sql_runner.execute_node``.

This package directory is retained as a stable import target; the
content pack is the new authoring surface.
"""

__all__: list[str] = []
