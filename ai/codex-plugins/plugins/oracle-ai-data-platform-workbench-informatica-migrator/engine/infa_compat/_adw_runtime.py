"""Private helper: the stage-via-JDBC-overwrite + driver-side-Oracle-SQL
mechanics shared by ``scd2.py`` and ``update_strategy.py``'s ADW paths.

Not part of the public API (leading underscore, not listed in
``SUPPORTED_OPERATIONS.md``) -- it exists so the "write the delta to a
staging table via JDBC overwrite, then run the real DML from the driver,
then drop the staging table" plumbing is written and
reasoned about once, rather than copy-pasted between the two modules
that both need it. Both callers own the SQL *shape* (what MERGE/INSERT
statement to run); this module only owns getting the data staged and
the statements executed transactionally.

UNVERIFIED against a live ADW -- see both callers' module docstrings.
``pyspark``/``oracledb`` usage is entirely inside function bodies, never
at module import time.
"""
from __future__ import annotations

from typing import Any, Sequence


def stage_via_jdbc_overwrite(df: Any, staging_table: str, jdbc_options: dict) -> None:
    """Overwrite ``staging_table`` with ``df`` via Spark JDBC.

    External (ADW/ALH/ATP) catalogs are overwrite-only from Spark -- no
    DDL, no MERGE, no conditional append -- so this is
    intentionally the ONLY write mode used here. ``jdbc_options`` must
    already carry the caller's staged-wallet ``url``/``driver``/
    ``user``/``password`` (built the same way
    ``write_strategies.AdwWriteStrategy._setup_lines`` documents for the
    generated-notebook path -- this module does not stage a wallet
    itself, it only writes through options already assembled).
    """
    (
        df.write.format("jdbc")
        .options(**jdbc_options)
        .option("dbtable", staging_table)
        .mode("overwrite")
        .save()
    )


def run_statements_transactionally(connection: Any, statements: Sequence[str]) -> None:
    """Execute ``statements`` in order on ``connection``, committing once
    at the end, rolling back on any failure. Statements are plain SQL
    strings built by the caller (structural identifiers only -- table and
    column names this migrator generated itself, not end-user data), so
    no bind-parameter interface is needed here.
    """
    cursor = connection.cursor()
    try:
        for statement in statements:
            cursor.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def drop_staging_table_best_effort(connection: Any, staging_table: str) -> None:
    """Best-effort cleanup: drop the staging table, swallowing any error
    (e.g. it was never created because staging itself failed) so cleanup
    never masks the original exception that brought us here.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE {staging_table} PURGE")
        connection.commit()
    except Exception:
        pass
    finally:
        cursor.close()
