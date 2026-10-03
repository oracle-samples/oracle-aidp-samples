"""Persisted, restart-safe surrogate-key sequences ( Class C).

Informatica's Sequence Generator ``NEXTVAL`` is a **persisted** counter
with a cache size and an optional cycle. Inline, a generator reaches for
``F.monotonically_increasing_id()``, which is:

- non-contiguous (it encodes a partition id into the high bits, so values
  are not "1, 2, 3, ..." even within one run),
- not persisted across runs (every job starts back at whatever the
  partition layout happens to produce), and
- not restart-safe (re-running a failed job re-issues the same range of
  values, colliding with rows the previous attempt already committed).

That is precisely why this is a library function with a real backing
store rather than an expression: the store is what makes restart-safety
possible.

Pluggable backend, chosen explicitly
-------------------------------------
Per the task brief and, the backend is **decided by target
type, never inferred** -- mirrors
``engine/infa2aidp/generators/write_strategies.get_write_strategy``'s
explicit ``target_catalog_type`` dispatch, which is's version of
the same rule for writes. Two backends:

- :class:`DeltaSequenceBackend` -- a Delta-backed transactional counter
  table, advanced via an optimistic-concurrency read/compare-and-swap
  retry loop (Delta has no ``SELECT ... FOR UPDATE``; CAS-and-retry is
  the standard pattern for a Delta-backed counter).
- :class:`AdwSequenceBackend` -- wraps a real Oracle ``SEQUENCE`` object,
  fetched via ``NEXTVAL`` through a driver-side ``python-oracledb``
  connection (Spark JDBC has no notion of a sequence -- this must run
  from the driver, same boundary ``AdwWriteStrategy`` draws for the
  stage-then-merge upsert path in ``write_strategies.py``).

Both backends only need to support one operation -- *atomically reserve a
block of N consecutive integers, return the first one* -- which is
:meth:`SequenceBackend.reserve_block`. Everything else (cache management,
cycle-wraparound, the public ``next_value``/``next_values`` calls) is
backend-agnostic and lives in :class:`Sequence`.

**Unverified against a live Spark/Delta or ADW runtime.** PySpark is not
installed in this development environment (see module import structure
below), so ``DeltaSequenceBackend``'s Delta merge/CAS retry loop and
``AdwSequenceBackend``'s ``oracledb`` calls have never executed. What
*is* tested is the backend-agnostic reservation/cache/cycle algorithm in
:class:`Sequence`, against a fake in-memory backend that implements the
exact same ``SequenceBackend`` contract the real backends do -- see
``tests/test_infa_compat.py``.

Cycle behavior (a genuinely ambiguous point without a live Integration
Service to check against): Informatica's Sequence Generator, when
``Cycle`` is disabled and the End Value is reached, fails the session
rather than wrapping. This module mirrors that as the common case:
``cycle=False`` (the default) raises :class:`SequenceExhausted` once
``max_value`` would be exceeded; ``cycle=True`` wraps back to ``start``.
This is documented here as an assumption, not silently guessed --
flag it for correction against a real Integration Service export if a
customer mapping's cycle behavior is ever observed to differ.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional


class SequenceExhausted(Exception):
    """Raised when a non-cycling sequence would exceed ``max_value``."""


class SequenceBackend(ABC):
    """Reserves blocks of consecutive integers from a persisted counter.

    ``reserve_block`` must be atomic with respect to concurrent callers
    against the same ``name`` -- two concurrent reservations for the same
    sequence name must never return overlapping ranges. It is allowed
    (and, under a crash, expected) to leave gaps: if a caller reserves a
    block of 1000 and only consumes 3 before the job dies, those 997
    values are gone forever. That mirrors real Informatica Sequence
    Generator cache behavior exactly -- gaps on restart are the accepted
    cost the task brief calls out; **collisions are not**.
    """

    @abstractmethod
    def reserve_block(self, name: str, count: int, increment: int) -> int:
        """Atomically advance the persisted counter for ``name`` by
        ``count * increment`` and return the FIRST value of the newly
        reserved block (the block is
        ``first, first + increment, first + 2*increment, ...`` for
        ``count`` values). Creates the counter at ``increment`` (i.e. its
        first-ever reservation starts at the backend's configured start
        value) if ``name`` has never been reserved before.
        """
        raise NotImplementedError


class DeltaSequenceBackend(SequenceBackend):
    """Delta-backed transactional counter.

    Table shape (created on first use if absent):
    ``sequence_name STRING, current_value LONG``. Advancing is a
    read-current / attempt-conditional-update retry loop: Delta has no
    row-level ``SELECT ... FOR UPDATE``, so correctness under concurrent
    writers comes from retrying the whole read-modify-write on a failed
    conditional update, standard optimistic-concurrency practice for a
    Delta-backed counter (the same shape Delta's own docs use for
    "counter table" recipes).

    UNVERIFIED -- no local Spark/Delta runtime in this environment. The
    ``pyspark``/``delta`` imports are deferred into ``reserve_block`` so
    importing this module (or the package) never requires PySpark to be
    installed.
    """

    def __init__(self, spark: Any, table: str = "_infa_compat_sequences", start: int = 1):
        self._spark = spark
        self._table = table
        self._start = start

    def _ensure_table(self) -> None:
        from delta.tables import DeltaTable  # noqa: F401  (deferred; see module docstring)

        if not self._spark.catalog.tableExists(self._table):
            self._spark.sql(
                f"CREATE TABLE {self._table} "
                f"(sequence_name STRING, current_value LONG) USING DELTA"
            )

    def reserve_block(self, name: str, count: int, increment: int) -> int:
        from delta.tables import DeltaTable

        self._ensure_table()
        from . import _names

        delta_table = DeltaTable.forName(self._spark, _names.delta_name(self._spark, self._table))
        advance = count * increment
        max_attempts = 10
        for _ in range(max_attempts):
            rows = (
                self._spark.table(self._table)
                .filter(f"sequence_name = '{name}'")
                .collect()
            )
            if not rows:
                first_value = self._start
                new_current = self._start + advance - increment
                try:
                    self._spark.sql(
                        f"INSERT INTO {self._table} VALUES ('{name}', {new_current})"
                    )
                    return first_value
                except Exception:
                    # Lost the race to create the row -- another writer
                    # got there first; fall through and retry as an
                    # update against whatever is there now.
                    continue
            current = rows[0]["current_value"]
            first_value = current + increment
            new_current = current + advance
            (
                delta_table.alias("t")
                .merge(
                    self._spark.createDataFrame(
                        [(name, current)], "sequence_name STRING, current_value LONG"
                    ).alias("s"),
                    f"t.sequence_name = s.sequence_name AND t.current_value = {current}",
                )
                .whenMatchedUpdate(set={"current_value": f"{new_current}"})
                .execute()
            )
            # Re-read to confirm the CAS actually landed (another writer
            # may have updated between the read above and this merge).
            confirmed = (
                self._spark.table(self._table)
                .filter(f"sequence_name = '{name}'")
                .collect()
            )
            if confirmed and confirmed[0]["current_value"] == new_current:
                return first_value
            # Lost the race -- retry from a fresh read.
        raise RuntimeError(
            f"DeltaSequenceBackend.reserve_block('{name}') did not converge "
            f"after {max_attempts} CAS attempts -- too much contention"
        )


class AdwSequenceBackend(SequenceBackend):
    """Wraps a real Oracle ``SEQUENCE`` via a driver-side connection.

    Requires the sequence object to already exist in the target schema
    (``CREATE SEQUENCE {name} INCREMENT BY {increment} ...`` is DDL and is
    out of scope for this call -- provisioning it is a deployment-time
    concern, not something a running notebook cell should be doing).

    Reserves a block by issuing a single round trip that pulls ``count``
    consecutive ``NEXTVAL`` calls via ``CONNECT BY LEVEL <= :count``. This
    gives ``count`` values that are consecutive *as returned to this
    caller*; if the underlying sequence is also being advanced by another
    concurrent session, this caller's block is still gap-free and
    collision-free (Oracle's ``NEXTVAL`` itself is what guarantees
    uniqueness), it just may not be contiguous with what came immediately
    before it from another session's perspective -- which is exactly the
    same "gaps allowed, collisions not" contract :class:`SequenceBackend`
    documents.

    UNVERIFIED -- no live ADW connection in this environment. The
    ``oracledb`` usage here assumes a connection object with the
    ``python-oracledb`` cursor API (``.cursor()``, ``.execute()``,
    ``.fetchall()``); it is not imported at module load, only used
    through the connection the caller supplies.
    """

    def __init__(self, connection: Any):
        self._connection = connection

    def reserve_block(self, name: str, count: int, increment: int) -> int:
        if count <= 0:
            raise ValueError(f"count must be positive, got {count}")
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                f"SELECT {name}.NEXTVAL FROM dual "
                f"CONNECT BY LEVEL <= :n",
                {"n": count},
            )
            values = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
        if len(values) != count:
            raise RuntimeError(
                f"AdwSequenceBackend.reserve_block('{name}') expected "
                f"{count} values, got {len(values)}"
            )
        return values[0]


_BACKEND_FACTORIES = {
    "delta": lambda spark, **kw: DeltaSequenceBackend(spark, **kw),
    "adw": lambda spark, **kw: AdwSequenceBackend(**kw),
}


def get_sequence_backend(target_catalog_type: str, spark: Any = None, **kwargs: Any) -> SequenceBackend:
    """Resolve a :class:`SequenceBackend` for an explicit catalog type.

    Mirrors ``write_strategies.get_write_strategy``: ``target_catalog_type``
    is always an explicit input, never inferred from a table name or
    connection string. For ``"adw"``, pass ``connection=<oracledb connection>``
    via ``kwargs``; for ``"delta"``, ``spark`` is required and ``table=``
    may be passed via ``kwargs`` to override the default counter table
    name.
    """
    key = (target_catalog_type or "delta").strip().lower()
    try:
        factory = _BACKEND_FACTORIES[key]
    except KeyError:
        raise ValueError(
            f"Unknown target_catalog_type {target_catalog_type!r} -- expected "
            f"one of {sorted(_BACKEND_FACTORIES)}. This is never inferred "
            f"from a table name or connection string; pass it explicitly."
        ) from None
    if key == "delta":
        if spark is None:
            raise ValueError("target_catalog_type='delta' requires spark=<SparkSession>")
        return factory(spark, **kwargs)
    if key == "adw":
        connection = kwargs.pop("connection", None)
        if connection is None:
            raise ValueError("target_catalog_type='adw' requires connection=<oracledb connection>")
        return factory(spark, connection=connection, **kwargs)
    raise AssertionError("unreachable")  # pragma: no cover


@dataclass
class _CacheBlock:
    next_value: int
    remaining: int


class Sequence:
    """User-facing handle: hands out values from an in-memory cache
    block, refilling via ``backend.reserve_block()`` when the block is
    exhausted. This is the "cache size" half of Informatica's Sequence
    Generator -- a bigger cache means fewer round trips to the backend
    (and a bigger potential gap if the job dies mid-cache), exactly the
    same trade-off Informatica's own cache-size setting makes.

    Coupling note: ``start`` only takes effect for a brand-new (never
    before reserved) persisted counter, exactly like Informatica's own
    "Start Value" -- irrelevant once a counter has already advanced. The
    :func:`sequence` factory function keeps a ``DeltaSequenceBackend``'s
    own ``start`` in sync with this class's automatically; constructing
    ``Sequence`` directly against a hand-built backend is the caller's
    responsibility to keep the two consistent (the ADW backend has no
    ``start`` at all -- see :func:`sequence`'s docstring).
    """

    def __init__(
        self,
        backend: SequenceBackend,
        name: str,
        start: int = 1,
        increment: int = 1,
        cache: int = 1000,
        cycle: bool = False,
        max_value: Optional[int] = None,
    ):
        if cache < 1:
            raise ValueError(f"cache must be >= 1, got {cache}")
        if increment == 0:
            raise ValueError("increment must not be 0")
        self._backend = backend
        self._name = name
        self._start = start
        self._increment = increment
        self._cache = cache
        self._cycle = cycle
        self._max_value = max_value
        self._block: Optional[_CacheBlock] = None

    def _refill(self) -> None:
        first = self._backend.reserve_block(self._name, self._cache, self._increment)
        self._block = _CacheBlock(next_value=first, remaining=self._cache)

    def next_value(self) -> int:
        """Return the next value, refilling the cache from the backend
        when exhausted, and applying cycle/exhaustion rules against
        ``max_value``.

        Cycle implementation note (the ambiguous point flagged in the
        module docstring): the backend's persisted counter is NEVER
        reset -- it keeps climbing monotonically forever, which is what
        keeps restart-safety and cross-process uniqueness intact.
        ``cycle=True`` instead remaps the delivered value back into
        ``[start, max_value]`` via modulo arithmetic before returning it.
        This is the common case (a bounded code range that's allowed to
        repeat) and is simple and restart-safe, but it does NOT give two
        independently-restarted jobs the identical wraparound sequence in
        lockstep (that would require resetting the backend counter
        itself, which reintroduces the collision risk this module exists
        to avoid) -- documented gap, not silently guessed.
        """
        if self._block is None or self._block.remaining == 0:
            self._refill()
        assert self._block is not None
        value = self._block.next_value
        self._block.next_value += self._increment
        self._block.remaining -= 1
        if self._max_value is not None and value > self._max_value:
            if not self._cycle:
                raise SequenceExhausted(
                    f"sequence '{self._name}' exhausted: next value {value} "
                    f"exceeds max_value={self._max_value} and cycle=False "
                    f"(mirrors Informatica's documented no-cycle behavior: "
                    f"the session fails rather than silently wrapping)"
                )
            span = self._max_value - self._start + 1
            if span <= 0:
                raise ValueError("max_value must be >= start for cycle=True")
            value = self._start + ((value - self._start) % span)
        return value

    def next_values(self, n: int) -> list[int]:
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n}")
        return [self.next_value() for _ in range(n)]

    def assign(self, df: Any, column: str) -> Any:
        """Add ``column`` to ``df`` with contiguous sequence values in a
        stable row order.

        Reserves exactly ``df.count()`` values as ONE block from the
        backend (one round trip; forces a count action on ``df``, which
        is unavoidable because the reservation size must be known first)
        and assigns them as ``first + (row_number - 1) * increment`` --
        a column expression evaluated on the executors.

        The previous implementation materialised every value in a Python
        list on the driver and shipped it back as a DataFrame to join on:
        for a fact table that is ``df.count()`` Python ints and a
        ``createDataFrame`` of the same size on the driver -- an
        out-of-memory failure on exactly the tables a Sequence Generator
        keys. The ``row_number()`` window is still a single global
        window (the price of contiguous values); ``max_value``/``cycle``
        are applied to the block as a whole. Deferred ``pyspark`` import;
        UNVERIFIED against a live Spark runtime.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        n = df.count()
        if n == 0:
            return df.withColumn(column, F.lit(None).cast("long"))
        first = self._backend.reserve_block(self._name, n, self._increment)
        last = first + (n - 1) * self._increment
        if self._max_value is not None and last > self._max_value and not self._cycle:
            raise SequenceExhausted(
                f"sequence '{self._name}' exhausted: assigning {n} values from "
                f"{first} reaches {last}, above max_value={self._max_value} and "
                f"cycle=False (mirrors Informatica's documented no-cycle "
                f"behavior: the session fails rather than silently wrapping)"
            )
        value = F.lit(first) + (
            F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1
        ) * F.lit(self._increment)
        if self._max_value is not None and self._cycle:
            span = self._max_value - self._start + 1
            if span <= 0:
                raise ValueError("max_value must be >= start for cycle=True")
            value = F.lit(self._start) + ((value - F.lit(self._start)) % F.lit(span))
        return df.withColumn(column, value.cast("long"))


def sequence(
    spark: Any,
    name: str,
    target_catalog_type: str = "delta",
    start: int = 1,
    increment: int = 1,
    cache: int = 1000,
    cycle: bool = False,
    max_value: Optional[int] = None,
    **backend_kwargs: Any,
) -> Sequence:
    """Build a restart-safe :class:`Sequence` for ``name``.

    ``target_catalog_type`` picks the backend (``"delta"`` default, or
    ``"adw"``) and is always explicit, never inferred -- see
    :func:`get_sequence_backend`. Extra ``backend_kwargs`` are forwarded
    to the backend factory (e.g. ``connection=`` for ``"adw"``, ``table=``
    to override the Delta counter table name).
    """
    key = (target_catalog_type or "delta").strip().lower()
    if key == "delta":
        # The Delta backend's "first ever reservation" value must agree
        # with the Sequence-level `start` -- forwarded here so a brand
        # new counter begins exactly where the caller asked, rather than
        # the backend's own separate default. (Not forwarded for "adw":
        # a real Oracle SEQUENCE's start value is fixed at CREATE
        # SEQUENCE time, a deployment-time DBA decision this call has no
        # business overriding.)
        backend_kwargs.setdefault("start", start)
    backend = get_sequence_backend(target_catalog_type, spark=spark, **backend_kwargs)
    return Sequence(
        backend, name, start=start, increment=increment, cache=cache,
        cycle=cycle, max_value=max_value,
    )
