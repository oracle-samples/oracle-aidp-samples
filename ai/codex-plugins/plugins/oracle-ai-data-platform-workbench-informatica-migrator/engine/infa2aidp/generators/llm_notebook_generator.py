"""LLM-First Notebook Generator.

Sends the entire mapping specification (sources, targets, transformations,
connectors) to the LLM as a single prompt and receives back a complete
PySpark notebook. The LLM handles the DAG, variable scoping, branching,
and all transformation logic — no rule-based converters in the primary path.

Rule-based converters are used ONLY as fallback when LLM is unavailable.

Architecture (format-agnostic from the parser onward — see):
  PowerCenter XML  -\\
                      -> Parser -> Canonical Mapping Spec (JSON) -> LLM
  IICS/IDMC JSON   -/            -> Generated Notebook

The LLM only ever sees the canonical spec built by ``_mapping_to_spec()``
below, never the raw export -- so it never knows or needs to know which of
the two source formats produced a given mapping. ``source_fidelity.py``
is the one place that still looks at the raw export, specifically because
everything else in this chain (including the validator) does not.
"""

import json
import logging
from datetime import datetime
from typing import Optional

from ..models import (
    Connector,
    FieldMapping,
    Mapping,
    Session,
    SourceDefinition,
    TargetDefinition,
    Transformation,
    TransformationType,
)

logger = logging.getLogger(__name__)


def _extract_text(message) -> str:
    """Return the first text block's content.

    Opus 5 runs adaptive thinking by default, so message.content[0] is a
    `thinking` block, not text - indexing [0].text raises AttributeError.
    Never return "" on a miss: a silent empty generation is worse than a
    loud failure (spec section 11).
    """
    for block in message.content:
        if getattr(block, "type", None) == "text":
            return block.text
    raise RuntimeError(
        f"No text block in response; got {[getattr(b, 'type', '?') for b in message.content]}"
    )


# The system prompt that instructs Claude how to generate PySpark notebooks
#
# The Class C constructs (sequence generator, lookup,
# $$PARAM/$$$SessStartTime resolution, SCD Type-2, update strategy) are
# owned by the tested `infa_compat` library (engine/infa_compat/), not by
# prose here. This prompt teaches the model to CALL infa_compat.X(...)
# with the real signatures -- copied from engine/infa_compat/__init__.py's
# public API and engine/infa_compat/SUPPORTED_OPERATIONS.md's closed
# contract, never retyped from memory. tests/test_prompt_calls_infa_compat.py
# imports the real functions and fails if this text ever drifts from their
# actual signatures. Class A (pure scalar expressions -- IIF, DECODE, NVL,
# casts, date-format masks, aggregates) stays prose here, per the design
# split: those are emitted inline by the deterministic compiler and
# legitimately need no library call.
_LLM_SYSTEM_PROMPT = """You are an expert Informatica PowerCenter to PySpark migration engineer.

You receive a complete Informatica mapping specification in JSON format and must generate
a production-ready PySpark notebook that faithfully replicates the mapping's ETL logic.

SOURCE FORMAT:
This spec was normalized from either a PowerCenter XML export or an IICS/IDMC cloud JSON
export -- the "## Source Format" note in the user message tells you which. You don't need
to treat them differently; both are already flattened into the same fields below:
  - "direction": INPUT / OUTPUT / INPUT_OUTPUT / VARIABLE, for every port on every
    transformation. PowerCenter marks this via a TRANSFORMFIELD's PORTTYPE attribute; IICS
    marks it via a per-field portType. VARIABLE ports (row-ordered, may be referenced by
    later ports) are the same concept in both formats.
  - "group_by_fields" (Aggregator) and the per-field "is_master" flag (Joiner) carry
    PowerCenter's TABLEATTRIBUTEs / MASTER-ISMASTER TRANSFORMFIELD attributes, or IICS's
    groupBy/isGroupBy and master/isMaster JSON flags -- already resolved to the same
    booleans/lists either way. Use them as given; don't guess from field order.

CRITICAL RULES — FOLLOW EXACTLY:

CLASS C SEMANTICS — CALL infa_compat, NEVER HAND-ROLL (READ THIS FIRST):
0. Sequence Generators, Lookups, $$PARAM/$$$SessStartTime resolution, SCD
   Type-2 effective-dating, and Update Strategy (DD_INSERT/DD_UPDATE/
   DD_DELETE/DD_REJECT) routing are NOT hand-written PySpark. `infa_compat`
   is a tested library already installed on the AIDP cluster -- import it
   with `import infa_compat` and CALL these functions with the arguments
   the mapping gives you. Do NOT reimplement their internals: no
   F.monotonically_increasing_id() surrogate keys, no hand-rolled MERGE for
   SCD2 or DD_DELETE, no spark.conf.get for $$PARAM. This is a CLOSED
   API -- every function you may call under the `infa_compat.` name is
   listed below and in engine/infa_compat/SUPPORTED_OPERATIONS.md; calling
   anything else there is a fabrication.

   infa_compat.sequence(spark, name, target_catalog_type="delta", start=1,
       increment=1, cache=1000, cycle=False, max_value=None, **backend_kwargs) -> Sequence
     Sequence.next_value() -> int  |  Sequence.next_values(n) -> list[int]
     Sequence.assign(df, column) -> DataFrame

   infa_compat.cached_lookup(df, lookup_df, on, policy="first", cache="static",
       order_by=None) -> DataFrame
     policy is one of "first"/"last"/"error"/"all" -- always pass it explicitly.
   infa_compat.lookup_join(df, lookup_df, condition, policy="first",
       order_by=None) -> DataFrame
     condition is a Spark SQL predicate over both sides (any operator, so an
     effective-dated lookup works); lookup columns must not share names with
     df's -- select them as <lookup>__<port>. First/Last resolve per input
     row, ordered by order_by (pass the lookup ports in port order).

   infa_compat.load_parameter_file(path) -> dict
   infa_compat.ParameterScope.from_file(path) -> ParameterScope
   infa_compat.activate(scope)                              # once per session
   infa_compat.param(name, default=None, *, scope=None) -> Any
   infa_compat.sess_start_time(*, scope=None) -> datetime.datetime

   infa_compat.scd2_merge(spark, target, source, keys, effective_from,
       effective_to, current_flag, high_date="9999-12-31", *,
       target_catalog_type="delta", current_flag_values=("Y", "N"),
       jdbc_options=None, adw_connection=None, staging_table=None) -> Scd2MergeResult

   infa_compat.apply_update_strategy(df, strategy_col="DD_STRATEGY") -> UpdateStrategyResult
     -> .inserts / .updates / .deletes / .rejects (four lazy DataFrame filters)
     strategy_col MUST hold the INTEGER UpdateStrategyCode values (0/1/2/3),
     NEVER the "DD_INSERT"/"DD_UPDATE"/"DD_DELETE"/"DD_REJECT" STRING names --
     see the UPDATE STRATEGY section below for why this is now enforced.
   infa_compat.write_update_strategy(result, target, keys, reject_sink, *,
       target_catalog_type="delta", spark=None, jdbc_options=None,
       adw_connection=None, staging_table=None, update_columns=None) -> None
     update_columns: the target columns DD_UPDATE sets (the connected ones);
     omitted, every column is set.
     reject_sink is REQUIRED (no default) -- rejects are never dropped.

   The full closed contract, with every argument explained, is in
   engine/infa_compat/SUPPORTED_OPERATIONS.md. If a mapping needs a Class C
   semantic not listed there, say so in a comment -- do not improvise Spark
   for it.

DATA FLOW:
1. Follow the CONNECTOR graph EXACTLY — it defines which transform feeds which.
   Do NOT guess the order from the transformation list.
2. Each source gets its own DataFrame (df_source, df_source_1, etc.)
3. Parallel branches must use SEPARATE DataFrame variables until they merge.

ROUTER (MOST IMPORTANT — THIS IS WHERE TOOLS FAIL):
4. After a Router, EVERY downstream transform operates on its SPLIT DataFrame
   (e.g., df_valid, df_suspicious, df_archive). NEVER reassign split DFs back to df.
5. Use CONNECTOR graph to determine which Router group feeds which downstream transform.
   Router → UPD_INSERT_VALID means df_valid_* feeds that path.
   Router → EXP_ALERT_REASON means the alert enrichment path.
6. Apply enrichment expressions (EXP_ALERT_REASON, EXP_REJECT_REASON, EXP_ERROR_REASON)
   to the CORRECT split DataFrame, not to df.
7. Each target write must use the CORRECT split DataFrame and select ONLY that target's columns.

AGGREGATOR AS SIDE-BRANCH (COMMON PATTERN):
8. When an Aggregator's output feeds a Lookup (check CONNECTORS), it is a SIDE-BRANCH.
   Compute the aggregation into a SEPARATE DataFrame (e.g., df_daily_agg).
   Then JOIN it back to the main pipeline df. Do NOT overwrite df with the grouped result.
   Alternative: use a Window function (SUM() OVER PARTITION BY) to add the aggregated
   column inline without a separate DataFrame.

SEQUENCE GENERATOR:
9. Sequence Generators are SIDE-INPUTS. Call infa_compat.sequence() and its
   .assign() — do NOT hand-roll a counter with F.monotonically_increasing_id()
   or row_number():
     _seq = infa_compat.sequence(spark, "<TRANSFORMATION_NAME>",
                                  target_catalog_type="delta",
                                  start=<"Current Value" from properties>,
                                  increment=<"Increment Value" from properties>)
     df = _seq.assign(df, "<output_port_name>")
   infa_compat.sequence() owns cache size, restart-safety, and cycle/gap
   behavior already — do not reimplement any of it inline.

EXPRESSIONS:
10. Use F.add_months() not ADD_MONTHS(), F.datediff() not DATE_DIFF()
11. TO_CHAR(number) → .cast('string'), TO_CHAR(date, fmt) → F.date_format()
12. TO_DATE → F.to_timestamp() for type consistency
13. .isin() takes plain Python values, NOT F.lit() wrapped
14. DECODE(val, match1, result1, match2, result2, default) → F.when() chain

LOOKUP:
15. Lookup output columns: alias with LKP_ prefix to avoid ambiguity with source columns.
16. Read actual column names from EXPRESSION attribute (e.g., EXPRESSION="TABLE.COL" → COL).
17. LKP_ columns must be renamed to short names if downstream transforms reference them without prefix.
18. If a Lookup's data comes from an Aggregator side-branch, join that df — don't read an external table.
19. Do NOT hand-roll the multiple-match policy with .dropDuplicates() — call
    infa_compat.cached_lookup(df, lookup_df, on=key_columns, policy="first")
    instead (Informatica's default "Use First Value"; pass policy="last"/
    "error"/"all" if the Lookup's own "Policy on Multiple Match" property
    says otherwise). It is always a left join — input rows are preserved,
    matching a connected Lookup's pass-through behavior — and it enforces
    one policy everywhere instead of a hand-written dropDuplicates() getting
    the tie-break order right in one mapping and wrong in the next.

TARGET WRITES:
19. Use natural keys (SOURCE_SYSTEM + SOURCE_ORDER_ID, CLAIM_ID) for Delta MERGE — NOT surrogate keys.
20. DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT routing: call
    infa_compat.apply_update_strategy() + infa_compat.write_update_strategy()
    — see the "UPDATE STRATEGY" section below. Do NOT hand-write per-code
    MERGE branches for this.
23. Multi-target: select ONLY the target's defined columns before each write.
24. Add LOAD_DATE, CURRENT_FLAG, audit columns BEFORE Router splits so all paths get them.

SOURCE READS:
25. ALL sources are accessed via spark.table("catalog.schema.table") — 3-part naming.
    Sources are registered as external catalogs in AIDP. NO JDBC connections needed.
    NO hardcoded credentials. NO spark.read.format("jdbc"). Just spark.table().
26. WHERE clauses with $$PARAM → do NOT resolve via spark.conf.get(). Load
    the session's .prm file once per notebook and resolve through
    infa_compat's scope-precedence loader instead:
      import infa_compat
      _scope = infa_compat.ParameterScope.from_file(PRM_PATH)
      _scope.mark_session_start()   # fixes $$$SessStartTime once, for sess_start_time() below
      infa_compat.activate(_scope)
      _resolved = infa_compat.param("PARAM_NAME", default="default")
      df = df.filter(F.col("DATE_COL") >= F.lit(_resolved))
    This applies workflow > worklet > session > mapping precedence — a
    plain spark.conf.get() has no notion of that precedence and silently
    picks whichever value happens to be set, which is wrong whenever a
    workflow-level override exists.

JAVA TRANSFORMATIONS:
27. Port the Java logic to PySpark. Java risk/score computations are ALWAYS ADDITIVE:
    Each condition ADDS points to a running score, then the total is thresholded.
    CORRECT pattern:
      _score = (F.when(cond1, points1).otherwise(0)
              + F.when(cond2, points2).otherwise(0)
              + F.when(cond3, points3).otherwise(0))
      df = df.withColumn("RISK_SCORE", F.when(_score >= 60, "HIGH").when(_score >= 30, "MEDIUM").otherwise("LOW"))
    WRONG pattern (DO NOT USE):
      F.when(cond1 & cond2 & cond3, "HIGH")  ← This requires ALL conditions true
    Each if-block in the Java code ADDS to score independently. They are NOT AND conditions.

STORED PROCEDURES:
28. Add placeholder output columns with F.lit("N") or F.lit(0) and a TODO comment.

SCD DIMENSIONS — SURROGATE KEY GENERATION (CRITICAL — MUST DO THIS):
28a. SCD dimensions typically have a NOT-NULL surrogate primary key column
     (its name comes from the target's own field definitions — never assume
     a fixed name, e.g. a column ending in _SK or _KEY, or whatever the
     target declares). Informatica often generates this via an IMPLICIT
     Sequence Generator (part of the dimension mapplet) that the spec may
     not list as a visible transformation, but the target column REQUIRES it.

     Do NOT hand-roll this with F.monotonically_increasing_id() or
     Window.orderBy(F.lit(1)) — both are exactly the restart-unsafe /
     non-contiguous failure modes infa_compat.sequence() exists to avoid.
     Call it instead, for INSERTs (new dimension members) only:
       _sk_seq = infa_compat.sequence(
           spark, "<TARGET_TABLE>_<surrogate_key_column>_SEQ",
           target_catalog_type="delta",
           # start only matters the FIRST TIME this sequence name is ever
           # used — compute the existing max key once so a brand-new
           # counter doesn't restart at 1 against a table that already has
           # rows:
           start=(spark.table("catalog.schema.<TARGET_TABLE>")
                       .agg(F.max("<surrogate_key_column>").alias("m"))
                       .collect()[0]["m"] or 0) + 1,
       )
       df_inserts = _sk_seq.assign(df_inserts, "<surrogate_key_column>")

     For UPDATEs (matched rows), keep the EXISTING surrogate key from the
     target — DO NOT regenerate. Use whenMatchedUpdate with specific columns,
     never whenMatchedUpdateAll (which would overwrite the key).

28c. EFFECTIVE_FROM_DT / EFFECTIVE_TO_DT / CURRENT_FLG (SCD Type-2 columns):
     do NOT hand-stamp these with current_timestamp()/spark.conf.get() —
     infa_compat.scd2_merge() (rule 28j, below) stamps all three on the new
     row and expires the prior one in a single call. When a source mapplet
     already computes SRC_EFFECTIVE_FROM_DT / SRC_EFFECTIVE_TO_DT, carry
     those through as the `effective_from`/`effective_to` column names you
     pass to that call — do NOT hardcode dates.

MAPPLETS (mplt_*):
28d. When a transformation references a mapplet (e.g., mplt_CustomerDimension,
     mplt_ComputeAuditColumns, mplt_SCD_Type2), and the mapplet's internal
     transformations are in scope:
     - EXPAND the mapplet's logic inline. Treat its internal expressions and
       lookups as if they were directly in the mapping.
     - Do NOT skip the mapplet by passing inputs through to outputs unchanged.
       That silently drops customization logic and produces wrong column values.
     - If the mapplet's internal transformations are NOT in the spec, emit
       explicit comments naming the mapplet so the operator knows where the
       gap is — never bypass silently.

UPDATE STRATEGY — DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT (CALL infa_compat, DO NOT HAND-ROLL):
28i. Do NOT write per-branch Delta MERGE calls or a bespoke DeltaTable.merge()
     for DD_DELETE by hand (its target is easy to get backwards — see the
     DD_DELETE note further below on finding it from the CONNECTOR graph).
     Call the two-step API instead:

       import infa_compat
       result = infa_compat.apply_update_strategy(df, strategy_col="DD_STRATEGY")
       infa_compat.write_update_strategy(
           result,
           target="catalog.schema.<TARGET_TABLE_FROM_CONNECTORS>",
           keys=natural_key_columns,          # natural/business key, never surrogate
           reject_sink="catalog.schema.<TARGET_TABLE>_REJECTS",  # REQUIRED -- rejects are never dropped
       )

     `strategy_col` ("DD_STRATEGY" above) MUST be built as the INTEGER
     `infa_compat.UpdateStrategyCode` value, NEVER the Informatica
     expression-language STRING name. Build it like this:

       # RIGHT:
       df = df.withColumn(
           "DD_STRATEGY",
           F.when(<delete_cond>, F.lit(int(infa_compat.UpdateStrategyCode.DD_DELETE)))
            .when(<reject_cond>, F.lit(int(infa_compat.UpdateStrategyCode.DD_REJECT)))
            .otherwise(F.lit(int(infa_compat.UpdateStrategyCode.DD_INSERT))),
       )

       # WRONG — DO NOT DO THIS. "DD_INSERT"/"DD_UPDATE"/"DD_DELETE"/
       # "DD_REJECT" look like the natural values to write here because
       # they're Informatica's own names, but they are STRING labels.
       # apply_update_strategy() filters strategy_col against the INTEGER
       # codes, so a string-typed column matches NONE of the four filters —
       # it now raises infa_compat.NonIntegerStrategyColumnError instead of
       # (as it used to) silently returning four empty partitions, but the
       # fix is still to never emit this in the first place:
       df = df.withColumn("DD_STRATEGY", F.when(<delete_cond>, F.lit("DD_DELETE")).otherwise(F.lit("DD_INSERT")))

     `target` MUST be the table the Update Strategy is CONNECTED TO via the
     CONNECTOR graph (Router → UPD_xxx → TARGET_TABLE) — not the source
     table, and not a guess. `write_update_strategy` already implements:
       - DD_INSERT → merge-insert (idempotent) or append
       - DD_UPDATE → whenMatchedUpdateAll(), no insert clause (a no-op for a
         non-existent key, never silently turned into an insert)
       - DD_DELETE → whenMatchedDelete() against the CONNECTED target only
       - DD_REJECT → routed to `reject_sink`, matching Informatica's Update
         Strategy "Forward Rejected Rows = YES" — never silently filtered out
     Do not reimplement any of these branches by hand.

SCD TYPE-2 EFFECTIVE-DATING (EFFECTIVE_FROM/TO, CURRENT_FLG, close-out):
28j. Do NOT hand-write the expire-then-insert MERGE for a Type-2 change —
     call infa_compat.scd2_merge(). It does BOTH steps (expire the prior
     current row, insert the new one) in one call:

       _high_date = infa_compat.param("HIGH_DATE", default="9999-12-31")
       # Informatica's own $$HIGH_DATE parameter is exactly this call's
       # high_date= argument.
       infa_compat.scd2_merge(
           spark,
           target="catalog.schema.<TARGET_TABLE>",
           source=df_type2_changes,        # ONLY new+changed keys -- see the
                                            # NULL-safe equality rule below
           keys=natural_key_columns,       # natural/business key, NEVER the surrogate
           effective_from="EFFECTIVE_FROM_DT",
           effective_to="EFFECTIVE_TO_DT",
           current_flag="CURRENT_FLG",
           high_date=_high_date,
           current_flag_values=("Y", "N"),  # flip to ("1", "0") if the mapping uses that convention
       )

     `source` must already be filtered to ONLY the rows needing a new
     current version (new keys + keys whose tracked attributes changed) —
     that filtering is the Lookup+comparison step described in the NULL-safe
     equality rule below, not something scd2_merge re-derives. Hand-writing
     this MERGE yourself is the most common bug in hand-coded SCD-2 PySpark
     migrations from Informatica — call the library instead.

INFORMATICA NULL COMPARISON (CRITICAL for change-detection):
28k. Informatica's '=' and '!=' follow three-valued logic, the SAME as Spark:
       Informatica:   NULL != 'X'   →   NULL, and IIF(NULL, 'Y', 'N') → 'N'
       Spark/SQL:     NULL != 'X'   →   NULL  (three-valued logic)
     So a change-detection expression like
       SYSTEMS_COLS_DIFF = IIF(SRC_X != LKP_X OR SRC_Y != LKP_Y, 'Y', 'N')
     does NOT flag a NULL-vs-value change in the source system either --
     translate '!=' as Spark '!=' and '=' as '=='. Use eqNullSafe ONLY when
     the Informatica expression itself is NULL-safe (wraps the operands in
     ISNULL()/NVL(), or compares IS_NULL flags) -- emitting eqNullSafe for a
     plain '!=' produces Type-2 rows the original mapping never produced.

     Use eqNullSafe and invert it:
       def _diff(a, b):
           # 'Y' if values differ (NULL counts as different from non-NULL)
           return F.when(F.col(a).eqNullSafe(F.col(b)), F.lit('N')).otherwise(F.lit('Y'))
       df = df.withColumn(
           "SYSTEMS_COLS_DIFF",
           F.when((_diff("SRC_X","LKP_X")=='Y') | (_diff("SRC_Y","LKP_Y")=='Y'),
                  F.lit('Y')).otherwise(F.lit('N'))
       )
     This diff is exactly what feeds `source=` in the SCD Type-2 rule above
     (28j, infa_compat.scd2_merge()) — filter to SYSTEMS_COLS_DIFF == 'Y'
     (plus genuinely new keys) before passing the DataFrame to that call.

SESSSTARTTIME vs current_timestamp (per-row timestamps are a bug):
28l. Informatica's SESSSTARTTIME is a SINGLE batch-level timestamp — all
     rows in the same session get the same value. F.current_timestamp() in
     PySpark evaluates per-row, so two rows in the same batch can get
     different timestamps (microseconds apart). For audit columns
     (e.g. LOAD_TS, UPDATED_TS, EFFECTIVE_FROM_DT for the SCD batch),
     this breaks downstream reconciliation that groups by load timestamp.

     Do NOT capture this by hand with datetime.now() — call
     infa_compat.sess_start_time() (it fixes the timestamp once for the
     session and raises if read before the $$PARAM setup cell's
     `_scope.mark_session_start()` has run — see rule 26 above):
       _SESS_START_TS = infa_compat.sess_start_time()
       _SESS_START    = F.lit(_SESS_START_TS).cast("timestamp")
       df = df.withColumn("LOAD_TS", _SESS_START)
       df = df.withColumn("EFFECTIVE_FROM_DT",
                          F.coalesce(F.col("SRC_EFFECTIVE_FROM_DT"), _SESS_START))

LOOKUP SQL OVERRIDE WITH $$PARAMETER FILTERS (IIF precedence):
28m. Informatica Lookup SQL Override often has conditional filters like:
       SELECT ... FROM TARGET_TABLE WHERE
         IIF('$$LOW_DATE_PARAMETER'='Y', SRC_EFF_FROM_DT=$$LOW_DATE_PARAMETER,
                                          CURRENT_FLG='Y')
     This is NOT an OR — it's an if/else evaluated at session start. Resolve
     the parameter ONCE via infa_compat.param() (never spark.conf.get()),
     then apply the right filter:

       _low_date_flag = infa_compat.param("LOW_DATE_MODE", default="N")
       if _low_date_flag == "Y":
           _lkp = spark.table(TGT_TBL).filter(F.col("SRC_EFF_FROM_DT") == F.lit(LOW_DATE))
       else:
           _lkp = spark.table(TGT_TBL).filter(F.col("CURRENT_FLG") == F.lit("Y"))

     Do NOT translate IIF to .filter(... OR ...) — that's logically broader and
     would return rows the lookup should never see.

UNCONNECTED-LOOKUP FALLBACK CONSTANT (match Informatica NVL exactly):
28n. Informatica NVL(:LKP.LKP_NAME(...), '<Master Code Not Found>') falls
     back to the MASTER_CODE_NOT_FOUND constant when the lookup misses.
     Do NOT fall back to the source code itself — that would mask data
     quality issues and silently use the source value when the master
     value should have been used:
       WRONG: F.coalesce(F.col("LKP_X_MASTER_VALUE"), F.col("SRC_X"))
       OK:    F.coalesce(F.col("LKP_X_MASTER_VALUE"),
                         F.lit(MASTER_CODE_NOT_FOUND))
     For LKP_CODES_GL_SEGMENTS and similar code-translation lookups, when
     the lookup returns NULL the column should reflect that the master
     value was missing — not silently substitute the raw source code.

CURRENCY CONVERSION NULL PROPAGATION (fact-table arithmetic):
28o. When unconnected currency-conversion lookups miss
     (e.g. MPLT_CURCY_CONVERSION_RATES, :LKP.LKP_EXCHANGE_RATE,
     :LKP.LKP_GLOBAL_CURRENCY_RATE), DO NOT default the rate to 1.0 —
     that silently produces WRONG global/document amounts that look
     valid but use a fabricated rate.

     Informatica's session-level arithmetic semantics propagate NULL:
       NULL * AMOUNT = NULL  →  GLOBAL_AMOUNT becomes NULL
       Reconcile catches the NULL; downstream reports highlight the gap.
     Defaulting to 1.0 destroys this signal.

     Correct pattern:
       df = df.withColumn(
           "EXCHANGE_RATE_GLOBAL1",
           F.coalesce(F.col("LKP_RATE_GLOBAL1"),
                      F.lit(None).cast("decimal(28,10)"))
       )
       df = df.withColumn(
           "GLOBAL1_AMOUNT",
           F.col("LOCAL_AMOUNT") * F.col("EXCHANGE_RATE_GLOBAL1")
       )

     Wrong pattern (silent data corruption):
       F.coalesce(F.col("LKP_RATE_GLOBAL1"), F.lit(1.0))   # NEVER

     Apply to ALL of: GLOBAL1_AMOUNT, GLOBAL2_AMOUNT, DOC_AMOUNT,
     EUR_AMOUNT, USD_AMOUNT, BASE_AMOUNT, LOC_AMOUNT, etc., and any
     other multi-currency amount columns. NULL is the signal that the
     rate is unknown — preserve it through the arithmetic.

SQL TRANSFORMATIONS:
29. Translate ~column~ bind syntax to PySpark column references.
30. Replace FROM DUAL with direct computation. Replace Oracle JOINs with PySpark joins.

JOINS:
31. When joining two DataFrames that share column names, ALWAYS alias both sides.
    NEVER use F.col("df.COL") — "df" is a Python variable, not a SQL alias.

JOINER MASTER OUTER (CRITICAL — GET THIS RIGHT):
    In Informatica, "Master Outer" with Master Source = SQ_POLICY means:
    - DETAIL (Claims) is the LEFT side — ALL detail rows are preserved
    - MASTER (Policy) is the RIGHT side — unmatched masters get NULLed out
    - This is: df_claims.join(df_policy, ..., "left")
    - NOT: df_policy.join(df_claims, ..., "left") ← WRONG, this preserves policies

    The DETAIL source is the one NOT named as Master Source.
    The MASTER source IS the one named as Master Source.
    Detail goes on the LEFT of .join(), Master goes on the RIGHT.

DD_DELETE (READ THIS VERY CAREFULLY — COMMON MISTAKE):
32. DD_DELETE's target is the table the Update Strategy is CONNECTED TO via
    CONNECTORS. Look at the CONNECTOR chain:
      Router → UPD_DELETE_xxx → TARGET_TABLE
    The TARGET_TABLE in that chain is where the DELETE happens — NOT the
    source table. Pass that table as `target` to
    infa_compat.write_update_strategy() (see "UPDATE STRATEGY" above, rule
    28i) — do NOT hand-write a DeltaTable.merge() for this.

    DO NOT: append/insert rows anywhere. DO NOT: write to the source table.
    DO NOT: "archive" rows by inserting then deleting. JUST DELETE from target.
    The target table name comes from the CONNECTOR graph, not from guessing.

OTHER:
33. 1=1 conditions in Router mean "all rows" — just assign df, no filter.
34. Normalizer OCCURS: use F.array() + F.explode(), extract _idx as sequence column.
35. Sorter: check SORTDIRECTION attribute — ASCENDING → .asc(), DESCENDING → .desc()
36. Sorter Distinct=YES → bare .dropDuplicates(). PowerCenter's "Distinct
    Output Rows" compares EVERY port (it treats all ports as sort keys), so
    deduplicating on all columns IS the faithful translation. Do not add a
    unique-ID column to the subset -- that makes the dedup a no-op.
38. Do NOT call .count() after .unpersist() — the DataFrame is gone. Count BEFORE unpersist.
39. Do NOT use mergeSchema=true unless the schema is genuinely evolving. Use exact schema matching.

OUTPUT FORMAT:
Return ONLY a JSON array of cell objects. Each cell is:
{"cell_type": "code", "source": "...python code..."}
or
{"cell_type": "markdown", "source": "...markdown..."}

First cell: markdown header with mapping name and description
Second cell: imports (pyspark.sql, functions as F, Window, types)
Third cell: parameters from spark.conf
Then: one cell per major transformation step
Then: validation cell (cache, count, null checks)
Then: target write cell(s) — one per target
Last cell: unpersist and summary

Do NOT include any text outside the JSON array. No explanation, no commentary."""


def _mapping_to_spec(mapping: Mapping, session: Session) -> dict:
    """Convert a Mapping to a clean JSON spec for the LLM prompt."""

    def _src_to_dict(s: SourceDefinition) -> dict:
        return {
            "name": s.name,
            "table_name": s.table_name or s.name,
            "owner": s.owner,
            "db_name": s.db_name,
            "database_type": "Oracle",
            "fields": [
                {"name": f.source_field if isinstance(f, FieldMapping) else f.get("name", ""),
                 "datatype": f.datatype if isinstance(f, FieldMapping) else f.get("datatype", ""),
                 "nullable": f.nullable if isinstance(f, FieldMapping) else f.get("nullable", True)}
                for f in s.fields
            ],
            "sql_query": s.sql_query,
        }

    def _tgt_to_dict(t: TargetDefinition) -> dict:
        return {
            "name": t.name,
            "table_name": t.warehouse_table or t.table_name or t.name,
            "owner": t.owner,
            "db_name": t.db_name,
            "load_strategy": t.load_strategy.value if t.load_strategy else "INSERT",
            "scd_type": t.scd_type,
            "fields": [
                {"name": f.target_field if isinstance(f, FieldMapping) else f.get("target_field", f.get("name", "")),
                 "datatype": f.datatype if isinstance(f, FieldMapping) else f.get("datatype", ""),
                 "is_key": f.is_key if isinstance(f, FieldMapping) else f.get("is_key", False),
                 "nullable": f.nullable if isinstance(f, FieldMapping) else f.get("nullable", True)}
                for f in t.fields
            ],
        }

    def _tx_to_dict(tx: Transformation) -> dict:
        d = {
            "name": tx.name,
            "type": tx.type.value,
            "fields": [
                {"name": f.name, "datatype": f.datatype, "direction": f.direction.value,
                 "expression": f.expression, "precision": f.precision, "scale": f.scale}
                for f in tx.fields
                if hasattr(f, 'name')
            ],
            "properties": {k: v for k, v in tx.properties.items() if k != "reusable"},
        }
        if tx.sql_override:
            d["sql_override"] = tx.sql_override
        if tx.lookup_table:
            d["lookup_table"] = tx.lookup_table
        if tx.lookup_condition:
            d["lookup_condition"] = tx.lookup_condition
        if tx.lookup_sql:
            d["lookup_sql"] = tx.lookup_sql
        if tx.join_condition:
            d["join_condition"] = tx.join_condition
        if tx.join_type:
            d["join_type"] = tx.join_type
        if tx.filter_condition:
            d["filter_condition"] = tx.filter_condition
        if tx.group_by_fields:
            d["group_by_fields"] = tx.group_by_fields
        if tx.router_groups:
            d["router_groups"] = tx.router_groups
        if tx.sort_keys:
            d["sort_keys"] = tx.sort_keys
        if tx.sort_direction:
            d["sort_direction"] = tx.sort_direction
        if tx.update_strategy_expression:
            d["update_strategy_expression"] = tx.update_strategy_expression
        if tx.start_value != 1:
            d["start_value"] = tx.start_value
        if tx.increment_by != 1:
            d["increment_by"] = tx.increment_by
        return d

    def _conn_to_dict(c: Connector) -> dict:
        return {
            "from": c.from_instance,
            "from_field": c.from_field,
            "to": c.to_instance,
            "to_field": c.to_field,
        }

    return {
        "mapping_name": mapping.name,
        "description": mapping.description,
        "folder": mapping.folder or "Migrated",
        "sources": [_src_to_dict(s) for s in mapping.sources],
        "targets": [_tgt_to_dict(t) for t in mapping.targets],
        "transformations": [_tx_to_dict(tx) for tx in mapping.transformations],
        "connectors": [_conn_to_dict(c) for c in mapping.connectors],
        "parameters": {**mapping.parameters, **session.parameters},
    }


def _detect_source_format(mapping: Mapping) -> str:
    """Best-effort label for which export format produced this mapping.

    The IR is deliberately format-agnostic by the time it reaches this
    generator (that's the whole point -- see the module docstring), so
    this is purely for the prompt's human-readable "## Source Format"
    note; nothing downstream branches on it. The signal used is
    ``xml_parser._parse_transformation`` unconditionally stamping a
    "reusable" key into every transformation's ``properties`` (from the
    XML's REUSABLE attribute, defaulting to False when absent) -- IICS's
    parser never sets that key. ``_mapping_to_spec`` already strips
    "reusable" back out of what the LLM sees, so this check has to run on
    the IR's ``Transformation.properties`` before that happens, not on the
    spec dict.
    """
    for tx in mapping.transformations:
        if "reusable" in tx.properties:
            return "PowerCenter"
    if mapping.transformations:
        return "IICS/IDMC"
    return "unknown"


class LLMNotebookGenerator:
    """Generate complete AIDP notebooks using LLM as the primary code generator.

    The LLM receives the entire mapping specification and produces a complete
    notebook in one shot. Validation and fixing happen afterward.
    """

    def __init__(self, llm_handler):
        """Args:
            llm_handler: LLMHandler instance (from handlers/codellama_handler.py)
        """
        self.llm = llm_handler

    def generate(
        self,
        mapping: Mapping,
        session: Session,
        output_format: str = "ipynb",
        target_score: int = None,
        max_attempts: int = None,
        source_path: str = None,
    ) -> Optional[str]:
        """Generate a complete notebook; score and fidelity are TRIAGE, not
        a regeneration gate.

        Flow:
          1. LLM generates a notebook from the mapping spec (JSON, built by
             ``_mapping_to_spec`` from either a PowerCenter XML or an
             IICS/IDMC JSON export -- the LLM never sees which).
          2. A genuinely unparseable/truncated response is a TRANSPORT
             failure, not a quality signal -- it gets exactly one retry
             (bounded by ``max_attempts``, but never more than one retry
             regardless of how high ``max_attempts`` is set).
          3. Once a response parses, ``llm_validator`` scores it against
             the SAME spec the generator used. That score can never see
             what the parser silently dropped before either of them ran
             (that's the exact defect this task exists to stop repeating
             -- see ``source_fidelity.py``), so a low score no longer
             triggers regeneration. Instead it's recorded
             (``self._last_score``, ``self._last_review_required``) so the
             mapping routes to human review. Feeding the same-spec-derived
             issues back and asking the LLM to "fix" them can't repair a
             gap the spec never described in the first place.
          4. If ``source_path`` is given, ``source_fidelity()`` compares
             the notebook against the RAW export directly -- independent
             of the spec -- and the result is recorded on
             ``self._last_fidelity`` alongside the score, so the report
             carries both the circular signal and the independent one.

        Args:
            mapping: Parsed Informatica mapping
            session: Session metadata
            output_format: "ipynb" (default) or "py"
            target_score: Minimum validation score (0-100) treated as
                "no review needed"
            max_attempts: Upper bound on transport-failure retries (at
                most one retry is ever used regardless of this value)
            source_path: Path to the raw PowerCenter XML / IICS JSON export
                this mapping was parsed from. Optional -- when omitted,
                ``self._last_fidelity`` is left as ``None``.

        Returns:
            Notebook content as string, or None if every attempt produced
            an unparseable response.
        """
        from .llm_validator import LLMMigrationValidator
        from .source_fidelity import source_fidelity
        from .. import config

        # Use config defaults if not explicitly passed
        if target_score is None:
            target_score = config.TARGET_SCORE
        if max_attempts is None:
            max_attempts = config.MAX_ATTEMPTS

        # Exactly one retry for a transport failure (unparseable/truncated
        # response), never more -- regardless of max_attempts. An explicit
        # max_attempts of 1 means "don't even retry that."
        max_transport_attempts = 1 if max_attempts is not None and max_attempts < 2 else 2

        spec = _mapping_to_spec(mapping, session)
        spec_json = json.dumps(spec, indent=2, default=str)
        validator = LLMMigrationValidator(self.llm)

        cells = None
        attempt = 0
        for attempt in range(1, max_transport_attempts + 1):
            logger.info("Generation attempt %d/%d for '%s'",
                         attempt, max_transport_attempts, mapping.name)

            prompt = self._build_prompt(spec_json, mapping)

            try:
                raw_response = self._call_llm(prompt, attempt_num=attempt)
            except Exception as exc:
                logger.error("LLM generation failed (attempt %d): %s", attempt, exc)
                self._last_raw_response = ""
                continue

            if not raw_response:
                self._last_raw_response = ""
                continue

            # Keep raw response on the instance so the caller can save it
            # to debug/ if everything ends up failing.
            self._last_raw_response = raw_response

            cells = self._parse_response(raw_response)
            if cells:
                break
            logger.warning("Attempt %d for '%s': response unparseable (transport "
                           "failure, not a quality issue) -- first 200 chars: %r",
                           attempt, mapping.name, raw_response[:200])

        self._last_attempts = attempt

        if not cells:
            logger.error("All %d attempt(s) produced an unparseable response for "
                         "'%s' -- giving up", max_transport_attempts, mapping.name)
            self._last_score = 0
            self._last_validation = None
            self._last_fidelity = None
            self._last_review_required = True
            return None

        # Post-process DD_DELETE
        cells = self._fix_dd_delete_cells(cells, mapping)

        # "Accepted costs: version skew" -- pin the
        # infa_compat version this notebook was generated against,
        # deterministically (not relying on the LLM to remember it).
        cells = self._ensure_infa_compat_header(cells)

        # Assemble notebook
        if output_format == "ipynb":
            notebook = self._assemble_ipynb(cells, mapping)
        else:
            notebook = self._assemble_py(cells)

        # Score is a TRIAGE SIGNAL now, never a retry gate: it is computed
        # once and recorded, not looped on.
        try:
            result = validator.validate(spec_json, notebook, mapping.name)
            score = result.score
            logger.info("Score for '%s': %d/100 (%d critical, %d warnings) -- "
                        "recorded for review, not a regeneration trigger",
                        mapping.name, score, len(result.critical_issues),
                        len(result.warnings))
        except Exception as exc:
            logger.warning("Validation failed for '%s': %s -- notebook still "
                           "returned, routed to review with score 0", mapping.name, exc)
            result = None
            score = 0

        num_critical = len(result.critical_issues) if result else 0
        num_warnings = len(result.warnings) if result else 0
        num_errors = (
            len([i for i in result.info if "error" in str(i).lower()]) if result else 0
        )

        accepted = (
            result is not None
            and score >= target_score
            and num_critical <= config.MAX_CRITICAL
            and (config.MAX_WARNINGS < 0 or num_warnings <= config.MAX_WARNINGS)
            and num_errors <= config.MAX_ERRORS
        )

        self._last_score = score
        self._last_validation = result
        self._last_review_required = not accepted

        if accepted:
            logger.info("Score %d >= %d, %d critical (<=%d), %d warnings (<=%s) — "
                        "no review required for '%s'",
                        score, target_score, num_critical, config.MAX_CRITICAL,
                        num_warnings, config.MAX_WARNINGS if config.MAX_WARNINGS >= 0 else "∞",
                        mapping.name)
        else:
            logger.info("Score %d/100 below threshold, or issue budget exceeded, for "
                        "'%s' -- routed to the human review gate (NOT regenerated: a "
                        "low score is a quality signal, not a transport failure)",
                        score, mapping.name)

        # Independent, non-circular check: compares the
        # notebook against the RAW export file, never the spec both the
        # generator and validator share above. Computed whenever a source
        # path is available -- not only on rejection -- so the report
        # always carries both signals side by side.
        fidelity = None
        if source_path:
            try:
                fidelity = source_fidelity(source_path, notebook)
                if fidelity.has_gaps:
                    logger.warning("source_fidelity gaps for '%s': %s",
                                   mapping.name, fidelity.summary())
            except Exception as exc:
                logger.warning("source_fidelity check failed for '%s': %s",
                               mapping.name, exc)
        self._last_fidelity = fidelity

        return notebook

    def _build_prompt(self, spec_json: str, mapping: Mapping) -> str:
        """Build the LLM prompt with the mapping specification."""

        # Summarize the data flow for context
        flow_desc = self._describe_flow(mapping)
        source_format = _detect_source_format(mapping)

        return f"""Convert this Informatica mapping to a complete PySpark notebook for Oracle AIDP.

## Source Format
This mapping was originally authored in {source_format}. It has already been normalized
into the JSON spec below -- see the system prompt's SOURCE FORMAT section for how the two
export formats' port-direction, groupBy/master, and TABLEATTRIBUTE-equivalent fields map to
the same JSON keys here. Convert from the spec below; you don't need the original export.

## Mapping Specification (JSON)

```json
{spec_json}
```

## Data Flow Summary
{flow_desc}

## Requirements
- Generate a complete, executable PySpark notebook
- Follow the CONNECTOR graph for data flow — do NOT guess the order
- Each source reads into its own DataFrame (df_source, df_source_1, ...)
- Handle parallel branches with separate DataFrame variables
- After Router splits, NEVER reassign split DFs back to main df
- Use Delta Lake MERGE for target writes (natural keys, not surrogate)
- DD_INSERT = append mode, DD_DELETE = whenMatchedDelete
- DD_STRATEGY column must hold the INTEGER infa_compat.UpdateStrategyCode
  values (0/1/2/3), never the "DD_INSERT"/etc. STRING names — a string
  column raises infa_compat.NonIntegerStrategyColumnError
- Add LOAD_DATE and audit columns BEFORE Router splits
- Select only target-defined columns before each target write
- Resolve $$PARAM via infa_compat.param() (activate the .prm ParameterScope once,
  then call param()) — never spark.conf.get()
- Source Qualifier WHERE clauses: apply as .filter() with the infa_compat.param()-resolved value

Return ONLY a JSON array of cell objects. No other text."""

    def _describe_flow(self, mapping: Mapping) -> str:
        """Generate a human-readable data flow description."""
        from collections import defaultdict
        fwd = defaultdict(set)
        for c in mapping.connectors:
            fwd[c.from_instance].add(c.to_instance)

        lines = []
        for src in mapping.sources:
            lines.append(f"Source: {src.name} ({src.db_name}.{src.owner}.{src.table_name or src.name})")

        for tx in mapping.transformations:
            succs = ", ".join(sorted(fwd.get(tx.name, set())))
            lines.append(f"  {tx.type.value}: {tx.name} → {succs}")

        for tgt in mapping.targets:
            lines.append(f"Target: {tgt.name} ({tgt.db_name}.{tgt.owner}.{tgt.table_name or tgt.name})")

        return "\n".join(lines)

    def _call_llm(self, prompt: str, attempt_num: int = 1) -> str:
        """Call the LLM and return raw response text. Retries on rate limit.

        attempt_num is the OUTER retry counter (1..MAX_ATTEMPTS). On attempts
        2+, an escalating "STRICT JSON" instruction is appended to the prompt
        so Claude knows the previous output was unparseable.

        Note: Opus rejects assistant-message prefill ("conversation must end
        with a user message"), so we rely on prompt-level enforcement.

        Uses the streaming API (messages.stream + get_final_message) rather
        than messages.create: CLAUDE_MAX_TOKENS defaults to 32000, above
        Anthropic's non-streaming ceiling, and notebook generation is
        exactly the long-output case streaming exists for. output_config
        effort=xhigh is Anthropic's documented best setting for coding/
        agentic work on Opus 5. Deliberately does NOT pass thinking={...}
        (Opus 5 runs adaptive thinking when omitted) or budget_tokens/
        temperature/top_p/top_k (all four return HTTP 400 on Opus 5).
        """
        import time

        # Build a strong format directive on every call. Critical to keep
        # parsing failures low — especially for Opus, which tends to wrap
        # output in markdown/prose.
        format_directive = (
            "\n\n=== OUTPUT FORMAT (STRICT) ===\n"
            "Respond with ONLY a JSON array. No markdown fences. No prose.\n"
            "The response MUST start with [ and end with ].\n"
            'Each element MUST be an object: {"cell_type":"code"|"markdown","source":"..."}.\n'
            "If your full response would exceed the max-tokens budget, return\n"
            "fewer-but-complete cells; never truncate mid-string mid-array.\n"
        )

        # On retries, escalate explicitly — tell Claude its previous attempt failed.
        if attempt_num > 1:
            format_directive += (
                "\nCRITICAL: A PREVIOUS ATTEMPT'S RESPONSE COULD NOT BE PARSED.\n"
                "Re-read the format directive above. Output JSON only.\n"
            )

        prompt = prompt + format_directive

        from .. import config as _cfg

        # Pick up the configured max_tokens (default 16384, max 32000 on Opus 5)
        # LLM_* first: this call runs against whichever provider is
        # configured, so reading a CLAUDE_-named setting for an OpenAI
        # call was confusing even though it worked.
        max_tokens_cfg = getattr(_cfg, "LLM_MAX_TOKENS", None) or getattr(
            _cfg, "CLAUDE_MAX_TOKENS", 16384)
        timeout_cfg = getattr(_cfg, "LLM_TIMEOUT_SECONDS", None) or getattr(
            _cfg, "CLAUDE_TIMEOUT_SECONDS", 600)

        max_retries = 3
        for retry in range(max_retries):
            try:
                if hasattr(self.llm, '_claude_client') and self.llm._claude_client:
                    with self.llm._claude_client.with_options(
                        timeout=timeout_cfg
                    ).messages.stream(
                        model=self.llm.claude_model,
                        max_tokens=max_tokens_cfg,
                        output_config={"effort": "xhigh"},
                        system=_LLM_SYSTEM_PROMPT,
                        messages=[{"role": "user", "content": prompt}],
                    ) as stream:
                        message = stream.get_final_message()
                    return _extract_text(message)
                else:
                    # Non-Anthropic provider (the Codex build defaults to
                    # OpenAI). Passing the system prompt and the limits
                    # explicitly matters: calling generate(prompt) bare
                    # would drop the notebook-generation system prompt and
                    # silently use the generic expression one.
                    return self.llm.generate(
                        prompt,
                        system=_LLM_SYSTEM_PROMPT,
                        max_tokens=max_tokens_cfg,
                        timeout=timeout_cfg,
                    ) or ""
            except Exception as exc:
                if "429" in str(exc) or "rate_limit" in str(exc).lower():
                    wait = (retry + 1) * 30  # 30s, 60s, 90s
                    logger.warning("Rate limited — waiting %ds before retry (%d/%d)",
                                   wait, retry + 1, max_retries)
                    time.sleep(wait)
                    continue
                raise
        raise RuntimeError(f"LLM call failed after {max_retries} retries")

    def _parse_response(self, response: str) -> Optional[list[dict]]:
        """Parse LLM response into a list of cell dicts."""
        # Try to extract JSON array from response
        response = response.strip()

        # Remove markdown code fences if present
        if response.startswith("```"):
            # Find the end of the opening fence
            first_newline = response.index("\n")
            last_fence = response.rfind("```")
            if last_fence > first_newline:
                response = response[first_newline + 1:last_fence].strip()

        try:
            cells = json.loads(response)
            if isinstance(cells, list):
                return cells
        except json.JSONDecodeError:
            pass

        # Try to find JSON array in the response
        import re
        match = re.search(r'\[\s*\{.*\}\s*\]', response, re.DOTALL)
        if match:
            try:
                cells = json.loads(match.group())
                if isinstance(cells, list):
                    return cells
            except json.JSONDecodeError:
                pass

        # Last resort: split by cell markers — accept multiple fence variants
        logger.warning("Could not parse LLM response as JSON — attempting cell extraction")
        cells = []
        # Accept ```python, ```py, ```pyspark, or bare ``` fences
        code_blocks = re.findall(
            r'```(?:python|py|pyspark)?\s*\n(.*?)```',
            response, re.DOTALL,
        )
        for block in code_blocks:
            cells.append({"cell_type": "code", "source": block.strip()})

        return cells if cells else None

    @staticmethod
    def _ensure_infa_compat_header(cells: list[dict]) -> list[dict]:
        """Deterministically insert an infa_compat import + version-pin cell.

        Spec S7 ("Accepted costs: version skew") records that a notebook
        generated against one infa_compat release, later run on a cluster
        with a different one installed, must fail LOUDLY at execution time
        rather than silently changing SCD2/lookup/sequence/update-strategy
        semantics. This is inserted here in Python -- not left as prose the
        LLM might forget -- so every LLM-generated notebook carries it
        regardless of what the model actually returned. The pinned version
        is read from THIS process's installed infa_compat (the same one the
        prompt's call instructions were written against), never hand-typed.
        """
        import infa_compat as _infa_compat_runtime

        generated_against = _infa_compat_runtime.__version__
        pin_cell = {
            "cell_type": "code",
            "source": (
                "import infa_compat\n"
                f"_INFA_COMPAT_GENERATED_AGAINST = {generated_against!r}\n"
                "if infa_compat.__version__ != _INFA_COMPAT_GENERATED_AGAINST:\n"
                "    raise RuntimeError(\n"
                "        f\"This notebook was generated against infa_compat \"\n"
                "        f\"{_INFA_COMPAT_GENERATED_AGAINST!r}, but the cluster has \"\n"
                "        f\"{infa_compat.__version__!r} installed. Version skew can \"\n"
                "        f\"silently change SCD2/lookup/sequence/update-strategy \"\n"
                "        f\"semantics -- reinstall the matching infa_compat build \"\n"
                "        f\"before re-running.\"\n"
                "    )"
            ),
        }
        # Insert right after the markdown header cell (cell 0 per the
        # OUTPUT FORMAT contract: "First cell: markdown header ... Second
        # cell: imports"), so the pin fails before any Class C call gets a
        # chance to run with the wrong semantics.
        insert_at = 1 if cells and cells[0].get("cell_type") == "markdown" else 0
        new_cells = list(cells)
        new_cells.insert(insert_at, pin_cell)
        return new_cells

    def _fix_dd_delete_cells(self, cells: list[dict], mapping: Mapping) -> list[dict]:
        """Post-process LLM output to fix DD_DELETE target writes.

        The LLM consistently generates insert+delete-from-source for DD_DELETE,
        but DD_DELETE means ONLY delete matching rows from the connected TARGET.
        This method detects and replaces incorrect DD_DELETE cells.
        """
        from collections import defaultdict

        # Find DD_DELETE update strategies and their target tables
        fwd = defaultdict(set)
        for conn in mapping.connectors:
            fwd[conn.from_instance].add(conn.to_instance)

        delete_targets = {}  # target_name -> (df_var, key_col)
        for tx in mapping.transformations:
            if tx.type != TransformationType.UPDATE_STRATEGY:
                continue
            if "DD_DELETE" not in (tx.update_strategy_expression or "").upper():
                continue
            # Find the target this UPD connects to
            for succ in fwd.get(tx.name, set()):
                for tgt in mapping.targets:
                    if tgt.name == succ:
                        # Build fully qualified table name
                        tgt_parts = []
                        if tgt.db_name:
                            tgt_parts.append(tgt.db_name)
                        if tgt.owner:
                            tgt_parts.append(tgt.owner)
                        tgt_parts.append(tgt.warehouse_table or tgt.table_name or tgt.name)
                        table = ".".join(tgt_parts)
                        # Find key column from target fields
                        key_col = None
                        for f in tgt.fields:
                            fname = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
                            if fname.upper().endswith("_ID"):
                                key_col = fname
                                break
                        if not key_col and tgt.fields:
                            f0 = tgt.fields[0]
                            key_col = f0.target_field if isinstance(f0, FieldMapping) else (f0.get("target_field") or f0.get("name", ""))
                        delete_targets[tgt.name] = (table, key_col or "ID")

        if not delete_targets:
            return cells

        # Find and replace cells that write to DD_DELETE targets
        fixed_cells = []
        for cell in cells:
            source = cell.get("source", "")
            if isinstance(source, list):
                source = "".join(source)

            # The prompt teaches the model to call
            # infa_compat.write_update_strategy() for DD_DELETE instead of
            # hand-writing a DeltaTable.merge() -- trust that call rather
            # than second-guessing it. Without this guard, the substring
            # "write" inside "write_update_strategy" would match the
            # heuristic below and this post-processor would clobber a
            # correct library call with the old hand-rolled Delta MERGE,
            # undoing the prompt fix.
            if "infa_compat" in source:
                fixed_cells.append(cell)
                continue

            # Check if this cell writes to a DD_DELETE target
            replaced = False
            for tgt_name, (table, key_col) in delete_targets.items():
                if tgt_name in source and ("write" in source.lower() or "merge" in source.lower() or "append" in source.lower() or "saveAsTable" in source):
                    # Find the df variable used for this target's Router group
                    df_var = "df_archive_old"
                    # Try to extract from the cell
                    import re
                    df_match = re.search(r'(df_\w+)\s*[.=]', source)
                    if df_match:
                        df_var = df_match.group(1)

                    # Replace with correct DD_DELETE code
                    correct_code = (
                        f"# Target: {tgt_name} — DD_DELETE (delete matching rows from target)\n"
                        f"# DD_DELETE operates on the CONNECTED TARGET, not the source table\n"
                        f"from delta.tables import DeltaTable\n"
                        f"import logging\n"
                        f"logger = logging.getLogger(__name__)\n"
                        f"try:\n"
                        f"    _del_keys = {df_var}.select(\"{key_col}\")\n"
                        f"    if _del_keys.count() > 0:\n"
                        f"        _delta = DeltaTable.forName(spark, "
                        f"{(chr(34) + table + chr(34)) if '.' in table else ('infa_compat.delta_name(spark, ' + chr(34) + table + chr(34) + ')')})\n"
                        f"        _delta.alias(\"t\").merge(\n"
                        f"            _del_keys.alias(\"s\"),\n"
                        f"            \"t.{key_col} = s.{key_col}\"\n"
                        f"        ).whenMatchedDelete().execute()\n"
                        f"        logger.info(f\"Deleted {{_del_keys.count()}} rows from {tgt_name}\")\n"
                        f"    else:\n"
                        f"        logger.info(\"No rows to delete from {tgt_name}\")\n"
                        f"except Exception as e:\n"
                        f"    logger.error(f\"DD_DELETE failed for {tgt_name}: {{e}}\")\n"
                        f"    raise"
                    )
                    fixed_cells.append({"cell_type": "code", "source": correct_code})
                    replaced = True
                    logger.info("Post-processed DD_DELETE cell for target %s", tgt_name)
                    break

            if not replaced:
                fixed_cells.append(cell)

        return fixed_cells

    @staticmethod
    def _assemble_ipynb(cells: list[dict], mapping: Mapping) -> str:
        """Build a Jupyter .ipynb notebook from cell dicts."""
        nb_cells = []
        for cell in cells:
            cell_type = cell.get("cell_type", "code")
            source = cell.get("source", "")
            if isinstance(source, list):
                source_lines = source
            else:
                source_lines = [line + "\n" for line in source.split("\n")]

            nb_cell = {
                "cell_type": cell_type,
                "metadata": {},
                "source": source_lines,
            }
            if cell_type == "code":
                nb_cell["execution_count"] = None
                nb_cell["outputs"] = []

            nb_cells.append(nb_cell)

        notebook = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {
                    "name": "python",
                    "version": "3.10.0",
                },
            },
            "cells": nb_cells,
        }
        return json.dumps(notebook, indent=1, ensure_ascii=False)

    @staticmethod
    def _assemble_py(cells: list[dict]) -> str:
        """Build a .py script from cell dicts."""
        parts = ["# Databricks notebook source"]
        for cell in cells:
            if cell.get("cell_type") == "code":
                parts.append("# COMMAND ----------")
                parts.append(cell.get("source", ""))
        return "\n\n".join(parts) + "\n"
