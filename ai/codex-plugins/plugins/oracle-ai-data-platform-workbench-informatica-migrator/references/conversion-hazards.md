# Cross-cutting conversion hazards — the 30-point review rubric

Thirty ways an Informatica mapping and its Spark translation can differ
**without producing an error**. Each has a defined outcome here, the way
every transformation type does: handled in code, open with a stated reason,
a review question a human must answer, or an operational concern outside
conversion.

This is the checklist a reviewer
works through. It turns review from an open-ended read of generated code
into a finite set of questions with known answers.

**Current tally:** 19 handled in code · 8 requiring a human decision that
cannot be made from an export · 3 operational rather than conversion concerns
· 3 that convert but diverge for some inputs and say so. **None open.**

Nothing here is verified against a real Informatica run. These are
documentation-derived claims plus code inspection; the ground-truth harness is
what turns them into verified behaviour.

**Status meanings**

| Status | Meaning |
| --- | --- |
| **HANDLED** | The generator does the right thing, and a test pins it |
| **REVIEW** | Cannot be decided from the export — a human must check against the data or the source system |
| **OPERATIONAL** | A cluster, deployment or design concern rather than a conversion one |

Nothing here is verified against a real Informatica run. These are
documentation-derived claims plus code inspection; the ground-truth harness
is what turns them into verified behaviour.

---

## Divergences reported rather than fixed

Some functions have a Spark equivalent that is right for common input and
wrong for some of it, with no primitive carrying Informatica's rule. There
is nothing better to emit, and refusing would block the cases that are
correct. These convert and carry a `REVIEW REQUIRED` naming the divergence:

- **`INITCAP`** — Informatica capitalises after any non-alphanumeric;
  Spark's splits on whitespace. `o'brien-smith` becomes `O'Brien-Smith`
  there and `O'brien-smith` here. Hits hyphenated and apostrophed names.
- **`SOUNDEX`** — edge cases differ (leading non-alpha, H/W separators,
  strings under four characters). Matters when codes are stored or used as
  join keys.
- **`MD5`** — the digest depends on input encoding; a non-ASCII source read
  under a different charset hashes differently, and both values look valid.

## Review — a human must decide

**19. String truncation to port precision** · *Medium* · symptom: different data, no error  
Informatica silently truncates a string to the output port's precision. The precision is in the export, but emitting `substring()` everywhere would be wrong: most mappings did not rely on truncation, and forcing it would change data the source never truncated. Only a human looking at the target can say which ports fed a fixed-width sink.

**17. Field name case sensitivity** · *Low* · symptom: resolution failures  
Spark resolves column names case-insensitively by default; Informatica is case-sensitive. Check when a mapping relies on two ports differing only in case.

**18. Trailing spaces in CHAR fields** · *Low* · symptom: failed joins and comparisons  
Trailing-space handling on CHAR depends on connector settings and cannot be read from the export. Compare key columns across engines before trusting a join.

**20. Empty string vs NULL** · *Low* · symptom: different results per source  
Oracle treats the empty string as NULL and Spark does not. A source-specific behaviour, not something the converter can decide.

**21. Sorted Input options are meaningless** · *Low* · symptom: wasted shuffles  
Sorted Input is a performance hint with no Spark equivalent. Harmless if ignored; wasted shuffles if reproduced literally.

**22. Unconnected lookups hide in expressions** · *Medium* · symptom: missed dependencies  
Unconnected lookups are parsed, but a lookup invoked from an expression hides a dependency the lineage report should surface.

**24. Timezone handling** · *Medium* · symptom: off-by-hours timestamps  
Generated job schedules carry an explicit timezone assumption; data timestamps still depend on the cluster session timezone.

**29. No Auto Loader equivalent** · *Medium* · symptom: file-arrival pipelines need redesign  
No Auto Loader equivalent; IDMC file listeners need a redesign as job triggers. Orchestration scope, not conversion.

## Handled in code

**1. Aggregator passthrough fields** · *High* · symptom: wrong values  
Passthrough ports emit F.last and a REVIEW REQUIRED naming the field; the ordering column cannot come from the export.

**2. Router groups are not exclusive** · *High* · symptom: missing rows  
Each Router group is an independent filter, never an elif chain. The default group gets rows matching no other group.

**3. Joiner master/detail direction** · *High* · symptom: wrong row counts  
Master/detail is read from the port-level MASTER flag or the Master Source property, not from connector order. When neither resolves it, the join type decides: an **inner** join is applied, because it is symmetric and the direction cannot change the result; an **outer** join emits REVIEW REQUIRED and is skipped, because direction decides which rows survive. The join condition is oriented by the mapping's CONNECTORs -- which DataFrame actually carries each port -- rather than by the Designer's "master written first" convention, which a real export can contradict.

**4. Dynamic lookup cache** · *High* · symptom: no equivalent  
infa_compat.cached_lookup raises NotImplementedError for a dynamic cache rather than running static semantics.

**5. TO_DATE / TO_CHAR format strings** · *High* · symptom: wrong values, no error  
Format strings are translated between the two languages; IS_DATE honours its format argument.

**6. CONCAT and NULL** · *High* · symptom: blanked columns  
CONCAT emits concat_ws('', ...), which skips NULLs as Informatica does.

**7. Sequence generator persistence** · *High* · symptom: duplicate or colliding keys  
infa_compat.sequence is a persisted, restart-safe counter backed by a control table.

**9. ANSI mode vs implicit coercion** · *Medium* · symptom: job failures or silent nulls  
Generated notebooks pin spark.sql.ansi.enabled=false rather than inheriting it, so a runtime upgrade cannot change behaviour silently.

**10. Divide by zero** · *Medium* · symptom: failures or changed values  
Covered by the same ANSI pin: division by zero yields NULL, as in Informatica.

**11. Reject rows vs job failure** · *High* · symptom: all-or-nothing loads  
DD_REJECT rows are routed rather than filtered away silently.

**12. Transaction Control granularity** · *Medium* · symptom: changed visibility contract  
Transaction Control passes rows through and names the lost commit boundary.

**13. Row order is not preserved** · *High* · symptom: nondeterministic results  
FIRST/LAST/CUME/MOVINGAVG/MOVINGSUM each emit a REVIEW REQUIRED naming the field and the function.

**14. Expression variable fields are sequential** · *Medium* · symptom: wrong values  
A self-referencing variable port emits REVIEW REQUIRED instead of an expression that silently means something else.

**15. explode drops empty arrays** · *High* · symptom: missing rows  
The hierarchy family uses explode_outer, never explode, so a parent with an empty child array keeps its row.

**16. UNION deduplicates, Informatica Union does not** · *Medium* · symptom: missing rows  
Union emits unionByName with no distinct, matching Informatica's non-deduplicating Union.

**28. SparkSession creation is automatic** · *Low* · symptom: errors or duplicate contexts  
Generated notebooks use SparkSession.builder.getOrCreate(), which is the documented-acceptable form rather than building a fresh context.

**8. Decimal precision and overflow** · *High* · symptom: silent nulls or rounding drift  
`TO_DECIMAL` with no scale emitted `decimal(38,0)`, truncating every fractional value — an amount column silently lost its cents. Informatica keeps the input's own scale, which an expression cannot see, so Spark has to be told a number. It now emits `decimal(38,10)`, matching the scale in Oracle's published AIDP mapping so the value survives the eventual write without a second rounding step, and the choice is reported as an assumption rather than passed off as a translation. An explicit scale, including 0, is still honoured exactly.
**23. Pre/post SQL commands live on the task** · *Medium* · symptom: missing side effects  
Emitted commented-out on **both** migration paths, with a marker. It is left commented because the statement is in the source database's dialect against a connection the notebook does not hold, so running it unchanged would either fail or succeed against the wrong system. The batch path used to discard the parsed session entirely; that is fixed, and a construct fixture now drives session SQL through both paths.
**30. Spark types do not survive a JDBC write to Oracle** · *High* · symptom: wrong totals, failed writes  
Every ADW write path now emits a type-fidelity check that runs before the write, graded against Oracle's published Spark-to-Oracle mapping: `MapType` **blocks** the write (no Oracle target type exists), while `double`, `boolean`, `float`, `array`, `struct` and interval types print a `REVIEW REQUIRED` naming the loss and the fix. `DecimalType(p,s)` is the only exact numeric path. Emitted rather than checked at generation time because the DataFrame's real types depend on every conversion upstream, so only the notebook knows them — and it matters because `storeAssignmentPolicy=LEGACY` is pinned, which makes Spark truncate rather than raise.

**31. A Sequence Generator key re-written by a re-run** · *High* · symptom: silently re-keyed dimension

The MERGE condition deliberately prefers a natural key over a surrogate one, because a Sequence Generator hands out a fresh value on every run. That is only half the problem: matching on the natural key and then calling `whenMatchedUpdateAll()` writes the fresh surrogate value over the stored one, so each re-run re-keys every row that already exists. Any table holding a foreign key into that dimension then points at the wrong row, and nothing fails -- the load reports success.

Informatica does not behave this way: NEXTVAL is consumed by the insert path, and an update leaves the key column alone. Generated writes now emit `_FROZEN_ON_UPDATE` listing the sequence-fed columns that the match condition does not use, and update every column except those. New rows still receive their sequence value through `whenNotMatchedInsertAll()` -- dropping the column from the insert path too would leave the key NULL, which is worse than the bug.

The frozen columns are traced forward through the export's CONNECTORs from the NEXTVAL port, not matched by name, so a port renamed on the way to the target does not escape.

**32. A translated SQL override or User Defined Join** · *Medium* · symptom: aborted run, or Oracle-vs-Spark dialect drift

An override that JOINs, UNIONs or sub-selects is run as Spark SQL against catalog-qualified tables rather than reported and skipped. Two gates must both hold: no Oracle-only construct (`(+)`, `ROWNUM`, `CONNECT BY`, `MINUS`, bind variables) and every table resolvable to a source in the mapping, so it can be qualified. A User Defined Join is synthesised into `SELECT * FROM a, b WHERE <condition>` and goes through the same gates.

The trade is deliberate and worth understanding: a mistranslation now surfaces as an `AnalysisException` on the first run instead of as silently different rows. The previous behaviour read the base table and reported that the override had not been applied -- honest, but the notebook still ran and returned the wrong rows, which a reviewer who skipped the comment would not notice.

Spark SQL accepts most of ANSI, but it is not Oracle. **Confirm the result matches what Oracle returned before trusting the output**; the generated cell carries that instruction.

## Operational, not conversion

**25. Speculative execution and retries** · *Medium* · symptom: duplicate side effects  
Spark may run a task twice. Any side-effecting UDF must be idempotent or moved out of the job. A cluster and design concern, not a conversion one.

**26. Notebook export is not supported** · *High* · symptom: unrecoverable code  
A notebook is an artefact, not source. Keep the generated output under version control.

**27. All-purpose clusters are shared** · *Medium* · symptom: you break other people's work  
Shared all-purpose clusters let one migration's config changes affect other work. Use a job cluster.

---

## The parallel batch path — fixed

Three defects lived on one code path, found independently from two
directions and all invisible to any check that asked whether notebooks were
produced. Each now has a test that was verified to fail with its fix
disabled.

**1. The parsed Session was discarded.** A synthetic `Session` was built per
mapping instead of resolving the parsed one, so pre/post SQL, connections,
commit interval and session-level parameters all vanished on this path
while looking correct everywhere else. It now resolves the session the same
way the default path does.

**2. Only the first mapping in a file was migrated.** `mappings[0]` with no
loop: a multi-mapping export silently lost the rest. `_migrate_single` now
returns one result per mapping, and the summary counts mappings rather than
files — reporting a three-mapping file as one unit of work is what hid the
two being dropped.

**3. Success was reported regardless of review state.** The generator sets a
flag when it declines its own output and nothing read it. Results now carry
`review_required`, the status distinguishes `review` from `success`, and the
batch report records the bucket.

Two consequences elsewhere, both followed through: the auto-rerun path now
matches the retried result by mapping name instead of taking the first, and
the post-batch comparison/lineage step in `migrator.py` — which deliberately
mirrored the old one-mapping behaviour — now matches by mapping name too,
or one mapping's lineage would have been attached to every other mapping in
its file.

### The test that did not work

The first version of the review-state test compared `review_required`
against the marker's presence across the multi-mapping fixture. Those
notebooks carry no markers, so it read `False == False` and passed with the
fix disabled. A test of review state needs input that produces a review.
Same failure shape as the four instruments in
worth recording because it happened
while writing the test *for* that rule.

---

## Unresolved: what REPOSITORY_VERSION 186 and 187 mean

The version gate refuses anything below 10.x, so this decides whether the
tool refuses supported exports or accepts unsupported ones. It is currently
mapped to 10.1/10.2 and accepts them.

**There is no evidence either way in this repository.** The only basis for
the competing claim — that 186 means 9.x — was a test docstring describing a
hand-authored fixture as "a genuine 9.x export". It was not one: its
`REPOSITORY_VERSION` was a typed number, not a measurement. Every fixture
here is authored for this project, so none of them can settle the
question.

**Informatica publishes no REPOSITORY_VERSION table.** KB 516223, which was
expected to settle this, documents a different attribute
(`SerializationSpecVersion`) for a different export format — the Developer
/ Model repository XML used by Data Quality and Data Engineering, not the
PowerCenter POWERMART export. That mapping is now implemented (see
`version_detector._SERIALIZATION_SPEC_RELEASE`), but it answers a different
question.

**What would settle it:** a real PowerCenter export whose release is
independently known. Nothing short of that.

**Meanwhile:** no bundled fixture carries 186 or 187, so nothing in the
published metrics depends on how the gate reads them. The exposure is
entirely on real exports: if 186 is in fact 9.x, a genuine 9.x export gets
converted instead of refused, which is the opposite of what the version gate
is for. The generated report names the dispute so it cannot be inherited
silently.
