# Known limitations

What this tool does **not** do, and why. Every entry here has a reason that
is not "we ran out of time": either the information needed is not in an
Informatica export, or there is no AIDP construct that means the same thing,
or verifying it needs access nobody on this project has.

Scope note: this is about the tool's ceiling, not its current bug list. For
what it *does* convert, see
[`conversion-coverage.md`](conversion-coverage.md); for behaviours that
differ between Informatica and Spark, see
[`conversion-hazards.md`](conversion-hazards.md).

**The honest summary: every mapping becomes a runnable Spark notebook and
every workflow becomes an AIDP job definition, but fidelity against a live
Informatica run has never been measured.** That single limitation matters
more than everything else on this page.

---

## 1. Not verified against a real Informatica estate

**Nothing here has ever parsed an export from a production PowerCenter or
IDMC repository.** Every fixture is hand-authored or synthetic.

This bounds every other claim in the project:

- **Fidelity is unproven.** Each transformation type has a defined outcome
  and generated notebooks execute correctly against hand-derived expected
  rows, but no generated output has been diffed against what Informatica
  computed from the same input.
- **Conversion-rate figures measure the tool against our own idea of
  Informatica**, because we wrote the denominator.
- **Repository versions 186 and 187 are inferred**, mapped to 10.1 and 10.2
  on reviewer recollection. If that is wrong, a genuine 9.x export is
  converted instead of refused — the opposite of the version gate's purpose.
  The generated report names the dispute rather than hiding it.

**Why not addressed:** no Informatica licence or support access. Oracle
eDelivery carries only 9.0.1; 10.5 needs a licence this project does not
have. **One real export from a known release would settle the version
question**, which is the cheapest high-value test available.

## 2. Transformations with no Spark equivalent

14 types are recognised, reported with a named reason, and not converted.
They divide into two kinds, and neither is a gap that more work closes.

**The logic is not in the export.** Cleanse, Labeler, Parse and Rule
Specification reference Cloud Data Quality assets — rules, dictionaries,
reference data — held outside the mapping. The export names the asset; it
does not contain it. A converter cannot emit logic it has never seen, and
guessing at a standardisation rule would produce confidently wrong data.
Export the asset and reimplement it, or keep the step on Informatica.

**There is no equivalent to emit.** Verifier validates addresses against
licensed reference data, and an unverified address is not a verified one.
B2B parses partner EDI/X12/EDIFACT with Informatica's own engine. Data
Services exposes a mapping as a queryable service — a serving pattern, not a
transformation. Web Services, Web Services Consumer and HTTP call an
endpoint per row, which from Spark executors would rate-limit or overwhelm
the target even if the endpoint and its credentials were in the export.
Access Policy applies entitlements, which belong in the catalog as roles and
restricted views, not in the job that writes the data. Unstructured Data
uses an Informatica data-transformation service.

Each reports the referenced asset name, why it cannot convert, and the
rebuild path. **A reported transformation is a successful run of this tool,
not a conversion** — the report says so, and the counts in
`conversion-coverage.md` never present it as one.

## 3. XML Parser and XML Generator: blocked on the Spark 4 upgrade

These two are the only reported types with a concrete unblock date.

`from_xml` and `to_xml` are built in from Spark 4. **AIDP is on Spark 3.5.0
on every cluster** (last checked 2026-10-01), where they do not exist —
they live in the external `spark-xml` package, which may not be installed.
Generated notebooks declare a 3.5.0 floor, so emitting `from_xml` today
would produce a notebook that fails on the cluster it was generated for.

A schema is derivable: the XML Parser's output ports define it. So this is
implementable the moment AIDP moves to Spark 4, and it is not implementable
before.

**Why not worked around:** branching the generator on a discovered cluster
version was considered and rejected — it couples a notebook to the cluster
it was generated against and makes the generator non-deterministic. See
`engine/infa2aidp/spark_target.py` for the reasoning. One Spark floor, no
per-version codegen.

## 4. Embedded code is carried, never translated

Java, Python, Velocity, Custom and External Procedure transformations embed
source in a language of their own. The original is **preserved verbatim**
next to the cell that needs it, as a review item.

This is deliberate. Translating arbitrary Java into PySpark is not a
mechanical transformation, and a plausible-looking port of someone's custom
logic is worse than an honest hand-off — it would be trusted. Nothing is
lost; the work moves to a human who can read the original.

## 5. Workflow tasks with no AIDP job-task equivalent

An AIDP job runs notebooks. Command runs shell on the Integration Service
host, Email sends mail, Decision branches on an expression, Timer waits on a
clock, Event-Wait blocks on a file, Control aborts deliberately, Assignment
sets a workflow variable at runtime. **None has an AIDP task that means the
same thing**, and inventing one would fabricate behaviour.

They are reported, with what the task did, a specific rebuild path, and
its own configured values read from the export's `<TASK>` attributes.

Worklets are **not** in this list — they are expanded in place and their
sessions become job tasks.

What *is* a residual limitation: `runIf` is per task and supports
`ALL_DONE`/`ALL_SUCCESS`, so a link condition richer than "upstream
succeeded" cannot be expressed on the dependency. A `$X.Status = FAILED`
link becomes `ALL_DONE`, which also runs on success; the review says so and
says to gate it inside the notebook.

## 6. Verification that needs access we do not have

| Not verified | What it would take |
| --- | --- |
| **ADW external-catalog writes** | A real Autonomous Database. The code path was derived from AIDP connector reference documentation and every generated cell carries an explicit marker saying so. |
| **A fired schedule** | A deployed job left to reach its cron time on a live cluster. The job *shape* is verified (`quartzCronExpression`/`timezoneId`/`pauseStatus`) and jobs deploy and run on demand; nobody has watched one fire on its own schedule. |
| **The live PowerCenter repository crawler** | A reachable PowerCenter repository. Exercised only against fakes. |

## 7. Breadth proves generation, not computation

Two kinds of evidence exist, and they cover different things:

- **Generation** — exports run through the generator, with the output
  asserted to parse, resolve every name, and abandon no DataFrame. This is
  broad.
- **Computation** — generated cells executed on real Spark and compared to
  hand-derived expected rows (the golden harness), plus notebooks run as
  AIDP jobs on a live cluster and reconciled against hand-computed numbers.
  This is narrow, and it uses our own fixtures.

"Zero parse errors across many mappings" is a real result and a narrow one.
It is not a statement about numbers coming out right. See §1.

## 8. Reporting precision

The source-fidelity report still flags some source-to-Source-Qualifier
connectors for columns nothing downstream uses. Those are usually
legitimate — Informatica does not carry an unconnected port either — so they
are false positives in practice.

Two larger classes of false positive were removed rather than tolerated. The
rest is left **reported rather than suppressed on purpose**: the remaining
cases are not reliably distinguishable from a column that should have been
used and was not, and hiding a real gap is worse than printing a
questionable one. Read the report as a review signal, which is what it calls
itself, not as a defect list.

## 9. Things that are decisions, not gaps

Recorded here because each gets proposed as an oversight:

- **No per-version Spark codegen.** One floor (3.5.0), deliberately. See §3.
- **Jobs deploy PAUSED.** Even with a declared timezone. A job that starts
  firing the moment it is deployed is an outward-facing side effect nobody
  asked for.
- **ANSI mode is pinned off** in generated notebooks, so migrated expression
  logic keeps Informatica's permissive evaluation. The write path is loud
  instead: a pre-write range check against the target's declared
  `NUMBER(p,s)` raises rather than letting Spark wrap a value silently.
- **An unconvertible condition refuses** rather than emitting a filter that
  silently matches nothing.
- **Inner joins are applied when master/detail is unresolvable; outer joins
  are not.** An inner join is symmetric, so the unresolved direction cannot
  change the result. For an outer join it decides which rows survive.
