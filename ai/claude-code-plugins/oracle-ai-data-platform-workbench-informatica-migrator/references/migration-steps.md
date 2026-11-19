# End-to-end migration steps, and who does each one

What a complete Informatica→AIDP migration actually involves, step by step,
with an explicit owner for every step: the tool, the tool plus a human, or a
human alone.

Three documents cover adjacent ground, and it is worth knowing which to open:

| Document | Answers |
| --- | --- |
| [`migration-inventory.md`](migration-inventory.md) | *What* has to move — the discovery checklist |
| **This file** | *In what order*, and *who does each step* |

## Owner legend

| Owner | Meaning |
| --- | --- |
| **TOOL** | Automated. You run a command and the output is the deliverable. |
| **TOOL + HUMAN** | The tool produces something; a human must decide or finish it. |
| **MANUAL — flagged** | No automation, but the tool tells you it is needed. |
| **MANUAL — unflagged** | No automation, **and the tool stays silent.** You have to know. |

The last row is the one to read carefully. Every item in it is a step a
migration can reach cutover without having done, because nothing prompts
for it. They are listed in full at the end.

---

## Before anything else: check every job schedule

**This applies to every migrated workflow, every time.** Getting it wrong is
silent — the job runs, loads data, reports success, at the wrong time.

The export cannot tell the tool the timezone: PowerCenter records `STARTTIME`
in the Integration Service's local time and stores no zone at all, so "03:30"
could be 03:30 anywhere on earth. **Only you know which.** So there are three
cases, and the tool tells you which one you are in:

**1. The schedule converted and you declared the timezone.** Best case. Pass
`--schedule-timezone` with the IANA zone that Integration Service ran in:

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli migrate \
    -i exports/ -o out/ --schedule-timezone America/Chicago
```

The cron and the zone are both right, and no timezone assumption is reported
because there is no longer one. A zone that is not a real IANA name is
refused immediately — AIDP rejects an unknown `timezoneId`, so a typo would
otherwise produce a whole migration of undeployable jobs. **Do this if you
know the zone**; it is the difference between a correct schedule and a
plausible one.

**2. The schedule converted and you did not.** The job carries a cron derived
from `STARTTIME`, for example `0 30 3 * * ?`. **The time is right; the
timezone is a guess** — the job is created as **UTC** because something has to
go in the field. If that Integration Service ran in US Central, your job is
now five or six hours off.

> This produces an entry under **Assumptions to confirm** in
> `workflows/<name>.review.md`, and `migrate` prints a `Schedules:` line
> counting the timezone assumptions specifically. Either re-run with
> `--schedule-timezone` or correct the zone in AIDP before enabling the job.

**3. The schedule did not convert.** The job is created with **no schedule at
all** and will never fire on a timer. This is deliberate — a schedule that
cannot be derived exactly is omitted rather than approximated, because an
unscheduled job is obviously unscheduled whereas a plausible wrong cron is
not. The source values are quoted in the review file so you can set it by
hand.

This happens when the interval has no exact cron (every 7 hours), when the
export has no `<SCHEDULER>`, or when the schedule shape is not one the
converter recognises. Run-on-demand workflows also produce no schedule, and
that one is correct — no action needed.

**Neither case is safe to skip.** Case 1 looks finished and may be hours off;
case 2 will silently never run.

## The shape of a migration

```
  1 Discover      get the estate out of Informatica
  2 Assess        inventory, complexity, what will not convert
  3 Prepare       catalogs, credentials, target DDL, control tables
  4 Convert       mappings -> notebooks, workflows -> job DAGs
  5 Review        resolve every low-confidence and REVIEW REQUIRED item
  6 Deploy        notebooks and jobs into AIDP
  7 Seed          sequences, watermarks, history, initial load
  8 Verify        reconcile against the last good Informatica run
  9 Cut over      repoint the scheduler, notifications, runbooks
```

Steps 3, 7 and 9 are almost entirely manual and are where cutovers fail.
Steps 4 and 5 are where the tool does most of its work.

---

## PowerCenter 10.5.x → AIDP

### 1. Discover

| Step | Owner | How |
| --- | --- | --- |
| Export repository objects | **TOOL** | `infa2aidp discover` — live repository via SOAP / `pmrep` |
| Export from a file drop instead | **TOOL** | Point `analyze`/`migrate` at a folder of `pmrep objectexport` XML |
| **Resolve shortcuts at export time** | **MANUAL — unflagged** | Must be verified on the export itself. An unresolved shortcut yields a mapping that looks complete and references nothing, and nothing here detects it |
| Collect connection list | **MANUAL — flagged** | `pmrep listconnections`. Connections referenced by sessions are parsed; the domain-level list is not |
| Collect parameter files (`.par`, `.prm`) | **MANUAL** | Integration Service filesystem |
| Collect server-side files: flat files, lookup files, indirect lists, scripts | **MANUAL — unflagged** | IS filesystem. Invisible to any object export |
| Record `$PMRootDir` and every `$PM*` value | **MANUAL — unflagged** | Domain configuration |
| **Capture Sequence Generator current values** | **MANUAL — unflagged** | Repository runtime tables. See the warning below |
| Capture persisted mapping variable values | **MANUAL — unflagged** | Repository runtime tables |
| Identify the external scheduler | **MANUAL — unflagged** | Control-M / Autosys / Tidal / cron calling `pmcmd`. Most estates are scheduled externally, not by PowerCenter |

### 2. Assess

| Step | Owner | How |
| --- | --- | --- |
| Inventory: mappings, transformations, complexity | **TOOL** | `infa2aidp analyze` |
| Flag unsupported transformations (Java, Custom, HTTP, XML) | **TOOL** | `handlers/compatibility_checker.py` — reported as ERROR |
| Flag manual-review types (Stored Procedure, SQL, Transaction Control) | **TOOL** | Same |
| Flag Oracle-dialect SQL risks (`CONNECT BY`, `MODEL`, `XMLTABLE`) | **TOOL** | Same |
| Flag flat-file connections needing path remapping | **TOOL** | Same |
| Field-level lineage | **TOOL** | `infa2aidp lineage` |
| Decide what is out of scope (IDQ, PowerExchange CDC, OBIEE/OAC) | **MANUAL** | Scope explicitly; these are separate projects |

### 3. Prepare the target

Entirely manual. The tool assumes the target already exists.

| Step | Owner |
| --- | --- |
| Create AIDP catalogs and schemas | **MANUAL** |
| **Translate and create target DDL** — `NUMBER(p,s)` precision, `DATE` vs `TIMESTAMP` | **MANUAL — unflagged** |
| **Re-gather every credential** into the AIDP credential store / OCI Vault | **MANUAL — unflagged** |
| Register connections as AIDP catalogs, or decide the JDBC fallback per source | **MANUAL** |
| Create control tables: watermarks, sequences, reject/audit | **MANUAL** — `infa_compat/sequence.py` creates its own sequence control table at runtime; the rest is yours |
| Upload flat files / lookup files to Object Storage | **MANUAL** |

### 4. Convert

| Step | Owner | How |
| --- | --- | --- |
| Mappings → PySpark notebooks | **TOOL** | `infa2aidp migrate` |
| Expressions, transformations, dataflow | **TOOL** | 100% of expressions represented on the bundled corpus |
| Stateful semantics — sequences, lookups, SCD2, update strategy, date masks, `DECODE` | **TOOL** | Generated notebooks call `infa_compat` rather than inlining |
| **Workflows → AIDP job DAG** | **TOOL** | Task instances and links → tasks with `depends_on`, topologically ordered |
| **Workflow schedule → Quartz cron** | **TOOL + HUMAN** | Converted where exact; otherwise **no schedule is emitted** and the source values are quoted in `workflows/<name>.review.md` |
| Worklets | **TOOL** | Expanded, reusable and nested; sessions become `<worklet>__<instance>` tasks. One missing from the export is named in the review report |
| Command / Email / Decision / Timer / Event-Wait / Control / Assignment tasks | **MANUAL — flagged** | Not translated; each is named in the review report |
| Link conditions | **TOOL / MANUAL — flagged** | `$X.Status = SUCCEEDED` is applied (`runIf: ALL_SUCCESS`); any other becomes an unconditional dependency and is quoted in the review report |
| **Pre/post-session SQL** | **MANUAL — unflagged** | Parsed into the model and **never emitted into a notebook** |
| **Parameter file values** | **TOOL** | `$$` values become per-task job parameters, narrowest section winning; non-`$$` session parameters are reported |
| Partitioning, commit interval, error thresholds, recovery strategy | **MANUAL — unflagged** | Not carried into write semantics |
| External procedure / Java / Custom transformation code | **MANUAL — flagged** | No automatic path; correctly flagged as unsupported |

### 5. Review

| Step | Owner | How |
| --- | --- | --- |
| Resolve every `REVIEW REQUIRED` / low-confidence item | **TOOL + HUMAN** | `infa2aidp review` — the approval gate |
| Read the orchestration review per workflow | **TOOL + HUMAN** | `workflows/<name>.review.md`. **Absence of this file means the workflow translated whole** |
| Check generated output against the raw export | **TOOL** | `reports/fidelity_report.md` — compares to the export, not to the parser's own model |
| Spark performance suggestions | **TOOL** | `infa2aidp optimize` |

### 6. Deploy

| Step | Owner | How |
| --- | --- | --- |
| Upload notebooks, create jobs | **TOOL** | `infa2aidp deploy` |
| Warn on incomplete translation and unscheduled jobs | **TOOL** | `deploy` logs both |
| **Verify a notebook actually runs** | **MANUAL — flagged** | No cell-by-cell execute/verify/fix loop exists. The README says so |

### 7. Seed

| Step | Owner |
| --- | --- |
| **Seed sequence control tables with the real `CURRVAL`** | **MANUAL — unflagged** |
| Seed mapping-variable values | **MANUAL — unflagged** |
| Initial/history load, especially effective-dated SCD2 rows | **MANUAL** |
| Seed watermarks for incremental loads | **MANUAL — unflagged** |

> **The one that bites hardest.** A Sequence Generator's current value lives
> in the repository, not in the mapping. Migrate without it and the new
> pipeline reissues surrogate keys from the initial value, colliding with
> every key already in the warehouse. The runtime for this exists
> (`infa_compat/sequence.py`); the seeding does not, and nothing warns you.

### 8. Verify

| Step | Owner | How |
| --- | --- | --- |
| Compare source rows to migrated AIDP targets | **TOOL** | `infa2aidp reconcile` |
| **Compare against the last good Informatica run** | **MANUAL — unflagged** | No baseline is captured. `reconcile` compares source to target *now*, which is not the same proof |
| Agree the equivalence criteria with the customer | **MANUAL** |

### 9. Cut over

Entirely manual, and entirely unflagged.

| Step | Owner |
| --- | --- |
| **Repoint the external scheduler** at AIDP jobs, or have it call the AIDP job API | **MANUAL — unflagged** |
| Recreate email/SMTP notifications | **MANUAL — unflagged** |
| Recreate reject/error-file routing and formats | **MANUAL — unflagged** |
| Map users, groups, roles, folder permissions to AIDP roles | **MANUAL — unflagged** |
| Rewrite restart/recovery runbooks and SLAs | **MANUAL — unflagged** |
| Update the reporting layer (OBIEE/OAC) or keep target shapes identical | **MANUAL** |

---

## IDMC / IICS → AIDP

Same nine phases. The difference is concentrated in steps 1 and 4, and it is
large: **IDMC orchestration has no implementation at all.**

| Step | Owner | Notes |
| --- | --- | --- |
| Export assets | **MANUAL** | No `discover` equivalent. Use the Asset Management CLI V2 or the v3 REST API — see Informatica's own CLI documentation. Always export **with dependencies** |
| Mappings → notebooks | **TOOL** | Same converter as PowerCenter once parsed |
| **Mapping tasks** | **MANUAL — unflagged** | Not parsed. Zero occurrences in the parser |
| **Taskflows → job DAG** | **MANUAL — unflagged** | Not parsed. The IDMC parser imports `Workflow` and never constructs one |
| **Parameter sets** | **MANUAL — unflagged** | Not parsed |
| **Schedules on tasks/taskflows** | **MANUAL — unflagged** | Not parsed |
| Connections | **TOOL** | Parsed |
| Shared sequences | **MANUAL — unflagged** | Same continuity problem as PowerCenter, and no parser |
| File listeners → job triggers | **MANUAL — unflagged** | Not parsed |
| Cloud mapplets, saved queries, hierarchical schemas, fixed-width formats, cloud UDFs | **MANUAL — unflagged** | Not parsed |
| Intelligent structure models (CLAIRE) | **MANUAL** | No equivalent exists; rewrite |
| Secure Agent files, scripts, JARs, add-on connectors | **MANUAL — unflagged** | Agent filesystem |
| Mass Ingestion, CDQ, CAI, MDM, CDGC, B2B | **MANUAL** | Separate projects — scope at discovery |

**A shortcut worth knowing:** mapping tasks, synchronization tasks and
PowerCenter tasks offer *Download XML*, and that XML is PowerCenter-shaped —
so it can be routed through the better-tested PowerCenter parser. It is not
sufficient on its own: it carries only *mapped* target fields where the JSON
carries all, exports one task at a time, and cannot carry taskflows or
parameter sets.

**And one that changes the risk profile:** mappings built in **CDI Advanced**
(formerly Elastic) already compile to Spark, so their semantics sit far closer
to the target. Ask whether Advanced mode is in use before scoping.

---

## What the tool covers, honestly

**Covered end to end:** mapping conversion including expressions and stateful
semantics, PowerCenter workflow DAGs with schedule conversion, compatibility
and lineage analysis, an independent fidelity check, notebook and job
deployment, source-to-target reconciliation.

**Covered as "we tell you, you do it":** worklets, non-session workflow
tasks, link conditions, unsupported transformation types, notebook
verification, unconvertible schedules.

**Not covered and not flagged — the list to work from.** Every one of these
is a step a migration can reach cutover without doing:

1. Sequence Generator and shared-sequence current values — not seeded, keys collide
2. Credentials — no inventory of what must be re-gathered
3. External scheduler integration — jobs get created that nothing triggers
4. Unresolved shortcuts — the export looks complete and is not
5. Server-side files, `$PM*` paths, Command-task scripts
6. Pre/post-session SQL — parsed, never emitted
7. Parameter file values — parsed, never substituted
8. Partitioning, commit interval, error thresholds, recovery strategy
9. Target DDL and type translation
10. Reconciliation baselines from the last good Informatica run
11. Notifications, reject-file routing, permissions, runbooks
12. All IDMC orchestration — taskflows, mapping tasks, parameter sets, schedules, file listeners

Items 1-5 are the inventory's ranked cutover killers. Items 6-8 are worse in
one respect: the data is already in the model, so the tool *knows* and says
nothing.

## What would change this

Most of the unflagged list does not need new parsing — it needs reporting.
`handlers/compatibility_checker.py` already emits severity-ranked issues that
`analyze` surfaces. Counting Sequence Generators, enumerating connections
needing credentials, detecting `SHORTCUT` elements, and reporting
schedule-less workflows would move items 1-5 from *unflagged* to *flagged*
without a real export and without touching the generators.

That is the single highest-value change available to this tool, and
this argues it ahead of
taskflows and worklets for exactly that reason.
