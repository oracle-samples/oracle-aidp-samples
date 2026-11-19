# Migration inventory — what has to move for a pipeline to run on AIDP

The complete list of things that must come across from an Informatica estate, for both
source platforms. This is the discovery checklist: walk it at a customer before scoping,
and check generated output against it before calling a migration done.

It is deliberately broader than what the utility parses. A mapping that translates
perfectly still fails in production if nobody moved the parameter file, seeded the
sequence, or pointed the scheduler at the new job. Those gaps are where cutovers die, so
they are listed here alongside the assets the parser handles.

Distinct from what to *build* in an IDMC trial
to exercise the parser. This covers what to *collect* from a real estate.

## Contents

- [PowerCenter 10.5.x](#powercenter-105x)
- [IDMC / IICS](#idmc--iics)
- [Applies to both](#applies-to-both)
- [The five that actually break cutovers](#the-five-that-actually-break-cutovers)

---

## PowerCenter 10.5.x

### Repository objects

Extracted with `pmrep objectexport`, Repository Manager, or the crawler's SOAP path.
These are what `parsers/xml_parser.py` consumes.

| Item | Becomes on AIDP |
| --- | --- |
| Folders, including shared folders | Workspace folder structure / catalog schemas |
| **Mappings** | PySpark notebook |
| **Mapplets** | Shared Python module or reusable notebook |
| **Sessions** | Job task config + Spark read/write options |
| **Workflows** | AIDP Job DAG |
| **Worklets** | Nested job / sub-DAG |
| Reusable transformations | Shared functions in `infa_compat` |
| Source and target definitions | Catalog table definitions / connector configs |
| **Shortcuts**, local and global | Must resolve at export time — see the warning below |
| User-defined functions | Spark UDFs or expression-library entries |
| Session config objects | Job-level defaults |
| Workflow tasks: Command, Decision, Assignment, Timer, Control, Event-Wait, Event-Raise, Email | Job task types |
| **Link conditions** between tasks | Conditional edges in the job DAG |
| Cubes, dimensions, business components | Rarely present; normally dropped |

**Shortcuts are a silent failure mode.** An export that does not resolve them produces
mappings that look complete and reference nothing. Verify shortcut resolution on the
export itself, not on the generated notebook — by the time it reaches the generator the
information is already gone.

### Not in the repository export

Everything below lives on the Integration Service host, in the domain configuration, or
in the repository's runtime tables. None of it arrives with an object export, and all of
it is required for the pipeline to actually run.

| Item | Where it lives | Becomes on AIDP |
| --- | --- | --- |
| **Connection objects** | Domain / repo, via `pmrep listconnections` | AIDP connectors |
| **Passwords** | Encrypted, not exportable | Re-gathered into the AIDP credential store |
| **Parameter files** (`.par`, `.prm`) | IS filesystem | Job parameters / `spark.conf` |
| **Persisted mapping variables** | Repository tables | Control table, seeded with current values |
| **Sequence Generator current values** | Repository | Control table — seed with the real `CURRVAL` |
| `$PMRootDir` and every `$PM*` variable | IS filesystem | Object Storage paths / volumes |
| **Flat files**: sources, targets, lookup files, indirect file lists | IS filesystem | Object Storage |
| XML/XSD schemas, COBOL copybooks | IS filesystem | Schema definitions |
| **Pre/post-session SQL** | Session properties | Cells before/after the write |
| **Pre/post-session commands**, Command tasks | Shell scripts on the server | Job shell tasks |
| **External procedure / Custom / Java transformation code** | Compiled `.so`, `.dll`, Java classes | Manual rewrite — no automatic path |
| **Stored procedures** behind SP transformations | Source/target database | Keep in the database, or port |
| SQL overrides: source qualifier, lookup, target | Inside mappings | Spark SQL — dialect differences bite |
| **Partitioning configuration**: partition points and types | Session | Spark partitioning — rarely 1:1 |
| Commit interval and type, treat-source-rows-as, error thresholds, row-error logging, recovery strategy, high precision | Session | Write semantics + reject tables |
| Persistent lookup caches | Cache directory | Normally rebuilt; check for named or shared caches |

**Sequence values are the one that bites hardest.** A Sequence Generator's current value
lives in the repository, not in the mapping. Migrate the mapping without it and the new
pipeline reissues surrogate keys starting from the initial value, colliding with every
key already in the warehouse. Seed the control table from the live repository as part of
cutover, not as a follow-up.

### External to PowerCenter

| Item | Becomes on AIDP |
| --- | --- |
| **External scheduler** — Control-M, Autosys, Tidal, cron — invoking `pmcmd` | AIDP Job schedule, or leave the scheduler in place calling the AIDP job API |
| `pmcmd` / `pmrep` wrapper scripts | Job definitions |
| Email and SMTP notification config | Job notifications |
| Users, groups, roles, folder permissions | AIDP roles |
| PowerExchange / CDC configuration | Incremental-load design |
| IDQ mappings, rules, dictionaries | Separate project — scope explicitly |
| Upstream dependencies and file-arrival triggers | Job triggers |
| OBIEE / OAC RPD or workbooks over the targets | Keep target shapes identical, or update the reporting layer |

Most estates are scheduled externally, not by PowerCenter's own scheduler. Ask which one
before assuming workflow schedules are the whole picture.

---

## IDMC / IICS

### Exportable assets

Via the Asset Management CLI V2 or the v3 REST API. Always export **with dependencies**
or connections and referenced assets are silently omitted.

| Item | Becomes on AIDP |
| --- | --- |
| Projects and folders | Workspace structure |
| **Mappings** | PySpark notebook |
| **Mapplets**, cloud and imported PowerCenter | Shared module |
| **Mapping tasks** | Job task config + runtime params |
| **Taskflows**, including linear taskflows | AIDP Job DAG — branching, decisions, parallel paths, error handlers |
| **Parameter sets** | Job parameters |
| Synchronization, Replication, Masking tasks | Simple read-write jobs |
| **PowerCenter tasks** — imported PC XML running in cloud | Route through the PowerCenter path instead |
| Dynamic mapping tasks | Parameterized job |
| **Connections** | AIDP connectors |
| **Shared sequences** | Control table — same continuity problem as PowerCenter |
| Saved queries | SQL in the notebook |
| Hierarchical schemas for XML/JSON | Schema definitions |
| Fixed-width file formats | Read options |
| Cloud user-defined functions | Spark UDFs |
| **File listeners** | Job triggers / object-storage events |
| **Schedules** on tasks and taskflows | Job cron |
| Intelligent structure models (CLAIRE) | No equivalent — rewrite |
| Business services | REST calls |
| Asset permissions, user groups, roles | AIDP roles |

Only mapping tasks, synchronization tasks and PowerCenter tasks offer **Download XML**,
and that XML is PowerCenter-shaped. Mappings, taskflows and parameter sets are JSON-only.
The XML route also carries only *mapped* target fields where the JSON carries all, and
exports one task at a time. Useful as a shortcut into the better-tested PowerCenter
parser; not sufficient on its own, because orchestration and parameters cannot come that
way.

### Lives on the Secure Agent

| Item | Notes |
| --- | --- |
| **Parameter files** | Agent filesystem, referenced by name from tasks |
| Local flat files, lookup files, reject and error files | Agent filesystem → Object Storage |
| Scripts invoked by Command tasks | Rewrite as job tasks |
| Custom JARs and Java transformation classes | Manual rewrite |
| **Add-on connectors** installed on the agent | Determines which AIDP connectors the migration needs |
| Agent custom properties, JVM options | Cluster configuration equivalents |

### In the estate, outside CDI

Mass Ingestion (database, file, streaming, CDC), Cloud Data Quality rules and
dictionaries, Cloud Application Integration processes and service connectors, MDM, CDGC
lineage, B2B Gateway. Each is a separate project. Scope them explicitly at discovery
rather than finding them mid-migration.

### Replaced, not migrated

Secure Agent, runtime environments, IPU metering, Monitor job history, advanced and
serverless cluster configuration. The AIDP cluster replaces all of it.

**One thing that helps:** mappings built in **CDI Advanced** (formerly Elastic) already
compile to Spark. Their semantics sit far closer to the target than classic Secure Agent
execution, so that subset carries lower translation risk. Ask whether Advanced mode is in
use — it changes the risk profile of the estate.

---

## Applies to both

| Item | Why it matters |
| --- | --- |
| **Credentials** | Never present in any export, from either platform. Always a manual re-gather |
| **Target DDL** | Moving targets into the lakehouse means translating types — `NUMBER(p,s)` precision, `DATE` vs `TIMESTAMP` |
| **History and initial load** | Existing warehouse data, especially effective-dated SCD2 rows |
| **Control and audit tables** | Watermarks, run logs, reject tables, sequence current values |
| **Reject and error-handling conventions** | The customer's operational contract, not an implementation detail |
| **Notifications, SLAs, restart and recovery runbooks** | Operations will not accept a cutover without them |
| **Reconciliation baselines** | Row counts and aggregates from the last good Informatica run — the proof of equivalence |

---

## The five that actually break cutovers

Ranked by how often they are discovered late.

1. **Sequence and shared-sequence current values** — not seeded, so surrogate keys
   collide or restart at 1.
2. **Credentials** — absent from every export, always found at the end.
3. **External scheduler integration** — the migration produces jobs that nothing
   triggers.
4. **Server-side files and scripts** — Command tasks, parameter files and flat-file
   directories are invisible to a repository or asset export.
5. **Unresolved shortcuts and missing dependencies at export** — the export looks
   complete and is not.

Each one is invisible to a parser working from an export file. They have to be collected
by asking, which is why this list exists.
