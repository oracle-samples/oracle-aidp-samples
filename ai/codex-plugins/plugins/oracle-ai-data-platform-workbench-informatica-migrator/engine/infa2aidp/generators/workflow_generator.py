"""Generate AIDP job definitions from Informatica workflows.

Produces the JSON body the AIDP Workspace jobs API accepts
(``POST /20240831/dataLakes/{lake}/workspaces/{ws}/jobs``): a ``tasks``
array of ``NOTEBOOK_TASK`` entries keyed by ``taskKey`` with ``notebookPath``
and ``dependsOn: [{"taskKey": ...}]``, an optional ``schedule`` of
``quartzCronExpression`` + ``timezoneId`` + ``pauseStatus``, and
``maxConcurrentRuns``. (The schedule keys were ``cronExpression`` /
``timezone`` until a live deploy on 2026-09-25 returned 400 "schedule.
timezoneId must not be null; schedule.quartzCronExpression must not be
null" -- every scheduled workflow was undeployable.) The
per-task ``cluster`` reference and the top-level ``jobClusters`` are added
by the deployer at deploy time -- the cluster is a deployment choice, not
something the export carries. (Earlier versions of this module emitted
Databricks Jobs 2.x keys -- ``task_key``, ``notebook_task.notebook_path``,
``depends_on``, ``quartz_cron_expression`` -- which the deployer then POSTed
verbatim to an API that does not know them.)

Nothing in here invents a value it could not derive
==================================================
An earlier version of this module returned a hardcoded
``"0 0 2 * * ?"`` (daily 02:00 UTC) whenever it could not read a schedule
off the workflow -- which was always, because the parser did not read
``<SCHEDULER>`` at all. The result deployed cleanly, reported no error,
and ran every migrated job at the wrong time.

The rule now: a value that cannot be derived from the export is **not
emitted**, and the reason is recorded as a review item. A job created
without a schedule is obviously unscheduled to whoever looks at it; a job
created with a plausible-looking wrong schedule is not. Review items
travel beside the job definition, never inside it -- the definition is
POSTed verbatim to the AIDP jobs API, so it carries only keys that API
understands.
"""

from __future__ import annotations

import functools
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from ..models import Workflow

logger = logging.getLogger(__name__)


@dataclass
class WorkflowTranslation:
    """One workflow's generated job, plus what it cost to produce it.

    Two lists, deliberately not one. They mean different things and a
    reader who conflates them loses the signal:

    ``not_translated``
        Constructs in the source workflow with no representation in the
        job -- an unexpanded worklet, a dropped task, an unapplied link
        condition. **An empty list here means the workflow came across
        whole.** That claim only stays useful if nothing else is allowed
        into this list.

    ``assumptions``
        Values the generator had to supply because the export does not
        carry them, and which may be wrong. The schedule timezone is the
        standing example: PowerCenter records STARTTIME in the Integration
        Service's local time with no zone. Not a translation failure, but
        the operator has to confirm it.
    """
    job: dict
    not_translated: list[str]
    assumptions: list[str]

    @property
    def needs_review(self) -> bool:
        return bool(self.not_translated or self.assumptions)

# Task instance types that become a task in the generated job. Everything
# else in a PowerCenter workflow -- Worklet, Command, Email, Decision,
# Timer, Event-Wait/Event-Raise, Control, Assignment -- has no notebook to
# run, so it cannot be translated one-for-one and is reported instead.
_RUNNABLE_TASK_TYPES = {"SESSION"}

# Task types that are correctly represented by their own absence. A
# PowerCenter workflow always has a Start task -- it marks the entry point
# and has no runtime behaviour, so a job DAG that omits it has lost
# nothing. Reporting it would put a line in every review file and train
# the reader to skim past the lines that do matter.
_IGNORABLE_TASK_TYPES = {"START"}

# A link condition that says exactly what the job's ALL_SUCCESS already
# does: the task the condition names is the link's own upstream and it must
# have SUCCEEDED. Informatica developers put this on nearly every link --
# without it PowerCenter runs the downstream task even after a failure --
# so reporting it as "not applied" put noise on every workflow.
_SUCCEEDED_CONDITION = re.compile(
    r"^\s*\$(?P<task>[\w.]+)\.(?:Status|PrevTaskStatus)\s*=\s*SUCCEEDED\s*$",
    re.IGNORECASE,
)

# STARTTIME spellings seen in PowerCenter exports. Parsed only to recover
# the hour and minute; the date component is irrelevant to a cron.
# Emitted when a schedule converts. Not derived -- see _build_schedule.
_ASSUMED_TIMEZONE = "UTC"

_STARTTIME_FORMATS = (
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


# The areas IANA files its zones under. Used ONLY when no tz database is
# installed -- see validate_schedule_timezone.
_IANA_AREAS = ("Africa", "America", "Antarctica", "Arctic", "Asia", "Atlantic",
               "Australia", "Europe", "Indian", "Pacific", "Etc")
_IANA_SHAPE = re.compile(
    r"^(?:%s)(?:/[A-Z][A-Za-z0-9_+\-]*){1,2}$" % "|".join(_IANA_AREAS)
)
_UNCHECKED_ZONES_WARNED: set[str] = set()


@functools.lru_cache(maxsize=1)
def _known_zones() -> frozenset:
    """Every zone name the local tz database knows; empty when there is none.

    Cached: on Linux this walks /usr/share/zoneinfo, and a migration
    constructs a generator per input file.
    """
    import zoneinfo
    try:
        return frozenset(zoneinfo.available_timezones())
    except Exception:  # an unreadable TZPATH entry is "no database", not a crash
        return frozenset()


def validate_schedule_timezone(zone: str) -> str:
    """Return ``zone`` if AIDP will accept it as a ``timezoneId``, else raise.

    Membership in ``zoneinfo.available_timezones()``, compared
    case-sensitively. ``ZoneInfo(zone)`` is not enough on its own: it reads
    a file, so on a case-insensitive filesystem ``america/new_york``
    resolves, and AIDP's Java ``ZoneId`` -- which is case-sensitive -- then
    rejects every job at deploy time.

    Python on Windows ships no tz database; it comes from the ``tzdata``
    package, which this project declares for that reason. When neither the
    system nor ``tzdata`` provides one, the honest options are to refuse
    every zone (calling ``America/New_York`` invalid, which is false) or to
    check what can still be checked. This does the latter: ``UTC``/``GMT``
    are accepted as they are, and a name shaped exactly like an IANA
    Area/City in IANA's case is accepted with a warning that the name
    itself was not verified -- a typo
    such as ``America/New_Yrok`` then surfaces as a 400 at deploy, not here.
    Anything else is refused with a message naming ``tzdata``, not one
    claiming the zone does not exist.

    Raises:
        ValueError: as described above.
    """
    known = _known_zones()
    if known:
        if zone in known:
            return zone
        near = sorted(z for z in known if z.lower() == str(zone).lower())
        raise ValueError(
            f"schedule_timezone {zone!r} is not an IANA timezone (e.g. "
            f"'America/New_York', 'Europe/London', 'UTC'). AIDP rejects an "
            f"unknown timezoneId."
            + (f" Zone names are case-sensitive: did you mean {near[0]!r}?" if near else "")
        )
    if zone in ("UTC", "GMT"):  # fixed IDs every ZoneId accepts; nothing to look up
        return zone
    if _IANA_SHAPE.match(str(zone)):
        if zone not in _UNCHECKED_ZONES_WARNED:
            _UNCHECKED_ZONES_WARNED.add(zone)
            logger.warning(
                "No IANA tz database is installed, so schedule_timezone %r was "
                "checked for shape only, not looked up. A misspelt zone will be "
                "rejected by AIDP at deploy. Install the 'tzdata' package "
                "(pip install tzdata) to check it now.", zone,
            )
        return zone
    raise ValueError(
        f"schedule_timezone {zone!r} cannot be checked: no IANA tz database is "
        f"installed (Python on Windows has none of its own -- install the "
        f"'tzdata' package: pip install tzdata). Without it only 'UTC' or an "
        f"Area/City name in IANA's exact case, e.g. 'America/New_York', can be "
        f"accepted."
    )


def _session_keys(workflow: Workflow) -> list[str]:
    return [s.name if hasattr(s, "name") else str(s) for s in workflow.sessions]


def _is_succeeded_condition(dep: dict) -> bool:
    """True for ``$X.Status = SUCCEEDED`` where X is the link's own
    upstream -- exactly what dependsOn + runIf ALL_SUCCESS enforces."""
    m = _SUCCEEDED_CONDITION.match(str(dep.get("condition") or ""))
    if not m:
        return False
    subject = dep.get("condition_task") or dep.get("from_task") or dep.get("from")
    return str(subject or "").lower() == m.group("task").lower()


def _norm_scope(scope: str) -> str:
    return re.sub(r"\s+", "", scope or "").strip("[]").upper()


def _parameter_file_values(pf, workflow: Workflow, key: str, instance, task_name: str) -> dict:
    """Parameter-file values in force for one session instance.

    Sections are applied from the widest scope to the narrowest, so the
    narrowest wins -- PowerCenter's rule. Widest first: values before any
    header, ``[Global]``, ``[folder.WF:wf]``, each enclosing worklet
    ``[folder.WF:wf.WT:wl...]``, the session by name (``[session]``,
    ``[folder.session]``), and the instance's full path
    ``[folder.WF:wf.WT:wl.ST:instance]``. Names compare case-insensitively.
    With no folder known, a section's folder part is not checked.
    """
    path = list((instance or {}).get("path") or [key])
    inst = path[-1]
    worklets = path[:-1]
    folder = (getattr(workflow, "folder", "") or "").upper()
    wf = f"WF:{workflow.name}".upper()

    ranked: list[str] = ["GLOBAL", wf]
    for i in range(1, len(worklets) + 1):
        ranked.append(".".join([wf] + [f"WT:{w}".upper() for w in worklets[:i]]))
    ranked += [n.upper() for n in dict.fromkeys([task_name, inst])]
    ranked.append(".".join([wf] + [f"WT:{w}".upper() for w in worklets] + [f"ST:{inst}".upper()]))

    def rank(scope: str) -> int:
        s = _norm_scope(scope)
        if s == "GLOBAL":
            return 0
        head, _, rest = s.partition(".")
        candidates = [s]
        if rest and (not folder or head == folder):
            candidates.append(rest)
        best = -1
        for c in candidates:
            if c in ranked[1:]:
                best = max(best, ranked.index(c))
        return best

    values: dict[str, str] = dict(getattr(pf, "global_params", {}) or {})
    applicable = [(rank(sc.scope), n, sc) for n, sc in enumerate(getattr(pf, "scopes", []) or [])]
    for _, _, sc in sorted((r, n, sc) for r, n, sc in applicable if r >= 0):
        values.update({k: str(v) for k, v in sc.parameters.items()})
    return values


# How to rebuild each non-session task type on AIDP, and which of its own
# <TASK> ATTRIBUTEs to quote so the operator does not have to go back to the
# export. None of these have an AIDP job-task equivalent -- an AIDP job runs
# notebooks, and these tasks run shell commands, send mail, wait on clocks
# and files, or branch on workflow variables. Inventing a task for them
# would be fabricating behaviour, so they stay reported; what this table
# fixes is a report that said "dropped, reproduce by hand" and left the
# operator to work out how.
#
# Each entry: (what it did, how to rebuild it, ATTRIBUTE names worth quoting).
_TASK_REBUILD: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "COMMAND": (
        "ran shell command(s) on the Integration Service host",
        "AIDP jobs run notebooks, not shell. If the command moved or archived "
        "files, rewrite it against Object Storage in a notebook cell; if it "
        "called pmcmd, it is orchestration that the job DAG now expresses; if "
        "it ran a host-local script, that host is not part of the migration "
        "and the logic has to be ported",
        ("Command", "Command1", "Fail Task if Any Command Fails"),
    ),
    "EMAIL": (
        "sent mail on success or failure",
        "Use the AIDP job's own notification configuration rather than a task. "
        "A notebook can also raise with a message, which surfaces in the job "
        "run -- but do not send mail from inside a notebook cell, because a "
        "retried cell re-sends it",
        ("Email User Name", "Email Subject", "Email Text"),
    ),
    "DECISION": (
        "evaluated a condition into $<task>.Condition for downstream links",
        "An AIDP job task runs or does not run; it cannot branch on an "
        "expression. Either move the condition into the downstream notebook "
        "(it reads the same parameters and can exit early), or split the "
        "workflow into two jobs and decide between them outside AIDP",
        ("Decision Name", "Decision Expression"),
    ),
    "TIMER": (
        "waited for a delay or until an absolute time before continuing",
        "Fold the wait into the job's own schedule where it was an absolute "
        "time -- a job that should start at 04:00 should be scheduled at "
        "04:00, not scheduled earlier and told to wait. A relative delay "
        "between two steps usually existed to let an external system finish; "
        "make that a dependency rather than a sleep",
        ("Absolute Time", "Relative Time", "Timer Type"),
    ),
    "EVENT-WAIT": (
        "blocked until a file appeared or a user-defined event was raised",
        "A file arrival is a trigger, not a step: run the job when the file "
        "lands rather than starting it early and waiting. A user-defined "
        "event paired with an Event-Raise is internal sequencing that the job "
        "DAG now expresses directly",
        ("Event Name", "User Defined Event", "Filewatch Name"),
    ),
    "EVENT-RAISE": (
        "raised a user-defined event that an Event-Wait elsewhere waited on",
        "This was internal sequencing between two points in one workflow. The "
        "job DAG expresses that ordering directly, so the pair is usually "
        "replaced by a dependency and nothing else is needed -- confirm the "
        "matching Event-Wait is accounted for",
        ("Event Name", "User Defined Event"),
    ),
    "CONTROL": (
        "stopped, aborted or failed the workflow deliberately",
        "Raise from the notebook that detects the condition. A raised cell "
        "fails its task, and the job's dependency rules stop the tasks "
        "downstream of it -- which is what Fail/Abort did",
        ("Control Option",),
    ),
    "ASSIGNMENT": (
        "assigned a value to a workflow variable at runtime",
        "Workflow variables that are constant for a run become job or task "
        "parameters, which generated notebooks already read through _param(). "
        "A value COMPUTED mid-run has no parameter equivalent: pass it "
        "between notebooks through a table, not through job metadata",
        ("User Defined Variable", "Assignment Expression"),
    ),
}


def _one_line(value: str, limit: int = 160) -> str:
    """An attribute value fit for a single review line.

    Command attributes are routinely multi-line scripts; a raw newline would
    break the review file's list formatting and bury the rest of the item.
    """
    flat = " ".join(str(value).split())
    return flat if len(flat) <= limit else flat[: limit - 1] + "\u2026"


class WorkflowGenerator:
    """Generates an AIDP job definition from an Informatica Workflow."""

    def __init__(self, schedule_timezone: Optional[str] = None) -> None:
        """
        Args:
            schedule_timezone: The IANA zone the source Integration Service
                ran in, e.g. ``America/New_York``. PowerCenter records
                STARTTIME in that service's local time and stores no zone,
                so the export cannot supply it and only the operator knows
                it. Passing it turns the schedule from an assumption into a
                translation: the hour is then correct rather than
                plausible. Left unset, the generator falls back to
                ``UTC`` and reports that as an assumption, as before.

        Raises:
            ValueError: if the zone is not a real IANA zone. A typo would
                otherwise reach the jobs API, which rejects an unknown
                ``timezoneId`` -- failing here names the mistake instead.
                See :func:`validate_schedule_timezone`, which a migration
                run also calls before it writes anything.
        """
        if schedule_timezone is not None:
            validate_schedule_timezone(schedule_timezone)
        self.schedule_timezone = schedule_timezone

    @property
    def _timezone(self) -> str:
        """The zone to write into the job definition."""
        return self.schedule_timezone or _ASSUMED_TIMEZONE

    @property
    def _timezone_is_declared(self) -> bool:
        """True when the operator told us the zone, so it is not a guess."""
        return self.schedule_timezone is not None

    def generate(
        self,
        workflow: Workflow,
        notebook_paths: dict[str, str],
        sessions: Optional[dict] = None,
        parameter_file=None,
    ) -> WorkflowTranslation:
        """Build an AIDP job definition.

        Args:
            workflow: Parsed Informatica workflow.
            notebook_paths: Map of session name -> notebook path on AIDP.
                Example: {"s_load_customers": "/Migrated/DIM_CUSTOMER/nb_load_customers"}
            sessions: Map of session name -> parsed ``Session``, for the
                ``$$`` overrides each session carries. A workflow only
                names its sessions; without this they had no parameters.
            parameter_file: A parsed ``.par`` file
                (``ParameterFileResult``). Each session task gets the
                ``$$`` values of every section that applies to it, the
                most specific section winning, as PowerCenter resolves them.

        Returns:
            A :class:`WorkflowTranslation`. Its ``job`` is POSTed to the AIDP
            jobs API as-is, so it contains only keys that API understands --
            neither review list is a key inside it.
        """
        review: list[str] = []
        assumptions: list[str] = []

        edges = self._session_edges(workflow)
        dep_graph = self._build_dependency_graph(workflow, edges)
        tasks, task_review = self._build_tasks(
            workflow, notebook_paths, dep_graph, sessions or {}, parameter_file,
            edges=edges,
        )
        # The review and assumption text below is written from the runIf
        # each task actually got, never from what a link would imply on its
        # own: one SUCCEEDED link makes the whole task ALL_SUCCESS, and a
        # line saying its other links "run on completion" was then false.
        run_if = {t["taskKey"]: t["runIf"] for t in tasks}
        review.extend(task_review)
        review.extend(self._review_untranslated_tasks(workflow))
        review.extend(self._review_link_conditions(workflow, run_if))
        link_assumptions, link_review = self._unconditional_links(workflow, run_if, edges)
        review.extend(link_review)
        assumptions.extend(link_assumptions)

        schedule, schedule_review, schedule_assumptions = self._build_schedule(workflow)
        review.extend(schedule_review)
        assumptions.extend(schedule_assumptions)

        job: dict = {
            "name": workflow.name,
            "description": (
                f"Migrated from Informatica workflow: {workflow.name}"
                + (f" | {workflow.description}" if workflow.description else "")
            ),
            "path": "jobs",
            "tasks": tasks,
            "maxConcurrentRuns": 1,
        }
        # Omitted entirely rather than defaulted when it could not be
        # derived -- see the module docstring.
        if schedule is not None:
            job["schedule"] = schedule

        return WorkflowTranslation(job=job, not_translated=review, assumptions=assumptions)

    # ------------------------------------------------------------------
    # Review: what this generator could not translate
    # ------------------------------------------------------------------

    @classmethod
    def _review_untranslated_tasks(cls, workflow: Workflow) -> list[str]:
        """Name every task instance that does not become a job task.

        ``workflow.tasks`` holds every instance the parser saw. Anything
        that is not a session has no notebook behind it, so the generated
        DAG is genuinely missing that step -- the point of this list is
        that the omission is stated rather than inferred from a task count.
        """
        review: list[str] = []
        by_type: dict[str, list[str]] = defaultdict(list)
        for task in workflow.tasks or []:
            ttype = str(task.get("type", "")).upper()
            if (
                ttype
                and ttype not in _RUNNABLE_TASK_TYPES
                and ttype not in _IGNORABLE_TASK_TYPES
            ):
                by_type[ttype].append(str(task.get("name") or task.get("instance_name") or "?"))

        for ttype in sorted(by_type):
            names = ", ".join(sorted(by_type[ttype]))
            if ttype == "WORKLET":
                review.append(
                    f"Worklet task instance(s) not expanded: {names}. A worklet "
                    f"groups its own sessions, and those sessions are NOT in this "
                    f"job -- the DAG is incomplete until they are added by hand or "
                    f"the worklet is migrated separately."
                )
            else:
                review.append(cls._rebuild_guidance(ttype, names, by_type[ttype], workflow))
        return review

    @staticmethod
    def _rebuild_guidance(ttype: str, names: str, task_names: list[str],
                          workflow: Workflow) -> str:
        """One review line for a dropped task type: what it did, how to
        rebuild it, and its own configured values.

        The values come from the export's <TASK> ATTRIBUTEs. Quoting them
        matters because the alternative is a true but unusable report: an
        operator told "cmd_archive was dropped" still has to open the export
        to find out what it ran.
        """
        entry = _TASK_REBUILD.get(ttype)
        if entry is None:
            return (
                f"{ttype} task instance(s) dropped: {names}. This task type has "
                f"no notebook equivalent and was not translated; reproduce it in "
                f"AIDP by hand if the workflow depends on it."
            )
        did, rebuild, attr_names = entry
        line = (
            f"{ttype} task instance(s) NOT translated: {names}. In PowerCenter "
            f"this {did}; AIDP has no equivalent job task. How to rebuild it: "
            f"{rebuild}."
        )

        # The configured values, per instance, when the export carried them.
        props_by_task = {
            str(tk.get("name") or ""): (tk.get("properties") or {})
            for tk in (workflow.tasks or [])
            if str(tk.get("type", "")).upper() == ttype
        }
        details: list[str] = []
        for tname in sorted(task_names):
            props = props_by_task.get(tname) or {}
            quoted = [
                f"{a}={_one_line(props[a])!r}" for a in attr_names if props.get(a)
            ]
            if quoted:
                details.append(f"{tname}: " + "; ".join(quoted))
        if details:
            line += " Configured as -- " + " | ".join(details)
        else:
            line += (
                f" The export carried no <TASK> definition for "
                f"{'these' if len(task_names) > 1 else 'this'}, so its settings "
                f"are not available here -- read them from PowerCenter before "
                f"rebuilding."
            )
        return line

    @classmethod
    def _review_link_conditions(cls, workflow: Workflow,
                                run_if: Optional[dict] = None) -> list[str]:
        """Report workflow links whose CONDITION was not applied.

        A conditional link becomes a plain ``dependsOn`` in the generated
        job. What that does depends on the runIf the downstream task ended
        up with (``run_if``, from :meth:`_run_if_for`): under ALL_DONE a
        step that used to run only when a condition held now runs every
        time; under ALL_SUCCESS -- forced by a SUCCEEDED condition elsewhere
        on its upstream links -- it runs only on success, so a FAILED-only
        link never fires at all. A link into a non-session node (Command,
        Decision, a worklet's Start) is judged by the job task(s) it gates.
        """
        run_if = run_if or {}
        links = cls._links(workflow)
        review: list[str] = []
        for dep in workflow.dependencies or []:
            if not isinstance(dep, dict):
                continue
            cond = str(dep.get("condition") or "").strip()
            if cond and not _is_succeeded_condition(dep):
                frm = dep.get("from_task") or dep.get("from") or "?"
                to = dep.get("to_instance") or dep.get("to_task") or dep.get("to") or "?"
                failed_cond = "FAILED" in cond.upper()
                to_node = dep.get("to") or dep.get("to_task") or ""
                fed = cls._downstream_sessions(to_node, workflow, links) if to_node else []
                via = (
                    f" {to} is not a job task; this link gates {', '.join(fed)} "
                    f"through it."
                    if fed and fed != [to_node] else ""
                )
                strict = [t for t in fed if run_if.get(t) == "ALL_SUCCESS"]
                if strict:
                    one = len(strict) == 1
                    review.append(
                        f"Link condition not applied on {frm} -> {to}: `{cond}`.{via} "
                        f"{', '.join(strict)} {'is' if one else 'are'} runIf "
                        f"ALL_SUCCESS (a success-only condition on "
                        f"{'its' if one else 'their'} upstream links requires it), "
                        f"so {'it runs' if one else 'they run'} only when every "
                        f"upstream task SUCCEEDS, and this condition is not checked."
                        + (
                            " This link only wanted to run on FAILURE, so on AIDP "
                            "that failure path NEVER runs: rebuild it outside this "
                            "job (e.g. the job's own failure notification)."
                            if failed_cond else
                            " Apply the condition inside the downstream notebook if "
                            "it must not run on every success."
                        )
                    )
                    continue
                review.append(
                    f"Link condition not applied on {frm} -> {to}: `{cond}`.{via} The "
                    f"dependency was generated as unconditional (runIf ALL_DONE), "
                    f"so the downstream task runs once the upstream one COMPLETES "
                    f"-- whether it succeeded or failed."
                    + (
                        " This link only wanted to run on FAILURE, so the task will "
                        "now also run on success: gate it inside the notebook, or "
                        "remove the task from the job."
                        if failed_cond else
                        " Apply the condition inside the downstream notebook if it "
                        "must not run in every case."
                    )
                )
        return review

    @classmethod
    def _run_if_for(cls, task_name: str, workflow: Workflow,
                    edges: Optional[dict] = None) -> str:
        """``ALL_DONE`` or ``ALL_SUCCESS`` for one task, from the links that
        lead to it.

        PowerCenter runs the downstream task of an **unconditional** link
        once the upstream one COMPLETES, whether it succeeded or failed.
        This emitted ``ALL_SUCCESS`` everywhere, which stops instead -- the
        safer pipeline, but not what the source did. A migration should
        reproduce behaviour and report what it cannot reproduce, not quietly
        tighten it: a job that diverges from the source for a reason not
        visible in the notebooks makes reconciliation harder, and deciding
        the customer's pipeline should be stricter is their call.

        So: ``ALL_DONE`` when every incoming link is unconditional, and
        ``ALL_SUCCESS`` as soon as one link carries
        ``$X.Status = SUCCEEDED`` -- that condition asks for success
        explicitly, and ``runIf`` is per task rather than per link, so one
        demanding link governs the task.

        "Its links" includes the hops in front of it. A condition on
        s_a -> cmd_archive in s_a -> cmd_archive -> s_b gates s_b exactly as
        it would on a direct link; reading only s_b's own (unconditional)
        link gave ALL_DONE, and s_b ran after s_a FAILED -- which the source
        never did. See :meth:`_session_edges`.

        A task with no incoming link always runs; it keeps ``ALL_SUCCESS``
        so this change touches only tasks whose behaviour it is about.
        """
        incoming = [
            dep for dep in (workflow.dependencies or [])
            if isinstance(dep, dict)
            and str(dep.get("to_instance") or dep.get("to_task") or "") == task_name
        ]
        if not incoming:
            return "ALL_SUCCESS"
        # Any other condition is reported, not applied. Without the
        # condition the link is unconditional in effect, so the PowerCenter
        # reading still applies.
        if any(_is_succeeded_condition(dep) for dep in incoming):
            return "ALL_SUCCESS"
        if edges is None:
            edges = cls._session_edges(workflow)
        if any((edges.get(task_name) or {}).values()):
            return "ALL_SUCCESS"
        return "ALL_DONE"

    @classmethod
    def _unconditional_links(cls, workflow: Workflow, run_if: dict,
                             edges: dict) -> tuple[list[str], list[str]]:
        """``(assumptions, not_translated)`` for unconditional links that
        leave a session.

        PowerCenter runs the downstream task of an unconditional link once
        the upstream one COMPLETES, whether it succeeded or failed. Where
        the downstream task got ALL_DONE (see :meth:`_run_if_for`) the job
        matches that. That is faithful and it is also permissive, so it is
        still stated as an assumption: a downstream task will run on a
        failed upstream's output, exactly as it does in PowerCenter today.

        Where the same task ALSO has a SUCCEEDED link, it got ALL_SUCCESS --
        runIf is per task, not per link -- and the unconditional link is no
        longer reproduced: PowerCenter still runs the task when that
        upstream fails, AIDP skips it. That is a divergence, not an
        assumption, so it goes in the review. (Saying "runIf ALL_DONE. This
        matches PowerCenter." for it, as this used to, was false.)
        """
        session_keys = set(_session_keys(workflow))
        links = cls._links(workflow)
        hits: set[str] = set()
        skipped: dict[str, list[str]] = defaultdict(list)
        for dep in workflow.dependencies or []:
            if (not isinstance(dep, dict) or str(dep.get("condition") or "").strip()
                    or dep.get("from_task") not in session_keys):
                continue
            frm = dep["from_task"]
            to_node = dep.get("to") or dep.get("to_task") or ""
            for t in cls._downstream_sessions(to_node, workflow, links) if to_node else []:
                if (edges.get(t) or {}).get(frm):
                    # Another hop between frm and t demands success, so the
                    # source stops there too: nothing diverges, nothing to say.
                    continue
                if run_if.get(t) == "ALL_SUCCESS":
                    if frm not in skipped[t]:
                        skipped[t].append(frm)
                else:
                    hits.add(f"{frm} -> {dep.get('to_instance') or dep.get('to_task')}")

        assumptions: list[str] = []
        if hits:
            assumptions.append(
                "ASSUMPTION: unconditional link(s) " + ", ".join(sorted(hits)) + " run "
                "the downstream task after the upstream session COMPLETES, whether it "
                "succeeded or failed (runIf ALL_DONE). This matches PowerCenter. "
                "It also means the downstream task runs on a failed upstream's "
                "output -- if you want the job to stop there instead, set runIf "
                "to ALL_SUCCESS on the downstream task."
            )
        review: list[str] = []
        for t in sorted(skipped):
            ups = ", ".join(skipped[t])
            review.append(
                f"Mixed incoming links on {t}: the link(s) from {ups} are "
                f"unconditional, but a SUCCEEDED condition on another of its "
                f"upstream links makes {t} runIf ALL_SUCCESS -- AIDP sets runIf per "
                f"task, not per link. This diverges from PowerCenter: there {t} "
                f"still runs if {ups} fails; in AIDP it will be skipped. If it must "
                f"run after that failure, set runIf to ALL_DONE on {t} (which then "
                f"no longer enforces the SUCCEEDED condition) and check that "
                f"condition inside the notebook instead."
            )
        return assumptions, review

    # ------------------------------------------------------------------
    # Dependency graph
    # ------------------------------------------------------------------

    @staticmethod
    def _links(workflow: Workflow) -> list[tuple[str, str, Optional[dict]]]:
        """Every workflow link as ``(from, to, link)``. ``link`` is None for
        a bare ``(from, to)`` pair, which carries no condition."""
        links: list[tuple[str, str, Optional[dict]]] = []
        for dep in workflow.dependencies or []:
            if isinstance(dep, (list, tuple)) and len(dep) >= 2:
                pred, succ, link = dep[0], dep[1], None
            elif isinstance(dep, dict):
                pred = dep.get("from") or dep.get("from_task") or dep.get("predecessor", "")
                succ = dep.get("to") or dep.get("to_task") or dep.get("successor", "")
                link = dep
            else:
                continue
            if pred and succ:
                links.append((pred, succ, link))
        return links

    @classmethod
    def _session_edges(cls, workflow: Workflow) -> dict[str, dict[str, bool]]:
        """``{session: {nearest session ancestor: needs_success}}``.

        A link may pass THROUGH a task that does not become a job task
        (Command, Email, Decision, Timer, a worklet's Start ... -- reported
        separately as dropped): s_a -> cmd_archive -> s_b. The dependency
        s_a -> s_b still holds, so each session's predecessors are its
        nearest SESSION ancestors, walking back through any non-session
        nodes. Keeping only direct session->session links (as before) made
        s_b independent of s_a and free to run first.

        The walk carries each hop's condition with it, because dropping the
        node does not drop the condition: ``$s_a.Status = SUCCEEDED`` on
        s_a -> cmd_archive still means s_b must not run after s_a fails.
        ``needs_success`` is True when any hop on any path from the ancestor
        carries that condition. Any other condition is not applied, and
        :meth:`_review_link_conditions` reports it whether or not it is on
        a hop.

        Only sessions with at least one incoming link appear.
        """
        session_names = set(_session_keys(workflow))
        incoming: dict[str, list[tuple[str, bool]]] = defaultdict(list)
        for pred, succ, link in cls._links(workflow):
            incoming[succ].append((pred, link is not None and _is_succeeded_condition(link)))

        memo: dict[str, dict[str, bool]] = {}

        def ancestors(node: str, path: frozenset) -> dict[str, bool]:
            if node in memo:
                return memo[node]
            found: dict[str, bool] = {}
            for pred, gated in incoming.get(node, []):
                if pred in path:  # a cycle -- shouldn't happen
                    continue
                reach = {pred: False} if pred in session_names else ancestors(pred, path | {pred})
                for anc, needs in reach.items():
                    found[anc] = found.get(anc, False) or needs or gated
            memo[node] = found
            return found

        return {
            s: ancestors(s, frozenset({s}))
            for s in _session_keys(workflow) if s in incoming
        }

    @classmethod
    def _downstream_sessions(cls, node: str, workflow: Workflow,
                             links: Optional[list] = None) -> list[str]:
        """The job tasks a link into ``node`` gates: ``node`` itself when it
        is a session, else the nearest sessions after it, walking forward
        through non-session nodes -- the mirror of :meth:`_session_edges`."""
        session_names = set(_session_keys(workflow))
        fwd: dict[str, list[str]] = defaultdict(list)
        for pred, succ, _ in (links if links is not None else cls._links(workflow)):
            fwd[pred].append(succ)
        found: list[str] = []
        seen: set[str] = set()

        def walk(n: str) -> None:
            if n in seen:
                return
            seen.add(n)
            if n in session_names:
                found.append(n)
                return
            for succ in fwd.get(n, []):
                walk(succ)

        walk(node)
        return found

    def _build_dependency_graph(self, workflow: Workflow,
                                edges: Optional[dict] = None) -> dict[str, list[str]]:
        """Return {session_name: [predecessor_session_names]}.

        Sources:
        1. workflow.dependencies -- list of (from, to) tuples or dicts,
           resolved to session ancestors by :meth:`_session_edges`
        2. workflow.execution_order -- flat ordered list (sequential deps)
        """
        graph: dict[str, list[str]] = defaultdict(list)
        session_names = set(_session_keys(workflow))
        if edges is None:
            edges = self._session_edges(workflow)
        linked = {n for pred, succ, _ in self._links(workflow) for n in (pred, succ)}
        for name in _session_keys(workflow):
            if name in edges:
                graph[name] = list(edges[name])
            elif name in linked:
                graph[name] = []

        # Fall back to execution_order if no explicit deps
        if not graph and workflow.execution_order:
            prev: Optional[str] = None
            for item in workflow.execution_order:
                name = item if isinstance(item, str) else str(item)
                if name in session_names:
                    graph[name] = [prev] if prev else []
                    prev = name

        # Make sure every session appears
        for sname in _session_keys(workflow):
            graph.setdefault(sname, [])

        return dict(graph)

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def _build_tasks(
        self,
        workflow: Workflow,
        notebook_paths: dict[str, str],
        dep_graph: dict[str, list[str]],
        sessions: dict,
        parameter_file=None,
        edges: Optional[dict] = None,
    ) -> tuple[list[dict], list[str]]:
        tasks: list[dict] = []
        if edges is None:
            edges = self._session_edges(workflow)
        review: list[str] = []
        # A hand-built Workflow may list Session objects; a parsed one lists
        # instance keys and maps each to its SESSION in session_instances.
        session_map = {(s.name if hasattr(s, 'name') else str(s)): s for s in workflow.sessions}
        instances = getattr(workflow, "session_instances", None) or {}
        ignored_params: dict[str, set] = defaultdict(set)

        ordered = self._topological_sort(dep_graph)

        for name in ordered:
            task_name = instances.get(name, {}).get("task", name)
            session = session_map.get(name)
            if not hasattr(session, "parameters"):
                session = sessions.get(task_name)
            # The notebook belongs to the SESSION (keyed by its name), not to
            # the instance: a reusable session run twice uses one notebook.
            nb_path = notebook_paths.get(task_name, notebook_paths.get(name))
            if nb_path is None:
                # Previously this silently became "/Migrated/<name>", a path
                # that need not exist -- the job would be created pointing at
                # nothing and fail only at run time.
                nb_path = f"/Workspace/Migrated/{name}.ipynb"
                review.append(
                    f"No migrated notebook found for session '{name}'; its task "
                    f"points at the placeholder path {nb_path}, which will fail at "
                    f"run time unless a notebook is uploaded there."
                )

            task: dict = {
                "taskKey": name,
                "type": "NOTEBOOK_TASK",
                "runIf": self._run_if_for(name, workflow, edges),
                "notebookPath": nb_path,
                "dependsOn": [
                    {"taskKey": d} for d in dep_graph.get(name, []) if d
                ],
            }

            # Session-level $$parameter overrides ride along as task
            # parameters. AIDP takes them as a LIST of {name, value}: the
            # flat {name: value} dict this used to emit was rejected with
            # 400 "Unable to process JSON input" (verified 2026-09-25), so
            # any workflow whose session set a parameter could not be
            # deployed. Inside the notebook task they are read with
            # oidlUtils.parameters.getParameter -- see _param().
            # Keyed by the lower-cased name: Informatica parameter names are
            # case-insensitive, so $$Env in one section and $$ENV in a
            # narrower one are the same parameter, not two task parameters.
            params: dict[str, str] = {}
            if session is not None and getattr(session, "parameters", None):
                params.update({k.lstrip("$").lower(): str(v) for k, v in session.parameters.items()})
            if parameter_file is not None:
                for k, v in _parameter_file_values(
                    parameter_file, workflow, name, instances.get(name), task_name,
                ).items():
                    if k.startswith("$$"):
                        params[k.lstrip("$").lower()] = v
                    else:
                        ignored_params[k].add(name)
            if params:
                task["parameters"] = [
                    {"name": f"migration.{k}", "value": v} for k, v in sorted(params.items())
                ]

            tasks.append(task)

        for pname in sorted(ignored_params):
            review.append(
                f"Parameter-file value {pname} not applied to "
                f"{', '.join(sorted(ignored_params[pname]))}: only $$ mapping "
                f"parameters become task parameters. A $DBConnection, "
                f"$InputFile or other session parameter names a connection or "
                f"path that has to be set up in AIDP."
            )

        return tasks, review

    # ------------------------------------------------------------------
    # Schedule
    # ------------------------------------------------------------------

    def _build_schedule(
        self, workflow: Workflow
    ) -> tuple[Optional[dict], list[str], list[str]]:
        """Convert the workflow's scheduler to a Quartz cron, or decline.

        Returns ``(schedule_or_None, not_translated, assumptions)``. Only
        shapes this can convert with confidence produce a schedule;
        everything else returns ``None`` plus an item quoting the raw
        attributes, so a human sets the schedule deliberately instead of
        inheriting a guess.
        """
        sched = dict(workflow.scheduler or {})
        if not sched:
            return None, [
                "No <SCHEDULER> in the export, so the job was created without a "
                "schedule. In PowerCenter that normally means the workflow is "
                "started on demand (pmcmd or the Workflow Manager); confirm that "
                "is the intent rather than a missing schedule."
            ], []

        # Already a job schedule (e.g. hand-edited, or a re-run of a
        # previously generated definition) -- pass through untouched.
        if "quartzCronExpression" in sched:
            return sched, [], []
        if "cronExpression" in sched:  # a definition generated before the key fix
            return {"quartzCronExpression": sched["cronExpression"],
                    "timezoneId": sched.get("timezone", self._timezone),
                    "pauseStatus": "PAUSED"}, [], []

        raw = ", ".join(f"{k}={v}" for k, v in sorted(sched.items()))
        run_kind = " ".join(
            str(sched.get(k, "")) for k in ("SCHEDULETYPE", "RUNOPTIONS", "STARTOPTIONS")
        ).upper()

        # Run-on-demand is faithfully represented by "no schedule".
        if "DEMAND" in run_kind:
            return None, [], []

        cron = self._cron_from_delta(sched)
        if cron:
            # The wall-clock time is derivable from STARTTIME; the timezone is
            # NOT. PowerCenter writes STARTTIME in the Integration Service's
            # local time and records no zone, so "03:30" could be 03:30 in any
            # zone on earth. Only the operator knows which, so
            # ``schedule_timezone`` lets them say -- and when they have, the
            # schedule is a translation rather than a guess and no assumption
            # is reported. Without it we still have to put something in the
            # job definition, so UTC is used and reported, because a job
            # running at the right minute of the wrong hour is the same class
            # of silent defect as the fabricated cron this module was
            # rewritten to remove, just smaller.
            #
            # Created PAUSED either way. A declared timezone removes the
            # timezone doubt, not every reason to check: the cron is derived
            # from PowerCenter's own run options, and a job that starts
            # firing the moment it is deployed is an outward-facing side
            # effect nobody asked for. Unpause it in AIDP (or PUT pauseStatus
            # UNPAUSED) once the schedule is confirmed.
            schedule = {"quartzCronExpression": cron,
                        "timezoneId": self._timezone,
                        "pauseStatus": "PAUSED"}
            if self._timezone_is_declared:
                return schedule, [], []
            return (
                schedule,
                [],
                [
                    f"Schedule converted to `{cron}` but the timezone is an "
                    f"ASSUMPTION: PowerCenter records STARTTIME in the Integration "
                    f"Service's local time with no zone, and this job was created as "
                    f"{_ASSUMED_TIMEZONE}. If that service did not run in "
                    f"{_ASSUMED_TIMEZONE}, the job runs at the wrong hour. Re-run the "
                    f"migrator with --schedule-timezone to set it correctly, or "
                    f"correct the timezone in AIDP."
                ],
            )

        return None, [
            f"Workflow schedule not converted; the job was created WITHOUT a "
            f"schedule. Set it by hand in AIDP from the source values: {raw}."
        ], []

    @classmethod
    def _cron_from_delta(cls, sched: dict) -> Optional[str]:
        """Quartz cron for a fixed-interval PowerCenter schedule, if exact.

        PowerCenter expresses a repeating schedule as DELTAVALUE seconds
        between runs. Only intervals that tile a day or an hour exactly are
        converted -- "every 7 hours" has no faithful cron, and approximating
        it is the behaviour this module exists to stop.
        """
        try:
            delta = int(float(sched.get("DELTAVALUE", 0)))
        except (TypeError, ValueError):
            return None
        if delta <= 0:
            return None

        hour, minute = cls._start_hour_minute(sched)

        if delta == 86400:                      # daily
            return f"0 {minute} {hour} * * ?"
        if delta % 3600 == 0:                   # every N hours
            hours = delta // 3600
            if 24 % hours == 0:
                return f"0 {minute} */{hours} * * ?"
            return None
        if delta % 60 == 0:                     # every N minutes
            minutes = delta // 60
            if 60 % minutes == 0:
                return f"0 */{minutes} * * * ?"
            return None
        return None

    @staticmethod
    def _start_hour_minute(sched: dict) -> tuple[int, int]:
        """Hour and minute from STARTTIME; midnight when unparseable."""
        start = str(sched.get("STARTTIME", "")).strip()
        for fmt in _STARTTIME_FORMATS:
            try:
                dt = datetime.strptime(start, fmt)
                return dt.hour, dt.minute
            except ValueError:
                continue
        m = re.search(r"\b(\d{1,2}):(\d{2})", start)
        if m:
            return int(m.group(1)) % 24, int(m.group(2)) % 60
        return 0, 0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _topological_sort(graph: dict[str, list[str]]) -> list[str]:
        """Kahn's algorithm. Deterministic (alphabetical tie-breaking)."""
        in_degree: dict[str, int] = {n: 0 for n in graph}
        for node, preds in graph.items():
            in_degree.setdefault(node, 0)
            for p in preds:
                in_degree.setdefault(p, 0)

        # Recount using forward adjacency
        fwd: dict[str, list[str]] = defaultdict(list)
        in_deg: dict[str, int] = {n: 0 for n in in_degree}
        for node, preds in graph.items():
            for p in preds:
                if p:
                    fwd[p].append(node)
                    in_deg[node] = in_deg.get(node, 0) + 1

        queue = sorted(n for n, d in in_deg.items() if d == 0)
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for succ in sorted(fwd.get(node, [])):
                in_deg[succ] -= 1
                if in_deg[succ] == 0:
                    queue.append(succ)
                    queue.sort()

        # Append any remaining (cycle handling -- shouldn't happen)
        for n in sorted(in_deg):
            if n not in result:
                result.append(n)

        return result
