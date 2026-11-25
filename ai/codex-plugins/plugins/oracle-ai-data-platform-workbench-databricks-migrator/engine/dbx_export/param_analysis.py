"""Static parameter analysis for DBX → AIDP workflow authoring.

Two problems this solves, both invisible in the DBX *job definition* (they live
in notebook CODE, which job_converter otherwise never sees):

Q1 — shared widget parameters.
    A task reads `dbutils.widgets.get("model_name")` but doesn't declare it
    (no `widgets.text(...)` and no task `base_parameters` entry). In DBX these
    values are supplied at run level (`notebook_params`) and broadcast to every
    task. The AIDP-correct model is a JOB-LEVEL parameter, visible to every
    task's getParameter. We hoist the union of read-but-undeclared widget names
    to job level.

Q2 — cross-task values (dbutils.jobs.taskValues).
    A setter task calls `taskValues.set("fl_score_table", ...)`; a reader task
    calls `taskValues.get(taskKey="FL_table_scoring", key="fl_score_table")`.
    AIDP resolves cross-task values only via a task-parameter reference
    `{{tasks.<setter>.values.<key>}}` in the workflow definition. We author:
      - on the reader task: parameter  key = "{{tasks.<setter>.values.<key>}}"
      - on the setter task: parameter  key = ""   (declared output, valued at run)
    The notebook code itself is NOT changed here — the migration rename turns
    taskValues.get→getParameter / set→setTaskValue, and getParameter resolves
    the reference (production: AIDP; single-cluster validation: the file-shim).

Only LITERAL keys/taskKeys are wired; dynamic (computed) ones are reported as
warnings and left to the runtime shim. Pure static analysis — no cluster, no
network.
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .sanitizer import sanitize_task_key

# ── widget read / declaration regexes (mirror build_dag_from_workflow Fix 4 and
#    job_migrate.extract_widget_reads_and_decls — kept intentionally duplicated
#    to avoid importing heavy runtime modules into the exporter). ────────────
_WIDGET_READ_RE_QUOTED = re.compile(
    r'''(?:dbutils\.widgets\.get|'''
    r'''oidlUtils\.notebook\.get_parameters\s*\(\s*\)\s*\.get|'''
    r'''oidlUtils\.parameters\.getParameter)\s*\(\s*(['"])([A-Za-z_][A-Za-z0-9_]*)\1'''
)
_WIDGET_DECL_RE = re.compile(
    r'''dbutils\.widgets\.(?:text|dropdown|combobox|multiselect)\s*\(\s*'''
    r'''(['"])([A-Za-z_][A-Za-z0-9_]*)\1\s*,\s*(['"])([^'"]*)\3'''
)
# Migrated setter form: oidlUtils.parameters.setTaskValue("key", ...). The DBX
# form dbutils.jobs.taskValues.set is handled by AST in _taskvalue_calls; this
# regex additionally catches ALREADY-MIGRATED setters so that on re-export a
# setter key is still recognized (and thus excluded from the Q1 widget hoist —
# its migrated getParameter read otherwise matches _WIDGET_READ_RE_QUOTED).
_SETTASKVALUE_RE = re.compile(
    r'''oidlUtils\.parameters\.setTaskValue\s*\(\s*(['"])([A-Za-z_][A-Za-z0-9_]*)\1'''
)


@dataclass
class ParamPlan:
    """Result of analyze_task_params.

    job_level_params: name -> value to declare at JOB level for an observed
        jobRunId context read. Undeclared widget reads are NOT hoisted.
    task_extra_params: SANITIZED task_key -> [{"name","value"}] to add to that
        task's parameters (Q2 setter/reader declarations).
    warnings: human-readable notes for the operator (dynamic keys, missing
        depends_on edges, duplicate setters).
    """
    job_level_params: Dict[str, str] = field(default_factory=dict)
    task_extra_params: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


def _code_cell_sources(nb_dict: Optional[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    if not nb_dict:
        return out
    for cell in nb_dict.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell.get("source")
        out.append("".join(src) if isinstance(src, list) else (src or ""))
    return out


def _active(src: str) -> str:
    """Drop comment and %magic lines so a commented call isn't counted and the
    remainder is closer to valid Python for ast.parse."""
    return "\n".join(
        ln for ln in src.splitlines()
        if ln.strip() and not ln.strip().startswith("#") and not ln.lstrip().startswith("%")
    )


def _widget_reads(sources: List[str]) -> set:
    names = set()
    for s in sources:
        for m in _WIDGET_READ_RE_QUOTED.finditer(_active(s)):
            names.add(m.group(2))
    return names


def _widget_decls(sources: List[str]) -> Dict[str, str]:
    decls: Dict[str, str] = {}
    for s in sources:
        for m in _WIDGET_DECL_RE.finditer(_active(s)):
            decls.setdefault(m.group(2), m.group(4))
    return decls


def _is_taskvalues_attr(func: ast.AST, method: str) -> bool:
    """True if `func` is the attribute chain dbutils.jobs.taskValues.<method>."""
    if not (isinstance(func, ast.Attribute) and func.attr == method):
        return False
    tv = func.value
    if not (isinstance(tv, ast.Attribute) and tv.attr == "taskValues"):
        return False
    jobs = tv.value
    if not (isinstance(jobs, ast.Attribute) and jobs.attr == "jobs"):
        return False
    return isinstance(jobs.value, ast.Name) and jobs.value.id == "dbutils"


def _literal_str(node: Optional[ast.AST]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _kw(call: ast.Call, name: str) -> Optional[ast.AST]:
    for k in call.keywords:
        if k.arg == name:
            return k.value
    return None


def _taskvalue_calls(sources: List[str]):
    """Yield ('set', key) and ('get', taskKey, key) tuples for literal calls;
    yield ('dynamic', raw) markers for non-literal set/get so callers can warn."""
    for s in sources:
        active = _active(s)
        try:
            tree = ast.parse(active)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _is_taskvalues_attr(node.func, "set"):
                # set(key, value) — key is arg0 or kw 'key'
                key = _literal_str(_kw(node, "key")) or _literal_str(
                    node.args[0] if node.args else None)
                yield ("set", key) if key is not None else ("dynamic", "set")
            elif _is_taskvalues_attr(node.func, "get"):
                # get(taskKey, key, default, debugValue) — positional order
                task_key = _literal_str(_kw(node, "taskKey")) or _literal_str(
                    node.args[0] if len(node.args) >= 1 else None)
                key = _literal_str(_kw(node, "key")) or _literal_str(
                    node.args[1] if len(node.args) >= 2 else None)
                if key is not None:
                    yield ("get", task_key, key)
                else:
                    yield ("dynamic", "get")


def _reads_job_run_context(sources: List[str], warn) -> bool:
    """Recognize literal Python context reads; author metadata, never rewrite code.

    Track straight-line aliases across cells. Unknown control flow invalidates
    aliases rather than treating an arbitrary dictionary's jobRunId as context.
    """
    names = {"dbutils": "dbutils", "json": "json"}
    methods = {
        ("entry_point", "getDbutils"): "utils",
        ("utils", "notebook"): "notebook",
        ("notebook", "getContext"): "context",
        ("context", "toJson"): "json_text",
        ("context", "tags"): "tags",
        ("run_option", "get"): "run_id",
    }

    def kind(node):
        if isinstance(node, ast.Name):
            return names.get(node.id)
        if isinstance(node, ast.Attribute):
            return {
                ("dbutils", "notebook"): "notebook",
                ("notebook", "entry_point"): "entry_point",
                ("json", "loads"): "loads",
            }.get((kind(node.value), node.attr))
        if isinstance(node, ast.Call) and not node.keywords:
            if kind(node.func) == "loads" and len(node.args) == 1:
                return "context_dict" if kind(node.args[0]) == "json_text" else None
            if isinstance(node.func, ast.Attribute):
                owner, method = kind(node.func.value), node.func.attr
                if not node.args:
                    return methods.get((owner, method))
                if method == "get" and 1 <= len(node.args) <= 2:
                    return {
                        ("context_dict", "tags"): "json_tags",
                        ("json_tags", "jobRunId"): "run_id",
                        ("tags", "jobRunId"): "run_option",
                    }.get((owner, _literal_str(node.args[0])))
        if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load):
            return {
                ("context_dict", "tags"): "json_tags",
                ("json_tags", "jobRunId"): "run_id",
            }.get((kind(node.value), _literal_str(node.slice)))
        return None

    found = False
    saw_context = False
    context_kinds = {"context", "json_text", "context_dict", "json_tags", "tags", "run_option"}
    for index, source in enumerate(sources):
        first = next((line.strip() for line in source.splitlines()
                      if line.strip() and not line.lstrip().startswith("#")), "")
        if first.startswith(("%", "!")) and not first.startswith("%python"):
            if "getContext" in source and "jobRunId" in source:
                warn(f"cell {index}: non-Python run-context lookup UNRESOLVED; not wired")
            continue
        try:
            tree = ast.parse(_active(source))
        except SyntaxError:
            # Magics/non-Python cells are not Python context evidence.
            if "jobRunId" in source or "getContext" in source:
                warn(f"cell {index}: run-context scan UNRESOLVED (not valid Python)")
            names.clear()
            continue
        for statement in tree.body:
            nodes = list(ast.walk(statement))
            # A dictionary write is not a context read. Aliases may share the
            # mutated dictionary, so conservatively forget mutable provenance;
            # a fresh context read can establish it again. Do not interpret writes.
            mutated = any(
                isinstance(node, ast.Subscript) and isinstance(node.ctx, (ast.Store, ast.Del))
                and any(kind(owner) in ("context_dict", "json_tags")
                        for owner in ast.walk(node.value))
                for node in nodes
            )
            # A read-only method can return an unrelated/untracked value.
            # Keep receiver provenance for get/accessors regardless of key;
            # kind() alone decides whether their result proves a run-ID read.
            unknown_method = any(
                isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and kind(node.func.value) in context_kinds
                and not (
                    (kind(node.func.value), node.func.attr) in methods
                    or (kind(node.func.value) in ("context_dict", "json_tags", "tags")
                        and node.func.attr == "get")
                )
                for node in nodes
            )
            if mutated or unknown_method:
                names = {name: value for name, value in names.items()
                         if value not in context_kinds}
                warn(f"cell {index}: context mutation or unmodeled method UNRESOLVED; provenance not proven, aliases not wired")
            straight_line = isinstance(statement, (ast.Assign, ast.AnnAssign, ast.Expr))
            # Inner scopes may shadow a context alias; do not infer across them.
            straight_line &= not any(isinstance(node, (
                ast.Lambda, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp,
            )) for node in nodes)
            saw_context |= any(kind(node) == "context" for node in nodes)
            matched = straight_line and any(kind(node) == "run_id" for node in nodes)
            found |= matched
            lookups = [
                (node.value, node.slice) for node in nodes
                if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load)
            ] + [
                (node.func.value, node.args[0]) for node in nodes
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "get" and node.args
            ]
            if not matched and (
                any((_literal_str(key) == "jobRunId" and isinstance(owner, (ast.Name, ast.Subscript, ast.Call)))
                    or kind(owner) in ("json_tags", "tags")
                    for owner, key in lookups)
                or (saw_context and any(_literal_str(node) == "jobRunId" for node in nodes))
            ):
                warn(f"cell {index}: jobRunId lookup not proven to be Databricks context; not wired")
            value = kind(statement.value) if isinstance(statement, (ast.Assign, ast.AnnAssign)) else None
            # Invalidate assignments, deletes, and names bound in compound scopes.
            for node in nodes:
                if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
                    names.pop(node.id, None)
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.pop(statement.name, None)
            if isinstance(statement, (ast.Assign, ast.AnnAssign)):
                targets = statement.targets if isinstance(statement, ast.Assign) else [statement.target]
                for target in targets:
                    if isinstance(target, ast.Name) and value:
                        names[target.id] = value
            elif isinstance(statement, ast.Import):
                for alias in statement.names:
                    names[alias.asname or alias.name] = "json" if alias.name == "json" else None
            elif isinstance(statement, ast.ImportFrom):
                for alias in statement.names:
                    names[alias.asname or alias.name] = (
                        "loads" if statement.module == "json" and alias.name == "loads" else None
                    )
    return found


def analyze_task_params(
    job_settings: Dict[str, Any],
    nb_dict_by_task: Dict[str, Any],
    log=None,
    context_dependencies: Optional[Dict[str, List[Dict[str, Any]]]] = None,
) -> ParamPlan:
    """Analyze a job's task notebooks and return the parameter plan.

    job_settings: the DBX job settings dict (has tasks[] with task_key,
        notebook_task.base_parameters, depends_on, and job-level parameters[]).
    nb_dict_by_task: {dbx_task_key: parsed notebook dict} for notebook tasks.
    context_dependencies: already-downloaded dependency notebooks per task;
        scanned for context bindings and widget conflicts, not widget/taskValue hoisting.
    """
    plan = ParamPlan()

    def warn(msg: str):
        plan.warnings.append(msg)
        if log:
            log(f"  [param-analysis] {msg}")

    tasks = job_settings.get("tasks", []) or []
    # Names already declared at job level in the DBX definition.
    existing_job_params = {
        p.get("name") for p in (job_settings.get("parameters") or []) if p.get("name")
    }
    context_readers = []
    context_widget_tasks = set()

    # Per-task collected facts.
    per_task = {}  # dbx_task_key -> {"reads","decls","base","deps","sources"}
    global_decls: Dict[str, str] = {}
    setters: Dict[str, str] = {}  # value key -> sanitized setter task key

    for t in tasks:
        tk = t.get("task_key", "")
        nb = t.get("notebook_task") or {}
        base = set((nb.get("base_parameters") or {}).keys())
        deps = {sanitize_task_key(d.get("task_key", ""))
                for d in (t.get("depends_on") or []) if d.get("task_key")}
        sources = _code_cell_sources(nb_dict_by_task.get(tk))
        context_notebooks = [nb_dict_by_task.get(tk)] + (context_dependencies or {}).get(tk, [])
        for context_notebook in context_notebooks:
            # A job-wide binding overrides declarations in children as well.
            if "jobRunId" in _widget_decls(_code_cell_sources(context_notebook)):
                context_widget_tasks.add(tk)
            if _reads_job_run_context(
                _code_cell_sources(context_notebook),
                lambda message: warn(f"task '{tk}': {message}"),
            ):
                context_readers.append(tk)
        reads = _widget_reads(sources)
        decls = _widget_decls(sources)
        for n, d in decls.items():
            if n in global_decls and global_decls[n] != d:
                warn(f"widget '{n}' declared with different defaults across "
                     f"tasks ({global_decls[n]!r} vs {d!r}); first-seen "
                     f"({global_decls[n]!r}) is used for the job-level hoist")
            global_decls.setdefault(n, d)
        per_task[tk] = {"reads": reads, "decls": set(decls), "base": base,
                        "deps": deps, "sources": sources}
        # record setters
        for call in _taskvalue_calls(sources):
            if call[0] == "set":
                key = call[1]
                if key in setters and setters[key] != sanitize_task_key(tk):
                    warn(f"taskValue '{key}' is set by multiple tasks "
                         f"({setters[key]}, {sanitize_task_key(tk)}); "
                         f"references will point at the last one")
                setters[key] = sanitize_task_key(tk)
            elif call == ("dynamic", "set"):
                warn(f"task '{tk}': taskValues.set() with a non-literal key — "
                     f"cannot declare the output statically; runtime shim only")
        # Also register already-migrated setters (oidlUtils...setTaskValue) so a
        # re-export recognizes them as setter keys and excludes them from the
        # Q1 hoist (see _SETTASKVALUE_RE).
        for s in sources:
            for m in _SETTASKVALUE_RE.finditer(_active(s)):
                setters.setdefault(m.group(2), sanitize_task_key(tk))

    # ── Q1 REMOVED (2026-08-05): do NOT hoist read-but-undeclared widgets to
    # job level. Confirmed against the AIDP widget PRD ("Support for aidputils
    # widgets" / "Notebook AIDP Utils Widget Support"): a widget must be
    # DECLARED in the notebook (aidputils.widgets.text/dropdown/…) to be
    # resolvable, and a Job/Task parameter overrides a widget ONLY when its name
    # matches a DECLARED widget ("if widget names match parameter names,
    # parameter values override widget defaults"). Hoisting a read-but-undeclared
    # widget as a job param with "" therefore (a) can't legitimately populate
    # anything on AIDP and (b) injected an empty value that BROKE notebooks whose
    # own code detects "was this widget supplied?" via try/except around get()
    # (the pilot deployment USE_ENSEMBLE → _widget_supplied wrongly True → silently forced
    # SCORING_MODE='dl_only', skipping the ensemble). Undeclared reads now raise
    # on AIDP exactly like Databricks; the notebook's own default/try-except
    # handles them. DBX job-level params + base_parameters still carry over
    # (job_converter._convert_parameters); Q2 cross-task taskValues below is
    # unaffected. Only explicit context reads are wired below.
    #
    # Setter keys are still recorded above for Q2; nothing to hoist here.

    if context_readers:
        # [verified: 2026-10, docs] System parameters, Table 8-1; job parameters
        # remain immutable across tasks for the workflow run:
        # https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/parameters.html
        binding = "{{job.run_id}}"
        for parameter in job_settings.get("parameters") or []:
            if parameter.get("name") == "jobRunId" and parameter.get("default", parameter.get("value")) != binding:
                raise ValueError("jobRunId context binding conflicts with an existing job parameter; not overwritten")
        for task in tasks:
            task_key = task.get("task_key", "")
            if task_key in context_widget_tasks:
                raise ValueError(f"task '{task_key}': jobRunId context binding conflicts with an existing widget declaration; not overwritten")
            base_parameters = (task.get("notebook_task") or {}).get("base_parameters") or {}
            value = base_parameters.get("jobRunId")
            if value is not None and value != binding:
                raise ValueError(f"task '{task.get('task_key')}': jobRunId context binding conflicts with an existing task parameter; not overwritten")
        plan.job_level_params["jobRunId"] = binding
        if log:
            log(f"  [param-analysis] jobRunId={binding} registered for context readers: {sorted(set(context_readers))}")

    # Transitive ancestor sets (sanitized keys). A {{tasks.X.values.K}}
    # reference resolves as long as X ran before the reader — i.e. X is a
    # TRANSITIVE ancestor, not necessarily a direct dependency. Checking only
    # direct depends_on would false-warn on the common chain A→B→C where C
    # reads a value B produced from A.
    direct_by_key = {sanitize_task_key(k): f["deps"] for k, f in per_task.items()}

    def _ancestors(sk: str) -> set:
        seen, stack = set(), list(direct_by_key.get(sk, ()))
        while stack:
            a = stack.pop()
            if a in seen:
                continue
            seen.add(a)
            stack.extend(direct_by_key.get(a, ()))
        return seen

    # ── Q2: wire taskValues.get → {{tasks.<setter>.values.<key>}} ─────────
    for tk, f in per_task.items():
        reader = sanitize_task_key(tk)
        for call in _taskvalue_calls(f["sources"]):
            if call[0] != "get":
                if call == ("dynamic", "get"):
                    warn(f"task '{tk}': taskValues.get() with a non-literal "
                         f"key/taskKey — cannot wire {{{{tasks...}}}} statically; "
                         f"runtime shim only")
                continue
            _tag, task_key_arg, key = call
            src_task = sanitize_task_key(task_key_arg) if task_key_arg else setters.get(key)
            if not src_task:
                warn(f"task '{tk}': taskValues.get(key='{key}') has no resolvable "
                     f"source task (no taskKey arg and no setter found) — skipped")
                continue
            if src_task == reader:
                # A task that both sets and reads the same key would get a
                # self-reference {{tasks.<self>.values.k}}, which AIDP cannot
                # resolve (a task can't read its own task value). The in-kernel
                # value is already live, so no wiring is needed — skip + warn.
                warn(f"task '{tk}' both sets and reads taskValue '{key}' — "
                     f"skipping self-reference (a task cannot read its own "
                     f"task value via {{{{tasks...}}}})")
                continue
            ref = f"{{{{tasks.{src_task}.values.{key}}}}}"
            _add_param(plan.task_extra_params, reader, key, ref)
            _add_param(plan.task_extra_params, src_task, key, "")  # setter declares output
            if src_task not in _ancestors(reader):
                warn(f"task '{tk}' reads taskValue '{key}' from '{src_task}' but "
                     f"'{src_task}' is not a (transitive) upstream task — the "
                     f"{{{{tasks...}}}} reference will not resolve at runtime "
                     f"unless a dependency path exists")

    return plan


def _add_param(bucket: Dict[str, List[Dict[str, str]]], task_key: str,
               name: str, value: str):
    """Add {name,value} to a task's extra-params list, deduped by name.
    A non-empty value (the reader reference) wins over an empty declaration."""
    lst = bucket.setdefault(task_key, [])
    for p in lst:
        if p["name"] == name:
            if value and not p["value"]:
                p["value"] = value
            return
    lst.append({"name": name, "value": value})
