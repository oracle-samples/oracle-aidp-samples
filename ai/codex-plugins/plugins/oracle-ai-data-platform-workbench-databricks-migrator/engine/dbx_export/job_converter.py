"""DBX job → AIDP workflow payload.

Slim port of convert_to_dbcJob + convert_dbcjob_to_aidpjob from the reference
notebook (cells 90, 92). Rewrites:
  - workflow name → <prefix>_<sanitized DBX name>
  - task notebookPath → <export_output>/notebooks/<DBX-relative path>
  - cluster_key → AIDP cluster key (via registry built earlier)
  - task_key → sanitized (translate "-() " to "____")
"""

from __future__ import annotations

import base64
import binascii
import posixpath
import re
from typing import Any, Dict, List, Optional, Set

from . import foreach_bridge, sanitizer
from .param_analysis import ParamPlan
from .sanitizer import dbx_to_aidp_path, sanitize_task_key, sanitize_workflow_name


# Databricks condition_task ops are enum names (EQUAL_TO, GREATER_THAN, …).
# AIDP's IF_ELSE_TASK expects a real boolean expression, so map to operators
# or it rejects with JOB_VALIDATE_0018 "not a boolean expression".
_DBX_CONDITION_OP = {
    "EQUAL_TO": "==",
    "NOT_EQUAL": "!=",
    "GREATER_THAN": ">",
    "GREATER_THAN_OR_EQUAL": ">=",
    "LESS_THAN": "<",
    "LESS_THAN_OR_EQUAL": "<=",
}

_TASK_VALUE_TEMPLATE_RE = re.compile(
    r"^\{\{\s*tasks\.(?P<task>[^.{}\s]+)\.values\."
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\}\}$"
)

_TASK_VALUE_REFERENCE_RE = re.compile(
    r"\{\{\s*tasks\.(?P<task>[^.{}\s]+)\.values\."
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\}\}"
)

_JOB_PARAMETER_TEMPLATE_RE = re.compile(
    r"^\{\{\s*job\.parameters\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}$"
)

_STRONG_SENSITIVE_PARAMETER_NAME_RE = re.compile(
    r"(?:passwords?|passwd|passphrase|secrets?|credentials?|"
    r"tokens?(?![a-z])|api[_-]?key|priv(?:ate)?[_-]?key|access[_-]?key|"
    r"ssh[_-]?key|signing[_-]?key|encryption[_-]?key|master[_-]?key|"
    r"license[_-]?key|client[_-]?key|"
    r"(?:account|storage[_-]?account|shared|hmac|subscription|app|aes|jwt|"
    r"deploy|tls|server|service|consumer|sp|principal)[_-]?key|"
    r"ghpat|tokenauth)",
    re.IGNORECASE,
)

_BOUNDED_SENSITIVE_PARAMETER_NAME_RE = re.compile(
    r"(?<![A-Za-z])(?:pwd|key[_-]?id|pat|auth|authorization|bearer|sas|"
    r"signature|sig|cert|certificate|pem|pfx|keystore|oauth)(?![A-Za-z])",
    re.IGNORECASE,
)

_SENSITIVE_PARAMETER_VALUE_RE = re.compile(
    r"(?:"
    r"\bBearer\s+\S+|"                         # Bearer header
    r"\b(?:password|passwd|pwd)\s*=\s*[^;\s]+|"  # connection string
    r"[a-z][a-z0-9+.-]*://[^/\s:@]*:[^@\s/]+@|"  # URI userinfo
    r"\bthin:[^/\s@]+/[^@\s]+@|"               # Oracle JDBC thin userinfo
    r"\b(?:dapi[a-z0-9]+|xox[baprs]-[a-z0-9-]+|AKIA[0-9A-Z]{16})\b|"
    r"[?&](?:sig|signature|token|access_token|api[_-]?key|apikey|"
    r"password|pwd|sas)=|"
    r"hooks\.slack\.com/services/|"
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r")",
    re.IGNORECASE,
)


def _contains_basic_auth_header(value: str) -> bool:
    """Recognize a real Basic base64(user:password) header."""
    for match in re.finditer(
        r"\bBasic\s+([A-Za-z0-9+/]+={0,2})(?=$|\s)",
        value,
        re.IGNORECASE,
    ):
        token = match.group(1)
        try:
            decoded = base64.b64decode(token, validate=True)
        except (binascii.Error, ValueError):
            continue
        if b":" in decoded:
            return True
    return False


def _format_parameter_value_for_log(name: str, value: Any) -> str:
    """Render an override value without leaking likely credentials."""
    text = "" if value is None else str(value)
    normalized_name = re.sub(
        r"(?<=[a-z0-9])(?=[A-Z])", "_", str(name),
    )
    # Paths/files naming a key/certificate store are locations, not
    # credentials. Keep them auditable unless another strong credential term
    # is present. URI/URL are deliberately not exempt: unlike filesystem
    # locations they can carry userinfo, signatures, tokens, or secret refs.
    bounded_name_match = _BOUNDED_SENSITIVE_PARAMETER_NAME_RE.search(
        normalized_name
    )
    if bounded_name_match and re.search(
        r"(?:^|[_-])(?:path|dir|directory|name|file|filename|filepath|location)"
        r"(?:$|[_-])",
        normalized_name,
        re.IGNORECASE,
    ):
        bounded_name_match = None
    if (
        _STRONG_SENSITIVE_PARAMETER_NAME_RE.search(normalized_name)
        or bounded_name_match
        or _SENSITIVE_PARAMETER_VALUE_RE.search(text)
        or _contains_basic_auth_header(text)
    ):
        return "<redacted>"
    return repr(text)


def _rewrite_condition_task_value_operand(
    value: Any,
    workflow_parameter_names: Optional[Set[str]],
) -> Any:
    """Use an explicitly declared workflow parameter in a condition operand."""
    text = "" if value is None else str(value).strip()
    match = _TASK_VALUE_TEMPLATE_RE.fullmatch(text)
    if match and match.group("name") in (workflow_parameter_names or set()):
        return "{{job.parameters." + match.group("name") + "}}"
    return value


def _is_same_job_parameter_reference(value: Any, name: str) -> bool:
    """Return whether a task value forwards the same workflow parameter."""
    match = _JOB_PARAMETER_TEMPLATE_RE.fullmatch(str(value or "").strip())
    return bool(match and match.group(1) == name)


def _fmt_condition_operand(v) -> str:
    """Render a condition_task operand for an AIDP boolean expression.

    Databricks left/right are strings that are either a templated reference
    ("{{job.parameters.x}}") or a literal. AIDP's parser treats a bare token
    like True/False/foo as an undefined identifier → "not a boolean expression".
    Numbers and {{…}} refs pass through; everything else is quoted so it is a
    valid string literal (matching DBX's string-comparison semantics, e.g. the
    "True" == "False" kill-switch).
    """
    s = "" if v is None else str(v)
    if s.startswith("{{") and s.endswith("}}"):
        return s
    # plain int/float literal
    t = s.lstrip("-")
    if t.isdigit() or (t.count(".") == 1 and t.replace(".", "").isdigit()):
        return s
    return '"' + s.replace('"', '\\"') + '"'


def _convert_parameters(items) -> List[Dict[str, str]]:
    if not items:
        return []
    out = []
    for it in items:
        if isinstance(it, dict):
            name = it.get("name")
            value = it.get("default", it.get("value"))
            if name is not None:
                out.append({"name": str(name), "value": str(value) if value is not None else ""})
    return out


def _canonical_workspace_path(value: str) -> str:
    """Normalize Databricks and AIDP spellings of workspace paths."""
    path = value.strip().replace("\\", "/")
    if path.startswith("/Workspace/"):
        path = path[len("/Workspace"):]
    return posixpath.normpath(path)


def _rewrite_workspace_resource_value(
    value: Any,
    workspace_resource_path_map: Optional[Dict[str, str]],
) -> Any:
    """Rewrite an exported file path or its containing parameter directory."""
    if not isinstance(value, str) or not workspace_resource_path_map:
        return value
    source_value = _canonical_workspace_path(value)
    candidates = set()
    for source_file, target_file in workspace_resource_path_map.items():
        source = _canonical_workspace_path(source_file)
        if source == source_value:
            candidates.add(posixpath.normpath(target_file))
            continue
        prefix = source_value.rstrip("/") + "/"
        if not source.startswith(prefix):
            continue
        relative = source[len(prefix):]
        target_root = posixpath.normpath(target_file)
        for _ in [part for part in relative.split("/") if part]:
            target_root = posixpath.dirname(target_root)
        candidates.add(target_root)
    # Multiple files below the same source parameter normally infer the same
    # exported root. Refuse an ambiguous rewrite rather than choosing one.
    return next(iter(candidates)) if len(candidates) == 1 else value


def _rewrite_parameter_values(
    parameters: List[Dict[str, str]],
    workspace_resource_path_map: Optional[Dict[str, str]],
) -> List[Dict[str, str]]:
    for parameter in parameters:
        parameter["value"] = str(_rewrite_workspace_resource_value(
            parameter.get("value", ""), workspace_resource_path_map,
        ))
    return parameters


def _merge_params(base: List[Dict[str, str]],
                  extra: Optional[List[Dict[str, str]]]) -> List[Dict[str, str]]:
    """Merge param-analysis extras (Q2 taskValues declarations) into a task's
    base parameters, deduped by name. A non-empty extra value (the reader's
    {{tasks...}} reference) overrides an empty base entry; base values are
    otherwise preserved."""
    if not extra:
        return base
    by_name = {p["name"]: p for p in base}
    for p in extra:
        cur = by_name.get(p["name"])
        if cur is None:
            base.append(p)
            by_name[p["name"]] = p
        elif p.get("value") and not cur.get("value"):
            cur["value"] = p["value"]
    return base


def _drop_shadowing_job_params(params: List[Dict[str, str]],
                               job_level_names: Optional[set],
                               workflow_parameter_names: Optional[Set[str]] = None,
                               task_key: str = "",
                               conversion_decisions: Optional[List[str]] = None,
                               ) -> List[Dict[str, str]]:
    """Drop redundant task params for a name hoisted to job level.

    Empty placeholders shadow the job-level value (task overrides job). An
    explicitly overridden parameter may also be forwarded back to itself as
    ``{{job.parameters.NAME}}``; that task-level template is equivalent and
    redundant. AIDP job parameters propagate to a task's ``getParameter``
    calls without a task-level declaration (confirmed by PR #44 live
    validation on 2026-08-02). Other non-empty task values are genuine
    overrides and remain.
    """
    if not job_level_names:
        return params
    kept: List[Dict[str, str]] = []
    for parameter in params:
        name = parameter["name"]
        value = parameter.get("value")
        is_explicit_override = name in (workflow_parameter_names or set())
        is_forwarder = (
            is_explicit_override
            and _is_same_job_parameter_reference(value, name)
        )
        is_empty_shadow = name in job_level_names and not value
        if not (is_empty_shadow or is_forwarder):
            kept.append(parameter)
            continue
        if conversion_decisions is not None and is_explicit_override:
            if is_forwarder:
                reason = "forwards the overridden workflow parameter"
            else:
                reason = "uses the overridden workflow parameter"
            conversion_decisions.append(
                f"task {task_key}: removed redundant base_parameter {name} "
                f"{value!r} ({reason})"
            )
    return kept


def _convert_schedule(
    sched: Optional[Dict[str, Any]],
    force_pause: bool = True,
) -> Optional[Dict[str, Any]]:
    """Convert a DBX schedule to the AIDP shape.

    By default the schedule is forced to PAUSED: a freshly-exported workflow is
    not yet known to run cleanly on AIDP, and an active cron would keep firing
    failing runs and wasting compute. Pass force_pause=False to carry over the
    original DBX pause_status instead.
    """
    if not sched:
        return None
    if force_pause:
        pause_status = "PAUSED"
    else:
        pause_status = sched.get("pause_status", "PAUSED")
    return {
        "timezoneId": sched.get("timezone_id"),
        "quartzCronExpression": sched.get("quartz_cron_expression"),
        "pauseStatus": pause_status,
    }


def _convert_depends_on(deps) -> List[Dict[str, Any]]:
    out = []
    for d in deps or []:
        out.append({
            "taskKey": sanitize_task_key(d.get("task_key", "")),
            "outcome": d.get("outcome"),
        })
    return out


def _planned_target(source, paths, *, notebook=False):
    """Use the orchestrator's selected physical identity, including legacy hits."""
    if not paths:
        return None
    if source in paths:
        return paths[source]
    def identity(path):
        path = path.removeprefix("/Workspace/").lstrip("/")
        if notebook:
            for extension in (".ipynb", ".py", ".scala", ".sql", ".r"):
                if path.lower().endswith(extension):
                    path = path[:-len(extension)]
                    break
        return path
    matches = {target for original, target in paths.items() if identity(original) == identity(source)}
    if len(matches) > 1:
        raise ValueError(f"Ambiguous planned notebook/file target for {source!r}: {sorted(matches)!r}")
    return next(iter(matches), None)


def convert_dbx_task(
    dbx_task: Dict[str, Any],
    export_output: str,
    dbx_cluster_key_to_aidp_key: Dict[str, str],
    dbx_existing_cluster_id_to_aidp_key: Optional[Dict[str, str]] = None,
    dbx_task_key_to_aidp_key: Optional[Dict[str, str]] = None,
    force_no_retries: bool = True,
    extra_params: Optional[List[Dict[str, str]]] = None,
    job_level_names: Optional[set] = None,
    workspace_resource_path_map: Optional[Dict[str, str]] = None,
    workflow_parameter_names: Optional[Set[str]] = None,
    conversion_decisions: Optional[List[str]] = None,
    generated_notebooks: Optional[List] = None,
    dbx_job_id: str = "<?>",
    notebook_path_map: Optional[Dict[str, str]] = None,
    file_path_map: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """Convert a single DBX task. Returns None if unsupported task type.

    generated_notebooks: out-param collecting (aidp_path, notebook_dict) for
    tool-generated artifacts (FOREACH_BRIDGE drivers) that Phase 3 must upload
    before creating the workflow. dbx_job_id is cosmetic (driver header).

    extra_params: task-value ({{tasks...}}) parameter declarations discovered by
    param_analysis (setter valueless / reader reference), merged into the
    notebook task's parameters. See dbx_export/param_analysis.py.

    job_level_names: names hoisted to JOB level (Q1). An empty task-level entry
    for such a name is a redundant placeholder that would SHADOW the job-level
    value at runtime (task params override job params) — so it is dropped. A
    non-empty task value is a real per-task override and is kept.

    Cluster resolution checks BOTH registries in this order:
      1. task.job_cluster_key → dbx_cluster_key_to_aidp_key (inline job_clusters[])
      2. task.existing_cluster_id → dbx_existing_cluster_id_to_aidp_key
         (standalone DBX clusters referenced by ID — phase 3 fetches their
         spec and creates AIDP equivalents)
    """
    task_key = sanitize_task_key(dbx_task.get("task_key", ""))
    depends = _convert_depends_on(dbx_task.get("depends_on", []))

    # Map cluster.
    cluster = None
    job_cluster_key = dbx_task.get("job_cluster_key")
    existing_cluster_id = dbx_task.get("existing_cluster_id")
    if job_cluster_key and job_cluster_key in dbx_cluster_key_to_aidp_key:
        cluster = {"clusterKey": dbx_cluster_key_to_aidp_key[job_cluster_key]}
    elif existing_cluster_id and dbx_existing_cluster_id_to_aidp_key \
            and existing_cluster_id in dbx_existing_cluster_id_to_aidp_key:
        cluster = {"clusterKey": dbx_existing_cluster_id_to_aidp_key[existing_cluster_id]}
    elif dbx_task.get("new_cluster") and dbx_task_key_to_aidp_key \
            and dbx_task.get("task_key") in dbx_task_key_to_aidp_key:
        cluster = {"clusterKey": dbx_task_key_to_aidp_key[dbx_task.get("task_key")]}

    run_if = dbx_task.get("run_if") or "ALL_SUCCESS"
    # Reference cell 88: ALL_DONE with no deps → ALL_SUCCESS
    if run_if == "ALL_DONE" and not depends:
        run_if = "ALL_SUCCESS"

    # Respect DBX's task-level timing/retry settings, but clamp the timeout
    # to a 10-hour floor: a freshly-exported workflow on AIDP often needs
    # more time than the DBX original, and a short DBX timeout would just
    # cause spurious task failures. Larger DBX timeouts pass through unchanged.
    # AIDP rejects 0 here too, so any falsy value becomes the floor.
    timeout = max(dbx_task.get("timeout_seconds") or 0, 36000)
    if force_no_retries:
        # A freshly-exported workflow is not yet known to run cleanly on AIDP;
        # retries would just re-run failing tasks and waste compute (same
        # rationale as forcing the schedule PAUSED). Force 0 at the task level
        # (AIDP has no separate job-level retry). --preserve-retries opts out.
        max_retries = 0
        retry_on_timeout = False
    else:
        max_retries = dbx_task.get("max_retries") or 0
        retry_on_timeout = bool(dbx_task.get("retry_on_timeout") or False)
    # AIDP caps minRetryIntervalMillis at 600000 (10 min) and validates the
    # range regardless of maxRetries. When maxRetries is 0 the value is
    # functionally unused; set a safe default. Otherwise clamp DBX's value.
    if max_retries <= 0:
        min_retry_interval_ms = 20
    else:
        min_retry_interval_ms = min(dbx_task.get("min_retry_interval_millis") or 20, 600000)

    nb = dbx_task.get("notebook_task")
    if nb:
        # DBX notebook_path is canonical REST form (/Users/foo@bar/notebook).
        # Use it as-is; dbx_to_aidp_path adds the .ipynb suffix and target
        # prefix (plus a GIT/ segment for git-source tasks, matching the
        # destination path the orchestrator used when uploading the notebook).
        nb_path = nb.get("notebook_path", "").strip()
        is_git = nb.get("source") == "GIT"
        aidp_nb_path = _planned_target(nb_path, notebook_path_map, notebook=True) or dbx_to_aidp_path(
            nb_path, export_output, sanitizer.KIND_NOTEBOOK, is_git=is_git,
        )
        return {
            "type": "NOTEBOOK_TASK",
            "taskKey": task_key,
            "notebookPath": aidp_nb_path,
            "dependsOn": depends,
            "runIf": run_if,
            "maxRetries": max_retries,
            "minRetryIntervalMillis": min_retry_interval_ms,
            "isRetryOnTimeout": retry_on_timeout,
            "cluster": cluster,
            # Order matters: shadow-strip the BASE (widget) params FIRST, then
            # merge the Q2 extras. Stripping after the merge could delete a Q2
            # setter's valueless output declaration ({name, value: ""}) if that
            # key were also job-level-hoisted — breaking {{tasks...}} resolution.
            "parameters": _rewrite_parameter_values(
                _merge_params(
                    _drop_shadowing_job_params(
                        _convert_parameters(
                            [{"name": k, "value": v}
                             for k, v in (nb.get("base_parameters") or {}).items()]
                        ),
                        job_level_names,
                        workflow_parameter_names,
                        task_key,
                        conversion_decisions,
                    ),
                    extra_params,
                ),
                workspace_resource_path_map,
            ),
            "timeoutSeconds": timeout,
        }

    py = dbx_task.get("spark_python_task")
    if py:
        # DBX python_file is canonical REST form too. Pass through unchanged.
        py_path = py.get("python_file", "").strip()
        aidp_py_path = (_planned_target(py_path, file_path_map) or dbx_to_aidp_path(py_path, export_output, sanitizer.KIND_CONFIG)) if py_path else ""
        return {
            "type": "PYTHON_TASK",
            "taskKey": task_key,
            "dependsOn": depends,
            "runIf": run_if,
            "maxRetries": max_retries,
            "minRetryIntervalMillis": min_retry_interval_ms,
            "isRetryOnTimeout": retry_on_timeout,
            "filePath": aidp_py_path,
            "commandLineArguments": "",
            "cluster": cluster,
            "parameters": [],
            "timeoutSeconds": timeout,
        }

    cond = dbx_task.get("condition_task")
    if cond:
        # DBX op is an enum name (EQUAL_TO, GREATER_THAN, …); AIDP needs a real
        # operator or it rejects with JOB_VALIDATE_0018 "not a boolean expression".
        # An unknown/missing op must fail LOUD here: passing the raw enum name
        # through would reproduce exactly the JOB_VALIDATE_0018 failure this
        # mapping was added to fix, silently, at workflow-validation time.
        raw_op = cond.get("op")
        op = _DBX_CONDITION_OP.get(raw_op)
        if op is None:
            raise ValueError(
                f"condition_task {task_key!r}: unknown/missing DBX condition op "
                f"{raw_op!r} — supported ops: {sorted(_DBX_CONDITION_OP)}"
            )
        raw_left = cond.get("left")
        raw_right = cond.get("right")
        rewritten_left = _rewrite_condition_task_value_operand(
            raw_left, workflow_parameter_names,
        )
        rewritten_right = _rewrite_condition_task_value_operand(
            raw_right, workflow_parameter_names,
        )
        if conversion_decisions is not None and rewritten_left != raw_left:
            conversion_decisions.append(
                f"condition {task_key}: left operand {raw_left!r} -> "
                f"{rewritten_left!r}"
            )
        if conversion_decisions is not None and rewritten_right != raw_right:
            conversion_decisions.append(
                f"condition {task_key}: right operand {raw_right!r} -> "
                f"{rewritten_right!r}"
            )
        left = _fmt_condition_operand(rewritten_left)
        right = _fmt_condition_operand(rewritten_right)
        return {
            "type": "IF_ELSE_TASK",
            "taskKey": task_key,
            "dependsOn": depends,
            "runIf": run_if,
            "expressions": [{"key": "A", "value": f"{left} {op} {right}"}],
            "condition": "A",
            "timeoutSeconds": timeout,
            "cluster": cluster,
        }

    # FOREACH_BRIDGE: a supported for_each_task becomes ONE tool-generated,
    # AIDP-native driver notebook task that re-implements only the loop
    # semantics (sequential; DBX concurrency surfaced in driver output). The
    # driver inherits this task's key, so downstream dependsOn edges and
    # clone_workflow are untouched. TEMPORARY: native AIDP for-each replaces
    # this branch on re-export. Unsupported shapes fall through to
    # task_conversion_refusal below and refuse loudly.
    if dbx_task.get("for_each_task") and \
            foreach_bridge.foreach_unsupported_reason(dbx_task) is None:
        nested = foreach_bridge.nested_notebook_task(dbx_task)
        nested_nb = nested["notebook_task"]
        inner_aidp = _planned_target(nested_nb["notebook_path"].strip(), notebook_path_map, notebook=True) or dbx_to_aidp_path(
            nested_nb["notebook_path"].strip(), export_output,
            sanitizer.KIND_NOTEBOOK, is_git=nested_nb.get("source") == "GIT",
        )
        driver_path = (
            posixpath.dirname(inner_aidp) + "/"
            + foreach_bridge.driver_notebook_name(task_key, dbx_job_id)
        )
        # Cross-task inputs need the same reader declaration param_analysis
        # gives every other consumer: the workflow engine resolves the
        # {{tasks.<k>.values.<n>}} reference into this task's parameter, which
        # the driver reads via getParameter. Verified live 2026-10-13: without
        # this the driver task had parameters=[] while sibling consumers
        # carried the declaration — the driver would only ever see its
        # missing-value sentinel.
        fe_def = dbx_task["for_each_task"]
        driver_parameters = []
        kind, detail = foreach_bridge._parse_inputs(fe_def)
        if kind == "value_ref":
            _producer, value_name = detail
            driver_parameters.append(
                {"name": value_name, "value": str(fe_def.get("inputs")).strip()}
            )
        # Iterations ran on the NESTED task's cluster on Databricks — map it
        # first; fall back to the outer task's mapping (usually None).
        nested_cluster = cluster
        nested_key = nested.get("job_cluster_key")
        nested_existing = nested.get("existing_cluster_id")
        if nested_key and nested_key in dbx_cluster_key_to_aidp_key:
            nested_cluster = {"clusterKey": dbx_cluster_key_to_aidp_key[nested_key]}
        elif nested_existing and dbx_existing_cluster_id_to_aidp_key \
                and nested_existing in dbx_existing_cluster_id_to_aidp_key:
            nested_cluster = {"clusterKey": dbx_existing_cluster_id_to_aidp_key[nested_existing]}
        driver_nb = foreach_bridge.build_driver_notebook(
            dbx_task, inner_aidp, job_id=dbx_job_id,
            inner_timeout_seconds=max(nested.get("timeout_seconds") or 0, 36000),
            workspace_resource_path_map=workspace_resource_path_map,
        )
        if generated_notebooks is not None:
            generated_notebooks.append((driver_path, driver_nb))
        if conversion_decisions is not None:
            fe = dbx_task["for_each_task"]
            _inputs = fe.get("inputs")
            _kind, _detail = foreach_bridge._parse_inputs(fe)
            # Log the inputs SHAPE, not contents — a static list can carry
            # sensitive values (review finding 2026-10-14). A task-value
            # reference is a template, safe to show verbatim.
            _inputs_desc = (
                f"task-value ref {_inputs!r}" if _kind == "value_ref"
                else f"static list ({len(_detail)} elements)"
            )
            conversion_decisions.append(
                f"task '{task_key}': for_each_task converted to a generated "
                f"driver notebook {driver_path} (FOREACH_BRIDGE, temporary "
                f"until native AIDP for-each; inputs={_inputs_desc}; "
                f"DBX concurrency={fe.get('concurrency') or 1} approximated "
                f"sequentially; inner notebook {inner_aidp})"
            )
        return {
            "type": "NOTEBOOK_TASK",
            "taskKey": task_key,
            "notebookPath": driver_path,
            "dependsOn": depends,
            "runIf": run_if,
            "maxRetries": max_retries,
            "minRetryIntervalMillis": min_retry_interval_ms,
            "isRetryOnTimeout": retry_on_timeout,
            "cluster": nested_cluster,
            "parameters": driver_parameters,
            "timeoutSeconds": timeout,
        }

    rj = dbx_task.get("run_job_task")
    if rj:
        # AIDP doesn't have a perfect equivalent for sub-job invocation in the
        # supported task types; we drop these tasks and leave a note. The child
        # job itself is still exported as its own AIDP workflow.
        if conversion_decisions is not None:
            conversion_decisions.append(
                f"task '{dbx_task.get('task_key', '<?>')}': run_job_task "
                f"(child job {rj.get('job_id', '<?>')}) dropped from this "
                "workflow by decision — the child job is exported as its own "
                "AIDP workflow"
            )
        return None

    refusal = task_conversion_refusal(dbx_task)
    if refusal:
        raise ValueError(refusal)
    return None


# Task types convert_dbx_task converts (or, for run_job_task, drops by
# documented decision). Everything else refuses loudly — see
# task_conversion_refusal below. Keep in sync with the branches above.
_HANDLED_TASK_KEYS = (
    "notebook_task", "spark_python_task", "condition_task", "run_job_task",
)


def task_conversion_refusal(dbx_task: Dict[str, Any]) -> Optional[str]:
    """Phase-3 refusal reason for a task the converter cannot convert.

    SINGLE source of truth, used by convert_dbx_task (raise at conversion)
    AND by the Phase-1 preflight in __main__ (so --dry-run and the pre-upload
    plan predict the refusal instead of exiting green — the EXBRIDGE dry-run of
    2026-10-13 reported "Task notebooks: 3 OK" for a 4-task job with zero
    findings). A silently dropped task loses its work AND leaves dangling
    dependsOn edges on its dependents ("no half-fixes" / fail-fast creed).
    run_job_task keeps its documented drop-with-note, not a refusal.
    This reporting is PERMANENT; only the for-each refusal text changes when
    the FOREACH_BRIDGE driver (and later native AIDP for-each) lands.
    """
    if any(dbx_task.get(k) for k in _HANDLED_TASK_KEYS):
        return None
    reason = foreach_bridge.foreach_unsupported_reason(dbx_task)
    if reason:
        return reason
    if dbx_task.get("for_each_task"):
        return None  # bridge-convertible for-each — the driver branch handles it
    present_types = sorted(k for k in dbx_task if k.endswith("_task"))
    if present_types:
        return (
            f"task '{dbx_task.get('task_key', '<?>')}': unsupported Databricks "
            f"task type(s) {present_types} — no AIDP equivalent; refusing to "
            "create a workflow with this task silently dropped"
        )
    return None


def validate_workflow_parameter_overrides(
    settings: Dict[str, Any],
    workflow_parameter_overrides: Optional[Dict[str, str]],
    param_plan: Optional[ParamPlan] = None,
) -> List[str]:
    """Validate workflow overrides without creating AIDP artifacts.

    The checks in this function depend only on the Databricks job definition,
    the static parameter plan, and the explicit operator overrides. Keeping
    them pure lets preflight and Phase 3 reject unsafe conversions before any
    cluster is created or any baseline library is installed.

    Returns operator-visible warnings for embedded condition references that
    cannot be redirected safely. Unsafe notebook/task wiring raises
    ``ValueError`` rather than exporting divergent branch and notebook inputs.
    """
    workflow_parameter_names = {
        str(name) for name in (workflow_parameter_overrides or {})
    }
    if not workflow_parameter_names:
        return []

    invalid_names = sorted(
        name for name in workflow_parameter_names
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
    )
    if invalid_names:
        raise ValueError(
            "Invalid workflow parameter name(s): "
            + ", ".join(repr(name) for name in invalid_names)
        )

    task_value_parameter_names = {
        str(parameter.get("name"))
        for parameters in (
            (param_plan.task_extra_params or {}).values() if param_plan else []
        )
        for parameter in parameters
        if parameter.get("name") is not None
    }
    for task in settings.get("tasks", []) or []:
        base_parameters = (task.get("notebook_task") or {}).get(
            "base_parameters"
        ) or {}
        for name, value in base_parameters.items():
            for match in _TASK_VALUE_REFERENCE_RE.finditer(
                str(value or "")
            ):
                # Track both the receiving task parameter and the referenced
                # task-value key. Aliased and embedded references can otherwise
                # let the condition and notebook observe different values.
                task_value_parameter_names.add(str(name))
                task_value_parameter_names.add(match.group("name"))
    conflicts = sorted(workflow_parameter_names & task_value_parameter_names)
    if conflicts:
        raise ValueError(
            "Workflow parameter override conflicts with task-value wiring: "
            + ", ".join(conflicts)
        )

    condition_task_value_producers: Dict[str, Set[str]] = {}
    warnings: List[str] = []
    for task in settings.get("tasks", []) or []:
        condition = task.get("condition_task") or {}
        task_key = sanitize_task_key(task.get("task_key", ""))
        for side in ("left", "right"):
            operand = condition.get(side)
            text = str(operand or "")
            exact_match = _TASK_VALUE_TEMPLATE_RE.fullmatch(text.strip())
            for match in _TASK_VALUE_REFERENCE_RE.finditer(text):
                name = match.group("name")
                if name not in workflow_parameter_names:
                    continue
                if exact_match:
                    condition_task_value_producers.setdefault(name, set()).add(
                        match.group("task")
                    )
                    continue
                warnings.append(
                    f"WARN condition {task_key}: {side} operand still references "
                    f"tasks.{match.group('task')}.values.{name} inside a larger "
                    f"expression; override {name} does not redirect it"
                )
    ambiguous_names = sorted(
        name for name, producers in condition_task_value_producers.items()
        if len(producers) > 1
    )
    if ambiguous_names:
        details = "; ".join(
            f"{name} (tasks: {', '.join(sorted(condition_task_value_producers[name]))})"
            for name in ambiguous_names
        )
        raise ValueError(
            "Workflow parameter override is ambiguous across condition "
            "task-value producers: " + details
        )

    shadowed_parameter_tasks: Dict[str, List[str]] = {}
    for task in settings.get("tasks", []) or []:
        base_parameters = (task.get("notebook_task") or {}).get(
            "base_parameters"
        ) or {}
        for name in workflow_parameter_names:
            if name not in base_parameters:
                continue
            task_value = base_parameters.get(name)
            if task_value in (None, ""):
                continue
            if _is_same_job_parameter_reference(task_value, name):
                continue
            override_value = (workflow_parameter_overrides or {}).get(name)
            if str(task_value) != str(override_value):
                shadowed_parameter_tasks.setdefault(name, []).append(
                    sanitize_task_key(task.get("task_key", ""))
                )
    if shadowed_parameter_tasks:
        details = "; ".join(
            f"{name} (task(s): {', '.join(tasks)})"
            for name, tasks in sorted(shadowed_parameter_tasks.items())
        )
        raise ValueError(
            "Workflow parameter override is shadowed by a non-empty task "
            "base_parameter: " + details + ". Align the task base_parameter "
            "with the workflow override, remove the task-level value, or "
            "drop the workflow override."
        )

    parameter_name_counts: Dict[str, int] = {}
    for parameter in _convert_parameters(settings.get("parameters", [])):
        name = str(parameter["name"])
        parameter_name_counts[name] = parameter_name_counts.get(name, 0) + 1
    duplicate_names = sorted(
        name for name in workflow_parameter_names
        if parameter_name_counts.get(name, 0) > 1
    )
    if duplicate_names:
        raise ValueError(
            "Workflow parameter override matches duplicate DBX parameter "
            "name(s): " + ", ".join(duplicate_names)
        )

    return list(dict.fromkeys(warnings))


def convert_dbx_job(
    job_data: Dict[str, Any],
    export_output: str,
    prefix: str,
    dbx_cluster_key_to_aidp_key: Dict[str, str],
    dbx_existing_cluster_id_to_aidp_key: Optional[Dict[str, str]] = None,
    dbx_task_key_to_aidp_key: Optional[Dict[str, str]] = None,
    aidp_default_cluster_key: Optional[str] = None,
    force_pause_schedule: bool = True,
    param_plan: Optional[ParamPlan] = None,
    workspace_resource_path_map: Optional[Dict[str, str]] = None,
    workflow_parameter_overrides: Optional[Dict[str, str]] = None,
    conversion_decisions: Optional[List[str]] = None,
    generated_notebooks: Optional[List] = None,
    notebook_path_map: Optional[Dict[str, str]] = None,
    file_path_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build the AIDP job payload (without the shell — caller POSTs shell first).

    Returns a dict ready for PUT /jobs/{key} with name, tasks, jobClusters, etc.

    param_plan: optional result of param_analysis.analyze_task_params — hoists
    shared widget params to job level (Q1) and injects taskValues {{tasks...}}
    references onto reader/setter tasks (Q2).
    """
    settings = job_data.get("settings") or job_data
    dbx_name = settings.get("name") or f"dbx_job_{job_data.get('job_id','unknown')}"
    aidp_name = sanitize_workflow_name(dbx_name, prefix)

    # Only an explicit --workflow-param may redirect a task-value condition to
    # a job parameter. Existing DBX job parameters are not evidence that the
    # upstream-computed value should be ignored; absent configuration is inert.
    workflow_parameter_names = {
        str(name) for name in (workflow_parameter_overrides or {})
    }
    validation_warnings = validate_workflow_parameter_overrides(
        settings,
        workflow_parameter_overrides,
        param_plan,
    )

    # Build jobClusters[] from the AIDP cluster registry (one entry per
    # distinct AIDP cluster used by any task).
    aidp_cluster_keys = sorted({k for k in dbx_cluster_key_to_aidp_key.values() if k})
    aidp_cluster_keys.extend(sorted({
        k for k in (dbx_task_key_to_aidp_key or {}).values()
        if k and k not in aidp_cluster_keys
    }))
    if aidp_default_cluster_key and aidp_default_cluster_key not in aidp_cluster_keys:
        aidp_cluster_keys.append(aidp_default_cluster_key)
    job_clusters = [{"clusterKey": k} for k in aidp_cluster_keys]

    # Preserve pre-feature Q1 behavior, and scope the new shadow stripping to
    # names the operator explicitly supplied through --workflow-param.
    shadowing_job_level_names = (
        set(param_plan.job_level_params) if param_plan else set()
    )
    shadowing_job_level_names.update(workflow_parameter_names)

    tasks = []
    condition_decisions: List[str] = []
    for t in settings.get("tasks", []):
        extra_params = None
        if param_plan and param_plan.task_extra_params:
            extra_params = param_plan.task_extra_params.get(
                sanitize_task_key(t.get("task_key", "")))
        converted = convert_dbx_task(
            t, export_output,
            dbx_cluster_key_to_aidp_key,
            dbx_existing_cluster_id_to_aidp_key=dbx_existing_cluster_id_to_aidp_key,
            dbx_task_key_to_aidp_key=dbx_task_key_to_aidp_key,
            extra_params=extra_params,
            job_level_names=shadowing_job_level_names,
            workspace_resource_path_map=workspace_resource_path_map,
            workflow_parameter_names=workflow_parameter_names,
            conversion_decisions=condition_decisions,
            generated_notebooks=generated_notebooks,
            dbx_job_id=str(job_data.get("job_id", "<?>")),
            notebook_path_map=notebook_path_map,
            file_path_map=file_path_map,
        )
        if converted:
            # Default cluster fallback if task had no job_cluster_key
            # AND no existing_cluster_id we could map.
            if not converted.get("cluster") and aidp_default_cluster_key:
                converted["cluster"] = {"clusterKey": aidp_default_cluster_key}
            tasks.append(converted)

    job_params = _convert_parameters(settings.get("parameters", []))
    # Add observed run-context bindings. Undeclared widgets are NOT hoisted.
    if param_plan and param_plan.job_level_params:
        existing = {p["name"] for p in job_params}
        for name, val in sorted(param_plan.job_level_params.items()):
            if name not in existing:
                job_params.append({"name": name, "value": val})
                existing.add(name)

    # Translate DBX-originated parameter values before applying explicit AIDP
    # overrides. An operator-provided --workflow-param value is already a
    # target-side value and must not be silently remapped as source metadata.
    _rewrite_parameter_values(job_params, workspace_resource_path_map)

    by_name = {p["name"]: p for p in job_params}
    parameter_decisions: List[str] = []
    for name, value in (workflow_parameter_overrides or {}).items():
        name = str(name)
        old_display = (
            _format_parameter_value_for_log(name, by_name[name]["value"])
            if name in by_name else "<not declared>"
        )
        new_display = _format_parameter_value_for_log(name, value)
        parameter_decisions.append(
            f"parameter {name}: exported default {old_display} -> "
            f"operator override {new_display}"
        )
        if name in by_name:
            by_name[name]["value"] = str(value)
        else:
            parameter = {"name": name, "value": str(value)}
            job_params.append(parameter)
            by_name[name] = parameter

    payload = {
        "name": aidp_name,
        "path": "jobs",
        "description": f"JobID:{job_data.get('job_id','?')} {dbx_name}",
        "maxConcurrentRuns": settings.get("max_concurrent_runs", 1),
        "jobClusters": job_clusters,
        "tasks": tasks,
        "parameters": job_params,
        # Clamp job-level timeout to a 10-hour floor (same rationale as task
        # timeouts above — freshly-exported workflows on AIDP often need more
        # headroom). Larger DBX values pass through.
        "timeoutSeconds": max(settings.get("timeout_seconds") or 0, 36000),
    }
    sched = _convert_schedule(settings.get("schedule"), force_pause=force_pause_schedule)
    if sched:
        payload["schedule"] = sched
    if conversion_decisions is not None:
        conversion_decisions.extend(parameter_decisions)
        conversion_decisions.extend(validation_warnings)
        conversion_decisions.extend(condition_decisions)
    return payload
