"""FOR-EACH BRIDGE — grep: FOREACH_BRIDGE

AIDP has no native for-each task yet (platform team estimate: a couple of
months from 2026-10). Until it ships, Databricks ``for_each_task`` needs a
bridge; this module is the single home for every bridge decision
(`grep -rn FOREACH_BRIDGE` finds every call site).

Temporary vs permanent — be precise when the native feature arrives:
- PERMANENT: ``nested_notebook_task`` (Databricks' nested task structure must
  be understood regardless of how AIDP represents the loop), the resolver's
  nested-notebook discovery, and loud refusal reporting for unsupported tasks.
- TEMPORARY: the driver-notebook conversion (and the "bridge pending" refusal
  text below) — replace with the native task conversion, then delete only
  those parts.

Evidence (live dry-run, 2026-10-13, Example_Base_Job 1000000000000001 on cluster
cccccc16): the exporter silently dropped the ``for_each_task`` — a 4-task job
exported "Task notebooks: 3 OK" with zero findings, the nested notebook
(``notebooks/generate_base``) was never discovered, and the downstream task
kept a dangling ``dependsOn``. Silent coverage loss violates the fail-fast
contract ("every silent tool decision must be surfaced").

Current scope: nested-task discovery, loud refusal for unsupported shapes,
and the driver-notebook conversion (one tool-generated AIDP-native task that
loops the nested notebook via ``notebook.run`` with the element's parameters).

KNOWN LIMITATION (unobserved input, loud at runtime): param_analysis declares
a producer's valueless task-value output only when some notebook reads it via
``taskValues.get``. If a for-each's ``inputs`` reference were the ONLY
consumer of that value, the producer-side declaration would be absent and the
driver's getParameter would hit its missing-value sentinel — a loud RuntimeError
naming the value and producer, never a silent skip. EXBRIDGE has other consumers,
so the handoff is complete there (live-verified 2026-10-13).
"""
import json
import re
from typing import Any, Dict, Optional


def nested_notebook_task(dbx_task: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the single task spec nested in a ``for_each_task``, else None.

    The DBX jobs API guarantees exactly one nested ``task`` per for-each;
    every iteration runs that same spec with the element bound to
    ``{{input}}`` — verified live against jobs/get 2.2 (EXBRIDGE, 2026-10-13).
    """
    fe = dbx_task.get("for_each_task")
    if not isinstance(fe, dict):
        return None
    nested = fe.get("task")
    return nested if isinstance(nested, dict) else None


#: ``{{input}}`` / ``{{input.a.b}}`` reference inside a nested base_parameter.
_INPUT_REF_RE = re.compile(r"\{\{\s*input((?:\.\w+)*)\s*\}\}")
#: inputs = ``{{tasks.<task>.values.<name>}}`` — the producing task's value.
_TASK_VALUE_REF_RE = re.compile(
    r"^\{\{\s*tasks\.(\w+)\.values\.(\w+)\s*\}\}$"
)
#: Any other ``{{...}}`` templating inside for-each parameters is unmodeled.
_OTHER_TEMPLATE_RE = re.compile(r"\{\{(?!\s*input[\s.}])")


def _parse_inputs(fe: Dict[str, Any]):
    """Classify ``for_each_task.inputs``.

    Returns ("value_ref", (producer_task, value_name)) for a task-value
    reference, ("literal", parsed_list) for a static JSON list, or
    ("unsupported", detail).
    """
    inputs = fe.get("inputs")
    if isinstance(inputs, list):
        return "literal", inputs
    if isinstance(inputs, str):
        m = _TASK_VALUE_REF_RE.match(inputs.strip())
        if m:
            return "value_ref", (m.group(1), m.group(2))
        try:
            parsed = json.loads(inputs)
        except ValueError:
            parsed = None
        if isinstance(parsed, list):
            return "literal", parsed
    return "unsupported", repr(inputs)





def _param_expression(template: Any) -> str:
    """Compile one nested base_parameter template into a Python expression
    over the loop element ``_el`` — DBX's ``{{input}}`` / ``{{input.field}}``
    substitution, reproduced faithfully (including literal text around refs).

    Raises ValueError for templating the driver does not model (anything that
    is not an ``input`` reference) — the conversion then refuses loudly.
    """
    if not isinstance(template, str):
        return repr(template)
    if _OTHER_TEMPLATE_RE.search(template):
        # No template text in the message — literals can embed values
        # (credential-class defense in depth, review 2026-10-14).
        raise ValueError("parameter template uses non-input templating")

    def ref_expr(match: "re.Match") -> str:
        path = match.group(1)
        if not path:
            return "_str(_el)"
        keys = tuple(path.lstrip(".").split("."))
        return f"_str(_lookup(_el, {keys!r}))"

    full = _INPUT_REF_RE.fullmatch(template)
    if full:
        return ref_expr(full)
    if not _INPUT_REF_RE.search(template):
        return repr(template)
    # Mixed literal + reference(s): plain string concatenation — repr() makes
    # every literal segment (whitespace, newlines, quotes, braces) safe, and
    # surrounding literals are preserved exactly as DBX substitution would.
    parts, last = [], 0
    for m in _INPUT_REF_RE.finditer(template):
        if template[last:m.start()]:
            parts.append(repr(template[last:m.start()]))
        parts.append(ref_expr(m))
        last = m.end()
    if template[last:]:
        parts.append(repr(template[last:]))
    return "(" + " + ".join(parts) + ")"


def foreach_unsupported_reason(dbx_task: Dict[str, Any]) -> Optional[str]:
    """Reason string when a ``for_each_task`` cannot be bridged, else None.

    A convertible for-each (notebook nested task, modeled inputs, modeled
    parameter templating) returns None and is handled by
    ``convert_for_each_task``. Everything else is refused loudly with a
    diagnosis the operator can act on.
    """
    fe = dbx_task.get("for_each_task")
    if not isinstance(fe, dict):
        return None
    task_key = dbx_task.get("task_key", "<?>")
    nested = nested_notebook_task(dbx_task)
    if nested is None or not (nested.get("notebook_task") or {}).get("notebook_path"):
        return (
            f"task '{task_key}': for_each_task nests a non-notebook task — "
            "unsupported on AIDP and outside the for-each driver bridge; "
            "manual conversion required"
        )
    kind, detail = _parse_inputs(fe)
    if kind == "unsupported":
        _inputs = fe.get("inputs")
        _shape = (f"{type(_inputs).__name__} of {len(str(_inputs))} chars"
                  if _inputs is not None else "None")
        # Shape only — raw inputs can carry literal values (review 2026-10-14).
        return (
            f"task '{task_key}': for_each_task inputs shape is not supported "
            f"by the driver bridge: {_shape} (supported: a static JSON list, "
            "or a single {{tasks.<task>.values.<name>}} reference)"
        )
    # Cluster provisioning enumerates only top-level task fields: a nested
    # existing_cluster_id / new_cluster never enters the registry — the driver
    # would silently run on a fallback cluster. Refuse those (fail-closed).
    # job_cluster_key is supported (job-level declaration, provisioned).
    unprovisioned = sorted(
        k for k in ("existing_cluster_id", "new_cluster") if nested.get(k)
    )
    if unprovisioned:
        return (
            f"task '{task_key}': for_each_task nested task declares "
            f"{unprovisioned} — the bridge does not provision nested compute; "
            "use a job-level job_cluster_key or convert manually"
        )
    # Nested libraries pass through (owner decision 2026-10-13: no
    # bridge-specific library handling). Remote coordinates (maven/pypi) get
    # the same record-only treatment as on top-level tasks, and hudi is
    # additionally covered by the tool baseline on every tool-touched cluster
    # [live evidence: EXBRIDGE's nested task declares the hudi maven coordinate].
    # KNOWN RESIDUAL: a file-backed library (jar/whl path) declared ONLY on
    # the nested task is not traversed by _enqueue_job_artifacts (top-level
    # tasks only) — unobserved input; if a job ever ships one, it fails
    # loudly at run time with a missing-library error and becomes the
    # evidenced fix.
    params = (nested.get("notebook_task") or {}).get("base_parameters") or {}
    for name, template in params.items():
        try:
            _param_expression(template)
        except ValueError:
            # Name only — the template text itself can embed literal values
            # (review 2026-10-14).
            return (
                f"task '{task_key}': for_each_task parameter '{name}' cannot "
                "be bridged — it uses templating other than {{input...}}"
            )
    return None


# ── TEMPORARY: driver-notebook generation (delete when AIDP ships native
#    for-each; the conversion call site in job_converter goes with it) ──────

#: Sentinel aligned with job_migrate's _TASKVALUE_MISSING_DEFAULT: obviously
#: not real data, so a missing producer value fails loudly instead of
#: producing garbage silently.
_PARAM_NOT_SET = "__PARAM_NOT_SET__"

_DRIVER_HEADER = """\
# ============================================================
# AIDP TOOL GENERATED — FOR-EACH DRIVER (FOREACH_BRIDGE)
# Replaces Databricks for_each_task {task_key!r} of job {job_id}.
# TEMPORARY until AIDP ships a native for-each task: re-export with
# native support replaces this task; then delete this notebook.
# Generated by dbx_export — do not edit by hand.
# ============================================================
"""


def driver_notebook_name(task_key: str, dbx_job_id: str = "") -> str:
    """Deterministic per (job, task): re-exporting the SAME job overwrites its
    own driver (idempotent), while two jobs sharing a task key and inner
    directory can never clobber each other's driver."""
    job_segment = f"{dbx_job_id}__" if dbx_job_id and dbx_job_id != "<?>" else ""
    return f"aidp_foreach_driver__{job_segment}{task_key}.ipynb"


def build_driver_notebook(
    dbx_task: Dict[str, Any],
    inner_aidp_path: str,
    job_id: str,
    inner_timeout_seconds: int,
    workspace_resource_path_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Generate the AIDP-native driver notebook for one for_each_task.

    The driver re-implements ONLY the for-each semantics: resolve ``inputs``
    (task-value reference via oidlUtils.parameters.getParameter — the
    AIDP-native cross-task channel — or an embedded static list), substitute
    ``{{input...}}`` into the nested task's parameters per element, and run
    the inner notebook SEQUENTIALLY via aidputils.notebook.run. DBX's
    ``concurrency`` is approximated sequentially (surfaced in output).
    Iterations are independent (as on Databricks): every element runs, all
    failures are collected, and the task fails at the end if any failed —
    never a silent skip.

    The caller must have validated the shape first (foreach_unsupported_reason
    is None); unsupported shapes raise ValueError here as a backstop.
    """
    reason = foreach_unsupported_reason(dbx_task)
    if reason:
        raise ValueError(reason)
    fe = dbx_task["for_each_task"]
    task_key = dbx_task.get("task_key", "<?>")
    nested_nb = nested_notebook_task(dbx_task)["notebook_task"]
    kind, detail = _parse_inputs(fe)
    if kind == "value_ref":
        producer, value_name = detail
        inputs_code = (
            f"_raw = oidlUtils.parameters.getParameter({value_name!r}, {_PARAM_NOT_SET!r})\n"
            f"if _raw == {_PARAM_NOT_SET!r}:\n"
            f"    raise RuntimeError(\n"
            f"        \"[foreach-driver] inputs task value {value_name!r} was never \"\n"
            f"        \"set by task {producer!r} — cannot run the loop\")\n"
            "_elements = json.loads(_raw) if isinstance(_raw, str) else _raw\n"
        )
    else:
        inputs_code = f"_elements = json.loads({json.dumps(json.dumps(detail))})\n"
    def _mapped(template: Any) -> Any:
        # Parity with the ordinary notebook branch (_rewrite_parameter_values):
        # a pure-literal value that names a translated workspace resource is
        # rewritten to its exported location. {{input...}} references and
        # mixed templates pass through untouched (review finding 2026-10-14).
        if (workspace_resource_path_map and isinstance(template, str)
                and not _INPUT_REF_RE.search(template)):
            from .job_converter import _rewrite_workspace_resource_value
            return _rewrite_workspace_resource_value(
                template, workspace_resource_path_map)
        return template

    param_items = ",\n        ".join(
        f"{name!r}: {_param_expression(_mapped(template))}"
        for name, template in (nested_nb.get("base_parameters") or {}).items()
    )
    concurrency = fe.get("concurrency") or 1
    body = (
        "import json\n"
        "\n"
        "def _str(v):\n"
        "    return v if isinstance(v, str) else json.dumps(v)\n"
        "\n"
        "def _lookup(el, keys):\n"
        "    cur = el\n"
        "    for k in keys:\n"
        "        if not isinstance(cur, dict) or k not in cur:\n"
        "            raise KeyError(f\"for-each element missing key {'.'.join(keys)!r}\")\n"
        "        cur = cur[k]\n"
        "    return cur\n"
        "\n"
        + inputs_code +
        "if not isinstance(_elements, list):\n"
        "    raise RuntimeError(f\"[foreach-driver] inputs did not resolve to a list: \"\n"
        "                       f\"{type(_elements).__name__}\")\n"
        f"print(f\"[foreach-driver] task {task_key}: {{len(_elements)}} element(s); \"\n"
        f"      f\"DBX concurrency={concurrency} approximated sequentially\")\n"
        "_failures = []\n"
        "for _idx, _el in enumerate(_elements, 1):\n"
        "    try:\n"
        "        _args = {\n"
        f"            {param_items}\n"
        "        }\n"
        "        # Keys only — task values may carry credentials; never print\n"
        "        # parameter VALUES or raw elements (review finding 2026-10-14).\n"
        "        print(f\"[foreach-driver] {_idx}/{len(_elements)}: args keys={sorted(_args)}\")\n"
        f"        _result = aidputils.notebook.run({inner_aidp_path!r}, {inner_timeout_seconds}, _args)\n"
        "        # Return values and exception messages may carry credentials\n"
        "        # (child code echoes its inputs); log type/length only\n"
        "        # (review finding 2026-10-14).\n"
        "        print(f\"[foreach-driver] {_idx}/{len(_elements)} OK: returned \"\n"
        "              f\"{type(_result).__name__} len={len(str(_result))}\")\n"
        "    except Exception as _exc:\n"
        "        _failures.append(f\"element {_idx}: {type(_exc).__name__}\")\n"
        "        print(f\"[foreach-driver] {_idx}/{len(_elements)} FAILED: \"\n"
        "              f\"{type(_exc).__name__}\")\n"
        "if _failures:\n"
        "    raise RuntimeError(\n"
        "        f\"[foreach-driver] {len(_failures)}/{len(_elements)} iteration(s) failed:\\n\"\n"
        "        + \"\\n\".join(_failures))\n"
        "print(f\"[foreach-driver] all {len(_elements)} iteration(s) succeeded\")\n"
    )
    header = _DRIVER_HEADER.format(task_key=task_key, job_id=job_id)
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "python"}},
        "cells": [{
            "cell_type": "code",
            "metadata": {},
            "outputs": [],
            "execution_count": None,
            "source": (header + body).splitlines(keepends=True),
        }],
    }
