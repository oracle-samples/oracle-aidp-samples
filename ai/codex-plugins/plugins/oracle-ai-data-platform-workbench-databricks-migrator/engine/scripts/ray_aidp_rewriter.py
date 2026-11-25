"""Deterministic Ray-on-Databricks to AIDP Spark-GPU conversion.

The supported shape is deliberately narrow: a notebook starts Ray with
``address="auto"`` and later calls ``execute_func(..., param_list=...)`` where
``func_name`` resolves to an independent Ray remote function.  AIDP does not
provide the Databricks-managed Ray cluster addressed by ``auto``.  For that
shape, distribute the unchanged remote function across Spark GPU executors and
run a local Ray runtime in each executor process.

This transformer never caps or samples the parameter list.  Progress batching
is only an execution boundary; result cardinality and input order are enforced.
"""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass, field
from typing import Iterable, List, Sequence, Tuple


MARKER = "# AIDP deterministic Ray map-style runner (full search; no truncation)"


class UnsupportedRayPattern(ValueError):
    """Raised only when a detected Ray-on-Databricks notebook is unsafe to rewrite."""


@dataclass
class RayRewriteReport:
    detected: bool = False
    converted: bool = False
    changed_cells: List[int] = field(default_factory=list)
    execute_calls_rewritten: int = 0
    init_calls_rewritten: int = 0
    reason: str = ""


_UNSUPPORTED_RAY_NAMESPACES = {"actor", "data", "tune", "train"}


_RUNNER = r'''# AIDP deterministic Ray map-style runner (full search; no truncation)
def _aidp_execute_func_spark_gpu(
    input_vars_path,
    func_name,
    num_gpus=0.2,
    num_cpus=1,
    store_model_path=None,
    param_list=None,
):
    import cloudpickle as _cloudpickle
    import math as _math
    import os as _os
    import pickle as _pickle
    import time as _time

    with open(input_vars_path, "rb") as _handle:
        _driver_input = _pickle.load(_handle)

    # Preserve execute_func's original common-object contract.  Databricks
    # notebooks can supply either {"value": object} or {"path": pickle_path}.
    # Resolve path-backed objects on the driver before broadcasting because
    # the driver's /Volumes path may not be mounted in Spark executor pods.
    _driver_input = dict(_driver_input)
    _resolved_common_objects = {}
    for _key, _value in (_driver_input.get("common_objects") or {}).items():
        if isinstance(_value, dict) and _value.get("path"):
            with open(_value["path"], "rb") as _common_handle:
                _common_value = _pickle.load(_common_handle)
        elif isinstance(_value, dict) and "value" in _value:
            _common_value = _value["value"]
        else:
            _common_value = _value
        _resolved_common_objects[_key] = {"value": _common_value}
    _driver_input["common_objects"] = _resolved_common_objects

    _all_params = (
        list(param_list)
        if param_list is not None
        else list(_driver_input.get("param_list") or [])
    )
    _input_count = len(_all_params)
    if not _input_count:
        return []

    _func_obj = globals().get(func_name)
    _func_impl = getattr(_func_obj, "_function", None)
    if _func_impl is None:
        raise TypeError(
            f"Unsupported Ray conversion: {func_name!r} is not a Ray remote function"
        )

    _worker_num_cpus = max(1, int(num_cpus))
    _worker_num_gpus = float(num_gpus)
    if not 0 < _worker_num_gpus <= 1:
        raise ValueError(
            "AIDP Spark-GPU runner requires num_gpus in (0, 1] per model; "
            f"got {_worker_num_gpus}"
        )

    _conf = spark.sparkContext.getConf()
    _worker_setting = _os.environ.get("AIDP_RAY_GPU_WORKERS", "")
    if _worker_setting:
        _gpu_workers = int(_worker_setting)
    else:
        _gpu_workers = 1
        for _candidate in (
            _conf.get("spark.executor.instances", "0"),
            _conf.get("spark.dynamicAllocation.maxExecutors", "0"),
        ):
            try:
                _candidate_count = int(_candidate)
            except (TypeError, ValueError):
                continue
            if _candidate_count > 0:
                _gpu_workers = _candidate_count
                break
    _gpu_workers = max(1, _gpu_workers)
    _partitions_per_worker = max(
        1, int(_os.environ.get("AIDP_RAY_PARTITIONS_PER_WORKER", "2"))
    )
    _progress_batch_size = max(
        1, int(_os.environ.get("AIDP_RAY_PROGRESS_BATCH_SIZE", "250"))
    )

    _func_payload_bc = spark.sparkContext.broadcast(
        _cloudpickle.dumps(_func_impl)
    )
    _driver_input_bc = spark.sparkContext.broadcast(_driver_input)

    def _run_partition(_indexed_params):
        import cloudpickle as _worker_cloudpickle
        import os as _worker_os
        import ray as _worker_ray
        from pyspark import TaskContext as _TaskContext

        _items = list(_indexed_params)
        if not _items:
            return iter(())

        _resources = _TaskContext.get().resources()
        _gpu = _resources.get("gpu")
        if _gpu is None or not getattr(_gpu, "addresses", None):
            raise RuntimeError(
                "Spark partition has no GPU assignment. Configure a GPU AIDP "
                "cluster and spark.task.resource.gpu.amount before migration."
            )

        _safe_cwd = "/tmp"
        _worker_os.makedirs(_safe_cwd, exist_ok=True)
        _worker_os.chdir(_safe_cwd)
        _worker_ray.shutdown()
        _worker_ray.init(ignore_reinit_error=True, include_dashboard=False)
        try:
            _payload = _driver_input_bc.value
            _common_refs = {}
            for _key, _value in (_payload.get("common_objects") or {}).items():
                _raw = _value.get("value") if isinstance(_value, dict) else _value
                _common_refs[_key] = _worker_ray.put(_raw)

            _input_variables = _payload.get("input_variables")
            _implementation = _worker_cloudpickle.loads(_func_payload_bc.value)
            _remote = _worker_ray.remote(
                num_gpus=_worker_num_gpus,
                max_calls=1,
            )(_implementation)

            _pending = []
            for _original_index, _params in _items:
                _ref = _remote.options(
                    num_cpus=_worker_num_cpus,
                    num_gpus=_worker_num_gpus,
                ).remote(
                    input_variables_dict=_input_variables,
                    common_object_id_dict=_common_refs,
                    params=_params,
                    store_model_path=store_model_path,
                )
                _pending.append((_original_index, _ref))

            _values = _worker_ray.get([_ref for _, _ref in _pending])
            return iter(
                (_index, _value)
                for (_index, _), _value in zip(_pending, _values)
            )
        finally:
            _worker_os.chdir(_safe_cwd)
            _worker_ray.shutdown()
            _worker_os.chdir(_safe_cwd)

    _indexed_params = list(enumerate(_all_params))
    _batch_count = _math.ceil(_input_count / _progress_batch_size)
    _started = _time.monotonic()
    _indexed_results = []

    for _batch_number, _batch_start in enumerate(
        range(0, _input_count, _progress_batch_size), start=1
    ):
        _batch = _indexed_params[
            _batch_start:_batch_start + _progress_batch_size
        ]
        _partition_count = min(
            len(_batch), _gpu_workers * _partitions_per_worker
        )
        print(
            f"[grid] BATCH {_batch_number}/{_batch_count} START "
            f"params={_batch_start + 1}-{_batch_start + len(_batch)} "
            f"count={len(_batch)} partitions={_partition_count}",
            flush=True,
        )
        _batch_results = (
            spark.sparkContext
            .parallelize(_batch, _partition_count)
            .mapPartitions(_run_partition)
            .collect()
        )
        _indexed_results.extend(_batch_results)
        print(
            f"[grid] BATCH {_batch_number}/{_batch_count} COMPLETE "
            f"cumulative={len(_indexed_results)}/{_input_count} "
            f"elapsed_minutes={(_time.monotonic() - _started) / 60:.1f}",
            flush=True,
        )

    _indexed_results.sort(key=lambda _item: _item[0])
    _result = [_item[1] for _item in _indexed_results]
    if len(_result) != _input_count:
        raise RuntimeError(
            "AIDP Spark-GPU grid search cardinality mismatch: "
            f"input={_input_count}, output={len(_result)}"
        )
    if [item[0] for item in _indexed_results] != list(range(_input_count)):
        raise RuntimeError("AIDP Spark-GPU grid search lost or duplicated parameters")
    print(f"All {_input_count} combinations complete", flush=True)
    return _result
'''


def _source(cell: dict) -> str:
    value = cell.get("source", [])
    return "".join(value) if isinstance(value, list) else str(value or "")


def _set_source(cell: dict, value: str) -> None:
    cell["source"] = value.splitlines(keepends=True)


def _python_body(source: str) -> Tuple[str, str]:
    """Return (prefix, parseable body), preserving a leading %python magic."""
    lines = source.splitlines(keepends=True)
    if lines and lines[0].lstrip().startswith("%python"):
        return lines[0], "".join(lines[1:])
    return "", source


def _offsets(source: str) -> List[int]:
    starts = [0]
    for index, char in enumerate(source):
        if char == "\n":
            starts.append(index + 1)
    return starts


def _span(starts: Sequence[int], node: ast.AST) -> Tuple[int, int]:
    return (
        starts[node.lineno - 1] + node.col_offset,
        starts[node.end_lineno - 1] + node.end_col_offset,
    )


def _is_auto_ray_init(node: ast.Call) -> bool:
    func = node.func
    if not (
        isinstance(func, ast.Attribute)
        and func.attr == "init"
        and isinstance(func.value, ast.Name)
        and func.value.id in {"ray", "_ray"}
    ):
        return False
    for keyword in node.keywords:
        if (
            keyword.arg == "address"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == "auto"
        ):
            return True
    return False


def _is_plain_ray_init(node: ast.Call) -> bool:
    """Only the no-argument source call opts into an existing Ray head."""
    return (
        isinstance(node.func, ast.Attribute)
        and node.func.attr == "init"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "ray"
        and not node.args
        and not node.keywords
    )


def notebook_has_plain_ray_init(cells: Sequence[dict]) -> bool:
    """Ignore comments/strings and non-Python cells when planning the cluster."""
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
        _, body = _python_body(_source(cell))
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        if any(
            isinstance(node, ast.Call) and _is_plain_ray_init(node)
            for node in ast.walk(tree)
        ):
            return True
    return False


def notebook_requires_ray_head_script(cells: Sequence[dict]) -> bool:
    """A bare or unconverted ``address='auto'`` init needs a cluster Ray head.

    The supported map-style Databricks pattern is converted to Spark-GPU
    execution instead, so attaching a head script to it would be unnecessary.
    """
    if notebook_has_plain_ray_init(cells):
        return True
    has_auto = False
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
        _, body = _python_body(_source(cell))
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        if any(isinstance(node, ast.Call) and _is_auto_ray_init(node)
               for node in ast.walk(tree)):
            has_auto = True
            break
    if not has_auto:
        return False
    try:
        return not rewrite_ray_on_databricks_notebook(copy.deepcopy(cells)).detected
    except UnsupportedRayPattern:
        # The migrator will fail loudly for this unsupported conversion;
        # an init script cannot make that semantic rewrite safe.
        return False


def _attribute_path(node: ast.AST) -> Tuple[str, ...]:
    parts: List[str] = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return tuple(reversed(parts))


def _is_ray_remote(node: ast.AST) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    return _attribute_path(target) in {("ray", "remote"), ("_ray", "remote")}


def _unsupported_ray_patterns(cells: Sequence[dict]) -> List[str]:
    """Return semantic Ray patterns that cannot preserve actor semantics.

    Only Python code cells are parsed, so markdown, comments and string literals
    mentioning ``ray.tune`` or similar APIs cannot reject a notebook.
    """
    trees: List[ast.AST] = []
    class_names = set()
    for cell in cells:
        if cell.get("cell_type") != "code":
            continue
        _, body = _python_body(_source(cell))
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        trees.append(tree)
        class_names.update(
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        )

    found = set()
    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and any(
                _is_ray_remote(decorator) for decorator in node.decorator_list
            ):
                found.add(f"Ray remote actor class {node.name}")
            if isinstance(node, ast.Call):
                path = _attribute_path(node.func)
                if (
                    len(path) >= 2
                    and path[0] in {"ray", "_ray"}
                    and path[1] in _UNSUPPORTED_RAY_NAMESPACES
                ):
                    found.add(".".join(path[:2]))
                if path in {("ray", "wait"), ("_ray", "wait")}:
                    found.add(".".join(path))
                if (
                    _is_ray_remote(node)
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id in class_names
                ):
                    found.add(f"ray.remote({node.args[0].id}) actor wrapper")
    return sorted(found)


def _statement_calls(statement: ast.stmt, predicate) -> List[ast.Call]:
    """Return calls owned by a statement, excluding calls in nested statements."""
    calls: List[ast.Call] = []
    for child in ast.iter_child_nodes(statement):
        if isinstance(child, ast.stmt):
            continue
        calls.extend(
            node
            for node in ast.walk(child)
            if isinstance(node, ast.Call) and predicate(node)
        )
    return calls


def _rewrite_init_calls(source: str) -> Tuple[str, int]:
    prefix, body = _python_body(source)
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return source, 0
    starts = _offsets(body)
    replacements = []
    for statement in ast.walk(tree):
        if not isinstance(statement, ast.stmt):
            continue
        calls = _statement_calls(statement, _is_auto_ray_init)
        if not calls:
            continue
        line_start = starts[statement.lineno - 1]
        statement_start, statement_end = _span(starts, statement)
        indent = body[line_start:statement_start]
        original = body[statement_start:statement_end]
        converted = original
        for call in sorted(calls, key=lambda item: _span(starts, item)[0], reverse=True):
            call_start, call_end = _span(starts, call)
            name = ast.get_source_segment(body, call.func) or "ray.init"
            relative_start = call_start - statement_start
            relative_end = call_end - statement_start
            converted = (
                converted[:relative_start]
                + f"{name}(ignore_reinit_error=True)"
                + converted[relative_end:]
            )
        audit = _audit_replacement(
            indent,
            original,
            "Original Databricks Ray bootstrap (disabled on AIDP; retained for audit)",
            "AIDP replacement: start a local Ray runtime in the current process",
        )
        replacements.append((line_start, statement_end, audit + indent + converted))
    for start, end, replacement in sorted(replacements, reverse=True):
        body = body[:start] + replacement + body[end:]
    return prefix + body, sum(
        1
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _is_auto_ray_init(node)
    )


def _rewrite_execute_calls(source: str) -> Tuple[str, int]:
    prefix, body = _python_body(source)
    try:
        tree = ast.parse(body)
    except SyntaxError:
        return source, 0
    starts = _offsets(body)
    call_count = sum(
        1
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "execute_func"
        )
    )
    if not call_count:
        return source, 0
    replacements = []
    for statement in ast.walk(tree):
        if not isinstance(statement, ast.stmt):
            continue
        calls = _statement_calls(
            statement,
            lambda node: (
                isinstance(node.func, ast.Name)
                and node.func.id == "execute_func"
            ),
        )
        if not calls:
            continue
        line_start = starts[statement.lineno - 1]
        statement_start, statement_end = _span(starts, statement)
        indent = body[line_start:statement_start]
        original = body[statement_start:statement_end]
        converted = original
        for call in sorted(calls, key=lambda item: _span(starts, item.func)[0], reverse=True):
            func_start, func_end = _span(starts, call.func)
            relative_start = func_start - statement_start
            relative_end = func_end - statement_start
            converted = (
                converted[:relative_start]
                + "_aidp_execute_func_spark_gpu"
                + converted[relative_end:]
            )
        audit = _audit_replacement(
            indent,
            original,
            "Original Databricks execute_func call (disabled on AIDP; retained for audit)",
            "AIDP replacement: full-search Spark-GPU runner; no parameter truncation",
        )
        replacements.append((line_start, statement_end, audit + indent + converted))
    for start, end, replacement in sorted(replacements, reverse=True):
        body = body[:start] + replacement + body[end:]
    return prefix + body, call_count


def _prepend_runner(source: str) -> str:
    prefix, body = _python_body(source)
    if MARKER in body:
        return source
    return prefix + _RUNNER + "\n\n" + body


def _audit_replacement(
    indent: str,
    original: str,
    original_label: str,
    replacement_label: str,
) -> str:
    """Render an executable-safe audit block immediately before a replacement."""
    lines = [f"{indent}# {original_label}:\n"]
    for line in original.splitlines():
        lines.append(f"{indent}# {line.lstrip()}\n")
    lines.append(f"{indent}# {replacement_label}:\n")
    return "".join(lines)


def _contains_migration_cap(source: str) -> bool:
    compact = "".join(source.split())
    return any(token in compact for token in (
        "all_params=all_params[:max_combos]",
        "final_param_list=final_param_list[:max_combos]",
        "all_params=all_params[:5]",
        "final_param_list=final_param_list[:5]",
    ))


def rewrite_ray_on_databricks_notebook(cells: Sequence[dict]) -> RayRewriteReport:
    """Rewrite a supported Ray map-style notebook in place.

    Detection is notebook-wide because ``ray.init``, ``execute_func`` and its
    invocation commonly live in different cells.  The rewrite is idempotent.
    """
    report = RayRewriteReport()
    sources = [_source(cell) for cell in cells if cell.get("cell_type") == "code"]
    combined = "\n".join(sources)
    already_converted = MARKER in combined
    has_auto = False
    for source in sources:
        prefix, body = _python_body(source)
        del prefix
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        if any(isinstance(node, ast.Call) and _is_auto_ray_init(node) for node in ast.walk(tree)):
            has_auto = True
            break

    has_execute_definition = False
    has_execute_call = False
    for source in sources:
        _, body = _python_body(source)
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        has_execute_definition |= any(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "execute_func"
            for node in ast.walk(tree)
        )
        has_execute_call |= any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "execute_func"
            for node in ast.walk(tree)
        )
    report.detected = already_converted or (has_auto and has_execute_definition and has_execute_call)
    if not report.detected:
        report.reason = "Ray-on-Databricks map-style pattern not detected"
        return report
    if already_converted:
        report.converted = True
        report.reason = "already converted"
        return report

    unsupported = _unsupported_ray_patterns(cells)
    if unsupported:
        raise UnsupportedRayPattern(
            "Ray notebook uses unsupported stateful/high-level API(s): "
            + ", ".join(unsupported)
        )
    if _contains_migration_cap(combined):
        raise UnsupportedRayPattern(
            "Ray search contains first-N/max_combos truncation; remove the "
            "migration-added cap before deterministic conversion"
        )

    runner_cell_index = None
    for index, cell in enumerate(cells):
        if cell.get("cell_type") != "code":
            continue
        source = _source(cell)
        rewritten, init_count = _rewrite_init_calls(source)
        rewritten, execute_count = _rewrite_execute_calls(rewritten)
        if execute_count and runner_cell_index is None:
            runner_cell_index = index
        if rewritten != source:
            _set_source(cell, rewritten)
            report.changed_cells.append(index)
        report.init_calls_rewritten += init_count
        report.execute_calls_rewritten += execute_count

    if runner_cell_index is not None:
        runner_cell = cells[runner_cell_index]
        runner_source = _source(runner_cell)
        with_runner = _prepend_runner(runner_source)
        if with_runner != runner_source:
            _set_source(runner_cell, with_runner)
        if runner_cell_index not in report.changed_cells:
            report.changed_cells.append(runner_cell_index)

    if report.execute_calls_rewritten == 0:
        raise UnsupportedRayPattern(
            "Detected ray.init(address='auto') but found no executable execute_func call"
        )
    result_source = "\n".join(_source(cell) for cell in cells)
    if _contains_migration_cap(result_source):
        raise AssertionError("Ray conversion introduced search-space truncation")
    if "_indexed_results.sort(key=lambda _item: _item[0])" not in result_source:
        raise AssertionError("Ray conversion is missing deterministic result ordering")
    if "len(_result) != _input_count" not in result_source:
        raise AssertionError("Ray conversion is missing result-cardinality validation")

    report.converted = True
    report.reason = "converted map-style Ray execution to full-search Spark-GPU runner"
    return report


def validate_ray_conversion(cells: Sequence[dict]) -> None:
    """Fail if a deterministic Ray rewrite was lost or weakened downstream."""
    sources = [_source(cell) for cell in cells if cell.get("cell_type") == "code"]
    combined = "\n".join(sources)
    required = {
        "runner marker": MARKER,
        "Spark partition execution": ".mapPartitions(",
        "deterministic result ordering": "_indexed_results.sort(key=lambda _item: _item[0])",
        "result-cardinality validation": "len(_result) != _input_count",
    }
    missing = [name for name, token in required.items() if token not in combined]

    aidp_calls = 0
    legacy_calls = 0
    for source in sources:
        _, body = _python_body(source)
        try:
            tree = ast.parse(body)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else ""
            if name == "_aidp_execute_func_spark_gpu":
                aidp_calls += 1
            elif name == "execute_func":
                legacy_calls += 1

    if aidp_calls == 0:
        missing.append("active AIDP distributed runner invocation")
    if legacy_calls:
        missing.append(f"no active legacy execute_func calls (found {legacy_calls})")
    if _contains_migration_cap(combined):
        missing.append("complete, untruncated parameter search")
    if missing:
        raise AssertionError("Invalid/lost Ray conversion: " + "; ".join(missing))


def rewritten_notebook_copy(notebook: dict) -> Tuple[dict, RayRewriteReport]:
    """Pure convenience wrapper used by tests and offline conversion tooling."""
    result = copy.deepcopy(notebook)
    report = rewrite_ray_on_databricks_notebook(result.get("cells") or [])
    return result, report
