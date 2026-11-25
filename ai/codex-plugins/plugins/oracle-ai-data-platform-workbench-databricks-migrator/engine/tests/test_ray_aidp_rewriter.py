from __future__ import annotations

import copy
from pathlib import Path

import pytest

from scripts.ray_aidp_rewriter import (
    MARKER,
    UnsupportedRayPattern,
    notebook_has_plain_ray_init,
    rewrite_ray_on_databricks_notebook,
    validate_ray_conversion,
)


def _cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "metadata": {},
        "source": source.splitlines(keepends=True),
        "outputs": [],
        "execution_count": None,
    }


def _source(cell: dict) -> str:
    return "".join(cell["source"])


def _canonical_cells():
    return [
        _cell(
            "import ray\n"
            "ray.init(ignore_reinit_error=True, address='auto', "
            "_redis_password='d4t4bricks')\n"
        ),
        _cell(
            "def execute_func(input_vars_path, func_name, num_gpus=0.2, "
            "num_cpus=1, store_model_path=None, param_list=None):\n"
            "    return ray.get([])\n"
        ),
        _cell(
            "@ray.remote\n"
            "def CustomGridSearch_MS(input_variables_dict, common_object_id_dict, "
            "params, store_model_path=None):\n"
            "    return params\n"
        ),
        _cell(
            "if param_list:\n"
            "    result = execute_func(\n"
            "        input_vars_path, func_name, num_gpus=num_gpus,\n"
            "        num_cpus=num_cpus, store_model_path=store_model_path,\n"
            "        param_list=final_param_list,\n"
            "    )\n"
            "else:\n"
            "    result = execute_func(\n"
            "        input_vars_path, func_name, num_gpus=num_gpus,\n"
            "        num_cpus=num_cpus, store_model_path=store_model_path,\n"
            "    )\n"
        ),
    ]


@pytest.mark.parametrize("source", [
    "# ray.init()\n", "print('ray.init()')\n",
    "ray.init(address='auto')\n", "ray.init(num_cpus=2)\n",
    "other.init()\n",
])
def test_plain_ray_detection_ignores_other_shapes(source):
    cells = [_cell(source)]
    before = copy.deepcopy(cells)
    assert not notebook_has_plain_ray_init(cells)
    assert cells == before


@pytest.mark.parametrize("source", [
    "ray.init()\n",
    "prepare_data(); ray.init()\n",
    "if enabled: ray.init()\n",
])
def test_plain_ray_detection_never_rewrites_customer_source(source):
    cells = [_cell(source)]
    before = copy.deepcopy(cells)
    assert notebook_has_plain_ray_init(cells)
    assert cells == before


def test_converts_cross_cell_ray_pattern_without_truncating_search():
    cells = _canonical_cells()
    report = rewrite_ray_on_databricks_notebook(cells)

    assert report.detected is True
    assert report.converted is True
    assert report.init_calls_rewritten == 1
    assert report.execute_calls_rewritten == 2
    assert report.changed_cells == [0, 3]

    combined = "\n".join(_source(cell) for cell in cells)
    assert "# ray.init(ignore_reinit_error=True, address='auto'," in combined
    assert "_redis_password='d4t4bricks')" in combined
    assert "Original Databricks Ray bootstrap" in combined
    assert "Original Databricks execute_func call" in combined
    assert "AIDP replacement: full-search Spark-GPU runner" in combined
    assert combined.count(MARKER) == 1
    assert combined.count("_aidp_execute_func_spark_gpu(") == 3  # def + two calls
    assert "_value.get(\"path\")" in combined
    assert "with open(_value[\"path\"], \"rb\")" in combined
    assert "_resolved_common_objects[_key] = {\"value\": _common_value}" in combined
    assert "list(enumerate(_all_params))" in combined
    assert "_indexed_results.sort(key=lambda _item: _item[0])" in combined
    assert "len(_result) != _input_count" in combined
    assert "all_params[:max_combos]" not in combined
    assert "final_param_list[:max_combos]" not in combined
    assert "range(0, _input_count, _progress_batch_size)" in combined

    for cell in cells:
        compile(_source(cell), "<cell>", "exec")


def test_audit_comments_preserve_original_statements_but_disable_them():
    cells = _canonical_cells()
    rewrite_ray_on_databricks_notebook(cells)

    init_source = _source(cells[0])
    call_source = _source(cells[3])
    init_tree = compile(init_source, "<init-cell>", "exec")
    call_tree = compile(call_source, "<call-cell>", "exec")

    assert init_tree is not None and call_tree is not None
    assert "# ray.init(ignore_reinit_error=True, address='auto'," in init_source
    assert "ray.init(ignore_reinit_error=True)" in init_source
    assert "# result = execute_func(" in call_source
    assert "result = _aidp_execute_func_spark_gpu(" in call_source


def test_rewrite_is_idempotent():
    cells = _canonical_cells()
    first = rewrite_ray_on_databricks_notebook(cells)
    snapshot = copy.deepcopy(cells)
    second = rewrite_ray_on_databricks_notebook(cells)

    assert first.converted is True
    assert second.converted is True
    assert second.changed_cells == []
    assert second.reason == "already converted"
    assert cells == snapshot


def test_final_conversion_contract_accepts_complete_distributed_rewrite():
    cells = _canonical_cells()
    rewrite_ray_on_databricks_notebook(cells)

    validate_ray_conversion(cells)


def test_final_conversion_contract_rejects_model_restored_legacy_call():
    cells = _canonical_cells()
    report = rewrite_ray_on_databricks_notebook(cells)
    call_index = report.changed_cells[-1]
    cells[call_index]["source"] = [
        "result = execute_func(path, name, param_list=final_param_list)\n"
    ]

    with pytest.raises(AssertionError, match="Invalid/lost Ray conversion"):
        validate_ray_conversion(cells)


def test_unrelated_execute_func_not_rewritten_without_auto_ray_cluster():
    cells = [_cell("result = execute_func(path, name, param_list=params)\n")]
    before = copy.deepcopy(cells)
    report = rewrite_ray_on_databricks_notebook(cells)

    assert report.detected is False
    assert report.converted is False
    assert cells == before


def test_fix_prompt_preserves_working_bare_ray_init_on_provisioned_head():
    source = (Path(__file__).parents[1] / "scripts" / "agent_migrate.py").read_text(
        encoding="utf-8"
    )

    assert "Ray 2.55 honors the provisioned RAY_ADDRESS" in source
    assert "Preserve a working bare call" in source
    assert "do not start a second local Ray head" in source


def test_stateful_ray_api_is_rejected_instead_of_semantically_changed():
    cells = _canonical_cells()
    cells.insert(2, _cell("ray.data.read_parquet(path)\n"))

    with pytest.raises(UnsupportedRayPattern, match="ray.data"):
        rewrite_ray_on_databricks_notebook(cells)


@pytest.mark.parametrize(
    "cap",
    [
        "all_params = all_params[:max_combos]\n",
        "final_param_list = final_param_list[:5]\n",
    ],
)
def test_first_n_search_cap_is_rejected(cap):
    cells = _canonical_cells()
    cells.append(_cell(cap))

    with pytest.raises(UnsupportedRayPattern, match="truncation"):
        rewrite_ray_on_databricks_notebook(cells)
