"""Regression tests for notebook-wide FileStore location normalization."""

import asyncio
import contextlib
import io
import os
import sys
import json
import inspect
from pathlib import Path

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


@pytest.fixture(scope="module")
def jm():
    import job_migrate

    return job_migrate


def _cell(source):
    return {"cell_type": "code", "source": [source], "metadata": {}}


def _source(cells, index):
    return "".join(cells[index]["source"])


def _active_source(cells, index):
    lines = []
    for line in _source(cells, index).splitlines():
        if line.lstrip().startswith("#"):
            continue
        lines.append(line.split("  # Oracle tool modification:", 1)[0])
    return "\n".join(lines)


def _assert_audited_change(cells, index, original):
    source = _source(cells, index)
    assert original in source
    assert "# Oracle tool modification: original before" in source
    assert "# Oracle tool modification: normalized FileStore/DBFS path" in source


def _execute_path(cells):
    namespace = {
        "joblib": type("Joblib", (), {"load": staticmethod(lambda path: path)}),
    }
    for cell in cells:
        exec(_source([cell], 0), namespace)
    return namespace.get("value", namespace.get("model"))


def _finalize_after_initial_translation(jm, cells):
    """Retain the old literal/root pins at their pre-execution boundary.

    REF-158 removes only the second, post-validation literal sweep. Ordinary
    first-pass translation still runs before execution and its fix loop.
    """
    migrated = [dict(cell, source=[jm._apply_dbfs_translations(_source([cell], 0))])
                for cell in cells]
    return jm._finalize_dbfs_location_variables(migrated)


def test_model_inference_pair_normalizes_location_and_drops_duplicate_root(jm):
    cells = [
        _cell('location = "/FileStore/tables/user8"'),
        _cell(
            "model = joblib.load("
            "'/dbfs' + location + '/Model/model_meta_geo_comb_snc.joblib')"
        ),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _active_source(result, 0) == (
        'location = "/Volumes/default/default/dbfs/FileStore/tables/user8"'
    )
    assert _active_source(result, 1) == (
        "model = joblib.load("
        "location + '/Model/model_meta_geo_comb_snc.joblib')"
    )
    _assert_audited_change(result, 0, 'location = "/FileStore/tables/user8"')
    _assert_audited_change(result, 1, "model = joblib.load('/dbfs' + location")


def test_rule_engine_direct_location_use_remains_valid(jm):
    cells = [
        _cell('location = "/FileStore/tables/user8"'),
        _cell(
            "MasterData_Scored = spark.read.csv("
            "location + '/Files/input.csv', header=True)"
        ),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _active_source(result, 0) == (
        'location = "/Volumes/default/default/dbfs/FileStore/tables/user8"'
    )
    assert _source(result, 1) == cells[1]["source"][0]


@pytest.mark.parametrize(
    "input_path",
    [
        "dbfs:/FileStore/tables/user8",
        "/dbfs/FileStore/tables/user8",
    ],
)
def test_dbfs_assignments_are_normalized(input_path, jm):
    result = jm._finalize_dbfs_location_variables(
        [_cell(f'location = "{input_path}"')]
    )
    assert _active_source(result, 0) == (
        'location = "/Volumes/default/default/dbfs/FileStore/tables/user8"'
    )


def test_existing_volume_assignment_is_idempotent(jm):
    path = (
        "/Volumes/default/default/dbfs/FileStore/"
        "user9/alerting_configs/auto_ft_daily.pkl"
    )
    cells = [
        _cell(f'location = "{path}"'),
        _cell("loaded = open(location, 'rb')"),
    ]

    once = jm._finalize_dbfs_location_variables(cells)
    twice = jm._finalize_dbfs_location_variables(once)

    assert once == twice
    assert _source(once, 0) == f'location = "{path}"'


def test_nonstandard_filestore_scheme_is_untouched(jm):
    cells = [
        _cell('location = "filestore:/tables/user8"'),
        _cell("value = '/dbfs' + location"),
    ]

    result = _finalize_after_initial_translation(jm, cells)

    assert _source(result, 0) == cells[0]["source"][0]
    # Pre-execution literal translation still applies, but the prefix is retained
    # because the non-standard filestore: value was not marked as normalized.
    assert _active_source(result, 1) == (
        "value = '/Volumes/default/default/dbfs' + location"
    )


def test_unrelated_location_assignment_does_not_trigger_prefix_removal(jm):
    cells = [
        _cell('location = "/FileStore/tables/user8"'),
        _cell('location = "/ordinary/local/path"'),
        _cell("value = '/dbfs' + location"),
    ]

    result = _finalize_after_initial_translation(jm, cells)

    assert _active_source(result, 2) == (
        "value = '/Volumes/default/default/dbfs' + location"
    )


def test_python_magic_and_translate_path_assignment_are_supported(jm):
    cells = [
        _cell(
            "%python\n"
            "location = translate_path('/FileStore/tables/user8')"
        ),
        _cell("model = joblib.load('/dbfs/' + location + '/Model/x.pkl')"),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _source(result, 0).startswith("%python\n")
    assert "location = translate_path(" in _source(result, 0)
    assert _active_source(result, 1) == (
        "model = joblib.load(location + '/Model/x.pkl')"
    )


def test_filestore_assignment_preserves_direct_spark_reader_use(jm):
    cells = [
        _cell('location = "/FileStore/tables/user8"'),
        _cell('df = spark.read.csv(location + "/Files/model.csv", header=True)'),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _active_source(result, 0) == (
        'location = "/Volumes/default/default/dbfs/FileStore/tables/user8"'
    )
    assert _source(result, 1) == cells[1]["source"][0]


def test_full_rule_engine_expression_uses_normalized_assignment_once(jm):
    cells = [
        _cell('location = "/FileStore/tables/user8"'),
        _cell(
            "MasterData_Scored=spark.read.csv("
            "location+'/Files/Meta_GL_All_Scored_latest_t2f'+msd+'_'+"
            "str_current_date+'.csv', header=True).withColumn("
            '"Predict",col("Predict").cast(DoubleType()))'
        ),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _active_source(result, 0) == (
        'location = "/Volumes/default/default/dbfs/FileStore/tables/user8"'
    )
    assert _source(result, 1) == cells[1]["source"][0]


@pytest.mark.parametrize(
    "consumer",
    [
        'value = f"/dbfs/{location}"',
        'value = f"/dbfs{location}"',
        'value = "/dbfs%s" % location',
        'value = "/dbfs" + location.strip()',
        'value = "/dbfs" + str(location)',
    ],
)
def test_root_prepend_consumers_never_double_a_normalized_location(jm, consumer):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell(consumer),
    ]

    result = jm._finalize_dbfs_location_variables(cells)
    repeated = jm._finalize_dbfs_location_variables(result)

    assert _execute_path(result) == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )
    assert repeated == result


def test_percent_wrapper_is_rewritten_without_double_root(jm):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell('value = "/dbfs%s" % str(location)'),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _execute_path(result) == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


@pytest.mark.parametrize(
    "consumer",
    [
        'value = f"/dbfs/prefix/{location}"',
        'value = "/dbfs/prefix/%s" % location',
    ],
)
def test_non_bare_root_consumers_keep_the_assignment_for_literal_translation(
    jm, consumer
):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell(consumer),
    ]

    result = _finalize_after_initial_translation(jm, cells)

    assert _source(result, 0) == cells[0]["source"][0]
    assert _execute_path(result) == (
        "/Volumes/default/default/dbfs/prefix//FileStore/tables/x"
    )


@pytest.mark.parametrize(
    "prefix",
    [
        'ruta_año = "ok"; ',
        'marker = "a\u2028b"; ',
        'marker = "a\fb"; ',
        'marker = "a\u0085b"; ',
    ],
)
def test_ast_slicing_is_character_correct_and_saved_source_reparses(jm, prefix):
    cells = [
        _cell(prefix + 'location = "/FileStore/tables/x"'),
        _cell('value = "/dbfs" + location'),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    for cell in result:
        compile(_source([cell], 0), "<final-cell>", "exec")
    assert _execute_path(result) == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


def test_conditional_reassignment_prevents_global_location_normalization(jm):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell('if use_other:\n    location = "/ordinary/path"'),
        _cell('value = "/dbfs" + location'),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _source(result, 0) == cells[0]["source"][0]


@pytest.mark.parametrize(
    "shadowing_source",
    [
        'def load(location):\n    return "/dbfs" + location',
        'values = ["/dbfs" + location for location in paths]',
    ],
)
def test_shadowed_names_are_not_rewritten_as_the_global_location(jm, shadowing_source):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell(shadowing_source),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert _source(result, 0) == cells[0]["source"][0]
    assert "/dbfs" in _source(result, 1) or "/Volumes" in _source(result, 1)


def test_use_before_assignment_in_same_cell_is_not_rewritten(jm):
    cells = [
        _cell(
            'before = "/dbfs" + location\n'
            'location = "/FileStore/tables/x"'
        )
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    assert 'location = "/FileStore/tables/x"' in _source(result, 0)


@pytest.mark.parametrize(
    "consumer",
    [
        'value = f"/Volumes/default/default/dbfs{location}"',
        'value = "/Volumes/default/default/dbfs%s" % location',
        'value = "{}{}".format("/Volumes/default/default/dbfs", location)',
        'value = "".join(["/Volumes/default/default/dbfs", location])',
        'value = os.path.join("/Volumes/default/default/dbfs", location.lstrip("/"))',
        'value = Path("/Volumes/default/default/dbfs") / location.lstrip("/")',
    ],
)
def test_already_translated_consumers_do_not_double(jm, consumer):
    cells = [_cell('location = "/FileStore/tables/x"'), _cell(consumer)]
    result = jm._finalize_dbfs_location_variables(cells)
    namespace = {"os": os, "Path": Path}
    for cell in result:
        exec(_active_source([cell], 0), namespace)

    assert str(namespace["value"]).replace("\\", "/") == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


def test_parent_consumer_prevents_child_producer_normalization(jm):
    parent_consumers = jm._dbfs_root_consumer_names(
        ['value = "/Volumes/default/default/dbfs" + location']
    )
    child = jm._finalize_child_artifact_sources(
        ['location = "/FileStore/tables/x"'],
        external_unsafe_names=parent_consumers,
    )

    assert _active_source([_cell(child[0])], 0) == 'location = "/FileStore/tables/x"'


def test_child_consumer_prevents_parent_producer_normalization(jm, monkeypatch):
    parent_path = "/Workspace/parent.ipynb"
    child_path = "/Workspace/child.ipynb"
    parent_cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell("%run /Workspace/child.ipynb"),
    ]
    child_cells = [
        _cell('value = "/Volumes/default/default/dbfs" + location')
    ]
    monkeypatch.setitem(
        jm._notebook_content_cache,
        child_path,
        json.dumps({"cells": child_cells}),
    )
    scope = jm._resolve_symbol_scope(parent_path, parent_cells)
    result = jm._finalize_dbfs_location_variables(
        parent_cells,
        external_unsafe_names=scope["descendant_dbfs_root_consumer_names"],
    )

    assert _active_source(result, 0) == 'location = "/FileStore/tables/x"'


def test_code_only_child_defers_normalization_until_parent_scope_is_known(jm):
    child_cells = [_cell('location = "/FileStore/tables/x"')]
    code_only = jm._finalize_dbfs_location_variables(
        child_cells, normalize_assignments=False
    )
    assert _active_source(code_only, 0) == 'location = "/FileStore/tables/x"'

    parent_consumers = jm._dbfs_root_consumer_names(
        ['value = "/Volumes/default/default/dbfs" + location']
    )
    inlined = jm._finalize_child_artifact_sources(
        [_source(code_only, 0)], external_unsafe_names=parent_consumers
    )

    assert _active_source([_cell(inlined[0])], 0) == (
        'location = "/FileStore/tables/x"'
    )


def test_shared_child_stays_parent_neutral_without_known_parent_consumers(jm):
    child = jm._finalize_child_artifact_sources(
        ['location = "/FileStore/tables/x"'],
        normalize_assignments=False,
    )

    assert _active_source([_cell(child[0])], 0) == (
        'location = "/FileStore/tables/x"'
    )


def test_manifest_dependency_detection_is_order_independent(jm):
    original = jm._DEP_RESOLUTION
    try:
        jm.set_dep_resolution(
            {
                "/Workspace/parent.ipynb": [
                    "/Workspace/shared/child.ipynb"
                ]
            }
        )

        assert jm._is_manifest_dependency(
            "/Workspace/shared/child.ipynb"
        )
        assert jm._is_manifest_dependency("Workspace/shared/child")
        assert not jm._is_manifest_dependency("/Workspace/root.ipynb")
    finally:
        jm.set_dep_resolution(original)


def test_code_only_root_and_dependency_use_distinct_normalization_controls(jm):
    process_source = inspect.getsource(jm.process_notebook)
    dependency_source = inspect.getsource(jm.ensure_migrated)
    parameter = inspect.signature(jm.process_notebook).parameters[
        "defer_filestore_assignments"
    ]

    assert parameter.default is False
    assert "normalize_assignments=not defer_filestore_assignments" in process_source
    assert "defer_filestore_assignments=True" in dependency_source


@pytest.mark.parametrize(
    "setup, consumer",
    [
        ('DBFS_ROOT = "/dbfs"', "value = DBFS_ROOT + location"),
        (
            'DBFS_ROOT = os.environ.get("DBFS_ROOT", "/dbfs")',
            'value = f"{DBFS_ROOT}{location}"',
        ),
        (
            'ROOTS = {"fuse": "/dbfs"}',
            'value = ROOTS["fuse"] + location',
        ),
        (
            'def root():\n    return "/dbfs"',
            "value = root() + location",
        ),
        (
            'class Paths:\n    fuse = "/dbfs"',
            "value = Paths.fuse + location",
        ),
        (
            'ROOTS = ("/dbfs", "dbfs:/")',
            "value = ROOTS[0] + location",
        ),
        ("", 'parts = ["/dbfs", location]\nvalue = "".join(parts)'),
        ("", 'loc2 = location\nvalue = "/dbfs" + loc2'),
        (
            "",
            'def to_fuse(path):\n    return "/dbfs" + path\n'
            "value = to_fuse(location)",
        ),
    ],
)
def test_indirect_dbfs_root_consumers_keep_filestore_producer_raw(
    jm, setup, consumer
):
    cells = [_cell('location = "/FileStore/tables/x"')]
    if setup:
        cells.insert(0, _cell(setup))
    cells.append(_cell(consumer))

    result = _finalize_after_initial_translation(jm, cells)
    namespace = {"os": os}
    for cell in result:
        exec(_active_source([cell], 0), namespace)

    producer_index = 1 if setup else 0
    assert _active_source(result, producer_index) == (
        'location = "/FileStore/tables/x"'
    )
    assert namespace["value"] == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


def test_cross_notebook_root_variable_marks_child_producer_unsafe(jm):
    parent_consumers = jm._dbfs_root_consumer_names(
        [
            'DBFS_ROOT = "/dbfs"',
            "value = DBFS_ROOT + location",
        ]
    )

    child = jm._finalize_child_artifact_sources(
        ['location = "/FileStore/tables/x"'],
        external_unsafe_names=parent_consumers,
    )

    assert "location" in parent_consumers
    assert _active_source([_cell(child[0])], 0) == (
        'location = "/FileStore/tables/x"'
    )


def test_dbfs_finalizer_preserves_backslash_continuation(jm):
    cells = [
        _cell('location = \\\n    "/FileStore/tables/x"'),
        _cell('value = "/dbfs" + location'),
    ]

    result = jm._finalize_dbfs_location_variables(cells)

    for cell in result:
        compile(_active_source([cell], 0), "<dbfs-backslash>", "exec")
    assert _execute_path(result) == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


def test_format_receiver_dbfs_root_does_not_double_normalized_location(jm):
    cells = [
        _cell('location = "/FileStore/tables/x"'),
        _cell('value = "/dbfs{}".format(location)'),
    ]

    result = _finalize_after_initial_translation(jm, cells)
    namespace = {"os": os}
    for cell in result:
        exec(_source([cell], 0), namespace)

    assert namespace["value"] == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


@pytest.mark.parametrize(
    "setup, consumer",
    [
        ('if True:\n    ROOT = "/dbfs"', "value = ROOT + location"),
        ("", 'value = "/dbfs"\nvalue += location'),
        ('ROOT, other = ("/dbfs", "x")', "value = ROOT + location"),
        (
            'class Paths:\n    @classmethod\n    def root(cls):\n        return "/dbfs"',
            "value = Paths.root() + location",
        ),
        (
            'class Paths:\n    @staticmethod\n    def root():\n        return "/dbfs"',
            "value = Paths.root() + location",
        ),
        (
            'class Paths:\n    def __init__(self):\n        self.fuse = "/dbfs"\npaths = Paths()',
            "value = paths.fuse + location",
        ),
        (
            'def base():\n    return "/dbfs"\ndef root():\n    return base()',
            "value = root() + location",
        ),
        (
            'def root():\n    local = "/dbfs"\n    return local',
            "value = root() + location",
        ),
    ],
)
def test_indirect_root_allowlist_prevents_double_prefix(jm, setup, consumer):
    cells = [_cell('location = "/FileStore/tables/x"')]
    if setup:
        cells.insert(0, _cell(setup))
    cells.append(_cell(consumer))

    result = _finalize_after_initial_translation(jm, cells)
    namespace = {}
    for cell in result:
        exec(_active_source([cell], 0), namespace)

    producer_index = 1 if setup else 0
    assert _active_source(result, producer_index) == (
        'location = "/FileStore/tables/x"'
    )
    assert namespace["value"] == (
        "/Volumes/default/default/dbfs/FileStore/tables/x"
    )


@pytest.mark.parametrize("template", ["dbfs:{}", "dbfs:{0}", "dbfs:{p}"])
def test_dbfs_colon_format_receivers_are_translated(jm, template):
    source = f'path = "{template}".format("/FileStore/x", p="/FileStore/x")'
    translated = jm._apply_dbfs_translations(source)

    assert "dbfs:" not in translated
    assert "/Volumes/default/default/dbfs" in translated


def test_translate_path_assignment_placeholder_is_not_corrupted(jm):
    source = 'location = translate_path("/FileStore/tables/x")'
    result = jm._finalize_dbfs_location_variables([_cell(source)])

    assert "__AIDP_" not in _source(result, 0)
    assert 'translate_path("/FileStore/tables/x")' in _source(result, 0)


def test_marker_normalized_child_fails_for_unsafe_parent(jm):
    normalized = jm._finalize_dbfs_location_variables(
        [_cell('location = "/FileStore/tables/x"')]
    )

    with pytest.raises(jm.FinalArtifactValidationError, match="unsafe for this parent"):
        jm._finalize_dbfs_location_variables(
            normalized, external_unsafe_names={"location"}
        )


def _recognizer_source(volume_root, form):
    """Working model repairs: recognition tokens are not file locations."""
    if form == "branches":
        body = (
            '    if path.startswith("dbfs:/"):\n'
            f'        return {volume_root!r} + path[len("dbfs:/"):]\n'
            '    if path.startswith("/dbfs/"):\n'
            f'        return {volume_root!r} + path[len("/dbfs/"):]\n'
        )
        setup = ""
    elif form == "constants":
        setup = 'DBFS_URI = "dbfs:/"\nDBFS_FUSE = "/dbfs/"\n'
        body = (
            '    for prefix in (DBFS_URI, DBFS_FUSE):\n'
            '        if path.startswith(prefix):\n'
            f'            return {volume_root!r} + path[len(prefix):]\n'
        )
    else:
        setup = ""
        body = (
            '    for prefix in ("dbfs:/", "/dbfs/"):\n'
            '        if path.startswith(prefix):\n'
            f'            return path.replace(prefix, {volume_root!r}, 1)\n'
        )
    return (
        '# Oracle tool modification: repaired DBFS input recognition\n'
        '# Keep dbfs:/ and /dbfs/ recognition distinct from the Volume target.\n'
        + setup + 'def _to_local(path):\n' + body
        + f'    if path.startswith({volume_root!r}):\n        return path\n'
        + '    return path\n'
    )


@pytest.mark.parametrize("form", ["branches", "constants", "replace"])
@pytest.mark.parametrize("save_path", ["parent", "child", "dependency"])
@pytest.mark.parametrize("configured_root", [None, "/Volumes/catalog/schema/dbfs/"])
def test_saved_recognizer_preserves_validated_behavior(
    jm, monkeypatch, form, save_path, configured_root
):
    for key in ("AIDP_DBFS_PREFIX", "AIDP_DBFS_CATALOG", "AIDP_DBFS_SCHEMA"):
        monkeypatch.delenv(key, raising=False)
    if configured_root:
        monkeypatch.setenv("AIDP_DBFS_PREFIX", configured_root)
    root = configured_root or "/Volumes/default/default/dbfs/"
    source = _recognizer_source(root, form)
    inputs = ["dbfs:/FileStore/plan.md", "/dbfs/FileStore/plan.md",
              root + "FileStore/plan.md", "/ordinary/plan.md"]
    expected = [root + "FileStore/plan.md"] * 3 + ["/ordinary/plan.md"]
    namespace = {}
    exec(source, namespace)
    assert [namespace["_to_local"](value) for value in inputs] == expected

    if save_path == "parent":
        cells = jm._finalize_dbfs_location_variables([_cell(source)])
    else:
        cells = [_cell(value) for value in jm._finalize_child_artifact_sources(
            [source], normalize_assignments=(save_path != "dependency")
        )]
    jm._assert_final_notebook_cells(cells)
    reloaded = json.loads(json.dumps(cells))
    namespace = {}
    exec(_source(reloaded, 0), namespace)
    assert [namespace["_to_local"](value) for value in inputs] == expected
    assert _source(reloaded, 0) == source
    assert jm._finalize_dbfs_location_variables(reloaded) == reloaded


@pytest.mark.parametrize("form", ["branches", "constants", "replace"])
@pytest.mark.parametrize("mode", ["parent", "child"])
def test_repaired_recognizer_survives_real_execution_and_save(
    jm, monkeypatch, tmp_path, form, mode
):
    """Execute the actual repair loop, save, reload and call with DBFS input.

    The fake cluster executes only the synthetic converter and child file
    save/probe scripts; all platform, catalog and model calls stay offline.
    """
    from test_conditional_exit_below_execute import _Kernel, _patch_common, _saved_sources
    import cluster_lifecycle

    root = "/Volumes/default/default/dbfs/"
    for key in ("AIDP_DBFS_PREFIX", "AIDP_DBFS_CATALOG", "AIDP_DBFS_SCHEMA"):
        monkeypatch.delenv(key, raising=False)
    repaired = _recognizer_source(root, form)
    # Force an actual execution error and call_fix, not DIRECT_EXECUTE or a
    # patched finalizer. The returned model repair is correct before saving.
    source = 'def _to_local(path):\n    return path\n'
    notebook = {"cells": [_cell(source)], "metadata": {},
                "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook)
    monkeypatch.setattr(jm, "_notebook_content_cache", {})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])
    fixes = []
    migrate_prompts = []

    async def migrate(system_prompt, *args, **kwargs):
        migrate_prompts.append(system_prompt)
        return source, {"input": 0, "output": 0}

    async def repair(*args, **kwargs):
        fixes.append(args)
        return repaired

    async def no_local_modules(*args, **kwargs):
        return set(), set(), []

    async def summarize(*args, **kwargs):
        return "synthetic path converter"

    monkeypatch.setattr(jm, "call_fix", repair)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(cluster_lifecycle, "_discover_local_modules", no_local_modules)

    class Kernel(_Kernel):
        def __init__(self):
            super().__init__()
            self.validated = []

        async def execute(self, code, **kwargs):
            if "__AIDP_CHILD_COUNT__" in code or "all_cells = payload.get" in code:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    exec(code, {})
                return {"status": "ok", "outputs": [{"type": "stream",
                        "name": "stdout", "text": output.getvalue()}]}
            if "def _to_local(" in code and "base64.b64decode" not in code:
                namespace = {}
                exec(code, namespace)
                actual = namespace["_to_local"]("dbfs:/FileStore/plan.md")
                if actual != root + "FileStore/plan.md":
                    return {"status": "error", "outputs": [{"type": "error",
                            "ename": "AssertionError", "evalue": "DBFS recognition failed",
                            "traceback": ["AssertionError: DBFS recognition failed"]}]}
                self.validated.append(code)
            return await super().execute(code, **kwargs)

    kernel = Kernel()
    if mode == "parent":
        # Exercise delivery of the DBFS guidance through the real model-call
        # boundary, then retain the existing execution/repair/save assertions.
        monkeypatch.setattr(jm, "_has_databricks_markers", lambda *a, **k: True)
        monkeypatch.setattr(jm, "call_opus_with_tools", migrate)
        result = asyncio.run(jm.process_notebook(
            "/Workspace/test_cond/source.ipynb", kernel, "fixture", "01_task", {}
        ))
        assert result["status"] == "PASS", result
        _, saved = _saved_sources(kernel)
        assert migrate_prompts == [jm.CELL_MIGRATE_PROMPT]
        assert (
            "For path reads that bypass the open shim (`Path.read_text`, `io.open`, "
            "`os.path` checks on raw paths), use Option B — wrap the path in "
            "`translate_path()` — rather than a hand-written recognizer."
        ) in " ".join(migrate_prompts[0].split())
    else:
        path = tmp_path / "child.ipynb"
        path.write_text(json.dumps(notebook), encoding="utf-8")
        monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
        monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
        monkeypatch.setitem(jm._notebook_content_cache, str(path), json.dumps(notebook))
        log = type("Log", (), {"log": lambda self, message: None})()
        result = asyncio.run(jm._inline_child_notebook(
            str(path), kernel, log, "fixture", "", "", "", {}
        ))
        assert result["status"] == "ok", result
        saved = json.loads(path.read_text(encoding="utf-8"))
        cached = json.loads(jm._notebook_content_cache[str(path)])
        assert [cell["source"] for cell in cached["cells"]] == [
            cell["source"] for cell in saved["cells"]
        ]
    assert len(fixes) == 1
    assert kernel.validated
    saved_source = next("".join(cell["source"]) for cell in saved["cells"]
                        if "def _to_local(" in "".join(cell["source"]))
    namespace = {}
    exec(saved_source, namespace)
    for value in ("dbfs:/FileStore/plan.md", "/dbfs/FileStore/plan.md",
                  root + "FileStore/plan.md"):
        assert namespace["_to_local"](value) == root + "FileStore/plan.md"
    assert repaired.strip() in saved_source
