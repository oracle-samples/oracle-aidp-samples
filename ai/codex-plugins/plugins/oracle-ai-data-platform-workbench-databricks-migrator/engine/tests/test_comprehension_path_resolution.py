"""REF-256: data-check resolution of comprehension-built read paths.

Incident: DBX job 800000000000006 (Cost Alerting) builds its CUR input as

    years = [2025]
    paths = [f"s3://example-data-metrics/.../year={y}" for y in years]
    spark.read.parquet(*paths)

Phase 1 left the reference DYNAMIC_REFERENCE/unresolved, so the concrete
input never became a verified tracker entry, no data-migration request was
raised, and Phase 2 failed with 404 BucketNotFound on both notebooks.

The fix extends the ONE central AST evaluator with bounded list/generator
comprehension evaluation and expands star/multi-path reads. Everything the
evaluator cannot prove stays unresolved — these tests pin both directions.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import data_check_extract as dce  # noqa: E402


CUR_CELL = (
    'years = [2025]\n'
    'paths = [f"s3://example-data-metrics/aws-cur-daily/DailyReport/DailyReport/year={y}"\n'
    '         for y in years]\n'
    'dailyreport = (\n'
    '    spark.read.parquet(*paths)\n'
    '    .filter(F.col("resource_tags_user_team") == "DataScience")\n'
    ')\n'
)
CUR_PATH = "s3://example-data-metrics/aws-cur-daily/DailyReport/DailyReport/year=2025"


def _analyze(src, cell_number=3, **kwargs):
    analyzer = dce._PythonReferenceAnalyzer(**kwargs)
    analyzer.analyze_cell(src, cell_number)
    return analyzer.result()


def _read_names(result):
    return [r["object_name"] for r in result["read_references"]]


def test_isu256_cur_cell_resolves_to_concrete_path():
    result = _analyze(CUR_CELL)
    assert _read_names(result) == [CUR_PATH]
    assert result["unresolved_scopes"] == []
    # the concrete reference keeps the read's filter metadata
    assert result["read_references"][0]["filters"] == [
        "resource_tags_user_team = 'DataScience'"]


def test_multiple_years_emit_one_reference_per_path_in_order():
    src = CUR_CELL.replace("[2025]", "[2024, 2025]")
    result = _analyze(src)
    assert _read_names(result) == [
        CUR_PATH.replace("2025", "2024"), CUR_PATH]


def test_extract_from_notebook_surfaces_concrete_path():
    nb = {"metadata": {"language_info": {"name": "python"}},
          "cells": [{"cell_type": "code", "source": [CUR_CELL]}]}
    agg = dce.extract_from_notebook(nb)
    assert CUR_PATH in agg["paths"]
    # The regex layer's dynamic-template row stays by design: dropping it
    # could hide a genuinely-dynamic use of the same template elsewhere.
    assert any("year={y}" in u for u in agg["unresolved"])


def test_unresolved_iterable_stays_unresolved():
    src = CUR_CELL.replace("years = [2025]",
                           'years = dbutils.widgets.get("years")')
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1
    assert result["unresolved_scopes"][0]["object_name"] is None


def test_condition_that_cannot_be_proven_stays_unresolved():
    src = CUR_CELL.replace("for y in years]", "for y in years if y > 2000]")
    result = _analyze(src)
    # _eval has no Compare support: the `if` is unprovable -> fail closed.
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_element_cap_fails_closed():
    many = ", ".join(str(2000 + i) for i in range(dce._COMPREHENSION_ELEMENT_CAP + 1))
    src = CUR_CELL.replace("[2025]", f"[{many}]")
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_tuple_unpacking_target_resolves():
    src = (
        'pairs = [(2025, 1), (2025, 2)]\n'
        'paths = [f"s3://b/x/year={y}/month={m}" for y, m in pairs]\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert _read_names(result) == [
        "s3://b/x/year=2025/month=1", "s3://b/x/year=2025/month=2"]


def test_generator_expression_argument_resolves():
    src = (
        'years = [2025]\n'
        'df = spark.read.parquet(*(f"s3://b/x/year={y}" for y in years))\n'
    )
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/x/year=2025"]


def test_multi_literal_paths_no_longer_drop_the_tail():
    # Legacy behaviour evaluated only args[0], silently dropping every
    # following path from the tracker — same REF-256 class.
    src = 'df = spark.read.parquet("s3://b/x/a", "s3://b/x/b")\n'
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/x/a", "s3://b/x/b"]


def test_mixed_resolved_and_unresolved_args_stay_one_unresolved_call():
    src = 'df = spark.read.parquet("s3://b/x/a", other)\n'
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1
    assert result["unresolved_scopes"][0]["argument_expression"] == \
        "'s3://b/x/a', other"


def test_set_comprehension_stays_unsupported():
    src = (
        'years = [2025]\n'
        'paths = {f"s3://b/x/year={y}" for y in years}\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_starred_table_read_keeps_legacy_unresolved_shape():
    src = 'names = ["db.t1"]\ndf = spark.read.table(*names)\n'
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_comprehension_variable_does_not_leak_or_clobber():
    src = (
        'y = "outer"\n'
        'years = [2025]\n'
        'paths = [f"s3://b/x/year={y}" for y in years]\n'
        'df = spark.read.parquet(f"s3://b/x/year={y}")\n'
    )
    result = _analyze(src)
    # `y` is restored after the comprehension: the later f-string read sees
    # the OUTER binding, not the comprehension's last loop value (2025).
    assert _read_names(result) == ["s3://b/x/year=outer"]
    assert result["resolved_symbols"]["y"] == "outer"
    assert result["resolved_symbols"]["paths"] == ["s3://b/x/year=2025"]


def test_nested_generators_over_literals_resolve():
    src = (
        'years = [2025]\n'
        'months = [1, 2]\n'
        'paths = [f"s3://b/x/{y}/{m}" for y in years for m in months]\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/x/2025/1", "s3://b/x/2025/2"]


# ── Round-2 review pins (2026-10-27) ────────────────────────────────────────

def test_stored_generator_is_not_materialized_at_binding():
    # A generator's body evaluates at CONSUMPTION; rebinding `prefix` between
    # binding and the read made the eager capture WRONG. Stored generators
    # must stay unresolved.
    src = (
        'prefix = "s3://b/old"\n'
        'paths = (f"{prefix}/year={y}" for y in [2025])\n'
        'prefix = "s3://b/new"\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1
    # and no stale value leaked into resolved symbols
    assert "s3://b/old/year=2025" not in str(result["resolved_symbols"])


def test_inline_generator_argument_still_resolves_at_consumption():
    src = (
        'prefix = "s3://b/old"\n'
        'prefix = "s3://b/new"\n'
        'df = spark.read.parquet(*(f"{prefix}/year={y}" for y in [2025]))\n'
    )
    result = _analyze(src)
    # consumption-site evaluation sees the CURRENT binding
    assert _read_names(result) == ["s3://b/new/year=2025"]


def test_format_spec_is_applied_not_dropped():
    src = (
        'months = [1]\n'
        'paths = [f"s3://b/x/month={m:02d}" for m in months]\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/x/month=01"]


def test_scalar_fstring_format_spec_fixed_too():
    # Pre-existing bug in the same class: scalar f-strings dropped the spec.
    src = 'm = 1\ndf = spark.read.parquet(f"s3://b/x/month={m:02d}")\n'
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/x/month=01"]


def test_unresolvable_format_spec_fails_closed():
    src = (
        'm = 1\n'
        'width = dbutils.widgets.get("w")\n'
        'df = spark.read.parquet(f"s3://b/x/month={m:{width}}")\n'
    )
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_load_positional_options_are_not_paths():
    # load(path, format, ...) — args[1:] are OPTIONS; the legacy single-arg
    # flow records exactly one path and never invents a "parquet" dependency.
    src = 'df = spark.read.load("s3://b/input", "parquet")\n'
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/input"]


def test_csv_second_positional_is_not_a_path():
    src = 'df = spark.read.csv("s3://b/input", schema)\n'
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/input"]


def test_cartesian_work_behind_false_filter_fails_closed():
    big = "[" + ", ".join(str(i) for i in range(100)) + "]"
    src = (
        f'a = {big}\n'
        f'b = {big}\n'
        'paths = [f"s3://b/{x}/{y}" for x in a for y in b if False]\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    # 10,000 iterations exceed the work cap long before any element is
    # emitted; the comprehension stays unresolved instead of spinning.
    # Asserting on the SYMBOL distinguishes the cap from an uncapped empty
    # result: without the cap `paths` would resolve to [] and appear here.
    assert "paths" not in result["resolved_symbols"]
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_orc_and_text_positional_options_are_not_paths():
    # Spark 3.5: only parquet is variadic; orc/text take (path_or_list,
    # *positional options). The second positional must not become a path
    # dependency, and the call must stay RESOLVED on its real path.
    for api, option in (("orc", "True"), ("text", "True")):
        src = f'df = spark.read.{api}("s3://b/input", {option})\n'
        result = _analyze(src)
        assert _read_names(result) == ["s3://b/input"], api


def test_nested_comprehensions_share_the_work_budget():
    # Each inner evaluation stays under the per-call cap (40*40=1600 < 2048)
    # but the outer loop re-evaluates it 40 times: ~64k total iterations.
    # A per-call counter would admit this; the shared budget must not.
    n40 = "[" + ", ".join(str(i) for i in range(40)) + "]"
    src = (
        f'n40 = {n40}\n'
        'empty = [a for a in n40\n'
        '         for b in [c for c in n40 for d in n40 if False]]\n'
        'df = spark.read.parquet(*empty)\n'
    )
    result = _analyze(src)
    assert "empty" not in result["resolved_symbols"]
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_budget_resets_between_separate_comprehensions():
    # The budget is per outermost evaluation, not per cell: two sibling
    # comprehensions each well under the cap must BOTH resolve.
    n40 = "[" + ", ".join(str(i) for i in range(40)) + "]"
    src = (
        f'n40 = {n40}\n'
        'a = [f"s3://b/a/{i}" for i in n40]\n'
        'b = [f"s3://b/b/{i}" for i in n40]\n'
        'df = spark.read.parquet(*a)\n'
        'df2 = spark.read.parquet(*b)\n'
    )
    result = _analyze(src)
    assert len(result["read_references"]) == 80


# ── Sentinel-propagation invariant (review round 2026-10-27) ─────────────────

def test_reviewer_repro_unknown_strip_stays_unresolved():
    # Reproduced finding: p.strip(unknown_widget_suffix) inside a
    # comprehension minted a WRONG concrete tracker entry (s3://b/foo/)
    # instead of staying unresolved. The actual runtime read depends on the
    # widget value, so nothing concrete may be reported.
    src = (
        "suffix = dbutils.widgets.get('suffix')\n"
        "paths = [f's3://b/{p.strip(suffix)}' for p in ['foo/']]\n"
        "df = spark.read.parquet(*paths)\n"
    )
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1
    assert "paths" not in result["resolved_symbols"]


def test_resolved_strip_still_evaluates():
    src = (
        "suffix = '/'\n"
        "paths = [f's3://b/{p.strip(suffix)}' for p in ['foo/']]\n"
        "df = spark.read.parquet(*paths)\n"
    )
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/foo"]


def test_unknown_argument_propagates_across_supported_calls():
    # Invariant: an unresolved argument makes the whole call unresolved for
    # EVERY coercing operation — none may stringify the sentinel.
    expressions = [
        "'a/'.strip(u)",
        "'a-b'.replace(u, '_')",
        "'a-b'.replace('-', u)",
        "str(u)",
        "{'k': 'v'}.get(u)",
        "{'k': 'v'}.get(u, 'd')",
    ]
    for expr in expressions:
        src = (
            "u = dbutils.widgets.get('u')\n"
            f"candidate = {expr}\n"
            "df = spark.read.parquet(f's3://b/{candidate}')\n"
        )
        result = _analyze(src)
        assert result["read_references"] == [], expr
        assert "candidate" not in result["resolved_symbols"], expr


def test_widget_default_sentinel_contract_untouched():
    # The widget/parameter getters sit ABOVE the guard: a provided runtime
    # parameter still resolves even when the call's default argument is
    # itself unresolved.
    src = (
        "v = dbutils.widgets.get('known', other_unknown)\n"
        "df = spark.read.parquet(f's3://b/{v}')\n"
    )
    result = _analyze(src, runtime_parameters={"known": "data"})
    assert _read_names(result) == ["s3://b/data"]


# ── Reviewer repro pins (2026-10-27, executed base/head repros) ───────────────

def test_class_body_comprehension_stays_unresolved():
    # Python resolves a comprehension body against the ENCLOSING scope —
    # class names are invisible to it. The evaluator's env holds class-local
    # bindings there, so resolving would certify the WRONG path
    # (/Volumes/demo/class/2025 while Python reads /Volumes/demo/global/2025)
    # — reproduced end-to-end as a false 'ready to migrate'. Fail closed:
    # exactly base behaviour.
    src = (
        'prefix = "/Volumes/demo/" + "global"\n'
        'class C:\n'
        '    prefix = "/Volumes/demo/" + "class"\n'
        '    paths = [prefix + "/" + str(y) for y in [2025]]\n'
        '    df = (spark.read).parquet(*paths)\n'
    )
    result = _analyze(src)
    assert result["read_references"] == []
    # no wrong concrete identity anywhere in the result
    assert "/Volumes/demo/class/2025" not in str(result)
    assert len(result["unresolved_scopes"]) == 1


def test_module_level_comprehension_unaffected_by_class_guard():
    src = (
        'prefix = "/Volumes/demo/global"\n'
        'paths = [prefix + "/" + str(y) for y in [2025]]\n'
        'df = spark.read.parquet(*paths)\n'
    )
    result = _analyze(src)
    assert _read_names(result) == ["/Volumes/demo/global/2025"]


def test_empty_resolved_star_expansion_contributes_zero_args():
    # parquet(p, *[]) == parquet(p): the known concrete read must survive.
    # Head previously unresolved the whole call and LOST the dependency.
    src = (
        'p = "/Volumes/demo/" + "base"\n'
        'extra = []\n'
        'df = spark.read.parquet(p, *extra)\n'
    )
    result = _analyze(src)
    assert _read_names(result) == ["/Volumes/demo/base"]
    assert result["unresolved_scopes"] == []


def test_all_empty_expansion_stays_an_explicit_gap():
    src = 'extra = []\ndf = spark.read.parquet(*extra)\n'
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_unresolved_star_still_unresolved_after_empty_fix():
    src = 'df = spark.read.parquet("s3://b/a", *unknown_list)\n'
    result = _analyze(src)
    assert result["read_references"] == []
    assert len(result["unresolved_scopes"]) == 1


def test_reviewer_repros_through_production_extraction():
    # Same two repros through the production extraction entry point
    # (extract_from_notebook): the class-scope case emits NO concrete path
    # (and no wrong identity); the empty-expansion case keeps its known path.
    class_src = (
        'prefix = "/Volumes/demo/" + "global"\n'
        'class C:\n'
        '    prefix = "/Volumes/demo/" + "class"\n'
        '    paths = [prefix + "/" + str(y) for y in [2025]]\n'
        '    df = (spark.read).parquet(*paths)\n'
    )
    empty_src = (
        'p = "/Volumes/demo/" + "base"\n'
        'extra = []\n'
        'df = spark.read.parquet(p, *extra)\n'
    )
    nb = lambda src: {"metadata": {"language_info": {"name": "python"}},
                      "cells": [{"cell_type": "code", "source": [src]}]}
    agg_class = dce.extract_from_notebook(nb(class_src))
    assert "/Volumes/demo/class/2025" not in str(agg_class)
    assert all("/Volumes/demo" not in p for p in agg_class["paths"])
    agg_empty = dce.extract_from_notebook(nb(empty_src))
    assert "/Volumes/demo/base" in agg_empty["paths"]


# ── Argument-shape strictness (reviewer-flagged residual, owner-gated; 2026-10-28)

def test_unmodeled_argument_shapes_decline_never_misevaluate():
    # A supported call must decline any argument shape it does not model —
    # dropping an argument mints a WRONG concrete value (int(p, base=16)
    # evaluated as decimal int(p): the reviewer's accepted-residual example).
    declined = [
        "int(p, base=16)",
        "int(p, 16)",
        "float(p, extra)",
        "str(p, 'utf-8')",
        "'aaa'.replace('a', 'b', 1)",
        "'a/'.strip('/', extra)",
        "{'k': 'v'}.get('k', 'd', extra)",
        "json.loads(p, object_hook=hook)",
    ]
    for expr in declined:
        src = (
            "p = '255'\n"
            f"candidate = {expr}\n"
            "df = spark.read.parquet(f's3://b/{candidate}')\n"
        )
        result = _analyze(src)
        assert "candidate" not in result["resolved_symbols"], expr
        assert result["read_references"] == [], expr


def test_modeled_shapes_still_evaluate():
    src = (
        "p = '255'\n"
        "a = int(p)\n"
        "b = 'a-b'.replace('-', '_')\n"
        "c = 'x/'.strip('/')\n"
        "df = spark.read.parquet(f's3://b/{a}/{b}/{c}')\n"
    )
    result = _analyze(src)
    assert _read_names(result) == ["s3://b/255/a_b/x"]


def test_timedelta_and_relativedelta_unmodeled_shapes_decline():
    # Wrong-value date shapes decline. The anchor comes from the runtime
    # parameter (datetime.now() -> run_date), so it IS resolved and the
    # guard under test is genuinely exercised: without it, timedelta(5)
    # evaluated as 0 days and produced the WRONG concrete 2025-01 path
    # (the prior version of this test used an unsupported datetime(...)
    # constructor whose unknown anchor masked the guard — review
    # 2026-10-28).
    for expr in ("timedelta(5)", "relativedelta(years=1)",
                 "relativedelta(months=1, hours=3)"):
        src = (
            "from datetime import datetime, timedelta\n"
            "from dateutil.relativedelta import relativedelta\n"
            "anchor = datetime.now()\n"
            f"when = anchor - {expr}\n"
            'df = spark.read.parquet(f"s3://b/{when.strftime(\'%Y-%m\')}")\n'
        )
        result = _analyze(src, runtime_parameters={"run_date": "2025-01-15"})
        assert result["read_references"] == [], expr
        assert "when" not in result["resolved_symbols"], expr


def test_supported_date_arithmetic_still_resolves():
    # Positive control proving the anchor is live in the decline test above.
    src = (
        "from datetime import datetime, timedelta\n"
        "anchor = datetime.now()\n"
        "when = anchor - timedelta(days=3)\n"
        'df = spark.read.parquet(f"s3://b/{when.strftime(\'%Y-%m-%d\')}")\n'
    )
    result = _analyze(src, runtime_parameters={"run_date": "2025-01-15"})
    assert _read_names(result) == ["s3://b/2025-01-12"]
    # fractional days are now modeled FAITHFULLY (no int() truncation):
    # anchor 2025-01-15 00:00 minus half a day is Jan 14, not Jan 15.
    half = src.replace("timedelta(days=3)", "timedelta(days=0.5)")
    result = _analyze(half, runtime_parameters={"run_date": "2025-01-15"})
    assert _read_names(result) == ["s3://b/2025-01-14"]


def test_value_semantics_unmodeled_shapes_decline():
    # One contract, three source-proven instances (review 2026-10-28):
    # dropped **kwargs, coerced non-str string-method args, truncated
    # fractional deltas. All decline instead of mis-evaluating.
    declined = [
        'int(p, **{"base": 16})',
        '"stone".strip(none_like)',
        '"a-b".replace(1, "_")',
    ]
    for expr in declined:
        src = (
            "p = '10'\n"
            "none_like = None\n"
            f"candidate = {expr}\n"
            "df = spark.read.parquet(f's3://b/{candidate}')\n"
        )
        result = _analyze(src)
        assert "candidate" not in result["resolved_symbols"], expr
        assert result["read_references"] == [], expr
    # production-extraction parity for the named case
    nb = {"metadata": {"language_info": {"name": "python"}},
          "cells": [{"cell_type": "code", "source": [
              'p = "10"\ncandidate = int(p, **{"base": 16})\n'
              'df = spark.read.parquet(f"s3://b/{candidate}")\n']}]}
    agg = dce.extract_from_notebook(nb)
    assert all("s3://b/1" not in x for x in agg["paths"])
