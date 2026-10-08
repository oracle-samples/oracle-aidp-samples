"""The YAML custom-rule engine (converters.custom_rules) and the Spark
optimizer driver (optimizer.optimizer).

Custom rules are the user's escape hatch for pushing conversion rates up
without code changes, so the pinned behaviour is the contract in the
docstrings: priority order, disabled rules dropped, template placeholders,
a broken rule never crashing the run, and validation reporting each
problem. For the optimizer the contract is what ``optimize`` reports and
what ``apply_auto_optimizations`` actually rewrites -- only auto-applicable
suggestions, each at most once, leaving everything else untouched.
"""
from __future__ import annotations

import pytest

from infa2aidp.converters.custom_rules import CustomRule, CustomRuleEngine
from infa2aidp.optimizer.models import (
    OptimizationReport,
    OptimizationSuggestion,
    OptimizationType,
)
from infa2aidp.optimizer.optimizer import SparkOptimizer
from infa2aidp.optimizer.rules import AQERule, BroadcastJoinRule, ColumnPruningRule


# ---------------------------------------------------------------------------
# Custom rule engine
# ---------------------------------------------------------------------------

RULES_YAML = """
rules:
  - name: low
    match_type: function
    match_pattern: 'IIF\\('
    replacement: 'F.when('
    priority: 10
  - name: high
    match_type: expression
    match_pattern: 'SYSDATE'
    replacement: 'F.current_timestamp()'
    priority: 90
  - name: off
    match_pattern: 'X'
    replacement: 'Y'
    enabled: false
  - match_pattern: 'NVL'
    replacement: 'F.coalesce'
"""


class TestCustomRuleLoading:
    def test_rules_load_sorted_by_priority_and_disabled_dropped(self, tmp_path):
        p = tmp_path / "rules.yaml"
        p.write_text(RULES_YAML, encoding="utf-8")
        eng = CustomRuleEngine(str(p))
        assert [r.name for r in eng.rules] == ["high", "unnamed", "low"]
        assert eng.rules[1].priority == 50            # default
        assert eng.rules[0].match_type == "expression"

    @pytest.mark.parametrize("text", ["", "other: 1\n"])
    def test_file_without_rules_loads_nothing(self, tmp_path, text):
        p = tmp_path / "rules.yaml"
        p.write_text(text, encoding="utf-8")
        assert CustomRuleEngine(str(p)).rules == []

    def test_no_path_means_no_rules(self):
        assert CustomRuleEngine().rules == []

    def test_add_rule_keeps_priority_order(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("a", match_pattern="a", replacement="b", priority=20))
        eng.add_rule(CustomRule("b", match_pattern="a", replacement="b", priority=80))
        assert [r.name for r in eng.rules] == ["b", "a"]


class TestCustomRuleApplication:
    def test_rules_apply_in_priority_order_and_report_names(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("second", match_pattern=r"B", replacement="C", priority=10))
        eng.add_rule(CustomRule("first", match_pattern=r"A", replacement="B", priority=90))
        out, applied = eng.apply_rules("A")
        assert out == "C"                       # A->B (first), then B->C (second)
        assert applied == ["first", "second"]

    def test_match_groups_and_context_placeholders(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule(
            "lkp", match_pattern=r"LKP_(\w+)\((\w+)\)",
            replacement='lookup("{schema}.{group1}", key={group2}, raw="{match}", t={table})'))
        out, applied = eng.apply_rules(
            "LKP_CUST(ID) + 1", context={"schema": "dw", "table": "orders"})
        assert out == 'lookup("dw.CUST", key=ID, raw="LKP_CUST(ID)", t=orders) + 1'
        assert applied == ["lkp"]

    def test_every_occurrence_is_replaced(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("nvl", match_pattern=r"NVL\(", replacement="F.coalesce("))
        out, _ = eng.apply_rules("NVL(a, NVL(b, 0))")
        assert out == "F.coalesce(a, F.coalesce(b, 0))"

    def test_unmatched_optional_group_is_left_as_placeholder(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("g", match_pattern=r"A(B)?", replacement="<{group1}>"))
        assert eng.apply_rules("A")[0] == "<{group1}>"
        assert eng.apply_rules("AB")[0] == "<B>"

    def test_no_match_and_identity_replacement_are_not_reported(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("nomatch", match_pattern="ZZZ", replacement="Q"))
        eng.add_rule(CustomRule("same", match_pattern="abc", replacement="{match}"))
        assert eng.apply_rules("abc") == ("abc", [])

    def test_a_broken_rule_is_skipped_not_fatal(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("bad", match_pattern="([", replacement="x", priority=99))
        eng.add_rule(CustomRule("good", match_pattern="a", replacement="b"))
        assert eng.apply_rules("a") == ("b", ["good"])


class TestCustomRuleValidation:
    def test_valid_rules_have_no_issues(self):
        eng = CustomRuleEngine()
        eng.add_rule(CustomRule("ok", match_type="sql", match_pattern="x", replacement="y", priority=0))
        eng.add_rule(CustomRule("ok2", match_pattern="x", replacement="y", priority=100))
        assert eng.validate_rules() == []

    def test_each_problem_is_reported(self):
        eng = CustomRuleEngine()
        eng.rules = [
            CustomRule("", match_pattern="a", replacement="b"),
            CustomRule("dup", match_pattern="a", replacement="b"),
            CustomRule("dup", match_pattern="a", replacement="b"),
            CustomRule("empty", match_pattern="", replacement=""),
            CustomRule("regex", match_pattern="(unclosed", replacement="b"),
            CustomRule("kind", match_type="magic", match_pattern="a", replacement="b"),
            CustomRule("prio", match_pattern="a", replacement="b", priority=150),
        ]
        issues = eng.validate_rules()
        assert "Rule with empty name found." in issues
        assert "Duplicate rule name: 'dup'." in issues
        assert "Rule 'empty': empty match_pattern." in issues
        assert "Rule 'empty': empty replacement." in issues
        assert any(i.startswith("Rule 'regex': invalid regex") for i in issues)
        assert "Rule 'kind': unknown match_type 'magic'." in issues
        assert "Rule 'prio': priority 150 outside 0-100 range." in issues
        assert len(issues) == 7


# ---------------------------------------------------------------------------
# Spark optimizer
# ---------------------------------------------------------------------------

NOTEBOOK = '''from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
df_orders = spark.table("src.orders")
lkp_cust = spark.table("dim.customer")
df_a = df_orders.join(lkp_cust, on=["CUST_ID"], how="left")
df_b = df_a.join(lkp_cust, on=["CUST_ID"], how="left")
df_c = df_b.groupBy("REGION").agg(F.sum(F.col("AMT")))
df_c.write.mode("overwrite").saveAsTable("dw.region_sales")
'''


class TestSparkOptimizer:
    def test_optimize_reports_counts_name_and_estimate(self):
        report = SparkOptimizer().optimize(NOTEBOOK, context={"notebook_name": "nb_sales"})
        assert report.notebook_name == "nb_sales"
        names = [s.rule_name for s in report.suggestions]
        assert names.count("Broadcast Join") == 2
        assert "Adaptive Query Execution" in names
        assert report.total_suggestions == len(report.suggestions)
        assert report.auto_applicable == sum(s.auto_applicable for s in report.suggestions)
        assert report.manual_review == report.total_suggestions - report.auto_applicable
        assert report.estimated_overall_improvement == "2-3x faster execution"   # 2 HIGH

    def test_auto_apply_rewrites_only_auto_applicable_suggestions(self):
        opt = SparkOptimizer()
        code, applied = opt.apply_auto_optimizations(NOTEBOOK)
        assert code.count(".join(F.broadcast(lkp_cust),") == 2
        assert ".join(lkp_cust," not in code
        assert 'spark.conf.set("spark.sql.adaptive.enabled", "true")' in code
        assert code.count("spark = SparkSession.builder.getOrCreate()") == 1
        assert all(s.auto_applicable for s in applied)
        assert {s.rule_name for s in applied} == {"Broadcast Join", "Adaptive Query Execution"}
        # manual-review suggestions (column pruning, delta optimize) are not applied
        assert 'spark.table("src.orders").select(' not in code
        assert "OPTIMIZE" not in code

    def test_auto_apply_is_idempotent(self):
        opt = SparkOptimizer()
        once, _ = opt.apply_auto_optimizations(NOTEBOOK)
        twice, applied = opt.apply_auto_optimizations(once)
        assert twice == once and applied == []

    def test_suggestion_whose_text_is_absent_is_not_reported_applied(self):
        class Stale(BroadcastJoinRule):
            def analyze(self, code, context=None):
                return [OptimizationSuggestion(
                    "stale", OptimizationType.BROADCAST_JOIN, "d",
                    original_code="not in the code", optimized_code="x", auto_applicable=True),
                    OptimizationSuggestion(
                    "blank", OptimizationType.BROADCAST_JOIN, "d",
                    original_code="", optimized_code="x", auto_applicable=True)]
        code, applied = SparkOptimizer(rules=[Stale()]).apply_auto_optimizations("abc")
        assert (code, applied) == ("abc", [])

    def test_explicit_empty_rule_list_suggests_nothing(self):
        r = SparkOptimizer(rules=[]).optimize(NOTEBOOK)
        assert r.suggestions == [] and r.estimated_overall_improvement == "No optimizations needed"

    @pytest.mark.parametrize("priorities,expected", [
        (["HIGH"] * 3, "3-5x faster execution"),
        (["CRITICAL"], "2-3x faster execution"),
        (["MEDIUM", "MEDIUM"], "1.5-2x faster execution"),
        (["LOW"], "Minor performance gains"),
        ([], "No optimizations needed"),
    ])
    def test_overall_estimate_tiers(self, priorities, expected):
        rep = OptimizationReport(suggestions=[
            OptimizationSuggestion("r", OptimizationType.AQE, "d", priority=p) for p in priorities])
        assert SparkOptimizer._estimate_overall(rep) == expected

    @pytest.mark.parametrize("priorities,expected", [
        (["HIGH"] * 5, "3-5x faster execution"),
        (["HIGH", "HIGH"], "2-3x faster execution"),
        (["LOW"] * 3, "1.5-2x faster execution"),
        (["LOW"], "Minor performance gains"),
        ([], "No optimizations needed"),
    ])
    def test_aggregate_estimate_tiers(self, priorities, expected):
        rep = OptimizationReport(suggestions=[
            OptimizationSuggestion("r", OptimizationType.AQE, "d", priority=p) for p in priorities])
        assert SparkOptimizer._aggregate_improvement([rep]) == expected

    def test_markdown_report(self, tmp_path):
        opt = SparkOptimizer()
        r1 = opt.optimize(NOTEBOOK, context={"notebook_name": "nb_sales"})
        r1.suggestions[0].description += " | with pipe"
        r2 = opt.optimize("x = 1", context={"notebook_name": "nb_empty"})
        out = tmp_path / "opt.md"
        md = opt.generate_report([r1, r2], str(out))
        assert out.read_text(encoding="utf-8") == md
        assert "- Notebooks analyzed: 2" in md
        assert f"- Total suggestions: {r1.total_suggestions}" in md
        assert f"- Auto-applicable: {r1.auto_applicable}" in md
        assert "## nb_sales" in md and "## nb_empty" not in md
        assert "\\| with pipe" in md                  # table cell pipes escaped
        assert "**Before:**" in md and "**After:**" in md
        assert ".join(F.broadcast(lkp_cust)," in md
        assert "*Estimated improvement: 2-10x faster join*" in md

    def test_report_without_output_path_only_returns(self):
        md = SparkOptimizer().generate_report([OptimizationReport()])
        assert "- Total suggestions: 0" in md and "Unnamed" not in md

    def test_unnamed_notebook_heading(self):
        rep = SparkOptimizer().optimize(NOTEBOOK)
        md = SparkOptimizer().generate_report([rep])
        assert "## Unnamed Notebook" in md


class TestRuleSpotChecks:
    """Two rules whose output the auto-apply path depends on directly."""

    def test_already_broadcast_join_is_not_suggested(self):
        code = "df = a.join(F.broadcast(lkp_x), 'k')\ndf2 = a.join(lkp_y, 'k')"
        hits = BroadcastJoinRule().analyze(code)
        assert [h.applies_to for h in hits] == ["lkp_y"]
        assert hits[0].line_number == 2

    def test_aqe_only_for_complex_pipelines_and_only_once(self):
        assert AQERule().analyze("a.join(b)") == []
        assert AQERule().analyze("a.groupBy('x').agg(F.sum('y'))")
        assert AQERule().analyze(
            'spark.conf.set("spark.sql.adaptive.enabled", "true")\na.join(b).join(c)') == []

    def test_column_pruning_lists_referenced_columns(self):
        code = 'df_x = spark.table("s.t")\ndf_y = df_x.where(F.col("A") > 1).select(F.col("B"))'
        (hit,) = ColumnPruningRule().analyze(code)
        assert hit.optimized_code == 'df_x = spark.table("s.t").select("A", "B")'
        assert not hit.auto_applicable
