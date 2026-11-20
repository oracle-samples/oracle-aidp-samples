"""Tests for conversions consolidated from the pilot deployment's manual-improvements sheet.

Covers the deterministic rewriters added to scripts/job_migrate.py, the
entity_decrypt (entity-crypto ensure_arn) support, and asserts the field-learned prompt
rules are present in the migration/fix prompts.

Run: python -m pytest tests/test_examplecorp_manual_improvements.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── Deterministic rewriters (job_migrate.py) ────────────────────────────────

@pytest.fixture(scope="module")
def jm():
    import job_migrate
    return job_migrate


class TestTransformersAdamW:
    def test_alone(self, jm):
        out = jm._rewrite_transformers_adamw("from transformers import AdamW")
        assert out.startswith("from torch.optim import AdamW")
        assert "transformers" not in out.split("#")[0]

    def test_with_other_symbols(self, jm):
        src = "from transformers import AdamW, get_linear_schedule_with_warmup"
        out = jm._rewrite_transformers_adamw(src)
        assert "from transformers import get_linear_schedule_with_warmup" in out
        assert "from torch.optim import AdamW" in out

    def test_preserves_indent(self, jm):
        out = jm._rewrite_transformers_adamw("    from transformers import BertModel, AdamW")
        assert "    from transformers import BertModel" in out
        assert "    from torch.optim import AdamW" in out

    def test_no_adamw_untouched(self, jm):
        src = "from transformers import BertModel, BertTokenizer"
        assert jm._rewrite_transformers_adamw(src) == src

    def test_alias(self, jm):
        out = jm._rewrite_transformers_adamw("from transformers import AdamW as Opt")
        assert "from torch.optim import AdamW as Opt" in out
        assert "transformers" not in out.split("#")[0]

    def test_parenthesized_multiline(self, jm):
        src = "from transformers import (\n    BertModel,\n    AdamW,\n)"
        out = jm._rewrite_transformers_adamw(src)
        assert "from torch.optim import AdamW" in out
        assert "BertModel" in out
        assert "AdamW" not in out.split("torch.optim")[0]  # not left in transformers part

    def test_idempotent(self, jm):
        once = jm._rewrite_transformers_adamw("from transformers import AdamW")
        assert jm._rewrite_transformers_adamw(once) == once


class TestMatplotlibMagic:
    def test_inline_commented(self, jm):
        out = jm._comment_out_unsupported_magics("%matplotlib inline\nimport matplotlib")
        assert out.splitlines()[0].lstrip().startswith("#")
        assert "%matplotlib inline" in out  # preserved in comment
        assert "import matplotlib" in out

    def test_notebook_variant_and_indent(self, jm):
        out = jm._comment_out_unsupported_magics("  %matplotlib notebook")
        assert out.strip().startswith("#")

    def test_trailing_comment_matched(self, jm):
        out = jm._comment_out_unsupported_magics("%matplotlib inline  # keep")
        assert out.lstrip().startswith("#")
        assert "keep" in out

    def test_no_magic_untouched(self, jm):
        src = "import matplotlib.pyplot as plt"
        assert jm._comment_out_unsupported_magics(src) == src

    def test_idempotent(self, jm):
        once = jm._comment_out_unsupported_magics("%matplotlib inline")
        assert jm._comment_out_unsupported_magics(once) == once


class TestSharedSwapHelper:
    def test_helper_runs_both_swaps(self, jm):
        src = "%matplotlib inline\nfrom transformers import AdamW"
        out = jm._apply_field_learned_swaps(src)
        assert out.splitlines()[0].lstrip().startswith("#")
        assert "from torch.optim import AdamW" in out

    def test_helper_idempotent(self, jm):
        src = "%matplotlib inline\nfrom transformers import AdamW"
        once = jm._apply_field_learned_swaps(src)
        assert jm._apply_field_learned_swaps(once) == once


class TestPandasDisplayRewrite:
    def test_dataframe_assignment_uses_ipython_display(self, jm):
        src = (
            "import pandas as pd\n"
            "column_report = pd.DataFrame({'column_name': ['eventid']})\n"
            "display(column_report)\n"
        )
        out = jm._rewrite_pandas_display_calls(src)
        assert "from IPython.display import display as _aidp_ipython_display" in out
        assert "_aidp_ipython_display(column_report)" in out
        assert "\ndisplay(column_report)" not in out

    def test_to_pandas_assignment_and_direct_call(self, jm):
        src = (
            "pdf = spark.table('cfg.prod_evaluation').toPandas()\n"
            "display(pdf)\n"
            "display(spark.table('cfg.other').toPandas())\n"
        )
        out = jm._rewrite_pandas_display_calls(src)
        assert out.count("_aidp_ipython_display(") == 2

    def test_pandas_read_and_derived_assignment(self, jm):
        src = (
            "import pandas as pandas_alias\n"
            "raw = pandas_alias.read_csv('/tmp/x.csv')\n"
            "filtered = raw.drop(columns=['unused'])\n"
            "display(filtered)\n"
        )
        out = jm._rewrite_pandas_display_calls(src)
        assert "_aidp_ipython_display(filtered)" in out

    def test_direct_dataframe_constructor(self, jm):
        src = "import pandas as pd\ndisplay(pd.DataFrame({'x': [1]}))\n"
        out = jm._rewrite_pandas_display_calls(src)
        assert "_aidp_ipython_display(pd.DataFrame" in out

    def test_spark_and_unknown_display_are_unchanged(self, jm):
        samples = [
            "display(spark.sql('select 1'))\n",
            "sdf = spark.table('cfg.prod_evaluation')\ndisplay(sdf)\n",
            "display(column_report)\n",
        ]
        for src in samples:
            assert jm._rewrite_pandas_display_calls(src) == src

    @pytest.mark.parametrize("magic", ["%python", "%pyspark"])
    def test_python_magic_is_preserved(self, jm, magic):
        src = f"{magic}\nimport pandas as pd\ndf = pd.DataFrame({{'x': [1]}})\ndisplay(df)\n"
        out = jm._rewrite_pandas_display_calls(src)
        assert out.startswith(f"{magic}\n")
        assert "_aidp_ipython_display(df)" in out

    def test_existing_ipython_display_is_untouched(self, jm):
        src = (
            "from IPython.display import display\n"
            "import pandas as pd\n"
            "df = pd.DataFrame({'x': [1]})\n"
            "display(df)\n"
        )
        assert jm._rewrite_pandas_display_calls(src) == src

    def test_comments_strings_and_spark_calls_are_not_rewritten(self, jm):
        src = (
            "import pandas as pd\n"
            "df = pd.DataFrame({'text': ['display(df)']})\n"
            "# display(df)\n"
            "display(spark.range(1))\n"
        )
        out = jm._rewrite_pandas_display_calls(src)
        assert out == src

    def test_idempotent(self, jm):
        src = "import pandas as pd\ndf = pd.DataFrame({'x': [1]})\ndisplay(df)\n"
        once = jm._rewrite_pandas_display_calls(src)
        assert jm._rewrite_pandas_display_calls(once) == once

    def test_shared_helper_applies_rule(self, jm):
        src = "import pandas as pd\ndf = pd.DataFrame({'x': [1]})\ndisplay(df)\n"
        assert "_aidp_ipython_display(df)" in jm._apply_field_learned_swaps(src)


# ─── entity_decrypt / entity-crypto ensure_arn ──────────────────────────────────────

class TestEntityDecryptGuidance:
    def test_cell_analyzer_recommends_ensure_arn(self):
        import cell_analyzer
        rule = next(r for r in cell_analyzer.RISK_PATTERNS if r["name"] == "entity_decrypt_udf")
        fix = rule["fix"]
        assert "ensure_arn" in fix
        assert "ENTITY_CRYPTO_ARN" in fix
        # comment-out is the documented fallback, not the default action
        assert "ONLY fall back to commenting" in fix


class TestEnsureEntityCryptoArn:
    def test_importable_and_exported(self):
        import aidp_compat
        assert hasattr(aidp_compat, "ensure_entity_crypto_arn")
        assert "ensure_entity_crypto_arn" in aidp_compat.__all__

    def test_sets_driver_property_and_registers_udf(self):
        from aidp_compat.entity_crypto import ensure_entity_crypto_arn, ENTITY_CRYPTO_ARN_PROP

        calls = {"prop": None, "udf": None}

        class _System:
            @staticmethod
            def setProperty(k, v):
                calls["prop"] = (k, v)

        class _JVM:
            System = _System

        class _UDFReg:
            def register(self, name, fn):
                calls["udf"] = name

        class _Spark:
            _jvm = _JVM()
            udf = _UDFReg()

        arn = "arn:aws:sts::000000000000:assumed-role/databricks-ec2-role/i-0865"
        # pyspark may be absent in the unit env; only assert the driver-side path then.
        try:
            ret = ensure_entity_crypto_arn(_Spark(), arn)
        except ModuleNotFoundError:
            assert calls["prop"] == (ENTITY_CRYPTO_ARN_PROP, arn)
            return
        assert ret == arn
        assert calls["prop"] == (ENTITY_CRYPTO_ARN_PROP, arn)
        assert calls["udf"] == "ensure_arn"


# ─── Field-learned prompt rules are wired into the prompts ───────────────────

class TestPromptRulesPresent:
    def test_cell_migrate_prompt_has_examplecorp_rules(self, jm):
        p = jm.CELL_MIGRATE_PROMPT
        assert "FIELD-LEARNED RULES" in p
        assert "read_table" in p
        assert "NEVER add a new cell" in p
        # createOrReplaceTempView->GlobalTempView rule was REMOVED: the current AIDP
        # runtime shares one SparkSession across cells (verified 2026-07-24, count=5
        # read from a later cell), so plain temp views persist — no conversion needed.
        assert "createOrReplaceGlobalTempView" not in p

    def test_fix_prompt_has_runtime_rules(self):
        import agent_migrate
        p = agent_migrate.FIX_PROMPT
        assert "cannot import name 'AdamW'" in p
        assert "pytorch-lifestream==0.6.0" in p

    def test_fix_prompt_forbids_adding_cells(self):
        import agent_migrate
        assert "NEVER add, drop, or delete a cell" in agent_migrate.FIX_PROMPT
