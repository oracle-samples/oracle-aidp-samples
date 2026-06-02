"""Tests for CLI run command using real PySpark."""

import pytest
import yaml

from qualifire.cli import main


class TestCLIRun:
    def test_run_pass(self, tmp_path):
        """CLI run with a passing threshold check."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("cli-test").getOrCreate()
        spark.sql("CREATE OR REPLACE TEMP VIEW cli_test AS SELECT 1 AS id, 100.0 AS amount")

        config = {
            "owner": "cli-team",
            "bu": "test",
            "system_table": str(tmp_path / "cli_history.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "cli_ds",
                "table": "cli_test",
                "validations": [{
                    "type": "threshold",
                    "collection": {
                        "type": "aggregation",
                        "expressions": {"cnt": "COUNT(*)"},
                    },
                    "rules": [{"metric": "cnt", "thresholds": {"error": {"min": 1}}}],
                }],
            }],
        }
        path = tmp_path / "cli_config.yaml"
        path.write_text(yaml.dump(config))

        result = main(["run", "-c", str(path)])
        assert result == 0

    def test_run_error(self, tmp_path):
        """CLI run with a failing threshold check returns 1."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("cli-test").getOrCreate()
        spark.sql("CREATE OR REPLACE TEMP VIEW cli_fail AS SELECT 1 AS id")

        config = {
            "owner": "cli-team",
            "bu": "test",
            "system_table": str(tmp_path / "cli_history.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "cli_fail_ds",
                "table": "cli_fail",
                "validations": [{
                    "type": "threshold",
                    "collection": {
                        "type": "aggregation",
                        "expressions": {"cnt": "COUNT(*)"},
                    },
                    "rules": [{"metric": "cnt", "thresholds": {"error": {"min": 9999}}}],
                }],
            }],
        }
        path = tmp_path / "cli_config.yaml"
        path.write_text(yaml.dump(config))

        result = main(["run", "-c", str(path)])
        assert result == 1

    def test_run_with_context(self, tmp_path):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("cli-test").getOrCreate()
        spark.sql("CREATE OR REPLACE TEMP VIEW cli_ctx AS SELECT 1 AS id")

        config = {
            "owner": "team",
            "bu": "bu",
            "system_table": str(tmp_path / "h.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "ds",
                "table": "cli_ctx",
                "validations": [{
                    "type": "threshold",
                    "collection": {
                        "type": "aggregation",
                        "expressions": {"cnt": "COUNT(*)"},
                    },
                    "rules": [{"metric": "cnt", "thresholds": {}}],
                }],
            }],
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))

        result = main(["run", "-c", str(path), "-C", "ds=2024-01-01"])
        assert result == 0

    def test_run_bad_context_format(self, tmp_path):
        config = {
            "owner": "t", "bu": "b",
            "system_table": str(tmp_path / "h.db"),
            "system_table_backend": "sqlite",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))

        result = main(["run", "-c", str(path), "-C", "invalid_no_equals"])
        assert result == 1

    def test_report_generates_html(self, tmp_path):
        config = {
            "owner": "t", "bu": "b",
            "system_table": str(tmp_path / "qf.db"),
            "system_table_backend": "sqlite",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))
        output = tmp_path / "health.html"

        result = main(["report", "-c", str(path), "-o", str(output), "--days", "7"])
        assert result == 0
        assert output.exists()

    def test_report_fails_fast_on_external_catalog_without_spark(self, tmp_path, monkeypatch, capsys):
        """Without PySpark, report on a Spark-only system_table_backend must fail with a clear message."""
        # Force the pyspark import inside _cmd_report to fail.
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pyspark.sql" or name.startswith("pyspark"):
                raise ImportError("simulated missing pyspark")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        config = {
            "owner": "t", "bu": "b",
            "system_table": "qf.history",
            "system_table_backend": "external_catalog",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))

        result = main(["report", "-c", str(path), "--days", "7"])
        assert result == 1
        err = capsys.readouterr().err
        assert "PySpark" in err
        assert "external_catalog" in err
        assert "sqlite" in err
