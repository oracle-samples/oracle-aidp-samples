"""Tests for Qualifire API: run_config, write_audit_publish, _build_notifiers."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
import yaml

from tests.conftest import MockDataFrame
from qualifire.api import Qualifire
from qualifire.core.exceptions import QualifireValidationError
from qualifire.core.models import Severity


def _make_backend(agg_return=None):
    backend = MagicMock()
    backend.spark = MagicMock()
    backend.execute_sql.return_value = MockDataFrame([{"cnt": 500}])
    backend.get_aggregations.return_value = agg_return or {"cnt": 500}
    return backend


class TestRunConfig:
    def test_run_config_pass(self, tmp_path):
        config = {
            "owner": "team",
            "bu": "bu",
            "system_table": str(tmp_path / "history.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "ds",
                "table": "t",
                "validations": [{
                    "type": "threshold",
                    "collection": {
                        "type": "aggregation",
                        "expressions": {"cnt": "COUNT(*)"},
                    },
                    "rules": [{"metric": "cnt", "thresholds": {"error": {"min": 10}}}],
                }],
            }],
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))

        qf = Qualifire(
            backend=_make_backend(),
            system_table=str(tmp_path / "history.db"),
            system_table_backend="sqlite",
            owner="team", bu="bu",
        )
        result = qf.run_config(str(path))
        assert result.overall_severity == Severity.PASS

    def test_run_config_error(self, tmp_path):
        config = {
            "owner": "team",
            "bu": "bu",
            "system_table": str(tmp_path / "history.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "ds",
                "table": "t",
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
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(config))

        qf = Qualifire(
            backend=_make_backend(),
            system_table=str(tmp_path / "history.db"),
            system_table_backend="sqlite",
            owner="team", bu="bu",
        )
        with pytest.raises(QualifireValidationError):
            qf.run_config(str(path))

    def test_run_config_with_context(self, tmp_path):
        config = {
            "owner": "team",
            "bu": "bu",
            "system_table": str(tmp_path / "history.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "ds",
                "table": "t",
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

        qf = Qualifire(
            backend=_make_backend(),
            system_table=str(tmp_path / "history.db"),
            system_table_backend="sqlite",
            owner="team", bu="bu",
        )
        result = qf.run_config(str(path), context={"ds": "2024-01-01"})
        assert result.overall_severity == Severity.PASS


class TestWriteAuditPublish:
    def test_wap_sql_driven_pass(self):
        backend = _make_backend()
        qf = Qualifire(backend=backend, owner="o", bu="b")
        result = qf.write_audit_publish(
            target_table="prod.table",
            sql="SELECT * FROM raw.table",
            validations=[
                Qualifire.threshold_check(
                    aggregations={"cnt": "COUNT(*)"},
                    rules=[{"metric": "cnt", "thresholds": {}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS

    def test_wap_df_driven_pass(self):
        backend = _make_backend()
        mock_df = MagicMock()
        qf = Qualifire(backend=backend, owner="o", bu="b")
        result = qf.write_audit_publish(
            target_table="prod.table",
            df=mock_df,
            validations=[
                Qualifire.threshold_check(
                    aggregations={"cnt": "COUNT(*)"},
                    rules=[{"metric": "cnt", "thresholds": {}}],
                ),
            ],
        )
        assert result.overall_severity == Severity.PASS
        # Verify df was written to staging
        backend.write_df.assert_called_once()

    def test_wap_df_driven_rollback_on_error(self):
        backend = _make_backend(agg_return={"cnt": 0})
        mock_df = MagicMock()
        qf = Qualifire(backend=backend, owner="o", bu="b")
        with pytest.raises(QualifireValidationError):
            qf.write_audit_publish(
                target_table="prod.table",
                df=mock_df,
                validations=[
                    Qualifire.threshold_check(
                        aggregations={"cnt": "COUNT(*)"},
                        rules=[{"metric": "cnt", "thresholds": {"error": {"min": 100}}}],
                    ),
                ],
            )
        # staging should be dropped (rollback)
        backend.drop_table.assert_called()


class TestBuildNotifiers:
    def test_builds_from_config(self):
        from qualifire.core.config import SlackNotificationConfig

        backend = _make_backend()
        qf = Qualifire(backend=backend, owner="o", bu="b")
        notifiers = qf._build_notifiers({
            "slack": SlackNotificationConfig(webhook_url="https://hooks.slack.com/test"),
        })
        assert "slack" in notifiers
        assert notifiers["slack"].channel_name == "slack"


class TestInitStorageExternal:
    def test_external_catalog_init(self):
        backend = MagicMock()
        backend.spark = MagicMock()
        qf = Qualifire(
            backend=backend,
            system_table="cat.schema.history",
            system_table_backend="external_catalog",
            owner="o", bu="b",
        )
        assert qf._storage is not None

    def test_delta_init(self):
        backend = MagicMock()
        backend.spark = MagicMock()
        qf = Qualifire(
            backend=backend,
            system_table="cat.schema.history",
            system_table_backend="delta",
            owner="o", bu="b",
        )
        assert qf._storage is not None
