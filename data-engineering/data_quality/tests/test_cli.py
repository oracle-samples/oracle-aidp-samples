"""Tests for CLI entry point."""

import pytest

from qualifire.cli import main


class TestCLI:
    def test_no_command_returns_1(self):
        assert main([]) == 1

    def test_validate_config_valid(self, tmp_path):
        import yaml

        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "valid.yaml"
        path.write_text(yaml.dump(config))

        assert main(["validate-config", "-c", str(path)]) == 0

    def test_validate_config_invalid(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("owner: team\n")  # missing required fields

        assert main(["validate-config", "-c", str(path)]) == 1

    def test_validate_config_missing_file(self):
        assert main(["validate-config", "-c", "/nonexistent.yaml"]) == 1

    def test_run_missing_config(self):
        # run requires --config
        with pytest.raises(SystemExit):
            main(["run"])


class TestCLIReportJDBCRequiresConnectionBlock:
    """Phase 2 P2.1: ``jdbc`` is now a wired backend, but the operator
    still has to supply connection settings. ``load_config()`` must
    fail at load time if ``system_table_backend: jdbc`` is set without
    a top-level ``jdbc:`` block, and the programmatic API must raise
    with the same migration pointer.
    """

    def _write_jdbc_config(self, tmp_path, include_jdbc_block=False):
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "qf_history",
            "system_table_backend": "jdbc",
            "datasets": [{"name": "d", "table": "t"}],
        }
        if include_jdbc_block:
            config["jdbc"] = {"url": "jdbc:postgresql://localhost:5432/qf"}
        path = tmp_path / "jdbc.yaml"
        path.write_text(yaml.dump(config))
        return path

    def test_report_rejects_jdbc_without_connection_block(self, tmp_path, capsys):
        path = self._write_jdbc_config(tmp_path)
        rc = main(["report", "-c", str(path), "-o", str(tmp_path / "out.html")])
        assert rc == 1
        err = capsys.readouterr().err
        assert "jdbc" in err.lower()
        # Must name the remediation (add a jdbc: block) rather than
        # leaving the operator to guess.
        assert "jdbc:" in err or "connection" in err.lower() or "url" in err.lower()

    def test_qualifire_init_rejects_jdbc_without_connection(self):
        """Belt-and-braces: any direct API caller hits the same gate."""
        from qualifire.api import Qualifire

        # Any non-None backend-like object; _init_storage runs before
        # it's used because system_table is truthy.
        class _NoopBackend:
            spark = None
        with pytest.raises(ValueError, match="jdbc.*connection|connection.*jdbc"):
            Qualifire(
                backend=_NoopBackend(),
                system_table="qf_history",
                system_table_backend="jdbc",
                owner="o", bu="b",
            )

    def test_validate_config_accepts_jdbc_with_connection_block(self, tmp_path, capsys):
        """Happy path: with a ``jdbc:`` block, ``validate-config`` passes.

        Runtime (``run``/``report``) still needs a JDBC driver + Spark
        at execution time — that's the storage layer's problem, not
        the config layer's.
        """
        path = self._write_jdbc_config(tmp_path, include_jdbc_block=True)
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 0

    def test_qualifire_init_wires_jdbc_storage_when_configured(self):
        """Happy path: with a ``JDBCConfig``, ``_init_storage`` routes
        through ``JDBCStorage`` rather than the old "not supported"
        ValueError."""
        from unittest.mock import MagicMock

        from pyspark.sql.types import DoubleType, StringType

        from qualifire.api import Qualifire
        from qualifire.core.config import JDBCConfig
        from qualifire.storage.base import COLUMN_DEFINITIONS, SYSTEM_TABLE_COLUMNS
        from qualifire.storage.jdbc_storage import JDBCStorage
        from tests.conftest import MockDataFrame

        # Simulate an existing, fully-schema-compatible JDBC table so
        # ``initialize()`` (ping + existence check + schema validation
        # of both names AND types) completes without raising. The
        # round-6 type check inspects ``field.dataType``, so we hand
        # it real Spark DataType instances instead of MagicMocks.
        schema_fields = []
        for col in SYSTEM_TABLE_COLUMNS:
            f = MagicMock()
            f.name = col
            f.dataType = (
                DoubleType() if COLUMN_DEFINITIONS[col][0] == "DOUBLE"
                else StringType()
            )
            schema_fields.append(f)

        ping_df = MockDataFrame([{"ping": 1}])
        existing_df = MockDataFrame(schema_fields=schema_fields)
        call_counter = {"n": 0}

        def read_side_effect(**kwargs):
            call_counter["n"] += 1
            return ping_df if call_counter["n"] == 1 else existing_df

        class _FakeSparkBackend:
            spark = MagicMock()

        backend = _FakeSparkBackend()
        backend.spark.read.jdbc.side_effect = read_side_effect

        qf = Qualifire(
            backend=backend,
            system_table="qf_history",
            system_table_backend="jdbc",
            jdbc=JDBCConfig(
                url="jdbc:postgresql://localhost:5432/qf",
                user="u",
                password="p",
            ),
            owner="o", bu="b",
        )
        assert isinstance(qf._storage, JDBCStorage)
        assert qf._storage._jdbc_url == "jdbc:postgresql://localhost:5432/qf"
        # Credentials normalized into the connection-properties dict so
        # Spark's JDBC writer picks them up without additional plumbing.
        assert qf._storage._props["user"] == "u"
        assert qf._storage._props["password"] == "p"

    def test_validate_config_accepts_templated_jdbc_block_on_non_jdbc_backend(
        self, tmp_path, capsys
    ):
        """Round-7 review: a config that templates in a ``jdbc:`` block
        with ``user``/``password`` but no ``url`` (deployment writes the
        URL per-environment) must NOT fail validation when the active
        backend isn't ``jdbc``. Previously ``JDBCConfig.url`` was
        declared ``str`` (required), so Pydantic rejected the stray
        block at field validation time — before the top-level
        ``_require_jdbc_when_needed`` model validator had a chance to
        tolerate it. ``url`` is now optional at the model level, and
        the model validator enforces presence only when the backend
        actually needs it."""
        import yaml

        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            # Deliberately NOT jdbc — the jdbc: block is dormant here
            # (e.g. a shared YAML template that turns into a real JDBC
            # config only on the prod deployment that sets both
            # system_table_backend: jdbc and jdbc.url).
            "system_table_backend": "external_catalog",
            "jdbc": {"user": "qf_user", "password": "${DB_PASSWORD}"},
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "templated_jdbc.yaml"
        path.write_text(yaml.dump(config))
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 0, capsys.readouterr().err

    def test_validate_config_rejects_invalid_step_in_historical_rule(
        self, tmp_path, capsys
    ):
        """Round-10 review: ``step`` on historical / forecast /
        anomaly-sample configs used to be a plain ``str | None`` with
        no validator, so an operator typo like ``step: 7dy`` passed
        ``validate-config`` silently and only crashed mid-run when
        the validator called ``parse_step_seconds()``. The round-10
        field validator wires ``parse_step_seconds`` into Pydantic,
        turning the failure into a preflight config error with the
        original grammar hint."""
        import yaml

        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [
                {
                    "name": "d",
                    "table": "t",
                    "validations": [
                        {
                            "name": "v1",
                            "kind": "historical",
                            "aggregations": {"avg": "AVG(x)"},
                            "rules": [
                                {
                                    "metric": "avg",
                                    "compare": {
                                        "past_values": 3,
                                        "step": "7dy",  # typo
                                    },
                                    "thresholds": {
                                        "warning": {"deviation_pct": {"min": -10, "max": 10}},
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        path = tmp_path / "bad_step.yaml"
        path.write_text(yaml.dump(config))
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err + capsys.readouterr().out
        # The exact duration hint from ``parse_step_seconds``'s
        # underlying grammar error must surface so operators can
        # self-fix from the preflight message alone.
        assert "Invalid duration format" in err or "7dy" in err

    def test_validate_config_rejects_invalid_step_in_forecast_model(
        self, tmp_path, capsys
    ):
        """Round-10 review companion: same preflight wiring for
        forecast rules. The trend validator's ``model.step`` default
        is ``"P1D"`` on main; a bogus override must fail during
        ``validate-config`` rather than during the Prophet training
        call."""
        import yaml

        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [
                {
                    "name": "d",
                    "table": "t",
                    "validations": [
                        {
                            "name": "v1",
                            "kind": "trend",
                            "aggregations": {"avg": "AVG(x)"},
                            "rules": [
                                {
                                    "metric": "avg",
                                    "model": {
                                        "history_count": 60,
                                        "step": "bogus",
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        path = tmp_path / "bad_forecast_step.yaml"
        path.write_text(yaml.dump(config))
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1

    def test_qualifire_init_rejects_jdbc_config_without_url(self):
        """Round-8 review: with JDBCConfig.url now Optional (to let a
        templated jdbc: block coexist with a non-jdbc backend), a
        programmatic caller that constructs ``JDBCConfig(user='u')``
        and pairs it with ``system_table_backend='jdbc'`` used to sneak
        past ``_init_storage``'s ``is None`` check and crash deep
        inside ``JDBCStorage._connectivity_probe_sql`` with an
        ``AttributeError`` on ``.lower()`` over the missing URL.
        The API path must short-circuit with the same migration text
        the YAML path already surfaces."""
        from qualifire.api import Qualifire
        from qualifire.core.config import JDBCConfig

        class _FakeSparkBackend:
            from unittest.mock import MagicMock as _M
            spark = _M()

        with pytest.raises(ValueError, match="requires a jdbc connection .*url"):
            Qualifire(
                backend=_FakeSparkBackend(),
                system_table="qf_history",
                system_table_backend="jdbc",
                jdbc=JDBCConfig(user="u", password="p"),  # no url
                owner="o", bu="b",
            )

    def test_qualifire_init_rejects_jdbc_with_non_spark_backend(self):
        """Round-5 review: the ``jdbc`` branch in ``_init_storage``
        previously dereferenced ``self.backend.spark`` unconditionally.
        ``PandasBackend`` has no such attribute, so a programmatic
        caller combining Pandas with JDBC got a bare
        ``AttributeError`` at construction — an opaque runtime crash
        instead of a documented unsupported-backend error. Pin the
        explicit ValueError so the failure mode stays predictable."""
        from qualifire.api import Qualifire
        from qualifire.core.config import JDBCConfig

        class _NoSparkBackend:
            """Backend without a ``.spark`` attribute (e.g. Pandas)."""

        with pytest.raises(ValueError, match="requires.*SparkBackend|SparkSession"):
            Qualifire(
                backend=_NoSparkBackend(),
                system_table="qf_history",
                system_table_backend="jdbc",
                jdbc=JDBCConfig(
                    url="jdbc:postgresql://ok/db", user="u", password="p"
                ),
                owner="o", bu="b",
            )


class TestCLIConfigErrorHandling:
    """Round-9 regression: config validators added in this branch
    (duplicate dataset names, custom_query.filter rejection, missing
    source, etc.) must surface through ``run`` and ``report`` as a
    clean exit-1 with a single-line message on stderr — NOT as a raw
    Python traceback.

    Before this fix, ``_cmd_run`` and ``_cmd_report`` called
    ``load_config()`` outside any ``QualifireConfigError`` handler,
    so a rejected config crashed the CLI with a traceback, which
    makes rollout/recovery materially harder in automation (log
    aggregators can't key on "exit 1 + remediation message").
    """

    def _write_duplicate_dataset_config(self, tmp_path):
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [
                {"name": "orders", "table": "t1"},
                {"name": "orders", "table": "t2"},  # duplicate name
            ],
        }
        path = tmp_path / "dup.yaml"
        path.write_text(yaml.dump(config))
        return path

    def test_run_exits_cleanly_on_duplicate_dataset_config(self, tmp_path, capsys):
        path = self._write_duplicate_dataset_config(tmp_path)
        rc = main(["run", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        # Single-line remediation, not a traceback.
        assert "Config validation failed" in err
        assert "Traceback" not in err
        # Error must name the offending dataset so the operator can
        # locate it.
        assert "orders" in err

    def test_report_exits_cleanly_on_duplicate_dataset_config(self, tmp_path, capsys):
        path = self._write_duplicate_dataset_config(tmp_path)
        rc = main(["report", "-c", str(path), "-o", str(tmp_path / "out.html")])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "Traceback" not in err

class TestCLIBackendConfigValidation:
    """Round-10 regression: ``system_table_backend`` is constrained at
    config-load time, so ``validate-config`` / ``run`` / ``report`` all
    reject typos and the unwired ``jdbc`` value uniformly.

    Before this fix, ``QualifireConfig.system_table_backend`` was an
    unconstrained ``str``. ``validate-config`` happily accepted
    ``system_table_backend: bogus``, and the failure only surfaced
    later — sometimes as the misleading "PySpark required" branch in
    ``_cmd_report``, sometimes as a ``ValueError`` constructor
    traceback in ``_cmd_run``. That broke the contract that a green
    ``validate-config`` means the config is safe to hand to ``run``.
    """

    def _write_backend_config(self, tmp_path, backend_value):
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "system_table_backend": backend_value,
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / f"backend_{backend_value}.yaml"
        path.write_text(yaml.dump(config))
        return path

    def test_validate_config_rejects_bogus_backend(self, tmp_path, capsys):
        path = self._write_backend_config(tmp_path, "bogus")
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        # Pydantic's Literal error enumerates the allowed values — that
        # IS the remediation pointer for a typo.
        assert "bogus" in err or "system_table_backend" in err
        assert "Traceback" not in err

    def test_run_rejects_bogus_backend(self, tmp_path, capsys):
        """Must NOT fall through to the 'PySpark required' branch — the
        remediation would be wrong."""
        path = self._write_backend_config(tmp_path, "bogus")
        rc = main(["run", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "PySpark is required" not in err
        assert "Traceback" not in err

    def test_report_rejects_bogus_backend(self, tmp_path, capsys):
        """Must NOT fall through to the 'PySpark required for ...' branch
        in ``_cmd_report`` — that message uses the backend string
        verbatim and would misdirect the operator to install PySpark."""
        path = self._write_backend_config(tmp_path, "bogus")
        rc = main(["report", "-c", str(path), "-o", str(tmp_path / "out.html")])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "requires PySpark for system_table_backend='bogus'" not in err
        assert "Traceback" not in err

    def test_validate_config_rejects_jdbc_without_connection_block(self, tmp_path, capsys):
        """Phase 2 P2.1: the JDBC gate now requires a ``jdbc:`` block.
        Missing the block must fail at config-load so preflight catches
        it, not deep inside the ``report`` pipeline or constructor."""
        path = self._write_backend_config(tmp_path, "jdbc")
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "jdbc" in err.lower()
        # Message must point at the missing ``jdbc:`` block so the
        # operator knows what to add.
        assert "jdbc:" in err or "url" in err.lower()

    def test_run_rejects_jdbc_without_connection_block(self, tmp_path, capsys):
        """Previously ``run`` with a JDBC config crashed with a
        constructor traceback on Spark-enabled machines; now it must
        fail cleanly at load with a remediation pointer."""
        path = self._write_backend_config(tmp_path, "jdbc")
        rc = main(["run", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "jdbc" in err.lower()
        assert "Traceback" not in err


class TestCLIDfFromYamlRejected:
    """Round-10 regression: a ``df:`` key inside a file-loaded dataset
    is always wrong — YAML/JSON cannot carry an in-process DataFrame —
    and must be rejected by ``load_config()`` so ``validate-config``
    catches it during preflight, not by ``_materialize_df`` as an
    opaque type error at runtime.
    """

    def _write_df_config(self, tmp_path, df_value):
        """Write a YAML config whose only dataset carries a ``df`` key.

        Note the dataset omits ``table``/``query``/``wap`` — before the
        new gate, ``df: []`` or ``df: {}`` still satisfied
        ``_require_source`` because ``df is not None``, so the config
        passed validation and failed only at runtime.
        """
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [{"name": "orders", "df": df_value}],
        }
        path = tmp_path / "df.yaml"
        path.write_text(yaml.dump(config))
        return path

    def test_validate_config_rejects_df_list(self, tmp_path, capsys):
        path = self._write_df_config(tmp_path, [])
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        # Error must name the offending dataset AND the offending field.
        assert "orders" in err
        assert "'df'" in err or "df " in err
        # Must point the operator at the correct fix: programmatic API
        # or table/query/wap for file configs.
        assert "programmatic" in err.lower() or "table" in err.lower()
        assert "Traceback" not in err

    def test_validate_config_rejects_df_dict(self, tmp_path, capsys):
        path = self._write_df_config(tmp_path, {})
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "orders" in err

    def test_validate_config_rejects_df_string(self, tmp_path, capsys):
        """Non-null scalar payloads (strings) are also rejected —
        ``df: "some/path.parquet"`` is a common templating mistake
        where the operator conflated a file pointer with a live
        DataFrame."""
        path = self._write_df_config(tmp_path, "some/path.parquet")
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "orders" in err

    def test_run_rejects_df_in_yaml(self, tmp_path, capsys):
        path = self._write_df_config(tmp_path, [])
        rc = main(["run", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "Traceback" not in err


class TestCLIDfNullToleratedInYaml:
    """Round-11 regression: ``df: null`` must load silently.

    Round-10 hard-failed any ``df`` key in a file config, but
    ``main`` ignored ``df`` entirely (it was an extra field).
    That upgrade path is a startup-breaker for any deployment with:
      - a YAML emitter that writes ``null`` for absent fields;
      - copy-paste configs that leave a ``df:`` key next to a real
        ``table:`` source;
      - any template that renders ``df: {{ maybe_df or "" }}``.
    Codex round 11 flagged the hard-fail as an unnecessary rollout
    hazard — null values are inert by construction and cannot be
    confused with a live DataFrame. Current behavior: strip the key
    silently, exactly as main did, and keep the hard-fail for
    non-null payloads (the actual correctness hazard).
    """

    def test_df_null_loads_silently_next_to_table(self, tmp_path, capsys):
        """``table: t`` + ``df: null`` loads with no error and no warning."""
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [{"name": "orders", "table": "t", "df": None}],
        }
        path = tmp_path / "df_null.yaml"
        path.write_text(yaml.dump(config))

        rc = main(["validate-config", "-c", str(path)])
        assert rc == 0
        out = capsys.readouterr()
        assert "Config validation failed" not in out.err
        assert "Traceback" not in out.err

    def test_df_null_alone_fails_on_missing_source_not_on_df(self, tmp_path, capsys):
        """With ``df: null`` stripped and no ``table``/``query``/``wap``
        left, the dataset legitimately has no source. The error must
        surface as "missing source", NOT as the round-10 "df cannot
        be set from a config file" — the latter message would be
        misleading, since the root cause after the null-strip is a
        missing source, not a forbidden one."""
        import yaml
        config = {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [{"name": "orders", "df": None}],
        }
        path = tmp_path / "df_null_only.yaml"
        path.write_text(yaml.dump(config))

        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        # The round-10 message must NOT fire for null values.
        assert "cannot be set from a config file" not in err
        # The real error: no source on the dataset.
        assert "table" in err.lower() or "source" in err.lower()


class TestCLIRunNoDoubleFileRead:
    """Round-11 regression: ``qualifire run`` must not re-read the
    config file after the preflight validator. Before this fix,
    ``_cmd_run`` called ``load_config()`` for preflight then invoked
    ``qf.run_config(args.config)``, which ``load_config()``'d the
    same path again. Between the two reads:

      1. A file-swap racer could report "validation passed" for
         snapshot A and execute snapshot B.
      2. ``Qualifire.run_config`` only reinitialized storage when
         ``system_table`` changed — a backend-only swap in snapshot
         B (e.g. ``sqlite`` → ``delta`` at the same path) reused
         the stale storage wiring silently.

    Mitigations: CLI calls ``run_config_parsed(config)`` with the
    already-parsed object, and ``run_config_parsed`` now reinitializes
    storage on either ``system_table`` or ``system_table_backend``
    change.
    """

    def test_cmd_run_does_not_read_config_file_twice(self, tmp_path, monkeypatch):
        """Count ``load_config`` invocations under ``_cmd_run``.

        Exactly one file read is permitted: the preflight in
        ``_cmd_run``. A second read is the TOCTOU hole.
        """
        import yaml
        from qualifire.core import config as config_module
        from qualifire import cli as cli_module

        cfg = {
            "owner": "team",
            "bu": "finance",
            "system_table": str(tmp_path / "hist.db"),
            "system_table_backend": "sqlite",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "cfg.yaml"
        path.write_text(yaml.dump(cfg))

        calls = {"load_config": 0}
        real_load_config = config_module.load_config

        def counting_load_config(p):
            calls["load_config"] += 1
            return real_load_config(p)

        # Patch both the definition site and the CLI's import-time
        # binding; argparse calls cli.load_config, not
        # qualifire.core.config.load_config, via the `from` import.
        monkeypatch.setattr(config_module, "load_config", counting_load_config)
        monkeypatch.setattr(cli_module, "load_config", counting_load_config)

        # Stub out SparkSession + Qualifire so the test runs without
        # PySpark. The Qualifire stub's run_config_parsed asserts it
        # received a QualifireConfig object (not a path str) — this is
        # the core TOCTOU guarantee.
        from qualifire.core.config import QualifireConfig
        from qualifire.core.models import QualifireResult

        class _FakeSpark:
            @staticmethod
            def getOrCreate():
                return _FakeSpark()
        class _FakeBuilder:
            def getOrCreate(self):
                return _FakeSpark()

        class _FakeSparkSession:
            builder = _FakeBuilder()

        class _FakeSparkBackend:
            def __init__(self, spark):
                self.spark = spark

        captured = {}

        class _FakeQualifire:
            def __init__(self, **kwargs):
                captured["init"] = kwargs
            @classmethod
            def _from_parsed_config(cls, cfg, **kwargs):
                assert isinstance(cfg, QualifireConfig), (
                    "CLI must pass a parsed QualifireConfig to "
                    "_from_parsed_config, not a path string"
                )
                captured["from_parsed_config_kwargs"] = kwargs
                inst = cls(**kwargs)
                inst._cfg = cfg
                return inst
            def run_config_parsed(
                self, cfg, context=None, *,
                skip_recollection=False,
                skip_renotification=False,
                skip_revalidation=False,
            ):
                assert isinstance(cfg, QualifireConfig), (
                    "CLI must pass a parsed QualifireConfig, not a path"
                )
                captured["parsed_cfg"] = cfg
                captured["skip_recollection"] = skip_recollection
                captured["skip_renotification"] = skip_renotification
                captured["skip_revalidation"] = skip_revalidation
                return QualifireResult(owner="o", bu="b", datasets=[])

        # The CLI does a deferred import inside _cmd_run, so patch the
        # module-level attributes before main() runs.
        import qualifire.api as api_module
        import qualifire.backends.spark_backend as spark_backend_module

        # Stub pyspark.sql.SparkSession too — cli imports it inside the
        # function.
        fake_pyspark_sql = type("M", (), {"SparkSession": _FakeSparkSession})
        monkeypatch.setitem(
            __import__("sys").modules, "pyspark", type("P", (), {"sql": fake_pyspark_sql})
        )
        monkeypatch.setitem(
            __import__("sys").modules, "pyspark.sql", fake_pyspark_sql
        )
        monkeypatch.setattr(api_module, "Qualifire", _FakeQualifire)
        monkeypatch.setattr(spark_backend_module, "SparkBackend", _FakeSparkBackend)

        rc = cli_module.main(["run", "-c", str(path)])
        assert rc == 0, "fake run should succeed"
        assert calls["load_config"] == 1, (
            f"config file read {calls['load_config']} times; "
            "exactly one preflight read is permitted (TOCTOU guard)"
        )
        assert "parsed_cfg" in captured

    def test_cmd_report_does_not_read_config_file_twice(
        self, tmp_path, monkeypatch,
    ):
        """Codex impl-R1 SHOULD-FIX 3 lock-in. ``qualifire report``
        must also load the config exactly once. The CLI calls
        ``Qualifire._from_parsed_config`` to avoid the second
        ``load_config`` call that would happen if construction
        re-read the path."""
        import yaml as _yaml
        from qualifire.core import config as config_module
        from qualifire import cli as cli_module
        from qualifire.core.config import QualifireConfig

        cfg = {
            "owner": "team", "bu": "fin",
            "system_table": str(tmp_path / "hist.db"),
            "system_table_backend": "sqlite",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "cfg.yaml"
        path.write_text(_yaml.dump(cfg))

        calls = {"load_config": 0}
        real = config_module.load_config

        def counting(p):
            calls["load_config"] += 1
            return real(p)

        monkeypatch.setattr(config_module, "load_config", counting)
        monkeypatch.setattr(cli_module, "load_config", counting)

        class _FakeQualifire:
            def __init__(self, **kwargs):
                pass
            @classmethod
            def _from_parsed_config(cls, cfg, **kwargs):
                assert isinstance(cfg, QualifireConfig)
                return cls(**kwargs)
            def health_report(self, **kwargs):
                from qualifire.core.report import HealthReport
                return HealthReport(
                    days=30, total_checks=0, pass_count=0,
                    warning_count=0, error_count=0, pass_rate=0.0,
                    warning_rate=0.0, error_rate=0.0,
                    by_check_type=[], worst_offenders=[],
                )

        # Stub PySpark + Qualifire
        class _FakeSpark:
            @staticmethod
            def getOrCreate():
                return _FakeSpark()
        class _FakeBuilder:
            def getOrCreate(self):
                return _FakeSpark()
        class _FakeSparkSession:
            builder = _FakeBuilder()
        class _FakeSparkBackend:
            def __init__(self, spark):
                self.spark = spark

        fake_pyspark_sql = type(
            "M", (), {"SparkSession": _FakeSparkSession},
        )
        monkeypatch.setitem(
            __import__("sys").modules, "pyspark",
            type("P", (), {"sql": fake_pyspark_sql}),
        )
        monkeypatch.setitem(
            __import__("sys").modules, "pyspark.sql", fake_pyspark_sql,
        )
        import qualifire.api as api_module
        import qualifire.backends.spark_backend as spark_backend_module
        monkeypatch.setattr(api_module, "Qualifire", _FakeQualifire)
        monkeypatch.setattr(
            spark_backend_module, "SparkBackend", _FakeSparkBackend,
        )

        rc = cli_module.main([
            "report", "-c", str(path),
            "-o", str(tmp_path / "out.html"),
        ])
        # rc may be non-zero (HealthReport stubbing is shallow); we
        # only assert the load_config count.
        assert calls["load_config"] == 1, (
            f"qualifire report load_config call_count="
            f"{calls['load_config']}; expected exactly 1 (TOCTOU)."
        )

    def test_cmd_backfill_does_not_read_config_file_twice(
        self, tmp_path, monkeypatch,
    ):
        """Codex impl-R1 SHOULD-FIX 3 lock-in for ``qualifire
        backfill``. The CLI loads once for preflight then routes
        through ``_open_qualifire_for_op`` →
        ``Qualifire._from_parsed_config``."""
        import yaml as _yaml
        from qualifire.core import config as config_module
        from qualifire import cli as cli_module
        from qualifire.core.config import QualifireConfig

        cfg = {
            "owner": "team", "bu": "fin",
            "system_table": str(tmp_path / "hist.db"),
            "system_table_backend": "sqlite",
            "datasets": [{
                "name": "d", "table": "t",
                "partition_ts": "event_date",
            }],
            "partition_step": "P1D",
        }
        path = tmp_path / "cfg.yaml"
        path.write_text(_yaml.dump(cfg))

        calls = {"load_config": 0}
        real = config_module.load_config

        def counting(p):
            calls["load_config"] += 1
            return real(p)

        monkeypatch.setattr(config_module, "load_config", counting)
        monkeypatch.setattr(cli_module, "load_config", counting)

        class _FakeQualifire:
            def __init__(self, **kwargs):
                pass
            @classmethod
            def _from_parsed_config(cls, cfg, **kwargs):
                assert isinstance(cfg, QualifireConfig)
                return cls(**kwargs)
            def backfill(self, config_or_path, *args, **kwargs):
                # Codex impl-R2 SHOULD-FIX 1: assert the CLI hands a
                # parsed config (NOT a string path) to backfill, so
                # a regression that bypasses the preflight parse and
                # passes ``args.config`` would be caught even though
                # the fake doesn't itself re-load.
                assert isinstance(config_or_path, QualifireConfig), (
                    f"qualifire backfill must receive a parsed "
                    f"QualifireConfig, got {type(config_or_path).__name__}"
                )
                from qualifire.core.backfill_report import BackfillReport
                return BackfillReport()

        # Force the no-PySpark sqlite path (RuntimeError caught
        # cleanly by _open_qualifire_for_op).
        import sys
        monkeypatch.setitem(sys.modules, "pyspark", None)

        import qualifire.api as api_module
        monkeypatch.setattr(api_module, "Qualifire", _FakeQualifire)

        rc = cli_module.main([
            "backfill", "-c", str(path),
            "--partition", "2026-01-01",
        ])
        assert calls["load_config"] == 1, (
            f"qualifire backfill load_config call_count="
            f"{calls['load_config']}; expected exactly 1 (TOCTOU)."
        )

    def test_cmd_deactivate_metric_does_not_read_config_file_twice(
        self, tmp_path, monkeypatch,
    ):
        """Codex impl-R1 SHOULD-FIX 3 lock-in for ``qualifire
        deactivate-metric``. Same TOCTOU guarantee."""
        import yaml as _yaml
        from qualifire.core import config as config_module
        from qualifire import cli as cli_module
        from qualifire.core.config import QualifireConfig

        cfg = {
            "owner": "team", "bu": "fin",
            "system_table": str(tmp_path / "hist.db"),
            "system_table_backend": "sqlite",
            "datasets": [{"name": "d", "table": "t"}],
        }
        path = tmp_path / "cfg.yaml"
        path.write_text(_yaml.dump(cfg))

        calls = {"load_config": 0}
        real = config_module.load_config

        def counting(p):
            calls["load_config"] += 1
            return real(p)

        monkeypatch.setattr(config_module, "load_config", counting)
        monkeypatch.setattr(cli_module, "load_config", counting)

        class _FakeQualifire:
            def __init__(self, **kwargs):
                pass
            @classmethod
            def _from_parsed_config(cls, cfg, **kwargs):
                assert isinstance(cfg, QualifireConfig)
                return cls(**kwargs)
            def deactivate_metric(self, *args, **kwargs):
                # Codex impl-R2 SHOULD-FIX 1: ensure the CLI passes a
                # parsed config when supplied. ``deactivate_metric``
                # accepts a ``config`` kwarg (or no config to use the
                # instance's own storage); when present it must be a
                # parsed QualifireConfig.
                cfg = kwargs.get("config")
                if cfg is not None:
                    assert isinstance(cfg, QualifireConfig), (
                        f"qualifire deactivate-metric must pass a "
                        f"parsed QualifireConfig, got "
                        f"{type(cfg).__name__}"
                    )
                return 1

        import sys
        monkeypatch.setitem(sys.modules, "pyspark", None)

        import qualifire.api as api_module
        monkeypatch.setattr(api_module, "Qualifire", _FakeQualifire)

        rc = cli_module.main([
            "deactivate-metric", "-c", str(path),
            "--dataset", "d", "--metric", "m",
        ])
        assert calls["load_config"] == 1, (
            f"qualifire deactivate-metric load_config call_count="
            f"{calls['load_config']}; expected exactly 1 (TOCTOU)."
        )


class TestRunConfigParsedStorageReinit:
    """Round-11 regression: ``run_config_parsed`` must reinitialize
    storage when EITHER ``system_table`` or ``system_table_backend``
    changes, not just on ``system_table`` change.
    """

    def test_backend_change_triggers_storage_reinit(self, tmp_path, monkeypatch):
        """Qualifire instance created with backend=sqlite; run with a
        config that swaps to external_catalog at the same system_table
        path. The storage backend must be rebuilt, not reused."""
        from qualifire.api import Qualifire
        from qualifire.core.config import DatasetConfig, QualifireConfig
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.spark = MagicMock()

        qf = Qualifire(
            backend=backend,
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="sqlite",
            owner="o", bu="b",
        )
        first_storage = qf._storage

        # New config with the SAME system_table string but a DIFFERENT
        # backend. If the reinit check keys only on system_table, the
        # backend swap is silent.
        new_config = QualifireConfig(
            owner="o",
            bu="b",
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="external_catalog",
            datasets=[DatasetConfig(name="d", table="t")],
        )

        # Stub _init_storage so we don't need ExternalCatalogStorage's
        # real wiring (Spark catalog etc.) — we just want to assert it
        # was called.
        reinit_calls = []
        def _fake_init_storage(**kwargs):
            mock = MagicMock(name="reinit_storage")
            reinit_calls.append(mock)
            return mock
        monkeypatch.setattr(qf, "_init_storage", _fake_init_storage)

        # Stub out the engine so run_config_parsed returns quickly.
        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.storage = kwargs["storage"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)

        qf.run_config_parsed(new_config)

        assert len(reinit_calls) == 1, (
            "backend-only change must trigger _init_storage; "
            "the round-10 implementation keyed only on system_table "
            "and missed this case"
        )
        assert qf.system_table_backend == "external_catalog"


class TestRunConfigParsedStoragePersistence:
    """Round-12 regression: the reinitialized storage handle must be
    persisted back onto the Qualifire instance.

    Round 11 reinitialized storage into a local variable only; the
    current engine invocation used the new backend, but every
    subsequent API call (``validate``, ``health_report``, a second
    ``run_config_parsed``) silently read ``self._storage`` and
    reverted to the stale handle. That is the classic
    write-to-new / read-from-old split: a migration appears to
    succeed on the first call and then silently corrupts history
    on every call after.
    """

    def _instance_and_stubs(self, tmp_path, monkeypatch):
        from qualifire.api import Qualifire
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.spark = MagicMock()
        qf = Qualifire(
            backend=backend,
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="sqlite",
            owner="o", bu="b",
        )

        reinit_calls = []
        def _fake_init_storage(**kwargs):
            mock = MagicMock(name=f"storage_{len(reinit_calls)}")
            reinit_calls.append(mock)
            return mock
        monkeypatch.setattr(qf, "_init_storage", _fake_init_storage)

        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.storage = kwargs["storage"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)
        return qf, reinit_calls

    def test_reinit_persists_to_self_storage(self, tmp_path, monkeypatch):
        """After a reinit, the new handle lives on the instance."""
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)
        first_storage = qf._storage

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="external_catalog",
            datasets=[DatasetConfig(name="d", table="t")],
        )
        qf.run_config_parsed(new_config)

        assert len(reinit_calls) == 1
        assert qf._storage is reinit_calls[0], (
            "reinitialized storage must be assigned back to self._storage; "
            "round-11 left it only in a local variable"
        )
        assert qf._storage is not first_storage

    def test_second_run_with_same_config_does_not_reinit(self, tmp_path, monkeypatch):
        """Once the instance is swapped to the new backend, a second
        call with the *same* overrides must NOT reinit. Without the
        round-12 persistence fix, the instance's self._storage still
        reflected the old backend, so the comparison
        ``config.system_table_backend != self.system_table_backend``
        still fired → infinite reinit on every call."""
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="external_catalog",
            datasets=[DatasetConfig(name="d", table="t")],
        )
        qf.run_config_parsed(new_config)
        first_reinit_storage = qf._storage
        qf.run_config_parsed(new_config)

        assert len(reinit_calls) == 1, (
            "second call with same overrides must not reinit"
        )
        assert qf._storage is first_reinit_storage

    def test_later_api_call_uses_reinitialized_storage(self, tmp_path, monkeypatch):
        """After run_config_parsed reinits, a follow-up call (here
        simulated by reading self._storage, as validate/health_report
        both do) must see the NEW handle, not the stale one. Makes
        the read/write split explicit."""
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)
        stale_storage = qf._storage

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="delta",
            datasets=[DatasetConfig(name="d", table="t")],
        )
        qf.run_config_parsed(new_config)

        # Simulates the read side of validate()/health_report(), which
        # both go through self._storage.
        storage_used_on_next_call = qf._storage
        assert storage_used_on_next_call is not stale_storage
        assert storage_used_on_next_call is reinit_calls[0]


class TestRunConfigParsedBackendOmitted:
    """Round-12 regression: an omitted ``system_table_backend`` in
    the parsed config must inherit the instance's current backend,
    not silently flip to the Pydantic default (``external_catalog``).

    ``QualifireConfig.system_table_backend`` defaults to
    ``"external_catalog"``, so a raw attribute comparison cannot
    distinguish "caller omitted the field" from "caller explicitly
    chose external_catalog". Before the fix, a programmatic caller
    who built ``Qualifire(..., system_table_backend="sqlite")`` and
    passed a parsed config without the field was silently
    reinitialized to external_catalog — a real compatibility
    regression that can redirect writes to a catalog that may not
    even exist in the environment.

    The fix keys on ``config.model_fields_set``, which Pydantic v2
    populates with exactly the fields the caller supplied.
    """

    def _instance_and_stubs(self, tmp_path, monkeypatch):
        from qualifire.api import Qualifire
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.spark = MagicMock()
        qf = Qualifire(
            backend=backend,
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="sqlite",
            owner="o", bu="b",
        )

        reinit_calls = []
        def _fake_init_storage(**kwargs):
            mock = MagicMock(name=f"storage_{len(reinit_calls)}")
            reinit_calls.append(mock)
            return mock
        monkeypatch.setattr(qf, "_init_storage", _fake_init_storage)

        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.storage = kwargs["storage"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)
        return qf, reinit_calls

    def test_omitted_backend_preserves_instance_setting(self, tmp_path, monkeypatch):
        """Construct a parsed config that omits system_table_backend
        via model_validate (the only path that actually omits it from
        ``model_fields_set``). The instance's backend must survive."""
        from qualifire.core.config import QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)
        initial_storage = qf._storage

        cfg = QualifireConfig.model_validate({
            "owner": "o",
            "bu": "b",
            "system_table": str(tmp_path / "hist.db"),
            # No system_table_backend — simulates a YAML that didn't
            # set it or a programmatic caller building via
            # model_validate with the field absent.
            "datasets": [{"name": "d", "table": "t"}],
        })
        assert "system_table_backend" not in cfg.model_fields_set

        qf.run_config_parsed(cfg)

        assert len(reinit_calls) == 0, (
            "omitted system_table_backend must NOT trigger reinit; "
            "round-11 compared against the Pydantic default and "
            "silently flipped the backend"
        )
        assert qf.system_table_backend == "sqlite"
        assert qf._storage is initial_storage

    def test_table_change_with_omitted_backend_keeps_instance_backend(
        self, tmp_path, monkeypatch
    ):
        """When only the table changes and backend is omitted, reinit
        fires (table change is load-bearing) but the backend stays
        whatever the instance was configured with."""
        from qualifire.core.config import QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)

        cfg = QualifireConfig.model_validate({
            "owner": "o", "bu": "b",
            "system_table": str(tmp_path / "new.db"),  # CHANGED
            # system_table_backend omitted — instance's "sqlite" wins.
            "datasets": [{"name": "d", "table": "t"}],
        })
        qf.run_config_parsed(cfg)

        assert len(reinit_calls) == 1
        assert qf.system_table == str(tmp_path / "new.db")
        assert qf.system_table_backend == "sqlite", (
            "omitted backend must inherit the instance's current "
            "setting, even when the table changed"
        )

    def test_explicit_backend_match_is_a_no_op(self, tmp_path, monkeypatch):
        """Explicitly setting the SAME backend the instance already
        has must NOT trigger reinit — the comparison is on value
        equality after confirming the field was explicit."""
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf, reinit_calls = self._instance_and_stubs(tmp_path, monkeypatch)

        cfg = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "hist.db"),
            system_table_backend="sqlite",  # same as instance
            datasets=[DatasetConfig(name="d", table="t")],
        )
        qf.run_config_parsed(cfg)
        assert len(reinit_calls) == 0


class TestRunConfigParsedReinitFailureRollback:
    """Round-13 regression: a failed ``_init_storage`` must not
    leave the Qualifire instance in a half-swapped state.

    The round-12 implementation mutated ``self.system_table`` and
    ``self.system_table_backend`` *before* calling
    ``_init_storage``. If init raised (bad catalog permissions,
    missing path, transient backend error), the instance was left
    with metadata pointing at the new target but ``self._storage``
    still on the old backend. On the next call, ``table_changed``
    and ``backend_changed`` evaluated False against the
    already-mutated fields, so the retry silently skipped reinit
    and kept writing history to the stale storage — exactly the
    rollback hazard this whole storage-reinit path was meant to
    close.

    The fix keeps candidate table/backend in locals and only
    assigns them (together with ``self._storage``) after
    ``_init_storage`` returns successfully.
    """

    def _instance(self, tmp_path):
        from qualifire.api import Qualifire
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.spark = MagicMock()
        return Qualifire(
            backend=backend,
            system_table=str(tmp_path / "old.db"),
            system_table_backend="sqlite",
            owner="o", bu="b",
        )

    def test_failed_reinit_preserves_instance_state(self, tmp_path, monkeypatch):
        """When ``_init_storage`` raises, ``self.system_table``,
        ``self.system_table_backend``, and ``self._storage`` must
        all still reflect the old backend."""
        import pytest
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf = self._instance(tmp_path)
        old_table = qf.system_table
        old_backend = qf.system_table_backend
        old_storage = qf._storage

        def _boom(**kwargs):
            raise RuntimeError("catalog permission denied")
        monkeypatch.setattr(qf, "_init_storage", _boom)

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "new.db"),
            system_table_backend="external_catalog",
            datasets=[DatasetConfig(name="d", table="t")],
        )

        with pytest.raises(RuntimeError, match="catalog permission denied"):
            qf.run_config_parsed(new_config)

        assert qf.system_table == old_table, (
            "failed reinit must not mutate system_table"
        )
        assert qf.system_table_backend == old_backend, (
            "failed reinit must not mutate system_table_backend"
        )
        assert qf._storage is old_storage, (
            "failed reinit must not mutate _storage"
        )

    def test_retry_after_failure_still_reinits(self, tmp_path, monkeypatch):
        """After a failed reinit, a retry with the same config must
        still trigger a fresh ``_init_storage`` call. The round-12
        bug was that instance fields were pre-mutated, so the retry
        saw ``config == self`` and silently skipped reinit."""
        import pytest
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf = self._instance(tmp_path)

        call_count = {"n": 0}
        from unittest.mock import MagicMock

        def _flaky_init(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("transient catalog init failure")
            return MagicMock(name="reinit_ok")
        monkeypatch.setattr(qf, "_init_storage", _flaky_init)

        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.storage = kwargs["storage"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "new.db"),
            system_table_backend="external_catalog",
            datasets=[DatasetConfig(name="d", table="t")],
        )

        with pytest.raises(RuntimeError, match="transient catalog init failure"):
            qf.run_config_parsed(new_config)
        assert call_count["n"] == 1

        qf.run_config_parsed(new_config)
        assert call_count["n"] == 2, (
            "retry after failed reinit must call _init_storage again; "
            "instead the pre-mutated state made it a silent no-op"
        )
        assert qf.system_table == str(tmp_path / "new.db")
        assert qf.system_table_backend == "external_catalog"

    def test_failed_reinit_passes_candidate_not_self(self, tmp_path, monkeypatch):
        """``_init_storage`` must be called with the *candidate*
        backend/table as kwargs, not the instance's current values.
        Guards against regressions that re-mutate self first and
        then call a no-arg ``_init_storage()``."""
        from qualifire.core.config import DatasetConfig, QualifireConfig

        qf = self._instance(tmp_path)

        captured = {}

        def _capture(**kwargs):
            captured.update(kwargs)
            raise RuntimeError("abort after capture")
        monkeypatch.setattr(qf, "_init_storage", _capture)

        new_config = QualifireConfig(
            owner="o", bu="b",
            system_table=str(tmp_path / "new.db"),
            system_table_backend="delta",
            datasets=[DatasetConfig(name="d", table="t")],
        )

        import pytest
        with pytest.raises(RuntimeError, match="abort after capture"):
            qf.run_config_parsed(new_config)

        assert captured.get("system_table") == str(tmp_path / "new.db")
        assert captured.get("system_table_backend") == "delta"


class TestCLIConfigErrorPrefixNotDoubled:
    """Round-14 regression: ``load_config`` raises
    ``QualifireConfigError("Config validation failed: {e}")`` and the
    three CLI subcommands used to also prepend
    ``"Config validation failed: "`` before printing, producing
    ``Config validation failed: Config validation failed: ...`` —
    noisy and obscures the actual failure reason. The fix is to keep
    the prefix only in ``load_config`` and print ``str(e)`` from the
    CLI, leaving a single clean prefix."""

    def _bad_config(self, tmp_path):
        import yaml
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({
            "owner": "o", "bu": "b",
            "system_table": "s",
            "system_table_backend": "totally_made_up",
            "datasets": [{"name": "d", "table": "t", "validations": []}],
        }))
        return path

    def _assert_single_prefix(self, err: str) -> None:
        # Exactly one ``Config validation failed`` substring; the
        # doubled-prefix bug produced two. Use a literal count so a
        # regression would be caught even if the outer prefix moved.
        assert err.count("Config validation failed") == 1, (
            f"expected a single 'Config validation failed' prefix, "
            f"got: {err!r}"
        )

    def test_validate_config_prefix_not_doubled(self, tmp_path, capsys):
        path = self._bad_config(tmp_path)
        rc = main(["validate-config", "-c", str(path)])
        assert rc == 1
        self._assert_single_prefix(capsys.readouterr().err)

    def test_run_prefix_not_doubled(self, tmp_path, capsys):
        path = self._bad_config(tmp_path)
        rc = main(["run", "-c", str(path)])
        assert rc == 1
        self._assert_single_prefix(capsys.readouterr().err)

    def test_report_prefix_not_doubled(self, tmp_path, capsys):
        path = self._bad_config(tmp_path)
        rc = main(["report", "-c", str(path), "-o", str(tmp_path / "out.html")])
        assert rc == 1
        self._assert_single_prefix(capsys.readouterr().err)


class TestValidateQueryRequiresNameForHistoryBackedValidators:
    """Round-14 regression (HIGH): ``validate_query()`` defaulted the
    dataset name to ``"custom_query"``. Two programmatic calls with
    different SQL but no explicit name share the same logical-table
    identifier, so drift/forecast/anomaly-detection validators silently
    read and write each other's baselines — false PASS/WARNING/ERROR
    with no runtime error. The fix mirrors the existing
    ``validate(df=...)`` gate: require an explicit ``name`` when a
    history-backed validator is attached. Stateless validators (SLO,
    threshold) still use the default.
    """

    def _qf(self):
        from qualifire.api import Qualifire
        from unittest.mock import MagicMock
        backend = MagicMock()
        backend.spark = MagicMock()
        return Qualifire(backend=backend, owner="o", bu="b")

    def _agg_collection(self):
        from qualifire.core.config import AggregationCollectionConfig
        return AggregationCollectionConfig(expressions={"row_count": "COUNT(*)"})

    def _sample_collection(self):
        from qualifire.core.config import SampleCollectionConfig
        return SampleCollectionConfig(n_records=100)

    def test_historical_without_name_raises(self):
        import pytest
        from qualifire.core.config import (
            HistoricalValidationConfig,
            HistoricalRuleConfig,
            HistoricalThresholds,
            HistoricalCompareConfig,
        )

        qf = self._qf()
        hist = HistoricalValidationConfig(
            collection=self._agg_collection(),
            rules=[HistoricalRuleConfig(
                metric="row_count",
                thresholds=HistoricalThresholds(warning={"deviation_pct": {"min": -10, "max": 10}}),
                compare=HistoricalCompareConfig(past_values=3, step="P7D"),
            )],
        )

        with pytest.raises(ValueError, match="requires an explicit 'name'"):
            qf.validate_query(query="SELECT 1", validations=[hist])

    def test_forecast_without_name_raises(self):
        import pytest
        from qualifire.core.config import (
            ForecastValidationConfig,
            ForecastRuleConfig,
            ForecastModelConfig,
        )

        qf = self._qf()
        fc = ForecastValidationConfig(
            collection=self._agg_collection(),
            rules=[ForecastRuleConfig(
                metric="row_count",
                model=ForecastModelConfig(step="P1D"),
            )],
        )

        with pytest.raises(ValueError, match="requires an explicit 'name'"):
            qf.validate_query(query="SELECT 1", validations=[fc])

    def test_anomaly_without_name_raises(self):
        import pytest
        from qualifire.core.config import (
            AnomalyDetectionValidationConfig,
            AnomalyModelConfig,
            AnomalyThresholdConfig,
        )

        qf = self._qf()
        ad = AnomalyDetectionValidationConfig(
            collection=self._sample_collection(),
            model=AnomalyModelConfig(),
            thresholds=AnomalyThresholdConfig(
                warning={"anomaly_score": 0.6},
                error={"anomaly_score": 0.8},
            ),
        )

        with pytest.raises(ValueError, match="requires an explicit 'name'"):
            qf.validate_query(query="SELECT 1", validations=[ad])

    def test_stateless_validators_still_accept_default_name(self, monkeypatch):
        """SLO/threshold don't touch history, so they must keep working
        without an explicit name. Guards against over-zealous tightening
        that would break the common ad-hoc case."""
        from qualifire.core.config import (
            ThresholdValidationConfig,
            ThresholdRuleConfig,
        )

        qf = self._qf()

        # Stub the engine so we never touch a real SparkSession.
        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.config = kwargs["config"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)

        thr = ThresholdValidationConfig(
            collection=self._agg_collection(),
            rules=[ThresholdRuleConfig(
                metric="row_count",
                thresholds={"error": {"min": 1}},
            )],
        )
        # Must not raise. Dataset name falls back to 'custom_query'.
        qf.validate_query(query="SELECT 1", validations=[thr])

    def test_explicit_name_allows_history_backed_validator(self, monkeypatch):
        """When the caller supplies ``name``, history-backed validators
        are allowed. That's the whole point of the gate — force the
        caller to own the identity choice, not block the feature."""
        from qualifire.core.config import (
            HistoricalValidationConfig,
            HistoricalRuleConfig,
            HistoricalThresholds,
            HistoricalCompareConfig,
        )

        qf = self._qf()

        import qualifire.api as api_module
        class _FakeEngine:
            def __init__(self, **kwargs):
                self.config = kwargs["config"]
            def run(self):
                from qualifire.core.models import QualifireResult
                return QualifireResult(owner="o", bu="b", datasets=[])
        monkeypatch.setattr(api_module, "QualifireEngine", _FakeEngine)

        hist = HistoricalValidationConfig(
            collection=self._agg_collection(),
            rules=[HistoricalRuleConfig(
                metric="row_count",
                thresholds=HistoricalThresholds(warning={"deviation_pct": {"min": -10, "max": 10}}),
                compare=HistoricalCompareConfig(past_values=3, step="P7D"),
            )],
        )
        # Must not raise when caller opts in with a name.
        qf.validate_query(
            query="SELECT 1",
            validations=[hist],
            name="orders_hourly_drift",
        )


class TestCLIPatternConfigValidation:
    """Pattern-check config (YAML type: ``pattern``) must fail at
    ``validate-config`` time for typos, out-of-range AUC thresholds,
    inverted warning/error, unknown threshold keys, out-of-range
    ``cv_folds``, and ``history.filters`` / ``past_dates`` mismatches.
    These are all preflight gates — operators should never discover
    them mid-run."""

    def _base_yaml(self, validation: dict) -> dict:
        return {
            "owner": "team",
            "bu": "finance",
            "system_table": "s.t.h",
            "datasets": [{
                "name": "d",
                "table": "t",
                "validations": [validation],
            }],
        }

    def _pattern_validation(self, **overrides) -> dict:
        v = {
            "type": "pattern",
            "name": "pv",
            "collection": {
                "type": "sample",
                "n_records": 1000,
                "history": {"past_dates": 3, "step": "P7D"},
            },
        }
        v.update(overrides)
        return v

    def test_valid_pattern_config_loads(self, tmp_path):
        import yaml
        cfg = self._base_yaml(self._pattern_validation(
            model={"cv_folds": 5, "exclude_columns": ["event_date"]},
            thresholds={"warning": {"auc": 0.65}, "error": {"auc": 0.80}},
        ))
        path = tmp_path / "pattern.yaml"
        path.write_text(yaml.dump(cfg))
        assert main(["validate-config", "-c", str(path)]) == 0

    def test_typo_type_pattten_is_rejected(self, tmp_path, capsys):
        """``type: patten`` must fail Pydantic's discriminated-union
        resolution at load time — not as a runtime 'Unknown validator
        type'."""
        import yaml
        val = self._pattern_validation()
        val["type"] = "patten"
        path = tmp_path / "typo.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "Config validation failed" in err
        assert "Traceback" not in err

    def test_cv_folds_below_min_rejected(self, tmp_path, capsys):
        """``cv_folds=1`` is not cross-validation."""
        import yaml
        val = self._pattern_validation(model={"cv_folds": 1})
        path = tmp_path / "cv_low.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "cv_folds" in err

    def test_cv_folds_above_max_rejected(self, tmp_path, capsys):
        import yaml
        val = self._pattern_validation(model={"cv_folds": 11})
        path = tmp_path / "cv_high.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "cv_folds" in err

    def test_threshold_typo_key_rejected(self, tmp_path, capsys):
        """Operator typos like ``aux`` must fail, not silently
        fall back to defaults — that would ship a known-wrong
        alert surface with no operator signal."""
        import yaml
        val = self._pattern_validation(
            thresholds={"warning": {"aux": 0.65}},
        )
        path = tmp_path / "typo_key.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "aux" in err or "Unknown pattern threshold key" in err

    def test_threshold_auc_out_of_range_low_rejected(self, tmp_path, capsys):
        """AUC < 0.5 means the classifier is worse than random —
        as an *alert* threshold that makes no sense."""
        import yaml
        val = self._pattern_validation(
            thresholds={"warning": {"auc": 0.3}},
        )
        path = tmp_path / "auc_low.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "auc" in err

    def test_threshold_auc_out_of_range_high_rejected(self, tmp_path, capsys):
        """AUC > 1.0 is mathematically impossible."""
        import yaml
        val = self._pattern_validation(
            thresholds={"warning": {"auc": 1.5}},
        )
        path = tmp_path / "auc_high.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1

    def test_threshold_warning_greater_than_error_rejected(self, tmp_path, capsys):
        """Inverted thresholds (``warning > error``) cause ERROR rows
        to evade WARNING and corrupt alert routing."""
        import yaml
        val = self._pattern_validation(
            thresholds={"warning": {"auc": 0.9}, "error": {"auc": 0.7}},
        )
        path = tmp_path / "inverted.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1

    def test_exclude_columns_bad_identifier_rejected(self, tmp_path, capsys):
        import yaml
        val = self._pattern_validation(
            model={"exclude_columns": ["bad; col"]},
        )
        path = tmp_path / "bad_exclude.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1

    def test_history_filters_length_mismatch_rejected(self, tmp_path, capsys):
        """``SamplerCollector`` silently prefers ``filters`` when
        both are present; a length mismatch would train on a
        different class balance than the operator declared."""
        import yaml
        val = self._pattern_validation(
            collection={
                "type": "sample",
                "n_records": 1000,
                "history": {
                    "past_dates": 3,
                    "filters": ["a", "b", "c", "d", "e"],
                },
            },
        )
        path = tmp_path / "len_mismatch.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 1
        err = capsys.readouterr().err
        assert "past_dates" in err or "filters" in err

    def test_history_filters_length_match_accepted(self, tmp_path):
        """Matching lengths are the operator's intended shape."""
        import yaml
        val = self._pattern_validation(
            collection={
                "type": "sample",
                "n_records": 1000,
                "history": {
                    "past_dates": 3,
                    "filters": ["a", "b", "c"],
                    # ``step`` is required even when ``filters`` is set;
                    # it isn't consulted on the filters path but the
                    # config layer enforces presence so every rule is
                    # reproducible.
                    "step": "P7D",
                },
            },
        )
        path = tmp_path / "len_match.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 0

    def test_history_filters_without_past_dates_accepted(self, tmp_path):
        """When ``past_dates`` was never set, the default shouldn't
        cause a phantom mismatch — the filters length wins silently
        in ``SamplerCollector`` and that's expected."""
        import yaml
        val = self._pattern_validation(
            collection={
                "type": "sample",
                "n_records": 1000,
                "history": {"filters": ["a", "b"], "step": "P7D"},
            },
        )
        path = tmp_path / "filters_only.yaml"
        path.write_text(yaml.dump(self._base_yaml(val)))
        assert main(["validate-config", "-c", str(path)]) == 0
