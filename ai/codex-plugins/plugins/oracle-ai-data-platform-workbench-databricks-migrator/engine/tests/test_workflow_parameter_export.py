"""Regression tests for dbx_export --workflow-param conversion support."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dbx_export.__main__ import (
    ExitCode,
    _record_workflow_conversion_decisions,
    _workflow_override_preflight_checks,
    build_arg_parser,
    main,
    phase3_create_clusters_and_workflows,
    pre_flight,
)
from dbx_export.job_converter import convert_dbx_job
from dbx_export.param_analysis import ParamPlan
from dbx_export.report import Report


def _job(condition_left="{{tasks.upstream.values.is_day_level_job}}"):
    return {
        "job_id": "123",
        "settings": {
            "name": "conditional_job",
            "parameters": [
                {"name": "existing", "default": "old"},
                {"name": "is_day_level_job", "default": "false"},
            ],
            "tasks": [
                {
                    "task_key": "gate",
                    "condition_task": {
                        "op": "EQUAL_TO",
                        "left": condition_left,
                        "right": "true",
                    },
                }
            ],
        },
    }


def _job_with_notebook_parameters():
    job = _job()
    job["settings"]["tasks"].insert(0, {
        "task_key": "upstream",
        "notebook_task": {
            "notebook_path": "/Users/example/upstream",
            "base_parameters": {
                "existing": "",
                "is_day_level_job": "",
                "unrelated": "",
            },
        },
    })
    return job


def test_parser_accepts_repeated_workflow_parameters():
    args = build_arg_parser().parse_args([
        "--workflow-param", "is_day_level_job=true",
        "--workflow-param", "query=a=b",
    ])
    assert args.workflow_param == ["is_day_level_job=true", "query=a=b"]


def test_override_is_persisted_and_condition_uses_job_parameter():
    decisions = []
    payload = convert_dbx_job(
        _job(),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
        conversion_decisions=decisions,
    )

    assert {p["name"]: p["value"] for p in payload["parameters"]} == {
        "existing": "old",
        "is_day_level_job": "true",
    }
    assert payload["tasks"][0]["expressions"] == [{
        "key": "A",
        "value": '{{job.parameters.is_day_level_job}} == "true"',
    }]
    assert decisions == [
        "parameter is_day_level_job: exported default 'false' -> "
        "operator override 'true'",
        "condition gate: left operand "
        "'{{tasks.upstream.values.is_day_level_job}}' -> "
        "'{{job.parameters.is_day_level_job}}'",
    ]


def test_new_workflow_parameter_is_added_without_duplicates():
    payload = convert_dbx_job(
        _job("{{job.parameters.existing}}"),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"new_value": "x", "existing": "new"},
    )

    params = payload["parameters"]
    assert [p["name"] for p in params].count("existing") == 1
    assert {p["name"]: p["value"] for p in params} == {
        "existing": "new",
        "is_day_level_job": "false",
        "new_value": "x",
    }


def test_unmatched_task_value_reference_is_preserved():
    payload = convert_dbx_job(
        _job("{{tasks.upstream.values.real_output}}"),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
    )
    assert payload["tasks"][0]["expressions"][0]["value"] == (
        '{{tasks.upstream.values.real_output}} == "true"'
    )


def test_no_override_preserves_task_value_condition():
    decisions = []
    payload = convert_dbx_job(
        _job(),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        conversion_decisions=decisions,
    )
    assert payload["tasks"][0]["expressions"][0]["value"] == (
        '{{tasks.upstream.values.is_day_level_job}} == "true"'
    )
    assert decisions == []


def test_sensitive_override_decision_redacts_values():
    job = _job("{{job.parameters.api_token}}")
    job["settings"]["parameters"].append({
        "name": "api_token",
        "default": "old-secret",
    })
    decisions = []

    convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"api_token": "new-secret"},
        conversion_decisions=decisions,
    )

    assert decisions == [
        "parameter api_token: exported default <redacted> -> "
        "operator override <redacted>"
    ]
    assert "old-secret" not in repr(decisions)
    assert "new-secret" not in repr(decisions)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("pwd", "plain-value"),
        ("passphrase", "plain-value"),
        ("pat", "plain-value"),
        ("auth_header", "plain-value"),
        ("aws_access_key_id", "plain-value"),
        ("sas", "plain-value"),
        ("flag", "xo" + "xb-123-456"),
        ("conn", "password=abc;user=u"),
        ("url", "https://user:" + "p4ss@host/db"),
        ("header", "Bearer eyJhbGciOiJIUzI1NiJ9"),
        ("workspace", "dapi1234567890"),
        ("aws", "AKIA" + "IOSFODNN7EXAMPLE"),
        ("signed_url", "https://host/path?sv=1&sig=secret"),
        ("dsn", "postgres://user:p4ss@host/db"),
        ("dsn", "mongodb+srv://user:p4ss@host/db"),
        ("dsn", "redis://:p4ss@host/0"),
        ("dsn", "mysql+pymysql://user:p4ss@host/db"),
        ("dsn", "jdbc:oracle:thin:user/p4ss@host:1521/service"),
        ("api_url", "https://host/api?token=abc123"),
        ("api_url", "https://host/api?access_token=abc123"),
        ("api_url", "https://host/api?apikey=abc123"),
        ("api_url", "https://host/api?api_key=abc123"),
        ("http_header", "Basic dXNlcjpwYXNz"),
        ("http_header", "basic dXNlcjpwYXNz"),
        ("http_header", "BASIC dXNlcjpwYXNz"),
        ("http_header", "BaSiC dXNlcjpwYXNz"),
        ("webhook_url", "https://hooks.slack.com/services/T/B/secret"),
        ("key_material", "-----BEGIN RSA PRIVATE KEY-----"),
    ],
)
def test_sensitive_override_decision_redacts_name_and_value_shapes(name, value):
    job = _job("{{job.parameters.existing}}")
    job["settings"]["parameters"].append({
        "name": name,
        "default": value,
    })
    decisions = []

    convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={name: value},
        conversion_decisions=decisions,
    )

    assert decisions == [
        f"parameter {name}: exported default <redacted> -> "
        "operator override <redacted>"
    ]
    assert value not in repr(decisions)


@pytest.mark.parametrize(
    "name",
    [
        "dbPassword",
        "clientSecret",
        "accessToken",
        "authToken",
        "apiToken",
        "sasToken",
        "oauthToken",
        "refreshToken",
        "secretKey",
        "bearerToken",
        "privateKeyPem",
        "databricksToken",
        "accessKeyId",
        "awsSecretAccessKey",
        "mypassword",
        "adminpassword",
        "clientsecret",
        "dbtoken",
        "credentials",
        "db_credentials",
        "tokens",
        "secrets",
        "passwords",
        "DBPASSWORD",
        "privkey",
        "priv_key",
        "ssh_key",
        "signing_key",
        "encryption_key",
        "master_key",
        "license_key",
        "client_key",
        "account_key",
        "accountKey",
        "ACCOUNT_KEY",
        "storage_account_key",
        "storageAccountKey",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "shared_key",
        "hmac_key",
        "subscription_key",
        "app_key",
        "aes_key",
        "jwt_key",
        "deploy_key",
        "tls_key",
        "server_key",
        "service_key",
        "consumer_key",
        "sp_key",
        "principal_key",
        "auth_url",
        "oauth_url",
        "sas_url",
        "cert_pem",
        "client_certificate",
        "ghpat",
        "tokenauth",
        "oauth",
        "max_tokens",
        "tokenCount",
    ],
)
def test_sensitive_parameter_names_are_redacted_in_all_operator_outputs(
    name, capsys, tmp_path,
):
    marker = "sensitive-marker-value"
    decisions = []
    convert_dbx_job(
        _job("{{job.parameters.existing}}"),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={name: marker},
        conversion_decisions=decisions,
    )

    report = Report(
        run_id="run",
        dbx_root_job_id="123",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    _record_workflow_conversion_decisions(
        "SP_AI_conditional_job", decisions, report,
    )
    report_path = report.write(str(tmp_path))
    written = json.loads(Path(report_path).read_text())

    assert "<redacted>" in decisions[0]
    assert marker not in decisions[0]
    assert "<redacted>" in capsys.readouterr().out
    assert marker not in report.errors[0]
    assert "<redacted>" in report.errors[0]
    assert marker not in json.dumps(written)
    assert "<redacted>" in json.dumps(written)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("input_path", "/Volumes/source/input"),
        ("output_path", "/Volumes/target/output"),
        ("author", "migration-operator"),
        ("authorName", "migration-operator"),
        ("pattern", "*.parquet"),
        ("patternId", "daily-load"),
        ("tokenizer", "wordpiece"),
        ("tokenization_mode", "sentence"),
        ("api_url", "https://host/api?sp=1&x=2"),
        ("api_url", "https://host/api?se=1&x=2"),
        ("api_url", "https://host/api?sv=1&x=2"),
        ("key_identifier", "row-key"),
        ("patience", "5"),
        ("authentication", "standard"),
        ("authorizer", "policy"),
        ("sass_mode", "enabled"),
        ("signed_count", "4"),
        ("api_version", "2026-09"),
        ("access_mode", "read"),
        ("private_mode", "false"),
        ("partition_key", "account_number"),
        ("primary_key", "id"),
        ("row_key", "customer_id"),
        ("sort_key", "event_time"),
        ("object_key", "daily/object.parquet"),
        ("s3_key", "source/object.parquet"),
        ("registry_key", "model-name"),
        ("keyvault_name", "shared-vault"),
        ("keystore_path", "/Volumes/security/keystore"),
        ("key_file", "/Volumes/security/signing.pem"),
        ("key_filename", "signing.pem"),
        ("key_filepath", "/Volumes/security/signing.pem"),
        ("key_location", "/Volumes/security"),
        ("cert_file", "/Volumes/security/client.crt"),
        ("pem_file", "/Volumes/security/client.pem"),
        ("pfx_file", "/Volumes/security/client.pfx"),
        ("keystore_file", "/Volumes/security/keystore.jks"),
        ("certificate_file", "/Volumes/security/client.crt"),
        ("cert_filename", "client.crt"),
        ("cert_filepath", "/Volumes/security/client.crt"),
        ("certFile", "/Volumes/security/client.crt"),
        ("description", "Basic Authentication"),
        ("description", "basic statistics"),
        ("description", "BASIC TRAINING"),
    ],
)
def test_non_sensitive_override_remains_auditable_in_decision_and_report(
    name, value, capsys,
):
    job = _job("{{job.parameters.existing}}")
    decisions = []

    convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={name: value},
        conversion_decisions=decisions,
    )

    report = Report(
        run_id="run",
        dbx_root_job_id="123",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    _record_workflow_conversion_decisions("SP_AI_conditional_job", decisions, report)

    assert value in decisions[0]
    assert value in capsys.readouterr().out
    assert value in report.errors[0]


def test_conversion_decisions_are_logged_and_persisted_in_report(capsys):
    report = Report(
        run_id="run",
        dbx_root_job_id="123",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    decision = (
        "condition gate: left operand "
        "'{{tasks.upstream.values.flag}}' -> "
        "'{{job.parameters.flag}}'"
    )

    _record_workflow_conversion_decisions(
        "SP_AI_conditional_job", [decision], report,
    )

    assert f"[workflow-parameter] {decision}" in capsys.readouterr().out
    assert report.errors == [
        f"INFO workflow-parameter: SP_AI_conditional_job: {decision}"
    ]
    assert report.summary()["info"] == 1
    assert report.summary()["errors"] == 0


def test_no_override_preserves_empty_task_parameter_shadow():
    payload = convert_dbx_job(
        _job_with_notebook_parameters(),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
    )
    assert payload["tasks"][0]["parameters"] == [
        {"name": "existing", "value": ""},
        {"name": "is_day_level_job", "value": ""},
        {"name": "unrelated", "value": ""},
    ]


def test_explicit_override_strips_only_matching_empty_task_parameter():
    decisions = []
    payload = convert_dbx_job(
        _job_with_notebook_parameters(),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
        conversion_decisions=decisions,
    )
    assert payload["tasks"][0]["parameters"] == [
        {"name": "existing", "value": ""},
        {"name": "unrelated", "value": ""},
    ]
    assert decisions[1] == (
        "task upstream: removed redundant base_parameter is_day_level_job '' "
        "(uses the overridden workflow parameter)"
    )


def test_conflicting_non_empty_task_parameter_override_is_rejected():
    job = _job_with_notebook_parameters()
    job["settings"]["tasks"][0]["notebook_task"]["base_parameters"][
        "is_day_level_job"
    ] = "false"

    with pytest.raises(
        ValueError,
        match="Align the task base_parameter with the workflow override",
    ):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )


def test_matching_non_empty_task_parameter_override_is_preserved():
    job = _job_with_notebook_parameters()
    job["settings"]["tasks"][0]["notebook_task"]["base_parameters"][
        "is_day_level_job"
    ] = "true"

    payload = convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
    )

    assert {p["name"]: p["value"] for p in payload["tasks"][0]["parameters"]}[
        "is_day_level_job"
    ] == "true"


@pytest.mark.parametrize(
    "forwarded",
    [
        "{{job.parameters.is_day_level_job}}",
        "{{  job.parameters.is_day_level_job  }}",
    ],
)
def test_same_name_job_parameter_forwarding_is_agreeing_and_dropped(forwarded):
    job = _job_with_notebook_parameters()
    job["settings"]["tasks"][0]["notebook_task"]["base_parameters"][
        "is_day_level_job"
    ] = forwarded

    decisions = []
    payload = convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
        conversion_decisions=decisions,
    )

    assert "is_day_level_job" not in {
        parameter["name"] for parameter in payload["tasks"][0]["parameters"]
    }
    assert decisions[1] == (
        "task upstream: removed redundant base_parameter is_day_level_job "
        f"{forwarded!r} (forwards the overridden workflow parameter)"
    )


@pytest.mark.parametrize(
    "forwarded",
    [
        "{{tasks.setter.values.is_day_level_job}}",
        "{{  tasks.setter.values.is_day_level_job  }}",
    ],
)
def test_base_parameter_task_value_forwarding_uses_wiring_refusal(forwarded):
    job = _job_with_notebook_parameters()
    job["settings"]["tasks"][0]["notebook_task"]["base_parameters"][
        "is_day_level_job"
    ] = forwarded

    with pytest.raises(ValueError, match="conflicts with task-value wiring"):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )


@pytest.mark.parametrize(
    "forwarded",
    [
        "{{tasks.setter.values.is_day_level_job}}",
        "{{  tasks.setter.values.is_day_level_job  }}",
    ],
)
def test_aliased_task_value_forwarding_uses_referenced_key_for_refusal(forwarded):
    job = _job_with_notebook_parameters()
    parameters = job["settings"]["tasks"][0]["notebook_task"][
        "base_parameters"
    ]
    parameters.pop("is_day_level_job")
    parameters["flag"] = forwarded

    with pytest.raises(ValueError, match="conflicts with task-value wiring"):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )


def test_embedded_task_value_in_base_parameter_uses_wiring_refusal():
    job = _job_with_notebook_parameters()
    parameters = job["settings"]["tasks"][0]["notebook_task"][
        "base_parameters"
    ]
    parameters.pop("is_day_level_job")
    parameters["path"] = (
        "/data/{{tasks.upstream.values.is_day_level_job}}/input"
    )

    with pytest.raises(ValueError, match="conflicts with task-value wiring"):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )


def test_embedded_condition_task_value_is_preserved_and_warned(capsys):
    job = _job("x{{tasks.upstream.values.is_day_level_job}}")
    decisions = []
    payload = convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"is_day_level_job": "true"},
        conversion_decisions=decisions,
    )

    assert payload["tasks"][0]["expressions"][0]["value"] == (
        '"x{{tasks.upstream.values.is_day_level_job}}" == "true"'
    )
    warning = (
        "WARN condition gate: left operand still references "
        "tasks.upstream.values.is_day_level_job inside a larger expression; "
        "override is_day_level_job does not redirect it"
    )
    assert warning in decisions

    report = Report(
        run_id="run",
        dbx_root_job_id="123",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    _record_workflow_conversion_decisions(
        "SP_AI_conditional_job", decisions, report,
    )
    assert "[workflow-parameter][WARN]" in capsys.readouterr().out
    assert any(
        item.startswith("WARN workflow-parameter:")
        for item in report.errors
    )
    assert report.summary()["warnings"] == 1


def test_q2_task_value_override_is_rejected():
    plan = ParamPlan(task_extra_params={
        "upstream": [{"name": "is_day_level_job", "value": ""}],
        "reader": [{
            "name": "is_day_level_job",
            "value": "{{tasks.upstream.values.is_day_level_job}}",
        }],
    })
    with pytest.raises(ValueError, match="conflicts with task-value wiring"):
        convert_dbx_job(
            _job(),
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            param_plan=plan,
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )

@pytest.mark.parametrize("raw", ["missing_equals", "=empty", "bad-key=value"])
def test_cli_rejects_malformed_workflow_parameter(raw, capsys):
    config = Path(__file__).parent / "config" / "fixtures" / "minimal.yaml"
    assert main([
        "--config", str(config),
        "--workflow-param", raw,
    ]) == ExitCode.BAD_ARGS
    assert "invalid --workflow-param" in capsys.readouterr().err


def test_cli_does_not_echo_malformed_workflow_parameter_value(capsys):
    sensitive_value = "super-secret"
    raw = f"api-key={sensitive_value}"

    assert main(["--workflow-param", raw]) == ExitCode.BAD_ARGS

    error = capsys.readouterr().err
    assert "invalid --workflow-param" in error
    assert raw not in error
    assert sensitive_value not in error


def test_cli_reports_malformed_workflow_parameter_position(capsys):
    sensitive_value = "super-secret"
    assert main([
        "--workflow-param", "valid=one",
        "--workflow-param", f"bad-key={sensitive_value}",
    ]) == ExitCode.BAD_ARGS

    error = capsys.readouterr().err
    assert "invalid --workflow-param #2 of 2" in error
    assert sensitive_value not in error


def test_cli_warns_when_duplicate_workflow_parameter_uses_last_value(
    capsys, monkeypatch,
):
    def stop_after_config(*args, **kwargs):
        raise RuntimeError("stop-after-parse")

    monkeypatch.setattr("aidp_config.load_config", stop_after_config)
    assert main([
        "--workflow-param", "flag=first",
        "--workflow-param", "flag=second",
    ]) == ExitCode.BAD_ARGS
    error = capsys.readouterr().err
    assert "duplicate --workflow-param key 'flag' at position 2" in error
    assert "first" not in error
    assert "second" not in error


def test_cli_does_not_echo_malformed_discovery_parameter_value(
    capsys, monkeypatch,
):
    config = SimpleNamespace(
        aidp=SimpleNamespace(
            lake_ocid="lake", workspace_id="workspace", oci_profile="CUSTOMER",
        ),
        paths=SimpleNamespace(export_output="/Workspace/export"),
        dbx_source=SimpleNamespace(host=None, proxy=None),
        secrets=SimpleNamespace(
            dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None,
        ),
    )
    monkeypatch.setattr("aidp_config.load_config", lambda **kwargs: config)
    monkeypatch.setattr(
        "dbx_export.__main__.configured_lib_repository_dir",
        lambda _path: "/Workspace/migration-dependencies/libs",
    )
    sensitive_value = "super-secret"
    assert main([
        "--param", f"={sensitive_value}",
    ]) == ExitCode.BAD_ARGS

    error = capsys.readouterr().err
    assert "invalid --param #1 of 1" in error
    assert sensitive_value not in error


def test_cli_warns_when_duplicate_discovery_parameter_uses_last_value(
    capsys, monkeypatch,
):
    config = SimpleNamespace(
        aidp=SimpleNamespace(
            lake_ocid="lake", workspace_id="workspace", oci_profile="CUSTOMER",
        ),
        paths=SimpleNamespace(export_output="/Workspace/export"),
        dbx_source=SimpleNamespace(host=None, proxy=None),
        secrets=SimpleNamespace(
            dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None,
        ),
    )
    monkeypatch.setattr("aidp_config.load_config", lambda **kwargs: config)
    monkeypatch.setattr(
        "dbx_export.__main__.configured_lib_repository_dir",
        lambda _path: "/Workspace/migration-dependencies/libs",
    )

    assert main([
        "--dbx-proxy", "https://proxy.invalid",
        "--param", "date=first",
        "--param", "date=second",
    ]) == ExitCode.BAD_ARGS
    error = capsys.readouterr().err
    assert "duplicate --param key 'date' at position 2" in error
    assert "first" not in error
    assert "second" not in error


def test_override_rejects_multiple_condition_task_value_producers():
    job = _job("{{tasks.a.values.flag}}")
    job["settings"]["tasks"][0]["condition_task"]["right"] = (
        "{{tasks.b.values.flag}}"
    )

    with pytest.raises(
        ValueError,
        match=(
            "ambiguous across condition task-value producers: "
            r"flag \(tasks: a, b\)"
        ),
    ):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"flag": "true"},
        )


def test_mixed_condition_rewrites_exact_operand_and_warns_for_embedded_operand():
    job = _job("{{tasks.upstream.values.flag}}")
    job["settings"]["tasks"][0]["condition_task"]["right"] = (
        "prefix-{{tasks.upstream.values.flag}}"
    )
    decisions = []

    payload = convert_dbx_job(
        job,
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workflow_parameter_overrides={"flag": "true"},
        conversion_decisions=decisions,
    )

    assert payload["tasks"][0]["expressions"] == [{
        "key": "A",
        "value": (
            "{{job.parameters.flag}} == "
            '"prefix-{{tasks.upstream.values.flag}}"'
        ),
    }]
    assert any(
        decision.startswith("WARN condition gate: right operand still references")
        for decision in decisions
    )


def test_converter_rejects_invalid_workflow_parameter_name():
    with pytest.raises(ValueError, match="Invalid workflow parameter name"):
        convert_dbx_job(
            _job(),
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"bad-key": "value"},
        )


def test_explicit_override_value_is_not_workspace_path_remapped():
    payload = convert_dbx_job(
        _job(),
        export_output="/Workspace/export",
        prefix="SP_AI",
        dbx_cluster_key_to_aidp_key={},
        workspace_resource_path_map={
            "/Workspace/source/path": "/Workspace/export/path",
        },
        workflow_parameter_overrides={
            "is_day_level_job": "/Workspace/source/path",
        },
    )
    assert {p["name"]: p["value"] for p in payload["parameters"]}[
        "is_day_level_job"
    ] == "/Workspace/source/path"


def test_override_rejects_duplicate_dbx_parameter_name():
    job = _job()
    job["settings"]["parameters"].append({
        "name": "is_day_level_job",
        "default": "duplicate",
    })
    with pytest.raises(ValueError, match="duplicate DBX parameter"):
        convert_dbx_job(
            job,
            export_output="/Workspace/export",
            prefix="SP_AI",
            dbx_cluster_key_to_aidp_key={},
            workflow_parameter_overrides={"is_day_level_job": "true"},
        )


def test_phase3_records_converter_refusal_and_preserves_prior_created_job(tmp_path):
    class FakeAidp:
        def __init__(self):
            self.created = []

        def find_workflow_by_name(self, name):
            return None

        def create_workflow_shell(self, name, description):
            key = f"workflow-{len(self.created) + 1}"
            self.created.append((key, name))
            return {"key": key}

        def update_workflow(self, key, payload):
            return None

    good_job = {
        "job_id": "1",
        "settings": {
            "name": "good_job",
            "parameters": [{"name": "k", "default": "old"}],
            "tasks": [{
                "task_key": "reader",
                "notebook_task": {
                    "notebook_path": "/Users/example/reader",
                    "base_parameters": {
                        "k": "{{job.parameters.k}}",
                    },
                },
            }],
        },
    }
    refused_job = {
        "job_id": "2",
        "settings": {
            "name": "refused_job",
            "parameters": [
                {"name": "k", "default": "first"},
                {"name": "k", "default": "duplicate"},
            ],
            "tasks": [],
        },
    }
    graph = SimpleNamespace(
        jobs={
            "1": SimpleNamespace(job_def=good_job, fetch_error=None),
            "2": SimpleNamespace(job_def=refused_job, fetch_error=None),
        },
        notebooks={},
        files={},
    )
    report = Report(
        run_id="run",
        dbx_root_job_id="1",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )
    aidp = FakeAidp()

    phase3_create_clusters_and_workflows(
        aidp=aidp,
        dbx=SimpleNamespace(),
        graph=graph,
        nb_target_paths={},
        uri_to_aidp={},
        export_output="/Workspace/export",
        prefix="SP_AI",
        overwrite_workflow=False,
        cluster_override_key="cluster-key",
        report=report,
        workflow_parameter_overrides={"k": "new"},
    )

    assert [job.status for job in report.jobs] == ["CREATED", "FAILED"]
    assert aidp.created == [("workflow-1", "SP_AI_good_job")]
    assert "duplicate DBX parameter" in report.jobs[1].failure_reason
    assert any(
        error.startswith("ERROR workflow-conversion: SP_AI_refused_job:")
        for error in report.errors
    )
    assert (
        "INFO workflow-parameter: SP_AI_good_job: task reader: removed "
        "redundant base_parameter k '{{job.parameters.k}}' (forwards the "
        "overridden workflow parameter)"
    ) in report.errors

    report_path = report.write(str(tmp_path))
    written = json.loads(Path(report_path).read_text())
    assert [job["status"] for job in written["jobs"]] == [
        "CREATED",
        "FAILED",
    ]
    assert written["summary"]["jobs_created"] == 1
    assert written["summary"]["jobs_failed"] == 1
    assert written["summary"]["info"] == 2


def test_phase3_refusal_happens_before_any_cluster_operation():
    class NoClusterOperations:
        def __getattr__(self, name):
            raise AssertionError(f"unexpected AIDP operation before refusal: {name}")

    refused_job = {
        "job_id": "2",
        "settings": {
            "name": "refused_job",
            "parameters": [
                {"name": "k", "default": "first"},
                {"name": "k", "default": "duplicate"},
            ],
            "job_clusters": [{
                "job_cluster_key": "jc",
                "new_cluster": {"spark_version": "15.4.x-scala2.12"},
            }],
            "tasks": [],
        },
    }
    graph = SimpleNamespace(
        jobs={
            "2": SimpleNamespace(job_def=refused_job, fetch_error=None),
        },
        notebooks={},
        files={},
    )
    report = Report(
        run_id="run",
        dbx_root_job_id="2",
        export_output="/Workspace/export",
        prefix="SP_AI",
    )

    phase3_create_clusters_and_workflows(
        aidp=NoClusterOperations(),
        dbx=SimpleNamespace(),
        graph=graph,
        nb_target_paths={},
        uri_to_aidp={},
        export_output="/Workspace/export",
        prefix="SP_AI",
        overwrite_workflow=False,
        cluster_override_key=None,
        report=report,
        workflow_parameter_overrides={"k": "new"},
    )

    assert [job.status for job in report.jobs] == ["FAILED"]
    assert report.jobs[0].cluster_status == ""
    assert "duplicate DBX parameter" in report.jobs[0].failure_reason


def test_preflight_surfaces_refusal_without_aidp_writes():
    refused_job = {
        "job_id": "2",
        "settings": {
            "name": "refused_job",
            "parameters": [
                {"name": "k", "default": "first"},
                {"name": "k", "default": "duplicate"},
            ],
            "tasks": [],
        },
    }
    graph = SimpleNamespace(
        jobs={
            "2": SimpleNamespace(job_def=refused_job, fetch_error=None),
        },
        notebooks={},
        files={},
        external_refs=[],
        skipped_loose_candidates=[],
    )
    checks = _workflow_override_preflight_checks(
        graph,
        "2",
        {"k": "new"},
    )
    rendered = pre_flight(
        graph,
        "/Workspace/export",
        "SP_AI",
        "2",
        workflow_parameter_overrides={"k": "new"},
        workflow_override_checks=checks,
    )

    assert checks == [{
        "job_id": "2",
        "name": "refused_job",
        "status": "WILL REFUSE",
        "detail": (
            "Workflow parameter override matches duplicate DBX parameter "
            "name(s): k"
        ),
    }]
    assert "Workflow:           SP_AI_refused_job      [WILL REFUSE]" in rendered
    assert "duplicate DBX parameter name(s): k" in rendered


def test_preflight_turns_malformed_job_structure_into_refusal():
    graph = SimpleNamespace(
        jobs={
            "2": SimpleNamespace(
                job_def={
                    "job_id": "2",
                    "settings": {
                        "name": "malformed_job",
                        "tasks": {"not": "a list"},
                    },
                },
                fetch_error=None,
            ),
        },
        notebooks={},
        files={},
        external_refs=[],
    )

    checks = _workflow_override_preflight_checks(graph, "2", {"k": "new"})

    assert len(checks) == 1
    assert checks[0]["name"] == "malformed_job"
    assert checks[0]["status"] == "WILL REFUSE"
    assert checks[0]["detail"]


def test_preflight_parameter_analysis_suppresses_duplicate_warning_logs(
    monkeypatch,
):
    observed_logs = []

    def fake_analyze(_settings, _notebooks, log=None, context_dependencies=None):
        assert context_dependencies == {}
        observed_logs.append(log)
        return ParamPlan()

    monkeypatch.setattr(
        "dbx_export.__main__.analyze_task_params",
        fake_analyze,
    )
    graph = SimpleNamespace(
        jobs={
            "2": SimpleNamespace(job_def=_job(), fetch_error=None),
        },
        notebooks={},
        files={},
        external_refs=[],
    )

    _workflow_override_preflight_checks(graph, "2", {"new_value": "x"})

    assert observed_logs == [None]


@pytest.mark.parametrize("dry_run", [True, False])
def test_main_reports_preflight_refusal_in_json(
    tmp_path, capsys, monkeypatch, dry_run,
):
    execution_order = []
    refused_job = {
        "job_id": "2",
        "settings": {
            "name": "refused_job",
            "parameters": [
                {"name": "k", "default": "first"},
                {"name": "k", "default": "duplicate"},
            ],
            "tasks": [],
        },
    }
    graph = SimpleNamespace(
        jobs={"2": SimpleNamespace(job_def=refused_job, fetch_error=None)},
        notebooks={},
        files={},
        external_refs=[],
        skipped_loose_candidates=[],
    )
    config = SimpleNamespace(
        aidp=SimpleNamespace(
            lake_ocid="lake", workspace_id="workspace", oci_profile="CUSTOMER",
        ),
        paths=SimpleNamespace(
            export_output="/Workspace/export",
            target_runtime_requirements_file=None,
            compatibility_policy_file=None,
        ),
        dbx_source=SimpleNamespace(host=None, proxy=None),
        secrets=SimpleNamespace(
            dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None,
        ),
    )

    class JarDiscovery:
        index_fallbacks = []
        resolutions = []
        index_errors = []
        blockers = []
        allow_unresolved = True

        @staticmethod
        def to_dict():
            return {"summary": {"missing": 0, "ambiguous": 0, "index_errors": 0}}

        @staticmethod
        def write(*args):
            return str(tmp_path / "jar-discovery.json")

    monkeypatch.setattr("aidp_config.load_config", lambda **kwargs: config)
    monkeypatch.setattr(
        "dbx_export.__main__.configured_lib_repository_dir",
        lambda _path: "/Workspace/migration-dependencies/libs",
    )
    monkeypatch.setattr("dbx_export.__main__.get_oci_signer", lambda **kwargs: ({}, object()))
    monkeypatch.setattr("dbx_export.__main__.AidpClient", lambda **kwargs: SimpleNamespace())
    monkeypatch.setattr("dbx_export.__main__._limits_startup_gate", lambda _args: None)
    monkeypatch.setattr("dbx_export.__main__.ph_mod.new_run_id", lambda: "run")
    monkeypatch.setattr("dbx_export.__main__.phase1_discover", lambda *args, **kwargs: graph)
    for name in (
        "phase1_discover_existing_cluster_libraries",
        "phase1_fetch_files",
        "phase1_discover_init_script_artifacts",
        "discover_notebook_pip_deps",
        "consolidate_cluster_requirements",
        "resolve_failed_jar_fallbacks",
        "reconcile_failed_declared_jars",
    ):
        monkeypatch.setattr(f"dbx_export.__main__.{name}", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "dbx_export.__main__.discover_notebook_jars",
        lambda *args, **kwargs: JarDiscovery(),
    )
    monkeypatch.setattr(
        "dbx_export.__main__.phase2_upload_notebooks",
        lambda *args, **kwargs: (
            execution_order.append("phase2_notebooks") or ({}, [])
        ),
    )
    monkeypatch.setattr(
        "dbx_export.__main__.phase2_upload_files",
        lambda *args, **kwargs: (
            execution_order.append("phase2_files") or ({}, False)
        ),
    )

    real_phase3 = phase3_create_clusters_and_workflows

    def recording_phase3(*args, **kwargs):
        execution_order.append("phase3_refusal")
        return real_phase3(*args, **kwargs)

    monkeypatch.setattr(
        "dbx_export.__main__.phase3_create_clusters_and_workflows",
        recording_phase3,
    )
    from dbx_export import __main__ as export_main
    real_tprint = export_main.tprint

    def recording_tprint(message):
        if "will be refused in Phase 3" in message:
            execution_order.append("preflight_warning_and_report")
        real_tprint(message)

    monkeypatch.setattr("dbx_export.__main__.tprint", recording_tprint)

    argv = [
        "--job-id", "2",
        "--dbx-proxy", "https://proxy.invalid",
        "--workflow-param", "k=new",
        "--report-dir", str(tmp_path),
    ]
    if dry_run:
        argv.append("--dry-run")
    result = main(argv)

    assert result == (
        ExitCode.JOB_CREATE_FAILED
    )
    output = capsys.readouterr().out
    assert "[WILL REFUSE]" in output
    if dry_run:
        assert "--dry-run: no AIDP writes performed" in output
    else:
        assert (
            "[WARN] refused_job will be refused in Phase 3; Phase 2 uploads "
            "continue (use --dry-run to preview)"
        ) in output
        assert execution_order == [
            "preflight_warning_and_report",
            "phase2_notebooks",
            "phase2_files",
            "phase3_refusal",
        ]
    report_path = tmp_path / "export_dbx_job_2_run.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert any(
        item.startswith("ERROR workflow-conversion-preflight:")
        for item in report["errors"]
    )
