from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

from aws_aidp.aidp_client.client import AidpClient, AidpConfig, AidpError


class Response:
    def __init__(self, status_code=200, text="{}", payload=None):
        self.status_code = status_code
        self.text = text
        self.payload = payload

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload if self.payload is not None else {}


def client(**kwargs):
    value = AidpClient(
        AidpConfig("P", "test-region-1", "lake", "workspace"),
        max_retries=kwargs.pop("max_retries", 0),
        **kwargs,
    )
    value._signer = object()
    return value


class AidpClientTests(unittest.TestCase):
    def fake_requests(self, response):
        module = types.SimpleNamespace(request=lambda *args, **kwargs: response)
        return patch.dict(sys.modules, {"requests": module})

    def test_empty_success_response(self):
        with self.fake_requests(Response(text="")):
            self.assertEqual(client()._req("GET", "https://example.test"), {})

    def test_non_json_success_response_is_preserved(self):
        response = Response(text="plain", payload=ValueError("not json"))
        with self.fake_requests(response):
            self.assertEqual(
                client()._req("GET", "https://example.test"), {"_raw": "plain"}
            )

    def test_http_error_contains_status(self):
        with self.fake_requests(Response(status_code=503, text="unavailable")):
            with self.assertRaises(AidpError) as raised:
                client()._req("GET", "https://example.test")
        self.assertEqual(raised.exception.status, 503)

    def test_create_run_requires_key(self):
        response = Response(status_code=201, text='{"state":"created"}', payload={"state": "created"})
        with self.fake_requests(response):
            with self.assertRaisesRegex(AidpError, "no `key`"):
                client().create_job_run("job")

    def test_transport_error_is_wrapped(self):
        module = types.SimpleNamespace(
            request=lambda *args, **kwargs: (_ for _ in ()).throw(TimeoutError("slow"))
        )
        with patch.dict(sys.modules, {"requests": module}):
            with self.assertRaises(AidpError) as raised:
                client()._req("GET", "https://example.test")
        self.assertEqual(raised.exception.status, 0)
        self.assertIn("slow", str(raised.exception))

    def test_non_object_json_success_is_rejected(self):
        with self.fake_requests(Response(text="[]", payload=[])):
            with self.assertRaisesRegex(AidpError, "non-object JSON"):
                client()._req("GET", "https://example.test")

    def test_resource_keys_are_url_encoded(self):
        value = client()
        with patch.object(value, "_req", return_value={}) as request:
            value.get_job_run("../../run key")
        self.assertTrue(request.call_args.args[1].endswith("/jobRuns/..%2F..%2Frun%20key"))

    def test_timeout_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "timeout"):
            AidpClient(AidpConfig("P", "test-region-1", "lake", "workspace"), timeout=0)

    def test_current_oracle_api_base(self):
        cfg = AidpConfig("P", "eu-frankfurt-1", "ocid1.aidataplatform.x", "ws")
        self.assertEqual(
            cfg.ws_base,
            "https://datalake.eu-frankfurt-1.oci.oraclecloud.com/20260430/"
            "aiDataPlatforms/ocid1.aidataplatform.x/workspaces/ws",
        )

    def test_all_resource_urls_use_the_documented_service_root_without_credentials(self):
        value = AidpClient(AidpConfig(
            "unused-profile", "eu-frankfurt-1", "platform/id", "workspace key",
        ))
        urls = []

        def request(method, url, **kwargs):
            urls.append((method, url))
            return {"key": "created-run"}

        with patch.object(value, "_req", side_effect=request):
            value.get_job("job/key")
            value.get_job_run("run/key")
            value.create_job_run("job/key", retry_token="stable-token")
            value.repair_job_run("run/key", ["task/key"], retry_token="stable-token")
            value.get_cluster("cluster/key")

        root = (
            "https://datalake.eu-frankfurt-1.oci.oraclecloud.com/20260430/"
            "aiDataPlatforms/platform%2Fid/workspaces/workspace%20key"
        )
        self.assertEqual(urls, [
            ("GET", f"{root}/jobs/job%2Fkey"),
            ("GET", f"{root}/jobRuns/run%2Fkey"),
            ("POST", f"{root}/jobRuns"),
            ("POST", f"{root}/jobRuns/run%2Fkey/actions/repair"),
            ("GET", f"{root}/clusters/cluster%2Fkey"),
        ])
        self.assertIsNone(value._signer)

    def test_api_version_selects_the_matching_default_host_and_path_prefix(self):
        cfg = AidpConfig("P", "eu-frankfurt-1", "platform", "workspace", "20260430")
        self.assertEqual(
            cfg.base,
            "https://datalake.eu-frankfurt-1.oci.oraclecloud.com/20260430/"
            "aiDataPlatforms",
        )

    def test_unknown_api_version_cannot_construct_an_unverified_route(self):
        with self.assertRaisesRegex(ValueError, "AIDP_ENDPOINT"):
            AidpConfig("P", "eu-frankfurt-1", "platform", "workspace", "20240831")

    def test_explicit_endpoint_overrides_both_host_and_path_prefix(self):
        cfg = AidpConfig(
            "P", "eu-frankfurt-1", "platform", "workspace",
            endpoint="https://private.example.test/20240831/dataLakes/",
        )
        self.assertIsNone(cfg.api_version)
        self.assertEqual(
            cfg.ws_base,
            "https://private.example.test/20240831/dataLakes/"
            "platform/workspaces/workspace",
        )
        value = AidpClient(cfg)
        with patch.object(value, "_req", return_value={}) as request:
            value.get_job("job")
        request.assert_called_once_with(
            "GET",
            "https://private.example.test/20240831/dataLakes/"
            "platform/workspaces/workspace/jobs/job",
        )
        self.assertIsNone(value._signer)

    def test_environment_endpoint_and_version_are_mutually_exclusive(self):
        required = {
            "OCI_PROFILE": "P",
            "OCI_REGION": "eu-frankfurt-1",
            "DATALAKE_OCID": "platform",
            "WORKSPACE_ID": "workspace",
        }
        with patch.dict(os.environ, required, clear=True):
            self.assertEqual(AidpConfig.from_env().api_version, "20260430")
        with patch.dict(os.environ, {
            **required,
            "AIDP_ENDPOINT": "https://private.example.test/20240831/dataLakes",
        }, clear=True):
            self.assertEqual(
                AidpConfig.from_env().base,
                "https://private.example.test/20240831/dataLakes",
            )
        with patch.dict(os.environ, {
            **required,
            "AIDP_ENDPOINT": "https://private.example.test/20240831/dataLakes",
            "AIDP_API_VERSION": "20260430",
        }, clear=True), self.assertRaisesRegex(ValueError, "either"):
            AidpConfig.from_env()

    def test_environment_blank_route_variables_are_ignored(self):
        # `.env` templates ship `AIDP_ENDPOINT=` with no value; a blank must
        # mean "unset", not "endpoint configured".
        with patch.dict(os.environ, {
            "OCI_PROFILE": "P",
            "OCI_REGION": "eu-frankfurt-1",
            "DATALAKE_OCID": "platform",
            "WORKSPACE_ID": "workspace",
            "AIDP_ENDPOINT": "",
            "AIDP_API_VERSION": "",
        }, clear=True):
            config = AidpConfig.from_env()
        self.assertEqual(config.api_version, "20260430")
        self.assertIsNone(config.endpoint)
        self.assertTrue(config.base.startswith("https://datalake.eu-frankfurt-1."))

    def test_explicit_endpoint_rejects_unsafe_or_ambiguous_urls(self):
        for endpoint in (
            "http://example.test/20260430",
            "https://user@example.test/20260430",
            "https://example.test/20260430?preview=true",
            "https://example.test/20260430#fragment",
            "https://example.test/20260430/aiDataPlatforms?",
            "https://example.test/20260430/aiDataPlatforms#",
            "https://example.test:/20260430/aiDataPlatforms",
            "https://example.test:bad/20260430/aiDataPlatforms",
            "https://example.test:65536/20260430/aiDataPlatforms",
            "https://example.test:0/20260430/aiDataPlatforms",
            "https://[2001:db8::1]:/20260430/aiDataPlatforms",
            "https://[invalid/20260430/aiDataPlatforms",
            "https://example.test/bad path",
            "https://example.test",
            "",
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                AidpConfig(
                    "P", "eu-frankfurt-1", "platform", "workspace",
                    endpoint=endpoint,
                )

    def test_explicit_endpoint_accepts_valid_ports_and_ipv6_hosts(self):
        cases = (
            "https://example.test:8443/custom/aiDataPlatforms/",
            "https://[2001:db8::1]:443/custom/aiDataPlatforms/",
        )
        for endpoint in cases:
            with self.subTest(endpoint=endpoint):
                cfg = AidpConfig(
                    "P", "eu-frankfurt-1", "platform", "workspace",
                    endpoint=endpoint,
                )
                self.assertEqual(cfg.base, endpoint.rstrip("/"))

    def test_mapping_parameters_are_normalized_to_oracle_array(self):
        value = client()
        with patch.object(value, "_req", return_value={"key": "run"}) as request:
            self.assertEqual(value.create_job_run("job", {"DATE": "2026-09-09"}), "run")
        self.assertEqual(request.call_args.kwargs["json"], {
            "jobKey": "job",
            "parameters": [{"name": "DATE", "value": "2026-09-09"}],
        })
        self.assertIn("opc-retry-token", request.call_args.kwargs["headers"])

    def test_mapping_parameters_reject_non_string_names_and_values_like_list_form(self):
        value = client()
        equivalent_invalid_forms = (
            ({"DATE": None}, [{"name": "DATE", "value": None}]),
            ({"COUNT": 3}, [{"name": "COUNT", "value": 3}]),
            ({None: "value"}, [{"name": None, "value": "value"}]),
            ({3: "value"}, [{"name": 3, "value": "value"}]),
        )
        for mapping, list_form in equivalent_invalid_forms:
            for parameters in (mapping, list_form):
                with self.subTest(parameters=parameters), self.assertRaises(ValueError):
                    value.create_job_run("job", parameters)

    def test_retryable_get_is_retried(self):
        responses = iter([
            Response(status_code=503, text="busy"),
            Response(status_code=200, text="{}", payload={"ok": True}),
        ])
        module = types.SimpleNamespace(request=lambda *args, **kwargs: next(responses))
        with patch.dict(sys.modules, {"requests": module}), patch("time.sleep") as sleep:
            result = client(max_retries=1, retry_backoff=0)._req("GET", "https://example.test")
        self.assertEqual(result, {"ok": True})
        sleep.assert_called_once_with(0)

    def test_repair_requires_nonempty_task_keys(self):
        with self.assertRaisesRegex(ValueError, "task_keys"):
            client().repair_job_run("run", [])

    def test_mutating_retry_reuses_one_idempotency_token(self):
        responses = iter([
            Response(status_code=503, text="busy"),
            Response(status_code=201, text='{"key":"run"}', payload={"key": "run"}),
        ])
        headers = []

        def request(*args, **kwargs):
            headers.append(kwargs["headers"])
            return next(responses)

        module = types.SimpleNamespace(request=request)
        with patch.dict(sys.modules, {"requests": module}), patch("time.sleep"):
            run_key = client(max_retries=1, retry_backoff=0).create_job_run("job")
        self.assertEqual(run_key, "run")
        self.assertEqual(len(headers), 2)
        self.assertEqual(
            headers[0]["opc-retry-token"], headers[1]["opc-retry-token"]
        )

    def test_config_rejects_values_that_could_change_the_endpoint(self):
        invalid = [
            ("bad.region", "lake", "workspace", "20260430"),
            ("test-region-1", "", "workspace", "20260430"),
            ("test-region-1", "lake", "", "20260430"),
            ("test-region-1", "lake", "workspace", "../latest"),
        ]
        for region, platform, workspace, api_version in invalid:
            with self.subTest(region=region, api_version=api_version), self.assertRaises(ValueError):
                AidpConfig("P", region, platform, workspace, api_version)

    def test_job_run_request_validates_current_oracle_schema(self):
        value = client()
        invalid_parameters = [
            [{"name": "has space", "value": "x"}],
            [{"name": "ok", "value": 3}],
            [{"name": "ok", "value": "x", "future": True}],
        ]
        for parameters in invalid_parameters:
            with self.subTest(parameters=parameters), self.assertRaises(ValueError):
                value.create_job_run("job", parameters)
        with self.assertRaisesRegex(ValueError, "job key"):
            value.create_job_run("")
        with self.assertRaisesRegex(ValueError, "retry token"):
            value.create_job_run("job", retry_token="x" * 65)

    def test_repair_validates_task_key_limit(self):
        with self.assertRaisesRegex(ValueError, "task key"):
            client().repair_job_run("run", ["x" * 256])


if __name__ == "__main__":
    unittest.main()
