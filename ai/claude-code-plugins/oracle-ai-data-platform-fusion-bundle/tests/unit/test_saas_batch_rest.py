"""Unit tests for the Fusion saas-batch REST extractor.

These tests mock requests.Session and verify the extractor builds the right
URLs, headers, JSON payloads, and handles each step's response correctly.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_bundle.extractors.saas_batch_rest import (
    fetch_output_files,
    get_token_relay,
    poll_job_until_complete,
    submit_extract_job,
)


# ---------------------------------------------------------------------------
# get_token_relay
# ---------------------------------------------------------------------------


def _mock_session(auth: tuple[str, str] | None = ("Casey.Brown", "pwd")):
    s = MagicMock()
    s.auth = auth
    return s


class TestGetTokenRelay:
    def test_calls_correct_url_with_audience_template(self) -> None:
        s = _mock_session()
        resp = MagicMock(text="abc.def.ghi")
        resp.raise_for_status = MagicMock()
        s.get.return_value = resp

        token = get_token_relay(s, host="my-pod.fa.example.com", pod_name="my-pod")
        assert token == "abc.def.ghi"

        call = s.get.call_args
        url = call.args[0] if call.args else call.kwargs["url"]
        params = call.kwargs["params"]
        assert url == "https://my-pod.fa.example.com/saas-batch/security/tokenrelay"
        assert params == {
            "username": "Casey.Brown",
            "audience": "urn:opc:resource:fusion:pod:name=my-pod",
            "scope": "*",
        }

    def test_handles_json_wrapped_token(self) -> None:
        s = _mock_session()
        resp = MagicMock(text='{"token": "xyz123"}')
        resp.raise_for_status = MagicMock()
        s.get.return_value = resp
        assert get_token_relay(s, host="h", pod_name="p") == "xyz123"

    def test_handles_access_token_key(self) -> None:
        s = _mock_session()
        resp = MagicMock(text='{"access_token": "via-access-token"}')
        resp.raise_for_status = MagicMock()
        s.get.return_value = resp
        assert get_token_relay(s, host="h", pod_name="p") == "via-access-token"

    def test_raises_on_empty_response(self) -> None:
        s = _mock_session()
        resp = MagicMock(text="   \n  ")
        resp.raise_for_status = MagicMock()
        s.get.return_value = resp
        with pytest.raises(ValueError, match="empty response"):
            get_token_relay(s, host="h", pod_name="p")

    def test_requires_session_auth(self) -> None:
        s = _mock_session(auth=None)
        with pytest.raises(ValueError, match="session.auth must be set"):
            get_token_relay(s, host="h", pod_name="p")


# ---------------------------------------------------------------------------
# submit_extract_job
# ---------------------------------------------------------------------------


class TestSubmitExtractJob:
    def test_posts_correct_payload(self) -> None:
        s = MagicMock()
        resp = MagicMock(headers={"Location": "https://h/api/saas-batch/jobScheduler/v1/jobRequests/777"})
        resp.raise_for_status = MagicMock()
        s.post.return_value = resp

        location = submit_extract_job(
            s,
            host="my-pod.fa.example.com",
            resource_path="workerAssignmentExtracts",
        )
        assert location.endswith("/jobRequests/777")

        call = s.post.call_args
        url = call.args[0] if call.args else call.kwargs["url"]
        payload = call.kwargs["json"]
        assert url == "https://my-pod.fa.example.com/api/saas-batch/jobScheduler/v1/jobRequests"
        # Per pdf2 p4:
        assert payload["jobDefinitionName"] == "AsyncDataExtraction"
        assert payload["serviceName"] == "boss"
        assert payload["requestParameters"]["boss.resource.path"] == "workerAssignmentExtracts"
        assert payload["requestParameters"]["boss.resource.version"] == "11"
        assert payload["requestParameters"]["boss.outputFormat"] == "json"
        assert "boss.maxAdvanceQuery" not in payload["requestParameters"]

    def test_advance_query_is_serialized(self) -> None:
        s = MagicMock()
        resp = MagicMock(headers={"Location": "loc"})
        resp.raise_for_status = MagicMock()
        s.post.return_value = resp

        submit_extract_job(
            s,
            host="h",
            resource_path="x",
            advance_query={"collection": [{"filter": "primaryFlag = true"}]},
        )

        body = s.post.call_args.kwargs["json"]
        raw = body["requestParameters"]["boss.maxAdvanceQuery"]
        assert json.loads(raw) == {"collection": [{"filter": "primaryFlag = true"}]}

    def test_raises_when_location_missing(self) -> None:
        s = MagicMock()
        resp = MagicMock(headers={})  # no Location header
        resp.raise_for_status = MagicMock()
        s.post.return_value = resp
        with pytest.raises(KeyError, match="Location"):
            submit_extract_job(s, host="h", resource_path="x")


# ---------------------------------------------------------------------------
# poll_job_until_complete
# ---------------------------------------------------------------------------


def _job_response(status: str, request_id: str | None = "REQ-1"):
    r = MagicMock()
    r.raise_for_status = MagicMock()
    details = {"jobStatus": status}
    if request_id:
        details["jobRequestId"] = request_id
    r.json.return_value = {"jobDetails": [details]}
    return r


class TestPollJobUntilComplete:
    def test_returns_completed_details(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No real sleeping in the test
        import oracle_ai_data_platform_fusion_bundle.extractors.saas_batch_rest as mod
        monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_kw: None)

        s = MagicMock()
        s.get.side_effect = [
            _job_response("RUNNING"),
            _job_response("RUNNING"),
            _job_response("COMPLETED", request_id="ABC-42"),
        ]

        details = poll_job_until_complete(s, "https://h/loc", poll_interval_seconds=0)
        assert details["jobStatus"] == "COMPLETED"
        assert details["jobRequestId"] == "ABC-42"
        assert s.get.call_count == 3

    def test_raises_on_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import oracle_ai_data_platform_fusion_bundle.extractors.saas_batch_rest as mod
        monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_kw: None)

        s = MagicMock()
        s.get.return_value = _job_response("FAILED")
        with pytest.raises(RuntimeError, match="FAILED"):
            poll_job_until_complete(s, "https://h/loc", poll_interval_seconds=0)

    def test_raises_on_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import oracle_ai_data_platform_fusion_bundle.extractors.saas_batch_rest as mod
        # Force time to march forward past deadline
        ticks = iter([1000.0, 1000.5, 99999.0])
        monkeypatch.setattr(mod.time, "time", lambda: next(ticks))
        monkeypatch.setattr(mod.time, "sleep", lambda *_a, **_kw: None)

        s = MagicMock()
        s.get.return_value = _job_response("RUNNING")
        with pytest.raises(TimeoutError):
            poll_job_until_complete(s, "https://h/loc", poll_interval_seconds=0, timeout_seconds=10)


# ---------------------------------------------------------------------------
# fetch_output_files
# ---------------------------------------------------------------------------


class TestFetchOutputFiles:
    def test_correct_url_used(self) -> None:
        s = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = [{"url": "https://h/file/0", "size": 12345}]
        s.get.return_value = resp

        out = fetch_output_files(s, host="my-pod.fa.example.com", job_request_id="ABC-42")
        url = s.get.call_args.args[0]
        assert url == "https://my-pod.fa.example.com/api/saas-batch/jobFileManager/v1/jobRequests/ABC-42/outputFiles"
        assert len(out) == 1
        assert out[0]["url"] == "https://h/file/0"

    def test_handles_dict_response_with_items(self) -> None:
        s = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"items": [{"url": "u1"}, {"url": "u2"}]}
        s.get.return_value = resp
        out = fetch_output_files(s, host="h", job_request_id="r")
        assert [f["url"] for f in out] == ["u1", "u2"]

    def test_returns_empty_for_unknown_shape(self) -> None:
        s = MagicMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"unrecognized": "shape"}
        s.get.return_value = resp
        assert fetch_output_files(s, host="h", job_request_id="r") == []
