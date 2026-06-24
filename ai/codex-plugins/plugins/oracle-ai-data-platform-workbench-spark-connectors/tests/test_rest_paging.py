"""Unit tests for REST helpers (no live HTTP)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_connectors.rest import fusion
from oracle_ai_data_platform_connectors.rest.fusion import (
    FUSION_PAGE_LIMIT_HARD_CAP,
    fetch_paged,
)
from oracle_ai_data_platform_connectors.streaming.kafka import (
    bootstrap_for_region,
    build_kafka_options_sasl_plain,
    validate_checkpoint_path,
)


# --- Fusion REST paging -----------------------------------------------------


def _make_session_returning(pages):
    """Build a mock session that returns the given JSON responses in order."""
    session = MagicMock()
    responses = []
    for page in pages:
        resp = MagicMock()
        resp.json.return_value = page
        resp.raise_for_status.return_value = None
        responses.append(resp)
    session.get.side_effect = responses
    return session


def test_fetch_paged_handles_single_page():
    session = _make_session_returning([
        {"items": [{"id": 1}, {"id": 2}], "hasMore": False},
    ])
    rows = list(fetch_paged(session, "https://x", "/p"))
    assert rows == [{"id": 1}, {"id": 2}]


def test_fetch_paged_walks_until_no_more():
    session = _make_session_returning([
        {"items": [{"id": 1}], "hasMore": True},
        {"items": [{"id": 2}], "hasMore": True},
        {"items": [{"id": 3}], "hasMore": False},
    ])
    rows = list(fetch_paged(session, "https://x", "/p", limit=1))
    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_fetch_paged_caps_limit_at_499():
    """Caller asks for 1000; helper silently caps at 499."""
    session = _make_session_returning([{"items": [], "hasMore": False}])
    list(fetch_paged(session, "https://x", "/p", limit=1000))

    # First (and only) call: inspect the params Fusion got
    _, kwargs = session.get.call_args
    assert kwargs["params"]["limit"] == FUSION_PAGE_LIMIT_HARD_CAP


def test_fetch_paged_sets_only_data():
    session = _make_session_returning([{"items": [], "hasMore": False}])
    list(fetch_paged(session, "https://x", "/p"))
    _, kwargs = session.get.call_args
    assert kwargs["params"]["onlyData"] == "true"


def test_fetch_paged_passes_extra_params_and_fields():
    session = _make_session_returning([{"items": [], "hasMore": False}])
    list(fetch_paged(
        session, "https://x", "/p",
        fields="A,B,C",
        extra_params={"q": "Status='OK'"},
    ))
    _, kwargs = session.get.call_args
    assert kwargs["params"]["fields"] == "A,B,C"
    assert kwargs["params"]["q"] == "Status='OK'"


# --- Kafka helpers ----------------------------------------------------------


def test_bootstrap_for_region():
    assert bootstrap_for_region("us-ashburn-1") == (
        "streaming.us-ashburn-1.oci.oraclecloud.com:9092"
    )
    assert bootstrap_for_region("eu-frankfurt-1") == (
        "streaming.eu-frankfurt-1.oci.oraclecloud.com:9092"
    )


def test_kafka_sasl_plain_username_format():
    opts = build_kafka_options_sasl_plain(
        bootstrap_servers="streaming.us-ashburn-1.oci.oraclecloud.com:9092",
        tenancy_name="my-tenancy",
        username="alice@example.com",
        stream_pool_ocid="ocid1.streampool.oc1.iad.aaaa",
        auth_token="t-xxx",
        topic="events",
    )
    jaas = opts["kafka.sasl.jaas.config"]
    assert "PlainLoginModule required" in jaas
    assert (
        'username="my-tenancy/alice@example.com/'
        'ocid1.streampool.oc1.iad.aaaa"'
    ) in jaas
    assert 'password="t-xxx"' in jaas
    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "PLAIN"
    assert opts["subscribe"] == "events"


def test_validate_checkpoint_rejects_workspace():
    with pytest.raises(ValueError, match="cannot live on /Workspace"):
        validate_checkpoint_path("/Workspace/Shared/_checkpoints/x")


def test_validate_checkpoint_rejects_oci():
    with pytest.raises(ValueError, match="cannot use oci://"):
        validate_checkpoint_path("oci://my-bucket@ns/checkpoints")


def test_validate_checkpoint_rejects_random_paths():
    with pytest.raises(ValueError, match="under /Volumes"):
        validate_checkpoint_path("/tmp/cp")


def test_validate_checkpoint_accepts_volumes():
    p = "/Volumes/cat/sch/vol/_checkpoints/streaming"
    assert validate_checkpoint_path(p) == p
