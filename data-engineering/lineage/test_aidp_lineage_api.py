"""
Conformance tests for the AIDP Semantic Catalog **lineage** API.

Purpose: prove, from live responses, that the lineage feature is actually released and
reachable — and separately, report whether the lineage *graph* is populated for the
tables under test.

Observed 2026-08-16 against one AI Data Platform instance in us-ashburn-1, SDK v4.2.1.
The Part B result below is that one observation, not a statement about the service.

The suite is deliberately split:

  Part A — API EXISTENCE (must be green)
      Proves the endpoint is deployed, authenticated, and enforcing its documented
      request contract. This is the part that answers "is lineage released?". It probes
      with UNRESOLVABLE_ANCHOR so that it stays green whether or not the graph is
      populated -- it must not depend on Part B's anchor continuing to fail.

  Part B — GRAPH POPULATION (xfail = known open gap)
      Proves whether lineage *data* exists for the tables under test. These are marked
      xfail because no `anchorNode` value was accepted at the time of observation. They
      are NOT deleted or skipped: if the graph is populated, or the node-id format turns
      out to be documented, they flip to XPASS and the suite reports that the gap closed.

Run (AIDP_DATALAKE must be exported -- there is no default):
    export AIDP_DATALAKE=ocid1.aidataplatform.oc1.<region>.<unique-id>

    pytest test_aidp_lineage_api.py -v
    pytest test_aidp_lineage_api.py -v -m existence     # just the proof-of-release
    pytest test_aidp_lineage_api.py -v -rx              # xfail reasons: why Part B is blocked
    pytest test_aidp_lineage_api.py -rP -k B0           # B0's anchor-candidate matrix
    pytest test_aidp_lineage_api.py -m "existence and not legacy"   # skip the legacy host

Requires: oci, requests, pytest  ·  a working ~/.oci/config profile  ·  AIDP_DATALAKE.
"""
import json
import os

import oci
import pytest
import requests

# --------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------
PROFILE = os.environ.get("AIDP_PROFILE", "DEFAULT")
REGION = os.environ.get("AIDP_REGION", "us-ashburn-1")
# Required: the OCID of YOUR AI Data Platform instance. There is deliberately no
# default -- a default would point every reader's signed requests at someone else's
# resource and fail with an ambiguous 404.
DATALAKE = os.environ.get("AIDP_DATALAKE")
if not DATALAKE:
    raise RuntimeError(
        "AIDP_DATALAKE is not set. Export the OCID of your own AI Data Platform "
        "instance before running this suite, e.g.\n"
        "    export AIDP_DATALAKE=ocid1.aidataplatform.oc1.<region>.<unique-id>\n"
        "See README.md for the full list of required environment variables."
    )

SCHEMA_KEY = os.environ.get("AIDP_SCHEMA", "default.lin_demo")
ANCHOR_TABLE = os.environ.get("AIDP_ANCHOR_TABLE", "%s.mart_customer_revenue" % SCHEMA_KEY)

# Part A proves the route exists and validates its body; it must stay green whether or not
# the lineage graph is populated. It therefore probes with an anchor that can never resolve,
# rather than with ANCHOR_TABLE -- which Part B expects to start returning 200 the day the
# graph is populated. Using the same value for both would turn that success into Part A
# failures. Part B passes ANCHOR_TABLE explicitly.
UNRESOLVABLE_ANCHOR = "aidp-samples-nonexistent-node-do-not-create"

# The lineage API lives on the DATA-PLANE host at API version 20260430 — NOT on
# aidp.{region}.../20240831, which is the older generation with no lineage surface.
DP_HOST = "https://datalake.%s.oci.oraclecloud.com" % REGION
DP_VERSION = "20260430"
DP_BASE = "%s/%s/aiDataPlatforms/%s" % (DP_HOST, DP_VERSION, DATALAKE)

LEGACY_BASE = "https://aidp.%s.oci.oraclecloud.com/20240831/dataLakes/%s" % (REGION, DATALAKE)

# Single attempt, no retry. requests' HTTPAdapter(max_retries=...) would not help here:
# it does not retry a POST that times out while reading the response, which is the case
# that actually bites. Re-run the suite instead of silently retrying a mutating-looking call.
TIMEOUT = 60


# --------------------------------------------------------------------------------------
# Signed-request helpers
# --------------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def signer():
    cfg = oci.config.from_file(profile_name=PROFILE)
    oci.config.validate_config(cfg)
    return oci.signer.Signer(
        tenancy=cfg["tenancy"],
        user=cfg["user"],
        fingerprint=cfg["fingerprint"],
        private_key_file_location=cfg["key_file"],
        pass_phrase=cfg.get("pass_phrase"),
    )


@pytest.fixture(scope="session")
def unresolvable_message(signer):
    """The server's own wording for an anchorNode it cannot resolve, captured once.

    Pinning the literal string "Invalid anchorNode" made A4/A5 fail on any Preview-API
    message change that merely echoed the value back. Capturing it from one probe and
    comparing the rest against that keeps the discrimination the tests are actually
    about -- reached anchor resolution vs. rejected earlier -- without pinning wording.
    """
    code, body = fetch_lineage(signer)
    assert code == 400, "expected 400 for an unresolvable anchor, got %s: %s" % (code, body)
    msg = body.get("message") if isinstance(body, dict) else str(body)
    assert msg, "server returned no message for an unresolvable anchor: %s" % body
    return msg


def _req(signer, method, url, body=None):
    """Signed request -> (status_code, parsed_json_or_text)."""
    kwargs = {"timeout": TIMEOUT, "auth": signer}
    if body is not None:
        kwargs["data"] = json.dumps(body)
        kwargs["headers"] = {"Content-Type": "application/json"}
    r = requests.request(method, url, **kwargs)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, r.text


def fetch_lineage(signer, **overrides):
    """POST actions/fetchLineage with a valid-by-schema body."""
    body = {
        "anchorNode": UNRESOLVABLE_ANCHOR,
        "maxDepth": 3,
        "level": "ENTITY",
        "direction": "BOTH",
        "shouldIncludeEdges": True,
    }
    body.update(overrides)
    body = {k: v for k, v in body.items() if v is not _OMIT}
    return _req(signer, "POST", DP_BASE + "/actions/fetchLineage", body)


class _Omit:
    def __repr__(self):
        return "<omitted>"


_OMIT = _Omit()


# ======================================================================================
# Part A — API EXISTENCE.  These prove the lineage feature is released and live.
# ======================================================================================


@pytest.mark.existence
def test_A0_auth_works_on_dataplane_host(signer):
    """Baseline: we are authenticated on the data-plane host.

    Without this, a 4xx from the lineage route would be ambiguous (auth vs route).
    """
    code, body = _req(signer, "GET", DP_BASE + "/catalogs?limit=1")
    assert code == 200, "expected 200 listing catalogs, got %s: %s" % (code, body)


@pytest.mark.existence
def test_A1_fetchLineage_route_exists(signer, unresolvable_message):
    """POST actions/fetchLineage is DEPLOYED.

    A deployed-but-validating route answers 400 InvalidParameter. A missing route
    answers 404 NotAuthorizedOrNotFound (see test_A2 for the control).

    Probed with UNRESOLVABLE_ANCHOR, not ANCHOR_TABLE: this assertion is about the route
    being deployed, so it must not start failing when the graph is populated.
    """
    code, body = fetch_lineage(signer)
    assert code != 404, "lineage route missing (404): %s" % body
    assert code == 400, "expected 400 from body validation, got %s: %s" % (code, body)
    assert body.get("code") == "InvalidParameter", body
    # It reached the operation's own parameter validation — proof of a real handler.
    assert body.get("message") == unresolvable_message, body


@pytest.mark.existence
def test_A2_control_bogus_action_is_404(signer):
    """Control that gives test_A1 its meaning.

    A nonexistent sibling action under the same base returns 404, so A1's 400 is a
    genuine discrimination between "route exists" and "route absent" — not an artifact
    of how this service reports errors.
    """
    code, body = _req(
        signer, "POST", DP_BASE + "/actions/zzzNotARealLineageAction", {"anchorNode": "x"}
    )
    assert code == 404, "expected 404 for a bogus action, got %s: %s" % (code, body)


@pytest.mark.existence
def test_A3_exportLineage_route_exists(signer):
    """The second documented lineage operation (CSV export) is also deployed."""
    code, body = _req(
        signer,
        "POST",
        DP_BASE + "/actions/exportLineage",
        {"anchorNode": UNRESOLVABLE_ANCHOR, "direction": "UPSTREAM"},
    )
    assert code != 404, "exportLineage route missing (404): %s" % body
    assert code == 400 and body.get("code") == "InvalidParameter", (code, body)


@pytest.mark.existence
def test_A4_request_contract_is_enforced_server_side(signer, unresolvable_message):
    """The server enforces the documented schema, distinguishing missing from invalid.

    Omitting anchorNode yields a *different* message than supplying a bad one. Only a
    real implementation of this operation can make that distinction.
    """
    code_missing, body_missing = fetch_lineage(signer, anchorNode=_OMIT)
    assert code_missing == 400, (code_missing, body_missing)
    assert "must not be null" in body_missing.get("message", ""), body_missing

    code_bad, body_bad = fetch_lineage(signer, anchorNode=UNRESOLVABLE_ANCHOR)
    assert code_bad == 400, (code_bad, body_bad)
    assert body_bad.get("message") == unresolvable_message, body_bad

    assert body_missing["message"] != body_bad["message"], (
        "server must distinguish missing from invalid anchorNode"
    )


@pytest.mark.existence
@pytest.mark.parametrize(
    "field,value",
    [
        ("level", "COLUMN"),          # column-level lineage is a released capability
        ("level", "ENTITY"),
        ("direction", "UPSTREAM"),
        ("direction", "DOWNSTREAM"),
        ("direction", "BOTH"),
    ],
)
def test_A5_documented_enums_are_accepted(signer, unresolvable_message, field, value):
    """Documented enum values pass schema validation.

    Each reaches anchorNode resolution ("Invalid anchorNode") rather than being rejected
    as a bad enum — so the server implements these options, including level=COLUMN.

    Uses UNRESOLVABLE_ANCHOR so the enum check is independent of whether any real table
    resolves; reaching anchor resolution at all is what proves the enum parsed.
    """
    code, body = fetch_lineage(signer, **{field: value})
    assert code == 400, (code, body)
    assert body.get("message") == unresolvable_message, (
        "%s=%s was rejected before anchor resolution: %s" % (field, value, body)
    )


@pytest.mark.existence
def test_A6_invalid_enum_is_rejected(signer, unresolvable_message):
    """Complement to A5: a bogus enum fails *earlier* than anchor resolution.

    This proves A5 is meaningful — the server really parses these fields rather than
    ignoring them and always complaining about anchorNode.
    """
    code, body = fetch_lineage(signer, direction="SIDEWAYS")
    assert code == 400, (code, body)
    assert body.get("message") != unresolvable_message, (
        "bogus enum should be rejected as an enum, not fall through to anchor: %s" % body
    )


@pytest.mark.existence
@pytest.mark.legacy
def test_A7_lineage_absent_from_legacy_api_generation(signer):
    """Documents *why* lineage looks missing if you probe the old surface.

    The previous generation (aidp.{region} + /20240831/dataLakes) has no lineage route,
    while still serving /catalogs. Anyone concluding "AIDP has no lineage API" from that
    host is probing a generation behind.

    Marked `legacy` as well as `existence`: it is the one test that depends on the
    previous generation staying up, so `-m "existence and not legacy"` skips it when that
    gateway is retired without losing the rest of Part A.
    """
    code_cat, _ = _req(signer, "GET", LEGACY_BASE + "/catalogs")
    assert code_cat == 200, "legacy base should still serve catalogs (%s)" % code_cat

    code_lin, _ = _req(signer, "GET", LEGACY_BASE + "/lineage")
    assert code_lin == 404, "legacy generation unexpectedly has /lineage (%s)" % code_lin


# ======================================================================================
# Part B — GRAPH POPULATION.  Known open gap: no anchorNode value is accepted yet.
# ======================================================================================
ANCHOR_CANDIDATES = [
    "default.lin_demo.mart_customer_revenue",           # table key (as ListTables returns)
    "mart_customer_revenue",                            # bare display name
    "hive.lin_demo.mart_customer_revenue",              # catalogGuid-qualified
    "default.cat/lin_demo.db/mart_customer_revenue",    # storage-style path
    "TABLE:default.lin_demo.mart_customer_revenue",     # type-prefixed
    DATALAKE,                                           # the platform OCID itself
]

BLOCKED = (
    "No accepted anchorNode format known (observed 2026-08-16, one DataLake in "
    "us-ashburn-1, SDK v4.2.1): every candidate returns 400 'Invalid anchorNode'. "
    "Either the lineage graph is not populated for that DataLake, or the node-id format "
    "is undocumented -- the CLI reference lists anchorNode with an empty description. "
    "Not yet separated."
)


@pytest.mark.population
def test_B0_report_all_anchor_candidates(signer):
    """Diagnostic, always green: records what every candidate id form returns.

    The tripwire that shows the moment any form starts resolving. Always passes, so its
    output is captured rather than displayed -- use `-rP -k B0` (or `-s`) to read it.
    """
    results = {}
    for cand in ANCHOR_CANDIDATES:
        code, body = fetch_lineage(signer, anchorNode=cand)
        msg = body.get("message") if isinstance(body, dict) else str(body)[:80]
        results[cand] = (code, msg)

    print("\n  anchorNode candidate probe:")
    for cand, (code, msg) in results.items():
        # The OCID candidate is redacted: this output is meant to be pasteable.
        shown = "<the DataLake OCID>" if cand == DATALAKE else cand[:52]
        print("    %-52s -> %s %s" % (shown, code, msg))

    accepted = [c for c, (code, _) in results.items() if code == 200]
    if accepted:
        print("\n  *** an anchorNode form is NOW ACCEPTED: %s ***" % accepted)
    # Always passes: this test reports, it does not gate.
    assert results, "expected probe results"


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B1_entity_lineage_returns_graph(signer):
    """Entity-level lineage for a known table returns nodes (and edges)."""
    code, body = fetch_lineage(
        signer, anchorNode=ANCHOR_TABLE, level="ENTITY", direction="BOTH"
    )
    assert code == 200, "fetchLineage failed: %s %s" % (code, body)
    assert "nodes" in body, "EntityLineage must carry nodes: %s" % body
    assert body["nodes"], "lineage graph is empty for %s" % ANCHOR_TABLE


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B2_upstream_contains_expected_sources(signer):
    """UPSTREAM of the mart reaches stg_orders and raw_customers.

    Mirrors the DAG asserted from the Spark plan in Verify_Data_Lineage.ipynb, so a green
    run here means the platform's own graph agrees with what actually executed.
    """
    code, body = fetch_lineage(
        signer, anchorNode=ANCHOR_TABLE, level="ENTITY", direction="UPSTREAM", maxDepth=3
    )
    assert code == 200, (code, body)
    names = {n.get("qualifiedName") or n.get("displayName") for n in body.get("nodes", [])}
    assert any("stg_orders" in (n or "") for n in names), names
    assert any("raw_customers" in (n or "") for n in names), names


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B3_column_level_lineage_returns_links(signer):
    """COLUMN-level lineage returns column nodes and edges.

    Target claim: mart.revenue traces back to stg_orders.amount.
    """
    code, body = fetch_lineage(
        signer,
        anchorNode=ANCHOR_TABLE,
        level="COLUMN",
        direction="UPSTREAM",
        maxDepth=3,
        shouldIncludeEdges=True,
    )
    assert code == 200, (code, body)
    assert body.get("links"), "expected column-level edges: %s" % body


@pytest.mark.population
@pytest.mark.xfail(reason=BLOCKED, strict=False)
def test_B4_export_lineage_returns_csv(signer):
    """exportLineage returns a CSV document for the anchor."""
    code, body = _req(
        signer,
        "POST",
        DP_BASE + "/actions/exportLineage",
        {"anchorNode": ANCHOR_TABLE, "direction": "UPSTREAM"},
    )
    assert code == 200, (code, body)
    assert isinstance(body, str) and "," in body, "expected CSV text, got: %r" % (body,)
