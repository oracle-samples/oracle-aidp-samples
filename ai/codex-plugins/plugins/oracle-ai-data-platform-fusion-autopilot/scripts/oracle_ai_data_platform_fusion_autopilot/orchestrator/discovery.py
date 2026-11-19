"""BICC offering schema auto-discovery helper.

Hits ``/biacm/rest/meta/datastores`` once per orchestrator run + returns a
mapping of ``{datastore_name: {offering_schemas}}``. Preflight calls this
on ``DATA_ACCESS_LAYER_0031`` to find which BICC offering schema actually
contains a PVO, instead of forcing every new customer to edit the plugin's
catalog when their tenant publishes a PVO under a different offering.

Distinct from ``commands/catalog.py::_extract_datastore_names`` (which
collects bare datastore names without schema context): this helper needs the
schema/datastore pairing so it can auto-correct in the unique-match case and
present candidates in the ambiguous case.
"""
from __future__ import annotations

from typing import Any

import requests

from .errors import DiscoveryProbeError

# BICC response shapes vary across releases — these are the keys observed
# in live responses. The walker accepts any of them so the helper survives
# Oracle's metadata-shape evolution.

_DATASTORE_KEYS = frozenset({
    "name",
    "datastoreName",
    "viewObjectName",
    "dataStoreName",
})
_SCHEMA_KEYS = frozenset({
    "offeringName",
    "offering",
    "schemaName",
    "schema",
})


def _walk(
    node: Any,
    ancestor_schemas: frozenset[str],
    mapping: dict[str, set[str]],
) -> None:
    """Recursive walker over the BICC response.

    Carries the nearest-enclosing offering schema(s) down the dict tree
    via ``ancestor_schemas``. When a node has at least one datastore-name
    key AND there's at least one schema in the ancestry stack, yield each
    ``(schema, datastore_name)`` pair into ``mapping``.

    Datastores found without any schema in the ancestry are SILENTLY
    SKIPPED — a datastore without an offering classification can't help
    auto-discovery (would only inflate ``len(candidates) == 1`` counts
    and risk a wrong auto-correct).
    """
    if isinstance(node, dict):
        # Extend ancestor stack with any schema keys on THIS node.
        node_schemas = frozenset(
            str(node[k]) for k in _SCHEMA_KEYS
            if isinstance(node.get(k), str)
        )
        effective_schemas = ancestor_schemas | node_schemas

        # Pairing rule: datastore-name key + at least one schema in ancestry
        # (the schema may have been seen on a parent OR on this same node).
        if effective_schemas:
            for k in _DATASTORE_KEYS:
                v = node.get(k)
                if isinstance(v, str):
                    for sch in effective_schemas:
                        mapping.setdefault(v, set()).add(sch)

        for v in node.values():
            _walk(v, effective_schemas, mapping)
    elif isinstance(node, list):
        for item in node:
            _walk(item, ancestor_schemas, mapping)


def discover_pvo_schemas(
    service_url: str,
    username: str,
    password: str,
    *,
    timeout_s: int = 60,
) -> dict[str, set[str]]:
    """Hit ``/biacm/rest/meta/datastores`` once + return
    ``{datastore_name: {offering_schemas}}``.

    The returned mapping is the lookup shape preflight needs:

    - ``len(candidates) == 1`` → unique → auto-correct.
    - ``len(candidates) >= 2`` → ambiguous → raise with candidate list.
    - ``mapping.get(datastore, set())`` returns empty → not found → raise
      with "PVO renamed / not in subscription" hint.

    Args:
        service_url: Fusion pod base URL (e.g.
            ``https://fa-<pod>.ds-fa.oraclecloud.com``).
        username: BICC HTTP basic username.
        password: BICC HTTP basic password (plaintext — orchestrator
            resolves before calling; never logged).
        timeout_s: Network timeout per request.

    Returns:
        ``{datastore_name: {schema_name, ...}}``. Always returns a dict;
        callers use ``.get(datastore, set())`` for the "not found" case.

    Raises:
        DiscoveryProbeError: HTTP non-200 OR ``requests.RequestException``
            (network failure, connection refused, timeout). The caller —
            ``preflight_bronze_schemas`` — decides whether to surface this
            directly OR fold it into a ``BronzeSchemaProbeError``.
    """
    url = service_url.rstrip("/") + "/biacm/rest/meta/datastores"
    try:
        response = requests.get(
            url, auth=(username, password), timeout=timeout_s,
        )
    except requests.RequestException as exc:
        raise DiscoveryProbeError(
            f"BICC discovery probe failed for {url}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if response.status_code != 200:
        raise DiscoveryProbeError(
            f"BICC discovery probe returned HTTP {response.status_code} "
            f"for {url}: {response.text[:300]}"
        )

    try:
        body = response.json()
    except ValueError as exc:
        raise DiscoveryProbeError(
            f"BICC discovery probe returned non-JSON body for {url}: "
            f"{response.text[:200]}"
        ) from exc

    mapping: dict[str, set[str]] = {}
    _walk(body, frozenset(), mapping)
    return mapping


__all__ = ["discover_pvo_schemas", "DiscoveryProbeError"]
