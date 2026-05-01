"""OAC REST API client (Bearer-token authenticated).

Wraps Oracle's documented public ``/api/20210901/...`` endpoints used by the
bundle's ``dashboard install`` / ``dashboard uninstall`` flow.

## Endpoint inventory (all from the canonical openapi.json, 2026-05-01)

Connection lifecycle (per-object, stable):

  * ``POST   /api/20210901/catalog/connections``
      Register the AIDP JDBC connection. The body schema is documented as
      ``type: object`` (server-side schema is open), so the AIDP-specific
      ``connectionType: "idljdbc"`` payload — captured from the OAC UI's
      network traffic (TC10h, 2026-05-01) — works at the wire layer.
      Note: AIDP is NOT in Oracle's 11 published connectionType samples;
      the body shape is reverse-engineered, not Oracle-blessed. See
      ``project_oac_aidp_rest_create_connection_payload.md``.
  * ``GET    /api/20210901/catalog?type=connections&search=<name>``
      Find connection by display name (the documented browse endpoint —
      NOT ``/catalog/connections``, which is POST-only).
  * ``GET    /api/20210901/catalog/connections/{connectionId}``
  * ``PUT    /api/20210901/catalog/connections/{connectionId}``
  * ``DELETE /api/20210901/catalog/connections/{connectionId}``
      Path-param ``connectionId`` is **Base64URL-encoded object ID**
      (e.g. ``'<owner>'.'<connection_name>'`` -> base64url), NOT the
      plain connection name.

Snapshot lifecycle (instance-level, stable — the ONLY public path for
deploying workbook content programmatically):

  * ``POST   /api/20210901/snapshots``
      Register a pre-uploaded ``.bar`` (or take one); async, returns 202.
  * ``GET    /api/20210901/snapshots``
  * ``GET    /api/20210901/snapshots/{id}``
  * ``DELETE /api/20210901/snapshots/{id}``
  * ``POST   /api/20210901/system/actions/restoreSnapshot``
      Restore a registered snapshot; async, returns 202 with
      ``oa-work-request-id`` header.
  * ``GET    /api/20210901/workRequests/{id}``
      Poll until status is SUCCEEDED / FAILED / CANCELED.

## Removed in TC10h-2 refactor (2026-05-01)

  * ``import_workbook`` — endpoint ``/catalog/workbooks/imports`` is NOT
    in Oracle's openapi.json (UI-only, no API stability guarantee).
    Replaced with snapshot ``register_snapshot`` + ``restore_snapshot``.
  * ``export_workbook`` — the documented ``/catalog/workbooks/{id}/exports``
    only exports PDF/PNG of canvases (async work-request), NOT ``.dva``.
    For per-workbook content distribution, snapshots are the documented
    path; ``.dva`` export is UI-only.
  * ``delete_workbook`` — no public DELETE for workbooks. Use folder
    cascade-delete.

Auth: each call attaches ``Authorization: Bearer <token>`` from
:class:`~.oauth.OacOauthFlow`. The token is cached and re-used across
calls until it expires; expiry triggers a refresh-token round-trip.

Reference: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/rest-endpoints.html
"""

from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Any, Literal

import requests

from .connection import AidpConnectionPayload
from .oauth import OacOauthFlow


# ----------------------------------------------------------------- helpers
def encode_catalog_id(plain_object_id: str) -> str:
    """Encode a catalog object's plain ID as Base64URL (Oracle's path-param shape).

    Oracle's catalog endpoints take a path parameter that is the **Base64URL
    encoding** of the catalog object's full quoted ID, e.g.
    ``'admin'.'oracle_ailakehouse_walletless'`` becomes
    ``J2FkbWluJy4nb3JhY2xlX2FpbGFrZWhvdXNlX3dhbGxldGxlc3Mn``. Documented in
    [Delete a connection]
    (https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/op-20210901-catalog-connections-connectionid-delete.html).

    Note: this returns the URL-safe Base64 with ``+/`` mapped to ``-_`` and
    no trailing ``=`` padding — exactly what Oracle's URL templates expect.
    """
    return base64.urlsafe_b64encode(plain_object_id.encode("utf-8")).rstrip(b"=").decode("ascii")


# ---------------------------------------------------------------- exceptions
class OacRestError(RuntimeError):
    """Wraps an OAC REST response that returned non-2xx."""

    def __init__(self, message: str, *, response: requests.Response | None = None) -> None:
        super().__init__(message)
        self.response = response


# ------------------------------------------------------------ work-request
class WorkRequestStatus:
    """OAC's documented work-request status enum."""
    ACCEPTED = "ACCEPTED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELING = "CANCELING"
    CANCELED = "CANCELED"
    TERMINAL = frozenset({SUCCEEDED, FAILED, CANCELED})


# ------------------------------------------------------------------ client
class OacRestClient:
    """Bearer-token-authenticated client for OAC's public REST API.

    Args:
        oac_url: Base URL of the OAC instance (e.g. ``https://oacai.example.com``).
        token_fetcher: Configured :class:`OacOauthFlow` to obtain user-context Bearer tokens.
        api_version: Path prefix segment (default ``20210901``).
        timeout: Per-request timeout in seconds.
        session: Optional pre-configured ``requests.Session`` for retries / rate-limiting / tests.
    """

    def __init__(
        self,
        oac_url: str,
        token_fetcher: OacOauthFlow,
        *,
        api_version: str = "20210901",
        timeout: int = 60,
        session: requests.Session | None = None,
    ) -> None:
        if not oac_url.startswith(("http://", "https://")):
            raise ValueError(f"oac_url must include scheme: got {oac_url!r}")
        self._base_url = oac_url.rstrip("/")
        self._api_root = f"{self._base_url}/api/{api_version}"
        self._token_fetcher = token_fetcher
        self._timeout = timeout
        self._session = session or requests.Session()

    # ---------------------------------------------------------------- helpers
    def _auth_headers(self, *, force_refresh: bool = False) -> dict[str, str]:
        token = self._token_fetcher.get_token(force_refresh=force_refresh)
        return {"Authorization": f"Bearer {token}"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        files: Any = None,
        data: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        url = f"{self._api_root}{path}"
        headers = self._auth_headers()
        if extra_headers:
            headers.update(extra_headers)
        if json_body is not None and files is None:
            headers["Content-Type"] = "application/json"
        response = self._session.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            files=files,
            data=data,
            headers=headers,
            timeout=self._timeout,
        )
        if response.status_code == 401:
            # Token may have expired or been rotated; one retry with a fresh token.
            headers = self._auth_headers(force_refresh=True)
            if extra_headers:
                headers.update(extra_headers)
            if json_body is not None and files is None:
                headers["Content-Type"] = "application/json"
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=json_body,
                files=files,
                data=data,
                headers=headers,
                timeout=self._timeout,
            )
        return response

    # ------------------------------------------------------------ connections
    def list_connections(self, *, search: str | None = None) -> list[dict[str, Any]]:
        """``GET /api/<v>/catalog?type=connections[&search=<term>]`` — list connections.

        Per Oracle's openapi.json, the documented connection-list endpoint is the
        generic ``/catalog`` browse with ``type=connections`` filter (NOT
        ``/catalog/connections`` which is POST-only).
        """
        params: dict[str, Any] = {"type": "connections"}
        if search:
            params["search"] = search
        response = self._request("GET", "/catalog", params=params)
        if response.status_code != 200:
            raise OacRestError(
                f"list_connections failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        body = response.json()
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("items", "connections", "results"):
                items = body.get(key)
                if isinstance(items, list):
                    return items
        return []

    def find_connection(self, name: str) -> dict[str, Any] | None:
        """Return the connection record whose display name matches ``name``, or ``None``."""
        for conn in self.list_connections(search=name):
            if conn.get("name") == name or conn.get("displayName") == name:
                return conn
        return None

    def create_connection(
        self,
        *,
        name: str,
        payload: AidpConnectionPayload,
        private_key_pem_path: Path | str,
        description: str | None = None,
        catalog: str = "fusion_catalog",
        connection_type: str = "idljdbc",
    ) -> dict[str, Any]:
        """``POST /api/<v>/catalog/connections`` — register the AIDP JDBC connection.

        Schema captured live 2026-05-01 from the OAC UI's actual create POST
        (Chrome DevTools network interceptor on ``oacai.cealinfra.com``):

          - The discriminator is ``provider-name: "idljdbc"`` (Intelligent
            DataLake JDBC). Aliased here as ``connectionType`` to match
            Oracle's documented public REST envelope vocabulary.
          - The PEM is **inlined** as a string field with literal ``\\n``
            escapes, NOT a separate cert file upload.
          - All inner field names are kebab-case (``private-key``, ``auth-type``)
            EXCEPT ``idlocid`` which is lowercase-no-separator.

        Args:
            name: Human-readable connection name (e.g. ``aidp_fusion_jdbc``).
            payload: 6-key AIDP detail payload from :func:`build_payload`.
            private_key_pem_path: Path to the RSA private key. Read into memory
                and inlined into the JSON.
            description: Optional connection description.
            catalog: AIDP catalog name (default ``fusion_catalog``).
            connection_type: OAC's internal provider discriminator. Default
                ``idljdbc`` (verified live 2026-05-01 via UI capture). NOT in
                Oracle's published 11 connectionType samples — use
                ``--connection-type`` to override if Oracle later publishes
                an official AIDP sample.

        Returns:
            ``{"connectionId": "<base64url>"}`` on success (Oracle-documented response).

        Raises:
            OacRestError: if OAC rejects the request.
        """
        pem_path = Path(private_key_pem_path)
        if not pem_path.exists():
            raise FileNotFoundError(f"private key PEM not found: {pem_path}")

        pem_text = pem_path.read_text(encoding="utf-8").strip()

        d = payload.to_dict()
        connection_params: dict[str, Any] = {
            "connectionType": connection_type,        # public REST envelope key
            "provider-name": connection_type,         # UI-internal duplicate
            "username": d["username"],
            "tenancy": d["tenancy"],
            "region": d["region"],
            "fingerprint": d["fingerprint"],
            "idlocid": d["idl-ocid"],                 # rename: idl-ocid -> idlocid
            "dsn": d["dsn"],
            "auth-type": "APIKey",
            "private-key": pem_text,                  # PEM inlined
            "catalog": catalog,
        }

        envelope: dict[str, Any] = {
            "version": "2.0.0",
            "type": "connection",
            "name": name,
            "content": {"connectionParams": connection_params},
        }
        if description:
            envelope["description"] = description

        response = self._request("POST", "/catalog/connections", json_body=envelope)

        if response.status_code not in (200, 201):
            raise OacRestError(
                f"create_connection failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json() if response.text else {}

    def delete_connection(self, connection_id_or_name: str, *, owner: str | None = None) -> bool:
        """``DELETE /api/<v>/catalog/connections/{base64url(objectId)}``.

        Per Oracle's docs, the path-param is the **Base64URL** encoding of the
        catalog object's plain ID ``'<owner>'.'<connection_name>'``. This helper
        accepts either:

          * A pre-encoded ``connectionId`` (already Base64URL — returned by
            :meth:`create_connection`).
          * A plain connection name + ``owner`` argument; we Base64URL-encode
            ``'<owner>'.'<name>'`` for you.

        Returns ``True`` on 2xx, ``False`` on 404.
        """
        # Heuristic: if it looks like a base64url string (no ``.`` or quotes), use as-is;
        # otherwise treat as a plain name and encode with the owner.
        if "." in connection_id_or_name or "'" in connection_id_or_name:
            connection_id = encode_catalog_id(connection_id_or_name)
        elif owner is None and len(connection_id_or_name) >= 16 and "_" in connection_id_or_name + "-":
            # Already encoded
            connection_id = connection_id_or_name
        elif owner:
            connection_id = encode_catalog_id(f"'{owner}'.'{connection_id_or_name}'")
        else:
            # Best-effort: assume already encoded
            connection_id = connection_id_or_name

        response = self._request("DELETE", f"/catalog/connections/{connection_id}")
        if response.status_code == 404:
            return False
        if response.status_code not in (200, 204):
            raise OacRestError(
                f"delete_connection failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return True

    # -------------------------------------------------------------- snapshots
    def register_snapshot(
        self,
        *,
        name: str,
        bucket: str,
        bar_uri: str,
        storage_type: Literal["OCI_NATIVE"] = "OCI_NATIVE",
        auth_type: Literal["OCI_RESOURCE_PRINCIPAL"] = "OCI_RESOURCE_PRINCIPAL",
        password: str | None = None,
    ) -> dict[str, Any]:
        """``POST /api/<v>/snapshots`` (REGISTER) — register a pre-uploaded ``.bar``.

        Per Oracle's [Snapshot REST API Prerequisites]
        (https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/prerequisites.html),
        the BAR must already live in OCI Object Storage; this call registers it
        with OAC so a subsequent restore knows where to read.

        Args:
            name: Display name for the registered snapshot (any string).
            bucket: OCI Object Storage bucket containing the ``.bar``.
            bar_uri: Object name (relative path within the bucket) of the ``.bar``.
            storage_type: ``"OCI_NATIVE"`` (Object Storage). Other values are
                possible per the OpenAPI but not commonly used.
            auth_type: How OAC authenticates to Object Storage. ``"OCI_RESOURCE_PRINCIPAL"``
                requires the customer to have set up a Resource Principal grant per
                the prerequisites doc.
            password: Optional BAR password. If the BAR was created with a password,
                pass it here so OAC can decrypt during restore.

        Returns:
            The registered snapshot record (includes the ``id`` you'll pass to
            :meth:`restore_snapshot`).

        Raises:
            OacRestError: if OAC rejects the request.
        """
        body: dict[str, Any] = {
            "type": "REGISTER",
            "name": name,
            "storage": {
                "type": storage_type,
                "bucket": bucket,
                "auth": {"type": auth_type},
            },
            "bar": {"uri": bar_uri},
        }
        if password:
            body["password"] = password

        response = self._request("POST", "/snapshots", json_body=body)
        if response.status_code not in (200, 201, 202):
            raise OacRestError(
                f"register_snapshot failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json() if response.text else {}

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """``GET /api/<v>/snapshots/{id}``."""
        response = self._request("GET", f"/snapshots/{snapshot_id}")
        if response.status_code != 200:
            raise OacRestError(
                f"get_snapshot failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json()

    def list_snapshots(self) -> list[dict[str, Any]]:
        """``GET /api/<v>/snapshots`` — list all registered snapshots."""
        response = self._request("GET", "/snapshots")
        if response.status_code != 200:
            raise OacRestError(
                f"list_snapshots failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        body = response.json()
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("items", "snapshots", "results"):
                items = body.get(key)
                if isinstance(items, list):
                    return items
        return []

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """``DELETE /api/<v>/snapshots/{id}`` — deregister a snapshot. Returns True on 2xx, False on 404."""
        response = self._request("DELETE", f"/snapshots/{snapshot_id}")
        if response.status_code == 404:
            return False
        if response.status_code not in (200, 204):
            raise OacRestError(
                f"delete_snapshot failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return True

    def restore_snapshot(
        self,
        snapshot_id: str,
        *,
        password: str | None = None,
    ) -> str:
        """``POST /api/<v>/system/actions/restoreSnapshot`` — restore a snapshot.

        Async (returns 202 with ``oa-work-request-id`` header). Use
        :meth:`poll_work_request` to wait for SUCCEEDED.

        Args:
            snapshot_id: The ``id`` returned from :meth:`register_snapshot`.
            password: BAR password (if the BAR was encrypted at create time).

        Returns:
            The ``oa-work-request-id`` to pass to :meth:`poll_work_request`.

        Raises:
            OacRestError: on non-2xx response.
            RuntimeError: if the response is missing ``oa-work-request-id``.
        """
        body: dict[str, Any] = {"snapshot": {"id": snapshot_id}}
        if password:
            body["snapshot"]["password"] = password

        response = self._request("POST", "/system/actions/restoreSnapshot", json_body=body)
        if response.status_code not in (200, 202):
            raise OacRestError(
                f"restore_snapshot failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        work_request_id = response.headers.get("oa-work-request-id") or response.headers.get("Location", "").rsplit("/", 1)[-1]
        if not work_request_id:
            raise RuntimeError(
                f"restore_snapshot accepted (HTTP {response.status_code}) but no "
                f"oa-work-request-id header found; response headers: {dict(response.headers)}"
            )
        return work_request_id

    # ---------------------------------------------------------- work requests
    def get_work_request(self, work_request_id: str) -> dict[str, Any]:
        """``GET /api/<v>/workRequests/{id}`` — fetch the current state of an async op."""
        response = self._request("GET", f"/workRequests/{work_request_id}")
        if response.status_code != 200:
            raise OacRestError(
                f"get_work_request failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json()

    def poll_work_request(
        self,
        work_request_id: str,
        *,
        timeout: int = 600,
        poll_interval: int = 5,
    ) -> dict[str, Any]:
        """Poll ``GET /workRequests/{id}`` until status is terminal (SUCCEEDED/FAILED/CANCELED).

        Args:
            work_request_id: The work-request ID returned from an async op.
            timeout: Maximum total wait in seconds (default 10 minutes — restore can take a few).
            poll_interval: Seconds between polls.

        Returns:
            The final work-request record (includes ``status`` and any error details).

        Raises:
            TimeoutError: if the work request doesn't reach a terminal state within ``timeout``.
            OacRestError: if individual GET requests fail.
        """
        deadline = time.time() + timeout
        last: dict[str, Any] = {}
        while time.time() < deadline:
            last = self.get_work_request(work_request_id)
            status = last.get("status") or last.get("lifecycleState") or ""
            if status in WorkRequestStatus.TERMINAL:
                return last
            time.sleep(poll_interval)
        raise TimeoutError(
            f"work request {work_request_id} did not reach terminal status within {timeout}s; "
            f"last status: {last.get('status') or last.get('lifecycleState') or '<unknown>'}"
        )


__all__ = [
    "OacRestClient",
    "OacRestError",
    "WorkRequestStatus",
    "encode_catalog_id",
]
