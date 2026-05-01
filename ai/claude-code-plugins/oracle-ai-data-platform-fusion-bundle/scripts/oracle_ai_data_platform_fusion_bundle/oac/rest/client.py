"""OAC REST API client (Bearer-token authenticated).

Wraps the public ``/api/20210901/...`` endpoints used by ``dashboard install``:

- ``POST /api/20210901/catalog/connections``
    Register the AIDP JDBC connection.
- ``GET  /api/20210901/catalog/connections``
    List existing connections (used to detect already-installed bundle connection).
- ``DELETE /api/20210901/catalog/connections/{name}``
    Used by ``dashboard uninstall``.
- ``POST /api/20210901/catalog/workbooks/imports``
    Upload a ``.dva`` workbook export (multipart/form-data).
- ``GET  /api/20210901/catalog/workbooks?name=...``
    Find imported workbooks by name (used for validate / uninstall).

Auth: each call attaches ``Authorization: Bearer <token>`` from
:class:`~.oauth.IdcsTokenFetcher`. The token is cached and re-used across
calls until it expires.

Reference: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/rest-endpoints.html
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests

from .connection import AidpConnectionPayload
from .oauth import IdcsTokenFetcher


class OacRestError(RuntimeError):
    """Wraps an OAC REST response that returned non-2xx."""

    def __init__(self, message: str, *, response: requests.Response | None = None) -> None:
        super().__init__(message)
        self.response = response


class OacRestClient:
    """Bearer-token-authenticated client for OAC's public REST API.

    Args:
        oac_url: Base URL of the OAC instance, e.g. ``https://oacai.example.com``.
            Trailing slash optional.
        token_fetcher: Configured :class:`IdcsTokenFetcher` to obtain Bearer tokens.
        api_version: Path prefix segment (default ``20210901`` matches the
            current public REST API release).
        timeout: Per-request timeout in seconds.
        session: Optional pre-configured ``requests.Session`` (useful for tests
            and for plugging in retries / rate-limiting).
    """

    def __init__(
        self,
        oac_url: str,
        token_fetcher: IdcsTokenFetcher,
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
        # JSON content-type only when posting JSON; multipart sets its own
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
            # Token may have been revoked or rotated mid-flight; one retry with a fresh token.
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
    def list_connections(self) -> list[dict[str, Any]]:
        """``GET /api/<v>/catalog/connections`` — list all connections in the OAC catalog."""
        response = self._request("GET", "/catalog/connections")
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
        """Return the connection record whose ``name`` matches, or ``None``."""
        for conn in self.list_connections():
            if conn.get("name") == name or conn.get("connectionName") == name:
                return conn
        return None

    def create_connection(
        self,
        *,
        name: str,
        payload: AidpConnectionPayload,
        private_key_pem_path: Path | str,
        description: str | None = None,
        connection_type: str = "oracle-ai-data-platform",
    ) -> dict[str, Any]:
        """``POST /api/<v>/catalog/connections`` — register the AIDP JDBC connection.

        Per Oracle's [REST API doc](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acapi/op-20210901-catalog-connections-post.html),
        OAC expects a multipart/form-data body with two fields:

          - ``connectionParams`` (string): JSON envelope with version + type + name +
            content.connectionParams (the per-connection-type key-bag)
          - ``cert`` (file): the private key PEM (or wallet zip for ATP-style connections)

        The 6-key AIDP details (``username``/``tenancy``/``region``/``fingerprint``/
        ``idl-ocid``/``dsn``) live inside ``content.connectionParams`` alongside the
        ``connectionType`` discriminator.

        Args:
            name: Human-readable connection name (e.g. ``aidp_fusion_jdbc``).
            payload: 6-key AIDP detail payload from
                :func:`oracle_ai_data_platform_fusion_bundle.oac.rest.connection.build_payload`.
            private_key_pem_path: Path to the RSA private key whose public half is
                registered on the user OCID.
            description: Optional connection description.
            connection_type: Discriminator key OAC uses internally. Default
                ``oracle-ai-data-platform`` matches what the OAC UI's connection
                picker emits (verified in network DevTools).

        Returns:
            The newly created connection record (as returned by OAC).

        Raises:
            OacRestError: if OAC rejects the request.
        """
        pem_path = Path(private_key_pem_path)
        if not pem_path.exists():
            raise FileNotFoundError(f"private key PEM not found: {pem_path}")

        # Build the documented JSON envelope.
        connection_params: dict[str, Any] = {
            "connectionType": connection_type,
            **payload.to_dict(),
            "systemConnection": False,
            "remoteData": False,
        }
        envelope: dict[str, Any] = {
            "version": "2.0.0",
            "type": "connection",
            "name": name,
            "content": {"connectionParams": connection_params},
        }
        if description:
            envelope["description"] = description

        with pem_path.open("rb") as pem_fh:
            # OAC's POST /catalog/connections wants both multipart parts as text/plain
            # (verified live 2026-05-01: application/json on connectionParams gets
            # "Media type is not text/plain").
            files = {
                "connectionParams": (None, json.dumps(envelope), "text/plain"),
                "cert": (pem_path.name, pem_fh.read(), "text/plain"),
            }
            response = self._request("POST", "/catalog/connections", files=files)

        if response.status_code not in (200, 201):
            raise OacRestError(
                f"create_connection failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json() if response.text else {}

    def delete_connection(self, name_or_id: str) -> bool:
        """``DELETE /api/<v>/catalog/connections/{name}``. Returns True on 2xx, False on 404."""
        response = self._request("DELETE", f"/catalog/connections/{name_or_id}")
        if response.status_code == 404:
            return False
        if response.status_code not in (200, 204):
            raise OacRestError(
                f"delete_connection failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return True

    # -------------------------------------------------------------- workbooks
    def list_workbooks(self, *, name: str | None = None) -> list[dict[str, Any]]:
        """``GET /api/<v>/catalog/workbooks`` — list workbooks, optional name filter."""
        params: dict[str, Any] | None = {"name": name} if name else None
        response = self._request("GET", "/catalog/workbooks", params=params)
        if response.status_code != 200:
            raise OacRestError(
                f"list_workbooks failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        body = response.json()
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            for key in ("items", "workbooks", "results"):
                items = body.get(key)
                if isinstance(items, list):
                    return items
        return []

    def import_workbook(
        self,
        dva_path: Path | str,
        *,
        target_folder: str | None = None,
    ) -> dict[str, Any]:
        """``POST /api/<v>/catalog/workbooks/imports`` — upload a ``.dva`` archive.

        Args:
            dva_path: Local path to the workbook export (``.dva`` is OAC's ZIP-based archive).
            target_folder: Optional target folder path in OAC's catalog (default: user's "My Folders").
        """
        path = Path(dva_path)
        if not path.exists():
            raise FileNotFoundError(f"DVA archive not found: {path}")
        with path.open("rb") as fh:
            files = {"file": (path.name, fh.read(), "application/octet-stream")}
            form_data: dict[str, str] = {}
            if target_folder:
                form_data["folder"] = target_folder
            response = self._request(
                "POST", "/catalog/workbooks/imports",
                files=files, data=form_data or None,
            )
        if response.status_code not in (200, 201, 202):
            raise OacRestError(
                f"import_workbook failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return response.json() if response.text else {}

    def delete_workbook(self, workbook_id: str) -> bool:
        """``DELETE /api/<v>/catalog/workbooks/{id}``."""
        response = self._request("DELETE", f"/catalog/workbooks/{workbook_id}")
        if response.status_code == 404:
            return False
        if response.status_code not in (200, 204):
            raise OacRestError(
                f"delete_workbook failed: HTTP {response.status_code}: {response.text}",
                response=response,
            )
        return True


__all__ = ["OacRestClient", "OacRestError"]
