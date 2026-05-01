"""OAC REST API wrappers used by ``aidp-fusion-bundle dashboard install --target oac``.

Public API:
    * :class:`OacOauthFlow` — Auth Code + PKCE / Device flow with refresh-token persistence
    * :func:`derive_oac_scope` / :func:`discover_oac_audience` — build the canonical scope string
    * :class:`AidpConnectionPayload` / :func:`build_payload` / :func:`render_template`
      — build and serialize the 6-key AIDP JDBC connection JSON
    * :class:`OacRestClient` — Bearer-authenticated wrapper for the documented public endpoints:
      ``/catalog/connections``, ``/snapshots``, ``/system/actions/restoreSnapshot``,
      ``/workRequests/{id}``
    * :class:`WorkRequestStatus` — enum of OAC's async-op statuses
    * :func:`encode_catalog_id` — Base64URL-encode a catalog object's plain ID for path-params
"""

from .client import OacRestClient, OacRestError, WorkRequestStatus, encode_catalog_id
from .connection import AidpConnectionPayload, build_dsn, build_payload, render_template
from .oauth import IdcsTokenFetcher, OacOauthFlow, TokenBundle, derive_oac_scope, discover_oac_audience

__all__ = [
    "OacRestClient",
    "OacRestError",
    "WorkRequestStatus",
    "encode_catalog_id",
    "AidpConnectionPayload",
    "build_dsn",
    "build_payload",
    "render_template",
    "OacOauthFlow",
    "IdcsTokenFetcher",
    "TokenBundle",
    "derive_oac_scope",
    "discover_oac_audience",
]
