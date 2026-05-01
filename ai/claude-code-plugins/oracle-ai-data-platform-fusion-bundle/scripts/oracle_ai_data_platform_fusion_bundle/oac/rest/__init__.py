"""OAC REST API wrappers used by ``aidp-fusion-bundle dashboard install --target oac``.

Public API:
    * :class:`OacOauthFlow` — Auth Code + PKCE / Device flow with refresh-token persistence
    * :func:`derive_oac_scope` — build the canonical scope string from an OAC URL
    * :class:`AidpConnectionPayload` / :func:`build_payload` / :func:`render_template`
      — build and serialize the 6-key AIDP JDBC connection JSON
    * :class:`OacRestClient` — Bearer-authenticated wrapper for
      ``/api/<v>/catalog/connections`` and ``/api/<v>/catalog/workbooks/imports``
"""

from .client import OacRestClient, OacRestError
from .connection import AidpConnectionPayload, build_dsn, build_payload, render_template
from .oauth import IdcsTokenFetcher, OacOauthFlow, TokenBundle, derive_oac_scope, discover_oac_audience

__all__ = [
    "OacRestClient",
    "OacRestError",
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
