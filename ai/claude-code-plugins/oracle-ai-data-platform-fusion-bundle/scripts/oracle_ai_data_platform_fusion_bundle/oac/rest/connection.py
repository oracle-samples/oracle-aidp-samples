"""Build the AIDP JDBC connection JSON payload for OAC.

The 6-key shape was discovered live 2026-04-30 against
``https://oacai.cealinfra.com`` by iterating through the OAC Connection
Details upload form (`Parameter <X> is missing in JSON` error
messages). See ``docs/oac_rest_api_setup.md`` and the saved memory
``project_oac_aidp_connector_schema.md`` for the full discovery trace.

The JSON the bundle generates is the *same* file an OAC admin would
upload via Data -> Connections -> Create -> "Oracle AI Data Platform" ->
Connection Details. Whether it's POSTed via OAC REST or pasted into the
UI form, the payload is identical.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AidpConnectionPayload:
    """The 6 required keys that OAC's "Oracle AI Data Platform" connector accepts.

    Field name traps (verified live 2026-04-30):
      - ``username`` (NOT ``user``) — OCID of the user that registered the API key
      - ``tenancy`` (NOT ``tenancy-ocid``) — OCID of the tenancy
      - ``idl-ocid`` (kebab-case) — the AIDP DataLake OCID
      - ``dsn`` — full JDBC URL with ``SparkServerType=AIDP`` and ``httpPath=cliservice/<cluster-key>``

    Each missing field triggers ``Parameter <field> is missing in JSON`` in the OAC UI.
    """

    username: str
    tenancy: str
    region: str
    fingerprint: str
    idl_ocid: str  # serialized as ``idl-ocid``
    dsn: str

    def to_dict(self) -> dict[str, str]:
        """Serialize with the exact key names OAC expects (``idl-ocid`` kebab-case)."""
        return {
            "username": self.username,
            "tenancy": self.tenancy,
            "region": self.region,
            "fingerprint": self.fingerprint,
            "idl-ocid": self.idl_ocid,
            "dsn": self.dsn,
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


def build_dsn(
    region: str,
    cluster_key: str,
    *,
    catalog: str = "default",
    gateway_host: str | None = None,
) -> str:
    """Build the JDBC DSN string for the AIDP Spark gateway.

    Pattern (verified live 2026-04-30 against AIDP IAD cluster ``tpcds``):
        ``jdbc:spark://gateway.aidp.<region>.oci.oraclecloud.com/<catalog>;``
        ``SparkServerType=AIDP;httpPath=cliservice/<cluster-key>``

    Args:
        region: OCI region key, e.g. ``us-ashburn-1``.
        cluster_key: AIDP cluster key (UUID-like) — see ``aidp.config.yaml`` env block.
        catalog: Default catalog. ``default`` works; OAC sees all catalogs in the schema tree.
        gateway_host: Override for the gateway hostname (rare; only for non-OCI deployments).
    """
    host = gateway_host or f"gateway.aidp.{region}.oci.oraclecloud.com"
    return (
        f"jdbc:spark://{host}/{catalog};"
        f"SparkServerType=AIDP;httpPath=cliservice/{cluster_key}"
    )


def build_payload(
    *,
    user_ocid: str,
    tenancy_ocid: str,
    region: str,
    fingerprint: str,
    idl_ocid: str,
    cluster_key: str,
    catalog: str = "default",
    gateway_host: str | None = None,
) -> AidpConnectionPayload:
    """Build a complete :class:`AidpConnectionPayload` from primitives."""
    dsn = build_dsn(region=region, cluster_key=cluster_key, catalog=catalog,
                    gateway_host=gateway_host)
    return AidpConnectionPayload(
        username=user_ocid,
        tenancy=tenancy_ocid,
        region=region,
        fingerprint=fingerprint,
        idl_ocid=idl_ocid,
        dsn=dsn,
    )


def render_template(payload: AidpConnectionPayload, output_path: Path | str) -> Path:
    """Write the connection JSON to ``output_path`` (creating parent dirs).

    For the print-only fallback (when IDCS confidential app is not registered),
    callers do this then point the user at OAC's Data -> Connections -> Create
    -> "Oracle AI Data Platform" form for manual upload.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(payload.to_json(), encoding="utf-8")
    return out


__all__ = [
    "AidpConnectionPayload",
    "build_dsn",
    "build_payload",
    "render_template",
]
