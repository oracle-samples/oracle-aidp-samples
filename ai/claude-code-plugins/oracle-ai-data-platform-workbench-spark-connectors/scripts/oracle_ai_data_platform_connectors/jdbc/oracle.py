"""Oracle JDBC URL + Spark options builders.

Used by the ALH, ATP, and ExaCS skills. ALH is plain Oracle 26ai under the
hood — same driver, same URL pattern, same TNS service-name shape — so the
three skills share this module.
"""

from __future__ import annotations

from typing import Optional

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"


def build_oracle_jdbc_url(
    host: Optional[str] = None,
    port: int = 1522,
    service_name: Optional[str] = None,
    tns_alias: Optional[str] = None,
    tns_admin: Optional[str] = None,
    use_tcps: bool = True,
) -> str:
    """Build an Oracle thin-JDBC URL.

    Two shapes are supported:

    1. **Direct host/port/service** (ExaCS, plain Oracle):
       ``jdbc:oracle:thin:@tcps://<host>:<port>/<service>``

    2. **TNS alias** (ALH, ATP wallet flow):
       ``jdbc:oracle:thin:@<tns_alias>?TNS_ADMIN=<dir>``

    Args:
        host: Hostname for direct mode. Mutually exclusive with ``tns_alias``.
        port: Port for direct mode. Default 1522 (TCPS).
        service_name: Oracle service name for direct mode (NOT a SID).
        tns_alias: TNS alias from ``tnsnames.ora`` (e.g. ``alh_high``,
            ``atp_high``).
        tns_admin: Path to the directory containing ``tnsnames.ora`` /
            ``sqlnet.ora`` / ``cwallet.sso``. Only relevant for the TNS-alias
            mode. Helper appends ``?TNS_ADMIN=...`` to the URL so the JDBC
            driver finds the wallet without an env-var dance.
        use_tcps: Direct mode only — whether to prefix with ``tcps://`` (TLS).
            Set to False for plain TCP (rare, only for legacy on-prem).

    Returns:
        A JDBC URL string.

    Raises:
        ValueError: If neither (host+service_name) nor tns_alias is given.
    """
    if tns_alias:
        url = f"jdbc:oracle:thin:@{tns_alias}"
        if tns_admin:
            url += f"?TNS_ADMIN={tns_admin}"
        return url

    if not (host and service_name):
        raise ValueError(
            "build_oracle_jdbc_url requires either tns_alias or (host + service_name)"
        )

    proto = "tcps" if use_tcps else "tcp"
    return f"jdbc:oracle:thin:@{proto}://{host}:{port}/{service_name}"


def spark_jdbc_options_wallet(
    url: str,
    user: str,
    password: str,
    *,
    fetchsize: int = 10_000,
    timezone_as_region: bool = False,
) -> dict:
    """Spark JDBC options for wallet (mTLS) auth.

    The wallet itself must already be on disk under ``/tmp/wallet/...`` and the
    URL must include ``?TNS_ADMIN=...`` (or ``TNS_ADMIN`` env var must be set).
    See ``oracle_ai_data_platform_connectors.auth.wallet.write_wallet_to_tmp``.
    """
    return {
        "url": url,
        "driver": ORACLE_DRIVER,
        "user": user,
        "password": password,
        "fetchsize": str(fetchsize),
        "oracle.jdbc.timezoneAsRegion": str(timezone_as_region).lower(),
    }


def spark_jdbc_options_dbtoken(
    url: str,
    token_dir: str,
    *,
    fetchsize: int = 10_000,
    timezone_as_region: bool = False,
) -> dict:
    """Spark JDBC options for IAM DB-Token auth.

    Args:
        url: The same URL as the wallet path (TCPS, port 1522, service name).
        token_dir: Directory under /tmp containing the ``token`` file. See
            ``oracle_ai_data_platform_connectors.auth.dbtoken.generate_db_token``.
        fetchsize: JDBC fetch size.
        timezone_as_region: Forwarded as ``oracle.jdbc.timezoneAsRegion``.
            Default False — avoids the well-known TZ region warning.

    Note:
        DB-Token auth needs `user`/`password` to be unset; the JDBC driver
        reads the token from ``oracle.jdbc.tokenLocation``.
    """
    return {
        "url": url,
        "driver": ORACLE_DRIVER,
        "oracle.jdbc.tokenAuthentication": "OCI_TOKEN",
        "oracle.jdbc.tokenLocation": token_dir,
        "fetchsize": str(fetchsize),
        "oracle.jdbc.timezoneAsRegion": str(timezone_as_region).lower(),
    }


def spark_jdbc_options_password(
    url: str,
    user: str,
    password: str,
    *,
    fetchsize: int = 10_000,
    timezone_as_region: bool = False,
) -> dict:
    """Plain user/password Spark JDBC options (legacy DB user, e.g. ExaCS)."""
    return {
        "url": url,
        "driver": ORACLE_DRIVER,
        "user": user,
        "password": password,
        "fetchsize": str(fetchsize),
        "oracle.jdbc.timezoneAsRegion": str(timezone_as_region).lower(),
    }
