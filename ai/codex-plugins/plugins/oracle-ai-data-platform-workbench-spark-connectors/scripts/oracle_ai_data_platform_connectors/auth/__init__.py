"""Auth helpers for Oracle AIDP Spark connectors.

Re-exports the most commonly used callables so notebooks can do:

    from oracle_ai_data_platform_connectors.auth import (
        write_wallet_to_tmp,
        generate_db_token,
        from_inline_pem,
        http_basic_session,
        oauth_token,
        get_secret,
    )
"""

from .wallet import write_wallet_to_tmp
from .dbtoken import generate_db_token, refresh_on_executors
from .oci_config import from_inline_pem
from .user_principal import http_basic_session, oauth_token
from .secrets import get_secret

__all__ = [
    "write_wallet_to_tmp",
    "generate_db_token",
    "refresh_on_executors",
    "from_inline_pem",
    "http_basic_session",
    "oauth_token",
    "get_secret",
]
