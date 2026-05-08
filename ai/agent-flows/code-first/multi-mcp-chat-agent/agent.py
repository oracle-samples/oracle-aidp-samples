"""
Multi-MCP Chat Agent for AIDP — Oracle ADB SelectAI + OAC + OIC.

This is the entry point AIDP looks for when it deploys this folder as
an agent. AIDP loads this file, finds the AgentBasic class, and calls
its setup() once and invoke() per chat message.

Reads configuration from ./config.yaml (the sibling file). Each
integration (adb / oac / oic) can be turned on or off independently.
See README.md for the end-to-end setup walkthrough.

Quick architecture:
  • Agent state lives in memory only (one Python process serves all
    chat users from the same AgentBasic instance).
  • MCP tool execution happens via short-lived HTTPS requests — there
    are no persistent network connections to keep alive.
  • Auth is on-demand only. All three integrations authenticate via
    OAuth 2.0 grants — none use a rotating refresh chain:
        - ADB:  password grant (RFC 6749 §4.3)
        - OAC:  JWT Bearer / User Assertion (RFC 7523), local-signed
                from a long-lived private key
        - OIC:  client_credentials (RFC 6749 §4.4)
    So we don't run any background refresh loop. Tokens get rebuilt
    only when a tool call fails with 401/connection error — the
    reactive retry in invoke() catches that, mints fresh bearers,
    and retries.
"""

import asyncio
import base64
import json
import logging
import os
import time
import traceback
import uuid
from pathlib import Path

import requests

from langchain_core.messages import HumanMessage, RemoveMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent

from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import OCIAIConf

logger = logging.getLogger(__name__)


# ╔══════════════════════════════════════════════════════════════╗
# ║   Config loading                                             ║
# ║                                                              ║
# ║   We read config.yaml at module import time, BUT wrap        ║
# ║   everything in try/except so a broken config doesn't kill   ║
# ║   the import — if it did, AIDP would just say "Agent not     ║
# ║   loaded or available" with no detail. By capturing the      ║
# ║   error here and surfacing it in chat from setup() later,    ║
# ║   the user sees a real diagnostic message.                   ║
# ╚══════════════════════════════════════════════════════════════╝

_CFG_INIT_ERROR: str | None = None
_CFG_PATHS_TRIED: list[str] = []
CFG: dict = {}


def _find_config_yaml() -> Path | None:
    """Look for config.yaml in the most likely places. AIDP doesn't always
    deploy non-.py files next to the agent script the way you'd expect, so
    we check a few candidate paths and fall back to a bounded recursive
    search if needed."""
    # Direct paths to try first — fast (no scanning).
    direct = [
        Path(__file__).resolve().parent / "config.yaml",  # next to agent.py
        Path.cwd() / "config.yaml",                       # current working dir
        Path.cwd().parent / "config.yaml",                # one level up
        Path.home() / "config.yaml",                      # $HOME
    ]
    for c in direct:
        _CFG_PATHS_TRIED.append(str(c))
        if c.exists():
            return c

    # Fallback: recursive search inside the deployment subtree only.
    # Important: NEVER recurse from Path.home() — that could scan a huge
    # tree on shared hosts and stall the agent for minutes.
    for base in (Path.cwd(), Path.cwd().parent):
        if not base.exists():
            continue
        try:
            for p in base.rglob("config.yaml"):
                _CFG_PATHS_TRIED.append(f"(rglob match) {p}")
                return p
        except Exception:
            pass  # filesystem errors during scan — try the next base
    return None


try:
    import yaml  # PyYAML — confirmed pre-installed in AIDP runtime (6.0.2)
    found = _find_config_yaml()
    if found is None:
        raise FileNotFoundError(
            "config.yaml not found in any expected location. Searched:\n"
            + "\n".join(f"  • {p}" for p in _CFG_PATHS_TRIED)
            + "\n\nAIDP may not bundle non-.py files automatically. "
            + "Try uploading config.yaml again from the AIDP project file "
            + "explorer (same place where you uploaded agent.py)."
        )
    # `yaml.safe_load` returns None for empty files — we coerce to {} so
    # downstream code can use .get() safely without None checks everywhere.
    CFG = yaml.safe_load(found.read_text()) or {}
    logger.info("Loaded config.yaml from %s", found)
except Exception:
    # Capture the full traceback so we can show it in chat later. We
    # deliberately catch BaseException-style broadly (Exception covers
    # everything we care about here — file errors, YAML parse errors,
    # ImportError if PyYAML somehow disappears).
    import traceback as _tb
    _CFG_INIT_ERROR = _tb.format_exc()

LLM_COMPARTMENT_ID = CFG.get("llm", {}).get("compartment_id", "")
LLM_REGION         = CFG.get("llm", {}).get("region", "us-ashburn-1")
LLM_MODEL_ID       = CFG.get("llm", {}).get("model_id", "")

ADB_CFG     = CFG.get("integrations", {}).get("adb", {}) or {}
ADB_ENABLED = bool(ADB_CFG.get("enabled", False))

OAC_CFG     = CFG.get("integrations", {}).get("oac", {}) or {}
OAC_ENABLED = bool(OAC_CFG.get("enabled", False))

OIC_CFG     = CFG.get("integrations", {}).get("oic", {}) or {}
OIC_ENABLED = bool(OIC_CFG.get("enabled", False))

# OAC paths (only used if OAC enabled)
OAC_HOME             = Path.home() / "oac-mcp"
OAC_PRIVATE_KEY      = OAC_HOME / "jwt-signing.pem"
OAC_PRIVATE_KEY_NAME = OAC_CFG.get("private_key_filename", "oac-jwt-key.txt")
OAC_TOKEN_URL        = (
    f"https://{OAC_CFG.get('idcs_host', '')}/oauth2/v1/token" if OAC_ENABLED else ""
)
OAC_MCP_URL          = f"{OAC_CFG.get('url', '')}/api/mcp"

# OIC OAuth (only used if OIC enabled)
OIC_TOKEN_URL = (
    f"https://{OIC_CFG.get('idcs_host', '')}/oauth2/v1/token" if OIC_ENABLED else ""
)


# ╔══════════════════════════════════════════════════════════════╗
# ║   Agent prompt — adapts to enabled integrations              ║
# ╚══════════════════════════════════════════════════════════════╝

def _build_system_prompt() -> str:
    parts = [
        "You are a helpful data assistant with access to tools loaded "
        "from one or more MCP servers."
    ]
    if ADB_ENABLED:
        parts.append(
            "  - ADB SelectAI tools: query a transactional Oracle Autonomous "
            "Database in natural language (transactions, accounts, customers)."
        )
    if OAC_ENABLED:
        parts.append(
            "  - Oracle Analytics Cloud (OAC) tools: query OAC subject areas / "
            "datasets with governance-aware Logical SQL. Use these for "
            "analytical / reporting questions about pre-modeled data."
        )
    if OIC_ENABLED:
        parts.append(
            "  - Oracle Integration Cloud (OIC) tools: invoke OIC integrations "
            "exposed as tools by the OIC project's MCP server. Use these "
            "for questions about integrations, audit reports, and "
            "orchestration exposed by the OIC instance."
        )
    parts.append("")
    parts.append("Rules:")
    parts.append(
        "  - Pick the right tool for each question. If unsure where data "
        "lives, use a discover-style tool first."
    )
    if ADB_ENABLED:
        parts.append(
            "  - For SelectAI: use action='runsql' for data, 'showsql' to "
            "see SQL, 'explainsql' to explain."
        )
    if OAC_ENABLED:
        parts.append(
            "  - For OAC Logical SQL: discover_data and describe_data "
            "before execute_logical_sql so you know table/column names."
        )
    parts.append("  - Pass the user's actual question; never substitute or invent.")
    parts.append("  - For follow-ups, rewrite using prior turn context, then call the tool.")
    parts.append("  - After a tool returns, summarize in plain English. Don't dump raw JSON unless asked.")
    parts.append("  - Never invent numbers — if a tool errored, say so.")
    return "\n".join(parts)


AGENT_SYSTEM_PROMPT = _build_system_prompt()

llm_conf = OCIAIConf(
    model_provider="generic",
    compartment_id=LLM_COMPARTMENT_ID,
    model_args={},
    endpoint=f"https://inference.generativeai.{LLM_REGION}.oci.oraclecloud.com",
    model_id=LLM_MODEL_ID,
    guardrails_config={"name": "Default", "description": "", "policies": []},
)

checkpointer = globals().get("checkpointer", None)


# ╔══════════════════════════════════════════════════════════════╗
# ║   ADB — password-grant bearer                                ║
# ╚══════════════════════════════════════════════════════════════╝

def _adb_bearer_token() -> str:
    auth_url = (
        f"https://dataaccess.adb.{ADB_CFG['region']}.oraclecloudapps.com"
        f"/adb/auth/v1/databases/{ADB_CFG['ocid']}/token"
    )
    r = requests.post(
        auth_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "password",
            "username": ADB_CFG["user"],
            "password": ADB_CFG["password"],
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _adb_mcp_url() -> str:
    return (
        f"https://dataaccess.adb.{ADB_CFG['region']}.oraclecloudapps.com"
        f"/adb/mcp/v1/databases/{ADB_CFG['ocid']}"
    )


# ╔══════════════════════════════════════════════════════════════╗
# ║   OIC — client_credentials against IDCS                      ║
# ╚══════════════════════════════════════════════════════════════╝

def _oic_bearer_token() -> str:
    r = requests.post(
        OIC_TOKEN_URL,
        auth=(OIC_CFG["client_id"], OIC_CFG["client_secret"]),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials", "scope": OIC_CFG["scope"]},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ╔══════════════════════════════════════════════════════════════╗
# ║   OAC — JWT User Assertion (no rotating tokens, no expiry)   ║
# ╚══════════════════════════════════════════════════════════════╝

def _stage_oac_private_key() -> None:
    """Find the uploaded PEM private key (default name: oac-jwt-key.txt
    — AIDP's file picker accepts .txt) and copy it to OAC_PRIVATE_KEY.
    Idempotent: only writes if the upload differs from the staged copy."""
    OAC_HOME.mkdir(parents=True, exist_ok=True)
    src = None
    for base in (Path.cwd(), Path.cwd().parent):
        if not base.exists():
            continue
        for p in base.rglob(OAC_PRIVATE_KEY_NAME):
            if p == OAC_PRIVATE_KEY:
                continue
            src = p
            break
        if src:
            break

    if src is None:
        if not OAC_PRIVATE_KEY.exists():
            raise FileNotFoundError(
                f"{OAC_PRIVATE_KEY_NAME} not found in AIDP project. "
                "Generate the keypair, register the cert in IDCS, then "
                f"upload the PEM private key as {OAC_PRIVATE_KEY_NAME} "
                "alongside agent.py. See README.md for full steps."
            )
        print(f"[stage] OAC private key at {OAC_PRIVATE_KEY} (no new upload)")
        return

    src_bytes    = src.read_bytes()
    staged_bytes = OAC_PRIVATE_KEY.read_bytes() if OAC_PRIVATE_KEY.exists() else b""
    if src_bytes == staged_bytes:
        print(f"[stage] Uploaded {OAC_PRIVATE_KEY_NAME} identical to staged — keeping")
        return

    print(f"[stage] New {OAC_PRIVATE_KEY_NAME} at {src} → {OAC_PRIVATE_KEY}")
    OAC_PRIVATE_KEY.write_bytes(src_bytes)


def _load_oac_private_key():
    """Load the PEM private key from disk. Returns a cryptography RSA
    private key object usable for signing JWTs."""
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    return load_pem_private_key(OAC_PRIVATE_KEY.read_bytes(), password=None)


def _mint_jwt(claims: dict, private_key, kid: str | None = None) -> str:
    """Hand-build an RS256-signed JWT.

    We don't use the PyJWT library because it's not guaranteed to be in
    AIDP's runtime, and we already established that runtime pip installs
    don't work reliably. The `cryptography` library IS guaranteed to be
    there (it's pulled in by the oci SDK and requests/urllib3), so we use
    it directly. Total: ~15 lines of base64 + JSON encoding instead of an
    extra dependency we'd have to babysit.

    JWT structure: three base64url-encoded parts joined by dots:
      <header>.<claims>.<signature>

    `kid` is the certificate alias you set in IDCS when uploading the
    public cert. IDCS uses it to look up which trusted cert to verify the
    signature with. Required for our use case — without it, IDCS rejects
    the assertion with a generic "system error" message."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    def b64url(d: dict) -> bytes:
        # JWT uses base64url encoding without padding ('=' chars stripped).
        # `separators=(",", ":")` produces compact JSON (no extra spaces).
        return base64.urlsafe_b64encode(
            json.dumps(d, separators=(",", ":")).encode()
        ).rstrip(b"=")

    header: dict = {"alg": "RS256", "typ": "JWT"}
    if kid:
        header["kid"] = kid

    # The signature is computed over "<header>.<claims>".
    body = b64url(header) + b"." + b64url(claims)
    signature = private_key.sign(body, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")

    return (body + b"." + sig_b64).decode()


def _fetch_oac_access_token(private_key) -> str:
    """Mint a fresh OAC access_token using the JWT User Assertion flow.

    How this flow works:
      1. We sign TWO short-lived JWTs locally with our private key:
         - The **client assertion** proves *we are this client*.
           sub = client_id (the Confidential App's ID).
         - The **user assertion** says *we're acting as this user*.
           sub = service_user_email.
      2. We POST both JWTs to IDCS's /oauth2/v1/token endpoint along
         with the JWT-bearer grant type. IDCS validates the signatures
         against the X.509 cert we registered (matched via the JWT's
         `kid` header), and if both check out, returns an access_token
         scoped to OAC and authenticated AS the asserted user.
      3. We use that access_token to call OAC's MCP endpoint.

    Important: we deliberately DO NOT use the refresh_token IDCS may
    return alongside the access_token. The whole reason we picked this
    flow is to escape the rotating-refresh chain that breaks overnight.
    When the access_token expires, we just mint a brand new JWT and
    exchange it again. Cheap (local crypto + one HTTPS call), reliable.

    Counter-intuitive details that took us a while to discover:
      • `aud` MUST be the LITERAL string "https://identity.oraclecloud.com/"
        with trailing slash. NOT your tenant's token URL. IDCS's generic
        Oracle audience for JWT bearer assertions.
      • `kid` in the JWT header MUST match the cert alias you set when
        you imported the cert in IDCS. Without it, IDCS can't figure
        out which trusted cert to verify the signature against.
      • Both JWTs share the same `iss` (always the client_id) and only
        differ in `sub`. Don't try to be clever with the issuer claim."""
    cert_alias = OAC_CFG.get("cert_alias")
    now = int(time.time())

    # Claims shared by both JWTs. We override `sub` per-assertion below.
    # exp = 5 min from now — IDCS rejects assertions issued more than
    # 5 min in the future or already expired, so keep this short.
    base_claims = {
        "iss": OAC_CFG["client_id"],
        "aud": "https://identity.oraclecloud.com/",
        "iat": now,
        "exp": now + 300,
        "jti": str(uuid.uuid4()),  # unique nonce — prevents replay attacks
    }
    client_jwt = _mint_jwt(
        {**base_claims, "sub": OAC_CFG["client_id"]},
        private_key,
        kid=cert_alias,
    )
    user_jwt = _mint_jwt(
        {**base_claims, "sub": OAC_CFG["service_user_email"]},
        private_key,
        kid=cert_alias,
    )

    # POST to the IDCS token endpoint with both assertions.
    r = requests.post(
        OAC_TOKEN_URL,
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": user_jwt,
            "client_assertion_type":
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": client_jwt,
            "scope": OAC_CFG.get("scope", "urn:opc:resource:consumer::all"),
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ╔══════════════════════════════════════════════════════════════╗
# ║   MCP server registry — only enabled servers are included    ║
# ╚══════════════════════════════════════════════════════════════╝

def _build_mcp_config(oac_access_token: str | None) -> dict:
    cfg: dict = {}
    if ADB_ENABLED:
        cfg["adb_selectai"] = {
            "transport": "streamable_http",
            "url": _adb_mcp_url(),
            "headers": {"Authorization": f"Bearer {_adb_bearer_token()}"},
        }
    if OAC_ENABLED:
        cfg["oac_analytics"] = {
            "transport": "streamable_http",
            "url": OAC_MCP_URL,
            "headers": {"Authorization": f"Bearer {oac_access_token}"},
        }
    if OIC_ENABLED:
        cfg["oic_integrations"] = {
            "transport": "streamable_http",
            "url": OIC_CFG["mcp_url"],
            "headers": {
                "Authorization": f"Bearer {_oic_bearer_token()}",
                "Accept": "application/json, text/event-stream",
            },
        }
    return cfg


def _is_auth_error(e: Exception) -> bool:
    """True when the exception text looks like an authentication or
    authorization failure (401/403/etc.). Used as a hint that retrying
    after refreshing tokens might succeed."""
    msg = str(e).lower()
    return any(s in msg for s in ("401", "403", "unauthorized", "forbidden", "expired"))


def _is_recoverable_error(e: BaseException) -> bool:
    """Decide whether a failed invoke is worth retrying after rebuilding
    the MCP stack. We retry on:
      • Auth errors (401/403) — token may have expired, fresh one might work
      • Connection-level errors — TCP socket dropped, fresh connection helps

    Why we recurse into ExceptionGroup: the MCP SDK runs tool calls inside
    asyncio TaskGroups, and a TaskGroup wraps any sub-task exception in an
    ExceptionGroup. So if a tool call hits a 401, the exception we receive
    is "ExceptionGroup containing HTTPStatusError(401)" rather than the
    HTTPStatusError directly. Recursing finds the real cause."""
    # ExceptionGroup carries its actual children in the `.exceptions`
    # attribute. If we have one, ask whether ANY sub-exception is recoverable.
    sub_exceptions = getattr(e, "exceptions", None)
    if sub_exceptions:
        return any(_is_recoverable_error(sub) for sub in sub_exceptions)

    # Auth errors: hint to refresh tokens.
    if isinstance(e, Exception) and _is_auth_error(e):
        return True

    # Connection-level errors by class name. These names come from various
    # libraries (anyio, httpx) — match by name so we don't have to import
    # all of them just to do isinstance checks.
    cls_name = type(e).__name__
    if cls_name in ("ClosedResourceError", "BrokenResourceError",
                    "ConnectionError", "RemoteProtocolError",
                    "HTTPStatusError"):
        return True

    # Last-resort string match — sometimes the class name is something
    # generic but the message tells us it was really a connection issue.
    msg = str(e).lower()
    return any(s in msg for s in (
        "closedresourceerror", "brokenresourceerror",
        "connection closed", "connection reset", "remote disconnected",
        "401 unauthorized", "403 forbidden",
    ))


# ╔══════════════════════════════════════════════════════════════╗
# ║   AgentBasic — what AIDP discovers                           ║
# ╚══════════════════════════════════════════════════════════════╝

class AgentBasic:
    def __init__(self) -> None:
        # The LangGraph react-agent that actually drives chat turns.
        # Built once in setup() with no tools (so the agent can respond
        # to messages even before MCP is reachable), then rebuilt with
        # the loaded MCP tools in _load_all_mcp_tools().
        self.llm = None
        self.graph = None

        # Set to True after MCP tools have been loaded at least once.
        # Used to gate the lazy first-load path in invoke().
        self._tools_loaded = False

        # If something goes wrong during setup() — config errors, OAC JWT
        # setup failures, etc. — we capture the message here and return
        # it to the user as a chat reply on every invoke. Better than
        # the generic "Agent not loaded" AIDP error.
        self._self_stage_error: str | None = None

        # Cached OAC bearer (minted via JWT User Assertion). Refreshed
        # on demand via _refresh_oac_token_and_rebuild().
        self._oac_access_token: str | None = None

        # The RSA private key used to sign OAC JWTs. Loaded once at
        # setup() and kept in memory for the agent's lifetime. The
        # corresponding public cert lives in IDCS as a Trusted Client
        # cert on the Confidential Application.
        self._oac_private_key = None

        # asyncio.Lock created lazily on first invoke. Serializes MCP
        # rebuilds so two concurrent invokes don't both try to refresh
        # at the same time.
        self._load_lock = None

    # ── Setup (sync) ────────────────────────────────────────────
    def setup(self) -> None:
        logger.info(
            "Initializing Multi-MCP Chat Agent — enabled: ADB=%s, OAC=%s, OIC=%s",
            ADB_ENABLED, OAC_ENABLED, OIC_ENABLED,
        )

        # If the config-load step at module import time failed, surface
        # the error now via a chat-friendly message. The user sees this
        # the first time they send a message after a broken deploy.
        if _CFG_INIT_ERROR:
            self._self_stage_error = (
                "config.yaml could not be loaded.\n\n"
                f"Error:\n{_CFG_INIT_ERROR}\n\n"
                "Make sure config.yaml is uploaded next to agent.py in the "
                "AIDP project, and that PyYAML is available in the runtime."
            )
            return

        if not (ADB_ENABLED or OAC_ENABLED or OIC_ENABLED):
            self._self_stage_error = (
                "No integration enabled in config.yaml. Set 'enabled: true' "
                "under [integrations.adb], [integrations.oac], or "
                "[integrations.oic]."
            )
            return

        if OAC_ENABLED:
            try:
                _stage_oac_private_key()
                self._oac_private_key = _load_oac_private_key()
                # Mint the first access_token at setup so the first invoke
                # doesn't pay the JWT-mint round-trip on top of MCP load.
                self._oac_access_token = _fetch_oac_access_token(self._oac_private_key)
            except Exception as e:
                self._self_stage_error = (
                    f"{type(e).__name__}: {e}\n\n"
                    f"Stack trace (last 800 chars):\n{traceback.format_exc()[-800:]}"
                )
                logger.error("OAC JWT setup failed: %s", e, exc_info=True)

        self.llm = init_oci_llm(llm_conf)
        self.graph = create_react_agent(
            self.llm, [], prompt=AGENT_SYSTEM_PROMPT, checkpointer=checkpointer,
        )
        logger.info("Stub graph ready; will load MCP tools on first invoke")

    # ── MCP loading (ephemeral sessions per tool call) ─────────
    async def _load_all_mcp_tools(self) -> None:
        """Build a fresh MultiServerMCPClient using the current cached
        bearer tokens, then ask it for the catalog of available tools.

        Important detail about how MultiServerMCPClient works in this
        sample: get_tools() uses **ephemeral sessions** — each tool
        invocation opens a new HTTPS request, runs, and closes. We never
        hold a persistent TCP connection that could be killed by a load
        balancer's idle timeout. Trade-off: ~200ms of TLS overhead per
        tool call. Worth it for not having to keep connections warm.

        Tokens are baked into request headers at client-build time, so
        when a token expires we just rebuild — driven on-demand by the
        reactive retry path in invoke() when a tool call fails with an
        auth or connection error. There's no scheduled refresh: every
        Oracle service we hit (ADB password grant, OAC JWT assertion,
        OIC client_credentials) can be re-authenticated independently
        from cached config + private key, with no rotating refresh chain
        to keep alive."""
        cfg = _build_mcp_config(self._oac_access_token)
        client = MultiServerMCPClient(cfg)
        tools = await client.get_tools()

        logger.info(
            "Loaded %d tool(s) from %d MCP server(s) [ephemeral sessions]: %s",
            len(tools), len(cfg), [t.name for t in tools],
        )
        # Re-create the LangGraph react agent with the freshly loaded
        # tools. The system prompt and checkpointer carry over so chat
        # state isn't lost when we rebuild.
        self.graph = create_react_agent(
            self.llm, tools, prompt=AGENT_SYSTEM_PROMPT, checkpointer=checkpointer,
        )
        self._tools_loaded = True

    # ── OAC token refresh (mint a fresh JWT-derived bearer) ─────
    async def _refresh_oac_token_and_rebuild(self) -> None:
        """Mint a new OAC access_token via the JWT User Assertion flow,
        then rebuild MCP so the fresh bearer is in the headers. No refresh
        chain — just sign a new JWT and exchange. Cheap and idempotent."""
        self._oac_access_token = await asyncio.to_thread(
            _fetch_oac_access_token, self._oac_private_key
        )
        logger.info("OAC access_token minted via JWT assertion")
        await self._load_all_mcp_tools()

    # ── Conversation state healing ──────────────────────────────
    async def _heal_orphan_tool_calls(self, config) -> int:
        """Clean up "orphan" tool-call records left in the conversation
        history by failed tool executions.

        Background: when the LLM asks to call a tool, LangGraph records
        an AIMessage with a `tool_calls` list. Normally this is followed
        by a ToolMessage holding the tool's response. Most LLM providers
        require this pairing — every tool_call must have a matching
        ToolMessage *immediately* after the AIMessage that asked for it.

        What goes wrong: if a tool execution crashes (network error,
        timeout, etc.) the AIMessage gets persisted but no ToolMessage
        follows. Then the user types another message, and now we send
        the LLM a history with [..., AIMessage(tool_calls), HumanMessage]
        — invalid sequence. Provider rejects it with INVALID_CHAT_HISTORY.

        Two ways to fix this:
          1. Append a fake ToolMessage at the end of history. Doesn't
             work — providers require IMMEDIATE adjacency, not
             eventual presence.
          2. Remove the orphan AIMessage entirely. Works — gap closes,
             history becomes valid again. We use RemoveMessage, which
             LangGraph's add_messages reducer recognizes as a deletion
             marker."""
        if not self.graph or not config:
            return 0
        try:
            state = await self.graph.aget_state(config)
        except Exception as e:
            logger.debug("Could not fetch state for orphan check: %s", e)
            return 0

        messages = state.values.get("messages", []) if state and state.values else []
        if not messages:
            return 0

        expected, fulfilled = set(), set()
        for msg in messages:
            for tc in getattr(msg, "tool_calls", None) or []:
                tcid = tc.get("id") if isinstance(tc, dict) else getattr(tc, "id", None)
                if tcid:
                    expected.add(tcid)
            tcid = getattr(msg, "tool_call_id", None)
            if tcid:
                fulfilled.add(tcid)

        orphans = expected - fulfilled
        if not orphans:
            return 0

        # Remove every AIMessage that contains an orphan tool_call. We use
        # RemoveMessage here (recognized by langgraph's add_messages reducer)
        # instead of appending healing ToolMessages, because providers
        # require ToolMessages to immediately follow their AIMessage —
        # appending at the end of history breaks that ordering rule.
        removals = []
        for msg in messages:
            msg_tc_ids = set()
            for tc in getattr(msg, "tool_calls", None) or []:
                tcid = tc.get("id") if isinstance(tc, dict) else getattr(tc, "id", None)
                if tcid:
                    msg_tc_ids.add(tcid)
            if msg_tc_ids & orphans:
                msg_id = getattr(msg, "id", None)
                if msg_id:
                    removals.append(RemoveMessage(id=msg_id))

        if not removals:
            logger.warning(
                "Detected %d orphan tool_call(s) but messages have no .id "
                "to remove them by; cannot heal automatically",
                len(orphans),
            )
            return 0

        logger.warning(
            "Removing %d orphan AIMessage(s) (cleared %d unmatched tool_call ids: %s)",
            len(removals), len(orphans), list(orphans),
        )
        await self.graph.aupdate_state(config, {"messages": removals})
        return len(removals)

    # ── Diagnostics & friendly messages ─────────────────────────
    def _runtime_diagnostics(self) -> str:
        lines = [
            f"Path.home() = {Path.home()}",
            f"euid/uid    = {os.geteuid()}/{os.getuid()}",
            f"USER env    = {os.environ.get('USER', '<unset>')}",
            f"cwd         = {Path.cwd()}",
            f"clock epoch = {int(time.time())}",
            "",
            f"enabled     : ADB={ADB_ENABLED}  OAC={OAC_ENABLED}  OIC={OIC_ENABLED}",
        ]
        if OAC_ENABLED:
            lines.append(
                f"OAC_PRIVATE_KEY: {OAC_PRIVATE_KEY} "
                f"({'EXISTS' if OAC_PRIVATE_KEY.exists() else 'MISSING'})"
            )
            try:
                access = self._oac_access_token or ""
                if access and access.count(".") == 2:
                    payload_b64 = access.split(".")[1]
                    payload_b64 += "=" * (-len(payload_b64) % 4)
                    payload = json.loads(base64.urlsafe_b64decode(payload_b64))
                    now = int(time.time())
                    exp = payload.get("exp")
                    lines.append("")
                    lines.append("In-memory OAC access token:")
                    lines.append(f"  first 30 : {access[:30]}...")
                    lines.append(f"  iat/exp  : {payload.get('iat')} / {exp}")
                    if exp:
                        delta = exp - now
                        lines.append(
                            f"  ⚠ EXPIRED ({-delta}s ago)" if delta < 0
                            else f"  ✓ valid for {delta}s"
                        )
            except Exception as e:
                lines.append(f"  (couldn't decode access_token: {e})")
        return "\n".join(lines)

    def _friendly_auth_message(self, exc: Exception) -> str | None:
        """Detect which integration's auth failed (by URL fragments / error
        shape) and return a per-source recovery message. Returns None if
        we can't classify the error — caller falls back to generic."""
        text = str(exc)
        text_lc = text.lower()

        # OAC JWT assertion — IDCS errors at /oauth2/v1/token, plus MCP
        # endpoint failures. We match by exact OAC token URL when we have
        # one; otherwise fall back to the generic IDCS path heuristic.
        if OAC_ENABLED:
            hit_oac_token_endpoint = bool(OAC_TOKEN_URL) and OAC_TOKEN_URL in text
            if hit_oac_token_endpoint:
                if "invalid_grant" in text_lc:
                    return self._oac_jwt_message(
                        "IDCS rejected the user assertion (invalid_grant) — "
                        "service user disabled, missing, or wrong email."
                    )
                if "invalid_client" in text_lc:
                    return self._oac_jwt_message(
                        "IDCS rejected the client assertion (invalid_client) — "
                        "cert not registered on the Confidential App, or wrong client_id."
                    )
                if "invalid_request" in text_lc or "signature" in text_lc:
                    return self._oac_jwt_message(
                        "IDCS rejected the JWT (invalid_request / signature) — "
                        "malformed JWT, wrong private key, or clock skew > 5 min."
                    )
                if "401" in text:
                    return self._oac_jwt_message(
                        "IDCS returned 401 on the JWT-bearer exchange — "
                        "check the Confidential App is Active and the cert is registered."
                    )
            if "/api/mcp" in text_lc and any(s in text for s in ("401", "403")):
                return self._oac_jwt_message(
                    "OAC's MCP endpoint rejected the access token. The JWT exchange "
                    "succeeded but the resulting bearer lacks permission — verify the "
                    "service user has access to the OAC datasets/subject areas."
                )

        # OIC — IDCS token endpoint or MCP endpoint failures
        if OIC_ENABLED:
            if "/oauth2/v1/token" in text_lc:
                if "invalid_scope" in text_lc:
                    return self._oic_message(
                        "OIC OAuth rejected the scope (invalid_scope).",
                        focus="scope",
                    )
                if "invalid_client" in text_lc or "inactive" in text_lc:
                    return self._oic_message(
                        "OIC OAuth rejected the client (invalid_client / inactive app).",
                        focus="client",
                    )
                if "401" in text:
                    return self._oic_message(
                        "OIC OAuth returned 401 — wrong client_id / client_secret.",
                        focus="client",
                    )
                return self._oic_message(
                    f"OIC OAuth call failed: {text[:200]}", focus="client",
                )
            if "/mcp-server/" in text_lc:
                if "403" in text:
                    return self._oic_message(
                        "OIC MCP returned 403 Forbidden — the token is valid but lacks "
                        "permission. Almost certainly the ServiceInvoker role mapping.",
                        focus="role",
                    )
                if "404" in text:
                    return self._oic_message(
                        "OIC MCP returned 404 — wrong project ID, or 'Enable MCP server' "
                        "is off on the project.",
                        focus="project",
                    )

        # ADB — auth endpoint or MCP endpoint failures
        if ADB_ENABLED:
            if "/adb/auth/" in text_lc and "401" in text:
                return self._adb_message(
                    "ADB rejected the password-grant credentials (401)."
                )
            if "/adb/mcp/" in text_lc and any(s in text for s in ("401", "403")):
                return self._adb_message(
                    "ADB MCP rejected the access token. Token may have expired between "
                    "fetch and use, or the user lacks Select AI execute privileges."
                )
            if "/adb/" in text_lc and "404" in text:
                return self._adb_message(
                    "ADB endpoint returned 404 — likely wrong OCID or region."
                )

        return None

    @staticmethod
    def _oac_jwt_message(reason: str) -> str:
        return (
            "🔑 OAC JWT authentication failed.\n\n"
            f"Reason: {reason}\n\n"
            "How to fix:\n"
            "  1. Verify the Confidential App in your OCI Identity Domain:\n"
            "       • Status is Active\n"
            "       • 'JWT Assertion' grant type is enabled\n"
            "       • Your X.509 cert (oac-cert.pem) is registered as a\n"
            "         Trusted Client on the app\n"
            "  2. Verify integrations.oac in config.yaml:\n"
            "       • client_id matches the Confidential App\n"
            "       • idcs_host matches your OCI Identity Domain\n"
            "       • service_user_email is an Active OAC user with the\n"
            "         permissions you want the agent to inherit\n"
            "  3. Verify the private key file (default: oac-jwt-key.txt) is\n"
            "     uploaded next to agent.py and matches the cert registered\n"
            "     in IDCS.\n"
            "  4. Use mint-and-exchange.py locally to test the full chain\n"
            "     before redeploying the agent.\n"
            "  5. Redeploy after fixing.\n"
        )

    @staticmethod
    def _oic_message(reason: str, focus: str) -> str:
        steps = {
            "role": (
                "  1. OCI Console → Identity → Domains → your domain →\n"
                "     Oracle Cloud Services → your OIC instance → Application Roles\n"
                "  2. Open ServiceInvoker → Assigned applications → assign the\n"
                "     Confidential App referenced in config.yaml (integrations.oic.client_id)\n"
                "  3. Save. The role propagates within ~60 seconds.\n"
                "  4. Re-send your message — no redeploy needed.\n"
                "\n"
                "Note: ServiceAdministrator alone is NOT enough — MCP requires ServiceInvoker."
            ),
            "scope": (
                "  1. Open the Confidential App in your OCI Identity Domain\n"
                "  2. OAuth Configuration → Token issuance policy → Resources\n"
                "  3. Copy BOTH scope values verbatim (urn:opc:resource:consumer::all\n"
                "     AND /ic/api/) — they use the IDCS-registered hash hostname,\n"
                "     NOT the user-facing design.* hostname\n"
                "  4. Paste them space-separated into integrations.oic.scope in config.yaml\n"
                "  5. Redeploy.\n"
            ),
            "client": (
                "  1. Verify integrations.oic.client_id and client_secret in config.yaml\n"
                "     match an Active Confidential Application in your OCI Identity Domain\n"
                "  2. App detail page → status badge should say 'Active'. If 'Inactive':\n"
                "     Actions → Activate.\n"
                "  3. Allowed grant types must include 'Client Credentials'.\n"
                "  4. Redeploy after fixing.\n"
            ),
            "project": (
                "  1. OIC Designer → open the project → pencil/Edit (top right) →\n"
                "     ☑ Enable MCP server → Save\n"
                "  2. Verify integrations.oic.mcp_url in config.yaml matches the URL shown\n"
                "     after saving. Canonical pattern:\n"
                "     https://<host>/mcp-server/v1/projects/<projectId>/mcp\n"
                "  3. Redeploy.\n"
            ),
        }[focus]
        return (
            "🔑 OIC authentication / authorization failed.\n\n"
            f"Reason: {reason}\n\n"
            "How to fix:\n"
            f"{steps}"
        )

    @staticmethod
    def _adb_message(reason: str) -> str:
        return (
            "🔑 ADB authentication failed.\n\n"
            f"Reason: {reason}\n\n"
            "How to fix:\n"
            "  1. Verify integrations.adb in config.yaml: ocid, region, user, password\n"
            "  2. Sanity check by logging into the same ADB via SQL Developer Web with\n"
            "     the same user/password\n"
            "  3. Confirm the user has Select AI privileges:\n"
            "       GRANT EXECUTE ON DBMS_CLOUD_AI TO <user>;\n"
            "  4. Redeploy after fixing config.yaml\n"
        )

    # ── Invoke (per user message) ───────────────────────────────
    async def invoke(self, user_query: str, **kwargs):
        """Called by AIDP for every chat message. The high-level flow:

        1. Bail out early if setup() captured an error (config or auth
           setup failed) — return that error as the chat reply.
        2. Run pre_invoke_setup() to configure auth context for OCI
           Generative AI.
        3. Lazy-load MCP tools the very first time we're invoked.
        4. Heal any orphan tool_calls left in conversation state by
           previously-failed tool executions.
        5. Run the actual react-agent graph.
        6. If the graph errored with something recoverable (auth/network),
           rebuild MCP, heal orphans, and retry once.

        There is NO scheduled token refresh. Every Oracle service we hit
        can be re-authenticated on demand from cached config + private
        key, with no rotating refresh chain to keep alive. When a tool
        call hits 401, step 6 mints a fresh bearer and retries — that's
        the entire freshness story."""

        # ─── Step 1: surface setup-time errors ─────────────────────
        if self._self_stage_error:
            return {"messages": [{"role": "ai", "content":
                f"Self-stage failed at setup:\n{self._self_stage_error}\n\n"
                f"Runtime diagnostics:\n{self._runtime_diagnostics()}"
            }]}

        # ─── Step 2: AIDP auth context for OCI Gen AI ──────────────
        # pre_invoke_setup wires the OCI signer into the request thread so
        # the LLM's HTTP calls get signed correctly. If it errors we soldier
        # on with an empty config — chat may still partially work.
        try:
            config = pre_invoke_setup(**kwargs)
        except Exception as e:
            logger.warning("pre_invoke_setup failed, using empty config: %s", e)
            config = {}

        # asyncio.Lock has to be created INSIDE a running event loop.
        # That's why we lazy-init here on the first invoke instead of
        # in __init__ (which runs at module import time, no loop).
        if self._load_lock is None:
            self._load_lock = asyncio.Lock()

        # ─── Step 3: lazy first-load of MCP tools ──────────────────
        # We don't load MCP at setup() time because setup is sync and MCP
        # loading is async. So the first chat message pays a one-time cost
        # of opening sessions and discovering tools. Subsequent messages
        # use the cached graph until something forces a rebuild (token
        # expiry caught by reactive retry in Step 6).
        # Double-checked locking: outer check is the fast path, the inner
        # check inside the lock prevents two concurrent first-invokes from
        # both running the load.
        if not self._tools_loaded:
            async with self._load_lock:
                if not self._tools_loaded:
                    try:
                        await self._load_all_mcp_tools()
                    except Exception as e:
                        logger.error("MCP tool load failed: %s", e, exc_info=True)
                        friendly = self._friendly_auth_message(e)
                        if friendly:
                            return {"messages": [{"role": "ai", "content": friendly}]}
                        return {"messages": [{"role": "ai", "content":
                            f"MCP setup error: {type(e).__name__}: {e}\n\n"
                            f"Runtime diagnostics:\n{self._runtime_diagnostics()}\n\n"
                            f"Stack trace (last 800 chars):\n{traceback.format_exc()[-800:]}"
                        }]}

        # ─── Step 4: heal orphan tool_calls from prior failures ────
        try:
            await self._heal_orphan_tool_calls(config)
        except Exception as e:
            logger.warning("Orphan healing failed (continuing): %s", e)

        # ─── Step 5: run the react-agent graph ─────────────────────
        # The graph handles the LLM ↔ tool-call ↔ LLM dance. It runs to
        # completion (final answer or error) before returning here.
        user_message = HumanMessage(content=user_query)
        messages = {"messages": [dict(user_message)]}

        try:
            return await self.graph.ainvoke(messages, config=config)
        except Exception as e:
            # ─── Step 6: reactive recovery ────────────────────────
            # If the failure looks transient (auth/network), try once more
            # after rebuilding MCP. This catches the common case where a
            # token expired in flight or a TCP socket got dropped.
            if _is_recoverable_error(e):
                logger.warning(
                    "Recoverable error mid-invoke (%s); rebuilding MCP and retrying",
                    type(e).__name__,
                )
                try:
                    # Rebuild under the lock so concurrent invokes don't
                    # both kick off a rebuild.
                    async with self._load_lock:
                        if OAC_ENABLED:
                            await self._refresh_oac_token_and_rebuild()
                        else:
                            await self._load_all_mcp_tools()
                    # The failed call may have left an orphan tool_call in
                    # conversation state — heal it before retrying or the
                    # LLM provider will reject the request.
                    try:
                        await self._heal_orphan_tool_calls(config)
                    except Exception as heal_err:
                        logger.warning("Orphan healing on retry failed: %s", heal_err)
                    return await self.graph.ainvoke(messages, config=config)
                except Exception as e2:
                    # Retry also failed. Surface a friendly message if
                    # we can identify the auth issue, else dump diagnostics.
                    friendly = self._friendly_auth_message(e2)
                    if friendly:
                        return {"messages": [{"role": "ai", "content": friendly}]}
                    logger.error("Retry after rebuild failed: %s", e2, exc_info=True)
                    return {"messages": [{"role": "ai", "content":
                        f"Agent error after rebuild retry: {type(e2).__name__}: {e2}\n\n"
                        f"Runtime diagnostics:\n{self._runtime_diagnostics()}"
                    }]}

            # Non-recoverable error path: log it and return the error text.
            # User sees a friendly-ish "Agent error: ..." message.
            logger.error("invoke error: %s", e, exc_info=True)
            return {"messages": [{"role": "ai", "content":
                f"Agent error: {type(e).__name__}: {e}"
            }]}
