"""AIDP REST client — OCI request signing + the endpoints we need.

Requires the `oci` and `requests` Python packages. Install with:
    pip install -e '.[oci]'
"""
from __future__ import annotations

import os
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlsplit


_API_VERSION = "20260430"
_API_COLLECTION_ROOTS = {
    # Oracle's generated 20260430 data-plane client pairs this version with
    # the ``datalake`` service hostname and ``aiDataPlatforms`` collection.
    "20260430": (
        "https://datalake.{region}.oci.oraclecloud.com"
        "/20260430/aiDataPlatforms"
    ),
}
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_REGION_RE = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)+")
_PARAMETER_NAME_RE = re.compile(r"[A-Za-z0-9_.-]+")


@dataclass
class AidpConfig:
    profile: str          # ~/.oci/config profile name
    region: str           # e.g. "ap-mumbai-1"
    datalake_ocid: str    # ocid1.aidataplatform...
    workspace_id: str     # UUID (NOT an OCID despite its history)
    api_version: str | None = None
    endpoint: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.profile, str) or not self.profile:
            raise ValueError("OCI profile must be a non-empty string")
        if not isinstance(self.region, str) or _REGION_RE.fullmatch(self.region) is None:
            raise ValueError("OCI region must contain lowercase letters, digits, and hyphens")
        _validated_key(self.datalake_ocid, "AI Data Platform ID", 500)
        _validated_key(self.workspace_id, "workspace key", 200)
        if self.endpoint is not None:
            if self.api_version is not None:
                raise ValueError(
                    "set either AIDP endpoint or API version, not both"
                )
            self.endpoint = _validated_endpoint(self.endpoint)
        else:
            self.api_version = _API_VERSION if self.api_version is None else self.api_version
            if (
                not isinstance(self.api_version, str)
                or re.fullmatch(r"\d{8}", self.api_version) is None
            ):
                raise ValueError("AIDP API version must be an 8-digit date")
            if self.api_version not in _API_COLLECTION_ROOTS:
                raise ValueError(
                    f"unsupported AIDP API version {self.api_version!r}; "
                    "set AIDP_ENDPOINT to the full versioned collection root"
                )

    @classmethod
    def from_env(cls) -> "AidpConfig":
        from aws_aidp._env import require
        e = require("OCI_PROFILE", "OCI_REGION", "DATALAKE_OCID", "WORKSPACE_ID")
        return cls(
            profile=e["OCI_PROFILE"],
            region=e["OCI_REGION"],
            datalake_ocid=e["DATALAKE_OCID"],
            workspace_id=e["WORKSPACE_ID"],
            # `.env` templates ship these keys with empty values; blank means unset.
            api_version=os.environ.get("AIDP_API_VERSION") or None,
            endpoint=os.environ.get("AIDP_ENDPOINT") or None,
        )

    @property
    def base(self) -> str:
        """Full versioned collection root, including its service hostname."""
        if self.endpoint is not None:
            return self.endpoint
        return _API_COLLECTION_ROOTS[self.api_version].format(region=self.region)

    @property
    def ws_base(self) -> str:
        return (
            f"{self.base}/{quote(self.datalake_ocid, safe='')}"
            f"/workspaces/{quote(self.workspace_id, safe='')}"
        )


class AidpClient:
    """Thin wrapper around AIDP REST. One signer, focused method surface."""

    def __init__(
        self,
        cfg: AidpConfig | None = None,
        *,
        timeout: float = 60.0,
        max_retries: int = 2,
        retry_backoff: float = 0.25,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if max_retries < 0:
            raise ValueError("max_retries must be zero or greater")
        if retry_backoff < 0:
            raise ValueError("retry_backoff must be zero or greater")
        self.cfg = cfg or AidpConfig.from_env()
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self._signer = None     # lazy

    @property
    def signer(self):
        if self._signer is None:
            try:
                import oci
            except ImportError as e:
                raise RuntimeError(
                    "the `oci` package is required for AIDP API calls. "
                    "install with: pip install -e '.[oci]'"
                ) from e
            config = oci.config.from_file(profile_name=self.cfg.profile)
            self._signer = oci.signer.Signer(
                tenancy=config["tenancy"],
                user=config["user"],
                fingerprint=config["fingerprint"],
                private_key_file_location=config["key_file"],
                pass_phrase=oci.config.get_config_value_or_default(config, "pass_phrase"),
            )
        return self._signer

    def _req(
        self,
        method: str,
        url: str,
        *,
        json: Any = None,
        expect: tuple[int, ...] = (200,),
        headers: dict[str, str] | None = None,
    ) -> dict:
        try:
            import requests
        except ImportError as e:
            raise RuntimeError("the `requests` package is required") from e
        method = method.upper()
        request_headers = {"Content-Type": "application/json", **(headers or {})}
        can_retry = method in {"GET", "HEAD", "OPTIONS"} or "opc-retry-token" in request_headers
        attempt = 0
        while True:
            try:
                r = requests.request(
                    method, url, json=json, auth=self.signer, timeout=self.timeout,
                    headers=request_headers,
                )
            except Exception as e:
                if can_retry and attempt < self.max_retries:
                    time.sleep(self.retry_backoff * (2 ** attempt))
                    attempt += 1
                    continue
                raise AidpError(
                    f"{method} {url} failed before receiving a response: {e}",
                    status=0,
                    body="",
                ) from e
            if r.status_code in _RETRYABLE_STATUS and can_retry and attempt < self.max_retries:
                time.sleep(self.retry_backoff * (2 ** attempt))
                attempt += 1
                continue
            break
        if r.status_code not in expect:
            raise AidpError(f"{method} {url} -> {r.status_code} {r.text[:500]}", status=r.status_code, body=r.text)
        if not r.text:
            return {}
        try:
            payload = r.json()
        except ValueError:
            return {"_raw": r.text}
        if not isinstance(payload, dict):
            raise AidpError(
                f"{method} {url} returned a non-object JSON response",
                status=r.status_code,
                body=r.text,
            )
        return payload

    # --- jobs / runs ---

    def get_job(self, job_key: str) -> dict:
        _validated_key(job_key, "job key", 255)
        return self._req("GET", f"{self.cfg.ws_base}/jobs/{quote(job_key, safe='')}")

    def get_job_run(self, run_key: str) -> dict:
        _validated_key(run_key, "run key", 255)
        return self._req("GET", f"{self.cfg.ws_base}/jobRuns/{quote(run_key, safe='')}")

    def create_job_run(
        self,
        job_key: str,
        parameters: list | dict | None = None,
        *,
        retry_token: str | None = None,
    ) -> str:
        """Trigger a new run. Returns the run key. Expects 201."""
        _validated_key(job_key, "job key", 255)
        body = {"jobKey": job_key, "parameters": _normalize_parameters(parameters)}
        token = _validated_retry_token(retry_token or uuid.uuid4().hex)
        resp = self._req(
            "POST", f"{self.cfg.ws_base}/jobRuns", json=body, expect=(201,),
            headers={"opc-retry-token": token},
        )
        run_key = resp.get("key")
        if not isinstance(run_key, str) or not run_key:
            raise AidpError(f"POST /jobRuns returned no `key`: {resp}", status=201, body=str(resp))
        return run_key

    def repair_job_run(
        self,
        run_key: str,
        task_keys: list[str],
        *,
        retry_token: str | None = None,
    ) -> dict:
        """Repair-run with ONLY the given failed task keys."""
        _validated_key(run_key, "run key", 255)
        keys = list(dict.fromkeys(task_keys))
        if not keys or len(keys) > 100:
            raise ValueError("task_keys must contain 1-100 non-empty strings")
        for key in keys:
            _validated_key(key, "task key", 255)
        body = {"taskKeys": keys}
        return self._req("POST", f"{self.cfg.ws_base}/jobRuns/{quote(run_key, safe='')}/actions/repair",
                         json=body, expect=(200, 201, 202),
                         headers={"opc-retry-token": _validated_retry_token(retry_token or uuid.uuid4().hex)})

    # --- clusters (read-only for now; sizing comes in a follow-up) ---

    def get_cluster(self, cluster_id: str) -> dict:
        _validated_key(cluster_id, "cluster ID", 255)
        return self._req("GET", f"{self.cfg.ws_base}/clusters/{quote(cluster_id, safe='')}")


class AidpError(Exception):
    def __init__(self, message: str, *, status: int, body: str) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


def _normalize_parameters(parameters: list | dict | None) -> list:
    """Convert the CLI-friendly mapping form into Oracle's Parameter array."""
    if parameters is None:
        return []
    if isinstance(parameters, dict):
        # Do not stringify invalid data.  Oracle's list form requires a string
        # name and (when supplied) a string value, so the convenience mapping
        # must enforce the exact same contract.
        values = [{"name": name, "value": value} for name, value in parameters.items()]
    elif isinstance(parameters, list):
        values = []
        for parameter in parameters:
            if not isinstance(parameter, dict):
                raise ValueError("each parameter must be an object with a valid name and optional string value")
            unknown = set(parameter) - {"name", "value"}
            if unknown:
                raise ValueError(
                    "unsupported parameter field(s): " + ", ".join(sorted(unknown))
                )
            values.append(dict(parameter))
    else:
        raise TypeError("parameters must be a list, mapping, or None")
    if len(values) > 200:
        raise ValueError("parameters cannot contain more than 200 entries")
    for value in values:
        name = value.get("name")
        if not isinstance(name, str) or _PARAMETER_NAME_RE.fullmatch(name) is None:
            raise ValueError(
                "parameter names may contain only letters, digits, '_', '-', and '.'"
            )
        if "value" in value and not isinstance(value["value"], str):
            raise ValueError("parameter values must be strings")
    return values


def _validated_key(value: str, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{label} must be a non-empty string of at most {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{label} cannot contain control characters")
    return value


def _validated_endpoint(value: str) -> str:
    """Validate an explicit full versioned collection root and trim slashes.

    The override owns both the host and path prefix.  We therefore never append
    ``AIDP_API_VERSION`` to it, which prevents constructing an unverified
    host/version pairing for private, sovereign, or future service endpoints.
    """
    if not isinstance(value, str) or not value or any(
        character.isspace() or ord(character) < 32 or ord(character) == 127
        for character in value
    ):
        raise ValueError(
            "AIDP endpoint must be a non-empty HTTPS collection URL without whitespace"
        )
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("AIDP endpoint contains an invalid port") from error
    authority = parsed.netloc.rsplit("@", 1)[-1]
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or authority.endswith(":")
        or port == 0
        or parsed.path in {"", "/"}
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
    ):
        raise ValueError(
            "AIDP endpoint must be an HTTPS collection root with a path and without credentials, query, or fragment"
        )
    return value.rstrip("/")


def _validated_retry_token(value: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError("OCI retry token must be a non-empty string of at most 64 characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError("OCI retry token cannot contain control characters")
    return value
