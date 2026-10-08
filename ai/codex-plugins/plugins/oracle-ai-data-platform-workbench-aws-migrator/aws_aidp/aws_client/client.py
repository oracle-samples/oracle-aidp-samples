"""boto3 wrapper. Lazy import so `aws-aidp --help` works even without boto3.

One AwsClient per region — boto3 sessions are cheap, regional services need region-scoped clients.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass
class AwsConfig:
    profile: str | None
    region: str

    @classmethod
    def from_env(cls, region: str | None = None) -> "AwsConfig":
        return cls(
            profile=os.environ.get("AWS_PROFILE"),
            region=region or os.environ.get("AWS_REGION") or "us-east-1",
        )


class AwsClient:
    """Per-region boto3 client cache. Resolves auth via the standard chain."""

    def __init__(self, cfg: AwsConfig | None = None) -> None:
        self.cfg = cfg or AwsConfig.from_env()
        self._session: Any = None
        self._clients: dict[str, Any] = {}

    @property
    def session(self):
        if self._session is None:
            try:
                import boto3
            except ImportError as e:
                raise RuntimeError("`boto3` is required (it's in install_requires)") from e
            self._session = boto3.Session(profile_name=self.cfg.profile, region_name=self.cfg.region)
        return self._session

    def client(self, service: str):
        if service not in self._clients:
            self._clients[service] = self.session.client(service)
        return self._clients[service]

    def account_id(self) -> str:
        """Resolve the calling AWS account id via STS — also validates creds."""
        sts = self.client("sts")
        return sts.get_caller_identity()["Account"]


class AwsAuthError(RuntimeError):
    """Raised when AWS credentials are missing or invalid."""
    pass


def check_creds(cfg: AwsConfig | None = None) -> str:
    """Return account id or raise AwsAuthError with a clean message."""
    client = AwsClient(cfg)
    try:
        return client.account_id()
    except Exception as e:
        raise AwsAuthError(
            f"AWS credentials check failed: {e}. "
            "Configure via `aws configure`, AWS_PROFILE env, or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        ) from e
