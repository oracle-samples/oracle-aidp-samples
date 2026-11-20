"""Environment and constants.

No instance identifiers or secrets live in code. The DataLake OCID, workspace key and cluster key come
from environment variables (NS_*), which a gitignored ``.env`` next to the package may supply; see
``.env.example``. Auth itself comes from the OCI config profile named by NS_OCI_PROFILE (API key).
"""
from __future__ import annotations

import os
import pathlib
import re
from dataclasses import dataclass, field

_ENV_FILE = pathlib.Path(__file__).resolve().parents[1] / ".env"


def _load_dotenv(path: pathlib.Path = _ENV_FILE) -> None:
    """Minimal .env loader: KEY=VALUE lines, '#' comments, no interpolation; never overrides the process env."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


DEFAULT_OPS = "default.nightshift_ops"
_OPS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


@dataclass(frozen=True)
class Env:
    region: str = _env("NS_REGION", "us-ashburn-1")
    data_lake_id: str = _env("NS_DATA_LAKE_ID")  # required for live use; empty in unit tests
    workspace_id: str = _env("NS_WORKSPACE_ID")
    cluster_id: str = _env("NS_CLUSTER_ID")
    config_profile: str = _env("NS_OCI_PROFILE", "DEFAULT")
    # The ledger location, `<catalog>.<schema>`: NS_OPS, the one variable the scheduled job, the external actuator,
    # the self-protection rule and every script read. Read when the Env is built, not when this module is imported,
    # so a kernel that sets NS_OPS before a pass gets it.
    ops: str = field(default_factory=lambda: _env("NS_OPS", DEFAULT_OPS))
    probe_notebook: str = _env("NS_NOTEBOOK", "Shared/nightshift/_nightshift_runner.ipynb")

    def __post_init__(self) -> None:
        if not _OPS_RE.match(self.ops):
            raise ValueError(f"NS_OPS must be <catalog>.<schema> (plain identifiers), got {self.ops!r}")

    @property
    def catalog(self) -> str:
        return self.ops.split(".", 1)[0] if "." in self.ops else "default"

    @property
    def ops_schema(self) -> str:
        return self.ops.rsplit(".", 1)[-1]

    def require_live(self) -> None:
        """Raise a clear error before any live call when the instance identifiers are missing."""
        missing = [n for n, v in (("NS_DATA_LAKE_ID", self.data_lake_id), ("NS_WORKSPACE_ID", self.workspace_id),
                                  ("NS_CLUSTER_ID", self.cluster_id)) if not v]
        if missing:
            raise RuntimeError(f"Night Shift live mode needs {', '.join(missing)} (set them in the environment or .env; "
                               "see .env.example)")


# Drafting models per tier: model ids as returned by SHOW MODELS on your instance.
# NS_DRAFT_MODEL serves the high and normal tiers, NS_DRAFT_MODEL_LOW the low_compute tier.
MODEL_LADDER = {
    "high": _env("NS_DRAFT_MODEL", "xai.grok-4.7"),
    "normal": _env("NS_DRAFT_MODEL", "xai.grok-4.7"),
    "low_compute": _env("NS_DRAFT_MODEL_LOW", "xai.grok-4.20-non-reasoning"),
    "critical": None,
    "paused": None,
}

TIERS = ("high", "normal", "low_compute", "critical", "paused")

# summarizeMetricsData accepted values
METRIC_AGGREGATIONS = ("MEAN", "SUM", "MAX", "MIN", "RATE")
METRIC_INTERVALS = ("1m", "5m", "15m", "1h", "2h", "1d")
METRIC_NAMES = (
    "CpuUtilization", "MemoryUtilization", "FileSystemUtilization", "GcCpuUtilization", "JvmHeapUsed",
    "DiskReadBytes", "DiskWriteBytes", "NetworkReceiveBytes", "NetworkTransmitBytes",
    "ActiveTasks", "TotalFailedTasks", "TotalCompletedTasks", "TotalTasks",
    "shuffleTotalBytesRead", "TotalShuffleWriteBytes",
)

JOB_CRON_MIN_MINUTES = 30
