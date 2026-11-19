"""Data models for AIDP deployment."""

from dataclasses import dataclass, field


@dataclass
class DeployConfig:
    """Configuration for deploying to AIDP.

    Authentication is OCI request signing (``AIDPClient.make_signer``:
    Resource Principal -> Instance Principal -> Session Token -> API Key
    from ``~/.oci/config``). There is no host/token pair: the earlier
    ``host``/``token`` fields were never consumed by the client and the
    values documented for them (``AIDP_HOST``/``AIDP_ACCESS_TOKEN``) were
    read by nothing.

    ``workspace_path`` is the folder under the AIDP workspace root that
    notebooks land in, spelled the way AIDP job definitions address a
    notebook (``/Workspace/...``).
    """
    region: str = ""                    # e.g. us-ashburn-1 (AIDP_REGION)
    instance_id: str = ""               # DataLake OCID (AIDP_INSTANCE_ID)
    workspace_key: str = ""             # workspace key/OCID (AIDP_WORKSPACE_KEY)
    oci_profile: str = "DEFAULT"        # ~/.oci/config profile (OCI_PROFILE)
    workspace_path: str = "/Workspace/Migrated"  # base path in the workspace
    cluster_key: str = ""               # cluster the job tasks run on (AIDP_CLUSTER_KEY)
    overwrite: bool = False             # Overwrite existing notebooks
    create_workflow: bool = True        # Create AIDP jobs from workflows/*.json
    dry_run: bool = False               # Preview what would be deployed without deploying

    def __post_init__(self):
        # Every remote path is built by string-prefixing workspace_path, and
        # the job body addresses notebooks as /Workspace/<folder>/<name>.
        # Nothing checked the prefix, so when Git Bash rewrote
        # "/Workspace/Migrated" into "C:/Program Files/Git/Workspace/Migrated"
        # the tool created a "C:" folder in the workspace and a job pointing
        # at it (live run, 2026-09-24). Normalise and refuse anything that is
        # not under /Workspace.
        raw = self.workspace_path or ""
        path = "/" + raw.replace("\\", "/").strip("/")
        if path != "/Workspace" and not path.startswith("/Workspace/"):
            hint = ""
            if "/Git/Workspace" in path or ":/" in path:
                hint = (" This looks like a Git Bash path rewrite: run with "
                        "MSYS_NO_PATHCONV=1 or pass //Workspace/...")
            raise ValueError(
                f"workspace_path must be /Workspace or below it, got {raw!r}.{hint}"
            )
        self.workspace_path = path

    @staticmethod
    def from_env():
        """Load config from environment variables (after ``infa2aidp.config``
        has loaded ``.env``)."""
        import os
        return DeployConfig(
            region=os.environ.get("AIDP_REGION", ""),
            instance_id=os.environ.get("AIDP_INSTANCE_ID", ""),
            workspace_key=os.environ.get("AIDP_WORKSPACE_KEY", ""),
            oci_profile=os.environ.get("OCI_PROFILE", "DEFAULT"),
            cluster_key=os.environ.get("AIDP_CLUSTER_KEY", ""),
            workspace_path=os.environ.get("AIDP_WORKSPACE_PATH", "/Workspace/Migrated"),
        )


@dataclass
class DeployedNotebook:
    local_path: str = ""
    remote_path: str = ""
    mapping_name: str = ""
    folder: str = ""          # source-derived organizational unit, or "Migrated"
    status: str = "pending"  # pending, uploaded, failed, skipped, dry_run
    error: str = ""


@dataclass
class DeployedWorkflow:
    name: str = ""
    job_id: str = ""
    tasks: list = field(default_factory=list)
    status: str = "pending"
    error: str = ""
    # Path to the companion review report, when the source workflow did not
    # translate completely. Empty means the translation was whole.
    review_file: str = ""


@dataclass
class DeployResult:
    notebooks: list = field(default_factory=list)   # List[DeployedNotebook]
    workflows: list = field(default_factory=list)    # List[DeployedWorkflow]
    total_uploaded: int = 0
    total_updated: int = 0    # jobs replaced in place under --overwrite
    total_failed: int = 0
    total_skipped: int = 0    # already present and --overwrite not given
    total_dry_run: int = 0    # would have been deployed (dry run only)
    dry_run: bool = False
