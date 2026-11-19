"""
AIDP Path Translator
====================
Translates Databricks/AWS storage paths to OCI equivalents.
This is the CRITICAL piece - thousands of path-translation issues across categories.

Path types handled:
- dbfs:/...              -> oci://bucket@namespace/...
- /dbfs/...              -> oci://bucket@namespace/...
- /mnt/<mount>/...       -> oci://bucket@namespace/... (via mount mapping)
- s3://bucket/path       -> oci://bucket@namespace/path (via bucket mapping)
- s3a://bucket/path      -> oci://bucket@namespace/path
- /Workspace/...         -> /home/datascience/... or AIDP workspace path
- /Volumes/...           -> oci://bucket@namespace/... (Unity Catalog Volumes)
"""

import os
import json
import re
from typing import Dict, Optional, Tuple


class PathTranslator:
    """Translates Databricks/AWS paths to OCI AIDP paths."""

    def __init__(self, config_path: str = None):
        # Mount point mappings: /mnt/name -> oci://bucket@namespace/prefix
        self._mounts: Dict[str, str] = {}

        # S3 bucket mappings: s3://bucket -> oci://bucket@namespace
        self._s3_mappings: Dict[str, str] = {}

        # DBFS root mapping
        self._dbfs_root: str = ""

        # Workspace root mapping
        self._workspace_root: str = "/home/datascience"

        # Load config
        self._load_config(config_path)

    def _load_config(self, config_path: str = None):
        """Load path mappings from config file or environment."""
        # Try config file
        config_path = config_path or os.environ.get(
            "AIDP_PATH_CONFIG",
            "/opt/aidp/config/path_mappings.json"
        )

        if config_path and os.path.exists(config_path):
            with open(config_path) as f:
                config = json.load(f)
                self._mounts = config.get("mounts", {})
                self._s3_mappings = config.get("s3_buckets", {})
                self._dbfs_root = config.get("dbfs_root", "")
                self._workspace_root = config.get("workspace_root", "/home/datascience")

        # Environment overrides
        mounts_json = os.environ.get("AIDP_MOUNT_MAPPINGS")
        if mounts_json:
            try:
                self._mounts.update(json.loads(mounts_json))
            except json.JSONDecodeError:
                pass

        s3_json = os.environ.get("AIDP_S3_MAPPINGS")
        if s3_json:
            try:
                self._s3_mappings.update(json.loads(s3_json))
            except json.JSONDecodeError:
                pass

        self._workspace_root = os.environ.get("AIDP_WORKSPACE_ROOT", self._workspace_root)
        self._dbfs_root = os.environ.get("AIDP_DBFS_ROOT", self._dbfs_root)

    def add_mount(self, mount_point: str, oci_path: str):
        """Add or update a mount point mapping."""
        self._mounts[mount_point] = oci_path

    def add_s3_mapping(self, s3_bucket: str, oci_path: str):
        """Add or update an S3 bucket mapping."""
        self._s3_mappings[s3_bucket] = oci_path

    def translate(self, path: str) -> str:
        """Translate any path to its AIDP equivalent.

        Returns the original path if no translation is needed/possible.
        """
        if not isinstance(path, str):
            return path

        original = path

        # Already an OCI path
        if path.startswith("oci://"):
            return path

        # dbfs: prefix
        if path.startswith("dbfs:"):
            path = path[5:]  # Strip dbfs:
            return self._translate_dbfs(path)

        # /dbfs/ prefix
        if path.startswith("/dbfs/"):
            path = path[5:]  # Strip /dbfs (keep leading /)
            return self._translate_dbfs(path)

        # Databricks root-level aliases for /dbfs/ paths.
        # In Databricks /FileStore/x resolves to /dbfs/FileStore/x because
        # /dbfs is mounted at root. On AIDP no such mount exists, so these
        # bare paths must be translated to the same Volume location as the
        # corresponding /dbfs/ path. Honors AIDP_DBFS_CATALOG/SCHEMA env vars
        # via the shared _translate_dbfs helper (no hard-coded /default/default).
        for _dbfs_alias in ("/FileStore/",):
            if path.startswith(_dbfs_alias):
                # Pass the path through unchanged — _translate_dbfs prepends
                # the configured /Volumes/{catalog}/{schema}/dbfs root.
                return self._translate_dbfs(path)

        # /Volumes/ (Unity Catalog Volumes)
        if path.startswith("/Volumes/"):
            return self._translate_volumes(path)

        # /mnt/ paths
        if path.startswith("/mnt/"):
            return self._translate_mount(path)

        # S3 paths
        if path.startswith("s3://") or path.startswith("s3a://"):
            return self._translate_s3(path)

        # /Workspace/ paths
        if path.startswith("/Workspace/") or path.startswith("/Workspace"):
            return self._translate_workspace(path)

        # HDFS paths pass through
        if path.startswith("hdfs://"):
            return path

        # Local paths pass through
        if path.startswith("/") or path.startswith("file://"):
            return path

        return original

    def _translate_dbfs(self, path: str) -> str:
        """Translate DBFS path to AIDP /Volumes mount.

        In Databricks, /dbfs/ and dbfs:/ are filesystem views of DBFS (backed by S3).
        In AIDP, the equivalent Volume is mounted at:
            /Volumes/{catalog}/{schema}/dbfs/
        where catalog and schema default to 'default'.

        So:
            /dbfs/FileStore/x  →  /Volumes/default/default/dbfs/FileStore/x
            dbfs:/FileStore/x  →  /Volumes/default/default/dbfs/FileStore/x

        Override via AIDP_DBFS_PREFIX env var or 'dbfs_root' in the path config file.
        """
        # Check if it's a mount point reference
        if path.startswith("/mnt/"):
            return self._translate_mount(path)

        # Explicit dbfs_root takes priority (from config file or env var)
        if self._dbfs_root:
            return f"{self._dbfs_root.rstrip('/')}{path}"

        # Default AIDP convention: /Volumes/{catalog}/{schema}/dbfs
        # catalog/schema default to 'default'; override via env vars
        catalog = os.environ.get("AIDP_DBFS_CATALOG", "default")
        schema = os.environ.get("AIDP_DBFS_SCHEMA", "default")
        aidp_prefix = os.environ.get(
            "AIDP_DBFS_PREFIX",
            f"/Volumes/{catalog}/{schema}/dbfs"
        )
        return f"{aidp_prefix.rstrip('/')}{path}"

    def _translate_mount(self, path: str) -> str:
        """Translate mount point path."""
        # Extract mount name: /mnt/<name>/rest/of/path
        parts = path.split("/", 3)
        if len(parts) >= 3:
            mount_name = f"/mnt/{parts[2]}"
            remainder = parts[3] if len(parts) > 3 else ""

            if mount_name in self._mounts:
                oci_base = self._mounts[mount_name].rstrip("/")
                return f"{oci_base}/{remainder}" if remainder else oci_base

        # No mapping found
        return path

    def _translate_s3(self, path: str) -> str:
        """Translate S3 path to OCI."""
        # Parse: s3://bucket/key or s3a://bucket/key
        match = re.match(r's3a?://([^/]+)(/.*)?', path)
        if not match:
            return path

        bucket = match.group(1)
        key = (match.group(2) or "").lstrip("/")

        # Check bucket mapping
        if bucket in self._s3_mappings:
            oci_base = self._s3_mappings[bucket].rstrip("/")
            return f"{oci_base}/{key}" if key else oci_base

        # Try with s3:// prefix in mappings
        s3_key = f"s3://{bucket}"
        if s3_key in self._s3_mappings:
            oci_base = self._s3_mappings[s3_key].rstrip("/")
            return f"{oci_base}/{key}" if key else oci_base

        return path

    def _translate_workspace(self, path: str) -> str:
        """Translate /Workspace/ paths."""
        relative = path.replace("/Workspace", "", 1)
        return f"{self._workspace_root}{relative}"

    def _translate_volumes(self, path: str) -> str:
        """Translate /Volumes/ paths (Unity Catalog Volumes)."""
        # /Volumes/catalog/schema/volume/path -> oci://...
        parts = path.split("/", 5)
        if len(parts) >= 5:
            catalog = parts[2]
            schema = parts[3]
            volume = parts[4]
            remainder = parts[5] if len(parts) > 5 else ""

            # Check for specific volume mapping
            volume_key = f"/Volumes/{catalog}/{schema}/{volume}"
            if volume_key in self._mounts:
                oci_base = self._mounts[volume_key].rstrip("/")
                return f"{oci_base}/{remainder}" if remainder else oci_base

        return path

    def save_config(self, path: str = None):
        """Save current mappings to config file."""
        path = path or os.environ.get("AIDP_PATH_CONFIG", "/opt/aidp/config/path_mappings.json")
        config = {
            "mounts": self._mounts,
            "s3_buckets": self._s3_mappings,
            "dbfs_root": self._dbfs_root,
            "workspace_root": self._workspace_root
        }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            json.dump(config, f, indent=2)


# Global instance
_translator = PathTranslator()


def translate_path(path: str) -> str:
    """Convenience function for path translation."""
    return _translator.translate(path)
