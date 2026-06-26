"""
AIDP File System Utils - Replacement for dbutils.fs
====================================================
Maps Databricks DBFS operations to:
1. OCI Object Storage (for oci:// and s3:// paths) via Hadoop FS
2. HDFS (for hdfs:// paths)
3. Local filesystem (for /tmp, file://, /Workspace paths)
4. AIDP workspace storage (for dbfs:/ paths -> mapped to OCI)

Confirmed AIDP environment (tested 2026-03-22):
- Hadoop FS via Spark JVM: AVAILABLE
- OCI HDFS connector (BmcFilesystem): AVAILABLE
- oci:// scheme: WORKS with OCI API key auth (config file)
- BmcFilesystem auth: API key via /Workspace/<oci-config-workspace-path> (DEFAULT profile)
- Resource principal auth is NOT used on AIDP (known failure modes; forbidden by policy)
- /Workspace filesystem: direct local access

Path Translation:
- dbfs:/mnt/<mount>/<path> -> oci://<bucket>@<namespace>/<path>
- s3://bucket/path -> oci://bucket@namespace/path (via mount mapping)
- /dbfs/path -> oci://workspace-bucket@namespace/path
"""

import os
import re
import json
import shutil
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Tuple


# â”€â”€ OCI Object Storage client (API key auth via CLI config file) â”€â”€
# Used by all fs operations on oci:// paths. NEVER uses
# oci.auth.signers.get_resource_principals_signer() â€” resource principal has
# known failure modes on AIDP (see feedback memory). Override the config file
# location via OCI_CONFIG_FILE / OCI_CONFIG_PROFILE env vars.
_AIDP_OCI_CONFIG_FILE = os.environ.get(
    "OCI_CONFIG_FILE", "/Workspace/<oci-config-workspace-path>"
)
_AIDP_OCI_CONFIG_PROFILE = os.environ.get("OCI_CONFIG_PROFILE", "DEFAULT")
_oci_storage_client = None
_oci_storage_namespace = None
_oci_storage_region = None


def _get_oci_storage_client():
    """Return a cached OCI ObjectStorageClient (API key auth).
    Also populates _oci_storage_region from the loaded config so copy_object
    calls (which require destination_region) can use it without re-reading."""
    global _oci_storage_client, _oci_storage_namespace, _oci_storage_region
    if _oci_storage_client is None:
        import oci
        config = oci.config.from_file(_AIDP_OCI_CONFIG_FILE, _AIDP_OCI_CONFIG_PROFILE)
        signer = oci.signer.Signer(
            tenancy=config["tenancy"],
            user=config["user"],
            fingerprint=config["fingerprint"],
            private_key_file_location=config["key_file"],
            pass_phrase=oci.config.get_config_value_or_default(config, "pass_phrase"),
        )
        _oci_storage_client = oci.object_storage.ObjectStorageClient(
            config=config, signer=signer)
        # Region is required by copy_object (destination_region). Read from
        # config; fall back to the client's base_client region if config
        # doesn't specify it.
        _oci_storage_region = (
            config.get("region")
            or getattr(getattr(_oci_storage_client, "base_client", None), "region", None)
        )
    return _oci_storage_client


def _get_oci_storage_region() -> str:
    """Return the OCI region for the cached storage client. Initializes
    the client lazily if not done already."""
    if _oci_storage_region is None:
        _get_oci_storage_client()
    return _oci_storage_region


# oci://<bucket>@<namespace>/<object_key>
_OCI_URI_RE = re.compile(r"^oci://(?P<bucket>[^/@\s]+)@(?P<ns>[^/\s]+)(?:/(?P<key>.*))?$")


def _parse_oci_uri(path: str) -> Optional[Tuple[str, str, str]]:
    """Parse oci://bucket@namespace/key into (bucket, namespace, key).
    Returns None if `path` is not an OCI URI."""
    m = _OCI_URI_RE.match(path.rstrip())
    if not m:
        return None
    return (m.group("bucket"), m.group("ns"), m.group("key") or "")


def _is_oci_uri(path: str) -> bool:
    return _parse_oci_uri(path) is not None


def _is_already_exists_error(exc: BaseException) -> bool:
    """Detect 'already exists' style errors across backends.

    Python's standard `FileExistsError` is caught by `os.makedirs(exist_ok=True)`,
    but AIDP's /Volumes FUSE mount and OCI Object Storage can raise non-standard
    exceptions for the same condition (`VolumeFileAlreadyExistsException`,
    OCI ServiceError code 'BucketAlreadyExists'/'ObjectAlreadyExists', etc.).
    Used by mkdirs to stay idempotent like Databricks' dbutils.fs.mkdirs.
    """
    if isinstance(exc, FileExistsError):
        return True
    msg = str(exc).lower()
    if "already exists" in msg or "alreadyexists" in msg:
        return True
    if "fileexists" in msg or "filealreadyexists" in msg:
        return True
    return False


@dataclass
class FileInfo:
    """Mirrors Databricks FileInfo."""
    path: str
    name: str
    size: int
    modificationTime: int = 0
    isDir: bool = False
    isFile: bool = True


@dataclass
class MountInfo:
    """Mirrors Databricks MountInfo."""
    mountPoint: str
    source: str
    encryptionType: str = ""


class AIDPFileSystemUtils:
    """Drop-in replacement for dbutils.fs on AIDP."""

    def __init__(self, spark=None):
        self._spark = spark
        self._mounts = {}
        self._mount_config_file = os.environ.get(
            "AIDP_MOUNT_CONFIG",
            "/opt/aidp/config/mounts.json"
        )
        self._load_mount_config()

    def _load_mount_config(self):
        """Load mount point mappings from config."""
        # Check environment variable overrides
        mount_json = os.environ.get("AIDP_MOUNTS_JSON")
        if mount_json:
            try:
                self._mounts = json.loads(mount_json)
                return
            except json.JSONDecodeError:
                pass

        # Check config file
        if os.path.exists(self._mount_config_file):
            with open(self._mount_config_file) as f:
                self._mounts = json.load(f)

    def _translate_path(self, path: str) -> str:
        """Translate Databricks paths to AIDP-compatible paths."""
        # Strip dbfs: prefix
        if path.startswith("dbfs:"):
            path = path[5:]

        # Handle /dbfs/ prefix
        if path.startswith("/dbfs/"):
            path = path[5:]

        # Handle mount points: /mnt/<mount_name>/... -> oci://...
        if path.startswith("/mnt/"):
            parts = path.split("/", 3)
            if len(parts) >= 3:
                mount_name = parts[2]
                remainder = parts[3] if len(parts) > 3 else ""
                mount_key = f"/mnt/{mount_name}"

                if mount_key in self._mounts:
                    oci_base = self._mounts[mount_key]
                    return f"{oci_base.rstrip('/')}/{remainder}" if remainder else oci_base

        # Handle s3:// paths - translate to oci:// if mapping exists
        if path.startswith("s3://") or path.startswith("s3a://"):
            s3_bucket = path.split("/")[2]
            s3_key = "/".join(path.split("/")[3:])
            s3_mount_key = f"s3://{s3_bucket}"

            if s3_mount_key in self._mounts:
                oci_base = self._mounts[s3_mount_key]
                return f"{oci_base.rstrip('/')}/{s3_key}" if s3_key else oci_base

        # Already an OCI path
        if path.startswith("oci://"):
            return path

        return path

    # â”€â”€ OCI helpers (used by all fs operations on oci:// paths) â”€â”€
    #
    # NOTE: The previous implementation of every fs operation used
    # jvm.org.apache.hadoop.fs.FileSystem (BmcFilesystem) which works during
    # interactive notebook execution but FAILS in source workload's scheduled workflow
    # runs (BmcFilesystem JVM class is not initialized the same way in workflow
    # contexts). The current implementation routes oci:// operations through the
    # OCI Python SDK with API key auth â€” works in both interactive and
    # workflow execution. Local-fs operations stay native Python.

    @staticmethod
    def _oci_object_size(client, namespace: str, bucket: str, key: str) -> Optional[int]:
        """Return object size in bytes, or None if not found."""
        try:
            meta = client.head_object(namespace, bucket, key)
            return int(meta.headers.get("content-length", "0"))
        except Exception:
            return None

    def ls(self, path: str) -> List[FileInfo]:
        """List contents of a directory."""
        translated = self._translate_path(path)
        parsed = _parse_oci_uri(translated)

        if parsed is not None:
            bucket, namespace, key = parsed
            client = _get_oci_storage_client()
            # Normalize prefix: object-storage uses '/' as delimiter for
            # emulated directories. A prefix that doesn't end with '/' lists
            # children of the matching object only.
            prefix = key
            if prefix and not prefix.endswith("/"):
                # If the prefix is a file (an object that exists), return a
                # single-entry FileInfo. Otherwise treat as a directory prefix.
                size = self._oci_object_size(client, namespace, bucket, prefix)
                if size is not None:
                    return [FileInfo(
                        path=translated, name=prefix.rsplit("/", 1)[-1],
                        size=size, modificationTime=0, isDir=False, isFile=True,
                    )]
                prefix = prefix + "/"
            results: List[FileInfo] = []
            seen_prefixes: set = set()
            next_token = None
            while True:
                resp = client.list_objects(
                    namespace_name=namespace,
                    bucket_name=bucket,
                    prefix=prefix or None,
                    delimiter="/",
                    fields="name,size,timeModified",
                    start=next_token,
                )
                data = resp.data
                for obj in (data.objects or []):
                    name = obj.name[len(prefix):] if prefix and obj.name.startswith(prefix) else obj.name
                    if not name:
                        continue
                    mt = 0
                    try:
                        if obj.time_modified:
                            mt = int(obj.time_modified.timestamp() * 1000)
                    except Exception:
                        mt = 0
                    results.append(FileInfo(
                        path=f"oci://{bucket}@{namespace}/{obj.name}",
                        name=name, size=int(obj.size or 0),
                        modificationTime=mt, isDir=False, isFile=True,
                    ))
                for pfx in (data.prefixes or []):
                    if pfx in seen_prefixes:
                        continue
                    seen_prefixes.add(pfx)
                    name = pfx[len(prefix):].rstrip("/") if prefix and pfx.startswith(prefix) else pfx.rstrip("/")
                    results.append(FileInfo(
                        path=f"oci://{bucket}@{namespace}/{pfx}",
                        name=name, size=0,
                        modificationTime=0, isDir=True, isFile=False,
                    ))
                next_token = getattr(data, "next_start_with", None)
                if not next_token:
                    break
            if not results and prefix:
                raise FileNotFoundError(f"Path not found: {translated}")
            return results

        # Local filesystem
        if os.path.isdir(translated):
            results = []
            for name in os.listdir(translated):
                full = os.path.join(translated, name)
                stat = os.stat(full)
                results.append(FileInfo(
                    path=full, name=name, size=stat.st_size,
                    modificationTime=int(stat.st_mtime * 1000),
                    isDir=os.path.isdir(full), isFile=os.path.isfile(full),
                ))
            return results
        if os.path.isfile(translated):
            stat = os.stat(translated)
            return [FileInfo(
                path=translated, name=os.path.basename(translated),
                size=stat.st_size, modificationTime=int(stat.st_mtime * 1000),
                isDir=False, isFile=True,
            )]
        raise FileNotFoundError(f"Path not found: {translated}")

    def cp(self, src: str, dst: str, recurse: bool = False) -> bool:
        """Copy a file or directory."""
        src_t = self._translate_path(src)
        dst_t = self._translate_path(dst)
        src_oci = _parse_oci_uri(src_t)
        dst_oci = _parse_oci_uri(dst_t)

        if src_oci is None and dst_oci is None:
            # local â†’ local
            if recurse and os.path.isdir(src_t):
                shutil.copytree(src_t, dst_t)
            else:
                if os.path.dirname(dst_t):
                    os.makedirs(os.path.dirname(dst_t), exist_ok=True)
                shutil.copy2(src_t, dst_t)
            return True

        client = _get_oci_storage_client()

        if src_oci is not None and dst_oci is None:
            # oci â†’ local
            src_bucket, src_ns, src_key = src_oci
            if recurse and (not src_key or src_key.endswith("/")):
                # Recursively download all objects under prefix
                prefix = src_key
                next_token = None
                while True:
                    resp = client.list_objects(
                        namespace_name=src_ns, bucket_name=src_bucket,
                        prefix=prefix or None, start=next_token,
                    )
                    for obj in (resp.data.objects or []):
                        rel = obj.name[len(prefix):] if prefix and obj.name.startswith(prefix) else obj.name
                        local_target = os.path.join(dst_t, rel)
                        os.makedirs(os.path.dirname(local_target) or ".", exist_ok=True)
                        body = client.get_object(src_ns, src_bucket, obj.name).data
                        with open(local_target, "wb") as f:
                            for chunk in body.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                    next_token = getattr(resp.data, "next_start_with", None)
                    if not next_token:
                        break
            else:
                if os.path.dirname(dst_t):
                    os.makedirs(os.path.dirname(dst_t), exist_ok=True)
                body = client.get_object(src_ns, src_bucket, src_key).data
                with open(dst_t, "wb") as f:
                    for chunk in body.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
            return True

        if src_oci is None and dst_oci is not None:
            # local â†’ oci
            dst_bucket, dst_ns, dst_key = dst_oci
            if recurse and os.path.isdir(src_t):
                for root, _, files in os.walk(src_t):
                    for fname in files:
                        full = os.path.join(root, fname)
                        rel = os.path.relpath(full, src_t).replace(os.sep, "/")
                        target_key = (dst_key.rstrip("/") + "/" + rel) if dst_key else rel
                        with open(full, "rb") as f:
                            client.put_object(dst_ns, dst_bucket, target_key, f)
            else:
                with open(src_t, "rb") as f:
                    client.put_object(dst_ns, dst_bucket, dst_key, f)
            return True

        # oci â†’ oci  (same-region copy via OCI Object Storage copy_object)
        # destination_region is REQUIRED by the API (even for same-region copies).
        # copy_object is ASYNCHRONOUS â€” returns 202 with opc-work-request-id;
        # the destination object isn't visible until the work request completes.
        # We poll the work request until COMPLETED (or fail loudly).
        import oci as _oci
        src_bucket, src_ns, src_key = src_oci
        dst_bucket, dst_ns, dst_key = dst_oci
        copy_details = _oci.object_storage.models.CopyObjectDetails(
            source_object_name=src_key,
            destination_region=_get_oci_storage_region(),
            destination_namespace=dst_ns,
            destination_bucket=dst_bucket,
            destination_object_name=dst_key,
        )
        resp = client.copy_object(
            namespace_name=src_ns, bucket_name=src_bucket,
            copy_object_details=copy_details,
        )
        # Wait for the async work request to complete. Header key
        # 'opc-work-request-id' is returned by every async object-storage call.
        wr_id = resp.headers.get("opc-work-request-id")
        if wr_id:
            try:
                _oci.wait_until(
                    client,
                    client.get_work_request(wr_id),
                    "status",
                    "COMPLETED",
                    max_wait_seconds=300,
                    succeed_on_not_found=False,
                )
            except Exception as e:
                # Surface a clear error so callers see what failed.
                raise IOError(
                    f"copy_object work-request {wr_id} did not COMPLETE: {e}"
                )
        return True

    def mv(self, src: str, dst: str, recurse: bool = False) -> bool:
        """Move a file or directory (copy + delete source)."""
        src_t = self._translate_path(src)
        dst_t = self._translate_path(dst)
        src_oci = _parse_oci_uri(src_t)

        if src_oci is None and _parse_oci_uri(dst_t) is None:
            shutil.move(src_t, dst_t)
            return True
        # Mixed or pure-oci â†’ copy + delete src
        self.cp(src_t, dst_t, recurse=recurse)
        self.rm(src_t, recurse=recurse)
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        """Remove a file or directory."""
        translated = self._translate_path(path)
        parsed = _parse_oci_uri(translated)

        if parsed is not None:
            bucket, namespace, key = parsed
            client = _get_oci_storage_client()
            if recurse and (not key or key.endswith("/")):
                prefix = key
                next_token = None
                while True:
                    resp = client.list_objects(
                        namespace_name=namespace, bucket_name=bucket,
                        prefix=prefix or None, start=next_token,
                    )
                    for obj in (resp.data.objects or []):
                        try:
                            client.delete_object(namespace, bucket, obj.name)
                        except Exception:
                            pass
                    next_token = getattr(resp.data, "next_start_with", None)
                    if not next_token:
                        break
            else:
                try:
                    client.delete_object(namespace, bucket, key)
                except Exception as e:
                    # Mimic the local rm semantic: error â†’ raise IOError
                    raise IOError(f"Remove failed: {e}")
            return True

        # Local
        if recurse and os.path.isdir(translated):
            shutil.rmtree(translated)
        elif os.path.exists(translated):
            os.remove(translated)
        return True

    def mkdirs(self, path: str) -> bool:
        """Create directory and parents â€” idempotent like Databricks' dbutils.fs.mkdirs.

        OCI Object Storage has no real directory concept (it's flat). Writing
        a zero-byte placeholder object ending with '/' is the conventional way
        to make 'directory' visible in object listings. Local paths use os.makedirs.

        Idempotency: returns True if the target already exists. Python's
        `os.makedirs(exist_ok=True)` swallows the standard `FileExistsError`,
        but AIDP's /Volumes FUSE mount can raise `VolumeFileAlreadyExistsException`
        (non-standard) when the directory exists. We pre-check + tolerate any
        "already exists" style error so the call behaves like dbutils.fs.mkdirs.
        """
        translated = self._translate_path(path)
        parsed = _parse_oci_uri(translated)

        if parsed is not None:
            bucket, namespace, key = parsed
            client = _get_oci_storage_client()
            if not key:
                # Bucket-level "mkdir" â€” bucket already exists (assumed). No-op.
                return True
            marker = key if key.endswith("/") else key + "/"
            try:
                client.put_object(namespace, bucket, marker, b"")
            except Exception as _e:
                if not _is_already_exists_error(_e):
                    raise
            return True

        # Local / FUSE-mounted path. Fast-path: already a directory â†’ no-op.
        if os.path.isdir(translated):
            return True
        try:
            os.makedirs(translated, exist_ok=True)
        except Exception as _e:
            # FUSE-specific "already exists" errors aren't FileExistsError,
            # so exist_ok=True doesn't swallow them. Tolerate them here.
            if not _is_already_exists_error(_e):
                raise
        return True

    def head(self, path: str, max_bytes: int = 65536) -> str:
        """Read the first max_bytes of a file as a string."""
        translated = self._translate_path(path)
        parsed = _parse_oci_uri(translated)

        if parsed is not None:
            bucket, namespace, key = parsed
            client = _get_oci_storage_client()
            # Use Range header to read just the prefix
            resp = client.get_object(
                namespace_name=namespace, bucket_name=bucket, object_name=key,
                range=f"bytes=0-{max(0, max_bytes - 1)}",
            )
            data = b"".join(resp.data.iter_content(chunk_size=1024 * 1024))
            return data.decode("utf-8", errors="replace")

        with open(translated, "r") as f:
            return f.read(max_bytes)

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        """Write string contents to a file."""
        translated = self._translate_path(path)
        parsed = _parse_oci_uri(translated)

        if parsed is not None:
            bucket, namespace, key = parsed
            client = _get_oci_storage_client()
            if not overwrite:
                # Check existence
                if self._oci_object_size(client, namespace, bucket, key) is not None:
                    raise FileExistsError(f"File already exists: {translated}")
            client.put_object(namespace, bucket, key, contents.encode("utf-8"))
            return True

        mode = "w" if overwrite else "x"
        if os.path.dirname(translated):
            os.makedirs(os.path.dirname(translated), exist_ok=True)
        with open(translated, mode) as f:
            f.write(contents)
        return True

    def mount(self, source: str, mount_point: str, encryption_type: str = "",
              owner: str = None, extra_configs: dict = None) -> bool:
        """Register a mount point mapping.

        On AIDP, this adds a path mapping rather than a real FUSE mount.
        The mapping is stored in memory and optionally persisted to config.
        """
        self._mounts[mount_point] = source
        print(f"[AIDP] Mount registered: {mount_point} -> {source}")
        print(f"[AIDP] Note: This is a path mapping, not a FUSE mount.")

        if extra_configs:
            # Store extra configs for credential reference
            for key, val in extra_configs.items():
                env_key = f"AIDP_MOUNT_{mount_point.replace('/', '_')}_{key}".upper()
                os.environ[env_key] = str(val)

        return True

    def unmount(self, mount_point: str) -> bool:
        """Remove a mount point mapping."""
        if mount_point in self._mounts:
            del self._mounts[mount_point]
            print(f"[AIDP] Mount removed: {mount_point}")
            return True
        return False

    def mounts(self) -> List[MountInfo]:
        """List all current mount point mappings."""
        return [
            MountInfo(mountPoint=mp, source=src)
            for mp, src in self._mounts.items()
        ]

    def refreshMounts(self) -> bool:
        """Reload mount config from file."""
        self._load_mount_config()
        return True

    def updateMount(self, source: str, mount_point: str, encryption_type: str = "",
                    owner: str = None, extra_configs: dict = None) -> bool:
        """Update an existing mount point."""
        return self.mount(source, mount_point, encryption_type, owner, extra_configs)

    def help(self, method: str = None):
        """Print help for fs utilities."""
        methods = {
            "cp": "cp(src, dst, recurse=False) - Copy files",
            "head": "head(file, maxBytes=65536) - Read first bytes of a file",
            "ls": "ls(dir) - List directory contents",
            "mkdirs": "mkdirs(dir) - Create directory with parents",
            "mount": "mount(source, mountPoint, ...) - Register mount mapping",
            "mounts": "mounts() - List mount mappings",
            "mv": "mv(src, dst, recurse=False) - Move files",
            "put": "put(file, contents, overwrite=False) - Write string to file",
            "refreshMounts": "refreshMounts() - Reload mount config",
            "rm": "rm(dir, recurse=False) - Remove file/directory",
            "unmount": "unmount(mountPoint) - Remove mount mapping",
            "updateMount": "updateMount(source, mountPoint, ...) - Update mount",
        }
        if method and method in methods:
            print(methods[method])
        else:
            print("dbutils.fs - AIDP File System Utils")
            print("=" * 40)
            for name, desc in sorted(methods.items()):
                print(f"  {desc}")
