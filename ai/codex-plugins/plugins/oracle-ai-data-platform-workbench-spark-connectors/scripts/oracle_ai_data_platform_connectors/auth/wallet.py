"""Oracle wallet handling for AIDP notebooks.

Wallets MUST live under /tmp — never /Workspace. The /Workspace mount is
FUSE-backed and disconnects intermittently (Errno 107), and os.chmod is a no-op
on it, so files cannot be made readable to the Java JDBC driver process.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import Union


def write_wallet_to_tmp(
    wallet: Union[bytes, str, Path],
    target_dir: Union[str, Path] = "/tmp/wallet",
    set_tns_admin: bool = True,
) -> str:
    """Materialize an Oracle wallet under /tmp and (optionally) export TNS_ADMIN.

    Args:
        wallet: One of:
            - bytes: a wallet ZIP's raw content (e.g., from OCI Vault).
            - str/Path: a filesystem path to either a wallet ZIP or an already-
              extracted wallet directory.
        target_dir: Where to write. Must be under /tmp (a non-FUSE filesystem)
            so the JDBC driver process can read the files. Defaults to
            ``/tmp/wallet``.
        set_tns_admin: If True, set ``os.environ["TNS_ADMIN"]`` to the target
            directory. The Oracle JDBC driver picks this up automatically.

    Returns:
        Absolute path of the directory containing the wallet files (the value
        you'd pass as ``TNS_ADMIN`` to the JDBC URL).

    Raises:
        ValueError: If ``target_dir`` is not under ``/tmp``.
        FileNotFoundError: If ``wallet`` is a path that does not exist.
    """
    # Validate prefix on the literal string, not the resolved path. On Windows
    # the AIDP-runtime convention "/tmp/..." resolves to "C:/tmp/..." which
    # would falsely fail the prefix check. The intent is: in the AIDP Linux
    # runtime, callers must pass a /tmp/... string.
    if not str(target_dir).replace("\\", "/").startswith("/tmp"):
        raise ValueError(
            "wallet target_dir must be under /tmp; "
            "/Workspace is FUSE-mounted and breaks JDBC reads"
        )

    # Validate the wallet source EXISTS before creating the target directory
    # — otherwise we leave an empty /tmp/... dir behind on every bad call.
    if isinstance(wallet, (str, Path)):
        src = Path(wallet)
        if not src.exists():
            raise FileNotFoundError(f"wallet path does not exist: {src}")

    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    if isinstance(wallet, (bytes, bytearray, memoryview)):
        # Spark's binaryFile reader hands back bytearray, not bytes. Accept either.
        _extract_zip_bytes_to(bytes(wallet), target)
    else:
        src = Path(wallet)
        if src.is_file() and src.suffix.lower() == ".zip":
            with src.open("rb") as fh:
                _extract_zip_bytes_to(fh.read(), target)
        elif src.is_dir():
            _copy_dir_to(src, target)
        else:
            raise ValueError(
                f"wallet path must be a .zip or a directory, got {src}"
            )

    if set_tns_admin:
        os.environ["TNS_ADMIN"] = str(target)

    return str(target)


def _extract_zip_bytes_to(zip_bytes: bytes, target: Path) -> None:
    """Extract a wallet ZIP to ``target`` with world-readable permissions."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for member in zf.namelist():
            if member.endswith("/"):
                continue
            data = zf.read(member)
            out_path = target / Path(member).name
            _write_world_readable(out_path, data)


def _copy_dir_to(src: Path, target: Path) -> None:
    for child in src.iterdir():
        if child.is_file():
            _write_world_readable(target / child.name, child.read_bytes())


def _write_world_readable(path: Path, data: bytes) -> None:
    """Write a file with mode 0o666 set up-front via os.open.

    os.chmod does NOT work on FUSE mounts in the AIDP notebook environment, so
    permissions must be applied at file-creation time via the os.open mode
    bits. The JDBC driver process runs as a different UID than the Python
    process; without world-readable bits, it can't read the wallet.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(str(path), flags, 0o666)
    try:
        os.write(fd, data)
    finally:
        os.close(fd)
