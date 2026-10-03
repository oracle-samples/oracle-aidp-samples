"""IICS export package (.zip) ingestion.

Relocated from the deleted api/_common.py. An IICS export is distributed as a
ZIP; callers need a directory of mapping JSON files.
"""
from __future__ import annotations

import zipfile
from pathlib import Path


def extract_package(archive: Path, dest: Path) -> Path:
    """Extract an IICS export package and return the directory of assets.

    Raises ValueError if `archive` is not a readable ZIP.
    """
    if not zipfile.is_zipfile(archive):
        raise ValueError(f"Not a valid IICS export package (not a ZIP): {archive}")
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive) as zf:
        total = 0
        infos = zf.infolist()
        # MAX_INPUT_SIZE (security.py) guards each XML/JSON string after
        # extraction; nothing guarded the archive itself, so a small zip
        # could expand without limit before any per-file check ran.
        if len(infos) > MAX_PACKAGE_ENTRIES:
            raise ValueError(
                f"Refusing package with {len(infos)} entries (limit {MAX_PACKAGE_ENTRIES})"
            )
        for info in infos:
            member = info.filename
            # refuse path traversal
            if member.startswith(("/", "..")) or ".." in Path(member).parts:
                raise ValueError(f"Refusing unsafe path in package: {member}")
            total += info.file_size
            if total > MAX_PACKAGE_BYTES:
                raise ValueError(
                    f"Refusing package: uncompressed size exceeds {MAX_PACKAGE_BYTES} bytes"
                )
        zf.extractall(dest)
    return dest


# Uncompressed limits for an IICS export package. A real export is a few
# MB of JSON/XML; these are generous ceilings, not tuned thresholds.
MAX_PACKAGE_BYTES = 512 * 1024 * 1024
MAX_PACKAGE_ENTRIES = 20_000
