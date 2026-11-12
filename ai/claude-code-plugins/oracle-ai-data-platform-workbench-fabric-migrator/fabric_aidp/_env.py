"""Minimal .env loader. No dependency on python-dotenv."""
from __future__ import annotations

import errno
import os
import stat
import sys
from pathlib import Path

# What a `.env` may set. The file is read from whatever directory the CLI runs
# in -- an export someone sent you, a shared work root -- so it is treated as
# untrusted input, and only keys that choose among the user's OWN targets are
# taken from it: which workspace, cluster, instance, namespace and OCI profile.
#
# Not taken, even though they begin AIDP_/OCI_/FABRIC_: keys that change WHERE
# requests or credentials go. These values also reach the `aidp` CLI that
# `publish` runs, so a planted `AIDP_ENDPOINT` would send signed API calls to
# another host and a planted `OCI_CONFIG_FILE` would swap in someone else's
# key -- flagged, unfixed, in the oracle-samples security review (#146).
# OCI_CLI_AUTH and OCI_CLI_REGION are not taken either: a planted
# OCI_CLI_AUTH=instance_principal on an OCI compute host makes `publish
# --apply` act as the machine's identity rather than the user's API key, and
# a region is the same kind of switch (review of oracle-samples #152).
# They still work when exported in the shell, which is a choice the user made.
# Earlier the rule was any key with one of those prefixes, which kept out
# NODE_OPTIONS and PATH but let both of these through.
ALLOWED_KEYS = frozenset({
    "AIDP_WORKSPACE_KEY", "AIDP_CLUSTER_KEY", "AIDP_INSTANCE_ID",
    "OCI_NAMESPACE", "OCI_CLI_PROFILE", "OCI_CONFIG_PROFILE",
})

# Keys that look like this tool's, so ignoring one is worth a warning. A
# stray PATH= or NODE_OPTIONS= was always ignored silently and still is.
KEY_PREFIXES = ("AIDP_", "OCI_", "FABRIC_")


def _warn(message: str) -> None:
    print(f"warning: {message}", file=sys.stderr)


def _group_is_private(gid: int, uid: int) -> bool:
    """Whether group `gid` holds nobody but user `uid`.

    Debian, Ubuntu and Fedora give each user a group of their own and a umask
    of 002, so every file they create is group-writable -- by a group with
    one member. Group-write is accepted on these terms (OpenSSH itself does
    not: it refuses any `st_mode & 022` file): the members are the group's
    own list plus every account whose primary group it is, and the owner
    must be the only one.
    """
    try:
        import grp
        import pwd
        group = grp.getgrgid(gid)
        owner = pwd.getpwuid(uid).pw_name
        members = set(group.gr_mem)
        members.update(user.pw_name for user in pwd.getpwall()
                       if user.pw_gid == gid)
    except (ImportError, KeyError, OSError):
        return False
    return members == {owner}


def _unsafe(info: os.stat_result) -> str:
    """Why a `.env` with this stat must not be trusted, or "" when it may be
    read.

    POSIX only: a `.env` someone else owns, or one anyone else can write, is
    not the user's. On Windows `st_mode` carries no owner or ACL bits, so the
    check would say nothing true and is skipped.
    """
    if os.name == "nt" or not hasattr(os, "getuid"):
        return ""
    if info.st_uid != os.getuid():
        return "it is owned by another user"
    if info.st_mode & stat.S_IWOTH:
        return "anyone can write to it"
    if info.st_mode & stat.S_IWGRP and not _group_is_private(info.st_gid,
                                                              info.st_uid):
        return "other users in its group can write to it"
    return ""


def _read(candidate: Path):
    """The file's text, or None after warning why it was not read.

    Opened once, and the permission check judges that handle: checking the
    path and then reopening it let a file be swapped in between, in a
    directory others can write (review of oracle-samples #152). A symlink is
    not followed, and anything but a regular file -- a FIFO would block the
    read forever -- is not read.
    """
    flags = (os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
             | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0))
    try:
        fd = os.open(candidate, flags)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK) and candidate.is_symlink():
            _warn(f"ignored {candidate}: it is a symbolic link. Replace it "
                  f"with the file itself, or export the settings in your shell.")
        return None
    with os.fdopen(fd, "rb") as handle:
        try:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                return None
            problem = _unsafe(info)
            if problem:
                _warn(f"ignored {candidate}: {problem}. Fix its owner or "
                      f"permissions (chmod 600), or export the settings in "
                      f"your shell.")
                return None
            return handle.read().decode("utf-8-sig")
        except (OSError, UnicodeError):
            return None


def load_dotenv(path=".env") -> None:
    candidate = Path(path)
    if not candidate.is_file():
        return
    text = _read(candidate)
    if text is None:
        return
    refused = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        if key not in ALLOWED_KEYS:
            if key.startswith(KEY_PREFIXES):
                refused.append(key)
            continue
        if key not in os.environ:
            os.environ[key] = value
    if refused:
        # Key names only, never values: a value may be a secret.
        _warn(f"ignored {', '.join(sorted(set(refused)))} from {candidate}: a "
              f".env may only set {', '.join(sorted(ALLOWED_KEYS))}. Export any "
              f"other setting in your shell if you mean it.")
