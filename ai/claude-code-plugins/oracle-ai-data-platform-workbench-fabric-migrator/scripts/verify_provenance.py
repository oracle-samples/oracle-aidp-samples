#!/usr/bin/env python3
"""Pin every vendored fixture's provenance to a commit that proves it.

A `/blob/HEAD/` URL is not provenance. HEAD moves, files are renamed and
deleted, and a year from now the link points at whatever that path holds --
or at nothing. What we need to be able to say is the checkable form:

    this file is byte-identical to <repo>@<sha>:<path>

So this fetches each vendored file from its repository at that repository's
current HEAD, compares the bytes, and on a match rewrites the entry to a
`/blob/<sha>/` URL with the sha and the date recorded.

A file whose bytes differ came from an older commit. It is left unpinned and
listed rather than given a sha that does not hold: a provenance record that
is wrong is worse than one that is merely vague, because it invites belief.

    python3 scripts/verify_provenance.py           # report, change nothing
    python3 scripts/verify_provenance.py --write   # rewrite the manifests

Needs the `gh` CLI, authenticated. Read-only against GitHub.
"""
from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REAL = ROOT / "tests" / "fixtures" / "real"
CORPORA = REAL / "corpora"


def _gh(*args):
    done = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        return None
    return done.stdout.strip()


def head_sha(repo: str, cache: dict):
    """The repository's current default-branch commit."""
    if repo not in cache:
        cache[repo] = _gh("api", f"repos/{repo}/commits/HEAD", "--jq", ".sha")
    return cache[repo]


def upstream_bytes(repo: str, path: str, sha: str):
    """The file's contents at `sha`, or None if it is not there."""
    encoded = _gh("api", f"repos/{repo}/contents/{path}?ref={sha}", "--jq", ".content")
    if not encoded:
        return None
    try:
        return base64.b64decode(encoded)
    except (ValueError, TypeError):
        return None


def check(entry: dict, local: Path, cache: dict) -> dict:
    """Return the entry with `sha`/`verified` set, plus a status."""
    repo, path = entry.get("repo", ""), entry.get("path", "")
    result = dict(entry)
    if not local.is_file():
        return dict(result, status="missing-locally")
    if not repo or not path:
        return dict(result, status="no-upstream-path")

    sha = head_sha(repo, cache)
    if not sha:
        return dict(result, status="repo-unreachable")

    remote = upstream_bytes(repo, path, sha)
    if remote is None:
        return dict(result, status="gone-from-head")
    if remote != local.read_bytes():
        return dict(result, status="differs-from-head")

    result["sha"] = sha
    result["url"] = f"https://github.com/{repo}/blob/{sha}/{path}"
    result["status"] = "pinned"
    return result


def manifests():
    """(manifest path, directory holding the files it describes)."""
    for path in sorted(CORPORA.glob("*/PROVENANCE.json")):
        yield path, path.parent
    legacy = REAL / "PROVENANCE.json"
    if legacy.is_file():
        yield legacy, REAL


def main() -> int:
    write = "--write" in sys.argv[1:]
    cache: dict = {}
    counts: dict = {}
    unpinned = []

    for manifest, directory in manifests():
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        checked = []
        for entry in payload.get("files", []):
            result = check(entry, directory / entry["file"], cache)
            status = result.pop("status")
            counts[status] = counts.get(status, 0) + 1
            if status != "pinned":
                unpinned.append((manifest.parent.name, entry["file"],
                                 entry.get("repo", "?"), status))
            checked.append(result)
        payload["files"] = checked
        payload["verified_against"] = "each repository's HEAD at the time of the run"
        if write:
            manifest.write_text(json.dumps(payload, indent=1) + "\n", encoding="utf-8")

    total = sum(counts.values())
    print(f"{total} vendored file(s)")
    for status, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {n:4}  {status}")
    if unpinned:
        print("\nnot pinned -- these came from a commit that is no longer HEAD,")
        print("and are recorded as such rather than given a sha that does not hold:")
        for corpus, name, repo, status in unpinned:
            print(f"  {corpus}/{name:12} {repo:48} {status}")
    print("\n(dry run; pass --write to rewrite the manifests)" if not write
          else "\nmanifests rewritten")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
