"""Publication guard: the WHOLE plugin tree must stay free of the source
deployment's identifiers.

Unlike test_no_hardcoded_customer_values.py (a config-hygiene ratchet over the
runtime code only), this test scans EVERY tracked text file — tests, fixtures,
config, schemas, docs, skills — for two things:

1. Banned literal tokens, loaded from an EXTERNAL file via
   AIDP_PUBLICATION_BANLIST (the list never ships with the repo in any
   reversible form); when unset, this check is skipped.
2. Identifier SHAPES that should only ever appear as synthetic values:
   full UUIDs outside the fake prefixes, OCI namespace-shaped tokens other
   than the exmplnspace* placeholders, and real-shaped oraclevcn.com hosts.

A hit is a publication blocker, not a style issue.
"""
from __future__ import annotations

import re
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# The banned-token list deliberately does NOT live in this repository in ANY
# form (base64 or hashes of short tokens are reversible — shipping them would
# publish exactly what they ban). The private release process points
# AIDP_PUBLICATION_BANLIST at a newline-separated token file; without it this
# test runs the SHAPE checks only.
import os

def _load_banned():
    path = os.environ.get("AIDP_PUBLICATION_BANLIST")
    if not path:
        return []
    if not Path(path).is_file():
        # An explicitly configured list that cannot be read must FAIL the scan,
        # not silently skip it — a release run would otherwise pass unchecked.
        raise AssertionError(
            f"AIDP_PUBLICATION_BANLIST is set but unreadable: {path!r}")
    return [ln.strip().lower() for ln in Path(path).read_text().splitlines()
            if ln.strip() and not ln.startswith("#")]

BANNED = _load_banned()

FAKE_UUID_PREFIXES = ("00000000", "aaaaaaaa", "11111111", "22222222", "33333333",
                      "44444444", "99999999", "12345678", "deadbeef", "ffffffff",
                      "eeeeeeee", "abcdefab")
UUID_RE = re.compile(r"(?<![0-9a-fA-F])([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-fA-F])")
NAMESPACE_RE = re.compile(r"(?<![a-z0-9])bm[a-z0-9]{10}(?![a-z0-9])")
VCN_RE = re.compile(r"[a-z0-9-]+\.compute\.(?!examplevcn\b|<)[a-z0-9]+\.oraclevcn\.com")

SKIP_PARTS = {".git", "__pycache__"}
SELF = Path(__file__).resolve()


def _tracked_text_files():
    for p in PLUGIN_ROOT.rglob("*"):
        if not p.is_file() or p.resolve() == SELF:
            continue
        if any(part in SKIP_PARTS for part in p.parts):
            continue
        try:
            yield p, p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue


def test_whole_tree_is_publication_clean():
    hits = []
    for p, text in _tracked_text_files():
        low = text.lower()
        rel = p.relative_to(PLUGIN_ROOT).as_posix()
        for tok in BANNED:
            if tok in low:
                i = low.index(tok)
                hits.append(f"{rel}: banned token ...{text[max(0, i - 20):i + len(tok) + 20]!r}")
        for m in UUID_RE.finditer(text):
            if not m.group(1).startswith(FAKE_UUID_PREFIXES):
                hits.append(f"{rel}: non-synthetic UUID {m.group(0)}")
        # shape checks run on the ORIGINAL case: real namespaces are
        # all-lowercase, while SDK classes (BmcException, BmcDataStore) are
        # CamelCase and must not false-positive.
        for m in NAMESPACE_RE.finditer(text):
            if not m.group(0).startswith("bmexample"):
                hits.append(f"{rel}: namespace-shaped token {m.group(0)}")
        for m in VCN_RE.finditer(text):
            hits.append(f"{rel}: real-shaped VCN host {m.group(0)}")
    assert not hits, "publication blockers found:\n  " + "\n  ".join(hits[:40])
