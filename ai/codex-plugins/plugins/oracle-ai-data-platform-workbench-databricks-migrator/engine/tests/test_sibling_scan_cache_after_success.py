"""#134 review — sibling-scan caches only AFTER a successful listing.

A *transient* listing failure (e.g. a proxy 429 that exhausts
`proxy_max_retries` and surfaces as `DbxExportError` from dbx_client) must NOT
mark the parent dir as "scanned" — otherwise a later re-scan short-circuits to
`[]` and never retries, permanently dropping a sibling the retry could have
recovered. A *successful* listing IS cached (no redundant re-list). Behaviour
on a genuine failure is otherwise unchanged: return `[]`
(retry-then-fall-back-to-today).
"""
from dbx_export.dbx_client import DbxExportError
from dbx_export.dependency_resolver import DependencyResolver, Graph, NotebookNode


class _FailOnceDbx:
    """`list_github_dir` raises once (transient), then succeeds."""

    def __init__(self, entries):
        self.calls = 0
        self._entries = entries

    def list_github_dir(self, owner, repo, parent, branch):
        self.calls += 1
        if self.calls == 1:
            raise DbxExportError("github_list_failed", "HTTP 429 after 20 attempt(s)", 429)
        return dict(self._entries)


class _AlwaysOkDbx:
    def __init__(self, entries):
        self.calls = 0
        self._entries = entries

    def list_github_dir(self, owner, repo, parent, branch):
        self.calls += 1
        return dict(self._entries)


def _node():
    return NotebookNode(
        dbx_path="folder/first", role="run_dep",
        git_owner="o", git_repo="r", git_branch="b",
    )


def test_transient_listing_failure_is_not_cached_and_rescan_recovers():
    dbx = _FailOnceDbx({"constants": "constants.py"})
    resolver = DependencyResolver(dbx, log=lambda _m: None)
    g = Graph()
    node = _node()

    # First scan: listing fails -> [] and parent is NOT marked scanned.
    first = resolver._discover_sibling_py_modules(g, node, node.dbx_path)
    assert first == []
    assert ("folder", "o", "r", "b") not in resolver._sibling_parent_scanned

    # Re-scan: the retry succeeds, so the sibling is recovered (not permanently
    # dropped by a cached transient failure).
    second = resolver._discover_sibling_py_modules(g, node, node.dbx_path)
    assert "folder/constants" in second
    assert "folder/constants" in g.notebooks
    assert dbx.calls == 2, "re-scan must actually re-list after a transient failure"


def test_successful_listing_is_cached_and_not_relisted():
    dbx = _AlwaysOkDbx({"constants": "constants.py"})
    resolver = DependencyResolver(dbx, log=lambda _m: None)
    g = Graph()
    node = _node()

    first = resolver._discover_sibling_py_modules(g, node, node.dbx_path)
    assert "folder/constants" in first
    assert ("folder", "o", "r", "b") in resolver._sibling_parent_scanned

    # Second call: cache hit -> [] without re-listing.
    second = resolver._discover_sibling_py_modules(g, node, node.dbx_path)
    assert second == []
    assert dbx.calls == 1, "a successfully-scanned parent must not be re-listed"
