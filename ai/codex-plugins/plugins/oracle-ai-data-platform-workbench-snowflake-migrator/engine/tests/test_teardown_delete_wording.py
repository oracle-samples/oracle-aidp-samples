"""TEARDOWN.md says a cluster is gone only when teardown saw it go.

Found in review. render_teardown printed "`delete` is final: the
registered copy jobs now point at a cluster that no longer exists" for
every delete run: the default dry run (which opens with "DRY RUN --
nothing was changed"), a delete the API refused with 403, and one still
DELETING when the poll ran out. And one try block covered both the delete
call and its read-back, so a 429 on the listing after an ACCEPTED delete
was recorded as `failed` -- indistinguishable from a refusal, with no
record that the delete had been sent.

A read-back that fails after the action was accepted is now
`delete_requested` (or `stop_requested`), unverified, with the error; the
final-delete warning is printed only over clusters verified deleted, and
phrased as what --execute would do in a dry run.
"""
import pytest

from target.teardown import render_teardown, teardown
from test_teardown import PROV, Fake


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)


class BlindAfterAction(Fake):
    """The action is accepted; every listing after it answers 429."""

    def __init__(self):
        super().__init__()
        self.sent = False

    def __call__(self, op, **kw):
        if op == "list_clusters" and self.sent:
            self.calls.append((op, None))
            raise RuntimeError("429 TooManyRequests list_clusters")
        if op in ("delete_cluster", "stop_cluster"):
            self.sent = True
        return super().__call__(op, **kw)


def test_a_failed_read_back_after_an_accepted_delete_is_requested():
    res = teardown(BlindAfterAction(), PROV, action="delete", execute=True,
                   delays=(0,))
    step = res["steps"][0]
    assert step["action"] == "delete_requested"
    assert step["verified"] is False
    assert "429" in step["detail"] and "sent" in step["detail"].lower()


def test_a_refused_delete_is_still_failed():
    res = teardown(Fake(refuse=("delete_cluster", "403 NotAuthorized")),
                   PROV, action="delete", execute=True, delays=(0,))
    assert {s["action"] for s in res["steps"] if s["cluster"]} == {"failed"}
    assert "no longer exists" not in render_teardown(res)


def test_a_dry_run_delete_does_not_say_the_cluster_is_gone():
    md = render_teardown(teardown(None, PROV, action="delete",
                                  execute=False))
    assert "no longer exists" not in md
    assert "--execute" in md and "final" in md


def test_an_unverified_delete_does_not_say_the_cluster_is_gone():
    res = teardown(Fake(lag=99), PROV, action="delete", execute=True,
                   delays=(0,))
    assert all(not s["verified"] for s in res["steps"])
    assert "no longer exists" not in render_teardown(res)


def test_a_verified_delete_warns_about_the_jobs():
    res = teardown(Fake(), PROV, action="delete", execute=True, delays=(0,))
    md = render_teardown(res)
    assert "no longer exists" in md and "`migration_assets`" in md
