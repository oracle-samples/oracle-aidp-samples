"""`teardown --scope credential | all` reads every workspace-object delete
back in the folder listing, and the read-back must be able to SEE the
object, whatever form the listing names it in (#175 hardening).

The AIDP SDK documents a listed object's `path` as fully qualified
(`/Shared/Folder1/Notebook1.ipynb`, leading slash) and its `displayName` as
the leaf, while this plugin addresses objects workspace-relative (a leading
slash is a live 400). An exact match of the relative path against those
never succeeds, so "not listed" -- gone -- was the answer to every delete,
and a delete accepted but never applied was reported verified. Matched the
way every upload read-back in this plugin matches (path, path leaf, or
display name), it is reported `delete_requested`, never verified.
"""
import pytest

from test_teardown_scopes import CRED, LEDGER, Lake, _prov, _run

NOTEBOOK = "backup-snowflake-migration/scripts/02_copy_schema.ipynb"
SHAPES = ["relative", "absolute", "leaf"]


class Shaped(Lake):
    def __init__(self, shape, **kw):
        super().__init__(**kw)
        self.shape = shape
        self.objects.add(NOTEBOOK)

    def __call__(self, op, **kw):
        if op != "list_ws_objects":
            return super().__call__(op, **kw)
        self.ops.append((op, kw))
        items = []
        for p in sorted(self.objects):
            if not p.startswith(kw["path"]):
                continue
            leaf = p.rsplit("/", 1)[-1]
            items.append({"relative": {"path": p, "displayName": leaf},
                          "absolute": {"path": "/" + p, "displayName": leaf},
                          "leaf": {"displayName": leaf}}[self.shape])
        return {"items": items}


def _cred_step(res):
    return next(s for s in res["steps"] if s.get("kind") == "credential")


@pytest.mark.parametrize("shape", SHAPES)
def test_a_credential_delete_never_applied_is_not_verified(shape):
    lake = Shaped(shape, sticky={CRED})
    step = _cred_step(_run(lake, scope="credential"))
    assert CRED in lake.objects, "the double kept it, as a live no-op would"
    assert step["action"] == "delete_requested" and step["verified"] is False


@pytest.mark.parametrize("shape", SHAPES)
def test_a_credential_delete_that_lands_is_verified(shape):
    lake = Shaped(shape)
    step = _cred_step(_run(lake, scope="credential"))
    assert CRED not in lake.objects
    assert step["action"] == "deleted" and step["verified"] is True


@pytest.mark.parametrize("shape", SHAPES)
def test_a_notebook_delete_never_applied_is_not_verified(shape):
    # --scope all on a workspace it did not create removes the migration's
    # notebooks one by one (the ledger's ws_object rows): the same read-back.
    lake = Shaped(shape, sticky={NOTEBOOK})
    ledger = LEDGER + [{"kind": "ws_object", "name": NOTEBOOK,
                        "workspace": "ws-1", "action": "created"}]
    prov = _prov(workspace={"name": "lab_ws", "key": "ws-1",
                            "created": False})
    from target.teardown import teardown_everything
    res = teardown_everything(lake, prov, scope="all", execute=True,
                              ledger=ledger,
                              datalake_ocid=prov["datalake_ocid"], delays=(),
                              async_delays=(0,), sleep=lambda _s: None)
    step = next(s for s in res["steps"] if s.get("kind") == "notebook")
    assert step["verified"] is False and step["action"] == "delete_requested"
