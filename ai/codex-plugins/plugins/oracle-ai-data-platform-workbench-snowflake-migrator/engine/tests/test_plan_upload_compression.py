"""A plan too large to upload goes up gzipped, behind a pointer the stages follow.

Live 2026-09-29, the 50k-table scale plan: provision's uploads of
ddl_plan.json (274 MB) and inventory.json (293 MB) failed 502 Bad Gateway,
DDL_PLAN.md (132 MB) failed in the CLI, and a 64 MB probe took 612 s to
land -- the route gives up on a long upload. plan.json (48 MB) went up. The
failed overwrite then left plan/ddl_plan.json EMPTY on the workspace, and
the structure job died on it with a bare `JSONDecodeError: ... char 0`.

JSON plans compress 49-75x (ddl_plan.json 262 MiB -> 4.3 MiB). A plan file
over COMPRESS_OVER_BYTES is now uploaded as <name>.gz, and only after that
lands a small pointer goes up under the plain name, so a plain copy left by
an earlier push is never read as this plan. The stages read the plan
through read_plan_json, which follows the pointer, checks the digest, and
fails naming the file when what is there is not a plan.
"""
import gzip
import hashlib
import json
import pathlib

import pytest

from target import provisioning
from target.provisioning import BACKUP_FOLDER, PLAN_FOLDER, provision
from tests.test_provisioning import Fake
from tests.test_workflow_params_and_backups import NOW, PLAN

DATAPLANE = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _source():
    import importlib.util
    import sys
    sys.path.insert(0, str(DATAPLANE))
    spec = importlib.util.spec_from_file_location("snowmig_source_gz", DATAPLANE / "snowmig_source.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BytesFake(Fake):
    """Keeps the uploaded BYTES (a .gz is not text), in upload order."""

    def __init__(self, *, fail_paths=(), **kw):
        super().__init__(**kw)
        self.fail_paths, self.order = set(fail_paths), []

    def __call__(self, operation, **kw):
        if operation == "upload_ws_file":
            self.ops.append((operation, kw))
            self.order.append(kw["path"])
            if kw["path"] in self.fail_paths:
                raise RuntimeError("upload_ws_file failed (exit 502): 502 Bad Gateway")
            self.contents[kw["path"]] = {"type": "FILE",
                                         "bytes": pathlib.Path(kw["local_path"]).read_bytes()}
            return {}
        return super().__call__(operation, **kw)


def _plan_files(tmp_path, *, big=b""):
    plan = json.dumps({**PLAN, "padding": big.decode()}).encode("utf-8")
    (tmp_path / "ddl_plan.json").write_bytes(plan)
    (tmp_path / "plan.json").write_text("{}", encoding="utf-8")
    return [tmp_path / "plan.json", tmp_path / "ddl_plan.json"], plan


def _push(fake, files):
    return provision(call=fake, workspace_name="acme", scripts=[], plan_files=files,
                     execute=True, delays=(), plan_label="FULL", now=NOW)


def test_a_large_plan_goes_up_gzipped_behind_a_pointer(tmp_path, monkeypatch):
    monkeypatch.setattr(provisioning, "COMPRESS_OVER_BYTES", 1024)
    files, plan = _plan_files(tmp_path, big=b"x" * 5000)
    fake = BytesFake()
    res = _push(fake, files)
    gz, ptr = f"{PLAN_FOLDER}/ddl_plan.json.gz", f"{PLAN_FOLDER}/ddl_plan.json"
    assert gzip.decompress(fake.contents[gz]["bytes"]) == plan
    pointer = json.loads(fake.contents[ptr]["bytes"])
    assert pointer["snowmig_compressed_to"] == "ddl_plan.json.gz"
    assert pointer["sha256"] == hashlib.sha256(plan).hexdigest()
    assert fake.order.index(gz) < fake.order.index(ptr), "the pointer only once its target is up"
    # The small file is untouched; the backup is a .gz copy with no pointer.
    assert json.loads(fake.contents[f"{PLAN_FOLDER}/plan.json"]["bytes"]) == {}
    assert f"{BACKUP_FOLDER}/ddl_plan_20260929T031000Z_FULL.json.gz" in fake.contents
    uploads = [s for s in res["steps"] if s["step"] in ("upload", "backup")]
    assert all(s["verified"] for s in uploads), uploads
    assert any("gzip" in s["detail"] for s in uploads)


def test_a_failed_gzip_upload_puts_no_pointer_up(tmp_path, monkeypatch):
    monkeypatch.setattr(provisioning, "COMPRESS_OVER_BYTES", 1024)
    files, _plan = _plan_files(tmp_path, big=b"x" * 5000)
    fake = BytesFake(fail_paths={f"{PLAN_FOLDER}/ddl_plan.json.gz"})
    res = _push(fake, files)
    assert f"{PLAN_FOLDER}/ddl_plan.json" not in fake.order
    failed = [s for s in res["steps"] if s["step"] == "upload" and s["action"] == "failed"]
    assert failed and "ddl_plan.json.gz" in failed[0]["detail"]


def test_a_small_plan_is_uploaded_as_before(tmp_path):
    files, plan = _plan_files(tmp_path)
    fake = BytesFake()
    _push(fake, files)
    assert fake.contents[f"{PLAN_FOLDER}/ddl_plan.json"]["bytes"] == plan
    assert not any(p.endswith(".gz") for p in fake.contents)


def test_the_stage_reader_follows_the_pointer_and_checks_it(tmp_path):
    src = _source()
    plan = json.dumps({"statements": [{"sql": "CREATE TABLE t (a INT)"}]}).encode("utf-8")
    (tmp_path / "ddl_plan.json.gz").write_bytes(gzip.compress(plan))
    (tmp_path / "ddl_plan.json").write_text(json.dumps(
        {"snowmig_compressed_to": "ddl_plan.json.gz", "bytes": len(plan),
         "sha256": hashlib.sha256(plan).hexdigest()}), encoding="utf-8")
    assert src.read_plan_json(tmp_path / "ddl_plan.json") == json.loads(plan)
    (tmp_path / "ddl_plan.json.gz").write_bytes(gzip.compress(b'{"statements": []}'))
    with pytest.raises(ValueError, match="ddl_plan.json.gz"):
        src.read_plan_json(tmp_path / "ddl_plan.json")


def test_the_stage_reader_reads_a_plain_plan(tmp_path):
    src = _source()
    (tmp_path / "ddl_plan.json").write_text('{"statements": []}', encoding="utf-8")
    assert src.read_plan_json(tmp_path / "ddl_plan.json") == {"statements": []}


def test_an_empty_plan_file_fails_naming_it_and_the_remedy(tmp_path):
    src = _source()
    (tmp_path / "ddl_plan.json").write_bytes(b"")
    with pytest.raises(ValueError) as e:
        src.read_plan_json(tmp_path / "ddl_plan.json")
    assert "ddl_plan.json" in str(e.value) and "0 bytes" in str(e.value)
    assert "provision" in str(e.value)


def test_the_pointer_cannot_name_a_file_outside_its_folder(tmp_path):
    src = _source()
    (tmp_path / "ddl_plan.json").write_text(json.dumps(
        {"snowmig_compressed_to": "../../etc/other.gz"}), encoding="utf-8")
    with pytest.raises(ValueError, match="pointer"):
        src.read_plan_json(tmp_path / "ddl_plan.json")


@pytest.mark.parametrize("stage", ["01_create_structure.py", "02_copy_schema.py",
                                   "03_reconcile.py"])
def test_every_plan_reading_stage_goes_through_the_reader(stage):
    text = (DATAPLANE / stage).read_text(encoding="utf-8")
    assert "read_plan_json(ddl_path)" in text
    assert "json.loads(ddl_path.read_text" not in text
