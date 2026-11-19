"""The PAR download is bounded, retried when transient, and never leaks the
URL.

Found in review. download_ws_file called `urlopen(url)` with no timeout,
and the process default socket timeout is None: a stalled object-storage
GET hung `fetch`, and `run --job snowmig_00_discover` after a successful
discovery, indefinitely -- the unbounded wait DEFAULT_CLI_TIMEOUT was added
to prevent. The GET was also not retried, so one transient blip reported
"manifest NOT fetched".

The GET now carries an explicit timeout and is retried by the read rule,
and neither the retry line nor the final error carries the
pre-authenticated URL (it grants read access for as long as it lives).
"""
import io
import urllib.error

import pytest

from target.provisioning import (
    DOWNLOAD_TIMEOUT, ProvisionTransportError, download_ws_file)

PAR = "https://objectstorage.example.invalid/p/SECRET-PAR-TOKEN/o/x"


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _call(op, **kw):
    return {"parUrl": PAR, "size": 3}


def test_the_get_carries_a_timeout(tmp_path):
    seen = []

    def opener(url, **kw):
        seen.append(kw)
        return _Resp(b"abc")

    download_ws_file(_call, workspace="ws", path="a/b.json",
                     dest=tmp_path / "b.json", opener=opener)
    assert seen == [{"timeout": DOWNLOAD_TIMEOUT}]
    assert 0 < DOWNLOAD_TIMEOUT <= 600


def test_a_transient_failure_is_retried_without_the_url(tmp_path, capsys):
    attempts = []

    def opener(url, **kw):
        attempts.append(url)
        if len(attempts) == 1:
            raise urllib.error.URLError(f"timed out reading {url}")
        return _Resp(b"abc")

    res = download_ws_file(_call, workspace="ws", path="a/b.json",
                           dest=tmp_path / "b.json", opener=opener,
                           sleep=lambda _s: None)
    assert res["size"] == 3 and len(attempts) == 2
    out = capsys.readouterr()
    assert "RETRY" in out.out
    assert "SECRET-PAR-TOKEN" not in out.out + out.err


def test_a_final_failure_names_the_path_not_the_url(tmp_path):
    def opener(url, **kw):
        raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)

    with pytest.raises(ProvisionTransportError) as exc:
        download_ws_file(_call, workspace="ws", path="a/b.json",
                         dest=tmp_path / "b.json", opener=opener,
                         sleep=lambda _s: None)
    assert "a/b.json" in str(exc.value)
    assert "SECRET-PAR-TOKEN" not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__context__ is None \
        or "SECRET-PAR-TOKEN" not in repr(exc.value.__context__)
    assert not (tmp_path / "b.json").exists()
