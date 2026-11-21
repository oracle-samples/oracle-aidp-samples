"""Pin: cluster-side dependency resolution installs CURATED mappings only.

An import name is installed iff it has a curated _IMPORT_TO_PIP mapping; the
mere existence of a same-named PyPI package must never install it (squatted or
unrelated packages would land on the customer's cluster), and unknown names
are reported, not passed through to pip.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import cluster_lifecycle as cl
import pytest


@pytest.fixture(autouse=True)
def _no_ambient_config(monkeypatch, tmp_path):
    """The caller pins drive real functions that run config discovery (via
    oci_auth); isolate them from any config another test staged under HOME."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("AIDP_CONFIG_PATH", raising=False)
    # the REST layer is neutered in the pins; the signer must not require a
    # real ~/.oci/config either
    monkeypatch.setattr(cl, "_get_signer", lambda: None)


def _fresh(monkeypatch, exists):
    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_exists", lambda name: exists)
    # ordinary cases assume the index is reachable; the offline cases below
    # stub reachability False explicitly — never depend on the real network
    monkeypatch.setattr(cl, "_pypi_reachable", lambda: True)


def test_unmapped_name_on_pypi_is_not_installed(monkeypatch, capsys):
    _fresh(monkeypatch, exists=True)
    assert cl._resolve_pip_name("totally_unmapped_import") is None
    assert "will NOT be installed automatically" in capsys.readouterr().out


def test_unmapped_name_not_on_pypi_is_not_installed(monkeypatch, capsys):
    _fresh(monkeypatch, exists=False)
    assert cl._resolve_pip_name("no_such_pkg_anywhere") is None
    assert "not installed" in capsys.readouterr().out


def test_curated_mapping_still_resolves(monkeypatch):
    _fresh(monkeypatch, exists=True)
    name, target = next(iter(cl._IMPORT_TO_PIP.items()))
    resolved = cl._resolve_pip_name(name)
    assert resolved is not None and resolved.lower().startswith(target.split("==")[0].split(">=")[0].lower()[:3])


def test_offline_unmapped_name_is_not_installed(monkeypatch, capsys):
    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_reachable", lambda: False)
    assert cl._resolve_pip_name("unmapped_offline_import") is None
    assert "NOT installed" in capsys.readouterr().out


def test_offline_curated_mapping_still_used(monkeypatch):
    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_reachable", lambda: False)
    name, target = next(iter(cl._IMPORT_TO_PIP.items()))
    assert cl._resolve_pip_name(name) == target


def test_batch_survives_a_rejected_import(monkeypatch, capsys):
    """Caller-level pin on the REAL ensure_requirements_installed: one unmapped
    import never disrupts the batch, and the install command carries no None."""
    import asyncio

    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_exists", lambda name: True)
    monkeypatch.setattr(cl, "_pypi_reachable", lambda: True)
    # The cluster-libraries REST layer is out of scope for this pin: record and no-op.
    rest_calls = []
    class _Resp:
        status_code = 200
        text = "{}"
        def json(self):
            return {"items": [], "status": "INSTALLED"}
        def raise_for_status(self):
            return None
    monkeypatch.setattr(cl.http_requests, "get", lambda *a, **k: (rest_calls.append(("get", a)), _Resp())[1])
    monkeypatch.setattr(cl.http_requests, "patch", lambda *a, **k: (rest_calls.append(("patch", a)), _Resp())[1])
    monkeypatch.setattr(cl.http_requests, "post", lambda *a, **k: (rest_calls.append(("post", a)), _Resp())[1])
    mapped_name = next(iter(cl._IMPORT_TO_PIP))

    executed = []

    class _Session:
        cluster_id = "fake-cluster"

        async def execute(self, code, timeout=120):
            executed.append(code)
            if "__AIDP_MIG_MISSING_BEGIN__" in code:
                # the import probe: report BOTH candidates missing
                text = ("__AIDP_MIG_MISSING_BEGIN__"
                        f"{mapped_name},totally_unmapped_import"
                        "__AIDP_MIG_MISSING_END__")
                return {"status": "ok", "outputs": [{"type": "stream", "text": text}]}
            if "job_requirements" in code or "requirements" in code:
                # existing requirements already carry the mapped package, so the
                # REST install step (out of scope for this pin) is skipped.
                resolved = cl._IMPORT_TO_PIP[mapped_name]
                return {"status": "ok", "outputs": [{"type": "stream", "text": resolved}]}
            return {"status": "ok", "outputs": [{"type": "stream", "text": "ok"}]}

    async def _drive():
        return await cl.ensure_requirements_installed(
            "fake-cluster", _Session(), [mapped_name, "totally_unmapped_import"], "/tmp/out")

    try:
        result = asyncio.run(_drive())
    except TypeError as exc:  # the pre-fix crash shape
        raise AssertionError(f"caller crashed on a None resolution: {exc}")
    blob = "\n".join(executed)
    assert "None" not in blob, "a None resolution leaked into the install command"
    out = capsys.readouterr().out
    assert "NOT installing" in out and "totally_unmapped_import" in out
    assert not any("totally_unmapped_import" in str(c) for c in rest_calls)


def test_single_install_refuses_unmapped(monkeypatch, capsys):
    """Caller-level pin on the REAL install_missing_package: an unmapped import
    returns False (its boolean contract) and writes nothing."""
    import asyncio

    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_exists", lambda name: True)
    monkeypatch.setattr(cl, "_pypi_reachable", lambda: True)

    executed = []

    class _Session:
        cluster_id = "fake-cluster"

        async def execute(self, code, timeout=120):
            executed.append(code)
            return {"status": "ok", "outputs": [{"type": "stream", "text": "MISSING"}]}

    async def _drive():
        return await cl.install_missing_package(
            "fake-cluster", _Session(), "totally_unmapped_import", "/tmp/out", timeout=60)

    ok = asyncio.run(_drive())
    assert ok is False
    assert not any("pip install" in c for c in executed), "nothing may be installed"
    assert "no curated pip mapping" in capsys.readouterr().out
