"""Provisioning policy exercised through the actual process_notebook loop."""
import asyncio
from types import SimpleNamespace

import pytest
import yaml

from test_pip_magic_gate import _Kernel, _patch_common, _saved_notebook, jm
import cluster_lifecycle
import customization_pack


SCRIPT = "/Workspace/test-dependencies/ray-head.sh"
SOURCE = "import ray\nray.init()\n"


@pytest.mark.parametrize("configuration", ["no_pack", "pack_no_entry", "configured"])
@pytest.mark.parametrize("outcome", ["present", "absent", "api_error"])
@pytest.mark.parametrize("run_all,task", [
    (True, "scoring"), (False, "scoring"), (False, "dep_child"),
])
def test_process_notebook_ray_provisioning_policy(
        monkeypatch, tmp_path, capsys, configuration, outcome, run_all, task):
    notebook = {"cells": [{"cell_type": "code", "source": [SOURCE],
                           "metadata": {}, "outputs": [], "execution_count": None}],
                "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
    _patch_common(monkeypatch, tmp_path, notebook, [])
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    pack_dir = tmp_path / "pack"
    pack_dir.mkdir()
    configured = configuration == "configured"
    (pack_dir / "overrides.yaml").write_text(yaml.safe_dump(
        {"ray_init_script": {"destination": SCRIPT}} if configured else {}
    ), encoding="utf-8")
    monkeypatch.setattr(customization_pack, "_PACK_DIR",
                        None if configuration == "no_pack" else str(pack_dir))
    calls = []
    monkeypatch.setattr(cluster_lifecycle, "_get_signer", lambda: "test-signer")
    monkeypatch.setattr(cluster_lifecycle, "_cluster_url", lambda key: f"https://test.invalid/{key}")

    def get(url, **kwargs):
        calls.append((url, kwargs))

        def check():
            if outcome == "api_error":
                raise RuntimeError("fixture cluster API unavailable")

        return SimpleNamespace(raise_for_status=check, json=lambda: {
            "clusterRuntimeConfig": {"initScripts": [
                {"destination": SCRIPT}, {"invalid": "ignored"}, None,
            ] if outcome == "present" else []},
        })

    monkeypatch.setattr(cluster_lifecycle.http_requests, "get", get)
    kernel = _Kernel()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_pip/source.ipynb", kernel, "test_pip", task, {}, run_all=run_all))
    blocked = run_all and configured and outcome != "present"
    if blocked:
        assert result["status"] == "RAY_CONVERSION_UNSUPPORTED"
        assert "execution cluster test-cluster" in result["error"]
        assert not any(code.strip() == SOURCE.strip() for code in kernel.executed)
        assert not kernel.files
    else:
        assert result["status"] == ("PASS" if run_all else "CONVERTED_NOT_VALIDATED"), result
        saved = _saved_notebook(kernel)
        assert any("".join(cell["source"]).strip() == SOURCE.strip() for cell in saved["cells"])
        assert any(code.strip() == SOURCE.strip() for code in kernel.executed) is run_all
    assert len(calls) == 1
    assert calls[0][0] == "https://test.invalid/test-cluster"
    if not configured or not run_all:
        output = capsys.readouterr().out
        assert output.count("WARN RAY PROVISIONING") == 1
        assert "cluster=test-cluster; current initScripts=" in output
        if outcome == "api_error":
            assert "UNVERIFIED" in output
            assert "fixture cluster API unavailable" in output
        else:
            assert f"current initScripts={repr([SCRIPT] if outcome == 'present' else [])}" in output
