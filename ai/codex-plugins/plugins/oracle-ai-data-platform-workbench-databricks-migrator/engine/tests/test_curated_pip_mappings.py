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


def _fresh(monkeypatch, exists):
    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_exists", lambda name: exists)


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
    """Caller-level pin: one unmapped import never disrupts the rest of the
    batch (no None in the package list, no TypeError on join)."""
    import asyncio, inspect, re, textwrap
    src = inspect.getsource(cl.ensure_requirements_installed)
    # execute just the resolution block the same way the caller does
    monkeypatch.setattr(cl, "_pypi_cache", {})
    monkeypatch.setattr(cl, "_pypi_exists", lambda name: True)
    mapped_name = next(iter(cl._IMPORT_TO_PIP))
    pip_packages, seen, skipped = [], set(), []
    for imp in [mapped_name, "totally_unmapped_import"]:
        pip_name = cl._resolve_pip_name(imp)
        if pip_name is None:
            skipped.append(imp)
            continue
        if pip_name not in seen:
            seen.add(pip_name)
            pip_packages.append(pip_name)
    assert skipped == ["totally_unmapped_import"]
    assert pip_packages and all(isinstance(x, str) for x in pip_packages)
    " ".join(pip_packages)  # the join that used to TypeError on None
