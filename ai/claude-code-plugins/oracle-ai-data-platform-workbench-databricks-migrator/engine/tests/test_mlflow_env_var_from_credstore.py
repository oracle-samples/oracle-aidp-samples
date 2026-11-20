"""recommended_env_vars(): MLflow tracking URI resolution for the cluster env var.

The MLflow URI embeds a password, so it lives in the credential store (a ref in
the YAML), not as plaintext. Because dbx_export runs ON an AIDP cluster, the
cluster-create path can resolve that ref via the kernel's `aidputils` global and
bake the real URI into the new cluster's sparkEnvVariables — so every task gets
it uniformly (vs. the reactive per-cell fix that only fires on error).

These tests mock `aidputils` so they run on a laptop.

Run: python -m pytest tests/test_mlflow_env_var_from_credstore.py -v
"""
from types import SimpleNamespace

from dbx_export import cluster_converter as cc


def _cfg(mlflow_val):
    return SimpleNamespace(
        secrets=SimpleNamespace(mlflow_tracking_uri=mlflow_val),
        aidp=SimpleNamespace(workspace_id="ws-123"),
    )


def _ref():
    return SimpleNamespace(name="migration_tool_secrets", key="mlflow-tracking-uri")


def _fake_aidputils(uri):
    return SimpleNamespace(
        secrets=SimpleNamespace(get=lambda name, key=None: uri)
    )


def test_credstore_ref_resolved_on_cluster(monkeypatch):
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(
        cc, "_get_aidputils_from_kernel",
        lambda: _fake_aidputils("https://user:pass@mlflow.example:5000 \n"),
    )
    out = cc.recommended_env_vars(_cfg(_ref()))
    assert out["MLFLOW_TRACKING_URI"] == "https://user:pass@mlflow.example:5000"


def test_credstore_ref_skipped_on_laptop(monkeypatch):
    # aidputils is None off-cluster → ref cannot be resolved → not injected
    # (unchanged laptop behavior; reactive in-code fix remains the backup).
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(cc, "_get_aidputils_from_kernel", lambda: None)
    out = cc.recommended_env_vars(_cfg(_ref()))
    assert "MLFLOW_TRACKING_URI" not in out


def test_plain_string_uri_injected_without_aidputils(monkeypatch):
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
    monkeypatch.setattr(cc, "_get_aidputils_from_kernel", lambda: None)
    out = cc.recommended_env_vars(_cfg("https://plain.example:5000"))
    assert out["MLFLOW_TRACKING_URI"] == "https://plain.example:5000"


def test_operator_env_wins_over_credstore(monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "https://operator.override:5000")
    monkeypatch.setattr(
        cc, "_get_aidputils_from_kernel",
        lambda: _fake_aidputils("https://user:pass@credstore:5000"),
    )
    out = cc.recommended_env_vars(_cfg(_ref()))
    assert out["MLFLOW_TRACKING_URI"] == "https://operator.override:5000"


def test_resolve_failure_does_not_raise(monkeypatch):
    # A credential-store fetch error must never block cluster creation.
    monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)

    def _boom(name, key=None):
        raise RuntimeError("cred store unavailable")

    monkeypatch.setattr(
        cc, "_get_aidputils_from_kernel",
        lambda: SimpleNamespace(secrets=SimpleNamespace(get=_boom)),
    )
    out = cc.recommended_env_vars(_cfg(_ref()))  # must not raise
    assert "MLFLOW_TRACKING_URI" not in out
