"""
AIDP Optuna Compatibility Helpers
==================================
Wraps optuna.create_study() to route SQLite/journal storage away from the
/Volumes FUSE mount.

The FUSE problem:
    optuna.create_study(storage="sqlite:///path/study.db")
    Where path is on /Volumes/ — SQLite WAL mode relies on atomic file
    operations that FUSE breaks. Symptoms: "no such table: trials", study
    lock errors, or data loss between trials.

Solution:
    Use /tmp (local disk) for the active SQLite DB during study.optimize().
    After optimize() finishes, copy the DB to /Volumes/ for persistence.
    /tmp is always available on AIDP cluster nodes.

Usage:
    from aidp_compat import safe_optuna_create_study, finalize_optuna_study

    # Replace:
    study = optuna.create_study(name="my_study", storage="sqlite:///path/study.db")
    study.optimize(objective, n_trials=100)

    # With:
    study = safe_optuna_create_study("my_study", storage_path="/Volumes/.../study.db")
    study.optimize(objective, n_trials=100)
    finalize_optuna_study(study)  # copies DB from /tmp to /Volumes/
"""

import os
import shutil
import time


def safe_optuna_create_study(
    name: str,
    storage_path: str = None,
    direction: str = "minimize",
    **kwargs,
):
    """Create an Optuna study with FUSE-safe storage.

    If storage_path is on /Volumes/ (or starts with /dbfs/), routes the
    active SQLite DB to /tmp during study.optimize() to avoid FUSE WAL issues.
    After optimization, call finalize_optuna_study(study) to persist to /Volumes/.

    If storage_path is None, creates an in-memory study (fastest, no persistence).
    If storage_path is a non-/Volumes/ path or a URL, uses it directly.

    Args:
        name: Study name (optuna study_name param)
        storage_path: Target persistence path or URL. Examples:
            "/Volumes/default/default/dbfs/.../study.db"  → routed to /tmp
            "/dbfs/FileStore/.../study.db"  → translated + routed to /tmp
            None  → in-memory (no persistence)
            "sqlite:////tmp/study.db"  → used directly (already in /tmp)
        direction: "minimize" or "maximize"
        **kwargs: Passed through to optuna.create_study()

    Returns:
        optuna.Study with _tmp_db and _final_path attributes if routing to /tmp.
    """
    try:
        import optuna
    except ImportError as e:
        raise ImportError(
            "optuna is not installed. Install via cluster libraries: optuna>=3.0"
        ) from e

    # Translate legacy /dbfs/ paths
    if storage_path and storage_path.startswith("/dbfs/"):
        storage_path = "/Volumes/default/default/dbfs" + storage_path[5:]

    _tmp_db = None
    _final_path = None

    if storage_path is None:
        storage = None
    elif "/Volumes/" in str(storage_path) and not storage_path.startswith("sqlite://"):
        # Route SQLite to /tmp — FUSE WAL unsafe
        _tmp_db = f"/tmp/optuna_{name}_{os.getpid()}.db"
        _final_path = storage_path
        storage = f"sqlite:///{_tmp_db}"
    elif storage_path.startswith("/Volumes/") or storage_path.startswith("sqlite:////Volumes/"):
        # Catch sqlite:////Volumes/... form too
        raw_path = storage_path.replace("sqlite:///", "", 1)
        _tmp_db = f"/tmp/optuna_{name}_{os.getpid()}.db"
        _final_path = raw_path
        storage = f"sqlite:///{_tmp_db}"
    else:
        storage = storage_path

    study = optuna.create_study(study_name=name, storage=storage, direction=direction, **kwargs)

    # Attach routing metadata so finalize_optuna_study() knows what to do
    study._aidp_tmp_db = _tmp_db
    study._aidp_final_path = _final_path

    return study


def finalize_optuna_study(study, delay: float = 3.0) -> None:
    """Persist an Optuna study DB from /tmp to /Volumes/ after optimize().

    Call this immediately after study.optimize() when using safe_optuna_create_study()
    with a /Volumes/ storage_path. No-op if the study used in-memory storage or
    was already stored in a non-/tmp location.

    Args:
        study: optuna.Study returned by safe_optuna_create_study()
        delay: Seconds to wait after copy for FUSE propagation
    """
    tmp_db = getattr(study, "_aidp_tmp_db", None)
    final_path = getattr(study, "_aidp_final_path", None)

    if not tmp_db or not final_path:
        return  # no routing was done

    if not os.path.exists(tmp_db):
        raise FileNotFoundError(
            f"Optuna study DB not found at {tmp_db}. "
            f"Was study.optimize() called before finalize_optuna_study()?"
        )

    os.makedirs(os.path.dirname(os.path.abspath(final_path)), exist_ok=True)
    shutil.copy2(tmp_db, final_path)
    time.sleep(delay)

    # Clean up tmp file
    try:
        os.unlink(tmp_db)
    except OSError:
        pass
