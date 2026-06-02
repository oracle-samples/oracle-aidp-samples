"""Test-time precheck: error out if optional deps are missing.

Used by `make test-precheck` (which `make test` depends on). Lives in
a real script file rather than an inline `python -c` invocation so the
quoting around install-command examples stays readable and POSIX-safe.

The error message recommends `pip install 'pyspark>=3.5'` (single-quoted)
so a contributor copying the command into a POSIX shell does not get
the `>` interpreted as stdout redirection — that would create a
spurious `=3.5` file and silently install pyspark with no version
constraint.
"""

from __future__ import annotations

import importlib.util
import sys


REQUIRED_EXTRAS = ("pandasql", "prophet", "sklearn", "shap")


def _is_missing(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is None


def main() -> int:
    extras_missing = [m for m in REQUIRED_EXTRAS if _is_missing(m)]
    spark_missing = _is_missing("pyspark")

    parts: list[str] = []
    if extras_missing:
        parts.append(
            "Missing extras: "
            + ", ".join(extras_missing)
            + " (run `make install-all`)"
        )
    if spark_missing:
        parts.append(
            "Missing pyspark — install separately: "
            "pip install 'pyspark>=3.5' "
            "(or pip install 'qualifire[spark]'). "
            "pyspark is intentionally not in [all] so AIDP installs "
            "do not overwrite the pre-provisioned runtime"
        )

    if parts:
        sys.stderr.write("; ".join(parts) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
