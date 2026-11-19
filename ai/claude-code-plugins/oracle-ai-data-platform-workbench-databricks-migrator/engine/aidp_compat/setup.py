import os
import re

from setuptools import setup, find_packages

# Read version from aidp_compat/__init__.py — single source of truth.
# Same declaration is used at runtime (import aidp_compat; aidp_compat.__version__)
# and at cluster-install time (scripts/cluster_lifecycle.py TARGET_WHEEL_PATH).
# setup.py lives inside the aidp_compat package directory (alongside __init__.py),
# not in a parent — see `package_dir={"": ".."}` below which treats the workspace
# root as the source root for find_packages.
_here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_here, "__init__.py")) as _f:
    # Anchor to line start so a commented `# __version__ = "..."` doesn't match.
    _v_match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', _f.read(), re.MULTILINE)
if not _v_match:
    raise RuntimeError("aidp_compat/__init__.py: __version__ not found")

setup(
    name="aidp_compat",
    version=_v_match.group(1),
    packages=find_packages(where=".."),
    package_dir={"": ".."},
    description="AIDP Compatibility Layer for Databricks DBUtils",
    python_requires=">=3.9",
)
