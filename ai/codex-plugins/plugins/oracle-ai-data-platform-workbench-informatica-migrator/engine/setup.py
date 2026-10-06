from setuptools import setup

# Cluster-side runtime library, installed ONTO the AIDP cluster as a
# cluster library: build with `pip wheel engine/ -w dist/` and install the
# resulting wheel on the cluster (README, "Installing infa_compat on the
# cluster"). Distinct from the root pyproject.toml, which packages the
# workstation-side infa2aidp library.
#
# Deliberately minimal: no install_requires -- every pyspark/delta/oracledb
# use is deferred into function bodies (see infa_compat/__init__.py), so
# this package has zero hard dependencies beyond the stdlib and imports
# cleanly on a machine with no Spark/cluster at all (demo.sh, the test
# suite). Runtime callers on the actual cluster get pyspark/delta from
# the cluster runtime itself, never from this package's own deps.
setup(
    name="infa_compat",
    version="0.1.0",
    packages=["infa_compat"],
    description="Informatica runtime semantics for Spark on Oracle AIDP",
    python_requires=">=3.9",
)
