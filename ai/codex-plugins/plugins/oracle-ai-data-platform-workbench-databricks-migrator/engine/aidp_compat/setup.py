from setuptools import setup, find_packages

setup(
    name="aidp_compat",
    version="0.5.4",
    packages=find_packages(where=".."),
    package_dir={"": ".."},
    description="AIDP Compatibility Layer for Databricks DBUtils",
    python_requires=">=3.9",
)
