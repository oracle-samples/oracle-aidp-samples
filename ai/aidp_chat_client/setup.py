"""
Setup script for AIDP Chat Client library.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="aidp-chat-client",
    version="1.0.0",
    author="Oracle",
    description="A Python client library for interacting with Oracle AIDP Chat Agent endpoints",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oracle-samples/oracle-aidp-samples",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Universal Permissive License (UPL)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    keywords="oracle oci aidp chat agent ai",
)
