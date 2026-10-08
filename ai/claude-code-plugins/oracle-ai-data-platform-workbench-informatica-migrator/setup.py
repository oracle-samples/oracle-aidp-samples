from setuptools import setup, find_packages

setup(
    name="infa2aidp",
    version="0.1.0",
    description="Migrate current Informatica ETL (IDMC/IICS, PowerCenter 10.5) to Spark on Oracle AI Data Platform",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Oracle AIDP Team",
    python_requires=">=3.9",
    packages=find_packages(),
    install_requires=[
        "pyyaml>=6.0",
        "requests>=2.28",
        "tzdata",  # IANA zones for --schedule-timezone; see pyproject.toml
    ],
    extras_require={
        "llm": ["anthropic>=0.40"],
        "reconcile": ["jaydebeapi>=1.2", "oracledb>=2.0"],
        "crawl": ["oracledb>=2.0"],  # PowerCenter repository access
        "dev": ["pytest>=7.0", "pytest-cov>=4.0"],
        "all": [
            "anthropic>=0.40",
            "jaydebeapi>=1.2",
            "oracledb>=2.0",
            "pytest>=7.0",
            "pytest-cov>=4.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "infa2aidp=infa2aidp.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Software Development :: Code Generators",
    ],
)
