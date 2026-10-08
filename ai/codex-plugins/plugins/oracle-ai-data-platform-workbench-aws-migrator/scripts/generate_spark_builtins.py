#!/usr/bin/env python3
"""Regenerate ``aws_aidp/translate/spark_builtins.py`` from a real Spark engine.

The Athena translator's ``unknown_function`` gate needs to know which functions
Spark actually provides.  Guessing that list is how silent mistranslations get
through, so it is generated from ``SHOW FUNCTIONS`` against the pinned runtime
rather than hand-maintained.

Usage (needs pyspark and a JDK; neither is a runtime dependency of this tool)::

    pip install 'pyspark~=3.5.0'
    python scripts/generate_spark_builtins.py

CI runs this with ``--check`` in the spark-parser lane to fail if the checked-in
list has drifted from the pinned Spark version.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

TARGET = Path(__file__).resolve().parent.parent / "aws_aidp" / "translate" / "spark_builtins.py"
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")

HEADER = '''"""Spark SQL vocabulary — GENERATED FILE, DO NOT EDIT BY HAND.

Regenerate with ``python scripts/generate_spark_builtins.py`` (requires pyspark).
Sources on Spark {version}: ``SHOW FUNCTIONS`` for the functions, the
``SqlBaseLexer`` ANTLR vocabulary for the keywords.

Operator entries (``+``, ``<=>``, ``||`` …) are excluded: the validation gate only
matches ``name(`` call syntax, so they can never appear as a candidate.

SPARK_KEYWORDS exists because many keywords are legally followed by "(" without
being a call -- ``GROUPING SETS (…)``, ``CASE … THEN (…) ELSE (…)``,
``CUBE (…)``.  Hand-maintaining that list is how false "unknown function"
flags get shipped, so it is generated too.
"""

SPARK_VERSION = "{version}"

SPARK_BUILTINS = frozenset({{
{entries}}})

SPARK_KEYWORDS = frozenset({{
{keywords}}})
'''


def collect(session) -> tuple[set[str], set[str], str]:
    rows = session.sql("SHOW FUNCTIONS").collect()
    names = {row[0].split(".")[-1].lower() for row in rows}
    return (
        {n for n in names if _IDENTIFIER.fullmatch(n)},
        collect_keywords(session),
        session.version,
    )


def collect_keywords(session) -> set[str]:
    """Every keyword literal in Spark's SQL lexer vocabulary."""
    vocabulary = (session._jvm.org.apache.spark.sql.catalyst.parser
                  .SqlBaseLexer.VOCABULARY)
    words: set[str] = set()
    index = 0
    misses = 0
    while misses < 50:
        literal = vocabulary.getLiteralName(index)
        if literal is None:
            misses += 1
        else:
            misses = 0
            word = literal.strip("'").lower()
            if _IDENTIFIER.fullmatch(word):
                words.add(word)
        index += 1
    return words


def render(names: set[str], keywords: set[str], version: str) -> str:
    entries = "".join(f"    {name!r},\n" for name in sorted(names))
    keyword_entries = "".join(f"    {word!r},\n" for word in sorted(keywords))
    return HEADER.format(version=version, entries=entries, keywords=keyword_entries)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="exit non-zero if the checked-in file is stale")
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("pyspark is not installed; install it with: pip install 'pyspark~=3.5.0'",
              file=sys.stderr)
        return 2

    session = (SparkSession.builder
               .master("local[1]")
               .appName("aws-aidp-builtins")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    try:
        names, keywords, version = collect(session)
    finally:
        session.stop()

    rendered = render(names, keywords, version)
    if args.check:
        current = TARGET.read_text(encoding="utf-8") if TARGET.exists() else ""
        if current != rendered:
            print(f"{TARGET} is stale for Spark {version}; "
                  f"rerun scripts/generate_spark_builtins.py", file=sys.stderr)
            return 1
        print(f"{TARGET.name} is current for Spark {version} "
              f"({len(names)} functions, {len(keywords)} keywords)")
        return 0

    TARGET.write_text(rendered, encoding="utf-8")
    print(f"wrote {TARGET} — {len(names)} functions, {len(keywords)} keywords "
          f"from Spark {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
