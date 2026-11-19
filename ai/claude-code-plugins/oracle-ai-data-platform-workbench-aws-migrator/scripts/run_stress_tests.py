#!/usr/bin/env python3
"""Run the dependency-free offline stress suite."""
from __future__ import annotations

import argparse
import os
import sys
import unittest
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release", action="store_true",
        help="enable slower release-scale tests",
    )
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.release:
        os.environ["AWS_AIDP_RUN_SLOW"] = "1"

    root = Path(__file__).resolve().parents[1]
    suite = unittest.defaultTestLoader.discover(
        str(root / "tests" / "stress"), pattern="test_*.py", top_level_dir=str(root)
    )
    result = unittest.TextTestRunner(verbosity=1 if args.quiet else 2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
