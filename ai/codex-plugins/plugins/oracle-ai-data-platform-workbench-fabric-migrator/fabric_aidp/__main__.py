"""`python3 -m fabric_aidp`: the CLI, for when the `fabric-aidp` script is
not on PATH (pip's user scripts directory often is not)."""
from fabric_aidp.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
