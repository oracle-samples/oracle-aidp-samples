"""%run-friendly runner for dbx_export.

Use from an AIDP notebook cell:

    %run /Workspace/<your-code-checkout>/dbx_export/run.py \
        --job-id $JOB_ID --dbx-proxy $DBX_PROXY --export-output $EXPORT_OUTPUT \
        --cluster-key $CLUSTER_KEY --dry-run

(--oci-config-path / --oci-profile are LAPTOP-ONLY flags: on cluster, OCI
credentials come from the AIDP Credential Store via aidputils and the flags
are ignored with a stderr note. dbx_export/auth.py reaches the kernel's
aidputils through get_ipython().user_ns as well as __main__, so the cluster
path survives IPython-style %run's __main__ swap.)

Self-injects its parent's parent onto sys.path so `import dbx_export` works
no matter where the package is deployed in /Workspace/. Does not call
sys.exit — returns the exit code as a printed line so the kernel survives.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PKG_PARENT = os.path.dirname(_HERE)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from dbx_export.__main__ import main  # noqa: E402

_rc = main()
print(f"\n[dbx_export.run] exit code: {_rc}")
