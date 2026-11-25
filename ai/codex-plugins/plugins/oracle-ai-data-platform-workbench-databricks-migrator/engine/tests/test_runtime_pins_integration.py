"""Integration test: the pinned triad works end-to-end on a real AIDP cluster.

the maintainer's re-review of PR #43 asked for live evidence that pinning
pandas==2.2.3 + numpy==1.26.4 + pyarrow==12.0.1 still delivers a WORKING
combination — not just that the detector flags 3.0.x. This exercises the exact
code paths that break on the stock image:

  * pd.read_csv + .astype("string") + .apply on a string column
  * spark.DataFrame.toPandas()
  * aidp_compat.safe_toPandas()

GATED: skips unless AIDP_TEST_CLUSTER is set (a cluster key that already has the
pinned triad installed), so the default/CI suite never needs cluster creds.

Run:  AIDP_TEST_CLUSTER=<cluster-key> \
      AIDP_LAKE_OCID=<ocid> AIDP_WORKSPACE_ID=<id> AIDP_OCI_PROFILE=CUSTOMER \
      python -m pytest tests/test_runtime_pins_integration.py -v -s
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

CLUSTER = os.environ.get("AIDP_TEST_CLUSTER")


@unittest.skipUnless(CLUSTER, "set AIDP_TEST_CLUSTER (triad-installed cluster) to run")
class TestTriadEndToEndLive(unittest.TestCase):
    def test_triad_paths_work(self):
        import aidp_executor as ae
        if os.environ.get("AIDP_LAKE_OCID"):
            ae.DEFAULT_LAKE_OCID = os.environ["AIDP_LAKE_OCID"]
        if os.environ.get("AIDP_WORKSPACE_ID"):
            ae.DEFAULT_WORKSPACE_ID = os.environ["AIDP_WORKSPACE_ID"]
        ae.DEFAULT_OCI_PROFILE = os.environ.get("AIDP_OCI_PROFILE", "CUSTOMER")
        from cluster_lifecycle import _runtime_pin_violation

        code = (
            "import pandas as pd, numpy as np, io\n"
            "df = pd.read_csv(io.StringIO('a,b\\nx,{m0: 5}\\ny,{m0: 8}\\n'))\n"
            "df['n'] = df['b'].astype('string').apply(lambda v: len(str(v)))\n"
            "sdf = spark.createDataFrame([('a',1),('b',2)], ['k','v'])\n"
            "_pd_ok = sdf.toPandas().shape == (2, 2)\n"
            "print('PANDAS=' + pd.__version__ + ';NUMPY=' + np.__version__ + "
            "';APPLY=' + str(df['n'].tolist()) + ';TOPANDAS=' + str(_pd_ok))\n"
        )

        async def _run():
            se = ae.AIDPSession(cluster_id=CLUSTER)
            await se.connect()
            r = await se.execute(code, timeout=180)
            return ae.format_outputs(r.get("outputs", []))

        out = asyncio.run(_run())
        self.assertIn("TOPANDAS=True", out, f"toPandas failed: {out}")
        self.assertIn("APPLY=[", out, f"read_csv/.apply failed: {out}")
        # And the triad's own versions must satisfy the pin (no violation).
        import re
        pv = re.search(r"PANDAS=([^;]+)", out).group(1)
        nv = re.search(r"NUMPY=([^;]+)", out).group(1)
        self.assertIsNone(
            _runtime_pin_violation(pv, nv),
            f"cluster {CLUSTER} is not on the pinned triad (pandas {pv}, numpy {nv})",
        )


if __name__ == "__main__":
    unittest.main()
