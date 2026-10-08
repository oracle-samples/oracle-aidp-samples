# See your data lineage in a notebook

AIDP records lineage on its own whenever a notebook or a workflow task runs: which tables were read,
which were written, by which job, and how each column was derived.
[`Visualize_AIDP_Lineage.ipynb`](./Visualize_AIDP_Lineage.ipynb) reads that lineage through the
AIDP **DataLineage** REST API and draws it inline:

| Graph | What it shows |
|---|---|
| Tables | Upstream and downstream tables around an anchor table, and the jobs or notebooks that wrote each one. Hover a box for its full name, or a job or notebook for its workspace and last run. |
| Columns | Which column feeds which, coloured by how it was derived: copied as is, transformed, or aggregated. |

The same response is then turned into two pandas DataFrames, nodes and edges, ready for a graph
library, a catalog or your own reports. A last section turns it into [OpenLineage](https://openlineage.io/)
events, the open format that DataHub, Marquez and OpenMetadata accept, column lineage included. The
events validate against the OpenLineage schemas; posting them to a catalog is shown but has not been
tested. Nothing is written to disk: for a file, use **Export** in the
Lineage view or the `exportLineage` operation, which return CSV.

## What you need

- A credential of type **Service account** in the AIDP Credential Store. The notebook reads its
  `userId`, `tenancyId`, `fingerprint` and `privateKey` fields.
- That service account granted **access to AIDP** and **read access to the data**. It is a separate
  identity and sees nothing until you grant it; the notebook stops after building the demo tables so
  you can do this by hand (step 3).
- A compute created, or restarted, after lineage became available in your instance. Only those
  capture lineage.
- Your AI Data Platform OCID and its region.

No extra libraries: requests are signed with the `oci` package the cluster already has. Outside AIDP
the notebook falls back to your local `~/.oci/config`, so it also runs on a laptop.

## Running it

Fill in the configuration cell and run top to bottom. Step 2 builds four small demo tables in
`default.lineage_sample` so there is something to draw, and the run stops at step 3 on purpose: grant
the service account access to AIDP and to that schema, set `ACCESS_GRANTED = True`, and run on. The
last cell drops the demo tables.

To draw one of your own tables instead, skip step 2, set `ANCHOR_TABLE`, and make sure the service
account can read that table's schema.

## Things worth knowing about lineage on AIDP

- **The anchor id is `aidp://catalogs@<AI Data Platform OCID>/o/<catalog.schema.table>`.** The API
  reference page shows this example with the OCID part missing. A bare `catalog.schema.table` is
  rejected with `400 Invalid anchorNode`; a well-formed id for a table that does not exist returns
  `404`.
- **Lineage lands seconds after the write**, while the run is still going (about 30 seconds in our
  test). The fetch cell waits up to two minutes for the demo tables it has just built.
- **Only the latest run of each job is kept.** If a job runs again without writing a table, that
  table's lineage from the earlier run disappears.
- **Only computes created or restarted after lineage became available capture it.** Setting
  `spark.aidp.lineage.enabled = false` on a compute turns capture off for that compute.
- **A task that writes several tables comes back as one node**, with every link tagged by the Spark
  stage that produced it. The drawing splits the task per stage, so the graph reads left to right
  instead of looping back on itself.
- **Prefer the SDK?** `DataLineageClient.fetch_entity_lineage` in
  [`aidp-python-client`](https://github.com/oracle-samples/aidataplatform-sdk) sends the same request.
  It has to be installed with `pip`, which needs the cluster to reach PyPI.

## About the committed outputs

The outputs in this notebook come from a reference run on a test instance, with the configuration
cell reset to placeholders. Your own outputs show your catalog, schema, table, job and workspace
names, so clear them before committing your copy anywhere public.
