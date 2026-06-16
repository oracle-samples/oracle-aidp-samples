# Working with Spark SQL Table Properties

This sample demonstrates how to manage **table properties** — arbitrary `key = value`
metadata attached to a table — from a Spark notebook on the Oracle AI Data Platform (AIDP)
lakehouse. Table properties are a convenient place to record ownership, data-classification
labels, lineage hints, or any custom governance metadata so the context travels with the
data itself.

The notebook walks through the full lifecycle of a property on a managed **Delta** table:

1. Create a demo table.
2. Inspect the engine-managed default properties (`SHOW TBLPROPERTIES`).
3. Store **structured (JSON) metadata** in a property (`json.dumps` → string value).
4. Read a property back — as a DataFrame, as a Python dict, and as a single raw value.
5. Overwrite an existing property (`SET TBLPROPERTIES` again).
6. Remove a property (`UNSET TBLPROPERTIES IF EXISTS`).

The sample artifact is:

- [working_with_table_properties.ipynb](./working_with_table_properties.ipynb)

The notebook keeps its captured outputs, so you can read the expected results on GitHub
without running it.

## Key idea: values are strings

Table-property values are always stored as **strings**. To attach structured metadata,
serialize a Python object with `json.dumps(...)` and store that JSON string; read it back
with `json.loads(...)`.

## Common operations

```python
# Set (or overwrite) a property
spark.sql("ALTER TABLE <catalog>.<schema>.<table> SET TBLPROPERTIES ('owner' = 'team-x')")

# Read all properties, or just one
spark.sql("SHOW TBLPROPERTIES <catalog>.<schema>.<table>").show(truncate=False)
spark.sql("SHOW TBLPROPERTIES <catalog>.<schema>.<table> ('owner')").show(truncate=False)

# Remove a property (IF EXISTS keeps it idempotent)
spark.sql("ALTER TABLE <catalog>.<schema>.<table> UNSET TBLPROPERTIES IF EXISTS ('owner')")
```

## Prerequisites

- A Spark environment with Delta support (e.g. AIDP Workbench with an active compute).
- An active Spark session (`spark`) — already provided inside an AIDP notebook.

## Environment

The notebook outputs were captured from a run in **AIDP Workbench**, whose Spark
runtime is Apache Spark 3.5.0 with Delta Lake. The operations are standard Spark
SQL and not specific to that version.

## Cleanup

The demo table is tiny and harmless, but you can drop it when finished:

```python
spark.sql("DROP TABLE IF EXISTS default.default.test_table_prop")
```

## Reference

- Spark SQL `ALTER TABLE ... SET/UNSET TBLPROPERTIES`:
  https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-alter-table.html
