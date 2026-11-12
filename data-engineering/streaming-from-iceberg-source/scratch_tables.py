# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/.
"""Scratch Iceberg tables that one notebook run creates -- and cleans up, and nothing else.

Notebook 3 runs the changes that would break notebooks 1 and 2 (renaming or dropping a column,
rebuilding a table, expiring snapshots) against small scratch copies. ``ScratchTables`` records
the exact name and location of every table it creates; ``drop_all`` drops only those and deletes
only their folders. Tables it did not create -- from an earlier run, another user or anything
else -- are never touched, whatever their names.

The record lives in the notebook's kernel. If the kernel restarts before the clean-up, the tables
of the earlier session are left in place; ``create`` prints each name so they can be dropped by
hand.
"""

from __future__ import annotations

import time
import uuid
from typing import Callable, List, Optional, Tuple


def delete_folder(spark, location: str, attempts: int = 5, wait_seconds: float = 20) -> bool:
    """Delete an Object Storage folder through the Hadoop FileSystem API; True once it is gone.

    Retries: right after ``DROP TABLE``, calls on the dropped table's folder can fail for a while
    with "GeneratePar operation in LakeSharing service (404, NotAuthorizedOrNotFound)", and one
    recursive delete of a large folder once left about half of its objects behind.
    """
    folder = spark._jvm.org.apache.hadoop.fs.Path(location)
    fs = folder.getFileSystem(spark._jsc.hadoopConfiguration())
    for attempt in range(attempts):
        try:
            if fs.exists(folder):
                fs.delete(folder, True)
            if not fs.exists(folder):
                return True
        except Exception as e:
            print("   retrying the delete:", str(e).splitlines()[0][:120])
        if attempt < attempts - 1:
            time.sleep(wait_seconds)
    return False


class ScratchTables:
    """Creates scratch Iceberg tables at ``<warehouse>/<schema>/<table>`` and remembers them."""

    def __init__(self, spark, catalog: str, schema: str, warehouse: str, prefix: str = "scratch_"):
        self.spark = spark
        self.catalog, self.schema = catalog, schema
        self.warehouse = warehouse.rstrip("/")
        self.prefix = prefix
        self.created: List[Tuple[str, str]] = []      # (full table name, location), in creation order

    def create(self, name: str, select_sql: str) -> str:
        """Create a new table ``<prefix><name>_<random>`` as ``select_sql`` and return its full name."""
        short = f"{self.prefix}{name}_{uuid.uuid4().hex[:6]}"
        table = f"{self.catalog}.{self.schema}.{short}"
        location = f"{self.warehouse}/{self.schema}/{short}"
        self.spark.sql(f"CREATE TABLE {table} USING iceberg LOCATION '{location}' AS {select_sql}")
        self.created.append((table, location))
        print("created scratch table", table)
        return table

    def drop_all(self, delete: Optional[Callable[[str], bool]] = None) -> List[str]:
        """Drop the tables this object created and delete their folders; returns the dropped names.

        ``delete`` deletes one folder and returns True once it is gone; by default
        ``delete_folder``. A table whose folder could not be deleted is reported, and kept in
        ``created`` so that a second call retries it.
        """
        delete = delete or (lambda location: delete_folder(self.spark, location))
        dropped, remaining = [], []
        for table, location in self.created:
            self.spark.sql(f"DROP TABLE IF EXISTS {table}")
            if delete(location):
                dropped.append(table)
                print("dropped", table)
            else:
                remaining.append((table, location))
                print("dropped", table, f"-- but its folder is left; delete {location} in the OCI Console")
        self.created = remaining
        return dropped
