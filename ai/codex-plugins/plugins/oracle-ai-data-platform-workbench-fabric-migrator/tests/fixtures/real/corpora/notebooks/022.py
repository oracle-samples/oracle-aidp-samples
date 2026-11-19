# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# variable "spark" is not set when calling sc.addPyFile()
_is_sc_addPyFile = not "spark" in locals()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark import  SparkContext
from pyspark.sql import DataFrame, SparkSession



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Using notebookutils

# CELL ********************

import notebookutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ls(path: str) -> DataFrame:
    print(f"Listing {path} ...")
    return notebookutils.fs.ls(path)

# a simple test which is only run when developing the library but not when the library is added via sc.addPyFile()
if not _is_sc_addPyFile:
    display(ls("/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Using Spark(`spark`) and SparkContext(`sc`)

# CELL ********************

if _is_sc_addPyFile:
    # as our library makes calls to spark, we need to create local instances of SparkContext "sc" and Sparksession "spark"
    if not "sc" in locals():
        #print("Creating local SparkContext variable 'sc' ... ", end = "")
        sc = SparkContext.getOrCreate()
        #print("Done!")

    if not "spark" in locals():
        #print("Creating local SparkSession variable 'spark' ... ", end = "")
        spark = (SparkSession(sc)
            .builder
            .getOrCreate())
        #print("Done!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_to_df(table_name: str) -> DataFrame:
    df = spark.sql(f"SELECT * FROM {table_name}")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
