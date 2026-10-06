# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "SalesLake"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Ingest claims
# Reads the daily drop and lands it.

# CELL ********************

raw = spark.read.parquet(
    "abfss://AcmeWS@onelake.dfs.fabric.microsoft.com/SalesLake.Lakehouse/Files/raw/claims")
display(raw)

# CELL ********************

raw.write.mode("overwrite").saveAsTable("claims_daily")
