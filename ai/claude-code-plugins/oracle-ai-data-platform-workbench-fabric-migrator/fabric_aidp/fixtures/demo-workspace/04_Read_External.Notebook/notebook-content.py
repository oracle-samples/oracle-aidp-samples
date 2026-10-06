# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "SalesLake"
# META     }
# META   }
# META }

# CELL ********************

external = spark.table("claims_raw_s3")
external.count()
