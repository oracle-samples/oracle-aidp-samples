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

agg = spark.table("claims_agg")
clean = spark.table("dbo.claim")
display(agg)
