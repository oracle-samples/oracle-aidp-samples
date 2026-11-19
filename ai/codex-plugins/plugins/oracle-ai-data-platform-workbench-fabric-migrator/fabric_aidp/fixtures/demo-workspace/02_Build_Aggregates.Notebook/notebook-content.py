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

daily = spark.table("claims_daily")
daily.groupBy("policy_no").count().write.saveAsTable("claims_agg")
