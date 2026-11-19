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

%%sql
SELECT policy_no, count(*) AS n
FROM dbo.claim
GROUP BY policy_no
