# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2925655f-0293-4f32-8bc6-86ab989099a7",
# META       "default_lakehouse_name": "SomeLakehouse",
# META       "default_lakehouse_workspace_id": "ca0a79b9-9c03-40b5-9d6a-cfd3fda1c31e",
# META       "known_lakehouses": [
# META         {
# META           "id": "2925655f-0293-4f32-8bc6-86ab989099a7"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Using LibraryManager to import custom code

# CELL ********************

%run load_LibraryManager

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Use log() function and PI constant from library

# CELL ********************

log(PI)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Use library function that uses `notebookutils` under the hood

# CELL ********************

display(ls("/"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Use library function that uses Spark under the hood

# CELL ********************

display(table_to_df("myTable"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Use nested library function 

# CELL ********************

print(get_circumference(8))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
