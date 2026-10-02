# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# <u>_**Attach Lakehouse**_</u>

# CELL ********************

default_lakehouse ="storage"
default_lakehouse_workspace_id = ""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import time
import json
import base64
 
def get_notebook_content(notebook_id_or_name):
    nb = notebookutils.notebook.get(notebook_id_or_name)
    workspaceId = nb['workspaceId']
    notebookId = nb['id']
    format = 'ipynb'
    headers = {
        "Authorization": "Bearer " + notebookutils.credentials.getToken("pbi")
    }
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{notebookId}/getDefinition?format={format}"
    response = requests.post(url, headers=headers)
 
    if response.status_code == 202:
        retry_after = int(response.headers.get("Retry-After", 5))
        location = response.headers.get("Location")
        for _ in range(3):
            time.sleep(retry_after)
            resp = requests.get(f"{location}/result", headers=headers)
            if resp.status_code == 200:
                return resp.text
    elif response.status_code == 200:
        return response.text
    raise Exception("get notebook context failed.")
 
def update_notebook_default_lakehouse(notebook_id_or_name, default_lakehouse, default_lakehouse_workspace_id):
    content = get_notebook_content(notebook_id_or_name)
    payload = json.loads(content)["definition"]["parts"][0]["payload"]
    content = json.loads(base64.b64decode(payload).decode('utf-8'))
    # remove current dependencies
    del content['metadata']['dependencies']
    # print(content)
    return notebookutils.notebook.updateDefinition(notebook_id_or_name, content, default_lakehouse, default_lakehouse_workspace_id)
 
for notebook_id_or_name in ["Process_Data_Every_24_Hours", "Process Data Every 5 Minutes"] :
    try: 
     resp = update_notebook_default_lakehouse(notebook_id_or_name, default_lakehouse, default_lakehouse_workspace_id)
     print(resp)
    except:
     print("attached already")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.run("Process_Data_Every_24_Hours", 2000,{"Nbr_Files_to_Download": 1 })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <u>_**Update PowerBI**_</u>

# CELL ********************

%pip -q install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
from sempy_labs import migration, report, directlake
from sempy_labs import lakehouse as lake
from sempy_labs.tom import connect_semantic_model

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report.report_rebind_all("Semantic_Model", "Semantic_Model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

labs.directlake.update_direct_lake_model_lakehouse_connection("Semantic_Model", lakehouse = "storage")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

labs.refresh_semantic_model("Semantic_Model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
