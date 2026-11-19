# Synapse Analytics notebook source

# METADATA ********************

# META {
# META   "synapse": {
# META     "lakehouse": {
# META       "default_lakehouse": "5dd598b3-0049-4e7a-aca4-8a71ba1ae005",
# META       "default_lakehouse_name": "casadinpadure",
# META       "default_lakehouse_workspace_id": "6b6606be-3275-4558-944e-aac247cc42a4",
# META       "known_lakehouses": [
# META         {
# META           "id": "5dd598b3-0049-4e7a-aca4-8a71ba1ae005"
# META         },
# META         {
# META           "id": "e0ad8bbb-7fef-4aea-8974-29afb80ca048"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "3555fd2b-975d-4c87-bb07-a6fa3c8ac2eb",
# META       "workspaceId": "2ee099f6-da9f-4900-94cf-ffa73f8cbaa1"
# META     }
# META   }
# META }

# CELL ********************

import os
from openai import AzureOpenAI
from notebookutils import mssparkutils
import ast

# CELL ********************

#add data here
AZURE_OPENAI_KEY = ''
AZURE_OPENAI_ENDPOINT = ''

# CELL ********************

client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,  
    api_version="2023-09-01-preview",
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)

# PARAMETERS CELL ********************

user_input = ''

# CELL ********************

data_dicts_list = ast.literal_eval(user_input)

# CELL ********************

dict_output = []
for data_dict in data_dicts_list:
    dataset_name = data_dict['dataset_name']
    prompt = data_dict['prompt']
    response = client.chat.completions.create(
        model="gpt-35-turbo",
        messages=[
            {"role": "system", "content": "You are a skilled Data Scientist."},
            {"role": "user", "content": prompt}
        ]
    )

    #print(response)
    print(response.model_dump_json(indent=2))
    print(response.choices[0].message.content)
    output_response = response.choices[0].message.content
    to_add = {'dataset_name': dataset_name, 'prompt': output_response}
    dict_output.append(to_add)

# CELL ********************

mssparkutils.notebook.exit(dict_output)
