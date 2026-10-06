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

# # Fabric Library Manager
# This notebook allows you to specify a set of notebooks from the current workspace to be bundled into a library. This library is stored in the `/Files` section of a lakehouse. Additionally a new notebook is created in the workspace called `load_LibraryManager` which you can call from your other notebooks using `%run` to load the LibraryManager and all its libraries into the current notebook session.
# 
# If you change anything in the library notebooks, simply run this LibraryManager notebook again to also update the actual library thats imported in all notebooks.

# CELL ********************

library_notebooks = [
    {
        "notebook": "MyLibrary",
        "library_name": "MyLibrary.py"
    },
    {
        "notebook": "MyOtherLibrary",
        "library_name": "MyOtherLibrary.py"
    },
    {
        "notebook": "MyFabricSparkLibrary",
        "library_name": "MyFabricSparkLibrary.py"
    },
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import base64
import zipfile
import time
import json
import requests

import notebookutils
from sempy.fabric import FabricRestClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Global Settings

# CELL ********************

RUNTIME_CONTEXT = {k:v for k,v in notebookutils.runtime.context.items() if v is not None}

LIBRARY_FOLDER = "/Libraries" # must start with "/"
LIBRARY_NAME = "LibraryManager"
LIBRARY_LAKEHOUSE_NAME = "SomeLakehouse"
LIBRARY_IMPORT = f"from {LIBRARY_NAME} import *" # could also be a named import
# LIBRARY_IMPORT = f"import {LIBRARY_NAME} as lm"
LIBRARY_LOAD_NOTEBOOK_NAME = f"load_{LIBRARY_NAME}"

LIBRARY_LAKEHOUSE = notebookutils.lakehouse.get(LIBRARY_LAKEHOUSE_NAME)

assert LIBRARY_FOLDER.startswith("/"), "LIBRARY_FOLDER must start with '/'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

REST_CLIENT = FabricRestClient()
ALL_NOTEBOOKS = REST_CLIENT.get_paged(f"v1/workspaces/{RUNTIME_CONTEXT['currentWorkspaceId']}/notebooks")

# we leverage the REST_CLIENT to get basic parameters for further requests
dummy_call = REST_CLIENT.get(f"v1/workspaces")
BASE_URL = dummy_call.url[:-13]
HEADERS = dummy_call.request.headers

invalid_libs = [lib for lib in library_notebooks if not lib["notebook"] in [nb["displayName"] for nb in ALL_NOTEBOOKS]]
assert invalid_libs == [], f"The following Library Notebooks could not be found: {invalid_libs}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # api_request 
# While we in general rely on SemPy library, there are some things that do not work for us or needed to be improved
# 
# - you cannot run a POST request with a body using SemPy 
# - the default delay for Long-Running-Operations (LRO) is 20 seconds, which is way too long to simply get the definition of a notebook

# CELL ********************

def api_request(method: str, api_path: str, body: dict = None, interval: int = 1):
    if body != None:
        if method.upper() == "POST":
            response = requests.post(BASE_URL + api_path, json = body, headers = HEADERS)
        else:
            raise Exception("Only method POST is supported with a body")
    else:
        response = REST_CLIENT.request(method, api_path)

    if response.status_code == 202:
        lro_path = response.headers["Location"]

        while lro_path:
            time.sleep(interval) # lower interval than the original sempy request with LRO_wait = True
            response = REST_CLIENT.request("GET", lro_path)
            lro_path = response.headers.get("Location")
            
            if lro_path and lro_path.endswith("/result"):
                response = REST_CLIENT.request("GET", lro_path)
                return response.json()            

    return response.json()

if False:
    method = "POST"
    api_path = f"v1/workspaces/{RUNTIME_CONTEXT['currentWorkspaceId']}/items/{RUNTIME_CONTEXT['currentNotebookId']}/getDefinition?format=fabricGitSource"
    definition = api_request(method, api_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # notebook_to_library
# 
# Downloads a notebook from the current workspace as `.py` file and stores it in the Lakehouse. From there we create the library where we add all individual libraries to a `.zip` file which is used and imported in the end.

# CELL ********************

def notebook_to_library(notebook: str, library_name: str = None):
    if not library_name: library_name = f"{notebook}.py"
    assert library_name.endswith(".py"), "library_name must end with '.py'"

    library_path = f"{LIBRARY_LAKEHOUSE['properties']['abfsPath']}/Files{LIBRARY_FOLDER}/{library_name}"

    print(f"Uploading library '{library_name}' to {library_path} ... ", end = "")

    library_notebook = [nb for nb in ALL_NOTEBOOKS if nb["displayName"] == notebook][0]
    notebook_id = library_notebook["id"]

    notebook_definition = api_request("POST", f"v1/workspaces/{RUNTIME_CONTEXT['currentWorkspaceId']}/items/{notebook_id}/getDefinition?format=fabricGitSource")
    notebook_part = [part for part in notebook_definition["definition"]["parts"] if part["path"].startswith("notebook-content")][0]

    try:
        file_content = base64.b64decode(notebook_part["payload"]).decode("utf-8")
        mssparkutils.fs.put(library_path, file_content, True)
        print("Done!")
    except Exception as e:
        print("ERROR!")
        print(str(e))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Update all know libraries

# CELL ********************

for library_notebook in library_notebooks:
    notebook_to_library(**library_notebook)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write final library as ZIP-file

# CELL ********************

# write CustomLibrary.zip
libraries_local_path = f"/lakehouse/default/Files{LIBRARY_FOLDER}"

libraries = [lib["library_name"] for lib in library_notebooks]

try:
    os.remove(f"{libraries_local_path}/{LIBRARY_NAME}.zip")
except OSError:
    pass


print(f"Writing __init__.py file ... ", end = "")
with open(f"{libraries_local_path}/__init__.py", "w") as init_file:
    init_file.writelines([f"from .{lib[:-3]} import *\n" for lib in libraries if lib != "__init__.py"])
print("Done!")

print(f"Creating new archive at {libraries_local_path}/{LIBRARY_NAME}.zip ... ")
with zipfile.ZipFile(f"{libraries_local_path}/{LIBRARY_NAME}.zip", "w", zipfile.ZIP_DEFLATED) as myzip:
    libs_to_add = libraries + ["__init__.py"]
    for lib in libs_to_add:
        print(f"\tAdding '{lib}' to archive ... ", end = "")
        myzip.write(f"{libraries_local_path}/{lib}", f"/{LIBRARY_NAME}/{lib}")
        print("Done!")

print("Done!")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write the notebook to initialize the LibraryManager

# CELL ********************


init_script = f"""# Fabric notebook source

# CELL ********************

library_path = "{LIBRARY_LAKEHOUSE['properties']['abfsPath']}/Files{LIBRARY_FOLDER}/{LIBRARY_NAME}.zip"
print(f"Loading {LIBRARY_NAME} from '{{library_path}}' ... ", end = "")
sc.addPyFile(library_path)
print("Done!")

{LIBRARY_IMPORT}
"""

existing_notebook_id = [nb for nb in ALL_NOTEBOOKS if nb["displayName"] == LIBRARY_LOAD_NOTEBOOK_NAME]

if existing_notebook_id:
    api_path = f"v1/workspaces/{RUNTIME_CONTEXT['currentWorkspaceId']}/items/{existing_notebook_id[0]['id']}/updateDefinition"
    body = {}
else:
    api_path = f"v1/workspaces/{RUNTIME_CONTEXT['currentWorkspaceId']}/notebooks"
    body = {
    "displayName": LIBRARY_LOAD_NOTEBOOK_NAME,
    "description": "A notebook that can be run to load the LibraryManager via run"
    }

definition = {
    "format": "fabricGitSource",
    "parts": [
        {
            "path": "notebook-content.py",
            "payload": base64.b64encode(init_script.encode("utf-8")).decode("utf-8"),
            "payloadType": "InlineBase64"
        }
    ]
}

body["definition"] = definition

result = api_request("POST", api_path, body)

print(f"Successfully created notebook '{LIBRARY_LOAD_NOTEBOOK_NAME}' (ID = {result['id']})!")
print(f"To load the LibraryManager into your notebooks, you can now use the following command in a notebook cell:")
print("═"*80)
print(f"%run {LIBRARY_LOAD_NOTEBOOK_NAME}")
print("═"*80)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
