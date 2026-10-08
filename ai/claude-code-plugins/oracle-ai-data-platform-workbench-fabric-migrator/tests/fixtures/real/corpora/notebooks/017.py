# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%pip install ms-fabric-cli --quiet
%pip install azure.identity --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

workspace = "FabConEU2025 Setup Workspace"
subscription_id = "aa66a6eb-7a02-47cf-a392-e71fe6c2c203"
resource_group = "FabConVienna2025"
storage_account = "fabconvienna2025sa"

# Key Vault URI
key_vault_uri = f"https://fabconwskv.vault.azure.net/"

# Key Vault secret name with the tenant id
key_vault_tenant_id = f"tenant-id"

# Key vault secret name with the App Id of the Service Principal
key_vault_client_id = f"app-id"

# Key vault secret name with the secret of the Service Principal
key_vault_client_secret = f"secret"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import subprocess
import os
import json
import requests
import zipfile
from io import BytesIO
import re
import time
from azure.identity import ClientSecretCredential
import requests
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

##### DO NET CHANGE UNLESS SPECIFIED OTHERWISE ####
repo_owner = "ecotte" 
repo_name = "Fabric-Monitoring-RTI" 
branch = "main"
folder_prefix = "" 
github_token = ""
###################################################

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def download_folder_as_zip(repo_owner, repo_name, output_zip, branch="main", folder_to_extract="src",  remove_folder_prefix = "", github_token = None):
        # Construct the URL for the GitHub API to download the repository as a zip file
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/zipball/{branch}"
        headers = None

        if github_token is not None and github_token != "":
        # Replace with your actual GitHub token
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        folder_to_extract = f"/{folder_to_extract}" if folder_to_extract[0] != "/" else folder_to_extract
        
        # Ensure the directory for the output zip file exists
        os.makedirs(os.path.dirname(output_zip), exist_ok=True)
        
        # Create a zip file in memory
        with zipfile.ZipFile(BytesIO(response.content)) as zipf:
            with zipfile.ZipFile(output_zip, 'w') as output_zipf:
                for file_info in zipf.infolist():
                    parts = file_info.filename.split('/')
                    if  re.sub(r'^.*?/', '/', file_info.filename).startswith(folder_to_extract): 
                        # Extract only the specified folder
                        file_data = zipf.read(file_info.filename)  
                        if folder_prefix != "":
                            parts.remove(remove_folder_prefix)
                        output_zipf.writestr(('/'.join(parts[1:])), file_data)

def uncompress_zip_to_folder(zip_path, extract_to):
    # Ensure the directory for extraction exists
    os.makedirs(extract_to, exist_ok=True)
    
    # Uncompress all files from the zip into the specified folder
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    
    # Delete the original zip file
    os.remove(zip_path)

def run_fab_command(command, capture_output: bool = False, silently_continue: bool = False, raw_output: bool = False):
        result = subprocess.run(["fab", "-c", command], capture_output=capture_output, text=True)
        if (not(silently_continue) and (result.returncode > 0 or result.stderr)):
            raise Exception(f"Error running fab command. exit_code: '{result.returncode}'; stderr: '{result}'")    
        if (capture_output and not raw_output): 
            output = result.stdout.strip()
            return output
        elif (capture_output and raw_output):
            return result

def fab_get_workspace_id(name):
    result = run_fab_command(f"get /{name} -q id" , capture_output = True, silently_continue= True)
    return result

def fab_get_workspace_identity(name):
    result = run_fab_command(f"get /{name} -q workspaceIdentity" , capture_output = True, silently_continue= True)
    return result

def fab_create_folder(workspace,display_name):
    folder = {
        "displayName": display_name
    }

    workspace_id = fab_get_workspace_id(f"{workspace}.Workspace")

    folderResponse = run_fab_command(f"api -X post /workspaces/{workspace_id}/folders/ -i {json.dumps(folder)}" , capture_output = True, silently_continue= True)

    jsonResponse = json.loads(folderResponse)

    return jsonResponse.get("text",{}).get("id")

def fab_create_item(workspace,item_name,item_type,folder_id):
    item = {
        "displayName": item_name,
        "type": item_type,
        "folderId": folder_id
    }

    workspace_id = fab_get_workspace_id(f"{workspace}.Workspace")

    itemResponse = run_fab_command(f"api -X post /workspaces/{workspace_id}/items/ -i {json.dumps(item)}" , capture_output = True, silently_continue= True)

    return itemResponse

def fab_provision_identity(workspace):

    workspace_id = fab_get_workspace_id(f"{workspace}.Workspace")

    response = run_fab_command(f"create /{workspace}.Workspace/.managedidentities/{workspace}.ManagedIdentity" , capture_output = True, silently_continue= True)

    return json.loads(fab_get_workspace_identity(name=f"{workspace}.Workspace"))

def storage_account_assign_role():
    
    # Azure AD tenant ID, client ID, and client secret of the service principal with permission to assign roles
    client_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
    client_secret = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_secret)
    tenant_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
    
    # The service principal ID (objectId) to which Storage Blob Data Reader role will be assigned
    target_sp_object_id = ws_identity.get("servicePrincipalId")
    
    # Azure subscription ID and scope for role assignment (e.g. a storage account resource ID)
    scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}"
    
    # Role definition ID for Storage Blob Data Reader
    # This is a built-in role and its ID is usually: "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
    role_definition_id = f"/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
    
    # Authenticate with the service principal to get token for Azure Resource Manager
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    token = credential.get_token("https://management.azure.com/.default").token
    
    # REST API endpoint for role assignments - generate a GUID for roleAssignmentId
    import uuid
    role_assignment_id = str(uuid.uuid4())
    role_assignment_url = (
        f"https://management.azure.com/{scope}/providers/Microsoft.Authorization/roleAssignments/{role_assignment_id}?api-version=2022-04-01"
    )
    
    # Prepare the payload for role assignment
    payload = {
        "properties": {
            "roleDefinitionId": role_definition_id,
            "principalId": target_sp_object_id
        }
    }
    
    # Prepare headers with authorization token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Make the PUT request to create the role assignment for the target service principal
    response = requests.put(role_assignment_url, headers=headers, json=payload)
    
    if response.status_code == 201 or response.status_code == 200:
        print("Role assignment created successfully.")
    else:
        print(f"Failed to create role assignment: {response.status_code} - {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Set environment parameters for Fabric CLI
token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

download_folder_as_zip(repo_owner, repo_name, output_zip = "./builtin/config/config.zip", branch = branch, folder_to_extract= f"{folder_prefix}/config" , remove_folder_prefix = f"{folder_prefix}", github_token=github_token)
uncompress_zip_to_folder(zip_path = "./builtin/config/config.zip", extract_to= "./builtin")

base_path = './builtin/'

deploy_order_path = os.path.join(base_path, 'config/deployment_order.json')
with open(deploy_order_path, 'r') as file:
        deployment_order = json.load(file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

print(workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

folder = "Lab 01"
item = ""

# fab_create_folder(workspace=workspace, display_name=workspace)
# fab_create_item(workspace=workspace,item_name="Test01",item_type="Eventstream", folder_id=folder_id)

fab_provision_identity(workspace=workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

for item in deployment_order:
    print(item.get("name"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

identity = json.loads(fab_get_workspace_identity(name=f"{workspace}.Workspace"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

ws_identity = fab_provision_identity(workspace)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

body

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!pip install azure.identity --quiet
!pip install azure.mgmt.storage --quiet
!pip install azure.mgmt.authorization --quiet


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

ws_identity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from azure.identity import ClientSecretCredential
import requests
import json
 
# Azure AD tenant ID, client ID, and client secret of the service principal with permission to assign roles
client_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_id)
client_secret = notebookutils.credentials.getSecret(key_vault_uri, key_vault_client_secret)
tenant_id = notebookutils.credentials.getSecret(key_vault_uri, key_vault_tenant_id)
 
# The service principal ID (objectId) to which Storage Blob Data Reader role will be assigned
target_sp_object_id = ws_identity.get("servicePrincipalId")
 
# Azure subscription ID and scope for role assignment (e.g. a storage account resource ID)
scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account}"
 
# Role definition ID for Storage Blob Data Reader
# This is a built-in role and its ID is usually: "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
role_definition_id = f"/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1"
 
# Authenticate with the service principal to get token for Azure Resource Manager
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
token = credential.get_token("https://management.azure.com/.default").token
 
# REST API endpoint for role assignments - generate a GUID for roleAssignmentId
import uuid
role_assignment_id = str(uuid.uuid4())
role_assignment_url = (
    f"https://management.azure.com/{scope}/providers/Microsoft.Authorization/roleAssignments/{role_assignment_id}?api-version=2022-04-01"
)
 
# Prepare the payload for role assignment
payload = {
    "properties": {
        "roleDefinitionId": role_definition_id,
        "principalId": target_sp_object_id
    }
}
 
# Prepare headers with authorization token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
 
# Make the PUT request to create the role assignment for the target service principal
response = requests.put(role_assignment_url, headers=headers, json=payload)
 
if response.status_code == 201 or response.status_code == 200:
    print("Role assignment created successfully.")
else:
    print(f"Failed to create role assignment: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
