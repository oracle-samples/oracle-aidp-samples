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

%pip install ms-fabric-cli --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************



token = notebookutils.credentials.getToken('pbi')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab ls -la

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab:/FabConEU2025 Test Workspace.Workspace$ exists .managedidentities/ws1.ManagedIdentity

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab ls -la /FabConEU2025 Test Workspace.Workspace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab ls /FabConEU2025 Dev Workspace.Workspace -q managedidentities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab get /FabConEU2025 Dev Workspace.Workspace -q sparkSettings

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab ls /FabConEU2025 Dev Workspace.Workspace .managedidentities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab ls .managedidentities -l /FabConEU2025 Dev Workspace.Workspace

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab -c "cd /FabConEU2025 Dev Workspace.workspace/.managedidentities" || echo "Failed to navigate"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!fab -c "exists .managedidentities/ws1.ManagedIdentity" || echo "Failed to navigate"

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
