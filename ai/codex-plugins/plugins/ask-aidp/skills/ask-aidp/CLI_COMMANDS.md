# AIDP CLI Command Catalog

Generated from https://github.com/oracle-samples/aidataplatform-sdk/blob/main/docs/cli/README.md.

This plugin supports all 215 documented AIDP CLI commands through `aidp_cli`, with lookup support through `aidp_cli_reference`.

## Async Operations (async-operations)
Async operations.

- `aidp async-operations get` - Get detailed information for a particular async operation. Get detailed information for a particular async operation
- `aidp async-operations list` - List all async operations for a resource type. List all async operations for a resource type. Filters can be used to narrow the search down.

## Audit (audit)
Audit logs.

- `aidp audit manage-logs` - Manages audit logs for AI Data Platform Workbench.
- `aidp audit search-logs` - Searches audit logs for AI Data Platform Workbench.

## Bundle (bundle)
Bundles, bundle deployment status, and sync bundles.

- `aidp bundle create` - (Preview) Creates a new bundle. (Preview) Creates a new bundle. A bundle is a self-contained, portable representation of selected workspace assets, such as jobs and agent flows, along with their dependencies and associated code artifacts. It captures both the resource configurations and the supporting assets required to recreate those resources in another workspace or environment. The bundle manifest is named aidp_workbench.yaml. The bundle preserves the workspace folder structure for code artifacts from the location where it was created, so the generated bundle mirrors the source layout. Depe
- `aidp bundle deploy` - (Preview) Deploys the specified bundle, creating or updating jobs and agent flows according to the bundle manifest. (Preview) Deploys the specified bundle, creating or updating jobs and agent flows according to the bundle manifest. Returns an async job key for tracking deployment progress. This operation is asynchronous. The request is accepted for background execution and returns an async operation key in the response headers. Deployment typically uses: - the bundle manifest at the bundle root - top-level resource descriptors in the bundle - dependency descriptors referenced by those top-leve
- `aidp bundle fetch-deployment-status` - (Preview) Returns a high-level summary of the most recent deployment activity recorded for the specified bundle. (Preview) Returns a high-level summary of the most recent deployment activity recorded for the specified bundle. This operation is useful after deploy or purge requests when you want the latest bundle-level deployment outcome rather than raw async operation details. The response can include: - overall deployment status - start and completion timestamps - summary message - resources associated with the last recorded deployment result Typical status values include: - IN_PROGRESS - SUC
- `aidp bundle purge` - (Preview) Tears down all resources deployed by the specified bundle in the workspace. (Preview) Tears down all resources deployed by the specified bundle in the workspace. This operation is intended to tear down resources that were created or managed through bundle deployment. It does not delete the bundle files themselves from the workspace volume. This operation is asynchronous. The service accepts the purge request, starts the background teardown workflow, and returns async operation headers. Typical use cases: - remove resources that were previously deployed from a bundle - clean up a work
- `aidp bundle sync-bundle` - (Preview) Synchronizes the code, descriptors, and mapping in the bundle by reconciling the contents with the resource origins. (Preview) Synchronizes the code, descriptors, and mapping in the bundle by reconciling the contents with the resource origins. Returns an async job key for tracking sync progress. This operation is intended for cases where the bundle should be refreshed to reflect newer source changes while preserving the bundle structure and identity. Sync uses the bundle's recorded origin metadata to rebuild the bundle from the source jobs and agent flows that were captured when the

## Catalog (catalog)
Catalogs, catalog permissions, and connections.

- `aidp catalog create` - Create a catalog in the AI Data Platform Workbench with the given ID.
- `aidp catalog delete` - Deletes the specified catalog from an AI Data Platform Workbench.
- `aidp catalog get` - Gets detailed information about an AI Data Platform Workbench catalog with a given catalog key.
- `aidp catalog list` - Get a list of catalogs in an AI Data Platform Workbench with a given ID.
- `aidp catalog list-permissions` - Gets a list of all permissions in the specified catalog of an AI Data Platform Workbench.
- `aidp catalog manage-permission` - Update permission details for a catalog in an AI Data Platform Workbench.
- `aidp catalog refresh` - Refresh a catalog in an AI Data Platform Workbench through a crawler.
- `aidp catalog test-connection` - Test the connection of an AI Data Platform Workbench to an external catalog.
- `aidp catalog update` - Update the details of an AI Data Platform Workbench catalog with the given information.

## Cluster (cluster)
Clusters, cluster logs, cluster libraries, and cluster permissions.

- `aidp cluster create` - Creates a new cluster with the provided details.
- `aidp cluster delete` - Deletes a cluster from a workspace.
- `aidp cluster download-logs` - Downloads logs within the specified cluster and time range. Downloads logs within the specified cluster and time range. The logs can be filtered by severity (logLevel), type (logContentTypeContains), and other parameters such as execution context and thread identifiers.
- `aidp cluster get` - Returns detailed information about a cluster.
- `aidp cluster get-default` - Gets information about the master catalog default cluster.
- `aidp cluster list` - Returns a list of all clusters in a given workspace.
- `aidp cluster list-libraries` - Gets a list of libraries installed on a cluster.
- `aidp cluster list-permissions` - Return a list of permissions for a given cluster.
- `aidp cluster manage-permission` - Updates the permissions for a given cluster.
- `aidp cluster patch-library` - Updates libraries of a cluster with the provided patches.
- `aidp cluster restart` - Restarts a running cluster.
- `aidp cluster search-logs` - Searches logs within the specified cluster and time range. Searches logs within the specified cluster and time range. Supports pagination and filtering.
- `aidp cluster start` - Starts a cluster that has halted operation.
- `aidp cluster stop` - Stops an active cluster.
- `aidp cluster summarize-metrics-data` - Provides summarized compute metrics for a compute cluster in the given workspace. Provides summarized compute metrics for a compute cluster in the given workspace. This API aggregates metric data points based on a specified namespace, metric name, and aggregation type. The response contains computed metric summaries.
- `aidp cluster update` - Update the details of a given cluster.

## Credentials (credentials)
Credentials.

- `aidp credentials create` - Creates a new credential object with the provided details. Creates a new credential object with the provided details. The operation completes synchronously; callers can invoke list or get to retrieve the resource payload.
- `aidp credentials delete` - Deletes a credential object. Deletes a credential object. The operation completes synchronously without a response body.
- `aidp credentials get` - Gets detailed information about credential with a given credential key.
- `aidp credentials list` - Returns a list of credentials.
- `aidp credentials update` - Updates a credential object. Updates a credential object. The operation completes synchronously; callers can invoke get to confirm the latest state.

## Delta Share (delta-share)
Recipients, shares, recipient permissions, and share data assets.

- `aidp delta-share create` - Create a Delta Share protocol in AI Data Platform Workbench.
- `aidp delta-share create-recipient` - Creates a recipient for a Delta Share protocol in AI Data Platform Workbench.
- `aidp delta-share delete` - Deletes a Delta Share from an AI Data Platform Workbench.
- `aidp delta-share delete-recipient` - Deletes a Delta Share recipient from an AI Data Platform Workbench.
- `aidp delta-share get` - Gets detailed information about a Delta Share.
- `aidp delta-share get-recipient` - Gets detailed information about a Delta Share recipient in an AI Data Platform Workbench instance.
- `aidp delta-share list` - Gets a list of Delta Shares in an AI Data Platform Workbench instance.
- `aidp delta-share list-data-assets` - Gets a list of Delta Shares assets in an AI Data Platform Workbench instance.
- `aidp delta-share list-permissions` - Returns a list of Delta Shares that the specified recipient has been granted access to.
- `aidp delta-share list-recipient-permissions` - Gets a detailed list of Delta Share recipient permissions.
- `aidp delta-share list-recipient-shares` - Returns a list of Delta Shares that the specified recipient has been granted access to.
- `aidp delta-share list-recipients` - Gets a list of Delta Share recipients in a AI Data Platform Workbench instance.
- `aidp delta-share list-share-recipients` - Gets a list of recipients that have been given access on the specified Delta Share.
- `aidp delta-share manage-access` - Updates consumer-side access on a share for a recipient. Updates consumer-side access on a share for a recipient. A provider user can grant or revoke access on a particular share for a given recipient.
- `aidp delta-share manage-data-asset` - Updates data assets on a Delta Share with the provided information.
- `aidp delta-share manage-permission` - Updates permissions on a Delta Share.
- `aidp delta-share manage-recipient-permission` - Updates the permissions of a Delta Share recipient in AI Data Platform Workbench.
- `aidp delta-share update` - Update a Delta Share with the provided metadata.
- `aidp delta-share update-recipient` - Updates the metadata of a Delta Share recipient in a AI Data Platform Workbench instance.

## ML Ops (mlops)
Experiments, experiment runs, registered models, and model versions.

- `aidp mlops create-experiment` - (Preview) Creates an experiment in a workspace.
- `aidp mlops create-experiment-run` - (Preview) Creates a new run within an experiment.
- `aidp mlops create-model-version` - (Preview) Creates a model version.
- `aidp mlops create-registered-model` - (Preview) Creates a registered model in a workspace.
- `aidp mlops create-workspace-model-version` - (Preview) Creates a new model version in a specified workspace.
- `aidp mlops delete-experiment` - (Preview) Deletes an experiment.
- `aidp mlops delete-experiment-run` - (Preview) Deletes an experiment run.
- `aidp mlops delete-experiment-run-tag` - (Preview) Deletes a tag on an experiment run.
- `aidp mlops delete-experiment-tag` - (Preview) Deletes a tag on an experiment.
- `aidp mlops delete-model-version` - (Preview) Deletes a model version.
- `aidp mlops delete-model-version-tag` - (Preview) Deletes a tag on a model version.
- `aidp mlops delete-registered-model` - (Preview) Deletes a registered model.
- `aidp mlops delete-registered-model-tag` - (Preview) Deletes a tag on a registered model.
- `aidp mlops get-experiment-by-id` - (Preview) Returns metadata for an experiment by ID. (Preview) Returns metadata for an experiment by ID. This method works on deleted experiments.
- `aidp mlops get-experiment-by-name` - (Preview) Returns experiment metadata for a given name. (Preview) Returns experiment metadata for a given name. Returns deleted experiments, but prefers the active experiment if an active and deleted experiment share the same name. If multiple deleted experiments share the same name, the API will return one of them.
- `aidp mlops get-experiment-run-by-id` - (Preview) Returns details of an experiment run by ID.
- `aidp mlops get-experiment-run-metric-history` - (Preview) Returns a history of experiment run metrics.
- `aidp mlops get-model-version` - (Preview) Returns detailed information for a model version.
- `aidp mlops get-registered-model` - (Preview) Returns details for a specified registered model.
- `aidp mlops list-artifacts` - (Preview) Returns a list of artifacts.
- `aidp mlops list-experiment-runs` - (Preview) Returns a list of experiment runs in a workspace.
- `aidp mlops list-experiments` - (Preview) Returns a list of experiments with the given details.
- `aidp mlops list-logged-models` - (Preview) Returns a list of logged models.
- `aidp mlops list-model-versions` - (Preview) Returns a list of model versions.
- `aidp mlops list-registered-models` - (Preview) Returns a list of registered models in a workspace.
- `aidp mlops log-experiment-run-batch` - (Preview) Logs an experiment run batch.
- `aidp mlops log-experiment-run-inputs` - (Preview) Logs experiment run inputs.
- `aidp mlops log-experiment-run-metric` - (Preview) Logs an experiment run metric.
- `aidp mlops log-experiment-run-model` - (Preview) Logs an experiment run model.
- `aidp mlops log-experiment-run-param` - (Preview) Logs an experiment run parameter.
- `aidp mlops rename-registered-model` - (Preview) Renames a registered model.
- `aidp mlops restore-experiment` - (Preview) Restores an experiment.
- `aidp mlops restore-experiment-run` - (Preview) Restores an experiment run.
- `aidp mlops set-experiment-run-tag` - (Preview) Sets a tag on an experiment run.
- `aidp mlops set-experiment-tag` - (Preview) Sets a tag on an experiment.
- `aidp mlops set-model-version-tag` - (Preview) Sets a tag on a model version.
- `aidp mlops set-registered-model-tag` - (Preview) Sets a tag on a registered model.
- `aidp mlops transition-model-version-stage` - (Preview) Transitions a model version stage.
- `aidp mlops update-experiment` - (Preview) Updates an experiment.
- `aidp mlops update-experiment-run` - (Preview) Updates an experiment run.
- `aidp mlops update-experiment-run-tags` - (Preview) Updates tags on an experiment run.
- `aidp mlops update-experiment-tags` - (Preview) Updates tags on experiment.
- `aidp mlops update-model-version` - (Preview) Updates a model version. (Preview) Updates a model version
- `aidp mlops update-model-version-tags` - (Preview) Updates tags on a model version.
- `aidp mlops update-registered-model` - (Preview) Updates a registered model with the provided details.
- `aidp mlops update-registered-model-tags` - (Preview) Updates tags on a registered model.

## Notebook (notebook)
Content and sessions.

- `aidp notebook create-content` - Creates a new, untitled, empty file or directory, or copies an existing notebook to a specified path. Creates a new, untitled, empty file or directory, or copies an existing notebook to a specified path. For example, a POST call to /api/contents/path with body containing copy_from set to /path/to/OtherNotebook.ipynb creates a new copy of OtherNotebook at the specified path.
- `aidp notebook create-session` - Creates a new session or returns an existing session if a session for the given path already exists.
- `aidp notebook delete-content` - Deletes a notebook file or directory.
- `aidp notebook delete-session` - Delete a session with given session ID.
- `aidp notebook export-contents` - Exports the notebook file contents. Exports the notebook file contents. You can optionally specify HTML or ipynb format through the request payload. If no format is specified, ipynb is used by default.
- `aidp notebook get-content` - Returns a list of contents for a given file or directory. Returns a list of contents for a given file or directory. You can optionally specify a type and/or format argument via URL parameter. When given, the Content service returns a model in the requested type and/or format. If the request cannot be satisfied, for example if type=text is requested, but the file is binary, then the request returns a 400 message and a JSON response with a Reason field identifying the issue. The value of the Reason field is ‘bad format’ or ‘bad type’, depending on what was requested.
- `aidp notebook get-session` - Returns session details for a given session ID.
- `aidp notebook list-sessions` - Returns a list of all available sessions.
- `aidp notebook modify-content` - Renames a file or directory without re-uploading content.
- `aidp notebook patch-session` - Patches a session with a given ID with the provided details. Patches a session with a given ID with the provided details. You can use this to rename a session.
- `aidp notebook update-content` - Updates the contents of an existing notebook with the provided details or saves a new notebook.

## Role (role)
Roles, role members, and role permissions.

- `aidp role add-member` - Assigns a given user/group/principal to a role.
- `aidp role create` - Creates a role.
- `aidp role delete` - Deletes a role.
- `aidp role get` - Returns detailed information about a role.
- `aidp role list` - Returns a list of roles.
- `aidp role list-permissions` - Returns a list of permissions for a given role.
- `aidp role remove-member` - Revoke a role from a given user or group.
- `aidp role update` - Updates a role with the provided information.

## Schema (schema)
Schemas, tables, views, and schema permissions.

- `aidp schema create` - Creates a schema.
- `aidp schema create-data-table` - Creates a managed table with data loaded from a sample file.
- `aidp schema create-table` - Creates a table.
- `aidp schema create-view` - Creates a view.
- `aidp schema delete` - Deletes a schema from an AI Data Platform Workbench.
- `aidp schema delete-table` - Deletes a table from an AI Data Platform Workbench.
- `aidp schema delete-view` - Deletes a view from AI Data Platform Workbench.
- `aidp schema generate-temp-file-upload-target` - Generates a URI for uploading a sample file to a temporary folder in a schema.
- `aidp schema get` - Returns detailed information about a specified schema.
- `aidp schema get-table` - Returns detailed information about a table.
- `aidp schema get-view` - Returns information about a view.
- `aidp schema infer` - Returns details of a table schema from the specified location.
- `aidp schema infer-with-preview` - Returns table schema and data from the specified location.
- `aidp schema list` - Returns a list of schemas in a given AI Data Platform Workbench.
- `aidp schema list-permissions` - Returns a list of permissions for a given schema.
- `aidp schema list-table-permissions` - Returns a list of permissions for a given table.
- `aidp schema list-tables` - Returns a list of tables in a schema.
- `aidp schema list-view-permissions` - Returns a list of view permissions.
- `aidp schema list-views` - Returns a list of views in a schema.
- `aidp schema manage-permission` - Updates the permissions for a given schema.
- `aidp schema manage-table-permission` - Updates the permissions for a given table.
- `aidp schema manage-view-permission` - Updates permissions on a view.
- `aidp schema refresh` - Refreshes schema in an AI Data Platform Workbench through the crawler.
- `aidp schema refresh-table` - Refreshes a table in an AI Data Platform Workbench through the crawler.
- `aidp schema retrieve-par` - Retrieve PAR for the entities created in AI Data Platform Workbench.
- `aidp schema update` - Updates a schema.
- `aidp schema update-table` - Updates a table with provided details.
- `aidp schema update-view` - Updates a view with given information.

## User Setting (user-setting)
User settings.

- `aidp user-setting create` - (Preview) The User Settings API allows you to manage user-specific configurations and credentials within an AI Data Platform instance. (Preview) The User Settings API allows you to manage user-specific configurations and credentials within an AI Data Platform instance. What you can do -> Store user credentials and integrations, including: -> IAM user credentials -> Git account configurations (e.g., GitHub PAT) -> Create and manage multiple settings -> Mark a setting as default for a given type -> Retrieve and filter settings by type or default status Supported setting types -> IAM_USER_CREDENT
- `aidp user-setting delete` - (Preview) Deletes a user setting and its credentials from this AI Data Platform instance, freeing the default slot for that type.
- `aidp user-setting get` - (Preview) Returns the full definition of user settings identified by its key, including type-specific payload and default flag.
- `aidp user-setting list` - (Preview) Returns a list of all user-specific configurations, with filters for setting type, default flag, and pagination when needed.
- `aidp user-setting update` - (Preview) Updates the metadata or payload of an existing user setting, letting you rotate credentials or change defaults.

## Volume (volume)
Files, volumes, directories, and volume permissions.

- `aidp volume create` - Creates a volume in AI Data Platform Workbench.
- `aidp volume delete` - Deletes a volume.
- `aidp volume delete-dir` - Deletes a directory in a volume.
- `aidp volume delete-file` - Deletes a file or folder in a volume.
- `aidp volume download-file` - Downloads a file from a volume.
- `aidp volume download-file-with-par` - provide the par info for downloading the file for given path.
- `aidp volume get` - Returns detailed information about a volume.
- `aidp volume list` - Returns a list of volumes.
- `aidp volume list-files` - Returns a list of files in a volume.
- `aidp volume list-permissions` - Returns a list of volume permissions.
- `aidp volume make-dir` - Creates a directory in a volume.
- `aidp volume manage-permission` - Updates the permissions on a volume.
- `aidp volume update` - Updates a volume with the provided information.
- `aidp volume update-dir` - Updates a directory in volume with the provided information.
- `aidp volume upload-file` - Uploads a file to volume. Uploads a file to volume. If the file already exists, it is updated.
- `aidp volume upload-file-with-par` - Uploads a volume file by generating PAR. Uploads a volume file by generating PAR. If file exists, then it will be updated.

## Workflow (workflow)
Job runs, jobs, task run output, and task runs.

- `aidp workflow cancel-job-run` - Cancels a job run.
- `aidp workflow cancel-job-runs` - Cancels all job runs for a given job.
- `aidp workflow create-job` - Creates a job in an AI Data Platform Workbench.
- `aidp workflow create-job-run` - Creates a job run for an AI Data Platform Workbench.
- `aidp workflow delete-job` - Deletes a job from an AI Data Platform Workbench.
- `aidp workflow delete-job-run` - Deletes a job run from an AI Data Platform Workbench.
- `aidp workflow export-task-run-output` - Exports task run output in HTML or ipynb format.
- `aidp workflow fetch-output` - Fetches the task run output from the runtime engine.
- `aidp workflow get-job` - Returns detailed information about a given job in AI Data Platform Workbench.
- `aidp workflow get-job-run` - Returns detailed information about a given job run.
- `aidp workflow get-task-run` - Returns detailed information about a task run with a given task run key.
- `aidp workflow list-job-permissions` - Returns a list of job permissions.
- `aidp workflow list-job-runs` - Returns a detailed list of job runs in an AI Data Platform Workbench.
- `aidp workflow list-jobs` - Returns a list of jobs for a given AI Data Platform Workbench.
- `aidp workflow list-recent-job-runs` - Returns a list of the latest job runs for a given job key.
- `aidp workflow list-task-runs` - Returns a list of tasks run in an AI Data Platform Workbench.
- `aidp workflow manage-job-permission` - Update job permissions with the provided details.
- `aidp workflow repair-job-run` - Repairs and reruns a job run.
- `aidp workflow update-job` - Update details for a job in AI Data Platform Workbench.

## Workspace (workspace)
Workspaces, workspace permissions, git folders, and workspace status.

- `aidp workspace create` - Creates a workspace.
- `aidp workspace create-git-folder` - Creates a git folder in the workspace. Creates a git folder in the workspace
- `aidp workspace delete` - Deletes a workspace.
- `aidp workspace get` - Gets detailed information about a workspace.
- `aidp workspace list` - Gets a list of workspaces.
- `aidp workspace list-create-permissions` - Gets a list of create workspace permission summary objects.
- `aidp workspace list-permissions` - Gets a list of workspace permissions.
- `aidp workspace manage-create-permission` - Updates create workspace permissions on a workspace.
- `aidp workspace manage-permission` - Updates permissions on a workspace.
- `aidp workspace update` - Updates the details of a workspace.
- `aidp workspace update-async-operation-status` - Updates the status of a workspace.

## Workspace Object (workspace-object)
Workspace objects, workspace files, and workspace object permissions.

- `aidp workspace-object copy` - Copy a workspace object to different location.
- `aidp workspace-object create` - Creates a workspace object. Creates a workspace object. You can create a file or folder in the workspace.
- `aidp workspace-object delete` - Deletes a workspace object.
- `aidp workspace-object download-with-par` - Downloads a workspace file by providing the PAR info for downloading the file for given path.
- `aidp workspace-object get` - Returns detailed information about a workspace object.
- `aidp workspace-object head` - Returns metadata about a workspace object. Returns metadata about a workspace object. The contents of the file are not retrieved.
- `aidp workspace-object list` - Returns a list of objects in the workspace.
- `aidp workspace-object list-permissions` - Returns a list of workspace object permissions.
- `aidp workspace-object manage-permission` - Updates permissions on a workspace object.
- `aidp workspace-object move` - Moves a workspace object to different location.
- `aidp workspace-object rename` - Renames a workspace object.
- `aidp workspace-object update` - Updates a workspace object with the provided information.
- `aidp workspace-object upload-with-par` - Creates a workspace file by generating PAR or updates the metadata by close file. Creates a workspace file by generating PAR or updates the metadata by close file. If file exists, then it will be updated.
