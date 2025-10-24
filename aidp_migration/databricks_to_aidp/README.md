# Databricks to AIDP migration utility

This utilities provides a sample implementation for exporting files and notebooks from a Databricks workspace to Oracle AI Data Platform (AIDP). It preserves the folder structure and converts notebooks to .ipynb format while supporting optional string replacement during the export process


## Running the Samples

Before running the notebook, replace the following placeholders with your environment-specific values:
Required Parameters

DATABRICKS_WORKSPACE_URL: Your Databricks workspace URL

DATABRICKS_TOKEN: Your Databricks personal access token

DATABRICKS_PATH: Source path in Databricks workspace to export

AIDP_PATH: Target directory path in AIDP

dbx_to_aidp_replacement_mappings: Optional String Replacement mappings if you need to modify content during export:

## Documentation

### Recursive Export
Traverses nested directory structures in Databricks workspace
### Format Preservation
Exports notebooks as Jupyter .ipynb files
### String Replacement
Supports source-to-target string mapping during export
### Structure Maintenance
Recreates the original folder hierarchy in AIDP
### Multiple File Types
Handles both notebooks and regular files

## Get Support


## Security

Please consult the [security guide](/SECURITY.md) for our responsible security vulnerability disclosure process.

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](/CONTRIBUTING.md).

## License

See [LICENSE](/LICENSE.txt)


