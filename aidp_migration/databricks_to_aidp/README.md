# Databricks to AIDP migration utility

Utility to Export Databricks files(non-data) and notebooks to Oracle AI Data Platform (AIDP)
* Preserves folder structure
* Converts notebooks to .ipynb
* No code translation; files are moved as-is
* Optional plain string replacement from a provided mapping. Replacement is simple find/replace (no parsing)
* This is not intended for Data Files.

Run this notebook from AIDP. The user must have read permission on Databricks Path and write permission on the AIDP destination path.

## Running the Samples

Before running the notebook, replace the following placeholders with your environment-specific values:
Required Parameters

DATABRICKS_WORKSPACE_URL: Your Databricks workspace URL

DATABRICKS_TOKEN: Your Databricks personal access token

DATABRICKS_PATH: Source path in Databricks workspace to export

AIDP_PATH: Target directory path in AIDP

dbx_to_aidp_replacement_mappings: Optional string-replacement map used during export. Basic example could be to rewrite path prefixes of referenced files/notebooks. 

## Documentation

### Recursive Export
Traverses nested directory structures in Databricks workspace
### Format Preservation
Exports notebooks as Jupyter(.ipynb) and other files as is. 
### String Replacement
Supports source-to-target string mapping during export
### Structure Maintenance
Recreates the original folder hierarchy in AIDP
### Multiple File Types
Handles both notebooks and regular files.
### No Code Conversion
It does not do any code conversion.
### Permissions
Need read permission on databricks and write permission on AIDP.

## Get Support


## Security

Please consult the [security guide](/SECURITY.md) for our responsible security vulnerability disclosure process.

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](/CONTRIBUTING.md).

## License

See [LICENSE](/LICENSE.txt)


