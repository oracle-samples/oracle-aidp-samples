// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information about the table in the schema.
*/
export interface Table {
    /**
    * The fully qualified name of the table in the format <catalog_name>.<schema_name>.<table_name>.
    */
    'key'?: string;
    /**
    * Table name.
    */
    'displayName': string;
    /**
    * The name of the catalog to which this table belongs.
    */
    'catalogKey'?: string;
    /**
    * The name of the schema to which this table belongs.
    */
    'schemaKey'?: string;
    /**
    * Location of the table data.
    */
    'location'?: string;
    /**
    * Table description.
    */
    'description'?: string;
    /**
    * Type of table. Managed, external or mount table.
    */
    'tableType'?: model.TableType;
    'managedTableDefinition'?: model.ManagedTableDefinition;
    'externalTableDefinition'?: model.ExternalTableDefinition;
    /**
    * Columns for table.
    */
    'tableFields'?: Array<model.TableFieldDetails>;
    /**
    * Columns to be used in partition for table.
    */
    'partitionKeys'?: Array<model.TableFieldDetails>;
    /**
    * Table properties.
    */
    'tableProperties'?: Array<model.TableProperty>;
    /**
    * The date and time the table was created.
    */
    'timeCreated'?: Date;
    /**
    * The date and time the table was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The OCID of the user/principal who created the table.
    */
    'createdBy'?: string;
    /**
    * The ID of the user who last updated the schema.
    */
    'updatedBy'?: string;
    /**
    * The state of the table.
    */
    'lifecycleState'?: model.TableLifecycleState;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleStateDetails'?: string;

   "entityType": string;
}

export namespace Table {



















    export function getJsonObj(obj: Table): object {
        const jsonObj = {...obj, ...{
            







                'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getJsonObj(obj.managedTableDefinition) : undefined,
                'externalTableDefinition': obj.externalTableDefinition ?
                
                
                model.ExternalTableDefinition.getJsonObj(obj.externalTableDefinition) : undefined,
                'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getJsonObj(item)})
                
                 : undefined,






        }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ORACLE":
                    return model.OracleTable.getJsonObj(<model.OracleTable>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwTable.getJsonObj(<model.AdwTable>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhTable.getJsonObj(<model.AlhTable>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardTable.getJsonObj(<model.StandardTable>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataTable.getJsonObj(<model.ExadataTable>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpTable.getJsonObj(<model.AtpTable>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacTable.getJsonObj(<model.OacTable>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Table): object {
        const jsonObj = {...obj, ...{
            







                    'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getDeserializedJsonObj(obj.managedTableDefinition) : undefined,
                    'externalTableDefinition': obj.externalTableDefinition ?
                
                
                model.ExternalTableDefinition.getDeserializedJsonObj(obj.externalTableDefinition) : undefined,
                    'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getDeserializedJsonObj(item)})
                
                 : undefined,






         }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ORACLE":
                    return model.OracleTable.getDeserializedJsonObj(<model.OracleTable>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwTable.getDeserializedJsonObj(<model.AdwTable>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhTable.getDeserializedJsonObj(<model.AlhTable>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardTable.getDeserializedJsonObj(<model.StandardTable>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataTable.getDeserializedJsonObj(<model.ExadataTable>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpTable.getDeserializedJsonObj(<model.AtpTable>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacTable.getDeserializedJsonObj(<model.OacTable>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)
        }
        }
        return jsonObj;
    }
}
