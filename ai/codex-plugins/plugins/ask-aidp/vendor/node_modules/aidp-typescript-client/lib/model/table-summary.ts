// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information of table in the schema.
*/
export interface TableSummary {
    /**
    * The fully qualified name of the table in the format <catalog_name>.<schema_name>.<table_name>.
    */
    'key'?: string;
    /**
    * Table name.
    */
    'displayName'?: string;
    /**
    * Type of table. Managed, external or mount table.
    */
    'tableType'?: model.TableType;
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

   "entityType": string;
}

export namespace TableSummary {









    export function getJsonObj(obj: TableSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "STANDARD":
                    return model.StandardTableSummary.getJsonObj(<model.StandardTableSummary>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhTableSummary.getJsonObj(<model.AlhTableSummary>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataTableSummary.getJsonObj(<model.ExadataTableSummary>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacTableSummary.getJsonObj(<model.OacTableSummary>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwTableSummary.getJsonObj(<model.AdwTableSummary>(<object>jsonObj), true);
                case "ORACLE":
                    return model.OracleTableSummary.getJsonObj(<model.OracleTableSummary>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpTableSummary.getJsonObj(<model.AtpTableSummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TableSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "STANDARD":
                    return model.StandardTableSummary.getDeserializedJsonObj(<model.StandardTableSummary>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhTableSummary.getDeserializedJsonObj(<model.AlhTableSummary>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataTableSummary.getDeserializedJsonObj(<model.ExadataTableSummary>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacTableSummary.getDeserializedJsonObj(<model.OacTableSummary>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwTableSummary.getDeserializedJsonObj(<model.AdwTableSummary>(<object>jsonObj), true);
                case "ORACLE":
                    return model.OracleTableSummary.getDeserializedJsonObj(<model.OracleTableSummary>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpTableSummary.getDeserializedJsonObj(<model.AtpTableSummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)
        }
        }
        return jsonObj;
    }
}
