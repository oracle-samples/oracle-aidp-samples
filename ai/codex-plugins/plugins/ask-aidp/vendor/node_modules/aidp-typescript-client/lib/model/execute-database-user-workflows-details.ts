// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Base schema for database user workflow execution. Contains common workflow fields.
*/
export interface ExecuteDatabaseUserWorkflowsDetails {

   "actionType": string;
}

export namespace ExecuteDatabaseUserWorkflowsDetails {

    export function getJsonObj(obj: ExecuteDatabaseUserWorkflowsDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "actionType" in obj && obj.actionType) {
            switch (obj.actionType) {
                case "EXECUTE_SQL_COMMAND":
                    return model.ExecuteSqlCommandDetails.getJsonObj(<model.ExecuteSqlCommandDetails>(<object>jsonObj), true);
                case "DEPROVISION_USER_SCHEMAS":
                    return model.DeProvisionUserSchemasDetails.getJsonObj(<model.DeProvisionUserSchemasDetails>(<object>jsonObj), true);
                case "LOAD_EMBEDDING_MODELS":
                    return model.EmbeddingModelDetails.getJsonObj(<model.EmbeddingModelDetails>(<object>jsonObj), true);
                case "PROVISION_USER_SCHEMA":
                    return model.ProvisionUserSchemaDetails.getJsonObj(<model.ProvisionUserSchemaDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.actionType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExecuteDatabaseUserWorkflowsDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "actionType" in obj && obj.actionType) {
            switch (obj.actionType) {
                case "EXECUTE_SQL_COMMAND":
                    return model.ExecuteSqlCommandDetails.getDeserializedJsonObj(<model.ExecuteSqlCommandDetails>(<object>jsonObj), true);
                case "DEPROVISION_USER_SCHEMAS":
                    return model.DeProvisionUserSchemasDetails.getDeserializedJsonObj(<model.DeProvisionUserSchemasDetails>(<object>jsonObj), true);
                case "LOAD_EMBEDDING_MODELS":
                    return model.EmbeddingModelDetails.getDeserializedJsonObj(<model.EmbeddingModelDetails>(<object>jsonObj), true);
                case "PROVISION_USER_SCHEMA":
                    return model.ProvisionUserSchemaDetails.getDeserializedJsonObj(<model.ProvisionUserSchemaDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.actionType}`)
        }
        }
        return jsonObj;
    }
}
