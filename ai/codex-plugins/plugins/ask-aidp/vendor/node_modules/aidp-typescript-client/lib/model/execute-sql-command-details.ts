// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Running SQL command.
*/
export interface ExecuteSqlCommandDetails extends model.ExecuteDatabaseUserWorkflowsDetails {
    /**
    * The user schema name. Only supports a valid knowledge base user and ADMIN.
    */
    'userSchemaName'?: string;
    /**
    * Base64-encoded SQL command.
    */
    'sqlCommand'?: string;

   "actionType": string;
}

export namespace ExecuteSqlCommandDetails {



    export function getJsonObj(obj: ExecuteSqlCommandDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getJsonObj(obj) as ExecuteSqlCommandDetails, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const actionType = 'EXECUTE_SQL_COMMAND';
    export function getDeserializedJsonObj(obj: ExecuteSqlCommandDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getDeserializedJsonObj(obj) as ExecuteSqlCommandDetails, ...{
            


         }};

        
        
        return jsonObj;
    }
}
