// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Deprovision user schemas created by AI Data Platform Workbench.
*/
export interface DeProvisionUserSchemasDetails extends model.ExecuteDatabaseUserWorkflowsDetails {

   "actionType": string;
}

export namespace DeProvisionUserSchemasDetails {

    export function getJsonObj(obj: DeProvisionUserSchemasDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getJsonObj(obj) as DeProvisionUserSchemasDetails, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const actionType = 'DEPROVISION_USER_SCHEMAS';
    export function getDeserializedJsonObj(obj: DeProvisionUserSchemasDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getDeserializedJsonObj(obj) as DeProvisionUserSchemasDetails, ...{
            
         }};

        
        
        return jsonObj;
    }
}
