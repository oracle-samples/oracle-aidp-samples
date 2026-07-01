// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Provision user schema which includes creating user schema, persist credentials in credential store.
*/
export interface ProvisionUserSchemaDetails extends model.ExecuteDatabaseUserWorkflowsDetails {
    /**
    * The user schema name. Example: AIDP_<uniqueSuffix>_KB_READONLY
    */
    'userSchemaName'?: string;
    /**
    * The grants to be assigned to the user schema.
    */
    'grants'?: Array<string>;

   "actionType": string;
}

export namespace ProvisionUserSchemaDetails {



    export function getJsonObj(obj: ProvisionUserSchemaDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getJsonObj(obj) as ProvisionUserSchemaDetails, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const actionType = 'PROVISION_USER_SCHEMA';
    export function getDeserializedJsonObj(obj: ProvisionUserSchemaDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ExecuteDatabaseUserWorkflowsDetails.getDeserializedJsonObj(obj) as ProvisionUserSchemaDetails, ...{
            


         }};

        
        
        return jsonObj;
    }
}
