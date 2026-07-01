// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a role.
*/
export interface UpdateRoleDetails {
    /**
    * The role name, it can be changed. No special characters except for \u201C_\u201D. Case insensitive.
    */
    'displayName'?: string;
    /**
    * The description of the role.
    */
    'description'?: string;

}

export namespace UpdateRoleDetails {



    export function getJsonObj(obj: UpdateRoleDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateRoleDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
