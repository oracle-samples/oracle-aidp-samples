// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details about the new role.
*/
export interface CreateRoleDetails {
    /**
    * The role name, it can be changed. No special characters except for \u201C_\u201D. Case insensitive.
    */
    'displayName': string;
    /**
    * The description of the role.
    */
    'description'?: string;

}

export namespace CreateRoleDetails {



    export function getJsonObj(obj: CreateRoleDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateRoleDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
