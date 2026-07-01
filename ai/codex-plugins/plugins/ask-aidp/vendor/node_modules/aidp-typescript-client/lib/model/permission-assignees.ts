// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the assignees
*/
export interface PermissionAssignees {
    /**
    * Grantee type.
    */
    'type': model.GranteeType;
    /**
    * The names/ocids of the users, groups or roles.
    */
    'targets': Array<string>;

}

export namespace PermissionAssignees {



    export function getJsonObj(obj: PermissionAssignees): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PermissionAssignees): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
