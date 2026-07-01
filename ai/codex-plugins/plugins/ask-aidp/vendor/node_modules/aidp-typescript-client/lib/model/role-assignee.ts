// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The user/principal/role the role can be assigned to.
*/
export interface RoleAssignee {
    /**
    * assignee type.
    */
    'type': model.RoleAssigneeType;
    /**
    * The OCID for a principal or role.
    */
    'target': string;
    /**
    * The name for a principal or role.
    */
    'targetName'?: string;

}

export namespace RoleAssignee {




    export function getJsonObj(obj: RoleAssignee): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RoleAssignee): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
