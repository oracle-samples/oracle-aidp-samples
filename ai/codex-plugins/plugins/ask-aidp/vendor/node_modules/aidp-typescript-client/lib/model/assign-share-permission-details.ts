// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the share to assignee.
*/
export interface AssignSharePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees.
    */
    'permissions': Array<AssignSharePermissionDetails.Permissions>;

}

export namespace AssignSharePermissionDetails {


    export enum Permissions {
    
    Admin = "ADMIN",
    Read = "READ",
    Use = "USE"

}


    export function getJsonObj(obj: AssignSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
