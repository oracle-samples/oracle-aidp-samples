// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the workspace to assignee.
*/
export interface AssignWorkspacePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees.
    */
    'permissions': Array<AssignWorkspacePermissionDetails.Permissions>;

}

export namespace AssignWorkspacePermissionDetails {


    export enum Permissions {
    
    User = "USER",
    PrivilegedUser = "PRIVILEGED_USER",
    Administrator = "ADMINISTRATOR"

}


    export function getJsonObj(obj: AssignWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
