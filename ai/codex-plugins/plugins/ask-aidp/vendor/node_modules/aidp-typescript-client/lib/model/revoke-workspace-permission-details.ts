// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a workspace.
*/
export interface RevokeWorkspacePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees
    */
    'permissions': Array<RevokeWorkspacePermissionDetails.Permissions>;

}

export namespace RevokeWorkspacePermissionDetails {


    export enum Permissions {
    
    User = "USER",
    PrivilegedUser = "PRIVILEGED_USER",
    Administrator = "ADMINISTRATOR"

}


    export function getJsonObj(obj: RevokeWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
