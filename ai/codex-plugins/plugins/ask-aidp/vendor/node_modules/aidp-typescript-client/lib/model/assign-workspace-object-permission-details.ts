// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the workspace object to assignee.
*/
export interface AssignWorkspaceObjectPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees.
    */
    'permissions': Array<AssignWorkspaceObjectPermissionDetails.Permissions>;
    /**
    * Property to determine if permission should be inheritable or not, its default value is true. This is applicable only on workspace folders not on files.
    */
    'isPermissionsInheritable'?: boolean;

}

export namespace AssignWorkspaceObjectPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN"

}



    export function getJsonObj(obj: AssignWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,


         }};

        
        
        return jsonObj;
    }
}
