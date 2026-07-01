// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a workspace object.
*/
export interface RevokeWorkspaceObjectPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees.
    */
    'permissions': Array<RevokeWorkspaceObjectPermissionDetails.Permissions>;
    /**
    * Property to determine that permission which should be removed is inheritable or not. This is applicable only on workspace folders not on files.
    */
    'isPermissionsInheritable'?: boolean;

}

export namespace RevokeWorkspaceObjectPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN"

}



    export function getJsonObj(obj: RevokeWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,


         }};

        
        
        return jsonObj;
    }
}
