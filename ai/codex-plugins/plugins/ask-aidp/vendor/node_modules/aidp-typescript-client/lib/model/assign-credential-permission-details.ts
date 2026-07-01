// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the credential to assignee.
*/
export interface AssignCredentialPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignCredentialPermissionDetails.Permissions>;

}

export namespace AssignCredentialPermissionDetails {


    export enum Permissions {
    
    Use = "USE",
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignCredentialPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignCredentialPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
