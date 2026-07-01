// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignee for a Agent flow.
*/
export interface RevokeAgentFlowPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignee.
    */
    'permissions': Array<RevokeAgentFlowPermissionDetails.Permissions>;
    /**
    * The list of columns to be included for the revoking of permissions.
    */
    'includeColumns': Array<string>;
    /**
    * The list of columns to be excluded for the revoking of permissions.
    */
    'excludeColumns': Array<string>;

}

export namespace RevokeAgentFlowPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: RevokeAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeAgentFlowPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,



         }};

        
        
        return jsonObj;
    }
}
