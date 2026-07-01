// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a job.
*/
export interface RevokeJobPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permission revoked from the assignee as per the index of assignee. This list should be same size as assignees list.
    */
    'permissions': Array<RevokeJobPermissionDetails.Permissions>;

}

export namespace RevokeJobPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: RevokeJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
