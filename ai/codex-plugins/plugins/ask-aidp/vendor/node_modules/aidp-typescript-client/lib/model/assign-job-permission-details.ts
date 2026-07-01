// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the Job to assignee.
*/
export interface AssignJobPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permission assigned to the assignee as per the index of assignee. This list should be same size as assignees list.
    */
    'permissions': Array<AssignJobPermissionDetails.Permissions>;

}

export namespace AssignJobPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Manage = "MANAGE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
