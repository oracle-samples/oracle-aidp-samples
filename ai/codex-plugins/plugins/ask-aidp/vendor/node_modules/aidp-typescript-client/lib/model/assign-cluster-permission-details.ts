// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the permissions assigned on the cluster to assignee.
*/
export interface AssignClusterPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * Permissions assigned to the assignees.
    */
    'permissions': Array<AssignClusterPermissionDetails.Permissions>;

}

export namespace AssignClusterPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
