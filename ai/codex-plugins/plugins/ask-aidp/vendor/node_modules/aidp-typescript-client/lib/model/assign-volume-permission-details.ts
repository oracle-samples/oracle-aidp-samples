// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the volume to assignee.
*/
export interface AssignVolumePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees.
    */
    'permissions': Array<AssignVolumePermissionDetails.Permissions>;

}

export namespace AssignVolumePermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Write = "WRITE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
