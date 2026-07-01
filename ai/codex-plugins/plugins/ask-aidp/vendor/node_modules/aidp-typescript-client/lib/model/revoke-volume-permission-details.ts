// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a volume.
*/
export interface RevokeVolumePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees.
    */
    'permissions': Array<RevokeVolumePermissionDetails.Permissions>;

}

export namespace RevokeVolumePermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Write = "WRITE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: RevokeVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
