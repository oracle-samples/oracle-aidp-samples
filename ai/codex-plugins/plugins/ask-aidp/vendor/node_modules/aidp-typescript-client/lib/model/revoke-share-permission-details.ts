// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a share.
*/
export interface RevokeSharePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees.
    */
    'permissions': Array<RevokeSharePermissionDetails.Permissions>;

}

export namespace RevokeSharePermissionDetails {


    export enum Permissions {
    
    Admin = "ADMIN",
    Read = "READ",
    Use = "USE"

}


    export function getJsonObj(obj: RevokeSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
