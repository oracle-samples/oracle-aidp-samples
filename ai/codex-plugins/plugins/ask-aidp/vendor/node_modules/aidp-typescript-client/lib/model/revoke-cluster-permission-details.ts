// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the permissions revoked from assignees for a cluster.
*/
export interface RevokeClusterPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * Permissions revoked from the assignees.
    */
    'permissions': Array<RevokeClusterPermissionDetails.Permissions>;

}

export namespace RevokeClusterPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Use = "USE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: RevokeClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
