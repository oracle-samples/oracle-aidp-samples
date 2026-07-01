// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a role.
*/
export interface ManageRolePermissionDetails {
    'assignRolePermissionDetails'?: model.AssignRolePermissionDetails;
    'revokeRolePermissionDetails'?: model.RevokeRolePermissionDetails;

}

export namespace ManageRolePermissionDetails {



    export function getJsonObj(obj: ManageRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignRolePermissionDetails': obj.assignRolePermissionDetails ?
                
                
                model.AssignRolePermissionDetails.getJsonObj(obj.assignRolePermissionDetails) : undefined,
                'revokeRolePermissionDetails': obj.revokeRolePermissionDetails ?
                
                
                model.RevokeRolePermissionDetails.getJsonObj(obj.revokeRolePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageRolePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignRolePermissionDetails': obj.assignRolePermissionDetails ?
                
                
                model.AssignRolePermissionDetails.getDeserializedJsonObj(obj.assignRolePermissionDetails) : undefined,
                    'revokeRolePermissionDetails': obj.revokeRolePermissionDetails ?
                
                
                model.RevokeRolePermissionDetails.getDeserializedJsonObj(obj.revokeRolePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
