// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a tool.
*/
export interface ManageToolPermissionDetails {
    'assignToolPermissionDetails'?: model.AssignToolPermissionDetails;
    'revokeToolPermissionDetails'?: model.RevokeToolPermissionDetails;

}

export namespace ManageToolPermissionDetails {



    export function getJsonObj(obj: ManageToolPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignToolPermissionDetails': obj.assignToolPermissionDetails ?
                
                
                model.AssignToolPermissionDetails.getJsonObj(obj.assignToolPermissionDetails) : undefined,
                'revokeToolPermissionDetails': obj.revokeToolPermissionDetails ?
                
                
                model.RevokeToolPermissionDetails.getJsonObj(obj.revokeToolPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageToolPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignToolPermissionDetails': obj.assignToolPermissionDetails ?
                
                
                model.AssignToolPermissionDetails.getDeserializedJsonObj(obj.assignToolPermissionDetails) : undefined,
                    'revokeToolPermissionDetails': obj.revokeToolPermissionDetails ?
                
                
                model.RevokeToolPermissionDetails.getDeserializedJsonObj(obj.revokeToolPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
