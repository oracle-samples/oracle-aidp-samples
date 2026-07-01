// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a view.
*/
export interface ManageViewPermissionDetails {
    'assignViewPermissionDetails'?: model.AssignViewPermissionDetails;
    'revokeViewPermissionDetails'?: model.RevokeViewPermissionDetails;

}

export namespace ManageViewPermissionDetails {



    export function getJsonObj(obj: ManageViewPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignViewPermissionDetails': obj.assignViewPermissionDetails ?
                
                
                model.AssignViewPermissionDetails.getJsonObj(obj.assignViewPermissionDetails) : undefined,
                'revokeViewPermissionDetails': obj.revokeViewPermissionDetails ?
                
                
                model.RevokeViewPermissionDetails.getJsonObj(obj.revokeViewPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageViewPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignViewPermissionDetails': obj.assignViewPermissionDetails ?
                
                
                model.AssignViewPermissionDetails.getDeserializedJsonObj(obj.assignViewPermissionDetails) : undefined,
                    'revokeViewPermissionDetails': obj.revokeViewPermissionDetails ?
                
                
                model.RevokeViewPermissionDetails.getDeserializedJsonObj(obj.revokeViewPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
