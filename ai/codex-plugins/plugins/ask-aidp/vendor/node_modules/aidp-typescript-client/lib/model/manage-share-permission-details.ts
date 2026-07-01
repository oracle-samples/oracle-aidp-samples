// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a share.
*/
export interface ManageSharePermissionDetails {
    'assignSharePermissionDetails'?: model.AssignSharePermissionDetails;
    'revokeSharePermissionDetails'?: model.RevokeSharePermissionDetails;

}

export namespace ManageSharePermissionDetails {



    export function getJsonObj(obj: ManageSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignSharePermissionDetails': obj.assignSharePermissionDetails ?
                
                
                model.AssignSharePermissionDetails.getJsonObj(obj.assignSharePermissionDetails) : undefined,
                'revokeSharePermissionDetails': obj.revokeSharePermissionDetails ?
                
                
                model.RevokeSharePermissionDetails.getJsonObj(obj.revokeSharePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageSharePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignSharePermissionDetails': obj.assignSharePermissionDetails ?
                
                
                model.AssignSharePermissionDetails.getDeserializedJsonObj(obj.assignSharePermissionDetails) : undefined,
                    'revokeSharePermissionDetails': obj.revokeSharePermissionDetails ?
                
                
                model.RevokeSharePermissionDetails.getDeserializedJsonObj(obj.revokeSharePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
