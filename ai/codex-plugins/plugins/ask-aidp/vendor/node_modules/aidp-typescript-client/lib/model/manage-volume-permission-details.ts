// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a volume.
*/
export interface ManageVolumePermissionDetails {
    'assignVolumePermissionDetails'?: model.AssignVolumePermissionDetails;
    'revokeVolumePermissionDetails'?: model.RevokeVolumePermissionDetails;

}

export namespace ManageVolumePermissionDetails {



    export function getJsonObj(obj: ManageVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignVolumePermissionDetails': obj.assignVolumePermissionDetails ?
                
                
                model.AssignVolumePermissionDetails.getJsonObj(obj.assignVolumePermissionDetails) : undefined,
                'revokeVolumePermissionDetails': obj.revokeVolumePermissionDetails ?
                
                
                model.RevokeVolumePermissionDetails.getJsonObj(obj.revokeVolumePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageVolumePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignVolumePermissionDetails': obj.assignVolumePermissionDetails ?
                
                
                model.AssignVolumePermissionDetails.getDeserializedJsonObj(obj.assignVolumePermissionDetails) : undefined,
                    'revokeVolumePermissionDetails': obj.revokeVolumePermissionDetails ?
                
                
                model.RevokeVolumePermissionDetails.getDeserializedJsonObj(obj.revokeVolumePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
