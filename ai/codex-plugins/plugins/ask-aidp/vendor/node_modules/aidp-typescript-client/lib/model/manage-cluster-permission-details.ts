// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a cluster.
*/
export interface ManageClusterPermissionDetails {
    'assignClusterPermissionDetails'?: model.AssignClusterPermissionDetails;
    'revokeClusterPermissionDetails'?: model.RevokeClusterPermissionDetails;

}

export namespace ManageClusterPermissionDetails {



    export function getJsonObj(obj: ManageClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignClusterPermissionDetails': obj.assignClusterPermissionDetails ?
                
                
                model.AssignClusterPermissionDetails.getJsonObj(obj.assignClusterPermissionDetails) : undefined,
                'revokeClusterPermissionDetails': obj.revokeClusterPermissionDetails ?
                
                
                model.RevokeClusterPermissionDetails.getJsonObj(obj.revokeClusterPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageClusterPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignClusterPermissionDetails': obj.assignClusterPermissionDetails ?
                
                
                model.AssignClusterPermissionDetails.getDeserializedJsonObj(obj.assignClusterPermissionDetails) : undefined,
                    'revokeClusterPermissionDetails': obj.revokeClusterPermissionDetails ?
                
                
                model.RevokeClusterPermissionDetails.getDeserializedJsonObj(obj.revokeClusterPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
