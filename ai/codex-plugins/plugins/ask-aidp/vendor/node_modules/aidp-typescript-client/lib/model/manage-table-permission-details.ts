// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a table.
*/
export interface ManageTablePermissionDetails {
    'assignTablePermissionDetails'?: model.AssignTablePermissionDetails;
    'revokeTablePermissionDetails'?: model.RevokeTablePermissionDetails;

}

export namespace ManageTablePermissionDetails {



    export function getJsonObj(obj: ManageTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignTablePermissionDetails': obj.assignTablePermissionDetails ?
                
                
                model.AssignTablePermissionDetails.getJsonObj(obj.assignTablePermissionDetails) : undefined,
                'revokeTablePermissionDetails': obj.revokeTablePermissionDetails ?
                
                
                model.RevokeTablePermissionDetails.getJsonObj(obj.revokeTablePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignTablePermissionDetails': obj.assignTablePermissionDetails ?
                
                
                model.AssignTablePermissionDetails.getDeserializedJsonObj(obj.assignTablePermissionDetails) : undefined,
                    'revokeTablePermissionDetails': obj.revokeTablePermissionDetails ?
                
                
                model.RevokeTablePermissionDetails.getDeserializedJsonObj(obj.revokeTablePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
