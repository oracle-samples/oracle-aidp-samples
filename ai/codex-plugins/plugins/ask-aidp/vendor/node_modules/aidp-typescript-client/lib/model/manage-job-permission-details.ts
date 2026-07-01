// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a job.
*/
export interface ManageJobPermissionDetails {
    'assignJobPermissionDetails'?: model.AssignJobPermissionDetails;
    'revokeJobPermissionDetails'?: model.RevokeJobPermissionDetails;

}

export namespace ManageJobPermissionDetails {



    export function getJsonObj(obj: ManageJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignJobPermissionDetails': obj.assignJobPermissionDetails ?
                
                
                model.AssignJobPermissionDetails.getJsonObj(obj.assignJobPermissionDetails) : undefined,
                'revokeJobPermissionDetails': obj.revokeJobPermissionDetails ?
                
                
                model.RevokeJobPermissionDetails.getJsonObj(obj.revokeJobPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageJobPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignJobPermissionDetails': obj.assignJobPermissionDetails ?
                
                
                model.AssignJobPermissionDetails.getDeserializedJsonObj(obj.assignJobPermissionDetails) : undefined,
                    'revokeJobPermissionDetails': obj.revokeJobPermissionDetails ?
                
                
                model.RevokeJobPermissionDetails.getDeserializedJsonObj(obj.revokeJobPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
