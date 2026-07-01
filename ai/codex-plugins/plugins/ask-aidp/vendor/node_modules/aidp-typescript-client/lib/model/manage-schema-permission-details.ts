// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a schema.
*/
export interface ManageSchemaPermissionDetails {
    'assignSchemaPermissionDetails'?: model.AssignSchemaPermissionDetails;
    'revokeSchemaPermissionDetails'?: model.RevokeSchemaPermissionDetails;

}

export namespace ManageSchemaPermissionDetails {



    export function getJsonObj(obj: ManageSchemaPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignSchemaPermissionDetails': obj.assignSchemaPermissionDetails ?
                
                
                model.AssignSchemaPermissionDetails.getJsonObj(obj.assignSchemaPermissionDetails) : undefined,
                'revokeSchemaPermissionDetails': obj.revokeSchemaPermissionDetails ?
                
                
                model.RevokeSchemaPermissionDetails.getJsonObj(obj.revokeSchemaPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageSchemaPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignSchemaPermissionDetails': obj.assignSchemaPermissionDetails ?
                
                
                model.AssignSchemaPermissionDetails.getDeserializedJsonObj(obj.assignSchemaPermissionDetails) : undefined,
                    'revokeSchemaPermissionDetails': obj.revokeSchemaPermissionDetails ?
                
                
                model.RevokeSchemaPermissionDetails.getDeserializedJsonObj(obj.revokeSchemaPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
