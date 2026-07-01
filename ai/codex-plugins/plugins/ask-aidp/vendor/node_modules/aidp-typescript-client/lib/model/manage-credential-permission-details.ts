// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a credential.
*/
export interface ManageCredentialPermissionDetails {
    'assignCredentialPermissionDetails'?: model.AssignCredentialPermissionDetails;
    'revokeCredentialPermissionDetails'?: model.RevokeCredentialPermissionDetails;

}

export namespace ManageCredentialPermissionDetails {



    export function getJsonObj(obj: ManageCredentialPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignCredentialPermissionDetails': obj.assignCredentialPermissionDetails ?
                
                
                model.AssignCredentialPermissionDetails.getJsonObj(obj.assignCredentialPermissionDetails) : undefined,
                'revokeCredentialPermissionDetails': obj.revokeCredentialPermissionDetails ?
                
                
                model.RevokeCredentialPermissionDetails.getJsonObj(obj.revokeCredentialPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageCredentialPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignCredentialPermissionDetails': obj.assignCredentialPermissionDetails ?
                
                
                model.AssignCredentialPermissionDetails.getDeserializedJsonObj(obj.assignCredentialPermissionDetails) : undefined,
                    'revokeCredentialPermissionDetails': obj.revokeCredentialPermissionDetails ?
                
                
                model.RevokeCredentialPermissionDetails.getDeserializedJsonObj(obj.revokeCredentialPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
