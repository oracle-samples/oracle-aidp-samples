// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a recipient.
*/
export interface ManageRecipientPermissionDetails {
    'assignRecipientPermissionDetails'?: model.AssignRecipientPermissionDetails;
    'revokeRecipientPermissionDetails'?: model.RevokeRecipientPermissionDetails;

}

export namespace ManageRecipientPermissionDetails {



    export function getJsonObj(obj: ManageRecipientPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignRecipientPermissionDetails': obj.assignRecipientPermissionDetails ?
                
                
                model.AssignRecipientPermissionDetails.getJsonObj(obj.assignRecipientPermissionDetails) : undefined,
                'revokeRecipientPermissionDetails': obj.revokeRecipientPermissionDetails ?
                
                
                model.RevokeRecipientPermissionDetails.getJsonObj(obj.revokeRecipientPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageRecipientPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignRecipientPermissionDetails': obj.assignRecipientPermissionDetails ?
                
                
                model.AssignRecipientPermissionDetails.getDeserializedJsonObj(obj.assignRecipientPermissionDetails) : undefined,
                    'revokeRecipientPermissionDetails': obj.revokeRecipientPermissionDetails ?
                
                
                model.RevokeRecipientPermissionDetails.getDeserializedJsonObj(obj.revokeRecipientPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
