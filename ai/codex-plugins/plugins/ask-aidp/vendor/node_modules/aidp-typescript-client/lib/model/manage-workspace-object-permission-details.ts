// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a workspace object.
*/
export interface ManageWorkspaceObjectPermissionDetails {
    'assignWorkspaceObjectPermissionDetails'?: model.AssignWorkspaceObjectPermissionDetails;
    'revokeWorkspaceObjectPermissionDetails'?: model.RevokeWorkspaceObjectPermissionDetails;

}

export namespace ManageWorkspaceObjectPermissionDetails {



    export function getJsonObj(obj: ManageWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignWorkspaceObjectPermissionDetails': obj.assignWorkspaceObjectPermissionDetails ?
                
                
                model.AssignWorkspaceObjectPermissionDetails.getJsonObj(obj.assignWorkspaceObjectPermissionDetails) : undefined,
                'revokeWorkspaceObjectPermissionDetails': obj.revokeWorkspaceObjectPermissionDetails ?
                
                
                model.RevokeWorkspaceObjectPermissionDetails.getJsonObj(obj.revokeWorkspaceObjectPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageWorkspaceObjectPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignWorkspaceObjectPermissionDetails': obj.assignWorkspaceObjectPermissionDetails ?
                
                
                model.AssignWorkspaceObjectPermissionDetails.getDeserializedJsonObj(obj.assignWorkspaceObjectPermissionDetails) : undefined,
                    'revokeWorkspaceObjectPermissionDetails': obj.revokeWorkspaceObjectPermissionDetails ?
                
                
                model.RevokeWorkspaceObjectPermissionDetails.getDeserializedJsonObj(obj.revokeWorkspaceObjectPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
