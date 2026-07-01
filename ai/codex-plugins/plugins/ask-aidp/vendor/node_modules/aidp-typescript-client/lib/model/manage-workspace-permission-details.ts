// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a workspace.
*/
export interface ManageWorkspacePermissionDetails {
    'assignWorkspacePermissionDetails'?: model.AssignWorkspacePermissionDetails;
    'revokeWorkspacePermissionDetails'?: model.RevokeWorkspacePermissionDetails;

}

export namespace ManageWorkspacePermissionDetails {



    export function getJsonObj(obj: ManageWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignWorkspacePermissionDetails': obj.assignWorkspacePermissionDetails ?
                
                
                model.AssignWorkspacePermissionDetails.getJsonObj(obj.assignWorkspacePermissionDetails) : undefined,
                'revokeWorkspacePermissionDetails': obj.revokeWorkspacePermissionDetails ?
                
                
                model.RevokeWorkspacePermissionDetails.getJsonObj(obj.revokeWorkspacePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignWorkspacePermissionDetails': obj.assignWorkspacePermissionDetails ?
                
                
                model.AssignWorkspacePermissionDetails.getDeserializedJsonObj(obj.assignWorkspacePermissionDetails) : undefined,
                    'revokeWorkspacePermissionDetails': obj.revokeWorkspacePermissionDetails ?
                
                
                model.RevokeWorkspacePermissionDetails.getDeserializedJsonObj(obj.revokeWorkspacePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
