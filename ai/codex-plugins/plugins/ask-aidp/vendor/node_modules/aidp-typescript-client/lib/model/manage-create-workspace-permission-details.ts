// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a workspace.
*/
export interface ManageCreateWorkspacePermissionDetails {
    'assignCreateWorkspacePermissionDetails'?: model.AssignCreateWorkspacePermissionDetails;
    'revokeCreateWorkspacePermissionDetails'?: model.RevokeCreateWorkspacePermissionDetails;

}

export namespace ManageCreateWorkspacePermissionDetails {



    export function getJsonObj(obj: ManageCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignCreateWorkspacePermissionDetails': obj.assignCreateWorkspacePermissionDetails ?
                
                
                model.AssignCreateWorkspacePermissionDetails.getJsonObj(obj.assignCreateWorkspacePermissionDetails) : undefined,
                'revokeCreateWorkspacePermissionDetails': obj.revokeCreateWorkspacePermissionDetails ?
                
                
                model.RevokeCreateWorkspacePermissionDetails.getJsonObj(obj.revokeCreateWorkspacePermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignCreateWorkspacePermissionDetails': obj.assignCreateWorkspacePermissionDetails ?
                
                
                model.AssignCreateWorkspacePermissionDetails.getDeserializedJsonObj(obj.assignCreateWorkspacePermissionDetails) : undefined,
                    'revokeCreateWorkspacePermissionDetails': obj.revokeCreateWorkspacePermissionDetails ?
                
                
                model.RevokeCreateWorkspacePermissionDetails.getDeserializedJsonObj(obj.revokeCreateWorkspacePermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
