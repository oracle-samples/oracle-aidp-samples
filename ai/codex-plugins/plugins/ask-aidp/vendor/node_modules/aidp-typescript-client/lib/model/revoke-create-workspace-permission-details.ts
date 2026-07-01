// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a workspace.
*/
export interface RevokeCreateWorkspacePermissionDetails {
    'assignees': model.PermissionAssignees;

}

export namespace RevokeCreateWorkspacePermissionDetails {


    export function getJsonObj(obj: RevokeCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,
         }};

        
        
        return jsonObj;
    }
}
