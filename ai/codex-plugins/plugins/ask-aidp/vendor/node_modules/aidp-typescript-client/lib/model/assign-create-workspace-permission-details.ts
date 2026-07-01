// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the workspace to assignee.
*/
export interface AssignCreateWorkspacePermissionDetails {
    'assignees': model.PermissionAssignees;

}

export namespace AssignCreateWorkspacePermissionDetails {


    export function getJsonObj(obj: AssignCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignCreateWorkspacePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,
         }};

        
        
        return jsonObj;
    }
}
