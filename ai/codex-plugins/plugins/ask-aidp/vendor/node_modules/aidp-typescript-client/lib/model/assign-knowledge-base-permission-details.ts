// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the Knowledge Base to assignee.
*/
export interface AssignKnowledgeBasePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignKnowledgeBasePermissionDetails.Permissions>;

}

export namespace AssignKnowledgeBasePermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
