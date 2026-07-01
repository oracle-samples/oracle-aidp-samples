// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a Knowledge Base.
*/
export interface RevokeKnowledgeBasePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees
    */
    'permissions': Array<RevokeKnowledgeBasePermissionDetails.Permissions>;

}

export namespace RevokeKnowledgeBasePermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: RevokeKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeKnowledgeBasePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
