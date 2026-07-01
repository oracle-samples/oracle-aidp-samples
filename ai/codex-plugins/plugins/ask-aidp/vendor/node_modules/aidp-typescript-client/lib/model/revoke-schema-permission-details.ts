// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a schema.
*/
export interface RevokeSchemaPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees.
    */
    'permissions': Array<RevokeSchemaPermissionDetails.Permissions>;

}

export namespace RevokeSchemaPermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Write = "WRITE",
    CreateView = "CREATE_VIEW",
    CreateVolume = "CREATE_VOLUME",
    CreateTable = "CREATE_TABLE",
    Admin = "ADMIN",
    CreateKnowledgeBase = "CREATE_KNOWLEDGE_BASE"

}


    export function getJsonObj(obj: RevokeSchemaPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeSchemaPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
