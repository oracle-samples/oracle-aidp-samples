// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the recipient to an assignee.
*/
export interface AssignRecipientPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignRecipientPermissionDetails.Permissions>;

}

export namespace AssignRecipientPermissionDetails {


    export enum Permissions {
    
    Admin = "ADMIN",
    Use = "USE",
    Read = "READ"

}


    export function getJsonObj(obj: AssignRecipientPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignRecipientPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
