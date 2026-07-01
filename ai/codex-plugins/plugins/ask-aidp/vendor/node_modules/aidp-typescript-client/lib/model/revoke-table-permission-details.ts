// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignee for a table.
*/
export interface RevokeTablePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignee.
    */
    'permissions': Array<RevokeTablePermissionDetails.Permissions>;
    /**
    * The list of columns to be included for the revoking of permissions.
    */
    'includeColumns': Array<string>;
    /**
    * The list of columns to be excluded for the revoking of permissions.
    */
    'excludeColumns': Array<string>;

}

export namespace RevokeTablePermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: RevokeTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,



         }};

        
        
        return jsonObj;
    }
}
