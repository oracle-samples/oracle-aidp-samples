// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the table to assignee.
*/
export interface AssignTablePermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees.
    */
    'permissions': Array<AssignTablePermissionDetails.Permissions>;
    /**
    * The list of columns to be included for the assigning of permissions.
    */
    'includeColumns': Array<string>;
    /**
    * The list of columns to be excluded for the assigning of permissions.
    */
    'excludeColumns': Array<string>;

}

export namespace AssignTablePermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Insert = "INSERT",
    Update = "UPDATE",
    Delete = "DELETE",
    Alter = "ALTER",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: AssignTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignTablePermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,



         }};

        
        
        return jsonObj;
    }
}
