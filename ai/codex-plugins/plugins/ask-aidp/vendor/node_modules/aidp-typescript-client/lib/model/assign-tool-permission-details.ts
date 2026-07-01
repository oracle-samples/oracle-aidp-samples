// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the tool to assignee.
*/
export interface AssignToolPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignToolPermissionDetails.Permissions>;
    /**
    * The list of columns to be included for the assigning of permissions
    */
    'includeColumns': Array<string>;
    /**
    * The list of columns to be excluded for the assigning of permissions
    */
    'excludeColumns': Array<string>;

}

export namespace AssignToolPermissionDetails {


    export enum Permissions {
    
    Read = "READ",
    Manage = "MANAGE",
    Admin = "ADMIN"

}




    export function getJsonObj(obj: AssignToolPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignToolPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,



         }};

        
        
        return jsonObj;
    }
}
