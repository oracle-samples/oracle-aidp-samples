// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the catalog to assignee.
*/
export interface AssignCatalogPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignCatalogPermissionDetails.Permissions>;

}

export namespace AssignCatalogPermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    CreateSchema = "CREATE_SCHEMA",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: AssignCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
