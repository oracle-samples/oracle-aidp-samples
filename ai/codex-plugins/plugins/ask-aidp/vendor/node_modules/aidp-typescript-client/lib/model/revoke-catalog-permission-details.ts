// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a catalog.
*/
export interface RevokeCatalogPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees.
    */
    'permissions': Array<RevokeCatalogPermissionDetails.Permissions>;

}

export namespace RevokeCatalogPermissionDetails {


    export enum Permissions {
    
    Select = "SELECT",
    Manage = "MANAGE",
    CreateSchema = "CREATE_SCHEMA",
    Admin = "ADMIN"

}


    export function getJsonObj(obj: RevokeCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
