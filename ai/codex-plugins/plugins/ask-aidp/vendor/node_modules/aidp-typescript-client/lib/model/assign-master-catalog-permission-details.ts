// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions assigned on the MasterCatalog to assignee.
*/
export interface AssignMasterCatalogPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions assigned to the assignees
    */
    'permissions': Array<AssignMasterCatalogPermissionDetails.Permissions>;

}

export namespace AssignMasterCatalogPermissionDetails {


    export enum Permissions {
    
    CreateCatalog = "CREATE_CATALOG",
    Admin = "ADMIN",
    CreateShare = "CREATE_SHARE",
    CreateRecipient = "CREATE_RECIPIENT",
    CreateCredential = "CREATE_CREDENTIAL"

}


    export function getJsonObj(obj: AssignMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AssignMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
