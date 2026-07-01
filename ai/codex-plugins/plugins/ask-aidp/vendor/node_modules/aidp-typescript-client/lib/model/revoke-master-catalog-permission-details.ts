// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of the permissions revoked from assignees for a MasterCatalog.
*/
export interface RevokeMasterCatalogPermissionDetails {
    'assignees': model.PermissionAssignees;
    /**
    * The permissions revoked from the assignees
    */
    'permissions': Array<RevokeMasterCatalogPermissionDetails.Permissions>;

}

export namespace RevokeMasterCatalogPermissionDetails {


    export enum Permissions {
    
    CreateCatalog = "CREATE_CATALOG",
    Admin = "ADMIN",
    CreateShare = "CREATE_SHARE",
    CreateRecipient = "CREATE_RECIPIENT",
    CreateCredential = "CREATE_CREDENTIAL"

}


    export function getJsonObj(obj: RevokeMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getJsonObj(obj.assignees) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RevokeMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignees': obj.assignees ?
                
                
                model.PermissionAssignees.getDeserializedJsonObj(obj.assignees) : undefined,

         }};

        
        
        return jsonObj;
    }
}
