// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a Master Catalog.
*/
export interface ManageMasterCatalogPermissionDetails {
    'assignMasterCatalogPermissionDetails'?: model.AssignMasterCatalogPermissionDetails;
    'revokeMasterCatalogPermissionDetails'?: model.RevokeMasterCatalogPermissionDetails;

}

export namespace ManageMasterCatalogPermissionDetails {



    export function getJsonObj(obj: ManageMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignMasterCatalogPermissionDetails': obj.assignMasterCatalogPermissionDetails ?
                
                
                model.AssignMasterCatalogPermissionDetails.getJsonObj(obj.assignMasterCatalogPermissionDetails) : undefined,
                'revokeMasterCatalogPermissionDetails': obj.revokeMasterCatalogPermissionDetails ?
                
                
                model.RevokeMasterCatalogPermissionDetails.getJsonObj(obj.revokeMasterCatalogPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageMasterCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignMasterCatalogPermissionDetails': obj.assignMasterCatalogPermissionDetails ?
                
                
                model.AssignMasterCatalogPermissionDetails.getDeserializedJsonObj(obj.assignMasterCatalogPermissionDetails) : undefined,
                    'revokeMasterCatalogPermissionDetails': obj.revokeMasterCatalogPermissionDetails ?
                
                
                model.RevokeMasterCatalogPermissionDetails.getDeserializedJsonObj(obj.revokeMasterCatalogPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
