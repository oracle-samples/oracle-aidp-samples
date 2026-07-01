// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update permissions on a catalog.
*/
export interface ManageCatalogPermissionDetails {
    'assignCatalogPermissionDetails'?: model.AssignCatalogPermissionDetails;
    'revokeCatalogPermissionDetails'?: model.RevokeCatalogPermissionDetails;

}

export namespace ManageCatalogPermissionDetails {



    export function getJsonObj(obj: ManageCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                'assignCatalogPermissionDetails': obj.assignCatalogPermissionDetails ?
                
                
                model.AssignCatalogPermissionDetails.getJsonObj(obj.assignCatalogPermissionDetails) : undefined,
                'revokeCatalogPermissionDetails': obj.revokeCatalogPermissionDetails ?
                
                
                model.RevokeCatalogPermissionDetails.getJsonObj(obj.revokeCatalogPermissionDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageCatalogPermissionDetails): object {
        const jsonObj = {...obj, ...{
            
                    'assignCatalogPermissionDetails': obj.assignCatalogPermissionDetails ?
                
                
                model.AssignCatalogPermissionDetails.getDeserializedJsonObj(obj.assignCatalogPermissionDetails) : undefined,
                    'revokeCatalogPermissionDetails': obj.revokeCatalogPermissionDetails ?
                
                
                model.RevokeCatalogPermissionDetails.getDeserializedJsonObj(obj.revokeCatalogPermissionDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
