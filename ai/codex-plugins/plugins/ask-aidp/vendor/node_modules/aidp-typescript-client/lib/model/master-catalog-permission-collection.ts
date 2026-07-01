// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of Master Catalog permissions.
*/
export interface MasterCatalogPermissionCollection {
    /**
    * List of Master Catalog permissions.
    */
    'items': Array<model.MasterCatalogPermissionSummary>;

}

export namespace MasterCatalogPermissionCollection {


    export function getJsonObj(obj: MasterCatalogPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.MasterCatalogPermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MasterCatalogPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.MasterCatalogPermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
