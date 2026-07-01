// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Catalog permissions
*/
export interface CatalogPermissionCollection {
    /**
    * List of catalog permissions.
    */
    'items': Array<model.CatalogPermissionSummary>;

}

export namespace CatalogPermissionCollection {


    export function getJsonObj(obj: CatalogPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.CatalogPermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.CatalogPermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
