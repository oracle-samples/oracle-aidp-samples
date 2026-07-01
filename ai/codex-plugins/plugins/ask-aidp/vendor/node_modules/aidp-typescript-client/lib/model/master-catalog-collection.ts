// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing Master Catalogs
*/
export interface MasterCatalogCollection {
    /**
    * List of Master Catalogs.
    */
    'items': Array<model.MasterCatalogSummary>;

}

export namespace MasterCatalogCollection {


    export function getJsonObj(obj: MasterCatalogCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.MasterCatalogSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MasterCatalogCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.MasterCatalogSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
