// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a Master Catalog.
*/
export interface UpdateMasterCatalogDetails {
    /**
    * Short description of the catalog
    */
    'description'?: string;

}

export namespace UpdateMasterCatalogDetails {


    export function getJsonObj(obj: UpdateMasterCatalogDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateMasterCatalogDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
