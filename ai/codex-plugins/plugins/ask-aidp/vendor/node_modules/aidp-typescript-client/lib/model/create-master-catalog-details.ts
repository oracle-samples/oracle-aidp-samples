// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a Master Catalog.
*/
export interface CreateMasterCatalogDetails {
    /**
    * A user-friendly name. Has to be unique and it's changeable.
    */
    'displayName': string;
    /**
    * Short description of the catalog
    */
    'description'?: string;

}

export namespace CreateMasterCatalogDetails {



    export function getJsonObj(obj: CreateMasterCatalogDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateMasterCatalogDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
