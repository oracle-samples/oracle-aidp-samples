// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of a connection associated with a catalog.
*/
export interface CatalogConnectionDetails {
    /**
    * Connection name.
    */
    'displayName'?: string;
    /**
    * Connection properties.
    */
    'connectionProperties': { [key: string]: string; };

}

export namespace CatalogConnectionDetails {



    export function getJsonObj(obj: CatalogConnectionDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CatalogConnectionDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
