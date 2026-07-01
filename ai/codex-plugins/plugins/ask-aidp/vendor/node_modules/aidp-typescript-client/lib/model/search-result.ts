// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single search result.
*/
export interface SearchResult {
    /**
    * The actual log data with field mappings.
    */
    'data': any;

}

export namespace SearchResult {


    export function getJsonObj(obj: SearchResult): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchResult): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
