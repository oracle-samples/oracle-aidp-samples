// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary of the search response.
*/
export interface SearchResultSummary {
    /**
    * Total number of search results. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'resultCount': number;
    /**
    * Total number of field schema information. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'fieldCount': number;

}

export namespace SearchResultSummary {



    export function getJsonObj(obj: SearchResultSummary): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchResultSummary): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
