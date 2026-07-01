// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details about suggest criteria
*/
export interface SuggestCriteria {
    /**
    * Suggest query string
    */
    'query'?: string;
    /**
    * The maximum number of items to return. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'limit'?: number;

}

export namespace SuggestCriteria {



    export function getJsonObj(obj: SuggestCriteria): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SuggestCriteria): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
