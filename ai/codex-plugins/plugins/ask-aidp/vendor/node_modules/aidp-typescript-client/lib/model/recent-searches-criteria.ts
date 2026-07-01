// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details about recent searches criteria
*/
export interface RecentSearchesCriteria {
    /**
    * The maximum number of items to return. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'limit'?: number;

}

export namespace RecentSearchesCriteria {


    export function getJsonObj(obj: RecentSearchesCriteria): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RecentSearchesCriteria): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
