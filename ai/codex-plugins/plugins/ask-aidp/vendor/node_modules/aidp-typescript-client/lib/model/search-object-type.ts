// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Possible types of search object
*/
export interface SearchObjectType {
    /**
    * Possible types of search object
    */
    'name'?: string;
    /**
    * Total count of search object Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'count'?: number;

}

export namespace SearchObjectType {



    export function getJsonObj(obj: SearchObjectType): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SearchObjectType): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
