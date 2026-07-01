// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A 2D point for edge handles on diagram elements.
*/
export interface Point {
    /**
    * X coordinate. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'x': number;
    /**
    * Y coordinate. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'y': number;

}

export namespace Point {



    export function getJsonObj(obj: Point): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Point): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
