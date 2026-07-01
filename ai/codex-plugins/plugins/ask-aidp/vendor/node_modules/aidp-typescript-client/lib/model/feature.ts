// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single feature and its value.
*/
export interface Feature {
    /**
    * The name of the queried feature.
    */
    'featureName': string;
    /**
    * The status value of the feature.
    */
    'value': string;

}

export namespace Feature {



    export function getJsonObj(obj: Feature): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Feature): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
