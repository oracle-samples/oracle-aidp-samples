// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Properties required to automatically scale the clusters up and down based on load.
*/
export interface AutoScale {
    /**
    * The minimum number of workers to which the cluster can scale down when underused. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'minWorkers'?: number;
    /**
    * The maximum number of workers to which the cluster can scale up when overloaded. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxWorkers'?: number;

}

export namespace AutoScale {



    export function getJsonObj(obj: AutoScale): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AutoScale): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
