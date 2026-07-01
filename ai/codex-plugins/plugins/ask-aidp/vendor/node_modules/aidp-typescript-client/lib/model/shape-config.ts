// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Shape of the driver or executor if a flexible shape is used.
*/
export interface ShapeConfig {
    /**
    * Total number of OCPUs used for the driver or workers. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'ocpus'?: number;
    /**
    * Total number of GPUs used for the driver or workers. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'gpus'?: number;
    /**
    * Amount of memory used for the driver or workers. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'memoryInGBs'?: number;

}

export namespace ShapeConfig {




    export function getJsonObj(obj: ShapeConfig): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ShapeConfig): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
