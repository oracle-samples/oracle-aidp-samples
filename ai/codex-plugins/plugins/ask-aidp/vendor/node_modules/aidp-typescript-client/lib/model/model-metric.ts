// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model metric.
*/
export interface ModelMetric {
    /**
    * Name of the metric.
    */
    'key'?: string;
    /**
    * Value of the metric. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'value'?: number;
    /**
    * Unix timestamp in milliseconds when this metric was recorded. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timestamp'?: number;
    /**
    * Step at which to log the metric. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'step'?: number;

}

export namespace ModelMetric {





    export function getJsonObj(obj: ModelMetric): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelMetric): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
