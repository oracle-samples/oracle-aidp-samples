// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run metric.
*/
export interface ExperimentRunMetric {
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

export namespace ExperimentRunMetric {





    export function getJsonObj(obj: ExperimentRunMetric): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunMetric): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
