// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single aggregated data point.
*/
export interface AggregatedDataPoint {
    /**
    * The timestamp of the aggregated data point.
    */
    'timestamp': Date;
    /**
    * The computed metric value at the given timestamp. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'value': number;

}

export namespace AggregatedDataPoint {



    export function getJsonObj(obj: AggregatedDataPoint): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AggregatedDataPoint): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
