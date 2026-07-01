// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Aggregated time series data point
*/
export interface AggregatedTimeSeriesDataPoint {
    /**
    * Bucket start timestamp in UTC
    */
    'timestamp': Date;
    /**
    * Value for that time bucket Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'value': number;

}

export namespace AggregatedTimeSeriesDataPoint {



    export function getJsonObj(obj: AggregatedTimeSeriesDataPoint): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AggregatedTimeSeriesDataPoint): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
