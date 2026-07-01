// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* keyed time series
*/
export interface KeyedTimeSeries {
    /**
    * KPI field name (e.g., totalSessions, totalInputTokenCount)
    */
    'key': string;
    /**
    * aggregated time series data points
    */
    'aggregatedTimeSeriesDataPoints': Array<model.AggregatedTimeSeriesDataPoint>;

}

export namespace KeyedTimeSeries {



    export function getJsonObj(obj: KeyedTimeSeries): object {
        const jsonObj = {...obj, ...{
            

                'aggregatedTimeSeriesDataPoints': obj.aggregatedTimeSeriesDataPoints ?
                
                obj.aggregatedTimeSeriesDataPoints.map((item)=>{return model.AggregatedTimeSeriesDataPoint.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KeyedTimeSeries): object {
        const jsonObj = {...obj, ...{
            

                    'aggregatedTimeSeriesDataPoints': obj.aggregatedTimeSeriesDataPoints ?
                
                obj.aggregatedTimeSeriesDataPoints.map((item)=>{return model.AggregatedTimeSeriesDataPoint.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
