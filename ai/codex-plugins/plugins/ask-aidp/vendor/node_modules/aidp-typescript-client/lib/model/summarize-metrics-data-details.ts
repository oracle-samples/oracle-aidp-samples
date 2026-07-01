// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for summarizing compute metrics data.
*/
export interface SummarizeMetricsDataDetails {
    /**
    * The type of aggregation to apply.
* - Standard Aggregations: {@code MAX}, {@code MEAN}, {@code MIN}, {@code SUM}, {@code RATE}.
* - Percentile Aggregation: Use {@code Pxx}, where {@code xx} is the percentile value (e.g., {@code P80} for the 80th percentile).
* 
    */
    'aggregationType': SummarizeMetricsDataDetails.AggregationType;
    /**
    * The metric to summarize. 
* - Supported values include but are not limited to:
*   - {@code CPU_UTILIZATION}, {@code MEMORY_UTILIZATION}, {@code FILE_SYSTEM_UTILIZATION}, {@code GC_CPU_UTILIZATION}
*   - {@code DISK_READ_BYTES}, {@code DISK_WRITE_BYTES}, {@code NETWORK_RECEIVE_BYTES}, {@code NETWORK_TRANSMIT_BYTES}
*   - {@code APP_STATUS}, {@code EXECUTOR_METRICS}, {@code SYSTEM_CPU}, {@code SYSTEM_MEMORY}
*   - {@code SYSTEM_NETWORK_IN}, {@code SYSTEM_NETWORK_OUT}, {@code SYSTEM_DISK_READ}, {@code SYSTEM_DISK_WRITE}
*   - Additional metrics such as task-related and shuffle metrics may be introduced in the future.
* - Refer to API documentation or contact support for the latest supported metric list.
* 
    */
    'metricName': string;
    /**
    * The beginning of the time range to use when searching for metric data points. Format is RFC 3339.
    */
    'timeBegin': Date;
    /**
    * The end of the time range to use when searching for metric data points. Format is RFC 3339.
    */
    'timeEnd': Date;
    /**
    * The time window used to convert the set of raw data points.
* The timestamp of the aggregated data point corresponds to the end of the time window during which raw data points are assessed.
* For example, for a five-minute interval, the timestamp \"2:05\" corresponds to the five-minute time window from 2:00:00 to 2:05:00.
* 
    */
    'interval': string;
    /**
    * The time between calculated aggregation windows. Use with the query interval to vary the frequency for returning aggregated data points.
* For example, use a query interval of 5 minutes with a resolution of 1 minute to retrieve five-minute aggregations at a one-minute frequency.
* The resolution must be equal to or less than the interval in the query.
* The default resolution is 1m (one minute).
* Supported values: 1m-60m, 1h-24h, 1d.
* 
    */
    'resolution'?: string;

}

export namespace SummarizeMetricsDataDetails {

    export enum AggregationType {
    
    Max = "MAX",
    Mean = "MEAN",
    Min = "MIN",
    Sum = "SUM",
    Rate = "RATE",
    P50 = "P50",
    P80 = "P80",
    P90 = "P90",
    P95 = "P95",
    P99 = "P99",
    P999 = "P99_9"

}







    export function getJsonObj(obj: SummarizeMetricsDataDetails): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SummarizeMetricsDataDetails): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
