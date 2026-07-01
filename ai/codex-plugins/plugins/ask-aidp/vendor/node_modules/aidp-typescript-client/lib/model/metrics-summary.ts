// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A summarized metric result containing aggregated data points.
*/
export interface MetricsSummary {
    /**
    * The source service or application that emitted the metric.
    */
    'namespace': string;
    /**
    * The OCID of the compartment containing the resources that the aggregated data was returned from.
    */
    'compartmentId': string;
    /**
    * The name of the metric.
    */
    'name': string;
    /**
    * Key-value pairs that provide context for the metric.
    */
    'dimensions': any;
    /**
    * Additional references provided in the metric definition.
    */
    'metadata'?: any;
    /**
    * The time between calculated aggregation windows.
    */
    'resolution'?: string;
    /**
    * A custom string used for grouping related metrics.
    */
    'resourceGroup'?: string;
    /**
    * List of timestamp-value pairs for the metric.
    */
    'aggregatedDataPoints': Array<model.AggregatedDataPoint>;

}

export namespace MetricsSummary {









    export function getJsonObj(obj: MetricsSummary): object {
        const jsonObj = {...obj, ...{
            







                'aggregatedDataPoints': obj.aggregatedDataPoints ?
                
                obj.aggregatedDataPoints.map((item)=>{return model.AggregatedDataPoint.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MetricsSummary): object {
        const jsonObj = {...obj, ...{
            







                    'aggregatedDataPoints': obj.aggregatedDataPoints ?
                
                obj.aggregatedDataPoints.map((item)=>{return model.AggregatedDataPoint.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
