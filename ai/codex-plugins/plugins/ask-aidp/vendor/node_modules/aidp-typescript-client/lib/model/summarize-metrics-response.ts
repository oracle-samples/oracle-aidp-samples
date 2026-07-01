// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response payload containing summarized metric data.
*/
export interface SummarizeMetricsResponse {
    /**
    * List of computed metric summary results.
    */
    'results': Array<model.MetricsSummary>;

}

export namespace SummarizeMetricsResponse {


    export function getJsonObj(obj: SummarizeMetricsResponse): object {
        const jsonObj = {...obj, ...{
            
                'results': obj.results ?
                
                obj.results.map((item)=>{return model.MetricsSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SummarizeMetricsResponse): object {
        const jsonObj = {...obj, ...{
            
                    'results': obj.results ?
                
                obj.results.map((item)=>{return model.MetricsSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
