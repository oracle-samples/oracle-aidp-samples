// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of history of ExperimentRun metrics.
*/
export interface ExperimentRunMetricHistoryCollection {
    /**
    * Logged values for the metric.
    */
    'metrics'?: Array<model.ExperimentRunMetric>;
    /**
    * Token that can be used to retrieve the next page of metric history. An empty token means that no more metric history are available for retrieval.
    */
    'nextPageToken'?: string;

}

export namespace ExperimentRunMetricHistoryCollection {



    export function getJsonObj(obj: ExperimentRunMetricHistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                'metrics': obj.metrics ?
                
                obj.metrics.map((item)=>{return model.ExperimentRunMetric.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<ExperimentRunMetricHistoryCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunMetricHistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                    'metrics': obj.metrics ?
                
                obj.metrics.map((item)=>{return model.ExperimentRunMetric.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
