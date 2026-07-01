// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Time range and granularity used for Metrics time-series data
*/
export interface AgentFlowSessionMetricsRange {
    /**
    * Start time for Metrics data
    */
    'timeBegin': Date;
    /**
    * End time for Metrics data
    */
    'timeEnd': Date;
    /**
    * Granularity within the selected time range
    */
    'granularity': string;
    /**
    * Time zone used for metrics data
    */
    'timezone': string;

}

export namespace AgentFlowSessionMetricsRange {





    export function getJsonObj(obj: AgentFlowSessionMetricsRange): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowSessionMetricsRange): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
