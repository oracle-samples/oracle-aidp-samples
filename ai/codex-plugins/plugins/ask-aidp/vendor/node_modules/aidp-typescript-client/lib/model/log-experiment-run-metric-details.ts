// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of an experiment run metric.
*/
export interface LogExperimentRunMetricDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Name of the metric.
    */
    'key': string;
    /**
    * Value of the metric. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'value': number;
    /**
    * Unix timestamp in milliseconds when this metric being recorded. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timestamp': number;
    /**
    * Step at which to log the metric. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'step'?: number;

}

export namespace LogExperimentRunMetricDetails {






    export function getJsonObj(obj: LogExperimentRunMetricDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,





        }};

        delete (jsonObj as Partial<LogExperimentRunMetricDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogExperimentRunMetricDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],





         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
