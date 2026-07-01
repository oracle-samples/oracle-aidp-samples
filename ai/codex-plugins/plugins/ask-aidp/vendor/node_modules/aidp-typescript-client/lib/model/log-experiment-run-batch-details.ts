// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run batch data to log.
*/
export interface LogExperimentRunBatchDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Metrics to log.
    */
    'metrics'?: Array<model.ExperimentRunMetric>;
    /**
    * Params to log.
    */
    'params'?: Array<model.ExperimentRunParam>;
    /**
    * Tags to log.
    */
    'tags'?: Array<model.ExperimentRunTag>;

}

export namespace LogExperimentRunBatchDetails {





    export function getJsonObj(obj: LogExperimentRunBatchDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

                'metrics': obj.metrics ?
                
                obj.metrics.map((item)=>{return model.ExperimentRunMetric.getJsonObj(item)})
                
                 : undefined,
                'params': obj.params ?
                
                obj.params.map((item)=>{return model.ExperimentRunParam.getJsonObj(item)})
                
                 : undefined,
                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentRunTag.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<LogExperimentRunBatchDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogExperimentRunBatchDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

                    'metrics': obj.metrics ?
                
                obj.metrics.map((item)=>{return model.ExperimentRunMetric.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'params': obj.params ?
                
                obj.params.map((item)=>{return model.ExperimentRunParam.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.ExperimentRunTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
