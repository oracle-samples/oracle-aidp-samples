// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run data.
*/
export interface ExperimentRunData {
    /**
    * Metrics logged for the run.
    */
    'metrics'?: Array<model.ExperimentRunMetric>;
    /**
    * Parameters logged for the run.
    */
    'params'?: Array<model.ExperimentRunParam>;
    /**
    * Tags to log.
    */
    'tags'?: Array<model.ExperimentRunTag>;

}

export namespace ExperimentRunData {




    export function getJsonObj(obj: ExperimentRunData): object {
        const jsonObj = {...obj, ...{
            
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

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunData): object {
        const jsonObj = {...obj, ...{
            
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

        
        
        return jsonObj;
    }
}
