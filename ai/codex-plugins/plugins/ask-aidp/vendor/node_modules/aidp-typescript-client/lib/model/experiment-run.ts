// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of an experiment run.
*/
export interface ExperimentRun {
    'info'?: model.ExperimentRunInfo;
    'data'?: model.ExperimentRunData;
    'inputs'?: model.ExperimentRunInputs;
    'outputs'?: model.ExperimentRunOutputs;

}

export namespace ExperimentRun {





    export function getJsonObj(obj: ExperimentRun): object {
        const jsonObj = {...obj, ...{
            
                'info': obj.info ?
                
                
                model.ExperimentRunInfo.getJsonObj(obj.info) : undefined,
                'data': obj.data ?
                
                
                model.ExperimentRunData.getJsonObj(obj.data) : undefined,
                'inputs': obj.inputs ?
                
                
                model.ExperimentRunInputs.getJsonObj(obj.inputs) : undefined,
                'outputs': obj.outputs ?
                
                
                model.ExperimentRunOutputs.getJsonObj(obj.outputs) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRun): object {
        const jsonObj = {...obj, ...{
            
                    'info': obj.info ?
                
                
                model.ExperimentRunInfo.getDeserializedJsonObj(obj.info) : undefined,
                    'data': obj.data ?
                
                
                model.ExperimentRunData.getDeserializedJsonObj(obj.data) : undefined,
                    'inputs': obj.inputs ?
                
                
                model.ExperimentRunInputs.getDeserializedJsonObj(obj.inputs) : undefined,
                    'outputs': obj.outputs ?
                
                
                model.ExperimentRunOutputs.getDeserializedJsonObj(obj.outputs) : undefined,
         }};

        
        
        return jsonObj;
    }
}
