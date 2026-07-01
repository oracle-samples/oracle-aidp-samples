// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run outputs.
*/
export interface ExperimentRunOutputs {
    /**
    * Model outputs for the run.
    */
    'modelOutputs'?: Array<model.ModelOutput>;

}

export namespace ExperimentRunOutputs {


    export function getJsonObj(obj: ExperimentRunOutputs): object {
        const jsonObj = {...obj, ...{
            
                'model_outputs': obj.modelOutputs ?
                
                obj.modelOutputs.map((item)=>{return model.ModelOutput.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<ExperimentRunOutputs>).modelOutputs;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunOutputs): object {
        const jsonObj = {...obj, ...{
            
                    'modelOutputs': (obj as any)["model_outputs"] ?
                
                (obj as any)["model_outputs"].map((item: any)=>{return model.ModelOutput.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["model_outputs"];
        
        return jsonObj;
    }
}
