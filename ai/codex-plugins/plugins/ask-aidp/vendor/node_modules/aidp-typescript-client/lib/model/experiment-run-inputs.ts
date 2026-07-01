// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run inputs.
*/
export interface ExperimentRunInputs {
    /**
    * Dataset inputs for the run.
    */
    'datasetInputs'?: Array<model.DatasetInput>;
    /**
    * Model inputs for the run.
    */
    'modelInputs'?: Array<model.ModelInput>;

}

export namespace ExperimentRunInputs {



    export function getJsonObj(obj: ExperimentRunInputs): object {
        const jsonObj = {...obj, ...{
            
                'dataset_inputs': obj.datasetInputs ?
                
                obj.datasetInputs.map((item)=>{return model.DatasetInput.getJsonObj(item)})
                
                 : undefined,
                'model_inputs': obj.modelInputs ?
                
                obj.modelInputs.map((item)=>{return model.ModelInput.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<ExperimentRunInputs>).datasetInputs;delete (jsonObj as Partial<ExperimentRunInputs>).modelInputs;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunInputs): object {
        const jsonObj = {...obj, ...{
            
                    'datasetInputs': (obj as any)["dataset_inputs"] ?
                
                (obj as any)["dataset_inputs"].map((item: any)=>{return model.DatasetInput.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'modelInputs': (obj as any)["model_inputs"] ?
                
                (obj as any)["model_inputs"].map((item: any)=>{return model.ModelInput.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["dataset_inputs"];delete (jsonObj as any)["model_inputs"];
        
        return jsonObj;
    }
}
