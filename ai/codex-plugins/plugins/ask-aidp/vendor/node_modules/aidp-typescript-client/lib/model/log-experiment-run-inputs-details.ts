// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run inputs.
*/
export interface LogExperimentRunInputsDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Dataset inputs for the run.
    */
    'datasetInputs'?: Array<model.DatasetInput>;

}

export namespace LogExperimentRunInputsDetails {



    export function getJsonObj(obj: LogExperimentRunInputsDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

                'dataset_inputs': obj.datasetInputs ?
                
                obj.datasetInputs.map((item)=>{return model.DatasetInput.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<LogExperimentRunInputsDetails>).runId;delete (jsonObj as Partial<LogExperimentRunInputsDetails>).datasetInputs;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogExperimentRunInputsDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

                    'datasetInputs': (obj as any)["dataset_inputs"] ?
                
                (obj as any)["dataset_inputs"].map((item: any)=>{return model.DatasetInput.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["dataset_inputs"];
        
        return jsonObj;
    }
}
