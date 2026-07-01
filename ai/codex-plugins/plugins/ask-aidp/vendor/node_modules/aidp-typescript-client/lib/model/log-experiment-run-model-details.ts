// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of an experiment run model.
*/
export interface LogExperimentRunModelDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Model file in JSON format
    */
    'modelJson': string;

}

export namespace LogExperimentRunModelDetails {



    export function getJsonObj(obj: LogExperimentRunModelDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

                'model_json': obj.modelJson,

        }};

        delete (jsonObj as Partial<LogExperimentRunModelDetails>).runId;delete (jsonObj as Partial<LogExperimentRunModelDetails>).modelJson;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogExperimentRunModelDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

                'modelJson': (obj as any)["model_json"],

         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["model_json"];
        
        return jsonObj;
    }
}
