// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update an experiment run.
*/
export interface UpdateExperimentRunDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Updated status of the run.
    */
    'status'?: model.ExperimentRunStatus;
    /**
    * Unix timestamp in milliseconds when the run ended. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime'?: number;
    /**
    * Updated name of the run.
    */
    'runName'?: string;

}

export namespace UpdateExperimentRunDetails {





    export function getJsonObj(obj: UpdateExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,


                'end_time': obj.endTime,

                'run_name': obj.runName,

        }};

        delete (jsonObj as Partial<UpdateExperimentRunDetails>).runId;delete (jsonObj as Partial<UpdateExperimentRunDetails>).endTime;delete (jsonObj as Partial<UpdateExperimentRunDetails>).runName;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],


                'endTime': (obj as any)["end_time"],

                'runName': (obj as any)["run_name"],

         }};

        delete (jsonObj as any)["run_id"];delete (jsonObj as any)["end_time"];delete (jsonObj as any)["run_name"];
        
        return jsonObj;
    }
}
