// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the experiment run to restore.
*/
export interface RestoreExperimentRunDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;

}

export namespace RestoreExperimentRunDetails {


    export function getJsonObj(obj: RestoreExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,

        }};

        delete (jsonObj as Partial<RestoreExperimentRunDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RestoreExperimentRunDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],

         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
