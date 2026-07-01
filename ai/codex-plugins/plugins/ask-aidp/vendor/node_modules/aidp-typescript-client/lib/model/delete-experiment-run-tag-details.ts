// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tag details to delete on an experiment run.
*/
export interface DeleteExperimentRunTagDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Key of the run tag.
    */
    'key': string;

}

export namespace DeleteExperimentRunTagDetails {



    export function getJsonObj(obj: DeleteExperimentRunTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,


        }};

        delete (jsonObj as Partial<DeleteExperimentRunTagDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentRunTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],


         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
