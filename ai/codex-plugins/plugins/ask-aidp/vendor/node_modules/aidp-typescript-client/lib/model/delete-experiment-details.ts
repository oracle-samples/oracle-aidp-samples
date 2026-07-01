// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the experiment to delete.
*/
export interface DeleteExperimentDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;

}

export namespace DeleteExperimentDetails {


    export function getJsonObj(obj: DeleteExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,

        }};

        delete (jsonObj as Partial<DeleteExperimentDetails>).experimentId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],

         }};

        delete (jsonObj as any)["experiment_id"];
        
        return jsonObj;
    }
}
