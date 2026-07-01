// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tag details to delete on an experiment.
*/
export interface DeleteExperimentTagDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;
    /**
    * Key of the experiment tag.
    */
    'key': string;

}

export namespace DeleteExperimentTagDetails {



    export function getJsonObj(obj: DeleteExperimentTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,


        }};

        delete (jsonObj as Partial<DeleteExperimentTagDetails>).experimentId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],


         }};

        delete (jsonObj as any)["experiment_id"];
        
        return jsonObj;
    }
}
