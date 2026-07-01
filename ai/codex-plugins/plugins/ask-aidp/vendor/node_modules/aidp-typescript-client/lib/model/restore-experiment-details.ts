// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the experiment to restore.
*/
export interface RestoreExperimentDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;

}

export namespace RestoreExperimentDetails {


    export function getJsonObj(obj: RestoreExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,

        }};

        delete (jsonObj as Partial<RestoreExperimentDetails>).experimentId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RestoreExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],

         }};

        delete (jsonObj as any)["experiment_id"];
        
        return jsonObj;
    }
}
