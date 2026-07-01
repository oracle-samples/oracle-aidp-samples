// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for creating an experiment.
*/
export interface CreateExperimentResponseDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;

}

export namespace CreateExperimentResponseDetails {


    export function getJsonObj(obj: CreateExperimentResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,

        }};

        delete (jsonObj as Partial<CreateExperimentResponseDetails>).experimentId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateExperimentResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],

         }};

        delete (jsonObj as any)["experiment_id"];
        
        return jsonObj;
    }
}
