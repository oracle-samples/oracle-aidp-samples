// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tag details to set on an experiment.
*/
export interface SetExperimentTagDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;
    /**
    * Key of the experiment tag.
    */
    'key': string;
    /**
    * Value of the experiment tag.
    */
    'value': string;

}

export namespace SetExperimentTagDetails {




    export function getJsonObj(obj: SetExperimentTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,



        }};

        delete (jsonObj as Partial<SetExperimentTagDetails>).experimentId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetExperimentTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],



         }};

        delete (jsonObj as any)["experiment_id"];
        
        return jsonObj;
    }
}
