// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tag details to set on an experiment run.
*/
export interface SetExperimentRunTagDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Key of the run tag.
    */
    'key': string;
    /**
    * Value of the run tag.
    */
    'value': string;

}

export namespace SetExperimentRunTagDetails {




    export function getJsonObj(obj: SetExperimentRunTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,



        }};

        delete (jsonObj as Partial<SetExperimentRunTagDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetExperimentRunTagDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],



         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
