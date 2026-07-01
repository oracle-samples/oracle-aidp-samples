// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update an experiment.
*/
export interface UpdateExperimentDetails {
    /**
    * Unique identifier for the experiment.
    */
    'experimentId': string;
    /**
    * If provided, name of the experiment is changed to the new name. The new name must be unique.
    */
    'newName'?: string;

}

export namespace UpdateExperimentDetails {



    export function getJsonObj(obj: UpdateExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experiment_id': obj.experimentId,

                'new_name': obj.newName,

        }};

        delete (jsonObj as Partial<UpdateExperimentDetails>).experimentId;delete (jsonObj as Partial<UpdateExperimentDetails>).newName;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentDetails): object {
        const jsonObj = {...obj, ...{
            
                'experimentId': (obj as any)["experiment_id"],

                'newName': (obj as any)["new_name"],

         }};

        delete (jsonObj as any)["experiment_id"];delete (jsonObj as any)["new_name"];
        
        return jsonObj;
    }
}
