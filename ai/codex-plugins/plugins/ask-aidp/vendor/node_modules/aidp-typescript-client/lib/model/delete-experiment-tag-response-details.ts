// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting tag on an experiment.
*/
export interface DeleteExperimentTagResponseDetails {

}

export namespace DeleteExperimentTagResponseDetails {

    export function getJsonObj(obj: DeleteExperimentTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
