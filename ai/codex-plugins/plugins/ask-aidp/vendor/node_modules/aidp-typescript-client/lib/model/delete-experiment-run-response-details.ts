// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting experiment run.
*/
export interface DeleteExperimentRunResponseDetails {

}

export namespace DeleteExperimentRunResponseDetails {

    export function getJsonObj(obj: DeleteExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
