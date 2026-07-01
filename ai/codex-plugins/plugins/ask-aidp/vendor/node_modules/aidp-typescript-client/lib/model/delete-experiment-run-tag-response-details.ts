// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for deleting experiment run tag
*/
export interface DeleteExperimentRunTagResponseDetails {

}

export namespace DeleteExperimentRunTagResponseDetails {

    export function getJsonObj(obj: DeleteExperimentRunTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeleteExperimentRunTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
